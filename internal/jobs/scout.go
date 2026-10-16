package jobs

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

// Route scout: probes paths from our node to the nodes of the network with at
// least QR-ScoutMinChannels public channels, biggest first, so the rebalancer
// learns the success chance of many node pairs (route_attempt_log, list scout)
// and gets parts to combine refill routes from (scout_path).
//
// A probe reached its target when the target itself returned the failure:
// only a node that received the HTLC can, so every channel on the way carried
// the amount, whatever the failure code. A target is done while QR-ScoutPaths
// of its verified paths (last scoutWindow) have a success chance of at least
// scoutSureProb. Otherwise a
// pass sends up to QR-ScoutTries probes to it: after a probe that reached the
// target the next path search leaves out a node pair in the middle of that
// path, after a failure the failing pair; slow pairs the statistics ban are
// always left out. A probe carries a random payment hash, so no payment can
// settle; its amount is the median of the rebalances paid in the last
// scoutWindow (at least AR-MinAmount), its fee limit AR-MaxFeeRate ppm. It goes
// out over our channels that hold the amount and carry fewer pending HTLCs
// than AR-HtlcSoftLimit, at most QR-ScoutPerHour per hour.
const (
	scoutSureProb = 0.5
	scoutWindow   = 7 * 24 * time.Hour
)

type scoutLightningClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error)
	DescribeGraph(ctx context.Context, in *lnrpc.ChannelGraphRequest, opts ...grpc.CallOption) (*lnrpc.ChannelGraph, error)
	ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error)
}

type scoutQuerier interface {
	settingsQuerier
	ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error)
	ListScoutPaths(ctx context.Context, lastVerified pgtype.Timestamptz) ([]db.ScoutPath, error)
	UpsertScoutPath(ctx context.Context, arg db.UpsertScoutPathParams) error
	DeleteOldScoutPaths(ctx context.Context, lastVerified pgtype.Timestamptz) error
	MedianRebalanceValue(ctx context.Context, stop pgtype.Timestamptz) (float64, error)
	InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error
	ListSentRouteAttempts(ctx context.Context, timestamp pgtype.Timestamptz) ([]db.ListSentRouteAttemptsRow, error)
}

// scoutSettings are the settings of one scout pass.
type scoutSettings struct {
	perHour, tries, paths, minChannels, htlcSoftLimit int
	minAmount, maxFeeRate                             int64
}

func readScoutSettings(ctx context.Context, q scoutQuerier) (scoutSettings, error) {
	var s scoutSettings
	ints := []struct {
		key, def string
		dst      *int
	}{
		{"QR-ScoutPerHour", "600", &s.perHour}, {"QR-ScoutTries", "6", &s.tries}, {"QR-ScoutPaths", "3", &s.paths},
		{"QR-ScoutMinChannels", "5", &s.minChannels}, {"AR-HtlcSoftLimit", "5", &s.htlcSoftLimit},
	}
	for _, i := range ints {
		v, err := getOrCreateInt(ctx, q, i.key, i.def)
		if err != nil {
			return s, err
		}
		*i.dst = v
	}
	minAmount, err := getOrCreateInt(ctx, q, "AR-MinAmount", "2500")
	if err != nil {
		return s, err
	}
	maxFeeRate, err := getOrCreateInt(ctx, q, "AR-MaxFeeRate", "500")
	if err != nil {
		return s, err
	}
	s.minAmount, s.maxFeeRate = int64(minAmount), int64(maxFeeRate)
	s.perHour = max(s.perHour, 1)
	return s, nil
}

// scoutTargets returns the nodes of the graph with at least minChannels
// channels, most channels first (then by pubkey), leaving out our node and our
// peers.
func scoutTargets(graph *lnrpc.ChannelGraph, self string, peers map[string]bool, minChannels int) []string {
	deg := map[string]int{}
	for _, e := range graph.GetEdges() {
		deg[e.GetNode1Pub()]++
		deg[e.GetNode2Pub()]++
	}
	var out []string
	for pk, n := range deg {
		if n >= minChannels && pk != self && !peers[pk] {
			out = append(out, pk)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if deg[out[i]] != deg[out[j]] {
			return deg[out[i]] > deg[out[j]]
		}
		return out[i] < out[j]
	})
	return out
}

// nodePair is an lnrpc.NodePair of two hex pubkeys; nil when one does not parse.
func nodePair(from, to string) *lnrpc.NodePair {
	f, ferr := hex.DecodeString(from)
	t, terr := hex.DecodeString(to)
	if ferr != nil || terr != nil {
		return nil
	}
	return &lnrpc.NodePair{From: f, To: t}
}

// scoutPass is the state of one pass over the targets.
type scoutPass struct {
	q      scoutQuerier
	ln     scoutLightningClient
	router probeRouterClient
	s      scoutSettings
	stats  rebalancer.RouteStats
	htlcs  *rebalancer.HtlcCounter
	sleep  func(time.Duration)
	now    func() time.Time
	amount int64

	sent, verified, done int
}

// outgoing returns our channels that hold the amount and carry fewer pending
// HTLCs than the soft limit.
func (p *scoutPass) outgoing(ctx context.Context, channels []db.GuiChannel) []uint64 {
	var out []uint64
	for _, c := range channels {
		if c.LocalBalance-int64(c.LocalChanReserve) < p.amount {
			continue
		}
		id, err := strconv.ParseUint(c.ChanID, 10, 64)
		if err != nil {
			continue
		}
		if p.s.htlcSoftLimit > 0 && p.htlcs.Count(ctx, p.ln, id, p.now()) >= p.s.htlcSoftLimit {
			continue
		}
		out = append(out, id)
	}
	return out
}

// scoutTarget probes paths to one target until QR-ScoutPaths good paths are
// known or QR-ScoutTries probes went out. known are its verified paths. It
// returns false when no channel can send the amount.
func (p *scoutPass) scoutTarget(ctx context.Context, target string, known []string, channels []db.GuiChannel) bool {
	good := 0
	for _, path := range known {
		if p.stats.RouteProb(path) >= scoutSureProb {
			good++
		}
	}
	if good >= p.s.paths {
		p.done++
		return true
	}
	var ignored []*lnrpc.NodePair
	for _, b := range p.stats.BannedPairs(p.now()) {
		if np := nodePair(b[0], b[1]); np != nil {
			ignored = append(ignored, np)
		}
	}
	for try := 0; try < p.s.tries && good < p.s.paths && ctx.Err() == nil; try++ {
		outs := p.outgoing(ctx, channels)
		if len(outs) == 0 {
			return false
		}
		resp, qerr := p.ln.QueryRoutes(ctx, &lnrpc.QueryRoutesRequest{
			PubKey:            target,
			Amt:               p.amount,
			OutgoingChanIds:   outs,
			FeeLimit:          &lnrpc.FeeLimit{Limit: &lnrpc.FeeLimit_Fixed{Fixed: p.amount * p.s.maxFeeRate / 1_000_000}},
			IgnoredPairs:      ignored,
			UseMissionControl: true,
		})
		if qerr != nil || len(resp.GetRoutes()) == 0 {
			return true
		}
		route := resp.GetRoutes()[0]
		hops := route.GetHops()
		if len(hops) < 2 || hops[len(hops)-1].GetPubKey() != target {
			return true
		}
		o := probeRouteSync(ctx, p.router, route, 30*time.Second)
		p.sent++
		path := rebalancer.RoutePath(route)
		outChan := formatChanID(hops[0].GetChanId())
		if o.status != "error" {
			rebalancer.RecordAttempt(ctx, p.q, rebalancer.RouteAttempt{
				Target: target, OutChan: outChan, List: "scout", Position: try + 1, ValueSat: p.amount, Outcome: "failed",
				Code: o.code, Hop: o.hop, Path: path, DurationMs: o.durationMs,
			}, p.now())
			p.stats.Observe(path, "failed", o.hop, o.durationMs, p.now())
		}
		reached := o.status != "error" && o.hop == len(hops)
		switch {
		case reached:
			p.verified++
			if b, merr := proto.Marshal(route); merr == nil {
				if e := p.q.UpsertScoutPath(ctx, db.UpsertScoutPathParams{
					TargetPubkey: target, OutgoingChanID: outChan, Route: path, RouteHex: hex.EncodeToString(b),
					ValueSat: p.amount, LastVerified: ts(p.now()),
				}); e != nil {
					dataLog(fmt.Sprintf("Error saving scout path: %s", e))
				}
			}
			if p.stats.RouteProb(path) >= scoutSureProb {
				good++
			}
			// the next path should differ: leave out a pair in its middle
			i := len(hops)/2 - 1
			if np := nodePair(hops[i].GetPubKey(), hops[i+1].GetPubKey()); np != nil {
				ignored = append(ignored, np)
			}
		case o.status != "error" && o.hop >= 1 && o.hop < len(hops):
			if np := nodePair(hops[o.hop-1].GetPubKey(), hops[o.hop].GetPubKey()); np != nil {
				ignored = append(ignored, np)
			}
		default:
			return true
		}
		p.sleep(time.Hour / time.Duration(p.s.perHour))
	}
	if good >= p.s.paths {
		p.done++
	}
	return true
}

// scoutRun runs one pass over the targets when QR-ScoutEnabled is set; the
// pass ends early when the setting is turned off.
func scoutRun(ctx context.Context, q scoutQuerier, ln scoutLightningClient, router probeRouterClient, sleep func(time.Duration), now func() time.Time) error {
	enabled, err := settingGateEnabled(ctx, q, "QR-ScoutEnabled")
	if err != nil || !enabled {
		return err
	}
	s, err := readScoutSettings(ctx, q)
	if err != nil {
		return err
	}
	start := now()
	median, err := q.MedianRebalanceValue(ctx, ts(start.Add(-scoutWindow)))
	if err != nil {
		return err
	}
	info, err := ln.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	graph, err := ln.DescribeGraph(ctx, &lnrpc.ChannelGraphRequest{})
	if err != nil {
		return err
	}
	channels, err := q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		return err
	}
	peers := map[string]bool{}
	for _, c := range channels {
		peers[c.RemotePubkey] = true
	}
	targets := scoutTargets(graph, info.GetIdentityPubkey(), peers, s.minChannels)
	if err := q.DeleteOldScoutPaths(ctx, ts(start.Add(-scoutWindow))); err != nil {
		return err
	}
	paths, err := q.ListScoutPaths(ctx, ts(start.Add(-scoutWindow)))
	if err != nil {
		return err
	}
	known := map[string][]string{}
	for _, sp := range paths {
		known[sp.TargetPubkey] = append(known[sp.TargetPubkey], sp.Route)
	}

	p := &scoutPass{q: q, ln: ln, router: router, s: s, stats: rebalancer.NewRouteStats(), htlcs: rebalancer.NewHtlcCounter(),
		sleep: sleep, now: now, amount: max(int64(median), s.minAmount)}
	dataLog(fmt.Sprintf("Route scout: %d targets with at least %d channels, %d sats per probe", len(targets), s.minChannels, p.amount))
	for i, target := range targets {
		if ctx.Err() != nil {
			return nil
		}
		if i%50 == 0 {
			if on, gerr := settingGateEnabled(ctx, q, "QR-ScoutEnabled"); gerr != nil || !on {
				dataLog(fmt.Sprintf("Route scout stopped after %d of %d targets", i, len(targets)))
				return gerr
			}
		}
		p.stats.Refresh(ctx, q, now())
		if !p.scoutTarget(ctx, target, known[target], channels) {
			dataLog("Route scout: no channel can send the probe amount, pass ended")
			break
		}
	}
	dataLog(fmt.Sprintf("Route scout pass: %d targets, %d done, %d probes, %d reached their target (%s)",
		len(targets), p.done, p.sent, p.verified, now().Sub(start).Round(time.Second)))
	return nil
}

// ScoutLoop runs the route scout in its own loop: a pass takes hours. Every
// minute it starts a pass when QR-ScoutEnabled is set; getConn returns the LND
// connection the stubs are built from.
func ScoutLoop(ctx context.Context, q scoutQuerier, getConn func() (*grpc.ClientConn, error), sleep func(time.Duration)) {
	for ctx.Err() == nil {
		conn, err := getConn()
		if err == nil {
			err = scoutRun(ctx, q, lnrpc.NewLightningClient(conn), routerrpc.NewRouterClient(conn), sleep, time.Now)
		}
		if err != nil && ctx.Err() == nil {
			dataLog(fmt.Sprintf("Error scouting routes: %s", err))
		}
		sleep(time.Minute)
	}
}
