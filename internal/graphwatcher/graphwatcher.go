// Package graphwatcher subscribes to the LND channel graph, detects new
// channels to auto-rebalance targets and, when the new peer's fee toward the
// target is below the target's buy price, probes refill routes through that
// new channel (jobs.ProbeVia) and buys over a verified route that costs less
// than the buy price. It also logs the routing policies around our peers
// (market.go).
package graphwatcher

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Querier bundles all database access required by graphwatcher.
type Querier interface {
	jobs.ProbeQuerier // settings, probe-Queries, GetPeerAlias
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
	ListAutoRebalancePeerPubkeys(ctx context.Context) ([]string, error)
	ListOpenARChannelsByPubkey(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error)
	RebalPaymentsForChannel(ctx context.Context, arg db.RebalPaymentsForChannelParams) ([]db.RebalPaymentsForChannelRow, error)
	InsertGraphProbeLog(ctx context.Context, arg db.InsertGraphProbeLogParams) error
	InsertGraphEvent(ctx context.Context, arg db.InsertGraphEventParams) error
	UpdateGraphEventRoutesFound(ctx context.Context, arg db.UpdateGraphEventRoutesFoundParams) error
	DeleteOldGraphEvents(ctx context.Context) error
	ListOpenPeerPubkeys(ctx context.Context) ([]string, error)
	InsertMarketPolicies(ctx context.Context, arg []db.InsertMarketPoliciesParams) (int64, error)
	LastMarketSnapshot(ctx context.Context) (pgtype.Timestamptz, error)
	DeleteOldMarketPolicies(ctx context.Context, timestamp pgtype.Timestamptz) (int64, error)
	InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error)
	UpdateRebalancerRecord(ctx context.Context, arg db.UpdateRebalancerRecordParams) error
}

// Client bundles the LND RPCs needed by graphwatcher (subset of LightningClient).
type Client interface {
	jobs.ProbeLightningClient // GetInfo, QueryRoutes
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
	GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error)
	AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error)
	SubscribeChannelGraph(ctx context.Context, in *lnrpc.GraphTopologySubscription, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.GraphTopologyUpdate], error)
}

// Deps bundles all external dependencies. Router is used by jobs.ProbeVia and
// buyVia (BuildRoute/SendToRouteV2).
type Deps struct {
	Q      Querier
	LN     Client
	Router jobs.ProbeRouterClient
	Now    func() time.Time
	Sleep  func(time.Duration)
}

func gwLog(msg string) {
	fmt.Printf("%s : [GraphWatcher] : %s\n", time.Now().Format("Mon Jan  2 15:04:05 2006"), msg)
}

func chanIDStr(id uint64) string { return strconv.FormatUint(id, 10) }

// getSetting returns the value of a local setting, or def if not found.
func getSetting(ctx context.Context, q Querier, key, def string) string {
	row, err := q.GetLocalSetting(ctx, key)
	if err != nil {
		return def
	}
	return row.Value
}

// ensureSetting returns the value of a local setting, creating it with the given default if absent.
func ensureSetting(ctx context.Context, q Querier, key, def string) string {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: def})
	if err != nil {
		return def
	}
	return row.Value
}

func atoiOr(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// loadARTargets returns auto-rebalance peer pubkeys minus any listed in the GW-Exclude setting.
func loadARTargets(ctx context.Context, q Querier) (map[string]struct{}, error) {
	excluded := map[string]struct{}{}
	raw := getSetting(ctx, q, "GW-Exclude", "")
	if raw != "" {
		for _, pk := range strings.Split(raw, ",") {
			if t := strings.TrimSpace(pk); t != "" {
				excluded[t] = struct{}{}
			}
		}
	}
	pubkeys, err := q.ListAutoRebalancePeerPubkeys(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for _, pk := range pubkeys {
		if _, ex := excluded[pk]; !ex {
			out[pk] = struct{}{}
		}
	}
	return out, nil
}

// getAlias resolves a display name for a pubkey: peer alias > node cache > first 12 chars.
func (d Deps) getAlias(ctx context.Context, pubkey string) string {
	alias, err := d.Q.GetPeerAlias(ctx, pubkey)
	if err == nil && alias.Valid && alias.String != "" {
		return alias.String
	}
	if info, err := lnd.GetNodeInfoCached(ctx, d.Q, d.LN, pubkey); err == nil {
		if a := info.GetNode().GetAlias(); a != "" {
			return a
		}
	}
	if len(pubkey) >= 12 {
		return pubkey[:12]
	}
	return pubkey
}

// probePlan is what probeGate hands to triggerProbe: the target's open AR
// channels (targets[0] sets fee and budget) and the gate's prices in ppm.
type probePlan struct {
	targets   []db.GuiChannel
	buyPpm    int
	budgetPpm int
}

// probeGate decides whether a new channel of another node to the target is
// worth a probe: the target has an open AR channel with remote balance above
// reserve, and the node's fee toward the target is below the buy price there.
// The buy price is the target's rebalance cost as the FLP floor computes it
// (af.AvgRebalanceCostPPM over FLP-Lookback rebalances, source fee included),
// or the budget (target fee * ar_max_cost %) without rebalance history. A nil
// plan comes with the reason it was skipped.
func (d Deps) probeGate(ctx context.Context, targetPubkey string, otherFeePpm int64) (*probePlan, string, error) {
	targets, err := d.Q.ListOpenARChannelsByPubkey(ctx, targetPubkey)
	if err != nil {
		return nil, "", err
	}
	if len(targets) == 0 {
		return nil, "no open AR channel", nil
	}
	allFull := true
	for _, c := range targets {
		if c.RemoteBalance > int64(c.LocalChanReserve) {
			allFull = false
			break
		}
	}
	if allFull {
		return nil, "all channels full", nil
	}
	ch := targets[0]
	plan := &probePlan{targets: targets, budgetPpm: int(float64(int(ch.LocalFeeRate)*int(ch.ArMaxCost)) / 100)}
	pays, err := d.Q.RebalPaymentsForChannel(ctx, db.RebalPaymentsForChannelParams{
		RebalChan: pgtype.Text{String: ch.ChanID, Valid: true},
		Limit:     int32(atoiOr(getSetting(ctx, d.Q, "FLP-Lookback", "10"), 10)),
	})
	if err != nil {
		return nil, "", err
	}
	plan.buyPpm = plan.budgetPpm
	if v := af.AvgRebalanceCostPPM(pays); v != nil {
		plan.buyPpm = *v
	}
	if otherFeePpm >= int64(plan.buyPpm) {
		return nil, fmt.Sprintf("peer fee %d ppm not below buy price %d ppm", otherFeePpm, plan.buyPpm), nil
	}
	return plan, "", nil
}

// triggerProbe probes refill routes of the target that run through the other
// node's new channel (jobs.ProbeVia), buys over a verified route that costs
// less than the buy price (buyVia), and logs both to GraphProbeLog. No route is
// stored. Returns the number of verified routes.
func (d Deps) triggerProbe(ctx context.Context, selfPubkey string, plan *probePlan, otherPubkey string, otherFeePpm int64, chanID string, now time.Time) (int, error) {
	channels, err := d.Q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		return 0, err
	}
	maxFeeRate := atoiOr(getSetting(ctx, d.Q, "AR-MaxFeeRate", "500"), 500)
	maxPerTarget := atoiOr(getSetting(ctx, d.Q, "QR-MaxPerTarget", "5"), 5)

	ch := plan.targets[0]
	otherAlias := d.getAlias(ctx, otherPubkey)
	gwLog(fmt.Sprintf("Probing %s via %s (fee=%d ppm, buy price %d ppm, budget %d ppm) triggered by new channel %s",
		ch.Alias, otherAlias, otherFeePpm, plan.buyPpm, plan.budgetPpm, chanID))

	attempts, err := jobs.ProbeVia(ctx, d.LN, d.Router, selfPubkey, ch, otherPubkey, channels, maxFeeRate, maxPerTarget)
	if err != nil {
		return 0, err
	}
	var lines []string
	addLine := func(l string) {
		lines = append(lines, l)
		gwLog("  " + l)
	}
	verified, failed := 0, 0
	var viaFee pgtype.Int4
	var viaAmount pgtype.Int8
	var best *jobs.ViaAttempt
	for i, a := range attempts {
		head := fmt.Sprintf("out=%s (source fee %d ppm), %d hops", a.SourceChanID, a.SourceFeeRate, len(a.HopKeys))
		if !a.Verified {
			failed++
			addLine(head + ": failed")
			continue
		}
		verified++
		best = &attempts[i]
		routePpm := 0.0 // no fee ppm from the probe: the route charged no fees
		if a.FeePpm != nil {
			routePpm = *a.FeePpm
		}
		viaFee = pgtype.Int4{Int32: int32(math.Round(routePpm)) + int32(a.SourceFeeRate), Valid: true}
		viaAmount = pgtype.Int8{Int64: a.AmountSat, Valid: true}
		addLine(fmt.Sprintf("%s: verified %d sat @ %d ppm incl. source fee", head, a.AmountSat, viaFee.Int32))
	}
	if len(attempts) == 0 {
		addLine("no route to " + otherAlias + " from any source")
	}

	var buyStatus pgtype.Int4
	bought := false
	switch {
	case best == nil:
	case int(viaFee.Int32) >= plan.buyPpm:
		addLine(fmt.Sprintf("no purchase: %d ppm not below buy price %d ppm", viaFee.Int32, plan.buyPpm))
	default:
		rec, sent, berr := d.buyVia(ctx, ch, *best, plan.buyPpm)
		switch {
		case berr != nil && !sent:
			addLine("purchase error: " + berr.Error())
		case !sent:
			addLine(fmt.Sprintf("no purchase: rebuilt route not below buy price %d ppm", plan.buyPpm))
		default:
			bought = true
			buyStatus = pgtype.Int4{Int32: rec.Status, Valid: true}
			if rec.Status == rebalSucceeded {
				addLine(fmt.Sprintf("bought %d sat for %.3f sat fees (rebalance %d)", rec.Value, rec.FeesPaid.Float64, rec.ID))
			} else {
				addLine(fmt.Sprintf("purchase of %d sat failed with status %d (rebalance %d)", rec.Value, rec.Status, rec.ID))
			}
			if berr != nil {
				addLine("error saving rebalance record: " + berr.Error())
			}
		}
	}

	if err := d.Q.InsertGraphProbeLog(ctx, db.InsertGraphProbeLogParams{
		Timestamp: pgtype.Timestamptz{Time: now, Valid: true}, TargetPubkey: ch.RemotePubkey, TargetAlias: ch.Alias,
		TargetFee: ch.LocalFeeRate, TargetMaxCost: ch.ArMaxCost, TriggerChanID: chanID,
		OtherPubkey: otherPubkey, OtherAlias: otherAlias, OtherFeePpm: pgtype.Int4{Int32: int32(otherFeePpm), Valid: true},
		BudgetPpm: int32(plan.budgetPpm), SourcesTried: int32(len(attempts)), Errors: int32(failed),
		RoutesViaNewPeer: int32(verified), RebalanceScheduled: bought, Details: strings.Join(lines, "\n"),
		BuyPpm: pgtype.Int4{Int32: int32(plan.buyPpm), Valid: true}, ViaFeePpm: viaFee, ViaAmountSat: viaAmount,
		BuyStatus: buyStatus,
	}); err != nil {
		return 0, err
	}
	return verified, nil
}
