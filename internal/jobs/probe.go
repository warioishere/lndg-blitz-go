package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

// probeLightningClient is the LightningClient subset required by the probe subsystem.
type probeLightningClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error)
}

// probeRouterClient is the RouterClient (routerrpc) subset used for binary search probing.
type probeRouterClient interface {
	SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error)
	BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error)
}

// probeQuerier bundles all DB access required by the probe subsystem.
type probeQuerier interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
	UpsertLocalSetting(ctx context.Context, arg db.UpsertLocalSettingParams) error
	ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error)
	GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error)
	InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error
	UpdateRebalanceRouteProbe(ctx context.Context, arg db.UpdateRebalanceRouteProbeParams) error
	InsertProbeLog(ctx context.Context, arg db.InsertProbeLogParams) error
	DeleteOldProbeLogs(ctx context.Context) error
	GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error)
	InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error
	NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error)
	NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error)
	ListNkPeerChannels(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error)
}

// refillProbeQuerier is the route prober's DB access: probeQuerier and the
// refill price it writes.
type refillProbeQuerier interface {
	probeQuerier
	UpdatePeerRefillPrice(ctx context.Context, arg db.UpdatePeerRefillPriceParams) error
}

// probeTargetDetail holds per-target probe statistics written to the ProbeLog
// details JSON. Errors is ProbeFailed + RPCErrors; OutChansTried counts the
// sources that had a route (or an RPC error) and used up a QR-MaxPerTarget slot.
// OverBudget counts verified routes above the target's budget, which are not
// stored; RefillPpm and RefillAmountSat are the refill price (nil: none).
type probeTargetDetail struct {
	Pubkey        string `json:"pubkey"`
	Alias         string `json:"alias"`
	New           int    `json:"new"`
	Existing      int    `json:"existing"`
	Errors        int    `json:"errors"`
	Verified      int    `json:"verified"`
	OutChansTried int    `json:"out_chans_tried"`
	Sources       int    `json:"sources"`
	OverBudget    int    `json:"over_budget"`
	NoRoute       int    `json:"no_route"`
	ProbeFailed   int    `json:"probe_failed"`
	RPCErrors     int    `json:"rpc_errors"`
	RefillPpm     *int   `json:"refill_ppm"`
	RefillAmount  int64  `json:"refill_amount_sat"`
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// probeOutcome is the result of one probe HTLC: status, and for a failed HTLC
// its failure code and failure source index and the time until the result.
type probeOutcome struct {
	status     string
	code, hop  int
	durationMs int
}

// probeRecorder records one probe HTLC over route for amountSat sats; nil
// records nothing.
type probeRecorder func(route *lnrpc.Route, amountSat int64, o probeOutcome)

// attemptRecorder records the probe HTLCs to target from outChan in
// route_attempt_log (list probe), the evidence of the rebalancer's route
// statistics: a verified route as a failure at our node (every node pair
// carried it), any other failure at its failing node.
func attemptRecorder(ctx context.Context, q rebalancer.AttemptLogQuerier, target, outChan string) probeRecorder {
	if q == nil {
		return nil
	}
	return func(route *lnrpc.Route, amountSat int64, o probeOutcome) {
		if o.status == "error" {
			return
		}
		rebalancer.RecordAttempt(ctx, q, rebalancer.RouteAttempt{
			Target: target, OutChan: outChan, List: "probe", ValueSat: amountSat, Outcome: "failed",
			Code: o.code, Hop: o.hop, Path: rebalancer.RoutePath(route), DurationMs: o.durationMs,
		}, time.Now())
	}
}

// probeRouteSync sends a probe HTLC with a random payment hash and classifies the result:
//
//	'verified'  — code==1 (INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS) and fsi==len(hops)
//	'liquidity' — code==15 (TEMPORARY_CHANNEL_FAILURE)
//	'fee'       — code==12 (FEE_INSUFFICIENT)
//	'invalid'   — any other failure code
//	'error'     — gRPC error or unexpected status
func probeRouteSync(ctx context.Context, routerstub probeRouterClient, route *lnrpc.Route, timeout time.Duration) probeOutcome {
	fakeHash := make([]byte, 32)
	if _, err := rand.Read(fakeHash); err != nil {
		return probeOutcome{status: "error"}
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	result, err := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{PaymentHash: fakeHash, Route: route})
	if err != nil {
		return probeOutcome{status: "error"}
	}
	if result.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED { // cannot succeed with a fake hash
		return probeOutcome{status: "error"}
	}
	failure := result.GetFailure()
	o := probeOutcome{code: int(failure.GetCode()), hop: int(failure.GetFailureSourceIndex()),
		durationMs: int(time.Since(start) / time.Millisecond)}
	nHops := len(route.GetHops())
	switch {
	case o.code == 1 && o.hop == nHops:
		o.status = "verified"
	case o.code == 15:
		o.status = "liquidity"
	case o.code == 12:
		o.status = "fee"
	default:
		o.status = "invalid"
	}
	return o
}

// probeSearch is the result of probeWithBinarySearch: the verified route
// (routeHex nil: none) with its largest verified amount and fee ppm, and the
// node pair a probe failed at before the target peer (failFrom empty: none),
// which a search for another path leaves out.
type probeSearch struct {
	routeHex         *string
	amountSat        int64
	feePpm           *float64
	failFrom, failTo string
}

// probeWithBinarySearch probes the route (source_chan_id, hop_pubkeys) from
// targetAmountSat on. A liquidity failure at any hop halves towards the last
// verified amount, never below minAmountSat. When nothing verified, the node
// pair of the last failure before the target peer comes back, so a search for
// another path leaves it out; any other failure ends the search with its pair
// at once. Search parameters: max_steps=10, gap threshold max(good/20, 1000),
// fee budget scaled proportionally to the amount. rec records every probe HTLC
// (nil: none).
func probeWithBinarySearch(ctx context.Context, routerstub probeRouterClient, sourceChanID string, hopPubkeys [][]byte, cltvDelta int32, targetAmountSat, feeLimitSat, minAmountSat int64, rec probeRecorder) probeSearch {
	const maxSteps = 10
	var res probeSearch
	var liqFrom, liqTo string
	var good int64 = 0
	bad := targetAmountSat + 1
	amount := targetAmountSat
	feeRetried := false

search:
	for step := 0; step < maxSteps; step++ {
		if amount <= 0 {
			break
		}
		outChanID, perr := strconv.ParseUint(sourceChanID, 10, 64)
		if perr != nil {
			break
		}
		bctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		built, berr := routerstub.BuildRoute(bctx, &routerrpc.BuildRouteRequest{
			OutgoingChanId: outChanID,
			AmtMsat:        amount * 1000,
			HopPubkeys:     hopPubkeys,
			FinalCltvDelta: cltvDelta,
		})
		cancel()
		if berr != nil {
			break
		}
		routeMsg := built.GetRoute()
		hops := routeMsg.GetHops()
		if len(hops) == 0 {
			break
		}
		// Scale the fee budget proportionally to the current probe amount; if the
		// route is already too expensive at this amount (due to constant base fees),
		// halving will not help so we abort.
		amountFeeLimitSat := int64(float64(feeLimitSat*amount) / float64(targetAmountSat))
		if amountFeeLimitSat < 1 {
			amountFeeLimitSat = 1
		}
		if routeMsg.GetTotalFeesMsat()/1000 > amountFeeLimitSat {
			break
		}
		var routeFeePpm *float64
		totalAmt := routeMsg.GetTotalAmtMsat()
		totalFees := routeMsg.GetTotalFeesMsat()
		if totalAmt != 0 && totalFees != 0 && totalAmt > totalFees {
			v := (float64(totalFees) / float64(totalAmt-totalFees)) * 1000000
			routeFeePpm = &v
		}
		o := probeRouteSync(ctx, routerstub, routeMsg, 30*time.Second)
		if rec != nil {
			rec(routeMsg, amount, o)
		}
		atTarget := o.hop == len(hops)-1 // the target peer is the last hop before our node
		beforeTarget := o.status != "error" && o.hop >= 1 && o.hop < len(hops)-1
		switch {
		case o.status == "verified":
			b, _ := proto.Marshal(routeMsg)
			h := hex.EncodeToString(b)
			res.routeHex, res.feePpm = &h, routeFeePpm
			good = amount
			if bad-good <= maxInt64(good/20, 1000) {
				break search
			}
			amount = (good + bad) / 2
		case o.status == "liquidity":
			if beforeTarget {
				liqFrom, liqTo = hops[o.hop-1].GetPubKey(), hops[o.hop].GetPubKey()
			} else if atTarget {
				liqFrom, liqTo = "", ""
			}
			bad = amount
			amount = (good + bad) / 2
			if bad-good <= maxInt64(good/20, 1000) || amount < minAmountSat {
				break search
			}
		case o.status == "fee":
			// Retry the same amount once; if the fee failure persists, abort.
			if feeRetried {
				break search
			}
			feeRetried = true
			continue
		default:
			if beforeTarget {
				res.failFrom, res.failTo = hops[o.hop-1].GetPubKey(), hops[o.hop].GetPubKey()
			}
			break search
		}
	}
	if good == 0 && res.failFrom == "" {
		res.failFrom, res.failTo = liqFrom, liqTo
	}
	res.amountSat = good
	return res
}

// Exported type aliases so the graph_watcher can reuse the probe interfaces.
type (
	ProbeLightningClient = probeLightningClient
	ProbeRouterClient    = probeRouterClient
	ProbeQuerier         = probeQuerier
)

// probeSource is a channel to probe from, with the fee limit for its probes.
type probeSource struct {
	chanID      string
	feeRate     int
	feeLimitSat int64
}

// sourceCandidates returns the sources the rebalancer uses for a rebalance of
// target's ar_amt_target sats (rebalancer.SourceFor), not a channel to the
// target, cheapest fee first (then by chan_id), without a fee limit.
func sourceCandidates(channels []db.GuiChannel, target db.GuiChannel) []probeSource {
	var sources []probeSource
	for _, c := range channels {
		if c.RemotePubkey != target.RemotePubkey && rebalancer.SourceFor(c, target.ArAmtTarget) {
			sources = append(sources, probeSource{chanID: c.ChanID, feeRate: int(c.LocalFeeRate)})
		}
	}
	sort.Slice(sources, func(i, j int) bool {
		if sources[i].feeRate != sources[j].feeRate {
			return sources[i].feeRate < sources[j].feeRate
		}
		return sources[i].chanID < sources[j].chanID
	})
	return sources
}

// probeSources returns the sourceCandidates of target with a fee budget above
// zero, each with its fee limit for a refill within targetBudgetPpm.
func probeSources(channels []db.GuiChannel, target db.GuiChannel, targetBudgetPpm, maxFeeRate int) []probeSource {
	var sources []probeSource
	for _, src := range sourceCandidates(channels, target) {
		if src.feeLimitSat = probeFeeLimitSat(target, targetBudgetPpm, src.feeRate, maxFeeRate); src.feeLimitSat > 0 {
			sources = append(sources, src)
		}
	}
	return sources
}

// noPath reports whether a QueryRoutes error means LND found no route.
func noPath(err error) bool { return strings.Contains(err.Error(), "unable to find a path") }

// probeTargets measures the refill price of every auto-rebalance channel the
// peer has not disabled, once per peer, with random-preimage HTLCs. Per target
// it probes up to maxPerTarget sourceCandidates that have a route, with
// AR-MaxFeeRate as the only fee limit so expensive routes show up too; per
// source up to pathsPerSource paths: when a probe fails before the target peer,
// QueryRoutes is asked again without the failing node pair. The refill price is
// the verified route with the largest amount, then the lowest fee ppm; it goes
// to all channels of the peer (no verified route: NULL price, unless an RPC
// error leaves the result open). A verified route is stored as a rebalance
// route only within the target's budget. channels is the rebalancer's channel
// set (active, open, public), sorted by chan_id. Returns new/existing/error
// counts and per-target details.
func probeTargets(ctx context.Context, stub probeLightningClient, routerstub probeRouterClient, q refillProbeQuerier, selfPubkey string, channels []db.GuiChannel, maxFeeRate, maxPerTarget, pathsPerSource int, minAmountSat int64) (int, int, int, []probeTargetDetail, error) {
	probedPubkeys := map[string]bool{}
	totalNew, totalExisting, totalErrors := 0, 0, 0
	targetDetails := []probeTargetDetail{}

	for _, chn := range channels {
		if probedPubkeys[chn.RemotePubkey] || !chn.AutoRebalance || chn.RemoteDisabled {
			continue
		}
		probedPubkeys[chn.RemotePubkey] = true
		lastHop, err := hex.DecodeString(chn.RemotePubkey)
		if err != nil {
			return 0, 0, 0, nil, err
		}
		budget, berr := af.TargetBudgetPpm(ctx, q, chn, time.Now())
		if berr != nil {
			dataLog(fmt.Sprintf("Error reading the NK budget of %s: %s", chn.ChanID, berr))
		}
		sources := sourceCandidates(channels, chn)
		probeLimitSat := max(int64(float64(int64(maxFeeRate)*chn.ArAmtTarget)/1000000), 1)
		d := probeTargetDetail{Pubkey: chn.RemotePubkey, Sources: len(sources)}
		var refillPpm float64
		var refillAmount int64

		for _, src := range sources {
			if d.OutChansTried >= maxPerTarget {
				break
			}
			outChanU, perr := strconv.ParseUint(src.chanID, 10, 64)
			if perr != nil {
				return 0, 0, 0, nil, perr
			}
			var ignored []*lnrpc.NodePair
			tried := false
			for attempt := 0; attempt < max(pathsPerSource, 1); attempt++ {
				response, qerr := stub.QueryRoutes(ctx, &lnrpc.QueryRoutesRequest{
					PubKey:            selfPubkey,
					Amt:               chn.ArAmtTarget,
					OutgoingChanIds:   []uint64{outChanU},
					LastHopPubkey:     lastHop,
					FeeLimit:          &lnrpc.FeeLimit{Limit: &lnrpc.FeeLimit_Fixed{Fixed: probeLimitSat}},
					IgnoredPairs:      ignored,
					UseMissionControl: true,
				})
				noRoute := qerr == nil && (len(response.GetRoutes()) == 0 || len(response.GetRoutes()[0].GetHops()) == 0)
				if qerr != nil && !noPath(qerr) {
					d.RPCErrors++
					if !tried {
						d.OutChansTried++
						tried = true
					}
					break
				}
				if qerr != nil || noRoute {
					if attempt == 0 {
						d.NoRoute++
					}
					break
				}
				if !tried {
					d.OutChansTried++
					tried = true
				}
				hops := response.GetRoutes()[0].GetHops()
				hopPubkeys := make([][]byte, 0, len(hops))
				for _, h := range hops {
					b, e := hex.DecodeString(h.GetPubKey())
					if e != nil {
						return 0, 0, 0, nil, e
					}
					hopPubkeys = append(hopPubkeys, b)
				}
				found := probeWithBinarySearch(ctx, routerstub, src.chanID, hopPubkeys, rebalancer.FinalCltvDelta, chn.ArAmtTarget, probeLimitSat, minAmountSat,
					attemptRecorder(ctx, q, chn.RemotePubkey, src.chanID))
				if found.routeHex == nil || found.amountSat <= 0 {
					d.ProbeFailed++
					if found.failFrom == "" {
						break
					}
					from, ferr := hex.DecodeString(found.failFrom)
					to, terr := hex.DecodeString(found.failTo)
					if ferr != nil || terr != nil {
						break
					}
					ignored = append(ignored, &lnrpc.NodePair{From: from, To: to})
					continue
				}
				d.Verified++
				// A verified route without a fee ppm from the probe charged no fees.
				var routePpm float64
				if found.feePpm != nil {
					routePpm = *found.feePpm
				}
				if found.amountSat > refillAmount || (found.amountSat == refillAmount && routePpm < refillPpm) {
					refillAmount, refillPpm = found.amountSat, routePpm
				}
				if budgetPpm := min(maxFeeRate, budget-src.feeRate); budgetPpm <= 0 || routePpm > float64(budgetPpm) {
					d.OverBudget++
					break
				}
				path := rebalancer.RoutePath(response.GetRoutes()[0])
				routeOutChan := formatChanID(hops[0].GetChanId())
				feePpm := pgtype.Float8{Float64: routePpm, Valid: true}
				probedAt := pgtype.Timestamptz{Time: time.Now(), Valid: true}
				probedAmount := pgtype.Int8{Int64: found.amountSat, Valid: true}
				existing, gerr := q.GetRebalanceRoute(ctx, db.GetRebalanceRouteParams{
					TargetPubkey: chn.RemotePubkey, OutgoingChanID: routeOutChan, Route: path,
				})
				switch {
				case isNoRows(gerr):
					if e := q.InsertRebalanceRoute(ctx, db.InsertRebalanceRouteParams{
						TargetPubkey: chn.RemotePubkey, OutgoingChanID: routeOutChan, Route: path,
						FinalCltvDelta:  rebalancer.FinalCltvDelta,
						RouteHex:        pgtype.Text{String: *found.routeHex, Valid: true},
						LastFeePpm:      feePpm,
						LastProbed:      probedAt,
						ProbedAmountSat: probedAmount,
					}); e != nil {
						return 0, 0, 0, nil, e
					}
					d.New++
				case gerr != nil:
					return 0, 0, 0, nil, gerr
				default:
					if e := q.UpdateRebalanceRouteProbe(ctx, db.UpdateRebalanceRouteProbeParams{
						ID: existing.ID, RouteHex: pgtype.Text{String: *found.routeHex, Valid: true}, LastFeePpm: feePpm,
						LastProbed: probedAt, ProbedAmountSat: probedAmount,
					}); e != nil {
						return 0, 0, 0, nil, e
					}
					d.Existing++
				}
				break
			}
		}

		if refillAmount > 0 || d.RPCErrors == 0 {
			price := db.UpdatePeerRefillPriceParams{RemotePubkey: chn.RemotePubkey, RefillChecked: ts(time.Now())}
			if refillAmount > 0 {
				ppm := int(math.Round(refillPpm))
				d.RefillPpm, d.RefillAmount = &ppm, refillAmount
				price.RefillPpm = pgtype.Int4{Int32: int32(ppm), Valid: true}
				price.RefillAmountSat = pgtype.Int8{Int64: refillAmount, Valid: true}
			}
			if err := q.UpdatePeerRefillPrice(ctx, price); err != nil {
				return 0, 0, 0, nil, err
			}
		}
		d.Errors = d.ProbeFailed + d.RPCErrors
		totalNew += d.New
		totalExisting += d.Existing
		totalErrors += d.Errors
		alias, isNone, err := peerAliasOpt(ctx, q, chn.RemotePubkey)
		if err != nil {
			return 0, 0, 0, nil, err
		}
		if !isNone {
			d.Alias = alias
		}
		targetDetails = append(targetDetails, d)
	}
	return totalNew, totalExisting, totalErrors, targetDetails, nil
}

// isoLayout is the timestamp format used for QR-LastProbe storage.
// parseISO accepts both this format and RFC3339Nano.
const isoLayout = "2006-01-02T15:04:05.000000"

func parseISO(s string) (time.Time, error) {
	for _, l := range []string{
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
	} {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse isoformat %q", s)
}

// ProbeRoutesJob runs the route probe when QR-Enabled is set and enough time has
// elapsed since QR-LastProbe. It measures the refill prices of the auto-rebalance
// channels (probeTargets) and writes a ProbeLog entry (keeping at most 100 records).
func ProbeRoutesJob(ctx context.Context, q refillProbeQuerier, stub probeLightningClient, routerstub probeRouterClient) error {
	enabled, err := settingGateEnabled(ctx, q, "QR-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	updateHours, err := getOrCreateFloat(ctx, q, "QR-UpdateHours", "6")
	if err != nil {
		return err
	}
	maxPerTarget, err := getOrCreateInt(ctx, q, "QR-MaxPerTarget", "5")
	if err != nil {
		return err
	}
	pathsPerSource, err := getOrCreateInt(ctx, q, "QR-PathsPerSource", "3")
	if err != nil {
		return err
	}
	minAmountSat, err := getOrCreateInt(ctx, q, "AR-MinAmount", "2500")
	if err != nil {
		return err
	}

	// Check whether enough time has passed since the last probe run.
	lastProbe := time.Time{}
	if row, e := q.GetLocalSetting(ctx, "QR-LastProbe"); e == nil {
		if t, perr := parseISO(row.Value); perr == nil {
			lastProbe = t
		}
	} else if !isNoRows(e) {
		return e
	}
	if time.Since(lastProbe) < time.Duration(updateHours*float64(time.Hour)) {
		return nil
	}

	probeStart := time.Now()
	channels, err := q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		return err
	}
	sort.SliceStable(channels, func(i, j int) bool { return channels[i].ChanID < channels[j].ChanID })

	// Read AR-MaxFeeRate; use 500 as default when the key is absent.
	maxFeeRate := 500
	if row, e := q.GetLocalSetting(ctx, "AR-MaxFeeRate"); e == nil {
		if row.Value != "" {
			v, perr := strconv.Atoi(row.Value)
			if perr != nil {
				return perr
			}
			maxFeeRate = v
		}
	} else if !isNoRows(e) {
		return e
	}

	selfInfo, err := stub.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	selfPubkey := selfInfo.GetIdentityPubkey()

	totalNew, totalExisting, totalErrors, details, err := probeTargets(ctx, stub, routerstub, q, selfPubkey, channels, maxFeeRate, maxPerTarget, pathsPerSource, int64(minAmountSat))
	if err != nil {
		return err
	}
	durationMs := int(time.Since(probeStart).Milliseconds())

	if err := q.UpsertLocalSetting(ctx, db.UpsertLocalSettingParams{Key: "QR-LastProbe", Value: time.Now().Format(isoLayout)}); err != nil {
		return err
	}
	detailsJSON, err := json.Marshal(details)
	if err != nil {
		return err
	}
	if err := q.InsertProbeLog(ctx, db.InsertProbeLogParams{
		Timestamp:      ts(time.Now()),
		TargetsScanned: int32(len(details)),
		RoutesFound:    int32(totalNew),
		RoutesExisting: int32(totalExisting),
		Errors:         int32(totalErrors),
		DurationMs:     int32(durationMs),
		Details:        detailsJSON,
	}); err != nil {
		return err
	}
	if err := q.DeleteOldProbeLogs(ctx); err != nil {
		return err
	}

	if totalNew != 0 {
		dataLog(fmt.Sprintf("QueryRoutes probe discovered %d new route(s) for %d target(s)", totalNew, len(details)))
	} else {
		dataLog(fmt.Sprintf("QueryRoutes probe completed: %d targets, %d existing routes, %d errors (%dms)", len(details), totalExisting, totalErrors, durationMs))
	}
	return nil
}

// ProbeLoop runs ProbeRoutesJob in its own loop, apart from the Data jobs: a
// probe run takes minutes and would hold them up. Every minute it checks whether
// a run is due (QR-Enabled, QR-UpdateHours); getConn returns the LND connection
// the stubs are built from.
func ProbeLoop(ctx context.Context, q refillProbeQuerier, getConn func() (*grpc.ClientConn, error), sleep func(time.Duration)) {
	for ctx.Err() == nil {
		conn, err := getConn()
		if err == nil {
			err = ProbeRoutesJob(ctx, q, lnrpc.NewLightningClient(conn), routerrpc.NewRouterClient(conn))
		}
		if err != nil && ctx.Err() == nil {
			dataLog(fmt.Sprintf("Error probing routes: %s", err))
		}
		sleep(time.Minute)
	}
}
