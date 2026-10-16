package rebalancer

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// savedRouteResult is how a route attempt ended. outcome is one of success,
// success_smaller (paid with the amount a probe found), failed, timeout (no
// result within AR-AttemptTimeout), skip_max_htlc, skip_failed_edge,
// skip_slow (a banned slow node pair), skip_htlc (the HTLC brake), skip_cost,
// build_failed, error; code and hop are the failure code and failure source
// index of a failed payment (-1: none); durationMs is the time until the result
// of a sent attempt (-1: not sent).
type savedRouteResult struct {
	outcome    string
	code, hop  int
	durationMs int
}

// notSent is the result of an attempt that ended before anything was sent.
func notSent(outcome string) savedRouteResult {
	return savedRouteResult{outcome: outcome, code: -1, hop: -1, durationMs: -1}
}

func (r savedRouteResult) succeeded() bool {
	return r.outcome == "success" || r.outcome == "success_smaller"
}

// trySavedRoute attempts a single saved-route entry; the outer loop stops when
// the result succeeded. It mutates rebalance, successfulOut/In,
// triedSavedSources, and savedSucceeded. RPC errors are treated as soft failures.
func (e *engine) trySavedRoute(
	ctx context.Context,
	stub rebalLightningClient,
	routerstub rebalRouterClient,
	q rebalancerQuerier,
	rebalance *db.GuiRebalancer,
	sr db.GuiRebalanceroute,
	inv *rebalInvoice,
	sourceFeeMap map[string]int,
	targetFeeRate, arMaxCost, maxFeeRate int,
	oppCostEnabled bool,
	feeLimitMsat int64,
	timeout time.Duration,
	send sendSettings,
	successfulOut, successfulIn **uint64,
	triedSavedSources map[string]struct{},
	savedSucceeded *bool,
	now func() time.Time,
) savedRouteResult {
	srLabel := e.alias.label(sr.OutgoingChanID)
	rebalLog(fmt.Sprintf("Trying saved route via %s", srLabel))

	var rebuiltHex *string
	handleErr := func(err error) {
		rebalLog(fmt.Sprintf("BuildRoute failed via %s - %s", srLabel, err))
		if rebuiltHex != nil {
			updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, false, false, -1, now())
		} else if sr.ID != 0 {
			markRouteFailure(ctx, q, sr.ID, now())
		}
	}

	hasHex := sr.RouteHex.Valid && sr.RouteHex.String != ""
	var parsedSr *lnrpc.Route
	var hopKeys [][]byte
	const cltvDelta = FinalCltvDelta

	if hasHex {
		p, err := parseRouteHex(sr.RouteHex.String)
		if err != nil {
			handleErr(err)
			return notSent("error")
		}
		parsedSr = p
		for _, h := range p.GetHops() {
			b, derr := hex.DecodeString(h.GetPubKey())
			if derr != nil {
				handleErr(derr)
				return notSent("error")
			}
			hopKeys = append(hopKeys, b)
		}
	} else {
		for _, k := range strings.Split(sr.Route, "-") {
			b, derr := hex.DecodeString(k)
			if derr != nil {
				handleErr(derr)
				return notSent("error")
			}
			hopKeys = append(hopKeys, b)
		}
	}

	// Cap by the smallest max_htlc_msat on the route, and validate against the
	// failed-edge cache: before BuildRoute for a stored route, after it otherwise.
	checkRoute := func(r *lnrpc.Route) (savedRouteResult, bool) {
		capMsat := e.chanInfo.maxAmountOnRouteMsat(ctx, stub, r)
		if capMsat != 0 && int64(rebalance.Value)*1000 > capMsat {
			rebalLog(fmt.Sprintf("Saved route via %s max_htlc cap is %d sats, less than rebalance value %d - skipping",
				srLabel, capMsat/1000, rebalance.Value))
			return notSent("skip_max_htlc"), false
		}
		if !e.mc.validateRoute(r) {
			rebalLog(fmt.Sprintf("Saved route via %s skipped — reuses a cached failed edge", srLabel))
			return notSent("skip_failed_edge"), false
		}
		if pair, slow := e.stats.bannedPair(routePath(r), now()); slow {
			rebalLog(fmt.Sprintf("Saved route via %s skipped — slow node pair %s", srLabel, pair))
			return notSent("skip_slow"), false
		}
		return savedRouteResult{}, true
	}
	if hasHex {
		if res, ok := checkRoute(parsedSr); !ok {
			return res
		}
	}

	outChanID, perr := strconv.ParseUint(sr.OutgoingChanID, 10, 64)
	if perr != nil {
		handleErr(perr)
		return notSent("error")
	}
	build, berr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
		OutgoingChanId: outChanID,
		AmtMsat:        int64(rebalance.Value) * 1000,
		HopPubkeys:     hopKeys,
		FinalCltvDelta: cltvDelta,
		PaymentAddr:    inv.resp.GetPaymentAddr(),
	})
	if berr != nil {
		handleErr(berr)
		return notSent("build_failed")
	}
	rebalLog(fmt.Sprintf("BuildRoute succeeded via %s", srLabel))
	if !hasHex {
		if res, ok := checkRoute(build.GetRoute()); !ok {
			return res
		}
	}

	if oppCostEnabled && !rebalance.Manual {
		allowed, totalPpm, budgetPpm := checkOpportunityCost(
			build.GetRoute().GetTotalFeesMsat(), int(rebalance.Value),
			lookupSourceFee(sourceFeeMap, sr.OutgoingChanID), targetFeeRate, arMaxCost, maxFeeRate)
		if !allowed {
			srcFee := lookupSourceFee(sourceFeeMap, sr.OutgoingChanID)
			rebalLog(fmt.Sprintf("BuildRoute via %s rejected: route %d ppm + source %d ppm outbound, max route budget %d ppm (target %d * %d%% - %d source) - skipping",
				srLabel, totalPpm, srcFee, budgetPpm, targetFeeRate, arMaxCost, srcFee))
			return notSent("skip_cost")
		}
	}

	if build.GetRoute().GetTotalFeesMsat() > feeLimitMsat {
		actualPpm := (float64(build.GetRoute().GetTotalFeesMsat()) / (float64(rebalance.Value) * 1000)) * 1000000
		rebalLog(fmt.Sprintf("BuildRoute via %s exceeds fee limit (%d > %d msat, %d ppm) - skipping without penalty",
			srLabel, build.GetRoute().GetTotalFeesMsat(), feeLimitMsat, int(actualPpm)))
		updateRouteFee(ctx, q, sr.ID, actualPpm)
		return notSent("skip_cost")
	}

	routeMsg := build.GetRoute()
	rb, merr := proto.Marshal(routeMsg)
	if merr != nil {
		handleErr(merr)
		return notSent("error")
	}
	h := hex.EncodeToString(rb)
	rebuiltHex = &h
	triedSavedSources[sr.OutgoingChanID] = struct{}{}

	sent := e.sendRoute(ctx, stub, stub, routerstub, inv, routeMsg, send)
	switch {
	case sent.skipped:
		return notSent("skip_htlc")
	case sent.err != nil:
		handleErr(sent.err)
		return savedRouteResult{outcome: "error", code: -1, hop: -1, durationMs: sent.durationMs}
	case sent.timedOut:
		rebalLog(fmt.Sprintf("Saved route via %s: no result within %s, going on with a new invoice", srLabel, send.attemptTimeout))
		return savedRouteResult{outcome: "timeout", code: -1, hop: -1, durationMs: sent.durationMs}
	}
	paymentResp := sent.resp

	if paymentResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED { // status == 1
		rebalance.Status = 2
		rebalance.PaymentHash = textOf(hex.EncodeToString(inv.resp.GetRHash()))
		feesMsat := paymentResp.GetRoute().GetTotalFeesMsat()
		if feesMsat != 0 {
			rebalance.FeesPaid = pgtype.Float8{Float64: float64(feesMsat) / 1000, Valid: true}
		} else {
			rebalance.FeesPaid = pgtype.Float8{Float64: 0, Valid: true}
		}
		respHops := paymentResp.GetRoute().GetHops()
		if len(respHops) > 0 {
			so := respHops[0].GetChanId()
			si := respHops[len(respHops)-1].GetChanId()
			*successfulOut, *successfulIn = &so, &si
			rebalLog(fmt.Sprintf("Saved route succeeded via %s - hash: %s", srLabel, textVal(rebalance.PaymentHash)))
			rebalLog(fmt.Sprintf("Used outgoing chan_id: %d, incoming chan_id: %d", so, si))
		} else {
			*successfulOut, *successfulIn = nil, nil
		}
		updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, true, false, -1, now())
		e.routePaid(routeMsg, sr.OutgoingChanID, rebalance.LastHopPubkey)
		*savedSucceeded = true
		return savedRouteResult{outcome: "success", code: -1, hop: -1, durationMs: sent.durationMs}
	}

	// Handle failure.
	failure := paymentResp.GetFailure()
	failureSet := failure != nil
	var codeNum, fsi int
	if failureSet {
		codeNum = int(failure.GetCode())
		fsi = int(failure.GetFailureSourceIndex())
		rebalLog(fmt.Sprintf("Saved route failed via %s - code: %s - failure_hop: %d",
			srLabel, failureCodeName(codeNum), fsi))
	} else {
		rebalLog(fmt.Sprintf("Saved route failed via %s - code: no-details - failure_hop: None", srLabel))
	}
	e.mc.recordRouteFailure(routeMsg, failure)
	failHop := -1
	if failureSet {
		failHop = fsi
	}
	updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, false, false, failHop, now())
	failed := savedRouteResult{outcome: "failed", code: -1, hop: -1, durationMs: sent.durationMs}
	if failureSet {
		failed.code, failed.hop = codeNum, fsi
	}

	// Probe for a smaller amount when the target peer lacks liquidity.
	totalHops := len(routeMsg.GetHops())
	if failureSet && codeNum == 15 && fsi == totalHops-2 {
		probed := probeRouteAmount(ctx, routerstub, hopKeys, sr.OutgoingChanID, cltvDelta, int64(rebalance.Value))
		if probed > 0 {
			rebalLog(fmt.Sprintf("Probe found max %d sats (was %d)", probed, rebalance.Value))
			probeInvoice, aerr := newRebalInvoice(ctx, stub, probed, timeout)
			if aerr != nil {
				handleErr(aerr)
				return failed
			}
			probeBuild, pberr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
				OutgoingChanId: outChanID,
				AmtMsat:        probed * 1000,
				HopPubkeys:     hopKeys,
				FinalCltvDelta: cltvDelta,
				PaymentAddr:    probeInvoice.resp.GetPaymentAddr(),
			})
			if pberr != nil {
				handleErr(pberr)
				return failed
			}
			scaledFeeLimit := feeLimitMsat * probed / int64(rebalance.Value)
			oppOk := true
			if oppCostEnabled && !rebalance.Manual {
				allowed, oppTotal, oppBudget := checkOpportunityCost(
					probeBuild.GetRoute().GetTotalFeesMsat(), int(probed),
					lookupSourceFee(sourceFeeMap, sr.OutgoingChanID), targetFeeRate, arMaxCost, maxFeeRate)
				oppOk = allowed
				if !allowed {
					rebalLog(fmt.Sprintf("Probe via %s rejected: route %d ppm exceeds budget %d ppm after opportunity cost",
						srLabel, oppTotal, oppBudget))
				}
			}
			if oppOk && probeBuild.GetRoute().GetTotalFeesMsat() <= scaledFeeLimit {
				psent := e.sendRoute(ctx, stub, stub, routerstub, probeInvoice, probeBuild.GetRoute(), send)
				if psent.err != nil {
					handleErr(psent.err)
					return failed
				}
				if probeResp := psent.resp; !psent.skipped && !psent.timedOut && probeResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED {
					rebalance.FeeLimit = pyround.Round(rebalance.FeeLimit*(float64(probed)/float64(rebalance.Value)), 3)
					rebalance.Value = int32(probed)
					rebalance.Status = 2
					rebalance.PaymentHash = textOf(hex.EncodeToString(probeInvoice.resp.GetRHash()))
					feesMsat := probeResp.GetRoute().GetTotalFeesMsat()
					rebalance.FeesPaid = pgtype.Float8{Float64: float64(feesMsat) / 1000, Valid: true}
					respHops := probeResp.GetRoute().GetHops()
					so := respHops[0].GetChanId()
					si := respHops[len(respHops)-1].GetChanId()
					*successfulOut, *successfulIn = &so, &si
					pb, _ := proto.Marshal(probeBuild.GetRoute())
					ph := hex.EncodeToString(pb)
					updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, ph, true, true, -1, now())
					e.routePaid(probeBuild.GetRoute(), sr.OutgoingChanID, rebalance.LastHopPubkey)
					rebalLog(fmt.Sprintf("Probe payment succeeded: %d sats via %s", probed, srLabel))
					*savedSucceeded = true
					return savedRouteResult{outcome: "success_smaller", code: -1, hop: -1, durationMs: psent.durationMs}
				}
			}
		}
	}
	return failed
}
