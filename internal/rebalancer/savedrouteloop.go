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

// trySavedRoute attempts a single saved-route entry. Returns true if the outer
// loop should stop (payment succeeded). It mutates rebalance, successfulOut/In,
// triedSavedSources, and savedSucceeded. RPC errors are treated as soft failures.
func (e *engine) trySavedRoute(
	ctx context.Context,
	stub rebalLightningClient,
	routerstub rebalRouterClient,
	q rebalancerQuerier,
	rebalance *db.GuiRebalancer,
	sr db.GuiRebalanceroute,
	invoiceResp *lnrpc.AddInvoiceResponse,
	sourceFeeMap map[string]int,
	targetFeeRate, arMaxCost, maxFeeRate int,
	oppCostEnabled bool,
	feeLimitMsat int64,
	timeout time.Duration,
	successfulOut, successfulIn **uint64,
	triedSavedSources map[string]struct{},
	savedSucceeded *bool,
	now func() time.Time,
) bool {
	srLabel := e.alias.label(sr.OutgoingChanID)
	rebalLog(fmt.Sprintf("Trying saved route via %s", srLabel))

	var rebuiltHex *string
	handleErr := func(err error) {
		rebalLog(fmt.Sprintf("BuildRoute failed via %s - %s", srLabel, err))
		if rebuiltHex != nil {
			updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, false, false, now())
		} else {
			markRouteFailure(ctx, q, sr.ID, now())
		}
	}

	hasHex := sr.RouteHex.Valid && sr.RouteHex.String != ""
	var parsedSr *lnrpc.Route
	var hopKeys [][]byte
	var cltvDelta int32

	if hasHex {
		p, err := parseRouteHex(sr.RouteHex.String)
		if err != nil {
			handleErr(err)
			return false
		}
		parsedSr = p
		for _, h := range p.GetHops() {
			b, derr := hex.DecodeString(h.GetPubKey())
			if derr != nil {
				handleErr(derr)
				return false
			}
			hopKeys = append(hopKeys, b)
		}
		hops := p.GetHops()
		if sr.FinalCltvDelta != 0 {
			cltvDelta = sr.FinalCltvDelta
		} else if len(hops) >= 2 {
			cltvDelta = int32(hops[len(hops)-1].GetExpiry()) - int32(hops[len(hops)-2].GetExpiry())
		} else {
			cltvDelta = 144
		}
	} else {
		for _, k := range strings.Split(sr.Route, "-") {
			b, derr := hex.DecodeString(k)
			if derr != nil {
				handleErr(derr)
				return false
			}
			hopKeys = append(hopKeys, b)
		}
		if sr.FinalCltvDelta != 0 {
			cltvDelta = sr.FinalCltvDelta
		} else {
			cltvDelta = 144
		}
	}

	// Cap by the smallest max_htlc_msat on the route, and validate against the failed-edge cache.
	if hasHex {
		capMsat := e.chanInfo.maxAmountOnRouteMsat(ctx, stub, parsedSr)
		if capMsat != 0 && int64(rebalance.Value)*1000 > capMsat {
			rebalLog(fmt.Sprintf("Saved route via %s max_htlc cap is %d sats, less than rebalance value %d - skipping",
				srLabel, capMsat/1000, rebalance.Value))
			return false
		}
		if !e.mc.validateRoute(parsedSr) {
			rebalLog(fmt.Sprintf("Saved route via %s skipped — reuses a cached failed edge", srLabel))
			return false
		}
	}

	outChanID, perr := strconv.ParseUint(sr.OutgoingChanID, 10, 64)
	if perr != nil {
		handleErr(perr)
		return false
	}
	build, berr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
		OutgoingChanId: outChanID,
		AmtMsat:        int64(rebalance.Value) * 1000,
		HopPubkeys:     hopKeys,
		FinalCltvDelta: cltvDelta,
		PaymentAddr:    invoiceResp.GetPaymentAddr(),
	})
	if berr != nil {
		handleErr(berr)
		return false
	}
	rebalLog(fmt.Sprintf("BuildRoute succeeded via %s", srLabel))

	if oppCostEnabled && !rebalance.Manual {
		allowed, totalPpm, budgetPpm := checkOpportunityCost(
			build.GetRoute().GetTotalFeesMsat(), int(rebalance.Value),
			lookupSourceFee(sourceFeeMap, sr.OutgoingChanID), targetFeeRate, arMaxCost, maxFeeRate)
		if !allowed {
			srcFee := lookupSourceFee(sourceFeeMap, sr.OutgoingChanID)
			rebalLog(fmt.Sprintf("BuildRoute via %s rejected: route %d ppm + source %d ppm outbound, max route budget %d ppm (target %d * %d%% - %d source) - skipping",
				srLabel, totalPpm, srcFee, budgetPpm, targetFeeRate, arMaxCost, srcFee))
			return false
		}
	}

	if build.GetRoute().GetTotalFeesMsat() > feeLimitMsat {
		actualPpm := (float64(build.GetRoute().GetTotalFeesMsat()) / (float64(rebalance.Value) * 1000)) * 1000000
		rebalLog(fmt.Sprintf("BuildRoute via %s exceeds fee limit (%d > %d msat, %d ppm) - skipping without penalty",
			srLabel, build.GetRoute().GetTotalFeesMsat(), feeLimitMsat, int(actualPpm)))
		updateRouteFee(ctx, q, sr.ID, actualPpm)
		return false
	}

	routeMsg := build.GetRoute()
	rb, merr := proto.Marshal(routeMsg)
	if merr != nil {
		handleErr(merr)
		return false
	}
	h := hex.EncodeToString(rb)
	rebuiltHex = &h
	triedSavedSources[sr.OutgoingChanID] = struct{}{}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	paymentResp, serr := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{
		PaymentHash: invoiceResp.GetRHash(), Route: routeMsg,
	})
	cancel()
	if serr != nil {
		handleErr(serr)
		return false
	}

	if paymentResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED { // status == 1
		rebalance.Status = 2
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
		updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, true, false, now())
		updateNodeReputations(ctx, q, *rebuiltHex, true, 0, false, now())
		*savedSucceeded = true
		return true
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
	updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, *rebuiltHex, false, false, now())
	updateNodeReputations(ctx, q, *rebuiltHex, false, fsi, failureSet, now())

	// Probe for a smaller amount when the target peer lacks liquidity.
	totalHops := len(routeMsg.GetHops())
	if failureSet && codeNum == 15 && fsi == totalHops-2 {
		probed := probeRouteAmount(ctx, routerstub, hopKeys, sr.OutgoingChanID, cltvDelta, int64(rebalance.Value))
		if probed > 0 {
			rebalLog(fmt.Sprintf("Probe found max %d sats (was %d)", probed, rebalance.Value))
			probeInvoice, aerr := stub.AddInvoice(ctx, &lnrpc.Invoice{Value: probed, Expiry: int64(timeout / time.Second)})
			if aerr != nil {
				handleErr(aerr)
				return false
			}
			probeBuild, pberr := routerstub.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
				OutgoingChanId: outChanID,
				AmtMsat:        probed * 1000,
				HopPubkeys:     hopKeys,
				FinalCltvDelta: cltvDelta,
				PaymentAddr:    probeInvoice.GetPaymentAddr(),
			})
			if pberr != nil {
				handleErr(pberr)
				return false
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
				pcctx, pcancel := context.WithTimeout(ctx, timeout)
				probeResp, prerr := routerstub.SendToRouteV2(pcctx, &routerrpc.SendToRouteRequest{
					PaymentHash: probeInvoice.GetRHash(), Route: probeBuild.GetRoute(),
				})
				pcancel()
				if prerr != nil {
					handleErr(prerr)
					return false
				}
				if probeResp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED {
					rebalance.FeeLimit = pyround.Round(rebalance.FeeLimit*(float64(probed)/float64(rebalance.Value)), 3)
					rebalance.Value = int32(probed)
					rebalance.Status = 2
					rebalance.PaymentHash = textOf(hex.EncodeToString(probeInvoice.GetRHash()))
					feesMsat := probeResp.GetRoute().GetTotalFeesMsat()
					rebalance.FeesPaid = pgtype.Float8{Float64: float64(feesMsat) / 1000, Valid: true}
					respHops := probeResp.GetRoute().GetHops()
					so := respHops[0].GetChanId()
					si := respHops[len(respHops)-1].GetChanId()
					*successfulOut, *successfulIn = &so, &si
					pb, _ := proto.Marshal(probeBuild.GetRoute())
					ph := hex.EncodeToString(pb)
					updateRoute(ctx, q, rebalance.LastHopPubkey, sr.OutgoingChanID, ph, true, true, now())
					updateNodeReputations(ctx, q, ph, true, 0, false, now())
					rebalLog(fmt.Sprintf("Probe payment succeeded: %d sats via %s", probed, srLabel))
					*savedSucceeded = true
					return true
				}
			}
		}
	}
	return false
}
