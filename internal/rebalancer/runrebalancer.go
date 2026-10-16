package rebalancer

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// RapidFire: after a paid rebalance the next one to the same target goes out
// with the same amount (at most what the target still needs); after a failed
// one with half the amount, down to AR-MinAmount sats.
const rapidFireDec = 2.0

// minAmount reads AR-MinAmount (default 2500 sats), the smallest RapidFire step.
func minAmount(ctx context.Context, q settingsQuerier) int64 {
	v, err := getLocalSettingInt(ctx, q, "AR-MinAmount", 2500)
	if err != nil || v < 1 {
		return 2500
	}
	return int64(v)
}

// rebalancerQuerier bundles all DB access required by runRebalancer.
type rebalancerQuerier interface {
	sourceQuerier
	savedRoutesQuerier
	comboQuerier
	attemptLogQuerier
	purgeQuerier
	sourceMapsQuerier
	targetInfoQuerier
	allowedSourcesQuerier
	remainingDrainQuerier
	updateChannelsQuerier
	aliasQuerier
	statsQuerier
	ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error)
	InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error)
	UpdateRebalancerRecord(ctx context.Context, arg db.UpdateRebalancerRecordParams) error
}

// rebalLightningClient bundles all LightningClient RPCs used by runRebalancer.
type rebalLightningClient interface {
	sourceLightningClient
	chanInfoClient
	listChannelsClient
}

// rebalRouterClient bundles all RouterClient RPCs used by runRebalancer.
type rebalRouterClient interface {
	sourceRouterClient
	SendPaymentV2(ctx context.Context, in *routerrpc.SendPaymentRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.Payment], error)
}

// saveRecord persists the rebalancer record to the DB. Errors are logged and swallowed.
func saveRecord(ctx context.Context, q rebalancerQuerier, r *db.GuiRebalancer) {
	if err := q.UpdateRebalancerRecord(ctx, db.UpdateRebalancerRecordParams{
		ID: r.ID, Value: r.Value, FeeLimit: r.FeeLimit, OutgoingChanIds: r.OutgoingChanIds,
		LastHopPubkey: r.LastHopPubkey, TargetAlias: r.TargetAlias, Duration: r.Duration,
		Start: r.Start, Stop: r.Stop, Status: r.Status, PaymentHash: r.PaymentHash,
		Manual: r.Manual, FeesPaid: r.FeesPaid,
	}); err != nil {
		rebalLog(fmt.Sprintf("Error saving database record: %s", err))
	}
}

// countInboundCans counts channels matching: remote_pubkey == lastHop, auto_rebalance,
// inbound_can >= 1, not remote_disabled.
func countInboundCans(annotated []annotatedChannel, lastHop string) int {
	n := 0
	for _, a := range annotated {
		c := a.ch
		if c.RemotePubkey == lastHop && c.AutoRebalance && a.inboundCan >= 1 && !c.RemoteDisabled {
			n++
		}
	}
	return n
}

// runRebalancer executes a single rebalance attempt and returns the next
// (already inserted) RapidFire rebalance, or nil. gRPC channels are managed
// by the caller and reused across attempts.
func (e *engine) runRebalancer(
	ctx context.Context,
	stub rebalLightningClient,
	routerstub rebalRouterClient,
	q rebalancerQuerier,
	rebalance *db.GuiRebalancer,
	worker string,
	now func() time.Time,
) *db.GuiRebalancer {
	allowMultishards, err := checkAndSetAllowMultishards(ctx, q)
	if err != nil {
		rebalLog(fmt.Sprintf("Error running rebalance attempt: %s", err))
		return nil
	}
	var maxParts uint32 // 0 = unlimited; 1 when MPP is disabled
	if !allowMultishards {
		maxParts = 1
	}

	channels, err := q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error running rebalance attempt: %s", err))
		return nil
	}
	// Initial annotation using integer subtraction of rebalance.value.
	initialAnnotated := annotateChannels(channels, float64(rebalance.Value), true)
	outboundCans := e.getOutCans(initialAnnotated, rebalance.LastHopPubkey)
	if len(outboundCans) == 0 && !rebalance.Manual {
		rebalLog("No outbound_cans")
		rebalance.Status = 406
		rebalance.Start = tsNow(now)
		rebalance.Stop = tsNow(now)
		saveRecord(ctx, q, rebalance)
		return nil
	} else if formatChanIDList(outboundCans) != rebalance.OutgoingChanIds && !rebalance.Manual {
		rebalance.OutgoingChanIds = formatChanIDList(outboundCans)
	}
	rebalance.Start = tsNow(now)

	var successfulOut, successfulIn *uint64

	innerErr := func() error {
		chanIDs := parseChanIDs(rebalance.OutgoingChanIds)
		chanIDs = e.sortChannelsByHtlc(ctx, stub, chanIDs)
		timeout := time.Duration(rebalance.Duration) * 60 * time.Second
		send := readSendSettings(ctx, q, timeout)
		if send.htlcSoftLimit > 0 {
			free := chanIDs[:0:0]
			for _, cid := range chanIDs {
				id, _ := strconv.ParseUint(cid, 10, 64)
				if n := e.htlcs.count(ctx, stub, id, now()); n >= send.htlcSoftLimit {
					rebalLog(fmt.Sprintf("HTLC brake: source %s has %d pending HTLCs", e.alias.label(cid), n))
					continue
				}
				free = append(free, cid)
			}
			chanIDs = free
		}
		if len(chanIDs) == 0 && !rebalance.Manual {
			rebalLog("No outbound channel without the HTLC brake")
			rebalance.Status = 406
			rebalance.Start = tsNow(now)
			rebalance.Stop = tsNow(now)
			saveRecord(ctx, q, rebalance)
			return nil
		}

		if err := e.alias.ensure(ctx, q); err != nil {
			return err
		}
		sourceFeeMap, sourceDiffMap, err := getSourceMaps(ctx, q, chanIDs)
		if err != nil {
			return err
		}
		targetFeeRate, arMaxCost, oppCostEnabled, err := getTargetInfo(ctx, q, rebalance.LastHopPubkey, now())
		if err != nil {
			return err
		}
		maxFeeRate, err := getMaxFeeRate(ctx, q)
		if err != nil {
			return err
		}

		// Opportunity-cost pre-filter: exclude sources where the budget would be zero or negative.
		if oppCostEnabled && !rebalance.Manual {
			originalCount := len(chanIDs)
			filtered := make([]string, 0, len(chanIDs))
			for _, cid := range chanIDs {
				srcFee := lookupSourceFee(sourceFeeMap, cid)
				srcDiff := sourceDiffMap[cid]
				if targetFeeRate < srcFee+srcDiff {
					rebalLog(fmt.Sprintf("Excluding source %s (target fee %d < source %d + ppm_diff %d for %s)",
						e.alias.label(cid), targetFeeRate, srcFee, srcDiff, rebalance.TargetAlias))
					continue
				}
				maxRoutePpm := int(float64(targetFeeRate)*(float64(arMaxCost)/100)) - srcFee
				if maxRoutePpm > 0 {
					filtered = append(filtered, cid)
				} else {
					rebalLog(fmt.Sprintf("Excluding source %s (outbound fee %d ppm eats entire budget for target %s)",
						e.alias.label(cid), srcFee, rebalance.TargetAlias))
				}
			}
			chanIDs = filtered
			if len(chanIDs) < originalCount {
				rebalLog(fmt.Sprintf("Opportunity cost filter: %d -> %d outbound channels", originalCount, len(chanIDs)))
			}
			if len(chanIDs) == 0 {
				rebalLog("No outbound channels left after opportunity cost filter")
				rebalance.Status = 406
				rebalance.Start = tsNow(now)
				rebalance.Stop = tsNow(now)
				saveRecord(ctx, q, rebalance)
				return nil
			}
		}

		inv, err := newRebalInvoice(ctx, stub, int64(rebalance.Value), timeout)
		if err != nil {
			return err
		}
		rebalance.PaymentHash = textOf(hex.EncodeToString(inv.resp.GetRHash()))
		rebalLog(fmt.Sprintf("%s starting rebalance for %s %s for %d sats and duration %d, using %d outbound channels",
			worker, rebalance.TargetAlias, rebalance.LastHopPubkey, rebalance.Value, rebalance.Duration, len(chanIDs)))

		e.stats.refresh(ctx, q, now())
		feeLimitMsat := int64(rebalance.FeeLimit * 1000)
		savedSucceeded := false
		triedSavedSources := make(map[string]struct{})

		// One pool of saved and combined routes, tried by success chance.
		var tryOrder []rankedRoute
		if savedRoutesEnabled(ctx, q) {
			purgeStaleRoutes(ctx, q, now())
			pool := getSavedRoutes(ctx, q, rebalance.LastHopPubkey, chanIDs, int64(rebalance.Value), now())
			feeLimitPpm := int(float64(feeLimitMsat) / float64(rebalance.Value) * 1000)
			budget := func(chanID string) int {
				if !oppCostEnabled || rebalance.Manual {
					return feeLimitPpm
				}
				return min(int(float64(targetFeeRate)*(float64(arMaxCost)/100))-lookupSourceFee(sourceFeeMap, chanID), maxFeeRate, feeLimitPpm)
			}
			e.combos.refresh(ctx, q, e.stats.routeProb, now())
			pool = append(pool, e.combos.candidates(rebalance.LastHopPubkey, chanIDs, budget)...)
			var sel selection
			tryOrder, sel = selectRoutes(pool, e.stats, e.draw, getRouteLimit(ctx, q)+combinedLimit, now())
			rebalLog(fmt.Sprintf("Route pool %d: %d to try (%s); left out %d unlikely, %d slow, %d too small, %d not drawn",
				len(pool), len(tryOrder), countLists(tryOrder), sel.weak, sel.slow, sel.small, sel.notDrawn))
		} else {
			rebalLog("Saved routes disabled")
		}

		for _, rr := range tryOrder {
			value := int64(rebalance.Value)
			res := e.trySavedRoute(ctx, stub, routerstub, q, rebalance, rr.row, inv, sourceFeeMap,
				targetFeeRate, arMaxCost, maxFeeRate, oppCostEnabled, feeLimitMsat, timeout, send,
				&successfulOut, &successfulIn, triedSavedSources, &savedSucceeded, now)
			e.logRouteAttempt(ctx, q, rebalance.ID, rr, value, res, now())
			if res.succeeded() {
				break
			}
		}

		perSourceEnabled := false
		if oppCostEnabled && !rebalance.Manual {
			ps, err := getPerSourceEnabled(ctx, q)
			if err != nil {
				return err
			}
			perSourceEnabled = ps
		}

		if perSourceEnabled && !savedSucceeded {
			rebalLog(fmt.Sprintf("Per-source iteration enabled, target budget %d ppm", int(float64(targetFeeRate)*float64(arMaxCost)/100)))
			allowedSources, hasAllowed, err := getAllowedSourcesForTarget(ctx, q, rebalance.LastHopPubkey)
			if err != nil {
				return err
			}
			ordered := make([]string, len(chanIDs))
			copy(ordered, chanIDs)
			// Stable sort by outbound fee ascending.
			stableSortByFee(ordered, sourceFeeMap)
			pruned := make([]string, 0, len(ordered))
			braked := 0
			for _, c := range ordered {
				if _, tried := triedSavedSources[c]; tried {
					continue
				}
				if hasAllowed {
					if _, ok := allowedSources[c]; !ok {
						continue
					}
				}
				if e.brake.blocked(c, rebalance.LastHopPubkey) {
					braked++
					continue
				}
				pruned = append(pruned, c)
			}
			if braked > 0 {
				rebalLog(fmt.Sprintf("Per-source: %d sources paused after recent misses, %d to try", braked, len(pruned)))
			}
			for _, sourceChan := range pruned {
				res, err := e.trySingleSource(ctx, stub, routerstub, q, *rebalance, sourceChan, sourceFeeMap,
					targetFeeRate, arMaxCost, maxFeeRate, inv, feeLimitMsat, timeout, send, now())
				if err != nil {
					return err
				}
				if res != nil {
					rebalance.Status = 2
					rebalance.FeesPaid = pgtype.Float8{Float64: res.feesPaid, Valid: true}
					rebalance.PaymentHash = textOf(res.paymentHash)
					if res.scaledFeeLimit != nil {
						rebalance.FeeLimit = *res.scaledFeeLimit
					}
					if res.value != int64(rebalance.Value) {
						rebalance.Value = int32(res.value)
					}
					so, si := res.successfulOut, res.successfulIn
					successfulOut, successfulIn = &so, &si
					rebalLog(fmt.Sprintf("Per-source succeeded via %s - hash: %s", e.alias.label(sourceChan), textVal(rebalance.PaymentHash)))
					rebalLog(fmt.Sprintf("Used outgoing chan_id: %d, incoming chan_id: %d", so, si))
					break
				}
			}
			if rebalance.Status != 2 {
				rebalLog(fmt.Sprintf("Per-source iteration: no route succeeded for %s", rebalance.TargetAlias))
			}
		} else if !savedSucceeded {
			if len(tryOrder) > 0 {
				rebalLog("Falling back to automatic routing")
			}
			rebalLog(fmt.Sprintf("SendPaymentV2: Sending to LND with outgoing_chan_ids=%s", formatChanIDList(chanIDs)))
			if err := e.sendPaymentV2(ctx, routerstub, q, rebalance, chanIDs, inv, timeout, maxParts, &successfulOut, &successfulIn, now); err != nil {
				return err
			}
		}
		return nil
	}()

	if innerErr != nil {
		if status.Code(innerErr) == codes.DeadlineExceeded {
			rebalance.Status = 408
		} else {
			rebalance.Status = 400
			rebalLog(fmt.Sprintf("Error while sending payment: %s", innerErr))
		}
	}
	// A rebalance that ends without a result found no route (FAILURE_REASON_NO_ROUTE):
	// pending (0) or, for a RapidFire follow-up, in flight (1) since it was queued.
	if rebalance.Status < 2 {
		rebalance.Status = 4
	}

	// Finalise the record regardless of outcome.
	rebalance.Stop = tsNow(now)
	saveRecord(ctx, q, rebalance)
	rebalLog(fmt.Sprintf("%s completed payment attempts for %s: %s", worker, rebalance.TargetAlias, textVal(rebalance.PaymentHash)))
	originalAlias := rebalance.TargetAlias

	if rebalance.Status == 2 {
		if successfulIn != nil && successfulOut != nil {
			updateChannelBalances(ctx, q, stub, *successfulIn, *successfulOut)
		}
		// Re-read the channels: the ones loaded at the start still hold the balances
		// from before this rebalance moved liquidity.
		fresh, ferr := q.ListActiveOpenPublicChannels(ctx)
		if ferr != nil {
			rebalLog(fmt.Sprintf("Error running rebalance attempt: %s", ferr))
			return nil
		}
		annotated := annotateChannels(fresh, float64(rebalance.Value), false)
		inboundLen := countInboundCans(annotated, rebalance.LastHopPubkey)
		rfOut := e.getOutCans(annotated, rebalance.LastHopPubkey)
		drain, derr := remainingDrain(ctx, q, rebalance.LastHopPubkey)
		if derr != nil {
			rebalLog(fmt.Sprintf("Error running rebalance attempt: %s", derr))
			return nil
		}
		proposed := int64(rebalance.Value)
		var nextValue int64
		if drain > 0 {
			nextValue = proposed
			if drain < nextValue {
				nextValue = drain
			}
		}
		if inboundLen > 0 && len(rfOut) > 0 && nextValue >= 1000 {
			scale := float64(nextValue) / float64(rebalance.Value)
			next := e.insertNextRebalance(ctx, q, int32(nextValue), pyround.Round(rebalance.FeeLimit*scale, 3),
				formatChanIDList(rfOut), rebalance.LastHopPubkey, originalAlias, now)
			capNote := ""
			if nextValue < proposed {
				capNote = fmt.Sprintf(" (capped to remaining drain %d)", drain)
			}
			rebalLog(fmt.Sprintf("RapidFire for %s again with %d (was %d)%s", next.TargetAlias, next.Value, rebalance.Value, capNote))
			return next
		}
		if drain < 1000 {
			rebalLog(fmt.Sprintf("RapidFire skipped for %s — channel already at/near ar_in_target (remaining drain %d sats)", originalAlias, drain))
		}
		return nil
	} else if nextValue := float64(rebalance.Value) / rapidFireDec; rebalance.Status > 2 && rebalance.Status != 406 && int64(nextValue) >= minAmount(ctx, q) {
		// 406 (no usable source) does not depend on the amount, a smaller retry cannot help.
		inboundLen := countInboundCans(initialAnnotated, rebalance.LastHopPubkey)
		if inboundLen > 0 && len(outboundCans) > 0 {
			next := e.insertNextRebalance(ctx, q, int32(nextValue),
				pyround.Round(rebalance.FeeLimit/(float64(rebalance.Value)/nextValue), 3),
				formatChanIDList(outboundCans), rebalance.LastHopPubkey, originalAlias, now)
			rebalLog(fmt.Sprintf("RapidFire decrease for %s from %d to %d", next.TargetAlias, rebalance.Value, next.Value))
			return next
		}
		return nil
	}
	return nil
}

// insertNextRebalance inserts and returns a new RapidFire follow-up rebalance record.
func (e *engine) insertNextRebalance(ctx context.Context, q rebalancerQuerier, value int32, feeLimit float64,
	outgoingChanIDs, lastHopPubkey, targetAlias string, now func() time.Time) *db.GuiRebalancer {
	req := tsNow(now)
	id, err := q.InsertRebalancerRecord(ctx, db.InsertRebalancerRecordParams{
		Requested: req, Value: value, FeeLimit: feeLimit, OutgoingChanIds: outgoingChanIDs,
		LastHopPubkey: lastHopPubkey, TargetAlias: targetAlias, Duration: 1, Status: 1,
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error saving database record: %s", err))
	}
	return &db.GuiRebalancer{
		ID: id, Requested: req, Value: value, FeeLimit: feeLimit, OutgoingChanIds: outgoingChanIDs,
		LastHopPubkey: lastHopPubkey, TargetAlias: targetAlias, Duration: 1, Status: 1,
	}
}

// tsNow returns the current time as a pgtype.Timestamptz.
func tsNow(now func() time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: now(), Valid: true}
}

// textVal returns the string value of a pgtype.Text, or "" for NULL.
func textVal(t pgtype.Text) string {
	if t.Valid {
		return t.String
	}
	return ""
}

// stableSortByFee sorts chan_ids stably by their value in feeMap, ascending.
func stableSortByFee(ids []string, feeMap map[string]int) {
	// Insertion sort (stable, efficient for small slices).
	for i := 1; i < len(ids); i++ {
		cur := ids[i]
		curFee := feeMap[cur]
		j := i - 1
		for j >= 0 && feeMap[ids[j]] > curFee {
			ids[j+1] = ids[j]
			j--
		}
		ids[j+1] = cur
	}
}

// sendPaymentV2 sends a payment via LND's SendPaymentV2 streaming RPC and
// updates the rebalance record and route/reputation tables based on the outcome.
func (e *engine) sendPaymentV2(
	ctx context.Context,
	routerstub rebalRouterClient,
	q rebalancerQuerier,
	rebalance *db.GuiRebalancer,
	chanIDs []string,
	inv *rebalInvoice,
	timeout time.Duration,
	maxParts uint32,
	successfulOut, successfulIn **uint64,
	now func() time.Time,
) error {
	lastHopBytes, err := hex.DecodeString(rebalance.LastHopPubkey)
	if err != nil {
		return err
	}
	outChanIDs := make([]uint64, 0, len(chanIDs))
	for _, c := range chanIDs {
		v, perr := strconv.ParseUint(c, 10, 64)
		if perr != nil {
			return perr
		}
		outChanIDs = append(outChanIDs, v)
	}
	sctx, scancel := context.WithTimeout(ctx, timeout+60*time.Second)
	defer scancel()
	stream, err := routerstub.SendPaymentV2(sctx, &routerrpc.SendPaymentRequest{
		PaymentRequest:   inv.resp.GetPaymentRequest(),
		FeeLimitMsat:     int64(rebalance.FeeLimit * 1000),
		OutgoingChanIds:  outChanIDs,
		LastHopPubkey:    lastHopBytes,
		TimeoutSeconds:   int32(timeout/time.Second) - 5,
		AllowSelfPayment: true,
		MaxParts:         maxParts,
	})
	if err != nil {
		return err
	}
	for {
		pay, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
		if s := pay.GetStatus(); s == lnrpc.Payment_SUCCEEDED || s == lnrpc.Payment_FAILED {
			e.logPaymentAttempts(ctx, q, rebalance, pay, now())
		}
		switch pay.GetStatus() {
		case lnrpc.Payment_IN_FLIGHT: // status == 1
			if rebalance.Status == 0 {
				rebalance.PaymentHash = textOf(pay.GetPaymentHash())
				rebalance.Status = 1
				saveRecord(ctx, q, rebalance)
			}
		case lnrpc.Payment_SUCCEEDED: // status == 2
			rebalance.Status = 2
			feesMsat := pay.GetFeeMsat()
			if feesMsat == 0 && len(pay.GetHtlcs()) > 0 {
				feesMsat = pay.GetHtlcs()[0].GetRoute().GetTotalFeesMsat()
			}
			if feesMsat != 0 {
				rebalance.FeesPaid = pgtype.Float8{Float64: float64(feesMsat) / 1000, Valid: true}
			} else {
				rebalance.FeesPaid = pgtype.Float8{Float64: 0, Valid: true}
			}
			htlc0 := pay.GetHtlcs()[0]
			hops := htlc0.GetRoute().GetHops()
			so := hops[0].GetChanId()
			si := hops[len(hops)-1].GetChanId()
			*successfulOut, *successfulIn = &so, &si
			rebalLog(fmt.Sprintf("Automatic route (SendPaymentV2) succeeded - hash: %s", textVal(rebalance.PaymentHash)))
			rebalLog(fmt.Sprintf("LND selected outgoing chan_id: %d from provided list: %s", so, formatChanIDList(chanIDs)))
			b, _ := proto.Marshal(htlc0.GetRoute())
			routeHex := hex.EncodeToString(b)
			outChan := strconv.FormatUint(so, 10)
			updateRoute(ctx, q, rebalance.LastHopPubkey, outChan, routeHex, true, false, -1, now())
			updateNodeReputations(ctx, q, routeHex, true, 0, false, now())
			e.routePaid(htlc0.GetRoute(), outChan, rebalance.LastHopPubkey)
		case lnrpc.Payment_FAILED: // status == 3
			if len(pay.GetHtlcs()) > 0 {
				htlc0 := pay.GetHtlcs()[0]
				b, _ := proto.Marshal(htlc0.GetRoute())
				routeHex := hex.EncodeToString(b)
				outChan := strconv.FormatUint(htlc0.GetRoute().GetHops()[0].GetChanId(), 10)
				fsi := int(htlc0.GetFailure().GetFailureSourceIndex())
				failHop := -1
				if htlc0.GetFailure() != nil {
					failHop = fsi
				}
				e.mc.recordRouteFailure(htlc0.GetRoute(), htlc0.GetFailure())
				updateRoute(ctx, q, rebalance.LastHopPubkey, outChan, routeHex, false, false, failHop, now())
				updateNodeReputations(ctx, q, routeHex, false, fsi, htlc0.GetFailure() != nil, now())
			}
			switch pay.GetFailureReason() {
			case lnrpc.PaymentFailureReason_FAILURE_REASON_TIMEOUT:
				rebalance.Status = 3
			case lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE:
				rebalance.Status = 4
			case lnrpc.PaymentFailureReason_FAILURE_REASON_ERROR:
				rebalance.Status = 5
			case lnrpc.PaymentFailureReason_FAILURE_REASON_INCORRECT_PAYMENT_DETAILS:
				rebalance.Status = 6
			case lnrpc.PaymentFailureReason_FAILURE_REASON_INSUFFICIENT_BALANCE:
				rebalance.Status = 7
			}
		default: // status == 0 (UNKNOWN)
			rebalance.Status = 400
		}
	}
	return nil
}

// logPaymentAttempts records every HTLC attempt LND made for a finished
// SendPaymentV2 payment (list auto) with its duration.
func (e *engine) logPaymentAttempts(ctx context.Context, q attemptLogQuerier, rebalance *db.GuiRebalancer, pay *lnrpc.Payment, now time.Time) {
	for i, h := range pay.GetHtlcs() {
		hops := h.GetRoute().GetHops()
		if len(hops) == 0 {
			continue
		}
		res := savedRouteResult{code: -1, hop: -1, durationMs: -1}
		switch h.GetStatus() {
		case lnrpc.HTLCAttempt_SUCCEEDED:
			res.outcome = "success"
		case lnrpc.HTLCAttempt_FAILED:
			res.outcome = "failed"
			if f := h.GetFailure(); f != nil {
				res.code, res.hop = int(f.GetCode()), int(f.GetFailureSourceIndex())
			}
		default:
			continue
		}
		if h.GetResolveTimeNs() > h.GetAttemptTimeNs() && h.GetAttemptTimeNs() > 0 {
			res.durationMs = int((h.GetResolveTimeNs() - h.GetAttemptTimeNs()) / 1e6)
		}
		rr := rankedRoute{list: listAuto, position: i + 1, row: db.GuiRebalanceroute{
			TargetPubkey: rebalance.LastHopPubkey, OutgoingChanID: strconv.FormatUint(hops[0].GetChanId(), 10), Route: routePath(h.GetRoute()),
		}}
		e.logRouteAttempt(ctx, q, rebalance.ID, rr, int64(rebalance.Value), res, now)
	}
}
