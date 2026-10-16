package graphwatcher

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

// Statuses of gui_rebalancer records, as the rebalancer writes them.
const (
	rebalInFlight   = 1
	rebalSucceeded  = 2
	rebalNoRoute    = 4
	rebalReqFailed  = 400
	rebalReqTimeout = 408
)

// buyVia pays a verified forced route (jobs.ProbeVia) for real: an invoice over
// the verified amount, the route rebuilt with its payment address, and one
// SendToRouteV2 within AR-Time minutes. It pays only while route fees plus the
// source fee stay below buyPpm; ok is false when the rebuilt route costs more
// and nothing was sent. The purchase is a gui_rebalancer record like any
// rebalance; the payments job attributes its cost to the target channel. The
// attempt goes to route_attempt_log (list graph), a paid route is saved as a
// paid route of the target, so every rebalance worker tries it.
func (d Deps) buyVia(ctx context.Context, target db.GuiChannel, a jobs.ViaAttempt, buyPpm int) (rec db.GuiRebalancer, ok bool, err error) {
	srcID, err := strconv.ParseUint(a.SourceChanID, 10, 64)
	if err != nil {
		return rec, false, err
	}
	arTime := atoiOr(getSetting(ctx, d.Q, "AR-Time", "5"), 5)
	timeout := time.Duration(arTime) * time.Minute
	// fees below (buyPpm - source fee) ppm of the amount, in msat
	limitMsat := a.AmountSat * int64(buyPpm-a.SourceFeeRate) / 1000
	if limitMsat <= 0 {
		return rec, false, nil
	}

	inv, err := d.LN.AddInvoice(ctx, &lnrpc.Invoice{Value: a.AmountSat, Expiry: int64(timeout / time.Second), CltvExpiry: rebalancer.FinalCltvDelta})
	if err != nil {
		return rec, false, err
	}
	built, err := d.Router.BuildRoute(ctx, &routerrpc.BuildRouteRequest{
		OutgoingChanId: srcID,
		AmtMsat:        a.AmountSat * 1000,
		HopPubkeys:     a.HopKeys,
		FinalCltvDelta: rebalancer.FinalCltvDelta,
		PaymentAddr:    inv.GetPaymentAddr(),
	})
	if err != nil {
		return rec, false, err
	}
	route := built.GetRoute()
	if route.GetTotalFeesMsat() >= limitMsat {
		return rec, false, nil
	}

	rec = db.GuiRebalancer{
		Requested: pgtype.Timestamptz{Time: d.Now(), Valid: true}, Value: int32(a.AmountSat),
		FeeLimit:        pyround.Round(float64(limitMsat)/1000, 3),
		OutgoingChanIds: "[" + a.SourceChanID + "]", LastHopPubkey: target.RemotePubkey, TargetAlias: target.Alias,
		Duration: int32(arTime), Status: rebalInFlight,
	}
	rec.ID, err = d.Q.InsertRebalancerRecord(ctx, db.InsertRebalancerRecordParams{
		Requested: rec.Requested, Value: rec.Value, FeeLimit: rec.FeeLimit, OutgoingChanIds: rec.OutgoingChanIds,
		LastHopPubkey: rec.LastHopPubkey, TargetAlias: rec.TargetAlias, Duration: rec.Duration, Status: rec.Status,
	})
	if err != nil {
		return rec, false, err
	}
	rec.Start = pgtype.Timestamptz{Time: d.Now(), Valid: true}
	rec.PaymentHash = pgtype.Text{String: hex.EncodeToString(inv.GetRHash()), Valid: true}

	sctx, cancel := context.WithTimeout(ctx, timeout)
	start := d.Now()
	resp, serr := d.Router.SendToRouteV2(sctx, &routerrpc.SendToRouteRequest{PaymentHash: inv.GetRHash(), Route: route})
	cancel()
	attempt := rebalancer.RouteAttempt{RebalanceID: rec.ID, Target: target.RemotePubkey, OutChan: a.SourceChanID, List: "graph",
		Position: 1, ValueSat: a.AmountSat, Code: -1, Hop: -1, Path: rebalancer.RoutePath(route),
		DurationMs: int(d.Now().Sub(start) / time.Millisecond)}
	switch {
	case serr != nil && status.Code(serr) == codes.DeadlineExceeded:
		rec.Status = rebalReqTimeout
		attempt.Outcome = "timeout"
	case serr != nil:
		rec.Status = rebalReqFailed
		gwLog(fmt.Sprintf("  payment error: %s", serr))
		attempt.Outcome, attempt.DurationMs = "error", -1
	case resp.GetStatus() == lnrpc.HTLCAttempt_SUCCEEDED:
		rec.Status = rebalSucceeded
		rec.FeesPaid = pgtype.Float8{Float64: float64(resp.GetRoute().GetTotalFeesMsat()) / 1000, Valid: true}
		attempt.Outcome = "success"
		rebalancer.RecordPaidRoute(ctx, d.Q, target.RemotePubkey, a.SourceChanID, route, d.Now())
	default:
		rec.Status = rebalNoRoute
		attempt.Outcome = "failed"
		if f := resp.GetFailure(); f != nil {
			attempt.Code, attempt.Hop = int(f.GetCode()), int(f.GetFailureSourceIndex())
		}
	}
	rebalancer.RecordAttempt(ctx, d.Q, attempt, d.Now())
	rec.Stop = pgtype.Timestamptz{Time: d.Now(), Valid: true}
	if err := d.Q.UpdateRebalancerRecord(ctx, db.UpdateRebalancerRecordParams{
		ID: rec.ID, Value: rec.Value, FeeLimit: rec.FeeLimit, OutgoingChanIds: rec.OutgoingChanIds,
		LastHopPubkey: rec.LastHopPubkey, TargetAlias: rec.TargetAlias, Duration: rec.Duration,
		Start: rec.Start, Stop: rec.Stop, Status: rec.Status, PaymentHash: rec.PaymentHash,
		Manual: rec.Manual, FeesPaid: rec.FeesPaid,
	}); err != nil {
		return rec, true, err
	}
	return rec, true, nil
}
