// Package htlcstream subscribes to LND HTLC events, records failed forwards
// in gui_failedhtlcs, and triggers the emergency-forward check on settle events.
package htlcstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// Querier bundles the DB access needed by this package: channel lookup,
// failed-HTLC insert, and the queries required by jobs.EmergencyForwardQuerier.
type Querier interface {
	jobs.EmergencyForwardQuerier
	GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error)
	InsertFailedHtlc(ctx context.Context, arg db.InsertFailedHtlcParams) error
}

// RouterClient is the routerrpc subset used for HTLC event subscription.
type RouterClient interface {
	SubscribeHtlcEvents(ctx context.Context, in *routerrpc.SubscribeHtlcEventsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[routerrpc.HtlcEvent], error)
}

// Deps holds the external dependencies. LN satisfies jobs.EmergencyForwardClient
// (ListChannels + UpdateChannelPolicy). Sleep is injectable for tests.
type Deps struct {
	Q      Querier
	LN     jobs.EmergencyForwardClient
	Router RouterClient
	Now    func() time.Time
	Sleep  func(time.Duration)
}

func htlcLog(msg string) {
	fmt.Printf("%s : [HTLC] : %s\n", time.Now().Format("Mon Jan  2 15:04:05 2006"), msg)
}

func formatChanID(id uint64) string { return strconv.FormatUint(id, 10) }

// forwardKey builds a correlation key for an HTLC event. The colon separator
// prevents collisions between different (incoming, outgoing, in_htlc, out_htlc)
// combinations that would produce the same string under pure concatenation.
func forwardKey(ev *routerrpc.HtlcEvent) string {
	return fmt.Sprintf("%d:%d:%d:%d", ev.GetIncomingChannelId(), ev.GetOutgoingChannelId(), ev.GetIncomingHtlcId(), ev.GetOutgoingHtlcId())
}

// Run loops indefinitely, resubscribing after any error or stream end with a
// 20-second pause between attempts. It exits when ctx is cancelled.
func (d Deps) Run(ctx context.Context) {
	for ctx.Err() == nil {
		d.runSession(ctx)
		d.Sleep(20 * time.Second)
	}
}

func (d Deps) runSession(ctx context.Context) {
	htlcLog("Starting failed HTLC stream...")
	stream, err := d.Router.SubscribeHtlcEvents(ctx, &routerrpc.SubscribeHtlcEventsRequest{})
	if err != nil {
		htlcLog(fmt.Sprintf("Error while running failed HTLC stream: %s", err))
		return
	}
	// forwards tracks in-flight forward events for the lifetime of this subscription.
	forwards := map[string]*routerrpc.ForwardEvent{}
	for {
		ev, rerr := stream.Recv()
		if rerr != nil {
			if ctx.Err() == nil {
				htlcLog(fmt.Sprintf("Error while running failed HTLC stream: %s", rerr))
			}
			return
		}
		if herr := d.handleEvent(ctx, ev, forwards); herr != nil {
			htlcLog(fmt.Sprintf("Error while running failed HTLC stream: %s", herr))
			return
		}
	}
}

// handleEvent processes a single HtlcEvent. Only FORWARD-type events are acted on.
func (d Deps) handleEvent(ctx context.Context, ev *routerrpc.HtlcEvent, forwards map[string]*routerrpc.ForwardEvent) error {
	if ev.GetEventType() != routerrpc.HtlcEvent_FORWARD {
		return nil
	}
	switch {
	case ev.GetLinkFailEvent() != nil:
		return d.recordLinkFail(ctx, ev)
	case ev.GetForwardEvent() != nil:
		forwards[forwardKey(ev)] = ev.GetForwardEvent()
	case ev.GetSettleEvent() != nil:
		key := forwardKey(ev)
		if _, ok := forwards[key]; ok {
			delete(forwards, key)
			return jobs.EmergencyForwardCheck(ctx, d.Q, d.LN, []string{formatChanID(ev.GetOutgoingChannelId())})
		}
	default:
		// Catch-all for forward_fail_event, final_htlc_event, and similar events
		// that carry no specific sub-event payload.
		key := forwardKey(ev)
		if fe, ok := forwards[key]; ok {
			if err := d.recordForwardFail(ctx, ev, fe); err != nil {
				return err
			}
			delete(forwards, key)
		}
	}
	return nil
}

// recordLinkFail writes a failed-HTLC record for a link-fail event.
func (d Deps) recordLinkFail(ctx context.Context, ev *routerrpc.HtlcEvent) error {
	inID := formatChanID(ev.GetIncomingChannelId())
	outID := formatChanID(ev.GetOutgoingChannelId())
	inCh, err := d.lookupChannel(ctx, inID)
	if err != nil {
		return err
	}
	outCh, err := d.lookupChannel(ctx, outID)
	if err != nil {
		return err
	}
	lf := ev.GetLinkFailEvent()
	info := lf.GetInfo()
	return d.Q.InsertFailedHtlc(ctx, db.InsertFailedHtlcParams{
		Timestamp:      pgtype.Timestamptz{Time: d.Now(), Valid: true},
		Amount:         int32(float64(info.GetOutgoingAmtMsat()) / 1000),
		ChanIDIn:       inID,
		ChanIDOut:      outID,
		ChanInAlias:    aliasText(inCh),
		ChanOutAlias:   aliasText(outCh),
		ChanOutLiq:     outLiq(outCh),
		ChanOutPending: outPending(outCh),
		WireFailure:    int32(lf.GetWireFailure()),
		FailureDetail:  int32(lf.GetFailureDetail()),
		MissedFee:      float64(int64(info.GetIncomingAmtMsat())-int64(info.GetOutgoingAmtMsat())) / 1000,
	})
}

// recordForwardFail writes a failed-HTLC record for a tracked forward that never settled.
func (d Deps) recordForwardFail(ctx context.Context, ev *routerrpc.HtlcEvent, fe *routerrpc.ForwardEvent) error {
	inID := formatChanID(ev.GetIncomingChannelId())
	outID := formatChanID(ev.GetOutgoingChannelId())
	inCh, err := d.lookupChannel(ctx, inID)
	if err != nil {
		return err
	}
	outCh, err := d.lookupChannel(ctx, outID)
	if err != nil {
		return err
	}
	info := fe.GetInfo()
	return d.Q.InsertFailedHtlc(ctx, db.InsertFailedHtlcParams{
		Timestamp:      pgtype.Timestamptz{Time: d.Now(), Valid: true},
		Amount:         int32(float64(info.GetIncomingAmtMsat()) / 1000),
		ChanIDIn:       inID,
		ChanIDOut:      outID,
		ChanInAlias:    aliasText(inCh),
		ChanOutAlias:   aliasText(outCh),
		ChanOutLiq:     outLiq(outCh),
		ChanOutPending: outPending(outCh),
		WireFailure:    99,
		FailureDetail:  99,
		MissedFee:      float64(int64(info.GetIncomingAmtMsat())-int64(info.GetOutgoingAmtMsat())) / 1000,
	})
}

// lookupChannel returns the channel row for chanID, or nil if it does not exist.
func (d Deps) lookupChannel(ctx context.Context, chanID string) (*db.GuiChannel, error) {
	row, err := d.Q.GetChannel(ctx, chanID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

func aliasText(ch *db.GuiChannel) pgtype.Text {
	if ch == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: ch.Alias, Valid: true}
}

// outLiq returns the spendable outbound liquidity: max(0, local_balance - local_chan_reserve).
// Returns an invalid (NULL) value when the channel is unknown.
func outLiq(ch *db.GuiChannel) pgtype.Int8 {
	if ch == nil {
		return pgtype.Int8{}
	}
	liq := ch.LocalBalance - int64(ch.LocalChanReserve)
	if liq < 0 {
		liq = 0
	}
	return pgtype.Int8{Int64: liq, Valid: true}
}

func outPending(ch *db.GuiChannel) pgtype.Int8 {
	if ch == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: ch.PendingOutbound, Valid: true}
}
