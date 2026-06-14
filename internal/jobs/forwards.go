package jobs

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// forwardsClient is the LND subset required by UpdateForwards and emergencyForwardCheck.
type forwardsClient interface {
	emergencyForwardClient
	ForwardingHistory(ctx context.Context, in *lnrpc.ForwardingHistoryRequest, opts ...grpc.CallOption) (*lnrpc.ForwardingHistoryResponse, error)
}

// forwardsQuerier is the DB subset required by UpdateForwards.
type forwardsQuerier interface {
	emergencyForwardQuerier
	LatestForward(ctx context.Context) (db.LatestForwardRow, error)
	CountForwardsAtDate(ctx context.Context, forwardDate pgtype.Timestamptz) (int64, error)
	GetChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error)
	InsertForward(ctx context.Context, arg db.InsertForwardParams) error
}

func textOf(s string) pgtype.Text { return pgtype.Text{String: s, Valid: true} }

// first12 returns the first 12 runes of s.
func first12(s string) string {
	r := []rune(s)
	if len(r) > 12 {
		return string(r[:12])
	}
	return s
}

// UpdateForwards fetches new forwarding events from LND and persists them to the DB.
func UpdateForwards(ctx context.Context, q forwardsQuerier, client forwardsClient) error {
	var startTime uint64 = 1420070400
	var processedCount int64
	latest, lerr := q.LatestForward(ctx)
	if lerr == nil {
		startTime = uint64(latest.ForwardDate.Time.Unix())
		cnt, e := q.CountForwardsAtDate(ctx, latest.ForwardDate)
		if e != nil {
			return e
		}
		processedCount = cnt
	} else if !isNoRows(lerr) {
		return lerr
	}

	resp, err := client.ForwardingHistory(ctx, &lnrpc.ForwardingHistoryRequest{
		StartTime: startTime, IndexOffset: uint32(processedCount), NumMaxEvents: 1000,
	})
	if err != nil {
		return err
	}
	forwards := resp.ForwardingEvents
	if len(forwards) == 0 {
		return nil
	}

	chanIDSet := map[string]bool{}
	for _, f := range forwards {
		chanIDSet[formatChanID(f.ChanIdIn)] = true
		chanIDSet[formatChanID(f.ChanIdOut)] = true
	}
	chanIDs := make([]string, 0, len(chanIDSet))
	for id := range chanIDSet {
		chanIDs = append(chanIDs, id)
	}
	chans, err := q.GetChannelsByIDs(ctx, chanIDs)
	if err != nil {
		return err
	}
	chanMap := make(map[string]db.GuiChannel, len(chans))
	for _, c := range chans {
		chanMap[c.ChanID] = c
	}

	chanOuts := make([]string, 0, len(forwards))
	for _, forward := range forwards {
		inID := formatChanID(forward.ChanIdIn)
		outID := formatChanID(forward.ChanIdOut)
		inbound, hasIn := chanMap[inID]
		outbound, hasOut := chanMap[outID]
		forwardDatetime := time.Unix(int64(forward.Timestamp), 0)
		amtInMsat := int64(forward.AmtInMsat)
		amtOutMsat := int64(forward.AmtOutMsat)

		var inFeeMsat int64
		if hasOut && outbound.FeesUpdated.Valid && outbound.FeesUpdated.Time.Before(forwardDatetime) {
			outFeeMsat := int64(float64(amtOutMsat)*(float64(outbound.LocalFeeRate)/1000000) + float64(outbound.LocalBaseFee))
			if int64(forward.FeeMsat) < outFeeMsat {
				inFeeMsat = outFeeMsat - int64(forward.FeeMsat)
			}
		}

		incomingAlias := forward.PeerAliasIn
		if hasIn {
			if inbound.Alias == "" {
				incomingAlias = first12(inbound.RemotePubkey)
			} else {
				incomingAlias = inbound.Alias
			}
		}
		outgoingAlias := forward.PeerAliasOut
		if hasOut {
			if outbound.Alias == "" {
				outgoingAlias = first12(outbound.RemotePubkey)
			} else {
				outgoingAlias = outbound.Alias
			}
		}

		if e := q.InsertForward(ctx, db.InsertForwardParams{
			ForwardDate: ts(forwardDatetime), ChanIDIn: inID, ChanIDOut: outID,
			ChanInAlias: textOf(incomingAlias), ChanOutAlias: textOf(outgoingAlias),
			AmtInMsat: amtInMsat, AmtOutMsat: amtOutMsat,
			Fee: roundTo(float64(forward.FeeMsat)/1000, 3), InboundFee: roundTo(float64(inFeeMsat)/1000, 3),
		}); e != nil {
			return e
		}
		chanOuts = append(chanOuts, outID)
	}
	return emergencyForwardCheck(ctx, q, client, chanOuts)
}
