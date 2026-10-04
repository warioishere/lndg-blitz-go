package jobs

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"math"
	"strconv"
	"time"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// paymentsClient is the LND subset required by UpdatePayments.
type paymentsClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
	ListPayments(ctx context.Context, in *lnrpc.ListPaymentsRequest, opts ...grpc.CallOption) (*lnrpc.ListPaymentsResponse, error)
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
}

// paymentsQuerier bundles node cache and payment DB access.
type paymentsQuerier interface {
	nodeCacheQ
	ListInflightPayments(ctx context.Context) ([]db.ListInflightPaymentsRow, error)
	MaxPaymentIndex(ctx context.Context) (int32, error)
	InsertPayment(ctx context.Context, arg db.InsertPaymentParams) error
	SetPaymentStatus(ctx context.Context, arg db.SetPaymentStatusParams) error
	UpdatePaymentBasic(ctx context.Context, arg db.UpdatePaymentBasicParams) error
	UpdatePaymentHopResults(ctx context.Context, arg db.UpdatePaymentHopResultsParams) error
	ChannelFeeRates(ctx context.Context, chanIds []string) ([]db.ChannelFeeRatesRow, error)
	DeletePaymentHops(ctx context.Context, paymentHashID string) error
	InsertPaymentHop(ctx context.Context, arg db.InsertPaymentHopParams) error
}

func textPtr(p *string) pgtype.Text {
	if p == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *p, Valid: true}
}

// UpdatePayments syncs in-flight payments and fetches new payments from LND.
func UpdatePayments(ctx context.Context, q paymentsQuerier, client paymentsClient) error {
	info, err := client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	selfPubkey := info.IdentityPubkey

	inflight, err := q.ListInflightPayments(ctx)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-30 * 24 * time.Hour)
	for _, payment := range inflight {
		resp, e := client.ListPayments(ctx, &lnrpc.ListPaymentsRequest{
			IncludeIncomplete: true, IndexOffset: uint64(payment.Index - 1), MaxPayments: 1,
		})
		if e != nil {
			return e
		}
		pd := resp.Payments
		if len(pd) > 0 && payment.PaymentHash == pd[0].PaymentHash && payment.CreationDate.Valid && payment.CreationDate.Time.After(cutoff) {
			if e := updatePayment(ctx, q, client, pd[0], selfPubkey); e != nil {
				return e
			}
		} else {
			if e := q.SetPaymentStatus(ctx, db.SetPaymentStatusParams{PaymentHash: payment.PaymentHash, Status: 3}); e != nil {
				return e
			}
		}
	}

	maxIdx, err := q.MaxPaymentIndex(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListPayments(ctx, &lnrpc.ListPaymentsRequest{
		IncludeIncomplete: true, IndexOffset: uint64(maxIdx), MaxPayments: 100,
	})
	if err != nil {
		return err
	}
	for _, payment := range resp.Payments {
		// Insert the payment record; log and continue on duplicate or other insert errors.
		if e := q.InsertPayment(ctx, db.InsertPaymentParams{
			CreationDate: ts(time.Unix(payment.CreationDate, 0)),
			PaymentHash:  payment.PaymentHash,
			Value:        pyround.Round(float64(payment.ValueMsat)/1000, 3),
			Fee:          pyround.Round(float64(payment.FeeMsat)/1000, 3),
			Status:       int32(payment.Status),
			Index:        int32(payment.PaymentIndex),
		}); e != nil {
			dataLog(fmt.Sprintf("Error processing payment %s: %s", payment.PaymentHash, e))
		}
		if e := updatePayment(ctx, q, client, payment, selfPubkey); e != nil {
			return e
		}
	}
	return nil
}

// updatePayment updates the basic payment fields and, for completed or in-flight
// payments, rebuilds all hop records and extracts outgoing channel, keysend
// preimage, message, and rebalance channel from the HTLC attempt data.
func updatePayment(ctx context.Context, q paymentsQuerier, client paymentsClient, payment *lnrpc.Payment, selfPubkey string) error {
	if err := q.UpdatePaymentBasic(ctx, db.UpdatePaymentBasicParams{
		PaymentHash:  payment.PaymentHash,
		CreationDate: ts(time.Unix(payment.CreationDate, 0)),
		Value:        pyround.Round(float64(payment.ValueMsat)/1000, 3),
		Fee:          pyround.Round(float64(payment.FeeMsat)/1000, 3),
		Status:       int32(payment.Status),
		Index:        int32(payment.PaymentIndex),
	}); err != nil {
		return err
	}
	status := int32(payment.Status)
	if status != 2 && status != 1 {
		return nil
	}

	if err := q.DeletePaymentHops(ctx, payment.PaymentHash); err != nil {
		return err
	}
	var chanOut, chanOutAlias, keysendPreimage, message, rebalChan *string
	for _, attempt := range payment.Htlcs {
		astatus := int32(attempt.Status)
		if astatus != 1 && astatus != 0 {
			continue
		}
		hops := attempt.GetRoute().GetHops()
		totalHops := len(hops)
		var costTo float64
		for i, hop := range hops {
			hopCount := i + 1
			alias := nodeAlias(ctx, q, client, hop.PubKey)
			fee := float64(hop.FeeMsat) / 1000
			if hopCount == totalHops {
				htlcInfo := fmt.Sprintf("[ %d-%d-%d-%d ]", status, astatus, int(attempt.GetFailure().GetCode()), attempt.GetFailure().GetFailureSourceIndex())
				alias = composeAliasWithHtlc(alias, htlcInfo)
			}
			if err := q.InsertPaymentHop(ctx, db.InsertPaymentHopParams{
				AttemptID: int32(attempt.AttemptId), Step: int32(hopCount), ChanID: formatChanID(hop.ChanId),
				Alias: alias, ChanCapacity: hop.ChanCapacity, NodePubkey: hop.PubKey,
				Amt: pyround.Round(float64(hop.AmtToForwardMsat)/1000, 3), Fee: pyround.Round(fee, 3),
				PaymentHashID: payment.PaymentHash, CostTo: pyround.Round(costTo, 3),
			}); err != nil {
				return err
			}
			costTo += fee
			if hopCount == 1 && astatus == 1 {
				if chanOut == nil {
					co := formatChanID(hop.ChanId)
					ca := alias
					chanOut, chanOutAlias = &co, &ca
				} else {
					mpp := "MPP"
					chanOut, chanOutAlias = &mpp, &mpp
				}
			}
			if hopCount == totalHops {
				if preimage, ok := hop.CustomRecords[5482373484]; ok && keysendPreimage == nil {
					ph := hex.EncodeToString(preimage)
					keysendPreimage = &ph
					if msg, ok := hop.CustomRecords[34349334]; ok {
						m := keysendMessage(msg)
						message = &m
					} else {
						message = nil
					}
				}
				if hop.PubKey == selfPubkey && rebalChan == nil {
					rc := formatChanID(hop.ChanId)
					rebalChan = &rc
				}
			}
		}
	}
	var sourceFeeRate pgtype.Int4
	if status == 2 && rebalChan != nil {
		v, err := rebalanceSourceFeeRate(ctx, q, payment)
		if err != nil {
			return err
		}
		sourceFeeRate = v
	}
	return q.UpdatePaymentHopResults(ctx, db.UpdatePaymentHopResultsParams{
		PaymentHash:     payment.PaymentHash,
		ChanOut:         textPtr(chanOut),
		ChanOutAlias:    textPtr(chanOutAlias),
		KeysendPreimage: textPtr(keysendPreimage),
		Message:         textPtr(message),
		RebalChan:       textPtr(rebalChan),
		SourceFeeRate:   sourceFeeRate,
	})
}

// rebalanceSourceFeeRate is the opportunity cost of a rebalance: the outbound ppm of
// the channel(s) the liquidity left through, weighted by the amount each successful
// part sent. Invalid if no source channel is known anymore, the cost lookup then
// falls back to its current fee.
func rebalanceSourceFeeRate(ctx context.Context, q paymentsQuerier, payment *lnrpc.Payment) (pgtype.Int4, error) {
	type part struct {
		chanID string
		amt    int64
	}
	var parts []part
	var ids []string
	for _, attempt := range payment.Htlcs {
		hops := attempt.GetRoute().GetHops()
		if int32(attempt.Status) != 1 || len(hops) == 0 {
			continue
		}
		id := formatChanID(hops[0].ChanId)
		parts = append(parts, part{chanID: id, amt: attempt.GetRoute().GetTotalAmtMsat()})
		ids = append(ids, id)
	}
	rows, err := q.ChannelFeeRates(ctx, ids)
	if err != nil {
		return pgtype.Int4{}, err
	}
	rates := make(map[string]int32, len(rows))
	for _, r := range rows {
		rates[r.ChanID] = r.LocalFeeRate
	}
	var weighted float64
	var totalAmt int64
	for _, p := range parts {
		if rate, ok := rates[p.chanID]; ok {
			weighted += float64(rate) * float64(p.amt)
			totalAmt += p.amt
		}
	}
	if totalAmt == 0 {
		return pgtype.Int4{}, nil
	}
	// RoundToEven matches Python's round()
	return pgtype.Int4{Int32: int32(math.RoundToEven(weighted / float64(totalAmt))), Valid: true}, nil
}

// formatChanID formats a numeric channel ID as a decimal string.
func formatChanID(chanID uint64) string {
	return strconv.FormatUint(chanID, 10)
}

// composeAliasWithHtlc appends htlcInfo to alias while keeping the combined
// string within the 32-rune varchar limit.
func composeAliasWithHtlc(alias, htlcInfo string) string {
	hr := []rune(htlcInfo)
	if len(hr) <= 32 {
		ar := []rune(alias)
		keep := 32 - len(hr)
		if len(ar) > keep {
			ar = ar[:keep]
		}
		return string(ar) + htlcInfo
	}
	return string(hr[:32])
}
