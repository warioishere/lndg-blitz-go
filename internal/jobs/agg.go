package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// aggQuerier is the DB subset required by AggFailedHtlcs.
type aggQuerier interface {
	SelectBalanceFailedIDs(ctx context.Context, timestamp pgtype.Timestamptz) ([]int64, error)
	SelectDownstreamFailedIDs(ctx context.Context, timestamp pgtype.Timestamptz) ([]int64, error)
	SelectOtherFailedIDs(ctx context.Context, timestamp pgtype.Timestamptz) ([]int64, error)
	AggregateFailedHTLCs(ctx context.Context, ids []int64) ([]db.AggregateFailedHTLCsRow, error)
	GetHistFailedHTLC(ctx context.Context, arg db.GetHistFailedHTLCParams) (db.GuiHistfailedhtlc, error)
	InsertHistFailedHTLC(ctx context.Context, arg db.InsertHistFailedHTLCParams) error
	UpdateHistFailedHTLC(ctx context.Context, arg db.UpdateHistFailedHTLCParams) error
	DeleteAggregatedFailedHTLCs(ctx context.Context, arg db.DeleteAggregatedFailedHTLCsParams) error
}

// AggFailedHtlcs aggregates failed HTLCs from the last 30 days into the history table,
// grouped by failure category (balance, downstream, other).
func AggFailedHtlcs(ctx context.Context, q aggQuerier) error {
	cutoff := ts(time.Now().Add(-30 * 24 * time.Hour))
	bal, err := q.SelectBalanceFailedIDs(ctx, cutoff)
	if err != nil {
		return err
	}
	aggHtlcsLogged(ctx, q, bal, "balance")
	down, err := q.SelectDownstreamFailedIDs(ctx, cutoff)
	if err != nil {
		return err
	}
	aggHtlcsLogged(ctx, q, down, "downstream")
	other, err := q.SelectOtherFailedIDs(ctx, cutoff)
	if err != nil {
		return err
	}
	aggHtlcsLogged(ctx, q, other, "other")
	return nil
}

// aggHtlcsLogged calls aggHtlcs and logs any error without propagating it.
func aggHtlcsLogged(ctx context.Context, q aggQuerier, ids []int64, category string) {
	if err := aggHtlcs(ctx, q, ids, category); err != nil {
		dataLog(fmt.Sprintf("Error processing agg_htlcs: %s", err))
	}
}

// aggHtlcs merges a batch of failed HTLC IDs into the history table.
// For each (day, chan_in, chan_out) group it either inserts a new history row
// or updates the running totals (count, amount, fee) and running averages
// (liq, pending) before deleting the source rows.
func aggHtlcs(ctx context.Context, q aggQuerier, ids []int64, category string) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := q.AggregateFailedHTLCs(ctx, ids)
	if err != nil {
		return err
	}
	for _, h := range rows {
		existing, gerr := q.GetHistFailedHTLC(ctx, db.GetHistFailedHTLCParams{
			Date: h.Day, ChanIDIn: h.ChanIDIn, ChanIDOut: h.ChanIDOut,
		})
		isNew := false
		hist := existing
		if isNoRows(gerr) {
			isNew = true
			hist = db.GuiHistfailedhtlc{
				Date: h.Day, ChanIDIn: h.ChanIDIn, ChanIDOut: h.ChanIDOut,
				ChanInAlias: textOf(h.ChanInAlias), ChanOutAlias: textOf(h.ChanOutAlias),
			}
		} else if gerr != nil {
			return gerr
		}

		// Update running average: accumulate count first, then compute new averages using the updated count.
		newCount := hist.HtlcCount + h.Count
		amountSum := hist.AmountSum + h.Amount
		feeSum := int64(float64(hist.FeeSum) + h.Fee) // += float, save trunkiert
		var liqAvg, pendingAvg int64
		if newCount != 0 {
			ratio := float64(h.Count) / float64(newCount)
			liqAvg = int64(float64(hist.LiqAvg) + ratio*(h.Liq-float64(hist.LiqAvg)))
			pendingAvg = int64(float64(hist.PendingAvg) + ratio*(h.Pending-float64(hist.PendingAvg)))
		} else {
			liqAvg, pendingAvg = hist.LiqAvg, hist.PendingAvg
		}
		balanceCount, downstreamCount, otherCount := hist.BalanceCount, hist.DownstreamCount, hist.OtherCount
		switch category {
		case "balance":
			balanceCount += h.Count
		case "downstream":
			downstreamCount += h.Count
		case "other":
			otherCount += h.Count
		}

		if isNew {
			if e := q.InsertHistFailedHTLC(ctx, db.InsertHistFailedHTLCParams{
				Date: h.Day, ChanIDIn: h.ChanIDIn, ChanIDOut: h.ChanIDOut,
				ChanInAlias: textOf(h.ChanInAlias), ChanOutAlias: textOf(h.ChanOutAlias),
				HtlcCount: newCount, AmountSum: amountSum, FeeSum: feeSum, LiqAvg: liqAvg, PendingAvg: pendingAvg,
				BalanceCount: balanceCount, DownstreamCount: downstreamCount, OtherCount: otherCount,
			}); e != nil {
				return e
			}
		} else {
			if e := q.UpdateHistFailedHTLC(ctx, db.UpdateHistFailedHTLCParams{
				Date: h.Day, ChanIDIn: h.ChanIDIn, ChanIDOut: h.ChanIDOut,
				HtlcCount: newCount, AmountSum: amountSum, FeeSum: feeSum, LiqAvg: liqAvg, PendingAvg: pendingAvg,
				BalanceCount: balanceCount, DownstreamCount: downstreamCount, OtherCount: otherCount,
			}); e != nil {
				return e
			}
		}

		if e := q.DeleteAggregatedFailedHTLCs(ctx, db.DeleteAggregatedFailedHTLCsParams{
			Column1: ids, ChanIDIn: h.ChanIDIn, ChanIDOut: h.ChanIDOut, Day: h.Day,
		}); e != nil {
			return e
		}
	}
	return nil
}
