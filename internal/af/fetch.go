package af

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// fetchQuerier is the narrow subset of the sqlc querier used for AF data retrieval.
type fetchQuerier interface {
	RebalPaymentsForChannel(ctx context.Context, arg db.RebalPaymentsForChannelParams) ([]db.RebalPaymentsForChannelRow, error)
	ForwardsInSumFee(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsInSumFeeRow, error)
	ForwardsInSum(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsInSumRow, error)
	ForwardsOutSumFee(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsOutSumFeeRow, error)
	ForwardsOutSum(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsOutSumRow, error)
	ForwardsLastOut(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsLastOutRow, error)
	ForwardsLastIn(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.ForwardsLastInRow, error)
	FailedHTLCsForAF(ctx context.Context, timestamp pgtype.Timestamptz) ([]db.FailedHTLCsForAFRow, error)
}

func ts(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

// channelToRow populates a ChannelFeeRow with the raw input fields from a channel record.
func channelToRow(ch db.GuiChannel) *ChannelFeeRow {
	return &ChannelFeeRow{
		ChanID:               ch.ChanID,
		RemotePubkey:         ch.RemotePubkey,
		Capacity:             ch.Capacity,
		LocalBalance:         ch.LocalBalance,
		RemoteBalance:        ch.RemoteBalance,
		PendingOutbound:      ch.PendingOutbound,
		PendingInbound:       ch.PendingInbound,
		LocalFeeRate:         int(ch.LocalFeeRate),
		LocalInboundFeeRate:  int(ch.LocalInboundFeeRate),
		RemoteFeeRate:        int(ch.RemoteFeeRate),
		RemoteInboundFeeRate: int(ch.RemoteInboundFeeRate),
		ArInTarget:           int(ch.ArInTarget),
		ArMaxCost:            int(ch.ArMaxCost),
		AutoRebalance:        ch.AutoRebalance,
		FeesUpdated:          ch.FeesUpdated.Time,
		FlpEnabled:           ch.FlpEnabled,
		FlpSafety:            int(ch.FlpSafety),
	}
}

// fetchAggregates builds per-channel aggregate maps from forwards, payments, and failed HTLCs.
func fetchAggregates(ctx context.Context, q fetchQuerier, channels []db.GuiChannel, s *Settings, now time.Time) (*aggregates, error) {
	agg := &aggregates{
		avgRebalanceCost:  map[string]*int{},
		amtRoutedIn1day:   map[string]int{},
		amtRoutedIn7day:   map[string]int{},
		amtRoutedOut7day:  map[string]int{},
		amtRoutedIn4h:     map[string]int{},
		amtRoutedOut4h:    map[string]int{},
		revenueAssist7day: map[string]float64{},
		revenue7day:       map[string]float64{},
		failedOut1day:     map[string]int{},
		failedOutBoost:    map[string]int{},
		lastForwardOut:    map[string]time.Time{},
		lastForwardIn:     map[string]time.Time{},
	}

	// Average rebalance cost per channel over the last `lookback` successful rebalances.
	for _, ch := range channels {
		pays, err := q.RebalPaymentsForChannel(ctx, db.RebalPaymentsForChannelParams{
			RebalChan: pgtype.Text{String: ch.ChanID, Valid: true},
			Limit:     int32(s.Lookback),
		})
		if err != nil {
			return nil, err
		}
		agg.avgRebalanceCost[ch.ChanID] = AvgRebalanceCostPPM(pays)
	}

	// Time window boundaries.
	filter1day := ts(now.Add(-24 * time.Hour))
	filter4h := ts(now.Add(-4 * time.Hour))
	filter7day := ts(now.Add(-7 * 24 * time.Hour))
	filterLastUpdated := ts(now.Add(-time.Duration(s.UpdateHours * float64(time.Hour))))
	filterHTLCBoost := ts(now.Add(-time.Duration(s.HtlcBoostInterval) * time.Minute))

	// Inbound routed amount over the last 24 hours.
	in1d, err := q.ForwardsInSum(ctx, filter1day)
	if err != nil {
		return nil, err
	}
	for _, r := range in1d {
		agg.amtRoutedIn1day[r.ChanIDIn] = int(r.AmtOutMsat / 1000) // convert msat to sat
	}
	// Inbound routed amount over the last 4 hours.
	in4h, err := q.ForwardsInSum(ctx, filter4h)
	if err != nil {
		return nil, err
	}
	for _, r := range in4h {
		agg.amtRoutedIn4h[r.ChanIDIn] = int(r.AmtOutMsat / 1000)
	}
	// Outbound routed amount over the last 4 hours.
	out4h, err := q.ForwardsOutSum(ctx, filter4h)
	if err != nil {
		return nil, err
	}
	for _, r := range out4h {
		agg.amtRoutedOut4h[r.ChanIDOut] = int(r.AmtOutMsat / 1000)
	}
	// Inbound routed amount and assist revenue over the last 7 days.
	in7d, err := q.ForwardsInSumFee(ctx, filter7day)
	if err != nil {
		return nil, err
	}
	for _, r := range in7d {
		agg.amtRoutedIn7day[r.ChanIDIn] = int(r.AmtOutMsat / 1000)
		agg.revenueAssist7day[r.ChanIDIn] = r.Fee
	}
	// Outbound routed amount and revenue over the last 7 days.
	out7d, err := q.ForwardsOutSumFee(ctx, filter7day)
	if err != nil {
		return nil, err
	}
	for _, r := range out7d {
		agg.amtRoutedOut7day[r.ChanIDOut] = int(r.AmtOutMsat / 1000)
		agg.revenue7day[r.ChanIDOut] = r.Fee
	}
	// Most recent outbound and inbound forward timestamps.
	lastOut, err := q.ForwardsLastOut(ctx, filter7day)
	if err != nil {
		return nil, err
	}
	for _, r := range lastOut {
		if r.LastOut.Valid {
			agg.lastForwardOut[r.ChanIDOut] = r.LastOut.Time
		}
	}
	lastIn, err := q.ForwardsLastIn(ctx, filter7day)
	if err != nil {
		return nil, err
	}
	for _, r := range lastIn {
		if r.LastIn.Valid {
			agg.lastForwardIn[r.ChanIDIn] = r.LastIn.Time
		}
	}

	// Failed HTLCs: count per outbound channel where amount exceeds available
	// liquidity + pending. Rows with NULL liquidity or pending are excluded.
	failed1d, err := q.FailedHTLCsForAF(ctx, filterLastUpdated)
	if err != nil {
		return nil, err
	}
	agg.failedOut1day = failedCounts(failed1d)

	failedBoost, err := q.FailedHTLCsForAF(ctx, filterHTLCBoost)
	if err != nil {
		return nil, err
	}
	agg.failedOutBoost = failedCounts(failedBoost)

	return agg, nil
}

// failedCounts counts failed HTLCs per outbound channel where the attempted
// amount exceeded available liquidity plus pending capacity.
func failedCounts(rows []db.FailedHTLCsForAFRow) map[string]int {
	m := map[string]int{}
	for _, r := range rows {
		// Skip rows where liquidity or pending is NULL.
		if !r.ChanOutLiq.Valid || !r.ChanOutPending.Valid {
			continue
		}
		if int64(r.Amount) > r.ChanOutLiq.Int64+r.ChanOutPending.Int64 {
			m[r.ChanIDOut]++
		}
	}
	return m
}

// AvgRebalanceCostPPM averages the cost of rebalance payments in ppm: the routing
// fee paid (fee*1e6/value) plus the outbound fee of the source channel the liquidity
// was taken from (opportunity cost). Payments with value 0 are skipped. Returns nil
// when nothing is left to average.
func AvgRebalanceCostPPM(pays []db.RebalPaymentsForChannelRow) *int {
	var sum float64
	var n int
	for _, p := range pays {
		if p.Value != 0 {
			sum += p.Fee*1000000/p.Value + float64(p.SourceFeeRate)
			n++
		}
	}
	if n == 0 {
		return nil
	}
	v := int(sum / float64(n)) // truncates toward zero
	return &v
}
