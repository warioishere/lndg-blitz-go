package af

import (
	"context"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Querier bundles all sqlc queries needed by an AF run. db.Queries satisfies it.
type Querier interface {
	settingsQuerier
	fetchQuerier
}

// Main computes new outbound and inbound fee targets for the supplied channels.
// The caller determines the filter set (e.g. open+active+non-private+auto_fees).
// now is the reference timestamp for all time-window calculations.
//
// The function is read-only with respect to channels and LND; the caller is
// responsible for applying the returned targets. Side-effect: LoadSettings
// creates any missing LocalSettings via get-or-create.
func Main(ctx context.Context, q Querier, channels []db.GuiChannel, now time.Time) ([]*ChannelFeeRow, error) {
	s, err := LoadSettings(ctx, q)
	if err != nil {
		return nil, err
	}

	// Return early when there are no channels to process.
	if len(channels) == 0 {
		return []*ChannelFeeRow{}, nil
	}

	agg, err := fetchAggregates(ctx, q, channels, s, now)
	if err != nil {
		return nil, err
	}

	rows := make([]*ChannelFeeRow, len(channels))
	for i, ch := range channels {
		rows[i] = channelToRow(ch)
	}

	computeChannelMetrics(s, rows, agg, now)
	groups := s.computeGroups(rows)
	mergeGroupsToChannels(rows, groups)

	// Compute outbound adjustment and fee rates for each channel independently,
	// then apply the cross-channel mirror step.
	for _, r := range rows {
		var adj int
		if s.CurveMode {
			adj = s.computeCurveOutboundAdjustment(r)
		} else {
			adj = s.computeOutboundAdjustment(r)
		}
		r.Adjustment = float64(adj)
		s.applyOutboundRate(r)
		s.applyInboundRate(r)
	}

	// Mirror fee targets across multi-channel peers.
	rows = MirrorPeerFeeTargets(rows)

	// A positive inbound fee is a manual override (advanced / fee rates iRate) - leave it
	// alone, AF only manages discounts. Applied after the mirror so a peer's other channels
	// cannot drag it back. Set the rate to 0 or below to hand the channel back to AF.
	for _, r := range rows {
		if r.LocalInboundFeeRate > 0 {
			r.NewInboundRate = float64(r.LocalInboundFeeRate)
			r.InboundAdjustment = 0
		}
	}

	return rows, nil
}
