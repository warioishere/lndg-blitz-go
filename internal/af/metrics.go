package af

import (
	"math"
	"time"
)

// aggregates holds pre-computed per-channel metrics keyed by chan_id.
// It is populated by the fetch layer (fetch.go).
type aggregates struct {
	avgRebalanceCost  map[string]*int      // nil when no history is available
	amtRoutedIn1day   map[string]int       // sats (converted from msat)
	amtRoutedIn7day   map[string]int       // sats (converted from msat)
	amtRoutedOut7day  map[string]int       // sats (converted from msat)
	amtRoutedIn4h     map[string]int       // sats (converted from msat)
	amtRoutedOut4h    map[string]int       // sats (converted from msat)
	revenueAssist7day map[string]float64   // sum of inbound fees earned
	revenue7day       map[string]float64   // sum of outbound fees earned
	failedOut1day     map[string]int       // failed HTLC count over update window
	failedOutBoost    map[string]int       // failed HTLC count over boost interval
	lastForwardOut    map[string]time.Time // most recent outbound forward
	lastForwardIn     map[string]time.Time // most recent inbound forward
}

// round1 rounds x to 1 decimal place using round-half-to-even.
func round1(x float64) float64 {
	return math.RoundToEven(x*10) / 10
}

// maxTime returns the later of two optional timestamps, ignoring nil values.
// Returns nil when both inputs are nil.
func maxTime(a, b *time.Time) *time.Time {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case b.After(*a):
		return b
	default:
		return a
	}
}

// computeChannelMetrics populates per-channel metric fields on each row in-place.
// now is the reference timestamp used for all age/elapsed calculations.
func computeChannelMetrics(s *Settings, rows []*ChannelFeeRow, agg *aggregates, now time.Time) {
	for _, r := range rows {
		r.AvgRebalanceCost = agg.avgRebalanceCost[r.ChanID]

		// Routed amounts — missing keys default to zero.
		r.AmtRoutedIn1day = agg.amtRoutedIn1day[r.ChanID]
		r.AmtRoutedIn7day = agg.amtRoutedIn7day[r.ChanID]
		r.AmtRoutedOut7day = agg.amtRoutedOut7day[r.ChanID]
		r.AmtRoutedIn4h = agg.amtRoutedIn4h[r.ChanID]
		r.AmtRoutedOut4h = agg.amtRoutedOut4h[r.ChanID]

		// Net flow ratio over 7 days, rounded to 1 decimal place.
		r.NetRouted7day = round1(float64(r.AmtRoutedOut7day-r.AmtRoutedIn7day) / float64(r.Capacity))

		// Apply pending amounts before computing out/in percent and group aggregation.
		r.LocalBalance += r.PendingOutbound
		r.RemoteBalance += r.PendingInbound

		// Outbound and inbound liquidity percentages, rounded to nearest integer.
		r.OutPercent = int(math.RoundToEven((float64(r.LocalBalance) / float64(r.Capacity)) * 100))
		r.InPercent = int(math.RoundToEven((float64(r.RemoteBalance) / float64(r.Capacity)) * 100))

		// A channel is eligible for a fee update when enough time has elapsed since the last update.
		r.Eligible = now.Sub(r.FeesUpdated).Seconds() > s.UpdateHours*3600

		// Most recent forward timestamps and combined last-forward.
		if t, ok := agg.lastForwardOut[r.ChanID]; ok {
			tc := t
			r.LastForwardOut = &tc
		}
		if t, ok := agg.lastForwardIn[r.ChanID]; ok {
			tc := t
			r.LastForwardIn = &tc
		}
		r.LastForward = maxTime(r.LastForwardOut, r.LastForwardIn)

		// Hours since last forward; 99999 signals no activity within the window.
		if r.LastForward == nil {
			r.HoursSinceLastForward = 99999
		} else {
			r.HoursSinceLastForward = now.Sub(*r.LastForward).Seconds() / 3600
		}

		r.FailedOut1day = agg.failedOut1day[r.ChanID]
		r.FailedOutBoostInterval = agg.failedOutBoost[r.ChanID]

		r.RevenueAssist7day = agg.revenueAssist7day[r.ChanID]
		r.Revenue7day = agg.revenue7day[r.ChanID]
	}
}

// groupOrder returns the unique remote pubkeys in first-seen order together
// with a map from pubkey to its channel rows.
func groupOrder(rows []*ChannelFeeRow) ([]string, map[string][]*ChannelFeeRow) {
	order := make([]string, 0)
	groups := make(map[string][]*ChannelFeeRow)
	for _, r := range rows {
		if _, ok := groups[r.RemotePubkey]; !ok {
			order = append(order, r.RemotePubkey)
		}
		groups[r.RemotePubkey] = append(groups[r.RemotePubkey], r)
	}
	return order, groups
}

// computeGroups aggregates per-channel rows by remote peer and computes the
// inbound adjustment for each peer group (curve or legacy mode).
func (s *Settings) computeGroups(rows []*ChannelFeeRow) map[string]*groupRow {
	order, groups := groupOrder(rows)
	result := make(map[string]*groupRow, len(order))
	for _, pk := range order {
		members := groups[pk]
		g := &groupRow{RemotePubkey: pk}
		minArIn := math.MaxInt
		for _, m := range members {
			g.TotalLocalBalance += m.LocalBalance
			g.TotalCapacity += m.Capacity
			g.TotalFailedOut1day += m.FailedOut1day
			g.TotalAmtRoutedIn1day += m.AmtRoutedIn1day
			g.TotalAmtRoutedIn7day += m.AmtRoutedIn7day
			g.TotalAmtRoutedOut7day += m.AmtRoutedOut7day
			g.TotalAmtRoutedIn4h += m.AmtRoutedIn4h
			g.TotalAmtRoutedOut4h += m.AmtRoutedOut4h
			g.TotalRevenue7day += m.Revenue7day
			g.TotalRevenueAssist7day += m.RevenueAssist7day
			if m.ArInTarget < minArIn {
				minArIn = m.ArInTarget
			}
		}

		// Overall outbound liquidity percentage across all channels in the group.
		if g.TotalCapacity > 0 {
			g.OverallOutPercent = (float64(g.TotalLocalBalance) / float64(g.TotalCapacity)) * 100
		} else {
			g.OverallOutPercent = 0
		}
		// Net flow ratio for the group over 7 days.
		if g.TotalCapacity > 0 {
			g.GroupNetRouted7day = float64(g.TotalAmtRoutedOut7day-g.TotalAmtRoutedIn7day) / float64(g.TotalCapacity)
		} else {
			g.GroupNetRouted7day = 0
		}
		// Peer outbound target: 100 minus the minimum ar_in_target across the group.
		g.PeerOutTarget = 100 - minArIn

		if s.CurveMode {
			g.InboundAdjustment = s.computeCurveInboundAdjustment(g)
		} else {
			g.InboundAdjustment = s.computeInboundAdjustment(g)
		}
		result[pk] = g
	}
	return result
}

// mergeGroupsToChannels copies peer-group aggregate fields back onto each channel row.
func mergeGroupsToChannels(rows []*ChannelFeeRow, groups map[string]*groupRow) {
	for _, r := range rows {
		g := groups[r.RemotePubkey]
		if g == nil {
			continue
		}
		r.OverallOutPercent = g.OverallOutPercent
		r.GroupNetRouted7day = g.GroupNetRouted7day
		r.TotalFailedOut1day = g.TotalFailedOut1day
		r.TotalAmtRoutedIn1day = g.TotalAmtRoutedIn1day
		r.TotalAmtRoutedIn7day = g.TotalAmtRoutedIn7day
		r.TotalAmtRoutedOut7day = g.TotalAmtRoutedOut7day
		r.TotalAmtRoutedIn4h = g.TotalAmtRoutedIn4h
		r.TotalAmtRoutedOut4h = g.TotalAmtRoutedOut4h
		r.TotalRevenue7day = g.TotalRevenue7day
		r.TotalRevenueAssist7day = g.TotalRevenueAssist7day
		r.InboundAdjustment = float64(g.InboundAdjustment)
	}
}
