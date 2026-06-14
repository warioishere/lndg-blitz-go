package af

import "math"

// clipF clamps x to [lower, upper].
func clipF(x, lower, upper float64) float64 {
	if x < lower {
		x = lower
	}
	if x > upper {
		x = upper
	}
	return x
}

// computeCostFloor calculates the minimum fee rate enforced by the fee-lock
// protection (FLP) feature. Returns 0 when FLP is disabled, the channel has
// no FLP flag set, or no average rebalance cost is available.
func (s *Settings) computeCostFloor(r *ChannelFeeRow) int {
	if !s.FlpEnabledGlobal {
		return 0
	}
	if !r.FlpEnabled {
		return 0
	}
	if r.AvgRebalanceCost == nil {
		return 0
	}
	avgCost := *r.AvgRebalanceCost
	channelSafety := r.FlpSafety
	safety := s.FlpSafetyGlobal + channelSafety
	currentRate := r.LocalFeeRate
	floorValue := avgCost + safety
	if floorValue < 0 { // max(avg_cost + safety, 0)
		floorValue = 0
	}
	if currentRate > 0 { // min(floor_value, current_rate)
		if floorValue > currentRate {
			floorValue = currentRate
		}
	}
	return int(math.RoundToEven(float64(floorValue)))
}

// enforceCostFloor prevents a proposed fee decrease from dropping below the
// FLP cost floor or the current rate when the floor exceeds the current rate.
func enforceCostFloor(r *ChannelFeeRow) float64 {
	proposed := r.NewRate
	current := float64(r.LocalFeeRate)
	floor := r.CostFloor
	if proposed < current {
		if floor > current {
			return current
		}
		if floor > proposed {
			return floor
		}
	}
	return proposed
}

// applyOutboundRate computes NewRate from LocalFeeRate plus the outbound adjustment,
// rounds to the configured increment, clips to [MinRate, MaxRate], applies the FLP
// cost floor, and recalculates Adjustment as (NewRate - LocalFeeRate).
//
// Precondition: r.Adjustment holds the integer value from compute_*_outbound_adjustment.
func (s *Settings) applyOutboundRate(r *ChannelFeeRow) {
	nr := float64(r.LocalFeeRate) + r.Adjustment
	nr = math.RoundToEven(nr/float64(s.Increment)) * float64(s.Increment)
	nr = clipF(nr, float64(s.MinRate), float64(s.MaxRate))
	r.NewRate = nr

	// Snapshot pre-floor values for diagnostic fields.
	r.NewRateBeforeFloor = nr
	r.AdjustmentBeforeFloor = r.Adjustment

	// Cost floor: clipped to MaxRate, defaulting to 0 when FLP is off.
	cf := float64(s.computeCostFloor(r))
	if cf > float64(s.MaxRate) {
		cf = float64(s.MaxRate)
	}
	r.CostFloor = cf

	r.NewRate = enforceCostFloor(r)
	r.Adjustment = r.NewRate - float64(r.LocalFeeRate)
}

// applyInboundRate computes NewInboundRate from LocalInboundFeeRate plus the
// inbound adjustment, rounds to the configured increment, clips to
// [-(ArMaxCost/100 * LocalFeeRate), 0], and recalculates InboundAdjustment.
//
// Precondition: r.InboundAdjustment holds the merged group integer value.
func (s *Settings) applyInboundRate(r *ChannelFeeRow) {
	nir := float64(r.LocalInboundFeeRate) + r.InboundAdjustment
	nir = math.RoundToEven(nir/float64(s.Increment)) * float64(s.Increment)
	lower := -((float64(r.ArMaxCost) / 100.0) * float64(r.LocalFeeRate))
	nir = clipF(nir, lower, 0)
	r.NewInboundRate = nir
	r.InboundAdjustment = r.NewInboundRate - float64(r.LocalInboundFeeRate)
}
