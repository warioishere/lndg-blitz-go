package af

import "math"

// MaxCostDiscount is the largest inbound discount worth paying on a channel, in
// ppm: what the rebalancer may pay at most to refill it (ar_max_cost % of the
// channel's outbound fee, source fee included).
func MaxCostDiscount(arMaxCost, localFeeRate int) float64 {
	return (float64(arMaxCost) / 100.0) * float64(localFeeRate)
}

// InboundInput is the channel state the dynamic inbound rate depends on.
// Dynamic marks a channel whose inbound fee is managed (inbound_offset != 0).
type InboundInput struct {
	Dynamic                                 bool
	LocalFeeRate, ArMaxCost, ArInTarget     int
	Capacity, LocalBalance, PendingOutbound int64
}

// InboundDynamicRate is the inbound fee rate of a channel with dynamic inbound
// fees. The discount cap is MaxCostDiscount; the discount follows the channel's
// liquidity: the full cap while the channel is empty, cap * (1 - fill^exponent)
// below its outbound target (fill = outbound share / target), none at or above
// the target. A discount is paid to refill the channel, so it fades out as the
// channel reaches its balance. discountCap is the cap in ppm; ok is false for a
// channel without dynamic inbound fees.
func InboundDynamicRate(in InboundInput, exponent float64) (rate int, discountCap float64, ok bool) {
	if !in.Dynamic {
		return 0, 0, false
	}
	discountCap = MaxCostDiscount(in.ArMaxCost, in.LocalFeeRate)
	if discountCap <= 0 {
		return 0, 0, true
	}
	target := float64(100 - in.ArInTarget)
	if target <= 0 {
		return 0, discountCap, true
	}
	out := 0.0
	if in.Capacity > 0 {
		out = float64(in.LocalBalance+in.PendingOutbound) * 100 / float64(in.Capacity)
	}
	fill := out / target
	if fill >= 1 {
		return 0, discountCap, true
	}
	return -int(math.Round(discountCap * (1 - math.Pow(fill, exponent)))), discountCap, true
}
