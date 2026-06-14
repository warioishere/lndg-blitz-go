package rebalancer

// lookupSourceFee looks up the source fee for a channel ID; returns 0 if absent.
func lookupSourceFee(m map[string]int, chanID string) int {
	return m[chanID] // missing key returns 0
}

// checkOpportunityCost returns (allowed, route_fee_ppm, max_route_ppm).
// max_route_fee = (target_fee * ar_max_cost%) - source_outbound_fee, capped at max_fee_rate.
func checkOpportunityCost(routeFeeMsat int64, amountSat, sourceFee, targetFeeRate, arMaxCost, maxFeeRate int) (bool, int, int) {
	maxRoutePpm := int(float64(targetFeeRate)*(float64(arMaxCost)/100)) - sourceFee
	if maxFeeRate < maxRoutePpm {
		maxRoutePpm = maxFeeRate
	}
	routeFeePpm := 0
	if amountSat > 0 {
		routeFeePpm = int((float64(routeFeeMsat) / (float64(amountSat) * 1000)) * 1000000)
	}
	return routeFeePpm <= maxRoutePpm, routeFeePpm, maxRoutePpm
}
