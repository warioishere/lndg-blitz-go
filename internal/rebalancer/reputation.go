package rebalancer

// calcSuccessRatio returns a Laplace-smoothed success ratio: (sc+1)/(sc+fc+2).
func calcSuccessRatio(successCount, failureCount int) float64 {
	return float64(successCount+1) / float64(successCount+failureCount+2)
}

// calcWeightedRatio returns the success ratio weighted by attempt count.
// weight defaults to 10 and represents prior pseudo-observations.
func calcWeightedRatio(successCount, failureCount, weight int) float64 {
	ratio := calcSuccessRatio(successCount, failureCount)
	total := successCount + failureCount
	if total+weight != 0 {
		return ratio * (float64(total) / float64(total+weight))
	}
	return ratio
}
