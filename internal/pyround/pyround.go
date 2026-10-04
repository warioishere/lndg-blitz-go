// Package pyround mirrors Python's two roundings to n decimals, which differ
// whenever x*10^n itself rounds (round(14315.15, 1): Round 14315.1, NumPy 14315.2).
package pyround

import (
	"math"
	"strconv"
)

// Round is Python's builtin round(x, n) on a float: the exact binary value
// rounded to n decimals, exact ties to even.
func Round(x float64, n int) float64 {
	r, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'f', n, 64), 64)
	return r
}

// NumPy is numpy's rounding (Series.round, np.round, round() on an
// np.float64): scale by 10^n, round half to even, scale back.
func NumPy(x float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.RoundToEven(x*p) / p
}
