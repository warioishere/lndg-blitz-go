package pyround

import (
	"math"
	"testing"
)

// expected values from CPython's round()
func TestRound(t *testing.T) {
	for _, c := range []struct {
		x    float64
		n    int
		want float64
	}{
		{14315.15, 1, 14315.1}, {72569.85, 1, 72569.9}, {-99.65, 1, -99.7},
		{2.675, 2, 2.67}, {505.4365, 3, 505.437}, {-692.3195, 3, -692.319},
		{0.125, 2, 0.12}, {1.0005, 3, 1.0}, {7, 3, 7},
	} {
		if got := Round(c.x, c.n); got != c.want {
			t.Errorf("Round(%v, %d) = %v, want %v", c.x, c.n, got, c.want)
		}
	}
	// numpy: np.round(14315.15, 1) = 14315.2, np.round(-99.65, 1) = -99.7
	if NumPy(14315.15, 1) != 14315.2 || NumPy(72569.85, 1) != 72569.8 {
		t.Error("NumPy must scale first")
	}
	if !math.IsInf(Round(math.Inf(1), 1), 1) || !math.IsNaN(Round(math.NaN(), 1)) {
		t.Error("inf/nan must pass through")
	}
}
