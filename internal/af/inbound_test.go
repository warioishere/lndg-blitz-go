package af

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// LNBiG-like channel: outbound 2321 ppm, offset -100, max cost 93% -> cap 2158.53,
// outbound target 50% (iTarget 50), curve exponent 2.
func TestInboundOffsetRate(t *testing.T) {
	ch := func(local int64) InboundInput {
		return InboundInput{LocalFeeRate: 2321, InboundOffset: -100, ArMaxCost: 93, ArInTarget: 50,
			Capacity: 20_000_000, LocalBalance: local}
	}
	for _, c := range []struct {
		name  string
		in    InboundInput
		rate  int
		ok    bool
		capPP float64
	}{
		{"empty: full cap", ch(0), -2159, true, 2158.53},
		{"25% out, half way: 75% of the cap", ch(5_000_000), -1619, true, 2158.53},
		{"37.5% out: 44%", ch(7_500_000), -944, true, 2158.53},
		{"45% out: 19%", ch(9_000_000), -410, true, 2158.53},
		{"at the 50% target: no discount", ch(10_000_000), 0, true, 2158.53},
		{"above the target: no discount", ch(19_600_000), 0, true, 2158.53},
		{"no offset: not managed", InboundInput{LocalFeeRate: 2321, ArMaxCost: 93, ArInTarget: 50, Capacity: 1}, 0, false, 0},
	} {
		rate, capPP, ok := InboundOffsetRate(c.in, 2)
		assert.Equal(t, c.ok, ok, c.name)
		assert.Equal(t, c.rate, rate, c.name)
		assert.InDelta(t, c.capPP, capPP, 0.01, c.name)
	}

	// the offset caps below max cost: fee 200, offset -100 -> cap 100 (max cost 95% = 190)
	rate, capPP, _ := InboundOffsetRate(InboundInput{LocalFeeRate: 200, InboundOffset: -100, ArMaxCost: 95, ArInTarget: 50, Capacity: 1}, 2)
	assert.Equal(t, -100, rate)
	assert.InDelta(t, 100, capPP, 1e-9)
	// pending outbound counts as outbound
	rate, _, _ = InboundOffsetRate(InboundInput{LocalFeeRate: 2321, InboundOffset: -100, ArMaxCost: 93, ArInTarget: 50,
		Capacity: 20_000_000, LocalBalance: 4_000_000, PendingOutbound: 6_000_000}, 2)
	assert.Equal(t, 0, rate)
	// offset larger than the fee: no discount at all
	rate, _, ok := InboundOffsetRate(InboundInput{LocalFeeRate: 50, InboundOffset: -100, ArMaxCost: 95, ArInTarget: 50, Capacity: 1}, 2)
	assert.True(t, ok)
	assert.Equal(t, 0, rate)
}
