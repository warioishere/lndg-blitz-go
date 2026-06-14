package af

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Differential test for the rate pipeline against reference outputs stored in
// testdata/af_rate_cases.json.

type jsonRateSettings struct {
	FlpEnabledGlobal bool `json:"flp_enabled_global"`
	FlpSafetyGlobal  int  `json:"flp_safety_global"`
	Increment        int  `json:"increment"`
	MinRate          int  `json:"min_rate"`
	MaxRate          int  `json:"max_rate"`
}

type jsonRateIn struct {
	LocalFeeRate        int      `json:"local_fee_rate"`
	Adjustment          int      `json:"adjustment"`
	InboundAdjustment   int      `json:"inbound_adjustment"`
	ArMaxCost           int      `json:"ar_max_cost"`
	LocalInboundFeeRate int      `json:"local_inbound_fee_rate"`
	FlpEnabled          bool     `json:"flp_enabled"`
	FlpSafety           int      `json:"flp_safety"`
	AvgRebalanceCost    *float64 `json:"avg_rebalance_cost"`
}

type jsonRateCase struct {
	Settings          jsonRateSettings `json:"settings"`
	In                jsonRateIn       `json:"in"`
	NewRate           float64          `json:"new_rate"`
	Adjustment        float64          `json:"adjustment"`
	CostFloor         float64          `json:"cost_floor"`
	NewInboundRate    float64          `json:"new_inbound_rate"`
	InboundAdjustment float64          `json:"inbound_adjustment"`
}

func TestRatePipelineAgainstPython(t *testing.T) {
	raw, err := os.ReadFile("testdata/af_rate_cases.json")
	require.NoError(t, err)
	var cases []jsonRateCase
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)

	const eps = 1e-9
	for i, c := range cases {
		s := &Settings{
			FlpEnabledGlobal: c.Settings.FlpEnabledGlobal,
			FlpSafetyGlobal:  c.Settings.FlpSafetyGlobal,
			Increment:        c.Settings.Increment,
			MinRate:          c.Settings.MinRate,
			MaxRate:          c.Settings.MaxRate,
		}
		r := &ChannelFeeRow{
			LocalFeeRate:        c.In.LocalFeeRate,
			Adjustment:          float64(c.In.Adjustment),
			InboundAdjustment:   float64(c.In.InboundAdjustment),
			ArMaxCost:           c.In.ArMaxCost,
			LocalInboundFeeRate: c.In.LocalInboundFeeRate,
			FlpEnabled:          c.In.FlpEnabled,
			FlpSafety:           c.In.FlpSafety,
		}
		if c.In.AvgRebalanceCost != nil {
			v := int(*c.In.AvgRebalanceCost)
			r.AvgRebalanceCost = &v
		}

		s.applyOutboundRate(r)
		s.applyInboundRate(r)

		check := func(name string, got, want float64) {
			if d := got - want; d > eps || d < -eps {
				t.Fatalf("case %d %s: got %v want %v\nsettings=%+v\nin=%+v", i, name, got, want, c.Settings, c.In)
			}
		}
		check("new_rate", r.NewRate, c.NewRate)
		check("adjustment", r.Adjustment, c.Adjustment)
		check("cost_floor", r.CostFloor, c.CostFloor)
		check("new_inbound_rate", r.NewInboundRate, c.NewInboundRate)
		check("inbound_adjustment", r.InboundAdjustment, c.InboundAdjustment)
	}
	t.Logf("verified %d rate cases against Python af.py", len(cases))
}
