package af

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Differential test: verifies all four adjustment functions against
// reference outputs stored in testdata/af_adjust_cases.json.

type jsonSettings struct {
	Intensity            int     `json:"intensity"`
	Exponent             float64 `json:"exponent"`
	MaxStep              int     `json:"max_step"`
	PeerRateCheck        bool    `json:"peer_rate_check"`
	PeerRateLimit        int     `json:"peer_rate_limit"`
	DownScale            float64 `json:"downscale"`
	FlowWeight           float64 `json:"flow_weight"`
	InboundIntensity     int     `json:"inbound_intensity"`
	LowLiqLimit          int     `json:"lowliq_limit"`
	ExcessLimit          int     `json:"excess_limit"`
	Multiplier           int     `json:"multiplier"`
	FlowScale            float64 `json:"flow_scale"`
	HtlcBoostAmount      int     `json:"htlc_boost_amount"`
	HtlcBoostThreshold   int     `json:"htlc_boost_threshold"`
	BypassPeerRateOnHTLC bool    `json:"bypass_peer_rate_on_htlc"`
	BoostArOnly          bool    `json:"boost_ar_only"`
	LowLiqBoost          float64 `json:"lowliq_boost"`
	ExcessBoostEnabled   bool    `json:"excess_boost_enabled"`
	ExcessBoost          float64 `json:"excess_boost"`
}

func (j jsonSettings) toSettings() *Settings {
	return &Settings{
		Intensity:            j.Intensity,
		Exponent:             j.Exponent,
		MaxStep:              j.MaxStep,
		PeerRateCheck:        j.PeerRateCheck,
		PeerRateLimit:        j.PeerRateLimit,
		DownScale:            j.DownScale,
		FlowWeight:           j.FlowWeight,
		InboundIntensity:     j.InboundIntensity,
		LowLiqLimit:          j.LowLiqLimit,
		ExcessLimit:          j.ExcessLimit,
		Multiplier:           j.Multiplier,
		FlowScale:            j.FlowScale,
		HtlcBoostAmount:      j.HtlcBoostAmount,
		HtlcBoostThreshold:   j.HtlcBoostThreshold,
		BypassPeerRateOnHTLC: j.BypassPeerRateOnHTLC,
		BoostArOnly:          j.BoostArOnly,
		LowLiqBoost:          j.LowLiqBoost,
		ExcessBoostEnabled:   j.ExcessBoostEnabled,
		ExcessBoost:          j.ExcessBoost,
	}
}

type jsonCh struct {
	ArInTarget             int     `json:"ar_in_target"`
	OutPercent             int     `json:"out_percent"`
	NetRouted7day          float64 `json:"net_routed_7day"`
	RemoteFeeRate          int     `json:"remote_fee_rate"`
	RemoteInboundFeeRate   int     `json:"remote_inbound_fee_rate"`
	OverallOutPercent      float64 `json:"overall_out_percent"`
	FailedOutBoostInterval int     `json:"failed_out_boost_interval"`
	AutoRebalance          bool    `json:"auto_rebalance"`
	HoursSinceLastForward  float64 `json:"hours_since_last_forward"`
	LastForwardBefore      *bool   `json:"last_forward_before"`
	TotalAmtRoutedIn7day   int     `json:"total_amt_routed_in_7day"`
	TotalAmtRoutedOut7day  int     `json:"total_amt_routed_out_7day"`
	TotalRevenueAssist7day float64 `json:"total_revenue_assist_7day"`
	TotalRevenue7day       float64 `json:"total_revenue_7day"`
	GroupNetRouted7day     float64 `json:"group_net_routed_7day"`
}

type jsonGrp struct {
	PeerOutTarget          int     `json:"peer_out_target"`
	OverallOutPercent      float64 `json:"overall_out_percent"`
	TotalAmtRoutedIn7day   int     `json:"total_amt_routed_in_7day"`
	TotalAmtRoutedOut7day  int     `json:"total_amt_routed_out_7day"`
	GroupNetRouted7day     float64 `json:"group_net_routed_7day"`
	TotalRevenueAssist7day float64 `json:"total_revenue_assist_7day"`
	TotalRevenue7day       float64 `json:"total_revenue_7day"`
}

type jsonCase struct {
	Settings  jsonSettings `json:"settings"`
	Ch        jsonCh       `json:"ch"`
	Grp       jsonGrp      `json:"grp"`
	CurveOut  int          `json:"curve_out"`
	CurveIn   int          `json:"curve_in"`
	LegacyIn  int          `json:"legacy_in"`
	LegacyOut int          `json:"legacy_out"`
}

func TestAdjustmentsAgainstPython(t *testing.T) {
	raw, err := os.ReadFile("testdata/af_adjust_cases.json")
	require.NoError(t, err)
	var cases []jsonCase
	require.NoError(t, json.Unmarshal(raw, &cases))
	require.NotEmpty(t, cases)

	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	for i, c := range cases {
		s := c.Settings.toSettings()

		// last_forward_before: nil -> LastForward nil (no last forward); true -> last_forward
		// is after fees_updated; false -> before it.
		ch := &ChannelFeeRow{
			ArInTarget:             c.Ch.ArInTarget,
			OutPercent:             c.Ch.OutPercent,
			NetRouted7day:          c.Ch.NetRouted7day,
			RemoteFeeRate:          c.Ch.RemoteFeeRate,
			RemoteInboundFeeRate:   c.Ch.RemoteInboundFeeRate,
			OverallOutPercent:      c.Ch.OverallOutPercent,
			FailedOutBoostInterval: c.Ch.FailedOutBoostInterval,
			AutoRebalance:          c.Ch.AutoRebalance,
			HoursSinceLastForward:  c.Ch.HoursSinceLastForward,
			TotalAmtRoutedIn7day:   c.Ch.TotalAmtRoutedIn7day,
			TotalAmtRoutedOut7day:  c.Ch.TotalAmtRoutedOut7day,
			TotalRevenueAssist7day: c.Ch.TotalRevenueAssist7day,
			TotalRevenue7day:       c.Ch.TotalRevenue7day,
			GroupNetRouted7day:     c.Ch.GroupNetRouted7day,
		}
		switch {
		case c.Ch.LastForwardBefore == nil:
			ch.LastForward = nil
			ch.FeesUpdated = t0
		case *c.Ch.LastForwardBefore:
			lf := t0.Add(time.Hour)
			ch.LastForward = &lf
			ch.FeesUpdated = t0
		default:
			lf := t0
			ch.LastForward = &lf
			ch.FeesUpdated = t0.Add(time.Hour)
		}

		grp := &groupRow{
			PeerOutTarget:          c.Grp.PeerOutTarget,
			OverallOutPercent:      c.Grp.OverallOutPercent,
			TotalAmtRoutedIn7day:   c.Grp.TotalAmtRoutedIn7day,
			TotalAmtRoutedOut7day:  c.Grp.TotalAmtRoutedOut7day,
			GroupNetRouted7day:     c.Grp.GroupNetRouted7day,
			TotalRevenueAssist7day: c.Grp.TotalRevenueAssist7day,
			TotalRevenue7day:       c.Grp.TotalRevenue7day,
		}

		if got := s.computeCurveOutboundAdjustment(ch); got != c.CurveOut {
			t.Fatalf("case %d curve_out: got %d want %d\nsettings=%+v\nch=%+v", i, got, c.CurveOut, c.Settings, c.Ch)
		}
		if got := s.computeCurveInboundAdjustment(grp); got != c.CurveIn {
			t.Fatalf("case %d curve_in: got %d want %d\nsettings=%+v\ngrp=%+v", i, got, c.CurveIn, c.Settings, c.Grp)
		}
		if got := s.computeInboundAdjustment(grp); got != c.LegacyIn {
			t.Fatalf("case %d legacy_in: got %d want %d\nsettings=%+v\ngrp=%+v", i, got, c.LegacyIn, c.Settings, c.Grp)
		}
		if got := s.computeOutboundAdjustment(ch); got != c.LegacyOut {
			t.Fatalf("case %d legacy_out: got %d want %d\nsettings=%+v\nch=%+v", i, got, c.LegacyOut, c.Settings, c.Ch)
		}
	}
	t.Logf("verified %d cases against Python af.py", len(cases))
}
