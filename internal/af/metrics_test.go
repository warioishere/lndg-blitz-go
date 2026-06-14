package af

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intPtr(v int) *int { return &v }

func TestComputeChannelMetricsAndGroups(t *testing.T) {
	now := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	s := &Settings{
		UpdateHours:      24,
		InboundIntensity: 20,
		Exponent:         2.0,
		MaxStep:          100,
		CurveMode:        true,
	}

	lfOut1 := now.Add(-2 * time.Hour)
	rows := []*ChannelFeeRow{
		{
			ChanID: "1", RemotePubkey: "A", Capacity: 1000000,
			LocalBalance: 600000, RemoteBalance: 400000, PendingOutbound: 0, PendingInbound: 0,
			ArInTarget: 30, FeesUpdated: now.Add(-48 * time.Hour),
		},
		{
			ChanID: "2", RemotePubkey: "A", Capacity: 2000000,
			LocalBalance: 200000, RemoteBalance: 1700000, PendingOutbound: 100000, PendingInbound: 0,
			ArInTarget: 50, FeesUpdated: now.Add(-1 * time.Hour),
		},
	}
	agg := &aggregates{
		avgRebalanceCost:  map[string]*int{"1": intPtr(50)},
		amtRoutedIn7day:   map[string]int{"1": 100000},
		amtRoutedOut7day:  map[string]int{"1": 300000},
		revenueAssist7day: map[string]float64{"1": 4.0},
		revenue7day:       map[string]float64{"1": 12.5},
		failedOut1day:     map[string]int{"1": 3},
		failedOutBoost:    map[string]int{},
		amtRoutedIn1day:   map[string]int{},
		amtRoutedIn4h:     map[string]int{},
		amtRoutedOut4h:    map[string]int{},
		lastForwardOut:    map[string]time.Time{"1": lfOut1},
		lastForwardIn:     map[string]time.Time{},
	}

	computeChannelMetrics(s, rows, agg, now)

	ch1, ch2 := rows[0], rows[1]
	// ch1
	assert.Equal(t, int64(600000), ch1.LocalBalance) // pending 0
	assert.Equal(t, 60, ch1.OutPercent)
	assert.Equal(t, 40, ch1.InPercent)
	assert.InDelta(t, 0.2, ch1.NetRouted7day, 1e-9)
	assert.True(t, ch1.Eligible, "48h > 24h update window")
	require.NotNil(t, ch1.LastForward)
	assert.InDelta(t, 2.0, ch1.HoursSinceLastForward, 1e-9)
	assert.Equal(t, 3, ch1.FailedOut1day)
	assert.InDelta(t, 12.5, ch1.Revenue7day, 1e-9)
	assert.InDelta(t, 4.0, ch1.RevenueAssist7day, 1e-9)
	require.NotNil(t, ch1.AvgRebalanceCost)
	assert.Equal(t, 50, *ch1.AvgRebalanceCost)

	// ch2: local 200000 + pending 100000 = 300000 -> out 15%
	assert.Equal(t, int64(300000), ch2.LocalBalance)
	assert.Equal(t, 15, ch2.OutPercent)
	assert.False(t, ch2.Eligible, "1h < 24h window")
	assert.Nil(t, ch2.LastForward)
	assert.InDelta(t, 99999, ch2.HoursSinceLastForward, 1e-9)
	assert.Nil(t, ch2.AvgRebalanceCost, "no entry -> None")

	// Groups
	groups := s.computeGroups(rows)
	gA := groups["A"]
	require.NotNil(t, gA)
	assert.Equal(t, int64(900000), gA.TotalLocalBalance) // 600000 + 300000 (updated)
	assert.Equal(t, int64(3000000), gA.TotalCapacity)
	assert.InDelta(t, 30.0, gA.OverallOutPercent, 1e-9) // unrounded
	assert.InDelta(t, float64(300000-100000)/3000000, gA.GroupNetRouted7day, 1e-9)
	assert.Equal(t, 70, gA.PeerOutTarget) // 100 - min(30,50)
	// curve inbound: 20 * 0.4^2 = 3.2 -> round -> 3
	assert.Equal(t, 3, gA.InboundAdjustment)

	// Merge
	mergeGroupsToChannels(rows, groups)
	assert.InDelta(t, 30.0, ch1.OverallOutPercent, 1e-9)
	assert.Equal(t, 3.0, ch1.InboundAdjustment)
	assert.Equal(t, 3.0, ch2.InboundAdjustment)
}

func TestRound1BankersRounding(t *testing.T) {
	// Rounds half-to-even on 1 decimal place.
	assert.InDelta(t, 0.2, round1(0.25), 1e-9) // 2.5 -> 2 (even)
	assert.InDelta(t, 0.4, round1(0.35), 1e-9) // 3.5 -> 4 (even)
	assert.InDelta(t, 0.2, round1(0.2), 1e-9)
}

func TestMaxTimeNaTSkip(t *testing.T) {
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Hour)
	assert.Equal(t, &t1, maxTime(&t0, &t1))
	assert.Equal(t, &t1, maxTime(&t1, &t0))
	assert.Equal(t, &t0, maxTime(&t0, nil))
	assert.Equal(t, &t1, maxTime(nil, &t1))
	assert.Nil(t, maxTime(nil, nil))
}
