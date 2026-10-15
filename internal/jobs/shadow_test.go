package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func intp(v int) *int { return &v }

func TestIdleDropRate(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	fourDaysAgo, oneDayAgo := now.Add(-96*time.Hour), now.Add(-24*time.Hour)
	full := func(mod func(r *af.ChannelFeeRow)) *af.ChannelFeeRow {
		r := &af.ChannelFeeRow{ChanID: "c1", ArInTarget: 50, OutPercent: 90, NewRate: 2600, AvgRebalanceCost: intp(1900), LastForwardOut: &fourDaysAgo}
		if mod != nil {
			mod(r)
		}
		return r
	}
	for _, c := range []struct {
		name string
		row  *af.ChannelFeeRow
		rate int
		ok   bool
	}{
		{"full, idle 4 days: drop to cost + 10%", full(nil), 2090, true},
		{"never forwarded in the window counts as idle", full(func(r *af.ChannelFeeRow) { r.LastForwardOut = nil }), 2090, true},
		{"sold yesterday", full(func(r *af.ChannelFeeRow) { r.LastForwardOut = &oneDayAgo }), 0, false},
		{"below its outbound target (50%)", full(func(r *af.ChannelFeeRow) { r.OutPercent = 40 }), 0, false},
		{"no rebalance history", full(func(r *af.ChannelFeeRow) { r.AvgRebalanceCost = nil }), 0, false},
		{"AF already at the floor", full(func(r *af.ChannelFeeRow) { r.NewRate = 2090 }), 0, false},
	} {
		rate, ok := idleDropRate(c.row, now)
		assert.Equal(t, c.ok, ok, c.name)
		assert.Equal(t, c.rate, rate, c.name)
	}
}

// AF logs the idle drop for a full channel without sales and still applies its own rate.
func TestAutoFees_ShadowIdleDrop(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	q := &fakeAfQ{
		settings: map[string]string{"AF-Enabled": "1", "AF-CurveMode": "0"},
		channels: []db.GuiChannel{{
			ChanID: "c1", RemotePubkey: "peerA", IsOpen: true, IsActive: true, AutoFees: true,
			Capacity: 1000000, LocalBalance: 900000, RemoteBalance: 100000,
			LocalFeeRate: 2600, LocalBaseFee: 1000, LocalCltv: 40, ArInTarget: 50, ArMaxCost: 90, Alias: "peerA",
			FeesUpdated: pgtype.Timestamptz{Time: old, Valid: true}, FundingTxid: "abc",
		}},
		// rebalances at 1800 and 2000 ppm -> cost 1900, floor 2090
		pays: map[string][]db.RebalPaymentsForChannelRow{"c1": {{Fee: 180, Value: 100000}, {Fee: 200, Value: 100000}}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, AutoFees(context.Background(), q, client))

	require.Len(t, q.autofees, 1, "AF still applies its own step")
	require.Len(t, q.shadow, 1)
	s := q.shadow[0]
	assert.Equal(t, "idle_drop", s.Rule)
	assert.Equal(t, int32(1900), s.AvgRebalanceCost)
	assert.Equal(t, q.autofees[0].NewValue, s.CurrentValue, "current = the rate AF sets")
	assert.Equal(t, int32(2090), s.ProposedValue)
	assert.False(t, s.HoursSinceOut.Valid, "no outbound forward in the window")
}
