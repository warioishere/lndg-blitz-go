package rebalancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func TestAutoEnableDisabled(t *testing.T) {
	q := newFakeRebalQ() // AR-Autopilot defaults to 0
	autoEnable(context.Background(), q, fixedNow())
	assert.Empty(t, q.setAR)
	assert.Empty(t, q.autopilots)
}

func TestAutoEnableCase2Enable(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Autopilot"] = "1"
	// Inbound-heavy channel with AR off -> enable (oapD>iapD*1.10, inbound%>75).
	q.activeChans = []db.GuiChannel{{
		ChanID: "111", RemotePubkey: "peer1", IsActive: true, IsOpen: true,
		Capacity: 1_000_000, LocalBalance: 100_000, RemoteBalance: 900_000,
		ArOutTarget: 80, AutoRebalance: false, Alias: "Peer1",
	}}
	q.aggOut = []db.AggForwardsOutSinceRow{{ChanIDOut: "111", Cnt: 1, SumMsat: 5_000_000_000}} // oapD=5.0
	autoEnable(context.Background(), q, fixedNow())
	require.Len(t, q.setAR, 1)
	assert.Equal(t, "111", q.setAR[0].ChanID)
	assert.True(t, q.setAR[0].AutoRebalance)
	require.Len(t, q.autopilots, 1)
	assert.Equal(t, int32(0), q.autopilots[0].OldValue)
	assert.Equal(t, int32(1), q.autopilots[0].NewValue)
	assert.Equal(t, "Enabled", q.autopilots[0].Setting)
}

func TestAutoEnableCase3Disable(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Autopilot"] = "1"
	// Outbound-heavy channel with AR on -> disable (oapD<iapD*1.10, outbound%>75).
	q.activeChans = []db.GuiChannel{{
		ChanID: "222", RemotePubkey: "peer2", IsActive: true, IsOpen: true,
		Capacity: 1_000_000, LocalBalance: 900_000, RemoteBalance: 100_000,
		ArOutTarget: 80, AutoRebalance: true, Alias: "Peer2",
	}}
	q.aggIn = []db.AggForwardsInSinceRow{{ChanIDIn: "222", Cnt: 1, SumMsat: 5_000_000_000}} // iapD=5.0, oapD=0
	autoEnable(context.Background(), q, fixedNow())
	require.Len(t, q.setAR, 1)
	assert.Equal(t, "222", q.setAR[0].ChanID)
	assert.False(t, q.setAR[0].AutoRebalance)
	require.Len(t, q.autopilots, 1)
	assert.Equal(t, int32(1), q.autopilots[0].OldValue)
	assert.Equal(t, int32(0), q.autopilots[0].NewValue)
}

func TestAutoEnable100TargetSkipped(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Autopilot"] = "1"
	q.activeChans = []db.GuiChannel{{
		ChanID: "333", RemotePubkey: "peer3", IsActive: true, IsOpen: true,
		Capacity: 1_000_000, LocalBalance: 900_000, RemoteBalance: 100_000,
		ArOutTarget: 100, AutoRebalance: true, Alias: "Loop",
	}}
	q.aggIn = []db.AggForwardsInSinceRow{{ChanIDIn: "333", Cnt: 1, SumMsat: 5_000_000_000}}
	autoEnable(context.Background(), q, fixedNow())
	assert.Empty(t, q.setAR) // 100% out-target with AR on -> skip-log only, no change
}
