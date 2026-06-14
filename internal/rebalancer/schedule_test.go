package rebalancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

const tgtPeer = "02aabbccddeeff"

func schedOutChan() db.GuiChannel {
	return db.GuiChannel{
		ChanID: "777", RemotePubkey: "peerOut", IsActive: true, IsOpen: true,
		Capacity: 10_000_000, LocalBalance: 9_000_000, ArOutTarget: 50, HtlcCount: 0,
		ArSource: true, AutoRebalance: true, LocalFeeRate: 100,
	}
}

func schedInChan() db.GuiChannel {
	return db.GuiChannel{
		ChanID: "888", RemotePubkey: tgtPeer, IsActive: true, IsOpen: true,
		Capacity: 2_000_000, LocalBalance: 100_000, RemoteBalance: 1_900_000, ArInTarget: 50,
		AutoRebalance: true, RemoteDisabled: false,
		LocalFeeRate: 1000, ArMaxCost: 80, RemoteFeeRate: 10, ArAmtTarget: 500_000, Alias: "TargetAlias",
	}
}

func TestAutoScheduleDisabled(t *testing.T) {
	q := newFakeRebalQ() // AR-Enabled defaults to 0
	e := newEngine()
	got := e.autoSchedule(context.Background(), q, fixedNow())
	assert.Empty(t, got)
}

func TestAutoScheduleLoop2(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Enabled"] = "1"
	q.activeChans = []db.GuiChannel{schedOutChan(), schedInChan()}
	e := newEngine()
	got := e.autoSchedule(context.Background(), q, fixedNow())
	require.Len(t, got, 1)
	r := got[0]
	// target_fee_rate = min(500, int(1000*0.8)-100)=700->500; value=500000; fee=round(500*500000*1e-6,3)=250
	assert.Equal(t, int32(500_000), r.Value)
	assert.InDelta(t, 250.0, r.FeeLimit, 1e-9)
	assert.Equal(t, "[777]", r.OutgoingChanIds)
	assert.Equal(t, tgtPeer, r.LastHopPubkey)
	assert.Equal(t, "TargetAlias", r.TargetAlias)
	assert.Equal(t, int32(5), r.Duration)
	assert.Equal(t, int32(0), r.Status)
	require.Len(t, q.inserted, 1)
}

func TestAutoScheduleLoop1AllowedTargetQuotedFormat(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Enabled"] = "1"
	q.activeChans = []db.GuiChannel{schedOutChan(), schedInChan()}
	q.allowedTargets = []db.ListAllAllowedTargetsRow{{SourceChanID: "777", TargetPubkey: tgtPeer}}
	e := newEngine()
	got := e.autoSchedule(context.Background(), q, fixedNow())
	require.Len(t, got, 1)
	// Loop 1 (AllowedTarget): outgoing_chan_ids without quotes -> JSON-parseable.
	assert.Equal(t, "[777]", got[0].OutgoingChanIds)
	assert.Equal(t, tgtPeer, got[0].LastHopPubkey)
}

func TestAutoScheduleSkipsActiveTarget(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Enabled"] = "1"
	q.activeChans = []db.GuiChannel{schedOutChan(), schedInChan()}
	q.activePubkeys = []string{tgtPeer} // target already active -> excluded from inbound candidates
	e := newEngine()
	got := e.autoSchedule(context.Background(), q, fixedNow())
	assert.Empty(t, got)
}

func TestAutoScheduleWaitPeriodBlocks(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-Enabled"] = "1"
	q.activeChans = []db.GuiChannel{schedOutChan(), schedInChan()}
	// last rebalance status=3 (fail), stopped 1 min ago -> within wait_period(30) -> blocked.
	q.lastRebal = map[string]db.GetLastRebalanceForPubkeyRow{
		tgtPeer: {Status: 3, Stop: tsNow(fixedNow())},
	}
	e := newEngine()
	got := e.autoSchedule(context.Background(), q, fixedNow())
	assert.Empty(t, got)
}
