package graphwatcher

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func policyUpdate(chanID uint64, adv, conn string, feePpm int64, maxHtlcMsat uint64) *lnrpc.ChannelEdgeUpdate {
	return &lnrpc.ChannelEdgeUpdate{ChanId: chanID, Capacity: 5_000_000, AdvertisingNode: adv, ConnectingNode: conn,
		RoutingPolicy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: feePpm, MaxHtlcMsat: maxHtlcMsat, InboundFeeRateMilliMsat: -50, LastUpdate: 1_700_000_000}}
}

func TestMarketUpdateRows(t *testing.T) {
	m := newMarketLog(time.Time{})
	m.setPeers([]string{"peerA", "peerB"})
	at := time.Unix(1_700_000_000, 0)

	// other node X sets its fee toward our peer A
	rows := m.updateRows(policyUpdate(7, "X", "peerA", 800, 1_000_000_000), at)
	require.Len(t, rows, 1)
	r := rows[0]
	assert.Equal(t, "7", r.ChanID)
	assert.Equal(t, "peerA", r.PeerPubkey)
	assert.Equal(t, "X", r.PolicyNode)
	assert.Equal(t, int64(800), r.FeePpm)
	assert.Equal(t, int64(1_000_000_000), r.MaxHtlcMsat)
	assert.Equal(t, int32(-50), r.InboundFeePpm)
	assert.Equal(t, "update", r.Source)
	assert.True(t, r.LastUpdate.Valid)

	// the same policy again is not written; a changed max_htlc is
	assert.Empty(t, m.updateRows(policyUpdate(7, "X", "peerA", 800, 1_000_000_000), at))
	assert.Len(t, m.updateRows(policyUpdate(7, "X", "peerA", 800, 500_000_000), at), 1)

	// a channel between two of our peers gives one row per peer
	both := m.updateRows(policyUpdate(8, "peerA", "peerB", 100, 1), at)
	require.Len(t, both, 2)
	assert.ElementsMatch(t, []string{"peerA", "peerB"}, []string{both[0].PeerPubkey, both[1].PeerPubkey})

	// channels without a peer of ours are ignored
	assert.Empty(t, m.updateRows(policyUpdate(9, "X", "Y", 1, 1), at))
}

type nodeInfoLN struct {
	fakeLN
	infos map[string]*lnrpc.NodeInfo
}

func (c nodeInfoLN) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return c.infos[in.GetPubKey()], nil
}

func TestMarketSnapshot(t *testing.T) {
	q := newFakeQ()
	q.openPeers = []string{"peerA"}
	now := time.Unix(1_700_000_000, 0).UTC()
	ln := nodeInfoLN{infos: map[string]*lnrpc.NodeInfo{"peerA": {Channels: []*lnrpc.ChannelEdge{
		{ChannelId: 11, Capacity: 2_000_000, Node1Pub: "X", Node2Pub: "peerA",
			Node1Policy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 900}, Node2Policy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 1}},
		{ChannelId: 12, Capacity: 1_000_000, Node1Pub: "peerA", Node2Pub: "Y", Node1Policy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 5}},
	}}}}
	d := Deps{Q: q, LN: ln, Now: func() time.Time { return now }}
	m := newMarketLog(now)
	d.refreshMarket(context.Background(), m) // no snapshot yet: lastSnapshot is the zero value
	require.Eventually(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return len(q.deletedBefore) == 1
	}, time.Second, 5*time.Millisecond)

	require.Len(t, q.market, 3) // two policies on channel 11, one on 12 (Y's is unknown)
	for _, r := range q.market {
		assert.Equal(t, "snapshot", r.Source)
		assert.Equal(t, "peerA", r.PeerPubkey)
	}
	assert.Equal(t, now.Add(-marketRetention), q.deletedBefore[0])

	// the snapshot seeds the last state: an unchanged update is not written again
	same := &lnrpc.ChannelEdgeUpdate{ChanId: 11, AdvertisingNode: "X", ConnectingNode: "peerA",
		RoutingPolicy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 900}}
	assert.Empty(t, m.updateRows(same, now))
}

func TestMarketTickSkipsRecentSnapshot(t *testing.T) {
	q := newFakeQ()
	now := time.Unix(1_700_000_000, 0).UTC()
	q.lastSnapshot = pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true}
	d := Deps{Q: q, LN: nodeInfoLN{}, Now: func() time.Time { return now }}
	m := newMarketLog(now)
	d.marketTick(context.Background(), m)
	m.mu.Lock()
	defer m.mu.Unlock()
	assert.False(t, m.snapshotting)
	assert.Empty(t, q.deletedBefore)
}
