package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

type fakePeerQ struct {
	peers     map[string]db.GuiPeer
	inactive  []string
	inserted  []db.InsertPeerParams
	updated   []db.UpdatePeerDataParams
	staleArg  []string
	connSet   []db.SetPeerConnectedParams
	reconnSet []db.SetPeerLastReconnectedParams
	aliasSet  []db.UpdatePeerAliasParams
	noAlias   []db.GuiPeer
}

func (f *fakePeerQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakePeerQ) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	return db.GuiNodecache{}, pgx.ErrNoRows
}
func (f *fakePeerQ) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error {
	return nil
}
func (f *fakePeerQ) GetPeer(ctx context.Context, pubkey string) (db.GuiPeer, error) {
	if p, ok := f.peers[pubkey]; ok {
		return p, nil
	}
	return db.GuiPeer{}, pgx.ErrNoRows
}
func (f *fakePeerQ) InsertPeer(ctx context.Context, arg db.InsertPeerParams) error {
	f.inserted = append(f.inserted, arg)
	return nil
}
func (f *fakePeerQ) UpdatePeerData(ctx context.Context, arg db.UpdatePeerDataParams) error {
	f.updated = append(f.updated, arg)
	return nil
}
func (f *fakePeerQ) DisconnectStalePeers(ctx context.Context, pubkeys []string) error {
	f.staleArg = pubkeys
	return nil
}
func (f *fakePeerQ) ListPeersNoAlias(ctx context.Context) ([]db.GuiPeer, error) {
	return f.noAlias, nil
}
func (f *fakePeerQ) UpdatePeerAlias(ctx context.Context, arg db.UpdatePeerAliasParams) error {
	f.aliasSet = append(f.aliasSet, arg)
	return nil
}
func (f *fakePeerQ) SetPeerConnected(ctx context.Context, arg db.SetPeerConnectedParams) error {
	f.connSet = append(f.connSet, arg)
	return nil
}
func (f *fakePeerQ) SetPeerLastReconnected(ctx context.Context, arg db.SetPeerLastReconnectedParams) error {
	f.reconnSet = append(f.reconnSet, arg)
	return nil
}
func (f *fakePeerQ) ListInactivePeerPubkeys(ctx context.Context) ([]string, error) {
	return f.inactive, nil
}

type fakePeerClient struct {
	peers      []*lnrpc.Peer
	nodeAlias  string
	nodeAddr   string
	connectReq []*lnrpc.ConnectPeerRequest
	disconnect []string
}

func (c *fakePeerClient) ListPeers(ctx context.Context, in *lnrpc.ListPeersRequest, opts ...grpc.CallOption) (*lnrpc.ListPeersResponse, error) {
	return &lnrpc.ListPeersResponse{Peers: c.peers}, nil
}
func (c *fakePeerClient) ConnectPeer(ctx context.Context, in *lnrpc.ConnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.ConnectPeerResponse, error) {
	c.connectReq = append(c.connectReq, in)
	return &lnrpc.ConnectPeerResponse{}, nil
}
func (c *fakePeerClient) DisconnectPeer(ctx context.Context, in *lnrpc.DisconnectPeerRequest, opts ...grpc.CallOption) (*lnrpc.DisconnectPeerResponse, error) {
	c.disconnect = append(c.disconnect, in.PubKey)
	return &lnrpc.DisconnectPeerResponse{}, nil
}
func (c *fakePeerClient) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	node := &lnrpc.LightningNode{Alias: c.nodeAlias}
	if c.nodeAddr != "" {
		node.Addresses = []*lnrpc.NodeAddress{{Addr: c.nodeAddr}}
	}
	return &lnrpc.NodeInfo{Node: node}, nil
}

func TestUpdatePeers_CreateUpdateStale(t *testing.T) {
	q := &fakePeerQ{peers: map[string]db.GuiPeer{
		"p1": {Pubkey: "p1", Alias: pgtype.Text{String: "old", Valid: true}},
		"px": {Pubkey: "px", Connected: true}, // not in ListPeers -> stale
	}}
	client := &fakePeerClient{
		nodeAlias: "Alice",
		peers: []*lnrpc.Peer{
			{PubKey: "p1", Address: "1.2.3.4:9735", SatSent: 100, SatRecv: 50, Inbound: false, PingTime: 5000},
			{PubKey: "p2", Address: "5.6.7.8:9735", PingTime: 3000},
		},
	}
	require.NoError(t, UpdatePeers(context.Background(), q, client))

	require.Len(t, q.updated, 1)
	assert.Equal(t, "p1", q.updated[0].Pubkey)
	assert.Equal(t, "Alice", q.updated[0].Alias.String, "alias refreshed from node info")
	assert.Equal(t, int64(5), q.updated[0].PingTime) // round(5000/1000)
	require.Len(t, q.inserted, 1)
	assert.Equal(t, "p2", q.inserted[0].Pubkey)
	assert.Equal(t, []string{"p1", "p2"}, q.staleArg)
}

func TestReconnectPeers_TimeGate(t *testing.T) {
	recent := time.Now().Add(-1 * time.Minute) // < 2 min -> skip
	old := time.Now().Add(-10 * time.Minute)   // > 2 min -> reconnect
	q := &fakePeerQ{
		inactive: []string{"recent", "old"},
		peers: map[string]db.GuiPeer{
			"recent": {Pubkey: "recent", Address: "1.1.1.1:9735", LastReconnected: pgtype.Timestamptz{Time: recent, Valid: true}},
			"old":    {Pubkey: "old", Address: "2.2.2.2:9735", LastReconnected: pgtype.Timestamptz{Time: old, Valid: true}},
		},
	}
	client := &fakePeerClient{nodeAddr: "9.9.9.9:9735"}
	require.NoError(t, ReconnectPeers(context.Background(), q, client))

	// only "old" reconnects
	require.Len(t, q.reconnSet, 1)
	assert.Equal(t, "old", q.reconnSet[0].Pubkey)
	require.NotEmpty(t, client.connectReq)
	assert.Equal(t, "old", client.connectReq[0].Addr.Pubkey)
	assert.Equal(t, "9.9.9.9:9735", client.connectReq[0].Addr.Host, "uses graph address")
}
