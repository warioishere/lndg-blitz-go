package lnd

import (
	"container/list"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// resetMemoryCache clears the package-global in-memory cache between tests.
func resetMemoryCache() {
	memoryCache.mu.Lock()
	defer memoryCache.mu.Unlock()
	memoryCache.ll = list.New()
	memoryCache.index = map[string]*list.Element{}
}

type fakeQuerier struct {
	settings        map[string]string
	nodeCache       map[string]db.GuiNodecache
	upserts         []db.UpsertNodeCacheParams
	getNodeCacheErr error
}

func (f *fakeQuerier) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}

func (f *fakeQuerier) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	if f.getNodeCacheErr != nil {
		return db.GuiNodecache{}, f.getNodeCacheErr
	}
	if v, ok := f.nodeCache[pubkey]; ok {
		return v, nil
	}
	return db.GuiNodecache{}, pgx.ErrNoRows
}

func (f *fakeQuerier) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error {
	f.upserts = append(f.upserts, arg)
	if f.nodeCache == nil {
		f.nodeCache = map[string]db.GuiNodecache{}
	}
	f.nodeCache[arg.Pubkey] = db.GuiNodecache{Pubkey: arg.Pubkey, Data: arg.Data, UpdatedAt: arg.UpdatedAt}
	return nil
}

type fakeStub struct {
	info  *lnrpc.NodeInfo
	err   error
	calls int
}

func (s *fakeStub) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.info, nil
}

func nodeInfo(pubkey, alias string) *lnrpc.NodeInfo {
	return &lnrpc.NodeInfo{
		Node:          &lnrpc.LightningNode{PubKey: pubkey, Alias: alias},
		NumChannels:   3,
		TotalCapacity: 1000,
	}
}

func TestGetNodeInfoCached_MemoryHitFresh(t *testing.T) {
	resetMemoryCache()
	q := &fakeQuerier{}
	stub := &fakeStub{info: nodeInfo("pk1", "alice")}
	ctx := context.Background()

	// First call: cache miss -> RPC + store.
	info1, err := GetNodeInfoCached(ctx, q, stub, "pk1")
	require.NoError(t, err)
	assert.Equal(t, "alice", info1.Node.Alias)
	assert.Equal(t, 1, stub.calls)
	assert.Len(t, q.upserts, 1)

	// Second call: in-memory hit (fresh) -> no additional RPC.
	info2, err := GetNodeInfoCached(ctx, q, stub, "pk1")
	require.NoError(t, err)
	assert.Same(t, info1, info2, "should return cached pointer")
	assert.Equal(t, 1, stub.calls, "no extra RPC on fresh memory hit")
}

func TestGetNodeInfoCached_DBHitFresh(t *testing.T) {
	resetMemoryCache()
	data, err := protojson.Marshal(nodeInfo("pk2", "bob"))
	require.NoError(t, err)
	q := &fakeQuerier{nodeCache: map[string]db.GuiNodecache{
		"pk2": {Pubkey: "pk2", Data: data, UpdatedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true}},
	}}
	stub := &fakeStub{info: nodeInfo("pk2", "should-not-be-used")}

	info, err := GetNodeInfoCached(context.Background(), q, stub, "pk2")
	require.NoError(t, err)
	assert.Equal(t, "bob", info.Node.Alias, "fresh DB cache used")
	assert.Equal(t, 0, stub.calls, "no RPC when DB cache fresh")
	assert.Len(t, q.upserts, 0)
}

func TestGetNodeInfoCached_DBStaleTriggersRPC(t *testing.T) {
	resetMemoryCache()
	data, _ := protojson.Marshal(nodeInfo("pk3", "old"))
	q := &fakeQuerier{nodeCache: map[string]db.GuiNodecache{
		"pk3": {Pubkey: "pk3", Data: data, UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(-2 * time.Hour), Valid: true}},
	}}
	stub := &fakeStub{info: nodeInfo("pk3", "new")}

	info, err := GetNodeInfoCached(context.Background(), q, stub, "pk3")
	require.NoError(t, err)
	assert.Equal(t, "new", info.Node.Alias, "stale DB cache -> fresh RPC")
	assert.Equal(t, 1, stub.calls)
	assert.Len(t, q.upserts, 1)
}

func TestGetNodeInfoCached_RPCFailFallsBackToStaleDB(t *testing.T) {
	resetMemoryCache()
	data, _ := protojson.Marshal(nodeInfo("pk4", "stale-but-returned"))
	q := &fakeQuerier{nodeCache: map[string]db.GuiNodecache{
		"pk4": {Pubkey: "pk4", Data: data, UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(-2 * time.Hour), Valid: true}},
	}}
	stub := &fakeStub{err: errors.New("rpc down")}

	info, err := GetNodeInfoCached(context.Background(), q, stub, "pk4")
	require.NoError(t, err, "RPC error is swallowed when a stale cache entry exists")
	assert.Equal(t, "stale-but-returned", info.Node.Alias)
	assert.Equal(t, 1, stub.calls)
}

func TestGetNodeInfoCached_RPCFailNoCacheReturnsEmpty(t *testing.T) {
	resetMemoryCache()
	q := &fakeQuerier{}
	stub := &fakeStub{err: errors.New("rpc down")}

	info, err := GetNodeInfoCached(context.Background(), q, stub, "pk5")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Nil(t, info.Node, "empty NodeInfo when RPC fails and no cache")
}

func TestGetNodeInfoCached_GetNodeCacheDBErrorPropagates(t *testing.T) {
	resetMemoryCache()
	q := &fakeQuerier{getNodeCacheErr: errors.New("db connection lost")}
	stub := &fakeStub{info: nodeInfo("pk6", "x")}

	_, err := GetNodeInfoCached(context.Background(), q, stub, "pk6")
	require.Error(t, err, "non-ErrNoRows DB error from GetNodeCache propagates")
}

func TestGetNodeInfoCached_LRUEviction(t *testing.T) {
	resetMemoryCache()
	q := &fakeQuerier{settings: map[string]string{"NODE_CACHE_MAX_ENTRIES": "2"}}
	ctx := context.Background()

	for _, pk := range []string{"a", "b", "c"} {
		stub := &fakeStub{info: nodeInfo(pk, pk)}
		_, err := GetNodeInfoCached(ctx, q, stub, pk)
		require.NoError(t, err)
	}

	memoryCache.mu.Lock()
	defer memoryCache.mu.Unlock()
	assert.Equal(t, 2, memoryCache.ll.Len(), "cache capped at max_entries=2")
	_, hasA := memoryCache.index["a"]
	_, hasC := memoryCache.index["c"]
	assert.False(t, hasA, "oldest entry 'a' evicted")
	assert.True(t, hasC, "newest entry 'c' kept")
}

func TestGetNodeInfoCached_ExpiryOverride(t *testing.T) {
	resetMemoryCache()
	// expiry=1min: a 2-minute-old DB entry is stale -> triggers RPC.
	data, _ := protojson.Marshal(nodeInfo("pk7", "old"))
	q := &fakeQuerier{
		settings:  map[string]string{"NODE_CACHE_EXPIRY_MINUTES": "1"},
		nodeCache: map[string]db.GuiNodecache{"pk7": {Pubkey: "pk7", Data: data, UpdatedAt: pgtype.Timestamptz{Time: time.Now().Add(-2 * time.Minute), Valid: true}}},
	}
	stub := &fakeStub{info: nodeInfo("pk7", "fresh")}

	info, err := GetNodeInfoCached(context.Background(), q, stub, "pk7")
	require.NoError(t, err)
	assert.Equal(t, "fresh", info.Node.Alias)
	assert.Equal(t, 1, stub.calls)
}

func TestCacheStats(t *testing.T) {
	resetMemoryCache()
	q := &fakeQuerier{}
	ctx := context.Background()
	_, err := GetNodeInfoCached(ctx, q, &fakeStub{info: nodeInfo("s1", "a")}, "s1")
	require.NoError(t, err)
	_, err = GetNodeInfoCached(ctx, q, &fakeStub{info: nodeInfo("s2", "b")}, "s2")
	require.NoError(t, err)

	n, bytes := CacheStats()
	assert.Equal(t, 2, n)
	assert.Greater(t, bytes, 0)
}
