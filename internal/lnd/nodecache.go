// Package lnd provides a combined in-memory LRU and database cache for
// NodeInfo. It reads NODE_CACHE_EXPIRY_MINUTES and NODE_CACHE_MAX_ENTRIES from
// LocalSettings, reads and writes the gui_nodecache table, and calls
// GetNodeInfo with a 5-second timeout. On RPC failure it falls back to a stale
// cache entry or an empty NodeInfo, silently swallowing the error.
//
// The package-global in-memory cache is protected by a mutex because multiple
// daemon goroutines may access it concurrently within the same process.
package lnd

import (
	"container/list"
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Default cache parameters used when no LocalSettings override is present.
const (
	defaultExpiryMinutes = 60
	defaultMaxEntries    = 500
)

// nodeInfoGetter is the narrow subset of lnrpc.LightningClient that the cache
// requires. It allows the stub to be replaced with a mock in tests.
type nodeInfoGetter interface {
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
}

// cacheQuerier is the narrow subset of the sqlc querier used by the cache.
// db.Queries satisfies it; tests inject a fake implementation.
type cacheQuerier interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
}

// memEntry holds a single cached NodeInfo value together with its insertion timestamp.
type memEntry struct {
	pubkey    string
	info      *lnrpc.NodeInfo
	updatedAt time.Time
}

// lruCache is a mutex-protected LRU cache backed by a doubly-linked list and
// an index map. The front of the list holds the most recently used entry;
// the back holds the oldest (eviction candidate).
type lruCache struct {
	mu    sync.Mutex
	ll    *list.List               // *memEntry, Front = newest
	index map[string]*list.Element // pubkey -> Element
}

var memoryCache = &lruCache{ll: list.New(), index: map[string]*list.Element{}}

// intSettingOverride looks up a LocalSettings integer value by key. If the key
// is not found, cur is returned unchanged. If the stored value cannot be parsed
// as an integer, cur is returned unchanged. Any other database error propagates
// to the caller.
func intSettingOverride(ctx context.Context, q cacheQuerier, key string, cur int) (int, error) {
	s, err := q.GetLocalSetting(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return cur, nil
	}
	if err != nil {
		return cur, err
	}
	if v, convErr := strconv.Atoi(s.Value); convErr == nil {
		return v, nil
	}
	return cur, nil
}

// GetNodeInfoCached returns NodeInfo for the given pubkey, consulting the
// in-memory LRU cache first, then the database cache, and finally making a
// live RPC call if both caches are absent or stale. The expiry window and
// maximum number of in-memory entries can be overridden via LocalSettings
// (NODE_CACHE_EXPIRY_MINUTES, NODE_CACHE_MAX_ENTRIES); the defaults are 60
// minutes and 500 entries. On RPC failure the function falls back to a stale
// database entry if one exists, or returns an empty NodeInfo; the error is
// swallowed in both fallback cases.
func GetNodeInfoCached(ctx context.Context, q cacheQuerier, stub nodeInfoGetter, pubkey string) (*lnrpc.NodeInfo, error) {
	expiryMinutes := defaultExpiryMinutes
	maxEntries := defaultMaxEntries

	var err error
	if expiryMinutes, err = intSettingOverride(ctx, q, "NODE_CACHE_EXPIRY_MINUTES", expiryMinutes); err != nil {
		return nil, err
	}
	if maxEntries, err = intSettingOverride(ctx, q, "NODE_CACHE_MAX_ENTRIES", maxEntries); err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-time.Duration(expiryMinutes) * time.Minute)

	// --- In-memory lookup ---
	memoryCache.mu.Lock()
	if el, ok := memoryCache.index[pubkey]; ok {
		entry := el.Value.(*memEntry)
		if !entry.updatedAt.Before(cutoff) { // updated_at >= cutoff
			memoryCache.ll.MoveToFront(el) // LRU touch
			info := entry.info
			memoryCache.mu.Unlock()
			return info, nil
		}
		// Stale: remove from cache.
		memoryCache.ll.Remove(el)
		delete(memoryCache.index, pubkey)
	}
	memoryCache.mu.Unlock()

	// --- Database cache lookup (errors propagate to the caller) ---
	var cacheRow db.GuiNodecache
	haveCache := false
	row, dbErr := q.GetNodeCache(ctx, pubkey)
	if dbErr == nil {
		cacheRow = row
		haveCache = true
	} else if !errors.Is(dbErr, pgx.ErrNoRows) {
		return nil, dbErr
	}

	var info *lnrpc.NodeInfo
	if haveCache && !cacheRow.UpdatedAt.Time.Before(cutoff) {
		info = &lnrpc.NodeInfo{}
		if perr := protojson.Unmarshal(cacheRow.Data, info); perr != nil {
			return nil, perr
		}
	} else {
		// Attempt a live RPC fetch and store. On failure fall back to the
		// stale database entry if available, or return an empty NodeInfo.
		fetched, ferr := fetchAndStore(ctx, q, stub, pubkey)
		if ferr != nil {
			if haveCache {
				info = &lnrpc.NodeInfo{}
				if perr := protojson.Unmarshal(cacheRow.Data, info); perr != nil {
					return nil, perr
				}
			} else {
				info = &lnrpc.NodeInfo{}
			}
		} else {
			info = fetched
		}
	}

	// --- Insert into in-memory cache with LRU eviction ---
	memoryCache.mu.Lock()
	// Evict the oldest entry when the cache is at capacity. Guard against an
	// empty list to avoid a nil-pointer panic when maxEntries <= 0.
	if memoryCache.ll.Len() >= maxEntries {
		if back := memoryCache.ll.Back(); back != nil {
			oldest := back.Value.(*memEntry)
			memoryCache.ll.Remove(back)
			delete(memoryCache.index, oldest.pubkey)
		}
	}
	entry := &memEntry{pubkey: pubkey, info: info, updatedAt: time.Now()}
	memoryCache.index[pubkey] = memoryCache.ll.PushFront(entry)
	memoryCache.mu.Unlock()

	return info, nil
}

// fetchAndStore calls GetNodeInfo with a 5-second timeout and writes the
// result to the database cache via UpsertNodeCache. Returns (info, nil) on
// success, or (nil, err) if either the RPC or the upsert fails.
func fetchAndStore(ctx context.Context, q cacheQuerier, stub nodeInfoGetter, pubkey string) (*lnrpc.NodeInfo, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	info, err := stub.GetNodeInfo(rpcCtx, &lnrpc.NodeInfoRequest{PubKey: pubkey, IncludeChannels: false})
	if err != nil {
		return nil, err
	}
	data, err := protojson.Marshal(info)
	if err != nil {
		return nil, err
	}
	if err := q.UpsertNodeCache(ctx, db.UpsertNodeCacheParams{
		Pubkey:    pubkey,
		Data:      data,
		UpdatedAt: pgtype.Timestamptz{Time: time.Now(), Valid: true},
	}); err != nil {
		return nil, err
	}
	return info, nil
}

// ResetMemoryCache clears the process-global in-memory node cache. Useful in
// tests that inspect cache size, or to force a full refresh at runtime.
func ResetMemoryCache() {
	memoryCache.mu.Lock()
	defer memoryCache.mu.Unlock()
	memoryCache.ll = list.New()
	memoryCache.index = map[string]*list.Element{}
}

// CacheStats returns the number of entries and total serialized byte size of
// the in-memory node cache.
func CacheStats() (int, int) {
	memoryCache.mu.Lock()
	defer memoryCache.mu.Unlock()
	totalBytes := 0
	for el := memoryCache.ll.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*memEntry)
		totalBytes += proto.Size(entry.info)
	}
	return memoryCache.ll.Len(), totalBytes
}
