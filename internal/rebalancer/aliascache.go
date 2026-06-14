package rebalancer

import (
	"context"
	"fmt"
	"sync"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

const aliasCacheTTL = 300 * time.Second // _ALIAS_CACHE_TTL

// aliasQuerier is the DB subset required by the alias cache.
type aliasQuerier interface {
	ListChannelAliases(ctx context.Context) ([]db.ListChannelAliasesRow, error)
}

// aliasCache stores a chan_id -> alias mapping, populated on demand with a TTL.
// A mutex serialises concurrent access.
// now is injectable for testing.
type aliasCache struct {
	mu       sync.Mutex
	aliases  map[string]string
	loadedAt time.Time
	now      func() time.Time
}

func newAliasCache() *aliasCache {
	return &aliasCache{aliases: map[string]string{}, now: time.Now}
}

// refresh reloads the alias map from the database.
func (a *aliasCache) refresh(ctx context.Context, q aliasQuerier) error {
	rows, err := q.ListChannelAliases(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r.ChanID] = r.Alias
	}
	a.mu.Lock()
	a.aliases = m
	a.loadedAt = a.now()
	a.mu.Unlock()
	return nil
}

// ensure refreshes the cache if it is empty or the TTL has elapsed.
func (a *aliasCache) ensure(ctx context.Context, q aliasQuerier) error {
	a.mu.Lock()
	stale := len(a.aliases) == 0 || a.now().Sub(a.loadedAt) > aliasCacheTTL
	a.mu.Unlock()
	if stale {
		return a.refresh(ctx, q)
	}
	return nil
}

// alias returns the alias for a channel ID, or "?" if not found.
func (a *aliasCache) alias(cid string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v, ok := a.aliases[cid]; ok {
		return v
	}
	return "?"
}

// label returns a formatted "{cid} ({alias})" string.
func (a *aliasCache) label(cid string) string {
	return fmt.Sprintf("%s (%s)", cid, a.alias(cid))
}
