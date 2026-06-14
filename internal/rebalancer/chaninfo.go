package rebalancer

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// chanInfoClient is the GetChanInfo subset required by maxAmountOnRouteMsat.
type chanInfoClient interface {
	GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error)
}

// selfPubkeyClient is the GetInfo subset required by the self-pubkey cache.
type selfPubkeyClient interface {
	GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error)
}

// selfPubkeyCache fetches and caches the node's own identity pubkey on first use.
// A mutex serialises concurrent access.
type selfPubkeyCache struct {
	mu  sync.Mutex
	pk  string
	set bool
}

func newSelfPubkeyCache() *selfPubkeyCache { return &selfPubkeyCache{} }

// get returns the cached pubkey, fetching it via GetInfo on the first call.
// A GetInfo error is propagated to the caller.
func (s *selfPubkeyCache) get(ctx context.Context, stub selfPubkeyClient) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.set {
		resp, err := stub.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if err != nil {
			return "", err
		}
		s.pk = resp.GetIdentityPubkey()
		s.set = true
	}
	return s.pk, nil
}

type chanInfoEntry struct {
	ts   time.Time
	info *lnrpc.ChannelEdge
}

// chanInfoCache caches channel edge info keyed by chan_id with a TTL of chanInfoTTL.
// A mutex serialises concurrent access.
type chanInfoCache struct {
	mu    sync.Mutex
	cache map[uint64]chanInfoEntry
	now   func() time.Time
}

func newChanInfoCache() *chanInfoCache {
	return &chanInfoCache{cache: map[uint64]chanInfoEntry{}, now: time.Now}
}

// maxAmountOnRouteMsat returns the smallest max_htlc_msat on the forwarding side
// across all hops of the given route. Returns 0 if no constraint is found or on error.
func (c *chanInfoCache) maxAmountOnRouteMsat(ctx context.Context, stub chanInfoClient, route *lnrpc.Route) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var capMsat int64
	hasCap := false
	now := c.now()
	for _, hop := range route.GetHops() {
		chanID := hop.GetChanId()
		var info *lnrpc.ChannelEdge
		if cached, ok := c.cache[chanID]; ok && now.Sub(cached.ts) < chanInfoTTL {
			info = cached.info
		} else {
			got, err := stub.GetChanInfo(ctx, &lnrpc.ChanInfoRequest{ChanId: chanID})
			if err != nil {
				continue
			}
			info = got
			c.cache[chanID] = chanInfoEntry{ts: now, info: info}
		}
		// Select the policy of the side that forwards toward hop.pub_key.
		var policy *lnrpc.RoutingPolicy
		if hop.GetPubKey() == info.GetNode2Pub() {
			policy = info.GetNode1Policy()
		} else {
			policy = info.GetNode2Policy()
		}
		mh := int64(policy.GetMaxHtlcMsat())
		if mh > 0 && (!hasCap || mh < capMsat) {
			capMsat = mh
			hasCap = true
		}
	}
	if !hasCap {
		return 0
	}
	return capMsat
}
