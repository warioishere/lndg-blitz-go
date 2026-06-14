// Package rebalancer implements the auto-rebalance engine: queue manager,
// workers, saved-route selection, mission-control edge cache, node reputation,
// opportunity-cost checks, probing, RapidFire, auto_schedule, and autopilot.
package rebalancer

import (
	"sync"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Mission control constants.
const (
	failedEdgeExpiry       = 600 * time.Second // _FAILED_EDGE_EXPIRY = 600
	failedEdgeTolerancePpm = 50_000            // _FAILED_EDGE_TOLERANCE_PPM (5%)
	chanInfoTTL            = 300 * time.Second // _CHAN_INFO_TTL
)

// edgeKey identifies a directed (prev -> hop) edge in the failed-edge cache.
type edgeKey struct {
	prev string
	hop  string
}

type edgeEntry struct {
	ts         time.Time
	amountMsat int64
}

// missionControl tracks recently failed edges so that routes reusing them can be
// skipped. A mutex protects the shared cache across concurrent goroutine workers.
// now is injectable for tests.
type missionControl struct {
	mu    sync.Mutex
	edges map[edgeKey]edgeEntry
	now   func() time.Time
}

func newMissionControl() *missionControl {
	return &missionControl{edges: map[edgeKey]edgeEntry{}, now: time.Now}
}

// pruneFailedEdges removes expired entries from the failed-edge cache.
func (mc *missionControl) pruneFailedEdges() {
	now := mc.now()
	for k, e := range mc.edges {
		if now.Sub(e.ts) > failedEdgeExpiry {
			delete(mc.edges, k)
		}
	}
}

// markEdgeFailed records a (prev -> hop) edge as failed for the given amount.
func (mc *missionControl) markEdgeFailed(prevPubkey, hopPubkey string, amountMsat int64) {
	if prevPubkey == "" || hopPubkey == "" {
		return
	}
	mc.edges[edgeKey{prevPubkey, hopPubkey}] = edgeEntry{ts: mc.now(), amountMsat: amountMsat}
}

// recordRouteFailure caches the (prev -> failure-source) edge when a route
// fails with TEMPORARY_CHANNEL_FAILURE (code 15).
func (mc *missionControl) recordRouteFailure(route *lnrpc.Route, failure *lnrpc.Failure) {
	if route == nil || failure == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	code := int(failure.GetCode())
	fsi := int(failure.GetFailureSourceIndex())
	if code != 15 || fsi <= 0 {
		return
	}
	hops := route.GetHops()
	if fsi >= len(hops) {
		return
	}
	prev := hops[fsi-1]
	failed := hops[fsi]
	mc.markEdgeFailed(prev.GetPubKey(), failed.GetPubKey(), prev.GetAmtToForwardMsat())
}

// validateRoute returns true if no consecutive hop pair in the route matches a
// cached failed edge at a similar amount.
func (mc *missionControl) validateRoute(route *lnrpc.Route) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.pruneFailedEdges()
	if route == nil || len(route.GetHops()) < 2 {
		return true
	}
	hops := route.GetHops()
	for i := 0; i < len(hops)-1; i++ {
		key := edgeKey{hops[i].GetPubKey(), hops[i+1].GetPubKey()}
		cached, ok := mc.edges[key]
		if !ok {
			continue
		}
		failedAmtMsat := cached.amountMsat
		curAmtMsat := hops[i].GetAmtToForwardMsat()
		if curAmtMsat <= 0 || failedAmtMsat <= 0 {
			return false
		}
		denom := failedAmtMsat
		if curAmtMsat > denom {
			denom = curAmtMsat
		}
		diff := failedAmtMsat - curAmtMsat
		if diff < 0 {
			diff = -diff
		}
		diffPpm := diff * 1_000_000 / denom // integer floor division; always positive
		if diffPpm <= failedEdgeTolerancePpm {
			return false
		}
	}
	return true
}
