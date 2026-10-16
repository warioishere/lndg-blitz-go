// Package rebalancer implements the auto-rebalance engine: queue manager,
// workers, saved-route selection, mission-control edge cache, route statistics,
// opportunity-cost checks, probing, RapidFire, auto_schedule, and autopilot.
package rebalancer

import (
	"sync"
	"time"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Mission control constants. A failed edge is blocked for failedEdgeExpiry;
// each further failure while it is still remembered doubles the block, up to
// failedEdgeMaxBan. An edge is forgotten failedEdgeMaxBan after its block ends.
const (
	failedEdgeExpiry       = 600 * time.Second // _FAILED_EDGE_EXPIRY = 600
	failedEdgeMaxBan       = 2 * time.Hour
	failedEdgeTolerancePpm = 50_000            // amounts up to 5% below the failed one are blocked too
	chanInfoTTL            = 300 * time.Second // _CHAN_INFO_TTL
)

// edgeFailureCodes are the failure codes that report a missing or unusable
// channel between the failing node and the next hop.
var edgeFailureCodes = map[int]bool{
	14: true, // CHANNEL_DISABLED
	15: true, // TEMPORARY_CHANNEL_FAILURE
	18: true, // UNKNOWN_NEXT_PEER
	21: true, // PERMANENT_CHANNEL_FAILURE
}

// edgeKey identifies a directed (prev -> hop) edge in the failed-edge cache.
type edgeKey struct {
	prev string
	hop  string
}

// edgeEntry is a failed edge: blocked until `until` for amounts from
// amountMsat (less the tolerance) upwards; fails counts the failures in a row.
type edgeEntry struct {
	until      time.Time
	amountMsat int64
	fails      int
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

// edgeBan is the block after the n-th failure in a row: failedEdgeExpiry
// doubled per further failure, capped at failedEdgeMaxBan.
func edgeBan(n int) time.Duration {
	d := failedEdgeExpiry
	for i := 1; i < n && d < failedEdgeMaxBan; i++ {
		d *= 2
	}
	return min(d, failedEdgeMaxBan)
}

// pruneFailedEdges forgets edges whose block ended more than failedEdgeMaxBan ago.
func (mc *missionControl) pruneFailedEdges() {
	now := mc.now()
	for k, e := range mc.edges {
		if now.Sub(e.until) > failedEdgeMaxBan {
			delete(mc.edges, k)
		}
	}
}

// markEdgeFailed records a (prev -> hop) edge as failed for the given amount.
// While the edge is still blocked, the smaller of the failed amounts counts.
func (mc *missionControl) markEdgeFailed(prevPubkey, hopPubkey string, amountMsat int64) {
	if prevPubkey == "" || hopPubkey == "" {
		return
	}
	now := mc.now()
	key := edgeKey{prevPubkey, hopPubkey}
	e, ok := mc.edges[key]
	if ok && now.Before(e.until) && e.amountMsat < amountMsat {
		amountMsat = e.amountMsat
	}
	e.fails++
	e.amountMsat = amountMsat
	e.until = now.Add(edgeBan(e.fails))
	mc.edges[key] = e
}

// recordRouteFailure caches the (prev -> failure-source) edge when a route
// fails with an edge failure code at a remote node.
func (mc *missionControl) recordRouteFailure(route *lnrpc.Route, failure *lnrpc.Failure) {
	if route == nil || failure == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	fsi := int(failure.GetFailureSourceIndex())
	if !edgeFailureCodes[int(failure.GetCode())] || fsi <= 0 {
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

// recordRouteSuccess forgets the failures of every edge a successful route used.
func (mc *missionControl) recordRouteSuccess(route *lnrpc.Route) {
	if route == nil {
		return
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()
	hops := route.GetHops()
	for i := 0; i+1 < len(hops); i++ {
		delete(mc.edges, edgeKey{hops[i].GetPubKey(), hops[i+1].GetPubKey()})
	}
}

// validateRoute returns true if no consecutive hop pair in the route matches a
// blocked edge at its failed amount or more (less the tolerance).
func (mc *missionControl) validateRoute(route *lnrpc.Route) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.pruneFailedEdges()
	if route == nil || len(route.GetHops()) < 2 {
		return true
	}
	now := mc.now()
	hops := route.GetHops()
	for i := 0; i < len(hops)-1; i++ {
		cached, ok := mc.edges[edgeKey{hops[i].GetPubKey(), hops[i+1].GetPubKey()}]
		if !ok || !now.Before(cached.until) {
			continue
		}
		curAmtMsat := hops[i].GetAmtToForwardMsat()
		if curAmtMsat <= 0 || cached.amountMsat <= 0 {
			return false
		}
		if curAmtMsat*1_000_000 >= cached.amountMsat*(1_000_000-failedEdgeTolerancePpm) {
			return false
		}
	}
	return true
}
