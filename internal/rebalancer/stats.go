package rebalancer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Route statistics from the attempts in route_attempt_log (every route the
// rebalancer, the route prober and the graph watcher sent over), rebuilt every
// statsRefresh from statsWindow of attempts and updated live after each attempt
// of this process.
//
// Success chance: per directed node pair the share of attempts that crossed
// it, from the attempts of statsWindow. A paid route counts for every pair, a
// failed route for the pairs before the failing node and against the pair
// after it; a failure without a failing node counts against every pair. A pair
// with few attempts is pulled towards the average of all pairs (probPrior
// attempts at that average), a pair without attempts gets the average. A
// route's chance is the product of its pairs.
//
// Slow pairs: an attempt's duration is spread evenly over its hops (on a
// failure over the hops up to the failing node). A pair with at least
// latencyMinSamples such shares, used within latencyRecent, whose 90th
// percentile is above AR-LatencyBanFactor times the 90th percentile of all
// shares, is banned for latencyBan; a rebuild that finds it slow again extends
// the ban.
const (
	statsRefresh      = 30 * time.Minute
	statsWindow       = 72 * time.Hour
	probPrior         = 1.0
	defaultPairProb   = 0.5 // average while no attempt is known
	minRouteProb      = 0.002
	sureRouteProb     = 0.5
	latencyMinSamples = 10
	latencyRecent     = 24 * time.Hour
	latencyBan        = 4 * time.Hour
)

type statsQuerier interface {
	settingsQuerier
	ListSentRouteAttempts(ctx context.Context, timestamp pgtype.Timestamptz) ([]db.ListSentRouteAttemptsRow, error)
}

// String shortens the pair's pubkeys for the log.
func (e pathEdge) String() string {
	short := func(pk string) string { return pk[:min(len(pk), 12)] }
	return short(e.from) + " -> " + short(e.to)
}

// pairStat is the evidence of one directed node pair.
type pairStat struct {
	ok, n    float64
	lastUsed time.Time
	hopMs    []float64
}

// routeStats is shared by all workers of the rebalancer.
type routeStats struct {
	mu       sync.Mutex
	built    time.Time
	pairs    map[pathEdge]*pairStat
	okAll    float64
	nAll     float64
	banned   map[pathEdge]time.Time
	globalMs float64 // 90th percentile of all hop shares at the last rebuild
}

func newRouteStats() *routeStats {
	return &routeStats{pairs: map[pathEdge]*pairStat{}, banned: map[pathEdge]time.Time{}}
}

// RouteStats are the route statistics for users outside the rebalancer (the
// route scout), built from the same attempts.
type RouteStats struct{ s *routeStats }

// StatsQuerier is the DB access RouteStats.Refresh needs.
type StatsQuerier = statsQuerier

// NewRouteStats returns empty route statistics.
func NewRouteStats() RouteStats { return RouteStats{newRouteStats()} }

// Refresh rebuilds the statistics when they are older than statsRefresh.
func (r RouteStats) Refresh(ctx context.Context, q StatsQuerier, now time.Time) {
	r.s.refresh(ctx, q, now)
}

// Observe adds one sent attempt at once (see routeStats.observe).
func (r RouteStats) Observe(path, outcome string, hop, durationMs int, at time.Time) {
	r.s.observe(path, outcome, hop, durationMs, at)
}

// RouteProb returns the success chance of a path (hop pubkeys joined by "-").
func (r RouteStats) RouteProb(path string) float64 { return r.s.routeProb(path) }

// BannedPairs returns the slow node pairs banned at now as (from, to) pubkeys.
func (r RouteStats) BannedPairs(now time.Time) [][2]string {
	var out [][2]string
	for _, e := range r.s.bannedPairs(now) {
		out = append(out, [2]string{e.from, e.to})
	}
	return out
}

// latencyBanFactor reads AR-LatencyBanFactor (default 2, 0 turns the bans off).
func latencyBanFactor(ctx context.Context, q settingsQuerier) float64 {
	v, err := getLocalSettingStr(ctx, q, "AR-LatencyBanFactor", "2")
	if err != nil {
		return 2
	}
	var f float64
	if _, serr := fmt.Sscan(v, &f); serr != nil {
		return 2
	}
	return f
}

// refresh rebuilds the statistics when they are older than statsRefresh. On
// error it logs and keeps the previous ones.
func (s *routeStats) refresh(ctx context.Context, q statsQuerier, now time.Time) {
	s.mu.Lock()
	due := s.built.IsZero() || now.Sub(s.built) >= statsRefresh
	s.mu.Unlock()
	if !due {
		return
	}
	rows, err := q.ListSentRouteAttempts(ctx, pgtype.Timestamptz{Time: now.Add(-statsWindow), Valid: true})
	if err != nil {
		rebalLog(fmt.Sprintf("Error reading route attempts: %s", err))
		return
	}
	factor := latencyBanFactor(ctx, q)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pairs = map[pathEdge]*pairStat{}
	s.okAll, s.nAll = 0, 0
	for _, r := range rows {
		hop := -1
		if r.FailureHop.Valid {
			hop = int(r.FailureHop.Int32)
		}
		dur := -1
		if r.DurationMs.Valid {
			dur = int(r.DurationMs.Int32)
		}
		s.observeLocked(r.Route.String, r.Outcome, hop, dur, tsTime(r.Timestamp))
	}
	s.rebuildBansLocked(factor, now)
	s.built = now
	rebalLog(fmt.Sprintf("Route statistics: %d attempts, %d node pairs, average pair chance %.0f%%, %d slow pairs banned",
		len(rows), len(s.pairs), s.avgLocked()*100, len(s.banned)))
}

// observe adds one sent attempt: its path (hop pubkeys joined by "-", our node
// last), outcome (success, success_smaller, failed, timeout), failure source
// index (-1: none) and duration in ms (-1: unknown).
func (s *routeStats) observe(path, outcome string, hop, durationMs int, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeLocked(path, outcome, hop, durationMs, at)
}

func (s *routeStats) pair(e pathEdge) *pairStat {
	p := s.pairs[e]
	if p == nil {
		p = &pairStat{}
		s.pairs[e] = p
	}
	return p
}

// sentOutcomes are the attempt outcomes the statistics count, the same ones
// ListSentRouteAttempts reads.
var sentOutcomes = map[string]bool{"success": true, "success_smaller": true, "failed": true, "timeout": true}

func (s *routeStats) observeLocked(path, outcome string, hop, durationMs int, at time.Time) {
	nodes := strings.Split(path, "-")
	if len(nodes) < 2 || !sentOutcomes[outcome] {
		return
	}
	edges := len(nodes) - 1
	count := func(i int, ok bool) {
		p := s.pair(pathEdge{nodes[i], nodes[i+1]})
		p.n++
		s.nAll++
		if ok {
			p.ok++
			s.okAll++
		}
		if at.After(p.lastUsed) {
			p.lastUsed = at
		}
	}
	// hops is the number of hops the duration is spread over; pairs the
	// number of leading pairs that get a share.
	hops, pairs := len(nodes), edges
	switch {
	case outcome == "success" || outcome == "success_smaller":
		for i := 0; i < edges; i++ {
			count(i, true)
		}
	case outcome == "failed" && hop >= len(nodes): // our node failed: every pair carried it
		for i := 0; i < edges; i++ {
			count(i, true)
		}
	case outcome == "failed" && hop >= 1:
		for i := 0; i < hop-1; i++ {
			count(i, true)
		}
		count(hop-1, false)
		hops, pairs = hop, hop-1
	case outcome == "failed" && hop < 0:
		for i := 0; i < edges; i++ {
			count(i, false)
		}
	case outcome == "failed": // failed at our own node: nothing about the pairs
		return
	}
	if durationMs < 0 || hops <= 0 {
		return
	}
	share := float64(durationMs) / float64(hops)
	for i := 0; i < pairs; i++ {
		p := s.pair(pathEdge{nodes[i], nodes[i+1]})
		p.hopMs = append(p.hopMs, share)
		if at.After(p.lastUsed) {
			p.lastUsed = at
		}
	}
}

func (s *routeStats) avgLocked() float64 {
	if s.nAll == 0 {
		return defaultPairProb
	}
	return s.okAll / s.nAll
}

func (s *routeStats) pairProbLocked(e pathEdge, avg float64) float64 {
	p := s.pairs[e]
	if p == nil {
		return avg
	}
	return (p.ok + probPrior*avg) / (p.n + probPrior)
}

// routeProb returns the success chance of a path (hop pubkeys, our node last).
func (s *routeStats) routeProb(path string) float64 {
	nodes := strings.Split(path, "-")
	s.mu.Lock()
	defer s.mu.Unlock()
	avg := s.avgLocked()
	prob := 1.0
	for i := 0; i+1 < len(nodes); i++ {
		prob *= s.pairProbLocked(pathEdge{nodes[i], nodes[i+1]}, avg)
	}
	return prob
}

func p90(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	return c[int(math.Ceil(0.9*float64(len(c))))-1]
}

// rebuildBansLocked bans the slow pairs and drops expired bans.
func (s *routeStats) rebuildBansLocked(factor float64, now time.Time) {
	for e, until := range s.banned {
		if !now.Before(until) {
			delete(s.banned, e)
		}
	}
	if factor <= 0 {
		s.banned = map[pathEdge]time.Time{}
		return
	}
	var all []float64
	for _, p := range s.pairs {
		all = append(all, p.hopMs...)
	}
	s.globalMs = p90(all)
	if s.globalMs <= 0 {
		return
	}
	for e, p := range s.pairs {
		if len(p.hopMs) < latencyMinSamples || now.Sub(p.lastUsed) > latencyRecent {
			continue
		}
		if p90(p.hopMs) > factor*s.globalMs {
			s.banned[e] = now.Add(latencyBan)
		}
	}
}

// bannedPair returns the first banned pair of a path, if any.
func (s *routeStats) bannedPair(path string, now time.Time) (pathEdge, bool) {
	nodes := strings.Split(path, "-")
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i+1 < len(nodes); i++ {
		e := pathEdge{nodes[i], nodes[i+1]}
		if until, ok := s.banned[e]; ok && now.Before(until) {
			return e, true
		}
	}
	return pathEdge{}, false
}

// bannedPairs returns every pair banned at now.
func (s *routeStats) bannedPairs(now time.Time) []pathEdge {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pathEdge, 0, len(s.banned))
	for e, until := range s.banned {
		if now.Before(until) {
			out = append(out, e)
		}
	}
	return out
}
