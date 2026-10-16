package rebalancer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Saved routes and the routes combined from them (combine.go) form one pool
// per rebalance. Each route gets its success chance from the route statistics
// (stats.go). A route below minRouteProb, over a slow node pair, or whose
// prober-verified amount is below half the rebalance value (and that was not
// paid since) is left out. A route at sureRouteProb or more is always tried, a
// weaker one with the chance prob / sureRouteProb, so it is tried less often
// but not never. The chosen routes go by falling chance, then fee. Routes LND finds per source come after them.
//
// list records where a route came from:
//
//	proven   - paid successfully within provenWindow, no failure since
//	explore  - verified by the prober (or paid longer ago), no failure since
//	failed   - the latest evidence is a failure older than failCooldown
//	combined - built from parts of saved routes
//	query    - found by QueryRoutes
//	auto     - an HTLC attempt LND made for SendPaymentV2 (only logged)
const (
	listProven   = "proven"
	listExplore  = "explore"
	listFailed   = "failed"
	listCombined = "combined"
	listQuery    = "query"
	listAuto     = "auto"

	provenWindow = 48 * time.Hour
	failCooldown = 30 * time.Minute
)

// savedRoutesQuerier bundles the DB selects required by getSavedRoutes.
type savedRoutesQuerier interface {
	ListRoutesForTarget(ctx context.Context, arg db.ListRoutesForTargetParams) ([]db.GuiRebalanceroute, error)
}

// rankedRoute is a saved route with the list it was taken from, its 1-based
// position in the try order and the time of its latest evidence (zero: none).
type rankedRoute struct {
	row      db.GuiRebalanceroute
	list     string
	position int
	evidence time.Time
}

func tsTime(t pgtype.Timestamptz) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time
}

func latestOf(ts ...time.Time) time.Time {
	var out time.Time
	for _, t := range ts {
		if t.After(out) {
			out = t
		}
	}
	return out
}

// classifyRoute returns the list of a route and the time of its latest evidence.
func classifyRoute(r db.GuiRebalanceroute, now time.Time) (string, time.Time) {
	success, failure, probed := tsTime(r.LastSuccess), tsTime(r.LastFailure), tsTime(r.LastProbed)
	evidence := latestOf(success, failure, probed)
	switch {
	case !failure.IsZero() && failure.Equal(evidence):
		return listFailed, evidence
	case !success.IsZero() && now.Sub(success) <= provenWindow:
		return listProven, evidence
	default:
		return listExplore, evidence
	}
}

// routeCandidate is a route of the pool with its sort keys.
type routeCandidate struct {
	ranked  rankedRoute
	carries bool    // no prober amount below half the rebalance value, or paid since the probe
	fee     float64 // last_fee_ppm (estimate for a combined route); +Inf when unknown
}

// getSavedRoutes returns the saved routes of a target over the given sources
// as candidates for selectRoutes. value is the rebalance amount in sats. On
// error it logs and returns nil.
func getSavedRoutes(ctx context.Context, q savedRoutesQuerier, pubkey string, chanIDs []string, value int64, now time.Time) []routeCandidate {
	rows, err := q.ListRoutesForTarget(ctx, db.ListRoutesForTargetParams{
		TargetPubkey: pubkey, ChanIds: chanIDs, Cutoff: pgtype.Timestamptz{Time: now.Add(-failCooldown), Valid: true},
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}
	if len(rows) == 0 {
		return nil
	}

	out := make([]routeCandidate, 0, len(rows))
	for _, r := range rows {
		list, evidence := classifyRoute(r, now)
		c := routeCandidate{
			ranked:  rankedRoute{row: r, list: list, evidence: evidence},
			carries: !r.ProbedAmountSat.Valid || 2*r.ProbedAmountSat.Int64 >= value || tsTime(r.LastSuccess).After(tsTime(r.LastProbed)),
			fee:     math.Inf(1),
		}
		if r.LastFeePpm.Valid {
			c.fee = r.LastFeePpm.Float64
		}
		out = append(out, c)
	}
	return out
}

// selection counts what selectRoutes left out.
type selection struct {
	weak, slow, small, notDrawn int
}

// selectRoutes picks up to limit routes of the pool to try, in the order to
// try them (see the rules above). draw returns a random number in [0, 1).
func selectRoutes(pool []routeCandidate, st *routeStats, draw func() float64, limit int, now time.Time) ([]rankedRoute, selection) {
	type scored struct {
		c    routeCandidate
		prob float64
	}
	var sel selection
	var picked []scored
	for _, c := range pool {
		if !c.carries {
			sel.small++
			continue
		}
		if _, slow := st.bannedPair(c.ranked.row.Route, now); slow {
			sel.slow++
			continue
		}
		prob := st.routeProb(c.ranked.row.Route)
		if prob < minRouteProb {
			sel.weak++
			continue
		}
		if prob < sureRouteProb && draw() >= prob/sureRouteProb {
			sel.notDrawn++
			continue
		}
		picked = append(picked, scored{c, prob})
	}
	sort.SliceStable(picked, func(i, j int) bool {
		a, b := picked[i], picked[j]
		if a.prob != b.prob {
			return a.prob > b.prob
		}
		if a.c.fee != b.c.fee {
			return a.c.fee < b.c.fee
		}
		return a.c.ranked.row.ID < b.c.ranked.row.ID
	})
	out := make([]rankedRoute, 0, min(limit, len(picked)))
	for _, s := range picked {
		if len(out) >= limit {
			break
		}
		r := s.c.ranked
		r.position = len(out) + 1
		out = append(out, r)
	}
	return out, sel
}

// countLists summarizes how many routes of each list a try order holds.
func countLists(rs []rankedRoute) string {
	n := map[string]int{}
	for _, r := range rs {
		n[r.list]++
	}
	return fmt.Sprintf("%d proven, %d explore, %d failed, %d combined", n[listProven], n[listExplore], n[listFailed], n[listCombined])
}

type attemptLogQuerier interface {
	InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error
}

// AttemptLogQuerier is the DB access RecordAttempt needs.
type AttemptLogQuerier = attemptLogQuerier

// RouteAttempt is one route attempt for route_attempt_log. RebalanceID is 0
// for an attempt outside a rebalance (route prober); Code and Hop are the
// failure code and failure source index (-1: none), DurationMs the time until
// the result (-1: not sent).
type RouteAttempt struct {
	RebalanceID, RouteID int64
	Target, OutChan      string
	List                 string
	Position             int
	EvidenceAge          time.Duration // 0: none
	FeePpm               pgtype.Float8
	ValueSat             int64
	Outcome              string
	Code, Hop            int
	Path                 string
	DurationMs           int
}

// RecordAttempt writes one attempt to route_attempt_log, the evidence of the
// route statistics; a failed write is logged and never stops the caller.
func RecordAttempt(ctx context.Context, q AttemptLogQuerier, a RouteAttempt, now time.Time) {
	p := db.InsertRouteAttemptParams{
		Timestamp: pgtype.Timestamptz{Time: now, Valid: true}, RebalanceID: a.RebalanceID,
		RouteID:      pgtype.Int8{Int64: a.RouteID, Valid: a.RouteID != 0},
		TargetPubkey: a.Target, OutgoingChanID: a.OutChan, List: a.List,
		Position: int32(a.Position), FeePpm: a.FeePpm, ValueSat: a.ValueSat, Outcome: a.Outcome,
		Route: pgtype.Text{String: a.Path, Valid: a.Path != ""},
	}
	if a.EvidenceAge > 0 {
		p.EvidenceAgeS = pgtype.Float8{Float64: a.EvidenceAge.Seconds(), Valid: true}
	}
	if a.Code >= 0 {
		p.FailureCode = pgtype.Int4{Int32: int32(a.Code), Valid: true}
		p.FailureHop = pgtype.Int4{Int32: int32(a.Hop), Valid: true}
	}
	if a.DurationMs >= 0 {
		p.DurationMs = pgtype.Int4{Int32: int32(a.DurationMs), Valid: true}
	}
	if err := q.InsertRouteAttempt(ctx, p); err != nil {
		rebalLog(fmt.Sprintf("Error writing route attempt log: %s", err))
	}
}

// logRouteAttempt records an attempt of a saved, combined or QueryRoutes route
// and adds a sent one to the route statistics at once.
func (e *engine) logRouteAttempt(ctx context.Context, q attemptLogQuerier, rebalanceID int64, rr rankedRoute, value int64, res savedRouteResult, now time.Time) {
	a := RouteAttempt{
		RebalanceID: rebalanceID, RouteID: rr.row.ID, Target: rr.row.TargetPubkey, OutChan: rr.row.OutgoingChanID,
		List: rr.list, Position: rr.position, FeePpm: rr.row.LastFeePpm, ValueSat: value, Outcome: res.outcome,
		Code: res.code, Hop: res.hop, Path: rr.row.Route, DurationMs: res.durationMs,
	}
	if !rr.evidence.IsZero() {
		a.EvidenceAge = now.Sub(rr.evidence)
	}
	RecordAttempt(ctx, q, a, now)
	if res.durationMs >= 0 && rr.row.Route != "" {
		hop := -1
		if res.code >= 0 {
			hop = res.hop
		}
		e.stats.observe(rr.row.Route, res.outcome, hop, res.durationMs, now)
	}
}
