package rebalancer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Saved routes are tried in an order built from each route's latest evidence:
// a payment over it, a failure, or a verification by the route prober. Three
// lists:
//
//	proven  - paid successfully within provenWindow, no failure since
//	explore - verified by the prober (or paid longer ago), no failure since
//	failed  - the latest evidence is a failure older than failCooldown
//
// proven and explore alternate (explore at positions 2, 5, 8, ...), so a
// rebalance also tries routes that were never paid; failed routes come last.
// Routes combined from parts of saved routes (list combined, see combine.go)
// go between explore and failed. Routes LND finds per source come after the
// saved ones; their attempts are logged as list query.
const (
	listProven   = "proven"
	listExplore  = "explore"
	listFailed   = "failed"
	listCombined = "combined"
	listQuery    = "query"

	provenWindow = 48 * time.Hour
	freshProbe   = 2 * time.Hour // one route prober interval
	failCooldown = 30 * time.Minute
)

// savedRoutesQuerier bundles the DB selects required by getSavedRoutes.
type savedRoutesQuerier interface {
	ListRoutesForTarget(ctx context.Context, arg db.ListRoutesForTargetParams) ([]db.GuiRebalanceroute, error)
	ListNodeReputations(ctx context.Context, dollar_1 []string) ([]db.ListNodeReputationsRow, error)
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

// routeCandidate holds the sort keys of a route within its list.
type routeCandidate struct {
	ranked  rankedRoute
	fresh   bool    // latest evidence within freshProbe
	carries bool    // no prober amount below the rebalance value, or paid since the probe
	fee     float64 // last_fee_ppm; +Inf when unknown
	rep     float64 // lowest node reputation on the route
	ratio   float64 // success / attempts
}

// getSavedRoutes returns up to limit saved routes of a target in the order to
// try them (see the list rules above). value is the rebalance amount in sats:
// a route whose prober-verified amount is below it, and that was not paid since,
// goes behind the others of its list. On error it logs and returns nil.
func getSavedRoutes(ctx context.Context, q savedRoutesQuerier, pubkey string, chanIDs []string, limit int, value int64, now time.Time) []rankedRoute {
	rows, err := q.ListRoutesForTarget(ctx, db.ListRoutesForTargetParams{
		TargetPubkey: pubkey, ChanIds: chanIDs, Cutoff: pgtype.Timestamptz{Time: now.Add(-failCooldown), Valid: true},
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}
	if len(rows) == 0 {
		return []rankedRoute{}
	}

	pubkeySet := map[string]struct{}{}
	for _, r := range rows {
		for _, pk := range strings.Split(r.Route, "-") {
			pubkeySet[pk] = struct{}{}
		}
	}
	pubkeys := make([]string, 0, len(pubkeySet))
	for pk := range pubkeySet {
		pubkeys = append(pubkeys, pk)
	}
	reps, err := q.ListNodeReputations(ctx, pubkeys)
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}
	repMap := make(map[string]float64, len(reps))
	for _, nr := range reps {
		repMap[nr.Pubkey] = calcWeightedRatio(int(nr.SuccessCount), int(nr.FailureCount), 10)
	}

	var proven, explore, failed []routeCandidate
	for _, r := range rows {
		list, evidence := classifyRoute(r, now)
		c := routeCandidate{
			ranked:  rankedRoute{row: r, list: list, evidence: evidence},
			fresh:   !evidence.IsZero() && now.Sub(evidence) <= freshProbe,
			carries: !r.ProbedAmountSat.Valid || r.ProbedAmountSat.Int64 >= value || tsTime(r.LastSuccess).After(tsTime(r.LastProbed)),
			fee:     math.Inf(1),
			rep:     1,
		}
		if r.LastFeePpm.Valid {
			c.fee = r.LastFeePpm.Float64
		}
		for _, pk := range strings.Split(r.Route, "-") {
			s, ok := repMap[pk]
			if !ok {
				s = 0.5
			}
			c.rep = math.Min(c.rep, s)
		}
		if n := r.SuccessCount + r.FailureCount; n > 0 {
			c.ratio = float64(r.SuccessCount) / float64(n)
		}
		switch list {
		case listProven:
			proven = append(proven, c)
		case listExplore:
			explore = append(explore, c)
		default:
			failed = append(failed, c)
		}
	}

	byFee := func(a, b routeCandidate) bool {
		if a.fee != b.fee {
			return a.fee < b.fee
		}
		if a.rep != b.rep {
			return a.rep > b.rep
		}
		return a.ranked.row.ID < b.ranked.row.ID
	}
	sort.SliceStable(proven, func(i, j int) bool {
		a, b := proven[i], proven[j]
		if a.carries != b.carries {
			return a.carries
		}
		return byFee(a, b)
	})
	sort.SliceStable(explore, func(i, j int) bool {
		a, b := explore[i], explore[j]
		if a.fresh != b.fresh {
			return a.fresh
		}
		if a.carries != b.carries {
			return a.carries
		}
		return byFee(a, b)
	})
	sort.SliceStable(failed, func(i, j int) bool {
		a, b := failed[i], failed[j]
		if a.ratio != b.ratio {
			return a.ratio > b.ratio
		}
		return byFee(a, b)
	})

	out := make([]rankedRoute, 0, min(limit, len(rows)))
	add := func(c routeCandidate) {
		c.ranked.position = len(out) + 1
		out = append(out, c.ranked)
	}
	pi, ei := 0, 0
	for len(out) < limit && (pi < len(proven) || ei < len(explore)) {
		takeExplore := (len(out)+1)%3 == 2
		if (takeExplore && ei < len(explore)) || pi >= len(proven) {
			add(explore[ei])
			ei++
		} else {
			add(proven[pi])
			pi++
		}
	}
	for _, c := range failed {
		if len(out) >= limit {
			break
		}
		add(c)
	}
	return out
}

// countLists summarizes how many routes of each list a try order holds.
func countLists(rs []rankedRoute) string {
	n := map[string]int{}
	for _, r := range rs {
		n[r.list]++
	}
	return fmt.Sprintf("%d proven, %d explore, %d failed", n[listProven], n[listExplore], n[listFailed])
}

type attemptLogQuerier interface {
	InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error
}

// logRouteAttempt writes one route attempt to route_attempt_log with the
// route's path (rr.row.Route); a failed write is logged and never stops the
// rebalance.
func logRouteAttempt(ctx context.Context, q attemptLogQuerier, rebalanceID int64, rr rankedRoute, value int64, res savedRouteResult, now time.Time) {
	p := db.InsertRouteAttemptParams{
		Timestamp: pgtype.Timestamptz{Time: now, Valid: true}, RebalanceID: rebalanceID,
		RouteID:      pgtype.Int8{Int64: rr.row.ID, Valid: rr.row.ID != 0},
		TargetPubkey: rr.row.TargetPubkey, OutgoingChanID: rr.row.OutgoingChanID, List: rr.list,
		Position: int32(rr.position), FeePpm: rr.row.LastFeePpm, ValueSat: value, Outcome: res.outcome,
		Route: pgtype.Text{String: rr.row.Route, Valid: rr.row.Route != ""},
	}
	if !rr.evidence.IsZero() {
		p.EvidenceAgeS = pgtype.Float8{Float64: now.Sub(rr.evidence).Seconds(), Valid: true}
	}
	if res.code >= 0 {
		p.FailureCode = pgtype.Int4{Int32: int32(res.code), Valid: true}
		p.FailureHop = pgtype.Int4{Int32: int32(res.hop), Valid: true}
	}
	if err := q.InsertRouteAttempt(ctx, p); err != nil {
		rebalLog(fmt.Sprintf("Error writing route attempt log: %s", err))
	}
}
