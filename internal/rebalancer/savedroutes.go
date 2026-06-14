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

// savedRoutesQuerier bundles the DB selects required by getSavedRoutes.
type savedRoutesQuerier interface {
	ListUntestedNoFeeRoutes(ctx context.Context, arg db.ListUntestedNoFeeRoutesParams) ([]db.GuiRebalanceroute, error)
	ListUntestedFeeKnownRoutes(ctx context.Context, arg db.ListUntestedFeeKnownRoutesParams) ([]db.GuiRebalanceroute, error)
	ListTestedRoutes(ctx context.Context, arg db.ListTestedRoutesParams) ([]db.GuiRebalanceroute, error)
	ListNodeReputations(ctx context.Context, dollar_1 []string) ([]db.ListNodeReputationsRow, error)
}

// scoredRoute attaches an adjusted score and hop set to a candidate route.
type scoredRoute struct {
	row      db.GuiRebalanceroute
	adjScore float64
	hops     map[string]struct{}
}

// routeUntested returns true when success_count == 0 && failure_count == 0.
func routeUntested(r db.GuiRebalanceroute) bool {
	return r.SuccessCount == 0 && r.FailureCount == 0
}

// feePos returns true when last_fee_ppm is set and positive.
func feePos(f pgtype.Float8) bool { return f.Valid && f.Float64 > 0 }

// feeFalsy returns true when last_fee_ppm is NULL or zero.
func feeFalsy(f pgtype.Float8) bool { return !f.Valid || f.Float64 == 0 }

// hopSet returns the set of pubkeys from a "-"-separated route string.
func hopSet(route string) map[string]struct{} {
	hops := make(map[string]struct{})
	for _, pk := range strings.Split(route, "-") {
		hops[pk] = struct{}{}
	}
	return hops
}

// getSavedRoutes selects and scores saved routes for a target pubkey using
// diversity-aware greedy selection. On error it logs and returns nil.
func getSavedRoutes(ctx context.Context, q savedRoutesQuerier, pubkey string, chanIDs []string, limit int, now time.Time) []db.GuiRebalanceroute {
	cutoff := pgtype.Timestamptz{Time: now.Add(-30 * time.Minute), Valid: true}
	fetchLimit := limit * 3
	if fetchLimit < 30 {
		fetchLimit = 30
	}

	trulyUntested, err := q.ListUntestedNoFeeRoutes(ctx, db.ListUntestedNoFeeRoutesParams{
		TargetPubkey: pubkey, ChanIds: chanIDs, Cutoff: cutoff, FetchLimit: int32(fetchLimit),
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}
	feeKnownUntested, err := q.ListUntestedFeeKnownRoutes(ctx, db.ListUntestedFeeKnownRoutesParams{
		TargetPubkey: pubkey, ChanIds: chanIDs, Cutoff: cutoff, FetchLimit: int32(fetchLimit),
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}
	tested, err := q.ListTestedRoutes(ctx, db.ListTestedRoutesParams{
		TargetPubkey: pubkey, ChanIds: chanIDs, Cutoff: cutoff, FetchLimit: int32(fetchLimit),
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
		return nil
	}

	// Merge: truly_untested + fee_known_untested + tested, capped at fetchLimit.
	candidates := make([]db.GuiRebalanceroute, 0, len(trulyUntested)+len(feeKnownUntested)+len(tested))
	candidates = append(candidates, trulyUntested...)
	candidates = append(candidates, feeKnownUntested...)
	candidates = append(candidates, tested...)
	if len(candidates) > fetchLimit {
		candidates = candidates[:fetchLimit]
	}
	if len(candidates) == 0 {
		return []db.GuiRebalanceroute{}
	}

	// Step 1: Node-reputation scoring.
	routeHops := make(map[int64]map[string]struct{}, len(candidates))
	allPubkeysSet := make(map[string]struct{})
	for _, r := range candidates {
		hops := hopSet(r.Route)
		routeHops[r.ID] = hops
		for pk := range hops {
			allPubkeysSet[pk] = struct{}{}
		}
	}

	repMap := make(map[string]float64)
	if len(allPubkeysSet) > 0 {
		allPubkeys := make([]string, 0, len(allPubkeysSet))
		for pk := range allPubkeysSet {
			allPubkeys = append(allPubkeys, pk)
		}
		reps, err := q.ListNodeReputations(ctx, allPubkeys)
		if err != nil {
			rebalLog(fmt.Sprintf("Error getting saved routes: %s", err))
			return nil
		}
		for _, nr := range reps {
			repMap[nr.Pubkey] = calcWeightedRatio(int(nr.SuccessCount), int(nr.FailureCount), 10)
		}
	}

	// Minimum known fee across candidates (only last_fee_ppm > 0).
	var minFee float64
	hasMinFee := false
	for _, r := range candidates {
		if feePos(r.LastFeePpm) {
			if !hasMinFee || r.LastFeePpm.Float64 < minFee {
				minFee = r.LastFeePpm.Float64
				hasMinFee = true
			}
		}
	}

	scored := make([]scoredRoute, 0, len(candidates))
	for _, r := range candidates {
		hops := routeHops[r.ID]
		var minNodeScore float64
		if len(hops) > 0 && len(repMap) > 0 {
			minNodeScore = math.Inf(1)
			for pk := range hops {
				s := 0.5
				if v, ok := repMap[pk]; ok {
					s = v
				}
				if s < minNodeScore {
					minNodeScore = s
				}
			}
		} else {
			minNodeScore = 0.5
		}

		var adjScore float64
		if routeUntested(r) {
			feeFactor := 1.0
			if hasMinFee && feePos(r.LastFeePpm) {
				feeFactor = minFee / r.LastFeePpm.Float64
			}
			adjScore = 0.5 * minNodeScore * feeFactor
		} else {
			// Time decay: half-life 48 hours.
			var timeFactor float64
			if r.LastSuccess.Valid {
				hoursAgo := now.Sub(r.LastSuccess.Time).Seconds() / 3600
				timeFactor = math.Pow(2, -hoursAgo/48)
			} else {
				timeFactor = 0.1
			}
			feeFactor := 0.5
			if hasMinFee && feePos(r.LastFeePpm) {
				feeFactor = minFee / r.LastFeePpm.Float64
			}
			weightedRatio := calcWeightedRatio(int(r.SuccessCount), int(r.FailureCount), 10)
			adjScore = weightedRatio * minNodeScore * timeFactor * feeFactor
		}
		scored = append(scored, scoredRoute{row: r, adjScore: adjScore, hops: hops})
	}

	// Step 2: Diversity-aware greedy selection. Stable sort preserves input order for ties.
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].adjScore > scored[j].adjScore })

	selected := make([]db.GuiRebalanceroute, 0, limit)
	usedHops := make(map[string]struct{})
	remaining := make([]scoredRoute, len(scored))
	copy(remaining, scored)

	for len(remaining) > 0 && len(selected) < limit {
		bestIdx := 0
		bestFinal := -1.0
		for i, sr := range remaining {
			shared := 0
			for pk := range sr.hops {
				if _, ok := usedHops[pk]; ok {
					shared++
				}
			}
			penalty := math.Pow(0.5, float64(shared))
			final := sr.adjScore * penalty
			if final > bestFinal {
				bestFinal = final
				bestIdx = i
			}
		}
		sr := remaining[bestIdx]
		remaining = append(remaining[:bestIdx], remaining[bestIdx+1:]...)
		selected = append(selected, sr.row)
		for pk := range sr.hops {
			usedHops[pk] = struct{}{}
		}
	}

	// Step 3: Reserve ~50% of slots for truly untested routes (no fee data at all).
	minExplore := limit / 2
	if minExplore < 1 {
		minExplore = 1
	}
	untestedCount := 0
	for _, r := range selected {
		if routeUntested(r) && feeFalsy(r.LastFeePpm) {
			untestedCount++
		}
	}
	if untestedCount < minExplore {
		selectedIDs := make(map[int64]struct{}, len(selected))
		for _, r := range selected {
			selectedIDs[r.ID] = struct{}{}
		}
		untestedAvail := make([]db.GuiRebalanceroute, 0)
		for _, sr := range scored {
			r := sr.row
			if _, picked := selectedIDs[r.ID]; routeUntested(r) && feeFalsy(r.LastFeePpm) && !picked {
				untestedAvail = append(untestedAvail, r)
			}
		}
		need := minExplore - untestedCount
		if need > len(untestedAvail) {
			need = len(untestedAvail)
		}
		if need > 0 {
			// Drop the lowest-priority tested routes (last chosen by diversity selection).
			keep := make([]db.GuiRebalanceroute, 0, len(selected))
			canDrop := need
			for i := len(selected) - 1; i >= 0; i-- {
				r := selected[i]
				if canDrop > 0 && (r.SuccessCount > 0 || r.FailureCount > 0) {
					canDrop--
				} else {
					keep = append(keep, r)
				}
			}
			// Reverse keep back to original order.
			for i, j := 0, len(keep)-1; i < j; i, j = i+1, j-1 {
				keep[i], keep[j] = keep[j], keep[i]
			}
			keep = append(keep, untestedAvail[:need]...)
			selected = keep
		}
	}

	return selected
}
