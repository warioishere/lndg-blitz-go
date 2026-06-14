package rebalancer

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// fakeSavedQ provides fixed candidate lists so that scoring is deterministic
// (the ORDER BY random() query is replaced by a fixed slice here).
type fakeSavedQ struct {
	noFee    []db.GuiRebalanceroute
	feeKnown []db.GuiRebalanceroute
	tested   []db.GuiRebalanceroute
	reps     []db.ListNodeReputationsRow
}

func (f *fakeSavedQ) ListUntestedNoFeeRoutes(ctx context.Context, arg db.ListUntestedNoFeeRoutesParams) ([]db.GuiRebalanceroute, error) {
	return f.noFee, nil
}
func (f *fakeSavedQ) ListUntestedFeeKnownRoutes(ctx context.Context, arg db.ListUntestedFeeKnownRoutesParams) ([]db.GuiRebalanceroute, error) {
	return f.feeKnown, nil
}
func (f *fakeSavedQ) ListTestedRoutes(ctx context.Context, arg db.ListTestedRoutesParams) ([]db.GuiRebalanceroute, error) {
	return f.tested, nil
}
func (f *fakeSavedQ) ListNodeReputations(ctx context.Context, dollar_1 []string) ([]db.ListNodeReputationsRow, error) {
	return f.reps, nil
}

func ppm(v float64) pgtype.Float8 { return pgtype.Float8{Float64: v, Valid: true} }
func ts(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func ids(rs []db.GuiRebalanceroute) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func TestGetSavedRoutesEmpty(t *testing.T) {
	q := &fakeSavedQ{}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, time.Now())
	assert.Empty(t, got)
}

// Tested routes are selected by adj_score; a recent, cheap, well-reputed
// route beats an old, expensive one.
func TestGetSavedRoutesTestedScoring(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	good := db.GuiRebalanceroute{
		ID: 1, Route: "A-B", SuccessCount: 10, FailureCount: 0,
		LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100),
	}
	bad := db.GuiRebalanceroute{
		ID: 2, Route: "C-D", SuccessCount: 1, FailureCount: 5,
		LastSuccess: ts(now.Add(-200 * time.Hour)), LastFeePpm: ppm(1000),
	}
	q := &fakeSavedQ{tested: []db.GuiRebalanceroute{bad, good}}

	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 2, now)
	require.Len(t, got, 2)
	// good first (higher adj_score), despite input order bad,good.
	assert.Equal(t, int64(1), got[0].ID)
}

// Reputation: a hop with poor reputation lowers the min_node_score and thus
// the adj_score of any route that passes through it.
func TestGetSavedRoutesReputationPenalty(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	clean := db.GuiRebalanceroute{
		ID: 1, Route: "A-B", SuccessCount: 5, FailureCount: 0,
		LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100),
	}
	viaBad := db.GuiRebalanceroute{
		ID: 2, Route: "A-X", SuccessCount: 5, FailureCount: 0,
		LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100),
	}
	q := &fakeSavedQ{
		tested: []db.GuiRebalanceroute{viaBad, clean},
		reps: []db.ListNodeReputationsRow{
			{Pubkey: "X", SuccessCount: 0, FailureCount: 20},
		},
	}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 2, now)
	require.Len(t, got, 2)
	assert.Equal(t, int64(1), got[0].ID)
}

// Exploration reserve: with limit=2, at least 1 truly untested route must be
// included even when tested routes score higher.
func TestGetSavedRoutesExplorationReserve(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	t1 := db.GuiRebalanceroute{ID: 1, Route: "A-B", SuccessCount: 10, LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100)}
	t2 := db.GuiRebalanceroute{ID: 2, Route: "C-D", SuccessCount: 10, LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100)}
	u1 := db.GuiRebalanceroute{ID: 3, Route: "E-F"} // untested, no fee
	q := &fakeSavedQ{
		noFee:  []db.GuiRebalanceroute{u1},
		tested: []db.GuiRebalanceroute{t1, t2},
	}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 2, now)
	require.Len(t, got, 2)
	assert.Contains(t, ids(got), int64(3), "untested route must be reserved for exploration")
}

// Diversity: shared hops are penalized so that a disjoint route wins over an
// overlapping one despite equal raw score.
func TestGetSavedRoutesDiversityPenalty(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	// Three tested routes; limit 2. Top route A-B. Then A-C (shares A)
	// and D-E (disjoint) compete with equal raw score — D-E must win via penalty.
	top := db.GuiRebalanceroute{ID: 1, Route: "A-B", SuccessCount: 20, LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100)}
	shares := db.GuiRebalanceroute{ID: 2, Route: "A-C", SuccessCount: 5, LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100)}
	disjoint := db.GuiRebalanceroute{ID: 3, Route: "D-E", SuccessCount: 5, LastSuccess: ts(now.Add(-1 * time.Hour)), LastFeePpm: ppm(100)}
	q := &fakeSavedQ{tested: []db.GuiRebalanceroute{top, shares, disjoint}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 2, now)
	require.Len(t, got, 2)
	assert.Equal(t, int64(1), got[0].ID)
	assert.Equal(t, int64(3), got[1].ID, "disjoint route wins via diversity penalty")
}

func TestGetSavedRoutesFetchLimitTruncation(t *testing.T) {
	now := time.Date(2026, 6, 11, 12, 0, 0, 0, time.UTC)
	// limit 1 -> fetch_limit max(3,30)=30. 40 untested -> truncate to 30 candidates.
	noFee := make([]db.GuiRebalanceroute, 40)
	for i := range noFee {
		noFee[i] = db.GuiRebalanceroute{ID: int64(i + 1), Route: "N" + string(rune('A'+i%26))}
	}
	q := &fakeSavedQ{noFee: noFee}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 1, now)
	require.Len(t, got, 1)
}
