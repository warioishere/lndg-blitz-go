package rebalancer

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// fakeSavedQ returns fixed routes.
type fakeSavedQ struct {
	routes   []db.GuiRebalanceroute
	attempts []db.InsertRouteAttemptParams
}

func (f *fakeSavedQ) ListRoutesForTarget(ctx context.Context, arg db.ListRoutesForTargetParams) ([]db.GuiRebalanceroute, error) {
	return f.routes, nil
}
func (f *fakeSavedQ) InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error {
	f.attempts = append(f.attempts, arg)
	return nil
}

func ppm(v float64) pgtype.Float8 { return pgtype.Float8{Float64: v, Valid: true} }
func ts(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}
func sats(v int64) pgtype.Int8 { return pgtype.Int8{Int64: v, Valid: true} }

func rankedIDs(rs []rankedRoute) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = r.row.ID
	}
	return out
}

func candidateIDs(cs []routeCandidate) []int64 {
	out := make([]int64, len(cs))
	for i, c := range cs {
		out[i] = c.ranked.row.ID
	}
	return out
}

var savedNow = time.Unix(1_700_000_000, 0)

// paid is a route paid successfully h hours ago at fee ppm.
func paid(id int64, h float64, fee float64) db.GuiRebalanceroute {
	return db.GuiRebalanceroute{ID: id, Route: "a-b", SuccessCount: 1,
		LastSuccess: ts(savedNow.Add(-time.Duration(h * float64(time.Hour)))), LastFeePpm: ppm(fee)}
}

// probed is a never-paid route the prober verified h hours ago at fee ppm.
func probed(id int64, h float64, fee float64) db.GuiRebalanceroute {
	return db.GuiRebalanceroute{ID: id, Route: "a-b",
		LastProbed: ts(savedNow.Add(-time.Duration(h * float64(time.Hour)))), LastFeePpm: ppm(fee)}
}

func TestGetSavedRoutesEmpty(t *testing.T) {
	got := getSavedRoutes(context.Background(), &fakeSavedQ{}, "tgt", []string{"1"}, 50_000, savedNow)
	assert.Empty(t, got)
}

func TestClassifyRoute(t *testing.T) {
	h := func(n float64) pgtype.Timestamptz { return ts(savedNow.Add(-time.Duration(n * float64(time.Hour)))) }
	cases := []struct {
		name string
		r    db.GuiRebalanceroute
		want string
	}{
		{"paid recently", db.GuiRebalanceroute{LastSuccess: h(1)}, listProven},
		{"paid 47h ago, probed since", db.GuiRebalanceroute{LastSuccess: h(47), LastProbed: h(1)}, listProven},
		{"paid 3 days ago", db.GuiRebalanceroute{LastSuccess: h(72)}, listExplore},
		{"paid 3 days ago, probed since", db.GuiRebalanceroute{LastSuccess: h(72), LastProbed: h(1)}, listExplore},
		{"only probed", db.GuiRebalanceroute{LastProbed: h(5)}, listExplore},
		{"no evidence", db.GuiRebalanceroute{}, listExplore},
		{"failed after the success", db.GuiRebalanceroute{LastSuccess: h(5), LastFailure: h(1)}, listFailed},
		{"failed after the probe", db.GuiRebalanceroute{LastProbed: h(5), LastFailure: h(1)}, listFailed},
		{"probed after the failure", db.GuiRebalanceroute{LastFailure: h(5), LastProbed: h(1)}, listExplore},
	}
	for _, c := range cases {
		got, _ := classifyRoute(c.r, savedNow)
		assert.Equal(t, c.want, got, c.name)
	}
}

func TestGetSavedRoutesCandidates(t *testing.T) {
	small := paid(1, 1, 50)
	small.ProbedAmountSat = sats(20_000)
	small.LastProbed = ts(savedNow.Add(-30 * time.Minute)) // probed after the payment: 20k counts
	paidSinceProbe := paid(2, 1, 400)
	paidSinceProbe.ProbedAmountSat = sats(20_000)
	paidSinceProbe.LastProbed = ts(savedNow.Add(-2 * time.Hour)) // paid after the probe: carries
	half := probed(3, 1, 100)
	half.ProbedAmountSat = sats(25_000) // half the value still carries
	unknownFee := paid(4, 1, 0)
	unknownFee.LastFeePpm = pgtype.Float8{}
	q := &fakeSavedQ{routes: []db.GuiRebalanceroute{small, paidSinceProbe, half, unknownFee}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 50_000, savedNow)
	require.Equal(t, []int64{1, 2, 3, 4}, candidateIDs(got))
	assert.False(t, got[0].carries, "probed 20k for a 50k rebalance")
	assert.True(t, got[1].carries)
	assert.True(t, got[2].carries)
	assert.Equal(t, listExplore, got[2].ranked.list)
	assert.True(t, math.IsInf(got[3].fee, 1))
	assert.Equal(t, savedNow.Add(-time.Hour), got[1].ranked.evidence)
}

// candidate is a pool entry over path with fee ppm.
func candidate(id int64, path string, fee float64) routeCandidate {
	return routeCandidate{ranked: rankedRoute{row: db.GuiRebalanceroute{ID: id, Route: path}, list: listProven},
		carries: true, fee: fee}
}

func TestSelectRoutes(t *testing.T) {
	st := newRouteStats()
	for i := 0; i < 9; i++ { // a-b works, b-c fails, c-me fine
		st.observe("a-b-me", "success", -1, -1, savedNow)
		st.observe("x-b-c-me", "failed", 2, -1, savedNow)
	}
	always := func() float64 { return 0 }
	never := func() float64 { return 0.999 }
	pool := []routeCandidate{
		candidate(1, "a-b-me", 300),
		candidate(2, "a-b-me", 100),    // same chance, cheaper first
		candidate(3, "b-c-me", 50),     // weak: b-c failed 9 times
		candidate(4, "q-r-s-t-me", 10), // unknown pairs: the average
	}
	small := candidate(5, "a-b-me", 1)
	small.carries = false
	pool = append(pool, small)

	got, sel := selectRoutes(pool, st, always, 10, savedNow)
	assert.Equal(t, []int64{2, 1, 4, 3}, rankedIDs(got), "by chance, then fee")
	for i, r := range got {
		assert.Equal(t, i+1, r.position)
	}
	assert.Equal(t, 1, sel.small)

	// a draw above prob/sureRouteProb leaves weaker routes out, sure ones stay
	got, sel = selectRoutes(pool, st, never, 10, savedNow)
	assert.Equal(t, []int64{2, 1}, rankedIDs(got))
	assert.Equal(t, 2, sel.notDrawn)

	// the limit cuts the order
	got, _ = selectRoutes(pool, st, always, 1, savedNow)
	assert.Equal(t, []int64{2}, rankedIDs(got))
}

func TestSelectRoutesDropsUnlikelyAndSlow(t *testing.T) {
	st := newRouteStats()
	for i := 0; i < 2000; i++ {
		st.observe("a-b-me", "failed", 1, -1, savedNow)
	}
	st.banned[pathEdge{"s", "t"}] = savedNow.Add(time.Hour)
	got, sel := selectRoutes([]routeCandidate{candidate(1, "a-b-me", 1), candidate(2, "s-t-me", 1)}, st,
		func() float64 { return 0 }, 10, savedNow)
	assert.Empty(t, got)
	assert.Equal(t, 1, sel.weak)
	assert.Equal(t, 1, sel.slow)
}

func TestLogRouteAttempt(t *testing.T) {
	q := &fakeSavedQ{}
	e := newEngine()
	rr := rankedRoute{row: db.GuiRebalanceroute{ID: 7, TargetPubkey: "tgt", OutgoingChanID: "111", LastFeePpm: ppm(250), Route: "aa-bb-self"},
		list: listExplore, position: 2, evidence: savedNow.Add(-90 * time.Second)}
	e.logRouteAttempt(context.Background(), q, 42, rr, 50_000, savedRouteResult{outcome: "failed", code: 15, hop: 2, durationMs: 1200}, savedNow)
	e.logRouteAttempt(context.Background(), q, 42, rankedRoute{row: db.GuiRebalanceroute{ID: 8}, list: listProven, position: 1},
		50_000, notSent("skip_cost"), savedNow)
	require.Len(t, q.attempts, 2)
	a := q.attempts[0]
	assert.Equal(t, int64(42), a.RebalanceID)
	assert.Equal(t, pgtype.Int8{Int64: 7, Valid: true}, a.RouteID)
	assert.Equal(t, "explore", a.List)
	assert.Equal(t, int32(2), a.Position)
	assert.InDelta(t, 90.0, a.EvidenceAgeS.Float64, 1e-9)
	assert.InDelta(t, 250.0, a.FeePpm.Float64, 1e-9)
	assert.Equal(t, int64(50_000), a.ValueSat)
	assert.Equal(t, "failed", a.Outcome)
	assert.Equal(t, int32(15), a.FailureCode.Int32)
	assert.Equal(t, int32(2), a.FailureHop.Int32)
	assert.Equal(t, pgtype.Text{String: "aa-bb-self", Valid: true}, a.Route)
	assert.Equal(t, pgtype.Int4{Int32: 1200, Valid: true}, a.DurationMs)
	b := q.attempts[1]
	assert.Equal(t, pgtype.Int8{Int64: 8, Valid: true}, b.RouteID)
	assert.False(t, b.EvidenceAgeS.Valid)
	assert.False(t, b.FailureCode.Valid)
	assert.False(t, b.DurationMs.Valid, "not sent")
	assert.Equal(t, "skip_cost", b.Outcome)
	assert.False(t, b.Route.Valid, "no path known")

	// the sent attempt counts in the statistics at once: aa-bb crossed, bb-self failed
	st := e.stats
	assert.Equal(t, 1.0, st.pairs[pathEdge{"aa", "bb"}].ok)
	assert.Equal(t, 1.0, st.pairs[pathEdge{"bb", "self"}].n)
	assert.Zero(t, st.pairs[pathEdge{"bb", "self"}].ok)
}

func TestLogRouteAttemptCombinedHasNoRouteID(t *testing.T) {
	q := &fakeSavedQ{}
	newEngine().logRouteAttempt(context.Background(), q, 1, rankedRoute{row: db.GuiRebalanceroute{TargetPubkey: "tgt", Route: "aa-xx-bb-self"}, list: listCombined, position: 3},
		50_000, notSent("skip_cost"), savedNow)
	require.Len(t, q.attempts, 1)
	assert.False(t, q.attempts[0].RouteID.Valid)
	assert.Equal(t, "combined", q.attempts[0].List)
	assert.Equal(t, "aa-xx-bb-self", q.attempts[0].Route.String, "the combined path is kept")
}
