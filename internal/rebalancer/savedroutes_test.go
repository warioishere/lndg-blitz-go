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

// fakeSavedQ returns fixed routes and node reputations.
type fakeSavedQ struct {
	routes   []db.GuiRebalanceroute
	reps     []db.ListNodeReputationsRow
	attempts []db.InsertRouteAttemptParams
}

func (f *fakeSavedQ) ListRoutesForTarget(ctx context.Context, arg db.ListRoutesForTargetParams) ([]db.GuiRebalanceroute, error) {
	return f.routes, nil
}
func (f *fakeSavedQ) ListNodeReputations(ctx context.Context, dollar_1 []string) ([]db.ListNodeReputationsRow, error) {
	return f.reps, nil
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
	got := getSavedRoutes(context.Background(), &fakeSavedQ{}, "tgt", []string{"1"}, 10, 50_000, savedNow)
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

func TestGetSavedRoutesInterleavesExplore(t *testing.T) {
	failedRoute := db.GuiRebalanceroute{ID: 9, Route: "a-b", SuccessCount: 1, FailureCount: 1,
		LastSuccess: ts(savedNow.Add(-5 * time.Hour)), LastFailure: ts(savedNow.Add(-time.Hour))}
	q := &fakeSavedQ{routes: []db.GuiRebalanceroute{
		failedRoute,
		paid(1, 1, 100), paid(2, 1, 200), paid(3, 1, 300),
		probed(4, 1, 100), probed(5, 1, 200), probed(6, 1, 300),
	}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, 50_000, savedNow)
	// explore at positions 2 and 5; when proven runs out explore fills; failed last
	assert.Equal(t, []int64{1, 4, 2, 3, 5, 6, 9}, rankedIDs(got))
	for i, r := range got {
		assert.Equal(t, i+1, r.position)
	}
	assert.Equal(t, listExplore, got[1].list)
	assert.Equal(t, listFailed, got[6].list)
	assert.Equal(t, savedNow.Add(-time.Hour), got[0].evidence)

	// the limit cuts the order
	got = getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 3, 50_000, savedNow)
	assert.Equal(t, []int64{1, 4, 2}, rankedIDs(got))
}

func TestGetSavedRoutesOnlyExplore(t *testing.T) {
	q := &fakeSavedQ{routes: []db.GuiRebalanceroute{probed(1, 1, 300), probed(2, 1, 100)}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, 50_000, savedNow)
	assert.Equal(t, []int64{2, 1}, rankedIDs(got))
}

func TestGetSavedRoutesProvenOrder(t *testing.T) {
	small := paid(1, 1, 50)
	small.ProbedAmountSat = sats(20_000)
	small.LastProbed = ts(savedNow.Add(-30 * time.Minute)) // probed after the payment: 20k counts
	paidSinceProbe := paid(2, 1, 400)
	paidSinceProbe.ProbedAmountSat = sats(20_000)
	paidSinceProbe.LastProbed = ts(savedNow.Add(-2 * time.Hour)) // paid after the probe: carries
	unknownFee := paid(3, 1, 0)
	unknownFee.LastFeePpm = pgtype.Float8{}
	badHop := paid(4, 1, 100)
	badHop.Route = "a-x"
	goodHop := paid(5, 1, 100)
	q := &fakeSavedQ{
		routes: []db.GuiRebalanceroute{small, paidSinceProbe, unknownFee, badHop, goodHop},
		reps:   []db.ListNodeReputationsRow{{Pubkey: "x", SuccessCount: 0, FailureCount: 20}},
	}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, 50_000, savedNow)
	// carrying routes by fee (same fee: better node reputation first), unknown fee
	// last among them, then the route whose probed amount is below 50k
	assert.Equal(t, []int64{5, 4, 2, 3, 1}, rankedIDs(got))
}

func TestGetSavedRoutesExploreOrder(t *testing.T) {
	stale := probed(1, 5, 10) // cheapest, but verified 5h ago
	small := probed(2, 1, 50)
	small.ProbedAmountSat = sats(20_000)
	fresh := probed(3, 1, 200)
	zeroFee := probed(4, 1, 0)
	q := &fakeSavedQ{routes: []db.GuiRebalanceroute{stale, small, fresh, zeroFee}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, 50_000, savedNow)
	// fresh first, then carrying, then by fee
	assert.Equal(t, []int64{4, 3, 2, 1}, rankedIDs(got))
}

func TestGetSavedRoutesFailedOrder(t *testing.T) {
	fail := func(id int64, s, f int32) db.GuiRebalanceroute {
		return db.GuiRebalanceroute{ID: id, Route: "a-b", SuccessCount: s, FailureCount: f,
			LastFailure: ts(savedNow.Add(-time.Hour)), LastFeePpm: ppm(100)}
	}
	q := &fakeSavedQ{routes: []db.GuiRebalanceroute{fail(1, 1, 9), fail(2, 3, 1), fail(3, 0, 1)}}
	got := getSavedRoutes(context.Background(), q, "tgt", []string{"1"}, 10, 50_000, savedNow)
	assert.Equal(t, []int64{2, 1, 3}, rankedIDs(got))
}

func TestLogRouteAttempt(t *testing.T) {
	q := &fakeSavedQ{}
	rr := rankedRoute{row: db.GuiRebalanceroute{ID: 7, TargetPubkey: "tgt", OutgoingChanID: "111", LastFeePpm: ppm(250)},
		list: listExplore, position: 2, evidence: savedNow.Add(-90 * time.Second)}
	logRouteAttempt(context.Background(), q, 42, rr, 50_000, savedRouteResult{outcome: "failed", code: 15, hop: 2}, savedNow)
	logRouteAttempt(context.Background(), q, 42, rankedRoute{row: db.GuiRebalanceroute{ID: 8}, list: listProven, position: 1},
		50_000, savedRouteResult{outcome: "success", code: -1, hop: -1}, savedNow)
	require.Len(t, q.attempts, 2)
	a := q.attempts[0]
	assert.Equal(t, int64(42), a.RebalanceID)
	assert.Equal(t, int64(7), a.RouteID)
	assert.Equal(t, "explore", a.List)
	assert.Equal(t, int32(2), a.Position)
	assert.InDelta(t, 90.0, a.EvidenceAgeS.Float64, 1e-9)
	assert.InDelta(t, 250.0, a.FeePpm.Float64, 1e-9)
	assert.Equal(t, int64(50_000), a.ValueSat)
	assert.Equal(t, "failed", a.Outcome)
	assert.Equal(t, int32(15), a.FailureCode.Int32)
	assert.Equal(t, int32(2), a.FailureHop.Int32)
	b := q.attempts[1]
	assert.False(t, b.EvidenceAgeS.Valid)
	assert.False(t, b.FailureCode.Valid)
	assert.Equal(t, "success", b.Outcome)
}
