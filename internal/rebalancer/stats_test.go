package rebalancer

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func TestRouteStatsObserve(t *testing.T) {
	st := newRouteStats()
	st.observe("a-b-c-me", "success", -1, 4000, savedNow)
	st.observe("a-b-c-me", "failed", 2, 2000, savedNow) // b failed to forward to c
	st.observe("a-b-c-me", "failed", 4, -1, savedNow)   // our node failed: every pair carried it
	st.observe("a-b-c-me", "failed", -1, -1, savedNow)  // no failing node: against every pair
	st.observe("a-b-c-me", "failed", 0, -1, savedNow)   // our own node: nothing about the pairs
	st.observe("a-b-c-me", "timeout", -1, 15000, savedNow)
	st.observe("a-b-c-me", "error", -1, 900, savedNow) // an RPC error says nothing about the pairs

	ab, bc, cme := st.pairs[pathEdge{"a", "b"}], st.pairs[pathEdge{"b", "c"}], st.pairs[pathEdge{"c", "me"}]
	assert.Equal(t, [2]float64{3, 4}, [2]float64{ab.ok, ab.n})
	assert.Equal(t, [2]float64{2, 4}, [2]float64{bc.ok, bc.n})
	assert.Equal(t, [2]float64{2, 3}, [2]float64{cme.ok, cme.n})
	// 4000 ms over 4 hops; 2000 ms over the 2 hops up to b; 15000 ms over 4 hops
	assert.Equal(t, []float64{1000, 1000, 3750}, ab.hopMs)
	assert.Equal(t, []float64{1000, 3750}, bc.hopMs)
}

func TestRouteProb(t *testing.T) {
	st := newRouteStats()
	assert.InDelta(t, 0.25, st.routeProb("x-y-me"), 1e-9, "no attempts: the default average per pair")
	for i := 0; i < 3; i++ {
		st.observe("a-b", "success", -1, -1, savedNow)
	}
	st.observe("a-c", "failed", -1, -1, savedNow)
	// average 3/4; a-b (3 + 0.75) / 4, a-c (0 + 0.75) / 2, unknown pairs 0.75
	assert.InDelta(t, 0.9375, st.routeProb("a-b"), 1e-9)
	assert.InDelta(t, 0.375, st.routeProb("a-c"), 1e-9)
	assert.InDelta(t, 0.9375*0.75, st.routeProb("a-b-z"), 1e-9)
}

func TestLatencyBans(t *testing.T) {
	st := newRouteStats()
	for i := 0; i < 500; i++ {
		st.observe("a-b-me", "success", -1, 3000, savedNow) // 1000 ms per hop
	}
	for i := 0; i < latencyMinSamples; i++ {
		st.observe("s-t-me", "success", -1, 30000, savedNow) // 10000 ms per hop
	}
	for i := 0; i < latencyMinSamples-1; i++ {
		st.observe("u-v-me", "success", -1, 30000, savedNow) // slow, but too few samples
	}
	st.rebuildBansLocked(2, savedNow)
	_, slow := st.bannedPair("x-s-t-me", savedNow)
	assert.True(t, slow)
	_, slow = st.bannedPair("u-v-me", savedNow)
	assert.False(t, slow)
	_, slow = st.bannedPair("a-b-me", savedNow)
	assert.False(t, slow)
	assert.Len(t, st.bannedPairs(savedNow), 2, "s-t and t-me")
	assert.Empty(t, st.bannedPairs(savedNow.Add(latencyBan)), "the ban ends")

	// a pair not used within latencyRecent is not banned
	st.rebuildBansLocked(2, savedNow.Add(latencyRecent+time.Hour))
	assert.Empty(t, st.bannedPairs(savedNow.Add(latencyBan)))

	// factor 0 turns the bans off
	st.rebuildBansLocked(2, savedNow)
	st.rebuildBansLocked(0, savedNow)
	assert.Empty(t, st.bannedPairs(savedNow))
}

type fakeStatsQ struct {
	settings map[string]string
	rows     []db.ListSentRouteAttemptsRow
	calls    int
}

func (f *fakeStatsQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeStatsQ) ListSentRouteAttempts(ctx context.Context, since pgtype.Timestamptz) ([]db.ListSentRouteAttemptsRow, error) {
	f.calls++
	return f.rows, nil
}

func TestRouteStatsRefresh(t *testing.T) {
	q := &fakeStatsQ{rows: []db.ListSentRouteAttemptsRow{
		{Timestamp: ts(savedNow), Route: pgtype.Text{String: "a-b-me", Valid: true}, Outcome: "success"},
		{Timestamp: ts(savedNow), Route: pgtype.Text{String: "a-b-me", Valid: true}, Outcome: "failed",
			FailureHop: pgtype.Int4{Int32: 2, Valid: true}, DurationMs: pgtype.Int4{Int32: 500, Valid: true}},
	}}
	st := newRouteStats()
	st.observe("x-y", "success", -1, -1, savedNow) // the rebuild starts from the attempt log alone
	st.refresh(context.Background(), q, savedNow)
	require.Equal(t, 1, q.calls)
	assert.Nil(t, st.pairs[pathEdge{"x", "y"}])
	assert.Equal(t, [2]float64{2, 2}, [2]float64{st.pairs[pathEdge{"a", "b"}].ok, st.pairs[pathEdge{"a", "b"}].n})
	assert.Equal(t, [2]float64{1, 2}, [2]float64{st.pairs[pathEdge{"b", "me"}].ok, st.pairs[pathEdge{"b", "me"}].n})
	assert.Equal(t, []float64{250}, st.pairs[pathEdge{"a", "b"}].hopMs, "b failed: 500 ms over the 2 hops up to b")

	st.refresh(context.Background(), q, savedNow.Add(statsRefresh-time.Second))
	assert.Equal(t, 1, q.calls, "not rebuilt within statsRefresh")
	st.refresh(context.Background(), q, savedNow.Add(statsRefresh))
	assert.Equal(t, 2, q.calls)
}

func TestPairReport(t *testing.T) {
	q := &fakeStatsQ{rows: []db.ListSentRouteAttemptsRow{
		{Timestamp: ts(savedNow), Route: pgtype.Text{String: "a-b-me", Valid: true}, Outcome: "success",
			DurationMs: pgtype.Int4{Int32: 3000, Valid: true}},
		{Timestamp: ts(savedNow), Route: pgtype.Text{String: "a-b-me", Valid: true}, Outcome: "failed",
			FailureHop: pgtype.Int4{Int32: 2, Valid: true}},
	}}
	pairs, avg, globalMs, err := PairReport(context.Background(), q, savedNow)
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].From < pairs[j].From })
	assert.InDelta(t, 0.75, avg, 1e-9, "3 of 4 pair crossings")
	assert.InDelta(t, 1000, globalMs, 1e-9)
	assert.Equal(t, PairStat{From: "a", To: "b", Attempts: 2, Successes: 2, Prob: (2 + 0.75) / 3,
		HopP90Ms: 1000, Samples: 1, LastUsed: savedNow}, pairs[0])
	assert.Equal(t, 1, pairs[1].Successes)
	assert.InDelta(t, (1+0.75)/3, pairs[1].Prob, 1e-9)
}

func TestP90(t *testing.T) {
	xs := []float64{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}
	assert.Equal(t, 9.0, p90(xs))
	assert.Equal(t, 0.0, p90(nil))
	assert.Equal(t, []float64{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}, xs, "the input stays unsorted")
}
