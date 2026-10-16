package rebalancer

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// savedRoute is a saved route over the given nodes (ending with "me"), each
// hop charging feePpm[i] ppm on 1'000'000 msat.
func savedRoute(id int64, target, outChan string, nodes []string, feePpm []int64) db.GuiRebalanceroute {
	r := &lnrpc.Route{}
	for i, n := range nodes {
		h := &lnrpc.Hop{PubKey: n, AmtToForwardMsat: 1_000_000}
		if i < len(feePpm) {
			h.FeeMsat = feePpm[i]
		}
		r.Hops = append(r.Hops, h)
	}
	return db.GuiRebalanceroute{ID: id, TargetPubkey: target, OutgoingChanID: outChan,
		Route: strings.Join(nodes, "-"), RouteHex: textOf(routeHexOf(r))}
}

func paidAgo(r db.GuiRebalanceroute, h float64) db.GuiRebalanceroute {
	r.SuccessCount = 1
	r.LastSuccess = ts(savedNow.Add(-time.Duration(h * float64(time.Hour))))
	return r
}

func failedAgo(r db.GuiRebalanceroute, h float64, hop int32) db.GuiRebalanceroute {
	r.FailureCount = 1
	r.LastFailure = ts(savedNow.Add(-time.Duration(h * float64(time.Hour))))
	r.LastFailureHop = pgtype.Int4{Int32: hop, Valid: true}
	return r
}

func comboPaths(rs []comboRoute) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.outChan + ":" + strings.Join(r.nodes, "-")
	}
	return out
}

func comboRows() []db.GuiRebalanceroute {
	return []db.GuiRebalanceroute{
		paidAgo(savedRoute(1, "T1", "1", []string{"S1", "X", "T1", "me"}, []int64{100, 300, 50}), 1),
		paidAgo(savedRoute(2, "T2", "2", []string{"S2", "X", "T2", "me"}, []int64{20, 500, 40}), 1),
		// failed at node 3: S3, Y and X were reached, X -> Z did not work
		failedAgo(savedRoute(3, "T3", "3", []string{"S3", "Y", "X", "Z", "me"}, []int64{10, 10, 999, 30}), 3, 3),
		// paid three days ago: too old to combine
		paidAgo(savedRoute(5, "T1", "5", []string{"S5", "X", "T1", "me"}, []int64{1, 1, 1}), 72),
	}
}

func TestCombineRoutes(t *testing.T) {
	got := combineRoutes(comboRows(), savedNow)

	// T1: heads up to X from route 2 and from the working part of route 3, tail from route 1
	assert.Equal(t, []string{"2:S2-X-T1-me", "3:S3-Y-X-T1-me"}, comboPaths(got["T1"]))
	assert.InDelta(t, 370, got["T1"][0].estPpm, 1e-9) // 20 + 300 + 50
	assert.InDelta(t, 370, got["T1"][1].estPpm, 1e-9) // 10 + 10 + 300 + 50; older evidence second
	assert.Equal(t, savedNow.Add(-3*time.Hour), got["T1"][1].evidence)

	// T2: cheapest first
	assert.Equal(t, []string{"3:S3-Y-X-T2-me", "1:S1-X-T2-me"}, comboPaths(got["T2"]))

	// T3 has no working route to it, so nothing ends there
	assert.Empty(t, got["T3"])
}

func TestCombineRoutesSkipsFailedEdgesAndSavedRoutes(t *testing.T) {
	rows := comboRows()
	// route 4 failed at node 2 after route 1 was paid: X -> T1 does not work now
	rows = append(rows, failedAgo(savedRoute(4, "T1", "4", []string{"S4", "X", "T1", "me"}, []int64{5, 300, 50}), 0.2, 2))
	// a saved route that equals a combination is not combined again
	rows = append(rows, savedRoute(6, "T2", "1", []string{"S1", "X", "T2", "me"}, []int64{100, 500, 40}))
	got := combineRoutes(rows, savedNow)

	assert.Empty(t, got["T1"])
	assert.Equal(t, []string{"4:S4-X-T2-me", "3:S3-Y-X-T2-me"}, comboPaths(got["T2"]))
}

func TestComboCandidates(t *testing.T) {
	c := newComboIndex()
	c.byTarget = combineRoutes(comboRows(), savedNow)
	budget := func(chanID string) int {
		if chanID == "3" {
			return 300 // 370 > 300 * comboSlack
		}
		return 400
	}
	got := c.candidates("T1", []string{"2", "3"}, budget, 10)
	require.Len(t, got, 1)
	r := got[0]
	assert.Equal(t, "2", r.row.OutgoingChanID)
	assert.Equal(t, "S2-X-T1-me", r.row.Route)
	assert.Equal(t, "T1", r.row.TargetPubkey)
	assert.Equal(t, listCombined, r.list)
	assert.Zero(t, r.row.ID)
	assert.InDelta(t, 370, r.row.LastFeePpm.Float64, 1e-9)

	assert.Empty(t, c.candidates("T1", []string{"9"}, budget, 10), "only the given sources")
}
