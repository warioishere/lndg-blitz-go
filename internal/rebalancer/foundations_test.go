package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func route(hops ...*lnrpc.Hop) *lnrpc.Route { return &lnrpc.Route{Hops: hops} }
func hop(pub string, amtMsat int64) *lnrpc.Hop {
	return &lnrpc.Hop{PubKey: pub, AmtToForwardMsat: amtMsat}
}

func TestCheckOpportunityCost(t *testing.T) {
	// target 1000 * 65% = 650; - source 50 = 600; min(600, max 500) = 500.
	// route 400000 msat over 1000 sat -> 400000/(1000*1000)*1e6 = 400000 ppm > 500 -> rejected.
	allowed, ppm, maxPpm := checkOpportunityCost(400_000, 1000, 50, 1000, 65, 500)
	assert.False(t, allowed)
	assert.Equal(t, 400_000, ppm) // 400000 msat / (1000 sat * 1000) * 1e6
	assert.Equal(t, 500, maxPpm)

	// cheap route: 100000 msat over 1_000_000 sat -> 100 ppm <= 500 -> allowed.
	allowed2, ppm2, _ := checkOpportunityCost(100_000, 1_000_000, 50, 1000, 65, 500)
	assert.True(t, allowed2)
	assert.Equal(t, 100, ppm2)

	// amount_sat == 0 -> route_fee_ppm 0.
	allowed3, ppm3, _ := checkOpportunityCost(999_999, 0, 0, 1000, 65, 500)
	assert.True(t, allowed3)
	assert.Equal(t, 0, ppm3)
}

func TestCheckOpportunityCost_BudgetCappedByMaxFeeRate(t *testing.T) {
	// target 100000 * 65% = 65000; - source 0 = 65000; min(65000, 500) = 500.
	_, _, maxPpm := checkOpportunityCost(0, 1000, 0, 100000, 65, 500)
	assert.Equal(t, 500, maxPpm)
}

func TestCalcSuccessRatio(t *testing.T) {
	assert.InDelta(t, 0.5, calcSuccessRatio(0, 0), 1e-9) // 1/2
	assert.InDelta(t, float64(11)/13, calcSuccessRatio(10, 1), 1e-9)
}

func TestCalcWeightedRatio(t *testing.T) {
	// total 0 -> ratio * (0/10) = 0.
	assert.InDelta(t, 0, calcWeightedRatio(0, 0, 10), 1e-9)
	// sc=9 fc=1: ratio = 10/12; total=10; weighted = ratio * 10/20.
	ratio := float64(10) / 12
	assert.InDelta(t, ratio*(10.0/20.0), calcWeightedRatio(9, 1, 10), 1e-9)
}

func TestFailureCodeName(t *testing.T) {
	assert.Equal(t, "TEMPORARY_CHANNEL_FAILURE", failureCodeName(15))
	assert.Equal(t, "FEE_INSUFFICIENT", failureCodeName(12))
	assert.Equal(t, "99", failureCodeName(99)) // unknown -> number
}

func TestMissionControl_RecordAndValidate(t *testing.T) {
	mc := newMissionControl()
	base := time.Unix(1_700_000_000, 0)
	mc.now = func() time.Time { return base }

	// Route A->B->C, fail at fsi=1 (edge prev=hop0 -> failed=hop1), code 15.
	r := route(hop("A", 1_000_000), hop("B", 990_000), hop("C", 980_000))
	mc.recordRouteFailure(r, &lnrpc.Failure{Code: 15, FailureSourceIndex: 1})

	// A route reusing edge (A->B) at the same amount must be invalidated.
	bad := route(hop("A", 1_000_000), hop("B", 990_000))
	assert.False(t, mc.validateRoute(bad))

	// A route reusing (A->B) at a very different amount passes (diff_ppm > tolerance).
	farAmt := route(hop("A", 100_000), hop("B", 99_000))
	assert.True(t, mc.validateRoute(farAmt))

	// A disjoint route passes.
	other := route(hop("X", 1_000_000), hop("Y", 990_000))
	assert.True(t, mc.validateRoute(other))
}

func TestMissionControl_NonTempFailureIgnored(t *testing.T) {
	mc := newMissionControl()
	r := route(hop("A", 1_000_000), hop("B", 990_000), hop("C", 980_000))
	mc.recordRouteFailure(r, &lnrpc.Failure{Code: 12, FailureSourceIndex: 1}) // not 15
	mc.recordRouteFailure(r, &lnrpc.Failure{Code: 15, FailureSourceIndex: 0}) // fsi<=0
	assert.Empty(t, mc.edges)
}

func TestMissionControl_Expiry(t *testing.T) {
	mc := newMissionControl()
	base := time.Unix(1_700_000_000, 0)
	mc.now = func() time.Time { return base }
	r := route(hop("A", 1_000_000), hop("B", 990_000), hop("C", 980_000))
	mc.recordRouteFailure(r, &lnrpc.Failure{Code: 15, FailureSourceIndex: 1})
	assert.Len(t, mc.edges, 1)
	// advance past expiry; validateRoute prunes.
	mc.now = func() time.Time { return base.Add(failedEdgeExpiry + time.Second) }
	assert.True(t, mc.validateRoute(route(hop("A", 1_000_000), hop("B", 990_000))))
	assert.Empty(t, mc.edges)
}

func TestMissionControl_ShortRouteAlwaysValid(t *testing.T) {
	mc := newMissionControl()
	assert.True(t, mc.validateRoute(nil))
	assert.True(t, mc.validateRoute(route(hop("A", 1000))))
}
