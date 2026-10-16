package af

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

var nkNow = time.Unix(1_700_000_000, 0)

func nkTestParams() NkParams {
	return NkParams{
		StepUpPct: 3, StepDownPct: 1, LockMin: 12 * time.Minute, LockMax: 360 * time.Minute,
		LockFlowPct: 2, LockFeeRef: 2000, AnchorPct: 110, CapFactor: 2, CapMin: 1000, MaxRate: 7000,
		MinRate: 1, EmptyPct: 2, MinInterval: 10 * time.Minute, Republish: 30 * time.Minute,
		BaseMsat: 1, InboundMargin: 11, BudgetMarginMin: 10, BudgetMarginMaxPct: 20,
	}
}

// sold24 is a flow that sold amt sats at fee ppm within the last 24 hours.
func sold24(feePpm, amt float64) NkFlow {
	fee := feePpm * amt / 1e6
	return NkFlow{Fwd24hAmt: amt, Fwd24hFee: fee, Fwd72hAmt: amt, Fwd72hFee: fee,
		All24hAmt: amt, All24hFee: fee, All72hAmt: amt, All72hFee: fee}
}

// sold30 is a flow that sold only further back than 72 hours: it sets the
// cap (2 x feePpm) without an anchor.
func sold30(feePpm float64) NkFlow {
	return NkFlow{All30dAmt: 1_000_000, All30dFee: feePpm}
}

// idle is an NkInput whose last change and publish are long past.
func idle(in NkInput) NkInput {
	in.LastChange, in.LastPublish = nkNow.Add(-time.Hour), nkNow.Add(-time.Hour)
	return in
}

func TestNkFlowWindows(t *testing.T) {
	f := NkFlow{All72hAmt: 1_000_000, All72hFee: 500, Fwd72hAmt: 1_000_000, Fwd72hFee: 500}
	a, ok := f.Anchor()
	require.True(t, ok)
	assert.Equal(t, 500, a, "72 h when nothing flowed in 24 h")
	f.All24hAmt, f.All24hFee = 100_000, 80
	a, _ = f.Anchor()
	assert.Equal(t, 800, a, "24 h first")

	s, ok := f.SoldPpm()
	require.True(t, ok)
	assert.Equal(t, 500, s)
	f.Fwd6hAmt, f.Fwd6hFee = 10_000, 9
	s, _ = f.SoldPpm()
	assert.Equal(t, 900, s, "6 h first")

	_, ok = NkFlow{}.Anchor()
	assert.False(t, ok)
}

func TestNkDecideSteps(t *testing.T) {
	p := nkTestParams()
	// below target: up 3 %
	d := NkDecide(idle(NkInput{Fee: 1000, LocalPct: 20, TargetPct: 50, Flow: sold30(1500)}), p, nkNow)
	assert.Equal(t, 1030, d.Fee)
	assert.Equal(t, "below target", d.Reason)
	assert.True(t, d.Publish)
	// a tiny fee still moves by 1 ppm
	assert.Equal(t, 2, NkDecide(idle(NkInput{Fee: 1, LocalPct: 20, TargetPct: 50}), p, nkNow).Fee)
	// above target, no flow: down 1 %
	d = NkDecide(idle(NkInput{Fee: 1000, LocalPct: 80, TargetPct: 50}), p, nkNow)
	assert.Equal(t, 990, d.Fee)
	assert.Equal(t, "above target", d.Reason)
	// never below the minimum rate
	assert.Equal(t, 1, NkDecide(idle(NkInput{Fee: 1, LocalPct: 80, TargetPct: 50}), p, nkNow).Fee)
}

func TestNkDecideLock(t *testing.T) {
	p := nkTestParams()
	in := idle(NkInput{Fee: 2000, LocalPct: 60, TargetPct: 50, Flow: sold30(1500)})
	in.LastPublish = nkNow.Add(-time.Minute)
	in.Flow.LastBig = nkNow.Add(-time.Hour)
	// fee at the reference, 60 % local: 12 min + 348 min x 1 x 0.4 = 151.2 min
	d := NkDecide(in, p, nkNow)
	assert.Equal(t, 2000, d.Fee)
	assert.Equal(t, "lock", d.Reason)
	assert.Equal(t, in.Flow.LastBig.Add(151*time.Minute+12*time.Second), d.LockUntil)
	assert.False(t, d.Publish, "nothing to send, republish not due")

	// a cheap source is locked 12 minutes only
	in.Fee = 1
	in.Flow.LastBig = nkNow.Add(-13 * time.Minute)
	assert.Equal(t, "above target", NkDecide(in, p, nkNow).Reason)
	// the lock never blocks an increase
	in.Fee, in.LocalPct = 2000, 20
	in.Flow.LastBig = nkNow.Add(-time.Minute)
	assert.Equal(t, 2060, NkDecide(in, p, nkNow).Fee)
}

func TestNkDecideAnchor(t *testing.T) {
	p := nkTestParams()
	// sold at 800: a fee of 1200 drops to 110 % = 880 at once
	in := idle(NkInput{Fee: 1200, LocalPct: 80, TargetPct: 50, Flow: sold24(800, 1_000_000)})
	d := NkDecide(in, p, nkNow)
	assert.Equal(t, 880, d.Fee)
	assert.Equal(t, "to anchor", d.Reason)
	// below the sold price it falls at half speed
	in.Fee = 700
	d = NkDecide(in, p, nkNow)
	assert.Equal(t, 696, d.Fee) // floor(700 x 0.995)
	assert.Equal(t, "above target, below flow", d.Reason)
}

func TestNkDecideCapAndEmpty(t *testing.T) {
	p := nkTestParams()
	// cap: 2 x 800 = 1600
	in := idle(NkInput{Fee: 1590, LocalPct: 10, TargetPct: 50, Flow: sold24(800, 1_000_000)})
	d := NkDecide(in, p, nkNow)
	assert.Equal(t, 1600, d.Fee)
	assert.Equal(t, 1600, d.Cap)
	assert.Equal(t, "below target, cap", d.Reason)
	// cap at least NK-CapMin
	assert.Equal(t, 1000, NkCap(sold24(100, 1_000_000), p))
	// no sale in 72 h: the sold price of 30 days
	assert.Equal(t, 1200, NkCap(NkFlow{All30dAmt: 1_000_000, All30dFee: 600}, p))
	// no sale in 30 days: NK-CapMin
	assert.Equal(t, 1000, NkCap(NkFlow{}, p))
	// empty: straight to the cap
	in.Fee, in.LocalPct = 500, 1
	d = NkDecide(in, p, nkNow)
	assert.True(t, d.Empty)
	assert.Equal(t, 1600, d.Fee)
	assert.Equal(t, "empty", d.Reason)
}

func TestNkDecidePacing(t *testing.T) {
	p := nkTestParams()
	in := NkInput{Fee: 1000, LocalPct: 20, TargetPct: 50, Flow: sold30(1500), LastChange: nkNow.Add(-5 * time.Minute), LastPublish: nkNow.Add(-5 * time.Minute)}
	d := NkDecide(in, p, nkNow)
	assert.Equal(t, 1000, d.Fee, "a change waits for NK-MinInterval")
	assert.Equal(t, "below target, wait", d.Reason)
	assert.False(t, d.Publish)
	in.LastPublish = nkNow.Add(-31 * time.Minute)
	d = NkDecide(in, p, nkNow)
	assert.Equal(t, 1000, d.Fee)
	assert.True(t, d.Publish, "republish when due")
}

func TestNkDecideInbound(t *testing.T) {
	p := nkTestParams()
	flow := sold24(794, 1_000_000)
	flow.Fwd6hAmt, flow.Fwd6hFee = 1_000_000, 794
	in := idle(NkInput{Fee: 1200, LocalPct: 20, TargetPct: 50, Flow: flow, InboundDynamic: true})
	d := NkDecide(in, p, nkNow)
	assert.Equal(t, -783, d.Inbound, "sold price less the margin")
	// at or above the target: no discount
	in.LocalPct, in.Inbound = 50, -783
	assert.Equal(t, 0, NkDecide(in, p, nkNow).Inbound)
	in.LocalPct = 20
	// never more than the own outbound fee
	in.Flow = sold24(5000, 1_000_000)
	in.Fee, in.LocalPct = 100, 20
	d = NkDecide(in, p, nkNow)
	assert.Equal(t, -d.Fee, d.Inbound)
	// without dynamic inbound a discount goes back to 0, a manual positive rate stays
	in.InboundDynamic, in.Inbound = false, -300
	assert.Equal(t, 0, NkDecide(in, p, nkNow).Inbound)
	in.Inbound = 50
	assert.Equal(t, 50, NkDecide(in, p, nkNow).Inbound)
	// no sale in 72 h: the discount stays
	in.InboundDynamic, in.Inbound, in.Flow = true, -400, NkFlow{}
	assert.Equal(t, -400, NkDecide(in, p, nkNow).Inbound)
}

func TestNkMaxHtlc(t *testing.T) {
	// 10M channel, max_htlc at capacity, 3M available: halved to 2.5M (inside 1.5M-3M)
	assert.Equal(t, int64(2_500_000_000), NkMaxHtlc(10_000_000_000, 3_000_000, 10_000_000, 1000, false))
	// inside the band: unchanged
	assert.Equal(t, int64(2_500_000_000), NkMaxHtlc(2_500_000_000, 4_000_000, 10_000_000, 1000, false))
	// below half: doubled
	assert.Equal(t, int64(5_000_000_000), NkMaxHtlc(2_500_000_000, 6_000_000, 10_000_000, 1000, false))
	// empty or nearly empty: minimum (1000 sats or min_htlc)
	assert.Equal(t, int64(1_000_000), NkMaxHtlc(2_500_000_000, 6_000_000, 10_000_000, 1000, true))
	assert.Equal(t, int64(2_000_000), NkMaxHtlc(2_500_000_000, 500, 10_000_000, 2_000_000, false))
}

func TestNkBudgetPpm(t *testing.T) {
	p := nkTestParams()
	flow := sold24(5000, 1_000_000)
	// empty: 5000 - 10
	assert.Equal(t, 4990, NkBudgetPpm(6000, flow, 0, 50, p))
	// at the target: 5000 - 20 %
	assert.Equal(t, 4000, NkBudgetPpm(6000, flow, 50, 50, p))
	// the current fee when it is below the sold price
	assert.Equal(t, 2990, NkBudgetPpm(3000, flow, 0, 50, p))
	// no sale in 72 h: the sold price of 30 days
	assert.Equal(t, 590, NkBudgetPpm(3000, NkFlow{All30dAmt: 1_000_000, All30dFee: 600}, 0, 50, p))
	// no sale in 30 days: NK-CapMin, never the raised fee of an empty channel
	assert.Equal(t, 990, NkBudgetPpm(3000, NkFlow{}, 0, 50, p))
	assert.Equal(t, 490, NkBudgetPpm(500, NkFlow{}, 0, 50, p))
}

type fakeNkQ struct {
	settings map[string]string
	fwd      []db.NkForwardFlowsRow
}

func (f *fakeNkQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeNkQ) NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error) {
	return f.fwd, nil
}
func (f *fakeNkQ) NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error) {
	return []db.NkBalanceOutFlowsRow{{ChanID: "1", Amt24h: 1_000_000, Fee24h: 1000, Amt72h: 1_000_000, Fee72h: 1000,
		LastBig: pgtype.Timestamptz{Time: nkNow, Valid: true}}}, nil
}

func TestLoadNkFlowsAddsBalanceOuts(t *testing.T) {
	q := &fakeNkQ{fwd: []db.NkForwardFlowsRow{{ChanID: "1", Amt24h: 1_000_000, Fee24h: 3000, Amt72h: 1_000_000, Fee72h: 3000,
		LastBig: pgtype.Timestamptz{Time: nkNow.Add(-time.Hour), Valid: true}}}}
	m, err := LoadNkFlows(context.Background(), q, []string{"1"}, 2, nkNow)
	require.NoError(t, err)
	f := m["1"]
	a, _ := f.Anchor()
	assert.Equal(t, 2000, a, "forwards at 3000 and balance-outs at 1000 count alike")
	s, _ := f.SoldPpm()
	assert.Equal(t, 3000, s, "the inbound price uses forwards only")
	assert.Equal(t, nkNow, f.LastBig)
}

func TestTargetBudget(t *testing.T) {
	q := &fakeNkQ{}
	tc := TargetChannel{ChanID: "1", LocalFeeRate: 3000, ArMaxCost: 90, ArInTarget: 50, Capacity: 10_000_000, AfMode: "lndg"}
	fee, cost, err := TargetBudget(context.Background(), q, tc, nkNow)
	require.NoError(t, err)
	assert.Equal(t, []int{3000, 90}, []int{fee, cost})

	// NK: min(3000, 1000 sold) - 10 at an empty channel, at 100 %
	tc.AfMode = "nk"
	fee, cost, err = TargetBudget(context.Background(), q, tc, nkNow)
	require.NoError(t, err)
	assert.Equal(t, []int{990, 100}, []int{fee, cost})
}

func TestLoadNkParamsDefaults(t *testing.T) {
	p, err := LoadNkParams(context.Background(), &fakeNkQ{settings: map[string]string{"NK-StepUp": "5"}})
	require.NoError(t, err)
	want := nkTestParams()
	want.Shadow = true
	want.StepUpPct = 5
	assert.Equal(t, want, p)
}

type keyRecorder struct{ keys []string }

func (k *keyRecorder) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	k.keys = append(k.keys, arg.Key)
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}

// gui_localsettings.key is varchar(20).
func TestNkSettingKeysFit(t *testing.T) {
	k := &keyRecorder{}
	_, err := LoadNkParams(context.Background(), k)
	require.NoError(t, err)
	for _, key := range k.keys {
		assert.LessOrEqual(t, len(key), 20, key)
	}
}
