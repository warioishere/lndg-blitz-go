package rebalancer

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// --- annotateChannel -----------------------------------------------------

func TestAnnotateChannelIntegerMode(t *testing.T) {
	ch := db.GuiChannel{
		LocalBalance: 1_000_000, PendingOutbound: 0, LocalChanReserve: 10_000,
		Capacity: 2_000_000, RemoteBalance: 900_000, PendingInbound: 0, ArInTarget: 50,
	}
	a := annotateChannel(ch, 100_000, true)
	assert.Equal(t, 44.0, a.percentOutbound) // (990000-100000)*100/2000000 = 44 (int)
	assert.Equal(t, int64(0), a.inboundCan)  // (900000*100/2000000=45)/50 = 0
}

func TestAnnotateChannelFloatMode(t *testing.T) {
	ch := db.GuiChannel{
		LocalBalance: 1_000_000, PendingOutbound: 0, LocalChanReserve: 10_000,
		Capacity: 2_000_000, RemoteBalance: 900_000, ArInTarget: 50,
	}
	a := annotateChannel(ch, 121_000, false) // value*inc, real division
	assert.InDelta(t, 43.45, a.percentOutbound, 1e-9)
}

// --- getOutCans ----------------------------------------------------------

func ac(chanID string, pct float64, arOut int32, htlc int32, localDisabled, arSource, autoReb bool, remote string) annotatedChannel {
	return annotatedChannel{
		ch: db.GuiChannel{
			ChanID: chanID, ArOutTarget: arOut, HtlcCount: htlc,
			LocalDisabled: localDisabled, ArSource: arSource, AutoRebalance: autoReb, RemotePubkey: remote,
		},
		percentOutbound: pct,
	}
}

func TestGetOutCansFiltersAndOrders(t *testing.T) {
	e := newEngine()
	chans := []annotatedChannel{
		ac("1", 50, 20, 5, false, false, false, "peerA"), // ok (auto_rebalance=false)
		ac("2", 10, 20, 1, false, true, true, "peerB"),   // pct<arOut -> out
		ac("3", 50, 20, 2, true, true, true, "peerC"),    // local_disabled -> out
		ac("4", 50, 20, 1, false, false, true, "peerD"),  // !ar_source && auto_rebalance -> out
		ac("5", 50, 20, 0, false, true, true, "peerE"),   // ok (ar_source)
		ac("6", 50, 20, 3, false, true, true, "lasthop"), // remote==lasthop -> out
	}
	got := e.getOutCans(chans, "lasthop")
	// Remaining: "1"(htlc5), "5"(htlc0) -> sorted by htlc_count: "5","1".
	assert.Equal(t, []string{"5", "1"}, got)
}

// --- sortChannelsByHtlc --------------------------------------------------

type fakeListChannels struct {
	resp *lnrpc.ListChannelsResponse
	err  error
}

func (f *fakeListChannels) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	return f.resp, f.err
}

func TestSortChannelsByHtlcPicksLowest(t *testing.T) {
	e := newEngine()
	// peerA has chan 1 (2 HTLCs) and chan 2 (0 HTLCs) -> chan 2 wins. peerB has single chan 3.
	resp := &lnrpc.ListChannelsResponse{Channels: []*lnrpc.Channel{
		{ChanId: 1, RemotePubkey: "peerA", PendingHtlcs: []*lnrpc.HTLC{{}, {}}},
		{ChanId: 2, RemotePubkey: "peerA", PendingHtlcs: []*lnrpc.HTLC{}},
		{ChanId: 3, RemotePubkey: "peerB", PendingHtlcs: []*lnrpc.HTLC{{}}},
	}}
	got := e.sortChannelsByHtlc(context.Background(), &fakeListChannels{resp: resp}, []string{"1", "2", "3"})
	assert.Equal(t, []string{"2", "3"}, got)
}

func TestSortChannelsByHtlcSingleNoop(t *testing.T) {
	e := newEngine()
	got := e.sortChannelsByHtlc(context.Background(), &fakeListChannels{}, []string{"1"})
	assert.Equal(t, []string{"1"}, got)
}

func TestSortChannelsByHtlcDropsUnknown(t *testing.T) {
	e := newEngine()
	// chan 9 is unknown to LND -> dropped.
	resp := &lnrpc.ListChannelsResponse{Channels: []*lnrpc.Channel{
		{ChanId: 1, RemotePubkey: "peerA", PendingHtlcs: []*lnrpc.HTLC{}},
	}}
	got := e.sortChannelsByHtlc(context.Background(), &fakeListChannels{resp: resp}, []string{"1", "9"})
	assert.Equal(t, []string{"1"}, got)
}

// --- format/parse chan_ids ----------------------------------------------

func TestFormatAndParseChanIDs(t *testing.T) {
	assert.Equal(t, "[123, 456]", formatChanIDList([]string{"123", "456"}))
	assert.Equal(t, "[]", formatChanIDList([]string{}))
	assert.Equal(t, []string{"123", "456"}, parseChanIDs("[123, 456]"))
	assert.Equal(t, []string{}, parseChanIDs("[]"))
	assert.Equal(t, []string{"7"}, parseChanIDs("[7]"))
}

// --- estimateLiquidity ---------------------------------------------------

func TestEstimateLiquidity(t *testing.T) {
	assert.Equal(t, int64(0), estimateLiquidity(nil))
	// FAILED, one attempt with failure at the last hop (fsi==total_hops) -> total_amt.
	p := &lnrpc.Payment{
		Status: lnrpc.Payment_FAILED,
		Htlcs: []*lnrpc.HTLCAttempt{
			{Route: &lnrpc.Route{TotalAmt: 50000, Hops: []*lnrpc.Hop{{}, {}}}, Failure: &lnrpc.Failure{FailureSourceIndex: 2}},
			{Route: &lnrpc.Route{TotalAmt: 30000, Hops: []*lnrpc.Hop{{}, {}}}, Failure: &lnrpc.Failure{FailureSourceIndex: 1}}, // not the last hop
		},
	}
	assert.Equal(t, int64(50000), estimateLiquidity(p))
}

func TestEstimateLiquidityNotFailed(t *testing.T) {
	p := &lnrpc.Payment{Status: lnrpc.Payment_SUCCEEDED}
	assert.Equal(t, int64(0), estimateLiquidity(p))
}

// --- fakeOrchQ -----------------------------------------------------------

type fakeOrchQ struct {
	settings   map[string]string
	feeDiff    []db.ListChannelFeeAndDiffByIDsRow
	target     db.GetTargetChannelInfoRow
	targetErr  error
	drainRows  []db.ListRemainingDrainChannelsRow
	allowed    []string
	balances   map[string]db.GetChannelBalancesRow
	setBal     []db.SetChannelBalancesParams
	insertArgs []db.InsertRebalancerRecordParams
}

func newFakeOrchQ() *fakeOrchQ {
	return &fakeOrchQ{settings: map[string]string{}, balances: map[string]db.GetChannelBalancesRow{}}
}

func (f *fakeOrchQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeOrchQ) ListChannelFeeAndDiffByIDs(ctx context.Context, dollar_1 []string) ([]db.ListChannelFeeAndDiffByIDsRow, error) {
	return f.feeDiff, nil
}
func (f *fakeOrchQ) GetTargetChannelInfo(ctx context.Context, remotePubkey string) (db.GetTargetChannelInfoRow, error) {
	return f.target, f.targetErr
}
func (f *fakeOrchQ) ListRemainingDrainChannels(ctx context.Context, remotePubkey string) ([]db.ListRemainingDrainChannelsRow, error) {
	return f.drainRows, nil
}
func (f *fakeOrchQ) ListAllowedTargetSources(ctx context.Context, targetPubkey string) ([]string, error) {
	return f.allowed, nil
}
func (f *fakeOrchQ) GetChannelBalances(ctx context.Context, chanID string) (db.GetChannelBalancesRow, error) {
	if b, ok := f.balances[chanID]; ok {
		return b, nil
	}
	return db.GetChannelBalancesRow{}, pgx.ErrNoRows
}
func (f *fakeOrchQ) SetChannelBalances(ctx context.Context, arg db.SetChannelBalancesParams) error {
	f.setBal = append(f.setBal, arg)
	return nil
}

func TestGetSourceMaps(t *testing.T) {
	q := newFakeOrchQ()
	q.feeDiff = []db.ListChannelFeeAndDiffByIDsRow{
		{ChanID: "1", LocalFeeRate: 100, ArSourcePpmDiff: 5},
		{ChanID: "2", LocalFeeRate: 200, ArSourcePpmDiff: 0},
	}
	fee, diff, err := getSourceMaps(context.Background(), q, []string{"1", "2"})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"1": 100, "2": 200}, fee)
	assert.Equal(t, map[string]int{"1": 5, "2": 0}, diff)
}

func TestGetTargetInfo(t *testing.T) {
	q := newFakeOrchQ()
	q.target = db.GetTargetChannelInfoRow{LocalFeeRate: 350, ArMaxCost: 50}
	fee, cost, ok, err := getTargetInfo(context.Background(), q, "peer")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 350, fee)
	assert.Equal(t, 50, cost)

	q.targetErr = pgx.ErrNoRows
	_, _, ok, err = getTargetInfo(context.Background(), q, "peer")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestGetAllowedSources(t *testing.T) {
	q := newFakeOrchQ()
	_, ok, err := getAllowedSourcesForTarget(context.Background(), q, "peer")
	require.NoError(t, err)
	assert.False(t, ok) // empty -> none

	q.allowed = []string{"1", "2"}
	ids, ok, err := getAllowedSourcesForTarget(context.Background(), q, "peer")
	require.NoError(t, err)
	assert.True(t, ok)
	_, has1 := ids["1"]
	assert.True(t, has1)
}

func TestRemainingDrain(t *testing.T) {
	q := newFakeOrchQ()
	// chan: cur_in=900000, tgt_in=2000000*40/100=800000 -> 100000. second is under target -> 0.
	q.drainRows = []db.ListRemainingDrainChannelsRow{
		{RemoteBalance: 900_000, PendingInbound: 0, Capacity: 2_000_000, ArInTarget: 40},
		{RemoteBalance: 100_000, PendingInbound: 0, Capacity: 2_000_000, ArInTarget: 40},
	}
	got, err := remainingDrain(context.Background(), q, "peer")
	require.NoError(t, err)
	assert.Equal(t, int64(100_000), got)
}

func TestUpdateChannelBalances(t *testing.T) {
	q := newFakeOrchQ()
	q.balances["111"] = db.GetChannelBalancesRow{LocalBalance: 1, RemoteBalance: 2}
	q.balances["222"] = db.GetChannelBalancesRow{LocalBalance: 3, RemoteBalance: 4}
	lc := &fakeListChannels{resp: &lnrpc.ListChannelsResponse{Channels: []*lnrpc.Channel{
		{ChanId: 111, LocalBalance: 10, RemoteBalance: 20},
		{ChanId: 222, LocalBalance: 30, RemoteBalance: 40},
	}}}
	updateChannelBalances(context.Background(), q, lc, 111, 222)
	require.Len(t, q.setBal, 2)
	assert.Equal(t, "111", q.setBal[0].ChanID)
	assert.Equal(t, int64(10), q.setBal[0].LocalBalance)
	assert.Equal(t, "222", q.setBal[1].ChanID)
	assert.Equal(t, int64(40), q.setBal[1].RemoteBalance)
}

func TestSettingsFlags(t *testing.T) {
	q := newFakeOrchQ()
	q.settings["LND-DisableMPP"] = "1"
	allow, err := checkAndSetAllowMultishards(context.Background(), q)
	require.NoError(t, err)
	assert.False(t, allow)

	q.settings["AR-PerSourceEnabled"] = "1"
	ps, err := getPerSourceEnabled(context.Background(), q)
	require.NoError(t, err)
	assert.True(t, ps)

	q.settings["RR-UseSavedRoutes"] = "0"
	assert.False(t, savedRoutesEnabled(context.Background(), q))

	q.settings["RR-RouteLimit"] = "7"
	assert.Equal(t, 7, getRouteLimit(context.Background(), q))
}
