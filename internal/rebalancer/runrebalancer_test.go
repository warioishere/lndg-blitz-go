package rebalancer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// --- fakeRebalQ: complete rebalancerQuerier implementation --------------------------

type fakeRebalQ struct {
	settings    map[string]string
	activeChans []db.GuiChannel
	// activeChansAfter, when set, is returned from the second channel listing on:
	// the state after the rebalance moved liquidity.
	activeChansAfter []db.GuiChannel
	activeCalls      int
	feeDiff          []db.ListChannelFeeAndDiffByIDsRow
	target           db.GetTargetChannelInfoRow
	targetErr        error
	drainRows        []db.ListRemainingDrainChannelsRow
	balances         map[string]db.GetChannelBalancesRow
	inserted         []db.InsertRebalancerRecordParams
	updated          []db.UpdateRebalancerRecordParams
	nextID           int64

	activePubkeys  []string
	allowedTargets []db.ListAllAllowedTargetsRow
	lastRebal      map[string]db.GetLastRebalanceForPubkeyRow
	aggIn          []db.AggForwardsInSinceRow
	aggOut         []db.AggForwardsOutSinceRow
	setAR          []db.SetChannelAutoRebalanceParams
	autopilots     []db.InsertAutopilotParams

	mu         sync.Mutex
	pending    []db.GuiRebalancer
	statusByID map[int64]int32
}

func (f *fakeRebalQ) ListActiveRebalancePubkeys(ctx context.Context) ([]string, error) {
	return f.activePubkeys, nil
}
func (f *fakeRebalQ) ListAllAllowedTargets(ctx context.Context) ([]db.ListAllAllowedTargetsRow, error) {
	return f.allowedTargets, nil
}
func (f *fakeRebalQ) GetLastRebalanceForPubkey(ctx context.Context, lastHopPubkey string) (db.GetLastRebalanceForPubkeyRow, error) {
	if r, ok := f.lastRebal[lastHopPubkey]; ok {
		return r, nil
	}
	return db.GetLastRebalanceForPubkeyRow{}, pgx.ErrNoRows
}
func (f *fakeRebalQ) AggForwardsInSince(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.AggForwardsInSinceRow, error) {
	return f.aggIn, nil
}
func (f *fakeRebalQ) AggForwardsOutSince(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.AggForwardsOutSinceRow, error) {
	return f.aggOut, nil
}
func (f *fakeRebalQ) SetChannelAutoRebalance(ctx context.Context, arg db.SetChannelAutoRebalanceParams) error {
	f.setAR = append(f.setAR, arg)
	return nil
}
func (f *fakeRebalQ) InsertAutopilot(ctx context.Context, arg db.InsertAutopilotParams) error {
	f.autopilots = append(f.autopilots, arg)
	return nil
}

func newFakeRebalQ() *fakeRebalQ {
	return &fakeRebalQ{settings: map[string]string{}, balances: map[string]db.GetChannelBalancesRow{}, nextID: 99}
}

func (f *fakeRebalQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}

// routeQuerier / reputationQuerier
func (f *fakeRebalQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeRebalQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	return nil
}
func (f *fakeRebalQ) UpdateRebalanceRoute(ctx context.Context, arg db.UpdateRebalanceRouteParams) error {
	return nil
}
func (f *fakeRebalQ) UpdateRebalanceRouteFee(ctx context.Context, arg db.UpdateRebalanceRouteFeeParams) error {
	return nil
}
func (f *fakeRebalQ) MarkRebalanceRouteFailure(ctx context.Context, arg db.MarkRebalanceRouteFailureParams) error {
	return nil
}
func (f *fakeRebalQ) IncrNodeReputationSuccess(ctx context.Context, arg db.IncrNodeReputationSuccessParams) error {
	return nil
}
func (f *fakeRebalQ) IncrNodeReputationFailure(ctx context.Context, arg db.IncrNodeReputationFailureParams) error {
	return nil
}

// savedRoutesQuerier
func (f *fakeRebalQ) ListUntestedNoFeeRoutes(ctx context.Context, arg db.ListUntestedNoFeeRoutesParams) ([]db.GuiRebalanceroute, error) {
	return nil, nil
}
func (f *fakeRebalQ) ListUntestedFeeKnownRoutes(ctx context.Context, arg db.ListUntestedFeeKnownRoutesParams) ([]db.GuiRebalanceroute, error) {
	return nil, nil
}
func (f *fakeRebalQ) ListTestedRoutes(ctx context.Context, arg db.ListTestedRoutesParams) ([]db.GuiRebalanceroute, error) {
	return nil, nil
}
func (f *fakeRebalQ) ListNodeReputations(ctx context.Context, dollar_1 []string) ([]db.ListNodeReputationsRow, error) {
	return nil, nil
}

// purgeQuerier
func (f *fakeRebalQ) ListOpenChannelIDs(ctx context.Context) ([]string, error) { return nil, nil }
func (f *fakeRebalQ) DeleteRoutesNotOpenOutgoing(ctx context.Context, dollar_1 []string) (int64, error) {
	return 0, nil
}
func (f *fakeRebalQ) DeleteStaleTestedRoutes(ctx context.Context, lastSuccess pgtype.Timestamptz) error {
	return nil
}
func (f *fakeRebalQ) DeleteStaleNodeReputations(ctx context.Context, lastSuccess pgtype.Timestamptz) error {
	return nil
}

// orchestrator queriers
func (f *fakeRebalQ) ListChannelFeeAndDiffByIDs(ctx context.Context, dollar_1 []string) ([]db.ListChannelFeeAndDiffByIDsRow, error) {
	return f.feeDiff, nil
}
func (f *fakeRebalQ) GetTargetChannelInfo(ctx context.Context, remotePubkey string) (db.GetTargetChannelInfoRow, error) {
	return f.target, f.targetErr
}
func (f *fakeRebalQ) ListAllowedTargetSources(ctx context.Context, targetPubkey string) ([]string, error) {
	return nil, nil
}
func (f *fakeRebalQ) ListRemainingDrainChannels(ctx context.Context, remotePubkey string) ([]db.ListRemainingDrainChannelsRow, error) {
	return f.drainRows, nil
}
func (f *fakeRebalQ) GetChannelBalances(ctx context.Context, chanID string) (db.GetChannelBalancesRow, error) {
	if b, ok := f.balances[chanID]; ok {
		return b, nil
	}
	return db.GetChannelBalancesRow{}, pgx.ErrNoRows
}
func (f *fakeRebalQ) SetChannelBalances(ctx context.Context, arg db.SetChannelBalancesParams) error {
	return nil
}
func (f *fakeRebalQ) ListChannelAliases(ctx context.Context) ([]db.ListChannelAliasesRow, error) {
	return nil, nil
}
func (f *fakeRebalQ) ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error) {
	f.activeCalls++
	if f.activeCalls > 1 && f.activeChansAfter != nil {
		return f.activeChansAfter, nil
	}
	return f.activeChans, nil
}
func (f *fakeRebalQ) InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inserted = append(f.inserted, arg)
	return f.nextID, nil
}
func (f *fakeRebalQ) UpdateRebalancerRecord(ctx context.Context, arg db.UpdateRebalancerRecordParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updated = append(f.updated, arg)
	if f.statusByID == nil {
		f.statusByID = map[int64]int32{}
	}
	f.statusByID[arg.ID] = arg.Status
	return nil
}

// queueQuerier
func (f *fakeRebalQ) ListPendingRebalances(ctx context.Context) ([]db.GuiRebalancer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []db.GuiRebalancer
	for _, r := range f.pending {
		if f.statusByID == nil || f.statusByID[r.ID] == 0 {
			out = append(out, r)
		}
	}
	return out, nil
}
func (f *fakeRebalQ) MarkInFlightRebalancesError(ctx context.Context, stop pgtype.Timestamptz) error {
	return nil
}

// --- fake router with streaming SendPaymentV2 -----------------------------

type fakePaymentStream struct {
	grpc.ClientStream
	msgs []*lnrpc.Payment
	idx  int
}

func (f *fakePaymentStream) Recv() (*lnrpc.Payment, error) {
	if f.idx >= len(f.msgs) {
		return nil, io.EOF
	}
	m := f.msgs[f.idx]
	f.idx++
	return m, nil
}

type fakeRebalRouter struct {
	payments []*lnrpc.Payment
}

func (f *fakeRebalRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{}}, nil
}
func (f *fakeRebalRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED}, nil
}
func (f *fakeRebalRouter) SendPaymentV2(ctx context.Context, in *routerrpc.SendPaymentRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.Payment], error) {
	return &fakePaymentStream{msgs: f.payments}, nil
}

// chanOut/chanIn helpers
func chanOut() db.GuiChannel {
	return db.GuiChannel{
		ChanID: "777", RemotePubkey: "peerOut", IsActive: true, IsOpen: true, Private: false,
		Capacity: 10_000_000, LocalBalance: 9_000_000, ArOutTarget: 50, HtlcCount: 0,
		ArSource: true, AutoRebalance: true,
	}
}
func chanIn() db.GuiChannel {
	return db.GuiChannel{
		ChanID: "888", RemotePubkey: "02aabbccddeeff", IsActive: true, IsOpen: true, Private: false,
		Capacity: 2_000_000, LocalBalance: 100_000, RemoteBalance: 1_900_000, ArInTarget: 50,
		AutoRebalance: true, RemoteDisabled: false,
	}
}

func fixedNow() func() time.Time {
	t := time.Unix(1_700_000_000, 0)
	return func() time.Time { return t }
}

func TestRunRebalancerNoOutbound(t *testing.T) {
	q := newFakeRebalQ() // no channels -> no outbound_cans
	e := newEngine()
	rb := &db.GuiRebalancer{ID: 1, Value: 100000, LastHopPubkey: "02aabbccddeeff", Duration: 1, FeeLimit: 1000}
	next := e.runRebalancer(context.Background(), &fakeLN{}, &fakeRebalRouter{}, q, rb, "Worker0", fixedNow())
	assert.Nil(t, next)
	assert.Equal(t, int32(406), rb.Status)
}

func TestRunRebalancerSendPaymentSuccessRapidFire(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["RR-UseSavedRoutes"] = "0" // saved routes off -> go straight to SendPaymentV2
	q.targetErr = pgx.ErrNoRows           // opp_cost disabled
	q.activeChans = []db.GuiChannel{chanOut(), chanIn()}
	q.drainRows = []db.ListRemainingDrainChannelsRow{
		{RemoteBalance: 1_900_000, PendingInbound: 0, Capacity: 2_000_000, ArInTarget: 50}, // drain 900000
	}
	ln := &fakeLN{invoice: &lnrpc.AddInvoiceResponse{RHash: []byte{0xaa}, PaymentRequest: "lnbc1"}}
	router := &fakeRebalRouter{payments: []*lnrpc.Payment{
		{
			Status:  lnrpc.Payment_SUCCEEDED,
			FeeMsat: 5000,
			Htlcs: []*lnrpc.HTLCAttempt{
				{Route: &lnrpc.Route{TotalFeesMsat: 5000, Hops: []*lnrpc.Hop{{ChanId: 111}, {ChanId: 222}}}},
			},
		},
	}}
	e := newEngine()
	rb := &db.GuiRebalancer{ID: 1, Value: 100000, LastHopPubkey: "02aabbccddeeff", Duration: 1, FeeLimit: 1000}
	next := e.runRebalancer(context.Background(), ln, router, q, rb, "Worker0", fixedNow())

	assert.Equal(t, int32(2), rb.Status)
	require.NotNil(t, next)
	assert.Equal(t, int32(121000), next.Value) // min(int(100000*1.21), drain 900000)
	assert.Equal(t, int32(1), next.Status)
	assert.Equal(t, "[777]", next.OutgoingChanIds)
	assert.Equal(t, "02aabbccddeeff", next.LastHopPubkey)
	assert.InDelta(t, 1210.0, next.FeeLimit, 1e-9)
	require.Len(t, q.inserted, 1)
}

func TestRunRebalancerSendPaymentFailureDecrease(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["RR-UseSavedRoutes"] = "0"
	q.targetErr = pgx.ErrNoRows
	q.activeChans = []db.GuiChannel{chanOut(), chanIn()}
	ln := &fakeLN{invoice: &lnrpc.AddInvoiceResponse{RHash: []byte{0xaa}, PaymentRequest: "lnbc1"}}
	router := &fakeRebalRouter{payments: []*lnrpc.Payment{
		{Status: lnrpc.Payment_FAILED, FailureReason: lnrpc.PaymentFailureReason_FAILURE_REASON_NO_ROUTE},
	}}
	e := newEngine()
	rb := &db.GuiRebalancer{ID: 1, Value: 100000, LastHopPubkey: "02aabbccddeeff", Duration: 1, FeeLimit: 1000}
	next := e.runRebalancer(context.Background(), ln, router, q, rb, "Worker0", fixedNow())

	assert.Equal(t, int32(4), rb.Status) // NO_ROUTE
	require.NotNil(t, next)
	assert.Equal(t, int32(50000), next.Value) // 100000/2
	assert.InDelta(t, 500.0, next.FeeLimit, 1e-9)
	assert.Equal(t, int32(1), next.Status)
}

// RapidFire after a success must judge the target with the balances after the
// rebalance: here the target is already back below its inbound threshold.
func TestRunRebalancerRapidFireUsesFreshBalances(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["RR-UseSavedRoutes"] = "0"
	q.targetErr = pgx.ErrNoRows
	q.activeChans = []db.GuiChannel{chanOut(), chanIn()}
	refilled := chanIn()
	refilled.LocalBalance, refilled.RemoteBalance = 1_100_000, 900_000 // inbound_can (45%/50) = 0
	q.activeChansAfter = []db.GuiChannel{chanOut(), refilled}
	q.drainRows = []db.ListRemainingDrainChannelsRow{
		{RemoteBalance: 1_900_000, Capacity: 2_000_000, ArInTarget: 50}, // drain alone would allow more
	}
	ln := &fakeLN{invoice: &lnrpc.AddInvoiceResponse{RHash: []byte{0xaa}, PaymentRequest: "lnbc1"}}
	router := &fakeRebalRouter{payments: []*lnrpc.Payment{{
		Status: lnrpc.Payment_SUCCEEDED, FeeMsat: 5000,
		Htlcs: []*lnrpc.HTLCAttempt{{Route: &lnrpc.Route{TotalFeesMsat: 5000, Hops: []*lnrpc.Hop{{ChanId: 111}, {ChanId: 222}}}}},
	}}}
	rb := &db.GuiRebalancer{ID: 1, Value: 100000, LastHopPubkey: "02aabbccddeeff", Duration: 1, FeeLimit: 1000}
	next := newEngine().runRebalancer(context.Background(), ln, router, q, rb, "Worker0", fixedNow())
	assert.Equal(t, int32(2), rb.Status)
	assert.Nil(t, next)
	assert.Empty(t, q.inserted)
}

// 406 (no source left after the opportunity-cost filter) does not depend on the
// amount, so no smaller RapidFire retry is queued.
func TestRunRebalancer406NoDecrease(t *testing.T) {
	q := newFakeRebalQ()
	q.activeChans = []db.GuiChannel{chanOut(), chanIn()}
	q.target = db.GetTargetChannelInfoRow{LocalFeeRate: 100, ArMaxCost: 50}
	q.feeDiff = []db.ListChannelFeeAndDiffByIDsRow{{ChanID: "777", LocalFeeRate: 500}} // 100 < 500 -> excluded
	rb := &db.GuiRebalancer{ID: 1, Value: 1_000_000, LastHopPubkey: "02aabbccddeeff", Duration: 1, FeeLimit: 1000}
	next := newEngine().runRebalancer(context.Background(), &fakeLN{}, &fakeRebalRouter{}, q, rb, "Worker0", fixedNow())
	assert.Equal(t, int32(406), rb.Status)
	assert.Nil(t, next)
	assert.Empty(t, q.inserted)
}
