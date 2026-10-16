package graphwatcher

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
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

// --- fakeQ: in-memory implementation of graphwatcher.Querier ---------------------------

type fakeQ struct {
	settings   map[string]string
	peers      map[string]string
	arPubkeys  []string
	arChannels map[string][]db.GuiChannel
	channels   []db.GuiChannel                            // ListActiveOpenPublicChannels
	pays       map[string][]db.RebalPaymentsForChannelRow // rebal_chan -> payments

	mu          sync.Mutex
	insertedLog []db.InsertGraphProbeLogParams
	events      []db.InsertGraphEventParams
	insertedReb []db.InsertRebalancerRecordParams
	updatedReb  []db.UpdateRebalancerRecordParams

	openPeers     []string
	market        []db.InsertMarketPoliciesParams
	lastSnapshot  pgtype.Timestamptz
	deletedBefore []time.Time
}

func newFakeQ() *fakeQ {
	return &fakeQ{settings: map[string]string{}, peers: map[string]string{}, arChannels: map[string][]db.GuiChannel{}, pays: map[string][]db.RebalPaymentsForChannelRow{}}
}

func (f *fakeQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeQ) UpsertLocalSetting(ctx context.Context, arg db.UpsertLocalSettingParams) error {
	return nil
}
func (f *fakeQ) ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	return nil
}
func (f *fakeQ) UpdateRebalanceRouteProbe(ctx context.Context, arg db.UpdateRebalanceRouteProbeParams) error {
	return nil
}
func (f *fakeQ) InsertProbeLog(ctx context.Context, arg db.InsertProbeLogParams) error { return nil }
func (f *fakeQ) DeleteOldProbeLogs(ctx context.Context) error                          { return nil }
func (f *fakeQ) GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error) {
	if a, ok := f.peers[pubkey]; ok {
		return pgtype.Text{String: a, Valid: true}, nil
	}
	return pgtype.Text{}, pgx.ErrNoRows
}
func (f *fakeQ) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	return db.GuiNodecache{}, pgx.ErrNoRows
}
func (f *fakeQ) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error { return nil }
func (f *fakeQ) ListAutoRebalancePeerPubkeys(ctx context.Context) ([]string, error) {
	return f.arPubkeys, nil
}
func (f *fakeQ) ListOpenARChannelsByPubkey(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error) {
	return f.arChannels[remotePubkey], nil
}
func (f *fakeQ) RebalPaymentsForChannel(ctx context.Context, arg db.RebalPaymentsForChannelParams) ([]db.RebalPaymentsForChannelRow, error) {
	return f.pays[arg.RebalChan.String], nil
}
func (f *fakeQ) InsertGraphProbeLog(ctx context.Context, arg db.InsertGraphProbeLogParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.insertedLog = append(f.insertedLog, arg)
	return nil
}
func (f *fakeQ) InsertGraphEvent(ctx context.Context, arg db.InsertGraphEventParams) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, arg)
	return nil
}
func (f *fakeQ) UpdateGraphEventRoutesFound(ctx context.Context, arg db.UpdateGraphEventRoutesFoundParams) error {
	return nil
}
func (f *fakeQ) DeleteOldGraphEvents(ctx context.Context) error { return nil }
func (f *fakeQ) ListOpenPeerPubkeys(ctx context.Context) ([]string, error) {
	return f.openPeers, nil
}
func (f *fakeQ) InsertMarketPolicies(ctx context.Context, arg []db.InsertMarketPoliciesParams) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.market = append(f.market, arg...)
	return int64(len(arg)), nil
}
func (f *fakeQ) LastMarketSnapshot(ctx context.Context) (pgtype.Timestamptz, error) {
	return f.lastSnapshot, nil
}
func (f *fakeQ) DeleteOldMarketPolicies(ctx context.Context, before pgtype.Timestamptz) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedBefore = append(f.deletedBefore, before.Time)
	return 0, nil
}
func (f *fakeQ) InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error) {
	f.insertedReb = append(f.insertedReb, arg)
	return int64(len(f.insertedReb)), nil
}
func (f *fakeQ) UpdateRebalancerRecord(ctx context.Context, arg db.UpdateRebalancerRecordParams) error {
	f.updatedReb = append(f.updatedReb, arg)
	return nil
}

// --- fakeLN: in-memory implementation of graphwatcher.Client -----------------------------------------

// fakeLN answers QueryRoutes with a direct hop to the requested node when
// routable is set, and GetChanInfo with chanInfo when set.
type fakeLN struct {
	routable bool
	chanInfo *lnrpc.ChannelEdge
}

func (fakeLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (c fakeLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	if !c.routable {
		return &lnrpc.QueryRoutesResponse{}, nil
	}
	return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{{PubKey: in.PubKey}}}}}, nil
}
func (fakeLN) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return nil, errors.New("no node info")
}
func (c fakeLN) GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error) {
	if c.chanInfo == nil {
		return nil, errors.New("no chan info")
	}
	return c.chanInfo, nil
}
func (fakeLN) AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
	return &lnrpc.AddInvoiceResponse{RHash: realHash, PaymentAddr: []byte("addr")}, nil
}
func (fakeLN) SubscribeChannelGraph(ctx context.Context, in *lnrpc.GraphTopologySubscription, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.GraphTopologyUpdate], error) {
	return nil, errors.New("not used")
}

// realHash is the payment hash of the invoice fakeLN.AddInvoice creates.
var realHash = []byte("real-payment-hash")

// verifyRouter verifies every probe. Built routes cost feeMsat, or payFeeMsat
// when built for the invoice (payment address set); payments over them succeed
// unless payFails.
type verifyRouter struct {
	feeMsat, payFeeMsat int64
	payFails            bool
}

func (r verifyRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	hops := make([]*lnrpc.Hop, 0, len(in.HopPubkeys))
	for _, pk := range in.HopPubkeys {
		hops = append(hops, &lnrpc.Hop{PubKey: hex.EncodeToString(pk)})
	}
	fee := r.feeMsat
	if len(in.PaymentAddr) > 0 {
		fee = r.payFeeMsat
	}
	return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{TotalAmtMsat: in.AmtMsat + fee, TotalFeesMsat: fee, Hops: hops}}, nil
}
func (r verifyRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	if string(in.PaymentHash) == string(realHash) {
		if r.payFails {
			return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15, FailureSourceIndex: 1}}, nil
		}
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_SUCCEEDED, Route: in.Route}, nil
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: uint32(len(in.Route.GetHops()))}}, nil
}

func testDeps(q *fakeQ) Deps {
	return Deps{Q: q, LN: fakeLN{}, Router: verifyRouter{}, Now: time.Unix(1_700_000_000, 0).UTC, Sleep: func(time.Duration) {}}
}

func pk(b byte) string { return strings.Repeat(hex.EncodeToString([]byte{b}), 33) }

// srcChannel is a non-AR source with fee feeRate and 80 % outbound.
func srcChannel(chanID string, feeRate int32) db.GuiChannel {
	return db.GuiChannel{ChanID: chanID, RemotePubkey: pk(0x11), Capacity: 1_000_000, LocalBalance: 800_000,
		RemoteBalance: 200_000, LocalChanReserve: 10_000, ArOutTarget: 50, ArInTarget: 50, LocalFeeRate: feeRate}
}

// tgtChannel is an AR channel to the target with room for inbound liquidity;
// budget = 1000 ppm * 80 % = 800 ppm.
func tgtChannel() db.GuiChannel {
	return db.GuiChannel{ChanID: "900", Alias: "Tgt", RemotePubkey: pk(0xbb), RemoteBalance: 1_000_000, LocalChanReserve: 1000,
		LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 100_000}
}

// --- Tests ---------------------------------------------------------------

func TestLoadARTargetsExcludes(t *testing.T) {
	q := newFakeQ()
	q.arPubkeys = []string{"aa", "bb", "cc"}
	q.settings["GW-Exclude"] = "bb, dd"
	got, err := loadARTargets(context.Background(), q)
	require.NoError(t, err)
	_, hasAA := got["aa"]
	_, hasBB := got["bb"]
	_, hasCC := got["cc"]
	assert.True(t, hasAA)
	assert.False(t, hasBB) // excluded
	assert.True(t, hasCC)
}

func TestGetAliasPeerThenTruncate(t *testing.T) {
	q := newFakeQ()
	q.peers["aabbccddee112233"] = "ACINQ"
	d := testDeps(q)
	assert.Equal(t, "ACINQ", d.getAlias(context.Background(), "aabbccddee112233"))
	// unknown pubkey: node cache miss -> first 12 characters.
	assert.Equal(t, "001122334455", d.getAlias(context.Background(), "001122334455667788"))
}

func TestProbeGate(t *testing.T) {
	ctx := context.Background()
	tgt := pk(0xbb)

	// no open AR channel
	plan, skip, err := testDeps(newFakeQ()).probeGate(ctx, tgt, 10)
	require.NoError(t, err)
	assert.Nil(t, plan)
	assert.Equal(t, "no open AR channel", skip)

	// all channels full
	q := newFakeQ()
	full := tgtChannel()
	full.RemoteBalance = 500
	q.arChannels[tgt] = []db.GuiChannel{full}
	plan, skip, err = testDeps(q).probeGate(ctx, tgt, 10)
	require.NoError(t, err)
	assert.Nil(t, plan)
	assert.Equal(t, "all channels full", skip)

	// buy price from rebalance history: (500 + 100) and (300 + 100) -> 500 ppm
	q = newFakeQ()
	q.arChannels[tgt] = []db.GuiChannel{tgtChannel()}
	q.pays["900"] = []db.RebalPaymentsForChannelRow{{Fee: 50, Value: 100_000, SourceFeeRate: 100}, {Fee: 30, Value: 100_000, SourceFeeRate: 100}}
	plan, _, err = testDeps(q).probeGate(ctx, tgt, 499)
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, 500, plan.buyPpm)
	assert.Equal(t, 800, plan.budgetPpm)
	plan, skip, err = testDeps(q).probeGate(ctx, tgt, 500)
	require.NoError(t, err)
	assert.Nil(t, plan)
	assert.Equal(t, "peer fee 500 ppm not below buy price 500 ppm", skip)

	// no rebalance history: the budget is the buy price
	q = newFakeQ()
	q.arChannels[tgt] = []db.GuiChannel{tgtChannel()}
	plan, _, err = testDeps(q).probeGate(ctx, tgt, 799)
	require.NoError(t, err)
	require.NotNil(t, plan)
	assert.Equal(t, 800, plan.buyPpm)
}

// probeSetup is a target with buy price 500 ppm, budget 800 ppm, one source at
// 50 ppm and a new peer with a route; probing verifies 100k sat at 200 ppm
// route fees (250 ppm incl. the source fee).
func probeSetup(r verifyRouter) (*fakeQ, Deps, *probePlan) {
	q := newFakeQ()
	q.arChannels[pk(0xbb)] = []db.GuiChannel{tgtChannel()}
	q.channels = []db.GuiChannel{srcChannel("100", 50), tgtChannel()}
	q.peers[pk(0xcc)] = "NewPeer"
	q.settings["AR-Time"] = "2"
	d := testDeps(q)
	d.LN = fakeLN{routable: true}
	r.feeMsat = 20_000
	d.Router = r
	return q, d, &probePlan{targets: q.arChannels[pk(0xbb)], buyPpm: 500, budgetPpm: 800}
}

func TestTriggerProbeBuysBelowBuyPrice(t *testing.T) {
	q, d, plan := probeSetup(verifyRouter{payFeeMsat: 21_000})

	got, err := d.triggerProbe(context.Background(), pk(0x03), plan, pk(0xcc), 150, "12345", d.Now())
	require.NoError(t, err)
	assert.Equal(t, 1, got)

	// the purchase is a rebalance record from the verified source to the target
	require.Len(t, q.insertedReb, 1)
	ins := q.insertedReb[0]
	assert.Equal(t, int32(100_000), ins.Value)
	assert.Equal(t, "[100]", ins.OutgoingChanIds)
	assert.Equal(t, pk(0xbb), ins.LastHopPubkey)
	assert.Equal(t, int32(2), ins.Duration)
	assert.Equal(t, int32(rebalInFlight), ins.Status)
	assert.InDelta(t, 45.0, ins.FeeLimit, 1e-9) // (500 - 50) ppm of 100k sat
	require.Len(t, q.updatedReb, 1)
	upd := q.updatedReb[0]
	assert.Equal(t, int32(rebalSucceeded), upd.Status)
	assert.InDelta(t, 21.0, upd.FeesPaid.Float64, 1e-9)
	assert.Equal(t, hex.EncodeToString(realHash), upd.PaymentHash.String)

	require.Len(t, q.insertedLog, 1)
	l := q.insertedLog[0]
	assert.Equal(t, "NewPeer", l.OtherAlias)
	assert.Equal(t, int32(150), l.OtherFeePpm.Int32)
	assert.Equal(t, int32(1), l.SourcesTried)
	assert.Equal(t, int32(1), l.RoutesViaNewPeer)
	assert.Equal(t, int32(0), l.Errors)
	assert.Equal(t, int32(500), l.BuyPpm.Int32)
	assert.Equal(t, int32(250), l.ViaFeePpm.Int32) // 200 route + 50 source
	assert.Equal(t, int64(100_000), l.ViaAmountSat.Int64)
	assert.True(t, l.RebalanceScheduled)
	assert.Equal(t, int32(rebalSucceeded), l.BuyStatus.Int32)
	assert.Contains(t, l.Details, "verified 100000 sat @ 250 ppm")
	assert.Contains(t, l.Details, "bought 100000 sat for 21.000 sat fees")
}

func TestTriggerProbeNoPurchaseAtOrAboveBuyPrice(t *testing.T) {
	q, d, plan := probeSetup(verifyRouter{payFeeMsat: 20_000})
	plan.buyPpm = 250 // route costs exactly the buy price

	_, err := d.triggerProbe(context.Background(), pk(0x03), plan, pk(0xcc), 150, "12345", d.Now())
	require.NoError(t, err)
	assert.Empty(t, q.insertedReb)
	require.Len(t, q.insertedLog, 1)
	assert.False(t, q.insertedLog[0].RebalanceScheduled)
	assert.False(t, q.insertedLog[0].BuyStatus.Valid)
	assert.Contains(t, q.insertedLog[0].Details, "no purchase: 250 ppm not below buy price 250 ppm")
}

func TestTriggerProbeNoPurchaseWhenRebuiltRouteIsDearer(t *testing.T) {
	q, d, plan := probeSetup(verifyRouter{payFeeMsat: 45_000}) // (500 - 50) ppm of 100k sat: not below
	_, err := d.triggerProbe(context.Background(), pk(0x03), plan, pk(0xcc), 150, "12345", d.Now())
	require.NoError(t, err)
	assert.Empty(t, q.insertedReb)
	require.Len(t, q.insertedLog, 1)
	assert.False(t, q.insertedLog[0].RebalanceScheduled)
	assert.Contains(t, q.insertedLog[0].Details, "rebuilt route not below buy price")
}

func TestTriggerProbeFailedPurchase(t *testing.T) {
	q, d, plan := probeSetup(verifyRouter{payFeeMsat: 21_000, payFails: true})
	_, err := d.triggerProbe(context.Background(), pk(0x03), plan, pk(0xcc), 150, "12345", d.Now())
	require.NoError(t, err)
	require.Len(t, q.updatedReb, 1)
	assert.Equal(t, int32(rebalNoRoute), q.updatedReb[0].Status)
	assert.False(t, q.updatedReb[0].FeesPaid.Valid)
	require.Len(t, q.insertedLog, 1)
	assert.True(t, q.insertedLog[0].RebalanceScheduled)
	assert.Equal(t, int32(rebalNoRoute), q.insertedLog[0].BuyStatus.Int32)
	assert.Contains(t, q.insertedLog[0].Details, "purchase of 100000 sat failed with status 4")
}

func TestTriggerProbeLogsNoRoute(t *testing.T) {
	q := newFakeQ()
	q.arChannels[pk(0xbb)] = []db.GuiChannel{tgtChannel()}
	q.channels = []db.GuiChannel{srcChannel("100", 50), tgtChannel()}
	d := testDeps(q)
	plan := &probePlan{targets: q.arChannels[pk(0xbb)], buyPpm: 500, budgetPpm: 800}

	got, err := d.triggerProbe(context.Background(), pk(0x03), plan, pk(0xcc), 150, "12345", d.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, got)
	require.Len(t, q.insertedLog, 1)
	l := q.insertedLog[0]
	assert.Equal(t, int32(0), l.SourcesTried)
	assert.False(t, l.ViaFeePpm.Valid)
	assert.False(t, l.ViaAmountSat.Valid)
	assert.Contains(t, l.Details, "no route to")
}

func TestHandleChannelUpdateWaitsForPeerPolicy(t *testing.T) {
	q := newFakeQ()
	q.arChannels[pk(0xbb)] = []db.GuiChannel{tgtChannel()}
	d := testDeps(q)
	arTargets := map[string]struct{}{pk(0xbb): {}}
	const height = 900_000
	chanID := uint64(height) << 40
	// advertised by the target; the new peer's policy is not in the graph yet
	cu := &lnrpc.ChannelEdgeUpdate{ChanId: chanID, AdvertisingNode: pk(0xbb), ConnectingNode: pk(0xcc),
		RoutingPolicy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 5}}
	d.LN = fakeLN{chanInfo: &lnrpc.ChannelEdge{Node1Pub: pk(0xbb), Node2Pub: pk(0xcc), Node1Policy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 5}}}
	n := 0
	require.NoError(t, d.handleChannelUpdate(context.Background(), cu, arTargets, height+1, map[string]struct{}{}, 300, newProbeClock(), pk(0x03), &n))
	require.Len(t, q.events, 1)
	assert.False(t, q.events[0].ProbeTriggered)
	assert.False(t, q.events[0].FeePpm.Valid) // the target's 5 ppm is not the peer's fee
}

func TestHandleChannelUpdateGatesOnBuyPrice(t *testing.T) {
	q := newFakeQ()
	q.arChannels[pk(0xbb)] = []db.GuiChannel{tgtChannel()} // no history: buy price = budget 800 ppm
	d := testDeps(q)
	arTargets := map[string]struct{}{pk(0xbb): {}}
	const height = 900_000
	clock := newProbeClock()
	state := map[string]struct{}{}
	n := 0

	// the new peer's own update at 800 ppm: not below the buy price
	cu := &lnrpc.ChannelEdgeUpdate{ChanId: uint64(height) << 40, AdvertisingNode: pk(0xcc), ConnectingNode: pk(0xbb),
		RoutingPolicy: &lnrpc.RoutingPolicy{FeeRateMilliMsat: 800}}
	require.NoError(t, d.handleChannelUpdate(context.Background(), cu, arTargets, height+1, state, 300, clock, pk(0x03), &n))
	require.Len(t, q.events, 1)
	assert.False(t, q.events[0].ProbeTriggered)
	assert.Equal(t, int32(800), q.events[0].FeePpm.Int32)
	assert.True(t, clock.get(pk(0xbb)+"|"+pk(0xcc)).IsZero()) // a skipped event does not start the cooldown
}

func (f *fakeQ) NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error) {
	return nil, nil
}
func (f *fakeQ) NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error) {
	return nil, nil
}
