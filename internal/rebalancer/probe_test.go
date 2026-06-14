package rebalancer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// --- Fakes ---------------------------------------------------------------

type fakeLN struct {
	info      *lnrpc.GetInfoResponse
	infoErr   error
	infoCalls int
	qr        *lnrpc.QueryRoutesResponse
	qrErr     error
	invoice   *lnrpc.AddInvoiceResponse
	chanInfo  map[uint64]*lnrpc.ChannelEdge
	listChans *lnrpc.ListChannelsResponse
}

func (f *fakeLN) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	if f.listChans != nil {
		return f.listChans, nil
	}
	return &lnrpc.ListChannelsResponse{}, nil
}

func (f *fakeLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	f.infoCalls++
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return f.info, nil
}
func (f *fakeLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	return f.qr, f.qrErr
}
func (f *fakeLN) AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
	return f.invoice, nil
}
func (f *fakeLN) GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error) {
	if e, ok := f.chanInfo[in.GetChanId()]; ok {
		return e, nil
	}
	return nil, errors.New("not found")
}

type fakeRouter struct {
	buildFn func(*routerrpc.BuildRouteRequest) (*routerrpc.BuildRouteResponse, error)
	sendFn  func(*routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error)
}

func (f *fakeRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	return f.buildFn(in)
}
func (f *fakeRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	return f.sendFn(in)
}

func failAttempt(code int) *lnrpc.HTLCAttempt {
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: lnrpc.Failure_FailureCode(code)}}
}

// --- probeRouteAmount ----------------------------------------------------

// echoBuild returns a route whose TotalAmtMsat matches the request,
// so the sendFn can identify the probed amount.
func echoBuild(req *routerrpc.BuildRouteRequest) (*routerrpc.BuildRouteResponse, error) {
	return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{
		TotalAmtMsat: req.AmtMsat,
		Hops:         []*lnrpc.Hop{{ChanId: 1}, {ChanId: 2}},
	}}, nil
}

func TestProbeRouteAmountConverges(t *testing.T) {
	// Threshold 300000: probe<=300000 -> code 1 (ok), otherwise code 15 (too much).
	r := &fakeRouter{
		buildFn: echoBuild,
		sendFn: func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) {
			amtSat := req.Route.GetTotalAmtMsat() / 1000
			if amtSat <= 300000 {
				return failAttempt(1), nil
			}
			return failAttempt(15), nil
		},
	}
	got := probeRouteAmount(context.Background(), r, [][]byte{{0xaa}}, "100", 144, 1_000_000)
	assert.Equal(t, int64(281250), got)
}

func TestProbeRouteAmountAllTooMuch(t *testing.T) {
	r := &fakeRouter{
		buildFn: echoBuild,
		sendFn:  func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) { return failAttempt(15), nil },
	}
	got := probeRouteAmount(context.Background(), r, [][]byte{{0xaa}}, "100", 144, 1_000_000)
	assert.Equal(t, int64(0), got)
}

func TestProbeRouteAmountBuildErrorBreaks(t *testing.T) {
	r := &fakeRouter{
		buildFn: func(*routerrpc.BuildRouteRequest) (*routerrpc.BuildRouteResponse, error) {
			return nil, errors.New("boom")
		},
		sendFn: func(*routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) { return failAttempt(1), nil },
	}
	got := probeRouteAmount(context.Background(), r, [][]byte{{0xaa}}, "100", 144, 1_000_000)
	assert.Equal(t, int64(0), got)
}

// --- trySingleSource -----------------------------------------------------

func sampleRebalance() db.GuiRebalancer {
	return db.GuiRebalancer{Value: 100000, LastHopPubkey: "aabb", FeeLimit: 1000}
}

func sampleQR() *lnrpc.QueryRoutesResponse {
	return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{
		Hops: []*lnrpc.Hop{
			{PubKey: "aa", Expiry: 800100, AmtToForwardMsat: 100_000_000},
			{PubKey: "bb", Expiry: 800000, AmtToForwardMsat: 100_000_000},
		},
	}}}
}

func TestTrySingleSourceSuccess(t *testing.T) {
	ln := &fakeLN{
		info: &lnrpc.GetInfoResponse{IdentityPubkey: "self"},
		qr:   sampleQR(),
	}
	r := &fakeRouter{
		buildFn: func(req *routerrpc.BuildRouteRequest) (*routerrpc.BuildRouteResponse, error) {
			return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{
				TotalAmtMsat: 100_050_000, TotalFeesMsat: 50_000,
				Hops: []*lnrpc.Hop{{PubKey: "aa", ChanId: 111}, {PubKey: "bb", ChanId: 222}},
			}}, nil
		},
		sendFn: func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) {
			return &lnrpc.HTLCAttempt{
				Status: lnrpc.HTLCAttempt_SUCCEEDED,
				Route:  &lnrpc.Route{TotalFeesMsat: 50_000, Hops: []*lnrpc.Hop{{ChanId: 111}, {ChanId: 222}}},
			}, nil
		},
	}
	q := newFakeRouteQ()
	e := newEngine()
	inv := &lnrpc.AddInvoiceResponse{RHash: []byte{0x01, 0x02}, PaymentAddr: []byte{0x09}}
	now := time.Unix(1_700_000_000, 0)

	res, err := e.trySingleSource(context.Background(), ln, r, q, sampleRebalance(), "555",
		map[string]int{}, 1000, 100, 2000, inv, 1_000_000_000, 30*time.Second, now)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int32(2), res.status)
	assert.InDelta(t, 50.0, res.feesPaid, 1e-9)
	assert.Equal(t, uint64(111), res.successfulOut)
	assert.Equal(t, uint64(222), res.successfulIn)
	assert.Equal(t, "0102", res.paymentHash)
	assert.Equal(t, int64(100000), res.value)
	assert.Nil(t, res.scaledFeeLimit)
	// update_route(success) + update_node_reputations(success) ran.
	require.Len(t, q.updated, 1)
	assert.Equal(t, int32(1), q.updated[0].SuccessCount)
	assert.Equal(t, []string{"aa", "bb"}, q.repSuccess)
}

func TestTrySingleSourceOppCostRejected(t *testing.T) {
	ln := &fakeLN{info: &lnrpc.GetInfoResponse{IdentityPubkey: "self"}, qr: sampleQR()}
	r := &fakeRouter{
		buildFn: func(req *routerrpc.BuildRouteRequest) (*routerrpc.BuildRouteResponse, error) {
			// 500_000 msat fee on 100000 sat = 5000 ppm > max 1000 ppm -> rejected.
			return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{
				TotalAmtMsat: 100_500_000, TotalFeesMsat: 500_000,
				Hops: []*lnrpc.Hop{{PubKey: "aa", ChanId: 111}, {PubKey: "bb", ChanId: 222}},
			}}, nil
		},
		sendFn: func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) {
			t.Fatal("SendToRouteV2 should not be called when opp-cost rejects")
			return nil, nil
		},
	}
	q := newFakeRouteQ()
	e := newEngine()
	inv := &lnrpc.AddInvoiceResponse{RHash: []byte{0x01}}
	res, err := e.trySingleSource(context.Background(), ln, r, q, sampleRebalance(), "555",
		map[string]int{}, 1000, 100, 2000, inv, 1_000_000_000, 30*time.Second, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Nil(t, res)
}

func TestTrySingleSourceSelfPubkeyErrorPropagates(t *testing.T) {
	ln := &fakeLN{infoErr: errors.New("node down")}
	r := &fakeRouter{}
	e := newEngine()
	res, err := e.trySingleSource(context.Background(), ln, r, newFakeRouteQ(), sampleRebalance(), "555",
		map[string]int{}, 1000, 100, 2000, &lnrpc.AddInvoiceResponse{}, 1_000_000_000, 30*time.Second, time.Unix(1, 0))
	require.Error(t, err)
	assert.Nil(t, res)
}

func TestTrySingleSourceBudgetTooLow(t *testing.T) {
	// targetFeeRate*arMaxCost% - srcFee <= 0 -> return nil immediately.
	ln := &fakeLN{info: &lnrpc.GetInfoResponse{IdentityPubkey: "self"}}
	e := newEngine()
	res, err := e.trySingleSource(context.Background(), ln, &fakeRouter{}, newFakeRouteQ(), sampleRebalance(),
		"555", map[string]int{"555": 2000}, 1000, 100, 2000, &lnrpc.AddInvoiceResponse{}, 1_000_000_000, 30*time.Second, time.Unix(1, 0))
	require.NoError(t, err)
	assert.Nil(t, res)
}

// --- chanInfoCache / maxAmountOnRouteMsat --------------------------------

func TestMaxAmountOnRouteMsat(t *testing.T) {
	route := &lnrpc.Route{Hops: []*lnrpc.Hop{
		{PubKey: "aa", ChanId: 1},
		{PubKey: "bb", ChanId: 2},
	}}
	ln := &fakeLN{chanInfo: map[uint64]*lnrpc.ChannelEdge{
		// Hop pub_key "aa" == node2 -> node1_policy applies (max 5_000_000).
		1: {Node2Pub: "aa", Node1Policy: &lnrpc.RoutingPolicy{MaxHtlcMsat: 5_000_000}, Node2Policy: &lnrpc.RoutingPolicy{MaxHtlcMsat: 9_000_000}},
		// Hop pub_key "bb" != node2 -> node2_policy applies (max 3_000_000, smallest).
		2: {Node2Pub: "zz", Node1Policy: &lnrpc.RoutingPolicy{MaxHtlcMsat: 8_000_000}, Node2Policy: &lnrpc.RoutingPolicy{MaxHtlcMsat: 3_000_000}},
	}}
	c := newChanInfoCache()
	got := c.maxAmountOnRouteMsat(context.Background(), ln, route)
	assert.Equal(t, int64(3_000_000), got)
}

func TestMaxAmountOnRouteMsatErrorSkipsHop(t *testing.T) {
	route := &lnrpc.Route{Hops: []*lnrpc.Hop{{PubKey: "aa", ChanId: 1}, {PubKey: "bb", ChanId: 99}}}
	ln := &fakeLN{chanInfo: map[uint64]*lnrpc.ChannelEdge{
		1: {Node2Pub: "aa", Node1Policy: &lnrpc.RoutingPolicy{MaxHtlcMsat: 5_000_000}},
	}}
	c := newChanInfoCache()
	got := c.maxAmountOnRouteMsat(context.Background(), ln, route) // chan 99 -> error -> skip
	assert.Equal(t, int64(5_000_000), got)
}

// --- selfPubkeyCache -----------------------------------------------------

func TestSelfPubkeyCacheCachesOnce(t *testing.T) {
	ln := &fakeLN{info: &lnrpc.GetInfoResponse{IdentityPubkey: "mypub"}}
	c := newSelfPubkeyCache()
	pk1, err := c.get(context.Background(), ln)
	require.NoError(t, err)
	pk2, err := c.get(context.Background(), ln)
	require.NoError(t, err)
	assert.Equal(t, "mypub", pk1)
	assert.Equal(t, "mypub", pk2)
	assert.Equal(t, 1, ln.infoCalls) // fetched only once
}

// --- aliasCache ----------------------------------------------------------

type fakeAliasQ struct {
	rows  []db.ListChannelAliasesRow
	calls int
}

func (f *fakeAliasQ) ListChannelAliases(ctx context.Context) ([]db.ListChannelAliasesRow, error) {
	f.calls++
	return f.rows, nil
}

func TestAliasCacheEnsureAndLabel(t *testing.T) {
	q := &fakeAliasQ{rows: []db.ListChannelAliasesRow{{ChanID: "555", Alias: "ACINQ"}}}
	a := newAliasCache()
	require.NoError(t, a.ensure(context.Background(), q))
	assert.Equal(t, "555 (ACINQ)", a.label("555"))
	assert.Equal(t, "999 (?)", a.label("999"))
	// Within TTL: no second refresh.
	require.NoError(t, a.ensure(context.Background(), q))
	assert.Equal(t, 1, q.calls)
}

func TestFailureLogParts(t *testing.T) {
	reason, fsi := failureLogParts(true, 15, 1)
	assert.Equal(t, "TEMPORARY_CHANNEL_FAILURE", reason)
	assert.Equal(t, "1", fsi)
	reason, fsi = failureLogParts(false, 0, 0)
	assert.Equal(t, "None", reason)
	assert.Equal(t, "None", fsi)
}
