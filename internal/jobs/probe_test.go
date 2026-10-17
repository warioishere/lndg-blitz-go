package jobs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

// ---- Fake probe querier ----

type fakeProbeQ struct {
	settings    map[string]string
	channels    []db.GuiChannel // ListActiveOpenPublicChannels
	routes      map[string]db.GuiRebalanceroute
	peerAliases map[string]pgtype.Text

	upserts       []db.UpsertLocalSettingParams
	insertedRR    []db.InsertRebalanceRouteParams
	updatedRR     []db.UpdateRebalanceRouteProbeParams
	probeLogs     []db.InsertProbeLogParams
	attempts      []db.InsertRouteAttemptParams
	refills       []db.UpdatePeerRefillPriceParams
	prunedOldLogs int
}

func newFakeProbeQ() *fakeProbeQ {
	return &fakeProbeQ{
		settings:    map[string]string{},
		routes:      map[string]db.GuiRebalanceroute{},
		peerAliases: map[string]pgtype.Text{},
	}
}

func rrKey(p db.GetRebalanceRouteParams) string {
	return p.TargetPubkey + "|" + p.OutgoingChanID + "|" + p.Route
}

func (f *fakeProbeQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeProbeQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeProbeQ) UpsertLocalSetting(ctx context.Context, arg db.UpsertLocalSettingParams) error {
	f.settings[arg.Key] = arg.Value
	f.upserts = append(f.upserts, arg)
	return nil
}
func (f *fakeProbeQ) ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeProbeQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	if r, ok := f.routes[rrKey(arg)]; ok {
		return r, nil
	}
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeProbeQ) InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error {
	f.attempts = append(f.attempts, arg)
	return nil
}
func (f *fakeProbeQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	f.insertedRR = append(f.insertedRR, arg)
	return nil
}
func (f *fakeProbeQ) UpdateRebalanceRouteProbe(ctx context.Context, arg db.UpdateRebalanceRouteProbeParams) error {
	f.updatedRR = append(f.updatedRR, arg)
	return nil
}
func (f *fakeProbeQ) InsertProbeLog(ctx context.Context, arg db.InsertProbeLogParams) error {
	f.probeLogs = append(f.probeLogs, arg)
	return nil
}
func (f *fakeProbeQ) UpdatePeerRefillPrice(ctx context.Context, arg db.UpdatePeerRefillPriceParams) error {
	f.refills = append(f.refills, arg)
	return nil
}
func (f *fakeProbeQ) DeleteOldProbeLogs(ctx context.Context) error {
	f.prunedOldLogs++
	return nil
}
func (f *fakeProbeQ) GetPeerAlias(ctx context.Context, pubkey string) (pgtype.Text, error) {
	if a, ok := f.peerAliases[pubkey]; ok {
		return a, nil
	}
	return pgtype.Text{}, pgx.ErrNoRows
}

// ---- Fake router client ----

type fakeProbeRouter struct {
	liquiditySat int64  // amounts (sat) <= this -> verified; else liquidity
	failHop      uint32 // failure source index of a liquidity failure; 0: the target peer
	feeMsat      int64  // total_fees_msat of built routes
	feeFailures  int    // the next N sends fail with FEE_INSUFFICIENT (code 12)
	buildCalls   int
	sendCalls    int
}

func (r *fakeProbeRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	r.buildCalls++
	hops := make([]*lnrpc.Hop, 0, len(in.HopPubkeys))
	for _, pk := range in.HopPubkeys {
		hops = append(hops, &lnrpc.Hop{PubKey: hex.EncodeToString(pk)})
	}
	return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{
		TotalAmtMsat:  in.AmtMsat,
		TotalFeesMsat: r.feeMsat,
		Hops:          hops,
	}}, nil
}
func (r *fakeProbeRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	r.sendCalls++
	if r.feeFailures > 0 {
		r.feeFailures--
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 12}}, nil
	}
	amtSat := in.Route.GetTotalAmtMsat() / 1000
	nHops := uint32(len(in.Route.GetHops()))
	if amtSat <= r.liquiditySat {
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: nHops}}, nil
	}
	hop := r.failHop
	if hop == 0 {
		hop = nHops - 1
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15, FailureSourceIndex: hop}}, nil
}

// ---- Fake lightning client ----

type fakeProbeLN struct {
	selfPubkey   string
	routes       []*lnrpc.Route // returned by QueryRoutes
	qrCalls      int
	lastFeeLimit int64
}

func (c *fakeProbeLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{IdentityPubkey: c.selfPubkey}, nil
}
func (c *fakeProbeLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	c.qrCalls++
	c.lastFeeLimit = in.GetFeeLimit().GetFixed()
	return &lnrpc.QueryRoutesResponse{Routes: c.routes}, nil
}

func hopPubkey(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 33) } // 33-byte hex

// peerPub is a valid 33-byte hex pubkey (remote_pubkey is hex-decoded for QueryRoutes).
var peerPub = hopPubkey(0xcc)

// refillTarget is an AR channel to pubkey the rebalancer would refill now:
// remote 60 % of capacity, ar_in_target 50 -> inbound_can 1.
func refillTarget(chanID, pubkey string) db.GuiChannel {
	return db.GuiChannel{ChanID: chanID, RemotePubkey: pubkey, AutoRebalance: true, Capacity: 1_000_000,
		LocalBalance: 400_000, RemoteBalance: 600_000, ArInTarget: 50, ArOutTarget: 75,
		LocalFeeRate: 800, ArMaxCost: 65, ArAmtTarget: 100_000}
}

// sourceChan is a non-AR channel with fee feeRate and 80 % outbound: a source
// for a 100k rebalance at ar_out_target 50 (80 % - 1 % reserve - 10 % value = 69 %).
func sourceChan(chanID string, feeRate int32) db.GuiChannel {
	return db.GuiChannel{ChanID: chanID, RemotePubkey: hopPubkey(0x10 + chanID[0]), Capacity: 1_000_000,
		LocalBalance: 800_000, RemoteBalance: 200_000, LocalChanReserve: 10_000, ArOutTarget: 50,
		ArInTarget: 50, LocalFeeRate: feeRate}
}

// ---- probeRouteSync classification ----

type fixedSendRouter struct {
	attempt *lnrpc.HTLCAttempt
	err     error
}

func (r *fixedSendRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	return nil, nil
}
func (r *fixedSendRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	return r.attempt, r.err
}

func TestProbeRouteSync_Classification(t *testing.T) {
	route := &lnrpc.Route{Hops: []*lnrpc.Hop{{}, {}}} // 2 hops
	cases := []struct {
		name    string
		attempt *lnrpc.HTLCAttempt
		err     error
		want    string
	}{
		{"verified", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: 2}}, nil, "verified"},
		{"verified-wrong-fsi", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: 1}}, nil, "invalid"},
		{"liquidity", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15}}, nil, "liquidity"},
		{"fee", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 12}}, nil, "fee"},
		{"invalid", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 99}}, nil, "invalid"},
		{"succeeded->error", &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_SUCCEEDED}, nil, "error"},
		{"grpc-error", nil, fmt.Errorf("rpc down"), "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &fixedSendRouter{attempt: tc.attempt, err: tc.err}
			got := probeRouteSync(context.Background(), r, route, 1)
			assert.Equal(t, tc.want, got.status)
		})
	}
}

// ---- binary search ----

func TestProbeWithBinarySearch_Converges(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 60000, feeMsat: 1000}
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	got := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000, 2500, nil)
	require.NotNil(t, got.routeHex)
	assert.Equal(t, int64(59375), got.amountSat)
	require.NotNil(t, got.feePpm)
	assert.NotEmpty(t, *got.routeHex)
	assert.Empty(t, got.failFrom)
}

// A liquidity failure before the target peer halves over the same path; when
// nothing verifies down to the minimum, the failing node pair comes back.
func TestProbeWithBinarySearch_FailureBeforeTargetReturnsPair(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 0, failHop: 1, feeMsat: 1000} // the source peer cannot forward to 0x02
	got := probeWithBinarySearch(context.Background(), r, "12345", [][]byte{{0x01}, {0x02}, {0x03}}, 40, 100000, 1000, 2500, nil)
	assert.Nil(t, got.routeHex)
	assert.Equal(t, 6, r.sendCalls, "100k down to 3125; 1562 is below the minimum")
	assert.Equal(t, "01", got.failFrom)
	assert.Equal(t, "02", got.failTo)
}

// A smaller amount gets past a hop before the target peer that lacks liquidity.
func TestProbeWithBinarySearch_HalvesBeforeTarget(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 30000, failHop: 1, feeMsat: 1000}
	got := probeWithBinarySearch(context.Background(), r, "12345", [][]byte{{0x01}, {0x02}, {0x03}}, 40, 100000, 1000, 2500, nil)
	require.NotNil(t, got.routeHex)
	assert.Greater(t, got.amountSat, int64(25000))
	assert.LessOrEqual(t, got.amountSat, int64(30000))
	assert.Empty(t, got.failFrom)
}

// Halving at the target peer stops below the minimum amount.
func TestProbeWithBinarySearch_MinAmount(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 0, feeMsat: 1000}
	got := probeWithBinarySearch(context.Background(), r, "12345", [][]byte{{0x01}, {0x02}}, 40, 100000, 1000, 30000, nil)
	assert.Nil(t, got.routeHex)
	assert.Equal(t, 2, r.sendCalls, "100k and 50k; 25k is below the minimum")
	assert.Empty(t, got.failFrom, "the target peer is never left out")
}

// Every probe HTLC goes to the recorder with its result.
func TestProbeWithBinarySearch_Records(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 60000, feeMsat: 1000}
	var got []probeOutcome
	var amounts []int64
	rec := func(route *lnrpc.Route, amountSat int64, o probeOutcome) {
		got = append(got, o)
		amounts = append(amounts, amountSat)
	}
	probeWithBinarySearch(context.Background(), r, "12345", [][]byte{{0x01}, {0x02}}, 40, 100000, 1000, 2500, rec)
	require.Equal(t, r.sendCalls, len(got))
	assert.Equal(t, int64(100000), amounts[0])
	assert.Equal(t, "liquidity", got[0].status)
	assert.Equal(t, 15, got[0].code)
}

func TestProbeWithBinarySearch_NoLiquidity(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 0, feeMsat: 1000} // nothing verifies
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	got := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000, 2500, nil)
	assert.Nil(t, got.routeHex)
	assert.Equal(t, int64(0), got.amountSat)
}

func TestProbeWithBinarySearch_FeeBudgetAbort(t *testing.T) {
	// total_fees 100000 msat = 100 sat; fee_limit 1 sat -> amount_fee_limit 1 -> abort immediately.
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 100000}
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	got := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1, 2500, nil)
	assert.Nil(t, got.routeHex)
	assert.Equal(t, int64(0), got.amountSat)
	assert.Equal(t, 1, r.buildCalls) // built once, aborted on fee check
	assert.Equal(t, 0, r.sendCalls)
}

func TestProbeWithBinarySearch_FeeFailureRetriedOnce(t *testing.T) {
	hopPubkeys := [][]byte{{0x01}, {0x02}}

	// one fee failure, then the same amount verifies
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000, feeFailures: 1}
	got := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000, 2500, nil)
	require.NotNil(t, got.routeHex)
	assert.Equal(t, int64(100000), got.amountSat)

	// a persisting fee failure aborts after one retry
	r = &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000, feeFailures: 100}
	got = probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000, 2500, nil)
	assert.Nil(t, got.routeHex)
	assert.Equal(t, 2, r.sendCalls)
}

// ---- probe_routes_job gating ----

func TestProbeRoutesJob_DisabledCreatesSetting(t *testing.T) {
	q := newFakeProbeQ()
	ln := &fakeProbeLN{}
	r := &fakeProbeRouter{}
	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))
	assert.Equal(t, "0", q.settings["QR-Enabled"])
	assert.Empty(t, q.probeLogs)
}

func TestProbeRoutesJob_TimeGateNotElapsed(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	q.settings["QR-UpdateHours"] = "6"
	q.settings["QR-LastProbe"] = nowISOForTest() // just now -> within 6h
	ln := &fakeProbeLN{}
	r := &fakeProbeRouter{}
	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))
	assert.Empty(t, q.probeLogs)
	assert.Empty(t, q.upserts)
}

func TestProbeRoutesJob_FullVerifiedNewRoute(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	// one target to peer P (its own channel 555 is no source) and one source 111.
	q.channels = []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	q.peerAliases[peerPub] = pgtype.Text{String: "peerAlias", Valid: true}
	ln := &fakeProbeLN{
		selfPubkey: "03self",
		routes: []*lnrpc.Route{{
			Hops: []*lnrpc.Hop{
				{PubKey: hopPubkey(0xaa), ChanId: 111, Expiry: 800100},
				{PubKey: hopPubkey(0xbb), ChanId: 222, Expiry: 800000},
			},
		}},
	}
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000} // everything verifies

	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))

	// QueryRoutes called once (only out_chan 111; 555 excluded as peer's own).
	assert.Equal(t, 1, ln.qrCalls)
	require.Len(t, q.insertedRR, 1)
	rr := q.insertedRR[0]
	assert.Equal(t, peerPub, rr.TargetPubkey)
	assert.Equal(t, "111", rr.OutgoingChanID) // str(route.hops[0].chan_id)
	assert.Equal(t, hopPubkey(0xaa)+"-"+hopPubkey(0xbb), rr.Route)
	assert.Equal(t, int32(rebalancer.FinalCltvDelta), rr.FinalCltvDelta)
	assert.True(t, rr.RouteHex.Valid)
	assert.True(t, rr.LastProbed.Valid)
	assert.Equal(t, int64(100000), rr.ProbedAmountSat.Int64)

	// ProbeLog + prune + QR-LastProbe upsert.
	require.Len(t, q.probeLogs, 1)
	pl := q.probeLogs[0]
	assert.Equal(t, int32(1), pl.TargetsScanned)
	assert.Equal(t, int32(1), pl.RoutesFound)
	assert.Equal(t, int32(0), pl.RoutesExisting)
	assert.Equal(t, 1, q.prunedOldLogs)
	assert.Equal(t, "QR-LastProbe", q.upserts[0].Key)

	// details JSON shape.
	var details []map[string]any
	require.NoError(t, json.Unmarshal(pl.Details, &details))
	require.Len(t, details, 1)
	assert.Equal(t, peerPub, details[0]["pubkey"])
	assert.Equal(t, "peerAlias", details[0]["alias"])
	assert.EqualValues(t, 1, details[0]["new"])
	assert.EqualValues(t, 1, details[0]["verified"])
	assert.EqualValues(t, 1, details[0]["out_chans_tried"])
}

func TestProbeTargetsZeroFeeRouteStoresZero(t *testing.T) {
	q := newFakeProbeQ()
	channels := []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	ln := &fakeProbeLN{selfPubkey: "03self", routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{
		{PubKey: hopPubkey(0xaa), ChanId: 111}, {PubKey: hopPubkey(0xbb), ChanId: 222},
	}}}}
	r := &fakeProbeRouter{liquiditySat: 100000} // no fees
	_, _, _, _, err := probeTargets(context.Background(), ln, r, q, hopPubkey(0x03), channels, 7000, 10, 3, 2500)
	require.NoError(t, err)
	require.Len(t, q.insertedRR, 1)
	assert.True(t, q.insertedRR[0].LastFeePpm.Valid) // 0, not unknown
	assert.Equal(t, 0.0, q.insertedRR[0].LastFeePpm.Float64)
}

func TestProbeRoutesJob_ExistingRouteUpdated(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	q.channels = []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	path := hopPubkey(0xaa) + "-" + hopPubkey(0xbb)
	q.routes[rrKey(db.GetRebalanceRouteParams{TargetPubkey: peerPub, OutgoingChanID: "111", Route: path})] =
		db.GuiRebalanceroute{ID: 7, TargetPubkey: peerPub, OutgoingChanID: "111", Route: path, LastFeePpm: pgtype.Float8{Float64: 42, Valid: true}}
	ln := &fakeProbeLN{
		selfPubkey: "03self",
		routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{
			{PubKey: hopPubkey(0xaa), ChanId: 111, Expiry: 800100},
			{PubKey: hopPubkey(0xbb), ChanId: 222, Expiry: 800000},
		}}},
	}
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000}

	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))
	assert.Empty(t, q.insertedRR)
	require.Len(t, q.updatedRR, 1)
	assert.Equal(t, int64(7), q.updatedRR[0].ID)
	assert.True(t, q.updatedRR[0].RouteHex.Valid)
	assert.True(t, q.updatedRR[0].LastProbed.Valid)
	assert.True(t, q.updatedRR[0].ProbedAmountSat.Valid)
	require.Len(t, q.probeLogs, 1)
	assert.Equal(t, int32(1), q.probeLogs[0].RoutesExisting)
}

func TestProbeRoutesJob_QueryRoutesError(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	q.channels = []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	ln := &fakeProbeLNErr{selfPubkey: "03self"}
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000}
	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))
	require.Len(t, q.probeLogs, 1)
	assert.Equal(t, int32(1), q.probeLogs[0].Errors) // QueryRoutes failed -> rpc_errors
	assert.Equal(t, int32(0), q.probeLogs[0].RoutesFound)
	assert.Empty(t, q.insertedRR)
	var details []map[string]any
	require.NoError(t, json.Unmarshal(q.probeLogs[0].Details, &details))
	require.Len(t, details, 1)
	assert.EqualValues(t, 1, details[0]["rpc_errors"])
	assert.EqualValues(t, 0, details[0]["probe_failed"])
	assert.EqualValues(t, 1, details[0]["out_chans_tried"])
	assert.Empty(t, q.refills, "an RPC error leaves the refill price as it is")
}

func TestProbeSources(t *testing.T) {
	target := refillTarget("555", peerPub) // budget 800 * 65 % = 520 ppm
	arNoSource := sourceChan("222", 10)
	arNoSource.AutoRebalance = true // AR channel without ar_source
	lowOut := sourceChan("333", 10)
	lowOut.LocalBalance = 500_000 // 50 % - 1 % - 10 % = 39 % < 50 %
	disabled := sourceChan("444", 10)
	disabled.LocalDisabled = true
	ownToTarget := sourceChan("666", 10)
	ownToTarget.RemotePubkey = peerPub
	channels := []db.GuiChannel{target, sourceChan("888", 100), sourceChan("777", 100), sourceChan("111", 50),
		sourceChan("999", 600), arNoSource, lowOut, disabled, ownToTarget}

	got := probeSources(channels, target, budgetOf(target), 7000)
	var ids []string
	for _, s := range got {
		ids = append(ids, s.chanID)
	}
	assert.Equal(t, []string{"111", "777", "888"}, ids) // cheapest first, then chan_id; 999: 600 ppm eats the 520 ppm budget
	assert.Equal(t, int64(47), got[0].feeLimitSat)      // (520 - 50) ppm of 100k

	ids = nil
	for _, s := range sourceCandidates(channels, target) {
		ids = append(ids, s.chanID)
	}
	assert.Equal(t, []string{"111", "777", "888", "999"}, ids, "without a budget")
}

// Every auto-rebalance channel the peer has not disabled gets a refill price,
// also one that needs no refill now; a peer without a route gets a NULL price.
func TestProbeTargetsAllAutoRebalanceChannels(t *testing.T) {
	full := refillTarget("556", hopPubkey(0xdd))
	full.RemoteBalance, full.LocalBalance = 300_000, 700_000 // inbound_can 30/50 = 0
	remoteDisabled := refillTarget("557", hopPubkey(0xee))
	remoteDisabled.RemoteDisabled = true
	channels := []db.GuiChannel{refillTarget("555", peerPub), full, remoteDisabled, sourceChan("111", 50)}
	ln := &fakeProbeLN{selfPubkey: "03self"}
	q := newFakeProbeQ()
	_, _, _, details, err := probeTargets(context.Background(), ln, &fakeProbeRouter{}, q, hopPubkey(0x03), channels, 7000, 10, 3, 2500)
	require.NoError(t, err)
	require.Len(t, details, 2)
	assert.Equal(t, peerPub, details[0].Pubkey)
	assert.Equal(t, hopPubkey(0xdd), details[1].Pubkey)
	assert.Equal(t, 1, details[0].Sources)
	assert.Equal(t, 1, details[0].NoRoute) // fakeProbeLN without routes
	assert.Equal(t, 0, details[0].OutChansTried)
	assert.Nil(t, details[0].RefillPpm)
	require.Len(t, q.refills, 2)
	assert.Equal(t, peerPub, q.refills[0].RemotePubkey)
	assert.False(t, q.refills[0].RefillPpm.Valid, "no route reached us")
	assert.True(t, q.refills[0].RefillChecked.Valid)
}

// A verified route above the target's budget is the refill price but no stored route.
func TestProbeTargetsRefillPriceOverBudget(t *testing.T) {
	channels := []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)} // budget 520 - 50 = 470 ppm
	ln := &fakeProbeLN{selfPubkey: "03self", routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{
		{PubKey: hopPubkey(0xaa), ChanId: 111}, {PubKey: hopPubkey(0xbb), ChanId: 222},
	}}}}
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 60_000} // 60 sat on 100k: ~600 ppm
	q := newFakeProbeQ()
	_, _, _, details, err := probeTargets(context.Background(), ln, r, q, hopPubkey(0x03), channels, 7000, 10, 3, 2500)
	require.NoError(t, err)
	assert.Empty(t, q.insertedRR)
	d := details[0]
	assert.Equal(t, 1, d.Verified)
	assert.Equal(t, 1, d.OverBudget)
	require.NotNil(t, d.RefillPpm)
	assert.Equal(t, 600, *d.RefillPpm)
	assert.Equal(t, int64(100000), d.RefillAmount)
	require.Len(t, q.refills, 1)
	assert.Equal(t, int32(600), q.refills[0].RefillPpm.Int32)
	assert.Equal(t, int64(100000), q.refills[0].RefillAmountSat.Int64)
	assert.Equal(t, int64(700), ln.lastFeeLimit, "AR-MaxFeeRate 7000 ppm of 100k, not the budget")
}

func TestProbeTargetsNoRouteDoesNotUseSlot(t *testing.T) {
	channels := []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 10), sourceChan("222", 20), sourceChan("333", 30)}
	ln := &viaLN{routable: map[uint64]bool{222: true, 333: true}}
	r := &viaRouter{liquid: map[uint64]bool{333: true}}
	_, _, _, details, err := probeTargets(context.Background(), ln, r, newFakeProbeQ(), hopPubkey(0x03), channels, 7000, 2, 3, 2500)
	require.NoError(t, err)
	require.Len(t, details, 1)
	d := details[0]
	assert.Equal(t, 3, d.Sources)
	assert.Equal(t, 1, d.NoRoute)       // 111
	assert.Equal(t, 2, d.OutChansTried) // 222 and 333 use the two slots
	assert.Equal(t, 1, d.ProbeFailed)   // 222
	assert.Equal(t, 1, d.Verified)      // 333
	assert.Equal(t, 1, d.Errors)
	assert.Len(t, ln.reqs, 3)
}

type fakeProbeLNErr struct {
	selfPubkey string
	err        error
}

func (c *fakeProbeLNErr) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{IdentityPubkey: c.selfPubkey}, nil
}
func (c *fakeProbeLNErr) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	return nil, fmt.Errorf("connection reset")
}

// LND reports a missing route as an error: it counts as no route, not as an error.
func TestProbeTargetsNoPathIsNoRoute(t *testing.T) {
	ln := &fakeProbeLNErr{selfPubkey: "03self", err: fmt.Errorf("rpc error: code = Unknown desc = unable to find a path to destination")}
	channels := []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	_, _, errs, details, err := probeTargets(context.Background(), ln, &fakeProbeRouter{}, newFakeProbeQ(), hopPubkey(0x03), channels, 7000, 10, 3, 2500)
	require.NoError(t, err)
	assert.Zero(t, errs)
	require.Len(t, details, 1)
	assert.Equal(t, 1, details[0].NoRoute)
	assert.Zero(t, details[0].RPCErrors)
	assert.Zero(t, details[0].OutChansTried)
}

// pathLN offers a path over 0xbb until that pair is left out, then one over 0xdd.
type pathLN struct{ reqs []*lnrpc.QueryRoutesRequest }

func (c *pathLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (c *pathLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	c.reqs = append(c.reqs, in)
	mid := hopPubkey(0xbb)
	if len(in.IgnoredPairs) > 0 {
		mid = hopPubkey(0xdd)
	}
	return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{
		{PubKey: hopPubkey(0xaa), ChanId: 111}, {PubKey: mid}, {PubKey: peerPub}, {PubKey: hopPubkey(0x03)},
	}}}}, nil
}

// pathRouter fails every probe over 0xbb at the source peer (0xaa -> 0xbb) and verifies the others.
type pathRouter struct{ fakeProbeRouter }

func (r *pathRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	r.sendCalls++
	hops := in.Route.GetHops()
	if hops[1].GetPubKey() == hopPubkey(0xbb) {
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15, FailureSourceIndex: 1}}, nil
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: uint32(len(hops))}}, nil
}

// A probe that fails before the target peer is followed by another path without the failing pair.
func TestProbeTargetsTriesAnotherPath(t *testing.T) {
	ln := &pathLN{}
	r := &pathRouter{}
	q := newFakeProbeQ()
	channels := []db.GuiChannel{refillTarget("555", peerPub), sourceChan("111", 50)}
	_, _, _, details, err := probeTargets(context.Background(), ln, r, q, hopPubkey(0x03), channels, 7000, 10, 3, 2500)
	require.NoError(t, err)
	require.Len(t, ln.reqs, 2)
	require.Len(t, ln.reqs[1].IgnoredPairs, 1)
	assert.Equal(t, hopPubkey(0xaa), hex.EncodeToString(ln.reqs[1].IgnoredPairs[0].From))
	assert.Equal(t, hopPubkey(0xbb), hex.EncodeToString(ln.reqs[1].IgnoredPairs[0].To))
	require.Len(t, q.insertedRR, 1)
	assert.Contains(t, q.insertedRR[0].Route, hopPubkey(0xdd))
	d := details[0]
	assert.Equal(t, 1, d.ProbeFailed)
	assert.Equal(t, 1, d.Verified)
	assert.Equal(t, 1, d.OutChansTried, "one source, two paths")
	assert.Len(t, q.attempts, 7, "six halvings over 0xbb (100k down to 3125) and the verified probe over 0xdd are recorded")

	// one path per source: no second QueryRoutes
	ln = &pathLN{}
	_, _, _, _, err = probeTargets(context.Background(), ln, &pathRouter{}, newFakeProbeQ(), hopPubkey(0x03), channels, 7000, 10, 1, 2500)
	require.NoError(t, err)
	assert.Len(t, ln.reqs, 1)
}

func nowISOForTest() string {
	// A value parseISO accepts and that is "now-ish"; use a far-future date so the
	// 6h gate always treats it as recent (now - future < 6h is true since negative).
	return "2099-01-01T00:00:00.000000"
}

func (f *fakeProbeQ) ListNkPeerChannels(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error) {
	return nil, nil
}
func (f *fakeProbeQ) NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error) {
	return nil, nil
}
func (f *fakeProbeQ) NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error) {
	return nil, nil
}
