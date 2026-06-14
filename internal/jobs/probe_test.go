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
)

// ---- Fake probe querier ----

type fakeProbeQ struct {
	settings    map[string]string
	autoRebal   []db.GuiChannel
	outbound    []db.ListOutboundCandidatesRow
	chanByPub   map[string][]string // remote_pubkey -> chan_ids (own channels)
	routes      map[string]db.GuiRebalanceroute
	peerAliases map[string]pgtype.Text

	upserts       []db.UpsertLocalSettingParams
	insertedRR    []db.InsertRebalanceRouteParams
	updatedRR     []db.UpdateRebalanceRouteHexParams
	probeLogs     []db.InsertProbeLogParams
	prunedOldLogs int
}

func newFakeProbeQ() *fakeProbeQ {
	return &fakeProbeQ{
		settings:    map[string]string{},
		chanByPub:   map[string][]string{},
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
func (f *fakeProbeQ) ListOpenAutoRebalanceChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.autoRebal, nil
}
func (f *fakeProbeQ) ListOutboundCandidates(ctx context.Context) ([]db.ListOutboundCandidatesRow, error) {
	return f.outbound, nil
}
func (f *fakeProbeQ) ListChanIDsByPubkey(ctx context.Context, remotePubkey string) ([]string, error) {
	return f.chanByPub[remotePubkey], nil
}
func (f *fakeProbeQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	if r, ok := f.routes[rrKey(arg)]; ok {
		return r, nil
	}
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeProbeQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	f.insertedRR = append(f.insertedRR, arg)
	return nil
}
func (f *fakeProbeQ) UpdateRebalanceRouteHex(ctx context.Context, arg db.UpdateRebalanceRouteHexParams) error {
	f.updatedRR = append(f.updatedRR, arg)
	return nil
}
func (f *fakeProbeQ) InsertProbeLog(ctx context.Context, arg db.InsertProbeLogParams) error {
	f.probeLogs = append(f.probeLogs, arg)
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
	liquiditySat int64 // amounts (sat) <= this -> verified; else liquidity
	feeMsat      int64 // total_fees_msat of built routes
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
	amtSat := in.Route.GetTotalAmtMsat() / 1000
	nHops := uint32(len(in.Route.GetHops()))
	if amtSat <= r.liquiditySat {
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: nHops}}, nil
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15}}, nil
}

// ---- Fake lightning client ----

type fakeProbeLN struct {
	selfPubkey string
	routes     []*lnrpc.Route // returned by QueryRoutes
	qrCalls    int
}

func (c *fakeProbeLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{IdentityPubkey: c.selfPubkey}, nil
}
func (c *fakeProbeLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	c.qrCalls++
	return &lnrpc.QueryRoutesResponse{Routes: c.routes}, nil
}

func hopPubkey(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 33) } // 33-byte hex

// peerPub is a valid 33-byte hex pubkey (remote_pubkey is hex-decoded for QueryRoutes).
var peerPub = hopPubkey(0xcc)

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
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---- binary search ----

func TestProbeWithBinarySearch_Converges(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 60000, feeMsat: 1000}
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	bestHex, good, feePpm := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000)
	require.NotNil(t, bestHex)
	assert.Equal(t, int64(59375), good)
	require.NotNil(t, feePpm)
	assert.NotEmpty(t, *bestHex)
}

func TestProbeWithBinarySearch_NoLiquidity(t *testing.T) {
	r := &fakeProbeRouter{liquiditySat: 0, feeMsat: 1000} // nothing verifies
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	bestHex, good, _ := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1000)
	assert.Nil(t, bestHex)
	assert.Equal(t, int64(0), good)
}

func TestProbeWithBinarySearch_FeeBudgetAbort(t *testing.T) {
	// total_fees 100000 msat = 100 sat; fee_limit 1 sat -> amount_fee_limit 1 -> abort immediately.
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 100000}
	hopPubkeys := [][]byte{{0x01}, {0x02}}
	bestHex, good, _ := probeWithBinarySearch(context.Background(), r, "12345", hopPubkeys, 40, 100000, 1)
	assert.Nil(t, bestHex)
	assert.Equal(t, int64(0), good)
	assert.Equal(t, 1, r.buildCalls) // built once, aborted on fee check
	assert.Equal(t, 0, r.sendCalls)
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
	// targets: one auto-rebalance channel to peer P.
	q.autoRebal = []db.GuiChannel{{
		ChanID: "555", RemotePubkey: peerPub, LocalFeeRate: 800, ArMaxCost: 65, ArAmtTarget: 100000,
	}}
	// outbound candidates: c1 (source) + c2 (own channel to peer, must be excluded).
	q.outbound = []db.ListOutboundCandidatesRow{{ChanID: "111", LocalFeeRate: 50}, {ChanID: "555", LocalFeeRate: 800}}
	q.chanByPub[peerPub] = []string{"555"} // peer's own channel -> excluded from out_chans
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
	assert.Equal(t, int32(800000-800100), rr.FinalCltvDelta) // hops[-1].expiry - hops[-2].expiry
	assert.True(t, rr.RouteHex.Valid)

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

func TestProbeRoutesJob_ExistingRouteUpdated(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	q.autoRebal = []db.GuiChannel{{
		ChanID: "555", RemotePubkey: peerPub, LocalFeeRate: 800, ArMaxCost: 65, ArAmtTarget: 100000,
	}}
	q.outbound = []db.ListOutboundCandidatesRow{{ChanID: "111", LocalFeeRate: 50}}
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
	require.Len(t, q.probeLogs, 1)
	assert.Equal(t, int32(1), q.probeLogs[0].RoutesExisting)
}

func TestProbeRoutesJob_QueryRoutesError(t *testing.T) {
	q := newFakeProbeQ()
	q.settings["QR-Enabled"] = "1"
	q.autoRebal = []db.GuiChannel{{
		ChanID: "555", RemotePubkey: peerPub, LocalFeeRate: 800, ArMaxCost: 65, ArAmtTarget: 100000,
	}}
	q.outbound = []db.ListOutboundCandidatesRow{{ChanID: "111", LocalFeeRate: 50}}
	ln := &fakeProbeLNErr{selfPubkey: "03self"}
	r := &fakeProbeRouter{liquiditySat: 100000, feeMsat: 1000}
	require.NoError(t, ProbeRoutesJob(context.Background(), q, ln, r))
	require.Len(t, q.probeLogs, 1)
	assert.Equal(t, int32(1), q.probeLogs[0].Errors) // QueryRoutes failed -> target_errors
	assert.Equal(t, int32(0), q.probeLogs[0].RoutesFound)
	assert.Empty(t, q.insertedRR)
}

type fakeProbeLNErr struct{ selfPubkey string }

func (c *fakeProbeLNErr) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{IdentityPubkey: c.selfPubkey}, nil
}
func (c *fakeProbeLNErr) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	return nil, fmt.Errorf("no route")
}

func nowISOForTest() string {
	// A value parseISO accepts and that is "now-ish"; use a far-future date so the
	// 6h gate always treats it as recent (now - future < 6h is true since negative).
	return "2099-01-01T00:00:00.000000"
}
