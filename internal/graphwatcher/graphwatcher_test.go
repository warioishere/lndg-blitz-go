package graphwatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// --- fakeQ: in-memory implementation of graphwatcher.Querier ---------------------------

type fakeQ struct {
	settings    map[string]string
	peers       map[string]string
	arPubkeys   []string
	arChannels  map[string][]db.GuiChannel
	hasActive   map[string]bool
	insertedReb []db.InsertRebalancerRecordParams
	insertedLog []db.InsertGraphProbeLogParams
}

func newFakeQ() *fakeQ {
	return &fakeQ{settings: map[string]string{}, peers: map[string]string{}, arChannels: map[string][]db.GuiChannel{}, hasActive: map[string]bool{}}
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
func (f *fakeQ) ListOpenAutoRebalanceChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return nil, nil
}
func (f *fakeQ) ListOutboundCandidates(ctx context.Context) ([]db.ListOutboundCandidatesRow, error) {
	return nil, nil
}
func (f *fakeQ) ListChanIDsByPubkey(ctx context.Context, remotePubkey string) ([]string, error) {
	return nil, nil
}
func (f *fakeQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	return nil
}
func (f *fakeQ) UpdateRebalanceRouteHex(ctx context.Context, arg db.UpdateRebalanceRouteHexParams) error {
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
func (f *fakeQ) ListGraphOutboundCans(ctx context.Context) ([]string, error) { return nil, nil }
func (f *fakeQ) ListChannelFeeAndDiffByIDs(ctx context.Context, dollar_1 []string) ([]db.ListChannelFeeAndDiffByIDsRow, error) {
	return nil, nil
}
func (f *fakeQ) ListRecentRoutesForTarget(ctx context.Context, targetPubkey string) ([]db.ListRecentRoutesForTargetRow, error) {
	return nil, nil
}
func (f *fakeQ) HasActiveRebalanceForPubkey(ctx context.Context, lastHopPubkey string) (bool, error) {
	return f.hasActive[lastHopPubkey], nil
}
func (f *fakeQ) InsertGraphProbeLog(ctx context.Context, arg db.InsertGraphProbeLogParams) error {
	f.insertedLog = append(f.insertedLog, arg)
	return nil
}
func (f *fakeQ) InsertGraphEvent(ctx context.Context, arg db.InsertGraphEventParams) error {
	return nil
}
func (f *fakeQ) UpdateGraphEventRoutesFound(ctx context.Context, arg db.UpdateGraphEventRoutesFoundParams) error {
	return nil
}
func (f *fakeQ) DeleteOldGraphEvents(ctx context.Context) error { return nil }
func (f *fakeQ) InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error) {
	f.insertedReb = append(f.insertedReb, arg)
	return 1, nil
}

// --- fakeLN: in-memory implementation of graphwatcher.Client -----------------------------------------

type fakeLN struct{}

func (fakeLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (fakeLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	return &lnrpc.QueryRoutesResponse{}, nil
}
func (fakeLN) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return nil, errors.New("no node info")
}
func (fakeLN) GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error) {
	return nil, errors.New("no chan info")
}
func (fakeLN) SubscribeChannelGraph(ctx context.Context, in *lnrpc.GraphTopologySubscription, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.GraphTopologyUpdate], error) {
	return nil, errors.New("not used")
}

func testDeps(q *fakeQ) Deps {
	return Deps{Q: q, LN: fakeLN{}, Now: time.Unix(1_700_000_000, 0).UTC, Sleep: func(time.Duration) {}}
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

func TestScheduleRebalanceHappy(t *testing.T) {
	q := newFakeQ()
	q.settings["AR-Time"] = "7"
	targets := []db.GuiChannel{{
		Alias: "Tgt", RemoteBalance: 1_000_000, LocalChanReserve: 1000,
		LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 500_000,
	}}
	d := testDeps(q)
	err := d.scheduleRebalance(context.Background(), "tgtpk", targets, []string{"777", "888"}, map[string]int{"777": 100, "888": 200}, 500, d.Now())
	require.NoError(t, err)
	require.Len(t, q.insertedReb, 1)
	r := q.insertedReb[0]
	// fee_rate = min(500, int(1000*0.8)-100=700) = 500; fee_limit=round(500*500000*1e-6,3)=250
	assert.Equal(t, int32(500_000), r.Value)
	assert.InDelta(t, 250.0, r.FeeLimit, 1e-9)
	assert.Equal(t, "[777, 888]", r.OutgoingChanIds)
	assert.Equal(t, "tgtpk", r.LastHopPubkey)
	assert.Equal(t, int32(7), r.Duration)
	assert.Equal(t, int32(0), r.Status)
}

func TestScheduleRebalanceSkips(t *testing.T) {
	d := testDeps(newFakeQ())
	base := []db.GuiChannel{{RemoteBalance: 1_000_000, LocalChanReserve: 1000, LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 500_000}}

	// active rebalance already running -> skip
	q1 := newFakeQ()
	q1.hasActive["tgtpk"] = true
	d1 := testDeps(q1)
	require.NoError(t, d1.scheduleRebalance(context.Background(), "tgtpk", base, []string{"1"}, map[string]int{"1": 0}, 500, d.Now()))
	assert.Empty(t, q1.insertedReb)

	// channel full (remote <= reserve) -> skip
	q2 := newFakeQ()
	full := []db.GuiChannel{{RemoteBalance: 500, LocalChanReserve: 1000, LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 500_000}}
	d2 := testDeps(q2)
	require.NoError(t, d2.scheduleRebalance(context.Background(), "tgtpk", full, []string{"1"}, map[string]int{"1": 0}, 500, d.Now()))
	assert.Empty(t, q2.insertedReb)

	// fee_rate <= 0 (source fee consumes entire budget) -> skip
	q3 := newFakeQ()
	d3 := testDeps(q3)
	require.NoError(t, d3.scheduleRebalance(context.Background(), "tgtpk", base, []string{"1"}, map[string]int{"1": 5000}, 500, d.Now()))
	assert.Empty(t, q3.insertedReb)
}

func TestTriggerProbeNoTargets(t *testing.T) {
	q := newFakeQ() // no arChannels
	d := testDeps(q)
	got, err := d.triggerProbe(context.Background(), "self", "tgtpk", "otherpk", pgtype.Int4{}, "12345", d.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, got)
	assert.Empty(t, q.insertedLog)
}

func TestTriggerProbeAllFull(t *testing.T) {
	q := newFakeQ()
	q.arChannels["tgtpk"] = []db.GuiChannel{{Alias: "Tgt", RemoteBalance: 500, LocalChanReserve: 1000}}
	d := testDeps(q)
	got, err := d.triggerProbe(context.Background(), "self", "tgtpk", "otherpk", pgtype.Int4{}, "12345", d.Now())
	require.NoError(t, err)
	assert.Equal(t, 0, got)        // all full -> skip before ProbeTargets
	assert.Empty(t, q.insertedLog) // no probe log written
}
