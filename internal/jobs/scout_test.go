package jobs

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
	"github.com/warioishere/lndg-blitz-go/internal/rebalancer"
)

func TestScoutTargets(t *testing.T) {
	edge := func(a, b string) *lnrpc.ChannelEdge { return &lnrpc.ChannelEdge{Node1Pub: a, Node2Pub: b} }
	g := &lnrpc.ChannelGraph{Edges: []*lnrpc.ChannelEdge{
		edge("big", "a"), edge("big", "b"), edge("big", "c"), edge("mid", "a"), edge("mid", "b"),
		edge("self", "big"), edge("peer", "a"), edge("peer", "b"), edge("peer", "c"),
	}}
	got := scoutTargets(g, "self", map[string]bool{"peer": true}, 3)
	assert.Equal(t, []string{"big", "a", "b"}, got, "most channels first, then pubkey; our node and peers left out")
}

type fakeScoutQ struct {
	settings map[string]string
	channels []db.GuiChannel
	paths    []db.ScoutPath
	upserts  []db.UpsertScoutPathParams
	attempts []db.InsertRouteAttemptParams
	median   float64
}

func (f *fakeScoutQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{Key: key, Value: f.settings[key]}, nil
}
func (f *fakeScoutQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeScoutQ) ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeScoutQ) ListScoutPaths(ctx context.Context, since pgtype.Timestamptz) ([]db.ScoutPath, error) {
	return f.paths, nil
}
func (f *fakeScoutQ) UpsertScoutPath(ctx context.Context, arg db.UpsertScoutPathParams) error {
	f.upserts = append(f.upserts, arg)
	return nil
}
func (f *fakeScoutQ) DeleteOldScoutPaths(ctx context.Context, since pgtype.Timestamptz) error {
	return nil
}
func (f *fakeScoutQ) MedianRebalanceValue(ctx context.Context, since pgtype.Timestamptz) (float64, error) {
	return f.median, nil
}
func (f *fakeScoutQ) InsertRouteAttempt(ctx context.Context, arg db.InsertRouteAttemptParams) error {
	f.attempts = append(f.attempts, arg)
	return nil
}
func (f *fakeScoutQ) ListSentRouteAttempts(ctx context.Context, since pgtype.Timestamptz) ([]db.ListSentRouteAttemptsRow, error) {
	return nil, nil
}

// scoutLN routes to the target over 0xbb first, over 0xdd once a pair is left
// out, over 0xee once two are.
type scoutLN struct {
	reqs []*lnrpc.QueryRoutesRequest
}

func (c *scoutLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{IdentityPubkey: hopPubkey(0x03)}, nil
}
func (c *scoutLN) DescribeGraph(ctx context.Context, in *lnrpc.ChannelGraphRequest, opts ...grpc.CallOption) (*lnrpc.ChannelGraph, error) {
	return &lnrpc.ChannelGraph{}, nil
}
func (c *scoutLN) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	return &lnrpc.ListChannelsResponse{}, nil
}
func (c *scoutLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	c.reqs = append(c.reqs, in)
	mid := []string{hopPubkey(0xbb), hopPubkey(0xdd), hopPubkey(0xee)}[min(len(in.IgnoredPairs), 2)]
	return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{
		{PubKey: hopPubkey(0xaa), ChanId: 111}, {PubKey: mid}, {PubKey: in.PubKey},
	}}}}, nil
}

// scoutRouter: 0xbb cannot forward to the target, every other path reaches it.
type scoutRouter struct{ sent int }

func (r *scoutRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	return nil, nil
}
func (r *scoutRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	r.sent++
	hops := in.Route.GetHops()
	if hops[1].GetPubKey() == hopPubkey(0xbb) {
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15, FailureSourceIndex: 2}}, nil
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: uint32(len(hops))}}, nil
}

func scoutChannel() db.GuiChannel {
	return db.GuiChannel{ChanID: "111", RemotePubkey: hopPubkey(0xaa), LocalBalance: 1_000_000, LocalChanReserve: 10_000}
}

func newScoutPass(q *fakeScoutQ, ln *scoutLN, r *scoutRouter, s scoutSettings) *scoutPass {
	return &scoutPass{q: q, ln: ln, router: r, s: s, stats: rebalancer.NewRouteStats(), htlcs: rebalancer.NewHtlcCounter(),
		sleep: func(time.Duration) {}, now: func() time.Time { return time.Unix(1_700_000_000, 0) }, amount: 80_000}
}

func TestScoutTargetPaths(t *testing.T) {
	q := &fakeScoutQ{}
	ln := &scoutLN{}
	r := &scoutRouter{}
	p := newScoutPass(q, ln, r, scoutSettings{perHour: 600, tries: 6, paths: 2, maxFeeRate: 500})
	target := hopPubkey(0xcc)
	require.True(t, p.scoutTarget(context.Background(), target, nil, []db.GuiChannel{scoutChannel()}))

	require.Len(t, ln.reqs, 3)
	assert.Equal(t, int64(80_000), ln.reqs[0].Amt)
	assert.Equal(t, int64(40), ln.reqs[0].FeeLimit.GetFixed(), "500 ppm of 80k")
	assert.Equal(t, []uint64{111}, ln.reqs[0].OutgoingChanIds)
	// after the failure at 0xbb: bb -> target left out
	require.Len(t, ln.reqs[1].IgnoredPairs, 1)
	assert.Equal(t, hopPubkey(0xbb), hex.EncodeToString(ln.reqs[1].IgnoredPairs[0].From))
	assert.Equal(t, target, hex.EncodeToString(ln.reqs[1].IgnoredPairs[0].To))
	// after reaching the target over 0xdd: the middle pair aa -> dd left out too
	require.Len(t, ln.reqs[2].IgnoredPairs, 2)
	assert.Equal(t, hopPubkey(0xdd), hex.EncodeToString(ln.reqs[2].IgnoredPairs[1].To))

	require.Len(t, q.upserts, 2, "the paths over 0xdd and 0xee")
	assert.Equal(t, target, q.upserts[0].TargetPubkey)
	assert.Equal(t, "111", q.upserts[0].OutgoingChanID)
	assert.NotEmpty(t, q.upserts[0].RouteHex)
	require.Len(t, q.attempts, 3)
	assert.Equal(t, "scout", q.attempts[0].List)
	assert.Equal(t, int32(15), q.attempts[0].FailureCode.Int32)
	assert.Equal(t, int32(1), q.attempts[1].FailureCode.Int32)
	assert.Equal(t, 1, p.done)
	assert.Equal(t, 2, p.verified)
}

func TestScoutTargetDoneAndNoChannel(t *testing.T) {
	q := &fakeScoutQ{}
	ln := &scoutLN{}
	p := newScoutPass(q, ln, &scoutRouter{}, scoutSettings{perHour: 600, tries: 6, paths: 1, maxFeeRate: 500})
	path := hopPubkey(0xaa) + "-" + hopPubkey(0xcc)
	for i := 0; i < 5; i++ {
		p.stats.Observe(path, "success", -1, -1, p.now())
	}
	// a known path with a good chance: done without probing
	require.True(t, p.scoutTarget(context.Background(), hopPubkey(0xcc), []string{path}, []db.GuiChannel{scoutChannel()}))
	assert.Empty(t, ln.reqs)
	assert.Equal(t, 1, p.done)

	// no channel holds the amount: the pass ends
	poor := scoutChannel()
	poor.LocalBalance = 50_000
	assert.False(t, p.scoutTarget(context.Background(), hopPubkey(0xdd), nil, []db.GuiChannel{poor}))
	assert.Empty(t, ln.reqs)
}

func TestScoutRunDisabled(t *testing.T) {
	q := &fakeScoutQ{settings: map[string]string{}}
	ln := &scoutLN{}
	require.NoError(t, scoutRun(context.Background(), q, ln, &scoutRouter{}, func(time.Duration) {}, time.Now))
	assert.Empty(t, ln.reqs)
}

// A failure the target itself returns (here invalid_onion_payload) means the probe reached it.
type payloadRouter struct{ scoutRouter }

func (r *payloadRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED,
		Failure: &lnrpc.Failure{Code: lnrpc.Failure_INVALID_ONION_PAYLOAD, FailureSourceIndex: uint32(len(in.Route.GetHops()))}}, nil
}

func TestScoutTargetReachedOnAnyTargetFailure(t *testing.T) {
	q := &fakeScoutQ{}
	ln := &scoutLN{}
	p := newScoutPass(q, ln, &scoutRouter{}, scoutSettings{perHour: 600, tries: 1, paths: 1, maxFeeRate: 500})
	p.router = &payloadRouter{}
	require.True(t, p.scoutTarget(context.Background(), hopPubkey(0xcc), nil, []db.GuiChannel{scoutChannel()}))
	require.Len(t, q.upserts, 1)
	assert.Equal(t, 1, p.verified)
	require.Len(t, q.attempts, 1)
	assert.Equal(t, int32(24), q.attempts[0].FailureCode.Int32)
}
