package rebalancer

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// routeHexOf marshals a Route to the hex string update_route/update_node_reputations parse.
func routeHexOf(r *lnrpc.Route) string {
	b, err := proto.Marshal(r)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

type fakeRouteQ struct {
	settings map[string]string
	routes   map[string]db.GuiRebalanceroute // by target|chan|path

	inserted     []db.InsertRebalanceRouteParams
	updated      []db.UpdateRebalanceRouteParams
	feeUpdates   []db.UpdateRebalanceRouteFeeParams
	markFails    []db.MarkRebalanceRouteFailureParams
	repSuccess   []string
	repFailure   []string
	openIDs      []string
	deletedNotIn [][]string
	delTested    int
	delReps      int
}

func newFakeRouteQ() *fakeRouteQ {
	return &fakeRouteQ{settings: map[string]string{}, routes: map[string]db.GuiRebalanceroute{}}
}

func (f *fakeRouteQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeRouteQ) GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error) {
	if r, ok := f.routes[arg.TargetPubkey+"|"+arg.OutgoingChanID+"|"+arg.Route]; ok {
		return r, nil
	}
	return db.GuiRebalanceroute{}, pgx.ErrNoRows
}
func (f *fakeRouteQ) InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error {
	f.inserted = append(f.inserted, arg)
	return nil
}
func (f *fakeRouteQ) UpdateRebalanceRoute(ctx context.Context, arg db.UpdateRebalanceRouteParams) error {
	f.updated = append(f.updated, arg)
	return nil
}
func (f *fakeRouteQ) UpdateRebalanceRouteFee(ctx context.Context, arg db.UpdateRebalanceRouteFeeParams) error {
	f.feeUpdates = append(f.feeUpdates, arg)
	return nil
}
func (f *fakeRouteQ) MarkRebalanceRouteFailure(ctx context.Context, arg db.MarkRebalanceRouteFailureParams) error {
	f.markFails = append(f.markFails, arg)
	return nil
}
func (f *fakeRouteQ) IncrNodeReputationSuccess(ctx context.Context, arg db.IncrNodeReputationSuccessParams) error {
	f.repSuccess = append(f.repSuccess, arg.Pubkey)
	return nil
}
func (f *fakeRouteQ) IncrNodeReputationFailure(ctx context.Context, arg db.IncrNodeReputationFailureParams) error {
	f.repFailure = append(f.repFailure, arg.Pubkey)
	return nil
}
func (f *fakeRouteQ) ListOpenChannelIDs(ctx context.Context) ([]string, error) { return f.openIDs, nil }
func (f *fakeRouteQ) DeleteRoutesNotOpenOutgoing(ctx context.Context, dollar_1 []string) (int64, error) {
	f.deletedNotIn = append(f.deletedNotIn, dollar_1)
	return 2, nil
}
func (f *fakeRouteQ) DeleteStaleTestedRoutes(ctx context.Context, ts pgtype.Timestamptz) error {
	f.delTested++
	return nil
}
func (f *fakeRouteQ) DeleteStaleNodeReputations(ctx context.Context, ts pgtype.Timestamptz) error {
	f.delReps++
	return nil
}

func twoHopRoute() *lnrpc.Route {
	return &lnrpc.Route{
		TotalAmtMsat:  1_010_000,
		TotalFeesMsat: 10_000,
		Hops: []*lnrpc.Hop{
			{PubKey: "aa", Expiry: 800100},
			{PubKey: "bb", Expiry: 800000},
		},
	}
}

func TestUpdateRoute_NewSuccess(t *testing.T) {
	q := newFakeRouteQ()
	now := time.Unix(1_700_000_000, 0)
	updateRoute(context.Background(), q, "target", "555", routeHexOf(twoHopRoute()), true, false, now)
	require.Len(t, q.inserted, 1)
	assert.Equal(t, "aa-bb", q.inserted[0].Route)
	assert.Equal(t, int32(-100), q.inserted[0].FinalCltvDelta) // 800000-800100
	require.Len(t, q.updated, 1)
	u := q.updated[0]
	assert.Equal(t, int32(1), u.SuccessCount)
	assert.True(t, u.LastSuccess.Valid)
	assert.False(t, u.LastFailure.Valid)
	assert.True(t, u.LastFeePpm.Valid)
	assert.InDelta(t, 10000, u.LastFeePpm.Float64, 1e-6) // 10000/(1010000-10000)*1e6
}

func TestUpdateRoute_ExistingSuccessWithin5MinNoIncrement(t *testing.T) {
	q := newFakeRouteQ()
	now := time.Unix(1_700_000_000, 0)
	q.routes["target|555|aa-bb"] = db.GuiRebalanceroute{
		TargetPubkey: "target", OutgoingChanID: "555", Route: "aa-bb",
		SuccessCount: 5, LastSuccess: pgtype.Timestamptz{Time: now.Add(-2 * time.Minute), Valid: true},
	}
	updateRoute(context.Background(), q, "target", "555", routeHexOf(twoHopRoute()), true, false, now)
	require.Len(t, q.updated, 1)
	assert.Equal(t, int32(5), q.updated[0].SuccessCount) // within 5 min -> not incremented
}

func TestUpdateRoute_ExistingSuccessAfter5MinIncrements(t *testing.T) {
	q := newFakeRouteQ()
	now := time.Unix(1_700_000_000, 0)
	q.routes["target|555|aa-bb"] = db.GuiRebalanceroute{
		TargetPubkey: "target", OutgoingChanID: "555", Route: "aa-bb",
		SuccessCount: 5, LastSuccess: pgtype.Timestamptz{Time: now.Add(-6 * time.Minute), Valid: true},
	}
	updateRoute(context.Background(), q, "target", "555", routeHexOf(twoHopRoute()), true, false, now)
	assert.Equal(t, int32(6), q.updated[0].SuccessCount)
}

func TestUpdateRoute_FailureAndForgive(t *testing.T) {
	q := newFakeRouteQ()
	now := time.Unix(1_700_000_000, 0)
	updateRoute(context.Background(), q, "t", "1", routeHexOf(twoHopRoute()), false, false, now)
	require.Len(t, q.updated, 1)
	assert.Equal(t, int32(1), q.updated[0].FailureCount)
	assert.True(t, q.updated[0].LastFailure.Valid)

	q2 := newFakeRouteQ()
	q2.routes["t|1|aa-bb"] = db.GuiRebalanceroute{TargetPubkey: "t", OutgoingChanID: "1", Route: "aa-bb", FailureCount: 3}
	updateRoute(context.Background(), q2, "t", "1", routeHexOf(twoHopRoute()), true, true, now)
	assert.Equal(t, int32(2), q2.updated[0].FailureCount) // forgive: 3 -> 2
}

func TestUpdateRoute_CollectDisabled(t *testing.T) {
	q := newFakeRouteQ()
	q.settings["RR-CollectRoutes"] = "0"
	updateRoute(context.Background(), q, "t", "1", routeHexOf(twoHopRoute()), true, false, time.Now())
	assert.Empty(t, q.inserted)
	assert.Empty(t, q.updated)
}

func TestUpdateNodeReputations_Success(t *testing.T) {
	q := newFakeRouteQ()
	updateNodeReputations(context.Background(), q, routeHexOf(twoHopRoute()), true, 0, false, time.Now())
	assert.Equal(t, []string{"aa", "bb"}, q.repSuccess)
	assert.Empty(t, q.repFailure)
}

func TestUpdateNodeReputations_FailureAtIndex(t *testing.T) {
	q := newFakeRouteQ()
	r := &lnrpc.Route{Hops: []*lnrpc.Hop{{PubKey: "aa"}, {PubKey: "bb"}, {PubKey: "cc"}}}
	updateNodeReputations(context.Background(), q, routeHexOf(r), false, 1, true, time.Now())
	assert.Equal(t, []string{"aa"}, q.repSuccess) // i<1 success
	assert.Equal(t, []string{"bb"}, q.repFailure) // i==1 failure, then break (cc untouched)
}

func TestMarkRouteFailure(t *testing.T) {
	q := newFakeRouteQ()
	markRouteFailure(context.Background(), q, 42, time.Unix(1_700_000_000, 0))
	require.Len(t, q.markFails, 1)
	assert.Equal(t, int64(42), q.markFails[0].ID)
}

func TestPurgeStaleRoutes(t *testing.T) {
	q := newFakeRouteQ()
	q.openIDs = []string{"111", "222"}
	purgeStaleRoutes(context.Background(), q, time.Unix(1_700_000_000, 0))
	require.Len(t, q.deletedNotIn, 1)
	assert.Equal(t, []string{"111", "222"}, q.deletedNotIn[0])
	assert.Equal(t, 1, q.delTested)
	assert.Equal(t, 1, q.delReps)
}

func TestUpdateRouteFee(t *testing.T) {
	q := newFakeRouteQ()
	updateRouteFee(context.Background(), q, 9, 123.5)
	require.Len(t, q.feeUpdates, 1)
	assert.Equal(t, int64(9), q.feeUpdates[0].ID)
	assert.InDelta(t, 123.5, q.feeUpdates[0].LastFeePpm.Float64, 1e-9)
}
