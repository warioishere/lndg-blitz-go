package htlcstream

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// --- Fakes ---------------------------------------------------------------

type fakeQ struct {
	channels map[string]db.GuiChannel
	inserted []db.InsertFailedHtlcParams
}

func (f *fakeQ) GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error) {
	if c, ok := f.channels[chanID]; ok {
		return c, nil
	}
	return db.GuiChannel{}, pgx.ErrNoRows
}
func (f *fakeQ) InsertFailedHtlc(ctx context.Context, arg db.InsertFailedHtlcParams) error {
	f.inserted = append(f.inserted, arg)
	return nil
}

// jobs.EmergencyForwardQuerier stub — EP-enabled is never set, so the check is a no-op.
func (f *fakeQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeQ) ListEpEnabledChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error) {
	return nil, nil
}
func (f *fakeQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error { return nil }
func (f *fakeQ) UpdateChannelEmergencyFee(ctx context.Context, arg db.UpdateChannelEmergencyFeeParams) error {
	return nil
}

type fakeLN struct {
	listCalls int
}

func (f *fakeLN) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	f.listCalls++
	return &lnrpc.ListChannelsResponse{}, nil
}
func (f *fakeLN) UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
	return &lnrpc.PolicyUpdateResponse{}, nil
}

func deps(q *fakeQ, ln *fakeLN) Deps {
	return Deps{Q: q, LN: ln, Now: time.Unix(1_700_000_000, 0).UTC, Sleep: func(time.Duration) {}}
}

// --- Tests ---------------------------------------------------------------

func linkFailEvent(in, out uint64) *routerrpc.HtlcEvent {
	return &routerrpc.HtlcEvent{
		EventType:         routerrpc.HtlcEvent_FORWARD,
		IncomingChannelId: in, OutgoingChannelId: out,
		Event: &routerrpc.HtlcEvent_LinkFailEvent{LinkFailEvent: &routerrpc.LinkFailEvent{
			Info:          &routerrpc.HtlcInfo{IncomingAmtMsat: 2500, OutgoingAmtMsat: 2000},
			WireFailure:   lnrpc.Failure_TEMPORARY_CHANNEL_FAILURE, // 15
			FailureDetail: routerrpc.FailureDetail_INSUFFICIENT_BALANCE,
		}},
	}
}

func TestLinkFailRecordsHTLC(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{
		"100": {Alias: "InPeer"},
		"200": {Alias: "OutPeer", LocalBalance: 1000, LocalChanReserve: 100, PendingOutbound: 50},
	}}
	d := deps(q, &fakeLN{})
	err := d.handleEvent(context.Background(), linkFailEvent(100, 200), map[string]*routerrpc.ForwardEvent{})
	require.NoError(t, err)
	require.Len(t, q.inserted, 1)
	got := q.inserted[0]
	assert.Equal(t, int32(2), got.Amount) // 2000/1000
	assert.Equal(t, "100", got.ChanIDIn)
	assert.Equal(t, "200", got.ChanIDOut)
	assert.Equal(t, "InPeer", got.ChanInAlias.String)
	assert.Equal(t, "OutPeer", got.ChanOutAlias.String)
	assert.Equal(t, int64(900), got.ChanOutLiq.Int64) // 1000-100
	assert.Equal(t, int64(50), got.ChanOutPending.Int64)
	assert.Equal(t, int32(15), got.WireFailure)
	assert.InDelta(t, 0.5, got.MissedFee, 1e-9) // (2500-2000)/1000
}

func TestLinkFailUnknownChannelsNullFields(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{}} // no channels registered
	d := deps(q, &fakeLN{})
	err := d.handleEvent(context.Background(), linkFailEvent(100, 200), map[string]*routerrpc.ForwardEvent{})
	require.NoError(t, err)
	require.Len(t, q.inserted, 1)
	got := q.inserted[0]
	assert.False(t, got.ChanInAlias.Valid)
	assert.False(t, got.ChanOutAlias.Valid)
	assert.False(t, got.ChanOutLiq.Valid)
	assert.False(t, got.ChanOutPending.Valid)
}

func TestForwardThenSettleRunsEmergency(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{}}
	ln := &fakeLN{}
	d := deps(q, ln)
	forwards := map[string]*routerrpc.ForwardEvent{}

	fwd := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_FORWARD, IncomingChannelId: 100, OutgoingChannelId: 200,
		IncomingHtlcId: 1, OutgoingHtlcId: 2,
		Event: &routerrpc.HtlcEvent_ForwardEvent{ForwardEvent: &routerrpc.ForwardEvent{Info: &routerrpc.HtlcInfo{IncomingAmtMsat: 3000, OutgoingAmtMsat: 2900}}},
	}
	require.NoError(t, d.handleEvent(context.Background(), fwd, forwards))
	assert.Len(t, forwards, 1) // tracked

	settle := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_FORWARD, IncomingChannelId: 100, OutgoingChannelId: 200,
		IncomingHtlcId: 1, OutgoingHtlcId: 2,
		Event: &routerrpc.HtlcEvent_SettleEvent{SettleEvent: &routerrpc.SettleEvent{}},
	}
	require.NoError(t, d.handleEvent(context.Background(), settle, forwards))
	assert.Empty(t, forwards)        // removed after settle
	assert.Empty(t, q.inserted)      // settle does not write a FailedHTLC record
	assert.Equal(t, 0, ln.listCalls) // EP not enabled; emergency check exits before ListChannels
}

func TestForwardThenFailRecordsCode99(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{}}
	d := deps(q, &fakeLN{})
	forwards := map[string]*routerrpc.ForwardEvent{}

	fwd := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_FORWARD, IncomingChannelId: 100, OutgoingChannelId: 200,
		IncomingHtlcId: 1, OutgoingHtlcId: 2,
		Event: &routerrpc.HtlcEvent_ForwardEvent{ForwardEvent: &routerrpc.ForwardEvent{Info: &routerrpc.HtlcInfo{IncomingAmtMsat: 3000, OutgoingAmtMsat: 2900}}},
	}
	require.NoError(t, d.handleEvent(context.Background(), fwd, forwards))

	// forward_fail hits the catch-all default branch.
	ff := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_FORWARD, IncomingChannelId: 100, OutgoingChannelId: 200,
		IncomingHtlcId: 1, OutgoingHtlcId: 2,
		Event: &routerrpc.HtlcEvent_ForwardFailEvent{ForwardFailEvent: &routerrpc.ForwardFailEvent{}},
	}
	require.NoError(t, d.handleEvent(context.Background(), ff, forwards))
	assert.Empty(t, forwards)
	require.Len(t, q.inserted, 1)
	got := q.inserted[0]
	assert.Equal(t, int32(3), got.Amount) // forward incoming 3000/1000
	assert.Equal(t, int32(99), got.WireFailure)
	assert.Equal(t, int32(99), got.FailureDetail)
	assert.InDelta(t, 0.1, got.MissedFee, 1e-9) // (3000-2900)/1000
}

func TestUntrackedFailNoInsert(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{}}
	d := deps(q, &fakeLN{})
	ff := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_FORWARD, IncomingChannelId: 100, OutgoingChannelId: 200,
		Event: &routerrpc.HtlcEvent_ForwardFailEvent{ForwardFailEvent: &routerrpc.ForwardFailEvent{}},
	}
	require.NoError(t, d.handleEvent(context.Background(), ff, map[string]*routerrpc.ForwardEvent{}))
	assert.Empty(t, q.inserted)
}

func TestNonForwardEventIgnored(t *testing.T) {
	q := &fakeQ{channels: map[string]db.GuiChannel{}}
	d := deps(q, &fakeLN{})
	ev := &routerrpc.HtlcEvent{
		EventType: routerrpc.HtlcEvent_SEND,
		Event:     &routerrpc.HtlcEvent_LinkFailEvent{LinkFailEvent: &routerrpc.LinkFailEvent{Info: &routerrpc.HtlcInfo{}}},
	}
	require.NoError(t, d.handleEvent(context.Background(), ev, map[string]*routerrpc.ForwardEvent{}))
	assert.Empty(t, q.inserted) // event_type != FORWARD is ignored
}

func TestForwardKey(t *testing.T) {
	ev := &routerrpc.HtlcEvent{IncomingChannelId: 1, OutgoingChannelId: 2, IncomingHtlcId: 3, OutgoingHtlcId: 4}
	assert.Equal(t, "1:2:3:4", forwardKey(ev)) // colon separator prevents key collisions
}
