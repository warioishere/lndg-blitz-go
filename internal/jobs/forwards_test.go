package jobs

import (
	"context"
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

type fakeForwardsQ struct {
	settings map[string]string
	channels []db.GuiChannel
	inserts  []db.InsertForwardParams
}

func (f *fakeForwardsQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeForwardsQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeForwardsQ) ListEpEnabledChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error) {
	return nil, nil
}
func (f *fakeForwardsQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error {
	return nil
}
func (f *fakeForwardsQ) UpdateChannelEmergencyFee(ctx context.Context, arg db.UpdateChannelEmergencyFeeParams) error {
	return nil
}
func (f *fakeForwardsQ) LatestForward(ctx context.Context) (db.LatestForwardRow, error) {
	return db.LatestForwardRow{}, pgx.ErrNoRows
}
func (f *fakeForwardsQ) CountForwardsAtDate(ctx context.Context, forwardDate pgtype.Timestamptz) (int64, error) {
	return 0, nil
}
func (f *fakeForwardsQ) GetChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeForwardsQ) InsertForward(ctx context.Context, arg db.InsertForwardParams) error {
	f.inserts = append(f.inserts, arg)
	return nil
}

type fakeForwardsClient struct {
	events []*lnrpc.ForwardingEvent
}

func (c *fakeForwardsClient) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	return &lnrpc.ListChannelsResponse{}, nil
}
func (c *fakeForwardsClient) UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
	return &lnrpc.PolicyUpdateResponse{}, nil
}
func (c *fakeForwardsClient) ForwardingHistory(ctx context.Context, in *lnrpc.ForwardingHistoryRequest, opts ...grpc.CallOption) (*lnrpc.ForwardingHistoryResponse, error) {
	return &lnrpc.ForwardingHistoryResponse{ForwardingEvents: c.events}, nil
}

func TestUpdateForwards_InboundFee(t *testing.T) {
	oldFeesUpdated := time.Now().Add(-48 * time.Hour)
	q := &fakeForwardsQ{
		settings: map[string]string{}, // EP-Enabled absent -> emergency check no-ops
		channels: []db.GuiChannel{{
			ChanID: "222", RemotePubkey: "bobpubkey", Alias: "Bob",
			LocalFeeRate: 500, LocalBaseFee: 1000,
			FeesUpdated: pgtype.Timestamptz{Time: oldFeesUpdated, Valid: true},
		}},
	}
	client := &fakeForwardsClient{events: []*lnrpc.ForwardingEvent{
		{Timestamp: uint64(time.Now().Unix()), ChanIdIn: 111, ChanIdOut: 222, FeeMsat: 1000, AmtInMsat: 1001000, AmtOutMsat: 1000000},
	}}
	require.NoError(t, UpdateForwards(context.Background(), q, client))

	require.Len(t, q.inserts, 1)
	ins := q.inserts[0]
	assert.Equal(t, "222", ins.ChanIDOut)
	assert.Equal(t, "Bob", ins.ChanOutAlias.String)
	assert.InDelta(t, 1.0, ins.Fee, 1e-9) // 1000 msat / 1000
	// out_fee = int(1000000*500/1e6 + 1000) = 1500; in_fee = 1500-1000 = 500 -> 0.5
	assert.InDelta(t, 0.5, ins.InboundFee, 1e-9)
}
