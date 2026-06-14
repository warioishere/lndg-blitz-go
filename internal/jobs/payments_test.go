package jobs

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

type fakePaymentsQ struct {
	hops       []db.InsertPaymentHopParams
	hopResults *db.UpdatePaymentHopResultsParams
	basic      []db.UpdatePaymentBasicParams
	deleted    []string
}

func (f *fakePaymentsQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakePaymentsQ) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	return db.GuiNodecache{}, pgx.ErrNoRows
}
func (f *fakePaymentsQ) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error {
	return nil
}
func (f *fakePaymentsQ) ListInflightPayments(ctx context.Context) ([]db.ListInflightPaymentsRow, error) {
	return nil, nil
}
func (f *fakePaymentsQ) MaxPaymentIndex(ctx context.Context) (int32, error) { return 0, nil }
func (f *fakePaymentsQ) InsertPayment(ctx context.Context, arg db.InsertPaymentParams) error {
	return nil
}
func (f *fakePaymentsQ) SetPaymentStatus(ctx context.Context, arg db.SetPaymentStatusParams) error {
	return nil
}
func (f *fakePaymentsQ) UpdatePaymentBasic(ctx context.Context, arg db.UpdatePaymentBasicParams) error {
	f.basic = append(f.basic, arg)
	return nil
}
func (f *fakePaymentsQ) UpdatePaymentHopResults(ctx context.Context, arg db.UpdatePaymentHopResultsParams) error {
	f.hopResults = &arg
	return nil
}
func (f *fakePaymentsQ) DeletePaymentHops(ctx context.Context, paymentHashID string) error {
	f.deleted = append(f.deleted, paymentHashID)
	return nil
}
func (f *fakePaymentsQ) InsertPaymentHop(ctx context.Context, arg db.InsertPaymentHopParams) error {
	f.hops = append(f.hops, arg)
	return nil
}

type fakePaymentsClient struct{ alias string }

func (c *fakePaymentsClient) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (c *fakePaymentsClient) ListPayments(ctx context.Context, in *lnrpc.ListPaymentsRequest, opts ...grpc.CallOption) (*lnrpc.ListPaymentsResponse, error) {
	return &lnrpc.ListPaymentsResponse{}, nil
}
func (c *fakePaymentsClient) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return &lnrpc.NodeInfo{Node: &lnrpc.LightningNode{Alias: c.alias}}, nil
}

func TestUpdatePayment_HopsAndRebal(t *testing.T) {
	q := &fakePaymentsQ{}
	client := &fakePaymentsClient{alias: "N"}
	payment := &lnrpc.Payment{
		PaymentHash: "ph1", Status: lnrpc.Payment_SUCCEEDED, ValueMsat: 100000, FeeMsat: 1000, PaymentIndex: 7,
		Htlcs: []*lnrpc.HTLCAttempt{{
			AttemptId: 42, Status: lnrpc.HTLCAttempt_SUCCEEDED,
			Route: &lnrpc.Route{Hops: []*lnrpc.Hop{
				{ChanId: 111, PubKey: "mid", FeeMsat: 500, AmtToForwardMsat: 100000, ChanCapacity: 1000000},
				{ChanId: 222, PubKey: "self", FeeMsat: 0, AmtToForwardMsat: 99500, ChanCapacity: 2000000},
			}},
		}},
	}
	require.NoError(t, updatePayment(context.Background(), q, client, payment, "self"))

	require.Len(t, q.deleted, 1)
	require.Len(t, q.hops, 2)
	assert.Equal(t, "111", q.hops[0].ChanID)
	assert.Equal(t, "N", q.hops[0].Alias, "first hop plain alias")
	assert.Equal(t, "222", q.hops[1].ChanID)
	assert.Equal(t, "N[ 2-1-0-0 ]", q.hops[1].Alias, "last hop alias + htlc info")

	require.NotNil(t, q.hopResults)
	assert.Equal(t, "111", q.hopResults.ChanOut.String)
	assert.Equal(t, "N", q.hopResults.ChanOutAlias.String)
	assert.True(t, q.hopResults.RebalChan.Valid)
	assert.Equal(t, "222", q.hopResults.RebalChan.String, "rebal_chan = self hop chan_id")
}

func TestUpdatePayment_MPP(t *testing.T) {
	q := &fakePaymentsQ{}
	client := &fakePaymentsClient{alias: "N"}
	mkAttempt := func(chanID uint64) *lnrpc.HTLCAttempt {
		return &lnrpc.HTLCAttempt{
			Status: lnrpc.HTLCAttempt_SUCCEEDED,
			Route:  &lnrpc.Route{Hops: []*lnrpc.Hop{{ChanId: chanID, PubKey: "x"}}},
		}
	}
	payment := &lnrpc.Payment{
		PaymentHash: "ph2", Status: lnrpc.Payment_SUCCEEDED,
		Htlcs: []*lnrpc.HTLCAttempt{mkAttempt(111), mkAttempt(333)},
	}
	require.NoError(t, updatePayment(context.Background(), q, client, payment, "self"))
	require.NotNil(t, q.hopResults)
	assert.Equal(t, "MPP", q.hopResults.ChanOut.String, "two succeeded attempts -> MPP")
	assert.Equal(t, "MPP", q.hopResults.ChanOutAlias.String)
}
