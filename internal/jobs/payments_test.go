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
	feeRates   map[string]int32 // chan_id -> local_fee_rate for ChannelFeeRates
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
func (f *fakePaymentsQ) ChannelFeeRates(ctx context.Context, chanIds []string) ([]db.ChannelFeeRatesRow, error) {
	var out []db.ChannelFeeRatesRow
	for _, id := range chanIds {
		if rate, ok := f.feeRates[id]; ok {
			out = append(out, db.ChannelFeeRatesRow{ChanID: id, LocalFeeRate: rate})
		}
	}
	return out, nil
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

// rebalance attempt: source channel src, last hop on our own node into chan 999
func rebalAttempt(src uint64, totalAmtMsat int64) *lnrpc.HTLCAttempt {
	return &lnrpc.HTLCAttempt{
		Status: lnrpc.HTLCAttempt_SUCCEEDED,
		Route: &lnrpc.Route{TotalAmtMsat: totalAmtMsat, Hops: []*lnrpc.Hop{
			{ChanId: src, PubKey: "mid"},
			{ChanId: 999, PubKey: "self"},
		}},
	}
}

func TestUpdatePayment_SourceFeeRate(t *testing.T) {
	client := &fakePaymentsClient{alias: "N"}
	run := func(q *fakePaymentsQ, status lnrpc.Payment_PaymentStatus, attempts ...*lnrpc.HTLCAttempt) *db.UpdatePaymentHopResultsParams {
		payment := &lnrpc.Payment{PaymentHash: "ph", Status: status, Htlcs: attempts}
		require.NoError(t, updatePayment(context.Background(), q, client, payment, "self"))
		require.NotNil(t, q.hopResults)
		return q.hopResults
	}

	res := run(&fakePaymentsQ{feeRates: map[string]int32{"111": 400}}, lnrpc.Payment_SUCCEEDED, rebalAttempt(111, 1000))
	assert.Equal(t, int32(400), res.SourceFeeRate.Int32, "single source -> its outbound fee")
	assert.True(t, res.SourceFeeRate.Valid)

	res = run(&fakePaymentsQ{feeRates: map[string]int32{"111": 400, "333": 100}}, lnrpc.Payment_SUCCEEDED,
		rebalAttempt(111, 3000), rebalAttempt(333, 1000))
	assert.Equal(t, int32(325), res.SourceFeeRate.Int32, "MPP -> amount weighted (400*3 + 100*1) / 4")

	res = run(&fakePaymentsQ{feeRates: map[string]int32{"111": 400}}, lnrpc.Payment_SUCCEEDED,
		rebalAttempt(111, 1000), rebalAttempt(555, 1000))
	assert.Equal(t, int32(400), res.SourceFeeRate.Int32, "unknown source part is left out of the average")

	res = run(&fakePaymentsQ{}, lnrpc.Payment_SUCCEEDED, rebalAttempt(111, 1000))
	assert.False(t, res.SourceFeeRate.Valid, "no known source -> NULL, read side falls back")

	res = run(&fakePaymentsQ{feeRates: map[string]int32{"111": 400}}, lnrpc.Payment_IN_FLIGHT, rebalAttempt(111, 1000))
	assert.False(t, res.SourceFeeRate.Valid, "only captured once the payment succeeded")

	notRebal := &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_SUCCEEDED, Route: &lnrpc.Route{TotalAmtMsat: 1000, Hops: []*lnrpc.Hop{
		{ChanId: 111, PubKey: "mid"}, {ChanId: 777, PubKey: "someone"},
	}}}
	res = run(&fakePaymentsQ{feeRates: map[string]int32{"111": 400}}, lnrpc.Payment_SUCCEEDED, notRebal)
	assert.False(t, res.SourceFeeRate.Valid, "not a rebalance -> no source fee")
}
