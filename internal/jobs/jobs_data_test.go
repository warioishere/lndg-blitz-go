package jobs

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// --- update_onchain ---

type fakeOnchainQ struct {
	deleted     bool
	nextBlock   int32
	inserted    []db.InsertOnchainParams
	insertErrOn int
}

func (f *fakeOnchainQ) DeleteOnchainZeroBlock(ctx context.Context) error {
	f.deleted = true
	return nil
}
func (f *fakeOnchainQ) NextOnchainBlockHeight(ctx context.Context) (int32, error) {
	return f.nextBlock, nil
}
func (f *fakeOnchainQ) InsertOnchain(ctx context.Context, arg db.InsertOnchainParams) error {
	f.inserted = append(f.inserted, arg)
	return nil
}

type fakeOnchainClient struct {
	startHeight int32
	txs         []*lnrpc.Transaction
}

func (c *fakeOnchainClient) GetTransactions(ctx context.Context, in *lnrpc.GetTransactionsRequest, opts ...grpc.CallOption) (*lnrpc.TransactionDetails, error) {
	c.startHeight = in.StartHeight
	return &lnrpc.TransactionDetails{Transactions: c.txs}, nil
}

func TestUpdateOnchain(t *testing.T) {
	q := &fakeOnchainQ{nextBlock: 800001}
	client := &fakeOnchainClient{txs: []*lnrpc.Transaction{
		{TxHash: "tx1", TimeStamp: 1700000000, Amount: -5000, TotalFees: 250, BlockHash: "bh1", BlockHeight: 800050, Label: "sweep"},
	}}
	require.NoError(t, UpdateOnchain(context.Background(), q, client))
	assert.True(t, q.deleted, "block_height=0 rows deleted")
	assert.Equal(t, int32(800001), client.startHeight, "start_height = next block")
	require.Len(t, q.inserted, 1)
	assert.Equal(t, "tx1", q.inserted[0].TxHash)
	assert.Equal(t, int64(-5000), q.inserted[0].Amount)
	assert.Equal(t, int32(250), q.inserted[0].Fee)
	assert.Equal(t, int32(800050), q.inserted[0].BlockHeight)
}

func TestTruncRunes(t *testing.T) {
	assert.Equal(t, "abc", truncRunes("abc", 100))
	long := make([]rune, 150)
	for i := range long {
		long[i] = 'x'
	}
	assert.Len(t, []rune(truncRunes(string(long), 100)), 100)
}

// --- clean_payments ---

type fakeCleanQ struct {
	settings map[string]string
	payments []db.ListPaymentsToCleanRow
	cleaned  []string
}

func (f *fakeCleanQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, errNoRowsSentinel
}
func (f *fakeCleanQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeCleanQ) ListPaymentsToClean(ctx context.Context, creationDate pgtype.Timestamptz) ([]db.ListPaymentsToCleanRow, error) {
	return f.payments, nil
}
func (f *fakeCleanQ) SetPaymentCleaned(ctx context.Context, paymentHash string) error {
	f.cleaned = append(f.cleaned, paymentHash)
	return nil
}

var errNoRowsSentinel = errors.New("no rows")

type fakeCleanClient struct {
	deleted []string
	err     error
}

func (c *fakeCleanClient) DeletePayment(ctx context.Context, in *lnrpc.DeletePaymentRequest, opts ...grpc.CallOption) (*lnrpc.DeletePaymentResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.deleted = append(c.deleted, string(in.PaymentHash))
	return &lnrpc.DeletePaymentResponse{}, nil
}

func TestCleanPayments_Disabled(t *testing.T) {
	q := &fakeCleanQ{settings: map[string]string{}}
	require.NoError(t, CleanPayments(context.Background(), q, &fakeCleanClient{}))
	assert.Equal(t, "0", q.settings["LND-CleanPayments"])
	assert.Empty(t, q.cleaned)
}

func TestCleanPayments_DeletesAndMarks(t *testing.T) {
	q := &fakeCleanQ{
		settings: map[string]string{"LND-CleanPayments": "1"},
		payments: []db.ListPaymentsToCleanRow{
			{PaymentHash: "deadbeef", Status: 2, Index: 5},
			{PaymentHash: "cafe", Status: 3, Index: 6},
		},
	}
	client := &fakeCleanClient{}
	require.NoError(t, CleanPayments(context.Background(), q, client))
	assert.Equal(t, []string{"deadbeef", "cafe"}, q.cleaned, "all marked cleaned")
	require.Len(t, client.deleted, 2)
}

func TestCleanPayments_RPCErrorStillMarks(t *testing.T) {
	q := &fakeCleanQ{
		settings: map[string]string{"LND-CleanPayments": "1"},
		payments: []db.ListPaymentsToCleanRow{{PaymentHash: "deadbeef", Status: 2, Index: 5}},
	}
	client := &fakeCleanClient{err: errors.New("delete failed")}
	require.NoError(t, CleanPayments(context.Background(), q, client))
	assert.Equal(t, []string{"deadbeef"}, q.cleaned, "finally marks cleaned even on RPC error")
}
