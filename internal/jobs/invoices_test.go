package jobs

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/signrpc"
)

type fakeInvoicesQ struct {
	settled  *db.UpdateInvoiceSettledParams
	stateSet []db.SetInvoiceStateParams
}

func (f *fakeInvoicesQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeInvoicesQ) GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error) {
	return db.GuiNodecache{}, pgx.ErrNoRows
}
func (f *fakeInvoicesQ) UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error {
	return nil
}
func (f *fakeInvoicesQ) ListOpenInvoices(ctx context.Context) ([]db.ListOpenInvoicesRow, error) {
	return nil, nil
}
func (f *fakeInvoicesQ) MaxInvoiceIndex(ctx context.Context) (int32, error) { return 0, nil }
func (f *fakeInvoicesQ) InsertInvoice(ctx context.Context, arg db.InsertInvoiceParams) error {
	return nil
}
func (f *fakeInvoicesQ) SetInvoiceState(ctx context.Context, arg db.SetInvoiceStateParams) error {
	f.stateSet = append(f.stateSet, arg)
	return nil
}
func (f *fakeInvoicesQ) UpdateInvoiceSettled(ctx context.Context, arg db.UpdateInvoiceSettledParams) error {
	f.settled = &arg
	return nil
}
func (f *fakeInvoicesQ) GetChannelAlias(ctx context.Context, chanID string) (string, error) {
	if chanID == "555" {
		return "Carol", nil
	}
	return "", pgx.ErrNoRows
}

type fakeInvoicesClient struct{}

func (c *fakeInvoicesClient) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (c *fakeInvoicesClient) ListInvoices(ctx context.Context, in *lnrpc.ListInvoiceRequest, opts ...grpc.CallOption) (*lnrpc.ListInvoiceResponse, error) {
	return &lnrpc.ListInvoiceResponse{}, nil
}
func (c *fakeInvoicesClient) GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error) {
	return &lnrpc.NodeInfo{}, nil
}
func (c *fakeInvoicesClient) VerifyMessage(ctx context.Context, in *signrpc.VerifyMessageReq, opts ...grpc.CallOption) (*signrpc.VerifyMessageResp, error) {
	return &signrpc.VerifyMessageResp{}, nil
}

func TestUpdateInvoice_SettledKeysend(t *testing.T) {
	q := &fakeInvoicesQ{}
	client := &fakeInvoicesClient{}
	invoice := &lnrpc.Invoice{
		State: lnrpc.Invoice_SETTLED, AmtPaidSat: 1234, SettleDate: 1700000000,
		Htlcs: []*lnrpc.InvoiceHTLC{{
			ChanId: 555,
			CustomRecords: map[uint64][]byte{
				5482373484: {0xde, 0xad},
				34349334:   []byte("hi there"),
			},
		}},
	}
	require.NoError(t, updateInvoice(context.Background(), q, client, invoice, "rh1"))

	require.NotNil(t, q.settled)
	assert.Equal(t, "555", q.settled.ChanIn.String)
	assert.Equal(t, "Carol", q.settled.ChanInAlias.String)
	assert.Equal(t, hex.EncodeToString([]byte{0xde, 0xad}), q.settled.KeysendPreimage.String)
	assert.Equal(t, "hi there", q.settled.Message.String)
	assert.False(t, q.settled.Sender.Valid, "no signer TLVs -> sender None")
	assert.Equal(t, int64(1234), q.settled.AmtPaid)
}

func TestUpdateInvoice_NotSettled(t *testing.T) {
	q := &fakeInvoicesQ{}
	invoice := &lnrpc.Invoice{State: lnrpc.Invoice_CANCELED}
	require.NoError(t, updateInvoice(context.Background(), q, &fakeInvoicesClient{}, invoice, "rh2"))
	require.Len(t, q.stateSet, 1)
	assert.Equal(t, int32(2), q.stateSet[0].State)
	assert.Nil(t, q.settled)
}
