package rebalancer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// countingLN hands out a new invoice per AddInvoice and fixed pending HTLCs.
type countingLN struct {
	fakeLN
	invoices int
}

func (c *countingLN) AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error) {
	c.invoices++
	return &lnrpc.AddInvoiceResponse{RHash: []byte{byte(c.invoices)}}, nil
}

func pendingOn(chanID uint64, n int) *lnrpc.ListChannelsResponse {
	return &lnrpc.ListChannelsResponse{Channels: []*lnrpc.Channel{{ChanId: chanID, PendingHtlcs: make([]*lnrpc.HTLC, n)}}}
}

func twoHops() *lnrpc.Route {
	return &lnrpc.Route{Hops: []*lnrpc.Hop{{PubKey: "aa", ChanId: 111}, {PubKey: "me", ChanId: 222}}}
}

func TestSendRouteTimeoutRenewsInvoice(t *testing.T) {
	ln := &countingLN{}
	inv, err := newRebalInvoice(context.Background(), ln, 1000, time.Minute)
	require.NoError(t, err)
	var hashes [][]byte
	r := &fakeRouter{sendFn: func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) {
		hashes = append(hashes, req.PaymentHash)
		return nil, status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	}}
	res := newEngine().sendRoute(context.Background(), ln, ln, r, inv, twoHops(), sendSettings{attemptTimeout: time.Second, htlcSoftLimit: 5})
	assert.True(t, res.timedOut)
	assert.NoError(t, res.err)
	assert.Equal(t, [][]byte{{1}}, hashes)
	assert.Equal(t, []byte{2}, inv.resp.GetRHash(), "the next attempt pays a new invoice")
}

func TestSendRouteHtlcBrake(t *testing.T) {
	sent := 0
	r := &fakeRouter{sendFn: func(req *routerrpc.SendToRouteRequest) (*lnrpc.HTLCAttempt, error) {
		sent++
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_SUCCEEDED}, nil
	}}
	inv := &rebalInvoice{resp: &lnrpc.AddInvoiceResponse{RHash: []byte{1}}}
	s := sendSettings{attemptTimeout: time.Second, htlcSoftLimit: 5}

	// 5 pending HTLCs on the last channel: skipped
	ln := &countingLN{fakeLN: fakeLN{listChans: pendingOn(222, 5)}}
	res := newEngine().sendRoute(context.Background(), ln, ln, r, inv, twoHops(), s)
	assert.True(t, res.skipped)
	assert.Zero(t, sent)

	// soft limit 0: no brake
	res = newEngine().sendRoute(context.Background(), ln, ln, r, inv, twoHops(), sendSettings{attemptTimeout: time.Second})
	assert.False(t, res.skipped)
	assert.Equal(t, 1, sent)

	// 1 pending HTLC: sent at once
	ln = &countingLN{fakeLN: fakeLN{listChans: pendingOn(111, 1)}}
	res = newEngine().sendRoute(context.Background(), ln, ln, r, inv, twoHops(), s)
	assert.Equal(t, lnrpc.HTLCAttempt_SUCCEEDED, res.resp.GetStatus())
	assert.GreaterOrEqual(t, res.durationMs, 0)
}

func TestHtlcWait(t *testing.T) {
	assert.Equal(t, time.Duration(0), htlcWait(1))
	assert.Equal(t, time.Second, htlcWait(2))
	assert.Equal(t, 4*time.Second, htlcWait(3))
	assert.Equal(t, 16*time.Second, htlcWait(4))
}
