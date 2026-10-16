package rebalancer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// Sending a route: the HTLC brake holds an attempt back while the channels at
// both ends of the route carry pending HTLCs (skipped from AR-HtlcSoftLimit on,
// below that a wait of 4^(n-2) seconds from 2 HTLCs on), and the rebalancer
// waits at most AR-AttemptTimeout for the result. An attempt that does not
// resolve in time stays in flight in LND; the rebalance goes on with a new
// invoice, because LND refuses a second HTLC for a payment hash with one in
// flight.
const htlcCountTTL = 2 * time.Second

// invoiceClient is the AddInvoice subset a rebalance invoice needs.
type invoiceClient interface {
	AddInvoice(ctx context.Context, in *lnrpc.Invoice, opts ...grpc.CallOption) (*lnrpc.AddInvoiceResponse, error)
}

// rebalInvoice is the invoice the routes of a rebalance pay.
type rebalInvoice struct {
	resp   *lnrpc.AddInvoiceResponse
	value  int64
	expiry time.Duration
}

func newRebalInvoice(ctx context.Context, stub invoiceClient, value int64, expiry time.Duration) (*rebalInvoice, error) {
	inv := &rebalInvoice{value: value, expiry: expiry}
	return inv, inv.renew(ctx, stub)
}

// renew replaces the invoice with a new one over the same amount.
func (i *rebalInvoice) renew(ctx context.Context, stub invoiceClient) error {
	resp, err := stub.AddInvoice(ctx, &lnrpc.Invoice{Value: i.value, Expiry: int64(i.expiry / time.Second), CltvExpiry: FinalCltvDelta})
	if err != nil {
		return err
	}
	i.resp = resp
	return nil
}

// htlcCounter caches the pending HTLCs per channel for htlcCountTTL.
type htlcCounter struct {
	mu     sync.Mutex
	at     time.Time
	counts map[uint64]int
}

func newHtlcCounter() *htlcCounter { return &htlcCounter{counts: map[uint64]int{}} }

// HtlcCounter counts pending HTLCs per channel for users outside the rebalancer.
type HtlcCounter = htlcCounter

// ListChannelsClient is the ListChannels subset HtlcCounter needs.
type ListChannelsClient = listChannelsClient

// NewHtlcCounter returns an empty HtlcCounter.
func NewHtlcCounter() *HtlcCounter { return newHtlcCounter() }

// Count returns the pending HTLCs of a channel (see htlcCounter.count).
func (h *htlcCounter) Count(ctx context.Context, lc ListChannelsClient, chanID uint64, now time.Time) int {
	return h.count(ctx, lc, chanID, now)
}

// count returns the pending HTLCs of a channel; 0 when LND cannot be asked.
func (h *htlcCounter) count(ctx context.Context, lc listChannelsClient, chanID uint64, now time.Time) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if now.Sub(h.at) >= htlcCountTTL {
		resp, err := lc.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
		if err != nil {
			rebalLog(fmt.Sprintf("Error reading pending HTLCs: %s", err))
			return 0
		}
		h.counts = make(map[uint64]int, len(resp.GetChannels()))
		for _, c := range resp.GetChannels() {
			h.counts[c.GetChanId()] = len(c.GetPendingHtlcs())
		}
		h.at = now
	}
	return h.counts[chanID]
}

// sendSettings are the per-rebalance limits of sendRoute.
type sendSettings struct {
	attemptTimeout time.Duration
	htlcSoftLimit  int // 0: no brake
}

// readSendSettings reads AR-AttemptTimeout (seconds, default 15) and
// AR-HtlcSoftLimit (default 5); the attempt timeout never exceeds the
// rebalance's own timeout.
func readSendSettings(ctx context.Context, q settingsQuerier, rebalanceTimeout time.Duration) sendSettings {
	s := sendSettings{attemptTimeout: 15 * time.Second, htlcSoftLimit: 5}
	if v, err := getLocalSettingInt(ctx, q, "AR-AttemptTimeout", 15); err == nil && v > 0 {
		s.attemptTimeout = time.Duration(v) * time.Second
	}
	if v, err := getLocalSettingInt(ctx, q, "AR-HtlcSoftLimit", 5); err == nil && v >= 0 {
		s.htlcSoftLimit = v
	}
	s.attemptTimeout = min(s.attemptTimeout, rebalanceTimeout)
	return s
}

// sendResult is the end of one sendRoute call. skipped is set when the HTLC
// brake held the route back and nothing was sent; timedOut when the result did
// not come within the attempt timeout.
type sendResult struct {
	resp       *lnrpc.HTLCAttempt
	err        error
	skipped    bool
	timedOut   bool
	durationMs int
}

// htlcWait is the brake's wait for n pending HTLCs below the soft limit.
func htlcWait(n int) time.Duration {
	if n < 2 {
		return 0
	}
	return time.Duration(math.Pow(4, float64(n-2))) * time.Second
}

// sendRoute sends one HTLC over route to the invoice, with the HTLC brake and
// the attempt timeout. After a timeout it renews the invoice.
func (e *engine) sendRoute(ctx context.Context, lc listChannelsClient, ic invoiceClient, routerstub sourceRouterClient,
	inv *rebalInvoice, route *lnrpc.Route, s sendSettings) sendResult {
	hops := route.GetHops()
	if len(hops) == 0 {
		return sendResult{err: errors.New("route without hops")}
	}
	now := time.Now()
	n := max(e.htlcs.count(ctx, lc, hops[0].GetChanId(), now), e.htlcs.count(ctx, lc, hops[len(hops)-1].GetChanId(), now))
	if s.htlcSoftLimit > 0 && n >= s.htlcSoftLimit {
		rebalLog(fmt.Sprintf("HTLC brake: %d pending HTLCs on %s or %s, route skipped", n,
			e.alias.label(strconv.FormatUint(hops[0].GetChanId(), 10)), e.alias.label(strconv.FormatUint(hops[len(hops)-1].GetChanId(), 10))))
		return sendResult{skipped: true}
	}
	if w := htlcWait(n); w > 0 && s.htlcSoftLimit > 0 {
		select {
		case <-ctx.Done():
			return sendResult{err: ctx.Err()}
		case <-time.After(w):
		}
	}
	start := time.Now()
	cctx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
	resp, err := routerstub.SendToRouteV2(cctx, &routerrpc.SendToRouteRequest{PaymentHash: inv.resp.GetRHash(), Route: route})
	timedOut := err != nil && (status.Code(err) == codes.DeadlineExceeded || errors.Is(cctx.Err(), context.DeadlineExceeded)) && ctx.Err() == nil
	cancel()
	res := sendResult{resp: resp, err: err, timedOut: timedOut, durationMs: int(time.Since(start) / time.Millisecond)}
	if timedOut {
		res.err = nil
		if rerr := inv.renew(ctx, ic); rerr != nil {
			res.err = rerr
		}
	}
	return res
}
