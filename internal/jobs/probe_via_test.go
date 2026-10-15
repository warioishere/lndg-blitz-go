package jobs

import (
	"context"
	"encoding/hex"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/routerrpc"
)

// viaLN answers QueryRoutes with a one-hop route to the requested node for the
// source channels in routable, and records the requests.
type viaLN struct {
	routable map[uint64]bool
	reqs     []*lnrpc.QueryRoutesRequest
}

func (c *viaLN) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{}, nil
}
func (c *viaLN) QueryRoutes(ctx context.Context, in *lnrpc.QueryRoutesRequest, opts ...grpc.CallOption) (*lnrpc.QueryRoutesResponse, error) {
	c.reqs = append(c.reqs, in)
	if !c.routable[in.OutgoingChanIds[0]] {
		return &lnrpc.QueryRoutesResponse{}, nil
	}
	return &lnrpc.QueryRoutesResponse{Routes: []*lnrpc.Route{{Hops: []*lnrpc.Hop{{PubKey: in.PubKey}}}}}, nil
}

// viaRouter verifies probes sent out of the source channels in liquid and
// fails all others for liquidity; it records the built hop lists.
type viaRouter struct {
	liquid  map[uint64]bool
	feeMsat int64
	built   [][][]byte
}

func (r *viaRouter) BuildRoute(ctx context.Context, in *routerrpc.BuildRouteRequest, opts ...grpc.CallOption) (*routerrpc.BuildRouteResponse, error) {
	r.built = append(r.built, in.HopPubkeys)
	hops := make([]*lnrpc.Hop, 0, len(in.HopPubkeys))
	for i, pk := range in.HopPubkeys {
		h := &lnrpc.Hop{PubKey: hex.EncodeToString(pk)}
		if i == 0 {
			h.ChanId = in.OutgoingChanId
		}
		hops = append(hops, h)
	}
	return &routerrpc.BuildRouteResponse{Route: &lnrpc.Route{TotalAmtMsat: in.AmtMsat + r.feeMsat, TotalFeesMsat: r.feeMsat, Hops: hops}}, nil
}
func (r *viaRouter) SendToRouteV2(ctx context.Context, in *routerrpc.SendToRouteRequest, opts ...grpc.CallOption) (*lnrpc.HTLCAttempt, error) {
	hops := in.Route.GetHops()
	if r.liquid[hops[0].GetChanId()] {
		return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 1, FailureSourceIndex: uint32(len(hops))}}, nil
	}
	return &lnrpc.HTLCAttempt{Status: lnrpc.HTLCAttempt_FAILED, Failure: &lnrpc.Failure{Code: 15}}, nil
}

// viaSetup is a target (budget 800 ppm) and sources 100 @ 300 ppm, 200 @ 0,
// 300 @ 50 ppm; the target's own channel 900 is no source.
func viaSetup() (db.GuiChannel, []db.GuiChannel) {
	target := db.GuiChannel{ChanID: "900", RemotePubkey: hopPubkey(0xbb), LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 100000}
	own := sourceChan("900", 1000)
	own.RemotePubkey = target.RemotePubkey
	return target, []db.GuiChannel{sourceChan("100", 300), sourceChan("200", 0), sourceChan("300", 50), own}
}

func TestProbeViaForcesRouteThroughNewPeer(t *testing.T) {
	target, channels := viaSetup()
	self, via := hopPubkey(0x03), hopPubkey(0xcc)
	ln := &viaLN{routable: map[uint64]bool{100: true, 200: true, 300: true}}
	r := &viaRouter{liquid: map[uint64]bool{300: true}, feeMsat: 20000}

	got, err := ProbeVia(context.Background(), ln, r, self, target, via, channels, 7000, 10)
	require.NoError(t, err)

	// cheapest source first (200 @ 0, then 300 @ 50); the target's own channel 900 is never a source
	require.Len(t, got, 2)
	assert.Equal(t, "200", got[0].SourceChanID)
	assert.False(t, got[0].Verified)
	assert.Equal(t, "300", got[1].SourceChanID)
	assert.True(t, got[1].Verified) // stops at the first verified route, 100 is not tried
	assert.Equal(t, 50, got[1].SourceFeeRate)
	assert.Equal(t, int64(100000), got[1].AmountSat)
	require.NotNil(t, got[1].FeePpm)
	assert.InDelta(t, 200.0, *got[1].FeePpm, 1e-9) // 20 sat on 100k sat

	// the leg to the new peer ignores the target; the built route ends via -> target -> self
	require.Len(t, ln.reqs, 2)
	assert.Equal(t, via, ln.reqs[0].PubKey)
	assert.Equal(t, [][]byte{mustHex(target.RemotePubkey)}, ln.reqs[0].IgnoredNodes)
	last := r.built[len(r.built)-1]
	require.Len(t, last, 3)
	assert.Equal(t, mustHex(via), last[0])
	assert.Equal(t, mustHex(target.RemotePubkey), last[1])
	assert.Equal(t, mustHex(self), last[2])
	assert.Equal(t, last, got[1].HopKeys) // the route a purchase rebuilds

}

func TestProbeViaCountsOnlySourcesWithRoute(t *testing.T) {
	target, channels := viaSetup()
	ln := &viaLN{routable: map[uint64]bool{100: true}}
	r := &viaRouter{liquid: map[uint64]bool{}}

	got, err := ProbeVia(context.Background(), ln, r, hopPubkey(0x03), target, hopPubkey(0xcc), channels, 7000, 1)
	require.NoError(t, err)
	// 200 and 300 have no route to the new peer and do not use up maxPerTarget
	require.Len(t, got, 1)
	assert.Equal(t, "100", got[0].SourceChanID)
	assert.False(t, got[0].Verified)
	assert.Len(t, ln.reqs, 3)
}

func TestProbeViaNoRoute(t *testing.T) {
	target, channels := viaSetup()
	got, err := ProbeVia(context.Background(), &viaLN{}, &viaRouter{}, hopPubkey(0x03), target, hopPubkey(0xcc), channels, 7000, 10)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestProbeFeeLimitSat(t *testing.T) {
	target := db.GuiChannel{LocalFeeRate: 1000, ArMaxCost: 80, ArAmtTarget: 100000}
	// min(7000, 800-50) = 750 ppm on 100k sat
	assert.Equal(t, int64(75), probeFeeLimitSat(target, 50, 7000))
	// capped by AR-MaxFeeRate: 300 ppm
	assert.Equal(t, int64(30), probeFeeLimitSat(target, 50, 300))
	// source fee eats the budget: no limit
	assert.Equal(t, int64(0), probeFeeLimitSat(target, 1000, 7000))
	assert.Equal(t, int64(0), probeFeeLimitSat(target, 800, 7000)) // budget exactly 0
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(strconv.Quote(s))
	}
	return b
}
