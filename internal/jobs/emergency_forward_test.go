package jobs

import (
	"context"
	"testing"

	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// fakeForwardQ adds the by-ID channel lookup to fakeFeeQ.
type fakeForwardQ struct{ *fakeFeeQ }

func (f fakeForwardQ) ListEpEnabledChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error) {
	want := map[string]bool{}
	for _, id := range chanIDs {
		want[id] = true
	}
	var out []db.GuiChannel
	for _, c := range f.channels {
		if want[c.ChanID] && c.EpEnabled {
			out = append(out, c)
		}
	}
	return out, nil
}

type fakeForwardClient struct {
	fakePolicyClient
	live []*lnrpc.Channel
}

func (c *fakeForwardClient) ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error) {
	return &lnrpc.ListChannelsResponse{Channels: c.live}, nil
}

// The live check raises the fee of a channel whose live outbound (balance +
// outgoing pending HTLCs) is below ep_live_threshold, with only EP-Enabled set.
func TestEmergencyForwardCheck(t *testing.T) {
	q := fakeForwardQ{&fakeFeeQ{
		settings: map[string]string{"EP-Enabled": "1"},
		channels: []db.GuiChannel{
			{ChanID: "100", EpEnabled: true, EpLiveThreshold: 40, EpLiveIncPct: 5, LocalFeeRate: 200, FundingTxid: "abc"},
			{ChanID: "200", EpEnabled: true, EpLiveThreshold: 40, EpLiveIncPct: 5, LocalFeeRate: 200, FundingTxid: "def"},
		},
	}}
	client := &fakeForwardClient{live: []*lnrpc.Channel{
		// 300k + 50k outgoing pending of 1M = 35% < 40 -> raise
		{ChanId: 100, Capacity: 1_000_000, LocalBalance: 300_000, PendingHtlcs: []*lnrpc.HTLC{
			{Incoming: false, Amount: 50_000}, {Incoming: true, Amount: 900_000},
		}},
		// 500k of 1M = 50% -> keep
		{ChanId: 200, Capacity: 1_000_000, LocalBalance: 500_000},
	}}
	require.NoError(t, EmergencyForwardCheck(context.Background(), q, client, []string{"100", "200"}))
	require.Len(t, q.emergencyUp, 1)
	assert.Equal(t, "100", q.emergencyUp[0].ChanID)
	assert.Equal(t, int32(210), q.emergencyUp[0].LocalFeeRate, "int(200 * 1.05)")
	require.Len(t, q.autofees, 1)
	assert.Equal(t, "EP-L", q.autofees[0].Setting)
}

// Within ep_cooldown since the last increase, a further forward does not raise again.
func TestEmergencyForwardCheck_Cooldown(t *testing.T) {
	recent := pgtype.Timestamptz{Time: time.Now().Add(-5 * time.Minute), Valid: true}
	q := fakeForwardQ{&fakeFeeQ{
		settings: map[string]string{"EP-Enabled": "1"},
		channels: []db.GuiChannel{{ChanID: "100", EpEnabled: true, EpLiveThreshold: 40, EpLiveIncPct: 5,
			EpCooldown: 10, EpUpdated: recent, LocalFeeRate: 200, FundingTxid: "abc"}},
	}}
	client := &fakeForwardClient{live: []*lnrpc.Channel{{ChanId: 100, Capacity: 1_000_000, LocalBalance: 100_000}}}
	require.NoError(t, EmergencyForwardCheck(context.Background(), q, client, []string{"100"}))
	assert.Empty(t, q.emergencyUp)
	assert.Empty(t, client.policyReqs)
}
