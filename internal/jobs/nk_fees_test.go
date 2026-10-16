package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

type fakeNkJobQ struct {
	settings map[string]string
	channels []db.GuiChannel
	fwd      []db.NkForwardFlowsRow
	updates  []db.UpdateChannelNkPolicyParams
	logs     []db.InsertNkFeeLogParams
	autofees []db.InsertAutofeeParams
	inbound  []db.InsertInboundFeeLogParams
	purged   int
}

func (f *fakeNkJobQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	return db.GuiLocalsetting{Key: key, Value: f.settings[key]}, nil
}
func (f *fakeNkJobQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeNkJobQ) ListNkJobChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeNkJobQ) NkForwardFlows(ctx context.Context, arg db.NkForwardFlowsParams) ([]db.NkForwardFlowsRow, error) {
	return f.fwd, nil
}
func (f *fakeNkJobQ) NkBalanceOutFlows(ctx context.Context, arg db.NkBalanceOutFlowsParams) ([]db.NkBalanceOutFlowsRow, error) {
	return nil, nil
}
func (f *fakeNkJobQ) UpdateChannelNkPolicy(ctx context.Context, arg db.UpdateChannelNkPolicyParams) error {
	f.updates = append(f.updates, arg)
	return nil
}
func (f *fakeNkJobQ) InsertNkFeeLog(ctx context.Context, arg db.InsertNkFeeLogParams) error {
	f.logs = append(f.logs, arg)
	return nil
}
func (f *fakeNkJobQ) DeleteOldNkFeeLogs(ctx context.Context, timestamp pgtype.Timestamptz) error {
	f.purged++
	return nil
}
func (f *fakeNkJobQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error {
	f.autofees = append(f.autofees, arg)
	return nil
}
func (f *fakeNkJobQ) InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error {
	f.inbound = append(f.inbound, arg)
	return nil
}

func nkChannel(id, peer, mode string, fee int32, local int64) db.GuiChannel {
	return db.GuiChannel{ChanID: id, RemotePubkey: peer, Alias: "peer-" + peer, AfMode: mode, LocalFeeRate: fee,
		Capacity: 10_000_000, LocalBalance: local, ArInTarget: 50, LocalCltv: 144, FundingTxid: "aa",
		LocalMaxHtlcMsat: 9_900_000_000, LocalMinHtlcMsat: 1000, LocalChanReserve: 100_000}
}

func TestNkFeesAppliesNkChannels(t *testing.T) {
	q := &fakeNkJobQ{settings: map[string]string{}, channels: []db.GuiChannel{
		nkChannel("1", "a", "nk", 1000, 2_000_000), // 20 % local, target 50 %: up 3 %
		nkChannel("2", "a", "nk", 900, 1_000_000),  // same peer: decided together
	}}
	q.channels[0].InboundOffset = -1
	q.fwd = []db.NkForwardFlowsRow{{ChanID: "1", Amt6h: 1_000_000, Fee6h: 1000, Amt24h: 1_000_000, Fee24h: 1000, Amt72h: 1_000_000, Fee72h: 1000}}
	client := &fakePolicyClient{version: "0.18.3-beta"}
	n := &nkRunner{shadow: map[string]*nkShadow{}, reason: map[string]string{}}
	now := time.Unix(1_700_000_000, 0)
	require.NoError(t, n.run(context.Background(), q, client, now))

	require.Len(t, client.policyReqs, 2)
	for _, r := range client.policyReqs {
		assert.InDelta(t, 1030.0/1e6, r.FeeRate, 1e-12, "the peer's highest fee + 3 %, below the target fee 1200")
		assert.Equal(t, int64(1), r.BaseFeeMsat)
		assert.Equal(t, int32(-961), r.InboundFee.FeeRatePpm, "sold at 1000 less the margin at 15 % local: 10 + (200 - 10) x 0.15")
		assert.Equal(t, int32(-1), r.InboundFee.BaseFeeMsat)
	}
	mh := float64(client.policyReqs[0].MaxHtlcMsat)
	assert.True(t, mh <= 1.9e9 && mh*1.2 > 1.9e9, "the highest 1.2 step below the 1.9M available")
	require.Len(t, q.updates, 2)
	assert.Equal(t, int32(1030), q.updates[0].LocalFeeRate)
	assert.True(t, q.updates[0].NkLastChange.Valid)
	assert.Len(t, q.autofees, 2)
	require.Len(t, q.logs, 2)
	assert.True(t, q.logs[0].Applied)
	assert.Equal(t, "below target", q.logs[0].Reason)

	// within a minute nothing runs again
	require.NoError(t, n.run(context.Background(), q, client, now.Add(30*time.Second)))
	assert.Len(t, client.policyReqs, 2)
}

func TestNkFeesShadow(t *testing.T) {
	q := &fakeNkJobQ{settings: map[string]string{}, channels: []db.GuiChannel{nkChannel("1", "a", "lndg", 1000, 8_000_000)}}
	client := &fakePolicyClient{version: "0.18.3-beta"}
	n := &nkRunner{shadow: map[string]*nkShadow{}, reason: map[string]string{}}
	now := time.Unix(1_700_000_000, 0)
	require.NoError(t, n.run(context.Background(), q, client, now))
	assert.Empty(t, client.policyReqs, "a shadow channel is never changed")
	require.Len(t, q.logs, 1)
	assert.False(t, q.logs[0].Applied)
	assert.Equal(t, int32(1000), q.logs[0].FeeOld)
	assert.Equal(t, int32(990), q.logs[0].FeeNew)

	// the shadow fee carries on: 3 minutes later it waits, 6 minutes later it steps again
	require.NoError(t, n.run(context.Background(), q, client, now.Add(3*time.Minute)))
	require.NoError(t, n.run(context.Background(), q, client, now.Add(6*time.Minute)))
	last := q.logs[len(q.logs)-1]
	assert.Equal(t, int32(990), last.FeeOld)
	assert.Equal(t, int32(980), last.FeeNew)

	// NK-Shadow 0: no shadow decisions
	q2 := &fakeNkJobQ{settings: map[string]string{"NK-Shadow": "0"}, channels: q.channels}
	n2 := &nkRunner{shadow: map[string]*nkShadow{}, reason: map[string]string{}}
	require.NoError(t, n2.run(context.Background(), q2, client, now))
	assert.Empty(t, q2.logs)
}

func TestNkGroupInputKeeps30Days(t *testing.T) {
	// two channels to one peer that sold only further back than 72 hours
	chs := []db.GuiChannel{nkChannel("1", "a", "nk", 4000, 100_000), nkChannel("2", "a", "nk", 4000, 100_000)}
	flows := map[string]af.NkFlow{
		"1": {All30dAmt: 1_000_000, All30dFee: 3800},
		"2": {All30dAmt: 1_000_000, All30dFee: 3900},
	}
	in := af.NkGroupInput(chs, flows)
	ref, ok := in.Flow.RefPrice()
	require.True(t, ok)
	assert.Equal(t, 3850, ref, "the 30-day price of both channels")
	p, err := af.LoadNkParams(context.Background(), &fakeNkJobQ{settings: map[string]string{}})
	require.NoError(t, err)
	assert.Equal(t, 7000, af.NkCap(in.Flow, p), "2 x 3850, bounded by NK-MaxRate")
}
