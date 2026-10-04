package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// fakeAfQ implements the auto_fees querier interface (settings, channels, writes).
type fakeAfQ struct {
	settings    map[string]string
	channels    []db.GuiChannel
	fresh       map[string]db.GuiChannel // GetChannel override: the row as changed after the snapshot
	autofees    []db.InsertAutofeeParams
	inboundLogs []db.InsertInboundFeeLogParams
	autoFeesUp  []db.UpdateChannelAutoFeesParams
}

// --- settings ---
func (f *fakeAfQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeAfQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeAfQ) CreateLocalSettingIfAbsent(ctx context.Context, arg db.CreateLocalSettingIfAbsentParams) error {
	if _, ok := f.settings[arg.Key]; !ok {
		f.settings[arg.Key] = arg.Value
	}
	return nil
}
func (f *fakeAfQ) AnyLocalSettingExists(ctx context.Context, keys []string) (bool, error) {
	for _, k := range keys {
		if _, ok := f.settings[k]; ok {
			return true, nil
		}
	}
	return false, nil
}

// --- fetch (all empty) ---
func (f *fakeAfQ) RebalPaymentsForChannel(ctx context.Context, arg db.RebalPaymentsForChannelParams) ([]db.RebalPaymentsForChannelRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsInSumFee(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsInSumFeeRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsInSum(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsInSumRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsOutSumFee(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsOutSumFeeRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsOutSum(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsOutSumRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsLastOut(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsLastOutRow, error) {
	return nil, nil
}
func (f *fakeAfQ) ForwardsLastIn(ctx context.Context, t pgtype.Timestamptz) ([]db.ForwardsLastInRow, error) {
	return nil, nil
}
func (f *fakeAfQ) FailedHTLCsForAF(ctx context.Context, t pgtype.Timestamptz) ([]db.FailedHTLCsForAFRow, error) {
	return nil, nil
}

// --- jobs ---
func (f *fakeAfQ) ListAutoFeesChannels(ctx context.Context) ([]db.GuiChannel, error) {
	return f.channels, nil
}
func (f *fakeAfQ) GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error) {
	if c, ok := f.fresh[chanID]; ok {
		return c, nil
	}
	for _, c := range f.channels {
		if c.ChanID == chanID {
			return c, nil
		}
	}
	return db.GuiChannel{}, pgx.ErrNoRows
}
func (f *fakeAfQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error {
	f.autofees = append(f.autofees, arg)
	return nil
}
func (f *fakeAfQ) InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error {
	f.inboundLogs = append(f.inboundLogs, arg)
	return nil
}
func (f *fakeAfQ) UpdateChannelAutoFees(ctx context.Context, arg db.UpdateChannelAutoFeesParams) error {
	f.autoFeesUp = append(f.autoFeesUp, arg)
	return nil
}

func TestAutoFees_Disabled(t *testing.T) {
	q := &fakeAfQ{settings: map[string]string{}}
	require.NoError(t, AutoFees(context.Background(), q, &fakePolicyClient{version: "0.21.0-beta"}))
	assert.Equal(t, "0", q.settings["AF-Enabled"])
	assert.Empty(t, q.autoFeesUp)
}

func TestAutoFees_AppliesOutboundChange(t *testing.T) {
	// Mid-range channel with no forwards: idle-decrease (-2). local_fee_rate=203
	// (not a multiple of increment 5): new_rate = round((203-2)/5)*5 = 200,
	// adjustment = 200-203 = -3. eligible (stale fees_updated).
	old := time.Now().Add(-72 * time.Hour)
	q := &fakeAfQ{
		settings: map[string]string{"AF-Enabled": "1", "AF-CurveMode": "0"},
		channels: []db.GuiChannel{{
			ChanID: "c1", RemotePubkey: "peerA", IsOpen: true, IsActive: true, AutoFees: true,
			Capacity: 1000000, LocalBalance: 500000, RemoteBalance: 500000,
			LocalFeeRate: 203, LocalInboundFeeRate: 0, LocalBaseFee: 1000, LocalCltv: 40,
			ArInTarget: 30, ArMaxCost: 50, Alias: "peerA",
			FeesUpdated: pgtype.Timestamptz{Time: old, Valid: true}, FundingTxid: "abc",
		}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, AutoFees(context.Background(), q, client))

	require.Len(t, client.policyReqs, 1)
	assert.InDelta(t, 200.0/1000000, client.policyReqs[0].FeeRate, 1e-12)
	require.Len(t, q.autofees, 1)
	assert.Equal(t, int32(203), q.autofees[0].OldValue)
	assert.Equal(t, int32(200), q.autofees[0].NewValue)
	assert.Equal(t, "AF [ 0.0:50:50 ]", q.autofees[0].Setting)
	require.Len(t, q.autoFeesUp, 1)
	assert.Equal(t, int32(200), q.autoFeesUp[0].LocalFeeRate)
}

// A fee changed manually after af.Main took its snapshot must not be overwritten
// with a rate computed from the stale snapshot.
func TestAutoFees_SkipsManualChange(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	listed := db.GuiChannel{
		ChanID: "c1", RemotePubkey: "peerA", IsOpen: true, IsActive: true, AutoFees: true,
		Capacity: 1000000, LocalBalance: 500000, RemoteBalance: 500000,
		LocalFeeRate: 203, LocalBaseFee: 1000, LocalCltv: 40, ArInTarget: 30, ArMaxCost: 50,
		Alias: "peerA", FeesUpdated: pgtype.Timestamptz{Time: old, Valid: true}, FundingTxid: "abc",
	}
	for name, change := range map[string]func(*db.GuiChannel){
		"outbound": func(c *db.GuiChannel) { c.LocalFeeRate = 300 },
		"inbound":  func(c *db.GuiChannel) { c.LocalInboundFeeRate = -50 },
	} {
		t.Run(name, func(t *testing.T) {
			fresh := listed
			change(&fresh)
			q := &fakeAfQ{
				settings: map[string]string{"AF-Enabled": "1", "AF-CurveMode": "0"},
				channels: []db.GuiChannel{listed},
				fresh:    map[string]db.GuiChannel{"c1": fresh},
			}
			client := &fakePolicyClient{version: "0.21.0-beta"}
			require.NoError(t, AutoFees(context.Background(), q, client))
			assert.Empty(t, client.policyReqs, "no policy pushed to LND")
			assert.Empty(t, q.autofees)
			assert.Empty(t, q.autoFeesUp)
		})
	}
}
