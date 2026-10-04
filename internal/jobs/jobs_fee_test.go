package jobs

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// --- Fakes ---

type fakeFeeQ struct {
	settings     map[string]string
	channels     []db.GuiChannel
	autofees     []db.InsertAutofeeParams
	emergencyUp  []db.UpdateChannelEmergencyFeeParams
	maxhtlcUp    []db.UpdateChannelMaxHtlcParams
	inboundLogs  []db.InsertInboundFeeLogParams
	inboundUp    []db.UpdateChannelInboundOffsetParams
	htlcChecked  []db.SetChannelHtlcBoostCheckedParams
	feeRateUp    []db.UpdateChannelFeeRateParams
	failedCounts map[string]int64
}

func (f *fakeFeeQ) ListInboundOffsetChannels(ctx context.Context) ([]db.GuiChannel, error) {
	var out []db.GuiChannel
	for _, c := range f.channels {
		if c.IsOpen && c.InboundOffset != 0 {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeFeeQ) InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error {
	f.inboundLogs = append(f.inboundLogs, arg)
	return nil
}
func (f *fakeFeeQ) UpdateChannelInboundOffset(ctx context.Context, arg db.UpdateChannelInboundOffsetParams) error {
	f.inboundUp = append(f.inboundUp, arg)
	return nil
}
func (f *fakeFeeQ) CountFailedHTLCBoost(ctx context.Context, arg db.CountFailedHTLCBoostParams) (int64, error) {
	return f.failedCounts[arg.ChanIDOut], nil
}
func (f *fakeFeeQ) SetChannelHtlcBoostChecked(ctx context.Context, arg db.SetChannelHtlcBoostCheckedParams) error {
	f.htlcChecked = append(f.htlcChecked, arg)
	return nil
}
func (f *fakeFeeQ) UpdateChannelFeeRate(ctx context.Context, arg db.UpdateChannelFeeRateParams) error {
	f.feeRateUp = append(f.feeRateUp, arg)
	return nil
}

func (f *fakeFeeQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}
func (f *fakeFeeQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.settings[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	if f.settings == nil {
		f.settings = map[string]string{}
	}
	f.settings[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}
func (f *fakeFeeQ) ListEpEnabledChannels(ctx context.Context) ([]db.GuiChannel, error) {
	var out []db.GuiChannel
	for _, c := range f.channels {
		if c.IsOpen && c.EpEnabled {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeFeeQ) ListOpenChannels(ctx context.Context) ([]db.GuiChannel, error) {
	var out []db.GuiChannel
	for _, c := range f.channels {
		if c.IsOpen {
			out = append(out, c)
		}
	}
	return out, nil
}
func (f *fakeFeeQ) GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error) {
	for _, c := range f.channels {
		if c.ChanID == chanID {
			return c, nil
		}
	}
	return db.GuiChannel{}, pgx.ErrNoRows
}
func (f *fakeFeeQ) InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error {
	f.autofees = append(f.autofees, arg)
	return nil
}
func (f *fakeFeeQ) UpdateChannelEmergencyFee(ctx context.Context, arg db.UpdateChannelEmergencyFeeParams) error {
	f.emergencyUp = append(f.emergencyUp, arg)
	return nil
}
func (f *fakeFeeQ) UpdateChannelMaxHtlc(ctx context.Context, arg db.UpdateChannelMaxHtlcParams) error {
	f.maxhtlcUp = append(f.maxhtlcUp, arg)
	return nil
}

type fakePolicyClient struct {
	version    string
	policyReqs []*lnrpc.PolicyUpdateRequest
	policyErr  error
}

func (c *fakePolicyClient) GetInfo(ctx context.Context, in *lnrpc.GetInfoRequest, opts ...grpc.CallOption) (*lnrpc.GetInfoResponse, error) {
	return &lnrpc.GetInfoResponse{Version: c.version}, nil
}
func (c *fakePolicyClient) UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error) {
	if c.policyErr != nil {
		return nil, c.policyErr
	}
	c.policyReqs = append(c.policyReqs, in)
	return &lnrpc.PolicyUpdateResponse{}, nil
}

// --- Tests ---

func TestEmergencyFeeJob_Disabled(t *testing.T) {
	q := &fakeFeeQ{settings: map[string]string{}}
	require.NoError(t, EmergencyFeeJob(context.Background(), q, &fakePolicyClient{version: "0.21.0-beta"}))
	assert.Equal(t, "0", q.settings["EP-Enabled"], "gate creates default")
}

func TestEmergencyFeeJob_AppliesBoost(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{
			"EP-Enabled": "1", "EP-DefaultTarget": "10", "EP-IncreasePct": "5", "EP-Cooldown": "60",
		},
		channels: []db.GuiChannel{
			{ // below target (5% < 10) -> boost
				ChanID: "c1", IsOpen: true, EpEnabled: true, Capacity: 1000000,
				LocalBalance: 50000, PendingOutbound: 0, EpTarget: 10, EpIncPct: 10, EpCooldown: 60,
				LocalFeeRate: 200, LocalBaseFee: 1000, LocalCltv: 40, Alias: "peer1",
				FundingTxid: "abc", OutputIndex: 0,
			},
			{ // above target (90% > 10) -> skip
				ChanID: "c2", IsOpen: true, EpEnabled: true, Capacity: 1000000,
				LocalBalance: 900000, EpTarget: 10, EpIncPct: 10, EpCooldown: 60, LocalFeeRate: 200,
			},
		},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, EmergencyFeeJob(context.Background(), q, client))

	require.Len(t, client.policyReqs, 1, "only c1 boosted")
	require.Len(t, q.emergencyUp, 1)
	// new_rate = int(200 * (1 + 10/100)) = 220
	assert.Equal(t, int32(220), q.emergencyUp[0].LocalFeeRate)
	assert.Equal(t, "c1", q.emergencyUp[0].ChanID)
	require.Len(t, q.autofees, 1)
	assert.Equal(t, "EP", q.autofees[0].Setting)
	assert.Equal(t, int32(200), q.autofees[0].OldValue)
	assert.Equal(t, int32(220), q.autofees[0].NewValue)
}

// Only EP-Enabled is needed: the per-channel ep_* columns drive the job, so a
// missing global EP-* default key must not stop it (or the data loop after it).
func TestEmergencyFeeJob_MissingDefaultKeys(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"EP-Enabled": "1"},
		channels: []db.GuiChannel{{
			ChanID: "c1", IsOpen: true, EpEnabled: true, Capacity: 1000000, LocalBalance: 50000,
			EpTarget: 10, EpIncPct: 10, EpCooldown: 60, LocalFeeRate: 200, FundingTxid: "abc",
		}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, EmergencyFeeJob(context.Background(), q, client))
	require.Len(t, q.emergencyUp, 1)
	assert.Equal(t, int32(220), q.emergencyUp[0].LocalFeeRate)
}

func TestEmergencyFeeJob_RPCErrorLogged(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"EP-Enabled": "1", "EP-DefaultTarget": "10", "EP-IncreasePct": "5", "EP-Cooldown": "60"},
		channels: []db.GuiChannel{{ChanID: "c1", IsOpen: true, EpEnabled: true, Capacity: 1000000, LocalBalance: 10000, EpTarget: 10, EpIncPct: 10, EpCooldown: 60, LocalFeeRate: 200}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta", policyErr: errors.New("rpc down")}
	require.NoError(t, EmergencyFeeJob(context.Background(), q, client))
	assert.Empty(t, q.emergencyUp, "no DB update when RPC fails")
	assert.Empty(t, q.autofees)
}

func TestAutoMaxhtlcJob_LiqUpper(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"MX-Enabled": "1"},
		channels: []db.GuiChannel{{
			ChanID: "c1", IsOpen: true, Capacity: 1000000, LocalBalance: 100000, PendingOutbound: 0,
			MxLiqUpper: 500000, MxLiqValue: 250000, LocalMaxHtlcMsat: 0,
			MaxhtlcUpdated: pgtype.Timestamptz{Valid: false},
			LocalFeeRate:   200, LocalBaseFee: 1000, LocalCltv: 40, FundingTxid: "abc",
		}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, AutoMaxhtlcJob(context.Background(), q, client))
	require.Len(t, client.policyReqs, 1)
	// expected = mx_liq_value * 1000 = 250000000
	assert.Equal(t, uint64(250000000), client.policyReqs[0].MaxHtlcMsat)
	require.Len(t, q.maxhtlcUp, 1)
	assert.Equal(t, int64(250000000), q.maxhtlcUp[0].LocalMaxHtlcMsat)
}

func TestAutoMaxhtlcJob_PercentPath(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"MX-Enabled": "1", "MX-Percent": "20"},
		channels: []db.GuiChannel{{
			ChanID: "c1", IsOpen: true, Capacity: 1000000, LocalBalance: 600000, PendingOutbound: 0,
			MxLiqUpper: 0, MxLiqThreshold: 0, MaxhtlcPercent: 0, LocalMaxHtlcMsat: 0,
			LocalFeeRate: 200, FundingTxid: "abc",
		}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, AutoMaxhtlcJob(context.Background(), q, client))
	require.Len(t, client.policyReqs, 1)
	// outbound=600000, percent=20 -> int(600000*80/100)*1000 = 480000*1000
	assert.Equal(t, uint64(480000000), client.policyReqs[0].MaxHtlcMsat)
}

func TestAutoMaxhtlcJob_Disabled(t *testing.T) {
	q := &fakeFeeQ{settings: map[string]string{}}
	require.NoError(t, AutoMaxhtlcJob(context.Background(), q, &fakePolicyClient{}))
	assert.Empty(t, q.maxhtlcUp)
	assert.Equal(t, "0", q.settings["MX-Enabled"])
}

func TestInboundOffsets_AppliesTarget(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"IO-Enabled": "1"},
		channels: []db.GuiChannel{{
			ChanID: "c1", IsOpen: true, InboundOffset: -10, LocalFeeRate: 200,
			LocalInboundFeeRate: 0, LocalBaseFee: 1000, LocalCltv: 40, FundingTxid: "abc",
			Alias: "peer1",
		}},
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, InboundOffsets(context.Background(), q, client))
	require.Len(t, client.policyReqs, 1)
	// balance = 200 + (-10) = 190 > 0 -> target = -190
	require.NotNil(t, client.policyReqs[0].InboundFee)
	assert.Equal(t, int32(-190), client.policyReqs[0].InboundFee.FeeRatePpm)
	require.Len(t, q.inboundUp, 1)
	assert.Equal(t, int32(-190), q.inboundUp[0].LocalInboundFeeRate)
	require.Len(t, q.inboundLogs, 1) // 0 != -190 -> logged
	assert.Equal(t, "Offset Job", q.inboundLogs[0].Setting)
}

// Auto-fees owns the inbound fee of auto_fees channels while AF-Enabled and
// AF-InboundFees are on, in legacy as well as curve mode.
func TestInboundOffsets_LeavesAutoFeesChannels(t *testing.T) {
	for _, tc := range []struct {
		name     string
		settings map[string]string
		applied  bool
	}{
		{"AF inbound on, legacy", map[string]string{"AF-Enabled": "1", "AF-InboundFees": "1", "AF-CurveMode": "0"}, false},
		{"AF inbound on, curve", map[string]string{"AF-Enabled": "1", "AF-InboundFees": "1", "AF-CurveMode": "1"}, false},
		{"AF inbound off", map[string]string{"AF-Enabled": "1", "AF-InboundFees": "0"}, true},
		{"AF disabled", map[string]string{"AF-Enabled": "0", "AF-InboundFees": "1"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := map[string]string{"IO-Enabled": "1"}
			for k, v := range tc.settings {
				settings[k] = v
			}
			q := &fakeFeeQ{settings: settings, channels: []db.GuiChannel{{
				ChanID: "c1", IsOpen: true, AutoFees: true, InboundOffset: -10, LocalFeeRate: 200, FundingTxid: "abc",
			}}}
			client := &fakePolicyClient{version: "0.21.0-beta"}
			require.NoError(t, InboundOffsets(context.Background(), q, client))
			assert.Equal(t, tc.applied, len(client.policyReqs) == 1)
		})
	}
}

func TestInboundOffsets_Pre018Skips(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{"IO-Enabled": "1"},
		channels: []db.GuiChannel{{ChanID: "c1", IsOpen: true, InboundOffset: -10}},
	}
	client := &fakePolicyClient{version: "0.17.0-beta"}
	require.NoError(t, InboundOffsets(context.Background(), q, client))
	assert.Empty(t, client.policyReqs, "LND < 0.18 skips inbound offsets")
}

func TestFailedHtlcBoostJob_AppliesBoost(t *testing.T) {
	q := &fakeFeeQ{
		settings: map[string]string{
			"AF-FailedHTLCBoost": "50", "AF-FailedHTLCs": "3", "AF-HTLCBoostIntvl": "15", "AF-LowLiqLimit": "5",
		},
		channels: []db.GuiChannel{{
			ChanID: "c1", IsOpen: true, Capacity: 1000000, LocalBalance: 20000, // 2% < 5
			ArInTarget: 30, LocalFeeRate: 200, LocalBaseFee: 1000, LocalCltv: 40, FundingTxid: "abc", Alias: "p1",
			HtlcBoostChecked: pgtype.Timestamptz{Valid: false},
		}},
		failedCounts: map[string]int64{"c1": 4}, // >= threshold 3
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, FailedHtlcBoostJob(context.Background(), q, client))
	require.Len(t, q.htlcChecked, 1, "channel marked checked")
	require.Len(t, client.policyReqs, 1)
	require.Len(t, q.feeRateUp, 1)
	assert.Equal(t, int32(250), q.feeRateUp[0].LocalFeeRate) // 200 + 50
	require.Len(t, q.autofees, 1)
	assert.Equal(t, "HTLC Boost Job", q.autofees[0].Setting)
}

func TestFailedHtlcBoostJob_DisabledWhenAmountZero(t *testing.T) {
	q := &fakeFeeQ{settings: map[string]string{"AF-FailedHTLCBoost": "0"}, channels: []db.GuiChannel{{ChanID: "c1", IsOpen: true, Capacity: 1000}}}
	require.NoError(t, FailedHtlcBoostJob(context.Background(), q, &fakePolicyClient{}))
	assert.Empty(t, q.htlcChecked)
}

func TestFailedHtlcBoostJob_BelowThresholdMarksOnly(t *testing.T) {
	q := &fakeFeeQ{
		settings:     map[string]string{"AF-FailedHTLCBoost": "50", "AF-FailedHTLCs": "5", "AF-LowLiqLimit": "5"},
		channels:     []db.GuiChannel{{ChanID: "c1", IsOpen: true, Capacity: 1000000, LocalBalance: 20000, LocalFeeRate: 200, FundingTxid: "abc"}},
		failedCounts: map[string]int64{"c1": 2}, // < threshold 5
	}
	client := &fakePolicyClient{version: "0.21.0-beta"}
	require.NoError(t, FailedHtlcBoostJob(context.Background(), q, client))
	require.Len(t, q.htlcChecked, 1, "still marks checked")
	assert.Empty(t, client.policyReqs, "no boost below threshold")
	assert.Empty(t, q.feeRateUp)
}
