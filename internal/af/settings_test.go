package af

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

type fakeSettingsQ struct {
	store map[string]string
}

func newFakeSettingsQ(seed map[string]string) *fakeSettingsQ {
	store := map[string]string{}
	for k, v := range seed {
		store[k] = v
	}
	return &fakeSettingsQ{store: store}
}

func (f *fakeSettingsQ) GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error) {
	if v, ok := f.store[key]; ok {
		return db.GuiLocalsetting{Key: key, Value: v}, nil
	}
	return db.GuiLocalsetting{}, pgx.ErrNoRows
}

func (f *fakeSettingsQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.store[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	f.store[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}

func (f *fakeSettingsQ) CreateLocalSettingIfAbsent(ctx context.Context, arg db.CreateLocalSettingIfAbsentParams) error {
	if _, ok := f.store[arg.Key]; !ok {
		f.store[arg.Key] = arg.Value
	}
	return nil
}

func (f *fakeSettingsQ) AnyLocalSettingExists(ctx context.Context, keys []string) (bool, error) {
	for _, k := range keys {
		if _, ok := f.store[k]; ok {
			return true, nil
		}
	}
	return false, nil
}

func TestLoadSettings_FreshDefaultsCurveModeDefault(t *testing.T) {
	q := newFakeSettingsQ(nil)
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)

	// Defaults loaded correctly.
	assert.Equal(t, 2500, s.MaxRate)
	assert.Equal(t, 5, s.Multiplier)
	assert.Equal(t, 5, s.LowLiqLimit)
	assert.Equal(t, 95, s.ExcessLimit)
	assert.Equal(t, 24.0, s.UpdateHours)
	assert.Equal(t, 0.5, s.FlowWeight)

	// Fresh install (no legacy keys pre-existing) defaults to curve mode (AF-CurveMode='1').
	assert.True(t, s.CurveMode, "fresh install defaults to curve mode (bug fixed)")
	assert.Equal(t, "1", q.store["AF-CurveMode"])

	// GetOrCreate side effect: all keys are persisted with their defaults.
	assert.Equal(t, "2500", q.store["AF-MaxRate"])
	assert.Equal(t, "24.0", q.store["AF-UpdateHours"])
	assert.Equal(t, "0", q.store["FLP-Enabled"])
}

// When legacy keys have been customized beforehand (here AF-Multiplier), the
// auto-detect correctly selects legacy mode (AF-CurveMode='0').
func TestLoadSettings_LegacyCustomizedDetectsLegacy(t *testing.T) {
	q := newFakeSettingsQ(map[string]string{"AF-Multiplier": "5"})
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)
	assert.False(t, s.CurveMode, "pre-existing legacy customization -> legacy mode")
	assert.Equal(t, "0", q.store["AF-CurveMode"])
}

func TestLoadSettings_ExistingCurveModeOne(t *testing.T) {
	q := newFakeSettingsQ(map[string]string{"AF-CurveMode": "1"})
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)
	assert.True(t, s.CurveMode, "explicit AF-CurveMode=1 honored")
}

func TestLoadSettings_ThresholdValidation(t *testing.T) {
	q := newFakeSettingsQ(map[string]string{"AF-LowLiqLimit": "95", "AF-ExcessLimit": "5"})
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, 5, s.LowLiqLimit, "invalid thresholds reset to defaults")
	assert.Equal(t, 95, s.ExcessLimit)
}

func TestLoadSettings_ValueErrorFallsBackToDefault(t *testing.T) {
	q := newFakeSettingsQ(map[string]string{"AF-MaxRate": "not-a-number", "AF-Exponent": "bad"})
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, 2500, s.MaxRate, "unparseable int -> default")
	assert.Equal(t, 2.0, s.Exponent, "unparseable float -> default")
}

func TestLoadSettings_BoolSettings(t *testing.T) {
	q := newFakeSettingsQ(map[string]string{
		"FLP-Enabled":      "1",
		"AF-PeerRateCheck": "1",
		"AF-ExcessBoostOn": "1",
		"AF-LowLiqBoostAR": "1",
	})
	s, err := LoadSettings(context.Background(), q)
	require.NoError(t, err)
	assert.True(t, s.FlpEnabledGlobal)
	assert.True(t, s.PeerRateCheck)
	assert.True(t, s.ExcessBoostEnabled)
	assert.True(t, s.BoostArOnly)
}
