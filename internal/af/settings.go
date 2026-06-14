package af

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Fixed constants used by the AF algorithms.
const (
	maxNetFlow     = 3    // maximum net flow ratio used in flow-weighting
	highFlowFactor = 0.25 // scaling factor applied in high-flow branches
)

// Settings holds all AF and FLP configuration values loaded from LocalSettings.
type Settings struct {
	Lookback             int     // FLP-Lookback
	FlpEnabledGlobal     bool    // FLP-Enabled == '1'
	FlpSafetyGlobal      int     // FLP-Safety
	MaxRate              int     // AF-MaxRate
	MinRate              int     // AF-MinRate
	Increment            int     // AF-Increment
	Multiplier           int     // AF-Multiplier
	HtlcBoostInterval    int     // AF-HTLCBoostIntvl
	HtlcBoostThreshold   int     // AF-FailedHTLCs
	HtlcBoostAmount      int     // AF-FailedHTLCBoost
	UpdateHours          float64 // AF-UpdateHours
	LowLiqLimit          int     // AF-LowLiqLimit
	ExcessLimit          int     // AF-ExcessLimit
	LowLiqBoost          float64 // AF-LowLiqBoost
	BoostArOnly          bool    // AF-LowLiqBoostAR == '1'
	ExcessBoost          float64 // AF-ExcessBoost
	ExcessBoostEnabled   bool    // AF-ExcessBoostOn == '1'
	PeerRateCheck        bool    // AF-PeerRateCheck == '1'
	PeerRateLimit        int     // AF-PeerRateLimit
	BypassPeerRateOnHTLC bool    // AF-BypassPeerHTLC == '1' (DB error → false)
	FlowScale            float64 // AF-FlowScale
	MaxStep              int     // AF-MaxStep
	CurveMode            bool    // AF-CurveMode == '1'
	Intensity            int     // AF-Intensity
	Exponent             float64 // AF-Exponent
	FlowWeight           float64 // AF-FlowWeight
	DownScale            float64 // AF-DownScale
	InboundIntensity     int     // AF-InboundIntensity
}

// settingsQuerier is the narrow subset of the sqlc querier used by the settings layer.
type settingsQuerier interface {
	GetLocalSetting(ctx context.Context, key string) (db.GuiLocalsetting, error)
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
	CreateLocalSettingIfAbsent(ctx context.Context, arg db.CreateLocalSettingIfAbsentParams) error
	AnyLocalSettingExists(ctx context.Context, dollar1 []string) (bool, error)
}

// getOrCreateRaw fetches the current string value for key, creating it with
// defStr as the default if it does not exist yet.
func getOrCreateRaw(ctx context.Context, q settingsQuerier, key, defStr string) (string, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: defStr})
	if err != nil {
		return defStr, err
	}
	return row.Value, nil
}

// getInt fetches a LocalSetting as an integer, falling back to the default on
// parse error.
func getInt(ctx context.Context, q settingsQuerier, key, defStr string) (int, error) {
	v, err := getOrCreateRaw(ctx, q, key, defStr)
	if err != nil {
		return 0, err
	}
	if n, convErr := strconv.Atoi(v); convErr == nil {
		return n, nil
	}
	d, _ := strconv.Atoi(defStr)
	return d, nil
}

// getFloat fetches a LocalSetting as a float64, falling back to the default on
// parse error.
func getFloat(ctx context.Context, q settingsQuerier, key, defStr string) (float64, error) {
	v, err := getOrCreateRaw(ctx, q, key, defStr)
	if err != nil {
		return 0, err
	}
	if f, convErr := strconv.ParseFloat(v, 64); convErr == nil {
		return f, nil
	}
	d, _ := strconv.ParseFloat(defStr, 64)
	return d, nil
}

// getStr fetches a LocalSetting as a raw string.
func getStr(ctx context.Context, q settingsQuerier, key, defStr string) (string, error) {
	return getOrCreateRaw(ctx, q, key, defStr)
}

// LoadSettings reads all AF and FLP settings from LocalSettings.
// The read order is significant for the CurveMode auto-detect logic below:
// reading AF-CurveMode before the legacy keys ensures a fresh install defaults
// to curve mode rather than legacy mode.
func LoadSettings(ctx context.Context, q settingsQuerier) (*Settings, error) {
	s := &Settings{}
	var err error
	var str string

	if s.Lookback, err = getInt(ctx, q, "FLP-Lookback", "10"); err != nil {
		return nil, err
	}
	if str, err = getStr(ctx, q, "FLP-Enabled", "0"); err != nil {
		return nil, err
	}
	s.FlpEnabledGlobal = str == "1"
	if s.FlpSafetyGlobal, err = getInt(ctx, q, "FLP-Safety", "0"); err != nil {
		return nil, err
	}
	// CurveMode auto-detect: if AF-CurveMode does not exist yet but legacy keys
	// do, create AF-CurveMode = '0' to preserve the existing legacy configuration.
	// Otherwise the default of '1' (curve mode) takes effect on first read below.
	// This block runs before the legacy keys are read so that get-or-create for
	// those keys does not interfere with detection.
	if _, gErr := q.GetLocalSetting(ctx, "AF-CurveMode"); errors.Is(gErr, pgx.ErrNoRows) {
		legacyKeys := []string{
			"AF-Multiplier", "AF-FlowScale", "AF-MaxStep", "AF-LowLiqLimit",
			"AF-ExcessLimit", "AF-LowLiqBoost", "AF-LowLiqBoostAR", "AF-ExcessBoost",
			"AF-ExcessBoostOn",
		}
		exists, eErr := q.AnyLocalSettingExists(ctx, legacyKeys)
		if eErr != nil {
			return nil, eErr
		}
		if exists {
			if cErr := q.CreateLocalSettingIfAbsent(ctx, db.CreateLocalSettingIfAbsentParams{Key: "AF-CurveMode", Value: "0"}); cErr != nil {
				return nil, cErr
			}
		}
	} else if gErr != nil {
		return nil, gErr
	}
	if s.MaxRate, err = getInt(ctx, q, "AF-MaxRate", "2500"); err != nil {
		return nil, err
	}
	if s.MinRate, err = getInt(ctx, q, "AF-MinRate", "0"); err != nil {
		return nil, err
	}
	if s.Increment, err = getInt(ctx, q, "AF-Increment", "5"); err != nil {
		return nil, err
	}
	if s.Multiplier, err = getInt(ctx, q, "AF-Multiplier", "5"); err != nil {
		return nil, err
	}
	if s.HtlcBoostInterval, err = getInt(ctx, q, "AF-HTLCBoostIntvl", "15"); err != nil {
		return nil, err
	}
	if s.HtlcBoostThreshold, err = getInt(ctx, q, "AF-FailedHTLCs", "5"); err != nil {
		return nil, err
	}
	if s.HtlcBoostAmount, err = getInt(ctx, q, "AF-FailedHTLCBoost", "0"); err != nil {
		return nil, err
	}
	if s.UpdateHours, err = getFloat(ctx, q, "AF-UpdateHours", "24.0"); err != nil {
		return nil, err
	}
	if s.LowLiqLimit, err = getInt(ctx, q, "AF-LowLiqLimit", "5"); err != nil {
		return nil, err
	}
	if s.ExcessLimit, err = getInt(ctx, q, "AF-ExcessLimit", "95"); err != nil {
		return nil, err
	}
	if s.LowLiqBoost, err = getFloat(ctx, q, "AF-LowLiqBoost", "1.0"); err != nil {
		return nil, err
	}
	if str, err = getStr(ctx, q, "AF-LowLiqBoostAR", "0"); err != nil {
		return nil, err
	}
	s.BoostArOnly = str == "1"
	if s.ExcessBoost, err = getFloat(ctx, q, "AF-ExcessBoost", "1.0"); err != nil {
		return nil, err
	}
	if str, err = getStr(ctx, q, "AF-ExcessBoostOn", "0"); err != nil {
		return nil, err
	}
	s.ExcessBoostEnabled = str == "1"
	if str, err = getStr(ctx, q, "AF-PeerRateCheck", "0"); err != nil {
		return nil, err
	}
	s.PeerRateCheck = str == "1"
	if s.PeerRateLimit, err = getInt(ctx, q, "AF-PeerRateLimit", "0"); err != nil {
		return nil, err
	}
	// A DB error reading this setting is treated as false rather than propagated.
	if str, bypassErr := getStr(ctx, q, "AF-BypassPeerHTLC", "0"); bypassErr == nil {
		s.BypassPeerRateOnHTLC = str == "1"
	} else {
		s.BypassPeerRateOnHTLC = false
	}
	if s.FlowScale, err = getFloat(ctx, q, "AF-FlowScale", "1.0"); err != nil {
		return nil, err
	}
	if s.MaxStep, err = getInt(ctx, q, "AF-MaxStep", "100"); err != nil {
		return nil, err
	}

	if str, err = getStr(ctx, q, "AF-CurveMode", "1"); err != nil {
		return nil, err
	}
	s.CurveMode = str == "1"

	if s.Intensity, err = getInt(ctx, q, "AF-Intensity", "50"); err != nil {
		return nil, err
	}
	if s.Exponent, err = getFloat(ctx, q, "AF-Exponent", "2.0"); err != nil {
		return nil, err
	}
	if s.FlowWeight, err = getFloat(ctx, q, "AF-FlowWeight", "0.5"); err != nil {
		return nil, err
	}
	if s.DownScale, err = getFloat(ctx, q, "AF-DownScale", "1.0"); err != nil {
		return nil, err
	}
	if s.InboundIntensity, err = getInt(ctx, q, "AF-InboundIntensity", "20"); err != nil {
		return nil, err
	}

	// Validate liquidity thresholds; reset to safe defaults if they are nonsensical.
	if s.LowLiqLimit >= s.ExcessLimit {
		fmt.Println("Invalid thresholds detected, using defaults...")
		s.LowLiqLimit = 5
		s.ExcessLimit = 95
	}

	return s, nil
}
