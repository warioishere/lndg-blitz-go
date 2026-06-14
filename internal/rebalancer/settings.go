package rebalancer

import (
	"context"
	"strconv"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// settingsQuerier is the LocalSettings subset used by get_or_create-style lookups.
type settingsQuerier interface {
	GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error)
}

// getLocalSettingStr returns the string value of a setting, creating it with the
// given default if absent. DB errors are propagated.
func getLocalSettingStr(ctx context.Context, q settingsQuerier, key, def string) (string, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: def})
	if err != nil {
		return "", err
	}
	return row.Value, nil
}

// getLocalSettingInt returns the integer value of a setting. Parse errors return
// the default. DB errors are propagated.
func getLocalSettingInt(ctx context.Context, q settingsQuerier, key string, def int) (int, error) {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: strconv.Itoa(def)})
	if err != nil {
		return 0, err
	}
	v, perr := strconv.Atoi(row.Value)
	if perr != nil {
		return def, nil
	}
	return v, nil
}

// collectRoutesDisabled returns true when the RR-CollectRoutes setting is '0'.
func collectRoutesDisabled(ctx context.Context, q settingsQuerier) (bool, error) {
	v, err := getLocalSettingStr(ctx, q, "RR-CollectRoutes", "1")
	if err != nil {
		return false, err
	}
	return v == "0", nil
}
