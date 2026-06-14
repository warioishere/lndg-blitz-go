package jobs

import (
	"context"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// applyChannelDefaults fills in missing auto-rebalancing and auto-fee fields on
// a channel record by reading the corresponding global settings. isNew should be
// true for freshly created channel records, where auto_fees has not yet been set.
func applyChannelDefaults(ctx context.Context, q settingsQuerier, ch *db.GuiChannel, isNew bool) error {
	if isNew {
		enabled, err := getOrCreateInt(ctx, q, "AF-Enabled", "0")
		if err != nil {
			return err
		}
		ch.AutoFees = enabled != 0
	}
	if ch.ArOutTarget == 0 { // not ch.ar_out_target
		v, err := getOrCreateInt(ctx, q, "AR-Outbound%", "75")
		if err != nil {
			return err
		}
		ch.ArOutTarget = int32(v)
	}
	if ch.ArInTarget == 0 {
		v, err := getOrCreateInt(ctx, q, "AR-Inbound%", "90")
		if err != nil {
			return err
		}
		ch.ArInTarget = int32(v)
	}
	if ch.ArAmtTarget == 0 {
		v, err := getOrCreateFloat(ctx, q, "AR-Target%", "3")
		if err != nil {
			return err
		}
		ch.ArAmtTarget = int64((v / 100) * float64(ch.Capacity)) // int() trunkiert
	}
	if ch.ArMaxCost == 0 {
		v, err := getOrCreateInt(ctx, q, "AR-MaxCost%", "65")
		if err != nil {
			return err
		}
		ch.ArMaxCost = int32(v)
	}
	return nil
}
