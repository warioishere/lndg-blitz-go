package jobs

import (
	"context"
	"fmt"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// emergencyQuerier is the DB subset required by EmergencyFeeJob.
type emergencyQuerier interface {
	settingsQuerier
	ListEpEnabledChannels(ctx context.Context) ([]db.GuiChannel, error)
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
	UpdateChannelEmergencyFee(ctx context.Context, arg db.UpdateChannelEmergencyFeeParams) error
}

// EmergencyFeeJob raises the fee rate on channels whose outbound liquidity falls
// below the configured emergency threshold, subject to a cooldown period.
func EmergencyFeeJob(ctx context.Context, q emergencyQuerier, client policyClient) error {
	enabled, err := settingGateEnabled(ctx, q, "EP-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	channels, err := q.ListEpEnabledChannels(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, ch := range channels {
		target := int(ch.EpTarget)
		incPct := ch.EpIncPct
		cooldown := int(ch.EpCooldown)

		var percent float64
		if ch.Capacity != 0 {
			percent = float64(ch.LocalBalance+ch.PendingOutbound) * 100 / float64(ch.Capacity)
		}
		if percent >= float64(target) {
			continue
		}
		if ch.EpUpdated.Valid && now.Sub(ch.EpUpdated.Time).Seconds() < float64(cooldown*60) {
			continue
		}
		newRate := int(float64(ch.LocalFeeRate) * (1 + incPct/100)) // int() trunkiert

		// Apply the fee update; log any error without aborting the loop.
		if applyErr := func() error {
			cp := channelPoint(ch.FundingTxid, ch.OutputIndex)
			if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
				BaseFeeMsat:   int64(ch.LocalBaseFee),
				FeeRate:       float64(newRate) / 1000000,
				TimeLockDelta: uint32(ch.LocalCltv),
			}); e != nil {
				return e
			}
			if e := q.InsertAutofee(ctx, db.InsertAutofeeParams{
				Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: ch.Alias,
				Setting: "EP", OldValue: ch.LocalFeeRate, NewValue: int32(newRate),
			}); e != nil {
				return e
			}
			return q.UpdateChannelEmergencyFee(ctx, db.UpdateChannelEmergencyFeeParams{
				ChanID: ch.ChanID, LocalFeeRate: int32(newRate),
				FeesUpdated: ts(now), EpUpdated: ts(now),
			})
		}(); applyErr != nil {
			dataLog(fmt.Sprintf("Error updating emergency fee for %s: %s", ch.ChanID, applyErr))
		}
	}
	return nil
}
