package jobs

import (
	"context"
	"fmt"
	"time"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// autoFeesQuerier combines af.Querier (required by af.Main) with the
// additional queries needed by the AutoFees job.
type autoFeesQuerier interface {
	af.Querier
	ListAutoFeesChannels(ctx context.Context) ([]db.GuiChannel, error)
	GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error)
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
	InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error
	UpdateChannelAutoFees(ctx context.Context, arg db.UpdateChannelAutoFeesParams) error
}

// AutoFees computes fee targets via af.Main and pushes eligible changes to LND and the DB.
// Only channels that are open, active, non-private, and have auto_fees enabled are processed.
func AutoFees(ctx context.Context, q autoFeesQuerier, client policyClient) error {
	enabled, err := settingGateEnabled(ctx, q, "AF-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	inbound, err := getOrCreateInt(ctx, q, "AF-InboundFees", "0")
	if err != nil {
		return err
	}
	inboundEnabled := inbound != 0

	// Any error in the body is logged rather than propagated.
	if e := autoFeesBody(ctx, q, client, inboundEnabled); e != nil {
		dataLog(fmt.Sprintf("Error processing auto_fees: %s", e))
	}
	return nil
}

func autoFeesBody(ctx context.Context, q autoFeesQuerier, client policyClient, inboundEnabled bool) error {
	now := time.Now()
	channels, err := q.ListAutoFeesChannels(ctx)
	if err != nil {
		return err
	}
	rows, err := af.Main(ctx, q, channels, now)
	if err != nil {
		return err
	}
	for _, row := range rows {
		// Only apply updates where the channel is eligible and at least one fee changed.
		if !row.Eligible {
			continue
		}
		if row.Adjustment == 0 && row.InboundAdjustment == 0 {
			continue
		}
		channel, gerr := q.GetChannel(ctx, row.ChanID)
		if gerr != nil {
			return gerr
		}
		// Skip if a manual fee change was made after the af snapshot was taken.
		if int(channel.LocalFeeRate) != row.LocalFeeRate {
			dataLog(fmt.Sprintf("Auto-fees skip outbound update on %s (%s): manual change detected (%d -> %d)",
				channel.ChanID, channel.Alias, row.LocalFeeRate, channel.LocalFeeRate))
			continue
		}
		if int(channel.LocalInboundFeeRate) != row.LocalInboundFeeRate {
			dataLog(fmt.Sprintf("Auto-fees skip update on %s (%s): manual inbound change detected (%d -> %d)",
				channel.ChanID, channel.Alias, row.LocalInboundFeeRate, channel.LocalInboundFeeRate))
			continue
		}

		cp := channelPoint(channel.FundingTxid, channel.OutputIndex)
		info, ierr := client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if ierr != nil {
			return ierr
		}
		settingStr := fmt.Sprintf("AF [ %s:%d:%d ]", pyFloat(row.NetRouted7day), row.InPercent, row.OutPercent)

		finalFeeRate := channel.LocalFeeRate
		finalInbound := channel.LocalInboundFeeRate

		if inboundEnabled && versionFloat(info.Version) >= 0.18 {
			inboundFeeRate := int(row.NewInboundRate) // int() trunkiert
			// if we are using a discount, then discount our base fee to mirror outbound.
			// a positive rate is a manual override, keep the base fee set alongside it.
			inboundBaseFee := int32(0)
			if inboundFeeRate < 0 {
				inboundBaseFee = -channel.LocalBaseFee
			} else if inboundFeeRate > 0 {
				inboundBaseFee = channel.LocalInboundBaseFee
			}
			if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
				BaseFeeMsat:   int64(channel.LocalBaseFee),
				FeeRate:       row.NewRate / 1000000,
				TimeLockDelta: uint32(channel.LocalCltv),
				InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: inboundBaseFee, FeeRatePpm: int32(inboundFeeRate)},
			}); e != nil {
				return e
			}
			if row.InboundAdjustment != 0 {
				dataLog(fmt.Sprintf("Updating inbound fees for channel %s to a value of: %s", row.ChanID, pyFloat(row.NewInboundRate)))
				finalInbound = int32(row.NewInboundRate)
				if e := q.InsertInboundFeeLog(ctx, db.InsertInboundFeeLogParams{
					Timestamp: ts(now), ChanID: channel.ChanID, PeerAlias: channel.Alias,
					Setting: settingStr, OldValue: int32(row.LocalInboundFeeRate), NewValue: int32(row.NewInboundRate),
				}); e != nil {
					return e
				}
			}
		} else {
			if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
				BaseFeeMsat:   int64(channel.LocalBaseFee),
				FeeRate:       row.NewRate / 1000000,
				TimeLockDelta: uint32(channel.LocalCltv),
			}); e != nil {
				return e
			}
		}

		if row.Adjustment != 0 {
			dataLog(fmt.Sprintf("Updating outbound fees for channel %s to a value of: %s", row.ChanID, pyFloat(row.NewRate)))
			finalFeeRate = int32(row.NewRate)
			if e := q.InsertAutofee(ctx, db.InsertAutofeeParams{
				Timestamp: ts(now), ChanID: channel.ChanID, PeerAlias: channel.Alias,
				Setting: settingStr, OldValue: int32(row.LocalFeeRate), NewValue: int32(row.NewRate),
			}); e != nil {
				return e
			}
		}

		// Persist the updated fee rates and timestamp to the channel record.
		if e := q.UpdateChannelAutoFees(ctx, db.UpdateChannelAutoFeesParams{
			ChanID: channel.ChanID, LocalFeeRate: finalFeeRate,
			LocalInboundFeeRate: finalInbound, FeesUpdated: ts(now),
		}); e != nil {
			return e
		}
	}
	return nil
}
