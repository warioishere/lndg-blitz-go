package jobs

import (
	"context"
	"fmt"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// inboundOffsetsQuerier is the DB subset required by InboundOffsets.
type inboundOffsetsQuerier interface {
	settingsQuerier
	ListInboundOffsetChannels(ctx context.Context) ([]db.GuiChannel, error)
	GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error)
	InsertInboundFeeLog(ctx context.Context, arg db.InsertInboundFeeLogParams) error
	UpdateChannelInboundOffset(ctx context.Context, arg db.UpdateChannelInboundOffsetParams) error
}

// InboundOffsets sets the inbound fee rate on each eligible channel so that
// the sum of local fee rate and inbound offset nets to zero (or stays negative).
func InboundOffsets(ctx context.Context, q inboundOffsetsQuerier, client policyClient) error {
	enabled, err := settingGateEnabled(ctx, q, "IO-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	updateHours, err := getOrCreateFloat(ctx, q, "IO-UpdateHours", "24")
	if err != nil {
		return err
	}
	now := time.Now()
	threshold := now.Add(-time.Duration(updateHours * float64(time.Hour)))

	info, err := client.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return err
	}
	if versionFloat(info.Version) < 0.18 {
		return nil
	}

	channels, err := q.ListInboundOffsetChannels(ctx)
	if err != nil {
		return err
	}
	// Auto-fees owns the inbound fee of its channels while it runs with inbound fees
	// on (curve and legacy mode alike), so leave those channels to it.
	afEnabled, err := settingEqualsNoCreate(ctx, q, "AF-Enabled", "1")
	if err != nil {
		return err
	}
	afInbound, err := settingEqualsNoCreate(ctx, q, "AF-InboundFees", "1")
	if err != nil {
		return err
	}

	for _, ch0 := range channels {
		if afEnabled && afInbound && ch0.AutoFees {
			continue
		}
		// Re-fetch the channel to get current values before updating.
		ch, gerr := q.GetChannel(ctx, ch0.ChanID)
		if gerr != nil {
			return gerr
		}
		// Skip channels updated within the configured interval.
		if ch.OffsetUpdated.Valid && !ch.OffsetUpdated.Time.Before(threshold) {
			continue
		}
		balance := int(ch.LocalFeeRate) + int(ch.InboundOffset)
		target := 0
		if balance > 0 {
			target = -balance
		}
		inboundBaseFee := int32(0)
		if ch.LocalInboundBaseFee != 0 {
			inboundBaseFee = ch.LocalInboundBaseFee
		}
		cp := channelPoint(ch.FundingTxid, ch.OutputIndex)
		if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
			BaseFeeMsat:   int64(ch.LocalBaseFee),
			FeeRate:       float64(ch.LocalFeeRate) / 1000000,
			TimeLockDelta: uint32(ch.LocalCltv),
			InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: inboundBaseFee, FeeRatePpm: int32(target)},
		}); e != nil {
			dataLog(fmt.Sprintf("Error updating inbound offset for %s: %s", ch.ChanID, e))
			continue
		}
		if int(ch.LocalInboundFeeRate) != target {
			if e := q.InsertInboundFeeLog(ctx, db.InsertInboundFeeLogParams{
				Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: ch.Alias,
				Setting: "Offset Job", OldValue: ch.LocalInboundFeeRate, NewValue: int32(target),
			}); e != nil {
				return e
			}
		}
		if e := q.UpdateChannelInboundOffset(ctx, db.UpdateChannelInboundOffsetParams{
			ChanID: ch.ChanID, LocalInboundFeeRate: int32(target), OffsetUpdated: ts(now),
		}); e != nil {
			return e
		}
	}
	return nil
}
