package jobs

import (
	"context"
	"fmt"
	"math"
	"time"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
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

// inboundMinChange is the smallest change of the computed inbound rate, as a
// share of the channel's discount cap, that is pushed to LND. Small liquidity
// moves do not each send a channel update into the network.
const inboundMinChange = 0.05

// InboundOffsets sets the inbound fee rate of each channel with dynamic inbound
// fees (inbound_offset != 0) to af.InboundDynamicRate: a discount that refills
// the channel up to its outbound target, capped by what the rebalancer may pay.
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

	exponent, err := getOrCreateFloat(ctx, q, "AF-Exponent", "2.0")
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
		target, discountCap, _ := af.InboundDynamicRate(af.InboundInput{
			Dynamic: ch.InboundOffset != 0, LocalFeeRate: int(ch.LocalFeeRate),
			ArMaxCost: int(ch.ArMaxCost), ArInTarget: int(ch.ArInTarget),
			Capacity: ch.Capacity, LocalBalance: ch.LocalBalance, PendingOutbound: ch.PendingOutbound,
		}, exponent)
		current := int(ch.LocalInboundFeeRate)
		if target == current || (target != 0 && math.Abs(float64(target-current)) < inboundMinChange*discountCap) {
			if e := q.UpdateChannelInboundOffset(ctx, db.UpdateChannelInboundOffsetParams{
				ChanID: ch.ChanID, LocalInboundFeeRate: ch.LocalInboundFeeRate, OffsetUpdated: ts(now),
			}); e != nil {
				return e
			}
			continue
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
		if e := q.InsertInboundFeeLog(ctx, db.InsertInboundFeeLogParams{
			Timestamp: ts(now), ChanID: ch.ChanID, PeerAlias: ch.Alias,
			Setting: "Offset Job", OldValue: ch.LocalInboundFeeRate, NewValue: int32(target),
		}); e != nil {
			return e
		}
		if e := q.UpdateChannelInboundOffset(ctx, db.UpdateChannelInboundOffsetParams{
			ChanID: ch.ChanID, LocalInboundFeeRate: int32(target), OffsetUpdated: ts(now),
		}); e != nil {
			return e
		}
	}
	return nil
}
