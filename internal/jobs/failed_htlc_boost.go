package jobs

import (
	"context"
	"fmt"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// boostQuerier is the DB subset required by FailedHtlcBoostJob.
type boostQuerier interface {
	settingsQuerier
	ListOpenChannels(ctx context.Context) ([]db.GuiChannel, error)
	CountFailedHTLCBoost(ctx context.Context, arg db.CountFailedHTLCBoostParams) (int64, error)
	SetChannelHtlcBoostChecked(ctx context.Context, arg db.SetChannelHtlcBoostCheckedParams) error
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
	UpdateChannelFeeRate(ctx context.Context, arg db.UpdateChannelFeeRateParams) error
}

// FailedHtlcBoostJob raises the fee rate on low-liquidity channels that have
// seen too many failed HTLCs within the configured interval.
func FailedHtlcBoostJob(ctx context.Context, q boostQuerier, client policyClient) error {
	boostInterval, err := getOptionalInt(ctx, q, "AF-HTLCBoostIntvl", 15)
	if err != nil {
		return err
	}
	boostThreshold, err := getOptionalInt(ctx, q, "AF-FailedHTLCs", 5)
	if err != nil {
		return err
	}
	boostAmount, err := getOptionalInt(ctx, q, "AF-FailedHTLCBoost", 0)
	if err != nil {
		return err
	}
	lowliqLimit, err := getOptionalInt(ctx, q, "AF-LowLiqLimit", 5)
	if err != nil {
		return err
	}
	curveMode, err := settingEqualsNoCreate(ctx, q, "AF-CurveMode", "1")
	if err != nil {
		return err
	}
	if boostAmount <= 0 {
		return nil
	}

	channels, err := q.ListOpenChannels(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, ch := range channels {
		var localPercent float64
		if ch.Capacity != 0 {
			localPercent = float64(ch.LocalBalance+ch.PendingOutbound) * 100 / float64(ch.Capacity)
		}
		liqThreshold := lowliqLimit
		if curveMode {
			liqThreshold = 100 - int(ch.ArInTarget)
		}
		if localPercent > float64(liqThreshold) {
			continue
		}
		thresholdTime := now.Add(-time.Duration(boostInterval) * time.Minute)
		if ch.HtlcBoostChecked.Valid && ch.HtlcBoostChecked.Time.After(thresholdTime) {
			continue
		}
		filterBoost := now.Add(-time.Duration(boostInterval) * time.Minute)
		count, cerr := q.CountFailedHTLCBoost(ctx, db.CountFailedHTLCBoostParams{
			ChanIDOut: ch.ChanID, Timestamp: ts(filterBoost),
		})
		if cerr != nil {
			return cerr
		}
		// Mark the channel as checked.
		if e := q.SetChannelHtlcBoostChecked(ctx, db.SetChannelHtlcBoostCheckedParams{
			ChanID: ch.ChanID, HtlcBoostChecked: ts(now),
		}); e != nil {
			return e
		}
		if int(count) < boostThreshold {
			continue
		}
		newRate := int(ch.LocalFeeRate) + boostAmount
		inboundBaseFee := int32(0)
		if ch.LocalInboundBaseFee != 0 {
			inboundBaseFee = ch.LocalInboundBaseFee
		}
		feeRatePpm := int32(0)
		if ch.LocalInboundFeeRate != 0 {
			feeRatePpm = ch.LocalInboundFeeRate
		}
		chRef := ch
		if applyErr := func() error {
			cp := channelPoint(chRef.FundingTxid, chRef.OutputIndex)
			if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
				BaseFeeMsat:   int64(chRef.LocalBaseFee),
				FeeRate:       float64(newRate) / 1000000,
				TimeLockDelta: uint32(chRef.LocalCltv),
				InboundFee:    &lnrpc.InboundFee{BaseFeeMsat: inboundBaseFee, FeeRatePpm: feeRatePpm},
			}); e != nil {
				return e
			}
			dataLog(fmt.Sprintf("Applied HTLC boost to %s: %d HTLCs >= %d threshold, +%d ppm", chRef.ChanID, count, boostThreshold, boostAmount))
			if e := q.InsertAutofee(ctx, db.InsertAutofeeParams{
				Timestamp: ts(now), ChanID: chRef.ChanID, PeerAlias: chRef.Alias,
				Setting: "HTLC Boost Job", OldValue: chRef.LocalFeeRate, NewValue: int32(newRate),
			}); e != nil {
				return e
			}
			return q.UpdateChannelFeeRate(ctx, db.UpdateChannelFeeRateParams{
				ChanID: chRef.ChanID, LocalFeeRate: int32(newRate), FeesUpdated: ts(now),
			})
		}(); applyErr != nil {
			dataLog(fmt.Sprintf("Error applying HTLC boost to %s: %s", chRef.ChanID, applyErr))
		}
	}
	return nil
}
