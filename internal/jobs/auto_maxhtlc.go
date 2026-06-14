package jobs

import (
	"context"
	"fmt"
	"math"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// maxhtlcQuerier is the DB subset required by AutoMaxhtlcJob.
type maxhtlcQuerier interface {
	settingsQuerier
	ListOpenChannels(ctx context.Context) ([]db.GuiChannel, error)
	GetChannel(ctx context.Context, chanID string) (db.GuiChannel, error)
	UpdateChannelMaxHtlc(ctx context.Context, arg db.UpdateChannelMaxHtlcParams) error
}

// AutoMaxhtlcJob updates the max_htlc_msat policy on open channels according to
// configured liquidity thresholds and percent-of-outbound rules.
func AutoMaxhtlcJob(ctx context.Context, q maxhtlcQuerier, client policyClient) error {
	enabled, err := settingGateEnabled(ctx, q, "MX-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	updateHours, err := getOrCreateFloat(ctx, q, "MX-UpdateHours", "24")
	if err != nil {
		return err
	}
	globalPercent, err := getOrCreateInt(ctx, q, "MX-Percent", "0")
	if err != nil {
		return err
	}
	now := time.Now()
	threshold := now.Add(-time.Duration(updateHours * float64(time.Hour)))

	channels, err := q.ListOpenChannels(ctx)
	if err != nil {
		return err
	}
	for _, ch := range channels {
		outbound := ch.LocalBalance + ch.PendingOutbound
		var expectedMsat int64
		haveExpected := false
		switch {
		case ch.MxLiqUpper != 0 && outbound < ch.MxLiqUpper:
			expectedMsat = ch.MxLiqValue * 1000
			haveExpected = true
		case ch.MxLiqThreshold != 0 && outbound < ch.MxLiqThreshold:
			expectedMsat = ch.MxLiqValue * 1000
			haveExpected = true
		default:
			percent := globalPercent
			if ch.MaxhtlcPercent != 0 {
				percent = int(ch.MaxhtlcPercent)
			}
			if percent != 0 {
				// Truncate: int(outbound * (100 - percent) / 100) * 1000
				expectedMsat = int64(math.Trunc(float64(outbound*int64(100-percent))/100)) * 1000
				haveExpected = true
			}
		}
		if !haveExpected {
			continue
		}
		if ch.LocalMaxHtlcMsat == expectedMsat && ch.MaxhtlcUpdated.Valid && !ch.MaxhtlcUpdated.Time.Before(threshold) {
			continue
		}
		// Re-fetch the channel to get the latest policy values before issuing the RPC.
		fresh, ferr := q.GetChannel(ctx, ch.ChanID)
		if ferr != nil {
			return ferr
		}
		cp := channelPoint(fresh.FundingTxid, fresh.OutputIndex)
		if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
			Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
			BaseFeeMsat:   int64(fresh.LocalBaseFee),
			FeeRate:       float64(fresh.LocalFeeRate) / 1000000,
			TimeLockDelta: uint32(fresh.LocalCltv),
			MaxHtlcMsat:   uint64(expectedMsat),
		}); e != nil {
			dataLog(fmt.Sprintf("Error updating max htlc for %s: %s", ch.ChanID, e))
			continue
		}
		if e := q.UpdateChannelMaxHtlc(ctx, db.UpdateChannelMaxHtlcParams{
			ChanID: ch.ChanID, LocalMaxHtlcMsat: expectedMsat, MaxhtlcUpdated: ts(now),
		}); e != nil {
			return e
		}
	}
	return nil
}
