package jobs

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// emergencyForwardClient is the LND subset required by emergencyForwardCheck.
type emergencyForwardClient interface {
	ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error)
	UpdateChannelPolicy(ctx context.Context, in *lnrpc.PolicyUpdateRequest, opts ...grpc.CallOption) (*lnrpc.PolicyUpdateResponse, error)
}

// emergencyForwardQuerier is the DB subset required by emergencyForwardCheck.
type emergencyForwardQuerier interface {
	settingsQuerier
	ListEpEnabledChannelsByIDs(ctx context.Context, chanIDs []string) ([]db.GuiChannel, error)
	InsertAutofee(ctx context.Context, arg db.InsertAutofeeParams) error
	UpdateChannelEmergencyFee(ctx context.Context, arg db.UpdateChannelEmergencyFeeParams) error
}

// Exported type aliases so that other packages (e.g. htlc_stream) can call
// EmergencyForwardCheck without duplicating the querier and client interfaces.
type (
	EmergencyForwardQuerier = emergencyForwardQuerier
	EmergencyForwardClient  = emergencyForwardClient
)

// EmergencyForwardCheck is the exported entry point for emergencyForwardCheck.
func EmergencyForwardCheck(ctx context.Context, q EmergencyForwardQuerier, client EmergencyForwardClient, chanIDs []string) error {
	return emergencyForwardCheck(ctx, q, client, chanIDs)
}

// emergencyForwardCheck raises the fee rate on the given channels when their
// live outbound liquidity (local balance + pending outgoing HTLCs) falls below
// the configured live emergency threshold.
func emergencyForwardCheck(ctx context.Context, q emergencyForwardQuerier, client emergencyForwardClient, chanIDs []string) error {
	enabled, err := settingGateEnabled(ctx, q, "EP-Enabled")
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	channels, err := q.ListEpEnabledChannelsByIDs(ctx, chanIDs)
	if err != nil {
		return err
	}
	if len(channels) == 0 {
		return nil
	}

	chanSet := make(map[string]bool, len(chanIDs))
	for _, id := range chanIDs {
		chanSet[id] = true
	}
	resp, err := client.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		return err
	}
	liveMap := map[string]*lnrpc.Channel{}
	for _, ch := range resp.Channels {
		id := formatChanID(ch.ChanId)
		if chanSet[id] {
			liveMap[id] = ch
		}
	}

	now := time.Now()
	for _, dbCh := range channels {
		liveCh, ok := liveMap[dbCh.ChanID]
		if !ok {
			continue
		}
		// Same per-channel cooldown as EmergencyFeeJob, so a burst of forwards
		// cannot compound the increase.
		if dbCh.EpUpdated.Valid && now.Sub(dbCh.EpUpdated.Time).Seconds() < float64(dbCh.EpCooldown*60) {
			continue
		}
		threshold := int(dbCh.EpLiveThreshold)
		incPct := dbCh.EpLiveIncPct
		var pendingOut int64
		for _, h := range liveCh.PendingHtlcs {
			if !h.Incoming {
				pendingOut += h.Amount
			}
		}
		var percent float64
		if liveCh.Capacity != 0 {
			percent = float64(liveCh.LocalBalance+pendingOut) * 100 / float64(liveCh.Capacity)
		}
		if percent >= float64(threshold) {
			continue
		}
		newRate := int(float64(dbCh.LocalFeeRate) * (1 + incPct/100))
		cp := channelPoint(dbCh.FundingTxid, dbCh.OutputIndex)
		if applyErr := func() error {
			if _, e := client.UpdateChannelPolicy(ctx, &lnrpc.PolicyUpdateRequest{
				Scope:         &lnrpc.PolicyUpdateRequest_ChanPoint{ChanPoint: cp},
				BaseFeeMsat:   int64(dbCh.LocalBaseFee),
				FeeRate:       float64(newRate) / 1000000,
				TimeLockDelta: uint32(dbCh.LocalCltv),
			}); e != nil {
				return e
			}
			if e := q.InsertAutofee(ctx, db.InsertAutofeeParams{
				Timestamp: ts(now), ChanID: dbCh.ChanID, PeerAlias: dbCh.Alias,
				Setting: "EP-L", OldValue: dbCh.LocalFeeRate, NewValue: int32(newRate),
			}); e != nil {
				return e
			}
			return q.UpdateChannelEmergencyFee(ctx, db.UpdateChannelEmergencyFeeParams{
				ChanID: dbCh.ChanID, LocalFeeRate: int32(newRate), FeesUpdated: ts(now), EpUpdated: ts(now),
			})
		}(); applyErr != nil {
			dataLog(fmt.Sprintf("Error updating live emergency fee for %s: %s", dbCh.ChanID, applyErr))
		}
	}
	return nil
}
