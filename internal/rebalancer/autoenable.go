package rebalancer

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// autoEnableQuerier bundles the DB access required by autoEnable.
type autoEnableQuerier interface {
	settingsQuerier
	ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error)
	AggForwardsInSince(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.AggForwardsInSinceRow, error)
	AggForwardsOutSince(ctx context.Context, forwardDate pgtype.Timestamptz) ([]db.AggForwardsOutSinceRow, error)
	SetChannelAutoRebalance(ctx context.Context, arg db.SetChannelAutoRebalanceParams) error
	InsertAutopilot(ctx context.Context, arg db.InsertAutopilotParams) error
}

// autoEnable is the autopilot: it enables or disables auto_rebalance per channel
// based on the forward balance over the last apdays days.
// Errors are logged and swallowed.
func autoEnable(ctx context.Context, q autoEnableQuerier, now func() time.Time) {
	enabled, err := getLocalSettingInt(ctx, q, "AR-Autopilot", 0)
	if err != nil {
		rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
		return
	}
	apdays, err := getLocalSettingInt(ctx, q, "AR-APDays", 7)
	if err != nil {
		rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
		return
	}
	if enabled != 1 {
		return
	}
	channels, err := q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
		return
	}
	filterDay := pgtype.Timestamptz{Time: now().AddDate(0, 0, -apdays), Valid: true}
	aggIn, err := q.AggForwardsInSince(ctx, filterDay)
	if err != nil {
		rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
		return
	}
	aggOut, err := q.AggForwardsOutSince(ctx, filterDay)
	if err != nil {
		rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
		return
	}
	type agg struct {
		cnt int64
		sum int64
	}
	inMap := make(map[string]agg, len(aggIn))
	for _, r := range aggIn {
		inMap[r.ChanIDIn] = agg{cnt: r.Cnt, sum: r.SumMsat}
	}
	outMap := make(map[string]agg, len(aggOut))
	for _, r := range aggOut {
		outMap[r.ChanIDOut] = agg{cnt: r.Cnt, sum: r.SumMsat}
	}

	// Group channels by remote_pubkey.
	var peerOrder []string
	peerChans := make(map[string][]db.GuiChannel)
	for _, c := range channels {
		if _, ok := peerChans[c.RemotePubkey]; !ok {
			peerOrder = append(peerOrder, c.RemotePubkey)
		}
		peerChans[c.RemotePubkey] = append(peerChans[c.RemotePubkey], c)
	}
	sort.Strings(peerOrder) // deterministic order

	for _, peer := range peerOrder {
		list := peerChans[peer]
		var sumLb, sumPo, sumCap, sumRb, sumPi int64
		for _, c := range list {
			sumLb += c.LocalBalance
			sumPo += c.PendingOutbound
			sumCap += c.Capacity
			sumRb += c.RemoteBalance
			sumPi += c.PendingInbound
		}
		var outboundPercentRaw, inboundPercentRaw int64
		if sumCap != 0 {
			outboundPercentRaw = (sumLb + sumPo) * 1000 / sumCap
			inboundPercentRaw = (sumRb + sumPi) * 1000 / sumCap
		}
		outboundPercent := int(math.RoundToEven(float64(outboundPercentRaw) / 10))
		inboundPercent := int(math.RoundToEven(float64(inboundPercentRaw) / 10))

		var routedIn, routedOut, totalInMsat, totalOutMsat int64
		for _, c := range list {
			if a, ok := inMap[c.ChanID]; ok {
				routedIn += a.cnt
				totalInMsat += a.sum
			}
			if a, ok := outMap[c.ChanID]; ok {
				routedOut += a.cnt
				totalOutMsat += a.sum
			}
		}
		iapD := 0.0
		if routedIn != 0 {
			iapD = float64(int64(float64(totalInMsat)/10000000)) / 100
		}
		oapD := 0.0
		if routedOut != 0 {
			oapD = float64(int64(float64(totalOutMsat)/10000000)) / 100
		}

		for _, ch := range list {
			switch {
			case ch.ArOutTarget == 100 && ch.AutoRebalance:
				rebalLog(fmt.Sprintf("Skipping AR enabled and 100%% oTarget channel: %s %s", ch.Alias, ch.ChanID))
			case oapD > iapD*1.10 && outboundPercent > 75:
				// Case 1: Pass
			case oapD > iapD*1.10 && inboundPercent > 75 && !ch.AutoRebalance:
				// Case 2: Enable AR
				if err := q.SetChannelAutoRebalance(ctx, db.SetChannelAutoRebalanceParams{ChanID: ch.ChanID, AutoRebalance: true}); err != nil {
					rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
					return
				}
				if err := q.InsertAutopilot(ctx, db.InsertAutopilotParams{
					Timestamp: tsNow(now), ChanID: ch.ChanID, PeerAlias: ch.Alias, Setting: "Enabled", OldValue: 0, NewValue: 1,
				}); err != nil {
					rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
					return
				}
				rebalLog(fmt.Sprintf("Auto Pilot Enabled for %s %s: %s %s", ch.Alias, ch.ChanID, pyFloatStr(oapD), pyFloatStr(iapD)))
			case oapD < iapD*1.10 && outboundPercent > 75 && ch.AutoRebalance:
				// Case 3: Disable AR
				if err := q.SetChannelAutoRebalance(ctx, db.SetChannelAutoRebalanceParams{ChanID: ch.ChanID, AutoRebalance: false}); err != nil {
					rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
					return
				}
				if err := q.InsertAutopilot(ctx, db.InsertAutopilotParams{
					Timestamp: tsNow(now), ChanID: ch.ChanID, PeerAlias: ch.Alias, Setting: "Enabled", OldValue: 1, NewValue: 0,
				}); err != nil {
					rebalLog(fmt.Sprintf("Error during auto channel enabling: %s", err))
					return
				}
				rebalLog(fmt.Sprintf("Auto Pilot Disabled for %s %s: %s %s", ch.Alias, ch.ChanID, pyFloatStr(oapD), pyFloatStr(iapD)))
			case oapD < iapD*1.10 && inboundPercent > 75:
				// Case 4: Pass
			default:
				// Case 5: Pass
			}
		}
	}
}
