package rebalancer

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// pyFloatStr formats a float for log strings: integers get one decimal place ("2.0"),
// other values are formatted compactly.
func pyFloatStr(v float64) string {
	if v == math.Trunc(v) && !math.IsInf(v, 0) && math.Abs(v) < 1e16 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// scheduleQuerier bundles the DB access required by autoSchedule.
type scheduleQuerier interface {
	settingsQuerier
	aliasQuerier
	ListActiveOpenPublicChannels(ctx context.Context) ([]db.GuiChannel, error)
	ListActiveRebalancePubkeys(ctx context.Context) ([]string, error)
	ListAllAllowedTargets(ctx context.Context) ([]db.ListAllAllowedTargetsRow, error)
	GetLastRebalanceForPubkey(ctx context.Context, lastHopPubkey string) (db.GetLastRebalanceForPubkeyRow, error)
	InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error)
}

// secretsChoiceRange returns a cryptographically random integer in [lo, hi].
func secretsChoiceRange(lo, hi int) int {
	n := int64(hi - lo + 1)
	v, err := rand.Int(rand.Reader, big.NewInt(n))
	if err != nil {
		return lo
	}
	return lo + int(v.Int64())
}

// scheduleBlocked returns true when the channel should be skipped due to the wait period.
func scheduleBlocked(last db.GetLastRebalanceForPubkeyRow, waitPeriod int, now time.Time) bool {
	allowed := false
	switch {
	case last.Status == 2:
		allowed = true
	case last.Status > 2 && last.Stop.Valid && int(now.Sub(last.Stop.Time).Seconds()/60) > waitPeriod:
		allowed = true
	case last.Status == 1 && last.Start.Valid && (int(now.Sub(last.Start.Time).Seconds()/60)-int(last.Duration)) > waitPeriod:
		allowed = true
	}
	return !allowed
}

// targetValue applies random variance to ar_amt_target.
func targetValue(arAmtTarget int64, variance int) int64 {
	choice := secretsChoiceRange(-1000, 1000)
	return int64(float64(arAmtTarget) + float64(arAmtTarget)*((float64(choice)/1000)*float64(variance)/100))
}

// autoSchedule creates new rebalance records for channels that need rebalancing.
// Returns the newly inserted records. On error it logs and returns the partial result.
func (e *engine) autoSchedule(ctx context.Context, q scheduleQuerier, now func() time.Time) []*db.GuiRebalancer {
	var toSchedule []*db.GuiRebalancer

	enabled, err := getLocalSettingInt(ctx, q, "AR-Enabled", 0)
	if err != nil {
		rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", err))
		return toSchedule
	}
	if enabled == 0 {
		return nil
	}

	channels, err := q.ListActiveOpenPublicChannels(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", err))
		return toSchedule
	}
	if len(channels) == 0 {
		return nil
	}
	annotated := annotateChannels(channels, 0, true) // percent_outbound without value subtraction

	// Touch defaults so settings are created if absent.
	_, _ = getLocalSettingInt(ctx, q, "AR-Outbound%", 75)
	_, _ = getLocalSettingInt(ctx, q, "AR-Inbound%", 90)

	activePubkeys, err := q.ListActiveRebalancePubkeys(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", err))
		return toSchedule
	}
	scheduledSet := make(map[string]struct{}, len(activePubkeys))
	for _, p := range activePubkeys {
		scheduledSet[p] = struct{}{}
	}

	// Outbound candidates (no last-hop exclusion), sorted by htlc_count.
	outFiltered := make([]annotatedChannel, 0, len(annotated))
	for _, a := range annotated {
		c := a.ch
		if a.percentOutbound < float64(c.ArOutTarget) || c.LocalDisabled {
			continue
		}
		if !c.ArSource && c.AutoRebalance {
			continue
		}
		outFiltered = append(outFiltered, a)
	}
	sort.SliceStable(outFiltered, func(i, j int) bool { return outFiltered[i].ch.HtlcCount < outFiltered[j].ch.HtlcCount })
	outboundCans := make([]string, 0, len(outFiltered))
	outboundSet := make(map[string]struct{}, len(outFiltered))
	chanToPeer := make(map[string]string, len(outFiltered))
	sourceFeeRateMap := make(map[string]int, len(outFiltered))
	for _, a := range outFiltered {
		outboundCans = append(outboundCans, a.ch.ChanID)
		outboundSet[a.ch.ChanID] = struct{}{}
		chanToPeer[a.ch.ChanID] = a.ch.RemotePubkey
		sourceFeeRateMap[a.ch.ChanID] = int(a.ch.LocalFeeRate)
	}

	// Inbound candidates: auto_rebalance, inbound_can >= 1, not remote_disabled, peer not already scheduled;
	// sorted by inbound_can descending.
	inboundList := make([]annotatedChannel, 0, len(annotated))
	for _, a := range annotated {
		c := a.ch
		if !c.AutoRebalance || a.inboundCan < 1 || c.RemoteDisabled {
			continue
		}
		if _, sched := scheduledSet[c.RemotePubkey]; sched {
			continue
		}
		inboundList = append(inboundList, a)
	}
	sort.SliceStable(inboundList, func(i, j int) bool { return inboundList[i].inboundCan > inboundList[j].inboundCan })

	if len(inboundList) == 0 || len(outboundCans) == 0 {
		return nil
	}

	maxFeeRate, _ := getLocalSettingInt(ctx, q, "AR-MaxFeeRate", 500)
	variance, _ := getLocalSettingInt(ctx, q, "AR-Variance", 0)
	waitPeriod, _ := getLocalSettingInt(ctx, q, "AR-WaitPeriod", 30)
	_, _ = getLocalSettingInt(ctx, q, "AR-Target%", 3)
	_, _ = getLocalSettingInt(ctx, q, "AR-MaxCost%", 65)

	minSourceFeeRate := 0
	first := true
	for _, v := range sourceFeeRateMap {
		if first || v < minSourceFeeRate {
			minSourceFeeRate = v
			first = false
		}
	}

	// allowed_map: source_chan_id -> [target_pubkeys], in insertion order.
	allowedRows, err := q.ListAllAllowedTargets(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", err))
		return toSchedule
	}
	var allowedOrder []string
	allowedMap := make(map[string][]string)
	for _, r := range allowedRows {
		if _, ok := allowedMap[r.SourceChanID]; !ok {
			allowedOrder = append(allowedOrder, r.SourceChanID)
		}
		allowedMap[r.SourceChanID] = append(allowedMap[r.SourceChanID], r.TargetPubkey)
	}

	scheduledTargets := make(map[string]struct{})

	// Loop 1: explicitly allowed (AllowedTarget) source/target pairs.
	for _, sourceID := range allowedOrder {
		if _, ok := outboundSet[sourceID]; !ok {
			continue
		}
		for _, pub := range allowedMap[sourceID] {
			if _, ok := scheduledSet[pub]; ok {
				continue
			}
			target, ok := firstInboundForPeer(inboundList, pub)
			if !ok {
				continue
			}
			sourceFeeRate := sourceFeeRateMap[sourceID]
			targetFeeRate := minInt(maxFeeRate, int(float64(target.ch.LocalFeeRate)*(float64(target.ch.ArMaxCost)/100))-sourceFeeRate)
			if targetFeeRate <= int(target.ch.RemoteFeeRate) {
				continue
			}
			tValue := targetValue(target.ch.ArAmtTarget, variance)
			tFee := pyround.Round(float64(targetFeeRate)*float64(tValue)*0.000001, 3)
			if tFee == 0 {
				continue
			}
			targetTime, _ := getLocalSettingInt(ctx, q, "AR-Time", 5)
			last, lerr := q.GetLastRebalanceForPubkey(ctx, pub)
			if lerr == nil {
				if scheduleBlocked(last, waitPeriod, now()) {
					continue
				}
			} else if !isNoRows(lerr) {
				rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", lerr))
				return toSchedule
			}
			rebalLog(fmt.Sprintf("Creating Auto Rebalance Request for allowed target %s via %s", pub, sourceID))
			rebalLog(fmt.Sprintf("Value: %d / %d | Fee: %s | Duration: %d", tValue, target.ch.ArAmtTarget, pyFloatStr(tFee), targetTime))
			_ = e.alias.ensure(ctx, q)
			rebalLog(fmt.Sprintf("Request routing outbound via: [%s]", e.alias.label(sourceID)))
			next := e.insertScheduledRebalance(ctx, q, int32(tValue), tFee, "["+sourceID+"]", pub, target.ch.Alias, int32(targetTime), now)
			toSchedule = append(toSchedule, next)
			scheduledTargets[pub] = struct{}{}
		}
	}

	// Loop 2: all remaining inbound targets using the cheapest source pool.
	for _, target := range inboundList {
		if _, ok := scheduledTargets[target.ch.RemotePubkey]; ok {
			continue
		}
		targetFeeRate := minInt(maxFeeRate, int(float64(target.ch.LocalFeeRate)*(float64(target.ch.ArMaxCost)/100))-minSourceFeeRate)
		if !(targetFeeRate > 0 && targetFeeRate > int(target.ch.RemoteFeeRate)) {
			continue
		}
		tValue := targetValue(target.ch.ArAmtTarget, variance)
		tFee := pyround.Round(float64(targetFeeRate)*float64(tValue)*0.000001, 3)
		if tFee == 0 {
			continue
		}
		targetTime, _ := getLocalSettingInt(ctx, q, "AR-Time", 5)
		last, lerr := q.GetLastRebalanceForPubkey(ctx, target.ch.RemotePubkey)
		if lerr == nil {
			if scheduleBlocked(last, waitPeriod, now()) {
				continue
			}
		} else if !isNoRows(lerr) {
			rebalLog(fmt.Sprintf("Error scheduling rebalances: %s", lerr))
			return toSchedule
		}
		rebalLog(fmt.Sprintf("Creating Auto Rebalance Request for: %s", target.ch.ChanID))
		rebalLog(fmt.Sprintf("Value: %d / %d | Fee: %s | Duration: %d", tValue, target.ch.ArAmtTarget, pyFloatStr(tFee), targetTime))
		_ = e.alias.ensure(ctx, q)
		// Exclude sources whose peer is the same as the target (sibling channels).
		targetOutbound := make([]string, 0, len(outboundCans))
		for _, c := range outboundCans {
			if chanToPeer[c] != target.ch.RemotePubkey {
				targetOutbound = append(targetOutbound, c)
			}
		}
		if len(targetOutbound) == 0 {
			continue
		}
		rebalLog(fmt.Sprintf("Request routing outbound via: %s", e.labelList(targetOutbound)))
		next := e.insertScheduledRebalance(ctx, q, int32(tValue), tFee, formatChanIDList(targetOutbound), target.ch.RemotePubkey, target.ch.Alias, int32(targetTime), now)
		toSchedule = append(toSchedule, next)
	}

	return toSchedule
}

// firstInboundForPeer returns the first annotated channel matching the given peer pubkey.
func firstInboundForPeer(inboundList []annotatedChannel, pub string) (annotatedChannel, bool) {
	for _, a := range inboundList {
		if a.ch.RemotePubkey == pub {
			return a, true
		}
	}
	return annotatedChannel{}, false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// insertScheduledRebalance inserts and returns a new auto-schedule rebalance record (status 0).
func (e *engine) insertScheduledRebalance(ctx context.Context, q scheduleQuerier, value int32, feeLimit float64,
	outgoingChanIDs, lastHopPubkey, targetAlias string, duration int32, now func() time.Time) *db.GuiRebalancer {
	req := tsNow(now)
	id, err := q.InsertRebalancerRecord(ctx, db.InsertRebalancerRecordParams{
		Requested: req, Value: value, FeeLimit: feeLimit, OutgoingChanIds: outgoingChanIDs,
		LastHopPubkey: lastHopPubkey, TargetAlias: targetAlias, Duration: duration, Status: 0,
	})
	if err != nil {
		rebalLog(fmt.Sprintf("Error saving database record: %s", err))
	}
	return &db.GuiRebalancer{
		ID: id, Requested: req, Value: value, FeeLimit: feeLimit, OutgoingChanIds: outgoingChanIDs,
		LastHopPubkey: lastHopPubkey, TargetAlias: targetAlias, Duration: duration, Status: 0,
	}
}
