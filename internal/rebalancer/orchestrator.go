package rebalancer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// listChannelsClient is the ListChannels subset used by sortChannelsByHtlc and updateChannelBalances.
type listChannelsClient interface {
	ListChannels(ctx context.Context, in *lnrpc.ListChannelsRequest, opts ...grpc.CallOption) (*lnrpc.ListChannelsResponse, error)
}

// --- Settings-Flags ------------------------------------------------------

// checkAndSetAllowMultishards returns false if LND-DisableMPP > 0. DB errors are propagated.
func checkAndSetAllowMultishards(ctx context.Context, q settingsQuerier) (bool, error) {
	v, err := getLocalSettingInt(ctx, q, "LND-DisableMPP", 0)
	if err != nil {
		return false, err
	}
	return v <= 0, nil
}

// getMaxFeeRate reads AR-MaxFeeRate (default 500).
func getMaxFeeRate(ctx context.Context, q settingsQuerier) (int, error) {
	return getLocalSettingInt(ctx, q, "AR-MaxFeeRate", 500)
}

// getPerSourceEnabled returns true when AR-PerSourceEnabled == 1.
func getPerSourceEnabled(ctx context.Context, q settingsQuerier) (bool, error) {
	v, err := getLocalSettingInt(ctx, q, "AR-PerSourceEnabled", 0)
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

// savedRoutesEnabled returns true when RR-UseSavedRoutes is not '0'.
// On error it logs and returns true.
func savedRoutesEnabled(ctx context.Context, q settingsQuerier) bool {
	v, err := getLocalSettingStr(ctx, q, "RR-UseSavedRoutes", "1")
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting saved route setting: %s", err))
		return true
	}
	return v != "0"
}

// getRouteLimit reads RR-RouteLimit (default 10). On error it logs and returns 10.
func getRouteLimit(ctx context.Context, q settingsQuerier) int {
	v, err := getLocalSettingInt(ctx, q, "RR-RouteLimit", 10)
	if err != nil {
		rebalLog(fmt.Sprintf("Error getting route limit: %s", err))
		return 10
	}
	return v
}

// --- Channel-Annotation --------------------------------------------------

// annotatedChannel holds computed percent_outbound and inbound_can values for a channel.
type annotatedChannel struct {
	ch              db.GuiChannel
	percentOutbound float64
	inboundCan      int64
}

// annotateChannel computes percent_outbound and inbound_can for a single channel.
// integerMode: value subtraction uses integer arithmetic (truncation toward zero).
// Otherwise float division is used. inbound_can always uses integer division.
func annotateChannel(ch db.GuiChannel, valueSubtract float64, integerMode bool) annotatedChannel {
	var pct float64
	base := ch.LocalBalance + ch.PendingOutbound - int64(ch.LocalChanReserve)
	if integerMode {
		num := base - int64(valueSubtract)
		if ch.Capacity != 0 {
			pct = float64((num * 100) / ch.Capacity)
		}
	} else {
		num := float64(base) - valueSubtract
		if ch.Capacity != 0 {
			pct = num * 100 / float64(ch.Capacity)
		}
	}
	var inboundCan int64
	if ch.Capacity != 0 && ch.ArInTarget != 0 {
		inboundCan = ((ch.RemoteBalance + ch.PendingInbound) * 100 / ch.Capacity) / int64(ch.ArInTarget)
	}
	return annotatedChannel{ch: ch, percentOutbound: pct, inboundCan: inboundCan}
}

func annotateChannels(chs []db.GuiChannel, valueSubtract float64, integerMode bool) []annotatedChannel {
	out := make([]annotatedChannel, 0, len(chs))
	for _, ch := range chs {
		out = append(out, annotateChannel(ch, valueSubtract, integerMode))
	}
	return out
}

// getOutCans returns chan_ids that are eligible outbound sources:
// percent_outbound >= ar_out_target, not local_disabled,
// (ar_source OR not auto_rebalance), remote_pubkey != last_hop;
// sorted by htlc_count ascending.
func (e *engine) getOutCans(annotated []annotatedChannel, lastHopPubkey string) []string {
	filtered := make([]annotatedChannel, 0, len(annotated))
	for _, a := range annotated {
		c := a.ch
		if a.percentOutbound < float64(c.ArOutTarget) {
			continue
		}
		if c.LocalDisabled {
			continue
		}
		if !c.ArSource && c.AutoRebalance {
			continue
		}
		if c.RemotePubkey == lastHopPubkey {
			continue
		}
		filtered = append(filtered, a)
	}
	sort.SliceStable(filtered, func(i, j int) bool { return filtered[i].ch.HtlcCount < filtered[j].ch.HtlcCount })
	result := make([]string, 0, len(filtered))
	for _, a := range filtered {
		result = append(result, a.ch.ChanID)
	}
	if len(result) > 1 {
		rebalLog(fmt.Sprintf("get_out_cans: Found %d candidate channels (ordered by DB htlc_count): %s", len(result), e.labelList(result)))
	}
	return result
}

// labelList formats a slice of channel IDs as a list string with aliases.
func (e *engine) labelList(cids []string) string {
	parts := make([]string, len(cids))
	for i, c := range cids {
		parts[i] = "'" + e.alias.label(c) + "'"
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// --- sort_channels_by_htlc ----------------------------------------------

// sortChannelsByHtlc deduplicates sibling channels (same peer) by keeping only
// the one with the lowest pending-HTLC count. Channels unknown to LND are dropped.
// On error the original list is returned.
func (e *engine) sortChannelsByHtlc(ctx context.Context, stub listChannelsClient, chanIDs []string) []string {
	if len(chanIDs) <= 1 {
		rebalLog("HTLC Filter: Only 1 channel, no filtering needed")
		return chanIDs
	}
	rebalLog(fmt.Sprintf("HTLC Filter: Checking %d channels from DB", len(chanIDs)))
	resp, err := stub.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		rebalLog(fmt.Sprintf("Error filtering channels by HTLC: %s", err))
		return chanIDs
	}
	want := make(map[string]struct{}, len(chanIDs))
	for _, c := range chanIDs {
		want[c] = struct{}{}
	}
	type cdata struct {
		htlc int
		peer string
	}
	chanData := make(map[string]cdata)
	for _, c := range resp.GetChannels() {
		cid := strconv.FormatUint(c.GetChanId(), 10)
		if _, ok := want[cid]; ok {
			chanData[cid] = cdata{htlc: len(c.GetPendingHtlcs()), peer: c.GetRemotePubkey()}
			rebalLog(fmt.Sprintf("HTLC Filter: Chan %s has %d pending HTLCs", e.alias.label(cid), len(c.GetPendingHtlcs())))
		}
	}
	type pc struct {
		chanID string
		htlc   int
	}
	var peerOrder []string
	peerChannels := make(map[string][]pc)
	for _, cid := range chanIDs {
		d, ok := chanData[cid]
		if !ok {
			continue
		}
		if _, seen := peerChannels[d.peer]; !seen {
			peerOrder = append(peerOrder, d.peer)
		}
		peerChannels[d.peer] = append(peerChannels[d.peer], pc{chanID: cid, htlc: d.htlc})
	}
	filtered := make([]string, 0, len(peerOrder))
	for _, peer := range peerOrder {
		list := peerChannels[peer]
		if len(list) > 1 {
			sorted := make([]pc, len(list))
			copy(sorted, list)
			sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].htlc < sorted[j].htlc })
			sel := sorted[0]
			filtered = append(filtered, sel.chanID)
			excl := make([]string, 0, len(sorted)-1)
			for _, c := range sorted[1:] {
				excl = append(excl, fmt.Sprintf("'%s (%d HTLCs)'", e.alias.label(c.chanID), c.htlc))
			}
			rebalLog(fmt.Sprintf("HTLC Filter: Peer %s has %d channels - selected %s with %d HTLCs, excluded: [%s]",
				e.alias.alias(sel.chanID), len(list), e.alias.label(sel.chanID), sel.htlc, strings.Join(excl, ", ")))
		} else {
			filtered = append(filtered, list[0].chanID)
		}
	}
	rebalLog(fmt.Sprintf("HTLC Filter: Original list: %s", e.labelList(chanIDs)))
	rebalLog(fmt.Sprintf("HTLC Filter: Filtered list: %s (removed %d channels)", e.labelList(filtered), len(chanIDs)-len(filtered)))
	return filtered
}

// --- chan_id-Liste Format/Parse -----------------------------------------

// formatChanIDList formats a slice of channel IDs as "[123, 456]".
func formatChanIDList(cids []string) string {
	return "[" + strings.Join(cids, ", ") + "]"
}

// parseChanIDs parses a "[123, 456]" string into a slice of chan_id strings.
func parseChanIDs(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// --- estimate_liquidity / _remaining_drain / update_channels -------------

// estimateLiquidity returns the largest total_amt of a failed attempt whose
// failure originated from the last hop, as a liquidity estimate in satoshis.
func estimateLiquidity(payment *lnrpc.Payment) int64 {
	if payment == nil {
		return 0
	}
	var est int64
	if payment.GetStatus() == lnrpc.Payment_FAILED { // status == 3
		for _, attempt := range payment.GetHtlcs() {
			totalHops := len(attempt.GetRoute().GetHops())
			if int(attempt.GetFailure().GetFailureSourceIndex()) == totalHops {
				if ta := attempt.GetRoute().GetTotalAmt(); ta > est {
					est = ta
				}
			}
		}
	}
	rebalLog(fmt.Sprintf("Estimated Liquidity %d for payment %s with status %d and reason %d",
		est, payment.GetPaymentHash(), int(payment.GetStatus()), int(payment.GetFailureReason())))
	return est
}

// remainingDrainQuerier is the DB subset required by remainingDrain.
type remainingDrainQuerier interface {
	ListRemainingDrainChannels(ctx context.Context, remotePubkey string) ([]db.ListRemainingDrainChannelsRow, error)
}

// remainingDrain returns the total satoshis above ar_in_target across all
// auto-rebalance channels to the given peer.
func remainingDrain(ctx context.Context, q remainingDrainQuerier, pubkey string) (int64, error) {
	rows, err := q.ListRemainingDrainChannels(ctx, pubkey)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, c := range rows {
		curIn := c.RemoteBalance + c.PendingInbound
		tgtIn := (c.Capacity * int64(c.ArInTarget)) / 100
		if d := curIn - tgtIn; d > 0 {
			total += d
		}
	}
	return total, nil
}

// updateChannelsQuerier is the DB subset required by updateChannelBalances.
type updateChannelsQuerier interface {
	GetChannelBalances(ctx context.Context, chanID string) (db.GetChannelBalancesRow, error)
	SetChannelBalances(ctx context.Context, arg db.SetChannelBalancesParams) error
}

// updateChannelBalances refreshes local/remote balance in the DB for the incoming
// and outgoing channels after a successful rebalance. Errors are logged and swallowed.
func updateChannelBalances(ctx context.Context, q updateChannelsQuerier, stub listChannelsClient, incomingChanID, outgoingChanID uint64) {
	rebalLog(fmt.Sprintf("update_channels: Updating balances for incoming=%d, outgoing=%d", incomingChanID, outgoingChanID))
	resp, err := stub.ListChannels(ctx, &lnrpc.ListChannelsRequest{})
	if err != nil {
		rebalLog(fmt.Sprintf("Error updating channel balances: %s", err))
		return
	}
	channels := resp.GetChannels()
	applyOne := func(chanID uint64, label string) {
		var lc *lnrpc.Channel
		for _, c := range channels {
			if c.GetChanId() == chanID {
				lc = c
				break
			}
		}
		if lc == nil {
			return
		}
		cidStr := strconv.FormatUint(chanID, 10)
		old, gerr := q.GetChannelBalances(ctx, cidStr)
		if gerr != nil {
			if isNoRows(gerr) {
				return
			}
			rebalLog(fmt.Sprintf("Error updating channel balances: %s", gerr))
			return
		}
		if serr := q.SetChannelBalances(ctx, db.SetChannelBalancesParams{
			ChanID: cidStr, LocalBalance: lc.GetLocalBalance(), RemoteBalance: lc.GetRemoteBalance(),
		}); serr != nil {
			rebalLog(fmt.Sprintf("Error updating channel balances: %s", serr))
			return
		}
		rebalLog(fmt.Sprintf("update_channels: %s chan %d - local: %d->%d, remote: %d->%d",
			label, chanID, old.LocalBalance, lc.GetLocalBalance(), old.RemoteBalance, lc.GetRemoteBalance()))
	}
	applyOne(incomingChanID, "Incoming")
	applyOne(outgoingChanID, "Outgoing")
}

// --- Source-Maps / Target-Info / Allowed-Sources -------------------------

// sourceMapsQuerier is the DB subset required by getSourceMaps.
type sourceMapsQuerier interface {
	ListChannelFeeAndDiffByIDs(ctx context.Context, dollar_1 []string) ([]db.ListChannelFeeAndDiffByIDsRow, error)
}

// getSourceMaps returns two maps keyed by chan_id: local_fee_rate and ar_source_ppm_diff.
func getSourceMaps(ctx context.Context, q sourceMapsQuerier, chanIDs []string) (feeMap, diffMap map[string]int, err error) {
	rows, err := q.ListChannelFeeAndDiffByIDs(ctx, chanIDs)
	if err != nil {
		return nil, nil, err
	}
	feeMap = make(map[string]int, len(rows))
	diffMap = make(map[string]int, len(rows))
	for _, r := range rows {
		feeMap[r.ChanID] = int(r.LocalFeeRate)
		diffMap[r.ChanID] = int(r.ArSourcePpmDiff)
	}
	return feeMap, diffMap, nil
}

// targetInfoQuerier is the DB subset required by getTargetInfo.
type targetInfoQuerier interface {
	GetTargetChannelInfo(ctx context.Context, remotePubkey string) (db.GetTargetChannelInfoRow, error)
}

// getTargetInfo returns (local_fee_rate, ar_max_cost, ok) for the target peer.
// ok is false when no matching channel exists.
func getTargetInfo(ctx context.Context, q targetInfoQuerier, lastHopPubkey string) (int, int, bool, error) {
	row, err := q.GetTargetChannelInfo(ctx, lastHopPubkey)
	if err != nil {
		if isNoRows(err) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	return int(row.LocalFeeRate), int(row.ArMaxCost), true, nil
}

// allowedSourcesQuerier is the DB subset required by getAllowedSourcesForTarget.
type allowedSourcesQuerier interface {
	ListAllowedTargetSources(ctx context.Context, targetPubkey string) ([]string, error)
}

// getAllowedSourcesForTarget returns the set of permitted source chan_ids for a
// target pubkey, or (nil, false) if none are configured.
func getAllowedSourcesForTarget(ctx context.Context, q allowedSourcesQuerier, targetPubkey string) (map[string]struct{}, bool, error) {
	rows, err := q.ListAllowedTargetSources(ctx, targetPubkey)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	ids := make(map[string]struct{}, len(rows))
	for _, id := range rows {
		ids[id] = struct{}{}
	}
	return ids, true, nil
}

var errNoTargetInfo = errors.New("no target info")
