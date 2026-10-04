// Package graphwatcher subscribes to the LND channel graph, detects new
// channels to auto-rebalance targets, probes routes via jobs.ProbeTargets,
// and schedules rebalances when viable routes are found.
package graphwatcher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/jobs"
	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// Querier bundles all database access required by graphwatcher.
type Querier interface {
	jobs.ProbeQuerier // settings, probe-Queries, GetPeerAlias
	GetNodeCache(ctx context.Context, pubkey string) (db.GuiNodecache, error)
	UpsertNodeCache(ctx context.Context, arg db.UpsertNodeCacheParams) error
	ListAutoRebalancePeerPubkeys(ctx context.Context) ([]string, error)
	ListOpenARChannelsByPubkey(ctx context.Context, remotePubkey string) ([]db.GuiChannel, error)
	ListGraphOutboundCans(ctx context.Context) ([]string, error)
	ListChannelFeeAndDiffByIDs(ctx context.Context, dollar_1 []string) ([]db.ListChannelFeeAndDiffByIDsRow, error)
	ListRecentRoutesForTarget(ctx context.Context, targetPubkey string) ([]db.ListRecentRoutesForTargetRow, error)
	HasActiveRebalanceForPubkey(ctx context.Context, lastHopPubkey string) (bool, error)
	InsertGraphProbeLog(ctx context.Context, arg db.InsertGraphProbeLogParams) error
	InsertGraphEvent(ctx context.Context, arg db.InsertGraphEventParams) error
	UpdateGraphEventRoutesFound(ctx context.Context, arg db.UpdateGraphEventRoutesFoundParams) error
	DeleteOldGraphEvents(ctx context.Context) error
	InsertRebalancerRecord(ctx context.Context, arg db.InsertRebalancerRecordParams) (int64, error)
}

// Client bundles the LND RPCs needed by graphwatcher (subset of LightningClient).
type Client interface {
	jobs.ProbeLightningClient // GetInfo, QueryRoutes
	GetNodeInfo(ctx context.Context, in *lnrpc.NodeInfoRequest, opts ...grpc.CallOption) (*lnrpc.NodeInfo, error)
	GetChanInfo(ctx context.Context, in *lnrpc.ChanInfoRequest, opts ...grpc.CallOption) (*lnrpc.ChannelEdge, error)
	SubscribeChannelGraph(ctx context.Context, in *lnrpc.GraphTopologySubscription, opts ...grpc.CallOption) (grpc.ServerStreamingClient[lnrpc.GraphTopologyUpdate], error)
}

// Deps bundles all external dependencies. Router is used by ProbeTargets (BuildRoute/SendToRouteV2).
type Deps struct {
	Q      Querier
	LN     Client
	Router jobs.ProbeRouterClient
	Now    func() time.Time
	Sleep  func(time.Duration)
}

func gwLog(msg string) {
	fmt.Printf("%s : [GraphWatcher] : %s\n", time.Now().Format("Mon Jan  2 15:04:05 2006"), msg)
}

func chanIDStr(id uint64) string { return strconv.FormatUint(id, 10) }

func formatChanIDList(cids []string) string { return "[" + strings.Join(cids, ", ") + "]" }

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// getSetting returns the value of a local setting, or def if not found.
func getSetting(ctx context.Context, q Querier, key, def string) string {
	row, err := q.GetLocalSetting(ctx, key)
	if err != nil {
		return def
	}
	return row.Value
}

// ensureSetting returns the value of a local setting, creating it with the given default if absent.
func ensureSetting(ctx context.Context, q Querier, key, def string) string {
	row, err := q.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{Key: key, Value: def})
	if err != nil {
		return def
	}
	return row.Value
}

func atoiOr(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// loadARTargets returns auto-rebalance peer pubkeys minus any listed in the GW-Exclude setting.
func loadARTargets(ctx context.Context, q Querier) (map[string]struct{}, error) {
	excluded := map[string]struct{}{}
	raw := getSetting(ctx, q, "GW-Exclude", "")
	if raw != "" {
		for _, pk := range strings.Split(raw, ",") {
			if t := strings.TrimSpace(pk); t != "" {
				excluded[t] = struct{}{}
			}
		}
	}
	pubkeys, err := q.ListAutoRebalancePeerPubkeys(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for _, pk := range pubkeys {
		if _, ex := excluded[pk]; !ex {
			out[pk] = struct{}{}
		}
	}
	return out, nil
}

// getAlias resolves a display name for a pubkey: peer alias > node cache > first 12 chars.
func (d Deps) getAlias(ctx context.Context, pubkey string) string {
	alias, err := d.Q.GetPeerAlias(ctx, pubkey)
	if err == nil && alias.Valid && alias.String != "" {
		return alias.String
	}
	if info, err := lnd.GetNodeInfoCached(ctx, d.Q, d.LN, pubkey); err == nil {
		if a := info.GetNode().GetAlias(); a != "" {
			return a
		}
	}
	if len(pubkey) >= 12 {
		return pubkey[:12]
	}
	return pubkey
}

// triggerProbe probes routes to a target pubkey from all eligible outbound sources,
// logs the results, and schedules a rebalance if any route was found. Returns total_new.
func (d Deps) triggerProbe(ctx context.Context, selfPubkey, targetPubkey, otherPubkey string, otherFeePpm pgtype.Int4, chanID string, now time.Time) (int, error) {
	targets, err := d.Q.ListOpenARChannelsByPubkey(ctx, targetPubkey)
	if err != nil {
		return 0, err
	}
	if len(targets) == 0 {
		return 0, nil
	}
	ch := targets[0]
	allFull := true
	for _, c := range targets {
		if c.RemoteBalance > int64(c.LocalChanReserve) {
			allFull = false
			break
		}
	}
	if allFull {
		gwLog(fmt.Sprintf("%s - skip, all channels full", ch.Alias))
		return 0, nil
	}
	outboundCans, err := d.Q.ListGraphOutboundCans(ctx)
	if err != nil {
		return 0, err
	}
	feeRows, err := d.Q.ListChannelFeeAndDiffByIDs(ctx, outboundCans)
	if err != nil {
		return 0, err
	}
	sourceFeeMap := make(map[string]int, len(feeRows))
	for _, r := range feeRows {
		sourceFeeMap[r.ChanID] = int(r.LocalFeeRate)
	}
	maxFeeRate := atoiOr(getSetting(ctx, d.Q, "AR-MaxFeeRate", "500"), 500)
	maxPerTarget := atoiOr(getSetting(ctx, d.Q, "QR-MaxPerTarget", "5"), 5)

	otherAlias := "?"
	if otherPubkey != "" {
		otherAlias = d.getAlias(ctx, otherPubkey)
	}
	budgetPpm := int(float64(int(ch.LocalFeeRate)*int(ch.ArMaxCost)) / 100)
	sourcesTried := minInt(len(outboundCans), maxPerTarget)
	gwLog(fmt.Sprintf("Probing %s (fee=%d ppm) triggered by new channel %s from %s (fee=%s ppm)",
		ch.Alias, ch.LocalFeeRate, chanID, otherAlias, int4Str(otherFeePpm)))
	gwLog(fmt.Sprintf("  trying %d of %d outbound sources, budget = %d * %d%% = %d ppm",
		sourcesTried, len(outboundCans), ch.LocalFeeRate, ch.ArMaxCost, budgetPpm))

	totalNew, totalExisting, totalErrors, err := jobs.ProbeTargets(ctx, d.LN, d.Router, d.Q, selfPubkey, targets, outboundCans, sourceFeeMap, maxFeeRate, maxPerTarget)
	if err != nil {
		return 0, err
	}

	recent, err := d.Q.ListRecentRoutesForTarget(ctx, targetPubkey)
	if err != nil {
		return 0, err
	}
	var routeLines []string
	routesViaNew := 0
	for _, rr := range recent {
		hops := strings.Split(rr.Route, "-")
		viaNew := false
		if otherPubkey != "" {
			for _, h := range hops {
				if h == otherPubkey {
					viaNew = true
					break
				}
			}
		}
		if viaNew {
			routesViaNew++
		}
		feeStr := "? ppm"
		if rr.LastFeePpm.Valid && rr.LastFeePpm.Float64 != 0 {
			feeStr = fmt.Sprintf("%d ppm", int(rr.LastFeePpm.Float64))
		}
		marker := ""
		if viaNew {
			marker = " ← via new peer!"
		}
		line := fmt.Sprintf("%d hops, %s, out=%s%s", len(hops), feeStr, rr.OutgoingChanID, marker)
		routeLines = append(routeLines, line)
		gwLog(fmt.Sprintf("  route: %s", line))
	}
	gwLog(fmt.Sprintf("  result: %d new, %d existing, %d errors, %d via new peer", totalNew, totalExisting, totalErrors, routesViaNew))

	scheduled := false
	if totalNew+totalExisting > 0 {
		if serr := d.scheduleRebalance(ctx, targetPubkey, targets, outboundCans, sourceFeeMap, maxFeeRate, now); serr != nil {
			return 0, serr
		}
		scheduled = true
	} else {
		gwLog("  no routes found, skipping rebalance")
	}

	if err := d.Q.InsertGraphProbeLog(ctx, db.InsertGraphProbeLogParams{
		Timestamp: pgtype.Timestamptz{Time: now, Valid: true}, TargetPubkey: targetPubkey, TargetAlias: ch.Alias,
		TargetFee: ch.LocalFeeRate, TargetMaxCost: ch.ArMaxCost, TriggerChanID: chanID,
		OtherPubkey: otherPubkey, OtherAlias: otherAlias, OtherFeePpm: otherFeePpm,
		BudgetPpm: int32(budgetPpm), SourcesTried: int32(sourcesTried),
		RoutesNew: int32(totalNew), RoutesExisting: int32(totalExisting), Errors: int32(totalErrors),
		RoutesViaNewPeer: int32(routesViaNew), RebalanceScheduled: scheduled, Details: strings.Join(routeLines, "\n"),
	}); err != nil {
		return 0, err
	}
	return totalNew, nil
}

// scheduleRebalance inserts a rebalancer record for the target if no active rebalance exists,
// the channel has usable remote balance, and the computed fee rate is positive.
func (d Deps) scheduleRebalance(ctx context.Context, targetPubkey string, targets []db.GuiChannel, outboundCans []string, sourceFeeMap map[string]int, maxFeeRate int, now time.Time) error {
	active, err := d.Q.HasActiveRebalanceForPubkey(ctx, targetPubkey)
	if err != nil {
		return err
	}
	if active {
		return nil
	}
	if len(targets) == 0 {
		return nil
	}
	ch := targets[0]
	if ch.RemoteBalance <= int64(ch.LocalChanReserve) {
		return nil
	}
	targetTime := atoiOr(getSetting(ctx, d.Q, "AR-Time", "5"), 5)
	minSourceFee := 0
	first := true
	for _, v := range sourceFeeMap {
		if first || v < minSourceFee {
			minSourceFee = v
			first = false
		}
	}
	feeRate := minInt(maxFeeRate, int(float64(ch.LocalFeeRate)*(float64(ch.ArMaxCost)/100))-minSourceFee)
	if feeRate <= 0 {
		return nil
	}
	feeLimit := pyround.Round(float64(feeRate)*float64(ch.ArAmtTarget)*0.000001, 3)
	if _, err := d.Q.InsertRebalancerRecord(ctx, db.InsertRebalancerRecordParams{
		Requested: pgtype.Timestamptz{Time: now, Valid: true}, Value: int32(ch.ArAmtTarget), FeeLimit: feeLimit,
		OutgoingChanIds: formatChanIDList(outboundCans), LastHopPubkey: targetPubkey, TargetAlias: ch.Alias,
		Duration: int32(targetTime), Status: 0,
	}); err != nil {
		return err
	}
	gwLog(fmt.Sprintf("Scheduled rebalance for %s: %d sats @ max %d ppm (bypassing ar_in_target)", ch.Alias, ch.ArAmtTarget, feeRate))
	return nil
}

func int4Str(v pgtype.Int4) string {
	if !v.Valid {
		return "None"
	}
	return strconv.Itoa(int(v.Int32))
}
