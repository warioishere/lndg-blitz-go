package rebalancer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

func textOf(s string) pgtype.Text { return pgtype.Text{String: s, Valid: true} }

// parseRouteHex deserialises a hex-encoded protobuf Route.
func parseRouteHex(routeHex string) (*lnrpc.Route, error) {
	raw, err := hex.DecodeString(routeHex)
	if err != nil {
		return nil, err
	}
	var r lnrpc.Route
	if err := proto.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// routePath returns the hop pubkeys joined by "-".
func routePath(r *lnrpc.Route) string {
	parts := make([]string, 0, len(r.GetHops()))
	for _, h := range r.GetHops() {
		parts = append(parts, h.GetPubKey())
	}
	return strings.Join(parts, "-")
}

// routeCltv returns the CLTV delta of the final hop, or 144 for single-hop routes.
func routeCltv(r *lnrpc.Route) int {
	hops := r.GetHops()
	if len(hops) >= 2 {
		return int(hops[len(hops)-1].GetExpiry()) - int(hops[len(hops)-2].GetExpiry())
	}
	return 144
}

// routeQuerier bundles the DB access required by the route-record functions.
type routeQuerier interface {
	settingsQuerier
	GetRebalanceRoute(ctx context.Context, arg db.GetRebalanceRouteParams) (db.GuiRebalanceroute, error)
	InsertRebalanceRoute(ctx context.Context, arg db.InsertRebalanceRouteParams) error
	UpdateRebalanceRoute(ctx context.Context, arg db.UpdateRebalanceRouteParams) error
	UpdateRebalanceRouteFee(ctx context.Context, arg db.UpdateRebalanceRouteFeeParams) error
	MarkRebalanceRouteFailure(ctx context.Context, arg db.MarkRebalanceRouteFailureParams) error
}

// updateRoute upserts the route record and updates success/failure bookkeeping.
// Errors are logged and swallowed.
func updateRoute(ctx context.Context, q routeQuerier, pubkey, chanID, routeHex string, success, forgiveFailure bool, now time.Time) {
	disabled, err := collectRoutesDisabled(ctx, q)
	if err != nil {
		rebalLog(fmt.Sprintf("Error updating route record: %s", err))
		return
	}
	if disabled {
		return
	}
	parsed, err := parseRouteHex(routeHex)
	if err != nil {
		rebalLog(fmt.Sprintf("Error updating route record: %s", err))
		return
	}
	path := routePath(parsed)
	cltv := routeCltv(parsed)

	key := db.GetRebalanceRouteParams{TargetPubkey: pubkey, OutgoingChanID: chanID, Route: path}
	ro, gerr := q.GetRebalanceRoute(ctx, key)
	if isNoRows(gerr) {
		// Insert new record with defaults; last_fee_ppm is left NULL.
		if e := q.InsertRebalanceRoute(ctx, db.InsertRebalanceRouteParams{
			TargetPubkey: pubkey, OutgoingChanID: chanID, Route: path,
			FinalCltvDelta: int32(cltv), RouteHex: textOf(routeHex), LastFeePpm: pgtype.Float8{},
		}); e != nil {
			rebalLog(fmt.Sprintf("Error updating route record: %s", e))
			return
		}
		ro = db.GuiRebalanceroute{
			TargetPubkey: pubkey, OutgoingChanID: chanID, Route: path,
			FinalCltvDelta: int32(cltv), RouteHex: textOf(routeHex),
		}
	} else if gerr != nil {
		rebalLog(fmt.Sprintf("Error updating route record: %s", gerr))
		return
	}

	if ro.FinalCltvDelta != int32(cltv) {
		ro.FinalCltvDelta = int32(cltv)
	}
	if !ro.RouteHex.Valid || ro.RouteHex.String == "" {
		ro.RouteHex = textOf(routeHex)
	}
	if success {
		// Increment success_count only when there has been no recent success (within 5 min).
		if !ro.LastSuccess.Valid || now.Sub(ro.LastSuccess.Time) > 5*time.Minute {
			ro.SuccessCount++
		}
		ro.LastSuccess = pgtype.Timestamptz{Time: now, Valid: true}
		ro.LastFailure = pgtype.Timestamptz{}
		if forgiveFailure && ro.FailureCount > 0 {
			ro.FailureCount--
		}
		if parsed.GetTotalAmtMsat() != 0 && parsed.GetTotalFeesMsat() != 0 {
			fee := float64(parsed.GetTotalFeesMsat()) / float64(parsed.GetTotalAmtMsat()-parsed.GetTotalFeesMsat()) * 1000000
			ro.LastFeePpm = pgtype.Float8{Float64: fee, Valid: true}
		}
	} else {
		ro.FailureCount++
		ro.LastFailure = pgtype.Timestamptz{Time: now, Valid: true}
	}

	if e := q.UpdateRebalanceRoute(ctx, db.UpdateRebalanceRouteParams{
		TargetPubkey: pubkey, OutgoingChanID: chanID, Route: path,
		FinalCltvDelta: ro.FinalCltvDelta, RouteHex: ro.RouteHex,
		SuccessCount: ro.SuccessCount, FailureCount: ro.FailureCount,
		LastSuccess: ro.LastSuccess, LastFailure: ro.LastFailure, LastFeePpm: ro.LastFeePpm,
	}); e != nil {
		rebalLog(fmt.Sprintf("Error updating route record: %s", e))
	}
}

// updateRouteFee persists the last observed fee ppm for a route record.
func updateRouteFee(ctx context.Context, q routeQuerier, id int64, feePpm float64) {
	if e := q.UpdateRebalanceRouteFee(ctx, db.UpdateRebalanceRouteFeeParams{
		ID: id, LastFeePpm: pgtype.Float8{Float64: feePpm, Valid: true},
	}); e != nil {
		rebalLog(fmt.Sprintf("Error updating route fee: %s", e))
	}
}

// markRouteFailure increments the failure count for a route record via a DB-side update.
func markRouteFailure(ctx context.Context, q routeQuerier, id int64, now time.Time) {
	disabled, err := collectRoutesDisabled(ctx, q)
	if err != nil {
		rebalLog(fmt.Sprintf("Error marking route failure: %s", err))
		return
	}
	if disabled {
		return
	}
	if e := q.MarkRebalanceRouteFailure(ctx, db.MarkRebalanceRouteFailureParams{
		ID: id, LastFailure: pgtype.Timestamptz{Time: now, Valid: true},
	}); e != nil {
		rebalLog(fmt.Sprintf("Error marking route failure: %s", e))
	}
}

// reputationQuerier bundles the DB access required for node reputation updates.
type reputationQuerier interface {
	settingsQuerier
	IncrNodeReputationSuccess(ctx context.Context, arg db.IncrNodeReputationSuccessParams) error
	IncrNodeReputationFailure(ctx context.Context, arg db.IncrNodeReputationFailureParams) error
}

// updateNodeReputations increments success/failure counts for the nodes on a route.
func updateNodeReputations(ctx context.Context, q reputationQuerier, routeHex string, success bool, failureSourceIndex int, fsiSet bool, now time.Time) {
	disabled, err := collectRoutesDisabled(ctx, q)
	if err != nil {
		rebalLog(fmt.Sprintf("Error updating node reputations: %s", err))
		return
	}
	if disabled {
		return
	}
	parsed, err := parseRouteHex(routeHex)
	if err != nil {
		rebalLog(fmt.Sprintf("Error updating node reputations: %s", err))
		return
	}
	hops := parsed.GetHops()
	if len(hops) == 0 {
		return
	}
	ts := pgtype.Timestamptz{Time: now, Valid: true}
	if success {
		for _, h := range hops {
			if e := q.IncrNodeReputationSuccess(ctx, db.IncrNodeReputationSuccessParams{Pubkey: h.GetPubKey(), LastSuccess: ts}); e != nil {
				rebalLog(fmt.Sprintf("Error updating node reputations: %s", e))
				return
			}
		}
	} else if fsiSet && failureSourceIndex < len(hops) {
		for i, h := range hops {
			if i < failureSourceIndex {
				if e := q.IncrNodeReputationSuccess(ctx, db.IncrNodeReputationSuccessParams{Pubkey: h.GetPubKey(), LastSuccess: ts}); e != nil {
					rebalLog(fmt.Sprintf("Error updating node reputations: %s", e))
					return
				}
			} else if i == failureSourceIndex {
				if e := q.IncrNodeReputationFailure(ctx, db.IncrNodeReputationFailureParams{Pubkey: h.GetPubKey(), LastFailure: ts}); e != nil {
					rebalLog(fmt.Sprintf("Error updating node reputations: %s", e))
				}
				break
			}
		}
	}
}

// purgeQuerier bundles the DB access required by purgeStaleRoutes.
type purgeQuerier interface {
	settingsQuerier
	ListOpenChannelIDs(ctx context.Context) ([]string, error)
	DeleteRoutesNotOpenOutgoing(ctx context.Context, dollar_1 []string) (int64, error)
	DeleteStaleTestedRoutes(ctx context.Context, lastSuccess pgtype.Timestamptz) error
	DeleteStaleNodeReputations(ctx context.Context, lastSuccess pgtype.Timestamptz) error
}

// purgeStaleRoutes removes routes for closed channels and records older than
// 7 days (routes) or 14 days (node reputations).
func purgeStaleRoutes(ctx context.Context, q purgeQuerier, now time.Time) {
	disabled, err := collectRoutesDisabled(ctx, q)
	if err != nil {
		rebalLog(fmt.Sprintf("Error purging stale routes: %s", err))
		return
	}
	if disabled {
		return
	}
	openIDs, err := q.ListOpenChannelIDs(ctx)
	if err != nil {
		rebalLog(fmt.Sprintf("Error purging stale routes: %s", err))
		return
	}
	dead, err := q.DeleteRoutesNotOpenOutgoing(ctx, openIDs)
	if err != nil {
		rebalLog(fmt.Sprintf("Error purging stale routes: %s", err))
		return
	}
	if dead > 0 {
		rebalLog(fmt.Sprintf("Purged %d routes for closed outgoing channels", dead))
	}
	cutoff := pgtype.Timestamptz{Time: now.Add(-7 * 24 * time.Hour), Valid: true}
	if e := q.DeleteStaleTestedRoutes(ctx, cutoff); e != nil {
		rebalLog(fmt.Sprintf("Error purging stale routes: %s", e))
		return
	}
	repCutoff := pgtype.Timestamptz{Time: now.Add(-14 * 24 * time.Hour), Valid: true}
	if e := q.DeleteStaleNodeReputations(ctx, repCutoff); e != nil {
		rebalLog(fmt.Sprintf("Error purging stale routes: %s", e))
	}
}
