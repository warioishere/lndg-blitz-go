package web

import (
	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// This file contains viewset helpers for endpoints that require JOINs,
// annotations, or computed fields that don't fit the generic list path.

// --- Rebalancer success/weighted ratios ---

// successRatio computes (success_count + 1) / (success_count + failure_count + 2).
func successRatio(sc, fc int32) float64 {
	return float64(sc+1) / float64(sc+fc+2)
}

// weightedRatio computes the weighted success ratio with weight=10:
// ratio * (total / (total + 10)), where ratio = (sc+1)/(sc+fc+2) and total = sc+fc.
// Uses float64 arithmetic matching the Postgres float8 expression used for ORDER BY.
func weightedRatio(sc, fc int32) float64 {
	scf, fcf := float64(sc), float64(fc)
	ratio := (scf + 1.0) / (scf + fcf + 2.0)
	total := scf + fcf
	return ratio * (total / (total + 10.0))
}

// weightedRatioSQL returns the float8 SQL expression for ORDER BY clauses,
// equivalent to the weightedRatio computation above.
func weightedRatioSQL(sc, fc string) string {
	num := "(" + sc + "::float8 + 1.0)"
	den := "(" + sc + "::float8 + " + fc + "::float8 + 2.0)"
	total := "(" + sc + "::float8 + " + fc + "::float8)"
	return "((" + num + "/" + den + ") * (" + total + "/(" + total + " + 10.0)))"
}

// --- PeerEvents (out_liq_percent computed via JOIN on gui_channels.capacity) ---

type peerEventRow struct {
	ID        int64              `json:"id"`
	Timestamp pgtype.Timestamptz `json:"timestamp"`
	ChanID    string             `json:"chan_id"`
	PeerAlias string             `json:"peer_alias"`
	Event     string             `json:"event"`
	OldValue  pgtype.Int8        `json:"old_value"`
	NewValue  int64              `json:"new_value"`
	OutLiq    int64              `json:"out_liq"`
	Capacity  pgtype.Int8        `json:"-"` // JOIN column, not included in output
}

// outLiqPercent computes int(round((out_liq/capacity)*100, 1)) using
// round-half-to-even on 1 decimal place, then truncates toward zero.
// Returns nil when the channel is missing or capacity is zero.
func outLiqPercent(outLiq int64, capacity pgtype.Int8) any {
	if !capacity.Valid || capacity.Int64 == 0 {
		return nil
	}
	x := float64(outLiq) / float64(capacity.Int64) * 100
	return int(pyround.Round(x, 1))
}

func peerEventToResult(row *peerEventRow) *orderedMap {
	base := db.GuiPeerevent{
		ID: row.ID, Timestamp: row.Timestamp, ChanID: row.ChanID,
		PeerAlias: row.PeerAlias, Event: row.Event, OldValue: row.OldValue,
		NewValue: row.NewValue, OutLiq: row.OutLiq,
	}
	return structToOrderedMap(&base).Set("out_liq_percent", outLiqPercent(row.OutLiq, row.Capacity))
}

// --- RebalanceRoute (annotated ratios + target/outgoing aliases) ---

type rebalanceRouteRow struct {
	ID             int64              `json:"id"`
	TargetPubkey   string             `json:"target_pubkey"`
	OutgoingChanID string             `json:"outgoing_chan_id"`
	Route          string             `json:"route"`
	FinalCltvDelta int32              `json:"final_cltv_delta"`
	SuccessCount   int32              `json:"success_count"`
	FailureCount   int32              `json:"failure_count"`
	LastSuccess    pgtype.Timestamptz `json:"last_success"`
	LastFailure    pgtype.Timestamptz `json:"last_failure"`
	RouteHex       pgtype.Text        `json:"route_hex"`
	LastFeePpm     pgtype.Float8      `json:"last_fee_ppm"`
	TargetAlias    pgtype.Text        `json:"-"`
	OutgoingAlias  pgtype.Text        `json:"-"`
}

func rebalanceRouteToResult(row *rebalanceRouteRow) *orderedMap {
	base := db.GuiRebalanceroute{
		ID: row.ID, TargetPubkey: row.TargetPubkey, OutgoingChanID: row.OutgoingChanID,
		Route: row.Route, FinalCltvDelta: row.FinalCltvDelta, SuccessCount: row.SuccessCount,
		FailureCount: row.FailureCount, LastSuccess: row.LastSuccess, LastFailure: row.LastFailure,
		RouteHex: row.RouteHex, LastFeePpm: row.LastFeePpm,
	}
	return structToOrderedMap(&base).
		Set("success_ratio", successRatio(row.SuccessCount, row.FailureCount)).
		Set("weighted_ratio", weightedRatio(row.SuccessCount, row.FailureCount)).
		Set("target_alias", row.TargetAlias).
		Set("outgoing_alias", row.OutgoingAlias)
}

// --- NodeReputation (explicit field list + weighted_ratio score + alias) ---

type nodeReputationRow struct {
	Pubkey       string             `json:"pubkey"`
	SuccessCount int32              `json:"success_count"`
	FailureCount int32              `json:"failure_count"`
	LastSuccess  pgtype.Timestamptz `json:"last_success"`
	LastFailure  pgtype.Timestamptz `json:"last_failure"`
	Alias        pgtype.Text        `json:"-"`
}

func nodeReputationToResult(row *nodeReputationRow) *orderedMap {
	// Fields are emitted in the order required by the serializer.
	return newOrderedMap().
		Set("pubkey", row.Pubkey).
		Set("success_count", row.SuccessCount).
		Set("failure_count", row.FailureCount).
		Set("last_success", drfDateTime(row.LastSuccess)).
		Set("last_failure", drfDateTime(row.LastFailure)).
		Set("weighted_ratio", weightedRatio(row.SuccessCount, row.FailureCount)).
		Set("alias", row.Alias)
}

// --- ProbeLog (details jsonb returned as raw JSON rather than base64) ---

func probeLogToResult(p *db.GuiProbelog) *orderedMap {
	return structToOrderedMap(p).Set("details", json.RawMessage(p.Details))
}
