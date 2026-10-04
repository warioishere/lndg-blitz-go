package web

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-chi/chi/v5"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// The Django API answers with HyperlinkedModelSerializers: every object starts
// with a "url" to its detail route, followed by the serializer fields in
// declaration order (declared fields first, then model fields), and a
// HyperlinkedModelSerializer has no "id" unless one is declared. drfShapes
// holds that field list per ViewSet (generated from the Python serializers).

type drfField struct{ out, src string }

type drfShape struct {
	pk     string // row key holding the primary key used in the url
	fields []drfField
}

var drfShapes = map[string]drfShape{
	"payments": {pk: "payment_hash", fields: []drfField{
		{"id", "index"}, {"payment_hash", "payment_hash"}, {"creation_date", "creation_date"}, {"value", "value"}, {"fee", "fee"}, {"status", "status"}, {"index", "index"}, {"chan_out", "chan_out"}, {"chan_out_alias", "chan_out_alias"}, {"keysend_preimage", "keysend_preimage"}, {"message", "message"}, {"cleaned", "cleaned"}, {"rebal_chan", "rebal_chan"}, {"source_fee_rate", "source_fee_rate"},
	}},
	"paymenthops": {pk: "id", fields: []drfField{
		{"payment_hash", "payment_hash_id"}, {"attempt_id", "attempt_id"}, {"step", "step"}, {"chan_id", "chan_id"}, {"alias", "alias"}, {"chan_capacity", "chan_capacity"}, {"node_pubkey", "node_pubkey"}, {"amt", "amt"}, {"fee", "fee"}, {"cost_to", "cost_to"},
	}},
	"invoices": {pk: "r_hash", fields: []drfField{
		{"id", "index"}, {"r_hash", "r_hash"}, {"creation_date", "creation_date"}, {"settle_date", "settle_date"}, {"value", "value"}, {"amt_paid", "amt_paid"}, {"state", "state"}, {"chan_in", "chan_in"}, {"chan_in_alias", "chan_in_alias"}, {"keysend_preimage", "keysend_preimage"}, {"message", "message"}, {"sender", "sender"}, {"sender_alias", "sender_alias"}, {"index", "index"}, {"is_revenue", "is_revenue"},
	}},
	"forwards": {pk: "id", fields: []drfField{
		{"id", "id"}, {"forward_date", "forward_date"}, {"chan_id_in", "chan_id_in"}, {"chan_id_out", "chan_id_out"}, {"chan_in_alias", "chan_in_alias"}, {"chan_out_alias", "chan_out_alias"}, {"amt_in_msat", "amt_in_msat"}, {"amt_out_msat", "amt_out_msat"}, {"fee", "fee"}, {"inbound_fee", "inbound_fee"},
	}},
	"onchain": {pk: "tx_hash", fields: []drfField{
		{"tx_hash", "tx_hash"}, {"amount", "amount"}, {"block_hash", "block_hash"}, {"block_height", "block_height"}, {"time_stamp", "time_stamp"}, {"fee", "fee"}, {"label", "label"},
	}},
	"closures": {pk: "id", fields: []drfField{
		{"id", "id"}, {"chan_id", "chan_id"}, {"funding_txid", "funding_txid"}, {"funding_index", "funding_index"}, {"closing_tx", "closing_tx"}, {"remote_pubkey", "remote_pubkey"}, {"capacity", "capacity"}, {"close_height", "close_height"}, {"settled_balance", "settled_balance"}, {"time_locked_balance", "time_locked_balance"}, {"close_type", "close_type"}, {"open_initiator", "open_initiator"}, {"close_initiator", "close_initiator"}, {"resolution_count", "resolution_count"}, {"closing_costs", "closing_costs"},
	}},
	"resolutions": {pk: "id", fields: []drfField{
		{"id", "id"}, {"chan_id", "chan_id"}, {"resolution_type", "resolution_type"}, {"outcome", "outcome"}, {"outpoint_tx", "outpoint_tx"}, {"outpoint_index", "outpoint_index"}, {"amount_sat", "amount_sat"}, {"sweep_txid", "sweep_txid"},
	}},
	"peers": {pk: "pubkey", fields: []drfField{
		{"pubkey", "pubkey"}, {"alias", "alias"}, {"address", "address"}, {"sat_sent", "sat_sent"}, {"sat_recv", "sat_recv"}, {"inbound", "inbound"}, {"connected", "connected"}, {"last_reconnected", "last_reconnected"}, {"ping_time", "ping_time"},
	}},
	"channels": {pk: "chan_id", fields: []drfField{
		{"chan_id", "chan_id"}, {"remote_pubkey", "remote_pubkey"}, {"funding_txid", "funding_txid"}, {"output_index", "output_index"}, {"capacity", "capacity"}, {"local_balance", "local_balance"}, {"remote_balance", "remote_balance"}, {"unsettled_balance", "unsettled_balance"}, {"local_commit", "local_commit"}, {"local_chan_reserve", "local_chan_reserve"}, {"initiator", "initiator"}, {"local_base_fee", "local_base_fee"}, {"local_fee_rate", "local_fee_rate"}, {"remote_base_fee", "remote_base_fee"}, {"remote_fee_rate", "remote_fee_rate"}, {"is_active", "is_active"}, {"is_open", "is_open"}, {"num_updates", "num_updates"}, {"local_disabled", "local_disabled"}, {"remote_disabled", "remote_disabled"}, {"last_update", "last_update"}, {"short_chan_id", "short_chan_id"}, {"total_sent", "total_sent"}, {"total_received", "total_received"}, {"private", "private"}, {"pending_outbound", "pending_outbound"}, {"pending_inbound", "pending_inbound"}, {"htlc_count", "htlc_count"}, {"local_cltv", "local_cltv"}, {"local_min_htlc_msat", "local_min_htlc_msat"}, {"local_max_htlc_msat", "local_max_htlc_msat"}, {"remote_cltv", "remote_cltv"}, {"remote_min_htlc_msat", "remote_min_htlc_msat"}, {"remote_max_htlc_msat", "remote_max_htlc_msat"}, {"alias", "alias"}, {"fees_updated", "fees_updated"}, {"push_amt", "push_amt"}, {"close_address", "close_address"}, {"opened_in", "opened_in"}, {"ar_max_cost", "ar_max_cost"}, {"ar_amt_target", "ar_amt_target"}, {"ar_out_target", "ar_out_target"}, {"ar_in_target", "ar_in_target"}, {"auto_fees", "auto_fees"}, {"local_inbound_base_fee", "local_inbound_base_fee"}, {"local_inbound_fee_rate", "local_inbound_fee_rate"}, {"inbound_offset", "inbound_offset"}, {"offset_updated", "offset_updated"}, {"maxhtlc_percent", "maxhtlc_percent"}, {"maxhtlc_updated", "maxhtlc_updated"}, {"mx_liq_threshold", "mx_liq_threshold"}, {"mx_liq_value", "mx_liq_value"}, {"mx_liq_upper", "mx_liq_upper"}, {"remote_inbound_base_fee", "remote_inbound_base_fee"}, {"remote_inbound_fee_rate", "remote_inbound_fee_rate"}, {"auto_rebalance", "auto_rebalance"}, {"ar_source", "ar_source"}, {"ar_source_ppm_diff", "ar_source_ppm_diff"}, {"ep_enabled", "ep_enabled"}, {"ep_target", "ep_target"}, {"ep_inc_pct", "ep_inc_pct"}, {"ep_cooldown", "ep_cooldown"}, {"ep_live_threshold", "ep_live_threshold"}, {"ep_live_inc_pct", "ep_live_inc_pct"}, {"flp_enabled", "flp_enabled"}, {"flp_safety", "flp_safety"}, {"ep_updated", "ep_updated"}, {"htlc_boost_checked", "htlc_boost_checked"}, {"notes", "notes"},
	}},
	"rebalancer": {pk: "id", fields: []drfField{
		{"id", "id"}, {"requested", "requested"}, {"start", "start"}, {"stop", "stop"}, {"fees_paid", "fees_paid"}, {"payment_hash", "payment_hash"}, {"value", "value"}, {"fee_limit", "fee_limit"}, {"outgoing_chan_ids", "outgoing_chan_ids"}, {"last_hop_pubkey", "last_hop_pubkey"}, {"target_alias", "target_alias"}, {"duration", "duration"}, {"status", "status"}, {"manual", "manual"},
	}},
	"settings": {pk: "key", fields: []drfField{
		{"key", "key"}, {"value", "value"},
	}},
	"pendinghtlcs": {pk: "id", fields: []drfField{
		{"id", "id"}, {"chan_id", "chan_id"}, {"alias", "alias"}, {"incoming", "incoming"}, {"amount", "amount"}, {"hash_lock", "hash_lock"}, {"expiration_height", "expiration_height"}, {"forwarding_channel", "forwarding_channel"}, {"forwarding_alias", "forwarding_alias"},
	}},
	"failedhtlcs": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"amount", "amount"}, {"chan_id_in", "chan_id_in"}, {"chan_id_out", "chan_id_out"}, {"chan_in_alias", "chan_in_alias"}, {"chan_out_alias", "chan_out_alias"}, {"chan_out_liq", "chan_out_liq"}, {"chan_out_pending", "chan_out_pending"}, {"wire_failure", "wire_failure"}, {"failure_detail", "failure_detail"}, {"missed_fee", "missed_fee"},
	}},
	"peerevents": {pk: "id", fields: []drfField{
		{"id", "id"}, {"out_liq_percent", "out_liq_percent"}, {"timestamp", "timestamp"}, {"chan_id", "chan_id"}, {"peer_alias", "peer_alias"}, {"event", "event"}, {"old_value", "old_value"}, {"new_value", "new_value"}, {"out_liq", "out_liq"},
	}},
	"feelog": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"chan_id", "chan_id"}, {"peer_alias", "peer_alias"}, {"setting", "setting"}, {"old_value", "old_value"}, {"new_value", "new_value"},
	}},
	"inboundfeelog": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"chan_id", "chan_id"}, {"peer_alias", "peer_alias"}, {"setting", "setting"}, {"old_value", "old_value"}, {"new_value", "new_value"},
	}},
	"rebalanceroutes": {pk: "id", fields: []drfField{
		{"id", "id"}, {"success_ratio", "success_ratio"}, {"weighted_ratio", "weighted_ratio"}, {"last_failure", "last_failure"}, {"target_alias", "target_alias"}, {"outgoing_alias", "outgoing_alias"}, {"target_pubkey", "target_pubkey"}, {"outgoing_chan_id", "outgoing_chan_id"}, {"route", "route"}, {"route_hex", "route_hex"}, {"final_cltv_delta", "final_cltv_delta"}, {"success_count", "success_count"}, {"failure_count", "failure_count"}, {"last_success", "last_success"}, {"last_fee_ppm", "last_fee_ppm"},
	}},
	"graphevents": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"event_type", "event_type"}, {"chan_id", "chan_id"}, {"capacity", "capacity"}, {"fee_ppm", "fee_ppm"}, {"base_fee_msat", "base_fee_msat"}, {"target_pubkey", "target_pubkey"}, {"target_alias", "target_alias"}, {"other_node", "other_node"}, {"other_alias", "other_alias"}, {"disabled", "disabled"}, {"probe_triggered", "probe_triggered"}, {"routes_found", "routes_found"}, {"policy_node", "policy_node"},
	}},
	"probelogs": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"targets_scanned", "targets_scanned"}, {"routes_found", "routes_found"}, {"routes_existing", "routes_existing"}, {"errors", "errors"}, {"duration_ms", "duration_ms"}, {"details", "details"},
	}},
	"graphprobelogs": {pk: "id", fields: []drfField{
		{"id", "id"}, {"timestamp", "timestamp"}, {"target_pubkey", "target_pubkey"}, {"target_alias", "target_alias"}, {"target_fee", "target_fee"}, {"target_max_cost", "target_max_cost"}, {"trigger_chan_id", "trigger_chan_id"}, {"other_pubkey", "other_pubkey"}, {"other_alias", "other_alias"}, {"other_fee_ppm", "other_fee_ppm"}, {"budget_ppm", "budget_ppm"}, {"sources_tried", "sources_tried"}, {"routes_new", "routes_new"}, {"routes_existing", "routes_existing"}, {"errors", "errors"}, {"routes_via_new_peer", "routes_via_new_peer"}, {"rebalance_scheduled", "rebalance_scheduled"}, {"details", "details"},
	}},
}

// shapeDRF reorders a row map into the DRF serializer shape of prefix.
// ViewSets without a shape are returned unchanged.
func shapeDRF(r *http.Request, prefix string, m *orderedMap) *orderedMap {
	shape, ok := drfShapes[prefix]
	if !ok {
		return m
	}
	out := newOrderedMap()
	out.Set("url", detailURL(r, prefix, m.values[shape.pk]))
	for _, f := range shape.fields {
		out.Set(f.out, m.values[f.src])
	}
	return out
}

// detailURL builds the absolute detail URL of an object like DRF's reverse():
// the request's scheme and host, and a ?format= query kept from the request.
func detailURL(r *http.Request, prefix string, pk any) string {
	u := requestScheme(r) + "://" + r.Host + "/api/" + prefix + "/" + url.PathEscape(fmt.Sprint(pk)) + "/"
	if f := r.URL.Query().Get("format"); f != "" {
		u += "?format=" + url.QueryEscape(f)
	}
	return u
}

// requestScheme is the scheme the client used (X-Forwarded-Proto behind a proxy).
func requestScheme(r *http.Request) string {
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// registerViewSet mounts the list route and the detail route of a ViewSet.
// model is the Django model name used in the 404 message, pkCol the primary
// key column as it appears in spec.fromExpr.
func registerViewSet[T any](api chi.Router, dbtx db.DBTX, prefix, model, pkCol string, spec listSpec, toResult func(*T) *orderedMap) {
	result := func(r *http.Request, x *T) *orderedMap { return shapeDRF(r, prefix, toResult(x)) }
	apiGet(api, prefix, listHandler(dbtx, spec, result))
	api.Get("/"+prefix+"/{pk}/", detailHandler(dbtx, spec, model, pkCol, result, detailExtras[prefix]))
}

// detailExtras adds fields a ViewSet's retrieve() puts on top of the serializer.
var detailExtras = map[string]func(ctx context.Context, dbtx db.DBTX, m *orderedMap) error{
	"rebalanceroutes": addRouteHops,
}

// addRouteHops is RebalanceRouteViewSet.retrieve: one entry per pubkey of the
// route with the peer alias ("" for an unknown peer, null for a peer without one).
func addRouteHops(ctx context.Context, dbtx db.DBTX, m *orderedMap) error {
	route, _ := m.values["route"].(string)
	pubkeys := strings.Split(route, "-")
	rows, err := dbtx.Query(ctx, `SELECT pubkey, alias FROM gui_peers WHERE pubkey = ANY($1)`, pubkeys)
	if err != nil {
		return err
	}
	aliases := map[string]any{}
	for rows.Next() {
		var pk string
		var alias *string
		if err := rows.Scan(&pk, &alias); err != nil {
			rows.Close()
			return err
		}
		if alias != nil {
			aliases[pk] = *alias
		} else {
			aliases[pk] = nil
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	hops := make([]*orderedMap, 0, len(pubkeys))
	for i, pk := range pubkeys {
		alias, known := aliases[pk]
		if !known {
			alias = ""
		}
		hops = append(hops, newOrderedMap().Set("attempt_id", 1).Set("step", i+1).Set("alias", alias).Set("pubkey", pk))
	}
	m.Set("hops", hops)
	return nil
}

// detailHandler answers GET /api/<prefix>/<pk>/ with one object, or 404 with
// Django's get_object_or_404 message.
func detailHandler[T any](dbtx db.DBTX, spec listSpec, model, pkCol string, toResult func(*http.Request, *T) *orderedMap,
	extra func(context.Context, db.DBTX, *orderedMap) error) http.HandlerFunc {
	one := listSpec{selectExpr: spec.selectExpr, fromExpr: spec.fromExpr}
	return func(w http.ResponseWriter, r *http.Request) {
		items, _, err := executeList[T](r.Context(), dbtx, one,
			[]string{pkCol + "::text = $1"}, []any{chi.URLParam(r, "pk")}, pagination{})
		if err != nil {
			writeDRFError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(items) == 0 {
			writeDRFError(w, http.StatusNotFound, "No "+model+" matches the given query.")
			return
		}
		res := toResult(r, &items[0])
		if extra != nil {
			if err := extra(r.Context(), dbtx, res); err != nil {
				writeDRFError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}
		writeJSON(w, http.StatusOK, res)
	}
}
