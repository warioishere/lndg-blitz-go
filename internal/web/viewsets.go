package web

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// mountAPI registers the read-only list endpoints under /api/. Only LIST
// endpoints (GET /api/<prefix>/) are provided — the frontend JS loads lists
// with filter/pagination via /api/; writes go through separate action endpoints.
// Trades are omitted.
func (s *Server) mountAPI(api chi.Router) {
	// payments — order -creation_date; +id (=index).
	apiGet(api, "payments", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiPayment](),
		fromExpr:   "gui_payments",
		order:      "creation_date DESC",
		filters: concatFilters(
			filterFields("status", filterInt, "exact", "lt", "gt"),
			filterFields("creation_date", filterDateTime, "lte", "gte"),
			filterFields("chan_out", filterString, "exact"),
			filterFields("index", filterInt, "lt"),
		),
		paginate: true,
	}, func(p *db.GuiPayment) *orderedMap {
		return structToOrderedMap(p).Set("id", p.Index)
	}))

	// paymenthops — no ordering or filters.
	apiGet(api, "paymenthops", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiPaymenthop](),
		fromExpr:   "gui_paymenthops",
		paginate:   true,
	}, plainResult[db.GuiPaymenthop]))

	// invoices — order -creation_date; +id (=index).
	apiGet(api, "invoices", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiInvoice](),
		fromExpr:   "gui_invoices",
		order:      "creation_date DESC",
		filters: concatFilters(
			filterFields("state", filterInt, "exact", "lt", "gt"),
			filterFields("is_revenue", filterBool, "exact"),
			filterFields("settle_date", filterDateTime, "gte"),
			filterFields("chan_in", filterString, "exact"),
			filterFields("index", filterInt, "lt"),
		),
		paginate: true,
	}, func(i *db.GuiInvoice) *orderedMap {
		return structToOrderedMap(i).Set("id", i.Index)
	}))

	// forwards — order -id; supports chan_in_or_out OR filter, forward_date, id__lt.
	apiGet(api, "forwards", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiForward](),
		fromExpr:   "gui_forwards",
		order:      "id DESC",
		filters: concatFilters(
			[]filterParam{orFilterField("chan_in_or_out", filterString, "chan_id_in", "chan_id_out")},
			filterFields("forward_date", filterDateTime, "lte", "gte", "lt", "gt"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, plainResult[db.GuiForward]))

	// onchain — filter time_stamp.
	apiGet(api, "onchain", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiOnchain](),
		fromExpr:   "gui_onchain",
		filters:    filterFields("time_stamp", filterDateTime, "lte", "gte"),
		paginate:   true,
	}, plainResult[db.GuiOnchain]))

	// closures — filter close_height.
	apiGet(api, "closures", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiClosure](),
		fromExpr:   "gui_closures",
		filters:    filterFields("close_height", filterInt, "lte", "gte"),
		paginate:   true,
	}, plainResult[db.GuiClosure]))

	// resolutions — no filters.
	apiGet(api, "resolutions", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiResolution](),
		fromExpr:   "gui_resolutions",
		paginate:   true,
	}, plainResult[db.GuiResolution]))

	// peers — no filters.
	apiGet(api, "peers", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiPeer](),
		fromExpr:   "gui_peers",
		paginate:   true,
	}, plainResult[db.GuiPeer]))

	// channels — filter is_open/private/is_active/auto_rebalance (bool, exact); +opened_in.
	apiGet(api, "channels", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiChannel](),
		fromExpr:   "gui_channels",
		filters: concatFilters(
			filterFields("is_open", filterBool, "exact"),
			filterFields("private", filterBool, "exact"),
			filterFields("is_active", filterBool, "exact"),
			filterFields("auto_rebalance", filterBool, "exact"),
		),
		paginate: true,
	}, func(c *db.GuiChannel) *orderedMap {
		return structToOrderedMap(c).Set("opened_in", openedIn(c.ShortChanID))
	}))

	// rebalancer — order -id.
	apiGet(api, "rebalancer", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiRebalancer](),
		fromExpr:   "gui_rebalancer",
		order:      "id DESC",
		filters: concatFilters(
			filterFields("status", filterInt, "lt", "gt", "exact"),
			filterFields("payment_hash", filterString, "exact"),
			filterFields("stop", filterDateTime, "gt"),
			filterFields("last_hop_pubkey", filterString, "exact"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, plainResult[db.GuiRebalancer]))

	// settings (LocalSettings) — no filters.
	apiGet(api, "settings", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiLocalsetting](),
		fromExpr:   "gui_localsettings",
		paginate:   true,
	}, plainResult[db.GuiLocalsetting]))

	// pendinghtlcs — no filters.
	apiGet(api, "pendinghtlcs", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiPendinghtlc](),
		fromExpr:   "gui_pendinghtlcs",
		paginate:   true,
	}, plainResult[db.GuiPendinghtlc]))

	// failedhtlcs — order -id; supports chan_in_or_out OR filter.
	apiGet(api, "failedhtlcs", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiFailedhtlc](),
		fromExpr:   "gui_failedhtlcs",
		order:      "id DESC",
		filters: concatFilters(
			[]filterParam{orFilterField("chan_in_or_out", filterString, "chan_id_in", "chan_id_out")},
			filterFields("chan_id_in", filterString, "exact"),
			filterFields("chan_id_out", filterString, "exact"),
			filterFields("wire_failure", filterInt, "lt", "gt"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, plainResult[db.GuiFailedhtlc]))

	// feelog (Autofees) — order -id; filter chan_id, id__lt.
	apiGet(api, "feelog", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiAutofee](),
		fromExpr:   "gui_autofees",
		order:      "id DESC",
		filters: concatFilters(
			filterFields("chan_id", filterString, "exact"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, plainResult[db.GuiAutofee]))

	// inboundfeelog — order -id; filter chan_id, id__lt.
	apiGet(api, "inboundfeelog", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiInboundfeelog](),
		fromExpr:   "gui_inboundfeelog",
		order:      "id DESC",
		filters: concatFilters(
			filterFields("chan_id", filterString, "exact"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, plainResult[db.GuiInboundfeelog]))

	// graphevents — no pagination; filter target_pubkey, event_type.
	apiGet(api, "graphevents", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiGraphevent](),
		fromExpr:   "gui_graphevent",
		filters: concatFilters(
			filterFields("target_pubkey", filterString, "exact"),
			filterFields("event_type", filterString, "exact"),
		),
		paginate: false,
	}, plainResult[db.GuiGraphevent]))

	// graphprobelogs — no pagination, no filters.
	apiGet(api, "graphprobelogs", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiGraphprobelog](),
		fromExpr:   "gui_graphprobelog",
		paginate:   false,
	}, plainResult[db.GuiGraphprobelog]))

	// probelog — details (jsonb) returned as raw JSON.
	apiGet(api, "probelogs", listHandler(s.db, listSpec{
		selectExpr: columnsOf[db.GuiProbelog](),
		fromExpr:   "gui_probelog",
		paginate:   true,
	}, probeLogToResult))

	// peerevents — order -id; out_liq_percent computed via JOIN on gui_channels.capacity.
	apiGet(api, "peerevents", listHandler(s.db, listSpec{
		selectExpr: "pe.id, pe.timestamp, pe.chan_id, pe.peer_alias, pe.event, pe.old_value, pe.new_value, pe.out_liq, c.capacity",
		fromExpr:   "gui_peerevents pe LEFT JOIN gui_channels c ON c.chan_id = pe.chan_id",
		order:      "pe.id DESC",
		filters: concatFilters(
			// chan_id is ambiguous in the JOIN — qualify it to pe.chan_id
			// while keeping "chan_id" as the query parameter name.
			qualify(filterFields("chan_id", filterString, "exact"), "pe.chan_id"),
			filterFields("id", filterInt, "lt"),
		),
		paginate: true,
	}, peerEventToResult))

	// rebalanceroutes — annotated ratios + alias subqueries; order target_alias, -weighted_ratio.
	apiGet(api, "rebalanceroutes", listHandler(s.db, listSpec{
		selectExpr: "r.id, r.target_pubkey, r.outgoing_chan_id, r.route, r.final_cltv_delta, " +
			"r.success_count, r.failure_count, r.last_success, r.last_failure, r.route_hex, r.last_fee_ppm, " +
			"p.alias AS target_alias, c.alias AS outgoing_alias",
		fromExpr: "gui_rebalanceroute r " +
			"LEFT JOIN gui_peers p ON p.pubkey = r.target_pubkey " +
			"LEFT JOIN gui_channels c ON c.chan_id = r.outgoing_chan_id",
		order: "target_alias, " + weightedRatioSQL("r.success_count", "r.failure_count") + " DESC",
		// target_pubkey/outgoing_chan_id exist only in gui_rebalanceroute — unambiguous without qualifier.
		filters: concatFilters(
			filterFields("target_pubkey", filterString, "exact"),
			filterFields("outgoing_chan_id", filterString, "exact"),
		),
		paginate: true,
	}, rebalanceRouteToResult))

	// nodereputation — explicit field list + weighted_ratio score + alias; order wr_score ASC.
	apiGet(api, "nodereputation", listHandler(s.db, listSpec{
		selectExpr: "nr.pubkey, nr.success_count, nr.failure_count, nr.last_success, nr.last_failure, p.alias",
		fromExpr:   "gui_nodereputation nr LEFT JOIN gui_peers p ON p.pubkey = nr.pubkey",
		order:      weightedRatioSQL("nr.success_count", "nr.failure_count"),
		paginate:   true,
	}, nodeReputationToResult))
}

// apiGet registers a list handler under /api/<prefix>/ and /api/<prefix>.
func apiGet(api chi.Router, prefix string, h http.HandlerFunc) {
	api.Get("/"+prefix+"/", h)
	api.Get("/"+prefix, h)
}

// concatFilters merges multiple filterParam slices into one.
func concatFilters(groups ...[]filterParam) []filterParam {
	var out []filterParam
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}

// qualify overrides the SQL column name for all params in the slice (e.g.
// "pe.chan_id" in a JOIN) while leaving the query parameter name unchanged.
// Use this for columns that are ambiguous in a multi-table query.
func qualify(params []filterParam, column string) []filterParam {
	for i := range params {
		params[i].column = column
	}
	return params
}

// plainResult maps a sqlc struct to the API JSON shape without extra fields.
func plainResult[T any](x *T) *orderedMap {
	return structToOrderedMap(x)
}

// openedIn extracts the block height from a short channel ID string
// (format: <block>x<tx>x<output>) by parsing the part before the first 'x'.
func openedIn(shortChanID string) int {
	first := shortChanID
	if idx := strings.IndexByte(shortChanID, 'x'); idx >= 0 {
		first = shortChanID[:idx]
	}
	n, _ := strconv.Atoi(first)
	return n
}
