package web

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// mountPages registers HTML page routes. Extended incrementally as new pages are added.
func (s *Server) mountPages(r chi.Router) {
	r.Get("/reset/", s.handleReset)
	r.Get("/payments", s.handlePayments)
	r.Get("/invoices", s.handleInvoices)
	r.Get("/peers", s.handlePeers)
	r.Get("/resolutions", s.handleResolutions)
	r.Get("/keysends/", s.handleKeysends)
	r.Get("/autopilot/", s.handleAutopilot)
	r.Get("/autofees/", s.handleOutboundFeeLog)
	r.Get("/inbound-fee-log/", s.handleInboundFeeLog)
	r.Get("/peerevents", s.handlePeerEvents)
	r.Get("/logs/", s.handleLogs)
	r.Get("/emergency-fees/", s.handleEmergencyFees)
	r.Get("/inbound-offset/", s.handleInboundOffset)
	r.Get("/auto-maxhtlc/", s.handleAutoMaxhtlc)
	r.Get("/full-fee-adj/", s.handleFullFeeAdj)
	r.Get("/fee-limit-protection/", s.handleFeeLimitProtection)
	r.Get("/fees/", s.handleFees)
	r.Get("/advanced/", s.handleAdvanced)
	r.Get("/advanced_rebalancing", s.handleAdvancedRebalancing)
	r.Get("/amboss-fees/", s.handleAmbossFees)
	r.Get("/batch", s.handleBatch)
	r.Get("/pending_htlcs", s.handlePendingHtlcs)
	r.Get("/addresses/", s.handleAddresses)
	r.Get("/rebalances", s.handleRebalances)
	r.Get("/route", s.handleRoute)
	r.Get("/routes", s.handleRoutes)
	r.Get("/forwards", s.handleForwards)
	r.Get("/failed_htlcs", s.handleFailedHtlcs)
	r.Get("/graphwatcher", s.handleGraphWatcher)
	r.Get("/rebalancing", s.handleRebalancing)
	r.Get("/rebalanceroutes", s.handleRebalanceRoutes)
	r.Get("/rebalanceroute/{id:[0-9]+}", s.handleRebalanceRouteDetail)
	r.Get("/towers", s.handleTowers)
	r.Get("/closures", s.handleClosures)
	r.Get("/balances", s.handleBalancesPage)
	r.Get("/income", s.handleIncome)
	r.Get("/opens/", s.handleOpens)
	r.Get("/unprofitable_channels/", s.handleUnprofitableChannels)
	r.Get("/actions/", s.handleActions)
	r.Get("/channels/", s.handleChannels)
	r.Get("/channel", s.handleChannelDetail)
	r.Get("/", s.handleHome)

	// Layer 6 — Forms / POST-Actions (Batch A: DB-only + redirect).
	r.Post("/update_closing/", s.handleUpdateClosing)
	r.Post("/update_keysend/", s.handleUpdateKeysend)
	r.Post("/add_avoid/", s.handleAddAvoid)
	r.Post("/remove_avoid/", s.handleRemoveAvoid)
	r.Post("/reset_node_reputation/", s.handleResetNodeReputation)
	r.Post("/reset/", s.handleResetPost)
	// Batch B: Channel/pending updates (LND policy + DB writes).
	r.Post("/update_channel/", s.handleUpdateChannel)
	r.Post("/update_pending/", s.handleUpdatePending)
	// Batch C: LND form endpoints (flash messages + redirect home).
	r.Post("/connectpeer/", s.handleConnectPeerForm)
	r.Post("/openchannel/", s.handleOpenChannelForm)
	r.Post("/closechannel/", s.handleCloseChannelForm)
	r.Post("/createinvoice/", s.handleAddInvoiceForm)
	// Batch D: Tower forms + rebalancer request.
	r.Post("/addtower/", s.handleAddTowerForm)
	r.Post("/deletetower/", s.handleDeleteTowerForm)
	r.Post("/removetower/", s.handleRemoveTowerForm)
	r.Post("/rebalancer/", s.handleRebalanceForm)
	// Batch E: Settings form.
	r.Post("/update_settings/", s.handleUpdateSettings)
	// Batch F: Bulk/LND POSTs.
	r.Post("/inbound-offset/", s.handleInboundOffsetPost)
	r.Post("/full-fee-adj/", s.handleFullFeeAdjPost)
	r.Post("/auto-maxhtlc/", s.handleAutoMaxhtlcPost)
	r.Post("/graphwatcher", s.handleGraphWatcherPost)
	r.Post("/update_setting/", s.handleUpdateSetting)
	r.Post("/batchopen/", s.handleBatchOpen)
	r.Post("/advanced_rebalancing", s.handleAdvancedRebalancingPost)
	r.Get("/get_fees/", s.handleGetFees)
}

// handleRebalances renders the rebalances page. The last_hop_pubkey query
// parameter is passed through to the template for filtering the rebalance tables.
func (s *Server) handleRebalances(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, r, "rebalances.html", map[string]any{
		"last_hop_pubkey": r.URL.Query().Get("last_hop_pubkey"),
	})
}

// renderError renders the error template with the given error message.
func (s *Server) renderError(w http.ResponseWriter, r *http.Request, errMsg string) {
	s.renderTemplate(w, r, "error.html", map[string]any{"error": errMsg})
}

// handlePayments renders the payments page with the last 150 non-failed payments.
// ppm is computed in Postgres as ROUND((fee*1000000)/value) to match banker's rounding.
func (s *Server) handlePayments(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT *, ROUND((fee*1000000)/value)::bigint AS ppm FROM gui_payments `+
			`WHERE status <> 3 ORDER BY creation_date DESC LIMIT 150`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "payments.html", map[string]any{"payments": rows})
}

// handleInvoices renders the invoices page with the last 150 settled invoices (state=1).
func (s *Server) handleInvoices(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT * FROM gui_invoices WHERE state = 1 ORDER BY creation_date DESC LIMIT 150`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "invoices.html", map[string]any{"invoices": rows})
}

// handlePeers renders the peers page with currently connected peers and their count.
func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(), `SELECT * FROM gui_peers WHERE connected = true`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "peers.html", map[string]any{"peers": rows, "num_peers": len(rows)})
}

// handleResolutions renders the resolutions page for a specific channel.
// The chan_id is taken from the first query parameter value (bare "?=<chan_id>" style).
func (s *Server) handleResolutions(w http.ResponseWriter, r *http.Request) {
	chanID := singleQueryParam(r)
	rows, err := s.queryMaps(r.Context(), `SELECT * FROM gui_resolutions WHERE chan_id = $1`, chanID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "resolutions.html", map[string]any{"chan_id": chanID, "resolutions": rows})
}

// resetPageTables is the ordered list of tables shown on the reset page,
// with their display name and the table used for the row count.
var resetPageTables = []struct{ name, table string }{
	{"Forwards", "gui_forwards"},
	{"Payments", "gui_payments"},
	{"PaymentHops", "gui_paymenthops"},
	{"Invoices", "gui_invoices"},
	{"Rebalancer", "gui_rebalancer"},
	{"Closures", "gui_closures"},
	{"Resolutions", "gui_resolutions"},
	{"Peers", "gui_peers"},
	{"Channels", "gui_channels"},
	{"PendingChannels", "gui_pendingchannels"},
	{"Onchain", "gui_onchain"},
	{"PendingHTLCs", "gui_pendinghtlcs"},
	{"FailedHTLCs", "gui_failedhtlcs"},
	{"HistFailedHTLC", "gui_histfailedhtlc"},
	{"Autopilot", "gui_autopilot"},
	{"Autofees", "gui_autofees"},
	{"AvoidNodes", "gui_avoidnodes"},
	{"PeerEvents", "gui_peerevents"},
	{"LocalSettings", "gui_localsettings"},
}

// handleReset renders the reset page with each table's name and row count.
func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tables := make([]map[string]any, 0, len(resetPageTables))
	for _, t := range resetPageTables {
		var count int64
		if err := s.db.QueryRow(ctx, "SELECT count(*) FROM "+t.table).Scan(&count); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tables = append(tables, map[string]any{"name": t.name, "count": count})
	}
	s.renderTemplate(w, r, "reset.html", map[string]any{"tables": tables})
}
