package web

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// mountActions registers the JSON action endpoints.
// Success responses: {"message":"success","data":...} with 200; errors: {"error":...} with 200.
func (s *Server) mountActions(api chi.Router) {
	api.Get("/getinfo/", s.handleGetInfo)
	api.Get("/getinfo", s.handleGetInfo)
	api.Get("/balances/", s.handleBalances)
	api.Get("/balances", s.handleBalances)
	api.Get("/rebalance_stats/", s.handleRebalanceStats)
	api.Get("/rebalance_stats", s.handleRebalanceStats)
	api.Get("/forwards_summary/", s.handleForwardsSummary)
	api.Get("/forwards_summary", s.handleForwardsSummary)
	api.Get("/node_info/", s.handleNodeInfo)
	api.Get("/node_info", s.handleNodeInfo)
	api.Get("/pendingchannels/", s.handlePendingChannels)
	api.Get("/pendingchannels", s.handlePendingChannels)
	api.Get("/income/", s.handleApiIncome)
	api.Get("/income", s.handleApiIncome)
	api.Get("/chart/", s.handleChart)
	api.Get("/chart", s.handleChart)
	api.Post("/sign_message/", s.handleSignMessage)
	api.Post("/sign_message", s.handleSignMessage)
	// a plain Django view, not DRF: checks the method itself
	api.HandleFunc("/amboss_channel_fees/", s.handleAmbossChannelFeeHistory)
	api.HandleFunc("/amboss_channel_fees", s.handleAmbossChannelFeeHistory)

	// Mutating endpoints (POST).
	postAction(api, "connectpeer", s.handleConnectPeer)
	postAction(api, "disconnectpeer", s.handleDisconnectPeer)
	postAction(api, "createinvoice", s.handleAddInvoice)
	postAction(api, "newaddress", s.handleNewAddress)
	postAction(api, "consolidate", s.handleConsolidateUtxos)
	postAction(api, "reset", s.handleResetApi)
	postAction(api, "bumpfee", s.handleBumpFee)
	postAction(api, "broadcast_tx", s.handleBroadcastTx)
	postAction(api, "openchannel", s.handleOpenChannel)
	postAction(api, "closechannel", s.handleCloseChannel)
	postAction(api, "chanpolicy", s.handleChanPolicy)
	// update_alias uses messages+redirect (not JSON) -> registered here rather than in Layer 4.
	postAction(api, "updatealias", s.handleUpdateAlias)
}

// postAction registers a POST handler at /api/<name>/ and /api/<name>.
func postAction(api chi.Router, name string, h http.HandlerFunc) {
	api.Post("/"+name+"/", h)
	api.Post("/"+name, h)
}

// forwardsSummarySQL aggregates forward activity per channel over 1-day and 7-day windows.
// Two groupings — by outgoing channel (chan_id_out) and incoming channel (chan_id_in) — are
// combined via UNION. Columns for the other direction are zero. HAVING retains only groups
// with activity. $1=filter_1day, $2=filter_7day.
const forwardsSummarySQL = `
SELECT chan_id_out AS chan_id,
  count(id) FILTER (WHERE forward_date >= $1) AS count_outgoing_1day,
  COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date >= $1), 0) AS sum_outgoing_1day,
  count(id) FILTER (WHERE forward_date >= $2) AS count_outgoing_7day,
  COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date >= $2), 0) AS sum_outgoing_7day,
  COALESCE(sum(fee) FILTER (WHERE forward_date >= $1), 0.0) AS sum_fees_1day,
  COALESCE(sum(fee) FILTER (WHERE forward_date >= $2), 0.0) AS sum_fees_7day,
  0 AS count_incoming_1day, 0 AS sum_incoming_1day, 0 AS count_incoming_7day, 0 AS sum_incoming_7day
FROM gui_forwards GROUP BY chan_id_out
HAVING count(id) FILTER (WHERE forward_date >= $1) > 0
    OR COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date >= $1), 0) > 0
    OR count(id) FILTER (WHERE forward_date >= $2) > 0
    OR COALESCE(sum(amt_out_msat) FILTER (WHERE forward_date >= $2), 0) > 0
    OR COALESCE(sum(fee) FILTER (WHERE forward_date >= $1), 0.0) > 0
    OR COALESCE(sum(fee) FILTER (WHERE forward_date >= $2), 0.0) > 0
UNION
SELECT chan_id_in AS chan_id,
  0, 0, 0, 0, 0, 0,
  count(id) FILTER (WHERE forward_date >= $1),
  COALESCE(sum(amt_in_msat) FILTER (WHERE forward_date >= $1), 0),
  count(id) FILTER (WHERE forward_date >= $2),
  COALESCE(sum(amt_in_msat) FILTER (WHERE forward_date >= $2), 0)
FROM gui_forwards GROUP BY chan_id_in
HAVING count(id) FILTER (WHERE forward_date >= $1) > 0
    OR COALESCE(sum(amt_in_msat) FILTER (WHERE forward_date >= $1), 0) > 0
    OR count(id) FILTER (WHERE forward_date >= $2) > 0
    OR COALESCE(sum(amt_in_msat) FILTER (WHERE forward_date >= $2), 0) > 0`

// handleForwardsSummary returns {"results": [...]} with per-channel forward aggregates.
func (s *Server) handleForwardsSummary(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	filter1day := now.Add(-24 * time.Hour)
	filter7day := now.Add(-7 * 24 * time.Hour)

	rows, err := s.db.Query(r.Context(), forwardsSummarySQL, filter1day, filter7day)
	if err != nil {
		writeAPIError(w, err.Error())
		return
	}
	defer rows.Close()

	results := make([]any, 0)
	for rows.Next() {
		var chanID string
		var cOut1, sOut1, cOut7, sOut7 int64
		var fees1, fees7 float64
		var cIn1, sIn1, cIn7, sIn7 int64
		if err := rows.Scan(&chanID, &cOut1, &sOut1, &cOut7, &sOut7, &fees1, &fees7, &cIn1, &sIn1, &cIn7, &sIn7); err != nil {
			writeAPIError(w, err.Error())
			return
		}
		results = append(results, newOrderedMap().
			Set("chan_id", chanID).
			Set("count_outgoing_1day", cOut1).
			Set("sum_outgoing_1day", sOut1).
			Set("count_outgoing_7day", cOut7).
			Set("sum_outgoing_7day", sOut7).
			Set("sum_fees_1day", fees1).
			Set("sum_fees_7day", fees7).
			Set("count_incoming_1day", cIn1).
			Set("sum_incoming_1day", sIn1).
			Set("count_incoming_7day", cIn7).
			Set("sum_incoming_7day", sIn7))
	}
	if err := rows.Err(); err != nil {
		writeAPIError(w, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, newOrderedMap().Set("results", results))
}

// handleGetInfo calls LND GetInfo and returns node details as a data object.
func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	resp, err := s.lnd.Lightning.GetInfo(r.Context(), &lnrpc.GetInfoRequest{})
	if err != nil {
		writeAPIError(w, "Failed to call getinfo! Error: "+grpcErrorMsg(err))
		return
	}
	chains := make([]*orderedMap, 0, len(resp.GetChains()))
	for _, c := range resp.GetChains() {
		chains = append(chains, newOrderedMap().Set("chain", c.GetChain()).Set("network", c.GetNetwork()))
	}
	uris := resp.GetUris()
	if uris == nil {
		uris = []string{}
	}
	data := newOrderedMap().
		Set("identity_pubkey", resp.GetIdentityPubkey()).
		Set("alias", resp.GetAlias()).
		Set("num_active_channels", resp.GetNumActiveChannels()).
		Set("num_peers", resp.GetNumPeers()).
		Set("block_height", resp.GetBlockHeight()).
		Set("block_hash", resp.GetBlockHash()).
		Set("synced_to_chain", resp.GetSyncedToChain()).
		Set("testnet", resp.GetTestnet()).
		Set("uris", uris).
		Set("best_header_timestamp", resp.GetBestHeaderTimestamp()).
		Set("version", resp.GetVersion()).
		Set("num_inactive_channels", resp.GetNumInactiveChannels()).
		Set("chains", chains).
		Set("color", resp.GetColor()).
		Set("synced_to_graph", resp.GetSyncedToGraph())
	writeSuccess(w, data)
}

// handleBalances returns wallet and channel balances. Offchain balance is
// sum(local_balance) + sum(pending_outbound) over open channels, plus
// pending-open and limbo balances. COALESCE avoids errors when no channels exist.
func (s *Server) handleBalances(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	balances, err := s.lnd.Lightning.WalletBalance(ctx, &lnrpc.WalletBalanceRequest{})
	if err != nil {
		writeAPIError(w, "Failed to get wallet balances! Error: "+grpcErrorMsg(err))
		return
	}
	pending, err := s.lnd.Lightning.PendingChannels(ctx, &lnrpc.PendingChannelsRequest{})
	if err != nil {
		writeAPIError(w, "Failed to get wallet balances! Error: "+grpcErrorMsg(err))
		return
	}
	limbo := pending.GetTotalLimboBalance()
	var pendingOpen int64
	for _, oc := range pending.GetPendingOpenChannels() {
		pendingOpen += oc.GetChannel().GetLocalBalance()
	}

	var sumLocal, sumPendingOut int64
	row := s.db.QueryRow(ctx,
		`SELECT COALESCE(sum(local_balance),0), COALESCE(sum(pending_outbound),0)
		 FROM gui_channels WHERE is_open = true`)
	if err := row.Scan(&sumLocal, &sumPendingOut); err != nil {
		writeAPIError(w, "Failed to get wallet balances! Error: "+err.Error())
		return
	}
	offchain := sumLocal + sumPendingOut + pendingOpen + limbo

	data := newOrderedMap().
		Set("total_balance", balances.GetTotalBalance()+offchain).
		Set("offchain_balance", offchain).
		Set("onchain_balance", balances.GetTotalBalance()).
		Set("confirmed_balance", balances.GetConfirmedBalance()).
		Set("unconfirmed_balance", balances.GetUnconfirmedBalance())
	writeSuccess(w, data)
}

// handleRebalanceStats returns rebalancer activity grouped by last_hop_pubkey
// for the last 7 days (stop > now-7d). Returns a bare list with no wrapper.
// successes is NULL when there are zero successful attempts.
func (s *Server) handleRebalanceStats(w http.ResponseWriter, r *http.Request) {
	filter7day := time.Now().Add(-7 * 24 * time.Hour)
	rows, err := s.db.Query(r.Context(),
		`SELECT last_hop_pubkey, count(last_hop_pubkey) AS attempts,
		        sum(CASE WHEN status = 2 THEN 1 END) AS successes
		 FROM gui_rebalancer WHERE stop > $1 GROUP BY last_hop_pubkey`, filter7day)
	if err != nil {
		writeAPIError(w, "Unable to fetch stats! Error: "+err.Error())
		return
	}
	defer rows.Close()

	results := make([]any, 0)
	for rows.Next() {
		var pubkey string
		var attempts int64
		var successes pgtype.Int8
		if err := rows.Scan(&pubkey, &attempts, &successes); err != nil {
			writeAPIError(w, "Unable to fetch stats! Error: "+err.Error())
			return
		}
		results = append(results, newOrderedMap().
			Set("last_hop_pubkey", pubkey).
			Set("attempts", attempts).
			Set("successes", successes))
	}
	if err := rows.Err(); err != nil {
		writeAPIError(w, "Unable to fetch stats! Error: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, results)
}
