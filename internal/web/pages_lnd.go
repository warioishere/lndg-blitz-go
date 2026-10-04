package web

import (
	"encoding/hex"
	"net/http"
	"strings"

	"google.golang.org/grpc/status"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
)

// grpcCodeString formats a gRPC error like Python's str(e.code()), e.g.
// "StatusCode.DEADLINE_EXCEEDED"; error.html checks for known cases.
// Non-gRPC errors fall through to err.Error().
func grpcCodeString(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return err.Error()
	}
	var b strings.Builder
	name := st.Code().String() // "DeadlineExceeded"
	for i, c := range name {
		if i > 0 && c >= 'A' && c <= 'Z' && name[i-1] >= 'a' && name[i-1] <= 'z' {
			b.WriteByte('_')
		}
		b.WriteRune(c)
	}
	return "StatusCode." + strings.ToUpper(b.String())
}

// handleBatch renders the Batch-Open form for up to 10 channels plus the current
// on-chain balance. DB/LND errors return 500.
func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	balances, err := s.lnd.Lightning.WalletBalance(r.Context(), &lnrpc.WalletBalanceRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	iterator := make([]int, 10)
	for i := range iterator {
		iterator[i] = i + 1
	}
	s.renderTemplate(w, r, "batch.html", map[string]any{
		"iterator": iterator,
		"balances": map[string]any{"total_balance": balances.GetTotalBalance()},
	})
}

// handlePendingHtlcs renders pending in/outgoing HTLCs with blocks_til_expiration
// and hours_til_expiration relative to the current block height. LND errors return 500.
func (s *Server) handlePendingHtlcs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	blockHeight := int64(info.GetBlockHeight())

	const sql = `SELECT *, (expiration_height - $1) AS blocks_til_expiration,
            ((expiration_height - $1)*10)/60 AS hours_til_expiration
        FROM gui_pendinghtlcs WHERE incoming = $2 ORDER BY expiration_height`
	incoming, err := s.queryMaps(ctx, sql, blockHeight, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outgoing, err := s.queryMaps(ctx, sql, blockHeight, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "pending_htlcs.html", map[string]any{
		"incoming_htlcs": incoming,
		"outgoing_htlcs": outgoing,
	})
}

// handleBalancesPage renders the balances page: pending sweeps (walletrpc), UTXOs
// (ListUnspent), and on-chain transactions (unconfirmed first, then by -block_height).
// RPC errors return 500. (handleBalances is the api/balances endpoint.)
func (s *Server) handleBalancesPage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sweepsResp, err := s.lnd.Wallet.PendingSweeps(ctx, &walletrpc.PendingSweepsRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var sweeps []map[string]any
	for _, ps := range sweepsResp.GetPendingSweeps() {
		op := ps.GetOutpoint()
		sweeps = append(sweeps, map[string]any{
			"txid_str":                reverseHex(op.GetTxidBytes()),
			"txid_index":              op.GetOutputIndex(),
			"amount_sat":              ps.GetAmountSat(),
			"witness_type":            int32(ps.GetWitnessType()),
			"requested_sat_per_vbyte": ps.GetRequestedSatPerVbyte(),
			"sat_per_vbyte":           ps.GetSatPerVbyte(),
			"requested_conf_target":   ps.GetRequestedConfTarget(),
			"broadcast_attempts":      ps.GetBroadcastAttempts(),
			"next_broadcast_height":   ps.GetNextBroadcastHeight(),
			"force":                   ps.GetForce(),
		})
	}

	utxoResp, err := s.lnd.Wallet.ListUnspent(ctx, &walletrpc.ListUnspentRequest{MinConfs: 0, MaxConfs: 9999999})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var utxos []map[string]any
	for _, u := range utxoResp.GetUtxos() {
		op := u.GetOutpoint()
		utxos = append(utxos, map[string]any{
			"address":    u.GetAddress(),
			"amount_sat": u.GetAmountSat(),
			"outpoint": map[string]any{
				"txid_str":     op.GetTxidStr(),
				"output_index": op.GetOutputIndex(),
			},
			"confirmations": u.GetConfirmations(),
		})
	}

	// Unconfirmed transactions first, then confirmed ordered by -block_height.
	unconf, err := s.queryMaps(ctx, `SELECT * FROM gui_onchain WHERE block_height = 0`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	conf, err := s.queryMaps(ctx, `SELECT * FROM gui_onchain WHERE block_height <> 0 ORDER BY block_height DESC`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.renderTemplate(w, r, "balances.html", map[string]any{
		"pending_sweeps": sweeps,
		"utxos":          utxos,
		"transactions":   append(unconf, conf...),
	})
}

// handleTowers renders the watchtower page: stats plus active/inactive towers
// (one row per tower address, pubkey hex encoded). RPC errors render error.html.
func (s *Server) handleTowers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	resp, err := s.lnd.Watchtower.ListTowers(ctx, &wtclientrpc.ListTowersRequest{IncludeSessions: false})
	if err != nil {
		s.renderError(w, r, grpcCodeString(err))
		return
	}
	var active, inactive []map[string]any
	for _, item := range resp.GetTowers() {
		for _, address := range item.GetAddresses() {
			tower := map[string]any{
				"pubkey":       hex.EncodeToString(item.GetPubkey()),
				"address":      address,
				"active":       item.GetActiveSessionCandidate(),
				"num_sessions": item.GetNumSessions(),
			}
			if item.GetActiveSessionCandidate() {
				active = append(active, tower)
			} else {
				inactive = append(inactive, tower)
			}
		}
	}
	stats, err := s.lnd.Watchtower.Stats(ctx, &wtclientrpc.StatsRequest{})
	if err != nil {
		s.renderError(w, r, grpcCodeString(err))
		return
	}
	s.renderTemplate(w, r, "towers.html", map[string]any{
		"active_towers":   active,
		"inactive_towers": inactive,
		"stats": map[string]any{
			"num_backups":            stats.GetNumBackups(),
			"num_pending_backups":    stats.GetNumPendingBackups(),
			"num_failed_backups":     stats.GetNumFailedBackups(),
			"num_sessions_acquired":  stats.GetNumSessionsAcquired(),
			"num_sessions_exhausted": stats.GetNumSessionsExhausted(),
		},
	})
}

// handleAddresses renders the on-chain addresses page, grouped by account
// (ListAddresses via walletrpc). RPC errors render error.html.
func (s *Server) handleAddresses(w http.ResponseWriter, r *http.Request) {
	resp, err := s.lnd.Wallet.ListAddresses(r.Context(), &walletrpc.ListAddressesRequest{})
	if err != nil {
		s.renderError(w, r, grpcCodeString(err))
		return
	}
	accounts := make([]map[string]any, 0, len(resp.GetAccountWithAddresses()))
	for _, acc := range resp.GetAccountWithAddresses() {
		addrs := make([]map[string]any, 0, len(acc.GetAddresses()))
		for _, a := range acc.GetAddresses() {
			addrs = append(addrs, map[string]any{
				"is_internal": a.GetIsInternal(),
				"balance":     a.GetBalance(),
				"address":     a.GetAddress(),
			})
		}
		accounts = append(accounts, map[string]any{
			"address_type": int(acc.GetAddressType()),
			"addresses":    addrs,
		})
	}
	s.renderTemplate(w, r, "addresses.html", map[string]any{
		"address_data": map[string]any{"account_with_addresses": accounts},
	})
}
