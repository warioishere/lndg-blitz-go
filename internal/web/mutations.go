package web

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/walletrpc"
)

// handleBumpFee calls walletrpc.BumpFee on the specified outpoint.
func (s *Server) handleBumpFee(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Txid      *string `json:"txid"`
		Index     *int64  `json:"index"`
		TargetFee *int64  `json:"target_fee"`
		Force     *bool   `json:"force"`
	}
	if !decodeJSON(r, &body) || body.Txid == nil || body.Index == nil || body.TargetFee == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	force := body.Force != nil && *body.Force
	_, err := s.lnd.Wallet.BumpFee(r.Context(), &walletrpc.BumpFeeRequest{
		Outpoint:    &lnrpc.OutPoint{TxidStr: *body.Txid, OutputIndex: uint32(*body.Index)},
		SatPerVbyte: uint64(*body.TargetFee),
		Force:       force,
	})
	if err != nil {
		writeAPIError(w, "Fee bump failed! Error: "+grpcErrorMsg(err))
		return
	}
	msg := "Fee bumped to " + strconv.FormatInt(*body.TargetFee, 10) + " sats/vbyte for outpoint: " +
		*body.Txid + ":" + strconv.FormatInt(*body.Index, 10)
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", msg))
}

// handleBroadcastTx publishes a raw transaction via walletrpc.PublishTransaction.
func (s *Server) handleBroadcastTx(w http.ResponseWriter, r *http.Request) {
	var body struct {
		RawTx *string `json:"raw_tx"`
	}
	if !decodeJSON(r, &body) || body.RawTx == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	raw, err := hex.DecodeString(*body.RawTx)
	if err != nil {
		writeAPIError(w, "TX broadcast failed! Error: "+err.Error())
		return
	}
	resp, err := s.lnd.Wallet.PublishTransaction(r.Context(), &walletrpc.Transaction{TxHex: raw})
	if err != nil {
		writeAPIError(w, "TX broadcast failed! Error: "+grpcErrorMsg(err))
		return
	}
	if resp.GetPublishError() == "" {
		writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "Successfully broadcast tx!"))
		return
	}
	writeAPIError(w, "Error while broadcasting TX: "+resp.GetPublishError())
}

// decodeJSON decodes the request body into v; returns false on error.
func decodeJSON(r *http.Request, v any) bool {
	return json.NewDecoder(r.Body).Decode(v) == nil
}

// handleConnectPeer connects to a peer by pubkey alone or by pubkey@host string.
func (s *Server) handleConnectPeer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID *string `json:"peer_id"`
	}
	if !decodeJSON(r, &body) || body.PeerID == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	peerID := strings.TrimSpace(*body.PeerID)
	if peerID == "" || len(peerID) > 200 { // CharField required, max_length=200
		writeAPIError(w, "Invalid request!")
		return
	}

	ctx := r.Context()
	var peerPubkey, host string
	switch {
	case strings.Count(peerID, "@") == 0 && len(peerID) == 66:
		peerPubkey = peerID
		node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, peerPubkey)
		if err != nil {
			writeAPIError(w, "Connection request failed! Error: "+grpcErrorMsg(err))
			return
		}
		addrs := node.GetNode().GetAddresses()
		if len(addrs) == 0 {
			writeAPIError(w, "Connection request failed! Error: list index out of range")
			return
		}
		host = addrs[0].GetAddr()
	case strings.Count(peerID, "@") == 1 && len(strings.SplitN(peerID, "@", 2)[0]) == 66:
		parts := strings.SplitN(peerID, "@", 2)
		peerPubkey, host = parts[0], parts[1]
	default:
		writeAPIError(w, "Invalid peer pubkey or connection string.")
		return
	}

	_, err := s.lnd.Lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
		Addr: &lnrpc.LightningAddress{Pubkey: peerPubkey, Host: host},
	})
	if err != nil {
		writeAPIError(w, "Connection request failed! Error: "+grpcErrorMsg(err))
		return
	}
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "Connection successful!"))
}

// handleDisconnectPeer disconnects a peer by pubkey.
func (s *Server) handleDisconnectPeer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID *string `json:"peer_id"`
	}
	if !decodeJSON(r, &body) || body.PeerID == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	peerID := strings.TrimSpace(*body.PeerID)
	if peerID == "" || len(peerID) > 66 { // CharField required, max_length=66
		writeAPIError(w, "Invalid request!")
		return
	}
	if len(peerID) != 66 {
		writeAPIError(w, "Invalid peer pubkey.")
		return
	}

	ctx := r.Context()
	if _, err := s.lnd.Lightning.DisconnectPeer(ctx, &lnrpc.DisconnectPeerRequest{PubKey: peerID}); err != nil {
		writeAPIError(w, "Connection request failed! Error: "+grpcErrorMsg(err))
		return
	}
	// Mark the peer as disconnected if it exists; updating zero rows is fine.
	_, _ = s.db.Exec(ctx, `UPDATE gui_peers SET connected = false WHERE pubkey = $1`, peerID)
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "Disconnection successful!"))
}

// handleAddInvoice creates an LND invoice for the requested amount.
func (s *Server) handleAddInvoice(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value *int64 `json:"value"`
	}
	// value must be present and non-negative
	if !decodeJSON(r, &body) || body.Value == nil || *body.Value < 0 {
		writeAPIError(w, "Invalid request!")
		return
	}
	resp, err := s.lnd.Lightning.AddInvoice(r.Context(), &lnrpc.Invoice{Value: *body.Value})
	if err != nil {
		writeAPIError(w, "Invoice creation failed! Error: "+grpcErrorMsg(err))
		return
	}
	writeJSON(w, http.StatusOK, newOrderedMap().
		Set("message", "Invoice created!").
		Set("data", resp.GetPaymentRequest()))
}

// getNewAddress returns a new on-chain address. Uses p2tr (type 4) for LND >= 0.15
// unless legacy mode is requested, otherwise falls back to p2wkh (type 0).
func (s *Server) getNewAddress(ctx context.Context, legacy bool) (*lnrpc.NewAddressResponse, error) {
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		return nil, err
	}
	v := info.GetVersion()
	if len(v) > 4 {
		v = v[:4]
	}
	ver, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil, err
	}
	addrType := lnrpc.AddressType(0)
	if ver >= 0.15 && !legacy {
		addrType = lnrpc.AddressType(4)
	}
	return s.lnd.Lightning.NewAddress(ctx, &lnrpc.NewAddressRequest{Type: addrType})
}

// handleNewAddress returns a new deposit address. Invalid body returns 400.
func (s *Server) handleNewAddress(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Legacy *bool `json:"legacy"`
	}
	if !decodeJSON(r, &body) { // legacy is optional, defaults to false
		writeJSON(w, http.StatusBadRequest, newOrderedMap().Set("error", "Invalid request!"))
		return
	}
	legacy := body.Legacy != nil && *body.Legacy
	resp, err := s.getNewAddress(r.Context(), legacy)
	if err != nil {
		writeAPIError(w, "Address creation failed! Error: "+grpcErrorMsg(err))
		return
	}
	writeJSON(w, http.StatusOK, newOrderedMap().
		Set("message", "Retrieved new deposit address!").
		Set("data", resp.GetAddress()))
}

// handleConsolidateUtxos sweeps all UTXOs to a new address at the specified fee rate.
func (s *Server) handleConsolidateUtxos(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SatPerVbyte *int64 `json:"sat_per_vbyte"`
	}
	if !decodeJSON(r, &body) || body.SatPerVbyte == nil {
		writeJSON(w, http.StatusBadRequest, newOrderedMap().Set("error", "Invalid request!"))
		return
	}
	ctx := r.Context()
	addr, err := s.getNewAddress(ctx, false)
	if err != nil {
		writeAPIError(w, "Failed to consolidate utxos! Error: "+grpcErrorMsg(err))
		return
	}
	resp, err := s.lnd.Lightning.SendCoins(ctx, &lnrpc.SendCoinsRequest{
		Addr: addr.GetAddress(), SendAll: true, SatPerVbyte: uint64(*body.SatPerVbyte),
	})
	if err != nil {
		writeAPIError(w, "Failed to consolidate utxos! Error: "+grpcErrorMsg(err))
		return
	}
	txid := resp.GetTxid()
	writeJSON(w, http.StatusOK, newOrderedMap().
		Set("message", "Successfully consolidated UXTOs: "+txid).
		Set("txid", txid))
}

// resetTables maps model names to the database tables to delete. Child tables
// are listed first to satisfy foreign key constraints.
var resetTables = map[string][]string{
	"Forwards":        {"gui_forwards"},
	"Payments":        {"gui_paymenthops", "gui_payments"},
	"PaymentHops":     {"gui_paymenthops"},
	"Invoices":        {"gui_invoices"},
	"Rebalancer":      {"gui_rebalancer"},
	"Closures":        {"gui_closures"},
	"Resolutions":     {"gui_resolutions"},
	"Peers":           {"gui_peers"},
	"Channels":        {"gui_allowedtarget", "gui_channels"},
	"PendingChannels": {"gui_pendingchannels"},
	"Onchain":         {"gui_onchain"},
	"PendingHTLCs":    {"gui_pendinghtlcs"},
	"FailedHTLCs":     {"gui_failedhtlcs"},
	"HistFailedHTLC":  {"gui_histfailedhtlc"},
	"Autopilot":       {"gui_autopilot"},
	"Autofees":        {"gui_autofees"},
	"AvoidNodes":      {"gui_avoidnodes"},
	"PeerEvents":      {"gui_peerevents"},
	"LocalSettings":   {"gui_localsettings"},
}

// handleResetApi deletes all rows from the specified table.
func (s *Server) handleResetApi(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Table *string `json:"table"`
	}
	// table is a CharField with max_length=20
	if !decodeJSON(r, &body) || body.Table == nil || len(strings.TrimSpace(*body.Table)) > 20 {
		writeJSON(w, http.StatusOK, newOrderedMap().Set("error",
			map[string]string{"invalid": "Invalid data. Expected a dictionary, but got {datatype}."}))
		return
	}
	table := strings.TrimSpace(*body.Table)
	tbls, ok := resetTables[table]
	if !ok {
		writeAPIError(w, "Error deleting table: '"+table+"'")
		return
	}
	ctx := r.Context()
	for _, t := range tbls {
		if _, err := s.db.Exec(ctx, "DELETE FROM "+t); err != nil {
			writeAPIError(w, "Error deleting table: "+err.Error())
			return
		}
	}
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "Successfully deleted table: "+table))
}
