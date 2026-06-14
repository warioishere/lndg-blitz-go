package web

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// reverseHex reverses the byte order of b and returns the hex-encoded result.
func reverseHex(b []byte) string {
	r := make([]byte, len(b))
	for i := range b {
		r[i] = b[len(b)-1-i]
	}
	return hex.EncodeToString(r)
}

// handleOpenChannel connects to the peer if not already connected, then opens
// a channel and waits for the first chan_pending streaming update to confirm.
func (s *Server) handleOpenChannel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerPubkey *string `json:"peer_pubkey"`
		LocalAmt   *int64  `json:"local_amt"`
		SatPerByte *int64  `json:"sat_per_byte"`
	}
	if !decodeJSON(r, &body) || body.PeerPubkey == nil || body.LocalAmt == nil || body.SatPerByte == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	peerPubkey := strings.TrimSpace(*body.PeerPubkey)
	if peerPubkey == "" || len(peerPubkey) > 66 { // CharField required, max_length=66
		writeAPIError(w, "Invalid request!")
		return
	}
	ctx := r.Context()

	// Connect to peer if not already connected.
	var connected bool
	if err := s.db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM gui_peers WHERE pubkey=$1 AND connected=true)`, peerPubkey).
		Scan(&connected); err != nil {
		writeAPIError(w, "Channel creation failed! Error: "+err.Error())
		return
	}
	if !connected {
		node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, peerPubkey)
		if err != nil {
			writeAPIError(w, "Error connecting to new peer: "+grpcErrorMsg(err))
			return
		}
		addrs := node.GetNode().GetAddresses()
		if len(addrs) == 0 {
			writeAPIError(w, "Error connecting to new peer: list index out of range")
			return
		}
		if _, err := s.lnd.Lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
			Addr: &lnrpc.LightningAddress{Pubkey: peerPubkey, Host: addrs[0].GetAddr()},
		}); err != nil {
			writeAPIError(w, "Error connecting to new peer: "+grpcErrorMsg(err))
			return
		}
	}

	nodePubkey, err := hex.DecodeString(peerPubkey)
	if err != nil {
		writeAPIError(w, "Channel creation failed! Error: "+err.Error())
		return
	}
	stream, err := s.lnd.Lightning.OpenChannel(ctx, &lnrpc.OpenChannelRequest{
		NodePubkey:         nodePubkey,
		LocalFundingAmount: *body.LocalAmt,
		SatPerByte:         *body.SatPerByte,
	})
	if err != nil {
		writeAPIError(w, "Channel creation failed! Error: "+grpcErrorMsg(err))
		return
	}
	update, err := stream.Recv()
	if err != nil {
		writeAPIError(w, "Channel creation failed! Error: "+grpcErrorMsg(err))
		return
	}
	pending := update.GetChanPending()
	msg := "Channel created! Funding TXID: " + reverseHex(pending.GetTxid()) + ":" +
		strconv.FormatUint(uint64(pending.GetOutputIndex()), 10)
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", msg))
}

// handleCloseChannel looks up the channel by chan_id or short_chan_id, then
// initiates a force or graceful close and waits for the first close_pending update.
func (s *Server) handleCloseChannel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ChanID    *string `json:"chan_id"`
		TargetFee *int64  `json:"target_fee"`
		Force     *bool   `json:"force"`
	}
	if !decodeJSON(r, &body) || body.ChanID == nil || body.TargetFee == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	force := body.Force != nil && *body.Force
	chanID := *body.ChanID
	ctx := r.Context()

	var query string
	if strings.Count(chanID, "x") == 2 && len(chanID) >= 5 {
		query = `SELECT funding_txid, output_index FROM gui_channels WHERE short_chan_id=$1`
	} else {
		query = `SELECT funding_txid, output_index FROM gui_channels WHERE chan_id=$1`
	}
	var fundingTxid string
	var outputIndex int32
	if err := s.db.QueryRow(ctx, query, chanID).Scan(&fundingTxid, &outputIndex); err != nil {
		writeAPIError(w, "Channel ID is not valid.")
		return
	}

	// Use the string variant of the funding_txid oneof.
	cp := &lnrpc.ChannelPoint{
		FundingTxid: &lnrpc.ChannelPoint_FundingTxidStr{FundingTxidStr: fundingTxid},
		OutputIndex: uint32(outputIndex),
	}

	var req *lnrpc.CloseChannelRequest
	var label string
	if force {
		req = &lnrpc.CloseChannelRequest{ChannelPoint: cp, Force: true}
		label = "Channel force closed! Closing TXID: "
	} else {
		req = &lnrpc.CloseChannelRequest{ChannelPoint: cp, SatPerByte: *body.TargetFee}
		label = "Channel gracefully closed! Closing TXID: "
	}
	stream, err := s.lnd.Lightning.CloseChannel(ctx, req)
	if err != nil {
		writeAPIError(w, "Channel close failed! Error: "+grpcErrorMsg(err))
		return
	}
	update, err := stream.Recv()
	if err != nil {
		writeAPIError(w, "Channel close failed! Error: "+grpcErrorMsg(err))
		return
	}
	pending := update.GetClosePending()
	msg := label + reverseHex(pending.GetTxid()) + ":" +
		strconv.FormatUint(uint64(pending.GetOutputIndex()), 10)
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", msg))
}
