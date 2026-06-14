package web

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// Batch C — LND-backed form endpoints that use flash messages and redirect.
// gRPC error details are extracted via grpcErrorMsg(err) (the gRPC status message).

// handleConnectPeerForm connects to a peer given either a pubkey or a
// pubkey@host connection string.
func (s *Server) handleConnectPeerForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	peerID := r.PostForm.Get("peer_id")
	if peerID == "" { // ConnectPeerForm: peer_id required
		f.add(invalidRequest)
		s.redirect(w, r, "/", f)
		return
	}

	var pubkey, host string
	switch {
	case strings.Count(peerID, "@") == 0 && len(peerID) == 66:
		pubkey = peerID
		node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, pubkey)
		if err != nil {
			f.add("Connection request failed! Error: " + grpcErrorMsg(err))
			s.redirect(w, r, "/", f)
			return
		}
		addrs := node.GetNode().GetAddresses()
		if len(addrs) == 0 {
			f.add("Connection request failed! Error: list index out of range")
			s.redirect(w, r, "/", f)
			return
		}
		host = addrs[0].GetAddr()
	case strings.Count(peerID, "@") == 1 && len(strings.SplitN(peerID, "@", 2)[0]) == 66:
		parts := strings.SplitN(peerID, "@", 2)
		pubkey, host = parts[0], parts[1]
	default:
		f.add("Connection request failed! Error: Invalid peer pubkey or connection string.")
		s.redirect(w, r, "/", f)
		return
	}

	if _, err := s.lnd.Lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
		Addr: &lnrpc.LightningAddress{Pubkey: pubkey, Host: host},
	}); err != nil {
		f.add("Connection request failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, "/", f)
		return
	}
	// An empty ConnectPeerResponse produces only the success prefix.
	f.add("Connection successful!")
	s.redirect(w, r, "/", f)
}

// handleOpenChannelForm connects to a peer if not already connected, then opens
// a channel.
func (s *Server) handleOpenChannelForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	peerPubkey := r.PostForm.Get("peer_pubkey")
	localAmt, e1 := strconv.ParseInt(r.PostForm.Get("local_amt"), 10, 64)
	satPerByte, e2 := strconv.ParseInt(r.PostForm.Get("sat_per_byte"), 10, 64)
	if peerPubkey == "" || len(peerPubkey) > 66 || e1 != nil || e2 != nil {
		f.add(invalidRequest)
		s.redirect(w, r, "/", f)
		return
	}

	// Connect if not already connected.
	var connected bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_peers WHERE pubkey=$1 AND connected=true)`, peerPubkey).Scan(&connected); err != nil {
		f.add("Channel creation failed! Error: " + err.Error())
		s.redirect(w, r, "/", f)
		return
	}
	if !connected {
		if node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, peerPubkey); err != nil {
			f.add("Error connecting to new peer: " + grpcErrorMsg(err))
		} else if addrs := node.GetNode().GetAddresses(); len(addrs) == 0 {
			f.add("Error connecting to new peer: list index out of range")
		} else if _, err := s.lnd.Lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
			Addr: &lnrpc.LightningAddress{Pubkey: peerPubkey, Host: addrs[0].GetAddr()},
		}); err != nil {
			f.add("Error connecting to new peer: " + grpcErrorMsg(err))
		} else {
			connected = true
		}
	}

	if connected {
		nodePubkey, err := hex.DecodeString(peerPubkey)
		if err != nil {
			f.add("Channel creation failed! Error: " + err.Error())
			s.redirect(w, r, "/", f)
			return
		}
		stream, err := s.lnd.Lightning.OpenChannel(ctx, &lnrpc.OpenChannelRequest{
			NodePubkey: nodePubkey, LocalFundingAmount: localAmt, SatPerByte: satPerByte,
		})
		if err != nil {
			f.add("Channel creation failed! Error: " + grpcErrorMsg(err))
			s.redirect(w, r, "/", f)
			return
		}
		update, err := stream.Recv()
		if err != nil {
			f.add("Channel creation failed! Error: " + grpcErrorMsg(err))
			s.redirect(w, r, "/", f)
			return
		}
		pending := update.GetChanPending()
		f.add("Channel created! Funding TXID: " + reverseHex(pending.GetTxid()) + ":" + strconv.FormatUint(uint64(pending.GetOutputIndex()), 10))
	}
	s.redirect(w, r, "/", f)
}

// handleCloseChannelForm closes a channel gracefully or by force.
func (s *Server) handleCloseChannelForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	chanID := r.PostForm.Get("chan_id")
	if chanID == "" {
		f.add(invalidRequest)
		s.redirect(w, r, "/", f)
		return
	}
	targetFee, _ := strconv.ParseInt(r.PostForm.Get("target_fee"), 10, 64) // required=False -> 0
	force := r.PostForm.Has("force")

	var query string
	if strings.Count(chanID, "x") == 2 && len(chanID) >= 5 {
		query = `SELECT funding_txid, output_index FROM gui_channels WHERE short_chan_id=$1`
	} else {
		query = `SELECT funding_txid, output_index FROM gui_channels WHERE chan_id=$1`
	}
	var fundingTxid string
	var outputIndex int32
	if err := s.db.QueryRow(ctx, query, chanID).Scan(&fundingTxid, &outputIndex); err != nil {
		f.add("Channel ID is not valid. Please try again.")
		s.redirect(w, r, "/", f)
		return
	}

	// Graceful close requires a fee rate; abort with an error message if missing.
	if !force && targetFee == 0 {
		f.add("Expected a fee rate for graceful closure. Please try again.")
		s.redirect(w, r, "/", f)
		return
	}

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
		req = &lnrpc.CloseChannelRequest{ChannelPoint: cp, SatPerByte: targetFee}
		label = "Channel gracefully closed! Closing TXID: "
	}
	stream, err := s.lnd.Lightning.CloseChannel(ctx, req)
	if err != nil {
		f.add("Channel close failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, "/", f)
		return
	}
	update, err := stream.Recv()
	if err != nil {
		f.add("Channel close failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, "/", f)
		return
	}
	pending := update.GetClosePending()
	f.add(label + reverseHex(pending.GetTxid()) + ":" + strconv.FormatUint(uint64(pending.GetOutputIndex()), 10))
	s.redirect(w, r, "/", f)
}

// handleAddInvoiceForm creates a Lightning invoice for the given value.
func (s *Server) handleAddInvoiceForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	value, err := strconv.ParseInt(r.PostForm.Get("value"), 10, 64)
	if err != nil { // AddInvoiceForm: value required IntegerField
		f.add(invalidRequest)
		s.redirect(w, r, "/", f)
		return
	}
	resp, err := s.lnd.Lightning.AddInvoice(ctx, &lnrpc.Invoice{Value: value})
	if err != nil {
		f.add("Invoice creation failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, "/", f)
		return
	}
	f.add("Invoice created! " + resp.GetPaymentRequest())
	s.redirect(w, r, "/", f)
}
