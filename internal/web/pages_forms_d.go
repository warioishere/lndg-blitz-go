package web

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc/wtclientrpc"
	"github.com/warioishere/lndg-blitz-go/internal/pyround"
)

// handleAddTowerForm adds a watchtower by connection string (pubkey@host).
func (s *Server) handleAddTowerForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	tower := r.PostForm.Get("tower")
	if tower == "" {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	if !(strings.Count(tower, "@") == 1 && len(strings.SplitN(tower, "@", 2)[0]) == 66) {
		f.add("Add tower request failed! Error: Invalid tower connection string.")
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	parts := strings.SplitN(tower, "@", 2)
	pk, err := hex.DecodeString(parts[0])
	if err != nil {
		f.add("Add tower request failed! Error: " + err.Error())
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	if _, err := s.lnd.Watchtower.AddTower(ctx, &wtclientrpc.AddTowerRequest{Pubkey: pk, Address: parts[1]}); err != nil {
		f.add("Add tower request failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	f.add("Tower addition successful!")
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleDeleteTowerForm removes a tower by pubkey and address.
func (s *Server) handleDeleteTowerForm(w http.ResponseWriter, r *http.Request) {
	s.removeTower(w, r, true)
}

// handleRemoveTowerForm removes a tower by pubkey only.
func (s *Server) handleRemoveTowerForm(w http.ResponseWriter, r *http.Request) {
	s.removeTower(w, r, false)
}

func (s *Server) removeTower(w http.ResponseWriter, r *http.Request, withAddress bool) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	pubkeyHex := r.PostForm.Get("pubkey")
	address := r.PostForm.Get("address")
	verb := "Remove tower request failed! Error: "
	okMsg := "Tower removal successful!"
	if withAddress {
		verb = "Delete tower request failed! Error: "
		okMsg = "Tower deletion successful!"
	}
	// DeleteTowerForm requires pubkey+address; RemoveTowerForm requires only pubkey.
	if pubkeyHex == "" || (withAddress && address == "") {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	pk, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		f.add(verb + err.Error())
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	req := &wtclientrpc.RemoveTowerRequest{Pubkey: pk}
	if withAddress {
		req.Address = address
	}
	if _, err := s.lnd.Watchtower.RemoveTower(ctx, req); err != nil {
		f.add(verb + grpcErrorMsg(err))
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	f.add(okMsg)
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleRebalanceForm creates a manual rebalancer request.
func (s *Server) handleRebalanceForm(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	value, e1 := strconv.ParseInt(r.PostForm.Get("value"), 10, 64)
	feeLimitIn, e2 := strconv.ParseFloat(r.PostForm.Get("fee_limit"), 64)
	duration, e3 := strconv.ParseInt(r.PostForm.Get("duration"), 10, 64)
	lastHop := r.PostForm.Get("last_hop_pubkey")
	outgoing := r.PostForm["outgoing_chan_ids"]
	if e1 != nil || e2 != nil || e3 != nil {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	// Each chan_id must correspond to an open, active channel.
	for _, cid := range outgoing {
		var ok bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_channels WHERE chan_id=$1 AND is_open=true AND is_active=true)`, cid).Scan(&ok); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			f.add(invalidRequest)
			s.redirect(w, r, refererOr(r, "/"), f)
			return
		}
	}

	// Target peer must exist (active+open) or be empty.
	targetValid := lastHop == ""
	if lastHop != "" {
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_channels WHERE is_active=true AND is_open=true AND remote_pubkey=$1)`, lastHop).Scan(&targetValid); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if !targetValid {
		f.add("Target peer is invalid or unknown.")
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	if len(outgoing) == 0 {
		f.add("You must select atleast one outgoing channel.")
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}

	targetAlias := ""
	if lastHop != "" {
		var alias, pubkey string
		if err := s.db.QueryRow(ctx, `SELECT COALESCE(alias,''), remote_pubkey FROM gui_channels WHERE is_active=true AND is_open=true AND remote_pubkey=$1 ORDER BY chan_id LIMIT 1`, lastHop).Scan(&alias, &pubkey); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if alias != "" {
			targetAlias = alias
		} else if len(pubkey) >= 12 {
			targetAlias = pubkey[:12]
		} else {
			targetAlias = pubkey
		}
	}
	feeLimit := pyround.Round(feeLimitIn*float64(value)*0.000001, 3)
	chanIDsStr := "[" + strings.Join(outgoing, ", ") + "]"
	if _, err := s.db.Exec(ctx,
		`INSERT INTO gui_rebalancer (requested, value, fee_limit, outgoing_chan_ids, last_hop_pubkey, target_alias, duration, status, manual)
		 VALUES (now(), $1, $2, $3, $4, $5, $6, 0, true)`,
		value, feeLimit, chanIDsStr, lastHop, targetAlias, duration); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.add("Rebalancer request created!")
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleUpdateAlias fetches the current node alias and writes it to all channels
// for that peer. Accepts either a form body or a JSON body.
func (s *Server) handleUpdateAlias(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	peerPubkey := r.PostForm.Get("peer_pubkey")
	if peerPubkey == "" {
		var body struct {
			PeerPubkey string `json:"peer_pubkey"`
		}
		if decodeJSON(r, &body) {
			peerPubkey = body.PeerPubkey
		}
	}
	if peerPubkey == "" {
		f.add(invalidRequest)
		s.redirect(w, r, "/", f)
		return
	}
	var exists bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_channels WHERE remote_pubkey=$1)`, peerPubkey).Scan(&exists); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !exists {
		f.add("Pubkey not in channels list.")
		s.redirect(w, r, "/", f)
		return
	}
	node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, peerPubkey)
	if err != nil {
		f.add("Error updating alias: " + grpcErrorMsg(err))
		s.redirect(w, r, "/", f)
		return
	}
	newAlias := node.GetNode().GetAlias()
	if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET alias=$2 WHERE remote_pubkey=$1`, peerPubkey, newAlias); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.add("Alias updated to: " + newAlias)
	s.redirect(w, r, "/", f)
}
