package web

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"

	lnd "github.com/warioishere/lndg-blitz-go/internal/lnd"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// openPeer returns true if the peer is already connected or can be connected
// successfully.
func (s *Server) openPeer(ctx context.Context, pubkey string) bool {
	var connected bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_peers WHERE pubkey=$1 AND connected=true)`, pubkey).Scan(&connected); err == nil && connected {
		return true
	}
	node, err := lnd.GetNodeInfoCached(ctx, s.queries, s.lnd.Lightning, pubkey)
	if err != nil {
		return false
	}
	addrs := node.GetNode().GetAddresses()
	if len(addrs) == 0 {
		return false
	}
	if _, err := s.lnd.Lightning.ConnectPeer(ctx, &lnrpc.ConnectPeerRequest{
		Addr: &lnrpc.LightningAddress{Pubkey: pubkey, Host: addrs[0].GetAddr()},
	}); err != nil {
		return false
	}
	return true
}

// handleBatchOpen connects to up to 10 peers and opens channels via BatchOpenChannel.
// Redirects to /batch.
func (s *Server) handleBatchOpen(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()

	feeRate, err := strconv.ParseInt(r.PostForm.Get("fee_rate"), 10, 64)
	if err != nil { // fee_rate IntegerField required
		f.add(invalidRequest)
		s.redirect(w, r, "/batch", f)
		return
	}
	// amt_i must parse when present (form validation).
	type pair struct {
		pubkey string
		amt    int64
	}
	var openList []pair
	failed := false
	for i := 1; i <= 10; i++ {
		pubkey := r.PostForm.Get("pubkey" + strconv.Itoa(i))
		amtRaw := r.PostForm.Get("amt" + strconv.Itoa(i))
		var amt int64
		if amtRaw != "" {
			v, perr := strconv.ParseInt(amtRaw, 10, 64)
			if perr != nil {
				f.add(invalidRequest)
				s.redirect(w, r, "/batch", f)
				return
			}
			amt = v
		}
		if pubkey != "" && amt != 0 && len(pubkey) == 66 {
			if s.openPeer(ctx, pubkey) {
				openList = append(openList, pair{pubkey, amt})
			} else {
				failed = true
				f.add(fmt.Sprintf("Unable to connect with peer %d!", i))
			}
		}
	}
	if failed {
		s.redirect(w, r, "/batch", f)
		return
	}
	if len(openList) == 0 {
		f.add("No channels specified!")
		s.redirect(w, r, "/batch", f)
		return
	}
	channels := make([]*lnrpc.BatchOpenChannel, 0, len(openList))
	for _, p := range openList {
		pk, derr := hex.DecodeString(p.pubkey)
		if derr != nil {
			f.add("Batch open failed! Error: " + derr.Error())
			s.redirect(w, r, "/batch", f)
			return
		}
		channels = append(channels, &lnrpc.BatchOpenChannel{NodePubkey: pk, LocalFundingAmount: p.amt})
	}
	if _, err := s.lnd.Lightning.BatchOpenChannel(ctx, &lnrpc.BatchOpenChannelRequest{Channels: channels, SatPerVbyte: feeRate}); err != nil {
		f.add("Batch open failed! Error: " + grpcErrorMsg(err))
		s.redirect(w, r, "/batch", f)
		return
	}
	f.add("Batch opened channels!")
	s.redirect(w, r, "/batch", f)
}

// handleAdvancedRebalancingPost handles the Advanced Rebalancing POST with three
// actions: "Apply All" (bulk ar_source_ppm_diff), "Save Targets" (gui_allowedtarget),
// or default "Rebalance" (creates rebalancer requests per target). Redirects to
// /advanced_rebalancing.
func (s *Server) handleAdvancedRebalancingPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	const dest = "/advanced_rebalancing"

	action := r.PostForm.Get("action")
	if action == "" {
		action = "Rebalance"
	}
	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}

	if action == "Apply All" {
		ppmRaw := r.PostForm.Get("ppm_diff_value")
		if ppmRaw == "" {
			f.add("Please enter a PPM Diff value.")
			s.redirect(w, r, dest, f)
			return
		}
		ppm, err := strconv.Atoi(ppmRaw)
		if err != nil {
			f.add("Invalid PPM Diff value.")
			s.redirect(w, r, dest, f)
			return
		}
		if ppm < 0 {
			f.add("PPM Diff must be a positive number.")
			s.redirect(w, r, dest, f)
			return
		}
		tag, err := s.db.Exec(ctx, `UPDATE gui_channels SET ar_source_ppm_diff=$1 WHERE is_open=true AND auto_rebalance=true`, int32(ppm))
		if fail(err) {
			return
		}
		count := tag.RowsAffected()
		f.add(fmt.Sprintf("Applied PPM Diff (%d) to %d channel%s.", ppm, count, plural1(count)))
		s.redirect(w, r, dest, f)
		return
	}

	sourceChanID := r.PostForm.Get("source_chan_id")
	targets := r.PostForm["targets"]
	value, ok1 := advIntDefault(w, r.PostForm.Get("value"), 0)
	feeLimit, ok2 := advFloatDefault(w, r.PostForm.Get("fee_limit"), 0)
	duration, ok3 := advIntDefault(w, r.PostForm.Get("duration"), 1)
	if !ok1 || !ok2 || !ok3 {
		return
	}

	var srcFeeRate, srcPpmDiff int32
	var srcRemote string
	if err := s.db.QueryRow(ctx, `SELECT local_fee_rate, ar_source_ppm_diff, remote_pubkey FROM gui_channels WHERE chan_id=$1`, sourceChanID).Scan(&srcFeeRate, &srcPpmDiff, &srcRemote); err != nil {
		f.add("Invalid channel id.")
		s.redirect(w, r, dest, f)
		return
	}

	if action == "Save Targets" {
		if _, err := s.db.Exec(ctx, `DELETE FROM gui_allowedtarget WHERE source_chan_id=$1`, sourceChanID); fail(err) {
			return
		}
		for _, pubkey := range targets {
			if _, err := s.db.Exec(ctx, `INSERT INTO gui_allowedtarget (source_chan_id, target_pubkey) VALUES ($1, $2)`, sourceChanID, pubkey); fail(err) {
				return
			}
		}
		f.add("Allowed targets updated.")
		s.redirect(w, r, dest, f)
		return
	}

	// Default action (Rebalance): determine targets from heuristic if none provided.
	if len(targets) == 0 {
		rows, err := s.db.Query(ctx, `SELECT DISTINCT remote_pubkey FROM gui_channels
			WHERE is_open=true AND auto_rebalance=true AND local_fee_rate >= $1 AND chan_id <> $2 AND remote_pubkey <> $3`,
			srcFeeRate+srcPpmDiff, sourceChanID, srcRemote)
		if fail(err) {
			return
		}
		for rows.Next() {
			var pk string
			if err := rows.Scan(&pk); err != nil {
				rows.Close()
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			targets = append(targets, pk)
		}
		rows.Close()
		if fail(rows.Err()) {
			return
		}
	}

	count := 0
	for _, pubkey := range targets {
		var targetAlias string
		// alias of the first channel with this remote_pubkey, or ''.
		_ = s.db.QueryRow(ctx, `SELECT COALESCE(alias,'') FROM gui_channels WHERE remote_pubkey=$1 ORDER BY chan_id LIMIT 1`, pubkey).Scan(&targetAlias)
		// outgoing_chan_ids stored without quotes: "[2000]" so the rebalancer worker can parse it.
		if _, err := s.db.Exec(ctx,
			`INSERT INTO gui_rebalancer (requested, value, fee_limit, outgoing_chan_ids, last_hop_pubkey, target_alias, duration, status, manual)
			 VALUES (now(), $1, $2, $3, $4, $5, $6, 0, true)`,
			value, feeLimit, "["+sourceChanID+"]", pubkey, targetAlias, duration); fail(err) {
			return
		}
		count++
	}
	f.add(fmt.Sprintf("%d rebalance request%s created!", count, plural1(int64(count))))
	s.redirect(w, r, dest, f)
}

// plural1 returns "s" when n != 1, otherwise "".
func plural1(n int64) string {
	if n != 1 {
		return "s"
	}
	return ""
}

// advIntDefault parses an optional integer field: absent returns def; present but
// unparseable returns 500.
func advIntDefault(w http.ResponseWriter, raw string, def int64) (int64, bool) {
	if raw == "" {
		return def, true
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return 0, false
	}
	return v, true
}

func advFloatDefault(w http.ResponseWriter, raw string, def float64) (float64, bool) {
	if raw == "" {
		return def, true
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return 0, false
	}
	return v, true
}
