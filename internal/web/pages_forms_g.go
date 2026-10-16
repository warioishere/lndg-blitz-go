package web

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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

// batchPubkey normalises an entered pubkey: surrounding whitespace dropped and
// an address after "@" (pubkey@host:port) cut off. ok is false unless 66 hex
// characters remain.
func batchPubkey(raw string) (pubkey string, ok bool) {
	pubkey, _, _ = strings.Cut(strings.TrimSpace(raw), "@")
	if len(pubkey) != 66 {
		return pubkey, false
	}
	_, err := hex.DecodeString(pubkey)
	return pubkey, err == nil
}

// handleBatchOpen connects to up to 10 peers and opens channels via
// BatchOpenChannel. On success it redirects to /batch; on any error it renders
// the form again with the entries kept and the reasons listed.
func (s *Server) handleBatchOpen(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	_ = r.ParseForm()

	lines := make([]batchLine, batchLines)
	for i := range lines {
		n := strconv.Itoa(i + 1)
		lines[i] = batchLine{pubkey: strings.TrimSpace(r.PostForm.Get("pubkey" + n)), amt: strings.TrimSpace(r.PostForm.Get("amt" + n))}
	}
	feeRaw := strings.TrimSpace(r.PostForm.Get("fee_rate"))
	var msgs []string
	fail := func() { s.renderBatch(w, r, lines, feeRaw, msgs) }

	feeRate, err := strconv.ParseInt(feeRaw, 10, 64)
	if err != nil || feeRate < 1 {
		msgs = append(msgs, "Opening fee rate missing or invalid.")
	}
	type pair struct {
		pubkey string
		amt    int64
	}
	var candidates []pair
	var lineOf []int
	for i, l := range lines {
		if l.pubkey == "" && l.amt == "" {
			continue
		}
		n := i + 1
		pubkey, ok := batchPubkey(l.pubkey)
		amt, aerr := strconv.ParseInt(l.amt, 10, 64)
		switch {
		case l.pubkey == "":
			msgs = append(msgs, fmt.Sprintf("Line %d: pubkey missing.", n))
		case !ok:
			msgs = append(msgs, fmt.Sprintf("Line %d: the pubkey must be 66 hex characters (got %d).", n, len(pubkey)))
		case aerr != nil || amt <= 0:
			msgs = append(msgs, fmt.Sprintf("Line %d: amount missing or invalid.", n))
		default:
			candidates = append(candidates, pair{pubkey, amt})
			lineOf = append(lineOf, n)
		}
	}
	if len(msgs) > 0 {
		fail()
		return
	}
	if len(candidates) == 0 {
		msgs = append(msgs, "No channels specified!")
		fail()
		return
	}
	for k, p := range candidates {
		if !s.openPeer(ctx, p.pubkey) {
			msgs = append(msgs, fmt.Sprintf("Unable to connect with peer %d!", lineOf[k]))
		}
	}
	if len(msgs) > 0 {
		fail()
		return
	}
	channels := make([]*lnrpc.BatchOpenChannel, 0, len(candidates))
	for _, p := range candidates {
		pk, _ := hex.DecodeString(p.pubkey)
		channels = append(channels, &lnrpc.BatchOpenChannel{NodePubkey: pk, LocalFundingAmount: p.amt})
	}
	if _, err := s.lnd.Lightning.BatchOpenChannel(ctx, &lnrpc.BatchOpenChannelRequest{Channels: channels, SatPerVbyte: feeRate}); err != nil {
		msgs = append(msgs, "Batch open failed! Error: "+grpcErrorMsg(err))
		fail()
		return
	}
	f := &flasher{}
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
