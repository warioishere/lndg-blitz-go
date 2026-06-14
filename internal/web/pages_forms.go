package web

import (
	"fmt"
	"net/http"
	"strconv"
)

// invalidRequest is the standard error message for form handlers.
const invalidRequest = "Invalid Request. Please try again."

// handleUpdateClosing updates the closing_costs of a closure record. Requires
// a valid form and an existing closure; on success flashes a confirmation.
// Redirects to the referrer.
func (s *Server) handleUpdateClosing(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	txid := r.PostForm.Get("funding_txid")
	idx, e1 := strconv.Atoi(r.PostForm.Get("funding_index"))
	target, e2 := strconv.Atoi(r.PostForm.Get("target"))

	done := false
	if txid != "" && e1 == nil && e2 == nil {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_closures WHERE funding_txid=$1 AND funding_index=$2)`, txid, idx).Scan(&exists); err == nil && exists {
			if _, err := s.db.Exec(ctx, `UPDATE gui_closures SET closing_costs=$1 WHERE funding_txid=$2 AND funding_index=$3`, target, txid, idx); err == nil {
				f.add("Updated closing costs for " + txid + ":" + strconv.Itoa(idx) + " updated to a value of: " + strconv.Itoa(target))
				done = true
			}
		}
	}
	if !done {
		f.add(invalidRequest)
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleUpdateKeysend toggles is_revenue on an invoice. Requires a valid form
// and an existing invoice; on success flashes a marked/unmarked confirmation.
func (s *Server) handleUpdateKeysend(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	rHash := r.PostForm.Get("r_hash")

	done := false
	if rHash != "" {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_invoices WHERE r_hash=$1)`, rHash).Scan(&exists); err == nil && exists {
			var isRev bool
			if err := s.db.QueryRow(ctx, `UPDATE gui_invoices SET is_revenue = NOT is_revenue WHERE r_hash=$1 RETURNING is_revenue`, rHash).Scan(&isRev); err == nil {
				word := "Unmarked"
				if isRev {
					word = "Marked"
				}
				f.add(word + " invoice " + rHash + " as revenue.")
				done = true
			}
		}
	}
	if !done {
		f.add(invalidRequest)
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleAddAvoid adds a node to the avoid list (gui_avoidnodes).
func (s *Server) handleAddAvoid(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	pubkey := r.PostForm.Get("pubkey")
	notes := r.PostForm.Get("notes") // required=False -> '' when absent

	done := false
	if pubkey != "" {
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_avoidnodes (pubkey, notes, updated) VALUES ($1, $2, now())`, pubkey, notes); err == nil {
			f.add("Successfully added node " + pubkey + " to the avoid list.")
			done = true
		}
	}
	if !done {
		f.add(invalidRequest)
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleRemoveAvoid removes a node from the avoid list when the form is valid
// and the node exists.
func (s *Server) handleRemoveAvoid(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	pubkey := r.PostForm.Get("pubkey")

	done := false
	if pubkey != "" {
		var exists bool
		if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_avoidnodes WHERE pubkey=$1)`, pubkey).Scan(&exists); err == nil && exists {
			if _, err := s.db.Exec(ctx, `DELETE FROM gui_avoidnodes WHERE pubkey=$1`, pubkey); err == nil {
				f.add("Successfully removed node " + pubkey + " from the avoid list.")
				done = true
			}
		}
	}
	if !done {
		f.add(invalidRequest)
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}

// handleResetNodeReputation deletes all NodeReputation records and redirects to
// /rebalanceroutes (or the referrer).
func (s *Server) handleResetNodeReputation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	var count int64
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM gui_nodereputation`).Scan(&count); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.db.Exec(ctx, `DELETE FROM gui_nodereputation`); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	plural := "s"
	if count == 1 {
		plural = ""
	}
	f.add("Cleared " + strconv.FormatInt(count, 10) + " node reputation record" + plural + ".")
	s.redirect(w, r, refererOr(r, "/rebalanceroutes"), f)
}

// handleResetPost handles the POST for the reset page (the actual reset runs via
// api/reset) and redirects to the referrer.
func (s *Server) handleResetPost(w http.ResponseWriter, r *http.Request) {
	s.redirect(w, r, refererOr(r, "/"), nil)
}

// handleUpdatePending performs a get-or-create on the PendingChannels row for the
// given funding_txid+output_index, then sets a single field according to
// update_target (codes 0-6, 8, 9). Redirects to the referrer.
func (s *Server) handleUpdatePending(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	txid := r.PostForm.Get("funding_txid")
	idx, e1 := strconv.Atoi(r.PostForm.Get("output_index"))
	target, e2 := strconv.Atoi(r.PostForm.Get("target"))
	ut, e3 := strconv.Atoi(r.PostForm.Get("update_target"))

	if txid == "" || e1 != nil || e2 != nil || e3 != nil || ut < 0 || ut > 23 {
		f.add(invalidRequest)
		s.redirect(w, r, refererOr(r, "/"), f)
		return
	}
	fail := func(e error) bool {
		if e != nil {
			http.Error(w, e.Error(), http.StatusInternalServerError)
			return true
		}
		return false
	}

	// get-or-create the PendingChannels row.
	var exists bool
	if fail(s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gui_pendingchannels WHERE funding_txid=$1 AND output_index=$2)`, txid, idx).Scan(&exists)) {
		return
	}
	if !exists {
		if _, err := s.db.Exec(ctx, `INSERT INTO gui_pendingchannels (funding_txid, output_index) VALUES ($1, $2)`, txid, idx); fail(err) {
			return
		}
	}
	upd := func(sql string, args ...any) bool {
		_, e := s.db.Exec(ctx, sql, args...)
		return !fail(e)
	}
	setField := func(col string, val any) bool {
		return upd(`UPDATE gui_pendingchannels SET `+col+`=$3 WHERE funding_txid=$1 AND output_index=$2`, txid, idx, val)
	}

	switch ut {
	case 0:
		if !setField("local_base_fee", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Base fee for pending channel (%s) updated to a value of: %d", txid, target))
	case 1:
		if !setField("local_fee_rate", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Fee rate for pending channel (%s) updated to a value of: %d", txid, target))
	case 2:
		if !setField("ar_amt_target", int64(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer target amount for pending channel (%s) updated to a value of: %d", txid, target))
	case 3:
		if !setField("ar_in_target", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer inbound target for pending channel (%s) updated to a value of: %d%%", txid, target))
	case 4:
		if !setField("ar_out_target", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer outbound target for pending channel (%s) updated to a value of: %d%%", txid, target))
	case 5:
		var cur *bool
		if fail(s.db.QueryRow(ctx, `SELECT auto_rebalance FROM gui_pendingchannels WHERE funding_txid=$1 AND output_index=$2`, txid, idx).Scan(&cur)) {
			return
		}
		newVal := !(cur != nil && *cur) // (False or None) -> True; (True) -> False
		if !setField("auto_rebalance", newVal) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer status for pending channel (%s) updated to a value of: %s", txid, pyBool(newVal)))
	case 6:
		if !setField("ar_max_cost", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("Auto rebalancer max cost for pending channel (%s) updated to a value of: %d%%", txid, target))
	case 8:
		var cur *bool
		if fail(s.db.QueryRow(ctx, `SELECT auto_fees FROM gui_pendingchannels WHERE funding_txid=$1 AND output_index=$2`, txid, idx).Scan(&cur)) {
			return
		}
		var newVal bool
		if cur != nil {
			newVal = !*cur
		} else {
			newVal = s.settingIntDefault(ctx, "AF-Enabled", 0) == 0
		}
		if !setField("auto_fees", newVal) {
			return
		}
		f.add(fmt.Sprintf("Auto fees status for pending channel (%s) updated to a value of: %s", txid, pyBool(newVal)))
	case 9:
		if !setField("local_cltv", int32(target)) {
			return
		}
		f.add(fmt.Sprintf("CLTV for pending channel (%s) updated to a value of: %d", txid, target))
	default:
		f.add("Invalid target code. Please try again.")
	}
	s.redirect(w, r, refererOr(r, "/"), f)
}
