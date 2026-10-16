package web

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
)

// nkTime formats a time for the NK pages ("" for the zero time).
func nkTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("01-02 15:04")
}

// nkPpm formats an optional ppm value ("" when unknown).
func nkPpm(v int, ok bool) string {
	if !ok {
		return ""
	}
	return strconv.Itoa(v)
}

// handleNk renders the NK fee mode page: per auto-fee channel its mode, budget
// flag and target, the inputs of the NK rules (fill, sold prices, cap, lock),
// the NK budget and the latest NK decision.
func (s *Server) handleNk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	channels, err := s.queries.ListNkJobChannels(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	p, err := af.LoadNkParams(ctx, s.queries)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ids := make([]string, len(channels))
	for i, ch := range channels {
		ids[i] = ch.ChanID
	}
	now := time.Now()
	flows, err := af.LoadNkFlows(ctx, s.queries, ids, p.LockFlowPct, now)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	last, err := s.queryMaps(ctx, `SELECT DISTINCT ON (chan_id) chan_id, timestamp, applied, reason, fee_old, fee_new
		FROM nk_fee_log WHERE chan_id = ANY($1) ORDER BY chan_id, timestamp DESC`, ids)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	lastBy := map[string]map[string]any{}
	for _, l := range last {
		lastBy[fmt.Sprint(l["chan_id"])] = l
	}

	rows := make([]map[string]any, 0, len(channels))
	nkCount := 0
	for _, ch := range channels {
		f := flows[ch.ChanID]
		localPct := 0.0
		if ch.Capacity > 0 {
			localPct = float64(ch.LocalBalance+ch.PendingOutbound) * 100 / float64(ch.Capacity)
		}
		target := af.NkTargetPct(ch.NkTarget, ch.ArInTarget)
		nk := ch.AfMode == "nk"
		if nk {
			nkCount++
		}
		row := map[string]any{
			"chan_id": ch.ChanID, "short_chan_id": ch.ShortChanID, "alias": ch.Alias, "remote_pubkey": ch.RemotePubkey,
			"nk": nk, "budget": ch.NkBudget, "target": target, "target_set": ch.NkTarget.Valid,
			"local_pct": int(math.Round(localPct)), "fee": ch.LocalFeeRate, "inbound": ch.LocalInboundFeeRate,
			"dynamic_inbound": ch.InboundOffset != 0, "max_htlc": ch.LocalMaxHtlcMsat / 1000,
			"cap": af.NkCap(f, p),
		}
		row["sold24"] = nkPpm(f.Flow24())
		row["sold72"] = nkPpm(f.Flow72())
		if lock := af.NkLockUntil(f, int(ch.LocalFeeRate), localPct, p); now.Before(lock) {
			row["lock_until"] = nkTime(lock)
		}
		if nk && ch.NkBudget {
			row["budget_ppm"] = af.NkBudgetPpm(int(ch.LocalFeeRate), f, localPct, target, p)
		}
		if l, ok := lastBy[ch.ChanID]; ok {
			t, _ := l["timestamp"].(time.Time)
			mode := "shadow"
			if l["applied"] == true {
				mode = "NK"
			}
			row["last"] = fmt.Sprintf("%s %s: %v → %v (%s)", nkTime(t), l["reason"], l["fee_old"], l["fee_new"], mode)
		}
		rows = append(rows, row)
	}
	s.renderTemplate(w, r, "nk.html", map[string]any{
		"channels":       rows,
		"nk_count":       nkCount,
		"local_settings": s.getLocalSettings(ctx, "NK-"),
	})
}

// handleNkPost saves per channel the fee mode (checked "nk": NK, else LNDg
// auto-fees), the NK budget flag and the NK target (empty: 100 - ar_in_target).
func (s *Server) handleNkPost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()
	set := func(name string) map[string]bool {
		m := map[string]bool{}
		for _, id := range r.PostForm[name] {
			m[id] = true
		}
		return m
	}
	nk, budget := set("nk"), set("budget")
	changed := 0
	for _, id := range r.PostForm["channels"] {
		mode := "lndg"
		if nk[id] {
			mode = "nk"
		}
		var target any
		if raw := strings.TrimSpace(r.PostForm.Get("target_" + id)); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 || v > 100 {
				f.add("Invalid target for " + id + ": " + raw)
				continue
			}
			target = v
		}
		tag, err := s.db.Exec(ctx, `UPDATE gui_channels SET af_mode = $2, nk_budget = $3, nk_target = $4
			WHERE chan_id = $1 AND (af_mode, nk_budget, nk_target) IS DISTINCT FROM ($2, $3, $4::integer)`,
			id, mode, budget[id], target)
		if err != nil {
			f.add("Error updating " + id + ": " + err.Error())
			continue
		}
		changed += int(tag.RowsAffected())
	}
	f.add(fmt.Sprintf("NK settings saved for %d channel(s).", changed))
	s.redirect(w, r, "/nk/", f)
}

// handleNkLog renders the latest NK decisions, optionally of one channel (?chan=).
func (s *Server) handleNkLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chanID := r.URL.Query().Get("chan")
	sql := `SELECT * FROM nk_fee_log`
	var args []any
	if chanID != "" {
		sql += ` WHERE chan_id = $1`
		args = append(args, chanID)
	}
	sql += ` ORDER BY id DESC LIMIT 500`
	rows, err := s.queryMaps(ctx, sql, args...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, row := range rows {
		if t, ok := row["timestamp"].(time.Time); ok {
			row["time"] = nkTime(t)
		}
		if t, ok := row["lock_until"].(time.Time); ok {
			row["lock"] = nkTime(t)
		}
		for _, k := range []string{"max_htlc_old", "max_htlc_new"} {
			if v, ok := toInt64(row[k]); ok {
				row[k] = v / 1000
			}
		}
		if v, ok := row["local_pct"].(float64); ok {
			row["local_pct"] = int(math.Round(v))
		}
	}
	s.renderTemplate(w, r, "nk_log.html", map[string]any{"rows": rows, "chan": chanID})
}
