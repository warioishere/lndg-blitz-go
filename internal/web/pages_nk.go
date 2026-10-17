package web

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	af "github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// refillLabel is the refill price the route prober measured over a channel's
// peer as table cell text, with the probe amount and time as its title.
func refillLabel(ppm pgtype.Int4, amountSat pgtype.Int8, checked pgtype.Timestamptz) (text, title string) {
	switch {
	case !checked.Valid:
		return "---", "not probed yet"
	case !ppm.Valid:
		return "none", "no probe reached us over the peer down to AR-MinAmount (" + nkTime(checked.Time) + " UTC)"
	}
	return intcomma(ppm.Int32), "route fee ppm before the source fee; " + intcomma(amountSat.Int64) + " sats got through (" + nkTime(checked.Time) + " UTC)"
}

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

// nkView is what the NK page and the NK budget API show: the NK job's
// channels, the NK settings, their flows and the input of each group of a
// peer's channels in one mode. Cap, lock and budget are those of the group, as
// the NK job and the rebalancer judge them.
type nkView struct {
	channels []db.GuiChannel
	p        af.NkParams
	flows    map[string]af.NkFlow
	groups   map[string]af.NkInput
}

func nkGroupKey(ch db.GuiChannel) string { return ch.RemotePubkey + "|" + ch.AfMode }

func (s *Server) loadNkView(ctx context.Context, now time.Time) (nkView, error) {
	var v nkView
	var err error
	if v.channels, err = s.queries.ListNkJobChannels(ctx); err != nil {
		return v, err
	}
	if v.p, err = af.LoadNkParams(ctx, s.queries); err != nil {
		return v, err
	}
	ids := make([]string, len(v.channels))
	for i, ch := range v.channels {
		ids[i] = ch.ChanID
	}
	if v.flows, err = af.LoadNkFlows(ctx, s.queries, ids, v.p.LockFlowPct, now); err != nil {
		return v, err
	}
	members := map[string][]db.GuiChannel{}
	for _, ch := range v.channels {
		members[nkGroupKey(ch)] = append(members[nkGroupKey(ch)], ch)
	}
	v.groups = make(map[string]af.NkInput, len(members))
	for k, chs := range members {
		v.groups[k] = af.NkGroupInput(chs, v.flows)
	}
	return v, nil
}

// nkBudget is the NK rebalance budget of a channel's group: for an NK channel
// the one the rebalancer uses, for an LNDg channel the one it would use with
// the group in NK mode at its current fee.
func (v nkView) nkBudget(ch db.GuiChannel) int {
	g := v.groups[nkGroupKey(ch)]
	return af.NkBudgetPpm(g.Fee, g.Flow, g.LocalPct, v.p)
}

// handleNk renders the NK fee mode page: per auto-fee channel its mode, budget
// flag and target, the inputs of the NK rules (fill, sold prices, cap, lock),
// the NK budget and the latest NK decision.
func (s *Server) handleNk(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now()
	v, err := s.loadNkView(ctx, now)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	channels, p, flows := v.channels, v.p, v.flows
	ids := make([]string, len(channels))
	for i, ch := range channels {
		ids[i] = ch.ChanID
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
		g := v.groups[nkGroupKey(ch)]
		localPct := 0.0
		if ch.Capacity > 0 {
			localPct = float64(ch.LocalBalance+ch.PendingOutbound) * 100 / float64(ch.Capacity)
		}
		target := af.NkTargetPct(ch.ArInTarget)
		nk := ch.AfMode == "nk"
		if nk {
			nkCount++
		}
		row := map[string]any{
			"chan_id": ch.ChanID, "short_chan_id": ch.ShortChanID, "alias": ch.Alias, "remote_pubkey": ch.RemotePubkey,
			"nk": nk, "target": target,
			"local_pct": int(math.Round(localPct)), "fee": ch.LocalFeeRate, "inbound": ch.LocalInboundFeeRate,
			"dynamic_inbound": ch.InboundOffset != 0, "max_htlc": ch.LocalMaxHtlcMsat / 1000,
			"cap": af.NkCap(g.Flow, p),
		}
		row["sold24"] = nkPpm(f.Flow24())
		row["sold72"] = nkPpm(f.Flow72())
		if lock := af.NkLockUntil(g.Flow, g.Fee, g.LocalPct, p); now.Before(lock) {
			row["lock_until"] = nkTime(lock)
		}
		row["budget_ppm"] = v.nkBudget(ch)
		row["refill"], row["refill_title"] = refillLabel(ch.RefillPpm, ch.RefillAmountSat, ch.RefillChecked)
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
// auto-fees) and, for an NK channel, its local target as ar_in_target = 100 -
// target (empty: unchanged). The target of an LNDg channel is its iTarget and
// is set on the main page.
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
	nk := set("nk")
	changed := 0
	for _, id := range r.PostForm["channels"] {
		mode := "lndg"
		if nk[id] {
			mode = "nk"
		}
		var arInTarget any
		if raw := strings.TrimSpace(r.PostForm.Get("target_" + id)); raw != "" && mode == "nk" {
			v, err := strconv.Atoi(raw)
			if err != nil || v < 0 || v > 99 {
				f.add("Invalid target for " + id + ": " + raw)
				continue
			}
			arInTarget = 100 - v
		}
		// a mode change starts the shadow afresh at the channel's own fee
		tag, err := s.db.Exec(ctx, `UPDATE gui_channels SET af_mode = $2, ar_in_target = COALESCE($3::integer, ar_in_target),
				nk_shadow_fee = CASE WHEN af_mode = $2 THEN nk_shadow_fee END,
				nk_shadow_inbound = CASE WHEN af_mode = $2 THEN nk_shadow_inbound END,
				nk_shadow_change = CASE WHEN af_mode = $2 THEN nk_shadow_change END
			WHERE chan_id = $1 AND (af_mode, ar_in_target) IS DISTINCT FROM ($2, COALESCE($3::integer, ar_in_target))`,
			id, mode, arInTarget)
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

// handleNkBudgets returns per open channel the budget the rebalancer uses to
// refill it now (af.TargetBudgetPpm, before the source fee) and, for an
// auto-fee channel, its NK budget (nkView.nkBudget).
func (s *Server) handleNkBudgets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	now := time.Now()
	v, err := s.loadNkView(ctx, now)
	if err != nil {
		writeAPIError(w, "Unable to fetch NK budgets! Error: "+err.Error())
		return
	}
	nkBudget := make(map[string]int, len(v.channels))
	for _, ch := range v.channels {
		nkBudget[ch.ChanID] = v.nkBudget(ch)
	}
	channels, err := s.queries.ListOpenChannels(ctx)
	if err != nil {
		writeAPIError(w, "Unable to fetch NK budgets! Error: "+err.Error())
		return
	}
	results := make([]any, 0, len(channels))
	for _, ch := range channels {
		budget, berr := af.TargetBudgetPpm(ctx, s.queries, ch, now)
		if berr != nil {
			writeAPIError(w, "Unable to fetch NK budgets! Error: "+berr.Error())
			return
		}
		row := newOrderedMap().Set("chan_id", ch.ChanID).Set("af_mode", ch.AfMode).Set("budget_ppm", budget)
		if b, ok := nkBudget[ch.ChanID]; ok {
			row.Set("nk_budget_ppm", b)
		} else {
			row.Set("nk_budget_ppm", nil)
		}
		text, title := refillLabel(ch.RefillPpm, ch.RefillAmountSat, ch.RefillChecked)
		row.Set("refill", text).Set("refill_title", title)
		if ch.RefillPpm.Valid {
			row.Set("refill_ppm", ch.RefillPpm.Int32)
		} else {
			row.Set("refill_ppm", nil)
		}
		results = append(results, row)
	}
	writeJSON(w, http.StatusOK, results)
}
