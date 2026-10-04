package web

import (
	"context"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/warioishere/lndg-blitz-go/internal/af"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// getLocalSettingInt reads a LocalSetting by key, creating it with str(default)
// if absent, and returns the value cast to int (error falls back to default).
func (s *Server) getLocalSettingInt(ctx context.Context, key string, def int) int {
	ls, err := s.queries.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{
		Key: key, Value: strconv.Itoa(def),
	})
	if err != nil {
		return def
	}
	if n, e := strconv.Atoi(ls.Value); e == nil {
		return n
	}
	return def
}

// settingValueFirst reads the value of a LocalSetting without creating it on miss.
// Returns (value, found).
func (s *Server) settingValueFirst(ctx context.Context, key string) (string, bool) {
	var value string
	if err := s.db.QueryRow(ctx, `SELECT value FROM gui_localsettings WHERE key = $1 LIMIT 1`, key).Scan(&value); err != nil {
		return "", false
	}
	return value, true
}

// handleAmbossFees renders the Amboss fee settings page with amb_enabled and
// amb_update_hours for the chart JS.
func (s *Server) handleAmbossFees(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ambEnabled := 0
	if v, ok := s.settingValueFirst(ctx, "AMB-Enabled"); ok {
		if n, err := strconv.Atoi(v); err == nil {
			ambEnabled = n
		}
	}
	var ambUpdateHours any = 0
	if v, ok := s.settingValueFirst(ctx, "AMB-UpdateHours"); ok && v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			ambUpdateHours = f
		}
	}
	s.renderTemplate(w, r, "amboss_fees.html", map[string]any{
		"local_settings":   s.getLocalSettings(ctx, "AMB-"),
		"amb_enabled":      ambEnabled,
		"amb_update_hours": ambUpdateHours,
	})
}

// handleRebalancing renders the rebalancing page (channel table loaded via
// static/rebalancing.js) plus the AR settings form. Filter query params
// (auto_rebalance/is_active) are passed to JS via window config.
func (s *Server) handleRebalancing(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, r, "rebalancing.html", map[string]any{
		"local_settings":     s.getLocalSettings(r.Context(), "AR-"),
		"req_auto_rebalance": r.URL.Query().Get("auto_rebalance"),
		"req_is_active":      r.URL.Query().Get("is_active"),
	})
}

// handleRebalanceRoutes renders the Saved Routes page (tables loaded via
// static/rebalance_routes.js) plus the RR-/QR- settings form. POST
// (purge/reset) is handled in Layer 6.
func (s *Server) handleRebalanceRoutes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	setting := func(key, def string) string {
		if v, ok := s.settingValueFirst(ctx, key); ok {
			return v
		}
		return def
	}
	s.renderTemplate(w, r, "rebalance_routes.html", map[string]any{
		"route_limit":       setting("RR-RouteLimit", "10"),
		"collect_routes":    setting("RR-CollectRoutes", "1") != "0",
		"use_saved_routes":  setting("RR-UseSavedRoutes", "1") != "0",
		"qr_enabled":        setting("QR-Enabled", "0") != "0",
		"qr_update_hours":   setting("QR-UpdateHours", "6"),
		"qr_max_per_target": setting("QR-MaxPerTarget", "5"),
	})
}

// handleAutoMaxhtlc renders the auto max HTLC page with open channels (outbound =
// local_balance+pending_outbound) and MX settings.
func (s *Server) handleAutoMaxhtlc(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT *, (local_balance+pending_outbound) AS outbound FROM gui_channels `+
			`WHERE is_open = true ORDER BY alias`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	form := s.getLocalSettings(r.Context(), "MX-")
	mxPercent := 0
	for _, f := range form {
		if f["id"] == "MX-Percent" {
			if n, ok := toInt64(f["value"]); ok {
				mxPercent = int(n)
			}
			break
		}
	}
	s.renderTemplate(w, r, "auto_maxhtlc.html", map[string]any{
		"channels":       rows,
		"local_settings": form,
		"mx_percent":     mxPercent,
	})
}

// handleFullFeeAdj renders the full fee adjustment page with open channels ordered
// by alias. POST (delta PPM application) is handled in Layer 6.
func (s *Server) handleFullFeeAdj(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT * FROM gui_channels WHERE is_open = true ORDER BY alias`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "full_fee_adj.html", map[string]any{"channels": rows})
}

// handleFeeLimitProtection renders the fee limit protection page: AR-enabled
// channels with average rebalancing cost (ppm) over the last FLP-Lookback
// successful payments, plus FLP settings.
func (s *Server) handleFeeLimitProtection(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	lookback := s.getLocalSettingInt(ctx, "FLP-Lookback", 10)
	channels, err := s.queryMaps(ctx,
		`SELECT chan_id, alias, flp_enabled, flp_safety FROM gui_channels `+
			`WHERE is_open = true AND auto_rebalance = true`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	list := make([]map[string]any, 0, len(channels))
	for _, ch := range channels {
		chanID, _ := ch["chan_id"].(string)
		pays, err := s.queries.RebalPaymentsForChannel(ctx, db.RebalPaymentsForChannelParams{
			RebalChan: pgtype.Text{String: chanID, Valid: true},
			Limit:     int32(lookback),
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// same number AF uses as cost floor
		var avgPpm any
		if v := af.AvgRebalanceCostPPM(pays); v != nil {
			avgPpm = int64(*v)
		}
		list = append(list, map[string]any{
			"chan_id":     ch["chan_id"],
			"alias":       ch["alias"],
			"avg_ppm":     avgPpm,
			"flp_enabled": ch["flp_enabled"],
			"flp_safety":  ch["flp_safety"],
		})
	}
	s.renderTemplate(w, r, "af_fee_limit.html", map[string]any{
		"channels":       list,
		"local_settings": s.getLocalSettings(ctx, "FLP-"),
	})
}

// handleEmergencyFees renders the emergency fees page with open channels including
// outbound_percent = (local_balance+pending_outbound)*100/capacity (integer
// division in Postgres) and EP settings.
func (s *Server) handleEmergencyFees(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT *, ((local_balance+pending_outbound)*100)/capacity AS outbound_percent `+
			`FROM gui_channels WHERE is_open = true ORDER BY alias`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "emergency_fees.html", map[string]any{
		"channels":       rows,
		"local_settings": s.getLocalSettings(r.Context(), "EP-"),
	})
}

// handleInboundOffset renders the inbound offset page with open channels ordered
// by alias and IO settings. POST/bulk update is handled in Layer 6.
func (s *Server) handleInboundOffset(w http.ResponseWriter, r *http.Request) {
	rows, err := s.queryMaps(r.Context(),
		`SELECT * FROM gui_channels WHERE is_open = true ORDER BY alias`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderTemplate(w, r, "inbound_offset.html", map[string]any{
		"channels":       rows,
		"local_settings": s.getLocalSettings(r.Context(), "IO-"),
	})
}

// lsf builds a settings form field map from alternating key/value pairs.
// Numeric types are preserved as-is (int vs float) because template truthiness
// and string formatting depend on the exact type (e.g. value 3.0 -> "3.0").
func lsf(kv ...any) map[string]any {
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

// buildSettingsForm returns the static form field list for the given prefixes
// (AR, AF, FLP, IO, MX, EP, NODE_CACHE, GUI, AMB, LND), in a fixed order
// independent of argument order.
func buildSettingsForm(prefixes []string) []map[string]any {
	has := func(p string) bool {
		for _, x := range prefixes {
			if x == p {
				return true
			}
		}
		return false
	}
	var form []map[string]any
	if has("AR-") {
		form = append(form,
			lsf("unit", "", "form_id", "update_channels", "id", "update_channels"),
			lsf("unit", "", "form_id", "enabled", "value", 0, "label", "AR Enabled", "id", "AR-Enabled", "title", "This enables or disables the auto-scheduling function", "min", 0, "max", 1),
			lsf("unit", "%", "form_id", "target_percent", "value", 3.0, "label", "AR Target Amount", "id", "AR-Target%", "title", "The percentage of the total capacity to target as the rebalance amount. Default 3", "min", 0.1, "max", 100),
			lsf("unit", "min", "form_id", "target_time", "value", 5, "label", "AR Target Time", "id", "AR-Time", "title", "The time spent in minutes for each individual rebalance attempt. Default 5", "min", 1, "max", 60),
			lsf("unit", "ppm", "form_id", "fee_rate", "value", 500, "label", "AR Max Fee Rate", "id", "AR-MaxFeeRate", "title", "The max rate we can ever use to refill a channel with outbound. Default 500", "min", 1),
			lsf("unit", "%", "form_id", "outbound_percent", "value", 75, "label", "AR Target Out Above", "id", "AR-Outbound%", "title", "Default oTarget% for new channels. When a channel is not AR enabled; the oTarget% is the minimum outbound a channel must have to be a source for refilling another channel. Default 75", "min", 1, "max", 100),
			lsf("unit", "%", "form_id", "inbound_percent", "value", 90, "label", "AR Target In Above", "id", "AR-Inbound%", "title", "Default iTarget% for new channels. When a channel is AR enabled; the iTarget% is the minimum inbound a channel must have before selected for auto rebalance. Default 90", "min", 1, "max", 100),
			lsf("unit", "%", "form_id", "max_cost", "value", 65, "label", "AR Max Cost", "id", "AR-MaxCost%", "title", "The ppm to target which is the percentage of the outbound fee rate for the channel being refilled. Default 65", "min", 1, "max", 100),
			lsf("unit", "%", "form_id", "variance", "value", 0, "label", "AR Variance", "id", "AR-Variance", "title", "The percentage of the target amount to be randomly varied with every rebalance attempt. Default 0", "min", 0, "max", 100),
			lsf("unit", "min", "form_id", "wait_period", "value", 30, "label", "AR Wait Period", "id", "AR-WaitPeriod", "title", "The minutes we should wait after a failed attempt before trying again. Default 30", "min", 1, "max", 10080),
			lsf("unit", "", "form_id", "autopilot", "value", 0, "label", "Autopilot", "id", "AR-Autopilot", "title", "This enables or disables the Auto-Rebalance function for individual channels based on flow (automatically acts upon suggestions on this page: /actions)", "min", 0, "max", 1),
			lsf("unit", "days", "form_id", "autopilotdays", "value", 7, "label", "Autopilot Days", "id", "AR-APDays", "title", "Number of days to consider for autopilot calculations. Default 7", "min", 0, "max", 100),
			lsf("unit", "", "form_id", "workers", "value", 1, "label", "Workers", "id", "AR-Workers", "title", "Number of concurrent rebalance workers to run at once (use a proper value for your hardware, this will increase the load on the lnd server). Default 1", "min", 1, "max", 12),
			lsf("unit", "", "form_id", "per_source_enabled", "value", 0, "label", "Per-Source Mode", "id", "AR-PerSourceEnabled", "title", "When enabled, iterate outgoing channels individually with per-source fee budget accounting for opportunity cost (route_fee + source_fee ≤ target_fee * max_cost%). Regolancer-style. Off = legacy SendPaymentV2 with global fee_limit.", "min", 0, "max", 1),
		)
	}
	if has("AF-") {
		form = append(form,
			lsf("unit", "", "form_id", "af_enabled", "value", 0, "label", "Autofee", "id", "AF-Enabled", "title", "Enable/Disable All Auto-fee functionality", "min", 0, "max", 1),
			lsf("unit", "", "form_id", "af_inbound", "value", 0, "label", "Inbound Fees", "id", "AF-InboundFees", "title", "Enable/Disable Inbound Auto-fee functionality", "min", 0, "max", 1),
			lsf("unit", "", "form_id", "af_curve_mode", "value", 1, "label", "Curve Mode", "id", "AF-CurveMode", "title", "Switch between smooth curve (On) and legacy zone-based (Off) fee adjustments", "min", 0, "max", 1),
			lsf("unit", "ppm", "form_id", "af_maxRate", "value", 2500, "label", "AF Max Rate", "id", "AF-MaxRate", "title", "Maximum Rate that can be adjusted to. Default 2500", "min", 0),
			lsf("unit", "ppm", "form_id", "af_minRate", "value", 0, "label", "AF Min Rate", "id", "AF-MinRate", "title", "Minimum Rate that can be adjusted to. Default 0", "min", 0, "max", 5000),
			lsf("unit", "ppm", "form_id", "af_increment", "value", 5, "label", "AF Increment", "id", "AF-Increment", "title", "Target fee rate will always be a multiple of this value. Default 5", "min", 1, "max", 100),
			lsf("unit", "hours", "form_id", "af_updateHours", "value", 24, "label", "AF Update", "id", "AF-UpdateHours", "title", "Minimum number of hours between fee updates for an individual channel. Default 24", "min", 0.01, "max", 100),
			lsf("unit", "ppm", "form_id", "af_intensity", "value", 50, "label", "Intensity", "id", "AF-Intensity", "title", "Max ppm adjustment per cycle at worst imbalance. Default 50", "min", 1, "max", 500, "css_class", "af-curve-setting"),
			lsf("unit", "", "form_id", "af_exponent", "value", 2.0, "label", "Exponent", "id", "AF-Exponent", "title", "Curve shape: 1=linear, 2=quadratic, 3=cubic. Default 2.0", "min", 0.5, "max", 5, "css_class", "af-curve-setting"),
			lsf("unit", "x", "form_id", "af_flow_weight", "value", 0.5, "label", "Flow Weight", "id", "AF-FlowWeight", "title", "Flow amplification factor; 0 disables flow modifier. Default 0.5", "min", 0, "max", 3, "css_class", "af-curve-setting"),
			lsf("unit", "x", "form_id", "af_downscale", "value", 1.0, "label", "DownScale", "id", "AF-DownScale", "title", "Multiplier for fee decreases only. >1 = faster drops, <1 = slower drops. Default 1.0", "min", 0.1, "max", 5, "css_class", "af-curve-setting"),
			lsf("unit", "ppm", "form_id", "af_inbound_intensity", "value", 20, "label", "Inbound Intensity", "id", "AF-InboundIntensity", "title", "Max inbound ppm adjustment per cycle at worst imbalance. Default 20", "min", 1, "max", 500, "css_class", "af-curve-setting"),
			lsf("unit", "x", "form_id", "af_multiplier", "value", 5, "label", "AF Multiplier", "id", "AF-Multiplier", "title", "Multiplier to be applied to Auto-Fee adjustments. Default 5", "min", 1, "max", 100, "css_class", "af-legacy-setting"),
			lsf("unit", "x", "form_id", "af_flow_scale", "value", 1.0, "label", "Flow Scale", "id", "AF-FlowScale", "title", "Scale flow-based adjustments; 0 disables flow factor", "min", 0, "css_class", "af-legacy-setting"),
			lsf("unit", "ppm", "form_id", "af_maxstep", "value", 100, "label", "Max Step", "id", "AF-MaxStep", "title", "Maximum change allowed per update", "min", 1, "css_class", "af-legacy-setting"),
			lsf("unit", "min", "form_id", "af_failedhtlcboost_interval", "value", 15, "label", "HTLC Boost Interval", "id", "AF-HTLCBoostIntvl", "title", "Time window in minutes to count failed HTLCs for boost mechanism. Default 15", "min", 1, "max", 1440),
			lsf("unit", "", "form_id", "af_failedHTLCs", "value", 5, "label", "Failed HTLCs", "id", "AF-FailedHTLCs", "title", "Number of failed HTLCs within interval to trigger boost. Default 5", "min", 1, "max", 100),
			lsf("unit", "ppm", "form_id", "af_failedhtlcboost", "value", 0, "label", "Failed HTLC Boost", "id", "AF-FailedHTLCBoost", "title", "Fixed fee increase in ppm when HTLC threshold is met. 0 disables this feature. Default 0", "min", 0, "max", 5000),
			lsf("unit", "%", "form_id", "af_lowliq", "value", 15, "label", "AF LowLiq", "id", "AF-LowLiqLimit", "title", "Limit for running low liq AF rules (increase when failed htlcs + no inbound). Default 15", "min", 0, "max", 100, "css_class", "af-legacy-setting"),
			lsf("unit", "x", "form_id", "af_lowliqboost", "value", 1, "label", "AF LowLiq Boost", "id", "AF-LowLiqBoost", "title", "Multiplier for extra fee bump when liquidity is below AF-LowLiqLimit. Default 1", "min", 0, "max", 10, "css_class", "af-legacy-setting"),
			lsf("unit", "", "form_id", "af_lowliqboostar", "value", 0, "label", "AF Boost AR Only", "id", "AF-LowLiqBoostAR", "title", "Apply boost only to channels with Auto-Rebalance enabled; Off disables the boost", "min", 0, "max", 1, "css_class", "af-legacy-setting"),
			lsf("unit", "ppm", "form_id", "af_peer_rate_limit", "value", 0, "label", "Peer oRate Limit", "id", "AF-PeerRateLimit", "title", "Only raise fees if peer oRate below this limit; 0 disables", "min", 0, "max", 5000),
			lsf("unit", "", "form_id", "af_peer_rate_check", "value", 0, "label", "Peer Rate Check", "id", "AF-PeerRateCheck", "title", "Enable/Disable peer oRate limit check", "min", 0, "max", 1),
			lsf("unit", "", "form_id", "af_bypass_peer_rate_on_htlc", "value", 0, "label", "Bypass PeerRate on HTLC", "id", "AF-BypassPeerHTLC", "title", "When enabled, channels with failed HTLCs meeting the boost threshold bypass the peer rate check (allows fee increases despite peer high oRate)", "min", 0, "max", 1),
			lsf("unit", "%", "form_id", "af_excess", "value", 95, "label", "AF Excess", "id", "AF-ExcessLimit", "title", "Limit for running excess liq AF rules (decrease for stagnant channels and those with assisting revenues). Default 95", "min", 0, "max", 100, "css_class", "af-legacy-setting"),
			lsf("unit", "x", "form_id", "af_excessboost", "value", 1, "label", "AF Excess Boost", "id", "AF-ExcessBoost", "title", "Multiplier for extra fee drop when liquidity exceeds AF-ExcessLimit. Default 1", "min", 0, "max", 10, "css_class", "af-legacy-setting"),
			lsf("unit", "", "form_id", "af_excessboost_on", "value", 0, "label", "Excess Boost", "id", "AF-ExcessBoostOn", "title", "Enable/Disable extra decrease for excess liquidity", "min", 0, "max", 1, "css_class", "af-legacy-setting"),
		)
	}
	if has("FLP-") {
		form = append(form,
			lsf("unit", "", "form_id", "update_channels", "id", "update_channels"),
			lsf("unit", "", "form_id", "flp_enabled", "value", 0, "label", "FLP Enabled", "id", "FLP-Enabled", "title", "Enable/Disable fee limit protection", "min", 0, "max", 1),
			lsf("unit", "ppm", "form_id", "flp_safety", "value", 0, "label", "Safety Distance", "id", "FLP-Safety", "title", "Positive value adds safety distance, negative value reduces cost floor (allows more aggressive fee reduction)", "max", 5000),
			lsf("unit", "", "form_id", "flp_lookback", "value", 10, "label", "Payments Lookback", "id", "FLP-Lookback", "title", "Number of recent rebalancing payments to average", "min", 1),
		)
	}
	if has("IO-") {
		form = append(form,
			lsf("unit", "", "form_id", "io_enabled", "value", 0, "label", "Offset Updates", "id", "IO-Enabled", "title", "Enable/Disable automatic inbound offset updates", "min", 0, "max", 1),
			lsf("unit", "hours", "form_id", "io_updateHours", "value", 24, "label", "IO Update", "id", "IO-UpdateHours", "title", "Hours between applying inbound offsets. Fractions allowed. Default 24", "min", 0.01, "max", 100),
		)
	}
	if has("MX-") {
		form = append(form,
			lsf("unit", "", "form_id", "mx_enabled", "value", 0, "label", "MaxHTLC Updates", "id", "MX-Enabled", "title", "Enable/Disable automatic max HTLC updates", "min", 0, "max", 1),
			lsf("unit", "hours", "form_id", "mx_updateHours", "value", 24, "label", "MX Update", "id", "MX-UpdateHours", "title", "Hours between applying max HTLC settings. Fractions allowed. Default 24", "min", 0.01, "max", 100),
			lsf("unit", "%", "form_id", "mx_percent", "value", 0, "label", "Offset %", "id", "MX-Percent", "title", "Default percent below outbound liquidity when no per-channel value is set", "min", 0, "max", 100),
		)
	}
	if has("EP-") {
		form = append(form,
			lsf("unit", "", "form_id", "update_channels", "id", "update_channels"),
			lsf("unit", "", "form_id", "ep_enabled", "value", 0, "label", "Enable", "id", "EP-Enabled", "title", "Enable/Disable emergency fee increases", "min", 0, "max", 1),
			lsf("unit", "%", "form_id", "ep_target_default", "value", 50, "label", "Default Target", "id", "EP-DefaultTarget", "title", "Default outbound liquidity target percent", "min", 0, "max", 100),
			lsf("unit", "%", "form_id", "ep_inc_pct", "value", 10, "label", "Increase %", "id", "EP-IncreasePct", "title", "Percent increase when triggered", "min", 0),
			lsf("unit", "min", "form_id", "ep_cooldown", "value", 10, "label", "Cooldown", "id", "EP-Cooldown", "title", "Minutes between increases", "min", 1),
			lsf("unit", "%", "form_id", "ep_live_threshold", "value", 40, "label", "Live Threshold", "id", "EP-LiveThreshold", "title", "Instant check threshold percent", "min", 0, "max", 100),
			lsf("unit", "%", "form_id", "ep_live_inc_pct", "value", 5, "label", "Live Increase %", "id", "EP-LiveIncreasePct", "title", "Instant increase percent", "min", 0),
		)
	}
	if has("NODE_CACHE") {
		form = append(form,
			lsf("unit", "min", "form_id", "node_cache_expiry_minutes", "value", 60, "label", "Node Cache Expiry", "id", "NODE_CACHE_EXPIRY_MINUTES", "title", "Minutes node info is cached in memory and DB before refresh", "min", 1, "max", 10080),
			lsf("unit", "", "form_id", "node_cache_max_entries", "value", 500, "label", "Node Cache Size", "id", "NODE_CACHE_MAX_ENTRIES", "title", "Maximum number of node info objects kept in RAM", "min", 1, "max", 5000),
		)
	}
	if has("GUI-") {
		form = append(form,
			lsf("unit", "", "form_id", "gui_graphLinks", "value", "https://mempool.space/lightning", "label", "Graph URL", "id", "GUI-GraphLinks", "title", "Preferred Graph URL. Default https://mempool.space/lightning"),
			lsf("unit", "", "form_id", "gui_netLinks", "value", "https://mempool.space", "label", "NET URL", "id", "GUI-NetLinks", "title", "Preferred NET URL. Default https://mempool.space"),
		)
	}
	if has("AMB-") {
		form = append(form,
			lsf("unit", "", "form_id", "amboss_api_key", "value", "", "label", "Amboss API Key", "id", "AMB-ApiKey", "title", "Amboss API Key for fee data"),
			lsf("unit", "", "form_id", "amb_enabled", "value", 0, "label", "Auto Fetch", "id", "AMB-Enabled", "title", "Enable/Disable automatic Amboss fetches", "min", 0, "max", 1),
			lsf("unit", "hours", "form_id", "amb_updateHours", "value", 0, "label", "Update", "id", "AMB-UpdateHours", "title", "Hours between Amboss fetches. Fractions allowed", "min", 0, "max", 100),
		)
	}
	if has("LND-") {
		form = append(form,
			lsf("unit", "", "form_id", "lnd_cleanPayments", "value", 0, "label", "LND Clean Payments", "id", "LND-CleanPayments", "title", "Clean LND Payments (toggles failed payment clean-up routine)", "min", 0, "max", 1),
			lsf("unit", "days", "form_id", "lnd_retentionDays", "value", 30, "label", "LND Retention", "id", "LND-RetentionDays", "title", "LND Retention days for failed payment data", "min", 1, "max", 1000),
		)
	}
	return form
}

// getLocalSettings returns the static field list for the given prefixes with
// values overridden by their stored LocalSettings. Key lookup uses a substring
// match (strpos > 0).
func (s *Server) getLocalSettings(ctx context.Context, prefixes ...string) []map[string]any {
	form := buildSettingsForm(prefixes)
	for _, prefix := range prefixes {
		sql := `SELECT key, value FROM gui_localsettings WHERE strpos(key, $1) > 0`
		if prefix == "AMB-" {
			sql += ` AND key <> 'AMB-SelectedPeers'`
		}
		sql += ` ORDER BY key`
		rows, err := s.queryMaps(ctx, sql, prefix)
		if err != nil {
			continue
		}
		for _, field := range form {
			for _, sett := range rows {
				if field["id"] == sett["key"] {
					field["value"] = sett["value"]
					break
				}
			}
		}
	}
	return form
}
