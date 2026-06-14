package web

import (
	"net/http"
	"strconv"
	"strings"
)

// settingField describes a field in the LocalSettings form (form name -> id, type).
type settingField struct {
	form, id, kind string // kind: "int" | "float" | "str"
}

// updateSettingsFields is the canonical field list for the update_settings form.
var updateSettingsFields = []settingField{
	{"enabled", "AR-Enabled", "int"},
	{"target_percent", "AR-Target%", "float"},
	{"target_time", "AR-Time", "int"},
	{"fee_rate", "AR-MaxFeeRate", "int"},
	{"outbound_percent", "AR-Outbound%", "int"},
	{"inbound_percent", "AR-Inbound%", "int"},
	{"max_cost", "AR-MaxCost%", "int"},
	{"variance", "AR-Variance", "int"},
	{"wait_period", "AR-WaitPeriod", "int"},
	{"autopilot", "AR-Autopilot", "int"},
	{"autopilotdays", "AR-APDays", "int"},
	{"workers", "AR-Workers", "int"},
	{"per_source_enabled", "AR-PerSourceEnabled", "int"},
	{"af_enabled", "AF-Enabled", "int"},
	{"af_inbound", "AF-InboundFees", "int"},
	{"af_curve_mode", "AF-CurveMode", "int"},
	{"af_maxRate", "AF-MaxRate", "int"},
	{"af_minRate", "AF-MinRate", "int"},
	{"af_increment", "AF-Increment", "int"},
	{"af_updateHours", "AF-UpdateHours", "float"},
	{"af_intensity", "AF-Intensity", "int"},
	{"af_exponent", "AF-Exponent", "float"},
	{"af_flow_weight", "AF-FlowWeight", "float"},
	{"af_downscale", "AF-DownScale", "float"},
	{"af_inbound_intensity", "AF-InboundIntensity", "int"},
	{"af_multiplier", "AF-Multiplier", "int"},
	{"af_flow_scale", "AF-FlowScale", "float"},
	{"af_maxstep", "AF-MaxStep", "int"},
	{"af_failedhtlcboost_interval", "AF-HTLCBoostIntvl", "int"},
	{"af_failedHTLCs", "AF-FailedHTLCs", "int"},
	{"af_failedhtlcboost", "AF-FailedHTLCBoost", "int"},
	{"af_lowliq", "AF-LowLiqLimit", "int"},
	{"af_lowliqboost", "AF-LowLiqBoost", "float"},
	{"af_lowliqboostar", "AF-LowLiqBoostAR", "int"},
	{"af_peer_rate_limit", "AF-PeerRateLimit", "int"},
	{"af_peer_rate_check", "AF-PeerRateCheck", "int"},
	{"af_bypass_peer_rate_on_htlc", "AF-BypassPeerHTLC", "int"},
	{"af_excess", "AF-ExcessLimit", "int"},
	{"af_excessboost", "AF-ExcessBoost", "float"},
	{"af_excessboost_on", "AF-ExcessBoostOn", "int"},
	{"flp_enabled", "FLP-Enabled", "int"},
	{"flp_safety", "FLP-Safety", "int"},
	{"flp_lookback", "FLP-Lookback", "int"},
	{"io_enabled", "IO-Enabled", "int"},
	{"io_updateHours", "IO-UpdateHours", "float"},
	{"mx_enabled", "MX-Enabled", "int"},
	{"mx_updateHours", "MX-UpdateHours", "float"},
	{"mx_percent", "MX-Percent", "int"},
	{"ep_enabled", "EP-Enabled", "int"},
	{"ep_target_default", "EP-DefaultTarget", "int"},
	{"ep_inc_pct", "EP-IncreasePct", "float"},
	{"ep_cooldown", "EP-Cooldown", "int"},
	{"ep_live_threshold", "EP-LiveThreshold", "int"},
	{"ep_live_inc_pct", "EP-LiveIncreasePct", "float"},
	{"gui_graphLinks", "GUI-GraphLinks", "str"},
	{"gui_netLinks", "GUI-NetLinks", "str"},
	{"amboss_api_key", "AMB-ApiKey", "str"},
	{"amb_enabled", "AMB-Enabled", "int"},
	{"amb_updateHours", "AMB-UpdateHours", "float"},
	{"lnd_cleanPayments", "LND-CleanPayments", "int"},
	{"lnd_retentionDays", "LND-RetentionDays", "int"},
	{"node_cache_expiry_minutes", "NODE_CACHE_EXPIRY_MINUTES", "int"},
	{"node_cache_max_entries", "NODE_CACHE_MAX_ENTRIES", "int"},
}

// handleUpdateSettings upserts all LocalSettings from the form and optionally
// propagates certain values to all channels (checkbox update_channels). Validation
// fails with "Invalid Request" if any int/float field cannot be parsed. Redirects
// to the referrer.
func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	f := &flasher{}
	_ = r.ParseForm()

	// 1) Form validation: every present int/float field must parse.
	for _, fld := range updateSettingsFields {
		raw := r.PostForm.Get(fld.form)
		if raw == "" {
			continue
		}
		switch fld.kind {
		case "int":
			if _, err := strconv.Atoi(raw); err != nil {
				f.add(invalidRequest)
				s.redirect(w, r, refererOr(r, "/"), f)
				return
			}
		case "float":
			if _, err := strconv.ParseFloat(raw, 64); err != nil {
				f.add(invalidRequest)
				s.redirect(w, r, refererOr(r, "/"), f)
				return
			}
		}
	}

	updateChannels := r.PostForm.Has("update_channels")

	// Load existing settings.
	settingsMap := map[string]string{}
	rows, err := s.db.Query(ctx, `SELECT key, value FROM gui_localsettings`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		settingsMap[k] = v
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var chanSet []string
	var chanArgs []any
	var chanMsgs []string
	chanField := func(col, expr string, arg any, id, stored string) {
		chanArgs = append(chanArgs, arg)
		if expr == "" {
			expr = col + " = $" + strconv.Itoa(len(chanArgs))
		} else {
			expr = strings.Replace(expr, "$?", "$"+strconv.Itoa(len(chanArgs)), 1)
		}
		chanSet = append(chanSet, expr)
		chanMsgs = append(chanMsgs, "All channels "+id+" updated to: "+stored)
	}

	// 2) Process fields.
	for _, fld := range updateSettingsFields {
		raw := r.PostForm.Get(fld.form)
		var stored string
		var iVal int
		var fVal float64
		switch fld.kind {
		case "int":
			if raw == "" {
				continue
			}
			iVal, _ = strconv.Atoi(raw)
			stored = strconv.Itoa(iVal)
		case "float":
			if raw == "" {
				continue
			}
			fVal, _ = strconv.ParseFloat(raw, 64)
			stored = pyFloatString(fVal)
		default: // str: empty string is stored as ''
			stored = raw
		}

		existing, ok := settingsMap[fld.id]
		if !ok {
			if stored != "" {
				if _, err := s.db.Exec(ctx, `INSERT INTO gui_localsettings (key, value) VALUES ($1, $2)`, fld.id, stored); err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				settingsMap[fld.id] = stored
				f.add(fld.id + " updated to: " + stored)
			}
		} else if existing != stored && stored != "" {
			if _, err := s.db.Exec(ctx, `UPDATE gui_localsettings SET value=$2 WHERE key=$1`, fld.id, stored); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			settingsMap[fld.id] = stored
			f.add(fld.id + " updated to: " + stored)
		}

		// Bulk propagation to all channels.
		if updateChannels {
			switch fld.id {
			case "AR-Target%":
				chanField("ar_amt_target", "ar_amt_target = round(capacity * $?::float8)::bigint", fVal/100.0, fld.id, stored)
			case "AR-Outbound%":
				chanField("ar_out_target", "", iVal, fld.id, stored)
			case "AR-Inbound%":
				chanField("ar_in_target", "", iVal, fld.id, stored)
			case "AR-MaxCost%":
				chanField("ar_max_cost", "", iVal, fld.id, stored)
			case "MX-Percent":
				chanField("maxhtlc_percent", "", iVal, fld.id, stored)
			case "EP-DefaultTarget":
				chanField("ep_target", "", iVal, fld.id, stored)
			case "EP-IncreasePct":
				chanField("ep_inc_pct", "", fVal, fld.id, stored)
			case "EP-Cooldown":
				chanField("ep_cooldown", "", iVal, fld.id, stored)
			case "EP-LiveThreshold":
				chanField("ep_live_threshold", "", iVal, fld.id, stored)
			case "EP-LiveIncreasePct":
				chanField("ep_live_inc_pct", "", fVal, fld.id, stored)
			case "EP-Enabled":
				chanField("ep_enabled", "", iVal != 0, fld.id, stored)
			case "FLP-Enabled":
				chanField("flp_enabled", "", iVal != 0, fld.id, stored)
			case "FLP-Safety":
				chanField("flp_safety", "", iVal, fld.id, stored)
			}
		}
	}

	if updateChannels && len(chanSet) > 0 {
		if _, err := s.db.Exec(ctx, `UPDATE gui_channels SET `+strings.Join(chanSet, ", "), chanArgs...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, m := range chanMsgs {
			f.add(m)
		}
	}

	s.redirect(w, r, refererOr(r, "/"), f)
}
