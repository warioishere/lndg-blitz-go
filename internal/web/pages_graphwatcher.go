package web

import (
	"encoding/json"
	"html/template"
	"net/http"
	"strings"
)

// handleGraphWatcher renders the Graph Watcher page (tables are populated via JS).
// gw_exclude_json is injected as a raw JS array using template.JS to prevent
// html/template from escaping it. gw_enabled/gw_cooldown control the settings form.
// POST (purge_events) is handled separately in Layer 6.
func (s *Server) handleGraphWatcher(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	setting := func(key, def string) string {
		if v, ok := s.settingValueFirst(ctx, key); ok {
			return v
		}
		return def
	}

	// gw_exclude = [pk.strip() for pk in raw.split(',') if pk.strip()]
	exclude := []string{}
	for _, pk := range strings.Split(setting("GW-Exclude", ""), ",") {
		if t := strings.TrimSpace(pk); t != "" {
			exclude = append(exclude, t)
		}
	}
	jsonBytes, err := json.Marshal(exclude) // [] -> "[]" (exclude is never nil)
	if err != nil {
		jsonBytes = []byte("[]")
	}

	s.renderTemplate(w, r, "graph_watcher.html", map[string]any{
		"gw_enabled":      setting("GW-Enabled", "0") != "0",
		"gw_cooldown":     setting("GW-Cooldown", "300"),
		"gw_exclude_json": template.JS(jsonBytes),
	})
}
