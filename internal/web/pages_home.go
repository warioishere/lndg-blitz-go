package web

import (
	"net/http"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleHome renders the dashboard start page. GetInfo supplies node_info fields;
// most data is loaded by the template via JS from the API. GetInfo errors render
// error.html with the gRPC status code string. The large inline script is in
// static/home.js; server values are passed via window.GRAPH_LINKS/NETWORK/NETWORK_LINKS
// in base.html.
func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	info, err := s.lnd.Lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		s.renderError(w, r, grpcCodeString(err))
		return
	}
	s.renderTemplate(w, r, "home.html", map[string]any{
		"node_info": map[string]any{
			"color":           info.GetColor(),
			"alias":           info.GetAlias(),
			"version":         info.GetVersion(),
			"identity_pubkey": info.GetIdentityPubkey(),
			"uris":            info.GetUris(),
		},
		"local_settings": s.getLocalSettings(ctx, "AR-"),
		// Auto-refresh toggle is pre-selected unless the refresh cookie is "false".
		"refresh_not_false": cookieValue(r, "refresh") != "false",
	})
}
