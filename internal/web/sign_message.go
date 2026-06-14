package web

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

// handleSignMessage handles POST {message} -> lnrpc.SignMessage. The message
// field is whitespace-trimmed; a missing or empty message returns
// {"error":"Invalid request!"}. On success returns {"message":"Success","data":sig}.
func (s *Server) handleSignMessage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Message *string `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Message == nil {
		writeAPIError(w, "Invalid request!")
		return
	}
	message := strings.TrimSpace(*body.Message)
	if message == "" {
		writeAPIError(w, "Invalid request!")
		return
	}
	resp, err := s.lnd.Lightning.SignMessage(r.Context(),
		&lnrpc.SignMessageRequest{Msg: []byte(message), SingleHash: false})
	if err != nil {
		writeAPIError(w, "Sign message failed! Error: "+grpcErrorMsg(err))
		return
	}
	writeJSON(w, http.StatusOK, newOrderedMap().Set("message", "Success").Set("data", resp.GetSignature()))
}
