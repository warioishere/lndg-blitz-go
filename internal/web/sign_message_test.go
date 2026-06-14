package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	"github.com/warioishere/lndg-blitz-go/internal/lnd/lnrpc"
)

func signMessageServer(t *testing.T) *Server {
	fake := &fakeLightning{signMessageFn: func(_ context.Context, in *lnrpc.SignMessageRequest, _ ...grpc.CallOption) (*lnrpc.SignMessageResponse, error) {
		require.Equal(t, "hello", string(in.GetMsg())) // trimmed
		require.False(t, in.GetSingleHash())
		return &lnrpc.SignMessageResponse{Signature: "sigABC"}, nil
	}}
	return NewServer(&config.Settings{}, nil, WithLND(&LND{Lightning: fake}))
}

func postJSON(t *testing.T, srv *Server, path, body string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

func TestSignMessageSuccess(t *testing.T) {
	srv := signMessageServer(t)
	out := postJSON(t, srv, "/api/sign_message/", `{"message":"  hello  "}`)
	require.Equal(t, "Success", out["message"])
	require.Equal(t, "sigABC", out["data"])
}

func TestSignMessageInvalid(t *testing.T) {
	srv := signMessageServer(t)
	// Empty/whitespace message -> Invalid request!
	out := postJSON(t, srv, "/api/sign_message/", `{"message":"   "}`)
	require.Equal(t, "Invalid request!", out["error"])
	// Missing message key -> Invalid request!
	out = postJSON(t, srv, "/api/sign_message/", `{}`)
	require.Equal(t, "Invalid request!", out["error"])
}
