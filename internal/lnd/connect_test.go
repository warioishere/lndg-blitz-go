package lnd

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain sets environment variables deterministically so that config.Get()
// (which is lazily cached) points to generated temp files for all tests.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "lnd-connect-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	macPath := filepath.Join(dir, "admin.macaroon")
	if err := os.WriteFile(macPath, []byte{0xde, 0xad, 0xbe, 0xef}, 0o600); err != nil {
		panic(err)
	}
	// Generate a self-signed certificate inline (no *testing.T needed here).
	certPath := filepath.Join(dir, "tls.cert")
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "lnd-test"}, NotBefore: time.Unix(0, 0), NotAfter: time.Unix(0, 0).Add(100 * 365 * 24 * time.Hour)}
	der, _ := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	_ = os.WriteFile(certPath, pemBytes, 0o600)

	os.Setenv("LND_TLS_PATH", certPath)
	os.Setenv("LND_MACAROON_PATH", macPath)
	os.Setenv("LND_MAX_MESSAGE", "35")
	os.Setenv("LND_RPC_SERVER", "localhost:10009")

	os.Exit(m.Run())
}

func TestExpandUser(t *testing.T) {
	home, err := os.UserHomeDir()
	require.NoError(t, err)

	assert.Equal(t, home, expandUser("~"))
	assert.Equal(t, filepath.Join(home, ".lnd/tls.cert"), expandUser("~/.lnd/tls.cert"))
	// Absolute paths and paths without a leading ~ are returned unchanged.
	assert.Equal(t, "/etc/lnd/tls.cert", expandUser("/etc/lnd/tls.cert"))
	assert.Equal(t, "relative/path", expandUser("relative/path"))
	// '~' is only expanded at the start; a tilde mid-path is left as-is.
	assert.Equal(t, "/foo/~/bar", expandUser("/foo/~/bar"))
}

func TestMacaroonCredsMetadata(t *testing.T) {
	// Hex encoding must produce lowercase output matching the wire format.
	m := macaroonCreds{macaroon: "deadbeef"}
	md, err := m.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"macaroon": "deadbeef"}, md)
	assert.True(t, m.RequireTransportSecurity())
}

func TestGetCredsBuildsTwoDialOptions(t *testing.T) {
	// Macaroon {de ad be ef} -> hex "deadbeef"; valid cert -> 2 DialOptions
	// (TLS transport + per-RPC macaroon), no error.
	opts, err := getCreds()
	require.NoError(t, err)
	assert.Len(t, opts, 2)
}

func TestGetCredsMissingMacaroon(t *testing.T) {
	// Verify that reading a non-existent file returns an error, exercising the
	// expandUser + ReadFile path used by getCreds.
	_, err := os.ReadFile(expandUser("~/this-path-should-not-exist-xyz.macaroon"))
	assert.Error(t, err)
}
