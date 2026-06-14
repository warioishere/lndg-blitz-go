// Package lnd handles the gRPC connection to LND, including TLS certificate
// and macaroon authentication, plus a process-wide shared channel with
// close/reset logic.
package lnd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// macaroonCreds implements credentials.PerRPCCredentials and attaches the
// hex-encoded macaroon as 'macaroon' request metadata.
type macaroonCreds struct {
	macaroon string
}

func (m macaroonCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"macaroon": m.macaroon}, nil
}

func (m macaroonCreds) RequireTransportSecurity() bool { return true }

// expandUser expands a leading '~' in path to the current user's home directory.
func expandUser(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			if path == "~" {
				return home
			}
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

// getCreds reads the macaroon and TLS certificate from the paths configured in
// LocalSettings and returns the two DialOptions needed to authenticate gRPC
// calls: one for TLS transport credentials and one for per-RPC macaroon
// metadata. Credentials are built lazily and cached; any file-read error
// surfaces on the first connection attempt.
func getCreds() ([]grpc.DialOption, error) {
	s := config.Get()

	// Read macaroon bytes and hex-encode them.
	macaroonBytes, err := os.ReadFile(expandUser(s.LND_MACAROON_PATH))
	if err != nil {
		return nil, err
	}
	macaroon := hex.EncodeToString(macaroonBytes)

	// Read and parse the TLS certificate.
	cert, err := os.ReadFile(expandUser(s.LND_TLS_PATH))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cert) {
		return nil, fmt.Errorf("failed to parse LND TLS cert at %s", s.LND_TLS_PATH)
	}
	certCreds := credentials.NewTLS(&tls.Config{RootCAs: pool})

	// Return TLS transport credentials and per-RPC macaroon as separate DialOptions.
	return []grpc.DialOption{
		grpc.WithTransportCredentials(certCreds),
		grpc.WithPerRPCCredentials(macaroonCreds{macaroon: macaroon}),
	}, nil
}

var (
	credsOnce  sync.Once
	credsValue []grpc.DialOption
	credsErr   error
)

// creds returns the cached DialOptions, building them once on first call.
func creds() ([]grpc.DialOption, error) {
	credsOnce.Do(func() {
		credsValue, credsErr = getCreds()
	})
	return credsValue, credsErr
}

// callOptions builds the max-message-size DialOption from LND_MAX_MESSAGE.
func callOptions() grpc.DialOption {
	maxMessage, _ := strconv.Atoi(config.Get().LND_MAX_MESSAGE)
	size := maxMessage * 1000000
	return grpc.WithDefaultCallOptions(
		grpc.MaxCallSendMsgSize(size),
		grpc.MaxCallRecvMsgSize(size),
	)
}

// lndConnect creates a new gRPC client connection to LND. The connection is
// lazy: no network traffic occurs until the first RPC call.
func lndConnect() (*grpc.ClientConn, error) {
	dialOpts, err := creds()
	if err != nil {
		return nil, err
	}
	opts := append([]grpc.DialOption{}, dialOpts...)
	opts = append(opts, callOptions())
	return grpc.NewClient(config.Get().LND_RPC_SERVER, opts...)
}

// asyncLndConnect creates an independent gRPC client connection identical to
// lndConnect. It exists as a separate constructor so the async shared channel
// has its own connection state.
func asyncLndConnect() (*grpc.ClientConn, error) {
	return lndConnect()
}

// Connect creates a fresh, independent LND connection (not the global shared
// channel). Used by the controller supervisor to give each daemon its own
// connection so that a channel reset does not affect the others.
func Connect() (*grpc.ClientConn, error) { return lndConnect() }

var (
	channelMu          sync.Mutex
	sharedChannel      *grpc.ClientConn
	sharedAsyncChannel *grpc.ClientConn
)

// GetSharedChannel returns the process-wide shared LND connection, creating it
// on first call.
func GetSharedChannel() (*grpc.ClientConn, error) {
	channelMu.Lock()
	defer channelMu.Unlock()
	if sharedChannel == nil {
		ch, err := lndConnect()
		if err != nil {
			return nil, err
		}
		sharedChannel = ch
	}
	return sharedChannel, nil
}

// GetSharedAsyncChannel returns the process-wide shared async LND connection,
// creating it on first call.
func GetSharedAsyncChannel() (*grpc.ClientConn, error) {
	channelMu.Lock()
	defer channelMu.Unlock()
	if sharedAsyncChannel == nil {
		ch, err := asyncLndConnect()
		if err != nil {
			return nil, err
		}
		sharedAsyncChannel = ch
	}
	return sharedAsyncChannel, nil
}

// CloseSharedChannel closes the shared LND connection and resets it to nil so
// the next call to GetSharedChannel creates a fresh one.
func CloseSharedChannel() {
	channelMu.Lock()
	defer channelMu.Unlock()
	if sharedChannel != nil {
		sharedChannel.Close()
		sharedChannel = nil
	}
}

// CloseSharedAsyncChannel closes the shared async LND connection. The channel
// pointer is always cleared via defer, even if Close returns an error.
func CloseSharedAsyncChannel() {
	channelMu.Lock()
	defer channelMu.Unlock()
	if sharedAsyncChannel != nil {
		defer func() { sharedAsyncChannel = nil }()
		sharedAsyncChannel.Close()
	}
}
