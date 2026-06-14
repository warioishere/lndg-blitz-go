// Package config provides bootstrap settings for the application, sourced from
// environment variables and an optional config file. Key names and defaults
// match the expected LND node configuration.
//
// Note: this covers only *bootstrap* settings. Runtime settings from the
// DB table LocalSettings (AR-Enabled, QR-UpdateHours, etc.) are handled
// separately in the DB layer.
package config

import (
	"bufio"
	"os"
	"strings"
	"sync"
)

// Settings holds LND connection parameters and application settings.
type Settings struct {
	LND_TLS_PATH      string
	LND_MACAROON_PATH string
	LND_DATABASE_PATH string
	LND_NETWORK       string
	LND_RPC_SERVER    string
	LND_MAX_MESSAGE   string
	// DATABASE_URL is the Postgres DSN used by pgxpool.
	DATABASE_URL string

	// Web layer settings.
	LOGIN_REQUIRED bool
	DEBUG          bool
	// WEB_BIND_ADDR is the host:port the HTTP server binds to.
	WEB_BIND_ADDR string
	// WEB_BASIC_AUTH_USER and WEB_BASIC_AUTH_PASS enable HTTP Basic Auth when both are set.
	WEB_BASIC_AUTH_USER string
	WEB_BASIC_AUTH_PASS string
}

var (
	settings *Settings
	once     sync.Once
)

// getenv returns the value of the environment variable key, or def if not set.
func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

// getbool parses common boolean string representations case-insensitively:
// 1/true/yes/on → true, 0/false/no/off/"" → false; returns def for unrecognised values.
func getbool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off", "":
		return false
	default:
		return def
	}
}

// loadFileIntoEnv reads an optional key=value config file and sets any keys
// not already present in the environment (env vars take precedence).
// The file path is taken from LNDG_CONFIG_FILE, falling back to ./lndg.conf if
// that file exists. Missing files are silently ignored. Lines starting with '#'
// and blank lines are skipped; values may be wrapped in " or ' quotes.
func loadFileIntoEnv() {
	path := os.Getenv("LNDG_CONFIG_FILE")
	if path == "" {
		if _, err := os.Stat("lndg.conf"); err != nil {
			return
		}
		path = "lndg.conf"
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, val)
		}
	}
}

// load builds a Settings instance from environment variables, applying defaults
// suitable for a local node.
func load() *Settings {
	return &Settings{
		LND_TLS_PATH:      getenv("LND_TLS_PATH", "~/.lnd/tls.cert"),
		LND_MACAROON_PATH: getenv("LND_MACAROON_PATH", "~/.lnd/data/chain/bitcoin/mainnet/admin.macaroon"),
		LND_DATABASE_PATH: getenv("LND_DATABASE_PATH", "~/.lnd/data/graph/mainnet/channel.db"),
		LND_NETWORK:       getenv("LND_NETWORK", "mainnet"),
		LND_RPC_SERVER:    getenv("LND_RPC_SERVER", "localhost:10009"),
		LND_MAX_MESSAGE:   getenv("LND_MAX_MESSAGE", "35"),
		DATABASE_URL:      getenv("DATABASE_URL", "postgres://lndg:lndg@localhost:5432/lndg?sslmode=disable"),

		LOGIN_REQUIRED:      getbool("LOGIN_REQUIRED", false),
		DEBUG:               getbool("DEBUG", true),
		WEB_BIND_ADDR:       getenv("WEB_BIND_ADDR", "0.0.0.0:8889"),
		WEB_BASIC_AUTH_USER: getenv("WEB_BASIC_AUTH_USER", ""),
		WEB_BASIC_AUTH_PASS: getenv("WEB_BASIC_AUTH_PASS", ""),
	}
}

// Get returns the process-wide Settings, loading them lazily on first call.
func Get() *Settings {
	once.Do(func() {
		loadFileIntoEnv()
		settings = load()
	})
	return settings
}
