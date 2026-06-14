package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetbool(t *testing.T) {
	cases := map[string]bool{
		"True": true, "true": true, "1": true, "yes": true, "on": true,
		"False": false, "false": false, "0": false, "no": false, "off": false, "": false,
	}
	for val, want := range cases {
		os.Setenv("TEST_BOOL_KEY", val)
		require.Equal(t, want, getbool("TEST_BOOL_KEY", true), "value %q", val)
	}
	os.Unsetenv("TEST_BOOL_KEY")
	require.True(t, getbool("TEST_BOOL_KEY", true))
	require.False(t, getbool("TEST_BOOL_KEY", false))
}

func TestLoadFileIntoEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "lndg.conf")
	content := "" +
		"# Kommentar\n" +
		"\n" +
		"LND_NETWORK = testnet\n" +
		"WEB_BIND_ADDR=\"127.0.0.1:9999\"\n" +
		"WEB_BASIC_AUTH_USER='alice'\n" +
		"DATABASE_URL=postgres://x\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	// Env vars take precedence: keys already set are not overwritten.
	os.Setenv("LND_NETWORK", "mainnet")
	defer os.Unsetenv("LND_NETWORK")
	for _, k := range []string{"WEB_BIND_ADDR", "WEB_BASIC_AUTH_USER", "DATABASE_URL"} {
		os.Unsetenv(k)
		defer os.Unsetenv(k)
	}

	os.Setenv("LNDG_CONFIG_FILE", path)
	defer os.Unsetenv("LNDG_CONFIG_FILE")

	loadFileIntoEnv()

	require.Equal(t, "mainnet", os.Getenv("LND_NETWORK"))          // env takes precedence
	require.Equal(t, "127.0.0.1:9999", os.Getenv("WEB_BIND_ADDR")) // double quotes stripped
	require.Equal(t, "alice", os.Getenv("WEB_BASIC_AUTH_USER"))    // single quotes stripped
	require.Equal(t, "postgres://x", os.Getenv("DATABASE_URL"))
}
