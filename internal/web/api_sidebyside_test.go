package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// TestAPISideBySide seeds the database through the Django ORM, takes the
// Python API's answers for every ViewSet route and compares them with the Go
// API on the same database: status, key order, values and JSON types. Needs
// the Python repo, a Python venv with its requirements and the schema-export
// Postgres; skipped when any of them is missing (same setup as the AF test).
func TestAPISideBySide(t *testing.T) {
	dbURL := envOr("LNDG_TEST_DB", "postgres://lndg:schema-export@localhost:15455/lndg?sslmode=disable")
	pyBin := envOr("LNDG_TEST_PY", "/tmp/lndg-schema-venv/bin/python")
	repo := envOr("LNDG_TEST_REPO", "/home/warioishere/github_repos/lndg-blitz")
	harness, _ := filepath.Abs("testdata/harness/api_dump_harness.py")
	for _, p := range []string{pyBin, repo, harness} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("side-by-side prerequisite missing: %s", p)
		}
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("cannot connect to test DB: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("test DB not reachable: %v", err)
	}
	// The side-by-side tests of af and web share this database and run in
	// parallel packages: hold a session advisory lock for the whole test.
	lockConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer lockConn.Release()
	_, err = lockConn.Exec(ctx, `SELECT pg_advisory_lock(727274)`)
	require.NoError(t, err)
	defer lockConn.Exec(ctx, `SELECT pg_advisory_unlock(727274)`)

	cmd := exec.Command(pyBin, harness)
	cmd.Dir = repo
	cmd.Env = append(os.Environ(), "PYTHONPATH="+repo, "DJANGO_SETTINGS_MODULE=lndg.settings")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	require.NoError(t, err, stderr.String())
	var pyAnswers map[string][2]any
	require.NoError(t, json.Unmarshal(out, &pyAnswers))
	require.NotEmpty(t, pyAnswers)

	ts := httptest.NewServer(NewServer(&config.Settings{}, pool).Handler())
	defer ts.Close()

	paths := make([]string, 0, len(pyAnswers))
	for p := range pyAnswers {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, path := range paths {
		pyStatus := int(pyAnswers[path][0].(float64))
		pyBody := strings.ReplaceAll(pyAnswers[path][1].(string), "http://testserver", "HOST")

		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		goBody := strings.ReplaceAll(string(b), ts.URL, "HOST")

		require.Equalf(t, pyStatus, resp.StatusCode, "%s status", path)
		pyVal, err := decodeOrdered([]byte(pyBody))
		require.NoError(t, err, path)
		goVal, err := decodeOrdered([]byte(goBody))
		require.NoError(t, err, path)
		require.Equalf(t, pyVal, goVal, "%s body\npy: %s\ngo: %s", path, pyBody, goBody)
	}
	t.Logf("%d API responses match the Python API", len(paths))
}

// decodeOrdered decodes JSON keeping object key order: objects become
// [][2]any{key, value} slices and numbers "int:<v>" / "float:<v>" strings, so
// Python's 1e-05 and Go's 0.00001 compare equal while 7 and 7.0 do not.
func decodeOrdered(b []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			var obj [][2]any
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				obj = append(obj, [2]any{k, v})
			}
			_, err := dec.Token() // }
			return obj, err
		case '[':
			arr := []any{}
			for dec.More() {
				v, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			_, err := dec.Token() // ]
			return arr, err
		}
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			return nil, err
		}
		kind := "int"
		if strings.ContainsAny(t.String(), ".eE") {
			kind = "float"
		}
		return fmt.Sprintf("%s:%v", kind, f), nil
	}
	return tok, nil
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}
