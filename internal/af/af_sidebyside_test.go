package af

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Side-by-side integration test: seeds a scenario into the database using an
// external harness, runs af.Main against the same database, and compares the
// per-channel results. Requires a local Postgres schema, a Python venv, and the
// harness script. The test is skipped when any prerequisite is absent
// (overridable via environment variables).

type sbsResult struct {
	ChanID            string  `json:"chan_id"`
	NewRate           float64 `json:"new_rate"`
	Adjustment        float64 `json:"adjustment"`
	NewInboundRate    float64 `json:"new_inbound_rate"`
	InboundAdjustment float64 `json:"inbound_adjustment"`
	OutPercent        int     `json:"out_percent"`
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func TestAfMainSideBySide(t *testing.T) {
	dbURL := envOr("LNDG_TEST_DB", "postgres://lndg:schema-export@localhost:15455/lndg?sslmode=disable")
	pyBin := envOr("LNDG_TEST_PY", "/tmp/lndg-schema-venv/bin/python")
	harness := envOr("LNDG_TEST_HARNESS", "testdata/harness/af_main_harness.py")
	repo := envOr("LNDG_TEST_REPO", "/home/warioishere/github_repos/lndg-blitz")

	// Resolve harness path to absolute so it remains valid when cmd.Dir changes.
	if abs, err := filepath.Abs(harness); err == nil {
		harness = abs
	}

	for _, p := range []string{pyBin, harness, repo} {
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

	for _, mode := range []string{"legacy", "curve"} {
		t.Run(mode, func(t *testing.T) {
			// 1. Run the harness to seed the database and collect reference results as JSON.
			cmd := exec.Command(pyBin, harness)
			cmd.Dir = repo
			cmd.Env = append(os.Environ(),
				"PYTHONPATH="+repo,
				"DJANGO_SETTINGS_MODULE=lndg.settings",
				"AF_MODE="+mode,
			)
			out, err := cmd.Output()
			if err != nil {
				if ee, ok := err.(*exec.ExitError); ok {
					t.Fatalf("python harness failed: %v\nstderr:\n%s", err, ee.Stderr)
				}
				t.Fatalf("python harness failed: %v", err)
			}
			var pyRows []sbsResult
			require.NoError(t, json.Unmarshal(out, &pyRows))
			require.NotEmpty(t, pyRows)

			// 2. Run af.Main against the same (seeded) database, using the same
			// channel set as the harness: is_open=True.
			q := db.New(pool)
			channels, err := q.ListOpenChannels(ctx)
			require.NoError(t, err)
			goRows, err := Main(ctx, q, channels, time.Now())
			require.NoError(t, err)

			goByID := map[string]*ChannelFeeRow{}
			for _, r := range goRows {
				goByID[r.ChanID] = r
			}
			require.Len(t, goRows, len(pyRows))

			const eps = 1e-9
			for _, py := range pyRows {
				g := goByID[py.ChanID]
				require.NotNil(t, g, "chan %s missing in Go result", py.ChanID)
				require.InDeltaf(t, py.NewRate, g.NewRate, eps, "chan %s new_rate", py.ChanID)
				require.InDeltaf(t, py.Adjustment, g.Adjustment, eps, "chan %s adjustment", py.ChanID)
				require.InDeltaf(t, py.NewInboundRate, g.NewInboundRate, eps, "chan %s new_inbound_rate", py.ChanID)
				require.InDeltaf(t, py.InboundAdjustment, g.InboundAdjustment, eps, "chan %s inbound_adjustment", py.ChanID)
				require.Equalf(t, py.OutPercent, g.OutPercent, "chan %s out_percent", py.ChanID)
			}
			t.Logf("%s mode: %d channels match Python af.main", mode, len(pyRows))
		})
	}
}
