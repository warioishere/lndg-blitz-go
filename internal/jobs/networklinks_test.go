package jobs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

type fakeNetLinksQ struct {
	store map[string]string
}

func (f *fakeNetLinksQ) GetOrCreateLocalSetting(ctx context.Context, arg db.GetOrCreateLocalSettingParams) (db.GuiLocalsetting, error) {
	if v, ok := f.store[arg.Key]; ok {
		return db.GuiLocalsetting{Key: arg.Key, Value: v}, nil
	}
	if f.store == nil {
		f.store = map[string]string{}
	}
	f.store[arg.Key] = arg.Value
	return db.GuiLocalsetting{Key: arg.Key, Value: arg.Value}, nil
}

func TestNetworkLinks_DefaultAndExisting(t *testing.T) {
	q := &fakeNetLinksQ{store: map[string]string{}}
	v, err := NetworkLinks(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, "https://mempool.space", v)
	assert.Equal(t, "https://mempool.space", q.store["GUI-NetLinks"], "default persisted")

	q.store["GUI-NetLinks"] = "https://custom.example"
	v, err = NetworkLinks(context.Background(), q)
	require.NoError(t, err)
	assert.Equal(t, "https://custom.example", v)
}

func TestGetTxFees(t *testing.T) {
	// Mock mempool API: returns {"fee": 1234} for /api/tx/<txid>.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tx/goodtx" {
			w.Write([]byte(`{"fee": 1234, "vsize": 200}`))
			return
		}
		if r.URL.Path == "/api/tx/nofee" {
			w.Write([]byte(`{"vsize": 200}`))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	q := &fakeNetLinksQ{store: map[string]string{"GUI-NetLinks": srv.URL}}

	fee, err := GetTxFees(context.Background(), q, "mainnet", "goodtx")
	require.NoError(t, err)
	assert.Equal(t, 1234, fee)

	// Missing 'fee' field -> silent 0.
	fee, err = GetTxFees(context.Background(), q, "mainnet", "nofee")
	require.NoError(t, err)
	assert.Equal(t, 0, fee)

	// 404 returns invalid JSON -> silent 0.
	fee, err = GetTxFees(context.Background(), q, "mainnet", "missing")
	require.NoError(t, err)
	assert.Equal(t, 0, fee)
}

func TestDataLogFormat(t *testing.T) {
	// Layout matching C's %c in the C locale: "Wed Jun 11 14:30:05 2025".
	tm := time.Date(2025, 6, 11, 14, 30, 5, 0, time.UTC)
	assert.Equal(t, "Wed Jun 11 14:30:05 2025", tm.Format(cTimeLayout))
	// Single-digit day is space-padded (%e).
	tm2 := time.Date(2025, 6, 1, 9, 5, 3, 0, time.UTC)
	assert.Equal(t, "Sun Jun  1 09:05:03 2025", tm2.Format(cTimeLayout))
}
