package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warioishere/lndg-blitz-go/internal/config"
)

// Django format, generated with hashlib.pbkdf2_hmac('sha256', b'correct horse', b'Xy7salt9', 1000).
const testDjangoHash = "pbkdf2_sha256$1000$Xy7salt9$KMryrPp8/IZ0du9B0ooCJIohXWbiBW8sA0Tm+jnuWPY="

func TestCheckDjangoPassword(t *testing.T) {
	assert.True(t, checkDjangoPassword("correct horse", testDjangoHash))
	assert.False(t, checkDjangoPassword("wrong", testDjangoHash))
	assert.False(t, checkDjangoPassword("correct horse", "argon2$argon2id$v=19$m=102400,t=2,p=8$c2FsdA$aGFzaA"), "other hasher")
	assert.False(t, checkDjangoPassword("correct horse", "pbkdf2_sha256$x$salt$aGFzaA=="), "bad iterations")
	assert.False(t, checkDjangoPassword("correct horse", ""), "empty")
}

func TestDjangoBasicAuthIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-based test in -short mode")
	}
	pool, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	status := func(ts *httptest.Server, user, pass string) int {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/channels/", nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	ts := httptest.NewServer(NewServer(&config.Settings{LOGIN_REQUIRED: true}, pool).Handler())
	defer ts.Close()

	// the Go schema has no auth_user (a Django table): refuse instead of opening up
	assert.Equal(t, 401, status(ts, "lndg-admin", "correct horse"), "no auth_user table")

	_, err := pool.Exec(ctx, `CREATE TABLE auth_user (id serial PRIMARY KEY, username varchar(150) UNIQUE NOT NULL,
		password varchar(128) NOT NULL, is_active boolean NOT NULL)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO auth_user (username, password, is_active) VALUES ('lndg-admin', $1, true), ('old', $1, false)`, testDjangoHash)
	require.NoError(t, err)
	assert.Equal(t, 401, status(ts, "", ""), "no login")
	assert.Equal(t, 401, status(ts, "lndg-admin", "wrong"))
	assert.Equal(t, 200, status(ts, "lndg-admin", "correct horse"))
	assert.Equal(t, 200, status(ts, "lndg-admin", "correct horse"), "cached")
	assert.Equal(t, 401, status(ts, "old", "correct horse"), "inactive user")

	// a password change (new hash) invalidates the remembered login
	_, err = pool.Exec(ctx, `UPDATE auth_user SET password = 'pbkdf2_sha256$1000$other$aGFzaA==' WHERE username = 'lndg-admin'`)
	require.NoError(t, err)
	assert.Equal(t, 401, status(ts, "lndg-admin", "correct horse"), "old password after change")

	// static Basic Auth credentials take precedence
	static := httptest.NewServer(NewServer(&config.Settings{LOGIN_REQUIRED: true, WEB_BASIC_AUTH_USER: "u", WEB_BASIC_AUTH_PASS: "p"}, pool).Handler())
	defer static.Close()
	assert.Equal(t, 200, status(static, "u", "p"))
	assert.Equal(t, 401, status(static, "lndg-admin", "correct horse"))
}
