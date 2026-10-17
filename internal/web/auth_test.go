package web

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

func TestDjangoAuthIntegration(t *testing.T) {
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
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	login := func(user, pass, next string) *http.Response {
		resp, err := noRedirect.PostForm(ts.URL+loginPath, url.Values{"username": {user}, "password": {pass}, "next": {next}})
		require.NoError(t, err)
		resp.Body.Close()
		return resp
	}
	get := func(path string, c *http.Cookie) *http.Response {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+path, nil)
		if c != nil {
			req.AddCookie(c)
		}
		resp, err := noRedirect.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp
	}
	session := func(resp *http.Response) *http.Cookie {
		for _, c := range resp.Cookies() {
			if c.Name == sessionCookie {
				return c
			}
		}
		return nil
	}

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

	// browsers: a page without session goes to the form, the API answers 401 without a Basic challenge
	resp := get("/channels?x=1", nil)
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, loginPath+"?next="+url.QueryEscape("/channels?x=1"), resp.Header.Get("Location"))
	resp = get("/api/channels/", nil)
	assert.Equal(t, 401, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("WWW-Authenticate"))
	assert.Equal(t, 200, get("/static/w3style.css", nil).StatusCode, "static without login")
	formResp, err := http.Get(ts.URL + loginPath + "?next=/channels")
	require.NoError(t, err)
	form, _ := io.ReadAll(formResp.Body)
	formResp.Body.Close()
	assert.Contains(t, string(form), `name="next" value="/channels"`)

	resp = login("lndg-admin", "wrong", "/channels")
	assert.Equal(t, 200, resp.StatusCode, "form again")
	assert.Nil(t, session(resp))
	assert.Nil(t, session(login("old", "correct horse", "/")), "inactive user")
	resp = login("lndg-admin", "correct horse", "/channels")
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, "/channels", resp.Header.Get("Location"))
	c := session(resp)
	require.NotNil(t, c)
	assert.True(t, c.HttpOnly)
	assert.Equal(t, http.SameSiteLaxMode, c.SameSite)
	assert.Equal(t, 200, get("/api/channels/", c).StatusCode)
	assert.Equal(t, "/", login("lndg-admin", "correct horse", "//evil.example").Header.Get("Location"), "next stays on site")

	parts := strings.Split(c.Value, ".")
	tampered := &http.Cookie{Name: sessionCookie, Value: parts[0] + "." + parts[1] + "9." + parts[2]} // later expiry, same signature
	assert.Equal(t, 401, get("/api/channels/", tampered).StatusCode)
	expired := &http.Cookie{Name: sessionCookie, Value: sessionValue("lndg-admin", 1, testDjangoHash)}
	assert.Equal(t, 401, get("/api/channels/", expired).StatusCode)

	basicPage, _ := http.NewRequest(http.MethodGet, ts.URL+"/channels", nil)
	basicPage.SetBasicAuth("lndg-admin", "correct horse")
	resp, err = noRedirect.Do(basicPage)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusFound, resp.StatusCode, "pages ignore Basic Auth")

	resp = get("/logout/", c)
	assert.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, -1, session(resp).MaxAge)

	// a password change (new hash) invalidates the remembered login
	_, err = pool.Exec(ctx, `UPDATE auth_user SET password = 'pbkdf2_sha256$1000$other$aGFzaA==' WHERE username = 'lndg-admin'`)
	require.NoError(t, err)
	assert.Equal(t, 401, status(ts, "lndg-admin", "correct horse"), "old password after change")
	assert.Equal(t, 401, get("/api/channels/", c).StatusCode, "session after password change")

	// static Basic Auth credentials take precedence
	static := httptest.NewServer(NewServer(&config.Settings{LOGIN_REQUIRED: true, WEB_BASIC_AUTH_USER: "u", WEB_BASIC_AUTH_PASS: "p"}, pool).Handler())
	defer static.Close()
	assert.Equal(t, 200, status(static, "u", "p"))
	assert.Equal(t, 401, status(static, "lndg-admin", "correct horse"))
}
