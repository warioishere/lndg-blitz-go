package web

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// With LOGIN_REQUIRED the web server takes logins from the Django app's
// auth_user table in the shared database, so the LNDg login keeps working and
// no password sits in the config. Browsers log in on a form and get a session
// cookie; API clients may send HTTP Basic Auth to /api/ instead. Without an active
// Django user (or without the table) every request is refused.

// djangoLogins remembers verified logins: a Django hash takes hundreds of
// thousands of PBKDF2 rounds, and Basic Auth sends the login with every request.
type djangoLogins struct {
	mu    sync.Mutex
	valid map[[32]byte]struct{} // sha256 of user, password and stored hash
}

const djangoLoginsMax = 64

const (
	loginPath     = "/lndg-admin/login/"
	sessionCookie = "lndg_session"
	sessionAge    = 14 * 24 * time.Hour // Django's SESSION_COOKIE_AGE default
)

// djangoAuth lets a request through with a valid session cookie; the API also
// takes a Basic Auth login. Pages ignore Basic Auth, so a browser that still
// sends a remembered one is logged out by /logout/. Otherwise a page GET goes
// to the login form, everything else gets 401.
func (s *Server) djangoAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		p := req.URL.Path
		if strings.HasPrefix(p, "/static/") || p == "/favicon.ico" || p == loginPath || p == "/logout/" {
			next.ServeHTTP(w, req)
			return
		}
		if s.sessionOK(req) {
			next.ServeHTTP(w, req)
			return
		}
		api := strings.HasPrefix(p, "/api/")
		if user, pass, ok := req.BasicAuth(); ok && api && s.djangoLoginOK(req.Context(), user, pass) {
			next.ServeHTTP(w, req)
			return
		}
		if req.Method == http.MethodGet && !api {
			http.Redirect(w, req, loginPath+"?next="+url.QueryEscape(req.URL.RequestURI()), http.StatusFound)
			return
		}
		writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "Authentication credentials were not provided."})
	})
}

func (s *Server) handleLoginForm(w http.ResponseWriter, r *http.Request) {
	s.renderTemplate(w, r, "login.html", map[string]any{"next": safeNext(r.URL.Query().Get("next"))})
}

// handleLogin checks the form login and sets the session cookie.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	user, pass, next := r.PostFormValue("username"), r.PostFormValue("password"), safeNext(r.PostFormValue("next"))
	stored, ok := s.storedPassword(r.Context(), user)
	if !ok || !s.djangoLoginOK(r.Context(), user, pass) {
		s.renderTemplate(w, r, "login.html", map[string]any{"next": next, "username": user,
			"error": "Please enter a correct username and password."})
		return
	}
	expires := time.Now().Add(sessionAge)
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: sessionValue(user, expires.Unix(), stored),
		Path: "/", Expires: expires, HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil})
	http.Redirect(w, r, next, http.StatusFound)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, "/", http.StatusFound)
}

// safeNext keeps the redirect after login on this site.
func safeNext(next string) string {
	if !strings.HasPrefix(next, "/") || strings.HasPrefix(next, "//") || strings.HasPrefix(next, "/\\") {
		return "/"
	}
	return next
}

// sessionValue signs user and expiry with the stored password hash, so the
// cookie needs no server-side state, survives restarts and dies with a
// password change.
func sessionValue(user string, expires int64, stored string) string {
	msg := base64.RawURLEncoding.EncodeToString([]byte(user)) + "." + strconv.FormatInt(expires, 10)
	mac := hmac.New(sha256.New, []byte(stored))
	mac.Write([]byte(msg))
	return msg + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// sessionOK checks the session cookie against the active Django user.
func (s *Server) sessionOK(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return false
	}
	parts := strings.Split(c.Value, ".")
	if len(parts) != 3 {
		return false
	}
	user, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	expires, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || time.Now().Unix() >= expires {
		return false
	}
	stored, ok := s.storedPassword(r.Context(), string(user))
	if !ok {
		return false
	}
	return hmac.Equal([]byte(c.Value), []byte(sessionValue(string(user), expires, stored)))
}

// storedPassword returns the password hash of the active Django user.
func (s *Server) storedPassword(ctx context.Context, user string) (string, bool) {
	var stored string
	if err := s.db.QueryRow(ctx, `SELECT password FROM auth_user WHERE username = $1 AND is_active`, user).Scan(&stored); err != nil {
		return "", false
	}
	return stored, true
}

// djangoLoginOK checks user and password against the active Django user. The
// stored hash is part of the cache key, so a password change takes effect at once.
func (s *Server) djangoLoginOK(ctx context.Context, user, pass string) bool {
	stored, ok := s.storedPassword(ctx, user)
	if !ok {
		return false
	}
	key := sha256.Sum256([]byte(user + "\x00" + pass + "\x00" + stored))
	s.logins.mu.Lock()
	_, known := s.logins.valid[key]
	s.logins.mu.Unlock()
	if known {
		return true
	}
	if !checkDjangoPassword(pass, stored) {
		return false
	}
	s.logins.mu.Lock()
	if s.logins.valid == nil || len(s.logins.valid) >= djangoLoginsMax {
		s.logins.valid = map[[32]byte]struct{}{}
	}
	s.logins.valid[key] = struct{}{}
	s.logins.mu.Unlock()
	return true
}

// checkDjangoPassword verifies password against a Django pbkdf2_sha256 hash
// ("pbkdf2_sha256$<iterations>$<salt>$<base64 key>"); other hashers are refused.
func checkDjangoPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != "pbkdf2_sha256" {
		return false
	}
	iter, err := strconv.Atoi(parts[1])
	if err != nil || iter <= 0 {
		return false
	}
	want, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil || len(want) == 0 {
		return false
	}
	got, err := pbkdf2.Key(sha256.New, password, []byte(parts[2]), iter, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}
