package web

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// With LOGIN_REQUIRED the web server takes HTTP Basic Auth logins from the
// Django app's auth_user table in the shared database, so the LNDg login keeps
// working and no password sits in the config. Without an active Django user
// (or without the table) every request is refused.

// djangoLogins remembers verified logins: a Django hash takes hundreds of
// thousands of PBKDF2 rounds, and Basic Auth sends the login with every request.
type djangoLogins struct {
	mu    sync.Mutex
	valid map[[32]byte]struct{} // sha256 of user, password and stored hash
}

const djangoLoginsMax = 64

func (s *Server) djangoBasicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		user, pass, ok := req.BasicAuth()
		if ok && s.djangoLoginOK(req.Context(), user, pass) {
			next.ServeHTTP(w, req)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="LNDg"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// djangoLoginOK checks user and password against the active Django user. The
// stored hash is part of the cache key, so a password change takes effect at once.
func (s *Server) djangoLoginOK(ctx context.Context, user, pass string) bool {
	var stored string
	if err := s.db.QueryRow(ctx, `SELECT password FROM auth_user WHERE username = $1 AND is_active`, user).Scan(&stored); err != nil {
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
