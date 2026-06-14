package web

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
)

// Flash messages are stored in a base64-encoded JSON cookie so that they
// survive a redirect and are displayed on the next page load, then cleared.
const flashCookie = "messages"

// flasher collects messages within a POST handler. The template displays only
// the text, so no severity level is tracked.
type flasher struct{ msgs []string }

func (f *flasher) add(msg string) { f.msgs = append(f.msgs, msg) }

// readFlashes reads flash messages from the cookie.
func readFlashes(r *http.Request) []string {
	c, err := r.Cookie(flashCookie)
	if err != nil || c.Value == "" {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c.Value)
	if err != nil {
		return nil
	}
	var msgs []string
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil
	}
	return msgs
}

// clearFlashes deletes the flash cookie after messages have been displayed.
func clearFlashes(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{Name: flashCookie, Value: "", Path: "/", MaxAge: -1})
}

// redirect writes any collected flash messages into the cookie and issues a
// 302 redirect to the given URL.
func (s *Server) redirect(w http.ResponseWriter, r *http.Request, url string, f *flasher) {
	if f != nil && len(f.msgs) > 0 {
		b, _ := json.Marshal(f.msgs)
		http.SetCookie(w, &http.Cookie{
			Name:  flashCookie,
			Value: base64.RawURLEncoding.EncodeToString(b),
			Path:  "/",
		})
	}
	http.Redirect(w, r, url, http.StatusFound)
}

// refererOr returns the request's Referer header, or fallback if it is absent.
func refererOr(r *http.Request, fallback string) string {
	if ref := r.Header.Get("Referer"); ref != "" {
		return ref
	}
	return fallback
}
