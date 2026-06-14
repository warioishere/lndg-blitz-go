// Package web implements the HTTP server for the lndg-blitz web layer,
// providing a REST API with limit/offset pagination, filter query parameters,
// snake_case JSON, and HTML page rendering.
package web

import (
	"crypto/subtle"
	"html/template"
	"io/fs"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/warioishere/lndg-blitz-go/internal/config"
	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Server bundles config, database access, and the HTTP router for the web layer.
// db is used for dynamic list queries (pagination/filtering), queries for sqlc
// generated queries. Additional dependencies (LND clients) are wired in via options.
type Server struct {
	cfg       *config.Settings
	db        db.DBTX
	queries   *db.Queries
	lnd       *LND
	amboss    ambossFetcher
	txFees    txFeeFetcher
	templates map[string]*template.Template
	router    chi.Router
}

// Option configures the Server during construction.
type Option func(*Server)

// WithLND wires up the LND clients for action endpoints.
func WithLND(lnd *LND) Option {
	return func(s *Server) { s.lnd = lnd }
}

// WithAmboss overrides the Amboss fetcher (useful for testing).
func WithAmboss(a ambossFetcher) Option {
	return func(s *Server) { s.amboss = a }
}

// WithTxFeeFetcher overrides the mempool TX fee fetcher (useful for testing).
func WithTxFeeFetcher(t txFeeFetcher) Option {
	return func(s *Server) { s.txFees = t }
}

// NewServer constructs the Server and registers all routes.
func NewServer(cfg *config.Settings, dbtx db.DBTX, opts ...Option) *Server {
	s := &Server{cfg: cfg, db: dbtx, queries: db.New(dbtx), amboss: newHTTPAmbossFetcher(), txFees: newHTTPTxFeeFetcher()}
	for _, o := range opts {
		o(s)
	}
	tmpls, err := parseTemplates()
	if err != nil {
		panic(err) // embedded templates: parse errors are bugs
	}
	s.templates = tmpls
	s.router = s.routes()
	return s
}

// Handler returns the http.Handler for the HTTP server.
func (s *Server) Handler() http.Handler { return s.router }

func (s *Server) routes() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	if s.basicAuthEnabled() {
		r.Use(s.basicAuth)
	}

	// Static assets served under /static/.
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err) // embed guarantees the subdirectory exists
	}
	fileServer := http.StripPrefix("/static/", http.FileServer(http.FS(sub)))
	r.Handle("/static/*", fileServer)
	// favicon.ico redirects to /static/favicon.ico.
	r.Get("/favicon.ico", func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "/static/favicon.ico", http.StatusFound)
	})

	// HTML pages and REST API under /api/.
	s.mountPages(r)
	r.Route("/api", func(api chi.Router) {
		s.mountAPI(api)
		s.mountActions(api)
	})

	// Trailing-slash redirect: when a GET request matches no route but the same
	// path with a trailing slash does, issue a 301 redirect preserving the query
	// string. Several URL patterns require a trailing slash but are linked without one.
	r.NotFound(s.appendSlash)

	return r
}

// appendSlash implements trailing-slash redirect behaviour for GET requests.
func (s *Server) appendSlash(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodGet && !strings.HasSuffix(req.URL.Path, "/") {
		p := req.URL.Path + "/"
		rctx := chi.NewRouteContext()
		if s.router.Match(rctx, req.Method, p) {
			target := p
			if req.URL.RawQuery != "" {
				target += "?" + req.URL.RawQuery
			}
			http.Redirect(w, req, target, http.StatusMovedPermanently)
			return
		}
	}
	http.NotFound(w, req)
}

// basicAuthEnabled returns true when both credentials are configured.
func (s *Server) basicAuthEnabled() bool {
	return s.cfg.WEB_BASIC_AUTH_USER != "" && s.cfg.WEB_BASIC_AUTH_PASS != ""
}

// basicAuth is a simple HTTP Basic Auth middleware using constant-time comparisons.
func (s *Server) basicAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		user, pass, ok := req.BasicAuth()
		if ok {
			userOK := subtle.ConstantTimeCompare([]byte(user), []byte(s.cfg.WEB_BASIC_AUTH_USER)) == 1
			passOK := subtle.ConstantTimeCompare([]byte(pass), []byte(s.cfg.WEB_BASIC_AUTH_PASS)) == 1
			if userOK && passOK {
				next.ServeHTTP(w, req)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="LNDg"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}
