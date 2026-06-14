package web

import (
	"context"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// queryMaps executes a query and returns rows as []map[string]any
// (column name -> value), suitable as a template context. NULL columns become
// nil; timestamptz columns become time.Time.
func (s *Server) queryMaps(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToMap)
}

// singleQueryParam extracts the value of a bare "?=<value>" query string, as
// used by several views (e.g. /resolutions?=<chan_id>, /route?=<payment_hash>).
func singleQueryParam(r *http.Request) string {
	return strings.TrimPrefix(r.URL.RawQuery, "=")
}
