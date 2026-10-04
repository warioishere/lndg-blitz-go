package web

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// listSpec configures a read-only list endpoint. selectExpr and fromExpr allow
// joins and computed columns in addition to plain tables; they must match the
// field order and count of the scan type T.
type listSpec struct {
	selectExpr string        // columns or expressions after SELECT
	fromExpr   string        // table (and joins) after FROM
	order      string        // ORDER BY body, e.g. "creation_date DESC"; "" = no ordering
	filters    []filterParam // allowed query parameter filters
	paginate   bool          // false = return a bare list with no pagination wrapper
}

// columnsOf returns a quoted comma-separated column list derived from the json
// tags of a sqlc struct, in field order. Quoting protects reserved names such as "timestamp".
func columnsOf[T any]() string {
	var zero T
	t := reflect.TypeOf(zero)
	parts := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name := strings.Split(t.Field(i).Tag.Get("json"), ",")[0]
		parts = append(parts, `"`+name+`"`)
	}
	return strings.Join(parts, ", ")
}

// scanRows reads all rows positionally into []T (fields in struct declaration order).
func scanRows[T any](rows pgx.Rows) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		var item T
		v := reflect.ValueOf(&item).Elem()
		dests := make([]any, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			dests[i] = v.Field(i).Addr().Interface()
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// executeList runs a COUNT query followed by a SELECT (with optional pagination)
// and returns the scanned rows plus the total count (0 when pagination is disabled).
func executeList[T any](ctx context.Context, dbtx db.DBTX, spec listSpec, clauses []string, args []any, p pagination) (items []T, count int, err error) {
	where := whereClause(clauses)
	order := ""
	if spec.order != "" {
		order = " ORDER BY " + spec.order
	}

	if spec.paginate {
		countSQL := "SELECT count(*) FROM " + spec.fromExpr + where
		if err = dbtx.QueryRow(ctx, countSQL, args...).Scan(&count); err != nil {
			return nil, 0, err
		}
		selSQL := "SELECT " + spec.selectExpr + " FROM " + spec.fromExpr + where + order +
			fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
		selArgs := append(append([]any{}, args...), p.limit, p.offset)
		rows, qerr := dbtx.Query(ctx, selSQL, selArgs...)
		if qerr != nil {
			return nil, 0, qerr
		}
		items, err = scanRows[T](rows)
		return items, count, err
	}

	selSQL := "SELECT " + spec.selectExpr + " FROM " + spec.fromExpr + where + order
	rows, qerr := dbtx.Query(ctx, selSQL, args...)
	if qerr != nil {
		return nil, 0, qerr
	}
	items, err = scanRows[T](rows)
	return items, 0, err
}

// listHandler returns the HTTP handler for a read-only list endpoint: parses
// filters, loads the list, translates each row via toResult into the response
// shape, and writes a paginated or bare JSON response.
func listHandler[T any](dbtx db.DBTX, spec listSpec, toResult func(*http.Request, *T) *orderedMap) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		clauses, args, bad := applyFilters(q, spec.filters)
		if bad {
			writeDRFError(w, http.StatusBadRequest, "Invalid filter value.")
			return
		}

		p := parsePagination(q)
		items, count, err := executeList[T](r.Context(), dbtx, spec, clauses, args, p)
		if err != nil {
			writeDRFError(w, http.StatusInternalServerError, err.Error())
			return
		}

		results := make([]any, 0, len(items))
		for i := range items {
			results = append(results, toResult(r, &items[i]))
		}

		if spec.paginate {
			writeJSON(w, http.StatusOK, p.buildResponse(r, count, results))
			return
		}
		writeJSON(w, http.StatusOK, results)
	}
}
