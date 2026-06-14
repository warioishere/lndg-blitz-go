package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestOrderedMapPreservesOrder(t *testing.T) {
	m := newOrderedMap()
	m.Set("zebra", 1).Set("apple", 2).Set("mango", nil)
	b, err := json.Marshal(m)
	require.NoError(t, err)
	require.Equal(t, `{"zebra":1,"apple":2,"mango":null}`, string(b))
}

func TestDRFDateTime(t *testing.T) {
	// microseconds == 0 -> no fractional part.
	tz := pgtype.Timestamptz{Time: time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC), Valid: true}
	require.Equal(t, "2024-01-02T15:04:05", drfDateTime(tz))

	// microseconds != 0 -> 6-digit fraction, no trailing trim.
	tz = pgtype.Timestamptz{Time: time.Date(2024, 1, 2, 15, 4, 5, 100000000, time.UTC), Valid: true}
	require.Equal(t, "2024-01-02T15:04:05.100000", drfDateTime(tz))

	// NULL -> nil.
	require.Nil(t, drfDateTime(pgtype.Timestamptz{}))

	// Non-UTC is converted to UTC (wall clock shifts accordingly).
	loc := time.FixedZone("CET", 3600)
	tz = pgtype.Timestamptz{Time: time.Date(2024, 1, 2, 15, 4, 5, 0, loc), Valid: true}
	require.Equal(t, "2024-01-02T14:04:05", drfDateTime(tz))
}

func TestParsePagination(t *testing.T) {
	cases := []struct {
		query      string
		wantLimit  int
		wantOffset int
	}{
		{"", 100, 0},
		{"limit=25&offset=50", 25, 50},
		{"limit=0", 100, 0},           // strictly positive -> default
		{"limit=-5", 100, 0},          // negative -> default
		{"limit=abc", 100, 0},         // non-int -> default
		{"offset=-1", 100, 0},         // negative -> 0
		{"offset=xyz", 100, 0},        // non-int -> 0
		{"limit=10&limit=20", 100, 0}, // last value wins? see below
	}
	for _, c := range cases {
		q, _ := url.ParseQuery(c.query)
		p := parsePagination(q)
		// special case: last value wins -> limit=20 expected
		if c.query == "limit=10&limit=20" {
			require.Equal(t, 20, p.limit, c.query)
			continue
		}
		require.Equal(t, c.wantLimit, p.limit, c.query)
		require.Equal(t, c.wantOffset, p.offset, c.query)
	}
}

func TestPaginationLinks(t *testing.T) {
	mk := func(rawquery string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/api/payments/?"+rawquery, nil)
		r.Host = "example.com"
		return r
	}

	// Middle page: next + previous both set.
	p := pagination{limit: 10, offset: 10}
	next := p.nextLink(mk("limit=10&offset=10"), 100)
	require.Equal(t, "http://example.com/api/payments/?limit=10&offset=20", next)
	prev := p.prevLink(mk("limit=10&offset=10"))
	// offset-limit == 0 -> offset param removed
	require.Equal(t, "http://example.com/api/payments/?limit=10", prev)

	// First page: previous == nil.
	p = pagination{limit: 10, offset: 0}
	require.Nil(t, p.prevLink(mk("limit=10")))

	// Last page: next == nil (offset+limit >= count).
	p = pagination{limit: 10, offset: 95}
	require.Nil(t, p.nextLink(mk("limit=10&offset=95"), 100))

	// previous with offset-limit > 0 keeps offset.
	p = pagination{limit: 10, offset: 25}
	prev = p.prevLink(mk("limit=10&offset=25"))
	require.Equal(t, "http://example.com/api/payments/?limit=10&offset=15", prev)

	// Filter params are preserved in the link.
	p = pagination{limit: 10, offset: 0}
	next = p.nextLink(mk("status=2&limit=10"), 50)
	u, _ := url.Parse(next.(string))
	require.Equal(t, "2", u.Query().Get("status"))
	require.Equal(t, "10", u.Query().Get("limit"))
	require.Equal(t, "10", u.Query().Get("offset"))
}

func TestApplyFilters(t *testing.T) {
	params := []filterParam{}
	params = append(params, filterFields("status", filterInt, "exact", "lt", "gt")...)
	params = append(params, filterFields("creation_date", filterDateTime, "lte", "gte")...)
	params = append(params, filterFields("chan_out", filterString, "exact")...)
	params = append(params, filterFields("is_open", filterBool, "exact")...)

	// exact + lt combined
	q, _ := url.ParseQuery("status=2&status__lt=3")
	clauses, args, bad := applyFilters(q, params)
	require.False(t, bad)
	require.Equal(t, []string{"status = $1", "status < $2"}, clauses)
	require.Equal(t, []any{int64(2), int64(3)}, args)

	// datetime gte
	q, _ = url.ParseQuery("creation_date__gte=2024-01-02T15:04:05")
	clauses, args, bad = applyFilters(q, params)
	require.False(t, bad)
	require.Equal(t, []string{"creation_date >= $1"}, clauses)
	require.Equal(t, time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC), args[0])

	// bool true
	q, _ = url.ParseQuery("is_open=true")
	clauses, args, _ = applyFilters(q, params)
	require.Equal(t, []string{"is_open = $1"}, clauses)
	require.Equal(t, []any{true}, args)

	// unknown bool value -> skipped (no filter, no error)
	q, _ = url.ParseQuery("is_open=maybe")
	clauses, _, bad = applyFilters(q, params)
	require.False(t, bad)
	require.Empty(t, clauses)

	// empty value -> skipped
	q, _ = url.ParseQuery("status=")
	clauses, _, bad = applyFilters(q, params)
	require.False(t, bad)
	require.Empty(t, clauses)

	// invalid int -> badRequest
	q, _ = url.ParseQuery("status=abc")
	_, _, bad = applyFilters(q, params)
	require.True(t, bad)

	// unknown param -> ignored
	q, _ = url.ParseQuery("unknown=5")
	clauses, _, bad = applyFilters(q, params)
	require.False(t, bad)
	require.Empty(t, clauses)
}

func TestColumnsOf(t *testing.T) {
	type sample struct {
		A int    `json:"col_a"`
		B string `json:"col_b,omitempty"`
		C bool   `json:"timestamp"`
	}
	require.Equal(t, `"col_a", "col_b", "timestamp"`, columnsOf[sample]())
}
