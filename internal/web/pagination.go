package web

import (
	"net/http"
	"net/url"
	"strconv"
)

// defaultPageSize is the default number of results per page.
const defaultPageSize = 100

// pagination holds the resolved limit/offset values of a request.
type pagination struct {
	limit  int
	offset int
}

// parsePagination parses limit and offset from query parameters:
//   - limit: must be strictly positive (> 0), otherwise defaults to 100.
//   - offset: must be non-negative (>= 0), otherwise defaults to 0.
//
// When a parameter appears multiple times, the last value is used.
func parsePagination(q url.Values) pagination {
	p := pagination{limit: defaultPageSize, offset: 0}
	if raw, ok := q["limit"]; ok && len(raw) > 0 {
		if n, err := strconv.Atoi(raw[len(raw)-1]); err == nil && n > 0 {
			p.limit = n
		}
	}
	if raw, ok := q["offset"]; ok && len(raw) > 0 {
		if n, err := strconv.Atoi(raw[len(raw)-1]); err == nil && n >= 0 {
			p.offset = n
		}
	}
	return p
}

// paginatedResponse is the standard paginated JSON envelope: {count, next, previous, results}
// in exactly this field order.
type paginatedResponse struct {
	Count    int `json:"count"`
	Next     any `json:"next"`
	Previous any `json:"previous"`
	Results  any `json:"results"`
}

// buildResponse assembles a paginatedResponse for the given total count and result slice.
func (p pagination) buildResponse(r *http.Request, count int, results any) paginatedResponse {
	return paginatedResponse{
		Count:    count,
		Next:     p.nextLink(r, count),
		Previous: p.prevLink(r),
		Results:  results,
	}
}

// nextLink returns nil when offset+limit >= count (no further page exists).
func (p pagination) nextLink(r *http.Request, count int) any {
	if p.offset+p.limit >= count {
		return nil
	}
	return buildPageURL(r, p.limit, p.offset+p.limit, false)
}

// prevLink returns nil when offset <= 0. Drops the offset parameter from the
// URL entirely when offset-limit <= 0 (i.e. the previous page starts at 0).
func (p pagination) prevLink(r *http.Request) any {
	if p.offset <= 0 {
		return nil
	}
	newOffset := p.offset - p.limit
	if newOffset <= 0 {
		return buildPageURL(r, p.limit, 0, true)
	}
	return buildPageURL(r, p.limit, newOffset, false)
}

// buildPageURL constructs an absolute URL with the given limit and offset,
// preserving any other query parameters (filters). When removeOffset is true,
// the offset parameter is omitted from the result URL.
func buildPageURL(r *http.Request, limit, offset int, removeOffset bool) string {
	scheme := requestScheme(r)
	q := r.URL.Query()
	q.Set("limit", strconv.Itoa(limit))
	if removeOffset {
		q.Del("offset")
	} else {
		q.Set("offset", strconv.Itoa(offset))
	}
	u := url.URL{Scheme: scheme, Host: r.Host, Path: r.URL.Path, RawQuery: q.Encode()}
	return u.String()
}
