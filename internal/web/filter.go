package web

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// filterKind determines how a query parameter value is parsed before binding.
type filterKind int

const (
	filterString filterKind = iota
	filterInt
	filterFloat
	filterBool
	filterDateTime
)

// lookupOps maps filter lookup names to their SQL operators.
var lookupOps = map[string]string{
	"exact": "=",
	"lt":    "<",
	"gt":    ">",
	"lte":   "<=",
	"gte":   ">=",
}

// filterParam describes an allowed filter query parameter. param is the query
// key ("status" for exact, otherwise "status__lt"); column is the DB column;
// op is the SQL operator.
type filterParam struct {
	param  string
	column string
	op     string
	kind   filterKind
	// orColumns is set for filters that test the same value against multiple columns
	// using OR (e.g. a combined chan_id_in OR chan_id_out filter).
	orColumns []string
}

// orFilterField creates a filter that compares a value against multiple columns with OR.
func orFilterField(param string, kind filterKind, columns ...string) filterParam {
	return filterParam{param: param, op: "=", kind: kind, orColumns: columns}
}

// filterFields builds the filterParam list for a column and the given lookups.
// For "exact" the query key is the column name itself; for others it is "col__lookup".
func filterFields(column string, kind filterKind, lookups ...string) []filterParam {
	out := make([]filterParam, 0, len(lookups))
	for _, lk := range lookups {
		op, ok := lookupOps[lk]
		if !ok {
			panic("unknown lookup: " + lk)
		}
		param := column
		if lk != "exact" {
			param = column + "__" + lk
		}
		out = append(out, filterParam{param: param, column: column, op: op, kind: kind})
	}
	return out
}

// datetimeLayouts are the accepted input formats for datetime filter values.
// All parsed values are treated as UTC.
var datetimeLayouts = []string{
	"2006-01-02T15:04:05.999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
	time.RFC3339Nano,
	time.RFC3339,
}

// applyFilters evaluates the allowed filter params against the query string and
// returns SQL WHERE clauses ($1..$N) with their arguments. An invalid int or
// datetime value sets badRequest=true (resulting in HTTP 400). Empty values and
// unparseable booleans are skipped.
func applyFilters(q url.Values, params []filterParam) (clauses []string, args []any, badRequest bool) {
	for _, fp := range params {
		raw, ok := q[fp.param]
		if !ok || len(raw) == 0 {
			continue
		}
		val := raw[len(raw)-1] // use the last value when the key appears multiple times
		if val == "" {
			continue
		}
		arg, skip, bad := parseFilterValue(val, fp.kind)
		if bad {
			return nil, nil, true
		}
		if skip {
			continue
		}
		args = append(args, arg)
		if len(fp.orColumns) > 0 {
			ors := make([]string, len(fp.orColumns))
			for i, col := range fp.orColumns {
				ors[i] = fmt.Sprintf("%s %s $%d", col, fp.op, len(args))
			}
			clauses = append(clauses, "("+strings.Join(ors, " OR ")+")")
			continue
		}
		clauses = append(clauses, fmt.Sprintf("%s %s $%d", fp.column, fp.op, len(args)))
	}
	return clauses, args, false
}

func parseFilterValue(val string, kind filterKind) (arg any, skip, bad bool) {
	switch kind {
	case filterInt:
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, false, true
		}
		return n, false, false
	case filterFloat:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, false, true
		}
		return f, false, false
	case filterBool:
		switch strings.ToLower(val) {
		case "true", "1":
			return true, false, false
		case "false", "0":
			return false, false, false
		default:
			// Unrecognized boolean value — skip the filter rather than erroring.
			return nil, true, false
		}
	case filterDateTime:
		for _, l := range datetimeLayouts {
			if t, err := time.Parse(l, val); err == nil {
				return t.UTC(), false, false
			}
		}
		return nil, false, true
	default: // filterString
		return val, false, false
	}
}

// whereClause builds a WHERE fragment from the given clauses, or returns "" if empty.
func whereClause(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}
