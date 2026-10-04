package web

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"math"
	"math/big"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// templateFS embeds the Go html/template files that make up the dashboard UI.
// base.html is the shared layout; pages override its blocks
// via {{define "content"}} / {{define "title"}}.
//
//go:embed templates
var templateFS embed.FS

// parseTemplates builds a separate template set per page (base + all partials +
// the page file) so that "content" blocks from different pages don't collide.
// Files containing {{define "content"}} are treated as pages; everything else
// (except base.html) is treated as a partial.
func parseTemplates() (map[string]*template.Template, error) {
	entries, err := templateFS.ReadDir("templates")
	if err != nil {
		return nil, err
	}
	base, err := templateFS.ReadFile("templates/base.html")
	if err != nil {
		return nil, err
	}

	var partials []struct{ name, content string }
	var pages []struct{ name, content string }
	for _, e := range entries {
		if e.IsDir() || e.Name() == "base.html" {
			continue
		}
		b, err := templateFS.ReadFile("templates/" + e.Name())
		if err != nil {
			return nil, err
		}
		c := string(b)
		if strings.Contains(c, `{{define "content"}}`) {
			pages = append(pages, struct{ name, content string }{e.Name(), c})
		} else {
			partials = append(partials, struct{ name, content string }{e.Name(), c})
		}
	}

	result := make(map[string]*template.Template, len(pages))
	for _, p := range pages {
		t := template.New("base.html").Funcs(templateFuncs())
		if _, err := t.Parse(string(base)); err != nil {
			return nil, fmt.Errorf("parse base.html: %w", err)
		}
		for _, pt := range partials {
			if _, err := t.New(pt.name).Parse(pt.content); err != nil {
				return nil, fmt.Errorf("parse %s: %w", pt.name, err)
			}
		}
		if _, err := t.Parse(p.content); err != nil {
			return nil, fmt.Errorf("parse %s: %w", p.name, err)
		}
		result[p.name] = t
	}
	return result, nil
}

// renderTemplate executes a page template with the shared layout. It injects
// the values that base.html always needs (graph_links, network_links, network,
// darkmode, messages, csrf_token) so individual handlers don't have to.
func (s *Server) renderTemplate(w http.ResponseWriter, r *http.Request, name string, ctx map[string]any) {
	t, ok := s.templates[name]
	if !ok {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return
	}
	if ctx == nil {
		ctx = map[string]any{}
	}
	reqCtx := r.Context()
	if _, ok := ctx["graph_links"]; !ok {
		ctx["graph_links"] = s.graphLinks(reqCtx)
	}
	if _, ok := ctx["network_links"]; !ok {
		ctx["network_links"] = s.networkLinks(reqCtx)
	}
	if _, ok := ctx["network"]; !ok {
		ctx["network"] = s.networkPrefix()
	}
	if _, ok := ctx["messages"]; !ok {
		// Read flash messages from the cookie, display them, then clear the cookie.
		if flashes := readFlashes(r); len(flashes) > 0 {
			msgs := make([]map[string]any, len(flashes))
			for i, m := range flashes {
				msgs[i] = map[string]any{"message": m}
			}
			ctx["messages"] = msgs
			clearFlashes(w)
		} else {
			ctx["messages"] = []any{}
		}
	}
	ctx["darkmode"] = cookieValue(r, "darkmode") == "true"
	ctx["csrf_token"] = ""

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "base.html", ctx); err != nil {
		// The response header may already be written; log and return best-effort.
		fmt.Printf("web: render %s: %v\n", name, err)
	}
}

// graphLinks returns the configured graph explorer base URL (GUI-GraphLinks setting),
// falling back to mempool.space/lightning if the setting is missing.
func (s *Server) graphLinks(ctx context.Context) string {
	ls, err := s.queries.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{
		Key: "GUI-GraphLinks", Value: "https://mempool.space/lightning",
	})
	if err != nil {
		return "https://mempool.space/lightning"
	}
	return ls.Value
}

func (s *Server) networkLinks(ctx context.Context) string {
	ls, err := s.queries.GetOrCreateLocalSetting(ctx, db.GetOrCreateLocalSettingParams{
		Key: "GUI-NetLinks", Value: "https://mempool.space",
	})
	if err != nil {
		return "https://mempool.space"
	}
	return ls.Value
}

// networkPrefix returns "testnet/" when running on testnet, otherwise "".
func (s *Server) networkPrefix() string {
	if s.cfg.LND_NETWORK == "testnet" {
		return "testnet/"
	}
	return ""
}

func cookieValue(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

// templateFuncs returns the template function map used by all page templates.
func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"intcomma":       intcomma,
		"default":        djangoDefault,
		"add":            djangoAdd,
		"slice":          djangoSlice,
		"floatformat":    floatformat,
		"dict_get":       dictGet,
		"naturaltime":    naturaltime,
		"notNone":        notNone,
		"djangodatetime": djangoDateTimeDefault,
		"date":           djangoDateFilter,
		"dict":           dictFunc,
		"str":            str,
		"pybool":         pybool,
		"pyEq":           pyEq,
		"numEq":          numEq,
		"reversed":       reversed,
	}
}

// notNone returns true for any non-nil value, including empty strings.
func notNone(v any) bool {
	return v != nil
}

// djangoDateFilter applies a date format to a value. Without a format argument
// the default format "N j, Y" is used. With a format argument the call
// convention is djangoDateFilter("fmt", value).
func djangoDateFilter(args ...any) string {
	if len(args) == 0 {
		return ""
	}
	format := "N j, Y"
	var v any
	if len(args) == 1 {
		v = args[0]
	} else {
		// Call form: {{ value | date:"fmt" }} -> djangoDateFilter("fmt", value)
		if f, ok := args[0].(string); ok {
			format = f
		}
		v = args[1]
	}
	t, ok := toTime(v)
	if !ok {
		return ""
	}
	return djangoDate(t.UTC(), format)
}

// dictFunc builds a map[string]any from key/value pairs, used as a substitute
// for passing named variables to included sub-templates.
func dictFunc(pairs ...any) map[string]any {
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		key, _ := pairs[i].(string)
		m[key] = pairs[i+1]
	}
	return m
}

// intcomma formats the integer part of a number with thousands separators.
func intcomma(v any) string {
	s := numberString(v)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	intPart, frac := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, frac = s[:i], s[i:]
	}
	intPart = groupThousands(intPart)
	out := intPart + frac
	if neg {
		out = "-" + out
	}
	return out
}

func groupThousands(s string) string {
	n := len(s)
	if n <= 3 {
		return s
	}
	var b strings.Builder
	pre := n % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if n > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < n; i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < n {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func numberString(v any) string {
	switch n := v.(type) {
	case int:
		return strconv.Itoa(n)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int64:
		return strconv.FormatInt(n, 10)
	case uint32:
		return strconv.FormatUint(uint64(n), 10)
	case uint64:
		return strconv.FormatUint(n, 10)
	case float64:
		return pyFloatString(n)
	case float32:
		return pyFloatString(float64(n))
	case string:
		return n
	default:
		return fmt.Sprintf("%v", v)
	}
}

// pyFloatString formats a float like Python's str(): shortest round-trip
// digits, ".0" on whole numbers, exponent form below 1e-4 and from 1e16,
// inf/nan in lower case.
func pyFloatString(f float64) string {
	switch {
	case math.IsNaN(f):
		return "nan"
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	if e := strconv.FormatFloat(f, 'e', -1, 64); f != 0 {
		if exp, _ := strconv.Atoi(e[strings.IndexByte(e, 'e')+1:]); exp < -4 || exp >= 16 {
			return e
		}
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// pybool renders a bool as "True" or "False" (capital first letter).
func pybool(v any) string {
	switch b := v.(type) {
	case bool:
		if b {
			return "True"
		}
		return "False"
	default:
		return numberString(v)
	}
}

// reversed returns a reversed copy of a slice or array.
func reversed(v any) []any {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[rv.Len()-1-i] = rv.Index(i).Interface()
	}
	return out
}

// pyEq compares two values using type-aware equality: both strings compare as
// strings, both numerics compare as float64, and string vs numeric is always
// unequal. This matters when settings values from the DB are strings while
// their defaults are int/float — select options should only be marked "selected"
// when the string representations match exactly.
func pyEq(a, b any) bool {
	as, aStr := a.(string)
	bs, bStr := b.(string)
	if aStr && bStr {
		return as == bs
	}
	if aStr != bStr {
		return false
	}
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		return af == bf
	}
	return a == b
}

// numEq compares two values numerically. Non-numeric values (nil, missing) are
// always unequal. Used for checkbox conditions such as min == 0 && max == 1.
func numEq(a, b any) bool {
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	return aok && bok && af == bf
}

// str converts a value to its string representation; nil is rendered as "".
func str(v any) string {
	if v == nil {
		return ""
	}
	return numberString(v)
}

// djangoDefault returns value when truthy, otherwise returns def.
func djangoDefault(def, value any) any {
	if isTruthy(value) {
		return value
	}
	return def
}

func isTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case int:
		return x != 0
	case int32:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	default:
		return true
	}
}

// djangoAdd performs numeric addition when both operands are numeric,
// otherwise falls back to string concatenation.
func djangoAdd(arg, value any) any {
	vi, vok := toInt64(value)
	ai, aok := toInt64(arg)
	if vok && aok {
		return vi + ai
	}
	return fmt.Sprintf("%v%v", value, arg)
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// djangoSlice applies a Python-style slice spec to a string (e.g. ":7", "12:", "1:5").
func djangoSlice(spec string, v any) string {
	s, ok := v.(string)
	if !ok {
		s = numberString(v)
	}
	n := len(s)
	start, stop := 0, n
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) == 2 {
		if parts[0] != "" {
			if i, err := strconv.Atoi(parts[0]); err == nil {
				start = clampIndex(i, n)
			}
		}
		if parts[1] != "" {
			if i, err := strconv.Atoi(parts[1]); err == nil {
				stop = clampIndex(i, n)
			}
		}
	} else if parts[0] != "" {
		if i, err := strconv.Atoi(parts[0]); err == nil {
			stop = clampIndex(i, n)
		}
	}
	if start > stop {
		start = stop
	}
	return s[start:stop]
}

func clampIndex(i, n int) int {
	if i < 0 {
		i += n
	}
	if i < 0 {
		return 0
	}
	if i > n {
		return n
	}
	return i
}

// floatformat is Django's floatformat for arg >= 0: the value's shortest
// decimal form (Python's str()) rounded half up to arg places, no "-0".
func floatformat(arg int, v any) string {
	f, ok := toFloat64(v)
	if !ok || math.IsInf(f, 0) || math.IsNaN(f) {
		return numberString(v)
	}
	d, _ := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
	scaled := d.Mul(d, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(arg)), nil)))
	neg := scaled.Sign() < 0
	scaled.Abs(scaled).Add(scaled, big.NewRat(1, 2))
	n := new(big.Int).Quo(scaled.Num(), scaled.Denom()).String()
	if len(n) <= arg {
		n = strings.Repeat("0", arg-len(n)+1) + n
	}
	if arg > 0 {
		n = n[:len(n)-arg] + "." + n[len(n)-arg:]
	}
	if neg && strings.Trim(n, "0.") != "" {
		n = "-" + n
	}
	return n
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// dictGet retrieves a key from a map[string]any, returning nil if absent.
func dictGet(d any, key string) any {
	if m, ok := d.(map[string]any); ok {
		return m[key]
	}
	return nil
}

// naturaltime is Django humanize's naturaltime: under a day "now",
// "a minute ago", "3 hours ago" and so on, beyond that timesince's two
// adjacent units ("1 day, 5 hours ago"); "from now" for future times.
// Count and unit are joined by a non-breaking space, as in Django.
func naturaltime(v any) string {
	t, ok := v.(time.Time)
	if !ok {
		return numberString(v)
	}
	now := time.Now().UTC()
	t = t.UTC()
	earlier, later, suffix := t, now, " ago"
	if t.After(now) {
		earlier, later, suffix = now, t, " from now"
	}
	secs := int64(later.Sub(earlier) / time.Second)
	switch {
	case secs >= 86400:
		return timesince(earlier, later) + suffix
	case secs == 0:
		return "now"
	case secs < 60:
		return countUnit(secs, "a second", "seconds") + suffix
	case secs < 3600:
		return countUnit(secs/60, "a minute", "minutes") + suffix
	default:
		return countUnit(secs/3600, "an hour", "hours") + suffix
	}
}

func countUnit(n int64, one, many string) string {
	if n == 1 {
		return one
	}
	return strconv.FormatInt(n, 10) + "\u00a0" + many
}

// timesince is django.utils.timesince with depth 2 for d before now: years
// and months counted on the calendar (via a pivot date), then weeks, days,
// hours, minutes; up to two adjacent non-zero units.
func timesince(d, now time.Time) string {
	unit := func(n int, name string) string {
		if n != 1 {
			name += "s"
		}
		return strconv.Itoa(n) + "\u00a0" + name
	}
	if now.Sub(d) < time.Second {
		return unit(0, "minute")
	}
	totalMonths := (now.Year()-d.Year())*12 + int(now.Month()) - int(d.Month())
	clock := func(t time.Time) time.Duration { return t.Sub(t.Truncate(24 * time.Hour)) }
	if d.Day() > now.Day() || (d.Day() == now.Day() && clock(d) > clock(now)) {
		totalMonths--
	}
	years, months := totalMonths/12, totalMonths%12
	pivot := d
	if years != 0 || months != 0 {
		py, pm := d.Year()+years, int(d.Month())+months
		if pm > 12 {
			pm -= 12
			py++
		}
		monthDays := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
		pivot = time.Date(py, time.Month(pm), min(monthDays[pm-1], d.Day()), d.Hour(), d.Minute(), d.Second(), 0, time.UTC)
	}
	remaining := now.Sub(pivot).Seconds()
	partials := []int{years, months}
	for _, chunk := range []float64{604800, 86400, 3600, 60} {
		n := math.Floor(remaining / chunk)
		partials = append(partials, int(n))
		remaining -= chunk * n
	}
	names := []string{"year", "month", "week", "day", "hour", "minute"}
	var parts []string
	for i, n := range partials {
		if n == 0 {
			if len(parts) > 0 {
				break
			}
			continue
		}
		parts = append(parts, unit(n, names[i]))
		if len(parts) == 2 {
			break
		}
	}
	if len(parts) == 0 {
		return unit(0, "minute")
	}
	return strings.Join(parts, ", ")
}
