package web

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

// Write routes of the Django REST Framework ViewSets: PUT channels/<pk>,
// invoices/<pk>, settings/<pk>, rebalancer/<pk> and POST rebalancer. The UI
// uses channels (inline edits), rebalancer (repeat / cancel). Field coercion,
// error messages and status codes follow DRF: invalid input answers 200 with
// {"field": ["message"]}, an unknown pk answers 404.

type fieldKind int

const (
	kindInt fieldKind = iota
	kindFloat
	kindBool
	kindText
	kindDateTime
)

// apiField is one writable serializer field, in serializer field order.
type apiField struct {
	name     string // JSON name
	column   string // DB column
	kind     fieldKind
	min, max int64 // kindInt range (Django IntegerField / BigIntegerField)
	maxLen   int   // kindText max_length, 0 = none
	nullable bool
	blank    bool // kindText allow_blank
	required bool // required on create and non-partial update
}

const (
	int32Min, int32Max = math.MinInt32, math.MaxInt32
	int64Min, int64Max = math.MinInt64, math.MaxInt64
)

func intField(name string, min, max int64) apiField {
	return apiField{name: name, column: name, kind: kindInt, min: min, max: max}
}
func boolField(name string) apiField  { return apiField{name: name, column: name, kind: kindBool} }
func floatField(name string) apiField { return apiField{name: name, column: name, kind: kindFloat} }
func timeField(name string, nullable bool) apiField {
	return apiField{name: name, column: name, kind: kindDateTime, nullable: nullable}
}

// channelFields: the writable ChannelSerializer fields (everything not ReadOnly).
var channelFields = []apiField{
	intField("ar_max_cost", int32Min, int32Max),
	intField("ar_amt_target", int64Min, int64Max),
	intField("ar_out_target", int32Min, int32Max),
	intField("ar_in_target", int32Min, int32Max),
	boolField("auto_fees"),
	intField("local_inbound_base_fee", int32Min, int32Max),
	intField("local_inbound_fee_rate", int32Min, int32Max),
	intField("inbound_offset", int32Min, int32Max),
	timeField("offset_updated", true),
	intField("maxhtlc_percent", int32Min, int32Max),
	timeField("maxhtlc_updated", true),
	intField("mx_liq_threshold", int64Min, int64Max),
	intField("mx_liq_value", int64Min, int64Max),
	intField("mx_liq_upper", int64Min, int64Max),
	intField("remote_inbound_base_fee", int32Min, int32Max),
	intField("remote_inbound_fee_rate", int32Min, int32Max),
	boolField("auto_rebalance"),
	boolField("ar_source"),
	intField("ar_source_ppm_diff", int32Min, int32Max),
	boolField("ep_enabled"),
	intField("ep_target", int32Min, int32Max),
	floatField("ep_inc_pct"),
	intField("ep_cooldown", int32Min, int32Max),
	intField("ep_live_threshold", int32Min, int32Max),
	floatField("ep_live_inc_pct"),
	boolField("flp_enabled"),
	intField("flp_safety", int32Min, int32Max),
	timeField("ep_updated", false),
	timeField("htlc_boost_checked", true),
	{name: "notes", column: "notes", kind: kindText, blank: true},
}

// rebalancerFields: the writable RebalancerSerializer fields.
var rebalancerFields = []apiField{
	{name: "value", column: "value", kind: kindInt, min: int32Min, max: int32Max, required: true},
	{name: "fee_limit", column: "fee_limit", kind: kindFloat, required: true},
	{name: "outgoing_chan_ids", column: "outgoing_chan_ids", kind: kindText},
	{name: "last_hop_pubkey", column: "last_hop_pubkey", kind: kindText, maxLen: 66},
	{name: "target_alias", column: "target_alias", kind: kindText, maxLen: 32},
	{name: "duration", column: "duration", kind: kindInt, min: int32Min, max: int32Max, required: true},
	intField("status", int32Min, int32Max),
	boolField("manual"),
}

// invoiceFields: InvoiceSerializer declares id (source=index) as a writable,
// required IntegerField; is_revenue is the only other writable field.
var invoiceFields = []apiField{
	{name: "id", column: "index", kind: kindInt, min: int64Min, max: int64Max, required: true},
	boolField("is_revenue"),
}

var settingFields = []apiField{{name: "value", column: "value", kind: kindText}}

// mountViewSetWrites registers the write routes on the /api router.
func (s *Server) mountViewSetWrites(api chi.Router) {
	put := func(prefix string, h http.HandlerFunc) {
		api.Put("/"+prefix+"/{pk}/", h)
		api.Put("/"+prefix+"/{pk}", h)
	}
	put("channels", s.handleChannelUpdate)
	put("invoices", s.handleInvoiceUpdate)
	put("settings", s.handleSettingUpdate)
	put("rebalancer", s.handleRebalancerUpdate)
	api.Post("/rebalancer/", s.handleRebalancerCreate)
	api.Post("/rebalancer", s.handleRebalancerCreate)
	// RebalanceRouteViewSet.destroy and its cleanup_untested action
	api.Delete("/rebalanceroutes/{pk}/", s.handleRebalanceRouteDelete)
	api.Delete("/rebalanceroutes/{pk}", s.handleRebalanceRouteDelete)
	api.Post("/rebalanceroutes/cleanup_untested/", s.handleRebalanceRouteCleanup)
}

func (s *Server) handleRebalanceRouteDelete(w http.ResponseWriter, r *http.Request) {
	tag, err := s.db.Exec(r.Context(), `DELETE FROM gui_rebalanceroute WHERE id::text = $1`, chi.URLParam(r, "pk"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		writeDRFError(w, http.StatusNotFound, "No RebalanceRoute matches the given query.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"message": "Deleted"})
}

// handleRebalanceRouteCleanup deletes the routes that were never tried.
func (s *Server) handleRebalanceRouteCleanup(w http.ResponseWriter, r *http.Request) {
	tag, err := s.db.Exec(r.Context(), `DELETE FROM gui_rebalanceroute WHERE success_count = 0 AND failure_count = 0`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"deleted": tag.RowsAffected()})
}

// --- handlers ---------------------------------------------------------------

func (s *Server) handleChannelUpdate(w http.ResponseWriter, r *http.Request) {
	s.viewSetUpdate(w, r, "channels", "gui_channels", "chan_id", "Channels", channelFields, true, nil,
		func(ctx context.Context, pk string) (*orderedMap, error) {
			return loadOne(ctx, s.db, "gui_channels", "chan_id", pk, channelResult)
		})
}

func (s *Server) handleInvoiceUpdate(w http.ResponseWriter, r *http.Request) {
	s.viewSetUpdate(w, r, "invoices", "gui_invoices", "r_hash", "Invoices", invoiceFields, false, nil,
		func(ctx context.Context, pk string) (*orderedMap, error) {
			return loadOne(ctx, s.db, "gui_invoices", "r_hash", pk, invoiceResult)
		})
}

func (s *Server) handleSettingUpdate(w http.ResponseWriter, r *http.Request) {
	s.viewSetUpdate(w, r, "settings", "gui_localsettings", "key", "LocalSettings", settingFields, false, nil,
		func(ctx context.Context, pk string) (*orderedMap, error) {
			return loadOne(ctx, s.db, "gui_localsettings", "key", pk, plainResult[db.GuiLocalsetting])
		})
}

// handleRebalancerUpdate stamps stop = now on every valid update, as the
// Python ViewSet does (the UI cancels a pending request with status 499).
func (s *Server) handleRebalancerUpdate(w http.ResponseWriter, r *http.Request) {
	stop := map[string]any{"stop": pgtype.Timestamptz{Time: time.Now(), Valid: true}}
	s.viewSetUpdate(w, r, "rebalancer", "gui_rebalancer", "id", "Rebalancer", rebalancerFields, true, stop,
		func(ctx context.Context, pk string) (*orderedMap, error) {
			return loadOne(ctx, s.db, "gui_rebalancer", "id", pk, plainResult[db.GuiRebalancer])
		})
}

func (s *Server) handleRebalancerCreate(w http.ResponseWriter, r *http.Request) {
	obj, ok := readBody(w, r)
	if !ok {
		return
	}
	values, errs := validateFields(obj, rebalancerFields, false)
	if errs != nil {
		writeJSON(w, http.StatusOK, errs)
		return
	}
	// Model defaults for omitted fields; start/stop/payment_hash/fees_paid stay NULL.
	row := map[string]any{
		"requested":         pgtype.Timestamptz{Time: time.Now(), Valid: true},
		"outgoing_chan_ids": "[]",
		"last_hop_pubkey":   "",
		"target_alias":      "",
		"status":            int64(0),
		"manual":            false,
	}
	for k, v := range values {
		row[k] = v
	}
	cols, args := sortedColumns(row)
	ctx := r.Context()
	var id int64
	err := s.db.QueryRow(ctx, fmt.Sprintf(`INSERT INTO gui_rebalancer (%s) VALUES (%s) RETURNING id`,
		quoteColumns(cols), placeholders(1, len(cols))), args...).Scan(&id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := loadOne(ctx, s.db, "gui_rebalancer", "id", strconv.FormatInt(id, 10), plainResult[db.GuiRebalancer])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, shapeDRF(r, "rebalancer", res))
}

// viewSetUpdate validates the body against fields and updates the row with
// primary key pk. extra columns are written along with valid input only.
func (s *Server) viewSetUpdate(w http.ResponseWriter, r *http.Request, prefix, table, pkCol, model string,
	fields []apiField, partial bool, extra map[string]any,
	load func(ctx context.Context, pk string) (*orderedMap, error)) {
	ctx := r.Context()
	pk := chi.URLParam(r, "pk")
	if res, err := load(ctx, pk); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if res == nil {
		writeDRFError(w, http.StatusNotFound, "No "+model+" matches the given query.")
		return
	}
	obj, ok := readBody(w, r)
	if !ok {
		return
	}
	values, errs := validateFields(obj, fields, partial)
	if errs != nil {
		writeJSON(w, http.StatusOK, errs)
		return
	}
	for k, v := range extra {
		values[k] = v
	}
	if len(values) > 0 {
		cols, args := sortedColumns(values)
		sets := make([]string, len(cols))
		for i, c := range cols {
			sets[i] = fmt.Sprintf(`"%s" = $%d`, c, i+2)
		}
		if _, err := s.db.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s WHERE "%s"::text = $1`, table, strings.Join(sets, ", "), pkCol),
			append([]any{pk}, args...)...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	res, err := load(ctx, pk)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, shapeDRF(r, prefix, res))
}

// loadOne reads the row with the given primary key, nil when it does not exist.
func loadOne[T any](ctx context.Context, dbtx db.DBTX, table, pkCol, pk string, toResult func(*T) *orderedMap) (*orderedMap, error) {
	rows, err := dbtx.Query(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE "%s"::text = $1`, columnsOf[T](), table, pkCol), pk)
	if err != nil {
		return nil, err
	}
	items, err := scanRows[T](rows)
	if err != nil || len(items) == 0 {
		return nil, err
	}
	return toResult(&items[0]), nil
}

// --- request body -------------------------------------------------------------

// readBody decodes the JSON request body (empty body = {}). A parse error
// answers 400 like DRF's JSONParser; a non-object body is a validation error.
func readBody(w http.ResponseWriter, r *http.Request) (map[string]json.RawMessage, bool) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeDRFError(w, http.StatusBadRequest, "JSON parse error - "+err.Error())
		return nil, false
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]json.RawMessage{}, true
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		writeDRFError(w, http.StatusBadRequest, "JSON parse error - "+err.Error())
		return nil, false
	}
	if _, isObj := v.(map[string]any); !isObj {
		writeJSON(w, http.StatusOK, map[string][]string{"non_field_errors": {
			"Invalid data. Expected a dictionary, but got " + pyTypeName(v) + "."}})
		return nil, false
	}
	var obj map[string]json.RawMessage
	_ = json.Unmarshal(raw, &obj) // already known to be a valid JSON object
	return obj, true
}

func pyTypeName(v any) string {
	switch v.(type) {
	case []any:
		return "list"
	case string:
		return "str"
	case bool:
		return "bool"
	case float64:
		return "float"
	default:
		return "NoneType"
	}
}

// validateFields coerces the present fields. Unknown and read-only keys are
// ignored. Returns column -> value, or the ordered DRF error object.
func validateFields(obj map[string]json.RawMessage, fields []apiField, partial bool) (map[string]any, *orderedMap) {
	values := map[string]any{}
	errs := newOrderedMap()
	for _, f := range fields {
		raw, present := obj[f.name]
		if !present {
			if f.required && !partial {
				errs.Set(f.name, []string{"This field is required."})
			}
			continue
		}
		v, msg := coerceField(f, raw)
		if msg != "" {
			errs.Set(f.name, []string{msg})
			continue
		}
		values[f.column] = v
	}
	if len(errs.keys) > 0 {
		return nil, errs
	}
	return values, nil
}

// coerceField converts one JSON value the way the matching DRF field does.
func coerceField(f apiField, raw json.RawMessage) (any, string) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	_ = dec.Decode(&v)
	if v == nil {
		if f.nullable {
			return nil, ""
		}
		return nil, "This field may not be null."
	}
	switch f.kind {
	case kindInt:
		return coerceInt(v, f.min, f.max)
	case kindFloat:
		return coerceFloat(v)
	case kindBool:
		return coerceBool(v)
	case kindText:
		return coerceText(v, f)
	default:
		return coerceDateTime(v)
	}
}

// reDecimal is DRF IntegerField.re_decimal: a trailing ".000" is accepted.
var reDecimal = regexp.MustCompile(`\.0*\s*$`)

func coerceInt(v any, min, max int64) (any, string) {
	const invalid = "A valid integer is required."
	var s string
	switch t := v.(type) {
	case json.Number:
		s = t.String()
		if f, err := strconv.ParseFloat(s, 64); err == nil && strings.ContainsAny(s, ".eE") {
			// Python formats JSON floats as repr(): integral values below 1e16 read "N.0"
			if f != math.Trunc(f) || math.Abs(f) >= 1e16 {
				return nil, invalid
			}
			s = strconv.FormatFloat(f, 'f', 0, 64)
		}
	case string:
		if len(t) > 1000 {
			return nil, "String value too large."
		}
		s = strings.TrimSpace(reDecimal.ReplaceAllString(t, ""))
	default:
		return nil, invalid
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		if ne, ok := err.(*strconv.NumError); ok && ne.Err == strconv.ErrRange {
			if strings.HasPrefix(s, "-") {
				return nil, fmt.Sprintf("Ensure this value is greater than or equal to %d.", min)
			}
			return nil, fmt.Sprintf("Ensure this value is less than or equal to %d.", max)
		}
		return nil, invalid
	}
	if n > max {
		return nil, fmt.Sprintf("Ensure this value is less than or equal to %d.", max)
	}
	if n < min {
		return nil, fmt.Sprintf("Ensure this value is greater than or equal to %d.", min)
	}
	return n, ""
}

func coerceFloat(v any) (any, string) {
	const invalid = "A valid number is required."
	var f float64
	var err error
	switch t := v.(type) {
	case json.Number:
		f, err = t.Float64()
	case string:
		if len(t) > 1000 {
			return nil, "String value too large."
		}
		f, err = strconv.ParseFloat(strings.TrimSpace(t), 64)
	case bool: // Python: float(True) == 1.0
		if t {
			return 1.0, ""
		}
		return 0.0, ""
	default:
		return nil, invalid
	}
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, invalid
	}
	return f, ""
}

// DRF BooleanField TRUE_VALUES / FALSE_VALUES (string and numeric forms).
var (
	boolTrue  = map[string]bool{"t": true, "T": true, "y": true, "Y": true, "yes": true, "Yes": true, "YES": true, "true": true, "True": true, "TRUE": true, "on": true, "On": true, "ON": true, "1": true}
	boolFalse = map[string]bool{"f": true, "F": true, "n": true, "N": true, "no": true, "No": true, "NO": true, "false": true, "False": true, "FALSE": true, "off": true, "Off": true, "OFF": true, "0": true}
)

func coerceBool(v any) (any, string) {
	switch t := v.(type) {
	case bool:
		return t, ""
	case string:
		if boolTrue[t] {
			return true, ""
		}
		if boolFalse[t] {
			return false, ""
		}
	case json.Number:
		if f, err := t.Float64(); err == nil {
			if f == 1 {
				return true, ""
			}
			if f == 0 {
				return false, ""
			}
		}
	}
	return nil, "Must be a valid boolean."
}

func coerceText(v any, f apiField) (any, string) {
	var s string
	switch t := v.(type) {
	case string:
		s = t
	case json.Number:
		s = t.String()
		if strings.ContainsAny(s, ".eE") { // Python str() of a JSON float
			if fl, err := t.Float64(); err == nil {
				s = pyFloatString(fl)
			}
		}
	default: // bool, list, object
		return nil, "Not a valid string."
	}
	s = strings.TrimSpace(s) // trim_whitespace=True
	if s == "" {
		if !f.blank {
			return nil, "This field may not be blank."
		}
		return "", ""
	}
	if f.maxLen > 0 && utf8.RuneCountInString(s) > f.maxLen {
		return nil, fmt.Sprintf("Ensure this field has no more than %d characters.", f.maxLen)
	}
	if strings.ContainsRune(s, 0) {
		return nil, "Null characters are not allowed."
	}
	return s, ""
}

// DRF DateTimeField with ISO-8601 input; a naive value is taken as UTC, the
// same convention the API uses for output.
func coerceDateTime(v any) (any, string) {
	const invalid = "Datetime has wrong format. Use one of these formats instead: YYYY-MM-DDThh:mm[:ss[.uuuuuu]][+HH:MM|-HH:MM|Z]."
	s, ok := v.(string)
	if !ok {
		return nil, invalid
	}
	s = strings.TrimSpace(s)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04",
		"2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04"} {
		if t, err := time.Parse(layout, s); err == nil {
			return pgtype.Timestamptz{Time: t, Valid: true}, ""
		}
	}
	return nil, invalid
}

// --- SQL helpers --------------------------------------------------------------

// sortedColumns returns the map keys in a stable order with matching args.
func sortedColumns(m map[string]any) ([]string, []any) {
	cols := make([]string, 0, len(m))
	for k := range m {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	args := make([]any, len(cols))
	for i, c := range cols {
		args[i] = m[c]
	}
	return cols, args
}

func quoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = `"` + c + `"`
	}
	return strings.Join(q, ", ")
}

func placeholders(from, n int) string {
	p := make([]string, n)
	for i := range p {
		p[i] = "$" + strconv.Itoa(from+i)
	}
	return strings.Join(p, ", ")
}
