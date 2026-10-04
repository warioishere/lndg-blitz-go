package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

var timestamptzType = reflect.TypeOf(pgtype.Timestamptz{})

// structToOrderedMap builds an orderedMap from a sqlc struct, preserving field
// declaration order using json tags as keys. Timestamptz fields are serialized
// as naive UTC ISO strings; all other pgtype values marshal themselves (null
// when !Valid). ViewSets may append additional fields (id, computed) afterward.
func structToOrderedMap(v any) *orderedMap {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	rt := rv.Type()
	m := newOrderedMap()
	for i := 0; i < rt.NumField(); i++ {
		key := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		if key == "" || key == "-" {
			continue
		}
		fv := rv.Field(i)
		if fv.Type() == timestamptzType {
			m.Set(key, drfDateTime(fv.Interface().(pgtype.Timestamptz)))
			continue
		}
		m.Set(key, fv.Interface())
	}
	return m
}

// orderedMap is a JSON object with a defined key order. Go's built-in map
// serializes keys alphabetically, but the API requires fields in a specific
// order. ViewSets build results as an orderedMap, appending fields in the
// required sequence.
type orderedMap struct {
	keys   []string
	values map[string]any
}

func newOrderedMap() *orderedMap {
	return &orderedMap{values: map[string]any{}}
}

// Set appends a key (or updates its value if it already exists, preserving order).
func (m *orderedMap) Set(key string, val any) *orderedMap {
	if _, ok := m.values[key]; !ok {
		m.keys = append(m.keys, key)
	}
	m.values[key] = val
	return m
}

func (m *orderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range m.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		keyJSON, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(keyJSON)
		buf.WriteByte(':')
		valJSON, err := marshalValue(m.values[k])
		if err != nil {
			return nil, err
		}
		buf.Write(valJSON)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// marshalValue writes floats as Python's json.dumps does ("84912.0",
// "1e-05"); everything else as encoding/json. NaN/Inf keep failing, as in DRF.
func marshalValue(v any) ([]byte, error) {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case float32:
		f = float64(n)
	case pgtype.Float8:
		if !n.Valid {
			return []byte("null"), nil
		}
		f = n.Float64
	case pgtype.Float4:
		if !n.Valid {
			return []byte("null"), nil
		}
		f = float64(n.Float32)
	default:
		return json.Marshal(v)
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return json.Marshal(f)
	}
	return []byte(pyFloatString(f)), nil
}

// drfDateTime serializes a timestamptz column as a naive UTC ISO-8601 string,
// with microseconds included only when non-zero. Returns JSON null for NULL values.
func drfDateTime(t pgtype.Timestamptz) any {
	if !t.Valid {
		return nil
	}
	return isoformatUTC(t.Time)
}

func isoformatUTC(t time.Time) string {
	t = t.UTC()
	if t.Nanosecond() == 0 {
		return t.Format("2006-01-02T15:04:05")
	}
	return t.Format("2006-01-02T15:04:05") + fmt.Sprintf(".%06d", t.Nanosecond()/1000)
}

// writeJSON serializes v as JSON with the given status code. HTML escaping of
// </> and & is disabled so those characters are written literally.
func writeJSON(w http.ResponseWriter, status int, v any) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// json.Encoder appends a trailing '\n' — strip it for a clean response body.
	out := bytes.TrimRight(buf.Bytes(), "\n")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(out)
}

// writeDRFError writes an error response as {"detail": "..."} with the given
// status code (e.g. 400 or 404).
func writeDRFError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
