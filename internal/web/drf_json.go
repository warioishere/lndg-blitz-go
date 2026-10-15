package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
)

// Request body fields of the action endpoints. Like DRF's IntegerField,
// FloatField and BooleanField they take a JSON value or its string form: the
// browser sends input values as strings ({"fee_rate": "500"}).

type drfInt int64
type drfFloat float64
type drfBool bool

func decodeAny(b []byte) (any, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return v, dec.Decode(&v)
}

func (d *drfInt) UnmarshalJSON(b []byte) error {
	v, err := decodeAny(b)
	if err != nil {
		return err
	}
	n, msg := coerceInt(v, math.MinInt64, math.MaxInt64)
	if msg != "" {
		return errors.New(msg)
	}
	*d = drfInt(n.(int64))
	return nil
}

func (d *drfFloat) UnmarshalJSON(b []byte) error {
	v, err := decodeAny(b)
	if err != nil {
		return err
	}
	f, msg := coerceFloat(v)
	if msg != "" {
		return errors.New(msg)
	}
	*d = drfFloat(f.(float64))
	return nil
}

func (d *drfBool) UnmarshalJSON(b []byte) error {
	v, err := decodeAny(b)
	if err != nil {
		return err
	}
	t, msg := coerceBool(v)
	if msg != "" {
		return errors.New(msg)
	}
	*d = drfBool(t.(bool))
	return nil
}
