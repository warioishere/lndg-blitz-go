package web

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The browser sends input values as strings; DRF accepts them, so must we.
func TestDRFJSONFields(t *testing.T) {
	var body struct {
		I *drfInt   `json:"i"`
		F *drfFloat `json:"f"`
		B *drfBool  `json:"b"`
	}
	for _, c := range []struct {
		in   string
		i    int64
		f    float64
		b    bool
		fail bool
	}{
		{`{"i": 500, "f": 1.5, "b": true}`, 500, 1.5, true, false},
		{`{"i": "500", "f": "1.5", "b": "true"}`, 500, 1.5, true, false},
		{`{"i": " 800 ", "f": "2", "b": "0"}`, 800, 2, false, false},
		{`{"i": "800.0", "f": 3, "b": 1}`, 800, 3, true, false},
		{`{"i": "5.5"}`, 0, 0, false, true},
		{`{"i": "abc"}`, 0, 0, false, true},
		{`{"f": "x"}`, 0, 0, false, true},
		{`{"b": "maybe"}`, 0, 0, false, true},
	} {
		body.I, body.F, body.B = nil, nil, nil
		err := json.Unmarshal([]byte(c.in), &body)
		if c.fail {
			assert.Error(t, err, c.in)
			continue
		}
		require.NoError(t, err, c.in)
		assert.EqualValues(t, c.i, *body.I, c.in)
		assert.EqualValues(t, c.f, *body.F, c.in)
		assert.EqualValues(t, c.b, *body.B, c.in)
	}
	body.I = nil
	require.NoError(t, json.Unmarshal([]byte(`{"i": null}`), &body))
	assert.Nil(t, body.I, "null stays absent")
}
