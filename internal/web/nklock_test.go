package web

import (
	"html/template"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNkLockDisablesInputsOfNkChannels(t *testing.T) {
	tpl := template.Must(template.New("t").Funcs(templateFuncs()).Parse(`<input name="target" value="{{.local_fee_rate}}" {{nkLock .}}>`))
	render := func(ch map[string]any) string {
		var b strings.Builder
		require.NoError(t, tpl.Execute(&b, ch))
		return b.String()
	}
	nk := render(map[string]any{"af_mode": "nk", "local_fee_rate": 500})
	assert.Contains(t, nk, `disabled title="Set by the NK fee mode (/nk/)"`)
	assert.NotContains(t, render(map[string]any{"af_mode": "lndg", "local_fee_rate": 500}), "disabled")
	assert.NotContains(t, render(map[string]any{"local_fee_rate": 500}), "disabled", "a row without af_mode stays editable")
}
