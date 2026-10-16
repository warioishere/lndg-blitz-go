package web

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The rebalances_table.html partial calls initRebalancesTable inline, so every
// page that includes it must load rebalances_table.js before the include.
func TestRebalancesTableScriptLoadsBeforeInclude(t *testing.T) {
	const include = `{{template "rebalances_table.html"`
	const script = `<script src="/static/rebalances_table.js"></script>`
	pages, err := fs.Glob(templateFS, "templates/*.html")
	require.NoError(t, err)
	checked := 0
	for _, p := range pages {
		b, err := templateFS.ReadFile(p)
		require.NoError(t, err)
		src := string(b)
		inc := strings.Index(src, include)
		if inc < 0 {
			continue
		}
		checked++
		js := strings.Index(src, script)
		assert.True(t, js >= 0 && js < inc, "%s loads rebalances_table.js after the table", p)
	}
	assert.Positive(t, checked)
}
