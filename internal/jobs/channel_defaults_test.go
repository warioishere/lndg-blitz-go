package jobs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func TestApplyChannelDefaults_NewChannel(t *testing.T) {
	q := &fakeFeeQ{settings: map[string]string{}}
	ch := &db.GuiChannel{Capacity: 1000000}
	require.NoError(t, applyChannelDefaults(context.Background(), q, ch, true))
	assert.False(t, ch.AutoFees) // AF-Enabled default '0'
	assert.Equal(t, int32(75), ch.ArOutTarget)
	assert.Equal(t, int32(90), ch.ArInTarget)
	assert.Equal(t, int64(30000), ch.ArAmtTarget) // int(3/100 * 1000000)
	assert.Equal(t, int32(65), ch.ArMaxCost)
}

func TestApplyChannelDefaults_ExistingKeepsValues(t *testing.T) {
	q := &fakeFeeQ{settings: map[string]string{}}
	ch := &db.GuiChannel{Capacity: 1000000, ArOutTarget: 50, ArInTarget: 40, ArAmtTarget: 5, ArMaxCost: 20, AutoFees: true}
	require.NoError(t, applyChannelDefaults(context.Background(), q, ch, false))
	assert.True(t, ch.AutoFees, "existing auto_fees untouched (not new)")
	assert.Equal(t, int32(50), ch.ArOutTarget)
	assert.Equal(t, int32(40), ch.ArInTarget)
}
