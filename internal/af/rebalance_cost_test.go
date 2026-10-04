package af

import (
	"testing"

	"github.com/stretchr/testify/assert"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func TestAvgRebalanceCostPPM(t *testing.T) {
	assert.Nil(t, AvgRebalanceCostPPM(nil), "no payments -> nil")
	assert.Nil(t, AvgRebalanceCostPPM([]db.RebalPaymentsForChannelRow{{Fee: 1, Value: 0, SourceFeeRate: 400}}), "value 0 skipped")

	// routing fee 300 ppm + source 400 ppm = 700 ppm
	v := AvgRebalanceCostPPM([]db.RebalPaymentsForChannelRow{{Fee: 300, Value: 1000000, SourceFeeRate: 400}})
	if assert.NotNil(t, v) {
		assert.Equal(t, 700, *v)
	}

	// (700 + 150) / 2 = 425; source 0 (MPP / unknown fallback) adds nothing
	v = AvgRebalanceCostPPM([]db.RebalPaymentsForChannelRow{
		{Fee: 300, Value: 1000000, SourceFeeRate: 400},
		{Fee: 150, Value: 1000000, SourceFeeRate: 0},
	})
	if assert.NotNil(t, v) {
		assert.Equal(t, 425, *v)
	}
}
