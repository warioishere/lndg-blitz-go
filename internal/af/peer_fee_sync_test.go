package af

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMirrorPeerFeeTargets_MirrorsLowestLiquidityController(t *testing.T) {
	// Peer "p1" has 3 channels; the one with min out_percent (20) is the controller.
	rows := []*ChannelFeeRow{
		{RemotePubkey: "p1", OutPercent: 50, NewRate: 100, NewInboundRate: -5, Eligible: false, LocalFeeRate: 80, LocalInboundFeeRate: 0},
		{RemotePubkey: "p1", OutPercent: 20, NewRate: 300, NewInboundRate: -10, Eligible: true, LocalFeeRate: 250, LocalInboundFeeRate: -2},
		{RemotePubkey: "p1", OutPercent: 70, NewRate: 150, NewInboundRate: -1, Eligible: false, LocalFeeRate: 90, LocalInboundFeeRate: 1},
	}
	out := MirrorPeerFeeTargets(rows)

	// Controller = rows[1] (out_percent 20). Followers adopt its targets.
	assert.Equal(t, float64(300), out[0].NewRate)
	assert.Equal(t, float64(300)-80, out[0].Adjustment)
	assert.Equal(t, float64(-10), out[0].NewInboundRate)
	assert.Equal(t, float64(-10)-0, out[0].InboundAdjustment)

	assert.Equal(t, float64(300), out[2].NewRate)
	assert.Equal(t, float64(300)-90, out[2].Adjustment)
	assert.Equal(t, float64(-10), out[2].NewInboundRate)
	assert.Equal(t, float64(-10)-1, out[2].InboundAdjustment)

	// Controller new_rate is unchanged.
	assert.Equal(t, float64(300), out[1].NewRate)

	// Whole group eligible = controller_eligible (true).
	assert.True(t, out[0].Eligible)
	assert.True(t, out[1].Eligible)
	assert.True(t, out[2].Eligible)
}

func TestMirrorPeerFeeTargets_SingleChannelPeerUntouched(t *testing.T) {
	rows := []*ChannelFeeRow{
		{RemotePubkey: "solo", OutPercent: 40, NewRate: 111, Eligible: false, LocalFeeRate: 100},
	}
	out := MirrorPeerFeeTargets(rows)
	assert.Equal(t, float64(111), out[0].NewRate)
	assert.Equal(t, float64(0), out[0].Adjustment, "single-channel peer not modified")
	assert.False(t, out[0].Eligible)
}

func TestMirrorPeerFeeTargets_IdxminTieBreakFirst(t *testing.T) {
	// Two channels with equal min out_percent (30) -> first is controller.
	rows := []*ChannelFeeRow{
		{RemotePubkey: "p", OutPercent: 30, NewRate: 200, LocalFeeRate: 150},
		{RemotePubkey: "p", OutPercent: 30, NewRate: 999, LocalFeeRate: 150},
	}
	out := MirrorPeerFeeTargets(rows)
	// Controller = rows[0] (first minimum). rows[1] adopts 200.
	assert.Equal(t, float64(200), out[1].NewRate)
	assert.Equal(t, float64(200), out[0].NewRate)
}

func TestMirrorPeerFeeTargets_MultiplePeersIndependent(t *testing.T) {
	rows := []*ChannelFeeRow{
		{RemotePubkey: "a", OutPercent: 10, NewRate: 100, LocalFeeRate: 50},
		{RemotePubkey: "b", OutPercent: 80, NewRate: 500, LocalFeeRate: 400},
		{RemotePubkey: "a", OutPercent: 60, NewRate: 999, LocalFeeRate: 50},
		{RemotePubkey: "b", OutPercent: 20, NewRate: 700, LocalFeeRate: 400},
	}
	out := MirrorPeerFeeTargets(rows)
	// Peer a: controller out=10 (rows[0], rate 100) -> rows[2] = 100.
	assert.Equal(t, float64(100), out[2].NewRate)
	// Peer b: controller out=20 (rows[3], rate 700) -> rows[1] = 700.
	assert.Equal(t, float64(700), out[1].NewRate)
}

func TestMirrorPeerFeeTargets_Empty(t *testing.T) {
	assert.Empty(t, MirrorPeerFeeTargets(nil))
	assert.Empty(t, MirrorPeerFeeTargets([]*ChannelFeeRow{}))
}
