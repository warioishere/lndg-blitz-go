package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSourceBrake(t *testing.T) {
	b := newSourceBrake()
	base := time.Unix(1_700_000_000, 0)
	now := base
	b.now = func() time.Time { return now }

	assert.False(t, b.blocked("s", "t1"))
	b.miss("s", "t1", false)
	assert.True(t, b.blocked("s", "t1"))
	assert.False(t, b.blocked("s", "t2"), "a miss for one target leaves the others")

	// the second miss in a row pauses twice as long
	now = base.Add(sourceBrakeBase + time.Second)
	assert.False(t, b.blocked("s", "t1"))
	b.miss("s", "t1", false)
	now = now.Add(2*sourceBrakeBase - time.Second)
	assert.True(t, b.blocked("s", "t1"))

	// a payment lifts it
	b.paid("s", "t1")
	assert.False(t, b.blocked("s", "t1"))

	// an onward failure pauses the source for every target
	b.miss("s", "t1", true)
	assert.True(t, b.blocked("s", "t2"))
	b.paid("s", "t2")
	assert.False(t, b.blocked("s", "t1"))
}

func TestSourceBrakeCountsAfterLongQuiet(t *testing.T) {
	b := newSourceBrake()
	now := time.Unix(1_700_000_000, 0)
	b.now = func() time.Time { return now }
	for range 10 {
		b.miss("s", "t", false)
	}
	assert.Equal(t, sourceBrakeMax, brakeFor(10))
	now = now.Add(sourceBrakeMax + sourceBrakeMax + time.Second)
	b.miss("s", "t", false)
	assert.Equal(t, 1, b.m[brakeKey{"s", "t"}].misses, "misses start over after a long quiet")
}

func TestOnwardFailure(t *testing.T) {
	assert.True(t, onwardFailure(15, 1, 4))  // source peer -> X, X is not the target
	assert.False(t, onwardFailure(15, 1, 3)) // source peer -> target
	assert.False(t, onwardFailure(15, 2, 4))
	assert.False(t, onwardFailure(12, 1, 4))
}
