package rebalancer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/warioishere/lndg-blitz-go/internal/db/generated"
)

func TestCoordinatorTransitions(t *testing.T) {
	c := newCoordinator()
	r1 := &db.GuiRebalancer{ID: 1}
	r2 := &db.GuiRebalancer{ID: 2}

	assert.True(t, c.tryEnqueue(r1))
	assert.False(t, c.tryEnqueue(r1)) // already scheduled
	assert.True(t, c.tryEnqueue(r2))
	assert.False(t, c.idleAndEmpty())

	got, ok := c.take()
	require.True(t, ok)
	assert.Equal(t, int64(1), got.ID) // FIFO
	// id 1 is now active, no longer scheduled -> cannot re-enqueue
	assert.False(t, c.tryEnqueue(r1))
	assert.False(t, c.idleAndEmpty()) // queue still has r2

	got2, _ := c.take()
	assert.Equal(t, int64(2), got2.ID)
	assert.False(t, c.idleAndEmpty()) // both active

	c.done(1)
	c.done(2)
	assert.True(t, c.idleAndEmpty())

	assert.False(t, c.isShutdown())
	c.setShutdown()
	assert.True(t, c.isShutdown())
}

func TestCoordinatorReset(t *testing.T) {
	c := newCoordinator()
	c.tryEnqueue(&db.GuiRebalancer{ID: 1})
	c.setShutdown()
	c.reset()
	assert.True(t, c.idleAndEmpty())
	assert.False(t, c.isShutdown())
}

// TestRunSessionProcessesPending: one worker drains the pending jobs (each gets
// status 406 due to no channels), then the manager shuts down for lack of work.
func TestRunSessionProcessesPending(t *testing.T) {
	q := newFakeRebalQ()
	q.settings["AR-DispatchInterval"] = "0" // no dispatch sleeps
	q.pending = []db.GuiRebalancer{
		{ID: 1, Value: 100000, LastHopPubkey: "02aa", Duration: 1, FeeLimit: 1000},
		{ID: 2, Value: 100000, LastHopPubkey: "02bb", Duration: 1, FeeLimit: 1000},
	}
	deps := QueueDeps{
		Engine: newEngine(),
		Stub:   &fakeLN{},
		Router: &fakeRebalRouter{},
		Q:      q,
		Now:    time.Unix(1_700_000_000, 0).UTC,
		Sleep:  func(time.Duration) { time.Sleep(time.Millisecond) },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := newCoordinator()

	done := make(chan struct{})
	go func() { deps.runSession(ctx, c, 1); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session did not shut down")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	assert.Equal(t, int32(406), q.statusByID[1])
	assert.Equal(t, int32(406), q.statusByID[2])
}
