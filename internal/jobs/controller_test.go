package jobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataLoop_ResetsOnErrorThenContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls, resetCh, resetPool := 0, 0, 0
	deps := DataLoopDeps{
		RunOnce: func(context.Context) error {
			calls++
			if calls == 1 {
				return errors.New("boom")
			}
			cancel() // stop after the second (successful) run
			return nil
		},
		ResetChannel: func() { resetCh++ },
		ResetPool:    func() { resetPool++ },
		Sleep:        time.Millisecond,
	}
	err := DataLoop(ctx, deps)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, calls)
	assert.Equal(t, 1, resetCh) // reset only after the failing run
	assert.Equal(t, 1, resetPool)
}

func TestDataLoop_NoResetOnSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	resetCh, resetPool := 0, 0
	deps := DataLoopDeps{
		RunOnce: func(context.Context) error {
			cancel()
			return nil
		},
		ResetChannel: func() { resetCh++ },
		ResetPool:    func() { resetPool++ },
		Sleep:        time.Millisecond,
	}
	require.ErrorIs(t, DataLoop(ctx, deps), context.Canceled)
	assert.Equal(t, 0, resetCh)
	assert.Equal(t, 0, resetPool)
}

func TestDataLoop_StopsImmediatelyIfCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	deps := DataLoopDeps{
		RunOnce:      func(context.Context) error { calls++; return nil },
		ResetChannel: func() {},
		ResetPool:    func() {},
		Sleep:        time.Millisecond,
	}
	require.ErrorIs(t, DataLoop(ctx, deps), context.Canceled)
	assert.Equal(t, 0, calls)
}
