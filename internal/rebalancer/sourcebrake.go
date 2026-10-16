package rebalancer

import (
	"sync"
	"time"
)

// Per-source brake: a source channel whose per-source attempt for a target
// did not pay is skipped for that target for sourceBrakeBase, doubled per
// further miss in a row, up to sourceBrakeMax. When the source peer itself
// cannot forward onward, the brake holds for every target. A payment through
// the source lifts both.
const (
	sourceBrakeBase = 10 * time.Minute
	sourceBrakeMax  = 2 * time.Hour
)

// brakeKey is a source channel and a target pubkey; an empty target is the
// brake of the source for all targets.
type brakeKey struct {
	source string
	target string
}

type brakeEntry struct {
	until  time.Time
	misses int
}

// sourceBrake is shared by all workers; a mutex guards it.
type sourceBrake struct {
	mu  sync.Mutex
	m   map[brakeKey]brakeEntry
	now func() time.Time
}

func newSourceBrake() *sourceBrake {
	return &sourceBrake{m: map[brakeKey]brakeEntry{}, now: time.Now}
}

// brakeFor is the pause after the n-th miss in a row.
func brakeFor(n int) time.Duration {
	d := sourceBrakeBase
	for i := 1; i < n && d < sourceBrakeMax; i++ {
		d *= 2
	}
	return min(d, sourceBrakeMax)
}

// blocked reports whether the source is paused for the target.
func (b *sourceBrake) blocked(source, target string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	for _, k := range []brakeKey{{source, target}, {source, ""}} {
		if e, ok := b.m[k]; ok && now.Before(e.until) {
			return true
		}
	}
	return false
}

// miss records an attempt via the source that did not pay; onward marks a
// failure at the source peer's onward channel, which pauses the source for
// every target.
func (b *sourceBrake) miss(source, target string, onward bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	k := brakeKey{source, target}
	if onward {
		k.target = ""
	}
	e := b.m[k]
	if now.Sub(e.until) > sourceBrakeMax {
		e.misses = 0
	}
	e.misses++
	e.until = now.Add(brakeFor(e.misses))
	b.m[k] = e
}

// paid lifts the brakes of the source for the target and for all targets.
func (b *sourceBrake) paid(source, target string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.m, brakeKey{source, target})
	delete(b.m, brakeKey{source, ""})
}

// onwardFailure reports whether a failure came from the source peer (failure
// source index 1) on a route of four hops or more, where the next channel
// leads to a node other than the target.
func onwardFailure(code, fsi, hops int) bool {
	return edgeFailureCodes[code] && fsi == 1 && hops >= 4
}
