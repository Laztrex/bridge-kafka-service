// Package fakeclock is a deterministic clock for tests: pass Clock.Now wherever production code
// accepts a func() time.Time (e.g. FileTransactionLogger.SetClock).
package fakeclock

import (
	"sync"
	"time"
)

// Clock is a manually advanced instant, safe for concurrent use.
type Clock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration // added after every Now() call (0 = manual advance only)
}

// NewStepping returns a Clock that advances by step on every Now, for code that reads the
// clock twice with no chance for the test to advance it in between.
func NewStepping(start time.Time, step time.Duration) *Clock {
	return &Clock{t: start, step: step}
}

// New returns a Clock initialised to the supplied starting instant.
func New(start time.Time) *Clock {
	return &Clock{t: start}
}

// Now returns the currently simulated instant (and, for a stepping clock,
// moves it forward by the step).
func (f *Clock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.t
	f.t = f.t.Add(f.step)
	return now
}

// Advance moves the simulated instant by d (negative d rewinds it).
func (f *Clock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// Set overwrites the simulated instant.
func (f *Clock) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = t
}
