package transport

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Retry timing for on_failure: retry. Fixed in code on purpose (the setting is a
// single word); variables only so that tests can shrink them.
var (
	RetryBackoffStart = time.Second
	RetryBackoffMax   = 30 * time.Second
	// poisonAttempts is how many failed attempts a record gets before it may be
	// skipped as poison (only while other calls of the pipeline succeed).
	poisonAttempts = 5
)

// retryBackoffJitter spreads the waits of concurrent slots by ±20% so they do not
// hit a recovering service in lockstep.
const retryBackoffJitter = 0.2

// RetryBackoff is the wait after failed attempt n (1-based): the start value
// doubled per attempt, capped, then jittered. It is shared by the upstream
// retry and the response-publication retry, so both back off identically.
func RetryBackoff(attempt int) time.Duration {
	d := RetryBackoffStart
	for i := 1; i < attempt && d < RetryBackoffMax; i++ {
		d *= 2
	}
	d = min(d, RetryBackoffMax)
	return time.Duration(float64(d) * (1 + retryBackoffJitter*(2*rand.Float64()-1)))
}

// retryState is shared by the calls of one pipeline running with on_failure: retry.
// It tells an outage (nothing succeeds) from a poison record (others succeed).
type retryState struct {
	lastSuccess atomic.Int64 // unix nanoseconds of the last call that yielded a response
}

func (r *retryState) succeeded() { r.lastSuccess.Store(time.Now().UnixNano()) }

// succeededSince reports whether any call of the pipeline yielded a response after t.
func (r *retryState) succeededSince(t time.Time) bool {
	return r.lastSuccess.Load() > t.UnixNano()
}
