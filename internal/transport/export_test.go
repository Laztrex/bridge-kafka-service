package transport

import (
	"context"
	"testing"
	"time"

	"bridge-kafka-service/internal/schemas"
)

// RetryState and SendToServiceRetrying expose the retry policy of a pipeline to
// the external tests.
type RetryState = retryState

func NewRetryState() *RetryState { return &retryState{} }

func SendToServiceRetrying(
	abandon context.Context,
	logTransaction schemas.TransactionLogger,
	upstream Upstream,
	msg *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	timeout time.Duration,
	retry *RetryState,
) {
	sendToService(abandon, logTransaction, upstream, msg, queue, timeout, retry)
}

func (r *retryState) Succeeded() { r.succeeded() }

// FastRetries shrinks the retry timing for one test.
func FastRetries(t testing.TB, start, max time.Duration) {
	t.Helper()
	oldStart, oldMax := RetryBackoffStart, RetryBackoffMax
	RetryBackoffStart, RetryBackoffMax = start, max
	t.Cleanup(func() { RetryBackoffStart, RetryBackoffMax = oldStart, oldMax })
}

// FastModeLimit lowers the effective fast-mode in-flight ceiling for one test.
func FastModeLimit(t testing.TB, limit int) {
	t.Helper()
	old := fastModeLimit
	fastModeLimit = limit
	t.Cleanup(func() { fastModeLimit = old })
}

// ConcurrencyLimit exposes the pipeline limit rule to the external tests.
var ConcurrencyLimit = concurrencyLimit

// PoisonAttempts is the number of failed attempts before a record may be skipped.
func PoisonAttempts() int { return poisonAttempts }
