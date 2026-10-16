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
	timeout time.Duration,
	retry *RetryState,
) {
	sendToService(abandon, logTransaction, upstream, msg, timeout, retry)
}

func (r *retryState) Succeeded() { r.succeeded() }

var RetryBackoff = retryBackoff

// FastRetries shrinks the retry timing for one test.
func FastRetries(t testing.TB, start, max time.Duration) {
	t.Helper()
	oldStart, oldMax := retryBackoffStart, retryBackoffMax
	retryBackoffStart, retryBackoffMax = start, max
	t.Cleanup(func() { retryBackoffStart, retryBackoffMax = oldStart, oldMax })
}

// PoisonAttempts is the number of failed attempts before a record may be skipped.
func PoisonAttempts() int { return poisonAttempts }
