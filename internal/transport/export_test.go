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
	sendToService(abandon, logTransaction, upstream, msg, queue, timeout, retry, nil)
}

// SendToServiceWithState is SendToServiceRetrying that also reports into state.
func SendToServiceWithState(
	abandon context.Context,
	logTransaction schemas.TransactionLogger,
	upstream Upstream,
	msg *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	timeout time.Duration,
	retry *RetryState,
	state *PipelineState,
) {
	sendToService(abandon, logTransaction, upstream, msg, queue, timeout, retry, state)
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

// HTTPMaxResponseBytes is the response-body limit of the HTTP upstream.
const HTTPMaxResponseBytes = httpMaxResponseBytes

// Permanent marks err as a permanent upstream failure, as a transport would.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err is marked as a permanent upstream failure.
var IsPermanent = isPermanent

// HeaderIssue is a header the transport cannot carry, as reported by HeaderIssues.
type HeaderIssue struct {
	Name string
	Drop bool
}

// HeaderIssues returns what the transport of up reports about headers (nil without a checker).
func HeaderIssues(up Upstream, headers map[string]string) []HeaderIssue {
	c, ok := up.(headerChecker)
	if !ok {
		return nil
	}
	var out []HeaderIssue
	for _, i := range c.headerIssues(headers) {
		out = append(out, HeaderIssue{Name: i.name, Drop: i.drop})
	}
	return out
}

// IsTransportControlled reports whether an HTTP call never takes name from a record.
var IsTransportControlled = isTransportControlled
