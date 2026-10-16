package observability

import (
	"testing"
	"time"

	data_accesslog "github.com/envoyproxy/go-control-plane/envoy/data/accesslog/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/fakeclock"
)

func TestResolveForwardedFor(t *testing.T) {
	assert.Equal(t, "10.0.0.1", resolveForwardedFor(map[string]string{"x-forwarded-for": "10.0.0.1"}))
	assert.Equal(t, fallbackForwardedFor, resolveForwardedFor(nil))
	assert.Equal(t, fallbackForwardedFor, resolveForwardedFor(map[string]string{"x-forwarded-for": ""}))
}

func TestLogAuditEvent_NilBodyDoesNotPanic(t *testing.T) {
	l, _ := newTestLogger(t)
	assert.NotPanics(t, func() {
		LogAuditEventWithLatency(l, "e", "r", nil, consts.AccessLogTypeDownstreamEnd, "/p", time.Now())
	})
}

func TestAuditDuration_CoversProcessingFromTheGivenStart(t *testing.T) {
	l, clock := auditLoggerWithoutLoop(t)
	started := clock.Now()
	clock.Advance(42 * time.Millisecond)

	assert.Equal(t, 42*time.Millisecond, ingressDuration(t, l, started))
}

func TestAuditDuration_ZeroSpanIsReportedAsOneNanosecond(t *testing.T) {
	l, clock := auditLoggerWithoutLoop(t)

	assert.Equal(t, time.Nanosecond, ingressDuration(t, l, clock.Now()))
}

func TestLookupAccessLogType(t *testing.T) {
	assert.Equal(t, data_accesslog.AccessLogType_DownstreamStart, lookupAccessLogType(consts.AccessLogTypeDownstreamStart))
	assert.Equal(t, data_accesslog.AccessLogType_DownstreamEnd, lookupAccessLogType(consts.AccessLogTypeBusinessRequestTransferred))
	assert.Equal(t, data_accesslog.AccessLogType_NotSet, lookupAccessLogType("unknown"))
}

// ingressDuration emits one ingress audit entry through the logger and returns
// its CommonProperties.Duration.
func ingressDuration(t *testing.T, l *FileTransactionLogger, startedAt time.Time) time.Duration {
	t.Helper()
	LogAuditEventWithLatency(l, "evt", "r1", &schemas.RequestBody{Data: []byte(`{}`)},
		consts.AccessLogTypeDownstreamStart, "/p", startedAt)
	select {
	case msg := <-l.eventsAudit:
		return msg.GetHttpLogs().GetLogEntry()[0].GetCommonProperties().GetDuration().AsDuration()
	case <-time.After(time.Second):
		t.Fatal("no audit entry emitted")
		return 0
	}
}

func auditLoggerWithoutLoop(t *testing.T) (*FileTransactionLogger, *fakeclock.Clock) {
	t.Helper()
	l, err := NewFileTransactionLogger(t.TempDir()+"/m.log", "")
	require.NoError(t, err)
	t.Cleanup(l.Shutdown)
	clock := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l.SetClock(clock.Now)
	return l, clock
}
