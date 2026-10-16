package observability

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/fakeclock"
	"bridge-kafka-service/testutil/metricsfile"
	"bridge-kafka-service/testutil/mockals"
)

func TestNewFileTransactionLogger_UnwritablePath(t *testing.T) {
	_, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "missing", "m.log"), "")
	assert.Error(t, err)
}

func TestNewFileTransactionLogger_BadEndpointKeepsRunning(t *testing.T) {
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "%zz://bad")
	require.NoError(t, err, "a broken audit endpoint must not prevent startup")
	l.Shutdown()
}

func TestNewFileTransactionLogger_UnwritableAuditPath(t *testing.T) {
	file := filepath.Join(t.TempDir(), "m.log")
	require.NoError(t, os.Mkdir(file+".audit", 0o755))

	_, err := NewFileTransactionLogger(file, "")

	assert.ErrorContains(t, err, "cannot open audit log File")
}

func TestShutdownFlushesQueuedEventsAndAudit(t *testing.T) {
	l, file := newTestLogger(t)

	for i := 0; i < 10; i++ {
		e := &schemas.Event{EventType: consts.EventConsume, RequestId: "r1", Status: "success", Success: 1}
		l.Start(e)
		l.Close(e)
	}
	LogAuditEventWithLatency(l, "evt", "r1", &schemas.RequestBody{Data: []byte(`{}`)}, consts.AccessLogTypeDownstreamStart, "/p", time.Now())

	l.Shutdown()
	l.Shutdown()

	metrics, err := os.ReadFile(file)
	require.NoError(t, err)
	assert.Equal(t, 10, strings.Count(string(metrics), "consume_success_total"), "every queued event must be written")

	audit, err := os.ReadFile(file + ".audit")
	require.NoError(t, err)
	assert.Contains(t, string(audit), "envoy-ingress-als")
}

func TestCloseAfterShutdownDoesNotBlock(t *testing.T) {
	l, _ := newTestLogger(t)
	l.Shutdown()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			l.Close(&schemas.Event{EventType: consts.EventConsume})
		}
		l.CloseAudit(nil)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close/CloseAudit blocked after Shutdown")
	}
}

func TestStartUsesInjectedClock(t *testing.T) {
	l, _ := newTestLogger(t)
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	fake := fakeclock.New(start)
	l.SetClock(fake.Now)

	e := &schemas.Event{}
	l.Start(e)
	assert.Equal(t, start, e.Started)

	fake.Advance(150 * time.Millisecond)
	l.Start(e)
	assert.Equal(t, start.Add(150*time.Millisecond), e.Started)

	l.SetClock(nil)
	assert.WithinDuration(t, time.Now(), l.now(), time.Second, "nil restores the wall clock")
}

func TestDuration_InterleavedEventsAreTimedIndependently(t *testing.T) {
	l, file, clock := fakeClockLogger(t)
	a, b := ev("A"), ev("B")

	l.Start(a)
	clock.Advance(150 * time.Millisecond)
	l.Start(b)
	clock.Advance(30 * time.Millisecond)
	l.Close(b)
	clock.Advance(30 * time.Millisecond)
	l.Close(a)
	l.Shutdown()

	got := metricsfile.Durations(t, file, consts.EventConsume)
	assert.Equal(t, int64(210), got["A"], "A spans the whole interval")
	assert.Equal(t, int64(30), got["B"], "B spans only its own interval")
}

// Holding l.mu stalls the metrics loop, so events wait in the queue after Close.
func TestDuration_QueueWaitIsNotCounted(t *testing.T) {
	l, file, clock := fakeClockLogger(t)
	l.mu.Lock()
	blocker, q := ev("block"), ev("Q")

	l.Start(blocker)
	l.Close(blocker)
	l.Start(q)
	clock.Advance(5 * time.Millisecond)
	l.Close(q)
	clock.Advance(300 * time.Millisecond)
	l.mu.Unlock()
	l.Shutdown()

	assert.Equal(t, int64(5), metricsfile.Durations(t, file, consts.EventConsume)["Q"])
}

func TestDuration_SequentialEvent(t *testing.T) {
	l, file, clock := fakeClockLogger(t)
	e := ev("S")

	l.Start(e)
	clock.Advance(60 * time.Millisecond)
	l.Close(e)
	l.Shutdown()

	assert.Equal(t, int64(60), metricsfile.Durations(t, file, consts.EventConsume)["S"])
}

func TestDuration_NeverStartedEventIsZero(t *testing.T) {
	l, file, _ := fakeClockLogger(t)

	l.Close(ev("N"))
	l.Shutdown()

	assert.Equal(t, int64(0), metricsfile.Durations(t, file, consts.EventConsume)["N"])
}

func TestDuration_ClockGoingBackwardsIsClampedToZero(t *testing.T) {
	l, file, clock := fakeClockLogger(t)
	e := ev("R")

	l.Start(e)
	clock.Advance(-time.Second)
	l.Close(e)
	l.Shutdown()

	assert.Equal(t, int64(0), metricsfile.Durations(t, file, consts.EventConsume)["R"])
}

func TestDuration_EveryEventTypeGetsItsOwnMetric(t *testing.T) {
	for _, et := range []consts.EventType{consts.EventConsume, consts.EventProduce, consts.EventRequestService, consts.EventResponseService} {
		t.Run(consts.LogMapEventName[et], func(t *testing.T) {
			l, file, clock := fakeClockLogger(t)
			e := &schemas.Event{EventType: et, RequestId: "X"}

			l.Start(e)
			clock.Advance(7 * time.Millisecond)
			l.Close(e)
			l.Shutdown()

			assert.Equal(t, int64(7), metricsfile.Durations(t, file, et)["X"])
		})
	}
}

// Real clock, many goroutines: every event must be measured by its own span,
// whatever the interleaving (run under -race).
func TestDuration_ConcurrentEventsRealClock(t *testing.T) {
	l, file := newTestLogger(t)
	const n = 200

	sleeps := make([]time.Duration, n)
	var wg sync.WaitGroup
	began := time.Now()
	for i := 0; i < n; i++ {
		sleeps[i] = time.Duration(i%20) * 3 * time.Millisecond
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := ev(strconv.Itoa(i))
			l.Start(e)
			time.Sleep(sleeps[i])
			l.Close(e)
		}(i)
	}
	wg.Wait()
	wall := time.Since(began).Milliseconds() + 5
	l.Shutdown()

	got := metricsfile.Durations(t, file, consts.EventConsume)
	require.Len(t, got, n)
	for i := 0; i < n; i++ {
		ms := got[strconv.Itoa(i)]
		assert.GreaterOrEqual(t, ms, sleeps[i].Milliseconds()-1, "event %d slept %v", i, sleeps[i])
		assert.LessOrEqual(t, ms, wall, "event %d cannot outlast the whole test", i)
	}
}

func TestCloseAudit_FullQueueDropsAfterBoundedWait(t *testing.T) {
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	defer l.Shutdown()

	start := time.Now()
	for i := 0; i < cap(l.eventsAudit)+1; i++ {
		l.CloseAudit(&accesslog.StreamAccessLogsMessage{})
	}

	assert.Len(t, l.eventsAudit, cap(l.eventsAudit))
	assert.Less(t, time.Since(start), 5*time.Second, "back-pressure is bounded")
}

func TestAuditErrorReportingNeverBlocks(t *testing.T) {
	l, _ := newTestLogger(t)
	for i := 0; i < 5; i++ {
		l.reportAuditError(assert.AnError)
	}
}

func TestAuditToFile_WriteFailureIsReportedNotFatal(t *testing.T) {
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	require.NoError(t, l.auditFile.Close())

	l.auditToFile(&accesslog.StreamAccessLogsMessage{})

	select {
	case e := <-l.errorsAudit:
		assert.ErrorContains(t, e, "failed to write audit event")
	default:
		t.Fatal("write failure was not reported")
	}
}

func TestAuditToGRPC_DeliversEntryAndKeepsFileFallback(t *testing.T) {
	srv := mockals.Start(t)
	l, file := newGRPCLogger(t, srv.Addr)

	LogAuditEventWithLatency(l, "evt", "r1", &schemas.RequestBody{Data: []byte(`{}`)}, consts.AccessLogTypeDownstreamStart, "/p", time.Now())
	l.Shutdown()

	msgs := srv.Messages()
	require.Len(t, msgs, 1)
	assert.Equal(t, "envoy-ingress-als", msgs[0].GetIdentifier().GetLogName())
	assert.Contains(t, auditLines(t, file), "envoy-ingress-als", "file fallback always active")
}

func TestAuditToGRPC_ServerErrorDoesNotStopProcessing(t *testing.T) {
	srv := mockals.Start(t)
	srv.FailWith(errors.New("receiver rejected"))
	l, file := newGRPCLogger(t, srv.Addr)

	for i := 0; i < 3; i++ {
		LogAuditEventWithLatency(l, "evt", "r1", nil, consts.AccessLogTypeDownstreamEnd, "/p", time.Now())
	}
	l.Shutdown()

	assert.Equal(t, 3, strings.Count(auditLines(t, file), "envoy-egress-als"), "every entry still reaches the file")
}

func TestAuditToGRPC_UnreachableEndpointFallsBackToFile(t *testing.T) {
	l, file := newGRPCLogger(t, "127.0.0.1:1")

	LogAuditEventWithLatency(l, "evt", "r1", nil, consts.AccessLogTypeDownstreamStart, "/p", time.Now())
	l.Shutdown()

	assert.Contains(t, auditLines(t, file), "envoy-ingress-als")
}

func TestCloseAll_ClosesEverythingEvenWhenOneCloseFails(t *testing.T) {
	srv := mockals.Start(t)
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), srv.Addr)
	require.NoError(t, err)
	require.NoError(t, l.File.Close())

	l.CloseAll()

	_, werr := l.auditFile.Write([]byte("x"))
	assert.ErrorIs(t, werr, os.ErrClosed, "audit file must be closed despite the earlier failure")
	assert.Equal(t, connectivity.Shutdown, l.grpcConn.GetState())
}

func newTestLogger(t *testing.T) (*FileTransactionLogger, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "metrics.log")
	l, err := NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	t.Cleanup(l.Shutdown)
	return l, file
}

func fakeClockLogger(t *testing.T) (*FileTransactionLogger, string, *fakeclock.Clock) {
	t.Helper()
	l, file := newTestLogger(t)
	clock := fakeclock.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	l.SetClock(clock.Now)
	return l, file, clock
}

func newGRPCLogger(t *testing.T, endpoint string) (*FileTransactionLogger, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "metrics.log")
	l, err := NewFileTransactionLogger(file, endpoint)
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	t.Cleanup(l.Shutdown)
	return l, file
}

func auditLines(t *testing.T, file string) string {
	t.Helper()
	b, err := os.ReadFile(file + ".audit")
	require.NoError(t, err)
	return string(b)
}

func ev(requestId string) *schemas.Event {
	return &schemas.Event{EventType: consts.EventConsume, RequestId: requestId}
}
