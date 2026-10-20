package observability

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v3 "github.com/envoyproxy/go-control-plane/envoy/data/accesslog/v3"
	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"
	"gopkg.in/natefinch/lumberjack.v2"

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

func TestAuditErrorReportingNeverBlocks(t *testing.T) {
	l, _ := newTestLogger(t)
	for i := 0; i < 5; i++ {
		l.reportAuditError(assert.AnError)
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

// failingSink fails every write and records whether it was closed.
type failingSink struct{ closed atomic.Bool }

func (*failingSink) Write([]byte) (int, error) { return 0, errors.New("disk gone") }
func (s *failingSink) Close() error {
	s.closed.Store(true)
	return errors.New("close failed")
}

// recordingSink records whether it was closed.
type recordingSink struct{ closed atomic.Bool }

func (*recordingSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *recordingSink) Close() error {
	s.closed.Store(true)
	return nil
}

func TestWriteAudit_WriteFailureIsReportedNotFatal(t *testing.T) {
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.auditSink = &failingSink{}

	l.writeAudit([]*accesslog.StreamAccessLogsMessage{{}})

	select {
	case e := <-l.errorsAudit:
		assert.ErrorContains(t, e, "failed to write audit event")
	default:
		t.Fatal("write failure was not reported")
	}
}

func TestCloseAll_ClosesEverythingEvenWhenOneCloseFails(t *testing.T) {
	srv := mockals.Start(t)
	l, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), srv.Addr)
	require.NoError(t, err)
	metrics, audit := &failingSink{}, &recordingSink{}
	l.Sink, l.auditSink = metrics, audit

	l.CloseAll()

	assert.True(t, metrics.closed.Load())
	assert.True(t, audit.closed.Load(), "audit file must be closed despite the earlier failure")
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

// rotate writes to w and rotates it n times, pausing so every backup gets its own timestamp.
func rotate(t *testing.T, w io.Writer, n int) {
	t.Helper()
	lj, ok := w.(*lumberjack.Logger)
	require.True(t, ok, "rotatingFile returns a lumberjack logger")
	for range n {
		_, err := w.Write([]byte("line\n"))
		require.NoError(t, err)
		require.NoError(t, lj.Rotate())
		time.Sleep(5 * time.Millisecond)
	}
}

func backups(t *testing.T, dir, pattern string) []string {
	t.Helper()
	got, err := filepath.Glob(filepath.Join(dir, pattern))
	require.NoError(t, err)
	return got
}

func TestRotatingFile_KeepsEveryBackupByDefault(t *testing.T) {
	dir := t.TempDir()
	w, err := rotatingFile(filepath.Join(dir, "metrics.log"), options{})
	require.NoError(t, err)

	rotate(t, w, 4)
	require.NoError(t, w.Close())

	require.Eventually(t, func() bool { return len(backups(t, dir, "metrics-*.log.gz")) == 4 }, 5*time.Second, 20*time.Millisecond,
		"every backup is kept without retention")
}

func TestRotatingFile_DeletesBackupsBeyondTheCount(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "metrics.log.audit")
	w, err := rotatingFile(name, options{maxBackups: 2})
	require.NoError(t, err)

	rotate(t, w, 4)
	require.NoError(t, w.Close())

	require.Eventually(t, func() bool {
		return len(backups(t, dir, "metrics.log-*.audit.gz")) == 2 && len(backups(t, dir, "metrics.log-*.audit")) == 0
	}, 5*time.Second, 20*time.Millisecond, "only the two newest backups survive")
	_, err = os.Stat(name)
	assert.NoError(t, err, "the current file is never deleted")
}

// Both files get the configured retention; without the option both keep everything.
func TestNewFileTransactionLogger_AppliesRetentionToBothFiles(t *testing.T) {
	limits := func(l *FileTransactionLogger) [2][2]int {
		m, a := l.Sink.(*lumberjack.Logger), l.auditSink.(*lumberjack.Logger)
		return [2][2]int{{m.MaxBackups, m.MaxAge}, {a.MaxBackups, a.MaxAge}}
	}

	plain, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	t.Cleanup(plain.Shutdown)
	assert.Equal(t, [2][2]int{}, limits(plain))

	bounded, err := NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "", WithRetention(10, 30))
	require.NoError(t, err)
	t.Cleanup(bounded.Shutdown)
	assert.Equal(t, [2][2]int{{10, 30}, {10, 30}}, limits(bounded))
}

// setVar overrides a tuning variable for one test.
func setVar[T any](t *testing.T, v *T, val T) {
	t.Helper()
	old := *v
	*v = val
	t.Cleanup(func() { *v = old })
}

type warnBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *warnBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *warnBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func captureWarnings(t *testing.T) *warnBuffer {
	t.Helper()
	buf := &warnBuffer{}
	old := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = old })
	return buf
}

// entry is an audit entry identified by n.
func entry(n int) *accesslog.StreamAccessLogsMessage {
	return &accesslog.StreamAccessLogsMessage{Identifier: &accesslog.StreamAccessLogsMessage_Identifier{LogName: fmt.Sprintf("entry-%d", n)}}
}

func countLines(s, sub string) int { return strings.Count(s, sub) }

func TestCloseAudit_BurstIsWrittenWithoutLoss(t *testing.T) {
	setVar(t, &auditQueueSize, 16)
	l, file := newTestLogger(t)

	for i := range 160 {
		l.CloseAudit(entry(i))
	}
	l.Shutdown()

	audit := auditLines(t, file)
	assert.Equal(t, 160, countLines(audit, `"log_name":"entry-`), "no entry may be dropped")
	assert.Less(t, strings.Index(audit, `"entry-0"`), strings.Index(audit, `"entry-159"`), "entries keep their order")
}

func TestAuditWriter_FsyncsWhenIdleAndOncePerBatchUnderLoad(t *testing.T) {
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	t.Cleanup(l.Shutdown)
	var syncs atomic.Int32
	real := l.syncAudit
	l.syncAudit = func() error { syncs.Add(1); return real() }

	for i := range 1000 {
		l.CloseAudit(entry(i)) // queued before the writer runs: a burst
	}
	l.RunAudit()
	require.Eventually(t, func() bool { return countLines(auditLines(t, file), `"entry-`) == 1000 }, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return syncs.Load() >= 1 }, time.Second, 5*time.Millisecond, "flushed once the queue ran empty")
	assert.LessOrEqual(t, syncs.Load(), int32(5), "a burst is flushed per batch, not per entry")

	before := syncs.Load()
	l.CloseAudit(entry(1000))
	require.Eventually(t, func() bool { return syncs.Load() > before }, time.Second, 5*time.Millisecond,
		"a single entry on an idle system is flushed within the second")
}

func TestAuditGRPC_SlowReceiverDoesNotSlowEmitters(t *testing.T) {
	setVar(t, &grpcQueueSize, 64)
	setVar(t, &grpcShutdownBudget, 300*time.Millisecond)
	logs := captureWarnings(t)
	srv := mockals.Start(t)
	srv.Delay(2 * time.Second)
	l, file := newGRPCLogger(t, srv.Addr)

	began := time.Now()
	for i := range 2000 {
		l.CloseAudit(entry(i))
	}
	assert.Less(t, time.Since(began), time.Second, "emitting is not slowed by the receiver")
	l.Shutdown()

	assert.Equal(t, 2000, countLines(auditLines(t, file), `"entry-`), "the audit file is complete")
	assert.Contains(t, logs.String(), "were not delivered to the ALS endpoint")
}

func TestAuditGRPC_SendsBatchesAndNothingTwice(t *testing.T) {
	srv := mockals.Start(t)
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := NewFileTransactionLogger(file, srv.Addr)
	require.NoError(t, err)
	for i := range 300 {
		l.CloseAudit(entry(i))
	}
	l.RunAudit()
	l.Shutdown()

	msgs := srv.Messages()
	require.Len(t, msgs, 300)
	seen := map[string]bool{}
	for _, m := range msgs {
		name := m.GetIdentifier().GetLogName()
		assert.False(t, seen[name], "%s delivered twice", name)
		seen[name] = true
	}
	assert.GreaterOrEqual(t, srv.Streams(), 2, "300 entries need at least two streams of 256")
	assert.Less(t, srv.Streams(), 300, "entries are batched, not one stream each")
}

func TestShutdown_BlackHoledReceiverIsBounded(t *testing.T) {
	setVar(t, &grpcShutdownBudget, 300*time.Millisecond)
	srv := mockals.Start(t)
	srv.Delay(time.Hour)
	l, file := newGRPCLogger(t, srv.Addr)
	for i := range 50 {
		l.CloseAudit(entry(i))
	}

	began := time.Now()
	l.Shutdown()

	assert.Less(t, time.Since(began), 2*time.Second, "shutdown is bounded by the gRPC budget")
	assert.Equal(t, 50, countLines(auditLines(t, file), `"entry-`))
}

func TestFiles_ArePrivateAndExistingModesAreFixed(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "m.log")
	require.NoError(t, os.WriteFile(file, nil, 0o755))
	require.NoError(t, os.Chmod(file, 0o755))

	l, err := NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.Shutdown()

	for _, name := range []string{file, file + ".audit"} {
		fi, err := os.Stat(name)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), name)
	}
}

func TestAuditFile_RotatesIntoACompressedBackupWithoutLoss(t *testing.T) {
	setVar(t, &rotateMaxSizeMB, 1)
	l, file := newTestLogger(t)
	big := strings.Repeat("x", 100<<10)
	for i := range 15 {
		e := entry(i)
		e.LogEntries = &accesslog.StreamAccessLogsMessage_HttpLogs{HttpLogs: &accesslog.StreamAccessLogsMessage_HTTPAccessLogEntries{
			LogEntry: []*v3.HTTPAccessLogEntry{{Request: &v3.HTTPRequestProperties{Path: big}}},
		}}
		l.CloseAudit(e)
	}
	l.Shutdown()

	var backups []string
	require.Eventually(t, func() bool {
		backups, _ = filepath.Glob(filepath.Join(filepath.Dir(file), "metrics.log-*.audit.gz"))
		pending, _ := filepath.Glob(filepath.Join(filepath.Dir(file), "metrics.log-*.audit"))
		return len(backups) >= 1 && len(pending) == 0 // compression runs in the background
	}, 5*time.Second, 20*time.Millisecond, "a compressed backup appears next to the audit file")

	total := countLines(auditLines(t, file), `"entry-`)
	for _, b := range backups {
		fi, err := os.Stat(b)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o600), fi.Mode().Perm(), "backups stay private")
		f, err := os.Open(b)
		require.NoError(t, err)
		zr, err := gzip.NewReader(f)
		require.NoError(t, err)
		data, err := io.ReadAll(zr)
		require.NoError(t, err)
		_ = f.Close()
		total += countLines(string(data), `"entry-`)
	}
	assert.Equal(t, 15, total, "no entry is lost across the rotation")
}
