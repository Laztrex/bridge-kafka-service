package transport_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

var errDown = errors.New("connection refused")

// scriptedUpstream fails while fail(call) is true, otherwise answers.
type scriptedUpstream struct {
	calls atomic.Int32
	fail  func(call int32) bool
}

func (s *scriptedUpstream) Call(context.Context, transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	if s.fail(s.calls.Add(1)) {
		return transport.UpstreamResponse{}, errDown
	}
	return transport.UpstreamResponse{Body: []byte(`{"ok":true}`), Header: http.Header{}}, nil
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) count(sub string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Count(s.b.String(), sub)
}

// captureLog redirects the global logger for one test.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	old := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = old })
	return buf
}

func TestRetryBackoff_DoublesUpToTheCap(t *testing.T) {
	within := func(attempt int, base time.Duration) {
		t.Helper()
		for range 50 {
			d := transport.RetryBackoff(attempt)
			assert.GreaterOrEqual(t, d, time.Duration(float64(base)*0.8), "attempt %d", attempt)
			assert.LessOrEqual(t, d, time.Duration(float64(base)*1.2), "attempt %d", attempt)
		}
	}
	within(1, time.Second)
	within(2, 2*time.Second)
	within(5, 16*time.Second)
	within(6, 30*time.Second)
	within(100, 30*time.Second)
}

func TestRetry_OutageThenRecoveryDeliversOnceWithOneOutcome(t *testing.T) {
	drainPublish()
	transport.FastRetries(t, time.Millisecond, 5*time.Millisecond)
	logs := captureLog(t)
	up := &scriptedUpstream{fail: func(call int32) bool { return call <= 3 }}
	ev, acks := newEvent("resp")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, time.Second, transport.NewRetryState())

	assert.Equal(t, int32(4), up.calls.Load())
	assert.Equal(t, 1, ev.Event.Success)
	assert.Zero(t, ev.Event.Failure, "a recovered record is not a failure")
	assert.Equal(t, 3, logs.count("upstream call failed, retrying"))
	assert.Zero(t, *acks, "the offset waits for the response to be published")
	select {
	case out := <-transport.PublishChannel:
		out.Ack()
		assert.Equal(t, 1, *acks)
	case <-time.After(time.Second):
		t.Fatal("response event not published")
	}
	assert.Empty(t, transport.PublishChannel, "published exactly once")
}

func TestRetry_ErrorResponseIsNotRetried(t *testing.T) {
	drainPublish()
	up := &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{"error":"busy"}`)}}
	ev, _ := newEvent("resp")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, time.Second, transport.NewRetryState())

	assert.Equal(t, 1, ev.Event.Success)
	assert.Len(t, transport.PublishChannel, 1)
	drainPublish()
}

func TestRetry_ShutdownDuringWaitLeavesTheRecordUnsettled(t *testing.T) {
	transport.FastRetries(t, time.Hour, time.Hour)
	up := &scriptedUpstream{fail: func(int32) bool { return true }}
	ev, acks := newEvent("")
	abandon, giveUp := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		transport.SendToServiceRetrying(abandon, logger(t), up, ev, time.Second, transport.NewRetryState())
	}()
	require.Eventually(t, func() bool { return up.calls.Load() == 1 }, time.Second, 5*time.Millisecond)
	giveUp()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the backoff wait did not end on shutdown")
	}
	assert.Zero(t, *acks, "the record must be redelivered")
	assert.Zero(t, ev.Event.Failure)
	assert.Zero(t, ev.Event.Success)
}

func TestRetry_PoisonRecordIsSkippedWhileOthersSucceed(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	logs := captureLog(t)
	state := transport.NewRetryState()
	up := &scriptedUpstream{fail: func(call int32) bool {
		if call == 2 {
			state.Succeeded()
		}
		return true
	}}
	ev, acks := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, time.Second, state)

	assert.Equal(t, int32(transport.PoisonAttempts()), up.calls.Load())
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "a poison record is settled")
	assert.Equal(t, 1, logs.count("poison record"))
	assert.Equal(t, 1, logs.count(`"`+consts.HeaderRequestIdKey+`":"req-1"`+`,"attempt":5`), "the poison warning names the record")
}

func TestRetry_NothingIsSkippedWhileNoCallSucceeds(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	const calls = 12
	abandon, giveUp := context.WithCancel(context.Background())
	up := &scriptedUpstream{fail: func(call int32) bool {
		if call == calls {
			giveUp()
		}
		return true
	}}
	ev, acks := newEvent("")

	transport.SendToServiceRetrying(abandon, logger(t), up, ev, time.Second, transport.NewRetryState())

	assert.GreaterOrEqual(t, up.calls.Load(), int32(calls), "kept retrying past the poison threshold")
	assert.Zero(t, ev.Event.Failure, "an outage never skips a record")
	assert.Zero(t, *acks)
}

func TestRetry_SlotsRetryingStopFurtherCalls(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, 5*time.Millisecond)
	var down atomic.Bool
	down.Store(true)
	var inFlight, maxInFlight atomic.Int32
	seen := sync.Map{}
	up := upstreamFunc(func(req transport.UpstreamRequest) (transport.UpstreamResponse, error) {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			m := maxInFlight.Load()
			if n <= m || maxInFlight.CompareAndSwap(m, n) {
				break
			}
		}
		seen.Store(req.Headers[consts.HeaderRequestIdKey], true)
		if down.Load() {
			return transport.UpstreamResponse{}, errDown
		}
		return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
	})
	conf := &schemas.ConsumerConfig{OnFailure: schemas.FailureRetry, Service: schemas.HostConfig{MaxConn: 2, Timeout: time.Second}}
	ch := make(chan *schemas.ConsumerEvent, 10)
	var acks atomic.Int32
	for i := range 5 {
		ev, _ := newEvent("")
		id := fmt.Sprintf("req-%d", i)
		ev.Values.RequestId, ev.Values.Body.Headers[consts.HeaderRequestIdKey] = id, id
		ev.SetAck(func() { acks.Add(1) })
		ch <- ev
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		transport.ConsumerResponseSlow(logger(t), conf, up, ch, context.Background())
	}()

	time.Sleep(100 * time.Millisecond)
	assert.LessOrEqual(t, maxInFlight.Load(), int32(2))
	distinct := 0
	seen.Range(func(any, any) bool { distinct++; return true })
	assert.Equal(t, 2, distinct, "only the records holding the slots reached the service")
	assert.Zero(t, acks.Load(), "nothing is settled during the outage")

	down.Store(false)
	require.Eventually(t, func() bool { return acks.Load() == 5 }, 2*time.Second, 5*time.Millisecond)
	close(ch)
	<-done
}

type upstreamFunc func(transport.UpstreamRequest) (transport.UpstreamResponse, error)

func (f upstreamFunc) Call(_ context.Context, req transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	return f(req)
}

func fillPublishChannel(t *testing.T) {
	t.Helper()
	drainPublish()
	for range cap(transport.PublishChannel) {
		transport.PublishChannel <- &schemas.ConsumerEvent{}
	}
	t.Cleanup(drainPublish)
}

func TestRetry_FullPublishChannelWaitsInsteadOfDropping(t *testing.T) {
	fillPublishChannel(t)
	up := &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{"ok":true}`)}}
	ev, acks := newEvent("resp")
	done := make(chan struct{})

	go func() {
		defer close(done)
		transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, time.Second, transport.NewRetryState())
	}()
	select {
	case <-done:
		t.Fatal("the response must wait for space, not be dropped")
	case <-time.After(50 * time.Millisecond):
	}
	assert.Zero(t, *acks)

	<-transport.PublishChannel
	<-done
	assert.Equal(t, 1, ev.Event.Success)
	assert.Zero(t, *acks, "settled only when the published response is acked")
	var published *schemas.ConsumerEvent
	for len(transport.PublishChannel) > 0 {
		published = <-transport.PublishChannel
	}
	require.NotNil(t, published)
	published.Ack()
	assert.Equal(t, 1, *acks)
}

func TestRetry_ShutdownWhileWaitingForThePublishChannel(t *testing.T) {
	fillPublishChannel(t)
	up := &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}
	ev, acks := newEvent("resp")
	abandon, giveUp := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		transport.SendToServiceRetrying(abandon, logger(t), up, ev, time.Second, transport.NewRetryState())
	}()
	time.Sleep(20 * time.Millisecond)
	giveUp()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the wait for the publish channel did not end on shutdown")
	}
	assert.Zero(t, *acks, "the record must be redelivered")
	assert.Zero(t, ev.Event.Success)
	assert.Zero(t, ev.Event.Failure)
	ev.Ack()
	assert.Equal(t, 1, *acks, "the ack stays with the record, not with a discarded response")
}
