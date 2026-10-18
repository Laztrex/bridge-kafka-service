package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/mocksarama"
)

func TestIsNonTransientPublishError(t *testing.T) {
	marshalErr := func() error {
		_, err := json.Marshal(json.RawMessage(`{"broken":`))
		assert.Error(t, err, "the fixture must be a real marshal failure")
		return err
	}()

	cases := []struct {
		name         string
		err          error
		nonTransient bool
	}{
		{"marshal error", marshalErr, true},
		{"marshal error wrapped", fmt.Errorf("send: %w", marshalErr), true},
		{"record too large for the broker", sarama.ErrMessageSizeTooLarge, true},
		{"corrupt message", sarama.ErrInvalidMessage, true},
		{"invalid message size", sarama.ErrInvalidMessageSize, true},
		{"producer error wrapping a non-transient KError",
			&sarama.ProducerError{Msg: &sarama.ProducerMessage{Topic: "out"}, Err: sarama.ErrMessageSizeTooLarge}, true},
		{"batch of non-transient producer errors",
			sarama.ProducerErrors{
				{Msg: &sarama.ProducerMessage{Topic: "out"}, Err: sarama.ErrInvalidMessage},
				{Msg: &sarama.ProducerMessage{Topic: "out"}, Err: sarama.ErrInvalidMessageSize},
			}, true},
		{"batch with one transient error",
			sarama.ProducerErrors{
				{Msg: &sarama.ProducerMessage{Topic: "out"}, Err: sarama.ErrMessageSizeTooLarge},
				{Msg: &sarama.ProducerMessage{Topic: "out"}, Err: sarama.ErrOutOfBrokers},
			}, false},
		{"out of brokers", sarama.ErrOutOfBrokers, false},
		{"not leader for partition", sarama.ErrNotLeaderForPartition, false},
		{"timeout", context.DeadlineExceeded, false},
		{"wrapped timeout", fmt.Errorf("produce request failed: %w", context.DeadlineExceeded), false},
		{"no error", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.nonTransient, isNonTransientPublishError(tc.err))
		})
	}
}

var errPublishDown = errors.New("broker unreachable")

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

// captureLogs redirects the global logger for one test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	old := log.Logger
	log.Logger = zerolog.New(buf)
	t.Cleanup(func() { log.Logger = old })
	return buf
}

// fastPublishBackoff shrinks the shared retry backoff for one test.
func fastPublishBackoff(t *testing.T, start, max time.Duration) {
	t.Helper()
	oldStart, oldMax := transport.RetryBackoffStart, transport.RetryBackoffMax
	transport.RetryBackoffStart, transport.RetryBackoffMax = start, max
	t.Cleanup(func() { transport.RetryBackoffStart, transport.RetryBackoffMax = oldStart, oldMax })
}

// publishLogger returns a file-backed transaction logger and its metrics file.
func publishLogger(t *testing.T) (*observability.FileTransactionLogger, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	t.Cleanup(l.Shutdown)
	return l, file
}

// countMetricValue counts metric lines (after Shutdown) where metric_name is
// name and metric_value is value: every event close writes a success and a
// failure line, so the value distinguishes the recorded outcome.
func countMetricValue(t *testing.T, file string, name string, value string) int {
	t.Helper()
	data, err := os.ReadFile(file)
	require.NoError(t, err)

	hits := 0
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var fields map[string]string
		require.NoError(t, json.Unmarshal([]byte(line), &fields), "bad metric line %q", line)
		if fields["metric_name"] == name && fields["metric_value"] == value {
			hits++
		}
	}
	return hits
}

// newPublishEvent builds a response event whose policy is retry or skip, with a
// counted ack.
func newPublishEvent(topic string, retry bool) (*schemas.ConsumerEvent, *int32) {
	acks := int32(0)
	e := &schemas.ConsumerEvent{
		ResponseTopic: topic,
		Values: &schemas.ContextValues{
			RequestId: "rr-1",
			Path:      "/predict",
			Body:      &schemas.RequestBody{Data: []byte(`{}`), Headers: map[string]string{}},
		},
	}
	if retry {
		e.Config = &schemas.ConsumerConfig{OnFailure: schemas.FailureRetry, Service: schemas.HostConfig{MaxConn: 1}}
	} else {
		e.Config = &schemas.ConsumerConfig{OnFailure: schemas.FailureSkip}
	}
	e.SetAck(func() { atomic.AddInt32(&acks, 1) })
	return e, &acks
}

// newMockRegistry returns a registry over a sarama mock producer whose
// expectations the test scripts (one per SendMessage, in order).
func newMockRegistry(t *testing.T, topics ...string) (*router.ProducerRegistry, *mocks.SyncProducer) {
	t.Helper()
	p := mocksarama.NewSyncProducer(t)
	mp, ok := p.(*mocks.SyncProducer)
	require.True(t, ok, "mocksarama.NewSyncProducer returns %T", p)
	reg := router.NewProducerRegistry()
	reg.Set(p, topics)
	return reg, mp
}

func TestPublishOneRetry_FailsTwiceThenSucceeds(t *testing.T) {
	fastPublishBackoff(t, time.Millisecond, 5*time.Millisecond)
	logs := captureLogs(t)
	l, file := publishLogger(t)
	reg, producer := newMockRegistry(t, "resp")
	producer.ExpectSendMessageAndFail(errPublishDown)
	producer.ExpectSendMessageAndFail(errPublishDown)
	producer.ExpectSendMessageAndSucceed()
	ev, acks := newPublishEvent("resp", true)

	publishOne(context.Background(), l, reg, ev)
	l.Shutdown()

	assert.Equal(t, int32(1), atomic.LoadInt32(acks), "acked once, after publication succeeds")
	assert.Equal(t, 1, ev.Event.Success)
	assert.Zero(t, ev.Event.Failure)
	assert.Equal(t, 2, logs.count("response publish failed, retrying"), "a warning per failed attempt")
	assert.Equal(t, 1, countMetricValue(t, file, consts.MetricsSuccessMapName[consts.EventResponseService], "1"),
		"exactly one success outcome is recorded")
	assert.Zero(t, countMetricValue(t, file, consts.MetricsFailureMapName[consts.EventResponseService], "1"))
}

func TestPublishOneRetry_ContextCancelledDuringBackoffDoesNotAck(t *testing.T) {
	fastPublishBackoff(t, time.Hour, time.Hour) // the loop sits in the backoff wait
	logs := captureLogs(t)
	l, _ := publishLogger(t)
	reg, producer := newMockRegistry(t, "resp")
	producer.ExpectSendMessageAndFail(errPublishDown)
	ev, acks := newPublishEvent("resp", true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		publishOne(ctx, l, reg, ev)
	}()
	require.Eventually(t, func() bool { return logs.count("response publish failed, retrying") == 1 },
		3*time.Second, 5*time.Millisecond, "the first attempt must fail before the wait")
	cancel()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the backoff wait did not end on cancellation")
	}
	assert.Zero(t, atomic.LoadInt32(acks), "the record must be redelivered")
	assert.Zero(t, ev.Event.Success)
	assert.Zero(t, ev.Event.Failure, "an abandoned publication records no outcome")
	assert.Equal(t, 1, logs.count("response publication abandoned on shutdown"))
}

func TestPublishOneRetry_NonTransientErrorSettlesAfterOneAttempt(t *testing.T) {
	logs := captureLogs(t)
	l, file := publishLogger(t)
	reg, producer := newMockRegistry(t, "resp")
	producer.ExpectSendMessageAndFail(sarama.ErrMessageSizeTooLarge)
	ev, acks := newPublishEvent("resp", true)

	publishOne(context.Background(), l, reg, ev)
	l.Shutdown()

	assert.Equal(t, int32(1), atomic.LoadInt32(acks), "a non-transient error settles the record")
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Zero(t, ev.Event.Success)
	assert.Zero(t, logs.count("response publish failed, retrying"), "the write is not repeated")
	assert.Equal(t, 1, countMetricValue(t, file, consts.MetricsFailureMapName[consts.EventResponseService], "1"),
		"the failure is recorded once")
}

func TestResponsePublishRetry_DrainGivesOneAttemptAndLeavesUnsettled(t *testing.T) {
	q := transport.NewResponseQueue()
	l, _ := publishLogger(t)
	reg, producer := newMockRegistry(t, "resp")
	producer.ExpectSendMessageAndFail(errPublishDown)
	producer.ExpectSendMessageAndFail(errPublishDown)

	ev1, acks1 := newPublishEvent("resp", true)
	ev2, acks2 := newPublishEvent("resp", true)
	ev2.Values.RequestId = "rr-2"

	// both events queued, then ctx cancelled: the drain that follows must give
	// each a single attempt (a retry would exhaust the mock's expectations)
	// and leave both unsettled, so the records are redelivered
	q <- ev1
	q <- ev2
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { defer close(done); ResponsePublish(ctx, q, 1, l, reg) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ResponsePublish did not drain and stop")
	}

	assert.Zero(t, atomic.LoadInt32(acks1), "a failed drain attempt leaves the record unsettled")
	assert.Zero(t, atomic.LoadInt32(acks2), "a failed drain attempt leaves the record unsettled")
	for _, ev := range []*schemas.ConsumerEvent{ev1, ev2} {
		assert.Zero(t, ev.Event.Success)
		assert.Zero(t, ev.Event.Failure, "an unsettled drain failure records no outcome")
	}
}

func TestPublishOneRetry_SkipEventKeepsSingleAttempt(t *testing.T) {
	l, _ := publishLogger(t)
	reg, producer := newMockRegistry(t, "resp")
	producer.ExpectSendMessageAndFail(errPublishDown)
	ev, acks := newPublishEvent("resp", false)

	publishOne(context.Background(), l, reg, ev)

	assert.Equal(t, int32(1), atomic.LoadInt32(acks), "a skip event settles after one attempt")
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Zero(t, ev.Event.Success)
}
