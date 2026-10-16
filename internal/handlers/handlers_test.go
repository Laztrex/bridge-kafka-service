package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/middlewares"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/fakeclock"
	"bridge-kafka-service/testutil/metricsfile"
	"bridge-kafka-service/testutil/mocksarama"
)

func newLogger(t *testing.T) *observability.FileTransactionLogger {
	t.Helper()
	l, err := observability.NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.Run()
	l.RunAudit()
	t.Cleanup(l.Shutdown)
	return l
}

func publish(h http.Handler, topic string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/"+consts.ControllerVersionAPI+"/publish", strings.NewReader(`{"a":1}`))
	r.Header.Set(consts.HeaderProduceTopicName, topic)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	middlewares.RequestIdMiddleware(h).ServeHTTP(w, r)
	return w
}

func TestHealth(t *testing.T) {
	w := httptest.NewRecorder()
	Health(w, httptest.NewRequest(http.MethodGet, "/health", nil))
	assert.Equal(t, 200, w.Code)
	assert.JSONEq(t, `{"status":"running"}`, w.Body.String())
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}

func TestPublishHandler_Success(t *testing.T) {
	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(producer)
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"out"})

	w := publish(NewHandlers(newLogger(t), reg), "out")

	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), "success")
	assert.NotEmpty(t, w.Header().Get(consts.HeaderRequestIdKey), "request id is echoed")
}

func TestPublishHandler_UnknownTopic(t *testing.T) {
	w := publish(NewHandlers(newLogger(t), router.NewProducerRegistry()), "nope")

	assert.Equal(t, 200, w.Code, "documented behaviour: failure body with HTTP 200")
	assert.Contains(t, w.Body.String(), "no producer")
}

func TestResponsePublish_PublishesAndAcks(t *testing.T) {
	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(producer)
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"resp"})

	acked := make(chan struct{}, 2)
	ev := func(topic string) *schemas.ConsumerEvent {
		e := &schemas.ConsumerEvent{
			ResponseTopic: topic,
			Values: &schemas.ContextValues{
				RequestId: "r1",
				Body:      &schemas.RequestBody{Data: []byte(`{}`), Headers: map[string]string{}},
			},
		}
		e.SetAck(func() { acked <- struct{}{} })
		return e
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); ResponsePublish(ctx, newLogger(t), reg) }()

	transport.PublishChannel <- ev("resp")
	transport.PublishChannel <- ev("missing")

	for i := 0; i < 2; i++ {
		select {
		case <-acked:
		case <-time.After(3 * time.Second):
			t.Fatalf("event %d was not settled", i)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ResponsePublish did not stop on context cancel")
	}
}

func steppingLogger(t *testing.T) (*observability.FileTransactionLogger, string) {
	t.Helper()
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.SetClock(fakeclock.NewStepping(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond).Now)
	l.Run()
	l.RunAudit()
	return l, file
}

func TestProduceEventHasNonZeroDuration(t *testing.T) {
	l, file := steppingLogger(t)
	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(producer)
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"out"})

	publish(NewHandlers(l, reg), "out")
	l.Shutdown()

	got := metricsfile.Durations(t, file, consts.EventProduce)
	require.Len(t, got, 1)
	for _, ms := range got {
		assert.Positive(t, ms)
	}
}

func TestResponseServiceEventHasNonZeroDuration(t *testing.T) {
	l, file := steppingLogger(t)
	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(producer)
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"resp"})

	acked := make(chan struct{}, 1)
	e := &schemas.ConsumerEvent{
		ResponseTopic: "resp",
		Values: &schemas.ContextValues{
			RequestId: "rs-1",
			Body:      &schemas.RequestBody{Data: []byte(`{}`), Headers: map[string]string{}},
		},
	}
	e.SetAck(func() { acked <- struct{}{} })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); ResponsePublish(ctx, l, reg) }()
	transport.PublishChannel <- e
	select {
	case <-acked:
	case <-time.After(3 * time.Second):
		t.Fatal("response not settled")
	}
	cancel()
	<-done
	l.Shutdown()

	assert.Positive(t, metricsfile.Durations(t, file, consts.EventResponseService)["rs-1"])
}

func TestPublishHandler_UsesTheXKafkaKeyHeaderAsTheRecordKey(t *testing.T) {
	producer := mocksarama.NewSyncProducer(t)
	var gotKey string
	mocksarama.ExpectSendAndMatch(producer, func(m *sarama.ProducerMessage) bool {
		k, _ := m.Key.Encode()
		gotKey = string(k)
		return true
	})
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"out"})

	r := httptest.NewRequest(http.MethodPost, "/api/"+consts.ControllerVersionAPI+"/publish", strings.NewReader(`{"a":1}`))
	r.Header.Set(consts.HeaderProduceTopicName, "out")
	r.Header.Set(consts.HeaderKafkaKey, "user-42")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	middlewares.RequestIdMiddleware(NewHandlers(newLogger(t), reg)).ServeHTTP(w, r)

	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "user-42", gotKey)
}
