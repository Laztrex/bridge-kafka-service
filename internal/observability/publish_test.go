package observability

import (
	"net/http/httptest"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/mocksarama"
)

func TestPublish_RecordsFailureAndSuccess(t *testing.T) {
	l, file := newTestLogger(t)
	body := &schemas.RequestBody{Data: []byte(`{}`), Headers: map[string]string{}}
	ctxv := &schemas.ContextValues{RequestId: "r1", Body: body, Path: "/p"}

	ok := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(ok)
	ev := &schemas.Event{EventType: consts.EventProduce}
	l.Publish(ok, "out", ctxv, ev, nil)
	assert.Equal(t, 1, ev.Success)

	bad := mocksarama.NewSyncProducer(t)
	bad.(*mocks.SyncProducer).ExpectSendMessageAndFail(assert.AnError)
	ev2 := &schemas.Event{EventType: consts.EventProduce}
	l.Publish(bad, "out", ctxv, ev2, nil)
	assert.Equal(t, 1, ev2.Failure)

	l.Shutdown()
	metrics, _ := os.ReadFile(file)
	assert.Contains(t, string(metrics), "produce_failure_total")
	audit, _ := os.ReadFile(file + ".audit")
	assert.Contains(t, string(audit), "envoy-egress-als")
}

func TestPublish_WritesHTTPResultWhenWriterGiven(t *testing.T) {
	l, _ := newTestLogger(t)
	body := &schemas.RequestBody{Data: []byte(`{}`), Headers: map[string]string{}}
	ctxv := &schemas.ContextValues{RequestId: "r1", Body: body, Path: "/p"}

	ok := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(ok)
	w := httptest.NewRecorder()
	l.Publish(ok, "out", ctxv, &schemas.Event{EventType: consts.EventProduce}, w)
	assert.JSONEq(t, `{"result":"success"}`, w.Body.String())

	bad := mocksarama.NewSyncProducer(t)
	bad.(*mocks.SyncProducer).ExpectSendMessageAndFail(assert.AnError)
	w = httptest.NewRecorder()
	l.Publish(bad, "out", ctxv, &schemas.Event{EventType: consts.EventProduce}, w)
	assert.JSONEq(t, `{"result":"failure"}`, w.Body.String())
}

func TestSend_PublishesMarshalledBodyWithKeyAndHeaders(t *testing.T) {
	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndMatch(producer, func(m *sarama.ProducerMessage) bool {
		val, _ := m.Value.Encode()
		key, _ := m.Key.Encode()
		return m.Topic == "out" && string(val) == `{"a":1}` && string(key) == "k1" && len(m.Headers) == 2
	})

	err := Send(producer, "out", &schemas.RequestBody{
		Data:    []byte(`{"a":1}`),
		Headers: map[string]string{consts.HeaderKafkaKey: "k1", consts.HeaderRequestIdKey: "r1"},
	}, "r1")
	assert.NoError(t, err)
}
