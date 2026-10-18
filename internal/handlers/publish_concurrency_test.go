package handlers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// funcProducer is a shared sync producer whose SendMessage is a test function, safe for the
// concurrent publishers of several pipelines.
type funcProducer struct {
	sarama.SyncProducer
	send func(*sarama.ProducerMessage) error
}

func (p *funcProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, p.send(msg)
}

func funcRegistry(send func(*sarama.ProducerMessage) error, topics ...string) *router.ProducerRegistry {
	reg := router.NewProducerRegistry()
	reg.Set(&funcProducer{send: send}, topics)
	return reg
}

// queuedEvents puts n events for topic into q, each counting its ack in acks and carrying
// its index as the body.
func queuedEvents(q chan *schemas.ConsumerEvent, topic string, retry bool, n int, acks *atomic.Int32) {
	for i := range n {
		ev, _ := newPublishEvent(topic, retry)
		ev.Values.Body.Data = []byte(fmt.Sprintf(`{"n":%d}`, i))
		ev.SetAck(func() { acks.Add(1) })
		q <- ev
	}
}

// runPublisher starts ResponsePublish for q and returns a function that cancels it and waits.
func runPublisher(t *testing.T, q chan *schemas.ConsumerEvent, workers int, reg *router.ProducerRegistry) func() {
	t.Helper()
	l, _ := publishLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); ResponsePublish(ctx, q, workers, l, reg) }()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("ResponsePublish did not stop")
		}
	}
	t.Cleanup(stop)
	return stop
}

func TestResponsePublish_StalledTopicHoldsBackOnlyItsPipeline(t *testing.T) {
	fastPublishBackoff(t, time.Millisecond, 5*time.Millisecond)
	reg := funcRegistry(func(msg *sarama.ProducerMessage) error {
		if msg.Topic == "broken" {
			return errPublishDown
		}
		return nil
	}, "broken", "healthy")

	broken, healthy := transport.NewResponseQueue(), transport.NewResponseQueue()
	var brokenAcks, healthyAcks atomic.Int32
	queuedEvents(broken, "broken", true, 5, &brokenAcks)
	runPublisher(t, broken, 2, reg)
	runPublisher(t, healthy, 2, reg)

	queuedEvents(healthy, "healthy", false, 50, &healthyAcks)
	require.Eventually(t, func() bool { return healthyAcks.Load() == 50 }, 3*time.Second, 5*time.Millisecond,
		"the healthy pipeline must publish while the other pipeline retries")
	assert.Zero(t, brokenAcks.Load(), "the broken pipeline's responses are still held for retry")
}

func TestResponsePublish_BusyPipelinePublishesConcurrently(t *testing.T) {
	release := make(chan struct{})
	var inFlight, maxInFlight atomic.Int32
	reg := funcRegistry(func(*sarama.ProducerMessage) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for m := maxInFlight.Load(); n > m && !maxInFlight.CompareAndSwap(m, n); m = maxInFlight.Load() {
		}
		<-release
		return nil
	}, "resp")

	q := transport.NewResponseQueue()
	var acks atomic.Int32
	queuedEvents(q, "resp", false, 20, &acks)
	runPublisher(t, q, transport.MaxPublishersPerPipeline, reg)
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	require.Eventually(t, func() bool { return inFlight.Load() == transport.MaxPublishersPerPipeline },
		3*time.Second, 5*time.Millisecond, "every publisher must be writing at once")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(transport.MaxPublishersPerPipeline), maxInFlight.Load(), "never more writes than publishers")
	assert.Zero(t, acks.Load())

	unblock()
	require.Eventually(t, func() bool { return acks.Load() == 20 }, 3*time.Second, 5*time.Millisecond)
}

func TestResponsePublish_SinglePublisherKeepsOrder(t *testing.T) {
	var mu sync.Mutex
	var got []string
	reg := funcRegistry(func(msg *sarama.ProducerMessage) error {
		b, err := msg.Value.Encode()
		require.NoError(t, err)
		mu.Lock()
		got = append(got, string(b))
		mu.Unlock()
		return nil
	}, "resp")

	q := transport.NewResponseQueue()
	var acks atomic.Int32
	queuedEvents(q, "resp", false, 3, &acks)
	runPublisher(t, q, 1, reg)

	require.Eventually(t, func() bool { return acks.Load() == 3 }, 3*time.Second, 5*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{`{"n":0}`, `{"n":1}`, `{"n":2}`}, got)
}

func TestResponsePublish_DrainWithWorkersGivesEveryEventOneAttempt(t *testing.T) {
	var sends atomic.Int32
	reg := funcRegistry(func(*sarama.ProducerMessage) error {
		sends.Add(1)
		return nil
	}, "resp")

	q := transport.NewResponseQueue()
	var acks atomic.Int32
	queuedEvents(q, "resp", false, 10, &acks)

	l, _ := publishLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { defer close(done); ResponsePublish(ctx, q, 4, l, reg) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ResponsePublish did not drain and return")
	}

	assert.Equal(t, int32(10), sends.Load(), "every queued event gets exactly one attempt")
	assert.Equal(t, int32(10), acks.Load())
	assert.Empty(t, q)
}
