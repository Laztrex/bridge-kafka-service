package handlers

import (
	"context"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// ProducerLookup resolves the producer allowed to publish to a topic.
type ProducerLookup interface {
	Lookup(topic string) (sarama.SyncProducer, bool)
}

// NewHandlers returns the publish handler: it forwards the request prepared by
// the request-id middleware to the producer registered for the target topic.
// Routing (path and POST-only) is the caller's concern.
func NewHandlers(
	logTransaction schemas.TransactionLogger,
	producers ProducerLookup,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, ok := r.Context().Value(consts.PublishContextKey).(schemas.ContextValues)
		if !ok {
			http.Error(w, "request context is missing", http.StatusInternalServerError)
			return
		}

		event := &schemas.Event{
			EventType: consts.EventProduce,
			RequestId: query.RequestId,
			Status:    "success",
		}
		logTransaction.Start(event)
		if producer, ok := producers.Lookup(query.Topic); ok {
			logTransaction.Publish(producer, query.Topic, &query, event, w)
		} else {
			event.SetFailure(query.RequestId)
			logTransaction.Close(event)
			transport.ProduceResponse("failure: no producer", w)
		}
	})
}

// ResponsePublish drains one pipeline's response queue with workers concurrent publishers and
// publishes service responses to their response topic; it returns once every worker has
// returned. Each pipeline has its own queue, so a response topic that is slow or rejects writes
// holds back only that pipeline. It must run whenever its pipeline is active, even with no
// producers configured (events are then failed and settled instead of piling up).
// Responses of on_failure: retry stanzas are retried with backoff until they
// are written (baseline decision PUB-1); after ctx is cancelled the drain gives every queued
// event a single attempt, so no new waits are added on shutdown.
func ResponsePublish(
	ctx context.Context,
	queue <-chan *schemas.ConsumerEvent,
	workers int,
	logTransaction schemas.TransactionLogger,
	producers ProducerLookup,
	state *transport.PipelineState,
) {
	var wg sync.WaitGroup
	for range max(workers, 1) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			publishFrom(ctx, queue, logTransaction, producers, state)
		}()
	}
	wg.Wait()
}

// publishFrom is one publisher of a queue: it publishes events while ctx is alive, then gives
// every event still queued one attempt and returns when the queue is empty.
func publishFrom(ctx context.Context, queue <-chan *schemas.ConsumerEvent, logTransaction schemas.TransactionLogger, producers ProducerLookup, state *transport.PipelineState) {
	for {
		select {
		case msg := <-queue:
			publishOne(ctx, logTransaction, producers, msg, state)
		case <-ctx.Done():
			for {
				select {
				case msg := <-queue:
					publishOne(ctx, logTransaction, producers, msg, state)
				default:
					return
				}
			}
		}
	}
}

func publishOne(ctx context.Context, logTransaction schemas.TransactionLogger, producers ProducerLookup, msg *schemas.ConsumerEvent, state *transport.PipelineState) {
	msg.Event = &schemas.Event{
		EventType: consts.EventResponseService,
		RequestId: msg.Values.RequestId,
	}

	if msg.Config != nil && msg.Config.EffectiveFailurePolicy() == schemas.FailureRetry {
		publishOneRetrying(ctx, logTransaction, producers, msg, state)
		return
	}

	logTransaction.Start(msg.Event)
	if producer, ok := producers.Lookup(msg.ResponseTopic); ok {
		logTransaction.Publish(producer, msg.ResponseTopic, msg.Values, msg.Event, nil)
	} else {
		msg.Event.SetFailure(msg.Event.RequestId)
		log.Error().Str(consts.HeaderRequestIdKey, msg.Event.RequestId).
			Msgf("no producer for topic: %s", msg.ResponseTopic)
		logTransaction.Close(msg.Event)
	}
	msg.Ack()
}
