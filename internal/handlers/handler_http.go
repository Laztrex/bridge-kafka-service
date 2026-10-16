package handlers

import (
	"context"
	"net/http"

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

// ResponsePublish drains transport.PublishChannel and publishes service
// responses to their response topics. It is independent of the HTTP publish
// endpoint and must run whenever consumers are active, even with no producers
// configured (events are then failed and settled instead of piling up).
// When ctx is cancelled it publishes what is already queued and returns.
func ResponsePublish(
	ctx context.Context,
	logTransaction schemas.TransactionLogger,
	producers ProducerLookup,
) {
	for {
		select {
		case msg := <-transport.PublishChannel:
			publishOne(logTransaction, producers, msg)
		case <-ctx.Done():
			for {
				select {
				case msg := <-transport.PublishChannel:
					publishOne(logTransaction, producers, msg)
				default:
					return
				}
			}
		}
	}
}

func publishOne(logTransaction schemas.TransactionLogger, producers ProducerLookup, msg *schemas.ConsumerEvent) {
	msg.Event = &schemas.Event{
		EventType: consts.EventResponseService,
		RequestId: msg.Values.RequestId,
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
