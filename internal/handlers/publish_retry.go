package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// publishOneRetrying publishes the response of an on_failure: retry stanza
// (baseline decision PUB-1): a transient send failure holds the offset while the write is
// repeated with the shared retry backoff until it succeeds, the error turns out
// non-transient, ctx ends or the record's claim ends (then the record is abandoned for
// redelivery and no outcome is recorded). A cancelled ctx turns the loop into a single attempt, so the
// post-cancellation drain adds no new waits. Metrics and audit record exactly
// one final outcome; the upstream is not called again. A retried response is listed in state until
// the loop ends.
func publishOneRetrying(ctx context.Context, logTransaction schemas.TransactionLogger, producers ProducerLookup, msg *schemas.ConsumerEvent, state *transport.PipelineState) {
	logTransaction.Start(msg.Event)
	defer state.PublicationDone(msg)

	producer, ok := producers.Lookup(msg.ResponseTopic)
	if !ok {
		// Startup validation makes this unreachable for retry stanzas; kept as a
		// defensive fallback that settles the record (baseline decision PUB-6).
		err := fmt.Errorf("no producer for topic: %s", msg.ResponseTopic)
		log.Error().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).Err(err).Msg("response publish failed")
		logTransaction.RecordPublishOutcome(msg.Values, msg.Event, msg.Event.Started, err)
		logTransaction.Close(msg.Event)
		msg.Ack()
		return
	}

	requestId := msg.Values.RequestId
	var firstFailure time.Time
	for attempt := 1; ; attempt++ {
		err := observability.Send(producer, msg.ResponseTopic, msg.Values.Body, requestId)
		if err == nil || isNonTransientPublishError(err) {
			logTransaction.RecordPublishOutcome(msg.Values, msg.Event, msg.Event.Started, err)
			logTransaction.Close(msg.Event)
			msg.Ack()
			return
		}

		if attempt == 1 {
			firstFailure = time.Now()
			state.PublicationRetrying(msg, firstFailure)
		}
		wait := transport.RetryBackoff(attempt)
		log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).
			Dur("failing_for", time.Since(firstFailure)).Dur("retry_in", wait).Err(err).
			Msg("response publish failed, retrying")
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-msg.ClaimDone():
			timer.Stop()
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("claim ended during the publication retry, the record will be redelivered to the partition's next owner")
			msg.Abandon()
			return
		case <-ctx.Done():
			timer.Stop()
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("response publication abandoned on shutdown, the record will be redelivered")
			msg.Abandon()
			return
		}
	}
}

// isNonTransientPublishError reports whether a response-publication error can
// never succeed on retry, so the record is settled as a failure like a poison
// record instead of holding its offset: marshal errors, client-side refusals and
// broker rejections of the record itself. Everything else (out of brokers, leader
// changes, timeouts, authorization) is transient: fixed by the broker or the
// configuration, so the write is repeated.
func isNonTransientPublishError(err error) bool {
	if err == nil {
		return false
	}

	// The body cannot be marshalled: repeating the send cannot fix it.
	var marshalErr *json.MarshalerError
	if errors.As(err, &marshalErr) {
		return true
	}

	// The client refused the record before sending it. In sarama v1.60.2 a send returns this only
	// for a record larger than Producer.MaxMessageBytes and for headers on Kafka older than 0.11:
	// both depend on the configuration and the record alone, so repeating can never succeed.
	var cfgErr sarama.ConfigurationError
	if errors.As(err, &cfgErr) {
		return true
	}

	// A batch of produce errors (Close / SendMessages) is non-transient only
	// when every wrapped error is; one transient entry keeps the writes going.
	var batch sarama.ProducerErrors
	if errors.As(err, &batch) {
		for _, pe := range batch {
			if !isNonTransientPublishError(pe) {
				return false
			}
		}
		return len(batch) > 0
	}

	// Sarama returns KError values (SendMessage unwraps its ProducerError);
	// errors.Is also reaches them through ProducerError.Unwrap.
	switch {
	case errors.Is(err, sarama.ErrMessageSizeTooLarge),
		errors.Is(err, sarama.ErrInvalidMessage),
		errors.Is(err, sarama.ErrInvalidMessageSize):
		return true
	}
	return false
}
