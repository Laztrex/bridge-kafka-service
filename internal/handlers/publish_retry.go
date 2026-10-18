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
// (design D1): a transient send failure holds the offset while the write is
// repeated with the shared retry backoff until it succeeds, the error turns out
// non-transient, or ctx ends (then the record is redelivered and no outcome is
// recorded). A cancelled ctx turns the loop into a single attempt, so the
// post-cancellation drain adds no new waits. Metrics and audit record exactly
// one final outcome; the upstream is not called again.
func publishOneRetrying(ctx context.Context, logTransaction schemas.TransactionLogger, producers ProducerLookup, msg *schemas.ConsumerEvent) {
	logTransaction.Start(msg.Event)

	producer, ok := producers.Lookup(msg.ResponseTopic)
	if !ok {
		// Startup validation makes this unreachable for retry stanzas; kept as a
		// defensive fallback that settles the record (design D6).
		err := fmt.Errorf("no producer for topic: %s", msg.ResponseTopic)
		log.Error().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).Err(err).Msg("response publish failed")
		logTransaction.RecordPublishOutcome(msg.Values, msg.Event, msg.Event.Started, err)
		logTransaction.Close(msg.Event)
		msg.Ack()
		return
	}

	requestId := msg.Values.RequestId
	for attempt := 1; ; attempt++ {
		err := observability.Send(producer, msg.ResponseTopic, msg.Values.Body, requestId)
		if err == nil || isNonTransientPublishError(err) {
			logTransaction.RecordPublishOutcome(msg.Values, msg.Event, msg.Event.Started, err)
			logTransaction.Close(msg.Event)
			msg.Ack()
			return
		}

		wait := transport.RetryBackoff(attempt)
		log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).
			Dur("retry_in", wait).Err(err).Msg("response publish failed, retrying")
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("response publication abandoned on shutdown, the record will be redelivered")
			return
		}
	}
}

// isNonTransientPublishError reports whether a response-publication error can
// never succeed on retry, so the record is settled as a failure like a poison
// record instead of holding its offset. Everything else (out of brokers, leader
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
