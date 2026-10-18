package observability

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/codec"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// Publish sends a record to topic, records the outcome on event, emits the ALS
// egress audit entry and, when w is non-nil, writes the HTTP API reply. It
// serves both the HTTP publish endpoint (w set) and the response publisher
// (w nil) with a single attempt.
func (l *FileTransactionLogger) Publish(
	producer sarama.SyncProducer,
	topic string,
	query *schemas.ContextValues,
	event *schemas.Event,
	w http.ResponseWriter,
) {
	start := l.now()
	defer l.Close(event)

	err := Send(producer, topic, query.Body, query.RequestId)
	l.RecordPublishOutcome(query, event, start, err)

	if w != nil {
		msg := "failure"
		if err == nil {
			msg = "success"
		}
		transport.ProduceResponse(msg, w)
	}
}

// RecordPublishOutcome records the single outcome of a publication on event
// (success or failure, by err) and emits the egress ALS audit entry measured
// from start. A publisher that retries its sends calls Send per attempt and
// this exactly once, so metrics and audit describe the whole publication, not
// each attempt.
func (l *FileTransactionLogger) RecordPublishOutcome(
	query *schemas.ContextValues,
	event *schemas.Event,
	start time.Time,
	err error,
) {
	if err != nil {
		event.SetFailure(query.RequestId)
	} else {
		event.SetSuccess(query.RequestId)
	}

	LogAuditEventWithLatency(
		l,
		consts.EventNameBusinessRequestTransferred,
		query.RequestId,
		query.Body,
		consts.AccessLogTypeDownstreamEnd,
		query.Path,
		start,
	)
}

// Send marshals a request body to JSON and dispatches it via the supplied
// SyncProducer, attaching partitioned headers prepared through codec helpers.
func Send(
	producer sarama.SyncProducer,
	topic string,
	data *schemas.RequestBody,
	requestId string,
) error {
	b, err := json.Marshal(data.Data)
	if err != nil {
		return err
	}

	strData := string(b)
	msg := codec.PrepareProduceMsg(topic, strData, data.Headers)

	p, o, err := producer.SendMessage(msg)
	if err != nil {
		log.Error().Str(consts.HeaderRequestIdKey, requestId).Msgf("error send: %v", err)
		return err
	}

	log.Info().Str(consts.HeaderRequestIdKey, requestId).
		Msgf("Published to %s. Partition: %d, offset: %d", topic, p, o)

	return nil
}
