package observability

import (
	"encoding/json"
	"net/http"

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
// (w nil).
func (l *FileTransactionLogger) Publish(
	producer sarama.SyncProducer,
	topic string,
	query *schemas.ContextValues,
	event *schemas.Event,
	w http.ResponseWriter,
) {
	start := l.now()
	defer l.Close(event)
	var msg string

	err := Send(producer, topic, query.Body, query.RequestId)
	if err != nil {
		event.SetFailure(query.RequestId)
		msg = "failure"
	} else {
		event.SetSuccess(query.RequestId)
		msg = "success"
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

	if w != nil {
		transport.ProduceResponse(msg, w)
	}
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
