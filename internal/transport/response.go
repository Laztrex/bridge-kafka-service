package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/middlewares"
	"bridge-kafka-service/internal/schemas"
)

// ProduceResponse writes the HTTP API reply of the publish endpoint.
func ProduceResponse(msg string, w http.ResponseWriter) {
	queryResponse := &schemas.ResponseBody{Result: msg}
	err := json.NewEncoder(w).Encode(queryResponse)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// errPublishAbandoned: the pipeline stopped while a response waited for space in
// the publish queue; the record stays unsettled and is redelivered.
var errPublishAbandoned = errors.New("response publication abandoned on shutdown")

// trackingHeaders are written under their lowercase wire names, never in
// net/http's canonical form.
var trackingHeaders = []string{
	consts.HeaderRequestIdKey, consts.HeaderRequestTimeKey, consts.HeaderTraceIdKey, consts.HeaderClientId,
}

// publishUpstreamResponse queues the response event in the pipeline's queue for the response
// topic (if any) and hands it the ack. The event carries the stanza config, so the response publisher can apply its
// failure policy. On a full queue the response is dropped, unless wait is set (on_failure: retry):
// then it waits until abandon is cancelled and returns errPublishAbandoned, keeping the ack.
// Tracking headers are always set under their wire names, even if the upstream did not echo them.
func publishUpstreamResponse(abandon context.Context, msg *schemas.ConsumerEvent, resp UpstreamResponse, queue chan<- *schemas.ConsumerEvent, wait bool) error {
	if msg.ResponseTopic == "" {
		return nil
	}

	header := resp.Header.Clone()
	if header == nil {
		header = http.Header{}
	}
	if header.Get(consts.HeaderRequestIdKey) == "" {
		header.Set(consts.HeaderRequestIdKey, msg.Values.RequestId)
	}
	clientId := header.Get(consts.HeaderClientId)
	if clientId == "" {
		clientId = msg.Values.Body.Headers[consts.HeaderClientId]
	}

	publishMsg := &schemas.RequestBody{Data: responseData(resp.Body), Headers: make(map[string]string)}
	for k, v := range header {
		if len(v) > 0 {
			publishMsg.Headers[k] = v[0]
		}
	}
	for _, k := range trackingHeaders {
		delete(publishMsg.Headers, http.CanonicalHeaderKey(k))
	}
	if clientId != "" {
		publishMsg.Headers[consts.HeaderClientId] = clientId
	}

	if err := middlewares.HeaderHttpHandler(header, publishMsg); err != nil {
		log.Error().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).
			Err(err).Msg("error processing response headers")
		return err
	}

	publishEvent := &schemas.ConsumerEvent{
		Values: &schemas.ContextValues{
			Body:      publishMsg,
			RequestId: publishMsg.Headers[consts.HeaderRequestIdKey],
			Path:      msg.Values.Path,
		},
		Event:         msg.Event,
		ResponseTopic: msg.ResponseTopic,
		Config:        msg.Config,
	}
	publishEvent.SetAck(msg.TakeAck())

	if wait {
		select {
		case queue <- publishEvent:
			log.Debug().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).
				Msg("response event sent to publish queue")
		case <-abandon.Done():
			msg.SetAck(publishEvent.TakeAck())
			return errPublishAbandoned
		}
		return nil
	}

	select {
	case queue <- publishEvent:
		log.Debug().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).
			Msg("response event sent to publish queue")
	default:
		log.Warn().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).
			Msg("publish queue is full, dropping response event")
		publishEvent.Ack()
	}
	return nil
}

// responseData normalises an upstream body: empty stays empty (nil), valid JSON
// is passed through, anything else is stored as a JSON string.
func responseData(body []byte) json.RawMessage {
	switch {
	case len(body) == 0:
		return nil
	case json.Valid(body):
		return body
	default:
		quoted, _ := json.Marshal(string(body))
		return quoted
	}
}
