package codec

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// coerceValue turns any record value into a valid JSON payload. It never
// fails: exactly one rule applies (empty object, verbatim JSON, recovered
// escapes, UTF-8 string, base64 string).
func coerceValue(value []byte) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("{}")
	}
	if json.Valid(value) {
		return value
	}
	if recovered, err := UnescapeUnicodeCharactersInJSON(value); err == nil && json.Valid(recovered) {
		return recovered
	}
	if !utf8.Valid(value) {
		value = []byte(base64.StdEncoding.EncodeToString(value))
	}
	quoted, _ := json.Marshal(string(value))
	return quoted
}

// GetConsumeMsg extracts the body and headers from a consumed Kafka record, coercing non-JSON
// payloads into a canonical JSON shape, and returns the record's request id. Absent x-request-id
// and x-client-id get defaults unless listed in consts.RequiredHeaders; a request id is generated
// for logs and audit either way.
func GetConsumeMsg(msg *sarama.ConsumerMessage, body *schemas.RequestBody) string {
	body.Data = coerceValue(msg.Value)

	headers := map[string]string{}

	for _, v := range msg.Headers {
		key := string(v.Key)
		value := string(v.Value)

		if IsBinaryData(value) {
			encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
			headers[key] = encodedValue
			log.Debug().Str("header", key).Msg("Binary header encoded to base64")
		} else {
			headers[key] = value
		}
	}

	normalizeTrackingHeaders(headers)

	requestId, ok := headers[consts.HeaderRequestIdKey]
	if !ok {
		requestId = NewRequestID()
		if !consts.IsRequiredHeader(consts.HeaderRequestIdKey) {
			headers[consts.HeaderRequestIdKey] = requestId
		}
	}
	if _, ok = headers[consts.HeaderClientId]; !ok && !consts.IsRequiredHeader(consts.HeaderClientId) {
		headers[consts.HeaderClientId] = SystemID()
	}

	log.Info().Str(consts.HeaderRequestIdKey, requestId).
		Msgf("message claimed: timestamp = %v, topic = %s", msg.Timestamp, msg.Topic)

	log.Debug().Str(consts.HeaderRequestIdKey, requestId).
		Msgf("message claimed: {\n	body : %s, \n	headers : %+v, \n	timestamp : %v, \n	topic : %s\n}", string(msg.Value), headers, msg.Timestamp, msg.Topic)

	body.Headers = headers
	return requestId
}

// trackingHeaders are the tracking headers a record may carry in any letter case.
var trackingHeaders = []string{
	consts.HeaderRequestIdKey, consts.HeaderRequestTimeKey, consts.HeaderTraceIdKey, consts.HeaderClientId,
}

// normalizeTrackingHeaders stores every tracking header under its lowercase wire name, whatever case
// the record used. When the record carries several spellings, the lowercase one wins, otherwise the
// first in byte order; the others are removed.
func normalizeTrackingHeaders(headers map[string]string) {
	for _, name := range trackingHeaders {
		var variants []string
		for k := range headers {
			if k != name && strings.EqualFold(k, name) {
				variants = append(variants, k)
			}
		}
		if len(variants) == 0 {
			continue
		}
		slices.Sort(variants)
		if _, ok := headers[name]; !ok {
			headers[name] = headers[variants[0]]
		}
		for _, k := range variants {
			delete(headers, k)
		}
	}
}

// IsBinaryData reports whether the string contains any non-printable byte.
func IsBinaryData(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// PrepareConsumeMsg parses a consumed Kafka message into a ConsumerEvent with
// normalised payload and tracking headers attached.
func PrepareConsumeMsg(msg *sarama.ConsumerMessage, event *schemas.Event, body *schemas.RequestBody) *schemas.ConsumerEvent {
	event.SetSuccess(GetConsumeMsg(msg, body))

	return &schemas.ConsumerEvent{
		Values: &schemas.ContextValues{Body: body, RequestId: event.RequestId},
	}
}

// PrepareProduceMsg builds a sarama producer message carrying the supplied
// payload and headers, with the partition key from HeaderKafkaKey (matched case-insensitively).
func PrepareProduceMsg(topic, message string, headers map[string]string) *sarama.ProducerMessage {
	var saramaHeaders []sarama.RecordHeader

	for k, v := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	var key string
	for k, v := range headers {
		if strings.EqualFold(k, consts.HeaderKafkaKey) {
			key = v
			break
		}
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Key:     sarama.StringEncoder(key),
		Headers: saramaHeaders,
	}

	return msg
}

// UnescapeUnicodeCharactersInJSON recovers Unicode (\uXXXX, \xXX) escapes that
// some producers emit in place of raw characters. Returns original bytes on
// failure so callers can fall through to other coercion paths.
func UnescapeUnicodeCharactersInJSON(_jsonRaw json.RawMessage) (json.RawMessage, error) {
	if len(_jsonRaw) == 0 {
		return _jsonRaw, nil
	}

	str, err := strconv.Unquote(
		strings.ReplaceAll(strconv.Quote(string(_jsonRaw)), `\\u`, `\u`),
	)
	if err != nil {
		return nil, err
	}
	str, err = strconv.Unquote(
		strings.ReplaceAll(strconv.Quote(str), `\\x`, `\x`),
	)
	if err != nil {
		return nil, err
	}

	return []byte(str), nil
}
