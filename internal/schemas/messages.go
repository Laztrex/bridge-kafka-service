package schemas

import (
	"encoding/json"
)

// ContextValues holds the request context.
type ContextValues struct {
	RequestId string
	Body      *RequestBody
	Topic     string
	Path      string
}

type ConsumerEvent struct {
	Values        *ContextValues
	Event         *Event
	ResponseTopic string
	Config        *ConsumerConfig

	ack func()
}

// SetAck registers the callback that settles the originating Kafka message.
// The callback must be idempotent.
func (e *ConsumerEvent) SetAck(fn func()) { e.ack = fn }

// Ack settles the originating Kafka message (no-op when none is registered).
func (e *ConsumerEvent) Ack() {
	if e.ack != nil {
		e.ack()
	}
}

// TakeAck hands the ack callback over to another event, e.g. the response event
// that continues the processing chain, leaving this one without it.
func (e *ConsumerEvent) TakeAck() func() {
	fn := e.ack
	e.ack = nil
	return fn
}

// RequestBody contains a field with the result of the request and other information fields
type RequestBody struct {
	Data    json.RawMessage   `json:"data"`
	Headers map[string]string `json:"headers"`
}

type ResponseBody struct {
	Result interface{} `json:"result" default:""`
	Status string      `json:"status,omitempty"`
	Detail string      `json:"detail,omitempty"`
}

type Health struct {
	Status string `json:"status"`
}
