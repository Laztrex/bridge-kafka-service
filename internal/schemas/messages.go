package schemas

import (
	"context"
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
	// ClaimCtx is the session context of the claim the record was consumed from (nil when the
	// event has none). Once it is done, the record belongs to the partition's next owner.
	ClaimCtx context.Context

	ack     func()
	abandon func()
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

// SetAbandon registers the callback that gives the originating Kafka message up without settling
// it, so it is redelivered. The callback must be idempotent.
func (e *ConsumerEvent) SetAbandon(fn func()) { e.abandon = fn }

// Abandon gives the originating Kafka message up unsettled (no-op when none is registered).
func (e *ConsumerEvent) Abandon() {
	if e.abandon != nil {
		e.abandon()
	}
}

// HandOver moves the settlement callbacks and the claim context to another event, e.g. the
// response event that continues the processing chain, leaving this one without them.
func (e *ConsumerEvent) HandOver(to *ConsumerEvent) {
	to.ack, to.abandon, to.ClaimCtx = e.ack, e.abandon, e.ClaimCtx
	e.ack, e.abandon = nil, nil
}

// ClaimEnded reports whether the record's claim session has ended.
func (e *ConsumerEvent) ClaimEnded() bool { return e.ClaimCtx != nil && e.ClaimCtx.Err() != nil }

// ClaimDone is closed when the record's claim session ends; nil (never ready) without a claim.
func (e *ConsumerEvent) ClaimDone() <-chan struct{} {
	if e.ClaimCtx == nil {
		return nil
	}
	return e.ClaimCtx.Done()
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
