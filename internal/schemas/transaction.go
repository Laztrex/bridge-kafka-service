package schemas

import (
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	accesslog "github.com/envoyproxy/go-control-plane/envoy/service/accesslog/v3"

	"bridge-kafka-service/internal/consts"
)

type TransactionLogger interface {
	Run()
	RunAudit()
	Close(*Event)
	CloseAudit(*accesslog.StreamAccessLogsMessage)
	Start(*Event)
	Publish(sarama.SyncProducer, string, *ContextValues, *Event, http.ResponseWriter)
	// RecordPublishOutcome records the single outcome of a publication on event
	// and emits its egress audit entry; publication retries call Send per
	// attempt and this exactly once (see FileTransactionLogger.RecordPublishOutcome).
	RecordPublishOutcome(*ContextValues, *Event, time.Time, error)
	CloseAll()
}

type FileTransactionLogger struct {
	File   *os.File
	Events chan Event
}

type Event struct {
	EventType consts.EventType
	RequestId string
	Status    string
	Success   int
	Failure   int
	// Started is stamped by TransactionLogger.Start; Duration is set by Close
	// (Close - Started, or 0 for an event that was never started).
	Started  time.Time
	Duration time.Duration
}

// SetSuccess marks the event as successful.
func (e *Event) SetSuccess(requestId string) {
	e.Success = 1
	e.Failure = 0
	e.Status = "success"
	e.RequestId = requestId
}

// SetFailure marks the event as failed.
func (e *Event) SetFailure(requestId string) {
	e.Success = 0
	e.Failure = 1
	e.Status = "failure"
	e.RequestId = requestId
}
