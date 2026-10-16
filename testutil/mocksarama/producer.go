// Package mocksarama wraps sarama's mock producers with assertion helpers and adds a hand-rolled
// consumer group (consumer_group.go), which sarama's mocks do not cover.
package mocksarama

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// NewSyncProducer returns a mock SyncProducer that reports unexpected sends through t
// (a nil t discards them).
func NewSyncProducer(t interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}) sarama.SyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return mocks.NewSyncProducer(&reporter{t: t}, cfg)
}

// NewAsyncProducer returns an AsyncProducer backed by the same mock infrastructure.
func NewAsyncProducer(t interface {
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}) sarama.AsyncProducer {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	return mocks.NewAsyncProducer(&reporter{t: t}, cfg)
}

// ExpectSendAndSucceed makes the next SendMessage succeed whatever the message;
// use ExpectSendAndMatch to check its contents.
func ExpectSendAndSucceed(p sarama.SyncProducer) {
	if mp, ok := p.(*mocks.SyncProducer); ok {
		mp.ExpectSendMessageAndSucceed()
		return
	}
	panic(fmt.Sprintf("ExpectSendAndSucceed: unsupported producer type %T", p))
}

// MessagePredicate reports whether an outgoing message is the expected one.
type MessagePredicate func(actual *sarama.ProducerMessage) bool

// ExpectSendAndMatch makes the next SendMessage succeed if match accepts the message,
// otherwise the mismatch is reported through the producer's t.
func ExpectSendAndMatch(p sarama.SyncProducer, match MessagePredicate) {
	if mp, ok := p.(*mocks.SyncProducer); ok {
		mp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
			if match(msg) {
				return nil
			}
			return fmt.Errorf("producer message did not match predicate")
		})
		return
	}
	panic(fmt.Sprintf("ExpectSendAndMatch: unsupported producer type %T", p))
}

// reporter adapts t to sarama's mocks.ErrorReporter (which only needs Errorf).
type reporter struct {
	t interface {
		Errorf(format string, args ...interface{})
	}
}

func (r *reporter) Errorf(format string, args ...interface{}) {
	if r.t == nil {
		return
	}
	r.t.Errorf(format, args...)
}
