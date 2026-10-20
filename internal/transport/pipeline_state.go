package transport

import (
	"sync"
	"sync/atomic"
	"time"

	"bridge-kafka-service/internal/schemas"
)

// PipelineState is the runtime state of one pipeline that /status reports: the records retrying
// their upstream call, the responses retrying their publication, each with the time of its first
// failure, and the records handed out by current claims and not yet committed. Every method is safe
// for concurrent use and does nothing on a nil *PipelineState.
type PipelineState struct {
	mu           sync.Mutex
	calls        map[*schemas.ConsumerEvent]time.Time
	publications map[*schemas.ConsumerEvent]time.Time
	uncommitted  atomic.Int64
}

// PipelineSnapshot is a point-in-time copy of a PipelineState.
type PipelineSnapshot struct {
	RetryingCalls        int
	RetryingPublications int
	OldestRetry          time.Duration
	Uncommitted          int64
}

// NewPipelineState returns the empty state of one pipeline.
func NewPipelineState() *PipelineState {
	return &PipelineState{
		calls:        map[*schemas.ConsumerEvent]time.Time{},
		publications: map[*schemas.ConsumerEvent]time.Time{},
	}
}

// CallRetrying records that msg's upstream call has been failing since since.
func (s *PipelineState) CallRetrying(msg *schemas.ConsumerEvent, since time.Time) {
	s.set(func() { s.calls[msg] = since })
}

// CallDone removes msg from the retrying calls, whatever its outcome.
func (s *PipelineState) CallDone(msg *schemas.ConsumerEvent) {
	s.set(func() { delete(s.calls, msg) })
}

// PublicationRetrying records that msg's response publication has been failing since since.
func (s *PipelineState) PublicationRetrying(msg *schemas.ConsumerEvent, since time.Time) {
	s.set(func() { s.publications[msg] = since })
}

// PublicationDone removes msg from the retrying publications, whatever its outcome.
func (s *PipelineState) PublicationDone(msg *schemas.ConsumerEvent) {
	s.set(func() { delete(s.publications, msg) })
}

// AddUncommitted changes the number of records handed out and not yet committed by n.
func (s *PipelineState) AddUncommitted(n int64) {
	if s != nil {
		s.uncommitted.Add(n)
	}
}

// Snapshot returns the current counts and the age of the oldest retry (zero when none).
func (s *PipelineState) Snapshot() PipelineSnapshot {
	if s == nil {
		return PipelineSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := PipelineSnapshot{
		RetryingCalls:        len(s.calls),
		RetryingPublications: len(s.publications),
		Uncommitted:          s.uncommitted.Load(),
	}
	var oldest time.Time
	for _, m := range []map[*schemas.ConsumerEvent]time.Time{s.calls, s.publications} {
		for _, since := range m {
			if oldest.IsZero() || since.Before(oldest) {
				oldest = since
			}
		}
	}
	if !oldest.IsZero() {
		out.OldestRetry = time.Since(oldest)
	}
	return out
}

func (s *PipelineState) set(change func()) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	change()
}
