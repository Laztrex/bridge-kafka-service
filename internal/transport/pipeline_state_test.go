package transport_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

func TestPipelineState_CountsAndOldestRetry(t *testing.T) {
	s := transport.NewPipelineState()
	a, b, c := &schemas.ConsumerEvent{}, &schemas.ConsumerEvent{}, &schemas.ConsumerEvent{}

	assert.Equal(t, transport.PipelineSnapshot{}, s.Snapshot(), "a fresh pipeline reports nothing")

	s.CallRetrying(a, time.Now().Add(-3*time.Minute))
	s.CallRetrying(b, time.Now().Add(-time.Minute))
	s.PublicationRetrying(c, time.Now().Add(-2*time.Minute))
	s.AddUncommitted(41)

	got := s.Snapshot()
	assert.Equal(t, 2, got.RetryingCalls)
	assert.Equal(t, 1, got.RetryingPublications)
	assert.InDelta(t, (3 * time.Minute).Seconds(), got.OldestRetry.Seconds(), 1)
	assert.Equal(t, int64(41), got.Uncommitted)

	s.CallDone(a)
	assert.InDelta(t, (2 * time.Minute).Seconds(), s.Snapshot().OldestRetry.Seconds(), 1, "the oldest is recomputed")
	s.CallDone(b)
	s.PublicationDone(c)
	s.AddUncommitted(-41)
	assert.Equal(t, transport.PipelineSnapshot{}, s.Snapshot())
}

func TestPipelineState_NilIsANoOp(t *testing.T) {
	var s *transport.PipelineState
	e := &schemas.ConsumerEvent{}

	s.CallRetrying(e, time.Now())
	s.CallDone(e)
	s.PublicationRetrying(e, time.Now())
	s.PublicationDone(e)
	s.AddUncommitted(1)

	assert.Equal(t, transport.PipelineSnapshot{}, s.Snapshot())
}

func TestPipelineState_ConcurrentUse(t *testing.T) {
	s := transport.NewPipelineState()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := &schemas.ConsumerEvent{}
			s.CallRetrying(e, time.Now())
			s.PublicationRetrying(e, time.Now())
			s.AddUncommitted(1)
			_ = s.Snapshot()
			s.CallDone(e)
			s.PublicationDone(e)
			s.AddUncommitted(-1)
		}()
	}
	wg.Wait()

	assert.Equal(t, transport.PipelineSnapshot{}, s.Snapshot())
}

func TestPipelineState_RetriedCallIsListedUntilItSucceeds(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	state := transport.NewPipelineState()
	var during transport.PipelineSnapshot
	up := &scriptedUpstream{fail: func(call int32) bool {
		if call == 3 {
			during = state.Snapshot()
		}
		return call <= 2
	}}
	ev, _ := newEvent("")

	transport.SendToServiceWithState(context.Background(), logger(t), up, ev, nil, time.Second, transport.NewRetryState(), state)

	assert.Equal(t, 1, during.RetryingCalls, "listed while it retries")
	assert.Equal(t, transport.PipelineSnapshot{}, state.Snapshot(), "removed once it succeeds")
}

func TestPipelineState_AbandonedRetryIsRemoved(t *testing.T) {
	transport.FastRetries(t, time.Hour, time.Hour)
	state := transport.NewPipelineState()
	up := &scriptedUpstream{fail: func(int32) bool { return true }}
	ev, _ := newEvent("")
	abandon, giveUp := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		transport.SendToServiceWithState(abandon, logger(t), up, ev, nil, time.Second, transport.NewRetryState(), state)
	}()
	require.Eventually(t, func() bool { return state.Snapshot().RetryingCalls == 1 }, time.Second, 5*time.Millisecond)
	giveUp()
	<-done

	assert.Equal(t, transport.PipelineSnapshot{}, state.Snapshot())
}
