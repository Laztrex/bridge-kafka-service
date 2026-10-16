package transport_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/metricsfile"
	"bridge-kafka-service/testutil/mockmodel"
)

// blockingUpstream answers after `after`, or fails with the context's error if
// the call is cancelled first (what a real transport does).
type blockingUpstream struct {
	after   time.Duration
	started chan struct{}
}

func newBlocking(after time.Duration) *blockingUpstream {
	return &blockingUpstream{after: after, started: make(chan struct{}, 16)}
}

func (b *blockingUpstream) Call(ctx context.Context, _ transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	b.started <- struct{}{}
	select {
	case <-time.After(b.after):
		return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
	case <-ctx.Done():
		return transport.UpstreamResponse{}, ctx.Err()
	}
}

// pipeline runs ConsumerResponseFast over one event and stops it the way
// RunSubscription does on shutdown: by closing the input channel.
type pipeline struct {
	ev    *schemas.ConsumerEvent
	acks  atomic.Int32
	ch    chan *schemas.ConsumerEvent
	done  chan struct{}
	file  string
	ended time.Time
}

func startPipeline(t *testing.T, upstream transport.Upstream, conf *schemas.ConsumerConfig, drain time.Duration) *pipeline {
	conf.DrainWindow = drain
	t.Helper()
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.Run()
	t.Cleanup(l.Shutdown)

	p := &pipeline{ch: make(chan *schemas.ConsumerEvent, 1), done: make(chan struct{}), file: file}
	p.ev, _ = newEvent("")
	p.ev.SetAck(func() { p.acks.Add(1) })

	go func() {
		defer close(p.done)
		transport.ConsumerResponseFast(l, conf, upstream, p.ch, context.Background())
		p.ended = time.Now()
	}()
	p.ch <- p.ev
	return p
}

// stop closes the input and waits for the pipeline to return, reporting how long it took.
func (p *pipeline) stop(t *testing.T, within time.Duration) time.Duration {
	t.Helper()
	began := time.Now()
	close(p.ch)
	select {
	case <-p.done:
	case <-time.After(within):
		t.Fatalf("pipeline did not stop within %v", within)
	}
	return p.ended.Sub(began)
}

func goroutinesSettleTo(t *testing.T, baseline int) {
	t.Helper()
	assert.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+2 }, 5*time.Second, 50*time.Millisecond,
		"goroutines leaked: %d now, %d before", runtime.NumGoroutine(), baseline)
}

func TestLifecycle_CallFinishingInsideTheDrainWindowIsSettledNormally(t *testing.T) {
	up := newBlocking(200 * time.Millisecond)
	p := startPipeline(t, up, &schemas.ConsumerConfig{Service: schemas.HostConfig{Timeout: 30 * time.Second}}, 5*time.Second)
	<-up.started

	took := p.stop(t, 3*time.Second)

	assert.GreaterOrEqual(t, took, 100*time.Millisecond, "the pipeline waited for the in-flight call")
	assert.Equal(t, int32(1), p.acks.Load(), "the offset is settled")
	assert.Equal(t, 1, p.ev.Event.Success)
}

func TestLifecycle_CallOutlivingTheDrainWindowIsAbandonedNotSettled(t *testing.T) {
	baseline := runtime.NumGoroutine()
	up := newBlocking(time.Hour)
	p := startPipeline(t, up, &schemas.ConsumerConfig{Service: schemas.HostConfig{Timeout: 30 * time.Second}}, 150*time.Millisecond)
	<-up.started

	took := p.stop(t, 3*time.Second)

	assert.GreaterOrEqual(t, took, 100*time.Millisecond, "it waited for the whole window first")
	assert.Zero(t, p.acks.Load(), "an abandoned record must stay unsettled so it is redelivered")
	assert.Zero(t, p.ev.Event.Failure, "an interrupted call is not a service failure")
	assert.Zero(t, p.ev.Event.Success)
	assert.Empty(t, metricsfile.Durations(t, p.file, consts.EventRequestService), "no metric is recorded for an abandoned call")
	goroutinesSettleTo(t, baseline)
}

func TestLifecycle_RealFailureWhileRunningIsStillAFailureAndSettled(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	conf := upstreamConf(t, srv, "")
	conf.Service.Timeout = time.Second
	srv.Close()
	p := startPipeline(t, transport.NewHTTPUpstream(conf), conf, time.Second)

	p.stop(t, 3*time.Second)

	assert.Equal(t, int32(1), p.acks.Load())
	assert.Equal(t, 1, p.ev.Event.Failure)
}

func TestLifecycle_HTTPCallOutlivingTheWindowIsAbandoned(t *testing.T) {
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer srv.Close()
	conf := upstreamConf(t, srv, "")
	conf.Service.Timeout = 30 * time.Second
	p := startPipeline(t, transport.NewHTTPUpstream(conf), conf, 150*time.Millisecond)
	<-started

	p.stop(t, 3*time.Second)

	assert.Zero(t, p.acks.Load())
	assert.Zero(t, p.ev.Event.Failure)
}

func TestLifecycle_GRPCCallOutlivingTheWindowIsAbandonedAndConnectionCloses(t *testing.T) {
	srv := mockmodel.Start(t)
	srv.Delay(time.Hour)
	conf := grpcConf(t, srv.Addr, schemas.ServiceTLS{})
	conf.Service.Timeout = 30 * time.Second
	up, err := transport.NewUpstream(conf)
	require.NoError(t, err)
	p := startPipeline(t, up, conf, 150*time.Millisecond)
	require.Eventually(t, func() bool { return len(srv.Requests()) == 1 }, 3*time.Second, 20*time.Millisecond)

	p.stop(t, 3*time.Second)

	assert.Zero(t, p.acks.Load())
	assert.Zero(t, p.ev.Event.Failure)
	assert.NoError(t, up.(io.Closer).Close(), "the connection can be closed once the calls are gone")
}
