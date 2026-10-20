package transport_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/fakeclock"
	"bridge-kafka-service/testutil/metricsfile"
	"bridge-kafka-service/testutil/mockmodel"
)

func upstreamConf(t *testing.T, srv *httptest.Server, responseTopic string) *schemas.ConsumerConfig {
	t.Helper()
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return &schemas.ConsumerConfig{
		ResponseTopic: responseTopic,
		Service:       schemas.HostConfig{Name: u.Hostname(), Port: u.Port(), Path: "/predict"},
	}
}

func newEvent(topic string) (*schemas.ConsumerEvent, *int) {
	acks := 0
	e := &schemas.ConsumerEvent{
		ResponseTopic: topic,
		Event:         &schemas.Event{EventType: consts.EventRequestService},
		Values: &schemas.ContextValues{
			RequestId: "req-1",
			Path:      "/predict",
			Body: &schemas.RequestBody{
				Data: []byte(`{"q":1}`),
				Headers: map[string]string{
					consts.HeaderRequestIdKey: "req-1",
					consts.HeaderClientId:     "client-7",
					"x-custom":                "v",
				},
			},
		},
	}
	e.SetAck(func() { acks++ })
	return e, &acks
}

func logger(t *testing.T) *observability.FileTransactionLogger {
	t.Helper()
	l, err := observability.NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.Run()
	t.Cleanup(l.Shutdown)
	return l
}

// ctxFrom returns a context cancelled as soon as done closes.
func ctxFrom(done <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-done; cancel() }()
	return ctx
}

func TestSendToService_ForwardsAndCarriesTrackingToResponse(t *testing.T) {
	q := transport.NewResponseQueue()
	var gotBody, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotCustom = string(b), r.Header.Get("x-custom")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, 3*time.Second)

	assert.JSONEq(t, `{"q":1}`, gotBody)
	assert.Equal(t, "v", gotCustom)
	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 0, *acks, "ack travels with the response event instead of settling now")

	select {
	case out := <-q:
		assert.JSONEq(t, `{"ok":true}`, string(out.Values.Body.Data))
		assert.Equal(t, "req-1", out.Values.Body.Headers[consts.HeaderRequestIdKey], "original Request-Id is carried")
		assert.Equal(t, "client-7", out.Values.Body.Headers[consts.HeaderClientId])
		out.Ack()
		assert.Equal(t, 1, *acks)
	case <-time.After(time.Second):
		t.Fatal("response event not published")
	}
}

func TestSendToService_NonJSONResponseStoredAsString(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("plain text")) }))
	defer srv.Close()

	ev, _ := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, 3*time.Second)

	out := <-q
	assert.JSONEq(t, `"plain text"`, string(out.Values.Body.Data))
}

func TestSendToService_EmptyResponseBody(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer srv.Close()

	ev, _ := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, 3*time.Second)

	out := <-q
	assert.Nil(t, out.Values.Body.Data)
}

func TestSendToService_NoResponseTopicSettlesImmediately(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()

	ev, acks := newEvent("")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "")), ev, q, 3*time.Second)

	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, q)
}

func TestSendToService_UpstreamDownIsFailureAndSettled(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.NotFoundHandler())
	conf := upstreamConf(t, srv, "resp")
	srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(conf), ev, q, time.Second)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "terminal failure must not stall the partition")
}

func TestSendToService_TimeoutIsFailure(t *testing.T) {
	q := transport.NewResponseQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, 100*time.Millisecond)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
}

func TestSlowModeCapsConcurrency(t *testing.T) {
	q := transport.NewResponseQueue()
	var inflight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inflight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		inflight.Add(-1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	conf := upstreamConf(t, srv, "")
	conf.Service.MaxConn = 2
	ch := make(chan *schemas.ConsumerEvent, 10)
	ctxDone := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		transport.ConsumerResponseSlow(logger(t), conf, transport.NewHTTPUpstream(conf), ch, q, ctxFrom(ctxDone), nil)
	}()

	var acked = make(chan struct{}, 6)
	for i := 0; i < 6; i++ {
		e, _ := newEvent("")
		e.SetAck(func() { acked <- struct{}{} })
		ch <- e
	}
	for i := 0; i < 6; i++ {
		select {
		case <-acked:
		case <-time.After(5 * time.Second):
			t.Fatal("not all messages processed")
		}
	}
	close(ch)
	<-finished
	assert.LessOrEqual(t, peak.Load(), int32(2))
}

func TestFastModeBelowCeilingDispatchesAllConcurrently(t *testing.T) {
	q := transport.NewResponseQueue()
	release := make(chan struct{})
	var started atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Add(1)
		<-release
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	conf := upstreamConf(t, srv, "")
	ch := make(chan *schemas.ConsumerEvent, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, q, ctxFrom(make(chan struct{})), nil)
	}()

	for i := 0; i < 5; i++ {
		e, _ := newEvent("")
		ch <- e
	}
	deadline := time.Now().Add(3 * time.Second)
	for started.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, int32(5), started.Load(), "below the ceiling every record starts concurrently")

	close(release)
	close(ch)
	<-done
}

func TestFastModeCeilingHoldsBackFurtherCalls(t *testing.T) {
	q := transport.NewResponseQueue()
	transport.FastModeLimit(t, 2)
	release := make(chan struct{})
	var started atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Add(1)
		<-release
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	conf := upstreamConf(t, srv, "")
	ch := make(chan *schemas.ConsumerEvent, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, q, ctxFrom(make(chan struct{})), nil)
	}()

	for i := 0; i < 5; i++ {
		e, _ := newEvent("")
		ch <- e
	}

	// exactly the ceiling starts; the queued records wait for a slot
	require.Eventually(t, func() bool { return started.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(2), started.Load(), "no call above the ceiling is in flight")

	release <- struct{}{} // one slot frees
	require.Eventually(t, func() bool { return started.Load() == 3 }, 3*time.Second, 10*time.Millisecond,
		"the next record starts only after a call finishes")

	close(release)
	close(ch)
	<-done
	assert.Equal(t, int32(5), started.Load(), "every queued record is dispatched in the end")
}

func TestFastModeShutdownAtCeilingLeavesWaitingRecordUnsettled(t *testing.T) {
	q := transport.NewResponseQueue()
	transport.FastModeLimit(t, 1)
	block := make(chan struct{})
	var started atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started.Add(1)
		<-block
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	conf := upstreamConf(t, srv, "")
	ch := make(chan *schemas.ConsumerEvent, 4)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, q, ctxFrom(stop), nil)
	}()

	flying, _ := newEvent("")
	ch <- flying // takes the only slot and blocks in the upstream
	waiting, waitingAcks := newEvent("")
	waiting.SetAck(func() { *waitingAcks++ })
	ch <- waiting // waits for a slot

	require.Eventually(t, func() bool { return started.Load() == 1 }, 3*time.Second, 10*time.Millisecond)
	close(stop)  // shutdown while the semaphore is full and a record waits
	close(block) // let the in-flight call finish so the drain completes

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a dispatch waiting for the ceiling must return immediately on shutdown")
	}
	assert.Zero(t, *waitingAcks, "the waiting record is not settled and will be redelivered")
}

// newConnCountingServer returns an httptest server that counts accepted
// connections (StateNew), started before it accepts anything.
func newConnCountingServer(t *testing.T, newConns *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// runPipelines sends total events through a dispatcher and waits for their acks.
func runPipelines(t *testing.T, conf *schemas.ConsumerConfig, total int, slowMode bool) {
	t.Helper()
	ch := make(chan *schemas.ConsumerEvent, total+1)
	acked := make(chan struct{}, total+1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if slowMode {
			transport.ConsumerResponseSlow(logger(t), conf, transport.NewHTTPUpstream(conf), ch, nil, ctxFrom(make(chan struct{})), nil)
		} else {
			transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, nil, ctxFrom(make(chan struct{})), nil)
		}
	}()

	for range total {
		e, _ := newEvent("")
		e.SetAck(func() { acked <- struct{}{} })
		ch <- e
	}
	for range total {
		select {
		case <-acked:
		case <-time.After(10 * time.Second):
			t.Fatal("not all calls finished")
		}
	}
	close(ch)
	<-done
}

func TestHTTPUpstreamReusesConnections_SlowMode(t *testing.T) {
	var newConns atomic.Int32
	srv := newConnCountingServer(t, &newConns)

	conf := upstreamConf(t, srv, "")
	conf.Service.MaxConn = 8

	runPipelines(t, conf, 200, true)

	// The pool must reuse connections: 200 calls through a pool sized to the
	// limit open far fewer connections than calls. The bound is the limit plus
	// slack, not the exact limit: with MaxConnsPerHost = 0 (baseline decision DEL-6, the
	// semaphore is the only cap) net/http briefly dials a new connection when
	// a request starts exactly while a finished connection is still returning
	// to the idle pool. Without a per-host pool the 200 calls would open ~200
	// connections (http.DefaultTransport keeps only 2 idle per host).
	assert.Positive(t, newConns.Load(), "the run must have opened at least one connection")
	assert.LessOrEqual(t, newConns.Load(), int32(16),
		"a steady 8-way concurrency must reuse a bounded pool of connections")
}

func TestHTTPUpstreamReusesConnections_FastModeCeiling(t *testing.T) {
	transport.FastModeLimit(t, 4)
	var newConns atomic.Int32
	srv := newConnCountingServer(t, &newConns)

	conf := upstreamConf(t, srv, "")

	runPipelines(t, conf, 200, false)

	// Same overshoot reasoning as the slow-mode variant: the bound is the
	// ceiling plus slack, proving reuse rather than an exact connection count.
	assert.Positive(t, newConns.Load(), "the run must have opened at least one connection")
	assert.LessOrEqual(t, newConns.Load(), int32(8),
		"a fast-mode pipeline must reuse a bounded pool of connections")
}

func TestDispatcherStopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer srv.Close()

	for name, maxConn := range map[string]int{"fast": 0, "slow, semaphore exhausted": 1} {
		t.Run(name, func(t *testing.T) {
			conf := upstreamConf(t, srv, "")
			conf.Service.MaxConn = maxConn
			conf.Service.Timeout = time.Second
			stop := make(chan struct{})
			ch := make(chan *schemas.ConsumerEvent, 4)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if maxConn > 0 {
					transport.ConsumerResponseSlow(logger(t), conf, transport.NewHTTPUpstream(conf), ch, nil, ctxFrom(stop), nil)
				} else {
					transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, nil, ctxFrom(stop), nil)
				}
			}()
			for i := 0; i < 2; i++ {
				e, _ := newEvent("")
				ch <- e
			}
			time.Sleep(100 * time.Millisecond)
			close(stop)

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("dispatcher did not stop on cancel")
			}
		})
	}
}

func TestPublishReply_FullPublishChannelDropsAndSettles(t *testing.T) {
	q := transport.NewResponseQueue()
	for i := 0; i < cap(q); i++ {
		q <- &schemas.ConsumerEvent{}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, q, 3*time.Second)

	assert.Equal(t, 1, *acks, "a dropped response must still settle the offset")
}

type brokenWriter struct {
	header http.Header
	code   int
}

func (w *brokenWriter) Header() http.Header        { return w.header }
func (w *brokenWriter) Write([]byte) (int, error)  { return 0, errors.New("closed") }
func (w *brokenWriter) WriteHeader(statusCode int) { w.code = statusCode }

func TestProduceResponse(t *testing.T) {
	w := httptest.NewRecorder()
	transport.ProduceResponse("success", w)
	assert.JSONEq(t, `{"result":"success"}`, w.Body.String())

	bw := &brokenWriter{header: http.Header{}}
	transport.ProduceResponse("success", bw)
	assert.Equal(t, http.StatusInternalServerError, bw.code)
}

// fakeUpstream proves the shared path (settlement, tracking headers, publication,
// metrics) does not depend on HTTP: any transport satisfying the interface gets it.
type fakeUpstream struct {
	resp transport.UpstreamResponse
	err  error
	got  transport.UpstreamRequest
}

func (f *fakeUpstream) Call(_ context.Context, req transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	f.got = req
	return f.resp, f.err
}

func TestSharedPath_ResponseIsPublishedWithCarriedTrackingHeaders(t *testing.T) {
	q := transport.NewResponseQueue()
	up := &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{"answer":42}`), Header: http.Header{"X-Other": {"1"}}}}
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), up, ev, q, time.Second)

	assert.JSONEq(t, `{"q":1}`, string(up.got.Body))
	assert.Equal(t, "client-7", up.got.Headers[consts.HeaderClientId])
	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 0, *acks, "ack travels with the response event")

	select {
	case out := <-q:
		assert.JSONEq(t, `{"answer":42}`, string(out.Values.Body.Data))
		assert.Equal(t, "req-1", out.Values.Body.Headers[consts.HeaderRequestIdKey])
		assert.Equal(t, "client-7", out.Values.Body.Headers[consts.HeaderClientId])
		assert.Equal(t, "1", out.Values.Body.Headers["X-Other"])
		assert.Equal(t, "resp", out.ResponseTopic)
	case <-time.After(time.Second):
		t.Fatal("nothing published")
	}
}

func TestSharedPath_NilResponseHeadersAreTolerated(t *testing.T) {
	q := transport.NewResponseQueue()
	ev, _ := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, q, time.Second)

	out := <-q
	assert.Equal(t, "req-1", out.Values.Body.Headers[consts.HeaderRequestIdKey])
}

func TestSharedPath_FailureSettlesAndPublishesNothing(t *testing.T) {
	q := transport.NewResponseQueue()
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{err: errors.New("down")}, ev, q, time.Second)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, q)
}

func TestSharedPath_NoResponseTopicSettlesAfterTheCall(t *testing.T) {
	q := transport.NewResponseQueue()
	ev, acks := newEvent("")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, q, time.Second)

	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, q)
}

func TestRequestServiceEventHasNonZeroDuration(t *testing.T) {
	q := transport.NewResponseQueue()
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.SetClock(fakeclock.NewStepping(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond).Now)
	l.Run()
	ev, _ := newEvent("")

	transport.SendToService(context.Background(), l, &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, q, time.Second)
	l.Shutdown()

	assert.Positive(t, metricsfile.Durations(t, file, consts.EventRequestService)["req-1"])
}

// countingUpstream fails every call with err and counts the calls.
type countingUpstream struct {
	calls atomic.Int32
	err   error
}

func (c *countingUpstream) Call(context.Context, transport.UpstreamRequest) (transport.UpstreamResponse, error) {
	c.calls.Add(1)
	return transport.UpstreamResponse{}, c.err
}

// failingFor returns the failing_for values (milliseconds) of the log lines carrying msg, in order.
func failingFor(t *testing.T, logs *syncBuffer, msg string) []float64 {
	t.Helper()
	logs.mu.Lock()
	text := logs.b.String()
	logs.mu.Unlock()
	var out []float64
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line))
		if line["message"] != msg {
			continue
		}
		v, ok := line["failing_for"].(float64)
		require.True(t, ok, "failing_for missing in %s", sc.Text())
		out = append(out, v)
	}
	return out
}

func TestPermanentFailure_RetryPipelineSettlesAfterOneAttempt(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	up := &countingUpstream{err: transport.Permanent(errors.New("response body too large"))}
	ev, acks := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, nil, time.Second, transport.NewRetryState())

	assert.Equal(t, int32(1), up.calls.Load(), "a permanent failure is not retried")
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "the offset is settled")
}

func TestPermanentFailure_SequentialRetryPipelineGoesOn(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	var calls atomic.Int32
	up := upstreamFunc(func(transport.UpstreamRequest) (transport.UpstreamResponse, error) {
		if calls.Add(1) == 1 {
			return transport.UpstreamResponse{}, transport.Permanent(errors.New("response body too large"))
		}
		return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
	})
	conf := &schemas.ConsumerConfig{OnFailure: schemas.FailureRetry, Service: schemas.HostConfig{MaxConn: 1, Timeout: time.Second}}
	ch := make(chan *schemas.ConsumerEvent, 2)
	var acks atomic.Int32
	for range 2 {
		ev, _ := newEvent("")
		ev.SetAck(func() { acks.Add(1) })
		ch <- ev
	}
	close(ch)

	transport.ConsumerResponseSlow(logger(t), conf, up, ch, nil, context.Background(), nil)

	assert.Equal(t, int32(2), calls.Load(), "one attempt per record")
	assert.Equal(t, int32(2), acks.Load(), "both records are settled, the pipeline does not stall")
}

func TestPermanentFailure_SkipPipelineSettlesOnce(t *testing.T) {
	up := &countingUpstream{err: transport.Permanent(errors.New("response body too large"))}
	ev, acks := newEvent("")

	transport.SendToService(context.Background(), logger(t), up, ev, nil, time.Second)

	assert.Equal(t, int32(1), up.calls.Load())
	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
}

func TestPermanentFailure_ConnectionErrorIsStillRetried(t *testing.T) {
	transport.FastRetries(t, time.Millisecond, time.Millisecond)
	up := &scriptedUpstream{fail: func(call int32) bool { return call <= 2 }}
	ev, _ := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, nil, time.Second, transport.NewRetryState())

	assert.Equal(t, int32(3), up.calls.Load())
	assert.Equal(t, 1, ev.Event.Success)
	assert.False(t, transport.IsPermanent(errDown))
}

func TestRetry_WarningStatesHowLongTheRecordHasBeenFailing(t *testing.T) {
	transport.FastRetries(t, 5*time.Millisecond, 5*time.Millisecond)
	logs := captureLog(t)
	up := &scriptedUpstream{fail: func(call int32) bool { return call <= 3 }}
	ev, _ := newEvent("")

	transport.SendToServiceRetrying(context.Background(), logger(t), up, ev, nil, time.Second, transport.NewRetryState())

	got := failingFor(t, logs, "upstream call failed, retrying")
	require.Len(t, got, 3)
	assert.Less(t, got[0], got[1])
	assert.Less(t, got[1], got[2])
}

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
		transport.ConsumerResponseFast(l, conf, upstream, p.ch, nil, context.Background(), nil)
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

// claimEvent is an event of a claim whose session ends when the returned cancel is called; it
// counts its acks and abandons.
func claimEvent(t *testing.T, id string) (ev *schemas.ConsumerEvent, endClaim context.CancelFunc, acks, abandons *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ev, _ = newEvent("")
	ev.Values.RequestId = id
	acks, abandons = &atomic.Int32{}, &atomic.Int32{}
	ev.ClaimCtx = ctx
	ev.SetAck(func() { acks.Add(1) })
	ev.SetAbandon(func() { abandons.Add(1) })
	return ev, cancel, acks, abandons
}

// runDispatcher runs a slow-mode dispatcher with max_conn slots over ch until the test ends.
func runDispatcher(t *testing.T, up transport.Upstream, maxConn int, policy string) chan *schemas.ConsumerEvent {
	t.Helper()
	conf := &schemas.ConsumerConfig{OnFailure: policy, Service: schemas.HostConfig{MaxConn: maxConn, Timeout: 5 * time.Second}, DrainWindow: 100 * time.Millisecond}
	ch := make(chan *schemas.ConsumerEvent, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); transport.ConsumerResponseSlow(logger(t), conf, up, ch, nil, ctx, nil) }()
	t.Cleanup(func() { cancel(); <-done })
	return ch
}

func TestDispatch_RecordOfAnEndedClaimIsNotSent(t *testing.T) {
	var calls atomic.Int32
	up := upstreamFunc(func(transport.UpstreamRequest) (transport.UpstreamResponse, error) {
		calls.Add(1)
		return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
	})
	ch := runDispatcher(t, up, 1, schemas.FailureSkip)

	ev, endClaim, acks, abandons := claimEvent(t, "ended")
	endClaim()
	ch <- ev

	require.Eventually(t, func() bool { return abandons.Load() == 1 }, 3*time.Second, 5*time.Millisecond)
	assert.Zero(t, calls.Load(), "the upstream is not called for a record of an ended claim")
	assert.Zero(t, acks.Load(), "the record is not settled")
}

func TestDispatch_SlotWaitEndsWhenTheClaimEnds(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	up := upstreamFunc(func(transport.UpstreamRequest) (transport.UpstreamResponse, error) {
		calls.Add(1)
		<-release
		return transport.UpstreamResponse{Body: []byte(`{}`)}, nil
	})
	ch := runDispatcher(t, up, 1, schemas.FailureSkip)
	t.Cleanup(func() { close(release) }) // runs before the dispatcher's cleanup, which waits for this call

	busy, _, _, _ := claimEvent(t, "busy")
	ch <- busy
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 3*time.Second, 5*time.Millisecond)

	waiting, endClaim, acks, abandons := claimEvent(t, "waiting")
	ch <- waiting
	time.Sleep(50 * time.Millisecond)
	endClaim()

	require.Eventually(t, func() bool { return abandons.Load() == 1 }, 3*time.Second, 5*time.Millisecond,
		"a record waiting for a slot is given up as soon as its claim ends")
	assert.Equal(t, int32(1), calls.Load())
	assert.Zero(t, acks.Load())
}

func TestDispatch_UpstreamRetryStopsWhenTheClaimEnds(t *testing.T) {
	transport.FastRetries(t, 20*time.Millisecond, 20*time.Millisecond)
	up := &scriptedUpstream{fail: func(int32) bool { return true }}
	ch := runDispatcher(t, up, 1, schemas.FailureRetry)

	ev, endClaim, acks, abandons := claimEvent(t, "retrying")
	ch <- ev
	require.Eventually(t, func() bool { return up.calls.Load() >= 2 }, 3*time.Second, 5*time.Millisecond)
	endClaim()

	require.Eventually(t, func() bool { return abandons.Load() == 1 }, 3*time.Second, 5*time.Millisecond)
	stopped := up.calls.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, stopped, up.calls.Load(), "no more upstream calls after the claim ended")
	assert.Zero(t, acks.Load())
}
