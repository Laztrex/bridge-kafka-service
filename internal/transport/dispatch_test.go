package transport_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
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

func drainPublish() {
	for {
		select {
		case <-transport.PublishChannel:
		default:
			return
		}
	}
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
	drainPublish()
	var gotBody, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotCustom = string(b), r.Header.Get("x-custom")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, 3*time.Second)

	assert.JSONEq(t, `{"q":1}`, gotBody)
	assert.Equal(t, "v", gotCustom)
	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 0, *acks, "ack travels with the response event instead of settling now")

	select {
	case out := <-transport.PublishChannel:
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
	drainPublish()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("plain text")) }))
	defer srv.Close()

	ev, _ := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, 3*time.Second)

	out := <-transport.PublishChannel
	assert.JSONEq(t, `"plain text"`, string(out.Values.Body.Data))
}

func TestSendToService_EmptyResponseBody(t *testing.T) {
	drainPublish()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	defer srv.Close()

	ev, _ := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, 3*time.Second)

	out := <-transport.PublishChannel
	assert.Nil(t, out.Values.Body.Data)
}

func TestSendToService_NoResponseTopicSettlesImmediately(t *testing.T) {
	drainPublish()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()

	ev, acks := newEvent("")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "")), ev, 3*time.Second)

	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, transport.PublishChannel)
}

func TestSendToService_UpstreamDownIsFailureAndSettled(t *testing.T) {
	drainPublish()
	srv := httptest.NewServer(http.NotFoundHandler())
	conf := upstreamConf(t, srv, "resp")
	srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(conf), ev, time.Second)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks, "terminal failure must not stall the partition")
}

func TestSendToService_TimeoutIsFailure(t *testing.T) {
	drainPublish()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(2 * time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ev, acks := newEvent("resp")
	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, 100*time.Millisecond)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
}

func TestSlowModeCapsConcurrency(t *testing.T) {
	drainPublish()
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
		transport.ConsumerResponseSlow(logger(t), conf, transport.NewHTTPUpstream(conf), ch, ctxFrom(ctxDone))
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

func TestFastModeDispatchesWithoutCap(t *testing.T) {
	drainPublish()
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
		transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, ctxFrom(make(chan struct{})))
	}()

	for i := 0; i < 5; i++ {
		e, _ := newEvent("")
		ch <- e
	}
	deadline := time.Now().Add(3 * time.Second)
	for started.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, int32(5), started.Load(), "fast mode must not cap concurrency")

	close(release)
	close(ch)
	<-done
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
					transport.ConsumerResponseSlow(logger(t), conf, transport.NewHTTPUpstream(conf), ch, ctxFrom(stop))
				} else {
					transport.ConsumerResponseFast(logger(t), conf, transport.NewHTTPUpstream(conf), ch, ctxFrom(stop))
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
	drainPublish()
	for i := 0; i < cap(transport.PublishChannel); i++ {
		transport.PublishChannel <- &schemas.ConsumerEvent{}
	}
	defer drainPublish()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer srv.Close()
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), transport.NewHTTPUpstream(upstreamConf(t, srv, "resp")), ev, 3*time.Second)

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
	drainPublish()
	up := &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{"answer":42}`), Header: http.Header{"X-Other": {"1"}}}}
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), up, ev, time.Second)

	assert.JSONEq(t, `{"q":1}`, string(up.got.Body))
	assert.Equal(t, "client-7", up.got.Headers[consts.HeaderClientId])
	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 0, *acks, "ack travels with the response event")

	select {
	case out := <-transport.PublishChannel:
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
	drainPublish()
	ev, _ := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, time.Second)

	out := <-transport.PublishChannel
	assert.Equal(t, "req-1", out.Values.Body.Headers[consts.HeaderRequestIdKey])
}

func TestSharedPath_FailureSettlesAndPublishesNothing(t *testing.T) {
	drainPublish()
	ev, acks := newEvent("resp")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{err: errors.New("down")}, ev, time.Second)

	assert.Equal(t, 1, ev.Event.Failure)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, transport.PublishChannel)
}

func TestSharedPath_NoResponseTopicSettlesAfterTheCall(t *testing.T) {
	drainPublish()
	ev, acks := newEvent("")

	transport.SendToService(context.Background(), logger(t), &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, time.Second)

	assert.Equal(t, 1, ev.Event.Success)
	assert.Equal(t, 1, *acks)
	assert.Empty(t, transport.PublishChannel)
}

func TestRequestServiceEventHasNonZeroDuration(t *testing.T) {
	drainPublish()
	file := filepath.Join(t.TempDir(), "m.log")
	l, err := observability.NewFileTransactionLogger(file, "")
	require.NoError(t, err)
	l.SetClock(fakeclock.NewStepping(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 10*time.Millisecond).Now)
	l.Run()
	ev, _ := newEvent("")

	transport.SendToService(context.Background(), l, &fakeUpstream{resp: transport.UpstreamResponse{Body: []byte(`{}`)}}, ev, time.Second)
	l.Shutdown()

	assert.Positive(t, metricsfile.Durations(t, file, consts.EventRequestService)["req-1"])
}
