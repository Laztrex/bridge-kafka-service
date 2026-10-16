package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/testutil/mocksarama"
)

func server(t *testing.T, cfg *schemas.KafkaInfo, reg *router.ProducerRegistry) *http.Server {
	t.Helper()
	l, err := observability.NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.Run()
	t.Cleanup(l.Shutdown)

	return newHTTPServer(l, reg, cfg, newReadiness(reg, cfg), "127.0.0.1:0")
}

func post(srv *http.Server, topic string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/"+consts.ControllerVersionAPI+"/publish", strings.NewReader(`{"a":1}`))
	r.Header.Set(consts.HeaderProduceTopicName, topic)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, r)
	return w
}

func TestHealthIsServedBeforeKafkaIsReady(t *testing.T) {
	cfg := &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}}
	srv := server(t, cfg, router.NewProducerRegistry())

	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health", nil))

	assert.Equal(t, 200, w.Code)
}

func TestPublishDisabledWithoutProducers(t *testing.T) {
	srv := server(t, &schemas.KafkaInfo{}, router.NewProducerRegistry())

	w := post(srv, "out")

	assert.Equal(t, 503, w.Code)
	assert.Contains(t, w.Body.String(), "no Kafka producers configured")
}

func TestPublishUnavailableUntilProducerConnects(t *testing.T) {
	cfg := &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}}
	reg := router.NewProducerRegistry()
	srv := server(t, cfg, reg)

	w := post(srv, "out")
	assert.Equal(t, 503, w.Code)
	assert.Contains(t, w.Body.String(), "not ready")

	producer := mocksarama.NewSyncProducer(t)
	mocksarama.ExpectSendAndSucceed(producer)
	reg.Set(producer, []string{"out"})

	w = post(srv, "out")
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), "success")
}

func TestPprofNotOnPublicListener(t *testing.T) {
	srv := server(t, &schemas.KafkaInfo{}, router.NewProducerRegistry())

	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))

	assert.Equal(t, 404, w.Code)
}

type countingProducer struct {
	sarama.SyncProducer
	closes int
}

func (p *countingProducer) Close() error { p.closes++; return nil }

func TestCloseProducerClosesSharedProducerOnce(t *testing.T) {
	p := &countingProducer{}
	reg := router.NewProducerRegistry()
	reg.Set(p, []string{"a", "b", "c"})

	closeProducer(reg)

	assert.Equal(t, 1, p.closes, "three topics, one producer, one Close")
}

func TestStartProducerRegistersEveryTopic(t *testing.T) {
	p := &countingProducer{}
	old := newProducerWithRetry
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo) (sarama.SyncProducer, error) { return p, nil }
	t.Cleanup(func() { newProducerWithRetry = old })
	reg := router.NewProducerRegistry()

	startProducer(context.Background(), &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}, {Topic: "b"}}}, reg)

	for _, topic := range []string{"a", "b"} {
		got, ok := reg.Lookup(topic)
		assert.True(t, ok)
		assert.Same(t, p, got)
	}
}

func TestStartProducerLeavesRegistryEmptyWhenUnavailableOrUnconfigured(t *testing.T) {
	reg := router.NewProducerRegistry()
	startProducer(context.Background(), &schemas.KafkaInfo{}, reg)
	assert.False(t, reg.Ready())

	old := newProducerWithRetry
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo) (sarama.SyncProducer, error) { return nil, errors.New("down") }
	t.Cleanup(func() { newProducerWithRetry = old })

	startProducer(context.Background(), &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}}}, reg)
	assert.False(t, reg.Ready(), "a permanent failure registers nothing")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	startProducer(ctx, &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}}}, reg)
	assert.False(t, reg.Ready())
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func canDial(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func TestStartPprof_ServesOnSeparateAddressAndStopsWithContext(t *testing.T) {
	addr := freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	startPprof(ctx, addr)

	require.Eventually(t, func() bool { return canDial(addr) }, 3*time.Second, 20*time.Millisecond)
	cancel()
	assert.Eventually(t, func() bool { return !canDial(addr) }, 3*time.Second, 20*time.Millisecond)
}

func TestStartPprof_DisabledWithoutAddress(t *testing.T) {
	assert.NotPanics(t, func() { startPprof(context.Background(), "") })
}

func TestPublishPathAcceptsOnlyPOST(t *testing.T) {
	path := "/api/" + consts.ControllerVersionAPI + "/publish"

	producer := mocksarama.NewSyncProducer(t)
	ready := router.NewProducerRegistry()
	ready.Set(producer, []string{"out"})

	cases := map[string]*http.Server{
		"no producers configured": server(t, &schemas.KafkaInfo{}, router.NewProducerRegistry()),
		"producer not ready": server(t, &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}},
			router.NewProducerRegistry()),
		"producer ready": server(t, &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}}, ready),
	}
	for name, srv := range cases {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			t.Run(name+"/"+method, func(t *testing.T) {
				w := httptest.NewRecorder()
				srv.Handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))

				assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
				assert.Equal(t, "POST", w.Header().Get("Allow"))
			})
		}
	}
}

func TestUnversionedPublishPathIsNotFound(t *testing.T) {
	srv := server(t, &schemas.KafkaInfo{}, router.NewProducerRegistry())
	w := httptest.NewRecorder()
	srv.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/publish", nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// connectEnv runs Connect from the repo root (it reads configs/*.yaml by relative
// path) with metrics redirected into a temp dir; the returned func wraps a Kafka setup
// into an application config.
func connectEnv(t *testing.T, retry time.Duration) func(*schemas.KafkaInfo) *config.Config {
	t.Helper()
	t.Chdir("../..")
	metrics := t.TempDir()

	old := retryWindow
	retryWindow = retry
	t.Cleanup(func() { retryWindow = old })
	return func(k *schemas.KafkaInfo) *config.Config {
		return &config.Config{
			HTTPAddr: "127.0.0.1:0", ShutdownTimeout: config.DefaultShutdownTimeout,
			MetricPath: metrics, MetricFileName: "m.log", Kafka: k,
		}
	}
}

func TestConnect_ConsumerPermanentFailureReturnsError(t *testing.T) {
	wrap := connectEnv(t, 300*time.Millisecond)
	cfg := &schemas.KafkaInfo{
		Brokers: []string{freeAddr(t)},
		Version: "2.3.0",
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}

	done := make(chan error, 1)
	go func() { done <- Connect(context.Background(), wrap(cfg)) }()

	select {
	case err := <-done:
		assert.ErrorContains(t, err, "permanently failed to create consumer client")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect kept running after the consumer could not be created")
	}
}

func TestConnect_GracefulShutdownOnContextCancel(t *testing.T) {
	wrap := connectEnv(t, 5*time.Minute)
	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, wrap(cfg)) }()

	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}

func TestConnect_InvalidConfig(t *testing.T) {
	wrap := connectEnv(t, time.Second)
	assert.Error(t, Connect(context.Background(), wrap(&schemas.KafkaInfo{})))
}

func TestConnect_InvalidUpstreamTLSAbortsBeforeAnythingStarts(t *testing.T) {
	wrap := connectEnv(t, time.Second)
	cfg := &schemas.KafkaInfo{
		Brokers: []string{freeAddr(t)},
		Version: "2.3.0",
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{
				Name: "svc", Port: "1", Path: "/",
				TLS: schemas.ServiceTLS{Mode: "tls", CAPath: "/nonexistent/ca.pem"},
			},
		}},
	}

	done := make(chan error, 1)
	go func() { done <- Connect(context.Background(), wrap(cfg)) }()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid upstream configuration")
		assert.ErrorContains(t, err, "ca_path")
	case <-time.After(5 * time.Second):
		t.Fatal("Connect must fail fast on an unusable TLS configuration")
	}
}

func TestConnect_UnknownProtocolAborts(t *testing.T) {
	wrap := connectEnv(t, time.Second)
	cfg := &schemas.KafkaInfo{
		Brokers: []string{freeAddr(t)},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Protocol: "soap"},
		}},
	}
	err := Connect(context.Background(), wrap(cfg))
	require.Error(t, err)
	assert.ErrorContains(t, err, `"soap"`)
}
