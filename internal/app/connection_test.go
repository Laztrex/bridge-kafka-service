package app

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/handlers"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
	"bridge-kafka-service/testutil/mocksarama"
)

func server(t *testing.T, cfg *schemas.KafkaInfo, reg *router.ProducerRegistry) *http.Server {
	t.Helper()
	l, err := observability.NewFileTransactionLogger(filepath.Join(t.TempDir(), "m.log"), "")
	require.NoError(t, err)
	l.Run()
	t.Cleanup(l.Shutdown)

	return newHTTPServer(l, reg, cfg, newRuntimeState(reg, cfg), "127.0.0.1:0")
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
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) { return p, nil }
	t.Cleanup(func() { newProducerWithRetry = old })
	reg := router.NewProducerRegistry()

	require.NoError(t, startProducer(context.Background(), &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}, {Topic: "b"}}}, nil, reg))

	for _, topic := range []string{"a", "b"} {
		got, ok := reg.Lookup(topic)
		assert.True(t, ok)
		assert.Same(t, p, got)
	}
}

func TestStartProducerReturnsItsErrorAndRegistersNothing(t *testing.T) {
	reg := router.NewProducerRegistry()

	require.NoError(t, startProducer(context.Background(), &schemas.KafkaInfo{}, nil, reg), "no producers: nothing to fail")
	assert.False(t, reg.Ready())

	old := newProducerWithRetry
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
		return nil, errors.New("down")
	}
	t.Cleanup(func() { newProducerWithRetry = old })

	err := startProducer(context.Background(), &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}}}, nil, reg)
	require.Error(t, err)
	assert.False(t, reg.Ready(), "a permanent failure registers nothing")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, startProducer(ctx, &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "a"}}}, nil, reg),
		"cancellation is still an error; the caller checks ctx")
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
func connectEnv(t *testing.T) func(*schemas.KafkaInfo) *config.Config {
	t.Helper()
	t.Chdir("../..")
	metrics := t.TempDir()
	return func(k *schemas.KafkaInfo) *config.Config {
		return &config.Config{
			HealthAddr: "127.0.0.1:0", ShutdownTimeout: config.DefaultShutdownTimeout,
			MetricPath: metrics, MetricFileName: "m.log", Kafka: k,
		}
	}
}

// An unreachable broker keeps the clients retrying (LIFE-3): the process stays up and not ready,
// /healthz answers 200, /readyz answers 503 naming the
// clients, and SIGTERM (a cancelled context) ends it with exit code 0.
func TestConnect_UnreachableBrokerKeepsServiceUp(t *testing.T) {
	wrap := connectEnv(t)
	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}
	appCfg := wrap(cfg)
	appCfg.HealthAddr = freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, appCfg) }()

	require.Eventually(t, func() bool { return canDial(appCfg.HealthAddr) }, 5*time.Second, 20*time.Millisecond)

	resp, err := http.Get("http://" + appCfg.HealthAddr + "/healthz")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode, "liveness must not depend on Kafka")

	readyz, err := http.Get("http://" + appCfg.HealthAddr + "/readyz")
	require.NoError(t, err)
	readyzBody, err := io.ReadAll(readyz.Body)
	require.NoError(t, err)
	require.NoError(t, readyz.Body.Close())
	assert.Equal(t, http.StatusServiceUnavailable, readyz.StatusCode)
	assert.Contains(t, string(readyzBody), "kafka producer is not connected")
	assert.Contains(t, string(readyzBody), "not all kafka consumers are running")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "SIGTERM must exit 0 while the broker is unreachable")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}

// Advance virtual time beyond the former five-minute window while exercising the real
// retry loops. Recovery must still register the producer and initialize the consumer.
func TestKafkaClientRetriesRecoverBeyondFormerWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		oldProducer, oldConsumer := newProducerAttempt, newConsumerClientAttempt
		t.Cleanup(func() { newProducerAttempt, newConsumerClientAttempt = oldProducer, oldConsumer })
		var reachable atomic.Bool
		var producerAttempts, consumerAttempts atomic.Int32
		producer := &countingProducer{}
		client := mocksarama.NewFakeClient()
		newProducerAttempt = func(*schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
			producerAttempts.Add(1)
			if !reachable.Load() {
				return nil, errors.New("broker unavailable")
			}
			return producer, nil
		}
		newConsumerClientAttempt = func(*schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
			consumerAttempts.Add(1)
			if !reachable.Load() {
				return nil, errors.New("broker unavailable")
			}
			return client, nil
		}
		conf := &schemas.ConsumerConfig{Topics: []string{"in"}}
		cfg := &schemas.KafkaInfo{
			Producers: []*schemas.ProducerConfig{{Topic: "out"}},
			Consumers: []*schemas.ConsumerConfig{conf},
		}
		registry := router.NewProducerRegistry()
		runtime := newRuntimeState(registry, cfg)
		srv := newHTTPServer(nil, registry, cfg, runtime, "")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		producerDone, consumerDone := make(chan error, 1), make(chan error, 1)
		go func() { producerDone <- startProducer(ctx, cfg, nil, registry) }()
		go func() {
			_, err := createConsumerClientWithRetry(ctx, cfg, conf, nil)
			if err == nil {
				runtime.consumerUp()
			}
			consumerDone <- err
		}()
		time.Sleep(6 * time.Minute)
		synctest.Wait()
		for name, done := range map[string]<-chan error{"producer": producerDone, "consumer": consumerDone} {
			select {
			case err := <-done:
				t.Fatalf("%s stopped retrying before recovery: %v", name, err)
			default:
			}
		}
		if producerAttempts.Load() < 2 || consumerAttempts.Load() < 2 {
			t.Fatal("both clients must make repeated connection attempts")
		}
		if w := probe(t, srv, http.MethodGet, "/readyz"); w.Code != http.StatusServiceUnavailable {
			t.Fatalf("during outage: %d %s", w.Code, w.Body.String())
		}
		if w := probe(t, srv, http.MethodGet, "/healthz"); w.Code != http.StatusOK {
			t.Fatalf("liveness during outage: %d", w.Code)
		}
		reachable.Store(true)
		if err := <-producerDone; err != nil {
			t.Fatal(err)
		}
		if err := <-consumerDone; err != nil {
			t.Fatal(err)
		}
		if w := probe(t, srv, http.MethodGet, "/readyz"); w.Code != http.StatusOK {
			t.Fatalf("after recovery: %d %s", w.Code, w.Body.String())
		}
	})
}

// subscriptionFailureSeams makes consumer clients fakes and replaces runSubscription with run.
func subscriptionFailureSeams(t *testing.T, run func(context.Context) error) {
	t.Helper()
	oldClient, oldRun := newConsumerClientWithRetry, runSubscription
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		return mocksarama.NewFakeClient(), nil
	}
	runSubscription = func(parent context.Context, _ *router.Pause, _ sarama.Client, _ *schemas.ConsumerConfig, _ chan<- *schemas.ConsumerEvent, _ *observability.FileTransactionLogger, _ *transport.PipelineState) error {
		return run(parent)
	}
	t.Cleanup(func() { newConsumerClientWithRetry, runSubscription = oldClient, oldRun })
}

func subscriptionFailureConfig(t *testing.T) *schemas.KafkaInfo {
	return &schemas.KafkaInfo{
		Brokers: []string{freeAddr(t)},
		Version: "2.3.0",
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"orders"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}
}

func TestConnect_SubscriptionStartFailureIsFatal(t *testing.T) {
	wrap := connectEnv(t)
	subscriptionFailureSeams(t, func(context.Context) error {
		return errors.New("subscription for topics [orders]: cannot create consumer group: no group")
	})

	done := make(chan error, 1)
	go func() { done <- Connect(context.Background(), wrap(subscriptionFailureConfig(t))) }()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorContains(t, err, "orders")
		assert.ErrorContains(t, err, "cannot create consumer group")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect kept running after a subscription could not start")
	}
}

func TestConnect_SubscriptionFailureDuringShutdownIsNotReported(t *testing.T) {
	wrap := connectEnv(t)
	started := make(chan struct{})
	subscriptionFailureSeams(t, func(parent context.Context) error {
		close(started)
		<-parent.Done()
		return errors.New("subscription for topics [orders]: cannot create consumer group: client closed")
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, wrap(subscriptionFailureConfig(t))) }()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("subscription did not start")
	}
	cancel()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}

// Each stanza with a response_topic gets its own queue and publishers; a stanza without one gets
// neither, and shutdown returns only after every publisher has drained.
func TestConnect_EachResponseTopicStanzaGetsItsOwnPublisher(t *testing.T) {
	wrap := connectEnv(t)
	oldProducer, oldClient, oldRun, oldPublish := newProducerWithRetry, newConsumerClientWithRetry, runSubscription, responsePublish
	t.Cleanup(func() {
		newProducerWithRetry, newConsumerClientWithRetry, runSubscription, responsePublish = oldProducer, oldClient, oldRun, oldPublish
	})
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
		return mocksarama.NewSyncProducer(t), nil
	}
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		return mocksarama.NewFakeClient(), nil
	}

	var mu sync.Mutex
	queueOf := map[string]chan<- *schemas.ConsumerEvent{}
	subscriptionState := map[string]*transport.PipelineState{}
	var subscribed sync.WaitGroup
	subscribed.Add(3)
	runSubscription = func(parent context.Context, _ *router.Pause, _ sarama.Client, conf *schemas.ConsumerConfig, queue chan<- *schemas.ConsumerEvent, _ *observability.FileTransactionLogger, state *transport.PipelineState) error {
		mu.Lock()
		queueOf[conf.Topics[0]] = queue
		subscriptionState[conf.Topics[0]] = state
		mu.Unlock()
		subscribed.Done()
		<-parent.Done()
		return nil
	}
	var workers []int
	var publisherStates []*transport.PipelineState
	var drained atomic.Int32
	responsePublish = func(ctx context.Context, _ <-chan *schemas.ConsumerEvent, n int, _ schemas.TransactionLogger, _ handlers.ProducerLookup, state *transport.PipelineState) {
		mu.Lock()
		workers = append(workers, n)
		publisherStates = append(publisherStates, state)
		mu.Unlock()
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		drained.Add(1)
	}

	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out-a"}, {Topic: "out-b"}},
		Consumers: []*schemas.ConsumerConfig{
			{Topics: []string{"a"}, Group: "g", ClientId: "c", ResponseTopic: "out-a",
				Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/", MaxConn: 1}},
			{Topics: []string{"b"}, Group: "g", ClientId: "c", ResponseTopic: "out-b",
				Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"}},
			{Topics: []string{"c"}, Group: "g", ClientId: "c",
				Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, wrap(cfg)) }()

	subscribed.Wait()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(workers) == 2
	}, 5*time.Second, 5*time.Millisecond, "both publisher sets start")
	mu.Lock()
	assert.NotNil(t, subscriptionState["a"])
	assert.NotSame(t, subscriptionState["a"], subscriptionState["b"], "each pipeline has its own state")
	assert.ElementsMatch(t, []*transport.PipelineState{subscriptionState["a"], subscriptionState["b"]}, publisherStates,
		"a pipeline's publishers report into the same state as its subscription")
	assert.NotNil(t, queueOf["a"])
	assert.NotNil(t, queueOf["b"])
	assert.True(t, queueOf["a"] != queueOf["b"], "each pipeline has its own queue")
	assert.Nil(t, queueOf["c"], "a stanza without response_topic gets no queue")
	assert.ElementsMatch(t, []int{1, transport.MaxPublishersPerPipeline}, workers,
		"one publisher set per response stanza, sized by its concurrency")
	mu.Unlock()

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
		assert.Equal(t, int32(2), drained.Load(), "Connect returns only after every publisher has drained")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}

// tuningDir switches the working directory to an empty one holding only the given files
// under configs/, so Connect sees exactly those tuning files.
func tuningDir(t *testing.T, files map[string]string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "configs"), 0o755))
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "configs", name), []byte(content), 0o600))
	}
	t.Chdir(dir)
}

func TestConnect_MissingProducerTuningStopsStartup(t *testing.T) {
	wrap := connectEnv(t)
	tuningDir(t, map[string]string{"config_consumer.yaml": "consumer:\n  return:\n    errors: true\n"})
	var producerCalls atomic.Int32
	old := newProducerWithRetry
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
		producerCalls.Add(1)
		return nil, errors.New("must not be called")
	}
	t.Cleanup(func() { newProducerWithRetry = old })

	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
	}
	start := time.Now()
	err := Connect(context.Background(), wrap(cfg))

	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid Kafka producer tuning")
	assert.ErrorContains(t, err, "config_producer.yaml")
	assert.Less(t, time.Since(start), 5*time.Second, "a missing file is not retried")
	assert.Zero(t, producerCalls.Load(), "startup stops before any client is created")
}

func TestConnect_TuningOfAnUnusedClientIsNotRequired(t *testing.T) {
	wrap := connectEnv(t)
	tuningDir(t, map[string]string{"config_consumer.yaml": "consumer:\n  return:\n    errors: true\n"})
	started := make(chan struct{})
	subscriptionFailureSeams(t, func(parent context.Context) error {
		close(started)
		<-parent.Done()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, wrap(subscriptionFailureConfig(t))) }()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("Connect stopped although no producer needs config_producer.yaml: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("subscription did not start")
	}
	cancel()
	assert.NoError(t, <-done)
}

func TestConnect_UnknownRebalanceStrategyStopsStartup(t *testing.T) {
	wrap := connectEnv(t)
	tuningDir(t, map[string]string{"config_consumer.yaml": "consumer:\n  group:\n    rebalance:\n      strategy: fair\n"})

	err := Connect(context.Background(), wrap(subscriptionFailureConfig(t)))

	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid Kafka consumer tuning")
	assert.ErrorContains(t, err, `"fair"`)
}

// A stanza without topics gets no Kafka client and no subscription, is named once in the log,
// and is not counted in /status.
func TestConnect_StanzaWithoutTopicsIsDisabled(t *testing.T) {
	wrap := connectEnv(t)
	logs := &syncLog{}
	oldLogger := log.Logger
	log.Logger = zerolog.New(logs)
	t.Cleanup(func() { log.Logger = oldLogger })

	var clients atomic.Int32
	started := make(chan struct{}, 2)
	oldClient, oldRun := newConsumerClientWithRetry, runSubscription
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		clients.Add(1)
		return mocksarama.NewFakeClient(), nil
	}
	runSubscription = func(parent context.Context, _ *router.Pause, _ sarama.Client, _ *schemas.ConsumerConfig, _ chan<- *schemas.ConsumerEvent, _ *observability.FileTransactionLogger, _ *transport.PipelineState) error {
		started <- struct{}{}
		<-parent.Done()
		return nil
	}
	t.Cleanup(func() { newConsumerClientWithRetry, runSubscription = oldClient, oldRun })

	cfg := &schemas.KafkaInfo{
		Brokers: []string{freeAddr(t)},
		Version: "2.3.0",
		Consumers: []*schemas.ConsumerConfig{
			{Topics: []string{"orders"}, Group: "g", ClientId: "c", Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"}},
			{Group: "g", ClientId: "c", Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"}},
		},
	}
	appCfg := wrap(cfg)
	appCfg.HealthAddr = freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, appCfg) }()

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("the active stanza did not start")
	}
	assert.Contains(t, pollStatus(t, appCfg.HealthAddr), `"consumers_total":1`)
	cancel()
	require.NoError(t, <-done)

	assert.Equal(t, int32(1), clients.Load(), "only the active stanza gets a Kafka client")
	assert.Contains(t, logs.String(), "consumer #2 has no topics: disabled")
	assert.NotContains(t, logs.String(), "no topics provided")
}

// During the drain after SIGTERM (a cancelled root context), with a slow in-flight upstream call
// still running: /healthz answers 200, /readyz answers 503 with the reason "shutting down", a new
// publish request answers 503 naming the shutdown, and Connect returns nil within
// timeouts.shutdown once the call finishes (LIFE-1).
func TestConnect_ProbesAnswerDuringDrain(t *testing.T) {
	wrap := connectEnv(t)

	// A slow in-flight upstream call: the subscription stays busy until the test releases it.
	release := make(chan struct{})
	oldProducer, oldClient, oldRun := newProducerWithRetry, newConsumerClientWithRetry, runSubscription
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
		return &countingProducer{}, nil
	}
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		return mocksarama.NewFakeClient(), nil
	}
	runSubscription = func(context.Context, *router.Pause, sarama.Client, *schemas.ConsumerConfig, chan<- *schemas.ConsumerEvent, *observability.FileTransactionLogger, *transport.PipelineState) error {
		<-release
		return nil
	}
	t.Cleanup(func() {
		newProducerWithRetry, newConsumerClientWithRetry, runSubscription = oldProducer, oldClient, oldRun
	})

	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"orders"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}
	appCfg := wrap(cfg)
	appCfg.HealthAddr = freeAddr(t)
	appCfg.ShutdownTimeout = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, appCfg) }()

	require.Eventually(t, func() bool { return canDial(appCfg.HealthAddr) }, 5*time.Second, 20*time.Millisecond)
	cancelled := time.Now()
	cancel()

	// /readyz answers 503 with "shutting down" while the in-flight call is still running.
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + appCfg.HealthAddr + "/readyz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(body), "shutting down")
	}, 5*time.Second, 20*time.Millisecond, "/readyz must report the drain")

	// /healthz keeps answering 200 until the drain ends.
	resp, err := http.Get("http://" + appCfg.HealthAddr + "/healthz")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// A new publish request answers 503 naming the shutdown, even with the producer connected.
	req, err := http.NewRequest(http.MethodPost, "http://"+appCfg.HealthAddr+"/api/"+consts.ControllerVersionAPI+"/publish", strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set(consts.HeaderProduceTopicName, "out")
	presp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(presp.Body)
	require.NoError(t, err)
	require.NoError(t, presp.Body.Close())
	assert.Equal(t, http.StatusServiceUnavailable, presp.StatusCode)
	assert.Contains(t, string(body), "service is shutting down")

	// The in-flight call finishes and the process exits 0 within timeouts.shutdown.
	close(release)
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(appCfg.ShutdownTimeout):
		t.Fatal("Connect did not return within timeouts.shutdown")
	}
	assert.Less(t, time.Since(cancelled), appCfg.ShutdownTimeout, "the drain must fit within timeouts.shutdown")
}

type syncLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func TestConnect_GracefulShutdownOnContextCancel(t *testing.T) {
	wrap := connectEnv(t)
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
	wrap := connectEnv(t)
	assert.Error(t, Connect(context.Background(), wrap(&schemas.KafkaInfo{})))
}

func TestConnect_InvalidUpstreamTLSAbortsBeforeAnythingStarts(t *testing.T) {
	wrap := connectEnv(t)
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
	wrap := connectEnv(t)
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

// failingProducerSeams overrides the app seams for one test: the producer fails
// after delay, consumer clients are fakes, and runSubscription is recorded
// instead of running (blocked until its ctx ends, like the real one). It
// returns the counter of subscription starts.
func failingProducerSeams(t *testing.T, delay time.Duration) *atomic.Int32 {
	t.Helper()

	oldProducer := newProducerWithRetry
	newProducerWithRetry = func(context.Context, *schemas.KafkaInfo, *router.Tuning) (sarama.SyncProducer, error) {
		time.Sleep(delay)
		return nil, errors.New("broker unreachable")
	}

	oldClient := newConsumerClientWithRetry
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		return mocksarama.NewFakeClient(), nil
	}

	var runs atomic.Int32
	oldRun := runSubscription
	runSubscription = func(parent context.Context, _ *router.Pause, _ sarama.Client, _ *schemas.ConsumerConfig, _ chan<- *schemas.ConsumerEvent, _ *observability.FileTransactionLogger, _ *transport.PipelineState) error {
		runs.Add(1)
		<-parent.Done()
		return nil
	}

	t.Cleanup(func() {
		newProducerWithRetry, newConsumerClientWithRetry, runSubscription = oldProducer, oldClient, oldRun
	})
	return &runs
}

// pollStatus returns the first /status body served while Connect is running.
func pollStatus(t *testing.T, addr string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + "/status")
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return string(body)
	}
	t.Fatal("could not reach /status while Connect was running")
	return ""
}

// A producer that never connects keeps the service up (LIFE-3): consumers with a response_topic
// never start reading, /readyz names the missing producer, and cancellation ends the process
// with exit code 0 instead of a permanent-failure exit.
func TestConnect_ProducerNeverReadyKeepsServiceUpAndGatesConsumers(t *testing.T) {
	wrap := connectEnv(t)
	// The producer retries "forever": like a real retry loop, it returns only on shutdown.
	oldProducer := newProducerWithRetry
	newProducerWithRetry = func(parent context.Context, _ *schemas.KafkaInfo, _ *router.Tuning) (sarama.SyncProducer, error) {
		<-parent.Done()
		return nil, parent.Err()
	}
	t.Cleanup(func() { newProducerWithRetry = oldProducer })

	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c", ResponseTopic: "out",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}
	appCfg := wrap(cfg)
	appCfg.HealthAddr = freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, appCfg) }()

	assert.Contains(t, pollStatus(t, appCfg.HealthAddr), `"consumers_up":0`,
		"no consumer subscribes while the producer is not up")
	assert.Contains(t, pollStatus(t, appCfg.HealthAddr), "kafka producer is not connected")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("Connect did not return after cancellation")
	}
}

func TestConnect_ProducerFailureGatesConsumersWithResponseTopics(t *testing.T) {
	wrap := connectEnv(t)
	runs := failingProducerSeams(t, 500*time.Millisecond) // a window to observe /status
	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c", ResponseTopic: "out",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}
	appCfg := wrap(cfg)
	appCfg.HealthAddr = freeAddr(t)

	done := make(chan error, 1)
	go func() { done <- Connect(context.Background(), appCfg) }()

	assert.Contains(t, pollStatus(t, appCfg.HealthAddr), `"consumers_up":0`,
		"no consumer subscribes when the producer cannot be created")

	select {
	case err := <-done:
		assert.ErrorContains(t, err, "producer creation failed")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect kept running after the producer failed permanently")
	}
	assert.Zero(t, runs.Load(), "a stanza with response_topic must never subscribe after a producer failure")
}

// An unexpected error returned by the producer factory is fatal even without a response_topic.
// Transient broker outages stay inside the retry loop and do not return such an error.
func TestConnect_ProducerFailureIsFatalWithoutResponseTopics(t *testing.T) {
	wrap := connectEnv(t)
	runs := failingProducerSeams(t, 0)
	cfg := &schemas.KafkaInfo{
		Brokers:   []string{freeAddr(t)},
		Version:   "2.3.0",
		Producers: []*schemas.ProducerConfig{{Topic: "out"}},
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"t"}, Group: "g", ClientId: "c",
			Service: schemas.HostConfig{Name: "svc", Port: "1", Path: "/"},
		}},
	}

	done := make(chan error, 1)
	go func() { done <- Connect(context.Background(), wrap(cfg)) }()

	select {
	case err := <-done:
		assert.ErrorContains(t, err, "producer creation failed")
	case <-time.After(15 * time.Second):
		t.Fatal("Connect kept running after the producer failed permanently")
	}
	assert.Zero(t, runs.Load(), "no stanza subscribes after a producer failure")
}

// Every subscription follows one process-wide pause, which /status reports per pipeline.
func TestConnect_SubscriptionsShareOnePause(t *testing.T) {
	wrap := connectEnv(t)
	oldClient, oldRun := newConsumerClientWithRetry, runSubscription
	t.Cleanup(func() { newConsumerClientWithRetry, runSubscription = oldClient, oldRun })
	newConsumerClientWithRetry = func(context.Context, *schemas.KafkaInfo, *schemas.ConsumerConfig, *router.Tuning) (sarama.Client, error) {
		return mocksarama.NewFakeClient(), nil
	}
	var mu sync.Mutex
	var pauses []*router.Pause
	var subscribed sync.WaitGroup
	subscribed.Add(2)
	runSubscription = func(parent context.Context, pause *router.Pause, _ sarama.Client, _ *schemas.ConsumerConfig, _ chan<- *schemas.ConsumerEvent, _ *observability.FileTransactionLogger, _ *transport.PipelineState) error {
		mu.Lock()
		pauses = append(pauses, pause)
		mu.Unlock()
		subscribed.Done()
		<-parent.Done()
		return nil
	}
	svc := schemas.HostConfig{Name: "svc", Port: "1", Path: "/"}
	cfg := &schemas.KafkaInfo{Brokers: []string{freeAddr(t)}, Version: "2.3.0", Consumers: []*schemas.ConsumerConfig{
		{Topics: []string{"a"}, Group: "g1", ClientId: "c", Service: svc},
		{Topics: []string{"b"}, Group: "g2", ClientId: "c", Service: svc},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Connect(ctx, wrap(cfg)) }()
	subscribed.Wait()

	mu.Lock()
	require.Len(t, pauses, 2)
	assert.NotNil(t, pauses[0])
	assert.Same(t, pauses[0], pauses[1], "both subscriptions follow the same pause")
	mu.Unlock()

	cancel()
	assert.NoError(t, <-done)
}

// One run reports every problem, from every part of the check, and Connect stops on it before
// opening the metric file.
func TestCheck_ReportsProblemsOfEveryPart(t *testing.T) {
	wrap := connectEnv(t)
	tuningDir(t, map[string]string{})
	cfg := &schemas.KafkaInfo{
		Brokers: []string{"127.0.0.1:1"},
		Version: "2.x",
		Consumers: []*schemas.ConsumerConfig{{
			Topics: []string{"orders"}, Group: "g", ResponseTopic: "orders",
			Service: schemas.HostConfig{Name: "svc", Port: "abc", Path: "/"},
		}},
	}

	warnings, err := Check(wrap(cfg))

	require.Error(t, err)
	assert.Empty(t, warnings)
	for _, want := range []string{`kafka.version "2.x"`, `service.port "abc"`, "orders -> orders", "invalid Kafka consumer tuning"} {
		assert.ErrorContains(t, err, want)
	}

	app := wrap(cfg)
	require.Error(t, Connect(context.Background(), app))
	_, statErr := os.Stat(app.MetricFilePath())
	assert.True(t, os.IsNotExist(statErr), "startup stops before the metric file is opened")
}

func TestCheck_ValidConfigurationReturnsTopologyWarnings(t *testing.T) {
	wrap := connectEnv(t)
	tuningDir(t, map[string]string{"config_consumer.yaml": "consumer:\n  return:\n    errors: true\n"})
	svc := schemas.HostConfig{Name: "svc", Port: "8080", Path: "/"}
	cfg := &schemas.KafkaInfo{
		Brokers: []string{"127.0.0.1:1"},
		Consumers: []*schemas.ConsumerConfig{
			{Topics: []string{"orders"}, Group: "g", Service: svc},
			{Topics: []string{"payments"}, Group: "g", Service: svc},
		},
	}

	warnings, err := Check(wrap(cfg))

	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "rebalance together")
}

// The retention line is logged once when a limit is set, and not at all with the defaults.
func TestLogRetention(t *testing.T) {
	logs := &syncLog{}
	old := log.Logger
	log.Logger = zerolog.New(logs)
	t.Cleanup(func() { log.Logger = old })

	logRetention(config.Retention{})
	assert.NotContains(t, logs.String(), "backups are pruned", "nothing is logged without limits")

	logRetention(config.Retention{MaxBackups: 10, MaxAgeDays: 30})
	assert.Contains(t, logs.String(), "at most 10 backups")
	assert.Contains(t, logs.String(), "younger than 30 days")
	assert.Contains(t, logs.String(), "older audit backups are deleted")

	logRetention(config.Retention{MaxAgeDays: 7})
	assert.Contains(t, logs.String(), "no count limit")
}

// slowProducer takes delay for every send and counts them.
type slowProducer struct {
	sarama.SyncProducer
	delay time.Duration
	sends atomic.Int32
}

func (p *slowProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	p.sends.Add(1)
	time.Sleep(p.delay)
	return 0, 0, nil
}

// serveListener starts the public listener with a producer for topic "out" and returns its address.
func serveListener(t *testing.T, producer sarama.SyncProducer) string {
	t.Helper()
	reg := router.NewProducerRegistry()
	reg.Set(producer, []string{"out"})
	srv := server(t, &schemas.KafkaInfo{Producers: []*schemas.ProducerConfig{{Topic: "out"}}}, reg)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return l.Addr().String()
}

func TestHTTPServer_Timeouts(t *testing.T) {
	srv := server(t, &schemas.KafkaInfo{}, router.NewProducerRegistry())

	assert.Equal(t, 10*time.Second, srv.ReadHeaderTimeout)
	assert.Equal(t, 30*time.Second, srv.ReadTimeout)
	assert.Equal(t, 120*time.Second, srv.IdleTimeout)
	assert.Zero(t, srv.WriteTimeout, "a slow publish must not be cut off")
}

func TestHTTPServer_StalledBodyIsClosedAndNothingPublished(t *testing.T) {
	old := httpReadTimeout
	httpReadTimeout = 200 * time.Millisecond
	t.Cleanup(func() { httpReadTimeout = old })
	producer := &slowProducer{}
	addr := serveListener(t, producer)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("POST /api/" + consts.ControllerVersionAPI + "/publish HTTP/1.1\r\nHost: x\r\n" +
		consts.HeaderProduceTopicName + ": out\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"a\":"))
	require.NoError(t, err)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
	began := time.Now()
	_, _ = bufio.NewReader(conn).ReadString('\n') // returns when the server gives up on the body
	assert.Less(t, time.Since(began), 2*time.Second, "the stalled request is closed after the read timeout")
	assert.Zero(t, producer.sends.Load(), "nothing is published from an incomplete body")
}

func TestHTTPServer_SlowPublishStillAnswers(t *testing.T) {
	old := httpReadTimeout
	httpReadTimeout = 200 * time.Millisecond
	t.Cleanup(func() { httpReadTimeout = old })
	producer := &slowProducer{delay: 500 * time.Millisecond}
	addr := serveListener(t, producer)

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/api/"+consts.ControllerVersionAPI+"/publish", strings.NewReader(`{"a":1}`))
	require.NoError(t, err)
	req.Header.Set(consts.HeaderProduceTopicName, "out")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)

	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "the publish result reaches the client after the read timeout")
	assert.Equal(t, int32(1), producer.sends.Load())
}
