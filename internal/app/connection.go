package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"net"
	"net/http/pprof"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/config"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/handlers"
	"bridge-kafka-service/internal/middlewares"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/router"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// newProducerWithRetry is a seam so tests can supply a producer without a broker.
var newProducerWithRetry = createProducerWithRetry

// newConsumerClientWithRetry is a seam so tests can supply a consumer client without a broker.
var newConsumerClientWithRetry = createConsumerClientWithRetry

// runSubscription is a seam so tests can observe whether consumption starts.
var runSubscription = router.RunSubscription

// responsePublish is a seam so tests can observe the per-pipeline publishers.
var responsePublish = handlers.ResponsePublish

// newProducerAttempt is one producer construction attempt, a seam for tests.
var newProducerAttempt = router.NewProducer

// newConsumerClientAttempt is one consumer client construction attempt, a seam for tests.
var newConsumerClientAttempt = router.NewConsumerClient

// Connect runs the service until ctx is cancelled or an unrecoverable error is returned.
// HTTP starts first and never waits for Kafka, which connects in the background with retries:
// producer and consumer clients retry with exponential backoff and jitter until shutdown, so an
// unreachable broker leaves the service up and not ready, with /readyz naming the missing
// clients (LIFE-3). Consumers start only after the producer settles, so no response goes into
// an empty registry. A subscription that cannot start for a reason retrying cannot fix — its
// upstream cannot be constructed, or its consumer group cannot be created from a connected
// client — is fatal: the process exits non-zero instead of reporting ready without reading.
// SIGUSR1 toggles the process-wide pause that every subscription follows.
// Shutdown answers 503 on publish and /readyz, keeps /healthz, /health and /status up until the
// drain ends, and closes the HTTP server last, all bounded by timeouts.shutdown.
func Connect(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	kafkaConfig := cfg.Kafka

	warnings, consumerTuning, producerTuning, err := check(kafkaConfig)
	for _, w := range warnings {
		log.Warn().Msg(w)
	}
	if err != nil {
		return err
	}

	consumers := activeConsumers(kafkaConfig)
	for i, c := range kafkaConfig.Consumers {
		if c == nil || len(c.Topics) == 0 {
			log.Info().Msgf("consumer #%d has no topics: disabled", i+1)
		}
	}

	logRetention(cfg.Retention)
	logTransaction, err := observability.NewFileTransactionLogger(cfg.MetricFilePath(), cfg.AuditGRPCEndpoint,
		observability.WithRetention(cfg.Retention.MaxBackups, cfg.Retention.MaxAgeDays))
	if err != nil {
		return fmt.Errorf("error create NewFileTransactionLogger: %w", err)
	}
	logTransaction.Run()
	logTransaction.RunAudit()
	defer logTransaction.Shutdown()

	registry := router.NewProducerRegistry()

	// errCh holds one report per reporter: every consumer, the HTTP server and
	// the producer, so no reporter ever blocks on a full channel.
	errCh := make(chan error, len(consumers)+2)

	runtime := newRuntimeState(registry, kafkaConfig)
	srv := newHTTPServer(logTransaction, registry, kafkaConfig, runtime, cfg.HealthAddr)
	go func() {
		log.Info().Msgf("starting HTTP server at %s...", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	startPprof(ctx, cfg.DebugAddr)

	// producerFailed records the producer outcome before producersSettled closes,
	// so a consumer never starts reading when its responses could not be published.
	var producerFailed atomic.Bool
	producersSettled := make(chan struct{})
	var producerBg sync.WaitGroup
	producerBg.Add(1)
	go func() {
		defer producerBg.Done()
		defer close(producersSettled)
		if err := startProducer(ctx, kafkaConfig, producerTuning, registry); err != nil && ctx.Err() == nil {
			producerFailed.Store(true)
			errCh <- fmt.Errorf("producer creation failed: %w", err)
		}
	}()

	// Each stanza with a response_topic gets its own response queue and publishers, so a
	// response topic that fails holds back only its own pipeline.
	publishCtx, stopPublish := context.WithCancel(context.Background())
	var publishers sync.WaitGroup
	queues := make([]chan *schemas.ConsumerEvent, len(consumers))
	for i, c := range consumers {
		if c.ResponseTopic == "" {
			continue
		}
		queues[i] = transport.NewResponseQueue()
		publishers.Add(1)
		go func(queue <-chan *schemas.ConsumerEvent, workers int, state *transport.PipelineState) {
			defer publishers.Done()
			responsePublish(publishCtx, queue, workers, logTransaction, registry, state)
		}(queues[i], transport.PublisherCount(c), runtime.pipelines[i].state)
	}

	var wg sync.WaitGroup
	for i, consumerConfig := range consumers {
		wg.Add(1)
		go func(conf *schemas.ConsumerConfig, queue chan<- *schemas.ConsumerEvent, state *transport.PipelineState) {
			defer wg.Done()

			consumerClient, err := newConsumerClientWithRetry(ctx, kafkaConfig, conf, consumerTuning)
			if err != nil {
				if ctx.Err() == nil {
					errCh <- fmt.Errorf("consumer client creation failed for %v: %w", conf.Topics, err)
				}
				return
			}
			defer func() {
				if err := consumerClient.Close(); err != nil {
					log.Error().Msgf("error closing consumer client: %v", err)
				}
			}()

			select {
			case <-producersSettled:
			case <-ctx.Done():
				return
			}

			if producerFailed.Load() {
				log.Warn().Msgf("producer failed: consumer for topics %v will not read, the process is stopping", conf.Topics)
				return
			}

			log.Info().Msgf("consumer client initialized for topics: %v", conf.Topics)
			runtime.consumerUp()
			if err := runSubscription(ctx, runtime.pause, consumerClient, conf, queue, logTransaction, state); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}(consumerConfig, queues[i], runtime.pipelines[i].state)
	}

	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	defer signal.Stop(usr1)

	var runErr error
loop:
	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("terminating: shutdown requested")
			break loop
		case runErr = <-errCh:
			log.Error().Err(runErr).Msg("terminating: unrecoverable error")
			break loop
		case <-usr1:
			runtime.pause.Toggle()
		}
	}

	// Shutdown keeps the probes answering (design D4): the draining flag makes the publish
	// endpoint and /readyz answer 503, intake stops, in-flight publish requests finish, then
	// the pipelines, the response publishers and the producer close — every step bounded by
	// timeouts.shutdown — and the HTTP server shuts down last with the time that remains.
	runtime.beginDrain()
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer stop()
	waitShutdownStep(shutdownCtx, "in-flight publish requests", runtime.inFlightPublish.Wait)
	waitShutdownStep(shutdownCtx, "consumer pipelines", wg.Wait)
	waitShutdownStep(shutdownCtx, "producer", producerBg.Wait)
	stopPublish()
	waitShutdownStep(shutdownCtx, "response publishers", publishers.Wait)
	closeProducer(registry)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("http server shutdown")
	}

	log.Info().Msg("shutdown complete")
	return runErr
}

// waitShutdownStep waits for one shutdown step, bounded by the shutdown deadline: a step that
// outlives it is logged and the shutdown moves on, so one stuck step cannot hold the process
// past timeouts.shutdown.
func waitShutdownStep(ctx context.Context, step string, wait func()) {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		log.Error().Msgf("shutdown did not finish %s within timeouts.shutdown", step)
	}
}

// startProducer creates the single shared producer (when any is configured) and
// binds it to every allowed topic. With retries unbounded by a window, an error
// means shutdown was requested; it is still returned so the caller can record the
// outcome. A producer that never connects leaves the registry empty: publishing
// answers 503 and consumers with a response_topic never start reading.
func startProducer(ctx context.Context, kafkaConfig *schemas.KafkaInfo, tuning *router.Tuning, registry *router.ProducerRegistry) error {
	if len(kafkaConfig.Producers) == 0 {
		return nil
	}
	producer, err := newProducerWithRetry(ctx, kafkaConfig, tuning)
	if err != nil {
		if ctx.Err() == nil {
			log.Error().Msgf("producer creation failed: %v", err)
		}
		return err
	}
	topics := make([]string, 0, len(kafkaConfig.Producers))
	for _, p := range kafkaConfig.Producers {
		topics = append(topics, p.Topic)
		log.Info().Msgf("producer initialized for topic: %s", p.Topic)
	}
	registry.Set(producer, topics)
	return nil
}

// activeConsumers returns the consumer stanzas that have topics. A stanza without topics is
// disabled: it gets no Kafka client, no subscription and no place in readiness.
func activeConsumers(kafkaConfig *schemas.KafkaInfo) []*schemas.ConsumerConfig {
	var active []*schemas.ConsumerConfig
	for _, c := range kafkaConfig.Consumers {
		if c != nil && len(c.Topics) > 0 {
			active = append(active, c)
		}
	}
	return active
}

// Check runs every configuration check startup runs, without connecting anywhere or opening log
// files: the Kafka setup, the upstream settings, the consumer topology and the Kafka tuning files.
// It returns the warnings and one error joining every problem found.
func Check(cfg *config.Config) (warnings []string, err error) {
	warnings, _, _, err = check(cfg.Kafka)
	return warnings, err
}

// check is Check that also returns the loaded tuning, for Connect. Every part runs even when an
// earlier one failed, and each keeps its own error prefix.
func check(kafkaConfig *schemas.KafkaInfo) (warnings []string, consumer, producer *router.Tuning, err error) {
	var errs []error
	if err := config.Validate(kafkaConfig); err != nil {
		errs = append(errs, fmt.Errorf("configurations are not installed correctly: %w", err))
	}
	if err := config.PrepareUpstreams(kafkaConfig); err != nil {
		errs = append(errs, fmt.Errorf("invalid upstream configuration: %w", err))
	}
	warnings, err = config.Topology(kafkaConfig)
	if err != nil {
		errs = append(errs, fmt.Errorf("invalid consumer topology: %w", err))
	}
	consumer, producer, err = loadTuning(kafkaConfig)
	if err != nil {
		errs = append(errs, err)
	}
	return warnings, consumer, producer, errors.Join(errs...)
}

// loadTuning loads the Kafka client tuning files once, before anything starts, and only those
// whose client is needed: the consumer file with an active stanza, the producer file with a
// producer. A missing or broken file can never heal, so it stops startup instead of being retried;
// both files are checked even when the first is broken.
func loadTuning(kafkaConfig *schemas.KafkaInfo) (consumer, producer *router.Tuning, err error) {
	var errs []error
	if len(activeConsumers(kafkaConfig)) > 0 {
		if consumer, err = router.LoadConsumerTuning(); err != nil {
			errs = append(errs, fmt.Errorf("invalid Kafka consumer tuning: %w", err))
		}
	}
	if len(kafkaConfig.Producers) > 0 {
		if producer, err = router.LoadProducerTuning(); err != nil {
			errs = append(errs, fmt.Errorf("invalid Kafka producer tuning: %w", err))
		}
	}
	if len(errs) > 0 {
		return nil, nil, errors.Join(errs...)
	}
	return consumer, producer, nil
}

// logRetention states the backup retention once at startup when a limit is set, since it means
// older audit entries are deleted; with no limit it logs nothing.
func logRetention(r config.Retention) {
	if r.MaxBackups == 0 && r.MaxAgeDays == 0 {
		return
	}
	count, age := "no count limit", "no age limit"
	if r.MaxBackups > 0 {
		count = fmt.Sprintf("at most %d backups", r.MaxBackups)
	}
	if r.MaxAgeDays > 0 {
		age = fmt.Sprintf("backups younger than %d days", r.MaxAgeDays)
	}
	log.Info().Msgf("metric and audit backups are pruned: keeping %s and %s of each file; older audit backups are deleted", count, age)
}

// closeProducer closes the shared producer exactly once, however many topics map to it.
func closeProducer(registry *router.ProducerRegistry) {
	closed := map[sarama.SyncProducer]bool{}
	for _, p := range registry.Producers() {
		if closed[p] {
			continue
		}
		closed[p] = true
		if err := p.Close(); err != nil {
			log.Error().Msgf("error closing producer: %v", err)
		}
	}
}

func createProducerWithRetry(ctx context.Context, kafkaConfig *schemas.KafkaInfo, tuning *router.Tuning) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer

	operation := func() error {
		p, err := newProducerAttempt(kafkaConfig, tuning)
		if err != nil {
			return fmt.Errorf("could not create producer: %v", err)
		}
		producer = p
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = 0 // no retry window: retry until the context is cancelled (LIFE-3)

	err := backoff.RetryNotify(operation, backoff.WithContext(expBackoff, ctx), func(err error, d time.Duration) {
		log.Warn().Msgf("failed to create producer: %v. Retrying in %s", err, d)
	})

	return producer, err
}

func createConsumerClientWithRetry(ctx context.Context, kafkaConfig *schemas.KafkaInfo, consumerConfig *schemas.ConsumerConfig, tuning *router.Tuning) (sarama.Client, error) {
	var consumerClient sarama.Client

	operation := func() error {
		c, err := newConsumerClientAttempt(kafkaConfig, consumerConfig, tuning)
		if err != nil {
			return fmt.Errorf("could not create consumer for topics %v: %v", consumerConfig.Topics, err)
		}
		consumerClient = c
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = 0 // no retry window: retry until the context is cancelled (LIFE-3)

	err := backoff.RetryNotify(operation, backoff.WithContext(expBackoff, ctx), func(err error, d time.Duration) {
		log.Warn().Msgf("consumer client connection failed: %v. Retrying in %s", err, d)
	})

	return consumerClient, err
}

// registerRoutes mounts the POST-only publish endpoint (other methods get 405).
func registerRoutes(mux *http.ServeMux, logTransaction schemas.TransactionLogger, registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo, runtime *runtimeState) {
	pattern := fmt.Sprintf("POST /api/%v/publish", consts.ControllerVersionAPI)

	if len(kafkaConfig.Producers) == 0 {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Publish functionality is disabled: no Kafka producers configured", http.StatusServiceUnavailable)
		})
		log.Warn().Msg("no producers configured, publish endpoint disabled")
		return
	}

	publish := middlewares.RequestIdMiddleware(handlers.NewHandlers(logTransaction, registry))
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if runtime.draining.Load() {
			http.Error(w, "service is shutting down", http.StatusServiceUnavailable)
			return
		}
		if !registry.Ready() {
			http.Error(w, "Publish functionality is not ready: Kafka producer is not connected", http.StatusServiceUnavailable)
			return
		}
		runtime.inFlightPublish.Add(1)
		defer runtime.inFlightPublish.Done()
		publish.ServeHTTP(w, r)
	})
	log.Info().Msg("publish endpoint registered")
}

// Public listener timeouts. A whole request (headers and the body of up to 2 MB) must arrive
// within httpReadTimeout, and an idle keep-alive connection is closed after httpIdleTimeout, above
// common load-balancer idle timeouts. There is deliberately no write timeout: a publish may wait
// for Kafka retries, and cutting the connection after the record may have been written would leave
// the client unsure whether to retry. httpReadTimeout is a variable so tests can shorten it.
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

var httpReadTimeout = 30 * time.Second

// newHTTPServer serves the publish API and probes (/health is the legacy alias of /healthz) on
// its own mux, which keeps the pprof handlers off the public listener.
func newHTTPServer(logTransaction schemas.TransactionLogger, registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo, runtime *runtimeState, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handlers.Health)
	mux.HandleFunc("GET /healthz", handlers.Health)
	mux.HandleFunc("GET /readyz", runtime.serveReadyz)
	mux.HandleFunc("GET /status", runtime.serveStatus)
	registerRoutes(mux, logTransaction, registry, kafkaConfig, runtime)

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

// startPprof serves the profiling handlers on their own address, only when
// debug.addr is configured, and stops with ctx.
func startPprof(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	if host, _, err := net.SplitHostPort(addr); err != nil || !isLoopback(host) {
		log.Warn().Msgf("debug.addr %s is not a loopback address: profiling is reachable from the network", addr)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		log.Info().Msgf("pprof listening at %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Msgf("pprof server: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
}

// isLoopback reports whether host (from host:port) only accepts local connections.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
