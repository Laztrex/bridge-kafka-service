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

// retryWindow is how long Kafka client creation is retried before giving up.
var retryWindow = 5 * time.Minute

// Connect runs the service until ctx is cancelled or an unrecoverable error is returned.
// HTTP starts first and never waits for Kafka, which connects in the background with retries;
// Consumers start only after the producer settles, so no response goes into an empty registry.
// A consumer that permanently cannot consume (client creation or subscription start fails) is fatal.
// A producer that fails permanently is fatal whenever producers are configured: no consumer reads a
// record and the process exits non-zero, so the supervisor restarts it instead of leaving it never
// ready. SIGUSR1 toggles the process-wide pause that every subscription follows.
// Shutdown stops intake, flushes queued responses, then closes the producer and sinks, all
// bounded by timeouts.shutdown.
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

	ready := newReadiness(registry, kafkaConfig)
	srv := newHTTPServer(logTransaction, registry, kafkaConfig, ready, cfg.HTTPAddr)
	go func() {
		log.Info().Msgf("starting HTTP server at %s...", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	startPprof(ctx, cfg.PprofAddr)

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
			errCh <- fmt.Errorf("permanently failed to create producer: %w", err)
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
		}(queues[i], transport.PublisherCount(c), ready.pipelines[i].state)
	}

	var wg sync.WaitGroup
	for i, consumerConfig := range consumers {
		wg.Add(1)
		go func(conf *schemas.ConsumerConfig, queue chan<- *schemas.ConsumerEvent, state *transport.PipelineState) {
			defer wg.Done()

			consumerClient, err := newConsumerClientWithRetry(ctx, kafkaConfig, conf, consumerTuning)
			if err != nil {
				if ctx.Err() == nil {
					errCh <- fmt.Errorf("permanently failed to create consumer client for %v: %w", conf.Topics, err)
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
			ready.consumerUp()
			if err := runSubscription(ctx, ready.pause, consumerClient, conf, queue, logTransaction, state); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}(consumerConfig, queues[i], ready.pipelines[i].state)
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
			ready.pause.Toggle()
		}
	}

	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer stop()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("http server shutdown")
	}
	wg.Wait()
	producerBg.Wait()
	stopPublish()
	publishers.Wait()
	closeProducer(registry)

	log.Info().Msg("shutdown complete")
	return runErr
}

// startProducer creates the single shared producer (when any is configured) and
// binds it to every allowed topic. An error is returned so the caller can decide
// whether it is fatal; a permanent failure is logged and leaves the registry
// empty: publishing then answers 503 while consumption continues.
func startProducer(ctx context.Context, kafkaConfig *schemas.KafkaInfo, tuning *router.Tuning, registry *router.ProducerRegistry) error {
	if len(kafkaConfig.Producers) == 0 {
		return nil
	}
	producer, err := newProducerWithRetry(ctx, kafkaConfig, tuning)
	if err != nil {
		if ctx.Err() == nil {
			log.Error().Msgf("permanently failed to create producer: %v", err)
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
	var err error

	operation := func() error {
		producer, err = router.NewProducer(kafkaConfig, tuning)
		if err != nil {
			return fmt.Errorf("could not create producer: %v", err)
		}
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = retryWindow

	err = backoff.RetryNotify(operation, backoff.WithContext(expBackoff, ctx), func(err error, d time.Duration) {
		log.Warn().Msgf("failed to create producer: %v. Retrying in %s", err, d)
	})

	return producer, err
}

func createConsumerClientWithRetry(ctx context.Context, kafkaConfig *schemas.KafkaInfo, consumerConfig *schemas.ConsumerConfig, tuning *router.Tuning) (sarama.Client, error) {
	var consumerClient sarama.Client
	var err error

	operation := func() error {
		consumerClient, err = router.NewConsumerClient(kafkaConfig, consumerConfig, tuning)
		if err != nil {
			return fmt.Errorf("could not create consumer for topics %v: %v", consumerConfig.Topics, err)
		}
		return nil
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = retryWindow

	err = backoff.RetryNotify(operation, backoff.WithContext(expBackoff, ctx), func(err error, d time.Duration) {
		log.Warn().Msgf("consumer client connection failed: %v. Retrying in %s", err, d)
	})

	return consumerClient, err
}

// registerRoutes mounts the POST-only publish endpoint (other methods get 405).
func registerRoutes(mux *http.ServeMux, logTransaction schemas.TransactionLogger, registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo) {
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
		if !registry.Ready() {
			http.Error(w, "Publish functionality is not ready: Kafka producer is not connected", http.StatusServiceUnavailable)
			return
		}
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
func newHTTPServer(logTransaction schemas.TransactionLogger, registry *router.ProducerRegistry, kafkaConfig *schemas.KafkaInfo, ready *readiness, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handlers.Health)
	mux.HandleFunc("GET /healthz", handlers.Health)
	mux.HandleFunc("GET /readyz", ready.serveReadyz)
	mux.HandleFunc("GET /status", ready.serveStatus)
	registerRoutes(mux, logTransaction, registry, kafkaConfig)

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}

// startPprof serves the profiling handlers on their own address, only when
// pprof.addr is configured, and stops with ctx.
func startPprof(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	if host, _, err := net.SplitHostPort(addr); err != nil || !isLoopback(host) {
		log.Warn().Msgf("pprof.addr %s is not a loopback address: profiling is reachable from the network", addr)
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
