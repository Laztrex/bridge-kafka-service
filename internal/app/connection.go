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
)

// newProducerWithRetry is a seam so tests can supply a producer without a broker.
var newProducerWithRetry = createProducerWithRetry

// newConsumerClientWithRetry is a seam so tests can supply a consumer client without a broker.
var newConsumerClientWithRetry = createConsumerClientWithRetry

// runSubscription is a seam so tests can observe whether consumption starts.
var runSubscription = router.RunSubscription

// retryWindow is how long Kafka client creation is retried before giving up.
var retryWindow = 5 * time.Minute

// Connect runs the service until ctx is cancelled or an unrecoverable error is returned.
// HTTP starts first and never waits for Kafka, which connects in the background with retries;
// Consumers start only after the producer settles, so no response goes into an empty registry.
// A consumer that permanently cannot consume (client creation or subscription start fails) is fatal.
// A producer that fails permanently while any active stanza has a response_topic is fatal:
// no consumer reads a record and the process exits non-zero, so the supervisor restarts it.
// Shutdown stops intake, flushes queued responses, then closes the producer and sinks, all
// bounded by timeouts.shutdown.
func Connect(ctx context.Context, cfg *config.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	kafkaConfig := cfg.Kafka

	if err := config.Validate(kafkaConfig); err != nil {
		return fmt.Errorf("configurations are not installed correctly: %w", err)
	}

	if err := config.PrepareUpstreams(kafkaConfig); err != nil {
		return fmt.Errorf("invalid upstream configuration: %w", err)
	}

	logTransaction, err := observability.NewFileTransactionLogger(cfg.MetricFilePath(), cfg.AuditGRPCEndpoint)
	if err != nil {
		return fmt.Errorf("error create NewFileTransactionLogger: %w", err)
	}
	logTransaction.Run()
	logTransaction.RunAudit()
	defer logTransaction.Shutdown()

	registry := router.NewProducerRegistry()

	// errCh holds one report per reporter: every consumer, the HTTP server and
	// the producer, so no reporter ever blocks on a full channel.
	errCh := make(chan error, len(kafkaConfig.Consumers)+2)

	// needsResponses: at least one active stanza must publish a response, so a
	// missing producer would consume records and silently discard their responses.
	needsResponses := false
	for _, c := range kafkaConfig.Consumers {
		if c != nil && len(c.Topics) > 0 && c.ResponseTopic != "" {
			needsResponses = true
			break
		}
	}

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
		if err := startProducer(ctx, kafkaConfig, registry); err != nil && ctx.Err() == nil {
			producerFailed.Store(true)
			if needsResponses {
				errCh <- fmt.Errorf("permanently failed to create producer: %w", err)
			}
		}
	}()

	publishCtx, stopPublish := context.WithCancel(context.Background())
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		handlers.ResponsePublish(publishCtx, logTransaction, registry)
	}()

	toggles := make([]chan struct{}, len(kafkaConfig.Consumers))
	var wg sync.WaitGroup
	for i, consumerConfig := range kafkaConfig.Consumers {
		toggles[i] = make(chan struct{}, 1)
		wg.Add(1)
		go func(conf *schemas.ConsumerConfig, toggle <-chan struct{}) {
			defer wg.Done()

			consumerClient, err := newConsumerClientWithRetry(ctx, kafkaConfig, conf)
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

			if producerFailed.Load() && conf.ResponseTopic != "" {
				log.Warn().Msgf("producer failed: consumer for topics %v will not read, its responses could not be published", conf.Topics)
				return
			}

			log.Info().Msgf("consumer client initialized for topics: %v", conf.Topics)
			ready.consumerUp()
			if err := runSubscription(ctx, toggle, consumerClient, conf, logTransaction); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}(consumerConfig, toggles[i])
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
			for _, t := range toggles {
				select {
				case t <- struct{}{}:
				default:
				}
			}
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
	<-publishDone
	closeProducer(registry)

	log.Info().Msg("shutdown complete")
	return runErr
}

// startProducer creates the single shared producer (when any is configured) and
// binds it to every allowed topic. An error is returned so the caller can decide
// whether it is fatal; a permanent failure is logged and leaves the registry
// empty: publishing then answers 503 while consumption continues.
func startProducer(ctx context.Context, kafkaConfig *schemas.KafkaInfo, registry *router.ProducerRegistry) error {
	if len(kafkaConfig.Producers) == 0 {
		return nil
	}
	producer, err := newProducerWithRetry(ctx, kafkaConfig)
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

func createProducerWithRetry(ctx context.Context, kafkaConfig *schemas.KafkaInfo) (sarama.SyncProducer, error) {
	var producer sarama.SyncProducer
	var err error

	operation := func() error {
		producer, err = router.NewProducer(kafkaConfig)
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

func createConsumerClientWithRetry(ctx context.Context, kafkaConfig *schemas.KafkaInfo, consumerConfig *schemas.ConsumerConfig) (sarama.Client, error) {
	var consumerClient sarama.Client
	var err error

	operation := func() error {
		consumerClient, err = router.NewConsumerClient(kafkaConfig, consumerConfig)
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
		ReadHeaderTimeout: 10 * time.Second,
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
