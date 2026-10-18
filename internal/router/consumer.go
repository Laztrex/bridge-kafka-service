package router

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"

	"bridge-kafka-service/internal/codec"
	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/observability"
	"bridge-kafka-service/internal/schemas"
	"bridge-kafka-service/internal/transport"
)

// Seams overridden only by tests: a real consumer group needs a live broker.
var (
	newConsumerGroup   = sarama.NewConsumerGroupFromClient
	newClient          = sarama.NewClient
	consumerConfigPath = consts.ConfPathConsumer
)

type configConsumerUpdater struct {
	Version                   string
	OffsetsAutoCommitInterval time.Duration `yaml:"OffsetsAutoCommitInterval"`
}

type Consumer struct {
	config    *schemas.ConsumerConfig
	logger    *observability.FileTransactionLogger
	out       chan<- *schemas.ConsumerEvent // records handed to the response handler
	ready     chan bool
	readyOnce sync.Once
}

// RunSubscription runs the consume loop and the response handler of one subscription until
// parent is cancelled or consumption ends. Each value on toggle flips pause/resume. On stop,
// consumption is cancelled before the event channel is closed, since ConsumeClaim may still send.
// A non-nil return means a permanent inability to start consuming (the upstream cannot be
// constructed, the consumer group cannot be created): the error names the stanza's topics and
// the caller treats it as fatal. Every clean stop (context cancelled, group closed, normal
// end) returns nil; transient consume-loop errors keep their 5-second retry.
func RunSubscription(
	parent context.Context,
	toggle <-chan struct{},
	consumerClient sarama.Client,
	consumerConfig *schemas.ConsumerConfig,
	logger *observability.FileTransactionLogger,
) error {
	consumptionIsPaused := false

	upstream, err := transport.NewUpstream(consumerConfig)
	if err != nil {
		log.Error().Msgf("cannot create upstream for topics %v: %v", consumerConfig.Topics, err)
		return fmt.Errorf("subscription for topics %v: cannot create upstream: %w", consumerConfig.Topics, err)
	}
	if closer, ok := upstream.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	ch := make(chan *schemas.ConsumerEvent, 1000)
	consumer := &Consumer{config: consumerConfig, logger: logger, out: ch, ready: make(chan bool)}

	ctx, cancel := context.WithCancel(parent)

	defer cancel()

	wg := &sync.WaitGroup{}
	consumeDone := make(chan struct{})

	client, err := newConsumerGroup(consumerConfig.Group, consumerClient)
	if err != nil {
		log.Error().Msgf("error creating consumer group client: %v", err)
		return fmt.Errorf("subscription for topics %v: cannot create consumer group: %w", consumerConfig.Topics, err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Error().Msgf("error closing consumer group: %v", err)
		}
	}()

	processingCtx, processingCancel := context.WithCancel(context.Background())
	defer processingCancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if consumerConfig.Service.MaxConn > 0 {
			transport.ConsumerResponseSlow(logger, consumerConfig, upstream, ch, processingCtx)
		} else {
			transport.ConsumerResponseFast(logger, consumerConfig, upstream, ch, processingCtx)
		}
		log.Info().Msg("response handler terminated")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(consumeDone)
		defer cancel()

		log.Info().Msgf("starting consumption for topics %v", consumerConfig.Topics)

		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("consumption stopped: context cancelled")
				return
			default:
				if err := client.Consume(ctx, consumerConfig.Topics, consumer); err != nil {
					if err == sarama.ErrClosedConsumerGroup {
						log.Info().Msg("consumer group closed")
						return
					}
					log.Error().Msgf("error from consumer: %v", err)

					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Second):
						continue
					}
				}

				if ctx.Err() != nil {
					return
				}
			}
		}
	}()

	select {
	case <-consumer.ready:
		log.Info().Msgf("consumer for topics %v up and running", consumerConfig.Topics)
	case <-ctx.Done():
		log.Info().Msg("terminating before consumer became ready")
	}

	for running := true; running; {
		select {
		case <-ctx.Done():
			log.Info().Msg("terminating: context cancelled")
			running = false
		case <-toggle:
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}

	log.Info().Msg("shutting down consumer...")

	cancel()
	<-consumeDone
	close(ch)
	wg.Wait()

	log.Info().Msg("consumer fully terminated")
	return nil
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if client == nil {
		log.Warn().Msg("cannot toggle consumption flow: consumer group client is not available")
		return
	}

	if *isPaused {
		client.ResumeAll()
		log.Info().Msg("resuming consumption")
	} else {
		client.PauseAll()
		log.Info().Msg("pausing consumption")
	}
	*isPaused = !*isPaused
}

func (k *Consumer) Setup(sarama.ConsumerGroupSession) error {
	k.readyOnce.Do(func() {
		close(k.ready)
	})
	return nil
}

func (k *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Info().Msg("cleanup completed")
	return nil
}

// pendingOffset is a consumed message whose processing has not settled yet.
type pendingOffset struct {
	msg  *sarama.ConsumerMessage
	done bool
}

// offsetTracker marks offsets only after the message has been fully processed
// (service call finished and the response handed to Kafka, or processing failed),
// and only in consumption order: an offset is never committed while an earlier
// one is still in flight, so a crash re-delivers unfinished work instead of losing it.
type offsetTracker struct {
	mu      sync.Mutex
	session sarama.ConsumerGroupSession
	pending []*pendingOffset
}

// track registers msg and returns an idempotent ack func that settles it.
func (t *offsetTracker) track(msg *sarama.ConsumerMessage) func() {
	p := &pendingOffset{msg: msg}

	t.mu.Lock()
	t.pending = append(t.pending, p)
	t.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			p.done = true
			for len(t.pending) > 0 && t.pending[0].done {
				t.session.MarkMessage(t.pending[0].msg, "")
				t.pending[0] = nil
				t.pending = t.pending[1:]
			}
		})
	}
}

// ConsumeClaim hands every record to the response handler. Each record gets fresh objects (they
// escape into other goroutines), and its audit duration spans consumption to hand-over.
func (k *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	tracker := &offsetTracker{session: session}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Info().Msg("message channel was closed")
				return nil
			}

			transaction := k.logger

			body := &schemas.RequestBody{}
			eventConsume := &schemas.Event{EventType: consts.EventConsume}
			transaction.Start(eventConsume)
			ack := tracker.track(message)

			value := codec.PrepareConsumeMsg(message, eventConsume, body)

			value.ResponseTopic = k.config.ResponseTopic
			value.Config = k.config
			value.SetAck(ack)

			value.Values = &schemas.ContextValues{
				RequestId: eventConsume.RequestId,
				Body:      body,
				Topic:     message.Topic,
				Path:      value.Config.Service.Path,
			}

			select {
			case <-session.Context().Done():
				return nil
			case k.out <- value:
			}

			transaction.Close(eventConsume)

			observability.LogAuditEventWithLatency(
				transaction,
				consts.EventNameBusinessRequestReceived,
				eventConsume.RequestId,
				body,
				consts.AccessLogTypeDownstreamStart,
				value.Config.Service.Path,
				eventConsume.Started,
			)

		case <-session.Context().Done():
			return nil
		}
	}
}

// NewConsumerClient builds a sarama.Client wired with the application's TLS
// settings and consumer-group tunables pulled from YAML configuration. Auto-commit is always on:
// settlement only marks offsets, which sarama sends to the broker only when auto-commit is enabled.
// TODO: move the rebalance strategy to the config yaml.
func NewConsumerClient(conf *schemas.KafkaInfo, consumerConfig *schemas.ConsumerConfig) (sarama.Client, error) {
	cfg := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		version = sarama.V2_7_0_0
	}

	cfg.Version = version
	cfg.ClientID = fmt.Sprintf("%s-%d", consumerConfig.ClientId, os.Getpid())

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	cfg.Consumer.Group.Session.Timeout = 60 * time.Second
	cfg.Consumer.Group.Heartbeat.Interval = 10 * time.Second
	cfg.Consumer.MaxProcessingTime = 10 * time.Second

	raw, err := os.ReadFile(consumerConfigPath)
	if err != nil {
		return nil, fmt.Errorf("unable to locate configuration file: %w", err)
	}

	err = yaml.Unmarshal(raw, &cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to parse configuration: %w", err)
	}

	cfgUpd := configConsumerUpdater{}
	err = yaml.Unmarshal(raw, &cfgUpd)
	if err != nil {
		return nil, fmt.Errorf("unable to parse configuration: %w", err)
	}

	if cfgUpd.OffsetsAutoCommitInterval > 0 {
		cfg.Consumer.Offsets.AutoCommit.Interval = cfgUpd.OffsetsAutoCommitInterval
	}
	cfg.Consumer.Offsets.AutoCommit.Enable = true

	if cfg.Net.TLS.Enable {
		cfg.Net.TLS.Config = conf.TLSConf
	}

	return newClient(conf.Brokers, cfg)
}
