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

type Consumer struct {
	config    *schemas.ConsumerConfig
	logger    *observability.FileTransactionLogger
	out       chan<- *schemas.ConsumerEvent // records handed to the response handler
	ready     chan bool
	readyOnce sync.Once

	// settleWait bounds how long an ending claim waits for its records to settle (zero: no wait).
	settleWait time.Duration
	// deadline, once set by a stopping pipeline, also bounds that wait.
	deadline *transport.DrainDeadline
	// state receives the pipeline's uncommitted count (nil: not reported).
	state *transport.PipelineState
	// pause is the process-wide pause state (nil: never paused); group is the consumer group it
	// pauses the claims of.
	pause *Pause
	group sarama.ConsumerGroup
}

// RunSubscription runs the consume loop and the response handler of one subscription until
// parent is cancelled or consumption ends; responses go to queue, the pipeline's own
// publication queue (nil when the stanza has no response_topic), and its runtime state goes to state
// (nil: not reported). The subscription follows pause, the process-wide pause state. On stop,
// consumption is cancelled before the event channel is closed, since ConsumeClaim may still send.
// A non-nil return means a permanent inability to start consuming (the upstream cannot be
// constructed, the consumer group cannot be created): the error names the stanza's topics and
// the caller treats it as fatal. Every clean stop (context cancelled, group closed, normal
// end) returns nil; transient consume-loop errors keep their 5-second retry.
func RunSubscription(
	parent context.Context,
	pause *Pause,
	consumerClient sarama.Client,
	consumerConfig *schemas.ConsumerConfig,
	queue chan<- *schemas.ConsumerEvent,
	logger *observability.FileTransactionLogger,
	state *transport.PipelineState,
) error {
	upstream, err := transport.NewUpstream(consumerConfig)
	if err != nil {
		log.Error().Msgf("cannot create upstream for topics %v: %v", consumerConfig.Topics, err)
		return fmt.Errorf("subscription for topics %v: cannot create upstream: %w", consumerConfig.Topics, err)
	}
	if closer, ok := upstream.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}

	ch := make(chan *schemas.ConsumerEvent, 1000)
	drainDeadline := &transport.DrainDeadline{}
	consumer := &Consumer{
		config: consumerConfig, logger: logger, out: ch, ready: make(chan bool),
		settleWait: min(transport.DrainWindow(consumerConfig), consumerClient.Config().Consumer.Group.Rebalance.Timeout),
		deadline:   drainDeadline,
		state:      state,
		pause:      pause,
	}

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
	consumer.group = client
	pause.Register(client)
	defer pause.Unregister(client)

	processingCtx, processingCancel := context.WithCancel(context.Background())
	defer processingCancel()
	processingCtx = transport.WithDrainDeadline(processingCtx, drainDeadline)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if consumerConfig.Service.MaxConn > 0 {
			transport.ConsumerResponseSlow(logger, consumerConfig, upstream, ch, queue, processingCtx, state)
		} else {
			transport.ConsumerResponseFast(logger, consumerConfig, upstream, ch, queue, processingCtx, state)
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

	<-ctx.Done()
	log.Info().Msg("terminating: context cancelled")

	log.Info().Msg("shutting down consumer...")

	drainDeadline.Start(transport.DrainWindow(consumerConfig))
	cancel()
	<-consumeDone
	close(ch)
	wg.Wait()

	log.Info().Msg("consumer fully terminated")
	return nil
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
// It also counts the records handed out and not yet settled or abandoned, so an ending
// claim can wait for them before sarama makes its final commit, and reports the records not yet
// marked to state as the pipeline's uncommitted count.
type offsetTracker struct {
	mu          sync.Mutex
	session     sarama.ConsumerGroupSession
	pending     []*pendingOffset
	outstanding int
	idle        chan struct{} // closed when outstanding drops to zero while a claim waits
	state       *transport.PipelineState
	ended       bool // the claim has ended: its unmarked records no longer count as uncommitted
}

// track registers msg and returns its ack, which settles it, and its abandon, which gives it up
// unsettled (it is then redelivered). Only the first of the two calls has an effect.
func (t *offsetTracker) track(msg *sarama.ConsumerMessage) (ack, abandon func()) {
	p := &pendingOffset{msg: msg}

	t.mu.Lock()
	t.pending = append(t.pending, p)
	t.outstanding++
	t.state.AddUncommitted(1)
	t.mu.Unlock()

	var once sync.Once
	ack = func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()

			p.done = true
			for len(t.pending) > 0 && t.pending[0].done {
				t.session.MarkMessage(t.pending[0].msg, "")
				t.pending[0] = nil
				t.pending = t.pending[1:]
				if !t.ended {
					t.state.AddUncommitted(-1)
				}
			}
			t.release()
		})
	}
	abandon = func() {
		once.Do(func() {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.release()
		})
	}
	return ack, abandon
}

// release accounts for one settled or abandoned record; t.mu must be held.
func (t *offsetTracker) release() {
	t.outstanding--
	if t.outstanding == 0 && t.idle != nil {
		close(t.idle)
		t.idle = nil
	}
}

// end stops counting the claim's unmarked records as uncommitted: they are redelivered to the
// partition's next owner.
func (t *offsetTracker) end() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.ended {
		t.ended = true
		t.state.AddUncommitted(-int64(len(t.pending)))
	}
}

// waitSettled waits until every handed-out record has settled or been abandoned, at most d.
// It returns the number of records still outstanding.
func (t *offsetTracker) waitSettled(d time.Duration) int {
	t.mu.Lock()
	if t.outstanding == 0 || d <= 0 {
		n := t.outstanding
		t.mu.Unlock()
		return n
	}
	idle := make(chan struct{})
	t.idle = idle
	t.mu.Unlock()

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-idle:
	case <-timer.C:
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.idle = nil
	return t.outstanding
}

// ConsumeClaim hands every record to the response handler. Each record gets fresh objects (they
// escape into other goroutines), and its audit duration spans consumption to hand-over. When the
// session ends it stops reading and waits, up to settleWait, for the records it handed out to
// settle or be abandoned, so their marks reach sarama's final commit instead of being dropped.
// While the process is paused the claim's partition is paused and no record is handed out: a
// received record waits, untracked, until resume, and is redelivered if the session ends first.
func (k *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	tracker := &offsetTracker{session: session, state: k.state}
	defer k.waitForClaim(tracker, claim)
	k.pause.PauseClaim(k.group, claim.Topic(), claim.Partition())

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Info().Msg("message channel was closed")
				return nil
			}
			if !k.pause.Wait(session.Context()) {
				return nil
			}

			transaction := k.logger

			body := &schemas.RequestBody{}
			eventConsume := &schemas.Event{EventType: consts.EventConsume}
			transaction.Start(eventConsume)
			ack, abandon := tracker.track(message)

			value := codec.PrepareConsumeMsg(message, eventConsume, body)

			value.ResponseTopic = k.config.ResponseTopic
			value.Config = k.config
			value.ClaimCtx = session.Context()
			value.SetAck(ack)
			value.SetAbandon(abandon)

			value.Values = &schemas.ContextValues{
				RequestId: eventConsume.RequestId,
				Body:      body,
				Topic:     message.Topic,
				Path:      value.Config.Service.Path,
			}

			select {
			case <-session.Context().Done():
				abandon()
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

// waitForClaim holds an ending claim until its records have settled or been abandoned, bounded by
// settleWait and, on shutdown, by the pipeline's drain deadline; records still outstanding after
// that are redelivered and no longer count as uncommitted.
func (k *Consumer) waitForClaim(tracker *offsetTracker, claim sarama.ConsumerGroupClaim) {
	defer tracker.end()
	wait := k.settleWait
	if at, ok := k.deadline.At(); ok {
		wait = min(wait, time.Until(at))
	}
	if left := tracker.waitSettled(wait); left > 0 {
		log.Warn().Msgf("claim %s/%d ended with %d records still in flight: they will be redelivered",
			claim.Topic(), claim.Partition(), left)
	}
}

// NewConsumerClient builds a sarama.Client wired with the application's TLS settings: code
// defaults first (range rebalance strategy, 60s session timeout, 10s heartbeat, 10s max
// processing time), then the consumer tuning loaded at startup, which overrides them. Auto-commit
// is always on: settlement only marks offsets, which sarama sends to the broker only when
// auto-commit is enabled.
func NewConsumerClient(conf *schemas.KafkaInfo, consumerConfig *schemas.ConsumerConfig, tuning *Tuning) (sarama.Client, error) {
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

	if err := tuning.apply(cfg); err != nil {
		return nil, err
	}
	cfg.Consumer.Offsets.AutoCommit.Enable = true

	if cfg.Net.TLS.Enable {
		cfg.Net.TLS.Config = conf.TLSConf
	}

	return newClient(conf.Brokers, cfg)
}
