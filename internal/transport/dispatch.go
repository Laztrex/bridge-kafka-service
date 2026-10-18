package transport

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"bridge-kafka-service/internal/consts"
	"bridge-kafka-service/internal/schemas"
)

// FastModeMaxInFlight is the built-in ceiling on concurrent upstream calls of a
// fast-mode pipeline (service.max_conn <= 0): high enough that normal operation
// never reaches it, low enough to stay well under the common soft ulimit -n of
// 1024 together with the Kafka, metrics and audit sockets. An operator who
// needs a different limit already has service.max_conn.
const FastModeMaxInFlight = 512

// fastModeLimit is the effective fast-mode ceiling; a variable only so that
// tests can lower it.
var fastModeLimit = FastModeMaxInFlight

// concurrencyLimit is the pipeline's in-flight ceiling: service.max_conn when
// set (slow mode), the built-in fast-mode ceiling otherwise. One rule for the
// dispatch semaphore and the upstream connection pool.
func concurrencyLimit(conf *schemas.ConsumerConfig) int {
	if conf.Service.MaxConn > 0 {
		return conf.Service.MaxConn
	}
	return fastModeLimit
}

func ConsumerResponseFast(
	logTransaction schemas.TransactionLogger,
	conf *schemas.ConsumerConfig,
	upstream Upstream,
	ch chan *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	ctx context.Context,
) {
	log.Info().Msgf(
		"fast mode for %s:%s%s (max_conn: %d, in-flight ceiling: %d)",
		conf.Service.Name, conf.Service.Port, conf.Service.Path, conf.Service.MaxConn, fastModeLimit,
	)
	withReplyService(logTransaction, conf, upstream, ch, queue, ctx)
}

func ConsumerResponseSlow(
	logTransaction schemas.TransactionLogger,
	conf *schemas.ConsumerConfig,
	upstream Upstream,
	ch chan *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	ctx context.Context,
) {
	log.Info().Msgf(
		"slow mode for %s:%s%s (max_conn: %d)",
		conf.Service.Name, conf.Service.Port, conf.Service.Path, conf.Service.MaxConn,
	)
	withReplyService(logTransaction, conf, upstream, ch, queue, ctx)
}

// DefaultDrainWindow is how long a stopping pipeline waits for in-flight upstream calls before
// abandoning them when ConsumerConfig.DrainWindow is unset.
const DefaultDrainWindow = 10 * time.Second

// SendToService performs one upstream call for msg, bounded by timeout, hands the response to
// the pipeline's queue and settles it. A call that fails after abandon is cancelled (drain window
// over) records no failure and leaves the offset unsettled, so the record is redelivered after restart.
func SendToService(
	abandon context.Context,
	logTransaction schemas.TransactionLogger,
	upstream Upstream,
	msg *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	timeout time.Duration,
) {
	sendToService(abandon, logTransaction, upstream, msg, queue, timeout, nil)
}

// sendToService is SendToService with the failure policy: nil retry means on_failure: skip;
// otherwise transport failures are retried until a response, a poison verdict or abandon.
func sendToService(
	abandon context.Context,
	logTransaction schemas.TransactionLogger,
	upstream Upstream,
	msg *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	timeout time.Duration,
	retry *retryState,
) {
	logTransaction.Start(msg.Event)
	abandoned := false
	defer func() {
		if abandoned {
			return
		}
		msg.Ack()
		logTransaction.Close(msg.Event)
	}()

	requestId := msg.Values.RequestId
	var firstFailure time.Time
	for attempt := 1; ; attempt++ {
		resp, err := callUpstream(abandon, upstream, msg, timeout)
		if err != nil && abandon.Err() != nil {
			abandoned = true
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("upstream call abandoned on shutdown, the record will be redelivered")
			return
		}
		if err == nil {
			if retry != nil {
				retry.succeeded()
			}
			err = publishUpstreamResponse(abandon, msg, resp, queue, retry != nil)
			if errors.Is(err, errPublishAbandoned) {
				abandoned = true
				log.Warn().Str(consts.HeaderRequestIdKey, requestId).
					Msg("response publication abandoned on shutdown, the record will be redelivered")
				return
			}
		}
		if err == nil {
			msg.Event.SetSuccess(requestId)
			return
		}
		if retry == nil || !isUpstreamFailure(err) {
			recordFailure(msg, err)
			return
		}

		if attempt == 1 {
			firstFailure = time.Now()
		}
		if attempt >= poisonAttempts && retry.succeededSince(firstFailure) {
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).Err(err).
				Msg("poison record: keeps failing while the service answers other records, skipping it")
			recordFailure(msg, err)
			return
		}

		wait := RetryBackoff(attempt)
		log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).
			Dur("retry_in", wait).Err(err).Msg("upstream call failed, retrying")
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-abandon.Done():
			timer.Stop()
			abandoned = true
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("retry abandoned on shutdown, the record will be redelivered")
			return
		}
	}
}

// upstreamError marks an error returned by Upstream.Call, as opposed to an error
// of handling its response; only the former is retried.
type upstreamError struct{ error }

func (e upstreamError) Unwrap() error { return e.error }

func isUpstreamFailure(err error) bool {
	var u upstreamError
	return errors.As(err, &u)
}

func callUpstream(abandon context.Context, upstream Upstream, msg *schemas.ConsumerEvent, timeout time.Duration) (UpstreamResponse, error) {
	ctx, cancel := context.WithTimeout(abandon, timeout)
	defer cancel()

	resp, err := upstream.Call(ctx, UpstreamRequest{
		Body:    msg.Values.Body.Data,
		Headers: msg.Values.Body.Headers,
	})
	if err != nil {
		return resp, upstreamError{err}
	}
	return resp, nil
}

func recordFailure(msg *schemas.ConsumerEvent, err error) {
	msg.Event.SetFailure(msg.Values.RequestId)
	log.Error().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).Msgf("error request to service: %v", err)
}

// withReplyService dispatches the events of ch to upstream and hands responses to queue, at most one call
// per semaphore slot at a time: service.max_conn slots in slow mode, the
// built-in fast-mode ceiling otherwise. On stop it waits for in-flight calls up
// to the drain window, then abandons the rest; a dispatch waiting for a free
// slot returns immediately, so consumption stops without new calls.
func withReplyService(
	logTransaction schemas.TransactionLogger,
	conf *schemas.ConsumerConfig,
	upstream Upstream,
	ch chan *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	ctx context.Context,
) {

	timeout := conf.Service.Timeout
	if timeout <= 0 {
		timeout = consts.DefaultServiceTimeout
	}

	var retry *retryState
	if conf.EffectiveFailurePolicy() == schemas.FailureRetry {
		retry = &retryState{}
	}

	limit := concurrencyLimit(conf)
	sem := make(chan struct{}, limit)

	drain := conf.DrainWindow
	if drain <= 0 {
		drain = DefaultDrainWindow
	}
	abandon, abandonCalls := context.WithCancel(context.Background())
	var calls sync.WaitGroup
	defer func() {
		finished := make(chan struct{})
		go func() { calls.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(drain):
			log.Warn().Msg("drain window elapsed, abandoning in-flight upstream calls")
			abandonCalls()
			<-finished
		}
		abandonCalls()
	}()

	for {
		select {
		case msgRequest, ok := <-ch:
			if !ok {
				log.Info().Msg("channel closed, exiting withReplyService")
				return
			}

			msgRequest.Event = &schemas.Event{
				EventType: consts.EventRequestService,
				RequestId: msgRequest.Values.RequestId,
			}

			select {
			case sem <- struct{}{}:
				calls.Add(1)
				go func(msg *schemas.ConsumerEvent) {
					defer calls.Done()
					defer func() { <-sem }()

					sendToService(abandon, logTransaction, upstream, msg, queue, timeout, retry)
				}(msgRequest)
			case <-ctx.Done():
				log.Info().Msg("context cancelled while waiting for a call slot, exiting withReplyService")
				return
			}
		case <-ctx.Done():
			log.Info().Msg("context cancelled, exiting withReplyService")
			return
		}
	}
}
