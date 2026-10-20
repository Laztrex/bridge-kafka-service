package transport

import (
	"context"
	"encoding/json"
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
	state *PipelineState,
) {
	log.Info().Msgf(
		"fast mode for %s:%s%s (max_conn: %d, in-flight ceiling: %d)",
		conf.Service.Name, conf.Service.Port, conf.Service.Path, conf.Service.MaxConn, fastModeLimit,
	)
	withReplyService(logTransaction, conf, upstream, ch, queue, ctx, state)
}

func ConsumerResponseSlow(
	logTransaction schemas.TransactionLogger,
	conf *schemas.ConsumerConfig,
	upstream Upstream,
	ch chan *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	ctx context.Context,
	state *PipelineState,
) {
	log.Info().Msgf(
		"slow mode for %s:%s%s (max_conn: %d)",
		conf.Service.Name, conf.Service.Port, conf.Service.Path, conf.Service.MaxConn,
	)
	withReplyService(logTransaction, conf, upstream, ch, queue, ctx, state)
}

// DefaultDrainWindow is how long a stopping pipeline waits for in-flight upstream calls before
// abandoning them when ConsumerConfig.DrainWindow is unset.
const DefaultDrainWindow = 10 * time.Second

// DrainDeadline is the moment a stopping pipeline gives up on in-flight upstream calls. It is set
// once, when the pipeline starts stopping, and shared by the claim handlers and the dispatcher, so
// shutdown waits one drain window in total instead of one per stage. A nil *DrainDeadline is unset.
type DrainDeadline struct {
	mu sync.Mutex
	at time.Time
}

// Start sets the deadline to now plus window, unless it is already set.
func (d *DrainDeadline) Start(window time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.at.IsZero() {
		d.at = time.Now().Add(window)
	}
}

// At returns the deadline and whether it has been set.
func (d *DrainDeadline) At() (time.Time, bool) {
	if d == nil {
		return time.Time{}, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.at, !d.at.IsZero()
}

type drainDeadlineKey struct{}

// WithDrainDeadline attaches d to the dispatcher's context.
func WithDrainDeadline(ctx context.Context, d *DrainDeadline) context.Context {
	return context.WithValue(ctx, drainDeadlineKey{}, d)
}

func drainDeadlineFrom(ctx context.Context) *DrainDeadline {
	d, _ := ctx.Value(drainDeadlineKey{}).(*DrainDeadline)
	return d
}

// DrainWindow is the stanza's drain window: ConsumerConfig.DrainWindow, or DefaultDrainWindow.
func DrainWindow(conf *schemas.ConsumerConfig) time.Duration {
	if conf.DrainWindow > 0 {
		return conf.DrainWindow
	}
	return DefaultDrainWindow
}

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
	sendToService(abandon, logTransaction, upstream, msg, queue, timeout, nil, nil)
}

// sendToService is SendToService with the failure policy: nil retry means on_failure: skip;
// otherwise transport failures are retried until a response, a poison verdict or abandon. A
// permanent failure is settled after one attempt under either policy. A retried record is listed
// in state until the call ends.
func sendToService(
	abandon context.Context,
	logTransaction schemas.TransactionLogger,
	upstream Upstream,
	msg *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	timeout time.Duration,
	retry *retryState,
	state *PipelineState,
) {
	logTransaction.Start(msg.Event)
	defer state.CallDone(msg)
	abandoned := false
	defer func() {
		if abandoned {
			msg.Abandon()
			return
		}
		msg.Ack()
		logTransaction.Close(msg.Event)
	}()

	requestId := msg.Values.RequestId
	headers, err := applyHeaderPolicy(upstream, msg.Values.Body.Headers, requestId)
	if err != nil {
		recordFailure(msg, err)
		return
	}
	var firstFailure time.Time
	for attempt := 1; ; attempt++ {
		resp, err := callUpstream(abandon, upstream, msg.Values.Body.Data, headers, timeout)
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
			err = publishUpstreamResponse(abandon, logTransaction, msg, resp, queue, retry != nil)
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
		if retry == nil || !isUpstreamFailure(err) || isPermanent(err) {
			recordFailure(msg, err)
			return
		}

		if attempt == 1 {
			firstFailure = time.Now()
			state.CallRetrying(msg, firstFailure)
		}
		if attempt >= poisonAttempts && retry.succeededSince(firstFailure) {
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).Err(err).
				Msg("poison record: keeps failing while the service answers other records, skipping it")
			recordFailure(msg, err)
			return
		}

		wait := RetryBackoff(attempt)
		log.Warn().Str(consts.HeaderRequestIdKey, requestId).Int("attempt", attempt).
			Dur("failing_for", time.Since(firstFailure)).Dur("retry_in", wait).Err(err).
			Msg("upstream call failed, retrying")
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-msg.ClaimDone():
			timer.Stop()
			abandoned = true
			log.Warn().Str(consts.HeaderRequestIdKey, requestId).
				Msg("claim ended during the retry, the record will be redelivered to the partition's next owner")
			return
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

// permanentError marks a call failure that depends only on the record or its response, so
// repeating the call cannot succeed. Transports mark it at the source.
type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

func callUpstream(abandon context.Context, upstream Upstream, body json.RawMessage, headers map[string]string, timeout time.Duration) (UpstreamResponse, error) {
	ctx, cancel := context.WithTimeout(abandon, timeout)
	defer cancel()

	resp, err := upstream.Call(ctx, UpstreamRequest{Body: body, Headers: headers})
	if err != nil {
		return resp, upstreamError{err}
	}
	return resp, nil
}

func recordFailure(msg *schemas.ConsumerEvent, err error) {
	msg.Event.SetFailure(msg.Values.RequestId)
	log.Error().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).Msgf("error request to service: %v", err)
}

// skipEndedClaim gives up a record whose claim session ended before it was dispatched: it now
// belongs to the partition's next owner, so it is not sent to the upstream and stays unsettled.
func skipEndedClaim(msg *schemas.ConsumerEvent) {
	msg.Abandon()
	log.Debug().Str(consts.HeaderRequestIdKey, msg.Values.RequestId).
		Msg("claim ended before dispatch, the record will be redelivered")
}

// withReplyService dispatches the events of ch to upstream and hands responses to queue, at most one call
// per semaphore slot at a time: service.max_conn slots in slow mode, the
// built-in fast-mode ceiling otherwise. On stop it waits for in-flight calls up
// to the drain window (or the pipeline's shared drain deadline, when one is set), then abandons the rest; a dispatch waiting for a free
// slot returns immediately, so consumption stops without new calls. A record whose claim session
// has ended is not dispatched: it is abandoned and redelivered to the partition's next owner.
func withReplyService(
	logTransaction schemas.TransactionLogger,
	conf *schemas.ConsumerConfig,
	upstream Upstream,
	ch chan *schemas.ConsumerEvent,
	queue chan<- *schemas.ConsumerEvent,
	ctx context.Context,
	state *PipelineState,
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

	drain := DrainWindow(conf)
	abandon, abandonCalls := context.WithCancel(context.Background())
	var calls sync.WaitGroup
	defer func() {
		wait := drain
		if at, ok := drainDeadlineFrom(ctx).At(); ok {
			wait = max(time.Until(at), 0)
		}
		finished := make(chan struct{})
		go func() { calls.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(wait):
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

			if msgRequest.ClaimEnded() {
				skipEndedClaim(msgRequest)
				continue
			}

			msgRequest.Event = &schemas.Event{
				EventType: consts.EventRequestService,
				RequestId: msgRequest.Values.RequestId,
			}

			select {
			case sem <- struct{}{}:
				if msgRequest.ClaimEnded() {
					<-sem
					skipEndedClaim(msgRequest)
					continue
				}
				calls.Add(1)
				go func(msg *schemas.ConsumerEvent) {
					defer calls.Done()
					defer func() { <-sem }()

					sendToService(abandon, logTransaction, upstream, msg, queue, timeout, retry, state)
				}(msgRequest)
			case <-msgRequest.ClaimDone():
				skipEndedClaim(msgRequest)
			case <-ctx.Done():
				msgRequest.Abandon()
				log.Info().Msg("context cancelled while waiting for a call slot, exiting withReplyService")
				return
			}
		case <-ctx.Done():
			log.Info().Msg("context cancelled, exiting withReplyService")
			return
		}
	}
}
