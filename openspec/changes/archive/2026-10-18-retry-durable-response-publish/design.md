# Design

## Context

See proposal.md (Why) for the motivation. The current flow of a response, and the two places where it is lost:

```
 sendToService (retry stanza)
   --> publishUpstreamResponse: builds publishEvent, hands the ack over (TakeAck),
       waits for space in transport.PublishChannel (wait=true)
   --> ResponsePublish (ONE goroutine for all pipelines, internal/handlers/handler_http.go)
         publishOne:
           Lookup(topic) fails       --> SetFailure ............ msg.Ack()   <-- loss #2
           FileTransactionLogger.Publish
             Send() fails            --> SetFailure ............ msg.Ack()   <-- loss #1
```

Facts that shape the approach:

- Sarama already retries a produce request internally (`configs/config_producer.yaml`: `retry.max: 30`, `backoff: 100ms`, `requiredacks: -1`). A `SendMessage` error means a sustained broker problem, not a single glitch.
- `publishEvent` (built in `internal/transport/response.go`) does not carry `Config`, so `publishOne` cannot tell a retry stanza from a skip stanza today.
- `observability.FileTransactionLogger.Publish` serves both the HTTP publish endpoint and the response publisher. It records the outcome and the audit entry once per call.
- `internal/app/connection.go`: `startProducer` only logs a permanent failure. `producersSettled` is closed on success *and* on failure, so consumers start in both cases. `errCh` has capacity `len(consumers)+1` (consumers + HTTP server).
- Shutdown order in `Connect`: cancel ctx, then HTTP shutdown, then `wg.Wait()` (consumers and their response handlers stop; retry-mode calls waiting for channel space are abandoned after the drain window), then `producerBg.Wait()`, then `stopPublish()` and a drain of `PublishChannel`, then `closeProducer`. The response publisher therefore outlives the pipelines.
- Backoff helper `retryBackoff(attempt)` and its test seams (`retryBackoffStart`, `retryBackoffMax`) live unexported in `internal/transport/retry.go`.

## Goals / Non-Goals

**Goals:**
- A retry-stanza response is either published or left unsettled (redelivered). It is never settled unpublished, except for a non-transient publish error.
- A permanently missing producer cannot silently consume records whose responses must be published.
- Skip stanzas, the HTTP publish endpoint, and deployments without `response_topic` keep their current behaviour.

**Non-Goals:**
- Per-pipeline or concurrent publishers.
- A shutdown deadline for the drain of `PublishChannel`.
- Fixing the already-existing quirk that offsets acked during the post-`wg.Wait()` drain belong to closed consumer-group sessions and are therefore not committed (those records are redelivered; at-least-once still holds).

## Decisions

### D1. The retry loop lives in the response publisher, not in the pipeline

`publishOne` decides by `msg.Config.EffectiveFailurePolicy()`. `publishUpstreamResponse` sets `publishEvent.Config = msg.Config`, so the policy reaches the publisher. For a retry event, `publishOne` calls `observability.Send` in a loop:

```
attempt := 1
loop:
  err := Send(...)
  err == nil              --> success, ack, done
  nonTransient(err)       --> failure, ack, done
  log warn (request id, attempt, retry_in, err)
  wait retryBackoff(attempt) or ctx.Done()
  ctx.Done()              --> NO ack, NO metric/audit outcome, log "redelivered", done
  attempt++
```

One outcome per response: the loop must not go through `FileTransactionLogger.Publish` once per attempt, because that records an outcome and an audit entry every time. Split `Publish` into the send part and an outcome-recording part (or add a variant that accepts a send function). The HTTP endpoint keeps calling the single-attempt path unchanged. The ALS `EventNameBusinessRequestTransferred` audit entry and the event Start/Close are emitted once, with the latency of the whole loop.

Alternative rejected: retrying inside `sendToService` (the pipeline goroutine) after the response was queued. The pipeline has already handed the event over, and publication happens asynchronously in another goroutine, so this would need a result channel back. Keeping the loop where the send happens is simpler.

Alternative rejected: leaving the offset unsettled without retrying. The partition stops committing until a restart, `offsetTracker.pending` grows without bound, and nothing heals without an operator.

### D2. Blocking the single publisher is acceptable

While a retry event is in its loop, every other pipeline's responses queue behind it. Skip stanzas start dropping once `PublishChannel` (1000 entries) is full, and retry stanzas apply backpressure. This is deliberate: the shared producer talks to the same brokers, so a sustained `SendMessage` failure would affect the other events as well. The cost is a topic-specific failure (one response topic broken, the others healthy) stalling everyone. D3 limits that to transient errors, and the case is logged on every attempt.

### D3. Transient vs non-transient publish errors

Non-transient, which means settle as a failure (like a poison record):
- the body cannot be marshalled (`json.Marshal` error from `Send`);
- `sarama.ErrMessageSizeTooLarge`, `sarama.ErrInvalidMessage`, `sarama.ErrInvalidMessageSize` (match with `errors.Is`; sarama returns `KError` values, and `ProducerError`/`ProducerErrors` should be unwrapped where needed. Verify against the sarama version in `go.mod` with a unit test).

Everything else is transient (out of brokers, leader not available, timeouts, authorization errors). Authorization or ACL errors are treated as transient on purpose: they are fixed by configuration, which matches how the upstream retry treats a TLS misconfiguration (retried until fixed).

### D4. Backoff reuse

Export the existing helper from `internal/transport/retry.go` (for example `transport.RetryBackoff`), or move it into a small shared package. Do not duplicate it. The test seams stay variables so tests can shrink the waits.

### D5. Producer failure is fatal when any active stanza has a response topic

- `startProducer` returns its error. In `Connect`, if the error is permanent (`ctx.Err() == nil`) and `needsResponses` is set (any consumer stanza with non-empty `Topics` and non-empty `ResponseTopic`), send `fmt.Errorf("permanently failed to create producer: %w", err)` to `errCh`.
- `errCh` capacity becomes `len(consumers)+2` (HTTP server + producer + each consumer), so no reporter blocks.
- Consumers must not read records when the producer failed. Today `producersSettled` is closed on failure as well, and the main loop cancels ctx only after it reads `errCh`, so a consumer could start between the two events. Record the producer outcome before closing `producersSettled` (for example an `atomic.Bool producerFailed`, or a result value read after the channel closes). A consumer goroutine whose stanza has a `ResponseTopic` returns without calling `ready.consumerUp()` / `RunSubscription` when the producer failed. A consumer without a `ResponseTopic` behaves as today.
- If no stanza needs responses, the failure stays a log line (current scenario kept).

Alternative rejected: retrying producer creation forever. The process looks alive while doing nothing, which is exactly the silent-failure shape the change removes, and the supervisor never gets a restart signal.

### D6. Startup validation for retry stanzas

Extend the `on_failure` validation in `internal/config/upstream.go` (or `config.Validate`): when the policy is `retry` and `ResponseTopic != ""`, the topic must be one of `k.Producers[*].Topic`. The error names the stanza in the same way the existing `on_failure`/`max_conn` errors do (see `upstream_test.go`). This makes `Lookup` failure in `publishOne` unreachable for retry events. Keep the existing "no producer" branch for skip events. For a retry event that still reaches it, treat it as non-transient (log an error, settle) as a defensive fallback.

## Risks / Trade-offs

- [A topic-specific transient error stalls all pipelines] → It is logged on every attempt with the topic and request id. Per-pipeline publishers are a separate, larger change.
- [Shutdown during a broker outage takes longer] → The backoff wait aborts on `publishCtx`. Each queued event in the post-cancel drain still makes one `SendMessage`, as today. No new waits are added after cancellation: retry events in the drain get a single attempt, and on failure they are left unsettled.
- [Operators see restarts instead of a silently idle process when Kafka is unreachable for over 5 minutes] → This is intended, and it is documented in the README next to the existing consumer-client bullet.
- [Error classification differs between sarama versions] → D3 asks for a unit test per non-transient error value.

## Migration Plan

No configuration migration. A config with a retry stanza whose `response_topic` is not a producer topic now fails at startup. Such a config was already losing every response, so failing fast is the fix. Rollback means redeploying the previous build.
