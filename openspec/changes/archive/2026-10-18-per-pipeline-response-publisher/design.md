# Design

## Context

See proposal.md (Why) for the motivation. Current shape:

```
 pipeline A --+                                       +--> publishOne (skip: 1 attempt)
 pipeline B --+--> transport.PublishChannel (1000) -->|    publishOneRetrying (retry: loop
 pipeline C --+    one global queue                   |    with backoff, holds the queue)
                                                      +--> ONE goroutine, SyncProducer.SendMessage
                                                           one record per ISR round trip
```

Facts that shape the approach:

- The hand-off is `publishUpstreamResponse` (`internal/transport/response.go:40`), reached through `RunSubscription` → `ConsumerResponseFast/Slow` → `withReplyService` → `sendToService`. It sends to the global `PublishChannel`: a non-blocking send under `skip` (drop and settle when full), a blocking send under `retry` (abandoned on shutdown).
- `handlers.ResponsePublish` (`internal/handlers/handler_http.go:57`) is started once by `Connect` with its own `publishCtx`. `Connect` cancels it only after every pipeline has ended (`wg.Wait`). After cancellation it drains the queue with one attempt per event (`publishOneRetrying` turns into one attempt when ctx is done).
- Concurrency safety is already there. `sarama.SyncProducer` is safe for concurrent use and batches concurrent sends into one produce request per broker (`net.maxopenrequests: 1` in `config_producer.yaml` limits requests, not records). `FileTransactionLogger` hands events to channels and is already called concurrently by the HTTP publish handler. Acks go to the per-claim `offsetTracker`, which fast mode already calls from many goroutines.
- Ordering today: a pipeline with concurrency limit > 1 completes upstream calls out of order, so its responses are already unordered. Only a `max_conn: 1` pipeline produces responses in order, and the single publisher keeps that order.
- `transport.concurrencyLimit(conf)` (`dispatch.go:29`) already gives the pipeline's limit: `max_conn`, or `fastModeLimit` (512, lowered in tests).
- About 38 test references use the global `transport.PublishChannel` (`internal/transport/*_test.go`, `internal/handlers/*_test.go`).

## Goals / Non-Goals

**Goals:**

- A response topic that fails holds back only its own pipeline, under both failure policies.
- A busy pipeline publishes concurrently, so the sync producer can batch.
- The order of responses of a `max_conn: 1` pipeline is kept.
- Publication semantics are otherwise unchanged: retry and classification, drop-and-settle, one outcome per response, the shutdown order.

**Non-Goals:**

- Global fairness or a cap on publishers across pipelines (each pipeline is bounded on its own; see Risks).
- Changing the HTTP publish endpoint path, which never used the queue.

## Decisions

### D1. One queue per pipeline, passed explicitly

`Connect` creates one queue per consumer stanza that has a `response_topic`: `make(chan *schemas.ConsumerEvent, transport.PublishQueueSize)` with `PublishQueueSize = 1000`, the same size as today. The queue is passed as a parameter along the existing call chain: `runSubscription(ctx, toggle, client, conf, queue, logger)` → `ConsumerResponseFast/Slow` → `withReplyService` → `sendToService` → `publishUpstreamResponse`, which sends to it instead of the global. Stanzas without a `response_topic` get a nil queue; `publishUpstreamResponse` already returns before touching it when `ResponseTopic == ""`. The global `transport.PublishChannel` is removed.

Alternative rejected: carrying the queue on `ConsumerConfig` (like the runtime field `UpstreamTLS`). Fewer signatures change, but it puts a live channel into a config schema, and tests would mutate config to redirect output. An explicit parameter keeps ownership visible.

Alternative rejected: `RunSubscription` owning its queue and publishers. Lifetimes would be local, but `router` would need the producer registry and the `handlers` package, and the process-wide shutdown order would be split across pipelines.

### D2. Publisher count: `min(concurrencyLimit(conf), 16)`

`transport.PublisherCount(conf)` returns `min(concurrencyLimit(conf), MaxPublishersPerPipeline)` with `MaxPublishersPerPipeline = 16`. A `max_conn: 1` pipeline gets 1 publisher, so its responses are written one at a time in completion order (spec scenario "Sequential pipeline keeps its order"). A pipeline with `max_conn: 4` gets 4, and fast mode gets 16.

Why 16: it is enough concurrent senders for the sync producer to put several records into each produce request (throughput grows with batch size, not with request count, because `maxopenrequests` is 1). It also keeps goroutines and in-flight records bounded per pipeline. More than 16 brings little: with one open request per broker, records beyond the batch only wait in sarama's buffers. The value stays a code constant (proposal Non-goals), like the fast-mode ceiling.

Alternative rejected: a shared pool of N workers over one global queue. It raises throughput but keeps the head-of-line problem: N stuck retry events stall everyone, just N times later.

### D3. `ResponsePublish` drains one queue with N workers

`handlers.ResponsePublish(ctx, queue, workers, logTransaction, producers)` starts `workers` goroutines. Each runs today's loop over its queue: `publishOne` per event while ctx is alive, then, once ctx is done, a drain that gives every remaining event one attempt until the queue is empty. The function returns when all its workers have returned. `publishOne`, `publishOneRetrying` and `isNonTransientPublishError` are unchanged. A retrying worker blocks only itself; the other workers of the same pipeline continue with the next events (head-of-line blocking within a pipeline is limited to the workers stuck on a failing topic).

### D4. Shutdown order unchanged

`Connect` starts one `ResponsePublish` per queue under the existing `publishCtx`, tracked by a `sync.WaitGroup` that replaces `publishDone`. The sequence stays: cancel → HTTP shutdown → `wg.Wait()` (pipelines end, nothing more is queued) → `producerBg.Wait()` → `stopPublish()` → wait for all publishers (each drains its queue with one attempt per event) → `closeProducer`. This is the order required by `runtime-control` "Graceful termination on standard signals".

Publishers start before the producer settles, as today. Under `skip` with no producer, events are failed and settled ("Responses drained without producers"). Retry stanzas never read before the producer is ready (`retry-durable-response-publish` D5).

### D5. Tests

- `internal/transport`: tests read from a queue they create and pass in, instead of the global (helper `newQueue(t)` in `export_test.go` or the test files). A full queue under `skip` drops and settles. A blocking send under `retry` is abandoned on shutdown. `PublisherCount` returns 1 for `max_conn: 1`, 4 for `max_conn: 4` and 16 in fast mode (also with the ceiling lowered through `fastModeLimit` below 16, where it returns the ceiling).
- `internal/handlers`:
  - **Isolation:** two queues, two `ResponsePublish` calls. Pipeline A's producer fails every send transiently (retry stanza), pipeline B's succeeds. B's events are all published and acked while A's are still retrying.
  - **Concurrency:** a producer whose `SendMessage` blocks until released. With 16 workers and 20 queued events, exactly 16 sends start before any release.
  - **Order:** 1 worker, events R1..R3 sent in order to a recording producer.
  - **Drain:** after ctx is cancelled, every queued event gets one attempt across workers and `ResponsePublish` returns.
- `internal/app`: `Connect` with two stanzas with response topics creates two queues, and shutdown waits for both publishers (an existing shutdown test extended); a stanza without `response_topic` gets no publishers.
- Run the transport and handlers tests with `-race` (part of `make test`).

## Risks / Trade-offs

- [More goroutines and memory: up to 16 publishers and 1000 queued responses per pipeline] → Bounded per pipeline. A deployment with 10 response-topic stanzas holds at most 10,000 queued responses instead of 1,000 in total. It is the same order as each pipeline's existing internal event channel (also 1000).
- [Total concurrent sends grow with the number of pipelines] → Concurrent sends are batched by the shared producer, and with one open request per broker the broker load grows in records per request, not in requests.
- [Two stanzas writing to the same response topic] → They still share the topic's health (a broken topic stalls both), but each has its own queue, so neither starves the other's healthy responses. Stated as a non-goal in the proposal.
- [Head-of-line within one pipeline] → A retry pipeline whose topic is broken ends up with all its workers retrying, and its queue then fills. That is the backpressure `on_failure: retry` promises, now limited to that pipeline.
- [Response order of concurrent pipelines] → Unchanged in substance: their upstream calls already complete out of order. Order is kept where it exists today (`max_conn: 1`).

## Migration Plan

No configuration migration. Rollback means redeploying the previous build.
