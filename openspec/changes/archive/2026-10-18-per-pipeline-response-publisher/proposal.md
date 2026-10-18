# Proposal

## Why

All upstream responses of all pipelines go through one global queue (`transport.PublishChannel`, 1000 entries) drained by one goroutine that calls the synchronous producer one record at a time (`handlers.ResponsePublish`). Two problems follow:

- **No isolation.** A response that cannot be written blocks every other pipeline's responses behind it. For an `on_failure: retry` stanza this lasts until the topic accepts writes again (the publish retry loop runs inside the single publisher), so one broken response topic — a missing topic, an ACL on one topic, a partition without a leader — stalls the responses of every stanza: `retry` stanzas stop consuming, `skip` stanzas start dropping responses once the shared queue is full. A `skip` stanza whose topic fails costs everyone the sarama retry budget (30 retries × 100 ms) per record.
- **Throughput ceiling.** With `requiredacks: -1`, each `SendMessage` waits for a full in-sync-replica round trip, and the next one starts only after it returns, so response throughput across the whole process is about one record per produce round trip. A fast-mode pipeline can run 512 upstream calls at once (`bound-fast-mode-concurrency`); the publisher, not the upstream, becomes the limit, and its queue fills, which under `skip` means dropped responses. The sync producer batches records from concurrent senders into one request, but a sequential caller never gives it more than one.

## What Changes

- Each pipeline (consumer stanza with a `response_topic`) gets its own publication queue (1000 entries, as today) and its own publishers. A pipeline's responses go only to its own queue, so a failing or slow response topic holds back only the pipeline that writes to it.
- A pipeline runs `min(concurrency limit, 16)` publishers, where the concurrency limit is the one the pipeline already uses for upstream calls (`max_conn`, or the fast-mode ceiling of 512). A pipeline with `max_conn: 1` keeps exactly one publisher, so the response order it has today is kept; pipelines with more concurrency already complete upstream calls out of order.
- Everything else about publication is unchanged: the retry loop and error classification for `retry` stanzas, drop-and-settle under `skip` when the queue is full, one outcome per response in metrics and audit, the shared producer, the HTTP publish endpoint, and the shutdown order (pipelines stop first, then queued responses get one attempt each, then the producer closes).
- Docs: README ("Service failures", publish queue sentence) and `docs/flow.md`.

## Non-goals

- Configuration keys for the publisher count or queue size: both stay code constants, like the fast-mode ceiling.
- A producer per pipeline, or async-producer publishing: the shared sync producer already batches concurrent sends.
- Response ordering guarantees beyond today's (no new per-key or per-partition ordering for concurrent pipelines).
- Isolation between two stanzas that write to the same response topic: they share the topic's fate, but not each other's queue.
- Changing sarama producer tuning (`config_producer.yaml`) or the HTTP publish endpoint.
- Bounding the shutdown drain in time (already a non-goal of `retry-durable-response-publish`).

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `transport-modes`: "Response decoupling from request path" — the shared response-publication channel becomes a per-pipeline queue; new scenarios state that a stalled response topic does not affect other pipelines and that a pipeline publishes up to `min(limit, 16)` responses concurrently while a `max_conn: 1` pipeline keeps a single publisher.
- `message-routing`: "Response publication retry under retry policy" — a new scenario states that a retried publication holds back only its own pipeline.

## Impact

- Code: `internal/transport/channels.go` (global `PublishChannel` removed), `internal/transport/response.go` and `dispatch.go` (the pipeline's queue is passed in instead of the global), `internal/handlers/handler_http.go` (`ResponsePublish` drains one queue with N workers), `internal/router/consumer.go` (passes the queue through `RunSubscription`), `internal/app/connection.go` (creates per-pipeline queues and publishers, stops them in the existing shutdown step), and the tests that use `transport.PublishChannel` (about 38 references in `internal/transport` and `internal/handlers`).
- Behaviour visible to operators: a broken response topic stalls or drops only its own pipeline's responses; response throughput of busy pipelines rises with concurrent publishing. More goroutines (up to 16 per pipeline) and up to 1000 queued responses per pipeline instead of 1000 in total.
- Docs: `README.md`, `docs/flow.md`.
- No new dependencies and no new configuration keys.
