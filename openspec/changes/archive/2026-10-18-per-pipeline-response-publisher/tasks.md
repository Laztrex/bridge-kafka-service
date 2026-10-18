# Tasks

## 1. Per-pipeline queue in the transport

- [x] 1.1 Replace the global `transport.PublishChannel` (`internal/transport/channels.go`) with `PublishQueueSize = 1000`, and pass the pipeline's queue through `ConsumerResponseFast/Slow` → `withReplyService` → `sendToService` → `publishUpstreamResponse`, keeping the non-blocking send under `skip` and the abandonable blocking send under `retry` (design D1). Move the `internal/transport` tests from the global to a queue they create; verify `go test -race ./internal/transport/` passes, including the full-queue drop test and the retry-mode wait/abandon tests
- [x] 1.2 Add `PublisherCount(conf)` = `min(concurrencyLimit(conf), MaxPublishersPerPipeline)` with `MaxPublishersPerPipeline = 16` (design D2), with a unit test: 1 for `max_conn: 1`, 4 for `max_conn: 4`, 16 in fast mode, and the lowered ceiling when `fastModeLimit` is below 16

## 2. Concurrent publisher per queue

- [x] 2.1 Change `handlers.ResponsePublish` (`internal/handlers/handler_http.go`) to take a queue and a worker count and run that many workers. Each drains the queue with `publishOne` while ctx is alive and gives every remaining event one attempt after ctx is done; the function returns when all workers have returned (design D3). `publishOne`/`publishOneRetrying` stay unchanged. Move the `internal/handlers` tests off the global queue; verify the existing retry, drain and no-producer tests pass with `-race`
- [x] 2.2 Add handler tests (design D5):
  - isolation: a retry pipeline whose producer always fails transiently does not delay a second pipeline's publications and acks;
  - concurrency: exactly 16 sends in flight with 20 queued events and blocking sends;
  - order: one worker publishes R1..R3 in order;
  - drain: after cancellation every queued event gets one attempt across workers and `ResponsePublish` returns.

  Verify with `go test -race ./internal/handlers/`

## 3. Wiring and shutdown

- [x] 3.1 Pass the queue through `RunSubscription` (`internal/router/consumer.go`) to the response handler, and update the `runSubscription` seam and the `RunSubscription` test call sites; verify `go test -race ./internal/router/ ./internal/app/` passes
- [x] 3.2 In `Connect` (`internal/app/connection.go`), create one queue per stanza with a `response_topic` (nil otherwise), start `ResponsePublish(publishCtx, queue, transport.PublisherCount(conf), ...)` for each under a `sync.WaitGroup`, and keep the shutdown order: pipelines end, producer settles, `stopPublish`, wait for all publishers, close the producer (design D4). Add an app test: two response-topic stanzas each get a publisher and shutdown waits for both, and a stanza without `response_topic` gets none; verify `go test -race ./internal/app/`
- [x] 3.3 Update the docs:
  - README: in "Service failures", the "internal publish queue (1000 responses)" sentence becomes a per-pipeline queue, and add that a failing response topic holds back only its own pipeline and that a pipeline publishes up to 16 responses at once (one at a time with `max_conn: 1`);
  - `docs/flow.md`: the key points on publication.

  Verify the wording matches the spec deltas and `make docs-check` passes

## 4. Integration

- [x] 4.1 Run `go build ./... && go vet ./... && go test -race ./...` and `openspec validate per-pipeline-response-publisher --strict`
- [x] 4.2 Run `make ci` (includes lint, the coverage gate and `docs-check`) and fix any failures
