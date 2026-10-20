# Design

Decision log of the baseline. Each entry gives the decision, the reason, and the alternatives rejected. IDs are stable: cite them from code comments and later changes. Behaviour itself is specified in `openspec/specs/`, and the specs win if this log and a spec ever disagree.

## Delivery and settlement

**DEL-1. Offsets are marked in consumption order, only after full processing.** Each claim owns an offset tracker. A record is marked when it settles, and only once every earlier record of the claim is settled too. One record still in flight therefore holds the marks of the records after it, which at-least-once allows: after a crash they are delivered again.

**DEL-2. Every record carries an ack and an abandon that share one `sync.Once`.** Ack settles the record; abandon gives it up unmarked, so it is redelivered. Every path that leaves a record unsettled calls abandon: shutdown, drain timeout, an ended claim, a slot that never freed. Otherwise an ending claim would wait for the full bound.

**DEL-3. An ending claim waits for its records before sarama's final commit.** `ConsumeClaim` stops reading and waits until its handed-out records settle or are abandoned, at most `min(drain window, consumer.group.rebalance.timeout)`. sarama waits for `ConsumeClaim` before releasing the session, so marks made in that window reach the commit.
*Rejected:* committing explicitly from late acks, since no session exists after the release; waiting in `Cleanup`, which runs once per session and would need a session-wide registry.

**DEL-4. Nothing is dispatched for a record whose claim has ended.** The dispatcher checks the claim context before and after taking a slot. The upstream and publication retry waits also end when the claim ends. Such a record now belongs to the partition's next owner and is abandoned.

**DEL-5. Shutdown waits one drain window in total.** One drain deadline, set when the pipeline starts stopping, bounds both the claim wait and the dispatcher's wait for in-flight calls.

**DEL-6. One concurrency rule for both modes.** Every pipeline dispatches through a semaphore: `service.max_conn` when set, otherwise a built-in fast-mode ceiling of 512. That ceiling stays well below the common soft `ulimit -n` of 1024, leaving room for Kafka, metric and audit sockets. Each HTTP upstream gets its own idle pool sized to the limit, with `MaxConnsPerHost = 0`, so the semaphore is the only cap and calls never queue invisibly inside `RoundTrip`.
*Rejected:* a separate worker pool for fast mode.

## Upstream failures and retry

**RET-1. Only transport failures are failures.** Unreachable, timeout, or gRPC `Unavailable`/`DeadlineExceeded`/`Canceled`. Any HTTP status, 4xx and 5xx included, and every other gRPC status is an answer and is published: gRPC as an empty body with `Grpc-Status` and `Grpc-Message`. Whether 5xx should be retryable is an open product question.

**RET-2. `on_failure: skip | retry`.** `retry` requires `service.max_conn > 0`, so retrying records turn into backpressure. Backoff starts at 1s, doubles up to 30s, with ±20% jitter, and is shared with publication retries. A record is skipped as poison after 5 attempts, but only if another call of the pipeline succeeded after its first failure. During an outage nothing is skipped.

**RET-3. Record-specific failures are marked permanent at their source.** The transport wraps errors that repeating cannot fix in `permanentError`: a response body over 16 MiB, a required header that is missing or that the transport cannot carry. They are settled after one attempt under both policies, so even a `max_conn: 1` pipeline never stalls on them.
Request construction errors stay retryable, because they come from the stanza's address and would turn a misconfiguration into mass skipping. CFG-3 rejects such addresses at startup.
*Rejected:* sentinel errors matched by the dispatcher, which would have to know every transport.

**RET-4. HTTP response bodies are read up to 16 MiB,** the same limit the gRPC client applies.
*Rejected:* publishing a synthetic status, which would hide that the answer was cut.

**RET-5. Failed-attempt warnings carry `failing_for`** in both retry loops, the upstream call and the publication.

## Response publication

**PUB-1. Publication retry lives in the publisher, not in the pipeline.** Under `retry` a transient write failure keeps the offset and repeats the write. The upstream is not called again. The final outcome is recorded once through `RecordPublishOutcome`.
*Rejected:* retrying inside `sendToService` after hand-off, which would need a result channel back; holding the offset without retrying, which never heals.

**PUB-2. Non-transient publication errors are settled as failures:**
- a body that cannot be marshalled;
- `ErrMessageSizeTooLarge`, `ErrInvalidMessage`, `ErrInvalidMessageSize`;
- any `sarama.ConfigurationError` at send time: a record over `Producer.MaxMessageBytes`, or headers on Kafka older than 0.11.

Everything else is transient, including authorization: configuration fixes it.
*Rejected:* matching error message text.

**PUB-3. Every pipeline has its own publish queue of 1000 and up to `min(limit, 16)` publishers.** The queue is passed explicitly. A `max_conn: 1` pipeline writes in completion order. A broken response topic holds back only its own pipeline.
*Rejected:* one shared publisher, the original design, where one broken topic stalled everyone; the queue on `ConsumerConfig`; `RunSubscription` owning its publishers; a shared worker pool, which keeps head-of-line blocking.

**PUB-4. Under `skip`, a full queue drops the response, and the drop is recorded as a failed `response_service` publication** through `RecordPublishOutcome`.
*Rejected:* a dedicated counter, which would be a new metric name.

**PUB-5. Shutdown order:** cancel → HTTP shutdown → pipelines end → producer settles → publishers stop → each queued response gets one attempt → producer closes.

**PUB-6. A `retry` stanza's `response_topic` must be a producer topic** (checked at startup). The "no producer" path in the publisher remains as a defensive fallback that settles the record.

## Headers

**HDR-1. One list of required headers, `consts.RequiredHeaders`,** empty for now, for both entry points:
- a consumed record without a listed header, or with one the transport cannot carry, is a permanent failure;
- a publish request without one gets 422, and a present one is carried onto the record;
- listed headers get no default value;
- upstream responses are not checked.

`HeaderRule.Required` was removed.
*Rejected:* two definitions of "required", which would drift apart and need a second change once the list fills.

**HDR-2. The policy is shared and validity is defined per transport.** `applyHeaderPolicy` runs once before the call and returns a filtered copy; the record keeps every header for audit and the response.
- HTTP uses `httpguts`.
- gRPC requires UTF-8 in `details` (proto3 refuses anything else at marshal), and printable ASCII for the metadata copy of `x-request-id`, which stays in `details`.

*Rejected:* validating inside each `Call`, which repeats the policy per transport; sanitising values, which sends something the producer never sent.

**HDR-3. Transport-controlled headers never come from a record in an HTTP call.** These are the hop-by-hop set of `httputil.ReverseProxy`, plus `Host`, `Content-Length` and `Content-Type`. They are left out without a warning, because chained bridges send them on every record. The call always carries `Content-Type: application/json`, by product decision; a record could override it earlier. gRPC keeps its own content type.

**HDR-4. Tracking headers are normalised to lowercase wire names when a record is consumed.** If a record carries two spellings, the lowercase one wins.
*Rejected:* making every lookup case-insensitive, which a future exact-key lookup would break.

**HDR-5. An HTTP call without `x-request-id` gets a generated one from the client round-tripper.** Kept as is: a record's invalid id that is left out of the call is replaced this way, and the response record still carries the original.

## Configuration and startup

**CFG-1. One check for startup and for `--check-config`.** `app.Check` runs `Validate`, `PrepareUpstreams`, `Topology` and the tuning-file load, continues after failures, and joins the errors, each part keeping its prefix. Errors from parsing the file itself still stop at the first, since decoding cannot continue.
*Rejected:* a separate validation command that copies the checks and drifts.

**CFG-2. `kafka.version`:** an empty value means the client default 2.7.0. A non-empty value that does not parse stops startup. The minimum is 0.10.2 with active consumers (consumer groups) and 0.11 with producers (record headers).
*Rejected:* silently replacing an unparseable value with 2.7.0, the earlier behaviour, which hid typos.

**CFG-3. Upstream addresses are checked without resolving.** For both protocols the host must have no whitespace or control characters and the port must be 1–65535. For HTTP the path must start with `/`, and `http.NewRequest` must accept the URL, which is exactly what each call does.
*Rejected:* a dry run of `grpc.NewClient`, which couples config to the transport.

**CFG-4. Topology:** links from input topics to response topics must not form a cycle. A DFS reports each cycle once, starting from its smallest topic. Chains are allowed. The same `group` with a shared topic is an error; the same `group` with disjoint topics is a warning; one topic in several groups is a normal fan-out.
*Rejected:* forbidding any response topic that is also an input, which also forbids legitimate chains.

**CFG-5. An empty `consumer` or `producer` entry (`- ~`) is an error naming its position.** A stanza is disabled only by having no topics. "Active" has one definition, `activeConsumers`.

**CFG-6. `--check-config` sets up no logging, opens no connections and writes no files.** It prints the verdict to stdout and `warning:` / `error:` lines to stderr, and exits 0 or 1.

**CFG-7. Retention keys `retention.max_backups` and `retention.max_age_days`** are whole numbers; 0 means no limit. They are days, not a duration, because lumberjack counts whole days and would silently round a duration.

## Kafka clients

**KAF-1. Tuning files are checked against the `sarama.Config` tree before decoding.** Unknown keys warn with their dotted path. Keys of interface or func kind cannot be set from YAML: they warn and are removed. Decoding runs inside `recover`.
*Rejected:* `KnownFields(true)`, which reports one key as a type dump and still panics on interface fields; `recover` alone, which loses typos silently.

**KAF-2. Rebalance strategy names:** `range`, `roundrobin`, `sticky` and `cooperative-sticky`. Any other value stops startup.

**KAF-3. Order of settings:** code defaults, then the tuning file, then forced settings (auto-commit on, TLS). Files are loaded once at startup, only for the clients needed, and both are checked even when one is broken.
*Rejected:* putting the tuning on `KafkaInfo`, which would be an import cycle.

## Runtime and lifecycle

**RUN-1. Permanent start failures are fatal.** A consumer client that cannot be created within the retry window, an upstream that cannot be built, or a group that cannot be created end the process with a non-zero status, reported through `errCh`; exit codes stay in `main`. Transient consume-loop errors are retried every 5s.

**RUN-2. A producer that cannot be created within the retry window is fatal whenever producers are configured,** by product decision. No consumer reads a record first.
*Rejected:* fatal only with a `response_topic`, the earlier behaviour, which left a process alive and never ready; retrying in the background; readiness ignoring the producer.

**RUN-3. Pause is one process-wide state, `router.Pause`,** and every sarama pause and resume call happens under its mutex. A claim pauses its own partition when it starts. After receiving a record it waits while paused, before the record is tracked, so already fetched records are held too.
*Rejected:* pausing in `Setup`, because sarama calls it before the partition consumers exist and `Pause` ignores those partitions; per-pipeline toggles, which drifted from the real state.

**RUN-4. The readiness counter only counts up.** A subscription that dies takes the process down with it.

**RUN-5. HTTP listener timeouts:** read header 10s, read 30s, idle 120s, no write timeout. A publish may wait for Kafka retries, and cutting it after the write leaves the client unsure whether to retry.
*Rejected:* `http.TimeoutHandler`, which answers 503 while publishing continues.

## Observability

**OBS-1. Audit goes through two stages: a lossless file stage, then a best-effort gRPC stage.**
- Emitting blocks on the file queue (4096).
- The writer flushes at least once a second and whenever idle.
- Entries are then offered to the gRPC queue (4096); a full queue drops for gRPC only, counted in a warning at most once a minute.
- Entries are sent in streams of up to 256, with 3s per stream and a 3s budget at shutdown.

*Rejected:* one long-lived stream, which needs reconnect and resend logic; the earlier one-second wait-and-drop, which lost audit entries.

**OBS-2. Rotation uses lumberjack: 100 MB, compressed, mode 0600.** Files created by older versions are switched to 0600 at startup. The audit `fsync` uses the writer's own read handle, reopened when lumberjack rotates.
*Rejected:* hand-written rotation just to keep direct `Sync` access.

**OBS-3. Retention is opt-in through `WithRetention`.** By default every backup is kept, and the operator owns cleanup. With limits, lumberjack prunes on the first write and at every rotation, and startup logs the limits once.

**OBS-4. `/status` has a `pipelines[]` section,** fed by one `PipelineState` per active stanza and passed explicitly:
- retrying calls and publications are tracked in maps keyed by event, bounded by `max_conn` and the publisher count;
- `uncommitted` is an atomic counter, so a late ack after a claim ends is not counted twice;
- `paused` is part of the entry.

Readiness ignores all of it, and `pipelines[]` is the place where future per-pipeline state goes.
*Rejected:* state carried on the config or in the context; gauges in the metric file, whose format is per request.
