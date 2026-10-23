# transport-modes Specification

## Purpose
Lets each routing pipeline choose between opening a fresh upstream connection for every message and reusing a bounded pool of concurrent connections, selected independently per consumer stanza.

## Requirements

### Requirement: Mode selection from configuration
The system MUST select between fast mode and slow mode based solely on the value of `service.max_conn` on the consumer stanza. A value greater than zero selects slow mode; zero or any negative value selects fast mode.

#### Scenario: Fast mode chosen
- **WHEN** a pipeline starts with `service.max_conn: 0` (or unset)
- **THEN** each consumed record triggers an independent outbound request, limited only by the fast-mode in-flight ceiling

#### Scenario: Slow mode chosen
- **WHEN** a pipeline starts with `service.max_conn: 10`
- **THEN** at most ten concurrent outbound requests are in flight at any time for that pipeline

### Requirement: Fast-mode independence
In fast mode the system MUST dispatch each upstream call as a self-contained unit whose lifecycle is decoupled from sibling calls. Failure of one call MUST NOT block or cancel others.

#### Scenario: Burst traffic in fast mode
- **WHEN** many records arrive while one previous upstream call is still pending
- **THEN** additional goroutines process them concurrently without waiting for older calls to complete

### Requirement: Slow-mode concurrency cap
In slow mode the system MUST permit at most `max_conn` simultaneous upstream requests per pipeline using an internal semaphore. Acquiring the semaphore blocks subsequent dispatches until a slot frees.

#### Scenario: Pool saturation
- **WHEN** `max_conn: 2` and three messages arrive faster than upstream responds
- **THEN** only two requests are in flight; the third waits until one of the first two finishes

#### Scenario: Shutdown drains slow mode
- **WHEN** the pipeline context is cancelled while requests hold semaphore slots
- **THEN** pending acquisitions return immediately rather than blocking indefinitely

### Requirement: Response decoupling from request path

The system MUST funnel every successful upstream response of a pipeline into that pipeline's own response-publication queue so that publishing failures, slow brokers, or pause/resume toggles never stall consumption. A pipeline's responses MUST NOT wait behind the responses of another pipeline: a response topic that is slow or rejects writes MUST hold back only the pipeline that publishes to it. Each pipeline MUST publish up to `min(L, 16)` responses concurrently, where `L` is the pipeline's upstream concurrency limit (`max_conn`, or the fast-mode ceiling); a pipeline with `max_conn: 1` therefore publishes one response at a time, in the order its upstream calls complete. The exception to non-blocking hand-off is a stanza with `on_failure: retry`: there a saturated queue MUST hold the upstream call until space frees, so the response is never dropped and backpressure stops consumption instead. A response dropped from a saturated queue MUST be recorded as a failed `response_service` event, in metrics and audit, exactly once.

#### Scenario: Publication channel saturated

- **WHEN** the pipeline's response queue is full because publishing is slow and the stanza's failure policy is `skip`
- **THEN** newly produced responses are dropped with a warning logged rather than blocking consumption, each dropped response is counted once in `bridge_kafka_service_response_service_failure_total` with its request id, and the originating message is still settled

#### Scenario: Publication channel saturated in retry mode

- **WHEN** the pipeline's response queue is full and the stanza sets `on_failure: retry`
- **THEN** the response waits for space in the queue, is not dropped, and the originating message is settled only after it is published

#### Scenario: Shutdown while waiting for the publication channel

- **WHEN** the pipeline stops while a response of a `retry` stanza waits for space in the queue
- **THEN** the wait ends within the shutdown timeout, the response is not published by that call and the originating message is not settled, so it is redelivered after restart

#### Scenario: Responses drained without producers

- **WHEN** consumers are active, no producers are configured and a `skip` stanza produces a response for its `response_topic`
- **THEN** the pipeline's publishers still run, fail each response event with a logged "no producer" error, and settle it, so the queue never fills with unconsumed events

#### Scenario: No response topic configured

- **WHEN** a pipeline has no response topic
- **THEN** responses bypass response publication entirely

#### Scenario: Stalled response topic does not affect other pipelines

- **WHEN** pipeline A's response topic rejects every write and pipeline B's response topic is healthy
- **THEN** pipeline B's responses keep being published and settled at their normal rate, and none of pipeline B's responses are dropped because of pipeline A

#### Scenario: Concurrent publication of a busy pipeline

- **WHEN** a fast-mode pipeline has 100 responses queued and each write takes one broker round trip
- **THEN** up to 16 of its responses are being written at the same time, instead of one after another

#### Scenario: Sequential pipeline keeps its order

- **WHEN** a pipeline with `max_conn: 1` produces responses R1, R2, R3 in that order
- **THEN** they are written to the response topic one at a time, in the order R1, R2, R3

### Requirement: Fast-mode in-flight ceiling
In fast mode the system MUST keep at most 512 upstream calls in flight per pipeline. When the ceiling is reached, the pipeline MUST NOT start another call or take another record from Kafka until a call finishes. On shutdown, a dispatch waiting for the ceiling MUST return immediately. The ceiling MUST be stated in the fast-mode startup log line.

#### Scenario: Burst below the ceiling
- **WHEN** 100 records arrive in fast mode while the upstream is slow
- **THEN** 100 calls are in flight concurrently

#### Scenario: Catch-up burst above the ceiling
- **WHEN** 2000 records are available in fast mode and the upstream answers slowly
- **THEN** no more than 512 calls are in flight at any time, and the remaining records are processed as calls finish

#### Scenario: Shutdown at the ceiling
- **WHEN** the pipeline is stopped while 512 calls are in flight and another record waits for a free slot
- **THEN** the waiting dispatch returns immediately and that record is not settled

### Requirement: Upstream connection reuse
For an HTTP upstream the system MUST keep up to the pipeline's concurrency limit (`service.max_conn` in slow mode, the fast-mode ceiling in fast mode) of idle connections to the upstream, so that a steady number of concurrent calls reuses connections instead of opening a new one per call. Pipelines MUST NOT share an HTTP connection pool.

#### Scenario: Steady concurrency reuses connections
- **WHEN** a slow-mode pipeline with `max_conn: 8` makes 200 calls with 8 in flight at a time to a keep-alive HTTP upstream
- **THEN** the upstream sees a small bounded number of new connections over the whole run (the limit plus a small transient overshoot of the client's dialler), far fewer than the number of calls

#### Scenario: Separate pools per pipeline
- **WHEN** two pipelines call two different upstreams
- **THEN** idle connections of one pipeline are not counted against or reused by the other
