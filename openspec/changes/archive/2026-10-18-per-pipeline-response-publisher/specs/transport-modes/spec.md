# Spec Delta

## MODIFIED Requirements

### Requirement: Response decoupling from request path

The system MUST funnel every successful upstream response of a pipeline into that pipeline's own response-publication queue so that publishing failures, slow brokers, or pause/resume toggles never stall consumption. A pipeline's responses MUST NOT wait behind the responses of another pipeline: a response topic that is slow or rejects writes MUST hold back only the pipeline that publishes to it. Each pipeline MUST publish up to `min(L, 16)` responses concurrently, where `L` is the pipeline's upstream concurrency limit (`max_conn`, or the fast-mode ceiling); a pipeline with `max_conn: 1` therefore publishes one response at a time, in the order its upstream calls complete. The exception to non-blocking hand-off is a stanza with `on_failure: retry`: there a saturated queue MUST hold the upstream call until space frees, so the response is never dropped and backpressure stops consumption instead.

#### Scenario: Publication channel saturated

- **WHEN** the pipeline's response queue is full because publishing is slow and the stanza's failure policy is `skip`
- **THEN** newly produced responses are dropped with a warning logged rather than blocking consumption, and the originating message is still settled

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
