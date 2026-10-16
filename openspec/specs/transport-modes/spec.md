# transport-modes Specification

## Purpose
Lets each routing pipeline choose between opening a fresh upstream connection for every message and reusing a bounded pool of concurrent connections, selected independently per consumer stanza.

## Requirements

### Requirement: Mode selection from configuration
The system MUST select between fast mode and slow mode based solely on the value of `service.max_conn` on the consumer stanza. A value greater than zero selects slow mode; zero or any negative value selects fast mode.

#### Scenario: Fast mode chosen
- **WHEN** a pipeline starts with `service.max_conn: 0` (or unset)
- **THEN** each consumed record triggers an independent outbound HTTP request without concurrency limiting

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
The system MUST funnel every successful upstream response into a shared response-publication channel so that publishing failures, slow brokers, or pause/resume toggles never stall consumption. The exception is a stanza with `on_failure: retry`: there a saturated channel MUST hold the upstream call until space frees, so the response is never dropped and backpressure stops consumption instead.

#### Scenario: Publication channel saturated
- **WHEN** the response channel is full because publishing is slow and the stanza's failure policy is `skip`
- **THEN** newly produced responses are dropped with a warning logged rather than blocking consumption, and the originating message is still settled

#### Scenario: Publication channel saturated in retry mode
- **WHEN** the response channel is full and the stanza sets `on_failure: retry`
- **THEN** the response waits for space in the channel, is not dropped, and the originating message is settled only after it is published

#### Scenario: Shutdown while waiting for the publication channel
- **WHEN** the pipeline stops while a response of a `retry` stanza waits for space in the channel
- **THEN** the wait ends within the shutdown timeout, the response is not published by that call and the originating message is not settled, so it is redelivered after restart

#### Scenario: Responses drained without producers
- **WHEN** consumers are active but no producers are configured (or producer creation failed)
- **THEN** the response-publication loop still runs, fails each response event with a logged "no producer" error, and settles it, so the channel never fills with unconsumed events

#### Scenario: No response topic configured
- **WHEN** a pipeline has no response topic
- **THEN** responses bypass the publication channel entirely
