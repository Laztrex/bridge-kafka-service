# runtime-control Specification

## Purpose
Lets operators stop the process cleanly and pause or resume Kafka consumption on demand without restarting the service.

## Requirements

### Requirement: Graceful termination on standard signals
The system MUST treat both `SIGINT` and `SIGTERM` as a request to shut down gracefully by cancelling a single root context. Shutdown MUST proceed in this order: mark the service as draining, so that publish requests answer 503 and `/readyz` reports `shutting down` while `/health`, `/healthz` and `/status` keep answering; stop claiming Kafka records and wait for in-flight publish requests; wait for every pipeline to terminate; publish responses already queued; close the shared producer exactly once; flush and close metric and audit sinks; shut down the HTTP server last. The whole sequence is bounded by `timeouts.shutdown`. Records not fully processed are not committed and are redelivered after restart.

#### Scenario: SIGTERM mid-run
- **WHEN** an operator sends `SIGTERM` while messages are being consumed and forwarded
- **THEN** no new records are claimed, queued metric and audit events are written to their files, the producer and consumer clients are closed, all goroutines started by the pipelines return and the process exits with status 0

#### Scenario: Probes during the drain
- **WHEN** `SIGTERM` arrives while an upstream call is in progress
- **THEN** until that call finishes, `GET /healthz` answers 200, `GET /readyz` answers 503 with `shutting down`, and a new publish request answers 503

#### Scenario: Events submitted after shutdown
- **WHEN** a straggler goroutine records an event after the sinks were closed
- **THEN** the call returns without blocking and the event is dropped

#### Scenario: Repeated signal still terminates
- **WHEN** shutdown is already in progress and another `SIGTERM` arrives
- **THEN** the second signal does not corrupt closing state and the process still exits cleanly

### Requirement: Fail fast on permanent consumer failure

The process MUST fail fast when a subscription cannot start for a reason that retrying cannot fix: its upstream cannot be constructed, or its consumer group cannot be created from a connected client. It MUST then shut down gracefully and exit with a non-zero status, and the terminating error MUST name the affected topics. An unreachable broker is not such a reason: producer and consumer clients MUST keep retrying with exponential backoff and jitter until shutdown, while the service stays up and `/readyz` names the clients that are not connected.

Once a subscription has started, transient errors from the consume loop MUST NOT terminate the process (see "Consumer-loop resilience during transient errors").

#### Scenario: Brokers unreachable beyond the retry window

- **WHEN** consumer client creation keeps failing for longer than five minutes
- **THEN** the process stays up, `/healthz` answers 200, `/readyz` answers 503 naming the consumer, and the client is created once the broker is reachable

#### Scenario: Cancellation interrupts retries

- **WHEN** shutdown is requested while a client creation is being retried
- **THEN** the retry stops immediately and no failure is reported

#### Scenario: Upstream cannot be constructed

- **WHEN** the upstream for a consumer stanza cannot be constructed at subscription start
- **THEN** `Connect` returns an error naming the stanza's topics and the construction failure, the process exits with status 1, and no pipeline for that stanza runs

#### Scenario: Consumer group cannot be created

- **WHEN** the consumer group for a stanza cannot be created at subscription start, after its client was created successfully
- **THEN** `Connect` returns an error naming the stanza's topics and the group creation failure, and the process exits with status 1

#### Scenario: Clean stop is not a failure

- **WHEN** a subscription ends because shutdown was requested or the consumer group was closed
- **THEN** no failure is reported and the process continues its normal shutdown path

### Requirement: Request context cancellation
Request-scoped work MUST derive its context from the incoming request (`r.Context()`), not from a detached background context, so client disconnects and server shutdown propagate.

#### Scenario: Client disconnects
- **WHEN** an HTTP publish client aborts the request
- **THEN** the context seen by downstream handlers is cancelled

### Requirement: Pause and resume via SIGUSR1
The system MUST toggle between active and paused consumption when it receives `SIGUSR1`. The pause MUST be one process-wide state that every running pipeline follows, so one signal affects all consumers. The first signal pauses every partition of every consumer; the next signal resumes them; subsequent signals keep toggling. While paused, no claim MUST hand out a record, including records already fetched, and a partition assigned to the process while paused, for example by a rebalance, MUST be paused as soon as its claim starts.

#### Scenario: First SIGUSR1 pauses
- **WHEN** consumption is active and a single `SIGUSR1` is delivered
- **THEN** all partitions across all consumer groups in the process enter a paused state and no further records are claimed until resumed

#### Scenario: Second SIGUSR1 resumes
- **WHEN** consumption has been paused and another `SIGUSR1` is delivered
- **THEN** every previously paused partition resumes claiming records from its last committed offset

#### Scenario: Several consumers in one process
- **WHEN** three pipelines run and one `SIGUSR1` is delivered
- **THEN** all three pipelines pause, not just one of them

#### Scenario: Alternating toggles
- **WHEN** three `SIGUSR1` signals arrive in sequence
- **THEN** the pipeline ends up paused if the count is odd, running if even

#### Scenario: Partition assigned while paused
- **WHEN** consumption is paused and a rebalance assigns the process a partition it did not own
- **THEN** that partition hands out no record until the next `SIGUSR1`, and that signal resumes it together with the others

#### Scenario: Records already buffered
- **WHEN** a claim has fetched records that it has not handed out yet and `SIGUSR1` pauses consumption
- **THEN** none of those records reaches the upstream until consumption is resumed

#### Scenario: Shutdown while paused
- **WHEN** the process is paused and receives `SIGTERM`
- **THEN** it shuts down within the shutdown timeout, as when it is not paused

### Requirement: Consumer-loop resilience during transient errors
If the underlying Sarama consumer reports an error that is not "group closed", the system MUST log the error, wait a short back-off interval, and re-enter the consume loop instead of terminating the process.

#### Scenario: Recoverable consumer error
- **WHEN** Sarama surfaces a non-fatal transport error during claiming
- **THEN** consumption restarts after a brief wait and no goroutine leak accumulates over repeated errors

#### Scenario: Group close propagates shutdown
- **WHEN** Sarama surfaces `ErrClosedConsumerGroup`
- **THEN** the consume loop returns and the calling pipeline can finish shutting down

### Requirement: Readiness wait is cancellable
A consumer MUST signal readiness exactly once, when Sarama invokes its first `Setup`; later rebalances MUST NOT signal again. Waiting for that first signal MUST NOT block forever: if consumption ends or the pipeline context is cancelled before the first `Setup`, the wait MUST return so the pipeline can shut down.

#### Scenario: Consumption ends before readiness
- **WHEN** the consume loop exits before Sarama calls `Setup`
- **THEN** the pipeline stops waiting for readiness and proceeds to shutdown

#### Scenario: First session setup fires once
- **WHEN** Sarama invokes `Setup` for the first time after startup
- **THEN** the readiness channel is closed and a single readiness log line is emitted

#### Scenario: Rebalance does not re-fire
- **WHEN** a later rebalance triggers another `Setup`
- **THEN** no additional readiness signal is emitted
