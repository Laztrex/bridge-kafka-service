## MODIFIED Requirements

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
