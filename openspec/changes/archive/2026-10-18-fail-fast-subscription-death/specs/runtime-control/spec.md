# Spec Delta

## MODIFIED Requirements

### Requirement: Fail fast on permanent consumer failure

The process MUST fail fast on any permanent inability to consume. If a consumer client cannot be created within the retry window (default five minutes of exponential back-off), or if a subscription cannot start because its upstream cannot be constructed or its consumer group cannot be created, the process MUST shut down gracefully and exit with a non-zero status instead of staying alive without consuming, so a supervisor can restart it. The terminating error MUST name the affected topics.

Once a subscription has started, transient errors from the consume loop MUST NOT terminate the process (see "Consumer-loop resilience during transient errors"); only the inability to start consuming is permanent.

#### Scenario: Brokers unreachable beyond the retry window

- **WHEN** consumer client creation keeps failing until the retry window elapses
- **THEN** `Connect` returns an error naming the affected topics and the process exits with status 1

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
