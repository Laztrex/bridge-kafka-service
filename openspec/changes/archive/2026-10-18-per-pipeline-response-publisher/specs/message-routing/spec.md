# Spec Delta

## MODIFIED Requirements

### Requirement: Response publication retry under retry policy

For a stanza with `on_failure: retry`, a transient failure to write the upstream response to `response_topic` MUST NOT settle the offset. The system MUST repeat the write with the backoff of "Retry until the upstream recovers" until it succeeds or the service stops; the upstream MUST NOT be called again. Each failed attempt MUST be logged as a warning; metrics and audit MUST record only the final outcome of the response. A response whose write is being repeated MUST hold back only the responses of its own pipeline, never those of other pipelines.

#### Scenario: Broker rejects writes temporarily

- **WHEN** a retry stanza's response cannot be written for 2 minutes and the broker then accepts writes again
- **THEN** the response is published exactly once after recovery, the upstream was called once, and the offset is settled only after the publication succeeds

#### Scenario: One outcome per response

- **WHEN** writing a retry stanza's response fails twice and succeeds on the third attempt
- **THEN** two warnings are logged and exactly one success, and no failure, is recorded for the response

#### Scenario: Shutdown while publication is retried

- **WHEN** the service stops while a retry stanza's response is waiting for its next write attempt
- **THEN** the wait ends, the response is not published by that attempt, and the originating offset is not settled, so the record is redelivered after restart

#### Scenario: Non-transient publication error

- **WHEN** writing a retry stanza's response fails because the record exceeds the broker's size limit
- **THEN** the write is not repeated, the failure is recorded and logged with the request id, and the offset is settled

#### Scenario: Skip stanza unaffected

- **WHEN** writing a response of a stanza with `on_failure: skip` fails
- **THEN** the write is not repeated and the offset is settled

#### Scenario: Retried publication is isolated to its pipeline

- **WHEN** a retry stanza's response topic rejects writes and its responses are being retried, while another stanza publishes to a healthy topic
- **THEN** the other stanza's responses are published without waiting for the retries, and only the retry stanza's own pipeline is held back
