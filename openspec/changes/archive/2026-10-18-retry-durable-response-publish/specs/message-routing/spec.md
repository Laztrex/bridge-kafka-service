# Spec Delta

## MODIFIED Requirements

### Requirement: At-least-once offset settlement
The system MUST NOT commit a Kafka offset until the corresponding message has been fully processed: the upstream call has finished and its response has been published to the response topic, or processing has terminally failed (upstream failure under `skip`, poison record under `retry`, dropped response under `skip`, failed response publication under `skip`, non-transient response publication error, missing producer under `skip`). Offsets MUST be marked per partition in consumption order: an offset MUST NOT be marked while any earlier offset of the same claim is still in flight, even if later messages finish first. Marked offsets MUST be committed to the broker periodically and on a clean session end, whatever the Kafka client tuning file says, so a restarted or rebalanced group resumes after the last settled record. Terminal failures are recorded in metrics and audit; dead-lettering is out of scope.

#### Scenario: Crash mid-flight redelivers
- **WHEN** the process terminates while an upstream call for offset N is still in flight
- **THEN** offset N is not committed and the record is redelivered after restart

#### Scenario: Out-of-order completion
- **WHEN** offsets 1, 2 and 3 are in flight concurrently and 3 then 2 complete before 1
- **THEN** no offset is marked until 1 completes, after which 1, 2 and 3 are marked together

#### Scenario: Terminal failure does not stall the partition
- **WHEN** the upstream call for a record fails and the stanza's failure policy is `skip`
- **THEN** the failure is recorded and the offset is settled so later offsets can be committed

#### Scenario: Settlement is idempotent
- **WHEN** the same message's settlement is signalled more than once
- **THEN** the offset is marked exactly once

#### Scenario: Settled offsets survive a restart
- **WHEN** offsets up to N have been settled, the commit interval has elapsed and the process restarts
- **THEN** the group resumes from offset N+1, not from the configured initial offset

#### Scenario: Tuning file disables auto-commit
- **WHEN** `config_consumer.yaml` sets `consumer.offsets.autocommit.enable: false`
- **THEN** settled offsets are still committed to the broker

#### Scenario: Response publication fails under skip
- **WHEN** a stanza with `on_failure: skip` gets an upstream response and writing it to `response_topic` fails
- **THEN** the failure is recorded and the offset is settled, as before

## ADDED Requirements

### Requirement: Response publication retry under retry policy
For a stanza with `on_failure: retry`, a transient failure to write the upstream response to `response_topic` MUST NOT settle the offset. The system MUST repeat the write with the backoff of "Retry until the upstream recovers" until it succeeds or the service stops; the upstream MUST NOT be called again. Each failed attempt MUST be logged as a warning; metrics and audit MUST record only the final outcome of the response.

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

### Requirement: Retry response topic must be served by a producer
A stanza with `on_failure: retry` and a non-empty `response_topic` MUST name a topic that is listed under `producer:`; otherwise startup MUST fail with an error naming the stanza and the topic. Stanzas with `on_failure: skip` are not checked.

#### Scenario: Retry stanza with unlisted response topic
- **WHEN** a stanza sets `on_failure: retry` and `response_topic: out.a`, and no `producer:` entry lists `out.a`
- **THEN** startup fails with an error naming the stanza and `out.a`

#### Scenario: Retry stanza with listed response topic
- **WHEN** a stanza sets `on_failure: retry`, `service.max_conn: 3` and `response_topic: out.a`, and a `producer:` entry lists `out.a`
- **THEN** the service starts normally

#### Scenario: Skip stanza with unlisted response topic
- **WHEN** a stanza with `on_failure: skip` names a `response_topic` that no `producer:` entry lists
- **THEN** startup is not aborted by this check
