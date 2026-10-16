# Spec Delta

## MODIFIED Requirements

### Requirement: One producer shared by all producer topics
When at least one `producer:` entry is configured, the system MUST create exactly one Sarama sync producer and expose it under every configured producer topic; the topic list acts as an allow-list for publishing. On shutdown the producer MUST be closed exactly once. If the producer cannot be created within the retry window while any active consumer stanza has a `response_topic`, the process MUST exit non-zero before any consumer reads a record. Startup ordering and HTTP behaviour while the producer is unavailable are specified by `health-probe` and `control-api`.

#### Scenario: Three producer topics
- **WHEN** three producer topics are configured
- **THEN** one producer is created, all three topics resolve to it, and it is closed once on exit

#### Scenario: Producer creation fails permanently
- **WHEN** the producer cannot be created after the retry window and no active consumer stanza has a `response_topic`
- **THEN** the error is logged, no topic is registered, and consumption still runs

#### Scenario: Producer creation fails permanently while responses are expected
- **WHEN** the producer cannot be created after the retry window and at least one active consumer stanza has a `response_topic`
- **THEN** the error is logged, no consumer reads a record, and the process exits with a non-zero code so the supervisor restarts it

#### Scenario: Consumers wait for the producer
- **WHEN** a consumer client is ready before the producer and a consumer stanza has a `response_topic`
- **THEN** that consumer does not read records until the producer is connected
