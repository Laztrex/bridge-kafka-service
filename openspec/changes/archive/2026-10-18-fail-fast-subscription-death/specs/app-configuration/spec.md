# Spec Delta

## ADDED Requirements

### Requirement: Kafka version supports consumer groups

When at least one consumer stanza is active, startup MUST fail if `kafka.version` parses to a Kafka version below 0.10.2, the minimum required by consumer groups; the error MUST name the configured value and the minimum. A value that cannot be parsed MUST keep the existing fallback (2.7.0) and MUST NOT stop startup. A configuration without active consumer stanzas MUST NOT be checked.

#### Scenario: Version too old for consumer groups

- **WHEN** `kafka.version` is `0.10.1.0` and a consumer stanza has topics
- **THEN** startup fails before the HTTP server starts, with an error naming `0.10.1.0` and the minimum `0.10.2`

#### Scenario: Minimum version accepted

- **WHEN** `kafka.version` is `0.10.2.0` and a consumer stanza has topics
- **THEN** startup proceeds

#### Scenario: Unparseable version keeps the fallback

- **WHEN** `kafka.version` cannot be parsed
- **THEN** startup proceeds and consumer clients use version 2.7.0

#### Scenario: Producer-only configuration

- **WHEN** `kafka.version` is `0.10.1.0` and no consumer stanza has topics
- **THEN** startup proceeds
