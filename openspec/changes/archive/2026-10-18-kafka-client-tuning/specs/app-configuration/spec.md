# Spec Delta

## MODIFIED Requirements

### Requirement: Separate Kafka client tuning files

Sarama consumer and producer tunables MUST be read from `configs/config_consumer.yaml` and `configs/config_producer.yaml` respectively and overlaid on Sarama defaults. Each file MUST be loaded and validated once at startup, before the HTTP server starts, and only when its client is needed: the consumer file when at least one consumer stanza is active, the producer file when at least one producer is configured. A missing or unparsable file that is needed MUST stop startup with an error naming the file, rather than silently using defaults or being retried. Client creation, including its retries, MUST use the settings loaded at startup.

#### Scenario: Producer file missing

- **WHEN** a producer is configured and `configs/config_producer.yaml` is absent at startup
- **THEN** startup fails before the HTTP server starts, with an error naming the file

#### Scenario: Consumer file unparsable

- **WHEN** a consumer stanza is active and `configs/config_consumer.yaml` is not valid YAML
- **THEN** startup fails before the HTTP server starts, with an error naming the file

#### Scenario: File of an unused client

- **WHEN** no producer is configured and `configs/config_producer.yaml` is absent
- **THEN** startup proceeds

## ADDED Requirements

### Requirement: Configurable rebalance strategy

`consumer.group.rebalance.strategy` in the consumer tuning file MUST select the consumer group rebalance strategy for every consumer: `range`, `roundrobin`, `sticky` or `cooperative-sticky`. When the key is absent, the strategy MUST be `range`. Any other value MUST stop startup with an error naming the key, the value and the allowed values.

#### Scenario: Strategy omitted

- **WHEN** `config_consumer.yaml` does not set `consumer.group.rebalance.strategy`
- **THEN** every consumer joins its group with the `range` strategy

#### Scenario: Sticky selected

- **WHEN** `config_consumer.yaml` sets `consumer.group.rebalance.strategy: sticky`
- **THEN** every consumer joins its group with the `sticky` strategy and the process does not crash

#### Scenario: Unknown strategy

- **WHEN** `config_consumer.yaml` sets `consumer.group.rebalance.strategy: fair`
- **THEN** startup fails with an error naming the key, `fair` and the allowed values

### Requirement: Tuning keys are checked at startup

When a tuning file is loaded, every key that matches no Sarama setting and is not one of the application's own keys (`OffsetsAutoCommitInterval`) MUST be logged as a warning naming the file and the key's full dotted path; such keys MUST NOT stop startup. A key whose Sarama setting cannot be expressed in YAML (an interface- or function-typed setting, other than `consumer.group.rebalance.strategy`) MUST be ignored with a warning naming the file and the key, and MUST NOT crash the process or stop startup.

#### Scenario: Misspelled key

- **WHEN** `config_producer.yaml` contains `net.maxopenrequest: 1`
- **THEN** a warning naming `config_producer.yaml` and `net.maxopenrequest` is logged, startup proceeds and the Sarama default for `net.maxopenrequests` is kept

#### Scenario: Key that cannot be set from YAML

- **WHEN** `config_producer.yaml` contains `producer.partitioner: random`
- **THEN** a warning naming the key is logged, the key is ignored, and the process keeps running

#### Scenario: Known keys only

- **WHEN** a tuning file contains only valid Sarama keys and `OffsetsAutoCommitInterval`
- **THEN** no tuning-key warning is logged

### Requirement: Consumer group timing defaults

Unless the consumer tuning file overrides them, every consumer MUST use a group session timeout of 60 seconds, a heartbeat interval of 10 seconds and a maximum processing time of 10 seconds. Values set in the consumer tuning file (`consumer.group.session.timeout`, `consumer.group.heartbeat.interval`, `consumer.maxprocessingtime`) MUST take precedence over these defaults.

#### Scenario: Defaults applied

- **WHEN** the consumer tuning file sets none of these keys
- **THEN** consumers use a 60s session timeout, a 10s heartbeat interval and a 10s maximum processing time

#### Scenario: Session timeout overridden

- **WHEN** the consumer tuning file sets `consumer.group.session.timeout: 20s`
- **THEN** consumers use a 20s session timeout and keep the other defaults
