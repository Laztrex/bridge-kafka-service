## MODIFIED Requirements

### Requirement: Legacy wire identifiers are frozen
The following external identifiers MUST keep their legacy form until an explicit migration change replaces them:
- HTTP headers `x-bridge-kafka-produce-topic` and the custom prefix `x-bridge-kafka-header-`;
- ALS `UpstreamCluster = "kafka-controller-cluster"` and fallback `ForwardedFor = "controller-kafka"`.

Metric names are not frozen: they carry the service prefix `bridge_kafka_service_` (spec observability-audit).

#### Scenario: Caller built against the old name
- **WHEN** a client publishes with `x-bridge-kafka-produce-topic`
- **THEN** the request is accepted exactly as before the rename

#### Scenario: Metric name
- **WHEN** a record is consumed
- **THEN** its metric lines are named `bridge_kafka_service_consume_*`, and no line starts with `controller_kafka_`
