## MODIFIED Requirements

### Requirement: Four-event taxonomy
The system MUST recognise four distinct event types tied to its lifecycle: record consumed from Kafka (`consume`), record produced to Kafka (`produce`), outbound call dispatched to upstream service (`request_service`), and upstream response routed back into Kafka (`response_service`). Each event type maps to a stable pair of human-readable label and set of metric names.

Metric names are `bridge_kafka_service_<event>_{success_total,failure_total,requests_total,duration_ms}`; the resource metrics are `bridge_kafka_service_pod_cpu_usage` and `bridge_kafka_service_pod_memory_usage`.

#### Scenario: Swallowed errors are logged
- **WHEN** the metric or audit goroutine receives an error from its error channel
- **THEN** it is logged at error level with a descriptive message

#### Scenario: All four event types recorded on round-trip
- **WHEN** one record travels through consume → request_service → produce path
- **THEN** exactly three events are emitted with their respective labels and metrics

#### Scenario: Metric prefix
- **WHEN** a `request_service` event fails
- **THEN** the metric file gets `bridge_kafka_service_request_service_failure_total` with value `1`
