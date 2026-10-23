## MODIFIED Requirements

### Requirement: Liveness endpoint always reachable
The system MUST register `GET /health` on the HTTP listener bound to `health.addr` from configuration. This endpoint MUST be available even when no consumers, no producers, or no upstream services are reachable, and until the shutdown drain ends.

#### Scenario: Healthy probe
- **WHEN** a client issues `GET /health` against the configured address
- **THEN** the response is HTTP 200 with a JSON body whose `status` field reads `"running"`

#### Scenario: Probe while Kafka is still connecting
- **WHEN** the process has just started and producers/consumers are still retrying their broker connections
- **THEN** `GET /health` already answers 200, because the HTTP listener starts before, and independently of, any Kafka connection

#### Scenario: Probe during partial degradation
- **WHEN** a downstream Kafka broker is unreachable but the HTTP server is otherwise healthy
- **THEN** the health endpoint still answers 200 because it reports only process liveness

#### Scenario: Probe during shutdown
- **WHEN** SIGTERM has arrived and pipelines are still draining
- **THEN** `GET /health` and `GET /healthz` answer 200, and `GET /readyz` answers 503 with the reason `shutting down`

## ADDED Requirements

### Requirement: Application readiness contract
The application MUST expose `GET /readyz` for an orchestrator readiness probe. It MUST report not ready until the required producer is registered and every active consumer client has initialized, and while shutting down. This check MUST remain independent of liveness and MUST NOT be treated as continuous broker reachability monitoring. Pause and upstream retries MUST NOT change readiness. Kubernetes and Istio probe handling consume the application's result; Kafka consumption remains controlled by the application.

#### Scenario: Clients still initializing
- **WHEN** a required Kafka client has not initialized
- **THEN** `/readyz` answers 503 naming the missing client category while `/healthz` answers 200

#### Scenario: Initialized application is paused or retrying
- **WHEN** all required clients have initialized, shutdown has not begun, and consumption is paused or an upstream call is retrying
- **THEN** `/readyz` answers 200 and `/status` reports the runtime state
