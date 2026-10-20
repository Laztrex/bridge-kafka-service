# health-probe Specification

## Purpose
Offers a lightweight liveness probe that orchestration platforms (Kubernetes/OpenShift, load balancers) can hit to confirm the process is responsive, independent of broker connectivity.

## Requirements

### Requirement: Liveness endpoint always reachable
The system MUST register `GET /health` on the HTTP listener bound to `http.addr` from configuration. This endpoint MUST be available even when no consumers, no producers, or no upstream services are reachable.

#### Scenario: Healthy probe
- **WHEN** a client issues `GET /health` against the configured address
- **THEN** the response is HTTP 200 with a JSON body whose `status` field reads `"running"`

#### Scenario: Probe while Kafka is still connecting
- **WHEN** the process has just started and producers/consumers are still retrying their broker connections
- **THEN** `GET /health` already answers 200, because the HTTP listener starts before, and independently of, any Kafka connection

#### Scenario: Probe during partial degradation
- **WHEN** a downstream Kafka broker is unreachable but the HTTP server is otherwise healthy
- **THEN** the health endpoint still answers 200 because it reports only process liveness

### Requirement: HTTP listener starts before Kafka
The HTTP listener MUST be started first during startup and MUST NOT wait for producer or consumer creation. Kafka clients connect in the background with exponential back-off. Orchestrator liveness probes therefore never fail merely because brokers are unreachable at boot.

#### Scenario: Brokers down at boot
- **WHEN** the process starts while every broker is unreachable
- **THEN** `/health` answers 200 within the first seconds and `POST /api/v{version}/publish` answers 503 until a producer is connected

### Requirement: Stable response shape
The response payload MUST be a JSON object including at least `status`. Clients depending on schema versioning MUST NOT receive additional unexpected top-level fields; downstream tooling MAY rely on the documented shape alone.

#### Scenario: Schema stability
- **WHEN** the system is upgraded across versions without changing the handler
- **THEN** `GET /health` continues to return the same fields and order-compatible JSON document

### Requirement: Handler runs without authentication
The endpoint MUST NOT require any authentication header or token; orchestration probes routinely cannot carry credentials.

#### Scenario: Anonymous probe succeeds
- **WHEN** a probe request carries no Authorization header
- **THEN** the endpoint replies successfully anyway

### Requirement: Liveness alias and readiness probe

The listener MUST also serve `GET /healthz` with the same liveness semantics and response as `GET /health` (which stays for existing probes). It MUST serve `GET /readyz`, answering 200 `{"status":"ready"}` only when the shared producer is connected (if any producer is configured) and the consumer client of every active consumer stanza (a stanza with topics) is running, and 503 `{"status":"not ready","reasons":[...]}` otherwise. `GET /status` MUST return the readiness details as JSON (`ready`, `reasons`, `producer_ready`, `consumers_up`, `consumers_total`) with status 200; `consumers_total` MUST count active consumer stanzas only.

#### Scenario: Kafka not connected yet

- **WHEN** the service has started but the producer is not connected
- **THEN** `GET /healthz` and `GET /health` answer 200 and `GET /readyz` answers 503 naming the producer

#### Scenario: Everything connected

- **WHEN** the producer is connected and every consumer client is running
- **THEN** `GET /readyz` answers 200

#### Scenario: Disabled stanza

- **WHEN** the configuration has two consumer stanzas with topics and one without, and both active consumer clients are running
- **THEN** `GET /readyz` answers 200 and `GET /status` reports `consumers_total: 2`

### Requirement: Pipeline retry state in status

`GET /status` MUST include `pipelines`: one entry per active consumer stanza, in configuration order, with `group`, `topics`, `retrying_calls` (records retrying the upstream call), `retrying_publications` (responses retrying their write), `oldest_retry_seconds` (age of the oldest of those since its first failed attempt, `0` when none), `uncommitted` (records handed out by current claims and not yet committed) and `paused` (whether consumption is paused by `SIGUSR1`). Readiness MUST NOT depend on these values.

#### Scenario: Healthy pipeline

- **WHEN** a pipeline processes records and none of them fails
- **THEN** its `pipelines` entry reports `retrying_calls: 0`, `retrying_publications: 0` and `oldest_retry_seconds: 0`

#### Scenario: Record stuck in retry

- **WHEN** a `retry` pipeline with `max_conn: 1` has been retrying one record for 3 minutes while the upstream is unreachable
- **THEN** its entry reports `retrying_calls: 1` and an `oldest_retry_seconds` of about 180, and `GET /readyz` is unaffected

#### Scenario: Records held behind a retry

- **WHEN** a fast-mode pipeline keeps retrying one record while 40 later records of the same partition finish
- **THEN** its entry reports `uncommitted` of at least 41

#### Scenario: Publication retry

- **WHEN** a `retry` stanza's response topic rejects writes and two responses are waiting for their next attempt
- **THEN** its entry reports `retrying_publications: 2`

#### Scenario: Disabled stanza

- **WHEN** the configuration has a stanza without topics
- **THEN** `pipelines` has no entry for it

#### Scenario: Paused process

- **WHEN** one `SIGUSR1` has paused consumption
- **THEN** every `pipelines` entry reports `paused: true`, `GET /readyz` is unaffected, and after the next `SIGUSR1` every entry reports `paused: false`
