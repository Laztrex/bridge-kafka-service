# observability-audit Specification

## Purpose
Records every event the system processes as a structured metric line and as an Envoy-compatible access-log entry, delivering the latter to either a remote gRPC sink or a local file so business traceability is preserved in both connected and disconnected deployments.

## Requirements

### Requirement: Four-event taxonomy
The system MUST recognise four distinct event types tied to its lifecycle: record consumed from Kafka (`consume`), record produced to Kafka (`produce`), outbound call dispatched to upstream service (`request_service`), and upstream response routed back into Kafka (`response_service`). Each event type maps to a stable pair of human-readable label and set of metric names.

Metric names keep the legacy `controller_kafka_<event>_{success_total,failure_total,requests_total,duration_ms}` form (see spec project-identity).

#### Scenario: Swallowed errors are logged
- **WHEN** the metric or audit goroutine receives an error from its error channel
- **THEN** it is logged at error level with a descriptive message

#### Scenario: All four event types recorded on round-trip
- **WHEN** one record travels through consume → request_service → produce path
- **THEN** exactly three events are emitted with their respective labels and metrics

### Requirement: Per-event metric lines written to local file
For every event the system MUST append a JSON-formatted log line containing the Request-Id, the metric name (specific to event type), and the metric value (success counter, failure counter, total requests, or duration in milliseconds). Lines MUST be written sequentially; partial writes MUST NOT interleave within a single line. An event's duration is measured from that event's own start to the moment the event is closed; it MUST NOT depend on any other event and MUST NOT include time the closed event spends waiting to be written.

#### Scenario: Success increments success counter
- **WHEN** an event of any type succeeds
- **THEN** the corresponding `<event>_success_total` line reflects an increment for that event's Request-Id batch

#### Scenario: Failure increments failure counter only
- **WHEN** an event fails
- **THEN** `<event>_failure_total` increments but `<event>_success_total` does not

#### Scenario: Duration captured per event
- **WHEN** an event closes after measurable elapsed time
- **THEN** the duration-in-milliseconds metric carries a non-negative integer equal to the wall-clock time between that event's start and its close

#### Scenario: Concurrent events are timed independently
- **WHEN** event A starts, event B starts later and closes first, and A closes after B
- **THEN** B's duration covers only B's own span and A's duration covers A's whole span, regardless of the interleaving

#### Scenario: Queueing delay is not counted
- **WHEN** a closed event waits in the metrics queue before being written
- **THEN** its duration reflects the time up to its close, not the time until it was written

#### Scenario: Event closed without a start
- **WHEN** an event is closed without having been started
- **THEN** its duration is `0` and processing continues

### Requirement: gRPC delivery when configured
When an operator supplies a gRPC endpoint address, the system MUST attempt to deliver each audit entry over a streaming gRPC client using the Envoy Access Log Service protocol. The connection is established lazily and never blocks startup. Successful delivery MAY close the stream; transient errors, including a rejecting or unreachable receiver, MUST NOT abort processing or lose the file fallback entry.

#### Scenario: Endpoint reachable
- **WHEN** the configured gRPC endpoint accepts the connection over insecure transport
- **THEN** each audit entry is sent via a fresh short-lived stream that is closed after receipt acknowledgement

#### Scenario: Endpoint unreachable at boot
- **WHEN** the configured endpoint cannot be reached (or its address is unusable)
- **THEN** startup still succeeds, each delivery attempt fails with a logged error (or a warning when the address cannot be dialled at all, in which case no gRPC client is kept), and audit entries still flow to the local fallback described below

#### Scenario: Receiver rejects entries
- **WHEN** the gRPC receiver answers with an error
- **THEN** the error is logged, processing continues, and every entry is still written to the local audit file

### Requirement: Local file fallback always active
Independent of the gRPC configuration status, the system MUST append every audit entry as a JSON-lines file next to the main metric file. This guarantees zero-loss auditing even in disconnected environments.

#### Scenario: Audit persists without network
- **WHEN** no gRPC endpoint is configured
- **THEN** each audit entry appears verbatim in the local audit file synchronously after emission

#### Scenario: Crash recovery
- **WHEN** the process restarts after abnormal termination
- **THEN** previously appended audit lines remain intact (append-mode writes)

### Requirement: Flush on shutdown and non-blocking producers
On shutdown the metric and audit loops MUST write every event already queued before files and the gRPC connection are closed; all resources MUST be closed even if closing one of them fails. Reporting an audit error MUST never block the audit loop itself (the error queue is bounded and drained by that same loop).

#### Scenario: Queued events survive shutdown
- **WHEN** events are queued and shutdown starts
- **THEN** the metric file contains a line set for every queued event and the audit file contains every queued entry

#### Scenario: Repeated audit failures
- **WHEN** the audit file fails to write several times in a row
- **THEN** the loop keeps running, excess errors are logged directly, and no deadlock occurs

#### Scenario: Shutdown is idempotent
- **WHEN** shutdown is requested twice
- **THEN** the second call is a no-op

### Requirement: Outbound HTTP call diagnostics
When the system makes an outbound call to a configured upstream, it MUST emit DNS-resolution logs at debug level for every request and additional request/response summary logs covering method, URL, status code, and elapsed seconds. Failures MUST include the originating host so operators can correlate incidents to specific upstreams.

#### Scenario: Upstream reachable
- **WHEN** an HTTP call resolves its target hostname successfully
- **THEN** a debug log lists the resolved addresses before the request fires, followed by an info log summarising method/path/status/latency on completion

#### Scenario: DNS resolution failure
- **WHEN** the target hostname cannot be resolved
- **THEN** an error log specifies the failing host and the call aborts without retrying DNS

### Requirement: Envoy ALS v1.3.0 audit-controller compliance
Every entry emitted under "One ALS entry per pipeline boundary" MUST additionally satisfy each of the following field-level invariants so that audit-controller v1.3.0's Validate() does not drop the event:

- `HTTPAccessLogEntry.CommonProperties.AccessLogType` MUST carry a numeric envoy-enum value other than `NotSet`/`Unspecified` matching the boundary type (`AccessLogType_DownstreamStart` or `AccessLogType_DownstreamEnd`). String labels from `consts.AccessLogType*` MUST be translated via `lookupAccessLogType(...)` before wire emission.
- `HTTPAccessLogEntry.CommonProperties.Duration` MUST carry a non-zero `google.protobuf.Duration` measuring wall-clock elapsed time between transaction start and entry emission. The start is the explicit start time supplied by the emitting boundary; for the ingress entry it is the start of the same record's consume event, so the duration covers processing up to hand-over.
- `HTTPAccessLogEntry.CommonProperties.UpstreamCluster` MUST equal the stable string literal `"kafka-controller-cluster"` identifying the emitting deployment. The literal is a wire-level identifier agreed with audit-controller and is intentionally kept stable across the project rename to `bridge-kafka-service`.
- `HTTPAccessLogEntry.Request.ForwardedFor` MUST be a non-empty string. Resolution order: trimmed inbound `x-forwarded-for` header value when present, falling back to the stable local placeholder string `"controller-kafka"` (legacy wire identifier, intentionally unchanged by the project rename).
- All required fields mentioned in the parent's scenarios (request.{headers,body,trailers}, response.{headers,body,trailers} sub-fields under filter-metadata Struct keyed `"envoy.<dir>.audit-controller.body.lua"`, `Identifier.LogName = "envoy-ingress-als"` / `"envoy-egress-als"`) MUST remain populated regardless of ingress/egress direction.

#### Scenario: Ingress event lands intact at audit-controller v1.3.0 receiver
- **WHEN** a record finishes initial consumption parsing
- **THEN** the produced ALS entry carries `Identifier.LogName = "envoy-ingress-als"`, `CommonProperties.AccessLogType = AccessLogType_DownstreamStart`, a positive `Duration`, `UpstreamCluster = "kafka-controller-cluster"`, and a non-empty `Request.ForwardedFor`
- **AND** the filter-metadata Struct for key `"envoy.ingress.audit-controller.body.lua"` contains all six expected sub-fields populated
- **AND** `audit-controller.Validate()` accepts the entry without recording a `user_node_is_empty` (or equivalent field-emptiness) drop metric

#### Scenario: Egress event lands intact at audit-controller v1.3.0 receiver
- **WHEN** a response publish completes successfully
- **THEN** the produced ALS entry mirrors the ingress invariants for the egress direction (`"envoy-egress-als"` log-name, `AccessLogType_DownstreamEnd` numeric enum value, `request-trailers`/`response-body` non-null placeholders, etc.)

#### Scenario: Header map completeness preserved
- **WHEN** the inbound record carried custom headers including `x-forwarded-for`
- **THEN** that header appears verbatim under `request.headers` inside the filter-metadata Struct
- **AND** `Request.ForwardedFor` equals the header value rather than the fallback placeholder

#### Scenario: Fallback forward identity when upstream omits x-forwarded-for
- **WHEN** the inbound record carries no `x-forwarded-for` header
- **THEN** `Request.ForwardedFor` carries the stable local fallback string `"controller-kafka"` rather than an empty value
- **AND** the emitted event still satisfies the receiver-side non-empty UserNode invariant

#### Scenario: Ingress duration covers processing
- **WHEN** a record's parsing and hand-over take measurable time
- **THEN** the ingress entry's `Duration` is at least that elapsed time (measured from the record's claim), not merely the time between hand-over and emission

### Requirement: One ALS entry per pipeline boundary
Every observable transition at pipeline boundaries (downstream-start, downstream-end, business-request-transferred) MUST materialise as an Envoy-style HTTP access-log entry that identifies ingress vs egress side via distinct log-name strings, attaches the header map and a body snapshot, and references the current Request-Id as the stream identifier. Field-level invariants (header completeness, forwarded-for, cluster, duration, access-log type) are specified by "Envoy ALS v1.3.0 audit-controller compliance".

#### Scenario: Ingress entry on consumption
- **WHEN** a record finishes initial consumption parsing
- **THEN** an ingress-type ALS entry is emitted with the parsed headers and the original payload

#### Scenario: Egress entry on publish
- **WHEN** a response or manual publish completes
- **THEN** an egress-type ALS entry is emitted with the published payload and shared headers
