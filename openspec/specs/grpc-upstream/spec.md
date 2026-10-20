# grpc-upstream Specification

## Purpose
Defines how a consumed Kafka record is delivered to a gRPC upstream service and how the outcome is interpreted, so a gRPC pipeline behaves exactly like an HTTP one apart from the call itself.

## Requirements

### Requirement: Predict call mapping
For `service.protocol: grpc` the system MUST call the unary method `/controller_kafka.BaseModelInterface/Predict` on `{service.name}:{service.port}`. The request carries all Kafka record headers in `details` and the normalised JSON payload in `data`. The `x-request-id` value MUST additionally be sent as gRPC metadata under the same key. Headers gRPC cannot carry, in `details` or in that metadata copy, follow the `tracking-enrichment` rules for required headers and for headers the transport cannot carry. The response `data` becomes the response body; response header and trailer metadata become response headers.

#### Scenario: Record forwarded
- **WHEN** a record with headers and payload is consumed on a gRPC pipeline
- **THEN** the service receives the payload as `data`, every record header in `details`, and `x-request-id` as metadata

#### Scenario: Metadata becomes response headers
- **WHEN** the service returns header or trailer metadata
- **THEN** those entries are carried as response headers into the response record

#### Scenario: Request id not valid as metadata
- **WHEN** the required list is empty and a record's `x-request-id` contains a non-ASCII character
- **THEN** the call is made with `x-request-id` in `details` but not in metadata, and a warning names the header and the request id

### Requirement: gRPC status semantics
A call that fails because the service could not be reached in time (status `Unavailable`, `DeadlineExceeded` or `Canceled`, or no status at all because the connection failed) MUST be a transport failure: recorded as failure in metrics and audit, nothing published, offset settled. Any other non-OK status means the service answered: it MUST be published as a response record with an empty body and the headers `grpc-status` (the status code name, e.g. `InvalidArgument`) and `grpc-message` (carried, like every response header, in net/http canonical form: `Grpc-Status`, `Grpc-Message`), and the request-service event is recorded as success, mirroring HTTP where any received response is an answer. An OK status publishes the response `data`.

#### Scenario: Service unavailable
- **WHEN** the service is down or the connection is refused
- **THEN** the event is recorded as failure, nothing is published and the offset is settled

#### Scenario: Deadline exceeded
- **WHEN** the call exceeds the consumer's timeout (default 30 seconds)
- **THEN** it is a transport failure as above

#### Scenario: Application error status
- **WHEN** the service answers with status `InvalidArgument` and a message
- **THEN** a response record with empty body and headers `Grpc-Status: InvalidArgument` and `Grpc-Message` is published, and the event is recorded as success

#### Scenario: Successful response
- **WHEN** the service answers OK with `data`
- **THEN** `data` is published as the response body with tracking headers carried over

### Requirement: Connection lifecycle
Each gRPC pipeline MUST use one long-lived client connection, established lazily so that an unreachable service never blocks startup, and closed when the pipeline stops after in-flight calls have drained. A call interrupted by pipeline shutdown MUST NOT be recorded as a failure and MUST NOT settle its offset, so the record is redelivered after restart.

#### Scenario: Service down at startup
- **WHEN** the gRPC service is unreachable when the process starts
- **THEN** startup succeeds and each record fails as a transport failure until the service is reachable

#### Scenario: Shutdown with calls in flight
- **WHEN** a termination signal arrives while calls are in flight
- **THEN** calls finishing within the drain window complete normally; calls still running afterwards are abandoned without settling their offsets

### Requirement: Message size limits

Send and receive message size limits of the gRPC client MUST be 16 MiB, so payloads larger than the gRPC default of 4 MiB are handled by the call. A message over the limit makes the call end with status `ResourceExhausted`, which, like any other status outside the transport-failure set, is published as an application answer (the requester learns that its request or the service's response was too large). Publishing a response is bounded separately by the Kafka record limit (`producer.maxmessagebytes`, 1 MiB by default, and the broker's `message.max.bytes`): a response above it MUST be recorded as a publication failure and settled, as described in "Response publication retry under retry policy".

#### Scenario: Large response

- **WHEN** the service returns 900 KiB of `data` and the producer limit is the default 1 MiB
- **THEN** it is received and published like any other response

#### Scenario: Response above the Kafka record limit

- **WHEN** the service returns 10 MiB of `data` and the producer limit is the default 1 MiB
- **THEN** the call succeeds, the publication fails once, the failure is recorded, and the offset is settled

#### Scenario: Response over the limit

- **WHEN** the service returns more than 16 MiB of `data`
- **THEN** a response record with empty body and `Grpc-Status: ResourceExhausted` is published

#### Scenario: Request over the limit

- **WHEN** a record's payload exceeds 16 MiB
- **THEN** nothing is sent to the service and a response record with `Grpc-Status: ResourceExhausted` is published

### Requirement: gRPC target validated at startup

For an active stanza (one with topics) with `service.protocol: grpc`, startup MUST fail with an error naming the stanza unless `service.name` contains no whitespace or control characters and `service.port` is a decimal number from 1 to 65535. The check MUST NOT resolve or contact the host: an unreachable or unresolvable service still does not block startup (see "Connection lifecycle"). Stanzas without topics MUST NOT be checked, as for the other per-stanza startup checks of active consumers.

#### Scenario: Invalid port

- **WHEN** a `grpc` stanza with topics has `service.port` set to `abc`, `0`, `70000` or empty
- **THEN** startup fails with an error naming the stanza and the port

#### Scenario: Valid target with unreachable host

- **WHEN** a `grpc` stanza has `service.name: model-svc` and `service.port: 50051` and the host does not resolve
- **THEN** startup succeeds and each record fails as a transport failure until the service is reachable

#### Scenario: HTTP stanza not checked

- **WHEN** an `http` stanza has `service.port: abc`
- **THEN** this check does not stop startup
