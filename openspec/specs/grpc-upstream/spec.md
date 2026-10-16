# grpc-upstream Specification

## Purpose
Defines how a consumed Kafka record is delivered to a gRPC upstream service and how the outcome is interpreted, so a gRPC pipeline behaves exactly like an HTTP one apart from the call itself.

## Requirements

### Requirement: Predict call mapping
For `service.protocol: grpc` the system MUST call the unary method `/controller_kafka.BaseModelInterface/Predict` on `{service.name}:{service.port}`. The request carries all Kafka record headers in `details` and the normalised JSON payload in `data`. The `x-request-id` value MUST additionally be sent as gRPC metadata under the same key. The response `data` becomes the response body; response header and trailer metadata become response headers.

#### Scenario: Record forwarded
- **WHEN** a record with headers and payload is consumed on a gRPC pipeline
- **THEN** the service receives the payload as `data`, every record header in `details`, and `x-request-id` as metadata

#### Scenario: Metadata becomes response headers
- **WHEN** the service returns header or trailer metadata
- **THEN** those entries are carried as response headers into the response record

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
Send and receive message size limits of the gRPC client MUST be 16 MiB, so payloads larger than the gRPC default of 4 MiB but within normal Kafka limits are handled. A message over the limit makes the call end with status `ResourceExhausted`, which, like any other status outside the transport-failure set, is published as an application answer (the requester learns that its request or the service's response was too large).

#### Scenario: Large response
- **WHEN** the service returns a 10 MiB `data`
- **THEN** it is received and published like any other response

#### Scenario: Response over the limit
- **WHEN** the service returns more than 16 MiB of `data`
- **THEN** a response record with empty body and `Grpc-Status: ResourceExhausted` is published

#### Scenario: Request over the limit
- **WHEN** a record's payload exceeds 16 MiB
- **THEN** nothing is sent to the service and a response record with `Grpc-Status: ResourceExhausted` is published
