# als-conformance-harness Specification

## Purpose
Provides a small self-contained gRPC sink (`test_go_grpc_app`) that receives Envoy Access Log Service traffic from bridge-kafka-service and prints every received entry to stdout, so developers can see what the bridge emits to an audit receiver. It is a manual inspection tool, not a validator: the audit-controller v1.3.0 field invariants are specified by `observability-audit` and verified by the producer-side tests.

## Requirements

### Requirement: ALS gRPC sink endpoint
The harness MUST expose an Envoy-compatible `AccessLogService.StreamAccessLogs` gRPC endpoint on a TCP port taken from `AUDIT_GRPC_PORT` and defaulting to `50053`, over insecure transport. The full method path MUST be `/envoy.service.accesslog.v3.AccessLogService/StreamAccessLogs`. For every stream it MUST read messages until EOF, reply with an empty `StreamAccessLogsResponse` and close cleanly. gRPC reflection is NOT provided.

#### Scenario: Default port
- **WHEN** the harness starts without `AUDIT_GRPC_PORT`
- **THEN** it listens on port 50053 and logs the port

#### Scenario: Port override
- **WHEN** `AUDIT_GRPC_PORT=50777` is set
- **THEN** it listens on port 50777

#### Scenario: Client streams entries
- **WHEN** a client invokes `StreamAccessLogs`, sends messages and closes its side
- **THEN** every message is received, the server replies with an empty response and the stream ends without error

### Requirement: Received entries are printed to stdout
For each received message the harness MUST print to stdout the log name, the node id and cluster, and for every HTTP entry: the start time, the filter-metadata keys with their contents, the request method, scheme, authority, path, user agent and body size, the response code and its details, and the protocol version; for TCP entries it prints their count. The output is plain text and has no flags, no TTY detection and no colour.

#### Scenario: Operator inspects an ingress entry
- **WHEN** the bridge emits an ingress ALS entry to the harness
- **THEN** the output shows `LogName: envoy-ingress-als`, the filter-metadata key `envoy.ingress.audit-controller.body.lua` with its request/response fields, and the request path

#### Scenario: Output is the same on a terminal and in a pipe
- **WHEN** stdout is redirected to a file
- **THEN** the same plain text is written

### Requirement: Self-contained harness module
`test_go_grpc_app` MUST be a standalone Go module that resolves with `go mod tidy` and builds with `go build`, independent of the main module's versions. Compiled binaries MUST NOT be committed, and `.gitignore` MUST exclude the harness binary.

#### Scenario: Fresh checkout produces no prebuilt binaries
- **WHEN** a developer clones the repository into a clean directory
- **THEN** `test_go_grpc_app/` contains only `main.go`, `go.mod`, `go.sum` and `Dockerfile`

#### Scenario: Module builds on its own
- **WHEN** `go mod tidy && go build` runs inside `test_go_grpc_app`
- **THEN** it succeeds without edits to the main module
