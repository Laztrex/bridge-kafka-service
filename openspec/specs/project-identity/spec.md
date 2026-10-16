# project-identity Specification

## Purpose
Fixes the canonical name of the service and enumerates the legacy wire-level identifiers that deliberately survive the rename from `provider_kafka` / `controller-kafka` to `bridge-kafka-service`, so internal renames never silently break external integrations (callers, dashboards, audit-controller).

## Requirements

### Requirement: Canonical project name
The Go module, the `cmd/` entrypoint directory, the binary, the Docker `APP_NAME` and the cobra command `Use` MUST all be `bridge-kafka-service`. The names `provider_kafka` and `provider-kafka` MUST NOT appear in source, build files or configs; the only permitted mention is the historical note in the README.

#### Scenario: Build from clean checkout
- **WHEN** `go build ./cmd/bridge-kafka-service` runs on a fresh clone
- **THEN** it produces the `bridge-kafka-service` binary and no import path references the old name

### Requirement: Legacy wire identifiers are frozen
The following external identifiers MUST keep their legacy `controller-kafka` form until an explicit migration change replaces them:
- HTTP headers `x-controller-kafka-produce-topic` and the custom prefix `x-controller-kafka-header-`;
- metric names `controller_kafka_<event>_*`;
- ALS `UpstreamCluster = "kafka-controller-cluster"` and fallback `ForwardedFor = "controller-kafka"`.

#### Scenario: Caller built against the old name
- **WHEN** a client publishes with `x-controller-kafka-produce-topic`
- **THEN** the request is accepted exactly as before the rename

### Requirement: Tracking header names
System tracking headers MUST be `x-request-id`, `x-request-time`, `x-trace-id`, `x-client-id`, plus optional `x-kafka-key` (partition key). Specs that say `Request-Id`, `Trace-Id`, `Client-Id` refer to these.

#### Scenario: Partition key
- **WHEN** a publish request carries `x-kafka-key`
- **THEN** its value is used as the Kafka record key

### Requirement: gRPC service identity is frozen
The gRPC contract with model services MUST keep the protobuf package `controller_kafka`, service `BaseModelInterface` and method `Predict`, i.e. the wire method `/controller_kafka.BaseModelInterface/Predict`, until an explicit migration change replaces it. Go package names and file locations of the generated code MAY change; the protobuf package and names MUST NOT.

#### Scenario: Regenerating the stubs
- **WHEN** the generated code is regenerated or moved
- **THEN** the full method name on the wire is still `/controller_kafka.BaseModelInterface/Predict`
