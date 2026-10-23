# quality-gates Specification

## Purpose
Defines the automated checks every change must pass: a race-enabled unit test suite built on mocked external boundaries and a statement-coverage gate usable identically locally and in CI.

## Requirements

### Requirement: Test suite with mocked boundaries
Unit tests MUST NOT require a live Kafka broker, gRPC audit sink or upstream HTTP service. Boundaries MUST be replaceable through the helpers in `testutil/` (`mocksarama`, `mockals`, `mockmodel`, `fakeclock`), an explicitly passed `*config.Config` and the logger's clock (`FileTransactionLogger.SetClock`).

#### Scenario: Offline test run
- **WHEN** `make test` runs on a machine without network access
- **THEN** all tests pass under `go test ./... -race -count=1`

### Requirement: Deterministic time and configuration
Code that reads wall-clock time for latency MUST do so through the logger's clock (a `func() time.Time`, `time.Now` by default), and configuration MUST be passed explicitly as `*config.Config`, so tests can drive them without global state. This includes event start stamping, event duration at close and audit-entry timestamps.

#### Scenario: Fake clock latency
- **WHEN** a test injects `fakeclock.Clock.Now` via `SetClock`, starts an event, advances the clock with `Advance` and closes the event
- **THEN** the recorded duration equals exactly the advanced amount, independent of wall-clock time

#### Scenario: Interleaved events under a fake clock
- **WHEN** two events are started and closed in interleaved order while the fake clock advances between steps
- **THEN** each event's duration equals its own span as computed from the fake clock

### Requirement: Single entrypoint for local checks
Besides the platform checks, `make ci` MUST run the project checks `proto-check` and `docs-check`. `make check` remains as `make build ci`.

#### Scenario: Clean tree
- **WHEN** `make ci` (or `make check`) runs on a healthy checkout
- **THEN** it exits 0

#### Scenario: Stale generated code fails the entrypoint
- **WHEN** `service.proto` changed without `make proto` and `make ci` runs
- **THEN** it fails in `proto-check`

### Requirement: Lifecycle behaviour is tested end to end
Startup and shutdown behaviour of `app.Connect` MUST be covered by tests that need no live broker: HTTP available while Kafka is unreachable, the service staying up and not ready while the broker is unreachable, probes answering during the shutdown drain, and graceful return on context cancellation.

#### Scenario: Unreachable broker
- **WHEN** the test points Connect at a closed local port
- **THEN** Connect keeps running, `/healthz` answers 200 and `/readyz` answers 503, and it returns nil after the context is cancelled

### Requirement: Regression tests for fixed defects
Every defect fixed in request validation, payload coercion, offset settlement or shutdown ordering MUST be accompanied by a test that fails without the fix.

#### Scenario: Trailing JSON value
- **WHEN** the request-id middleware test posts two concatenated JSON objects
- **THEN** it expects 422 and the downstream handler not called

### Requirement: Broker-free tests for the consume and produce paths
The consume loop, claim handling, consumer/producer client construction and the gRPC audit delivery path MUST be testable without a live Kafka broker or external gRPC service, through injectable factories and a loopback gRPC server.

#### Scenario: Claim handling
- **WHEN** a test feeds a fake claim with a valid, an invalid-payload and a cancelled-context message
- **THEN** valid messages reach the response channel with an ack, invalid ones are settled as failures, and cancellation stops the loop without leaking goroutines

#### Scenario: gRPC audit delivery
- **WHEN** a loopback ALS server is started and the logger is configured with its address
- **THEN** the server receives the audit entry that was also written to the audit file

### Requirement: Seams only at external boundaries
Test seams (injectable factories and paths) MUST exist only where code would otherwise need a live external system (Kafka client/consumer group/producer, gRPC receiver, filesystem config paths). A seam MUST NOT be introduced solely to reach a branch that production inputs cannot trigger; such a branch MUST be removed instead.

#### Scenario: Unreachable branch
- **WHEN** coverage analysis shows a branch that no production input can reach
- **THEN** the branch is deleted (or the function simplified) rather than exercised through a test-only hook

### Requirement: Generated code is reproducible
Generated protobuf/gRPC code that is committed MUST be producible from its `.proto` source by one documented command (`make proto`) using tool versions pinned in `go.mod`. A check (`make proto-check`) MUST regenerate the code and fail when the result differs from what is committed; CI runs it. A committed generated file whose source `.proto` is not in the repository is not allowed for new code.

#### Scenario: Stale generated code
- **WHEN** `service.proto` is edited without regenerating
- **THEN** `make proto-check` fails and names the stale files

#### Scenario: Clean checkout builds without protoc
- **WHEN** a developer or builder without `protoc` runs `go build ./...`
- **THEN** it succeeds, because the generated code is committed
