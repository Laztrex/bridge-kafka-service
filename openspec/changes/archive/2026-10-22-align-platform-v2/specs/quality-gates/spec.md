## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: Coverage gate
**Reason**: Restates platform requirements TEST-4 and GOQ-6 (SDD-9).
**Migration**: The thresholds and exclusions come from the platform; `scripts/coverage.sh` and `make coverage-check` stay as they are.

### Requirement: Pinned toolchain
**Reason**: Restates platform requirement GOQ-1 (SDD-9).
**Migration**: The Go version follows GOQ-1; no code change.
