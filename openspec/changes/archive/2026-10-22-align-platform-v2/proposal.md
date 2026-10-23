# Proposal

## Why

The project is not aligned with the platform (SDD-2): it has no `.platform-standards.yaml` (SDD-6, LAYOUT-2), and an audit against platform release v2.0.0 found violated `baseline` requirements. The service follows `docs/conventions.md`, a copy of the shared Go conventions that predates the platform, and keeps its deviations as a list in the README; the platform does not count such a list (SDD-6). Feature work waits until this change is applied.

## What Changes

- Connect the project: `references: platform` and the binding lines in `openspec/config.yaml` (SDD-8, done when the change was proposed); `.platform-standards.yaml` with `aligned_with: v2.0.0` once every baseline requirement below is met or exempted (SDD-6).
- CONF-2: an unknown configuration key fails startup with an error naming the key; `--check-config` reports it the same way. The "unknown key is ignored" warning is gone.
- HTTP-1: the service HTTP server listens on `health.addr` (default `:8081`). `http.addr` stays a deprecated alias: it is used when `health.addr` is unset, startup logs a deprecation warning, and setting both fails validation.
- HTTP-4: the profiling listener is configured by `debug.addr`, with `pprof.addr` as a deprecated alias under the same rules.
- LOG-3: the log level is `log.level`, default `info`; `logLevel` is a deprecated alias. The default changes from `debug` to `info`.
- MET-2: metric names carry the service prefix `bridge_kafka_service_` instead of `controller_kafka_` and `bridge_service_`. The rename follows the migration plan in `design.md` (MET-6).
- LIFE-1: on shutdown the publish endpoint answers `503` and `/readyz` reports `shutting down`, while `/healthz`, `/readyz` and `/status` keep answering until the drain ends; the HTTP server closes last.
- LIFE-3: when a broker is unreachable, Kafka clients keep retrying with backoff and the service stays up and not ready instead of exiting after five minutes. Conditional: see Platform constraints.
- Review the responsibilities of `readiness` and rename it if it owns broader application runtime state; retain application readiness for Kubernetes/Istio probes and preserve endpoint behaviour.
- CONF-6: `configs/config_app.yaml` leaves git. The stand config becomes `configs/config_compose.yaml` (STAND-2), the template `configs/config_app.yaml.example`, and `configs/config_app.yaml` is listed in `.gitignore`.
- SDD-9: project specs stop restating platform requirements (coverage gate, toolchain version, the `make ci` target).
- `docs/conventions.md` is removed, and the README section "Conventions: exceptions and deviations" becomes "Platform": the followed release, the deprecated aliases and a pointer to the exemption registry.

Platform requirements touched (SDD-3): SDD-2, SDD-6, SDD-8, SDD-9, SDD-11, LAYOUT-2, CONF-2, CONF-6, HTTP-1, HTTP-4, LOG-3, MET-2, MET-6, LIFE-1, LIFE-3, STAND-2.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `app-configuration`: unknown keys fail startup; `health.addr`, `debug.addr` and `log.level` replace `http.addr`, `pprof.addr` and `logLevel`, which stay as deprecated aliases.
- `health-probe`: the listener is bound to `health.addr`; probes keep answering during shutdown.
- `runtime-control`: shutdown keeps the probes up; broker unavailability no longer ends the process (conditional, see Platform constraints).
- `observability-audit`, `project-identity`: metric names use the service prefix; the other legacy wire identifiers stay frozen.
- `quality-gates`: requirements that restated platform requirements are removed or narrowed to the project's own part.

## Impact

- Code: `internal/config` (strict keys, new keys and aliases), `internal/app` (shutdown order, retry window), `internal/consts` (metric names), `internal/handlers`, `internal/logging` (`SetLevel`).
- Operations: config files with unknown keys stop loading; run `bridge-kafka-service --check-config` on every deployed file before rolling out. Dashboards and alerts on `controller_kafka_*` must switch to the new names (design, Migration). The default log level drops to `info`.
- Tests: config compatibility fixtures with unknown keys change their expected result; new tests for aliases, shutdown probes and the metric prefix.

## Non-goals

- `docs/runbook.md` (LAYOUT-3, standard): added in its own change.
- Moving internal values (paths, ports, defaults) out of `internal/consts` (GO-9, standard).
- Converting testify tests (GOQ-7, advanced; converted when touched).
- Renaming the other legacy wire identifiers (headers, ALS values, the gRPC method): they are not metric names, and `project-identity` keeps them frozen.

## Platform constraints

- Requested (project spec `runtime-control`, "Fail fast on permanent consumer failure"; confirmed by the developer): the process exits with code 1 when a Kafka client cannot be created within five minutes, so the supervisor restarts it. Platform: LIFE-3 requires the service to keep running, retry in the background and report not ready. Applied: the platform behavior, unless the exemption below is approved before this change is applied. The developer asked for an exemption; a pull request adding `LIFE-3` (`kind: compatibility`) to `exemptions/bridge-kafka-service.yaml` in the platform registry is drafted and needs one guild code-owner approval (SDD-6). Task group 6 implements the platform behavior only when the exemption is not merged by then.
