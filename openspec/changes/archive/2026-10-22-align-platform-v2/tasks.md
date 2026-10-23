# Tasks

## 1. Connect the project to the platform

- [x] 1.1 Add `references: platform` and the binding lines to `openspec/config.yaml`; verify `openspec doctor` shows `platform: ok`

## 2. Configuration (CONF-2, CONF-6, HTTP-1, HTTP-4, LOG-3)

- [x] 2.1 Make unknown keys an error joined with the other validation errors; `--check-config` exits 1 on them; drop `Config.Warnings` and its logging in `main`. Verify with a test that loads a file with two unknown keys and gets both named
- [x] 2.2 Add `health.addr`, `debug.addr` and `log.level` with `http.addr`, `pprof.addr` and `logLevel` as aliases (design D1): alias alone works and warns once, both set fails validation; environment overrides `HEALTH_ADDR`, `DEBUG_ADDR`. Verify with table tests per pair
- [x] 2.3 Default log level `info`; `logging.SetLevel` accepts `debug`, `info`, `warn`, `error` and rejects other values at validation
- [x] 2.4 Update the compatibility fixtures in `internal/config/testdata/compat`: files with unknown keys expect the error; the stand and example files load without warnings
- [x] 2.5 Rename `configs/config_app.yaml` to `configs/config_compose.yaml`, point the stand at it (`CONFIG_PATH` in `docker.env`), add `configs/config_app.yaml.example` with every key and no real values, list `configs/config_app.yaml` in `.gitignore`
- [x] 2.6 Rename the keys in `configs/`, `examples/`, the README "Keys" section and the `app-configuration` Technical Notes; `make docs-check` passes

## 3. Shutdown keeps the probes (LIFE-1)

- [x] 3.1 Add the draining state (design D4): publish answers `503`, `/readyz` answers `503` with `shutting down`, `/healthz`, `/health` and `/status` keep answering
- [x] 3.2 Reorder `Connect` shutdown: drain flag, cancel intake, wait for in-flight publish requests, pipelines, publishers and producer, then `srv.Shutdown` with the rest of `timeouts.shutdown`
- [x] 3.3 Test: during a slow in-flight upstream call after SIGTERM, `/healthz` answers 200, `/readyz` 503, a new publish 503, and the process exits 0 within `timeouts.shutdown`

## 4. Metric names (MET-2, MET-6)

- [x] 4.1 Rename the metric names in `internal/consts` to the prefix `bridge_kafka_service_` (design D3); derive them from one prefix constant instead of four literal maps
- [x] 4.2 Update the README "Metrics" section with the old-to-new table, `docs/flow.md`, and the tests that assert metric names
- [x] 4.3 Record in the pull request that the metric consumers have switched (design D3, step 2); do not deploy before that

## 5. Documentation and specs (SDD-9, SDD-11)

- [x] 5.1 Remove `docs/conventions.md`; replace the README section "Conventions: exceptions and deviations" with "Platform": `aligned_with`, the deprecated key aliases, the rollout note for strict keys, and a pointer to the platform exemption registry; fix links to `docs/conventions.md` in code comments and docs
- [x] 5.2 Spec deltas: `app-configuration`, `health-probe`, `runtime-control`, `observability-audit`, `project-identity`, `quality-gates`
- [x] 5.3 README and `docs/flow.md` quote the new log and error messages (`unknown config key`, deprecation warnings, `shutting down`)

## 6. LIFE-3 (conditional, design D5)

- [x] 6.1 `git -C <store root> pull`, then check `exemptions/bridge-kafka-service.yaml` for an entry `requirement: LIFE-3`. If it exists, skip 6.2–6.4, delete the `runtime-control` requirement "Fail fast on permanent consumer failure" and the `quality-gates` requirement "Lifecycle behaviour is tested end to end" from this change's deltas, and go to 7
- [x] 6.2 Remove the retry window from producer and consumer client creation: retry with exponential backoff and jitter until the context is cancelled; log every attempt as today
- [x] 6.3 Keep upstream construction and consumer group creation failures fatal
- [x] 6.4 Test: with the broker unreachable longer than the former window, the process stays up, `/healthz` answers 200, `/readyz` 503 naming the clients, and SIGTERM exits 0; becoming ready once the broker is reachable

## 7. Alignment record

- [x] 7.1 Check `git -C <store root> describe --tags` prints `v2.0.0` (SDD-4), then add `.platform-standards.yaml` with `aligned_with: v2.0.0`

## 8. Readiness responsibility review (design D6)

- [x] 8.1 Analyze ownership of readiness, pipeline state, pause, draining and in-flight publish tracking; rename the object and affected references if its responsibilities exceed readiness, documenting the decision
- [x] 8.2 Preserve and verify readiness/liveness/status and shutdown behaviour; document how application readiness relates to Kubernetes/Istio probes and its initialization-only Kafka checks
- [x] 8.3 Run `graphify update .` after code changes and validate the updated change

## 9. Check

- [x] 9.1 Run `make ci`
