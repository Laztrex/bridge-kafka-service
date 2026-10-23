# Design

## Context

An audit of the service against platform release v2.0.0 (layers `core`, `env-public`, `lang-go`), requirement by requirement at the `baseline` level:

| Requirement | State | In this change |
|---|---|---|
| SDD-6, LAYOUT-2 | no `.platform-standards.yaml` | add it last |
| SDD-8 | no store reference, no binding line | done at proposal time |
| SDD-9 | `quality-gates` restates TEST-4, GOQ-1, BUILD-3 | spec deltas |
| CONF-2 | unknown keys are warnings | strict |
| CONF-6 | `configs/config_app.yaml` is in git | stand config renamed, template added |
| HTTP-1 | key `http.addr`, not `health.addr` | new key, alias |
| HTTP-4 | key `pprof.addr`, not `debug.addr` | new key, alias |
| MET-2 | prefix `controller_kafka_`, `bridge_service_` | rename (D3) |
| LIFE-1 | the shared HTTP server stops before the drain ends, so probes stop too | D4 |
| LIFE-3 | Kafka client creation gives up after five minutes and the process exits | D5, conditional |
| BUILD-1..3, IMAGE-1, 2, 4, 5, ENV-1..3, CI-1..3, TEST-4, GOQ-1, GO-1, LOG-1, 2, 4, MET-1, MET-6, HTTP-2, SDD-13 | met | none |

LOG-4: the configuration holds no secret values (only TLS file paths), so nothing is registered with the redactor; the redactor itself is in place.

## Decisions

### D1. New keys with deprecated aliases

`health.addr`, `debug.addr` and `log.level` are the keys. The old keys `http.addr`, `pprof.addr` and `logLevel` are still read, so deployed files keep working under strict parsing: an alias is a known key, not an unknown one. Rules, the same for each pair:

- only the new key: used;
- only the alias: used, and startup logs `<alias> is deprecated, use <key>` once;
- both: validation fails, naming both keys.

The environment override follows the key (`HEALTH_ADDR`, `DEBUG_ADDR`); the alias overrides (`HTTP_ADDR`, `PPROF_ADDR`) are read under the same rules. Aliases are a project addition (SDD-12) and need no exemption: the platform behavior holds for every file that uses the new keys.

*Alternative:* drop the old keys. Every deployed file would stop loading at once, on top of the strict-key change.

### D2. Strict keys

The loader keeps its compatibility parsing (case-insensitive keys, dotted or nested form, environment overrides, weak scalar conversion), and an unknown key becomes an error collected with the other validation errors (CONF-4): `unknown config key "<key>"`. `--check-config` exits 1 on it. The compatibility fixtures in `internal/config/testdata/compat` that contain unknown keys get the error as their expected result.

### D3. Metric rename and its migration (MET-2, MET-6)

| Old | New |
|---|---|
| `controller_kafka_<event>_{success_total,failure_total,requests_total,duration_ms}` | `bridge_kafka_service_<event>_…` |
| `bridge_service_pod_cpu_usage` | `bridge_kafka_service_pod_cpu_usage` |
| `bridge_service_pod_memory_usage` | `bridge_kafka_service_pod_memory_usage` |

Migration plan (MET-6):

1. The README gets the table above under "Metrics", and the release notes of the version that ships the change name it.
2. Before that version is deployed, the owners of dashboards and alerts that read the metric file switch their queries to the new names (or to both). The deploy waits for their confirmation; task 4.3 records it.
3. The old names are not written in parallel: a metric file with both names would double every counter that consumers sum by event.

The other legacy identifiers (headers, ALS values, the gRPC method) are not metric names and stay frozen by `project-identity`.

### D4. Probes outlive the drain (LIFE-1)

The publish API and the probes share one server. Today shutdown calls `srv.Shutdown` first, which closes the listener: probes stop answering while pipelines are still draining. New order:

1. A `draining` flag is set: the publish handler answers `503` (`service is shutting down`), and `/readyz` answers `503` with the reason `shutting down`. `/healthz`, `/health` and `/status` answer as before.
2. Intake stops: the root context is cancelled, consumers stop claiming, in-flight publish requests finish (they are tracked by a wait group).
3. Pipelines, publishers and the producer close as today, bounded by `timeouts.shutdown`.
4. The HTTP server shuts down last, with the remaining part of the same deadline.

### D5. LIFE-3, applied only without an exemption

The developer asked for an exemption from LIFE-3; the request is drafted in the platform registry. Until it is approved the platform behavior applies (SDD-1), so task group 6 starts by checking the registry:

- exemption merged: group 6 is skipped, and the `runtime-control` and `quality-gates` deltas of this change that concern fail-fast are removed before archiving;
- not merged: `retryWindow` no longer bounds broker connection; producer and consumer clients retry with exponential backoff and jitter until shutdown (LIFE-5), the service stays up, and `/readyz` reports what is missing. Failures that are not unavailability stay fatal: an upstream that cannot be constructed and a consumer group that cannot be created from a valid client are configuration errors.

### D6. Readiness and application runtime state

Review ownership of readiness checks, pipeline state, pause, draining and in-flight publish tracking. If one object owns these together, name it for application runtime state rather than readiness alone; keep readiness evaluation explicit and avoid splitting shared lifecycle state solely to rename it. Update affected references and tests.

Review outcome: the object owns all five concerns, with one lifetime controlled by `Connect`. Rename `readiness` / `newReadiness` and their files to `runtimeState` / `newRuntimeState` in `runtime_state.go`, retaining readiness methods as a view of that state. No additional lifecycle owner is needed.

The application determines readiness; Kubernetes consumes that result through a readiness probe, and Istio probe forwarding does not replace it. `/readyz` reports whether required Kafka clients have initialized and whether shutdown has begun. It is not a continuous broker reachability check. Liveness remains independent; pause and upstream retries remain visible in `/status` without changing readiness. Kubernetes readiness does not control Kafka consumption.

## Risks / Trade-offs

- Strict keys stop files that loaded before. → `--check-config` in the rollout; the README "Platform" section says so.
- A process that never connects stays unready forever instead of crash-looping. → `/readyz` and `/status` name the missing clients; alerts move from restarts to readiness.
- Dashboards break if the rename ships before consumers switch. → D3, step 2.

## Migration Plan

1. Run `--check-config` against every deployed configuration; remove unknown keys; optionally rename `http.addr`, `pprof.addr`, `logLevel`.
2. Switch metric consumers (D3).
3. Deploy. Set `log.level: debug` explicitly where debug output is needed.
