# Go Service Conventions

**Version 1.6 · 2026-10-13**

Shared rules for the Go services that adopt them, including services that do not exist yet. This file lives at `docs/conventions.md` in every such repository and is **byte-for-byte identical**: it changes in all of them at once, with a version bump. It names no services: anything specific to one service belongs in that service's README.

How to read it: **must** is mandatory, and any deviation is recorded in the service's README (see "Exceptions and deviations"); **should** is the default, and doing otherwise needs a reason stated in code or the README. Rules marked as a **platform requirement** come from the platform and are not negotiable per service.

## 1. Language

Everything in the repository is in English: code comments, log messages, error texts, the Makefile, Dockerfile and CI workflows, the README, everything under `docs/` (runbook, diagrams, this document), and OpenSpec proposals, designs, specs and tasks.

Log and error messages are part of the code: people search logs for them and build alerts on them. When they change, the runbook and README examples are updated in the same change.

## 2. Repository layout

- The module name matches the repository name, in kebab-case: `module <repository-name>`.
- The entrypoint is `cmd/<repository-name>/main.go`. One service binary per repository; auxiliary programs go outside `cmd/` with a descriptive name: runnable examples under `examples/`, test sinks under `test/`.
- Code lives in `internal/`, in packages named by purpose: `config`, `logging`, `metrics`, `status`, `app`, and domain packages (`s3`, `router`, …). Catch-all packages `utils`, `common` and `helpers` are **forbidden**: a function lives next to its owner.
- Dev-only code (local tools, debug UIs) is excluded from release builds with the build tag `release`: such files carry `//go:build !release`, and a dev-only `main` gets a `//go:build release` stub that exits with an error. Builds without the tag are dev builds.
- No package, interface or exported function exists only for tests. A test seam is a field or a parameter of the code under test (e.g. `Now func() time.Time`, nil meaning `time.Now`); fakes and helpers live in `_test.go` files, `testutil/` or `test/harness/`. Dead code (unused packages, empty placeholder files, generated code without its source) is deleted, not kept "for later".
- Constants live in the package that owns them. A shared `consts` package is allowed only for external identifiers (headers, metric names) read by several packages.
- Required files at the root: `README.md`, `Makefile`, `Dockerfile`, `.dockerignore`, `.gitignore`, `.golangci.yml`, `compose.yaml` (local stand), `go.mod`, and the CI workflow `.github/workflows/ci.yml`.
- `docs/` holds `conventions.md` (this file), `runbook.md` (incidents and procedures) and `flow.md` (Mermaid diagram of how the service works). Other documents also go in `docs/`, not at the root.
- Tests: unit tests sit next to the code; integration tests go in `test/integration/` behind the `integration` build tag; shared test helpers go in `test/harness/` (preferred) or `testutil/`.

## 3. Entrypoint and version

- Flags use the standard `flag` package. No cobra, banners, colored output or tables on stdout: stdout is the log stream.
- `main` only parses flags, builds the application, calls `Run(ctx) error` and exits with code `0` on a clean stop or `1` on error. `os.Exit` and `log.Fatal` outside `main` are **forbidden**.
- The version is embedded at build time:

  ```go
  var (
  	version = "dev"
  	commit  = "none"
  	date    = "unknown"
  )
  ```

  Set with `-ldflags "-X main.version=… -X main.commit=… -X main.date=…"`. The first log entry after startup is one structured record with `version`, `commit`, `date` and `go_version`.
- The `-version` flag prints the version and exits.

## 4. Configuration

- One YAML file per service. Path resolution: `-config` flag → `CONFIG_PATH` environment variable → `configs/config_app.yaml`.
- Parsing is strict: an unknown key fails startup (`go.yaml.in/yaml/v3` with `Decoder.KnownFields(true)`). viper is **not used**.
- Values may reference environment variables as `${VAR}`; they are expanded before parsing.
- The configuration is a plain struct passed explicitly. There is no global configuration state.
- Defaults are set in one place (an `applyDefaults` function). When zero is a valid value, a pointer distinguishes "not set" from an explicit zero.
- `Validate() error` returns **all** problems at once (`errors.Join`), each prefixed with the key path: `limits.queue_size: must be positive`.
- Durations are Go duration strings (`30s`, `5m`).
- Secrets never reach the repository: git holds only `configs/config_app.yaml.example`. Secret values are registered with the log redactor (section 6) right after loading.
- Every key is documented in the README; `make docs-check` enforces this.
- Library tunables that a dependency reads as they are (e.g. a client library's own config struct decoded straight from YAML) are separate files under `configs/`, not part of the application config.

## 5. Lifecycle

- Signals are handled with `signal.NotifyContext(ctx, SIGINT, SIGTERM)` in `main`; the context is passed down explicitly.
- Shutdown has two phases:
  1. stop accepting new work (close intake of events, messages and HTTP requests, except probes);
  2. wait for in-flight operations for at most `timeouts.shutdown`; when it expires, cancel their context and persist unfinished work if the service supports that.
- `timeouts.shutdown` is configurable; its default is at least 5 seconds shorter than the pod's `terminationGracePeriodSeconds`.
- Every goroutine has an owner that stops it and waits for it. Background loops take a `ctx` and exit on `ctx.Done()`.
- If an external dependency is unavailable at startup, the service does **not** exit: it connects in the background with retries, and `/readyz` reflects this.

## 6. Logs

The format is a **platform requirement**.

- The library is `github.com/rs/zerolog`; the global `log.Logger` is configured once, in the `internal/logging` package.
- Record fields:

  | Field | Meaning |
  |---|---|
  | `rqTime` | timestamp, RFC 3339 |
  | `loglevel` | `debug` / `info` / `warn` / `error` |
  | `message` | text |
  | `funcName` | call site |
  | `host` | host name |
  | `service` | service name (= repository name) |

- Outputs:
  - stdout: zerolog `ConsoleWriter`, no color, ordered `rqTime | loglevel | message`, without `host`, `service` and `funcName`;
  - file `${LOG_PATH}/${LOG_FILE_NAME}` (default `./app.log`): JSON with **info and error records only** (platform requirement), rotated by `lumberjack` at 5 MB, kept 7 days, 2 backups, compressed.
- The level comes from the `log.level` config key; the default is `info`.
- Context field names are constants in `internal/logging` (`KeyRequestID`, `KeyOp`, `KeyError`, …), in snake_case. Ad-hoc field names are not used in log calls.
- Secrets **never** reach the logs: a redactor wraps both outputs as an `io.Writer` and replaces every registered value, including its JSON-escaped form, with `***`.
- An error is logged once, where it is handled. A function that returns an error does not also log it.

## 7. Metrics and audit

The format is a **platform requirement**.

- Metrics are written as JSON lines to `${METRIC_PATH}/${METRIC_FILE_NAME}` (default `./app-metrics.log`):

  ```json
  {"rqUId":"<operation id>","metric_name":"<name>","metric_value":"<value>"}
  ```

- Each event produces four metrics: `<prefix>_<event>_success_total` (`1`/`0`), `<prefix>_<event>_failure_total` (`1`/`0`), `<prefix>_<event>_requests_total` (`success`/`failure`) and `<prefix>_<event>_duration_ms`. `<prefix>` is the service name in snake_case; `duration_ms` is the duration of that event alone.
- Each service defines its events (one per kind of external operation) and lists them in its README.
- Audit (Envoy ALS) is written only by services the platform requires it from; the README states whether a service writes audit and for which operations.
- Audit entries are sent as Envoy ALS v3 over gRPC to `audit.grpc_endpoint` and are always mirrored to `<metrics file>.audit` (JSON lines), even when the receiver is unavailable. An unavailable receiver does not stop the service.
- Audit entries never carry file content, error texts or credentials.
- Prometheus is not used: metrics go to the file only. Operation-state gauges (queues, replica lag) are exposed in `/status`.
- Metrics and audit are written asynchronously through a bounded buffer; on shutdown the queue is flushed completely.
- `rqUId` identifies the operation: the incoming `x-request-id` when present, otherwise a new UUID.
- External names (metrics, headers, audit values) are a contract with the platform: renaming them requires a migration plan.

## 8. Service HTTP server

- One service HTTP server at `health.addr`, default `:8081`.
- Endpoints:

  | Path | Purpose | Response |
  |---|---|---|
  | `/healthz` | liveness: the process is alive and not stuck | `200` / `503` |
  | `/readyz` | readiness: able to do work (dependencies reachable) | `200` / `503` |
  | `/status` | detailed state as JSON, for humans | `200` |

  An unavailable external dependency affects only `/readyz`, never `/healthz`; otherwise the orchestrator restarts a healthy pod because of someone else's outage.
- `/health` may remain as an alias of `/healthz` while deployment manifests migrate.
- `net/http/pprof` on the service address is **forbidden**. If needed, it runs on a separate address (`debug.addr`; an existing project key is kept for compatibility and listed as an exception), disabled by default and meant for `127.0.0.1`; a non-loopback address is logged as a warning. Its handlers are registered on a dedicated mux, never served from `http.DefaultServeMux`.
- Server timeouts (`ReadHeaderTimeout` and others) are set explicitly.

## 9. Errors and context

- Errors are wrapped with the operation's context: `fmt.Errorf("upload %s: %w", key, err)`. They are compared with `errors.Is` / `errors.As`.
- Error strings start lowercase and have no trailing period.
- Every I/O operation takes `ctx` as its first argument and honors cancellation.
- Retries use exponential backoff with jitter, bounded by attempts or time; their parameters come from configuration or named constants.

## 10. Tests and coverage

- Tests use the standard `testing` package. No new assertion dependencies are added; existing testify tests are converted when they are touched anyway.
- Table-driven tests use `t.Run` with a case name.
- External boundaries (S3, Kafka, gRPC, HTTP) are hidden behind small interfaces with hand-written fakes. Time is injected as a `func() time.Time` field where a test must control it; there is no clock package or interface in production code.
- Integration tests use the `integration` build tag and start dependencies with testcontainers; run them with `make test-integration`.
- Unit tests run with `-race`.
- Coverage thresholds: **80%** total and **60%** per package, excluding generated code, `cmd/` and test helpers. Enforced by `make coverage-check`.

## 11. Makefile

One set of targets with identical names. A project may add its own targets but must not rename the shared ones.

| Target | What it does |
|---|---|
| `ci` | every required check, as in CI: `fmt-check vet test lint vuln coverage-check docs-check` plus project-specific ones (e.g. `proto-check`) |
| `build` | binary at `./bin/<repository-name>` with the version embedded via ldflags and `BUILD_TAGS` as `-tags` |
| `fmt-check` | fails if `gofmt` would change any file |
| `vet` | `go vet`, plain and with the `integration` tag |
| `test` | unit tests with `-race` |
| `test-integration` | integration tests (needs docker); only in projects that have them |
| `coverage`, `coverage-check` | coverage profile and threshold check |
| `lint` | `golangci-lint` with `.golangci.yml` |
| `vuln` | `govulncheck` |
| `docs-check` | every config key is documented in the README |
| `image` | `docker build` with `BUILDER_IMAGE`, `RUNTIME_IMAGE`, `IMAGE`, `VERSION`, `BUILD_TAGS` |
| `tools` | installs the check tools |

- `BUILD_TAGS` (comma-separated Go build tags, empty by default) is defined in `make/common.mk`: `build` passes it as `-tags`, `image` as the `BUILD_TAGS` build argument. A release build sets `BUILD_TAGS=release`.
- Tool versions are pinned in the Makefile; binaries are installed into `./bin` with the version in the file name:

  | Tool | Version |
  |---|---|
  | golangci-lint | v2.14.0 |
  | govulncheck | v1.9.0 |

- Shared targets live in `make/common.mk`, byte-for-byte identical across repositories. The project `Makefile` sets `APP`, `CMD`, `FMT_DIRS`, `COVERAGE_SCRIPT`, `TEST_DEPS` and `CI_EXTRA`, includes it and adds project targets. A change to the shared part is made in `make/common.mk` of every repository at once.
- `.golangci.yml` and the coverage gate script (`ci/coverage.sh` or `scripts/coverage.sh`) are identical across repositories.
- `VERSION` is `git describe --tags --always --dirty`: release tags are `vMAJOR.MINOR.PATCH` (e.g. `v0.6.0`).

## 12. Image

- Two stages: build in `BUILDER_IMAGE`, run in `RUNTIME_IMAGE`. Both are `ARG`s with public defaults:
  - `BUILDER_IMAGE=golang:<Go version>`;
  - `RUNTIME_IMAGE=alpine:3.21`.
- The Go version in `BUILDER_IMAGE` matches the `go` directive in `go.mod` in its minor version and is not lower in its patch version. All repositories use the same Go version.
- Build: `CGO_ENABLED=0`, `-trimpath`, `-ldflags "-s -w -X main.version=…"`, `-tags` from the `BUILD_TAGS` argument; `--mount=type=cache` for modules and the build cache; `go.mod`/`go.sum` are copied before the sources.
- The Dockerfile is self-sufficient: CI may run plain `docker build`, so everything a build needs is an `ARG` (`BUILDER_IMAGE`, `RUNTIME_IMAGE`, `VERSION`, `COMMIT`, `BUILD_DATE`, `BUILD_TAGS`), and make targets only wrap it for local runs. `BUILD_TAGS` is empty by default (a dev image); the release image is built with `--build-arg BUILD_TAGS=release`, and the Dockerfile itself leaves dev-only binaries out of it.
- Runtime: user `1001:1001`, not root; `EXPOSE 8081`; configuration is mounted from outside, never baked into the image.
- Build secrets go only through `--secret`, never `ARG` or `ENV`.
- `.dockerignore` excludes everything the build does not need.

## 13. CI

- CI is GitHub Actions (`.github/workflows/ci.yml`); a `Jenkinsfile` is not kept in git. The workflow holds no check logic: its steps call Makefile targets — checks (`make ci`), integration (`make test-integration`, where the project has integration tests), image (`make image BUILD_TAGS=release`). A pipeline that builds the image with plain `docker build` passes the same build arguments, including `--build-arg BUILD_TAGS=release`.
- Checks run on the Go version from `go.mod` (`actions/setup-go` with `go-version-file`), the same Go version the binary is built with.
- When the Dockerfile default images are not reachable from GitHub-hosted runners, the workflow sets public `BUILDER_IMAGE` and `RUNTIME_IMAGE` in its `env` and passes them to `make image`.

## 14. Local stand

- `compose.yaml` at the root starts the service and its dependencies; the service is built from the `Dockerfile` with the same `BUILDER_IMAGE`/`RUNTIME_IMAGE` as CI.
- The stand's configuration is `configs/config_compose.yaml`, mounted into the container.
- Dependencies wait for readiness via `healthcheck` and `depends_on: condition: service_healthy`.
- Stand data lives in `./.stand/` (listed in `.gitignore`).

## 15. OpenSpec

- `openspec/config.yaml` contains `context` (purpose, stack, layout, conventions) and `rules` for proposals, specs and tasks.
- Shared rules:
  - proposal: list the affected specs; include a Non-goals section;
  - specs: SHALL/MUST wording and at least one `#### Scenario` per requirement;
  - tasks: the last task runs `make ci`.
- Specs capture only non-trivial behavior; obvious fixes do not get spec entries.

## Exceptions and deviations

Each service keeps, in its README under "Conventions: exceptions and deviations", two short lists:

- **compatibility exceptions**: behavior kept on purpose so that existing deployments need no changes (for example a legacy config key or environment variable honoured after the standard one), each with the rule it deviates from;
- **open deviations**: what still differs from these conventions and is to be fixed; an item is removed once it is fixed.

A new service starts with both lists empty.
