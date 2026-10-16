# Proposal

## Why

Fast mode (`service.max_conn` unset or `<= 0`) starts one goroutine and one upstream call per consumed record with no limit, and it is the default: every stanza in `configs/config_app.yaml` leaves `max_conn` commented out. During a catch-up after a restart with consumer lag, records are read at fetch speed. The bridge then opens thousands of concurrent upstream connections (the HTTP client uses `http.DefaultTransport`: no per-host connection limit and only 2 idle connections kept per host, so connections churn) and grows memory without bound. Once the upstream degrades, calls hit the timeout, and under the default `on_failure: skip` those records are settled as failures. An overload in the default configuration becomes data loss. Even in slow mode, a `max_conn` above 2 reconnects constantly, for the same idle-pool reason.

## What Changes

- Fast mode gets a built-in in-flight ceiling: at most 512 upstream calls per pipeline at a time. When the ceiling is reached, the pipeline stops taking records until a call finishes, so consumption is held back instead of piling up calls. The ceiling is a code constant; no configuration key is added. Below the ceiling, fast mode behaves as today.
- Each HTTP upstream gets its own connection pool sized to the pipeline's concurrency limit (`max_conn` in slow mode, the ceiling in fast mode), so concurrent calls reuse connections instead of reconnecting. Pipelines no longer share `http.DefaultTransport`. TLS settings are applied as before.
- The fast-mode startup log line states the ceiling.
- Docs: README (`max_conn` description, troubleshooting note on memory, features list) and `docs/flow.md` ("Concurrency" key point).

## Non-goals

- A configuration key for the ceiling, or changing the meaning of `service.max_conn`.
- Changing slow mode's concurrency rules or `on_failure: retry` (which still requires `max_conn > 0`).
- Making the internal channel buffers (1000) configurable.
- A per-host connection cap inside the HTTP client: the in-flight ceiling already bounds concurrent calls.
- gRPC upstream connection handling (one multiplexed connection per pipeline, unchanged).
- Rate limiting or adaptive concurrency.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `transport-modes`: "Mode selection from configuration", where the fast-mode scenario is no longer "without concurrency limiting"; new requirements "Fast-mode in-flight ceiling" and "Upstream connection reuse".

## Impact

- Code: `internal/transport/dispatch.go` (`withReplyService` uses a semaphore in both modes, sized by `max_conn` or the ceiling), `internal/transport/upstream_http.go` (a per-upstream cloned `http.Transport` with idle-pool sizing), and the tests next to them. `TestFastModeDispatchesWithoutCap` changes meaning: it stays valid below the ceiling.
- Behaviour visible to operators: fast-mode pipelines cannot exceed 512 concurrent upstream calls. For most deployments nothing changes; a catch-up burst is spread over time instead of hitting the upstream all at once.
- Docs: `README.md`, `docs/flow.md`.
- No new dependencies and no new configuration keys.
