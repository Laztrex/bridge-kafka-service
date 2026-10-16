# Proposal

## Why

`on_failure: retry` promises that a response is never dropped (README, "Service failures"), but the last step of the pipeline does not keep that promise. When writing the response to `response_topic` fails, the response publisher records a failure and still settles the offset, so the record is lost. When the shared producer cannot be created within the retry window, consumers start anyway: every record is sent to the upstream (a side effect), its response is discarded as "no producer" and its offset is committed. A stanza can also name a `response_topic` that no producer serves, which loses every response the same way. The archived change `2026-10-14-upstream-failure-retry` left producer errors out of scope on purpose. This change closes that gap.

## What Changes

- **Response publication retry (retry stanzas only).** If writing a response of an `on_failure: retry` stanza to Kafka fails with a transient error, the offset is held and the write is repeated with the same backoff the upstream retry uses (about 1s doubling to about 30s, with jitter) until it succeeds or the service stops. On stop, the offset of an unpublished response is not settled, so the record is redelivered. Each failed attempt is logged as a warning, and metrics and audit record only the final outcome. A non-transient error (for example, a record too large for the broker) is recorded as a failure and settled, like a poison record. `skip` stanzas behave as before.
- **BREAKING (operational): permanent producer failure is fatal when responses must be published.** If the shared producer cannot be created within the retry window and at least one active consumer stanza has a `response_topic`, the process exits non-zero, the same way it does when a consumer client cannot be created. No consumer starts reading before the producer is ready. Deployments where no stanza has a `response_topic` keep the current behaviour (logged, consumption runs).
- **Startup validation:** a stanza with `on_failure: retry` whose `response_topic` is not among the configured `producer:` topics aborts startup with an error that names the stanza and the topic.
- Docs: README "Service failures" and startup-behaviour bullets; `docs/flow.md` key points where publication and settlement are described.

## Non-goals

- Dead-letter storage for responses that cannot be published.
- Retrying response publication for `skip` stanzas, or for responses dropped because the publication queue is full under `skip` (that is the documented `skip` trade-off).
- Changing the HTTP publish endpoint (`POST /api/v1/publish`): it still reports a single attempt to its caller.
- Making the response publisher concurrent or per-pipeline (throughput ceiling, tracked separately).
- Making the drain of queued responses on shutdown bounded in time.
- Readiness semantics (`/readyz`, `/status`) and silent subscription failures.
- Configurable publish backoff: it reuses the code constants of the upstream retry.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `message-routing`: "At-least-once offset settlement" no longer lists "missing producer" as a terminal outcome for `retry` stanzas; a new requirement covers response publication retry for `retry` stanzas; "Retry requires a concurrency cap" gains a sibling requirement that the response topic of a retry stanza must be a producer topic.
- `transport-modes`: "Response decoupling from request path", where the scenario "Responses drained without producers" no longer covers a failed producer creation when responses must be published.
- `app-configuration`: "One producer shared by all producer topics", where the scenario "Producer creation fails permanently" splits into a fatal case (a `response_topic` is configured) and the current case (no `response_topic`).

## Impact

- Code: `internal/handlers/handler_http.go` (`publishOne` retry loop for retry events), `internal/transport/response.go` (the response event carries the stanza config), `internal/app/connection.go` (producer failure reported to `errCh`, consumers gated on producer success, `errCh` capacity), `internal/config` (new validation), plus tests next to each.
- Behaviour visible to operators: a broker that accepts no writes now stalls `retry` pipelines instead of losing their responses, and a producer that never connects restarts the process instead of leaving it running and discarding responses.
- Docs: `README.md`, `docs/flow.md`.
- No new dependencies and no new configuration keys.
