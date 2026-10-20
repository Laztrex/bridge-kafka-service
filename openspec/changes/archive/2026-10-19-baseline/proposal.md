# Proposal

## Why

The service reached its first stable shape through a series of hardening changes made on 2026-10-18 and 2026-10-19. Their requirements are already merged into `openspec/specs/`, which are the source of truth for what the service does. What the main specs do not keep is *why* it is built this way: the decisions taken and the alternatives rejected. This record replaces the individual change records with one baseline, so the archive starts with "this is how the service was designed", and the reasoning stays in one place.

## What Changes

Nothing in behaviour. This is a record-keeping change:

- the per-change records listed below are replaced by this baseline;
- `design.md` keeps every decision that still holds, grouped by area, with stable IDs (for example `DEL-3`, `RUN-3`) that code comments and later changes can cite;
- where a later change reversed an earlier decision, only the final decision is kept, and the earlier one is listed as a rejected alternative.

Consolidated records (their decisions live on in `design.md`; the records themselves are not kept):

| Date | Change | Area |
|---|---|---|
| 2026-10-18 | `bound-fast-mode-concurrency` | concurrency limits |
| 2026-10-18 | `retry-durable-response-publish` | response publication retry |
| 2026-10-18 | `fail-fast-subscription-death` | fatal start failures |
| 2026-10-18 | `per-pipeline-response-publisher` | publication isolation |
| 2026-10-18 | `kafka-client-tuning` | Kafka tuning files |
| 2026-10-18 | `settle-in-flight-before-release` | settlement on session end |
| 2026-10-18 | `publish-size-and-version-limits` | size and version limits |
| 2026-10-18 | `audit-and-metrics-sinks` | audit and metric files |
| 2026-10-18 | `operational-hygiene` | listener timeouts, disabled stanzas |
| 2026-10-19 | `close-silent-stall-and-loss-paths` | non-retryable failures, headers, `/status` |
| 2026-10-19 | `startup-config-guards` | startup checks, `--check-config` |
| 2026-10-19 | `runtime-state-consistency` | pause, producer failure |
| 2026-10-19 | `disk-retention` | backup retention |

## Non-goals

- Changing any requirement, scenario or behaviour.
- Rewriting git history.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

_None._ No spec-level behaviour changes; the change declares `skip_specs`.

## Impact

- `openspec/changes/archive/`: thirteen records become one.
- Code comments that cited a decision of a consolidated record (`design D1 of audit-and-metrics-sinks`, `design D1`, `design D3`, `design D6`) cite the baseline IDs instead.
