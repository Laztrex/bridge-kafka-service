# Proposal

## Why

A subscription that cannot start dies silently. `RunSubscription` has two early-return paths — upstream construction failure and consumer-group creation failure — that only log an error and return; the error channel never learns, `/readyz` keeps answering ready and `/status` keeps showing `consumers_up: N/N` (the counter is incremented before the subscription starts) while records pile up in the topic. The failure is discovered by whoever reads the response topics, not by monitoring. Consumer *client* creation failure is already fatal (exits the process, supervisor restarts); subscription death is the same class of permanent inability to consume but gets the opposite, silent treatment.

Both start failures are deterministic, not broker-side: `sarama.NewConsumerGroupFromClient` does no network I/O and fails only on a configured Kafka version below 0.10.2, an invalid rebalance strategy or an already-closed client; `grpc.NewClient` connects lazily and fails only on a target it cannot parse. A restart does not heal them. Making them fatal at subscription start turns silent death into a visible crash loop; the configuration-driven causes should instead be rejected at startup validation, before the HTTP server starts and before any Kafka connection, with an error naming the setting.

## What Changes

- `RunSubscription` returns an `error` instead of logging-and-returning: a permanent start failure (upstream cannot be constructed, consumer group cannot be created) is wrapped with the stanza's topics; clean stops (context cancelled, `ErrClosedConsumerGroup`) return `nil`.
- `Connect` reports that error to `errCh` from the existing per-consumer goroutine (same slot that already reports client-creation failure), so the process shuts down gracefully and exits non-zero, exactly like a permanent client-creation failure. This is the safety net for causes validation cannot see (closed client, future failure modes).
- Startup validation rejects the configuration-driven causes:
  - `config.Validate`: when at least one consumer stanza is active, a `kafka.version` that parses to a version below 0.10.2 stops startup (consumer groups require it). An unparseable value keeps today's fallback to 2.7.0, which supports groups.
  - `config.PrepareUpstreams`: for a `grpc` stanza, `service.name` must be non-empty and `service.port` a number in 1–65535, so `{name}:{port}` is a valid target; otherwise startup fails with an error naming the stanza.
- Transient consume-loop errors keep today's behaviour: the existing retry loop (5s back-off, `ErrClosedConsumerGroup` ends the loop) is unchanged.
- Docs: README startup-behaviour bullets and the configuration section.

## Non-goals

- A forward-progress health probe (current-vs-committed offset, liveness restarts): the process now dies instead of hanging, which is the cheaper half of that pattern; a progress probe can be added separately if hangs are ever observed.
- Readiness decrement semantics: `consumers_up` stays monotonic; a dead subscription now means a dead process, and the graceful shutdown stops the probes.
- Isolating a failed stanza while the others keep running: one stanza that cannot start terminates the process with all healthy stanzas. For a Kafka consumer readiness gates almost no traffic (only the publish API), so a degraded-but-running process would just be silent death again.
- Lag monitoring and alerting (an external concern, e.g. Burrow-style).
- Retrying subscription start: every known cause is deterministic, so neither an in-process retry nor a restart heals it.
- Making an unparseable `kafka.version` fatal: it changes what configs are accepted today (`app-configuration` backward compatibility); the fallback version supports consumer groups.
- Port validation for `http` stanzas and DNS resolution of gRPC hosts at startup: HTTP upstream construction cannot fail, and resolution failures are transient transport failures by design (`grpc-upstream` "Service down at startup").
- Changing `errCh` capacity: it is already `len(consumers)+2`; this change adds no new reporting goroutine.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `runtime-control`: "Fail fast on permanent consumer failure" is extended from client-creation failure to any permanent inability to start consuming (upstream construction, group creation); new scenarios cover both, and the boundary to the transient retry loop is stated explicitly.
- `app-configuration`: new requirement — a Kafka version below 0.10.2 with active consumers stops startup.
- `grpc-upstream`: new requirement — a gRPC stanza's target (`service.name`, `service.port`) is validated at startup.

## Impact

- Code: `internal/router/consumer.go` (`RunSubscription` signature and its two early returns — already changed in the working tree), `internal/app/connection.go` (report the returned error to `errCh`), `internal/config/config.go` (`Validate`), `internal/config/upstream.go` (`PrepareUpstreams`), tests next to each, including the `runSubscription` seam in `internal/app/connection_test.go`.
- Behaviour visible to operators: a misconfigured Kafka version or gRPC target now stops startup with an error naming the setting; a subscription that still cannot start terminates the process (non-zero exit) instead of leaving it running idle-but-ready; the error names the affected topics.
- Docs: `README.md`.
- No new dependencies (`internal/config` starts importing `IBM/sarama`, already a module dependency) and no new configuration keys.
