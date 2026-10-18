# Design

## Context

See proposal.md (Why) for the motivation. Current state of the failure paths:

```
 Connect (internal/app/connection.go)
   config.Validate / config.PrepareUpstreams        -- startup checks, before HTTP
   per-consumer goroutine:
     newConsumerClientWithRetry FAIL (ctx alive) --> errCh --> exit 1   (already fatal)
     ...
     ready.consumerUp()                                  -- monotonic counter, never falls
     runSubscription(ctx, toggle, client, conf, logger)  -- return value ignored (:155)
        NewUpstream FAIL         --> log, return error    <-- silent death #1 (consumer.go:58)
        newConsumerGroup FAIL    --> log, return error    <-- silent death #2 (consumer.go:77)
        consume loop: transient errors retried every 5s;  <-- already resilient,
          ErrClosedConsumerGroup ends the loop                stays as is
```

Facts that shape the approach:

- The consume loop (`internal/router/consumer.go`) already retries transient session errors with a 5-second back-off and only exits on context cancellation or `ErrClosedConsumerGroup`. Permanent death therefore happens only at subscription *start*; the two early returns are the whole surface of the problem.
- Neither start failure involves the broker. `sarama.NewConsumerGroupFromClient` (sarama v1.60.2, `consumer_group.go:145`) fails only when `Version < V0_10_2_0`, when the rebalance strategies are invalid, or when the client is already closed (`ErrClosedClient`). `NewHTTPUpstream` cannot fail; `grpc.NewClient` (v1.83.2) connects lazily and fails only when the target cannot be parsed or dial options conflict (credentials are always set, so only the target remains).
- `kafka.version` is parsed in `NewConsumerClient` (`consumer.go:300`); an unparseable value falls back to `V2_7_0_0`, but a parseable old value such as `0.10.1.0` is kept (0.x versions parse only in four-part form, so `0.10.0` falls back) and makes group creation fail. `config.Validate` does not look at the version today. The sarama tuning-file overlay cannot change `Version` (its fields are unexported).
- The gRPC target is `service.name + ":" + service.port` (`schemas.HostConfig.Target`); `PrepareUpstreams` checks protocol, failure policy and TLS but not the target.
- Working tree: `RunSubscription` already returns `error` with both wraps and `return nil` at the end. `Connect` still ignores the value, and the `runSubscription` seam in `internal/app/connection_test.go:343` still has the old signature, so `internal/app` tests do not compile.
- `errCh` has capacity `len(consumers)+2` (`connection.go:75`): HTTP server, producer, and one report per consumer goroutine. This change reuses the per-consumer slot — no new reporter, no capacity change.
- A stanza's `Topics` is the operator-facing identity used by the existing client-creation error ("permanently failed to create consumer client for %v").

## Goals / Non-Goals

**Goals:**

- One failure taxonomy: a permanent inability to consume — at client creation or at subscription start — is fatal, named by topics.
- Configuration that is known to make a subscription unstartable is rejected at startup validation, so the runtime path is a safety net and not the usual way operators learn about a typo.
- No change to the transient retry behaviour, shutdown semantics, or readiness counting.

**Non-Goals:**

- A forward-progress probe, readiness decrements or per-stanza isolation (see proposal Non-goals).
- Start retries inside `RunSubscription`: every known cause is deterministic.

## Decisions

### D1. `RunSubscription` returns `error`; only permanent failures are non-nil

The two early returns become `fmt.Errorf("subscription for topics %v: cannot create upstream: %w", ...)` and `... cannot create consumer group: %w`. Every other exit (context cancelled before or during start, `ErrClosedConsumerGroup`, normal end) returns `nil`. The error text names the topics, matching the existing client-creation error family. (Already in the working tree.)

Alternative rejected: reporting to a channel from inside `RunSubscription`. It would push app-level plumbing (`errCh`) into the `router` package for no behavioural gain.

### D2. `Connect` reports the returned error through the existing goroutine

At the single call site the per-consumer goroutine captures the return value and sends it to `errCh` only when it is non-nil and `ctx.Err() == nil` — the same guard the client-creation failure uses, so a shutdown racing a start failure stays a clean stop. The existing `loop:` select already turns any `errCh` value into a graceful termination with a non-zero exit. The test seam `runSubscription` keeps the production signature (`... ) error`).

Alternative rejected: calling `os.Exit` from the subscription path. Exit codes belong to `main`, and `errCh` is the established route.

### D3. Readiness stays monotonic on purpose

`consumers_up` keeps counting up only. A subscription that dies is now a process that dies; between the `errCh` report and the exit, the graceful shutdown cancels the root context and stops the HTTP server. A decrementing counter would only matter for a degraded-but-running state this change makes unreachable.

### D4. Kafka version is validated in `config.Validate`

When at least one consumer stanza is active (non-empty `Topics`), `Validate` parses `kafka.version` with `sarama.ParseKafkaVersion`; if parsing succeeds and the result is below `sarama.V0_10_2_0`, it returns an error naming the configured value and the minimum (`kafka.version %q is below 0.10.2, required by consumer groups`). An unparseable value is left alone: it falls back to `V2_7_0_0` in `NewConsumerClient`, which satisfies the check. Producer-only deployments are not checked (the sync producer does not need groups).

The check mirrors sarama's own condition and runs before the HTTP server starts and before any broker connection, so `Connect` returns immediately with exit status 1.

Alternative rejected: making an unparseable version fatal. It changes which configs are accepted (backward-compatible config format) and is unrelated to group creation.

### D5. gRPC target is validated in `config.PrepareUpstreams`

For an active stanza (non-empty `Topics`, as in `Validate`) whose effective protocol is `grpc`, `PrepareUpstreams` requires a non-empty `service.name` (already guaranteed for active stanzas by `Validate`), no whitespace or control characters in it, and a `service.port` that is a decimal number in 1–65535. Otherwise it returns an error naming the stanza (`consumer #N (topics [...]): service.port %q is not a valid port for grpc`). This is the same error style as the existing protocol and failure-policy checks.

The check is a shape check, not a dry run of `grpc.NewClient`: it keeps `internal/config` free of gRPC, and the residue (exotic targets that pass the shape check but fail parsing) is caught by D1/D2 at runtime. DNS resolution stays lazy (`grpc-upstream` "Service down at startup").

Alternative rejected: constructing and closing the gRPC client during validation. Exact, but it couples config validation to the transport package for a case the runtime safety net already covers.

### D6. Tests

- `internal/router`: `RunSubscription` returns an error naming the topics when the upstream cannot be constructed (a `grpc` stanza whose target fails `grpc.NewClient` parsing, bypassing `PrepareUpstreams`) and when `newConsumerGroup` fails through the seam; returns `nil` when the context is cancelled before start. Existing tests ignore the return value and stay unchanged.
- `internal/app`: the `runSubscription` seam in `connection_test.go` is updated to return `error`. With the `newConsumerGroup` path failing after a successful client creation (seam returning an error), `Connect` returns an error mentioning the topics; with the context cancelled in that window, nothing is reported through that path.
- `internal/config`: `Validate` rejects `version: 0.10.0` with an active consumer, accepts `0.10.2.0`, accepts an unparseable value, and accepts `0.10.1.0` for a producer-only config. `PrepareUpstreams` rejects a `grpc` stanza with port `""`, `"abc"`, `"0"`, `"70000"` and accepts `"50051"`; an `http` stanza with the same bad port is not rejected by this check.

## Risks / Trade-offs

- [A cause slips past validation and the subscription fails at start] → The process exits non-zero with an error naming the topics; since the causes are deterministic, the supervisor sees a crash loop. That is the intended visibility: it replaces a process that looked healthy while consuming nothing.
- [One bad stanza stops the healthy ones] → Accepted (proposal Non-goals); with D4/D5 the realistic causes stop at startup, before any stanza consumes.
- [A config with `kafka.version` below 0.10.2 that used to start now refuses to] → Such a config already could not consume (every subscription died silently); only producer-only configs could use it, and they are not checked.
- [Operators see an exit where they used to see a quiet log line] → Documented in the README next to the existing client-creation bullet.

## Migration Plan

No configuration migration: configs rejected by D4/D5 could not consume on the affected stanzas before. Rollback means redeploying the previous build.
