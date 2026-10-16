# Design

## Context

See proposal.md (Why) for the motivation. Current state:

```
 ConsumeClaim --> ch (buf 1000) --> withReplyService
                                     slowMode: sem(max_conn) --> go sendToService
                                     fastMode: ------------------> go sendToService   (no limit)
                                                                       |
                                       httpUpstream: http.DefaultTransport (shared by all
                                       non-TLS pipelines; MaxIdleConnsPerHost=2,
                                       MaxIdleConns=100, MaxConnsPerHost=0)
```

- `internal/transport/dispatch.go` `withReplyService` already has every piece needed: the semaphore path with the `ctx.Done()` escape, plus in-flight tracking (`calls` WaitGroup) and the drain window. Fast mode differs only by skipping the semaphore.
- The `offsetTracker.pending` slice (`internal/router/consumer.go`) grows with in-flight records, so bounding calls also bounds it, together with `ch`.
- The default upstream timeout is 30s (`consts.DefaultServiceTimeout`). A stalled upstream holds a slot for up to 30s.
- `NewHTTPUpstream` clones `http.DefaultTransport` only when TLS is set. Otherwise it uses the shared global transport.

## Goals / Non-Goals

**Goals:**
- Bound goroutines, sockets and memory per fast-mode pipeline without new config.
- Stop connection churn for both modes on HTTP upstreams.

**Non-Goals:**
- Protecting the upstream from a deliberately high load: that remains the job of `max_conn`.
- Any change to retry or settlement semantics.

## Decisions

### D1. One code path: a semaphore in both modes

`withReplyService` computes `limit := conf.Service.MaxConn; if limit <= 0 { limit = FastModeMaxInFlight }`, always creates `sem := make(chan struct{}, limit)`, and uses the existing slow-mode branch for every record. The `slowMode` flag stays only for logging and naming (`ConsumerResponseFast`/`ConsumerResponseSlow` and the "fast mode"/"slow mode" log lines remain, since the spec and operators refer to them). Export the constant (`transport.FastModeMaxInFlight = 512`) so tests and the log line can use it.

Alternative rejected: a separate goroutine-pool or worker model for fast mode. It would add a second dispatch shape for no behavioural gain.

### D2. Ceiling value 512

The value has to be high enough that existing fast-mode users do not notice it in normal operation, and low enough to stay well under the common default soft `ulimit -n` of 1024. The process also holds Kafka broker sockets, the metric file and audit gRPC connections, and an HTTP upstream call uses one FD. 1000 (the channel buffer size) would leave too little FD headroom. The constant stays a variable-free `const`. If an operator needs more, `max_conn` already expresses an explicit limit.

### D3. Per-upstream HTTP transport sized to the limit

`NewHTTPUpstream` always clones `http.DefaultTransport.(*http.Transport)`. It sets `MaxIdleConnsPerHost = limit` and `MaxIdleConns = limit`, where `limit` comes from the same rule as D1 (factor a small helper, for example `concurrencyLimit(conf)`, used by both), and keeps `TLSClientConfig` when TLS is configured. `MaxConnsPerHost` stays 0: the semaphore is the cap, and a second cap inside the transport would only make calls queue invisibly inside `RoundTrip` while still counting against the upstream timeout.

### D4. Tests

- Replace `TestFastModeDispatchesWithoutCap` with two tests. Below the ceiling, N records start concurrently (the old assertion with N=5). At the ceiling, temporarily lower the ceiling through a test seam (for example an unexported `var fastModeLimit = FastModeMaxInFlight` exported via `export_test.go`, as `FastRetries` already is), block the upstream, and assert that exactly `limit` calls start and that the next one starts only after one is released.
- Shutdown at the ceiling: cancel ctx while the semaphore is full and a record waits; `withReplyService` returns and the waiting record's ack is not called.
- Connection reuse: an `httptest.Server` with a `ConnState` hook counting `StateNew`; slow mode `max_conn: 8`, 200 calls, assert `<= 8` new connections. Add the same test with the fast-mode ceiling lowered through the seam.

## Risks / Trade-offs

- [A fast-mode deployment that relied on more than 512 concurrent calls slows down] → This is unlikely, since 512 concurrent calls with a 30s timeout is already an overloaded upstream. It is documented in the README, and `max_conn` can be set explicitly. Note that `max_conn` is not capped by the ceiling.
- [512 idle connections per fast-mode pipeline can stay open after a burst] → `IdleConnTimeout` (90s, inherited from `DefaultTransport`) closes them.
- [Behaviour change visible in logs: Kafka reading pauses at the ceiling] → This is the intended backpressure, the same as slow mode today.

## Migration Plan

No configuration migration. Rollback means redeploying the previous build.
