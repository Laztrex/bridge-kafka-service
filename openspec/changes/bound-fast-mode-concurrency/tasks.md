# Tasks

## 1. Fast-mode in-flight ceiling

- [ ] 1.1 Add `transport.FastModeMaxInFlight = 512` and a test seam for the effective fast-mode limit, exported through `internal/transport/export_test.go` (design D1, D4); verify `go build ./...`
- [ ] 1.2 Make `withReplyService` use the semaphore path in both modes, sized by `max_conn` or the ceiling, and include the ceiling in the fast-mode startup log line (design D1); verify that the existing slow-mode, retry and lifecycle tests in `internal/transport` pass unchanged
- [ ] 1.3 Replace `TestFastModeDispatchesWithoutCap` (`internal/transport/dispatch_test.go:217`) with a below-ceiling test and an at-ceiling test using the lowered seam; add a shutdown-at-ceiling test asserting the waiting record is not acked (design D4); verify `go test -race ./internal/transport/...`
- [ ] 1.4 Update README (`max_conn` description: "0 = fast mode, at most 512 concurrent calls"; troubleshooting "Memory usage" bullet; features "Connection pool" bullet) and `docs/flow.md` "Concurrency" key point; verify the text matches the spec

## 2. Upstream connection reuse

- [ ] 2.1 Factor the concurrency-limit rule into one helper used by `withReplyService` and `NewHTTPUpstream` (design D3); verify with a unit test for `max_conn` 0, -1 and 8
- [ ] 2.2 Make `NewHTTPUpstream` always clone `http.DefaultTransport` and set `MaxIdleConnsPerHost`/`MaxIdleConns` to the limit, keeping the TLS config; verify with the existing `upstream_http` and TLS tests
- [ ] 2.3 Add the `httptest.Server` `ConnState` test (slow mode `max_conn: 8`, 200 calls, at most 8 new connections) and the fast-mode variant with the lowered seam (design D4); verify `go test -race ./internal/transport/...`

## 3. Integration

- [ ] 3.1 Run `go build ./... && go vet ./... && go test -race ./...` and `openspec validate bound-fast-mode-concurrency --strict`
- [ ] 3.2 Run `make ci` (includes `docs-check` and the coverage gate) and fix any failures
