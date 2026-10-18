# Tasks

## 1. Subscription start failures become errors

- [x] 1.1 Make `RunSubscription` (`internal/router/consumer.go:49`) return `error`: wrap the upstream construction failure and the `newConsumerGroup` failure with the stanza's topics; return `nil` on every clean-stop path (design D1). The working tree already has this change — review it against D1 and verify `go build ./...` and the existing `internal/router` tests pass unchanged
- [x] 1.2 Report the returned error in `Connect` (`internal/app/connection.go:155`) to `errCh` when it is non-nil and `ctx.Err() == nil`, with the same guard shape as the client-creation failure (design D2)
- [x] 1.3 Update the `runSubscription` seam in `internal/app/connection_test.go:343` to the new signature (return `nil`), so `internal/app` tests compile again; verify they pass

## 2. Startup validation

- [x] 2.1 In `config.Validate` (`internal/config/config.go`), reject a `kafka.version` that parses to below 0.10.2 when at least one consumer stanza is active; leave unparseable values and producer-only configs alone (design D4)
- [x] 2.2 In `config.PrepareUpstreams` (`internal/config/upstream.go`), require for `grpc` stanzas a `service.name` without whitespace or control characters and a decimal `service.port` in 1–65535, with an error naming the stanza (design D5)

## 3. Failure and clean-stop coverage

- [x] 3.1 Add router tests (design D6): `RunSubscription` returns an error naming the topics when the upstream cannot be constructed and when the `newConsumerGroup` seam fails; returns `nil` when the context is cancelled before start
- [x] 3.2 Add an app test (design D6): with the subscription start failing after a successful client creation, `Connect` returns an error mentioning the topics; with the context cancelled in the same window, no error is reported through that path
- [x] 3.3 Add config tests (design D6): version boundary (`0.10.1.0` rejected, `0.10.2.0` accepted, unparseable accepted, producer-only accepted) and gRPC port cases (`""`, `"abc"`, `"0"`, `"70000"` rejected, `"50051"` accepted, `http` and topic-less stanzas unaffected)

## 4. Docs and integration

- [x] 4.1 Update the README: in "Startup, shutdown and signals", next to the "consumer client cannot be created within 5 minutes" bullet, state that a subscription that cannot start terminates the process with exit code 1 and names the topics; in the configuration section, state that `kafka.version` below 0.10.2 with active consumers and an invalid gRPC `service.port` stop startup; verify the text matches the spec deltas
- [x] 4.2 Run `go build ./... && go vet ./... && go test -race ./...` and `openspec validate fail-fast-subscription-death --strict`
- [x] 4.3 Run `make ci` (includes `docs-check` and the coverage gate) and fix any failures
