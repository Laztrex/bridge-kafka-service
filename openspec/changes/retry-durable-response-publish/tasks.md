# Tasks

## 1. Startup validation for retry stanzas

- [ ] 1.1 In `internal/config` reject an `on_failure: retry` stanza whose non-empty `response_topic` is not a `producer:` topic, with an error naming the stanza and the topic (design D6); verify with table cases in `internal/config/upstream_test.go` (unlisted -> error mentions stanza and topic; listed -> ok; skip with unlisted -> ok)
- [ ] 1.2 Check `configs/*.yaml` and `internal/config/testdata` examples still validate (`go test ./internal/config/...`), and update the README consumer configuration section to state the rule

## 2. Response publication retry

- [ ] 2.1 Set `publishEvent.Config = msg.Config` in `internal/transport/response.go`; verify with a test that the queued event carries the stanza config
- [ ] 2.2 Export the backoff helper from `internal/transport/retry.go` (design D4) without changing its behaviour; verify the existing retry tests still pass
- [ ] 2.3 Split `observability.FileTransactionLogger.Publish` so that the send and the single outcome/audit recording can be driven separately; the HTTP publish endpoint keeps its single-attempt behaviour; verify with the existing `internal/observability` and `internal/handlers` tests unchanged
- [ ] 2.4 Add the non-transient error classifier (design D3) with a unit test per error (marshal error, `ErrMessageSizeTooLarge`, `ErrInvalidMessage`, `ErrInvalidMessageSize` -> non-transient; `ErrOutOfBrokers`, `ErrNotLeaderForPartition`, a timeout -> transient)
- [ ] 2.5 Implement the retry loop in `publishOne` for events whose `Config` has `on_failure: retry` (design D1): ack only on success or non-transient error, abort the wait on ctx without ack; verify with tests using a failing mock `SyncProducer` from `testutil/`: fails twice then succeeds -> one send success, two warnings, ack once; ctx cancelled during backoff -> no ack; non-transient -> one attempt, ack, failure recorded; skip event failing -> one attempt, ack
- [ ] 2.6 Make the drain after ctx cancellation give retry events a single attempt and leave them unsettled on failure; verify with a test that cancels ctx while two retry events are queued and the producer fails
- [ ] 2.7 Update `TestResponsePublish_PublishesAndAcks` (`internal/handlers/handlers_test.go:98`) so the "no producer" case uses a skip event, and add the retry-event fallback case (design D6, last sentence)
- [ ] 2.8 Update README "Service failures" (the "never dropped" paragraph) to describe publication retry, the non-transient exception and shutdown redelivery, and update `docs/flow.md` key points on settlement

## 3. Fatal producer failure when responses are expected

- [ ] 3.1 Make `startProducer` return its error and, in `Connect`, report a permanent failure to `errCh` when any active stanza has a `response_topic`; raise `errCh` capacity to `len(consumers)+2` (design D5); verify with a test in `internal/app` that uses the `newProducerWithRetry` seam to fail: `Connect` returns a non-nil error mentioning the producer
- [ ] 3.2 Gate consumers on the recorded producer outcome so a stanza with `response_topic` never calls `RunSubscription` after a producer failure; verify with a test that a consumer seam is never invoked (and `/status` shows `consumers_up: 0`) when the producer fails
- [ ] 3.3 Keep the current behaviour when no stanza has a `response_topic`; verify with a test that `Connect` keeps running after a producer failure in that configuration
- [ ] 3.4 Update the README startup bullets (next to "If a consumer client cannot be created within 5 minutes, the process exits with code 1")

## 4. Integration

- [ ] 4.1 Run `go build ./... && go vet ./... && go test -race ./...` and `openspec validate retry-durable-response-publish --strict`
- [ ] 4.2 Run `make ci` (includes `docs-check` and the coverage gate) and fix any failures
