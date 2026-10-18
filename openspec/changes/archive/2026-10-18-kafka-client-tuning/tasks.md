# Tasks

## 1. Tuning loader

- [x] 1.1 Add `internal/router/tuning.go` with `Tuning`, `LoadConsumerTuning()`, `LoadProducerTuning()` and `(*Tuning).apply(cfg)` (design D1):
  - read and parse into a `yaml.Node`, with errors naming the file;
  - walk the tree against `sarama.Config` with the lowercased-field-name rule;
  - warn about and remove unknown keys (full dotted path) and unsettable interface/func keys;
  - read and remove `OffsetsAutoCommitInterval`;
  - `apply` decodes the pruned node under a `recover` guard and is a no-op on nil.

  Verify with the unknown-key, unsettable-key, app-key, missing-file, unparsable-file and nil cases of design D5 (`go test -race ./internal/router/`)
- [x] 1.2 Parse `consumer.group.rebalance.strategy` in `LoadConsumerTuning` (design D2): `range`, `roundrobin`, `sticky` and `cooperative-sticky` map to the sarama constructors, case-insensitive; any other value is an error naming the key, the value and the allowed list; an absent key leaves the default. Verify with a table test comparing `Name()` of the applied strategy, plus the error case

## 2. Clients use the loaded tuning

- [x] 2.1 Change `NewConsumerClient(conf, stanza, tuning)` and `NewProducer(conf, tuning)` to apply the tuning after the code defaults and before the forced settings, with no file I/O (design D3). Remove `configConsumerUpdater` and resolve the TODO at `consumer.go:297`. Move the missing/broken-file tests from the constructors to the loader, keep the overlay test, and add two tests: `consumer.group.session.timeout: 20s` overrides 60s while heartbeat stays 10s, and the strategy reaches `GroupStrategies`. Verify with `go test -race ./internal/router/`
- [x] 2.2 Add a test that the shipped `configs/config_consumer.yaml` and `configs/config_producer.yaml` load without warnings. Remove `producer.offsets.*` and the consumer-only `OffsetsAutoCommitInterval` from `configs/config_producer.yaml`: sarama's producer config has no such fields. Verify the test passes

## 3. Startup loading

- [x] 3.1 In `Connect` (`internal/app/connection.go`), after `config.PrepareUpstreams` and before the transaction logger and the HTTP server, load the consumer tuning when a stanza is active and the producer tuning when producers are configured, returning `invalid Kafka consumer/producer tuning: ...` on error. Pass the tuning to `createConsumerClientWithRetry` / `createProducerWithRetry` and their seams (design D4), and update the 11 seam assignments in `internal/app` tests. Verify with `go test -race ./internal/app/`
- [x] 3.2 Add app tests (design D5): a missing producer file with a producer configured fails `Connect` before the HTTP server starts; the same missing file with no producers does not; an unknown strategy fails startup. Verify with `go test -race ./internal/app/`
- [x] 3.3 Update the README configuration section (Kafka client tunables):
  - tuning files are validated at startup and a missing or broken file stops startup;
  - unknown keys and keys that cannot be set from YAML are warned about and ignored;
  - `consumer.group.rebalance.strategy` with its four values, default `range`, and the migration note (stop every member, or expect `InconsistentGroupProtocol` while strategies are mixed);
  - the timing defaults (session 60s, heartbeat 10s, max processing time 10s) and that the file overrides them.

  Verify the wording matches the spec delta and `make docs-check` passes

## 4. Integration

- [x] 4.1 Run `go build ./... && go vet ./... && go test -race ./...` and `openspec validate kafka-client-tuning --strict`
- [x] 4.2 Run `make ci` (includes lint, the coverage gate and `docs-check`) and fix any failures
