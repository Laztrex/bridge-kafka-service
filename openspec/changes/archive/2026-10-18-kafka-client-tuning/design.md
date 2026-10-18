# Design

## Context

See proposal.md (Why) for the motivation. Current flow:

```
 Connect: Validate -> PrepareUpstreams -> logger -> HTTP -> background:
   createConsumerClientWithRetry (5 min backoff)
     NewConsumerClient(kafkaConfig, stanza)          internal/router/consumer.go:300
       cfg := sarama.NewConfig(); version; ClientID
       code defaults: GroupStrategies=[range], session 60s, heartbeat 10s, MaxProcessingTime 10s
       os.ReadFile(config_consumer.yaml)              <- every attempt
       yaml.Unmarshal(raw, &cfg)                      <- lenient; panics on interface fields
       yaml.Unmarshal(raw, &configConsumerUpdater)    <- OffsetsAutoCommitInterval
       AutoCommit.Enable = true; TLS
   createProducerWithRetry -> NewProducer: same pattern with config_producer.yaml
```

Facts that shape the approach (checked against sarama v1.60.2 and yaml.v3 v3.0.1):

- yaml.v3 maps a struct field to its lowercased name (sarama has no yaml tags). Unknown keys are dropped silently unless `KnownFields(true)` is set. `time.Duration` decodes from strings like `20s`.
- Decoding a scalar into an interface-typed field panics (`reflect.Set: value of type string is not assignable to type sarama.BalanceStrategy`), and yaml.v3 re-panics it. The affected fields include `consumer.group.rebalance.strategy` (deprecated `Strategy BalanceStrategy`), `consumer.group.rebalance.groupstrategies` (`[]BalanceStrategy`), `producer.partitioner` (func), `producer.interceptors` / `consumer.interceptors`, `net.sasl.tokenprovider`, `net.proxy.dialer`, `metricregistry`, and the func fields of `net.tls.config`.
- sarama provides `NewBalanceStrategyRange`, `NewBalanceStrategyRoundRobin`, `NewBalanceStrategySticky` and `NewBalanceStrategyCooperativeSticky`.
- `KafkaInfo` cannot carry a router type: `schemas` is imported by `router`, so a router type in `schemas` would create an import cycle.
- `internal/app` tests `t.Chdir` to the repo root, so `configs/config_consumer.yaml` and `config_producer.yaml` are present in Connect tests. 11 test assignments use the `newConsumerClientWithRetry` / `newProducerWithRetry` seams.
- Existing router tests (`TestNewConsumerClient_ConfigHandling`, `TestNewProducer_MissingAndBrokenConfig`, `..._AppliesDefaultsAndYAMLOverlay`) exercise the file through the client constructors via the `consumerConfigPath` / `producerConfigPath` seams.

## Goals / Non-Goals

**Goals:**

- No tuning-file content can crash the process.
- A file problem that can never heal (missing, unparsable, invalid strategy) is reported at startup, before HTTP. A key problem that is only suspicious (unknown or unsettable key) is a warning.
- Client creation and its retries never touch the file system.

**Non-Goals:**

- Value validation beyond sarama's `Config.Validate` (it still runs in `sarama.NewClient` / `NewSyncProducer`).
- Per-stanza tuning, or tuning keys in `config_app.yaml`.

## Decisions

### D1. A loader validates the YAML tree against `sarama.Config` before decoding

New file `internal/router/tuning.go`:

```
type Tuning struct {
    file     string             // for messages
    node     *yaml.Node         // document with app keys, the strategy key and unsettable keys removed
    strategy sarama.BalanceStrategy // consumer only; nil means default
    autoCommitInterval time.Duration // consumer only (OffsetsAutoCommitInterval)
}
func LoadConsumerTuning() (*Tuning, error)  // reads consumerConfigPath
func LoadProducerTuning() (*Tuning, error)  // reads producerConfigPath
func (t *Tuning) apply(cfg *sarama.Config) error
```

Loading: read the file (error `unable to locate configuration file %s: %w`), parse it into a `yaml.Node` (error `unable to parse configuration file %s: %w`), then walk the mapping against `reflect.TypeOf(sarama.Config{})`:

- The key name of a struct field is its lowercased name (the yaml.v3 rule). A key with no matching field is logged as `log.Warn` `unknown tuning key <dotted.path> in <file> is ignored` and removed from the node.
- Struct fields and pointers to structs: recurse into mapping values.
- Fields of kind interface or func, or slices/maps whose element kind is interface or func: logged as `tuning key <dotted.path> in <file> cannot be set from YAML and is ignored` and removed.
- Any other kind (scalars, durations, slices/maps of scalars): accepted without descending.
- Top-level app keys (`OffsetsAutoCommitInterval`, consumer file only) are read into the `Tuning` and removed, so they produce no warning.
- `consumer.group.rebalance.strategy` (consumer file only) is read (D2) and removed before the walker reaches it.

`apply` decodes the pruned node into `cfg` with `node.Decode`, wrapped in `recover` that turns a panic into an error naming the file. With the walker this is unreachable, but a defensive guard is cheap and protects against sarama adding new field kinds. `apply` then sets `GroupStrategies` when `strategy` is set and the auto-commit interval when it is positive.

Alternative rejected: `KnownFields(true)`. It reports only the first unknown key, as a Go type dump, and does not prevent the panic on interface fields.

Alternative rejected: decoding in a `recover` only. That stops the crash but still loses typos silently and gives operators no key name.

### D2. Strategy names map to sarama constructors

`range` → `NewBalanceStrategyRange()`, `roundrobin` → `NewBalanceStrategyRoundRobin()`, `sticky` → `NewBalanceStrategySticky()`, `cooperative-sticky` → `NewBalanceStrategyCooperativeSticky()`. Matching is case-insensitive after trimming. Any other value, or a non-scalar, makes `LoadConsumerTuning` return `config_consumer.yaml: consumer.group.rebalance.strategy %q is not supported (allowed: range, roundrobin, sticky, cooperative-sticky)`. With the key absent, `strategy` stays nil and the code default (`range`) applies.

The key in the file stays the sarama name of the deprecated field (`strategy`), which is what an operator reading sarama docs would try first. Only one strategy is advertised; changing it on a running group needs a full restart (README, proposal Non-goals).

### D3. Clients take the loaded tuning; code defaults stay before the file

`NewConsumerClient(conf, stanza, tuning *Tuning)` and `NewProducer(conf, tuning *Tuning)` keep their order: `sarama.NewConfig()`, version and client id, code defaults (strategy range, session 60s, heartbeat 10s, MaxProcessingTime 10s; producer compression ZSTD and hash partitioner), then `tuning.apply(cfg)`, then the forced settings (auto-commit enabled, TLS from `conf.TLSConf`). The file therefore overrides code defaults, as today, and never overrides the forced settings. No file I/O happens in client creation.

`createConsumerClientWithRetry` and `createProducerWithRetry`, and their seams, gain a `*router.Tuning` parameter. The 11 seam assignments in `internal/app` tests change mechanically.

Alternative rejected: a runtime field on `KafkaInfo` (like `TLSConf`). It would avoid the signature change, but `schemas` cannot reference `router.Tuning` without an import cycle, and a neutral package just for this type is not worth it.

### D4. Load at startup, only what is needed

In `Connect`, after `config.PrepareUpstreams` and before the transaction logger and the HTTP server:

- If any consumer stanza is active (non-empty `Topics`), `consumerTuning, err := router.LoadConsumerTuning()`. An error returns `fmt.Errorf("invalid Kafka consumer tuning: %w", err)`.
- If any producer is configured, `producerTuning, err := router.LoadProducerTuning()`, with the same pattern.

An inactive stanza still starts a goroutine today, but it never creates a client that reads records; it receives the consumer tuning when one was loaded, and nil otherwise (`apply` on a nil `*Tuning` is a no-op).

Warnings are logged once per process start instead of once per client attempt.

### D5. Tests

- `internal/router` `tuning_test.go`:
  - unknown keys (`net.maxopenrequest`, `MaxOpenRequests`) warn with the dotted path and keep the sarama default;
  - unsettable keys (`producer.partitioner: random`, `consumer.group.rebalance.groupstrategies: [sticky]`) warn and do not panic;
  - each strategy name maps to the matching sarama strategy (compare `Name()`), an unknown one errors with the allowed list, and an absent one leaves the default;
  - `OffsetsAutoCommitInterval` produces no warning and sets the interval;
  - a missing file and an unparsable file return errors naming the file;
  - `apply` on nil is a no-op;
  - the repository's own `configs/config_consumer.yaml` and `config_producer.yaml` load without warnings (guards the shipped files).
- Router client tests: move the missing/broken-file cases from `NewConsumerClient` / `NewProducer` to the loader. Keep the overlay test through `NewProducer(conf, tuning)`. Add `consumer.group.session.timeout: 20s` overriding the 60s default while heartbeat stays 10s, and the strategy reaching `cfg.Consumer.Group.Rebalance.GroupStrategies`.
- `internal/app`: `Connect` with a producer and a missing producer file (path seam) fails before the HTTP server starts; with no producers the missing file is not required; an unknown strategy fails startup.

## Risks / Trade-offs

- [A deployment with a missing or broken tuning file used to retry for five minutes and now fails at once] → Such a deployment could never create the client anyway; failing at startup with the file name is faster and clearer. Called out as BREAKING (operational) in the proposal.
- [The walker's view of yaml.v3 naming drifts from yaml.v3] → It follows one rule (lowercased field name, no tags in sarama), and the test that loads the shipped files plus the overlay tests would catch a mismatch. The `recover` guard in `apply` remains the last line.
- [New warnings appear for existing files with unused keys] → Intended. The shipped files are checked to produce none. Note: `producer.offsets.*` in `config_producer.yaml` does not exist in sarama's producer config and will now be reported. The shipped file is corrected in the same change (task 3.3).
- [Strategy change on a live group] → Documented. Mixed strategies fail the join with `InconsistentGroupProtocol` until every member runs the same strategy.

## Migration Plan

Operators who see new warnings fix or delete the keys named in them. Changing the strategy needs a full restart of every member of the group. Rollback means redeploying the previous build. Tuning files written for this version stay valid for the previous one, except for `consumer.group.rebalance.strategy`, which crashes the previous build.
