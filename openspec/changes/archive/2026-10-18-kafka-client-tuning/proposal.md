# Proposal

## Why

The Kafka client tuning files (`configs/config_consumer.yaml`, `configs/config_producer.yaml`) are decoded straight into `sarama.Config` with a lenient YAML decoder, which makes them unsafe and incomplete:

- **A plausible key crashes the process.** `consumer.group.rebalance.strategy: sticky` maps to sarama's deprecated interface field `Rebalance.Strategy`. The decoder panics trying to assign a string to it, and the panic kills the process from the client-creation goroutine with a stack trace. The same happens for every other interface- or function-typed field (`groupstrategies`, `producer.partitioner`, `net.sasl.tokenprovider`, ...).
- **The rebalance strategy cannot be chosen.** It is hard-coded to `range` (`internal/router/consumer.go:297`, with a TODO). `range` reshuffles more partitions on every rebalance, which under at-least-once means more redelivered records.
- **Typos are silently ignored.** `maxopenrequest: 1` or `MaxOpenRequests: 1` (keys must be the exact lowercased field name) leaves the sarama default in place without a word. The operator believes a setting is applied when it is not.
- **A broken file is found late.** The file is read and parsed on every client-creation attempt, so a missing or malformed file shows up only after the five-minute retry window, as a generic client-creation failure.
- **Overridable defaults are undocumented.** The code sets session timeout 60s, heartbeat 10s and max processing time 10s before applying the file, so the file can already override them, but nothing says so.

## What Changes

- **Rebalance strategy in the consumer tuning file:** `consumer.group.rebalance.strategy` accepts `range` (default, unchanged), `roundrobin`, `sticky` and `cooperative-sticky`. Any other value stops startup with an error naming the key and the allowed values. README documents that changing the strategy of a running group needs a full restart of every member, because members with different strategies cannot join one group (`InconsistentGroupProtocol`).
- **No more panics:** keys whose sarama field cannot be set from YAML (interface or function types, other than the strategy key above) are ignored with a warning naming the key, instead of crashing the process.
- **Unknown keys are warned about:** each key in either tuning file that matches no sarama setting (and is not one of the app's own keys, such as `OffsetsAutoCommitInterval`) is logged as a warning at startup with its full dotted path; startup continues. This matches how unknown keys of the application config are handled.
- **BREAKING (operational): tuning files are loaded once, at startup.** Each file is read and validated before the HTTP server starts, and only when its client is needed (consumer file with active consumers, producer file with producers). A missing or unparsable file stops startup immediately with an error naming the file, instead of being retried for five minutes. Client creation reuses the validated settings.
- **Documented defaults:** README lists the consumer group timing defaults the bridge sets (session timeout 60s, heartbeat 10s, max processing time 10s) and that the tuning file overrides them.

## Non-goals

- Per-stanza tuning: one consumer file and one producer file stay global for all stanzas.
- Changing the default strategy, or advertising several strategies at once for in-place migration.
- Making unknown keys fatal.
- Validating values beyond what sarama's own `Config.Validate` already checks at client creation.
- Moving tuning into `config_app.yaml`, or changing the file paths.
- Hot reload of tuning files.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `app-configuration`: "Separate Kafka client tuning files" changes from "a missing or unparsable file fails client creation and is retried" to "files are loaded and validated once at startup, a missing or unparsable file stops startup". New requirements: "Configurable rebalance strategy", "Tuning keys are checked at startup" (unknown keys warned, unsettable keys ignored with a warning, never a crash), and "Consumer group timing defaults".

## Impact

- Code:
  - `internal/router/consumer.go` and `producer.go`: a tuning loader that validates keys against `sarama.Config`, maps the strategy, and decodes without panicking; `NewConsumerClient` and `NewProducer` use the loaded settings;
  - `internal/schemas/kafka.go`: runtime fields for the loaded tuning, like `TLSConf`;
  - `internal/app/connection.go`: load at startup;
  - tests next to each.
- Behaviour visible to operators: a broken or missing tuning file now stops startup at once; typos and unsupported keys appear as warnings; the rebalance strategy is configurable.
- Docs: `README.md` (configuration section).
- No new dependencies and no new keys in `config_app.yaml`.
