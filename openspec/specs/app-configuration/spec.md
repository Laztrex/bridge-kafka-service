# app-configuration Specification

## Purpose
Defines how the service locates and layers its configuration (command-line flag, environment, YAML files) and which diagnostic listeners are exposed, so operators can predict which settings win and what is reachable on which port.

## Requirements

### Requirement: Backward-compatible config format
The loader MUST accept every config file the former viper-based loader accepted and MUST produce the same values for it: keys are case-insensitive; a nested key MAY be written nested or dotted; a non-empty environment variable named after a top-level key (upper case, dots as underscores) overrides it; strings split into lists on commas where a list is expected and scalars convert weakly (`port: 8080` is `"8080"`). Unknown keys MUST NOT stop startup: each MUST be logged as a warning. Loading MUST NOT use global state and MUST return errors instead of exiting.

#### Scenario: Former files load unchanged
- **WHEN** the stand config, the examples and the edge-case fixtures in `internal/config/testdata/compat` are loaded with their recorded environment
- **THEN** every value equals the snapshot recorded from the former loader

#### Scenario: Unknown key
- **WHEN** the file contains a key the loader does not know
- **THEN** the service starts and logs `unknown config key "<key>" is ignored`

### Requirement: Configurable shutdown timeout
`timeouts.shutdown` (a Go duration, default `10s`) MUST bound both the HTTP server drain and the wait for active upstream calls on termination. A non-positive or malformed value MUST stop startup.

#### Scenario: Custom timeout
- **WHEN** `timeouts.shutdown: 25s` is configured and SIGTERM arrives during a long upstream call
- **THEN** the call gets up to 25 seconds to finish before it is aborted without settling its offset

### Requirement: Application config file resolution
The system MUST resolve the application config file (`kafka`, `consumer`, `producer`, `audit`, `http`, `logLevel` keys) in this order, taking the first that is set: the `--config` / `-c` command-line flag, the `CONFIG_PATH` environment variable, `CONFIG_PATH` from the `.env` file in the working directory, then the built-in default `configs/config_app.yaml`. The file MUST always be loaded, whether or not an env file (`.env`) exists.

#### Scenario: Flag wins
- **WHEN** the binary starts with `--config /etc/bridge/app.yaml` and `CONFIG_PATH` points elsewhere
- **THEN** `/etc/bridge/app.yaml` is the file read

#### Scenario: Environment fallback
- **WHEN** no flag is given and `CONFIG_PATH` is set
- **THEN** the file named by `CONFIG_PATH` is read

#### Scenario: Built-in default
- **WHEN** neither the flag nor `CONFIG_PATH` is set
- **THEN** `configs/config_app.yaml` is read and a warning states that the default is used

#### Scenario: Env file present does not shadow the config
- **WHEN** a `.env` file exists in the working directory
- **THEN** only its `CONFIG_PATH` is used and the application config file is still loaded

#### Scenario: Unreadable config aborts startup
- **WHEN** the resolved config file cannot be read or parsed
- **THEN** the process logs the error and exits non-zero

### Requirement: Logging and config initialised on run only
Logging and configuration MUST be initialised after command-line parsing, only when the service actually runs, so `--help` and `--version` do not require config files or create log files.

#### Scenario: Version flag
- **WHEN** the binary is started with `--version`
- **THEN** the version is printed and no configuration file is read

### Requirement: Separate Kafka client tuning files

Sarama consumer and producer tunables MUST be read from `configs/config_consumer.yaml` and `configs/config_producer.yaml` respectively and overlaid on Sarama defaults. Each file MUST be loaded and validated once at startup, before the HTTP server starts, and only when its client is needed: the consumer file when at least one consumer stanza is active, the producer file when at least one producer is configured. A missing or unparsable file that is needed MUST stop startup with an error naming the file, rather than silently using defaults or being retried. Client creation, including its retries, MUST use the settings loaded at startup.

#### Scenario: Producer file missing

- **WHEN** a producer is configured and `configs/config_producer.yaml` is absent at startup
- **THEN** startup fails before the HTTP server starts, with an error naming the file

#### Scenario: Consumer file unparsable

- **WHEN** a consumer stanza is active and `configs/config_consumer.yaml` is not valid YAML
- **THEN** startup fails before the HTTP server starts, with an error naming the file

#### Scenario: File of an unused client

- **WHEN** no producer is configured and `configs/config_producer.yaml` is absent
- **THEN** startup proceeds

### Requirement: Offset auto-commit interval default
`OffsetsAutoCommitInterval` in `config_consumer.yaml` MUST override Sarama's auto-commit interval only when set to a positive duration. When omitted or zero, the Sarama default MUST be kept (a zero interval makes client creation fail validation). Offset auto-commit itself MUST always be enabled: `consumer.offsets.autocommit.enable` in the tuning file MUST NOT be able to turn it off, because offset settlement relies on it.

#### Scenario: Interval omitted
- **WHEN** `config_consumer.yaml` does not define `OffsetsAutoCommitInterval`
- **THEN** the consumer client is created with the Sarama default interval

#### Scenario: Interval set
- **WHEN** `OffsetsAutoCommitInterval: 2s` is configured
- **THEN** the consumer client commits offsets every two seconds

#### Scenario: Auto-commit disabled in the tuning file
- **WHEN** `config_consumer.yaml` sets `consumer.offsets.autocommit.enable: false`
- **THEN** the consumer client is created with auto-commit enabled

### Requirement: Diagnostic listener isolation
Go runtime profiling (pprof) MUST NOT be reachable on the public HTTP listener (`http.addr`). It MUST be served only when `pprof.addr` is configured, on that separate address; a `pprof.addr` that is not a loopback address MUST be logged as a warning.

#### Scenario: Default deployment
- **WHEN** `pprof.addr` is not configured
- **THEN** requests to `/debug/pprof/` on `http.addr` return 404 and no pprof listener is started

#### Scenario: Profiling enabled
- **WHEN** `pprof.addr: 127.0.0.1:6060` is configured
- **THEN** pprof handlers answer on that address only

### Requirement: Fatal startup preconditions
If the transaction/metric log file cannot be opened, the service MUST log the error and stop starting instead of continuing without a transaction logger.

#### Scenario: Unwritable metric path
- **WHEN** the metric log path is not writable
- **THEN** startup aborts with an error and no consumers are started

### Requirement: One producer shared by all producer topics
When at least one `producer:` entry is configured, the system MUST create exactly one Sarama sync producer and expose it under every configured producer topic; the topic list acts as an allow-list for publishing. On shutdown the producer MUST be closed exactly once. If the producer cannot be created within the retry window, the process MUST exit non-zero before any consumer reads a record, whether or not an active consumer stanza has a `response_topic`, so the supervisor restarts it instead of leaving it running and never ready. Startup ordering and HTTP behaviour while the producer is unavailable are specified by `health-probe` and `control-api`.

#### Scenario: Three producer topics
- **WHEN** three producer topics are configured
- **THEN** one producer is created, all three topics resolve to it, and it is closed once on exit

#### Scenario: Producer creation fails permanently
- **WHEN** the producer cannot be created after the retry window and no active consumer stanza has a `response_topic`
- **THEN** the error is logged, no consumer reads a record, and the process exits with a non-zero code so the supervisor restarts it

#### Scenario: Producer creation fails permanently while responses are expected
- **WHEN** the producer cannot be created after the retry window and at least one active consumer stanza has a `response_topic`
- **THEN** the error is logged, no consumer reads a record, and the process exits with a non-zero code so the supervisor restarts it

#### Scenario: Consumers wait for the producer
- **WHEN** a consumer client is ready before the producer and a consumer stanza has a `response_topic`
- **THEN** that consumer does not read records until the producer is connected

#### Scenario: No producer configured
- **WHEN** no `producer:` entry is configured
- **THEN** no producer is created, consumption runs, and the publish endpoint answers 503 as specified by `control-api`

### Requirement: Configurable rebalance strategy

`consumer.group.rebalance.strategy` in the consumer tuning file MUST select the consumer group rebalance strategy for every consumer: `range`, `roundrobin`, `sticky` or `cooperative-sticky`. When the key is absent, the strategy MUST be `range`. Any other value MUST stop startup with an error naming the key, the value and the allowed values.

#### Scenario: Strategy omitted

- **WHEN** `config_consumer.yaml` does not set `consumer.group.rebalance.strategy`
- **THEN** every consumer joins its group with the `range` strategy

#### Scenario: Sticky selected

- **WHEN** `config_consumer.yaml` sets `consumer.group.rebalance.strategy: sticky`
- **THEN** every consumer joins its group with the `sticky` strategy and the process does not crash

#### Scenario: Unknown strategy

- **WHEN** `config_consumer.yaml` sets `consumer.group.rebalance.strategy: fair`
- **THEN** startup fails with an error naming the key, `fair` and the allowed values

### Requirement: Tuning keys are checked at startup

When a tuning file is loaded, every key that matches no Sarama setting and is not one of the application's own keys (`OffsetsAutoCommitInterval`) MUST be logged as a warning naming the file and the key's full dotted path; such keys MUST NOT stop startup. A key whose Sarama setting cannot be expressed in YAML (an interface- or function-typed setting, other than `consumer.group.rebalance.strategy`) MUST be ignored with a warning naming the file and the key, and MUST NOT crash the process or stop startup.

#### Scenario: Misspelled key

- **WHEN** `config_producer.yaml` contains `net.maxopenrequest: 1`
- **THEN** a warning naming `config_producer.yaml` and `net.maxopenrequest` is logged, startup proceeds and the Sarama default for `net.maxopenrequests` is kept

#### Scenario: Key that cannot be set from YAML

- **WHEN** `config_producer.yaml` contains `producer.partitioner: random`
- **THEN** a warning naming the key is logged, the key is ignored, and the process keeps running

#### Scenario: Known keys only

- **WHEN** a tuning file contains only valid Sarama keys and `OffsetsAutoCommitInterval`
- **THEN** no tuning-key warning is logged

### Requirement: Consumer group timing defaults

Unless the consumer tuning file overrides them, every consumer MUST use a group session timeout of 60 seconds, a heartbeat interval of 10 seconds and a maximum processing time of 10 seconds. Values set in the consumer tuning file (`consumer.group.session.timeout`, `consumer.group.heartbeat.interval`, `consumer.maxprocessingtime`) MUST take precedence over these defaults.

#### Scenario: Defaults applied

- **WHEN** the consumer tuning file sets none of these keys
- **THEN** consumers use a 60s session timeout, a 10s heartbeat interval and a 10s maximum processing time

#### Scenario: Session timeout overridden

- **WHEN** the consumer tuning file sets `consumer.group.session.timeout: 20s`
- **THEN** consumers use a 20s session timeout and keep the other defaults

### Requirement: Kafka version supports record headers

When at least one producer is configured, startup MUST fail if `kafka.version` parses to a Kafka version below 0.11, the minimum for record headers, which every published record carries; the error MUST name the configured value and the minimum. A non-empty value that cannot be parsed is handled by "Kafka version is valid and supports consumer groups": it stops startup.

#### Scenario: Version too old for headers

- **WHEN** a producer is configured and `kafka.version` is `0.10.2.0`
- **THEN** startup fails before the HTTP server starts, with an error naming `0.10.2.0` and the minimum `0.11`

#### Scenario: Consumer-only deployment

- **WHEN** no producer is configured and `kafka.version` is `0.10.2.0`
- **THEN** this check does not stop startup

### Requirement: All configuration errors reported together

After the configuration file is loaded, startup MUST run every configuration check before stopping: the Kafka setup, the upstream settings of each stanza, the topology checks and the Kafka tuning files. If any check fails, startup MUST fail with one error listing every problem found, each naming its stanza (position and topics) where there is one, and the rule it breaks. Warnings MUST be logged and MUST NOT stop startup.

#### Scenario: Three mistakes in one file

- **WHEN** one stanza has `service.port: abc`, another has `on_failure: park`, and `kafka.version` is `2.x`
- **THEN** startup fails once, and the error names all three problems with their stanzas

#### Scenario: Load-time parse error

- **WHEN** the configuration file is not valid YAML
- **THEN** startup fails with that parse error alone, as before

### Requirement: Configuration check mode

The binary MUST accept `--check-config`. In that mode it MUST resolve and load the configuration as for a normal start, run exactly the checks startup runs, print each warning and each error, and exit with code 0 if no check failed and 1 otherwise. It MUST NOT connect to Kafka or to any upstream, and MUST NOT create log, metric or audit files.

#### Scenario: Valid configuration

- **WHEN** `bridge-kafka-service --check-config --config app.yaml` runs against a valid file with one unknown key
- **THEN** the unknown-key warning is printed, a line states that the configuration is valid, and the exit code is 0

#### Scenario: Invalid configuration

- **WHEN** the same command runs against a file with a response-topic cycle
- **THEN** the error naming the cycle is printed and the exit code is 1

#### Scenario: No side effects

- **WHEN** `--check-config` runs with no broker reachable and the log and metric paths pointing at an empty directory
- **THEN** the result depends only on the file, and the directory stays empty

### Requirement: No response-topic cycles

For the active stanzas, each input topic leads to the stanza's `response_topic`, when one is set. Startup MUST fail if these links form a cycle, naming the topics and stanzas on it. A stanza whose `response_topic` is one of its own input topics is a cycle. A chain without a cycle MUST be accepted.

#### Scenario: Self loop

- **WHEN** an active stanza reads `orders` and has `response_topic: orders`
- **THEN** startup fails with an error naming the stanza and `orders`

#### Scenario: Loop through two stanzas

- **WHEN** stanza A reads `a` and responds to `b`, and stanza B reads `b` and responds to `a`
- **THEN** startup fails with an error naming both stanzas and the topics `a` and `b`

#### Scenario: Chain

- **WHEN** stanza A reads `a` and responds to `b`, and stanza B reads `b` and responds to `c`
- **THEN** startup proceeds

#### Scenario: Disabled stanza ignored

- **WHEN** a stanza without topics has `response_topic: a` and an active stanza reads `a`
- **THEN** this check does not stop startup

### Requirement: Consumer groups do not share topics

Startup MUST fail if two active stanzas have the same `group` and at least one topic in common, naming both stanzas, the group and the shared topics. Two active stanzas with the same `group` and no topic in common MUST be accepted with a warning that they rebalance together. The same topic in different groups MUST be accepted without a warning.

#### Scenario: Shared group and topic

- **WHEN** two active stanzas both have `group: bridge` and both read `orders`
- **THEN** startup fails with an error naming both stanzas, `bridge` and `orders`

#### Scenario: Shared group, disjoint topics

- **WHEN** two active stanzas have `group: bridge` and read `orders` and `payments`
- **THEN** startup proceeds and a warning names both stanzas and the group

#### Scenario: Fan-out across groups

- **WHEN** two active stanzas read `orders` with groups `g1` and `g2`
- **THEN** startup proceeds without a warning

### Requirement: Configuration entries are not empty

A `consumer` or `producer` list entry that is empty (for example `- ~`) MUST stop startup with an error naming its list and position. It MUST NOT crash the process, and an empty `consumer` entry MUST NOT be treated as a disabled stanza; a stanza is disabled only by having no topics.

#### Scenario: Empty entry

- **WHEN** the second `consumer` entry is `~`
- **THEN** startup fails with an error naming consumer #2, and the process does not panic

#### Scenario: Empty producer entry

- **WHEN** the second `producer` entry is `~`
- **THEN** startup fails with an error naming producer #2, and the process does not panic

#### Scenario: Stanza without topics

- **WHEN** a `consumer` entry has a group and a service but no topics
- **THEN** it is disabled as before and this check does not stop startup

### Requirement: Kafka version is valid and supports consumer groups

When at least one consumer stanza is active, startup MUST fail if `kafka.version` parses to a Kafka version below 0.10.2, the minimum required by consumer groups; the error MUST name the configured value and the minimum. An empty `kafka.version` MUST keep the default (2.7.0). A non-empty value that cannot be parsed MUST stop startup with an error naming the value, whether or not consumers or producers are configured. A configuration without active consumer stanzas MUST NOT be checked by the minimum of this requirement (producers are checked by "Kafka version supports record headers").

#### Scenario: Version too old for consumer groups

- **WHEN** `kafka.version` is `0.10.1.0` and a consumer stanza has topics
- **THEN** startup fails before the HTTP server starts, with an error naming `0.10.1.0` and the minimum `0.10.2`

#### Scenario: Minimum version accepted

- **WHEN** `kafka.version` is `0.10.2.0`, a consumer stanza has topics and no producer is configured
- **THEN** startup proceeds

#### Scenario: Empty version keeps the default

- **WHEN** `kafka.version` is not set
- **THEN** startup proceeds and Kafka clients use version 2.7.0

#### Scenario: Unparseable version stops startup

- **WHEN** `kafka.version` is `2.x`
- **THEN** startup fails before the HTTP server starts, with an error naming `2.x`

#### Scenario: Producer-only configuration

- **WHEN** `kafka.version` is `0.10.1.0` and no consumer stanza has topics
- **THEN** the consumer-group check does not stop startup, and the record-header check applies instead

### Requirement: Backup retention settings

The configuration MUST accept `retention.max_backups` and `retention.max_age_days`, whole numbers that apply to the backups of both the metric file and the audit file. `0`, the default, MUST mean no limit. A negative or non-numeric value MUST stop startup with an error naming the key and the value. When either limit is set, startup MUST log one line naming both limits and stating that older audit backups will be deleted.

#### Scenario: Defaults

- **WHEN** neither key is set
- **THEN** every backup is kept and no retention line is logged

#### Scenario: Limits set

- **WHEN** `retention.max_backups: 10` and `retention.max_age_days: 30` are set
- **THEN** startup logs one line naming both limits, and backups beyond either limit are deleted when the file is first written and at every rotation

#### Scenario: Invalid value

- **WHEN** `retention.max_age_days` is `-1` or `month`
- **THEN** startup fails with an error naming `retention.max_age_days` and the value

## Technical Notes

Keys of the application config (checked against this spec by `make docs-check`):

```yaml
logLevel:
http.addr:
pprof.addr:
audit.grpc_endpoint:
timeouts.shutdown:
retention.max_backups:
retention.max_age_days:
kafka:
  brokers:
  version:
kafka.tls.client.cert.path:
kafka.tls.client.key.path:
kafka.tls.ca.path:
kafka.tls.skip.insecure.verify:
consumer:
  - topics:
    group:
    client_id:
    response_topic:
    on_failure:
    service:
      name:
      port:
      path:
      protocol:
      max_conn:
      timeout:
      tls:
        mode:
        ca_path:
        cert_path:
        key_path:
        server_name:
        skip_verify:
producer:
  - topic:
SYSTEM_ID:
METRIC_PATH:
METRIC_FILE_NAME:
```
