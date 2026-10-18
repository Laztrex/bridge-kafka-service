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
When at least one `producer:` entry is configured, the system MUST create exactly one Sarama sync producer and expose it under every configured producer topic; the topic list acts as an allow-list for publishing. On shutdown the producer MUST be closed exactly once. If the producer cannot be created within the retry window while any active consumer stanza has a `response_topic`, the process MUST exit non-zero before any consumer reads a record. Startup ordering and HTTP behaviour while the producer is unavailable are specified by `health-probe` and `control-api`.

#### Scenario: Three producer topics
- **WHEN** three producer topics are configured
- **THEN** one producer is created, all three topics resolve to it, and it is closed once on exit

#### Scenario: Producer creation fails permanently
- **WHEN** the producer cannot be created after the retry window and no active consumer stanza has a `response_topic`
- **THEN** the error is logged, no topic is registered, and consumption still runs

#### Scenario: Producer creation fails permanently while responses are expected
- **WHEN** the producer cannot be created after the retry window and at least one active consumer stanza has a `response_topic`
- **THEN** the error is logged, no consumer reads a record, and the process exits with a non-zero code so the supervisor restarts it

#### Scenario: Consumers wait for the producer
- **WHEN** a consumer client is ready before the producer and a consumer stanza has a `response_topic`
- **THEN** that consumer does not read records until the producer is connected

### Requirement: Kafka version supports consumer groups

When at least one consumer stanza is active, startup MUST fail if `kafka.version` parses to a Kafka version below 0.10.2, the minimum required by consumer groups; the error MUST name the configured value and the minimum. A value that cannot be parsed MUST keep the existing fallback (2.7.0) and MUST NOT stop startup. A configuration without active consumer stanzas MUST NOT be checked.

#### Scenario: Version too old for consumer groups

- **WHEN** `kafka.version` is `0.10.1.0` and a consumer stanza has topics
- **THEN** startup fails before the HTTP server starts, with an error naming `0.10.1.0` and the minimum `0.10.2`

#### Scenario: Minimum version accepted

- **WHEN** `kafka.version` is `0.10.2.0` and a consumer stanza has topics
- **THEN** startup proceeds

#### Scenario: Unparseable version keeps the fallback

- **WHEN** `kafka.version` cannot be parsed
- **THEN** startup proceeds and consumer clients use version 2.7.0

#### Scenario: Producer-only configuration

- **WHEN** `kafka.version` is `0.10.1.0` and no consumer stanza has topics
- **THEN** startup proceeds

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

## Technical Notes

Keys of the application config (checked against this spec by `make docs-check`):

```yaml
logLevel:
http.addr:
pprof.addr:
audit.grpc_endpoint:
timeouts.shutdown:
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
