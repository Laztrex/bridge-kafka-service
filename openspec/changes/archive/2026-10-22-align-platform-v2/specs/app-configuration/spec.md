## MODIFIED Requirements

### Requirement: Backward-compatible config format
The loader MUST accept every config file the former viper-based loader accepted that contains only known keys, and MUST produce the same values for it: keys are case-insensitive; a nested key MAY be written nested or dotted; a non-empty environment variable named after a top-level key (upper case, dots as underscores) overrides it; strings split into lists on commas where a list is expected and scalars convert weakly (`port: 8080` is `"8080"`). An unknown key MUST stop startup with an error naming it, reported together with every other configuration error. Loading MUST NOT use global state and MUST return errors instead of exiting.

#### Scenario: Former files load unchanged
- **WHEN** the stand config, the examples and the edge-case fixtures in `internal/config/testdata/compat` without unknown keys are loaded with their recorded environment
- **THEN** every value equals the snapshot recorded from the former loader

#### Scenario: Unknown key
- **WHEN** the file contains the keys `kafka.brokrs` and `timeouts.shutdwon`, which the loader does not know
- **THEN** startup fails with `unknown config key "kafka.brokrs"` and `unknown config key "timeouts.shutdwon"`, and no listener or Kafka client is started

### Requirement: Application config file resolution
The system MUST resolve the application config file (`kafka`, `consumer`, `producer`, `audit`, `health`, `log` keys) in this order, taking the first that is set: the `--config` / `-c` command-line flag, the `CONFIG_PATH` environment variable, `CONFIG_PATH` from the `.env` file in the working directory, then the built-in default `configs/config_app.yaml`. The file MUST always be loaded, whether or not an env file (`.env`) exists.

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

### Requirement: Diagnostic listener isolation
Go runtime profiling (pprof) MUST NOT be reachable on the service HTTP listener (`health.addr`). It MUST be served only when `debug.addr` is configured, on that separate address; a `debug.addr` that is not a loopback address MUST be logged as a warning.

#### Scenario: Default deployment
- **WHEN** `debug.addr` is not configured
- **THEN** requests to `/debug/pprof/` on `health.addr` return 404 and no profiling listener is started

#### Scenario: Profiling enabled
- **WHEN** `debug.addr: 127.0.0.1:6060` is configured
- **THEN** pprof handlers answer on that address only

## ADDED Requirements

### Requirement: Deprecated key aliases
The keys `http.addr`, `pprof.addr` and `logLevel` MUST be accepted as deprecated aliases of `health.addr`, `debug.addr` and `log.level`, including their environment overrides. An alias used alone MUST take effect and MUST be logged once as deprecated, naming the key to use. A key and its alias set together MUST fail validation, naming both. `log.level` MUST default to `info`.

#### Scenario: Deployed file with the old key
- **WHEN** the file sets `http.addr: 0.0.0.0:9000` and no `health.addr`
- **THEN** the service HTTP server listens on `0.0.0.0:9000` and startup logs `http.addr is deprecated, use health.addr`

#### Scenario: Key and alias together
- **WHEN** the file sets both `logLevel: debug` and `log.level: info`
- **THEN** startup fails with an error naming `logLevel` and `log.level`

#### Scenario: Level not set
- **WHEN** neither `log.level` nor `logLevel` is set
- **THEN** the log level is `info`
