# tls-kafka-broker Specification

## Purpose
Negotiates a mutually authenticated TLS session with configured Kafka brokers, optionally bypassing certificate verification, and falls back safely when certificate material is unavailable so the process can boot in development.

## Requirements

### Requirement: Client mTLS when materials are present
When both a client certificate and key are readable from their configured paths and a CA bundle is loaded successfully, the system MUST establish a TLS session against Kafka brokers presenting the client's certificate chain and validating broker identity against the supplied CA roots.

#### Scenario: Boot with full PKI
- **WHEN** startup finds valid PEM-encoded client cert, key, and CA files
- **THEN** Sarama is initialised with those credentials, broker hostnames are validated against the CA, and the resulting clients attach to the cluster over TLS

### Requirement: Optional certificate verification bypass
The system MUST honour a configuration switch that disables strict verification of the broker certificate chain. When enabled, hostname mismatches and untrusted CAs do not abort the handshake.

#### Scenario: Verification disabled by config
- **WHEN** the configuration explicitly sets the skip-verification flag to true
- **THEN** the Sarama TLS configuration includes `InsecureSkipVerify=true`

#### Scenario: Verification left at default
- **WHEN** the configuration omits the skip-verification flag
- **THEN** standard verification applies and untrusted brokers are rejected

### Requirement: Configuration precedence for certificate paths
The system MUST resolve certificate paths using this order, taking the first non-empty value: explicit configuration value, environment-derived override, built-in default path under `./certs/`.

#### Scenario: Config wins over default
- **WHEN** configuration supplies a custom certificate path and the default directory also contains certificates
- **THEN** the configured path is read

#### Scenario: Falls back to default
- **WHEN** configuration leaves the path empty
- **THEN** the built-in default location under `./certs/` is consulted

### Requirement: Safe degradation when material missing
If any required certificate file is unreadable, malformed, or fails to parse, the system MUST emit a clearly worded warning and continue operating with an insecure connection rather than refusing to start. This permits developer workflows where certificates are absent.

#### Scenario: Missing cert file
- **WHEN** the configured client certificate path does not exist
- **THEN** a warning is logged describing which file was missing and the system proceeds with an insecure TLS configuration

#### Scenario: Malformed PEM
- **WHEN** a certificate file exists but cannot be parsed
- **THEN** the same warning+continue behaviour fires and the system keeps running

### Requirement: Shared TLS material, per-client enablement
Consumer and producer clients MUST be configured from the same TLS configuration object built once at startup (a `*tls.Config` is safe for concurrent use and is not mutated after construction). Whether a client uses TLS is decided independently by the `net.tls.enable` flag in its own YAML file (`config_consumer.yaml`, `config_producer.yaml`).

#### Scenario: Both roles reuse the loaded certificates
- **WHEN** the application boots with valid certificates and TLS enabled for both roles
- **THEN** consumer and producer Sarama clients are both configured with the TLS configuration built from those certificates

#### Scenario: TLS enable flag is per role
- **WHEN** `net.tls.enable` is false in the producer YAML but true in the consumer YAML
- **THEN** only the consumer negotiates TLS, even though certificate paths were resolved for both
