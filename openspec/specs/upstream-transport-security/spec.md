# upstream-transport-security Specification

## Purpose
Lets operators encrypt (and optionally mutually authenticate) the connection to upstream services per consumer stanza, identically for HTTP and gRPC, and guarantees that a requested level of security is never silently weakened.

## Requirements

### Requirement: Encryption mode per consumer stanza
Each consumer stanza MUST accept `service.tls.mode` with values `none` (no encryption, the default when omitted), `tls` (encrypted, server authenticated) and `mtls` (encrypted, client also authenticated). The mode applies to the stanza's upstream whatever its protocol (`http` uses `https://`, `grpc` uses TLS transport credentials). An unknown mode MUST abort startup.

#### Scenario: Default is plaintext
- **WHEN** a stanza has no `service.tls` section
- **THEN** the upstream connection is unencrypted exactly as before

#### Scenario: TLS over HTTP
- **WHEN** a stanza has `service.protocol: http` and `service.tls.mode: tls`
- **THEN** requests go to `https://{name}:{port}{path}` and the server certificate is verified against the configured CA (or the system roots when `ca_path` is empty)

#### Scenario: TLS over gRPC
- **WHEN** a stanza has `service.protocol: grpc` and `service.tls.mode: tls`
- **THEN** the gRPC connection uses TLS with the same verification rules

### Requirement: TLS material and options
`service.tls` MUST support `ca_path`, `cert_path`, `key_path`, `server_name` (overrides the name verified in the server certificate) and `skip_verify` (default false). Mode `mtls` MUST require both `cert_path` and `key_path`. `skip_verify: true` disables server certificate verification and MUST be logged as a warning at startup.

#### Scenario: Mutual TLS
- **WHEN** mode is `mtls` with valid `cert_path` and `key_path`
- **THEN** the client presents that certificate to the upstream

#### Scenario: Incomplete mutual TLS
- **WHEN** mode is `mtls` and `key_path` is missing
- **THEN** startup fails with an error naming the stanza and the missing key

#### Scenario: Verification bypass is visible
- **WHEN** `skip_verify: true` is configured
- **THEN** verification is disabled and a warning naming the stanza is logged

### Requirement: Fail closed
If TLS is requested (`tls` or `mtls`) and any referenced file is unreadable or malformed, startup MUST fail with an error naming the stanza and the file. The system MUST NOT fall back to a plaintext or unverified connection. (This differs deliberately from the Kafka broker behaviour in `tls-kafka-broker`, which degrades for development convenience.)

#### Scenario: Missing CA file
- **WHEN** mode is `tls` and `ca_path` points to a missing file
- **THEN** the process exits with an error and no pipeline starts

#### Scenario: Handshake failure at runtime
- **WHEN** the upstream presents a certificate that fails verification
- **THEN** the call is a transport failure (recorded, nothing published, offset settled) and the connection is never downgraded
