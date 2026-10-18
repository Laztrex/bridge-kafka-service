# Spec Delta

## ADDED Requirements

### Requirement: gRPC target validated at startup

For an active stanza (one with topics) with `service.protocol: grpc`, startup MUST fail with an error naming the stanza unless `service.name` contains no whitespace or control characters and `service.port` is a decimal number from 1 to 65535. The check MUST NOT resolve or contact the host: an unreachable or unresolvable service still does not block startup (see "Connection lifecycle"). Stanzas without topics MUST NOT be checked, as for the other per-stanza startup checks of active consumers.

#### Scenario: Invalid port

- **WHEN** a `grpc` stanza with topics has `service.port` set to `abc`, `0`, `70000` or empty
- **THEN** startup fails with an error naming the stanza and the port

#### Scenario: Valid target with unreachable host

- **WHEN** a `grpc` stanza has `service.name: model-svc` and `service.port: 50051` and the host does not resolve
- **THEN** startup succeeds and each record fails as a transport failure until the service is reachable

#### Scenario: HTTP stanza not checked

- **WHEN** an `http` stanza has `service.port: abc`
- **THEN** this check does not stop startup
