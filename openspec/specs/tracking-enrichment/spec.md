# tracking-enrichment Specification

## Purpose
Guarantees every message crossing the bridge carries a complete, consistent set of tracking identifiers and binary-safe header encodings, regardless of what arrived on the wire.

## Requirements

### Requirement: Tracking-header rules
This capability is the single owner of tracking-header behaviour for every entry point (Kafka consume, HTTP publish, HTTP response from an upstream). The system MUST apply a fixed rule list, in declared order, to populate these headers (wire names, lowercase):
- `x-request-id`: optional on input; a UUID v4 is generated if absent
- `x-request-time`: optional; defaults to the current UTC timestamp
- `x-trace-id`: optional; defaults to the resulting `x-request-id`

Later rules MAY observe earlier results. When an upstream HTTP response omits tracking headers, the originating request's `x-request-id` and `x-client-id` MUST be carried onto the response record.

#### Scenario: Inbound record missing Request-Id
- **WHEN** a record or request arrives without `x-request-id`
- **THEN** the system generates a UUID, sets `x-request-id`, derives `x-trace-id` from it, and stamps `x-request-time` with current UTC

#### Scenario: Inbound record already has Request-Id
- **WHEN** a record or request arrives with a non-empty `x-request-id`
- **THEN** that exact value is reused unchanged across all internal hops and surfaced in the eventual response topic

#### Scenario: Upstream omits tracking headers
- **WHEN** the upstream response carries none of the tracking headers
- **THEN** the response record carries the originating `x-request-id` and `x-client-id`

### Requirement: System identity propagation
The system MUST stamp the `Client-Id` header on every outbound forwarded record with the value sourced from the runtime `SYSTEM_ID` environment variable. A missing env var yields an empty `Client-Id` value rather than blocking processing.

#### Scenario: SYSTEM_ID present
- **WHEN** the operating environment exports `SYSTEM_ID`
- **THEN** every Kafka record leaving the system contains that exact string under the `x-client-id` header

#### Scenario: SYSTEM_ID absent
- **WHEN** `SYSTEM_ID` is not exported
- **THEN** records still flow, the `x-client-id` field is left empty, and a warning surfaces in logs identifying which identifier was defaulted

### Requirement: Binary-safe header encoding
Each Kafka record header value MUST be inspected for non-printable bytes. If any byte fails the printable-or-whitespace predicate, the system MUST base64-encode the raw value and store the encoded string instead of the original bytes. Printable values pass through verbatim.

#### Scenario: UTF-8 text header
- **WHEN** a header carries ordinary UTF-8 text
- **THEN** its value reaches downstream consumers unmodified

#### Scenario: Binary blob header
- **WHEN** a header value contains control characters or non-text bytes
- **THEN** the value reaching downstream is the base64 representation of the original bytes, and a debug log notes that the header was re-encoded

### Requirement: Custom prefix passthrough
Headers starting with the application-defined custom prefix on incoming HTTP traffic MUST have the prefix stripped and the remaining suffix used as the Kafka header key. Conversely, on inbound Kafka traffic, no special handling of these prefixes applies because they originate from external systems.

#### Scenario: HTTP→Kafka translate
- **WHEN** an HTTP publish request includes a header prefixed with the documented custom marker
- **THEN** the resulting Kafka record carries the same value under the unprefixed header name, in the HTTP-canonical form produced by the server (e.g. prefix + `tenant` yields key `Tenant`)

#### Scenario: Kafka→HTTP forward
- **WHEN** a Kafka record whose headers include arbitrary user keys flows toward an upstream HTTP service
- **THEN** the existing header keys and values reach the upstream service unchanged, without applying the custom-prefix rule

### Requirement: Trace continuity across hops
Within a single round-trip, the `Trace-Id` value used when emitting metric lines and audit entries MUST equal the `Request-Id` driving the request. Operators correlating logs MUST see identical trace identifiers end-to-end.

#### Scenario: Round-trip correlation
- **WHEN** a record enters consume, traverses upstream forwarding, then exits via produce
- **THEN** all metric-file lines and audit entries associated with that request share a single common `Request-Id`-shaped identifier
