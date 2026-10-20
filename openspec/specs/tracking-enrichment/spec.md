# tracking-enrichment Specification

## Purpose
Guarantees every message crossing the bridge carries a complete, consistent set of tracking identifiers and binary-safe header encodings, regardless of what arrived on the wire.

## Requirements

### Requirement: Tracking-header rules
This capability is the single owner of tracking-header behaviour for every entry point (Kafka consume, HTTP publish, HTTP response from an upstream). The system MUST apply a fixed rule list, in declared order, to populate these headers (wire names, lowercase):
- `x-request-id`: optional on input; a UUID v4 is generated if absent
- `x-request-time`: optional; defaults to the current UTC timestamp
- `x-trace-id`: optional; defaults to the resulting `x-request-id`

Later rules MAY observe earlier results. When an upstream HTTP response omits tracking headers, the originating request's `x-request-id` and `x-client-id` MUST be carried onto the response record. A header listed under "Required headers" is not optional: it gets no default at the entry points that requirement covers, and its absence is handled there. A request id is still generated for logs, metrics and audit when the input has none.

#### Scenario: Inbound record missing Request-Id
- **WHEN** a record or request arrives without `x-request-id`
- **THEN** the system generates a UUID, sets `x-request-id`, derives `x-trace-id` from it, and stamps `x-request-time` with current UTC

#### Scenario: Inbound record already has Request-Id
- **WHEN** a record or request arrives with a non-empty `x-request-id`
- **THEN** that exact value is reused unchanged across all internal hops and surfaced in the eventual response topic

#### Scenario: Upstream omits tracking headers
- **WHEN** the upstream response carries none of the tracking headers
- **THEN** the response record carries the originating `x-request-id` and `x-client-id`

#### Scenario: Required request id is not defaulted
- **WHEN** `x-request-id` is in the required list and a record arrives without it
- **THEN** no `x-request-id` header is added to the record, the record is handled by "Required headers", and its logs, metrics and audit carry a generated id

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
- **THEN** the existing header keys and values reach the upstream service unchanged, without applying the custom-prefix rule, except the headers covered by "Transport-controlled headers" and "Headers the upstream transport cannot carry"

### Requirement: Trace continuity across hops
Within a single round-trip, the `Trace-Id` value used when emitting metric lines and audit entries MUST equal the `Request-Id` driving the request. Operators correlating logs MUST see identical trace identifiers end-to-end.

#### Scenario: Round-trip correlation
- **WHEN** a record enters consume, traverses upstream forwarding, then exits via produce
- **THEN** all metric-file lines and audit entries associated with that request share a single common `Request-Id`-shaped identifier

### Requirement: Required headers

The system MUST keep one built-in list of required headers, empty in this release, applied at both entry points: records consumed from Kafka and publish API requests. Names are matched case-insensitively. A listed header MUST NOT get a default value. A consumed record whose listed header is absent, or invalid for the upstream transport, MUST NOT be sent and MUST be settled as a non-retryable upstream failure (see `message-routing`). Upstream responses are not checked.

#### Scenario: Empty list

- **WHEN** the required list is empty and a record arrives without `x-request-id` and `x-client-id`
- **THEN** the record is processed exactly as before: both headers get their defaults and the upstream is called

#### Scenario: Required header absent from a record

- **WHEN** `x-client-id` is in the required list and a consumed record has no `x-client-id` header
- **THEN** the upstream is not called, the failure is recorded and logged with the request id and `x-client-id`, and the offset is settled

#### Scenario: Required header invalid for the transport

- **WHEN** `x-request-id` is in the required list and a record on an HTTP pipeline carries `x-request-id` with a carriage return in its value
- **THEN** the upstream is not called and the record is settled as failure; the request id used in logs, metrics and audit is still the record's value

#### Scenario: Required header absent from a publish request

- **WHEN** `x-client-id` is in the required list and a publish request has no `x-client-id` header
- **THEN** the request is rejected as described in `control-api` "Required headers on publish requests" and nothing is published

#### Scenario: Upstream response without a listed header

- **WHEN** `x-trace-id` is in the required list and an upstream response carries no `x-trace-id`
- **THEN** the response is published as before

#### Scenario: Case-insensitive match

- **WHEN** `x-client-id` is in the required list and a record carries the header as `X-Client-Id`
- **THEN** the header counts as present

#### Scenario: Required names are never transport-controlled

- **WHEN** the built-in required list is inspected
- **THEN** none of its names is a transport-controlled header (see "Transport-controlled headers")

### Requirement: Tracking headers ignore letter case

A tracking header of a consumed record (`x-request-id`, `x-request-time`, `x-trace-id`, `x-client-id`) MUST be recognised in any letter case and stored under its lowercase wire name before any other rule applies. When a record carries several spellings of one header, the lowercase spelling MUST win, otherwise the first in byte order; the other spellings MUST be dropped.

#### Scenario: Canonical-case request id

- **WHEN** a record carries `X-Request-Id: abc` and no `x-request-id`
- **THEN** `abc` is the request id in logs, metrics and audit, no new id is generated, the upstream receives it as `x-request-id`, and the response record carries `x-request-id: abc`

#### Scenario: Canonical-case client id

- **WHEN** a record carries `X-Client-Id: billing`
- **THEN** the record carries exactly one `x-client-id`, with the value `billing`, and no default from `SYSTEM_ID` is added beside it

#### Scenario: Two spellings

- **WHEN** a record carries both `x-request-id: lower` and `X-Request-Id: upper`
- **THEN** the request id is `lower` and only `x-request-id` is forwarded

### Requirement: Headers the upstream transport cannot carry

A non-required header of a consumed record that the upstream transport cannot carry MUST be left out of the upstream call only, with a warning naming the header and the request id; the record MUST still be processed. HTTP cannot carry an invalid field name, or a value with a control character other than tab. gRPC cannot carry text that is not valid UTF-8, or a metadata `x-request-id` that is not printable ASCII.

#### Scenario: Line feed in an optional header over HTTP

- **WHEN** a record on an HTTP pipeline carries the header `note` with the value `a\nb`
- **THEN** the upstream is called without `note`, a warning names `note` and the request id, and the response is handled like any other

#### Scenario: Same header over gRPC

- **WHEN** the same record is consumed on a gRPC pipeline
- **THEN** `note` is sent in `details` unchanged, because gRPC can carry it

#### Scenario: Invalid UTF-8 over gRPC

- **WHEN** a record on a gRPC pipeline carries a header whose value is not valid UTF-8 and is not required
- **THEN** the header is left out of `details` with a warning, and the call is made

#### Scenario: Optional tracking header is invalid

- **WHEN** the required list is empty and a record on an HTTP pipeline carries `x-request-id` with a line feed
- **THEN** the record's `x-request-id` is left out of the call (the HTTP client then stamps a newly generated one, as for any outgoing call without it), a warning is logged, and the response record still carries the record's `x-request-id`

### Requirement: Transport-controlled headers

Headers whose meaning belongs to the HTTP connection or message framing MUST NOT be taken from a consumed record into an HTTP upstream call: the hop-by-hop headers (`Connection`, `Proxy-Connection`, `Keep-Alive`, `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`), `Host`, `Content-Length` and `Content-Type`. They are left out without a warning, and the call MUST carry `Content-Type: application/json`.

#### Scenario: Record carries Content-Type

- **WHEN** a record on an HTTP pipeline carries `Content-Type: text/plain`
- **THEN** the upstream receives `Content-Type: application/json` and no warning is logged

#### Scenario: Chained bridge

- **WHEN** a record was published by another bridge from an upstream response and carries `Content-Length`, `Connection` and `Keep-Alive`
- **THEN** none of them is sent to the upstream, and the call's own framing is used

#### Scenario: JSON subtype from a record

- **WHEN** a record on an HTTP pipeline carries `Content-Type: application/vnd.api+json`
- **THEN** the upstream still receives `Content-Type: application/json`

#### Scenario: gRPC is not affected

- **WHEN** the same record is consumed on a gRPC pipeline
- **THEN** every header, including `Content-Type`, is sent in `details` as data, and the call itself carries gRPC's own content type, which record headers cannot change
