# payload Specification

## Purpose
Normalises every consumed Kafka record body into a canonical JSON shape so downstream services receive predictable input regardless of how producers formatted the original value.

## Requirements

### Requirement: Empty body becomes empty object
When a consumed record's value is empty, the system MUST treat it as the JSON literal `{}`. The downstream caller receives a syntactically valid empty object and never a null/missing value.

#### Scenario: Tombstone record
- **WHEN** a Kafka tombstone (null-valued record) is received
- **THEN** the data field downstream equals `{}`

### Requirement: Valid JSON passes through unchanged
If the raw value parses directly as JSON, the system MUST propagate those bytes verbatim into the downstream data field with no rewriting.

#### Scenario: Already-canonical JSON
- **WHEN** the producer sends well-formed JSON
- **THEN** consumers downstream see the exact original byte sequence

### Requirement: Unicode-escaped JSON recovers
When the raw value fails direct JSON parse but matches a unicode-escape-then-re-quote pattern (i.e., the bytes contain `\uXXXX` escapes that need decoding), the system MUST recover the underlying JSON document and use that instead.

Both the `\uXXXX` and `\xXX` decoding passes MUST compose: the output of the first pass is the input of the second, so a decoding performed by the first pass is never discarded.

#### Scenario: Escaped Cyrillic payload
- **WHEN** a producer delivers a JSON-looking value whose only issue is escaped Unicode sequences
- **THEN** the decoded form reaches downstream with proper Unicode characters intact

### Requirement: Non-JSON UTF-8 string coercion
When the raw value is a printable UTF-8 string that is not valid JSON, the system MUST marshal that string as a JSON string and surface the marshalled result downstream. The original textual content MUST survive intact.

#### Scenario: Plain-text payload
- **WHEN** a producer publishes plain UTF-8 text outside JSON syntax
- **THEN** downstream sees a JSON-string wrapper around the original text

### Requirement: Raw byte payload wrapped as base64
When the raw value is neither valid JSON nor printable UTF-8, the system MUST base64-encode the bytes and embed the encoded string into a JSON wrapper. Downstream services therefore always receive syntactically valid JSON.

#### Scenario: Arbitrary binary payload
- **WHEN** a producer places non-text bytes into the value
- **THEN** downstream receives a JSON document whose string field decodes (via base64) to the original bytes

### Requirement: Coercion applies once at consumption
Payload normalisation happens exclusively during initial consumption. Downstream stages (forwarding, response publication, manual publish API) MUST treat the normalised JSON as immutable; no further coercion attempts are made.

#### Scenario: Forwarded payload not re-normalised
- **WHEN** the normalised value is forwarded to the upstream HTTP service
- **THEN** the upstream receives the canonical JSON shape that resulted from the first-stage coercion, never the original mixed encoding

### Requirement: Payload coercion never fails
Every consumed Kafka record MUST yield a syntactically valid JSON payload: coercion has no failure mode that drops or rejects a record. Whatever the bytes are, exactly one of the documented rules (empty object, verbatim JSON, unicode recovery, UTF-8 string, base64 string) applies.

#### Scenario: Arbitrary bytes
- **WHEN** a record carries any byte sequence, including invalid UTF-8 and NUL bytes
- **THEN** the downstream payload is valid JSON and the record is processed like any other
