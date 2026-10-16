# control-api Specification

## Purpose
Exposes an HTTP endpoint that publishes arbitrary payloads into a chosen Kafka topic so external systems can drive responses without owning a Kafka client.

## Requirements

### Requirement: Single publish endpoint at versioned path
The system MUST expose `POST /api/v{version}/publish`, where `{version}` matches the build-time API version constant. Requests to this path route through the request-id middleware before reaching the publish handler.

#### Scenario: Default path resolves
- **WHEN** the binary is built with the default API version constant
- **THEN** `POST /api/v1/publish` is registered and accepts traffic

#### Scenario: Unversioned root
- **WHEN** a client calls `/publish` directly without the version segment
- **THEN** the request is rejected as not found by the router

### Requirement: Target topic must be declared
Every inbound request MUST carry the header naming the target topic. Missing headers MUST abort processing before any payload parsing occurs and return HTTP 422 with a structured failure body.

#### Scenario: Header present
- **WHEN** the request contains the produce-topic header referencing a known configured producer
- **THEN** the handler forwards the body to that producer and returns success

#### Scenario: Header missing
- **WHEN** the request lacks the produce-topic header
- **THEN** the response carries HTTP 422 and a JSON object describing which header was required

#### Scenario: Topic not configured
- **WHEN** the header names a topic that has no matching producer in this deployment
- **THEN** the response indicates failure with detail explaining the missing producer, HTTP 200 is used to match existing behaviour rather than escalating to 5xx, and a failure event is still recorded in metrics

### Requirement: Body validation rules
The request body MUST be valid JSON containing a single top-level object. Bodies exceeding two megabytes MUST be rejected outright. Content-Type other than `application/json` MUST also be rejected.

#### Scenario: Valid JSON object accepted
- **WHEN** the body parses as a single JSON object under two megabytes
- **THEN** the data field is preserved verbatim in the published record

#### Scenario: Oversized body rejected
- **WHEN** the body exceeds the configured limit
- **THEN** the request fails fast with HTTP 422 and a clear message before consuming further bytes

#### Scenario: Wrong content type
- **WHEN** Content-Type is anything other than `application/json`
- **THEN** HTTP 415 is returned, no body is read and the handler is not invoked

#### Scenario: Trailing data rejected
- **WHEN** the body contains more than one JSON value (e.g. `{"a":1}{"b":2}`)
- **THEN** HTTP 422 is returned and the handler is not invoked

#### Scenario: Empty body rejected
- **WHEN** the request body is empty
- **THEN** HTTP 422 with a "must not be empty" diagnostic is returned

### Requirement: Endpoint unavailable until the producer is connected
When producers are configured but the shared producer is not connected yet (or never connected), the endpoint MUST answer HTTP 503 with an explanatory message instead of failing mid-request. After the producer is registered, requests are served normally without a restart.

#### Scenario: Request during startup
- **WHEN** a publish request arrives before the producer is registered
- **THEN** HTTP 503 "not ready" is returned and nothing is published

#### Scenario: Request after connection
- **WHEN** the producer becomes available
- **THEN** the next request is published and answered with success

### Requirement: Endpoint disabled when no producers configured
If the deployment declares zero producers, the publish endpoint MUST respond with HTTP 503 and an explanatory message rather than attempting to publish. This keeps deployments that only consume from making unsafe manual publishes.

#### Scenario: Producer-less deployment
- **WHEN** startup completes with an empty producer list
- **THEN** `POST /api/v{version}/publish` always answers 503 with a fixed explanatory body

### Requirement: Request id echoed on the publish response
The publish endpoint MUST apply the tracking-header rules owned by the `tracking-enrichment` capability to the inbound request, write the resulting header set onto the produced Kafka record, and echo the effective `x-request-id` in the HTTP response headers.

#### Scenario: Caller omits request id
- **WHEN** the inbound request carries no `x-request-id`
- **THEN** the generated id is echoed in the HTTP response headers and stored on the produced record

#### Scenario: Caller supplies request id
- **WHEN** the inbound request carries `x-request-id`
- **THEN** the same value is echoed in the HTTP response and stored on the produced record

### Requirement: Only POST is accepted on the publish path
The publish path MUST answer `405 Method Not Allowed` (with an `Allow: POST` header) to any method other than `POST`, in every deployment mode, including when producers are not configured or not yet connected.

#### Scenario: GET on the publish path
- **WHEN** a client sends `GET /api/v{version}/publish`
- **THEN** the response is 405 and nothing is published

#### Scenario: POST still routed
- **WHEN** a client sends `POST /api/v{version}/publish`
- **THEN** it is handled according to the existing publish requirements
