# message-routing Specification

## Purpose
Routes Kafka records arriving on configured input topics to a configurable upstream service and publishes the upstream response back to a separate Kafka topic, one independent pipeline per consumer stanza in configuration. HTTP is the only transport implemented today; the pipeline is transport-agnostic so gRPC and other protocols can be added without changing the consume/settle/publish contract.

## Requirements

### Requirement: Pipeline per consumer stanza
For every entry under `consumer:` in the application configuration, the system SHALL start an independent routing pipeline identified by its topics, group id, target service and response topic. Multiple pipelines MUST operate concurrently without sharing goroutines, channels, or producer instances across them.

#### Scenario: Two pipelines run side by side
- **WHEN** the configuration contains two consumer stanzas for disjoint topic lists, distinct groups and distinct upstream services
- **THEN** each pipeline consumes only its declared topics and routes exclusively to its own upstream service

#### Scenario: Single-pipeline deployment
- **WHEN** the configuration declares exactly one consumer stanza
- **THEN** exactly one pipeline runs and binds to the single stanza's topics

### Requirement: HTTP forwarding of consumed record
When a Kafka record is claimed, the system SHALL issue a `POST` to `{scheme}://{service.name}:{service.port}{service.path}` (`scheme` is `http` for TLS mode `none` and `https` otherwise, see `upstream-transport-security`) whose body carries the normalised record payload, with the original Kafka record headers attached as request headers. The request MUST be issued within the timeout configured for that consumer; if no timeout is configured the system MUST default to 30 seconds.

#### Scenario: Successful upstream call
- **WHEN** a record arrives on a subscribed topic and the upstream responds with HTTP 2xx within the timeout
- **THEN** the request body matches the normalised payload and all eligible Kafka headers are sent as HTTP headers

#### Scenario: Upstream unreachable
- **WHEN** the upstream host cannot be resolved or rejects the TCP connection
- **THEN** the record is handled by the stanza's failure policy (see "Upstream failure policy"): with `skip` the event outcome is recorded as failure, the message is settled and processing continues with the next message

#### Scenario: Upstream slower than timeout
- **WHEN** the upstream does not respond within the configured timeout
- **THEN** the request is aborted and the record is handled by the stanza's failure policy: with `skip` the event outcome is recorded as failure, the message is settled and processing continues with the next message

### Requirement: Response publication into response topic
When the upstream HTTP call completes (success or any non-network failure that still yields a response), the system SHALL publish the upstream response body to the pipeline's configured `response_topic`. The published Kafka record MUST carry forward the enriched tracking headers (`Request-Id`, `Trace-Id`, `Client-Id`) plus any other response headers exposed by the upstream.

#### Scenario: Upstream does not echo tracking headers
- **WHEN** the upstream response lacks `x-request-id` (or `x-client-id`)
- **THEN** the published record carries the `Request-Id` (and `Client-Id`) of the originating Kafka record rather than newly generated values, so request and response correlate

#### Scenario: Tracking headers use their wire names
- **WHEN** a response record is published
- **THEN** `x-request-id`, `x-request-time`, `x-trace-id` and `x-client-id` appear exactly once each under their lowercase wire names, with no HTTP-canonicalised duplicates such as `X-Client-Id`

#### Scenario: Successful round-trip
- **WHEN** upstream returns a parseable JSON response
- **THEN** that response body appears as a new record in the configured response topic with preserved tracking headers

#### Scenario: Non-JSON upstream response
- **WHEN** upstream returns a body that fails JSON parsing
- **THEN** the raw bytes are stored under the data field as a JSON-encoded string and published to the response topic

#### Scenario: Empty upstream response body
- **WHEN** upstream returns HTTP 2xx with an empty body
- **THEN** a record is published to the response topic carrying an empty/null data field

#### Scenario: Response topic absent
- **WHEN** the pipeline configuration provides no response topic
- **THEN** the upstream response is consumed but never republished to Kafka

### Requirement: At-least-once offset settlement
The system MUST NOT commit a Kafka offset until the corresponding message has been fully processed: the upstream call has finished and its response has been published to the response topic, or processing has terminally failed (upstream failure under `skip`, poison record under `retry`, dropped response under `skip`, failed response publication under `skip`, non-transient response publication error, missing producer under `skip`). Offsets MUST be marked per partition in consumption order: an offset MUST NOT be marked while any earlier offset of the same claim is still in flight, even if later messages finish first. Marked offsets MUST be committed to the broker periodically and on a clean session end, whatever the Kafka client tuning file says, so a restarted or rebalanced group resumes after the last settled record. Terminal failures are recorded in metrics and audit; dead-lettering is out of scope.

#### Scenario: Crash mid-flight redelivers
- **WHEN** the process terminates while an upstream call for offset N is still in flight
- **THEN** offset N is not committed and the record is redelivered after restart

#### Scenario: Out-of-order completion
- **WHEN** offsets 1, 2 and 3 are in flight concurrently and 3 then 2 complete before 1
- **THEN** no offset is marked until 1 completes, after which 1, 2 and 3 are marked together

#### Scenario: Terminal failure does not stall the partition
- **WHEN** the upstream call for a record fails and the stanza's failure policy is `skip`
- **THEN** the failure is recorded and the offset is settled so later offsets can be committed

#### Scenario: Settlement is idempotent
- **WHEN** the same message's settlement is signalled more than once
- **THEN** the offset is marked exactly once

#### Scenario: Settled offsets survive a restart
- **WHEN** offsets up to N have been settled, the commit interval has elapsed and the process restarts
- **THEN** the group resumes from offset N+1, not from the configured initial offset

#### Scenario: Tuning file disables auto-commit
- **WHEN** `config_consumer.yaml` sets `consumer.offsets.autocommit.enable: false`
- **THEN** settled offsets are still committed to the broker

#### Scenario: Response publication fails under skip
- **WHEN** a stanza with `on_failure: skip` gets an upstream response and writing it to `response_topic` fails
- **THEN** the failure is recorded and the offset is settled, as before

### Requirement: Orderly pipeline shutdown
On termination a pipeline MUST first stop consumption (cancel the consume context and wait for the consume loop to return), only then close the shared event channel, and finally wait for the response handler. The event channel MUST NOT be closed while any claim handler can still send to it.

#### Scenario: Shutdown while claims are active
- **WHEN** a termination signal arrives while `ConsumeClaim` is handing records to the response handler
- **THEN** no goroutine panics with "send on closed channel" and all pipeline goroutines return

### Requirement: Per-message payload ownership
Objects created for a consumed record (request body, event) are handed to other goroutines (upstream call, response publication) and MUST be freshly allocated per message. They MUST NOT be taken from or returned to a shared pool while any goroutine can still read them.

#### Scenario: Concurrent messages do not corrupt each other
- **WHEN** message M1 is being forwarded while M2 is consumed from the same partition
- **THEN** the request forwarded for M1 contains exactly M1's payload and headers

### Requirement: Configuration-driven upstream address
The system MUST derive the upstream URL strictly from the consumer stanza fields `service.name`, `service.port`, and `service.path` (with sensible defaults applied for any omitted field). Operators MUST NOT need code changes to point a pipeline at a different upstream.

#### Scenario: Default port resolution
- **WHEN** `service.port` is empty and the operator has set the matching `<NAME>_SERVICE_PORT` environment variable
- **THEN** that environment value is used as the upstream port

#### Scenario: Default path fallback
- **WHEN** `service.path` is empty
- **THEN** the system's built-in default base path is used

### Requirement: Transport-neutral upstream contract
Every upstream transport (HTTP today, gRPC in a future release) MUST implement the same interaction contract, and everything outside "perform the call" MUST be shared across transports and behave identically: the normalised payload and the Kafka record headers are sent to the upstream; the call is bounded by the consumer's timeout (default 30 seconds); the outcome is exactly one of "response (body + headers)" or "failure"; a failure is recorded in metrics and audit and is settled or retried according to the stanza's failure policy; a response is published to `response_topic` with tracking headers carried over from the originating record whenever the upstream omits them; without a `response_topic` the offset settles right after the call; the concurrency cap (`max_conn`) and fast/slow modes apply unchanged. A new transport MUST NOT introduce its own settlement, retry, header-enrichment or publication behaviour. Each transport MUST state which headers it cannot carry and MUST mark a call failure that depends only on the record or its response as non-retryable; the shared header policy (see `tracking-enrichment`) and "Non-retryable upstream failures" decide the outcome.

#### Scenario: Same outcome regardless of transport
- **WHEN** the same record is processed once over HTTP and once over a future gRPC transport against equivalent upstreams
- **THEN** the published response record, the metric lines, the audit entries and the settled offsets are identical apart from transport-specific header names

#### Scenario: Upstream failure over any transport
- **WHEN** the upstream is unreachable, times out or returns a transport-level error and the stanza's failure policy is `skip`
- **THEN** the event is recorded as failure, nothing is published, and the offset is settled

#### Scenario: Retry over any transport
- **WHEN** the stanza's failure policy is `retry` and the upstream fails at transport level over HTTP or gRPC
- **THEN** the call is retried identically for both protocols

#### Scenario: Tracking headers absent from the upstream response
- **WHEN** the upstream response carries no tracking headers
- **THEN** the response record carries the originating `x-request-id` and `x-client-id`

#### Scenario: Header a transport cannot carry
- **WHEN** a record carries an optional header that one transport can carry and another cannot
- **THEN** the transport that cannot carry it leaves it out under the shared policy, and everything else about the record's processing is identical on both transports

### Requirement: Upstream protocol selection
Each consumer stanza MUST select its upstream protocol with `service.protocol`, either `http` or `grpc`. When omitted it MUST default to `http`, so existing configurations behave exactly as before. An unknown value MUST abort startup with an error naming the stanza. Everything defined by "Transport-neutral upstream contract" applies identically to both protocols; for `grpc`, `service.path` is not used (the method is fixed by the service contract).

#### Scenario: Default protocol
- **WHEN** a consumer stanza has no `service.protocol`
- **THEN** its records are forwarded over HTTP

#### Scenario: gRPC selected
- **WHEN** a stanza sets `service.protocol: grpc`
- **THEN** its records are forwarded to `{service.name}:{service.port}` over gRPC and settlement, headers, publication and metrics behave as for HTTP

#### Scenario: Unknown protocol
- **WHEN** a stanza sets `service.protocol: soap`
- **THEN** startup fails with an error naming the stanza and the allowed values

### Requirement: Upstream failure policy
Each consumer stanza MUST select how transport failures are handled with `on_failure`, either `skip` or `retry`. When omitted it MUST default to `skip`, so existing configurations behave exactly as before. An unknown value MUST abort startup with an error naming the stanza. Upstream responses, including HTTP 4xx/5xx and non-transport gRPC statuses, are not failures and MUST NOT be retried.

#### Scenario: Default policy
- **WHEN** a consumer stanza has no `on_failure`
- **THEN** a transport failure is recorded and its offset is settled, as with `on_failure: skip`

#### Scenario: Unknown policy
- **WHEN** a stanza sets `on_failure: park`
- **THEN** startup fails with an error naming the stanza and the allowed values

#### Scenario: Error responses are not retried
- **WHEN** a stanza sets `on_failure: retry` and the upstream answers HTTP 503
- **THEN** the response is published to `response_topic` once and the call is not repeated

### Requirement: Retry until the upstream recovers
With `on_failure: retry` a transport failure MUST NOT settle the offset. The system MUST repeat the call with exponential backoff, starting at about 1 second and capped at about 30 seconds, until the call yields a response, the record is skipped as poison, or the pipeline stops. Each failed attempt MUST be logged as a warning that states the attempt number and how long the record has been failing since its first failed attempt; failure metrics and audit MUST record only the record's final outcome.

#### Scenario: Upstream outage
- **WHEN** the upstream is down for 10 minutes and then recovers
- **THEN** every record consumed during the outage is delivered to the upstream after recovery, its response is published, and no record is settled as failure

#### Scenario: One outcome per record
- **WHEN** a record fails three attempts and succeeds on the fourth
- **THEN** three warnings are logged and exactly one success, and no failure, is counted for it

#### Scenario: Failing duration in the warning
- **WHEN** a record has been failing for 2 minutes and its next attempt fails
- **THEN** the warning for that attempt states the attempt number and a failing duration of about 2 minutes

#### Scenario: Shutdown during retry
- **WHEN** the pipeline stops while a record is waiting for its next attempt
- **THEN** the wait ends within the shutdown timeout, the offset is not settled and the record is redelivered after restart

#### Scenario: Consumption stops while all slots retry
- **WHEN** every `max_conn` slot holds a record that is being retried
- **THEN** no further upstream calls are started for the pipeline until a slot frees

### Requirement: Poison record skipping
With `on_failure: retry` a record that has failed 5 attempts MUST be recorded as failure, logged as poison and settled, but only if another call of the same pipeline succeeded after the record's first failure. While no call of the pipeline succeeds the record MUST keep being retried.

#### Scenario: Record fails while the upstream is healthy
- **WHEN** a record times out on 5 attempts while other records of the pipeline get responses in the meantime
- **THEN** the record is recorded as failure, a poison warning naming its request id is logged and its offset is settled

#### Scenario: No neighbour succeeds
- **WHEN** a pipeline with `max_conn: 1` has a record failing on every attempt
- **THEN** the record keeps being retried and is never skipped

### Requirement: Retry requires a concurrency cap
A stanza with `on_failure: retry` MUST set `service.max_conn` greater than zero; otherwise startup MUST fail with an error naming the stanza.

#### Scenario: Retry without max_conn
- **WHEN** a stanza sets `on_failure: retry` and leaves `service.max_conn` unset
- **THEN** startup fails with an error naming the stanza and `service.max_conn`

#### Scenario: Retry with max_conn
- **WHEN** a stanza sets `on_failure: retry` and `service.max_conn: 3`
- **THEN** the pipeline starts in slow mode with retries enabled

### Requirement: Response publication retry under retry policy

For a stanza with `on_failure: retry`, a transient failure to write the upstream response to `response_topic` MUST NOT settle the offset. The system MUST repeat the write with the backoff of "Retry until the upstream recovers" until it succeeds or the service stops; the upstream MUST NOT be called again. Each failed attempt MUST be logged as a warning that states the attempt number and how long the response has been failing since its first failed write; metrics and audit MUST record only the final outcome of the response. A response whose write is being repeated MUST hold back only the responses of its own pipeline, never those of other pipelines. A failure that cannot succeed on retry MUST NOT be repeated: the failure MUST be recorded and the offset settled. Such failures are an unmarshallable body, a broker rejection of the record itself (size or format), and a refusal by the Kafka client before sending, such as a record larger than `producer.maxmessagebytes` or a feature the configured Kafka version does not support.

#### Scenario: Broker rejects writes temporarily

- **WHEN** a retry stanza's response cannot be written for 2 minutes and the broker then accepts writes again
- **THEN** the response is published exactly once after recovery, the upstream was called once, and the offset is settled only after the publication succeeds

#### Scenario: One outcome per response

- **WHEN** writing a retry stanza's response fails twice and succeeds on the third attempt
- **THEN** two warnings are logged and exactly one success, and no failure, is recorded for the response

#### Scenario: Failing duration in the publication warning

- **WHEN** a retry stanza's response has been failing to be written for 2 minutes and the next write fails
- **THEN** the warning for that attempt states the attempt number and a failing duration of about 2 minutes

#### Scenario: Shutdown while publication is retried

- **WHEN** the service stops while a retry stanza's response is waiting for its next write attempt
- **THEN** the wait ends, the response is not published by that attempt, and the originating offset is not settled, so the record is redelivered after restart

#### Scenario: Non-transient publication error

- **WHEN** writing a retry stanza's response fails because the record exceeds the broker's size limit
- **THEN** the write is not repeated, the failure is recorded and logged with the request id, and the offset is settled

#### Scenario: Response larger than the producer limit

- **WHEN** a retry stanza's response is larger than `producer.maxmessagebytes` (1 MiB by default)
- **THEN** the write is attempted once, the failure is recorded and logged with the request id, the offset is settled, and the pipeline goes on with the next responses

#### Scenario: Skip stanza unaffected

- **WHEN** writing a response of a stanza with `on_failure: skip` fails
- **THEN** the write is not repeated and the offset is settled

#### Scenario: Retried publication is isolated to its pipeline

- **WHEN** a retry stanza's response topic rejects writes and its responses are being retried, while another stanza publishes to a healthy topic
- **THEN** the other stanza's responses are published without waiting for the retries, and only the retry stanza's own pipeline is held back

### Requirement: Retry response topic must be served by a producer
A stanza with `on_failure: retry` and a non-empty `response_topic` MUST name a topic that is listed under `producer:`; otherwise startup MUST fail with an error naming the stanza and the topic. Stanzas with `on_failure: skip` are not checked.

#### Scenario: Retry stanza with unlisted response topic
- **WHEN** a stanza sets `on_failure: retry` and `response_topic: out.a`, and no `producer:` entry lists `out.a`
- **THEN** startup fails with an error naming the stanza and `out.a`

#### Scenario: Retry stanza with listed response topic
- **WHEN** a stanza sets `on_failure: retry`, `service.max_conn: 3` and `response_topic: out.a`, and a `producer:` entry lists `out.a`
- **THEN** the service starts normally

#### Scenario: Skip stanza with unlisted response topic
- **WHEN** a stanza with `on_failure: skip` names a `response_topic` that no `producer:` entry lists
- **THEN** startup is not aborted by this check

### Requirement: Upstream response size limit

The HTTP upstream MUST read at most 16 MiB of a response body, the same limit the gRPC upstream applies. A response body larger than that MUST fail the call as a non-retryable upstream failure (see "Non-retryable upstream failures"): it is recorded as failure and settled under every `on_failure` policy, and it MUST NOT be buffered beyond the limit.

#### Scenario: Response over the limit

- **WHEN** an HTTP upstream answers with a 20 MiB body
- **THEN** the call fails, at most 16 MiB plus one byte of the body is read, the failure is recorded and the offset is settled without a retry

#### Scenario: Response within the limit

- **WHEN** an HTTP upstream answers with a 2 MiB body
- **THEN** the body is read in full and handled like any other response

### Requirement: Settlement across session end

When a consumer group session ends (shutdown or rebalance), the system MUST NOT let the session's final offset commit happen before the records handed out by that session have settled or been abandoned, waiting at most `min(drain window, consumer.group.rebalance.timeout)`. A record settled within that wait MUST be committed and MUST NOT be redelivered. A record whose session has ended and that has not been dispatched to the upstream yet MUST NOT be dispatched: it MUST be left unsettled without an upstream call, so it is redelivered without a duplicate side effect. A response publication repeated under `on_failure: retry` MUST stop without settling once its record's session has ended. During shutdown, waiting for in-flight upstream calls MUST take at most one drain window in total.

#### Scenario: Call finishes within the drain window on shutdown

- **WHEN** a termination signal arrives while an upstream call is in flight and the call finishes (and its response is published) within the drain window
- **THEN** its offset is committed before the session is released, and the record is not redelivered after restart

#### Scenario: Buffered records of a revoked partition

- **WHEN** a rebalance ends the session while records of that session are buffered and not yet dispatched
- **THEN** none of them is sent to the upstream, none is settled, and they are redelivered to the partition's next owner

#### Scenario: In-flight call during a rebalance

- **WHEN** a rebalance ends the session while an upstream call of that session is in flight and finishes within the wait bound
- **THEN** its offset is committed before the session is released, and the next owner does not receive the record again

#### Scenario: Wait bound elapses

- **WHEN** an in-flight call of an ended session does not finish within the wait bound
- **THEN** the session is released without its mark, and the record is redelivered

#### Scenario: Retried publication of an ended session

- **WHEN** a retry stanza's response is being republished and its record's session ends
- **THEN** the republication stops, the offset is not settled, and the record is redelivered

### Requirement: Stanzas without topics are not started

A consumer stanza without topics MUST be treated as disabled: the system MUST NOT create a Kafka client or a subscription for it, and MUST log one informational line naming the stanza at startup. It MUST NOT produce repeated errors while the process runs.

#### Scenario: Disabled stanza

- **WHEN** the configuration contains a consumer stanza with an empty `topics` list next to an active one
- **THEN** only the active stanza gets a Kafka client and a subscription, one info line names the disabled stanza, and no "no topics provided" error is ever logged

### Requirement: Non-retryable upstream failures

A call failure that depends only on the record or on its response MUST be recorded as failure and settled after one attempt, under every `on_failure` policy, and MUST NOT count toward the poison detector. These failures are an HTTP response body over the upstream response size limit, and a required header that is missing or invalid for the transport (see `tracking-enrichment`). Every other call failure keeps its `on_failure` handling.

#### Scenario: Oversized response on a sequential retry pipeline

- **WHEN** a stanza with `on_failure: retry` and `max_conn: 1` calls an HTTP upstream that answers one record with a 20 MiB body
- **THEN** the call is made once, the failure is recorded and logged with the request id, the offset is settled, and the next record of the partition is processed

#### Scenario: Invalid required header on a retry pipeline

- **WHEN** a header is listed as required, a consumed record carries it with a value containing a line feed, and the stanza sets `on_failure: retry`
- **THEN** the upstream is not called, the failure is recorded and logged with the request id and the header name, and the offset is settled

#### Scenario: Skip stanza

- **WHEN** a stanza with `on_failure: skip` receives a 20 MiB HTTP response
- **THEN** the failure is recorded once and the offset is settled, as before

#### Scenario: Network failures are still retried

- **WHEN** a stanza with `on_failure: retry` cannot connect to its upstream, or the call times out
- **THEN** the call is retried as described in "Retry until the upstream recovers"

### Requirement: HTTP upstream address validated at startup

For an active stanza with `service.protocol: http`, startup MUST fail with an error naming the stanza unless `service.name` contains no whitespace or control characters, `service.port` is a decimal number from 1 to 65535, `service.path` starts with `/`, and the URL built from them forms a valid HTTP request. The check MUST NOT resolve or contact the host. Stanzas without topics MUST NOT be checked.

#### Scenario: Invalid port

- **WHEN** an `http` stanza with topics has `service.port` set to `abc`, `0` or `70000`
- **THEN** startup fails with an error naming the stanza and the port

#### Scenario: Path without a leading slash

- **WHEN** an `http` stanza with topics has `service.path: predict`
- **THEN** startup fails with an error naming the stanza and the path

#### Scenario: Unreachable host

- **WHEN** an `http` stanza has `service.name: model-svc`, `service.port: 8080` and the host does not resolve
- **THEN** startup succeeds and each record fails as a transport failure until the service is reachable

#### Scenario: Defaults are valid

- **WHEN** an `http` stanza sets only `service.name`
- **THEN** the default port and path pass the check
