# bridge-kafka-service message flow

The service bridges Kafka and a business service (upstream). It has two independent paths:

- **Publishing:** HTTP client → `POST /api/v1/publish` → Kafka topic.
- **Processing:** Kafka topic → upstream (HTTP or gRPC) → response to `response_topic`.

```mermaid
sequenceDiagram
    autonumber
    actor Client as HTTP client
    participant API as Bridge: HTTP API
    participant K as Kafka
    participant B as Bridge: Consumer
    participant UP as Upstream (HTTP / gRPC)

    rect rgba(100, 150, 255, 0.1)
    Note over Client,K: Publishing via the HTTP API
    Client->>API: POST /api/v1/publish<br/>header x-bridge-kafka-produce-topic + JSON
    API->>API: validate the request, x-request-id / x-trace-id
    alt invalid request or producer not ready
        API-->>Client: 4xx / 503
    else topic allowed (listed in the producer config)
        API->>K: write to the topic (key = x-kafka-key)
        API-->>Client: 200 {"result":"success" | "failure"}
    end
    end

    rect rgba(100, 200, 120, 0.1)
    Note over K,UP: Record processing
    K-->>B: record from a consumer topic
    B->>B: body → JSON, tracking headers
    B->>B: header policy: required header missing / not carriable → failure, no call<br/>other headers the transport cannot carry → left out, warning
    B->>UP: POST /path or Predict(details, data)<br/>with service.timeout
    alt upstream responded (including HTTP 4xx/5xx and gRPC errors)
        UP-->>B: response
        opt response_topic set
            B->>K: write the response to response_topic
        end
    else depends only on the record (response > 16 MiB), any on_failure
        UP--xB: error → failure, settled after one attempt
    else unavailable / timeout, on_failure: skip (default)
        UP--xB: error → failure
    else unavailable / timeout, on_failure: retry
        loop backoff 1s → 30s, offset held
            B->>UP: repeat the call
        end
        Note over B,UP: success → continue as "upstream responded"<br/>5 failures while other calls succeed → poison: failure, skip
    end
    B->>K: commit the offset (strictly in order)
    end
```

At every step the service writes `bridge_kafka_service_*` metrics to a file and audit to ALS
(file and gRPC). They are not shown on the diagram.

## Key points

- **Broker outages do not terminate startup retries.** Producer and consumer clients
  retry with exponential backoff and jitter until shutdown. The HTTP server stays
  available: `/healthz` answers 200 and `/readyz` answers 503 while required clients
  initialize. Consumers wait for the configured producer to be ready. Readiness tracks
  initialization and draining, not continuous broker reachability; pause and upstream
  retries are reported separately in `/status`.
- **The offset is committed after processing.** A record counts as processed when
  the response is written to Kafka or the upstream call has finally failed. Offsets
  are committed strictly in read order. When the consumer group session ends (shutdown
  or rebalance), the claim waits up to `timeouts.shutdown` for its in-flight records,
  so the ones that finish are committed before the partition is released; records of
  the ended session that were not sent to the service yet are not sent at all. Only
  records whose full processing is still unfinished after the wait are redelivered,
  including responses still waiting to be published.
- **Response publication follows `on_failure` too.** Under `skip` a failed write to
  `response_topic` is recorded as a failure and the offset is settled. Under
  `retry` the write is repeated with the shared backoff (1s doubling up to 30s)
  until it succeeds, the offset held and the upstream not called again; only a
  non-transient error (for example a record too large for the broker) is settled
  as a failure. On shutdown the wait ends, an unpublished response is not settled
  and its record is redelivered.
- **Each pipeline publishes its own responses.** Every stanza with a `response_topic`
  has its own publish queue (1000 responses) and writes up to 16 responses at once
  (`max_conn` if lower, so a `max_conn: 1` pipeline writes one at a time, in order).
  A response topic that is slow or rejects writes holds back only its own pipeline.
- **Upstream failures follow `on_failure`.** A failure is only "unavailable / timeout";
  any HTTP status or gRPC status is a response. `skip` (default) records a failure and
  commits the offset. `retry` holds the offset and repeats the call with backoff until
  the upstream recovers, so an outage loses nothing. A record that keeps failing while
  other calls succeed is treated as poison and skipped after 5 attempts. Requires
  `service.max_conn`. A failure that depends only on the record (a response over
  16 MiB, a required header that is missing or that the transport cannot carry) is
  settled after one attempt under both policies, so it never stalls a pipeline, even
  one with `max_conn: 1`. Retries are visible per pipeline in `GET /status`.
- **Headers are checked before the call.** Transport-controlled headers (hop-by-hop,
  `Host`, `Content-Length`, `Content-Type`) never go into an HTTP call, which always
  carries `Content-Type: application/json`. Other headers the transport cannot carry
  are left out of the call with a warning; the record keeps them for audit and the
  response record.
- **The HTTP API returns 200 even if the Kafka write failed.** The outcome is
  in the `result` field.
- **One producer for all topics.** Only topics from the `producer` config can be
  published to, both via the HTTP API and via `response_topic`.
- **Concurrency:** `service.max_conn > 0` limits concurrent upstream calls to
  that number; otherwise fast mode applies a built-in ceiling of 512 concurrent
  calls per pipeline (reading pauses until a call finishes). Each HTTP upstream
  keeps its own connection pool sized to the pipeline's limit, so calls reuse
  connections instead of reconnecting.
- **Signals:** `SIGUSR1` pauses or resumes consumption of the whole process: partitions
  received while paused stay paused, and already delivered records are held until resume.
  `SIGINT`/`SIGTERM` stop the service: the publish API and `/readyz` answer 503 with
  `shutting down` while `/healthz`, `/health` and `/status` keep answering, intake stops,
  upstream calls get up to `timeouts.shutdown` (default 10s) to finish, accumulated responses
  are published, and the HTTP server closes last.
