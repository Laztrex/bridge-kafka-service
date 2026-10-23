# bridge-kafka-service

**bridge-kafka-service** bridges Kafka topics and external services. It consumes records from Kafka topics, forwards them to a configured service and publishes the response back to Kafka. HTTP and gRPC transports are implemented; the architecture allows adding other protocols.

> The headers `x-bridge-kafka-produce-topic` and `x-bridge-kafka-header-*`, legacy Envoy ALS audit values and the gRPC method `/controller_kafka.BaseModelInterface/Predict` are frozen by spec `project-identity`. Metrics use the prefix `bridge_kafka_service_`; see the migration table under "Logs and metrics".

## Run

The app alone (Kafka and the target service must be reachable; details in "Quick start"):

~~~bash
go build -o bridge-kafka-service ./cmd/bridge-kafka-service
./bridge-kafka-service --config examples/config_local_http.yaml
~~~

Check a configuration without starting the service (for example in a deploy pipeline, before the rollout):

~~~bash
./bridge-kafka-service --check-config --config examples/config_local_http.yaml
# configuration examples/config_local_http.yaml is valid      (stdout, exit code 0)
# error: unknown config key "kafka.brokrs"                   (stderr, exit code 1)
# error: invalid consumer topology: response topics form a cycle ...   (stderr, exit code 1)
~~~

`--check-config` loads the file and runs exactly the checks startup runs, including the TLS and Kafka tuning files, so run it where those files are (the target image, the same working directory). It prints every warning and every error, one per line, connects to nothing and writes no log or metric files. **Before upgrading, run it with the new binary against each deployed configuration**: newer releases reject mistakes older ones let through.

The whole stand in Docker Compose, with a check of the chain Kafka → service → Kafka (the upstream service is wired in a local `compose.override.yaml`; see "Local stand"):

~~~bash
docker compose up -d --build
until docker compose logs bridge-kafka-service | grep -q "up and running"; do sleep 2; done
printf 'x-request-id:demo-001\t{"params":[5,10]}\n' | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic bridge.requests.1 --property parse.headers=true
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic bridge.responses.1 --from-beginning --max-messages 1   # the response from your service
docker compose down -v         # stop and remove the stand
~~~

## Quick start

Requirements:

* Docker + Docker Compose for the full stand (the simplest path);
* stable Go 1.27.x, at least 1.27.3, to build and test without Docker;
* `protoc` only if you change `.proto` files (see "gRPC stubs");
* `openssl` only for demo certificates (TLS/mTLS examples).

Run the bridge on the host with Kafka from Compose (convenient for debugging and the gRPC/TLS examples; the consumer in `examples/config_local_http.yaml` calls an HTTP service on `localhost:8000` — run your own, or point the config at one you like):

~~~bash
# 0. Repository
git clone <repository-url> && cd bridge-kafka-service

# 1. Build
go build -o bridge-kafka-service ./cmd/bridge-kafka-service

# 2. Infrastructure only (no bridge container)
docker compose up -d zookeeper kafka kafka2 kafka3 kafka4

# 3. Brokers advertise themselves as kafka..kafka4, and their ports are published on the host
echo "127.0.0.1 kafka kafka2 kafka3 kafka4" | sudo tee -a /etc/hosts

# 4. Run from the repository root (configs/config_consumer.yaml and config_producer.yaml are read from here)
./bridge-kafka-service --config examples/config_local_http.yaml
~~~
Ready-made configs are in `examples/` (`config_local_http.yaml`, `config_grpc.yaml`, `config_grpc_mtls.yaml`); tests validate them. Config selection order: the `--config` flag → the `CONFIG_PATH` variable → `configs/config_app.yaml`.

To use the default path, copy `configs/config_app.yaml.example` to `configs/config_app.yaml` and fill in the deployment values; the local file is ignored by Git. The Compose stand selects `configs/config_compose.yaml` through `CONFIG_PATH` in `docker.env`.

Send and read with the same commands as in "Local stand", but with this config's topics (`demo.in` → `demo.out`; `curl` publishing goes to `demo.http.out`):
~~~bash
printf 'x-request-id:local-001\t{"params":[5,10]}\n' | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic demo.in --property parse.headers=true
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic demo.out --from-beginning --max-messages 1 --property print.headers=true
~~~
Topics are created automatically; to create one explicitly: `docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --topic demo.in --partitions 1 --replication-factor 1 --if-not-exists`.

Stop: `Ctrl+C` / `SIGTERM` (graceful, up to `timeouts.shutdown`, default 10s, for active calls to finish); pause/resume reading: `kill -USR1 <pid>`.

## Local stand

Starts 4 Kafka brokers + ZooKeeper, Kafka UI and the bridge. The stand configuration (`configs/config_compose.yaml`, mounted read-only, selected by `CONFIG_PATH` in `docker.env`) points its consumers at `app-svc` and the audit at `audit-grpc-svc`; provide these services — or your own — in a `compose.override.yaml` next to `compose.yaml`: docker compose merges it automatically when it exists.

| What | Address from the host | Address inside the compose network |
|---|---|---|
| Bridge (health, publish) | `http://localhost:8081` | `bridge-kafka-service:8081` |
| Kafka UI | `http://localhost:8090` | — |
| Kafka (brokers) | `localhost:39092`, `localhost:39093`, `localhost:39094`, `localhost:39095` | `kafka:29092`, `kafka2:29093`, `kafka3:29094`, `kafka4:29095` |

**Step 1. Start.** The images default to public `golang:1.27.3` and `alpine:3.21`; set `BUILDER_IMAGE`/`RUNTIME_IMAGE` to use others:
~~~bash
docker compose up -d --build
docker compose ps                                  # every service must be Up
~~~

**Step 2. Check that the bridge is alive** (it answers immediately, without waiting for Kafka):
~~~bash
curl -s http://localhost:8081/health
# {"status":"running"}
~~~

**Step 3. Wait for the consumers.** The bridge reads only *new* records (`offsets.initial: -1`), so send them after this line:
~~~bash
docker compose logs bridge-kafka-service | grep "up and running"
# consumer for topics [bridge.requests.1] up and running
~~~

**Step 4. Send a record to the input topic.** With your upstream in place (the previous paragraph): the `bridge.requests.1` consumer calls `app-svc:8080/predict` and publishes the response to `bridge.responses.1`. The `x-request-id` header is optional (without it the bridge generates a UUID).
~~~bash
printf 'x-request-id:demo-001\t{"params":[5,10]}\n' | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic bridge.requests.1 \
  --property parse.headers=true
~~~
(the header/value separator is a tab; `printf` produces it from `\t`).

**Step 5. Read the response from the output topic:**
~~~bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic bridge.responses.1 --from-beginning --max-messages 1 --property print.headers=true
~~~
Expected result: headers and body come from your service (header order is not guaranteed; `Date`, `Content-Length` and times will differ). What matters: **the `x-request-id` of the input record reached the response**, and the service's own response headers are carried over unchanged.

**Step 6. See what happened.**
~~~bash
# the bridge event chain (Read from topic -> Send to service -> Response from service)
docker compose logs bridge-kafka-service | grep demo-001
#  event: Read from topic, x-request-id: demo-001, status: success, latency: 2
#  event: Send to service, x-request-id: demo-001, status: success, latency: 45
#  event: Response from service, x-request-id: demo-001, status: success, latency: 3

# metrics of this request (JSON lines)
docker compose exec bridge-kafka-service sh -c 'grep demo-001 app-metrics.log'
#  {"rqUId":"demo-001","metric_name":"bridge_kafka_service_consume_success_total","metric_value":"1"}
#  ...

# audit of this request: always mirrored to app-metrics.log.audit (JSON lines),
# and sent to the ALS receiver configured in audit.grpc_endpoint (see "Audit sink")
docker compose exec bridge-kafka-service sh -c 'grep demo-001 app-metrics.log.audit'
~~~
The same is visible in Kafka UI: <http://localhost:8090> → Topics → `bridge.responses.1` → Messages.

**Step 7. Stop.**
~~~bash
docker compose down -v        # -v also removes volumes
~~~

> The stand config is `configs/` of the repository (`config_compose.yaml`, `config_consumer.yaml`, `config_producer.yaml`), mounted read-only into the container; variables are in `docker.env` (`CONFIG_PATH` selects the app config). Logs and metrics are written to the container working directory `/go/src/bridge-kafka-service` (`app.log`, `app-metrics.log`, `app-metrics.log.audit`).

## Tests

```bash
make test               # unit tests with -race (no Kafka, Docker or network needed)
make ci                 # every check CI runs: fmt-check, vet, tests, lint, vuln, coverage gate, proto-check, docs-check
```

There are no integration tests: Kafka, gRPC sinks and HTTP services are replaced by stubs from `testutil/` or loopback servers; the whole chain is checked on the local stand (see "Local stand").

### Individual packages, tests and modes
~~~bash
go test ./internal/transport/...                          # one package
go test -race -run 'TestParity' ./internal/transport      # by test name
go test -v -run 'TestGRPCUpstream_TLS' ./internal/transport
go test -count=3 -race ./...                              # hunt for flaky tests
go test ./examples/grpc-model                             # the example gRPC service against the bridge client (plain/TLS/mTLS)
~~~

### What covers what
| Area | Where | How |
|---|---|---|
| Consumer, offsets, pause/resume, shutdown | `internal/router` | a fake consumer group (`testutil/mocksarama`); offsets are marked only after a response |
| HTTP/gRPC service calls, statuses, TLS/mTLS | `internal/transport` | `httptest` (including HTTPS) and loopback gRPC (`testutil/mockmodel`); certificates are generated in the test (`testutil/testcerts`) |
| **HTTP and gRPC parity** | `internal/transport/parity_test.go` | one scenario table for both transports; outcomes must match |
| Bridge HTTP API | `internal/handlers`, `internal/middlewares`, `internal/app` | response codes, headers, 405/503 |
| Metrics and audit | `internal/observability` | a fake clock (`testutil/fakeclock`), a loopback ALS server (`testutil/mockals`) |
| Startup and shutdown | `internal/app` (`connect_test.go`) | `Connect` against a closed port: health works without Kafka, error exit, clean shutdown |
| Configuration | `internal/config`, `examples/` | YAML parsing compatible with earlier versions (snapshots in `internal/config/testdata/compat`), fail-closed TLS, example validity |

### Coverage
- **Coverage gate**: `make coverage-check` generates `coverage.out` and fails if total statement coverage is below `COVER_THRESHOLD` (**80%**) or any package is below `PACKAGE_FLOOR` (**60%**). `*.pb.go`, `testutil/`, `test/`, `cmd/` and `examples/` are excluded. Details: `docs/coverage.md`.
- HTML report: `make coverage-html && open coverage.html`.
- Override the thresholds locally: `COVER_THRESHOLD=50 PACKAGE_FLOOR=40 make coverage-check`.

### Manual end-to-end check
The end-to-end scenario (Kafka → service → Kafka, HTTP publishing, gRPC, TLS) is in "Local stand" and "Usage examples".

### Adding a test
Behavior expressible as a "WHEN … THEN …" scenario is first captured in `openspec/specs/<capability>/spec.md`, then covered by a test with the same meaning. External boundaries (Kafka, gRPC, the filesystem) are replaced through the seams listed in the `quality-gates` spec; do not add a seam just for an unreachable branch: remove the branch.

### gRPC stubs (protobuf)
The service contract is `internal/schemas/model/service.proto` (the `controller_kafka` package is part of the wire method name and must not change). Generated code is **committed** to `internal/transport/modelrpc`, so `go build` does not need `protoc`. The `protoc-gen-go` and `protoc-gen-go-grpc` plugins are pinned in `go.mod` as `tool` dependencies; only an installed `protoc` is required.

* `make proto` regenerates code after a `.proto` change;
* `make proto-check` compares the committed code with a fresh generation (part of `make ci`; skipped locally without `protoc`, an error when `CI` is set).

Other Makefile targets: `make build` (binary at `./bin/bridge-kafka-service` with the version, commit and build date embedded), `make fmt-check`, `make vet`, `make lint` (`golangci-lint`, rules in `.golangci.yml`), `make vuln` (`govulncheck`), `make image`, `make tools`; `make check` is kept as an alias of `make build ci`. Targets shared with other Go services live in `make/common.mk` (identical across repositories); CI (`.github/workflows/ci.yml`) runs `make ci`, then `make image BUILD_TAGS=release`.

View the HTML coverage report locally:

~~~bash
make coverage-html && open coverage.html
~~~

The history of decisions and the long-term rationale for the threshold scale are in
[docs/coverage.md](docs/coverage.md).

## How it works

### Features

* *At-least-once*: an offset is committed only after the record is processed, strictly in order within a partition (by default a service failure is recorded and does not block the partition; with `on_failure: retry` the record is retried until the service answers)

* *Two modes*: fast (a connection per record) and slow (a connection pool)

* *TLS*: secure connections to Kafka brokers

* *Metrics and logging*: full monitoring with structured logging

* *Retries*: exponential backoff on connection failures

* *Graceful shutdown*: correct handling of termination signals

* *Health checks*: a built-in health endpoint

* *Connection pool*: a per-pipeline connection limit (`max_conn` in slow mode, a built-in 512-call ceiling in fast mode) with connection reuse

### Components
#### Consumer:
Reads records from Kafka topics and forwards them to the business service over HTTP or gRPC

#### Producer:
Writes processed records back to Kafka topics

#### HTTP server:
Serves the health check and manual publish endpoints

#### Transaction logger:
Tracks every operation and records its metrics

#### Upstream (transport to the service):
A narrow `transport.Upstream` interface: "send a body and headers, get a body and headers or an error". Two transports are implemented, selected by `service.protocol`: HTTP (`transport/upstream_http.go`, JSON `POST`) and gRPC (`transport/upstream_grpc.go`, unary `BaseModelInterface.Predict`). Everything else (timeout, offset settle, tracking header propagation, response publishing, metrics) is shared by all transports, so a new transport only has to implement this interface (see the `message-routing` spec).

#### Flow
Sequence diagram of a record's full path: [docs/flow.md](docs/flow.md).

-> A record is consumed from a Kafka topic

-> Headers are processed and enriched with tracking data

-> The record is forwarded to the configured service (HTTP or gRPC)

-> The service response is published to the configured Kafka topic

-> Every operation is logged and its metrics recorded

-> Response handling (responsePublish)
* responsePublish carries the stream of service responses (HTTP or gRPC) back to Kafka topics:

* listens on the service response channel

* forwards successful responses to the configured Kafka topic

* handles errors and records the corresponding metrics

## Configuration

The application config is one YAML file: the `--config` flag, then `CONFIG_PATH` (environment, then the `.env` file in the working directory), otherwise `configs/config_app.yaml`. It is read once at startup. The Kafka client tunables `configs/config_consumer.yaml` and `configs/config_producer.yaml` are separate files read by the Kafka client as they are (sarama settings). One exception: offset auto-commit is always on, whatever `consumer.offsets.autocommit.enable` says, because settled offsets reach the broker only through it (`OffsetsAutoCommitInterval` sets how often). A group that has never committed starts from `offsets.initial`; after that a restart resumes after the last settled record.

### Kafka client tunables

`config_consumer.yaml` and `config_producer.yaml` hold sarama settings, written as sarama's field names in lower case (`net.maxopenrequests`, `consumer.group.session.timeout`). Both are loaded and checked once at startup, and only when their client is needed (the consumer file with an active consumer, the producer file with a producer):

* a missing or broken file, or a value of the wrong type, **stops startup** with an error naming the file. Nothing is retried, because a broken file cannot fix itself;
* an unknown key (a typo such as `maxopenrequest`, or a key in the wrong case such as `MaxOpenRequests`) is logged as a warning (`unknown tuning key net.maxopenrequest in configs/config_producer.yaml is ignored`) and ignored, so the sarama default stays;
* a sarama setting that cannot be expressed in YAML (callbacks and plug-ins such as `producer.partitioner` or `net.sasl.tokenprovider`) is logged as a warning and ignored.

A record is published only if it fits `producer.maxmessagebytes` in the producer file (1 MiB by default, keep it in line with the broker's `message.max.bytes`).

Consumer group defaults set by the bridge, which the consumer file overrides:

| Key | Default |
|---|---|
| `consumer.group.rebalance.strategy` | `range`; also `roundrobin`, `sticky`, `cooperative-sticky`. Any other value stops startup |
| `consumer.group.session.timeout` | `60s` |
| `consumer.group.heartbeat.interval` | `10s` |
| `consumer.maxprocessingtime` | `10s` |

Changing the rebalance strategy of a running group needs a restart of **every** instance of the group. Members with different strategies cannot join the same group: while old and new instances are mixed, joins fail with `InconsistentGroupProtocol`. Stop all instances, then start them with the new setting.

### Keys

Values are defaults or examples.

~~~yaml
log.level: info                         # debug | info (default) | warn | error; an unknown value stops startup
health.addr: 0.0.0.0:8081               # publish API and probes
debug.addr: ""                          # profiling address; empty (default) disables it, keep it on 127.0.0.1
audit.grpc_endpoint: ""                 # Envoy ALS receiver; audit is always written to <metrics file>.audit too
timeouts.shutdown: 10s                  # on SIGTERM: drain of HTTP requests and active service calls
retention.max_backups: 0                # backups kept per metric/audit file; 0 (default) keeps all
retention.max_age_days: 0               # delete backups older than this many days; 0 (default) never

kafka:
  brokers: kafka:29092,kafka2:29093     # a comma-separated string or a list
  version: 2.3.0                        # at least 0.10.2 with consumers, 0.11 with producers (0.x written as 0.11.0.0), else startup fails; empty = 2.7.0, a value that does not parse stops startup
kafka.tls.client.cert.path: certs/client_cert.pem
kafka.tls.client.key.path: certs/client_key.pem
kafka.tls.ca.path: certs/cacert.pem
kafka.tls.skip.insecure.verify: false

consumer:
  - topics: bridge.requests.1          # a comma-separated string or a list
    group: bridge.stand                   # required
    client_id: bridge-stand
    service:
      name: app-svc                     # required
      port: "8080"                      # default: <NAME>_SERVICE_PORT, otherwise 8080
      path: /predict                    # default /predict; http only
      protocol: http                    # http (default) | grpc
      max_conn: 0                       # >0 slow mode (pool), 0 fast mode
      timeout: 30                       # call timeout: seconds (30, 1.5) or a duration ("10s", "2m"); default 30s
      tls:                              # see "Service connection encryption"
        mode: none
        ca_path: ""
        cert_path: ""
        key_path: ""
        server_name: ""
        skip_verify: false
    response_topic: bridge.responses.1        # must be listed under producer
    on_failure: skip                    # skip (default) | retry; retry requires max_conn > 0 and response_topic to be a producer topic

producer:
  - topic: bridge.publish.out
  - topic: bridge.responses.1
~~~

Compatibility rules (kept from earlier versions, so existing files need few changes):
- keys are case-insensitive, and a nested key may be written nested (`health: {addr: ...}`) or dotted (`health.addr: ...`);
- a non-empty environment variable named after a top-level key overrides it: `HEALTH_ADDR`, `LOG_LEVEL`, `DEBUG_ADDR`, `AUDIT_GRPC_ENDPOINT`, `TIMEOUTS_SHUTDOWN`, `KAFKA_TLS_CA_PATH` and so on;
- `http.addr`, `pprof.addr` and `logLevel` are deprecated aliases of `health.addr`, `debug.addr` and `log.level`: an alias alone takes effect and logs one deprecation warning naming the key to use, while a key set together with its alias stops startup naming both;
- an unknown key stops startup with an error naming it (`unknown config key "..."`), reported together with every other configuration error.

### Environment variables
~~~shell
CONFIG_PATH: config file path (lower priority than the --config flag)
SYSTEM_ID: x-client-id for records that carry none (or SYSTEM_ID in the config file)
METRIC_PATH: metrics file directory (or the same key in the config file)
METRIC_FILE_NAME: metrics file name (or the same key in the config file)
LOG_PATH: run log directory (environment only)
LOG_FILE_NAME: run log file name (environment only)
<NAME>_SERVICE_PORT: port of service <NAME> (upper case, dashes as underscores) when service.port is unset
~~~

### Consumers

Each consumer has:

* topics: topics to consume (a stanza with no topics is disabled: it gets no Kafka client and is not counted in readiness)

* group: Kafka consumer group

* client_id: client identifier

* service: target service settings

* name: service name

* port: service port, a number from 1 to 65535, otherwise startup fails

* path: API path (`protocol: http` only); it must start with `/`, otherwise startup fails

* protocol: `http` (default) or `grpc`

* tls: encryption of the service connection (see below), **off** by default

* max_conn: maximum number of connections (`0` = fast mode, at most 512 concurrent calls; a negative value also selects fast mode)

* timeout: service call timeout, seconds (`30`) or a duration (`"10s"`); default 30 seconds

* response_topic: topic the service response is published to (without it the response is not published)

* on_failure: what to do when the service cannot be reached: `skip` (default) or `retry` (see below). With `retry`, a non-empty `response_topic` must be listed as a `producer:` topic: startup fails otherwise, because such a config would lose every response.

How stanzas fit together is checked at startup as well (stanzas without topics are ignored):

* **No response-topic cycle.** If response topics lead back into the stanzas' own input topics (`orders` → `orders`, or A reads `a` and answers to `b` while B reads `b` and answers to `a`), every response would be consumed again: startup fails and names the cycle. A chain without a cycle (A → `x` → B → `y`) is fine.
* **No shared group and topic.** Two stanzas with the same `group` and a common topic would split that topic's partitions between them, so each record would reach whichever service owns its partition: startup fails. For more concurrency, use one stanza with a higher `service.max_conn`. The same `group` with different topics is allowed, with a warning that the stanzas rebalance together; the same topic in different groups is a normal fan-out.
* **No empty entry.** A `consumer` or `producer` entry that is empty (`- ~`) stops startup with an error naming its position; to disable a consumer stanza, leave its `topics` empty.

### Service transport: HTTP or gRPC
`service.protocol` selects how a record reaches the service. Everything else (timeout, offset settle, `x-request-id`/`x-client-id` propagation, response publishing, metrics, `max_conn` modes) is **identical** for both protocols.

**gRPC** calls `/controller_kafka.BaseModelInterface/Predict` (contract: `internal/schemas/model/service.proto`):

| What | Where |
|---|---|
| Kafka record headers | request `details` (plus `x-request-id` in metadata) |
| payload | request `data` |
| response `data` | response body published to `response_topic` |
| response header/trailer metadata | response record headers |

gRPC statuses: `Unavailable`, `DeadlineExceeded`, `Canceled` (and connection errors) are transport failures, handled by `on_failure` (with `skip`: a failure metric, nothing published, the offset is settled). Any other non-OK status is **a service response**: a record with an empty body and `Grpc-Status`/`Grpc-Message` headers is published, and the event counts as successful (like HTTP 4xx/5xx). The call limit is 16 MiB (larger → `ResourceExhausted`); an HTTP service response is read up to the same 16 MiB, a larger one fails the call without a retry (see "Service failures"). Publishing the response has its own, smaller limit: see "Service failures". One connection per pipeline, established lazily: a service unavailable at startup does not block startup.

**Record headers on the way to the service.** Every record header is forwarded, with these exceptions:

* **Transport-controlled headers** never go into an HTTP call: the hop-by-hop headers (`Connection`, `Proxy-Connection`, `Keep-Alive`, `Proxy-Authenticate`, `Proxy-Authorization`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`), `Host`, `Content-Length` and `Content-Type`. They are left out without a warning (records published by another bridge carry them), and an HTTP call always has `Content-Type: application/json`: a record's own `Content-Type`, even a JSON subtype such as `application/vnd.api+json`, no longer overrides it. A gRPC call keeps gRPC's own content type; record headers, `Content-Type` included, travel in `details` as data.
* **Headers the transport cannot carry** are left out of the call with a warning naming the header and the request id, and the record is processed. HTTP cannot carry an invalid header name or a value with a control character other than tab (a line feed, for example). gRPC cannot carry text that is not valid UTF-8, and sends an `x-request-id` that is not printable ASCII in `details` only, not as metadata. The record keeps the header for audit and for the response record. This changes `skip` stanzas too: such a record used to fail, now it gets a response.
* **Tracking headers ignore letter case.** `X-Request-Id`, `X-Client-Id`, `X-Trace-Id` and `X-Request-Time` in a record are recognised in any case and forwarded under their lowercase names; if a record carries two spellings, the lowercase one wins.
* **Required headers.** The code keeps a list of headers a record must carry (`consts.RequiredHeaders`), empty in this release; the tracking headers are the candidates. A record without a listed header, or with one the transport cannot carry, is not sent: it is a failure settled after one attempt (see "Service failures"). A listed header gets no default value.

### Service connection encryption (`service.tls`)
Works the same for `http` (it becomes `https://`) and `grpc`. The section is optional: **without it (or without `mode`) the connection is not encrypted**, as before.

~~~yaml
service:
  name: model-svc
  port: "50051"
  protocol: grpc
  tls:
    mode: mtls              # none (default) | tls | mtls
    ca_path: certs/ca.pem   # server CA; empty means the system roots
    cert_path: certs/client.pem   # mtls only
    key_path: certs/client.key    # mtls only
    server_name: ""         # verify the certificate under another name
    skip_verify: false      # true disables certificate verification (a warning is logged)
~~~

| mode | Encryption | Client certificate |
|---|---|---|
| `none` | no | no |
| `tls` | yes, server verified | no |
| `mtls` | yes, server verified | yes (`cert_path` + `key_path` required) |

The config is validated at startup and **never falls back to a weaker setting**: an unknown `mode`/`protocol`, an unreadable or broken file, or `mtls` without a key stops startup with an error naming the consumer and the file. This differs from TLS to Kafka brokers, which degrades to insecure in dev mode.

Shutdown: the pipeline waits up to `timeouts.shutdown` (default 10s) for active calls, then aborts the rest **without** settling their offsets; such records will be redelivered.

### Service failures (`on_failure`)
A failure is only a transport failure: the service is unreachable, does not answer within `timeout`, or returns gRPC `Unavailable`/`DeadlineExceeded`/`Canceled`. Any HTTP status (4xx/5xx included) and any other gRPC status is a response and is published, never retried.

Some failures depend only on the record or its response, so repeating the call cannot help: an HTTP response body over 16 MiB, and a required header that is missing or that the transport cannot carry (see "Record headers on the way to the service"). They are attempted once, recorded as a failure and settled under **both** policies, so they never stall a `retry` pipeline.

| `on_failure` | On a failure |
|---|---|
| `skip` (default) | a failure metric, nothing published, the offset is settled; the record is not delivered again |
| `retry` | the offset is held and the call repeated with backoff (1s doubling up to 30s) until the service answers; the record stays in Kafka meanwhile |

`retry` requires `service.max_conn > 0`, otherwise startup fails. When all `max_conn` slots are retrying, the pipeline stops reading Kafka until the service recovers, so an outage loses nothing. Each failed attempt is logged as a warning with the attempt number and `failing_for`, how long the record has been failing; metrics count only the final outcome of a record. `GET /status` shows, per pipeline, how many records are retrying and how old the oldest retry is (see "Probes").

With `retry` a response is never dropped either: when the pipeline's publish queue (1000 responses) is full because Kafka writes are slow, the call waits for space instead of dropping the response and settling the offset (`skip` drops it with a warning and counts it as a failed publication in `bridge_kafka_service_response_service_failure_total`, with its egress audit entry). And when Kafka rejects the write itself, the response is republished with the same backoff (1s doubling up to 30s) until the broker accepts it: the offset is held and the upstream is not called again, so a broker outage stalls `retry` pipelines instead of losing their responses. Each pipeline has its own publish queue and writes up to 16 responses at once (`max_conn` if lower, so a `max_conn: 1` pipeline writes one at a time, in order), so a response topic that is broken or slow holds back only the pipeline that writes to it, never the others. Each failed attempt is logged as a warning; metrics and audit record only the final outcome. The exception is a non-transient publish error: a response larger than `producer.maxmessagebytes` (1 MiB by default; the broker's `message.max.bytes` applies as well), a record the broker rejects as invalid, or a feature the configured Kafka version lacks. It is attempted once, recorded as a failure and the offset is settled, like a poison record. To publish larger responses, raise `producer.maxmessagebytes` in `config_producer.yaml` together with the broker and topic limits. On shutdown the backoff wait ends and an unpublished response is not settled, so its record is redelivered after restart.

A record that fails 5 attempts **while other records of the same pipeline get answers** is treated as poison: a failure metric, a `poison` warning with its request id, the offset is settled. During an outage nothing is skipped. With `max_conn: 1` there are no other calls to compare with, so such a record is retried until the service answers; failures that depend only on the record (above) are not retried at all, so only network failures, such as a timeout on one heavy payload, can hold a `max_conn: 1` pipeline this way, and `/status` shows it.

Records are delivered at least once: with `retry`, records consumed after a retrying one are processed but not committed past it, so a restart or rebalance delivers them again. The service should be idempotent on `x-request-id`.

On a rebalance, a partition that moves to another instance is handed over cleanly: its records that are buffered but not yet sent to the service are not sent (the new owner gets them), retries for its records stop, and its in-flight calls get up to `timeouts.shutdown` (capped by `consumer.group.rebalance.timeout`, 60s by default) to finish and be committed before the partition is released. Only calls still running after that are delivered again.

## Logs and metrics

### Metrics
The app tracks four metric types per operation:

* Success Total: successful operations

* Failure Total: failed operations

* Requests Total: all attempts

* Duration Milliseconds: operation duration, from the start of *that* event to its close (independent of concurrent requests, excluding time in the metrics queue; an event without a start yields 0)

A response dropped from a full publish queue under `on_failure: skip` counts as a `response_service` failure. Earlier releases showed such drops only in the logs, so the metric may rise after an upgrade on pipelines whose queue saturates.

Metric names carry the service prefix `bridge_kafka_service_` (MET-2). The rename from the former prefixes (MET-6):

| Old name | New name |
|---|---|
| `controller_kafka_<event>_{success_total,failure_total,requests_total,duration_ms}` | `bridge_kafka_service_<event>_{success_total,failure_total,requests_total,duration_ms}` |
| `bridge_service_pod_cpu_usage` | `bridge_kafka_service_pod_cpu_usage` |
| `bridge_service_pod_memory_usage` | `bridge_kafka_service_pod_memory_usage` |

The `<event>` part is one of `consume`, `produce`, `request_service`, `response_service`. The old names are not written in parallel — a metric file with both spellings would double every counter that consumers sum by event. Before this version is deployed, dashboards and alerts that read the metric file switch their queries to the new names (or match both).

### Logging
* Structured logging with Zerolog

* Separate audit logs for business events

* DNS lookups logged at debug level

Log levels:

*debug*: detailed debugging information

*info*: normal operation

*warn*: warnings

*error*: errors

## Probes

| Path | Purpose | Response |
|---|---|---|
| `GET /healthz` (and the former `/health`) | liveness: the process is running; does not depend on Kafka | `200 {"status":"running"}` |
| `GET /readyz` | readiness: the required producer is registered, every active consumer client has initialized, and shutdown has not begun | `200 {"status":"ready"}` / `503 {"status":"not ready","reasons":[...]}` |
| `GET /status` | details for humans (`consumers_total` counts stanzas with topics) | `200 {"ready":..., "producer_ready":..., "consumers_up":..., "consumers_total":..., "pipelines":[...]}` |

Configure the application container's Kubernetes readiness probe to call `/readyz` on `health.addr`. Istio can forward this probe through its sidecar; sidecar readiness alone cannot determine whether the application's Kafka clients have initialized. The application supplies the readiness result, and Kubernetes uses it to control Service traffic. This does not stop Kafka consumption, which the application controls separately.

The Kafka readiness checks track client initialization, not continuous broker reachability. Pause and upstream retries do not change readiness. Internally, `runtimeState` owns this shared state together with pipeline status and shutdown tracking; readiness is one view of it.

`pipelines` has one entry per stanza with topics, in configuration order. Readiness does not depend on it:

~~~json
{"group":"bridge-group","topics":["bridge.requests.1"],
 "retrying_calls":1,"retrying_publications":0,"oldest_retry_seconds":180,"uncommitted":41,"paused":false}
~~~

* `retrying_calls`: records retrying their call to the service; `retrying_publications`: responses retrying their write to the response topic (`on_failure: retry`)
* `oldest_retry_seconds`: age of the oldest of those since its first failed attempt, `0` when none
* `uncommitted`: records handed out by the current claims and not yet committed, including those processed but held behind an earlier record (they are delivered again after a restart)
* `paused`: whether consumption is paused by `SIGUSR1` (one state for the whole process)

**No authentication.** `POST /api/v1/publish` and the probes on `health.addr` accept any client that can reach the port: a client can publish to every topic listed under `producer:`. Expose `health.addr` only to trusted clients (bind address, network policy, or an authenticating gateway in front). The listener closes a connection whose request is not fully received within 30 seconds or that stays idle for 120 seconds; a publish that waits for Kafka is never cut off while it answers.

Profiling (`net/http/pprof`) is never served on this port: it runs only on `debug.addr` (off by default); a non-loopback `debug.addr` is logged as a warning.

### Health check
~~~shell
text
GET /health

Response:

json
{
"status": "running"
}
~~~

## Image and deployment

One `Dockerfile`, two images (build arguments; without a value docker takes them from environment variables of the same name):

| Argument | Purpose | Default |
|---|---|---|
| `BUILDER_IMAGE` | Go image that builds the binary; use stable Go 1.27.x with a patch version at least the `go` directive in `go.mod` | `golang:1.27.3` |
| `RUNTIME_IMAGE` | lightweight runtime image: the binary is static (`CGO_ENABLED=0`, time zones embedded) and needs only `sh` and the system CA roots | `alpine:3.21` |
| `BUILD_TAGS` | extra Go build tags, comma-separated, appended to `timetzdata`; the release image is built with `release` (platform spec `lang-go/platform-go-quality`) | empty (dev) |

The image runs as an unprivileged user (uid 1001). Configuration is never baked into the image: mount it at `./configs`.

~~~bash
make image                                                      # version, commit and build date embedded
# other base images
docker build --build-arg BUILDER_IMAGE=golang:1.27.3 --build-arg RUNTIME_IMAGE=alpine:3.21 -t bridge-kafka-service .
~~~

> **Changed:** the `runtime-dev` target (configs baked into the image) is removed; the compose stand mounts `configs/` instead. Other base images are passed with `BUILDER_IMAGE`/`RUNTIME_IMAGE` as above.

The working directory is `/go/src/bridge-kafka-service`, the port is `8081`. Certificates and configs are mounted as volumes (see the example below).

Example deployment.yaml:

~~~yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bridge-kafka-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: bridge-kafka-service
template:
  metadata:
  labels:
    app: bridge-kafka-service
  spec:
  containers:
    - name: bridge-kafka-service
      image: bridge-kafka-service:latest
      ports:
        - containerPort: 8081
      env:
        - name: SYSTEM_ID
          valueFrom:
            configMapKeyRef:
              name: bridge-kafka-service-config
              key: system_id
      volumeMounts:
        - name: config-volume
          mountPath: /go/src/bridge-kafka-service/configs
        - name: certs-volume
          mountPath: /go/src/bridge-kafka-service/certs
    volumes:
    - name: config-volume
      configMap:
        name: bridge-kafka-service-config
~~~

### Startup, shutdown and signals

* **The HTTP server and `/health` start immediately**, without waiting for Kafka. The producer and consumers connect in the background with exponential backoff and jitter until shutdown, without a five-minute retry window. Until the producer connects, `POST /api/v1/publish` returns `503`; consumers start reading only once the configured producer is ready. A consumption-only deployment that does not publish should list no `producer:` entries.
* An unreachable broker leaves the process **up and not ready**: `/healthz` answers 200 and `/readyz` answers 503 naming the clients still initializing. Connection attempts continue, and readiness becomes 200 when the required clients initialize. `SIGTERM` interrupts retry backoff and starts normal shutdown.
* If a subscription cannot start after its client was created (the service connection cannot be set up, the consumer group cannot be created), the process also **exits with code 1**, and the error names the stanza's topics; it never keeps running while reporting ready without reading. The configuration causes of this are rejected at startup instead: `kafka.version` below 0.10.2 with active consumers, an invalid `service.name`/`service.port` of any stanza, an HTTP `service.path` that does not start with `/`.
* **Configuration errors are reported together.** Startup runs every check (Kafka setup, service settings, how stanzas fit together, Kafka tuning files) before stopping, so one run lists every problem, each with its stanza and rule. Only a file that cannot be parsed at all stops at the first error. `--check-config` runs the same checks without starting (see "Run").
* `SIGTERM` / `SIGINT`: graceful shutdown. The service first turns **draining**: `POST /api/v1/publish` answers `503` (`service is shutting down`) and `/readyz` answers `503` with the reason `shutting down`, while `/healthz`, `/health` and `/status` keep answering until the drain ends — the HTTP server closes last. HTTP intake and Kafka reading stop; in-flight upstream calls get up to `timeouts.shutdown` in total to finish, and every record that finishes in that time is committed before the consumer group session is released, so it is not redelivered. Records still unfinished after it, and buffered records not yet sent to the service, are not committed and will be redelivered (at-least-once). Then the metrics and audit queues are flushed and the producer and files are closed.
* `SIGUSR1` toggles pause/resume of reading **for all** consumers of the process. The pause is one state of the process: a partition the process receives while paused (after a rebalance, for example) is paused as soon as its claim starts, and records Kafka has already delivered are held, not sent to the service, until the next `SIGUSR1`. Records already sent finish as usual. `GET /status` shows `paused` for every pipeline.

Kubernetes/OpenShift probe example:
~~~yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8081 }   # /health still works
readinessProbe:
  httpGet: { path: /readyz, port: 8081 }
~~~

## Platform

The service follows the platform (layers `core`, `env-public`, `lang-go`), read through the `platform` store; the release it follows is recorded in `.platform-standards.yaml` (`aligned_with`). The binding workflow is `core/platform-sdd-workflow`.

The service is aligned with `v3.0.0`. Its module minimum and default builder are Go 1.27.3; build and test toolchains must use stable Go 1.27.x with a patch version at least the module minimum.

Register the platform clone once per machine with `openspec store register <platform-clone> --id platform`; `openspec doctor` verifies the reference. The store's `.openspec-store/store.yaml` identifies the source repository. This service consumes it through `references` in `openspec/config.yaml` and does not need its own store declaration.

* **Strict keys:** an unknown configuration key fails startup and `--check-config` with the key named. Run `bridge-kafka-service --check-config` on every deployed configuration before upgrading: files that loaded with warnings stop loading.
* **Deprecated key aliases:** `http.addr`, `pprof.addr` and `logLevel` still work and are logged once as deprecated; each is used only when `health.addr`, `debug.addr` or `log.level` is unset, and a key set together with its alias fails validation.
* The other legacy wire identifiers (the headers `x-bridge-kafka-produce-topic` and `x-bridge-kafka-header-*`, the ALS cluster and fallback values, the gRPC method `/controller_kafka.BaseModelInterface/Predict`) are frozen by spec `project-identity` until an explicit migration change replaces them.
* Deviations from platform requirements are approved exemptions in the platform registry (`exemptions/bridge-kafka-service.yaml` in the platform repository), never lists in this repository.

## Project layout

~~~text
bridge-kafka-service/
├── cmd/
│   └── bridge-kafka-service/  # Entrypoint
├── internal/
│   ├── app/             # App wiring, Kafka connection, HTTP server
│   ├── codec/           # Payload and header normalization
│   ├── config/          # Config loading (backward-compatible format), validation, Kafka and service TLS
│   ├── consts/          # App constants
│   ├── handlers/        # HTTP handlers
│   ├── logging/         # Platform log format and secret masking
│   ├── middlewares/     # HTTP middleware
│   ├── observability/   # Metrics logging and Envoy ALS audit
│   ├── router/          # Kafka consumer/producer lifecycle
│   ├── schemas/         # Data structures
│   └── transport/       # Service calls: the Upstream interface (HTTP and gRPC), shared settle/response publishing; modelrpc/ holds generated gRPC stubs
├── configs/             # Config files (config_compose.yaml, config_consumer.yaml, config_producer.yaml; config_app.yaml.example template)
├── docs/                # Flow diagram and coverage gate
├── examples/            # Ready-made example configs (HTTP, gRPC, gRPC+mTLS)
│   └── grpc-model/      # Example gRPC service (Predict) for checking protocol: grpc
├── make/common.mk       # Makefile part shared with other Go services
├── scripts/             # coverage.sh, gen-demo-certs.sh
├── certs/               # TLS certificates (not committed; demo: scripts/gen-demo-certs.sh)
├── openspec/            # Behavior specs (specs/) and change history
└── testutil/            # Test stubs: mocksarama, mockals, mockmodel, testcerts, fakeclock, metricsfile
~~~

## API

### Publish a record
~~~shell
text
POST /api/v1/publish
~~~
Headers:
~~~
x-bridge-kafka-produce-topic: target topic (required)

x-request-id: request id (optional, generated otherwise)

x-request-time: timestamp (optional)

x-kafka-key: partitioning key (optional)

x-bridge-kafka-header-<name>: becomes Kafka header <Name> (the prefix is dropped)
~~~
A header listed as required (`consts.RequiredHeaders`, empty in this release) must be present, and is carried onto the record under its lowercase name.

Response codes: `422` missing topic header / missing required header / invalid body (including more than one JSON value), `415` Content-Type is not `application/json`, `503` the producer is not configured or not connected yet.

Request body:
~~~
json
{
"key": "value",
"data": {"any": "json"}
}
~~~
Response:
~~~
json
{
"result": "success",
"detail": "Message published successfully"
}

~~~

## Usage examples

The HTTP API responses below are captured from the real handler; the gRPC, TLS/mTLS and parity scenarios are covered by automated tests. Kafka CLI and `docker compose` commands are not checked automatically: they are standard `cp-kafka 7.4` (Kafka 3.4) tools; times, `Date`, `Content-Length` and header order in your responses will differ.

### Bridge HTTP API
~~~bash
# health
curl -s http://localhost:8081/health
# {"status":"running"}

# publish to Kafka (the topic from the header must be listed under producer)
curl -i -X POST http://localhost:8081/api/v1/publish \
  -H 'x-bridge-kafka-produce-topic: bridge.publish.out' \
  -H 'Content-Type: application/json' \
  -H 'x-request-id: demo-001' \
  -H 'x-kafka-key: user-42' \
  -H 'x-bridge-kafka-header-tenant: acme' \
  -d '{"params":[5,10]}'
~~~
Response:
~~~text
HTTP/1.1 200 OK
Content-Length: 21
Content-Type: application/json
Date: Sat, 10 Oct 2026 13:28:45 GMT
X-Request-Id: demo-001
X-Request-Time: 2026-10-10 13:28:45Z
X-Trace-Id: demo-001

{"result":"success"}
~~~
Topic `bridge.publish.out` receives a record with **value** `{"params":[5,10]}`, **key** `user-42` (from `x-kafka-key`) and **headers** `x-request-id`, `x-request-time`, `x-trace-id`, `x-kafka-key` and `Tenant: acme` (the `x-bridge-kafka-header-` prefix is dropped; the name arrives canonicalized by `net/http`). Without `x-request-id` the bridge generates a UUID and returns it in `X-Request-Id`.

Read it back:
~~~bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic bridge.publish.out --from-beginning --max-messages 1 \
  --property print.headers=true --property print.key=true
~~~

**Errors** (bodies and codes as in real responses):

| Situation                                | Code | Body                                                                                                           |
|------------------------------------------|---|----------------------------------------------------------------------------------------------------------------|
| no `x-bridge-kafka-produce-topic` header | 422 | `{"result":{},"status":"failure","detail":"RequestHeader not contains header 'x-bridge-kafka-produce-topic'"}` |
| empty body                               | 422 | `{"result":{},"status":"failure","detail":"RequestBody body must not be empty"}`                               |
| invalid JSON                             | 422 | `{"result":{},"status":"failure","detail":"RequestBody body contains badly-formed JSON"}`                      |
| several JSON values in the body          | 422 | `RequestBody body must only contain a single JSON object` (text/plain)                                         |
| body over 2 MB                           | 422 | `{"result":{},"status":"failure","detail":"RequestBody body must not be larger than 2MB"}`                     |
| `Content-Type` is not `application/json` | 415 | `Content-Type header is not application/json` (text/plain)                                                     |
| topic not configured as a producer       | **200** | `{"result":"failure: no producer"}` (kept for compatibility)                                                   |
| method is not `POST`                     | 405 | `Method Not Allowed` + `Allow: POST`                                                                           |
| producer not connected yet               | 503 | `Publish functionality is not ready: Kafka producer is not connected`                                          |
| no producers in the config               | 503 | `Publish functionality is disabled: no Kafka producers configured`                                             |

Every response except 405/503 returns `X-Request-Id`, `X-Trace-Id` and `X-Request-Time`.

### Calling the HTTP service directly
This is the request the bridge sends to the service (body: the record payload; headers: the record headers), here against an HTTP upstream listening on `localhost:8000`:
~~~bash
curl -i -X POST http://localhost:8000/predict \
  -H 'Content-Type: application/json' -H 'x-request-id: direct-001' \
  -d '{"params":[5,10]}'
# HTTP/1.1 200 OK
# x-request-id: direct-001
# ...the headers and body your service returns...
~~~

### gRPC service
The example service is `examples/grpc-model`: it implements `controller_kafka.BaseModelInterface/Predict` and echoes the received data.
~~~bash
go run ./examples/grpc-model -addr 127.0.0.1:50051
# example-grpc-model listening on 127.0.0.1:50051 (plaintext)
~~~
Direct call (requires [grpcurl](https://github.com/fullstorydev/grpcurl); `data` is base64 of `{"params":[1,2,3]}`):
~~~bash
grpcurl -plaintext -import-path internal/schemas/model -proto service.proto \
  -d '{"details":{"x-request-id":"abc-123"},"data":"eyJwYXJhbXMiOlsxLDIsM119"}' \
  127.0.0.1:50051 controller_kafka.BaseModelInterface/Predict
~~~
Response (the `data` field is base64 as well and decodes to `{"echo":{"params":[1,2,3]},"request_id":"abc-123","service":"example-grpc-model"}`).

Through the bridge: run the bridge on the host (see "Quick start") with `examples/config_grpc.yaml` and send a record:
~~~bash
printf 'x-request-id:g-001\t{"params":[1,2,3]}\n' | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic demo.grpc.in --property parse.headers=true
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic demo.grpc.out --from-beginning --max-messages 1 --property print.headers=true
# x-request-id:g-001,x-client-id:example-grpc-model,...	{"echo":{"params":[1,2,3]},"request_id":"g-001","service":"example-grpc-model"}
~~~
The same scenario with a service error: a status such as `InvalidArgument` is not lost; the output topic receives a record with an empty value and the headers `Grpc-Status: InvalidArgument`, `Grpc-Message: ...`.

### TLS and mTLS
~~~bash
scripts/gen-demo-certs.sh                       # certs/demo/{ca,server,client}.{pem,key}; demo only

# server with mutual authentication
go run ./examples/grpc-model -addr 127.0.0.1:50051 \
  -tls-cert certs/demo/server.pem -tls-key certs/demo/server.key -client-ca certs/demo/ca.pem
# example-grpc-model listening on 127.0.0.1:50051 (mutual TLS)

# bridge with a client certificate
./bridge-kafka-service --config examples/config_grpc_mtls.yaml
~~~
Client modes (`service.tls.mode`): `none` no encryption (default), `tls` encryption + server verification, `mtls` plus a client certificate. For an HTTP service the same keys give `https://`:
~~~yaml
service: { name: app-svc, port: "8443", path: /predict, tls: { mode: tls, ca_path: certs/demo/ca.pem } }
~~~
If a certificate file is missing or `mtls` has no key, the bridge **does not start** and names the consumer and file at fault. Without `mode` the connection is not encrypted.

If the server requires a client certificate but the bridge config says `tls` or `none`, the handshake fails: a transport failure (log: `Send to service ... status: failure`), nothing is written to the output topic, and with `on_failure: skip` the offset is settled, so the record does not block the partition (with `retry` it is retried until the config is fixed or it is skipped as poison).

### Audit sink (Envoy ALS)
Any Envoy ALS v3 receiver works as a sink: point `audit.grpc_endpoint` at it (in the stand config: `audit-grpc-svc:50053`; on the host: `127.0.0.1:50053`). Audit is **always** written to `app-metrics.log.audit` next to the metrics (JSON lines), even when the sink is unavailable:

* **The audit file is complete** for the backups the service keeps (all of them by default). No entry is ever dropped from it; entries are written in order and flushed to disk at least once a second (and whenever the service is idle), so a machine crash loses at most the last second.
* **ALS delivery is best-effort and never slows the service.** Entries are sent after they are in the file, in batches over one stream per batch. If the receiver is slow or down and its queue fills, entries are skipped for ALS only, and a warning reports how many (`N audit entries were not delivered to the ALS endpoint (the audit file has them)`, at most once a minute). On shutdown the remaining entries get one attempt of at most 3 seconds.
* **Rotation.** `app-metrics.log` and `app-metrics.log.audit` rotate at 100 MB into compressed backups next to them (`app-metrics-<time>.log.gz`, `app-metrics.log-<time>.audit.gz`).
* **Retention.** By default the service never deletes a backup, so nothing is lost, and cleaning up is the operator's job (a log shipper that removes what it has collected, or a cron job): without it the disk keeps filling. To let the service bound it, set `retention.max_backups` (keep at most N backups of each file) and/or `retention.max_age_days` (delete backups older than N days). Backups beyond the limits are deleted when the file is first written after a start and at every rotation; the current files are never deleted, so with `retention.max_backups: N` each file takes at most about `(N + 1) × 100 MB`. A non-zero limit is stated in one log line at startup, because audit entries in deleted backups are gone.
* **Permissions.** Both files and their backups are readable and writable by the service user only (`0600`); the audit holds full request and response bodies. Files created by earlier versions are switched to `0600` at startup.

### What to look for in metrics and logs
~~~bash
# 4 metrics per event: *_success_total, *_failure_total, *_requests_total, *_duration_ms
grep '"rqUId":"demo-001"' app-metrics.log
# events: consume, request_service, response_service (from Kafka) and produce (HTTP /publish)
~~~
`*_duration_ms` is the duration of that event alone (from its start to its close).

## Security

TLS configuration
* Client certificate authentication
* Custom CA certificates
* Configurable certificate paths
* An option to skip certificate verification

### Header security
* Binary headers are base64-encoded automatically
* Transport-controlled headers of a record (hop-by-hop, `Host`, `Content-Length`, `Content-Type`) never reach an HTTP service, and values a transport cannot carry are left out with a warning
* Request-ID validation and generation
* System identifier check

## Troubleshooting

Common problems:

* Certificate errors: check TLS certificate paths and permissions

* Connection timeouts: check network connectivity to the Kafka brokers

* Memory usage: monitor the number of concurrent service requests (capped at 512 per fast-mode pipeline; set `service.max_conn` for a lower limit)

* Profiling: enabled only by `debug.addr` in the config (a separate port; keep it on 127.0.0.1)

* DNS resolution: enable debug logging to diagnose DNS problems

To diagnose, enable debug logging in the config:

~~~yaml
log.level: debug
~~~
