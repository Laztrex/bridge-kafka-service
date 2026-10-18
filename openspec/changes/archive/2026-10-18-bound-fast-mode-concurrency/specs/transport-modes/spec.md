# Spec Delta

## MODIFIED Requirements

### Requirement: Mode selection from configuration
The system MUST select between fast mode and slow mode based solely on the value of `service.max_conn` on the consumer stanza. A value greater than zero selects slow mode; zero or any negative value selects fast mode.

#### Scenario: Fast mode chosen
- **WHEN** a pipeline starts with `service.max_conn: 0` (or unset)
- **THEN** each consumed record triggers an independent outbound request, limited only by the fast-mode in-flight ceiling

#### Scenario: Slow mode chosen
- **WHEN** a pipeline starts with `service.max_conn: 10`
- **THEN** at most ten concurrent outbound requests are in flight at any time for that pipeline

## ADDED Requirements

### Requirement: Fast-mode in-flight ceiling
In fast mode the system MUST keep at most 512 upstream calls in flight per pipeline. When the ceiling is reached, the pipeline MUST NOT start another call or take another record from Kafka until a call finishes. On shutdown, a dispatch waiting for the ceiling MUST return immediately. The ceiling MUST be stated in the fast-mode startup log line.

#### Scenario: Burst below the ceiling
- **WHEN** 100 records arrive in fast mode while the upstream is slow
- **THEN** 100 calls are in flight concurrently

#### Scenario: Catch-up burst above the ceiling
- **WHEN** 2000 records are available in fast mode and the upstream answers slowly
- **THEN** no more than 512 calls are in flight at any time, and the remaining records are processed as calls finish

#### Scenario: Shutdown at the ceiling
- **WHEN** the pipeline is stopped while 512 calls are in flight and another record waits for a free slot
- **THEN** the waiting dispatch returns immediately and that record is not settled

### Requirement: Upstream connection reuse
For an HTTP upstream the system MUST keep up to the pipeline's concurrency limit (`service.max_conn` in slow mode, the fast-mode ceiling in fast mode) of idle connections to the upstream, so that a steady number of concurrent calls reuses connections instead of opening a new one per call. Pipelines MUST NOT share an HTTP connection pool.

#### Scenario: Steady concurrency reuses connections
- **WHEN** a slow-mode pipeline with `max_conn: 8` makes 200 calls with 8 in flight at a time to a keep-alive HTTP upstream
- **THEN** the upstream sees a small bounded number of new connections over the whole run (the limit plus a small transient overshoot of the client's dialler), far fewer than the number of calls

#### Scenario: Separate pools per pipeline
- **WHEN** two pipelines call two different upstreams
- **THEN** idle connections of one pipeline are not counted against or reused by the other
