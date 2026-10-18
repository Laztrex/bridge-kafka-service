# Spec Delta

## MODIFIED Requirements

### Requirement: Response decoupling from request path
The system MUST funnel every successful upstream response into a shared response-publication channel so that publishing failures, slow brokers, or pause/resume toggles never stall consumption. The exception is a stanza with `on_failure: retry`: there a saturated channel MUST hold the upstream call until space frees, so the response is never dropped and backpressure stops consumption instead.

#### Scenario: Publication channel saturated
- **WHEN** the response channel is full because publishing is slow and the stanza's failure policy is `skip`
- **THEN** newly produced responses are dropped with a warning logged rather than blocking consumption, and the originating message is still settled

#### Scenario: Publication channel saturated in retry mode
- **WHEN** the response channel is full and the stanza sets `on_failure: retry`
- **THEN** the response waits for space in the channel, is not dropped, and the originating message is settled only after it is published

#### Scenario: Shutdown while waiting for the publication channel
- **WHEN** the pipeline stops while a response of a `retry` stanza waits for space in the channel
- **THEN** the wait ends within the shutdown timeout, the response is not published by that call and the originating message is not settled, so it is redelivered after restart

#### Scenario: Responses drained without producers
- **WHEN** consumers are active, no producers are configured and a `skip` stanza produces a response for its `response_topic`
- **THEN** the response-publication loop still runs, fails each response event with a logged "no producer" error, and settles it, so the channel never fills with unconsumed events

#### Scenario: No response topic configured
- **WHEN** a pipeline has no response topic
- **THEN** responses bypass the publication channel entirely
