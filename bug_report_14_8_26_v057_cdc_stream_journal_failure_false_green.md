# UDB v0.5.7 CDC stream journal failures are reported as empty progress

Date: 2026-08-14
Status: PublishCDC corrected; LiveQuery resume half remains open
Affected paths: `DataBroker.PublishCDC`, LiveQuery durable resume

## Summary

The broker's durable CDC stream converts journal query/decode failures into an
empty event batch. The public stream remains open and keeps polling, so a client
cannot distinguish “no changes” from “UDB cannot read the durable Kafka publish
journal.” LiveQuery uses a second helper with the same empty-vector contract and
can move from a failed resume attempt into snapshot plus process-local live
broadcast without reporting that missed history was not recovered.

## Confirmed served path

- `DataBroker.PublishCDC` delegates directly to `CdcEngine::stream_cdc` and maps
  only yielded `Status` values; the journal poll helper itself cannot return one.
- `cdc_journal_poll` returns `Vec<CdcEnvelope>`. A SQL stream error is logged and
  skipped, after which the caller treats the short/empty vector as caught up.
- The live loop repeats the same helper every 500 ms. A durable-store outage
  therefore leaves the gRPC stream connected and silent instead of retryably
  failing it.
- Row field decode failures are also skipped. Once `event_id` and `published_at`
  decode, the cursor advances before topic, payload, and partition-key decoding;
  a later decode failure permanently moves this subscription past that row.
- `journal_replay_for_scope` likewise returns a vector and converts page-fetch
  failure to a warning plus partial/empty replay.
- LiveQuery calls that helper before its snapshot and delta task. It cannot tell
  replay failure from a genuine empty gap and continues on the node-local
  broadcast feed, which cannot recover changes emitted by another replica.

## Consequences

- Consumers can remain apparently healthy while missing security, entity, or
  native-service events already accepted by Kafka.
- A LiveQuery reconnect can silently omit part or all of its durable gap.
- Client retry/reconnect logic never runs because the stream returns no status.
- There is no reliable metric separating an idle stream from journal failure.

## Required correction

- Make journal polling/replay return `Result`, preserving partial-batch safety
  without converting database or required-field decode errors into success.
- Terminate `PublishCDC` with a retryable `UNAVAILABLE` status on durable journal
  failure; do not advance the cursor until a complete row is decoded and admitted
  or intentionally filtered.
- Fail a LiveQuery resume before returning its snapshot when the requested gap
  cannot be read, or return an explicit resume-watermark/error frame that forces
  the client to re-establish a safe snapshot boundary.
- Add live fault tests for query failure at initial replay and during journal
  backstop polling, plus a cross-replica LiveQuery reconnect test.

## Verification log

- Source trace completed from both public handlers through the shared journal
  helpers, cursor advancement, and stream loops.
- `PublishCDC` journal polling now returns `Result`, terminates on query/decode
  failure, and decodes the full durable row before cursor advancement.
- PostgreSQL and Kafka-enabled library checks passed for the corrected path.
- `journal_replay_for_scope` and the LiveQuery resume contract still require the
  same fail-closed conversion and cross-replica fault test; this report therefore
  remains partially open.
