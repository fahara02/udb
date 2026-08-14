# UDB v0.5.7 PublishCDC releases admission limits before streaming work begins

Date: 2026-08-14
Status: core lifetime correction implemented; served cap test pending
Affected path: `DataBroker.PublishCDC`

## Summary

The CDC channel permit, inflight metric, and configured channel deadline cover
only construction of the `PublishCDC` stream object. They are released before
the server begins its permanent replay/poll/delivery loop. A tenant can therefore
open arbitrarily many long-lived streams without consuming the configured
global, tenant, or project CDC concurrency budget.

## Confirmed served path

- `publish_cdc_inner` wraps only the call to `cdc_engine.stream_cdc(...)` in
  `execute_with_channel_scoped(OperationChannel::Cdc, ...)`.
- `stream_cdc` subscribes to the broadcast channel and returns a boxed lazy
  `try_stream`; its journal queries and infinite loop execute later, when tonic
  polls the returned object.
- `execute_with_channel_scoped` owns `_permit`, increments inflight, and applies
  the CDC timeout only until its future returns `Ok(stream)`. It then decrements
  inflight and drops `_permit` before `publish_cdc_inner` puts the stream in the
  gRPC response.
- There is no separate per-stream budget guard like LiveQuery's global and
  per-tenant stream budget.

## Consequences

- One credential can create unbounded journal pollers, broadcast receivers,
  dedup sets, and client sockets while channel metrics report no CDC work in
  flight.
- Configured CDC concurrency and deadline controls do not protect PostgreSQL or
  broker memory from slow or abandoned subscribers.
- Fair-share admission is charged to opening a cheap object, not to the costly
  lifetime it was intended to govern.

## Required correction

- Move an owned admission/budget guard into the returned stream so it remains
  alive until the client disconnects or the stream terminates.
- Add explicit global, per-tenant, per-project, and per-credential stream caps;
  decide separately whether event delivery also consumes a rate budget.
- Enforce maximum idle and total stream lifetimes, with keepalive behavior that
  does not turn a silent client into a permanent database poller.
- Record stream-duration, active-stream, replay-scan, and disconnect-reason
  metrics from the actual stream lifecycle.
- Add a served concurrency test that holds the configured number of streams open
  and verifies the next subscription is rejected until one guard is dropped.

## Verification log

- Traced ownership of the channel permit and the lazy async-stream execution
  boundary through the served handler.
- The returned response stream now owns the scoped global/tenant/project channel
  permit, inflight metric, and duration measurement until completion, error,
  deadline, or client cancellation.
- A server-enforced maximum lifetime is clamped to verified credential expiry,
  preventing abandoned subscribers from remaining permanent pollers.
- PostgreSQL and Kafka-enabled library checks passed and the three lifetime-
  budget unit regressions passed. A served saturation/drop-release concurrency
  test and any dedicated per-credential cap remain open.
