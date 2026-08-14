# UDB v0.5.7 Kafka topic policy is startup-only and fails open

Date: 2026-08-14
Status: correction implemented; live multi-replica verification pending
Affected runtime: CDC engine construction, publication, subscription, DLQ retry policy

## Summary

The CDC engine loads topic policies once into a plain vector before starting.
Load failure is warning-only and the engine starts with an empty vector, whose
documented meaning is allow all. No reload caller exists after startup, so later
disable/ownership/schema/retry changes do not reach the publisher or subscriber
until process restart.

## Confirmed served path

- Startup calls `engine.load_topic_policies()` once and logs a warning on error,
  then always spawns the tailer.
- `topic_policies: Vec<TopicPolicy>` is stored inside the engine; no ArcSwap,
  revision watch, polling reload, or SIGHUP caller was found.
- Empty policy vector disables publisher allowlist enforcement and makes
  subscription classification unaware of policy-owned tenant topics.
- Publisher schema/project checks and DLQ retry limits read the cached vector.
- `EnqueueOutboxEvent` independently queries the database on each call, so its
  admission world can be newer than the publisher's world.

## Consequences

- A transient startup policy-read failure turns a configured deny-by-default
  deployment into open Kafka publication for the lifetime of that process.
- Disabled/reassigned topics continue using stale ownership and schema/retry
  rules, while newly admitted topics can be rejected by the old publisher cache.
- Replicas started at different times can enforce different Kafka and CDC-stream
  policy worlds.

## Required correction

- Fail closed/readiness when a configured policy store cannot be loaded.
- Publish monotonic policy revisions and atomically swap one shared immutable
  matcher used by ingress, publisher, subscription, and DLQ logic.
- Surface loaded revision/age in CDC health and require replica convergence.
- Add startup-failure, live-disable, ownership-change, and multi-replica reload
  tests with real outbox/Kafka traffic.

## Verification log

- Source trace completed across engine construction, load error handling, policy
  storage/callers, publisher/subscription/DLQ consumers, and live ingress query.
- The engine now strictly decodes all rows into one immutable ArcSwap generation,
  polls at a resolved configuration interval, retains disabled rows, refuses CDC
  startup on initial load failure, and swaps an unavailable fail-closed sentinel
  on runtime reload failure.
- Publishers retain pending rows while policy state is unavailable; subscribers
  terminate instead of using an indefinitely stale allow. Generation, age, and
  availability are exposed as Prometheus gauges.
- PostgreSQL and Kafka-enabled library checks and focused reload/disable/scope
  unit tests passed. Live-disable and replica-convergence tests remain for CI.
