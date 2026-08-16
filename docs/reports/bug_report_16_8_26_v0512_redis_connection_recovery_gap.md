# Bug report: Redis connections could remain permanently poisoned

## Release

- Target: v0.5.12
- Evidence: post-release benchmark workflow run `31956785039`
- Severity: release-blocking availability and error-classification defect

## Observed failure

The PHP sweep was the last and longest SDK sweep against one broker/Redis lifetime. Its Redis
seed and earlier cache operations succeeded, then the broker logged `Multiplexed connection
driver unexpectedly terminated`. Every later `CacheSet` used the same cached dead connection and
failed `UNAVAILABLE`. After the failure threshold, `CacheDelete` was incorrectly reported as
`FAILED_PRECONDITION: backend executor 'redis:default' is not registered`, although the executor
was registered and its circuit was open.

Go, Python, and TypeScript passed the same Redis operations earlier in the shared run. PHP did not
use a different Redis session or reset contract. This is a product lifecycle defect exposed by
ordering and elapsed time, not a PHP SDK defect.

## Root cause

Several long-lived runtime owners cached `redis::aio::MultiplexedConnection` handles. Once their
driver terminated, the handle could not reconnect and remained poisoned until broker restart:

- generic Redis executor (`src/runtime/executors/redis.rs`);
- Redis canonical system store (`src/runtime/canonical_store/redis.rs`);
- distributed service rate limiter (`src/runtime/service/mod.rs`);
- CDC Redis idempotency guard (`src/runtime/cdc/engine_tail.rs`).

The rate limiter also re-ran its mutating `INCR`/`EXPIRE` Lua program after an I/O error. Because
Redis may have committed the first invocation before its response was lost, that same-request
retry could double-count a request.

The following raw-connection paths are intentionally not changed because they acquire a fresh
connection for each operation and cannot retain a poisoned handle across requests:

- authentication session store (`src/runtime/authn/mod.rs`);
- token JTI denylist (`src/runtime/authn/revocation.rs`), which also falls through to its durable
  database decision on Redis failure;
- native cache-service Redis engine (`src/runtime/service/cache_service/redis_engine.rs`).

The transaction/object cache helpers (`src/runtime/core/tx_object.rs`), backend probes
(`src/runtime/core/probe_dispatch.rs`), and Redis saga compensator
(`src/runtime/saga_compensators.rs`) also dial inside each operation and drop the handle on return.
The read-through cache and probes explicitly degrade/report their per-call failure; the compensator
uses idempotent `DEL`. They therefore cannot poison future calls and were left unchanged.

## Required invariant

Long-lived Redis owners use `redis::aio::ConnectionManager`. The first command that observes an
I/O failure is returned to the caller and is never replayed by UDB. The manager starts a background
reconnect and atomically supplies the replacement connection to a later request. This preserves
at-most-once application dispatch for uncertain mutations while allowing recovery without a broker
restart.

The rate limiter sends an uncertain Lua failure directly to its configured `closed`, `local`, or
explicit `open` fallback. It does not retry the mutation. CDC retains its durable outbox/journal
state and relies on the existing Kafka/idempotency recovery rules when the Redis guard command
fails. A registered executor whose circuit is open returns typed retryable `UNAVAILABLE` with
`circuit_breaker_open`, never a false registration precondition.

## Regression coverage

- `redis_executor_recovers_after_exact_cached_connection_is_killed` establishes the cached
  connection, kills that exact Redis client ID, proves the first command fails instead of being
  replayed, and proves a later request uses a different reconnected client ID.
- `circuit_breaker_failover_skips_open_instance` now also proves that an all-open registered backend is
  reported as retryable `UNAVAILABLE`.
- PR CI run `31960319688` exposed a stale error-detail posture literal after the production error
  binding was named `err`; the guard now pins the actual typed retryable status construction instead
  of the obsolete `{e}` spelling.

CI must run the normal locked all-features checks and the ignored Redis IR live filter; no local
Cargo build or test was run because this repair is CI-only by instruction.
