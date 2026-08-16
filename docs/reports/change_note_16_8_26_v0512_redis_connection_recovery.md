# Change note: recover long-lived Redis connections without mutation replay

Target release: v0.5.12.

- Enabled Redis `connection-manager` support and recorded its locked `backon` dependency.
- Replaced permanently cached raw Redis connections in the generic executor, canonical Redis
  system store, distributed rate limiter, and CDC idempotency guard with reconnecting managers.
- Removed the rate limiter's unsafe same-request retry of the atomic `INCR`/`EXPIRE` Lua command.
  The original uncertain failure now enters the operator-selected failure mode; a later request
  uses the reconnected transport.
- Preserved the public CDC live-test seam for raw Redis connections while making internal guard
  helpers generic over Redis `ConnectionLike` so production uses the manager.
- Corrected executor resolution so an open circuit is a typed retryable availability failure and
  is not misreported as an unregistered backend.
- Added ignored live coverage that kills the exact cached Redis client and verifies first-failure,
  no-replay, and later-request reconnection semantics, plus unit coverage for all-open circuit
  classification.

Files covered by this note: `Cargo.toml`, `Cargo.lock`,
`src/runtime/executors/redis.rs`, `src/runtime/canonical_store/redis.rs`,
`src/runtime/service/mod.rs`, `src/runtime/cdc/engine_tail.rs`,
`src/runtime/core/accessors.rs`, `src/runtime/core/mod.rs`, and
`src/ir/compile/live_tests/redis_live.rs`.
