//! Static configuration for the native `CacheService`: the key-shape root, the
//! sweep command + bounds, the byte-budget default, the reserved meta suffix,
//! the versioned outbox topics, and the resolve-once CDC-invalidation interval
//! knob. No per-request env reads.

/// Root prefix for every cache key. The full data-key shape is
/// `udb:cache:<tenant>:<ns>:k:<key>`; bookkeeping keys (the byte counter and the
/// namespace meta blob) share the `udb:cache:<tenant>:<ns>:` prefix but use a
/// reserved suffix so a data-key `SCAN` never returns them.
pub(crate) const KEY_ROOT: &str = "udb:cache";

/// The Redis command used for every prefix sweep. Load-bearing: the engine calls
/// `redis::cmd(SWEEP_COMMAND)`, and the guard test asserts it is `SCAN` (cursor,
/// non-blocking) and never `KEYS` (O(N), blocks the server).
pub(crate) const SWEEP_COMMAND: &str = "SCAN";

/// `COUNT` hint per SCAN round-trip (matches `core/tx_object::cache_delete_pattern`).
/// Also the `Scan` default page size when the caller sends no positive `limit`.
pub(crate) const SWEEP_COUNT: u32 = 500;

/// Hard cap on a caller-supplied `Scan` page `limit` (it becomes the SCAN `COUNT`
/// hint). Without the clamp a caller could push an arbitrarily large COUNT into
/// the cursor walk and turn SCAN into a KEYS-shaped stall — see
/// [`clamped_scan_count`], the pure gate `redis_engine::scan` applies.
pub(crate) const MAX_SCAN_PAGE_LIMIT: u32 = 1_000;

/// Byte-counter reconciliation bounds (the worker pass that heals TTL-expiry
/// drift — see `redis_engine::reconcile_bytes_counters_once`): at most this many
/// namespaces have their counter recomputed per worker pass (a rotating subset,
/// resumed via [`reconcile_cursor_key`]).
pub(crate) const RECONCILE_NAMESPACES_PER_PASS: usize = 16;

/// At most this many SCAN round-trips per pass while DISCOVERING namespace meta
/// keys, so discovery stays bounded even on a huge, meta-sparse keyspace.
pub(crate) const RECONCILE_DISCOVERY_MAX_ROUNDS: u32 = 32;

/// A namespace holding more data keys than this is SKIPPED for the pass instead
/// of being SET from a partial sum — a partial sum would UNDER-count usage and
/// quietly widen the byte budget (fail-open); skipping keeps the stale counter,
/// which only over-counts (fail-closed).
pub(crate) const RECONCILE_MAX_KEYS_PER_NAMESPACE: usize = 5_000;

/// Service-default per-namespace byte budget when a namespace declares none
/// (`max_bytes <= 0`). Bounds the shared Redis so one tenant/namespace cannot
/// exhaust it.
pub(crate) const DEFAULT_NAMESPACE_MAX_BYTES: i64 = 64 * 1024 * 1024;

/// Reserved suffix of the per-namespace meta blob key (single definition so the
/// key builder, the reconciliation discovery pattern, and the parser never drift).
pub(crate) const META_SUFFIX: &str = "__meta__";

/// Invalidation event topic emitted by `DeleteNamespace` and the CDC worker.
#[cfg(feature = "redis")]
pub(crate) const TOPIC_INVALIDATED: &str = "udb.cache.invalidated.v1";
#[cfg(feature = "redis")]
pub(crate) const TOPIC_ENTRY_SET: &str = "udb.cache.entry.set.v1";
#[cfg(feature = "redis")]
pub(crate) const TOPIC_ENTRY_DELETED: &str = "udb.cache.entry.deleted.v1";
#[cfg(feature = "redis")]
pub(crate) const TOPIC_NAMESPACE_CREATED: &str = "udb.cache.namespace.created.v1";
#[cfg(feature = "redis")]
pub(crate) const CACHE_INVALIDATION_BATCH: i64 = 200;
#[cfg(feature = "redis")]
const DEFAULT_CACHE_INVALIDATION_INTERVAL_SECS: u64 = 30;
#[cfg(feature = "redis")]
const CACHE_INVALIDATION_INTERVAL_ENV: &str = "UDB_CACHE_INVALIDATION_INTERVAL_SECS";

#[cfg(feature = "redis")]
pub(crate) fn cache_invalidation_interval() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var(CACHE_INVALIDATION_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_CACHE_INVALIDATION_INTERVAL_SECS),
    )
}
