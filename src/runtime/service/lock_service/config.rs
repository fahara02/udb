//! Static configuration for the native `LockService`: the entity message name,
//! the versioned outbox topics, the durable status tokens, the TTL/quota bounds,
//! and the expiry-reaper cadence knobs. Extracted verbatim from the former god
//! file; every value is byte-stable for downstream audit/CDC consumers.

/// Default page size for `ListLocks` when the caller does not specify one.
pub(crate) const DEFAULT_LOCK_LIST_LIMIT: i32 = 100;

pub(crate) const LOCK_MSG: &str = "udb.core.lock.entity.v1.Lock";

pub(crate) const TOPIC_ACQUIRED: &str = "udb.lock.lock.acquired.v1";
pub(crate) const TOPIC_RENEWED: &str = "udb.lock.lock.renewed.v1";
pub(crate) const TOPIC_RELEASED: &str = "udb.lock.lock.released.v1";
/// Emitted by the leader-elected expiry reaper when a lapsed HELD row is flipped
/// to EXPIRED (16.5.1).
pub(crate) const TOPIC_EXPIRED: &str = "udb.lock.lock.expired.v1";

pub(crate) const STATUS_HELD: &str = "HELD";
pub(crate) const STATUS_RELEASED: &str = "RELEASED";
/// Terminal state stamped by the expiry reaper on a HELD row whose lease lapsed
/// without a release. The `status` column is a VARCHAR(20); the entity-proto
/// comment currently enumerates only `HELD | RELEASED` (out-of-fence follow-up).
pub(crate) const STATUS_EXPIRED: &str = "EXPIRED";

/// Default lease TTL when the caller does not specify one.
pub(crate) const DEFAULT_LEASE_TTL_SECONDS: i64 = 30;
/// Upper bound on a lease TTL so a caller cannot pin a lock indefinitely.
pub(crate) const MAX_LEASE_TTL_SECONDS: i64 = 3600;
/// Per-tenant active-lock budget. Bounds the durable lock table so one tenant
/// cannot exhaust the shared store; a new acquire beyond this fails closed.
pub(crate) const MAX_ACTIVE_LOCKS_PER_TENANT: usize = 256;
/// Upper bound on rows one expiry-reaper pass claims — bounds the sweep
/// transaction the same way `SCHEDULER_TICK_BATCH` bounds the scheduler tick.
/// `pub(crate)` so the leader spawn site passes the named const (16.11.3).
pub(crate) const LOCK_EXPIRY_SWEEP_BATCH: i64 = 200;
/// Default reaper cadence; overridable via [`LOCK_EXPIRY_INTERVAL_ENV`].
const DEFAULT_LOCK_EXPIRY_INTERVAL_SECS: u64 = 30;
const LOCK_EXPIRY_INTERVAL_ENV: &str = "UDB_LOCK_EXPIRY_INTERVAL_SECS";

/// Reaper cadence for the leader spawn site — env resolved once at spawn,
/// mirroring `cache_service::cache_invalidation_interval`.
pub(crate) fn lock_expiry_interval() -> std::time::Duration {
    // Resolved ONCE via OnceLock (worker-spawn cadence knob), never per-request —
    // matches the sibling native-service startup-config reads.
    static SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let secs = *SECS.get_or_init(|| {
        std::env::var(LOCK_EXPIRY_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_LOCK_EXPIRY_INTERVAL_SECS)
    });
    std::time::Duration::from_secs(secs)
}

/// Clamp a requested TTL into `[DEFAULT, MAX]`; non-positive → default.
pub(crate) fn resolve_ttl_seconds(requested: i32) -> i64 {
    let requested = i64::from(requested);
    if requested <= 0 {
        DEFAULT_LEASE_TTL_SECONDS
    } else {
        requested.min(MAX_LEASE_TTL_SECONDS)
    }
}
