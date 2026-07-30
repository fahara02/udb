//! Per-tenant transit crypto-op volume cap for the native `VaultService`.
//!
//! Every transit encrypt/sign/hmac/batch-encrypt op unwraps the tenant's transit
//! DEK through the master KEK; without a cap a single tenant can monopolize that
//! path (and the shared native store) at another tenant's expense. This is a
//! fail-closed, process-local fixed-window counter — NOT a durable store: it caps
//! the RATE of ephemeral crypto work, the same way the fair-admission manager caps
//! concurrency. The map holds one small entry per active tenant (bounded by the
//! tenant count), reset lazily each window.
//!
//! The cap is opt-out-able and generous (`UDB_VAULT_TRANSIT_QUOTA_PER_MINUTE`,
//! default 6000/min; `0` disables). Over-cap ops fail closed with a typed
//! `RESOURCE_EXHAUSTED` (`kind = QUOTA`) advertising the remaining window as the
//! retry-after.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tonic::Status;

use super::config::vault_transit_quota_per_minute;

/// The rolling counting window.
const QUOTA_WINDOW: Duration = Duration::from_secs(60);

/// Pure quota decision, unit-testable without any clock or lock: with a positive
/// `limit`, the op that brought the in-window tally to `count_after` is refused
/// once that tally exceeds the limit. A non-positive `limit` disables the cap
/// (never refused). Deterministic, no I/O.
pub(crate) fn transit_quota_exceeded(count_after: i64, limit: i64) -> bool {
    limit > 0 && count_after > limit
}

/// One tenant's fixed-window tally.
struct WindowCount {
    window_start: Instant,
    count: i64,
}

fn counters() -> &'static Mutex<HashMap<String, WindowCount>> {
    static COUNTERS: OnceLock<Mutex<HashMap<String, WindowCount>>> = OnceLock::new();
    COUNTERS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Admit one transit crypto op for `tenant_id` against the per-tenant per-minute
/// volume cap. Increments the tenant's in-window tally and, when the cap is
/// exceeded, fails closed with a typed `RESOURCE_EXHAUSTED`. A disabled cap
/// (`limit <= 0`) is a no-op that never touches the shared map.
pub(crate) fn admit_transit_op(tenant_id: &str, operation: &'static str) -> Result<(), Status> {
    let limit = vault_transit_quota_per_minute();
    if limit <= 0 {
        return Ok(());
    }
    let now = Instant::now();
    // Increment under the lock; recover from a poisoned lock rather than panicking
    // a crypto request (the counter is advisory, never a correctness gate).
    let (count_after, window_remaining) = {
        let mut map = counters()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = map.entry(tenant_id.to_string()).or_insert(WindowCount {
            window_start: now,
            count: 0,
        });
        if now.duration_since(entry.window_start) >= QUOTA_WINDOW {
            entry.window_start = now;
            entry.count = 0;
        }
        entry.count += 1;
        let remaining = QUOTA_WINDOW.saturating_sub(now.duration_since(entry.window_start));
        (entry.count, remaining)
    };
    if transit_quota_exceeded(count_after, limit) {
        // Retryable after the window rolls over. `quota_status` is
        // `ResourceExhausted` / `kind = QUOTA`.
        return Err(crate::runtime::executor_utils::quota_status(
            "vault",
            operation,
            window_remaining.as_millis() as i64,
            format!(
                "vault transit quota exceeded: tenant is over the {limit} ops/min transit cap \
                 (set UDB_VAULT_TRANSIT_QUOTA_PER_MINUTE to adjust)"
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::transit_quota_exceeded;

    #[test]
    fn transit_quota_decision_is_a_strict_over_limit_check() {
        // A positive limit: refuse only once the in-window tally is strictly over
        // the limit; the op that hits exactly the limit is still admitted.
        assert!(!transit_quota_exceeded(1, 10));
        assert!(!transit_quota_exceeded(10, 10));
        assert!(transit_quota_exceeded(11, 10));
        // A non-positive limit disables the cap regardless of the tally.
        assert!(!transit_quota_exceeded(1_000_000, 0));
        assert!(!transit_quota_exceeded(1_000_000, -5));
    }
}
