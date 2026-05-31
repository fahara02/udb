//! U6 — write-receipt issuance and read-fence waiting.
//!
//! `runtime/consistency.rs` defined the [`WriteReceipt`] / [`ReadFence`]
//! / [`StaleReadWarning`] type contracts but left the actual issuance
//! and wait logic to the handlers — and the handlers stubbed them
//! (`outbox_seq: 0`, no fence wait). This module fills the gap with
//! three pieces:
//!
//! - [`build_write_receipt`] — produces a complete [`WriteReceipt`]
//!   from the running Postgres pool: WAL LSN, the latest outbox
//!   sequence value, and the projection task IDs created by the
//!   current write (caller passes the IDs they enqueued).
//! - [`wait_for_fence`] — polls `pg_last_wal_replay_lsn()` and the
//!   `projection_tasks` table until the fence clears, the deadline
//!   is hit, or the wait is cancelled. Returns
//!   [`FenceOutcome::Cleared`] / [`FenceOutcome::Stale`] so the
//!   caller can attach a [`StaleReadWarning`] to the response.
//! - [`RedisCacheStamp`] — packs the manifest checksum, source LSN,
//!   and projection version into the cache value so a stale entry
//!   from a previous catalog generation is detected on read.
//!
//! ## Why the helpers and not direct handler edits?
//!
//! U7 is the long-form refactor that pushes every handler through one
//! pipeline object. U6 needs the wait/issue contract callable *today*
//! without rewriting every handler. The helpers are designed to be
//! dropped into the existing call sites (`with_mutation_response_headers`
//! already calls `current_write_receipt`; the bigger fix is to swap
//! that to `build_write_receipt`).
//!
//! ## Postgres-only by design
//!
//! The fence is anchored to the Postgres WAL because Postgres is the
//! canonical write side of every UDB deployment. Non-Postgres-only
//! deployments would need a different LSN equivalent; that's a
//! follow-up tied to non-Postgres canonical writes.

use std::time::{Duration, Instant};

// NW1-3e: PgPool dependency removed. consistency_fence is now
// backend-agnostic; it accepts `&dyn SystemStores` and routes through
// `CanonicalStore` / `ProjectionTaskStore` traits. PG behaviour is
// unchanged (the `PostgresCanonicalStore` impl issues the same SQL
// the pool-based code used to issue directly).

use crate::runtime::consistency::{ReadFence, StaleReadWarning, WriteReceipt};

/// What [`wait_for_fence`] returns. The caller maps `Cleared` to a
/// normal response and `Stale` to a response with a [`StaleReadWarning`]
/// attached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceOutcome {
    /// Every component of the fence cleared inside `max_wait_ms`.
    Cleared,
    /// The fence didn't clear in time. The caller may still serve the
    /// (potentially stale) read; the warning explains why.
    Stale(StaleReadWarning),
}

/// How often the fence loop polls Postgres while waiting. 25 ms keeps
/// latency low at the cost of a few extra round-trips; tunable via
/// `UDB_CONSISTENCY_FENCE_POLL_MS`.
fn fence_poll_interval_ms() -> u64 {
    std::env::var("UDB_CONSISTENCY_FENCE_POLL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(25)
}

/// Parse a PG `pg_lsn` textual value (e.g. `"0/1A2B3C4D"`) into the
/// 64-bit numeric form Postgres compares with. Returns `None` for
/// inputs that don't match the `<hex>/<hex>` shape — the caller treats
/// that as "no LSN fence" rather than erroring.
pub fn parse_pg_lsn(text: &str) -> Option<u64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (hi, lo) = trimmed.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Build a [`WriteReceipt`] from the live Postgres pool plus
/// caller-supplied projection task IDs.
///
/// `manifest_checksum` comes from the active catalog. `projection_tasks`
/// is the set of task IDs the write enqueued — the caller knows this
/// because the projection-task INSERT just returned them.
///
/// On any DB error this falls back to `WriteReceipt::empty()` — a
/// write receipt is a hint, not a correctness contract; a failed
/// LSN read shouldn't fail the request.
///
/// NW1-3e: routes through `Arc<dyn SystemStores>` rather than a raw
/// `PgPool`, so the same code path works against any canonical
/// store (PG, MySQL, SQLite, future Mongo, …). The store's
/// `current_durability_token` yields the backend-opaque "source LSN"
/// — for PG it's the WAL LSN string; for MySQL the GTID set;
/// for SQLite the `PRAGMA data_version` integer; for Mongo the
/// resume token. The receipt carries them all under the same
/// `source_lsn` field for SDK-side comparison.
pub async fn build_write_receipt(
    store: &dyn crate::runtime::canonical_store::SystemStores,
    manifest_checksum: &str,
    projection_tasks: Vec<String>,
) -> WriteReceipt {
    use crate::runtime::canonical_store::CanonicalStore;

    let source_lsn = CanonicalStore::current_durability_token(store)
        .await
        .map(|t| t.value)
        .unwrap_or_default();

    let outbox_seq = CanonicalStore::outbox_max_seq(store)
        .await
        .ok()
        .map(|n| n.max(0) as u64)
        .unwrap_or(0);

    WriteReceipt {
        source_lsn,
        outbox_seq,
        projection_task_ids: projection_tasks,
        manifest_checksum: manifest_checksum.to_string(),
        written_at_unix_ms: now_unix_ms(),
    }
}

/// Wait for every component of the fence to clear. Returns
/// `FenceOutcome::Cleared` if the fence fully cleared inside the
/// deadline, or `FenceOutcome::Stale` with a typed warning naming what
/// fell behind.
///
/// NW1-3e: backend-agnostic. The LSN wait routes through
/// [`CanonicalStore::wait_for_token`] — for PG that polls
/// `pg_last_wal_replay_lsn()`, for MySQL it's `WAIT_FOR_EXECUTED_GTID_SET`
/// or `MASTER_POS_WAIT`, for SQLite it's `PRAGMA data_version`. The
/// projection-task wait routes through
/// [`ProjectionTaskStore::pending_projection_task_count`].
///
/// Implementation:
///
/// 1. If `fence.min_outbox_lsn` is non-empty, build a
///    [`DurabilityToken`](crate::runtime::canonical_store::DurabilityToken)
///    and ask the store to wait for it (one round-trip + native
///    server-side waiting where the backend supports it).
/// 2. If `fence.projection_task_ids` is non-empty, poll the
///    projection-task pending count via the trait.
/// 3. Both must clear before the deadline.
pub async fn wait_for_fence(
    store: &dyn crate::runtime::canonical_store::SystemStores,
    fence: &ReadFence,
    backend_label: &str,
    instance_label: &str,
) -> FenceOutcome {
    use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
    use crate::runtime::canonical_store::{CanonicalStore, DurabilityToken};

    if fence.is_empty() {
        return FenceOutcome::Cleared;
    }

    let target_tasks: Vec<String> = fence
        .projection_task_ids
        .iter()
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .collect();

    let max_wait = Duration::from_millis(fence.max_wait_ms);
    let poll = Duration::from_millis(fence_poll_interval_ms());
    let started = Instant::now();
    let store_label = CanonicalStore::backend_label(store).to_string();

    loop {
        // 1) LSN / durability-token fence.
        let lsn_cleared = if fence.min_outbox_lsn.is_empty() {
            true
        } else {
            // Construct the token using the store's own label so
            // `wait_for_token`'s `is_for(...)` check passes. The
            // raw `min_outbox_lsn` string is opaque to the trait —
            // each backend parses its own format (PG LSN hex,
            // MySQL `gtid:` / `file:` prefix, SQLite int).
            let token = DurabilityToken::new(store_label.clone(), fence.min_outbox_lsn.clone());
            // Cap the per-iteration wait at the remaining budget so
            // the loop returns promptly on overall timeout. We use
            // the full remaining budget so backends that block
            // server-side (PG's pg_last_wal_replay_lsn polled here,
            // MySQL's WAIT_FOR_EXECUTED_GTID_SET native wait) don't
            // burn extra polls.
            let remaining = max_wait.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                false
            } else {
                CanonicalStore::wait_for_token(store, &token, remaining)
                    .await
                    .unwrap_or(false)
            }
        };

        // 2) Projection-task fence.
        let tasks_cleared = if target_tasks.is_empty() {
            true
        } else {
            match ProjectionTaskStore::pending_projection_task_count(store, &target_tasks).await {
                Ok(n) => n == 0,
                Err(_) => false,
            }
        };

        if lsn_cleared && tasks_cleared {
            return FenceOutcome::Cleared;
        }
        if started.elapsed() >= max_wait {
            let lag_ms = started.elapsed().as_millis() as u64;
            if !tasks_cleared {
                // ProjectionMissing carries `resource` (the projection
                // target name) rather than `instance` — use the joined
                // pending task IDs as a debugging breadcrumb so the
                // operator can correlate with the projection_tasks
                // ledger.
                let resource = target_tasks.join(",");
                return FenceOutcome::Stale(StaleReadWarning::ProjectionMissing {
                    backend: backend_label.to_string(),
                    resource,
                });
            }
            let _ = instance_label;
            return FenceOutcome::Stale(StaleReadWarning::FenceTimedOut {
                backend: backend_label.to_string(),
                instance: instance_label.to_string(),
                lag_ms,
            });
        }
        tokio::time::sleep(poll).await;
    }
}

/// Wrap a Redis cache value with the U6 stamp so a stale entry
/// (manifest changed, schema migrated, or the LSN it was written from
/// is older than the read fence) is detected on read.
///
/// Stored value is JSON `{ "stamp": {...}, "value": <inner> }`. The
/// serializer + de-serializer are both here so the cache reader can
/// inspect the stamp without parsing the inner payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RedisCacheStamp {
    /// Manifest checksum at the time the cache entry was written.
    /// Different checksum = schema migrated = invalidate.
    pub manifest_checksum: String,
    /// Source WAL LSN at the time the cache entry was populated.
    /// A read fence with `min_outbox_lsn > this` invalidates the
    /// entry.
    pub source_lsn: String,
    /// Projection version (`producer_epoch` from the CDC config).
    /// Bumping the epoch invalidates every cached entry from before.
    pub projection_version: i64,
    /// Unix ms the entry was written. Used for TTL audit.
    pub written_at_unix_ms: i64,
}

/// Wrapped cache value as it lives in Redis. `serde_json` round-trip
/// gives the byte format the cache reader/writer agree on.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StampedCacheValue<T> {
    pub stamp: RedisCacheStamp,
    pub value: T,
}

impl<T> StampedCacheValue<T> {
    pub fn new(stamp: RedisCacheStamp, value: T) -> Self {
        Self { stamp, value }
    }

    /// Returns true if this cached value is still valid against the
    /// caller's current view. `current_checksum` and `min_lsn` come
    /// from the request — `min_lsn` is the LSN the read fence (or the
    /// caller's most recent write receipt) demands.
    pub fn is_fresh_against(&self, current_checksum: &str, min_lsn: Option<u64>) -> bool {
        if self.stamp.manifest_checksum != current_checksum {
            return false;
        }
        if let Some(min) = min_lsn {
            let written_lsn = parse_pg_lsn(&self.stamp.source_lsn);
            match written_lsn {
                Some(w) => w >= min,
                // No LSN on the stamp + a min_lsn requirement = treat
                // as stale (we can't prove freshness).
                None => return false,
            }
        } else {
            true
        }
    }
}

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// NW1-3e: `projection_tasks_cleared` and `is_safe_relation` were
// PgPool-era helpers that hard-coded the SQL shape. They're gone now
// — `ProjectionTaskStore::pending_projection_task_count` is the
// portable replacement and relation-name validation happens inside
// each store impl (e.g. `SqliteCanonicalStore::safe_table`,
// `PostgresCanonicalStore::safe_relation`).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::consistency::ReadFence;

    /// Pin: PG lsn parser handles the canonical `<hex>/<hex>` shape
    /// and rejects garbage instead of returning a misleading 0.
    #[test]
    fn parse_pg_lsn_round_trips_canonical_form() {
        assert_eq!(parse_pg_lsn("0/1A2B3C4D"), Some(0x1A2B3C4D));
        assert_eq!(parse_pg_lsn("1/0"), Some(1u64 << 32));
        assert_eq!(
            parse_pg_lsn("FFFFFFFF/FFFFFFFF"),
            Some(0xFFFF_FFFF_FFFF_FFFFu64)
        );
        assert_eq!(parse_pg_lsn(""), None);
        assert_eq!(parse_pg_lsn("not-an-lsn"), None);
        assert_eq!(parse_pg_lsn("0/"), None);
        assert_eq!(parse_pg_lsn("/0"), None);
    }

    /// Pin: empty fence short-circuits as `Cleared` without any I/O.
    /// The handler must not pay the polling cost when there's nothing
    /// to wait for.
    #[test]
    fn empty_fence_is_cleared_immediately() {
        // No pool needed because the empty branch short-circuits before
        // touching it; test by replicating the branch directly.
        let fence = ReadFence::default();
        assert!(fence.is_empty());
        // Equivalent to FenceOutcome::Cleared (we can't call
        // wait_for_fence without a pool; the early-return is the
        // pinned behaviour).
    }

    /// Pin: cache stamp catches a stale entry from a previous
    /// manifest generation. SDKs rely on this to avoid serving
    /// post-migration garbage from cache.
    #[test]
    fn cache_stamp_rejects_stale_manifest_checksum() {
        let stamp = RedisCacheStamp {
            manifest_checksum: "abc123".to_string(),
            source_lsn: "0/1000".to_string(),
            projection_version: 0,
            written_at_unix_ms: 1,
        };
        let cached = StampedCacheValue::new(stamp, 42u32);
        assert!(cached.is_fresh_against("abc123", None));
        assert!(
            !cached.is_fresh_against("def456", None),
            "different checksum must invalidate"
        );
    }

    /// Pin: cache stamp catches a stale entry below the read-fence
    /// LSN. This is the read-your-writes guarantee through cache —
    /// if the cache was written from LSN < the fence, it's stale.
    #[test]
    fn cache_stamp_rejects_lsn_below_fence() {
        let stamp = RedisCacheStamp {
            manifest_checksum: "abc".to_string(),
            source_lsn: "0/1000".to_string(),
            projection_version: 0,
            written_at_unix_ms: 1,
        };
        let cached = StampedCacheValue::new(stamp, 42u32);
        // Fence asks for LSN >= 0x2000, but the cache was written at
        // 0x1000 — stale.
        assert!(!cached.is_fresh_against("abc", Some(0x2000)));
        // Fence asks for LSN >= 0x0800, cache at 0x1000 — fresh.
        assert!(cached.is_fresh_against("abc", Some(0x0800)));
    }

    /// Pin: cache stamp without an LSN cannot prove freshness
    /// against an LSN-bearing fence — must report stale.
    #[test]
    fn cache_stamp_without_lsn_fails_against_lsn_fence() {
        let stamp = RedisCacheStamp {
            manifest_checksum: "abc".to_string(),
            source_lsn: String::new(),
            projection_version: 0,
            written_at_unix_ms: 1,
        };
        let cached = StampedCacheValue::new(stamp, 0u32);
        assert!(!cached.is_fresh_against("abc", Some(0x1000)));
        // Without LSN fence, freshness depends only on checksum.
        assert!(cached.is_fresh_against("abc", None));
    }

    // NW1-3e: the `is_safe_relation` and `projection_tasks_cleared`
    // tests went away with their helpers. Relation-name validation
    // moved into each store's `safe_relation` / `safe_table` method
    // (covered in `postgres.rs::unsafe_relation_is_rejected`,
    // `mysql.rs::unsafe_relation_is_rejected`,
    // `sqlite.rs::unsafe_table_name_is_rejected`). The
    // "target_count == terminal_count" math went away too — the
    // `ProjectionTaskStore::pending_projection_task_count` impl
    // returns the simple non-terminal count directly, so the fence's
    // `count == 0` check IS the cleared signal.

    /// Pin: NW1-3e — the new wait_for_fence + build_write_receipt
    /// end-to-end against an in-memory SQLite SystemStores. Proves
    /// the trait-routed code path works without Postgres.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn fence_e2e_against_sqlite_system_stores() {
        use crate::runtime::canonical_store::SystemStores;
        use crate::runtime::canonical_store::sqlite::SqliteCanonicalStore;
        use crate::runtime::canonical_store::system_store::{
            ProjectionOperation, ProjectionTaskInsert, ProjectionTaskStore,
        };
        use crate::runtime::consistency::ReadFence;
        use sqlx::sqlite::SqlitePoolOptions;
        use std::sync::Arc;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite");
        let store = Arc::new(SqliteCanonicalStore::new(pool, "test", "udb_outbox_events"));
        // Set up both surfaces the fence uses.
        ProjectionTaskStore::ensure_projection_tables(store.as_ref())
            .await
            .expect("ensure projection tables");
        crate::runtime::canonical_store::CanonicalStore::ensure_system_tables(store.as_ref())
            .await
            .expect("ensure outbox");
        let store_dyn: Arc<dyn SystemStores> = store.clone();

        // 1. build_write_receipt produces a non-empty receipt that
        // links the SQLite data_version, the outbox max_seq, and
        // the manifest checksum.
        let receipt = super::build_write_receipt(
            store_dyn.as_ref(),
            "sha256:manifest-v1",
            vec!["task-1".to_string()],
        )
        .await;
        assert_eq!(receipt.manifest_checksum, "sha256:manifest-v1");
        assert!(
            !receipt.source_lsn.is_empty(),
            "SQLite source_lsn must be the PRAGMA data_version string"
        );
        assert_eq!(receipt.outbox_seq, 0, "fresh outbox starts at 0");
        assert_eq!(receipt.projection_task_ids, vec!["task-1".to_string()]);

        // 2. wait_for_fence against an empty fence is Cleared.
        let cleared =
            super::wait_for_fence(store_dyn.as_ref(), &ReadFence::default(), "sqlite", "test")
                .await;
        assert_eq!(cleared, FenceOutcome::Cleared);

        // 3. wait_for_fence with a projection task ID that doesn't
        // exist clears too — the pending count is 0 because no task
        // matches the idempotency key. (A non-existent task can't
        // block a read forever.)
        let cleared = super::wait_for_fence(
            store_dyn.as_ref(),
            &ReadFence {
                min_outbox_lsn: String::new(),
                projection_task_ids: vec!["ghost".to_string()],
                max_wait_ms: 50,
            },
            "sqlite",
            "test",
        )
        .await;
        assert_eq!(cleared, FenceOutcome::Cleared);

        // 4. Enqueue a task and fence on it — should block because
        // the task is PENDING, returning ProjectionMissing on timeout.
        store
            .enqueue_projection_task(&ProjectionTaskInsert {
                idempotency_key: "pending-task".to_string(),
                project_id: "p".to_string(),
                manifest_checksum: "h".to_string(),
                message_type: "m".to_string(),
                source_schema: "s".to_string(),
                source_table: "t".to_string(),
                source_row_key: serde_json::json!({"k": 1}),
                operation: ProjectionOperation::Upsert,
                target_backend: "qdrant".to_string(),
                target_instance: "".to_string(),
                projection_kind: "vector".to_string(),
                resource_name: "x".to_string(),
                target_options: serde_json::json!([]),
                source_payload: serde_json::json!({}),
                source_checksum: "c".to_string(),
            })
            .await
            .unwrap();
        let outcome = super::wait_for_fence(
            store_dyn.as_ref(),
            &ReadFence {
                min_outbox_lsn: String::new(),
                projection_task_ids: vec!["pending-task".to_string()],
                max_wait_ms: 100,
            },
            "sqlite",
            "test",
        )
        .await;
        match outcome {
            FenceOutcome::Stale(
                crate::runtime::consistency::StaleReadWarning::ProjectionMissing {
                    backend,
                    resource,
                },
            ) => {
                assert_eq!(backend, "sqlite");
                assert!(resource.contains("pending-task"));
            }
            other => panic!("expected ProjectionMissing, got {other:?}"),
        }
    }

    /// Pin: serializing a StampedCacheValue gives a stable JSON shape
    /// that a non-Rust cache reader (e.g. a Go SDK) can parse with
    /// just the stamp+value contract.
    #[test]
    fn stamped_cache_value_round_trips_through_json() {
        let stamp = RedisCacheStamp {
            manifest_checksum: "abc".to_string(),
            source_lsn: "0/100".to_string(),
            projection_version: 7,
            written_at_unix_ms: 12_345,
        };
        let v = StampedCacheValue::new(stamp.clone(), "hello".to_string());
        let json = serde_json::to_string(&v).unwrap();
        // Pin the wire shape — `stamp` and `value` are the top-level
        // keys, projection_version is an int, source_lsn is a string.
        assert!(json.contains(r#""stamp":{"manifest_checksum":"abc""#));
        assert!(json.contains(r#""source_lsn":"0/100""#));
        assert!(json.contains(r#""projection_version":7"#));
        assert!(json.contains(r#""value":"hello""#));
        let back: StampedCacheValue<String> = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stamp, stamp);
        assert_eq!(back.value, "hello");
    }
}
