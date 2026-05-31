//! Pluggable canonical-store layer.
//!
//! ## Why this module exists
//!
//! Earlier UDB data-plane orchestration was implicitly Postgres:
//!
//! - `runtime/cdc/engine_tail.rs` (~1,000 LOC) tailed the Postgres WAL
//!   via logical replication. There was no MySQL binlog reader, no
//!   MongoDB change-stream subscriber.
//! - `runtime/saga.rs` opened a `sqlx::PgPool` and selected from
//!   `udb_sagas`. Saga state had no other home.
//! - `runtime/projection/mod.rs` selected projection tasks from
//!   `udb_projection_tasks` via `PgPool`.
//! - `runtime/consistency_fence.rs` is `#[cfg(feature = "postgres")]`
//!   by design — it calls `pg_current_wal_lsn()` and
//!   `pg_last_wal_replay_lsn()` directly.
//! - `runtime/migration_audit.rs` writes to `udb_migration_runs` via
//!   `PgPool`.
//!
//! The IR + executor layers were truly DB-agnostic, but the
//! orchestration layer wasn't — every backend that wasn't Postgres
//! was a **projection target**, not a peer.
//!
//! The target architecture is peer canonical stores: each supported
//! canonical-class DB should be capable of hosting UDB system tables and
//! producing the durability tokens write-receipts and read-fences wait on.
//!
//! This module is the architectural seam for that migration:
//!
//! - [`CanonicalStore`] — the async trait every canonical-class
//!   backend implements. Object-safe so the runtime can hold a
//!   `Arc<dyn CanonicalStore>` and route through it.
//! - [`DurabilityToken`] — typed wrapper around the backend-opaque
//!   token (PG LSN, MySQL GTID / binlog position, MongoDB cluster
//!   time / resumeToken, SQLite `PRAGMA data_version`).
//! - [`CanonicalStoreRegistry`] — keyed by backend label + instance
//!   name; the runtime selects the active canonical store at startup
//!   based on operator config.
//! - [`OutboxEvent`] — the wire shape every canonical store records
//!   in its `udb_outbox_events` table.
//!
//! ## What this module does NOT do
//!
//! - Migrate existing PG-coupled code (cdc/saga/projection/audit) to
//!   use the trait. That's the multi-thousand-line follow-up. The
//!   seam is here; the current operator-facing architecture is documented in
//!   `docs/architecture.md`.
//! - Implement MySQL binlog tailing or MongoDB change streams. The
//!   `current_durability_token` / `wait_for_token` methods provide
//!   the polling-based fallback; native streaming is the next phase.
//!
//! ## Per-backend impl pointers
//!
//! - [`postgres`] — wraps the existing PG behaviour; ensures the
//!   current binary is unchanged.
//! - [`mysql`] — new, sqlx-mysql-backed.
//! - [`sqlite`] — new, sqlx-sqlite-backed (with `:memory:` support so
//!   the long-pending in-memory test profile becomes real).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;
// NW1-1a: system-table operations (projection tasks first).
pub mod system_store;
// NW1-1a: SQLite implementation of ProjectionTaskStore.
#[cfg(feature = "sqlite")]
mod sqlite_projection;
// NW1-1a: PostgreSQL implementation of ProjectionTaskStore.
#[cfg(feature = "postgres")]
mod postgres_projection;
// NW1-1a: MySQL implementation of ProjectionTaskStore.
#[cfg(feature = "mysql")]
mod mysql_projection;
// NW1-1b: SQLite implementation of SagaStore.
#[cfg(feature = "sqlite")]
mod sqlite_saga;
// NW1-1b: PostgreSQL implementation of SagaStore.
#[cfg(feature = "postgres")]
mod postgres_saga;
// NW1-1b: MySQL implementation of SagaStore.
#[cfg(feature = "mysql")]
mod mysql_saga;
// NW1-1c: SQLite implementation of AdminAuditStore.
#[cfg(feature = "sqlite")]
mod sqlite_admin_audit;
// NW1-1c: PostgreSQL implementation of AdminAuditStore.
#[cfg(feature = "postgres")]
mod postgres_admin_audit;
// NW1-1c: MySQL implementation of AdminAuditStore.
#[cfg(feature = "mysql")]
mod mysql_admin_audit;
// NW1-1d: SQLite implementation of MigrationAuditStore.
#[cfg(feature = "sqlite")]
mod sqlite_migration_audit;
// NW1-1d: PostgreSQL implementation of MigrationAuditStore.
#[cfg(feature = "postgres")]
mod postgres_migration_audit;
// NW1-1d: MySQL implementation of MigrationAuditStore.
#[cfg(feature = "mysql")]
mod mysql_migration_audit;
// P2P-8: shared CanonicalStore contract harness. Lives under
// `#[cfg(test)]` so it's only compiled into the test binary.
#[cfg(all(test, feature = "sqlite"))]
mod conformance;

// ── DurabilityToken ───────────────────────────────────────────────────────────

/// Backend-opaque token capturing "the state of the canonical store
/// at this moment." Tokens are **never** parsed by callers — only
/// compared via `wait_for_token`. The internal `kind` distinguishes
/// which backend produced it so the wait code can dispatch correctly,
/// and `value` is the literal token (PG LSN string, MySQL GTID set,
/// SQLite data_version int as string, MongoDB resumeToken JSON).
///
/// Carrying the backend kind on the token means a write-receipt
/// re-attached to a read against a different backend fails fast with
/// "this fence was minted by Postgres; you can't apply it to MySQL"
/// rather than silently returning stale data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityToken {
    /// Backend label that minted this token (`"postgres"`, `"mysql"`,
    /// `"sqlite"`, `"mongodb"`).
    pub backend_label: String,
    /// Opaque per-backend value. Format pinned per impl.
    pub value: String,
}

impl DurabilityToken {
    pub fn new(backend_label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            backend_label: backend_label.into(),
            value: value.into(),
        }
    }

    pub fn is_for(&self, backend_label: &str) -> bool {
        self.backend_label.eq_ignore_ascii_case(backend_label)
    }
}

// ── OutboxEvent ───────────────────────────────────────────────────────────────

/// One row in the canonical store's `udb_outbox_events` table. The
/// schema is fixed so a consumer can read events from any canonical
/// store without per-backend knowledge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxEvent {
    /// Monotone per-store sequence. Postgres: `BIGSERIAL`. MySQL:
    /// `BIGINT AUTO_INCREMENT`. SQLite: `INTEGER PRIMARY KEY`. The
    /// store guarantees this is unique within itself.
    pub event_seq: i64,
    /// Logical event identifier (UUID string). Idempotency key for
    /// the consumer.
    pub event_id: String,
    /// Kafka topic-like label. The CDC publisher routes by this.
    pub topic: String,
    /// Partition key for the downstream broker.
    pub partition_key: String,
    /// Event body as JSON.
    pub payload: serde_json::Value,
    /// Unix milliseconds when the row was inserted.
    pub created_at_unix_ms: i64,
}

// ── CanonicalStore trait ──────────────────────────────────────────────────────

/// What a canonical-class backend implements. Object-safe; held as
/// `Arc<dyn CanonicalStore>` in [`CanonicalStoreRegistry`].
///
/// Implementations live in the per-backend submodules
/// (`postgres::PostgresCanonicalStore`, etc.). Each must be honest
/// about what it can do — if a method isn't natively supported,
/// return a clear error (don't silently degrade).
#[async_trait]
pub trait CanonicalStore: Send + Sync {
    /// Backend label (`"postgres"`, `"mysql"`, `"sqlite"`).
    fn backend_label(&self) -> &'static str;

    /// Operator-supplied instance name. Distinguishes two canonical
    /// stores of the same backend kind in a single broker (e.g. two
    /// MySQL databases).
    fn instance_name(&self) -> &str;

    /// Capture the current write-durability token. The caller embeds
    /// it in a `WriteReceipt` and the next read can fence against it.
    ///
    /// Per-backend semantics:
    /// - Postgres: `pg_current_wal_lsn()::TEXT`.
    /// - MySQL: `@@GTID_EXECUTED` if GTID mode is on, otherwise
    ///   `SHOW MASTER STATUS` → `(file, pos)` rendered as
    ///   `"<file>:<pos>"`.
    /// - SQLite: `PRAGMA data_version` (monotone within a connection
    ///   lifetime; the runtime maintains one connection per store).
    /// - MongoDB: `$clusterTime` from the last write concern receipt.
    async fn current_durability_token(&self) -> Result<DurabilityToken, String>;

    /// Block until the store has applied at least `token`, or
    /// `timeout` elapses (returning `Ok(false)` on timeout — the
    /// caller decides whether to escalate to a stale-read warning).
    ///
    /// Returns `Ok(true)` if the token cleared in time.
    /// Returns `Err(_)` only for malformed tokens / connection
    /// failures (operator-actionable errors).
    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String>;

    /// Append one event to the outbox. Returns the assigned
    /// `event_seq`. Caller passes a fully-built event minus the
    /// store-assigned `event_seq` and `created_at_unix_ms`.
    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String>;

    /// Current high-water mark of the outbox. Used by write-receipt
    /// builders so the receipt embeds the max sequence the request
    /// advanced past.
    async fn outbox_max_seq(&self) -> Result<i64, String>;

    /// Ensure the UDB system tables exist on this store. Idempotent —
    /// safe to call on every startup. Per-backend DDL lives in each
    /// impl. The SQL dialect differs (Postgres `JSONB`, MySQL `JSON`,
    /// SQLite `TEXT`) but the logical schema is the same.
    async fn ensure_system_tables(&self) -> Result<(), String>;

    // ── NW1-3c — portable advisory lease ─────────────────────────────

    /// Idempotent DDL for the lease table. Every canonical-class
    /// backend keeps a `udb_advisory_leases` table with rows of
    /// `(lease_name, owner_id, expires_at)`. The recovery workers
    /// (saga + projection reconciliation + CDC tailer) use this
    /// instead of PG's `pg_try_advisory_lock` so the worker lease
    /// model is portable across PG / MySQL / SQLite.
    async fn ensure_advisory_lease_table(&self) -> Result<(), String>;

    /// Try to acquire the named lease, holding it for `ttl`. Returns
    /// `Ok(true)` if we now own it, `Ok(false)` if another owner
    /// holds an unexpired lease.
    ///
    /// The implementation is **atomic at the SQL layer**: a single
    /// statement inserts a new row OR re-claims an expired row. No
    /// transaction wrapping needed.
    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String>;

    /// Release the lease. No-op if we don't own it. Idempotent.
    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String>;
}

// ── SystemStores supertrait ──────────────────────────────────────────────────

use system_store::{AdminAuditStore, MigrationAuditStore, ProjectionTaskStore, SagaStore};

/// NW1-3: the union of every system-table contract a canonical store
/// must satisfy. Every concrete canonical-class backend
/// (PG / MySQL / SQLite) implements all four of the system-store
/// traits plus the durability/outbox surface, so the runtime can
/// hand call sites **one** trait object that exposes everything.
///
/// The blanket impl below registers any type that satisfies all five
/// traits as a `SystemStores`, so no impl-side boilerplate is needed.
///
/// **Why a supertrait instead of multiple Arc fields in the registry?**
/// A single trait object lets `consistency_fence::wait_for_fence`
/// call methods from both `CanonicalStore` (LSN wait) and
/// `ProjectionTaskStore` (projection task wait) without juggling two
/// `Arc`s. The registry stores one entry per backend instance and
/// the call site picks whichever methods it needs.
pub trait SystemStores:
    CanonicalStore
    + ProjectionTaskStore
    + SagaStore
    + AdminAuditStore
    + MigrationAuditStore
    + Send
    + Sync
{
}

impl<T> SystemStores for T where
    T: CanonicalStore
        + ProjectionTaskStore
        + SagaStore
        + AdminAuditStore
        + MigrationAuditStore
        + Send
        + Sync
        + ?Sized
{
}

// ── CanonicalStoreRegistry ────────────────────────────────────────────────────

/// Operator-built registry of every canonical-class backend the
/// broker has open. Keyed by `(backend_label, instance_name)` so two
/// MySQL databases or two Postgres servers can co-exist.
///
/// The runtime selects the **active** canonical store per-project
/// (multi-tenancy) or per-request (cross-store consistency fences).
#[derive(Default, Clone)]
// `Debug` is implemented manually below so `Arc<Mutex<CanonicalStoreRegistry>>`
// inside `DataBrokerRuntime` can derive its own Debug.
pub struct CanonicalStoreRegistry {
    by_key: HashMap<(String, String), Arc<dyn CanonicalStore>>,
    /// NW1-3: parallel map keyed identically, holding the richer
    /// `SystemStores` view for call sites that need projection /
    /// saga / admin-audit / migration-audit methods alongside the
    /// CanonicalStore surface. Populated by [`Self::register_full`].
    system_stores: HashMap<(String, String), Arc<dyn SystemStores>>,
    /// The default store the runtime falls back to when no specific
    /// store is selected. Set explicitly by the supervisor; if absent,
    /// look-ups for `default()` return `None` and the caller decides
    /// what to do.
    default_key: Option<(String, String)>,
}

impl std::fmt::Debug for CanonicalStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalStoreRegistry")
            .field("registered_keys", &self.registered_keys())
            .field("has_default", &self.default_key.is_some())
            .finish()
    }
}

impl CanonicalStoreRegistry {
    pub fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            system_stores: HashMap::new(),
            default_key: None,
        }
    }

    /// NW1-3: register a store that satisfies the full
    /// `SystemStores` contract (the common case — every built-in
    /// canonical-class backend). Stores it in both the
    /// `CanonicalStore`-only view and the rich `SystemStores` view,
    /// so call sites can pull whichever they need.
    pub fn register_full(&mut self, store: Arc<dyn SystemStores>) {
        let key = (
            CanonicalStore::backend_label(store.as_ref()).to_ascii_lowercase(),
            CanonicalStore::instance_name(store.as_ref()).to_string(),
        );
        if self.default_key.is_none() {
            self.default_key = Some(key.clone());
        }
        // Store as Arc<dyn CanonicalStore> via a clone — Rust 1.86+
        // supports this implicit trait-object coercion.
        self.by_key.insert(key.clone(), store.clone());
        self.system_stores.insert(key, store);
    }

    /// NW1-3: rich-view lookup.
    pub fn get_full(
        &self,
        backend_label: &str,
        instance_name: &str,
    ) -> Option<Arc<dyn SystemStores>> {
        self.system_stores
            .get(&(
                backend_label.to_ascii_lowercase(),
                instance_name.to_string(),
            ))
            .cloned()
    }

    /// NW1-3: default rich-view store. The runtime's NW1 step 3+
    /// call sites use this — it exposes every system-table contract
    /// in one trait object.
    pub fn default_full_store(&self) -> Option<Arc<dyn SystemStores>> {
        self.default_key
            .as_ref()
            .and_then(|key| self.system_stores.get(key).cloned())
    }

    /// Register a store. Idempotent — re-registering with the same key
    /// replaces the previous entry.
    pub fn register(&mut self, store: Arc<dyn CanonicalStore>) {
        let key = (
            store.backend_label().to_ascii_lowercase(),
            store.instance_name().to_string(),
        );
        if self.default_key.is_none() {
            // First insertion becomes the default. Operator can
            // override via `set_default`.
            self.default_key = Some(key.clone());
        }
        self.by_key.insert(key, store);
    }

    pub fn set_default(&mut self, backend_label: &str, instance_name: &str) -> Result<(), String> {
        let key = (
            backend_label.to_ascii_lowercase(),
            instance_name.to_string(),
        );
        if !self.by_key.contains_key(&key) {
            return Err(format!(
                "canonical store '{backend_label}:{instance_name}' is not registered"
            ));
        }
        self.default_key = Some(key);
        Ok(())
    }

    pub fn get(&self, backend_label: &str, instance_name: &str) -> Option<Arc<dyn CanonicalStore>> {
        self.by_key
            .get(&(
                backend_label.to_ascii_lowercase(),
                instance_name.to_string(),
            ))
            .cloned()
    }

    /// The store the runtime falls back to when no specific one is
    /// selected. Returns `None` if `set_default` was never called.
    /// Named `default_store` (not `default`) to avoid colliding with
    /// `Default::default`.
    pub fn default_store(&self) -> Option<Arc<dyn CanonicalStore>> {
        self.default_key
            .as_ref()
            .and_then(|key| self.by_key.get(key).cloned())
    }

    /// Every registered store. Used by `udb doctor` to report what's
    /// active.
    pub fn all(&self) -> Vec<Arc<dyn CanonicalStore>> {
        self.by_key.values().cloned().collect()
    }

    /// Returns the registered (backend_label, instance_name) keys for
    /// dashboard listings. Sorted for stable output.
    pub fn registered_keys(&self) -> Vec<(String, String)> {
        let mut keys: Vec<(String, String)> = self.by_key.keys().cloned().collect();
        keys.sort();
        keys
    }
}

// ── Shared SQL for the system-table DDL ──────────────────────────────────────

/// Logical schema the system tables follow. Per-backend impls
/// translate this to dialect-specific DDL.
///
/// Documented here (not embedded as SQL) so the contract is
/// dialect-agnostic. The columns map 1:1 across impls; only the type
/// names differ (`JSONB` vs `JSON` vs `TEXT`, `BIGSERIAL` vs
/// `AUTO_INCREMENT` vs `INTEGER PRIMARY KEY`).
pub const OUTBOX_TABLE_SCHEMA: &str = r#"
    udb_outbox_events:
      event_seq        BIGINT auto-increment, primary key
      event_id         CHAR(36)  unique, not null
      topic            VARCHAR(255) not null
      partition_key    VARCHAR(255) not null default ''
      payload          JSON not null
      created_at       TIMESTAMP not null default now
"#;

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin: token carries its backend identity so a Postgres receipt
    /// can't accidentally be applied as a MySQL fence.
    #[test]
    fn durability_token_remembers_its_backend() {
        let pg = DurabilityToken::new("postgres", "0/1A2B3C4D");
        assert!(pg.is_for("postgres"));
        assert!(pg.is_for("POSTGRES"), "case-insensitive");
        assert!(!pg.is_for("mysql"));

        let mysql = DurabilityToken::new("mysql", "binlog.000123:4567");
        assert!(mysql.is_for("mysql"));
        assert!(!mysql.is_for("postgres"));
    }

    /// Pin: tokens round-trip through serde (write-receipts JSON
    /// serialise them).
    #[test]
    fn durability_token_round_trips_through_serde() {
        let t = DurabilityToken::new("sqlite", "42");
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains(r#""backend_label":"sqlite""#));
        assert!(json.contains(r#""value":"42""#));
        let back: DurabilityToken = serde_json::from_str(&json).unwrap();
        assert_eq!(back, t);
    }
}
