//! `ClickHouseCanonicalStore` — B.10c PHASE 1. ClickHouse-backed
//! [`CanonicalStore`](super::CanonicalStore) implementation over the EXISTING
//! HTTP executor
//! ([`ClickHouseExecutor`](crate::runtime::executors::clickhouse::ClickHouseExecutor)).
//! No new dependency: every operation is SQL over the ClickHouse HTTP interface,
//! reusing the executor's `select_rows` (JSONCompact decode) and `execute_ddl`
//! (DDL / INSERT) helpers.
//!
//! This module implements the base canonical-store surface: durability token,
//! outbox, advisory leases, and `ensure_system_tables`. The companion
//! `clickhouse_*` modules implement the `SystemStores` traits
//! (`ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
//! `MigrationAuditStore`). Runtime registration is still intentionally gated by
//! `UDB_ALLOW_PROJECTION_SYSTEM_STORE=1` because the sequence and lease paths
//! below rely on the single-writer assumption documented here.
//!
//! ## Why ClickHouse is the hardest canonical target
//!
//! ClickHouse has **none** of the primitives the SQL canonical stores lean on:
//!
//! - **No multi-statement transactions** — every statement auto-commits.
//! - **No row locks / `SELECT … FOR UPDATE`.**
//! - **No native compare-and-set / `INSERT … ON CONFLICT`.**
//! - **Append-optimised storage** (`MergeTree`). Real in-place mutation
//!   (`ALTER TABLE … UPDATE/DELETE`) is an asynchronous, heavy background
//!   operation — never use it on the hot path.
//!
//! ### The mapping this store uses
//!
//! - **Append-friendly data (the outbox)** → plain `INSERT` into a `MergeTree`
//!   (`udb_outbox_events`, ordered by `event_seq`).
//! - **Mutable state (the outbox sequence counter; advisory leases)** →
//!   a `ReplacingMergeTree(version)` keyed by id with a monotonic `version`
//!   column. We never UPDATE a row in place: to change state we INSERT a NEW
//!   row carrying `version + 1`, and we always READ the latest with
//!   `SELECT … FINAL` (which collapses superseded rows by the engine's
//!   replacing key, keeping only the highest `version`). Equivalent reads can
//!   use `argMax(col, version)`; this store uses `FINAL` for clarity.
//! - **"Compare-and-set"** is *emulated* by read-current-version →
//!   insert-version+1 → re-read-FINAL-to-confirm-we-won
//!   (last-writer-by-version wins).
//!
//! ## CONCURRENCY CAVEAT (single-writer assumption) — read before reuse
//!
//! Because ClickHouse offers no atomic CAS, the read-insert-reread emulation
//! below is **only correct under a single concurrent writer per (counter id /
//! lease name)**. Two writers racing the same counter can both read seq `N`,
//! both insert `N+1`, and `FINAL` then collapses the two `N+1` rows into one —
//! losing one event's slot (a duplicate `event_seq`) — or two lease acquirers
//! can each believe they won. The conformance run is single-threaded, so this
//! is sufficient to satisfy the base contract; a hardened multi-writer phase-2
//! path would need a `Keeper`-backed lock, an external sequencer, or a
//! `VersionedCollapsingMergeTree` reconciliation pass. Every place this matters
//! is commented inline.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value as Json;

use super::{CanonicalStore, DurabilityToken};
use crate::runtime::executors::clickhouse::ClickHouseExecutor;

/// Well-known id of the single outbox-sequence counter row in `udb_counters`.
const OUTBOX_SEQ_ID: &str = "outbox_seq";

/// Validate a ClickHouse identifier the store interpolates into SQL (the
/// database name, since the table names are compile-time constants). Mirrors the
/// executor's `validate_ch_identifier` guard so an operator-supplied database
/// name can never break out of the `` `db`.`table` `` quoting.
fn safe_ident(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 64 {
        return Err(format!(
            "ClickHouse identifier '{id}' is invalid: must be 1-64 characters"
        ));
    }
    let first = id.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(format!(
            "ClickHouse identifier '{id}' must start with a letter or underscore"
        ));
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "ClickHouse identifier '{id}' contains invalid characters; \
             only ASCII letters, digits, and underscores are allowed"
        ));
    }
    Ok(())
}

/// Escape a string literal for inline SQL (single quotes doubled). The store
/// uses inline literals (not bound parameters) because the executor speaks the
/// raw-SQL HTTP body; callers only ever pass UDB-internal ids / owner ids /
/// event ids here, all additionally length-bounded by the schema.
///
/// `pub(super)` so the phase-2 system-store impls reuse the exact same escaping
/// for every inline literal they interpolate (idempotency keys, JSON payloads,
/// status strings, error text, etc.).
pub(super) fn sql_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

pub struct ClickHouseCanonicalStore {
    // `pub(super)` so the phase-2 system-store impls
    // (`clickhouse_projection` / `_saga` / `_admin_audit` / `_migration_audit`)
    // in sibling modules can reach the executor + database directly through the
    // accessors below.
    pub(super) executor: ClickHouseExecutor,
    pub(super) instance_name: String,
    pub(super) database: String,
}

impl ClickHouseCanonicalStore {
    pub fn new(
        executor: ClickHouseExecutor,
        instance_name: impl Into<String>,
        database: impl Into<String>,
    ) -> Self {
        Self {
            executor,
            instance_name: instance_name.into(),
            database: database.into(),
        }
    }

    /// Executor accessor for the phase-2 system-store impls in sibling modules.
    pub(super) fn executor(&self) -> &ClickHouseExecutor {
        &self.executor
    }

    /// Fully-qualified, back-quoted `` `db`.`table` `` for one of our fixed
    /// system tables. The database is operator-supplied so it is validated; the
    /// `table` argument is always a compile-time constant from this module.
    ///
    /// `pub(super)` so the phase-2 system-store impls qualify their own fixed
    /// table names through this single validated path.
    pub(super) fn qualified(&self, table: &str) -> Result<String, String> {
        safe_ident(&self.database)?;
        Ok(format!("`{}`.`{}`", self.database, table))
    }

    /// Unix milliseconds now. Used for advisory-lease `expires_at` so the lease
    /// math is identical to the SQL / Neo4j stores and unit-testable without a
    /// server (rather than relying on ClickHouse `now64()`).
    ///
    /// `pub(super)` so the phase-2 system-store impls stamp their timestamp
    /// columns (created_at / updated_at / applied_at / …) from the same clock.
    pub(super) fn now_unix_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }

    /// Read the current counter `seq` for `OUTBOX_SEQ_ID` via `FINAL` so any
    /// superseded versions collapse to the highest-`version` row. Returns 0 when
    /// the counter row is absent (fresh store / never enqueued).
    ///
    /// `FINAL` forces ClickHouse to merge the ReplacingMergeTree parts at read
    /// time so we never observe a stale, superseded version — at the cost of a
    /// heavier read (acceptable: the counter table holds at most a handful of
    /// live keys).
    async fn current_counter_seq(&self) -> Result<i64, String> {
        let counters = self.qualified("udb_counters")?;
        let sql = format!(
            "SELECT seq FROM {counters} FINAL WHERE id = {id}",
            id = sql_lit(OUTBOX_SEQ_ID)
        );
        let rows = self.executor.select_rows(&sql).await?;
        Ok(cell_i64(rows.first(), "seq"))
    }
}

#[async_trait]
impl CanonicalStore for ClickHouseCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "clickhouse"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        // Outbox: append-only MergeTree, ordered by event_seq. Plain INSERTs —
        // the append-optimised path ClickHouse is built for. `created_at` is a
        // non-load-bearing audit field; the seq is the durability coordinate.
        let outbox = self.qualified("udb_outbox_events")?;
        self.executor
            .execute_ddl(&format!(
                "CREATE TABLE IF NOT EXISTS {outbox} (\
                 event_seq Int64, \
                 event_id String, \
                 topic String, \
                 partition_key String, \
                 payload String, \
                 created_at DateTime64(3)\
                 ) ENGINE = MergeTree ORDER BY event_seq"
            ))
            .await
            .map_err(|e| format!("ensure_system_tables (clickhouse outbox) failed: {e}"))?;

        // Counter: ReplacingMergeTree keyed by id, deduped by the monotonic
        // `version` column. State change = INSERT a new (id, seq, version) row;
        // reads use FINAL to keep only the highest version. No in-place UPDATE.
        let counters = self.qualified("udb_counters")?;
        self.executor
            .execute_ddl(&format!(
                "CREATE TABLE IF NOT EXISTS {counters} (\
                 id String, \
                 seq Int64, \
                 version UInt64\
                 ) ENGINE = ReplacingMergeTree(version) ORDER BY id"
            ))
            .await
            .map_err(|e| format!("ensure_system_tables (clickhouse counters) failed: {e}"))?;
        Ok(())
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        // Seq allocation via the ReplacingMergeTree counter, emulating CAS:
        //   1. read current seq with FINAL (collapses superseded versions);
        //   2. INSERT a NEW counter row (id, seq=current+1, version=current+1) —
        //      ClickHouse never mutates in place, so this supersedes the prior
        //      version once FINAL/merge runs;
        //   3. re-read with FINAL to CONFIRM we observe at least our new seq
        //      (last-writer-by-version wins).
        //
        // SINGLE-WRITER CAVEAT (see module docs): two concurrent enqueues can
        // both read `current`, both insert `current+1`, and FINAL then collapses
        // the two `current+1` rows into one — yielding a duplicate event_seq.
        // The conformance run is single-threaded so this holds; multi-writer
        // hardening needs an external sequencer / Keeper lock.
        let current = self.current_counter_seq().await?;
        let next = current + 1;

        let counters = self.qualified("udb_counters")?;
        // version == seq: both are monotone and start at 1, so the highest seq is
        // always the highest version — FINAL therefore always surfaces the newest
        // allocation.
        self.executor
            .execute_ddl(&format!(
                "INSERT INTO {counters} (id, seq, version) VALUES ({id}, {next}, {next})",
                id = sql_lit(OUTBOX_SEQ_ID)
            ))
            .await
            .map_err(|e| format!("outbox seq counter insert failed: {e}"))?;

        // Re-read with FINAL to confirm our allocation is the live one. Under the
        // single-writer assumption this must observe `>= next`; if it somehow
        // reads back lower, surface it rather than handing out a colliding seq.
        let confirmed = self.current_counter_seq().await?;
        if confirmed < next {
            return Err(format!(
                "outbox seq allocation lost a race: inserted {next}, re-read {confirmed} \
                 (single-writer assumption violated — see ClickHouseCanonicalStore docs)"
            ));
        }

        // Insert the event row carrying the freshly-allocated seq. `now64(3)`
        // stamps the audit-only created_at server-side at millisecond precision.
        let outbox = self.qualified("udb_outbox_events")?;
        let payload_text = serde_json::to_string(payload)
            .map_err(|e| format!("outbox payload serialise failed: {e}"))?;
        self.executor
            .execute_ddl(&format!(
                "INSERT INTO {outbox} \
                 (event_seq, event_id, topic, partition_key, payload, created_at) \
                 VALUES ({next}, {eid}, {topic}, {pk}, {payload}, now64(3))",
                eid = sql_lit(event_id),
                topic = sql_lit(topic),
                pk = sql_lit(partition_key),
                payload = sql_lit(&payload_text),
            ))
            .await
            .map_err(|e| format!("outbox event insert failed: {e}"))?;
        Ok(next)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        // The counter is the authoritative high-water mark (it advances in
        // lock-step with the event INSERT and never regresses), so reading it via
        // FINAL is both correct and cheaper than `max(event_seq)` over the whole
        // outbox MergeTree.
        self.current_counter_seq().await
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let seq = self.outbox_max_seq().await?;
        Ok(DurabilityToken::new("clickhouse", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("clickhouse") {
            return Err(format!(
                "ClickHouseCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token.value.parse().map_err(|e| {
            format!(
                "malformed clickhouse durability token '{}': {e}",
                token.value
            )
        })?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, super::CLICKHOUSE_DURABILITY_POLL_MS);
        loop {
            if self.outbox_max_seq().await? >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        // Leases: ReplacingMergeTree keyed by lease_name, deduped by `version`.
        // Every state change (acquire / refresh / release) is an INSERT of a new
        // versioned row; reads use FINAL to see only the latest. A release writes
        // a tombstone row with an empty owner_id and a bumped version, which
        // supersedes the live row and frees the lease (no DELETE mutation needed).
        let leases = self.qualified("udb_advisory_leases")?;
        self.executor
            .execute_ddl(&format!(
                "CREATE TABLE IF NOT EXISTS {leases} (\
                 lease_name String, \
                 owner_id String, \
                 expires_at Int64, \
                 version UInt64\
                 ) ENGINE = ReplacingMergeTree(version) ORDER BY lease_name"
            ))
            .await
            .map_err(|e| format!("ensure_advisory_lease_table (clickhouse) failed: {e}"))?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // Versioned-CAS emulation of PG's
        //   INSERT … ON CONFLICT DO UPDATE … WHERE expired OR same-owner.
        //
        // Steps (all over the ReplacingMergeTree, no transaction / lock):
        //   1. read the current lease row via FINAL (latest version);
        //   2. DECIDE per the 6-case contract whether we may take it;
        //   3. to acquire, INSERT a new row with version+1 (our owner, new
        //      expires_at), then re-read FINAL and confirm the live row is ours.
        //
        // The 6 contract cases (mirrors PG / Neo4j):
        //   1. fresh (no row)              → acquire (insert version 1).
        //   2. live, different owner       → DENY → Ok(false).
        //   3. same owner, refresh         → acquire (bump expires_at).
        //   4. wrong-owner release         → handled in release (owner-scoped).
        //   5. owner release then reacquire→ release wrote an empty-owner
        //                                     tombstone; a later acquire sees an
        //                                     empty owner (treated as free) → acquire.
        //   6. zero-ttl takeover           → ttl=0 ⇒ expires_at = now; a later
        //                                     acquirer sees expires_at <= now → acquire.
        //
        // SINGLE-WRITER CAVEAT (see module docs): the read-decide-insert-reread is
        // NOT atomic, so two acquirers racing the same fresh/expired lease can
        // both insert version+1 and both believe they won. The conformance run is
        // single-threaded so the contract holds; a hardened path needs Keeper.
        let leases = self.qualified("udb_advisory_leases")?;
        let now = Self::now_unix_ms();
        let new_expires = now + (ttl.as_millis() as i64);

        // 1. Current live row via FINAL.
        let read_sql = format!(
            "SELECT owner_id, expires_at, version FROM {leases} FINAL \
             WHERE lease_name = {name}",
            name = sql_lit(lease_name)
        );
        let rows = self.executor.select_rows(&read_sql).await?;
        let current = rows.first();
        let cur_owner = cell_str(current, "owner_id");
        let cur_expires = cell_i64(current, "expires_at");
        let cur_version = cell_u64(current, "version");

        // 2. Decide. A row with an empty owner_id is a release tombstone → free.
        // A row whose expires_at <= now is expired → free. Same owner always
        // refreshes. A live row owned by someone else denies.
        let is_free = current.is_none() || cur_owner.is_empty() || cur_expires <= now;
        let may_acquire = is_free || cur_owner == owner_id;
        if !may_acquire {
            // Case 2: live lease held by a different owner.
            return Ok(false);
        }

        // 3. Acquire by inserting a superseding version. version starts at 1 for a
        // fresh lease; otherwise bump the latest version we read.
        let next_version = cur_version.saturating_add(1).max(1);
        self.executor
            .execute_ddl(&format!(
                "INSERT INTO {leases} (lease_name, owner_id, expires_at, version) \
                 VALUES ({name}, {owner}, {exp}, {ver})",
                name = sql_lit(lease_name),
                owner = sql_lit(owner_id),
                exp = new_expires,
                ver = next_version,
            ))
            .await
            .map_err(|e| format!("try_acquire_advisory_lease insert (clickhouse) failed: {e}"))?;

        // Re-read FINAL and confirm the live row is ours (last-writer-by-version).
        let confirm = self.executor.select_rows(&read_sql).await?;
        let won = cell_str(confirm.first(), "owner_id") == owner_id;
        Ok(won)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped release: read the live row via FINAL; only supersede it
        // when WE are the current owner (contract case 4 — a wrong-owner release
        // must be a no-op and must NOT free the lease). Releasing = INSERT a
        // tombstone row (empty owner_id) with version+1, which FINAL surfaces as
        // the new live row; `try_acquire_advisory_lease` treats an empty owner as
        // free. We do NOT issue an `ALTER TABLE … DELETE` mutation (async + heavy);
        // the tombstone is the cheap, immediately-visible release.
        let leases = self.qualified("udb_advisory_leases")?;
        let read_sql = format!(
            "SELECT owner_id, version FROM {leases} FINAL WHERE lease_name = {name}",
            name = sql_lit(lease_name)
        );
        let rows = self.executor.select_rows(&read_sql).await?;
        let current = rows.first();
        let cur_owner = cell_str(current, "owner_id");
        // Wrong-owner (or already-free) release is a no-op.
        if cur_owner != owner_id {
            return Ok(());
        }
        let next_version = cell_u64(current, "version").saturating_add(1).max(1);
        // Empty owner_id + expires_at 0 = free tombstone.
        self.executor
            .execute_ddl(&format!(
                "INSERT INTO {leases} (lease_name, owner_id, expires_at, version) \
                 VALUES ({name}, '', 0, {ver})",
                name = sql_lit(lease_name),
                ver = next_version,
            ))
            .await
            .map_err(|e| format!("release_advisory_lease insert (clickhouse) failed: {e}"))?;
        Ok(())
    }
}

// ── JSONCompact cell helpers ──────────────────────────────────────────────────
//
// `ClickHouseExecutor::select_rows` returns `Vec<Json>` where each row is a JSON
// object keyed by column name (it zips the JSONCompact `meta` names onto the
// `data` cells). ClickHouse renders `Int64` as a JSON number, but `UInt64`
// (our `version`) is rendered as a JSON *string* in JSONCompact to preserve
// precision beyond 2^53, so the helpers accept both number and string forms.

/// Read an `i64` cell (e.g. `seq`, `expires_at`) from an optional row → 0 when
/// the row / cell is absent or unparseable.
fn cell_i64(row: Option<&Json>, key: &str) -> i64 {
    let Some(cell) = row.and_then(|r| r.get(key)) else {
        return 0;
    };
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse::<i64>().ok()))
        .unwrap_or(0)
}

/// Read a `UInt64` cell (`version`). JSONCompact renders UInt64 as a string, so
/// prefer the string form, falling back to a JSON number.
fn cell_u64(row: Option<&Json>, key: &str) -> u64 {
    let Some(cell) = row.and_then(|r| r.get(key)) else {
        return 0;
    };
    cell.as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| cell.as_u64())
        .unwrap_or(0)
}

/// Read a `String` cell (`owner_id`) → empty string when absent.
fn cell_str(row: Option<&Json>, key: &str) -> String {
    row.and_then(|r| r.get(key))
        .and_then(Json::as_str)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::executors::clickhouse::ClickHouseConfig;

    fn dummy_store() -> ClickHouseCanonicalStore {
        let exec = ClickHouseExecutor::new(ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: String::new(),
            database: "udb".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        });
        ClickHouseCanonicalStore::new(exec, "primary", "udb")
    }

    /// Pin: backend label is `"clickhouse"` exactly (registry key + token
    /// identity).
    #[test]
    fn backend_label_is_pinned() {
        let store = dummy_store();
        assert_eq!(store.backend_label(), "clickhouse");
        assert_eq!(store.instance_name(), "primary");
    }

    /// Pin: a foreign-backend token is rejected before any HTTP call.
    #[tokio::test]
    async fn wait_for_token_rejects_foreign_backend() {
        let store = dummy_store();
        let foreign = DurabilityToken::new("postgres", "0/100");
        let err = store
            .wait_for_token(&foreign, Duration::from_millis(1))
            .await
            .expect_err("foreign token must be rejected");
        assert!(err.contains("cannot wait on"));
    }

    /// Pin: a malformed (non-integer) clickhouse token surfaces as an error, not
    /// a silent hang.
    #[tokio::test]
    async fn wait_for_token_rejects_malformed_value() {
        let store = dummy_store();
        let bad = DurabilityToken::new("clickhouse", "not-an-int");
        let err = store
            .wait_for_token(&bad, Duration::from_millis(1))
            .await
            .expect_err("malformed token must error");
        assert!(err.contains("malformed"));
    }

    /// Pin: an unsafe database name is rejected before any SQL is built.
    #[test]
    fn unsafe_database_is_rejected() {
        let exec = ClickHouseExecutor::new(ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: String::new(),
            database: "udb".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        });
        let store = ClickHouseCanonicalStore::new(exec, "primary", "evil`; DROP");
        assert!(store.qualified("udb_counters").is_err());
    }

    /// Pin: JSONCompact UInt64-as-string and Int64-as-number both decode.
    #[test]
    fn cell_helpers_accept_string_and_number_forms() {
        let row = serde_json::json!({
            "seq": 42,
            "version": "7",
            "owner_id": "owner-a",
        });
        assert_eq!(cell_i64(Some(&row), "seq"), 42);
        assert_eq!(cell_u64(Some(&row), "version"), 7);
        assert_eq!(cell_str(Some(&row), "owner_id"), "owner-a");
        // Absent cells default cleanly.
        assert_eq!(cell_i64(Some(&row), "missing"), 0);
        assert_eq!(cell_u64(None, "version"), 0);
        assert_eq!(cell_str(None, "owner_id"), "");
    }
}
