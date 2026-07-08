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
//! `MigrationAuditStore`). Runtime registration requires ClickHouse Keeper
//! support: advisory leases are stored in a KeeperMap table, and the outbox
//! sequence allocator runs under an internal Keeper-backed lease. Without
//! KeeperMap the store fails closed during `ensure_system_tables` instead of
//! advertising an HA-canonical posture.
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
//! - **Mutable state (the outbox sequence counter)** →
//!   a `ReplacingMergeTree(version)` keyed by id with a monotonic `version`
//!   column. We never UPDATE a row in place: to change state we INSERT a NEW
//!   row carrying `version + 1`, and we always READ the latest with
//!   `SELECT … FINAL` (which collapses superseded rows by the engine's
//!   replacing key, keeping only the highest `version`). Equivalent reads can
//!   use `argMax(col, version)`; this store uses `FINAL` for clarity.
//! - **Advisory leases** → a `KeeperMap` table keyed by lease name. Acquires use
//!   strict insert/update operations plus an owner token confirmation so racing
//!   acquirers have one observable winner. Releases are owner-scoped updates to a
//!   tombstone row; they never use async `ALTER DELETE` mutations.
//! - **"Compare-and-set"** is *emulated* by read-current-version →
//!   insert-version+1 → re-read-FINAL-to-confirm-we-won
//!   (last-writer-by-version wins).
//!
//! ## CONCURRENCY CAVEAT — read before reuse
//!
//! Because ClickHouse offers no atomic CAS, the read-insert-reread emulation
//! below would be unsafe if it were exposed directly to multiple writers. It is
//! now executed only while holding the internal KeeperMap sequence lease, so the
//! sequence path has a single writer per allocation. Every place this matters is
//! commented inline.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::Value as Json;
use uuid::Uuid;

use super::{CanonicalStore, DurabilityToken};
use crate::runtime::executors::clickhouse::ClickHouseExecutor;

/// Well-known id of the single outbox-sequence counter row in `udb_counters`.
const OUTBOX_SEQ_ID: &str = "outbox_seq";
const OUTBOX_SEQ_LOCK: &str = "__udb_clickhouse_outbox_seq";
const CLICKHOUSE_COUNTER_LOCK_TTL: Duration = Duration::from_secs(30);
const CLICKHOUSE_COUNTER_LOCK_WAIT: Duration = Duration::from_secs(30);
const CLICKHOUSE_COUNTER_LOCK_POLL: Duration = Duration::from_millis(50);
const CLICKHOUSE_SYSTEM_MUTATION_LOCK_TTL: Duration = Duration::from_secs(30);
const CLICKHOUSE_SYSTEM_MUTATION_LOCK_WAIT: Duration = Duration::from_secs(30);
const CLICKHOUSE_SYSTEM_MUTATION_LOCK_POLL: Duration = Duration::from_millis(50);
const KEEPER_LEASE_TABLE: &str = "udb_keeper_advisory_leases";
const KEEPER_LEASE_LIMIT: usize = 100_000;
const KEEPER_STRICT_SETTING: &str = "SETTINGS keeper_map_strict_mode=1";

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

fn safe_keeper_path_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().max(1));
    for ch in raw.chars().take(64) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "default".to_string()
    } else {
        out
    }
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

    fn keeper_lease_table(&self) -> Result<String, String> {
        self.qualified(KEEPER_LEASE_TABLE)
    }

    fn keeper_lease_path(&self) -> String {
        format!(
            "/udb/{}/{}/advisory_leases",
            safe_keeper_path_component(&self.database),
            safe_keeper_path_component(&self.instance_name)
        )
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

    async fn acquire_counter_sequence_lease(&self, owner_id: &str) -> Result<(), String> {
        let started = Instant::now();
        loop {
            if self
                .try_acquire_advisory_lease(OUTBOX_SEQ_LOCK, owner_id, CLICKHOUSE_COUNTER_LOCK_TTL)
                .await?
            {
                return Ok(());
            }
            if started.elapsed() >= CLICKHOUSE_COUNTER_LOCK_WAIT {
                return Err(format!(
                    "clickhouse outbox sequence lock was not acquired within {:?}",
                    CLICKHOUSE_COUNTER_LOCK_WAIT
                ));
            }
            tokio::time::sleep(CLICKHOUSE_COUNTER_LOCK_POLL).await;
        }
    }

    pub(super) async fn acquire_system_mutation_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        op: &str,
    ) -> Result<(), String> {
        let started = Instant::now();
        loop {
            if self
                .try_acquire_advisory_lease(
                    lease_name,
                    owner_id,
                    CLICKHOUSE_SYSTEM_MUTATION_LOCK_TTL,
                )
                .await?
            {
                return Ok(());
            }
            if started.elapsed() >= CLICKHOUSE_SYSTEM_MUTATION_LOCK_WAIT {
                return Err(format!(
                    "clickhouse {op} mutation lock '{lease_name}' was not acquired within {:?}",
                    CLICKHOUSE_SYSTEM_MUTATION_LOCK_WAIT
                ));
            }
            tokio::time::sleep(CLICKHOUSE_SYSTEM_MUTATION_LOCK_POLL).await;
        }
    }

    pub(super) async fn release_system_mutation_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        op: &str,
    ) -> Result<(), String> {
        self.release_advisory_lease(lease_name, owner_id)
            .await
            .map_err(|err| {
                format!("clickhouse {op} mutation lock '{lease_name}' release failed: {err}")
            })
    }

    fn keeper_insert_lease_sql(
        table: &str,
        lease_name: &str,
        owner_id: &str,
        expires_at: i64,
        token: &str,
    ) -> String {
        format!(
            "INSERT INTO {table} (lease_name, owner_id, expires_at, token) \
             {KEEPER_STRICT_SETTING} VALUES ({name}, {owner}, {expires}, {token})",
            name = sql_lit(lease_name),
            owner = sql_lit(owner_id),
            expires = expires_at,
            token = sql_lit(token),
        )
    }

    fn keeper_update_lease_sql(
        table: &str,
        lease_name: &str,
        owner_id: &str,
        expires_at: i64,
        token: &str,
        previous_token: &str,
    ) -> String {
        format!(
            "ALTER TABLE {table} UPDATE owner_id = {owner}, expires_at = {expires}, token = {token} \
             WHERE lease_name = {name} AND token = {prev_token} {KEEPER_STRICT_SETTING}",
            owner = sql_lit(owner_id),
            expires = expires_at,
            token = sql_lit(token),
            name = sql_lit(lease_name),
            prev_token = sql_lit(previous_token),
        )
    }

    fn keeper_select_lease_sql(table: &str, lease_name: &str) -> String {
        format!(
            "SELECT owner_id, expires_at, token FROM {table} WHERE lease_name = {name}",
            name = sql_lit(lease_name)
        )
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
        self.ensure_advisory_lease_table().await?;
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
        // The read-insert-confirm sequence is safe only while the internal
        // KeeperMap lease is held. If Keeper is unavailable, acquisition fails and
        // the store refuses to allocate a sequence rather than duplicating one.
        let sequence_owner = format!("outbox:{event_id}:{}", Uuid::new_v4());
        self.acquire_counter_sequence_lease(&sequence_owner).await?;
        let allocation = async {
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

            // Re-read with FINAL to confirm our allocation is the live one. The
            // Keeper lease should make this single-writer; if it reads lower,
            // fail closed rather than handing out a colliding seq.
            let confirmed = self.current_counter_seq().await?;
            if confirmed < next {
                return Err(format!(
                    "outbox seq allocation lost a race: inserted {next}, re-read {confirmed} \
                     (Keeper sequence lock invariant violated)"
                ));
            }
            Ok(next)
        }
        .await;
        let release = self
            .release_advisory_lease(OUTBOX_SEQ_LOCK, &sequence_owner)
            .await;
        if let Err(err) = release {
            return Err(format!(
                "clickhouse outbox sequence lock release failed: {err}"
            ));
        }
        let next = allocation?;

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
        // Leases: KeeperMap is backed by ClickHouse Keeper, not MergeTree parts.
        // Strict insert/update/delete semantics give the advisory-lease contract
        // one observable winner across broker processes. If the deployment lacks
        // KeeperMap configuration, this DDL fails and ClickHouse refuses to act as
        // an HA-canonical system store.
        let leases = self.keeper_lease_table()?;
        let keeper_path = self.keeper_lease_path();
        self.executor
            .execute_ddl(&format!(
                "CREATE TABLE IF NOT EXISTS {leases} (\
                 lease_name String, \
                 owner_id String, \
                 expires_at Int64, \
                 token String\
                 ) ENGINE = KeeperMap({path}, {limit}) PRIMARY KEY lease_name",
                path = sql_lit(&keeper_path),
                limit = KEEPER_LEASE_LIMIT,
            ))
            .await
            .map_err(|e| {
                format!(
                    "ensure_advisory_lease_table (clickhouse KeeperMap) failed: {e}; \
                     configure ClickHouse Keeper/KeeperMap for HA-canonical ClickHouse"
                )
            })?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        let leases = self.keeper_lease_table()?;
        let now = Self::now_unix_ms();
        let new_expires = now + (ttl.as_millis() as i64);
        let token = format!("{}:{now}:{}", owner_id, Uuid::new_v4());

        // Fast path for a fresh key. In strict KeeperMap mode this succeeds for
        // exactly one first acquirer. Duplicate-key failures fall through to the
        // refresh/expired-takeover path below.
        let insert_sql =
            Self::keeper_insert_lease_sql(&leases, lease_name, owner_id, new_expires, &token);
        match self.executor.execute_ddl(&insert_sql).await {
            Ok(()) => return Ok(true),
            Err(insert_err) => {
                let read_sql = Self::keeper_select_lease_sql(&leases, lease_name);
                let rows = self
                    .executor
                    .select_rows(&read_sql)
                    .await
                    .map_err(|read_err| {
                        format!(
                            "try_acquire_advisory_lease fresh insert failed ({insert_err}); \
                         current lease read also failed: {read_err}"
                        )
                    })?;
                if rows.first().is_none() {
                    return Err(format!(
                        "try_acquire_advisory_lease fresh insert failed and no existing \
                         KeeperMap row was visible: {insert_err}"
                    ));
                }
            }
        }

        let read_sql = Self::keeper_select_lease_sql(&leases, lease_name);
        let rows = self.executor.select_rows(&read_sql).await?;
        let current = rows.first();
        let cur_owner = cell_str(current, "owner_id");
        let cur_expires = cell_i64(current, "expires_at");
        let cur_token = cell_str(current, "token");

        // Decide. A row with an empty owner_id is a release tombstone → free. A
        // row whose expires_at <= now is expired → free. Same owner always
        // refreshes. A live row owned by someone else denies.
        let is_free = current.is_none() || cur_owner.is_empty() || cur_expires <= now;
        let may_acquire = is_free || cur_owner == owner_id;
        if !may_acquire {
            return Ok(false);
        }

        // Conditional update by the observed token. If another process refreshed
        // or took over after our read, the token no longer matches and our final
        // confirmation below returns false.
        let update_sql = Self::keeper_update_lease_sql(
            &leases,
            lease_name,
            owner_id,
            new_expires,
            &token,
            &cur_token,
        );
        self.executor.execute_ddl(&update_sql).await.map_err(|e| {
            format!("try_acquire_advisory_lease KeeperMap update (clickhouse) failed: {e}")
        })?;

        let confirm = self.executor.select_rows(&read_sql).await?;
        let won = cell_str(confirm.first(), "owner_id") == owner_id
            && cell_str(confirm.first(), "token") == token;
        Ok(won)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped release: only the current owner can convert the key into a
        // free tombstone. Wrong-owner release is a no-op and must not free the
        // lease for a competing process.
        let leases = self.keeper_lease_table()?;
        let read_sql = Self::keeper_select_lease_sql(&leases, lease_name);
        let rows = self.executor.select_rows(&read_sql).await?;
        let current = rows.first();
        let cur_owner = cell_str(current, "owner_id");
        if cur_owner != owner_id {
            return Ok(());
        }
        let cur_token = cell_str(current, "token");
        let release_token = format!("released:{}:{}", Self::now_unix_ms(), Uuid::new_v4());
        let release_sql =
            Self::keeper_update_lease_sql(&leases, lease_name, "", 0, &release_token, &cur_token);
        self.executor.execute_ddl(&release_sql).await.map_err(|e| {
            format!("release_advisory_lease KeeperMap update (clickhouse) failed: {e}")
        })?;
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
#[cfg(test)]
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

    #[test]
    fn keeper_lease_table_and_path_are_pinned() {
        let store = dummy_store();
        assert_eq!(
            store.keeper_lease_table().unwrap(),
            "`udb`.`udb_keeper_advisory_leases`"
        );
        assert_eq!(
            store.keeper_lease_path(),
            "/udb/udb/primary/advisory_leases"
        );

        let exec = ClickHouseExecutor::new(ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: String::new(),
            database: "udb".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        });
        let weird = ClickHouseCanonicalStore::new(exec, "primary/blue:1", "udb");
        assert_eq!(
            weird.keeper_lease_path(),
            "/udb/udb/primary_blue_1/advisory_leases"
        );
    }

    #[test]
    fn keeper_lease_sql_uses_strict_mode_and_token_cas() {
        let table = "`udb`.`udb_keeper_advisory_leases`";
        let insert = ClickHouseCanonicalStore::keeper_insert_lease_sql(
            table, "lease-a", "owner-a", 10, "t1",
        );
        assert!(insert.contains("INSERT INTO `udb`.`udb_keeper_advisory_leases`"));
        assert!(insert.contains("SETTINGS keeper_map_strict_mode=1 VALUES"));
        assert!(insert.contains("'lease-a'"));
        assert!(insert.contains("'owner-a'"));

        let update = ClickHouseCanonicalStore::keeper_update_lease_sql(
            table, "lease-a", "owner-b", 20, "t2", "t1",
        );
        assert!(update.starts_with("ALTER TABLE `udb`.`udb_keeper_advisory_leases` UPDATE"));
        assert!(update.contains("owner_id = 'owner-b'"));
        assert!(update.contains("token = 't2'"));
        assert!(update.contains("WHERE lease_name = 'lease-a' AND token = 't1'"));
        assert!(update.ends_with("SETTINGS keeper_map_strict_mode=1"));
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
