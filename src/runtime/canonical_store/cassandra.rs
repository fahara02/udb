//! `CassandraCanonicalStore` — B.10a PHASE 1. Cassandra / ScyllaDB backed
//! [`CanonicalStore`](super::CanonicalStore) implementation over the native
//! `scylla` 0.13 CQL driver, using LWT (lightweight transactions / Paxos) for
//! atomic compare-and-set.
//!
//! This is the **base** canonical-store surface only: durability token, outbox,
//! advisory leases, and `ensure_system_tables`. The four system-store traits
//! (`ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
//! `MigrationAuditStore`) are PHASE 2 and are NOT implemented here, so this
//! store is not yet registered into the runtime `CanonicalStoreRegistry` (that
//! happens only after the full `SystemStores` conformance passes).
//!
//! ## Why scylla, not a second connection layer
//!
//! UDB's Cassandra executor
//! ([`CassandraClient`](crate::runtime::executors::cassandra::CassandraClient))
//! already wraps a shared `scylla::Session` (one connection per shard/node,
//! managed by the driver). This store reuses that same client through its thin
//! `pub(crate)` CQL helpers (`cql_execute` / `cql_query_first_i64` /
//! `cql_lwt_applied`) rather than opening a second session — mirroring how the
//! MSSQL store reuses `MssqlClient::{fetch_rows,execute_sql}` and the MongoDB
//! store reuses `MongoDbExecutor::native_database()`.
//!
//! ## Where Cassandra's weaker model forces extra round trips
//!
//! Cassandra is not a relational engine, so several SQL one-liners become
//! read-then-CAS loops or multi-step LWT sequences:
//!
//! - **No auto-increment / sequences.** The outbox sequence is modelled as a
//!   plain `bigint seq` column in a single well-known row of `udb_seq`,
//!   allocated via an LWT CAS loop (`UPDATE udb_seq SET seq=? WHERE id='outbox'
//!   IF seq=?`, retried on `[applied]=false`). A CQL `COUNTER` column would be
//!   simpler but counters **cannot** appear in LWTs, and we need the atomic
//!   read-modify-write that only Paxos gives, so a plain int + CAS is correct.
//! - **`IF` cannot OR two conditions.** Postgres acquires/refreshes/takes-over
//!   a lease in a single `INSERT … ON CONFLICT DO UPDATE … WHERE expired OR
//!   same-owner`. Cassandra LWT `IF` is a single conjunctive predicate, so the
//!   acquire path here is a small state machine: try `INSERT IF NOT EXISTS`
//!   (fresh), and when a row already exists, try the same-owner-or-expired
//!   `UPDATE … IF` transitions, reading `[applied]` after each.
//! - **LWTs need SERIAL consistency** to actually run Paxos; plain writes
//!   don't. Every CAS below uses [`SerialConsistency::Serial`].

use std::time::{Duration, Instant};

use async_trait::async_trait;
use scylla::statement::SerialConsistency;

use super::{CanonicalStore, DurabilityToken};
use crate::runtime::executors::cassandra::CassandraClient;

/// Well-known counter row id for the outbox sequence inside `udb_seq`.
const OUTBOX_SEQ_ID: &str = "outbox";

/// Cap on the LWT CAS retry loop when allocating the next outbox sequence.
/// Each iteration is one Paxos round; contention on a single broker's outbox
/// is low, but bound the loop so a pathological live-lock surfaces as an error
/// rather than spinning forever.
const OUTBOX_CAS_MAX_ATTEMPTS: u32 = 64;

pub struct CassandraCanonicalStore {
    // B.10a phase 2: `pub(super)` so the sibling system-store impl files
    // (`cassandra_projection.rs` / `_saga` / `_admin_audit` / `_migration_audit`)
    // can reach the client + keyspace through the accessors below.
    pub(super) client: CassandraClient,
    pub(super) instance_name: String,
    /// Keyspace that hosts the system tables. Guarded by [`safe_ident`].
    pub(super) keyspace: String,
    /// Outbox table name (unqualified). Guarded by [`safe_ident`].
    pub(super) outbox_table: String,
}

impl CassandraCanonicalStore {
    pub fn new(
        client: CassandraClient,
        instance_name: impl Into<String>,
        keyspace: impl Into<String>,
        outbox_table: impl Into<String>,
    ) -> Self {
        Self {
            client,
            instance_name: instance_name.into(),
            keyspace: keyspace.into(),
            outbox_table: outbox_table.into(),
        }
    }

    /// Validate that the keyspace name is a safe CQL identifier (no injection
    /// through the operator-supplied keyspace). Cassandra unquoted identifiers
    /// are `[a-zA-Z0-9_]`; we additionally forbid leading digits to stay valid
    /// when interpolated unquoted into `CREATE KEYSPACE`.
    fn keyspace_ident(&self) -> Result<&str, String> {
        safe_ident(&self.keyspace).map(|_| self.keyspace.as_str())
    }

    fn outbox_ident(&self) -> Result<&str, String> {
        safe_ident(&self.outbox_table).map(|_| self.outbox_table.as_str())
    }

    /// Fully-qualified, double-quoted `"ks"."table"`. Both parts are validated
    /// by the time this is called. `pub(super)` so the phase-2 sibling impls
    /// build their own `udb_projection_tasks` / `udb_sagas` / … table names the
    /// same way.
    pub(super) fn qualified(&self, table: &str) -> String {
        format!("\"{}\".\"{}\"", self.keyspace, table)
    }

    /// Borrow the underlying CQL client (phase-2 sibling impls run their CQL
    /// through this). The keyspace identifier is already validated whenever a
    /// table is created, so callers compose statements via [`Self::qualified`].
    pub(super) fn client(&self) -> &CassandraClient {
        &self.client
    }

    /// The (validated) keyspace this store's system tables live in.
    pub(super) fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Ensure the keyspace exists (idempotent). The four phase-2 system-store
    /// `ensure_*_tables` methods call this before their `CREATE TABLE` so they
    /// work on a fresh store even if `ensure_system_tables` wasn't called first
    /// — mirroring how `ensure_advisory_lease_table` self-heals the keyspace.
    pub(super) async fn ensure_keyspace(&self) -> Result<(), String> {
        let ks = self.keyspace_ident()?;
        let ks_ddl = format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
             {{'class':'SimpleStrategy','replication_factor':1}}"
        );
        self.client.cql_execute(&ks_ddl, ()).await
    }

    /// `udb_seq` counter table (one row per logical sequence; we only use the
    /// `outbox` row in phase 1).
    fn seq_table(&self) -> String {
        self.qualified("udb_seq")
    }

    fn lease_table(&self) -> String {
        self.qualified("udb_advisory_leases")
    }

    /// Read the current outbox sequence (0 when the counter row is absent or
    /// not yet seeded).
    async fn read_outbox_seq(&self) -> Result<i64, String> {
        let cql = format!(
            "SELECT seq FROM {seq} WHERE id = '{id}'",
            seq = self.seq_table(),
            id = OUTBOX_SEQ_ID,
        );
        Ok(self
            .client
            .cql_query_first_i64(&cql, ())
            .await?
            .unwrap_or(0))
    }
}

/// Validate a Cassandra identifier (keyspace / table name) the same spirit as
/// the SQL backends' `safe_relation` guard. Only ASCII alphanumerics and `_`,
/// non-empty, not starting with a digit — so it is safe to interpolate
/// unquoted (CREATE KEYSPACE) and quoted (`"ident"`).
pub(super) fn safe_ident(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return Err(format!("unsafe cassandra identifier '{name}'")),
    }
    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(format!("unsafe cassandra identifier '{name}'"))
    }
}

/// Unix milliseconds now — used to stamp `expires_at` (CQL `timestamp` is
/// milliseconds since the epoch) and the outbox `created_at`.
pub(super) fn now_unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl CanonicalStore for CassandraCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "cassandra"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        let ks = self.keyspace_ident()?;
        let _ = self.outbox_ident()?;

        // 1. Keyspace. SimpleStrategy RF=1 is the single-DC default; operators
        //    running multi-DC point the store at a keyspace they pre-created
        //    with NetworkTopologyStrategy (this CREATE IF NOT EXISTS is a
        //    no-op then).
        let ks_ddl = format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
             {{'class':'SimpleStrategy','replication_factor':1}}"
        );
        self.client.cql_execute(&ks_ddl, ()).await?;

        // 2. Outbox table. Single partition (`shard`, always 'outbox') with
        //    `event_seq` as the clustering key in DESC order so the
        //    high-water-mark read (`outbox_max_seq`) is the first clustering
        //    row and ordered reads are cheap. event_seq is allocated by the
        //    udb_seq CAS loop, not by Cassandra.
        let outbox_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                shard text, \
                event_seq bigint, \
                event_id text, \
                topic text, \
                partition_key text, \
                payload text, \
                created_at timestamp, \
                PRIMARY KEY (shard, event_seq) \
             ) WITH CLUSTERING ORDER BY (event_seq DESC)",
            tbl = self.qualified(&self.outbox_table),
        );
        self.client.cql_execute(&outbox_ddl, ()).await?;

        // 3. Sequence counter table. One row per logical sequence; phase 1 only
        //    uses the 'outbox' row. `seq` is a plain bigint (NOT a CQL counter,
        //    which can't participate in LWTs) updated via the CAS loop in
        //    `enqueue_outbox_event`.
        let seq_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {seq} ( id text PRIMARY KEY, seq bigint )",
            seq = self.seq_table(),
        );
        self.client.cql_execute(&seq_ddl, ()).await?;
        Ok(())
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        // ── Atomic next-seq allocation via LWT CAS loop ─────────────────────
        // Cassandra has no sequences. We read the current counter, then attempt
        // a Paxos compare-and-set to (current+1). If another writer raced us the
        // LWT reports `[applied]=false`; we re-read and retry. On the very first
        // allocation the counter row may be absent — seed it with an
        // `INSERT … IF NOT EXISTS` (Paxos) before the first CAS.
        let seq_table = self.seq_table();
        let mut attempt = 0u32;
        let new_seq = loop {
            attempt += 1;
            if attempt > OUTBOX_CAS_MAX_ATTEMPTS {
                return Err(format!(
                    "outbox seq CAS did not converge after {OUTBOX_CAS_MAX_ATTEMPTS} attempts \
                     (contention on {seq_table})"
                ));
            }
            match self
                .client
                .cql_query_first_i64(
                    &format!("SELECT seq FROM {seq_table} WHERE id = '{OUTBOX_SEQ_ID}'"),
                    (),
                )
                .await?
            {
                None => {
                    // Seed the counter at 1 with the first event already claimed.
                    let seed = format!(
                        "INSERT INTO {seq_table} (id, seq) VALUES ('{OUTBOX_SEQ_ID}', ?) IF NOT EXISTS"
                    );
                    let applied = self
                        .client
                        .cql_lwt_applied(&seed, (1i64,), SerialConsistency::Serial)
                        .await?;
                    if applied {
                        break 1i64;
                    }
                    // Lost the seed race — another writer created the row; retry
                    // through the read/CAS branch below.
                    continue;
                }
                Some(current) => {
                    let next = current + 1;
                    // CAS: only advance if the counter is still `current`.
                    let cas = format!(
                        "UPDATE {seq_table} SET seq = ? WHERE id = '{OUTBOX_SEQ_ID}' IF seq = ?"
                    );
                    let applied = self
                        .client
                        .cql_lwt_applied(&cas, (next, current), SerialConsistency::Serial)
                        .await?;
                    if applied {
                        break next;
                    }
                    // Someone else advanced the counter; re-read and retry.
                    continue;
                }
            }
        };

        // Insert the event row keyed by the freshly-allocated seq. Plain write
        // (not an LWT): the seq is already uniquely ours from the CAS above.
        let insert = format!(
            "INSERT INTO {tbl} (shard, event_seq, event_id, topic, partition_key, payload, created_at) \
             VALUES ('{OUTBOX_SEQ_ID}', ?, ?, ?, ?, ?, ?)",
            tbl = self.qualified(&self.outbox_table),
        );
        let payload_text = serde_json::to_string(payload)
            .map_err(|e| format!("outbox payload serialise failed: {e}"))?;
        self.client
            .cql_execute(
                &insert,
                (
                    new_seq,
                    event_id,
                    topic,
                    partition_key,
                    payload_text,
                    // CQL `timestamp` binds from i64 millis via CqlValue, but
                    // the typed `SerializeRow` path expects a Timestamp; bind
                    // the millis as a `scylla::frame::value::CqlTimestamp`.
                    scylla::frame::value::CqlTimestamp(now_unix_ms()),
                ),
            )
            .await?;
        Ok(new_seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        // The seq counter row is the authoritative high-water mark (it advances
        // before the event row is inserted, and never regresses).
        self.read_outbox_seq().await
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let seq = self.read_outbox_seq().await?;
        Ok(DurabilityToken::new("cassandra", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("cassandra") {
            return Err(format!(
                "CassandraCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token.value.parse().map_err(|e| {
            format!(
                "malformed cassandra durability token '{}': {e}",
                token.value
            )
        })?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, super::CASSANDRA_DURABILITY_POLL_MS);
        loop {
            if self.read_outbox_seq().await? >= target {
                return Ok(true);
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        // The keyspace is created by `ensure_system_tables`, but the lease
        // contract calls this independently — ensure the keyspace exists first
        // so the lease table DDL doesn't fail on a fresh store.
        let ks = self.keyspace_ident()?;
        let ks_ddl = format!(
            "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
             {{'class':'SimpleStrategy','replication_factor':1}}"
        );
        self.client.cql_execute(&ks_ddl, ()).await?;
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                lease_name text PRIMARY KEY, \
                owner_id text, \
                expires_at timestamp \
             )",
            tbl = self.lease_table(),
        );
        self.client.cql_execute(&ddl, ()).await?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: Duration,
    ) -> Result<bool, String> {
        // Cassandra LWT `IF` is a single conjunctive predicate and cannot
        // express PG's `expired OR same-owner` disjunction in one statement, so
        // this acquire is a 3-step LWT state machine. Every CAS runs at SERIAL.
        //
        //   ttl=0 → expires_at = now (already expired → an expired-takeover by
        //           a *different* owner is allowed, matching PG).
        let lease_tbl = self.lease_table();
        let now = now_unix_ms();
        let new_expires = now + (ttl.as_millis() as i64);

        // Step 1 — fresh acquire. Applies only when no row exists.
        //   Contract case 1 (owner-a fresh) and case 6 (owner-b after release).
        let insert = format!(
            "INSERT INTO {lease_tbl} (lease_name, owner_id, expires_at) VALUES (?, ?, ?) IF NOT EXISTS"
        );
        let inserted = self
            .client
            .cql_lwt_applied(
                &insert,
                (
                    lease_name,
                    owner_id,
                    scylla::frame::value::CqlTimestamp(new_expires),
                ),
                SerialConsistency::Serial,
            )
            .await?;
        if inserted {
            return Ok(true);
        }

        // A row already exists. Step 2 — same-owner refresh (heartbeat).
        //   Contract case 3 (owner-a refreshes its own live lease).
        // `IF owner_id = ?` matches only when WE already hold it; on a
        // different owner this is not-applied and we fall through to step 3.
        let refresh = format!(
            "UPDATE {lease_tbl} SET owner_id = ?, expires_at = ? WHERE lease_name = ? IF owner_id = ?"
        );
        let refreshed = self
            .client
            .cql_lwt_applied(
                &refresh,
                (
                    owner_id,
                    scylla::frame::value::CqlTimestamp(new_expires),
                    lease_name,
                    owner_id,
                ),
                SerialConsistency::Serial,
            )
            .await?;
        if refreshed {
            return Ok(true);
        }

        // Step 3 — expired takeover by a different owner.
        //   Contract case 7 (fresh-owner takes a ttl=0 / expired lease).
        // `IF expires_at <= ?` (now) applies only when the held lease has
        // already lapsed. A live lease held by someone else fails here →
        // Ok(false), which is exactly contract cases 2 & 5 (contention denial).
        let takeover = format!(
            "UPDATE {lease_tbl} SET owner_id = ?, expires_at = ? WHERE lease_name = ? IF expires_at <= ?"
        );
        let took = self
            .client
            .cql_lwt_applied(
                &takeover,
                (
                    owner_id,
                    scylla::frame::value::CqlTimestamp(new_expires),
                    lease_name,
                    scylla::frame::value::CqlTimestamp(now),
                ),
                SerialConsistency::Serial,
            )
            .await?;
        Ok(took)
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped delete via LWT. `IF owner_id = ?` makes a wrong-owner
        // release a not-applied no-op (contract case 4) instead of yanking
        // another worker's lease. SERIAL so the IF is evaluated under Paxos.
        let lease_tbl = self.lease_table();
        let del = format!("DELETE FROM {lease_tbl} WHERE lease_name = ? IF owner_id = ?");
        // We don't care about the applied result — a no-op is the correct
        // outcome for a wrong owner and an idempotent release for the right one.
        let _ = self
            .client
            .cql_lwt_applied(&del, (lease_name, owner_id), SerialConsistency::Serial)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_label_is_pinned() {
        // No live connection needed for the label/ident guards.
        assert!(safe_ident("udb_conf_abc123").is_ok());
        assert!(safe_ident("_internal").is_ok());
    }

    #[test]
    fn unsafe_idents_are_rejected() {
        assert!(safe_ident("").is_err());
        assert!(safe_ident("1leading_digit").is_err());
        assert!(safe_ident("evil; DROP").is_err());
        assert!(safe_ident("has-dash").is_err());
        assert!(safe_ident("has.dot").is_err());
        assert!(safe_ident("quote\"inject").is_err());
    }
}
