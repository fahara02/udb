//! `PostgresCanonicalStore` — wraps the existing PG-based behaviour
//! behind the [`CanonicalStore`](super::CanonicalStore) trait.
//!
//! This impl is intentionally **non-disruptive**: every operation
//! delegates to the same SQL the pre-P2P codebase already runs. The
//! point is to put the trait in front of the existing behaviour so
//! the rest of the runtime can migrate to the trait without each PR
//! also rewriting the SQL.
//!
//! Once the migration is complete, the inline `PgPool` references in
//! `runtime/saga.rs`, `runtime/projection/mod.rs`,
//! `runtime/migration_audit.rs`, and `runtime/consistency_fence.rs`
//! collapse into calls through this trait, and **none of the Postgres
//! behaviour changes**.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use sqlx::PgPool;

use super::{CanonicalStore, DurabilityToken};

pub struct PostgresCanonicalStore {
    pub(super) pool: PgPool,
    instance_name: String,
    /// Outbox relation (`udb_system.udb_outbox_events` by default).
    /// Mirrors `CdcConfig::outbox_relation()` so this impl writes to
    /// the same table the existing CDC tailer reads from.
    outbox_relation: String,
    /// Optional override for the projection_tasks relation. Set via
    /// [`Self::with_projection_relation`]. `None` falls back to
    /// `"udb_system"."udb_projection_tasks"`.
    pub(super) projection_relation: Option<String>,
    /// Optional override for the sagas relation. Set via
    /// [`Self::with_saga_relation`]. `None` falls back to
    /// `"udb_system"."udb_sagas"`.
    pub(super) saga_relation: Option<String>,
    /// Optional override for the admin audit log relation. Set via
    /// [`Self::with_admin_audit_relation`]. `None` falls back to
    /// `"udb_system"."udb_admin_audit_log"`.
    pub(super) admin_audit_relation: Option<String>,
    /// Optional override for migration audit relations. Set via
    /// [`Self::with_migration_relations`]. `None` falls back to
    /// `"udb_system"."udb_migration_runs"` /
    /// `"udb_system"."udb_migration_op_ledger"`.
    pub(super) migration_runs_relation: Option<String>,
    pub(super) migration_ledger_relation: Option<String>,
}

impl PostgresCanonicalStore {
    pub fn new(
        pool: PgPool,
        instance_name: impl Into<String>,
        outbox_relation: impl Into<String>,
    ) -> Self {
        Self {
            pool,
            instance_name: instance_name.into(),
            outbox_relation: outbox_relation.into(),
            projection_relation: None,
            saga_relation: None,
            admin_audit_relation: None,
            migration_runs_relation: None,
            migration_ledger_relation: None,
        }
    }

    /// Validate the relation name to avoid SQL injection through
    /// the operator-supplied `outbox_relation`. Identical guard
    /// used by `runtime/cdc/indoubt_recovery.rs`.
    fn safe_relation(&self) -> Result<&str, String> {
        let rel = self.outbox_relation.as_str();
        if rel.is_empty()
            || !rel
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '"')
        {
            return Err(format!("unsafe outbox_relation '{rel}'"));
        }
        Ok(rel)
    }
}

#[async_trait]
impl CanonicalStore for PostgresCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "postgres"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let lsn: String = sqlx::query_scalar("SELECT pg_current_wal_lsn()::TEXT")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("pg_current_wal_lsn() failed: {e}"))?;
        Ok(DurabilityToken::new("postgres", lsn))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("postgres") {
            return Err(format!(
                "PostgresCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target_lsn = token.value.clone();
        let started = Instant::now();
        let poll = crate::runtime::canonical_store::durability_poll_interval(
            timeout,
            crate::runtime::canonical_store::POSTGRES_DURABILITY_POLL_MS,
        );
        loop {
            // pg_last_wal_replay_lsn returns NULL on a primary;
            // pg_current_wal_lsn is always ≥ any replica LSN.
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT COALESCE(pg_last_wal_replay_lsn()::TEXT, pg_current_wal_lsn()::TEXT)",
            )
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("LSN poll failed: {e}"))?;
            if let Some((current,)) = row {
                // PG compares LSNs lexicographically when both are
                // canonical `hex/hex` strings, but sqlx can give us
                // the comparison directly via SQL — use a SQL-side
                // compare to avoid client-side parsing bugs.
                let cleared: bool = sqlx::query_scalar("SELECT $1::pg_lsn <= $2::pg_lsn")
                    .bind(&target_lsn)
                    .bind(&current)
                    .fetch_one(&self.pool)
                    .await
                    .unwrap_or(false);
                if cleared {
                    return Ok(true);
                }
            }
            if started.elapsed() >= timeout {
                return Ok(false);
            }
            tokio::time::sleep(poll).await;
        }
    }

    async fn enqueue_outbox_event(
        &self,
        event_id: &str,
        topic: &str,
        partition_key: &str,
        payload: &serde_json::Value,
    ) -> Result<i64, String> {
        let rel = self.safe_relation()?;
        // Use the same column order the existing CDC tailer reads.
        // `event_id` is a `UUID` column; the parameter arrives as text over the
        // wire, so cast it explicitly ($1::uuid). Without the cast PostgreSQL
        // rejects the bind ("column is of type uuid but expression is of type
        // text") — SQLite's TEXT column hid this, but real Postgres does not
        // implicitly cast a text parameter to uuid.
        let sql = format!(
            "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
             VALUES ($1::uuid, $2, $3, $4, NOW()) \
             RETURNING event_seq"
        );
        let event_seq: i64 = sqlx::query_scalar(&sql)
            .bind(event_id)
            .bind(topic)
            .bind(partition_key)
            .bind(payload)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("outbox insert failed: {e}"))?;
        Ok(event_seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        let rel = self.safe_relation()?;
        let sql = format!("SELECT COALESCE(MAX(event_seq), 0) FROM {rel}");
        let max: i64 = sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| format!("outbox max seq query failed: {e}"))?;
        Ok(max)
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        let rel = self.safe_relation()?;
        // Create the containing schema BEFORE the outbox table. The outbox
        // relation is schema-qualified (e.g. `"udb_system"."udb_outbox_events"`)
        // and `CREATE TABLE IF NOT EXISTS` does not create a missing schema, so
        // on a brand-new database the bare table DDL fails with
        // `schema "udb_system" does not exist`. That error is swallowed by the
        // best-effort registration in `setup_data::register_postgres`, so the
        // canonical store silently never registers and every saga/audit admin
        // RPC then returns UNAVAILABLE (masked as ~700 ms by client retries)
        // while health stays green. Derive the schema from the segment before
        // the first '.' (already quoted when the relation is quoted; a bare,
        // unqualified relation lives in the default schema and needs nothing).
        // Mirrors the guard in `ensure_advisory_lease_table`; `safe_relation`
        // has already validated the characters, so the formatted name is safe.
        if let Some((schema, _table)) = rel.split_once('.') {
            let _ = sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
                .execute(&self.pool)
                .await;
        }
        // B.7: outbox DDL comes from the shared `sql_schema` renderer (single
        // source of truth across SQL backends); execute/error-handling below
        // is unchanged.
        let sql = super::sql_schema::postgres_outbox_ddl(rel);
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_system_tables (postgres) failed: {e}"))?;
        Ok(())
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS "udb_system"."udb_advisory_leases" (
                lease_name TEXT PRIMARY KEY,
                owner_id   TEXT NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL
            )
        "#;
        // Ensure the system schema exists first; ignore "already
        // exists" errors.
        let _ = sqlx::query(r#"CREATE SCHEMA IF NOT EXISTS "udb_system""#)
            .execute(&self.pool)
            .await;
        sqlx::query(sql)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure_advisory_lease_table (postgres) failed: {e}"))?;
        Ok(())
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // PG atomic acquire via INSERT … ON CONFLICT DO UPDATE. The WHERE clause
        // deliberately encodes TWO distinct, safe transitions (do not collapse or
        // reorder without re-reading both):
        //   1. `expires_at < NOW()`            → take over an EXPIRED lease.
        //   2. `owner_id = EXCLUDED.owner_id`  → the current owner REFRESHES its
        //                                         own TTL (heartbeat).
        // It must NEVER allow stealing a non-expired lease owned by someone else;
        // both branches preserve that (branch 2 only matches when we already own
        // it). The RETURNING owner_id + the equality check below confirm we hold
        // the lease (a non-firing UPDATE returns no row → Ok(false)).
        let ttl_secs = ttl.as_secs() as i64;
        let sql = r#"
            INSERT INTO "udb_system"."udb_advisory_leases" (lease_name, owner_id, expires_at)
            VALUES ($1, $2, NOW() + make_interval(secs => $3::double precision))
            ON CONFLICT (lease_name) DO UPDATE
              SET owner_id   = EXCLUDED.owner_id,
                  expires_at = EXCLUDED.expires_at
              WHERE "udb_system"."udb_advisory_leases".expires_at < NOW()
                 OR "udb_system"."udb_advisory_leases".owner_id = EXCLUDED.owner_id
            RETURNING owner_id
        "#;
        let resulting_owner: Option<String> = sqlx::query_scalar(sql)
            .bind(lease_name)
            .bind(owner_id)
            .bind(ttl_secs as f64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| format!("try_acquire_advisory_lease (postgres) failed: {e}"))?;
        match resulting_owner {
            Some(o) if o == owner_id => Ok(true),
            // `None` means the UPDATE didn't fire (existing live row)
            // — the lease is held by someone else.
            _ => Ok(false),
        }
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        let sql = r#"DELETE FROM "udb_system"."udb_advisory_leases" WHERE lease_name = $1 AND owner_id = $2"#;
        sqlx::query(sql)
            .bind(lease_name)
            .bind(owner_id)
            .execute(&self.pool)
            .await
            .map_err(|e| format!("release_advisory_lease (postgres) failed: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin: backend label is `"postgres"` exactly. Audit logs +
    /// `DurabilityToken::backend_label` depend on this.
    /// (`tokio::test` because PgPool::Drop requires a runtime.)
    #[tokio::test]
    async fn backend_label_is_pinned() {
        let pool = PgPool::connect_lazy("postgres://invalid:0/none").unwrap();
        let store = PostgresCanonicalStore::new(pool, "primary", "udb_system.udb_outbox_events");
        assert_eq!(store.backend_label(), "postgres");
        assert_eq!(store.instance_name(), "primary");
    }

    /// Pin: unsafe relation names are rejected before reaching sqlx.
    #[tokio::test]
    async fn unsafe_relation_is_rejected() {
        let pool = PgPool::connect_lazy("postgres://invalid:0/none").unwrap();
        let store = PostgresCanonicalStore::new(pool, "primary", "evil; DROP");
        assert!(store.safe_relation().is_err());
    }
}
