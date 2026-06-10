//! `MssqlCanonicalStore` — B.8 PHASE 1. SQL Server (Tiberius) backed
//! [`CanonicalStore`](super::CanonicalStore) implementation.
//!
//! This is the **base** canonical-store surface only: durability token,
//! outbox, advisory leases, and `ensure_system_tables`. The four
//! system-store traits (`ProjectionTaskStore` / `SagaStore` /
//! `AdminAuditStore` / `MigrationAuditStore`) are PHASE 2 and are NOT
//! implemented here, so this store is not yet registered into the
//! runtime `CanonicalStoreRegistry` (that happens only after the full
//! `SystemStores` conformance passes).
//!
//! ## Why Tiberius, not sqlx
//!
//! SQL Server speaks TDS; UDB's SQL Server executor
//! ([`MssqlClient`](crate::runtime::executors::mssql::MssqlClient))
//! wraps the canonical `tiberius` driver. This store reuses that same
//! client (and its lazy pool / auto-reconnect) via the thin
//! `pub(crate)` query helpers (`fetch_rows` / `execute_sql` /
//! `simple_batch`) rather than opening a second connection layer.
//!
//! ## Durability token strategy
//!
//! SQL Server's write-progress token is the canonical outbox high-water mark.
//! The `event_seq` IDENTITY value is assigned by the write transaction itself,
//! so `current_durability_token` returns `MAX(event_seq)` and `wait_for_token`
//! polls that value instead of comparing wall-clock time.

use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::{CanonicalStore, DurabilityToken};
use crate::runtime::executors::mssql::{MssqlClient, SqlParam};

/// Poll interval used while waiting for the outbox high-water mark to advance.
const MSSQL_DURABILITY_POLL_MS: u64 = 10;

pub struct MssqlCanonicalStore {
    /// `pub(super)` so the sibling system-store impl files
    /// (`mssql_projection`, `mssql_saga`, `mssql_admin_audit`,
    /// `mssql_migration_audit`) can reach the client — mirrors how
    /// `PostgresCanonicalStore` exposes its pool to `postgres_*`.
    pub(super) client: MssqlClient,
    pub(super) instance_name: String,
    /// Outbox relation (object name, e.g. `udb_outbox_events`). Guarded
    /// by [`Self::safe_relation`].
    pub(super) outbox_relation: String,
    /// B.8 phase 2 — optional per-table relation overrides for the four
    /// system-store traits. `None` falls back to the canonical default
    /// object name (`udb_projection_tasks`, …). Validated by
    /// [`Self::safe_object_name`] before interpolation.
    pub(super) projection_relation: Option<String>,
    pub(super) saga_relation: Option<String>,
    pub(super) admin_audit_relation: Option<String>,
    pub(super) migration_runs_relation: Option<String>,
    pub(super) migration_ledger_relation: Option<String>,
}

impl MssqlCanonicalStore {
    pub fn new(
        client: MssqlClient,
        instance_name: impl Into<String>,
        outbox_relation: impl Into<String>,
    ) -> Self {
        Self {
            client,
            instance_name: instance_name.into(),
            outbox_relation: outbox_relation.into(),
            projection_relation: None,
            saga_relation: None,
            admin_audit_relation: None,
            migration_runs_relation: None,
            migration_ledger_relation: None,
        }
    }

    /// Accessor for sibling impl files.
    pub(super) fn client(&self) -> &MssqlClient {
        &self.client
    }

    /// Validate an interpolated object name (relation) the same way
    /// [`Self::safe_relation`] guards the outbox relation: only
    /// alphanumerics / `_` / `.` / bracket quoting are allowed, so no
    /// caller-supplied relation can carry injection. Returns the borrowed
    /// name on success.
    pub(super) fn safe_object_name(name: &str) -> Result<&str, String> {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '[' || c == ']')
        {
            return Err(format!("unsafe relation name '{name}'"));
        }
        Ok(name)
    }

    /// Validate the relation name to avoid SQL injection through the
    /// operator-supplied `outbox_relation`. Mirrors the Postgres guard
    /// but allows the bracketed `[ident]` form SQL Server uses for
    /// quoting in addition to alphanumerics / `_` / `.`.
    fn safe_relation(&self) -> Result<&str, String> {
        let rel = self.outbox_relation.as_str();
        if rel.is_empty()
            || !rel
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '[' || c == ']')
        {
            return Err(format!("unsafe outbox_relation '{rel}'"));
        }
        Ok(rel)
    }
}

#[async_trait]
impl CanonicalStore for MssqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mssql"
    }

    fn instance_name(&self) -> &str {
        &self.instance_name
    }

    async fn current_durability_token(&self) -> Result<DurabilityToken, String> {
        let seq = self.outbox_max_seq().await?;
        Ok(DurabilityToken::new("mssql", seq.to_string()))
    }

    async fn wait_for_token(
        &self,
        token: &DurabilityToken,
        timeout: Duration,
    ) -> Result<bool, String> {
        if !token.is_for("mssql") {
            return Err(format!(
                "MssqlCanonicalStore cannot wait on a '{}' token",
                token.backend_label
            ));
        }
        let target: i64 = token
            .value
            .parse()
            .map_err(|e| format!("malformed mssql durability token '{}': {e}", token.value))?;
        let started = Instant::now();
        let poll = super::durability_poll_interval(timeout, MSSQL_DURABILITY_POLL_MS);
        loop {
            let current = self.current_durability_token().await?;
            // Both values are decimal outbox `event_seq` high-water marks minted
            // by the same store, so a numeric compare is correct.
            let cur: i64 = current
                .value
                .parse()
                .map_err(|e| format!("malformed current mssql token '{}': {e}", current.value))?;
            if cur >= target {
                return Ok(true);
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
        // `event_id` is a UNIQUEIDENTIFIER column; the parameter arrives
        // as text, so CONVERT(UNIQUEIDENTIFIER, @P1) casts it explicitly
        // (tiberius binds @P1..@Pn positionally). `OUTPUT inserted.event_seq`
        // returns the IDENTITY-assigned sequence (T-SQL's RETURNING).
        // `payload` is bound as text into the NVARCHAR(MAX)/ISJSON column.
        let sql = format!(
            "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
             OUTPUT inserted.event_seq \
             VALUES (CONVERT(UNIQUEIDENTIFIER, @P1), @P2, @P3, @P4, SYSUTCDATETIME())"
        );
        let params = [
            SqlParam::from_json(&serde_json::Value::String(event_id.to_string())),
            SqlParam::from_json(&serde_json::Value::String(topic.to_string())),
            SqlParam::from_json(&serde_json::Value::String(partition_key.to_string())),
            SqlParam::from_json(&serde_json::Value::String(payload.to_string())),
        ];
        let rows = self.client.fetch_rows(&sql, &params).await?;
        let seq: i64 = rows
            .first()
            .ok_or_else(|| "outbox insert returned no event_seq".to_string())?
            .try_get::<i64, _>(0)
            .map_err(|e| format!("outbox event_seq decode failed: {e}"))?
            .ok_or_else(|| "outbox event_seq was NULL".to_string())?;
        Ok(seq)
    }

    async fn outbox_max_seq(&self) -> Result<i64, String> {
        let rel = self.safe_relation()?;
        // ISNULL(...) so an empty table reports 0 rather than NULL.
        let sql = format!("SELECT ISNULL(MAX(event_seq), 0) FROM {rel}");
        let rows = self.client.fetch_rows(&sql, &[]).await?;
        let max: i64 = rows
            .first()
            .ok_or_else(|| "outbox max seq query returned no rows".to_string())?
            .try_get::<i64, _>(0)
            .map_err(|e| format!("outbox max seq decode failed: {e}"))?
            .unwrap_or(0);
        Ok(max)
    }

    async fn ensure_system_tables(&self) -> Result<(), String> {
        let rel = self.safe_relation()?;
        // B.8: outbox DDL comes from the shared `sql_schema` renderer
        // (single source of truth across SQL backends). The T-SQL is a
        // guarded multi-statement batch, so `simple_batch` is the right
        // primitive.
        let sql = super::sql_schema::mssql_outbox_ddl(rel);
        self.client.simple_batch(&sql).await
    }

    async fn ensure_advisory_lease_table(&self) -> Result<(), String> {
        let sql = super::sql_schema::mssql_advisory_lease_ddl();
        self.client.simple_batch(&sql).await
    }

    async fn try_acquire_advisory_lease(
        &self,
        lease_name: &str,
        owner_id: &str,
        ttl: std::time::Duration,
    ) -> Result<bool, String> {
        // Atomic acquire via T-SQL MERGE — the SQL Server equivalent of
        // Postgres' `INSERT … ON CONFLICT DO UPDATE`. The semantics must
        // match the contract EXACTLY (see conformance.rs cases):
        //   * no row              → INSERT (fresh acquire succeeds)
        //   * matched + expired   → UPDATE owner/expiry (takeover)
        //   * matched + same owner→ UPDATE expiry (heartbeat refresh)
        //   * matched + live + different owner → DO NOTHING (deny)
        // We OUTPUT the post-merge owner_id and compare it to the caller;
        // a denied merge changes nothing, so the existing (foreign) owner
        // is returned → Ok(false). `HOLDLOCK` serialises concurrent merges
        // on the same key, closing the read-modify-write race.
        //
        // The `@ttl` is bound as an int; expiry is computed server-side as
        // DATEADD(SECOND, @ttl, SYSUTCDATETIME()) so a ttl=0 lease is born
        // already-expired and is taken over by the next acquirer.
        let ttl_secs = ttl.as_secs() as i64;
        let sql = "\
            DECLARE @result TABLE (owner_id NVARCHAR(255)); \
            MERGE udb_advisory_leases WITH (HOLDLOCK) AS target \
            USING (SELECT @P1 AS lease_name, @P2 AS owner_id, @P3 AS ttl) AS src \
            ON target.lease_name = src.lease_name \
            WHEN MATCHED AND (target.expires_at < SYSUTCDATETIME() OR target.owner_id = src.owner_id) \
                THEN UPDATE SET owner_id = src.owner_id, \
                               expires_at = DATEADD(SECOND, src.ttl, SYSUTCDATETIME()) \
            WHEN NOT MATCHED \
                THEN INSERT (lease_name, owner_id, expires_at) \
                     VALUES (src.lease_name, src.owner_id, DATEADD(SECOND, src.ttl, SYSUTCDATETIME())) \
            OUTPUT inserted.owner_id INTO @result; \
            SELECT owner_id FROM @result; \
        ";
        let params = [
            SqlParam::Str(lease_name.to_string()),
            SqlParam::Str(owner_id.to_string()),
            SqlParam::Int(ttl_secs),
        ];
        let rows = self.client.fetch_rows(sql, &params).await?;
        // A denied merge produces no `inserted` row → empty result set →
        // the caller did not get the lease. A fired INSERT/UPDATE outputs
        // the post-merge owner; it equals the caller iff they hold it.
        let resulting_owner: Option<String> = match rows.first() {
            Some(row) => row
                .try_get::<&str, _>(0)
                .map_err(|e| format!("advisory-lease MERGE owner decode failed: {e}"))?
                .map(|s| s.to_string()),
            None => None,
        };
        Ok(matches!(resulting_owner, Some(o) if o == owner_id))
    }

    async fn release_advisory_lease(&self, lease_name: &str, owner_id: &str) -> Result<(), String> {
        // Owner-scoped delete: a wrong-owner release matches no row and is
        // a no-op, exactly like the Postgres/MySQL impls.
        let sql = "DELETE FROM udb_advisory_leases WHERE lease_name = @P1 AND owner_id = @P2";
        let params = [
            SqlParam::Str(lease_name.to_string()),
            SqlParam::Str(owner_id.to_string()),
        ];
        self.client.execute_sql(sql, &params).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin: backend label is `"mssql"` exactly. The registry uses it as
    /// a key and `DurabilityToken::backend_label` depends on it.
    #[test]
    fn backend_label_is_pinned() {
        let store = MssqlCanonicalStore::new(
            MssqlClient::new("Server=localhost,1433;User=sa;Password=x;"),
            "primary",
            "udb_outbox_events",
        );
        assert_eq!(store.backend_label(), "mssql");
        assert_eq!(store.instance_name(), "primary");
    }

    /// Pin: unsafe relation names are rejected before reaching tiberius.
    #[test]
    fn unsafe_relation_is_rejected() {
        let store =
            MssqlCanonicalStore::new(MssqlClient::new("Server=x;"), "primary", "evil; DROP");
        assert!(store.safe_relation().is_err());
    }

    /// Pin: the bracketed `[ident]` form SQL Server uses for quoting is
    /// accepted by the relation guard.
    #[test]
    fn bracketed_relation_is_allowed() {
        let store = MssqlCanonicalStore::new(
            MssqlClient::new("Server=x;"),
            "primary",
            "[dbo].[udb_outbox_events]",
        );
        assert!(store.safe_relation().is_ok());
    }

    /// Pin: cross-backend tokens are rejected (a PG receipt must not be
    /// waited on against a SQL Server store).
    #[tokio::test]
    async fn rejects_non_mssql_token() {
        let store = MssqlCanonicalStore::new(
            MssqlClient::new("Server=x;"),
            "primary",
            "udb_outbox_events",
        );
        let pg_token = DurabilityToken::new("postgres", "0/100");
        let err = store
            .wait_for_token(&pg_token, Duration::from_millis(1))
            .await
            .expect_err("must reject cross-backend token");
        assert!(err.contains("cannot wait on a 'postgres'"), "got: {err}");
    }
}
