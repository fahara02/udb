//! PostgreSQL implementation of [`SagaStore`].
//!
//! Schema mirrors the existing PG `udb_sagas` table from
//! `runtime/system.rs` exactly: `UUID` saga_id, `JSONB` for steps +
//! compensations, `TIMESTAMPTZ` timestamps, the existing index on
//! `(tenant_id, status, updated_at DESC)`.
//!
//! SQL operations mirror the existing `runtime/saga.rs` helpers
//! verbatim (record/list/get/mark_reviewed/retry_compensation/
//! recovery_attempts increment) so the call-site migration in
//! NW1 step 3+ is a swap with no behaviour change.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{
    SqlDialect, apply_saga_summary_bucket, build_eq_where, normalize_limit_offset,
};
use super::postgres::PostgresCanonicalStore;
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

const DEFAULT_REL: &str = r#""udb_system"."udb_sagas""#;

impl PostgresCanonicalStore {
    /// Override the sagas relation. Defaults to
    /// `"udb_system"."udb_sagas"`.
    pub fn with_saga_relation(mut self, relation: impl Into<String>) -> Self {
        self.saga_relation = Some(relation.into());
        self
    }

    fn saga_relation_ref(&self) -> &str {
        self.saga_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

/// Upgrade carry-over: before the store and the data plane shared one
/// relation, store-recorded sagas (workflow sagas, admin retries) lived in
/// the default `"udb_system"."udb_sagas"`. Copy the still-actionable ones
/// (every non-terminal status) into the shared relation so recovery and the
/// workflow engine keep seeing them. Idempotent (`ON CONFLICT DO NOTHING`)
/// and a no-op when the legacy table does not exist; terminal history stays
/// queryable in the legacy table.
fn legacy_saga_carry_over_sql(rel: &str) -> String {
    const COLS: &str = "saga_id, tx_id, tenant_id, correlation_id, status, backend_instance, \
                        operation, current_step, retry_count, recovery_attempts, \
                        compensation_status, steps, compensations, last_error, created_at, \
                        updated_at";
    format!(
        "DO $udb_saga_carry$
         BEGIN
             IF to_regclass('{legacy}') IS NOT NULL
                AND to_regclass('{legacy}') IS DISTINCT FROM to_regclass('{rel_lit}') THEN
                 INSERT INTO {rel} ({COLS})
                 SELECT {COLS} FROM {legacy}
                 WHERE status NOT IN ('committed', 'compensated')
                 ON CONFLICT (saga_id) DO NOTHING;
             END IF;
         END $udb_saga_carry$",
        legacy = DEFAULT_REL.replace('\'', "''"),
        rel_lit = rel.replace('\'', "''"),
    )
}

/// The relation is shared with the BeginTx data plane, whose terminal-status
/// writer historically stamped `compensation_status = 'failed'` alongside
/// `status = 'failed_compensation'`. That token is not a `CompensationStatus`
/// variant; an unparseable row would fail the WHOLE list/claim query and blind
/// recovery to every saga. The status column already carries the failure, so
/// the legacy token reads as `None` (no successful compensation recorded).
/// An empty value (pre-column rows) reads the same way.
fn parse_pg_compensation_status(token: &str) -> Option<CompensationStatus> {
    match token {
        "" | "failed" => Some(CompensationStatus::None),
        other => CompensationStatus::parse(other),
    }
}

fn row_to_saga(row: sqlx::postgres::PgRow) -> SystemStoreResult<SagaRow> {
    let saga_id: Uuid = row
        .try_get("saga_id")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT saga_id", e))?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT status", e))?;
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in PG row"))
    })?;
    let comp_status_str: String = row.try_get("compensation_status").unwrap_or_default();
    let compensation_status = parse_pg_compensation_status(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in PG row"
        ))
    })?;

    Ok(SagaRow {
        saga_id,
        tx_id: row.try_get("tx_id").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        correlation_id: row.try_get("correlation_id").unwrap_or_default(),
        status,
        backend_instance: row.try_get("backend_instance").unwrap_or_default(),
        operation: row.try_get("operation").unwrap_or_default(),
        current_step: row.try_get("current_step").unwrap_or(0),
        retry_count: row.try_get("retry_count").unwrap_or(0),
        recovery_attempts: row.try_get("recovery_attempts").unwrap_or(0),
        compensation_status,
        steps: row
            .try_get("steps")
            .unwrap_or(serde_json::Value::Array(vec![])),
        compensations: row
            .try_get("compensations")
            .unwrap_or(serde_json::Value::Array(vec![])),
        last_error: row.try_get("last_error").unwrap_or_default(),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .unwrap_or_else(|_| Utc::now()),
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .unwrap_or_else(|_| Utc::now()),
    })
}

#[async_trait]
impl SagaStore for PostgresCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "postgres"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        let rel = self.saga_relation_ref();
        // B.7: DDL strings come from the shared `sql_schema` renderer (single
        // source of truth across SQL backends); the execute/error-handling
        // loop below is unchanged.
        let mut stmts = super::sql_schema::postgres_sagas_ddl(rel);
        // The production store shares its relation with the BeginTx data
        // plane, which stamps the owning node on every in-progress saga so the
        // startup crash sweep only touches this node's sagas. Additive +
        // idempotent, so a pre-existing table (either shape) is upgraded.
        stmts.push(crate::runtime::saga::saga_owner_column_ddl(rel));
        // Only the production wiring (store pointed at the data-plane saga
        // relation) inherits the legacy store's open sagas; scratch relations
        // (conformance tests, isolated schemas) never do.
        if rel != DEFAULT_REL && rel == crate::runtime::saga::data_plane_saga_relation() {
            stmts.push(legacy_saga_carry_over_sql(rel));
        }
        for sql in stmts.iter() {
            sqlx::query(sql)
                .execute(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        }
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let rel = self.saga_relation_ref();
        let saga_id = Uuid::new_v4();
        let sql = format!(
            r#"
            INSERT INTO {rel} (
                saga_id, tx_id, tenant_id, correlation_id, status,
                backend_instance, operation, steps, compensations
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb
            )
            "#
        );
        sqlx::query(&sql)
            .bind(saga_id)
            .bind(&saga.tx_id)
            .bind(&saga.tenant_id)
            .bind(&saga.correlation_id)
            .bind(saga.status.as_str())
            .bind(&saga.backend_instance)
            .bind(&saga.operation)
            .bind(saga.steps.to_string())
            .bind(saga.compensations.to_string())
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                      backend_instance, operation, current_step, retry_count,
                      recovery_attempts, compensation_status, steps, compensations,
                      last_error, created_at, updated_at
               FROM {rel}
               WHERE saga_id = $1"#
        );
        let row = sqlx::query(&sql)
            .bind(saga_id)
            .fetch_optional(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        match row {
            Some(r) => Ok(Some(row_to_saga(r)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let rel = self.saga_relation_ref();
        // Build the WHERE clause with placeholder numbers that match
        // the bind order. We bind values in the same order they're
        // pushed below. tx_id: PG schema declares tx_id as TEXT, not
        // UUID, so we compare as string. Caller may pass any opaque tx
        // token.
        let w = build_eq_where(
            SqlDialect::POSTGRES,
            &[
                ("tenant_id", filter.tenant_id.is_some()),
                ("status", filter.status.is_some()),
                ("tx_id", filter.tx_id.is_some()),
                ("correlation_id", filter.correlation_id.is_some()),
            ],
        );
        let where_sql = &w.where_sql;
        let limit_placeholder = &w.limit_placeholder;
        let offset_placeholder = &w.offset_placeholder;
        let (limit, offset) = normalize_limit_offset(filter.limit, filter.offset);
        let sql = format!(
            r#"SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                      backend_instance, operation, current_step, retry_count,
                      recovery_attempts, compensation_status, steps, compensations,
                      last_error, created_at, updated_at
               FROM {rel}
               {where_sql}
               ORDER BY updated_at DESC
               LIMIT {limit_placeholder} OFFSET {offset_placeholder}"#
        );
        let mut q = sqlx::query(&sql);
        if let Some(t) = &filter.tenant_id {
            q = q.bind(t.clone());
        }
        if let Some(s) = filter.status {
            q = q.bind(s.as_str());
        }
        if let Some(t) = &filter.tx_id {
            q = q.bind(t.clone());
        }
        if let Some(c) = &filter.correlation_id {
            q = q.bind(c.clone());
        }
        q = q.bind(limit).bind(offset);
        let rows = q
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_saga(r)?);
        }
        Ok(out)
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = $1, compensation_status = $2, updated_at = NOW()
               WHERE saga_id = $3"#
        );
        let result = sqlx::query(&sql)
            .bind(status.as_str())
            .bind(compensation_status.as_str())
            .bind(saga_id)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for update_saga_status"
            )));
        }
        Ok(())
    }

    async fn update_saga_statuses_batch(
        &self,
        saga_ids: &[Uuid],
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        if saga_ids.is_empty() {
            return Ok(());
        }
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = $1, compensation_status = $2, updated_at = NOW()
               WHERE saga_id = ANY($3)"#
        );
        sqlx::query(&sql)
            .bind(status.as_str())
            .bind(compensation_status.as_str())
            .bind(saga_ids)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(())
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'manual_review', updated_at = NOW()
               WHERE saga_id = $1"#
        );
        let result = sqlx::query(&sql)
            .bind(saga_id)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'indeterminate',
                   last_error = '',
                   retry_count = retry_count + 1,
                   compensation_status = 'retry_requested',
                   updated_at = NOW()
               WHERE saga_id = $1
                 AND status IN ('failed_compensation', 'manual_review')"#
        );
        let result = sqlx::query(&sql)
            .bind(saga_id)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        }
        Ok(())
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET recovery_attempts = recovery_attempts + 1,
                   last_error = $1,
                   updated_at = NOW()
               WHERE saga_id = $2
               RETURNING recovery_attempts::BIGINT"#
        );
        let n: Option<i64> = sqlx::query_scalar(&sql)
            .bind(error)
            .bind(saga_id)
            .fetch_optional(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        n.ok_or_else(|| {
            SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for increment_recovery_attempts"
            ))
        })
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let rel = self.saga_relation_ref();
        // PG's EXTRACT(EPOCH FROM (NOW() - updated_at)) gives seconds
        // delta; cross-compare with the seconds argument.
        let sql = format!(
            r#"SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                      backend_instance, operation, current_step, retry_count,
                      recovery_attempts, compensation_status, steps, compensations,
                      last_error, created_at, updated_at
               FROM {rel}
               WHERE status IN ('indeterminate', 'in_doubt')
                  OR (status = 'in_progress'
                      AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > $1::double precision)
               ORDER BY updated_at ASC
               LIMIT $2"#
        );
        let rows = sqlx::query(&sql)
            .bind(stale_after.as_secs_f64())
            .bind(limit.max(1))
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(row_to_saga(r)?);
        }
        Ok(out)
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let rel = self.saga_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'indeterminate',
                   last_error = 'stale in-progress reconciled at startup',
                   updated_at = NOW()
               WHERE status = 'in_progress'
                 AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > $1::double precision"#
        );
        let result = sqlx::query(&sql)
            .bind(stale_after.as_secs_f64())
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let rel = self.saga_relation_ref();
        let sql = format!(r#"SELECT status, COUNT(*)::BIGINT AS n FROM {rel} GROUP BY status"#);
        let rows = sqlx::query(&sql)
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut s = SagaSummary::default();
        for row in rows {
            let status: String = row.try_get("status").unwrap_or_default();
            let n: i64 = row.try_get("n").unwrap_or(0);
            apply_saga_summary_bucket(&mut s, "postgres", &status, n)?;
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::saga::{
        data_plane_saga_begin_sql, data_plane_saga_relation, saga_owner_column_ddl,
        startup_mark_indeterminate_sql,
    };

    #[test]
    fn legacy_data_plane_compensation_tokens_do_not_poison_reads() {
        assert_eq!(
            parse_pg_compensation_status("failed"),
            Some(CompensationStatus::None)
        );
        assert_eq!(
            parse_pg_compensation_status(""),
            Some(CompensationStatus::None)
        );
        assert_eq!(
            parse_pg_compensation_status("completed"),
            Some(CompensationStatus::Completed)
        );
        assert_eq!(parse_pg_compensation_status("bogus"), None);
    }

    #[test]
    fn data_plane_and_store_name_the_same_relation() {
        // The production store is built with this relation; the data-plane
        // SQL builders interpolate it verbatim.
        let rel = data_plane_saga_relation();
        assert!(data_plane_saga_begin_sql(&rel).contains(&format!("INSERT INTO {rel}")));
        assert!(startup_mark_indeterminate_sql(&rel).contains(&format!("UPDATE {rel}")));
        assert!(saga_owner_column_ddl(&rel).starts_with(&format!("ALTER TABLE {rel}")));
    }

    #[test]
    fn legacy_carry_over_copies_only_actionable_sagas_idempotently() {
        let sql = legacy_saga_carry_over_sql(r#""udb_system"."udb_saga_coordinator""#);
        assert!(sql.contains(r#"INSERT INTO "udb_system"."udb_saga_coordinator""#));
        assert!(sql.contains(&format!("FROM {DEFAULT_REL}")));
        assert!(sql.contains("ON CONFLICT (saga_id) DO NOTHING"));
        assert!(sql.contains("status NOT IN ('committed', 'compensated')"));
        assert!(
            sql.contains("to_regclass"),
            "no-op when the legacy table is absent"
        );
    }

    fn live_pg_dsn() -> Option<String> {
        std::env::var("UDB_LIVE_SAGA_PG_DSN")
            .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
            .ok()
    }

    async fn live_store() -> Option<(sqlx::PgPool, PostgresCanonicalStore)> {
        let dsn = live_pg_dsn()?;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&dsn)
            .await
            .unwrap_or_else(|err| panic!("connect live saga postgres at {dsn}: {err}"));
        crate::runtime::system::ensure_system_catalog(&pool)
            .await
            .expect("ensure live UDB system catalog");
        let store = PostgresCanonicalStore::new(pool.clone(), "primary", "")
            .with_saga_relation(data_plane_saga_relation());
        SagaStore::ensure_saga_tables(&store)
            .await
            .expect("ensure saga tables on the shared relation");
        Some((pool, store))
    }

    async fn data_plane_begin(pool: &sqlx::PgPool, tx_id: &str, owner: &str) -> Uuid {
        let saga_id = Uuid::new_v4();
        sqlx::query(&data_plane_saga_begin_sql(&data_plane_saga_relation()))
            .bind(saga_id.to_string())
            .bind(tx_id)
            .bind("tenant-live-saga")
            .bind("corr-live-saga")
            .bind("primary")
            .bind("upsert")
            .bind(owner)
            .execute(pool)
            .await
            .expect("data-plane saga_begin insert");
        saga_id
    }

    /// A saga written through the BeginTx data-plane SQL is visible to the
    /// recovery store (list + claim), i.e. both sides share one relation.
    #[tokio::test]
    #[ignore = "requires Postgres; set UDB_INTEGRATION_PG_DSN (or UDB_LIVE_SAGA_PG_DSN) and run with --ignored"]
    async fn data_plane_saga_is_listed_by_recovery_store() {
        let Some((pool, store)) = live_store().await else {
            eprintln!("skipped: set UDB_INTEGRATION_PG_DSN or UDB_LIVE_SAGA_PG_DSN");
            return;
        };
        let tx_id = format!("live-saga-{}", Uuid::new_v4());
        let saga_id = data_plane_begin(&pool, &tx_id, "live-node-a").await;

        let listed = SagaStore::list_sagas(
            &store,
            &SagaListFilter {
                tx_id: Some(tx_id.clone()),
                ..SagaListFilter::default()
            },
        )
        .await
        .expect("recovery store lists data-plane sagas");
        assert_eq!(
            listed.len(),
            1,
            "data-plane saga must be visible to recovery"
        );
        assert_eq!(listed[0].saga_id, saga_id);
        assert_eq!(listed[0].status, SagaStatus::InProgress);

        // Once the owner's startup sweep flags it, the recovery claim sees it.
        let sweep = startup_mark_indeterminate_sql(&data_plane_saga_relation());
        sqlx::query(&sweep)
            .bind("live-node-a")
            .bind(86_400.0_f64)
            .execute(&pool)
            .await
            .expect("owner startup sweep");
        let row = SagaStore::get_saga(&store, saga_id)
            .await
            .expect("get saga")
            .expect("saga present");
        assert_eq!(row.status, SagaStatus::Indeterminate);
    }

    /// The startup sweep never flips a peer node's fresh in-flight saga.
    #[tokio::test]
    #[ignore = "requires Postgres; set UDB_INTEGRATION_PG_DSN (or UDB_LIVE_SAGA_PG_DSN) and run with --ignored"]
    async fn startup_sweep_leaves_peer_node_in_flight_sagas_alone() {
        let Some((pool, store)) = live_store().await else {
            eprintln!("skipped: set UDB_INTEGRATION_PG_DSN or UDB_LIVE_SAGA_PG_DSN");
            return;
        };
        let peer =
            data_plane_begin(&pool, &format!("live-peer-{}", Uuid::new_v4()), "peer-node").await;
        let sweep = startup_mark_indeterminate_sql(&data_plane_saga_relation());
        sqlx::query(&sweep)
            .bind("restarting-node")
            .bind(86_400.0_f64)
            .execute(&pool)
            .await
            .expect("startup sweep");
        let row = SagaStore::get_saga(&store, peer)
            .await
            .expect("get saga")
            .expect("saga present");
        assert_eq!(
            row.status,
            SagaStatus::InProgress,
            "a peer's fresh in-flight saga must not be marked indeterminate"
        );
    }
}
