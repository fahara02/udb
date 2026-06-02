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
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
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
        let stmts = super::sql_schema::postgres_sagas_ddl(rel);
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
