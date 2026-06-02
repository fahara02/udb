//! B.8 phase 2 — SQL Server (Tiberius) implementation of [`SagaStore`].
//!
//! Mirrors the Postgres impl's semantics in T-SQL. uuids and timestamps
//! are cast to text in every SELECT (see `mssql_projection` for why), and
//! list paging uses `OFFSET @Pn ROWS FETCH NEXT @Pm ROWS ONLY` instead of
//! PG's `LIMIT/OFFSET`.

use std::time::Duration;

use async_trait::async_trait;
use tiberius::Row;
use uuid::Uuid;

use super::dialect::{apply_saga_summary_bucket, normalize_limit_offset};
use super::mssql::MssqlCanonicalStore;
use super::mssql_projection::{i32_col, opt_str_col, parse_ts_cell, parse_uuid_cell, str_col};
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};
use crate::runtime::executors::mssql::SqlParam;

const DEFAULT_REL: &str = "udb_sagas";

impl MssqlCanonicalStore {
    /// Override the sagas relation. Defaults to `udb_sagas`.
    pub fn with_saga_relation(mut self, relation: impl Into<String>) -> Self {
        self.saga_relation = Some(relation.into());
        self
    }

    pub(super) fn saga_relation_ref(&self) -> &str {
        self.saga_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

/// JSON column decode that defaults to `[]` (sagas store arrays), matching
/// the PG impl's `unwrap_or(Value::Array(vec![]))`.
fn json_array_col(row: &Row, name: &str) -> serde_json::Value {
    match opt_str_col(row, name) {
        Some(text) => {
            serde_json::from_str(&text).unwrap_or_else(|_| serde_json::Value::Array(vec![]))
        }
        None => serde_json::Value::Array(vec![]),
    }
}

/// Column projection used by get / list / claim. uuids + timestamps cast to
/// text so tiberius decodes them as `&str`.
const SAGA_SELECT_COLS: &str = "\
    CONVERT(NVARCHAR(36), saga_id) AS saga_id, tx_id, tenant_id, correlation_id, status, \
    backend_instance, operation, current_step, retry_count, recovery_attempts, \
    compensation_status, steps, compensations, last_error, \
    CONVERT(NVARCHAR(33), created_at, 127) AS created_at, \
    CONVERT(NVARCHAR(33), updated_at, 127) AS updated_at";

fn row_to_saga(row: &Row) -> SystemStoreResult<SagaRow> {
    let saga_id_str = opt_str_col(row, "saga_id")
        .ok_or_else(|| SystemStoreError::query("mssql", "SELECT saga_id", "saga_id was NULL"))?;
    let saga_id = parse_uuid_cell(&saga_id_str)?;
    let status_str = str_col(row, "status");
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in mssql row"))
    })?;
    let comp_str = str_col(row, "compensation_status");
    let compensation_status = CompensationStatus::parse(&comp_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_str}' in mssql row"
        ))
    })?;
    Ok(SagaRow {
        saga_id,
        tx_id: str_col(row, "tx_id"),
        tenant_id: str_col(row, "tenant_id"),
        correlation_id: str_col(row, "correlation_id"),
        status,
        backend_instance: str_col(row, "backend_instance"),
        operation: str_col(row, "operation"),
        current_step: i32_col(row, "current_step"),
        retry_count: i32_col(row, "retry_count"),
        recovery_attempts: i32_col(row, "recovery_attempts"),
        compensation_status,
        steps: json_array_col(row, "steps"),
        compensations: json_array_col(row, "compensations"),
        last_error: str_col(row, "last_error"),
        created_at: parse_ts_cell(opt_str_col(row, "created_at").as_deref()),
        updated_at: parse_ts_cell(opt_str_col(row, "updated_at").as_deref()),
    })
}

#[async_trait]
impl SagaStore for MssqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mssql"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = super::sql_schema::mssql_sagas_ddl(rel);
        self.client()
            .simple_batch(&sql)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let saga_id = Uuid::new_v4();
        let sql = format!(
            "INSERT INTO {rel} ( \
                saga_id, tx_id, tenant_id, correlation_id, status, \
                backend_instance, operation, steps, compensations \
             ) VALUES ( \
                CONVERT(UNIQUEIDENTIFIER, @P1), @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9 \
             )"
        );
        let params = [
            SqlParam::Str(saga_id.to_string()),
            SqlParam::Str(saga.tx_id.clone()),
            SqlParam::Str(saga.tenant_id.clone()),
            SqlParam::Str(saga.correlation_id.clone()),
            SqlParam::Str(saga.status.as_str().to_string()),
            SqlParam::Str(saga.backend_instance.clone()),
            SqlParam::Str(saga.operation.clone()),
            SqlParam::Str(saga.steps.to_string()),
            SqlParam::Str(saga.compensations.to_string()),
        ];
        self.client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "SELECT {SAGA_SELECT_COLS} FROM {rel} WHERE saga_id = CONVERT(UNIQUEIDENTIFIER, @P1)"
        );
        let params = [SqlParam::Str(saga_id.to_string())];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        match rows.first() {
            Some(r) => Ok(Some(row_to_saga(r)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // Build the WHERE inline with sequential @Pn placeholders, binding in
        // the same order. tx_id is compared as text (schema stores it as
        // NVARCHAR), matching the PG impl.
        let mut clauses: Vec<String> = Vec::new();
        let mut params: Vec<SqlParam> = Vec::new();
        let mut n = 1usize;
        if let Some(t) = &filter.tenant_id {
            clauses.push(format!("tenant_id = @P{n}"));
            params.push(SqlParam::Str(t.clone()));
            n += 1;
        }
        if let Some(s) = filter.status {
            clauses.push(format!("status = @P{n}"));
            params.push(SqlParam::Str(s.as_str().to_string()));
            n += 1;
        }
        if let Some(t) = &filter.tx_id {
            clauses.push(format!("tx_id = @P{n}"));
            params.push(SqlParam::Str(t.clone()));
            n += 1;
        }
        if let Some(c) = &filter.correlation_id {
            clauses.push(format!("correlation_id = @P{n}"));
            params.push(SqlParam::Str(c.clone()));
            n += 1;
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let (limit, offset) = normalize_limit_offset(filter.limit, filter.offset);
        // OFFSET/FETCH is T-SQL's paging (requires an ORDER BY). offset is
        // @Pn, limit is @P{n+1}.
        let offset_ph = format!("@P{n}");
        let limit_ph = format!("@P{}", n + 1);
        params.push(SqlParam::Int(offset));
        params.push(SqlParam::Int(limit));
        let sql = format!(
            "SELECT {SAGA_SELECT_COLS} FROM {rel} \
             {where_sql} \
             ORDER BY updated_at DESC \
             OFFSET {offset_ph} ROWS FETCH NEXT {limit_ph} ROWS ONLY"
        );
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
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
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = @P1, compensation_status = @P2, updated_at = SYSUTCDATETIME() \
             WHERE saga_id = CONVERT(UNIQUEIDENTIFIER, @P3)"
        );
        let params = [
            SqlParam::Str(status.as_str().to_string()),
            SqlParam::Str(compensation_status.as_str().to_string()),
            SqlParam::Str(saga_id.to_string()),
        ];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        if n == 0 {
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
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // SQL Server has no `= ANY(array)`. Bind the ids as a comma-joined
        // CSV of UUID strings and match via STRING_SPLIT — one round-trip,
        // injection-safe (the CSV is a single bound parameter).
        let csv = saga_ids
            .iter()
            .map(Uuid::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE {rel} \
             SET status = @P1, compensation_status = @P2, updated_at = SYSUTCDATETIME() \
             WHERE CONVERT(NVARCHAR(36), saga_id) IN (SELECT value FROM STRING_SPLIT(@P3, ','))"
        );
        let params = [
            SqlParam::Str(status.as_str().to_string()),
            SqlParam::Str(compensation_status.as_str().to_string()),
            SqlParam::Str(csv),
        ];
        self.client()
            .execute_sql(&sql, &params)
            .await
            .map(|_| ())
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'manual_review', updated_at = SYSUTCDATETIME() \
             WHERE saga_id = CONVERT(UNIQUEIDENTIFIER, @P1)"
        );
        let params = [SqlParam::Str(saga_id.to_string())];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        if n == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'indeterminate', \
                 last_error = '', \
                 retry_count = retry_count + 1, \
                 compensation_status = 'retry_requested', \
                 updated_at = SYSUTCDATETIME() \
             WHERE saga_id = CONVERT(UNIQUEIDENTIFIER, @P1) \
               AND status IN ('failed_compensation', 'manual_review')"
        );
        let params = [SqlParam::Str(saga_id.to_string())];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        if n == 0 {
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
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // UPDATE … OUTPUT inserted.recovery_attempts returns the
        // post-increment counter in one round-trip (T-SQL's RETURNING).
        let sql = format!(
            "UPDATE {rel} \
             SET recovery_attempts = recovery_attempts + 1, \
                 last_error = @P1, \
                 updated_at = SYSUTCDATETIME() \
             OUTPUT inserted.recovery_attempts AS recovery_attempts \
             WHERE saga_id = CONVERT(UNIQUEIDENTIFIER, @P2)"
        );
        let params = [
            SqlParam::Str(error.to_string()),
            SqlParam::Str(saga_id.to_string()),
        ];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        match rows.first() {
            Some(r) => Ok(i64::from(i32_col(r, "recovery_attempts"))),
            None => Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for increment_recovery_attempts"
            ))),
        }
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // indeterminate/in_doubt always; in_progress only when stale
        // (DATEDIFF seconds > @stale), matching the PG predicate.
        let sql = format!(
            "SELECT TOP (@P2) {SAGA_SELECT_COLS} FROM {rel} \
             WHERE status IN ('indeterminate', 'in_doubt') \
                OR (status = 'in_progress' \
                    AND DATEDIFF(SECOND, updated_at, SYSUTCDATETIME()) > @P1) \
             ORDER BY updated_at ASC"
        );
        let params = [
            SqlParam::Int(stale_after.as_secs() as i64),
            SqlParam::Int(limit.max(1)),
        ];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            out.push(row_to_saga(r)?);
        }
        Ok(out)
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'indeterminate', \
                 last_error = 'stale in-progress reconciled at startup', \
                 updated_at = SYSUTCDATETIME() \
             WHERE status = 'in_progress' \
               AND DATEDIFF(SECOND, updated_at, SYSUTCDATETIME()) > @P1"
        );
        let params = [SqlParam::Int(stale_after.as_secs() as i64)];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(n as i64)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let rel = Self::safe_object_name(self.saga_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!("SELECT status, COUNT(*) AS n FROM {rel} GROUP BY status");
        let rows = self
            .client()
            .fetch_rows(&sql, &[])
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut s = SagaSummary::default();
        for row in &rows {
            let status = str_col(row, "status");
            let n = i64::from(i32_col(row, "n"));
            apply_saga_summary_bucket(&mut s, "mssql", &status, n)?;
        }
        Ok(s)
    }
}
