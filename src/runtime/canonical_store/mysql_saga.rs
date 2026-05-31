//! MySQL implementation of [`SagaStore`].
//!
//! Same dialect choices as `mysql_projection.rs`: CHAR(36) UUID,
//! native `JSON` column type, `TIMESTAMP(6)` for microsecond
//! precision, CHECK constraints (8.0.16+) for the status enums,
//! `TIMESTAMPDIFF` for staleness. MySQL has no `UPDATE … RETURNING`,
//! so `increment_recovery_attempts` does UPDATE + SELECT in one
//! transaction.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::mysql::MysqlCanonicalStore;
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

const TABLE: &str = "udb_sagas";

fn row_to_saga(row: sqlx::mysql::MySqlRow) -> SystemStoreResult<SagaRow> {
    let saga_id_str: String = row
        .try_get("saga_id")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT saga_id", e))?;
    let saga_id = Uuid::parse_str(&saga_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("saga_id '{saga_id_str}' is not a valid UUID: {e}"))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT status", e))?;
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in MySQL row"))
    })?;
    let comp_status_str: String = row.try_get("compensation_status").unwrap_or_default();
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in MySQL row"
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
impl SagaStore for MysqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mysql"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                saga_id              CHAR(36) NOT NULL PRIMARY KEY,
                tx_id                VARCHAR(255) NOT NULL DEFAULT '',
                tenant_id            VARCHAR(255) NOT NULL DEFAULT '',
                correlation_id       VARCHAR(255) NOT NULL DEFAULT '',
                status               VARCHAR(32) NOT NULL DEFAULT 'pending',
                backend_instance     VARCHAR(255) NOT NULL DEFAULT '',
                operation            VARCHAR(255) NOT NULL DEFAULT '',
                current_step         INT NOT NULL DEFAULT 0,
                retry_count          INT NOT NULL DEFAULT 0,
                recovery_attempts    INT NOT NULL DEFAULT 0,
                compensation_status  VARCHAR(32) NOT NULL DEFAULT 'none',
                steps                JSON NOT NULL,
                compensations        JSON NOT NULL,
                last_error           TEXT NOT NULL,
                created_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at           TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                CONSTRAINT chk_{TABLE}_status CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','failed_compensation','manual_review')),
                CONSTRAINT chk_{TABLE}_comp_status CHECK (compensation_status IN ('none','completed','manual_review','retry_requested'))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#
        );
        let create_idx = format!(
            "CREATE INDEX idx_{TABLE}_tenant_status \
             ON {TABLE} (tenant_id, status, updated_at)"
        );
        sqlx::query(&create_table)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", create_table.clone(), e))?;
        // Index creation idempotent across MySQL versions: tolerate
        // "Duplicate key name".
        if let Err(e) = sqlx::query(&create_idx).execute(self.mysql_pool()).await {
            let msg = e.to_string();
            if !msg.contains("Duplicate key name") && !msg.contains("already exists") {
                return Err(SystemStoreError::query("mysql", create_idx.clone(), e));
            }
        }
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let saga_id = Uuid::new_v4();
        let steps = serde_json::to_string(&saga.steps)
            .map_err(|e| SystemStoreError::InvalidInput(format!("steps: {e}")))?;
        let compensations = serde_json::to_string(&saga.compensations)
            .map_err(|e| SystemStoreError::InvalidInput(format!("compensations: {e}")))?;
        let sql = format!(
            "INSERT INTO {TABLE} (
                saga_id, tx_id, tenant_id, correlation_id, status,
                backend_instance, operation, steps, compensations, last_error
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ''
            )"
        );
        sqlx::query(&sql)
            .bind(saga_id.to_string())
            .bind(&saga.tx_id)
            .bind(&saga.tenant_id)
            .bind(&saga.correlation_id)
            .bind(saga.status.as_str())
            .bind(&saga.backend_instance)
            .bind(&saga.operation)
            .bind(&steps)
            .bind(&compensations)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let sql = format!(
            "SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                    backend_instance, operation, current_step, retry_count,
                    recovery_attempts, compensation_status, steps, compensations,
                    last_error, created_at, updated_at
             FROM {TABLE}
             WHERE saga_id = ?"
        );
        let row = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .fetch_optional(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        match row {
            Some(r) => Ok(Some(row_to_saga(r)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        // MySQL uses `?` placeholders — order matters but the numbers
        // don't need to be embedded.
        let mut clauses: Vec<&str> = Vec::new();
        if filter.tenant_id.is_some() {
            clauses.push("tenant_id = ?");
        }
        if filter.status.is_some() {
            clauses.push("status = ?");
        }
        if filter.tx_id.is_some() {
            clauses.push("tx_id = ?");
        }
        if filter.correlation_id.is_some() {
            clauses.push("correlation_id = ?");
        }
        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let limit = if filter.limit <= 0 { 100 } else { filter.limit };
        let offset = filter.offset.max(0);
        let sql = format!(
            "SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                    backend_instance, operation, current_step, retry_count,
                    recovery_attempts, compensation_status, steps, compensations,
                    last_error, created_at, updated_at
             FROM {TABLE}
             {where_sql}
             ORDER BY updated_at DESC
             LIMIT ? OFFSET ?"
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
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
        let sql = format!(
            "UPDATE {TABLE}
             SET status = ?, compensation_status = ?, updated_at = NOW(6)
             WHERE saga_id = ?"
        );
        let result = sqlx::query(&sql)
            .bind(status.as_str())
            .bind(compensation_status.as_str())
            .bind(saga_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for update_saga_status"
            )));
        }
        Ok(())
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'manual_review', updated_at = NOW(6)
             WHERE saga_id = ?"
        );
        let result = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'indeterminate',
                 last_error = '',
                 retry_count = retry_count + 1,
                 compensation_status = 'retry_requested',
                 updated_at = NOW(6)
             WHERE saga_id = ?
               AND status IN ('failed_compensation', 'manual_review')"
        );
        let result = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
        // MySQL doesn't support UPDATE … RETURNING. Run UPDATE +
        // SELECT inside a single transaction so we read the
        // post-increment value atomically.
        let mut tx = self
            .mysql_pool()
            .begin()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;
        let update_sql = format!(
            "UPDATE {TABLE}
             SET recovery_attempts = recovery_attempts + 1,
                 last_error = ?,
                 updated_at = NOW(6)
             WHERE saga_id = ?"
        );
        let result = sqlx::query(&update_sql)
            .bind(error)
            .bind(saga_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", update_sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for increment_recovery_attempts"
            )));
        }
        let select_sql = format!("SELECT recovery_attempts FROM {TABLE} WHERE saga_id = ?");
        let n: i64 = sqlx::query_scalar::<_, i32>(&select_sql)
            .bind(saga_id.to_string())
            .fetch_one(&mut *tx)
            .await
            .map(|n| n as i64)
            .map_err(|e| SystemStoreError::query("mysql", select_sql.clone(), e))?;
        tx.commit()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;
        Ok(n)
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let sql = format!(
            "SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                    backend_instance, operation, current_step, retry_count,
                    recovery_attempts, compensation_status, steps, compensations,
                    last_error, created_at, updated_at
             FROM {TABLE}
             WHERE status = 'indeterminate'
                OR (status = 'in_progress'
                    AND TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) > ?)
             ORDER BY updated_at ASC
             LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(stale_after.as_secs() as i64)
            .bind(limit.max(1))
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'indeterminate',
                 last_error = 'stale in-progress reconciled at startup',
                 updated_at = NOW(6)
             WHERE status = 'in_progress'
               AND TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) > ?"
        );
        let result = sqlx::query(&sql)
            .bind(stale_after.as_secs() as i64)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let sql = format!("SELECT status, COUNT(*) AS n FROM {TABLE} GROUP BY status");
        let rows: Vec<(String, i64)> = sqlx::query_as(&sql)
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        let mut s = SagaSummary::default();
        for (status, n) in rows {
            match SagaStatus::parse(&status) {
                Some(SagaStatus::Indeterminate) => s.indeterminate = n,
                Some(SagaStatus::InProgress) => s.in_progress = n,
                Some(SagaStatus::Pending) => s.pending = n,
                Some(SagaStatus::Committed) => s.committed = n,
                Some(SagaStatus::Compensated) => s.compensated = n,
                Some(SagaStatus::Failed) => s.failed = n,
                Some(SagaStatus::FailedCompensation) => s.failed_compensation = n,
                Some(SagaStatus::ManualReview) => s.manual_review = n,
                None => {
                    return Err(SystemStoreError::SchemaMismatch {
                        backend: "mysql",
                        detail: format!(
                            "unexpected saga status '{status}' (CHECK constraint should prevent this)"
                        ),
                    });
                }
            }
        }
        Ok(s)
    }
}
