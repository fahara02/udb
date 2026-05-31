//! SQLite implementation of [`SagaStore`].
//!
//! Same dialect choices as `sqlite_projection.rs`: TEXT for JSON,
//! TEXT for UUID, `BEGIN IMMEDIATE` where atomic claim matters,
//! `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` for timestamps, CHECK
//! constraints for status enums.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::sqlite::SqliteCanonicalStore;
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

const TABLE: &str = "udb_sagas";

fn parse_iso(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn row_to_saga(row: sqlx::sqlite::SqliteRow) -> SystemStoreResult<SagaRow> {
    let saga_id_str: String = row
        .try_get("saga_id")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT saga_id", e))?;
    let saga_id = Uuid::parse_str(&saga_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("saga_id '{saga_id_str}' is not a valid UUID: {e}"))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT status", e))?;
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("unknown saga status '{status_str}' in SQLite row"))
    })?;
    let comp_status_str: String = row.try_get("compensation_status").unwrap_or_default();
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in SQLite row"
        ))
    })?;

    let parse_json = |s: &str, field: &'static str| -> SystemStoreResult<serde_json::Value> {
        if s.is_empty() {
            return Ok(serde_json::Value::Array(Vec::new()));
        }
        serde_json::from_str(s).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "field '{field}' is not valid JSON: {e} (raw: '{s}')"
            ))
        })
    };
    let steps_text: String = row.try_get("steps").unwrap_or_default();
    let compensations_text: String = row.try_get("compensations").unwrap_or_default();

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
        steps: parse_json(&steps_text, "steps")?,
        compensations: parse_json(&compensations_text, "compensations")?,
        last_error: row.try_get("last_error").unwrap_or_default(),
        created_at: row
            .try_get::<String, _>("created_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: row
            .try_get::<String, _>("updated_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
    })
}

#[async_trait]
impl SagaStore for SqliteCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "sqlite"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {TABLE} (
                saga_id              TEXT PRIMARY KEY,
                tx_id                TEXT NOT NULL DEFAULT '',
                tenant_id            TEXT NOT NULL DEFAULT '',
                correlation_id       TEXT NOT NULL DEFAULT '',
                status               TEXT NOT NULL DEFAULT 'pending'
                                     CHECK (status IN ('indeterminate','in_progress','pending','committed','compensated','failed','failed_compensation','manual_review')),
                backend_instance     TEXT NOT NULL DEFAULT '',
                operation            TEXT NOT NULL DEFAULT '',
                current_step         INTEGER NOT NULL DEFAULT 0,
                retry_count          INTEGER NOT NULL DEFAULT 0,
                recovery_attempts    INTEGER NOT NULL DEFAULT 0,
                compensation_status  TEXT NOT NULL DEFAULT 'none'
                                     CHECK (compensation_status IN ('none','completed','manual_review','retry_requested')),
                steps                TEXT NOT NULL DEFAULT '[]',
                compensations        TEXT NOT NULL DEFAULT '[]',
                last_error           TEXT NOT NULL DEFAULT '',
                created_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                updated_at           TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            )
            "#
        );
        let create_idx = format!(
            "CREATE INDEX IF NOT EXISTS idx_{TABLE}_tenant_status \
             ON {TABLE} (tenant_id, status, updated_at DESC)"
        );
        for sql in [create_table, create_idx] {
            sqlx::query(&sql)
                .execute(self.pool_ref())
                .await
                .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
                backend_instance, operation, steps, compensations
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
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
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let sql = format!(
            "SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                    backend_instance, operation, current_step, retry_count,
                    recovery_attempts, compensation_status, steps, compensations,
                    last_error, created_at, updated_at
             FROM {TABLE}
             WHERE saga_id = ?1"
        );
        let row = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .fetch_optional(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        match row {
            Some(r) => Ok(Some(row_to_saga(r)?)),
            None => Ok(None),
        }
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        // Build the WHERE clause dynamically — only include columns
        // the caller filtered on. All values are bound; no string
        // interpolation of caller input.
        let mut clauses: Vec<&str> = Vec::with_capacity(4);
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
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
             SET status = ?1,
                 compensation_status = ?2,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE saga_id = ?3"
        );
        let result = sqlx::query(&sql)
            .bind(status.as_str())
            .bind(compensation_status.as_str())
            .bind(saga_id.to_string())
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
             SET status = 'manual_review',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE saga_id = ?1"
        );
        let result = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        if result.rows_affected() == 0 {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        // Refuses unless the saga is currently failed_compensation or
        // manual_review. Mirrors `runtime/saga.rs::retry_saga_compensation`.
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'indeterminate',
                 last_error = '',
                 retry_count = retry_count + 1,
                 compensation_status = 'retry_requested',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE saga_id = ?1
               AND status IN ('failed_compensation', 'manual_review')"
        );
        let result = sqlx::query(&sql)
            .bind(saga_id.to_string())
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
        // SQLite's RETURNING (3.35+) gives us the post-increment value
        // in one round-trip.
        let sql = format!(
            "UPDATE {TABLE}
             SET recovery_attempts = recovery_attempts + 1,
                 last_error = ?1,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE saga_id = ?2
             RETURNING recovery_attempts"
        );
        let n: i64 = sqlx::query_scalar(&sql)
            .bind(error)
            .bind(saga_id.to_string())
            .fetch_optional(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?
            .ok_or_else(|| {
                SystemStoreError::InvalidInput(format!(
                    "saga {saga_id} not found for increment_recovery_attempts"
                ))
            })?;
        Ok(n)
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let cutoff = Utc::now() - chrono::Duration::seconds(stale_after.as_secs() as i64);
        let cutoff_str = cutoff.to_rfc3339();
        let sql = format!(
            "SELECT saga_id, tx_id, tenant_id, correlation_id, status,
                    backend_instance, operation, current_step, retry_count,
                    recovery_attempts, compensation_status, steps, compensations,
                    last_error, created_at, updated_at
             FROM {TABLE}
             WHERE status = 'indeterminate'
                OR (status = 'in_progress' AND updated_at < ?1)
             ORDER BY updated_at ASC
             LIMIT ?2"
        );
        let rows = sqlx::query(&sql)
            .bind(&cutoff_str)
            .bind(limit.max(1))
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
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
        let cutoff = Utc::now() - chrono::Duration::seconds(stale_after.as_secs() as i64);
        let cutoff_str = cutoff.to_rfc3339();
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'indeterminate',
                 last_error = 'stale in-progress reconciled at startup',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'in_progress' AND updated_at < ?1"
        );
        let result = sqlx::query(&sql)
            .bind(&cutoff_str)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let sql = format!("SELECT status, COUNT(*) AS n FROM {TABLE} GROUP BY status");
        let rows = sqlx::query(&sql)
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        let mut s = SagaSummary::default();
        for row in rows {
            let status: String = row.try_get("status").unwrap_or_default();
            let n: i64 = row.try_get("n").unwrap_or(0);
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
                        backend: "sqlite",
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn fresh_store() -> SqliteCanonicalStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        SagaStore::ensure_saga_tables(&store).await.expect("DDL");
        store
    }

    fn sample_saga(tenant: &str, status: SagaStatus) -> SagaInsert {
        SagaInsert {
            tx_id: Uuid::new_v4().to_string(),
            tenant_id: tenant.to_string(),
            correlation_id: "corr-1".to_string(),
            backend_instance: "primary".to_string(),
            operation: "upsert.User".to_string(),
            status,
            steps: serde_json::json!([{"backend": "postgres", "op": "INSERT"}]),
            compensations: serde_json::json!([
                {"backend": "qdrant", "operation": "delete_points", "resource_uri": "qdrant://users", "payload": {"point_ids": ["1"]}}
            ]),
        }
    }

    /// Pin: record + get round-trips with full JSON payloads. The
    /// recovery worker reads `compensations` to dispatch — we must
    /// not lose the structure.
    #[tokio::test]
    async fn record_and_get_round_trip() {
        let store = fresh_store().await;
        let id = store
            .record_saga(&sample_saga("t1", SagaStatus::Indeterminate))
            .await
            .expect("record");
        let row = store.get_saga(id).await.expect("get").expect("Some(saga)");
        assert_eq!(row.saga_id, id);
        assert_eq!(row.tenant_id, "t1");
        assert_eq!(row.status, SagaStatus::Indeterminate);
        assert_eq!(row.compensation_status, CompensationStatus::None);
        assert_eq!(row.recovery_attempts, 0);
        assert!(
            row.compensations
                .as_array()
                .map(|a| !a.is_empty())
                .unwrap_or(false),
            "compensation JSON must round-trip"
        );
    }

    /// Pin: list with each filter axis returns the right subset.
    #[tokio::test]
    async fn list_sagas_honours_each_filter() {
        let store = fresh_store().await;
        let id_a = store
            .record_saga(&sample_saga("alpha", SagaStatus::Indeterminate))
            .await
            .unwrap();
        let id_b = store
            .record_saga(&sample_saga("beta", SagaStatus::Committed))
            .await
            .unwrap();
        let _id_c = store
            .record_saga(&sample_saga("alpha", SagaStatus::Failed))
            .await
            .unwrap();

        // Filter by tenant.
        let alpha = store
            .list_sagas(&SagaListFilter {
                tenant_id: Some("alpha".to_string()),
                limit: 100,
                ..SagaListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(alpha.len(), 2);
        assert!(alpha.iter().all(|r| r.tenant_id == "alpha"));

        // Filter by status.
        let only_indet = store
            .list_sagas(&SagaListFilter {
                status: Some(SagaStatus::Indeterminate),
                limit: 100,
                ..SagaListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_indet.len(), 1);
        assert_eq!(only_indet[0].saga_id, id_a);

        // Filter by tenant + status combined.
        let alpha_failed = store
            .list_sagas(&SagaListFilter {
                tenant_id: Some("alpha".to_string()),
                status: Some(SagaStatus::Failed),
                limit: 100,
                ..SagaListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(alpha_failed.len(), 1);

        // Limit + offset.
        let only_one = store
            .list_sagas(&SagaListFilter {
                limit: 1,
                offset: 0,
                ..SagaListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_one.len(), 1);

        // Beta tenant has one saga.
        let beta = store
            .list_sagas(&SagaListFilter {
                tenant_id: Some("beta".to_string()),
                limit: 100,
                ..SagaListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(beta.len(), 1);
        assert_eq!(beta[0].saga_id, id_b);
    }

    /// Pin: increment_recovery_attempts returns the post-increment
    /// value and bumps last_error.
    #[tokio::test]
    async fn increment_recovery_attempts_returns_new_value() {
        let store = fresh_store().await;
        let id = store
            .record_saga(&sample_saga("t", SagaStatus::Indeterminate))
            .await
            .unwrap();
        let n1 = store
            .increment_recovery_attempts(id, "first failure")
            .await
            .unwrap();
        assert_eq!(n1, 1);
        let n2 = store
            .increment_recovery_attempts(id, "second failure")
            .await
            .unwrap();
        assert_eq!(n2, 2);
        let row = store.get_saga(id).await.unwrap().unwrap();
        assert_eq!(row.recovery_attempts, 2);
        assert_eq!(row.last_error, "second failure");
    }

    /// Pin: increment_recovery_attempts on a missing saga returns
    /// InvalidInput, not a silent zero.
    #[tokio::test]
    async fn increment_recovery_attempts_on_missing_returns_error() {
        let store = fresh_store().await;
        let phantom = Uuid::new_v4();
        let err = store
            .increment_recovery_attempts(phantom, "nope")
            .await
            .expect_err("missing saga must error");
        match err {
            SystemStoreError::InvalidInput(msg) => assert!(msg.contains("not found")),
            other => panic!("expected InvalidInput, got: {other}"),
        }
    }

    /// Pin: claim_recoverable_sagas returns indeterminate + stale
    /// in_progress, sorted oldest-first.
    #[tokio::test]
    async fn claim_recoverable_returns_indeterminate_and_stale_in_progress() {
        let store = fresh_store().await;
        let id_indet = store
            .record_saga(&sample_saga("t", SagaStatus::Indeterminate))
            .await
            .unwrap();
        let _id_inprog = store
            .record_saga(&sample_saga("t", SagaStatus::InProgress))
            .await
            .unwrap();
        let _id_committed = store
            .record_saga(&sample_saga("t", SagaStatus::Committed))
            .await
            .unwrap();

        // Force the rows into the past so the staleness comparison is
        // unambiguous regardless of strftime precision quirks. Real
        // production callers pass stale_after >= recovery worker
        // interval (typically 60s), so this edge case is test-only.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // stale_after = 0s → every in_progress counts as stale.
        let claimable = store
            .claim_recoverable_sagas(Duration::from_secs(0), 10)
            .await
            .unwrap();
        // Indeterminate + stale in_progress; committed excluded.
        assert_eq!(claimable.len(), 2);
        let kinds: Vec<SagaStatus> = claimable.iter().map(|r| r.status).collect();
        assert!(kinds.contains(&SagaStatus::Indeterminate));
        assert!(kinds.contains(&SagaStatus::InProgress));
        assert!(!kinds.contains(&SagaStatus::Committed));

        // stale_after = 1 day → in_progress is NOT stale (it was just
        // inserted); only indeterminate is recoverable.
        let only_indet = store
            .claim_recoverable_sagas(Duration::from_secs(86_400), 10)
            .await
            .unwrap();
        assert_eq!(only_indet.len(), 1);
        assert_eq!(only_indet[0].saga_id, id_indet);
    }

    /// Pin: mark_stale_in_progress_indeterminate. Startup reconciliation
    /// flips stale rows to indeterminate.
    #[tokio::test]
    async fn mark_stale_in_progress_indeterminate_flips_rows() {
        let store = fresh_store().await;
        store
            .record_saga(&sample_saga("t", SagaStatus::InProgress))
            .await
            .unwrap();
        store
            .record_saga(&sample_saga("t", SagaStatus::Committed))
            .await
            .unwrap();
        // Force into the past (see claim_recoverable_returns_…).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let n = store
            .mark_stale_in_progress_indeterminate(Duration::from_secs(0))
            .await
            .unwrap();
        assert_eq!(n, 1);
        let summary = store.saga_summary().await.unwrap();
        assert_eq!(summary.in_progress, 0);
        assert_eq!(summary.indeterminate, 1);
        assert_eq!(summary.committed, 1);
    }

    /// Pin: update_saga_status changes both status + compensation_status
    /// atomically and updates updated_at.
    #[tokio::test]
    async fn update_saga_status_atomic_and_versioned() {
        let store = fresh_store().await;
        let id = store
            .record_saga(&sample_saga("t", SagaStatus::Indeterminate))
            .await
            .unwrap();
        store
            .update_saga_status(id, SagaStatus::Compensated, CompensationStatus::Completed)
            .await
            .unwrap();
        let row = store.get_saga(id).await.unwrap().unwrap();
        assert_eq!(row.status, SagaStatus::Compensated);
        assert_eq!(row.compensation_status, CompensationStatus::Completed);
    }

    /// Pin: update_saga_status on missing saga errors.
    #[tokio::test]
    async fn update_saga_status_on_missing_errors() {
        let store = fresh_store().await;
        let err = store
            .update_saga_status(
                Uuid::new_v4(),
                SagaStatus::Committed,
                CompensationStatus::None,
            )
            .await
            .expect_err("must error");
        match err {
            SystemStoreError::InvalidInput(msg) => assert!(msg.contains("not found")),
            other => panic!("expected InvalidInput, got: {other}"),
        }
    }

    /// Pin: mark_saga_manual_review + request_saga_recompensation
    /// state machine. Recompensation refuses from non-retryable states.
    #[tokio::test]
    async fn manual_review_and_request_recompensation_workflow() {
        let store = fresh_store().await;
        let id = store
            .record_saga(&sample_saga("t", SagaStatus::FailedCompensation))
            .await
            .unwrap();

        // Recompensation from failed_compensation: OK.
        store.request_saga_recompensation(id).await.unwrap();
        let row = store.get_saga(id).await.unwrap().unwrap();
        assert_eq!(row.status, SagaStatus::Indeterminate);
        assert_eq!(row.compensation_status, CompensationStatus::RetryRequested);
        assert_eq!(row.retry_count, 1);

        // Recompensation from indeterminate (after retry): NOT OK.
        let err = store
            .request_saga_recompensation(id)
            .await
            .expect_err("must refuse from indeterminate");
        match err {
            SystemStoreError::InvalidInput(msg) => {
                assert!(msg.contains("retryable state"));
            }
            other => panic!("expected InvalidInput, got: {other}"),
        }

        // Manual review from any state: OK.
        store.mark_saga_manual_review(id).await.unwrap();
        let row = store.get_saga(id).await.unwrap().unwrap();
        assert_eq!(row.status, SagaStatus::ManualReview);

        // Now recompensation works again from manual_review.
        store.request_saga_recompensation(id).await.unwrap();
        let row = store.get_saga(id).await.unwrap().unwrap();
        assert_eq!(row.status, SagaStatus::Indeterminate);
    }

    /// Pin: saga_summary counts every status bucket.
    #[tokio::test]
    async fn saga_summary_counts_every_bucket() {
        let store = fresh_store().await;
        store
            .record_saga(&sample_saga("t", SagaStatus::Indeterminate))
            .await
            .unwrap();
        store
            .record_saga(&sample_saga("t", SagaStatus::Committed))
            .await
            .unwrap();
        store
            .record_saga(&sample_saga("t", SagaStatus::Committed))
            .await
            .unwrap();
        store
            .record_saga(&sample_saga("t", SagaStatus::FailedCompensation))
            .await
            .unwrap();
        let s = store.saga_summary().await.unwrap();
        assert_eq!(s.indeterminate, 1);
        assert_eq!(s.committed, 2);
        assert_eq!(s.failed_compensation, 1);
        assert_eq!(s.total(), 4);
        assert_eq!(s.recoverable(), 2); // indeterminate + failed_compensation
    }

    /// Pin: get_saga on a missing saga returns Ok(None), not Err.
    #[tokio::test]
    async fn get_saga_returns_none_when_missing() {
        let store = fresh_store().await;
        let phantom = Uuid::new_v4();
        let row = store.get_saga(phantom).await.unwrap();
        assert!(row.is_none());
    }
}
