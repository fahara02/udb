//! Cassandra / ScyllaDB implementation of [`SagaStore`] (B.10a PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_saga.rs`) exactly so the
//! cross-backend conformance contract passes byte-for-byte. Cassandra has no
//! multi-row transactions, so the recovery-claim and the recovery-attempts
//! counter use per-row LWT (`UPDATE … IF …`) for the atomic compare-and-set
//! that PG gets from row locks / `RETURNING`.
//!
//! ## Schema (mirrors the logical PG `udb_sagas`; Cassandra types)
//!
//! - `udb_sagas` — `saga_id text PRIMARY KEY` (uuid as text), status /
//!   compensation_status as their EXACT PG canonical strings, `steps` /
//!   `compensations` as JSON `text`, timestamps as `timestamp`.
//!
//! ## `ALLOW FILTERING`
//!
//! `list_sagas`, `claim_recoverable_sagas`, `mark_stale_in_progress_indeterminate`
//! and `saga_summary` filter / scan on non-key columns and so use
//! `ALLOW FILTERING` + a Rust-side fold. Acceptable for the conformance
//! contract's tiny data; production would carry a `(tenant_id, status,
//! updated_at)` index / MV. Each such site is commented.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::frame::response::result::Row;
use scylla::statement::SerialConsistency;
use uuid::Uuid;

use super::cassandra::{CassandraCanonicalStore, now_unix_ms};
use super::cassandra_projection::{
    cass_err, cql_ts, get_dt, get_i32, get_json, get_text, get_uuid,
};
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};

/// Canonical saga SELECT column order. Pinned next to the mapper.
const SAGA_COLS: &str = "saga_id, tx_id, tenant_id, correlation_id, status, backend_instance, \
     operation, current_step, retry_count, recovery_attempts, compensation_status, \
     steps, compensations, last_error, created_at, updated_at";

fn row_to_saga(row: &Row) -> SystemStoreResult<SagaRow> {
    let saga_id = get_uuid(row, 0)?;
    let status_str = get_text(row, 4);
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown saga status '{status_str}' in cassandra row"
        ))
    })?;
    let comp_status_str = get_text(row, 10);
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in cassandra row"
        ))
    })?;
    Ok(SagaRow {
        saga_id,
        tx_id: get_text(row, 1),
        tenant_id: get_text(row, 2),
        correlation_id: get_text(row, 3),
        status,
        backend_instance: get_text(row, 5),
        operation: get_text(row, 6),
        current_step: get_i32(row, 7),
        retry_count: get_i32(row, 8),
        recovery_attempts: get_i32(row, 9),
        compensation_status,
        steps: get_json(row, 11, serde_json::Value::Array(vec![])),
        compensations: get_json(row, 12, serde_json::Value::Array(vec![])),
        last_error: get_text(row, 13),
        created_at: get_dt(row, 14),
        updated_at: get_dt(row, 15),
    })
}

impl CassandraCanonicalStore {
    fn saga_table(&self) -> String {
        self.qualified("udb_sagas")
    }

    /// Point read of one saga row (used internally by the LWT-CAS paths to
    /// re-read the current state before computing the next value).
    async fn read_saga_row(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        let sql = format!(
            "SELECT {SAGA_COLS} FROM {tbl} WHERE saga_id = ?",
            tbl = self.saga_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&sql, (saga_id.to_string(),))
            .await
            .map_err(|e| cass_err("get_saga", e))?;
        match rows.first() {
            Some(r) => Ok(Some(row_to_saga(r)?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl SagaStore for CassandraCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "cassandra"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        self.ensure_keyspace()
            .await
            .map_err(|e| cass_err("ensure_saga_tables keyspace", e))?;
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                saga_id text PRIMARY KEY, \
                tx_id text, \
                tenant_id text, \
                correlation_id text, \
                status text, \
                backend_instance text, \
                operation text, \
                current_step int, \
                retry_count int, \
                recovery_attempts int, \
                compensation_status text, \
                steps text, \
                compensations text, \
                last_error text, \
                created_at timestamp, \
                updated_at timestamp \
             )",
            tbl = self.saga_table(),
        );
        self.client()
            .cql_execute(&ddl, ())
            .await
            .map_err(|e| cass_err("ensure_saga_tables", e))?;
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let saga_id = Uuid::new_v4();
        let now = now_unix_ms();
        let sql = format!(
            "INSERT INTO {tbl} ( \
                saga_id, tx_id, tenant_id, correlation_id, status, backend_instance, \
                operation, current_step, retry_count, recovery_attempts, compensation_status, \
                steps, compensations, last_error, created_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.saga_table(),
        );
        self.client()
            .cql_execute(
                &sql,
                (
                    saga_id.to_string(),
                    saga.tx_id.as_str(),
                    saga.tenant_id.as_str(),
                    saga.correlation_id.as_str(),
                    saga.status.as_str(),
                    saga.backend_instance.as_str(),
                    saga.operation.as_str(),
                    0_i32,
                    0_i32,
                    0_i32,
                    CompensationStatus::None.as_str(),
                    saga.steps.to_string(),
                    saga.compensations.to_string(),
                    "",
                    cql_ts(now),
                    cql_ts(now),
                ),
            )
            .await
            .map_err(|e| cass_err("record_saga", e))?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        self.read_saga_row(saga_id).await
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        // Full scan + Rust-side filter / sort / page. Cassandra can't order by
        // a non-clustering column or OR-combine arbitrary equality predicates,
        // so we mirror the PG `WHERE … ORDER BY updated_at DESC LIMIT/OFFSET`
        // in Rust. ALLOW FILTERING: tiny conformance data.
        let scan = format!(
            "SELECT {SAGA_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.saga_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("list_sagas scan", e))?;
        let mut sagas: Vec<SagaRow> = Vec::new();
        for row in &rows {
            let saga = row_to_saga(row)?;
            if let Some(t) = &filter.tenant_id {
                if &saga.tenant_id != t {
                    continue;
                }
            }
            if let Some(s) = filter.status {
                if saga.status != s {
                    continue;
                }
            }
            if let Some(t) = &filter.tx_id {
                if &saga.tx_id != t {
                    continue;
                }
            }
            if let Some(c) = &filter.correlation_id {
                if &saga.correlation_id != c {
                    continue;
                }
            }
            sagas.push(saga);
        }
        // updated_at DESC, like PG.
        sagas.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        let limit = if filter.limit <= 0 { 100 } else { filter.limit } as usize;
        let offset = filter.offset.max(0) as usize;
        Ok(sagas.into_iter().skip(offset).take(limit).collect())
    }

    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        // Guard existence with an LWT `IF EXISTS` so a missing saga surfaces as
        // an error (PG returns rows_affected == 0 → InvalidInput).
        let now = now_unix_ms();
        let sql = format!(
            "UPDATE {tbl} SET status = ?, compensation_status = ?, updated_at = ? \
             WHERE saga_id = ? IF EXISTS",
            tbl = self.saga_table(),
        );
        let applied = self
            .client()
            .cql_lwt_applied(
                &sql,
                (
                    status.as_str(),
                    compensation_status.as_str(),
                    cql_ts(now),
                    saga_id.to_string(),
                ),
                SerialConsistency::Serial,
            )
            .await
            .map_err(|e| cass_err("update_saga_status", e))?;
        if !applied {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for update_saga_status"
            )));
        }
        Ok(())
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let now = now_unix_ms();
        let sql = format!(
            "UPDATE {tbl} SET status = ?, updated_at = ? WHERE saga_id = ? IF EXISTS",
            tbl = self.saga_table(),
        );
        let applied = self
            .client()
            .cql_lwt_applied(
                &sql,
                (
                    SagaStatus::ManualReview.as_str(),
                    cql_ts(now),
                    saga_id.to_string(),
                ),
                SerialConsistency::Serial,
            )
            .await
            .map_err(|e| cass_err("mark_saga_manual_review", e))?;
        if !applied {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        }
        Ok(())
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        // PG: conditional update WHERE status IN ('failed_compensation',
        // 'manual_review'), bumping retry_count. Cassandra LWT `IF` can't OR two
        // status equalities AND read-modify retry_count atomically in one shot,
        // so: read current state, validate the guard + compute retry_count+1,
        // then CAS `IF status = <observed>` (retry on a lost race is unnecessary
        // for the single-writer recovery path, but the CAS guards against a
        // concurrent transition out of the retryable state).
        let Some(current) = self.read_saga_row(saga_id).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        };
        if !matches!(
            current.status,
            SagaStatus::FailedCompensation | SagaStatus::ManualReview
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        }
        let now = now_unix_ms();
        let sql = format!(
            "UPDATE {tbl} SET status = ?, last_error = ?, retry_count = ?, \
             compensation_status = ?, updated_at = ? WHERE saga_id = ? IF status = ?",
            tbl = self.saga_table(),
        );
        let applied = self
            .client()
            .cql_lwt_applied(
                &sql,
                (
                    SagaStatus::Indeterminate.as_str(),
                    "",
                    current.retry_count + 1,
                    CompensationStatus::RetryRequested.as_str(),
                    cql_ts(now),
                    saga_id.to_string(),
                    current.status.as_str(),
                ),
                SerialConsistency::Serial,
            )
            .await
            .map_err(|e| cass_err("request_saga_recompensation", e))?;
        if !applied {
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
        // PG: `UPDATE … SET recovery_attempts = recovery_attempts + 1 … RETURNING`.
        // Cassandra: read current, CAS `IF recovery_attempts = <observed>`,
        // retrying the read/CAS on a lost race so the returned counter is the
        // value we actually wrote (the monotone-counter contract).
        const MAX_ATTEMPTS: u32 = 64;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            if attempt > MAX_ATTEMPTS {
                return Err(cass_err(
                    "increment_recovery_attempts",
                    "recovery_attempts CAS did not converge",
                ));
            }
            let Some(current) = self.read_saga_row(saga_id).await? else {
                return Err(SystemStoreError::InvalidInput(format!(
                    "saga {saga_id} not found for increment_recovery_attempts"
                )));
            };
            let next = current.recovery_attempts + 1;
            let now = now_unix_ms();
            let sql = format!(
                "UPDATE {tbl} SET recovery_attempts = ?, last_error = ?, updated_at = ? \
                 WHERE saga_id = ? IF recovery_attempts = ?",
                tbl = self.saga_table(),
            );
            let applied = self
                .client()
                .cql_lwt_applied(
                    &sql,
                    (
                        next,
                        error,
                        cql_ts(now),
                        saga_id.to_string(),
                        current.recovery_attempts,
                    ),
                    SerialConsistency::Serial,
                )
                .await
                .map_err(|e| cass_err("increment_recovery_attempts", e))?;
            if applied {
                return Ok(next as i64);
            }
            // Lost the CAS race — re-read and retry.
        }
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        // Scan candidates (indeterminate / in_doubt always; stale in_progress),
        // oldest-first, then per-row LWT flip the stale in_progress rows to
        // indeterminate (the Mongo impl's claim+flip-in-one-pass). A not-applied
        // flip means another worker won that row — skip it.
        let scan = format!(
            "SELECT {SAGA_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.saga_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("claim_recoverable_sagas scan", e))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::zero());
        let mut candidates: Vec<SagaRow> = Vec::new();
        for row in &rows {
            let saga = row_to_saga(row)?;
            let recoverable =
                matches!(saga.status, SagaStatus::Indeterminate | SagaStatus::InDoubt)
                    || (saga.status == SagaStatus::InProgress && saga.updated_at < cutoff);
            if recoverable {
                candidates.push(saga);
            }
        }
        candidates.sort_by_key(|s| s.updated_at);
        candidates.truncate(limit.max(1) as usize);

        let mut out = Vec::with_capacity(candidates.len());
        for mut saga in candidates {
            if saga.status == SagaStatus::InProgress {
                let now = now_unix_ms();
                let sql = format!(
                    "UPDATE {tbl} SET status = ?, last_error = ?, updated_at = ? \
                     WHERE saga_id = ? IF status = ?",
                    tbl = self.saga_table(),
                );
                let applied = self
                    .client()
                    .cql_lwt_applied(
                        &sql,
                        (
                            SagaStatus::Indeterminate.as_str(),
                            "stale in-progress reconciled at recovery claim",
                            cql_ts(now),
                            saga.saga_id.to_string(),
                            SagaStatus::InProgress.as_str(),
                        ),
                        SerialConsistency::Serial,
                    )
                    .await
                    .map_err(|e| cass_err("claim_recoverable_sagas LWT", e))?;
                if !applied {
                    // Another worker claimed this stale row first — skip it.
                    continue;
                }
                saga.status = SagaStatus::Indeterminate;
                saga.updated_at =
                    DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            }
            out.push(saga);
        }
        Ok(out)
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let scan = format!(
            "SELECT {SAGA_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.saga_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("mark_stale_in_progress_indeterminate scan", e))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::zero());
        let mut n = 0i64;
        for row in &rows {
            let saga = row_to_saga(row)?;
            if saga.status != SagaStatus::InProgress || saga.updated_at >= cutoff {
                continue;
            }
            let now = now_unix_ms();
            let sql = format!(
                "UPDATE {tbl} SET status = ?, last_error = ?, updated_at = ? \
                 WHERE saga_id = ? IF status = ?",
                tbl = self.saga_table(),
            );
            let applied = self
                .client()
                .cql_lwt_applied(
                    &sql,
                    (
                        SagaStatus::Indeterminate.as_str(),
                        "stale in-progress reconciled at startup",
                        cql_ts(now),
                        saga.saga_id.to_string(),
                        SagaStatus::InProgress.as_str(),
                    ),
                    SerialConsistency::Serial,
                )
                .await
                .map_err(|e| cass_err("mark_stale_in_progress_indeterminate LWT", e))?;
            if applied {
                n += 1;
            }
        }
        Ok(n)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let scan = format!(
            "SELECT status FROM {tbl} ALLOW FILTERING",
            tbl = self.saga_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("saga_summary scan", e))?;
        let mut s = SagaSummary::default();
        for row in &rows {
            let status = get_text(row, 0);
            match SagaStatus::parse(&status) {
                Some(SagaStatus::Indeterminate) => s.indeterminate += 1,
                Some(SagaStatus::InProgress) => s.in_progress += 1,
                Some(SagaStatus::Pending) => s.pending += 1,
                Some(SagaStatus::Committed) => s.committed += 1,
                Some(SagaStatus::Compensated) => s.compensated += 1,
                Some(SagaStatus::Failed) => s.failed += 1,
                Some(SagaStatus::InDoubt) => s.in_doubt += 1,
                Some(SagaStatus::FailedCompensation) => s.failed_compensation += 1,
                Some(SagaStatus::ManualReview) => s.manual_review += 1,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown saga status '{status}' in cassandra summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
