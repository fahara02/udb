//! ClickHouse implementation of [`SagaStore`] (B.10c PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_saga.rs`) exactly so the
//! cross-backend conformance contract passes byte-for-byte. ClickHouse has no
//! transactions / row locks / native CAS, so mutable saga state lives in a
//! `ReplacingMergeTree(version) ORDER BY saga_id`: every mutation INSERTs a NEW
//! row carrying `version + 1`, and EVERY read uses `SELECT … FINAL` so the
//! engine collapses superseded rows by `saga_id` and only the highest-`version`
//! row is observed. A raw (non-`FINAL`) read would leak stale, superseded rows
//! and break correctness.
//!
//! The recovery-attempts counter and the claim/stale flips are the versioned-CAS
//! read-insert-reread emulation (read FINAL → INSERT version+1 → re-read FINAL).
//! SINGLE-WRITER CAVEAT (see `clickhouse.rs` module docs): the read-modify-write
//! is not atomic; the conformance run is single-threaded so the monotone-counter
//! and claim contracts hold. Every such site is commented.
//!
//! ## Schema (mirrors the logical PG `udb_sagas`; ClickHouse types)
//!
//! - `udb_sagas` — `saga_id String` (uuid as text) keyed ReplacingMergeTree;
//!   status / compensation_status as their EXACT PG canonical strings, `steps` /
//!   `compensations` as JSON `String`, timestamps as epoch-millis `Int64`.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;

use super::clickhouse::{ClickHouseCanonicalStore, sql_lit};
use super::clickhouse_projection::{
    ch_dt, ch_err, ch_i32, ch_i64, ch_json, ch_str, ch_u64, ch_uuid,
};
use super::system_store::{
    CompensationStatus, SagaInsert, SagaListFilter, SagaRow, SagaStatus, SagaStore, SagaSummary,
    SystemStoreError, SystemStoreResult,
};
use uuid::Uuid;

/// Canonical saga SELECT column order. Pinned next to the mapper.
const SAGA_COLS: &str = "saga_id, tx_id, tenant_id, correlation_id, status, backend_instance, \
     operation, current_step, retry_count, recovery_attempts, compensation_status, \
     steps, compensations, last_error, created_at, updated_at";

fn row_to_saga(row: &Json) -> SystemStoreResult<SagaRow> {
    let saga_id = ch_uuid(row, "saga_id")?;
    let status_str = ch_str(row, "status");
    let status = SagaStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown saga status '{status_str}' in clickhouse row"
        ))
    })?;
    let comp_status_str = ch_str(row, "compensation_status");
    let compensation_status = CompensationStatus::parse(&comp_status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown compensation_status '{comp_status_str}' in clickhouse row"
        ))
    })?;
    Ok(SagaRow {
        saga_id,
        tx_id: ch_str(row, "tx_id"),
        tenant_id: ch_str(row, "tenant_id"),
        correlation_id: ch_str(row, "correlation_id"),
        status,
        backend_instance: ch_str(row, "backend_instance"),
        operation: ch_str(row, "operation"),
        current_step: ch_i32(row, "current_step"),
        retry_count: ch_i32(row, "retry_count"),
        recovery_attempts: ch_i32(row, "recovery_attempts"),
        compensation_status,
        steps: ch_json(row, "steps", Json::Array(vec![])),
        compensations: ch_json(row, "compensations", Json::Array(vec![])),
        last_error: ch_str(row, "last_error"),
        created_at: ch_dt(row, "created_at"),
        updated_at: ch_dt(row, "updated_at"),
    })
}

impl ClickHouseCanonicalStore {
    fn saga_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_sagas")
            .map_err(|e| ch_err("saga_table", e))
    }

    /// Read one saga's FINAL row + its version. `FINAL` collapses superseded
    /// ReplacingMergeTree parts so only the highest version is observed.
    async fn read_saga_versioned(
        &self,
        saga_id: Uuid,
    ) -> SystemStoreResult<Option<(SagaRow, u64)>> {
        let tbl = self.saga_table()?;
        let sql = format!(
            "SELECT {SAGA_COLS}, version FROM {tbl} FINAL WHERE saga_id = {id}",
            id = sql_lit(&saga_id.to_string()),
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("read_saga", e))?;
        match rows.first() {
            Some(r) => Ok(Some((row_to_saga(r)?, ch_u64(r, "version")))),
            None => Ok(None),
        }
    }

    /// INSERT a full saga row at `version`. Every mutation supersedes the prior
    /// version once FINAL/merge runs.
    async fn insert_saga_version(&self, saga: &SagaRow, version: u64) -> SystemStoreResult<()> {
        let tbl = self.saga_table()?;
        let sql = format!(
            "INSERT INTO {tbl} ({SAGA_COLS}, version) VALUES (\
             {saga_id}, {tx}, {tenant}, {corr}, {status}, {backend}, {operation}, \
             {step}, {retry}, {recovery}, {comp}, {steps}, {comps}, {error}, \
             {created}, {updated}, {version})",
            saga_id = sql_lit(&saga.saga_id.to_string()),
            tx = sql_lit(&saga.tx_id),
            tenant = sql_lit(&saga.tenant_id),
            corr = sql_lit(&saga.correlation_id),
            status = sql_lit(saga.status.as_str()),
            backend = sql_lit(&saga.backend_instance),
            operation = sql_lit(&saga.operation),
            step = saga.current_step,
            retry = saga.retry_count,
            recovery = saga.recovery_attempts,
            comp = sql_lit(saga.compensation_status.as_str()),
            steps = sql_lit(&saga.steps.to_string()),
            comps = sql_lit(&saga.compensations.to_string()),
            error = sql_lit(&saga.last_error),
            created = saga.created_at.timestamp_millis(),
            updated = saga.updated_at.timestamp_millis(),
        );
        self.executor()
            .execute_ddl(&sql)
            .await
            .map_err(|e| ch_err("insert_saga_version", e))
    }
}

#[async_trait]
impl SagaStore for ClickHouseCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "clickhouse"
    }

    async fn ensure_saga_tables(&self) -> SystemStoreResult<()> {
        let tbl = self.saga_table()?;
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} (\
             saga_id String, \
             tx_id String, \
             tenant_id String, \
             correlation_id String, \
             status String, \
             backend_instance String, \
             operation String, \
             current_step Int32, \
             retry_count Int32, \
             recovery_attempts Int32, \
             compensation_status String, \
             steps String, \
             compensations String, \
             last_error String, \
             created_at Int64, \
             updated_at Int64, \
             version UInt64\
             ) ENGINE = ReplacingMergeTree(version) ORDER BY saga_id"
        );
        self.executor()
            .execute_ddl(&ddl)
            .await
            .map_err(|e| ch_err("ensure_saga_tables", e))?;
        Ok(())
    }

    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid> {
        let saga_id = Uuid::new_v4();
        let now = Self::now_unix_ms();
        let now_dt = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
        // Fresh saga: version 1, current_step / retry_count / recovery_attempts 0,
        // compensation_status none.
        let row = SagaRow {
            saga_id,
            tx_id: saga.tx_id.clone(),
            tenant_id: saga.tenant_id.clone(),
            correlation_id: saga.correlation_id.clone(),
            status: saga.status,
            backend_instance: saga.backend_instance.clone(),
            operation: saga.operation.clone(),
            current_step: 0,
            retry_count: 0,
            recovery_attempts: 0,
            compensation_status: CompensationStatus::None,
            steps: saga.steps.clone(),
            compensations: saga.compensations.clone(),
            last_error: String::new(),
            created_at: now_dt,
            updated_at: now_dt,
        };
        self.insert_saga_version(&row, 1).await?;
        Ok(saga_id)
    }

    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>> {
        Ok(self.read_saga_versioned(saga_id).await?.map(|(row, _)| row))
    }

    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>> {
        let tbl = self.saga_table()?;
        // FINAL scan + Rust filter / DESC sort / page (mirrors PG's
        // `WHERE … ORDER BY updated_at DESC LIMIT/OFFSET`). FINAL collapses
        // superseded versions so each saga is listed once at its latest state.
        let scan = format!("SELECT {SAGA_COLS}, version FROM {tbl} FINAL");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("list_sagas scan", e))?;
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
        // Read FINAL, INSERT version+1 with the new status. A missing saga is an
        // error (PG returns rows_affected == 0 → InvalidInput).
        let Some((mut saga, version)) = self.read_saga_versioned(saga_id).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for update_saga_status"
            )));
        };
        saga.status = status;
        saga.compensation_status = compensation_status;
        saga.updated_at =
            DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms()).unwrap_or_else(Utc::now);
        self.insert_saga_version(&saga, version.saturating_add(1).max(1))
            .await
    }

    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        let Some((mut saga, version)) = self.read_saga_versioned(saga_id).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found"
            )));
        };
        saga.status = SagaStatus::ManualReview;
        saga.updated_at =
            DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms()).unwrap_or_else(Utc::now);
        self.insert_saga_version(&saga, version.saturating_add(1).max(1))
            .await
    }

    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()> {
        // PG: conditional update WHERE status IN ('failed_compensation',
        // 'manual_review'), bumping retry_count and resetting to indeterminate.
        // Read FINAL, validate the guard, INSERT version+1. SINGLE-WRITER CAVEAT
        // (see clickhouse.rs module docs): no atomic CAS, but the conformance
        // recovery path is single-threaded.
        let Some((mut saga, version)) = self.read_saga_versioned(saga_id).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        };
        if !matches!(
            saga.status,
            SagaStatus::FailedCompensation | SagaStatus::ManualReview
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} is not in a retryable state (must be failed_compensation or manual_review)"
            )));
        }
        saga.status = SagaStatus::Indeterminate;
        saga.last_error = String::new();
        saga.retry_count += 1;
        saga.compensation_status = CompensationStatus::RetryRequested;
        saga.updated_at =
            DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms()).unwrap_or_else(Utc::now);
        self.insert_saga_version(&saga, version.saturating_add(1).max(1))
            .await
    }

    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64> {
        // PG: `recovery_attempts = recovery_attempts + 1 … RETURNING`. Read FINAL,
        // INSERT version+1 with count+1, return the new count. SINGLE-WRITER
        // CAVEAT (see clickhouse.rs module docs): no atomic CAS — under a single
        // writer the returned counter is the value we wrote (the monotone-counter
        // contract); the conformance run is single-threaded.
        let Some((mut saga, version)) = self.read_saga_versioned(saga_id).await? else {
            return Err(SystemStoreError::InvalidInput(format!(
                "saga {saga_id} not found for increment_recovery_attempts"
            )));
        };
        let next = saga.recovery_attempts + 1;
        saga.recovery_attempts = next;
        saga.last_error = error.to_string();
        saga.updated_at =
            DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms()).unwrap_or_else(Utc::now);
        self.insert_saga_version(&saga, version.saturating_add(1).max(1))
            .await?;
        Ok(next as i64)
    }

    async fn claim_recoverable_sagas(
        &self,
        stale_after: Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>> {
        let tbl = self.saga_table()?;
        // FINAL scan for recoverable candidates: indeterminate / in_doubt always,
        // plus stale in_progress (updated_at older than the cutoff). Like Neo4j,
        // the indeterminate/in_doubt rows are READ as-is (the contract asserts an
        // indeterminate saga stays indeterminate); only stale in_progress rows are
        // flipped to indeterminate via versioned-CAS.
        let scan = format!("SELECT {SAGA_COLS}, version FROM {tbl} FINAL");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("claim_recoverable_sagas scan", e))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::zero());
        let mut candidates: Vec<(SagaRow, u64)> = Vec::new();
        for row in &rows {
            let saga = row_to_saga(row)?;
            let recoverable =
                matches!(saga.status, SagaStatus::Indeterminate | SagaStatus::InDoubt)
                    || (saga.status == SagaStatus::InProgress && saga.updated_at < cutoff);
            if recoverable {
                candidates.push((saga, ch_u64(row, "version")));
            }
        }
        candidates.sort_by_key(|(s, _)| s.updated_at);
        candidates.truncate(limit.max(1) as usize);

        let mut out = Vec::with_capacity(candidates.len());
        for (mut saga, version) in candidates {
            if saga.status == SagaStatus::InProgress {
                // Versioned-CAS flip stale in_progress → indeterminate. SINGLE-WRITER
                // CAVEAT (see clickhouse.rs module docs): no atomic CAS; conformance
                // run is single-threaded.
                let now_dt = DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms())
                    .unwrap_or_else(Utc::now);
                saga.status = SagaStatus::Indeterminate;
                saga.last_error = "stale in-progress reconciled at recovery claim".to_string();
                saga.updated_at = now_dt;
                self.insert_saga_version(&saga, version.saturating_add(1).max(1))
                    .await?;
            }
            out.push(saga);
        }
        Ok(out)
    }

    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: Duration,
    ) -> SystemStoreResult<i64> {
        let tbl = self.saga_table()?;
        let scan = format!("SELECT {SAGA_COLS}, version FROM {tbl} FINAL");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("mark_stale_in_progress_indeterminate scan", e))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::zero());
        let mut n = 0i64;
        for row in &rows {
            let mut saga = row_to_saga(row)?;
            if saga.status != SagaStatus::InProgress || saga.updated_at >= cutoff {
                continue;
            }
            let version = ch_u64(row, "version");
            // Versioned-CAS flip. SINGLE-WRITER CAVEAT (see clickhouse.rs module
            // docs): no atomic CAS; conformance run is single-threaded.
            saga.status = SagaStatus::Indeterminate;
            saga.last_error = "stale in-progress reconciled at startup".to_string();
            saga.updated_at = DateTime::<Utc>::from_timestamp_millis(Self::now_unix_ms())
                .unwrap_or_else(Utc::now);
            self.insert_saga_version(&saga, version.saturating_add(1).max(1))
                .await?;
            n += 1;
        }
        Ok(n)
    }

    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary> {
        let tbl = self.saga_table()?;
        // FINAL group-by status so each saga counts once at its latest status.
        let sql = format!("SELECT status, count() AS n FROM {tbl} FINAL GROUP BY status");
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("saga_summary", e))?;
        let mut s = SagaSummary::default();
        for row in &rows {
            let status = ch_str(row, "status");
            let n = ch_i64(row, "n");
            match SagaStatus::parse(&status) {
                Some(SagaStatus::Indeterminate) => s.indeterminate += n,
                Some(SagaStatus::InProgress) => s.in_progress += n,
                Some(SagaStatus::Pending) => s.pending += n,
                Some(SagaStatus::Committed) => s.committed += n,
                Some(SagaStatus::Compensated) => s.compensated += n,
                Some(SagaStatus::Failed) => s.failed += n,
                Some(SagaStatus::InDoubt) => s.in_doubt += n,
                Some(SagaStatus::FailedCompensation) => s.failed_compensation += n,
                Some(SagaStatus::ManualReview) => s.manual_review += n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown saga status '{status}' in clickhouse summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
