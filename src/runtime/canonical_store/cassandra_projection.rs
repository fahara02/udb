//! Cassandra / ScyllaDB implementation of [`ProjectionTaskStore`] (B.10a
//! PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_projection.rs`) exactly so
//! the cross-backend conformance contract passes byte-for-byte. Cassandra has
//! **no multi-row transactions and no `FOR UPDATE SKIP LOCKED`**, so the atomic
//! batch claim is done one row at a time via per-row LWT
//! (`UPDATE … IF status IN ('PENDING','FAILED')`): rows whose LWT didn't apply
//! (another worker won the Paxos round) are skipped, and we collect the applied
//! ones up to `batch_size`.
//!
//! ## Schema (mirrors the logical PG schema; Cassandra types)
//!
//! - `udb_projection_tasks` — `task_id text PRIMARY KEY` (uuid rendered as
//!   text, matching phase 1's `event_id text`), enums as their EXACT PG
//!   canonical strings, JSON columns as `text`, timestamps as `timestamp`
//!   (`CqlTimestamp` millis). `next_retry_at` / `completed_at` are nullable
//!   timestamps that round-trip `None`-when-absent.
//! - `udb_projection_idem (idempotency_key text PRIMARY KEY, task_id text)` —
//!   the idempotent-enqueue lookup table. An LWT `INSERT … IF NOT EXISTS` on
//!   this table is the Cassandra analogue of PG's
//!   `ON CONFLICT (idempotency_key) DO NOTHING`.
//!
//! ## `ALLOW FILTERING`
//!
//! Several reads (claim candidate scan, summary, metrics, dead-letter groups)
//! filter on non-key columns and therefore use `ALLOW FILTERING` + a Rust-side
//! fold. That is acceptable for the conformance contract (tiny data); a
//! production deployment would carry a `status`-keyed secondary index / MV.
//! Each such site is commented.
//!
//! This file also hosts the `pub(super)` CqlValue→typed row helpers reused by
//! the sibling system-store impls (`cassandra_saga.rs`,
//! `cassandra_migration_audit.rs`).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use scylla::frame::response::result::{CqlValue, Row};
use uuid::Uuid;

use super::cassandra::{CassandraCanonicalStore, now_unix_ms};
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

// ── Shared CqlValue→typed helpers (reused by the sibling system-store impls) ──

/// Column ordinal lookup on a `scylla::Row`. The driver returns
/// `columns: Vec<Option<CqlValue>>` in SELECT order, so the row mappers below
/// index by position (the SELECT column lists are pinned next to each mapper).
pub(super) fn col(row: &Row, idx: usize) -> Option<&CqlValue> {
    row.columns.get(idx).and_then(|c| c.as_ref())
}

/// Read a text column → owned `String` (empty when absent/null/non-text).
pub(super) fn get_text(row: &Row, idx: usize) -> String {
    col(row, idx)
        .and_then(|v| v.as_text())
        .cloned()
        .unwrap_or_default()
}

/// Read an `int`/`bigint` column → `i32` (0 when absent). Accepts a bigint for
/// safety (Cassandra `int` decodes as `CqlValue::Int`).
pub(super) fn get_i32(row: &Row, idx: usize) -> i32 {
    col(row, idx)
        .and_then(|v| v.as_int().or_else(|| v.as_bigint().map(|b| b as i32)))
        .unwrap_or(0)
}

/// Read a `bigint` column → `i64` (0 when absent).
pub(super) fn get_i64(row: &Row, idx: usize) -> i64 {
    col(row, idx)
        .and_then(|v| v.as_bigint().or_else(|| v.as_int().map(i64::from)))
        .unwrap_or(0)
}

/// Read a CQL `timestamp` column (millis since epoch) as a chrono
/// `DateTime<Utc>`. Falls back to `Utc::now()` when absent/unparseable,
/// matching the SQL impls' tolerant `unwrap_or_else(|_| Utc::now())`.
pub(super) fn get_dt(row: &Row, idx: usize) -> DateTime<Utc> {
    get_opt_dt(row, idx).unwrap_or_else(Utc::now)
}

/// Read an OPTIONAL CQL `timestamp` column: `None` when the column is absent or
/// CQL-null, `Some` when a timestamp is present. This is the round-trip
/// discipline the MSSQL bug flagged — a missing column must parse back to
/// `None` (not `Some(now)`).
pub(super) fn get_opt_dt(row: &Row, idx: usize) -> Option<DateTime<Utc>> {
    match col(row, idx) {
        // scylla 0.13 decodes a `timestamp` column to `CqlValue::Timestamp`
        // carrying a `CqlTimestamp(i64 millis)`. `as_cql_timestamp()` yields
        // that wrapper; `.0` is the millis. Accept a raw bigint too for safety.
        Some(v) => {
            let millis = v
                .as_cql_timestamp()
                .map(|t| t.0)
                .or_else(|| v.as_bigint())?;
            DateTime::<Utc>::from_timestamp_millis(millis)
        }
        None => None,
    }
}

/// Parse a `Uuid` from a text column. The store renders uuid PKs as text
/// (matching phase 1's `event_id text`), so the parse is text→uuid.
pub(super) fn get_uuid(row: &Row, idx: usize) -> SystemStoreResult<Uuid> {
    let raw = col(row, idx).and_then(|v| v.as_text()).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!("cassandra row missing text uuid at column {idx}"))
    })?;
    Uuid::parse_str(raw).map_err(|e| {
        SystemStoreError::InvalidInput(format!(
            "cassandra row column {idx} is not a uuid '{raw}': {e}"
        ))
    })
}

/// Decode a JSON-shaped text column back into `serde_json::Value`.
/// Absent/null/unparseable → `default`.
pub(super) fn get_json(row: &Row, idx: usize, default: serde_json::Value) -> serde_json::Value {
    match col(row, idx).and_then(|v| v.as_text()) {
        Some(s) => serde_json::from_str(s).unwrap_or(default),
        None => default,
    }
}

/// Map any client error string to a typed `SystemStoreError::Io` for the
/// `"cassandra"` backend. `op` names the failed operation.
pub(super) fn cass_err(op: &str, err: impl std::fmt::Display) -> SystemStoreError {
    SystemStoreError::Io {
        backend: "cassandra",
        source: format!("{op}: {err}"),
    }
}

/// `scylla::frame::value::CqlTimestamp` from unix millis — the bind type CQL
/// `timestamp` columns expect on the typed `SerializeRow` path (same as
/// phase 1's outbox `created_at`).
pub(super) fn cql_ts(millis: i64) -> scylla::frame::value::CqlTimestamp {
    scylla::frame::value::CqlTimestamp(millis)
}

// ── Row mapping ───────────────────────────────────────────────────────────────

/// The canonical projection-task SELECT column order. Both the table mapper
/// below and every `SELECT` that feeds it use this exact order.
const TASK_COLS: &str = "task_id, idempotency_key, project_id, target_backend, target_instance, \
     projection_kind, resource_name, operation, source_row_key, target_options, \
     source_payload, source_checksum, status, retry_count, last_error, \
     created_at, updated_at, next_retry_at, completed_at, manifest_checksum";

fn row_to_projection_task(row: &Row) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id = get_uuid(row, 0)?;
    let operation_str = get_text(row, 7);
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in cassandra row"
        ))
    })?;
    let status_str = get_text(row, 12);
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in cassandra row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: get_text(row, 1),
        project_id: get_text(row, 2),
        manifest_checksum: get_text(row, 19),
        target_backend: get_text(row, 3),
        target_instance: get_text(row, 4),
        projection_kind: get_text(row, 5),
        resource_name: get_text(row, 6),
        operation,
        source_row_key: get_json(row, 8, serde_json::Value::Null),
        target_options: get_json(row, 9, serde_json::Value::Null),
        source_payload: get_json(row, 10, serde_json::Value::Null),
        source_checksum: get_text(row, 11),
        status,
        retry_count: get_i32(row, 13),
        last_error: get_text(row, 14),
        created_at: get_dt(row, 15),
        updated_at: get_dt(row, 16),
        next_retry_at: get_opt_dt(row, 17),
        completed_at: get_opt_dt(row, 18),
    })
}

impl CassandraCanonicalStore {
    fn projection_table(&self) -> String {
        self.qualified("udb_projection_tasks")
    }
    fn idem_table(&self) -> String {
        self.qualified("udb_projection_idem")
    }
}

#[async_trait]
impl ProjectionTaskStore for CassandraCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "cassandra"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        self.ensure_keyspace()
            .await
            .map_err(|e| cass_err("ensure_projection_tables keyspace", e))?;
        let tasks_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( \
                task_id text PRIMARY KEY, \
                idempotency_key text, \
                project_id text, \
                manifest_checksum text, \
                message_type text, \
                source_schema text, \
                source_table text, \
                source_row_key text, \
                operation text, \
                target_backend text, \
                target_instance text, \
                projection_kind text, \
                resource_name text, \
                target_options text, \
                source_payload text, \
                source_checksum text, \
                status text, \
                retry_count int, \
                last_error text, \
                created_at timestamp, \
                updated_at timestamp, \
                next_retry_at timestamp, \
                completed_at timestamp \
             )",
            tbl = self.projection_table(),
        );
        self.client()
            .cql_execute(&tasks_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_projection_tables tasks", e))?;
        // Idempotency lookup table — the Cassandra analogue of the PG UNIQUE
        // constraint that backs idempotent enqueue.
        let idem_ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} ( idempotency_key text PRIMARY KEY, task_id text )",
            tbl = self.idem_table(),
        );
        self.client()
            .cql_execute(&idem_ddl, ())
            .await
            .map_err(|e| cass_err("ensure_projection_tables idem", e))?;
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        use scylla::statement::SerialConsistency;
        let new_id = Uuid::new_v4();
        // 1. Claim the idempotency key via LWT `INSERT … IF NOT EXISTS`. Applied
        //    ⇒ we own a fresh task_id. Not-applied ⇒ a row already exists; read
        //    the existing task_id and return it (idempotent — PG's CTE fallback).
        let idem_insert = format!(
            "INSERT INTO {tbl} (idempotency_key, task_id) VALUES (?, ?) IF NOT EXISTS",
            tbl = self.idem_table(),
        );
        let applied = self
            .client()
            .cql_lwt_applied(
                &idem_insert,
                (task.idempotency_key.as_str(), new_id.to_string()),
                SerialConsistency::Serial,
            )
            .await
            .map_err(|e| cass_err("enqueue_projection_task idem LWT", e))?;
        if !applied {
            let lookup = format!(
                "SELECT task_id FROM {tbl} WHERE idempotency_key = ?",
                tbl = self.idem_table(),
            );
            let rows = self
                .client()
                .cql_query_rows(&lookup, (task.idempotency_key.as_str(),))
                .await
                .map_err(|e| cass_err("enqueue_projection_task idem lookup", e))?;
            let row = rows.first().ok_or_else(|| {
                cass_err(
                    "enqueue_projection_task",
                    "idempotency row vanished after not-applied LWT",
                )
            })?;
            return get_uuid(row, 0);
        }

        // 2. We won the key — insert the full task row. Plain write: the
        //    task_id is uniquely ours from the LWT above.
        let now = now_unix_ms();
        let insert = format!(
            "INSERT INTO {tbl} ( \
                task_id, idempotency_key, project_id, manifest_checksum, message_type, \
                source_schema, source_table, source_row_key, operation, \
                target_backend, target_instance, projection_kind, resource_name, \
                target_options, source_payload, source_checksum, \
                status, retry_count, last_error, created_at, updated_at \
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tbl = self.projection_table(),
        );
        // 21 columns exceeds scylla's tuple `SerializeRow` impls (max 16), so
        // bind a `Vec<CqlValue>` (which impls `SerializeRow` for any length).
        use scylla::frame::response::result::CqlValue;
        let params: Vec<CqlValue> = vec![
            CqlValue::Text(new_id.to_string()),
            CqlValue::Text(task.idempotency_key.clone()),
            CqlValue::Text(task.project_id.clone()),
            CqlValue::Text(task.manifest_checksum.clone()),
            CqlValue::Text(task.message_type.clone()),
            CqlValue::Text(task.source_schema.clone()),
            CqlValue::Text(task.source_table.clone()),
            CqlValue::Text(task.source_row_key.to_string()),
            CqlValue::Text(task.operation.as_str().to_string()),
            CqlValue::Text(task.target_backend.clone()),
            CqlValue::Text(task.target_instance.clone()),
            CqlValue::Text(task.projection_kind.clone()),
            CqlValue::Text(task.resource_name.clone()),
            CqlValue::Text(task.target_options.to_string()),
            CqlValue::Text(task.source_payload.to_string()),
            CqlValue::Text(task.source_checksum.clone()),
            CqlValue::Text(ProjectionTaskStatus::Pending.as_str().to_string()),
            CqlValue::Int(0),
            CqlValue::Text(String::new()),
            CqlValue::Timestamp(cql_ts(now)),
            CqlValue::Timestamp(cql_ts(now)),
        ];
        self.client()
            .cql_execute(&insert, params)
            .await
            .map_err(|e| cass_err("enqueue_projection_task insert", e))?;
        Ok(new_id)
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        use scylla::statement::SerialConsistency;
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        // Candidate scan. Cassandra can't OR two status predicates with the
        // other equality filters efficiently, so we scan the whole table with
        // ALLOW FILTERING and apply the PENDING/FAILED + retry/next_retry/target
        // predicates in Rust (mirrors the PG candidate CTEs). Acceptable for the
        // conformance contract's tiny data; production would use a status index.
        let scan = format!(
            "SELECT {TASK_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("claim_projection_tasks scan", e))?;
        let now = Utc::now();
        let mut candidates: Vec<ProjectionTaskRow> = Vec::new();
        for row in &rows {
            let task = row_to_projection_task(row)?;
            if !task.status.is_claimable() {
                continue;
            }
            if task.retry_count >= filter.max_retries {
                continue;
            }
            if let Some(p) = &filter.project_id {
                if &task.project_id != p {
                    continue;
                }
            }
            if let Some(b) = &filter.target_backend {
                if &task.target_backend != b {
                    continue;
                }
            }
            if let Some(i) = &filter.target_instance {
                if &task.target_instance != i {
                    continue;
                }
            }
            // FAILED rows are only eligible once next_retry_at is due (PG's
            // `next_retry_at IS NULL OR next_retry_at <= NOW()`). PENDING is
            // always eligible.
            if task.status == ProjectionTaskStatus::Failed {
                if let Some(nra) = task.next_retry_at {
                    if nra > now {
                        continue;
                    }
                }
            }
            candidates.push(task);
        }
        // Oldest-first, like PG's `ORDER BY created_at`.
        candidates.sort_by_key(|t| t.created_at);

        // Per-row LWT claim: flip each candidate to IN_PROGRESS only if it is
        // still PENDING/FAILED. A not-applied LWT means another worker won that
        // row — skip it. Collect applied rows up to batch_size.
        let claim_now = now_unix_ms();
        let mut out = Vec::new();
        for mut task in candidates {
            if out.len() as i64 >= filter.batch_size {
                break;
            }
            let cas = format!(
                "UPDATE {tbl} SET status = ?, updated_at = ? WHERE task_id = ? \
                 IF status IN ('PENDING','FAILED')",
                tbl = self.projection_table(),
            );
            let applied = self
                .client()
                .cql_lwt_applied(
                    &cas,
                    (
                        ProjectionTaskStatus::InProgress.as_str(),
                        cql_ts(claim_now),
                        task.task_id.to_string(),
                    ),
                    SerialConsistency::Serial,
                )
                .await
                .map_err(|e| cass_err("claim_projection_tasks LWT", e))?;
            if applied {
                // Reflect the post-claim state in the returned row (PG's
                // UPDATE … RETURNING shows IN_PROGRESS at the new updated_at).
                task.status = ProjectionTaskStatus::InProgress;
                task.updated_at =
                    DateTime::<Utc>::from_timestamp_millis(claim_now).unwrap_or_else(Utc::now);
                out.push(task);
            }
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        // next_retry_at + completed_at: clearing next_retry_at to CQL null
        // round-trips as None (PG sets next_retry_at = NULL).
        let now = now_unix_ms();
        let sql = format!(
            "UPDATE {tbl} SET status = ?, completed_at = ?, next_retry_at = ?, updated_at = ? \
             WHERE task_id = ?",
            tbl = self.projection_table(),
        );
        self.client()
            .cql_execute(
                &sql,
                (
                    ProjectionTaskStatus::Completed.as_str(),
                    cql_ts(now),
                    None::<scylla::frame::value::CqlTimestamp>,
                    cql_ts(now),
                    task_id.to_string(),
                ),
            )
            .await
            .map_err(|e| cass_err("mark_projection_task_completed", e))?;
        Ok(())
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        if !matches!(
            new_status,
            ProjectionTaskStatus::Failed | ProjectionTaskStatus::DeadLetter
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "mark_projection_task_failed only accepts FAILED or DEAD_LETTER, got {}",
                new_status.as_str()
            )));
        }
        let now = now_unix_ms();
        // PG clears next_retry_at to NULL here. The conformance contract re-claims
        // the FAILED row immediately afterward, so a NULL next_retry_at (always
        // due) matches PG's observable behaviour.
        let sql = format!(
            "UPDATE {tbl} SET status = ?, retry_count = ?, last_error = ?, \
             next_retry_at = ?, updated_at = ? WHERE task_id = ?",
            tbl = self.projection_table(),
        );
        self.client()
            .cql_execute(
                &sql,
                (
                    new_status.as_str(),
                    new_retry_count,
                    error,
                    None::<scylla::frame::value::CqlTimestamp>,
                    cql_ts(now),
                    task_id.to_string(),
                ),
            )
            .await
            .map_err(|e| cass_err("mark_projection_task_failed", e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        // Scan DEAD_LETTER rows (ALLOW FILTERING) + per-row UPDATE back to
        // PENDING. Cassandra has no `UPDATE … WHERE status=…` count, so we
        // count rows we touch in Rust.
        let scan = format!(
            "SELECT {TASK_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("requeue_dead_letter_tasks scan", e))?;
        let now = now_unix_ms();
        let mut n = 0i64;
        for row in &rows {
            let task = row_to_projection_task(row)?;
            if task.status != ProjectionTaskStatus::DeadLetter {
                continue;
            }
            if let Some(b) = target_backend {
                if &task.target_backend != b {
                    continue;
                }
            }
            let upd = format!(
                "UPDATE {tbl} SET status = ?, retry_count = ?, last_error = ?, \
                 next_retry_at = ?, updated_at = ? WHERE task_id = ?",
                tbl = self.projection_table(),
            );
            self.client()
                .cql_execute(
                    &upd,
                    (
                        ProjectionTaskStatus::Pending.as_str(),
                        0_i32,
                        "",
                        None::<scylla::frame::value::CqlTimestamp>,
                        cql_ts(now),
                        task.task_id.to_string(),
                    ),
                )
                .await
                .map_err(|e| cass_err("requeue_dead_letter_tasks update", e))?;
            n += 1;
        }
        Ok(n)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let scan = format!(
            "SELECT {TASK_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("reset_stale_in_progress_tasks scan", e))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(stale_after).unwrap_or_else(|_| chrono::Duration::zero());
        let now = now_unix_ms();
        let mut n = 0i64;
        for row in &rows {
            let task = row_to_projection_task(row)?;
            if task.status != ProjectionTaskStatus::InProgress {
                continue;
            }
            if task.updated_at >= cutoff {
                continue;
            }
            let upd = format!(
                "UPDATE {tbl} SET status = ?, last_error = ?, updated_at = ? WHERE task_id = ?",
                tbl = self.projection_table(),
            );
            self.client()
                .cql_execute(
                    &upd,
                    (
                        ProjectionTaskStatus::Pending.as_str(),
                        "stale in-progress reconciliation",
                        cql_ts(now),
                        task.task_id.to_string(),
                    ),
                )
                .await
                .map_err(|e| cass_err("reset_stale_in_progress_tasks update", e))?;
            n += 1;
        }
        Ok(n)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        // Aggregate PENDING+FAILED per (project, backend, instance, kind) in
        // Rust (Cassandra GROUP BY is limited to the partition/clustering keys).
        let scan = format!(
            "SELECT {TASK_COLS} FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("pending_task_metrics scan", e))?;
        use std::collections::HashMap;
        // group key → (count, oldest created_at)
        let mut groups: HashMap<(String, String, String, String), (i64, DateTime<Utc>)> =
            HashMap::new();
        for row in &rows {
            let task = row_to_projection_task(row)?;
            if !matches!(
                task.status,
                ProjectionTaskStatus::Pending | ProjectionTaskStatus::Failed
            ) {
                continue;
            }
            let key = (
                task.project_id.clone(),
                task.target_backend.clone(),
                task.target_instance.clone(),
                task.projection_kind.clone(),
            );
            let entry = groups.entry(key).or_insert((0, task.created_at));
            entry.0 += 1;
            if task.created_at < entry.1 {
                entry.1 = task.created_at;
            }
        }
        let now = Utc::now();
        let mut out: Vec<PendingTaskMetric> = groups
            .into_iter()
            .map(
                |((project_id, target_backend, target_instance, projection_kind), (n, oldest))| {
                    let age = (now - oldest).num_milliseconds() as f64 / 1000.0;
                    PendingTaskMetric {
                        project_id,
                        target_backend,
                        target_instance,
                        projection_kind,
                        pending: n,
                        oldest_age_seconds: age.max(0.0),
                    }
                },
            )
            .collect();
        out.truncate(limit.max(1) as usize);
        Ok(out)
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        // `source_table` isn't in the claim-row column set, so this scan selects
        // it directly: (project_id, source_table, target_backend,
        // target_instance, status, last_error).
        let scan = format!(
            "SELECT project_id, source_table, target_backend, target_instance, status, last_error \
             FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("dead_letter_groups scan", e))?;
        use std::collections::HashMap;
        let mut groups: HashMap<(String, String, String, String), i64> =
            HashMap::new();
        for row in &rows {
            if get_text(row, 4) != ProjectionTaskStatus::DeadLetter.as_str()
                || get_text(row, 5).starts_with(
                    super::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX,
                )
            {
                continue;
            }
            *groups
                .entry((
                    get_text(row, 0),
                    get_text(row, 1),
                    get_text(row, 2),
                    get_text(row, 3),
                ))
                .or_insert(0) += 1;
        }
        let mut out: Vec<DeadLetterGroup> = groups
            .into_iter()
            .map(
                |(
                    (project_id, source_table, target_backend, target_instance),
                    dead_count,
                )| DeadLetterGroup {
                    project_id,
                    source_table,
                    target_backend,
                    target_instance,
                    dead_count,
                },
            )
            .collect();
        out.truncate(limit.max(1) as usize);
        Ok(out)
    }

    async fn requeue_dead_letter_by_source(
        &self,
        project_id: &str,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        // Scan + per-row update for the matching (project_id, source_table,
        // backend, instance) tuple. source_table is read via the dedicated
        // SELECT below (it isn't in the claim-row column set).
        let scan = format!(
            "SELECT task_id, project_id, source_table, target_backend, target_instance, status, last_error \
             FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("requeue_dead_letter_by_source scan", e))?;
        let now = now_unix_ms();
        let mut n = 0i64;
        for row in &rows {
            let status = get_text(row, 5);
            if status != ProjectionTaskStatus::DeadLetter.as_str()
                || get_text(row, 6).starts_with(
                    super::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX,
                )
            {
                continue;
            }
            if get_text(row, 1) != project_id
                || get_text(row, 2) != source_table
                || get_text(row, 3) != target_backend
                || get_text(row, 4) != target_instance
            {
                continue;
            }
            let task_id = get_text(row, 0);
            let upd = format!(
                "UPDATE {tbl} SET status = ?, retry_count = ?, last_error = ?, \
                 next_retry_at = ?, updated_at = ? WHERE task_id = ?",
                tbl = self.projection_table(),
            );
            self.client()
                .cql_execute(
                    &upd,
                    (
                        ProjectionTaskStatus::Pending.as_str(),
                        0_i32,
                        "reconciliation repair",
                        None::<scylla::frame::value::CqlTimestamp>,
                        cql_ts(now),
                        task_id,
                    ),
                )
                .await
                .map_err(|e| cass_err("requeue_dead_letter_by_source update", e))?;
            n += 1;
        }
        Ok(n)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        // Resolve each idempotency_key → task_id via the idem lookup table
        // (point reads on the PK), then read that task's status. A key whose
        // task is COMPLETED/DEAD_LETTER/FAILED is "settled" and not counted.
        let mut pending = 0i64;
        for key in idempotency_keys {
            let lookup = format!(
                "SELECT task_id FROM {tbl} WHERE idempotency_key = ?",
                tbl = self.idem_table(),
            );
            let idem_rows = self
                .client()
                .cql_query_rows(&lookup, (key.as_str(),))
                .await
                .map_err(|e| cass_err("pending_projection_task_count lookup", e))?;
            let Some(idem_row) = idem_rows.first() else {
                continue;
            };
            let task_id = get_text(idem_row, 0);
            let status_sql = format!(
                "SELECT status FROM {tbl} WHERE task_id = ?",
                tbl = self.projection_table(),
            );
            let status_rows = self
                .client()
                .cql_query_rows(&status_sql, (task_id,))
                .await
                .map_err(|e| cass_err("pending_projection_task_count status", e))?;
            let Some(status_row) = status_rows.first() else {
                continue;
            };
            let status = get_text(status_row, 0);
            // P2-1 NF-1/NF-2: only COMPLETED settles the fence; FAILED (will retry)
            // and DEAD_LETTER (will never complete) are not projected yet, so they
            // still count as pending for read-your-writes.
            let settled = matches!(
                ProjectionTaskStatus::parse(&status),
                Some(ProjectionTaskStatus::Completed)
            );
            if !settled {
                pending += 1;
            }
        }
        Ok(pending)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        // Scan + Rust fold by status (Cassandra has no global GROUP BY across
        // partitions).
        let scan = format!(
            "SELECT status FROM {tbl} ALLOW FILTERING",
            tbl = self.projection_table(),
        );
        let rows = self
            .client()
            .cql_query_rows(&scan, ())
            .await
            .map_err(|e| cass_err("projection_task_summary scan", e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in &rows {
            let status = get_text(row, 0);
            match ProjectionTaskStatus::parse(&status) {
                Some(ProjectionTaskStatus::Pending) => s.pending += 1,
                Some(ProjectionTaskStatus::InProgress) => s.in_progress += 1,
                Some(ProjectionTaskStatus::Completed) => s.completed += 1,
                Some(ProjectionTaskStatus::Failed) => s.failed += 1,
                Some(ProjectionTaskStatus::DeadLetter) => s.dead_letter += 1,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown projection status '{status}' in cassandra summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
