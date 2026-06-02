//! B.8 phase 2 — SQL Server (Tiberius) implementation of
//! [`ProjectionTaskStore`].
//!
//! Mirrors the Postgres impl's semantics in T-SQL:
//!
//! - `task_id UNIQUEIDENTIFIER PRIMARY KEY` (assigned by Rust via
//!   `Uuid::new_v4`, not a column default — tiberius binds it as text and
//!   the INSERT `CONVERT`s).
//! - `NVARCHAR(MAX)` + `ISJSON(...) = 1` for the JSON columns.
//! - `DATETIME2(7) DEFAULT SYSUTCDATETIME()` for timestamps.
//! - Idempotent enqueue keyed on `idempotency_key` via `MERGE`
//!   (`WHEN NOT MATCHED INSERT`) then a `SELECT` that always returns the
//!   (new or existing) `task_id` — the SQL Server analogue of PG's
//!   `ON CONFLICT DO NOTHING` + CTE fallback.
//! - Atomic claim with skip-locked semantics via
//!   `WITH (READPAST, UPDLOCK, ROWLOCK)` + `UPDATE … OUTPUT inserted.*`.
//!
//! ## Row decoding without tiberius type features
//!
//! The crate enables tiberius with only `tds73` + `rustls` (no `chrono`
//! / `time` / `uuid` decode features), so this impl never asks tiberius
//! to decode a `UNIQUEIDENTIFIER` or `DATETIME2` directly. Every SELECT
//! casts uuids to `NVARCHAR(36)` and timestamps to ISO-8601 via
//! `CONVERT(NVARCHAR(33), col, 127)`, then Rust parses the strings. This
//! matches the executor's existing string-first decode posture
//! (`executors::mssql::row_to_json`).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tiberius::Row;
use uuid::Uuid;

use super::dialect::apply_projection_summary_bucket;
use super::mssql::MssqlCanonicalStore;
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};
use crate::runtime::executors::mssql::SqlParam;

/// Default object name when no override is supplied. Matches the canonical
/// `udb_projection_tasks` table the DDL builder creates.
const DEFAULT_REL: &str = "udb_projection_tasks";

impl MssqlCanonicalStore {
    /// Override the projection_tasks relation. Defaults to
    /// `udb_projection_tasks`.
    pub fn with_projection_relation(mut self, relation: impl Into<String>) -> Self {
        self.projection_relation = Some(relation.into());
        self
    }

    pub(super) fn projection_relation_ref(&self) -> &str {
        self.projection_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

// ── String → typed helpers (shared posture across the mssql_* impls) ──────────

/// Parse a `NVARCHAR(36)` uuid cell.
pub(super) fn parse_uuid_cell(s: &str) -> SystemStoreResult<Uuid> {
    Uuid::parse_str(s.trim()).map_err(|e| {
        SystemStoreError::InvalidInput(format!("invalid uuid '{s}' in mssql row: {e}"))
    })
}

/// Parse an ISO-8601 timestamp cell (`CONVERT(..., 127)` → e.g.
/// `2026-06-02T12:34:56.1234567Z`). Falls back to `Utc::now()` on a
/// malformed/empty value, matching the PG impl's tolerant decode.
pub(super) fn parse_ts_cell(s: Option<&str>) -> DateTime<Utc> {
    parse_iso_utc(s).unwrap_or_else(Utc::now)
}

/// Parse an optional ISO-8601 timestamp cell (NULL → `None`).
pub(super) fn parse_opt_ts_cell(s: Option<&str>) -> Option<DateTime<Utc>> {
    parse_iso_utc(s)
}

/// Parse an ISO-8601 timestamp cell into UTC. MSSQL renders `datetime2` via
/// `CONVERT(NVARCHAR, …, 127)` WITHOUT a timezone offset (e.g.
/// `2026-06-02T10:15:30.1234567`), so RFC3339 (which requires an offset) is
/// tried first and a naive-datetime parse (assumed UTC) is the fallback. This
/// is why `finished_at`/`applied_at` (Option fields) previously read back as
/// None even though the row had a value.
fn parse_iso_utc(s: Option<&str>) -> Option<DateTime<Utc>> {
    let text = s.map(str::trim).filter(|t| !t.is_empty())?;
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, fmt) {
            return Some(DateTime::from_naive_utc_and_offset(ndt, Utc));
        }
    }
    None
}

/// Read a column as an owned `String`, surfacing NULL as `String::new()`.
pub(super) fn str_col(row: &Row, name: &str) -> String {
    row.try_get::<&str, _>(name)
        .ok()
        .flatten()
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Read a column as `Option<String>` (NULL → `None`).
pub(super) fn opt_str_col(row: &Row, name: &str) -> Option<String> {
    row.try_get::<&str, _>(name)
        .ok()
        .flatten()
        .map(|s| s.to_string())
}

/// Read a column as `i32`, defaulting to 0.
pub(super) fn i32_col(row: &Row, name: &str) -> i32 {
    row.try_get::<i32, _>(name).ok().flatten().unwrap_or(0)
}

/// Read a JSON column (stored as NVARCHAR text) and parse it.
pub(super) fn json_col(row: &Row, name: &str) -> serde_json::Value {
    match opt_str_col(row, name) {
        Some(text) => serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
    }
}

fn row_to_projection_task(row: &Row) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id_str = opt_str_col(row, "task_id")
        .ok_or_else(|| SystemStoreError::query("mssql", "SELECT task_id", "task_id was NULL"))?;
    let task_id = parse_uuid_cell(&task_id_str)?;
    let operation_str = str_col(row, "operation");
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in mssql row"
        ))
    })?;
    let status_str = str_col(row, "status");
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in mssql row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: str_col(row, "idempotency_key"),
        project_id: str_col(row, "project_id"),
        target_backend: str_col(row, "target_backend"),
        target_instance: str_col(row, "target_instance"),
        projection_kind: str_col(row, "projection_kind"),
        resource_name: str_col(row, "resource_name"),
        operation,
        source_row_key: json_col(row, "source_row_key"),
        target_options: json_col(row, "target_options"),
        source_payload: json_col(row, "source_payload"),
        source_checksum: str_col(row, "source_checksum"),
        status,
        retry_count: i32_col(row, "retry_count"),
        last_error: str_col(row, "last_error"),
        created_at: parse_ts_cell(opt_str_col(row, "created_at").as_deref()),
        updated_at: parse_ts_cell(opt_str_col(row, "updated_at").as_deref()),
        next_retry_at: parse_opt_ts_cell(opt_str_col(row, "next_retry_at").as_deref()),
        completed_at: parse_opt_ts_cell(opt_str_col(row, "completed_at").as_deref()),
    })
}

#[async_trait]
impl ProjectionTaskStore for MssqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mssql"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = super::sql_schema::mssql_projection_tasks_ddl(rel);
        self.client()
            .simple_batch(&sql)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // SQL Server has no ON CONFLICT. MERGE inserts only when the
        // idempotency_key is absent (`WHEN NOT MATCHED`); the trailing
        // SELECT then returns the row's task_id whether it was just
        // inserted or already existed — mirroring PG's "return the new or
        // existing id". HOLDLOCK serialises concurrent enqueues on the key
        // so two racers can't both insert. A freshly minted UUID is bound
        // for the NOT-MATCHED branch.
        let new_task_id = Uuid::new_v4();
        let sql = format!(
            "MERGE {rel} WITH (HOLDLOCK) AS target \
             USING (SELECT @P1 AS idempotency_key) AS src \
             ON target.idempotency_key = src.idempotency_key \
             WHEN NOT MATCHED THEN \
                INSERT (task_id, idempotency_key, project_id, manifest_checksum, message_type, \
                        source_schema, source_table, source_row_key, operation, \
                        target_backend, target_instance, projection_kind, resource_name, \
                        target_options, source_payload, source_checksum) \
                VALUES (CONVERT(UNIQUEIDENTIFIER, @P2), @P1, @P3, @P4, @P5, \
                        @P6, @P7, @P8, @P9, \
                        @P10, @P11, @P12, @P13, \
                        @P14, @P15, @P16); \
             SELECT CONVERT(NVARCHAR(36), task_id) AS task_id FROM {rel} WHERE idempotency_key = @P1;"
        );
        let params = [
            SqlParam::Str(task.idempotency_key.clone()),
            SqlParam::Str(new_task_id.to_string()),
            SqlParam::Str(task.project_id.clone()),
            SqlParam::Str(task.manifest_checksum.clone()),
            SqlParam::Str(task.message_type.clone()),
            SqlParam::Str(task.source_schema.clone()),
            SqlParam::Str(task.source_table.clone()),
            SqlParam::Str(task.source_row_key.to_string()),
            SqlParam::Str(task.operation.as_str().to_string()),
            SqlParam::Str(task.target_backend.clone()),
            SqlParam::Str(task.target_instance.clone()),
            SqlParam::Str(task.projection_kind.clone()),
            SqlParam::Str(task.resource_name.clone()),
            SqlParam::Str(task.target_options.to_string()),
            SqlParam::Str(task.source_payload.to_string()),
            SqlParam::Str(task.source_checksum.clone()),
        ];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let id_str = rows
            .first()
            .and_then(|r| r.try_get::<&str, _>("task_id").ok().flatten())
            .ok_or_else(|| {
                SystemStoreError::query("mssql", sql.clone(), "enqueue returned no task_id")
            })?;
        parse_uuid_cell(id_str)
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // Build the optional project/target filters with sequential
        // @Pn placeholders. @P1 = max_retries, @P2 = batch_size are fixed.
        let mut next_param = 3usize;
        let project_filter = if filter.project_id.is_some() {
            let clause = format!("AND project_id = @P{next_param}");
            next_param += 1;
            clause
        } else {
            String::new()
        };
        let target_filter = match (&filter.target_backend, &filter.target_instance) {
            (Some(_), Some(_)) => {
                let clause = format!(
                    "AND target_backend = @P{next_param} AND target_instance = @P{}",
                    next_param + 1
                );
                clause
            }
            (Some(_), None) => format!("AND target_backend = @P{next_param}"),
            (None, Some(_)) => format!("AND target_instance = @P{next_param}"),
            (None, None) => String::new(),
        };
        // Atomic claim with skip-locked semantics. READPAST skips rows other
        // workers have locked (the SQL Server analogue of `SKIP LOCKED`);
        // UPDLOCK + ROWLOCK take an update lock so the chosen rows can't be
        // grabbed by a concurrent claimer between the CTE read and the UPDATE.
        // The filter matches the PG impl exactly: PENDING with retry_count <
        // max, plus FAILED with retry_count < max AND next_retry_at due. The
        // UPDATE OUTPUT clause is T-SQL's RETURNING. Wrapped in BEGIN/COMMIT
        // so the lock + update are one transaction. The OUTPUT casts uuids /
        // timestamps to text for tiberius.
        let sql = format!(
            "BEGIN TRANSACTION; \
             WITH candidates AS ( \
                SELECT TOP (@P2) task_id FROM {rel} WITH (READPAST, UPDLOCK, ROWLOCK) \
                WHERE retry_count < @P1 \
                  AND ( \
                       status = 'PENDING' \
                    OR (status = 'FAILED' AND (next_retry_at IS NULL OR next_retry_at <= SYSUTCDATETIME())) \
                  ) \
                  {project_filter} \
                  {target_filter} \
                ORDER BY created_at \
             ) \
             UPDATE {rel} \
             SET status = 'IN_PROGRESS', updated_at = SYSUTCDATETIME() \
             OUTPUT \
                CONVERT(NVARCHAR(36), inserted.task_id) AS task_id, inserted.idempotency_key, inserted.project_id, \
                inserted.target_backend, inserted.target_instance, inserted.projection_kind, inserted.resource_name, \
                inserted.operation, inserted.source_row_key, inserted.target_options, inserted.source_payload, \
                inserted.source_checksum, inserted.status, inserted.retry_count, inserted.last_error, \
                CONVERT(NVARCHAR(33), inserted.created_at, 127) AS created_at, \
                CONVERT(NVARCHAR(33), inserted.updated_at, 127) AS updated_at, \
                CONVERT(NVARCHAR(33), inserted.next_retry_at, 127) AS next_retry_at, \
                CONVERT(NVARCHAR(33), inserted.completed_at, 127) AS completed_at \
             WHERE task_id IN (SELECT task_id FROM candidates); \
             COMMIT TRANSACTION;"
        );
        let mut params: Vec<SqlParam> = vec![
            SqlParam::Int(i64::from(filter.max_retries)),
            SqlParam::Int(filter.batch_size),
        ];
        if let Some(project_id) = &filter.project_id {
            params.push(SqlParam::Str(project_id.clone()));
        }
        match (&filter.target_backend, &filter.target_instance) {
            (Some(b), Some(i)) => {
                params.push(SqlParam::Str(b.clone()));
                params.push(SqlParam::Str(i.clone()));
            }
            (Some(b), None) => params.push(SqlParam::Str(b.clone())),
            (None, Some(i)) => params.push(SqlParam::Str(i.clone())),
            (None, None) => {}
        }
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(row_to_projection_task(row)?);
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'COMPLETED', completed_at = SYSUTCDATETIME(), next_retry_at = NULL, updated_at = SYSUTCDATETIME() \
             WHERE task_id = CONVERT(UNIQUEIDENTIFIER, @P1)"
        );
        let params = [SqlParam::Str(task_id.to_string())];
        self.client()
            .execute_sql(&sql, &params)
            .await
            .map(|_| ())
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
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
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = @P1, retry_count = @P2, last_error = @P3, \
                 next_retry_at = NULL, updated_at = SYSUTCDATETIME() \
             WHERE task_id = CONVERT(UNIQUEIDENTIFIER, @P4)"
        );
        let params = [
            SqlParam::Str(new_status.as_str().to_string()),
            SqlParam::Int(i64::from(new_retry_count)),
            SqlParam::Str(error.to_string()),
            SqlParam::Str(task_id.to_string()),
        ];
        self.client()
            .execute_sql(&sql, &params)
            .await
            .map(|_| ())
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let (where_clause, params): (&str, Vec<SqlParam>) = match target_backend {
            Some(b) => (
                "AND target_backend = @P1",
                vec![SqlParam::Str(b.to_string())],
            ),
            None => ("", Vec::new()),
        };
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'PENDING', retry_count = 0, last_error = '', next_retry_at = NULL, updated_at = SYSUTCDATETIME() \
             WHERE status = 'DEAD_LETTER' {where_clause}"
        );
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(n as i64)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // DATEADD(SECOND, -@stale, now) is the T-SQL analogue of PG's
        // `NOW() - make_interval(...)`.
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'PENDING', \
                 last_error = 'stale in-progress reconciliation', \
                 updated_at = SYSUTCDATETIME() \
             WHERE status = 'IN_PROGRESS' \
               AND updated_at < DATEADD(SECOND, -@P1, SYSUTCDATETIME())"
        );
        let params = [SqlParam::Int(stale_after.as_secs() as i64)];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(n as i64)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // DATEDIFF(SECOND, MIN(created_at), now) is the age of the oldest
        // PENDING/FAILED row in the group, matching PG's EXTRACT(EPOCH …).
        let sql = format!(
            "SELECT TOP (@P1) project_id, target_backend, target_instance, projection_kind, \
                    COUNT(*) AS pending, \
                    DATEDIFF(SECOND, MIN(created_at), SYSUTCDATETIME()) AS oldest_age_seconds \
             FROM {rel} \
             WHERE status IN ('PENDING','FAILED') \
             GROUP BY project_id, target_backend, target_instance, projection_kind"
        );
        let params = [SqlParam::Int(limit.max(1))];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(PendingTaskMetric {
                project_id: str_col(row, "project_id"),
                target_backend: str_col(row, "target_backend"),
                target_instance: str_col(row, "target_instance"),
                projection_kind: str_col(row, "projection_kind"),
                pending: i64::from(i32_col(row, "pending")),
                oldest_age_seconds: f64::from(i32_col(row, "oldest_age_seconds")),
            });
        }
        Ok(out)
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "SELECT TOP (@P1) source_table, target_backend, target_instance, \
                    COUNT(*) AS dead_count \
             FROM {rel} \
             WHERE status = 'DEAD_LETTER' \
             GROUP BY source_table, target_backend, target_instance"
        );
        let params = [SqlParam::Int(limit.max(1))];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(DeadLetterGroup {
                source_table: str_col(row, "source_table"),
                target_backend: str_col(row, "target_backend"),
                target_instance: str_col(row, "target_instance"),
                dead_count: i64::from(i32_col(row, "dead_count")),
            });
        }
        Ok(out)
    }

    async fn requeue_dead_letter_by_source(
        &self,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!(
            "UPDATE {rel} \
             SET status = 'PENDING', retry_count = 0, \
                 last_error = 'reconciliation repair', updated_at = SYSUTCDATETIME() \
             WHERE status = 'DEAD_LETTER' \
               AND source_table = @P1 \
               AND target_backend = @P2 \
               AND target_instance = @P3"
        );
        let params = [
            SqlParam::Str(source_table.to_string()),
            SqlParam::Str(target_backend.to_string()),
            SqlParam::Str(target_instance.to_string()),
        ];
        let n = self
            .client()
            .execute_sql(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        Ok(n as i64)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        // SQL Server has no array bind; pass the keys as a comma-joined CSV
        // and split server-side via STRING_SPLIT. Keys are idempotency
        // tokens (UUID-derived in practice) and are bound as a single
        // parameter — never interpolated — so this stays injection-safe.
        let csv = idempotency_keys.join(",");
        let sql = format!(
            "SELECT COUNT(*) AS n FROM {rel} \
             WHERE idempotency_key IN (SELECT value FROM STRING_SPLIT(@P1, ',')) \
               AND status NOT IN ('COMPLETED','DEAD_LETTER','FAILED')"
        );
        let params = [SqlParam::Str(csv)];
        let rows = self
            .client()
            .fetch_rows(&sql, &params)
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let n = rows.first().map(|r| i32_col(r, "n")).unwrap_or(0);
        Ok(i64::from(n))
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let rel = Self::safe_object_name(self.projection_relation_ref())
            .map_err(SystemStoreError::InvalidInput)?;
        let sql = format!("SELECT status, COUNT(*) AS n FROM {rel} GROUP BY status");
        let rows = self
            .client()
            .fetch_rows(&sql, &[])
            .await
            .map_err(|e| SystemStoreError::query("mssql", sql.clone(), e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in &rows {
            let status = str_col(row, "status");
            let n = i64::from(i32_col(row, "n"));
            apply_projection_summary_bucket(&mut s, "mssql", &status, n)?;
        }
        Ok(s)
    }
}
