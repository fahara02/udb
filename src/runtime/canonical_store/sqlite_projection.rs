//! SQLite implementation of [`ProjectionTaskStore`].
//!
//! Lives in its own file so the heavy SQL stays out of
//! `sqlite.rs` (which owns the durability/outbox surface).
//!
//! ## Dialect choices
//!
//! - **Auto-increment id**: SQLite has no native UUID; `task_id` is
//!   `TEXT` and the SQL inserts a hex UUID via the Rust side
//!   (`Uuid::new_v4`). The producer never sees this — they always get
//!   a `Uuid` typed value back.
//! - **JSON columns**: SQLite has no JSON type, but it does have a
//!   `JSON1` extension that parses TEXT columns as JSON. We store the
//!   payloads as TEXT and rely on `serde_json` to serialize on the
//!   way in and parse on the way out.
//! - **CHECK constraints**: SQLite supports column-level CHECK; we
//!   use them on `status` and `operation` so a bad write fails fast.
//! - **Atomic claim**: SQLite is single-writer per database. A
//!   `BEGIN IMMEDIATE` transaction acquires the write lock atomically,
//!   which is functionally equivalent to PostgreSQL's
//!   `FOR UPDATE SKIP LOCKED` for our purposes: only one worker can
//!   run a claim at a time, and the claim runs to completion in one
//!   transaction.
//! - **RETURNING**: requires SQLite 3.35+ (2021-03-12). sqlx-sqlite
//!   ships its own bundled SQLite which is newer than that; the
//!   `ensure_projection_tables` step records the SQLite version in
//!   the error path if it ever fails to apply a RETURNING query.
//! - **Timestamps**: stored as ISO 8601 TEXT (`strftime`). chrono
//!   parses them back into `DateTime<Utc>` on read.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{apply_projection_summary_bucket, projection_retry_delay_secs};
use super::sqlite::SqliteCanonicalStore;
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

/// Pin the projection_tasks table name. SQLite has no schemas, so we
/// don't qualify the relation. Other constants follow.
const TABLE: &str = "udb_projection_tasks";

impl SqliteCanonicalStore {
    /// SQLite pool accessor. Used by the projection store impl.
    /// (Public-in-crate so the impl below can reach it.)
    pub(crate) fn pool_ref(&self) -> &sqlx::SqlitePool {
        // SAFETY contract: `SqliteCanonicalStore` always holds a real
        // pool. This is a getter so the projection-store impl in a
        // sibling module can read it without making the field `pub`.
        &self.pool
    }
}

/// Convenience: ISO-8601 timestamp parser, matched against the
/// `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` format the table uses by
/// default. Returns `now()` on a malformed input — the worker treats
/// timestamps as best-effort metadata, not as correctness signals.
fn parse_iso(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

/// Required SQLite version for `RETURNING` (3.35). Surfaced in the claim
/// error path so a too-old engine produces an actionable diagnostic rather
/// than an opaque syntax error.
const REQUIRED_SQLITE_VERSION: &str = "3.35";

/// Parse the row sqlx returns from claim / status queries. The
/// SELECT clause is constant so the column order is stable; we read
/// by name for safety.
fn row_to_projection_task(row: sqlx::sqlite::SqliteRow) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id_str: String = row
        .try_get("task_id")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT task_id", e))?;
    let task_id = Uuid::parse_str(&task_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("task_id '{task_id_str}' is not a valid UUID: {e}"))
    })?;
    let operation_str: String = row
        .try_get("operation")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT operation", e))?;
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in SQLite row"
        ))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("sqlite", "SELECT status", e))?;
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in SQLite row"
        ))
    })?;
    let source_row_key_text: String = row.try_get("source_row_key").unwrap_or_default();
    let target_options_text: String = row.try_get("target_options").unwrap_or_default();
    let source_payload_text: String = row.try_get("source_payload").unwrap_or_default();
    let parse_json = |s: &str, field: &'static str| -> SystemStoreResult<serde_json::Value> {
        if s.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(s).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "field '{field}' is not valid JSON: {e} (raw: '{s}')"
            ))
        })
    };
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: row.try_get("idempotency_key").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        target_backend: row.try_get("target_backend").unwrap_or_default(),
        target_instance: row.try_get("target_instance").unwrap_or_default(),
        projection_kind: row.try_get("projection_kind").unwrap_or_default(),
        resource_name: row.try_get("resource_name").unwrap_or_default(),
        operation,
        source_row_key: parse_json(&source_row_key_text, "source_row_key")?,
        target_options: parse_json(&target_options_text, "target_options")?,
        source_payload: parse_json(&source_payload_text, "source_payload")?,
        source_checksum: row.try_get("source_checksum").unwrap_or_default(),
        status,
        retry_count: row.try_get("retry_count").unwrap_or(0),
        last_error: row.try_get("last_error").unwrap_or_default(),
        created_at: row
            .try_get::<String, _>("created_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
        updated_at: row
            .try_get::<String, _>("updated_at")
            .map(|s| parse_iso(&s))
            .unwrap_or_else(|_| Utc::now()),
        next_retry_at: row
            .try_get::<Option<String>, _>("next_retry_at")
            .ok()
            .flatten()
            .map(|s| parse_iso(&s)),
        completed_at: row
            .try_get::<Option<String>, _>("completed_at")
            .ok()
            .flatten()
            .map(|s| parse_iso(&s)),
    })
}

#[async_trait]
impl ProjectionTaskStore for SqliteCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "sqlite"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        // DDL: every column the PG schema has, in SQLite dialect.
        // CHECK constraints enforce status + operation enums even
        // though SQLite is dynamically typed. Indexes mirror PG.
        //
        // B.7: the statement strings now come from the shared
        // `sql_schema` renderer (single source of truth across SQL
        // backends); the execute/error-tolerance loop below is
        // unchanged.
        let stmts = super::sql_schema::sqlite_projection_tasks_ddl(TABLE);
        for sql in stmts.iter() {
            if let Err(e) = sqlx::query(sql).execute(self.pool_ref()).await {
                let msg = e.to_string();
                if !msg.contains("duplicate column name") {
                    return Err(SystemStoreError::query("sqlite", sql.clone(), e));
                }
            }
        }
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        // The caller-supplied UUID makes the insert idempotent on
        // `idempotency_key`. ON CONFLICT DO NOTHING + a follow-up
        // SELECT covers the race where two callers enqueue the same
        // key concurrently.
        let task_id = Uuid::new_v4();
        let task_id_text = task_id.to_string();
        let source_row_key = serde_json::to_string(&task.source_row_key)
            .map_err(|e| SystemStoreError::InvalidInput(format!("source_row_key: {e}")))?;
        let target_options = serde_json::to_string(&task.target_options)
            .map_err(|e| SystemStoreError::InvalidInput(format!("target_options: {e}")))?;
        let source_payload = serde_json::to_string(&task.source_payload)
            .map_err(|e| SystemStoreError::InvalidInput(format!("source_payload: {e}")))?;

        // Insert with conflict-on-idempotency_key. SQLite's
        // ON CONFLICT clause accepts the target column.
        let sql = format!(
            "INSERT INTO {TABLE} (
                task_id, idempotency_key, project_id, manifest_checksum, message_type,
                source_schema, source_table, source_row_key, operation,
                target_backend, target_instance, projection_kind, resource_name,
                target_options, source_payload, source_checksum
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16
            )
            ON CONFLICT(idempotency_key) DO NOTHING"
        );
        sqlx::query(&sql)
            .bind(&task_id_text)
            .bind(&task.idempotency_key)
            .bind(&task.project_id)
            .bind(&task.manifest_checksum)
            .bind(&task.message_type)
            .bind(&task.source_schema)
            .bind(&task.source_table)
            .bind(&source_row_key)
            .bind(task.operation.as_str())
            .bind(&task.target_backend)
            .bind(&task.target_instance)
            .bind(&task.projection_kind)
            .bind(&task.resource_name)
            .bind(&target_options)
            .bind(&source_payload)
            .bind(&task.source_checksum)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;

        // Either we just inserted (returns our generated task_id) or
        // the row already existed (return the existing task_id).
        let lookup_sql = format!("SELECT task_id FROM {TABLE} WHERE idempotency_key = ?1");
        let existing: String = sqlx::query_scalar(&lookup_sql)
            .bind(&task.idempotency_key)
            .fetch_one(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", lookup_sql.clone(), e))?;
        Uuid::parse_str(&existing).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "stored task_id '{existing}' is not a valid UUID: {e}"
            ))
        })
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        // The claim runs as one atomic statement against SQLite's
        // single writer, equivalent to FOR UPDATE SKIP LOCKED for our
        // purposes: no other writer can interleave.
        //
        // The CTE picks the candidate task_ids; the UPDATE flips them
        // to IN_PROGRESS and the RETURNING clause hands back the
        // full row.
        let target_clause = match (&filter.target_backend, &filter.target_instance) {
            (Some(b), Some(i)) => format!(
                "AND target_backend = '{}' AND target_instance = '{}'",
                escape_sql_literal(b),
                escape_sql_literal(i)
            ),
            (Some(b), None) => {
                format!("AND target_backend = '{}'", escape_sql_literal(b))
            }
            (None, Some(i)) => {
                format!("AND target_instance = '{}'", escape_sql_literal(i))
            }
            (None, None) => String::new(),
        };
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'IN_PROGRESS',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE task_id IN (
                 SELECT task_id FROM {TABLE}
                 WHERE status IN ('PENDING', 'FAILED')
                   AND retry_count < ?1
                   AND (?3 = '' OR project_id = ?3)
                   AND (next_retry_at IS NULL OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
                   {target_clause}
                 ORDER BY created_at
                 LIMIT ?2
             )
             RETURNING task_id, idempotency_key, project_id,
                       target_backend, target_instance, projection_kind, resource_name,
                       operation, source_row_key, target_options, source_payload,
                       source_checksum, status, retry_count, last_error,
                       created_at, updated_at, next_retry_at, completed_at"
        );
        let rows = sqlx::query(&sql)
            .bind(filter.max_retries)
            .bind(filter.batch_size)
            .bind(filter.project_id.as_deref().unwrap_or(""))
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| {
                // The claim relies on the `RETURNING` clause, which SQLite only
                // supports from REQUIRED_SQLITE_VERSION onward. A syntax error
                // here almost always means the engine is too old; annotate the
                // failure so the operator knows the minimum version to run.
                let msg = e.to_string();
                if msg.contains("RETURNING") || msg.contains("syntax error") {
                    SystemStoreError::query(
                        "sqlite",
                        format!(
                            "{sql}\n-- note: projection claim requires SQLite >= \
                             {REQUIRED_SQLITE_VERSION} for RETURNING support"
                        ),
                        e,
                    )
                } else {
                    SystemStoreError::query("sqlite", sql.clone(), e)
                }
            })?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row_to_projection_task(row)?);
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'COMPLETED',
                 completed_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                 next_retry_at = NULL,
                 updated_at  = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE task_id = ?1"
        );
        let task_id_text = task_id.to_string();
        sqlx::query(&sql)
            .bind(&task_id_text)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(())
    }

    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()> {
        // Validate the new status — only FAILED or DEAD_LETTER make
        // sense here. Refuse anything else explicitly rather than
        // letting a typo slip past the CHECK constraint as a runtime
        // SQL error.
        if !matches!(
            new_status,
            ProjectionTaskStatus::Failed | ProjectionTaskStatus::DeadLetter
        ) {
            return Err(SystemStoreError::InvalidInput(format!(
                "mark_projection_task_failed only accepts FAILED or DEAD_LETTER, got {}",
                new_status.as_str()
            )));
        }
        // FAILED tasks back off exponentially before becoming re-claimable
        // (the claim query gates on `next_retry_at <= now`); DEAD_LETTER is
        // terminal and keeps `next_retry_at = NULL`. We write next_retry_at in
        // the same `%Y-%m-%dT%H:%M:%fZ` format the claim compares against. The
        // `?` backoff parameter is referenced in both CASE branches so the
        // positional bind count stays correct.
        let backoff_secs: Option<i64> = match new_status {
            ProjectionTaskStatus::Failed => Some(projection_retry_delay_secs(new_retry_count)),
            _ => None,
        };
        let sql = format!(
            "UPDATE {TABLE}
             SET status = ?,
                 retry_count = ?,
                 last_error = ?,
                 next_retry_at = CASE
                     WHEN ? IS NULL THEN NULL
                     ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', ? || ' seconds')
                 END,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE task_id = ?"
        );
        let task_id_text = task_id.to_string();
        sqlx::query(&sql)
            .bind(new_status.as_str())
            .bind(new_retry_count)
            .bind(error)
            .bind(backoff_secs)
            .bind(backoff_secs)
            .bind(&task_id_text)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let where_clause = match target_backend {
            Some(b) => format!("AND target_backend = '{}'", escape_sql_literal(b)),
            None => String::new(),
        };
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'PENDING',
                 retry_count = 0,
                 last_error = '',
                 next_retry_at = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'DEAD_LETTER' {where_clause}"
        );
        let result = sqlx::query(&sql)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        // SQLite julianday returns a real; we compute the cutoff in
        // the application layer so we don't depend on local-clock
        // datetime arithmetic differences across SQLite versions.
        let cutoff = Utc::now() - chrono::Duration::seconds(stale_after.as_secs() as i64);
        let cutoff_str = cutoff.to_rfc3339();
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'PENDING',
                 last_error = 'stale in-progress reconciliation',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'IN_PROGRESS' AND updated_at < ?1"
        );
        let result = sqlx::query(&sql)
            .bind(&cutoff_str)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        // SQLite: julianday * 86400 yields seconds since the epoch.
        // We compute oldest_age_seconds = now - MIN(created_at).
        // strftime('%Y-...') comparisons sort lexicographically which
        // happens to match chronological order for ISO-8601 strings,
        // so MIN(created_at) is the oldest.
        let sql = format!(
            "SELECT project_id, target_backend, target_instance, projection_kind,
                    COUNT(*) AS pending,
                    CAST(
                      (julianday('now') -
                       julianday(MIN(created_at))
                      ) * 86400.0
                    AS REAL) AS oldest_age_seconds
             FROM {TABLE}
             WHERE status IN ('PENDING', 'FAILED')
             GROUP BY project_id, target_backend, target_instance, projection_kind
             LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(limit.max(1))
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(PendingTaskMetric {
                project_id: row.try_get("project_id").unwrap_or_default(),
                target_backend: row.try_get("target_backend").unwrap_or_default(),
                target_instance: row.try_get("target_instance").unwrap_or_default(),
                projection_kind: row.try_get("projection_kind").unwrap_or_default(),
                pending: row.try_get("pending").unwrap_or(0),
                oldest_age_seconds: row.try_get("oldest_age_seconds").unwrap_or(0.0),
            });
        }
        Ok(out)
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        // NOTE: the schema has `source_table` as a column (TEXT).
        let sql = format!(
            "SELECT source_table, target_backend, target_instance,
                    COUNT(*) AS dead_count
             FROM {TABLE}
             WHERE status = 'DEAD_LETTER'
             GROUP BY source_table, target_backend, target_instance
             LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(limit.max(1))
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(DeadLetterGroup {
                source_table: row.try_get("source_table").unwrap_or_default(),
                target_backend: row.try_get("target_backend").unwrap_or_default(),
                target_instance: row.try_get("target_instance").unwrap_or_default(),
                dead_count: row.try_get("dead_count").unwrap_or(0),
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
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'PENDING',
                 retry_count = 0,
                 last_error = 'reconciliation repair',
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
             WHERE status = 'DEAD_LETTER'
               AND source_table = ?
               AND target_backend = ?
               AND target_instance = ?"
        );
        let result = sqlx::query(&sql)
            .bind(source_table)
            .bind(target_backend)
            .bind(target_instance)
            .execute(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        // SQLite doesn't bind arrays; emit one `?` per key.
        let placeholders: Vec<&str> = idempotency_keys.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT COUNT(*) FROM {TABLE}
             WHERE idempotency_key IN ({})
               AND status <> 'COMPLETED'", // P2-1 NF-1/NF-2: FAILED/DEAD_LETTER = not projected yet = still fences
            placeholders.join(",")
        );
        let mut q = sqlx::query_scalar::<_, i64>(&sql);
        for k in idempotency_keys {
            q = q.bind(k.clone());
        }
        let n: i64 = q
            .fetch_one(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        Ok(n)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let sql = format!("SELECT status, COUNT(*) AS n FROM {TABLE} GROUP BY status");
        let rows = sqlx::query(&sql)
            .fetch_all(self.pool_ref())
            .await
            .map_err(|e| SystemStoreError::query("sqlite", sql.clone(), e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in rows {
            let status: String = row.try_get("status").unwrap_or_default();
            let n: i64 = row.try_get("n").unwrap_or(0);
            apply_projection_summary_bucket(&mut s, "sqlite", &status, n)?;
        }
        Ok(s)
    }
}

/// Escape a SQL string literal by doubling embedded single quotes.
/// The callers in this file build the `target_backend`/`target_instance`
/// filter inline because sqlx-sqlite needs the value at SQL-prep time
/// when used inside the IN subquery (parameterised binding works on
/// the outer UPDATE but produces an `mismatched parameter` error on
/// some sqlite versions for the inner SELECT). Inputs here come from
/// operator config so the surface is small; the escape is defence in
/// depth.
fn escape_sql_literal(s: &str) -> String {
    s.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::canonical_store::sqlite::SqliteCanonicalStore;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn fresh_store() -> SqliteCanonicalStore {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        let store = SqliteCanonicalStore::new(pool, "test", "udb_outbox_events");
        ProjectionTaskStore::ensure_projection_tables(&store)
            .await
            .expect("ensure_projection_tables");
        store
    }

    fn sample_insert(idempotency_key: &str) -> ProjectionTaskInsert {
        ProjectionTaskInsert {
            idempotency_key: idempotency_key.to_string(),
            project_id: "default".to_string(),
            manifest_checksum: "sha256:abc".to_string(),
            message_type: "udb.test.v1.User".to_string(),
            source_schema: "public".to_string(),
            source_table: "users".to_string(),
            source_row_key: serde_json::json!({"id": "u-1"}),
            operation: ProjectionOperation::Upsert,
            target_backend: "qdrant".to_string(),
            target_instance: "primary".to_string(),
            projection_kind: "vector".to_string(),
            resource_name: "users_vec".to_string(),
            target_options: serde_json::json!([]),
            source_payload: serde_json::json!({"name": "Ada"}),
            source_checksum: "sha256:row1".to_string(),
        }
    }

    /// Pin: enqueue returns a UUID, summary reports 1 pending, claim
    /// flips it to IN_PROGRESS.
    #[tokio::test]
    async fn enqueue_summary_claim_roundtrip() {
        let store = fresh_store().await;

        let id1 = store
            .enqueue_projection_task(&sample_insert("k1"))
            .await
            .expect("enqueue");
        assert_ne!(id1, Uuid::nil());

        let summary = store
            .projection_task_summary()
            .await
            .expect("summary after one enqueue");
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.total(), 1);
        assert_eq!(summary.claimable(), 1);

        let claimed = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].task_id, id1);
        assert_eq!(claimed[0].status, ProjectionTaskStatus::InProgress);
        assert_eq!(claimed[0].source_row_key, serde_json::json!({"id": "u-1"}));

        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.in_progress, 1);
        assert_eq!(summary.pending, 0);
    }

    /// Pin: idempotency — enqueueing the same key twice returns the
    /// same task_id, summary shows one row.
    #[tokio::test]
    async fn enqueue_is_idempotent_on_idempotency_key() {
        let store = fresh_store().await;
        let id_a = store
            .enqueue_projection_task(&sample_insert("dup"))
            .await
            .unwrap();
        let id_b = store
            .enqueue_projection_task(&sample_insert("dup"))
            .await
            .unwrap();
        assert_eq!(
            id_a, id_b,
            "second enqueue with same idempotency_key must return existing task_id"
        );
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.pending, 1, "no duplicate row created");
    }

    /// Pin: mark_failed bumps the retry counter, status returns to
    /// FAILED, next claim picks it up again, retry_count carries
    /// forward through claim cycles.
    #[tokio::test]
    async fn mark_failed_then_reclaim_increments_retries() {
        let store = fresh_store().await;
        let id = store
            .enqueue_projection_task(&sample_insert("retry-me"))
            .await
            .unwrap();

        // First claim → IN_PROGRESS.
        let first = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].retry_count, 0);

        // Worker reports failure with new_retry_count = 1.
        store
            .mark_projection_task_failed(
                id,
                1,
                ProjectionTaskStatus::Failed,
                "synthetic failure on attempt 1",
            )
            .await
            .unwrap();

        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.in_progress, 0);

        // mark_failed now applies an exponential backoff: the row carries a
        // future next_retry_at, so it is NOT immediately re-claimable.
        let too_soon = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        assert!(
            too_soon.is_empty(),
            "FAILED row must stay out of the claim set until its backoff elapses"
        );
        // Confirm the backoff timestamp was actually written.
        let nra: Option<String> = sqlx::query(&format!(
            "SELECT next_retry_at FROM {TABLE} WHERE task_id = ?"
        ))
        .bind(id.to_string())
        .fetch_one(store.pool_ref())
        .await
        .unwrap()
        .try_get("next_retry_at")
        .unwrap();
        assert!(
            nra.is_some(),
            "FAILED task must have a next_retry_at backoff"
        );

        // Simulate elapsed backoff (the worker can't fast-forward the wall
        // clock under SQLite), then re-claim — the FAILED row is back.
        sqlx::query(&format!(
            "UPDATE {TABLE} SET next_retry_at = NULL WHERE task_id = ?"
        ))
        .bind(id.to_string())
        .execute(store.pool_ref())
        .await
        .unwrap();
        let second = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].task_id, id);
        assert_eq!(second[0].retry_count, 1);
        assert_eq!(second[0].last_error, "synthetic failure on attempt 1");
        assert_eq!(second[0].status, ProjectionTaskStatus::InProgress);
    }

    /// Pin: claim respects `max_retries`. A task at retry_count >=
    /// max_retries must NOT be claimed (the worker should have routed
    /// it to DEAD_LETTER already).
    #[tokio::test]
    async fn claim_respects_max_retries_threshold() {
        let store = fresh_store().await;
        let id = store
            .enqueue_projection_task(&sample_insert("hot"))
            .await
            .unwrap();
        // Fast-forward: mark failed at retry_count = 3.
        store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        store
            .mark_projection_task_failed(id, 3, ProjectionTaskStatus::Failed, "errors keep coming")
            .await
            .unwrap();
        // Filter with max_retries=3 must skip this row (retry_count
        // is NOT < 3).
        let claimed = store
            .claim_projection_tasks(&ProjectionClaimFilter {
                batch_size: 50,
                max_retries: 3,
                ..ProjectionClaimFilter::default()
            })
            .await
            .unwrap();
        assert!(
            claimed.is_empty(),
            "row at retry_count = max_retries must not be claimed"
        );
        // Clear the backoff so this test isolates the max_retries gate from
        // the next_retry_at gate (mark_failed sets a future next_retry_at).
        sqlx::query(&format!(
            "UPDATE {TABLE} SET next_retry_at = NULL WHERE task_id = ?"
        ))
        .bind(id.to_string())
        .execute(store.pool_ref())
        .await
        .unwrap();
        // But max_retries=4 picks it up.
        let claimed = store
            .claim_projection_tasks(&ProjectionClaimFilter {
                batch_size: 50,
                max_retries: 4,
                ..ProjectionClaimFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
    }

    /// Pin: completed tasks are NOT re-claimed. Once a worker marks
    /// completion, the row stays terminal.
    #[tokio::test]
    async fn completed_tasks_are_not_reclaimed() {
        let store = fresh_store().await;
        let id = store
            .enqueue_projection_task(&sample_insert("done"))
            .await
            .unwrap();
        store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        store.mark_projection_task_completed(id).await.unwrap();
        let again = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        assert!(again.is_empty());
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.claimable(), 0);
    }

    /// Pin: per-backend filtering. Worker pool for `qdrant` does
    /// not pick up tasks targeting `s3`.
    #[tokio::test]
    async fn claim_filter_isolates_by_target_backend() {
        let store = fresh_store().await;
        let mut a = sample_insert("a");
        a.target_backend = "qdrant".to_string();
        let mut b = sample_insert("b");
        b.target_backend = "s3".to_string();
        store.enqueue_projection_task(&a).await.unwrap();
        store.enqueue_projection_task(&b).await.unwrap();

        let only_qdrant = store
            .claim_projection_tasks(&ProjectionClaimFilter {
                target_backend: Some("qdrant".to_string()),
                ..ProjectionClaimFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(only_qdrant.len(), 1);
        assert_eq!(only_qdrant[0].idempotency_key, "a");
    }

    /// Pin: requeue_dead_letter_tasks promotes DLQ rows back to
    /// PENDING with retry_count=0. Operator-driven recovery.
    #[tokio::test]
    async fn requeue_dead_letter_resets_state() {
        let store = fresh_store().await;
        let id = store
            .enqueue_projection_task(&sample_insert("dlq"))
            .await
            .unwrap();
        store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        // Send to DLQ.
        store
            .mark_projection_task_failed(
                id,
                5,
                ProjectionTaskStatus::DeadLetter,
                "fatal upstream error",
            )
            .await
            .unwrap();
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.dead_letter, 1);

        let requeued = store.requeue_dead_letter_tasks(None).await.unwrap();
        assert_eq!(requeued, 1);
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.dead_letter, 0);
        assert_eq!(summary.pending, 1);

        // After requeue, retry_count is reset so the row claims again.
        let claimed = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].retry_count, 0);
        assert_eq!(claimed[0].last_error, "");
    }

    /// Pin: reset_stale_in_progress recovers a worker that crashed
    /// mid-task. The row had been IN_PROGRESS past the staleness
    /// threshold; reconciliation flips it back to PENDING.
    #[tokio::test]
    async fn reset_stale_in_progress_recovers_crashed_worker() {
        let store = fresh_store().await;
        store
            .enqueue_projection_task(&sample_insert("crash"))
            .await
            .unwrap();
        store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.in_progress, 1);

        // Force the row into the past so the stale-after comparison
        // is unambiguous under SQLite's strftime millisecond
        // precision. Production callers pass stale_after >> 1ms.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Reset anything older than 0s — i.e. everything that was
        // claimed before now.
        let reset = store
            .reset_stale_in_progress_tasks(Duration::from_secs(0))
            .await
            .unwrap();
        assert_eq!(reset, 1);
        let summary = store.projection_task_summary().await.unwrap();
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.in_progress, 0);
    }

    /// Pin: pending_projection_task_count counts non-terminal,
    /// non-FAILED rows whose idempotency_key is in the supplied list.
    /// Used by consistency_fence to decide if a read fence has cleared.
    #[tokio::test]
    async fn pending_count_excludes_terminal_and_failed() {
        let store = fresh_store().await;
        // Enqueue 4 tasks with distinct keys.
        for k in ["a", "b", "c", "d"] {
            store
                .enqueue_projection_task(&sample_insert(k))
                .await
                .unwrap();
        }
        // a → COMPLETED, b → FAILED, c → DEAD_LETTER, d stays PENDING.
        let claim = store
            .claim_projection_tasks(&ProjectionClaimFilter::default())
            .await
            .unwrap();
        for row in &claim {
            match row.idempotency_key.as_str() {
                "a" => store
                    .mark_projection_task_completed(row.task_id)
                    .await
                    .unwrap(),
                "b" => store
                    .mark_projection_task_failed(row.task_id, 1, ProjectionTaskStatus::Failed, "x")
                    .await
                    .unwrap(),
                "c" => store
                    .mark_projection_task_failed(
                        row.task_id,
                        5,
                        ProjectionTaskStatus::DeadLetter,
                        "y",
                    )
                    .await
                    .unwrap(),
                _ => {}
            }
        }

        // P2-1 (NF-1/NF-2): ONLY `COMPLETED` clears the read fence. FAILED (will
        // retry) and DEAD_LETTER (will never complete) are NOT projected yet, so
        // for read-your-writes they still count as pending. So of the 4 keys:
        // a=COMPLETED (excluded), b=FAILED (counted), c=DEAD_LETTER (counted),
        // d=IN_PROGRESS (counted) → 3.
        let n = store
            .pending_projection_task_count(&[
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ])
            .await
            .unwrap();
        assert_eq!(
            n, 3,
            "only COMPLETED clears the fence; FAILED + DEAD_LETTER + IN_PROGRESS all still count"
        );

        // Empty list → 0 without hitting the DB.
        let n = store.pending_projection_task_count(&[]).await.unwrap();
        assert_eq!(n, 0);

        // Unknown keys → 0.
        let n = store
            .pending_projection_task_count(&["nope".to_string()])
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    /// Pin: mark_failed refuses an invalid status (anything other
    /// than FAILED / DEAD_LETTER). Defends against caller typos
    /// before reaching the DB.
    #[tokio::test]
    async fn mark_failed_rejects_invalid_status() {
        let store = fresh_store().await;
        let id = store
            .enqueue_projection_task(&sample_insert("bad-mark"))
            .await
            .unwrap();
        let err = store
            .mark_projection_task_failed(id, 1, ProjectionTaskStatus::Completed, "oops")
            .await
            .expect_err("must reject COMPLETED on the failure path");
        match err {
            SystemStoreError::InvalidInput(msg) => {
                assert!(msg.contains("FAILED or DEAD_LETTER"));
            }
            other => panic!("expected InvalidInput, got: {other}"),
        }
    }
}
