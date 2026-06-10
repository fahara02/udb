//! MySQL implementation of [`ProjectionTaskStore`].
//!
//! ## Dialect choices
//!
//! - **No RETURNING**. MySQL doesn't support RETURNING on UPDATE
//!   (MariaDB has it; vanilla MySQL doesn't). We work around by
//!   selecting the claim candidate IDs first inside a transaction,
//!   then UPDATE-ing them by ID, then SELECT-ing the full rows.
//!   The transaction provides atomicity.
//! - **`FOR UPDATE SKIP LOCKED`** — requires MySQL 8.0.1+ (2017-09).
//!   Older MySQL falls back to plain `FOR UPDATE` which serialises
//!   claims (correctness preserved, throughput reduced).
//! - **Auto-increment for `task_id`**: MySQL has no native UUID type
//!   short of CHAR(36). We generate the UUID Rust-side and pass it
//!   in; the column is `CHAR(36) NOT NULL` with the PRIMARY KEY.
//! - **JSON columns**: native `JSON` column type (MySQL 5.7+).
//! - **Timestamps**: `TIMESTAMP(6)` for microsecond precision with
//!   `DEFAULT CURRENT_TIMESTAMP(6)`.
//! - **`ON DUPLICATE KEY UPDATE id=id`** idempotency idiom for
//!   enqueue — same effect as PG's `ON CONFLICT DO NOTHING`.
//! - **CHECK constraints**: supported since MySQL 8.0.16; enforced
//!   for `status` and `operation`.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::{apply_projection_summary_bucket, projection_retry_delay_secs};
use super::mysql::MysqlCanonicalStore;
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

const TABLE: &str = "udb_projection_tasks";

impl MysqlCanonicalStore {
    /// Pool getter for the projection impl.
    pub(crate) fn mysql_pool(&self) -> &sqlx::MySqlPool {
        &self.pool
    }
}

fn row_to_projection_task(row: sqlx::mysql::MySqlRow) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id_str: String = row
        .try_get("task_id")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT task_id", e))?;
    let task_id = Uuid::parse_str(&task_id_str).map_err(|e| {
        SystemStoreError::InvalidInput(format!("task_id '{task_id_str}' is not a valid UUID: {e}"))
    })?;
    let operation_str: String = row
        .try_get("operation")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT operation", e))?;
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in MySQL row"
        ))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("mysql", "SELECT status", e))?;
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in MySQL row"
        ))
    })?;

    let source_row_key: serde_json::Value = row
        .try_get("source_row_key")
        .unwrap_or(serde_json::Value::Null);
    let target_options: serde_json::Value = row
        .try_get("target_options")
        .unwrap_or(serde_json::Value::Null);
    let source_payload: serde_json::Value = row
        .try_get("source_payload")
        .unwrap_or(serde_json::Value::Null);

    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: row.try_get("idempotency_key").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        target_backend: row.try_get("target_backend").unwrap_or_default(),
        target_instance: row.try_get("target_instance").unwrap_or_default(),
        projection_kind: row.try_get("projection_kind").unwrap_or_default(),
        resource_name: row.try_get("resource_name").unwrap_or_default(),
        operation,
        source_row_key,
        target_options,
        source_payload,
        source_checksum: row.try_get("source_checksum").unwrap_or_default(),
        status,
        retry_count: row.try_get("retry_count").unwrap_or(0),
        last_error: row.try_get("last_error").unwrap_or_default(),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .unwrap_or_else(|_| Utc::now()),
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .unwrap_or_else(|_| Utc::now()),
        next_retry_at: row
            .try_get::<Option<DateTime<Utc>>, _>("next_retry_at")
            .ok()
            .flatten(),
        completed_at: row
            .try_get::<Option<DateTime<Utc>>, _>("completed_at")
            .ok()
            .flatten(),
    })
}

#[async_trait]
impl ProjectionTaskStore for MysqlCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "mysql"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        // DDL applied in three separate statements so each is
        // independently idempotent. `CREATE TABLE IF NOT EXISTS` +
        // `CREATE INDEX` (no IF NOT EXISTS in older MySQL — wrap in
        // a session var trick below).
        //
        // B.7: the statement strings now come from the shared
        // `sql_schema` renderer (single source of truth across SQL
        // backends); the execute/error-tolerance logic below is
        // unchanged. `CREATE INDEX IF NOT EXISTS` was added in MySQL
        // 8.0.29, so for broader compatibility we attempt each create
        // and tolerate the "already exists" / "Duplicate key name"
        // errors below.
        let super::sql_schema::MysqlProjectionTasksDdl {
            create_table,
            create_idx_status,
            create_idx_project_status,
            create_idx_backend,
            add_next_retry,
            create_idx_retry,
        } = super::sql_schema::mysql_projection_tasks_ddl(TABLE);
        sqlx::query(&create_table)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", create_table.clone(), e))?;
        if let Err(e) = sqlx::query(&add_next_retry)
            .execute(self.mysql_pool())
            .await
        {
            let msg = e.to_string();
            if !msg.contains("Duplicate column name") && !msg.contains("already exists") {
                return Err(SystemStoreError::query("mysql", add_next_retry, e));
            }
        }
        // Index creation is best-effort: on a fresh DB it succeeds;
        // on a re-run it errors as "Duplicate key name" which we
        // treat as success.
        for sql in [
            &create_idx_status,
            &create_idx_project_status,
            &create_idx_backend,
            &create_idx_retry,
        ] {
            if let Err(e) = sqlx::query(sql).execute(self.mysql_pool()).await {
                let msg = e.to_string();
                if !msg.contains("Duplicate key name") && !msg.contains("already exists") {
                    return Err(SystemStoreError::query("mysql", sql.clone(), e));
                }
            }
        }
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        // We generate the UUID Rust-side. If the idempotency_key is
        // already present, ON DUPLICATE KEY UPDATE prevents an INSERT
        // collision — the existing row is unchanged and the SELECT
        // afterwards returns its task_id. If a fresh row is inserted,
        // the same SELECT returns the new task_id we just generated.
        let task_id = Uuid::new_v4();
        let task_id_str = task_id.to_string();
        let source_row_key = serde_json::to_string(&task.source_row_key)
            .map_err(|e| SystemStoreError::InvalidInput(format!("source_row_key: {e}")))?;
        let target_options = serde_json::to_string(&task.target_options)
            .map_err(|e| SystemStoreError::InvalidInput(format!("target_options: {e}")))?;
        let source_payload = serde_json::to_string(&task.source_payload)
            .map_err(|e| SystemStoreError::InvalidInput(format!("source_payload: {e}")))?;

        // `ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key`
        // is the canonical MySQL no-op upsert idiom — it consumes the
        // duplicate-key error without changing the row.
        let insert_sql = format!(
            "INSERT INTO {TABLE} (
                task_id, idempotency_key, project_id, manifest_checksum, message_type,
                source_schema, source_table, source_row_key, operation,
                target_backend, target_instance, projection_kind, resource_name,
                target_options, source_payload, source_checksum, last_error
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), ?, ''
            )
            ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key"
        );
        sqlx::query(&insert_sql)
            .bind(&task_id_str)
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
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", insert_sql.clone(), e))?;

        // SELECT to get the task_id (ours if INSERT happened,
        // existing if ON DUPLICATE KEY fired).
        let lookup_sql = format!("SELECT task_id FROM {TABLE} WHERE idempotency_key = ?");
        let stored: (String,) = sqlx::query_as(&lookup_sql)
            .bind(&task.idempotency_key)
            .fetch_one(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", lookup_sql.clone(), e))?;
        Uuid::parse_str(&stored.0).map_err(|e| {
            SystemStoreError::InvalidInput(format!(
                "stored task_id '{}' is not a valid UUID: {e}",
                stored.0
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
        // MySQL workflow without RETURNING:
        // 1. BEGIN.
        // 2. SELECT candidate task_ids FOR UPDATE SKIP LOCKED.
        // 3. UPDATE the selected rows to IN_PROGRESS.
        // 4. SELECT the full rows by those IDs.
        // 5. COMMIT.
        let mut tx = self
            .mysql_pool()
            .begin()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;

        // Step 2: pick candidate IDs with SKIP LOCKED. Bind the
        // backend/instance filter parameters in a fixed shape.
        let (target_clause, n_extra_binds): (&str, usize) =
            match (&filter.target_backend, &filter.target_instance) {
                (Some(_), Some(_)) => ("AND target_backend = ? AND target_instance = ?", 2),
                (Some(_), None) => ("AND target_backend = ?", 1),
                (None, Some(_)) => ("AND target_instance = ?", 1),
                (None, None) => ("", 0),
            };
        let select_sql = format!(
            "SELECT task_id FROM {TABLE}
             WHERE status IN ('PENDING', 'FAILED')
               AND retry_count < ?
               AND (? = '' OR project_id = ?)
               AND (next_retry_at IS NULL OR next_retry_at <= NOW(6))
               {target_clause}
             ORDER BY created_at
             LIMIT ?
             FOR UPDATE SKIP LOCKED"
        );
        let project_id = filter.project_id.as_deref().unwrap_or("");
        let mut q = sqlx::query_scalar::<_, String>(&select_sql)
            .bind(filter.max_retries)
            .bind(project_id)
            .bind(project_id);
        match (&filter.target_backend, &filter.target_instance) {
            (Some(b), Some(i)) => {
                q = q.bind(b.clone()).bind(i.clone());
            }
            (Some(b), None) => {
                q = q.bind(b.clone());
            }
            (None, Some(i)) => {
                q = q.bind(i.clone());
            }
            (None, None) => {}
        }
        let _ = n_extra_binds; // documentation only
        let candidate_ids: Vec<String> = q
            .bind(filter.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", select_sql.clone(), e))?;

        if candidate_ids.is_empty() {
            tx.commit()
                .await
                .map_err(|e| SystemStoreError::io("mysql", e))?;
            return Ok(Vec::new());
        }

        // Step 3: UPDATE those IDs to IN_PROGRESS. We build the
        // `IN (...)` placeholder list dynamically based on count.
        let placeholders: Vec<&str> = candidate_ids.iter().map(|_| "?").collect();
        let update_sql = format!(
            "UPDATE {TABLE}
             SET status = 'IN_PROGRESS', updated_at = NOW(6)
             WHERE task_id IN ({})",
            placeholders.join(",")
        );
        let mut upd = sqlx::query(&update_sql);
        for id in &candidate_ids {
            upd = upd.bind(id);
        }
        upd.execute(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", update_sql.clone(), e))?;

        // Step 4: SELECT the updated rows.
        let select_full_sql = format!(
            "SELECT task_id, idempotency_key, project_id,
                    target_backend, target_instance, projection_kind, resource_name,
                    operation, source_row_key, target_options, source_payload,
                    source_checksum, status, retry_count, last_error,
                    created_at, updated_at, next_retry_at, completed_at
             FROM {TABLE}
             WHERE task_id IN ({})
             ORDER BY created_at",
            placeholders.join(",")
        );
        let mut sel = sqlx::query(&select_full_sql);
        for id in &candidate_ids {
            sel = sel.bind(id);
        }
        let rows = sel
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| SystemStoreError::query("mysql", select_full_sql.clone(), e))?;

        tx.commit()
            .await
            .map_err(|e| SystemStoreError::io("mysql", e))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row_to_projection_task(row)?);
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'COMPLETED', completed_at = NOW(6), next_retry_at = NULL, updated_at = NOW(6)
             WHERE task_id = ?"
        );
        sqlx::query(&sql)
            .bind(task_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
        // FAILED tasks back off exponentially before becoming re-claimable
        // (claim gates on `next_retry_at <= NOW(6)`); DEAD_LETTER is terminal
        // and keeps `next_retry_at = NULL`. The `?` backoff parameter is
        // referenced in both CASE branches so the bind count stays correct.
        let backoff_secs: Option<i64> = match new_status {
            ProjectionTaskStatus::Failed => Some(projection_retry_delay_secs(new_retry_count)),
            _ => None,
        };
        let sql = format!(
            "UPDATE {TABLE}
             SET status = ?, retry_count = ?, last_error = ?,
                 next_retry_at = CASE
                     WHEN ? IS NULL THEN NULL
                     ELSE DATE_ADD(NOW(6), INTERVAL ? SECOND)
                 END,
                 updated_at = NOW(6)
             WHERE task_id = ?"
        );
        sqlx::query(&sql)
            .bind(new_status.as_str())
            .bind(new_retry_count)
            .bind(error)
            .bind(backoff_secs)
            .bind(backoff_secs)
            .bind(task_id.to_string())
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let (where_clause, bind_backend) = match target_backend {
            Some(b) => ("AND target_backend = ?", Some(b.to_string())),
            None => ("", None),
        };
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'PENDING', retry_count = 0, last_error = '', next_retry_at = NULL, updated_at = NOW(6)
             WHERE status = 'DEAD_LETTER' {where_clause}"
        );
        let mut q = sqlx::query(&sql);
        if let Some(b) = bind_backend {
            q = q.bind(b);
        }
        let result = q
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        // MySQL's TIMESTAMPDIFF is the canonical way to do this.
        // We bind the seconds parameter and let the server compare.
        let sql = format!(
            "UPDATE {TABLE}
             SET status = 'PENDING',
                 last_error = 'stale in-progress reconciliation',
                 updated_at = NOW(6)
             WHERE status = 'IN_PROGRESS'
               AND TIMESTAMPDIFF(SECOND, updated_at, NOW(6)) >= ?"
        );
        let result = sqlx::query(&sql)
            .bind(stale_after.as_secs() as i64)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        // MySQL: TIMESTAMPDIFF(MICROSECOND, oldest, NOW(6)) gives
        // microseconds; divide by 1_000_000 for seconds-as-double.
        let sql = format!(
            "SELECT project_id, target_backend, target_instance, projection_kind,
                    COUNT(*) AS pending,
                    TIMESTAMPDIFF(MICROSECOND, MIN(created_at), NOW(6)) / 1000000.0
                      AS oldest_age_seconds
             FROM {TABLE}
             WHERE status IN ('PENDING', 'FAILED')
             GROUP BY project_id, target_backend, target_instance, projection_kind
             LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(limit.max(1))
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
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
             SET status = 'PENDING', retry_count = 0,
                 last_error = 'reconciliation repair', updated_at = NOW(6)
             WHERE status = 'DEAD_LETTER'
               AND source_table = ?
               AND target_backend = ?
               AND target_instance = ?"
        );
        let result = sqlx::query(&sql)
            .bind(source_table)
            .bind(target_backend)
            .bind(target_instance)
            .execute(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        // MySQL: build the IN list with one ? per key.
        let placeholders: Vec<&str> = idempotency_keys.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT COUNT(*) FROM {TABLE}
             WHERE idempotency_key IN ({})
               AND status NOT IN ('COMPLETED','DEAD_LETTER','FAILED')",
            placeholders.join(",")
        );
        let mut q = sqlx::query_scalar::<_, i64>(&sql);
        for k in idempotency_keys {
            q = q.bind(k.clone());
        }
        let n: i64 = q
            .fetch_one(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        Ok(n)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let sql = format!("SELECT status, COUNT(*) AS n FROM {TABLE} GROUP BY status");
        let rows: Vec<(String, i64)> = sqlx::query_as(&sql)
            .fetch_all(self.mysql_pool())
            .await
            .map_err(|e| SystemStoreError::query("mysql", sql.clone(), e))?;
        let mut s = ProjectionTaskSummary::default();
        for (status, n) in rows {
            apply_projection_summary_bucket(&mut s, "mysql", &status, n)?;
        }
        Ok(s)
    }
}
