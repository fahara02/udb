//! PostgreSQL implementation of [`ProjectionTaskStore`].
//!
//! Wraps the existing PG schema + SQL the runtime already runs (in
//! `runtime/system.rs` for DDL and `runtime/projection/mod.rs` for
//! claim/mark). The point of putting this behind the trait is that
//! the call sites in NW1 step 3+ swap to
//! `Arc<dyn ProjectionTaskStore>` and PG behavior stays identical.
//!
//! ## Dialect choices (matching the existing schema bit-for-bit)
//!
//! - `task_id UUID PRIMARY KEY DEFAULT gen_random_uuid()`
//! - JSONB for `source_row_key`, `target_options`, `source_payload`
//! - `TIMESTAMPTZ DEFAULT NOW()` for timestamps
//! - `FOR UPDATE SKIP LOCKED` for atomic claim
//! - `RETURNING` for both insert and update
//! - `ON CONFLICT (idempotency_key) DO NOTHING` for idempotent enqueue
//! - CHECK constraints on `operation` and `status` (pinned by the
//!   schema, validated by [`ProjectionOperation::parse`] /
//!   [`ProjectionTaskStatus::parse`] on read)

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::dialect::apply_projection_summary_bucket;
use super::postgres::PostgresCanonicalStore;
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

/// Default relation when no system catalog override is supplied. Matches
/// `SystemCatalogConfig::projection_tasks_relation()` for the canonical
/// `udb_system.udb_projection_tasks` table.
const DEFAULT_REL: &str = r#""udb_system"."udb_projection_tasks""#;

fn projection_retry_delay_secs(retry_count: i32) -> i64 {
    let attempt = retry_count.max(1).min(12) as u32;
    (1_i64 << (attempt - 1)).min(3600)
}

impl PostgresCanonicalStore {
    /// PG pool getter for the projection impl below.
    pub(crate) fn pg_pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Override the projection_tasks relation. Defaults to
    /// `"udb_system"."udb_projection_tasks"`. Used by deployments that
    /// rename the system schema; matches the pattern in
    /// `SystemCatalogConfig::projection_tasks_relation`.
    pub fn with_projection_relation(mut self, relation: impl Into<String>) -> Self {
        self.projection_relation = Some(relation.into());
        self
    }

    fn projection_relation_ref(&self) -> &str {
        self.projection_relation.as_deref().unwrap_or(DEFAULT_REL)
    }
}

/// Build the row that comes back from claim / select queries. Same
/// column order as the SELECT below; reading by name keeps it safe.
fn row_to_projection_task(row: sqlx::postgres::PgRow) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id: Uuid = row
        .try_get("task_id")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT task_id", e))?;
    let operation_str: String = row
        .try_get("operation")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT operation", e))?;
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in PG row"
        ))
    })?;
    let status_str: String = row
        .try_get("status")
        .map_err(|e| SystemStoreError::query("postgres", "SELECT status", e))?;
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in PG row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: row.try_get("idempotency_key").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        target_backend: row.try_get("target_backend").unwrap_or_default(),
        target_instance: row.try_get("target_instance").unwrap_or_default(),
        projection_kind: row.try_get("projection_kind").unwrap_or_default(),
        resource_name: row.try_get("resource_name").unwrap_or_default(),
        operation,
        source_row_key: row
            .try_get("source_row_key")
            .unwrap_or(serde_json::Value::Null),
        target_options: row
            .try_get("target_options")
            .unwrap_or(serde_json::Value::Null),
        source_payload: row
            .try_get("source_payload")
            .unwrap_or(serde_json::Value::Null),
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
impl ProjectionTaskStore for PostgresCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "postgres"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        let rel = self.projection_relation_ref();
        // The DDL is the exact PG schema runtime/system.rs declares.
        // Repeating it here means the store is self-sufficient — a
        // call site that has the store can prepare its tables without
        // pulling in `runtime/system.rs::ensure_system_catalog`. PG's
        // `CREATE TABLE IF NOT EXISTS` makes the operation idempotent.
        //
        // The DDL is split into multiple statements because we want
        // each to be idempotent (CREATE INDEX IF NOT EXISTS is
        // per-statement).
        let stmts = [
            format!(
                r#"
                CREATE TABLE IF NOT EXISTS {rel} (
                    task_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    idempotency_key    TEXT NOT NULL UNIQUE,
                    project_id         TEXT NOT NULL DEFAULT '',
                    manifest_checksum  TEXT NOT NULL DEFAULT '',
                    message_type       TEXT NOT NULL DEFAULT '',
                    source_schema      TEXT NOT NULL DEFAULT '',
                    source_table       TEXT NOT NULL DEFAULT '',
                    source_row_key     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    operation          TEXT NOT NULL DEFAULT 'upsert'
                                       CHECK (operation IN ('upsert','delete')),
                    target_backend     TEXT NOT NULL DEFAULT '',
                    target_instance    TEXT NOT NULL DEFAULT '',
                    projection_kind    TEXT NOT NULL DEFAULT '',
                    resource_name      TEXT NOT NULL DEFAULT '',
                    target_options     JSONB NOT NULL DEFAULT '[]'::JSONB,
                    source_payload     JSONB NOT NULL DEFAULT '{{}}'::JSONB,
                    source_checksum    TEXT NOT NULL DEFAULT '',
                    status             TEXT NOT NULL DEFAULT 'PENDING'
                                       CHECK (status IN ('PENDING','IN_PROGRESS','COMPLETED','FAILED','DEAD_LETTER')),
                    retry_count        INTEGER NOT NULL DEFAULT 0,
                    last_error         TEXT NOT NULL DEFAULT '',
                    next_retry_at      TIMESTAMPTZ,
                    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    completed_at       TIMESTAMPTZ
                )
                "#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_status_created_at"
                     ON {rel} (status, created_at)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_project_status_created_at"
                     ON {rel} (project_id, status, created_at)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_backend_status"
                     ON {rel} (target_backend, target_instance, status)"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_pending"
                     ON {rel} (project_id, created_at, task_id)
                     WHERE status = 'PENDING'"#
            ),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_claim_failed"
                     ON {rel} (project_id, next_retry_at, created_at, task_id)
                     WHERE status = 'FAILED'"#
            ),
            format!(r#"ALTER TABLE {rel} ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ"#),
            format!(
                r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_next_retry"
                     ON {rel} (next_retry_at) WHERE status = 'FAILED' AND next_retry_at IS NOT NULL"#
            ),
        ];
        for sql in stmts.iter() {
            sqlx::query(sql)
                .execute(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        }
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let rel = self.projection_relation_ref();
        // CTE handles the race: INSERT returns task_id if it
        // succeeds; the UNION ALL fallback reads the existing
        // task_id if the unique constraint fired. The LIMIT 1
        // ensures exactly one row comes back.
        let sql = format!(
            r#"
            WITH inserted AS (
                INSERT INTO {rel} (
                    idempotency_key, project_id, manifest_checksum, message_type,
                    source_schema, source_table, source_row_key, operation,
                    target_backend, target_instance, projection_kind, resource_name,
                    target_options, source_payload, source_checksum
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7::jsonb, $8,
                    $9, $10, $11, $12, $13::jsonb, $14::jsonb, $15
                )
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING task_id
            )
            SELECT task_id FROM inserted
            UNION ALL
            SELECT task_id FROM {rel} WHERE idempotency_key = $1
            LIMIT 1
            "#
        );
        let task_id: Uuid = sqlx::query_scalar(&sql)
            .bind(&task.idempotency_key)
            .bind(&task.project_id)
            .bind(&task.manifest_checksum)
            .bind(&task.message_type)
            .bind(&task.source_schema)
            .bind(&task.source_table)
            .bind(task.source_row_key.to_string())
            .bind(task.operation.as_str())
            .bind(&task.target_backend)
            .bind(&task.target_instance)
            .bind(&task.projection_kind)
            .bind(&task.resource_name)
            .bind(task.target_options.to_string())
            .bind(task.source_payload.to_string())
            .bind(&task.source_checksum)
            .fetch_one(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(task_id)
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        let rel = self.projection_relation_ref();
        let mut next_param = 3;
        let project_filter = if filter.project_id.is_some() {
            let clause = format!("AND project_id = ${next_param}");
            next_param += 1;
            clause
        } else {
            String::new()
        };
        let target_filter = match (&filter.target_backend, &filter.target_instance) {
            (Some(_), Some(_)) => {
                let clause = format!(
                    "AND target_backend = ${next_param} AND target_instance = ${}",
                    next_param + 1
                );
                next_param += 2;
                clause
            }
            (Some(_), None) => {
                let clause = format!("AND target_backend = ${next_param}");
                next_param += 1;
                clause
            }
            (None, Some(_)) => {
                let clause = format!("AND target_instance = ${next_param}");
                next_param += 1;
                clause
            }
            (None, None) => String::new(),
        };
        let sql = format!(
            r#"
            WITH pending_candidates AS (
                SELECT task_id, created_at FROM {rel}
                WHERE status = 'PENDING'
                  AND retry_count < $1
                  {project_filter}
                  {target_filter}
                ORDER BY created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            failed_candidates AS (
                SELECT task_id, created_at FROM {rel}
                WHERE status = 'FAILED'
                  AND retry_count < $1
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                  {project_filter}
                  {target_filter}
                ORDER BY created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            candidates AS (
                SELECT task_id FROM (
                    SELECT task_id, created_at FROM pending_candidates
                    UNION ALL
                    SELECT task_id, created_at FROM failed_candidates
                ) c
                ORDER BY created_at
                LIMIT $2
            )
            UPDATE {rel}
            SET status = 'IN_PROGRESS', updated_at = NOW()
            WHERE task_id IN (SELECT task_id FROM candidates)
            RETURNING task_id, idempotency_key, project_id,
                      target_backend, target_instance, projection_kind, resource_name,
                      operation, source_row_key, target_options, source_payload,
                      source_checksum, status, retry_count, last_error,
                      created_at, updated_at, next_retry_at, completed_at
            "#
        );
        // Bind in the order matching the placeholders.
        let mut q = sqlx::query(&sql)
            .bind(filter.max_retries)
            .bind(filter.batch_size);
        if let Some(project_id) = &filter.project_id {
            q = q.bind(project_id);
        }
        match (&filter.target_backend, &filter.target_instance) {
            (Some(b), Some(i)) => {
                q = q.bind(b).bind(i);
            }
            (Some(b), None) => {
                q = q.bind(b);
            }
            (None, Some(i)) => {
                q = q.bind(i);
            }
            (None, None) => {}
        }
        let rows = q
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row_to_projection_task(row)?);
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'COMPLETED', completed_at = NOW(), next_retry_at = NULL, updated_at = NOW()
               WHERE task_id = $1"#
        );
        sqlx::query(&sql)
            .bind(task_id)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = $1, retry_count = $2, last_error = $3,
                   next_retry_at = NULL,
                   updated_at = NOW()
               WHERE task_id = $4"#
        );
        sqlx::query(&sql)
            .bind(new_status.as_str())
            .bind(new_retry_count)
            .bind(error)
            .bind(task_id)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let rel = self.projection_relation_ref();
        let (where_clause, bind_backend) = match target_backend {
            Some(b) => ("AND target_backend = $1", Some(b)),
            None => ("", None),
        };
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'PENDING', retry_count = 0, last_error = '', next_retry_at = NULL, updated_at = NOW()
               WHERE status = 'DEAD_LETTER' {where_clause}"#
        );
        let mut q = sqlx::query(&sql);
        if let Some(b) = bind_backend {
            q = q.bind(b);
        }
        let result = q
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let rel = self.projection_relation_ref();
        // PG handles `make_interval` natively; passing the seconds as
        // a parameter keeps the SQL parameterised.
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'PENDING',
                   last_error = 'stale in-progress reconciliation',
                   updated_at = NOW()
               WHERE status = 'IN_PROGRESS'
                 AND updated_at < NOW() - make_interval(secs => $1::double precision)"#
        );
        let result = sqlx::query(&sql)
            .bind(stale_after.as_secs_f64())
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"SELECT project_id, target_backend, target_instance, projection_kind,
                      COUNT(*)::BIGINT AS pending,
                      EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::DOUBLE PRECISION AS oldest_age_seconds
               FROM {rel}
               WHERE status IN ('PENDING', 'FAILED')
               GROUP BY project_id, target_backend, target_instance, projection_kind
               LIMIT $1"#
        );
        let rows = sqlx::query(&sql)
            .bind(limit.max(1))
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"SELECT source_table, target_backend, target_instance,
                      COUNT(*)::BIGINT AS dead_count
               FROM {rel}
               WHERE status = 'DEAD_LETTER'
               GROUP BY source_table, target_backend, target_instance
               LIMIT $1"#
        );
        let rows = sqlx::query(&sql)
            .bind(limit.max(1))
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
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
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = 'PENDING', retry_count = 0,
                   last_error = 'reconciliation repair', updated_at = NOW()
               WHERE status = 'DEAD_LETTER'
                 AND source_table = $1
                 AND target_backend = $2
                 AND target_instance = $3"#
        );
        let result = sqlx::query(&sql)
            .bind(source_table)
            .bind(target_backend)
            .bind(target_instance)
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(result.rows_affected() as i64)
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        let rel = self.projection_relation_ref();
        // PG accepts a TEXT[] bound via `= ANY($1)`.
        let sql = format!(
            r#"SELECT COUNT(*)::BIGINT FROM {rel}
               WHERE idempotency_key = ANY($1)
                 AND status NOT IN ('COMPLETED','DEAD_LETTER','FAILED')"#
        );
        let n: i64 = sqlx::query_scalar(&sql)
            .bind(idempotency_keys)
            .fetch_one(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        Ok(n)
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let rel = self.projection_relation_ref();
        let sql = format!(r#"SELECT status, COUNT(*)::BIGINT AS n FROM {rel} GROUP BY status"#);
        let rows = sqlx::query(&sql)
            .fetch_all(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in rows {
            let status: String = row.try_get("status").unwrap_or_default();
            let n: i64 = row.try_get("n").unwrap_or(0);
            apply_projection_summary_bucket(&mut s, "postgres", &status, n)?;
        }
        Ok(s)
    }
}
