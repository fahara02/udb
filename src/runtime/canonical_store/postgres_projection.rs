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

use super::dialect::{apply_projection_summary_bucket, projection_retry_delay_secs};
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
        manifest_checksum: row.try_get("manifest_checksum").unwrap_or_default(),
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
        //
        // B.7: the statement strings now come from the shared
        // `sql_schema` renderer (single source of truth across SQL
        // backends); the execute/error-handling loop below is
        // unchanged.
        let stmts = super::sql_schema::postgres_projection_tasks_ddl(rel);
        for sql in stmts.iter() {
            sqlx::query(sql)
                .execute(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        }
        // D9: the monotonic per-task row revision the ordering contract keys
        // on, then the index behind the per-row ordering lookups (supersede
        // on claim, re-arm on enqueue, in-flight exclusivity on claim), which
        // probe "the newest task for this row and target".
        for sql in [
            postgres_row_revision_ddl(rel),
            postgres_row_order_index_ddl(rel),
        ] {
            sqlx::query(&sql)
                .execute(self.pg_pool())
                .await
                .map_err(|e| SystemStoreError::query("postgres", sql.clone(), e))?;
        }
        Ok(())
    }

    fn enforces_per_row_ordering(&self) -> bool {
        true
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
        // Per-row ordering: before claiming, retire every PENDING/FAILED task
        // that a NEWER task (higher `row_revision`) for the same row and target
        // has superseded. Each task carries the full row state, so the newest
        // one is the only one worth applying — and applying an older one after
        // it (a retried failure, a requeued dead letter) would roll the target
        // back. The claim below additionally never takes a task whose row has
        // a task IN_PROGRESS or a newer queued task, so at most one task per
        // row is in flight and tasks for one row apply in revision order.
        let supersede_sql = postgres_supersede_sql(rel, filter.project_id.is_some());
        let mut supersede = sqlx::query(&supersede_sql);
        if let Some(project_id) = &filter.project_id {
            supersede = supersede.bind(project_id);
        }
        supersede
            .execute(self.pg_pool())
            .await
            .map_err(|e| SystemStoreError::query("postgres", supersede_sql.clone(), e))?;
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
                format!(
                    "AND target_backend = ${next_param} AND target_instance = ${}",
                    next_param + 1
                )
            }
            (Some(_), None) => format!("AND target_backend = ${next_param}"),
            (None, Some(_)) => format!("AND target_instance = ${next_param}"),
            (None, None) => String::new(),
        };
        let row_exclusive = postgres_claim_row_exclusive_sql(rel);
        let sql = format!(
            r#"
            WITH pending_candidates AS (
                SELECT task_id, created_at FROM {rel} AS cand
                WHERE status = 'PENDING'
                  AND retry_count < $1
                  {project_filter}
                  {target_filter}
                  {row_exclusive}
                ORDER BY created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            failed_candidates AS (
                SELECT task_id, created_at FROM {rel} AS cand
                WHERE status = 'FAILED'
                  AND retry_count < $1
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                  {project_filter}
                  {target_filter}
                  {row_exclusive}
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
            RETURNING task_id, idempotency_key, project_id, manifest_checksum,
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
        // FAILED tasks become re-claimable after an exponential backoff so a
        // persistently-failing projection doesn't hot-loop the worker; the
        // claim query gates on `next_retry_at <= NOW()`. DEAD_LETTER is
        // terminal, so it keeps `next_retry_at = NULL` and is never reclaimed.
        // `NULL` backoff (DEAD_LETTER) yields `next_retry_at = NULL`; the
        // `$5` parameter is still referenced in both branches so PG doesn't
        // complain about an unused bind parameter.
        let backoff_secs = match new_status {
            ProjectionTaskStatus::Failed => Some(projection_retry_delay_secs(new_retry_count)),
            _ => None,
        };
        let rel = self.projection_relation_ref();
        let sql = format!(
            r#"UPDATE {rel}
               SET status = $1, retry_count = $2, last_error = $3,
                   next_retry_at = CASE
                       WHEN $5::bigint IS NULL THEN NULL
                       ELSE NOW() + ($5::bigint * INTERVAL '1 second')
                   END,
                   updated_at = NOW()
               WHERE task_id = $4"#
        );
        sqlx::query(&sql)
            .bind(new_status.as_str())
            .bind(new_retry_count)
            .bind(error)
            .bind(task_id)
            .bind(backoff_secs)
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
            r#"SELECT project_id, source_table, target_backend, target_instance,
                      COUNT(*)::BIGINT AS dead_count
               FROM {rel}
               WHERE status = 'DEAD_LETTER'
                 AND last_error NOT LIKE 'projection authority rejected:%'
               GROUP BY project_id, source_table, target_backend, target_instance
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
                project_id: row.try_get("project_id").unwrap_or_default(),
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
        project_id: &str,
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
                 AND last_error NOT LIKE 'projection authority rejected:%'
                 AND project_id = $1
                 AND source_table = $2
                 AND target_backend = $3
                 AND target_instance = $4"#
        );
        let result = sqlx::query(&sql)
            .bind(project_id)
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
        // P2-1 (NF-1/NF-2): only COMPLETED clears the read fence. FAILED (a
        // projection that will RETRY) and DEAD_LETTER (a projection that will
        // NEVER complete) are NOT projected yet, so for read-your-writes they must
        // count as PENDING — a FAILED task keeps the fence blocking until the retry
        // lands; a DEAD_LETTER task never clears, so the fence times out into a
        // ProjectionMissing (the honest "this write can't be read consistently"),
        // never a silent stale-as-fresh clear.
        //
        // A requested key with NO task row counts as pending too. The keys come
        // from this broker's own write responses, so a missing row means the
        // fence is looking in the wrong ledger (another project's store, a
        // reset table) — treating that as "0 pending" cleared the fence and
        // served a stale read as fresh. Counting it keeps the fence closed until
        // it times out into the honest ProjectionMissing. Superseded tasks are
        // retired as COMPLETED, never deleted, so they still clear.
        let sql = format!(
            r#"SELECT COUNT(*)::BIGINT
               FROM (SELECT DISTINCT key FROM UNNEST($1::TEXT[]) AS requested(key)) AS k
               WHERE NOT EXISTS (
                   SELECT 1 FROM {rel} AS t
                   WHERE t.idempotency_key = k.key AND t.status = 'COMPLETED'
               )"#
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

/// D9: the monotonic row revision every projection task carries. A
/// `BIGSERIAL` draws `nextval` when the task row is INSERTed, and the live
/// write path inserts its task inside the writer's transaction AFTER the row
/// write — i.e. while holding the source row's lock — so for two committed
/// writes to the same row the later writer's task has the strictly higher
/// revision. Unlike a timestamp it cannot tie or step backwards with the clock.
/// The re-arm on enqueue (projection engine) takes a FRESH revision from the
/// proposed row (`EXCLUDED.row_revision`), so a row returning to an earlier
/// value orders after the value it replaced.
fn postgres_row_revision_ddl(rel: &str) -> String {
    format!(r#"ALTER TABLE {rel} ADD COLUMN IF NOT EXISTS row_revision BIGSERIAL"#)
}

/// Index backing the per-row ordering lookups: "is there a newer task for this
/// (project, source row, target)?". The row key is indexed by its md5 so an
/// arbitrarily large JSONB key (a delete filter on a table with no primary key)
/// can never exceed the btree entry limit and fail the enqueue.
fn postgres_row_order_index_ddl(rel: &str) -> String {
    format!(
        r#"CREATE INDEX IF NOT EXISTS "idx_udb_projection_tasks_row_revision"
                 ON {rel} (project_id, source_table, target_backend, target_instance,
                           resource_name, md5(source_row_key::text), row_revision)"#
    )
}

/// SQL for the tenant a projection task's row belongs to: the value of the
/// target's `tenant_field` option inside the task's source payload (empty when
/// the target declares none). Two tenants' rows can share a primary key, so
/// "the same row" for ordering purposes is (row key, tenant) — matching on the
/// key alone let one tenant's newer write supersede (and so silently drop)
/// another tenant's task.
pub(crate) fn projection_task_row_tenant_sql(alias: &str) -> String {
    format!(
        "COALESCE({alias}.source_payload ->> (SELECT o ->> 'value' FROM jsonb_array_elements(         CASE WHEN jsonb_typeof({alias}.target_options) = 'array'          THEN {alias}.target_options ELSE '[]'::jsonb END) AS o          WHERE o ->> 'key' = 'tenant_field' LIMIT 1), '')"
    )
}

/// The "same row and same target" predicate between two aliased task rows —
/// the identity every per-row ordering rule (supersede, re-arm, in-flight
/// exclusivity) compares on.
pub(crate) fn projection_task_same_row_sql(a: &str, b: &str) -> String {
    format!(
        "{a}.project_id = {b}.project_id \
         AND {a}.source_table = {b}.source_table \
         AND {a}.target_backend = {b}.target_backend \
         AND {a}.target_instance = {b}.target_instance \
         AND {a}.resource_name = {b}.resource_name \
         AND md5({a}.source_row_key::text) = md5({b}.source_row_key::text) \
         AND {a_tenant} = {b_tenant}",
        a_tenant = projection_task_row_tenant_sql(a),
        b_tenant = projection_task_row_tenant_sql(b),
    )
}

/// Claim-side half of the D9 ordering contract: never claim a task (`cand`)
/// while another task for the same row and target is IN_PROGRESS, or while a
/// NEWER task for it is still queued (PENDING/FAILED — the next supersede pass
/// retires `cand` in its favour). At most one task per row is in flight, so a
/// slow worker can never apply an older row state after a newer one landed.
fn postgres_claim_row_exclusive_sql(rel: &str) -> String {
    format!(
        "AND NOT EXISTS (
                      SELECT 1 FROM {rel} AS sib
                      WHERE sib.task_id <> cand.task_id
                        AND {same_row}
                        AND (sib.status = 'IN_PROGRESS'
                             OR (sib.status IN ('PENDING', 'FAILED')
                                 AND sib.row_revision > cand.row_revision)))",
        same_row = projection_task_same_row_sql("sib", "cand"),
    )
}

/// Retire (mark COMPLETED, with a `superseded` note) every PENDING/FAILED task
/// for which a strictly newer task exists for the same row and target. The
/// ordering key is the monotonic `row_revision` (see
/// [`postgres_row_revision_ddl`]). IN_PROGRESS tasks are left to their worker.
fn postgres_supersede_sql(rel: &str, project_scoped: bool) -> String {
    let project_filter = if project_scoped {
        "AND older.project_id = $1"
    } else {
        ""
    };
    format!(
        r#"UPDATE {rel} AS older
           SET status = 'COMPLETED', completed_at = NOW(), next_retry_at = NULL,
               updated_at = NOW(),
               last_error = 'superseded by a newer projection task for the same row'
           WHERE older.status IN ('PENDING', 'FAILED')
             {project_filter}
             AND EXISTS (
                 SELECT 1 FROM {rel} AS newer
                 WHERE {same_row}
                   AND newer.row_revision > older.row_revision)"#,
        same_row = projection_task_same_row_sql("newer", "older"),
    )
}

#[cfg(test)]
mod row_order_tests {
    use super::*;

    /// Two tenants' rows can share a primary key: "the same row" must include
    /// the tenant, or tenant B's newer write retires tenant A's queued task and
    /// A's change is never projected.
    #[test]
    fn supersede_only_matches_a_sibling_in_the_same_tenant() {
        let sql = postgres_supersede_sql(DEFAULT_REL, false);
        assert!(
            sql.contains(&format!(
                "{} = {}",
                projection_task_row_tenant_sql("newer"),
                projection_task_row_tenant_sql("older")
            )),
            "{sql}"
        );
        let tenant = projection_task_row_tenant_sql("older");
        assert!(tenant.contains("'tenant_field'"), "{tenant}");
        assert!(
            tenant.contains("jsonb_typeof(older.target_options) = 'array'"),
            "{tenant}"
        );
    }

    #[test]
    fn supersede_retires_only_queued_tasks_older_than_a_sibling() {
        let sql = postgres_supersede_sql(DEFAULT_REL, false);
        assert!(
            sql.contains("older.status IN ('PENDING', 'FAILED')"),
            "{sql}"
        );
        assert!(
            !sql.contains("IN_PROGRESS"),
            "in-flight tasks belong to their worker"
        );
        // Same row AND same target: a newer Mongo task must not retire a Qdrant one.
        for column in [
            "project_id",
            "source_table",
            "target_backend",
            "target_instance",
            "resource_name",
        ] {
            assert!(
                sql.contains(&format!("newer.{column} = older.{column}")),
                "{column}: {sql}"
            );
        }
        assert!(sql.contains("md5(newer.source_row_key::text) = md5(older.source_row_key::text)"));
        assert!(
            sql.contains("newer.row_revision > older.row_revision"),
            "{sql}"
        );
        assert!(
            !sql.contains("created_at)"),
            "ordering is by revision: {sql}"
        );
        assert!(!sql.contains("$1"), "unscoped pass binds nothing");
        let scoped = postgres_supersede_sql(DEFAULT_REL, true);
        assert!(scoped.contains("AND older.project_id = $1"), "{scoped}");
    }

    #[test]
    fn row_order_index_uses_a_bounded_row_key() {
        let ddl = postgres_row_order_index_ddl(DEFAULT_REL);
        assert!(
            ddl.contains("md5(source_row_key::text), row_revision)"),
            "{ddl}"
        );
        assert!(ddl.contains("CREATE INDEX IF NOT EXISTS"), "{ddl}");
    }

    /// D9: every task carries a sequence-drawn revision (not a clock stamp).
    #[test]
    fn row_revision_column_is_a_sequence() {
        let ddl = postgres_row_revision_ddl(DEFAULT_REL);
        assert!(
            ddl.contains("ADD COLUMN IF NOT EXISTS row_revision BIGSERIAL"),
            "{ddl}"
        );
    }

    /// D9: a claim never takes a task whose row already has one in flight or
    /// a newer one queued, so one row's tasks apply one at a time, in order.
    #[test]
    fn claim_is_exclusive_per_row_and_takes_only_the_newest() {
        let sql = postgres_claim_row_exclusive_sql(DEFAULT_REL);
        assert!(sql.contains("AND NOT EXISTS"), "{sql}");
        assert!(sql.contains("sib.task_id <> cand.task_id"), "{sql}");
        assert!(sql.contains("sib.status = 'IN_PROGRESS'"), "{sql}");
        assert!(
            sql.contains("sib.row_revision > cand.row_revision"),
            "{sql}"
        );
        assert!(
            sql.contains("md5(sib.source_row_key::text) = md5(cand.source_row_key::text)"),
            "{sql}"
        );
    }
}
