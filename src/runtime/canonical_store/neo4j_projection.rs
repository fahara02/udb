//! Neo4j implementation of [`ProjectionTaskStore`] (B.10b PHASE 2).
//!
//! Cypher over the HTTP transactional API, mirroring the Postgres impl
//! (`postgres_projection.rs`) SEMANTICS exactly so the cross-backend
//! conformance contract passes byte-for-byte. Every row is a
//! `(:UdbProjectionTask)` node carrying `run_tag` (test isolation on Community
//! single-DB) + the domain id as a property; every query is scoped by
//! `{run_tag:$tag}`.
//!
//! ## Why one Cypher statement is enough where PG needs `FOR UPDATE SKIP LOCKED`
//!
//! Neo4j's auto-commit endpoint runs each statement as one ACID transaction. A
//! `MATCH … WITH … LIMIT … SET …` runs entirely inside that transaction and
//! takes write locks on the matched nodes, so the atomic batch claim is a SINGLE
//! statement (no candidate CTE + second UPDATE round-trip). Concurrent claimers
//! serialise on the node write locks the way PG serialises on the row locks.
//!
//! ## Modeling choices (mirror the logical PG schema)
//!
//! - enums/status stored as the EXACT PG canonical strings
//!   ([`ProjectionTaskStatus::as_str`] / [`ProjectionOperation::as_str`]).
//! - JSON columns (`source_row_key` / `target_options` / `source_payload`)
//!   stored as Cypher string properties (serialised/parsed in Rust).
//! - timestamps stored as client-computed epoch-millis integers; the optional
//!   `next_retry_at` / `completed_at` are ABSENT properties when null and
//!   round-trip back to `None`.
//! - idempotent enqueue is `MERGE (t {run_tag, idempotency_key})`, the Neo4j
//!   analogue of PG's `ON CONFLICT (idempotency_key) DO NOTHING` — re-enqueueing
//!   the same key returns the existing `task_id`.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value as Json, json};
use uuid::Uuid;

use super::neo4j::{
    LABEL_PROJECTION_TASK, Neo4jCanonicalStore, neo_err, now_unix_ms, prop_dt, prop_i32, prop_json,
    prop_opt_dt, prop_str, prop_uuid,
};
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

/// Build a `ProjectionTaskRow` from a `node{.*}` map projection.
fn node_to_projection_task(node: &Json) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id = prop_uuid(node, "task_id")?;
    let operation_str = prop_str(node, "operation");
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in neo4j row"
        ))
    })?;
    let status_str = prop_str(node, "status");
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in neo4j row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: prop_str(node, "idempotency_key"),
        project_id: prop_str(node, "project_id"),
        target_backend: prop_str(node, "target_backend"),
        target_instance: prop_str(node, "target_instance"),
        projection_kind: prop_str(node, "projection_kind"),
        resource_name: prop_str(node, "resource_name"),
        operation,
        source_row_key: prop_json(node, "source_row_key", Json::Null),
        target_options: prop_json(node, "target_options", Json::Null),
        source_payload: prop_json(node, "source_payload", Json::Null),
        source_checksum: prop_str(node, "source_checksum"),
        status,
        retry_count: prop_i32(node, "retry_count"),
        last_error: prop_str(node, "last_error"),
        created_at: prop_dt(node, "created_at"),
        updated_at: prop_dt(node, "updated_at"),
        next_retry_at: prop_opt_dt(node, "next_retry_at"),
        completed_at: prop_opt_dt(node, "completed_at"),
    })
}

impl Neo4jCanonicalStore {
    /// The run-tag bound param value (mirrors phase-1's `tag()`).
    fn proj_tag(&self) -> Json {
        json!(self.run_tag())
    }
}

#[async_trait]
impl ProjectionTaskStore for Neo4jCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "neo4j"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        // Composite RANGE indexes (Community-compatible — see phase-1
        // `ensure_system_tables` for why NOT uniqueness constraints; the
        // logical singleton per (run_tag, idempotency_key) is enforced by the
        // MERGE pattern, the index only makes the lookups cheap). `CREATE INDEX
        // IF NOT EXISTS` is idempotent (Neo4j 5 syntax) and Community-safe.
        let indexes = [
            format!(
                "CREATE INDEX udb_projection_idem_idx IF NOT EXISTS \
                 FOR (t:{LABEL_PROJECTION_TASK}) ON (t.run_tag, t.idempotency_key)"
            ),
            format!(
                "CREATE INDEX udb_projection_status_idx IF NOT EXISTS \
                 FOR (t:{LABEL_PROJECTION_TASK}) ON (t.run_tag, t.status)"
            ),
            format!(
                "CREATE INDEX udb_projection_task_id_idx IF NOT EXISTS \
                 FOR (t:{LABEL_PROJECTION_TASK}) ON (t.run_tag, t.task_id)"
            ),
        ];
        for ddl in indexes {
            self.executor()
                .cypher_rows(&ddl, json!({}))
                .await
                .map_err(|e| neo_err("ensure_projection_tables", e))?;
        }
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        // Idempotent on idempotency_key: MERGE the node, ON CREATE stamp a fresh
        // task_id + the full payload + PENDING/retry_count=0 + created/updated.
        // ON MATCH touches nothing, so a re-enqueue returns the existing
        // task_id (PG's `ON CONFLICT … DO NOTHING` + CTE fallback). Two enqueues
        // of the same key serialise on the node write lock the MERGE takes.
        let new_id = Uuid::new_v4().to_string();
        let now = now_unix_ms();
        let cypher = format!(
            "MERGE (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag, idempotency_key:$idem}}) \
             ON CREATE SET t.task_id=$task_id, t.project_id=$project_id, \
                 t.manifest_checksum=$manifest_checksum, t.message_type=$message_type, \
                 t.source_schema=$source_schema, t.source_table=$source_table, \
                 t.source_row_key=$source_row_key, t.operation=$operation, \
                 t.target_backend=$target_backend, t.target_instance=$target_instance, \
                 t.projection_kind=$projection_kind, t.resource_name=$resource_name, \
                 t.target_options=$target_options, t.source_payload=$source_payload, \
                 t.source_checksum=$source_checksum, t.status=$status, t.retry_count=0, \
                 t.last_error='', t.created_at=$now, t.updated_at=$now \
             RETURN t.task_id AS task_id"
        );
        let params = json!({
            "tag": self.proj_tag(),
            "idem": task.idempotency_key,
            "task_id": new_id,
            "project_id": task.project_id,
            "manifest_checksum": task.manifest_checksum,
            "message_type": task.message_type,
            "source_schema": task.source_schema,
            "source_table": task.source_table,
            "source_row_key": task.source_row_key.to_string(),
            "operation": task.operation.as_str(),
            "target_backend": task.target_backend,
            "target_instance": task.target_instance,
            "projection_kind": task.projection_kind,
            "resource_name": task.resource_name,
            "target_options": task.target_options.to_string(),
            "source_payload": task.source_payload.to_string(),
            "source_checksum": task.source_checksum,
            "status": ProjectionTaskStatus::Pending.as_str(),
            "now": now,
        });
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("enqueue_projection_task", e))?;
        let raw = rows
            .first()
            .and_then(|r| r.get("task_id"))
            .and_then(Json::as_str)
            .ok_or_else(|| neo_err("enqueue_projection_task", "MERGE returned no task_id"))?;
        Uuid::parse_str(raw).map_err(|e| {
            neo_err(
                "enqueue_projection_task",
                format!("bad task_id '{raw}': {e}"),
            )
        })
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        // Single atomic statement (MATCH+SET in one auto-commit tx): select
        // PENDING|FAILED rows under retry cap whose next_retry_at is due,
        // oldest-first, LIMIT batch, flip to IN_PROGRESS + bump updated_at, and
        // RETURN the post-claim node maps. The PG candidate CTEs collapse here
        // because Neo4j's WHERE can OR the status predicate directly and the SET
        // takes the node write locks atomically.
        let now = now_unix_ms();
        let mut filters = String::new();
        if filter.project_id.is_some() {
            filters.push_str(" AND t.project_id = $project_id");
        }
        if filter.target_backend.is_some() {
            filters.push_str(" AND t.target_backend = $target_backend");
        }
        if filter.target_instance.is_some() {
            filters.push_str(" AND t.target_instance = $target_instance");
        }
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status IN ['PENDING','FAILED'] AND t.retry_count < $max \
               AND (t.next_retry_at IS NULL OR t.next_retry_at <= $now){filters} \
             WITH t ORDER BY t.created_at LIMIT $batch \
             SET t.status='IN_PROGRESS', t.updated_at=$now \
             RETURN t{{.*}} AS t"
        );
        let mut params = json!({
            "tag": self.proj_tag(),
            "max": filter.max_retries,
            "now": now,
            "batch": filter.batch_size,
        });
        let obj = params.as_object_mut().expect("params is an object");
        if let Some(p) = &filter.project_id {
            obj.insert("project_id".to_string(), json!(p));
        }
        if let Some(b) = &filter.target_backend {
            obj.insert("target_backend".to_string(), json!(b));
        }
        if let Some(i) = &filter.target_instance {
            obj.insert("target_instance".to_string(), json!(i));
        }
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("claim_projection_tasks", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let node = row
                .get("t")
                .ok_or_else(|| neo_err("claim_projection_tasks", "row missing 't' projection"))?;
            out.push(node_to_projection_task(node)?);
        }
        Ok(out)
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        // COMPLETED + completed_at; clear next_retry_at (REMOVE → absent → None).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag, task_id:$task_id}}) \
             SET t.status=$status, t.completed_at=$now, t.updated_at=$now \
             REMOVE t.next_retry_at"
        );
        self.executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.proj_tag(),
                    "task_id": task_id.to_string(),
                    "status": ProjectionTaskStatus::Completed.as_str(),
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("mark_projection_task_completed", e))?;
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
        // PG clears next_retry_at to NULL here; REMOVE → absent → None, so a
        // re-claimed FAILED row is always due (matches PG's observable behaviour
        // in the contract's immediate re-claim).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag, task_id:$task_id}}) \
             SET t.status=$status, t.retry_count=$retry, t.last_error=$err, t.updated_at=$now \
             REMOVE t.next_retry_at"
        );
        self.executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.proj_tag(),
                    "task_id": task_id.to_string(),
                    "status": new_status.as_str(),
                    "retry": new_retry_count,
                    "err": error,
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("mark_projection_task_failed", e))?;
        Ok(())
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let now = now_unix_ms();
        let backend_filter = if target_backend.is_some() {
            " AND t.target_backend = $backend"
        } else {
            ""
        };
        // SET back to PENDING + reset retry/last_error; REMOVE next_retry_at.
        // `count(t)` returns rows-affected (PG's rows_affected()).
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status = 'DEAD_LETTER'{backend_filter} \
             SET t.status='PENDING', t.retry_count=0, t.last_error='', t.updated_at=$now \
             REMOVE t.next_retry_at \
             RETURN count(t) AS n"
        );
        let mut params = json!({ "tag": self.proj_tag(), "now": now });
        if let Some(b) = target_backend {
            params
                .as_object_mut()
                .expect("params is an object")
                .insert("backend".to_string(), json!(b));
        }
        let rows = self
            .executor()
            .cypher_rows(&cypher, params)
            .await
            .map_err(|e| neo_err("requeue_dead_letter_tasks", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let now = now_unix_ms();
        let cutoff = now - (stale_after.as_millis() as i64);
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status = 'IN_PROGRESS' AND t.updated_at < $cutoff \
             SET t.status='PENDING', t.last_error='stale in-progress reconciliation', \
                 t.updated_at=$now \
             RETURN count(t) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.proj_tag(), "cutoff": cutoff, "now": now }),
            )
            .await
            .map_err(|e| neo_err("reset_stale_in_progress_tasks", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        // Aggregate PENDING+FAILED per (project, backend, instance, kind) with
        // Cypher `count()`/`min()`; compute the oldest-age in Rust from the
        // min(created_at) epoch-millis (PG's EXTRACT(EPOCH FROM NOW()-MIN)).
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status IN ['PENDING','FAILED'] \
             WITH t.project_id AS project_id, t.target_backend AS target_backend, \
                  t.target_instance AS target_instance, t.projection_kind AS projection_kind, \
                  count(t) AS pending, min(t.created_at) AS oldest \
             RETURN project_id, target_backend, target_instance, projection_kind, pending, oldest \
             LIMIT $limit"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.proj_tag(), "limit": limit.max(1) }),
            )
            .await
            .map_err(|e| neo_err("pending_task_metrics", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let oldest = row.get("oldest").and_then(Json::as_i64).unwrap_or(now);
            let age = ((now - oldest) as f64 / 1000.0).max(0.0);
            out.push(PendingTaskMetric {
                project_id: prop_str(row, "project_id"),
                target_backend: prop_str(row, "target_backend"),
                target_instance: prop_str(row, "target_instance"),
                projection_kind: prop_str(row, "projection_kind"),
                pending: row.get("pending").and_then(Json::as_i64).unwrap_or(0),
                oldest_age_seconds: age,
            });
        }
        Ok(out)
    }

    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>> {
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status = 'DEAD_LETTER' \
             WITH t.source_table AS source_table, t.target_backend AS target_backend, \
                  t.target_instance AS target_instance, count(t) AS dead_count \
             RETURN source_table, target_backend, target_instance, dead_count \
             LIMIT $limit"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.proj_tag(), "limit": limit.max(1) }),
            )
            .await
            .map_err(|e| neo_err("dead_letter_groups", e))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(DeadLetterGroup {
                source_table: prop_str(row, "source_table"),
                target_backend: prop_str(row, "target_backend"),
                target_instance: prop_str(row, "target_instance"),
                dead_count: row.get("dead_count").and_then(Json::as_i64).unwrap_or(0),
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
        let now = now_unix_ms();
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.status = 'DEAD_LETTER' AND t.source_table = $source_table \
               AND t.target_backend = $target_backend AND t.target_instance = $target_instance \
             SET t.status='PENDING', t.retry_count=0, t.last_error='reconciliation repair', \
                 t.updated_at=$now \
             REMOVE t.next_retry_at \
             RETURN count(t) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({
                    "tag": self.proj_tag(),
                    "source_table": source_table,
                    "target_backend": target_backend,
                    "target_instance": target_instance,
                    "now": now,
                }),
            )
            .await
            .map_err(|e| neo_err("requeue_dead_letter_by_source", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        // Count rows whose idempotency_key is in the list and whose status is
        // NOT settled (COMPLETED/DEAD_LETTER/FAILED). `IN $keys` binds a string
        // list directly (PG's `= ANY($1)`).
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             WHERE t.idempotency_key IN $keys \
               AND t.status <> 'COMPLETED' \
             RETURN count(t) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(
                &cypher,
                json!({ "tag": self.proj_tag(), "keys": idempotency_keys }),
            )
            .await
            .map_err(|e| neo_err("pending_projection_task_count", e))?;
        Ok(rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(Json::as_i64)
            .unwrap_or(0))
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        // Count by status with Cypher aggregation; fold into the typed summary
        // in Rust (an unknown status string is a hard error, like the SQL/Cass
        // impls).
        let cypher = format!(
            "MATCH (t:{LABEL_PROJECTION_TASK} {{run_tag:$tag}}) \
             RETURN t.status AS status, count(t) AS n"
        );
        let rows = self
            .executor()
            .cypher_rows(&cypher, json!({ "tag": self.proj_tag() }))
            .await
            .map_err(|e| neo_err("projection_task_summary", e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in &rows {
            let status = prop_str(row, "status");
            let n = row.get("n").and_then(Json::as_i64).unwrap_or(0);
            match ProjectionTaskStatus::parse(&status) {
                Some(ProjectionTaskStatus::Pending) => s.pending += n,
                Some(ProjectionTaskStatus::InProgress) => s.in_progress += n,
                Some(ProjectionTaskStatus::Completed) => s.completed += n,
                Some(ProjectionTaskStatus::Failed) => s.failed += n,
                Some(ProjectionTaskStatus::DeadLetter) => s.dead_letter += n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown projection status '{status}' in neo4j summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
