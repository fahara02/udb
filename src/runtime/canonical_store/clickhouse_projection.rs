//! ClickHouse implementation of [`ProjectionTaskStore`] (B.10c PHASE 2).
//!
//! Semantics mirror the Postgres impl (`postgres_projection.rs`) exactly so the
//! cross-backend conformance contract passes byte-for-byte. ClickHouse has
//! **no multi-statement transactions, no row locks, no `FOR UPDATE SKIP
//! LOCKED`, and no native `INSERT … ON CONFLICT`**, so mutable task state lives
//! in a `ReplacingMergeTree(version)` keyed by `task_id`: every mutation INSERTs
//! a NEW row carrying `version + 1`, and EVERY read uses `SELECT … FINAL` so the
//! engine collapses superseded rows by the replacing key and only the
//! highest-`version` row is observed. A raw (non-`FINAL`) read would leak stale,
//! superseded rows and break correctness, so this file never reads without
//! `FINAL`.
//!
//! ## Atomic claim — versioned-CAS flip
//!
//! The PG atomic batch claim becomes: read the FINAL candidates filtered by
//! status / retry / next_retry / target, then per-row versioned-CAS flip
//! PENDING/FAILED → IN_PROGRESS (INSERT version+1 with the new status), re-read
//! that row's FINAL status, and keep it only if it now reads IN_PROGRESS. The
//! read-decide-insert-reread sequence runs under a KeeperMap-backed projection
//! mutation lease, so two broker processes do not claim the same task.
//!
//! ## Schema (mirrors the logical PG schema; ClickHouse types)
//!
//! - `udb_projection_tasks` — `ReplacingMergeTree(version) ORDER BY task_id`.
//!   `task_id` is the uuid rendered as text (matching phase 1's `event_id`
//!   String). Enums are stored as their EXACT PG canonical strings, JSON columns
//!   as `String` (parsed in Rust), timestamps as epoch-millis `Int64`. The
//!   nullable timestamps (`next_retry_at` / `completed_at`) use the sentinel
//!   `0` (= absent), so they round-trip `None`-when-absent.
//!
//! This file also hosts the `pub(super)` JSONCompact-row → typed helpers reused
//! by the sibling system-store impls (`clickhouse_saga.rs`,
//! `clickhouse_admin_audit.rs`, `clickhouse_migration_audit.rs`).

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use uuid::Uuid;

use super::clickhouse::{ClickHouseCanonicalStore, sql_lit};
use super::system_store::{
    DeadLetterGroup, PendingTaskMetric, ProjectionClaimFilter, ProjectionOperation,
    ProjectionTaskInsert, ProjectionTaskRow, ProjectionTaskStatus, ProjectionTaskStore,
    ProjectionTaskSummary, SystemStoreError, SystemStoreResult,
};

const CLICKHOUSE_PROJECTION_MUTATION_LOCK: &str = "__udb_clickhouse_projection_tasks";

// ── Shared JSONCompact cell helpers (reused by every phase-2 impl) ────────────
//
// `ClickHouseExecutor::select_rows` returns `Vec<Json>` where each row is a JSON
// object keyed by column name. ClickHouse renders `Int64` as a JSON number but
// `UInt64` (our `version`) as a JSON *string* in JSONCompact (to preserve
// precision beyond 2^53), so the integer helpers accept both forms.

/// Read an `i64` cell (epoch millis, seq, retry counts) → 0 when absent.
pub(super) fn ch_i64(row: &Json, key: &str) -> i64 {
    let Some(cell) = row.get(key) else {
        return 0;
    };
    cell.as_i64()
        .or_else(|| cell.as_str().and_then(|s| s.parse::<i64>().ok()))
        .unwrap_or(0)
}

/// Read an `i32` cell (retry_count, operation_index) → 0 when absent.
pub(super) fn ch_i32(row: &Json, key: &str) -> i32 {
    ch_i64(row, key) as i32
}

/// Read a `UInt64` cell (`version`). JSONCompact renders UInt64 as a string, so
/// prefer the string form, falling back to a JSON number.
pub(super) fn ch_u64(row: &Json, key: &str) -> u64 {
    let Some(cell) = row.get(key) else {
        return 0;
    };
    cell.as_str()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| cell.as_u64())
        .unwrap_or(0)
}

/// Read a `String` cell → empty string when absent.
pub(super) fn ch_str(row: &Json, key: &str) -> String {
    row.get(key)
        .and_then(Json::as_str)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

/// Parse a uuid from a text column (uuid PKs are rendered as text, matching
/// phase 1's `event_id String`).
pub(super) fn ch_uuid(row: &Json, key: &str) -> SystemStoreResult<Uuid> {
    let raw = ch_str(row, key);
    Uuid::parse_str(&raw).map_err(|e| {
        SystemStoreError::InvalidInput(format!(
            "clickhouse row column '{key}' is not a uuid '{raw}': {e}"
        ))
    })
}

/// Decode a JSON-shaped String column back into `serde_json::Value`.
/// Absent/empty/unparseable → `default`.
pub(super) fn ch_json(row: &Json, key: &str, default: Json) -> Json {
    match row.get(key).and_then(Json::as_str) {
        Some(s) if !s.is_empty() => serde_json::from_str(s).unwrap_or(default),
        _ => default,
    }
}

/// A required epoch-millis `Int64` column → `DateTime<Utc>` (falls back to
/// `Utc::now()` when absent/unparseable, matching the SQL impls' tolerant
/// behaviour).
pub(super) fn ch_dt(row: &Json, key: &str) -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp_millis(ch_i64(row, key)).unwrap_or_else(Utc::now)
}

/// An OPTIONAL epoch-millis `Int64` column: the sentinel `0` (and any absent /
/// unparseable cell) decodes to `None`; any non-zero millis decodes to `Some`.
/// This is the round-trip discipline the MSSQL bug flagged — a missing /
/// cleared timestamp must parse back to `None`, never `Some(epoch-0)` or
/// `Some(now)`.
pub(super) fn ch_opt_dt(row: &Json, key: &str) -> Option<DateTime<Utc>> {
    match ch_i64(row, key) {
        0 => None,
        millis => DateTime::<Utc>::from_timestamp_millis(millis),
    }
}

/// Map any executor error string to a typed `SystemStoreError::Io` for the
/// `"clickhouse"` backend. `op` names the failed operation.
pub(super) fn ch_err(op: &str, err: impl std::fmt::Display) -> SystemStoreError {
    SystemStoreError::Io {
        backend: "clickhouse",
        source: format!("{op}: {err}"),
    }
}

/// Render an OPTIONAL `DateTime<Utc>` to the epoch-millis `Int64` SQL literal,
/// using the sentinel `0` for `None` so it round-trips as absent.
pub(super) fn opt_dt_lit(dt: Option<DateTime<Utc>>) -> i64 {
    dt.map(|d| d.timestamp_millis()).unwrap_or(0)
}

// ── Row mapping ───────────────────────────────────────────────────────────────

/// The canonical projection-task SELECT column order. Every read below uses it.
const TASK_COLS: &str = "task_id, idempotency_key, project_id, target_backend, target_instance, \
     projection_kind, resource_name, operation, source_row_key, target_options, \
     source_payload, source_checksum, status, retry_count, last_error, \
     created_at, updated_at, next_retry_at, completed_at";

/// The four "static" columns NOT carried on [`ProjectionTaskRow`]
/// (`manifest_checksum` / `message_type` / `source_schema` / `source_table`) but
/// stored on the table. Every mutation (which rewrites the whole row at
/// version+1) MUST preserve them so `dead_letter_groups` /
/// `requeue_dead_letter_by_source` keep seeing the original `source_table`.
const TASK_STATIC_COLS: &str = "manifest_checksum, message_type, source_schema, source_table";

/// The static-column values read alongside a task row, threaded through each
/// version+1 mutation INSERT so they are never lost on a status flip.
#[derive(Default, Clone)]
struct TaskStaticCols {
    manifest_checksum: String,
    message_type: String,
    source_schema: String,
    source_table: String,
}

fn row_to_static_cols(row: &Json) -> TaskStaticCols {
    TaskStaticCols {
        manifest_checksum: ch_str(row, "manifest_checksum"),
        message_type: ch_str(row, "message_type"),
        source_schema: ch_str(row, "source_schema"),
        source_table: ch_str(row, "source_table"),
    }
}

fn row_to_projection_task(row: &Json) -> SystemStoreResult<ProjectionTaskRow> {
    let task_id = ch_uuid(row, "task_id")?;
    let operation_str = ch_str(row, "operation");
    let operation = ProjectionOperation::parse(&operation_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection operation '{operation_str}' in clickhouse row"
        ))
    })?;
    let status_str = ch_str(row, "status");
    let status = ProjectionTaskStatus::parse(&status_str).ok_or_else(|| {
        SystemStoreError::InvalidInput(format!(
            "unknown projection status '{status_str}' in clickhouse row"
        ))
    })?;
    Ok(ProjectionTaskRow {
        task_id,
        idempotency_key: ch_str(row, "idempotency_key"),
        project_id: ch_str(row, "project_id"),
        target_backend: ch_str(row, "target_backend"),
        target_instance: ch_str(row, "target_instance"),
        projection_kind: ch_str(row, "projection_kind"),
        resource_name: ch_str(row, "resource_name"),
        operation,
        source_row_key: ch_json(row, "source_row_key", Json::Null),
        target_options: ch_json(row, "target_options", Json::Null),
        source_payload: ch_json(row, "source_payload", Json::Null),
        source_checksum: ch_str(row, "source_checksum"),
        status,
        retry_count: ch_i32(row, "retry_count"),
        last_error: ch_str(row, "last_error"),
        created_at: ch_dt(row, "created_at"),
        updated_at: ch_dt(row, "updated_at"),
        next_retry_at: ch_opt_dt(row, "next_retry_at"),
        completed_at: ch_opt_dt(row, "completed_at"),
    })
}

impl ClickHouseCanonicalStore {
    fn projection_table(&self) -> SystemStoreResult<String> {
        self.qualified("udb_projection_tasks")
            .map_err(|e| ch_err("projection_table", e))
    }

    /// Read the FINAL (latest-version) row for one task_id, returning the typed
    /// row, its static columns, and its version. `FINAL` collapses the
    /// ReplacingMergeTree parts so superseded versions never leak.
    async fn read_task_final(
        &self,
        task_id: Uuid,
    ) -> SystemStoreResult<Option<(ProjectionTaskRow, TaskStaticCols, u64)>> {
        let tbl = self.projection_table()?;
        let sql = format!(
            "SELECT {TASK_COLS}, {TASK_STATIC_COLS}, version FROM {tbl} FINAL WHERE task_id = {id}",
            id = sql_lit(&task_id.to_string()),
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("read_task_final", e))?;
        match rows.first() {
            Some(r) => Ok(Some((
                row_to_projection_task(r)?,
                row_to_static_cols(r),
                ch_u64(r, "version"),
            ))),
            None => Ok(None),
        }
    }

    /// INSERT a full task row at `version`, preserving the static columns. Every
    /// mutation supersedes the prior version once FINAL/merge runs; the caller
    /// mutates the `ProjectionTaskRow` fields it needs (status / updated_at /
    /// next_retry_at / completed_at / retry_count / last_error) before calling.
    async fn insert_task_version(
        &self,
        task: &ProjectionTaskRow,
        statics: &TaskStaticCols,
        version: u64,
    ) -> SystemStoreResult<()> {
        let tbl = self.projection_table()?;
        let sql = format!(
            "INSERT INTO {tbl} ({TASK_COLS}, {TASK_STATIC_COLS}, version) VALUES (\
             {task_id}, {idem}, {project}, {backend}, {instance}, {kind}, {resource}, \
             {operation}, {row_key}, {options}, {payload}, {checksum}, {status}, \
             {retry}, {error}, {created}, {updated}, {next_retry}, {completed}, \
             {manifest}, {message}, {schema}, {source_table}, {version})",
            task_id = sql_lit(&task.task_id.to_string()),
            idem = sql_lit(&task.idempotency_key),
            project = sql_lit(&task.project_id),
            backend = sql_lit(&task.target_backend),
            instance = sql_lit(&task.target_instance),
            kind = sql_lit(&task.projection_kind),
            resource = sql_lit(&task.resource_name),
            operation = sql_lit(task.operation.as_str()),
            row_key = sql_lit(&task.source_row_key.to_string()),
            options = sql_lit(&task.target_options.to_string()),
            payload = sql_lit(&task.source_payload.to_string()),
            checksum = sql_lit(&task.source_checksum),
            status = sql_lit(task.status.as_str()),
            retry = task.retry_count,
            error = sql_lit(&task.last_error),
            created = task.created_at.timestamp_millis(),
            updated = task.updated_at.timestamp_millis(),
            next_retry = opt_dt_lit(task.next_retry_at),
            completed = opt_dt_lit(task.completed_at),
            manifest = sql_lit(&statics.manifest_checksum),
            message = sql_lit(&statics.message_type),
            schema = sql_lit(&statics.source_schema),
            source_table = sql_lit(&statics.source_table),
        );
        self.executor()
            .execute_ddl(&sql)
            .await
            .map_err(|e| ch_err("insert_task_version", e))
    }

    async fn acquire_projection_mutation_lock(&self, op: &str) -> SystemStoreResult<String> {
        let owner = format!("{op}:{}", Uuid::new_v4());
        self.acquire_system_mutation_lease(CLICKHOUSE_PROJECTION_MUTATION_LOCK, &owner, op)
            .await
            .map_err(|err| ch_err(op, err))?;
        Ok(owner)
    }

    async fn release_projection_mutation_lock(
        &self,
        owner: &str,
        op: &str,
    ) -> SystemStoreResult<()> {
        self.release_system_mutation_lease(CLICKHOUSE_PROJECTION_MUTATION_LOCK, owner, op)
            .await
            .map_err(|err| ch_err(op, err))
    }
}

#[async_trait]
impl ProjectionTaskStore for ClickHouseCanonicalStore {
    fn backend_label(&self) -> &'static str {
        "clickhouse"
    }

    async fn ensure_projection_tables(&self) -> SystemStoreResult<()> {
        let tbl = self.projection_table()?;
        // ReplacingMergeTree(version) keyed by task_id: every mutation INSERTs a
        // version+1 row, FINAL reads keep only the highest. Timestamps are
        // epoch-millis Int64 (nullable ones use sentinel 0). manifest_checksum /
        // message_type / source_schema / source_table are stored (for
        // dead_letter_groups' source_table) even though the claim row omits them.
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {tbl} (\
             task_id String, \
             idempotency_key String, \
             project_id String, \
             manifest_checksum String, \
             message_type String, \
             source_schema String, \
             source_table String, \
             source_row_key String, \
             operation String, \
             target_backend String, \
             target_instance String, \
             projection_kind String, \
             resource_name String, \
             target_options String, \
             source_payload String, \
             source_checksum String, \
             status String, \
             retry_count Int32, \
             last_error String, \
             created_at Int64, \
             updated_at Int64, \
             next_retry_at Int64, \
             completed_at Int64, \
             version UInt64\
             ) ENGINE = ReplacingMergeTree(version) ORDER BY task_id"
        );
        self.executor()
            .execute_ddl(&ddl)
            .await
            .map_err(|e| ch_err("ensure_projection_tables", e))?;
        Ok(())
    }

    async fn enqueue_projection_task(
        &self,
        task: &ProjectionTaskInsert,
    ) -> SystemStoreResult<Uuid> {
        let owner = self
            .acquire_projection_mutation_lock("enqueue_projection_task")
            .await?;
        let result = async {
            let tbl = self.projection_table()?;
            // Idempotent on idempotency_key: read FINAL by key; if a row exists return
            // its task_id (PG's `ON CONFLICT (idempotency_key) DO NOTHING` + fallback
            // SELECT). The KeeperMap mutation lease serializes this read→insert path
            // across broker processes.
            let existing_sql = format!(
                "SELECT task_id FROM {tbl} FINAL WHERE idempotency_key = {key}",
                key = sql_lit(&task.idempotency_key),
            );
            let existing = self
                .executor()
                .select_rows(&existing_sql)
                .await
                .map_err(|e| ch_err("enqueue_projection_task lookup", e))?;
            if let Some(row) = existing.first() {
                return ch_uuid(row, "task_id");
            }

            // Fresh enqueue: insert version 1 with full payload (incl. the
            // manifest/message/schema/source_table columns the claim row omits but
            // dead_letter_groups needs). Status starts PENDING, retry_count 0,
            // next_retry_at / completed_at absent (sentinel 0).
            let new_id = Uuid::new_v4();
            let now = Self::now_unix_ms();
            let sql = format!(
                "INSERT INTO {tbl} (\
                 task_id, idempotency_key, project_id, manifest_checksum, message_type, \
                 source_schema, source_table, source_row_key, operation, target_backend, \
                 target_instance, projection_kind, resource_name, target_options, \
                 source_payload, source_checksum, status, retry_count, last_error, \
                 created_at, updated_at, next_retry_at, completed_at, version) VALUES (\
                 {task_id}, {idem}, {project}, {manifest}, {message}, {schema}, {source_table}, \
                 {row_key}, {operation}, {backend}, {instance}, {kind}, {resource}, {options}, \
                 {payload}, {checksum}, {status}, 0, '', {now}, {now}, 0, 0, 1)",
                task_id = sql_lit(&new_id.to_string()),
                idem = sql_lit(&task.idempotency_key),
                project = sql_lit(&task.project_id),
                manifest = sql_lit(&task.manifest_checksum),
                message = sql_lit(&task.message_type),
                schema = sql_lit(&task.source_schema),
                source_table = sql_lit(&task.source_table),
                row_key = sql_lit(&task.source_row_key.to_string()),
                operation = sql_lit(task.operation.as_str()),
                backend = sql_lit(&task.target_backend),
                instance = sql_lit(&task.target_instance),
                kind = sql_lit(&task.projection_kind),
                resource = sql_lit(&task.resource_name),
                options = sql_lit(&task.target_options.to_string()),
                payload = sql_lit(&task.source_payload.to_string()),
                checksum = sql_lit(&task.source_checksum),
                status = sql_lit(ProjectionTaskStatus::Pending.as_str()),
            );
            self.executor()
                .execute_ddl(&sql)
                .await
                .map_err(|e| ch_err("enqueue_projection_task insert", e))?;
            Ok(new_id)
        }
        .await;
        self.release_projection_mutation_lock(&owner, "enqueue_projection_task")
            .await?;
        result
    }

    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>> {
        if filter.batch_size <= 0 {
            return Ok(Vec::new());
        }
        let owner = self
            .acquire_projection_mutation_lock("claim_projection_tasks")
            .await?;
        let result = async {
            let tbl = self.projection_table()?;
            // FINAL candidate scan: read latest-version rows (with the static cols +
            // version so each version+1 flip preserves them), filter PENDING/FAILED +
            // retry/next_retry/target in Rust (mirrors PG's pending/failed candidate
            // CTEs). FINAL is required — a raw read would surface superseded rows.
            let scan = format!("SELECT {TASK_COLS}, {TASK_STATIC_COLS}, version FROM {tbl} FINAL");
            let rows = self
                .executor()
                .select_rows(&scan)
                .await
                .map_err(|e| ch_err("claim_projection_tasks scan", e))?;
            let now = Utc::now();
            let mut candidates: Vec<(ProjectionTaskRow, TaskStaticCols, u64)> = Vec::new();
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
                // `next_retry_at IS NULL OR next_retry_at <= NOW()`). PENDING is always
                // eligible. mark_failed clears next_retry_at to sentinel-0 → always due.
                if task.status == ProjectionTaskStatus::Failed {
                    if let Some(nra) = task.next_retry_at {
                        if nra > now {
                            continue;
                        }
                    }
                }
                candidates.push((task, row_to_static_cols(row), ch_u64(row, "version")));
            }
            // Oldest-first, like PG's `ORDER BY created_at`.
            candidates.sort_by(|a, b| a.0.created_at.cmp(&b.0.created_at));

            // Per-row versioned flip PENDING/FAILED → IN_PROGRESS. The KeeperMap
            // mutation lease serializes this read-decide-insert-reread path across
            // broker processes.
            let claim_now = Self::now_unix_ms();
            let claim_dt =
                DateTime::<Utc>::from_timestamp_millis(claim_now).unwrap_or_else(Utc::now);
            let mut out = Vec::new();
            for (mut task, statics, version) in candidates {
                if out.len() as i64 >= filter.batch_size {
                    break;
                }
                // version+1 INSERT carrying the IN_PROGRESS status at the new
                // updated_at, preserving every other field + the static cols.
                let next_version = version.saturating_add(1).max(1);
                let mut flipped = task.clone();
                flipped.status = ProjectionTaskStatus::InProgress;
                flipped.updated_at = claim_dt;
                self.insert_task_version(&flipped, &statics, next_version)
                    .await?;

                // Re-read FINAL to confirm the live row is now IN_PROGRESS (the flip
                // won the last-writer-by-version collapse).
                if let Some((confirmed, _, _)) = self.read_task_final(task.task_id).await? {
                    if confirmed.status == ProjectionTaskStatus::InProgress {
                        task.status = ProjectionTaskStatus::InProgress;
                        task.updated_at = claim_dt;
                        out.push(task);
                    }
                }
            }
            Ok(out)
        }
        .await;
        self.release_projection_mutation_lock(&owner, "claim_projection_tasks")
            .await?;
        result
    }

    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()> {
        let owner = self
            .acquire_projection_mutation_lock("mark_projection_task_completed")
            .await?;
        let result = async {
            // PG: status COMPLETED, completed_at = NOW(), next_retry_at = NULL.
            // version+1 INSERT preserving the rest of the row + static cols.
            let Some((mut task, statics, version)) = self.read_task_final(task_id).await? else {
                return Ok(());
            };
            let now = Self::now_unix_ms();
            let now_dt = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            task.status = ProjectionTaskStatus::Completed;
            task.updated_at = now_dt;
            task.completed_at = Some(now_dt);
            task.next_retry_at = None; // cleared → sentinel 0 → round-trips None.
            self.insert_task_version(&task, &statics, version.saturating_add(1).max(1))
                .await
        }
        .await;
        self.release_projection_mutation_lock(&owner, "mark_projection_task_completed")
            .await?;
        result
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
        let owner = self
            .acquire_projection_mutation_lock("mark_projection_task_failed")
            .await?;
        let result = async {
            let Some((mut task, statics, version)) = self.read_task_final(task_id).await? else {
                return Ok(());
            };
            let now = Self::now_unix_ms();
            // PG clears next_retry_at to NULL here; the contract re-claims the FAILED
            // row immediately afterward, so a None next_retry_at (always due) matches.
            task.status = new_status;
            task.retry_count = new_retry_count;
            task.last_error = error.to_string();
            task.next_retry_at = None;
            task.updated_at = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            self.insert_task_version(&task, &statics, version.saturating_add(1).max(1))
                .await
        }
        .await;
        self.release_projection_mutation_lock(&owner, "mark_projection_task_failed")
            .await?;
        result
    }

    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64> {
        let owner = self
            .acquire_projection_mutation_lock("requeue_dead_letter_tasks")
            .await?;
        let result = async {
            let tbl = self.projection_table()?;
            let scan = format!("SELECT {TASK_COLS}, {TASK_STATIC_COLS}, version FROM {tbl} FINAL");
            let rows = self
                .executor()
                .select_rows(&scan)
                .await
                .map_err(|e| ch_err("requeue_dead_letter_tasks scan", e))?;
            let now = Self::now_unix_ms();
            let now_dt = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            let mut n = 0i64;
            for row in &rows {
                let mut task = row_to_projection_task(row)?;
                if task.status != ProjectionTaskStatus::DeadLetter {
                    continue;
                }
                if let Some(b) = target_backend {
                    if &task.target_backend != b {
                        continue;
                    }
                }
                let next_version = ch_u64(row, "version").saturating_add(1).max(1);
                task.status = ProjectionTaskStatus::Pending;
                task.retry_count = 0;
                task.last_error = String::new();
                task.next_retry_at = None;
                task.updated_at = now_dt;
                self.insert_task_version(&task, &row_to_static_cols(row), next_version)
                    .await?;
                n += 1;
            }
            Ok(n)
        }
        .await;
        self.release_projection_mutation_lock(&owner, "requeue_dead_letter_tasks")
            .await?;
        result
    }

    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64> {
        let owner = self
            .acquire_projection_mutation_lock("reset_stale_in_progress_tasks")
            .await?;
        let result = async {
            let tbl = self.projection_table()?;
            let scan = format!("SELECT {TASK_COLS}, {TASK_STATIC_COLS}, version FROM {tbl} FINAL");
            let rows = self
                .executor()
                .select_rows(&scan)
                .await
                .map_err(|e| ch_err("reset_stale_in_progress_tasks scan", e))?;
            let cutoff = Utc::now()
                - chrono::Duration::from_std(stale_after)
                    .unwrap_or_else(|_| chrono::Duration::zero());
            let now = Self::now_unix_ms();
            let now_dt = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            let mut n = 0i64;
            for row in &rows {
                let mut task = row_to_projection_task(row)?;
                if task.status != ProjectionTaskStatus::InProgress || task.updated_at >= cutoff {
                    continue;
                }
                let next_version = ch_u64(row, "version").saturating_add(1).max(1);
                task.status = ProjectionTaskStatus::Pending;
                task.last_error = "stale in-progress reconciliation".to_string();
                task.updated_at = now_dt;
                self.insert_task_version(&task, &row_to_static_cols(row), next_version)
                    .await?;
                n += 1;
            }
            Ok(n)
        }
        .await;
        self.release_projection_mutation_lock(&owner, "reset_stale_in_progress_tasks")
            .await?;
        result
    }

    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>> {
        let tbl = self.projection_table()?;
        let scan = format!("SELECT {TASK_COLS} FROM {tbl} FINAL");
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("pending_task_metrics scan", e))?;
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
        let tbl = self.projection_table()?;
        // source_table isn't in TASK_COLS, so select it directly. FINAL collapses
        // to the latest version per task_id.
        let scan = format!(
            "SELECT source_table, target_backend, target_instance, status FROM {tbl} FINAL"
        );
        let rows = self
            .executor()
            .select_rows(&scan)
            .await
            .map_err(|e| ch_err("dead_letter_groups scan", e))?;
        use std::collections::HashMap;
        let mut groups: HashMap<(String, String, String), i64> = HashMap::new();
        for row in &rows {
            if ch_str(row, "status") != ProjectionTaskStatus::DeadLetter.as_str() {
                continue;
            }
            *groups
                .entry((
                    ch_str(row, "source_table"),
                    ch_str(row, "target_backend"),
                    ch_str(row, "target_instance"),
                ))
                .or_insert(0) += 1;
        }
        let mut out: Vec<DeadLetterGroup> = groups
            .into_iter()
            .map(
                |((source_table, target_backend, target_instance), dead_count)| DeadLetterGroup {
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
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64> {
        let owner = self
            .acquire_projection_mutation_lock("requeue_dead_letter_by_source")
            .await?;
        let result = async {
            let tbl = self.projection_table()?;
            // The static cols (incl. source_table) aren't on ProjectionTaskRow, so
            // select them explicitly — both to match on source_table and to preserve
            // them on the version+1 rewrite. FINAL collapses to the latest version.
            let scan = format!("SELECT {TASK_COLS}, {TASK_STATIC_COLS}, version FROM {tbl} FINAL");
            let rows = self
                .executor()
                .select_rows(&scan)
                .await
                .map_err(|e| ch_err("requeue_dead_letter_by_source scan", e))?;
            let now = Self::now_unix_ms();
            let now_dt = DateTime::<Utc>::from_timestamp_millis(now).unwrap_or_else(Utc::now);
            let mut n = 0i64;
            for row in &rows {
                let mut task = row_to_projection_task(row)?;
                if task.status != ProjectionTaskStatus::DeadLetter {
                    continue;
                }
                let statics = row_to_static_cols(row);
                if statics.source_table != source_table
                    || task.target_backend != target_backend
                    || task.target_instance != target_instance
                {
                    continue;
                }
                let next_version = ch_u64(row, "version").saturating_add(1).max(1);
                task.status = ProjectionTaskStatus::Pending;
                task.retry_count = 0;
                task.last_error = "reconciliation repair".to_string();
                task.next_retry_at = None;
                task.updated_at = now_dt;
                self.insert_task_version(&task, &statics, next_version)
                    .await?;
                n += 1;
            }
            Ok(n)
        }
        .await;
        self.release_projection_mutation_lock(&owner, "requeue_dead_letter_by_source")
            .await?;
        result
    }

    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64> {
        if idempotency_keys.is_empty() {
            return Ok(0);
        }
        let tbl = self.projection_table()?;
        // FINAL count of rows whose idempotency_key is in the list and whose
        // status is NOT settled (PG's `status NOT IN
        // ('COMPLETED','DEAD_LETTER','FAILED')`). FINAL is required so superseded
        // versions don't double-count.
        let in_list = idempotency_keys
            .iter()
            .map(|k| sql_lit(k))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT count() AS n FROM {tbl} FINAL WHERE idempotency_key IN ({in_list}) \
             AND status <> 'COMPLETED'" // P2-1 NF-1/NF-2: FAILED/DEAD_LETTER = not projected yet = still fences
        );
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("pending_projection_task_count", e))?;
        Ok(rows.first().map(|r| ch_i64(r, "n")).unwrap_or(0))
    }

    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary> {
        let tbl = self.projection_table()?;
        // FINAL group-by status. FINAL collapses superseded versions so each
        // task_id is counted once at its latest status.
        let sql = format!("SELECT status, count() AS n FROM {tbl} FINAL GROUP BY status");
        let rows = self
            .executor()
            .select_rows(&sql)
            .await
            .map_err(|e| ch_err("projection_task_summary", e))?;
        let mut s = ProjectionTaskSummary::default();
        for row in &rows {
            let status = ch_str(row, "status");
            let n = ch_i64(row, "n");
            match ProjectionTaskStatus::parse(&status) {
                Some(ProjectionTaskStatus::Pending) => s.pending += n,
                Some(ProjectionTaskStatus::InProgress) => s.in_progress += n,
                Some(ProjectionTaskStatus::Completed) => s.completed += n,
                Some(ProjectionTaskStatus::Failed) => s.failed += n,
                Some(ProjectionTaskStatus::DeadLetter) => s.dead_letter += n,
                None => {
                    return Err(SystemStoreError::InvalidInput(format!(
                        "unknown projection status '{status}' in clickhouse summary"
                    )));
                }
            }
        }
        Ok(s)
    }
}
