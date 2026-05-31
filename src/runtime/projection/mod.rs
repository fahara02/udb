//! # Projection Materialization Engine (U3)
//!
//! Keeps document/vector/graph/analytics/cache/object projections synchronized
//! from canonical Postgres writes. A durable task table (`udb_projection_tasks`)
//! provides at-least-once delivery with idempotent execution, retry, dead-letter
//! handling, and reconciliation.
//!
//! ## Components
//!
//! * [`ProjectionPlan`] – derived from [`CatalogManifest`], lists which
//!   backends must be updated for each message type.
//! * [`ProjectionEngine`] – lightweight enqueue-only handle (hold in service).
//! * [`ProjectionWorker`] – background loop: claims PENDING tasks, dispatches
//!   to backend executors, marks COMPLETED / FAILED / DEAD_LETTER.
//! * [`ReconciliationWorker`] – background loop: detects DEAD_LETTER tasks
//!   and re-enqueues them as PENDING (repair).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use sqlx::Postgres;
use uuid::Uuid;

use crate::generation::{CatalogManifest, ManifestStoreOption};
use crate::metrics::MetricsRecorder;
use crate::runtime::system::SystemCatalogConfig;

// ── Task status constants ─────────────────────────────────────────────────────

pub const TASK_PENDING: &str = "PENDING";
pub const TASK_IN_PROGRESS: &str = "IN_PROGRESS";
pub const TASK_COMPLETED: &str = "COMPLETED";
pub const TASK_FAILED: &str = "FAILED";
pub const TASK_DEAD_LETTER: &str = "DEAD_LETTER";

// ── ProjectionPlan ────────────────────────────────────────────────────────────

/// Pre-computed set of projection targets for one message type.
///
/// Built from a [`CatalogManifest`] and cached per manifest checksum.
/// Rebuilding is cheap (pure in-memory).
#[derive(Debug, Clone)]
pub struct ProjectionPlan {
    pub message_type: String,
    pub source_schema: String,
    pub source_table: String,
    /// Primary-key column names, in declaration order.
    pub primary_key_columns: Vec<String>,
    pub manifest_checksum: String,
    pub targets: Vec<ProjectionTarget>,
}

/// A single backend target derived from a [`ManifestProjection`].
#[derive(Debug, Clone)]
pub struct ProjectionTarget {
    pub projection_kind: String,
    pub backend: String,
    pub instance: String,
    pub resource_name: String,
    pub write_policy: String,
    pub fanout_policy: String,
    pub options: Vec<ManifestStoreOption>,
}

impl ProjectionPlan {
    /// Derive plans for every message type that has at least one projection.
    pub fn from_manifest(manifest: &CatalogManifest) -> Vec<ProjectionPlan> {
        let checksum = manifest.checksum_sha256.clone();
        manifest
            .tables
            .iter()
            .filter_map(|table| {
                // Collect projections: per-table projections take precedence;
                // fall back to manifest-level projections keyed to this message.
                let projections: Vec<&crate::generation::manifest::ManifestProjection> =
                    if !table.projections.is_empty() {
                        table.projections.iter().collect()
                    } else {
                        manifest
                            .projections
                            .iter()
                            .filter(|p| p.message_type == table.message_name)
                            .collect()
                    };
                if projections.is_empty() {
                    return None;
                }
                let targets: Vec<ProjectionTarget> = projections
                    .iter()
                    .filter(|p| should_materialize_projection(p))
                    .map(|p| ProjectionTarget {
                        projection_kind: p.projection_kind.clone(),
                        backend: p.backend.clone(),
                        instance: p.instance.clone(),
                        resource_name: p.resource_name.clone(),
                        write_policy: p.write_policy.clone(),
                        fanout_policy: p.fanout_policy.clone(),
                        options: p.options.clone(),
                    })
                    .collect();
                if targets.is_empty() {
                    return None;
                }
                Some(ProjectionPlan {
                    message_type: table.message_name.clone(),
                    source_schema: table.schema.clone(),
                    source_table: table.table.clone(),
                    primary_key_columns: table.primary_key.clone(),
                    manifest_checksum: checksum.clone(),
                    targets,
                })
            })
            .collect()
    }

    /// Extract primary-key values from a record payload.
    ///
    /// Returns the subset of `payload` whose keys match `primary_key_columns`,
    /// or the full payload when the primary key is unknown.
    pub fn extract_row_key(&self, payload: &serde_json::Value) -> serde_json::Value {
        if self.primary_key_columns.is_empty() {
            return payload.clone();
        }
        if let serde_json::Value::Object(map) = payload {
            let mut key = serde_json::Map::new();
            for col in &self.primary_key_columns {
                if let Some(val) = map.get(col) {
                    key.insert(col.clone(), val.clone());
                }
            }
            if !key.is_empty() {
                return serde_json::Value::Object(key);
            }
        }
        payload.clone()
    }
}

fn should_materialize_projection(p: &crate::generation::manifest::ManifestProjection) -> bool {
    let backend = normalize_backend(&p.backend);
    let policy = p.fanout_policy.to_ascii_lowercase();
    let write_policy = p.write_policy.to_ascii_lowercase();
    !(backend == "postgres"
        && p.projection_kind.eq_ignore_ascii_case("relational")
        && (policy.is_empty() || policy == "primary_only")
        && (write_policy.is_empty() || write_policy == "primary"))
}

// ── ProjectionEngine ──────────────────────────────────────────────────────────

/// Lightweight enqueue-only handle.
///
/// Hold an `Arc<ProjectionEngine>` in the service; pass pool + runtime to
/// [`ProjectionWorker`] for the background processing loop.
///
/// NW1-3b — the engine holds **two** handles:
///
/// - `pool: PgPool` is used only by [`Self::replay_by_primary_key`] /
///   [`Self::replay_range`] which scan the canonical Postgres source
///   tables to re-emit projection tasks for historical rows. Those
///   reads are inherently PG-coupled because they read tenant
///   tables (e.g. `udb.public.users`), not UDB system tables.
/// - `store: Arc<dyn SystemStores>` carries the system-table
///   surface: every projection-task INSERT and read goes through the
///   `ProjectionTaskStore` trait so the trait impl owns the DDL
///   dialect (PG `JSONB`, MySQL `JSON`, SQLite `TEXT`).
///
/// Once canonical writes can land on non-PG backends, the pool
/// field disappears; the replay functions become trait methods on
/// the canonical store.
#[derive(Clone)]
pub struct ProjectionEngine {
    pool: PgPool,
    store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    config: SystemCatalogConfig,
}

impl std::fmt::Debug for ProjectionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionEngine").finish_non_exhaustive()
    }
}

impl ProjectionEngine {
    pub fn new(
        pool: PgPool,
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
        config: SystemCatalogConfig,
    ) -> Self {
        Self {
            pool,
            store,
            config,
        }
    }

    /// Compute a deterministic idempotency key for one projection task.
    ///
    /// The SHA-256 hash uniquely identifies: which source row, which projection
    /// target, which operation, and which manifest version drove the write.
    /// Identical writes with the same manifest checksum produce the same key →
    /// `ON CONFLICT DO NOTHING` deduplicates re-enqueues.  A catalog upgrade
    /// produces a new checksum and forces every row to be re-projected.
    pub fn idempotency_key(
        project_id: &str,
        source_table: &str,
        source_row_key: &serde_json::Value,
        operation: &str,
        target_backend: &str,
        target_instance: &str,
        manifest_checksum: &str,
        source_checksum: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(project_id.as_bytes());
        hasher.update(b"|");
        hasher.update(source_table.as_bytes());
        hasher.update(b"|");
        hasher.update(source_row_key.to_string().as_bytes());
        hasher.update(b"|");
        hasher.update(operation.as_bytes());
        hasher.update(b"|");
        hasher.update(target_backend.as_bytes());
        hasher.update(b"|");
        hasher.update(target_instance.as_bytes());
        hasher.update(b"|");
        hasher.update(manifest_checksum.as_bytes());
        hasher.update(b"|");
        hasher.update(source_checksum.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn source_checksum(source_payload: &serde_json::Value) -> String {
        let mut hasher = Sha256::new();
        hasher.update(source_payload.to_string().as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Insert projection tasks inside the caller's PostgreSQL transaction.
    ///
    /// This is the normal write-path entry point.  Keeping task creation in the
    /// same transaction as the canonical mutation avoids the lost-task window
    /// that exists with post-commit fire-and-forget enqueue.
    pub async fn enqueue_write_tasks_tx(
        tx: &mut sqlx::Transaction<'_, Postgres>,
        config: &SystemCatalogConfig,
        project_id: &str,
        message_type: &str,
        operation: &str,
        source_payload: &serde_json::Value,
        plans: &[ProjectionPlan],
    ) -> Result<Vec<String>, String> {
        let mut task_keys = Vec::new();
        for plan in plans.iter().filter(|p| p.message_type == message_type) {
            let source_row_key = plan.extract_row_key(source_payload);
            let source_checksum = Self::source_checksum(source_payload);
            for target in &plan.targets {
                let idempotency_key = Self::idempotency_key(
                    project_id,
                    &plan.source_table,
                    &source_row_key,
                    operation,
                    &target.backend,
                    &target.instance,
                    &plan.manifest_checksum,
                    &source_checksum,
                );
                let task_id = insert_task_if_absent_on(
                    &mut **tx,
                    config,
                    &idempotency_key,
                    project_id,
                    &plan.manifest_checksum,
                    message_type,
                    &plan.source_schema,
                    &plan.source_table,
                    &source_row_key,
                    operation,
                    &target.backend,
                    &target.instance,
                    &target.projection_kind,
                    &target.resource_name,
                    &target.options,
                    source_payload,
                    &source_checksum,
                )
                .await?;
                task_keys.push(task_id);
            }
        }
        task_keys.sort();
        task_keys.dedup();
        Ok(task_keys)
    }

    /// Replay projection tasks from the canonical PostgreSQL row identified by
    /// its primary-key JSON object, for example `{ "id": "p1" }`.
    pub async fn replay_by_primary_key(
        &self,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        row_key: &serde_json::Value,
    ) -> Result<u64, String> {
        let rows = self
            .load_source_rows(manifest, message_type, Some(row_key), None, None, 1)
            .await?;
        self.enqueue_replay_rows(manifest, project_id, message_type, rows)
            .await
    }

    /// Replay projection tasks by first primary-key column range. Empty bounds
    /// are treated as open-ended. Values are compared through PostgreSQL text
    /// casts so the method can operate generically across scalar key types.
    pub async fn replay_range(
        &self,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        start: Option<&str>,
        end: Option<&str>,
        limit: i64,
    ) -> Result<u64, String> {
        let rows = self
            .load_source_rows(manifest, message_type, None, start, end, limit.max(1))
            .await?;
        self.enqueue_replay_rows(manifest, project_id, message_type, rows)
            .await
    }

    async fn enqueue_replay_rows(
        &self,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        rows: Vec<serde_json::Value>,
    ) -> Result<u64, String> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| format!("projection replay begin failed: {err}"))?;
        let plans = ProjectionPlan::from_manifest(manifest);
        let mut inserted = 0u64;
        for row in rows {
            let task_keys = Self::enqueue_write_tasks_tx(
                &mut tx,
                &self.config,
                project_id,
                message_type,
                "upsert",
                &row,
                &plans,
            )
            .await?;
            inserted += task_keys.len() as u64;
        }
        tx.commit()
            .await
            .map_err(|err| format!("projection replay commit failed: {err}"))?;
        Ok(inserted)
    }

    async fn load_source_rows(
        &self,
        manifest: &CatalogManifest,
        message_type: &str,
        row_key: Option<&serde_json::Value>,
        start: Option<&str>,
        end: Option<&str>,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, String> {
        let table = manifest
            .tables
            .iter()
            .find(|table| table.message_name == message_type)
            .ok_or_else(|| format!("unknown message_type {message_type}"))?;
        let mut predicates = Vec::new();
        let mut binds = Vec::new();
        if let Some(row_key) = row_key.and_then(serde_json::Value::as_object) {
            for (key, value) in row_key {
                if !table
                    .columns
                    .iter()
                    .any(|column| column.column_name == *key)
                {
                    return Err(format!("unknown primary-key field {key}"));
                }
                binds.push(json_scalar_to_string(value));
                predicates.push(format!("t.{}::text = ${}", qi(key), binds.len()));
            }
        } else if let Some(pk) = table.primary_key.first() {
            if let Some(start) = start {
                binds.push(start.to_string());
                predicates.push(format!("t.{}::text >= ${}", qi(pk), binds.len()));
            }
            if let Some(end) = end {
                binds.push(end.to_string());
                predicates.push(format!("t.{}::text <= ${}", qi(pk), binds.len()));
            }
        }
        let where_clause = if predicates.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", predicates.join(" AND "))
        };
        let sql = format!(
            "SELECT to_jsonb(t) AS payload FROM {}.{} AS t{} ORDER BY 1 LIMIT {}",
            qi(&table.schema),
            qi(&table.table),
            where_clause,
            limit.max(1)
        );
        let mut query = sqlx::query(&sql);
        for bind in binds {
            query = query.bind(bind);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|err| format!("projection replay source scan failed: {err}"))?;
        rows.into_iter()
            .map(|row| {
                use sqlx::Row;
                row.try_get("payload")
                    .map_err(|err| format!("projection replay source row decode failed: {err}"))
            })
            .collect()
    }

    /// Fire-and-forget projection task enqueue.
    ///
    /// Spawns a Tokio task so the write-path RPC is never blocked.  Any errors
    /// are logged as warnings; they do not affect the caller's response.
    pub fn enqueue_write_tasks_spawn(
        self: Arc<Self>,
        project_id: String,
        message_type: String,
        operation: String,
        source_payload: serde_json::Value,
        plans: Arc<Vec<ProjectionPlan>>,
    ) {
        tokio::spawn(async move {
            for plan in plans.iter().filter(|p| p.message_type == message_type) {
                let source_row_key = plan.extract_row_key(&source_payload);
                let source_checksum = Self::source_checksum(&source_payload);
                for target in &plan.targets {
                    let idempotency_key = Self::idempotency_key(
                        &project_id,
                        &plan.source_table,
                        &source_row_key,
                        &operation,
                        &target.backend,
                        &target.instance,
                        &plan.manifest_checksum,
                        &source_checksum,
                    );
                    if let Err(err) = self
                        .insert_task_if_absent(
                            &idempotency_key,
                            &project_id,
                            &plan.manifest_checksum,
                            &message_type,
                            &plan.source_schema,
                            &plan.source_table,
                            &source_row_key,
                            &operation,
                            &target.backend,
                            &target.instance,
                            &target.projection_kind,
                            &target.resource_name,
                            &target.options,
                            &source_payload,
                            &source_checksum,
                        )
                        .await
                    {
                        tracing::warn!(
                            error = %err,
                            message_type = %message_type,
                            backend = %target.backend,
                            "projection task enqueue failed",
                        );
                    }
                }
            }
        });
    }

    async fn insert_task_if_absent(
        &self,
        idempotency_key: &str,
        project_id: &str,
        manifest_checksum: &str,
        message_type: &str,
        source_schema: &str,
        source_table: &str,
        source_row_key: &serde_json::Value,
        operation: &str,
        target_backend: &str,
        target_instance: &str,
        projection_kind: &str,
        resource_name: &str,
        target_options: &[ManifestStoreOption],
        source_payload: &serde_json::Value,
        source_checksum: &str,
    ) -> Result<(), String> {
        // NW1-3b: route through ProjectionTaskStore. The trait
        // method is idempotent on `idempotency_key` — re-emission
        // with the same key returns the existing task_id without
        // creating a duplicate. Matches the pre-NW1-3b
        // `INSERT … ON CONFLICT DO NOTHING` semantics.
        use crate::runtime::canonical_store::system_store::{
            ProjectionOperation, ProjectionTaskInsert, ProjectionTaskStore,
        };
        let op = ProjectionOperation::parse(operation).ok_or_else(|| {
            format!("unknown projection operation '{operation}' (expected upsert|delete)")
        })?;
        let target_options_json =
            serde_json::to_value(target_options).unwrap_or(serde_json::Value::Array(vec![]));
        let insert = ProjectionTaskInsert {
            idempotency_key: idempotency_key.to_string(),
            project_id: project_id.to_string(),
            manifest_checksum: manifest_checksum.to_string(),
            message_type: message_type.to_string(),
            source_schema: source_schema.to_string(),
            source_table: source_table.to_string(),
            source_row_key: source_row_key.clone(),
            operation: op,
            target_backend: target_backend.to_string(),
            target_instance: target_instance.to_string(),
            projection_kind: projection_kind.to_string(),
            resource_name: resource_name.to_string(),
            target_options: target_options_json,
            source_payload: source_payload.clone(),
            source_checksum: source_checksum.to_string(),
        };
        // `config` is retained for replay paths (PG-canonical reads
        // below); unused on the trait path.
        let _ = &self.config;
        ProjectionTaskStore::enqueue_projection_task(self.store.as_ref(), &insert)
            .await
            .map(|_| ())
            .map_err(|err| format!("enqueue_projection_task: {err}"))
    }
}

#[allow(clippy::too_many_arguments)]
async fn insert_task_if_absent_on<'e, E>(
    executor: E,
    config: &SystemCatalogConfig,
    idempotency_key: &str,
    project_id: &str,
    manifest_checksum: &str,
    message_type: &str,
    source_schema: &str,
    source_table: &str,
    source_row_key: &serde_json::Value,
    operation: &str,
    target_backend: &str,
    target_instance: &str,
    projection_kind: &str,
    resource_name: &str,
    target_options: &[ManifestStoreOption],
    source_payload: &serde_json::Value,
    source_checksum: &str,
) -> Result<String, String>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    let rel = config.projection_tasks_relation();
    let target_options = serde_json::to_value(target_options).unwrap_or(serde_json::Value::Null);
    let sql = format!(
        "WITH inserted AS (
             INSERT INTO {rel}
             (idempotency_key, project_id, manifest_checksum, message_type,
              source_schema, source_table, source_row_key, operation, target_backend,
              target_instance, projection_kind, resource_name, target_options,
              source_payload, source_checksum)
             VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15)
             ON CONFLICT (idempotency_key) DO NOTHING
             RETURNING task_id
         )
         SELECT task_id::TEXT FROM inserted
         UNION ALL
         SELECT task_id::TEXT FROM {rel} WHERE idempotency_key = $1
         LIMIT 1"
    );
    sqlx::query_scalar::<_, String>(&sql)
        .bind(idempotency_key)
        .bind(project_id)
        .bind(manifest_checksum)
        .bind(message_type)
        .bind(source_schema)
        .bind(source_table)
        .bind(source_row_key.to_string())
        .bind(operation)
        .bind(target_backend)
        .bind(target_instance)
        .bind(projection_kind)
        .bind(resource_name)
        .bind(target_options.to_string())
        .bind(source_payload.to_string())
        .bind(source_checksum)
        .fetch_one(executor)
        .await
        .map_err(|err| format!("insert projection task: {err}"))
}

// ── ProjectionWorker ──────────────────────────────────────────────────────────

/// Settings for the projection worker, populated from environment variables.
#[derive(Debug, Clone)]
pub struct ProjectionWorkerSettings {
    pub enabled: bool,
    pub poll_interval_secs: u64,
    pub batch_size: i64,
    pub max_retries: i32,
}

impl Default for ProjectionWorkerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            batch_size: 50,
            max_retries: 5,
        }
    }
}

impl ProjectionWorkerSettings {
    pub fn from_env() -> Self {
        let mut s = Self::default();
        if let Ok(val) = std::env::var("UDB_PROJECTION_ENABLED") {
            s.enabled = !matches!(
                val.to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            );
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_POLL_INTERVAL_SECS") {
            if let Ok(n) = val.parse::<u64>() {
                s.poll_interval_secs = n.max(1);
            }
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_BATCH_SIZE") {
            if let Ok(n) = val.parse::<i64>() {
                s.batch_size = n.max(1);
            }
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_MAX_RETRIES") {
            if let Ok(n) = val.parse::<i32>() {
                s.max_retries = n.max(0);
            }
        }
        s
    }
}

/// Background worker that polls `udb_projection_tasks`, dispatches each task
/// to the appropriate backend executor, and records completion or failure.
pub struct ProjectionWorker {
    /// NW1-3b: replaced raw `PgPool` with `Arc<dyn SystemStores>`.
    store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    runtime: crate::runtime::DataBrokerRuntime,
    config: SystemCatalogConfig,
    settings: ProjectionWorkerSettings,
    metrics: Arc<dyn MetricsRecorder>,
}

impl ProjectionWorker {
    pub fn new(
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
        runtime: crate::runtime::DataBrokerRuntime,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            store,
            runtime,
            config: SystemCatalogConfig::current(),
            settings: ProjectionWorkerSettings::from_env(),
            metrics,
        }
    }

    /// Whether the worker is enabled (checks env at construction time).
    pub fn is_enabled() -> bool {
        ProjectionWorkerSettings::from_env().enabled
    }

    /// Run the worker loop forever.  Call from a dedicated `tokio::spawn`.
    pub async fn run_forever(self) {
        let interval = Duration::from_secs(self.settings.poll_interval_secs.max(1));
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let (completed, failed) = self.run_once().await;
            if completed + failed > 0 {
                tracing::info!(completed, failed, "projection worker pass");
            }
        }
    }

    /// Execute one pass: claim up to `batch_size` pending tasks and process them.
    ///
    /// NW1-3b: routes through `ProjectionTaskStore::claim_projection_tasks`
    /// + `mark_projection_task_completed` / `mark_projection_task_failed`.
    /// The pre-NW1-3b inline PG `UPDATE ... RETURNING` is replaced by
    /// the trait's atomic claim which uses the same `FOR UPDATE SKIP
    /// LOCKED` pattern on PG and the dialect-equivalent on MySQL /
    /// SQLite.
    ///
    /// Returns `(completed_count, failed_count)`.
    pub async fn run_once(&self) -> (usize, usize) {
        use crate::runtime::canonical_store::system_store::{
            ProjectionClaimFilter, ProjectionTaskStatus, ProjectionTaskStore,
        };
        self.refresh_pending_metrics().await;
        let filter = ProjectionClaimFilter {
            batch_size: self.settings.batch_size,
            max_retries: self.settings.max_retries,
            target_backend: None,
            target_instance: None,
        };
        let claimed =
            match ProjectionTaskStore::claim_projection_tasks(self.store.as_ref(), &filter).await {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::warn!(error = %err, "projection worker: failed to claim tasks");
                    return (0, 0);
                }
            };

        let mut completed = 0usize;
        let mut failed = 0usize;

        for row in &claimed {
            let task_id = row.task_id;
            let target_backend = row.target_backend.clone();
            let target_instance = row.target_instance.clone();
            let projection_kind = row.projection_kind.clone();
            let resource_name = row.resource_name.clone();
            let operation = row.operation.as_str().to_string();
            let source_row_key = row.source_row_key.clone();
            let target_options = row.target_options.clone();
            let source_payload = row.source_payload.clone();
            let retry_count = row.retry_count;
            let created_at = Some(row.created_at);

            let instance_opt = if target_instance.is_empty() {
                None
            } else {
                Some(target_instance.as_str())
            };

            let result = self
                .execute_task(
                    &target_backend,
                    instance_opt,
                    &projection_kind,
                    &resource_name,
                    &operation,
                    &source_row_key,
                    &target_options,
                    &source_payload,
                )
                .await;

            match result {
                Ok(_) => {
                    let _ = self.mark_completed(task_id).await;
                    self.metrics.inc_projection_tasks_completed_total(
                        &target_backend,
                        &target_instance,
                        &projection_kind,
                    );
                    if let Some(created_at) = created_at {
                        self.metrics.observe_projection_lag_seconds(
                            &target_backend,
                            &target_instance,
                            &projection_kind,
                            (Utc::now() - created_at)
                                .to_std()
                                .map(|duration| duration.as_secs_f64())
                                .unwrap_or(0.0),
                        );
                    }
                    completed += 1;
                }
                Err(err) => {
                    let new_retry = retry_count + 1;
                    let new_status = if new_retry >= self.settings.max_retries {
                        ProjectionTaskStatus::DeadLetter
                    } else {
                        ProjectionTaskStatus::Failed
                    };
                    let _ = self.mark_failed(task_id, new_retry, new_status, &err).await;
                    self.metrics.inc_projection_tasks_failed_total(
                        &target_backend,
                        &target_instance,
                        &projection_kind,
                    );
                    tracing::warn!(
                        task_id = %task_id,
                        backend = %target_backend,
                        retry = new_retry,
                        error = %err,
                        "projection task failed",
                    );
                    failed += 1;
                }
            }
        }

        self.refresh_pending_metrics().await;
        (completed, failed)
    }

    async fn refresh_pending_metrics(&self) {
        // NW1-3b: routes through `pending_task_metrics`. The aggregate
        // shape (group by project + backend + instance + kind, count
        // + oldest_age_seconds, LIMIT 500) is identical to the
        // pre-NW1-3b inline SQL; each store impl emits dialect-correct
        // SQL.
        use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
        let metrics =
            match ProjectionTaskStore::pending_task_metrics(self.store.as_ref(), 500).await {
                Ok(m) => m,
                Err(_) => return,
            };
        let _ = &self.config; // retained for replay paths
        for m in metrics {
            let project = if m.project_id.is_empty() {
                "default".to_string()
            } else {
                m.project_id
            };
            self.metrics.set_projection_tasks_pending(
                &m.target_backend,
                &m.target_instance,
                &m.projection_kind,
                m.pending,
            );
            self.metrics.set_projection_oldest_pending_age_seconds(
                &project,
                &m.target_backend,
                &m.target_instance,
                &m.projection_kind,
                m.oldest_age_seconds,
            );
        }
    }

    async fn execute_task(
        &self,
        backend: &str,
        instance: Option<&str>,
        projection_kind: &str,
        resource_name: &str,
        operation: &str,
        source_row_key: &serde_json::Value,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
    ) -> Result<(), String> {
        let normalized_backend = normalize_backend(backend);
        if normalized_backend == "redis" || projection_kind.eq_ignore_ascii_case("cache") {
            return self
                .execute_redis_projection(
                    resource_name,
                    operation,
                    source_row_key,
                    target_options,
                    source_payload,
                )
                .await;
        }
        if normalized_backend == "s3"
            || normalized_backend == "minio"
            || projection_kind.eq_ignore_ascii_case("object")
        {
            return self
                .execute_object_projection(
                    &normalized_backend,
                    instance,
                    resource_name,
                    operation,
                    source_row_key,
                    target_options,
                    source_payload,
                )
                .await;
        }

        let request = render_projection_mutation(
            &normalized_backend,
            projection_kind,
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
        )?;
        self.runtime
            .mutate_backend_target(&normalized_backend, instance, &request.to_string())
            .await
            .map(|_| ())
            .map_err(|s| s.message().to_string())
    }

    async fn execute_redis_projection(
        &self,
        resource_name: &str,
        operation: &str,
        source_row_key: &serde_json::Value,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
    ) -> Result<(), String> {
        let ttl = option_value(target_options, "ttl_seconds")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300);
        let pattern = option_value(target_options, "key_pattern")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| resource_name.to_string());
        let key = render_key_pattern(&pattern, source_row_key, source_payload);
        if operation.eq_ignore_ascii_case("delete") {
            self.runtime.cache_delete_pattern(&key).await?;
            return Ok(());
        }
        let bytes = serde_json::to_vec(source_payload).map_err(|err| err.to_string())?;
        self.runtime.cache_set(&key, &[bytes], ttl).await
    }

    async fn execute_object_projection(
        &self,
        backend: &str,
        instance: Option<&str>,
        resource_name: &str,
        operation: &str,
        source_row_key: &serde_json::Value,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
    ) -> Result<(), String> {
        let key_prefix = option_value(target_options, "key_prefix").unwrap_or_default();
        let id = row_identity(source_row_key, source_payload, target_options)?;
        let object_key = if key_prefix.trim().is_empty() {
            format!("{id}.json")
        } else {
            format!("{}/{}.json", key_prefix.trim_matches('/'), id)
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "operation": operation,
            "source_row_key": source_row_key,
            "payload": source_payload,
        }))
        .map_err(|err| err.to_string())?;
        let request = serde_json::json!({
            "bucket": resource_name,
            "object_key": object_key,
            "content_type": "application/json",
        });
        self.runtime
            .put_object_backend_target(backend, instance, &request.to_string(), body)
            .await
            .map(|_| ())
            .map_err(|s| s.message().to_string())
    }

    async fn mark_completed(&self, task_id: Uuid) -> Result<(), String> {
        use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
        ProjectionTaskStore::mark_projection_task_completed(self.store.as_ref(), task_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn mark_failed(
        &self,
        task_id: Uuid,
        retry_count: i32,
        new_status: crate::runtime::canonical_store::system_store::ProjectionTaskStatus,
        error: &str,
    ) -> Result<(), String> {
        use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
        ProjectionTaskStore::mark_projection_task_failed(
            self.store.as_ref(),
            task_id,
            retry_count,
            new_status,
            error,
        )
        .await
        .map_err(|e| e.to_string())
    }
}

fn normalize_backend(backend: &str) -> String {
    match backend.to_ascii_lowercase().as_str() {
        "pg" | "postgresql" => "postgres".to_string(),
        "mongo" => "mongodb".to_string(),
        "minio" => "s3".to_string(),
        other => other.to_string(),
    }
}

fn qi(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn option_value(options: &serde_json::Value, key: &str) -> Option<String> {
    options.as_array()?.iter().find_map(|entry| {
        let entry_key = entry.get("key").and_then(serde_json::Value::as_str)?;
        if entry_key.eq_ignore_ascii_case(key) {
            entry
                .get("value")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string)
        } else {
            None
        }
    })
}

fn row_identity(
    source_row_key: &serde_json::Value,
    source_payload: &serde_json::Value,
    target_options: &serde_json::Value,
) -> Result<String, String> {
    for key in [
        "id_field",
        "point_id_field",
        "document_id_field",
        "primary_key",
        "partition_key",
    ] {
        if let Some(field) = option_value(target_options, key)
            && let Some(value) = source_payload
                .get(&field)
                .or_else(|| source_row_key.get(&field))
        {
            return Ok(json_scalar_to_string(value));
        }
    }
    if let Some(obj) = source_row_key.as_object() {
        if obj.len() == 1 {
            if let Some(value) = obj.values().next() {
                return Ok(json_scalar_to_string(value));
            }
        }
        if !obj.is_empty() {
            return Ok(source_row_key.to_string());
        }
    }
    for field in ["id", "uuid", "key"] {
        if let Some(value) = source_payload.get(field) {
            return Ok(json_scalar_to_string(value));
        }
    }
    Err("projection task cannot determine source row identity".to_string())
}

fn json_scalar_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn render_key_pattern(
    pattern: &str,
    source_row_key: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> String {
    let mut rendered = pattern.to_string();
    for object in [source_payload, source_row_key] {
        if let Some(map) = object.as_object() {
            for (key, value) in map {
                rendered = rendered.replace(&format!("{{{key}}}"), &json_scalar_to_string(value));
                rendered = rendered.replace(&format!(":${key}"), &json_scalar_to_string(value));
            }
        }
    }
    rendered
}

fn render_projection_mutation(
    backend: &str,
    projection_kind: &str,
    resource_name: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    match backend {
        "mongodb" => render_mongodb_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
        ),
        "qdrant" => render_qdrant_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
        ),
        "neo4j" => render_neo4j_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
        ),
        "clickhouse" => render_clickhouse_projection(resource_name, operation, source_payload),
        "postgres" => render_postgres_projection(resource_name, operation, source_payload),
        other => Err(format!(
            "projection backend '{other}' is not supported for kind '{projection_kind}'"
        )),
    }
}

fn render_mongodb_projection(
    collection: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id_field = option_value(target_options, "id_field")
        .or_else(|| option_value(target_options, "partition_key"))
        .unwrap_or_else(|| "id".to_string());
    if operation.eq_ignore_ascii_case("delete") {
        let filter = if let Some(map) = source_payload.as_object()
            && !map.is_empty()
        {
            serde_json::Value::Object(map.clone())
        } else {
            let id = row_identity(source_row_key, source_payload, target_options)?;
            serde_json::json!({ id_field: id })
        };
        return Ok(serde_json::json!({
            "operation": "delete",
            "collection": collection,
            "filter": filter,
        }));
    }
    let id = row_identity(source_row_key, source_payload, target_options)?;
    let filter = serde_json::json!({ id_field.clone(): id });
    let mut document = source_payload.clone();
    if let serde_json::Value::Object(map) = &mut document {
        map.entry(id_field).or_insert(serde_json::Value::String(id));
    }
    Ok(serde_json::json!({
        "operation": "upsert",
        "collection": collection,
        "filter": filter,
        "document": document,
    }))
}

fn render_qdrant_projection(
    collection: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let id = row_identity(source_row_key, source_payload, target_options)?;
    if operation.eq_ignore_ascii_case("delete") {
        return Ok(serde_json::json!({
            "operation": "delete",
            "collection": collection,
            "point_ids": [id],
        }));
    }
    let vector = option_value(target_options, "vector_field")
        .or_else(|| option_value(target_options, "embedding_field"))
        .and_then(|field| source_payload.get(&field).cloned())
        .or_else(|| source_payload.get("vector").cloned())
        .or_else(|| source_payload.get("embedding").cloned())
        .or_else(|| source_payload.get("embeddings").cloned())
        .ok_or_else(|| {
            "vector projection requires a vector_field/embedding_field option or vector payload field"
                .to_string()
        })?;
    Ok(serde_json::json!({
        "operation": "upsert",
        "collection": collection,
        "points": [{
            "id": id,
            "vector": vector,
            "payload": source_payload,
        }],
    }))
}

fn render_neo4j_projection(
    resource_name: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    let label = option_value(target_options, "node_label")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| resource_name.to_string());
    let id = row_identity(source_row_key, source_payload, target_options)?;
    if operation.eq_ignore_ascii_case("delete") {
        return Ok(serde_json::json!({
            "operation": "delete_node",
            "label": label,
            "id": id,
        }));
    }
    Ok(serde_json::json!({
        "operation": "create_node",
        "label": label,
        "id": id,
        "properties": source_payload,
    }))
}

fn render_clickhouse_projection(
    table: &str,
    operation: &str,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if operation.eq_ignore_ascii_case("delete") {
        return Err(
            "ClickHouse projection deletes require a replacing/collapsing table policy".to_string(),
        );
    }
    Ok(serde_json::json!({
        "table": table,
        "rows": [source_payload],
    }))
}

fn render_postgres_projection(
    resource_name: &str,
    _operation: &str,
    _source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    Err(format!(
        "postgres projection target '{resource_name}' must be handled by the canonical write path"
    ))
}

// ── ReconciliationWorker ──────────────────────────────────────────────────────

/// Settings for the reconciliation worker.
#[derive(Debug, Clone)]
pub struct ReconciliationSettings {
    pub enabled: bool,
    pub interval_secs: u64,
    pub stale_in_progress_secs: u64,
    pub max_source_scan_rows: i64,
}

impl Default for ReconciliationSettings {
    fn default() -> Self {
        // Off by default; operator must explicitly enable.
        Self {
            enabled: false,
            interval_secs: 3600,
            stale_in_progress_secs: 900,
            max_source_scan_rows: 500,
        }
    }
}

impl ReconciliationSettings {
    pub fn from_env() -> Self {
        let mut s = Self::default();
        if let Ok(val) = std::env::var("UDB_PROJECTION_RECONCILE_ENABLED") {
            s.enabled = !matches!(
                val.to_ascii_lowercase().as_str(),
                "0" | "false" | "no" | "off"
            );
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_RECONCILE_INTERVAL_SECS") {
            if let Ok(n) = val.parse::<u64>() {
                s.interval_secs = n.max(60);
            }
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_STALE_IN_PROGRESS_SECS") {
            if let Ok(n) = val.parse::<u64>() {
                s.stale_in_progress_secs = n.max(60);
            }
        }
        if let Ok(val) = std::env::var("UDB_PROJECTION_RECONCILE_MAX_SOURCE_ROWS") {
            if let Ok(n) = val.parse::<i64>() {
                s.max_source_scan_rows = n.max(1);
            }
        }
        s
    }
}

/// Report produced by one reconciliation pass.
#[derive(Debug, Clone)]
pub struct ReconciliationReport {
    pub source_table: String,
    pub target_backend: String,
    pub target_instance: String,
    pub dead_letter_count: i64,
    pub repair_tasks_enqueued: i64,
}

/// Background worker that detects DEAD_LETTER tasks and re-enqueues them as
/// PENDING (repair).
pub struct ReconciliationWorker {
    /// NW1-3b: PG pool retained ONLY for `replay_source_rows_for_repair`
    /// which scans canonical tenant tables (PG-specific). All
    /// projection-task reads/writes go through the trait.
    pool: PgPool,
    store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    config: SystemCatalogConfig,
    settings: ReconciliationSettings,
    metrics: Arc<dyn MetricsRecorder>,
    manifest: Arc<CatalogManifest>,
    project_id: String,
}

impl ReconciliationWorker {
    pub fn new(
        pool: PgPool,
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
        metrics: Arc<dyn MetricsRecorder>,
        manifest: Arc<CatalogManifest>,
        project_id: String,
    ) -> Self {
        Self {
            pool,
            store,
            config: SystemCatalogConfig::current(),
            settings: ReconciliationSettings::from_env(),
            metrics,
            manifest,
            project_id,
        }
    }

    pub fn is_enabled() -> bool {
        ReconciliationSettings::from_env().enabled
    }

    pub async fn run_forever(self) {
        let interval = Duration::from_secs(self.settings.interval_secs);
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let reports = self.run_once().await;
            for r in &reports {
                tracing::info!(
                    source_table = %r.source_table,
                    backend = %r.target_backend,
                    dead_letter = %r.dead_letter_count,
                    repaired = %r.repair_tasks_enqueued,
                    "projection reconciliation pass",
                );
            }
        }
    }

    /// One reconciliation pass: reset DEAD_LETTER tasks to PENDING.
    ///
    /// NW1-3b: routes through `reset_stale_in_progress_tasks` +
    /// `dead_letter_groups` + `requeue_dead_letter_by_source`. The
    /// `replay_source_rows_for_repair` step stays PG-coupled because
    /// it reads canonical tenant tables.
    ///
    /// Returns one report per (source_table, target_backend, target_instance)
    /// combination that had dead-letter tasks.
    pub async fn run_once(&self) -> Vec<ReconciliationReport> {
        use crate::runtime::canonical_store::system_store::ProjectionTaskStore;
        let mut reports = Vec::new();
        // 1. Reset stale IN_PROGRESS → PENDING.
        match ProjectionTaskStore::reset_stale_in_progress_tasks(
            self.store.as_ref(),
            Duration::from_secs(self.settings.stale_in_progress_secs),
        )
        .await
        {
            Ok(repaired) if repaired > 0 => {
                tracing::info!(repaired, "projection reconciliation reset stale tasks");
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, "stale in-progress reset failed");
            }
        }
        let _ = &self.config; // PG-coupled replay below uses it
        // 2. Replay canonical source rows (PG-only).
        self.replay_source_rows_for_repair().await;

        // 3. Find all (source_table, target_backend, target_instance)
        // groups with dead-letter tasks.
        let groups = match ProjectionTaskStore::dead_letter_groups(self.store.as_ref(), 500).await {
            Ok(g) => g,
            Err(err) => {
                tracing::warn!(error = %err, "reconciliation scan failed");
                return reports;
            }
        };

        for group in groups {
            let source_table = group.source_table.clone();
            let target_backend = group.target_backend.clone();
            let target_instance = group.target_instance.clone();
            let dead_count = group.dead_count;

            // 4. Per-group requeue: DEAD_LETTER → PENDING with
            // retry_count = 0.
            let repaired = match ProjectionTaskStore::requeue_dead_letter_by_source(
                self.store.as_ref(),
                &source_table,
                &target_backend,
                &target_instance,
            )
            .await
            {
                Ok(n) => n,
                Err(err) => {
                    tracing::warn!(error = %err, "reconciliation repair failed");
                    0
                }
            };
            for _ in 0..repaired {
                self.metrics
                    .inc_projection_reconciliation_repairs_total(&target_backend, &target_instance);
            }

            reports.push(ReconciliationReport {
                source_table,
                target_backend,
                target_instance,
                dead_letter_count: dead_count,
                repair_tasks_enqueued: repaired,
            });
        }

        reports
    }

    async fn replay_source_rows_for_repair(&self) {
        let engine =
            ProjectionEngine::new(self.pool.clone(), self.store.clone(), self.config.clone());
        let mut seen = std::collections::BTreeSet::new();
        for plan in ProjectionPlan::from_manifest(&self.manifest) {
            if !seen.insert(plan.message_type.clone()) {
                continue;
            }
            match engine
                .replay_range(
                    &self.manifest,
                    &self.project_id,
                    &plan.message_type,
                    None,
                    None,
                    self.settings.max_source_scan_rows,
                )
                .await
            {
                Ok(inserted) if inserted > 0 => {
                    tracing::info!(
                        message_type = %plan.message_type,
                        inserted,
                        "projection reconciliation enqueued missing or stale source rows",
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(
                        message_type = %plan.message_type,
                        error = %err,
                        "projection reconciliation source replay failed",
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestProjection, ManifestStoreOption, ManifestTable};
    use serde_json::json;

    fn opt(key: &str, value: &str) -> ManifestStoreOption {
        ManifestStoreOption {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn projection_plan_skips_primary_relational_owner() {
        let manifest = CatalogManifest {
            checksum_sha256: "catalog1".to_string(),
            tables: vec![ManifestTable {
                message_name: "Patient".to_string(),
                schema: "public".to_string(),
                table: "patients".to_string(),
                primary_key: vec!["id".to_string()],
                projections: vec![
                    ManifestProjection {
                        message_type: "Patient".to_string(),
                        projection_kind: "relational".to_string(),
                        backend: "postgres".to_string(),
                        resource_name: "public.patients".to_string(),
                        write_policy: "primary".to_string(),
                        fanout_policy: "primary_only".to_string(),
                        write_owner: true,
                        ..ManifestProjection::default()
                    },
                    ManifestProjection {
                        message_type: "Patient".to_string(),
                        projection_kind: "document".to_string(),
                        backend: "mongodb".to_string(),
                        resource_name: "patients".to_string(),
                        write_policy: "projection".to_string(),
                        fanout_policy: "async_projection".to_string(),
                        options: vec![opt("id_field", "id")],
                        ..ManifestProjection::default()
                    },
                ],
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        };
        let plans = ProjectionPlan::from_manifest(&manifest);
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].targets.len(), 1);
        assert_eq!(plans[0].targets[0].backend, "mongodb");
    }

    #[test]
    fn renders_backend_specific_projection_mutations() {
        let options =
            json!([{"key":"id_field","value":"id"},{"key":"node_label","value":"Patient"}]);
        let payload = json!({"id":"p1","name":"Ada","vector":[0.1,0.2]});
        let key = json!({"id":"p1"});

        let mongo = render_projection_mutation(
            "mongodb", "document", "patients", "upsert", &key, &options, &payload,
        )
        .unwrap();
        assert_eq!(mongo["operation"], "upsert");
        assert_eq!(mongo["collection"], "patients");
        assert_eq!(mongo["filter"]["id"], "p1");

        let qdrant = render_projection_mutation(
            "qdrant",
            "vector",
            "patient_vectors",
            "upsert",
            &key,
            &options,
            &payload,
        )
        .unwrap();
        assert_eq!(qdrant["collection"], "patient_vectors");
        assert_eq!(qdrant["points"][0]["id"], "p1");

        let neo4j = render_projection_mutation(
            "neo4j", "graph", "Patient", "delete", &key, &options, &payload,
        )
        .unwrap();
        assert_eq!(neo4j["operation"], "delete_node");
        assert_eq!(neo4j["label"], "Patient");
    }

    #[test]
    fn idempotency_key_changes_when_source_checksum_changes() {
        let key = json!({"id":"p1"});
        let first = ProjectionEngine::idempotency_key(
            "tenant",
            "patients",
            &key,
            "upsert",
            "mongodb",
            "default",
            "catalog1",
            &ProjectionEngine::source_checksum(&json!({"id":"p1","name":"Ada"})),
        );
        let second = ProjectionEngine::idempotency_key(
            "tenant",
            "patients",
            &key,
            "upsert",
            "mongodb",
            "default",
            "catalog1",
            &ProjectionEngine::source_checksum(&json!({"id":"p1","name":"Grace"})),
        );
        assert_ne!(first, second);
    }
}
