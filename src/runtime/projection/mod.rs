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
//! * [`ReconciliationWorker`] – OPT-IN background loop (off by default; enable
//!   via `ReconciliationSettings.enabled`): detects DEAD_LETTER tasks and
//!   re-enqueues them as PENDING (repair). With it disabled — the default —
//!   a task that exhausts `max_retries` stays DEAD_LETTER and is NOT repaired,
//!   so operators relying on automatic repair must turn it on explicitly (F-8).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use sqlx::Postgres;
use uuid::Uuid;

use crate::generation::{CatalogManifest, ManifestStoreOption};
use crate::metrics::MetricsRecorder;
use crate::runtime::catalog::CatalogManager;
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
    ///
    /// #213: memoized by manifest checksum. `from_manifest` is called on every
    /// write enqueue (tx_object, setup_data, the projection worker), but the
    /// plan set only changes when the manifest does. A single-entry cache keyed
    /// by `checksum_sha256` skips the per-call table/projection rebuild; the
    /// cache holds one entry (the current manifest) so it cannot grow.
    pub fn from_manifest(manifest: &CatalogManifest) -> Vec<ProjectionPlan> {
        static PLAN_CACHE: std::sync::Mutex<Option<(String, std::sync::Arc<Vec<ProjectionPlan>>)>> =
            std::sync::Mutex::new(None);
        let checksum = manifest.checksum_sha256.clone();
        if !checksum.is_empty()
            && let Ok(guard) = PLAN_CACHE.lock()
            && let Some((cached_sum, plans)) = guard.as_ref()
            && *cached_sum == checksum
        {
            return (**plans).clone();
        }
        let plans = Self::build_from_manifest(manifest);
        if !checksum.is_empty()
            && let Ok(mut guard) = PLAN_CACHE.lock()
        {
            *guard = Some((checksum, std::sync::Arc::new(plans.clone())));
        }
        plans
    }

    /// Pure builder for [`from_manifest`] (un-memoized).
    fn build_from_manifest(manifest: &CatalogManifest) -> Vec<ProjectionPlan> {
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
                            .filter(|p| message_type_matches(&p.message_type, &table.message_name))
                            .collect()
                    };
                if projections.is_empty() {
                    return None;
                }
                let tenant_column = crate::generation::sql::resolve_tenant_column(table);
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
                        options: with_tenant_field(&p.options, tenant_column),
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

/// Name the source row's tenant column on the target as `tenant_field`, so the
/// worker can stamp the projected record's `_tenant_id` from the canonical row.
/// The worker runs with no request context, and the row is the only tenant
/// source that is identical on the live-write path and on replay: the planner
/// refuses an upsert whose tenant column differs from the caller's verified
/// tenant, and replay reads the same column back. An explicitly declared
/// `tenant_field` (graph/document store options) wins.
fn with_tenant_field(
    options: &[ManifestStoreOption],
    tenant_column: Option<&str>,
) -> Vec<ManifestStoreOption> {
    let mut options = options.to_vec();
    let declared = options
        .iter()
        .any(|o| o.key.eq_ignore_ascii_case("tenant_field") && !o.value.trim().is_empty());
    if !declared && let Some(column) = tenant_column {
        options.retain(|o| !o.key.eq_ignore_ascii_case("tenant_field"));
        options.push(ManifestStoreOption {
            key: "tenant_field".to_string(),
            value: column.to_string(),
        });
    }
    options
}

/// The project a projection task belongs to: the writer's project, with an
/// empty one resolved to the default project the same way catalog lookup does.
/// The worker validates and routes each task by this value, and stamps it on
/// vector and graph records as `_project_id`.
pub fn task_project_id(project_id: &str) -> &str {
    let trimmed = project_id.trim();
    if trimmed.is_empty() {
        crate::runtime::catalog::DEFAULT_PROJECT_ID
    } else {
        trimmed
    }
}

/// Tenant/project a projected record is stamped with. Search-side scoping
/// (`VectorSearch`, the IR compilers) filters on the `_tenant_id` /
/// `_project_id` system fields, so a record projected without them is
/// invisible to every tenant-scoped read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProjectionScope {
    tenant_id: Option<String>,
    project_id: Option<String>,
    /// The `tenant_field` the target declares but whose value this record does
    /// not carry as a plain scalar (a delete filter using an operator, say).
    unresolved_tenant_field: Option<String>,
}

impl ProjectionScope {
    fn resolve(
        project_id: &str,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
    ) -> Self {
        let tenant_field = option_value(target_options, "tenant_field")
            .map(|field| field.trim().to_string())
            .filter(|field| !field.is_empty());
        let tenant_id = tenant_field
            .as_deref()
            .and_then(|field| source_payload.get(field))
            .filter(|value| value.is_string() || value.is_number())
            .map(json_scalar_to_string)
            .filter(|value| !value.trim().is_empty());
        let project_id = Some(project_id.trim().to_string()).filter(|p| !p.is_empty());
        Self {
            unresolved_tenant_field: tenant_field.filter(|_| tenant_id.is_none()),
            tenant_id,
            project_id,
        }
    }

    /// Graph records are KEYED by the scope, so dropping an unresolved tenant
    /// would widen a delete (or an edge's endpoint match) to every tenant in
    /// the project. Refuse instead.
    fn require_tenant(&self, what: &str) -> Result<(), String> {
        match &self.unresolved_tenant_field {
            Some(field) => Err(format!(
                "{what}: tenant field '{field}' has no scalar value in the projected record; refusing to write it outside its tenant scope"
            )),
            None => Ok(()),
        }
    }

    /// `{_tenant_id, _project_id}` for whichever of the two is known.
    fn fields(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut fields = serde_json::Map::new();
        if let Some(tenant_id) = &self.tenant_id {
            fields.insert("_tenant_id".to_string(), tenant_id.clone().into());
        }
        if let Some(project_id) = &self.project_id {
            fields.insert("_project_id".to_string(), project_id.clone().into());
        }
        fields
    }

    /// The payload with the scope fields stamped over it. The stamp overwrites:
    /// a source column that happens to be named `_tenant_id` must not let a row
    /// claim a tenant other than the one its tenant column carries.
    fn stamp(&self, payload: &serde_json::Value) -> serde_json::Value {
        let mut stamped = payload.clone();
        if let serde_json::Value::Object(map) = &mut stamped {
            map.extend(self.fields());
        }
        stamped
    }

    /// `key` namespaced by the scope: `t:{tenant}/p:{project}/{key}`, each
    /// segment present only when known. Key-addressed targets (Redis keys,
    /// object keys, vector point ids) use it so two tenants — or two projects —
    /// whose rows share a primary key never overwrite or delete each other's
    /// record. A `/` inside a tenant or project id is escaped so a crafted id
    /// cannot forge another scope's prefix.
    fn scoped_key(&self, key: &str) -> String {
        let escape = |value: &str| value.replace('%', "%25").replace('/', "%2F");
        let mut scoped = String::new();
        if let Some(tenant_id) = &self.tenant_id {
            scoped.push_str("t:");
            scoped.push_str(&escape(tenant_id));
            scoped.push('/');
        }
        if let Some(project_id) = &self.project_id {
            scoped.push_str("p:");
            scoped.push_str(&escape(project_id));
            scoped.push('/');
        }
        scoped.push_str(key);
        scoped
    }
}

/// The source payload a projected DELETE carries: the caller's filter with its
/// keys resolved to physical column names (what projected rows are keyed by),
/// and the table's tenant column set to the VERIFIED tenant the delete ran
/// under. The raw filter alone may omit the tenant (it was enforced by the
/// write path's own predicate) or carry it as an operator, and then the worker
/// cannot resolve which tenant's projected record to remove — it must refuse
/// rather than widen the delete to every tenant.
pub(crate) fn scoped_delete_payload(
    manifest: &CatalogManifest,
    message_type: &str,
    filter: &serde_json::Value,
    verified_tenant_id: &str,
) -> serde_json::Value {
    let Some(table) = manifest
        .tables
        .iter()
        .find(|table| message_type_matches(&table.message_name, message_type))
    else {
        return filter.clone();
    };
    let mut payload = crate::planning::broker::normalize_filter_keys(
        &crate::planning::broker::column_resolver(table),
        filter,
    );
    let tenant_id = verified_tenant_id.trim();
    if let (Some(tenant_column), serde_json::Value::Object(map)) = (
        crate::generation::sql::resolve_tenant_column(table),
        &mut payload,
    ) && !tenant_id.is_empty()
    {
        map.insert(
            tenant_column.to_string(),
            serde_json::Value::String(tenant_id.to_string()),
        );
    }
    payload
}

/// Backends [`render_projection_mutation`] renders a mutation for.
const RENDERED_PROJECTION_BACKENDS: [&str; 4] = ["mongodb", "qdrant", "neo4j", "clickhouse"];

/// Whether the projection worker can materialize `projection` — the dispatch of
/// `ProjectionWorker::execute_task` as a predicate. `udb lint` uses it so a
/// projection onto a backend the worker has no writer for (weaviate, pinecone,
/// elasticsearch, an unknown name such as milvus) is rejected at build time
/// instead of dead-lettering every write at runtime.
pub fn projection_target_supported(p: &crate::generation::manifest::ManifestProjection) -> bool {
    if !should_materialize_projection(p) {
        return true;
    }
    let backend = normalize_backend(&p.backend);
    let kind = p.projection_kind.trim();
    if backend == "clickhouse" {
        // The worker cannot project a delete onto ClickHouse; only a target
        // that declares itself append-only is an honest fit.
        let options = serde_json::to_value(&p.options).unwrap_or(serde_json::Value::Null);
        return clickhouse_append_only(&options);
    }
    backend == "redis"
        || kind.eq_ignore_ascii_case("cache")
        || backend == "s3"
        || kind.eq_ignore_ascii_case("object")
        || RENDERED_PROJECTION_BACKENDS.contains(&backend.as_str())
}

/// The backends the projection worker materializes, for diagnostics.
pub fn supported_projection_backends() -> String {
    let mut backends = vec!["redis (cache)", "s3/minio (object)"];
    backends.extend(
        RENDERED_PROJECTION_BACKENDS
            .iter()
            .copied()
            .filter(|backend| *backend != "clickhouse"),
    );
    backends.push("clickhouse (append_only=true only; source deletes are not projected)");
    backends.join(", ")
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
/// The engine's `pool: PgPool` is the canonical projection-task ledger used by
/// replay enqueue transactions. Replay and drift source reads separately
/// resolve an exact project-bound PostgreSQL pool from the live runtime; they
/// never scan tenant rows through this ledger/default pool. Inline task INSERTs
/// go through [`Self::enqueue_write_tasks_tx`], which writes inside the caller's
/// canonical Postgres transaction so a task cannot be lost relative to the row
/// it projects.
#[derive(Clone)]
pub struct ProjectionEngine {
    pool: PgPool,
    config: SystemCatalogConfig,
}

impl std::fmt::Debug for ProjectionEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionEngine").finish_non_exhaustive()
    }
}

impl ProjectionEngine {
    pub fn new(pool: PgPool, config: SystemCatalogConfig) -> Self {
        Self { pool, config }
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
        // #166 REVERTED: source_checksum MUST be part of the key. The
        // `projection_acceptance_tests::idempotency_key_changes_at_each_correctness_boundary`
        // contract requires a changed source payload to mint a distinct task —
        // otherwise an updated row whose (project, row, op, target, manifest) key
        // matches an already-COMPLETED task is deduped and SKIPPED, leaving the
        // projection target stale (silent data loss). The accumulation concern
        // (FAILED/DEAD tasks per version) is a GC problem, not a reason to weaken
        // the correctness key.
        hasher.update(b"|");
        hasher.update(source_checksum.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    pub fn source_checksum(source_payload: &serde_json::Value) -> String {
        let mut hasher = Sha256::new();
        hasher.update(strip_nul_json(source_payload).to_string().as_bytes());
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
        let source_payload = strip_nul_json(source_payload);
        for plan in plans
            .iter()
            .filter(|p| message_type_matches(&p.message_type, message_type))
        {
            let source_row_key = plan.extract_row_key(&source_payload);
            let source_checksum = Self::source_checksum(&source_payload);
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
                insert_task_if_absent_on(
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
                    &source_payload,
                    &source_checksum,
                )
                .await?;
                // P2-1: the read fence matches projection work by its NATURAL key
                // (`idempotency_key` — what `pending_projection_task_count` queries
                // and what `insert_task_if_absent_on` conflicts on), NOT the random
                // `task_id` the INSERT returns. Carrying the task_id here made the
                // fence's `WHERE idempotency_key = ANY($1)` never match → the
                // projection fence cleared instantly → read-your-writes was DEAD for
                // every projection-backed read. Carry the stable idempotency_key so
                // the fence actually waits for this write to project.
                task_keys.push(idempotency_key);
            }
        }
        task_keys.sort();
        task_keys.dedup();
        Ok(task_keys)
    }

    /// Replay projection tasks from the exact project's canonical PostgreSQL
    /// row identified by its primary-key JSON object, for example
    /// `{ "id": "p1" }`.
    pub async fn replay_by_primary_key(
        &self,
        runtime: &crate::runtime::DataBrokerRuntime,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        row_key: &serde_json::Value,
    ) -> Result<u64, String> {
        let source_pool = project_source_pool(runtime, project_id)
            .map_err(|status| status.message().to_string())?;
        let rows = self
            .load_source_rows_on(
                source_pool,
                manifest,
                message_type,
                Some(row_key),
                None,
                None,
                1,
            )
            .await?;
        self.enqueue_replay_rows(manifest, project_id, message_type, rows)
            .await
    }

    /// Replay projection tasks for many primary-key JSON objects in bounded
    /// enqueue transactions. `resume_after` is the last successfully checkpointed
    /// row key from a previous run; processing resumes after that key.
    pub async fn replay_batch_rows(
        &self,
        runtime: &crate::runtime::DataBrokerRuntime,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        row_keys: &[serde_json::Value],
        batch_size: usize,
        resume_after: Option<&serde_json::Value>,
    ) -> Result<(u64, Option<serde_json::Value>), String> {
        let source_pool = project_source_pool(runtime, project_id)
            .map_err(|status| status.message().to_string())?;
        let batch_size = batch_size.max(1);
        let mut enqueued = 0u64;
        let mut checkpoint = resume_after.cloned();
        let mut rows = Vec::new();
        let mut batch_last_key: Option<serde_json::Value> = None;
        let mut skipping = resume_after.is_some();

        for row_key in row_keys {
            if skipping {
                if Some(row_key) == resume_after {
                    skipping = false;
                }
                continue;
            }
            let mut loaded = self
                .load_source_rows_on(
                    source_pool,
                    manifest,
                    message_type,
                    Some(row_key),
                    None,
                    None,
                    1,
                )
                .await?;
            rows.append(&mut loaded);
            batch_last_key = Some(row_key.clone());

            if rows.len() >= batch_size {
                enqueued += self
                    .enqueue_replay_rows(
                        manifest,
                        project_id,
                        message_type,
                        std::mem::take(&mut rows),
                    )
                    .await?;
                checkpoint = batch_last_key.clone();
            }
        }

        if !rows.is_empty() {
            enqueued += self
                .enqueue_replay_rows(manifest, project_id, message_type, rows)
                .await?;
            checkpoint = batch_last_key;
        }
        Ok((enqueued, checkpoint))
    }

    /// Replay projection tasks by first primary-key column range. Empty bounds
    /// are treated as open-ended. Values are compared through PostgreSQL text
    /// casts so the method can operate generically across scalar key types.
    pub async fn replay_range(
        &self,
        runtime: &crate::runtime::DataBrokerRuntime,
        manifest: &CatalogManifest,
        project_id: &str,
        message_type: &str,
        start: Option<&str>,
        end: Option<&str>,
        limit: i64,
    ) -> Result<u64, String> {
        let source_pool = project_source_pool(runtime, project_id)
            .map_err(|status| status.message().to_string())?;
        let rows = self
            .load_source_rows_on(
                source_pool,
                manifest,
                message_type,
                None,
                start,
                end,
                limit.max(1),
            )
            .await?;
        self.enqueue_replay_rows(manifest, project_id, message_type, rows)
            .await
    }

    /// Load canonical source rows as drift-scanner samples. This is the
    /// production source side for projection drift checks: rows come from the
    /// proto-derived canonical table mapping, and row keys come from the
    /// same `ProjectionPlan` extraction used by replay.
    pub async fn load_source_samples(
        &self,
        runtime: &crate::runtime::DataBrokerRuntime,
        project_id: &str,
        manifest: &CatalogManifest,
        message_type: &str,
        limit: i64,
    ) -> Result<Vec<crate::runtime::drift_reconciliation::SourceSample>, tonic::Status> {
        let source_pool = project_source_pool(runtime, project_id)?;
        let plan = ProjectionPlan::from_manifest(manifest)
            .into_iter()
            .find(|plan| message_type_matches(&plan.message_type, message_type))
            .ok_or_else(|| {
                crate::runtime::executor_utils::internal_status(
                    "projection",
                    "load_source_samples",
                    format!("unknown projection message_type {message_type}"),
                )
            })?;
        let rows = self
            .load_source_rows_on(
                source_pool,
                manifest,
                message_type,
                None,
                None,
                None,
                limit.max(1),
            )
            .await
            .map_err(|error| {
                crate::runtime::executor_utils::internal_status(
                    "projection",
                    "load_source_samples",
                    error,
                )
            })?;
        Ok(rows
            .into_iter()
            .map(|row| {
                crate::runtime::drift_reconciliation::SourceSample::new(
                    plan.extract_row_key(&row),
                    row,
                )
            })
            .collect())
    }

    pub(crate) async fn enqueue_replay_rows(
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

    async fn load_source_rows_on(
        &self,
        pool: &PgPool,
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
            .find(|table| message_type_matches(&table.message_name, message_type))
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
            .fetch_all(pool)
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
}

fn project_source_pool<'a>(
    runtime: &'a crate::runtime::DataBrokerRuntime,
    project_id: &str,
) -> Result<&'a PgPool, tonic::Status> {
    let source_target =
        runtime.resolve_projection_read_target_for_project("postgres", None, project_id)?;
    runtime.pg_pool_for_instance(source_target.instance.as_deref())
}

/// PostgreSQL JSONB rejects `\u0000` escapes with "unsupported Unicode escape
/// sequence". Canonical row writes strip NULs at the Postgres bind edge; mirror
/// that normalization before projection tasks persist the row payload as JSONB.
fn strip_nul_json(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) if s.contains('\u{0}') => {
            serde_json::Value::String(s.replace('\u{0}', ""))
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(strip_nul_json).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (key.replace('\u{0}', ""), strip_nul_json(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(crate) fn message_type_matches(
    catalog_message_type: &str,
    requested_message_type: &str,
) -> bool {
    let catalog = catalog_message_type.trim();
    let requested = requested_message_type.trim();
    if catalog.eq_ignore_ascii_case(requested) {
        return true;
    }
    let catalog_leaf = catalog.rsplit('.').next().unwrap_or(catalog);
    let requested_leaf = requested.rsplit('.').next().unwrap_or(requested);
    !catalog_leaf.is_empty() && catalog_leaf.eq_ignore_ascii_case(requested_leaf)
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
    let sql = projection_task_insert_sql(&rel);
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

/// The task INSERT, and the per-row ORDERING contract the worker relies on.
///
/// Ordering key: `row_revision`, a `BIGSERIAL` drawn when the task row is
/// inserted. The live write path inserts its task inside the writer's
/// transaction AFTER the row write, i.e. while holding that row's lock, so for
/// two committed writes to the same row the later writer's task has the
/// strictly higher revision — a sequence cannot tie or step backwards the way
/// a clock stamp can. `created_at` (stamped with `clock_timestamp()`, not the
/// transaction-start `NOW()`) stays the claim's fairness order only.
/// `source_checksum` cannot order tasks — it is a content hash.
///
/// The PostgreSQL claim (`postgres_projection.rs`) retires a PENDING/FAILED
/// task once a task with a higher revision exists for the same row and
/// target, and never claims a task while its row has a task IN_PROGRESS or a
/// newer one queued — so a retried stale task can never be applied over a
/// newer one.
///
/// Re-arm on conflict: the idempotency key hashes the row CONTENT, so a row
/// that returns to an earlier value (v1 → v2 → v1) maps onto v1's existing
/// task. `DO NOTHING` would then leave the target at v2. When ANY task with a
/// higher revision exists for the row, the conflicting task takes a FRESH
/// revision (`EXCLUDED.row_revision`, the value the proposed insert drew) so
/// it orders after v2 again: a queued/finished/dead-lettered one is re-armed
/// as PENDING; an IN_PROGRESS one keeps its status (its worker is applying
/// exactly this content) and simply becomes the newest. A replay of an
/// unchanged row (its task is the newest) stays deduplicated.
fn projection_task_insert_sql(rel: &str) -> String {
    format!(
        "WITH inserted AS (
             INSERT INTO {rel} AS existing
             (idempotency_key, project_id, manifest_checksum, message_type,
              source_schema, source_table, source_row_key, operation, target_backend,
              target_instance, projection_kind, resource_name, target_options,
              source_payload, source_checksum, created_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,
                     clock_timestamp())
             ON CONFLICT (idempotency_key) DO UPDATE
                 SET row_revision = EXCLUDED.row_revision,
                     status = CASE WHEN existing.status = 'IN_PROGRESS'
                                   THEN existing.status ELSE 'PENDING' END,
                     retry_count = CASE WHEN existing.status = 'IN_PROGRESS'
                                        THEN existing.retry_count ELSE 0 END,
                     last_error = CASE WHEN existing.status = 'IN_PROGRESS'
                                       THEN existing.last_error ELSE '' END,
                     next_retry_at = CASE WHEN existing.status = 'IN_PROGRESS'
                                          THEN existing.next_retry_at ELSE NULL END,
                     completed_at = CASE WHEN existing.status = 'IN_PROGRESS'
                                         THEN existing.completed_at ELSE NULL END,
                     updated_at = CASE WHEN existing.status = 'IN_PROGRESS'
                                       THEN existing.updated_at ELSE NOW() END,
                     created_at = clock_timestamp()
                 WHERE EXISTS (
                       SELECT 1 FROM {rel} AS newer
                       WHERE {same_row}
                         AND newer.row_revision > existing.row_revision)
             RETURNING task_id
         )
         SELECT task_id::TEXT FROM inserted
         UNION ALL
         SELECT task_id::TEXT FROM {rel} WHERE idempotency_key = $1
         LIMIT 1",
        same_row =
            crate::runtime::canonical_store::postgres_projection::projection_task_same_row_sql(
                "newer", "existing"
            ),
    )
}

// ── ProjectionWorker ──────────────────────────────────────────────────────────

/// How many times the worker tries to record a successful apply before
/// leaving the task to its claim lease.
const MARK_COMPLETED_ATTEMPTS: u32 = 3;

/// Settings for the projection worker, populated from environment variables.
#[derive(Debug, Clone)]
pub struct ProjectionWorkerSettings {
    pub enabled: bool,
    pub poll_interval_secs: u64,
    pub batch_size: i64,
    pub max_retries: i32,
    pub project_id: Option<String>,
    /// Claim lease: a task IN_PROGRESS for longer than this is presumed
    /// abandoned (its worker crashed or lost leadership) and is returned to
    /// PENDING by the worker's next pass (`UDB_PROJECTION_TASK_LEASE_SECS`).
    /// Must exceed the slowest single batch, or a live batch is re-run.
    pub task_lease_secs: u64,
}

/// Floor for [`ProjectionWorkerSettings::task_lease_secs`].
const MIN_TASK_LEASE_SECS: u64 = 30;

impl Default for ProjectionWorkerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_secs: 5,
            batch_size: 50,
            max_retries: 5,
            project_id: None,
            task_lease_secs: 300,
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
        if let Ok(val) = std::env::var("UDB_PROJECTION_TASK_LEASE_SECS") {
            if let Ok(n) = val.parse::<u64>() {
                s.task_lease_secs = n.max(MIN_TASK_LEASE_SECS);
            }
        }
        s.project_id = std::env::var("UDB_PROJECTION_PROJECT_ID")
            .or_else(|_| std::env::var("UDB_PROJECT_ID"))
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        s
    }
}

/// Background worker that polls `udb_projection_tasks`, dispatches each task
/// to the appropriate backend executor, and records completion or failure.
pub struct ProjectionWorker {
    /// NW1-3b: replaced raw `PgPool` with `Arc<dyn SystemStores>`.
    store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    runtime: Arc<crate::runtime::DataBrokerRuntime>,
    config: SystemCatalogConfig,
    settings: ProjectionWorkerSettings,
    metrics: Arc<dyn MetricsRecorder>,
    catalog: Arc<CatalogManager>,
}

impl ProjectionWorker {
    pub fn new(
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
        runtime: Arc<crate::runtime::DataBrokerRuntime>,
        metrics: Arc<dyn MetricsRecorder>,
        catalog: Arc<CatalogManager>,
    ) -> Self {
        Self {
            store,
            runtime,
            config: SystemCatalogConfig::current(),
            settings: ProjectionWorkerSettings::from_env(),
            metrics,
            catalog,
        }
    }

    /// Whether the worker is enabled (checks env at construction time).
    pub fn is_enabled() -> bool {
        ProjectionWorkerSettings::from_env().enabled
    }

    /// Run the worker loop forever.  Call from a dedicated `tokio::spawn`.
    pub async fn run_forever(self) {
        self.run_loop(None::<std::future::Pending<()>>).await;
    }

    /// H5: the worker loop under a singleton lease. The fencing token is
    /// re-verified before every pass; the first failed check returns, so a
    /// superseded leader (paused past its TTL while a peer took over) claims
    /// and materializes nothing further. The lease heartbeat additionally
    /// drops a pass mid-flight when ownership is lost.
    pub async fn run_forever_fenced(self, fence: crate::runtime::singleton::LeaseFence) {
        let interval = Duration::from_secs(self.settings.poll_interval_secs.max(1));
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = fence.check().await {
                tracing::warn!(error = %err, "projection worker stopping: lease fence lost");
                return;
            }
            self.run_and_log_once().await;
        }
    }

    /// Run the worker loop until `shutdown` resolves.
    pub async fn run_until_cancelled<F>(self, shutdown: F)
    where
        F: std::future::Future<Output = ()>,
    {
        self.run_loop(Some(shutdown)).await;
    }

    async fn run_loop<F>(self, shutdown: Option<F>)
    where
        F: std::future::Future<Output = ()>,
    {
        let interval = Duration::from_secs(self.settings.poll_interval_secs.max(1));
        let mut ticker = tokio::time::interval(interval);
        if let Some(shutdown) = shutdown {
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = ticker.tick() => self.run_and_log_once().await,
                }
            }
        } else {
            loop {
                ticker.tick().await;
                self.run_and_log_once().await;
            }
        }
    }

    async fn run_and_log_once(&self) {
        let (completed, failed) = self.run_once().await;
        if completed + failed > 0 {
            tracing::info!(completed, failed, "projection worker pass");
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
        if !self.catalog.authority_is_fresh() {
            tracing::warn!("projection worker pass skipped: catalog authority is stale");
            return (0, 0);
        }
        self.refresh_pending_metrics().await;
        // Claim lease: a task left IN_PROGRESS by a worker that crashed (or
        // lost leadership) mid-batch is reclaimed here, by the worker itself,
        // once its lease expires — not only by the opt-in reconciliation
        // worker, which is off by default. Re-running such a task is safe:
        // every projected mutation is an idempotent upsert/delete by key.
        match ProjectionTaskStore::reset_stale_in_progress_tasks(
            self.store.as_ref(),
            Duration::from_secs(self.settings.task_lease_secs),
        )
        .await
        {
            Ok(reclaimed) if reclaimed > 0 => {
                tracing::warn!(
                    reclaimed,
                    lease_secs = self.settings.task_lease_secs,
                    "projection worker reclaimed tasks whose claim lease expired"
                );
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(error = %err, "projection worker: expired-lease reset failed");
            }
        }
        let filter = ProjectionClaimFilter {
            batch_size: self.settings.batch_size,
            max_retries: self.settings.max_retries,
            target_backend: None,
            target_instance: None,
            project_id: self.settings.project_id.clone(),
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
        let ordering_enforced = ProjectionTaskStore::enforces_per_row_ordering(self.store.as_ref());

        // Consume the claimed tasks and move each field into its local — `claimed`
        // is not used after this loop, so the per-field clones are unnecessary (#107).
        for row in claimed {
            let task_id = row.task_id;
            let project_id = row.project_id;
            let manifest_checksum = row.manifest_checksum;
            let target_backend = row.target_backend;
            let target_instance = row.target_instance;
            let projection_kind = row.projection_kind;
            let resource_name = row.resource_name;
            let operation = row.operation.as_str().to_string();
            let source_row_key = row.source_row_key;
            let target_options = row.target_options;
            let source_payload = row.source_payload;
            let retry_count = row.retry_count;
            let created_at = Some(row.created_at);

            let instance_opt = if target_instance.is_empty() {
                None
            } else {
                Some(target_instance.as_str())
            };

            let result = match validate_projection_task_catalog(
                &self.catalog,
                &project_id,
                &manifest_checksum,
            ) {
                // D9: a ledger that cannot order one row's tasks must not drive
                // a keyed target — refuse instead of applying out of order.
                Ok(()) if !ordering_enforced
                    && projection_task_is_ordering_dependent(&target_backend, &target_options) =>
                {
                    Err(projection_ordering_refusal(ProjectionTaskStore::backend_label(
                        self.store.as_ref(),
                    )))
                }
                Ok(()) => {
                    self.execute_task(
                        &project_id,
                        &target_backend,
                        instance_opt,
                        &projection_kind,
                        &resource_name,
                        &operation,
                        &source_row_key,
                        &target_options,
                        &source_payload,
                    )
                    .await
                }
                Err(error) => Err(format!(
                    "{} {error}",
                    crate::runtime::canonical_store::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX
                )),
            };

            match result {
                Ok(_) => {
                    if let Err(err) = self.mark_completed_with_retry(task_id).await {
                        // The target was written but the ledger still says
                        // IN_PROGRESS. Do not report success: the claim lease
                        // reclaims the task and the idempotent mutation is
                        // re-applied, so the read fence never clears on a
                        // task whose completion was never recorded.
                        tracing::error!(
                            task_id = %task_id,
                            project_id = %project_id,
                            backend = %target_backend,
                            error = %err,
                            "projection task applied but could not be marked COMPLETED; it will be re-applied after its claim lease expires",
                        );
                        self.metrics.inc_projection_tasks_failed_total(
                            &target_backend,
                            &target_instance,
                            &projection_kind,
                        );
                        failed += 1;
                        continue;
                    }
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
                    // An ordering refusal is permanent for this ledger:
                    // dead-letter it now rather than burn the retry budget.
                    let new_status = if new_retry >= self.settings.max_retries
                        || err.starts_with(PROJECTION_ORDERING_REFUSAL)
                    {
                        ProjectionTaskStatus::DeadLetter
                    } else {
                        ProjectionTaskStatus::Failed
                    };
                    if let Err(mark_err) =
                        self.mark_failed(task_id, new_retry, new_status, &err).await
                    {
                        tracing::error!(
                            task_id = %task_id,
                            error = %mark_err,
                            "projection task failure could not be recorded; it will be retried after its claim lease expires",
                        );
                    }
                    self.metrics.inc_projection_tasks_failed_total(
                        &target_backend,
                        &target_instance,
                        &projection_kind,
                    );
                    tracing::warn!(
                        task_id = %task_id,
                        project_id = %project_id,
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
        project_id: &str,
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
        // Every target is scoped — resolve the scope BEFORE dispatching, so no
        // backend can be reached with a record whose tenant is unknown. A
        // record that names a tenant field without a scalar value (a delete
        // filter carrying the tenant as an operator, say) is refused: writing
        // or deleting it unscoped would reach every tenant's records.
        let scope = ProjectionScope::resolve(project_id, target_options, source_payload);
        scope.require_tenant(&format!(
            "{normalized_backend} projection '{resource_name}'"
        ))?;
        if normalized_backend == "redis" || projection_kind.eq_ignore_ascii_case("cache") {
            return self
                .execute_redis_projection(
                    project_id,
                    instance,
                    resource_name,
                    operation,
                    source_row_key,
                    target_options,
                    source_payload,
                    &scope,
                )
                .await;
        }
        if normalized_backend == "s3"
            || normalized_backend == "minio"
            || projection_kind.eq_ignore_ascii_case("object")
        {
            return self
                .execute_object_projection(
                    project_id,
                    &normalized_backend,
                    instance,
                    resource_name,
                    operation,
                    source_row_key,
                    target_options,
                    source_payload,
                    &scope,
                )
                .await;
        }

        // E8: an edge projection's endpoint labels come from the manifest
        // (the referenced tables' node labels) when the target does not
        // declare them, so the edge MATCH addresses the same labels the node
        // projection / IR / DDL use.
        let target_options = if normalized_backend == "neo4j" {
            with_manifest_edge_labels(
                &self.catalog.active_for(project_id).manifest,
                resource_name,
                target_options,
            )
        } else {
            target_options.clone()
        };
        let request = render_projection_mutation(
            &normalized_backend,
            projection_kind,
            resource_name,
            operation,
            source_row_key,
            &target_options,
            source_payload,
            &scope,
        )?;
        if request.is_null() {
            // The renderer decided this operation has nothing to apply on the
            // target (a delete on an append-only analytical table).
            return Ok(());
        }
        self.runtime
            .mutate_backend_target_for_project(
                &normalized_backend,
                instance,
                project_id,
                &request.to_string(),
            )
            .await
            .map(|_| ())
            .map_err(|s| s.message().to_string())
    }

    async fn execute_redis_projection(
        &self,
        project_id: &str,
        instance: Option<&str>,
        resource_name: &str,
        operation: &str,
        source_row_key: &serde_json::Value,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
        scope: &ProjectionScope,
    ) -> Result<(), String> {
        let ttl = option_value(target_options, "ttl_seconds")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(300);
        let pattern = option_value(target_options, "key_pattern")
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| resource_name.to_string());
        let key = redis_projection_key(&pattern, source_row_key, source_payload, scope)?;
        if operation.eq_ignore_ascii_case("delete") {
            // Exact-key DEL (never a SCAN MATCH pattern): a `*` in a row value
            // can never widen the delete to other records or tenants.
            self.runtime
                .projection_cache_delete_for_project(instance, project_id, &key)
                .await?;
            return Ok(());
        }
        let bytes =
            serde_json::to_vec(&scope.stamp(source_payload)).map_err(|err| err.to_string())?;
        self.runtime
            .projection_cache_set_for_project(instance, project_id, &key, &[bytes], ttl)
            .await
    }

    async fn execute_object_projection(
        &self,
        project_id: &str,
        backend: &str,
        instance: Option<&str>,
        resource_name: &str,
        operation: &str,
        source_row_key: &serde_json::Value,
        target_options: &serde_json::Value,
        source_payload: &serde_json::Value,
        scope: &ProjectionScope,
    ) -> Result<(), String> {
        let object_key =
            object_projection_key(source_row_key, source_payload, target_options, scope)?;
        // `normalize_backend` folds `minio` into `s3`, but the env-configured
        // MinIO instance registers as `minio:<name>`: resolving only `s3`
        // failed every object projection with "s3:default is not configured".
        // Try the S3-compatible aliases the way the object data path does.
        let target = self
            .runtime
            .resolve_projection_write_target_for_project(backend, instance, project_id)
            .or_else(|first| match backend {
                "s3" => self
                    .runtime
                    .resolve_projection_write_target_for_project("minio", instance, project_id)
                    .map_err(|_| first),
                "minio" => self
                    .runtime
                    .resolve_projection_write_target_for_project("s3", instance, project_id)
                    .map_err(|_| first),
                _ => Err(first),
            })
            .map_err(|status| status.message().to_string())?;
        if operation.eq_ignore_ascii_case("delete") {
            // A projected delete must remove the object, not write a tombstone
            // body that leaves the stale object readable.
            let request = serde_json::json!({
                "bucket": resource_name,
                "object_key": object_key,
            });
            return self
                .runtime
                .delete_object_backend_target(
                    &target.backend,
                    target.instance.as_deref(),
                    project_id,
                    &request.to_string(),
                )
                .await
                .map_err(|s| s.message().to_string());
        }
        let body = serde_json::to_vec(&serde_json::json!({
            "operation": operation,
            "source_row_key": source_row_key,
            "payload": scope.stamp(source_payload),
        }))
        .map_err(|err| err.to_string())?;
        let request = serde_json::json!({
            "bucket": resource_name,
            "object_key": object_key,
            "content_type": "application/json",
        });
        self.runtime
            .put_object_backend_target_for_project(
                &target.backend,
                target.instance.as_deref(),
                project_id,
                &request.to_string(),
                body,
            )
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

    /// [`Self::mark_completed`] with a short bounded retry: a transient ledger
    /// error right after a successful apply should not cost a full lease
    /// period and a re-application.
    async fn mark_completed_with_retry(&self, task_id: Uuid) -> Result<(), String> {
        let mut last_error = String::new();
        for attempt in 0..MARK_COMPLETED_ATTEMPTS {
            match self.mark_completed(task_id).await {
                Ok(()) => return Ok(()),
                Err(err) => last_error = err,
            }
            if attempt + 1 < MARK_COMPLETED_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(100u64 << attempt)).await;
            }
        }
        Err(last_error)
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

fn validate_projection_task_catalog(
    catalog: &CatalogManager,
    project_id: &str,
    task_manifest_checksum: &str,
) -> Result<(), String> {
    if !catalog.authority_is_fresh() {
        return Err("catalog authority became stale before projection dispatch".to_string());
    }
    let project_id = project_id.trim();
    if project_id.is_empty() {
        return Err("projection task has no project_id".to_string());
    }
    let active = catalog
        .active_exact_for(project_id)
        .ok_or_else(|| format!("project '{project_id}' has no exact active catalog"))?;
    let active_checksum = active.manifest.checksum_sha256.trim();
    let task_checksum = task_manifest_checksum.trim();
    if task_checksum.is_empty() || active_checksum.is_empty() {
        return Err(format!(
            "project '{project_id}' projection task/catalog checksum is empty"
        ));
    }
    if task_checksum != active_checksum {
        return Err(format!(
            "project '{project_id}' projection task catalog '{task_checksum}' does not match active catalog '{active_checksum}'"
        ));
    }
    Ok(())
}

fn normalize_backend(backend: &str) -> String {
    match backend.to_ascii_lowercase().as_str() {
        "pg" | "postgresql" => "postgres".to_string(),
        "mongo" => "mongodb".to_string(),
        "minio" => "s3".to_string(),
        // `STORAGE_BACKEND_AZURE_BLOB` normalizes to `azure_blob`.
        "azure_blob" | "azure" => "azureblob".to_string(),
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

/// The Redis key a cache projection writes: the rendered `key_pattern`,
/// namespaced by tenant/project. A pattern placeholder the record does not
/// fill is an error — the literal `{field}` would make every such row share
/// (and overwrite) one key.
fn redis_projection_key(
    pattern: &str,
    source_row_key: &serde_json::Value,
    source_payload: &serde_json::Value,
    scope: &ProjectionScope,
) -> Result<String, String> {
    let rendered = render_key_pattern(pattern, source_row_key, source_payload);
    if let Some(start) = rendered.find('{')
        && rendered[start..].contains('}')
    {
        return Err(format!(
            "cache projection key pattern '{pattern}' has a placeholder the record does not fill (rendered '{rendered}')"
        ));
    }
    Ok(scope.scoped_key(&rendered))
}

/// The object key an object projection writes:
/// `t:{tenant}/p:{project}/{key_prefix}/{id}.json`.
fn object_projection_key(
    source_row_key: &serde_json::Value,
    source_payload: &serde_json::Value,
    target_options: &serde_json::Value,
    scope: &ProjectionScope,
) -> Result<String, String> {
    let key_prefix = option_value(target_options, "key_prefix").unwrap_or_default();
    let id = row_identity(source_row_key, source_payload, target_options)?;
    let object_key = if key_prefix.trim().trim_matches('/').is_empty() {
        format!("{id}.json")
    } else {
        format!("{}/{}.json", key_prefix.trim().trim_matches('/'), id)
    };
    Ok(scope.scoped_key(&object_key))
}

/// Prefix of the error a projection task is dead-lettered with when the task
/// ledger cannot enforce per-row ordering (D9). It starts with the authority
/// prefix so reconciliation repair never requeues it into the same refusal.
const PROJECTION_ORDERING_REFUSAL: &str =
    "projection authority rejected: per-row ordering unavailable:";

fn projection_ordering_refusal(store_label: &str) -> String {
    format!(
        "{PROJECTION_ORDERING_REFUSAL} the '{store_label}' projection task ledger does not \
         enforce per-row ordering (monotonic row revision + supersede + one in-flight task \
         per row), so a keyed projection target could be rolled back to an older row state; \
         run the projection worker on the PostgreSQL system store"
    )
}

/// Whether applying this task out of order could leave the target holding an
/// older row state. Every keyed target (upsert/delete by id) is; only an
/// append-only ClickHouse target, which records every change as a new row and
/// ignores deletes, is order-independent.
fn projection_task_is_ordering_dependent(
    target_backend: &str,
    target_options: &serde_json::Value,
) -> bool {
    !(normalize_backend(target_backend) == "clickhouse" && clickhouse_append_only(target_options))
}

/// Whether a ClickHouse projection target is declared append-only
/// (`append_only` / `insert_only` = true): rows are inserted, and source
/// deletes are deliberately not projected.
fn clickhouse_append_only(options: &serde_json::Value) -> bool {
    ["append_only", "insert_only"].iter().any(|key| {
        option_value(options, key).is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
    })
}

#[allow(clippy::too_many_arguments)]
fn render_projection_mutation(
    backend: &str,
    projection_kind: &str,
    resource_name: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
    scope: &ProjectionScope,
) -> Result<serde_json::Value, String> {
    match backend {
        "mongodb" => render_mongodb_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
            scope,
        ),
        "qdrant" => render_qdrant_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
            scope,
        ),
        "neo4j" => render_neo4j_projection(
            resource_name,
            operation,
            source_row_key,
            target_options,
            source_payload,
            scope,
        ),
        "clickhouse" => {
            render_clickhouse_projection(resource_name, operation, target_options, source_payload)
        }
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
    scope: &ProjectionScope,
) -> Result<serde_json::Value, String> {
    let id_field = option_value(target_options, "id_field")
        .or_else(|| option_value(target_options, "partition_key"))
        .unwrap_or_else(|| "id".to_string());
    // Documents are stamped with `_tenant_id`/`_project_id`, and every filter
    // (the upsert key and the delete filter) carries them, so two tenants'
    // rows that share an id are two documents and neither can replace or
    // delete the other's.
    if operation.eq_ignore_ascii_case("delete") {
        let filter = if let Some(map) = source_payload.as_object()
            && !map.is_empty()
        {
            scope.stamp(&serde_json::Value::Object(map.clone()))
        } else {
            let id = row_identity(source_row_key, source_payload, target_options)?;
            scope.stamp(&serde_json::json!({ id_field: id }))
        };
        return Ok(serde_json::json!({
            "operation": "delete",
            "collection": collection,
            "filter": filter,
        }));
    }
    let id = row_identity(source_row_key, source_payload, target_options)?;
    let filter = scope.stamp(&serde_json::json!({ id_field.clone(): id }));
    let mut document = scope.stamp(source_payload);
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
    scope: &ProjectionScope,
) -> Result<serde_json::Value, String> {
    // The point id is the row id namespaced by tenant/project (the executor
    // hashes any non-UUID, non-integer id into a stable UUID), so two
    // tenants' rows with the same primary key are two points: neither tenant's
    // upsert replaces, nor its delete removes, the other's vector.
    let id = scope.scoped_key(&row_identity(
        source_row_key,
        source_payload,
        target_options,
    )?);
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
            "payload": scope.stamp(source_payload),
        }],
    }))
}

/// Option keys an edge projection declares its endpoint labels under (first
/// wins), in `[source, target]` order. Shared with [`render_neo4j_projection`].
const EDGE_ENDPOINT_LABEL_KEYS: [[&str; 2]; 2] = [
    ["edge_source_label", "from_label"],
    ["edge_target_label", "to_label"],
];

/// `target_options` plus the endpoint labels the manifest implies for an edge
/// projection that does not declare them. The edge's source table is the table
/// whose neo4j projection targets `resource_name`; each endpoint field that is a
/// single-column foreign key resolves to the referenced table's node label —
/// the label of that table's own neo4j node projection when it has one, else
/// the shared IR/DDL label ([`neo4j_label_for_table`]). Declared labels win;
/// an endpoint the manifest cannot resolve stays unlabeled (an unlabeled MATCH
/// is still scope-keyed, only slower).
///
/// [`neo4j_label_for_table`]: crate::generation::neo4j_labels::neo4j_label_for_table
fn with_manifest_edge_labels(
    manifest: &CatalogManifest,
    resource_name: &str,
    target_options: &serde_json::Value,
) -> serde_json::Value {
    let declared = |key: &str| option_value(target_options, key).filter(|v| !v.trim().is_empty());
    let (Some(source_field), Some(target_field)) =
        (declared("edge_source_field"), declared("edge_target_field"))
    else {
        return target_options.clone();
    };
    let is_neo4j = |target: &ProjectionTarget| normalize_backend(&target.backend) == "neo4j";
    let plans = ProjectionPlan::from_manifest(manifest);
    let Some(source_table) = plans
        .iter()
        .find(|plan| {
            plan.targets
                .iter()
                .any(|target| is_neo4j(target) && target.resource_name == resource_name)
        })
        .and_then(|plan| manifest_table_named(manifest, &plan.source_schema, &plan.source_table))
    else {
        return target_options.clone();
    };
    let node_label = |table: &crate::generation::manifest::ManifestTable| -> String {
        plans
            .iter()
            .filter(|plan| plan.source_table == table.table && plan.source_schema == table.schema)
            .flat_map(|plan| plan.targets.iter())
            .find(|&target| {
                is_neo4j(target)
                    && !target
                        .options
                        .iter()
                        .any(|o| o.key.eq_ignore_ascii_case("edge_source_field"))
            })
            .map(|target| {
                let label_override = crate::generation::backends::neo4j::NEO4J_LABEL_OPTION_KEYS
                    .iter()
                    .find_map(|key| {
                        target
                            .options
                            .iter()
                            .find(|o| o.key.eq_ignore_ascii_case(key) && !o.value.trim().is_empty())
                    })
                    .map(|o| o.value.as_str());
                crate::generation::backends::neo4j::resolve_neo4j_label(
                    &target.resource_name,
                    label_override,
                )
            })
            .unwrap_or_else(|| {
                crate::generation::neo4j_labels::neo4j_label_for_table(manifest, table)
            })
    };
    let mut options = match target_options {
        serde_json::Value::Array(entries) => entries.clone(),
        _ => Vec::new(),
    };
    for (keys, field) in EDGE_ENDPOINT_LABEL_KEYS
        .iter()
        .zip([source_field.trim(), target_field.trim()])
    {
        if keys.iter().any(|&key| declared(key).is_some()) {
            continue;
        }
        let Some(referenced) = source_table
            .foreign_keys
            .iter()
            .find(|fk| fk.columns.len() == 1 && fk.columns[0].eq_ignore_ascii_case(field))
            .and_then(|fk| manifest_table_named(manifest, &fk.ref_schema, &fk.ref_table))
        else {
            continue;
        };
        options.push(serde_json::json!({ "key": keys[0], "value": node_label(referenced) }));
    }
    serde_json::Value::Array(options)
}

/// The manifest table `schema.name`, or the only-by-name match when the schema
/// differs (a foreign key's `ref_schema` defaults to `public`).
fn manifest_table_named<'m>(
    manifest: &'m CatalogManifest,
    schema: &str,
    name: &str,
) -> Option<&'m crate::generation::manifest::ManifestTable> {
    let by_name = || manifest.tables.iter().filter(move |t| t.table == name);
    by_name()
        .find(|t| schema.trim().is_empty() || t.schema == schema)
        .or_else(|| by_name().next())
}

fn render_neo4j_projection(
    resource_name: &str,
    operation: &str,
    source_row_key: &serde_json::Value,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
    scope: &ProjectionScope,
) -> Result<serde_json::Value, String> {
    // THE label resolver the DDL generator and the IR compiler use, so all
    // three address the same nodes.
    let label_override = crate::generation::backends::neo4j::NEO4J_LABEL_OPTION_KEYS
        .iter()
        .find_map(|key| option_value(target_options, key))
        .filter(|value| !value.trim().is_empty());
    let label = crate::generation::backends::neo4j::resolve_neo4j_label(
        resource_name,
        label_override.as_deref(),
    );
    let id = row_identity(source_row_key, source_payload, target_options)?;
    scope.require_tenant(&format!("graph projection '{label}' record '{id}'"))?;
    let scope_fields = serde_json::Value::Object(scope.fields());
    let delete = operation.eq_ignore_ascii_case("delete");
    let edge_source =
        option_value(target_options, "edge_source_field").filter(|v| !v.trim().is_empty());
    let edge_target =
        option_value(target_options, "edge_target_field").filter(|v| !v.trim().is_empty());
    match (edge_source, edge_target) {
        // A graph store naming both endpoint fields models one EDGE per row: the
        // store label is the relationship type, and the endpoints are the nodes
        // whose `id` the two fields carry, inside the row's own tenant/project.
        (Some(source_field), Some(target_field)) => {
            if delete {
                return Ok(serde_json::json!({
                    "operation": "delete_edge",
                    "rel_type": label,
                    "id": id,
                    "scope": scope_fields,
                }));
            }
            let endpoint = |field: &str| -> Result<String, String> {
                source_payload
                    .get(field.trim())
                    .map(json_scalar_to_string)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        format!(
                            "graph edge projection '{label}' row '{id}' has no value for endpoint field '{field}'"
                        )
                    })
            };
            let mut edge = serde_json::json!({
                "operation": "upsert_edge",
                "rel_type": label,
                "id": id,
                "from_id": endpoint(&source_field)?,
                "to_id": endpoint(&target_field)?,
                "properties": scope.stamp(source_payload),
                "scope": scope_fields,
            });
            // Endpoint labels, when declared, let the executor's edge MATCH
            // use the label index instead of scanning every node.
            for (option_keys, field) in EDGE_ENDPOINT_LABEL_KEYS
                .into_iter()
                .zip(["from_label", "to_label"])
            {
                if let Some(endpoint_label) = option_keys
                    .iter()
                    .find_map(|key| option_value(target_options, key))
                    .filter(|value| !value.trim().is_empty())
                {
                    edge[field] = serde_json::Value::String(
                        crate::generation::backends::neo4j::resolve_neo4j_label(
                            &endpoint_label,
                            None,
                        ),
                    );
                }
            }
            Ok(edge)
        }
        (None, None) => {
            if delete {
                return Ok(serde_json::json!({
                    "operation": "delete_node",
                    "label": label,
                    "id": id,
                    "scope": scope_fields,
                }));
            }
            Ok(serde_json::json!({
                "operation": "create_node",
                "label": label,
                "id": id,
                "properties": scope.stamp(source_payload),
                "scope": scope_fields,
            }))
        }
        _ => Err(format!(
            "graph projection '{label}' declares only one of edge_source_field / edge_target_field; an edge needs both"
        )),
    }
}

/// ClickHouse targets are append-only: the worker inserts rows and has no
/// delete it can apply (a `DELETE` mutation is asynchronous and a tombstone
/// needs a table-specific version/sign column the manifest does not declare).
/// A target must therefore say `append_only = true`, which makes a source
/// delete a deliberate no-op (returned as `Null`); without it a delete is
/// refused as unsupported, and `udb lint` rejects the projection up front.
fn render_clickhouse_projection(
    table: &str,
    operation: &str,
    target_options: &serde_json::Value,
    source_payload: &serde_json::Value,
) -> Result<serde_json::Value, String> {
    if operation.eq_ignore_ascii_case("delete") {
        if clickhouse_append_only(target_options) {
            return Ok(serde_json::Value::Null);
        }
        return Err(format!(
            "projection backend 'clickhouse' is not supported for deletes on '{table}': \
             declare the target append_only=true (deletes are then not projected)"
        ));
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
    pub project_id: String,
    pub source_table: String,
    pub target_backend: String,
    pub target_instance: String,
    pub dead_letter_count: i64,
    pub repair_tasks_enqueued: i64,
}

/// Background worker that detects DEAD_LETTER tasks and re-enqueues them as
/// PENDING (repair).
pub struct ReconciliationWorker {
    /// Canonical PostgreSQL projection ledger used only to enqueue replay
    /// tasks. Project source reads resolve through `runtime`; all normal task
    /// lifecycle reads/writes go through the store trait.
    pool: PgPool,
    store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    config: SystemCatalogConfig,
    settings: ReconciliationSettings,
    metrics: Arc<dyn MetricsRecorder>,
    catalog: Arc<CatalogManager>,
    runtime: Arc<crate::runtime::DataBrokerRuntime>,
}

impl ReconciliationWorker {
    pub fn new(
        pool: PgPool,
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
        metrics: Arc<dyn MetricsRecorder>,
        catalog: Arc<CatalogManager>,
        runtime: Arc<crate::runtime::DataBrokerRuntime>,
    ) -> Self {
        Self {
            pool,
            store,
            config: SystemCatalogConfig::current(),
            settings: ReconciliationSettings::from_env(),
            metrics,
            catalog,
            runtime,
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
            self.run_and_log_once().await;
        }
    }

    /// H5: [`Self::run_forever`] under a singleton lease: the fencing token is
    /// re-verified before every pass and the first failed check returns, so a
    /// superseded leader stops requeueing / replaying.
    pub async fn run_forever_fenced(self, fence: crate::runtime::singleton::LeaseFence) {
        let interval = Duration::from_secs(self.settings.interval_secs);
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = fence.check().await {
                tracing::warn!(error = %err, "projection reconciliation stopping: lease fence lost");
                return;
            }
            self.run_and_log_once().await;
        }
    }

    async fn run_and_log_once(&self) {
        let reports = self.run_once().await;
        for r in &reports {
            tracing::info!(
                project_id = %r.project_id,
                source_table = %r.source_table,
                backend = %r.target_backend,
                dead_letter = %r.dead_letter_count,
                repaired = %r.repair_tasks_enqueued,
                "projection reconciliation pass",
            );
        }
    }

    /// One reconciliation pass: reset DEAD_LETTER tasks to PENDING.
    ///
    /// NW1-3b: routes through `reset_stale_in_progress_tasks` +
    /// `dead_letter_groups` + `requeue_dead_letter_by_source`. The
    /// `replay_source_rows_for_repair` stays PG-coupled, but resolves a
    /// project-authorized source pool rather than using the ledger pool.
    ///
    /// Returns one report per
    /// (project_id, source_table, target_backend, target_instance) combination
    /// that had dead-letter tasks.
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
        // 2. Replay canonical source rows (PG-only) only while catalog
        // authority is fresh. The worker retains the manager, not a startup
        // snapshot, so later project activation and catalog reload are seen.
        if self.catalog.authority_is_fresh() {
            self.replay_source_rows_for_repair().await;
        } else {
            tracing::warn!(
                "projection reconciliation source replay skipped: catalog authority is stale"
            );
        }

        // 3. Find all (project_id, source_table, target_backend, target_instance)
        // groups with dead-letter tasks.
        let groups = match ProjectionTaskStore::dead_letter_groups(self.store.as_ref(), 500).await {
            Ok(g) => g,
            Err(err) => {
                tracing::warn!(error = %err, "reconciliation scan failed");
                return reports;
            }
        };

        for group in groups {
            let project_id = group.project_id.clone();
            let source_table = group.source_table.clone();
            let target_backend = group.target_backend.clone();
            let target_instance = group.target_instance.clone();
            let dead_count = group.dead_count;

            if project_id.trim().is_empty() {
                tracing::warn!(
                    source_table = %source_table,
                    backend = %target_backend,
                    instance = %target_instance,
                    "projection reconciliation refused legacy dead letters without project_id"
                );
                reports.push(ReconciliationReport {
                    project_id,
                    source_table,
                    target_backend,
                    target_instance,
                    dead_letter_count: dead_count,
                    repair_tasks_enqueued: 0,
                });
                continue;
            }
            if !self.catalog.authority_is_fresh()
                || self.catalog.active_exact_for(&project_id).is_none()
            {
                tracing::warn!(
                    project_id = %project_id,
                    source_table = %source_table,
                    backend = %target_backend,
                    instance = %target_instance,
                    "projection reconciliation refused dead letters without fresh exact project authority"
                );
                reports.push(ReconciliationReport {
                    project_id,
                    source_table,
                    target_backend,
                    target_instance,
                    dead_letter_count: dead_count,
                    repair_tasks_enqueued: 0,
                });
                continue;
            }

            // 4. Per-group requeue: DEAD_LETTER → PENDING with
            // retry_count = 0.
            let repaired = match ProjectionTaskStore::requeue_dead_letter_by_source(
                self.store.as_ref(),
                &project_id,
                &source_table,
                &target_backend,
                &target_instance,
            )
            .await
            {
                Ok(n) => n,
                Err(err) => {
                    tracing::warn!(
                        project_id = %project_id,
                        error = %err,
                        "reconciliation repair failed"
                    );
                    0
                }
            };
            for _ in 0..repaired {
                self.metrics
                    .inc_projection_reconciliation_repairs_total(&target_backend, &target_instance);
            }

            reports.push(ReconciliationReport {
                project_id,
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
        let engine = ProjectionEngine::new(self.pool.clone(), self.config.clone());
        for (project_id, manifest) in active_reconciliation_catalogs(&self.catalog) {
            let mut seen = std::collections::BTreeSet::new();
            for plan in ProjectionPlan::from_manifest(&manifest) {
                if !seen.insert(plan.message_type.clone()) {
                    continue;
                }
                match engine
                    .replay_range(
                        &self.runtime,
                        &manifest,
                        &project_id,
                        &plan.message_type,
                        None,
                        None,
                        self.settings.max_source_scan_rows,
                    )
                    .await
                {
                    Ok(inserted) if inserted > 0 => {
                        tracing::info!(
                            project_id = %project_id,
                            message_type = %plan.message_type,
                            inserted,
                            "projection reconciliation enqueued missing or stale source rows",
                        );
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!(
                            project_id = %project_id,
                            message_type = %plan.message_type,
                            error = %err,
                            "projection reconciliation source replay failed",
                        );
                    }
                }
            }
        }
    }
}

fn active_reconciliation_catalogs(catalog: &CatalogManager) -> Vec<(String, Arc<CatalogManifest>)> {
    if !catalog.authority_is_fresh() {
        return Vec::new();
    }
    catalog
        .active_project_ids()
        .into_iter()
        .filter_map(|project_id| {
            catalog
                .active_exact_for(&project_id)
                .map(|state| (project_id, Arc::clone(&state.manifest)))
        })
        .collect()
}

#[cfg(all(test, feature = "postgres", feature = "qdrant"))]
mod live_tests;

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

    #[tokio::test]
    async fn reconciliation_catalog_selection_is_live_and_fails_closed_when_stale() {
        let catalog = Arc::new(CatalogManager::new(CatalogManifest {
            checksum_sha256: "initial-catalog".to_string(),
            ..CatalogManifest::default()
        }));
        assert_eq!(active_reconciliation_catalogs(&catalog).len(), 1);
        assert!(
            validate_projection_task_catalog(
                &catalog,
                crate::runtime::catalog::DEFAULT_PROJECT_ID,
                "initial-catalog",
            )
            .is_ok()
        );
        assert!(
            validate_projection_task_catalog(
                &catalog,
                crate::runtime::catalog::DEFAULT_PROJECT_ID,
                "old-catalog",
            )
            .unwrap_err()
            .contains("does not match active catalog")
        );
        assert!(
            validate_projection_task_catalog(&catalog, "missing-project", "initial-catalog")
                .unwrap_err()
                .contains("no exact active catalog")
        );

        catalog.set_authority_fresh(false);
        assert!(active_reconciliation_catalogs(&catalog).is_empty());
        assert!(
            validate_projection_task_catalog(
                &catalog,
                crate::runtime::catalog::DEFAULT_PROJECT_ID,
                "initial-catalog",
            )
            .unwrap_err()
            .contains("authority became stale")
        );

        catalog.set_authority_fresh(true);
        catalog
            .stage_catalog(
                CatalogManifest {
                    checksum_sha256: "late-activation".to_string(),
                    ..CatalogManifest::default()
                },
                "later-project".to_string(),
                "2.0.0".to_string(),
                "exact".to_string(),
            )
            .await
            .unwrap();
        catalog
            .activate_catalog_for("later-project", "2.0.0")
            .await
            .unwrap();
        let after_activation = active_reconciliation_catalogs(&catalog);
        assert!(after_activation.iter().any(|(project_id, manifest)| {
            project_id == "later-project" && manifest.checksum_sha256 == "late-activation"
        }));

        catalog.replace_durable_active_catalogs(vec![(
            "later-project".to_string(),
            CatalogManifest {
                checksum_sha256: "reloaded-catalog".to_string(),
                ..CatalogManifest::default()
            },
            "2.0.1".to_string(),
            "reloaded-catalog".to_string(),
            "exact".to_string(),
            1,
        )]);
        let after_reload = active_reconciliation_catalogs(&catalog);
        assert!(after_reload.iter().any(|(project_id, manifest)| {
            project_id == "later-project" && manifest.checksum_sha256 == "reloaded-catalog"
        }));
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

    fn tenanted_vector_manifest(
        checksum: &str,
        options: Vec<ManifestStoreOption>,
    ) -> CatalogManifest {
        CatalogManifest {
            checksum_sha256: checksum.to_string(),
            tables: vec![ManifestTable {
                message_name: "Document".to_string(),
                schema: "app".to_string(),
                table: "documents".to_string(),
                primary_key: vec!["id".to_string()],
                columns: vec![
                    crate::generation::manifest::ManifestColumn {
                        column_name: "id".to_string(),
                        ..Default::default()
                    },
                    crate::generation::manifest::ManifestColumn {
                        column_name: "tenant_id".to_string(),
                        is_tenant_column: true,
                        ..Default::default()
                    },
                ],
                ..ManifestTable::default()
            }],
            projections: vec![ManifestProjection {
                message_type: "Document".to_string(),
                projection_kind: "vector".to_string(),
                backend: "qdrant".to_string(),
                resource_name: "documents".to_string(),
                write_policy: "projection".to_string(),
                fanout_policy: "async_projection".to_string(),
                options,
                ..ManifestProjection::default()
            }],
            ..CatalogManifest::default()
        }
    }

    #[test]
    fn projection_plan_names_the_source_tenant_column() {
        let plans =
            ProjectionPlan::from_manifest(&tenanted_vector_manifest("tenant-field", vec![]));
        assert!(
            plans[0].targets[0]
                .options
                .contains(&opt("tenant_field", "tenant_id")),
            "{:?}",
            plans[0].targets[0].options
        );
        // A declared tenant_field (graph/document store option) wins.
        let plans = ProjectionPlan::from_manifest(&tenanted_vector_manifest(
            "tenant-field-declared",
            vec![opt("tenant_field", "org")],
        ));
        let tenant_fields: Vec<_> = plans[0].targets[0]
            .options
            .iter()
            .filter(|o| o.key == "tenant_field")
            .collect();
        assert_eq!(tenant_fields, vec![&opt("tenant_field", "org")]);
    }

    /// The bug: projected points carried no `_tenant_id`/`_project_id`, and
    /// `VectorSearch` ANDs exactly those two keys into every filter, so no
    /// tenant-scoped search could ever see a projected point.
    #[test]
    fn vector_projection_stamps_the_keys_vector_search_filters_on() {
        let plans =
            ProjectionPlan::from_manifest(&tenanted_vector_manifest("vector-stamp", vec![]));
        let options = serde_json::to_value(&plans[0].targets[0].options).unwrap();
        let payload = json!({"id":"d1","tenant_id":"t1","vector":[0.1,0.2]});
        let scope = ProjectionScope::resolve("proj-a", &options, &payload);
        let request = render_projection_mutation(
            "qdrant",
            "vector",
            "documents",
            "upsert",
            &json!({"id":"d1"}),
            &options,
            &payload,
            &scope,
        )
        .unwrap();
        let point_payload = &request["points"][0]["payload"];
        assert_eq!(point_payload["_tenant_id"], "t1");
        assert_eq!(point_payload["_project_id"], "proj-a");
        assert_eq!(point_payload["tenant_id"], "t1", "source columns are kept");

        // A spoofed `_tenant_id` source field cannot override the tenant column.
        let spoofed = json!({"id":"d1","tenant_id":"t1","_tenant_id":"t2","vector":[0.1]});
        let scope = ProjectionScope::resolve("proj-a", &options, &spoofed);
        assert_eq!(scope.stamp(&spoofed)["_tenant_id"], "t1");
    }

    /// `projection_target_supported` (what `udb lint` checks) must agree with
    /// what `render_projection_mutation` (what the worker runs) can render.
    #[test]
    fn lint_predicate_matches_the_worker_dispatch() {
        let options = json!([{"key":"vector_field","value":"vector"}]);
        let payload = json!({"id":"p1","vector":[0.1],"_id":"p1"});
        for backend in [
            "qdrant",
            "mongodb",
            "neo4j",
            "clickhouse",
            "weaviate",
            "pinecone",
            "elasticsearch",
            "milvus",
            "postgres",
        ] {
            let projection = ManifestProjection {
                projection_kind: "vector".to_string(),
                backend: backend.to_string(),
                write_policy: "projection".to_string(),
                fanout_policy: "async_projection".to_string(),
                ..ManifestProjection::default()
            };
            // A target is supported only if the worker can apply BOTH an upsert
            // and a delete onto it.
            let rendered = ["upsert", "delete"].map(|operation| {
                render_projection_mutation(
                    backend,
                    "vector",
                    "r",
                    operation,
                    &json!({"id":"p1"}),
                    &options,
                    &payload,
                    &ProjectionScope::default(),
                )
            });
            let unsupported = rendered.iter().any(|r| {
                matches!(r, Err(e) if e.contains("is not supported")
                    || e.contains("must be handled by the canonical write path"))
            });
            assert_eq!(
                projection_target_supported(&projection),
                !unsupported,
                "{backend}: {rendered:?}"
            );
        }
    }

    /// D9: on a task ledger that cannot order one row's tasks, every keyed
    /// target is refused (dead-lettered with the named reason, which the
    /// reconciliation repair never requeues); only an append-only ClickHouse
    /// target — where order cannot change the outcome — still applies.
    #[test]
    fn unordered_ledgers_refuse_every_keyed_projection_target() {
        let none = json!([]);
        let append_only = json!([{"key": "append_only", "value": "true"}]);
        for backend in [
            "qdrant", "mongodb", "neo4j", "redis", "s3", "minio", "postgres",
        ] {
            assert!(
                projection_task_is_ordering_dependent(backend, &none),
                "{backend} is keyed"
            );
        }
        assert!(projection_task_is_ordering_dependent("clickhouse", &none));
        assert!(!projection_task_is_ordering_dependent(
            "clickhouse",
            &append_only
        ));
        let refusal = projection_ordering_refusal("sqlite");
        assert!(
            refusal.starts_with(PROJECTION_ORDERING_REFUSAL),
            "{refusal}"
        );
        assert!(
            refusal.starts_with(
                crate::runtime::canonical_store::system_store::PROJECTION_AUTHORITY_FAILURE_PREFIX
            ),
            "reconciliation repair must never requeue an ordering refusal: {refusal}"
        );
        assert!(refusal.contains("'sqlite'"), "{refusal}");
    }

    /// ClickHouse cannot apply a projected delete. Only a target declared
    /// append-only is supported (lint), and then a delete is a no-op rather
    /// than a dead letter.
    #[test]
    fn clickhouse_projection_is_supported_only_when_append_only() {
        let projection = |options: Vec<ManifestStoreOption>| ManifestProjection {
            projection_kind: "columnar".to_string(),
            backend: "clickhouse".to_string(),
            write_policy: "projection".to_string(),
            fanout_policy: "async_projection".to_string(),
            options,
            ..ManifestProjection::default()
        };
        assert!(!projection_target_supported(&projection(vec![])));
        assert!(projection_target_supported(&projection(vec![opt(
            "append_only",
            "true"
        )])));
        assert!(supported_projection_backends().contains("append_only"));

        let payload = json!({"id":"p1","amount":3});
        let key = json!({"id":"p1"});
        let scope = ProjectionScope::default();
        let err = render_projection_mutation(
            "clickhouse",
            "columnar",
            "events",
            "delete",
            &key,
            &json!([]),
            &key,
            &scope,
        )
        .unwrap_err();
        assert!(err.contains("is not supported"), "{err}");
        let append_only = json!([{"key":"append_only","value":"true"}]);
        let noop = render_projection_mutation(
            "clickhouse",
            "columnar",
            "events",
            "delete",
            &key,
            &append_only,
            &key,
            &scope,
        )
        .unwrap();
        assert!(noop.is_null(), "{noop}");
        let insert = render_projection_mutation(
            "clickhouse",
            "columnar",
            "events",
            "upsert",
            &key,
            &append_only,
            &payload,
            &scope,
        )
        .unwrap();
        assert_eq!(insert["rows"][0]["amount"], 3);
    }

    fn tenant_scope(tenant: &str) -> (serde_json::Value, ProjectionScope) {
        let options = json!([{"key":"tenant_field","value":"tenant_id"}]);
        let scope = ProjectionScope::resolve("proj-a", &options, &json!({"tenant_id": tenant}));
        (options, scope)
    }

    /// The bug: Mongo documents were keyed by the bare row id, so tenant B's
    /// row with A's id replaced A's document, and B's delete removed it.
    #[test]
    fn mongodb_projection_keys_and_stamps_documents_by_scope() {
        let (options, scope) = tenant_scope("t1");
        let payload = json!({"id":"p1","tenant_id":"t1","name":"Ada"});
        let key = json!({"id":"p1"});
        let upsert = render_projection_mutation(
            "mongodb", "document", "patients", "upsert", &key, &options, &payload, &scope,
        )
        .unwrap();
        assert_eq!(
            upsert["filter"],
            json!({"id":"p1","_tenant_id":"t1","_project_id":"proj-a"})
        );
        assert_eq!(upsert["document"]["_tenant_id"], "t1");
        assert_eq!(upsert["document"]["_project_id"], "proj-a");
        let delete_payload = json!({"id":"p1","tenant_id":"t1"});
        let delete = render_projection_mutation(
            "mongodb",
            "document",
            "patients",
            "delete",
            &key,
            &options,
            &delete_payload,
            &scope,
        )
        .unwrap();
        assert_eq!(delete["filter"]["_tenant_id"], "t1");
        assert_eq!(delete["filter"]["_project_id"], "proj-a");
        assert_eq!(delete["filter"]["id"], "p1");
    }

    /// The bug: Qdrant point ids were the bare row id, so two tenants' rows with
    /// the same primary key collided on one point.
    #[test]
    fn qdrant_projection_point_id_is_scoped_by_tenant_and_project() {
        let payload = |tenant: &str| json!({"id":"p1","tenant_id":tenant,"vector":[0.1]});
        let key = json!({"id":"p1"});
        let point_id = |tenant: &str, operation: &str| {
            let (options, scope) = tenant_scope(tenant);
            let rendered = render_projection_mutation(
                "qdrant",
                "vector",
                "docs",
                operation,
                &key,
                &options,
                &payload(tenant),
                &scope,
            )
            .unwrap();
            if operation == "delete" {
                rendered["point_ids"][0].clone()
            } else {
                rendered["points"][0]["id"].clone()
            }
        };
        assert_eq!(point_id("t1", "upsert"), json!("t:t1/p:proj-a/p1"));
        assert_ne!(point_id("t1", "upsert"), point_id("t2", "upsert"));
        // A delete addresses exactly the point its own tenant's upsert wrote.
        assert_eq!(point_id("t1", "upsert"), point_id("t1", "delete"));
    }

    #[test]
    fn scoped_key_escapes_separators_in_scope_ids() {
        let scope = ProjectionScope {
            tenant_id: Some("a/p:x".to_string()),
            project_id: Some("proj".to_string()),
            unresolved_tenant_field: None,
        };
        assert_eq!(scope.scoped_key("k"), "t:a%2Fp:x/p:proj/k");
        assert_eq!(ProjectionScope::default().scoped_key("k"), "k");
    }

    /// The bug: Redis keys were the bare rendered pattern (shared by every
    /// tenant) and the delete ran it as a SCAN MATCH glob.
    #[test]
    fn redis_projection_key_is_scoped_and_delete_is_exact() {
        let (_, scope) = tenant_scope("t1");
        let key = redis_projection_key(
            "patient:{id}",
            &json!({"id":"p*"}),
            &json!({"id":"p*","tenant_id":"t1"}),
            &scope,
        )
        .unwrap();
        assert_eq!(key, "t:t1/p:proj-a/patient:p*");
        // The delete removes exactly this rendered key (`DEL`, never a SCAN
        // MATCH pattern), so the `*` stays a literal character.
        // An unfilled placeholder would make every such row share one key.
        let err = redis_projection_key("patient:{id}", &json!({}), &json!({}), &scope).unwrap_err();
        assert!(err.contains("placeholder"), "{err}");
    }

    /// The bug: object keys were `{prefix}/{id}.json` for every tenant.
    #[test]
    fn object_projection_key_is_prefixed_by_scope() {
        let (_, scope) = tenant_scope("t1");
        let options = json!([{"key":"key_prefix","value":"/v1/customers/"}]);
        let key = object_projection_key(
            &json!({"id":"c1"}),
            &json!({"id":"c1","tenant_id":"t1"}),
            &options,
            &scope,
        )
        .unwrap();
        assert_eq!(key, "t:t1/p:proj-a/v1/customers/c1.json");
        let bare =
            object_projection_key(&json!({"id":"c1"}), &json!({"id":"c1"}), &json!([]), &scope)
                .unwrap();
        assert_eq!(bare, "t:t1/p:proj-a/c1.json");
    }

    /// D7: a projected delete carries the verified tenant, so a scoped
    /// delete resolves even when the caller's filter named no tenant (or
    /// named it through an operator).
    #[test]
    fn scoped_delete_payload_carries_the_verified_tenant() {
        let manifest = tenanted_vector_manifest("delete-scope", vec![]);
        let payload =
            scoped_delete_payload(&manifest, "Document", &json!({"id":"d1"}), "t-verified");
        assert_eq!(payload, json!({"id":"d1","tenant_id":"t-verified"}));
        // A filter naming the tenant through an operator is overwritten with the
        // verified scalar (the delete itself ran under the verified tenant).
        let payload = scoped_delete_payload(
            &manifest,
            "Document",
            &json!({"id":"d1","tenant_id":{"$eq":"t-other"}}),
            "t-verified",
        );
        assert_eq!(payload["tenant_id"], "t-verified");
        let plans = ProjectionPlan::from_manifest(&manifest);
        let options = serde_json::to_value(&plans[0].targets[0].options).unwrap();
        let scope = ProjectionScope::resolve("proj-a", &options, &payload);
        assert_eq!(scope.tenant_id.as_deref(), Some("t-verified"));
        assert!(scope.require_tenant("delete").is_ok());
        // No verified tenant: the filter is passed through unchanged.
        assert_eq!(
            scoped_delete_payload(&manifest, "Document", &json!({"id":"d1"}), " "),
            json!({"id":"d1"})
        );
    }

    #[test]
    fn worker_settings_default_to_a_bounded_claim_lease() {
        let settings = ProjectionWorkerSettings::default();
        assert!(settings.task_lease_secs >= MIN_TASK_LEASE_SECS);
    }

    /// D9: tasks are ordered by a sequence-drawn revision taken while the
    /// writer holds the row lock, and a row returning to an earlier value
    /// re-arms that value's task with a FRESH revision — whatever its status.
    #[test]
    fn task_insert_orders_by_row_revision_and_rearms_aba_rows() {
        let sql = projection_task_insert_sql("\"udb_system\".\"udb_projection_tasks\"");
        assert!(sql.contains("source_checksum, created_at)"), "{sql}");
        assert!(sql.contains("clock_timestamp())"), "{sql}");
        assert!(
            sql.contains("ON CONFLICT (idempotency_key) DO UPDATE"),
            "{sql}"
        );
        assert!(
            sql.contains("SET row_revision = EXCLUDED.row_revision"),
            "{sql}"
        );
        assert!(
            sql.contains("newer.row_revision > existing.row_revision"),
            "{sql}"
        );
        // An in-flight task keeps its status (its worker is applying exactly
        // this content); every other status is re-armed as PENDING.
        assert!(
            sql.contains("THEN existing.status ELSE 'PENDING' END"),
            "{sql}"
        );
        assert!(
            !sql.contains("WHERE existing.status = 'COMPLETED'"),
            "{sql}"
        );
        assert!(!sql.contains("DO NOTHING"), "{sql}");
    }

    /// Projection, IR and DDL must agree on the node label; declared endpoint
    /// labels ride on the edge so its MATCH is label-indexed.
    #[test]
    fn graph_projection_uses_the_shared_label_resolver_and_endpoint_labels() {
        let payload = json!({"id":"e1","doctor_id":"d1","patient_id":"p1"});
        let key = json!({"id":"e1"});
        let scope = ProjectionScope::default();
        let node = render_projection_mutation(
            "neo4j",
            "graph",
            "clinic.patients",
            "upsert",
            &key,
            &json!([]),
            &payload,
            &scope,
        )
        .unwrap();
        assert_eq!(
            node["label"],
            crate::generation::backends::neo4j::resolve_neo4j_label("clinic.patients", None)
        );
        let options = json!([
            {"key":"udb.neo4j_label","value":"TREATS"},
            {"key":"edge_source_field","value":"doctor_id"},
            {"key":"edge_target_field","value":"patient_id"},
            {"key":"edge_source_label","value":"Doctor"},
            {"key":"edge_target_label","value":"Patient"}
        ]);
        let edge = render_projection_mutation(
            "neo4j",
            "graph",
            "treatments",
            "upsert",
            &key,
            &options,
            &payload,
            &scope,
        )
        .unwrap();
        assert_eq!(edge["rel_type"], "TREATS");
        assert_eq!(edge["from_label"], "Doctor");
        assert_eq!(edge["to_label"], "Patient");
    }

    /// E8: an edge projection that does not declare its endpoint labels gets
    /// them from the manifest — the referenced tables' node labels — so the
    /// edge MATCH and the node projection address the same labels.
    #[test]
    fn graph_edge_endpoint_labels_derive_from_the_manifest() {
        use crate::generation::manifest::ManifestForeignKey;
        let opt = |key: &str, value: &str| ManifestStoreOption {
            key: key.into(),
            value: value.into(),
        };
        let neo4j = |resource: &str, options: Vec<ManifestStoreOption>| ManifestProjection {
            projection_kind: "graph".into(),
            backend: "neo4j".into(),
            resource_name: resource.into(),
            options,
            ..Default::default()
        };
        let fk = |column: &str, ref_table: &str| ManifestForeignKey {
            columns: vec![column.into()],
            ref_schema: "clinic".into(),
            ref_table: ref_table.into(),
            ref_columns: vec!["id".into()],
            ..Default::default()
        };
        let manifest = CatalogManifest {
            tables: vec![
                ManifestTable {
                    message_name: "Doctor".into(),
                    schema: "clinic".into(),
                    table: "doctors".into(),
                    projections: vec![neo4j("doctors", vec![opt("node_label", "Doctor")])],
                    ..Default::default()
                },
                // No node projection: falls back to the shared IR/DDL label.
                ManifestTable {
                    message_name: "Patient".into(),
                    schema: "clinic".into(),
                    table: "patients".into(),
                    ..Default::default()
                },
                ManifestTable {
                    message_name: "Treatment".into(),
                    schema: "clinic".into(),
                    table: "treatments".into(),
                    foreign_keys: vec![fk("doctor_id", "doctors"), fk("patient_id", "patients")],
                    projections: vec![neo4j(
                        "treatments",
                        vec![
                            opt("udb.neo4j_label", "TREATS"),
                            opt("edge_source_field", "doctor_id"),
                            opt("edge_target_field", "patient_id"),
                        ],
                    )],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let options = json!([
            {"key":"udb.neo4j_label","value":"TREATS"},
            {"key":"edge_source_field","value":"doctor_id"},
            {"key":"edge_target_field","value":"patient_id"}
        ]);
        let derived = with_manifest_edge_labels(&manifest, "treatments", &options);
        let payload = json!({"id":"e1","doctor_id":"d1","patient_id":"p1"});
        let edge = render_projection_mutation(
            "neo4j",
            "graph",
            "treatments",
            "upsert",
            &json!({"id":"e1"}),
            &derived,
            &payload,
            &ProjectionScope::default(),
        )
        .unwrap();
        assert_eq!(edge["from_label"], "Doctor");
        assert_eq!(
            edge["to_label"],
            crate::generation::neo4j_labels::neo4j_label_for_table(&manifest, &manifest.tables[1])
        );
        // A declared endpoint label wins over the manifest.
        let declared = json!([
            {"key":"edge_source_field","value":"doctor_id"},
            {"key":"edge_target_field","value":"patient_id"},
            {"key":"from_label","value":"Physician"}
        ]);
        let kept = with_manifest_edge_labels(&manifest, "treatments", &declared);
        assert_eq!(
            option_value(&kept, "from_label").as_deref(),
            Some("Physician")
        );
        assert!(option_value(&kept, "edge_source_label").is_none());
        // Node projections (no edge fields) are untouched.
        let node = json!([{"key":"node_label","value":"Doctor"}]);
        assert_eq!(with_manifest_edge_labels(&manifest, "doctors", &node), node);
    }

    #[test]
    fn graph_projection_refuses_a_record_whose_tenant_it_cannot_resolve() {
        let options = json!([
            {"key":"node_label","value":"Patient"},
            {"key":"tenant_field","value":"tenant_id"}
        ]);
        // A delete filter carrying the tenant as an operator, not a value.
        let filter = json!({"id":"p1","tenant_id":{"$eq":"t1"}});
        let scope = ProjectionScope::resolve("proj-a", &options, &filter);
        let err = render_projection_mutation(
            "neo4j",
            "graph",
            "patients",
            "delete",
            &json!({"id":"p1"}),
            &options,
            &filter,
            &scope,
        )
        .unwrap_err();
        assert!(err.contains("tenant field 'tenant_id'"), "{err}");
    }

    #[test]
    fn task_project_id_resolves_empty_to_the_default_project() {
        assert_eq!(
            task_project_id(""),
            crate::runtime::catalog::DEFAULT_PROJECT_ID
        );
        assert_eq!(
            task_project_id("  "),
            crate::runtime::catalog::DEFAULT_PROJECT_ID
        );
        assert_eq!(task_project_id(" billing "), "billing");
    }

    #[test]
    fn graph_projection_scopes_nodes_by_tenant_and_project() {
        let options = json!([
            {"key":"node_label","value":"Patient"},
            {"key":"tenant_field","value":"tenant_id"}
        ]);
        let payload = json!({"id":"p1","tenant_id":"t1","name":"Ada"});
        let scope = ProjectionScope::resolve("proj-a", &options, &payload);
        let key = json!({"id":"p1"});
        let upsert = render_projection_mutation(
            "neo4j", "graph", "patients", "upsert", &key, &options, &payload, &scope,
        )
        .unwrap();
        assert_eq!(upsert["operation"], "create_node");
        assert_eq!(
            upsert["scope"],
            json!({"_tenant_id":"t1","_project_id":"proj-a"})
        );
        assert_eq!(upsert["properties"]["_tenant_id"], "t1");
        let delete = render_projection_mutation(
            "neo4j", "graph", "patients", "delete", &key, &options, &key, &scope,
        )
        .unwrap();
        assert_eq!(delete["operation"], "delete_node");
        assert_eq!(delete["scope"]["_tenant_id"], "t1");
    }

    /// The bug: `edge_source_field` / `edge_target_field` were never read, so
    /// an edge store projected each row as a lone node and no edge existed.
    #[test]
    fn graph_projection_with_edge_fields_renders_a_scoped_edge() {
        let options = json!([
            {"key":"node_label","value":"TREATS"},
            {"key":"tenant_field","value":"tenant_id"},
            {"key":"edge_source_field","value":"doctor_id"},
            {"key":"edge_target_field","value":"patient_id"}
        ]);
        let payload = json!({"id":"e1","tenant_id":"t1","doctor_id":"d1","patient_id":"p1"});
        let scope = ProjectionScope::resolve("proj-a", &options, &payload);
        let key = json!({"id":"e1"});
        let upsert = render_projection_mutation(
            "neo4j",
            "graph",
            "treatments",
            "upsert",
            &key,
            &options,
            &payload,
            &scope,
        )
        .unwrap();
        assert_eq!(upsert["operation"], "upsert_edge");
        assert_eq!(upsert["rel_type"], "TREATS");
        assert_eq!(upsert["id"], "e1");
        assert_eq!(upsert["from_id"], "d1");
        assert_eq!(upsert["to_id"], "p1");
        assert_eq!(
            upsert["scope"],
            json!({"_tenant_id":"t1","_project_id":"proj-a"})
        );

        let delete = render_projection_mutation(
            "neo4j",
            "graph",
            "treatments",
            "delete",
            &key,
            &options,
            &key,
            &scope,
        )
        .unwrap();
        assert_eq!(delete["operation"], "delete_edge");
        assert_eq!(delete["rel_type"], "TREATS");

        // An edge row without an endpoint value fails (and retries) loudly.
        let missing = json!({"id":"e2","tenant_id":"t1","doctor_id":"d1"});
        let err = render_projection_mutation(
            "neo4j",
            "graph",
            "treatments",
            "upsert",
            &json!({"id":"e2"}),
            &options,
            &missing,
            &scope,
        )
        .unwrap_err();
        assert!(err.contains("patient_id"), "{err}");

        // Half an edge declaration is a configuration error, not a node.
        let half = json!([{"key":"edge_source_field","value":"doctor_id"}]);
        assert!(
            render_projection_mutation(
                "neo4j",
                "graph",
                "treatments",
                "upsert",
                &key,
                &half,
                &payload,
                &scope,
            )
            .is_err()
        );
    }

    #[test]
    fn message_type_match_accepts_full_name_and_leaf_alias() {
        assert!(message_type_matches(
            "SdkLiveRecord",
            "udb.sdk.live.v1.SdkLiveRecord"
        ));
        assert!(message_type_matches(
            "udb.sdk.live.v1.SdkLiveRecord",
            "SdkLiveRecord"
        ));
        assert!(!message_type_matches(
            "udb.sdk.live.v1.SdkLiveRecord",
            "udb.sdk.live.v1.OtherRecord"
        ));
    }

    #[test]
    fn projection_plan_uses_manifest_level_projection_with_full_name_alias() {
        let manifest = CatalogManifest {
            checksum_sha256: "catalog-alias".to_string(),
            tables: vec![ManifestTable {
                message_name: "SdkLiveRecord".to_string(),
                schema: "udb_sdk_live".to_string(),
                table: "sdk_live_records".to_string(),
                primary_key: vec!["record_id".to_string()],
                ..ManifestTable::default()
            }],
            projections: vec![ManifestProjection {
                message_type: "udb.sdk.live.v1.SdkLiveRecord".to_string(),
                projection_kind: "vector".to_string(),
                backend: "qdrant".to_string(),
                resource_name: "sdk_live_records".to_string(),
                write_policy: "projection".to_string(),
                fanout_policy: "async_projection".to_string(),
                ..ManifestProjection::default()
            }],
            ..CatalogManifest::default()
        };

        let plans = ProjectionPlan::from_manifest(&manifest);
        assert_eq!(plans.len(), 1);
        assert!(message_type_matches(
            &plans[0].message_type,
            "udb.sdk.live.v1.SdkLiveRecord"
        ));
        assert_eq!(plans[0].targets.len(), 1);
        assert_eq!(plans[0].targets[0].backend, "qdrant");
    }

    #[test]
    fn renders_backend_specific_projection_mutations() {
        let options =
            json!([{"key":"id_field","value":"id"},{"key":"node_label","value":"Patient"}]);
        let payload = json!({"id":"p1","name":"Ada","vector":[0.1,0.2]});
        let key = json!({"id":"p1"});

        let scope = ProjectionScope::default();

        let mongo = render_projection_mutation(
            "mongodb", "document", "patients", "upsert", &key, &options, &payload, &scope,
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
            &scope,
        )
        .unwrap();
        assert_eq!(qdrant["collection"], "patient_vectors");
        assert_eq!(qdrant["points"][0]["id"], "p1");

        let neo4j = render_projection_mutation(
            "neo4j", "graph", "Patient", "delete", &key, &options, &payload, &scope,
        )
        .unwrap();
        assert_eq!(neo4j["operation"], "delete_node");
        assert_eq!(neo4j["label"], "Patient");
    }

    #[test]
    fn idempotency_key_changes_with_source_checksum() {
        // #166 REVERTED: the idempotency key MUST change when the source-row value
        // (and thus its checksum) changes — otherwise an updated row whose key
        // matches an already-COMPLETED task is deduped/skipped and the projection
        // target is left stale (see `projection_acceptance_tests`).
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
