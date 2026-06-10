//! Native `AssetService` — proto-driven Postgres CRUD + processing-pipeline
//! orchestration over the UDB-owned `udb_asset.{assets,pipeline_definitions,
//! pipeline_instances,pipeline_steps}` tables.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.

use std::sync::Arc;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    admit_on as native_admit_on, emit_payload_event, storage_object_defaults,
    validate_request_scope, validate_request_tenant,
};

const ASSET_MSG: &str = "udb.core.asset.entity.v1.Asset";
const PIPELINE_DEFINITION_MSG: &str = "udb.core.asset.entity.v1.PipelineDefinition";
const PIPELINE_INSTANCE_MSG: &str = "udb.core.asset.entity.v1.PipelineInstance";
const PIPELINE_STEP_MSG: &str = "udb.core.asset.entity.v1.PipelineStep";

/// Vector collection EMBED-step vectors are upserted into, when not overridden by
/// `UDB_ASSET_VECTOR_COLLECTION`.
const DEFAULT_VECTOR_COLLECTION: &str = "udb_asset_embeddings";

/// Postgres-backed `AssetService` handler.
pub struct AssetServiceImpl {
    pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → downstream consumers. `None` = no emit.
    outbox_relation: Option<String>,
    /// Runtime handle used to push `EMBED`-step vectors into the vector backend.
    /// `None` = embeddings stay in the step result only (no vector upsert).
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Mutating/orchestration RPCs acquire a
    /// per-tenant `Object` budget through this so one tenant can't starve shared
    /// pipeline capacity. `None` only in test construction (no runtime wired).
    channels: Option<ChannelManager>,
    /// Vector collection EMBED vectors are upserted into.
    vector_collection: String,
    metrics: Arc<dyn MetricsRecorder>,
}

// ── outbox topics (dot-only per Kafka topic policy) ───────────────────────────
const ASSET_REGISTERED_TOPIC: &str = "udb.asset.asset.registered.v1";
const PIPELINE_STARTED_TOPIC: &str = "udb.asset.pipeline.started.v1";
const PIPELINE_STEP_COMPLETED_TOPIC: &str = "udb.asset.pipeline.step_completed.v1";
const PIPELINE_COMPLETED_TOPIC: &str = "udb.asset.pipeline.completed.v1";
const PIPELINE_FAILED_TOPIC: &str = "udb.asset.pipeline.failed.v1";

impl AssetServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            runtime: None,
            channels: None,
            vector_collection: DEFAULT_VECTOR_COLLECTION.to_string(),
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the runtime handle + target collection so completed `EMBED` steps
    /// upsert their vector into the vector backend (best-effort).
    pub(crate) fn with_vector(
        mut self,
        runtime: Option<Arc<DataBrokerRuntime>>,
        collection: String,
    ) -> Self {
        // Capture the shared per-tenant fair-admission manager (same path as the
        // data plane) so mutating RPCs acquire the per-tenant Object budget.
        self.channels = runtime.as_ref().map(|rt| rt.channels().clone());
        self.runtime = runtime;
        if !collection.trim().is_empty() {
            self.vector_collection = collection;
        }
        self
    }

    fn encrypt_native_json_state(&self, raw_json: &str) -> Result<String, Status> {
        match self.runtime.as_ref() {
            Some(runtime) => runtime
                .encrypt_native_json_state_at_rest(raw_json)
                .map_err(|err| {
                    Status::failed_precondition(format!("native-state encryption failed: {err}"))
                }),
            None => Ok(raw_json.to_string()),
        }
    }

    fn decrypt_native_json_state(&self, stored_json: &str) -> Result<String, Status> {
        if stored_json.trim().is_empty() {
            return Ok(String::new());
        }
        match self.runtime.as_ref() {
            Some(runtime) => runtime
                .decrypt_native_json_state_at_rest(stored_json)
                .map_err(|err| {
                    Status::failed_precondition(format!("native-state decryption failed: {err}"))
                }),
            None => Ok(stored_json.to_string()),
        }
    }

    /// Per-tenant fair admission for a mutating/orchestration asset RPC.
    /// Acquires the shared `Object` channel budget SCOPED to the validated tenant
    /// (+ project) so a single tenant's pipeline flood cannot starve other
    /// tenants — the exact path the data plane uses via
    /// `execute_with_channel_scoped`. On exhaustion returns the same
    /// `Status::resource_exhausted` backpressure as the data plane. The returned
    /// [`ChannelPermit`] must be held for the whole RPC (drop = release). `None`
    /// channels (no runtime — test mode) admit without a permit.
    ///
    /// `tenant` MUST be the VALIDATED tenant (post `validate_request_*`).
    async fn admit(&self, tenant: &str, project: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "asset",
            OperationChannel::Object,
            tenant,
            Some(project),
        )
        .await
    }

    /// Lighter per-tenant fair admission for a READ RPC (get/list pipeline/asset).
    /// Acquires the cheap `Read` channel budget scoped to the validated tenant so
    /// one tenant cannot exhaust the shared pool with reads, without charging the
    /// heavier `Object` cost the mutating/orchestration RPCs pay.
    async fn admit_read(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "asset",
            OperationChannel::Read,
            tenant,
            Some(""),
        )
        .await
    }

    /// Best-effort: push a completed EMBED step's vector into the vector backend.
    /// `point_id` is the asset id; the embedding + dim come from the step result.
    /// Never fails the pipeline — a vector-backend outage just logs.
    async fn upsert_embedding(
        &self,
        project_id: &str,
        point_id: &str,
        result: &serde_json::Value,
    ) -> Option<VectorEmbeddingTarget> {
        let Some(runtime) = self.runtime.as_ref() else {
            return None;
        };
        let Some(arr) = result.get("embedding").and_then(|e| e.as_array()) else {
            return None;
        };
        let vector: Vec<f32> = arr
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect();
        if vector.is_empty() {
            return None;
        }
        let dim = result
            .get("dim")
            .and_then(|d| d.as_i64())
            .unwrap_or(vector.len() as i64) as i32;
        let point = crate::proto::VectorPointMutation {
            id: point_id.to_string(),
            vector,
            payload: None,
        };
        let vector_instance = runtime
            .choose_instance_name_for_project("qdrant", true, project_id)
            .map(str::to_string)
            .unwrap_or_else(|| "default".to_string());
        if let Err(err) = runtime
            .vector_upsert_backend_target(
                Some(&vector_instance),
                project_id,
                &self.vector_collection,
                dim,
                vec![point],
            )
            .await
        {
            tracing::warn!(error = %err, collection = %self.vector_collection, point_id, "asset embedding vector upsert failed");
            None
        } else {
            Some(VectorEmbeddingTarget {
                project_id: project_id.to_string(),
                instance: vector_instance,
            })
        }
    }

    /// Best-effort: remove an asset's embedding (point id = asset_id) from the
    /// vector backend. Called on pipeline failure so a failed run leaves no orphan
    /// vector. Never fails the caller.
    async fn delete_embedding(
        &self,
        project_id: &str,
        vector_instance: Option<&str>,
        point_id: &str,
    ) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        if point_id.trim().is_empty() {
            return;
        }
        if let Err(err) = runtime
            .vector_delete_backend_target(
                vector_instance,
                project_id,
                &self.vector_collection,
                vec![point_id.to_string()],
            )
            .await
        {
            tracing::warn!(error = %err, collection = %self.vector_collection, point_id, "asset embedding vector delete failed");
        }
    }

    /// CDC trigger handler: on a finalized storage file
    /// (`udb.storage.file.finalized.v1`), auto-register the asset and start the
    /// tenant's active pipeline whose `media_type` matches the file's content type.
    /// Idempotent: the asset is reused per `file_id`, and the pipeline is deduped on
    /// `correlation_id = file_id`. Returns the started instance id, or `None` when
    /// the file is gone or no matching active pipeline definition exists (no-op).
    pub(crate) async fn handle_storage_finalized(
        &self,
        file_id: &str,
        tenant_id: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let tenant_uuid = parse_uuid("tenant_id", tenant_id)?;
        let file_uuid = parse_uuid("file_id", file_id)?;

        // Resolve the file's content_type + filename from storage (proto-driven).
        let fm = native_model(
            "udb.core.storage.entity.v1.File",
            &["file_id", "content_type", "filename"],
        );
        // Tenant-bound file lookup: only act on a file owned by this tenant.
        let frow = sqlx::query(&format!(
            "SELECT {ct}, {fname}, {project_id} FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL",
            ct = fm.text_or_empty_as("content_type", "content_type"),
            fname = fm.text_or_empty_as("filename", "filename"),
            project_id = fm.text_or_empty_as("project_id", "project_id"),
            rel = fm.relation,
            fid = fm.q("file_id"),
            tid = fm.q("tenant_id"),
            del = fm.q("deleted_at"),
        ))
        .bind(file_uuid)
        .bind(tenant_uuid)
        .fetch_optional(pool)
        .await
        .map_err(|e| Status::internal(format!("resolve finalized file failed: {e}")))?;
        let Some(frow) = frow else {
            return Ok(None);
        };
        let content_type: String = frow.try_get("content_type").unwrap_or_default();
        let filename: String = frow.try_get("filename").unwrap_or_default();
        let project_id: String = frow.try_get("project_id").unwrap_or_default();
        // image/png → "image"; falls back to the whole string if no slash.
        let media_type = content_type
            .split('/')
            .next()
            .unwrap_or("")
            .trim()
            .to_string();

        // Match an active pipeline definition for this tenant + media type.
        let dm = pipeline_definition_model();
        let def_id: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {did}::TEXT FROM {rel} \
             WHERE {tid} = $1::UUID AND {mt} = $2 AND {status} = 'ACTIVE' \
             ORDER BY {ver} DESC LIMIT 1",
            did = dm.q("definition_id"),
            rel = dm.relation,
            tid = dm.q("tenant_id"),
            mt = dm.q("media_type"),
            status = dm.q("status"),
            ver = dm.q("version"),
        ))
        .bind(tenant_uuid)
        .bind(&media_type)
        .fetch_optional(pool)
        .await
        .map_err(|e| Status::internal(format!("match pipeline definition failed: {e}")))?;
        let Some(definition_id) = def_id else {
            return Ok(None);
        };

        // Reuse an existing asset for this file, else register one.
        let am = asset_model();
        let existing: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {aid}::TEXT FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL LIMIT 1",
            aid = am.q("asset_id"),
            rel = am.relation,
            fid = am.q("file_id"),
            tid = am.q("tenant_id"),
            del = am.q("deleted_at"),
        ))
        .bind(file_uuid)
        .bind(tenant_uuid)
        .fetch_optional(pool)
        .await
        .map_err(|e| Status::internal(format!("lookup asset for file failed: {e}")))?;
        let asset_id = match existing {
            Some(a) => a,
            None => {
                self.register_asset(Request::new(asset_pb::RegisterAssetRequest {
                    tenant_id: tenant_id.to_string(),
                    project_id: project_id.clone(),
                    file_id: file_id.to_string(),
                    name: if filename.is_empty() {
                        file_id.to_string()
                    } else {
                        filename
                    },
                    media_type: media_type.clone(),
                    ..Default::default()
                }))
                .await?
                .into_inner()
                .asset_id
            }
        };

        // Start the pipeline, idempotent on correlation_id = file_id.
        let started = self
            .start_pipeline(Request::new(asset_pb::StartPipelineRequest {
                tenant_id: tenant_id.to_string(),
                definition_id,
                asset_id,
                correlation_id: file_id.to_string(),
                ..Default::default()
            }))
            .await?
            .into_inner();
        Ok(Some(started.instance_id))
    }

    /// Resolve a storage file's `object_key` (UDB-owned `udb_storage.files`),
    /// **tenant-bound** so a byte step can only read a file owned by its tenant.
    /// Proto-driven via the embedded manifest — no hardcoded table/columns.
    async fn resolve_object_key(
        &self,
        pool: &PgPool,
        file_id: Uuid,
        tenant_id: Uuid,
    ) -> Option<String> {
        let m = native_model(
            "udb.core.storage.entity.v1.File",
            &["file_id", "object_key"],
        );
        let rel = m.relation.clone();
        sqlx::query_scalar::<_, String>(&format!(
            "SELECT {ok}::TEXT FROM {rel} \
             WHERE {fid} = $1::UUID AND {tid} = $2::UUID AND {del} IS NULL",
            ok = m.q("object_key"),
            fid = m.q("file_id"),
            tid = m.q("tenant_id"),
            del = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
    }

    /// Run a byte-IO step (THUMBNAIL/RESIZE): fetch the source object bytes,
    /// transform them, and store a derived object. Image processing is behind the
    /// `asset-image` feature; without it the step fails explicitly (no fake
    /// success). Source bytes/derived objects use the same object backend+bucket
    /// as the storage service (`UDB_STORAGE_OBJECT_BACKEND` / `UDB_STORAGE_BUCKET`).
    async fn run_byte_step(
        &self,
        pool: &PgPool,
        step_type_i32: i32,
        file_id_str: &str,
        tenant_id: Uuid,
        project_id: &str,
    ) -> StepOutcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return StepOutcome::Failed(
                "byte steps require a runtime object handle (none configured)".to_string(),
            );
        };
        let Ok(file_id) = Uuid::parse_str(file_id_str.trim()) else {
            return StepOutcome::Failed("asset has no valid file_id for a byte step".to_string());
        };
        let Some(object_key) = self.resolve_object_key(pool, file_id, tenant_id).await else {
            return StepOutcome::Failed("source file not found for tenant".to_string());
        };
        let (backend, bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );

        #[cfg(not(feature = "asset-image"))]
        {
            let _ = (
                runtime,
                step_type_i32,
                &object_key,
                &backend,
                &bucket,
                project_id,
            );
            StepOutcome::Failed(
                "THUMBNAIL/RESIZE require the `asset-image` feature build".to_string(),
            )
        }
        #[cfg(feature = "asset-image")]
        {
            let _ = step_type_i32;
            let get_req = crate::runtime::core::setup_data::object_request_json(
                "get",
                &bucket,
                &object_key,
                "",
            );
            let bytes = match runtime
                .get_object_backend_target_for_project(&backend, None, project_id, &get_req)
                .await
            {
                Ok(b) => b,
                Err(err) => {
                    return StepOutcome::Failed(format!("fetch source bytes failed: {err}"));
                }
            };
            let img = match image::load_from_memory(&bytes) {
                Ok(i) => i,
                Err(err) => return StepOutcome::Failed(format!("decode image failed: {err}")),
            };
            let thumb = img.thumbnail(256, 256);
            let mut out = std::io::Cursor::new(Vec::new());
            if let Err(err) = thumb.write_to(&mut out, image::ImageFormat::Png) {
                return StepOutcome::Failed(format!("encode thumbnail failed: {err}"));
            }
            let derived_key = format!("{object_key}.thumb.png");
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &bucket,
                &derived_key,
                "image/png",
            );
            if let Err(err) = runtime
                .put_object_backend_target_for_project(
                    &backend,
                    None,
                    project_id,
                    &put_req,
                    out.into_inner(),
                )
                .await
            {
                return StepOutcome::Failed(format!("store derived object failed: {err}"));
            }
            StepOutcome::Completed(serde_json::json!({
                "derived_object_key": derived_key,
                "width": thumb.width(),
                "height": thumb.height(),
                "format": "png",
            }))
        }
    }

    /// Wire the transactional outbox so asset/pipeline lifecycle events publish
    /// domain events to Kafka (via the CDC relay). `relation` is the
    /// schema-qualified table, e.g. `"udb_system"."outbox_events"`.
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    /// Asset CRUD is durable-only: fail closed when no Postgres pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "asset service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

// ── pure-Rust step execution ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct VectorEmbeddingTarget {
    project_id: String,
    instance: String,
}

enum StepOutcome {
    Completed(serde_json::Value),
    Failed(String),
}

/// THUMBNAIL/RESIZE are byte-IO steps run async (fetch→transform→store) outside
/// the sync metadata-step registry.
fn is_byte_step(step_type: i32) -> bool {
    use asset_entity_pb::StepType as T;
    matches!(T::try_from(step_type), Ok(T::Thumbnail) | Ok(T::Resize))
}

/// Inputs available to a step without object bytes (object-byte fetch is a
/// separate, not-yet-wired gap).
struct StepContext<'a> {
    asset_name: &'a str,
    metadata_json: &'a str,
}

/// Executes one asset-pipeline step. Implementations are pure/in-process for v1.
trait AssetStepExecutor: Send + Sync {
    /// The proto StepType enum value this executor handles.
    fn step_type(&self) -> i32;
    fn execute(&self, ctx: &StepContext) -> StepOutcome;
}

/// Signed feature-hashing embedding (the "hashing trick") — a real, deterministic,
/// dependency-free text embedding. Not a neural model, but a legitimate scheme.
fn embed_text(text: &str, dim: usize) -> Vec<f32> {
    let mut v = vec![0f32; dim];
    for token in text.split_whitespace() {
        let mut h: u64 = 0xcbf29ce484222325;
        for b in token.to_ascii_lowercase().bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        let idx = (h % dim as u64) as usize;
        v[idx] += if (h >> 1) & 1 == 0 { 1.0 } else { -1.0 };
    }
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

/// EMBED: signed feature-hashing embedding over `asset_name + metadata`.
struct EmbedStepExecutor;
impl AssetStepExecutor for EmbedStepExecutor {
    fn step_type(&self) -> i32 {
        asset_entity_pb::StepType::Embed as i32
    }
    fn execute(&self, ctx: &StepContext) -> StepOutcome {
        let text = format!("{} {}", ctx.asset_name, ctx.metadata_json);
        let emb = embed_text(&text, 64);
        StepOutcome::Completed(serde_json::json!({ "embedding": emb, "dim": 64 }))
    }
}

/// EXTRACT: trivial text extraction from the available (non-byte) inputs.
struct ExtractStepExecutor;
impl AssetStepExecutor for ExtractStepExecutor {
    fn step_type(&self) -> i32 {
        asset_entity_pb::StepType::Extract as i32
    }
    fn execute(&self, ctx: &StepContext) -> StepOutcome {
        let text = format!("{} {}", ctx.asset_name, ctx.metadata_json);
        StepOutcome::Completed(serde_json::json!({
            "text": text.trim(),
            "chars": text.trim().chars().count(),
        }))
    }
}

/// Registry of step executors keyed by proto `StepType` enum value. Adding a new
/// step type = register an executor in [`StepRegistry::default_registry`]; the
/// pipeline orchestration (`start_pipeline`) never changes.
struct StepRegistry {
    by_type: std::collections::HashMap<i32, Box<dyn AssetStepExecutor>>,
}

impl StepRegistry {
    fn default_registry() -> Self {
        let mut by_type: std::collections::HashMap<i32, Box<dyn AssetStepExecutor>> =
            std::collections::HashMap::new();
        for executor in [
            Box::new(EmbedStepExecutor) as Box<dyn AssetStepExecutor>,
            Box::new(ExtractStepExecutor) as Box<dyn AssetStepExecutor>,
        ] {
            by_type.insert(executor.step_type(), executor);
        }
        Self { by_type }
    }

    /// Dispatch by `step_type`. Unregistered types (incl. media steps) fail
    /// EXPLICITLY with a clear "not yet implemented" message — keeping the
    /// no-capability-lies contract (no faked success).
    fn run(&self, step_type: i32, ctx: &StepContext) -> StepOutcome {
        match self.by_type.get(&step_type) {
            Some(executor) => executor.execute(ctx),
            None => {
                use asset_entity_pb::StepType as T;
                let name = T::try_from(step_type)
                    .map(|t| t.as_str_name())
                    .unwrap_or("STEP_TYPE_UNSPECIFIED");
                StepOutcome::Failed(format!(
                    "step type {name} not yet implemented \
                     (needs object-store integration + asset-media)"
                ))
            }
        }
    }
}

/// Process-wide default registry, built once. Adding a step type means editing
/// [`StepRegistry::default_registry`] only — not this accessor or `start_pipeline`.
fn step_registry() -> &'static StepRegistry {
    static REGISTRY: std::sync::OnceLock<StepRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(StepRegistry::default_registry)
}

/// Roll a pipeline instance to a terminal state when all steps are accounted for,
/// or to FAILED on any failed step. Shared by `start_pipeline` (inline execution)
/// and `complete_step` (externally-driven). Emits the terminal domain event.
/// Returns the terminal status token (`"COMPLETED"`/`"FAILED"`) if one was set.
async fn advance_instance(
    svc: &AssetServiceImpl,
    pool: &PgPool,
    instance_id: Uuid,
    tenant_id: Uuid,
) -> Result<Option<&'static str>, Status> {
    let step = pipeline_step_model();
    let step_rel = step.relation.clone();
    let counts = sqlx::query(&format!(
        "SELECT \
           COUNT(*) AS total, \
           COUNT(*) FILTER (WHERE {status} IN ('COMPLETED', 'SKIPPED')) AS done, \
           COUNT(*) FILTER (WHERE {status} = 'FAILED') AS failed \
         FROM {step_rel} WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
        status = step.q("status"),
        instance_id = step.q("instance_id"),
        tenant_id = step.q("tenant_id"),
    ))
    .bind(instance_id)
    .bind(tenant_id)
    .fetch_one(pool)
    .await
    .map_err(|err| Status::internal(format!("aggregate step status failed: {err}")))?;
    let total: i64 = counts
        .try_get("total")
        .map_err(|e| Status::internal(format!("decode total failed: {e}")))?;
    let done: i64 = counts
        .try_get("done")
        .map_err(|e| Status::internal(format!("decode done failed: {e}")))?;
    let failed: i64 = counts
        .try_get("failed")
        .map_err(|e| Status::internal(format!("decode failed failed: {e}")))?;

    let new_instance_status = if failed > 0 {
        Some("FAILED")
    } else if total > 0 && done == total {
        Some("COMPLETED")
    } else {
        None
    };
    if let Some(terminal) = new_instance_status {
        let inst = pipeline_instance_model();
        let inst_rel = inst.relation.clone();
        sqlx::query(&format!(
            "UPDATE {inst_rel} SET {status} = $3, {completed_at} = CURRENT_TIMESTAMP \
             WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
            status = inst.q("status"),
            completed_at = inst.q("completed_at"),
            instance_id = inst.q("instance_id"),
            tenant_id = inst.q("tenant_id"),
        ))
        .bind(instance_id)
        .bind(tenant_id)
        .bind(terminal)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("advance pipeline instance failed: {err}")))?;

        let topic = if terminal == "FAILED" {
            PIPELINE_FAILED_TOPIC
        } else {
            PIPELINE_COMPLETED_TOPIC
        };
        emit_payload_event(
            pool,
            svc.outbox_relation.as_deref(),
            topic,
            &instance_id.to_string(),
            serde_json::json!({
                "instance_id": instance_id.to_string(),
                "tenant_id": tenant_id.to_string(),
                "status": terminal,
            }),
            Some(&svc.metrics),
        )
        .await;

        // On failure, remove the asset's embedding so a failed run leaves no
        // orphan vector behind (best-effort).
        if terminal == "FAILED"
            && let Ok(Some(row)) = sqlx::query(&format!(
                "SELECT i.{asset_id}::TEXT AS asset_id, COALESCE(a.{project_id}::TEXT, '') AS project_id, \
                        COALESCE(s.{result}::TEXT, '{{}}') AS vector_result \
                 FROM {inst_rel} i \
                 LEFT JOIN {asset_rel} a ON a.{asset_pk} = i.{asset_id} AND a.{asset_tenant} = i.{tenant_id} \
                 LEFT JOIN LATERAL ( \
                    SELECT {step_result} \
                    FROM {step_rel} \
                    WHERE {step_instance_id} = i.{instance_id} \
                      AND {step_tenant_id} = i.{tenant_id} \
                      AND {step_type} = 'EMBED' \
                      AND {step_status} = 'COMPLETED' \
                    ORDER BY {step_completed_at} DESC NULLS LAST \
                    LIMIT 1 \
                 ) s ON TRUE \
                 WHERE i.{instance_id} = $1::UUID AND i.{tenant_id} = $2::UUID",
                asset_id = inst.q("asset_id"),
                asset_rel = asset_model().relation,
                asset_pk = asset_model().q("asset_id"),
                asset_tenant = asset_model().q("tenant_id"),
                project_id = asset_model().q("project_id"),
                result = step.q("result"),
                step_result = step.q("result"),
                step_rel = step.relation,
                step_instance_id = step.q("instance_id"),
                step_tenant_id = step.q("tenant_id"),
                step_type = step.q("step_type"),
                step_status = step.q("status"),
                step_completed_at = step.q("completed_at"),
                instance_id = inst.q("instance_id"),
                tenant_id = inst.q("tenant_id"),
            ))
            .bind(instance_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await
        {
            if let Ok(asset_id) = row.try_get::<String, _>("asset_id") {
                let fallback_project = row.try_get::<String, _>("project_id").unwrap_or_default();
                let vector_result = row
                    .try_get::<String, _>("vector_result")
                    .unwrap_or_else(|_| "{}".to_string());
                let decoded = svc
                    .decrypt_native_json_state(&vector_result)
                    .unwrap_or(vector_result);
                let vector_target = serde_json::from_str::<serde_json::Value>(&decoded).ok();
                let vector_project = vector_target
                    .as_ref()
                    .and_then(|value| value.get("vector_project_id"))
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or(&fallback_project);
                let vector_instance = vector_target
                    .as_ref()
                    .and_then(|value| value.get("vector_backend_instance"))
                    .and_then(|value| value.as_str())
                    .filter(|value| !value.trim().is_empty());
                svc.delete_embedding(vector_project, vector_instance, &asset_id)
                    .await;
            }
        }
    }
    Ok(new_instance_status)
}

impl Default for AssetServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── native models (table + column resolution from the embedded proto manifest) ─

fn asset_model() -> NativeModel {
    native_model(
        ASSET_MSG,
        &[
            "asset_id",
            "tenant_id",
            "project_id",
            "file_id",
            "name",
            "media_type",
            "status",
            "metadata",
        ],
    )
}

fn pipeline_definition_model() -> NativeModel {
    native_model(
        PIPELINE_DEFINITION_MSG,
        &[
            "definition_id",
            "tenant_id",
            "name",
            "description",
            "media_type",
            "steps",
            "version",
            "status",
        ],
    )
}

fn pipeline_instance_model() -> NativeModel {
    native_model(
        PIPELINE_INSTANCE_MSG,
        &[
            "instance_id",
            "definition_id",
            "asset_id",
            "tenant_id",
            "status",
            "current_step",
            "context",
            "correlation_id",
            "started_at",
            "completed_at",
        ],
    )
}

fn pipeline_step_model() -> NativeModel {
    native_model(
        PIPELINE_STEP_MSG,
        &[
            "step_id",
            "instance_id",
            "tenant_id",
            "step_name",
            "step_type",
            "status",
            "result",
            "error",
            "retry_count",
            "started_at",
            "completed_at",
        ],
    )
}

use super::native_helpers::{non_empty_json, parse_uuid};

// ── enum<->db (stored as SHORT tokens in VARCHAR(20) via the proto_enum serializer) ─

fn asset_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::AssetStatus as S;
    match value {
        "PENDING" | "ASSET_STATUS_PENDING" => S::Pending as i32,
        "READY" | "ASSET_STATUS_READY" => S::Ready as i32,
        "FAILED" | "ASSET_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

fn asset_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "ASSET_STATUS_PENDING" => "PENDING",
        "READY" | "ASSET_STATUS_READY" => "READY",
        "FAILED" | "ASSET_STATUS_FAILED" => "FAILED",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown asset status: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn pipeline_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::PipelineStatus as S;
    match value {
        "PENDING" | "PIPELINE_STATUS_PENDING" => S::Pending as i32,
        "RUNNING" | "PIPELINE_STATUS_RUNNING" => S::Running as i32,
        "COMPLETED" | "PIPELINE_STATUS_COMPLETED" => S::Completed as i32,
        "FAILED" | "PIPELINE_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

fn step_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::StepStatus as S;
    match value {
        "PENDING" | "STEP_STATUS_PENDING" => S::Pending as i32,
        "RUNNING" | "STEP_STATUS_RUNNING" => S::Running as i32,
        "COMPLETED" | "STEP_STATUS_COMPLETED" => S::Completed as i32,
        "SKIPPED" | "STEP_STATUS_SKIPPED" => S::Skipped as i32,
        "FAILED" | "STEP_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

/// Normalize a step-status string to the canonical SHORT stored token. Accepts
/// the short or proto-prefixed form, empty→`default`, rejects unknown input so
/// it never overflows VARCHAR(20) or reads back as Unspecified.
fn step_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "STEP_STATUS_PENDING" => "PENDING",
        "RUNNING" | "STEP_STATUS_RUNNING" => "RUNNING",
        "COMPLETED" | "STEP_STATUS_COMPLETED" => "COMPLETED",
        "SKIPPED" | "STEP_STATUS_SKIPPED" => "SKIPPED",
        "FAILED" | "STEP_STATUS_FAILED" => "FAILED",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown step status: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn step_type_from_db(value: &str) -> i32 {
    use asset_entity_pb::StepType as T;
    match value {
        "EMBED" | "STEP_TYPE_EMBED" => T::Embed as i32,
        "THUMBNAIL" | "STEP_TYPE_THUMBNAIL" => T::Thumbnail as i32,
        "RESIZE" | "STEP_TYPE_RESIZE" => T::Resize as i32,
        "TRANSCODE" | "STEP_TYPE_TRANSCODE" => T::Transcode as i32,
        "CAPTION" | "STEP_TYPE_CAPTION" => T::Caption as i32,
        "EXTRACT" | "STEP_TYPE_EXTRACT" => T::Extract as i32,
        _ => T::Unspecified as i32,
    }
}

/// Normalize a step-type string to the canonical SHORT stored token. Same
/// accept-both-forms / reject-unknown / empty→default contract as
/// [`step_status_to_db`].
fn step_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "EMBED" | "STEP_TYPE_EMBED" => "EMBED",
        "THUMBNAIL" | "STEP_TYPE_THUMBNAIL" => "THUMBNAIL",
        "RESIZE" | "STEP_TYPE_RESIZE" => "RESIZE",
        "TRANSCODE" | "STEP_TYPE_TRANSCODE" => "TRANSCODE",
        "CAPTION" | "STEP_TYPE_CAPTION" => "CAPTION",
        "EXTRACT" | "STEP_TYPE_EXTRACT" => "EXTRACT",
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown step type: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

// ── projections + row mappers ─────────────────────────────────────────────────

fn asset_select_projection(m: &NativeModel) -> String {
    [
        m.text("asset_id"),
        m.text("tenant_id"),
        m.text_or_empty("project_id"),
        m.text("file_id"),
        m.text_or_empty("name"),
        m.text_or_empty("media_type"),
        m.text_or_empty("status"),
        m.text_or_empty("metadata"),
    ]
    .join(", ")
}

fn asset_from_row(row: &sqlx::postgres::PgRow) -> Result<asset_entity_pb::Asset, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode asset failed: {e}"));
    Ok(asset_entity_pb::Asset {
        asset_id: row.try_get("asset_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        file_id: row.try_get("file_id").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        media_type: row.try_get("media_type").map_err(map)?,
        status: asset_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        metadata: row.try_get("metadata").map_err(map)?,
        ..Default::default()
    })
}

fn pipeline_definition_select_projection(m: &NativeModel) -> String {
    [
        m.text("definition_id"),
        m.text("tenant_id"),
        m.text_or_empty("name"),
        m.text_or_empty("description"),
        m.text_or_empty("media_type"),
        m.text_or_empty("steps"),
        m.select("version"),
        m.text_or_empty("status"),
    ]
    .join(", ")
}

fn pipeline_definition_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineDefinition, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode pipeline definition failed: {e}"));
    Ok(asset_entity_pb::PipelineDefinition {
        definition_id: row.try_get("definition_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        description: row.try_get("description").map_err(map)?,
        media_type: row.try_get("media_type").map_err(map)?,
        steps: row.try_get("steps").map_err(map)?,
        version: row.try_get::<i32, _>("version").map_err(map)?,
        status: row.try_get("status").map_err(map)?,
        ..Default::default()
    })
}

fn pipeline_instance_select_projection(m: &NativeModel) -> String {
    [
        m.text("instance_id"),
        m.text("definition_id"),
        m.text("asset_id"),
        m.text("tenant_id"),
        m.text_or_empty("status"),
        m.text_or_empty("current_step"),
        m.text_or_empty("context"),
        m.text_or_empty("correlation_id"),
    ]
    .join(", ")
}

fn pipeline_instance_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineInstance, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode pipeline instance failed: {e}"));
    Ok(asset_entity_pb::PipelineInstance {
        instance_id: row.try_get("instance_id").map_err(map)?,
        definition_id: row.try_get("definition_id").map_err(map)?,
        asset_id: row.try_get("asset_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        status: pipeline_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        current_step: row.try_get("current_step").map_err(map)?,
        context: row.try_get("context").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        ..Default::default()
    })
}

fn pipeline_step_select_projection(m: &NativeModel) -> String {
    [
        m.text("step_id"),
        m.text("instance_id"),
        m.text("tenant_id"),
        m.text_or_empty("step_name"),
        m.text_or_empty("step_type"),
        m.text_or_empty("status"),
        m.text_or_empty("result"),
        m.text_or_empty("error"),
        m.select("retry_count"),
    ]
    .join(", ")
}

fn pipeline_step_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineStep, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode pipeline step failed: {e}"));
    Ok(asset_entity_pb::PipelineStep {
        step_id: row.try_get("step_id").map_err(map)?,
        instance_id: row.try_get("instance_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        step_name: row.try_get("step_name").map_err(map)?,
        step_type: step_type_from_db(&row.try_get::<String, _>("step_type").map_err(map)?),
        status: step_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        result: row.try_get("result").map_err(map)?,
        error: row.try_get("error").map_err(map)?,
        retry_count: row.try_get::<i32, _>("retry_count").map_err(map)?,
        ..Default::default()
    })
}

#[tonic::async_trait]
impl AssetService for AssetServiceImpl {
    async fn create_pipeline_definition(
        &self,
        request: Request<asset_pb::CreatePipelineDefinitionRequest>,
    ) -> Result<Response<asset_pb::CreatePipelineDefinitionResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (Write budget) so one tenant's definition
        // writes can't starve the shared pool.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "asset",
            OperationChannel::Write,
            &req.tenant_id,
            Some(""),
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        let pool = self.require_pool()?;
        let m = pipeline_definition_model();
        let rel = m.relation.clone();
        let steps = {
            let s = req.steps.trim();
            if s.is_empty() {
                "[]".to_string()
            } else {
                serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                    Status::invalid_argument(format!("steps must be valid JSON: {e}"))
                })?;
                s.to_string()
            }
        };
        let version = if req.version > 0 { req.version } else { 1 };
        let definition_id = Uuid::new_v4().to_string();
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({definition_id}, {tenant_id}, {name}, {description}, {media_type}, {steps}, {version}, {status}) \
             VALUES ($1::UUID, $2::UUID, $3, $4, $5, $6::JSONB, $7, 'ACTIVE')",
            definition_id = m.q("definition_id"),
            tenant_id = m.q("tenant_id"),
            name = m.q("name"),
            description = m.q("description"),
            media_type = m.q("media_type"),
            steps = m.q("steps"),
            version = m.q("version"),
            status = m.q("status"),
        ))
        .bind(&definition_id)
        .bind(tenant_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(&req.media_type)
        .bind(&steps)
        .bind(version)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("create pipeline definition failed: {err}")))?;
        Ok(Response::new(asset_pb::CreatePipelineDefinitionResponse {
            definition_id,
            message: "pipeline definition created".to_string(),
            error: None,
        }))
    }

    async fn get_pipeline_definition(
        &self,
        request: Request<asset_pb::GetPipelineDefinitionRequest>,
    ) -> Result<Response<asset_pb::GetPipelineDefinitionResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let definition_id = parse_uuid("definition_id", &req.definition_id)?;
        let pool = self.require_pool()?;
        let m = pipeline_definition_model();
        let rel = m.relation.clone();
        let projection = pipeline_definition_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {definition_id} = $1::UUID AND {tenant_id} = $2::UUID",
            definition_id = m.q("definition_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(definition_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get pipeline definition failed: {err}")))?;
        let definition = match row {
            Some(row) => Some(pipeline_definition_from_row(&row)?),
            None => return Err(Status::not_found("pipeline definition not found")),
        };
        Ok(Response::new(asset_pb::GetPipelineDefinitionResponse {
            definition,
            error: None,
        }))
    }

    async fn register_asset(
        &self,
        request: Request<asset_pb::RegisterAssetRequest>,
    ) -> Result<Response<asset_pb::RegisterAssetResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
        // Per-tenant fair admission (held for the whole RPC).
        let _admit = self.admit(&req.tenant_id, &req.project_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        if req.file_id.trim().is_empty() {
            return Err(Status::invalid_argument("file_id is required"));
        }
        let pool = self.require_pool()?;
        // Tenant-bind the referenced storage file: refuse to wrap a file that
        // isn't an active file owned by this tenant (prevents cross-tenant
        // file references via a forged file_id).
        let file_uuid = parse_uuid("file_id", &req.file_id)?;
        if self
            .resolve_object_key(pool, file_uuid, tenant_id)
            .await
            .is_none()
        {
            return Err(Status::invalid_argument(
                "file_id does not reference an active storage file owned by this tenant",
            ));
        }
        let m = asset_model();
        let rel = m.relation.clone();
        let asset_id = Uuid::new_v4().to_string();
        let metadata = self.encrypt_native_json_state(&non_empty_json(&req.metadata))?;
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({asset_id}, {tenant_id}, {project_id}, {file_id}, {name}, {media_type}, {status}, {metadata}) \
             VALUES ($1::UUID, $2::UUID, NULLIF($3, '')::UUID, $4::UUID, $5, $6, 'PENDING', $7::JSONB)",
            asset_id = m.q("asset_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            file_id = m.q("file_id"),
            name = m.q("name"),
            media_type = m.q("media_type"),
            status = m.q("status"),
            metadata = m.q("metadata"),
        ))
        .bind(&asset_id)
        .bind(tenant_id)
        .bind(req.project_id.trim())
        .bind(req.file_id.trim())
        .bind(&req.name)
        .bind(&req.media_type)
        .bind(&metadata)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("register asset failed: {err}")))?;
        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            ASSET_REGISTERED_TOPIC,
            &asset_id,
            serde_json::json!({
                "asset_id": asset_id,
                "tenant_id": req.tenant_id,
                "project_id": req.project_id,
                "file_id": req.file_id.trim(),
                "name": req.name,
                "media_type": req.media_type,
            }),
            Some(&self.metrics),
        )
        .await;
        Ok(Response::new(asset_pb::RegisterAssetResponse {
            asset_id,
            message: "asset registered".to_string(),
            error: None,
        }))
    }

    async fn start_pipeline(
        &self,
        request: Request<asset_pb::StartPipelineRequest>,
    ) -> Result<Response<asset_pb::StartPipelineResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC) — starting a
        // pipeline schedules heavy step work, so it's gated per tenant.
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let definition_id = parse_uuid("definition_id", &req.definition_id)?;
        let asset_id = parse_uuid("asset_id", &req.asset_id)?;
        let pool = self.require_pool()?;
        let inst = pipeline_instance_model();
        let inst_rel = inst.relation.clone();
        let def = pipeline_definition_model();
        let def_rel = def.relation.clone();
        let step = pipeline_step_model();
        let step_rel = step.relation.clone();

        let correlation_id = req.correlation_id.trim().to_string();

        // IDEMPOTENCY: if a correlation id is supplied and an instance already
        // exists for it, return that instance without re-triggering.
        if !correlation_id.is_empty() {
            if let Some(existing) = sqlx::query(&format!(
                "SELECT {instance_id}::TEXT AS instance_id FROM {inst_rel} \
                 WHERE {tenant_id} = $1::UUID AND {correlation_id} = $2",
                instance_id = inst.q("instance_id"),
                tenant_id = inst.q("tenant_id"),
                correlation_id = inst.q("correlation_id"),
            ))
            .bind(tenant_id)
            .bind(&correlation_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| Status::internal(format!("start pipeline lookup failed: {err}")))?
            {
                let instance_id: String = existing
                    .try_get("instance_id")
                    .map_err(|e| Status::internal(format!("decode instance id failed: {e}")))?;
                return Ok(Response::new(asset_pb::StartPipelineResponse {
                    instance_id,
                    message: "pipeline already started".to_string(),
                    error: None,
                }));
            }
        }

        // Load the definition's step list.
        let steps_json: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {steps}::TEXT FROM {def_rel} \
             WHERE {definition_id} = $1::UUID AND {tenant_id} = $2::UUID",
            steps = def.q("steps"),
            definition_id = def.q("definition_id"),
            tenant_id = def.q("tenant_id"),
        ))
        .bind(definition_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load pipeline definition failed: {err}")))?;
        let steps_json = match steps_json {
            Some(s) => s,
            None => return Err(Status::not_found("pipeline definition not found")),
        };
        let parsed: serde_json::Value = serde_json::from_str(&steps_json)
            .map_err(|e| Status::internal(format!("pipeline definition steps not JSON: {e}")))?;
        let step_array: Vec<serde_json::Value> = match parsed {
            serde_json::Value::Array(a) => a,
            _ => Vec::new(),
        };
        let first_step_name = step_array
            .first()
            .and_then(|el| el.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let instance_id = Uuid::new_v4().to_string();
        let context = self.encrypt_native_json_state(&non_empty_json(&req.context))?;
        let insert_result = sqlx::query(&format!(
            "INSERT INTO {inst_rel} \
             ({instance_id}, {definition_id}, {asset_id}, {tenant_id}, {status}, {current_step}, {context}, {correlation_id}, {started_at}) \
             VALUES ($1::UUID, $2::UUID, $3::UUID, $4::UUID, 'RUNNING', $5, $6::JSONB, NULLIF($7, ''), CURRENT_TIMESTAMP)",
            instance_id = inst.q("instance_id"),
            definition_id = inst.q("definition_id"),
            asset_id = inst.q("asset_id"),
            tenant_id = inst.q("tenant_id"),
            status = inst.q("status"),
            current_step = inst.q("current_step"),
            context = inst.q("context"),
            correlation_id = inst.q("correlation_id"),
            started_at = inst.q("started_at"),
        ))
        .bind(&instance_id)
        .bind(definition_id)
        .bind(asset_id)
        .bind(tenant_id)
        .bind(&first_step_name)
        .bind(&context)
        .bind(&correlation_id)
        .execute(pool)
        .await;

        if let Err(err) = insert_result {
            let is_unique = err
                .as_database_error()
                .map(|e| e.is_unique_violation())
                .unwrap_or(false);
            if is_unique && !correlation_id.is_empty() {
                // Concurrent start with the same correlation id won the race;
                // return the existing instance instead of failing.
                let existing = sqlx::query(&format!(
                    "SELECT {instance_id}::TEXT AS instance_id FROM {inst_rel} \
                     WHERE {tenant_id} = $1::UUID AND {correlation_id} = $2",
                    instance_id = inst.q("instance_id"),
                    tenant_id = inst.q("tenant_id"),
                    correlation_id = inst.q("correlation_id"),
                ))
                .bind(tenant_id)
                .bind(&correlation_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| Status::internal(format!("start pipeline re-lookup failed: {e}")))?;
                if let Some(row) = existing {
                    let id: String = row
                        .try_get("instance_id")
                        .map_err(|e| Status::internal(format!("decode instance id failed: {e}")))?;
                    return Ok(Response::new(asset_pb::StartPipelineResponse {
                        instance_id: id,
                        message: "pipeline already started".to_string(),
                        error: None,
                    }));
                }
            }
            return Err(Status::internal(format!("start pipeline failed: {err}")));
        }

        // Pipeline started → emit the lifecycle event.
        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            PIPELINE_STARTED_TOPIC,
            &instance_id,
            serde_json::json!({
                "instance_id": instance_id,
                "definition_id": req.definition_id,
                "asset_id": req.asset_id,
                "tenant_id": req.tenant_id,
            }),
            Some(&self.metrics),
        )
        .await;

        // Load the asset's name + metadata once: these are the inputs available
        // to in-process steps without object bytes. Missing asset → empty inputs.
        let am = asset_model();
        let am_rel = am.relation.clone();
        let asset_inputs: Option<(String, String, String, String)> = sqlx::query(&format!(
            "SELECT {name}, {metadata}, {file_id}, {project_id} FROM {am_rel} \
             WHERE {asset_id} = $1::UUID AND {tenant_id} = $2::UUID",
            name = am.text_or_empty_as("name", "asset_name"),
            metadata = am.text_or_empty_as("metadata", "asset_metadata"),
            file_id = am.text_or_empty_as("file_id", "file_id"),
            project_id = am.text_or_empty_as("project_id", "project_id"),
            asset_id = am.q("asset_id"),
            tenant_id = am.q("tenant_id"),
        ))
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("load asset for pipeline failed: {err}")))?
        .map(|row| {
            let name: String = row.try_get("asset_name").unwrap_or_default();
            let metadata: String = row.try_get("asset_metadata").unwrap_or_default();
            let file_id: String = row.try_get("file_id").unwrap_or_default();
            let project_id: String = row.try_get("project_id").unwrap_or_default();
            (name, metadata, file_id, project_id)
        });
        let (asset_name, asset_metadata, asset_file_id, asset_project_id) =
            asset_inputs.unwrap_or_default();
        let asset_metadata = self.decrypt_native_json_state(&asset_metadata)?;

        // Materialize each step, RUN it in-process, and record the outcome.
        for el in &step_array {
            let step_name = el.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let step_type_str = el.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let step_type = step_type_to_db(step_type_str, "EMBED")?;
            let step_type_i32 = step_type_from_db(&step_type);
            let step_id = Uuid::new_v4().to_string();

            // Pure-CPU metadata steps (EMBED/EXTRACT) run via the sync registry.
            // Byte-IO steps (THUMBNAIL/RESIZE) fetch the source object, transform,
            // and store a derived object — inherently async, so they take a
            // separate path (still no registry edit to add metadata step types).
            let outcome = if is_byte_step(step_type_i32) {
                self.run_byte_step(
                    pool,
                    step_type_i32,
                    &asset_file_id,
                    tenant_id,
                    &asset_project_id,
                )
                .await
            } else {
                step_registry().run(
                    step_type_i32,
                    &StepContext {
                        asset_name: &asset_name,
                        metadata_json: &asset_metadata,
                    },
                )
            };
            let outcome = if step_type == "EMBED" {
                match outcome {
                    StepOutcome::Completed(mut value) => {
                        if let Some(target) = self
                            .upsert_embedding(&asset_project_id, &req.asset_id, &value)
                            .await
                        {
                            if let Some(object) = value.as_object_mut() {
                                object.insert(
                                    "vector_backend".to_string(),
                                    serde_json::Value::String("qdrant".to_string()),
                                );
                                object.insert(
                                    "vector_backend_instance".to_string(),
                                    serde_json::Value::String(target.instance),
                                );
                                object.insert(
                                    "vector_project_id".to_string(),
                                    serde_json::Value::String(target.project_id),
                                );
                            }
                        }
                        StepOutcome::Completed(value)
                    }
                    other => other,
                }
            } else {
                outcome
            };
            let (status_token, result_json, error_text) = match &outcome {
                StepOutcome::Completed(v) => ("COMPLETED", v.to_string(), String::new()),
                StepOutcome::Failed(msg) => ("FAILED", "{}".to_string(), msg.clone()),
            };
            let result_json = self.encrypt_native_json_state(&result_json)?;

            sqlx::query(&format!(
                "INSERT INTO {step_rel} \
                 ({step_id}, {instance_id}, {tenant_id}, {step_name}, {step_type}, {status}, {result}, {error}, {completed_at}) \
                 VALUES ($1::UUID, $2::UUID, $3::UUID, $4, $5, $6, $7::JSONB, NULLIF($8, ''), CURRENT_TIMESTAMP)",
                step_id = step.q("step_id"),
                instance_id = step.q("instance_id"),
                tenant_id = step.q("tenant_id"),
                step_name = step.q("step_name"),
                step_type = step.q("step_type"),
                status = step.q("status"),
                result = step.q("result"),
                error = step.q("error"),
                completed_at = step.q("completed_at"),
            ))
            .bind(&step_id)
            .bind(&instance_id)
            .bind(tenant_id)
            .bind(step_name)
            .bind(&step_type)
            .bind(status_token)
            .bind(&result_json)
            .bind(&error_text)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("create pipeline step failed: {err}")))?;

            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                PIPELINE_STEP_COMPLETED_TOPIC,
                &instance_id,
                serde_json::json!({
                    "instance_id": instance_id,
                    "tenant_id": req.tenant_id,
                    "step_id": step_id,
                    "step_name": step_name,
                    "step_type": step_type,
                    "status": status_token,
                }),
                Some(&self.metrics),
            )
            .await;
        }

        // Advance the instance to a terminal state (emits completed/failed).
        let instance_uuid = parse_uuid("instance_id", &instance_id)?;
        advance_instance(self, pool, instance_uuid, tenant_id).await?;

        Ok(Response::new(asset_pb::StartPipelineResponse {
            instance_id,
            message: "pipeline started".to_string(),
            error: None,
        }))
    }

    async fn get_pipeline(
        &self,
        request: Request<asset_pb::GetPipelineRequest>,
    ) -> Result<Response<asset_pb::GetPipelineResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let instance_id = parse_uuid("instance_id", &req.instance_id)?;
        let pool = self.require_pool()?;
        let inst = pipeline_instance_model();
        let inst_rel = inst.relation.clone();
        let inst_projection = pipeline_instance_select_projection(&inst);
        let row = sqlx::query(&format!(
            "SELECT {inst_projection} FROM {inst_rel} \
             WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID",
            instance_id = inst.q("instance_id"),
            tenant_id = inst.q("tenant_id"),
        ))
        .bind(instance_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get pipeline failed: {err}")))?;
        let instance = match row {
            Some(row) => {
                let mut instance = pipeline_instance_from_row(&row)?;
                instance.context = self.decrypt_native_json_state(&instance.context)?;
                Some(instance)
            }
            None => return Err(Status::not_found("pipeline instance not found")),
        };

        let step = pipeline_step_model();
        let step_rel = step.relation.clone();
        let step_projection = pipeline_step_select_projection(&step);
        let step_rows = sqlx::query(&format!(
            "SELECT {step_projection} FROM {step_rel} \
             WHERE {instance_id} = $1::UUID AND {tenant_id} = $2::UUID ORDER BY {step_name}",
            instance_id = step.q("instance_id"),
            tenant_id = step.q("tenant_id"),
            step_name = step.q("step_name"),
        ))
        .bind(instance_id)
        .bind(tenant_id)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("get pipeline steps failed: {err}")))?;
        let mut steps = Vec::with_capacity(step_rows.len());
        for r in &step_rows {
            let mut step = pipeline_step_from_row(r)?;
            step.result = self.decrypt_native_json_state(&step.result)?;
            steps.push(step);
        }

        Ok(Response::new(asset_pb::GetPipelineResponse {
            instance,
            steps,
            error: None,
        }))
    }

    async fn complete_step(
        &self,
        request: Request<asset_pb::CompleteStepRequest>,
    ) -> Result<Response<asset_pb::CompleteStepResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC) — completing a step
        // can trigger the next step + vector upserts, so it's gated per tenant.
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let step_id = parse_uuid("step_id", &req.step_id)?;
        let pool = self.require_pool()?;
        let step = pipeline_step_model();
        let step_rel = step.relation.clone();
        let status = step_status_to_db(&req.status, "COMPLETED")?;
        let result_json = if req.result.trim().is_empty() {
            String::new()
        } else {
            self.encrypt_native_json_state(req.result.trim())?
        };

        let result = sqlx::query(&format!(
            "UPDATE {step_rel} SET \
               {status} = $3, \
               {result} = CASE WHEN $4 = '' THEN {result} ELSE $4::JSONB END, \
               {error} = NULLIF($5, ''), \
               {completed_at} = CURRENT_TIMESTAMP \
             WHERE {step_id} = $1::UUID AND {tenant_id} = $2::UUID",
            status = step.q("status"),
            result = step.q("result"),
            error = step.q("error"),
            completed_at = step.q("completed_at"),
            step_id = step.q("step_id"),
            tenant_id = step.q("tenant_id"),
        ))
        .bind(step_id)
        .bind(tenant_id)
        .bind(&status)
        .bind(&result_json)
        .bind(req.error_message.trim())
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("complete step failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("pipeline step not found"));
        }

        // Resolve the owning instance for advance.
        let instance_id: Uuid = sqlx::query_scalar(&format!(
            "SELECT {instance_id} FROM {step_rel} \
             WHERE {step_id} = $1::UUID AND {tenant_id} = $2::UUID",
            instance_id = step.q("instance_id"),
            step_id = step.q("step_id"),
            tenant_id = step.q("tenant_id"),
        ))
        .bind(step_id)
        .bind(tenant_id)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("resolve step instance failed: {err}")))?;

        // Per-step completion event (externally-driven step).
        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            PIPELINE_STEP_COMPLETED_TOPIC,
            &instance_id.to_string(),
            serde_json::json!({
                "instance_id": instance_id.to_string(),
                "tenant_id": req.tenant_id,
                "step_id": req.step_id,
                "status": status,
            }),
            Some(&self.metrics),
        )
        .await;

        // Roll the instance to a terminal state when all steps are accounted for,
        // or to FAILED on any failed step (shared with start_pipeline).
        advance_instance(self, pool, instance_id, tenant_id).await?;

        Ok(Response::new(asset_pb::CompleteStepResponse {
            message: "step completed".to_string(),
            error: None,
        }))
    }

    async fn list_assets(
        &self,
        request: Request<asset_pb::ListAssetsRequest>,
    ) -> Result<Response<asset_pb::ListAssetsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so list scans can't starve the pool.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let pool = self.require_pool()?;
        let m = asset_model();
        let rel = m.relation.clone();
        let projection = asset_select_projection(&m);
        let media_filter = req.media_type.trim().to_string();
        let status_filter = asset_status_to_db(&req.status, "")?;
        let page_size = if req.page_size > 0 { req.page_size } else { 50 }.min(500) as i64;
        let page = if req.page > 0 { req.page } else { 1 } as i64;
        let offset = (page - 1) * page_size;
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL \
             AND ($2 = '' OR {media_type} = $2) AND ($3 = '' OR {status} = $3)",
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
            media_type = m.q("media_type"),
            status = m.q("status"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(tenant_id)
            .bind(&media_filter)
            .bind(&status_filter)
            .fetch_one(pool)
            .await
            .map_err(|err| Status::internal(format!("count assets failed: {err}")))?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} ORDER BY {name} LIMIT $4 OFFSET $5",
            name = m.q("name"),
        ))
        .bind(tenant_id)
        .bind(&media_filter)
        .bind(&status_filter)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list assets failed: {err}")))?;
        let mut assets = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut asset = asset_from_row(row)?;
            asset.metadata = self.decrypt_native_json_state(&asset.metadata)?;
            assets.push(asset);
        }
        Ok(Response::new(asset_pb::ListAssetsResponse {
            assets,
            total_count: total as i32,
            error: None,
        }))
    }

    async fn get_asset(
        &self,
        request: Request<asset_pb::GetAssetRequest>,
    ) -> Result<Response<asset_pb::GetAssetResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so reads can't starve the pool.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let asset_id = parse_uuid("asset_id", &req.asset_id)?;
        let pool = self.require_pool()?;
        let m = asset_model();
        let rel = m.relation.clone();
        let projection = asset_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {asset_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            asset_id = m.q("asset_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(asset_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get asset failed: {err}")))?;
        let asset = match row {
            Some(row) => {
                let mut asset = asset_from_row(&row)?;
                asset.metadata = self.decrypt_native_json_state(&asset.metadata)?;
                Some(asset)
            }
            None => return Err(Status::not_found("asset not found")),
        };
        Ok(Response::new(asset_pb::GetAssetResponse {
            asset,
            error: None,
        }))
    }
}

#[cfg(test)]
mod step_executor_tests {
    use super::*;
    use asset_entity_pb::StepType as T;

    #[test]
    fn registry_dispatches_embed_extract_and_fails_media() {
        let registry = StepRegistry::default_registry();
        let ctx = StepContext {
            asset_name: "report",
            metadata_json: "{\"k\":\"v\"}",
        };

        match registry.run(T::Embed as i32, &ctx) {
            StepOutcome::Completed(v) => {
                assert!(
                    v.get("embedding").and_then(|e| e.as_array()).is_some(),
                    "EMBED must produce an `embedding` array, got {v}"
                );
            }
            StepOutcome::Failed(msg) => panic!("EMBED should complete, failed with: {msg}"),
        }

        assert!(
            matches!(
                registry.run(T::Extract as i32, &ctx),
                StepOutcome::Completed(_)
            ),
            "EXTRACT should complete"
        );

        match registry.run(T::Transcode as i32, &ctx) {
            StepOutcome::Failed(msg) => {
                assert!(
                    msg.contains("not yet implemented"),
                    "TRANSCODE failure message should explain it is unimplemented, got: {msg}"
                );
            }
            StepOutcome::Completed(_) => panic!("TRANSCODE must fail (no capability lie)"),
        }
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    /// A caller scoped to tenant-a must not read another tenant's asset by putting
    /// a foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any pool/DB access (no Postgres needed).
    #[tokio::test]
    async fn get_asset_rejects_cross_tenant_body() {
        let svc = AssetServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(asset_pb::GetAssetRequest {
            tenant_id: "tenant-b".to_string(),
            asset_id: "00000000-0000-0000-0000-000000000001".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_asset(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }
}

impl DataBrokerService {
    /// Build the native `AssetService`, wired to the broker's Postgres pool.
    pub(crate) fn build_asset_service(&self) -> AssetServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
        let outbox = runtime.config().cdc.outbox_relation();
        let collection = std::env::var("UDB_ASSET_VECTOR_COLLECTION")
            .unwrap_or_else(|_| DEFAULT_VECTOR_COLLECTION.to_string());
        AssetServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_metrics(self.metrics.clone())
            .with_vector(Some(runtime.clone()), collection)
    }
}

/// Topic the storage service emits on finalize; the auto-trigger consumes it.
#[cfg(feature = "kafka")]
const STORAGE_FINALIZED_TOPIC: &str = "udb.storage.file.finalized.v1";

#[cfg(feature = "kafka")]
fn storage_finalized_consumer_config(brokers: &str) -> rdkafka::ClientConfig {
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", "udb-asset-storage-finalized-trigger")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    config
}

#[cfg(feature = "kafka")]
fn storage_finalized_payload_ids(bytes: &[u8]) -> Option<(String, String)> {
    let env = serde_json::from_slice::<serde_json::Value>(bytes).ok()?;
    // Canonical envelope: tenant_id at top level; file_id in the payload
    // (document_id is the partition key = file_id too).
    let tenant_id = env.get("tenant_id").and_then(|v| v.as_str())?.trim();
    let file_id = env
        .get("payload")
        .and_then(|p| p.get("file_id"))
        .and_then(|v| v.as_str())
        .or_else(|| env.get("document_id").and_then(|v| v.as_str()))?
        .trim();
    if tenant_id.is_empty() || file_id.is_empty() {
        return None;
    }
    Some((tenant_id.to_string(), file_id.to_string()))
}

#[cfg(feature = "kafka")]
fn storage_finalized_commit_offsets(
    topic: &str,
    partition: i32,
    message_offset: i64,
) -> rdkafka::error::KafkaResult<rdkafka::TopicPartitionList> {
    let mut offsets = rdkafka::TopicPartitionList::new();
    offsets.add_partition_offset(
        topic,
        partition,
        rdkafka::Offset::Offset(message_offset.saturating_add(1)),
    )?;
    Ok(offsets)
}

#[cfg(feature = "kafka")]
fn should_commit_storage_finalized_offset(result: &Result<Option<String>, Status>) -> bool {
    result.is_ok()
}

#[cfg(feature = "kafka")]
impl AssetServiceImpl {
    /// Spawn the storage→asset auto-trigger: a background Kafka consumer on
    /// `udb.storage.file.finalized.v1` that, per finalized file, registers the
    /// asset and starts the matching pipeline via [`handle_storage_finalized`]
    /// (idempotent). Offsets are committed only after successful handling, so
    /// backlog is replayed at-least-once across restarts. Best-effort — a
    /// consumer error logs; the broker keeps running.
    pub(crate) fn spawn_storage_finalized_consumer(self: std::sync::Arc<Self>, brokers: String) {
        tokio::spawn(async move {
            use rdkafka::Message;
            use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
            let config = storage_finalized_consumer_config(&brokers);
            let consumer: StreamConsumer = match config.create() {
                Ok(c) => c,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "asset storage-finalized consumer: create failed"
                    );
                    return;
                }
            };
            if let Err(err) = consumer.subscribe(&[STORAGE_FINALIZED_TOPIC]) {
                tracing::error!(error = %err, "asset storage-finalized consumer: subscribe failed");
                return;
            }
            tracing::info!(
                topic = STORAGE_FINALIZED_TOPIC,
                "storage→asset auto-trigger consumer started"
            );
            loop {
                match consumer.recv().await {
                    Ok(msg) => {
                        let Some(bytes) = msg.payload() else {
                            tracing::warn!(
                                "asset storage-finalized consumer: message missing payload"
                            );
                            continue;
                        };
                        let Some((tenant_id, file_id)) = storage_finalized_payload_ids(bytes)
                        else {
                            tracing::warn!(
                                topic = msg.topic(),
                                partition = msg.partition(),
                                offset = msg.offset(),
                                "asset storage-finalized consumer: invalid envelope"
                            );
                            continue;
                        };
                        let topic = msg.topic().to_string();
                        let partition = msg.partition();
                        let offset = msg.offset();
                        let result = self.handle_storage_finalized(&file_id, &tenant_id).await;
                        if should_commit_storage_finalized_offset(&result) {
                            match storage_finalized_commit_offsets(&topic, partition, offset) {
                                Ok(offsets) => {
                                    if let Err(err) = consumer.commit(&offsets, CommitMode::Async) {
                                        tracing::warn!(
                                            error = %err,
                                            file_id = %file_id,
                                            topic = %topic,
                                            partition,
                                            offset,
                                            "asset storage-finalized consumer commit failed"
                                        );
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        file_id = %file_id,
                                        topic = %topic,
                                        partition,
                                        offset,
                                        "asset storage-finalized consumer commit offset build failed"
                                    );
                                }
                            }
                        }
                        if let Err(err) = result {
                            tracing::warn!(
                                error = %err,
                                file_id = %file_id,
                                "storage→asset trigger failed"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "asset storage-finalized consumer recv error");
                    }
                }
            }
        });
    }
}

#[cfg(all(test, feature = "kafka"))]
mod storage_finalized_consumer_tests {
    use super::*;

    #[test]
    fn consumer_config_replays_backlog_and_disables_auto_commit() {
        let config = storage_finalized_consumer_config("broker-a:9092");

        assert_eq!(config.get("bootstrap.servers"), Some("broker-a:9092"));
        assert_eq!(
            config.get("group.id"),
            Some("udb-asset-storage-finalized-trigger")
        );
        assert_eq!(config.get("auto.offset.reset"), Some("earliest"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn commit_offset_advances_only_the_processed_message() {
        let offsets = storage_finalized_commit_offsets(STORAGE_FINALIZED_TOPIC, 2, 41).unwrap();
        let elem = offsets
            .find_partition(STORAGE_FINALIZED_TOPIC, 2)
            .expect("topic partition offset should be present");

        assert_eq!(elem.offset(), rdkafka::Offset::Offset(42));
    }

    #[test]
    fn commit_decision_follows_handler_success() {
        assert!(should_commit_storage_finalized_offset(&Ok(Some(
            "instance-1".to_string()
        ))));
        assert!(should_commit_storage_finalized_offset(&Ok(None)));
        assert!(!should_commit_storage_finalized_offset(&Err(
            Status::internal("handler failed")
        )));
    }

    #[test]
    fn finalized_payload_extracts_payload_file_id_then_document_id() {
        let direct = br#"{
            "tenant_id": "tenant-a",
            "document_id": "fallback",
            "payload": { "file_id": "file-a" }
        }"#;
        assert_eq!(
            storage_finalized_payload_ids(direct),
            Some(("tenant-a".to_string(), "file-a".to_string()))
        );

        let fallback = br#"{
            "tenant_id": "tenant-a",
            "document_id": "file-b",
            "payload": {}
        }"#;
        assert_eq!(
            storage_finalized_payload_ids(fallback),
            Some(("tenant-a".to_string(), "file-b".to_string()))
        );

        assert_eq!(
            storage_finalized_payload_ids(br#"{"tenant_id":"tenant-a"}"#),
            None
        );
    }
}
