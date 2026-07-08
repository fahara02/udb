//! Native `AssetService` — proto-driven Postgres CRUD + processing-pipeline
//! orchestration over the UDB-owned `udb_asset.{assets,pipeline_definitions,
//! pipeline_instances,pipeline_steps}` tables.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalSort, LogicalValue, SortDirection,
};
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
    admit_on as native_admit_on, emit_payload_event, native_next_page_token_for_total,
    native_offset_page_window, native_service_context, storage_object_defaults,
    validate_request_scope, validate_request_tenant,
};

const ASSET_MSG: &str = "udb.core.asset.entity.v1.Asset";
const PIPELINE_DEFINITION_MSG: &str = "udb.core.asset.entity.v1.PipelineDefinition";
const PIPELINE_INSTANCE_MSG: &str = "udb.core.asset.entity.v1.PipelineInstance";
const PIPELINE_STEP_MSG: &str = "udb.core.asset.entity.v1.PipelineStep";

// ── stable machine-readable error reasons ─────────────────────────────────────
// Attached to the matching pipeline failures so SDK callers can branch on a
// stable code instead of parsing human text. The gRPC Status *code* is left
// unchanged at each site. The repo has no `google.rpc.ErrorInfo` status-detail
// infrastructure, so non-OK statuses carry the reason on the `error-reason`
// metadata trailer (uniform with the storage/webrtc/notification services); the
// OK "already started" return carries it in its response `message` body field.
/// The pipeline definition is structurally invalid (e.g. its persisted step
/// list is not valid JSON).
const PIPELINE_DEFINITION_INVALID: &str = "PIPELINE_DEFINITION_INVALID";
/// A definition step declares a `type` the runtime does not support.
const STEP_TYPE_UNSUPPORTED: &str = "STEP_TYPE_UNSUPPORTED";
/// Reserved: the source asset/file required by a step is missing. No hard
/// failure site exists today (a missing asset yields empty step inputs), so this
/// is held for the future byte-step "source object not found" path.
#[allow(dead_code)]
const ASSET_FILE_MISSING: &str = "ASSET_FILE_MISSING";
/// A concurrent start with the same correlation id won the race; the existing
/// instance is returned instead of starting a new pipeline.
const PIPELINE_ALREADY_STARTED: &str = "PIPELINE_ALREADY_STARTED";

/// Attach a stable machine-readable `reason` to a non-OK gRPC `Status` via the
/// `error-reason` metadata trailer — uniform with the storage/webrtc/notification
/// services (a non-OK status is trailers-only, so the sub-code rides a trailer).
fn status_with_reason(mut status: Status, reason: &'static str) -> Status {
    status.metadata_mut().insert(
        "error-reason",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

fn asset_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn asset_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    asset_invalid_field(field, description, message)
}

fn asset_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "asset",
        operation,
        capability_required,
        message,
    )
}

fn asset_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("asset", operation, message)
}

fn asset_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "asset",
        operation,
        schema_code,
        message,
    )
}

fn native_state_encryption_failed_status(err: impl std::fmt::Display) -> Status {
    asset_capability_status(
        "native_state_encrypt",
        "native_state_encryption",
        format!("native-state encryption failed: {err}"),
    )
}

fn native_state_decryption_failed_status(err: impl std::fmt::Display) -> Status {
    asset_capability_status(
        "native_state_decrypt",
        "native_state_encryption",
        format!("native-state decryption failed: {err}"),
    )
}

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

/// Resolved metadata for a finalized storage file that an asset pipeline acts on.
/// Shared by the storage-finalized and Kafka-trigger handlers so both derive the
/// asset's name/project/media_type identically.
struct FinalizedFile {
    filename: String,
    project_id: String,
    media_type: String,
}

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
                .map_err(native_state_encryption_failed_status),
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
                .map_err(native_state_decryption_failed_status),
            None => Ok(stored_json.to_string()),
        }
    }

    /// Typed native entity dispatch is the P4 production path for the isolated
    /// AssetService entity CRUD/read methods. Pipeline orchestration still keeps
    /// the transitional Postgres pool for multi-table workflow state.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            asset_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "asset service requires runtime native entity dispatch",
            )
        })
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

        let Some(file) = self
            .resolve_finalized_file(pool, tenant_uuid, file_uuid)
            .await?
        else {
            return Ok(None);
        };

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
        .bind(&file.media_type)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "handle_storage_finalized",
                format!("match pipeline definition failed: {e}"),
            )
        })?;
        let Some(definition_id) = def_id else {
            return Ok(None);
        };

        let instance_id = self
            .start_pipeline_for_file(
                tenant_id,
                file_id,
                file_uuid,
                tenant_uuid,
                definition_id,
                &file,
            )
            .await?;
        Ok(Some(instance_id))
    }

    /// Kafka-trigger handler (master-plan 5.2): start the most recent active
    /// pipeline definition whose `trigger_topic` matches `topic` for the file's
    /// tenant. Mirrors [`Self::handle_storage_finalized`] but selects definitions
    /// by `trigger_topic` instead of media_type, sharing the same file-resolve and
    /// asset-register/start path. Idempotent on `correlation_id = file_id`, so the
    /// trigger consumer's at-least-once redelivery is safe. Returns the started
    /// instance id, or `None` when no file or no matching definition is found.
    // Reached from the trigger consumer, which serve() wires via
    // `spawn_trigger_manager` (master-plan 5.2); allow keeps the build clean until then.
    #[allow(dead_code)]
    pub(crate) async fn handle_trigger_event(
        &self,
        topic: &str,
        file_id: &str,
        tenant_id: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let tenant_uuid = parse_uuid("tenant_id", tenant_id)?;
        let file_uuid = parse_uuid("file_id", file_id)?;

        let Some(file) = self
            .resolve_finalized_file(pool, tenant_uuid, file_uuid)
            .await?
        else {
            return Ok(None);
        };

        // Match an active pipeline definition for this tenant + trigger_topic.
        let dm = pipeline_definition_model();
        let def_id: Option<String> = sqlx::query_scalar(&format!(
            "SELECT {did}::TEXT FROM {rel} \
             WHERE {tid} = $1::UUID AND {tt} = $2 AND {status} = 'ACTIVE' \
             ORDER BY {ver} DESC LIMIT 1",
            did = dm.q("definition_id"),
            rel = dm.relation,
            tid = dm.q("tenant_id"),
            tt = dm.q("trigger_topic"),
            status = dm.q("status"),
            ver = dm.q("version"),
        ))
        .bind(tenant_uuid)
        .bind(topic)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "handle_trigger_event",
                format!("match pipeline definition by trigger_topic failed: {e}"),
            )
        })?;
        let Some(definition_id) = def_id else {
            return Ok(None);
        };

        let instance_id = self
            .start_pipeline_for_file(
                tenant_id,
                file_id,
                file_uuid,
                tenant_uuid,
                definition_id,
                &file,
            )
            .await?;
        Ok(Some(instance_id))
    }

    /// Tenant-bound resolve of a finalized storage file's metadata (proto-driven).
    /// Only acts on a file owned by `tenant_uuid`; returns `None` when absent.
    /// Shared by [`Self::handle_storage_finalized`] and [`Self::handle_trigger_event`].
    async fn resolve_finalized_file(
        &self,
        pool: &PgPool,
        tenant_uuid: Uuid,
        file_uuid: Uuid,
    ) -> Result<Option<FinalizedFile>, Status> {
        let fm = native_model(
            "udb.core.storage.entity.v1.File",
            &["file_id", "content_type", "filename"],
        );
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
        .map_err(|e| {
            asset_internal_status(
                "resolve_finalized_file",
                format!("resolve finalized file failed: {e}"),
            )
        })?;
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
        Ok(Some(FinalizedFile {
            filename,
            project_id,
            media_type,
        }))
    }

    /// Reuse an existing asset for `file_id`, else register one, then start
    /// `definition_id`'s pipeline (idempotent on `correlation_id = file_id`).
    /// Returns the pipeline instance id. Shared trigger/finalize tail.
    async fn start_pipeline_for_file(
        &self,
        tenant_id: &str,
        file_id: &str,
        file_uuid: Uuid,
        tenant_uuid: Uuid,
        definition_id: String,
        file: &FinalizedFile,
    ) -> Result<String, Status> {
        let pool = self.require_pool()?;
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
        .map_err(|e| {
            asset_internal_status(
                "start_pipeline_for_file",
                format!("lookup asset for file failed: {e}"),
            )
        })?;
        let asset_id = match existing {
            Some(a) => a,
            None => {
                self.register_asset(Request::new(asset_pb::RegisterAssetRequest {
                    tenant_id: tenant_id.to_string(),
                    project_id: file.project_id.clone(),
                    file_id: file_id.to_string(),
                    name: if file.filename.is_empty() {
                        file_id.to_string()
                    } else {
                        file.filename.clone()
                    },
                    media_type: file.media_type.clone(),
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
        Ok(started.instance_id)
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

    /// Run a byte-IO step: fetch the source object bytes, transform them per the
    /// step's [`ByteStepParams`], store the derived object under the `derived/`
    /// namespace, and register it as a `udb_storage.files` row. Image processing
    /// is behind the `asset-image` feature; TRANSCODE uses the ffmpeg executor.
    /// Without the required executor the step fails explicitly (no fake success).
    /// Source bytes/derived objects use the same object backend+bucket as the storage service
    /// (`UDB_STORAGE_OBJECT_BACKEND` / `UDB_STORAGE_BUCKET`).
    async fn run_byte_step(
        &self,
        pool: &PgPool,
        step_type_i32: i32,
        file_id_str: &str,
        tenant_id: Uuid,
        project_id: &str,
        params: &ByteStepParams,
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
        let step_type = asset_entity_pb::StepType::try_from(step_type_i32)
            .unwrap_or(asset_entity_pb::StepType::Unspecified);

        if matches!(step_type, asset_entity_pb::StepType::Transcode) {
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
            let (out_bytes, content_type, ext) = match run_ffmpeg_transcode(&bytes, params).await {
                Ok(output) => output,
                Err(reason) => return StepOutcome::Failed(reason),
            };
            let out_len = out_bytes.len();
            let derived_key = derived_object_key(&object_key, step_type, ext);
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &bucket,
                &derived_key,
                content_type,
            );
            if let Err(err) = runtime
                .put_object_backend_target_for_project(
                    &backend, None, project_id, &put_req, out_bytes,
                )
                .await
            {
                return StepOutcome::Failed(format!("store derived object failed: {err}"));
            }
            if let Err(err) = register_derived_file(
                pool,
                tenant_id,
                &derived_key,
                &backend,
                &bucket,
                content_type,
                "VIDEO",
                out_len as i64,
            )
            .await
            {
                return StepOutcome::Failed(format!("register derived object failed: {err}"));
            }
            return StepOutcome::Completed(serde_json::json!({
                "derived_object_key": derived_key,
                "format": ext,
                "content_type": content_type,
                "bytes": out_len,
            }));
        }

        #[cfg(not(feature = "asset-image"))]
        {
            let _ = (
                runtime,
                step_type_i32,
                &object_key,
                &backend,
                &bucket,
                project_id,
                params,
            );
            StepOutcome::Failed(
                "THUMBNAIL/RESIZE require the `asset-image` feature build".to_string(),
            )
        }
        #[cfg(feature = "asset-image")]
        {
            use asset_entity_pb::StepType as T;
            let step_type = T::try_from(step_type_i32).unwrap_or(T::Unspecified);

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

            // (1) byte cap BEFORE decode — bound memory before the decoder allocates.
            if let Err(reason) = check_input_bytes(bytes.len() as u64) {
                return StepOutcome::Failed(reason);
            }
            // (2) header pixel cap BEFORE full decode — probe dimensions only, so a
            //     pixel-flood decompression bomb is rejected pre-decode.
            let probe = match image::ImageReader::new(std::io::Cursor::new(bytes.as_slice()))
                .with_guessed_format()
            {
                Ok(reader) => reader.into_dimensions(),
                Err(err) => {
                    return StepOutcome::Failed(format!("probe image header failed: {err}"));
                }
            };
            let (src_w, src_h) = match probe {
                Ok(dims) => dims,
                Err(err) => {
                    return StepOutcome::Failed(format!("probe image dimensions failed: {err}"));
                }
            };
            if let Err(reason) = check_image_pixels(src_w, src_h) {
                return StepOutcome::Failed(reason);
            }

            // (3) full decode — now bounded by the two checks above.
            let img = match image::load_from_memory(&bytes) {
                Ok(i) => i,
                Err(err) => return StepOutcome::Failed(format!("decode image failed: {err}")),
            };

            // (4) transform per step type + requested params.
            let (out_format, content_type, ext) =
                match resolve_output_format(params.format.as_deref(), image::ImageFormat::Png) {
                    Ok(triple) => triple,
                    Err(reason) => return StepOutcome::Failed(reason),
                };
            let transformed = match apply_image_transform(img, step_type, params) {
                Ok(t) => t,
                Err(reason) => return StepOutcome::Failed(reason),
            };

            // (5) encode.
            let mut out = std::io::Cursor::new(Vec::new());
            if let Err(err) = transformed.write_to(&mut out, out_format) {
                return StepOutcome::Failed(format!("encode derived image failed: {err}"));
            }
            let out_bytes = out.into_inner();
            let out_len = out_bytes.len();
            let (out_w, out_h) = (transformed.width(), transformed.height());

            // (6) store under the `derived/` namespace (no source-key collision).
            let derived_key = derived_object_key(&object_key, step_type, ext);
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &bucket,
                &derived_key,
                content_type,
            );
            if let Err(err) = runtime
                .put_object_backend_target_for_project(
                    &backend, None, project_id, &put_req, out_bytes,
                )
                .await
            {
                return StepOutcome::Failed(format!("store derived object failed: {err}"));
            }

            // (7) register the derived object as a tracked `udb_storage.files` row.
            if let Err(err) = register_derived_file(
                pool,
                tenant_id,
                &derived_key,
                &backend,
                &bucket,
                content_type,
                "IMAGE",
                out_len as i64,
            )
            .await
            {
                return StepOutcome::Failed(format!("register derived object failed: {err}"));
            }

            StepOutcome::Completed(serde_json::json!({
                "derived_object_key": derived_key,
                "width": out_w,
                "height": out_h,
                "format": ext,
                "bytes": out_len,
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
            asset_capability_status(
                "postgres_store",
                "postgres_store",
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

/// THUMBNAIL/RESIZE/TRANSCODE are byte-IO steps run async
/// (fetch→transform→store) outside the sync metadata-step registry.
fn is_byte_step(step_type: i32) -> bool {
    use asset_entity_pb::StepType as T;
    matches!(
        T::try_from(step_type),
        Ok(T::Thumbnail) | Ok(T::Resize) | Ok(T::Transcode)
    )
}

/// Optional transform parameters carried by a pipeline-definition step element
/// (the step JSON object). Read from a `params` sub-object, falling back to the
/// step's top-level keys, so a definition can declare either:
///   `{"type":"RESIZE","params":{"width":800,"height":600,"format":"jpeg"}}`
/// or the flattened `{"type":"RESIZE","width":800,"format":"jpeg"}`.
/// All fields are optional; absent/zero/invalid values fall back to per-step
/// defaults (THUMBNAIL → 256², RESIZE → required, format → PNG).
// Fields are consumed by the image RESIZE/CONVERT path, which is `asset-image`-gated;
// without that feature the parsed params are intentionally unused (not dead code).
#[cfg_attr(not(feature = "asset-image"), allow(dead_code))]
#[derive(Debug, Clone, Default)]
struct ByteStepParams {
    width: Option<u32>,
    height: Option<u32>,
    format: Option<String>,
}

/// Parse the optional [`ByteStepParams`] from a pipeline-definition step element.
/// Pure JSON reading — cheap and side-effect-free even without `asset-image`, so
/// it is NOT feature-gated (the call site is shared by both builds).
fn parse_byte_step_params(el: &serde_json::Value) -> ByteStepParams {
    let params = el.get("params");
    let read_u32 = |key: &str| -> Option<u32> {
        let src = params.and_then(|p| p.get(key)).or_else(|| el.get(key))?;
        src.as_u64()
            .or_else(|| src.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            .filter(|v| *v > 0)
            .map(|v| v.min(u32::MAX as u64) as u32)
    };
    let format = params
        .and_then(|p| p.get("format"))
        .or_else(|| el.get("format"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty());
    ByteStepParams {
        width: read_u32("width"),
        height: read_u32("height"),
        format,
    }
}

// ── image-step decode limits (decompression-bomb guard) ───────────────────────
// Both limits are enforced BEFORE the full decode: the byte cap against the
// fetched object length, and the pixel cap against the image HEADER dimensions
// (a header probe, not a full decode). Over-limit fails the step CLOSED with a
// typed reason — never a panic, never a silent pass.

/// Largest source object (in bytes) an image step will even attempt to decode.
/// 32 MiB — generous for real photos/scans, small enough to bound memory before
/// the decoder allocates.
#[cfg(feature = "asset-image")]
const MAX_IMAGE_INPUT_BYTES: u64 = 32 * 1024 * 1024;

/// Largest source image (width × height) an image step will decode. 64 MP caps
/// the post-decode RGBA buffer (~256 MiB at 4 B/px) and rejects pixel-flood
/// decompression bombs whose tiny encoded size passes the byte cap.
#[cfg(feature = "asset-image")]
const MAX_IMAGE_PIXELS: u64 = 64_000_000;

/// Default square edge for a THUMBNAIL step when no dimensions are requested.
#[cfg(feature = "asset-image")]
const DEFAULT_THUMBNAIL_EDGE: u32 = 256;

/// Object-key prefix for every derived media object so derived blobs live in their own
/// namespace and can NEVER collide with (or shadow) a source `object_key`.
const DERIVED_OBJECT_PREFIX: &str = "derived/";

/// Largest source object (in bytes) a transcode step will send to ffmpeg.
/// 512 MiB keeps broker temp-disk/memory bounded until streaming transcode jobs
/// are added.
const MAX_TRANSCODE_INPUT_BYTES: u64 = 512 * 1024 * 1024;

/// Largest transcode output (in bytes) accepted back from ffmpeg.
const MAX_TRANSCODE_OUTPUT_BYTES: u64 = 512 * 1024 * 1024;

const UDB_FFMPEG_BIN_ENV: &str = "UDB_FFMPEG_BIN";
const UDB_FFMPEG_ROOT_ENV: &str = "UDB_FFMPEG_ROOT";
const DEFAULT_FFMPEG_TIMEOUT_SECS: u64 = 120;
const MAX_FFMPEG_TIMEOUT_SECS: u64 = 600;

fn check_transcode_input_bytes(len: u64) -> Result<(), String> {
    if len > MAX_TRANSCODE_INPUT_BYTES {
        return Err(format!(
            "source media is {len} bytes, exceeds the {MAX_TRANSCODE_INPUT_BYTES}-byte transcode input limit"
        ));
    }
    Ok(())
}

fn check_transcode_output_bytes(len: u64) -> Result<(), String> {
    if len > MAX_TRANSCODE_OUTPUT_BYTES {
        return Err(format!(
            "transcoded media is {len} bytes, exceeds the {MAX_TRANSCODE_OUTPUT_BYTES}-byte transcode output limit"
        ));
    }
    Ok(())
}

fn ffmpeg_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        let secs = std::env::var("UDB_ASSET_FFMPEG_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|secs| *secs > 0)
            .unwrap_or(DEFAULT_FFMPEG_TIMEOUT_SECS)
            .min(MAX_FFMPEG_TIMEOUT_SECS);
        Duration::from_secs(secs)
    })
}

fn ffmpeg_exe_name() -> &'static str {
    if cfg!(windows) {
        "ffmpeg.exe"
    } else {
        "ffmpeg"
    }
}

fn ffmpeg_platform_dir() -> &'static str {
    if cfg!(windows) {
        "windows"
    } else if cfg!(target_os = "macos") {
        "macos"
    } else {
        "linux"
    }
}

fn ffmpeg_under_root(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref()
        .join("bin")
        .join(ffmpeg_platform_dir())
        .join(ffmpeg_exe_name())
}

fn push_ffmpeg_root(candidates: &mut Vec<PathBuf>, root: PathBuf) {
    let path = ffmpeg_under_root(root);
    if !candidates.iter().any(|candidate| candidate == &path) {
        candidates.push(path);
    }
}

fn vendored_ffmpeg_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(root) = std::env::var(UDB_FFMPEG_ROOT_ENV) {
        let root = root.trim();
        if !root.is_empty() {
            push_ffmpeg_root(&mut candidates, PathBuf::from(root));
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        push_ffmpeg_root(&mut candidates, cwd.join("third_party").join("ffmpeg"));
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        // Raw release bundles can place `third_party/ffmpeg` beside the binary.
        // The release Docker image is covered by the current-working-directory
        // candidate because it starts in `/app` and copies `third_party` there.
        push_ffmpeg_root(&mut candidates, dir.join("third_party").join("ffmpeg"));
        if let Some(parent) = dir.parent() {
            push_ffmpeg_root(&mut candidates, parent.join("third_party").join("ffmpeg"));
        }
    }
    push_ffmpeg_root(
        &mut candidates,
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("third_party")
            .join("ffmpeg"),
    );
    candidates
}

#[cfg(test)]
fn vendored_ffmpeg_path() -> PathBuf {
    vendored_ffmpeg_candidates()
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("third_party")
                .join("ffmpeg")
                .join("bin")
                .join(ffmpeg_platform_dir())
                .join(ffmpeg_exe_name())
        })
}

fn resolve_ffmpeg_binary() -> Result<PathBuf, String> {
    static FFMPEG_BINARY: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    FFMPEG_BINARY
        .get_or_init(resolve_ffmpeg_binary_uncached)
        .clone()
}

fn resolve_ffmpeg_binary_uncached() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var(UDB_FFMPEG_BIN_ENV) {
        let path = PathBuf::from(path.trim());
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "{UDB_FFMPEG_BIN_ENV} points to missing ffmpeg binary: {}",
            path.display()
        ));
    }
    let candidates = vendored_ffmpeg_candidates();
    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }
    let searched = candidates
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "ffmpeg transcode executor unavailable: missing vendored binary; searched: {searched} (or set {UDB_FFMPEG_BIN_ENV} / {UDB_FFMPEG_ROOT_ENV})"
    ))
}

fn resolve_transcode_output_format(
    requested: Option<&str>,
) -> Result<(&'static str, &'static str, &'static str), String> {
    match requested.unwrap_or("mp4") {
        "mp4" => Ok(("mp4", "video/mp4", "mp4")),
        other => Err(format!(
            "unsupported transcode output format '{other}' (this build supports: mp4)"
        )),
    }
}

async fn run_ffmpeg_transcode(
    source: &[u8],
    params: &ByteStepParams,
) -> Result<(Vec<u8>, &'static str, &'static str), String> {
    check_transcode_input_bytes(source.len() as u64)?;
    let (_container, content_type, ext) =
        resolve_transcode_output_format(params.format.as_deref())?;
    let ffmpeg = resolve_ffmpeg_binary()?;
    let job_id = Uuid::new_v4().to_string();
    let work_dir = std::env::temp_dir().join("udb-asset-ffmpeg").join(job_id);
    tokio::fs::create_dir_all(&work_dir)
        .await
        .map_err(|err| format!("create ffmpeg work dir failed: {err}"))?;
    let input_path = work_dir.join("input.bin");
    let output_path = work_dir.join(format!("output.{ext}"));
    let cleanup_dir = work_dir.clone();

    let result = async {
        tokio::fs::write(&input_path, source)
            .await
            .map_err(|err| format!("write ffmpeg input failed: {err}"))?;
        let mut command = tokio::process::Command::new(&ffmpeg);
        command
            .arg("-nostdin")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-y")
            .arg("-i")
            .arg(&input_path)
            .arg("-map")
            .arg("0:v:0?")
            .arg("-map")
            .arg("0:a:0?")
            .arg("-c:v")
            .arg("libx264")
            .arg("-preset")
            .arg("veryfast")
            .arg("-movflags")
            .arg("+faststart")
            .arg("-c:a")
            .arg("aac")
            .arg("-f")
            .arg("mp4")
            .arg(&output_path)
            .kill_on_drop(true);
        let output = tokio::time::timeout(ffmpeg_timeout(), command.output())
            .await
            .map_err(|_| "ffmpeg transcode timed out".to_string())?
            .map_err(|err| format!("spawn ffmpeg failed: {err}"))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let message = stderr.trim();
            return Err(if message.is_empty() {
                format!("ffmpeg transcode failed with status {}", output.status)
            } else {
                format!("ffmpeg transcode failed: {message}")
            });
        }
        let out = tokio::fs::read(&output_path)
            .await
            .map_err(|err| format!("read ffmpeg output failed: {err}"))?;
        check_transcode_output_bytes(out.len() as u64)?;
        Ok((out, content_type, ext))
    }
    .await;

    let _ = tokio::fs::remove_dir_all(cleanup_dir).await;
    result
}

/// Reject a source object whose byte length exceeds [`MAX_IMAGE_INPUT_BYTES`].
/// Pure predicate (no decode) so it is unit-testable without the image crate.
#[cfg(feature = "asset-image")]
fn check_input_bytes(len: u64) -> Result<(), String> {
    if len > MAX_IMAGE_INPUT_BYTES {
        return Err(format!(
            "source image is {len} bytes, exceeds the {MAX_IMAGE_INPUT_BYTES}-byte image-step decode limit"
        ));
    }
    Ok(())
}

/// Reject a source image whose HEADER pixel count exceeds [`MAX_IMAGE_PIXELS`].
/// Pure predicate over already-probed header dimensions (no decode), so it is
/// unit-testable without the image crate.
#[cfg(feature = "asset-image")]
fn check_image_pixels(width: u32, height: u32) -> Result<(), String> {
    let pixels = u64::from(width) * u64::from(height);
    if pixels > MAX_IMAGE_PIXELS {
        return Err(format!(
            "source image is {width}x{height} ({pixels} px), exceeds the {MAX_IMAGE_PIXELS}-pixel image-step decode limit"
        ));
    }
    Ok(())
}

/// Resolve the requested output `format` to a concrete encoder + content-type +
/// file extension. `None` uses the per-step default. This build links only the
/// PNG and JPEG codecs (Cargo `image` feature set), so any other format fails
/// CLOSED with `invalid_argument`-grade reasoning rather than a capability lie.
#[cfg(feature = "asset-image")]
fn resolve_output_format(
    requested: Option<&str>,
    default: image::ImageFormat,
) -> Result<(image::ImageFormat, &'static str, &'static str), String> {
    let triple = |fmt: image::ImageFormat| match fmt {
        image::ImageFormat::Jpeg => (image::ImageFormat::Jpeg, "image/jpeg", "jpg"),
        _ => (image::ImageFormat::Png, "image/png", "png"),
    };
    match requested {
        None => Ok(triple(default)),
        Some(f) => match f {
            "png" => Ok(triple(image::ImageFormat::Png)),
            "jpg" | "jpeg" => Ok(triple(image::ImageFormat::Jpeg)),
            other => Err(format!(
                "unsupported output image format '{other}' (this build supports: png, jpeg)"
            )),
        },
    }
}

/// Apply the parameterized image transform for a byte step. Pure (decode→encode
/// happen around it in `run_byte_step`), so the param→geometry behavior is unit-
/// testable on a tiny in-memory image with no object backend or pool.
///
/// THUMBNAIL squares to [`DEFAULT_THUMBNAIL_EDGE`] unless params override the edge.
/// RESIZE honors the requested width/height (aspect-preserving, fits the box). A
/// RESIZE with no dimensions but a `format` is a CONVERT: keep the original size,
/// re-encode into the requested format. A RESIZE with neither dimensions nor a
/// format, or any non-image step type, fails EXPLICITLY (no silent fallback).
#[cfg(feature = "asset-image")]
fn apply_image_transform(
    img: image::DynamicImage,
    step_type: asset_entity_pb::StepType,
    params: &ByteStepParams,
) -> Result<image::DynamicImage, String> {
    use asset_entity_pb::StepType as T;
    let out = match step_type {
        T::Thumbnail => {
            let w = params.width.unwrap_or(DEFAULT_THUMBNAIL_EDGE);
            let h = params.height.unwrap_or(DEFAULT_THUMBNAIL_EDGE);
            img.thumbnail(w, h)
        }
        T::Resize => match (params.width, params.height) {
            (None, None) if params.format.is_some() => img,
            (None, None) => {
                return Err(
                    "RESIZE requires a width and/or height param (or a format param to convert only)"
                        .to_string(),
                );
            }
            (w, h) => img.resize(
                w.unwrap_or(u32::MAX),
                h.unwrap_or(u32::MAX),
                image::imageops::FilterType::Lanczos3,
            ),
        },
        other => {
            return Err(format!(
                "byte step {} is not an image transform",
                other.as_str_name()
            ));
        }
    };
    Ok(out)
}

/// Build the derived object's key under the [`DERIVED_OBJECT_PREFIX`] namespace,
/// tagged by step type + output extension so multiple derivations of one source
/// stay distinct AND unique (the File table's `object_key` is UNIQUE). Fixes the
/// old `{object_key}.thumb.png` collision that shared the source key's prefix.
fn derived_object_key(source_key: &str, step: asset_entity_pb::StepType, ext: &str) -> String {
    let tag = step
        .as_str_name()
        .trim_start_matches("STEP_TYPE_")
        .to_ascii_lowercase();
    format!("{DERIVED_OBJECT_PREFIX}{source_key}.{tag}.{ext}")
}

/// Register derived media as a first-class `udb_storage.files` row so it is a
/// tracked object (quota/GC/lifecycle), not an orphan blob. Mirrors
/// [`AssetServiceImpl::resolve_object_key`]'s manifest-driven, tenant-bound raw
/// SQL on the same pool; idempotent via `ON CONFLICT (object_key) DO NOTHING`.
async fn register_derived_file(
    pool: &PgPool,
    tenant_id: Uuid,
    derived_key: &str,
    backend: &str,
    bucket: &str,
    content_type: &str,
    file_type: &str,
    size_bytes: i64,
) -> Result<(), String> {
    let m = native_model(
        "udb.core.storage.entity.v1.File",
        &["file_id", "object_key"],
    );
    let rel = m.relation.clone();
    let filename = derived_key.rsplit('/').next().unwrap_or(derived_key);
    sqlx::query(&format!(
        "INSERT INTO {rel} \
           ({tid}, {fname}, {okey}, {be}, {bk}, {ct}, {sz}, {st}, {ft}) \
         VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, 'ACTIVE', $8) \
         ON CONFLICT ({okey}) DO NOTHING",
        tid = m.q("tenant_id"),
        fname = m.q("filename"),
        okey = m.q("object_key"),
        be = m.q("backend"),
        bk = m.q("bucket"),
        ct = m.q("content_type"),
        sz = m.q("size_bytes"),
        st = m.q("status"),
        ft = m.q("file_type"),
    ))
    .bind(tenant_id)
    .bind(filename)
    .bind(derived_key)
    .bind(backend)
    .bind(bucket)
    .bind(content_type)
    .bind(size_bytes)
    .bind(file_type)
    .execute(pool)
    .await
    .map_err(|e| format!("insert derived file row failed: {e}"))?;
    Ok(())
}

/// Transform parameters parsed from a pipeline step; the byte-step executor
/// fetches object bytes separately before applying these settings.
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
    .map_err(|err| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("aggregate step status failed: {err}"),
        )
    })?;
    let total: i64 = counts.try_get("total").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode total failed: {e}"),
        )
    })?;
    let done: i64 = counts.try_get("done").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode done failed: {e}"),
        )
    })?;
    let failed: i64 = counts.try_get("failed").map_err(|e| {
        asset_internal_status(
            "advance_pipeline_instance",
            format!("decode failed failed: {e}"),
        )
    })?;

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
        .map_err(|err| {
            asset_internal_status(
                "advance_pipeline_instance",
                format!("advance pipeline instance failed: {err}"),
            )
        })?;

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
            "trigger_topic",
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
            "params",
            "retry_count",
            "started_at",
            "completed_at",
        ],
    )
}

use super::native_helpers::{non_empty_json, parse_uuid};

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn logical_json_text(value: &str) -> Result<LogicalValue, Status> {
    serde_json::from_str::<serde_json::Value>(value)
        .map(LogicalValue::Json)
        .map_err(|err| {
            asset_invalid_field(
                "json",
                "must be valid native JSON",
                format!("native JSON field is invalid: {err}"),
            )
        })
}

fn eq_filter(field: &str, value: impl Into<String>) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

fn and_filter(filters: Vec<LogicalFilter>) -> LogicalFilter {
    LogicalFilter::And(filters)
}

fn asset_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "asset_id".to_string(),
        "tenant_id".to_string(),
        "project_id".to_string(),
        "file_id".to_string(),
        "name".to_string(),
        "media_type".to_string(),
        "status".to_string(),
        "metadata".to_string(),
    ])
}

fn pipeline_definition_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "definition_id".to_string(),
        "tenant_id".to_string(),
        "name".to_string(),
        "description".to_string(),
        "media_type".to_string(),
        "steps".to_string(),
        "version".to_string(),
        "status".to_string(),
        "trigger_topic".to_string(),
    ])
}

fn asset_read(
    tenant_id: &str,
    asset_id: Option<&str>,
    media_type: Option<&str>,
    status: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    let mut filters = vec![
        eq_filter("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ];
    if let Some(asset_id) = asset_id.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("asset_id", asset_id));
    }
    if let Some(media_type) = media_type.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("media_type", media_type));
    }
    if let Some(status) = status.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("status", status));
    }
    LogicalRead {
        message_type: ASSET_MSG.to_string(),
        filter: Some(and_filter(filters)),
        projection: Some(asset_projection()),
        sort: vec![LogicalSort {
            field: "name".to_string(),
            direction: SortDirection::Asc,
            nulls: Default::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

fn pipeline_definition_read(tenant_id: &str, definition_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: PIPELINE_DEFINITION_MSG.to_string(),
        filter: Some(and_filter(vec![
            eq_filter("definition_id", definition_id),
            eq_filter("tenant_id", tenant_id),
        ])),
        projection: Some(pipeline_definition_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn native_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string_field(row: &serde_json::Map<String, serde_json::Value>, logical: &str) -> String {
    row.get(logical)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Some(value.to_string()),
            serde_json::Value::Null => None,
        })
        .unwrap_or_default()
}

fn json_i32_field(row: &serde_json::Map<String, serde_json::Value>, logical: &str) -> i32 {
    row.get(logical)
        .and_then(|value| value.as_i64())
        .unwrap_or_default() as i32
}

fn asset_from_json(row: &serde_json::Value) -> asset_entity_pb::Asset {
    let row = native_json_object(row);
    asset_entity_pb::Asset {
        asset_id: json_string_field(row, "asset_id"),
        tenant_id: json_string_field(row, "tenant_id"),
        project_id: json_string_field(row, "project_id"),
        file_id: json_string_field(row, "file_id"),
        name: json_string_field(row, "name"),
        media_type: json_string_field(row, "media_type"),
        status: asset_status_from_db(&json_string_field(row, "status")),
        metadata: json_string_field(row, "metadata"),
        ..Default::default()
    }
}

fn pipeline_definition_from_json(row: &serde_json::Value) -> asset_entity_pb::PipelineDefinition {
    let row = native_json_object(row);
    asset_entity_pb::PipelineDefinition {
        definition_id: json_string_field(row, "definition_id"),
        tenant_id: json_string_field(row, "tenant_id"),
        name: json_string_field(row, "name"),
        description: json_string_field(row, "description"),
        media_type: json_string_field(row, "media_type"),
        steps: json_string_field(row, "steps"),
        version: json_i32_field(row, "version"),
        status: json_string_field(row, "status"),
        trigger_topic: json_string_field(row, "trigger_topic"),
        ..Default::default()
    }
}

fn asset_record(
    asset_id: &str,
    tenant_id: &str,
    project_id: &str,
    req: &asset_pb::RegisterAssetRequest,
    metadata_json: &str,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert("asset_id".to_string(), logical_string(asset_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert(
        "project_id".to_string(),
        if project_id.trim().is_empty() {
            LogicalValue::Null
        } else {
            logical_string(project_id)
        },
    );
    record.insert("file_id".to_string(), logical_string(req.file_id.trim()));
    record.insert("name".to_string(), logical_string(req.name.clone()));
    record.insert(
        "media_type".to_string(),
        logical_string(req.media_type.clone()),
    );
    record.insert("status".to_string(), logical_string("PENDING"));
    record.insert("metadata".to_string(), logical_json_text(metadata_json)?);
    Ok(record)
}

fn pipeline_definition_record(
    definition_id: &str,
    tenant_id: &str,
    req: &asset_pb::CreatePipelineDefinitionRequest,
    steps_json: &str,
    version: i32,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert("definition_id".to_string(), logical_string(definition_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("name".to_string(), logical_string(req.name.clone()));
    record.insert(
        "description".to_string(),
        logical_string(req.description.clone()),
    );
    record.insert(
        "media_type".to_string(),
        logical_string(req.media_type.clone()),
    );
    record.insert("steps".to_string(), logical_json_text(steps_json)?);
    record.insert("version".to_string(), LogicalValue::Int(version as i64));
    record.insert("status".to_string(), logical_string("ACTIVE"));
    Ok(record)
}

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
            return Err(asset_invalid_field(
                "status",
                "must be a supported AssetStatus enum value",
                format!("unknown asset status: {other}"),
            ));
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
            return Err(asset_invalid_field(
                "status",
                "must be a supported StepStatus enum value",
                format!("unknown step status: {other}"),
            ));
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
            return Err(asset_invalid_field(
                "step_type",
                "must be a supported StepType enum value",
                format!("unknown step type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

fn active_storage_file_required_status() -> Status {
    asset_invalid_field(
        "file_id",
        "must reference an active storage file owned by this tenant",
        "file_id does not reference an active storage file owned by this tenant",
    )
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
    let map = |e: sqlx::Error| {
        asset_internal_status(
            "decode_pipeline_instance",
            format!("decode pipeline instance failed: {e}"),
        )
    };
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
        m.text_or_empty("params"),
        m.select("retry_count"),
    ]
    .join(", ")
}

fn pipeline_step_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineStep, Status> {
    let map = |e: sqlx::Error| {
        asset_internal_status(
            "decode_pipeline_step",
            format!("decode pipeline step failed: {e}"),
        )
    };
    Ok(asset_entity_pb::PipelineStep {
        step_id: row.try_get("step_id").map_err(map)?,
        instance_id: row.try_get("instance_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        step_name: row.try_get("step_name").map_err(map)?,
        step_type: step_type_from_db(&row.try_get::<String, _>("step_type").map_err(map)?),
        status: step_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        result: row.try_get("result").map_err(map)?,
        error: row.try_get("error").map_err(map)?,
        params: row.try_get("params").map_err(map)?,
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
            return Err(asset_required_field(
                "name",
                "must be a non-empty pipeline definition name",
                "name is required",
            ));
        }
        let steps = {
            let s = req.steps.trim();
            if s.is_empty() {
                "[]".to_string()
            } else {
                serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                    asset_invalid_field(
                        "steps",
                        "must be valid JSON",
                        format!("steps must be valid JSON: {e}"),
                    )
                })?;
                s.to_string()
            }
        };
        let version = if req.version > 0 { req.version } else { 1 };
        let definition_id = Uuid::new_v4().to_string();
        let context = native_service_context(&metadata, &req.tenant_id, "");
        self.require_runtime()?
            .native_entity_write_for_service(
                "asset",
                &context,
                PIPELINE_DEFINITION_MSG,
                pipeline_definition_record(
                    &definition_id,
                    &tenant_id.to_string(),
                    &req,
                    &steps,
                    version,
                )?,
                ConflictStrategy::Error,
            )
            .await
            .map_err(|err| {
                crate::runtime::executor_utils::prefix_status(
                    "create pipeline definition failed",
                    err,
                )
            })?;
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
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let rows = self
            .require_runtime()?
            .native_entity_read_for_service(
                "asset",
                &context,
                pipeline_definition_read(&tenant_id.to_string(), &definition_id.to_string()),
            )
            .await?;
        let definition = rows.first().map(pipeline_definition_from_json);
        if definition.is_none() {
            return Err(asset_schema_not_found_status(
                "get_pipeline_definition",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ));
        }
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
            return Err(asset_required_field(
                "file_id",
                "must be a non-empty storage file id",
                "file_id is required",
            ));
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
            return Err(active_storage_file_required_status());
        }
        let asset_id = Uuid::new_v4().to_string();
        let asset_metadata = self.encrypt_native_json_state(&non_empty_json(&req.metadata))?;
        let context = native_service_context(&metadata, &req.tenant_id, req.project_id.trim());
        self.require_runtime()?
            .native_entity_write_for_service(
                "asset",
                &context,
                ASSET_MSG,
                asset_record(
                    &asset_id,
                    &tenant_id.to_string(),
                    req.project_id.trim(),
                    &req,
                    &asset_metadata,
                )?,
                ConflictStrategy::Error,
            )
            .await
            .map_err(|err| {
                crate::runtime::executor_utils::prefix_status("register asset failed", err)
            })?;
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
            .map_err(|err| {
                asset_internal_status(
                    "start_pipeline",
                    format!("start pipeline lookup failed: {err}"),
                )
            })? {
                let instance_id: String = existing.try_get("instance_id").map_err(|e| {
                    asset_internal_status(
                        "start_pipeline",
                        format!("decode instance id failed: {e}"),
                    )
                })?;
                return Ok(Response::new(asset_pb::StartPipelineResponse {
                    instance_id,
                    message: format!("pipeline already started [{PIPELINE_ALREADY_STARTED}]"),
                    error: None,
                    // Idempotent hit: only the existing instance id is in scope.
                    // Steps are left empty to avoid an extra round-trip; callers
                    // wanting them for an already-running instance call GetPipeline.
                    steps: Vec::new(),
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
        .map_err(|err| {
            asset_internal_status(
                "start_pipeline",
                format!("load pipeline definition failed: {err}"),
            )
        })?;
        let steps_json = match steps_json {
            Some(s) => s,
            None => {
                return Err(asset_schema_not_found_status(
                    "start_pipeline",
                    "pipeline_definition_not_found",
                    "pipeline definition not found",
                ));
            }
        };
        let parsed: serde_json::Value = serde_json::from_str(&steps_json).map_err(|e| {
            status_with_reason(
                asset_internal_status(
                    "start_pipeline",
                    format!("pipeline definition steps not JSON: {e}"),
                ),
                PIPELINE_DEFINITION_INVALID,
            )
        })?;
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
                .map_err(|e| {
                    asset_internal_status(
                        "start_pipeline",
                        format!("start pipeline re-lookup failed: {e}"),
                    )
                })?;
                if let Some(row) = existing {
                    let id: String = row.try_get("instance_id").map_err(|e| {
                        asset_internal_status(
                            "start_pipeline",
                            format!("decode instance id failed: {e}"),
                        )
                    })?;
                    return Ok(Response::new(asset_pb::StartPipelineResponse {
                        instance_id: id,
                        message: format!("pipeline already started [{PIPELINE_ALREADY_STARTED}]"),
                        error: None,
                        // Race branch: only the existing instance id is in scope.
                        // Reading its steps would cost an extra round-trip, so the
                        // step list is left empty here; callers wanting steps for an
                        // already-running instance call GetPipeline.
                        steps: Vec::new(),
                    }));
                }
            }
            return Err(crate::runtime::executor_utils::sqlx_error_to_status(
                "start pipeline failed",
                &err,
            ));
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
        .map_err(|err| {
            asset_internal_status(
                "start_pipeline",
                format!("load asset for pipeline failed: {err}"),
            )
        })?
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

        // Accumulate the materialized steps so the response can return them
        // inline (mirrors GetPipelineResponse.steps) without a follow-up read.
        let mut response_steps: Vec<asset_entity_pb::PipelineStep> =
            Vec::with_capacity(step_array.len());

        // Materialize each step, RUN it in-process, and record the outcome.
        for el in &step_array {
            let step_name = el.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let step_type_str = el.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let step_type = step_type_to_db(step_type_str, "EMBED").map_err(|e| {
                // Same Status code; only add the stable reason for SDK branching.
                status_with_reason(e, STEP_TYPE_UNSUPPORTED)
            })?;
            let step_type_i32 = step_type_from_db(&step_type);
            let step_id = Uuid::new_v4().to_string();
            // Persist the step's transform params (RESIZE width/height, CONVERT
            // format) so the configuration is a first-class part of the step row
            // and is echoed back on read — not just consumed inline by the byte
            // step. A step with no `params` object stores `{}`.
            let step_params_json = el
                .get("params")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}))
                .to_string();

            // Pure-CPU metadata steps (EMBED/EXTRACT) run via the sync registry.
            // Byte-IO steps (THUMBNAIL/RESIZE/TRANSCODE) fetch the source object,
            // transform, and store a derived object — inherently async, so they
            // take a separate path (still no registry edit to add metadata step types).
            let outcome = if is_byte_step(step_type_i32) {
                self.run_byte_step(
                    pool,
                    step_type_i32,
                    &asset_file_id,
                    tenant_id,
                    &asset_project_id,
                    &parse_byte_step_params(el),
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
                 ({step_id}, {instance_id}, {tenant_id}, {step_name}, {step_type}, {status}, {result}, {error}, {params}, {completed_at}) \
                 VALUES ($1::UUID, $2::UUID, $3::UUID, $4, $5, $6, $7::JSONB, NULLIF($8, ''), $9::JSONB, CURRENT_TIMESTAMP)",
                step_id = step.q("step_id"),
                instance_id = step.q("instance_id"),
                tenant_id = step.q("tenant_id"),
                step_name = step.q("step_name"),
                step_type = step.q("step_type"),
                status = step.q("status"),
                result = step.q("result"),
                error = step.q("error"),
                params = step.q("params"),
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
            .bind(&step_params_json)
            .execute(pool)
            .await
            .map_err(|err| {
                crate::runtime::executor_utils::sqlx_error_to_status(
                    "create pipeline step failed",
                    &err,
                )
            })?;

            // Mirror the persisted row into the response. The plaintext result /
            // error come straight from `outcome` (the same values the row holds,
            // pre-encryption), matching what GetPipeline returns after decrypt.
            let (step_result_plain, step_error_plain) = match &outcome {
                StepOutcome::Completed(v) => (v.to_string(), String::new()),
                StepOutcome::Failed(msg) => ("{}".to_string(), msg.clone()),
            };
            response_steps.push(asset_entity_pb::PipelineStep {
                step_id: step_id.clone(),
                instance_id: instance_id.clone(),
                tenant_id: req.tenant_id.clone(),
                step_name: step_name.to_string(),
                step_type: step_type_i32,
                status: step_status_from_db(status_token),
                result: step_result_plain,
                error: step_error_plain,
                params: step_params_json.clone(),
                ..Default::default()
            });

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
            steps: response_steps,
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
        .map_err(|err| {
            asset_internal_status("get_pipeline", format!("get pipeline failed: {err}"))
        })?;
        let instance = match row {
            Some(row) => {
                let mut instance = pipeline_instance_from_row(&row)?;
                instance.context = self.decrypt_native_json_state(&instance.context)?;
                Some(instance)
            }
            None => {
                return Err(asset_schema_not_found_status(
                    "get_pipeline",
                    "pipeline_instance_not_found",
                    "pipeline instance not found",
                ));
            }
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
        .map_err(|err| {
            asset_internal_status("get_pipeline", format!("get pipeline steps failed: {err}"))
        })?;
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
        .map_err(|err| {
            asset_internal_status("complete_step", format!("complete step failed: {err}"))
        })?;
        if result.rows_affected() == 0 {
            return Err(asset_schema_not_found_status(
                "complete_step",
                "pipeline_step_not_found",
                "pipeline step not found",
            ));
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
        .map_err(|err| {
            asset_internal_status(
                "complete_step",
                format!("resolve step instance failed: {err}"),
            )
        })?;

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
        let m = asset_model();
        let rel = m.relation.clone();
        let media_filter = req.media_type.trim().to_string();
        let status_filter = asset_status_to_db(&req.status, "")?;
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let pool = self.require_pool()?;
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
            .map_err(|err| {
                asset_internal_status("list_assets", format!("count assets failed: {err}"))
            })?;
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let rows = self
            .require_runtime()?
            .native_entity_read_for_service(
                "asset",
                &context,
                asset_read(
                    &tenant_id.to_string(),
                    None,
                    Some(&media_filter),
                    Some(&status_filter),
                    page_window.offset as u64,
                    page_window.limit as u32,
                ),
            )
            .await?;
        let mut assets = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut asset = asset_from_json(row);
            asset.metadata = self.decrypt_native_json_state(&asset.metadata)?;
            assets.push(asset);
        }
        Ok(Response::new(asset_pb::ListAssetsResponse {
            assets,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
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
        let context = native_service_context(&metadata, &req.tenant_id, "");
        let rows = self
            .require_runtime()?
            .native_entity_read_for_service(
                "asset",
                &context,
                asset_read(
                    &tenant_id.to_string(),
                    Some(&asset_id.to_string()),
                    None,
                    None,
                    0,
                    1,
                ),
            )
            .await?;
        let asset = match rows.first() {
            Some(row) => {
                let mut asset = asset_from_json(row);
                asset.metadata = self.decrypt_native_json_state(&asset.metadata)?;
                Some(asset)
            }
            None => {
                return Err(asset_schema_not_found_status(
                    "get_asset",
                    "asset_not_found",
                    "asset not found",
                ));
            }
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
    fn registry_dispatches_embed_extract_and_fails_unknown_metadata_step() {
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

        match registry.run(T::Caption as i32, &ctx) {
            StepOutcome::Failed(msg) => {
                assert!(
                    msg.contains("not yet implemented"),
                    "CAPTION failure message should explain it is unimplemented, got: {msg}"
                );
            }
            StepOutcome::Completed(_) => panic!("CAPTION must fail (no capability lie)"),
        }
    }

    #[test]
    fn transcode_is_async_byte_step_not_registry_step() {
        assert!(is_byte_step(T::Transcode as i32));
        assert!(!is_byte_step(T::Caption as i32));
    }
}

#[cfg(test)]
mod byte_step_param_tests {
    use super::*;

    #[test]
    fn parses_params_subobject_and_flattened_keys() {
        let nested = serde_json::json!({
            "type": "RESIZE",
            "params": { "width": 800, "height": 600, "format": "JPEG" }
        });
        let p = parse_byte_step_params(&nested);
        assert_eq!(p.width, Some(800));
        assert_eq!(p.height, Some(600));
        assert_eq!(p.format.as_deref(), Some("jpeg"), "format is lower-cased");

        // Flattened top-level keys (and string-encoded numbers) also resolve.
        let flat = serde_json::json!({ "type": "RESIZE", "width": "1024", "format": "png" });
        let p = parse_byte_step_params(&flat);
        assert_eq!(p.width, Some(1024));
        assert_eq!(p.height, None);
        assert_eq!(p.format.as_deref(), Some("png"));

        // Absent/zero/blank values fall back to None (per-step defaults apply).
        let empty = serde_json::json!({ "type": "THUMBNAIL", "width": 0, "format": "  " });
        let p = parse_byte_step_params(&empty);
        assert_eq!(p.width, None);
        assert_eq!(p.format, None);
    }

    #[test]
    fn transcode_format_is_allowlisted() {
        let (_, content_type, ext) =
            resolve_transcode_output_format(None).expect("mp4 is the default");
        assert_eq!(content_type, "video/mp4");
        assert_eq!(ext, "mp4");
        assert!(resolve_transcode_output_format(Some("mp4")).is_ok());
        assert!(resolve_transcode_output_format(Some("webm")).is_err());
    }

    #[test]
    fn transcode_byte_limits_are_bounded() {
        assert!(check_transcode_input_bytes(MAX_TRANSCODE_INPUT_BYTES).is_ok());
        assert!(check_transcode_input_bytes(MAX_TRANSCODE_INPUT_BYTES + 1).is_err());
        assert!(check_transcode_output_bytes(MAX_TRANSCODE_OUTPUT_BYTES).is_ok());
        assert!(check_transcode_output_bytes(MAX_TRANSCODE_OUTPUT_BYTES + 1).is_err());
    }

    #[test]
    fn vendored_ffmpeg_search_contract_uses_platform_layout() {
        let expected_suffix = Path::new("third_party")
            .join("ffmpeg")
            .join("bin")
            .join(ffmpeg_platform_dir())
            .join(ffmpeg_exe_name());
        let candidates = vendored_ffmpeg_candidates();
        assert!(
            !candidates.is_empty(),
            "vendored ffmpeg search must always have at least one candidate"
        );
        assert!(
            candidates
                .iter()
                .any(|path| path.ends_with(&expected_suffix)),
            "search must include the documented third_party/ffmpeg/bin/<platform> layout: {candidates:?}"
        );
        assert!(vendored_ffmpeg_path().ends_with(expected_suffix));
    }
}

/// Pure decode-limit guard tests — exercise the predicates without decoding any
/// real image, proving the byte/pixel caps reject before the full decode.
#[cfg(all(test, feature = "asset-image"))]
mod image_limit_tests {
    use super::*;

    #[test]
    fn input_bytes_over_limit_rejected() {
        assert!(check_input_bytes(MAX_IMAGE_INPUT_BYTES + 1).is_err());
    }

    #[test]
    fn input_bytes_at_or_under_limit_ok() {
        assert!(check_input_bytes(MAX_IMAGE_INPUT_BYTES).is_ok());
        assert!(check_input_bytes(0).is_ok());
        assert!(check_input_bytes(1024).is_ok());
    }

    #[test]
    fn pixels_over_limit_rejected() {
        // A 1×(MAX+1) header probe (a classic pixel-flood bomb shape) is rejected.
        assert!(check_image_pixels(1, (MAX_IMAGE_PIXELS + 1) as u32).is_err());
        // 9000×9000 = 81 MP > 64 MP cap.
        assert!(check_image_pixels(9000, 9000).is_err());
    }

    #[test]
    fn pixels_at_or_under_limit_ok() {
        assert!(check_image_pixels(8000, 8000).is_ok()); // 64 MP, exactly the cap
        assert!(check_image_pixels(1920, 1080).is_ok());
        assert!(check_image_pixels(0, 0).is_ok());
    }

    #[test]
    fn output_format_resolves_and_rejects_unknown() {
        assert_eq!(
            resolve_output_format(Some("jpg"), image::ImageFormat::Png)
                .unwrap()
                .2,
            "jpg"
        );
        assert_eq!(
            resolve_output_format(None, image::ImageFormat::Png)
                .unwrap()
                .2,
            "png"
        );
        assert!(resolve_output_format(Some("gif"), image::ImageFormat::Png).is_err());
    }

    #[test]
    fn derived_key_is_namespaced_and_collision_free() {
        let key = derived_object_key(
            "tenant/file/photo.png",
            asset_entity_pb::StepType::Thumbnail,
            "png",
        );
        assert_eq!(key, "derived/tenant/file/photo.png.thumbnail.png");
        assert!(key.starts_with(DERIVED_OBJECT_PREFIX));
        assert_ne!(
            key, "tenant/file/photo.png",
            "must not collide with the source key"
        );
    }

    fn solid_image(w: u32, h: u32) -> image::DynamicImage {
        image::DynamicImage::ImageRgba8(image::RgbaImage::new(w, h))
    }

    #[test]
    fn resize_param_produces_requested_dimensions() {
        // A square source resized into a 4x4 box yields exactly 4x4 (aspect 1:1).
        let out = apply_image_transform(
            solid_image(10, 10),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams {
                width: Some(4),
                height: Some(4),
                format: None,
            },
        )
        .expect("RESIZE with dimensions must succeed");
        assert_eq!((out.width(), out.height()), (4, 4));
    }

    #[test]
    fn convert_only_resize_preserves_dimensions() {
        // RESIZE with a format but no dimensions == CONVERT: keep the source size.
        let out = apply_image_transform(
            solid_image(7, 5),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams {
                width: None,
                height: None,
                format: Some("jpeg".to_string()),
            },
        )
        .expect("format-only RESIZE (CONVERT) must succeed");
        assert_eq!((out.width(), out.height()), (7, 5));
    }

    #[test]
    fn resize_without_dimensions_or_format_fails_explicitly() {
        let err = apply_image_transform(
            solid_image(7, 5),
            asset_entity_pb::StepType::Resize,
            &ByteStepParams::default(),
        )
        .expect_err("RESIZE with neither dimensions nor format must fail explicitly");
        assert!(
            err.contains("RESIZE requires"),
            "explicit reason, got: {err}"
        );
    }

    #[test]
    fn thumbnail_honors_param_edge_within_box() {
        // 100x50 thumbnailed into a 16-box fits within 16x16 (aspect-preserving).
        let out = apply_image_transform(
            solid_image(100, 50),
            asset_entity_pb::StepType::Thumbnail,
            &ByteStepParams {
                width: Some(16),
                height: Some(16),
                format: None,
            },
        )
        .expect("THUMBNAIL must succeed");
        assert!(
            out.width() <= 16 && out.height() <= 16,
            "got {}x{}",
            out.width(),
            out.height()
        );
    }

    #[test]
    fn non_image_byte_step_fails_explicitly() {
        let err = apply_image_transform(
            solid_image(2, 2),
            asset_entity_pb::StepType::Transcode,
            &ByteStepParams::default(),
        )
        .expect_err("a non-image step type must not be silently transformed");
        assert!(
            err.contains("not an image transform"),
            "explicit reason, got: {err}"
        );
    }

    #[test]
    fn unimplemented_convert_format_fails_explicitly() {
        // An unsupported output format (this build links only png/jpeg) fails CLOSED
        // rather than silently re-encoding as the default — no capability lie.
        assert!(resolve_output_format(Some("webp"), image::ImageFormat::Png).is_err());
        assert!(resolve_output_format(Some("tiff"), image::ImageFormat::Png).is_err());
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn asset_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &asset_internal_status(
                "start_pipeline",
                "load pipeline definition failed: unavailable",
            ),
            "start_pipeline",
            "load pipeline definition failed: unavailable",
        );
    }

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

    #[tokio::test]
    async fn create_pipeline_definition_missing_name_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no runtime; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: " ".to_string(),
            steps: "[]".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .create_pipeline_definition(request)
            .await
            .expect_err("missing pipeline name must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty pipeline definition name"
        );
    }

    #[tokio::test]
    async fn create_pipeline_definition_invalid_steps_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no runtime; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::CreatePipelineDefinitionRequest {
            tenant_id: tenant_id.clone(),
            name: "resize-images".to_string(),
            steps: "{not-json".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .create_pipeline_definition(request)
            .await
            .expect_err("invalid steps JSON must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("steps must be valid JSON:"));
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "steps");
        assert_eq!(detail.field_violations[0].description, "must be valid JSON");
    }

    #[tokio::test]
    async fn register_asset_missing_file_id_carries_field_violation() {
        let svc = AssetServiceImpl::new(); // no pool; validation runs first
        let tenant_id = Uuid::new_v4().to_string();
        let mut request = Request::new(asset_pb::RegisterAssetRequest {
            tenant_id: tenant_id.clone(),
            file_id: " ".to_string(),
            ..Default::default()
        });
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(tenant_id.as_str()).unwrap(),
        );

        let err = svc
            .register_asset(request)
            .await
            .expect_err("missing file id must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "file_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "file_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty storage file id"
        );
    }

    #[test]
    fn asset_helper_validation_carries_field_violations() {
        let json = logical_json_text("{not-json").expect_err("invalid JSON must fail");
        assert_eq!(json.code(), tonic::Code::InvalidArgument);
        assert!(json.message().starts_with("native JSON field is invalid:"));
        assert_single_field_violation(&json, "json", "must be valid native JSON");

        let asset_status =
            asset_status_to_db("archived", "PENDING").expect_err("unknown asset status");
        assert_eq!(asset_status.code(), tonic::Code::InvalidArgument);
        assert_eq!(asset_status.message(), "unknown asset status: ARCHIVED");
        assert_single_field_violation(
            &asset_status,
            "status",
            "must be a supported AssetStatus enum value",
        );

        let step_status = step_status_to_db("paused", "PENDING").expect_err("unknown step status");
        assert_eq!(step_status.code(), tonic::Code::InvalidArgument);
        assert_eq!(step_status.message(), "unknown step status: PAUSED");
        assert_single_field_violation(
            &step_status,
            "status",
            "must be a supported StepStatus enum value",
        );

        let step_type = step_type_to_db("watermark", "EMBED").expect_err("unknown step type");
        assert_eq!(step_type.code(), tonic::Code::InvalidArgument);
        assert_eq!(step_type.message(), "unknown step type: WATERMARK");
        assert_single_field_violation(
            &step_type,
            "step_type",
            "must be a supported StepType enum value",
        );
    }

    #[test]
    fn register_asset_inactive_file_status_carries_field_violation() {
        let err = active_storage_file_required_status();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "file_id does not reference an active storage file owned by this tenant"
        );
        assert_single_field_violation(
            &err,
            "file_id",
            "must reference an active storage file owned by this tenant",
        );
    }

    #[test]
    fn asset_missing_runtime_capability_carries_typed_detail() {
        let err = asset_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "asset service requires runtime native entity dispatch",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "asset service requires runtime native entity dispatch"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "asset");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }

    #[test]
    fn native_state_crypto_failures_carry_capability_detail() {
        for (err, message, operation) in [
            (
                native_state_encryption_failed_status("key unavailable"),
                "native-state encryption failed: key unavailable",
                "native_state_encrypt",
            ),
            (
                native_state_decryption_failed_status("ciphertext invalid"),
                "native-state decryption failed: ciphertext invalid",
                "native_state_decrypt",
            ),
        ] {
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "asset");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, "native_state_encryption");
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn asset_not_found_statuses_carry_schema_detail() {
        for (operation, schema_code, message) in [
            (
                "get_pipeline_definition",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ),
            (
                "start_pipeline",
                "pipeline_definition_not_found",
                "pipeline definition not found",
            ),
            (
                "get_pipeline",
                "pipeline_instance_not_found",
                "pipeline instance not found",
            ),
            (
                "complete_step",
                "pipeline_step_not_found",
                "pipeline step not found",
            ),
            ("get_asset", "asset_not_found", "asset not found"),
        ] {
            assert_schema_not_found_detail(
                &asset_schema_not_found_status(operation, schema_code, message),
                operation,
                schema_code,
                message,
            );
        }
    }
}

impl DataBrokerService {
    /// Build the native `AssetService`, wired to the broker's Postgres pool.
    pub(crate) fn build_asset_service(&self) -> AssetServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("asset", true, "")
            .ok();
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

/// Backoff after a consumer recv error so an unreachable broker (no Kafka in the
/// deployment) can't spin this loop. Matches the topic-not-visible sleep below.
#[cfg(feature = "kafka")]
const STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS: u64 = 2;

/// Cooldown for the recv-error WARN log, so a persistently unreachable broker
/// collapses to one line per window (with a suppressed count) instead of a flood.
#[cfg(feature = "kafka")]
const STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS: u64 = 30;

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
async fn ensure_storage_finalized_topic(brokers: &str) -> Result<(), String> {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|err| format!("create Kafka admin client failed: {err}"))?;
    match admin
        .create_topics(
            &[NewTopic::new(
                STORAGE_FINALIZED_TOPIC,
                1,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
    {
        Ok(results) => {
            for result in results {
                if let Err((name, code)) = result
                    && !format!("{code:?}").contains("TopicAlreadyExists")
                {
                    return Err(format!("create Kafka topic {name} failed: {code:?}"));
                }
            }
        }
        Err(err) => return Err(format!("create Kafka topic request failed: {err}")),
    }
    admin
        .inner()
        .fetch_metadata(
            Some(STORAGE_FINALIZED_TOPIC),
            std::time::Duration::from_secs(10),
        )
        .map_err(|err| {
            format!("Kafka topic {STORAGE_FINALIZED_TOPIC} metadata was not visible: {err}")
        })?;
    Ok(())
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
fn is_storage_finalized_topic_missing_error(err: &rdkafka::error::KafkaError) -> bool {
    let text = err.to_string();
    text.contains("UnknownTopicOrPartition")
        || text.contains("Broker: Unknown topic or partition")
        || text.contains("unknown topic or partition")
}

#[cfg(feature = "kafka")]
impl AssetServiceImpl {
    /// Spawn the storage→asset auto-trigger: a background Kafka consumer on
    /// `udb.storage.file.finalized.v1` that, per finalized file, registers the
    /// asset and starts the matching pipeline via [`handle_storage_finalized`]
    /// (idempotent). Offsets are committed only after successful handling, so
    /// backlog is replayed at-least-once across restarts. Best-effort — a
    /// consumer error logs; the broker keeps running.
    ///
    /// Lifecycle (P6.4 decision): this runs **per node**, intentionally — every
    /// replica joins the **shared** Kafka consumer group
    /// `udb-asset-storage-finalized-trigger`, so the group coordinator
    /// distributes partitions across replicas and each message is delivered to
    /// exactly one consumer. This is NOT the leader-elected `NativeWorkerHost`
    /// pattern and must NOT be converted to it: a singleton lease would collapse
    /// every partition onto one node and forfeit horizontal consume throughput.
    /// At-least-once redelivery on rebalance is made safe by
    /// [`handle_storage_finalized`]'s idempotency, not by single-ownership.
    pub(crate) fn spawn_storage_finalized_consumer(self: std::sync::Arc<Self>, brokers: String) {
        tokio::spawn(async move {
            use rdkafka::Message;
            use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
            if let Err(err) = ensure_storage_finalized_topic(&brokers).await {
                tracing::warn!(
                    error = %err,
                    topic = STORAGE_FINALIZED_TOPIC,
                    "asset storage-finalized consumer: topic preflight failed; consumer will retry metadata"
                );
            }
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
            // Collapse the recv-error WARN so an unreachable broker can't flood
            // the log; the per-error backoff below prevents a tight spin.
            let recv_error_gate = crate::runtime::executor_utils::LogRateGate::new(
                std::time::Duration::from_secs(STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS),
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
                        if is_storage_finalized_topic_missing_error(&err) {
                            tracing::debug!(
                                error = %err,
                                topic = STORAGE_FINALIZED_TOPIC,
                                "asset storage-finalized consumer: topic not visible yet"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            continue;
                        }
                        if let Some(suppressed) = recv_error_gate.check() {
                            if suppressed > 0 {
                                tracing::warn!(
                                    error = %err,
                                    suppressed,
                                    "asset storage-finalized consumer recv error ({suppressed} more \
                                     suppressed since last log; is Kafka reachable?)"
                                );
                            } else {
                                tracing::warn!(error = %err, "asset storage-finalized consumer recv error");
                            }
                        }
                        // Back off so a persistently unreachable broker can't spin
                        // this loop at full speed.
                        tokio::time::sleep(std::time::Duration::from_secs(
                            STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                        ))
                        .await;
                    }
                }
            }
        });
    }
}

// ── Kafka-triggered pipelines (master-plan 5.2) ──────────────────────────────
//
// The static storage-finalized consumer above is hardcoded to one topic and runs
// per-node. Operator-defined `trigger_topic`s are dynamic, so a leader-elected
// manager reconciles the distinct active trigger topics and runs exactly one
// consumer per topic cluster-wide (start on add, stop on remove). The per-topic
// consumer GENERALIZES `spawn_storage_finalized_consumer`: same
// `enable.auto.commit=false` + commit-after-success gate, keyed by topic and
// routed through `handle_trigger_event` instead of the hardcoded
// `STORAGE_FINALIZED_TOPIC` / `handle_storage_finalized`.

/// Start/stop delta for the trigger-topic consumer set.
#[cfg(any(feature = "kafka", test))]
#[allow(dead_code)] // alive via tests + the (serve-wired) manager loop
#[derive(Debug, Default, PartialEq, Eq)]
struct TriggerReconcile {
    to_start: Vec<String>,
    to_stop: Vec<String>,
}

/// Pure reconcile of the trigger-topic consumer set: given the topics that
/// currently have a running consumer and the desired set (distinct active
/// `trigger_topic`s), return which topics to START (desired ∖ running) and which
/// to STOP (running ∖ desired). Topic-keyed, so the manager owns at most one
/// consumer per topic. Side-effect free + deterministic (BTreeSet ordering) so it
/// is unit-tested without Kafka.
#[cfg(any(feature = "kafka", test))]
#[allow(dead_code)] // alive via tests + the (serve-wired) manager loop
fn reconcile_trigger_topics(
    running: &std::collections::BTreeSet<String>,
    desired: &std::collections::BTreeSet<String>,
) -> TriggerReconcile {
    TriggerReconcile {
        to_start: desired.difference(running).cloned().collect(),
        to_stop: running.difference(desired).cloned().collect(),
    }
}

/// Shared consumer-group prefix for trigger-topic consumers; the topic is appended
/// so each topic gets its own group (offsets tracked per topic). Dot-delimited per
/// the Kafka topic-naming policy.
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
fn trigger_consumer_group_id(topic: &str) -> String {
    format!("udb-asset-trigger.{topic}")
}

/// Per-topic trigger consumer config. Mirrors `storage_finalized_consumer_config`:
/// `enable.auto.commit=false` (offsets committed only after the handler succeeds)
/// and `auto.offset.reset=earliest` (replay backlog), keyed by `topic`.
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
fn trigger_consumer_config(brokers: &str, topic: &str) -> rdkafka::ClientConfig {
    let mut config = rdkafka::ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set("group.id", trigger_consumer_group_id(topic))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    config
}

/// Fail-closed existence check for a `trigger_topic`: returns `true` only when the
/// topic already exists in the cluster. The broker NEVER auto-creates a trigger
/// topic (unlike `ensure_storage_finalized_topic`). Full-cluster metadata is
/// fetched (`topic = None`) precisely so a per-topic metadata probe can't trigger
/// broker-side `auto.create.topics.enable`; a topic we don't see is treated as
/// absent (fail closed).
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
async fn trigger_topic_exists(brokers: &str, topic: &str) -> Result<bool, String> {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", trigger_consumer_group_id(topic))
        .create()
        .map_err(|err| format!("create Kafka client for topic check failed: {err}"))?;
    let metadata = consumer
        .fetch_metadata(None, std::time::Duration::from_secs(10))
        .map_err(|err| format!("fetch Kafka metadata failed: {err}"))?;
    Ok(metadata
        .topics()
        .iter()
        .any(|t| t.name() == topic && !t.partitions().is_empty()))
}

/// Owns the per-topic consumer tasks the manager has spawned. Aborting on `Drop`
/// is load-bearing: when the singleton lease is lost the manager-loop future is
/// dropped, and a tokio `JoinHandle` detaches (does NOT abort) on its own drop —
/// so without this guard the consumers would outlive the lease and a new leader
/// would double-spawn them. The guard guarantees consumers stop whenever leadership
/// ends, preserving "exactly one consumer per topic cluster-wide".
#[cfg(feature = "kafka")]
#[allow(dead_code)] // alive once serve() wires spawn_trigger_manager (master-plan 5.2)
struct TriggerConsumers {
    handles: std::collections::BTreeMap<String, tokio::task::JoinHandle<()>>,
}

#[cfg(feature = "kafka")]
impl Drop for TriggerConsumers {
    fn drop(&mut self) {
        for handle in self.handles.values() {
            handle.abort();
        }
    }
}

#[cfg(feature = "kafka")]
// Methods are alive once serve() wires `spawn_trigger_manager` (master-plan 5.2);
// the allow keeps the default build warning-clean until that one-line wiring lands.
#[allow(dead_code)]
impl AssetServiceImpl {
    /// Spawn the leader-elected, reconciling Kafka-trigger manager (master-plan
    /// 5.2). Under the singleton lease [`crate::runtime::singleton::WORKER_ASSET_TRIGGER_MANAGER`]
    /// it periodically reads the distinct active `trigger_topic`s and runs exactly
    /// one consumer per topic cluster-wide, starting consumers for newly-referenced
    /// topics and stopping those no longer referenced. Non-leaders idle. Fails
    /// CLOSED on a missing topic (never auto-creates).
    ///
    /// NOTE for `serve()` — wire this exactly like the other leader-elected workers,
    /// next to the `spawn_storage_finalized_consumer` block:
    ///
    /// ```ignore
    /// #[cfg(feature = "kafka")]
    /// if crate::runtime::cdc::cdc_delivery_enabled() {
    ///     if let Some(brokers) = runtime_config.kafka_brokers.clone() {
    ///         let trigger_runtime = service.runtime.load_full();
    ///         if let Ok(trigger_pool) =
    ///             trigger_runtime.native_store_pool_for_service("asset", true, "")
    ///         {
    ///             let singleton_relation = trigger_runtime.config().cdc.lock_log_relation();
    ///             std::sync::Arc::new(service.build_asset_service())
    ///                 .spawn_trigger_manager(brokers, trigger_pool, singleton_relation);
    ///         }
    ///     }
    /// }
    /// ```
    ///
    /// The `run_while_leader(WORKER_ASSET_TRIGGER_MANAGER, ...)` call lives inside
    /// this fn (below), so `serve()` only needs the spawn above.
    pub(crate) fn spawn_trigger_manager(
        self: std::sync::Arc<Self>,
        brokers: String,
        singleton_pool: sqlx::PgPool,
        singleton_relation: String,
    ) {
        tokio::spawn(async move {
            loop {
                let manager = self.clone();
                let brokers = brokers.clone();
                // Leader-elected: only the lease holder runs the manager loop, so
                // the per-topic consumers exist once cluster-wide (no per-node dup).
                match crate::runtime::singleton::run_while_leader(
                    &singleton_pool,
                    &singleton_relation,
                    crate::runtime::singleton::WORKER_ASSET_TRIGGER_MANAGER,
                    crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                    || async move { manager.run_trigger_manager_loop(brokers).await },
                )
                .await
                {
                    Ok(Some(())) => {}
                    Ok(None) => {
                        tracing::debug!("asset trigger manager idle: singleton lease held by peer")
                    }
                    Err(err) => {
                        // Lease lost (or acquisition failed): the loop future was
                        // dropped, so its TriggerConsumers guard already aborted every
                        // consumer. Back off, then re-contend for leadership.
                        tracing::warn!("asset trigger manager lease ended: {err}")
                    }
                }
                tokio::time::sleep(crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP).await;
            }
        });
    }

    /// The manager loop, run only while this node holds the lease. Reconciles the
    /// running consumer set against the desired trigger-topic set every interval.
    async fn run_trigger_manager_loop(self: std::sync::Arc<Self>, brokers: String) {
        let reconcile_interval = std::time::Duration::from_secs(
            std::env::var("UDB_ASSET_TRIGGER_RECONCILE_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(30),
        );
        // Drop-guard: aborts all consumers if this future is cancelled (lease lost).
        let mut consumers = TriggerConsumers {
            handles: std::collections::BTreeMap::new(),
        };
        let mut ticker = tokio::time::interval(reconcile_interval);
        loop {
            ticker.tick().await;
            // Forget consumers whose task already exited so they can be restarted.
            consumers
                .handles
                .retain(|_topic, handle| !handle.is_finished());
            let desired = match self.distinct_trigger_topics().await {
                Ok(set) => set,
                Err(err) => {
                    tracing::warn!(error = %err, "asset trigger manager: load trigger topics failed");
                    continue;
                }
            };
            let running: std::collections::BTreeSet<String> =
                consumers.handles.keys().cloned().collect();
            let delta = reconcile_trigger_topics(&running, &desired);
            for topic in delta.to_stop {
                if let Some(handle) = consumers.handles.remove(&topic) {
                    handle.abort();
                    tracing::info!(topic = %topic, "asset trigger consumer stopped (definition removed)");
                }
            }
            for topic in delta.to_start {
                // Fail CLOSED: never auto-create a trigger topic. Skip until it
                // exists; the next reconcile retries once an operator creates it.
                match trigger_topic_exists(&brokers, &topic).await {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            topic = %topic,
                            "asset trigger consumer not started: trigger_topic does not exist (no auto-create); will retry"
                        );
                        continue;
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            topic = %topic,
                            "asset trigger consumer not started: topic existence check failed; will retry"
                        );
                        continue;
                    }
                }
                let handle = tokio::spawn(
                    self.clone()
                        .run_trigger_consumer(brokers.clone(), topic.clone()),
                );
                consumers.handles.insert(topic.clone(), handle);
                tracing::info!(topic = %topic, "asset trigger consumer started (definition added)");
            }
        }
    }

    /// Distinct active `trigger_topic`s across all pipeline definitions (the
    /// desired consumer set). Runs cluster-wide on the asset native pool, mirroring
    /// the scheduler/webhook leader workers' cross-tenant reads.
    async fn distinct_trigger_topics(&self) -> Result<std::collections::BTreeSet<String>, Status> {
        let pool = self.require_pool()?;
        let dm = pipeline_definition_model();
        let rows: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT {tt} FROM {rel} \
             WHERE {status} = 'ACTIVE' AND {tt} IS NOT NULL AND {tt} <> ''",
            tt = dm.q("trigger_topic"),
            rel = dm.relation,
            status = dm.q("status"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|e| {
            asset_internal_status(
                "load_trigger_topics",
                format!("load trigger topics failed: {e}"),
            )
        })?;
        Ok(rows
            .into_iter()
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .collect())
    }

    /// Run one trigger-topic consumer until the task is aborted (the manager owns
    /// the abort handle). GENERALIZES [`Self::spawn_storage_finalized_consumer`]:
    /// identical `enable.auto.commit=false` + commit-after-success posture and the
    /// same [`storage_finalized_payload_ids`] envelope, but keyed by `topic` and
    /// routed through [`Self::handle_trigger_event`].
    async fn run_trigger_consumer(self: std::sync::Arc<Self>, brokers: String, topic: String) {
        use rdkafka::Message;
        use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
        let config = trigger_consumer_config(&brokers, &topic);
        let consumer: StreamConsumer = match config.create() {
            Ok(c) => c,
            Err(err) => {
                tracing::error!(error = %err, topic = %topic, "asset trigger consumer: create failed");
                return;
            }
        };
        if let Err(err) = consumer.subscribe(&[topic.as_str()]) {
            tracing::error!(error = %err, topic = %topic, "asset trigger consumer: subscribe failed");
            return;
        }
        tracing::info!(topic = %topic, "asset trigger pipeline consumer started");
        let recv_error_gate = crate::runtime::executor_utils::LogRateGate::new(
            std::time::Duration::from_secs(STORAGE_FINALIZED_RECV_ERROR_LOG_COOLDOWN_SECS),
        );
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let Some(bytes) = msg.payload() else {
                        tracing::warn!(topic = %topic, "asset trigger consumer: message missing payload");
                        continue;
                    };
                    let Some((tenant_id, file_id)) = storage_finalized_payload_ids(bytes) else {
                        tracing::warn!(
                            topic = msg.topic(),
                            partition = msg.partition(),
                            offset = msg.offset(),
                            "asset trigger consumer: invalid envelope"
                        );
                        continue;
                    };
                    let msg_topic = msg.topic().to_string();
                    let partition = msg.partition();
                    let offset = msg.offset();
                    let result = self
                        .handle_trigger_event(&topic, &file_id, &tenant_id)
                        .await;
                    // Same should_commit gate as the storage-finalized consumer:
                    // commit only after the handler succeeds (at-least-once replay).
                    if should_commit_storage_finalized_offset(&result) {
                        match storage_finalized_commit_offsets(&msg_topic, partition, offset) {
                            Ok(offsets) => {
                                if let Err(err) = consumer.commit(&offsets, CommitMode::Async) {
                                    tracing::warn!(
                                        error = %err,
                                        topic = %msg_topic,
                                        partition,
                                        offset,
                                        "asset trigger consumer commit failed"
                                    );
                                }
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    topic = %msg_topic,
                                    partition,
                                    offset,
                                    "asset trigger consumer commit offset build failed"
                                );
                            }
                        }
                    }
                    if let Err(err) = result {
                        tracing::warn!(
                            error = %err,
                            file_id = %file_id,
                            topic = %topic,
                            "asset trigger pipeline failed"
                        );
                    }
                }
                Err(err) => {
                    if is_storage_finalized_topic_missing_error(&err) {
                        tracing::debug!(error = %err, topic = %topic, "asset trigger consumer: topic not visible yet");
                        tokio::time::sleep(std::time::Duration::from_secs(
                            STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                        ))
                        .await;
                        continue;
                    }
                    if let Some(suppressed) = recv_error_gate.check() {
                        if suppressed > 0 {
                            tracing::warn!(
                                error = %err,
                                suppressed,
                                topic = %topic,
                                "asset trigger consumer recv error ({suppressed} more suppressed since last log; is Kafka reachable?)"
                            );
                        } else {
                            tracing::warn!(error = %err, topic = %topic, "asset trigger consumer recv error");
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(
                        STORAGE_FINALIZED_RECV_ERROR_BACKOFF_SECS,
                    ))
                    .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod trigger_reconcile_tests {
    use super::*;
    use std::collections::BTreeSet;

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn reconcile_starts_new_and_stops_removed_topics() {
        let running = set(&["a", "b", "c"]);
        let desired = set(&["b", "c", "d"]);
        let delta = reconcile_trigger_topics(&running, &desired);
        assert_eq!(delta.to_start, vec!["d".to_string()]);
        assert_eq!(delta.to_stop, vec!["a".to_string()]);
    }

    #[test]
    fn reconcile_first_start_from_empty() {
        let running = BTreeSet::new();
        let desired = set(&["t1", "t2"]);
        let delta = reconcile_trigger_topics(&running, &desired);
        assert_eq!(delta.to_start, vec!["t1".to_string(), "t2".to_string()]);
        assert!(delta.to_stop.is_empty());
    }

    #[test]
    fn reconcile_noop_when_sets_match() {
        let s = set(&["x", "y"]);
        let delta = reconcile_trigger_topics(&s, &s);
        assert!(delta.to_start.is_empty());
        assert!(delta.to_stop.is_empty());
    }

    #[test]
    fn reconcile_full_teardown_when_desired_empty() {
        let running = set(&["a", "b"]);
        let desired = BTreeSet::new();
        let delta = reconcile_trigger_topics(&running, &desired);
        assert!(delta.to_start.is_empty());
        assert_eq!(delta.to_stop, vec!["a".to_string(), "b".to_string()]);
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
            asset_internal_status("handle_storage_finalized", "handler failed")
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
