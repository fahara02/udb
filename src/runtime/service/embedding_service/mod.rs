//! Native `EmbeddingService` (master-plan 9.11) — the AI data plane.
//!
//! Mirrors `lock_service`/`search_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. The durable, tenant-scoped `udb_embedding.embedding_sources`
//! row records which source entity is vector-indexed on change, the text field(s)
//! to embed, the target vector collection, the non-secret model id the sidecar
//! must use, and — critically — the SOURCE table's resolved tenant column. A source
//! is registered ONLY when that column resolves, so every reported vector and every
//! `Retrieve` has a server-side tenant predicate (fail closed).
//!
//! ARCHITECTURE GUARD (9.11): **inference runs in SIDECARS ONLY.** No embedding
//! model is ever linked into the broker. On a source row change (and on `Backfill`)
//! the broker emits a `udb.embedding.work.v1` event carrying ONLY the row primary
//! key + extracted text + non-secret routing ({tenant_id, source, row_pk, text,
//! model_id, target_collection}) — NEVER a credential / API key (those live only in
//! the sidecar). A sidecar computes the embedding and returns it via the
//! internal-only `ReportEmbedding` callback, which upserts the vector through the
//! SAME runtime vector seam the asset service uses
//! ([`DataBrokerRuntime::vector_upsert_backend_target`] /
//! `vector_delete_backend_target`, the implementation behind
//! `asset_service::AssetServiceImpl::{upsert_embedding,delete_embedding}`) — never a
//! second vector-upsert. Every reported vector is tagged with the verified-claim
//! tenant; a vector with no/foreign tenant is rejected (no fail-open).
//!
//! `Retrieve` is deadline-bounded and DELEGATES to the SearchService (9.5) hybrid
//! seam ([`DataBrokerRuntime::vector_hybrid_search`] / `vector_search`) with a
//! server-side tenant filter injected from the verified claim — never a raw engine
//! query (the batch-RPC authz-bypass lesson).

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use chrono::Utc;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::proto::udb::core::embedding::services::v1::embedding_service_server::EmbeddingService;
use crate::proto::{
    SelectRequest, Sort, VectorHybridSearchRequest, VectorPointMutation, VectorSearchRequest,
    VectorSet,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::CatalogManager;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::embedding::services::v1::embedding_service_server::EmbeddingServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token, native_offset_page_window, native_service_context, non_empty_json,
    validate_request_tenant,
};

const EMBEDDING_SOURCE_MSG: &str = "udb.core.embedding.entity.v1.EmbeddingSource";

/// The change-driven embedding WORK topic the sidecar pool consumes. Its payload
/// carries ONLY the row pk + text + non-secret routing — never any credential.
const TOPIC_WORK: &str = "udb.embedding.work.v1";
const TOPIC_SOURCE_REGISTERED: &str = "udb.embedding.source.registered.v1";
const TOPIC_SOURCE_DELETED: &str = "udb.embedding.source.deleted.v1";
const TOPIC_BACKFILL_REQUESTED: &str = "udb.embedding.backfill.requested.v1";
const TOPIC_BACKFILL_COMPLETED: &str = "udb.embedding.backfill.completed.v1";
/// Completion marker for a deleted source's vector teardown, keyed by
/// `teardown_event_id` (the journal event id of the source-deleted event) so the
/// leader pass never re-runs a finished teardown (mirrors the backfill
/// requested/completed pairing).
const TOPIC_SOURCE_TEARDOWN_COMPLETED: &str = "udb.embedding.source.teardown.completed.v1";

const STATUS_ACTIVE: &str = "ACTIVE";
const STATUS_DELETED: &str = "DELETED";
pub(crate) const EMBEDDING_WORK_EMITTER_BATCH: i64 = 200;
/// Page size for enumerating (and deleting) a deleted source's point ids during
/// vector teardown — bounds each journal scan and each vector-seam delete call.
const EMBEDDING_TEARDOWN_DELETE_BATCH: i64 = 200;
const EMBEDDING_BACKFILL_PAGE_LIMIT: i32 = 200;
const DEFAULT_EMBEDDING_WORK_EMITTER_INTERVAL_SECS: u64 = 30;
const EMBEDDING_WORK_EMITTER_INTERVAL_ENV: &str = "UDB_EMBEDDING_WORK_EMITTER_INTERVAL_SECS";

/// Minimum similarity score a vector hit must clear to be returned by `Retrieve`.
/// Operator-tunable (was a hardcoded `0.0`); resolved once via a `OnceLock`. `0.0`
/// keeps the historical "return everything the engine ranks" behavior by default.
/// A `RetrieveRequest.score_threshold` proto field (Part B.2.3) will later let a
/// caller raise this per query; until then this is the server-side floor.
const DEFAULT_EMBEDDING_RETRIEVE_SCORE_THRESHOLD: f32 = 0.0;
const EMBEDDING_RETRIEVE_SCORE_THRESHOLD_ENV: &str = "UDB_EMBEDDING_RETRIEVE_SCORE_THRESHOLD";
/// Comma-separated `lexical,vector` weights for hybrid-search fusion, e.g.
/// `"0.4,0.6"`. Empty (the default) preserves the delegated engine's built-in
/// fusion weighting (was a hardcoded empty vec).
const EMBEDDING_RETRIEVE_FUSION_WEIGHTS_ENV: &str = "UDB_EMBEDDING_RETRIEVE_FUSION_WEIGHTS";

/// Fallback vector collection when a source row somehow carries no target (mirrors
/// `asset_service::DEFAULT_VECTOR_COLLECTION`; a source normally always specifies
/// its own `target_collection`, validated non-empty at register time).
const DEFAULT_VECTOR_COLLECTION: &str = "udb_asset_embeddings";

/// Default number of hits returned when the caller does not specify `top_k`.
const DEFAULT_TOP_K: i32 = 10;
/// Upper bound on `top_k` so one query cannot pull an unbounded result set.
const MAX_TOP_K: i32 = 200;
/// Per-tenant registered-source budget. Bounds the durable table so one tenant
/// cannot exhaust the shared store; a new source beyond this fails closed.
const MAX_SOURCES_PER_TENANT: usize = 128;

/// Postgres-backed `EmbeddingService` handler.
pub struct EmbeddingServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `embedding`).
    pg_pool: Option<PgPool>,
    /// Runtime handle for mediated source CRUD, the shared vector upsert/delete
    /// seam, and the mediated hybrid-search dispatch `Retrieve` delegates to.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Project-active catalog: resolves the source entity's manifest table (and
    /// thus its tenant column) plus the manifest `Retrieve` hybrid search needs.
    catalog: Option<Arc<CatalogManager>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn embedding_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "embedding",
        operation,
        capability_required,
        message,
    )
}

fn embedding_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

fn embedding_source_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "embedding",
        operation,
        "embedding_source_not_found",
        "embedding source not found",
    )
}

impl EmbeddingServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            catalog: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_catalog(mut self, catalog: Option<Arc<CatalogManager>>) -> Self {
        self.catalog = catalog;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Source state is durable-only: fail closed when no runtime dispatch exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            embedding_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "embedding service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    fn require_catalog(&self) -> Result<&CatalogManager, Status> {
        self.catalog.as_deref().ok_or_else(|| {
            embedding_capability_status(
                "catalog_lookup",
                "active_catalog",
                "embedding service requires the active catalog (no catalog configured)",
            )
        })
    }
}

impl Default for EmbeddingServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

/// Clamp a requested `top_k` into `[1, MAX_TOP_K]`; non-positive → default.
fn resolve_top_k(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_TOP_K
    } else {
        requested.min(MAX_TOP_K)
    }
}

fn embedding_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    embedding_field_violation(field, description, message)
}

fn embedding_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn validate_register_source_required_fields(
    source_name: &str,
    source_message_type: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if source_name.trim().is_empty() {
        fields.push(("source_name", "must be a non-empty embedding source name"));
    }
    if source_message_type.trim().is_empty() {
        fields.push((
            "source_message_type",
            "must be a non-empty source message type",
        ));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_name and source_message_type are required",
            fields,
        ));
    }
    Ok(())
}

fn validate_report_embedding_required_fields(
    source_name: &str,
    row_pk: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if source_name.trim().is_empty() {
        fields.push(("source_name", "must be a non-empty embedding source name"));
    }
    if row_pk.trim().is_empty() {
        fields.push(("row_pk", "must be a non-empty source row primary key"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_name and row_pk are required",
            fields,
        ));
    }
    Ok(())
}

/// Reported-vector shape gate: an empty vector, or a `dims` claim that
/// contradicts the actual vector length, is rejected with an
/// `invalid_argument` field violation BEFORE any admission, store read, or
/// vector-seam call — a mis-declared dimension would otherwise poison the
/// collection's dimension at ensure-time. Pure — unit-tested.
fn validate_reported_vector(dims: i32, vector: &[f32]) -> Result<(), Status> {
    if vector.is_empty() {
        return Err(embedding_required_field(
            "vector",
            "must contain at least one embedding dimension",
            "vector is required",
        ));
    }
    if dims > 0 && dims as usize != vector.len() {
        return Err(embedding_field_violation(
            "dims",
            "must equal the reported vector length when set",
            format!(
                "dims ({dims}) does not match the reported vector length ({})",
                vector.len()
            ),
        ));
    }
    Ok(())
}

/// The fail-closed source-tenant-column gate (mirrors `search_service`). An
/// EmbeddingSource MUST be scoped by the source table's tenant column; if the
/// shared catalog resolver returns none, the source is not tenant-isolated and we
/// refuse to register it (a reported vector / Retrieve would otherwise have no
/// tenant predicate). Pure — unit-tested without a manifest.
fn require_source_tenant_column(
    resolved: Option<String>,
    message_type: &str,
) -> Result<String, Status> {
    match resolved.map(|column| column.trim().to_string()) {
        Some(column) if !column.is_empty() => Ok(column),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "source entity '{message_type}' has no resolvable tenant column; refusing to register a \
                 tenant-scoped embedding source over it (fail closed)"
            ),
            [(
                "source_message_type",
                "must resolve to a tenant-scoped source entity",
            )],
        )),
    }
}

/// Build the `udb.embedding.work.v1` payload the sidecar pool consumes. Pure so
/// the no-credential invariant is unit-asserted: it carries ONLY the row pk +
/// text + non-secret routing. There is NO credential/API-key field here — model
/// credentials live exclusively in the sidecar (architecture guard 9.11).
fn build_work_event_payload(
    tenant_id: &str,
    source_name: &str,
    row_pk: &str,
    text: &str,
    model_id: &str,
    target_collection: &str,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "source": source_name,
        "row_pk": row_pk,
        "text": text,
        "model_id": model_id,
        "target_collection": target_collection,
    })
}

/// Build the tenant-tagged vector point for a reported embedding. Fail CLOSED: an
/// empty (unverified) tenant is rejected so an unscoped vector can never be stored
/// (no fail-open); the stored point carries the `_tenant_id` payload key — the same
/// key the Qdrant IR compiler stamps at write time and the search seam ANDs into
/// its `must` clause — so a later Retrieve's server-side tenant filter matches.
/// Pure — unit-tested without an engine.
fn build_embedding_point(
    row_pk: &str,
    vector: Vec<f32>,
    tenant_id: &str,
) -> Result<VectorPointMutation, Status> {
    let tenant = tenant_id.trim();
    if tenant.is_empty() {
        return Err(embedding_policy_status_with_code(
            tonic::Code::PermissionDenied,
            "embedding_vector_upsert",
            "verified_tenant_required",
            "embedding upsert requires a verified tenant; refusing to store an unscoped vector \
             (no fail-open)",
        ));
    }
    if row_pk.trim().is_empty() {
        return Err(embedding_required_field(
            "row_pk",
            "must be a non-empty source row primary key",
            "row_pk is required",
        ));
    }
    if vector.is_empty() {
        return Err(embedding_required_field(
            "vector",
            "must contain at least one embedding dimension",
            "vector is required",
        ));
    }
    let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
        "_tenant_id": tenant,
    }));
    Ok(VectorPointMutation {
        id: row_pk.to_string(),
        vector,
        payload,
    })
}

/// Serialize a retrieved point's payload for a `RetrieveHit`: the internal
/// `_tenant_id` write-stamp is stripped (it only mirrors the verified claim the
/// caller already holds — never expose the isolation key), and the remaining
/// user payload is serialized as JSON. No payload, a non-object payload, or a
/// tag-only payload all yield an empty string. Pure — unit-tested.
fn retrieve_hit_payload_json(payload: Option<&prost_types::Struct>) -> String {
    let Some(payload) = payload else {
        return String::new();
    };
    let mut json = crate::runtime::executor_utils::struct_to_json(payload);
    let Some(object) = json.as_object_mut() else {
        return String::new();
    };
    object.remove("_tenant_id");
    if object.is_empty() {
        return String::new();
    }
    serde_json::Value::Object(std::mem::take(object)).to_string()
}

/// Parse a gRPC `grpc-timeout` header value (`<digits><unit>`, units
/// H/M/S/m/u/n) into a `Duration`. Returns `None` for a malformed value.
fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.len() < 2 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    let amount: u64 = digits.parse().ok()?;
    let nanos = match unit {
        "H" => amount.checked_mul(3_600_000_000_000)?,
        "M" => amount.checked_mul(60_000_000_000)?,
        "S" => amount.checked_mul(1_000_000_000)?,
        "m" => amount.checked_mul(1_000_000)?,
        "u" => amount.checked_mul(1_000)?,
        "n" => amount,
        _ => return None,
    };
    Some(Duration::from_nanos(nanos))
}

/// Deadline gate for `Retrieve`. Given the optional absolute deadline (derived
/// from the request's gRPC timeout) and now, returns the remaining budget, or
/// `deadline_exceeded` when the deadline is already past. Pure — unit-tested.
fn remaining_before_deadline(
    deadline: Option<Instant>,
    now: Instant,
) -> Result<Option<Duration>, Status> {
    match deadline {
        None => Ok(None),
        Some(deadline) => match deadline.checked_duration_since(now) {
            Some(remaining) if !remaining.is_zero() => Ok(Some(remaining)),
            _ => Err(crate::runtime::executor_utils::deadline_exceeded_status(
                "embedding",
                "retrieve",
                crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                "retrieve deadline exceeded before semantic search dispatch",
            )),
        },
    }
}

/// A registered source decoded from the native read JSON.
pub(crate) struct StoredSource {
    source_id: String,
    tenant_id: String,
    source_name: String,
    source_message_type: String,
    text_fields_json: String,
    target_collection: String,
    model_id: String,
    tenant_column: String,
    source_cdc_topic: String,
    status: String,
}

impl StoredSource {
    /// The effective vector collection (falls back to the asset default only if a
    /// row was persisted without one, which register-time validation prevents).
    fn collection(&self) -> String {
        if self.target_collection.trim().is_empty() {
            DEFAULT_VECTOR_COLLECTION.to_string()
        } else {
            self.target_collection.clone()
        }
    }
}

fn source_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        Some(value @ serde_json::Value::Array(_)) => value.to_string(),
        Some(value @ serde_json::Value::Object(_)) => value.to_string(),
        _ => String::new(),
    }
}

fn stored_source_from_json(row: &serde_json::Value) -> StoredSource {
    let map = source_json_object(row);
    StoredSource {
        source_id: json_str(map, "source_id"),
        tenant_id: json_str(map, "tenant_id"),
        source_name: json_str(map, "source_name"),
        source_message_type: json_str(map, "source_message_type"),
        text_fields_json: json_str(map, "text_fields_json"),
        target_collection: json_str(map, "target_collection"),
        model_id: json_str(map, "model_id"),
        tenant_column: json_str(map, "tenant_column"),
        source_cdc_topic: json_str(map, "source_cdc_topic"),
        status: json_str(map, "status"),
    }
}

fn source_filter(
    tenant_id: &str,
    source_name: Option<&str>,
    status: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(source_name) = source_name {
        filters.push(LogicalFilter::Comparison {
            field: "source_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(source_name),
        });
    }
    if let Some(status) = status {
        filters.push(LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(status),
        });
    }
    LogicalFilter::And(filters)
}

fn source_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "source_id".to_string(),
        "tenant_id".to_string(),
        "source_name".to_string(),
        "source_message_type".to_string(),
        "text_fields_json".to_string(),
        "target_collection".to_string(),
        "model_id".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
    ])
}

fn embedding_source_model() -> NativeModel {
    native_model(
        EMBEDDING_SOURCE_MSG,
        &[
            "source_id",
            "tenant_id",
            "source_name",
            "source_message_type",
            "text_fields_json",
            "target_collection",
            "model_id",
            "tenant_column",
            "source_cdc_topic",
            "status",
        ],
    )
}

pub(crate) fn embedding_work_emitter_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(EMBEDDING_WORK_EMITTER_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_EMBEDDING_WORK_EMITTER_INTERVAL_SECS),
        )
    })
}

/// Server-side minimum-score floor applied to every mediated `Retrieve`. Resolved
/// once (no per-request env read); a non-finite or negative override is ignored so
/// the floor is always a real, sane bound.
fn retrieve_score_threshold() -> f32 {
    static THRESHOLD: OnceLock<f32> = OnceLock::new();
    *THRESHOLD.get_or_init(|| {
        std::env::var(EMBEDDING_RETRIEVE_SCORE_THRESHOLD_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<f32>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(DEFAULT_EMBEDDING_RETRIEVE_SCORE_THRESHOLD)
    })
}

/// Hybrid-search fusion weights, parsed once from a comma-separated list. Empty
/// (the default, and the fallback for any malformed/negative entry) hands fusion
/// weighting back to the delegated engine — identical to the previous hardcoded
/// `Vec::new()`.
fn retrieve_fusion_weights() -> Vec<f32> {
    static WEIGHTS: OnceLock<Vec<f32>> = OnceLock::new();
    WEIGHTS
        .get_or_init(|| {
            let Ok(raw) = std::env::var(EMBEDDING_RETRIEVE_FUSION_WEIGHTS_ENV) else {
                return Vec::new();
            };
            let parsed: Option<Vec<f32>> = raw
                .split(',')
                .map(|part| {
                    part.trim()
                        .parse::<f32>()
                        .ok()
                        .filter(|v| v.is_finite() && *v >= 0.0)
                })
                .collect();
            // ALL entries must parse to a valid non-negative weight, else fall back
            // to engine-default fusion rather than apply a half-parsed weighting.
            parsed
                .filter(|weights| !weights.is_empty())
                .unwrap_or_default()
        })
        .clone()
}

fn source_read_by_name(tenant_id: &str, source_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: EMBEDDING_SOURCE_MSG.to_string(),
        filter: Some(source_filter(tenant_id, Some(source_name), None)),
        projection: Some(source_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn active_sources_read(tenant_id: &str, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: EMBEDDING_SOURCE_MSG.to_string(),
        filter: Some(source_filter(tenant_id, None, Some(STATUS_ACTIVE))),
        projection: Some(source_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
fn source_record(
    source_id: &str,
    tenant_id: &str,
    source_name: &str,
    source_message_type: &str,
    text_fields_json: &str,
    target_collection: &str,
    model_id: &str,
    tenant_column: &str,
    source_cdc_topic: &str,
    status: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("source_id".to_string(), logical_string(source_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("source_name".to_string(), logical_string(source_name));
    record.insert(
        "source_message_type".to_string(),
        logical_string(source_message_type),
    );
    record.insert(
        "text_fields_json".to_string(),
        logical_string(text_fields_json),
    );
    record.insert(
        "target_collection".to_string(),
        logical_string(target_collection),
    );
    record.insert("model_id".to_string(), logical_string(model_id));
    record.insert("tenant_column".to_string(), logical_string(tenant_column));
    record.insert(
        "source_cdc_topic".to_string(),
        logical_string(source_cdc_topic),
    );
    record.insert("status".to_string(), logical_string(status));
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// Mutable columns an upsert may overwrite on re-register / status change. The
/// conflict target is the message primary key (`source_id`), reused for an
/// existing (tenant, source_name) row so a re-register never violates the unique
/// index.
fn source_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "source_message_type".to_string(),
        "text_fields_json".to_string(),
        "target_collection".to_string(),
        "model_id".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
        "updated_at".to_string(),
        "metadata_json".to_string(),
    ])
}

impl EmbeddingServiceImpl {
    /// Resolve the SOURCE entity's tenant column through the project-active
    /// manifest using the SHARED catalog resolver (no duplicate). Returns the
    /// column name + the source CDC topic, or a fail-closed error when the source
    /// is unknown or not tenant-isolated. Mirrors `search_service`.
    fn resolve_source_tenant_column(
        &self,
        project_id: &str,
        source_message_type: &str,
    ) -> Result<(String, String), Status> {
        let catalog = self.require_catalog()?;
        let state = catalog.active_for(project_id);
        let manifest = &state.manifest;
        let table = crate::broker::table_for_message(manifest, source_message_type).ok_or_else(
            || {
                embedding_field_violation(
                    "source_message_type",
                    "must be present in the active catalog manifest",
                    format!(
                        "source_message_type '{source_message_type}' is not present in the active catalog \
                         manifest"
                    ),
                )
            },
        )?;
        // SHARED resolver: same `util::resolve_tenant_column` family behind
        // `native_catalog::NativeModel::tenant_column`.
        let resolved = crate::runtime::postgres_helpers::tenant_column_ref(table)
            .map(|column| column.column_name.clone());
        let tenant_column = require_source_tenant_column(resolved, source_message_type)?;
        let cdc_topic = table.cdc_topic.clone();
        Ok((tenant_column, cdc_topic))
    }

    /// Emit a per-mutation versioned dot-topic control event (best-effort).
    async fn emit_source_event(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        extra: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = serde_json::json!({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "source": source_name,
        });
        if let (Some(object), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
            for (key, value) in extra {
                object.insert(key.clone(), value.clone());
            }
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            source_name,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    /// Emit a `udb.embedding.work.v1` event for one source row (the seam the CDC
    /// change handler and the leader-spawned backfill worker both call). The
    /// payload is the no-credential [`build_work_event_payload`]; the partition key
    /// is the row pk so a sidecar pool fans out by row.
    #[allow(dead_code)] // called by the injected-event test seam `run_embedding_work_emitter`
    async fn emit_work_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
    ) {
        self.emit_work_event_with_source_event(
            tenant_id,
            project_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
            None,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_backfill_work_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
        backfill_event_id: &str,
        backfill_id: &str,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = build_work_event_payload(
            tenant_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
        );
        if let Some(object) = payload.as_object_mut() {
            object.insert(
                "backfill_event_id".to_string(),
                serde_json::Value::String(backfill_event_id.to_string()),
            );
            object.insert(
                "backfill_id".to_string(),
                serde_json::Value::String(backfill_id.to_string()),
            );
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_WORK,
            row_pk,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: "embedding.backfill.work.emit".to_string(),
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_work_event_with_source_event(
        &self,
        tenant_id: &str,
        project_id: &str,
        source_name: &str,
        row_pk: &str,
        text: &str,
        model_id: &str,
        target_collection: &str,
        source_event_id: Option<&str>,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = build_work_event_payload(
            tenant_id,
            source_name,
            row_pk,
            text,
            model_id,
            target_collection,
        );
        if let (Some(event_id), Some(object)) = (source_event_id, payload.as_object_mut()) {
            object.insert(
                "source_event_id".to_string(),
                serde_json::Value::String(event_id.to_string()),
            );
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_WORK,
            row_pk,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                operation: "embedding.work.emit".to_string(),
                target_resource: source_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    /// Upsert a reported embedding through the SHARED runtime vector seam — the
    /// exact path `asset_service::AssetServiceImpl::upsert_embedding` wraps
    /// ([`choose_instance_name_for_project`] + `vector_upsert_backend_target`) — so
    /// there is no second vector-upsert. The point is tenant-tagged; an empty tenant
    /// fails closed before any backend call (no fail-open).
    async fn upsert_reported_embedding(
        &self,
        project_id: &str,
        collection: &str,
        tenant_id: &str,
        row_pk: &str,
        vector: Vec<f32>,
        dims: i32,
    ) -> Result<(), Status> {
        // Dimension truth gate: empty vectors and a `dims` claim contradicting
        // the actual vector length are rejected before any backend call.
        validate_reported_vector(dims, &vector)?;
        let runtime = self.require_runtime()?;
        let dim = if dims > 0 { dims } else { vector.len() as i32 };
        // Fail closed + tenant-tag (mirrors search's `_tenant_id` write stamp).
        let point = build_embedding_point(row_pk, vector, tenant_id)?;
        let vector_instance = runtime
            .choose_instance_name_for_project("qdrant", true, project_id)
            .map(str::to_string)
            .unwrap_or_else(|| "default".to_string());
        runtime
            .vector_upsert_backend_target(
                Some(&vector_instance),
                project_id,
                collection,
                dim,
                vec![point],
            )
            .await
    }

    /// Batch point deletion through the SHARED vector seam — the exact path
    /// `asset_service::delete_embedding` wraps (`vector_delete_backend_target`),
    /// never a second vector client. Errors are RETURNED (not swallowed) so the
    /// source-teardown pass can withhold its completion marker and retry;
    /// a missing runtime fails closed with a typed capability detail.
    async fn delete_embedding_points(
        &self,
        project_id: &str,
        collection: &str,
        point_ids: Vec<String>,
    ) -> Result<(), Status> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Err(embedding_capability_status(
                "embedding_vector_delete",
                "runtime_vector_seam",
                "embedding vector delete requires the runtime vector seam (no runtime configured)",
            ));
        };
        if point_ids.is_empty() {
            return Ok(());
        }
        runtime
            .vector_delete_backend_target(None, project_id, collection, point_ids)
            .await
    }

    /// Best-effort: remove a source row's embedding (point id = row pk) through the
    /// SHARED vector seam — the path `asset_service::delete_embedding` wraps. Used
    /// by the leader work-emitter pass on a row delete so a deleted row leaves no
    /// orphan vector. Never fails the caller (logs and continues).
    async fn delete_embedding_point(&self, project_id: &str, collection: &str, row_pk: &str) {
        if row_pk.trim().is_empty() {
            return;
        }
        if let Err(err) = self
            .delete_embedding_points(project_id, collection, vec![row_pk.to_string()])
            .await
        {
            tracing::warn!(error = %err, collection, row_pk, "embedding vector delete failed");
        }
    }
}

#[tonic::async_trait]
impl EmbeddingService for EmbeddingServiceImpl {
    async fn register_source(
        &self,
        request: Request<embedding_pb::RegisterSourceRequest>,
    ) -> Result<Response<embedding_pb::RegisterSourceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header before any catalog/store access.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let source_name = req.source_name.trim().to_string();
        let source_message_type = req.source_message_type.trim().to_string();
        let target_collection = req.target_collection.trim().to_string();
        validate_register_source_required_fields(&source_name, &source_message_type)?;
        if target_collection.is_empty() {
            return Err(embedding_required_field(
                "target_collection",
                "must be a non-empty target vector collection",
                "target_collection is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        // FAIL CLOSED: resolve the source table's tenant column via the shared
        // catalog resolver. No tenant column ⇒ no source registration.
        let (tenant_column, source_cdc_topic) =
            self.resolve_source_tenant_column(&context.project_id, &source_message_type)?;

        // Per-tenant source quota (only a brand-new source counts).
        let existing = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                source_read_by_name(&tenant_id, &source_name),
            )
            .await?
            .first()
            .map(stored_source_from_json);
        if existing.is_none() {
            let active = runtime
                .native_entity_read_for_service(
                    "embedding",
                    &context,
                    active_sources_read(&tenant_id, 0, (MAX_SOURCES_PER_TENANT as u32) + 1),
                )
                .await?;
            if active.len() >= MAX_SOURCES_PER_TENANT {
                return Err(crate::runtime::executor_utils::quota_refusal_status(
                    "embedding",
                    "tenant embedding-source quota",
                    format!("tenant embedding-source quota exhausted ({MAX_SOURCES_PER_TENANT})"),
                ));
            }
        }

        let source_id = existing
            .as_ref()
            .map(|row| row.source_id.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        // Persist the text-field list as a JSON array (trimmed, non-empty).
        let text_fields: Vec<String> = req
            .text_fields
            .iter()
            .map(|field| field.trim().to_string())
            .filter(|field| !field.is_empty())
            .collect();
        let text_fields_json =
            serde_json::to_string(&text_fields).unwrap_or_else(|_| "[]".to_string());
        let model_id = req.model_id.trim().to_string();
        let metadata_json = non_empty_json(&req.metadata_json);

        runtime
            .native_entity_write_for_service(
                "embedding",
                &context,
                EMBEDDING_SOURCE_MSG,
                source_record(
                    &source_id,
                    &tenant_id,
                    &source_name,
                    &source_message_type,
                    &text_fields_json,
                    &target_collection,
                    &model_id,
                    &tenant_column,
                    &source_cdc_topic,
                    STATUS_ACTIVE,
                    &metadata_json,
                ),
                source_conflict(),
            )
            .await?;

        self.emit_source_event(
            TOPIC_SOURCE_REGISTERED,
            &tenant_id,
            &context.project_id,
            &source_name,
            serde_json::json!({
                "source_message_type": source_message_type,
                "target_collection": target_collection,
                "model_id": model_id,
                "tenant_column": tenant_column,
            }),
        )
        .await;

        // Reindex-on-model-change guard (Part B.1.3): a re-register that switches
        // the model or the target collection would otherwise leave the collection
        // silently MIXING vectors from the old model/dims with newly-reported ones
        // — corrupting retrieval (different models embed into incomparable spaces).
        // Auto-enqueue a backfill using the SAME control event the Backfill RPC
        // emits (the leader work-emitter re-embeds every existing row under the new
        // binding). A brand-new source, or a re-register that changes neither the
        // model nor the collection, needs no reindex.
        if let Some(prev) = existing.as_ref() {
            let model_changed = prev.model_id.trim() != model_id;
            let collection_changed = prev.target_collection.trim() != target_collection;
            if model_changed || collection_changed {
                let reindex_id = Uuid::new_v4().to_string();
                self.emit_source_event(
                    TOPIC_BACKFILL_REQUESTED,
                    &tenant_id,
                    &context.project_id,
                    &source_name,
                    serde_json::json!({
                        "backfill_id": reindex_id,
                        "source_message_type": source_message_type,
                        "target_collection": target_collection,
                        "model_id": model_id,
                        "reason": "model_or_collection_changed",
                        "previous_model_id": prev.model_id,
                        "previous_collection": prev.target_collection,
                    }),
                )
                .await;
            }
        }

        Ok(Response::new(embedding_pb::RegisterSourceResponse {
            source_id,
            source_name,
            tenant_column,
            message: "embedding source registered".to_string(),
            error: None,
        }))
    }

    async fn list_sources(
        &self,
        request: Request<embedding_pb::ListSourcesRequest>,
    ) -> Result<Response<embedding_pb::ListSourcesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        // READ_ONLY RPC ⇒ the shared Read admission lane (like sibling list
        // RPCs), not Admin — listing must never contend with control mutations.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let page_window = native_offset_page_window(
            1,
            req.page_size,
            &req.page_token,
            MAX_SOURCES_PER_TENANT as i32,
        );

        let rows = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                active_sources_read(
                    &tenant_id,
                    page_window.offset as u64,
                    (page_window.limit as u32).min(MAX_SOURCES_PER_TENANT as u32),
                ),
            )
            .await?;
        let sources = rows
            .iter()
            .map(stored_source_from_json)
            .map(|source| embedding_pb::EmbeddingSourceSummary {
                source_id: source.source_id,
                source_name: source.source_name,
                source_message_type: source.source_message_type,
                target_collection: source.target_collection,
                model_id: source.model_id,
                status: source.status,
            })
            .collect::<Vec<_>>();
        let next_page_token =
            native_next_page_token(page_window.offset, page_window.limit, sources.len());

        Ok(Response::new(embedding_pb::ListSourcesResponse {
            sources,
            message: "ok".to_string(),
            error: None,
            next_page_token,
        }))
    }

    async fn delete_source(
        &self,
        request: Request<embedding_pb::DeleteSourceRequest>,
    ) -> Result<Response<embedding_pb::DeleteSourceResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let source_name = req.source_name.trim().to_string();
        if source_name.is_empty() {
            return Err(embedding_required_field(
                "source_name",
                "must be a non-empty embedding source name",
                "source_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let stored = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                source_read_by_name(&tenant_id, &source_name),
            )
            .await?
            .first()
            .map(stored_source_from_json);
        let Some(stored) = stored.filter(|row| row.status != STATUS_DELETED) else {
            // Idempotent: nothing to delete.
            return Ok(Response::new(embedding_pb::DeleteSourceResponse {
                deleted: true,
                message: "embedding source not found".to_string(),
                error: None,
            }));
        };

        runtime
            .native_entity_write_for_service(
                "embedding",
                &context,
                EMBEDDING_SOURCE_MSG,
                source_record(
                    &stored.source_id,
                    &tenant_id,
                    &stored.source_name,
                    &stored.source_message_type,
                    &stored.text_fields_json,
                    &stored.target_collection,
                    &stored.model_id,
                    &stored.tenant_column,
                    &stored.source_cdc_topic,
                    STATUS_DELETED,
                    "{}",
                ),
                source_conflict(),
            )
            .await?;

        // Vector teardown runs on the leader work-emitter pass: it consumes this
        // source-deleted event from the durable journal, enumerates the source's
        // emitted work-event point ids, and deletes them per collection through
        // the shared vector seam, then marks the event done with a
        // `teardown.completed` marker (see `process_embedding_teardown_job`).
        self.emit_source_event(
            TOPIC_SOURCE_DELETED,
            &tenant_id,
            &context.project_id,
            &source_name,
            serde_json::json!({ "target_collection": stored.target_collection }),
        )
        .await;

        Ok(Response::new(embedding_pb::DeleteSourceResponse {
            deleted: true,
            message: "embedding source deleted".to_string(),
            error: None,
        }))
    }

    async fn backfill(
        &self,
        request: Request<embedding_pb::BackfillRequest>,
    ) -> Result<Response<embedding_pb::BackfillResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let source_name = req.source_name.trim().to_string();
        if source_name.is_empty() {
            return Err(embedding_required_field(
                "source_name",
                "must be a non-empty embedding source name",
                "source_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let stored = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                source_read_by_name(&tenant_id, &source_name),
            )
            .await?
            .first()
            .map(stored_source_from_json)
            .filter(|source| source.status == STATUS_ACTIVE)
            .ok_or_else(|| embedding_source_not_found_status("backfill"))?;

        // Emit only the backfill-request control event from the admitting RPC. The
        // leader-spawned work emitter enumerates the source's existing rows through
        // the served DataBroker SELECT path and emits per-row work out-of-band.
        let backfill_id = Uuid::new_v4().to_string();
        self.emit_source_event(
            TOPIC_BACKFILL_REQUESTED,
            &tenant_id,
            &context.project_id,
            &source_name,
            serde_json::json!({
                "backfill_id": backfill_id,
                "source_message_type": stored.source_message_type,
                "target_collection": stored.target_collection,
                "model_id": stored.model_id,
            }),
        )
        .await;

        Ok(Response::new(embedding_pb::BackfillResponse {
            backfill_id,
            accepted: true,
            message: "embedding backfill requested".to_string(),
            error: None,
        }))
    }

    async fn report_embedding(
        &self,
        request: Request<embedding_pb::ReportEmbeddingRequest>,
    ) -> Result<Response<embedding_pb::ReportEmbeddingResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the VERIFIED
        // claim/header. After this passes the body value IS the verified tenant, so
        // the stored vector is tagged from the verified claim, never raw body.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let source_name = req.source_name.trim().to_string();
        let row_pk = req.row_pk.trim().to_string();
        validate_report_embedding_required_fields(&source_name, &row_pk)?;
        // Dimension truth: empty vector / contradictory `dims` rejected up front.
        validate_reported_vector(req.dims, &req.vector)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Vector,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        // Resolve the source the sidecar reported against — bounded to THIS tenant
        // (the read filters on the verified tenant), so a foreign source cannot be
        // targeted. An unknown/deleted source is rejected (no blind upsert).
        let stored = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                source_read_by_name(&tenant_id, &source_name),
            )
            .await?
            .first()
            .map(stored_source_from_json)
            .filter(|source| source.status == STATUS_ACTIVE)
            .ok_or_else(|| embedding_source_not_found_status("report_embedding"))?;

        self.upsert_reported_embedding(
            &context.project_id,
            &stored.collection(),
            &tenant_id,
            &row_pk,
            req.vector,
            req.dims,
        )
        .await?;

        Ok(Response::new(embedding_pb::ReportEmbeddingResponse {
            upserted: true,
            message: "embedding upserted".to_string(),
            error: None,
        }))
    }

    async fn retrieve(
        &self,
        request: Request<embedding_pb::RetrieveRequest>,
    ) -> Result<Response<embedding_pb::RetrieveResponse>, Status> {
        let metadata = request.metadata().clone();
        // Derive the absolute deadline from the request's gRPC timeout BEFORE any
        // work, so the budget covers the whole semantic-search delegation.
        let deadline = metadata
            .get("grpc-timeout")
            .and_then(|value| value.to_str().ok())
            .and_then(parse_grpc_timeout)
            .map(|budget| Instant::now() + budget);
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let source_name = req.source_name.trim().to_string();
        if source_name.is_empty() {
            return Err(embedding_required_field(
                "source_name",
                "must be a non-empty embedding source name",
                "source_name is required",
            ));
        }
        // The broker NEVER embeds the query: a Retrieve must carry an already-
        // embedded query vector (the only mediated semantic path in this build).
        if req.query_vector.is_empty() {
            return Err(embedding_required_field(
                "query_vector",
                "must contain at least one embedding dimension",
                "query_vector is required (the broker does not embed queries; supply a vector)",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "embedding",
            OperationChannel::Vector,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let catalog = self.require_catalog()?;
        let mut context = native_service_context(&metadata, &tenant_id, "");
        context.scopes.push("udb:vector:read".to_string());
        let top_k = resolve_top_k(req.top_k);

        let stored = runtime
            .native_entity_read_for_service(
                "embedding",
                &context,
                source_read_by_name(&tenant_id, &source_name),
            )
            .await?
            .first()
            .map(stored_source_from_json)
            .filter(|source| source.status == STATUS_ACTIVE)
            .ok_or_else(|| embedding_source_not_found_status("retrieve"))?;

        // SERVER-SIDE tenant filter built from the VERIFIED claim, injected into the
        // delegated engine query (the `_tenant_id` must-clause). Never from the body.
        let tenant_filter = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "must": [ { "key": "_tenant_id", "match": { "value": tenant_id } } ]
        }));
        let state = catalog.active_for(&context.project_id);
        let manifest = &state.manifest;
        let collection = stored.collection();

        // Deadline gate before dispatch, then bound the delegated 9.5 hybrid search
        // by the remaining budget. `query_text` present ⇒ hybrid fusion; otherwise
        // a mediated vector search — never a raw engine query in either case.
        let remaining = remaining_before_deadline(deadline, Instant::now())?;
        let has_text = !req.query_text.trim().is_empty();
        let result: VectorSet = if has_text {
            let search = VectorHybridSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                text_query: req.query_text.clone(),
                filter: tenant_filter,
                limit: top_k,
                fusion_weights: retrieve_fusion_weights(),
                // Hits carry the stored user payload (minus the internal
                // `_tenant_id` write-stamp, stripped in the hit mapping below).
                with_payload: true,
            };
            let fut = runtime.vector_hybrid_search(manifest, search, context.clone());
            match remaining {
                Some(budget) => tokio::time::timeout(budget, fut).await.map_err(|_| {
                    crate::runtime::executor_utils::deadline_exceeded_status(
                        "embedding",
                        "retrieve_hybrid_search",
                        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                        "retrieve exceeded its deadline during hybrid search",
                    )
                })??,
                None => fut.await?,
            }
        } else {
            let search = VectorSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                filter: tenant_filter,
                limit: top_k,
                score_threshold: retrieve_score_threshold(),
                // Hits carry the stored user payload (minus the internal
                // `_tenant_id` write-stamp, stripped in the hit mapping below).
                with_payload: true,
            };
            let fut = runtime.vector_search(manifest, search, context.clone());
            match remaining {
                Some(budget) => tokio::time::timeout(budget, fut).await.map_err(|_| {
                    crate::runtime::executor_utils::deadline_exceeded_status(
                        "embedding",
                        "retrieve_vector_search",
                        crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                        "retrieve exceeded its deadline during vector search",
                    )
                })??,
                None => fut.await?,
            }
        };

        let hits = result
            .points
            .into_iter()
            .take(top_k as usize)
            .map(|point| embedding_pb::RetrieveHit {
                payload_json: retrieve_hit_payload_json(point.payload.as_ref()),
                id: point.id,
                score: f64::from(point.score),
            })
            .collect::<Vec<_>>();

        Ok(Response::new(embedding_pb::RetrieveResponse {
            hits,
            message: "ok".to_string(),
            error: None,
        }))
    }
}

struct EmbeddingWorkJob {
    source: StoredSource,
    event_id: String,
    tenant_id: String,
    project_id: String,
    payload: serde_json::Value,
}

struct EmbeddingBackfillJob {
    source: StoredSource,
    event_id: String,
    tenant_id: String,
    project_id: String,
    backfill_id: String,
}

/// One `udb.embedding.source.deleted.v1` journal event still awaiting vector
/// teardown (no `teardown.completed` marker keyed by its event id yet).
struct EmbeddingTeardownJob {
    event_id: String,
    tenant_id: String,
    project_id: String,
    source_name: String,
    /// The collection recorded on the source-deleted event (may be empty for a
    /// legacy row; the processor falls back to [`DEFAULT_VECTOR_COLLECTION`]).
    target_collection: String,
    /// CURRENT status of the (tenant, source_name) source row, if any — a source
    /// re-registered ACTIVE after the delete supersedes the teardown (deleting
    /// then would wipe fresh vectors).
    source_status: String,
}

/// SQL for one change-driven work-emitter journal pass. Every journal/outbox
/// payload key is read with the `COALESCE(payload->>k, payload->'payload'->>k)`
/// nesting fallback because the CDC journal ENVELOPE-nests emitted payloads
/// under `payload.payload` — the same lesson already applied to the backfill
/// loader; a top-level-only read matches zero real rows. Pure so the SQL shape
/// is unit-asserted.
fn embedding_work_jobs_sql(journal_relation: &str, outbox_relation: &str) -> String {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    format!(
        "SELECT \
            s.{source_id}::TEXT AS source_id, \
            s.{tenant_id}::TEXT AS source_tenant_id, \
            s.{source_name}::TEXT AS source_name, \
            s.{source_message_type}::TEXT AS source_message_type, \
            s.{text_fields_json}::TEXT AS text_fields_json, \
            s.{target_collection}::TEXT AS target_collection, \
            s.{model_id}::TEXT AS model_id, \
            s.{tenant_column}::TEXT AS tenant_column, \
            s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
            s.{status}::TEXT AS status, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{status} = $1 \
          AND COALESCE(s.{source_cdc_topic}::TEXT, '') <> '' \
          AND s.{source_cdc_topic} = j.topic \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND j.topic <> $2 \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $2 \
                 AND COALESCE(o.payload->>'source_event_id', o.payload->'payload'->>'source_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $2 \
                 AND COALESCE(done.payload->>'source_event_id', done.payload->'payload'->>'source_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $3",
        source_id = source.q("source_id"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
        source_message_type = source.q("source_message_type"),
        text_fields_json = source.q("text_fields_json"),
        target_collection = source.q("target_collection"),
        model_id = source.q("model_id"),
        tenant_column = source.q("tenant_column"),
        source_cdc_topic = source.q("source_cdc_topic"),
        status = source.q("status"),
    )
}

async fn load_embedding_work_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingWorkJob>, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&embedding_work_jobs_sql(journal_relation, outbox_relation))
        .bind(STATUS_ACTIVE)
        .bind(TOPIC_WORK)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("load embedding work jobs failed: {err}"))?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode embedding source payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode embedding source payload JSON failed: {err}"))?;
        jobs.push(EmbeddingWorkJob {
            source: StoredSource {
                source_id: row
                    .try_get("source_id")
                    .map_err(|err| format!("decode embedding source id failed: {err}"))?,
                tenant_id: row
                    .try_get("source_tenant_id")
                    .map_err(|err| format!("decode embedding source tenant failed: {err}"))?,
                source_name: row
                    .try_get("source_name")
                    .map_err(|err| format!("decode embedding source name failed: {err}"))?,
                source_message_type: row
                    .try_get("source_message_type")
                    .map_err(|err| format!("decode embedding source message type failed: {err}"))?,
                text_fields_json: row
                    .try_get("text_fields_json")
                    .map_err(|err| format!("decode embedding text fields failed: {err}"))?,
                target_collection: row
                    .try_get("target_collection")
                    .map_err(|err| format!("decode embedding collection failed: {err}"))?,
                model_id: row
                    .try_get("model_id")
                    .map_err(|err| format!("decode embedding model id failed: {err}"))?,
                tenant_column: row
                    .try_get("tenant_column")
                    .map_err(|err| format!("decode embedding tenant column failed: {err}"))?,
                source_cdc_topic: row
                    .try_get("source_cdc_topic")
                    .map_err(|err| format!("decode embedding source cdc topic failed: {err}"))?,
                status: row
                    .try_get("status")
                    .map_err(|err| format!("decode embedding source status failed: {err}"))?,
            },
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding event tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding event project failed: {err}"))?,
            payload,
        });
    }
    Ok(jobs)
}

async fn load_embedding_backfill_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingBackfillJob>, String> {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT \
            s.{source_id}::TEXT AS source_id, \
            s.{tenant_id}::TEXT AS source_tenant_id, \
            s.{source_name}::TEXT AS source_name, \
            s.{source_message_type}::TEXT AS source_message_type, \
            s.{text_fields_json}::TEXT AS text_fields_json, \
            s.{target_collection}::TEXT AS target_collection, \
            s.{model_id}::TEXT AS model_id, \
            s.{tenant_column}::TEXT AS tenant_column, \
            s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
            s.{status}::TEXT AS status, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'backfill_id', j.payload->'payload'->>'backfill_id', '') AS backfill_id \
         FROM {journal_relation} j \
         JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{source_name}::TEXT = COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') \
          AND s.{status} = $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'backfill_id', j.payload->'payload'->>'backfill_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} pending_done \
               WHERE pending_done.topic = $3 \
                 AND COALESCE(pending_done.payload->>'backfill_event_id', pending_done.payload->'payload'->>'backfill_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'backfill_event_id', done.payload->'payload'->>'backfill_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        source_id = source.q("source_id"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
        source_message_type = source.q("source_message_type"),
        text_fields_json = source.q("text_fields_json"),
        target_collection = source.q("target_collection"),
        model_id = source.q("model_id"),
        tenant_column = source.q("tenant_column"),
        source_cdc_topic = source.q("source_cdc_topic"),
        status = source.q("status"),
    ))
    .bind(STATUS_ACTIVE)
    .bind(TOPIC_BACKFILL_REQUESTED)
    .bind(TOPIC_BACKFILL_COMPLETED)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load embedding backfill jobs failed: {err}"))?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(EmbeddingBackfillJob {
            source: StoredSource {
                source_id: row
                    .try_get("source_id")
                    .map_err(|err| format!("decode embedding source id failed: {err}"))?,
                tenant_id: row
                    .try_get("source_tenant_id")
                    .map_err(|err| format!("decode embedding source tenant failed: {err}"))?,
                source_name: row
                    .try_get("source_name")
                    .map_err(|err| format!("decode embedding source name failed: {err}"))?,
                source_message_type: row
                    .try_get("source_message_type")
                    .map_err(|err| format!("decode embedding source message type failed: {err}"))?,
                text_fields_json: row
                    .try_get("text_fields_json")
                    .map_err(|err| format!("decode embedding text fields failed: {err}"))?,
                target_collection: row
                    .try_get("target_collection")
                    .map_err(|err| format!("decode embedding collection failed: {err}"))?,
                model_id: row
                    .try_get("model_id")
                    .map_err(|err| format!("decode embedding model id failed: {err}"))?,
                tenant_column: row
                    .try_get("tenant_column")
                    .map_err(|err| format!("decode embedding tenant column failed: {err}"))?,
                source_cdc_topic: row
                    .try_get("source_cdc_topic")
                    .map_err(|err| format!("decode embedding source cdc topic failed: {err}"))?,
                status: row
                    .try_get("status")
                    .map_err(|err| format!("decode embedding source status failed: {err}"))?,
            },
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding backfill event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding backfill tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding backfill project failed: {err}"))?,
            backfill_id: row
                .try_get("backfill_id")
                .map_err(|err| format!("decode embedding backfill id failed: {err}"))?,
        });
    }
    Ok(jobs)
}

/// SQL loading `udb.embedding.source.deleted.v1` journal events that have not
/// yet emitted a teardown-completed marker (checked in BOTH the pending outbox
/// and the replay journal, keyed by `teardown_event_id` — the dedup shape the
/// backfill loader uses). Payload keys use the nested-envelope COALESCE
/// fallback (see [`embedding_work_jobs_sql`]). The LEFT JOIN surfaces the
/// source row's CURRENT status so a re-registered (ACTIVE-again) source is
/// never torn down. Pure so the SQL shape is unit-asserted.
fn embedding_teardown_jobs_sql(journal_relation: &str, outbox_relation: &str) -> String {
    let source = embedding_source_model();
    let source_rel = source.relation.clone();
    format!(
        "SELECT \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') AS source_name, \
            COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '') AS target_collection, \
            COALESCE(s.{status}::TEXT, '') AS source_status \
         FROM {journal_relation} j \
         LEFT JOIN {source_rel} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{source_name}::TEXT = COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $1 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $2 \
                 AND COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $2 \
                 AND COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $3",
        status = source.q("status"),
        tenant_id = source.q("tenant_id"),
        source_name = source.q("source_name"),
    )
}

/// SQL enumerating a deleted source's emitted point ids: DISTINCT
/// `(target_collection, row_pk)` pairs from `udb.embedding.work.v1` events in
/// the replay journal UNION the pending outbox, tenant+source scoped, with the
/// nested-envelope COALESCE fallback on every key, keyset-paginated on the pair
/// so each pass is bounded. Pure so the SQL shape is unit-asserted.
fn embedding_teardown_point_ids_sql(journal_relation: &str, outbox_relation: &str) -> String {
    format!(
        "SELECT ids.target_collection, ids.row_pk FROM ( \
            SELECT \
                COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '') AS target_collection, \
                COALESCE(j.payload->>'row_pk', j.payload->'payload'->>'row_pk', '') AS row_pk \
            FROM {journal_relation} j \
            WHERE j.topic = $1 \
              AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') = $2 \
              AND COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '') = $3 \
            UNION \
            SELECT \
                COALESCE(o.payload->>'target_collection', o.payload->'payload'->>'target_collection', ''), \
                COALESCE(o.payload->>'row_pk', o.payload->'payload'->>'row_pk', '') \
            FROM {outbox_relation} o \
            WHERE o.topic = $1 \
              AND COALESCE(o.payload->>'tenant_id', o.payload->'payload'->>'tenant_id', '') = $2 \
              AND COALESCE(o.payload->>'source', o.payload->'payload'->>'source', '') = $3 \
         ) ids \
         WHERE ids.row_pk <> '' \
           AND (ids.target_collection, ids.row_pk) > ($4, $5) \
         ORDER BY ids.target_collection ASC, ids.row_pk ASC \
         LIMIT $6"
    )
}

async fn load_embedding_source_teardown_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<EmbeddingTeardownJob>, String> {
    let limit = batch.max(1);
    let rows = sqlx::query(&embedding_teardown_jobs_sql(
        journal_relation,
        outbox_relation,
    ))
    .bind(TOPIC_SOURCE_DELETED)
    .bind(TOPIC_SOURCE_TEARDOWN_COMPLETED)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load embedding teardown jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(EmbeddingTeardownJob {
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode embedding teardown event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode embedding teardown tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode embedding teardown project failed: {err}"))?,
            source_name: row
                .try_get("source_name")
                .map_err(|err| format!("decode embedding teardown source failed: {err}"))?,
            target_collection: row
                .try_get("target_collection")
                .map_err(|err| format!("decode embedding teardown collection failed: {err}"))?,
            source_status: row
                .try_get("source_status")
                .map_err(|err| format!("decode embedding teardown source status failed: {err}"))?,
        });
    }
    Ok(jobs)
}

/// Tear down a deleted source's vectors. Point ids are enumerated from the
/// source's emitted `udb.embedding.work.v1` events (replay journal + pending
/// outbox), grouped per target collection, and deleted through the SHARED
/// vector seam in [`EMBEDDING_TEARDOWN_DELETE_BATCH`]-bounded batches; only
/// after every batch succeeds is the `teardown.completed` marker emitted (a
/// delete failure leaves the job pending so the next leader pass retries —
/// fail closed, no false completion). A source re-registered ACTIVE after the
/// delete supersedes the teardown: it is marked completed WITHOUT deleting.
///
/// LIMITATION (best-effort, deliberate): the runtime exposes only a
/// per-point-id vector delete (`vector_delete_backend_target`) — no
/// tenant+source filter-delete seam — so teardown can only remove points whose
/// work events are still present in the journal/outbox. A point whose work
/// event was purged by journal retention, or that a sidecar reported for a row
/// that never produced a work event, is not enumerable here and survives until
/// a runtime filter-delete seam exists.
async fn process_embedding_teardown_job(
    service: &EmbeddingServiceImpl,
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    job: &EmbeddingTeardownJob,
) -> Result<i64, String> {
    if job.source_status == STATUS_ACTIVE {
        // Superseded by a re-registration: mark the delete event handled without
        // touching the store (deleting now would wipe the fresh source's vectors).
        service
            .emit_source_event(
                TOPIC_SOURCE_TEARDOWN_COMPLETED,
                &job.tenant_id,
                &job.project_id,
                &job.source_name,
                serde_json::json!({
                    "teardown_event_id": job.event_id,
                    "deleted": 0,
                    "superseded": true,
                }),
            )
            .await;
        return Ok(0);
    }
    let fallback_collection = if job.target_collection.trim().is_empty() {
        DEFAULT_VECTOR_COLLECTION.to_string()
    } else {
        job.target_collection.clone()
    };
    let mut deleted = 0i64;
    let mut cursor_collection = String::new();
    let mut cursor_pk = String::new();
    loop {
        let rows = sqlx::query(&embedding_teardown_point_ids_sql(
            journal_relation,
            outbox_relation,
        ))
        .bind(TOPIC_WORK)
        .bind(&job.tenant_id)
        .bind(&job.source_name)
        .bind(&cursor_collection)
        .bind(&cursor_pk)
        .bind(EMBEDDING_TEARDOWN_DELETE_BATCH)
        .fetch_all(pool)
        .await
        .map_err(|err| format!("enumerate embedding teardown points failed: {err}"))?;
        if rows.is_empty() {
            break;
        }
        let mut points = Vec::with_capacity(rows.len());
        for row in &rows {
            let collection: String = row.try_get("target_collection").map_err(|err| {
                format!("decode embedding teardown point collection failed: {err}")
            })?;
            let row_pk: String = row
                .try_get("row_pk")
                .map_err(|err| format!("decode embedding teardown point pk failed: {err}"))?;
            points.push((collection, row_pk));
        }
        if let Some((last_collection, last_pk)) = points.last() {
            cursor_collection = last_collection.clone();
            cursor_pk = last_pk.clone();
        }
        // Rows arrive ordered by collection, so consecutive runs form one
        // bounded seam call per collection.
        let mut index = 0usize;
        while index < points.len() {
            let group_collection = points[index].0.clone();
            let mut ids = Vec::new();
            while index < points.len() && points[index].0 == group_collection {
                ids.push(points[index].1.clone());
                index += 1;
            }
            let effective_collection = if group_collection.trim().is_empty() {
                fallback_collection.clone()
            } else {
                group_collection
            };
            let count = ids.len() as i64;
            service
                .delete_embedding_points(&job.project_id, &effective_collection, ids)
                .await
                .map_err(|err| {
                    format!(
                        "embedding teardown vector delete failed (collection \
                         {effective_collection}): {err}"
                    )
                })?;
            deleted = deleted.saturating_add(count);
        }
        if points.len() < EMBEDDING_TEARDOWN_DELETE_BATCH as usize {
            break;
        }
    }
    service
        .emit_source_event(
            TOPIC_SOURCE_TEARDOWN_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.source_name,
            serde_json::json!({
                "teardown_event_id": job.event_id,
                "target_collection": job.target_collection,
                "deleted": deleted,
            }),
        )
        .await;
    Ok(deleted)
}

fn backfill_read_context(tenant_id: &str, project_id: &str) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        purpose: "embedding_backfill".to_string(),
        scopes: vec!["udb:read".to_string()],
        service_identity: "udb.embedding.backfill".to_string(),
        ..crate::RequestContext::default()
    }
}

fn backfill_select_request(
    source: &StoredSource,
    primary_key: &str,
    text_fields: &[String],
    after_pk: Option<&str>,
    project_column: Option<&str>,
    project_id: &str,
) -> SelectRequest {
    let mut fields = Vec::with_capacity(text_fields.len().saturating_add(1));
    fields.push(primary_key.to_string());
    let effective_text_fields = if text_fields.is_empty() {
        vec!["text".to_string()]
    } else {
        text_fields.to_vec()
    };
    for field in &effective_text_fields {
        if field != primary_key && !fields.contains(field) {
            fields.push(field.clone());
        }
    }
    let mut tenant_filter = serde_json::Map::new();
    tenant_filter.insert(
        source.tenant_column.clone(),
        serde_json::Value::String(source.tenant_id.clone()),
    );
    let mut filters = vec![serde_json::Value::Object(tenant_filter)];
    // Project isolation: the data-plane planner rejects a select over a
    // project-scoped source table unless the filter carries the project column
    // (`project isolation requires filter on <col>`). Backfill enumerates through
    // that same served SELECT path, so it must add the source's project column =
    // the backfill job's project_id (empty in single-project mode, which matches
    // the empty project_id stored on single-project rows).
    if let Some(project_column) = project_column.filter(|col| !col.trim().is_empty()) {
        let mut project_filter = serde_json::Map::new();
        project_filter.insert(
            project_column.to_string(),
            serde_json::Value::String(project_id.to_string()),
        );
        filters.push(serde_json::Value::Object(project_filter));
    }
    if let Some(after_pk) = after_pk.filter(|value| !value.trim().is_empty()) {
        let mut cursor_op = serde_json::Map::new();
        cursor_op.insert(
            "$gt".to_string(),
            serde_json::Value::String(after_pk.to_string()),
        );
        let mut cursor_filter = serde_json::Map::new();
        cursor_filter.insert(
            primary_key.to_string(),
            serde_json::Value::Object(cursor_op),
        );
        filters.push(serde_json::Value::Object(cursor_filter));
    }
    SelectRequest {
        message_type: source.source_message_type.clone(),
        filter: crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "$and": filters,
        })),
        fields,
        limit: EMBEDDING_BACKFILL_PAGE_LIMIT,
        sort: vec![Sort {
            field: primary_key.to_string(),
            descending: false,
        }],
        ..SelectRequest::default()
    }
}

fn json_row_pk(row: &serde_json::Value, primary_key: &str) -> String {
    json_string_field(row, primary_key).unwrap_or_default()
}

async fn process_embedding_backfill_job(
    service: &EmbeddingServiceImpl,
    job: &EmbeddingBackfillJob,
) -> Result<i64, String> {
    if job.source.status != STATUS_ACTIVE
        || job.source.tenant_id.trim().is_empty()
        || job.source.tenant_id.trim() != job.tenant_id.trim()
    {
        return Ok(0);
    }
    let runtime = service
        .runtime
        .as_ref()
        .ok_or_else(|| "embedding backfill requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_ref()
        .ok_or_else(|| "embedding backfill requires active catalog".to_string())?;
    let state = catalog.active_for(&job.project_id);
    let table = crate::broker::table_for_message(&state.manifest, &job.source.source_message_type)
        .ok_or_else(|| {
            format!(
                "embedding backfill source entity '{}' is not present in active catalog",
                job.source.source_message_type
            )
        })?;
    let primary_key = table.primary_key.first().cloned().ok_or_else(|| {
        format!(
            "embedding backfill source entity '{}' has no primary key",
            job.source.source_message_type
        )
    })?;
    let text_fields = parse_source_text_fields(&job.source.text_fields_json);
    // The source table's project column (if project-scoped) — the backfill SELECT
    // must filter on it or the planner rejects it for project isolation.
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = backfill_read_context(&job.tenant_id, &job.project_id);
    let mut emitted = 0i64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = backfill_select_request(
            &job.source,
            &primary_key,
            &text_fields,
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(&state.manifest, request, context.clone())
            .await
            .map_err(|err| format!("embedding backfill source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let mut last_pk = String::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode embedding backfill source row failed: {err}"))?;
            let row_pk = json_row_pk(&row, &primary_key);
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            let text = extract_backfill_source_text(&row, &text_fields, table);
            if text.trim().is_empty() {
                continue;
            }
            service
                .emit_backfill_work_event(
                    &job.tenant_id,
                    &job.project_id,
                    &job.source.source_name,
                    &row_pk,
                    &text,
                    &job.source.model_id,
                    &job.source.collection(),
                    &job.event_id,
                    &job.backfill_id,
                )
                .await;
            emitted = emitted.saturating_add(1);
        }
        if record_set.records_json.len() < EMBEDDING_BACKFILL_PAGE_LIMIT as usize
            || last_pk.is_empty()
        {
            break;
        }
        after_pk = Some(last_pk);
    }
    service
        .emit_source_event(
            TOPIC_BACKFILL_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.source.source_name,
            serde_json::json!({
                "backfill_event_id": job.event_id,
                "backfill_id": job.backfill_id,
                "emitted": emitted,
            }),
        )
        .await;
    Ok(emitted)
}

fn json_string_field(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn source_change_row(payload: &serde_json::Value) -> Option<&serde_json::Value> {
    payload
        .get("after")
        .or_else(|| payload.get("payload").and_then(|inner| inner.get("after")))
        .or_else(|| payload.get("payload"))
}

fn source_change_row_pk(payload: &serde_json::Value) -> String {
    for key in ["document_id", "row_pk", "id"] {
        if let Some(value) = json_string_field(payload, key) {
            return value;
        }
    }
    if let Some(row) = source_change_row(payload) {
        for key in ["row_pk", "id"] {
            if let Some(value) = json_string_field(row, key) {
                return value;
            }
        }
    }
    String::new()
}

fn source_change_operation(payload: &serde_json::Value) -> String {
    json_string_field(payload, "operation")
        .or_else(|| json_string_field(payload, "op"))
        .or_else(|| {
            payload
                .get("payload")
                .and_then(|inner| json_string_field(inner, "op"))
        })
        .unwrap_or_default()
}

fn source_change_is_delete(payload: &serde_json::Value) -> bool {
    let op = source_change_operation(payload);
    op.eq_ignore_ascii_case("delete") || op.eq_ignore_ascii_case("d")
}

fn parse_source_text_fields(raw: &str) -> Vec<String> {
    if let Ok(fields) = serde_json::from_str::<Vec<String>>(raw) {
        return fields;
    }
    if let Ok(inner) = serde_json::from_str::<String>(raw) {
        if let Ok(fields) = serde_json::from_str::<Vec<String>>(&inner) {
            return fields;
        }
    }
    Vec::new()
}

async fn process_embedding_work_job(
    service: &EmbeddingServiceImpl,
    job: &EmbeddingWorkJob,
) -> bool {
    let source_tenant = job.source.tenant_id.trim();
    let event_tenant = job.tenant_id.trim();
    if source_tenant.is_empty() || event_tenant != source_tenant {
        return false;
    }
    if job.source.status != STATUS_ACTIVE || job.source.source_cdc_topic.trim().is_empty() {
        return false;
    }
    let row_pk = source_change_row_pk(&job.payload);
    if row_pk.is_empty() {
        return false;
    }
    let collection = job.source.collection();
    if source_change_is_delete(&job.payload) {
        service
            .delete_embedding_point(&job.project_id, &collection, &row_pk)
            .await;
        return true;
    }

    let text_fields = parse_source_text_fields(&job.source.text_fields_json);
    let row = source_change_row(&job.payload).unwrap_or(&job.payload);
    let text = extract_source_text(row, &text_fields);
    if text.trim().is_empty() {
        return false;
    }
    service
        .emit_work_event_with_source_event(
            event_tenant,
            &job.project_id,
            &job.source.source_name,
            &row_pk,
            &text,
            &job.source.model_id,
            &collection,
            Some(&job.event_id),
        )
        .await;
    true
}

/// Run one leader-owned pass over the durable CDC journal, in three stages:
/// (1) active embedding sources are joined by `(tenant_id, source_cdc_topic)`
/// to published source changes — source events that already produced
/// `udb.embedding.work.v1` are skipped by `source_event_id` in both the outbox
/// and replay journal; (2) requested backfills enumerate their source rows via
/// the served SELECT path and emit per-row work; (3) `source.deleted` events
/// tear down the deleted source's vectors through the shared vector seam,
/// deduplicated by a `teardown.completed` marker.
pub(crate) async fn run_embedding_work_emitter_once(
    service: Arc<EmbeddingServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "embedding work emitter requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "embedding work emitter requires transactional outbox".to_string())?;
    let jobs = load_embedding_work_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut acted = 0i64;
    for job in &jobs {
        if process_embedding_work_job(&service, job).await {
            acted = acted.saturating_add(1);
        }
    }
    let backfills =
        load_embedding_backfill_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &backfills {
        acted = acted.saturating_add(process_embedding_backfill_job(&service, job).await?);
    }
    let teardowns =
        load_embedding_source_teardown_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &teardowns {
        acted = acted.saturating_add(
            process_embedding_teardown_job(&service, pool, journal_relation, outbox_relation, job)
                .await?,
        );
    }
    Ok(acted)
}

/// Per-source change-driven WORK emitter (master-plan 9.11). For each event on the
/// source entity's tenant-scoped CDC topic, drop anything outside the source's
/// tenant scope (fail closed) before acting; a row delete removes the orphan vector
/// via the shared vector seam, and any other change extracts the configured text
/// field(s) and emits a `udb.embedding.work.v1` event (row pk + text + non-secret
/// routing ONLY — never a credential) for the sidecar pool to embed. This channel
/// form stays as a narrow injected-event test seam; production uses
/// [`run_embedding_work_emitter_once`] over the durable CDC journal.
#[allow(dead_code)]
pub(crate) async fn run_embedding_work_emitter(
    service: Arc<EmbeddingServiceImpl>,
    source: StoredSource,
    tenant_scope: String,
    project_id: String,
    mut events: tokio::sync::mpsc::Receiver<serde_json::Value>,
) {
    let collection = source.collection();
    let text_fields = parse_source_text_fields(&source.text_fields_json);
    while let Some(payload) = events.recv().await {
        // Fail closed: a missing/foreign tenant_id payload is never acted on.
        let event_tenant = payload
            .get("tenant_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .trim();
        if tenant_scope.trim().is_empty() || event_tenant != tenant_scope.trim() {
            continue;
        }
        let row_pk = payload
            .get("row_pk")
            .or_else(|| payload.get("id"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        if row_pk.is_empty() {
            continue;
        }
        // A delete change removes the orphan vector through the shared seam.
        let op = payload
            .get("op")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if op.eq_ignore_ascii_case("delete") || op.eq_ignore_ascii_case("d") {
            service
                .delete_embedding_point(&project_id, &collection, &row_pk)
                .await;
            continue;
        }
        // Extract the configured text field(s); empty text ⇒ nothing to embed.
        let row = payload.get("after").unwrap_or(&payload);
        let text = extract_source_text(row, &text_fields);
        if text.trim().is_empty() {
            continue;
        }
        service
            .emit_work_event(
                &tenant_scope,
                &project_id,
                &source.source_name,
                &row_pk,
                &text,
                &source.model_id,
                &collection,
            )
            .await;
    }
}

/// Concatenate the configured source text field(s) from a change row. When no
/// fields are configured, falls back to a `text` field if present. Pure.
fn extract_source_text(row: &serde_json::Value, fields: &[String]) -> String {
    if fields.is_empty() {
        return row
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
    }
    fields
        .iter()
        .filter_map(|field| row.get(field).and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

fn extract_backfill_source_text(
    row: &serde_json::Value,
    fields: &[String],
    table: &crate::generation::ManifestTable,
) -> String {
    if fields.is_empty() {
        return extract_source_text(row, fields);
    }
    fields
        .iter()
        .filter_map(|field| {
            row.get(field)
                .and_then(serde_json::Value::as_str)
                .or_else(|| {
                    table
                        .columns
                        .iter()
                        .find(|column| column.field_name == *field)
                        .and_then(|column| row.get(&column.column_name))
                        .and_then(serde_json::Value::as_str)
                })
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod embedding_scope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail metadata");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_schema_not_found_detail(status: &Status, operation: &str) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "embedding source not found");
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "embedding");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "embedding_source_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// The change/backfill WORK payload carries the row pk + text + non-secret
    /// routing, and provably NO credential field — model creds live in the sidecar.
    #[test]
    fn work_event_payload_has_no_credentials() {
        let payload = build_work_event_payload(
            "acme",
            "contacts",
            "row-1",
            "Ada Lovelace, London",
            "text-embedding-3-small",
            "acme_contacts_vectors",
        );
        let object = payload.as_object().expect("object payload");
        // The intended non-secret routing keys are present.
        for key in [
            "tenant_id",
            "source",
            "row_pk",
            "text",
            "model_id",
            "target_collection",
        ] {
            assert!(object.contains_key(key), "missing routing key {key}");
        }
        assert_eq!(object.get("row_pk").and_then(|v| v.as_str()), Some("row-1"));
        assert_eq!(
            object.get("text").and_then(|v| v.as_str()),
            Some("Ada Lovelace, London")
        );
        // No credential-shaped key may EVER appear in a work payload.
        for forbidden in [
            "api_key",
            "apikey",
            "secret",
            "token",
            "password",
            "credential",
            "credentials",
            "authorization",
            "key",
        ] {
            assert!(
                !object.contains_key(forbidden),
                "work payload leaked credential-shaped key {forbidden}"
            );
        }
    }

    /// A sidecar callback scoped to tenant-a must not report a vector for tenant-b
    /// by putting a foreign tenant_id in the request BODY; the scope guard rejects
    /// this before any store/vector access (no Postgres needed).
    #[tokio::test]
    async fn report_embedding_rejects_cross_tenant_body() {
        let svc = EmbeddingServiceImpl::new(); // no runtime/catalog (guard runs first)
        let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
            tenant_id: "tenant-b".to_string(),
            source_name: "contacts".to_string(),
            row_pk: "row-1".to_string(),
            vector: vec![0.1, 0.2, 0.3],
            model: "m".to_string(),
            dims: 3,
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .report_embedding(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn register_source_missing_required_fields_carries_field_violations() {
        let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
        let mut request = Request::new(embedding_pb::RegisterSourceRequest {
            tenant_id: "tenant-a".to_string(),
            source_name: " ".to_string(),
            source_message_type: String::new(),
            text_fields: Vec::new(),
            target_collection: "contacts_vec".to_string(),
            model_id: String::new(),
            metadata_json: String::new(),
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .register_source(request)
            .await
            .expect_err("missing source identity must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "source_name and source_message_type are required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "source_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty embedding source name"
        );
        assert_eq!(detail.field_violations[1].field, "source_message_type");
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty source message type"
        );
    }

    #[tokio::test]
    async fn report_embedding_missing_identity_carries_field_violations() {
        let svc = EmbeddingServiceImpl::new(); // no runtime; validation runs first
        let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
            tenant_id: "tenant-a".to_string(),
            source_name: String::new(),
            row_pk: " ".to_string(),
            vector: vec![0.1, 0.2],
            model: "m".to_string(),
            dims: 2,
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .report_embedding(request)
            .await
            .expect_err("missing report identity must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "source_name and row_pk are required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "source_name");
        assert_eq!(detail.field_violations[1].field, "row_pk");
    }

    #[tokio::test]
    async fn retrieve_missing_query_vector_carries_field_violation() {
        let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
        let mut request = Request::new(embedding_pb::RetrieveRequest {
            tenant_id: "tenant-a".to_string(),
            source_name: "contacts".to_string(),
            query_text: String::new(),
            query_vector: Vec::new(),
            top_k: 10,
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));

        let err = svc
            .retrieve(request)
            .await
            .expect_err("missing query vector must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "query_vector is required (the broker does not embed queries; supply a vector)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "query_vector");
        assert_eq!(
            detail.field_violations[0].description,
            "must contain at least one embedding dimension"
        );
    }

    #[test]
    fn source_message_type_missing_from_catalog_carries_field_violation() {
        let catalog = Arc::new(CatalogManager::new(
            crate::generation::CatalogManifest::default(),
        ));
        let svc = EmbeddingServiceImpl::new().with_catalog(Some(catalog));
        let err = svc
            .resolve_source_tenant_column("default", "acme.crm.entity.v1.Contact")
            .expect_err("unknown source message type must fail before store/vector access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "source_message_type 'acme.crm.entity.v1.Contact' is not present in the active catalog manifest"
        );
        assert_single_field_violation(
            &err,
            "source_message_type",
            "must be present in the active catalog manifest",
        );
    }

    #[test]
    fn embedding_missing_setup_capabilities_carry_typed_detail() {
        for (operation, capability, message) in [
            (
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "embedding service requires runtime native-entity dispatch (no runtime configured)",
            ),
            (
                "catalog_lookup",
                "active_catalog",
                "embedding service requires the active catalog (no catalog configured)",
            ),
        ] {
            let err = embedding_capability_status(operation, capability, message);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "embedding");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, capability);
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn embedding_source_not_found_statuses_carry_schema_detail() {
        for operation in ["backfill", "report_embedding", "retrieve"] {
            assert_schema_not_found_detail(
                &embedding_source_not_found_status(operation),
                operation,
            );
        }
    }

    /// The reported embedding is tenant-tagged and fails CLOSED on an empty tenant
    /// (no fail-open): the stored point carries the `_tenant_id` payload key.
    #[test]
    fn embedding_point_is_tenant_tagged_no_fail_open() {
        // Empty (unverified) tenant ⇒ fail closed, never stored.
        let err = build_embedding_point("row-1", vec![0.1, 0.2], "  ")
            .expect_err("empty tenant must fail closed");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert_eq!(
            err.message(),
            "embedding upsert requires a verified tenant; refusing to store an unscoped vector \
             (no fail-open)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "embedding_vector_upsert");
        assert_eq!(detail.policy_decision_id, "verified_tenant_required");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        // A verified tenant ⇒ the point is tagged with `_tenant_id`.
        let point =
            build_embedding_point("row-1", vec![0.1, 0.2], "acme").expect("verified tenant ok");
        assert_eq!(point.id, "row-1");
        let payload = point.payload.expect("tenant-tag payload present");
        assert!(
            payload.fields.contains_key("_tenant_id"),
            "stored vector is not tenant-tagged"
        );
    }

    /// RegisterSource fails closed when the source entity has no resolvable tenant
    /// column (a reported vector / Retrieve would otherwise have no tenant
    /// predicate to inject).
    #[test]
    fn register_source_fails_closed_without_source_tenant_column() {
        let err = require_source_tenant_column(None, "acme.crm.entity.v1.Contact")
            .expect_err("missing tenant column must fail closed");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "source entity 'acme.crm.entity.v1.Contact' has no resolvable tenant column; refusing to register a tenant-scoped embedding source over it (fail closed)"
        );
        assert_single_field_violation(
            &err,
            "source_message_type",
            "must resolve to a tenant-scoped source entity",
        );
        let blank = require_source_tenant_column(Some("  ".to_string()), "X")
            .expect_err("blank column must fail closed");
        assert_eq!(blank.code(), tonic::Code::InvalidArgument);
        assert_single_field_violation(
            &blank,
            "source_message_type",
            "must resolve to a tenant-scoped source entity",
        );
        assert_eq!(
            require_source_tenant_column(Some("tenant_id".to_string()), "X").unwrap(),
            "tenant_id"
        );
    }

    /// A Retrieve whose gRPC deadline is already past returns `deadline_exceeded`
    /// before any semantic-search dispatch; a future deadline yields the remaining
    /// budget.
    #[test]
    fn retrieve_past_deadline_returns_deadline_exceeded() {
        let base = Instant::now();
        // Deadline in the past relative to `now` ⇒ exceeded.
        let now = base + Duration::from_secs(1);
        let err = remaining_before_deadline(Some(base), now)
            .expect_err("past deadline must be deadline_exceeded");
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
        // A future deadline ⇒ a positive remaining budget.
        let future = base + Duration::from_secs(30);
        let remaining = remaining_before_deadline(Some(future), base)
            .expect("future deadline ok")
            .expect("some remaining budget");
        assert!(remaining > Duration::from_secs(20));
        // No deadline ⇒ unbounded (None).
        assert!(
            remaining_before_deadline(None, base)
                .expect("no deadline ok")
                .is_none()
        );
    }

    /// The gRPC timeout header parser handles the standard unit suffixes.
    #[test]
    fn parses_grpc_timeout_units() {
        assert_eq!(parse_grpc_timeout("5S"), Some(Duration::from_secs(5)));
        assert_eq!(parse_grpc_timeout("250m"), Some(Duration::from_millis(250)));
        assert_eq!(parse_grpc_timeout("1M"), Some(Duration::from_secs(60)));
        assert_eq!(parse_grpc_timeout(""), None);
        assert_eq!(parse_grpc_timeout("abc"), None);
    }

    #[test]
    fn source_change_helpers_accept_cdc_envelope() {
        let payload = serde_json::json!({
            "tenant_id": "tenant-a",
            "project_id": "project-a",
            "operation": "upsert",
            "document_id": "invoice-1",
            "payload": {
                "email": "ada@example.com",
                "status": "OPEN"
            }
        });
        assert_eq!(source_change_row_pk(&payload), "invoice-1");
        assert!(!source_change_is_delete(&payload));
        let row = source_change_row(&payload).expect("source row");
        assert_eq!(
            extract_source_text(row, &["email".to_string(), "status".to_string()]),
            "ada@example.com OPEN"
        );

        let deleted = serde_json::json!({
            "tenant_id": "tenant-a",
            "operation": "delete",
            "document_id": "invoice-1",
            "payload": { "email": "ada@example.com" }
        });
        assert!(source_change_is_delete(&deleted));
    }

    #[test]
    fn source_text_fields_parse_jsonb_and_string_wrapped_json() {
        assert_eq!(
            parse_source_text_fields(r#"["email","status"]"#),
            vec!["email".to_string(), "status".to_string()]
        );
        assert_eq!(
            parse_source_text_fields(r#""[\"email\",\"status\"]""#),
            vec!["email".to_string(), "status".to_string()]
        );
        assert!(parse_source_text_fields("not-json").is_empty());
    }

    #[test]
    fn backfill_select_request_is_tenant_scoped_and_cursor_bounded() {
        let source = StoredSource {
            source_id: "src-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            source_name: "contacts".to_string(),
            source_message_type: "acme.crm.v1.Contact".to_string(),
            text_fields_json: r#"["email","status"]"#.to_string(),
            target_collection: "contacts_vec".to_string(),
            model_id: "model-a".to_string(),
            tenant_column: "tenant_id".to_string(),
            source_cdc_topic: "acme.contacts.cdc".to_string(),
            status: STATUS_ACTIVE.to_string(),
        };
        let request = backfill_select_request(
            &source,
            "contact_id",
            &["email".to_string(), "status".to_string()],
            Some("contact-7"),
            None,
            "",
        );
        assert_eq!(request.message_type, "acme.crm.v1.Contact");
        assert_eq!(request.fields, ["contact_id", "email", "status"]);
        assert_eq!(request.limit, EMBEDDING_BACKFILL_PAGE_LIMIT);
        assert_eq!(request.sort[0].field, "contact_id");
        let filter = request.filter.as_ref().expect("tenant filter");
        let json = crate::runtime::executor_utils::struct_to_json(filter);
        assert_eq!(json["$and"][0]["tenant_id"], "tenant-a");
        assert_eq!(json["$and"][1]["contact_id"]["$gt"], "contact-7");

        // With a project-scoped source, the SELECT MUST carry the project column
        // (else the planner rejects it: "project isolation requires filter on
        // project_id") — the bug that made backfill emit zero work per row.
        let scoped = backfill_select_request(
            &source,
            "contact_id",
            &["email".to_string()],
            None,
            Some("project_id"),
            "proj-9",
        );
        let scoped_json =
            crate::runtime::executor_utils::struct_to_json(scoped.filter.as_ref().unwrap());
        assert_eq!(scoped_json["$and"][0]["tenant_id"], "tenant-a");
        assert_eq!(scoped_json["$and"][1]["project_id"], "proj-9");
    }

    #[test]
    fn backfill_read_context_uses_served_read_gate() {
        let context = backfill_read_context("tenant-a", "project-a");
        assert_eq!(context.tenant_id, "tenant-a");
        assert_eq!(context.project_id, "project-a");
        assert_eq!(context.purpose, "embedding_backfill");
        assert_eq!(context.scopes, vec!["udb:read".to_string()]);
        assert_eq!(context.service_identity, "udb.embedding.backfill");
    }

    /// Dimension truth (16.6.2 shape gate): an empty vector and a `dims` claim
    /// contradicting the actual vector length are both rejected with typed
    /// field violations; an unset (non-positive) or matching `dims` passes.
    #[test]
    fn reported_vector_shape_gate_rejects_empty_and_mismatched_dims() {
        let err = validate_reported_vector(0, &[]).expect_err("empty vector must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "vector is required");
        assert_single_field_violation(
            &err,
            "vector",
            "must contain at least one embedding dimension",
        );

        let err =
            validate_reported_vector(5, &[0.1, 0.2, 0.3]).expect_err("dims mismatch must fail");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "dims (5) does not match the reported vector length (3)"
        );
        assert_single_field_violation(
            &err,
            "dims",
            "must equal the reported vector length when set",
        );

        validate_reported_vector(0, &[0.1]).expect("unset dims must pass");
        validate_reported_vector(-1, &[0.1]).expect("negative dims treated as unset");
        validate_reported_vector(3, &[0.1, 0.2, 0.3]).expect("matching dims must pass");
    }

    /// The RPC surface enforces the shape gate before admission/store/vector
    /// access: a mismatched `dims` is refused on a bare service (no runtime).
    #[tokio::test]
    async fn report_embedding_dims_mismatch_carries_field_violation() {
        let svc = EmbeddingServiceImpl::new(); // no runtime/catalog; validation runs first
        let mut request = Request::new(embedding_pb::ReportEmbeddingRequest {
            tenant_id: "tenant-a".to_string(),
            source_name: "contacts".to_string(),
            row_pk: "row-1".to_string(),
            vector: vec![0.1, 0.2, 0.3],
            model: "m".to_string(),
            dims: 5,
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .report_embedding(request)
            .await
            .expect_err("dims mismatch must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_single_field_violation(
            &err,
            "dims",
            "must equal the reported vector length when set",
        );
    }

    /// Loader parity (16.6.3): the change-driven work loader reads every journal
    /// payload key with the SAME `COALESCE(top-level, payload->'payload'->>…)`
    /// nesting fallback the backfill loader uses — the CDC journal ENVELOPE-nests
    /// emitted payloads, so a top-level-only read matches zero real rows.
    #[test]
    fn work_loader_sql_reads_nested_journal_envelope() {
        let sql = embedding_work_jobs_sql("udb_cdc_event_journal", "udb_outbox");
        for fragment in [
            "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
            "COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '')",
            "COALESCE(o.payload->>'source_event_id', o.payload->'payload'->>'source_event_id')",
            "COALESCE(done.payload->>'source_event_id', done.payload->'payload'->>'source_event_id')",
        ] {
            assert!(
                sql.contains(fragment),
                "work loader SQL lost the nested-envelope fallback: missing {fragment}"
            );
        }
        // No key may still be read top-level-only.
        assert!(
            !sql.contains("COALESCE(j.payload->>'tenant_id', '')"),
            "work loader SQL still reads tenant_id top-level only"
        );
    }

    /// Source teardown reads its journal keys with the same nested-envelope
    /// fallback, and dedups by `teardown_event_id` in both outbox and journal.
    #[test]
    fn teardown_sqls_read_nested_journal_envelope() {
        let jobs_sql = embedding_teardown_jobs_sql("udb_cdc_event_journal", "udb_outbox");
        for fragment in [
            "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
            "COALESCE(j.payload->>'source', j.payload->'payload'->>'source', '')",
            "COALESCE(j.payload->>'target_collection', j.payload->'payload'->>'target_collection', '')",
            "COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id')",
            "COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id')",
        ] {
            assert!(
                jobs_sql.contains(fragment),
                "teardown loader SQL missing nested-envelope fallback: {fragment}"
            );
        }
        let ids_sql = embedding_teardown_point_ids_sql("udb_cdc_event_journal", "udb_outbox");
        for fragment in [
            "COALESCE(j.payload->>'row_pk', j.payload->'payload'->>'row_pk', '')",
            "COALESCE(o.payload->>'row_pk', o.payload->'payload'->>'row_pk', '')",
            "COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '')",
            "COALESCE(o.payload->>'source', o.payload->'payload'->>'source', '')",
        ] {
            assert!(
                ids_sql.contains(fragment),
                "teardown point-id SQL missing nested-envelope fallback: {fragment}"
            );
        }
        // Bounded, keyset-paginated enumeration — never an unbounded scan.
        assert!(ids_sql.contains("LIMIT $6"));
        assert!(ids_sql.contains("(ids.target_collection, ids.row_pk) > ($4, $5)"));
    }

    /// Retrieve hits carry the stored user payload with the internal
    /// `_tenant_id` write-stamp stripped; tag-only or absent payloads stay "".
    #[test]
    fn retrieve_hit_payload_strips_internal_tenant_tag() {
        let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "_tenant_id": "acme",
            "title": "Q3 report",
            "chunk": 4,
        }))
        .expect("struct payload");
        let json = retrieve_hit_payload_json(Some(&payload));
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("payload_json is valid JSON");
        assert_eq!(value["title"], "Q3 report");
        assert_eq!(value["chunk"], 4.0);
        assert!(
            value.get("_tenant_id").is_none(),
            "internal tenant tag leaked into RetrieveHit payload_json"
        );

        // A payload that is ONLY the internal tag serializes to the empty
        // sentinel, and no payload stays empty too.
        let tag_only = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "_tenant_id": "acme",
        }))
        .expect("struct payload");
        assert_eq!(retrieve_hit_payload_json(Some(&tag_only)), "");
        assert_eq!(retrieve_hit_payload_json(None), "");
    }
}

impl DataBrokerService {
    /// Build the native `EmbeddingService`, wired to the broker's Postgres pool, the
    /// mediated-dispatch + shared-vector-seam runtime, the project-active catalog
    /// (source tenant-column resolution + the manifest Retrieve delegates with), and
    /// the shared outbox.
    pub(crate) fn build_embedding_service(&self) -> EmbeddingServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("embedding", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        EmbeddingServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_catalog(Some(self.catalog.clone()))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
