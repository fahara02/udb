//! Native `SearchService` (master-plan 9.5) — one search box over everything.
//!
//! Mirrors `lock_service`: proto-driven, no in-memory store, no hand-mapped
//! schema. The durable, tenant-scoped `udb_search.search_indexes` row records
//! which source entity is indexed, on which engine, and — critically — the
//! SOURCE table's resolved tenant column. An index is created ONLY when that
//! column resolves, so every `Search` has a server-side tenant predicate to
//! inject (fail closed).
//!
//! Doctrine (Phase 9, item 9.5):
//! - **Mediated path only.** Index CRUD rides the neutral-IR native-entity path
//!   ([`DataBrokerRuntime::native_entity_read_for_service`] /
//!   `native_entity_write_for_service`). Queries ride the runtime's mediated
//!   vector dispatch ([`DataBrokerRuntime::vector_search`] /
//!   `vector_hybrid_search`) — NEVER a hand-built raw engine query (the
//!   batch-RPC authz-bypass lesson).
//! - **Shared tenant resolver.** The source table's tenant column is resolved
//!   via the shared catalog resolver (`postgres_helpers::tenant_column_ref`, the
//!   same `util::resolve_tenant_column` family behind
//!   `native_catalog::NativeModel::tenant_column`), never a second copy.
//! - **Server-side tenant filter on every Search.** The verified-claim tenant is
//!   injected into the engine query (Qdrant `must` clause / Elasticsearch term)
//!   from the claim, never from the request body.
//! - **Tenant-scoped CDC freshness.** The per-index consumer drops a
//!   missing/foreign `tenant_id` payload (fail closed) before it can be indexed.
//! - **Pure RRF.** Cross-index rank fusion is the pure
//!   `score = Σ 1/(60 + rank_i)` function, unit-tested.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use chrono::Utc;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::search::services::v1 as search_pb;
use crate::proto::udb::core::search::services::v1::search_service_server::SearchService;
use crate::proto::{
    SelectRequest, Sort, VectorHybridSearchRequest, VectorPoint, VectorPointMutation,
    VectorSearchRequest, VectorSet, VectorUpsertRequest,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::CatalogManager;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::search::services::v1::search_service_server::SearchServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token, native_offset_page_window, native_service_context, non_empty_json,
    validate_request_tenant,
};

const SEARCH_INDEX_MSG: &str = "udb.core.search.entity.v1.SearchIndex";

const TOPIC_CREATED: &str = "udb.search.index.created.v1";
const TOPIC_DELETED: &str = "udb.search.index.deleted.v1";
const TOPIC_REINDEX: &str = "udb.search.index.reindex.requested.v1";
/// Completion marker for one executed reindex request, keyed by
/// `reindex_event_id` (the journal event id of the reindex.requested event) so
/// the leader pass never re-runs a finished reindex (mirrors the embedding
/// backfill requested/completed pairing).
const TOPIC_REINDEX_COMPLETED: &str = "udb.search.index.reindex.completed.v1";
/// Completion marker for a deleted index's engine teardown, keyed by
/// `teardown_event_id` (the journal event id of the index.deleted event the
/// teardown job consumes).
const TOPIC_TEARDOWN_COMPLETED: &str = "udb.search.index.teardown.completed.v1";
/// Applied marker for one consumed CDC freshness event, keyed by
/// `source_event_id`, so the leader freshness pass never re-consumes a journal
/// event (the same output-event-as-dedup-marker pattern as cache invalidation
/// and the embedding work emitter).
const TOPIC_FRESHNESS_APPLIED: &str = "udb.search.index.freshness.applied.v1";

const STATUS_ACTIVE: &str = "ACTIVE";
const STATUS_REINDEXING: &str = "REINDEXING";
const STATUS_DELETED: &str = "DELETED";

const BACKEND_QDRANT: &str = "qdrant";
const BACKEND_ELASTICSEARCH: &str = "elasticsearch";

/// Reciprocal-rank-fusion constant. The canonical `k` from the original RRF
/// paper (Cormack et al.); larger values flatten the contribution of top ranks.
const RRF_K: f64 = 60.0;

/// Default number of hits returned when the caller does not specify `top_k`.
const DEFAULT_TOP_K: i32 = 10;
/// Upper bound on `top_k` so one query cannot pull an unbounded result set.
const MAX_TOP_K: i32 = 200;
/// Per-tenant registered-index budget. Bounds the durable table so one tenant
/// cannot exhaust the shared store; a new index beyond this fails closed.
const MAX_INDEXES_PER_TENANT: usize = 128;

/// Journal events consumed per leader freshness pass (mirrors
/// `EMBEDDING_WORK_EMITTER_BATCH` / `CACHE_INVALIDATION_BATCH`).
pub(crate) const SEARCH_FRESHNESS_BATCH: i64 = 200;
/// Reindex/teardown jobs consumed per leader pass. Each job pages through its
/// ENTIRE source table, so the job batch is deliberately smaller than the
/// per-event journal batch.
pub(crate) const SEARCH_REINDEX_BATCH: i64 = 25;
/// Page size for enumerating source rows during a reindex/teardown pass
/// (mirrors `EMBEDDING_BACKFILL_PAGE_LIMIT`).
const SEARCH_REINDEX_PAGE_LIMIT: i32 = 200;
const DEFAULT_SEARCH_FRESHNESS_INTERVAL_SECS: u64 = 30;
const SEARCH_FRESHNESS_INTERVAL_ENV: &str = "UDB_SEARCH_FRESHNESS_INTERVAL_SECS";
const DEFAULT_SEARCH_REINDEX_INTERVAL_SECS: u64 = 30;
const SEARCH_REINDEX_INTERVAL_ENV: &str = "UDB_SEARCH_REINDEX_INTERVAL_SECS";

/// The tenant-scope payload key the vector IR compiler stamps at write time.
/// Every Search both filters on it server-side and strips it from returned hit
/// payloads (it is broker bookkeeping, never application data).
const TENANT_SCOPE_PAYLOAD_KEY: &str = "_tenant_id";
/// Vector distance metric provisioned for a new engine collection (cosine —
/// the same default the shared `vector_upsert_backend_target` seam ensures).
const ENGINE_VECTOR_DISTANCE: &str = "cosine";

/// Leader poll interval for the CDC freshness pass (env resolved ONCE).
pub(crate) fn search_freshness_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(SEARCH_FRESHNESS_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_SEARCH_FRESHNESS_INTERVAL_SECS),
        )
    })
}

/// Leader poll interval for the reindex/teardown pass (env resolved ONCE).
pub(crate) fn search_reindex_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(SEARCH_REINDEX_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_SEARCH_REINDEX_INTERVAL_SECS),
        )
    })
}

/// Postgres-backed `SearchService` handler.
pub struct SearchServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `search`).
    pg_pool: Option<PgPool>,
    /// Runtime handle for mediated index CRUD and mediated vector dispatch.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Project-active catalog: resolves the source entity's manifest table (and
    /// thus its tenant column) and the per-project search backend routing.
    catalog: Option<Arc<CatalogManager>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn search_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "search",
        operation,
        capability_required,
        message,
    )
}

fn full_text_only_requires_mediated_ir_status() -> Status {
    search_capability_status(
        "full_text_only_search",
        "mediated_ir_full_text_path",
        "full-text-only search requires the mediated IR full-text path (P2.2, pending); \
         supply query_vector for vector or hybrid search",
    )
}

fn search_index_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "search",
        operation,
        "search_index_not_found",
        "search index not found",
    )
}

impl SearchServiceImpl {
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

    /// Index state is durable-only: fail closed when no runtime dispatch exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            search_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "search service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    fn require_catalog(&self) -> Result<&CatalogManager, Status> {
        self.catalog.as_deref().ok_or_else(|| {
            search_capability_status(
                "catalog_lookup",
                "active_catalog",
                "search service requires the active catalog (no catalog configured)",
            )
        })
    }
}

impl Default for SearchServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn search_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    search_field_violation(field, description, message)
}

fn search_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn validate_create_index_required_fields(
    index_name: &str,
    source_message_type: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if index_name.trim().is_empty() {
        fields.push(("index_name", "must be a non-empty search index name"));
    }
    if source_message_type.trim().is_empty() {
        fields.push((
            "source_message_type",
            "must be a non-empty source message type",
        ));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "index_name and source_message_type are required",
            fields,
        ));
    }
    Ok(())
}

fn validate_search_query(req: &search_pb::SearchRequest) -> Result<(), Status> {
    let has_vector = !req.query_vector.is_empty();
    let has_text = !req.query_text.trim().is_empty();
    if !has_vector && !has_text {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "search requires query_text and/or query_vector",
            [
                ("query_text", "must be non-empty when query_vector is empty"),
                ("query_vector", "must be non-empty when query_text is empty"),
            ],
        ));
    }
    Ok(())
}

/// Clamp a requested `top_k` into `[1, MAX_TOP_K]`; non-positive → default.
fn resolve_top_k(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_TOP_K
    } else {
        requested.min(MAX_TOP_K)
    }
}

/// The fail-closed source-tenant-column gate. A SearchIndex MUST be scoped by the
/// source table's tenant column; if the shared catalog resolver returns none, the
/// source entity is not tenant-isolated and we refuse to register an index over
/// it (a Search would otherwise have no tenant predicate to inject). Pure —
/// unit-tested without a manifest.
fn require_source_tenant_column(
    resolved: Option<String>,
    message_type: &str,
) -> Result<String, Status> {
    match resolved.map(|column| column.trim().to_string()) {
        Some(column) if !column.is_empty() => Ok(column),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "source entity '{message_type}' has no resolvable tenant column; refusing to register a \
                 tenant-scoped search index over it (fail closed)"
            ),
            [(
                "source_message_type",
                "must resolve to a tenant-scoped source entity",
            )],
        )),
    }
}

/// Fail-closed tenant scope check for a CDC freshness event, mirroring the
/// tenant-scoped branch of `cdc::engine_tail::payload_value_matches_stream_scope`
/// (#8): a tenant-less or foreign-`tenant_id` payload is dropped, never indexed.
/// Reuses the public [`crate::runtime::cdc::tenant_scoped_topic`] seam to assert
/// the source stream is a tenant topic. Pure — unit-tested.
fn event_in_index_scope(topic: &str, payload: &serde_json::Value, tenant_scope: &str) -> bool {
    let tenant_scope = tenant_scope.trim();
    // An index is always tenant-bound; a missing scope can match nothing.
    if tenant_scope.is_empty() {
        return false;
    }
    let event_tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    // Fail closed: a tenant-less payload is never indexed into a tenant index,
    // even on a topic that is not itself tenant-scoped by prefix.
    if event_tenant.is_empty() {
        let _ = crate::runtime::cdc::tenant_scoped_topic(topic);
        return false;
    }
    event_tenant == tenant_scope
}

/// Pure reciprocal-rank fusion: `score(doc) = Σ_lists 1/(RRF_K + rank)`, where
/// `rank` is the doc's 0-based position in each ranked list it appears in. Used
/// to fuse the per-index ranked id lists when a Search spans several of the
/// tenant's indexes ("one search box over everything"). Returns `(id, score)`
/// sorted by score descending then id ascending for determinism. Reuses the same
/// `1/(60 + rank)` definition Qdrant applies internally for single-collection
/// hybrid search, so cross-index fusion is consistent with intra-index fusion.
fn reciprocal_rank_fusion(ranked_lists: &[Vec<String>]) -> Vec<(String, f64)> {
    use std::collections::HashMap;
    let mut scores: HashMap<String, f64> = HashMap::new();
    for list in ranked_lists {
        for (rank, id) in list.iter().enumerate() {
            *scores.entry(id.clone()).or_insert(0.0) += 1.0 / (RRF_K + rank as f64);
        }
    }
    let mut fused: Vec<(String, f64)> = scores.into_iter().collect();
    fused.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    fused
}

/// Build the server-side tenant `must`/term filter as a protobuf `Struct` from
/// the VERIFIED claim tenant. Stamped onto every engine query so a caller can
/// never widen past their tenant. The `_tenant_id` payload key is the same one
/// the Qdrant IR compiler stamps at write time and ANDs into the `must` clause;
/// the Elasticsearch executor ANDs the equivalent term.
fn tenant_scope_filter(tenant_id: &str) -> Option<prost_types::Struct> {
    let body = serde_json::json!({
        "must": [
            { "key": TENANT_SCOPE_PAYLOAD_KEY, "match": { "value": tenant_id } }
        ]
    });
    crate::runtime::executor_utils::json_to_struct(&body)
}

/// Serialize a returned engine payload into the hit's `payload_json`, stripping
/// the broker-stamped tenant-scope key ([`TENANT_SCOPE_PAYLOAD_KEY`]) — it is
/// write-time bookkeeping, never application data. Empty/absent payloads map to
/// an empty string. Pure — unit-tested.
fn hit_payload_json(payload: Option<&prost_types::Struct>) -> String {
    let Some(payload) = payload else {
        return String::new();
    };
    let mut value = crate::runtime::executor_utils::struct_to_json(payload);
    let Some(object) = value.as_object_mut() else {
        return String::new();
    };
    object.remove(TENANT_SCOPE_PAYLOAD_KEY);
    if object.is_empty() {
        return String::new();
    }
    value.to_string()
}

/// Engine collection an index queries/writes: the explicit resource name, or
/// the index name when unset. ONE resolution shared by query, freshness,
/// reindex, teardown, and provisioning.
fn collection_name(resource_name: &str, index_name: &str) -> String {
    if resource_name.trim().is_empty() {
        index_name.to_string()
    } else {
        resource_name.to_string()
    }
}

/// A registered index decoded from the native read JSON.
pub(crate) struct StoredIndex {
    index_id: String,
    index_name: String,
    source_message_type: String,
    backend: String,
    resource_name: String,
    vector_dims: i32,
    tenant_column: String,
    source_cdc_topic: String,
    status: String,
}

impl StoredIndex {
    fn collection(&self) -> String {
        collection_name(&self.resource_name, &self.index_name)
    }
}

fn index_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
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
        _ => String::new(),
    }
}

fn json_i32(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i32 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) as i32,
        Some(serde_json::Value::String(value)) => value.trim().parse::<i32>().unwrap_or(0),
        _ => 0,
    }
}

fn stored_index_from_json(row: &serde_json::Value) -> StoredIndex {
    let map = index_json_object(row);
    StoredIndex {
        index_id: json_str(map, "index_id"),
        index_name: json_str(map, "index_name"),
        source_message_type: json_str(map, "source_message_type"),
        backend: json_str(map, "backend"),
        resource_name: json_str(map, "resource_name"),
        vector_dims: json_i32(map, "vector_dims"),
        tenant_column: json_str(map, "tenant_column"),
        source_cdc_topic: json_str(map, "source_cdc_topic"),
        status: json_str(map, "status"),
    }
}

fn index_filter(tenant_id: &str, index_name: Option<&str>, status: Option<&str>) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(index_name) = index_name {
        filters.push(LogicalFilter::Comparison {
            field: "index_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(index_name),
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

fn index_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "index_id".to_string(),
        "index_name".to_string(),
        "source_message_type".to_string(),
        "backend".to_string(),
        "resource_name".to_string(),
        "vector_dims".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
    ])
}

fn index_read_by_name(tenant_id: &str, index_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: SEARCH_INDEX_MSG.to_string(),
        filter: Some(index_filter(tenant_id, Some(index_name), None)),
        projection: Some(index_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn active_indexes_read(tenant_id: &str, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: SEARCH_INDEX_MSG.to_string(),
        filter: Some(index_filter(tenant_id, None, Some(STATUS_ACTIVE))),
        projection: Some(index_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
fn index_record(
    index_id: &str,
    tenant_id: &str,
    index_name: &str,
    source_message_type: &str,
    backend: &str,
    resource_name: &str,
    vector_dims: i32,
    tenant_column: &str,
    source_cdc_topic: &str,
    status: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("index_id".to_string(), logical_string(index_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("index_name".to_string(), logical_string(index_name));
    record.insert(
        "source_message_type".to_string(),
        logical_string(source_message_type),
    );
    record.insert("backend".to_string(), logical_string(backend));
    record.insert("resource_name".to_string(), logical_string(resource_name));
    record.insert(
        "vector_dims".to_string(),
        LogicalValue::Int(i64::from(vector_dims)),
    );
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
/// conflict target is the message primary key (`index_id`), reused for an
/// existing (tenant, index_name) row so a re-register never violates the unique
/// index.
fn index_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "source_message_type".to_string(),
        "backend".to_string(),
        "resource_name".to_string(),
        "vector_dims".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
        "updated_at".to_string(),
        "metadata_json".to_string(),
    ])
}

impl SearchServiceImpl {
    /// Resolve the SOURCE entity's tenant column through the project-active
    /// manifest using the shared catalog resolver. Returns the column name or a
    /// fail-closed error when the source is unknown or not tenant-isolated.
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
                search_field_violation(
                    "source_message_type",
                    "must be present in the active catalog manifest",
                    format!(
                        "source_message_type '{source_message_type}' is not present in the active catalog \
                         manifest"
                    ),
                )
            },
        )?;
        // SHARED resolver (no duplicate): same `util::resolve_tenant_column` family
        // behind `native_catalog::NativeModel::tenant_column`.
        let resolved = crate::runtime::postgres_helpers::tenant_column_ref(table)
            .map(|column| column.column_name.clone());
        let tenant_column = require_source_tenant_column(resolved, source_message_type)?;
        let cdc_topic = table.cdc_topic.clone();
        Ok((tenant_column, cdc_topic))
    }

    /// Emit a per-mutation versioned dot-topic outbox event (best-effort).
    async fn emit_index_event(
        &self,
        topic: &str,
        tenant_id: &str,
        project_id: &str,
        index_name: &str,
        extra: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let mut payload = serde_json::json!({
            "tenant_id": tenant_id,
            "project_id": project_id,
            "index_name": index_name,
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
            index_name,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: index_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }

    /// Query one index through the runtime's mediated vector dispatch, returning
    /// the ranked points (ids + engine payloads). The tenant filter is injected
    /// server-side from the verified claim; this never hand-builds a raw engine
    /// query. Returns an empty list for an index whose backend is not
    /// query-wired here.
    async fn query_one_index(
        &self,
        runtime: &DataBrokerRuntime,
        manifest: &crate::generation::CatalogManifest,
        context: &crate::RequestContext,
        index: &StoredIndex,
        req: &search_pb::SearchRequest,
        top_k: i32,
        tenant_filter: Option<prost_types::Struct>,
    ) -> Result<Vec<VectorPoint>, Status> {
        validate_search_query(req)?;
        let has_vector = !req.query_vector.is_empty();
        let has_text = !req.query_text.trim().is_empty();
        // Lexical relevance rides the hybrid dispatch (Qdrant's `_full_text`
        // prefetch + RRF), which requires a query vector. A pure text-only query
        // with no vector has no reachable mediated full-text path in this build
        // (P2.2): fail closed rather than silently route an empty vector.
        if has_text && !has_vector {
            return Err(full_text_only_requires_mediated_ir_status());
        }
        let collection = index.collection();
        // Hybrid (text + vector): the runtime's mediated hybrid dispatch applies
        // Qdrant native RRF; the tenant filter is injected server-side.
        let result: VectorSet = if has_text && has_vector {
            let request = VectorHybridSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                text_query: req.query_text.clone(),
                filter: tenant_filter,
                limit: top_k,
                fusion_weights: Vec::new(),
                with_payload: true,
            };
            runtime
                .vector_hybrid_search(manifest, request, context.clone())
                .await?
        } else {
            // Vector-only: the mediated vector dispatch routes to the index backend
            // (Qdrant `/points/search`, or the Elasticsearch executor for an
            // ES-backed index), with the tenant filter injected server-side.
            let request = VectorSearchRequest {
                context: None,
                collection,
                vector: req.query_vector.clone(),
                filter: tenant_filter,
                limit: top_k,
                score_threshold: 0.0,
                with_payload: true,
            };
            runtime
                .vector_search(manifest, request, context.clone())
                .await?
        };
        Ok(result.points)
    }
}

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn create_index(
        &self,
        request: Request<search_pb::CreateIndexRequest>,
    ) -> Result<Response<search_pb::CreateIndexResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header before any catalog/store access.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let index_name = req.index_name.trim().to_string();
        let source_message_type = req.source_message_type.trim().to_string();
        let backend = req.backend.trim().to_ascii_lowercase();
        validate_create_index_required_fields(&index_name, &source_message_type)?;
        if backend != BACKEND_QDRANT && backend != BACKEND_ELASTICSEARCH {
            return Err(search_field_violation(
                "backend",
                format!("must be '{BACKEND_QDRANT}' or '{BACKEND_ELASTICSEARCH}'"),
                format!(
                    "unsupported search backend '{backend}' (expected '{BACKEND_QDRANT}' or \
                     '{BACKEND_ELASTICSEARCH}')"
                ),
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "search",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        // FAIL CLOSED: resolve the source table's tenant column via the shared
        // catalog resolver. No tenant column ⇒ no index.
        let (tenant_column, source_cdc_topic) =
            self.resolve_source_tenant_column(&context.project_id, &source_message_type)?;

        // Per-tenant index quota (only a brand-new index counts).
        let existing = runtime
            .native_entity_read_for_service(
                "search",
                &context,
                index_read_by_name(&tenant_id, &index_name),
            )
            .await?
            .first()
            .map(stored_index_from_json);
        if existing.is_none() {
            let active = runtime
                .native_entity_read_for_service(
                    "search",
                    &context,
                    active_indexes_read(&tenant_id, 0, (MAX_INDEXES_PER_TENANT as u32) + 1),
                )
                .await?;
            if active.len() >= MAX_INDEXES_PER_TENANT {
                return Err(crate::runtime::executor_utils::quota_refusal_status(
                    "search",
                    "tenant search-index quota",
                    format!("tenant search-index quota exhausted ({MAX_INDEXES_PER_TENANT})"),
                ));
            }
        }

        let resource_name = req.resource_name.trim().to_string();

        // Engine lifecycle (provision on create): a brand-new vector index
        // ensures its backing engine resource up front through the runtime's
        // EXISTING resource-admin seam (`ensure_resource_backend_target` →
        // Qdrant `ensure_collection` / Elasticsearch index PUT) — never a
        // parallel engine client. FAIL CLOSED: an unregistered or unreachable
        // backend rejects the registration instead of recording a capability
        // lie. Re-registration of an existing index skips the ensure (the
        // resource already exists, and the Elasticsearch index PUT is not
        // idempotent); a full-text-only index (vector_dims == 0) is provisioned
        // engine-side on first write.
        if existing.is_none() && req.vector_dims > 0 {
            let engine_spec = serde_json::json!({
                "dimension": req.vector_dims,
                "distance": ENGINE_VECTOR_DISTANCE,
            })
            .to_string();
            runtime
                .ensure_resource_backend_target(
                    &backend,
                    None,
                    &collection_name(&resource_name, &index_name),
                    &engine_spec,
                )
                .await?;
        }

        let index_id = existing
            .as_ref()
            .map(|row| row.index_id.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let metadata_json = non_empty_json(&req.metadata_json);

        runtime
            .native_entity_write_for_service(
                "search",
                &context,
                SEARCH_INDEX_MSG,
                index_record(
                    &index_id,
                    &tenant_id,
                    &index_name,
                    &source_message_type,
                    &backend,
                    &resource_name,
                    req.vector_dims,
                    &tenant_column,
                    &source_cdc_topic,
                    STATUS_ACTIVE,
                    &metadata_json,
                ),
                index_conflict(),
            )
            .await?;

        self.emit_index_event(
            TOPIC_CREATED,
            &tenant_id,
            &context.project_id,
            &index_name,
            serde_json::json!({
                "source_message_type": source_message_type,
                "backend": backend,
                "tenant_column": tenant_column,
            }),
        )
        .await;

        Ok(Response::new(search_pb::CreateIndexResponse {
            index_id,
            index_name,
            tenant_column,
            message: "search index registered".to_string(),
            error: None,
        }))
    }

    async fn delete_index(
        &self,
        request: Request<search_pb::DeleteIndexRequest>,
    ) -> Result<Response<search_pb::DeleteIndexResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let index_name = req.index_name.trim().to_string();
        if index_name.is_empty() {
            return Err(search_required_field(
                "index_name",
                "must be a non-empty search index name",
                "index_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "search",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let stored = runtime
            .native_entity_read_for_service(
                "search",
                &context,
                index_read_by_name(&tenant_id, &index_name),
            )
            .await?
            .first()
            .map(stored_index_from_json);
        let Some(stored) = stored.filter(|row| row.status != STATUS_DELETED) else {
            // Idempotent: nothing to delete.
            return Ok(Response::new(search_pb::DeleteIndexResponse {
                deleted: true,
                message: "search index not found".to_string(),
                error: None,
            }));
        };

        runtime
            .native_entity_write_for_service(
                "search",
                &context,
                SEARCH_INDEX_MSG,
                index_record(
                    &stored.index_id,
                    &tenant_id,
                    &stored.index_name,
                    &stored.source_message_type,
                    &stored.backend,
                    &stored.resource_name,
                    stored.vector_dims,
                    &stored.tenant_column,
                    &stored.source_cdc_topic,
                    STATUS_DELETED,
                    "{}",
                ),
                index_conflict(),
            )
            .await?;

        // Tear down on delete: this deleted-event IS the teardown job — the
        // leader-owned `run_search_reindex_once` pass consumes it, purges the
        // index's tenant-scoped engine points through the existing per-point
        // vector delete seam, and emits `TOPIC_TEARDOWN_COMPLETED` when done.
        self.emit_index_event(
            TOPIC_DELETED,
            &tenant_id,
            &context.project_id,
            &index_name,
            serde_json::json!({
                "backend": stored.backend,
                "resource_name": stored.resource_name,
                "source_message_type": stored.source_message_type,
            }),
        )
        .await;

        Ok(Response::new(search_pb::DeleteIndexResponse {
            deleted: true,
            message: "search index deleted".to_string(),
            error: None,
        }))
    }

    async fn list_indexes(
        &self,
        request: Request<search_pb::ListIndexesRequest>,
    ) -> Result<Response<search_pb::ListIndexesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "search",
            OperationChannel::Admin,
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
            MAX_INDEXES_PER_TENANT as i32,
        );

        let rows = runtime
            .native_entity_read_for_service(
                "search",
                &context,
                active_indexes_read(
                    &tenant_id,
                    page_window.offset as u64,
                    (page_window.limit as u32).min(MAX_INDEXES_PER_TENANT as u32),
                ),
            )
            .await?;
        let indexes = rows
            .iter()
            .map(stored_index_from_json)
            .map(|index| search_pb::SearchIndexSummary {
                index_id: index.index_id,
                index_name: index.index_name,
                source_message_type: index.source_message_type,
                backend: index.backend,
                resource_name: index.resource_name,
                vector_dims: index.vector_dims,
                status: index.status,
            })
            .collect::<Vec<_>>();
        let next_page_token =
            native_next_page_token(page_window.offset, page_window.limit, indexes.len());

        Ok(Response::new(search_pb::ListIndexesResponse {
            indexes,
            message: "ok".to_string(),
            error: None,
            next_page_token,
        }))
    }

    async fn search(
        &self,
        request: Request<search_pb::SearchRequest>,
    ) -> Result<Response<search_pb::SearchResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "search",
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

        // Resolve the target index(es): a single named index, or all of the
        // tenant's active indexes when none is named ("one search box").
        let target_name = req.index_name.trim();
        let targets: Vec<StoredIndex> = if target_name.is_empty() {
            runtime
                .native_entity_read_for_service(
                    "search",
                    &context,
                    active_indexes_read(&tenant_id, 0, MAX_INDEXES_PER_TENANT as u32),
                )
                .await?
                .iter()
                .map(stored_index_from_json)
                .collect()
        } else {
            runtime
                .native_entity_read_for_service(
                    "search",
                    &context,
                    index_read_by_name(&tenant_id, target_name),
                )
                .await?
                .first()
                .map(stored_index_from_json)
                .filter(|index| index.status == STATUS_ACTIVE)
                .into_iter()
                .collect()
        };
        if targets.is_empty() {
            return Ok(Response::new(search_pb::SearchResponse {
                hits: Vec::new(),
                message: "no matching index".to_string(),
                error: None,
                next_page_token: String::new(),
            }));
        }

        // SERVER-SIDE tenant filter built from the VERIFIED claim, injected into
        // every engine query (Qdrant `must` / ES term). Never from the body.
        let tenant_filter = tenant_scope_filter(&tenant_id);
        let state = catalog.active_for(&context.project_id);
        let manifest = &state.manifest;

        // Query each target through the mediated dispatch, then fuse the ranked
        // id lists across indexes with pure RRF. Per hit id the first-seen
        // (index name, tenant-stripped payload_json) pair is kept for mapping.
        let mut ranked_lists: Vec<Vec<String>> = Vec::with_capacity(targets.len());
        let mut hit_meta: std::collections::HashMap<String, (String, String)> =
            std::collections::HashMap::new();
        for index in &targets {
            let points = self
                .query_one_index(
                    runtime,
                    manifest,
                    &context,
                    index,
                    &req,
                    top_k,
                    tenant_filter.clone(),
                )
                .await?;
            let mut ids = Vec::with_capacity(points.len());
            for point in points {
                hit_meta.entry(point.id.clone()).or_insert_with(|| {
                    (
                        index.index_name.clone(),
                        hit_payload_json(point.payload.as_ref()),
                    )
                });
                ids.push(point.id);
            }
            ranked_lists.push(ids);
        }

        let fused = reciprocal_rank_fusion(&ranked_lists);
        let requested_page_size = if req.page_size > 0 {
            req.page_size
        } else {
            top_k
        };
        let page_window =
            native_offset_page_window(1, requested_page_size, &req.page_token, top_k.max(1));
        let hits = fused
            .into_iter()
            .skip(page_window.offset)
            .take(page_window.limit.min(top_k as usize))
            .map(|(id, score)| {
                let (index_name, payload_json) = hit_meta.get(&id).cloned().unwrap_or_default();
                search_pb::SearchHit {
                    index_name,
                    id,
                    score,
                    payload_json,
                }
            })
            .collect::<Vec<_>>();
        let next_page_token = native_next_page_token(
            page_window.offset,
            page_window.limit.min(top_k as usize),
            hits.len(),
        );

        Ok(Response::new(search_pb::SearchResponse {
            hits,
            message: "ok".to_string(),
            error: None,
            next_page_token,
        }))
    }

    async fn reindex(
        &self,
        request: Request<search_pb::ReindexRequest>,
    ) -> Result<Response<search_pb::ReindexResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let index_name = req.index_name.trim().to_string();
        if index_name.is_empty() {
            return Err(search_required_field(
                "index_name",
                "must be a non-empty search index name",
                "index_name is required",
            ));
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "search",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let stored = runtime
            .native_entity_read_for_service(
                "search",
                &context,
                index_read_by_name(&tenant_id, &index_name),
            )
            .await?
            .first()
            .map(stored_index_from_json)
            .filter(|index| index.status != STATUS_DELETED)
            .ok_or_else(|| search_index_not_found_status("reindex"))?;

        // Mark REINDEXING and emit the work event. The backfill loop (which reads
        // source rows ONLY through the mediated IR path) runs in the leader-owned
        // `run_search_reindex_once` pass, which restores ACTIVE and emits
        // `TOPIC_REINDEX_COMPLETED`; this RPC just admits the request idempotently.
        runtime
            .native_entity_write_for_service(
                "search",
                &context,
                SEARCH_INDEX_MSG,
                index_record(
                    &stored.index_id,
                    &tenant_id,
                    &stored.index_name,
                    &stored.source_message_type,
                    &stored.backend,
                    &stored.resource_name,
                    stored.vector_dims,
                    &stored.tenant_column,
                    &stored.source_cdc_topic,
                    STATUS_REINDEXING,
                    "{}",
                ),
                index_conflict(),
            )
            .await?;

        let reindex_id = Uuid::new_v4().to_string();
        self.emit_index_event(
            TOPIC_REINDEX,
            &tenant_id,
            &context.project_id,
            &index_name,
            serde_json::json!({
                "reindex_id": reindex_id,
                "source_message_type": stored.source_message_type,
            }),
        )
        .await;

        Ok(Response::new(search_pb::ReindexResponse {
            reindex_id,
            accepted: true,
            message: "reindex requested".to_string(),
            error: None,
        }))
    }
}

// ── Leader-owned background passes (freshness / reindex / teardown) ──────────
//
// All three passes follow the house journal-consumer pattern (cache
// invalidation, embedding work emitter): read published events from the durable
// CDC journal, join them to this service's own registrations, dedup by an
// output event carrying the source journal event id, and act ONLY through the
// mediated runtime seams. They are spawned by the leader via
// `NativeWorkerHost::spawn_while_leader` (never a bare per-replica loop).

/// The search-index registration as a native SQL model (relation + quoted
/// columns), resolved from the embedded native manifest — never a hand-mapped
/// schema.
fn search_index_model() -> NativeModel {
    native_model(
        SEARCH_INDEX_MSG,
        &[
            "index_id",
            "tenant_id",
            "index_name",
            "source_message_type",
            "backend",
            "resource_name",
            "vector_dims",
            "tenant_column",
            "source_cdc_topic",
            "status",
        ],
    )
}

/// The `s.<col> AS <name>` select list shared by all three job loaders.
fn stored_index_columns_sql(model: &NativeModel) -> String {
    format!(
        "s.{index_id}::TEXT AS index_id, \
         s.{index_name}::TEXT AS index_name, \
         s.{source_message_type}::TEXT AS source_message_type, \
         s.{backend}::TEXT AS backend, \
         s.{resource_name}::TEXT AS resource_name, \
         s.{vector_dims} AS vector_dims, \
         s.{tenant_column}::TEXT AS tenant_column, \
         s.{source_cdc_topic}::TEXT AS source_cdc_topic, \
         s.{status}::TEXT AS status",
        index_id = model.q("index_id"),
        index_name = model.q("index_name"),
        source_message_type = model.q("source_message_type"),
        backend = model.q("backend"),
        resource_name = model.q("resource_name"),
        vector_dims = model.q("vector_dims"),
        tenant_column = model.q("tenant_column"),
        source_cdc_topic = model.q("source_cdc_topic"),
        status = model.q("status"),
    )
}

fn stored_index_from_pg_row(row: &sqlx::postgres::PgRow) -> Result<StoredIndex, String> {
    Ok(StoredIndex {
        index_id: row
            .try_get("index_id")
            .map_err(|err| format!("decode search index id failed: {err}"))?,
        index_name: row
            .try_get("index_name")
            .map_err(|err| format!("decode search index name failed: {err}"))?,
        source_message_type: row
            .try_get("source_message_type")
            .map_err(|err| format!("decode search source message type failed: {err}"))?,
        backend: row
            .try_get("backend")
            .map_err(|err| format!("decode search index backend failed: {err}"))?,
        resource_name: row
            .try_get("resource_name")
            .map_err(|err| format!("decode search resource name failed: {err}"))?,
        vector_dims: row
            .try_get("vector_dims")
            .map_err(|err| format!("decode search vector dims failed: {err}"))?,
        tenant_column: row
            .try_get("tenant_column")
            .map_err(|err| format!("decode search tenant column failed: {err}"))?,
        source_cdc_topic: row
            .try_get("source_cdc_topic")
            .map_err(|err| format!("decode search source cdc topic failed: {err}"))?,
        status: row
            .try_get("status")
            .map_err(|err| format!("decode search index status failed: {err}"))?,
    })
}

/// A journal CDC event joined to the ACTIVE index registered over its topic.
struct SearchFreshnessJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
    payload: serde_json::Value,
}

/// A `reindex.requested` journal event joined to its (non-deleted) index row.
struct SearchReindexJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
    reindex_id: String,
}

/// An `index.deleted` journal event joined to its DELETED index row.
struct SearchTeardownJob {
    index: StoredIndex,
    event_id: String,
    tenant_id: String,
    project_id: String,
}

/// Freshness candidates: published source-topic CDC events for ACTIVE indexes
/// that carry a dense embedding, minus events already marked applied. The
/// journal may ENVELOPE-NEST the emitted payload under `payload.payload`, so
/// every payload key is read at BOTH levels (`COALESCE(top, nested)`).
/// Binds: `$1` = active status, `$2` = applied-marker topic, `$3` = limit.
fn freshness_jobs_sql(
    model: &NativeModel,
    journal_relation: &str,
    outbox_relation: &str,
) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{status} = $1 \
          AND COALESCE(s.{source_cdc_topic}::TEXT, '') <> '' \
          AND s.{source_cdc_topic} = j.topic \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND (jsonb_typeof(j.payload->'vector') = 'array' \
                OR jsonb_typeof(j.payload->'payload'->'vector') = 'array') \
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
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        status = model.q("status"),
        source_cdc_topic = model.q("source_cdc_topic"),
    )
}

/// Reindex jobs: `reindex.requested` events joined to their non-deleted index
/// row, minus requests that already produced a `reindex.completed` marker
/// (keyed `reindex_event_id`). Binds: `$1` = deleted status (excluded),
/// `$2` = requested topic, `$3` = completed topic, `$4` = limit.
fn reindex_jobs_sql(model: &NativeModel, journal_relation: &str, outbox_relation: &str) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id, \
            COALESCE(j.payload->>'reindex_id', j.payload->'payload'->>'reindex_id', '') AS reindex_id \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{index_name}::TEXT = COALESCE(j.payload->>'index_name', j.payload->'payload'->>'index_name', '') \
          AND s.{status} <> $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND COALESCE(j.payload->>'reindex_id', j.payload->'payload'->>'reindex_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $3 \
                 AND COALESCE(o.payload->>'reindex_event_id', o.payload->'payload'->>'reindex_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'reindex_event_id', done.payload->'payload'->>'reindex_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        index_name = model.q("index_name"),
        status = model.q("status"),
    )
}

/// Teardown jobs: `index.deleted` events joined to their DELETED index row,
/// minus deletions that already produced a `teardown.completed` marker (keyed
/// `teardown_event_id`). Binds: `$1` = deleted status, `$2` = deleted topic,
/// `$3` = completed topic, `$4` = limit.
fn teardown_jobs_sql(model: &NativeModel, journal_relation: &str, outbox_relation: &str) -> String {
    format!(
        "SELECT {cols}, \
            j.event_id::TEXT AS event_id, \
            COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') AS event_tenant_id, \
            COALESCE(j.payload->>'project_id', j.payload->'payload'->>'project_id', '') AS project_id \
         FROM {journal_relation} j \
         JOIN {relation} s \
           ON s.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') \
          AND s.{index_name}::TEXT = COALESCE(j.payload->>'index_name', j.payload->'payload'->>'index_name', '') \
          AND s.{status} = $1 \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic = $2 \
           AND COALESCE(j.payload->>'tenant_id', j.payload->'payload'->>'tenant_id', '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {outbox_relation} o \
               WHERE o.topic = $3 \
                 AND COALESCE(o.payload->>'teardown_event_id', o.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM {journal_relation} done \
               WHERE done.topic = $3 \
                 AND COALESCE(done.payload->>'teardown_event_id', done.payload->'payload'->>'teardown_event_id') = j.event_id::TEXT \
           ) \
         ORDER BY j.published_at ASC, j.event_id ASC \
         LIMIT $4",
        cols = stored_index_columns_sql(model),
        relation = model.relation,
        tenant_id = model.q("tenant_id"),
        index_name = model.q("index_name"),
        status = model.q("status"),
    )
}

async fn load_search_freshness_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchFreshnessJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&freshness_jobs_sql(
        &model,
        journal_relation,
        outbox_relation,
    ))
    .bind(STATUS_ACTIVE)
    .bind(TOPIC_FRESHNESS_APPLIED)
    .bind(batch.max(1))
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load search freshness jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode search freshness payload failed: {err}"))?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode search freshness payload JSON failed: {err}"))?;
        jobs.push(SearchFreshnessJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search freshness event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search freshness tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search freshness project failed: {err}"))?,
            payload,
        });
    }
    Ok(jobs)
}

async fn load_search_reindex_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchReindexJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&reindex_jobs_sql(&model, journal_relation, outbox_relation))
        .bind(STATUS_DELETED)
        .bind(TOPIC_REINDEX)
        .bind(TOPIC_REINDEX_COMPLETED)
        .bind(batch.max(1))
        .fetch_all(pool)
        .await
        .map_err(|err| format!("load search reindex jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(SearchReindexJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search reindex event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search reindex tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search reindex project failed: {err}"))?,
            reindex_id: row
                .try_get("reindex_id")
                .map_err(|err| format!("decode search reindex id failed: {err}"))?,
        });
    }
    Ok(jobs)
}

async fn load_search_teardown_jobs(
    pool: &PgPool,
    journal_relation: &str,
    outbox_relation: &str,
    batch: i64,
) -> Result<Vec<SearchTeardownJob>, String> {
    let model = search_index_model();
    let rows = sqlx::query(&teardown_jobs_sql(
        &model,
        journal_relation,
        outbox_relation,
    ))
    .bind(STATUS_DELETED)
    .bind(TOPIC_DELETED)
    .bind(TOPIC_TEARDOWN_COMPLETED)
    .bind(batch.max(1))
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load search teardown jobs failed: {err}"))?;
    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        jobs.push(SearchTeardownJob {
            index: stored_index_from_pg_row(&row)?,
            event_id: row
                .try_get("event_id")
                .map_err(|err| format!("decode search teardown event id failed: {err}"))?,
            tenant_id: row
                .try_get("event_tenant_id")
                .map_err(|err| format!("decode search teardown tenant failed: {err}"))?,
            project_id: row
                .try_get("project_id")
                .map_err(|err| format!("decode search teardown project failed: {err}"))?,
        });
    }
    Ok(jobs)
}

/// Worker-side request context for mediated reads/writes performed by the
/// leader passes (mirrors the embedding backfill's `backfill_read_context`).
fn search_worker_context(
    tenant_id: &str,
    project_id: &str,
    purpose: &str,
) -> crate::RequestContext {
    crate::RequestContext {
        tenant_id: tenant_id.to_string(),
        project_id: project_id.to_string(),
        purpose: purpose.to_string(),
        scopes: vec!["udb:read".to_string()],
        service_identity: "udb.search.worker".to_string(),
        ..crate::RequestContext::default()
    }
}

/// Journal rows may ENVELOPE-NEST the emitted payload under `payload.payload`
/// (the CDC journal envelope); read the nested level when the top level lacks
/// the freshness keys. Pure — unit-tested.
fn freshness_event_body(payload: &serde_json::Value) -> &serde_json::Value {
    if payload.get("vector").is_some() || payload.get("id").is_some() {
        return payload;
    }
    payload.get("payload").unwrap_or(payload)
}

/// Extract a dense embedding from a freshness event body or a source row. Only
/// a change that already carries its embedding (`vector`, or an `embedding`
/// column on a source row — e.g. reported by an embedding sidecar, master-plan
/// 9.11) is upserted; pure-text freshness for an ES-backed index is
/// engine-side. Pure — unit-tested.
fn payload_vector(body: &serde_json::Value) -> Option<Vec<f32>> {
    ["vector", "embedding"].iter().find_map(|key| {
        body.get(*key)
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_f64().map(|number| number as f32))
                    .collect::<Vec<f32>>()
            })
            .filter(|vector| !vector.is_empty())
    })
}

/// Pure status/metadata writeback for a finished reindex pass. BOTH outcomes
/// restore `ACTIVE` — an index is NEVER stranded in `REINDEXING` (the
/// active-only Search/List filter would make it vanish). The SearchIndex entity
/// has no dedicated error column, so a failure records `last_error` in the
/// mutable `metadata_json` column (which the Reindex RPC already resets).
fn reindex_writeback(error: Option<&str>) -> (&'static str, String) {
    match error {
        None => (STATUS_ACTIVE, "{}".to_string()),
        Some(error) => (
            STATUS_ACTIVE,
            serde_json::json!({ "last_error": error }).to_string(),
        ),
    }
}

/// The engine point payload for a re-upserted source row: the full row minus
/// its dense-vector column(s). The mediated `vector_upsert` stamps
/// [`TENANT_SCOPE_PAYLOAD_KEY`] itself at write time.
fn reindex_point_payload(row: &serde_json::Value) -> Option<prost_types::Struct> {
    let mut object = row.as_object().cloned().unwrap_or_default();
    for key in ["vector", "embedding"] {
        object.remove(key);
    }
    crate::runtime::executor_utils::json_to_struct(&serde_json::Value::Object(object))
}

/// Mediated tenant-scoped SELECT over an index's source entity, cursor-paged on
/// the primary key (mirrors the embedding backfill's `backfill_select_request`,
/// including the project-isolation predicate the served planner requires).
fn source_rows_select_request(
    index: &StoredIndex,
    tenant_id: &str,
    primary_key: &str,
    fields: Vec<String>,
    after_pk: Option<&str>,
    project_column: Option<&str>,
    project_id: &str,
) -> SelectRequest {
    let mut tenant_filter = serde_json::Map::new();
    tenant_filter.insert(
        index.tenant_column.clone(),
        serde_json::Value::String(tenant_id.to_string()),
    );
    let mut filters = vec![serde_json::Value::Object(tenant_filter)];
    // Project isolation: the data-plane planner rejects a select over a
    // project-scoped source table unless the filter carries the project column.
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
        message_type: index.source_message_type.clone(),
        filter: crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            "$and": filters,
        })),
        fields,
        limit: SEARCH_REINDEX_PAGE_LIMIT,
        sort: vec![Sort {
            field: primary_key.to_string(),
            descending: false,
        }],
        ..SelectRequest::default()
    }
}

/// Resolve the source entity's manifest table + primary key for a worker job.
fn source_table_and_pk<'a>(
    manifest: &'a crate::generation::CatalogManifest,
    source_message_type: &str,
) -> Result<(&'a crate::generation::ManifestTable, String), String> {
    let table =
        crate::broker::table_for_message(manifest, source_message_type).ok_or_else(|| {
            format!("search source entity '{source_message_type}' is not present in active catalog")
        })?;
    let primary_key = table.primary_key.first().cloned().ok_or_else(|| {
        format!("search source entity '{source_message_type}' has no primary key")
    })?;
    Ok((table, primary_key))
}

/// Apply one in-scope CDC freshness event through the runtime's MEDIATED
/// `vector_upsert` — the same mediated vector dispatch family `Search` queries
/// — never a raw engine write. Returns `Some(true)` when a point was upserted,
/// `Some(false)` for a terminal skip (out-of-scope / no id / no vector —
/// retrying cannot change it, so the applied marker is still written), and
/// `None` for a transient failure (no marker: the next leader pass retries).
async fn apply_search_freshness_job(
    service: &SearchServiceImpl,
    job: &SearchFreshnessJob,
) -> Option<bool> {
    let (Some(runtime), Some(catalog)) = (service.runtime.as_deref(), service.catalog.as_deref())
    else {
        // Capability missing: leave the event unmarked so a later, fully wired
        // pass picks it up (fail closed, never silently drop).
        return None;
    };
    let body = freshness_event_body(&job.payload);
    // Fail closed: a missing/foreign tenant_id payload is dropped, not indexed.
    if !event_in_index_scope(&job.index.source_cdc_topic, body, &job.tenant_id) {
        return Some(false);
    }
    let Some(vector) = payload_vector(body) else {
        return Some(false);
    };
    let id = body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    if id.is_empty() {
        return Some(false);
    }
    let state = catalog.active_for(&job.project_id);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_freshness");
    let request = VectorUpsertRequest {
        context: None,
        collection: job.index.collection(),
        points: vec![VectorPointMutation {
            id,
            vector,
            payload: body
                .get("payload")
                .and_then(|value| crate::runtime::executor_utils::json_to_struct(value)),
        }],
        idempotency_key: String::new(),
    };
    if let Err(err) = runtime
        .vector_upsert(&state.manifest, request, context)
        .await
    {
        tracing::warn!(
            index = %job.index.index_name,
            error = %err,
            "search index freshness upsert failed; will retry on next pass"
        );
        return None;
    }
    Some(true)
}

/// Per-index CDC freshness consumer (master-plan 9.5) — ONE leader-owned pass
/// over the durable CDC journal. Published events on an ACTIVE index's source
/// topic are joined tenant-scoped, dropped when outside the index's tenant
/// scope ([`event_in_index_scope`], fail closed), and an in-scope
/// embedding-carrying change is upserted through the runtime's MEDIATED vector
/// dispatch. Each consumed event is marked with a
/// [`TOPIC_FRESHNESS_APPLIED`] outbox event keyed by `source_event_id`, so a
/// pass never re-consumes the journal. The leader spawns this via
/// `NativeWorkerHost::spawn_while_leader` (the `run_cache_invalidation_worker_once`
/// shape). Returns the number of points upserted.
pub(crate) async fn run_index_freshness_consumer(
    service: Arc<SearchServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "search freshness consumer requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "search freshness consumer requires transactional outbox".to_string())?;
    let jobs = load_search_freshness_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut applied = 0i64;
    for job in &jobs {
        let Some(was_applied) = apply_search_freshness_job(&service, job).await else {
            // Transient failure: no applied marker, retried on the next pass.
            continue;
        };
        if was_applied {
            applied = applied.saturating_add(1);
        }
        service
            .emit_index_event(
                TOPIC_FRESHNESS_APPLIED,
                &job.tenant_id,
                &job.project_id,
                &job.index.index_name,
                serde_json::json!({
                    "source_event_id": job.event_id,
                    "applied": was_applied,
                }),
            )
            .await;
    }
    Ok(applied)
}

/// Page every tenant-scoped source row through the served mediated SELECT and
/// re-upsert each dense-vector row into the engine through the SAME mediated
/// `vector_upsert` dispatch family `Search` queries — never a raw engine write.
/// Rows without a `vector`/`embedding` column are skipped (a full-text-only
/// index has no dense vector to rebuild here; its content is engine-side).
async fn reindex_source_rows(
    runtime: &DataBrokerRuntime,
    manifest: &crate::generation::CatalogManifest,
    job: &SearchReindexJob,
) -> Result<u64, String> {
    let (table, primary_key) = source_table_and_pk(manifest, &job.index.source_message_type)?;
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_reindex");
    let collection = job.index.collection();
    let mut upserted = 0u64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = source_rows_select_request(
            &job.index,
            &job.tenant_id,
            &primary_key,
            // All columns: the row's dense vector AND its payload fields.
            Vec::new(),
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(manifest, request, context.clone())
            .await
            .map_err(|err| format!("search reindex source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let fetched = record_set.records_json.len();
        let mut last_pk = String::new();
        let mut points = Vec::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode search reindex source row failed: {err}"))?;
            let row_pk = row
                .as_object()
                .map(|map| json_str(map, &primary_key))
                .unwrap_or_default();
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            let Some(vector) = payload_vector(&row) else {
                continue;
            };
            points.push(VectorPointMutation {
                id: row_pk,
                vector,
                payload: reindex_point_payload(&row),
            });
        }
        let page_points = points.len() as u64;
        if !points.is_empty() {
            runtime
                .vector_upsert(
                    manifest,
                    VectorUpsertRequest {
                        context: None,
                        collection: collection.clone(),
                        points,
                        idempotency_key: String::new(),
                    },
                    context.clone(),
                )
                .await
                .map_err(|err| format!("search reindex vector upsert failed: {err}"))?;
            upserted = upserted.saturating_add(page_points);
        }
        if fetched < SEARCH_REINDEX_PAGE_LIMIT as usize || last_pk.is_empty() {
            break;
        }
        after_pk = Some(last_pk);
    }
    Ok(upserted)
}

/// Execute one reindex job end-to-end: mediated source backfill, then the
/// `REINDEXING → ACTIVE` writeback and the completion event. On a work failure
/// the index is restored to `ACTIVE` with `last_error` recorded — NEVER
/// stranded in `REINDEXING` — and the completion event carries
/// `success: false`. Only a failed writeback bubbles (no completion marker is
/// written, so the next leader pass retries the still-REINDEXING job).
async fn process_search_reindex_job(
    service: &SearchServiceImpl,
    job: &SearchReindexJob,
) -> Result<i64, String> {
    let runtime = service
        .runtime
        .as_deref()
        .ok_or_else(|| "search reindex requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_deref()
        .ok_or_else(|| "search reindex requires active catalog".to_string())?;
    if job.tenant_id.trim().is_empty() {
        return Ok(0);
    }
    let state = catalog.active_for(&job.project_id);
    let outcome = reindex_source_rows(runtime, &state.manifest, job).await;
    let (upserted, error) = match &outcome {
        Ok(count) => (*count, None),
        Err(err) => {
            tracing::warn!(
                index = %job.index.index_name,
                error = %err,
                "search reindex failed; restoring index to ACTIVE with last_error"
            );
            (0, Some(err.as_str()))
        }
    };
    let (status, metadata_json) = reindex_writeback(error);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_reindex");
    runtime
        .native_entity_write_for_service(
            "search",
            &context,
            SEARCH_INDEX_MSG,
            index_record(
                &job.index.index_id,
                &job.tenant_id,
                &job.index.index_name,
                &job.index.source_message_type,
                &job.index.backend,
                &job.index.resource_name,
                job.index.vector_dims,
                &job.index.tenant_column,
                &job.index.source_cdc_topic,
                status,
                &metadata_json,
            ),
            index_conflict(),
        )
        .await
        .map_err(|err| format!("search reindex status writeback failed: {err}"))?;
    service
        .emit_index_event(
            TOPIC_REINDEX_COMPLETED,
            &job.tenant_id,
            &job.project_id,
            &job.index.index_name,
            serde_json::json!({
                "reindex_event_id": job.event_id,
                "reindex_id": job.reindex_id,
                "upserted": upserted,
                "success": error.is_none(),
                "error": error.unwrap_or_default(),
            }),
        )
        .await;
    Ok(i64::try_from(upserted).unwrap_or(i64::MAX))
}

/// Purge a deleted index's engine points for its tenant through the EXISTING
/// per-point delete seam (`vector_delete_backend_target` — the same shared path
/// the embedding/asset services delete through; never a parallel engine
/// client). Point ids are enumerated from the source entity via the served
/// mediated SELECT (tenant-scoped): the runtime exposes no engine-scroll seam,
/// and dropping the whole collection would destroy other tenants' points in a
/// shared collection. HONESTY: points whose source rows were deleted after
/// indexing are not enumerable here and may remain as orphans until an operator
/// drops the collection.
async fn teardown_index_points(
    runtime: &DataBrokerRuntime,
    manifest: &crate::generation::CatalogManifest,
    job: &SearchTeardownJob,
) -> Result<u64, String> {
    let (table, primary_key) = source_table_and_pk(manifest, &job.index.source_message_type)?;
    let project_column = crate::generation::sql::resolve_project_column(table);
    let context = search_worker_context(&job.tenant_id, &job.project_id, "search_teardown");
    let collection = job.index.collection();
    let mut deleted = 0u64;
    let mut after_pk: Option<String> = None;
    loop {
        let request = source_rows_select_request(
            &job.index,
            &job.tenant_id,
            &primary_key,
            vec![primary_key.clone()],
            after_pk.as_deref(),
            project_column,
            &job.project_id,
        );
        let (record_set, _) = runtime
            .select(manifest, request, context.clone())
            .await
            .map_err(|err| format!("search teardown source select failed: {err}"))?;
        if record_set.records_json.is_empty() {
            break;
        }
        let fetched = record_set.records_json.len();
        let mut last_pk = String::new();
        let mut page_ids = Vec::new();
        for raw in &record_set.records_json {
            let row: serde_json::Value = serde_json::from_slice(raw)
                .map_err(|err| format!("decode search teardown source row failed: {err}"))?;
            let row_pk = row
                .as_object()
                .map(|map| json_str(map, &primary_key))
                .unwrap_or_default();
            if row_pk.trim().is_empty() {
                continue;
            }
            last_pk = row_pk.clone();
            page_ids.push(row_pk);
        }
        let page_count = page_ids.len() as u64;
        if !page_ids.is_empty() {
            runtime
                .vector_delete_backend_target(None, &job.project_id, &collection, page_ids)
                .await
                .map_err(|err| format!("search teardown vector delete failed: {err}"))?;
            deleted = deleted.saturating_add(page_count);
        }
        if fetched < SEARCH_REINDEX_PAGE_LIMIT as usize || last_pk.is_empty() {
            break;
        }
        after_pk = Some(last_pk);
    }
    Ok(deleted)
}

/// Execute one teardown job. Success emits the [`TOPIC_TEARDOWN_COMPLETED`]
/// marker; a failure logs and leaves the job unmarked so the next leader pass
/// retries it (a deleted index's engine data is never silently abandoned).
async fn process_search_teardown_job(
    service: &SearchServiceImpl,
    job: &SearchTeardownJob,
) -> Result<i64, String> {
    let runtime = service
        .runtime
        .as_deref()
        .ok_or_else(|| "search teardown requires runtime dispatch".to_string())?;
    let catalog = service
        .catalog
        .as_deref()
        .ok_or_else(|| "search teardown requires active catalog".to_string())?;
    if job.tenant_id.trim().is_empty() {
        return Ok(0);
    }
    let state = catalog.active_for(&job.project_id);
    match teardown_index_points(runtime, &state.manifest, job).await {
        Ok(deleted) => {
            service
                .emit_index_event(
                    TOPIC_TEARDOWN_COMPLETED,
                    &job.tenant_id,
                    &job.project_id,
                    &job.index.index_name,
                    serde_json::json!({
                        "teardown_event_id": job.event_id,
                        "points_deleted": deleted,
                    }),
                )
                .await;
            Ok(i64::try_from(deleted).unwrap_or(i64::MAX))
        }
        Err(err) => {
            tracing::warn!(
                index = %job.index.index_name,
                error = %err,
                "search index teardown failed; will retry on next pass"
            );
            Ok(0)
        }
    }
}

/// ONE leader-owned reindex/teardown pass (master-plan 16.2.2/16.2.3).
/// Consumes the `reindex.requested` jobs the Reindex RPC emits (mediated source
/// backfill → engine re-upsert → `REINDEXING → ACTIVE` writeback →
/// `reindex.completed`) and the `index.deleted` teardown jobs DeleteIndex
/// enqueues (per-point engine purge → `teardown.completed`). The leader spawns
/// this via `NativeWorkerHost::spawn_while_leader` (the
/// `run_cache_invalidation_worker_once` shape). Returns rows acted on.
pub(crate) async fn run_search_reindex_once(
    service: Arc<SearchServiceImpl>,
    journal_relation: &str,
    batch: i64,
) -> Result<i64, String> {
    let pool = service
        .pg_pool
        .as_ref()
        .ok_or_else(|| "search reindex worker requires native Postgres store".to_string())?;
    let outbox_relation = service
        .outbox_relation
        .as_deref()
        .ok_or_else(|| "search reindex worker requires transactional outbox".to_string())?;
    let jobs = load_search_reindex_jobs(pool, journal_relation, outbox_relation, batch).await?;
    let mut acted = 0i64;
    for job in &jobs {
        acted = acted.saturating_add(process_search_reindex_job(&service, job).await?);
    }
    let teardowns =
        load_search_teardown_jobs(pool, journal_relation, outbox_relation, batch).await?;
    for job in &teardowns {
        acted = acted.saturating_add(process_search_teardown_job(&service, job).await?);
    }
    Ok(acted)
}

#[cfg(test)]
mod search_scope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
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
        assert_eq!(status.message(), "search index not found");
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "search");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "search_index_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not register an index under tenant-b by
    /// putting a foreign tenant_id in the request BODY; the scope guard rejects
    /// this before any catalog/store access (no Postgres needed).
    #[tokio::test]
    async fn create_index_rejects_cross_tenant_body() {
        let svc = SearchServiceImpl::new(); // no runtime/catalog (guard runs first)
        let mut request = Request::new(search_pb::CreateIndexRequest {
            tenant_id: "tenant-b".to_string(),
            index_name: "contacts".to_string(),
            source_message_type: "acme.crm.entity.v1.Contact".to_string(),
            backend: "qdrant".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_index(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_index_missing_required_fields_carries_field_violations() {
        let svc = SearchServiceImpl::new(); // no runtime/catalog; validation must fire first
        let mut request = Request::new(search_pb::CreateIndexRequest {
            tenant_id: "tenant-a".to_string(),
            index_name: "  ".to_string(),
            source_message_type: String::new(),
            backend: "qdrant".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_index(request)
            .await
            .expect_err("missing index fields must be rejected before runtime/catalog access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "index_name and source_message_type are required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "index_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty search index name"
        );
        assert_eq!(detail.field_violations[1].field, "source_message_type");
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty source message type"
        );
    }

    #[tokio::test]
    async fn create_index_unsupported_backend_carries_field_violation() {
        let svc = SearchServiceImpl::new(); // no runtime/catalog; backend validation fires first
        let mut request = Request::new(search_pb::CreateIndexRequest {
            tenant_id: "tenant-a".to_string(),
            index_name: "contacts".to_string(),
            source_message_type: "acme.crm.entity.v1.Contact".to_string(),
            backend: "memory".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_index(request)
            .await
            .expect_err("unsupported backend must be rejected before runtime/catalog access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "unsupported search backend 'memory' (expected 'qdrant' or 'elasticsearch')"
        );
        assert_single_field_violation(&err, "backend", "must be 'qdrant' or 'elasticsearch'");
    }

    #[tokio::test]
    async fn delete_index_missing_index_name_carries_field_violation() {
        let svc = SearchServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(search_pb::DeleteIndexRequest {
            tenant_id: "tenant-a".to_string(),
            index_name: String::new(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .delete_index(request)
            .await
            .expect_err("missing index_name must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "index_name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "index_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty search index name"
        );
    }

    #[test]
    fn source_message_type_missing_from_catalog_carries_field_violation() {
        let catalog = Arc::new(CatalogManager::new(
            crate::generation::CatalogManifest::default(),
        ));
        let svc = SearchServiceImpl::new().with_catalog(Some(catalog));
        let err = svc
            .resolve_source_tenant_column("default", "acme.crm.entity.v1.Contact")
            .expect_err("unknown source message type must fail before store access");
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
    fn search_missing_setup_capabilities_carry_typed_detail() {
        for (operation, capability, message) in [
            (
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "search service requires runtime native-entity dispatch (no runtime configured)",
            ),
            (
                "catalog_lookup",
                "active_catalog",
                "search service requires the active catalog (no catalog configured)",
            ),
        ] {
            let err = search_capability_status(operation, capability, message);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "search");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, capability);
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn full_text_only_search_requires_typed_capability_detail() {
        let err = full_text_only_requires_mediated_ir_status();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "full-text-only search requires the mediated IR full-text path (P2.2, pending); \
         supply query_vector for vector or hybrid search"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "search");
        assert_eq!(detail.operation, "full_text_only_search");
        assert_eq!(detail.capability_required, "mediated_ir_full_text_path");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn search_index_not_found_status_carries_schema_detail() {
        assert_schema_not_found_detail(&search_index_not_found_status("reindex"), "reindex");
    }

    #[test]
    fn empty_search_query_carries_field_violations() {
        let err = validate_search_query(&search_pb::SearchRequest {
            query_text: " ".to_string(),
            query_vector: Vec::new(),
            ..Default::default()
        })
        .expect_err("empty search query must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "search requires query_text and/or query_vector"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "query_text");
        assert_eq!(
            detail.field_violations[0].description,
            "must be non-empty when query_vector is empty"
        );
        assert_eq!(detail.field_violations[1].field, "query_vector");
        assert_eq!(
            detail.field_violations[1].description,
            "must be non-empty when query_text is empty"
        );
    }

    /// CreateIndex fails closed when the source entity has no resolvable tenant
    /// column (a Search would otherwise have no tenant predicate to inject).
    #[test]
    fn create_index_fails_closed_without_source_tenant_column() {
        let err = require_source_tenant_column(None, "acme.crm.entity.v1.Contact")
            .expect_err("missing tenant column must fail closed");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "source entity 'acme.crm.entity.v1.Contact' has no resolvable tenant column; refusing to register a tenant-scoped search index over it (fail closed)"
        );
        assert_single_field_violation(
            &err,
            "source_message_type",
            "must resolve to a tenant-scoped source entity",
        );
        // A blank/whitespace column is treated as unresolved (still fail closed).
        let blank = require_source_tenant_column(Some("  ".to_string()), "X")
            .expect_err("blank column must fail closed");
        assert_eq!(blank.code(), tonic::Code::InvalidArgument);
        assert_single_field_violation(
            &blank,
            "source_message_type",
            "must resolve to a tenant-scoped source entity",
        );
        // A real column resolves.
        assert_eq!(
            require_source_tenant_column(Some("tenant_id".to_string()), "X").unwrap(),
            "tenant_id"
        );
    }

    /// The CDC freshness filter drops a tenant-less event and a foreign-tenant
    /// event, and keeps only a matching-tenant event (fail closed).
    #[test]
    fn freshness_filter_drops_tenantless_and_foreign_events() {
        let topic = "udb.search.search_indexes.cdc";
        // Tenant-less payload → dropped.
        let tenantless = serde_json::json!({ "id": "row-1" });
        assert!(!event_in_index_scope(topic, &tenantless, "acme"));
        // Foreign tenant → dropped.
        let foreign = serde_json::json!({ "id": "row-1", "tenant_id": "other" });
        assert!(!event_in_index_scope(topic, &foreign, "acme"));
        // Matching tenant → kept.
        let matching = serde_json::json!({ "id": "row-1", "tenant_id": "acme" });
        assert!(event_in_index_scope(topic, &matching, "acme"));
        // Empty index scope can match nothing.
        assert!(!event_in_index_scope(topic, &matching, ""));
    }

    /// Pure RRF: known ranks → known fused scores. With list_one = [a,b,c] and
    /// list_two = [b,a]: doc "a" is rank 0 then rank 1 (1/60 + 1/61); doc "b" is
    /// rank 1 then rank 0 (1/61 + 1/60) — so a and b are EXACTLY tied, broken by
    /// id ascending → a before b; doc "c" appears once at rank 2 (1/62). Order:
    /// a, b, c.
    #[test]
    fn reciprocal_rank_fusion_known_scores() {
        let list_one = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let list_two = vec!["b".to_string(), "a".to_string()];
        let fused = reciprocal_rank_fusion(&[list_one, list_two]);

        let score_of = |id: &str| {
            fused
                .iter()
                .find(|(d, _)| d == id)
                .map(|(_, s)| *s)
                .unwrap()
        };
        let expect_a = 1.0 / 60.0 + 1.0 / 61.0;
        let expect_b = 1.0 / 61.0 + 1.0 / 60.0;
        let expect_c = 1.0 / 62.0;
        assert!((score_of("a") - expect_a).abs() < 1e-12);
        assert!((score_of("b") - expect_b).abs() < 1e-12);
        assert!((score_of("c") - expect_c).abs() < 1e-12);
        // a and b are tied on score; the id-ascending tie-break makes order
        // deterministic. c is strictly lowest.
        let order: Vec<&str> = fused.iter().map(|(d, _)| d.as_str()).collect();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    /// Hit payload mapping (16.2.4): the broker-stamped `_tenant_id` scope key
    /// is stripped from returned payloads; application fields survive; an
    /// absent or tenant-only payload maps to an empty `payload_json`.
    #[test]
    fn hit_payload_json_strips_tenant_scope_key() {
        let payload = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            TENANT_SCOPE_PAYLOAD_KEY: "acme",
            "title": "hello",
        }))
        .expect("payload struct");
        let json = hit_payload_json(Some(&payload));
        let value: serde_json::Value = serde_json::from_str(&json).expect("payload_json decodes");
        assert!(
            value.get(TENANT_SCOPE_PAYLOAD_KEY).is_none(),
            "the tenant scope key must never leak into hit payloads"
        );
        assert_eq!(value.get("title").and_then(|v| v.as_str()), Some("hello"));
        // No payload → empty string.
        assert_eq!(hit_payload_json(None), "");
        // A tenant-only payload has nothing left to return.
        let tenant_only = crate::runtime::executor_utils::json_to_struct(&serde_json::json!({
            TENANT_SCOPE_PAYLOAD_KEY: "acme",
        }))
        .expect("tenant-only struct");
        assert_eq!(hit_payload_json(Some(&tenant_only)), "");
    }

    /// The reindex failure path NEVER strands an index in REINDEXING: both
    /// outcomes restore ACTIVE, and a failure records `last_error` in the
    /// mutable metadata column.
    #[test]
    fn reindex_writeback_never_strands_reindexing() {
        let (ok_status, ok_metadata) = reindex_writeback(None);
        assert_eq!(ok_status, STATUS_ACTIVE);
        assert_ne!(ok_status, STATUS_REINDEXING);
        assert_eq!(ok_metadata, "{}");
        let (err_status, err_metadata) = reindex_writeback(Some("qdrant unreachable"));
        assert_eq!(err_status, STATUS_ACTIVE);
        assert_ne!(err_status, STATUS_REINDEXING);
        let metadata: serde_json::Value =
            serde_json::from_str(&err_metadata).expect("failure metadata decodes");
        assert_eq!(
            metadata.get("last_error").and_then(|v| v.as_str()),
            Some("qdrant unreachable")
        );
    }

    /// Job-loading SQL shape (mirrors the metering SQL-shape tests): journal
    /// join, BOTH payload envelope levels (the CDC journal envelope-nesting
    /// lesson), the outbox+journal completion dedup pair, deterministic order,
    /// and a bound LIMIT.
    #[test]
    fn worker_job_loading_sql_shape() {
        let model = search_index_model();
        let journal = "udb_system.cdc_event_journal";
        let outbox = "udb_system.outbox_events";

        let reindex = reindex_jobs_sql(&model, journal, outbox);
        assert!(reindex.contains(&format!("FROM {journal} j")));
        assert!(reindex.contains("search_indexes"));
        assert!(reindex.contains("j.payload->'payload'->>'index_name'"));
        assert!(reindex.contains("j.payload->'payload'->>'tenant_id'"));
        assert!(reindex.contains("reindex_event_id"));
        assert_eq!(
            reindex.matches("NOT EXISTS").count(),
            2,
            "reindex dedup must check the completion marker in BOTH outbox and journal"
        );
        assert!(reindex.contains("ORDER BY j.published_at ASC, j.event_id ASC"));
        assert!(reindex.contains("LIMIT $4"));

        let teardown = teardown_jobs_sql(&model, journal, outbox);
        assert!(teardown.contains("teardown_event_id"));
        assert_eq!(teardown.matches("NOT EXISTS").count(), 2);
        assert!(teardown.contains("LIMIT $4"));

        let freshness = freshness_jobs_sql(&model, journal, outbox);
        // Only embedding-carrying events are candidates (no journal clog from
        // vector-less source changes), at EITHER envelope level.
        assert!(freshness.contains("jsonb_typeof(j.payload->'vector') = 'array'"));
        assert!(freshness.contains("jsonb_typeof(j.payload->'payload'->'vector') = 'array'"));
        assert!(freshness.contains("source_event_id"));
        assert_eq!(freshness.matches("NOT EXISTS").count(), 2);
        assert!(freshness.contains("LIMIT $3"));
    }

    /// The CDC journal may envelope-nest the emitted payload under
    /// `payload.payload`; the freshness body reader must fall through to the
    /// nested level (and the vector extractor accepts `vector`/`embedding`).
    #[test]
    fn freshness_event_body_reads_nested_journal_envelope() {
        let nested = serde_json::json!({
            "topic": "acme.crm.contacts.cdc",
            "payload": { "id": "row-1", "tenant_id": "acme", "vector": [0.25, 0.5] }
        });
        let body = freshness_event_body(&nested);
        assert_eq!(body.get("id").and_then(|v| v.as_str()), Some("row-1"));
        assert_eq!(payload_vector(body), Some(vec![0.25, 0.5]));
        let flat = serde_json::json!({ "id": "row-2", "vector": [1.0] });
        assert_eq!(
            freshness_event_body(&flat)
                .get("id")
                .and_then(|v| v.as_str()),
            Some("row-2")
        );
        // Source rows may carry the embedding under `embedding` instead.
        assert_eq!(
            payload_vector(&serde_json::json!({ "embedding": [0.5, 1.0] })),
            Some(vec![0.5, 1.0])
        );
        // An empty vector is no vector (never upsert a zero-length embedding).
        assert_eq!(payload_vector(&serde_json::json!({ "vector": [] })), None);
    }

    /// The reindex/teardown source enumeration is tenant-scoped via the STORED
    /// source tenant column, project-scoped when the table requires it, and
    /// cursor-paged on the primary key (mirrors the embedding backfill's
    /// select-request test).
    #[test]
    fn source_rows_select_request_is_tenant_scoped_and_paged() {
        let index = StoredIndex {
            index_id: "idx-1".to_string(),
            index_name: "contacts".to_string(),
            source_message_type: "acme.crm.entity.v1.Contact".to_string(),
            backend: BACKEND_QDRANT.to_string(),
            resource_name: String::new(),
            vector_dims: 3,
            tenant_column: "tenant_id".to_string(),
            source_cdc_topic: "acme.crm.contacts.cdc".to_string(),
            status: STATUS_REINDEXING.to_string(),
        };
        let request = source_rows_select_request(
            &index,
            "acme",
            "contact_id",
            Vec::new(),
            Some("after-1"),
            Some("project_id"),
            "proj-1",
        );
        assert_eq!(request.message_type, "acme.crm.entity.v1.Contact");
        assert_eq!(request.limit, SEARCH_REINDEX_PAGE_LIMIT);
        assert_eq!(request.sort.len(), 1);
        assert_eq!(request.sort[0].field, "contact_id");
        assert!(!request.sort[0].descending);
        let filter = crate::runtime::executor_utils::struct_to_json(
            request.filter.as_ref().expect("filter present"),
        );
        let ands = filter
            .get("$and")
            .and_then(|v| v.as_array())
            .expect("$and filter");
        assert!(
            ands.iter()
                .any(|f| f.get("tenant_id").and_then(|v| v.as_str()) == Some("acme")),
            "the stored source tenant column must scope the read"
        );
        assert!(
            ands.iter()
                .any(|f| f.get("project_id").and_then(|v| v.as_str()) == Some("proj-1")),
            "a project-scoped source table must carry the project predicate"
        );
        assert!(
            ands.iter().any(|f| {
                f.get("contact_id")
                    .and_then(|c| c.get("$gt"))
                    .and_then(|v| v.as_str())
                    == Some("after-1")
            }),
            "pagination must cursor on the primary key"
        );
    }
}

impl DataBrokerService {
    /// Build the native `SearchService`, wired to the broker's Postgres pool, the
    /// mediated-dispatch runtime, the project-active catalog (source tenant-column
    /// resolution + per-project backend routing), and the shared outbox.
    pub(crate) fn build_search_service(&self) -> SearchServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("search", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        SearchServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_catalog(Some(self.catalog.clone()))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
