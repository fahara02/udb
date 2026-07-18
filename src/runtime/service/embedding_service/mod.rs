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
//!
//! Module layout (no god file): [`config`] the statics + top-k/quota/batch/
//! emitter-interval + retrieve score-threshold/fusion-weight knobs, [`errors`] the
//! typed statuses + field validators + reported-vector shape gate + fail-closed
//! source-tenant-column gate, [`model`] the `StoredSource` DTO + JSON decoders +
//! tenant-tagged point builder, [`store`] the neutral-IR query/record builders +
//! native SQL model, [`events`] the shared outbox emits + no-credential work
//! payload builder, [`handlers`] the six RPCs + per-request `Retrieve` helpers,
//! [`workers`] the leader-owned work-emit / backfill / teardown passes + CDC
//! decoders + the shared vector-delete seam — `mod.rs` keeps only the struct, the
//! builders/require guards, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::proto::udb::core::embedding::services::v1::embedding_service_server::EmbeddingService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::CatalogManager;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::embedding::services::v1::embedding_service_server::EmbeddingServiceServer;

use super::DataBrokerService;

mod chunking;
mod config;
mod errors;
mod events;
mod handlers;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod workers;

// Re-exported at the module root for `serve()`, which spawns the leader-elected
// change-driven work emitter under leader election, and reads the env-resolved
// cadence/batch knobs.
pub(crate) use config::{EMBEDDING_WORK_EMITTER_BATCH, embedding_work_emitter_interval};
pub(crate) use workers::run_embedding_work_emitter_once;

/// Postgres-backed `EmbeddingService` handler.
pub struct EmbeddingServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `embedding`).
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for mediated source CRUD, the shared vector upsert/delete
    /// seam, and the mediated hybrid-search dispatch `Retrieve` delegates to.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Project-active catalog: resolves the source entity's manifest table (and
    /// thus its tenant column) plus the manifest `Retrieve` hybrid search needs.
    pub(crate) catalog: Option<Arc<CatalogManager>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
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
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::embedding_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "embedding service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    pub(crate) fn require_catalog(&self) -> Result<&CatalogManager, Status> {
        self.catalog.as_deref().ok_or_else(|| {
            errors::embedding_capability_status(
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

#[tonic::async_trait]
impl EmbeddingService for EmbeddingServiceImpl {
    async fn register_source(
        &self,
        request: Request<embedding_pb::RegisterSourceRequest>,
    ) -> Result<Response<embedding_pb::RegisterSourceResponse>, Status> {
        handlers::register_source(self, request).await
    }

    async fn list_sources(
        &self,
        request: Request<embedding_pb::ListSourcesRequest>,
    ) -> Result<Response<embedding_pb::ListSourcesResponse>, Status> {
        handlers::list_sources(self, request).await
    }

    async fn delete_source(
        &self,
        request: Request<embedding_pb::DeleteSourceRequest>,
    ) -> Result<Response<embedding_pb::DeleteSourceResponse>, Status> {
        handlers::delete_source(self, request).await
    }

    async fn backfill(
        &self,
        request: Request<embedding_pb::BackfillRequest>,
    ) -> Result<Response<embedding_pb::BackfillResponse>, Status> {
        handlers::backfill(self, request).await
    }

    async fn report_embedding(
        &self,
        request: Request<embedding_pb::ReportEmbeddingRequest>,
    ) -> Result<Response<embedding_pb::ReportEmbeddingResponse>, Status> {
        handlers::report_embedding(self, request).await
    }

    async fn retrieve(
        &self,
        request: Request<embedding_pb::RetrieveRequest>,
    ) -> Result<Response<embedding_pb::RetrieveResponse>, Status> {
        handlers::retrieve(self, request).await
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
