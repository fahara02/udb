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
//!
//! Module layout (no god file): [`config`] the statics + top-k/quota/batch/
//! interval knobs, [`errors`] the typed statuses + field validators + fail-closed
//! source-tenant-column gate, [`model`] the `StoredIndex` DTO + JSON/PgRow
//! decoders + collection resolver, [`store`] the neutral-IR query/record builders
//! + native SQL model, [`fusion`] the isolated pure reciprocal-rank fusion,
//! [`events`] the shared per-mutation outbox emit, [`handlers`] the five RPCs +
//! per-request search helpers, [`workers`] the leader-owned freshness / reindex /
//! teardown passes — `mod.rs` keeps only the struct, the builders/require guards,
//! and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::search::services::v1 as search_pb;
use crate::proto::udb::core::search::services::v1::search_service_server::SearchService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::CatalogManager;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::search::services::v1::search_service_server::SearchServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod fusion;
mod handlers;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod workers;

// Re-exported at the module root for `serve()`, which spawns the leader-elected
// index-freshness and reindex/teardown passes under leader election, and reads
// the env-resolved cadence/batch knobs.
pub(crate) use config::{
    SEARCH_FRESHNESS_BATCH, SEARCH_REINDEX_BATCH, search_freshness_interval,
    search_reindex_interval,
};
pub(crate) use workers::{run_index_freshness_consumer, run_search_reindex_once};

/// Postgres-backed `SearchService` handler.
pub struct SearchServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `search`).
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for mediated index CRUD and mediated vector dispatch.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Project-active catalog: resolves the source entity's manifest table (and
    /// thus its tenant column) and the per-project search backend routing.
    pub(crate) catalog: Option<Arc<CatalogManager>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
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
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::search_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "search service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    pub(crate) fn require_catalog(&self) -> Result<&CatalogManager, Status> {
        self.catalog.as_deref().ok_or_else(|| {
            errors::search_capability_status(
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

#[tonic::async_trait]
impl SearchService for SearchServiceImpl {
    async fn create_index(
        &self,
        request: Request<search_pb::CreateIndexRequest>,
    ) -> Result<Response<search_pb::CreateIndexResponse>, Status> {
        handlers::create_index(self, request).await
    }

    async fn delete_index(
        &self,
        request: Request<search_pb::DeleteIndexRequest>,
    ) -> Result<Response<search_pb::DeleteIndexResponse>, Status> {
        handlers::delete_index(self, request).await
    }

    async fn list_indexes(
        &self,
        request: Request<search_pb::ListIndexesRequest>,
    ) -> Result<Response<search_pb::ListIndexesResponse>, Status> {
        handlers::list_indexes(self, request).await
    }

    async fn search(
        &self,
        request: Request<search_pb::SearchRequest>,
    ) -> Result<Response<search_pb::SearchResponse>, Status> {
        handlers::search(self, request).await
    }

    async fn reindex(
        &self,
        request: Request<search_pb::ReindexRequest>,
    ) -> Result<Response<search_pb::ReindexResponse>, Status> {
        handlers::reindex(self, request).await
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
