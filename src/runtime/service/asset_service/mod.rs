//! Native `AssetService` — proto-driven Postgres CRUD + processing-pipeline
//! orchestration over the UDB-owned `udb_asset.{assets,pipeline_definitions,
//! pipeline_instances,pipeline_steps}` tables.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! Module layout (no god file): [`config`] the entity message names + stable
//! error-reason codes + default vector collection + versioned outbox topics,
//! [`errors`] the typed statuses/reason-trailer helpers + validators, [`model`]
//! the native models + enum<->db converters + JSON/PgRow decoders, [`store`] the
//! neutral-IR query/record builders, [`embedding`] the shared vector-point
//! upsert/delete seam, [`steps`] the pure step registry + the feature-gated
//! image/transcode byte-step primitives, [`handlers`] the eight RPCs, [`execution`]
//! the auto-trigger orchestration + async byte-step runner + terminal advance,
//! [`consumer`] the Kafka storage-finalized + trigger-topic consumers — `mod.rs`
//! keeps only the struct, the builders/admission/require guards, the native-state
//! crypto helpers, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::asset::services::v1 as asset_pb;
use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};

pub use crate::proto::udb::core::asset::services::v1::asset_service_server::AssetServiceServer;

use super::DataBrokerService;
use super::native_helpers::admit_on as native_admit_on;

mod config;
mod consumer;
mod embedding;
mod errors;
mod execution;
mod handlers;
mod model;
mod steps;
mod store;
#[cfg(test)]
mod tests;

use config::DEFAULT_VECTOR_COLLECTION;
use errors::{
    asset_capability_status, native_state_decryption_failed_status,
    native_state_encryption_failed_status,
};

/// Postgres-backed `AssetService` handler.
pub struct AssetServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka → downstream consumers. `None` = no emit.
    pub(crate) outbox_relation: Option<String>,
    /// Runtime handle used to push `EMBED`-step vectors into the vector backend.
    /// `None` = embeddings stay in the step result only (no vector upsert).
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Mutating/orchestration RPCs acquire a
    /// per-tenant `Object` budget through this so one tenant can't starve shared
    /// pipeline capacity. `None` only in test construction (no runtime wired).
    pub(crate) channels: Option<ChannelManager>,
    /// Vector collection EMBED vectors are upserted into.
    pub(crate) vector_collection: String,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
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

impl Default for AssetServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl AssetService for AssetServiceImpl {
    async fn create_pipeline_definition(
        &self,
        request: Request<asset_pb::CreatePipelineDefinitionRequest>,
    ) -> Result<Response<asset_pb::CreatePipelineDefinitionResponse>, Status> {
        handlers::create_pipeline_definition(self, request).await
    }

    async fn get_pipeline_definition(
        &self,
        request: Request<asset_pb::GetPipelineDefinitionRequest>,
    ) -> Result<Response<asset_pb::GetPipelineDefinitionResponse>, Status> {
        handlers::get_pipeline_definition(self, request).await
    }

    async fn register_asset(
        &self,
        request: Request<asset_pb::RegisterAssetRequest>,
    ) -> Result<Response<asset_pb::RegisterAssetResponse>, Status> {
        handlers::register_asset(self, request).await
    }

    async fn start_pipeline(
        &self,
        request: Request<asset_pb::StartPipelineRequest>,
    ) -> Result<Response<asset_pb::StartPipelineResponse>, Status> {
        handlers::start_pipeline(self, request).await
    }

    async fn get_pipeline(
        &self,
        request: Request<asset_pb::GetPipelineRequest>,
    ) -> Result<Response<asset_pb::GetPipelineResponse>, Status> {
        handlers::get_pipeline(self, request).await
    }

    async fn complete_step(
        &self,
        request: Request<asset_pb::CompleteStepRequest>,
    ) -> Result<Response<asset_pb::CompleteStepResponse>, Status> {
        handlers::complete_step(self, request).await
    }

    async fn list_assets(
        &self,
        request: Request<asset_pb::ListAssetsRequest>,
    ) -> Result<Response<asset_pb::ListAssetsResponse>, Status> {
        handlers::list_assets(self, request).await
    }

    async fn get_asset(
        &self,
        request: Request<asset_pb::GetAssetRequest>,
    ) -> Result<Response<asset_pb::GetAssetResponse>, Status> {
        handlers::get_asset(self, request).await
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
