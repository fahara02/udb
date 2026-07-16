//! Native `MeteringService` (master-plan 9.9) — usage metering and quotas.
//!
//! Mirrors `lock_service`/`config_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Usage is an append-only, durable stream of `UsageEvent`
//! rows; quotas (`QuotaRule`) cap a metric over a rolling window. The tenant is
//! always taken from the VERIFIED claim, never the request body.
//!
//! Doctrine (Phase 9):
//! - **Metering NEVER fails the metered request.** The ingest seam
//!   [`admission::record_usage`] is a single cheap INSERT that log-and-swallows on
//!   error and returns `Ok(())` — it is the seam the leader calls from the
//!   admission hook (`native_helpers::admit_on`).
//! - **Durable, not in-memory.** Usage is summed from durable rows (a counter in
//!   RAM lies across restarts and replicas). CheckQuota is PURE aggregation: a
//!   windowed durable SUM compared against the rule limit via the deterministic
//!   [`calc::quota_decision`].
//! - **Quota checks fail closed.** Usage ingest remains best-effort so metering
//!   never fails the metered request, but explicit usage/quota reads must not
//!   fabricate a lower total when the durable aggregate is unavailable.
//! - **Resolve-once / outbox on change.** Window defaults are named consts;
//!   quota mutations bump a monotone per-row revision and emit a versioned
//!   dot-topic outbox event.
//!
//! Module layout (no god file): [`config`] statics + rollup env knobs, [`calc`]
//! the pure math + time source, [`admission`] the ingest seam + process-local
//! sink, [`store`] the models/aggregate SQL/JSON decode/quota IR, [`rollup`] the
//! leader export, [`errors`] typed statuses, [`handlers`] the six RPCs — `mod.rs`
//! keeps only the struct, the builder, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::metering::services::v1 as metering_pb;
use crate::proto::udb::core::metering::services::v1::metering_service_server::MeteringService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::metering::services::v1::metering_service_server::MeteringServiceServer;

use super::DataBrokerService;

mod admission;
mod calc;
mod config;
mod errors;
mod handlers;
mod rollup;
mod store;
#[cfg(test)]
mod tests;

// Re-exported at the module root for the admission hook (`native_helpers`) and
// the leader rollup worker (`serve()`).
pub(crate) use admission::{admission_metering_method, admission_metering_pool, record_usage};
pub(crate) use calc::now_unix;
pub(crate) use config::{ADMISSION_METERING_UNIT, METERING_ROLLUP_BATCH, metering_rollup_interval};
pub(crate) use rollup::run_metering_rollup_once;

/// Postgres-backed `MeteringService` handler.
pub struct MeteringServiceImpl {
    /// Outbox-event Postgres pool (also the raw target for `record_usage`).
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for typed native-entity dispatch (reads/writes/aggregates).
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl MeteringServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
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

    /// Quota state is durable-only: fail closed when no runtime dispatch exists.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::metering_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "metering service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for MeteringServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl MeteringService for MeteringServiceImpl {
    async fn record_usage(
        &self,
        request: Request<metering_pb::RecordUsageRequest>,
    ) -> Result<Response<metering_pb::RecordUsageResponse>, Status> {
        handlers::record_usage(self, request).await
    }

    async fn query_usage(
        &self,
        request: Request<metering_pb::QueryUsageRequest>,
    ) -> Result<Response<metering_pb::QueryUsageResponse>, Status> {
        handlers::query_usage(self, request).await
    }

    async fn put_quota(
        &self,
        request: Request<metering_pb::PutQuotaRequest>,
    ) -> Result<Response<metering_pb::PutQuotaResponse>, Status> {
        handlers::put_quota(self, request).await
    }

    async fn get_quota(
        &self,
        request: Request<metering_pb::GetQuotaRequest>,
    ) -> Result<Response<metering_pb::GetQuotaResponse>, Status> {
        handlers::get_quota(self, request).await
    }

    async fn list_quotas(
        &self,
        request: Request<metering_pb::ListQuotasRequest>,
    ) -> Result<Response<metering_pb::ListQuotasResponse>, Status> {
        handlers::list_quotas(self, request).await
    }

    async fn check_quota(
        &self,
        request: Request<metering_pb::CheckQuotaRequest>,
    ) -> Result<Response<metering_pb::CheckQuotaResponse>, Status> {
        handlers::check_quota(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `MeteringService`, wired to the broker's Postgres pool,
    /// the native-entity dispatch runtime, and the shared outbox.
    pub(crate) fn build_metering_service(&self) -> MeteringServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("metering", true, "")
            .ok();
        admission::install_admission_metering_pool(pg_pool.clone());
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        MeteringServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
