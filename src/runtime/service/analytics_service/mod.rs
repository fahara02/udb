//! Native `AnalyticsService` — proto-driven Postgres over the UDB-owned
//! `udb_analytics.{pipeline_metric_snapshots,executor_performance_summaries,
//! reconciliation_analytics_summaries}` tables. Same contract as the other
//! native services: no in-memory store, every identifier resolved from the
//! embedded proto manifest via [`NativeModel`], fail-closed when no PG pool is
//! configured.
//!
//! There is no separate raw-observation table in the proto contract, so
//! `RecordPipelineMetric` performs **online aggregation**: it upserts directly
//! into the hourly snapshot keyed by the table's real unique key
//! `(snapshot_hour, stage_name, tenant_id)`, maintaining running counts, a
//! running mean latency, error rate, and throughput. Latency percentiles
//! (p50/p95/p99) cannot be derived online from a single observation; they are
//! filled in by the rollup pass ([`run_analytics_rollup_once`], the seam the
//! leader-elected worker drives), which `TriggerSnapshot` also runs inline
//! (tenant-scoped) so its `snapshots_written` reports rows genuinely written.
//! The accumulator keeps only per-hour counts and a running mean — raw
//! per-request latencies are never stored — so the pass computes
//! `percentile_cont` over the trailing window of HOURLY MEAN latencies per
//! (tenant, stage). That is an honest, documented approximation (percentiles of
//! hourly means), NOT per-request latency percentiles; deriving true request
//! percentiles would require a raw-observation store the contract doesn't have.
//!
//! Module layout (no god file): [`config`] the statics (entity messages, the
//! proto-declared outbox topic + event types, read caps, rollup-window env
//! knob), [`model`] the manifest models + row/JSON decoders, [`store`] the
//! neutral-IR query builders + bespoke SQL + tenant-scope GUC, [`events`] the
//! best-effort outbox emit + payload builder, [`errors`] the typed statuses +
//! validators + platform-admin guards, [`rollup`] the percentile rollup pass +
//! leader export, [`handlers`] the seven RPCs — `mod.rs` keeps only the struct,
//! the builders/require-guards, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::analytics::services::v1 as ana_pb;
use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::analytics::services::v1::analytics_service_server::AnalyticsServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod handlers;
mod model;
mod rollup;
mod store;
#[cfg(test)]
mod tests;

// Re-exported at the module root for the leader-elected rollup worker (`serve()`
// in `service/mod.rs`, mirrored by `singleton.rs`).
pub(crate) use rollup::run_analytics_rollup_once;

pub struct AnalyticsServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    pub(crate) outbox_relation: Option<String>,
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl AnalyticsServiceImpl {
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

    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::analytics_capability_status(
                "postgres_store",
                "postgres_store",
                "analytics service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    pub(crate) fn runtime(&self) -> Option<&DataBrokerRuntime> {
        self.runtime.as_deref()
    }
}

impl Default for AnalyticsServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl AnalyticsService for AnalyticsServiceImpl {
    async fn record_pipeline_metric(
        &self,
        request: Request<ana_pb::RecordPipelineMetricRequest>,
    ) -> Result<Response<ana_pb::RecordPipelineMetricResponse>, Status> {
        handlers::record_pipeline_metric(self, request).await
    }

    async fn get_pipeline_summary(
        &self,
        request: Request<ana_pb::GetPipelineSummaryRequest>,
    ) -> Result<Response<ana_pb::GetPipelineSummaryResponse>, Status> {
        handlers::get_pipeline_summary(self, request).await
    }

    async fn get_executor_performance(
        &self,
        request: Request<ana_pb::GetExecutorPerformanceRequest>,
    ) -> Result<Response<ana_pb::GetExecutorPerformanceResponse>, Status> {
        handlers::get_executor_performance(self, request).await
    }

    async fn get_reconciliation_analytics(
        &self,
        request: Request<ana_pb::GetReconciliationAnalyticsRequest>,
    ) -> Result<Response<ana_pb::GetReconciliationAnalyticsResponse>, Status> {
        handlers::get_reconciliation_analytics(self, request).await
    }

    async fn get_throughput(
        &self,
        request: Request<ana_pb::GetThroughputRequest>,
    ) -> Result<Response<ana_pb::GetThroughputResponse>, Status> {
        handlers::get_throughput(self, request).await
    }

    async fn get_sla_compliance(
        &self,
        request: Request<ana_pb::GetSlaComplianceRequest>,
    ) -> Result<Response<ana_pb::GetSlaComplianceResponse>, Status> {
        handlers::get_sla_compliance(self, request).await
    }

    async fn trigger_snapshot(
        &self,
        request: Request<ana_pb::TriggerSnapshotRequest>,
    ) -> Result<Response<ana_pb::TriggerSnapshotResponse>, Status> {
        handlers::trigger_snapshot(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `AnalyticsService`, wired to the broker's Postgres pool.
    pub(crate) fn build_analytics_service(&self) -> AnalyticsServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("analytics", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        AnalyticsServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
