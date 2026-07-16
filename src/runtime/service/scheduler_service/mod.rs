//! Native `SchedulerService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_scheduler.scheduled_jobs` table, plus the leader-elected scheduler tick
//! (master-plan 9.3).
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! `NativeModel` (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! Doctrine (9.3): mutations persist durably to the canonical store, are
//! tenant-scoped by the verified claim, ride per-tenant fair admission, and emit
//! one outbox event each. The tick is leader-elected (one firer cluster-wide),
//! claims DUE rows with `FOR UPDATE SKIP LOCKED` so two leaders can never
//! double-fire, and FIRES EVENTS ONLY (`udb.scheduler.job.fired.v1`) — it never
//! executes a job payload in-process. A job that cannot make forward progress
//! after `max_attempts` is routed to the dead-letter topic
//! (`udb.scheduler.job.dead.v1`) and marked DEAD. Fail closed on a missing pool
//! or outbox relation.
//!
//! Module layout (no god file): [`config`] statics, [`quota`] the per-tenant
//! budget gate, [`cron`] the self-contained evaluator + missed-run accounting,
//! [`model`] the manifest model + row mapping, [`errors`] typed statuses,
//! [`handlers`] the six RPCs, [`tick`] the leader-elected firer — `mod.rs` keeps
//! only the struct, the builder, and one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::scheduler::services::v1 as scheduler_pb;
use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerService;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::scheduler::services::v1::scheduler_service_server::SchedulerServiceServer;

use super::DataBrokerService;

mod config;
mod cron;
mod errors;
mod handlers;
mod model;
mod quota;
#[cfg(test)]
mod tests;
mod tick;

// Re-exported at the module root for `serve()`, which spawns the tick worker
// under leader election.
pub(crate) use config::SCHEDULER_TICK_BATCH;
pub(crate) use tick::run_scheduler_tick_once;

/// Postgres-backed `SchedulerService` handler.
pub struct SchedulerServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Transactional-outbox relation; mutations and tick fires enqueue here.
    pub(crate) outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl SchedulerServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
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

    /// Scheduler CRUD is durable-only: fail closed when no Postgres pool exists.
    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::scheduler_capability_status(
                "postgres_store",
                "postgres_store",
                "scheduler service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for SchedulerServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl SchedulerService for SchedulerServiceImpl {
    async fn create_job(
        &self,
        request: Request<scheduler_pb::CreateJobRequest>,
    ) -> Result<Response<scheduler_pb::CreateJobResponse>, Status> {
        handlers::create_job(self, request).await
    }

    async fn get_job(
        &self,
        request: Request<scheduler_pb::GetJobRequest>,
    ) -> Result<Response<scheduler_pb::GetJobResponse>, Status> {
        handlers::get_job(self, request).await
    }

    async fn list_jobs(
        &self,
        request: Request<scheduler_pb::ListJobsRequest>,
    ) -> Result<Response<scheduler_pb::ListJobsResponse>, Status> {
        handlers::list_jobs(self, request).await
    }

    async fn delete_job(
        &self,
        request: Request<scheduler_pb::DeleteJobRequest>,
    ) -> Result<Response<scheduler_pb::DeleteJobResponse>, Status> {
        handlers::delete_job(self, request).await
    }

    async fn pause_job(
        &self,
        request: Request<scheduler_pb::PauseJobRequest>,
    ) -> Result<Response<scheduler_pb::PauseJobResponse>, Status> {
        handlers::pause_job(self, request).await
    }

    async fn resume_job(
        &self,
        request: Request<scheduler_pb::ResumeJobRequest>,
    ) -> Result<Response<scheduler_pb::ResumeJobResponse>, Status> {
        handlers::resume_job(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `SchedulerService`, wired to the broker's Postgres pool
    /// and the transactional outbox. The leader-elected tick worker is spawned by
    /// `serve()` (not here), so a non-leader replica never fires.
    pub(crate) fn build_scheduler_service(&self) -> SchedulerServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("scheduler", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        SchedulerServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
