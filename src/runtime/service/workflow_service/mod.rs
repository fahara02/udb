//! Native `WorkflowService` (master-plan 9.12) — durable multi-step operations with
//! compensation, exposed as a first-class native service.
//!
//! Mirrors `scheduler_service`: proto-driven Postgres CRUD over the UDB-owned
//! `udb_workflow.workflow_instances` table (column identifiers resolved from the
//! embedded proto manifest via [`NativeModel`]), plus a leader-elected tick that
//! claims DUE instances with `FOR UPDATE SKIP LOCKED` and advances them.
//!
//! Doctrine (9.12) — REUSE the saga engine, never a second orchestration loop:
//!   * `StartWorkflow` persists a durable instance, then records a saga via the
//!     EXISTING [`crate::runtime::saga::start_workflow_saga`] seam (which calls
//!     `SagaStore::record_saga`) tagged with [`SagaKind::Workflow`]. The instance
//!     is the durable state that survives a restart / leader change.
//!   * `CancelWorkflow` flips that linked saga into the recoverable state (the
//!     saga engine keeps owning the saga ROW's lifecycle) and moves the instance
//!     to COMPENSATING. The INSTANCE terminal state is owned by the tick's
//!     compensation driver: workflow steps are application-level, so the
//!     data-plane `CompensatorRegistry` can never undo them — instead the tick
//!     emits one `udb.workflow.compensate.step.v1` event per completed step in
//!     REVERSE order (application-driven undo) and settles the instance
//!     COMPENSATING → COMPENSATED, atomically with the events.
//!   * `SignalWorkflow` delivers an external signal to a waiting step and resumes
//!     it: a step declared (in the payload `signal_waits` map) to block on a named
//!     signal parks the instance in WAITING_SIGNAL on the forward pass; the
//!     matching `SignalWorkflow` records the signal and returns it to RUNNING so a
//!     later tick advances it. A COMPENSATING instance is non-signalable (a signal
//!     never reverts an in-flight compensation).
//!   * The leader-elected tick fires transition events ONLY (it never executes a
//!     payload in-process), exactly like the scheduler tick. It also sweeps
//!     RUNNING instances whose last transition exceeded the step timeout: one with
//!     completed steps moves to COMPENSATING (its steps are undone before it
//!     settles COMPENSATED), one with none settles FAILED — both emit
//!     `udb.workflow.failed.v1` as the timeout cause.
//!
//! Every state transition emits one versioned dot-topic outbox event
//! (`udb.workflow.<state>.v1`). Tenant identity always comes from the VERIFIED
//! claim (never the request body); state is durable; the service fails closed on a
//! missing pool, runtime, or outbox relation.
//!
//! Module layout (no god file): [`config`] statics + once-resolved knobs,
//! [`errors`] typed statuses, [`model`] the manifest model + enum<->db + row
//! mapping, [`store`] the scope predicate + projection builders, [`events`] the
//! transactional-outbox writers, [`handlers`] the five RPCs, [`tick`] the
//! leader-elected worker — `mod.rs` keeps only the struct, the builders, and the
//! one-line trait delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::workflow::services::v1 as workflow_pb;
use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::canonical_store::SystemStores;
use crate::runtime::canonical_store::system_store::{CompensationStatus, SagaStatus, SagaStore};
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::workflow::services::v1::workflow_service_server::WorkflowServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod handlers;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod tick;

// Re-exported at the module root for `serve()`, which spawns the tick worker
// under leader election.
pub(crate) use config::WORKFLOW_TICK_BATCH;
pub(crate) use tick::run_workflow_tick_once;

/// Postgres-backed `WorkflowService` handler.
pub struct WorkflowServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for the default `SystemStores` (the saga engine the workflow
    /// is handed to). `None` only in bare unit-test construction.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Transactional-outbox relation; mutations and tick transitions enqueue here.
    pub(crate) outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl WorkflowServiceImpl {
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

    /// Workflow CRUD is durable-only: fail closed when no Postgres pool exists.
    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::workflow_capability_status(
                "postgres_store",
                "postgres_store",
                "workflow service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// The default `SystemStores` (the saga engine). When absent (slim deployment)
    /// a workflow is still durable in its own table but has no linked saga, so
    /// compensation on cancel is unavailable — `StartWorkflow` records that as an
    /// empty `saga_id` rather than failing the create.
    pub(crate) fn system_stores(&self) -> Option<Arc<dyn SystemStores>> {
        self.runtime.as_ref()?.default_system_stores()
    }

    /// Compensating settle for a saga recorded ahead of a workflow-row INSERT that
    /// then failed (16.3.3): the saga is cross-store, so it cannot join the row's
    /// PG transaction, and the canonical saga API exposes no delete — the orphan
    /// is settled terminal instead (`Compensated`/`Completed`: zero forward steps
    /// ran, so nothing stands and the recovery worker must never pick it up).
    /// Best-effort — the RPC has already failed; this only keeps the saga queue
    /// consistent.
    pub(crate) async fn settle_orphan_saga(&self, saga_id: &str) {
        if saga_id.is_empty() {
            return;
        }
        let Some(store) = self.system_stores() else {
            return;
        };
        let Ok(saga_uuid) = saga_id.parse::<Uuid>() else {
            return;
        };
        if let Err(err) = SagaStore::update_saga_status(
            store.as_ref(),
            saga_uuid,
            SagaStatus::Compensated,
            CompensationStatus::Completed,
        )
        .await
        {
            tracing::warn!(error = %err, saga_id, "workflow start: orphan saga settle failed");
        }
    }
}

impl Default for WorkflowServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl WorkflowService for WorkflowServiceImpl {
    async fn start_workflow(
        &self,
        request: Request<workflow_pb::StartWorkflowRequest>,
    ) -> Result<Response<workflow_pb::StartWorkflowResponse>, Status> {
        handlers::start_workflow(self, request).await
    }

    async fn get_workflow(
        &self,
        request: Request<workflow_pb::GetWorkflowRequest>,
    ) -> Result<Response<workflow_pb::GetWorkflowResponse>, Status> {
        handlers::get_workflow(self, request).await
    }

    async fn list_workflows(
        &self,
        request: Request<workflow_pb::ListWorkflowsRequest>,
    ) -> Result<Response<workflow_pb::ListWorkflowsResponse>, Status> {
        handlers::list_workflows(self, request).await
    }

    async fn cancel_workflow(
        &self,
        request: Request<workflow_pb::CancelWorkflowRequest>,
    ) -> Result<Response<workflow_pb::CancelWorkflowResponse>, Status> {
        handlers::cancel_workflow(self, request).await
    }

    async fn signal_workflow(
        &self,
        request: Request<workflow_pb::SignalWorkflowRequest>,
    ) -> Result<Response<workflow_pb::SignalWorkflowResponse>, Status> {
        handlers::signal_workflow(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `WorkflowService`, wired to the broker's Postgres pool, the
    /// runtime (for the saga engine), and the transactional outbox. The
    /// leader-elected tick worker (`run_workflow_tick_once`) is spawned by `serve()`
    /// (not here) under `NativeWorkerHost::spawn_while_leader(WORKER_WORKFLOW_TICK,
    /// ...)`, so a non-leader replica never advances.
    pub(crate) fn build_workflow_service(&self) -> WorkflowServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("workflow", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        WorkflowServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
