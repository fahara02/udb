//! Native `LockService` (master-plan 9.2) — distributed application locks.
//!
//! Mirrors `tenant_service`: proto-driven, no in-memory store, no hand-mapped
//! schema. Mutual exclusion is the portable `udb_advisory_leases` primitive taken
//! atomically at the SQL layer — reused via
//! [`DataBrokerRuntime::try_acquire_native_lease`] /
//! [`DataBrokerRuntime::release_native_lease`] — NOT re-implemented here. The
//! durable, tenant-scoped `udb_lock.locks` row records who holds each lock and
//! the monotone fencing token handed out at grant time, so a slow/partitioned
//! holder presenting a stale token is fenced off (`failed_precondition`).
//!
//! Doctrine (Phase 9): the lock name is always derived from the VERIFIED claim
//! tenant (never the request body), per-tenant active-lock quota is enforced,
//! admission is fair (`native_helpers::admit_on`), state is durable in the
//! canonical store, and every mutation emits a versioned dot-topic outbox event.
//!
//! Lifecycle (16.5.1): the leader-elected expiry reaper ([`run_lock_expiry_once`],
//! spawned under `WORKER_LOCK_EXPIRY_REAPER`) flips lapsed `HELD` rows to
//! `EXPIRED` and emits `udb.lock.lock.expired.v1` per lock, transactionally with
//! the flip; independently, the acquire-time quota count excludes lapsed rows so
//! an un-released lock never exhausts the tenant budget between sweeps.
//!
//! Module layout (no god file): [`config`] the statics + TTL/quota/interval
//! knobs, [`errors`] the typed statuses + validators + fencing check, [`model`]
//! the DTO/JSON decoders + lease-name helper, [`store`] the neutral-IR query /
//! record builders + manifest model, [`events`] the outbox emit + shared payload,
//! [`workers`] the leader-elected expiry reaper, [`handlers`] the five RPCs —
//! `mod.rs` keeps only the struct, the builders/require-guard, and one-line trait
//! delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::lock::services::v1 as lock_pb;
use crate::proto::udb::core::lock::services::v1::lock_service_server::LockService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::lock::services::v1::lock_service_server::LockServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod handlers;
mod model;
mod store;
#[cfg(test)]
mod tests;
mod workers;

// Re-exported at the module root for `serve()`, which spawns the expiry-reaper
// worker under leader election.
pub(crate) use config::{LOCK_EXPIRY_SWEEP_BATCH, lock_expiry_interval};
pub(crate) use workers::run_lock_expiry_once;

// gate 25 (lock-fencing-at-commit): the DataBroker mutation path reuses the
// LockService's entity name (to resolve the durable lock table from the native
// manifest) and its PURE fencing-token freshness decision — so no lock logic is
// duplicated in the data plane, only a scoped read of the row the service owns.
pub(crate) use config::LOCK_MSG;
pub(crate) use errors::ensure_fencing_token_fresh;

/// Postgres-backed `LockService` handler.
pub struct LockServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `lock`).
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for the advisory-lease primitive, the monotone fencing-token
    /// source (canonical outbox high-water mark), and typed native-entity dispatch.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` drops events LOUDLY (error log +
    /// `udb_outbox_enqueue_failures_total`) — see [`events::emit_lock_event`].
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
}

impl LockServiceImpl {
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

    /// Lock state is durable-only: fail closed when no canonical/PG store exists.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::lock_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "lock service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for LockServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl LockService for LockServiceImpl {
    async fn acquire_lock(
        &self,
        request: Request<lock_pb::AcquireLockRequest>,
    ) -> Result<Response<lock_pb::AcquireLockResponse>, Status> {
        handlers::acquire_lock(self, request).await
    }

    async fn renew_lock(
        &self,
        request: Request<lock_pb::RenewLockRequest>,
    ) -> Result<Response<lock_pb::RenewLockResponse>, Status> {
        handlers::renew_lock(self, request).await
    }

    async fn release_lock(
        &self,
        request: Request<lock_pb::ReleaseLockRequest>,
    ) -> Result<Response<lock_pb::ReleaseLockResponse>, Status> {
        handlers::release_lock(self, request).await
    }

    async fn get_lock(
        &self,
        request: Request<lock_pb::GetLockRequest>,
    ) -> Result<Response<lock_pb::GetLockResponse>, Status> {
        handlers::get_lock(self, request).await
    }

    async fn list_locks(
        &self,
        request: Request<lock_pb::ListLocksRequest>,
    ) -> Result<Response<lock_pb::ListLocksResponse>, Status> {
        handlers::list_locks(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `LockService`, wired to the broker's Postgres pool, the
    /// advisory-lease/fencing-token runtime, and the shared outbox.
    pub(crate) fn build_lock_service(&self) -> LockServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime.native_store_pool_for_service("lock", true, "").ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        LockServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
