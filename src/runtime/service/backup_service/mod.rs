//! Native `BackupService` (master-plan 9.10) — tenant-level logical backup and
//! restore.
//!
//! Mirrors `lock_service`/`tenant_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. The retention policy and the per-run journal are real
//! proto entities persisted through native-entity dispatch; the backup/restore
//! ROW movement is raw, RLS-bypassing SQL on the tenant's owned tables, so it is
//! gated through the SHARED fail-closed movement validator and uses the SHARED
//! tenant-table enumeration — never a second copy of either.
//!
//! Doctrine (Phase 9 / movement-safety):
//!   * Backup/restore RPCs are gated through
//!     [`crate::runtime::tenant_movement::validate_tenant_movement_scope`] with
//!     `TenantMovementOperation::{BackupExport, RestoreImport}` — the SAME
//!     validator the startup gate uses; no bespoke scope check here.
//!   * The set of tenant-owned tables comes from the SHARED
//!     [`crate::runtime::core::tenant_purge::plan_tenant_purge`] planner (which
//!     resolves the tenant column via `generation::sql::resolve_tenant_column_ref`)
//!     — the identical enumeration `PurgeTenant` uses. Tables WITHOUT a resolvable
//!     tenant column are REPORTED as excluded in the manifest, never silently
//!     skipped (no capability lie).
//!   * Row payloads are encrypted at rest via
//!     [`DataBrokerRuntime::encrypt_secret_at_rest`] and written through the
//!     existing object-store helpers — no new crypto, no new object layer.
//!   * A restore refuses to write over a live (non-empty) target tenant
//!     (`failed_precondition`) and rewrites the tenant column to the target on
//!     insert. Cross-tenant restores require explicit privileged approval through
//!     the shared validator.
//!   * Every mutation appends a durable journal row and emits a versioned
//!     dot-topic outbox event.
//!
//! Retention pruning runs as a SchedulerService (9.3) job: a leader-elected tick
//! fires `udb.scheduler.job.fired.v1` for a due `BackupPolicy`, which a worker
//! turns into a StartTenantBackup and prunes per the policy's retention window.
//! The scheduling wiring itself lives in the scheduler lane; this module owns the
//! durable policy contract and the backup/restore mechanics.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::generation::CatalogManifest;
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, storage_object_defaults,
};

mod config;
mod errors;
mod events;
mod export;
mod handlers;
mod import;
mod model;
mod store;
#[cfg(test)]
mod tests;

/// Postgres-backed `BackupService` handler.
pub struct BackupServiceImpl {
    /// Pool for the raw tenant-row movement SQL and the outbox. Resolved from this
    /// service's `native_service` binding (the data-plane Postgres in a shared
    /// deployment), mirroring `tenant_service::PurgeTenant`.
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle: typed native-entity dispatch (journal + policy), the
    /// encrypt/decrypt-at-rest envelope, and the object-store helpers.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// Active catalog manifest — the table set the backup/restore ripples over,
    /// enumerated via the SHARED purge planner. `None` only in bare unit-test
    /// construction (the data-movement RPCs fail closed then).
    pub(crate) manifest: Option<CatalogManifest>,
    /// Default object-store target when neither the request nor a policy overrides.
    pub(crate) object_backend: String,
    pub(crate) object_bucket: String,
}

impl BackupServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            manifest: None,
            object_backend: DEFAULT_OBJECT_BACKEND.to_string(),
            object_bucket: DEFAULT_OBJECT_BUCKET.to_string(),
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

    pub(crate) fn with_manifest(mut self, manifest: Option<CatalogManifest>) -> Self {
        self.manifest = manifest;
        self
    }

    pub(crate) fn with_object(mut self, backend: String, bucket: String) -> Self {
        if !backend.trim().is_empty() {
            self.object_backend = backend;
        }
        if !bucket.trim().is_empty() {
            self.object_bucket = bucket;
        }
        self
    }

    /// Typed journal/policy entities + encryption + object IO all live on the
    /// runtime; fail closed when no runtime is configured.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::backup_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "backup service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    /// Raw tenant-row movement is durable-only: fail closed when no PG pool exists.
    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::backup_capability_status(
                "postgres_store",
                "postgres_store",
                "backup service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// The backup/restore ripple enumerates the same manifest table set the purge
    /// planner walks; fail closed when the manifest is absent.
    pub(crate) fn require_manifest(&self) -> Result<&CatalogManifest, Status> {
        self.manifest.as_ref().ok_or_else(|| {
            errors::backup_capability_status(
                "tenant_table_enumeration",
                "catalog_manifest",
                "backup service requires the catalog manifest to enumerate tenant tables",
            )
        })
    }
}

impl Default for BackupServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl BackupService for BackupServiceImpl {
    async fn start_tenant_backup(
        &self,
        request: Request<backup_pb::StartTenantBackupRequest>,
    ) -> Result<Response<backup_pb::StartTenantBackupResponse>, Status> {
        export::start_tenant_backup(self, request).await
    }

    async fn restore_tenant(
        &self,
        request: Request<backup_pb::RestoreTenantRequest>,
    ) -> Result<Response<backup_pb::RestoreTenantResponse>, Status> {
        import::restore_tenant(self, request).await
    }

    async fn list_backups(
        &self,
        request: Request<backup_pb::ListBackupsRequest>,
    ) -> Result<Response<backup_pb::ListBackupsResponse>, Status> {
        handlers::list_backups(self, request).await
    }

    async fn get_backup(
        &self,
        request: Request<backup_pb::GetBackupRequest>,
    ) -> Result<Response<backup_pb::GetBackupResponse>, Status> {
        handlers::get_backup(self, request).await
    }

    async fn put_backup_policy(
        &self,
        request: Request<backup_pb::PutBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::PutBackupPolicyResponse>, Status> {
        handlers::put_backup_policy(self, request).await
    }

    async fn get_backup_policy(
        &self,
        request: Request<backup_pb::GetBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::GetBackupPolicyResponse>, Status> {
        handlers::get_backup_policy(self, request).await
    }

    async fn list_backup_policies(
        &self,
        request: Request<backup_pb::ListBackupPoliciesRequest>,
    ) -> Result<Response<backup_pb::ListBackupPoliciesResponse>, Status> {
        handlers::list_backup_policies(self, request).await
    }

    async fn delete_backup_policy(
        &self,
        request: Request<backup_pb::DeleteBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::DeleteBackupPolicyResponse>, Status> {
        handlers::delete_backup_policy(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `BackupService`, wired to the broker's Postgres pool (for
    /// the raw tenant-row movement + outbox), the runtime (native-entity journal/
    /// policy dispatch + encrypt-at-rest + object IO), the active manifest (the
    /// tenant-table enumeration source), and the default object-store target.
    pub(crate) fn build_backup_service(&self) -> BackupServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("backup", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        let (object_backend, object_bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        BackupServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_manifest(Some(self.manifest.clone()))
            .with_object(object_backend, object_bucket)
    }
}
