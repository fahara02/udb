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
//! Retention pruning is implemented here in [`retention::prune_tenant_backups`]:
//! a bounded, fail-safe routine that enforces a `BackupPolicy`'s `retention_days`
//! / `max_retained_backups` by deleting the oldest runs (journal rows + their
//! encrypted objects), tenant-scoped, and never on an unconfigured policy — so
//! runs and objects no longer accumulate without bound. The pure prune selector
//! ([`retention::runs_to_prune`]) is unit-tested.
//!
//! The leader-lane DRIVERS both triggers call are implemented here in
//! [`retention`]: [`retention::enabled_backup_policies`] is the bounded,
//! project-by-project cross-tenant read of every ENABLED `BackupPolicy`;
//! [`retention::run_backup_retention_once`] prunes each tenant's over-retention
//! runs; and [`retention::run_scheduled_backups_once`] fires a due scheduled
//! backup per policy through the SAME internal routine `StartTenantBackup` uses
//! ([`export::run_tenant_backup`]), never through the gRPC layer. The due
//! decision ([`retention::backup_due`]) is a PURE, unit-tested comparison.
//!
//! The periodic, leader-elected spawn that calls these drivers lives in the
//! shared scheduler lane (`service::serve()` under
//! `singleton::WORKER_BACKUP_RETENTION`). This module owns the durable policy
//! contract, backup/restore mechanics, retention pruning, and maintenance
//! drivers.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::catalog::{CatalogManager, DEFAULT_PROJECT_ID};
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, storage_object_defaults,
};

// `config` (the `backup_maintenance_interval` cadence knob) and `retention` (the
// leader-lane maintenance drivers `run_backup_retention_once` /
// `run_scheduled_backups_once`) are `pub(crate)` so the leader-elected spawn in
// `service::serve()` can reach them; see the `TODO(leader-wire)` in `retention.rs`.
pub(crate) mod config;
mod errors;
mod events;
mod export;
mod handlers;
mod import;
mod model;
pub(crate) mod retention;
mod store;
#[cfg(test)]
mod tests;

/// Postgres-backed `BackupService` handler.
pub struct BackupServiceImpl {
    /// Startup Backup pool used for best-effort outbox emission and bare tests.
    /// Project data and maintenance reads resolve their exact canonical store.
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle: typed native-entity dispatch (journal + policy), the
    /// encrypt/decrypt-at-rest envelope, and the object-store helpers.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    pub(crate) outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// Live multi-project catalog. Backup/restore resolves the request project's
    /// current state at operation start; it must never retain a startup manifest.
    pub(crate) catalog: Option<Arc<CatalogManager>>,
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
            catalog: None,
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

    pub(crate) fn with_catalog(mut self, catalog: Option<Arc<CatalogManager>>) -> Self {
        self.catalog = catalog;
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

    /// Resolve an explicitly published project catalog without demanding that
    /// the project's current table topology is executable as a logical backup.
    /// Inventory reads need the exact project/store authority, but must remain
    /// available while an operator repairs a temporarily non-backup-capable
    /// topology.
    pub(crate) fn require_active_project(
        &self,
        requested_project_id: &str,
    ) -> Result<String, Status> {
        let catalog = self.catalog.as_deref().ok_or_else(|| {
            errors::backup_capability_status(
                "project_catalog_binding",
                "catalog_manifest",
                "backup service requires the live project catalog",
            )
        })?;
        let project_id = match requested_project_id.trim() {
            "" => DEFAULT_PROJECT_ID.to_string(),
            value => value.to_string(),
        };
        if catalog.active_exact_for(&project_id).is_none() {
            return Err(errors::backup_policy_status(
                "resolve_project_topology",
                "backup_project_catalog_not_active",
                format!(
                    "project '{project_id}' has no explicitly active catalog; default-project fallback is refused"
                ),
            ));
        }
        Ok(project_id)
    }

    /// Resolve one explicitly active project, its current catalog, and the single
    /// canonical Postgres write instance that owns every relational backup table.
    /// Multi-instance catalogs are refused until a coordinated distributed
    /// snapshot protocol exists; silently producing a fuzzy partial backup is not
    /// an acceptable fallback.
    pub(crate) fn resolve_project_snapshot(
        &self,
        requested_project_id: &str,
    ) -> Result<BackupProjectSnapshot, Status> {
        let runtime = self.require_runtime()?;
        let catalog = self.catalog.as_deref().ok_or_else(|| {
            errors::backup_capability_status(
                "tenant_table_enumeration",
                "catalog_manifest",
                "backup service requires the live project catalog to enumerate tenant tables",
            )
        })?;
        let project_id = self.require_active_project(requested_project_id)?;
        let state = catalog
            .active_exact_for(&project_id)
            .expect("require_active_project established exact catalog authority");
        let plan = crate::runtime::core::tenant_purge::plan_tenant_purge(&state.manifest);
        let default_instance = runtime
            .choose_instance_name_for_project("postgres", true, &project_id)
            .map(str::to_string);
        let mut instances = std::collections::BTreeSet::new();

        for target in &plan.targets {
            let table = state
                .manifest
                .tables
                .iter()
                .find(|table| table.schema == target.schema && table.table == target.table)
                .ok_or_else(|| {
                    errors::backup_internal_status(
                        "resolve_project_topology",
                        format!(
                            "catalog table disappeared while resolving backup topology: {}.{}",
                            target.schema, target.table
                        ),
                    )
                })?;
            let owners: Vec<_> = table
                .projections
                .iter()
                .filter(|projection| projection.write_owner)
                .collect();
            if owners.len() != 1 {
                return Err(errors::backup_policy_status(
                    "resolve_project_topology",
                    "backup_write_owner_not_unique",
                    format!(
                        "backup table {}.{} must have exactly one canonical write owner (found {})",
                        target.schema,
                        target.table,
                        owners.len()
                    ),
                ));
            }
            let owner = owners[0];
            if !owner.backend.eq_ignore_ascii_case("postgres")
                || !owner.projection_kind.eq_ignore_ascii_case("relational")
            {
                return Err(errors::backup_capability_status(
                    "resolve_project_topology",
                    "postgres_relational_write_owner",
                    "logical tenant backup currently requires every canonical write owner to be relational Postgres",
                ));
            }
            let instance = if owner.instance.trim().is_empty() {
                default_instance
                    .clone()
                    .unwrap_or_else(|| "primary".to_string())
            } else {
                owner.instance.trim().to_string()
            };
            instances.insert(instance);
        }

        if instances.len() > 1 {
            return Err(errors::backup_capability_status(
                "resolve_project_topology",
                "coordinated_multi_instance_snapshot",
                "project backup spans multiple canonical Postgres instances; a coordinated snapshot is required and fuzzy completion is refused",
            ));
        }
        let postgres_instance = instances
            .into_iter()
            .next()
            .or(default_instance)
            .unwrap_or_else(|| "primary".to_string());
        let pool = match runtime.pg_pool_for_instance(Some(&postgres_instance)) {
            Ok(pool) => pool.clone(),
            Err(_) if postgres_instance == "primary" => runtime.pg_pool_for_instance(None)?.clone(),
            Err(err) => return Err(err),
        };

        Ok(BackupProjectSnapshot {
            project_id,
            catalog_version: state.metadata.version.clone(),
            catalog_checksum: state.metadata.checksum.clone(),
            manifest_checksum: state.manifest.checksum_sha256.clone(),
            manifest: state.manifest.clone(),
            postgres_instance,
            pool,
        })
    }
}

/// Immutable operation-start view used by export and restore. It deliberately
/// contains a concrete pool/instance rather than a routing hint that could be
/// re-evaluated differently halfway through a run.
pub(crate) struct BackupProjectSnapshot {
    pub(crate) project_id: String,
    pub(crate) catalog_version: String,
    pub(crate) catalog_checksum: String,
    pub(crate) manifest_checksum: String,
    pub(crate) manifest: Arc<crate::generation::CatalogManifest>,
    pub(crate) postgres_instance: String,
    pub(crate) pool: PgPool,
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
        // Best-effort outbox emission keeps a startup pool. Project data and
        // maintenance operations resolve an exact project store per operation.
        let event_pool = runtime
            .native_store_pool_for_service("backup", true, DEFAULT_PROJECT_ID)
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        let (object_backend, object_bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        BackupServiceImpl::new()
            .with_postgres(event_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_catalog(Some(self.catalog.clone()))
            .with_object(object_backend, object_bucket)
    }
}
