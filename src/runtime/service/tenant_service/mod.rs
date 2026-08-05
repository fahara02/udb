//! Native `TenantService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_tenant.{tenants,tenant_configs}` tables.
//!
//! Mirrors `auth_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`](crate::runtime::native_catalog::NativeModel) (see
//! `runtime::native_catalog`), so the SQL here follows the same
//! single-source-of-truth rule as the rest of the native services.
//!
//! Module layout (no god file): [`config`] the statics (entity messages,
//! versioned outbox topics, proto-declared event types, stored-token defaults),
//! [`model`] the manifest model + enum<->db converters + row/JSON decoders,
//! [`store`] the neutral-IR query builders + `ListTenants` scope resolution,
//! [`errors`] the typed statuses + field validators, [`events`] the best-effort
//! outbox emit + no-secrets payloads, [`handlers`] the seven RPCs — `mod.rs`
//! keeps only the struct, the builders/require-guards, and one-line trait
//! delegators.

use std::sync::Arc;

use sqlx::PgPool;
use tonic::{Request, Response, Status};

use crate::generation::CatalogManifest;
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::ChannelManager;

pub use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantServiceServer;

use super::DataBrokerService;

mod config;
mod errors;
mod events;
mod gate;
mod handlers;
mod model;
mod store;
mod tenant_purge;
#[cfg(test)]
mod tests;

// Fail-closed request-time tenant-status gate. Re-exported at the module root so
// the shared method-security tower layer can invoke it on the validated claim
// tenant before dispatch as `crate::runtime::service::tenant_service::tenant_status_gate`
// (see `gate::tenant_status_gate` TODO(leader-wire)). `allow(unused_imports)`
// until that leader-owned call site is wired.
#[allow(unused_imports)]
pub(crate) use gate::tenant_status_gate;

/// Postgres-backed `TenantService` handler.
pub struct TenantServiceImpl {
    pub(crate) pg_pool: Option<PgPool>,
    /// Runtime handle for P4 native-entity data-plane operations. Tenant config is
    /// stored as the real `TenantConfig` proto entity through the native catalog,
    /// neutral IR compiler, and selected backend executor/native driver.
    pub(crate) runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Control-plane mutating/listing RPCs acquire
    /// a per-tenant budget through this so one tenant can't starve the shared
    /// control plane. `None` only in bare unit-test construction (no runtime
    /// wired) — `build_tenant_service` always wires it in production.
    pub(crate) channels: Option<ChannelManager>,
    pub(crate) metrics: Arc<dyn MetricsRecorder>,
    /// Configured outbox relation; `None` disables best-effort purge audit emission.
    pub(crate) outbox_relation: Option<String>,
    /// Redis-backed cluster cutoff denylist for fast post-purge token rejection.
    #[cfg(feature = "redis")]
    pub(crate) jti_denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    /// Active catalog manifest — used by `PurgeTenant` to enumerate the tenant's
    /// owned tables for the ripple hard-delete. Set by `build_tenant_service`;
    /// `None` only in bare unit-test construction (PurgeTenant fails closed then).
    pub(crate) manifest: Option<CatalogManifest>,
}

impl TenantServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            outbox_relation: None,
            #[cfg(feature = "redis")]
            jti_denylist: None,
            manifest: None,
        }
    }

    /// Wire the active catalog manifest (the table set `PurgeTenant` ripples over).
    pub(crate) fn with_manifest(mut self, manifest: Option<CatalogManifest>) -> Self {
        self.manifest = manifest;
        self
    }

    /// PurgeTenant needs the manifest to enumerate tenant-owned tables; fail closed when absent.
    pub(crate) fn require_manifest(&self) -> Result<&CatalogManifest, Status> {
        self.manifest.as_ref().ok_or_else(|| {
            errors::tenant_capability_status(
                "purge_tenant",
                "catalog_manifest",
                "tenant service requires the catalog manifest for purge",
            )
        })
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Wire the runtime used for typed native-entity tenant config persistence.
    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    /// Typed tenant entities persist through native entity dispatch; fail closed when absent.
    pub(crate) fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            errors::tenant_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "tenant service requires runtime native entity dispatch",
            )
        })
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_jti_denylist(
        mut self,
        denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    ) -> Self {
        self.jti_denylist = denylist;
        self
    }

    /// Wire the shared per-tenant fair-admission manager (same one the data plane
    /// uses) so control-plane RPCs are bounded per tenant. No-op (`None`) leaves
    /// admission disabled for bare unit-test construction.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Tenant CRUD is durable-only: fail closed when no Postgres pool exists.
    pub(crate) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            errors::tenant_capability_status(
                "postgres_store",
                "postgres_store",
                "tenant service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for TenantServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl TenantService for TenantServiceImpl {
    async fn create_tenant(
        &self,
        request: Request<tenant_pb::CreateTenantRequest>,
    ) -> Result<Response<tenant_pb::CreateTenantResponse>, Status> {
        handlers::create_tenant(self, request).await
    }

    async fn purge_tenant(
        &self,
        request: Request<tenant_pb::PurgeTenantRequest>,
    ) -> Result<Response<tenant_pb::PurgeTenantResponse>, Status> {
        handlers::purge_tenant(self, request).await
    }

    async fn admin_purge_tenant(
        &self,
        request: Request<tenant_pb::AdminPurgeTenantRequest>,
    ) -> Result<Response<tenant_pb::AdminPurgeTenantResponse>, Status> {
        tenant_purge::admin_purge_tenant(self, request).await
    }

    async fn get_tenant(
        &self,
        request: Request<tenant_pb::GetTenantRequest>,
    ) -> Result<Response<tenant_pb::GetTenantResponse>, Status> {
        handlers::get_tenant(self, request).await
    }

    async fn list_tenants(
        &self,
        request: Request<tenant_pb::ListTenantsRequest>,
    ) -> Result<Response<tenant_pb::ListTenantsResponse>, Status> {
        handlers::list_tenants(self, request).await
    }

    async fn update_tenant(
        &self,
        request: Request<tenant_pb::UpdateTenantRequest>,
    ) -> Result<Response<tenant_pb::UpdateTenantResponse>, Status> {
        handlers::update_tenant(self, request).await
    }

    async fn get_tenant_config(
        &self,
        request: Request<tenant_pb::GetTenantConfigRequest>,
    ) -> Result<Response<tenant_pb::GetTenantConfigResponse>, Status> {
        handlers::get_tenant_config(self, request).await
    }

    async fn update_tenant_config(
        &self,
        request: Request<tenant_pb::UpdateTenantConfigRequest>,
    ) -> Result<Response<tenant_pb::UpdateTenantConfigResponse>, Status> {
        handlers::update_tenant_config(self, request).await
    }
}

impl DataBrokerService {
    /// Build the native `TenantService`, wired to the broker's Postgres pool.
    pub(crate) fn build_tenant_service(&self) -> TenantServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("tenant", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        #[cfg(feature = "redis")]
        let jti_denylist = runtime.redis_clone().map(|redis| {
            crate::runtime::authn::revocation::JtiDenylist::new(
                redis,
                crate::runtime::security::SecurityConfig::current().jwt_access_ttl_secs,
            )
        });
        let service = TenantServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_outbox(Some(outbox))
            .with_manifest(Some(self.manifest.clone()));
        #[cfg(feature = "redis")]
        let service = service.with_jti_denylist(jti_denylist);
        service
    }
}
