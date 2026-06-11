#![allow(clippy::result_large_err)]

use std::fs;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ast::ProtoSchema;
use crate::cdc::{CdcEngine, CdcRedactionMode};
use crate::engine::FsmState;
use crate::generation::CatalogManifest;
use crate::lifecycle::run_startup_lifecycle;
use crate::metrics::{MetricsRecorder, NoopMetrics, PrometheusMetrics};
use crate::proto::data_broker_server::{DataBroker, DataBrokerServer};
use crate::proto::{
    AdminAuditLogRecord, AdminAuditLogRequest, AdminAuditLogResponse, AdminAuditVerifyRequest,
    AdminAuditVerifyResponse, AdminBackendSummary, AdminCatalogSummary, AdminCdcSummary,
    AdminSagaSummary, AdminSummaryRequest, AdminSummaryResponse, BackendInstanceStatus,
    CapabilitiesRequest, CapabilitiesResponse, CatalogManifestRequest, CatalogManifestResponse,
    CatalogValidationResponse, CatalogVersionListResponse, CatalogVersionRequest,
    CatalogVersionResponse, CdcControlRequest, CdcEnvelope, CdcRedactionPreviewRequest,
    CdcRedactionPreviewResponse, CdcStatusResponse, CdcSubscriptionRequest, Chunk, DeleteRequest,
    DlqActionRequest, DlqEventRecord, DlqEventRequest, DlqEventResponse, DlqListRequest,
    DlqListResponse, EnqueueOutboxEventRequest, EnqueueOutboxEventResponse, EnsureProjectRequest,
    GenericDispatchRequest, GenericDispatchResponse, HealthReportRequest, HealthReportResponse,
    MessageFieldDescriptor, MessageSchemaDescriptor, MessageSchemaListRequest,
    MessageSchemaListResponse, MessageSchemaLookupRequest, MessageSchemaLookupResponse,
    MigrationApplyRequest, MigrationPlanRequest, MigrationPlanResponse, MigrationRunListRequest,
    MigrationRunListResponse, MigrationRunRequest, MigrationStatusResponse, MultipartUploadRequest,
    MultipartUploadResponse, Mutation, MutationResponse, PolicyLintResponse, PolicyListRequest,
    PolicyListResponse, PolicyRecord, PolicyRequest, ProjectListRequest, ProjectListResponse,
    ProjectRecord, ProjectionDriftDivergentRow, ProjectionDriftScanRequest,
    ProjectionDriftScanResponse, ProjectionDriftTargetReport, PutPolicyRequest, RecordSet,
    ResourceAdminRequest, ResourceListResponse, SagaListRequest, SagaListResponse, SagaRecord,
    SagaRequest, SagaResponse, SelectRequest, StageCatalogRequest, TxStatus, UpsertRequest,
    UrlRequest, UrlResponse, VectorHybridSearchRequest, VectorSearchRequest, VectorSet,
    VectorUpsertRequest, ViewDefinition,
};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::authz::{Authorizer, AuthzQuery, AuthzSnapshot, Principal, ResourceRef};
use crate::security::{
    AbacPolicy, SecurityConfig, SecurityContext, enforce_select_export_controls, evaluate_abac,
    ip_matches_allow_entry, security_from_request, validate_bearer_token,
};

mod analytics_service;
mod asset_service;
mod auth_service;
// Phase 10: top-level `udb doctor` folds auth-readiness into one shared
// readiness fact set; re-export the adapter so the bin crate can reach it.
pub use auth_service::auth_readiness_triples;
// urgent_fix #20: offline root-bootstrap entry point for the `udb auth bootstrap`
// CLI (the bin crate can only reach `pub` items re-exported to this level).
pub use auth_service::bootstrap_admin_user;
/// Wildcard CDC topic patterns covering every native auth/authz/apikey/idp/ops
/// event. Re-exported so the CDC config (`crate::runtime::cdc`) can guarantee a
/// tightened operator topic allowlist never silences auth security/audit events
/// — see `CdcConfig::normalize`. (The `auth_service` module is private to this
/// `service` module, so this `pub(crate)` re-export is the reachable handle.)
pub(crate) use auth_service::events::topics::AUTH_TOPIC_PATTERNS;
mod method_security;
mod native_helpers;
pub mod native_registry;
mod notification_service;
mod storage_service;
mod tenant_service;
mod webrtc_service;

const UDB_FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("udb_descriptor");

fn build_abac_snapshot(
    version: impl Into<String>,
    policies: &[AbacPolicy],
    default_allow: bool,
) -> Arc<AuthzSnapshot> {
    let mut snapshot = AuthzSnapshot::from_abac_policies(version, policies);
    snapshot.default_allow = default_allow;
    Arc::new(snapshot)
}

/// Whether the v2 authz decision engine is enabled for broker authorization.
/// Read once (`UDB_AUTHZ_V2`). Default ON as of item 132 — parity with the
/// legacy `evaluate_abac` path is asserted by `broker_v2_matches_legacy_abac_decisions`.
/// Set `UDB_AUTHZ_V2=0|false|no|off` to fall back to the legacy path.
fn authz_v2_enabled() -> bool {
    use std::sync::OnceLock;
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("UDB_AUTHZ_V2")
            .map(|v| !matches!(v.as_str(), "0" | "false" | "no" | "off"))
            .unwrap_or(true)
    })
}

fn startup_bool_env(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

/// Fold the uniform RPC prologue shared by ~46 handlers: start the timing
/// clock, extract the [`SecurityContext`] from request metadata, and run the
/// standard `authorize(security, "*", method)` gate. On any failure it returns
/// from the enclosing handler via `self.record_grpc(method, started, Err(..))`
/// so per-method gRPC metrics stay accurate.
///
/// Binds `started` and `security` into the caller's scope:
///
/// ```ignore
/// let (started, security) = authorized_call!(self, request, "ListPolicies");
/// ```
///
/// Only applicable to handlers whose prologue is exactly this triple. Handlers
/// that need the `authorize` decision id, a non-`"*"` message type, or other
/// bespoke setup (e.g. `delete_inner`) must keep their hand-written prologue.
macro_rules! authorized_call {
    ($self:expr, $request:expr, $method:literal) => {{
        let started = Instant::now();
        let security = match security_from_request(&$request) {
            Ok(s) => s,
            Err(e) => return $self.record_grpc($method, started, Err(e)),
        };
        if let Err(err) = $self.authorize(&security, "*", $method).await {
            return $self.record_grpc($method, started, Err(err));
        }
        (started, security)
    }};
}

#[derive(Debug, Clone)]
pub struct DataBrokerService {
    pub catalog: Arc<crate::runtime::catalog::CatalogManager>,
    pub manifest: CatalogManifest,
    pub runtime: Arc<ArcSwap<DataBrokerRuntime>>,
    lifecycle_state: Arc<RwLock<FsmState>>,
    abac_policies: Arc<RwLock<Vec<AbacPolicy>>>,
    abac_snapshot: Arc<RwLock<Arc<AuthzSnapshot>>>,
    metrics: Arc<dyn MetricsRecorder>,
    cdc_engine: Option<Arc<CdcEngine>>,
    projection_engine: Option<Arc<crate::runtime::projection::ProjectionEngine>>,
    #[cfg(feature = "redis")]
    rate_limit_redis: Arc<tokio::sync::Mutex<Option<redis::aio::MultiplexedConnection>>>,
    abac_default_allow: bool,
    /// Per-instance override for the v2 authz decision engine. `None` falls back
    /// to the process-wide `UDB_AUTHZ_V2` env flag (`authz_v2_enabled`); `Some(b)`
    /// forces v2 on/off for this instance. Lets tests exercise the v2 broker gate
    /// deterministically without racing on the global `OnceLock`, and is the seam
    /// the staged rollout (item 132) flips once parity is proven.
    abac_v2_override: Option<bool>,
}

pub(crate) const UDB_PROTOCOL_VERSION: &str = "1.0.0";

pub(crate) const SUPPORTED_RPC_NAMES: &[&str] = &[
    "Select",
    "BatchSelect",
    "SelectV2",
    "Upsert",
    "BatchUpsert",
    "Delete",
    "VectorSearch",
    "VectorHybridSearch",
    "VectorUpsert",
    "VectorBatchUpsert",
    "PutObject",
    "GetObject",
    "GeneratePresignedUrl",
    "InitiateMultipartUpload",
    "BeginTx",
    "PublishCDC",
    "EnqueueOutboxEvent",
    "StageCatalog",
    "ActivateCatalog",
    "RollbackCatalog",
    "ValidateCatalog",
    "GetCatalogVersions",
    "GetCatalogVersion",
    "PlanMigration",
    "ApplyMigration",
    "GetMigrationStatus",
    "ListMigrationRuns",
    "ApproveMigrationPlan",
    "ListDlqEvents",
    "GetDlqEvent",
    "ReplayDlqEvent",
    "DismissDlqEvent",
    "QuarantineDlqEvent",
    "GetCdcStatus",
    "PauseCdc",
    "ResumeCdc",
    "StepDownCdcLeader",
    "PreviewCdcRedaction",
    "ScanProjectionDrift",
    "ListSagas",
    "GetSaga",
    "RetrySagaCompensation",
    "MarkSagaReviewed",
    "ListPolicies",
    "PutPolicy",
    "DeletePolicy",
    "ReloadPolicies",
    "LintPolicies",
    "GenericDispatch",
    "EnsureResource",
    "DropResource",
    "ListResources",
    "CreateMaterializedView",
    "CacheGet",
    "CacheSet",
    "CacheDelete",
    "CacheScan",
    "DocumentGet",
    "DocumentFind",
    "DocumentUpsert",
    "DocumentDelete",
    "GraphQuery",
    "GraphMutate",
    "TimeSeriesWrite",
    "TimeSeriesQuery",
    "AnalyticalQuery",
    "GetCapabilities",
    "GetCatalogManifest",
    "LookupMessageSchema",
    "ListMessageSchemas",
    "GetHealthReport",
    "EnsureProject",
    "ListProjects",
    "GetAdminSummary",
    "ListAdminAuditLogs",
    "VerifyAdminAuditLog",
];

impl DataBrokerService {
    pub fn new(manifest: CatalogManifest) -> Self {
        let catalog = Arc::new(crate::runtime::catalog::CatalogManager::new(
            manifest.clone(),
        ));
        Self {
            catalog,
            manifest,
            runtime: Arc::new(ArcSwap::from_pointee(DataBrokerRuntime::planning_only())),
            lifecycle_state: Arc::new(RwLock::new(FsmState::Idle)),
            abac_policies: Arc::new(RwLock::new(Vec::new())),
            abac_snapshot: Arc::new(RwLock::new(build_abac_snapshot("live-abac", &[], false))),
            metrics: service_metrics_recorder(),
            cdc_engine: None,
            projection_engine: None,
            #[cfg(feature = "redis")]
            rate_limit_redis: Arc::new(tokio::sync::Mutex::new(None)),
            abac_default_allow: false,
            abac_v2_override: None,
        }
    }

    pub fn with_runtime(manifest: CatalogManifest, runtime: DataBrokerRuntime) -> Self {
        let catalog = Arc::new(crate::runtime::catalog::CatalogManager::new(
            manifest.clone(),
        ));
        Self {
            catalog,
            manifest,
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            lifecycle_state: Arc::new(RwLock::new(FsmState::Completed)),
            abac_policies: Arc::new(RwLock::new(Vec::new())),
            abac_snapshot: Arc::new(RwLock::new(build_abac_snapshot("live-abac", &[], false))),
            metrics: service_metrics_recorder(),
            cdc_engine: None,
            projection_engine: None,
            #[cfg(feature = "redis")]
            rate_limit_redis: Arc::new(tokio::sync::Mutex::new(None)),
            abac_default_allow: false,
            abac_v2_override: None,
        }
    }

    pub fn with_runtime_and_state(
        manifest: CatalogManifest,
        runtime: DataBrokerRuntime,
        lifecycle_state: Arc<RwLock<FsmState>>,
        abac_policies: Arc<RwLock<Vec<AbacPolicy>>>,
        metrics: Arc<dyn MetricsRecorder>,
        cdc_engine: Option<Arc<CdcEngine>>,
        abac_default_allow: bool,
    ) -> Self {
        let catalog = Arc::new(crate::runtime::catalog::CatalogManager::new(
            manifest.clone(),
        ));
        let abac_snapshot = abac_policies
            .read()
            .map(|policies| build_abac_snapshot("live-abac", &policies, abac_default_allow))
            .unwrap_or_else(|_| build_abac_snapshot("live-abac", &[], abac_default_allow));
        Self {
            catalog,
            manifest,
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            lifecycle_state,
            abac_policies,
            abac_snapshot: Arc::new(RwLock::new(abac_snapshot)),
            metrics,
            cdc_engine,
            projection_engine: None,
            #[cfg(feature = "redis")]
            rate_limit_redis: Arc::new(tokio::sync::Mutex::new(None)),
            abac_default_allow,
            abac_v2_override: None,
        }
    }

    /// Force the v2 authz decision engine on/off for this instance, overriding
    /// the process-wide `UDB_AUTHZ_V2` env flag. Used by broker authz tests to
    /// exercise the v2 gate deterministically (the env flag is a cached
    /// `OnceLock`, so it cannot vary per test).
    #[cfg(test)]
    pub(crate) fn set_authz_v2_override(&mut self, on: bool) {
        self.abac_v2_override = Some(on);
        self.refresh_abac_snapshot();
    }

    fn replace_abac_policies(&self, fresh: Vec<AbacPolicy>) {
        let snapshot = build_abac_snapshot("live-abac", &fresh, self.abac_default_allow);
        if let Ok(mut guard) = self.abac_policies.write() {
            *guard = fresh;
        }
        if let Ok(mut guard) = self.abac_snapshot.write() {
            *guard = snapshot;
        }
    }

    fn refresh_abac_snapshot(&self) {
        let snapshot = self
            .abac_policies
            .read()
            .map(|policies| build_abac_snapshot("live-abac", &policies, self.abac_default_allow))
            .unwrap_or_else(|_| build_abac_snapshot("live-abac", &[], self.abac_default_allow));
        if let Ok(mut guard) = self.abac_snapshot.write() {
            *guard = snapshot;
        }
    }

    fn current_abac_snapshot(&self) -> Arc<AuthzSnapshot> {
        if let Ok(snapshot_guard) = self.abac_snapshot.read() {
            let snapshot = Arc::clone(&*snapshot_guard);
            if let Ok(policy_guard) = self.abac_policies.read()
                && snapshot.policies.len() == policy_guard.len()
            {
                return snapshot;
            }
        }
        self.refresh_abac_snapshot();
        self.abac_snapshot
            .read()
            .map(|snapshot| Arc::clone(&*snapshot))
            .unwrap_or_else(|_| build_abac_snapshot("live-abac", &[], self.abac_default_allow))
    }

    /// The shared, atomically-reloadable ABAC/authz snapshot cell. Cloned (Arc)
    /// so callers can build a `'static` version probe over the live snapshot
    /// (used by the control-plane reload subscriber to detect authz-only policy
    /// changes that do not alter the sourced RLS/method-security registry).
    fn abac_snapshot(&self) -> Arc<RwLock<Arc<AuthzSnapshot>>> {
        self.abac_snapshot.clone()
    }

    pub fn runtime_snapshot(&self) -> Arc<DataBrokerRuntime> {
        self.runtime.load_full()
    }

    pub async fn reload_runtime_from_config(
        &self,
        config: crate::runtime::config::UdbConfig,
        options: crate::runtime::ConfigReloadOptions,
    ) -> crate::runtime::ConfigReloadReport {
        let mut next = self.runtime.load_full().as_ref().clone();
        let report = next.reload_from_config(config, options).await;
        if report.applied {
            self.runtime.store(Arc::new(next));
        }
        report
    }

    pub async fn reload_runtime_from_env(
        &self,
        reason: impl Into<String>,
    ) -> crate::runtime::ConfigReloadReport {
        self.reload_runtime_from_config(
            crate::runtime::config::UdbConfig::from_merged_env(),
            crate::runtime::ConfigReloadOptions {
                reason: reason.into(),
                require_connected_backends: true,
                rollback_on_failed_health: true,
                ..crate::runtime::ConfigReloadOptions::default()
            },
        )
        .await
    }

    pub(crate) fn ensure_ready(&self) -> Result<(), Status> {
        let state = self
            .lifecycle_state
            .read()
            .map(|state| state.clone())
            .unwrap_or(FsmState::Error);
        if state == FsmState::Completed {
            Ok(())
        } else {
            Err(Status::unavailable(format!(
                "UDB startup lifecycle is {}, DataBroker is not ready",
                state.as_str()
            )))
        }
    }

    /// Authorize a broker RPC. On allow, returns the decision id (empty under
    /// the legacy path) so callers can stamp it into the backend context
    /// (`app.current_decision_id`) for row-level audit correlation. On deny,
    /// returns a `permission_denied`/`unauthenticated` status.
    pub(crate) async fn authorize(
        &self,
        security: &SecurityContext,
        message_type: &str,
        operation: &str,
    ) -> Result<String, Status> {
        self.ensure_ready()?;

        // Phase 4 - Catalog Version Compatibility
        if let Some(detail) = self
            .catalog
            .compatibility_error(&security.client_catalog_version, &security.project_id)
        {
            let warn_only = self
                .runtime_snapshot()
                .config()
                .service
                .catalog_compat_warn_only;

            let msg = format!(
                "incompatible catalog version: client is '{}', active is '{}': {}",
                security.client_catalog_version,
                self.catalog.active().metadata.version,
                detail
            );

            if warn_only {
                tracing::warn!(
                    trace_id = security.trace_id,
                    project_id = security.project_id,
                    "{}",
                    msg
                );
            } else {
                return Err(Status::failed_precondition(msg));
            }
        }

        let safe = security.log_safe();

        // GAP 40: Per-tenant fixed-window rate limiting (the key embeds
        // `unix_epoch / window_secs`, so each window is a discrete bucket — not
        // a true sliding window).
        if self.runtime_snapshot().config().service.rate_limit_enabled && !safe.tenant_id.is_empty()
        {
            self.check_rate_limit(&safe.tenant_id, operation).await?;
        }

        tracing::debug!(
            trace_id = security.trace_id,
            correlation_id = safe.correlation_id,
            tenant_id = safe.tenant_id,
            purpose = safe.purpose,
            service_identity = safe.service_identity,
            message_type = message_type,
            operation = operation,
            "authorizing UDB request"
        );
        // Milestone 7: when UDB_AUTHZ_V2 is enabled, route the same loaded ABAC
        // policies through the v2 decision engine (structured Decision +
        // decision_id), preserving the mandatory tenant/purpose checks so
        // behavior matches `evaluate_abac`. Default OFF → unchanged legacy path.
        // A per-instance override wins over the env flag (deterministic tests;
        // staged rollout seam for item 132).
        if self.abac_v2_override.unwrap_or_else(authz_v2_enabled) {
            if security.tenant_id.trim().is_empty() {
                return Err(Status::unauthenticated("tenant_id is required"));
            }
            if security.purpose.trim().is_empty() {
                return Err(Status::permission_denied("purpose is required"));
            }
            let principal = Principal::from_security_context(security, Vec::new());
            let resource = ResourceRef::message(message_type);
            let attributes = std::collections::BTreeMap::new();
            let snapshot = self.current_abac_snapshot();
            let decision = snapshot.authorize(&AuthzQuery {
                principal: &principal,
                resource: &resource,
                action: operation,
                purpose: &security.purpose,
                attributes: &attributes,
            });
            tracing::debug!(
                trace_id = security.trace_id,
                decision_id = decision.decision_id,
                allowed = decision.allowed,
                "authz v2 decision"
            );
            return if decision.allowed {
                Ok(decision.decision_id)
            } else {
                Err(Status::permission_denied(decision.deny_reason))
            };
        }

        // Legacy (v2-off) path: `evaluate_abac` is synchronous, so borrow the
        // policies under the read guard instead of cloning the whole Vec per
        // request. The default v2 path above already serves the cached
        // `current_abac_snapshot()` (built once per reload) — #83.
        let result = match self.abac_policies.read() {
            Ok(guard) => evaluate_abac(
                &guard,
                security,
                message_type,
                operation,
                self.abac_default_allow,
            ),
            Err(_) => evaluate_abac(
                &[],
                security,
                message_type,
                operation,
                self.abac_default_allow,
            ),
        };
        result.map(|()| Uuid::new_v4().to_string())
    }

    /// #112: per-item ABAC authorization usable from inside a `'static` batch
    /// stream. `authorize` itself borrows `&self` (catalog-compat + rate-limit
    /// prologue) and can't be moved into the streaming closure, but its ABAC
    /// decision core only needs cloneable inputs — the cached `Arc<AuthzSnapshot>`
    /// (v2) or the `Arc<RwLock<Vec<AbacPolicy>>>` (legacy) — which the batch
    /// handlers capture once and call this with per streamed item. Mirrors the
    /// v2/legacy branch in `authorize` and returns the per-item `decision_id`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn authorize_message_item(
        abac_v2: bool,
        snapshot: &AuthzSnapshot,
        policies: &RwLock<Vec<AbacPolicy>>,
        default_allow: bool,
        security: &SecurityContext,
        message_type: &str,
        operation: &str,
    ) -> Result<String, Status> {
        if abac_v2 {
            if security.tenant_id.trim().is_empty() {
                return Err(Status::unauthenticated("tenant_id is required"));
            }
            if security.purpose.trim().is_empty() {
                return Err(Status::permission_denied("purpose is required"));
            }
            let principal = Principal::from_security_context(security, Vec::new());
            let resource = ResourceRef::message(message_type);
            let attributes = std::collections::BTreeMap::new();
            let decision = snapshot.authorize(&AuthzQuery {
                principal: &principal,
                resource: &resource,
                action: operation,
                purpose: &security.purpose,
                attributes: &attributes,
            });
            if decision.allowed {
                Ok(decision.decision_id)
            } else {
                Err(Status::permission_denied(decision.deny_reason))
            }
        } else {
            let result = match policies.read() {
                Ok(guard) => {
                    evaluate_abac(&guard, security, message_type, operation, default_allow)
                }
                Err(_) => evaluate_abac(&[], security, message_type, operation, default_allow),
            };
            result.map(|()| Uuid::new_v4().to_string())
        }
    }

    pub(crate) fn require_portal_permission(
        &self,
        security: &SecurityContext,
        operation: &str,
        mutation: bool,
    ) -> Result<(), Status> {
        let allowed = if security.has_scope("udb:admin") {
            true
        } else if mutation {
            security.has_scope("udb:portal:operator") || security.has_scope("udb:portal:admin")
        } else {
            security.has_scope("udb:portal:viewer")
                || security.has_scope("udb:portal:operator")
                || security.has_scope("udb:portal:admin")
        };
        if allowed {
            Ok(())
        } else {
            Err(Status::permission_denied(format!(
                "scope udb:admin{} is required for {operation}",
                if mutation {
                    " or udb:portal:operator"
                } else {
                    " or udb:portal:viewer"
                }
            )))
        }
    }

    // GAP 40: Distributed rate limiter via Redis (no-op when the redis feature is off).
    #[cfg(not(feature = "redis"))]
    pub(crate) async fn check_rate_limit(
        &self,
        _tenant_id: &str,
        _operation: &str,
    ) -> Result<(), Status> {
        static WARN_ONCE: std::sync::Once = std::sync::Once::new();
        WARN_ONCE.call_once(|| {
            tracing::warn!("rate limiting disabled in this build because the redis feature is off");
        });
        Ok(())
    }

    #[cfg(feature = "redis")]
    pub(crate) async fn check_rate_limit(
        &self,
        tenant_id: &str,
        operation: &str,
    ) -> Result<(), Status> {
        let Some(redis) = self.runtime_snapshot().redis_clone() else {
            return Ok(());
        };
        let window_secs = self
            .runtime_snapshot()
            .config()
            .service
            .rate_limit_window_secs
            .max(1);
        let max_rps = self
            .runtime_snapshot()
            .config()
            .service
            .rate_limit_max_per_window;

        let unix_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let key = format!(
            "udb:ratelimit:{}:{}:{}",
            tenant_id,
            operation,
            unix_epoch / window_secs
        );

        let mut conn = {
            let mut guard = self.rate_limit_redis.lock().await;
            if guard.is_none() {
                *guard = Some(
                    redis
                        .get_multiplexed_async_connection()
                        .await
                        .map_err(|e| Status::internal(format!("rate limit redis error: {}", e)))?,
                );
            }
            guard
                .as_ref()
                .expect("rate limit redis connection just initialized")
                .clone()
        };

        // INCR + EXPIRE-on-first-hit must be atomic and fail-CLOSED:
        //  - A bare INCR followed by a separate EXPIRE can orphan a key without
        //    a TTL (crash/EXPIRE-error between the two), which then counts
        //    forever and permanently blocks the tenant.
        //  - `unwrap_or(0)` on errors fails OPEN (a Redis blip disables the
        //    limiter entirely). A rate limiter that silently stops limiting is
        //    worse than one that rejects on infra failure.
        // A single Lua eval does both atomically and propagates errors.
        const RATE_LIMIT_LUA: &str = "local c = redis.call('INCR', KEYS[1]) \
             if c == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end \
             return c";
        let count: u64 = redis::Script::new(RATE_LIMIT_LUA)
            .key(&key)
            .arg(window_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| Status::internal(format!("rate limit redis error: {e}")))?;

        if count > u64::from(max_rps) {
            return Err(Status::resource_exhausted(format!(
                "rate limit exceeded: {}/{} requests per {}s window",
                count, max_rps, window_secs
            )));
        }
        Ok(())
    }

    pub(crate) fn record_grpc<T>(
        &self,
        method: &'static str,
        started: Instant,
        result: Result<Response<T>, Status>,
    ) -> Result<Response<T>, Status> {
        let status = result
            .as_ref()
            .map(|_| "ok".to_string())
            .unwrap_or_else(|err| format!("{:?}", err.code()).to_ascii_lowercase());
        self.metrics
            .record_grpc(method, &status, started.elapsed().as_secs_f64());
        result
    }

    pub(crate) fn with_catalog_response_headers<T>(
        &self,
        mut response: Response<T>,
        context: &crate::RequestContext,
    ) -> Response<T> {
        let active = self.catalog.active_for(&context.project_id);
        let metadata = response.metadata_mut();
        insert_ascii_header(metadata, "x-udb-project-id", &active.metadata.project_id);
        insert_ascii_header(metadata, "x-udb-catalog-version", &active.metadata.version);
        insert_ascii_header(
            metadata,
            "x-udb-manifest-checksum",
            &active.metadata.checksum,
        );
        let mut consistency = crate::runtime::consistency::ConsistencyPolicy::from_request_context(
            &context.consistency,
            context.max_replica_lag_ms,
            context.primary_read,
            context.eventual_consistency_allowed,
        );
        let mut read_fence_invalid = false;
        if !context.read_fence_json.trim().is_empty() {
            match serde_json::from_str::<crate::runtime::consistency::ReadFence>(
                &context.read_fence_json,
            ) {
                Ok(fence) => {
                    consistency = consistency.with_fence(fence);
                }
                Err(_) => {
                    read_fence_invalid = true;
                }
            }
        }
        insert_ascii_header(
            metadata,
            "x-udb-consistency-mode",
            consistency.mode.as_str(),
        );
        insert_ascii_header(
            metadata,
            "x-udb-read-fence-present",
            if !consistency.fence.is_empty() {
                "true"
            } else {
                "false"
            },
        );
        insert_ascii_header(
            metadata,
            "x-udb-read-fence-honored",
            if !consistency.fence.is_empty() && consistency.mode.honours_fence() {
                "true"
            } else {
                "false"
            },
        );
        insert_ascii_header(
            metadata,
            "x-udb-read-fence-invalid",
            if read_fence_invalid { "true" } else { "false" },
        );
        insert_ascii_header(
            metadata,
            "x-udb-primary-read",
            if context.primary_read {
                "true"
            } else {
                "false"
            },
        );
        insert_ascii_header(
            metadata,
            "x-udb-eventual-consistency-allowed",
            if context.eventual_consistency_allowed {
                "true"
            } else {
                "false"
            },
        );
        response
    }

    pub(crate) async fn with_mutation_response_headers(
        &self,
        mut mutation: MutationResponse,
        context: &crate::RequestContext,
    ) -> Response<MutationResponse> {
        let active = self.catalog.active_for(&context.project_id);
        let receipt_json = if mutation.write_receipt_json.trim().is_empty() {
            let receipt = self
                .runtime_snapshot()
                .current_write_receipt(&active.metadata.checksum)
                .await;
            serde_json::to_string(&receipt).unwrap_or_default()
        } else {
            mutation.write_receipt_json.clone()
        };
        mutation.write_receipt_json = receipt_json.clone();
        let mut response = self.with_catalog_response_headers(Response::new(mutation), context);
        insert_ascii_header(
            response.metadata_mut(),
            "x-udb-write-receipt",
            &receipt_json,
        );
        response
    }

    pub(crate) async fn execute_with_channel<F, Fut, T>(
        &self,
        op: crate::runtime::channels::OperationChannel,
        f: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        self.execute_with_channel_scoped(op, None, None, f).await
    }

    pub(crate) async fn execute_with_channel_scoped<F, Fut, T>(
        &self,
        op: crate::runtime::channels::OperationChannel,
        context: Option<&crate::RequestContext>,
        backend: Option<&str>,
        f: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, Status>>,
    {
        let runtime = self.runtime_snapshot();
        let channels = runtime.channels();
        let project = context
            .map(|context| context.project_id.as_str())
            .unwrap_or("default");
        let tenant = context
            .map(|context| context.tenant_id.as_str())
            .unwrap_or("anonymous");
        let tenant_hash = tenant_hash_label(tenant);
        let backend_label = backend
            .or_else(|| context.and_then(|context| non_empty(&context.target_backend)))
            .unwrap_or("default");
        let instance_label = context
            .and_then(|context| non_empty(&context.target_instance))
            .unwrap_or("default");
        let cost = op.default_cost();
        let _permit = match channels
            .acquire_fair_with_backpressure(
                op,
                context.map(|context| context.tenant_id.as_str()),
                context.map(|context| context.project_id.as_str()),
                Some(backend_label),
                context.map(|context| context.target_instance.as_str()),
                cost,
            )
            .await
        {
            Ok(permit) => {
                self.metrics.record_fair_admission(
                    project,
                    &tenant_hash,
                    backend_label,
                    instance_label,
                    op.as_str(),
                    "accepted",
                );
                self.metrics.add_fair_cost(
                    project,
                    &tenant_hash,
                    backend_label,
                    instance_label,
                    op.as_str(),
                    f64::from(cost),
                );
                permit
            }
            Err(e) => {
                self.metrics.inc_channel_rejected(op.as_str());
                self.metrics.record_fair_admission(
                    project,
                    &tenant_hash,
                    backend_label,
                    instance_label,
                    op.as_str(),
                    "rejected",
                );
                return Err(e);
            }
        };

        self.metrics.inc_channel_inflight(op.as_str());
        let start = Instant::now();

        let timeout_secs = channels.deadline_secs(op, backend);
        let res = tokio::time::timeout(Duration::from_secs(timeout_secs), f()).await;

        self.metrics.dec_channel_inflight(op.as_str());
        self.metrics
            .observe_channel_latency(op.as_str(), start.elapsed().as_secs_f64());

        match res {
            Ok(Ok(val)) => Ok(val),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                self.metrics.inc_channel_timeout(op.as_str());
                Err(Status::deadline_exceeded(format!(
                    "{} channel timeout",
                    op.as_str()
                )))
            }
        }
    }
}

fn service_metrics_recorder() -> Arc<dyn MetricsRecorder> {
    match PrometheusMetrics::new() {
        Ok(metrics) => Arc::new(metrics),
        Err(err) => {
            tracing::warn!("prometheus metrics disabled: {err}");
            Arc::new(NoopMetrics)
        }
    }
}

async fn admit_stream_batch_item(
    channels: &crate::runtime::channels::ChannelManager,
    metrics: &Arc<dyn MetricsRecorder>,
    context: &crate::RequestContext,
    op: crate::runtime::channels::OperationChannel,
    backend: &'static str,
) -> Result<crate::runtime::channels::ChannelPermit, Status> {
    let project = non_empty(&context.project_id).unwrap_or("default");
    let tenant_hash = tenant_hash_label(&context.tenant_id);
    let instance = non_empty(&context.target_instance).unwrap_or("default");
    match channels
        .acquire_fair_with_backpressure(
            op,
            Some(&context.tenant_id),
            Some(&context.project_id),
            Some(backend),
            Some(&context.target_instance),
            op.default_cost(),
        )
        .await
    {
        Ok(permit) => {
            metrics.record_fair_admission(
                project,
                &tenant_hash,
                backend,
                instance,
                op.as_str(),
                "accepted",
            );
            metrics.add_fair_cost(
                project,
                &tenant_hash,
                backend,
                instance,
                op.as_str(),
                f64::from(op.default_cost()),
            );
            Ok(permit)
        }
        Err(err) => {
            metrics.inc_channel_rejected(op.as_str());
            metrics.record_fair_admission(
                project,
                &tenant_hash,
                backend,
                instance,
                op.as_str(),
                "rejected",
            );
            Err(err)
        }
    }
}

fn check_backend_capability(
    backend: &str,
    operation: &str,
    capability_fn: impl Fn(&crate::planning::backend::BackendCapability) -> bool,
) -> Result<(), Status> {
    use crate::planning::backend::BackendKind;
    let backend_base = backend_selector_base(backend);
    let Some(kind) = BackendKind::from_store_kind("", backend_base) else {
        return Err(Status::invalid_argument(format!(
            "unknown backend '{backend}'"
        )));
    };
    let state = crate::backend::support_state_for_kind(&kind);
    if !state.is_runtime_supported() {
        return Err(Status::failed_precondition(state.diagnostic(kind.as_str())));
    }
    let cap = kind.capabilities();
    if capability_fn(&cap) {
        Ok(())
    } else {
        Err(crate::runtime::executor_utils::capability_status(
            backend,
            operation,
            crate::backend::UNSUPPORTED_OPERATION_CODE,
            format!(
                "{}: backend '{backend}' does not support operation '{operation}'",
                crate::backend::UNSUPPORTED_OPERATION_CODE
            ),
        ))
    }
}

fn check_generic_dispatch_operation(backend: &str, operation: &str) -> Result<(), Status> {
    use crate::planning::backend::BackendKind;
    let backend_base = backend_selector_base(backend);
    let Some(kind) = BackendKind::from_store_kind("", backend_base) else {
        return Err(Status::invalid_argument(format!(
            "unknown backend '{backend}'"
        )));
    };
    let state = crate::backend::support_state_for_kind(&kind);
    if !state.is_runtime_supported() {
        return Err(Status::failed_precondition(state.diagnostic(kind.as_str())));
    }
    let supported = match operation {
        "ping" | "probe" | "ensure_resource" | "drop_resource" | "list_resources" | "query"
        | "mutate" | "transaction" | "search" | "get_object" | "put_object" => {
            kind.supports_operation(operation)
        }
        other => {
            return Err(Status::invalid_argument(format!(
                "unknown operation '{other}'; allowed: ping, probe, ensure_resource, drop_resource, list_resources, query, mutate, transaction, search, get_object, put_object"
            )));
        }
    };
    if supported {
        Ok(())
    } else {
        Err(crate::runtime::executor_utils::capability_status(
            backend,
            operation,
            crate::backend::UNSUPPORTED_OPERATION_CODE,
            format!(
                "{}: backend '{backend}' does not support operation '{operation}'",
                crate::backend::UNSUPPORTED_OPERATION_CODE
            ),
        ))
    }
}

fn backend_selector_base(selector: &str) -> &str {
    selector
        .split_once(':')
        .map(|(backend, _)| backend)
        .or_else(|| selector.split_once('.').map(|(backend, _)| backend))
        .unwrap_or(selector)
}

fn bounded_list_limit(limit: i32) -> i32 {
    if limit <= 0 { 100 } else { limit.min(1000) }
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

/// Require that the caller carries the `udb:admin` scope (or a matching
/// wildcard) for admin-only RPCs. Delegates to [`SecurityContext::has_scope`],
/// which honors the exact `udb:admin` scope as well as the `udb:*` and `*`
/// wildcards. On failure returns a `permission_denied` `Status`; call sites map
/// it through their own `record_grpc(...)` so per-method metrics stay accurate.
fn require_admin_scope(security: &SecurityContext) -> Result<(), Status> {
    if security.has_scope("udb:admin") {
        Ok(())
    } else {
        Err(Status::permission_denied("scope udb:admin is required"))
    }
}

fn rls_bypass_ack(spec_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(spec_json)
        .ok()
        .and_then(|value| {
            value
                .get("udb_allow_rls_bypass")
                .or_else(|| value.get("allow_rls_bypass"))
                .and_then(|flag| flag.as_bool())
        })
        .unwrap_or(false)
}

fn contains_rls_bypass_sql(spec_json: &str) -> bool {
    let lower = spec_json.to_ascii_lowercase();
    lower.contains("truncate ")
        || lower.contains(" truncate")
        || lower.contains(" cascade")
        || lower.contains("disable row level security")
        || lower.contains("alter table")
        || lower.contains("drop table")
        || lower.contains("create unique index")
        || lower.contains(" unique ")
        || lower.contains(" primary key")
}

fn guard_rls_bypass_operation(operation: &str, spec_json: &str) -> Result<(), Status> {
    let bypass_like = matches!(operation, "drop_resource")
        || (matches!(operation, "query" | "mutate" | "transaction")
            && contains_rls_bypass_sql(spec_json));
    if bypass_like && !rls_bypass_ack(spec_json) {
        return Err(Status::failed_precondition(
            "operation may bypass tenant isolation/RLS; set spec_json.udb_allow_rls_bypass=true after explicit tenant-scope review",
        ));
    }
    Ok(())
}

#[derive(Clone)]
struct WebrtcPeerTokenAuth {
    security: SecurityConfig,
}

impl WebrtcPeerTokenAuth {
    fn new() -> Self {
        Self {
            security: SecurityConfig::current(),
        }
    }
}

impl tonic::service::Interceptor for WebrtcPeerTokenAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let metadata = request.metadata();
        let auth_header = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
            Status::unauthenticated(
                "missing or invalid authorization header (WebRTC peer bearer required)",
            )
        })?;
        let claims =
            validate_bearer_token(&self.security, token).map_err(Status::unauthenticated)?;
        let scopes = claims.resolved_scopes();
        let allowed = scopes.iter().any(|scope| {
            matches!(
                scope.as_str(),
                "*" | "udb:*" | "udb:webrtc:*" | "udb:webrtc:peer" | "udb:webrtc:signal"
            )
        });
        if !allowed {
            return Err(Status::permission_denied(
                "scope udb:webrtc:peer or udb:webrtc:signal is required",
            ));
        }
        if let Some(header_tenant) = metadata
            .get("x-tenant-id")
            .and_then(|value| value.to_str().ok())
            && !header_tenant.trim().is_empty()
            && claims.tenant_id.as_deref().unwrap_or_default() != header_tenant
        {
            return Err(Status::permission_denied(
                "x-tenant-id must match the peer token tenant",
            ));
        }
        Ok(request)
    }
}

fn tenant_hash_label(tenant: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(tenant.as_bytes());
    let digest = hasher.finalize();
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn page_offset(page_token: &str) -> i32 {
    page_token.parse::<i32>().unwrap_or_default().max(0)
}

fn next_page_token(offset: i32, limit: i32, returned: i32) -> String {
    if returned >= limit {
        (offset + returned).to_string()
    } else {
        String::new()
    }
}

pub fn context_from_metadata(metadata: &tonic::metadata::MetadataMap) -> crate::RequestContext {
    let header = |name: &str| {
        metadata
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string()
    };
    crate::RequestContext {
        tenant_id: header("x-tenant-id"),
        purpose: header("x-purpose"),
        correlation_id: header("x-correlation-id"),
        user_id: header("x-user-id"),
        project_id: header("x-udb-project-id"),
        scopes: header("x-scopes")
            .split(',')
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(ToString::to_string)
            .collect(),
        consistency: header("x-udb-consistency"),
        max_replica_lag_ms: header("x-udb-max-replica-lag-ms")
            .parse::<u64>()
            .unwrap_or_default(),
        client_catalog_version: header("x-udb-client-catalog-version"),
        target_backend: header("x-udb-target-backend"),
        target_instance: header("x-udb-target-instance"),
        routing_policy: header("x-udb-routing-policy"),
        primary_read: matches!(
            header("x-udb-primary-read").to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        eventual_consistency_allowed: matches!(
            header("x-udb-eventual-consistency-allowed")
                .to_ascii_lowercase()
                .as_str(),
            "1" | "true" | "yes" | "on"
        ) || matches!(
            header("x-udb-consistency")
                .to_ascii_lowercase()
                .replace('-', "_")
                .as_str(),
            "eventual" | "eventual_consistency"
        ),
        read_fence_json: header("x-udb-read-fence"),
        service_identity: header("x-service-identity"),
        decision_id: String::new(),
    }
}

fn backend_instance_status(
    instance: &crate::runtime::core::RuntimeBackendInstance,
) -> BackendInstanceStatus {
    BackendInstanceStatus {
        backend: instance.backend.clone(),
        instance_name: instance.name.clone(),
        role: instance.role.clone(),
        enabled: instance.enabled,
        configured: instance.configured,
        connected: instance.connected,
        read_weight: instance.read_weight,
        write_weight: instance.write_weight,
        labels: instance.labels.clone(),
        capabilities: instance.capabilities.clone(),
        routing_status: if !instance.enabled {
            "disabled".to_string()
        } else if instance.circuit_open {
            "circuit_open".to_string()
        } else if instance.connected {
            "available".to_string()
        } else if instance.configured {
            "degraded".to_string()
        } else {
            "unconfigured".to_string()
        },
        healthy: instance.healthy,
        circuit_open: instance.circuit_open,
    }
}

fn parse_catalog_manifest_payload(bytes: &[u8]) -> Result<CatalogManifest, Status> {
    if bytes.is_empty() {
        return Err(Status::invalid_argument("manifest_json is required"));
    }
    serde_json::from_slice::<CatalogManifest>(bytes).map_err(|err| {
        Status::invalid_argument(format!("manifest_json is not a CatalogManifest: {err}"))
    })
}

fn catalog_payload_version(bytes: &[u8], manifest: &CatalogManifest) -> String {
    if !bytes.is_empty()
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes)
        && let Some(version) = value.get("version").and_then(|v| v.as_str())
        && !version.trim().is_empty()
    {
        return version.trim().to_string();
    }
    if !manifest.generator_version.trim().is_empty() {
        return format!("generator-{}", manifest.generator_version.trim());
    }
    if !manifest.checksum_sha256.trim().is_empty() {
        return manifest.checksum_sha256.chars().take(12).collect();
    }
    "unversioned".to_string()
}

pub async fn serve(
    manifest: CatalogManifest,
    schemas: Vec<ProtoSchema>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = DataBrokerRuntime::try_from_env().await.map_err(|err| {
        std::io::Error::other(format!("UDB startup config validation failed: {err}"))
    })?;
    let runtime_config = runtime.config().clone();
    native_registry::install_native_service_runtime_config(&runtime_config);
    // Secure-transport gate (always fatal when the operator has explicitly
    // enabled UDB_REQUIRE_SECURE_TRANSPORT / UDB_MTLS_REQUIRED but left certs
    // unconfigured).
    let transport_violation = validate_secure_transport(&runtime_config.service).err();
    if let Some(err) = &transport_violation {
        // Explicit secure-transport request without certs is a hard error
        // regardless of posture (matches prior behavior).
        if runtime_config.service.require_secure_transport || runtime_config.service.mtls_required {
            return Err(std::io::Error::other(format!(
                "secure transport startup gate failed: {err}"
            ))
            .into());
        }
    }
    // Phase 5 fail-closed posture gate: in production / fail-closed mode a
    // non-empty production-validation or secure-transport violation list ABORTS
    // startup. In a dev posture (not production, not fail-closed) these are
    // advisory only, so a plaintext local deployment is still permitted.
    {
        let transport = transport_violation.into_iter().collect::<Vec<_>>();
        let violations = crate::runtime::security::hardened_startup_violations(&transport);
        if violations.is_empty() {
            // Dev posture (or clean prod): if there were advisory findings, surface
            // them without aborting.
            if !transport.is_empty()
                || crate::runtime::security::SecurityConfig::current()
                    .validate_production()
                    .is_err()
            {
                tracing::warn!(
                    "security posture advisory (not enforced in dev mode): set UDB_ENV=production \
                     or UDB_FAIL_CLOSED to make these fatal"
                );
            }
        } else {
            return Err(std::io::Error::other(format!(
                "production/secure-transport startup gate failed (enterprise mode refuses \
                 insecure transport): {}",
                violations.join("; ")
            ))
            .into());
        }
    }
    // Phase 5 / urgent_fix #3: compliance-profile startup gate. When an operator
    // selects a profile (`UDB_COMPLIANCE_PROFILE=soc2|iso27001|pci_hipaa`), validate
    // it against actual deployment facts and REFUSE to serve on violation — making
    // the profile an enforced runtime posture, not a documentation claim. Previously
    // `validate_compliance_profile` was only exercised by tests.
    {
        let raw_profile = std::env::var("UDB_COMPLIANCE_PROFILE").unwrap_or_default();
        match crate::runtime::security::selected_compliance_profile() {
            Some(profile) => {
                let cfg = crate::runtime::security::SecurityConfig::current();
                let facts = cfg.compliance_profile_facts();
                if let Err(violations) = cfg.validate_compliance_profile(profile, &facts) {
                    return Err(std::io::Error::other(format!(
                        "compliance profile '{}' startup gate failed: {}",
                        profile.as_str(),
                        violations.join("; ")
                    ))
                    .into());
                }
                tracing::info!(profile = profile.as_str(), "compliance profile gate passed");
            }
            None if !raw_profile.trim().is_empty()
                && !raw_profile.trim().eq_ignore_ascii_case("none") =>
            {
                return Err(std::io::Error::other(format!(
                    "unknown UDB_COMPLIANCE_PROFILE '{}' (expected soc2 | iso27001 | pci_hipaa)",
                    raw_profile.trim()
                ))
                .into());
            }
            None => {}
        }
    }
    // urgent_fix #30: backup / replication tenant-scope startup gate. UDB does not
    // perform bulk data movement in-process (the external DBs / operator do — §0
    // doctrine: "databases own data replication"); UDB owns the tenant-scope
    // CONTRACT for it. When a backup DB is configured, REACH the fail-closed
    // `tenant_movement` guard at startup so the contract is enforced runtime
    // behavior, not a test-only validator: a backup copies the whole broker store
    // across tenants, so an enterprise deployment must EITHER scope it to one
    // tenant (`UDB_BACKUP_TENANT_ID`) OR explicitly acknowledge the privileged
    // cross-tenant copy (`UDB_ALLOW_CROSS_TENANT_BACKUP=true`), else we refuse to
    // serve.
    if runtime_config.has_backup() {
        let backup_tenant = std::env::var("UDB_BACKUP_TENANT_ID").unwrap_or_default();
        let backup_tenant = backup_tenant.trim();
        let privileged = std::env::var("UDB_ALLOW_CROSS_TENANT_BACKUP")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let movement = crate::runtime::tenant_movement::TenantMovementRequest {
            operation: crate::runtime::tenant_movement::TenantMovementOperation::BackupExport,
            tenant_id: backup_tenant,
            target_tenant_id: None,
            tenant_filter_present: !backup_tenant.is_empty(),
            privileged_cross_tenant: privileged,
        };
        match crate::runtime::tenant_movement::validate_tenant_movement_scope(&movement) {
            Ok(()) => tracing::info!(
                tenant_scoped = !backup_tenant.is_empty(),
                privileged_cross_tenant = privileged,
                "backup tenant-scope gate passed"
            ),
            Err(violation) if crate::runtime::security::fail_closed_mode() => {
                return Err(std::io::Error::other(format!(
                    "backup tenant-scope startup gate failed: {violation}. Set \
                     UDB_BACKUP_TENANT_ID=<tenant> for a tenant-scoped backup, or \
                     UDB_ALLOW_CROSS_TENANT_BACKUP=true to acknowledge a privileged \
                     broker-wide backup."
                ))
                .into());
            }
            Err(violation) => tracing::warn!(
                violation = %violation,
                "backup tenant-scope advisory (not enforced in dev mode): set \
                 UDB_FAIL_CLOSED or UDB_ENV=production to make this fatal"
            ),
        }
    }
    // urgent_fix #34: resolve the descriptor-derived method-security registry
    // EAGERLY at startup so a corrupt/empty embedded descriptor fails fast here
    // (fail-closed) rather than lazily on the first RPC after we are already
    // serving. `method_security_registry()` aborts on a zero-service / undecodable
    // manifest via `descriptor_contract_manifest()`.
    let _ = crate::runtime::service::method_security::method_security_registry();

    if !runtime.postgres_configured() {
        // Distinguish "no PostgreSQL config supplied at all" from "config was
        // supplied (URL or libpq-style PGHOST/… components) but the server was
        // unreachable". `postgres_configured()` only flips true once the pool
        // actually connects, so a misleading "UDB_PG_DSN is required" used to be
        // emitted even when a perfectly valid DSN had been resolved.
        let primary = &runtime.config().primary;
        match crate::runtime::core::postgres_dsn_from_config(primary) {
            Some(resolved_dsn) => {
                return Err(format!(
                    "PostgreSQL startup health gate failed: a connection string was resolved \
                     ({}) but the database could not be reached. Verify the host/port, \
                     credentials and TLS settings.",
                    crate::generation::dsn::redact_dsn(&resolved_dsn)
                )
                .into());
            }
            None => {
                return Err(
                    "PostgreSQL startup health gate failed: no PostgreSQL configuration found. \
                     Provide a connection URL via UDB_PG_DSN / DATABASE_URL, or libpq-style \
                     component variables (PGHOST + PGDATABASE [+ PGUSER/PGPASSWORD/PGPORT/\
                     PGSSLMODE])."
                        .into(),
                );
            }
        }
    }
    // Fail fast on a malformed operator-supplied Casbin model (UDB_AUTHZ_CASBIN_MODEL[_PATH])
    // rather than denying every authorization at runtime.
    crate::runtime::authz::validate_casbin_model()
        .await
        .map_err(|err| std::io::Error::other(format!("authz startup gate failed: {err}")))?;
    if !runtime.qdrant_configured() {
        tracing::warn!("Qdrant startup health gate degraded: vector RPCs will return UNAVAILABLE");
    }
    #[cfg(feature = "s3")]
    if !runtime.s3_configured() {
        tracing::warn!(
            "S3/MinIO startup health gate degraded: object RPCs will return UNAVAILABLE"
        );
    }
    let system_report = runtime.ensure_system_catalog().await?;
    tracing::info!(
        schema = system_report.schema,
        statements_applied = system_report.statements_applied,
        "UDB internal system catalog is ready"
    );
    let prometheus_metrics = match PrometheusMetrics::new() {
        Ok(metrics) => Some(Arc::new(metrics)),
        Err(err) => {
            tracing::warn!("prometheus metrics disabled: {err}");
            None
        }
    };
    let metrics: Arc<dyn MetricsRecorder> = prometheus_metrics
        .as_ref()
        .map(|metrics| metrics.clone() as Arc<dyn MetricsRecorder>)
        .unwrap_or_else(|| Arc::new(NoopMetrics));
    let metrics_socket: SocketAddr = runtime_config.service.metrics_addr.parse()?;
    if let Some(prometheus_metrics) = prometheus_metrics.clone() {
        tokio::spawn(metrics_http_server(
            prometheus_metrics,
            runtime.clone(),
            metrics_socket,
            runtime_config.service.metrics_allowed_cidr.clone(),
        ));
    }
    tokio::spawn(cdc_metrics_poller(runtime.clone(), metrics.clone()));

    let lifecycle_state = Arc::new(RwLock::new(FsmState::Initialising));
    // Item 8: time the startup migration run and emit the (now wired) migration
    // metrics — run count by terminal status + run duration.
    let lifecycle_started = Instant::now();
    let startup_force_sync = startup_bool_env("UDB_STARTUP_FORCE_SYNC");
    let startup_dry_run = startup_bool_env("UDB_STARTUP_DRY_RUN");
    tracing::info!(
        startup_force_sync,
        startup_dry_run,
        "running UDB startup lifecycle"
    );
    match run_startup_lifecycle(
        &runtime,
        &manifest,
        &schemas,
        startup_force_sync,
        startup_dry_run,
    )
    .await
    {
        Ok(report) => {
            let elapsed = lifecycle_started.elapsed().as_secs_f64();
            metrics.inc_runs_total("completed");
            metrics.observe_run_duration("completed", elapsed);
            metrics.set_pending_files(report.pending_migration_files);
            for op in &report.migration_metric_operations {
                metrics.inc_operations_total(&op.kind, &op.schema, &op.safety);
                if op.safety == "blocked" || op.safety == "requires_review" {
                    metrics.set_blocked_operations(&op.schema, &op.kind, 1);
                }
            }
            // Surface lint warnings the run accumulated (kind label is the
            // recorder's coarse "startup" bucket — detailed kinds are emitted by
            // the lint pass itself once instrumented).
            for _ in &report.warnings {
                metrics.inc_lint_warnings("startup");
            }
            tracing::info!(
                run_id = report.run_id,
                applied_sql_artifacts = report.applied_sql_artifacts,
                verified_tables = report.verified_tables,
                "UDB startup lifecycle completed"
            );
            if let Ok(mut state) = lifecycle_state.write() {
                *state = FsmState::Completed;
            }
            // Block 1 (auth_fix.md, change-point 1): seed the system authz
            // defaults the bootstrap admin path depends on — the global
            // `organization_owner` role + the `org_owner ⇒ allow(*, *)` policy.
            // Post-DDL so the tables exist, idempotent, every startup; skipped on
            // dry-run (it must not write). Non-fatal.
            if !startup_dry_run {
                if let Ok(pool) = runtime.pg_pool() {
                    if let Err(err) =
                        auth_service::seed_system_authz_defaults(pool).await
                    {
                        tracing::warn!(error = %err, "seed system authz defaults failed (non-fatal)");
                    }
                }
            }
        }
        Err(err) => {
            metrics.inc_runs_total("error");
            metrics.observe_run_duration("error", lifecycle_started.elapsed().as_secs_f64());
            if let Ok(mut state) = lifecycle_state.write() {
                *state = FsmState::Error;
            }
            return Err(err.into());
        }
    }
    runtime.mark_indeterminate_sagas().await;
    // Items 3/5/23/24: XA recovery — drive in-doubt ledger rows terminal and
    // run the ledger-aware presumed-abort sweep over aged `udb-%` prepared
    // transactions. Runs immediately at startup and then on every
    // `RecoveryConfig.interval` tick, lease-gated so exactly one node sweeps
    // at a time. Configured MySQL instances are registered as in-doubt
    // participants so MySQL `XA RECOVER` xids are driven terminal too. Rows
    // that keep failing past `RecoveryConfig.max_attempts` are parked as
    // `manual_review`. Grace window via UDB_XA_RECOVERY_GRACE_SECS (default
    // 300s) leaves prepares from in-flight requests untouched.
    if let Some(pg_pool) = runtime.pg_pool_clone() {
        let sys_config = crate::runtime::system::SystemCatalogConfig::current();
        let singleton_relation = runtime_config.cdc.lock_log_relation();
        let recovery_config = crate::runtime::xa_recovery::RecoveryConfig::default();
        let grace = std::env::var("UDB_XA_RECOVERY_GRACE_SECS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(300);
        #[allow(unused_mut)]
        let mut registry = crate::runtime::xa_recovery::default_indoubt_registry(&pg_pool);
        #[cfg(feature = "mysql")]
        {
            let mut instance_names: Vec<&String> = runtime.mysql_instances.keys().collect();
            instance_names.sort();
            for name in instance_names {
                if let Some(mysql_pool) = runtime.mysql_instances.get(name) {
                    registry.register(std::sync::Arc::new(
                        crate::runtime::xa_recovery::MysqlInDoubtParticipant {
                            label: format!("mysql:{name}"),
                            pool: mysql_pool.clone(),
                        },
                    ));
                }
            }
            // Bare-backend fallback (mirrors the Postgres registration) so
            // ledger rows labelled plain "mysql" still resolve to the primary.
            if let Some(mysql_pool) = runtime.mysql_pool_for_instance("primary") {
                registry.register(std::sync::Arc::new(
                    crate::runtime::xa_recovery::MysqlInDoubtParticipant {
                        label: "mysql".to_string(),
                        pool: mysql_pool.clone(),
                    },
                ));
            }
        }
        let xa_recovery_lease_ttl = std::cmp::max(
            recovery_config.interval,
            crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
        );
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(recovery_config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                // The first tick completes immediately, preserving the
                // startup-recovery semantics of the old one-shot call.
                interval.tick().await;
                match crate::runtime::singleton::run_once(
                    &pg_pool,
                    &singleton_relation,
                    crate::runtime::singleton::WORKER_XA_RECOVERY,
                    xa_recovery_lease_ttl,
                    || async {
                        crate::runtime::xa_recovery::run_xa_recovery_pass(
                            &pg_pool,
                            &sys_config,
                            &registry,
                            &recovery_config,
                            grace,
                        )
                        .await
                    },
                )
                .await
                {
                    Ok(Some(Ok((ledger, abandoned)))) => {
                        if ledger > 0 {
                            tracing::warn!(
                                "XA recovery: drove {ledger} ledger in-doubt transaction(s) terminal"
                            );
                        }
                        if abandoned > 0 {
                            tracing::warn!(
                                "XA recovery: drove {abandoned} aged prepared transaction(s) terminal"
                            );
                        }
                    }
                    Ok(Some(Err(e))) => tracing::warn!("XA recovery sweep failed: {e}"),
                    Ok(None) => {
                        tracing::debug!("XA recovery skipped: singleton lease held by peer")
                    }
                    Err(e) => tracing::warn!("XA recovery sweep failed: {e}"),
                }
            }
        });
        tracing::info!("XA recovery worker started (periodic, lease-gated)");
    }
    if crate::runtime::saga::SagaRecoveryWorker::is_enabled_with_settings(&runtime_config.saga) {
        // NW1-3c: route through the SystemStores registry instead of
        // the bare PG pool. Slim deployments without a canonical
        // store skip the worker entirely.
        if let Some(store) = runtime.default_system_stores() {
            let worker = crate::runtime::saga::SagaRecoveryWorker::with_settings(
                store,
                &runtime_config.saga,
            )
            .with_compensators(runtime.saga_compensator_registry())
            .with_metrics(metrics.clone());
            tokio::spawn(async move { worker.run_forever().await });
            tracing::info!("saga recovery worker started");
        } else {
            tracing::warn!("saga recovery worker disabled: no canonical store is registered");
        }
    }
    let abac_policies = Arc::new(RwLock::new(runtime.load_abac_policies().await));

    let scheduled_views = runtime.start_materialized_view_refresh(&manifest);
    if scheduled_views > 0 {
        tracing::info!(
            scheduled_views = scheduled_views,
            "scheduled materialized view auto-refresh tasks"
        );
    }
    let abac_default_allow = runtime_config.service.abac_default_allow;
    #[cfg(feature = "kafka")]
    let cdc_engine = start_cdc_engine(&runtime, metrics.clone()).await;
    #[cfg(not(feature = "kafka"))]
    let cdc_engine: Option<Arc<CdcEngine>> = {
        let _ = &metrics;
        None
    };
    let mut service = DataBrokerService::with_runtime_and_state(
        manifest,
        runtime,
        lifecycle_state,
        abac_policies,
        metrics.clone(),
        cdc_engine,
        abac_default_allow,
    );
    spawn_config_reload_watcher(service.clone());

    // GAP 36 / F83: refresh the legacy policy vector and the v2 authz snapshot
    // together so authorize() never rebuilds the ABAC snapshot per request.
    {
        let abac_refresh_secs = runtime_config.service.abac_refresh_secs;
        let runtime_bg = service.runtime_snapshot();
        let service_bg = service.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(abac_refresh_secs));
            loop {
                interval.tick().await;
                let fresh = runtime_bg.load_abac_policies().await;
                if fresh.is_empty() {
                    tracing::warn!(
                        "ABAC policy refresh returned empty set - retaining stale policies \
                         to avoid accidental deny-all"
                    );
                    continue;
                }
                service_bg.replace_abac_policies(fresh);
            }
        });
    }

    // ── U3 + NW1-3b: Projection materialization engine ────────────────────
    // The engine needs a `SystemStores` trait object for the projection
    // task ledger AND a PG pool for canonical source replay. If
    // either is missing we disable the engine.
    if let (Some(pg_pool), Some(store)) = (
        service.runtime_snapshot().pg_pool_clone(),
        service.runtime_snapshot().default_system_stores(),
    ) {
        use crate::runtime::projection::{
            ProjectionEngine, ProjectionWorker, ReconciliationWorker,
        };
        let config = crate::runtime::system::SystemCatalogConfig::current();
        let engine = Arc::new(ProjectionEngine::new(pg_pool.clone(), config));
        service.projection_engine = Some(Arc::clone(&engine));

        let singleton_relation = service.runtime_snapshot().config().cdc.lock_log_relation();
        if ProjectionWorker::is_enabled() {
            let metrics: Arc<dyn MetricsRecorder> = service.metrics.clone();
            let singleton_pool = pg_pool.clone();
            let singleton_relation = singleton_relation.clone();
            let runtime = service.runtime_snapshot().clone();
            let store = store.clone();
            tokio::spawn(async move {
                loop {
                    let metrics = metrics.clone();
                    let runtime = runtime.clone();
                    let store = store.clone();
                    match crate::runtime::singleton::run_while_leader(
                        &singleton_pool,
                        &singleton_relation,
                        crate::runtime::singleton::WORKER_PROJECTION_MATERIALIZER,
                        crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                        || async move {
                            ProjectionWorker::new(store, runtime, metrics)
                                .run_forever()
                                .await;
                            Ok::<(), String>(())
                        },
                    )
                    .await
                    {
                        Ok(Some(Ok(()))) => {}
                        Ok(Some(Err(err))) => {
                            tracing::warn!("projection materialization worker exited: {err}")
                        }
                        Ok(None) => tracing::debug!(
                            "projection materialization worker idle: singleton lease held by peer"
                        ),
                        Err(err) => tracing::warn!(
                            "projection materialization worker singleton lease failed: {err}"
                        ),
                    }
                    tokio::time::sleep(crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP)
                        .await;
                }
            });
            tracing::info!("projection materialization worker started");
        }
        if ReconciliationWorker::is_enabled() {
            let metrics: Arc<dyn MetricsRecorder> = service.metrics.clone();
            let active_catalog = service.catalog.active();
            let manifest = active_catalog.manifest.clone();
            let project_id = active_catalog.metadata.project_id.clone();
            let singleton_pool = pg_pool.clone();
            let singleton_relation = singleton_relation.clone();
            let worker_pool = pg_pool.clone();
            let store = store.clone();
            tokio::spawn(async move {
                loop {
                    let metrics = metrics.clone();
                    let manifest = manifest.clone();
                    let project_id = project_id.clone();
                    let store = store.clone();
                    let worker_pool = worker_pool.clone();
                    match crate::runtime::singleton::run_while_leader(
                        &singleton_pool,
                        &singleton_relation,
                        crate::runtime::singleton::WORKER_PROJECTION_RECONCILIATION,
                        crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                        || async move {
                            ReconciliationWorker::new(
                                worker_pool,
                                store,
                                metrics,
                                manifest,
                                project_id,
                            )
                            .run_forever()
                            .await;
                            Ok::<(), String>(())
                        },
                    )
                    .await
                    {
                        Ok(Some(Ok(()))) => {}
                        Ok(Some(Err(err))) => {
                            tracing::warn!("projection reconciliation worker exited: {err}")
                        }
                        Ok(None) => tracing::debug!(
                            "projection reconciliation worker idle: singleton lease held by peer"
                        ),
                        Err(err) => tracing::warn!(
                            "projection reconciliation worker singleton lease failed: {err}"
                        ),
                    }
                    tokio::time::sleep(crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP)
                        .await;
                }
            });
            tracing::info!("projection reconciliation worker started");
        }
    } else {
        tracing::warn!(
            "projection engine disabled: PostgreSQL pool and/or canonical store not available"
        );
    }
    let health_runtime = service.runtime_snapshot();
    let health_service = handlers_meta::build_listener_health_service(
        handlers_meta::HealthPlane::DataBroker,
        &runtime_config,
        Some(health_runtime.as_ref()),
    )
    .await;

    // ── Startup summary log ───────────────────────────────────────────────────
    {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        for t in &service.catalog.active().manifest.tables {
            hasher.update(t.message_name.as_bytes());
        }
        let checksum = format!("{:x}", hasher.finalize());
        let mut enabled_backends = service.runtime_snapshot().enabled_backend_names();
        enabled_backends.sort();
        enabled_backends.dedup();
        tracing::info!(
            addr = %addr,
            schema_checksum = %checksum,
            table_count = service.catalog.active().manifest.tables.len(),
            store_count = service.catalog.active().manifest.stores.len(),
            enabled_backends = ?enabled_backends,
            protocol_version = UDB_PROTOCOL_VERSION,
            cdc_enabled = service.cdc_engine.is_some(),
            supported_rpcs = SUPPORTED_RPC_NAMES.len(),
            "UDB DataBroker is ready"
        );
    }

    // ── gRPC server with timeout + concurrency limit (GAP 22) ────────────────
    let grpc_timeout = Duration::from_secs(runtime_config.service.grpc_timeout_secs);
    let grpc_max_concurrent: usize = runtime_config.service.grpc_max_concurrent;

    let make_layer = || {
        tower::ServiceBuilder::new()
            // Phase 10: outermost layer extracts the inbound W3C `traceparent`
            // into the per-request trace-context task-local so compliance
            // envelopes carry trace/span ids and CDC publish can re-inject them.
            .layer(crate::runtime::otel::TraceExtractLayer::new())
            .timeout(grpc_timeout)
            .concurrency_limit(grpc_max_concurrent)
            .into_inner()
    };

    let mut server = tonic::transport::Server::builder().layer(make_layer());
    if let Some(tls) = tls_config_from_settings(&runtime_config.service.tls)? {
        server = server.tls_config(tls)?;
    }
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(UDB_FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let native_control_plane_enabled = native_registry::any_control_plane_enabled(&runtime_config);
    let native_webrtc_peer_enabled = native_registry::any_webrtc_peer_enabled(&runtime_config);
    if !native_control_plane_enabled && !native_webrtc_peer_enabled {
        tracing::info!(
            public_addr = %addr,
            "native services disabled or no native listener selected; only the public DataBroker listener will start"
        );
        server
            .add_service(reflection_service)
            .add_service(health_service)
            .add_service(DataBrokerServer::new(service))
            .serve_with_shutdown(addr, shutdown_signal())
            .await?;
        return Ok(());
    }

    // Stage 1 native auth control plane, seeded from the broker's loaded policies.
    let (authn_service, authz_service, api_key_service) = service.build_auth_services();
    // Phase 9: spawn the canary evaluator (metric-based auto-rollback of bad
    // policy canaries before fleet-wide promotion). Detached background task.
    let _canary_evaluator = authz_service.spawn_canary_evaluator();

    // Block 1 (auth_fix.md, Decision A): eager-warm the SHARED authz snapshot
    // from Postgres before serving, then keep it warm on an interval. The cell is
    // built from ABAC at boot and otherwise only reloads lazily on an authz RPC,
    // so the FIRST login on a cold broker would see no role bindings. The authn
    // login path reads this same shared cell to project roles→scopes.
    // `warm_shared_snapshot` retains the last good snapshot on a reload error
    // (GAP-36 posture).
    if service.runtime_snapshot().pg_pool_clone().is_some() {
        authz_service.warm_shared_snapshot().await;
        let warmer = authz_service.clone();
        let warm_interval = warmer.snapshot_ttl().max(std::time::Duration::from_secs(5));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(warm_interval);
            interval.tick().await; // consume the immediate first tick (eager warm already ran)
            loop {
                interval.tick().await;
                warmer.warm_shared_snapshot().await;
            }
        });
    }

    // Phase 3 (I2.1): seed the DB-backed JWT signing-key registry from the env
    // key when the registry is empty, so existing single-key deployments keep
    // working and JWKS publishes from the registry. Best-effort (never fatal).
    let runtime_snapshot = service.runtime_snapshot();
    authn_service
        .seed_signing_key_registry(runtime_snapshot.as_ref())
        .await;

    // Phase 6: auth-plane readiness — surface JWT key / Casbin model
    // misconfiguration loudly at boot instead of failing the first request.
    // Non-fatal (an operator may intentionally run sessions-only with no keys);
    // a failed check logs at error level so it is visible in startup logs.
    {
        let readiness =
            auth_service::readiness::check_auth_readiness(&SecurityConfig::current()).await;
        for check in &readiness.checks {
            if check.ok {
                tracing::info!(check = %check.name, detail = %check.detail, "auth readiness ok");
            } else {
                tracing::error!(check = %check.name, detail = %check.detail, "auth readiness FAILED");
            }
        }
        if !readiness.ok {
            tracing::error!(
                "auth-plane readiness checks failed; serving anyway — review the failed checks above"
            );
            // Emit the operations-plane readiness-failure event so a degraded auth
            // boot is visible on the audit/ops stream (not only in startup logs).
            // The body names exactly which probes failed; their `detail` strings
            // are caller-safe — they never carry key material, by construction in
            // `readiness.rs`. Best-effort: a publish failure is logged, never
            // blocks serving.
            let failed: Vec<serde_json::Value> = readiness
                .checks
                .iter()
                .filter(|c| !c.ok)
                .map(|c| serde_json::json!({ "check": c.name, "detail": c.detail }))
                .collect();
            let failed_names = readiness
                .checks
                .iter()
                .filter(|c| !c.ok)
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
                .join(",");
            let operation_id = uuid::Uuid::new_v4().to_string();
            authn_service
                .emit_ops_event(
                    auth_service::events::AuthEvent::new(
                        auth_service::events::topics::OPS_READINESS_FAILURE,
                        operation_id.clone(),
                        String::new(),
                        serde_json::json!({
                            "operation_id": operation_id,
                            "failed_checks": failed,
                        }),
                    )
                    .with_correlation(operation_id.clone())
                    .with_compliance(
                        auth_service::events::ComplianceEnvelope {
                            actor: "udb.auth.readiness".to_string(),
                            target_resource: "auth-plane".to_string(),
                            operation: "readiness_check".to_string(),
                            outcome: "failure".to_string(),
                            reason_code: if failed_names.is_empty() {
                                "auth_readiness_failed".to_string()
                            } else {
                                format!("auth_readiness_failed:{failed_names}")
                            },
                            ..auth_service::events::ComplianceEnvelope::default()
                        },
                    ),
                )
                .await;
        }
    }

    // Phase J: native enterprise IdP control-plane service (providers, SAML,
    // SCIM, JIT, external-identity linking). Proto-driven Postgres CRUD.
    let identity_provider_service = service.build_identity_provider_service();
    // Tier-7 #31: a second IdP impl (same pool/runtime/sink) backs the optional
    // SCIM 2.0 HTTP/REST surface. Built here while `service` is still in scope
    // (it is moved into the data-plane server below). The listener only binds
    // when UDB_SCIM_HTTP_ADDR is set, so this is otherwise an idle Arc.
    let scim_http_idp = std::sync::Arc::new(service.build_identity_provider_service());

    // Phase 9: versioned control-plane policy distribution (xDS-style) — streams
    // versioned resources to nodes with ACK/NACK/nonce + ordered delivery.
    let control_plane_service = service.build_control_plane_service();

    // Native tenant + notification + analytics control-plane services
    // (proto-driven Postgres CRUD).
    let tenant_service = service.build_tenant_service();
    let notification_service = service.build_notification_service();
    let analytics_service = service.build_analytics_service();
    // Native storage (metadata/lifecycle), asset-management (pipelines), and
    // WebRTC (rooms/peers/tracks/TURN/signaling) control-plane services.
    let storage_service = service.build_storage_service();
    let asset_service = service.build_asset_service();
    let webrtc_service = service.build_webrtc_service();

    // storage→asset auto-trigger: a detached Kafka consumer that turns finalized
    // storage files (`udb.storage.file.finalized.v1`) into asset pipelines. Only
    // when the kafka feature is built and brokers are configured.
    #[cfg(feature = "kafka")]
    if let Some(brokers) = runtime_config.kafka_brokers.clone() {
        std::sync::Arc::new(service.build_asset_service())
            .spawn_storage_finalized_consumer(brokers);
    }

    // Network-isolate the native auth control plane. `AuthnService` /
    // `AuthzService` / `ApiKeyService` trust their caller — they are a policy
    // decision point that accepts the subject principal as input, plus a
    // verified-external-claims bridge — so they must NOT be exposed on the public
    // `DataBroker` listener where any client could assert arbitrary identity,
    // roles, or scopes. They bind to a separate address (`UDB_AUTH_GRPC_ADDR`),
    // defaulting to loopback so only trusted same-host PEPs/gateways can reach
    // them out of the box; operators set it to an internal interface for
    // cross-host PEPs.
    let auth_addr: SocketAddr = match runtime_config.native_services.control_plane_addr.as_str() {
        raw if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .map_err(|err| format!("invalid UDB_AUTH_GRPC_ADDR '{raw}': {err}"))?,
        _ => SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port().wrapping_add(10),
        ),
    };
    tracing::info!(
        %auth_addr,
        public_addr = %addr,
        "native auth control plane (Authn/Authz/ApiKey) bound to an internal \
         listener, isolated from the public DataBroker port; set \
         UDB_AUTH_GRPC_ADDR to expose it on a trusted interface"
    );

    // Peer-facing WebRTC listener. Admin/control-plane WebRTC RPCs stay on the
    // native listener above; browser/app peers use this separate listener with
    // room/peer JWT scopes instead of `udb:admin`.
    let webrtc_addr: SocketAddr = match runtime_config.native_services.webrtc_peer_addr.as_str() {
        raw if !raw.trim().is_empty() => raw
            .trim()
            .parse()
            .map_err(|err| format!("invalid UDB_WEBRTC_GRPC_ADDR '{raw}': {err}"))?,
        _ => SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            addr.port().wrapping_add(20),
        ),
    };
    tracing::info!(
        %webrtc_addr,
        public_addr = %addr,
        "WebRTC peer listener bound with peer-token auth; set \
         UDB_WEBRTC_GRPC_ADDR to expose it on a trusted interface"
    );

    let mut auth_server = tonic::transport::Server::builder().layer(make_layer());
    if let Some(tls) = tls_config_from_settings(&runtime_config.service.tls)? {
        auth_server = auth_server.tls_config(tls)?;
    }
    // Phase 1: per-RPC auth is driven by the proto `endpoint_security`
    // annotations (see `method_security`). A tower layer is used instead of a
    // tonic interceptor because only the layer sees the request URI/path needed
    // to select per-method policy.
    let msec = method_security::MethodSecurityLayer::new().with_metrics(metrics.clone());
    // Phase 10: gRPC health reporting for the native control-plane listener
    // (DataBroker already has one); marks each native service Serving iff mounted.
    let native_health_runtime = service.runtime_snapshot();
    let native_health = handlers_meta::build_listener_health_service(
        handlers_meta::HealthPlane::NativeControlPlane,
        &runtime_config,
        Some(native_health_runtime.as_ref()),
    )
    .await;
    let auth_fut = auth_server
        .add_service(native_health)
        .add_service(msec.wrap(auth_service::AuthnServiceServer::new(authn_service)))
        .add_service(msec.wrap(auth_service::AuthzServiceServer::new(authz_service)))
        .add_service(msec.wrap(auth_service::ApiKeyServiceServer::new(api_key_service)))
        .add_service(msec.wrap(auth_service::IdentityProviderServiceServer::new(
            identity_provider_service,
        )))
        .add_service(msec.wrap(auth_service::ControlPlaneServiceServer::new(
            control_plane_service,
        )))
        .add_service(msec.wrap(tenant_service::TenantServiceServer::new(tenant_service)))
        .add_service(
            msec.wrap(notification_service::NotificationServiceServer::new(
                notification_service,
            )),
        )
        .add_service(msec.wrap(analytics_service::AnalyticsServiceServer::new(
            analytics_service,
        )))
        .add_service(msec.wrap(storage_service::StorageServiceServer::new(storage_service)))
        .add_service(msec.wrap(asset_service::AssetServiceServer::new(asset_service)))
        // WebRTC ships five tonic services on one (Clone) impl; each mounts with
        // the same proto-driven method-security layer.
        .add_service(msec.wrap(webrtc_service::RoomServiceServer::new(
            webrtc_service.clone(),
        )))
        .add_service(msec.wrap(webrtc_service::PeerServiceServer::new(
            webrtc_service.clone(),
        )))
        .add_service(msec.wrap(webrtc_service::TrackServiceServer::new(
            webrtc_service.clone(),
        )))
        .add_service(msec.wrap(webrtc_service::TurnServiceServer::new(
            webrtc_service.clone(),
        )))
        .add_service(msec.wrap(webrtc_service::SignalingServiceServer::new(
            webrtc_service.clone(),
        )))
        .serve_with_shutdown(auth_addr, shutdown_signal());

    let mut webrtc_peer_server = tonic::transport::Server::builder().layer(make_layer());
    if let Some(tls) = tls_config_from_settings(&runtime_config.service.tls)? {
        webrtc_peer_server = webrtc_peer_server.tls_config(tls)?;
    }
    let peer_auth = WebrtcPeerTokenAuth::new();
    let webrtc_health_runtime = service.runtime_snapshot();
    let webrtc_peer_health = handlers_meta::build_listener_health_service(
        handlers_meta::HealthPlane::WebRtcPeer,
        &runtime_config,
        Some(webrtc_health_runtime.as_ref()),
    )
    .await;
    let webrtc_peer_fut = webrtc_peer_server
        .add_service(webrtc_peer_health)
        .add_service(
            msec.wrap(webrtc_service::PeerServiceServer::with_interceptor(
                webrtc_service.clone(),
                peer_auth.clone(),
            )),
        )
        .add_service(
            msec.wrap(webrtc_service::TrackServiceServer::with_interceptor(
                webrtc_service.clone(),
                peer_auth.clone(),
            )),
        )
        .add_service(
            msec.wrap(webrtc_service::TurnServiceServer::with_interceptor(
                webrtc_service.clone(),
                peer_auth.clone(),
            )),
        )
        .add_service(
            msec.wrap(webrtc_service::SignalingServiceServer::with_interceptor(
                webrtc_service,
                peer_auth,
            )),
        )
        .serve_with_shutdown(webrtc_addr, shutdown_signal());

    let main_fut = server
        .add_service(reflection_service)
        .add_service(health_service)
        .add_service(DataBrokerServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal());

    // Optional ws:// signalling bridge (feature `ws-signalling`, activated by
    // UDB_WS_SIGNALLING_ADDR). Runs as a detached task bound to the same shutdown
    // signal as tonic: a signalling failure logs but never brings down the data
    // plane.
    #[cfg(feature = "ws-signalling")]
    let _ws_signalling = crate::runtime::signalling::SignalingServer::spawn_from_env_with_shutdown(
        shutdown_signal(),
    );

    // Tier-7 #31: optional SCIM 2.0 HTTP/REST surface for off-the-shelf
    // provisioners (Okta/Entra/OneLogin). OFF by default; binds only when
    // UDB_SCIM_HTTP_ADDR is set (and a bearer token is configured). Maps HTTP
    // requests onto the SAME gRPC SCIM handlers, so persistence + IdP events are
    // not duplicated. Detached task bound to the shared shutdown signal.
    let _scim_http = auth_service::spawn_scim_http_from_env(scim_http_idp, shutdown_signal());

    // Run selected listeners together; if one exits (error or shutdown), bring down all.
    match (native_control_plane_enabled, native_webrtc_peer_enabled) {
        (true, true) => {
            tokio::try_join!(main_fut, auth_fut, webrtc_peer_fut)?;
        }
        (true, false) => {
            tokio::try_join!(main_fut, auth_fut)?;
        }
        (false, true) => {
            tokio::try_join!(main_fut, webrtc_peer_fut)?;
        }
        (false, false) => unreachable!("handled before native service construction"),
    }
    Ok(())
}

#[cfg(feature = "kafka")]
async fn start_cdc_engine(
    runtime: &DataBrokerRuntime,
    metrics: Arc<dyn MetricsRecorder>,
) -> Option<Arc<CdcEngine>> {
    let Some(kafka_brokers) = runtime.config().kafka_brokers.clone() else {
        tracing::info!("CDC tailer disabled: kafka_brokers is not configured");
        return None;
    };
    let Some(pg_pool) = runtime.pg_pool_clone() else {
        tracing::warn!("CDC tailer disabled: PostgreSQL pool is not configured");
        return None;
    };
    let pg_dsn = if !runtime.config().primary.direct_dsn.trim().is_empty() {
        runtime.config().primary.direct_dsn.trim().to_string()
    } else {
        runtime.config().primary.pooler_dsn.trim().to_string()
    };
    if pg_dsn.is_empty() {
        tracing::warn!("CDC tailer disabled: primary PostgreSQL DSN is not configured");
        return None;
    };
    let singleton_pool = pg_pool.clone();
    let singleton_relation = runtime.config().cdc.lock_log_relation();

    #[cfg(feature = "redis")]
    let engine = CdcEngine::new(
        pg_pool,
        runtime.redis_clone(),
        &kafka_brokers,
        pg_dsn,
        metrics,
        runtime.config().cdc.clone(),
    );
    #[cfg(not(feature = "redis"))]
    let engine = CdcEngine::new(
        pg_pool,
        &kafka_brokers,
        pg_dsn,
        metrics,
        runtime.config().cdc.clone(),
    );
    match engine {
        Ok(mut engine) => {
            // Phase 7: load the topic-policy allowlist into the engine before it
            // starts tailing. Without this the allowlist stays empty and topic
            // policy enforcement in process_outbox_event is dormant in prod.
            if let Err(err) = engine.load_topic_policies().await {
                tracing::warn!("CDC topic policy load failed: {err}");
            }
            let engine = Arc::new(engine);
            // U21: reset in-doubt `publishing` rows left by a prior process epoch
            // before tailing (no-op outside KafkaTransactional mode).
            if let Err(err) = engine.run_indoubt_recovery_on_startup().await {
                tracing::warn!("CDC in-doubt recovery on startup failed: {err}");
            }
            tokio::spawn({
                let engine = engine.clone();
                async move {
                    engine.run_advisory_lock_loop().await;
                }
            });
            // Item 6: wire the generic `CdcSource` path for configured native
            // source adapters. Each source persists offsets through the same
            // `tail_source` loop; operators opt in per backend with env config.
            if let Ok(dsn) = std::env::var("UDB_CDC_POSTGRES_SOURCE_DSN") {
                if !dsn.trim().is_empty() {
                    let relation = std::env::var("UDB_CDC_POSTGRES_SOURCE_TABLE")
                        .unwrap_or_else(|_| "udb_system.udb_cdc_outbox".to_string());
                    let source: std::sync::Arc<dyn crate::runtime::cdc::CdcSource> =
                        std::sync::Arc::new(crate::runtime::cdc::source::PostgresCdcSource {
                            dsn,
                            publication: relation,
                            slot: "udb-postgres-source".to_string(),
                        });
                    let engine = engine.clone();
                    let singleton_pool = singleton_pool.clone();
                    let singleton_relation = singleton_relation.clone();
                    tokio::spawn(async move {
                        loop {
                            let engine = engine.clone();
                            let source = source.clone();
                            match crate::runtime::singleton::run_while_leader(
                                &singleton_pool,
                                &singleton_relation,
                                crate::runtime::singleton::WORKER_CDC_POSTGRES_SOURCE,
                                crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                                || async move { engine.tail_source(source).await },
                            )
                            .await
                            {
                                Ok(Some(Ok(()))) => {}
                                Ok(Some(Err(err))) => {
                                    tracing::warn!("CDC Postgres source tailer exited: {err}")
                                }
                                Ok(None) => tracing::debug!(
                                    "CDC Postgres source tailer idle: singleton lease held by peer"
                                ),
                                Err(err) => {
                                    tracing::warn!("CDC Postgres source tailer lease failed: {err}")
                                }
                            }
                            tokio::time::sleep(
                                crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP,
                            )
                            .await;
                        }
                    });
                    tracing::info!("CDC Postgres table source tailer started");
                }
            }
            #[cfg(feature = "mysql")]
            if let Ok(dsn) = std::env::var("UDB_CDC_MYSQL_SOURCE_DSN") {
                if !dsn.trim().is_empty() {
                    let server_id = std::env::var("UDB_CDC_MYSQL_SOURCE_SERVER_ID")
                        .ok()
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(5401);
                    let source: std::sync::Arc<dyn crate::runtime::cdc::CdcSource> =
                        std::sync::Arc::new(crate::runtime::cdc::source::MysqlBinlogSource {
                            dsn,
                            server_id,
                        });
                    let engine = engine.clone();
                    let singleton_pool = singleton_pool.clone();
                    let singleton_relation = singleton_relation.clone();
                    tokio::spawn(async move {
                        loop {
                            let engine = engine.clone();
                            let source = source.clone();
                            match crate::runtime::singleton::run_while_leader(
                                &singleton_pool,
                                &singleton_relation,
                                crate::runtime::singleton::WORKER_CDC_MYSQL_SOURCE,
                                crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                                || async move { engine.tail_source(source).await },
                            )
                            .await
                            {
                                Ok(Some(Ok(()))) => {}
                                Ok(Some(Err(err))) => {
                                    tracing::warn!("CDC MySQL source tailer exited: {err}")
                                }
                                Ok(None) => tracing::debug!(
                                    "CDC MySQL source tailer idle: singleton lease held by peer"
                                ),
                                Err(err) => {
                                    tracing::warn!("CDC MySQL source tailer lease failed: {err}")
                                }
                            }
                            tokio::time::sleep(
                                crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP,
                            )
                            .await;
                        }
                    });
                    tracing::info!("CDC MySQL table source tailer started");
                }
            }
            #[cfg(feature = "mongodb-native")]
            if let Ok(uri) = std::env::var("UDB_CDC_MONGO_SOURCE_URI") {
                let database = std::env::var("UDB_CDC_MONGO_SOURCE_DB").unwrap_or_default();
                if !uri.trim().is_empty() && !database.trim().is_empty() {
                    let collection = std::env::var("UDB_CDC_MONGO_SOURCE_COLLECTION")
                        .ok()
                        .filter(|v| !v.trim().is_empty());
                    let source: std::sync::Arc<dyn crate::runtime::cdc::CdcSource> =
                        std::sync::Arc::new(crate::runtime::cdc::source::MongoCdcSource {
                            uri,
                            database,
                            collection,
                        });
                    let engine = engine.clone();
                    let singleton_pool = singleton_pool.clone();
                    let singleton_relation = singleton_relation.clone();
                    tokio::spawn(async move {
                        loop {
                            let engine = engine.clone();
                            let source = source.clone();
                            match crate::runtime::singleton::run_while_leader(
                                &singleton_pool,
                                &singleton_relation,
                                crate::runtime::singleton::WORKER_CDC_MONGODB_SOURCE,
                                crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                                || async move { engine.tail_source(source).await },
                            )
                            .await
                            {
                                Ok(Some(Ok(()))) => {}
                                Ok(Some(Err(err))) => {
                                    tracing::warn!("CDC MongoDB source tailer exited: {err}")
                                }
                                Ok(None) => tracing::debug!(
                                    "CDC MongoDB source tailer idle: singleton lease held by peer"
                                ),
                                Err(err) => {
                                    tracing::warn!("CDC MongoDB source tailer lease failed: {err}")
                                }
                            }
                            tokio::time::sleep(
                                crate::runtime::singleton::WORKER_SINGLETON_RETRY_SLEEP,
                            )
                            .await;
                        }
                    });
                    tracing::info!("CDC MongoDB change-stream source tailer started");
                }
            }
            Some(engine)
        }
        Err(err) => {
            tracing::warn!("CDC tailer disabled: Kafka producer initialization failed: {err}");
            None
        }
    }
}

async fn metrics_http_server(
    metrics: Arc<PrometheusMetrics>,
    runtime: DataBrokerRuntime,
    addr: SocketAddr,
    allowed_cidr: Option<String>,
) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!("metrics endpoint disabled: {err}");
            return;
        }
    };

    // GAP 18: Optional IP-allowlist for the metrics scrape endpoint.
    let allowed_cidr: Option<String> = allowed_cidr
        .map(|cidr| cidr.trim().to_string())
        .filter(|cidr| !cidr.is_empty());

    loop {
        let Ok((mut socket, peer)) = listener.accept().await else {
            continue;
        };

        // GAP 18: Enforce IP allowlist when configured.
        if let Some(allow) = &allowed_cidr
            && !ip_matches_allow_entry(peer.ip(), allow)
        {
            tracing::debug!(peer = %peer.ip(), "metrics scrape rejected: not in allowed CIDR");
            continue;
        }

        refresh_native_service_degraded_metrics(&runtime, metrics.as_ref());
        let text = metrics.gather_text(&format!(
            "{}{}",
            runtime.cache_metrics_text(),
            format!(
                "{}{}",
                runtime.encryption_metrics_text(),
                runtime.pg_pool_metrics_text()
            )
        ));
        let ready_runtime = runtime.clone();
        let ready_metrics = metrics.clone();
        tokio::spawn(async move {
            // GAP 18: Read the incoming request with a 5-second deadline.
            // Without this, port scanners get a 200 OK with full metrics data
            // before sending a single byte, and slow-loris clients hold
            // connections open indefinitely.
            let mut buf = [0u8; 256];
            let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or(0);
            // Phase 10: the metrics listener also answers HTTP liveness/readiness
            // probes (`/healthz`, `/readyz`). `/readyz` is derived from the same
            // ReadinessFacts as GetHealthReport and doctor, so the scrape
            // listener cannot claim a disconnected static readiness posture.
            let request = String::from_utf8_lossy(&buf[..n]);
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/");
            let response = if path == "/healthz" {
                let body = "ok\n";
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
            } else if path == "/readyz" {
                let (status, body) =
                    metrics_readiness_response(&ready_runtime, ready_metrics.as_ref()).await;
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
            } else {
                // GAP 18: charset=utf-8 is required by the Prometheus text format spec.
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    text.len(),
                    text
                )
            };
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

async fn metrics_readiness_response(
    runtime: &DataBrokerRuntime,
    metrics: &dyn MetricsRecorder,
) -> (&'static str, String) {
    let native_statuses =
        crate::runtime::service::native_registry::resolved_native_service_statuses(
            runtime.config(),
        );
    refresh_native_service_degraded_metrics(runtime, metrics);
    let auth_triples = auth_readiness_triples(&SecurityConfig::current()).await;
    let readiness = crate::runtime::slo::build_readiness_facts(
        runtime.init_report(),
        &native_statuses,
        &auth_triples,
    );
    if readiness.passed() {
        return ("200 OK", "ready\n".to_string());
    }

    let mut body = String::from("not ready\n");
    for err in readiness.errors() {
        body.push_str("error: ");
        body.push_str(&err);
        body.push('\n');
    }
    for warn in readiness.warnings() {
        body.push_str("warning: ");
        body.push_str(&warn);
        body.push('\n');
    }
    ("503 Service Unavailable", body)
}

fn refresh_native_service_degraded_metrics(
    runtime: &DataBrokerRuntime,
    metrics: &dyn MetricsRecorder,
) {
    for status in
        crate::runtime::service::native_registry::resolved_native_service_statuses(runtime.config())
    {
        metrics.set_native_service_degraded(
            &status.service_id,
            status.degraded || (status.enabled && !status.mounted),
        );
    }
}

async fn cdc_metrics_poller(runtime: DataBrokerRuntime, metrics: Arc<dyn MetricsRecorder>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;
        if let Ok((lag, depth)) = runtime.cdc_outbox_metrics().await {
            metrics.set_cdc_lag_seconds(lag);
            metrics.set_cdc_outbox_depth(depth);
        }
    }
}

fn validate_secure_transport(
    service: &crate::runtime::config::ServiceSettings,
) -> Result<(), String> {
    if service.require_secure_transport && !service.tls.has_server_identity() {
        return Err(
            "UDB_REQUIRE_SECURE_TRANSPORT/UDB_TLS_REQUIRED is enabled but TLS cert/key is not configured"
                .to_string(),
        );
    }
    let any_mtls_required = service.mtls_required
        || service.broker_to_broker_mtls_required
        || service.internal_control_mtls_required;
    if any_mtls_required {
        if !service.tls.has_server_identity() {
            return Err("mTLS is enabled but TLS cert/key is not configured".to_string());
        }
        if !service.tls.has_client_ca() {
            return Err(
                "mTLS is enabled but UDB_MTLS_CLIENT_CA_PEM/PATH is not configured".to_string(),
            );
        }
    }
    Ok(())
}

fn tls_config_from_settings(
    settings: &crate::runtime::config::TlsSettings,
) -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    let Some(cert) = config_bytes(settings.cert_pem.as_deref(), settings.cert_path.as_deref())?
    else {
        return Ok(None);
    };
    let Some(key) = config_bytes(settings.key_pem.as_deref(), settings.key_path.as_deref())? else {
        return Ok(None);
    };
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
    if let Some(ca) = config_bytes(
        settings.client_ca_pem.as_deref(),
        settings.client_ca_path.as_deref(),
    )? {
        tls = tls.client_ca_root(Certificate::from_pem(ca));
    }
    Ok(Some(tls))
}

fn config_bytes(
    pem: Option<&str>,
    path: Option<&str>,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    if let Some(value) = pem.filter(|value| !value.trim().is_empty()) {
        return Ok(Some(value.as_bytes().to_vec()));
    }
    if let Some(value) = path.filter(|value| !value.trim().is_empty()) {
        return Ok(Some(fs::read(value)?));
    }
    Ok(None)
}

fn proto_cdc_envelope(envelope: crate::cdc::CdcEnvelope) -> CdcEnvelope {
    CdcEnvelope {
        event_id: envelope.event_id,
        topic: envelope.topic,
        partition_key: envelope.partition_key,
        payload_json: envelope.payload_json,
        published_at: Some(prost_types::Timestamp {
            seconds: envelope.published_at.timestamp(),
            nanos: envelope.published_at.timestamp_subsec_nanos() as i32,
        }),
    }
}

type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .unwrap_or_else(|err| {
                    // Signal handler installation can fail in some restricted container
                    // environments. Log the error but don't crash — ctrl-c will still work.
                    tracing::warn!(
                        "failed to install SIGTERM handler: {}; only SIGINT (ctrl-c) will trigger graceful shutdown",
                        err
                    );
                    // Return a signal stream that never fires as a safe fallback.
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                        .expect("install SIGHUP fallback handler")
                });
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn spawn_config_reload_watcher(service: DataBrokerService) {
    #[cfg(unix)]
    tokio::spawn(async move {
        let mut hangup = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        {
            Ok(signal) => signal,
            Err(err) => {
                tracing::warn!("failed to install SIGHUP reload handler: {err}");
                return;
            }
        };
        while hangup.recv().await.is_some() {
            let report = service.reload_runtime_from_env("sighup").await;
            if report.applied {
                tracing::info!(
                    reload_id = report.reload_id,
                    previous_generation = report.previous_generation,
                    new_generation = report.new_generation,
                    previous_client_count = report.previous_client_count,
                    active_client_count = report.active_client_count,
                    changed_instances = ?report.changed_instances,
                    "UDB runtime config reload applied"
                );
            } else {
                tracing::warn!(
                    reload_id = report.reload_id,
                    accepted = report.accepted,
                    rolled_back = report.rolled_back,
                    validation_errors = ?report.validation_errors,
                    failed_health_checks = ?report.failed_health_checks,
                    warnings = ?report.warnings,
                    "UDB runtime config reload rejected"
                );
            }
        }
    });

    #[cfg(not(unix))]
    {
        let _ = service;
    }
}

fn saga_record_to_proto(r: crate::runtime::saga::SagaAdminRecord) -> SagaRecord {
    SagaRecord {
        saga_id: r.saga_id,
        tx_id: r.tx_id,
        tenant_id: r.tenant_id,
        correlation_id: r.correlation_id,
        status: r.status,
        current_step: r.current_step,
        steps_json: r.steps_json.to_string().into_bytes(),
        compensations_json: r.compensations_json.to_string().into_bytes(),
        last_error: r.last_error,
        created_at_unix: r.created_at.timestamp(),
        updated_at_unix: r.updated_at.timestamp(),
    }
}

fn insert_ascii_header(
    metadata: &mut tonic::metadata::MetadataMap,
    key: &'static str,
    value: &str,
) {
    if value.trim().is_empty() {
        return;
    }
    if let Ok(parsed) = value.parse() {
        metadata.insert(key, parsed);
    }
}

// Phase G: service.rs split — inherent RPC handler bodies + tests.
mod handlers_admin;
mod handlers_catalog;
mod handlers_data;
mod handlers_meta;
mod handlers_object;
mod handlers_policy;
mod handlers_resource;
mod handlers_stores;
mod handlers_tx;
mod handlers_vector;

#[tonic::async_trait]

impl DataBroker for DataBrokerService {
    type BatchSelectStream = ResponseStream<RecordSet>;
    type SelectV2Stream = ResponseStream<crate::proto::RecordBatchV2>;
    type BatchUpsertStream = ResponseStream<MutationResponse>;
    type VectorBatchUpsertStream = ResponseStream<MutationResponse>;
    type GetObjectStream = ResponseStream<Chunk>;
    type BeginTxStream = ResponseStream<TxStatus>;
    type PublishCDCStream = ResponseStream<CdcEnvelope>;

    async fn select(&self, request: Request<SelectRequest>) -> Result<Response<RecordSet>, Status> {
        self.select_inner(request).await
    }

    async fn select_v2(
        &self,
        request: Request<SelectRequest>,
    ) -> Result<Response<Self::SelectV2Stream>, Status> {
        self.select_v2_inner(request).await
    }

    async fn batch_select(
        &self,
        request: Request<tonic::Streaming<SelectRequest>>,
    ) -> Result<Response<Self::BatchSelectStream>, Status> {
        self.batch_select_inner(request).await
    }

    async fn upsert(
        &self,
        request: Request<UpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.upsert_inner(request).await
    }

    async fn batch_upsert(
        &self,
        request: Request<tonic::Streaming<UpsertRequest>>,
    ) -> Result<Response<Self::BatchUpsertStream>, Status> {
        self.batch_upsert_inner(request).await
    }

    async fn vector_search(
        &self,
        request: Request<VectorSearchRequest>,
    ) -> Result<Response<VectorSet>, Status> {
        self.vector_search_inner(request).await
    }

    async fn vector_hybrid_search(
        &self,
        request: Request<VectorHybridSearchRequest>,
    ) -> Result<Response<VectorSet>, Status> {
        self.vector_hybrid_search_inner(request).await
    }

    async fn vector_upsert(
        &self,
        request: Request<VectorUpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.vector_upsert_inner(request).await
    }

    async fn vector_batch_upsert(
        &self,
        request: Request<tonic::Streaming<VectorUpsertRequest>>,
    ) -> Result<Response<Self::VectorBatchUpsertStream>, Status> {
        self.vector_batch_upsert_inner(request).await
    }

    async fn put_object(
        &self,
        request: Request<tonic::Streaming<Chunk>>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.put_object_inner(request).await
    }

    async fn get_object(
        &self,
        request: Request<crate::proto::ObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        self.get_object_inner(request).await
    }

    async fn generate_presigned_url(
        &self,
        request: Request<UrlRequest>,
    ) -> Result<Response<UrlResponse>, Status> {
        self.generate_presigned_url_inner(request).await
    }

    async fn initiate_multipart_upload(
        &self,
        request: Request<MultipartUploadRequest>,
    ) -> Result<Response<MultipartUploadResponse>, Status> {
        self.initiate_multipart_upload_inner(request).await
    }

    async fn begin_tx(
        &self,
        request: Request<tonic::Streaming<Mutation>>,
    ) -> Result<Response<Self::BeginTxStream>, Status> {
        self.begin_tx_inner(request).await
    }

    async fn publish_cdc(
        &self,
        request: Request<CdcSubscriptionRequest>,
    ) -> Result<Response<Self::PublishCDCStream>, Status> {
        self.publish_cdc_inner(request).await
    }

    async fn create_materialized_view(
        &self,
        request: Request<ViewDefinition>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.create_materialized_view_inner(request).await
    }

    async fn enqueue_outbox_event(
        &self,
        request: Request<EnqueueOutboxEventRequest>,
    ) -> Result<Response<EnqueueOutboxEventResponse>, Status> {
        self.enqueue_outbox_event_inner(request).await
    }

    async fn get_capabilities(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<CapabilitiesResponse>, Status> {
        self.get_capabilities_inner(request).await
    }

    async fn get_catalog_manifest(
        &self,
        request: Request<CatalogManifestRequest>,
    ) -> Result<Response<CatalogManifestResponse>, Status> {
        self.get_catalog_manifest_inner(request).await
    }

    async fn lookup_message_schema(
        &self,
        request: Request<MessageSchemaLookupRequest>,
    ) -> Result<Response<MessageSchemaLookupResponse>, Status> {
        self.lookup_message_schema_inner(request).await
    }

    async fn list_message_schemas(
        &self,
        request: Request<MessageSchemaListRequest>,
    ) -> Result<Response<MessageSchemaListResponse>, Status> {
        self.list_message_schemas_inner(request).await
    }

    async fn get_health_report(
        &self,
        request: Request<HealthReportRequest>,
    ) -> Result<Response<HealthReportResponse>, Status> {
        self.get_health_report_inner(request).await
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.delete_inner(request).await
    }

    async fn generic_dispatch(
        &self,
        request: Request<GenericDispatchRequest>,
    ) -> Result<Response<GenericDispatchResponse>, Status> {
        self.generic_dispatch_inner(request).await
    }

    async fn ensure_resource(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.ensure_resource_inner(request).await
    }

    async fn drop_resource(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.drop_resource_inner(request).await
    }

    async fn list_resources(
        &self,
        request: Request<ResourceAdminRequest>,
    ) -> Result<Response<ResourceListResponse>, Status> {
        self.list_resources_inner(request).await
    }

    async fn stage_catalog(
        &self,
        request: Request<StageCatalogRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        self.stage_catalog_inner(request).await
    }

    async fn activate_catalog(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        self.activate_catalog_inner(request).await
    }

    async fn rollback_catalog(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        self.rollback_catalog_inner(request).await
    }

    async fn validate_catalog(
        &self,
        request: Request<StageCatalogRequest>,
    ) -> Result<Response<CatalogValidationResponse>, Status> {
        self.validate_catalog_inner(request).await
    }

    async fn get_catalog_versions(
        &self,
        request: Request<CatalogManifestRequest>,
    ) -> Result<Response<CatalogVersionListResponse>, Status> {
        self.get_catalog_versions_inner(request).await
    }

    async fn get_catalog_version(
        &self,
        request: Request<CatalogVersionRequest>,
    ) -> Result<Response<CatalogVersionResponse>, Status> {
        self.get_catalog_version_inner(request).await
    }

    async fn plan_migration(
        &self,
        request: Request<MigrationPlanRequest>,
    ) -> Result<Response<MigrationPlanResponse>, Status> {
        self.plan_migration_inner(request).await
    }

    async fn apply_migration(
        &self,
        request: Request<MigrationApplyRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        self.apply_migration_inner(request).await
    }

    async fn get_migration_status(
        &self,
        request: Request<MigrationRunRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        self.get_migration_status_inner(request).await
    }

    async fn list_migration_runs(
        &self,
        request: Request<MigrationRunListRequest>,
    ) -> Result<Response<MigrationRunListResponse>, Status> {
        self.list_migration_runs_inner(request).await
    }

    async fn approve_migration_plan(
        &self,
        request: Request<MigrationRunRequest>,
    ) -> Result<Response<MigrationStatusResponse>, Status> {
        self.approve_migration_plan_inner(request).await
    }

    async fn list_dlq_events(
        &self,
        request: Request<DlqListRequest>,
    ) -> Result<Response<DlqListResponse>, Status> {
        self.list_dlq_events_inner(request).await
    }

    async fn get_dlq_event(
        &self,
        request: Request<DlqEventRequest>,
    ) -> Result<Response<DlqEventResponse>, Status> {
        self.get_dlq_event_inner(request).await
    }

    async fn replay_dlq_event(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.replay_dlq_event_inner(request).await
    }

    async fn dismiss_dlq_event(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.dismiss_dlq_event_inner(request).await
    }

    async fn quarantine_dlq_event(
        &self,
        request: Request<DlqActionRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.quarantine_dlq_event_inner(request).await
    }

    async fn get_cdc_status(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        self.get_cdc_status_inner(request).await
    }

    async fn pause_cdc(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        self.pause_cdc_inner(request).await
    }

    async fn resume_cdc(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        self.resume_cdc_inner(request).await
    }

    async fn step_down_cdc_leader(
        &self,
        request: Request<CdcControlRequest>,
    ) -> Result<Response<CdcStatusResponse>, Status> {
        self.step_down_cdc_leader_inner(request).await
    }

    async fn preview_cdc_redaction(
        &self,
        request: Request<CdcRedactionPreviewRequest>,
    ) -> Result<Response<CdcRedactionPreviewResponse>, Status> {
        self.preview_cdc_redaction_inner(request).await
    }

    async fn scan_projection_drift(
        &self,
        request: Request<ProjectionDriftScanRequest>,
    ) -> Result<Response<ProjectionDriftScanResponse>, Status> {
        self.scan_projection_drift_inner(request).await
    }

    async fn list_sagas(
        &self,
        request: Request<SagaListRequest>,
    ) -> Result<Response<SagaListResponse>, Status> {
        self.list_sagas_inner(request).await
    }

    async fn get_saga(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        self.get_saga_inner(request).await
    }

    async fn retry_saga_compensation(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        self.retry_saga_compensation_inner(request).await
    }

    async fn mark_saga_reviewed(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaResponse>, Status> {
        self.mark_saga_reviewed_inner(request).await
    }

    async fn list_policies(
        &self,
        request: Request<PolicyListRequest>,
    ) -> Result<Response<PolicyListResponse>, Status> {
        self.list_policies_inner(request).await
    }

    async fn put_policy(
        &self,
        request: Request<PutPolicyRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.put_policy_inner(request).await
    }

    async fn delete_policy(
        &self,
        request: Request<PolicyRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.delete_policy_inner(request).await
    }

    async fn reload_policies(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.reload_policies_inner(request).await
    }

    async fn lint_policies(
        &self,
        request: Request<CapabilitiesRequest>,
    ) -> Result<Response<PolicyLintResponse>, Status> {
        self.lint_policies_inner(request).await
    }

    async fn ensure_project(
        &self,
        request: Request<EnsureProjectRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.ensure_project_inner(request).await
    }

    async fn list_projects(
        &self,
        request: Request<ProjectListRequest>,
    ) -> Result<Response<ProjectListResponse>, Status> {
        self.list_projects_inner(request).await
    }

    async fn get_admin_summary(
        &self,
        request: Request<AdminSummaryRequest>,
    ) -> Result<Response<AdminSummaryResponse>, Status> {
        self.get_admin_summary_inner(request).await
    }

    async fn list_admin_audit_logs(
        &self,
        request: Request<AdminAuditLogRequest>,
    ) -> Result<Response<AdminAuditLogResponse>, Status> {
        self.list_admin_audit_logs_inner(request).await
    }

    async fn verify_admin_audit_log(
        &self,
        request: Request<AdminAuditVerifyRequest>,
    ) -> Result<Response<AdminAuditVerifyResponse>, Status> {
        self.verify_admin_audit_log_inner(request).await
    }

    // ── Cache / Document / Graph / Time-series / Analytical ────────────────────
    // Typed store RPCs added by the 2026-06 proto reorg. Each resolves the
    // backend from its `StoreResource` and runs the real backend executor
    // (`query`/`mutate`/`search`) — implementations in `handlers_stores.rs`.
    async fn cache_get(
        &self,
        request: Request<crate::proto::CacheGetRequest>,
    ) -> Result<Response<crate::proto::CacheGetResponse>, Status> {
        self.cache_get_inner(request).await
    }

    async fn cache_set(
        &self,
        request: Request<crate::proto::CacheSetRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.cache_set_inner(request).await
    }

    async fn cache_delete(
        &self,
        request: Request<crate::proto::CacheDeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.cache_delete_inner(request).await
    }

    async fn cache_scan(
        &self,
        request: Request<crate::proto::CacheScanRequest>,
    ) -> Result<Response<crate::proto::CacheScanResponse>, Status> {
        self.cache_scan_inner(request).await
    }

    async fn document_get(
        &self,
        request: Request<crate::proto::DocumentGetRequest>,
    ) -> Result<Response<crate::proto::DocumentSet>, Status> {
        self.document_get_inner(request).await
    }

    async fn document_find(
        &self,
        request: Request<crate::proto::DocumentFindRequest>,
    ) -> Result<Response<crate::proto::DocumentSet>, Status> {
        self.document_find_inner(request).await
    }

    async fn document_upsert(
        &self,
        request: Request<crate::proto::DocumentUpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.document_upsert_inner(request).await
    }

    async fn document_delete(
        &self,
        request: Request<crate::proto::DocumentDeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.document_delete_inner(request).await
    }

    async fn graph_query(
        &self,
        request: Request<crate::proto::GraphQueryRequest>,
    ) -> Result<Response<crate::proto::GraphResultSet>, Status> {
        self.graph_query_inner(request).await
    }

    async fn graph_mutate(
        &self,
        request: Request<crate::proto::GraphMutationRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.graph_mutate_inner(request).await
    }

    async fn time_series_write(
        &self,
        request: Request<crate::proto::TimeSeriesWriteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.time_series_write_inner(request).await
    }

    async fn time_series_query(
        &self,
        request: Request<crate::proto::TimeSeriesQueryRequest>,
    ) -> Result<Response<crate::proto::TimeSeriesQueryResponse>, Status> {
        self.time_series_query_inner(request).await
    }

    async fn analytical_query(
        &self,
        request: Request<crate::proto::AnalyticalQueryRequest>,
    ) -> Result<Response<crate::proto::AnalyticalQueryResponse>, Status> {
        self.analytical_query_inner(request).await
    }
}

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod tests;
