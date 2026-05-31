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

use crate::ast::ProtoSchema;
use crate::cdc::CdcEngine;
use crate::engine::FsmState;
use crate::generation::CatalogManifest;
use crate::lifecycle::run_startup_lifecycle;
use crate::metrics::{MetricsRecorder, PrometheusMetrics};
use crate::proto::data_broker_server::{DataBroker, DataBrokerServer};
use crate::proto::{
    AdminAuditLogRecord, AdminAuditLogRequest, AdminAuditLogResponse, AdminAuditVerifyRequest,
    AdminAuditVerifyResponse, AdminBackendSummary, AdminCatalogSummary, AdminCdcSummary,
    AdminSagaSummary, AdminSummaryRequest, AdminSummaryResponse, BackendInstanceStatus,
    CapabilitiesRequest, CapabilitiesResponse, CatalogManifestRequest, CatalogManifestResponse,
    CatalogValidationResponse, CatalogVersionListResponse, CatalogVersionRequest,
    CatalogVersionResponse, CdcControlRequest, CdcEnvelope, CdcStatusResponse,
    CdcSubscriptionRequest, Chunk, DeleteRequest, DlqActionRequest, DlqEventRecord,
    DlqEventRequest, DlqEventResponse, DlqListRequest, DlqListResponse, EnqueueOutboxEventRequest,
    EnqueueOutboxEventResponse, EnsureProjectRequest, GenericDispatchRequest,
    GenericDispatchResponse, HealthReportRequest, HealthReportResponse, MessageFieldDescriptor,
    MessageSchemaDescriptor, MessageSchemaListRequest, MessageSchemaListResponse,
    MessageSchemaLookupRequest, MessageSchemaLookupResponse, MigrationApplyRequest,
    MigrationPlanRequest, MigrationPlanResponse, MigrationRunListRequest, MigrationRunListResponse,
    MigrationRunRequest, MigrationStatusResponse, MultipartUploadRequest, MultipartUploadResponse,
    Mutation, MutationResponse, PolicyLintResponse, PolicyListRequest, PolicyListResponse,
    PolicyRecord, PolicyRequest, ProjectListRequest, ProjectListResponse, ProjectRecord,
    PutPolicyRequest, RecordSet, ResourceAdminRequest, ResourceListResponse, SagaListRequest,
    SagaListResponse, SagaRecord, SagaRequest, SagaResponse, SelectRequest, StageCatalogRequest,
    TxStatus, UpsertRequest, UrlRequest, UrlResponse, VectorHybridSearchRequest,
    VectorSearchRequest, VectorSet, VectorUpsertRequest, ViewDefinition,
};
use crate::runtime::DataBrokerRuntime;
use crate::security::{
    AbacPolicy, SecurityContext, enforce_select_export_controls, evaluate_abac,
    ip_matches_allow_entry, security_from_request,
};

const UDB_FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("udb_descriptor");

#[derive(Debug, Clone)]
pub struct DataBrokerService {
    pub catalog: Arc<crate::runtime::catalog::CatalogManager>,
    pub manifest: CatalogManifest,
    pub runtime: Arc<ArcSwap<DataBrokerRuntime>>,
    lifecycle_state: Arc<RwLock<FsmState>>,
    abac_policies: Arc<RwLock<Vec<AbacPolicy>>>,
    metrics: Arc<PrometheusMetrics>,
    cdc_engine: Option<Arc<CdcEngine>>,
    projection_engine: Option<Arc<crate::runtime::projection::ProjectionEngine>>,
    abac_default_allow: bool,
}

pub(crate) const UDB_PROTOCOL_VERSION: &str = "1.0.0";

pub(crate) const SUPPORTED_RPC_NAMES: &[&str] = &[
    "Select",
    "BatchSelect",
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
            metrics: Arc::new(PrometheusMetrics::new().expect("create prometheus metrics")),
            cdc_engine: None,
            projection_engine: None,
            abac_default_allow: false,
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
            metrics: Arc::new(PrometheusMetrics::new().expect("create prometheus metrics")),
            cdc_engine: None,
            projection_engine: None,
            abac_default_allow: false,
        }
    }

    pub fn with_runtime_and_state(
        manifest: CatalogManifest,
        runtime: DataBrokerRuntime,
        lifecycle_state: Arc<RwLock<FsmState>>,
        abac_policies: Arc<RwLock<Vec<AbacPolicy>>>,
        metrics: Arc<PrometheusMetrics>,
        cdc_engine: Option<Arc<CdcEngine>>,
        abac_default_allow: bool,
    ) -> Self {
        let catalog = Arc::new(crate::runtime::catalog::CatalogManager::new(
            manifest.clone(),
        ));
        Self {
            catalog,
            manifest,
            runtime: Arc::new(ArcSwap::from_pointee(runtime)),
            lifecycle_state,
            abac_policies,
            metrics,
            cdc_engine,
            projection_engine: None,
            abac_default_allow,
        }
    }

    pub fn runtime_snapshot(&self) -> DataBrokerRuntime {
        self.runtime.load_full().as_ref().clone()
    }

    pub async fn reload_runtime_from_config(
        &self,
        config: crate::runtime::config::UdbConfig,
        options: crate::runtime::ConfigReloadOptions,
    ) -> crate::runtime::ConfigReloadReport {
        let mut next = self.runtime_snapshot();
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

    pub(crate) async fn authorize(
        &self,
        security: &SecurityContext,
        message_type: &str,
        operation: &str,
    ) -> Result<(), Status> {
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

        // GAP 40: Per-tenant sliding-window rate limiting
        if !safe.tenant_id.is_empty() {
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
        let policies = self
            .abac_policies
            .read()
            .map(|policies| policies.clone())
            .unwrap_or_default();
        evaluate_abac(
            &policies,
            security,
            message_type,
            operation,
            self.abac_default_allow,
        )
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

        let mut conn = redis
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| Status::internal(format!("rate limit redis error: {}", e)))?;

        let count: u64 = redis::cmd("INCR")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);

        if count == 1 {
            let _: () = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(window_secs)
                .query_async(&mut conn)
                .await
                .unwrap_or(());
        }

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
        Err(Status::failed_precondition(format!(
            "{}: backend '{backend}' does not support operation '{operation}'",
            crate::backend::UNSUPPORTED_OPERATION_CODE
        )))
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
        Err(Status::failed_precondition(format!(
            "{}: backend '{backend}' does not support operation '{operation}'",
            crate::backend::UNSUPPORTED_OPERATION_CODE
        )))
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
    if !runtime.postgres_configured() {
        return Err(
            "PostgreSQL startup health gate failed: UDB_PG_DSN/DATABASE_URL is required".into(),
        );
    }
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
    let metrics = Arc::new(PrometheusMetrics::new()?);
    let metrics_socket: SocketAddr = runtime_config.service.metrics_addr.parse()?;
    tokio::spawn(metrics_http_server(
        metrics.clone(),
        runtime.clone(),
        metrics_socket,
        runtime_config.service.metrics_allowed_cidr.clone(),
    ));
    tokio::spawn(cdc_metrics_poller(runtime.clone(), metrics.clone()));

    let lifecycle_state = Arc::new(RwLock::new(FsmState::Initialising));
    match run_startup_lifecycle(&runtime, &manifest, &schemas, false, false).await {
        Ok(report) => {
            tracing::info!(
                run_id = report.run_id,
                applied_sql_artifacts = report.applied_sql_artifacts,
                verified_tables = report.verified_tables,
                "UDB startup lifecycle completed"
            );
            if let Ok(mut state) = lifecycle_state.write() {
                *state = FsmState::Completed;
            }
        }
        Err(err) => {
            if let Ok(mut state) = lifecycle_state.write() {
                *state = FsmState::Error;
            }
            return Err(err.into());
        }
    }
    runtime.mark_indeterminate_sagas().await;
    if crate::runtime::saga::SagaRecoveryWorker::is_enabled_with_settings(&runtime_config.saga) {
        // NW1-3c: route through the SystemStores registry instead of
        // the bare PG pool. Slim deployments without a canonical
        // store skip the worker entirely.
        if let Some(store) = runtime.default_system_stores() {
            let worker = crate::runtime::saga::SagaRecoveryWorker::with_settings(
                store,
                &runtime_config.saga,
            )
            .with_compensators(runtime.saga_compensator_registry());
            tokio::spawn(async move { worker.run_forever().await });
            tracing::info!("saga recovery worker started");
        } else {
            tracing::warn!("saga recovery worker disabled: no canonical store is registered");
        }
    }
    let abac_policies = Arc::new(RwLock::new(runtime.load_abac_policies().await));

    // GAP 36: Background ABAC policy cache refresh — keeps the in-memory policy
    // set current without requiring a service restart.  When the DB returns an
    // empty set we retain the stale (non-empty) cache to avoid a silent deny-all.
    {
        let abac_refresh_secs = runtime_config.service.abac_refresh_secs;
        let abac_policies_bg = abac_policies.clone();
        let runtime_bg = runtime.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(abac_refresh_secs));
            loop {
                interval.tick().await;
                let fresh = runtime_bg.load_abac_policies().await;
                if fresh.is_empty() {
                    tracing::warn!(
                        "ABAC policy refresh returned empty set — retaining stale policies \
                         to avoid accidental deny-all"
                    );
                    continue;
                }
                if let Ok(mut guard) = abac_policies_bg.write() {
                    *guard = fresh;
                }
            }
        });
    }
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
        metrics,
        cdc_engine,
        abac_default_allow,
    );
    spawn_config_reload_watcher(service.clone());

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
        let engine = Arc::new(ProjectionEngine::new(
            pg_pool.clone(),
            store.clone(),
            config,
        ));
        service.projection_engine = Some(Arc::clone(&engine));

        if ProjectionWorker::is_enabled() {
            let metrics: Arc<dyn MetricsRecorder> = service.metrics.clone();
            let worker =
                ProjectionWorker::new(store.clone(), service.runtime_snapshot().clone(), metrics);
            tokio::spawn(async move { worker.run_forever().await });
            tracing::info!("projection materialization worker started");
        }
        if ReconciliationWorker::is_enabled() {
            let metrics: Arc<dyn MetricsRecorder> = service.metrics.clone();
            let active_catalog = service.catalog.active();
            let worker = ReconciliationWorker::new(
                pg_pool.clone(),
                store.clone(),
                metrics,
                active_catalog.manifest.clone(),
                active_catalog.metadata.project_id.clone(),
            );
            tokio::spawn(async move { worker.run_forever().await });
            tracing::info!("projection reconciliation worker started");
        }
    } else {
        tracing::warn!(
            "projection engine disabled: PostgreSQL pool and/or canonical store not available"
        );
    }
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<DataBrokerServer<DataBrokerService>>()
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

    let layer = tower::ServiceBuilder::new()
        .timeout(grpc_timeout)
        .concurrency_limit(grpc_max_concurrent)
        .into_inner();

    let mut server = tonic::transport::Server::builder().layer(layer);
    if let Some(tls) = tls_config_from_settings(&runtime_config.service.tls)? {
        server = server.tls_config(tls)?;
    }
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(UDB_FILE_DESCRIPTOR_SET)
        .build_v1()?;

    server
        .add_service(reflection_service)
        .add_service(health_service)
        .add_service(DataBrokerServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    Ok(())
}

#[cfg(feature = "kafka")]
async fn start_cdc_engine(
    runtime: &DataBrokerRuntime,
    metrics: Arc<PrometheusMetrics>,
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

    let metrics: Arc<dyn MetricsRecorder> = metrics;
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
        Ok(engine) => {
            let engine = Arc::new(engine);
            tokio::spawn({
                let engine = engine.clone();
                async move {
                    engine.run_advisory_lock_loop().await;
                }
            });
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

        let text = metrics.gather_text(&format!(
            "{}{}",
            runtime.cache_metrics_text(),
            runtime.encryption_metrics_text() + &runtime.pg_pool_metrics_text()
        ));
        tokio::spawn(async move {
            // GAP 18: Read the incoming request with a 5-second deadline.
            // Without this, port scanners get a 200 OK with full metrics data
            // before sending a single byte, and slow-loris clients hold
            // connections open indefinitely.
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                let mut buf = [0u8; 256];
                let _ = socket.read(&mut buf).await;
            })
            .await;

            // GAP 18: charset=utf-8 is required by the Prometheus text format spec.
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                text.len(),
                text
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

async fn cdc_metrics_poller(runtime: DataBrokerRuntime, metrics: Arc<PrometheusMetrics>) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    loop {
        interval.tick().await;
        if let Ok((lag, depth)) = runtime.cdc_outbox_metrics().await {
            metrics.set_cdc_lag_seconds(lag);
            metrics.set_cdc_outbox_depth(depth);
        }
    }
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

#[tonic::async_trait]

impl DataBroker for DataBrokerService {
    type BatchSelectStream = ResponseStream<RecordSet>;
    type BatchUpsertStream = ResponseStream<MutationResponse>;
    type VectorBatchUpsertStream = ResponseStream<MutationResponse>;
    type GetObjectStream = ResponseStream<Chunk>;
    type BeginTxStream = ResponseStream<TxStatus>;
    type PublishCDCStream = ResponseStream<CdcEnvelope>;

    async fn select(&self, request: Request<SelectRequest>) -> Result<Response<RecordSet>, Status> {
        self.select_inner(request).await
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
}

// Phase G: service.rs split — inherent RPC handler bodies + tests.
mod handlers_admin;
mod handlers_catalog;
mod handlers_data;
mod handlers_meta;
mod handlers_object;
mod handlers_policy;
mod handlers_resource;
mod handlers_tx;
mod handlers_vector;
#[cfg(test)]
mod tests;
