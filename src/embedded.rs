use std::sync::{Arc, RwLock};

use tonic::{Request, Status};

use crate::RequestContext;
use crate::engine::FsmState;
use crate::generation::CatalogManifest;
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{
    DeleteRequest, GenericDispatchRequest, GenericDispatchResponse, HealthReportRequest,
    HealthReportResponse, MutationResponse, RecordSet, SelectRequest, UpsertRequest,
};
use crate::runtime::config::UdbConfig;
use crate::runtime::core::DataBrokerRuntime;
use crate::runtime::executor_utils::invalid_argument_fields;
use crate::runtime::metrics::PrometheusMetrics;
use crate::runtime::service::DataBrokerService;

/// In-process UDB runtime facade for tests, CLIs, batch jobs, and local workers.
///
/// The embedded facade drives the same `DataBrokerService` authorization,
/// catalog, routing, and connection-manager path as the network server, but it
/// does not bind a socket or require generated client code.
#[derive(Clone)]
pub struct EmbeddedRuntime {
    service: DataBrokerService,
}

impl EmbeddedRuntime {
    pub async fn from_config(manifest: CatalogManifest, config: UdbConfig) -> Result<Self, String> {
        let runtime = DataBrokerRuntime::try_from_config(config).await?;
        Ok(Self::from_runtime(manifest, runtime).await)
    }

    pub async fn from_runtime(manifest: CatalogManifest, runtime: DataBrokerRuntime) -> Self {
        let abac_default_allow = runtime.config().service.abac_default_allow;
        let metrics = Arc::new(PrometheusMetrics::new().expect("create prometheus metrics"));
        let service = DataBrokerService::with_runtime_and_state(
            manifest,
            runtime,
            Arc::new(RwLock::new(FsmState::Completed)),
            metrics,
            None,
            abac_default_allow,
        );
        Self { service }
    }

    pub fn planning_only(manifest: CatalogManifest) -> Self {
        let service = DataBrokerService::with_runtime_and_state(
            manifest,
            DataBrokerRuntime::planning_only(),
            Arc::new(RwLock::new(FsmState::Completed)),
            Arc::new(PrometheusMetrics::new().expect("create prometheus metrics")),
            None,
            true,
        );
        Self { service }
    }

    pub async fn health_report(
        &self,
        request: HealthReportRequest,
        context: RequestContext,
    ) -> Result<HealthReportResponse, Status> {
        self.service
            .get_health_report(request_with_context(request, &context)?)
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn select(
        &self,
        request: SelectRequest,
        context: RequestContext,
    ) -> Result<RecordSet, Status> {
        self.service
            .select(request_with_context(request, &context)?)
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn upsert(
        &self,
        request: UpsertRequest,
        context: RequestContext,
    ) -> Result<MutationResponse, Status> {
        self.service
            .upsert(request_with_context(request, &context)?)
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn delete(
        &self,
        request: DeleteRequest,
        context: RequestContext,
    ) -> Result<MutationResponse, Status> {
        self.service
            .delete(request_with_context(request, &context)?)
            .await
            .map(tonic::Response::into_inner)
    }

    pub async fn generic_dispatch(
        &self,
        request: GenericDispatchRequest,
        context: RequestContext,
    ) -> Result<GenericDispatchResponse, Status> {
        self.service
            .generic_dispatch(request_with_context(request, &context)?)
            .await
            .map(tonic::Response::into_inner)
    }

    pub fn service(&self) -> &DataBrokerService {
        &self.service
    }
}

fn request_with_context<T>(message: T, context: &RequestContext) -> Result<Request<T>, Status> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    insert_ascii(metadata, "x-tenant-id", &context.tenant_id)?;
    insert_ascii(metadata, "x-purpose", &context.purpose)?;
    insert_ascii(metadata, "x-correlation-id", &context.correlation_id)?;
    insert_ascii(metadata, "x-user-id", &context.user_id)?;
    insert_ascii(metadata, "x-udb-project-id", &context.project_id)?;
    insert_ascii(metadata, "x-udb-consistency", &context.consistency)?;
    insert_ascii(
        metadata,
        "x-udb-client-catalog-version",
        &context.client_catalog_version,
    )?;
    insert_ascii(metadata, "x-udb-target-backend", &context.target_backend)?;
    insert_ascii(metadata, "x-udb-target-instance", &context.target_instance)?;
    insert_ascii(metadata, "x-udb-routing-policy", &context.routing_policy)?;
    // 03.1.1.1: forward the read fence on the embedded in-process path. The
    // served path reads it exclusively from this header (security.rs), so
    // without this an embedded caller silently loses any fence it set.
    // `insert_ascii` early-returns on empty input → zero cost when unset.
    insert_ascii(metadata, "x-udb-read-fence", &context.read_fence_json)?;
    if context.max_replica_lag_ms > 0 {
        insert_ascii(
            metadata,
            "x-udb-max-replica-lag-ms",
            &context.max_replica_lag_ms.to_string(),
        )?;
    }
    if context.primary_read {
        insert_ascii(metadata, "x-udb-primary-read", "true")?;
    }
    if context.eventual_consistency_allowed {
        insert_ascii(metadata, "x-udb-eventual-consistency-allowed", "true")?;
    }
    if !context.scopes.is_empty() {
        insert_ascii(metadata, "x-scopes", &context.scopes.join(","))?;
    }
    Ok(request)
}

fn insert_ascii(
    metadata: &mut tonic::metadata::MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Ok(());
    }
    let value = tonic::metadata::MetadataValue::try_from(value).map_err(|_| {
        invalid_argument_fields(
            format!("{key} is not valid ASCII metadata"),
            [(key, "must be valid ASCII gRPC metadata")],
        )
    })?;
    metadata.insert(key, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[tokio::test]
    async fn embedded_runtime_serves_health_without_binding_port() {
        let mut config = UdbConfig::default();
        config.service.abac_default_allow = true;
        config.security.mtls_required = false;
        config.security.service_identity_required = false;
        config.security.allow_header_scopes = true;
        config.security.tls_required = false;
        let runtime = EmbeddedRuntime::from_runtime(
            CatalogManifest::default(),
            DataBrokerRuntime::from_config(config).await,
        )
        .await;
        let response = runtime
            .health_report(
                HealthReportRequest::default(),
                RequestContext {
                    tenant_id: "tenant-a".to_string(),
                    purpose: "test".to_string(),
                    scopes: vec!["udb:admin".to_string()],
                    ..RequestContext::default()
                },
            )
            .await
            .expect("health report should be served in-process");

        assert!(!response.errors.is_empty());
    }

    #[test]
    fn embedded_context_rejects_invalid_metadata() {
        let err = request_with_context(
            HealthReportRequest::default(),
            &RequestContext {
                tenant_id: "bad\nvalue".to_string(),
                purpose: "test".to_string(),
                ..RequestContext::default()
            },
        )
        .expect_err("newlines are invalid gRPC metadata");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "x-tenant-id is not valid ASCII metadata");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "x-tenant-id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be valid ASCII gRPC metadata"
        );
    }

    #[test]
    fn embedded_context_forwards_read_fence_metadata() {
        let request = request_with_context(
            HealthReportRequest::default(),
            &RequestContext {
                read_fence_json: r#"{"source":"test","lsn":42}"#.to_string(),
                ..RequestContext::default()
            },
        )
        .expect("read fence metadata is valid ASCII");

        assert_eq!(
            request
                .metadata()
                .get("x-udb-read-fence")
                .and_then(|value| value.to_str().ok()),
            Some(r#"{"source":"test","lsn":42}"#)
        );
    }
}
