use std::path::Path;
use std::sync::{Arc, RwLock};

use tonic::Request;
use udb::proto::data_broker_server::DataBroker;
use udb::proto::{
    GenericDispatchRequest, RequestContext, SelectRequest, UpsertRequest, UrlRequest,
    VectorSearchRequest,
};
use udb::runtime::config::{
    BackendInstance, BackendInstanceConfig, BackendInstanceRole, DbConfig, UdbConfig,
    merge_udb_config,
};
use udb::{
    CatalogManifest, DataBrokerRuntime, DataBrokerService, FsmState, PrometheusMetrics,
    parse_directory_report,
};

fn acme_manifest() -> CatalogManifest {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/arbitrary_project/proto");
    let report = parse_directory_report(&root, &udb::ParserConfig::default())
        .expect("arbitrary project proto should parse");
    CatalogManifest::from_schemas(&report.schemas).expect("manifest should build")
}

fn open_service(manifest: CatalogManifest) -> DataBrokerService {
    unsafe {
        std::env::set_var("UDB_MTLS_REQUIRED", "false");
        std::env::set_var("UDB_ALLOW_HEADER_SCOPES", "1");
    }
    DataBrokerService::with_runtime_and_state(
        manifest,
        DataBrokerRuntime::planning_only(),
        Arc::new(RwLock::new(FsmState::Completed)),
        Arc::new(RwLock::new(Vec::new())),
        Arc::new(PrometheusMetrics::new().expect("metrics")),
        None,
        true,
    )
}

fn authed<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", "phase10-tenant".parse().unwrap());
    metadata.insert("x-purpose", "phase10-test".parse().unwrap());
    metadata.insert("x-correlation-id", "phase10-correlation".parse().unwrap());
    metadata.insert("x-service-identity", "phase10.test".parse().unwrap());
    metadata.insert(
        "x-scopes",
        "udb:admin,udb:read,udb:write,udb:dispatch,udb:vector:read,udb:object:presign"
            .parse()
            .unwrap(),
    );
    request
}

fn request_context() -> RequestContext {
    RequestContext {
        tenant_id: "phase10-tenant".to_string(),
        purpose: "phase10-test".to_string(),
        correlation_id: "phase10-correlation".to_string(),
        service_identity: "phase10.test".to_string(),
        scopes: vec![
            "udb:admin".to_string(),
            "udb:read".to_string(),
            "udb:write".to_string(),
            "udb:dispatch".to_string(),
            "udb:vector:read".to_string(),
            "udb:object:presign".to_string(),
        ],
        ..RequestContext::default()
    }
}

#[test]
fn config_merge_unit_test_covers_file_overlay_precedence() {
    let defaults = UdbConfig {
        app_name: "default".to_string(),
        default_limit: 50,
        max_limit: 1000,
        primary: DbConfig {
            direct_dsn: "postgres://default/udb".to_string(),
            ..DbConfig::default()
        },
        ..UdbConfig::default()
    };
    let file = UdbConfig {
        app_name: "file".to_string(),
        default_limit: 250,
        primary: DbConfig {
            direct_dsn: "postgres://file/udb".to_string(),
            ..DbConfig::default()
        },
        backend_instances: BackendInstanceConfig {
            instances: vec![BackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                role: BackendInstanceRole::ReadWrite,
                dsn: Some("postgres://file/udb".to_string()),
                dsn_env: None,
                ..BackendInstance::default()
            }],
        },
        ..UdbConfig::default()
    };

    let merged = merge_udb_config(defaults, file);

    assert_eq!(merged.app_name, "file");
    assert_eq!(merged.default_limit, 250);
    assert_eq!(merged.max_limit, 1000);
    assert_eq!(merged.primary.direct_dsn, "postgres://file/udb");
    assert_eq!(
        merged.backend_instances.instances[0].routing_key(),
        "postgres:primary"
    );
}

#[tokio::test]
async fn arbitrary_project_parse_manifest_and_broker_contracts() {
    let manifest = acme_manifest();
    assert!(manifest.table("billing", "invoices").is_some());
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.backend == "qdrant" && store.resource_name == "acme_products")
    );
    assert!(
        manifest
            .stores
            .iter()
            .any(|store| store.backend == "s3" && store.resource_name == "acme-billing-documents")
    );

    let service = open_service(manifest);

    let select = service
        .select(authed(SelectRequest {
            context: Some(request_context()),
            message_type: "Invoice".to_string(),
            limit: 1,
            ..SelectRequest::default()
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(
            select.code(),
            tonic::Code::FailedPrecondition
                | tonic::Code::InvalidArgument
                | tonic::Code::PermissionDenied
        ),
        "select failed with unexpected status: {}",
        select.message()
    );

    let upsert = service
        .upsert(authed(UpsertRequest {
            context: Some(request_context()),
            message_type: "Invoice".to_string(),
            record_json: br#"{"invoice_id":"inv-1","org_id":"org-1"}"#.to_vec(),
            idempotency_key: "phase10-upsert".to_string(),
            ..UpsertRequest::default()
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(
            upsert.code(),
            tonic::Code::FailedPrecondition | tonic::Code::Unavailable
        ),
        "upsert failed with unexpected status: {}",
        upsert.message()
    );

    let search = service
        .vector_search(authed(VectorSearchRequest {
            context: Some(request_context()),
            collection: "acme_products".to_string(),
            vector: vec![0.1; 768],
            limit: 1,
            with_payload: true,
            ..VectorSearchRequest::default()
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(
            search.code(),
            tonic::Code::FailedPrecondition | tonic::Code::Unavailable
        ),
        "search failed with unexpected status: {}",
        search.message()
    );
    assert!(search.message().contains("Qdrant"));

    let object = service
        .generate_presigned_url(authed(UrlRequest {
            context: Some(request_context()),
            bucket: "acme-billing-documents".to_string(),
            object_key: "invoices/inv-1.pdf".to_string(),
            method: "GET".to_string(),
            ttl_seconds: 60,
            ..UrlRequest::default()
        }))
        .await
        .unwrap_err();
    assert!(
        matches!(
            object.code(),
            tonic::Code::FailedPrecondition
                | tonic::Code::InvalidArgument
                | tonic::Code::Unavailable
        ),
        "object failed with unexpected status: {}",
        object.message()
    );
}

#[tokio::test]
async fn backend_executor_contracts_cover_supported_backends_when_unconfigured() {
    let runtime = DataBrokerRuntime::planning_only();
    let cases = [
        ("postgres", "ping", "PostgreSQL"),
        ("redis", "ping", "redis"),
        ("mongodb", "ping", "mongodb"),
        ("neo4j", "ping", "neo4j"),
        ("clickhouse", "ping", "clickhouse"),
        ("qdrant", "search", "qdrant"),
        ("minio", "put_object", "S3"),
    ];

    for (backend, operation, expected) in cases {
        let err = match operation {
            "ping" => runtime.ping_backend(backend).await.unwrap_err(),
            "search" => runtime
                .search_backend(
                    backend,
                    r#"{"collection":"phase10","vector":[0.1,0.2],"limit":1}"#,
                )
                .await
                .unwrap_err(),
            "put_object" => runtime
                .put_object_backend(
                    backend,
                    r#"{"bucket":"phase10","key":"probe.txt","content_text":"hello"}"#,
                    Vec::new(),
                )
                .await
                .unwrap_err(),
            _ => unreachable!("unexpected operation"),
        };
        assert!(
            matches!(
                err.code(),
                tonic::Code::FailedPrecondition | tonic::Code::Unavailable
            ),
            "{backend}:{operation}: {}",
            err.message()
        );
        assert!(
            err.message()
                .to_ascii_lowercase()
                .contains(&expected.to_ascii_lowercase()),
            "{backend}:{operation}: {}",
            err.message()
        );
    }
}

#[tokio::test]
async fn multi_instance_postgres_config_is_validated_before_connection() {
    let config = BackendInstanceConfig {
        instances: vec![
            BackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                role: BackendInstanceRole::Write,
                dsn: Some("postgres://udb:udb@localhost:55432/udb".to_string()),
                dsn_env: None,
                labels: [("region".to_string(), "local".to_string())]
                    .into_iter()
                    .collect(),
                ..BackendInstance::default()
            },
            BackendInstance {
                name: "replica_a".to_string(),
                backend: "postgres".to_string(),
                role: BackendInstanceRole::Read,
                dsn: Some("postgres://udb:udb@localhost:55432/udb".to_string()),
                dsn_env: None,
                read_weight: 5,
                write_weight: 0,
                labels: [("region".to_string(), "local".to_string())]
                    .into_iter()
                    .collect(),
                ..BackendInstance::default()
            },
        ],
    };

    assert!(config.validate().is_empty());
    assert_eq!(
        config
            .instances
            .iter()
            .map(BackendInstance::routing_key)
            .collect::<Vec<_>>(),
        vec!["postgres:primary", "postgres:replica_a"]
    );
}

#[tokio::test]
async fn cross_backend_saga_dispatch_dry_run_returns_compilable_contract() {
    let service = open_service(CatalogManifest::default());
    let response = service
        .generic_dispatch(authed(GenericDispatchRequest {
            backend: "qdrant".to_string(),
            operation: "mutate".to_string(),
            spec_json: r#"{"operation":"upsert","collection":"phase10","points":[]}"#.to_string(),
            idempotency_key: "phase10-saga-step".to_string(),
            dry_run: true,
            ..GenericDispatchRequest::default()
        }))
        .await
        .expect("dry-run dispatch should not require a live backend")
        .into_inner();

    let json: serde_json::Value =
        serde_json::from_str(&response.result_json).expect("dry-run JSON");
    assert_eq!(response.backend, "qdrant");
    assert_eq!(response.operation, "mutate");
    assert_eq!(json["dry_run"], true);
    assert_eq!(json["idempotency_key"], "phase10-saga-step");
}
