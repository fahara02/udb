// Per-backend executor modules (relocated from runtime/*_executor.rs in Phase D).
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub(crate) mod handle;
#[cfg(feature = "http-client")]
pub(crate) mod http; // §8.2: shared reqwest client builder for HTTP backends
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "neo4j")]
pub mod neo4j;
pub(crate) mod postgres;
// P2P: first-class MySQL + SQLite generic-dispatch executors.
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "qdrant")]
pub(crate) mod qdrant;
#[cfg(feature = "sqlite")]
pub mod sqlite;
// C9: Elasticsearch executor — HTTP/JSON to ES Query DSL.
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
// C9: Memcached executor — sync `memcache` crate wrapped in
// spawn_blocking. Real binary-protocol driver, not a stub.
#[cfg(feature = "memcached")]
pub mod memcached;
// C9: SQL Server executor — TDS protocol via tiberius driver.
#[cfg(feature = "mssql")]
pub mod mssql;
// C9: Weaviate executor — REST + GraphQL via reqwest.
#[cfg(feature = "weaviate")]
pub mod weaviate;
// C9: Pinecone executor — REST via reqwest.
#[cfg(feature = "pinecone")]
pub mod pinecone;
// C9: Cassandra / ScyllaDB executor via the official `scylla` driver.
#[cfg(feature = "cassandra")]
pub mod cassandra;
// C9: Azure Blob Storage via the official `azure_storage_blobs` SDK.
#[cfg(feature = "azureblob")]
pub mod azureblob;
// C9: Google Cloud Storage via the canonical `google-cloud-storage` crate.
#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(all(test, feature = "s3"))]
pub(crate) mod object_stream_live_tests;
#[cfg(feature = "redis")]
pub(crate) mod redis;
#[cfg(feature = "s3")]
pub(crate) mod s3; // Phase E: enum-dispatch handle (DispatchExecutor) // A.6 live MinIO streaming round-trip (env-gated, #[ignore])
pub(crate) use handle::DispatchExecutor;

// `DataBrokerRuntime` import was needed by the deleted `DefaultBackendExecutor`
// forwarding adapter; the dispatch path now uses `DispatchExecutor` directly.
use bytes::Bytes;
use serde::Serialize;
use std::collections::BTreeMap;
use std::pin::Pin;
use tokio_stream::Stream;

pub type ExecutorByteStream =
    Pin<Box<dyn Stream<Item = Result<Bytes, tonic::Status>> + Send + 'static>>;

#[allow(async_fn_in_trait)]
pub trait BackendHealth: Send + Sync {
    async fn ping(&self) -> Result<(), String>;
}

// Data-operation executors return `tonic::Status` so precise gRPC codes
// (InvalidArgument vs FailedPrecondition vs Internal) survive dispatch. Only
// `BackendHealth::ping` keeps a `String` error (a ping failure is always mapped
// to `Unavailable`, so there is no fidelity to preserve).
#[allow(async_fn_in_trait)]
pub trait QueryExecutor: Send + Sync {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status>;
    async fn query_stream(&self, request_json: &str) -> Result<ExecutorByteStream, tonic::Status> {
        let response = self.query(request_json).await?;
        Ok(Box::pin(tokio_stream::iter([Ok(Bytes::from(response))])))
    }
}

#[allow(async_fn_in_trait)]
pub trait MutationExecutor: Send + Sync {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status>;
}

#[allow(async_fn_in_trait)]
pub trait SearchExecutor: Send + Sync {
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status>;
}

#[allow(async_fn_in_trait)]
pub trait ObjectExecutor: Send + Sync {
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status>;
    async fn get_object_stream(
        &self,
        request_json: &str,
    ) -> Result<ExecutorByteStream, tonic::Status> {
        let bytes = self.get_object(request_json).await?;
        Ok(Box::pin(tokio_stream::iter([Ok(Bytes::from(bytes))])))
    }
    async fn put_object(&self, request_json: &str, bytes: Vec<u8>)
    -> Result<String, tonic::Status>;
    /// Streaming upload (A.6). The default collects the byte stream into one
    /// buffer and delegates to [`ObjectExecutor::put_object`], so backends that
    /// cannot stream keep working unchanged. Real object stores (S3/MinIO, …)
    /// override this to forward bytes to the provider without buffering the
    /// whole object in UDB.
    async fn put_object_stream(
        &self,
        request_json: &str,
        stream: ExecutorByteStream,
    ) -> Result<String, tonic::Status> {
        use tokio_stream::StreamExt as _;
        let mut stream = stream;
        let mut body: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            body.extend_from_slice(&chunk?);
        }
        self.put_object(request_json, body).await
    }
    /// Remove an object by `{"bucket","object_key"|"key"}`. Only real object
    /// stores (S3/MinIO, GCS, Azure Blob) override this; other backends report a
    /// typed capability refusal so callers never silently no-op a delete.
    async fn delete_object(&self, _request_json: &str) -> Result<(), tonic::Status> {
        Err(crate::runtime::executor_utils::capability_status(
            "object",
            "delete_object",
            "object_delete",
            "delete_object is not supported by this backend",
        ))
    }
}

#[allow(async_fn_in_trait)]
pub trait ResourceAdminExecutor: Send + Sync {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status>;
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status>;
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status>;
}

#[allow(async_fn_in_trait)]
pub trait BackendExecutor:
    BackendHealth
    + QueryExecutor
    + MutationExecutor
    + SearchExecutor
    + ObjectExecutor
    + ResourceAdminExecutor
    + Send
    + Sync
{
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status>;
    async fn probe(&self) -> Result<BackendProbe, tonic::Status>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackendProbe {
    pub backend: String,
    pub instance: Option<String>,
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackendExecutorRegistration {
    pub backend: String,
    pub instance: Option<String>,
    pub role: String,
    pub connected: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct BackendExecutorRegistry {
    entries: BTreeMap<(String, String), BackendExecutorRegistration>,
}

impl BackendExecutorRegistry {
    pub fn register(&mut self, registration: BackendExecutorRegistration) {
        let instance = registration
            .instance
            .clone()
            .unwrap_or_else(|| "default".to_string());
        self.entries
            .insert((registration.backend.clone(), instance), registration);
    }

    pub fn get(
        &self,
        backend: &str,
        instance: Option<&str>,
    ) -> Option<&BackendExecutorRegistration> {
        let instance = instance.unwrap_or("default");
        self.entries
            .get(&(backend.to_string(), instance.to_string()))
            .or_else(|| {
                if instance == "default" {
                    self.entries
                        .iter()
                        .find(|((candidate_backend, _), entry)| {
                            candidate_backend == backend && entry.connected
                        })
                        .map(|(_, entry)| entry)
                } else {
                    None
                }
            })
    }

    pub fn all(&self) -> impl Iterator<Item = &BackendExecutorRegistration> {
        self.entries.values()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// `DefaultBackendExecutor` was deleted in U2 step 6. The generic-dispatch
// gRPC handler builds a typed `DispatchExecutor` directly via
// `DataBrokerRuntime::resolve_dispatch_executor` (which is plugin-keyed) and
// uses `backend_executor()` only for the registry/connectivity/circuit-breaker
// metadata resolution. The old forwarding adapter is no longer needed.

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod capability_tests {
    use super::compensation_for_step;
    use crate::planning::backend::BackendKind;
    use serde_json::json;

    fn cap(backend: &str) -> Option<crate::planning::backend::BackendCapability> {
        BackendKind::from_store_kind("", backend).map(|k| k.capabilities())
    }

    #[test]
    fn postgres_supports_query_and_mutation() {
        let c = cap("postgres").unwrap();
        assert!(c.supports_sql_ddl);
        assert!(c.supports_transactions);
        assert!(c.is_migration_ledger_capable);
        assert!(c.supports_resource_lifecycle);
        assert!(!c.is_object_store);
        assert!(!c.supports_vector_search);
    }

    #[test]
    fn redis_does_not_support_query_or_resource_lifecycle() {
        let c = cap("redis").unwrap();
        assert!(!c.supports_sql_ddl);
        assert!(!c.supports_resource_lifecycle);
        assert!(!c.is_object_store);
        assert!(!c.is_migration_ledger_capable);
        assert!(c.supports_ttl);
    }

    #[test]
    fn qdrant_supports_vector_search_not_query() {
        let c = cap("qdrant").unwrap();
        assert!(c.supports_vector_search);
        assert!(c.supports_hybrid_search);
        assert!(!c.supports_sql_ddl);
        assert!(!c.is_object_store);
        assert!(!c.is_migration_ledger_capable);
        assert!(c.supports_resource_lifecycle); // collection CRUD
    }

    #[test]
    fn minio_is_object_store_not_query() {
        let c = cap("minio").unwrap();
        assert!(c.is_object_store);
        assert!(!c.supports_sql_ddl);
        assert!(!c.supports_vector_search);
        assert!(!c.is_migration_ledger_capable);
        // MinIO supports bucket lifecycle via EnsureResource/DropResource/ListResources
        // admin RPCs — supports_resource_lifecycle is correctly true.
        assert!(c.supports_resource_lifecycle);
    }

    #[test]
    fn mongodb_supports_mutation_not_vector_search() {
        let c = cap("mongodb").unwrap();
        assert!(!c.supports_sql_ddl);
        assert!(c.supports_transactions);
        assert!(!c.supports_vector_search);
        assert!(!c.is_object_store);
        assert!(!c.is_migration_ledger_capable);
    }

    #[test]
    fn mongodb_supports_resource_lifecycle() {
        let c = cap("mongodb").unwrap();
        assert!(c.supports_resource_lifecycle);
    }

    #[test]
    fn clickhouse_supports_resource_lifecycle() {
        let c = cap("clickhouse").unwrap();
        assert!(c.supports_resource_lifecycle);
    }

    #[test]
    fn neo4j_supports_transactions_not_sql_ddl() {
        let c = cap("neo4j").unwrap();
        assert!(!c.supports_sql_ddl);
        assert!(c.supports_transactions);
        assert!(!c.is_object_store);
        assert!(!c.supports_vector_search);
        assert!(c.supports_resource_lifecycle);
    }

    #[test]
    fn clickhouse_is_analytics_not_transactional() {
        let c = cap("clickhouse").unwrap();
        assert!(c.supports_sql_ddl);
        assert!(!c.supports_transactions);
        assert!(c.supports_ttl);
        assert!(!c.is_migration_ledger_capable);
        assert!(!c.is_object_store);
    }

    #[test]
    fn capability_rejection_dispatch_map() {
        // Verify the dispatch operation table used in check_generic_dispatch_operation.
        // ping → any backend
        for backend in &[
            "postgres",
            "redis",
            "qdrant",
            "minio",
            "mongodb",
            "neo4j",
            "clickhouse",
        ] {
            assert!(
                BackendKind::from_store_kind("", backend).is_some(),
                "backend '{backend}' not recognised"
            );
        }

        // object ops → only object stores
        let minio_cap = cap("minio").unwrap();
        assert!(minio_cap.is_object_store, "minio must be object store");
        let redis_cap = cap("redis").unwrap();
        assert!(!redis_cap.is_object_store, "redis must NOT be object store");

        // search → only vector/hybrid backends
        let qdrant_cap = cap("qdrant").unwrap();
        assert!(qdrant_cap.supports_vector_search || qdrant_cap.supports_hybrid_search);
        let postgres_cap = cap("postgres").unwrap();
        assert!(
            !postgres_cap.supports_vector_search && !postgres_cap.supports_hybrid_search,
            "postgres must NOT support vector/hybrid search"
        );

        // resource lifecycle → postgres, qdrant, minio, mongodb, neo4j; NOT redis
        assert!(cap("postgres").unwrap().supports_resource_lifecycle);
        assert!(cap("qdrant").unwrap().supports_resource_lifecycle);
        assert!(
            !cap("redis").unwrap().supports_resource_lifecycle,
            "redis must NOT support resource lifecycle (no ensure/drop keyspace)"
        );
    }

    #[test]
    fn compensation_descriptors_cover_cross_backend_writes() {
        assert_eq!(
            compensation_for_step("qdrant", &json!({"collection":"c","ids":["p1"]}))["operation"],
            "delete_points"
        );
        assert_eq!(
            compensation_for_step("minio", &json!({"bucket":"b","key":"k"}))["operation"],
            "delete_object"
        );
        assert_eq!(
            compensation_for_step("mongodb", &json!({"operation":"insert","collection":"c"}))["operation"],
            "delete_one"
        );
        assert_eq!(
            compensation_for_step(
                "neo4j",
                &json!({"operation":"create_node","label":"L","id":"1"})
            )["operation"],
            "delete_node"
        );
        assert_eq!(
            compensation_for_step("clickhouse", &json!({"table":"events"}))["operation"],
            "manual_review"
        );
    }
}

// `SearchExecutor` / `ObjectExecutor` / `BackendExecutor` impls for the deleted
// `DefaultBackendExecutor` are gone with the struct. The same trait surface is
// satisfied by `DispatchExecutor` (via the `on_variant!` macro in `handle.rs`),
// which the gRPC generic-dispatch handler uses directly.

/// Compensation descriptor builder for cross-backend transaction rollback.
/// Kept after the `DefaultBackendExecutor` removal because the descriptor
/// shapes are a documented contract (covered by `compensation_descriptors_*`
/// tests) and the planner/saga layer can rely on it.
#[cfg(test)]
pub(crate) fn compensation_for_step(backend: &str, step: &serde_json::Value) -> serde_json::Value {
    let operation = step
        .get("operation")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match backend {
        "qdrant" => serde_json::json!({
            "backend": "qdrant",
            "operation": "delete_points",
            "collection": step.get("collection").cloned().unwrap_or_default(),
            "point_ids": step.get("point_ids").or_else(|| step.get("ids")).cloned().unwrap_or_default(),
            "status": "pending"
        }),
        "s3" | "minio" => serde_json::json!({
            "backend": backend,
            "operation": "delete_object",
            "bucket": step.get("bucket").cloned().unwrap_or_default(),
            "object_key": step.get("object_key").or_else(|| step.get("key")).cloned().unwrap_or_default(),
            "status": "pending"
        }),
        "mongodb" | "mongo" if matches!(operation, "insert" | "insert_one") => serde_json::json!({
            "backend": "mongodb",
            "operation": "delete_one",
            "collection": step.get("collection").cloned().unwrap_or_default(),
            "filter": step.get("compensation_filter").or_else(|| step.get("filter")).cloned().unwrap_or_default(),
            "status": "pending"
        }),
        "neo4j" if operation == "create_node" => serde_json::json!({
            "backend": "neo4j",
            "operation": "delete_node",
            "label": step.get("label").cloned().unwrap_or_default(),
            "id": step.get("id").cloned().unwrap_or_default(),
            "status": "pending"
        }),
        "clickhouse" => serde_json::json!({
            "backend": "clickhouse",
            "operation": "manual_review",
            "reason": "ClickHouse mutations are not generally reversible without caller-provided compensation",
            "status": "manual_review"
        }),
        _ => serde_json::json!({
            "backend": backend,
            "operation": "manual_review",
            "status": "manual_review"
        }),
    }
}
