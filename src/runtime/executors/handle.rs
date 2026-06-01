//! Enum-dispatch handle (Phase E). `BackendExecutor` uses native `async fn` and is
//! not `dyn`-compatible, so generic dispatch resolves `(BackendKind, instance)` to
//! this sized enum instead of `Box<dyn BackendExecutor>`. Each method delegates to
//! the active variant via UFCS (`Trait::method(e, …)`), keeping method dispatch
//! explicit across the concrete executor types.
//!
//! U2 step 5 — the per-backend construction match formerly in
//! `DataBrokerRuntime::resolve_dispatch_executor` now lives in each plugin via
//! [`DispatchFactory`]. The resolver looks the plugin up by `BackendKind` and
//! delegates; adding a backend means one plugin module + one factory impl, with
//! zero edits to the resolver.
#[cfg(feature = "azureblob")]
use super::azureblob::AzureBlobExecutor;
#[cfg(feature = "cassandra")]
use super::cassandra::CassandraExecutor;
#[cfg(feature = "clickhouse")]
use super::clickhouse::ClickHouseExecutor;
#[cfg(feature = "elasticsearch")]
use super::elasticsearch::ElasticsearchExecutor;
#[cfg(feature = "gcs")]
use super::gcs::GcsExecutor;
#[cfg(feature = "memcached")]
use super::memcached::MemcachedExecutor;
#[cfg(feature = "mongodb")]
use super::mongodb::MongoDbExecutor;
#[cfg(feature = "mssql")]
use super::mssql::MssqlExecutor;
#[cfg(feature = "neo4j")]
use super::neo4j::Neo4jExecutor;
#[cfg(feature = "pinecone")]
use super::pinecone::PineconeExecutor;
use super::postgres::PostgresExecutor;
#[cfg(feature = "qdrant")]
use super::qdrant::QdrantExecutor;
#[cfg(feature = "redis")]
use super::redis::RedisExecutor;
#[cfg(feature = "s3")]
use super::s3::S3Executor;
#[cfg(feature = "weaviate")]
use super::weaviate::WeaviateExecutor;
use super::{
    BackendExecutor, BackendHealth, BackendProbe, ExecutorByteStream, MutationExecutor,
    ObjectExecutor, QueryExecutor, ResourceAdminExecutor, SearchExecutor,
};
use crate::backend::BackendKind;
use crate::runtime::core::DataBrokerRuntime;

/// A resolved, ready-to-use executor for one `(backend, instance)` target.
/// Non-Postgres variants are feature-gated (Phase J); Postgres is always present.
pub(crate) enum DispatchExecutor {
    Postgres(PostgresExecutor),
    #[cfg(feature = "mysql")]
    Mysql(crate::runtime::executors::mysql::MysqlExecutor),
    #[cfg(feature = "sqlite")]
    Sqlite(crate::runtime::executors::sqlite::SqliteExecutor),
    #[cfg(feature = "qdrant")]
    Qdrant(QdrantExecutor),
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(ElasticsearchExecutor),
    #[cfg(feature = "memcached")]
    Memcached(MemcachedExecutor),
    #[cfg(feature = "mssql")]
    Mssql(MssqlExecutor),
    #[cfg(feature = "weaviate")]
    Weaviate(WeaviateExecutor),
    #[cfg(feature = "pinecone")]
    Pinecone(PineconeExecutor),
    #[cfg(feature = "cassandra")]
    Cassandra(CassandraExecutor),
    #[cfg(feature = "azureblob")]
    AzureBlob(AzureBlobExecutor),
    #[cfg(feature = "gcs")]
    Gcs(GcsExecutor),
    #[cfg(feature = "redis")]
    Redis(RedisExecutor),
    #[cfg(feature = "s3")]
    S3(S3Executor),
    #[cfg(feature = "mongodb")]
    MongoDb(MongoDbExecutor),
    #[cfg(feature = "neo4j")]
    Neo4j(Neo4jExecutor),
    #[cfg(feature = "clickhouse")]
    ClickHouse(ClickHouseExecutor),
}

/// `match self { each variant(e) => $body }` with `e` bound to the concrete executor.
/// Arms for disabled backends are `#[cfg]`-removed in lock-step with the variants.
macro_rules! on_variant {
    ($self:ident, $e:ident => $body:expr) => {
        match $self {
            DispatchExecutor::Postgres($e) => $body,
            #[cfg(feature = "mysql")]
            DispatchExecutor::Mysql($e) => $body,
            #[cfg(feature = "sqlite")]
            DispatchExecutor::Sqlite($e) => $body,
            #[cfg(feature = "qdrant")]
            DispatchExecutor::Qdrant($e) => $body,
            #[cfg(feature = "elasticsearch")]
            DispatchExecutor::Elasticsearch($e) => $body,
            #[cfg(feature = "memcached")]
            DispatchExecutor::Memcached($e) => $body,
            #[cfg(feature = "mssql")]
            DispatchExecutor::Mssql($e) => $body,
            #[cfg(feature = "weaviate")]
            DispatchExecutor::Weaviate($e) => $body,
            #[cfg(feature = "pinecone")]
            DispatchExecutor::Pinecone($e) => $body,
            #[cfg(feature = "cassandra")]
            DispatchExecutor::Cassandra($e) => $body,
            #[cfg(feature = "azureblob")]
            DispatchExecutor::AzureBlob($e) => $body,
            #[cfg(feature = "gcs")]
            DispatchExecutor::Gcs($e) => $body,
            #[cfg(feature = "redis")]
            DispatchExecutor::Redis($e) => $body,
            #[cfg(feature = "s3")]
            DispatchExecutor::S3($e) => $body,
            #[cfg(feature = "mongodb")]
            DispatchExecutor::MongoDb($e) => $body,
            #[cfg(feature = "neo4j")]
            DispatchExecutor::Neo4j($e) => $body,
            #[cfg(feature = "clickhouse")]
            DispatchExecutor::ClickHouse($e) => $body,
        }
    };
}

impl BackendHealth for DispatchExecutor {
    async fn ping(&self) -> Result<(), String> {
        on_variant!(self, e => BackendHealth::ping(e).await)
    }
}

impl QueryExecutor for DispatchExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        on_variant!(self, e => QueryExecutor::query(e, request_json).await)
    }

    async fn query_stream(&self, request_json: &str) -> Result<ExecutorByteStream, tonic::Status> {
        on_variant!(self, e => QueryExecutor::query_stream(e, request_json).await)
    }
}

impl MutationExecutor for DispatchExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        on_variant!(self, e => MutationExecutor::mutate(e, request_json).await)
    }
}

impl SearchExecutor for DispatchExecutor {
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status> {
        on_variant!(self, e => SearchExecutor::search(e, request_json).await)
    }
}

impl ObjectExecutor for DispatchExecutor {
    async fn get_object(&self, request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        on_variant!(self, e => ObjectExecutor::get_object(e, request_json).await)
    }
    async fn get_object_stream(
        &self,
        request_json: &str,
    ) -> Result<ExecutorByteStream, tonic::Status> {
        on_variant!(self, e => ObjectExecutor::get_object_stream(e, request_json).await)
    }
    async fn put_object(
        &self,
        request_json: &str,
        bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        on_variant!(self, e => ObjectExecutor::put_object(e, request_json, bytes).await)
    }
    async fn delete_object(&self, request_json: &str) -> Result<(), tonic::Status> {
        on_variant!(self, e => ObjectExecutor::delete_object(e, request_json).await)
    }
}

impl ResourceAdminExecutor for DispatchExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        on_variant!(self, e => ResourceAdminExecutor::ensure_resource(e, resource_name, spec_json).await)
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        on_variant!(self, e => ResourceAdminExecutor::drop_resource(e, resource_name).await)
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        on_variant!(self, e => ResourceAdminExecutor::list_resources(e).await)
    }
}

impl BackendExecutor for DispatchExecutor {
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status> {
        on_variant!(self, e => BackendExecutor::transaction(e, request_json).await)
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        on_variant!(self, e => BackendExecutor::probe(e).await)
    }
}

// Item 4: expose the per-backend RequestContext enforcer on the resolved enum so
// the generic-dispatch path can record each backend's ContextEffect (RLS posture)
// for the request it just resolved. Delegates to the active variant.
impl crate::runtime::backend_context::BackendContextEnforcer for DispatchExecutor {
    fn backend_label(&self) -> &str {
        use crate::runtime::backend_context::BackendContextEnforcer;
        on_variant!(self, e => BackendContextEnforcer::backend_label(e))
    }
    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        use crate::runtime::backend_context::BackendContextEnforcer;
        on_variant!(self, e => BackendContextEnforcer::enforce(e, ctx))
    }
}

// ── Plugin-driven dispatch construction (U2 step 5) ──────────────────────────
//
// `DispatchFactory` is implemented by each `backend::plugins::*` plugin to
// produce its own `DispatchExecutor` variant. The resolver in `core` looks the
// factory up by `BackendKind` (no per-backend match) and delegates. Keeping the
// trait `pub(crate)` lets the factory method return the `pub(crate)`
// `DispatchExecutor` enum without leaking it into the public API.
//
// §9.1 preserved: factories receive `&DataBrokerRuntime` and call the existing
// `pub(crate)` instance accessors (`pg_pool_for_instance`, `*_for_instance`)
// plus `choose_instance_name` for write-routing — the same orchestration the
// old match did. Replica/cache/encryption/channels stay in the typed RPC paths.

pub(crate) trait DispatchFactory: Send + Sync {
    /// Build the active executor for `(self.kind(), instance, write)`.
    ///
    /// `context` is the request's `RequestContext` if one is available — the
    /// Postgres factory bakes it into the executor so generic-dispatch SQL
    /// runs inside a transaction with `set_request_local_settings` applied
    /// (U7's RLS fix). Backends without RLS semantics (Qdrant/Mongo/S3/Neo4j/
    /// ClickHouse) ignore the parameter. Probe/health paths pass `None`.
    ///
    /// Returns a `tonic::Status` (typically `FailedPrecondition`) if the
    /// requested instance is not configured/connected.
    fn build_dispatch_executor(
        &self,
        runtime: &DataBrokerRuntime,
        instance: Option<&str>,
        write: bool,
        context: Option<&crate::broker::RequestContext>,
    ) -> Result<DispatchExecutor, tonic::Status>;
}

/// Look up the dispatch factory for a `BackendKind`. Returns `None` for
/// backends without a generic-dispatch executor in the current binary
/// (unsupported kinds, or kinds whose Cargo feature is disabled).
#[allow(unreachable_patterns)]
pub(crate) fn dispatch_factory_for(kind: &BackendKind) -> Option<&'static dyn DispatchFactory> {
    use crate::backend::plugins;
    match kind {
        BackendKind::Postgres => Some(&plugins::postgres::PLUGIN),
        #[cfg(feature = "mysql")]
        BackendKind::Mysql => Some(&plugins::mysql::PLUGIN),
        #[cfg(feature = "sqlite")]
        BackendKind::Sqlite => Some(&plugins::sqlite::PLUGIN),
        #[cfg(feature = "qdrant")]
        BackendKind::Qdrant => Some(&plugins::qdrant::PLUGIN),
        #[cfg(feature = "elasticsearch")]
        BackendKind::Elasticsearch => Some(&plugins::elasticsearch::PLUGIN),
        #[cfg(feature = "memcached")]
        BackendKind::Memcached => Some(&plugins::memcached::PLUGIN),
        #[cfg(feature = "mssql")]
        BackendKind::Mssql => Some(&plugins::mssql::PLUGIN),
        #[cfg(feature = "weaviate")]
        BackendKind::Weaviate => Some(&plugins::weaviate::PLUGIN),
        #[cfg(feature = "pinecone")]
        BackendKind::Pinecone => Some(&plugins::pinecone::PLUGIN),
        #[cfg(feature = "cassandra")]
        BackendKind::Cassandra => Some(&plugins::cassandra::PLUGIN),
        #[cfg(feature = "azureblob")]
        BackendKind::AzureBlob => Some(&plugins::azureblob::PLUGIN),
        #[cfg(feature = "gcs")]
        BackendKind::Gcs => Some(&plugins::gcs::PLUGIN),
        #[cfg(feature = "redis")]
        BackendKind::Redis => Some(&plugins::redis::PLUGIN),
        #[cfg(feature = "s3")]
        BackendKind::S3 => Some(&plugins::s3::PLUGIN),
        #[cfg(feature = "s3")]
        BackendKind::Minio => Some(&plugins::minio::PLUGIN),
        #[cfg(feature = "mongodb")]
        BackendKind::Mongodb => Some(&plugins::mongodb::PLUGIN),
        #[cfg(feature = "neo4j")]
        BackendKind::Neo4j => Some(&plugins::neo4j::PLUGIN),
        #[cfg(feature = "clickhouse")]
        BackendKind::Clickhouse => Some(&plugins::clickhouse::PLUGIN),
        _ => None,
    }
}
