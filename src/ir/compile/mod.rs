//! Compilers from the neutral IR to backend-specific wire shapes (U2 step 2).
//!
//! One `Compiler` impl per backend plugin: Postgres lowers to SQL with
//! bind parameters, Mongo lowers to find/update documents, Neo4j lowers to
//! parameterised Cypher, Qdrant lowers to its HTTP JSON, etc. The trait is
//! intentionally **sync** — compilation is CPU work over a manifest snapshot
//! and never does I/O, so it composes inside the multi-leg planner without
//! Send/Sync futures gymnastics.
//!
//! The compilation product is the [`CompiledRendering`] enum: a small,
//! backend-shaped variant the executor knows how to bind. Keeping a single
//! enum (instead of one type per backend) is what lets the dispatcher fan
//! out to N backends without N return types.

use serde::Serialize;

use super::operations::{
    LogicalAggregate, LogicalDelete, LogicalRead, LogicalResourceOp, LogicalSearch, LogicalUpdate,
    LogicalWrite,
};
use super::value::LogicalValue;
use crate::backend::BackendKind;
use crate::generation::CatalogManifest;

pub mod postgres;

#[cfg(any(feature = "mysql", test))]
pub mod mysql;

#[cfg(any(feature = "sqlite", test))]
pub mod sqlite;

#[cfg(any(feature = "mongodb", test))]
pub mod mongodb;

#[cfg(any(feature = "neo4j", test))]
pub mod neo4j;

#[cfg(any(feature = "clickhouse", test))]
pub mod clickhouse;

#[cfg(any(feature = "qdrant", test))]
pub mod qdrant;

#[cfg(any(feature = "elasticsearch", test))]
pub mod elasticsearch;

#[cfg(any(feature = "memcached", test))]
pub mod memcached;

// C9: Mssql T-SQL compiler. The IR compiler is always test-compiled so
// the cross-backend test suite can verify dialect lowerings without
// needing the heavy tiberius driver dep enabled.
#[cfg(any(feature = "mssql", test))]
pub mod mssql;

#[cfg(any(feature = "weaviate", test))]
pub mod weaviate;

#[cfg(any(feature = "pinecone", test))]
pub mod pinecone;

#[cfg(any(feature = "cassandra", test))]
pub mod cassandra;

#[cfg(any(feature = "azureblob", test))]
pub mod azureblob;

#[cfg(any(feature = "gcs", test))]
pub mod gcs;

#[cfg(any(feature = "redis", test))]
pub mod redis;

#[cfg(any(feature = "s3", test))]
pub mod s3;

mod sql_dialect;
mod util;

#[cfg(all(test, not(udb_portable)))]
mod live_tests;

/// What every per-backend compiler implements.
///
/// All methods default to a typed `OperationNotSupported` error so a
/// plugin only overrides the ops it can handle — Redis overrides
/// `compile_resource_op` but rejects `compile_search`, S3 overrides nothing
/// directly readable, etc. The capability matrix surfaced by
/// `Backend::capabilities()` lines up with which methods are overridden.
pub trait Compiler: Send + Sync {
    /// The backend this compiler targets. Used by the dispatcher to look
    /// the right compiler up by `BackendKind`.
    fn kind(&self) -> BackendKind;

    /// True only for compilers that explicitly lower `LogicalRead.include`.
    /// Backends that do not opt in keep the shared fail-closed guard in
    /// [`compile_with`], so eager includes are never silently ignored.
    fn supports_read_include(&self) -> bool {
        false
    }

    fn compile_read(
        &self,
        _op: &LogicalRead,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(self.kind(), "read"))
    }

    fn compile_write(
        &self,
        _op: &LogicalWrite,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(self.kind(), "write"))
    }

    fn compile_update(
        &self,
        _op: &LogicalUpdate,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(self.kind(), "update"))
    }

    fn compile_delete(
        &self,
        _op: &LogicalDelete,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(self.kind(), "delete"))
    }

    fn compile_search(
        &self,
        _op: &LogicalSearch,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(self.kind(), "search"))
    }

    fn compile_resource_op(
        &self,
        _op: &LogicalResourceOp,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(
            self.kind(),
            "resource_op",
        ))
    }

    /// NW2: lower a `LogicalAggregate` (GROUP BY + aggregate functions +
    /// HAVING) to backend-native form. Only backends with an actual
    /// aggregation surface (Postgres, MySQL, SQLite, ClickHouse,
    /// MongoDB `$group`, …) override this. KV/object stores correctly
    /// fall through to the default `OperationNotSupported`.
    fn compile_aggregate(
        &self,
        _op: &LogicalAggregate,
        _ctx: &CompileContext<'_>,
    ) -> Result<CompiledRendering, CompileError> {
        Err(CompileError::operation_unsupported(
            self.kind(),
            "aggregate",
        ))
    }
}

/// Borrowed neutral operation for runtime dispatch. This keeps the compiler
/// registry independent of the gRPC/request-layer envelope while still giving
/// the runtime one call-site for every backend/op pair.
pub enum CompileOperation<'a> {
    Read(&'a LogicalRead),
    Write(&'a LogicalWrite),
    Update(&'a LogicalUpdate),
    Delete(&'a LogicalDelete),
    Search(&'a LogicalSearch),
    ResourceOp(&'a LogicalResourceOp),
    Aggregate(&'a LogicalAggregate),
}

impl<'a> CompileOperation<'a> {
    pub fn token(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::Write(_) => "write",
            Self::Update(_) => "update",
            Self::Delete(_) => "delete",
            Self::Search(_) => "search",
            Self::ResourceOp(_) => "resource_op",
            Self::Aggregate(_) => "aggregate",
        }
    }
}

fn compile_with<C: Compiler>(
    compiler: C,
    op: CompileOperation<'_>,
    ctx: &CompileContext<'_>,
) -> Result<CompiledRendering, CompileError> {
    if let CompileOperation::Read(read) = op
        && !read.include.is_empty()
        && !compiler.supports_read_include()
    {
        return Err(CompileError::OperatorUnsupported {
            backend: compiler.kind(),
            op: "include",
        });
    }
    match op {
        CompileOperation::Read(op) => compiler.compile_read(op, ctx),
        CompileOperation::Write(op) => compiler.compile_write(op, ctx),
        CompileOperation::Update(op) => compiler.compile_update(op, ctx),
        CompileOperation::Delete(op) => compiler.compile_delete(op, ctx),
        CompileOperation::Search(op) => compiler.compile_search(op, ctx),
        CompileOperation::ResourceOp(op) => compiler.compile_resource_op(op, ctx),
        CompileOperation::Aggregate(op) => compiler.compile_aggregate(op, ctx),
    }
}

/// Compile one neutral operation for a backend whose compiler is present in
/// this build. `None` means the backend has no compiler compiled into the
/// binary, which is distinct from a compiler rejecting an unsupported op.
pub fn compile_for_backend(
    kind: &BackendKind,
    op: CompileOperation<'_>,
    ctx: &CompileContext<'_>,
) -> Option<Result<CompiledRendering, CompileError>> {
    match kind {
        BackendKind::Postgres => Some(compile_with(postgres::PostgresCompiler, op, ctx)),
        BackendKind::Mysql => {
            #[cfg(any(feature = "mysql", test))]
            {
                Some(compile_with(mysql::MysqlCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "mysql", test)))]
            {
                None
            }
        }
        BackendKind::Sqlite => {
            #[cfg(any(feature = "sqlite", test))]
            {
                Some(compile_with(sqlite::SqliteCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "sqlite", test)))]
            {
                None
            }
        }
        BackendKind::Mssql => {
            #[cfg(any(feature = "mssql", test))]
            {
                Some(compile_with(mssql::MssqlCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "mssql", test)))]
            {
                None
            }
        }
        BackendKind::Clickhouse => {
            #[cfg(any(feature = "clickhouse", test))]
            {
                Some(compile_with(clickhouse::ClickHouseCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "clickhouse", test)))]
            {
                None
            }
        }
        BackendKind::Mongodb => {
            #[cfg(any(feature = "mongodb", test))]
            {
                Some(compile_with(mongodb::MongoDbCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "mongodb", test)))]
            {
                None
            }
        }
        BackendKind::Neo4j => {
            #[cfg(any(feature = "neo4j", test))]
            {
                Some(compile_with(neo4j::Neo4jCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "neo4j", test)))]
            {
                None
            }
        }
        BackendKind::Qdrant => {
            #[cfg(any(feature = "qdrant", test))]
            {
                Some(compile_with(qdrant::QdrantCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "qdrant", test)))]
            {
                None
            }
        }
        BackendKind::Elasticsearch => {
            #[cfg(any(feature = "elasticsearch", test))]
            {
                Some(compile_with(elasticsearch::ElasticsearchCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "elasticsearch", test)))]
            {
                None
            }
        }
        BackendKind::Memcached => {
            #[cfg(any(feature = "memcached", test))]
            {
                Some(compile_with(memcached::MemcachedCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "memcached", test)))]
            {
                None
            }
        }
        BackendKind::Weaviate => {
            #[cfg(any(feature = "weaviate", test))]
            {
                Some(compile_with(weaviate::WeaviateCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "weaviate", test)))]
            {
                None
            }
        }
        BackendKind::Pinecone => {
            #[cfg(any(feature = "pinecone", test))]
            {
                Some(compile_with(pinecone::PineconeCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "pinecone", test)))]
            {
                None
            }
        }
        BackendKind::Cassandra => {
            #[cfg(any(feature = "cassandra", test))]
            {
                Some(compile_with(cassandra::CassandraCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "cassandra", test)))]
            {
                None
            }
        }
        BackendKind::AzureBlob => {
            #[cfg(any(feature = "azureblob", test))]
            {
                Some(compile_with(azureblob::AzureBlobCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "azureblob", test)))]
            {
                None
            }
        }
        BackendKind::Gcs => {
            #[cfg(any(feature = "gcs", test))]
            {
                Some(compile_with(gcs::GcsCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "gcs", test)))]
            {
                None
            }
        }
        BackendKind::Redis => {
            #[cfg(any(feature = "redis", test))]
            {
                Some(compile_with(redis::RedisCompiler, op, ctx))
            }
            #[cfg(not(any(feature = "redis", test)))]
            {
                None
            }
        }
        BackendKind::S3 | BackendKind::Minio => {
            #[cfg(any(feature = "s3", test))]
            {
                Some(compile_with(s3::S3Compiler, op, ctx))
            }
            #[cfg(not(any(feature = "s3", test)))]
            {
                None
            }
        }
    }
}

/// The backends whose IR compiler is actually compiled into this build, i.e.
/// the exact set for which [`compile_for_backend`] returns `Some(..)` rather
/// than `None`.
///
/// This is the ONE source of truth for "does this build have a compiler for
/// `kind`". It mirrors [`compile_for_backend`] arm-for-arm using the SAME
/// `#[cfg(any(feature = "...", test))]` gating, so the list cannot drift from
/// what the compiler dispatch can actually lower — keep the two functions
/// adjacent and edit them together. `BackendKind::Minio` shares the S3
/// compiler, matching the `S3 | Minio` arm above.
pub fn mediated_backends() -> Vec<BackendKind> {
    let mut out = Vec::new();
    // Postgres is always compiled in (no feature gate).
    out.push(BackendKind::Postgres);
    #[cfg(any(feature = "mysql", test))]
    out.push(BackendKind::Mysql);
    #[cfg(any(feature = "sqlite", test))]
    out.push(BackendKind::Sqlite);
    #[cfg(any(feature = "mssql", test))]
    out.push(BackendKind::Mssql);
    #[cfg(any(feature = "clickhouse", test))]
    out.push(BackendKind::Clickhouse);
    #[cfg(any(feature = "mongodb", test))]
    out.push(BackendKind::Mongodb);
    #[cfg(any(feature = "neo4j", test))]
    out.push(BackendKind::Neo4j);
    #[cfg(any(feature = "qdrant", test))]
    out.push(BackendKind::Qdrant);
    #[cfg(any(feature = "elasticsearch", test))]
    out.push(BackendKind::Elasticsearch);
    #[cfg(any(feature = "memcached", test))]
    out.push(BackendKind::Memcached);
    #[cfg(any(feature = "weaviate", test))]
    out.push(BackendKind::Weaviate);
    #[cfg(any(feature = "pinecone", test))]
    out.push(BackendKind::Pinecone);
    #[cfg(any(feature = "cassandra", test))]
    out.push(BackendKind::Cassandra);
    #[cfg(any(feature = "azureblob", test))]
    out.push(BackendKind::AzureBlob);
    #[cfg(any(feature = "gcs", test))]
    out.push(BackendKind::Gcs);
    #[cfg(any(feature = "redis", test))]
    out.push(BackendKind::Redis);
    #[cfg(any(feature = "s3", test))]
    {
        // The `S3 | Minio` arm of `compile_for_backend` is gated on `feature = "s3"`.
        out.push(BackendKind::S3);
        out.push(BackendKind::Minio);
    }
    out
}

/// True when this build has an IR compiler compiled in for `kind` — i.e. when
/// [`compile_for_backend`] would return `Some(..)`. Delegates to
/// [`mediated_backends`] so there is a single source of truth.
pub fn is_mediated_backend(kind: &BackendKind) -> bool {
    mediated_backends().contains(kind)
}

/// Inputs every compiler needs but doesn't own.
///
/// `manifest` resolves logical message types to physical
/// tables/collections; `tenant_id` and `project_id` are *not* injected into
/// the rendered output here — that's the runtime's job (it sets transaction-
/// local settings before executing the rendered SQL). The compiler only
/// uses them for cache-key canonicalisation and capability gating.
#[derive(Debug, Clone, Copy)]
pub struct CompileContext<'a> {
    pub manifest: &'a CatalogManifest,
    pub tenant_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub backend_instance: Option<&'a str>,
}

impl<'a> CompileContext<'a> {
    pub fn new(manifest: &'a CatalogManifest) -> Self {
        Self {
            manifest,
            tenant_id: None,
            project_id: None,
            backend_instance: None,
        }
    }

    pub fn with_tenant(mut self, tenant_id: &'a str) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    pub fn with_project(mut self, project_id: &'a str) -> Self {
        self.project_id = Some(project_id);
        self
    }

    pub fn with_instance(mut self, backend_instance: &'a str) -> Self {
        self.backend_instance = Some(backend_instance);
        self
    }
}

/// The result of a successful compilation: a backend-shaped wire form
/// the executor can hand directly to its driver.
///
/// One enum (not one struct per backend) so the multi-leg planner can
/// store `Vec<CompiledRendering>` heterogeneously and the dispatcher can
/// match once at execution.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum CompiledRendering {
    /// Postgres / ClickHouse / any SQL backend.
    Sql {
        backend: BackendKind,
        statement: String,
        params: Vec<LogicalValue>,
    },
    /// Mongo / Qdrant / Neo4j HTTP / S3 admin — anything that posts JSON.
    Json {
        backend: BackendKind,
        method: HttpMethod,
        path: String,
        body: serde_json::Value,
    },
    /// Redis key plan — the executor expands the template with the active
    /// tenant/instance and issues the right command (`GET`, `SET`, `DEL`).
    KeyValue {
        backend: BackendKind,
        op: KeyValueOp,
        key_template: String,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>,
    },
    /// S3 / MinIO — the bucket + key + operation form. Body bytes are
    /// streamed at execution time, not embedded here.
    Object {
        backend: BackendKind,
        op: ObjectOp,
        bucket: String,
        key: String,
        content_type: Option<String>,
    },
}

impl CompiledRendering {
    /// Backend this rendering targets. Useful for the dispatcher to pick
    /// the right `DispatchExecutor` variant without re-matching.
    pub fn backend(&self) -> BackendKind {
        match self {
            Self::Sql { backend, .. }
            | Self::Json { backend, .. }
            | Self::KeyValue { backend, .. }
            | Self::Object { backend, .. } => backend.clone(),
        }
    }

    /// Stable shape name for traces and the canonical cache key.
    pub fn shape_token(&self) -> &'static str {
        match self {
            Self::Sql { .. } => "sql",
            Self::Json { .. } => "json",
            Self::KeyValue { .. } => "kv",
            Self::Object { .. } => "object",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyValueOp {
    Get,
    Set,
    Delete,
    Exists,
    Scan,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectOp {
    GetObject,
    PutObject,
    HeadObject,
    DeleteObject,
    ListObjects,
    GeneratePresigned,
}

/// Typed compile errors. Each carries enough context for the runtime to
/// translate to the right `tonic::Status` code without re-parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum CompileError {
    /// The IR refers to a message type the manifest doesn't know.
    UnknownMessageType { message_type: String },
    /// The IR refers to a field the manifest doesn't declare for this
    /// message type, on a backend that requires it (most do).
    UnknownField { message_type: String, field: String },
    /// The op semantically can't be performed on this backend (e.g. vector
    /// search on Postgres without pgvector).
    OperationNotSupported {
        backend: BackendKind,
        op: &'static str,
    },
    /// The op could be lowered but the requested operator isn't supported
    /// (e.g. `ILike` on a backend with no case-insensitive index).
    OperatorUnsupported {
        backend: BackendKind,
        op: &'static str,
    },
    /// The IR is internally inconsistent (e.g. `LogicalSearch` with no
    /// vector and no text query, or pagination with both offset and cursor).
    Malformed { reason: String },
    /// A `LogicalValue::Int` would overflow the backend's column width.
    ValueOutOfRange {
        backend: BackendKind,
        field: String,
        reason: String,
    },
    /// Catch-all for backend-specific compile failures (Cypher escape error,
    /// SQL identifier validation, …).
    BackendSpecific {
        backend: BackendKind,
        message: String,
    },
}

impl CompileError {
    pub(crate) fn operation_unsupported(backend: BackendKind, op: &'static str) -> Self {
        Self::OperationNotSupported { backend, op }
    }

    /// Stable token used in tonic Status messages and traces. Don't change
    /// without updating the SDKs' error-code constants.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownMessageType { .. } => "unknown_message_type",
            Self::UnknownField { .. } => "unknown_field",
            Self::OperationNotSupported { .. } => "operation_not_supported",
            Self::OperatorUnsupported { .. } => "operator_unsupported",
            Self::Malformed { .. } => "malformed",
            Self::ValueOutOfRange { .. } => "value_out_of_range",
            Self::BackendSpecific { .. } => "backend_specific",
        }
    }
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownMessageType { message_type } => {
                write!(f, "unknown message type: {message_type}")
            }
            Self::UnknownField {
                message_type,
                field,
            } => write!(f, "unknown field '{field}' on message '{message_type}'"),
            Self::OperationNotSupported { backend, op } => write!(
                f,
                "{backend_name} does not support the {op} operation",
                backend_name = backend.as_str()
            ),
            Self::OperatorUnsupported { backend, op } => write!(
                f,
                "{backend_name} does not support the {op} operator",
                backend_name = backend.as_str()
            ),
            Self::Malformed { reason } => write!(f, "malformed operation: {reason}"),
            Self::ValueOutOfRange {
                backend,
                field,
                reason,
            } => write!(
                f,
                "{backend_name} value out of range for field '{field}': {reason}",
                backend_name = backend.as_str()
            ),
            Self::BackendSpecific { backend, message } => {
                write!(f, "{}: {message}", backend.as_str())
            }
        }
    }
}

impl std::error::Error for CompileError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopCompiler;
    impl Compiler for NoopCompiler {
        fn kind(&self) -> BackendKind {
            BackendKind::Postgres
        }
        // No method overrides — every op returns OperationNotSupported by default.
    }

    fn manifest() -> CatalogManifest {
        // Use the simplest manifest the type allows; we don't actually
        // resolve anything against it in this test.
        CatalogManifest::default()
    }

    #[test]
    fn default_impls_reject_every_op() {
        let c = NoopCompiler;
        let m = manifest();
        let ctx = CompileContext::new(&m);

        let read = LogicalRead::message("X");
        let err = c.compile_read(&read, &ctx).unwrap_err();
        assert!(matches!(
            err,
            CompileError::OperationNotSupported { op: "read", .. }
        ));
        assert_eq!(err.code(), "operation_not_supported");
    }

    #[test]
    fn error_codes_are_pinned() {
        // SDKs branch on these; pin them.
        assert_eq!(
            CompileError::operation_unsupported(BackendKind::Redis, "search").code(),
            "operation_not_supported"
        );
        assert_eq!(
            CompileError::Malformed { reason: "x".into() }.code(),
            "malformed"
        );
    }

    #[test]
    fn read_include_fails_closed_for_backend_without_lowering() {
        let m = manifest();
        let ctx = CompileContext::new(&m);
        let read = LogicalRead::message("X").with_include("tenant");

        let err = compile_with(NoopCompiler, CompileOperation::Read(&read), &ctx)
            .expect_err("include must not be silently ignored");
        assert!(matches!(
            err,
            CompileError::OperatorUnsupported { op: "include", .. }
        ));
        assert_eq!(err.code(), "operator_unsupported");
    }

    #[test]
    fn every_mediated_backend_has_a_real_compile_arm() {
        // Anti-drift guard for master-plan item 2.3: `mediated_backends()` and
        // `compile_for_backend` share the same `#[cfg(..)]` arms, so for every
        // backend the list claims is mediated, `compile_for_backend` must (a)
        // actually return `Some(..)` (the arm exists / is compiled in) and (b)
        // not reject a trivial read with `OperationNotSupported` (the compiler
        // really lowers reads). A drift between the two lists shows up here as a
        // `None` or an `OperationNotSupported`.
        let m = manifest();
        let ctx = CompileContext::new(&m);
        let mediated = mediated_backends();
        assert!(
            !mediated.is_empty(),
            "at least Postgres is always compiled in"
        );
        assert!(
            mediated.contains(&BackendKind::Postgres),
            "Postgres has no feature gate and must always be mediated"
        );
        for kind in mediated {
            // `is_mediated_backend` must agree with the list it derives from.
            assert!(
                is_mediated_backend(&kind),
                "{kind:?} in mediated_backends() but is_mediated_backend() said no"
            );

            let read = LogicalRead::message("X");
            let result = compile_for_backend(&kind, CompileOperation::Read(&read), &ctx);
            let outcome = result
                .unwrap_or_else(|| panic!("{kind:?} is mediated but compile arm returned None"));
            if let Err(CompileError::OperationNotSupported { op, .. }) = &outcome {
                panic!("{kind:?} is mediated but rejected a trivial read as unsupported (op={op})");
            }
        }
    }

    #[test]
    fn compiled_rendering_carries_backend_through() {
        let r = CompiledRendering::Sql {
            backend: BackendKind::Postgres,
            statement: "SELECT 1".into(),
            params: vec![],
        };
        assert_eq!(r.backend(), BackendKind::Postgres);
        assert_eq!(r.shape_token(), "sql");
    }
}
