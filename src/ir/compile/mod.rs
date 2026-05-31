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
    LogicalAggregate, LogicalDelete, LogicalRead, LogicalResourceOp, LogicalSearch, LogicalWrite,
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

mod util;

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
