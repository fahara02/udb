// src/backend/kind.rs — the portable `BackendKind` enum, split out of
// `backend/mod.rs` so the WASM/edge-safe `udb-portable` crate can
// `#[path]`-include JUST the backend-identity enum (which the IR→SQL
// compilers match on) WITHOUT dragging in `backend::plugin` /
// `backend::plugins` (the native driver inventory).
//
// All `impl BackendKind { … }` blocks (as_str/tier/role/capabilities/…)
// stay in `mod.rs`; they reference this enum via `pub use kind::BackendKind`.
// The server build is byte-identical — only the enum's physical location moved.

use serde::{Deserialize, Serialize};

/// All storage backends the UDB can manage or broker connections to.
///
/// Used for:
/// - Resolving the correct config block from `UdbConfig`
/// - Building the `udb+<tier>+<backend>://…` DSN scheme
/// - Routing APPLYING-phase commands (SQL DDL vs bucket/collection creation)
/// - Labelling metrics (tier + backend)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    // ── Tier 1 — SQL / Relational ─────────────────────────────────────────────
    /// PostgreSQL — the primary migration ledger and main relational store.
    /// Also used for the backup DB and signal (analytics) DB.
    Postgres,
    /// MySQL / MariaDB (extended deployment).
    Mysql,
    /// SQLite (embedded; test/dev only).
    Sqlite,
    /// Microsoft SQL Server (on-premise banking integration).
    Mssql,
    /// ClickHouse (analytics column store via SQL wire protocol).
    Clickhouse,

    // ── Tier 2 — Cache ────────────────────────────────────────────────────────
    /// Redis — default cache tier (session, rate-limit, hot read-through).
    Redis,
    /// Memcached (legacy cache fallback).
    Memcached,

    // ── Tier 3 — Vector ───────────────────────────────────────────────────────
    /// Qdrant — default vector store (embeddings, similarity search).
    Qdrant,
    /// Weaviate (alternative vector DB).
    Weaviate,
    /// Pinecone (managed vector DB — cloud deployments).
    Pinecone,

    // ── Tier 4 — Blob / Object ────────────────────────────────────────────────
    /// MinIO — default S3-compatible object store (artifacts, exports).
    Minio,
    /// AWS S3 (cloud deployments).
    S3,
    /// Azure Blob Storage.
    AzureBlob,
    /// Google Cloud Storage.
    Gcs,

    // ── Extended stores ───────────────────────────────────────────────────────
    /// MongoDB (document store for unstructured data).
    Mongodb,
    /// Elasticsearch (full-text search + analytics).
    Elasticsearch,
    /// Neo4j (graph DB for relationship queries).
    Neo4j,
    /// Cassandra / ScyllaDB (wide-column).
    Cassandra,
}

impl BackendKind {
    /// Returns the canonical lowercase identifier used in DSN scheme construction
    /// and the `backend` field of `UnifiedDsn`.
    ///
    /// Lives here (not in `mod.rs`) because the portable IR→SQL compilers call it
    /// ~100× for backend-tagged labels/cache keys; method resolution finds it from
    /// either crate. All other `BackendKind` methods stay in `mod.rs`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Sqlite => "sqlite",
            Self::Mssql => "sqlserver",
            Self::Clickhouse => "clickhouse",
            Self::Redis => "redis",
            Self::Memcached => "memcached",
            Self::Qdrant => "qdrant",
            Self::Weaviate => "weaviate",
            Self::Pinecone => "pinecone",
            Self::Minio => "minio",
            Self::S3 => "s3",
            Self::AzureBlob => "azureblob",
            Self::Gcs => "gcs",
            Self::Mongodb => "mongodb",
            Self::Elasticsearch => "elasticsearch",
            Self::Neo4j => "neo4j",
            Self::Cassandra => "cassandra",
        }
    }
}
