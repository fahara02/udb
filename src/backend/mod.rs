// src/backend/mod.rs — Canonical registry of all UDB storage backends.
//
// U2 (refactor plan §2/§5): in addition to the `BackendKind` enum below, this
// module exposes a `Backend` plugin trait (`backend::plugin::Backend`) and a
// per-backend plugin inventory (`backend::plugins::*`). Adding a backend means
// one plugin module + one entry in `plugins::all()` — no edits in dispatch,
// generation, or the CLI. The trait is intentionally object-safe so consumers
// iterate `&[&dyn Backend]` rather than matching on `BackendKind`.

pub mod plugin;
pub mod plugins;

pub use plugin::{
    Backend, BackendConformanceReport, BackendPluginContract, BackendPluginSurface,
    BackendSupportState, all_plugins, has_runtime_implementation, plugin_for, plugin_for_kind,
    support_state_for_kind, support_state_for_token,
};

// The Universal Data Broker abstracts over four primary
// storage tiers mandated by the spec (§16.1):
//
//   Tier 1 — SQL / Relational   : PostgreSQL
//   Tier 2 — Cache              : Redis
//   Tier 3 — Vector             : Qdrant  (default)
//   Tier 4 — Blob / Object      : MinIO   (default)
//
// Additional backends are recognised for multi-cloud / extended deployments.
// The `BackendKind` enum is the single source of truth for backend identity
// used in DSN construction, APPLYING-phase dispatch, auto-alter repair routing,
// and metrics labelling.
//
// Design note:
//   The Go legacy_sql reference only supports PostgreSQL because it was built as
//   a pure relational migration engine.  UDB extends that with
//   three more storage tiers.  This module is the bridge type that makes the
//   Rust library truly "universal".

use serde::{Deserialize, Serialize};

// ── Backend kind ──────────────────────────────────────────────────────────────

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

    /// Parse a canonical backend token (the inverse of [`Self::as_str`]) into a
    /// `BackendKind`. Case-insensitive. Returns `None` for unknown tokens.
    ///
    /// This is the canonical token parser for typed dispatch. It mirrors
    /// `as_str` exactly (including the `mssql -> sqlserver` token); it does NOT
    /// use the serde representation, which differs for some variants (see the
    /// `known_as_str_vs_serde_divergences_are_locked` test).
    pub fn from_token(token: &str) -> Option<Self> {
        match token.trim().to_ascii_lowercase().as_str() {
            "postgres" => Some(Self::Postgres),
            "mysql" => Some(Self::Mysql),
            "sqlite" => Some(Self::Sqlite),
            "sqlserver" => Some(Self::Mssql),
            "clickhouse" => Some(Self::Clickhouse),
            "redis" => Some(Self::Redis),
            "memcached" => Some(Self::Memcached),
            "qdrant" => Some(Self::Qdrant),
            "weaviate" => Some(Self::Weaviate),
            "pinecone" => Some(Self::Pinecone),
            "minio" => Some(Self::Minio),
            "s3" => Some(Self::S3),
            "azureblob" => Some(Self::AzureBlob),
            "gcs" => Some(Self::Gcs),
            "mongodb" => Some(Self::Mongodb),
            "elasticsearch" => Some(Self::Elasticsearch),
            "neo4j" => Some(Self::Neo4j),
            "cassandra" => Some(Self::Cassandra),
            _ => None,
        }
    }

    /// Returns the storage tier this backend belongs to.
    pub fn tier(&self) -> BackendTier {
        match self {
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql | Self::Clickhouse => {
                BackendTier::Sql
            }
            Self::Redis | Self::Memcached => BackendTier::Cache,
            Self::Qdrant | Self::Weaviate | Self::Pinecone => BackendTier::Vector,
            Self::Minio | Self::S3 | Self::AzureBlob | Self::Gcs => BackendTier::Object,
            Self::Mongodb | Self::Elasticsearch => BackendTier::Document,
            Self::Neo4j => BackendTier::Graph,
            Self::Cassandra => BackendTier::Column,
        }
    }

    /// P2P: returns the backend's data-plane role.
    ///
    /// Pinned per backend kind. `Canonical` backends can host the UDB
    /// system tables and act as a write-durability anchor; `Projection`
    /// backends are write targets only; `Both` can play either role
    /// depending on operator config.
    pub fn role(&self) -> BackendRole {
        match self {
            // Canonical: SQL stores with strong durability + a
            // queryable write-progress token (LSN / GTID / data_version
            // / resumeToken).
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql | Self::Mongodb => {
                BackendRole::Canonical
            }
            // Both: durable enough to host system tables AND useful as
            // a projection target. Operator picks the role per
            // deployment.
            Self::Clickhouse | Self::Neo4j => BackendRole::Both,
            // Projection-only: no durable write-progress token, or
            // semantics that don't fit canonical roles (cache,
            // object store, vector index).
            Self::Redis
            | Self::Memcached
            | Self::Qdrant
            | Self::Weaviate
            | Self::Pinecone
            | Self::Minio
            | Self::S3
            | Self::AzureBlob
            | Self::Gcs
            | Self::Elasticsearch
            | Self::Cassandra => BackendRole::Projection,
        }
    }

    /// Returns the capability flags for this backend.
    pub fn capabilities(&self) -> BackendCapability {
        match self {
            Self::Postgres => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: true,
                supports_xa: false,
                supports_two_phase_commit: true,
                supports_rls: true,
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: true,
                supports_idempotency: true,
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "strong".into(),
            },
            // MySQL: XA participant wired via XaMysqlParticipant (NW-deep).
            // `XA START / END / PREPARE / COMMIT / ROLLBACK` syntax goes
            // through the executor; the saga compensator is registered
            // for full row rollback.
            Self::Mysql => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: true,
                supports_xa: true,
                supports_two_phase_commit: true,
                supports_rls: true, // session-var enforcement via BackendContextEnforcer (NW-deep)
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: true, // P2P: MySQL canonical store wired
                supports_idempotency: true,
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "strong".into(),
            },
            // C9: SQL Server is now a real plugin via the tiberius
            // driver. Real TDS-protocol implementation; ADO connection
            // string accepted via UDB_MSSQL_DSN. T-SQL compiler covers
            // all 6 IR ops (MERGE-based upsert, OFFSET/FETCH NEXT
            // pagination, CONTAINS() full-text search, sys.tables
            // resource ops).
            Self::Mssql => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: true, // BEGIN/COMMIT TRANSACTION wired
                supports_xa: false,          // distributed TX would need MSDTC
                supports_two_phase_commit: false,
                supports_rls: false, // SESSION_CONTEXT injection is operator-side
                supports_vector_search: false,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true, // MERGE handles upsert idempotency
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "strong".into(),
            },
            Self::Sqlite => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: true,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // temp-table scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: true, // P2P: SQLite canonical store wired
                supports_idempotency: true,
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "strong".into(),
            },
            Self::Clickhouse => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // session-setting scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: true,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: false,
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "eventual".into(),
            },
            Self::Redis => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // key-namespace scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: true,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: false,
                max_payload_bytes: 536_870_912, // 512 MiB (Redis default)
                consistency_model: "eventual".into(),
            },
            Self::Memcached => BackendCapability {
                // C9: Memcached is now a real plugin via the
                // canonical `memcache` crate (binary protocol,
                // wrapped in spawn_blocking). KV-only — no SQL, no
                // search, no resource lifecycle.
                supports_sql_ddl: false,
                supports_transactions: false, // single-key CAS only; not a tx
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // key namespace scoping
                supports_vector_search: false,
                supports_streaming: false,
                supports_ttl: true, // per-item expiration
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true, // set is idempotent
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: false, // no buckets / namespaces
                max_payload_bytes: 1_048_576,       // 1 MiB (memcached's default item limit)
                consistency_model: "eventual".into(),
            },
            Self::Qdrant => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // payload-filter scoping via BackendContextEnforcer
                supports_vector_search: true,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: true,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "eventual".into(),
            },
            // C9: Weaviate is a real plugin (REST + GraphQL via
            // reqwest). Full Read/Write/Delete/Search/ResourceOp +
            // hybrid (nearVector + bm25) coverage.
            Self::Weaviate => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true,
                supports_vector_search: true,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: true,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "eventual".into(),
            },
            // C9 (expanded): Pinecone now has full coverage of the
            // IR ops — not just vectors. Hybrid sparse+dense search
            // via precomputed sparse_values; partial metadata updates
            // via /vectors/update; metadata scans via /vectors/list;
            // count + per-namespace aggregate via
            // /describe_index_stats; index + collection lifecycle.
            // Namespaces (per-project) give two-level multi-tenant
            // isolation alongside metadata-filter tenant_id.
            Self::Pinecone => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // namespace + metadata filter
                supports_vector_search: true,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: true, // sparse+dense via sparse_values
                supports_resource_lifecycle: true,
                max_payload_bytes: 2_097_152, // 2 MiB per upsert request
                consistency_model: "eventual".into(),
            },
            Self::Minio | Self::S3 => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // key-prefix scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: true,
                is_object_store: true,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 5_368_709_120, // 5 GiB (S3 part limit)
                consistency_model: "strong".into(),
            },
            // C9: Azure Blob + GCS are now real plugins via their
            // official cloud SDKs. Object-store semantics matching S3.
            Self::AzureBlob | Self::Gcs => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: false,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // key-prefix scoping
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: true,
                is_object_store: true,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 5_368_709_120, // 5 GiB (Azure single PUT, GCS resumable handles larger)
                consistency_model: "strong".into(),
            },
            Self::Mongodb => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: true,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // filter-prefix scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: true,
                supports_ttl: true,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true,
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 16_777_216, // 16 MiB (BSON doc limit)
                consistency_model: "causal".into(),
            },
            Self::Elasticsearch => BackendCapability {
                // C9: Elasticsearch is now a wired plugin.
                // Full Read/Write/Delete/Aggregate/Search/ResourceOp
                // via the ES Query DSL over reqwest. Tenant context
                // is protocol-enforced via `_tenant_id`/`_project_id`
                // term filters (BackendContextEnforcer reports
                // `Enforced`).
                supports_sql_ddl: false,
                supports_transactions: false, // ES is per-shard atomic; no cross-doc tx
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true,           // term-filter scoping
                supports_vector_search: true, // knn since 8.x
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true, // _bulk with stable _id is idempotent
                supports_schema_migration: false,
                supports_hybrid_search: true, // knn + multi_match in one query
                supports_resource_lifecycle: true,
                max_payload_bytes: 104_857_600, // 100 MiB (http.max_content_length)
                consistency_model: "eventual".into(),
            },
            // C9: Cassandra / ScyllaDB is now a real plugin via the
            // `scylla` driver. Wide-column store; CQL compiler with
            // PK-required filter validation.
            Self::Cassandra => BackendCapability {
                supports_sql_ddl: true,
                supports_transactions: false, // LWT only — per-row atomic, not multi-statement
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: false, // partition-key tenant convention (operator-modelled)
                supports_vector_search: false,
                supports_streaming: false,
                supports_ttl: true, // per-column TTL native
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: true, // INSERT IS upsert; LWT for IF NOT EXISTS
                supports_schema_migration: true,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "tunable".into(), // configurable per-query (ONE/QUORUM/ALL)
            },
            Self::Neo4j => BackendCapability {
                supports_sql_ddl: false,
                supports_transactions: true,
                supports_xa: false,
                supports_two_phase_commit: false,
                supports_rls: true, // Cypher-parameter scoping via BackendContextEnforcer
                supports_vector_search: false,
                supports_streaming: false,
                supports_ttl: false,
                is_object_store: false,
                is_migration_ledger_capable: false,
                supports_idempotency: false,
                supports_schema_migration: false,
                supports_hybrid_search: false,
                supports_resource_lifecycle: true,
                max_payload_bytes: 0,
                consistency_model: "strong".into(),
            },
        }
    }

    /// Returns the default environment variable name for the connection DSN.
    pub fn default_env_key(&self) -> &'static str {
        match self {
            Self::Postgres => "UDB_SQL_DSN",
            Self::Mysql => "UDB_MYSQL_DSN",
            Self::Sqlite => "UDB_SQLITE_DSN",
            Self::Mssql => "UDB_MSSQL_DSN",
            Self::Clickhouse => "UDB_COLUMN_DSN",
            Self::Redis => "UDB_CACHE_DSN",
            Self::Memcached => "UDB_MEMCACHED_DSN",
            Self::Qdrant => "UDB_VECTOR_DSN",
            Self::Weaviate => "UDB_WEAVIATE_DSN",
            Self::Pinecone => "UDB_PINECONE_DSN",
            Self::Minio => "UDB_OBJECT_DSN",
            Self::S3 => "UDB_S3_DSN",
            Self::AzureBlob => "UDB_AZUREBLOB_DSN",
            Self::Gcs => "UDB_GCS_DSN",
            Self::Mongodb => "UDB_NOSQL_DSN",
            Self::Elasticsearch => "UDB_ELASTIC_DSN",
            Self::Neo4j => "UDB_GRAPH_DSN",
            Self::Cassandra => "UDB_CASSANDRA_DSN",
        }
    }

    /// Returns the UDB DSN URI scheme for this backend.
    ///
    /// The full form is `udb+<tier>+<backend>://env:<ENV_KEY>/<resource>`.
    /// e.g. `udb+vector+qdrant://env:UDB_VECTOR_DSN/past_corrections`.
    pub fn dsn_scheme(&self) -> String {
        format!("udb+{}+{}", self.tier().as_str(), self.as_str())
    }

    /// Infer a `BackendKind` from a store-kind string produced by the proto parser.
    ///
    /// Maps `store_kind` values from `ManifestStore` / `UnifiedDsn` to the
    /// canonical backend.  Falls back to `None` for unrecognised values.
    pub fn from_store_kind(store_kind: &str, backend_hint: &str) -> Option<Self> {
        // Backend hint takes precedence when present.
        if !backend_hint.is_empty() {
            match backend_hint.to_lowercase().as_str() {
                "postgres" | "postgresql" => return Some(Self::Postgres),
                "mysql" | "mariadb" => return Some(Self::Mysql),
                "sqlite" => return Some(Self::Sqlite),
                "sqlserver" | "mssql" => return Some(Self::Mssql),
                "clickhouse" => return Some(Self::Clickhouse),
                "redis" => return Some(Self::Redis),
                "memcached" => return Some(Self::Memcached),
                "qdrant" => return Some(Self::Qdrant),
                "weaviate" => return Some(Self::Weaviate),
                "pinecone" => return Some(Self::Pinecone),
                "minio" => return Some(Self::Minio),
                "s3" => return Some(Self::S3),
                "azureblob" | "azure" => return Some(Self::AzureBlob),
                "gcs" => return Some(Self::Gcs),
                "mongodb" | "mongo" => return Some(Self::Mongodb),
                "elasticsearch" | "elastic" => return Some(Self::Elasticsearch),
                "neo4j" => return Some(Self::Neo4j),
                "cassandra" | "scylla" => return Some(Self::Cassandra),
                _ => {}
            }
        }
        // Fall back to tier default based on store_kind.
        match store_kind {
            "sql" | "relational" => Some(Self::Postgres),
            "cache" | "kv" | "key_value" | "key-value" | "keyvalue" => Some(Self::Redis),
            "vector" => Some(Self::Qdrant),
            "storage" | "object" | "blob" => Some(Self::Minio),
            "nosql" | "document" => Some(Self::Mongodb),
            "graph" => Some(Self::Neo4j),
            "timeseries" | "time-series" | "column" | "columnar" | "wide-column" => {
                Some(Self::Clickhouse)
            }
            "search" => Some(Self::Elasticsearch),
            _ => None,
        }
    }
}

// ── Storage tier ──────────────────────────────────────────────────────────────

/// The broad storage tier a backend belongs to.
/// Maps to the `store_kind` field in `ManifestStore` / `UnifiedDsn`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendTier {
    Sql,
    Cache,
    Vector,
    Object,
    Document,
    Graph,
    Column,
}

impl BackendTier {
    /// Returns the tier label used in DSN scheme and metric labels.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sql => "sql",
            Self::Cache => "cache",
            Self::Vector => "vector",
            Self::Object => "object",
            Self::Document => "document",
            Self::Graph => "graph",
            Self::Column => "column",
        }
    }
}

// ── Backend capabilities ──────────────────────────────────────────────────────

/// Feature flags for a backend, used by the APPLYING phase to determine which
/// provisioning operations are applicable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BackendCapability {
    /// Supports SQL DDL (`CREATE TABLE`, `ALTER TABLE`, etc.).
    pub supports_sql_ddl: bool,
    /// Supports ACID transactions.
    pub supports_transactions: bool,
    /// Supports XA-style distributed transaction coordination.
    pub supports_xa: bool,
    /// Supports two-phase commit / prepared transaction semantics.
    pub supports_two_phase_commit: bool,
    /// Supports row-level security policies (PostgreSQL-specific).
    pub supports_rls: bool,
    /// Supports vector similarity search (ANN).
    pub supports_vector_search: bool,
    /// Supports server-sent event streaming.
    pub supports_streaming: bool,
    /// Supports native TTL / expiry on records.
    pub supports_ttl: bool,
    /// Is an object / blob store (bucket + key semantics).
    pub is_object_store: bool,
    /// Can host the migration ledger tables (`schema_migrations`, etc.).
    /// Only `Postgres` is `true` here — the ledger always lives in the primary DB.
    pub is_migration_ledger_capable: bool,
    // ── Phase 4.1 extended capability matrix ─────────────────────────────────
    /// Backend guarantees idempotent writes (e.g. supports `ON CONFLICT` or
    /// equivalent upsert semantics with client-supplied idempotency keys).
    pub supports_idempotency: bool,
    /// Backend can apply schema migrations (DDL) driven by the migration apply
    /// engine — i.e. the migration apply engine is permitted to execute DDL on it.
    pub supports_schema_migration: bool,
    /// Backend supports hybrid (dense + sparse / keyword + vector) search.
    pub supports_hybrid_search: bool,
    /// Backend supports lifecycle management via `EnsureResource` /
    /// `DropResource` / `ListResources` admin RPCs.
    pub supports_resource_lifecycle: bool,
    /// Maximum payload size in bytes the backend can accept per write operation.
    /// `0` means unlimited / unknown.
    pub max_payload_bytes: u64,
    /// Consistency model advertised by the backend.
    /// Values: "strong", "eventual", "causal", "read-your-writes".
    pub consistency_model: String,
}

/// P2P — what role a backend plays in the UDB data plane.
///
/// Pre-P2P, the architecture was implicitly **Postgres-as-canonical +
/// everything else as projection target**. CDC tailed Postgres' WAL,
/// saga state lived in Postgres, migration audit lived in Postgres,
/// write receipts came from `pg_current_wal_lsn()`. That worked but
/// it meant "DB-agnostic" only at the read/write IR layer; the
/// orchestration layer was Postgres-bound.
///
/// `BackendRole` makes the distinction explicit so backends can
/// declare which side they play on:
///
/// - **`Canonical`** — the backend can host UDB's system tables
///   (`udb_outbox_events`, `udb_sagas`, `udb_projection_tasks`,
///   `udb_migration_runs`) AND can serve as a write durability
///   anchor (produces a token that fence/receipt logic can wait on).
///   Postgres, MySQL, SQLite, MongoDB.
/// - **`Projection`** — the backend is a write target downstream of a
///   canonical store. It cannot host system tables or produce a
///   durability token. Qdrant, Redis, S3, Memcached, object stores.
/// - **`Both`** — the backend can play either role. ClickHouse and
///   Neo4j are durable enough to host system tables (so they can be
///   canonical for some deployments) but also work well as projection
///   targets (analytics / graph views of canonical data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRole {
    Canonical,
    Projection,
    Both,
}

impl BackendRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Canonical => "canonical",
            Self::Projection => "projection",
            Self::Both => "both",
        }
    }
    /// Can this backend host the UDB system tables?
    pub fn can_host_system_tables(self) -> bool {
        matches!(self, Self::Canonical | Self::Both)
    }
    /// Can this backend serve as a write durability anchor (produce
    /// the token write-receipts and read-fences wait on)?
    pub fn can_be_durability_anchor(self) -> bool {
        matches!(self, Self::Canonical | Self::Both)
    }
    /// Can this backend receive projection writes?
    pub fn can_receive_projections(self) -> bool {
        matches!(self, Self::Projection | Self::Both)
    }
}

/// Machine-readable operation support for one backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilityMatrixEntry {
    pub backend: String,
    pub tier: String,
    pub operations: Vec<String>,
    pub unsupported_error_code: String,
    pub consistency_model: String,
    pub max_payload_bytes: u64,
    pub supports_xa: bool,
    pub supports_two_phase_commit: bool,
    /// P2P: the backend's role in the data plane. Pinned per backend
    /// kind in [`BackendKind::role`]. Surfaced through `udb doctor`
    /// and `GetCapabilities` so operators see which backends can
    /// host system tables.
    #[serde(default = "default_backend_role")]
    pub role: BackendRole,
}

fn default_backend_role() -> BackendRole {
    BackendRole::Projection
}

const OP_PING: &str = "ping";
const OP_PROBE: &str = "probe";
const OP_QUERY: &str = "query";
const OP_MUTATE: &str = "mutate";
const OP_TRANSACTION: &str = "transaction";
const OP_SEARCH: &str = "search";
const OP_GET_OBJECT: &str = "get_object";
const OP_PUT_OBJECT: &str = "put_object";
const OP_ENSURE_RESOURCE: &str = "ensure_resource";
const OP_DROP_RESOURCE: &str = "drop_resource";
const OP_LIST_RESOURCES: &str = "list_resources";

pub const UNSUPPORTED_OPERATION_CODE: &str = "UDB_UNSUPPORTED_OPERATION";

impl BackendKind {
    /// All recognised backend kinds in stable inventory order.
    pub fn all_known() -> &'static [BackendKind] {
        const ALL: &[BackendKind] = &[
            BackendKind::Postgres,
            BackendKind::Mysql,
            BackendKind::Sqlite,
            BackendKind::Mssql,
            BackendKind::Clickhouse,
            BackendKind::Redis,
            BackendKind::Memcached,
            BackendKind::Qdrant,
            BackendKind::Weaviate,
            BackendKind::Pinecone,
            BackendKind::Minio,
            BackendKind::S3,
            BackendKind::AzureBlob,
            BackendKind::Gcs,
            BackendKind::Mongodb,
            BackendKind::Elasticsearch,
            BackendKind::Neo4j,
            BackendKind::Cassandra,
        ];
        ALL
    }

    /// Operation tokens supported by the generic dispatch plane for this backend.
    pub fn supported_operations(&self) -> Vec<&'static str> {
        let cap = self.capabilities();
        let mut ops = vec![OP_PING, OP_PROBE];
        if cap.supports_resource_lifecycle {
            ops.extend([OP_ENSURE_RESOURCE, OP_DROP_RESOURCE, OP_LIST_RESOURCES]);
        }
        match self {
            Self::Postgres | Self::Clickhouse | Self::Mongodb | Self::Neo4j | Self::Redis => {
                ops.push(OP_QUERY);
            }
            _ => {}
        }
        match self {
            Self::Postgres
            | Self::Mongodb
            | Self::Neo4j
            | Self::Qdrant
            | Self::Clickhouse
            | Self::Redis => ops.push(OP_MUTATE),
            _ => {}
        }
        if cap.supports_transactions {
            ops.push(OP_TRANSACTION);
        }
        if cap.supports_vector_search || cap.supports_hybrid_search {
            ops.push(OP_SEARCH);
        }
        if cap.is_object_store {
            ops.extend([OP_GET_OBJECT, OP_PUT_OBJECT]);
        }
        ops.sort_unstable();
        ops.dedup();
        ops
    }

    pub fn supports_operation(&self, operation: &str) -> bool {
        self.supported_operations().contains(&operation)
    }

    pub fn capability_matrix_entry(&self) -> BackendCapabilityMatrixEntry {
        let cap = self.capabilities();
        BackendCapabilityMatrixEntry {
            backend: self.as_str().to_string(),
            tier: self.tier().as_str().to_string(),
            operations: self
                .supported_operations()
                .into_iter()
                .map(ToString::to_string)
                .collect(),
            unsupported_error_code: UNSUPPORTED_OPERATION_CODE.to_string(),
            consistency_model: cap.consistency_model,
            max_payload_bytes: cap.max_payload_bytes,
            supports_xa: cap.supports_xa,
            supports_two_phase_commit: cap.supports_two_phase_commit,
            // P2P: include the role so doctor + GetCapabilities show
            // which backends can host system tables.
            role: self.role(),
        }
    }
}

pub fn capability_matrix() -> Vec<BackendCapabilityMatrixEntry> {
    all_plugins()
        .into_iter()
        .map(|plugin| plugin.kind().capability_matrix_entry())
        .collect()
}

/// The four core storage backends mandated by UDB spec §16.1.
/// In order of spec listing.
pub const CORE_BACKENDS: [BackendKind; 4] = [
    BackendKind::Postgres,
    BackendKind::Redis,
    BackendKind::Qdrant,
    BackendKind::Minio,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_backends_have_correct_tiers() {
        assert_eq!(BackendKind::Postgres.tier(), BackendTier::Sql);
        assert_eq!(BackendKind::Redis.tier(), BackendTier::Cache);
        assert_eq!(BackendKind::Qdrant.tier(), BackendTier::Vector);
        assert_eq!(BackendKind::Minio.tier(), BackendTier::Object);
    }

    #[test]
    fn only_postgres_is_ledger_capable() {
        for b in &CORE_BACKENDS {
            let cap = b.capabilities();
            if *b == BackendKind::Postgres {
                assert!(
                    cap.is_migration_ledger_capable,
                    "postgres must be ledger capable"
                );
            } else {
                assert!(
                    !cap.is_migration_ledger_capable,
                    "{} must NOT be ledger capable",
                    b.as_str()
                );
            }
        }
    }

    #[test]
    fn dsn_scheme_follows_udb_convention() {
        assert_eq!(BackendKind::Postgres.dsn_scheme(), "udb+sql+postgres");
        assert_eq!(BackendKind::Redis.dsn_scheme(), "udb+cache+redis");
        assert_eq!(BackendKind::Qdrant.dsn_scheme(), "udb+vector+qdrant");
        assert_eq!(BackendKind::Minio.dsn_scheme(), "udb+object+minio");
    }

    #[test]
    fn from_store_kind_falls_back_to_tier_default() {
        assert_eq!(
            BackendKind::from_store_kind("sql", ""),
            Some(BackendKind::Postgres)
        );
        assert_eq!(
            BackendKind::from_store_kind("cache", ""),
            Some(BackendKind::Redis)
        );
        assert_eq!(
            BackendKind::from_store_kind("vector", ""),
            Some(BackendKind::Qdrant)
        );
        assert_eq!(
            BackendKind::from_store_kind("storage", ""),
            Some(BackendKind::Minio)
        );
    }

    #[test]
    fn from_store_kind_respects_backend_hint() {
        assert_eq!(
            BackendKind::from_store_kind("vector", "weaviate"),
            Some(BackendKind::Weaviate)
        );
        assert_eq!(
            BackendKind::from_store_kind("object", "s3"),
            Some(BackendKind::S3)
        );
    }

    #[test]
    fn wired_vector_backends_support_vector_search() {
        // Wired vector backends with real executors + compilers.
        // C9 ships Qdrant, Elasticsearch (knn since 8.x), Weaviate
        // (nearVector + bm25 hybrid), and Pinecone (vector-only).
        for b in [
            BackendKind::Qdrant,
            BackendKind::Elasticsearch,
            BackendKind::Weaviate,
            BackendKind::Pinecone,
        ] {
            assert!(
                b.capabilities().supports_vector_search,
                "{} must support vector search",
                b.as_str()
            );
        }
    }

    #[test]
    fn metadata_only_backends_advertise_no_capabilities() {
        // The remaining metadata-only backends (no plugin / executor /
        // compiler) must NOT advertise any active capability — that
        // would mislead the planner and GetCapabilities clients into
        // routing requests that have nowhere to go.
        //
        // C9 complete: all 8 metadata-only backends promoted to
        // real plugins. The previous assertion (specific kinds
        // advertise no capabilities) is replaced by a positive
        // assertion that every BackendKind now has at least one
        // honest capability flag set.
        use crate::backend::BackendKind;
        for b in [
            BackendKind::Postgres,
            BackendKind::Mysql,
            BackendKind::Sqlite,
            BackendKind::Mssql,
            BackendKind::Clickhouse,
            BackendKind::Redis,
            BackendKind::Memcached,
            BackendKind::Qdrant,
            BackendKind::Weaviate,
            BackendKind::Pinecone,
            BackendKind::Minio,
            BackendKind::S3,
            BackendKind::AzureBlob,
            BackendKind::Gcs,
            BackendKind::Mongodb,
            BackendKind::Elasticsearch,
            BackendKind::Neo4j,
            BackendKind::Cassandra,
        ] {
            let caps = b.capabilities();
            assert_ne!(
                caps.consistency_model,
                "metadata_only",
                "{} should NOT be metadata_only — all 18 backends wired in C9",
                b.as_str()
            );
        }
    }

    #[test]
    fn object_backends_are_object_stores() {
        for b in [
            BackendKind::Minio,
            BackendKind::S3,
            BackendKind::AzureBlob,
            BackendKind::Gcs,
        ] {
            assert!(
                b.capabilities().is_object_store,
                "{} must be object store",
                b.as_str()
            );
        }
    }
    #[test]
    fn test_capability_rejection() {
        let postgres = BackendKind::Postgres;
        assert!(postgres.capabilities().supports_sql_ddl);

        let redis = BackendKind::Redis;
        assert!(!redis.capabilities().supports_sql_ddl);
        assert!(!redis.capabilities().supports_vector_search);

        let qdrant = BackendKind::Qdrant;
        assert!(qdrant.capabilities().supports_vector_search);
        assert!(!qdrant.capabilities().supports_sql_ddl);
    }

    #[test]
    fn capability_matrix_lists_generic_dispatch_operations() {
        // C9 complete: every backend in the enum is now wired with
        // a real plugin / executor / compiler. No more metadata-only
        // exclusions.
        let matrix = capability_matrix();
        #[cfg(feature = "redis")]
        {
            let redis = matrix
                .iter()
                .find(|entry| entry.backend == "redis")
                .expect("redis matrix entry");
            assert!(redis.operations.contains(&"query".to_string()));
            assert!(redis.operations.contains(&"mutate".to_string()));
            assert_eq!(redis.unsupported_error_code, UNSUPPORTED_OPERATION_CODE);
        }

        #[cfg(feature = "qdrant")]
        {
            let qdrant = matrix
                .iter()
                .find(|entry| entry.backend == "qdrant")
                .expect("qdrant matrix entry");
            assert!(qdrant.operations.contains(&"search".to_string()));
            assert!(!qdrant.operations.contains(&"query".to_string()));
        }
    }

    // ── Serialization contract guard (refactor plan §9.2) ──────────────────────
    // Backend tokens are public contract: configs/*.yaml deserialize `backend:
    // <token>` straight into BackendKind, and `as_str()` is emitted into DSNs,
    // gRPC `target_backend`, and manifests. `as_str()` is hand-written and
    // SEPARATE from the serde derive — these tests pin both paths so neither can
    // drift silently. Do not "fix" a failure by changing a token; that breaks
    // user-authored configs.

    const ALL_KINDS: [BackendKind; 18] = [
        BackendKind::Postgres,
        BackendKind::Mysql,
        BackendKind::Sqlite,
        BackendKind::Mssql,
        BackendKind::Clickhouse,
        BackendKind::Redis,
        BackendKind::Memcached,
        BackendKind::Qdrant,
        BackendKind::Weaviate,
        BackendKind::Pinecone,
        BackendKind::Minio,
        BackendKind::S3,
        BackendKind::AzureBlob,
        BackendKind::Gcs,
        BackendKind::Mongodb,
        BackendKind::Elasticsearch,
        BackendKind::Neo4j,
        BackendKind::Cassandra,
    ];

    fn serde_token(b: &BackendKind) -> String {
        serde_json::to_string(b)
            .unwrap()
            .trim_matches('"')
            .to_string()
    }

    #[test]
    fn as_str_tokens_are_pinned() {
        // Exact, stable tokens. Changing any of these is a breaking change.
        let expected: [(BackendKind, &str); 18] = [
            (BackendKind::Postgres, "postgres"),
            (BackendKind::Mysql, "mysql"),
            (BackendKind::Sqlite, "sqlite"),
            (BackendKind::Mssql, "sqlserver"),
            (BackendKind::Clickhouse, "clickhouse"),
            (BackendKind::Redis, "redis"),
            (BackendKind::Memcached, "memcached"),
            (BackendKind::Qdrant, "qdrant"),
            (BackendKind::Weaviate, "weaviate"),
            (BackendKind::Pinecone, "pinecone"),
            (BackendKind::Minio, "minio"),
            (BackendKind::S3, "s3"),
            (BackendKind::AzureBlob, "azureblob"),
            (BackendKind::Gcs, "gcs"),
            (BackendKind::Mongodb, "mongodb"),
            (BackendKind::Elasticsearch, "elasticsearch"),
            (BackendKind::Neo4j, "neo4j"),
            (BackendKind::Cassandra, "cassandra"),
        ];
        for (kind, token) in expected {
            assert_eq!(kind.as_str(), token, "as_str token changed for {kind:?}");
        }
    }

    #[test]
    fn serde_round_trips_for_every_variant() {
        for kind in ALL_KINDS {
            let json = serde_json::to_string(&kind).unwrap();
            let back: BackendKind = serde_json::from_str(&json).unwrap();
            assert_eq!(back, kind, "serde round-trip failed for {kind:?}");
        }
    }

    #[test]
    fn config_backend_tokens_agree_across_as_str_and_serde() {
        // The backends actually used in configs/*.yaml MUST have identical
        // as_str() and serde tokens, otherwise YAML config and DSN/wire emission
        // would disagree.
        for kind in [
            BackendKind::Postgres,
            BackendKind::Redis,
            BackendKind::Qdrant,
            BackendKind::Clickhouse,
            BackendKind::Mongodb,
            BackendKind::Neo4j,
            BackendKind::Minio,
            BackendKind::S3,
        ] {
            assert_eq!(
                kind.as_str(),
                serde_token(&kind),
                "as_str() and serde token diverged for config backend {kind:?}"
            );
        }
    }

    #[test]
    fn from_token_is_exact_inverse_of_as_str() {
        for kind in ALL_KINDS {
            assert_eq!(
                BackendKind::from_token(kind.as_str()),
                Some(kind.clone()),
                "from_token(as_str()) must round-trip for {kind:?}"
            );
        }
    }

    #[test]
    fn from_token_is_case_insensitive_and_rejects_unknown() {
        assert_eq!(
            BackendKind::from_token("POSTGRES"),
            Some(BackendKind::Postgres)
        );
        assert_eq!(
            BackendKind::from_token("  Qdrant "),
            Some(BackendKind::Qdrant)
        );
        // `sqlserver` is the canonical token for Mssql (matches as_str, not serde).
        assert_eq!(
            BackendKind::from_token("sqlserver"),
            Some(BackendKind::Mssql)
        );
        assert_eq!(BackendKind::from_token("mssql"), None);
        assert_eq!(BackendKind::from_token("not_a_backend"), None);
        assert_eq!(BackendKind::from_token(""), None);
    }

    #[test]
    fn known_as_str_vs_serde_divergences_are_locked() {
        // These variants intentionally differ between as_str() and serde. Pinned
        // so the divergence is documented and cannot change unnoticed.
        assert_eq!(BackendKind::Mssql.as_str(), "sqlserver");
        assert_eq!(serde_token(&BackendKind::Mssql), "mssql");
        assert_eq!(BackendKind::AzureBlob.as_str(), "azureblob");
        assert_eq!(serde_token(&BackendKind::AzureBlob), "azure_blob");
    }
}
