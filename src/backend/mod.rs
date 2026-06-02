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
            // Canonical: engines with a concrete `SystemStores` canonical-store
            // implementation (outbox + advisory leases + saga/audit/migration
            // stores) AND a queryable write-progress token. Only these can host
            // the system catalog and act as the write-durability anchor — they
            // are the backends `register_full_canonical_store` actually registers
            // (Postgres/MySQL/SQLite, and SQL Server via the Tiberius canonical
            // store — B.8, conformance-verified on real SQL Server 2022). (#129)
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql => BackendRole::Canonical,
            // B.14: Redis has a native `SystemStores` implementation using
            // Redis atomic primitives. Runtime registration is still refused
            // unless the server exposes the durable AOF profile.
            #[cfg(feature = "redis")]
            Self::Redis => BackendRole::Canonical,
            #[cfg(not(feature = "redis"))]
            Self::Redis => BackendRole::Projection,
            // B.9: native MongoDB (replica set / sharded cluster) has a real
            // `SystemStores` canonical store — but only when built with the
            // `mongodb-native` feature. The scalar / Data-API build keeps it
            // Projection (no store compiled). Runtime registration further gates
            // on live topology (standalone mongod stays projection).
            #[cfg(feature = "mongodb-native")]
            Self::Mongodb => BackendRole::Canonical,
            #[cfg(not(feature = "mongodb-native"))]
            Self::Mongodb => BackendRole::Projection,
            // B.10a: Cassandra has a native CQL + LWT `SystemStores` canonical
            // store (full 5-contract conformance verified live). Canonical only
            // in `cassandra` builds; otherwise no store is compiled → Projection.
            #[cfg(feature = "cassandra")]
            Self::Cassandra => BackendRole::Canonical,
            #[cfg(not(feature = "cassandra"))]
            Self::Cassandra => BackendRole::Projection,
            // B.10b: Neo4j has a native HTTP-transactional-Cypher `SystemStores`
            // canonical store (full 5-contract conformance verified live).
            // Canonical in `neo4j` builds; otherwise no store is compiled.
            #[cfg(feature = "neo4j")]
            Self::Neo4j => BackendRole::Canonical,
            #[cfg(not(feature = "neo4j"))]
            Self::Neo4j => BackendRole::Projection,
            // B.11: Qdrant now ships a native SystemStores adapter in qdrant
            // builds, stored in a dedicated system collection with deterministic
            // point IDs and strongly ordered writes.
            #[cfg(feature = "qdrant")]
            Self::Qdrant => BackendRole::Canonical,
            #[cfg(not(feature = "qdrant"))]
            Self::Qdrant => BackendRole::Projection,
            #[cfg(feature = "pinecone")]
            Self::Pinecone => BackendRole::Canonical,
            #[cfg(not(feature = "pinecone"))]
            Self::Pinecone => BackendRole::Projection,
            #[cfg(feature = "weaviate")]
            Self::Weaviate => BackendRole::Canonical,
            #[cfg(not(feature = "weaviate"))]
            Self::Weaviate => BackendRole::Projection,
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch => BackendRole::Canonical,
            #[cfg(not(feature = "elasticsearch"))]
            Self::Elasticsearch => BackendRole::Projection,
            // B.10c: ClickHouse has a native HTTP `SystemStores` canonical store
            // (ReplacingMergeTree(version) + SELECT … FINAL versioned-CAS; full
            // 5-contract conformance verified live). Canonical in `clickhouse`
            // builds; the registration carries a documented single-writer caveat.
            #[cfg(feature = "clickhouse")]
            Self::Clickhouse => BackendRole::Canonical,
            #[cfg(not(feature = "clickhouse"))]
            Self::Clickhouse => BackendRole::Projection,
            // Projection: read/write targets that do NOT implement a canonical
            // `SystemStores` backing.
            Self::Memcached | Self::Minio | Self::S3 | Self::AzureBlob | Self::Gcs => {
                BackendRole::Projection
            }
        }
    }

    /// Returns the V1 capability flags for this backend.
    ///
    /// B.1: V1 is now a pure projection of the V2 model. The per-backend feature
    /// truths still live in the big match below (now `capabilities_v1_fields`),
    /// but the resource-lifecycle boolean and the new dimensions flow through
    /// [`Self::capabilities_v2`] so the two views can never disagree.
    pub fn capabilities(&self) -> BackendCapability {
        self.capabilities_v2().derive_v1()
    }

    /// B.1: the per-backend feature truths (everything except the lifecycle /
    /// native-executor / compiler / system-store dimensions, which
    /// [`Self::capabilities_v2`] supplies). Kept private; callers use
    /// `capabilities()` (V1) or `capabilities_v2()` (V2).
    fn capabilities_v1_fields(&self) -> BackendCapability {
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

    /// B.1 — the V2 capability model: orthogonal dimensions per backend.
    ///
    /// V1 derives from this (see [`Self::capabilities`]). The per-backend feature
    /// truths come from [`Self::capabilities_v1_fields`]; this method layers on
    /// the five V2 dimensions (native executor, compiler-mediated, dispatch
    /// admission, resource lifecycle kind, canonical system-store).
    ///
    /// CRITICAL: the lifecycle classification here is evidence-backed against the
    /// concrete `ResourceAdminExecutor` impls under `runtime/executors/*`:
    ///   - `CatalogMigration` — native ensure/drop return `failed_precondition`,
    ///     lifecycle is via catalog migrations (Postgres/MySQL/SQLite).
    ///   - `CompilerMediated` — native ensure/drop return `unimplemented`; DDL is
    ///     via `compile_resource_op` + `mutate` (MSSQL, Cassandra).
    ///   - `Native` — native ensure/drop do the I/O directly (object stores,
    ///     Qdrant, MongoDB, Neo4j, ClickHouse, Weaviate, Pinecone, Elasticsearch).
    ///   - `None` — no resource lifecycle at all (Redis, Memcached).
    pub fn capabilities_v2(&self) -> BackendCapabilityV2 {
        let v1 = self.capabilities_v1_fields();
        let dispatch_operations = self.supported_operations();
        let lifecycle = self.lifecycle_support();
        let system_store = self.system_store_support();
        let canonical_candidate = self.canonical_candidate_profile();
        let canonical_goal = self.canonical_promotion_goal();
        // `consistency_model` in V1 is an owned String; the V2 view keeps the
        // canonical static token. Map the known values back to 'static strs.
        let consistency_model: &'static str = match v1.consistency_model.as_str() {
            "strong" => "strong",
            "eventual" => "eventual",
            "causal" => "causal",
            "tunable" => "tunable",
            "read-your-writes" => "read-your-writes",
            // Unknown / future tokens: keep the V1 string honest by not silently
            // mapping it to a wrong canonical token.
            _ => "unknown",
        };
        BackendCapabilityV2 {
            // Native executor support == has a compiled-in runtime plugin (the
            // dispatch factory + executor live behind the same feature gate).
            native_executor: crate::backend::has_runtime_implementation(self),
            // Compiler-mediated support: every backend with an `ir::compile::*`
            // dialect. SQL/CQL DDL backends and the document/graph/vector
            // backends all lower neutral ops through a compiler. KV stores
            // (Redis/Memcached) and pure object stores have no logical compiler.
            compiler_mediated: !matches!(
                self,
                Self::Redis
                    | Self::Memcached
                    | Self::Minio
                    | Self::S3
                    | Self::AzureBlob
                    | Self::Gcs
            ),
            dispatch_operations,
            lifecycle,
            system_store,
            canonical_candidate,
            canonical_goal,
            supports_sql_ddl: v1.supports_sql_ddl,
            supports_transactions: v1.supports_transactions,
            supports_xa: v1.supports_xa,
            supports_two_phase_commit: v1.supports_two_phase_commit,
            supports_rls: v1.supports_rls,
            supports_vector_search: v1.supports_vector_search,
            supports_streaming: v1.supports_streaming,
            supports_ttl: v1.supports_ttl,
            is_object_store: v1.is_object_store,
            is_migration_ledger_capable: v1.is_migration_ledger_capable,
            supports_idempotency: v1.supports_idempotency,
            supports_schema_migration: v1.supports_schema_migration,
            supports_hybrid_search: v1.supports_hybrid_search,
            max_payload_bytes: v1.max_payload_bytes,
            consistency_model,
        }
    }

    /// B.1 — how this backend implements resource lifecycle. Evidence-backed
    /// against `runtime/executors/*::ResourceAdminExecutor` (read-only):
    /// see the `capabilities_v2` doc comment for the per-class rationale.
    pub fn lifecycle_support(&self) -> LifecycleSupport {
        match self {
            // Native ensure/drop return failed_precondition; lifecycle is via
            // catalog migrations. Generic dispatch still admits the ops.
            Self::Postgres | Self::Mysql | Self::Sqlite => LifecycleSupport::CatalogMigration,
            // Native ensure/drop return `unimplemented`; DDL is compiler-mediated
            // (`compile_resource_op` + mutate). `list_resources` is a native
            // catalog query, but ensure/drop are NOT native.
            Self::Mssql | Self::Cassandra => LifecycleSupport::CompilerMediated,
            // No buckets / namespaces — no lifecycle at all.
            Self::Redis | Self::Memcached => LifecycleSupport::None,
            // Native ensure/drop do real I/O (buckets, collections, indexes,
            // constraints, tables).
            Self::Minio
            | Self::S3
            | Self::AzureBlob
            | Self::Gcs
            | Self::Qdrant
            | Self::Weaviate
            | Self::Pinecone
            | Self::Mongodb
            | Self::Elasticsearch
            | Self::Neo4j
            | Self::Clickhouse => LifecycleSupport::Native,
        }
    }

    /// B.1 / B.2 — does this backend have a concrete canonical `SystemStores`
    /// implementation registered in this source tree? This is the capability-
    /// level mirror of [`Self::role`]; the two MUST agree (asserted in tests).
    /// Kept in lock-step with `CANONICAL_SYSTEM_STORE_BACKENDS`.
    pub fn system_store_support(&self) -> SystemStoreSupport {
        if CANONICAL_SYSTEM_STORE_BACKENDS.contains(self) {
            SystemStoreSupport::Full
        } else {
            SystemStoreSupport::None
        }
    }

    /// B.12-B.14 — machine-readable canonical roadmap class for every backend.
    ///
    /// This does not promote a backend. Promotion still requires a concrete
    /// `SystemStores` implementation and allowlist evidence. The profile makes
    /// the capability matrix explicit about which canonical proof remains.
    pub fn canonical_candidate_profile(&self) -> CanonicalCandidateProfile {
        match self {
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql => {
                CanonicalCandidateProfile::Implemented
            }
            #[cfg(feature = "redis")]
            Self::Redis => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "redis"))]
            Self::Redis => CanonicalCandidateProfile::CacheDurabilityRequired,
            #[cfg(feature = "mongodb-native")]
            Self::Mongodb => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "mongodb-native"))]
            Self::Mongodb => CanonicalCandidateProfile::NativeDriverRequired,
            // B.10a: Cassandra implemented (native CQL + LWT canonical store) in
            // `cassandra` builds; semantics-gated candidate otherwise.
            #[cfg(feature = "cassandra")]
            Self::Cassandra => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "cassandra"))]
            Self::Cassandra => CanonicalCandidateProfile::SemanticsGated,
            // B.10b: Neo4j implemented (native HTTP-transactional Cypher canonical
            // store) in `neo4j` builds; semantics-gated candidate otherwise.
            #[cfg(feature = "neo4j")]
            Self::Neo4j => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "neo4j"))]
            Self::Neo4j => CanonicalCandidateProfile::SemanticsGated,
            // B.10c: ClickHouse implemented (ReplacingMergeTree versioned-CAS
            // canonical store) in `clickhouse` builds; semantics-gated otherwise.
            #[cfg(feature = "clickhouse")]
            Self::Clickhouse => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "clickhouse"))]
            Self::Clickhouse => CanonicalCandidateProfile::SemanticsGated,
            #[cfg(feature = "qdrant")]
            Self::Qdrant => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "qdrant"))]
            Self::Qdrant => CanonicalCandidateProfile::VectorNativeCasUnsupported,
            #[cfg(feature = "pinecone")]
            Self::Pinecone => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "pinecone"))]
            Self::Pinecone => CanonicalCandidateProfile::VectorFeasibilityRequired,
            #[cfg(feature = "weaviate")]
            Self::Weaviate => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "weaviate"))]
            Self::Weaviate => CanonicalCandidateProfile::VectorFeasibilityRequired,
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch => CanonicalCandidateProfile::Implemented,
            #[cfg(not(feature = "elasticsearch"))]
            Self::Elasticsearch => CanonicalCandidateProfile::VectorFeasibilityRequired,
            Self::Minio | Self::S3 | Self::AzureBlob | Self::Gcs => {
                CanonicalCandidateProfile::ObjectConditionalWrites
            }
            Self::Memcached => CanonicalCandidateProfile::ExplicitlyNotSupported,
        }
    }

    /// B.12-B.15 — operator-visible goal that must be satisfied before this
    /// backend can claim canonical status, or the conformance status for
    /// already-canonical backends.
    pub fn canonical_promotion_goal(&self) -> &'static str {
        match self {
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql => {
                "canonical store implemented; keep five-contract conformance live"
            }
            #[cfg(feature = "redis")]
            Self::Redis => {
                "B.14: native Redis SystemStores implemented; runtime registration requires durable AOF profile and live conformance"
            }
            #[cfg(not(feature = "redis"))]
            Self::Redis => {
                "B.14: compile Redis native SystemStores and require durable AOF profile before canonical promotion"
            }
            #[cfg(feature = "mongodb-native")]
            Self::Mongodb => {
                "canonical store implemented for native MongoDB replica-set/sharded topology; keep topology-gated conformance live"
            }
            #[cfg(not(feature = "mongodb-native"))]
            Self::Mongodb => {
                "B.9: compile native MongoDB SystemStores and prove replica-set/sharded durability; scalar/Data-API builds remain projection"
            }
            Self::Clickhouse => {
                "B.10: prove canonical semantics for leases, outbox, saga, audit, and migration stores or keep append analytics projection"
            }
            Self::Neo4j => {
                "B.10: implement graph-native SystemStores with transactional claims, idempotent audit, and recoverable saga/outbox state"
            }
            Self::Cassandra => {
                "B.10: implement wide-column SystemStores with compare-and-set claims, quorum guidance, and replay-safe progress tokens"
            }
            #[cfg(feature = "qdrant")]
            Self::Qdrant => {
                "B.11: native Qdrant SystemStores implemented with a dedicated system collection, deterministic point IDs, strongly ordered writes, and live conformance under UDB_QDRANT_URL"
            }
            #[cfg(not(feature = "qdrant"))]
            Self::Qdrant => {
                "B.11: compile the native Qdrant SystemStores adapter and prove live conformance before canonical promotion"
            }
            #[cfg(feature = "pinecone")]
            Self::Pinecone => {
                "B.12: native Pinecone SystemStores implemented in the shared vector system adapter; keep live conformance under UDB_PINECONE_DSN"
            }
            #[cfg(not(feature = "pinecone"))]
            Self::Pinecone => {
                "B.12: compile the Pinecone vector SystemStores adapter and prove live conformance"
            }
            #[cfg(feature = "weaviate")]
            Self::Weaviate => {
                "B.12: native Weaviate SystemStores implemented in the shared vector system adapter; keep live conformance under UDB_WEAVIATE_DSN"
            }
            #[cfg(not(feature = "weaviate"))]
            Self::Weaviate => {
                "B.12: compile the Weaviate vector SystemStores adapter and prove live conformance"
            }
            #[cfg(feature = "elasticsearch")]
            Self::Elasticsearch => {
                "B.12: native Elasticsearch SystemStores implemented in the shared vector system adapter; keep live conformance under UDB_ELASTIC_DSN"
            }
            #[cfg(not(feature = "elasticsearch"))]
            Self::Elasticsearch => {
                "B.12: compile the Elasticsearch vector/search SystemStores adapter and prove live conformance"
            }
            Self::Minio | Self::S3 | Self::AzureBlob | Self::Gcs => {
                "B.13: prove object-store canonical profile using conditional writes, generation/etag fencing, listing recovery, and multipart safety"
            }
            Self::Memcached => {
                "B.14: explicitly unsupported for canonical state unless a durable backend profile is added and conformance-proven"
            }
        }
    }

    /// B.13-B.15 — the structured canonical feasibility profile for this backend.
    ///
    /// This expands [`Self::canonical_candidate_profile`] into the concrete proof
    /// dimensions (atomic claims, ordered progress, tenant isolation, read fence)
    /// plus operator prerequisites and remaining gaps. Object-store and cache/KV
    /// families (B.13/B.14) are the primary consumers, but every backend returns
    /// a profile so no capability-matrix row is a silent roadmap island (B.15).
    pub fn canonical_feasibility_profile(&self) -> CanonicalFeasibilityProfile {
        let backend = self.as_str();
        let candidate = self.canonical_candidate_profile();
        let implemented = self.system_store_support().is_canonical();
        match self {
            // ── SQL canonical family (implemented) ──────────────────────────
            Self::Postgres | Self::Mysql | Self::Sqlite | Self::Mssql => {
                CanonicalFeasibilityProfile {
                    backend,
                    family: "sql",
                    candidate,
                    implemented,
                    atomic_claim_strategy: "row-level UPDATE ... WHERE owner IS NULL guarded by a transactional lock",
                    ordered_progress_strategy: "monotonic outbox sequence column under transactional isolation",
                    tenant_isolation_strategy: "tenant_id column + RLS / session-context enforcement",
                    read_fence_strategy: "transactional write-progress token (LSN / GTID / rowversion)",
                    durability_prerequisites: &[],
                    blocking_gaps: &[],
                    live_conformance_env: Some(match self {
                        Self::Mysql => "UDB_MYSQL_DSN",
                        Self::Mssql => "UDB_MSSQL_DSN",
                        _ => "UDB_PG_DSN",
                    }),
                }
            }
            // ── Document family (implemented only under mongodb-native) ──────
            Self::Mongodb => CanonicalFeasibilityProfile {
                backend,
                family: "document",
                candidate,
                implemented,
                atomic_claim_strategy: "findOneAndUpdate with an owner guard (single-document atomic CAS)",
                ordered_progress_strategy: "monotonic counter document + change-stream resume token",
                tenant_isolation_strategy: "tenant_id field + per-collection scoping",
                read_fence_strategy: "majority read-concern resume token / cluster time",
                durability_prerequisites: &[
                    "replica set or sharded cluster with majority write concern",
                ],
                blocking_gaps: if implemented {
                    &[]
                } else {
                    &[
                        "mongodb-native feature not compiled — Data-API/scalar build stays projection",
                    ]
                },
                live_conformance_env: Some("UDB_MONGODB_DSN"),
            },
            // ── Column / graph families (semantics-gated) ───────────────────
            Self::Clickhouse => CanonicalFeasibilityProfile {
                backend,
                family: "column",
                candidate,
                implemented,
                atomic_claim_strategy: "none native — append-only engine has no row update/lock for CAS claims",
                ordered_progress_strategy: "insert-block ordering only (eventual)",
                tenant_isolation_strategy: "session-setting scoping (no row policy)",
                read_fence_strategy: "none proven — eventual visibility of inserted blocks",
                durability_prerequisites: &[],
                // B.10c: native HTTP canonical store implemented via
                // ReplacingMergeTree(version) + SELECT … FINAL versioned-CAS
                // (full 5-contract conformance verified live) in `clickhouse`
                // builds → no blocking gaps; the remaining concern (multi-writer
                // CAS atomicity) is a documented runtime caveat, not a missing
                // store. Otherwise the candidate gaps stand.
                blocking_gaps: if cfg!(feature = "clickhouse") {
                    &[]
                } else {
                    &[
                        "no atomic claim/lease primitive",
                        "no multi-statement transaction for saga/outbox",
                        "no canonical SystemStores module for column store",
                    ]
                },
                live_conformance_env: Some("UDB_COLUMN_DSN"),
            },
            Self::Neo4j => CanonicalFeasibilityProfile {
                backend,
                family: "graph",
                candidate,
                implemented,
                atomic_claim_strategy: "MERGE under a write transaction with a node lock",
                ordered_progress_strategy: "sequence node + transaction commit order",
                tenant_isolation_strategy: "tenant property + label scoping",
                read_fence_strategy: "transaction bookmark",
                durability_prerequisites: &[],
                // B.10b: graph-native SystemStores implemented (HTTP-transactional
                // Cypher; full 5-contract conformance verified live) in `neo4j`
                // builds → no blocking gaps; otherwise the candidate gap stands.
                blocking_gaps: if cfg!(feature = "neo4j") {
                    &[]
                } else {
                    &["graph-native SystemStores (leases/outbox/saga/audit) not implemented"]
                },
                live_conformance_env: Some("UDB_GRAPH_DSN"),
            },
            Self::Cassandra => CanonicalFeasibilityProfile {
                backend,
                family: "column",
                candidate,
                implemented,
                atomic_claim_strategy: "lightweight transaction compare-and-set (IF clause, Paxos)",
                ordered_progress_strategy: "monotonic timeuuid under quorum writes",
                tenant_isolation_strategy: "tenant partition-key prefix",
                read_fence_strategy: "LWT applied flag at quorum",
                durability_prerequisites: &[
                    "QUORUM/LOCAL_QUORUM read+write consistency configured",
                ],
                // B.10a: native CQL + LWT SystemStores implemented (full
                // 5-contract conformance verified live) in `cassandra` builds →
                // no blocking gaps; otherwise the candidate gaps stand.
                blocking_gaps: if cfg!(feature = "cassandra") {
                    &[]
                } else {
                    &[
                        "wide-column SystemStores not implemented",
                        "quorum operator guidance + replay-safe progress token unproven",
                    ]
                },
                live_conformance_env: Some("UDB_CASSANDRA_DSN"),
            },
            // ── Vector / search families ────────────────────────────────────
            Self::Qdrant => CanonicalFeasibilityProfile {
                backend,
                family: "vector",
                candidate,
                implemented,
                atomic_claim_strategy: "dedicated system collection + deterministic point IDs + strongly ordered point writes; adapter serializes read-modify-write system operations through the canonical store",
                ordered_progress_strategy: "monotonic outbox sequence document persisted as a Qdrant point under strong write ordering",
                tenant_isolation_strategy: "dedicated system collection per UDB instance plus tenant/project fields inside system records",
                read_fence_strategy: "outbox sequence durability token stored in the system collection and polled by wait_for_token",
                durability_prerequisites: VECTOR_CANONICAL_PLANE_PREREQS,
                blocking_gaps: if implemented {
                    &[]
                } else {
                    QDRANT_BLOCKING_GAPS
                },
                live_conformance_env: Some("UDB_QDRANT_URL"),
            },
            Self::Pinecone => CanonicalFeasibilityProfile {
                backend,
                family: "vector",
                candidate,
                implemented,
                atomic_claim_strategy: "dedicated namespace + deterministic vector IDs; adapter serializes read-modify-write system operations through the canonical store",
                ordered_progress_strategy: "monotonic outbox sequence metadata record persisted through Pinecone vector upsert",
                tenant_isolation_strategy: "dedicated namespace per UDB instance plus tenant/project fields inside system records",
                read_fence_strategy: "outbox sequence durability token stored in the namespace and polled by wait_for_token",
                durability_prerequisites: VECTOR_CANONICAL_PLANE_PREREQS,
                blocking_gaps: if implemented {
                    &[]
                } else {
                    PINECONE_BLOCKING_GAPS
                },
                live_conformance_env: Some("UDB_PINECONE_DSN"),
            },
            Self::Weaviate => CanonicalFeasibilityProfile {
                backend,
                family: "vector",
                candidate,
                implemented,
                atomic_claim_strategy: "dedicated class + deterministic object UUIDs; adapter serializes read-modify-write system operations through the canonical store",
                ordered_progress_strategy: "monotonic outbox sequence object persisted through Weaviate object upsert",
                tenant_isolation_strategy: "dedicated system class per UDB instance plus tenant/project fields inside system records",
                read_fence_strategy: "outbox sequence durability token stored in the class and polled by wait_for_token",
                durability_prerequisites: VECTOR_CANONICAL_PLANE_PREREQS,
                blocking_gaps: if implemented {
                    &[]
                } else {
                    WEAVIATE_BLOCKING_GAPS
                },
                live_conformance_env: Some("UDB_WEAVIATE_DSN"),
            },
            Self::Elasticsearch => CanonicalFeasibilityProfile {
                backend,
                family: "vector",
                candidate,
                implemented,
                atomic_claim_strategy: "dedicated system index + deterministic document IDs; adapter serializes read-modify-write system operations through the canonical store",
                ordered_progress_strategy: "monotonic outbox sequence document persisted with refresh=wait_for",
                tenant_isolation_strategy: "dedicated system index per UDB instance plus tenant/project fields inside system records",
                read_fence_strategy: "outbox sequence durability token stored in the index and polled by wait_for_token",
                durability_prerequisites: VECTOR_CANONICAL_PLANE_PREREQS,
                blocking_gaps: if implemented {
                    &[]
                } else {
                    ELASTICSEARCH_VECTOR_BLOCKING_GAPS
                },
                live_conformance_env: Some("UDB_ELASTIC_DSN"),
            },
            // ── B.13 object-store family ────────────────────────────────────
            Self::S3 | Self::Minio => CanonicalFeasibilityProfile {
                backend,
                family: "object",
                candidate,
                implemented,
                atomic_claim_strategy: "conditional PUT via If-None-Match / If-Match ETag + version-id",
                ordered_progress_strategy: "no native cross-object sequence — needs single-writer or external sequencer profile",
                tenant_isolation_strategy: "tenant/project key prefix + bucket policy",
                read_fence_strategy: "ETag / version-id per object",
                durability_prerequisites: OBJECT_DURABILITY_PREREQS,
                blocking_gaps: OBJECT_BLOCKING_GAPS,
                live_conformance_env: Some("UDB_BENCH_S3_ENDPOINT"),
            },
            Self::AzureBlob => CanonicalFeasibilityProfile {
                backend,
                family: "object",
                candidate,
                implemented,
                atomic_claim_strategy: "blob lease + If-Match ETag conditional write",
                ordered_progress_strategy: "no native cross-blob sequence — needs single-writer or external sequencer profile",
                tenant_isolation_strategy: "tenant/project key prefix + container policy",
                read_fence_strategy: "ETag per blob",
                durability_prerequisites: OBJECT_DURABILITY_PREREQS,
                blocking_gaps: OBJECT_BLOCKING_GAPS,
                live_conformance_env: Some("UDB_BENCH_AZURE_BLOB"),
            },
            Self::Gcs => CanonicalFeasibilityProfile {
                backend,
                family: "object",
                candidate,
                implemented,
                atomic_claim_strategy: "x-goog-if-generation-match generation precondition on write",
                ordered_progress_strategy: "no native cross-object sequence — needs single-writer or external sequencer profile",
                tenant_isolation_strategy: "tenant/project key prefix + bucket IAM",
                read_fence_strategy: "object generation number",
                durability_prerequisites: OBJECT_DURABILITY_PREREQS,
                blocking_gaps: OBJECT_BLOCKING_GAPS,
                live_conformance_env: Some("UDB_BENCH_GCS"),
            },
            // ── B.14 cache/KV family ────────────────────────────────────────
            Self::Redis => CanonicalFeasibilityProfile {
                backend,
                family: "cache",
                candidate,
                implemented,
                atomic_claim_strategy: "Lua / MULTI compare-and-set on an owner key with a TTL lease",
                ordered_progress_strategy: "INCR sequence + per-record JSON documents (Streams-ready)",
                tenant_isolation_strategy: "udb:system:{instance} key-prefix namespacing",
                read_fence_strategy: "INCR durability token gated on AOF fsync",
                durability_prerequisites: REDIS_DURABILITY_PREREQS,
                blocking_gaps: if implemented {
                    &[]
                } else {
                    &["redis feature not compiled — native SystemStores absent"]
                },
                live_conformance_env: Some("UDB_INTEGRATION_REDIS_URL"),
            },
            Self::Memcached => CanonicalFeasibilityProfile {
                backend,
                family: "cache",
                candidate,
                implemented,
                atomic_claim_strategy: "single-key CAS token only — no multi-key claim",
                ordered_progress_strategy: "none — no durable log or scan to recover outbox order",
                tenant_isolation_strategy: "key-prefix only (no enumeration to audit)",
                read_fence_strategy: "none — no durable write-progress token",
                durability_prerequisites: &[
                    "a durable persistence layer Memcached does not provide",
                ],
                blocking_gaps: MEMCACHED_BLOCKING_GAPS,
                live_conformance_env: None,
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

// ── B.1 V2 capability model ───────────────────────────────────────────────────
//
// The V1 `BackendCapability` flags conflated several independent truths into one
// boolean per feature — most damagingly `supports_resource_lifecycle`, which was
// read both as "generic dispatch admits ensure/drop/list" AND as "the backend has
// a working native `ResourceAdminExecutor`". Those are different facts: MSSQL and
// Cassandra advertise lifecycle and admit the dispatch ops, but their native
// `ResourceAdminExecutor::{ensure,drop}_resource` return `unimplemented` — the
// real work goes through the dialect compiler (`compile_resource_op`) + `mutate`.
//
// `BackendCapabilityV2` splits the truth into orthogonal dimensions per backend.
// V1 is DERIVED from V2 (see `BackendCapabilityV2::derive_v1`) so existing callers
// of `BackendCapability` / `capabilities()` / `capability_matrix()` are unchanged.

/// How a backend implements resource lifecycle (`EnsureResource` / `DropResource`
/// / `ListResources`).
///
/// This is the dimension that B.1 separates out of the V1
/// `supports_resource_lifecycle` boolean: that flag answered "does the backend
/// expose lifecycle at all", but said nothing about *how*. Native and
/// compiler-mediated lifecycle have very different failure modes — a native
/// executor does the I/O directly, while a compiler-mediated one is only reachable
/// through `compile_resource_op` + `mutate` and its `ResourceAdminExecutor`
/// methods deliberately return `unimplemented`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleSupport {
    /// No resource lifecycle (KV/cache stores with no buckets/namespaces).
    None,
    /// Lifecycle is driven through the dialect compiler's `compile_resource_op`
    /// plus a `mutate` call. The backend's native `ResourceAdminExecutor`
    /// `ensure_resource`/`drop_resource` return `unimplemented` on purpose
    /// (MSSQL, Cassandra). `list_resources` may still be a native catalog query.
    CompilerMediated,
    /// The backend implements native `ResourceAdminExecutor::{ensure,drop}_resource`
    /// that perform the I/O directly (object stores, Qdrant, MongoDB, Neo4j,
    /// ClickHouse, Weaviate, Pinecone, Elasticsearch).
    Native,
    /// Lifecycle for the relational system catalog is managed out-of-band via
    /// catalog migrations, not the per-request executor. The native
    /// `ResourceAdminExecutor` methods return `failed_precondition`
    /// (Postgres/MySQL/SQLite). Generic dispatch still admits the ops.
    CatalogMigration,
}

impl LifecycleSupport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CompilerMediated => "compiler_mediated",
            Self::Native => "native",
            Self::CatalogMigration => "catalog_migration",
        }
    }
    /// Does generic dispatch admit `ensure_resource`/`drop_resource`/
    /// `list_resources` for this backend? True for every variant except `None`.
    /// This is the V1 `supports_resource_lifecycle` projection.
    pub fn admits_dispatch(self) -> bool {
        !matches!(self, Self::None)
    }
    /// Does the backend have a working native `ResourceAdminExecutor` that does
    /// the ensure/drop I/O directly (i.e. NOT compiler-mediated and NOT
    /// catalog-migration)? This is the distinct dimension B.1 introduces — it is
    /// what was previously (incorrectly) read off `supports_resource_lifecycle`.
    pub fn is_native_executor(self) -> bool {
        matches!(self, Self::Native)
    }
}

/// Whether a backend has a concrete canonical `SystemStores` implementation
/// registered in this source tree. This mirrors `BackendKind::role` but is the
/// capability-level fact (B.1 dimension 5 "canonical system-store support").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemStoreSupport {
    /// No canonical `SystemStores` impl — this backend can only be a projection
    /// target (it cannot host outbox/saga/projection/audit tables).
    None,
    /// A full `SystemStores` supertrait impl exists under
    /// `runtime/canonical_store/*` (Postgres/MySQL/SQLite today).
    Full,
}

impl SystemStoreSupport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Full => "full",
        }
    }
    pub fn is_canonical(self) -> bool {
        matches!(self, Self::Full)
    }
}

/// B.12-B.15 — canonical promotion roadmap bucket.
///
/// `Implemented` is the only profile that may coincide with
/// `SystemStoreSupport::Full`; every other profile is a named proof obligation
/// and must remain `BackendRole::Projection` until real canonical-store evidence
/// lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalCandidateProfile {
    Implemented,
    NativeDriverRequired,
    SemanticsGated,
    VectorAtomicClaims,
    VectorNativeCasUnsupported,
    VectorFeasibilityRequired,
    ObjectConditionalWrites,
    CacheDurabilityRequired,
    ExplicitlyNotSupported,
}

impl CanonicalCandidateProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::NativeDriverRequired => "native_driver_required",
            Self::SemanticsGated => "semantics_gated",
            Self::VectorAtomicClaims => "vector_atomic_claims",
            Self::VectorNativeCasUnsupported => "vector_native_cas_unsupported",
            Self::VectorFeasibilityRequired => "vector_feasibility_required",
            Self::ObjectConditionalWrites => "object_conditional_writes",
            Self::CacheDurabilityRequired => "cache_durability_required",
            Self::ExplicitlyNotSupported => "explicitly_not_supported",
        }
    }
}

fn default_canonical_candidate_profile() -> CanonicalCandidateProfile {
    CanonicalCandidateProfile::ExplicitlyNotSupported
}

/// B.13-B.15 — structured canonical feasibility profile for one backend.
///
/// Where [`CanonicalCandidateProfile`] is a single roadmap *bucket*, this records
/// the concrete proof obligations a backend family must satisfy before its
/// [`BackendRole`] can flip to canonical: how atomic claims/leases are expressed,
/// how outbox ordering survives concurrent writers, how tenant isolation and read
/// fences work, the operator prerequisites (AOF, bucket versioning, …) that must
/// hold, and the remaining blocking gaps. It is descriptive evidence, never a
/// capability claim — `role` + `system_store` stay authoritative. `blocking_gaps`
/// is empty IFF the backend already ships a `SystemStores` implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalFeasibilityProfile {
    pub backend: &'static str,
    /// Backend family this profile reasons about: "sql", "document", "graph",
    /// "column", "vector", "object", or "cache".
    pub family: &'static str,
    pub candidate: CanonicalCandidateProfile,
    /// Does a real `SystemStores` implementation compile for this backend in the
    /// current feature set?
    pub implemented: bool,
    /// How atomic lease/claim ownership is (or would be) expressed natively.
    pub atomic_claim_strategy: &'static str,
    /// How ordered outbox progress survives concurrent writers.
    pub ordered_progress_strategy: &'static str,
    /// How per-tenant isolation is enforced for system records.
    pub tenant_isolation_strategy: &'static str,
    /// How a recoverable read fence / durable write-progress token is obtained.
    pub read_fence_strategy: &'static str,
    /// Operator prerequisites that MUST hold before canonical registration.
    pub durability_prerequisites: &'static [&'static str],
    /// Concrete proofs still missing before promotion (empty == implemented).
    pub blocking_gaps: &'static [&'static str],
    /// Env var that gates live conformance for this family, if any.
    pub live_conformance_env: Option<&'static str>,
}

// ── B.13/B.14 — shared prerequisite/gap tables (kept module-level so the
// per-backend match stays declarative and the same operator wording is reused
// across an object/cache family without divergence). ────────────────────────
const OBJECT_DURABILITY_PREREQS: &[&str] = &[
    "bucket/container versioning enabled",
    "object-lock / retention enabled for audit-chain immutability",
    "conditional-write (precondition) support enabled on the endpoint",
    "lifecycle/retention policy provisioned for outbox compaction",
];
const OBJECT_BLOCKING_GAPS: &[&str] = &[
    "no native cross-object monotonic sequence — outbox ordering needs a documented single-writer or external-sequencer profile",
    "list-after-write / read-after-overwrite consistency must be proven per provider",
    "multipart write atomicity for large system records is unproven",
    "no canonical SystemStores module compiled for object backends",
];
const REDIS_DURABILITY_PREREQS: &[&str] = &[
    "AOF persistence enabled (INFO persistence aof_enabled:1)",
    "appendfsync everysec or always",
    "replica or RDB+AOF retention for failover durability",
    "non-volatile maxmemory-policy (no eviction of system keys)",
];
const MEMCACHED_BLOCKING_GAPS: &[&str] = &[
    "no durable storage — items live in volatile RAM",
    "no key scan / enumeration to recover or audit system state",
    "no atomic multi-key claim (single-key CAS only)",
    "eviction can silently drop committed system state",
];
const VECTOR_CANONICAL_PLANE_PREREQS: &[&str] = &[
    "atomic claim winner for leases and task claims",
    "durable monotonic outbox progress token across writers",
    "tenant/project-scoped system-state namespace",
    "read fence that can prove later reads observe writes up to a token",
];
const QDRANT_BLOCKING_GAPS: &[&str] = &[
    "qdrant feature not compiled, so the native SystemStores adapter is absent",
    "live Qdrant conformance proof has not run for this build profile",
    "native compare-and-set / insert-if-absent primitive is not documented for point or payload updates",
    "insert_only avoids overwriting an existing point but does not return a distributed lease winner",
    "filtered payload updates can select existing points but do not return an atomic claim winner",
    "WAL-backed writes make individual point mutations durable, but do not create a monotonic cross-point outbox sequence",
    "strong write ordering orders submitted operations but does not make read-modify-write lease acquisition atomic across brokers",
];
const PINECONE_BLOCKING_GAPS: &[&str] = &[
    "metadata-filter update can change matching records, but does not expose compare-and-set or a claim winner",
    "upsert/update by id has no native monotonic outbox sequence",
    "namespace scoping is usable for tenant isolation but not for durable read fencing",
    "no canonical SystemStores module for the vector plane",
];
const WEAVIATE_BLOCKING_GAPS: &[&str] = &[
    "object update/PATCH and consistency levels do not expose native compare-and-set claim semantics",
    "leaderless/tunable consistency is not a durable ordered outbox token",
    "multi-tenancy scopes records but does not provide recovery ordering for SystemStores",
    "no canonical SystemStores module for the vector plane",
];
const ELASTICSEARCH_VECTOR_BLOCKING_GAPS: &[&str] = &[
    "optimistic concurrency can guard a single document update, but vector/search SystemStores are not implemented",
    "ordered outbox, lease, saga, audit, and migration contracts are not mapped to Elasticsearch",
    "tenant scoping and read fencing need a canonical index/profile, not projection search indexes",
];

/// B.1 — the orthogonal capability dimensions for one backend.
///
/// Each field answers an independent question; V1 `BackendCapability` is the
/// OR/projection of these (see [`Self::derive_v1`]).
///
/// This is a computed view (always rebuilt from `BackendKind::capabilities_v2`),
/// so it derives `Serialize` (for GetCapabilities / compat-matrix output) but not
/// `Deserialize` — the `&'static str` fields cannot be deserialized into owned
/// statics, and there is no reason to round-trip a computed view back in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BackendCapabilityV2 {
    // Dimension 1 — native executor support: the backend ships a real
    // `runtime/executors/*` adapter compiled into the build for query/mutate I/O.
    pub native_executor: bool,
    // Dimension 2 — compiler-mediated support: the backend has an
    // `ir::compile::*` dialect compiler so neutral logical ops are lowered to its
    // wire DDL/DML (every SQL/CQL backend; document/graph/vector use it too).
    pub compiler_mediated: bool,
    // Dimension 3 — generic dispatch admission: the set of generic-dispatch
    // operation tokens the runtime admits for this backend. Derived from the
    // existing `supported_operations()` so dispatch behavior is unchanged.
    pub dispatch_operations: Vec<&'static str>,
    // Dimension 4 — resource lifecycle support: how ensure/drop/list is
    // implemented (the native-vs-compiler-mediated distinction B.1 demands).
    pub lifecycle: LifecycleSupport,
    // Dimension 5 — canonical system-store support.
    pub system_store: SystemStoreSupport,
    // B.12-B.15 — named proof bucket and concrete promotion/conformance goal.
    pub canonical_candidate: CanonicalCandidateProfile,
    pub canonical_goal: &'static str,

    // Feature truths that V1 exposes verbatim and that have no finer V2 split.
    // These stay here so V1 derives entirely from V2 (single source of truth).
    pub supports_sql_ddl: bool,
    pub supports_transactions: bool,
    pub supports_xa: bool,
    pub supports_two_phase_commit: bool,
    pub supports_rls: bool,
    pub supports_vector_search: bool,
    pub supports_streaming: bool,
    pub supports_ttl: bool,
    pub is_object_store: bool,
    pub is_migration_ledger_capable: bool,
    pub supports_idempotency: bool,
    pub supports_schema_migration: bool,
    pub supports_hybrid_search: bool,
    pub max_payload_bytes: u64,
    pub consistency_model: &'static str,
}

impl BackendCapabilityV2 {
    /// Project the V2 dimensions down to the stable V1 `BackendCapability`.
    ///
    /// V1 `supports_resource_lifecycle` = "generic dispatch admits lifecycle
    /// ops" = `lifecycle.admits_dispatch()`. The native-executor truth is NOT
    /// folded into this boolean (that is the whole point of B.1) — callers that
    /// need it read `lifecycle.is_native_executor()` off V2.
    pub fn derive_v1(&self) -> BackendCapability {
        BackendCapability {
            supports_sql_ddl: self.supports_sql_ddl,
            supports_transactions: self.supports_transactions,
            supports_xa: self.supports_xa,
            supports_two_phase_commit: self.supports_two_phase_commit,
            supports_rls: self.supports_rls,
            supports_vector_search: self.supports_vector_search,
            supports_streaming: self.supports_streaming,
            supports_ttl: self.supports_ttl,
            is_object_store: self.is_object_store,
            is_migration_ledger_capable: self.is_migration_ledger_capable,
            supports_idempotency: self.supports_idempotency,
            supports_schema_migration: self.supports_schema_migration,
            supports_hybrid_search: self.supports_hybrid_search,
            supports_resource_lifecycle: self.lifecycle.admits_dispatch(),
            max_payload_bytes: self.max_payload_bytes,
            consistency_model: self.consistency_model.to_string(),
        }
    }
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
///   Postgres, MySQL, and SQLite currently satisfy this contract.
/// - **`Projection`** — the backend is a write target downstream of a
///   canonical store. It cannot host system tables or produce a
///   durability token. Qdrant, Redis, S3, Memcached, object stores.
/// - **`Both`** — the backend can play either role. No backend currently
///   advertises `Both`; future durable stores must implement the full
///   canonical-store contract before using it.
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
    /// B.12-B.15: roadmap bucket for canonical promotion evidence. This is not
    /// a capability claim by itself; `role` + `system_store` remain authoritative.
    #[serde(default = "default_canonical_candidate_profile")]
    pub canonical_candidate: CanonicalCandidateProfile,
    /// Concrete implementation/proof goal required for canonical promotion, or
    /// the live conformance duty for already-canonical stores.
    #[serde(default)]
    pub canonical_goal: String,
    /// B.13-B.15: structured canonical feasibility profile (proof dimensions,
    /// operator prerequisites, remaining gaps). Surfaced through `doctor` and
    /// `compat-matrix` so object/cache rows expose concrete promotion evidence.
    /// Computed view — serialized out, never read back in (so the value type only
    /// needs `Serialize`).
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub canonical_feasibility: Option<CanonicalFeasibilityProfile>,
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
        // NOTE: read the raw V1 feature fields here, NOT `capabilities()` /
        // `capabilities_v2()` — those call back into `supported_operations()`
        // (V2 dimension 3 is the dispatch op set), which would recurse. The
        // lifecycle admission decision below is identical to
        // `lifecycle_support().admits_dispatch()` by construction.
        let cap = self.capabilities_v1_fields();
        let mut ops = vec![OP_PING, OP_PROBE];
        if cap.supports_resource_lifecycle {
            ops.extend([OP_ENSURE_RESOURCE, OP_DROP_RESOURCE, OP_LIST_RESOURCES]);
        }
        match self {
            Self::Postgres
            | Self::Mysql
            | Self::Sqlite
            | Self::Mssql
            | Self::Clickhouse
            | Self::Redis
            | Self::Memcached
            | Self::Mongodb
            | Self::Elasticsearch
            | Self::Neo4j
            | Self::Cassandra
            | Self::Weaviate
            | Self::Pinecone => ops.push(OP_QUERY),
            _ => {}
        }
        match self {
            Self::Postgres
            | Self::Mysql
            | Self::Sqlite
            | Self::Mssql
            | Self::Clickhouse
            | Self::Redis
            | Self::Memcached
            | Self::Mongodb
            | Self::Neo4j
            | Self::Qdrant
            | Self::Elasticsearch
            | Self::Cassandra
            | Self::Weaviate
            | Self::Pinecone => ops.push(OP_MUTATE),
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
        match operation {
            OP_PING | OP_PROBE | OP_ENSURE_RESOURCE | OP_DROP_RESOURCE | OP_LIST_RESOURCES
            | OP_QUERY | OP_MUTATE | OP_TRANSACTION | OP_SEARCH | OP_GET_OBJECT | OP_PUT_OBJECT => {
                self.supported_operations().contains(&operation)
            }
            _ => false,
        }
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
            canonical_candidate: self.canonical_candidate_profile(),
            canonical_goal: self.canonical_promotion_goal().to_string(),
            canonical_feasibility: Some(self.canonical_feasibility_profile()),
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

/// B.2 — the SystemStores-evidence allowlist.
///
/// `BackendKind::role` cannot enumerate the runtime `CanonicalStoreRegistry`
/// (that registry is built at startup from operator config and is not reachable
/// from this compile-time module). So the "evidence" that a backend may claim a
/// `Canonical`/`Both` role is encoded here as an explicit allowlist of the
/// backends that have a concrete `SystemStores` supertrait implementation
/// committed under `src/runtime/canonical_store/*` (Postgres = `postgres.rs` +
/// `postgres_{projection,saga,admin_audit,migration_audit}.rs`; likewise MySQL
/// and SQLite). The `SystemStores` supertrait itself lives in
/// `runtime/canonical_store/mod.rs` and is the union of `CanonicalStore`,
/// `ProjectionTaskStore`, `SagaStore`, `AdminAuditStore`, `MigrationAuditStore`.
///
/// Adding a backend to a canonical role REQUIRES adding it here AND landing its
/// store impl — the `role_matches_system_store_evidence` test fails otherwise,
/// so role output, doctor output, and the real registry cannot drift (B.2 done
/// condition).
///
/// Some non-SQL canonical stores are feature-gated below because their native
/// drivers and live-conformance profiles are optional build targets. Backends
/// absent from this list stay `Projection`.
pub const CANONICAL_SYSTEM_STORE_BACKENDS: &[BackendKind] = &[
    BackendKind::Postgres,
    BackendKind::Mysql,
    BackendKind::Sqlite,
    // B.8: SQL Server canonical store (Tiberius) — promoted after the full
    // 5-contract conformance passed live on SQL Server 2022.
    BackendKind::Mssql,
    // B.14: Redis native canonical store — compiled with the redis feature.
    // Runtime registration still validates AOF persistence before exposing the
    // live instance as a SystemStores object.
    #[cfg(feature = "redis")]
    BackendKind::Redis,
    // B.9: native MongoDB canonical store — only in `mongodb-native` builds
    // (the scalar/Data-API build has no compiled store). Promoted after the full
    // 5-contract conformance passed live on a MongoDB replica set.
    #[cfg(feature = "mongodb-native")]
    BackendKind::Mongodb,
    // B.10a: Cassandra native CQL + LWT canonical store — compiled with the
    // `cassandra` feature. Promoted after the full 5-contract conformance passed
    // live on Cassandra 5.
    #[cfg(feature = "cassandra")]
    BackendKind::Cassandra,
    // B.10b: Neo4j native HTTP-transactional-Cypher canonical store — compiled
    // with the `neo4j` feature. Promoted after the full 5-contract conformance
    // passed live on Neo4j 5.
    #[cfg(feature = "neo4j")]
    BackendKind::Neo4j,
    // B.11: Qdrant native SystemStores — compiled with the `qdrant` feature.
    // Startup registration verifies the system collection can be created before
    // exposing the live instance as a SystemStores object.
    #[cfg(feature = "qdrant")]
    BackendKind::Qdrant,
    // B.12: shared vector/search canonical store — compiled per backend feature.
    #[cfg(feature = "pinecone")]
    BackendKind::Pinecone,
    #[cfg(feature = "weaviate")]
    BackendKind::Weaviate,
    #[cfg(feature = "elasticsearch")]
    BackendKind::Elasticsearch,
    // B.10c: ClickHouse native HTTP canonical store (ReplacingMergeTree +
    // versioned-CAS) — compiled with the `clickhouse` feature. Promoted after the
    // full 5-contract conformance passed live on ClickHouse 24.8 (single-writer
    // caveat documented).
    #[cfg(feature = "clickhouse")]
    BackendKind::Clickhouse,
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

    #[test]
    fn advertised_generic_dispatch_operations_are_admitted() {
        for backend in ALL_KINDS {
            for operation in backend.supported_operations() {
                assert!(
                    backend.supports_operation(operation),
                    "{} advertises '{operation}' but rejects it",
                    backend.as_str()
                );
            }
        }
    }

    #[test]
    fn generic_dispatch_operation_table_pins_known_drift_cases() {
        for backend in [
            BackendKind::Mysql,
            BackendKind::Sqlite,
            BackendKind::Mssql,
            BackendKind::Memcached,
            BackendKind::Elasticsearch,
            BackendKind::Cassandra,
            BackendKind::Weaviate,
            BackendKind::Pinecone,
        ] {
            assert!(
                backend.supported_operations().contains(&OP_QUERY),
                "{} has a real generic query executor",
                backend.as_str()
            );
            assert!(
                backend.supported_operations().contains(&OP_MUTATE),
                "{} has a real generic mutate executor",
                backend.as_str()
            );
        }

        for backend in [
            BackendKind::Qdrant,
            BackendKind::AzureBlob,
            BackendKind::Gcs,
        ] {
            assert!(
                !backend.supports_operation(OP_QUERY),
                "{} query is an unsupported fallback and must not be admitted",
                backend.as_str()
            );
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

    // ── B.1: V2 capability model ───────────────────────────────────────────────

    #[test]
    fn v1_capabilities_are_a_pure_projection_of_v2() {
        // V1 must be exactly `capabilities_v2().derive_v1()` for every backend.
        // This pins that V1 callers see no behavior change after the V2 split.
        for kind in ALL_KINDS {
            assert_eq!(
                kind.capabilities(),
                kind.capabilities_v2().derive_v1(),
                "V1 capabilities diverged from V2 projection for {kind:?}"
            );
        }
    }

    #[test]
    fn v1_resource_lifecycle_equals_dispatch_admission_dimension() {
        // The V1 `supports_resource_lifecycle` boolean is precisely the
        // "generic dispatch admits lifecycle ops" projection of V2.
        for kind in ALL_KINDS {
            let v2 = kind.capabilities_v2();
            assert_eq!(
                kind.capabilities().supports_resource_lifecycle,
                v2.lifecycle.admits_dispatch(),
                "{kind:?}: V1 supports_resource_lifecycle must equal lifecycle.admits_dispatch()"
            );
        }
    }

    #[test]
    fn advertised_operations_have_compiler_or_executor_evidence() {
        // B.1 done-condition: every advertised operation must have evidence —
        // either a native runtime executor OR a compiler dialect. A backend that
        // advertises an operation with NEITHER is a capability lie. (No backend
        // in the matrix should hit the panic; this test fails loudly if a future
        // edit adds an operation to a backend that has no implementation seam.)
        for kind in ALL_KINDS {
            let v2 = kind.capabilities_v2();
            for op in &v2.dispatch_operations {
                // ping/probe are universal control ops — always admissible.
                if *op == OP_PING || *op == OP_PROBE {
                    continue;
                }
                assert!(
                    v2.native_executor || v2.compiler_mediated,
                    "{kind:?} advertises operation '{op}' but has neither native \
                     executor nor compiler evidence"
                );
            }
        }
    }

    #[test]
    fn lifecycle_op_admission_requires_nonzero_lifecycle_dimension() {
        // ensure/drop/list admission must be backed by a lifecycle dimension
        // that is not `None` — i.e. the dispatch admission and the lifecycle
        // truth cannot disagree.
        for kind in ALL_KINDS {
            let v2 = kind.capabilities_v2();
            let advertises_lifecycle = v2.dispatch_operations.contains(&OP_ENSURE_RESOURCE)
                || v2.dispatch_operations.contains(&OP_DROP_RESOURCE)
                || v2.dispatch_operations.contains(&OP_LIST_RESOURCES);
            assert_eq!(
                advertises_lifecycle,
                v2.lifecycle.admits_dispatch(),
                "{kind:?}: lifecycle op admission must match lifecycle.admits_dispatch()"
            );
        }
    }

    #[test]
    fn mssql_lifecycle_is_compiler_mediated_not_native() {
        // MSSQL admits ensure/drop/list on the generic-dispatch plane, but its
        // native ResourceAdminExecutor::{ensure,drop}_resource return
        // `unimplemented` — the real DDL goes through compile_resource_op +
        // mutate. So: dispatch admits lifecycle, lifecycle is CompilerMediated,
        // and it is NOT a native executor lifecycle.
        let v2 = BackendKind::Mssql.capabilities_v2();
        assert_eq!(v2.lifecycle, LifecycleSupport::CompilerMediated);
        assert!(v2.lifecycle.admits_dispatch());
        assert!(
            !v2.lifecycle.is_native_executor(),
            "MSSQL lifecycle must NOT claim native executor coverage"
        );
        assert!(
            v2.compiler_mediated,
            "MSSQL lifecycle is reached through the T-SQL compiler"
        );
        // V1 still advertises lifecycle (unchanged behavior).
        assert!(
            BackendKind::Mssql
                .capabilities()
                .supports_resource_lifecycle
        );
    }

    #[test]
    fn cassandra_lifecycle_is_compiler_mediated_not_native() {
        // Same distinction as MSSQL: Cassandra's native ensure/drop are
        // `unimplemented`; DDL is compiler-mediated (CQL via compile_resource_op).
        let v2 = BackendKind::Cassandra.capabilities_v2();
        assert_eq!(v2.lifecycle, LifecycleSupport::CompilerMediated);
        assert!(v2.lifecycle.admits_dispatch());
        assert!(
            !v2.lifecycle.is_native_executor(),
            "Cassandra lifecycle must NOT claim native executor coverage"
        );
        assert!(v2.compiler_mediated);
        assert!(
            BackendKind::Cassandra
                .capabilities()
                .supports_resource_lifecycle
        );
    }

    #[test]
    fn native_lifecycle_backends_are_distinct_from_compiler_mediated() {
        // Object stores / Qdrant / Mongo / Neo4j / ClickHouse / ES / Weaviate /
        // Pinecone have native ResourceAdminExecutor ensure/drop that do real
        // I/O — they are Native, NOT CompilerMediated.
        for kind in [
            BackendKind::Minio,
            BackendKind::S3,
            BackendKind::Qdrant,
            BackendKind::Mongodb,
            BackendKind::Neo4j,
            BackendKind::Clickhouse,
            BackendKind::Elasticsearch,
            BackendKind::Weaviate,
            BackendKind::Pinecone,
        ] {
            let v2 = kind.capabilities_v2();
            assert_eq!(
                v2.lifecycle,
                LifecycleSupport::Native,
                "{kind:?} has a native ResourceAdminExecutor lifecycle"
            );
            assert!(v2.lifecycle.is_native_executor(), "{kind:?}");
        }
    }

    #[test]
    fn sql_canonical_backends_use_catalog_migration_lifecycle() {
        // Postgres/MySQL/SQLite native ensure/drop return failed_precondition;
        // lifecycle is via catalog migrations. They still admit dispatch ops.
        // NOTE: this is the *relational catalog-migration* trio, NOT the whole
        // CANONICAL_SYSTEM_STORE_BACKENDS allowlist — SQL Server (B.8) is also a
        // canonical system store but its RESOURCE lifecycle is CompilerMediated
        // (compile_resource_op + mutate), a separate V2 dimension asserted by
        // `mssql_lifecycle_is_compiler_mediated_not_native`.
        for kind in [
            BackendKind::Postgres,
            BackendKind::Mysql,
            BackendKind::Sqlite,
        ] {
            let v2 = kind.capabilities_v2();
            assert_eq!(
                v2.lifecycle,
                LifecycleSupport::CatalogMigration,
                "{kind:?} relational lifecycle is catalog-migration managed"
            );
            assert!(!v2.lifecycle.is_native_executor(), "{kind:?}");
            assert!(v2.lifecycle.admits_dispatch(), "{kind:?}");
        }
    }

    #[test]
    fn no_lifecycle_backends_reject_lifecycle_ops() {
        for kind in [BackendKind::Redis, BackendKind::Memcached] {
            let v2 = kind.capabilities_v2();
            assert_eq!(v2.lifecycle, LifecycleSupport::None, "{kind:?}");
            assert!(!v2.lifecycle.admits_dispatch(), "{kind:?}");
            assert!(
                !kind.capabilities().supports_resource_lifecycle,
                "{kind:?} must not advertise V1 lifecycle"
            );
        }
    }

    // ── B.2: BackendRole honesty vs SystemStores evidence ──────────────────────

    #[test]
    fn role_matches_system_store_evidence() {
        // Any backend whose role is Canonical/Both MUST have a registered
        // SystemStores evidence entry (the CANONICAL_SYSTEM_STORE_BACKENDS
        // allowlist, which mirrors the committed
        // runtime/canonical_store/* impls). Projection backends must NOT be on
        // the allowlist. This is the role-vs-allowlist consistency check (B.2).
        for kind in ALL_KINDS {
            let role_is_canonical = kind.role().can_host_system_tables();
            let has_evidence = CANONICAL_SYSTEM_STORE_BACKENDS.contains(&kind);
            assert_eq!(
                role_is_canonical, has_evidence,
                "{kind:?}: role.can_host_system_tables()={role_is_canonical} but \
                 SystemStores evidence={has_evidence} — role and committed \
                 canonical_store impls must agree"
            );
            // The capability-level mirror must also agree.
            assert_eq!(
                kind.system_store_support().is_canonical(),
                has_evidence,
                "{kind:?}: system_store_support() must mirror the allowlist"
            );
        }
    }

    #[test]
    fn target_backends_stay_projection_until_canonical_stores_exist() {
        // B.2 forbids promoting these from Projection until their SystemStores
        // impls land. SQL Server graduated in B.8, and native MongoDB in B.9 (so
        // MongoDB is pinned here ONLY in non-`mongodb-native` builds, where no
        // store is compiled). These remain projection-only until their stores
        // exist.
        for kind in [
            #[cfg(not(feature = "mongodb-native"))]
            BackendKind::Mongodb,
            // B.10c: ClickHouse graduated (native ReplacingMergeTree store).
            // Pinned here only in non-`clickhouse` builds.
            #[cfg(not(feature = "clickhouse"))]
            BackendKind::Clickhouse,
            // B.10b: Neo4j graduated (native HTTP-Cypher store). Pinned here only
            // in non-`neo4j` builds.
            #[cfg(not(feature = "neo4j"))]
            BackendKind::Neo4j,
            // B.10a: Cassandra graduated (native CQL+LWT store). Pinned here only
            // in non-`cassandra` builds, where no store is compiled.
            #[cfg(not(feature = "cassandra"))]
            BackendKind::Cassandra,
        ] {
            assert_eq!(
                kind.role(),
                BackendRole::Projection,
                "{kind:?} must stay Projection until its canonical SystemStores exists"
            );
            assert!(
                !CANONICAL_SYSTEM_STORE_BACKENDS.contains(&kind),
                "{kind:?} must not be on the SystemStores allowlist yet"
            );
        }
    }

    #[test]
    fn all_backend_rows_have_canonical_candidate_goals() {
        // B.15: every capability-matrix row must say what canonical completion
        // means. Projection backends cannot be silent roadmap islands.
        for kind in ALL_KINDS {
            let v2 = kind.capabilities_v2();
            let matrix = kind.capability_matrix_entry();
            assert_eq!(
                matrix.canonical_candidate, v2.canonical_candidate,
                "{kind:?}: matrix candidate profile must mirror V2"
            );
            assert_eq!(
                matrix.canonical_goal, v2.canonical_goal,
                "{kind:?}: matrix canonical goal must mirror V2"
            );
            assert!(
                !v2.canonical_goal.trim().is_empty(),
                "{kind:?}: canonical goal must be explicit"
            );
        }
    }

    #[test]
    fn b14_cache_backends_have_durability_or_rejection_profiles() {
        #[cfg(feature = "redis")]
        assert_eq!(
            BackendKind::Redis.canonical_candidate_profile(),
            CanonicalCandidateProfile::Implemented
        );
        #[cfg(not(feature = "redis"))]
        assert_eq!(
            BackendKind::Redis.canonical_candidate_profile(),
            CanonicalCandidateProfile::CacheDurabilityRequired
        );
        assert_eq!(
            BackendKind::Memcached.canonical_candidate_profile(),
            CanonicalCandidateProfile::ExplicitlyNotSupported
        );
        #[cfg(feature = "redis")]
        assert_eq!(BackendKind::Redis.role(), BackendRole::Canonical);
        #[cfg(not(feature = "redis"))]
        assert_eq!(BackendKind::Redis.role(), BackendRole::Projection);
        assert_eq!(BackendKind::Memcached.role(), BackendRole::Projection);
    }

    #[test]
    fn vector_plane_backends_share_native_canonical_blockers() {
        for kind in [
            BackendKind::Qdrant,
            BackendKind::Pinecone,
            BackendKind::Weaviate,
            BackendKind::Elasticsearch,
        ] {
            let profile = kind.canonical_feasibility_profile();
            assert_eq!(profile.family, "vector", "{kind:?}");
            if kind.system_store_support().is_canonical() {
                assert!(profile.implemented, "{kind:?} must expose SystemStores");
                assert_eq!(kind.role(), BackendRole::Canonical, "{kind:?}");
                assert_eq!(
                    kind.system_store_support(),
                    SystemStoreSupport::Full,
                    "{kind:?}"
                );
                assert!(
                    profile.blocking_gaps.is_empty(),
                    "{kind:?} must not carry blockers after promotion"
                );
            } else {
                assert!(!profile.implemented, "{kind:?} must not fake SystemStores");
                assert_eq!(kind.role(), BackendRole::Projection, "{kind:?}");
                assert_eq!(
                    kind.system_store_support(),
                    SystemStoreSupport::None,
                    "{kind:?}"
                );
                assert!(
                    !profile.blocking_gaps.is_empty(),
                    "{kind:?} must expose native vector-plane blockers"
                );
            }
            assert!(
                profile
                    .durability_prerequisites
                    .contains(&"atomic claim winner for leases and task claims"),
                "{kind:?} must share the vector-plane atomic-claim prerequisite"
            );
            if kind.system_store_support().is_canonical() {
                let expected_env = match kind {
                    BackendKind::Qdrant => "UDB_QDRANT_URL",
                    BackendKind::Pinecone => "UDB_PINECONE_DSN",
                    BackendKind::Weaviate => "UDB_WEAVIATE_DSN",
                    BackendKind::Elasticsearch => "UDB_ELASTIC_DSN",
                    _ => unreachable!(),
                };
                assert_eq!(profile.live_conformance_env, Some(expected_env));
                assert!(CANONICAL_SYSTEM_STORE_BACKENDS.contains(&kind));
            } else {
                assert!(
                    profile.live_conformance_env.is_some(),
                    "{kind:?} must advertise the live conformance env for promotion"
                );
                assert!(
                    !CANONICAL_SYSTEM_STORE_BACKENDS.contains(&kind),
                    "{kind:?} must not enter the canonical allowlist"
                );
            }
        }
    }

    #[test]
    fn qdrant_canonical_profile_tracks_compiled_adapter() {
        let profile = BackendKind::Qdrant.canonical_feasibility_profile();
        if cfg!(feature = "qdrant") {
            assert_eq!(
                BackendKind::Qdrant.canonical_candidate_profile(),
                CanonicalCandidateProfile::Implemented
            );
            assert!(profile.blocking_gaps.is_empty());
            assert_eq!(BackendKind::Qdrant.role(), BackendRole::Canonical);
        } else {
            assert_eq!(
                BackendKind::Qdrant.canonical_candidate_profile(),
                CanonicalCandidateProfile::VectorNativeCasUnsupported
            );
            assert!(
                profile
                    .blocking_gaps
                    .iter()
                    .any(|gap| gap.contains("qdrant feature not compiled")),
                "Qdrant must explain why the adapter is unavailable"
            );
        }
    }

    #[test]
    fn noncanonical_candidate_profiles_do_not_claim_system_store_support() {
        for kind in ALL_KINDS {
            if kind.canonical_candidate_profile() == CanonicalCandidateProfile::Implemented {
                assert!(
                    kind.system_store_support().is_canonical(),
                    "{kind:?}: implemented canonical profile must have SystemStores evidence"
                );
            } else {
                assert_eq!(
                    kind.role(),
                    BackendRole::Projection,
                    "{kind:?}: non-implemented canonical profile must stay Projection"
                );
                assert_eq!(
                    kind.system_store_support(),
                    SystemStoreSupport::None,
                    "{kind:?}: non-implemented canonical profile must not claim SystemStores"
                );
            }
        }
    }

    #[test]
    fn clickhouse_keeps_append_analytics_profile() {
        // ClickHouse keeps its eventual-consistency / no-multi-statement-txn V1
        // profile, but B.10c promoted it to a canonical SystemStores backend
        // (ReplacingMergeTree(version) + SELECT … FINAL versioned-CAS) in
        // `clickhouse` builds; projection otherwise.
        let cap = BackendKind::Clickhouse.capabilities();
        assert_eq!(cap.consistency_model, "eventual");
        assert!(!cap.supports_transactions);
        #[cfg(feature = "clickhouse")]
        assert_eq!(BackendKind::Clickhouse.role(), BackendRole::Canonical);
        #[cfg(not(feature = "clickhouse"))]
        assert_eq!(BackendKind::Clickhouse.role(), BackendRole::Projection);
    }

    #[test]
    fn canonical_allowlist_backends_are_runtime_capable() {
        // Every backend on the SystemStores allowlist must actually have a
        // runtime implementation (otherwise the "evidence" is hollow).
        for kind in CANONICAL_SYSTEM_STORE_BACKENDS {
            assert!(
                crate::backend::has_runtime_implementation(kind),
                "{kind:?} is on the canonical allowlist but has no runtime impl"
            );
        }
    }

    #[test]
    fn every_backend_exposes_a_feasibility_profile() {
        // B.15: no capability-matrix row is a silent roadmap island — every
        // backend returns a structured feasibility profile that mirrors its
        // identity and candidate bucket, with all proof dimensions populated.
        for kind in ALL_KINDS {
            let p = kind.canonical_feasibility_profile();
            assert_eq!(
                p.backend,
                kind.as_str(),
                "{kind:?}: profile.backend must equal as_str()"
            );
            assert_eq!(
                p.candidate,
                kind.canonical_candidate_profile(),
                "{kind:?}: profile.candidate must mirror canonical_candidate_profile()"
            );
            assert!(
                !p.family.trim().is_empty(),
                "{kind:?}: family must be explicit"
            );
            for (name, s) in [
                ("atomic_claim_strategy", p.atomic_claim_strategy),
                ("ordered_progress_strategy", p.ordered_progress_strategy),
                ("tenant_isolation_strategy", p.tenant_isolation_strategy),
                ("read_fence_strategy", p.read_fence_strategy),
            ] {
                assert!(
                    !s.trim().is_empty(),
                    "{kind:?}: {name} must be a non-empty strategy description"
                );
            }
        }
    }

    #[test]
    fn feasibility_implemented_flag_matches_system_store_evidence() {
        // The `implemented` flag is the SystemStores-evidence truth, and the
        // documented invariant is: blocking_gaps is empty IFF a real
        // SystemStores impl compiles. Both must hold for every backend.
        for kind in ALL_KINDS {
            let p = kind.canonical_feasibility_profile();
            assert_eq!(
                p.implemented,
                kind.system_store_support().is_canonical(),
                "{kind:?}: implemented flag must mirror system_store_support().is_canonical()"
            );
            assert_eq!(
                p.implemented,
                p.blocking_gaps.is_empty(),
                "{kind:?}: a backend ships a real SystemStores impl IFF it has no blocking gaps"
            );
        }
    }

    #[test]
    fn feasibility_profile_is_mirrored_in_capability_matrix() {
        // Every matrix row must carry the exact feasibility profile of its
        // backend — and no row may omit it (it is always Some).
        for entry in capability_matrix() {
            let kind = ALL_KINDS
                .into_iter()
                .find(|k| entry.backend == k.as_str())
                .unwrap_or_else(|| panic!("matrix row {} has no matching kind", entry.backend));
            assert_eq!(
                entry.canonical_feasibility,
                Some(kind.canonical_feasibility_profile()),
                "{kind:?}: matrix feasibility must mirror canonical_feasibility_profile()"
            );
            assert!(
                entry.canonical_feasibility.is_some(),
                "{kind:?}: every matrix row must expose a feasibility profile"
            );
        }
    }

    #[test]
    fn b13_object_family_feasibility_is_complete() {
        // B.13: object stores expose explicit operator prerequisites and the
        // remaining ordering proof gaps, are gated by a live-conformance env,
        // and stay Projection until those proofs land.
        for kind in [
            BackendKind::S3,
            BackendKind::Minio,
            BackendKind::AzureBlob,
            BackendKind::Gcs,
        ] {
            let p = kind.canonical_feasibility_profile();
            assert_eq!(p.family, "object", "{kind:?}: object family");
            assert_eq!(
                p.candidate,
                CanonicalCandidateProfile::ObjectConditionalWrites,
                "{kind:?}: object stores use the conditional-writes candidate bucket"
            );
            assert!(
                !p.durability_prerequisites.is_empty(),
                "{kind:?}: object operator prerequisites must be explicit"
            );
            assert!(
                !p.blocking_gaps.is_empty(),
                "{kind:?}: object stores have remaining blocking gaps"
            );
            assert!(
                p.blocking_gaps.iter().any(|g| g.contains("sequence")
                    || g.contains("ordering")
                    || g.contains("order")),
                "{kind:?}: a blocking gap must call out the ordering/sequence proof"
            );
            assert!(
                p.live_conformance_env.is_some(),
                "{kind:?}: a live-conformance gate env must be wired"
            );
            assert_eq!(
                kind.role(),
                BackendRole::Projection,
                "{kind:?}: object stores stay Projection until proofs land"
            );
        }
    }

    #[test]
    fn b14_cache_family_feasibility_is_explicit() {
        // B.14: Redis is a durability-gated candidate (AOF prerequisites,
        // live gate wired); Memcached is explicitly not supported with
        // durability/volatility blocking gaps and no live gate.
        let p = BackendKind::Redis.canonical_feasibility_profile();
        assert_eq!(p.family, "cache", "Redis: cache family");
        assert!(
            p.durability_prerequisites
                .iter()
                .any(|s| s.contains("AOF") || s.contains("aof")),
            "Redis durability prerequisites must mention AOF persistence"
        );
        assert!(
            p.live_conformance_env.is_some(),
            "Redis: a live-conformance gate env must be wired"
        );

        let p = BackendKind::Memcached.canonical_feasibility_profile();
        assert_eq!(p.family, "cache", "Memcached: cache family");
        assert_eq!(
            p.candidate,
            CanonicalCandidateProfile::ExplicitlyNotSupported,
            "Memcached is explicitly not supported for canonical state"
        );
        assert!(!p.implemented, "Memcached: no canonical SystemStores impl");
        assert!(
            !p.blocking_gaps.is_empty(),
            "Memcached: must enumerate blocking gaps"
        );
        assert!(
            p.blocking_gaps
                .iter()
                .any(|g| g.contains("durable") || g.contains("volatile") || g.contains("eviction")),
            "Memcached: a blocking gap must call out durability/volatility/eviction"
        );
        assert!(
            p.live_conformance_env.is_none(),
            "Memcached: no live-conformance gate (nothing to prove canonical)"
        );
    }

    #[test]
    fn memcached_can_never_claim_canonical() {
        // B.14/B.15 negative test: Memcached's role, host-ability, system-store
        // support, and candidate bucket must all refuse canonical promotion.
        assert_eq!(BackendKind::Memcached.role(), BackendRole::Projection);
        assert!(
            !BackendKind::Memcached.role().can_host_system_tables(),
            "Memcached role must not be able to host system tables"
        );
        assert_eq!(
            BackendKind::Memcached.system_store_support(),
            SystemStoreSupport::None
        );
        assert_eq!(
            BackendKind::Memcached.canonical_candidate_profile(),
            CanonicalCandidateProfile::ExplicitlyNotSupported
        );
    }

    #[test]
    fn dispatch_matrix_parity_for_every_backend() {
        // B.15: the matrix row's advertised operations must be exactly the
        // generic-dispatch op set (same sorted/deduped order), each op must be
        // independently accepted by supports_operation, and object RPCs are
        // admitted IFF the backend is an object store.
        for kind in ALL_KINDS {
            let entry = kind.capability_matrix_entry();
            let expected: Vec<String> = kind
                .supported_operations()
                .into_iter()
                .map(ToString::to_string)
                .collect();
            assert_eq!(
                entry.operations, expected,
                "{kind:?}: matrix operations must match supported_operations() in order"
            );
            for op in &entry.operations {
                assert!(
                    kind.supports_operation(op),
                    "{kind:?}: advertised op '{op}' must be accepted by supports_operation()"
                );
            }
            let admits_object_rpc = entry
                .operations
                .iter()
                .any(|o| o == "get_object" || o == "put_object");
            assert_eq!(
                admits_object_rpc,
                kind.capabilities().is_object_store,
                "{kind:?}: object RPCs are admitted IFF the backend is an object store"
            );
        }
    }
}
