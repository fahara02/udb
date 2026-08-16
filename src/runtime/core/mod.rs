#![allow(clippy::result_large_err)]

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "s3")]
use aws_config::BehaviorVersion;
#[cfg(feature = "s3")]
use aws_sdk_s3::config::{Credentials, Region};
#[cfg(feature = "s3")]
use aws_sdk_s3::presigning::PresigningConfig;
#[cfg(feature = "s3")]
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
// prost_types imported transitively via executor_utils::*
#[cfg(feature = "kafka")]
use rdkafka::ClientConfig;
#[cfg(feature = "kafka")]
use rdkafka::consumer::{BaseConsumer, Consumer};
#[cfg(feature = "redis")]
use redis::AsyncCommands;
// reqwest::StatusCode used via executor_utils::qdrant_status re-export
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
// sha2 re-exported transitively via executor_utils; no direct use in core.rs
use sqlx::postgres::{PgPoolOptions, PgRow};
// sqlx::query::Query used via postgres_helpers::bind_values re-export
use sqlx::{Column, Executor, PgPool, Row, TypeInfo};
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::broker::{
    DeletePlanRequest, RequestContext, SelectPlanRequest, SortSpec, UpsertPlanRequest,
    build_delete_plan, build_select_query_plan, build_upsert_plan, resolve_table_for_message,
};
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
use crate::broker::{
    ObjectAccessRequest, ObjectStreamPlanRequest, build_object_stream_plan, evaluate_object_access,
};
#[cfg(feature = "qdrant")]
use crate::broker::{
    VectorSearchPlanRequest, VectorUpsertPlanRequest, build_vector_search_plan,
    build_vector_upsert_plan,
};
use crate::generation::{CatalogManifest, GeneratedArtifact, ManifestStore, ManifestTable};
use crate::proto::{
    Chunk, MultipartUploadRequest, MultipartUploadResponse, Mutation, MutationResponse, RecordSet,
    Row as ProtoRow, SelectRequest, TxStatus, UpsertRequest, UrlRequest, UrlResponse,
    VectorHybridSearchRequest, VectorPoint, VectorPointMutation, VectorSearchRequest, VectorSet,
    VectorUpsertRequest, ViewDefinition,
};

#[cfg(feature = "s3")]
use super::config::MinioConfig;
#[cfg(feature = "redis")]
use super::config::RedisConfig;
use super::config::{
    BackendInstance, BackendInstanceConfig, BackendInstanceRole, DbConfig, UdbConfig,
};
use super::connection_manager::ConnectionManager;
use super::encryption::EncryptionRuntime;
use super::executor_utils::*;
#[cfg(feature = "clickhouse")]
use super::executors::clickhouse::ClickHouseConfig;
#[cfg(feature = "clickhouse")]
use super::executors::clickhouse::ClickHouseExecutor;
#[cfg(feature = "mongodb")]
use super::executors::mongodb::MongoDbConfig;
#[cfg(feature = "mongodb")]
use super::executors::mongodb::MongoDbExecutor;
#[cfg(feature = "mongodb-native")]
use super::executors::mongodb::MongoDbNativeConfig;
#[cfg(feature = "neo4j")]
use super::executors::neo4j::Neo4jConfig;
#[cfg(feature = "neo4j")]
use super::executors::neo4j::Neo4jExecutor;
// PostgresExecutor / S3Executor are now constructed by their respective
// `DispatchFactory` plugin impls (U2 step 5), not directly here.
#[cfg(feature = "qdrant")]
use super::executors::qdrant::{QdrantExecutor, QdrantHttpClient};
use super::executors::{BackendExecutorRegistration, BackendExecutorRegistry};
// `DefaultBackendExecutor` was deleted in U2 step 6; `backend_executor()` now
// returns a typed `ResolvedExecutorTarget` and the live dispatch executor is
// built via `resolve_dispatch_executor` (plugin-keyed).
use super::postgres_helpers::*;
use super::replica::{
    PgReplicaManager, PgReplicaPool, PgReplicaSnapshot, PgReplicaStrategy, append_application_name,
};
use super::system::{
    SystemCatalogInspection, SystemCatalogReport, ensure_system_catalog, inspect_system_catalog,
};

#[derive(Debug, Clone, Default)]
pub struct RuntimeInitReport {
    pub postgres_configured: bool,
    pub redis_configured: bool,
    pub qdrant_configured: bool,
    pub s3_configured: bool,
    pub encryption_configured: bool,
    pub mongodb_configured: bool,
    pub neo4j_configured: bool,
    pub clickhouse_configured: bool,
    /// NW3-1: MySQL primary configured via UDB_MYSQL_DSN.
    pub mysql_configured: bool,
    /// NW3-2: SQLite primary configured via UDB_SQLITE_DSN (or
    /// `sqlite::memory:` for the test profile).
    pub sqlite_configured: bool,
    /// C9: Elasticsearch primary configured via UDB_ELASTIC_DSN.
    pub elasticsearch_configured: bool,
    /// C9: Memcached primary configured via UDB_MEMCACHED_DSN.
    pub memcached_configured: bool,
    /// C9: SQL Server primary configured via UDB_MSSQL_DSN.
    pub mssql_configured: bool,
    /// C9: Weaviate primary configured via UDB_WEAVIATE_DSN.
    pub weaviate_configured: bool,
    /// C9: Pinecone primary configured via UDB_PINECONE_DSN.
    pub pinecone_configured: bool,
    /// C9: Cassandra primary configured via UDB_CASSANDRA_DSN.
    pub cassandra_configured: bool,
    /// C9: Azure Blob primary configured via UDB_AZUREBLOB_DSN.
    pub azureblob_configured: bool,
    /// C9: GCS primary configured via UDB_GCS_DSN.
    pub gcs_configured: bool,
    pub backend_instances: Vec<RuntimeBackendInstance>,
    /// S1: a full canonical system store (saga / admin-audit / migration-audit /
    /// projection tables) actually registered as the default `SystemStores`.
    /// `false` while a relational backend is configured means `udb_system` was
    /// not provisioned (`ensure_full_system_store_tables` failed) → the
    /// saga/audit/admin RPCs fail-fast with `FAILED_PRECONDITION`. Surfaced as a
    /// failing readiness fact (`slo::build_readiness_facts`) + a loud boot log.
    pub full_system_store_registered: bool,
    pub warnings: Vec<String>,
}

/// PostgreSQL privilege check results used by `doctor` and `GetHealthReport`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PostgresPrivilegeReport {
    /// Whether the privilege checks were actually executed (false if PG not configured).
    pub checked: bool,
    /// Role has CREATE privilege on the current database (needed to create schemas).
    pub create_schema: bool,
    /// Role has CREATE privilege on at least one relevant schema (for tables).
    pub create_table: bool,
    /// Role has superuser, replication, or pg_publication_admin membership
    /// (needed for CREATE PUBLICATION).
    pub create_publication: bool,
    /// Role has superuser or replication role (needed for logical replication slots).
    pub replication_slot: bool,
    /// Role can successfully acquire a session-level advisory lock.
    pub advisory_lock: bool,
    /// Non-fatal errors encountered during privilege checks.
    pub errors: Vec<String>,
}

/// Liveness probe result for a single backend.
#[derive(Debug, Clone, Serialize)]
pub struct BackendProbeResult {
    pub backend: String,
    pub ok: bool,
    pub latency_ms: u64,
    pub error: Option<String>,
}

/// Redacted runtime view of a configured backend instance.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeBackendInstance {
    pub name: String,
    pub backend: String,
    pub role: String,
    pub enabled: bool,
    pub configured: bool,
    pub connected: bool,
    pub read_weight: u32,
    pub write_weight: u32,
    pub dsn_env: Option<String>,
    pub labels: HashMap<String, String>,
    pub capabilities: Vec<String>,
    pub healthy: bool,
    pub circuit_open: bool,
}

/// One exact, project-authorized PostgreSQL write target. The instance alias is
/// persisted with migration plans, while `provenance_sha256` binds that alias
/// to both its routing policy and the physical PostgreSQL endpoint observed at
/// resolution time. Callers must re-resolve and compare the provenance before
/// executing deferred work.
#[derive(Debug, Clone)]
pub(crate) struct ProjectPostgresWriteTarget {
    pub instance: String,
    pub pool: PgPool,
    pub provenance_sha256: String,
}

/// Canonical runtime routing target parsed from a backend selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBackendSelector {
    pub backend: String,
    pub instance: Option<String>,
}

/// Resolved dispatch target — what `backend_executor()` returns after the
/// registry/connectivity/circuit-breaker checks (U2 step 6 replacement for
/// the former `DefaultBackendExecutor` adapter struct). Combine with
/// `resolve_dispatch_executor` to obtain the live executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedExecutorTarget {
    pub backend: String,
    pub instance: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DataBrokerRuntime {
    pg_pool: Option<PgPool>,
    /// Named PostgreSQL pools for generic instance-aware routing.
    pg_instances: HashMap<String, PgPool>,
    /// NW3-1: MySQL primary pool. Mirrors `pg_pool` for the MySQL
    /// backend. None means no MySQL is configured.
    #[cfg(feature = "mysql")]
    pub(crate) mysql_pool: Option<sqlx::MySqlPool>,
    #[cfg(feature = "mysql")]
    pub(crate) mysql_instances: HashMap<String, sqlx::MySqlPool>,
    /// NW3-2: SQLite primary pool.
    #[cfg(feature = "sqlite")]
    pub(crate) sqlite_pool: Option<sqlx::SqlitePool>,
    #[cfg(feature = "sqlite")]
    pub(crate) sqlite_instances: HashMap<String, sqlx::SqlitePool>,
    /// Optional read-replica pools — used for SELECT queries when healthy.
    pg_replicas: PgReplicaManager,
    #[cfg(feature = "redis")]
    redis: Option<redis::Client>,
    #[cfg(feature = "redis")]
    redis_instances: HashMap<String, redis::Client>,
    #[cfg(feature = "qdrant")]
    qdrant: Option<QdrantHttpClient>,
    #[cfg(feature = "qdrant")]
    qdrant_instances: HashMap<String, QdrantHttpClient>,
    /// Ad-hoc vector collections created through `EnsureResource` do not exist in
    /// the active catalog manifest yet. Remember their serving backend so typed
    /// VectorUpsert/VectorSearch do not silently fall back to Qdrant.
    vector_resource_routes: Arc<Mutex<HashMap<String, ResolvedBackendSelector>>>,
    /// C9: Elasticsearch primary client (`UDB_ELASTIC_DSN` deployment).
    #[cfg(feature = "elasticsearch")]
    pub(crate) elasticsearch:
        Option<crate::runtime::executors::elasticsearch::ElasticsearchHttpClient>,
    #[cfg(feature = "elasticsearch")]
    pub(crate) elasticsearch_instances:
        HashMap<String, crate::runtime::executors::elasticsearch::ElasticsearchHttpClient>,
    /// C9: Memcached primary client (`UDB_MEMCACHED_DSN` deployment).
    /// Real binary-protocol driver via the `memcache` crate.
    #[cfg(feature = "memcached")]
    pub(crate) memcached: Option<crate::runtime::executors::memcached::MemcachedClient>,
    #[cfg(feature = "memcached")]
    pub(crate) memcached_instances:
        HashMap<String, crate::runtime::executors::memcached::MemcachedClient>,
    /// C9: SQL Server primary client (`UDB_MSSQL_DSN` deployment).
    /// Real TDS-protocol driver via tiberius; lazy connect.
    #[cfg(feature = "mssql")]
    pub(crate) mssql: Option<crate::runtime::executors::mssql::MssqlClient>,
    #[cfg(feature = "mssql")]
    pub(crate) mssql_instances: HashMap<String, crate::runtime::executors::mssql::MssqlClient>,
    /// C9: Weaviate (REST + GraphQL via reqwest).
    #[cfg(feature = "weaviate")]
    pub(crate) weaviate: Option<crate::runtime::executors::weaviate::WeaviateHttpClient>,
    #[cfg(feature = "weaviate")]
    pub(crate) weaviate_instances:
        HashMap<String, crate::runtime::executors::weaviate::WeaviateHttpClient>,
    /// C9: Pinecone (REST via reqwest).
    #[cfg(feature = "pinecone")]
    pub(crate) pinecone: Option<crate::runtime::executors::pinecone::PineconeHttpClient>,
    #[cfg(feature = "pinecone")]
    pub(crate) pinecone_instances:
        HashMap<String, crate::runtime::executors::pinecone::PineconeHttpClient>,
    /// C9: Cassandra / ScyllaDB session via the `scylla` driver.
    #[cfg(feature = "cassandra")]
    pub(crate) cassandra: Option<crate::runtime::executors::cassandra::CassandraClient>,
    #[cfg(feature = "cassandra")]
    pub(crate) cassandra_instances:
        HashMap<String, crate::runtime::executors::cassandra::CassandraClient>,
    /// C9: Azure Blob Storage SDK client.
    #[cfg(feature = "azureblob")]
    pub(crate) azureblob: Option<crate::runtime::executors::azureblob::AzureBlobClient>,
    #[cfg(feature = "azureblob")]
    pub(crate) azureblob_instances:
        HashMap<String, crate::runtime::executors::azureblob::AzureBlobClient>,
    /// C9: Google Cloud Storage SDK client.
    #[cfg(feature = "gcs")]
    pub(crate) gcs: Option<crate::runtime::executors::gcs::GcsClient>,
    #[cfg(feature = "gcs")]
    pub(crate) gcs_instances: HashMap<String, crate::runtime::executors::gcs::GcsClient>,
    #[cfg(feature = "s3")]
    s3: Option<aws_sdk_s3::Client>,
    #[cfg(feature = "s3")]
    s3_instances: HashMap<String, aws_sdk_s3::Client>,
    encryption: Option<EncryptionRuntime>,
    #[cfg(feature = "mongodb")]
    mongodb: Option<MongoDbExecutor>,
    #[cfg(feature = "mongodb")]
    mongodb_instances: HashMap<String, MongoDbExecutor>,
    #[cfg(feature = "neo4j")]
    neo4j: Option<Neo4jExecutor>,
    #[cfg(feature = "neo4j")]
    neo4j_instances: HashMap<String, Neo4jExecutor>,
    #[cfg(feature = "clickhouse")]
    clickhouse: Option<ClickHouseExecutor>,
    #[cfg(feature = "clickhouse")]
    clickhouse_instances: HashMap<String, ClickHouseExecutor>,
    connections: ConnectionManager,
    backend_instances: Vec<RuntimeBackendInstance>,
    circuit_breakers: Arc<Mutex<HashMap<String, CircuitBreakerState>>>,
    routing_counters: Arc<Mutex<HashMap<String, u64>>>,
    executor_registry: BackendExecutorRegistry,
    report: RuntimeInitReport,
    config: UdbConfig,
    cache_metrics: CacheMetrics,
    encryption_metrics: EncryptionMetrics,
    channels: super::channels::ChannelManager,
    /// NW1-2: pluggable canonical-store registry. Each
    /// canonical-class backend (PG/MySQL/SQLite/Mongo) registers
    /// itself at startup; downstream system-table call sites
    /// (NW1 step 3+) route through this rather than direct PgPool.
    /// Wrapped in `Arc<Mutex<…>>` so config reload can rebuild the
    /// registry without mutating the snapshot held by in-flight
    /// requests — the swap is atomic at the `Arc` level.
    canonical_stores: Arc<Mutex<crate::runtime::canonical_store::CanonicalStoreRegistry>>,
}

impl DataBrokerRuntime {
    /// NW1-2: snapshot accessor. Returns the current registry by
    /// cloning the Arc-Mutex view (Mutex on the registry itself is
    /// only held briefly to clone the `HashMap`-backed registry).
    pub fn canonical_stores(&self) -> crate::runtime::canonical_store::CanonicalStoreRegistry {
        self.canonical_stores
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default()
    }

    /// NW1-2: look up the default canonical store. Returns `None` if
    /// no canonical store is registered (slim deployments without
    /// Postgres / MySQL / SQLite).
    pub fn default_canonical_store(
        &self,
    ) -> Option<Arc<dyn crate::runtime::canonical_store::CanonicalStore>> {
        self.canonical_stores.lock().ok()?.default_store()
    }

    /// Encrypt a UDB-owned secret string for at-rest storage (Phase 5). Returns
    /// the AES-256-GCM-SIV envelope when an encryption key is configured. When no
    /// key is configured: plaintext in dev, but ERROR in `fail_closed_mode` (so
    /// production cannot silently store secrets in the clear).
    pub fn encrypt_secret_at_rest(&self, plaintext: &str) -> Result<String, String> {
        match self.encryption.as_ref() {
            Some(enc) => {
                enc.encrypt_json_value(&serde_json::Value::String(plaintext.to_string()))
            }
            None if crate::runtime::security::fail_closed_mode() => Err(
                "encryption-at-rest required for this secret but no UDB_ENCRYPTION_KEYS/Vault key is configured"
                    .into(),
            ),
            None => Ok(plaintext.to_string()),
        }
    }

    /// Like [`Self::encrypt_secret_at_rest`] but MANDATORY: fails when no
    /// encryption key is configured REGARDLESS of `fail_closed_mode`. For callers
    /// (tenant-data backups) that must never produce a plaintext artifact labeled
    /// as encrypted (D1). Keyed deployments behave identically to the base method.
    pub fn encrypt_secret_at_rest_required(&self, plaintext: &str) -> Result<String, String> {
        match self.encryption.as_ref() {
            Some(enc) => {
                enc.encrypt_json_value(&serde_json::Value::String(plaintext.to_string()))
            }
            None => Err(
                "encryption-at-rest is required for tenant-data backups but no UDB_ENCRYPTION_KEYS/Vault key is configured"
                    .into(),
            ),
        }
    }

    /// Reverse of [`Self::encrypt_secret_at_rest`]. Plaintext (legacy/dev) and
    /// values lacking the `udb-aead:` envelope prefix pass through unchanged, so
    /// mixed plaintext/ciphertext stores remain readable during rollout.
    pub fn decrypt_secret_at_rest(&self, stored: &str) -> Result<String, String> {
        match self.encryption.as_ref() {
            Some(enc) => match enc.decrypt_json_value(stored)? {
                serde_json::Value::String(s) => Ok(s),
                other => Ok(other.to_string()),
            },
            None => Ok(stored.to_string()),
        }
    }

    /// Encrypt a JSON payload for UDB-owned native/object state.
    ///
    /// The return value is JSON text suitable for binding into a `JSONB` column:
    /// plaintext state remains an object/array JSON document in dev, while
    /// encrypted state is stored as a JSON string containing the AEAD envelope.
    pub fn encrypt_native_json_state_at_rest(&self, raw_json: &str) -> Result<String, String> {
        let value = parse_native_state_json(raw_json)?;
        match self.encryption.as_ref() {
            Some(enc) => {
                let envelope = enc.encrypt_json_value(&value)?;
                serde_json::to_string(&envelope)
                    .map_err(|err| format!("native-state envelope serialization failed: {err}"))
            }
            None if self.config.encryption.object_native_state_required
                || crate::runtime::security::fail_closed_mode() =>
            {
                Err(
                    "encryption-at-rest required for native/object state but no UDB_ENCRYPTION_KEY/Vault key is configured"
                        .into(),
                )
            }
            None => Ok(value.to_string()),
        }
    }

    /// Decrypt JSON payloads produced by [`Self::encrypt_native_json_state_at_rest`].
    /// Plain JSON objects/arrays and legacy plaintext pass through unchanged.
    pub fn decrypt_native_json_state_at_rest(&self, stored_json: &str) -> Result<String, String> {
        let trimmed = stored_json.trim();
        if trimmed.starts_with("udb-aead:") {
            let Some(enc) = self.encryption.as_ref() else {
                return Err(
                    "native/object state is encrypted but no UDB encryption key is configured"
                        .into(),
                );
            };
            return Ok(enc.decrypt_json_value(trimmed)?.to_string());
        }
        let value = parse_native_state_json(stored_json)?;
        let serde_json::Value::String(ciphertext) = value else {
            return Ok(value.to_string());
        };
        if !ciphertext.starts_with("udb-aead:") {
            return serde_json::to_string(&ciphertext)
                .map_err(|err| format!("native-state JSON string serialization failed: {err}"));
        }
        let Some(enc) = self.encryption.as_ref() else {
            return Err(
                "native/object state is encrypted but no UDB encryption key is configured".into(),
            );
        };
        Ok(enc.decrypt_json_value(&ciphertext)?.to_string())
    }

    /// NW1-3: register a store that satisfies every system-store
    /// trait. The supervisor calls this once per canonical-class
    /// backend at startup.
    pub(crate) fn register_full_canonical_store(
        &self,
        store: Arc<dyn crate::runtime::canonical_store::SystemStores>,
    ) {
        if let Ok(mut guard) = self.canonical_stores.lock() {
            guard.register_full(store);
        }
    }

    /// NW1-3: the rich-view default store. NW1 step 3+ call sites
    /// (consistency_fence, projection worker, saga worker, audit
    /// writers) pull this once and call whichever system-store
    /// methods they need.
    pub fn default_system_stores(
        &self,
    ) -> Option<Arc<dyn crate::runtime::canonical_store::SystemStores>> {
        self.canonical_stores.lock().ok()?.default_full_store()
    }
}

fn parse_native_state_json(raw_json: &str) -> Result<serde_json::Value, String> {
    let trimmed = raw_json.trim();
    if trimmed.is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_str(trimmed).map_err(|err| format!("native-state JSON is invalid: {err}"))
}

#[derive(Debug, Clone, Default)]
struct CircuitBreakerState {
    failures: u32,
    opened_until: Option<Instant>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CircuitBreakerSnapshot {
    pub backend: String,
    pub instance: String,
    pub failure_count: u32,
    pub open: bool,
    pub opened_until_unix_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct CacheMetricSnapshot {
    pub udb_cache_hit_total: u64,
    pub udb_cache_miss_total: u64,
    pub udb_cache_invalidation_total: u64,
}

#[derive(Debug, Clone, Default)]
struct CacheMetrics {
    hit_total: Arc<AtomicU64>,
    miss_total: Arc<AtomicU64>,
    invalidation_total: Arc<AtomicU64>,
}

impl CacheMetrics {
    #[cfg(feature = "redis")]
    fn hit(&self) {
        self.hit_total.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "redis")]
    fn miss(&self) {
        self.miss_total.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "redis")]
    fn invalidated(&self, count: u64) {
        self.invalidation_total.fetch_add(count, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CacheMetricSnapshot {
        CacheMetricSnapshot {
            udb_cache_hit_total: self.hit_total.load(Ordering::Relaxed),
            udb_cache_miss_total: self.miss_total.load(Ordering::Relaxed),
            udb_cache_invalidation_total: self.invalidation_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct EncryptionMetricSnapshot {
    pub encrypt_ok: u64,
    pub encrypt_error: u64,
    pub decrypt_ok: u64,
    pub decrypt_error: u64,
}

#[derive(Debug, Clone, Default)]
struct EncryptionMetrics {
    encrypt_ok: Arc<AtomicU64>,
    encrypt_error: Arc<AtomicU64>,
    decrypt_ok: Arc<AtomicU64>,
    decrypt_error: Arc<AtomicU64>,
}

impl EncryptionMetrics {
    fn record(&self, op: &str, ok: bool) {
        match (op, ok) {
            ("encrypt", true) => self.encrypt_ok.fetch_add(1, Ordering::Relaxed),
            ("encrypt", false) => self.encrypt_error.fetch_add(1, Ordering::Relaxed),
            ("decrypt", true) => self.decrypt_ok.fetch_add(1, Ordering::Relaxed),
            ("decrypt", false) => self.decrypt_error.fetch_add(1, Ordering::Relaxed),
            _ => return,
        };
    }

    fn snapshot(&self) -> EncryptionMetricSnapshot {
        EncryptionMetricSnapshot {
            encrypt_ok: self.encrypt_ok.load(Ordering::Relaxed),
            encrypt_error: self.encrypt_error.load(Ordering::Relaxed),
            decrypt_ok: self.decrypt_ok.load(Ordering::Relaxed),
            decrypt_error: self.decrypt_error.load(Ordering::Relaxed),
        }
    }
}

/// Exact durable catalog row for one project. This is deliberately distinct
/// from the global proto-schema history used during process startup: served
/// catalog transitions must publish the row committed for the requested
/// project, never whichever manifest happened to migrate last.
#[derive(Debug)]
pub(crate) struct ProjectCatalogRecord {
    pub(crate) catalog_id: String,
    pub(crate) version: String,
    pub(crate) checksum_sha256: String,
    pub(crate) manifest: CatalogManifest,
    pub(crate) status: String,
    pub(crate) compatibility_level: String,
    pub(crate) created_at_unix: i64,
}

/// Durable result of a catalog mutation. `replayed` means the same
/// project/action/idempotency key was already committed; callers must return
/// the recorded row without executing or publishing the mutation again.
#[derive(Debug)]
pub(crate) struct CatalogMutationResult {
    pub(crate) catalog: ProjectCatalogRecord,
    pub(crate) replayed: bool,
}

// Phase F: God-impl split into continuation impl blocks.
pub(crate) mod audit;
mod helpers;
pub(crate) mod pagination;
pub(crate) use helpers::*;
mod accessors;
pub(crate) use accessors::RoutedReadPool;
mod catalog_admin;
mod catalog_sql;
pub(crate) mod native_store;
pub use catalog_sql::ManifestDrift;
mod probe_dispatch;
mod reload;
pub use reload::{ConfigReloadMode, ConfigReloadOptions, ConfigReloadReport};
pub(crate) mod setup_data;

/// Operator-declared deployment tier (`UDB_DEPLOYMENT_TIER`), resolved EXACTLY
/// once at startup via a `OnceLock` (master-plan 3.5). `None` = no tier declared
/// (the permissive dev default). Public re-export of the setup-data resolver so
/// the CLI (`udb doctor`) and the `GetCapabilities` handler surface the SAME
/// cached value — no second env read, no copy of the classification.
pub fn declared_deployment_tier() -> Option<crate::backend::ControlPlaneHaLevel> {
    setup_data::declared_deployment_tier()
}
// GDPR tenant/account hard-delete ripple (planner + executor). The PurgeTenant
// gRPC handler is wired after codegen (W7/M2); the glob re-export keeps the
// planner/executor reachable without an unused-import warning until then.
pub(crate) mod tenant_purge;
pub(crate) use tenant_purge::*;
mod tx_object;

/// Result returned by `enqueue_outbox_event`.
#[derive(Debug, Clone)]
pub struct EnqueueOutboxEventResult {
    pub event_id: String,
    pub enqueued: bool,
    pub was_duplicate: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxSemantics {
    SingleBackendAcid,
    CrossBackendSaga,
}

impl TxSemantics {
    fn as_str(self) -> &'static str {
        match self {
            Self::SingleBackendAcid => "single_backend_acid",
            Self::CrossBackendSaga => "cross_backend_saga",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxStrategy {
    Saga,
    BestEffort,
    TwoPhase,
}

fn core_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.into(), description.into())],
    )
}

fn two_phase_unsupported_operation_status(operation: &str) -> tonic::Status {
    crate::runtime::executor_utils::policy_status(
        "transaction_strategy",
        "two_phase_participant_required",
        format!(
            "two_phase requested but operation '{operation}' is not a prepared-transaction participant"
        ),
    )
}

fn two_phase_disabled_status() -> tonic::Status {
    crate::runtime::executor_utils::policy_status(
        "transaction_strategy",
        "two_phase_execution_disabled",
        "two_phase requested but prepared transaction execution is disabled; \
         set UDB_2PC_ENABLED=true to enable live PREPARE TRANSACTION + COMMIT PREPARED",
    )
}

fn decrypt_encryption_key_missing_status(column_name: &str) -> tonic::Status {
    crate::runtime::executor_utils::capability_status(
        "encryption",
        "record_decryption",
        "udb_encryption_key",
        format!("column {column_name} is encrypted but UDB encryption key is not configured"),
    )
}

fn core_internal_status(operation: impl Into<String>, message: impl Into<String>) -> tonic::Status {
    internal_status("core", operation, message)
}

fn requested_tx_strategy(
    metadata_context: &RequestContext,
    mutations: &[Mutation],
) -> Result<TxStrategy, tonic::Status> {
    let mut strategy = None;
    for policy in std::iter::once(metadata_context.routing_policy.as_str()).chain(
        mutations
            .iter()
            .filter_map(|mutation| mutation.context.as_ref())
            .map(|context| context.routing_policy.as_str()),
    ) {
        let Some(parsed) = parse_tx_strategy(policy)? else {
            continue;
        };
        if let Some(existing) = strategy
            && existing != parsed
        {
            return Err(core_invalid_field(
                "routing_policy",
                "must declare at most one transaction strategy",
                "conflicting transaction strategies in request routing policy",
            ));
        }
        strategy = Some(parsed);
    }
    Ok(strategy.unwrap_or(TxStrategy::Saga))
}

fn parse_tx_strategy(policy: &str) -> Result<Option<TxStrategy>, tonic::Status> {
    for token in policy
        .split([',', ';', ' '])
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let value = token
            .strip_prefix("tx_strategy=")
            .or_else(|| token.strip_prefix("transaction_strategy="))
            .or_else(|| token.strip_prefix("tx:"))
            .unwrap_or(token)
            .trim()
            .to_ascii_lowercase();
        let parsed = match value.as_str() {
            "saga" => Some(TxStrategy::Saga),
            "best_effort" | "best-effort" | "besteffort" => Some(TxStrategy::BestEffort),
            "two_phase" | "two-phase" | "2pc" | "xa" => Some(TxStrategy::TwoPhase),
            _ if token.contains("tx_strategy=")
                || token.contains("transaction_strategy=")
                || token.starts_with("tx:") =>
            {
                return Err(core_invalid_field(
                    "routing_policy",
                    "must be saga, best_effort, or two_phase",
                    format!("unsupported transaction strategy '{value}'"),
                ));
            }
            _ => None,
        };
        if parsed.is_some() {
            return Ok(parsed);
        }
    }
    Ok(None)
}

/// B (2026-05-30): operator opt-in for live two-phase commit.
/// Re-exported from `runtime::config` so the runtime env-discipline
/// test stays satisfied (env reads live in the allowlisted config
/// module). When `UDB_2PC_ENABLED=true`, requests with
/// `tx_strategy=two_phase` no longer fail closed; the runtime
/// drives the prepared-transaction path through `XaCoordinator`.
pub(crate) use crate::runtime::config::two_phase_runtime_enabled;

fn validate_tx_strategy(strategy: TxStrategy, mutations: &[Mutation]) -> Result<(), tonic::Status> {
    match strategy {
        TxStrategy::Saga | TxStrategy::BestEffort => Ok(()),
        TxStrategy::TwoPhase => {
            let unsupported = mutations
                .iter()
                .filter(|mutation| !mutation.commit && !mutation.rollback)
                .map(|mutation| mutation.operation.to_ascii_lowercase())
                .find(|operation| !matches!(operation.as_str(), "upsert" | "delete"));
            if let Some(operation) = unsupported {
                return Err(two_phase_unsupported_operation_status(&operation));
            }
            // B: when operator has opted-in via `UDB_2PC_ENABLED=true`,
            // accept the request — `tx_object::begin_tx` will replace
            // the plain COMMIT with PREPARE TRANSACTION + COMMIT
            // PREPARED via the XA coordinator. Otherwise stay
            // fail-closed (the historical default).
            if two_phase_runtime_enabled() {
                Ok(())
            } else {
                Err(two_phase_disabled_status())
            }
        }
    }
}

fn classify_tx_semantics(mutations: &[Mutation]) -> TxSemantics {
    let has_external_side_effect = mutations
        .iter()
        .filter(|mutation| !mutation.commit && !mutation.rollback)
        .any(|mutation| {
            matches!(
                mutation.operation.to_ascii_lowercase().as_str(),
                "vector_upsert" | "put_object"
            )
        });
    if has_external_side_effect {
        TxSemantics::CrossBackendSaga
    } else {
        TxSemantics::SingleBackendAcid
    }
}

fn tx_backend_instance(mutations: &[Mutation]) -> Option<String> {
    let mut instances = mutations
        .iter()
        .filter_map(|mutation| mutation.context.as_ref())
        .map(|context| context.target_instance.trim())
        .filter(|instance| !instance.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    instances.sort();
    instances.dedup();
    match instances.len() {
        0 => None,
        1 => instances.pop(),
        _ => Some("multiple".to_string()),
    }
}

fn prepare_outbox_envelope(
    topic: &str,
    partition_key: &str,
    payload: serde_json::Value,
    schema_uri: Option<&str>,
) -> Result<(Uuid, String, serde_json::Value), tonic::Status> {
    if topic.trim().is_empty() {
        return Err(core_invalid_field(
            "topic",
            "must be non-empty",
            "outbox topic is required",
        ));
    }
    if partition_key.trim().is_empty() {
        return Err(core_invalid_field(
            "partition_key",
            "must be non-empty",
            "outbox partition_key is required",
        ));
    }
    let obj = payload.as_object().ok_or_else(|| {
        core_invalid_field(
            "payload",
            "must be a JSON object conforming to the EventEnvelope schema",
            "event payload must be a JSON object conforming to the EventEnvelope schema",
        )
    })?;
    let envelope_field = |field: &str| -> Result<&str, tonic::Status> {
        obj.get(field)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                core_invalid_field(
                    format!("payload.{field}"),
                    "must be a non-empty string",
                    format!("event payload field '{field}' must be a non-empty string"),
                )
            })
    };
    let event_id_uuid = Uuid::parse_str(envelope_field("event_id")?).map_err(|err| {
        core_invalid_field(
            "payload.event_id",
            "must be a valid UUID",
            format!("event payload field 'event_id' must be a valid UUID: {err}"),
        )
    })?;
    for field in ["event_type", "correlation_id", "document_id"] {
        envelope_field(field)?;
    }
    if crate::runtime::cdc::tenant_scoped_topic(topic) {
        envelope_field("tenant_id")?;
    }
    let document_id = envelope_field("document_id")?;
    if partition_key != document_id {
        return Err(core_invalid_field(
            "partition_key",
            "must equal payload.document_id",
            "outbox partition_key must equal payload.document_id",
        ));
    }

    let event_id = event_id_uuid.to_string();
    let mut enriched_obj = obj.clone();
    enriched_obj.insert(
        "event_id".to_string(),
        serde_json::Value::String(event_id.clone()),
    );
    if let Some(uri) = schema_uri {
        enriched_obj
            .entry("schema_uri".to_string())
            .or_insert_with(|| serde_json::Value::String(uri.to_string()));
    }
    enriched_obj
        .entry("timestamp".to_string())
        .or_insert_with(|| {
            // Canonical `Z` wire form (see logical_value_to_json).
            serde_json::Value::String(
                Utc::now().to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
            )
        });
    enriched_obj
        .entry("payload".to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    enriched_obj
        .entry("redaction_mode".to_string())
        .or_insert_with(|| serde_json::Value::String("none".to_string()));
    enriched_obj
        .entry("redaction_version".to_string())
        .or_insert_with(|| serde_json::Value::Number(serde_json::Number::from(1)));
    enriched_obj
        .entry("redacted_fields".to_string())
        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
    Ok((
        event_id_uuid,
        event_id,
        serde_json::Value::Object(enriched_obj),
    ))
}

fn artifact_content_checksum(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn extract_manifest_checksum(content: &str) -> String {
    for line in content.lines().take(16) {
        if let Some(value) = line.strip_prefix("-- UDB:proto_manifest_checksum=") {
            return value.trim().to_string();
        }
    }
    String::new()
}

fn decode_catalog_manifest_row(
    json_row: Option<(String,)>,
) -> Result<Option<CatalogManifest>, tonic::Status> {
    match json_row {
        None => Ok(None),
        Some((json,)) => match serde_json::from_str::<CatalogManifest>(&json) {
            Ok(manifest) => Ok(Some(manifest)),
            Err(err) => {
                // Schema evolution: stored manifest was serialized by an older
                // UDB binary that lacked newly-added fields. Rather than
                // aborting the migration, treat it as "no prior manifest" and
                // let the caller fall back to an idempotent bootstrap plan.
                tracing::warn!(
                    error = %err,
                    "stored manifest in proto_schema_versions is incompatible with \
                     current schema (likely a forward-migration); treating as absent \
                     — delta will regenerate all alter statements idempotently"
                );
                Ok(None)
            }
        },
    }
}

/// Build the canonical, ordered list of `(GUC key, value)` pairs the broker
/// installs to drive Postgres RLS for a request. This is the SINGLE source of
/// truth for BOTH the transaction-scoped write path and the session-scoped
/// read path, so the two can never drift: the keys/values applied (and later
/// reset) are byte-identical regardless of which host (tx vs connection) is
/// used. `app.current_*` come from `AppliedContext::session_context_pairs`
/// (item 135); `application_name` is PG-specific and added here.
fn request_local_setting_pairs(context: &RequestContext) -> Vec<(&'static str, String)> {
    let app_name = if context.correlation_id.trim().is_empty() {
        "udb".to_string()
    } else {
        format!(
            "udb/{}",
            &context.correlation_id[..context.correlation_id.len().min(58)]
        )
    };
    let applied = crate::runtime::backend_context::AppliedContext::from_request(context);
    let mut settings: Vec<(&'static str, String)> = vec![("application_name", app_name)];
    settings.extend(
        applied
            .session_context_pairs()
            .into_iter()
            .map(|(key, value)| (key, value.to_string())),
    );
    settings
}

/// Apply the request's RLS context GUCs against an arbitrary Postgres executor
/// (a transaction OR a single pooled connection). `is_local` selects the
/// `set_config` scope:
///   * `true`  — transaction-local (`SET LOCAL` semantics), auto-reset on
///     COMMIT/ROLLBACK. Used by the WRITE path, which already runs in a tx.
///   * `false` — session-level on the connection, persists until explicitly
///     reset. Used by the READ path, which acquires ONE connection, applies the
///     context, runs the SELECT, then ALWAYS resets via
///     [`reset_request_local_settings_conn`] before returning the connection to
///     the pool.
///
/// Both scopes install IDENTICAL keys/values (see `request_local_setting_pairs`);
/// only the local-vs-session flag differs, so RLS permits the exact same rows
/// either way.
async fn apply_request_local_settings<'e, E>(
    executor: E,
    context: &RequestContext,
    is_local: bool,
) -> Result<(), tonic::Status>
where
    E: sqlx::PgExecutor<'e>,
{
    let settings = request_local_setting_pairs(context);
    if settings.is_empty() {
        return Ok(());
    }
    // Perf: batch every setting into a SINGLE round-trip
    //   SELECT set_config($1,$2,<is_local>), set_config($3,$4,<is_local>), …
    // instead of one `SELECT set_config(...)` round-trip PER setting. Each
    // request previously paid 4-6 PG round-trips just to install the RLS
    // `app.current_*` context (the dominant fixed cost on the read/write fast
    // path); all `set_config(...)` calls are order-independent so collapsing
    // them is semantically identical. The `is_local` flag is a fixed boolean
    // literal (never user input), so direct interpolation is injection-safe.
    let is_local_literal = if is_local { "true" } else { "false" };
    let mut sql = String::with_capacity(8 + settings.len() * 30);
    sql.push_str("SELECT ");
    for i in 0..settings.len() {
        if i > 0 {
            sql.push_str(", ");
        }
        use std::fmt::Write as _;
        let _ = write!(
            sql,
            "set_config(${}, ${}, {is_local_literal})",
            2 * i + 1,
            2 * i + 2
        );
    }
    let mut query = sqlx::query(&sql);
    for (key, value) in &settings {
        query = query.bind(*key).bind(value.as_str());
    }
    query.execute(executor).await.map_err(|err| {
        core_internal_status(
            "set_request_context",
            format!("failed to set request database context: {err}"),
        )
    })?;
    Ok(())
}

/// WRITE-path entry point (UNCHANGED public contract): install the request's
/// RLS context as transaction-local settings inside the open write transaction.
/// Auto-resets on COMMIT/ROLLBACK, so no explicit reset is needed.
pub(crate) async fn set_request_local_settings(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &RequestContext,
) -> Result<(), tonic::Status> {
    apply_request_local_settings(&mut **tx, context, /* is_local = */ true).await
}

/// READ-path entry point: install the request's RLS context as SESSION-level
/// settings on a single pooled connection (the read path no longer opens a
/// transaction). Because session GUCs PERSIST on a pooled connection after it
/// returns to the pool, the caller MUST pair this with
/// [`reset_request_local_settings_conn`] on the SAME connection on BOTH the
/// success and error paths before the connection drops — otherwise a later
/// request reusing that connection could observe a stale (wider) tenant
/// context. See `select` / `select_join_fusion` in `setup_data.rs`.
pub(crate) async fn set_request_local_settings_conn(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    context: &RequestContext,
) -> Result<(), tonic::Status> {
    apply_request_local_settings(&mut **conn, context, /* is_local = */ false).await
}

/// READ-path teardown: clear EVERY session GUC installed by
/// [`set_request_local_settings_conn`] so the connection returns to the pool
/// with NO residual tenant context. This is leak-safety-critical and MUST run
/// unconditionally (success AND error) before the connection drops.
///
/// `RESET <name>` restores each GUC to its server/role default. We reset the
/// exact same key list that was set (iterating `request_local_setting_pairs`),
/// so no key set can survive un-reset. Defense in depth: every read/write path
/// re-applies the full context BEFORE its own query (overwrite-before-use), so
/// even a hypothetically-missed reset could not WIDEN visibility — but that is
/// NOT relied upon here; this explicit reset is the primary guard.
pub(crate) async fn reset_request_local_settings_conn(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    context: &RequestContext,
) -> Result<(), tonic::Status> {
    let settings = request_local_setting_pairs(context);
    if settings.is_empty() {
        return Ok(());
    }
    // `RESET app.current_tenant_id;` is the correct inverse of a GUC set via
    // `set_config(name, value, false)` — it restores the role/server default
    // (effectively unset for our custom `app.*` namespace). Batch all RESETs
    // into one statement to keep teardown a single round-trip. Keys are fixed
    // identifiers from `session_context_pairs` (no user input), so direct
    // interpolation is injection-safe.
    let mut sql = String::with_capacity(settings.len() * 32);
    for (key, _) in &settings {
        use std::fmt::Write as _;
        let _ = write!(sql, "RESET {key}; ");
    }
    // IMPORTANT: run the multi-statement RESET string via `Executor::execute`
    // with a raw `&str` (the SIMPLE query protocol), which DOES support several
    // `;`-separated statements in one round-trip. The prepared/extended path
    // (`sqlx::query(...).execute()`) does NOT accept multi-statement strings on
    // a PoolConnection — see control::lifecycle for the same caveat — so we must
    // NOT use it here.
    let executor: &mut sqlx::PgConnection = &mut **conn;
    executor.execute(sql.as_str()).await.map_err(|err| {
        core_internal_status(
            "reset_request_context",
            format!("failed to reset request database context: {err}"),
        )
    })?;
    Ok(())
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod rls_conn_leak_tests {
    use super::*;

    fn ctx_for(tenant: &str) -> RequestContext {
        RequestContext {
            tenant_id: tenant.to_string(),
            project_id: "p".to_string(),
            correlation_id: "rls-leak-test".to_string(),
            ..Default::default()
        }
    }

    async fn current_tenant_guc(conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>) -> String {
        let val: Option<String> =
            sqlx::query_scalar("SELECT current_setting('app.current_tenant_id', true)")
                .fetch_one(&mut **conn)
                .await
                .expect("read app.current_tenant_id");
        val.unwrap_or_default()
    }

    /// B3: the session-GUC RLS read path must NOT leak a tenant's context onto a
    /// pooled connection. Proves, on a live Postgres connection:
    ///   1. `set` installs `app.current_tenant_id` (the read path can see its tenant);
    ///   2. `reset` CLEARS it — so the next pool user inherits no residual tenant
    ///      (the cross-tenant-leak guard the txn-drop relies on);
    ///   3. `set(B)` after `set(A)` OVERWRITES — defense-in-depth, since every
    ///      request re-applies its own context before querying.
    /// Env-gated on a live PG DSN (the CI live-DB job); a no-op otherwise.
    #[tokio::test]
    async fn rls_session_settings_reset_and_overwrite_no_cross_tenant_leak() {
        let Some(dsn) = crate::runtime::core::helpers::live_postgres_dsn_for_tests() else {
            eprintln!("skipping B3 RLS leak test: set UDB_PG_DSN / DATABASE_URL");
            return;
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&dsn)
            .await
            .expect("connect live PG");
        let mut conn = pool.acquire().await.expect("acquire");

        // (1) set(A) is visible on this connection.
        set_request_local_settings_conn(&mut conn, &ctx_for("tenant-A"))
            .await
            .expect("set A");
        assert_eq!(current_tenant_guc(&mut conn).await, "tenant-A");

        // (2) reset CLEARS it — no residual tenant leaks to the next pool user.
        reset_request_local_settings_conn(&mut conn, &ctx_for("tenant-A"))
            .await
            .expect("reset A");
        assert_eq!(
            current_tenant_guc(&mut conn).await,
            "",
            "reset must clear app.current_tenant_id (cross-tenant leak guard)"
        );

        // (3) set(A) then set(B) without an intervening reset → B overwrites A.
        set_request_local_settings_conn(&mut conn, &ctx_for("tenant-A"))
            .await
            .expect("set A again");
        set_request_local_settings_conn(&mut conn, &ctx_for("tenant-B"))
            .await
            .expect("set B");
        assert_eq!(
            current_tenant_guc(&mut conn).await,
            "tenant-B",
            "a later set must overwrite the prior tenant (overwrite-before-use)"
        );

        reset_request_local_settings_conn(&mut conn, &ctx_for("tenant-B"))
            .await
            .expect("reset B");
    }
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod outbox_envelope_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use serde_json::json;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed error detail trailer");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field_violation(status: &tonic::Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_policy_detail(
        status: &tonic::Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_capability_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        capability_required: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "core");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn core_internal_status_carries_typed_detail() {
        let status = core_internal_status(
            "set_request_context",
            "failed to set request database context",
        );
        assert_internal_detail(
            &status,
            "set_request_context",
            "failed to set request database context",
        );
    }

    #[test]
    fn prepare_outbox_envelope_requires_document_partition_key() {
        let err = prepare_outbox_envelope(
            "document.uploaded.v1",
            "doc-2",
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "document.uploaded.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            None,
        )
        .unwrap_err();
        assert_eq!(
            err.message(),
            "outbox partition_key must equal payload.document_id"
        );
        assert_single_field_violation(&err, "partition_key", "must equal payload.document_id");
    }

    #[test]
    fn prepare_outbox_envelope_boundary_errors_carry_field_violations() {
        let empty_topic = prepare_outbox_envelope(
            "",
            "doc-1",
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "document.uploaded.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            None,
        )
        .unwrap_err();
        assert_eq!(empty_topic.message(), "outbox topic is required");
        assert_single_field_violation(&empty_topic, "topic", "must be non-empty");

        let empty_partition = prepare_outbox_envelope(
            "document.uploaded.v1",
            "",
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "document.uploaded.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            None,
        )
        .unwrap_err();
        assert_eq!(
            empty_partition.message(),
            "outbox partition_key is required"
        );
        assert_single_field_violation(&empty_partition, "partition_key", "must be non-empty");

        let non_object =
            prepare_outbox_envelope("document.uploaded.v1", "doc-1", json!("bad"), None)
                .unwrap_err();
        assert_eq!(
            non_object.message(),
            "event payload must be a JSON object conforming to the EventEnvelope schema"
        );
        assert_single_field_violation(
            &non_object,
            "payload",
            "must be a JSON object conforming to the EventEnvelope schema",
        );

        let invalid_event_id = prepare_outbox_envelope(
            "document.uploaded.v1",
            "doc-1",
            json!({
                "event_id": "not-a-uuid",
                "event_type": "document.uploaded.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            None,
        )
        .unwrap_err();
        assert!(
            invalid_event_id
                .message()
                .starts_with("event payload field 'event_id' must be a valid UUID:"),
            "unexpected message: {}",
            invalid_event_id.message()
        );
        assert_single_field_violation(
            &invalid_event_id,
            "payload.event_id",
            "must be a valid UUID",
        );
    }

    #[test]
    fn prepare_outbox_envelope_enriches_schema_timestamp_and_payload() {
        let (_, event_id, payload) = prepare_outbox_envelope(
            "document.uploaded.v1",
            "doc-1",
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "document.uploaded.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            Some("buf.build/example/document.uploaded.v1"),
        )
        .unwrap();
        assert_eq!(event_id, "11111111-1111-4111-8111-111111111111");
        assert_eq!(
            payload["schema_uri"],
            "buf.build/example/document.uploaded.v1"
        );
        assert!(
            payload["timestamp"]
                .as_str()
                .unwrap_or_default()
                .contains('T')
        );
        assert!(payload["payload"].is_object());
        assert_eq!(payload["redaction_mode"], "none");
        assert_eq!(payload["redaction_version"], 1);
        assert!(payload["redacted_fields"].as_array().is_some());
    }

    #[test]
    fn prepare_outbox_envelope_requires_tenant_for_udb_topics() {
        let err = prepare_outbox_envelope(
            "udb.storage.file.finalized.v1",
            "doc-1",
            json!({
                "event_id": "11111111-1111-4111-8111-111111111111",
                "event_type": "udb.storage.file.finalized.v1",
                "correlation_id": "corr-1",
                "document_id": "doc-1"
            }),
            None,
        )
        .unwrap_err();
        assert_eq!(
            err.message(),
            "event payload field 'tenant_id' must be a non-empty string"
        );
        assert_single_field_violation(&err, "payload.tenant_id", "must be a non-empty string");
    }

    #[test]
    fn split_backend_selector_accepts_colon_and_dot() {
        assert_eq!(
            split_backend_selector("postgres:primary"),
            ("postgres", Some("primary"))
        );
        assert_eq!(
            split_backend_selector("qdrant.vector_a"),
            ("qdrant", Some("vector_a"))
        );
        assert_eq!(split_backend_selector("mongodb"), ("mongodb", None));
    }

    #[test]
    fn resolve_backend_selector_validates_named_instance() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![RuntimeBackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: Some("UDB_PG_DSN".to_string()),
                labels: HashMap::new(),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            }],
            ..DataBrokerRuntime::default()
        };

        let resolved = runtime
            .resolve_backend_selector("postgres:primary")
            .unwrap();
        assert_eq!(resolved.backend, "postgres");
        assert_eq!(resolved.instance.as_deref(), Some("primary"));
        assert_eq!(
            runtime
                .resolve_backend_selector("postgres:missing")
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }

    #[test]
    fn resolve_backend_targets_supports_all_and_label_filters() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![
                RuntimeBackendInstance {
                    name: "vector_a".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 1,
                    dsn_env: None,
                    labels: HashMap::from([("region".to_string(), "local".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
                RuntimeBackendInstance {
                    name: "vector_b".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 0,
                    dsn_env: None,
                    labels: HashMap::from([("region".to_string(), "remote".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
            ],
            ..DataBrokerRuntime::default()
        };

        let all = runtime.resolve_backend_targets("qdrant:*", "{}").unwrap();
        assert_eq!(all.len(), 2);
        let local = runtime
            .resolve_backend_targets("qdrant", r#"{"target_labels":{"region":"local"}}"#)
            .unwrap();
        assert_eq!(local.len(), 1);
        assert_eq!(local[0].instance.as_deref(), Some("vector_a"));
    }

    #[test]
    fn resolve_backend_selector_filters_named_instance_by_project_label() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![RuntimeBackendInstance {
                name: "billing_vector".to_string(),
                backend: "qdrant".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: None,
                labels: HashMap::from([("project_id".to_string(), "billing".to_string())]),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            }],
            ..DataBrokerRuntime::default()
        };

        let resolved = runtime
            .resolve_backend_selector_for_project("qdrant:billing_vector", "billing")
            .unwrap();
        assert_eq!(resolved.instance.as_deref(), Some("billing_vector"));
        assert_eq!(
            runtime
                .resolve_backend_selector_for_project("qdrant:billing_vector", "hr")
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }

    #[test]
    fn projection_routing_requires_project_bound_instance() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![RuntimeBackendInstance {
                name: "billing_vector".to_string(),
                backend: "qdrant".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: None,
                labels: HashMap::from([("project_id".to_string(), "billing".to_string())]),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            }],
            ..DataBrokerRuntime::default()
        };

        let selected = runtime
            .resolve_projection_write_target_for_project("qdrant", None, "billing")
            .unwrap();
        assert_eq!(selected.instance.as_deref(), Some("billing_vector"));
        let read_selected = runtime
            .resolve_projection_read_target_for_project("qdrant", None, "billing")
            .unwrap();
        assert_eq!(read_selected.instance.as_deref(), Some("billing_vector"));
        assert!(
            runtime
                .resolve_projection_write_target_for_project(
                    "qdrant",
                    Some("billing_vector"),
                    "hr",
                )
                .is_err()
        );
        assert!(
            runtime
                .resolve_projection_write_target_for_project("qdrant", None, "hr")
                .is_err()
        );
        assert!(
            runtime
                .resolve_projection_read_target_for_project("qdrant", Some("billing_vector"), "hr",)
                .is_err()
        );
        assert!(
            runtime
                .resolve_projection_read_target_for_project("qdrant", None, "hr")
                .is_err()
        );
        assert!(
            runtime
                .resolve_projection_write_target_for_project("qdrant", None, "")
                .is_err()
        );
    }

    #[test]
    fn resolve_backend_targets_filters_wildcards_by_project_labels() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![
                RuntimeBackendInstance {
                    name: "billing_vector".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 1,
                    dsn_env: None,
                    labels: HashMap::from([("projects".to_string(), "billing,ocr".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
                RuntimeBackendInstance {
                    name: "hr_vector".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 1,
                    dsn_env: None,
                    labels: HashMap::from([("project".to_string(), "hr".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
                RuntimeBackendInstance {
                    name: "global_vector".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 1,
                    dsn_env: None,
                    labels: HashMap::new(),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
            ],
            ..DataBrokerRuntime::default()
        };

        let billing = runtime
            .resolve_backend_targets_for_project("qdrant:*", "{}", "billing")
            .unwrap();
        assert_eq!(
            billing
                .iter()
                .filter_map(|target| target.instance.as_deref())
                .collect::<Vec<_>>(),
            vec!["billing_vector", "global_vector"]
        );
    }

    #[test]
    fn strict_project_routing_blocks_unlabeled_instances() {
        let runtime = DataBrokerRuntime {
            config: UdbConfig {
                project_routing_mode: "strict".to_string(),
                ..UdbConfig::default()
            },
            backend_instances: vec![RuntimeBackendInstance {
                name: "global_vector".to_string(),
                backend: "qdrant".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: None,
                labels: HashMap::new(),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            }],
            ..DataBrokerRuntime::default()
        };

        assert_eq!(
            runtime
                .resolve_backend_selector_for_project("qdrant:global_vector", "billing")
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert!(
            runtime
                .resolve_backend_selector_for_project("qdrant:global_vector", "default")
                .is_ok()
        );
    }

    #[test]
    fn strict_project_routing_blocks_direct_postgres_instance_pool_lookup() {
        let runtime = DataBrokerRuntime {
            config: UdbConfig {
                project_routing_mode: "strict".to_string(),
                ..UdbConfig::default()
            },
            backend_instances: vec![RuntimeBackendInstance {
                name: "primary".to_string(),
                backend: "postgres".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: None,
                labels: HashMap::new(),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            }],
            ..DataBrokerRuntime::default()
        };
        let context = RequestContext {
            project_id: "billing".to_string(),
            target_backend: "postgres".to_string(),
            target_instance: "primary".to_string(),
            ..RequestContext::default()
        };

        let err = runtime
            .pg_read_pool_for_context_checked(&context)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("strict routing"));
    }

    #[test]
    fn choose_instance_name_filters_by_project_labels() {
        let runtime = DataBrokerRuntime {
            backend_instances: vec![
                RuntimeBackendInstance {
                    name: "hr_vector".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 100,
                    write_weight: 100,
                    dsn_env: None,
                    labels: HashMap::from([("project".to_string(), "hr".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
                RuntimeBackendInstance {
                    name: "billing_vector".to_string(),
                    backend: "qdrant".to_string(),
                    role: "read_write".to_string(),
                    enabled: true,
                    configured: true,
                    connected: true,
                    read_weight: 1,
                    write_weight: 1,
                    dsn_env: None,
                    labels: HashMap::from([("project".to_string(), "billing".to_string())]),
                    capabilities: Vec::new(),
                    healthy: true,
                    circuit_open: false,
                },
            ],
            ..DataBrokerRuntime::default()
        };

        assert_eq!(
            runtime.choose_instance_name_for_project("qdrant", false, "billing"),
            Some("billing_vector")
        );
    }

    #[test]
    fn classify_tx_semantics_distinguishes_acid_from_saga() {
        let relational = vec![Mutation {
            operation: "upsert".to_string(),
            message_type: "Patient".to_string(),
            ..Mutation::default()
        }];
        assert_eq!(
            classify_tx_semantics(&relational),
            TxSemantics::SingleBackendAcid
        );

        let cross_backend = vec![
            Mutation {
                operation: "upsert".to_string(),
                message_type: "Patient".to_string(),
                ..Mutation::default()
            },
            Mutation {
                operation: "vector_upsert".to_string(),
                collection: "patient_embeddings".to_string(),
                ..Mutation::default()
            },
        ];
        assert_eq!(
            classify_tx_semantics(&cross_backend),
            TxSemantics::CrossBackendSaga
        );
    }

    #[test]
    fn tx_backend_instance_reports_single_or_multiple_targets() {
        let one = vec![Mutation {
            context: Some(crate::proto::RequestContext {
                target_instance: "primary".to_string(),
                ..Default::default()
            }),
            ..Mutation::default()
        }];
        assert_eq!(tx_backend_instance(&one).as_deref(), Some("primary"));

        let multiple = vec![
            Mutation {
                context: Some(crate::proto::RequestContext {
                    target_instance: "a".to_string(),
                    ..Default::default()
                }),
                ..Mutation::default()
            },
            Mutation {
                context: Some(crate::proto::RequestContext {
                    target_instance: "b".to_string(),
                    ..Default::default()
                }),
                ..Mutation::default()
            },
        ];
        assert_eq!(tx_backend_instance(&multiple).as_deref(), Some("multiple"));
    }

    #[test]
    fn requested_tx_strategy_parses_and_rejects_conflicts() {
        let metadata = RequestContext {
            routing_policy: "tx_strategy=best_effort".to_string(),
            ..Default::default()
        };
        assert_eq!(
            requested_tx_strategy(&metadata, &[]).unwrap(),
            TxStrategy::BestEffort
        );

        let conflict = requested_tx_strategy(
            &metadata,
            &[Mutation {
                context: Some(crate::proto::RequestContext {
                    routing_policy: "tx_strategy=saga".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        )
        .unwrap_err();
        assert_eq!(
            conflict.message(),
            "conflicting transaction strategies in request routing policy"
        );
        assert_single_field_violation(
            &conflict,
            "routing_policy",
            "must declare at most one transaction strategy",
        );

        let unsupported = parse_tx_strategy("tx_strategy=maybe").unwrap_err();
        assert_eq!(
            unsupported.message(),
            "unsupported transaction strategy 'maybe'"
        );
        assert_single_field_violation(
            &unsupported,
            "routing_policy",
            "must be saga, best_effort, or two_phase",
        );
    }

    /// Serialise env-flipping 2PC tests so they don't race with
    /// each other on the process-wide `UDB_2PC_ENABLED` variable.
    fn two_phase_env_lock() -> &'static std::sync::Mutex<()> {
        use std::sync::OnceLock;
        static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[test]
    fn two_phase_strategy_fails_before_side_effects() {
        let _g = two_phase_env_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // SAFETY: test reads/writes UDB_2PC_ENABLED. Make sure the
        // env is unset before asserting the fail-closed branch.
        unsafe {
            std::env::remove_var("UDB_2PC_ENABLED");
        }
        let relational_only = vec![Mutation {
            operation: "upsert".to_string(),
            message_type: "Patient".to_string(),
            ..Default::default()
        }];
        let err = validate_tx_strategy(TxStrategy::TwoPhase, &relational_only).unwrap_err();
        assert_policy_detail(
            &err,
            "transaction_strategy",
            "two_phase_execution_disabled",
            "two_phase requested but prepared transaction execution is disabled; set UDB_2PC_ENABLED=true to enable live PREPARE TRANSACTION + COMMIT PREPARED",
        );

        let external = vec![Mutation {
            operation: "vector_upsert".to_string(),
            collection: "patient_embeddings".to_string(),
            ..Default::default()
        }];
        let err = validate_tx_strategy(TxStrategy::TwoPhase, &external).unwrap_err();
        assert_policy_detail(
            &err,
            "transaction_strategy",
            "two_phase_participant_required",
            "two_phase requested but operation 'vector_upsert' is not a prepared-transaction participant",
        );
    }

    #[test]
    fn decrypt_without_configured_key_carries_capability_detail() {
        let metrics = EncryptionMetrics::default();
        let err = decrypt_record_value(
            None,
            &metrics,
            "email",
            JsonValue::String("udb-aead:v1:test".to_string()),
        )
        .expect_err("ciphertext without configured encryption key must fail");

        assert_capability_detail(
            &err,
            "encryption",
            "record_decryption",
            "udb_encryption_key",
            "column email is encrypted but UDB encryption key is not configured",
        );
        assert_eq!(metrics.snapshot().decrypt_error, 1);
    }

    #[test]
    fn b_two_phase_accepted_when_runtime_enabled_env_set() {
        let _g = two_phase_env_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        // B (2026-05-30): with UDB_2PC_ENABLED=true the validate
        // step accepts the request and lets begin_tx drive the
        // live PREPARE TRANSACTION + COMMIT PREPARED path.
        unsafe {
            std::env::set_var("UDB_2PC_ENABLED", "true");
        }
        let relational_only = vec![Mutation {
            operation: "upsert".to_string(),
            message_type: "Patient".to_string(),
            ..Default::default()
        }];
        let res = validate_tx_strategy(TxStrategy::TwoPhase, &relational_only);
        unsafe {
            std::env::remove_var("UDB_2PC_ENABLED");
        }
        assert!(res.is_ok(), "got: {res:?}");
    }

    #[test]
    fn b_runtime_toggle_recognises_common_truthy_values() {
        // Tests cargo runs in parallel; reading the process env
        // races with sibling tests that set/clear it. Drive the
        // pure parser instead.
        use crate::runtime::config::parse_two_phase_toggle;
        for v in ["1", "true", "TRUE", "Yes", "on"] {
            assert!(parse_two_phase_toggle(Some(v)), "should be true for '{v}'");
        }
        for v in ["0", "false", "no", "off", ""] {
            assert!(
                !parse_two_phase_toggle(Some(v)),
                "should be false for '{v}'"
            );
        }
        assert!(!parse_two_phase_toggle(None), "None must be false");
    }

    #[test]
    fn backend_executor_resolves_registered_instance() {
        let instances = vec![RuntimeBackendInstance {
            name: "vector_a".to_string(),
            backend: "qdrant".to_string(),
            role: "read_write".to_string(),
            enabled: true,
            configured: true,
            connected: true,
            read_weight: 1,
            write_weight: 1,
            dsn_env: None,
            labels: HashMap::from([("region".to_string(), "local".to_string())]),
            capabilities: Vec::new(),
            healthy: true,
            circuit_open: false,
        }];
        let runtime = DataBrokerRuntime {
            executor_registry: build_executor_registry(&instances),
            backend_instances: instances,
            ..DataBrokerRuntime::default()
        };

        let executor = runtime
            .backend_executor("qdrant", Some("vector_a"))
            .unwrap();
        assert_eq!(executor.backend, "qdrant");
        assert_eq!(executor.instance.as_deref(), Some("vector_a"));
        assert!(
            runtime
                .executor_registry()
                .get("qdrant", Some("vector_a"))
                .is_some()
        );
    }

    #[test]
    fn dispatch_reconciliation_disconnects_unbuildable_factory() {
        let mut instances = vec![RuntimeBackendInstance {
            name: "primary".to_string(),
            backend: "postgres".to_string(),
            role: "read_write".to_string(),
            enabled: true,
            configured: true,
            connected: true,
            read_weight: 1,
            write_weight: 1,
            dsn_env: None,
            labels: HashMap::new(),
            capabilities: Vec::new(),
            healthy: true,
            circuit_open: false,
        }];
        let runtime = DataBrokerRuntime::default();
        let mut warnings = Vec::new();

        reconcile_dispatch_factories(&mut instances, &runtime, &mut warnings);

        assert!(!instances[0].connected);
        assert!(!instances[0].healthy);
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("postgres:primary")
                    && warning.contains("could not build executor")),
            "warnings: {warnings:?}"
        );
        let registry = build_executor_registry(&instances);
        assert!(
            registry
                .get("postgres", Some("primary"))
                .is_some_and(|registration| !registration.connected)
        );
    }

    #[tokio::test]
    async fn checked_runtime_constructor_rejects_invalid_config_before_registration() {
        let config = UdbConfig {
            primary: DbConfig {
                host: "pg".to_string(),
                database: "db".to_string(),
                role: "app".to_string(),
                ..DbConfig::default()
            },
            backend_instances: BackendInstanceConfig {
                instances: vec![BackendInstance {
                    name: "cache".to_string(),
                    backend: "redis".to_string(),
                    dsn: Some("not a dsn".to_string()),
                    dsn_env: None,
                    ..BackendInstance::default()
                }],
            },
            ..UdbConfig::default()
        };

        let result = DataBrokerRuntime::try_from_config(config).await;
        let err = match result {
            Ok(_) => panic!("invalid config should fail before runtime registration"),
            Err(err) => err,
        };
        assert!(err.contains("config validation failed"));
        assert!(err.contains("does not look valid"));
    }

    #[test]
    fn circuit_breaker_failover_skips_open_instance() {
        let instances = vec![
            RuntimeBackendInstance {
                name: "vector_a".to_string(),
                backend: "qdrant".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 10,
                write_weight: 10,
                dsn_env: None,
                labels: HashMap::from([("region".to_string(), "a".to_string())]),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            },
            RuntimeBackendInstance {
                name: "vector_b".to_string(),
                backend: "qdrant".to_string(),
                role: "read_write".to_string(),
                enabled: true,
                configured: true,
                connected: true,
                read_weight: 1,
                write_weight: 1,
                dsn_env: None,
                labels: HashMap::from([("region".to_string(), "b".to_string())]),
                capabilities: Vec::new(),
                healthy: true,
                circuit_open: false,
            },
        ];
        let runtime = DataBrokerRuntime {
            config: UdbConfig {
                circuit_breaker: crate::runtime::config::CircuitBreakerSettings {
                    failure_threshold: 1,
                    cooldown_secs: 60,
                },
                ..UdbConfig::default()
            },
            executor_registry: build_executor_registry(&instances),
            backend_instances: instances,
            ..DataBrokerRuntime::default()
        };

        runtime.record_backend_result("qdrant", Some("vector_a"), false);
        assert!(!runtime.circuit_breaker_allows("qdrant", Some("vector_a")));
        let explicit_err = match runtime.backend_executor("qdrant", Some("vector_a")) {
            Ok(_) => panic!("open circuit should reject explicitly targeted vector_a"),
            Err(err) => err,
        };
        assert_eq!(explicit_err.code(), tonic::Code::Unavailable);
        let executor = runtime.backend_executor("qdrant", None).unwrap();
        assert_eq!(executor.instance.as_deref(), Some("vector_b"));

        let targets = runtime.resolve_backend_targets("qdrant:*", "{}").unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].instance.as_deref(), Some("vector_b"));

        runtime.record_backend_result("qdrant", Some("vector_b"), false);
        let all_open_err = runtime
            .backend_executor("qdrant", None)
            .expect_err("all registered executors with open circuits must be retryable");
        assert_eq!(all_open_err.code(), tonic::Code::Unavailable);
        assert!(all_open_err.message().contains("circuit breaker is open"));
    }

    #[test]
    fn pg_dispatch_sql_validation_separates_reads_and_writes() {
        assert!(validate_pg_read_sql("SELECT 1").is_ok());
        assert!(validate_pg_read_sql("WITH x AS (SELECT 1) SELECT * FROM x").is_ok());
        assert_eq!(
            validate_pg_read_sql("DELETE FROM users")
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert!(validate_pg_mutation_sql("INSERT INTO audit_log(message) VALUES ($1)").is_ok());
        assert!(validate_pg_mutation_sql("UPDATE users SET name = $1 WHERE id = $2").is_ok());
        assert_eq!(
            validate_pg_mutation_sql("DROP TABLE users")
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            validate_pg_read_sql("SELECT 1; SELECT 2")
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn dispatch_params_accepts_params_or_parameters_array() {
        assert_eq!(
            dispatch_params(&json!({"params": [1, "two"]})).unwrap(),
            vec![json!(1), json!("two")]
        );
        assert_eq!(
            dispatch_params(&json!({"parameters": [true]})).unwrap(),
            vec![json!(true)]
        );
        assert_eq!(
            dispatch_params(&json!({"params": {"bad": true}}))
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn object_bytes_from_json_accepts_base64_and_text() {
        let base64 = object_bytes_from_json(&json!({"data_base64": "aGVsbG8="})).unwrap();
        assert_eq!(base64, b"hello");
        let text = object_bytes_from_json(&json!({"content_text": "plain"})).unwrap();
        assert_eq!(text, b"plain");
    }

    // ── Phase 5: encryption-at-rest secret helpers ───────────────────────────

    /// With an encryption key configured, `encrypt_secret_at_rest` produces an
    /// AEAD envelope that `decrypt_secret_at_rest` reverses to the original.
    #[tokio::test]
    async fn secret_at_rest_round_trips_with_key() {
        use crate::runtime::config::EncryptionSettings;
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        let mut settings = EncryptionSettings::default();
        // 32-byte all-0x2a key, base64-encoded (decode path in encryption.rs).
        settings.keys.insert(1, B64.encode([0x2au8; 32]));
        let enc = EncryptionRuntime::from_settings(&settings)
            .await
            .expect("settings parse")
            .expect("a key was configured");
        let runtime = DataBrokerRuntime {
            encryption: Some(enc),
            ..DataBrokerRuntime::default()
        };

        let secret = "-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----";
        let sealed = runtime.encrypt_secret_at_rest(secret).unwrap();
        assert!(
            sealed.starts_with("udb-aead:v"),
            "sealed value must be an AEAD envelope, got {sealed}"
        );
        assert_ne!(sealed, secret, "ciphertext must differ from plaintext");
        assert_eq!(runtime.decrypt_secret_at_rest(&sealed).unwrap(), secret);
        // Legacy/dev plaintext (no envelope prefix) passes through unchanged.
        assert_eq!(
            runtime.decrypt_secret_at_rest("legacy-plaintext").unwrap(),
            "legacy-plaintext"
        );
    }

    /// Without an encryption key configured, the dev path stores plaintext and
    /// the round-trip is the identity (no envelope, no error).
    #[test]
    fn secret_at_rest_passthrough_without_key_in_dev() {
        // SAFETY: single-threaded unit test; ensures we are not in fail_closed.
        unsafe {
            std::env::remove_var("UDB_FAIL_CLOSED");
            std::env::remove_var("UDB_ENTERPRISE_AUDIT");
        }
        if crate::runtime::security::fail_closed_mode() {
            // Production-like environment; the dev passthrough assertion does not
            // apply. The fail-closed error path is asserted by the next branch.
            return;
        }
        let runtime = DataBrokerRuntime::default();
        let sealed = runtime.encrypt_secret_at_rest("plain-secret").unwrap();
        assert_eq!(sealed, "plain-secret");
        assert_eq!(
            runtime.decrypt_secret_at_rest("plain-secret").unwrap(),
            "plain-secret"
        );
    }

    #[tokio::test]
    async fn native_json_state_round_trips_with_key() {
        use crate::runtime::config::{EncryptionSettings, UdbConfig};
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        let mut settings = EncryptionSettings::default();
        settings.object_native_state_required = true;
        settings.keys.insert(1, B64.encode([0x33u8; 32]));
        let enc = EncryptionRuntime::from_settings(&settings)
            .await
            .expect("settings parse")
            .expect("a key was configured");
        let runtime = DataBrokerRuntime {
            encryption: Some(enc),
            config: UdbConfig {
                encryption: settings,
                ..UdbConfig::default()
            },
            ..DataBrokerRuntime::default()
        };

        let plaintext = serde_json::json!({"pii":"a@example.com","nested":{"ok":true}}).to_string();
        let stored = runtime
            .encrypt_native_json_state_at_rest(&plaintext)
            .expect("encrypt native state");
        let envelope: serde_json::Value = serde_json::from_str(&stored).unwrap();
        assert!(
            envelope
                .as_str()
                .unwrap_or_default()
                .starts_with("udb-aead:v"),
            "encrypted native state should be stored as a JSON string envelope"
        );
        let restored = runtime
            .decrypt_native_json_state_at_rest(&stored)
            .expect("decrypt native state");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&restored).unwrap(),
            serde_json::from_str::<serde_json::Value>(&plaintext).unwrap()
        );
    }

    #[test]
    fn native_json_state_fails_closed_when_required_without_key() {
        let runtime = DataBrokerRuntime {
            config: crate::runtime::config::UdbConfig {
                encryption: crate::runtime::config::EncryptionSettings {
                    object_native_state_required: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..DataBrokerRuntime::default()
        };
        let err = runtime
            .encrypt_native_json_state_at_rest(r#"{"secret":"value"}"#)
            .expect_err("required native-state encryption must fail without a key");
        assert!(err.contains("native/object state"));
    }

    #[test]
    fn native_json_state_plaintext_passes_through_in_dev() {
        // SAFETY: single-threaded unit test; ensures we are not in fail_closed.
        unsafe {
            std::env::remove_var("UDB_FAIL_CLOSED");
            std::env::remove_var("UDB_ENTERPRISE_AUDIT");
        }
        if crate::runtime::security::fail_closed_mode() {
            return;
        }
        let runtime = DataBrokerRuntime::default();
        let json = r#"{"public":"metadata"}"#;
        assert_eq!(
            runtime.encrypt_native_json_state_at_rest(json).unwrap(),
            json
        );
        assert_eq!(
            runtime.decrypt_native_json_state_at_rest(json).unwrap(),
            json
        );
    }
}

// ── Internal postgres helpers, join fusion, bind helpers are in postgres_helpers.rs ──
// ── rows_to_record_set, decrypt_record_value, row_value_to_json remain here   ──
// ── because they depend on the private EncryptionMetrics type.                 ──

/// Whether a row set is being built for a CLIENT response (PII masking applies)
/// or for the broker's own internal comparison (it must see stored values).
///
/// This used to be a `masked_columns: &[String]` parameter, and four of its five
/// call sites passed `&[]` — so `return_record` on Upsert/Update handed back PII
/// that the very same caller's `Select` masked. A mask that every caller has to
/// remember to supply is a mask some caller will forget, so the column set is now
/// derived from the table here and the only choice left is an explicit, named
/// intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PiiMasking<'a> {
    /// Mask `is_pii` / `mask_in_logs` columns unless the caller holds
    /// `udb:pii:read`. Correct for anything that reaches a client.
    ClientVisible,
    /// Same policy, but for a row set whose columns are NOT this table's bare
    /// column names — join fusion aliases every output as
    /// `"{Message}__{column}"`, so a mask keyed on bare names would match
    /// nothing. The caller supplies the already-aliased names.
    ClientVisibleAliased(&'a [String]),
    /// Return stored values verbatim. ONLY for internal reads the broker
    /// compares against stored state (e.g. the CAS precondition), where a
    /// `***MASKED***` placeholder would break the comparison.
    InternalUnmasked,
}

fn rows_to_record_set(
    rows: Vec<PgRow>,
    table: Option<&ManifestTable>,
    masking: PiiMasking<'_>,
    context: &RequestContext,
    encryption: Option<&EncryptionRuntime>,
    encryption_metrics: &EncryptionMetrics,
) -> Result<RecordSet, tonic::Status> {
    let can_read_pii = context
        .scopes
        .iter()
        .any(|scope| scope == "udb:pii:read" || scope == "udb:*" || scope == "*");
    // Same rule as the planner's `masked_columns(table)`: PII plus anything
    // flagged mask-in-logs.
    let masked_columns: Vec<String> = match (masking, table) {
        (PiiMasking::ClientVisible, Some(table)) => table
            .columns
            .iter()
            .filter(|column| column.security.is_pii || column.security.mask_in_logs)
            .map(|column| column.column_name.clone())
            .collect(),
        (PiiMasking::ClientVisibleAliased(columns), _) => columns.to_vec(),
        (PiiMasking::ClientVisible, None) | (PiiMasking::InternalUnmasked, _) => Vec::new(),
    };
    // Preallocate the result vectors to the known row count so the per-row push
    // loop never reallocates+copies the growing Vec (matters for large result
    // sets — the row-build path is the relational read hot path).
    let mut proto_rows = Vec::with_capacity(rows.len());
    let mut records_json = Vec::with_capacity(rows.len());
    for row in rows {
        // The default relational read path serialises every row into
        // `records_json` (the bytes every SDK actually decodes). The per-row
        // proto `fields` map was a parallel representation that no client reads,
        // so we no longer build it on this path: that drops a per-row HashMap
        // allocation plus a `json_to_prost_value` convert per column. The proto
        // `ProtoRow.fields` field is preserved in the message (emitted empty)
        // to keep the wire contract intact.
        //
        // Size the per-row json map to the column count up front, so a wide row
        // does not rehash/regrow cell-by-cell.
        let columns = row.columns();
        let mut json_row = serde_json::Map::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            let name = column.name().to_string();
            let mut json_value = row_value_to_json(&row, idx, column.type_info().name())?;
            if table
                .and_then(|table| {
                    table
                        .columns
                        .iter()
                        .find(|column| column.column_name == name)
                })
                .is_some_and(is_encrypted_column)
            {
                json_value =
                    decrypt_record_value(encryption, encryption_metrics, &name, json_value)?;
            }
            if masked_columns.contains(&name) && !can_read_pii {
                json_value = JsonValue::String("***MASKED***".to_string());
            }
            json_row.insert(name, json_value);
        }
        records_json.push(
            serde_json::to_vec(&JsonValue::Object(json_row)).map_err(|err| {
                core_internal_status(
                    "serialize_record_json",
                    format!("failed to serialize record JSON: {err}"),
                )
            })?,
        );
        proto_rows.push(ProtoRow::default());
    }
    Ok(RecordSet {
        total_count: proto_rows.len() as i32,
        rows: proto_rows,
        records_json,
        ..RecordSet::default()
    })
}

fn decrypt_record_value(
    encryption: Option<&EncryptionRuntime>,
    metrics: &EncryptionMetrics,
    column_name: &str,
    value: JsonValue,
) -> Result<JsonValue, tonic::Status> {
    let JsonValue::String(ciphertext) = value else {
        return Ok(value);
    };
    if !is_ciphertext(&ciphertext) {
        return Ok(JsonValue::String(ciphertext));
    }
    let Some(encryption) = encryption else {
        metrics.record("decrypt", false);
        return Err(decrypt_encryption_key_missing_status(column_name));
    };
    match encryption.decrypt_json_value(&ciphertext) {
        Ok(value) => {
            metrics.record("decrypt", true);
            Ok(value)
        }
        Err(err) => {
            metrics.record("decrypt", false);
            Err(core_internal_status(
                "decrypt_record_value",
                format!("failed to decrypt column {column_name}: {err}"),
            ))
        }
    }
}

/// Decode the PostgreSQL binary wire format of an `inet`/`cidr` value to its
/// canonical text form. Layout: `[family, netmask_bits, is_cidr, addr_len, addr…]`
/// (family 2 = IPv4, 3 = IPv6). A host `inet` (full mask, non-cidr) renders without
/// the mask ("10.1.2.3"), matching PostgreSQL's own `::text`; a `cidr` or masked
/// `inet` renders "addr/bits". Returns `None` on a malformed buffer.
fn pg_inet_to_text(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 4 {
        return None;
    }
    let family = bytes[0];
    let bits = bytes[1];
    let is_cidr = bytes[2] != 0;
    let addr_len = bytes[3] as usize;
    let addr = bytes.get(4..4 + addr_len)?;
    let (ip, full_bits) = match family {
        2 if addr_len == 4 => (
            format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]),
            32u8,
        ),
        3 if addr_len == 16 => {
            let segments: Vec<String> = (0..8)
                .map(|i| format!("{:x}", u16::from_be_bytes([addr[2 * i], addr[2 * i + 1]])))
                .collect();
            (segments.join(":"), 128u8)
        }
        _ => return None,
    };
    if is_cidr || bits != full_bits {
        Some(format!("{ip}/{bits}"))
    } else {
        Some(ip)
    }
}

/// Decode the PostgreSQL binary wire format of a `macaddr` (6 bytes) / `macaddr8`
/// (8 bytes) value to canonical colon-separated hex ("08:00:2b:01:02:03").
fn pg_macaddr_to_text(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 6 && bytes.len() != 8 {
        return None;
    }
    Some(
        bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<Vec<_>>()
            .join(":"),
    )
}

#[cfg(test)]
mod inet_decode_tests {
    use super::{pg_inet_to_text, pg_macaddr_to_text};

    #[test]
    fn inet_binary_decodes_to_canonical_text() {
        // The exact bytes from the bug report: `022000040A010203` = 10.1.2.3.
        // family=2(v4) bits=0x20(32) is_cidr=0 len=4 addr=10.1.2.3 → host, no mask.
        assert_eq!(
            pg_inet_to_text(&[0x02, 0x20, 0x00, 0x04, 0x0A, 0x01, 0x02, 0x03]),
            Some("10.1.2.3".to_string())
        );
        // A masked/cidr value keeps the /bits suffix.
        assert_eq!(
            pg_inet_to_text(&[0x02, 0x18, 0x01, 0x04, 0x0A, 0x01, 0x02, 0x00]),
            Some("10.1.2.0/24".to_string())
        );
        // Malformed buffers decode to None (→ NULL, never a panic).
        assert_eq!(pg_inet_to_text(&[0x02, 0x20]), None);
    }

    #[test]
    fn macaddr_binary_decodes_to_colon_hex() {
        assert_eq!(
            pg_macaddr_to_text(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]),
            Some("08:00:2b:01:02:03".to_string())
        );
        assert_eq!(pg_macaddr_to_text(&[0x08, 0x00]), None);
    }
}

fn row_value_to_json(row: &PgRow, idx: usize, type_name: &str) -> Result<JsonValue, tonic::Status> {
    let type_name = type_name.to_ascii_uppercase();
    // W2: arrays MUST be matched BEFORE the scalar branches. sqlx reports a
    // bigint array as `INT8[]`, so `contains("INT8")` below would route it into
    // the scalar i64 decoder, which fails and used to collapse a populated array
    // to JSON null. Decode the SQLx-enabled element matrix exactly and preserve
    // nullable elements. NUMERIC[] / DECIMAL[] remain intentionally unsupported:
    // this build does not enable a sqlx decimal codec, so do not claim them in the
    // supported matrix or silently route them through a scalar decoder.
    if let Some(element) = type_name.strip_suffix("[]") {
        if matches!(element, "NUMERIC" | "DECIMAL") {
            return Err(core_internal_status(
                "read_column",
                format!(
                    "PostgreSQL {element}[] decoding requires a decimal codec that is not enabled"
                ),
            ));
        }

        fn wrap<T: Into<JsonValue>>(items: Option<Vec<Option<T>>>) -> JsonValue {
            match items {
                Some(items) => JsonValue::Array(
                    items
                        .into_iter()
                        .map(|item| item.map(Into::into).unwrap_or(JsonValue::Null))
                        .collect(),
                ),
                None => JsonValue::Null,
            }
        }

        let decoded = match element {
            "INT2" | "SMALLINT" => row.try_get::<Option<Vec<Option<i16>>>, _>(idx).map(wrap),
            "INT4" | "INTEGER" | "INT" => row.try_get::<Option<Vec<Option<i32>>>, _>(idx).map(wrap),
            "INT8" | "BIGINT" => row.try_get::<Option<Vec<Option<i64>>>, _>(idx).map(wrap),
            "FLOAT8" | "DOUBLE PRECISION" => {
                row.try_get::<Option<Vec<Option<f64>>>, _>(idx).map(wrap)
            }
            "FLOAT4" | "REAL" => row.try_get::<Option<Vec<Option<f32>>>, _>(idx).map(wrap),
            "BOOL" | "BOOLEAN" => row.try_get::<Option<Vec<Option<bool>>>, _>(idx).map(wrap),
            "UUID" => {
                row.try_get::<Option<Vec<Option<uuid::Uuid>>>, _>(idx)
                    .map(|items| match items {
                        Some(items) => JsonValue::Array(
                            items
                                .into_iter()
                                .map(|item| {
                                    item.map(|value| JsonValue::from(value.to_string()))
                                        .unwrap_or(JsonValue::Null)
                                })
                                .collect(),
                        ),
                        None => JsonValue::Null,
                    })
            }
            _ => row.try_get::<Option<Vec<Option<String>>>, _>(idx).map(wrap),
        }
        .map_err(|err| {
            core_internal_status(
                "read_column",
                format!("PostgreSQL {type_name} array decode failed: {err}"),
            )
        })?;
        return Ok(decoded);
    }
    // INT2/SMALLINT MUST decode via i16 — sqlx's i32 decoder rejects the INT2 OID
    // with a type-mismatch, which the NULL fallback would otherwise swallow into a
    // silent NULL for a non-null column (bug_report 2026-07-16 #3).
    if type_name.contains("INT2") || type_name == "SMALLINT" {
        return Ok(row
            .try_get::<Option<i16>, _>(idx)
            .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("INT4") || type_name == "INTEGER" || type_name == "INT" {
        return Ok(row
            .try_get::<Option<i32>, _>(idx)
            .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("INT8") || type_name == "BIGINT" {
        return Ok(row
            .try_get::<Option<i64>, _>(idx)
            .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("FLOAT") || type_name.contains("DOUBLE") || type_name.contains("REAL") {
        // W1: sqlx-postgres decodes float4/REAL ONLY as f32 and float8/DOUBLE ONLY
        // as f64 (type-exact, no widening). A single f64 probe therefore returned
        // Err for every float4/REAL column → unwrap_or(Null) = SILENT data loss on
        // read. Probe f64 first (float8/double), then fall back to f32 (float4/real).
        return Ok(row
            .try_get::<Option<f64>, _>(idx)
            .or_else(|_| {
                row.try_get::<Option<f32>, _>(idx)
                    .map(|value| value.map(f64::from))
            })
            .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    // GAP 10: NUMERIC / DECIMAL — deserialise as string to avoid floating-point
    // precision loss.  Clients can parse the string value to their preferred
    // arbitrary-precision type.
    if type_name.contains("NUMERIC") || type_name.contains("DECIMAL") {
        return Ok(row
            .try_get::<Option<String>, _>(idx)
            .or_else(|_| {
                // Fallback: try f64 and convert to string when text cast fails.
                row.try_get::<Option<f64>, _>(idx)
                    .map(|v| v.map(|f| f.to_string()))
            })
            .map(|value| value.map(JsonValue::String).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("BOOL") {
        return Ok(row
            .try_get::<Option<bool>, _>(idx)
            .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("UUID") {
        return Ok(row
            .try_get::<Option<Uuid>, _>(idx)
            .map(|value| {
                value
                    .map(|uuid| JsonValue::String(uuid.to_string()))
                    .unwrap_or(JsonValue::Null)
            })
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("JSON") {
        return Ok(row
            .try_get::<Option<sqlx::types::Json<JsonValue>>, _>(idx)
            .map(|value| value.map(|json| json.0).unwrap_or(JsonValue::Null))
            .unwrap_or(JsonValue::Null));
    }
    // GAP 10: BYTEA — encode as base64 string so the binary data survives JSON.
    if type_name == "BYTEA" {
        use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
        return Ok(row
            .try_get::<Option<Vec<u8>>, _>(idx)
            .map(|value| {
                value
                    .map(|bytes| JsonValue::String(B64.encode(&bytes)))
                    .unwrap_or(JsonValue::Null)
            })
            .unwrap_or(JsonValue::Null));
    }
    if type_name.contains("TIMESTAMPTZ") {
        return Ok(row
            .try_get::<Option<DateTime<Utc>>, _>(idx)
            .map(|value| {
                value
                    // Canonical `Z` wire form, not `+00:00` (Z-3): consumers
                    // parse the broker's timestamp rendering across six SDKs;
                    // one canonical shape beats per-SDK layout ladders.
                    .map(|dt| {
                        JsonValue::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
                    })
                    .unwrap_or(JsonValue::Null)
            })
            .unwrap_or(JsonValue::Null));
    }
    if type_name == "TIMESTAMP" {
        return Ok(row
            .try_get::<Option<NaiveDateTime>, _>(idx)
            .map(|value| {
                value
                    .map(|dt| JsonValue::String(dt.to_string()))
                    .unwrap_or(JsonValue::Null)
            })
            .unwrap_or(JsonValue::Null));
    }
    if type_name == "DATE" {
        return Ok(row
            .try_get::<Option<NaiveDate>, _>(idx)
            .map(|value| {
                value
                    .map(|dt| JsonValue::String(dt.to_string()))
                    .unwrap_or(JsonValue::Null)
            })
            .unwrap_or(JsonValue::Null));
    }
    // INET / CIDR / MACADDR: sqlx's String decoder is TEXT-family-only and REJECTS
    // these OIDs, and the generic hex-of-binary fallback below would return the raw
    // PG binary wire form (e.g. `022000040A010203`), which is asymmetric — feeding
    // it back on a write fails 22P02. Decode the binary wire format to its canonical
    // TEXT form ("10.1.2.3", "10.1.2.3/24", "08:00:2b:01:02:03") so the value
    // round-trips symmetrically with the `$n::INET`/`::MACADDR` write cast.
    // (bug_report 2026-07-16 #1c — the old "GAP 10 falls through to the string
    // catch-all which handles these correctly" claim was wrong.)
    if type_name.contains("INET") || type_name.contains("CIDR") || type_name.contains("MACADDR") {
        use sqlx::ValueRef as _;
        let raw = row.try_get_raw(idx).map_err(|err| {
            core_internal_status("read_column", format!("row decode failed: {err}"))
        })?;
        if raw.is_null() {
            return Ok(JsonValue::Null);
        }
        let bytes = raw.as_bytes().map_err(|err| {
            core_internal_status("read_column", format!("raw column read failed: {err}"))
        })?;
        let text = match raw.format() {
            sqlx::postgres::PgValueFormat::Binary => {
                if type_name.contains("MACADDR") {
                    pg_macaddr_to_text(bytes)
                } else {
                    pg_inet_to_text(bytes)
                }
            }
            sqlx::postgres::PgValueFormat::Text => {
                Some(String::from_utf8_lossy(bytes).into_owned())
            }
        };
        return Ok(text.map(JsonValue::String).unwrap_or(JsonValue::Null));
    }
    // TSVECTOR and other text-representable types: handled by the String catch-all.
    // An unknown/user-defined OID (e.g. PostGIS `geography`/`geometry`) makes sqlx's
    // String decoder REJECT the value; the old `.unwrap_or(JsonValue::Null)` then
    // swallowed that decode error into a silent NULL for a NOT-NULL column
    // (bug_report 2026-07-16 #1b — silent data loss). Fall back to the RAW value and
    // preserve it as a string: PostGIS emits binary EWKB, which we hex-encode into
    // the exact hex-EWKB form clients also supply on write (a symmetric round-trip);
    // an already-text payload passes through.
    match row.try_get::<Option<String>, _>(idx) {
        Ok(value) => Ok(value.map(JsonValue::from).unwrap_or(JsonValue::Null)),
        Err(_) => {
            use sqlx::ValueRef as _;
            let raw = row.try_get_raw(idx).map_err(|err| {
                core_internal_status("read_column", format!("row decode failed: {err}"))
            })?;
            if raw.is_null() {
                return Ok(JsonValue::Null);
            }
            let format = raw.format();
            let bytes = raw.as_bytes().map_err(|err| {
                core_internal_status("read_column", format!("raw column read failed: {err}"))
            })?;
            match format {
                sqlx::postgres::PgValueFormat::Binary => Ok(JsonValue::String(
                    bytes.iter().map(|b| format!("{b:02X}")).collect(),
                )),
                sqlx::postgres::PgValueFormat::Text => Ok(JsonValue::String(
                    String::from_utf8_lossy(bytes).into_owned(),
                )),
            }
        }
    }
}
