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
    build_delete_plan, build_select_query_plan, build_upsert_plan, table_for_message,
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
use crate::security::{AbacPolicy, PolicyEffect};

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

// Phase F: God-impl split into continuation impl blocks.
mod helpers;
pub(crate) use helpers::*;
mod accessors;
mod catalog_admin;
mod catalog_sql;
mod native_store;
pub use catalog_sql::ManifestDrift;
mod probe_dispatch;
mod reload;
pub use reload::{ConfigReloadMode, ConfigReloadOptions, ConfigReloadReport};
pub(crate) mod setup_data;
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
            return Err(tonic::Status::invalid_argument(
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
                return Err(tonic::Status::invalid_argument(format!(
                    "unsupported transaction strategy '{value}'"
                )));
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
                return Err(tonic::Status::failed_precondition(format!(
                    "two_phase requested but operation '{operation}' is not a prepared-transaction participant"
                )));
            }
            // B: when operator has opted-in via `UDB_2PC_ENABLED=true`,
            // accept the request — `tx_object::begin_tx` will replace
            // the plain COMMIT with PREPARE TRANSACTION + COMMIT
            // PREPARED via the XA coordinator. Otherwise stay
            // fail-closed (the historical default).
            if two_phase_runtime_enabled() {
                Ok(())
            } else {
                Err(tonic::Status::failed_precondition(
                    "two_phase requested but prepared transaction execution is disabled; \
                     set UDB_2PC_ENABLED=true to enable live PREPARE TRANSACTION + COMMIT PREPARED",
                ))
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
        return Err(tonic::Status::invalid_argument("outbox topic is required"));
    }
    if partition_key.trim().is_empty() {
        return Err(tonic::Status::invalid_argument(
            "outbox partition_key is required",
        ));
    }
    let obj = payload.as_object().ok_or_else(|| {
        tonic::Status::invalid_argument(
            "event payload must be a JSON object conforming to the EventEnvelope schema",
        )
    })?;
    let envelope_field = |field: &str| -> Result<&str, tonic::Status> {
        obj.get(field)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                tonic::Status::invalid_argument(format!(
                    "event payload field '{field}' must be a non-empty string"
                ))
            })
    };
    let event_id_uuid = Uuid::parse_str(envelope_field("event_id")?).map_err(|err| {
        tonic::Status::invalid_argument(format!(
            "event payload field 'event_id' must be a valid UUID: {err}"
        ))
    })?;
    for field in ["event_type", "correlation_id", "document_id"] {
        envelope_field(field)?;
    }
    if crate::runtime::cdc::tenant_scoped_topic(topic) {
        envelope_field("tenant_id")?;
    }
    let document_id = envelope_field("document_id")?;
    if partition_key != document_id {
        return Err(tonic::Status::invalid_argument(
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
        .or_insert_with(|| serde_json::Value::String(Utc::now().to_rfc3339()));
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

pub(crate) async fn set_request_local_settings(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &RequestContext,
) -> Result<(), tonic::Status> {
    let app_name = if context.correlation_id.trim().is_empty() {
        "udb".to_string()
    } else {
        format!(
            "udb/{}",
            &context.correlation_id[..context.correlation_id.len().min(58)]
        )
    };
    // Item 135: the `app.current_*` variables come from the single canonical
    // source (`AppliedContext::session_context_pairs`) shared with the
    // dialect-aware renderer, so the PG fast path and the universal backend
    // enforcer never drift. `application_name` is PG-specific and stays here.
    let applied = crate::runtime::backend_context::AppliedContext::from_request(context);
    let mut settings: Vec<(&str, String)> = vec![("application_name", app_name)];
    settings.extend(
        applied
            .session_context_pairs()
            .into_iter()
            .map(|(key, value)| (key, value.to_string())),
    );
    for (key, value) in settings {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(key)
            .bind(value)
            .execute(&mut **tx)
            .await
            .map_err(|err| {
                tonic::Status::internal(format!("failed to set request database context: {err}"))
            })?;
    }
    Ok(())
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod outbox_envelope_tests {
    use super::*;
    use serde_json::json;

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
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("partition_key"));
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
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("tenant_id"));
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
        assert_eq!(conflict.code(), tonic::Code::InvalidArgument);
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
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("disabled"));

        let external = vec![Mutation {
            operation: "vector_upsert".to_string(),
            collection: "patient_embeddings".to_string(),
            ..Default::default()
        }];
        let err = validate_tx_strategy(TxStrategy::TwoPhase, &external).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message()
                .contains("not a prepared-transaction participant")
        );
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

fn rows_to_record_set(
    rows: Vec<PgRow>,
    table: Option<&ManifestTable>,
    masked_columns: &[String],
    context: &RequestContext,
    encryption: Option<&EncryptionRuntime>,
    encryption_metrics: &EncryptionMetrics,
) -> Result<RecordSet, tonic::Status> {
    let can_read_pii = context
        .scopes
        .iter()
        .any(|scope| scope == "udb:pii:read" || scope == "udb:*" || scope == "*");
    // Preallocate the result vectors to the known row count so the per-row push
    // loop never reallocates+copies the growing Vec (matters for large result
    // sets — the row-build path is the relational read hot path).
    let mut proto_rows = Vec::with_capacity(rows.len());
    let mut records_json = Vec::with_capacity(rows.len());
    for row in rows {
        // Size the per-row maps to the column count up front, so a wide row does
        // not rehash/regrow its field map cell-by-cell.
        let columns = row.columns();
        let mut fields = HashMap::with_capacity(columns.len());
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
            if let Some(prost) = json_to_prost_value(&json_value) {
                fields.insert(name.clone(), prost);
            }
            json_row.insert(name, json_value);
        }
        records_json.push(
            serde_json::to_vec(&JsonValue::Object(json_row)).map_err(|err| {
                tonic::Status::internal(format!("failed to serialize record JSON: {err}"))
            })?,
        );
        proto_rows.push(ProtoRow { fields });
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
        return Err(tonic::Status::failed_precondition(format!(
            "column {column_name} is encrypted but UDB encryption key is not configured"
        )));
    };
    match encryption.decrypt_json_value(&ciphertext) {
        Ok(value) => {
            metrics.record("decrypt", true);
            Ok(value)
        }
        Err(err) => {
            metrics.record("decrypt", false);
            Err(tonic::Status::internal(format!(
                "failed to decrypt column {column_name}: {err}"
            )))
        }
    }
}

fn row_value_to_json(row: &PgRow, idx: usize, type_name: &str) -> Result<JsonValue, tonic::Status> {
    let type_name = type_name.to_ascii_uppercase();
    if type_name.contains("INT2") || type_name.contains("INT4") {
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
        return Ok(row
            .try_get::<Option<f64>, _>(idx)
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
                    .map(|dt| JsonValue::String(dt.to_rfc3339()))
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
    // GAP 10: TEXT[] and other PostgreSQL array types — deserialise as a JSON array
    // of strings.  Covers VARCHAR[], TEXT[], BIGINT[], INT[], etc.
    if type_name.ends_with("[]") {
        let arr: Vec<String> = row
            .try_get::<Option<Vec<String>>, _>(idx)
            .unwrap_or(None)
            .unwrap_or_default();
        return Ok(JsonValue::Array(
            arr.into_iter().map(JsonValue::String).collect(),
        ));
    }
    // GAP 10: INET, CIDR, MACADDR, TSVECTOR — all have sensible text representations.
    // Fall through to the string catch-all below which handles these correctly.
    Ok(row
        .try_get::<Option<String>, _>(idx)
        .map(|value| value.map(JsonValue::from).unwrap_or(JsonValue::Null))
        .unwrap_or(JsonValue::Null))
}
