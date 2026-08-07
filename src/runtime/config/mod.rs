// src/config.rs — Universal Data Broker configuration types.
//
// `UdbConfig` is the top-level configuration consumed by the Go UDB service.
// It is produced by loading `configs/database.yaml` (with env-var expansion) and
// merging it with `.env` / `.env.dev` / `.env.prod` files at startup.
//
// The Rust library exposes these types so the CLI tool can emit a config skeleton,
// validate a supplied YAML file, or print a summary of the resolved configuration.
//
// Storage tiers covered by UdbConfig (spec §16.1):
//   Tier 1 SQL     — primary / backup / signal  : `DbConfig`   (PostgreSQL)
//   Tier 2 Cache   — session / rate-limit        : `RedisConfig`
//   Tier 3 Vector  — embeddings / ANN search     : `QdrantConfig`
//   Tier 4 Object  — artifacts / exports         : `MinioConfig`
//
// Aligned with:
//   - legacy Go service db/config.go       (Config, DatabaseConfig, DBConfig)  — Tier 1 only
//   - UDB spec §16.1                       (four storage tiers)
//   - UDB spec §16.3                       (migration options)
//
// ── Deploy mode ───────────────────────────────────────────────────────────────
//
// Each backend can be declared as `SelfHosted` (Docker / on-premise) or `Cloud`
// (managed service).  The deploy mode drives sensible defaults for TLS, ports,
// endpoint format, auth, and health-check paths without requiring the operator
// to set every individual field.
//
// Detection order (per backend):
//   1. Explicit `UDB_<BACKEND>_DEPLOY_MODE` env-var override (`self_hosted`|`cloud`)
//   2. Cloud-provider DSN pattern heuristic (e.g. `.neon.tech`, `.upstash.io`,
//      `.qdrant.io`, `.amazonaws.com`)
//   3. DOCKER-inherited signals (`DOCKER_HOST`, `UDB_DOCKER` env-vars set, or
//      default hostname of `localhost` / `127.0.0.1` / bare service name)
//   4. Falls back to `SelfHosted`
//
// Reference env-vars for each backend:
//   Postgres  : UDB_PG_DEPLOY_MODE   / DATABASE_URL / UDB_PG_DSN
//   Redis     : UDB_REDIS_DEPLOY_MODE / REDIS_URL / UDB_REDIS_DSN
//   Qdrant    : UDB_QDRANT_DEPLOY_MODE / QDRANT_URL / UDB_QDRANT_DSN
//   MinIO/S3  : UDB_MINIO_DEPLOY_MODE / AWS_ENDPOINT_URL / UDB_MINIO_DSN
//   MongoDB   : UDB_MONGO_DEPLOY_MODE / MONGODB_URI / UDB_NOSQL_DSN
//   Neo4j     : UDB_NEO4J_DEPLOY_MODE / NEO4J_URI / UDB_GRAPH_DSN
//   ClickHouse: UDB_CH_DEPLOY_MODE   / CLICKHOUSE_URL / UDB_COLUMN_DSN

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::control::tracker::DEFAULT_LEDGER_SCHEMA;
use crate::generation::{UnifiedDsnCatalog, parse_unified_dsn};
use crate::runtime::executor_utils::{env_first, env_i32, env_u32};
use crate::runtime::{cdc::CdcConfig, channels::ChannelSettings, security::SecurityConfig};

// ── Default helpers ───────────────────────────────────────────────────────────

pub(crate) const DEFAULT_DB_MIN_CONNECTIONS: i32 = 5;
pub(crate) const DEFAULT_DB_MAX_OPEN_CONNS: i32 = 50;
pub(crate) const DEFAULT_DB_ACQUIRE_TIMEOUT_SECS: u64 = 10;
pub(crate) const DEFAULT_DB_IDLE_TIMEOUT_SECS: u64 = 600;
pub(crate) const DEFAULT_DB_MAX_LIFETIME_SECS: u64 = 1800;

fn five_i32() -> i32 {
    DEFAULT_DB_MIN_CONNECTIONS
}
fn ten_u64() -> u64 {
    DEFAULT_DB_ACQUIRE_TIMEOUT_SECS
}
fn hundred_i32() -> i32 {
    100
}
fn thousand_i32() -> i32 {
    1000
}
/// Default startup DDL concurrency. **Serial (1)** on purpose: bootstrap SQL
/// artifacts touch shared PostgreSQL catalog rows, and applying them in parallel
/// makes Postgres reject the loser with `tuple concurrently updated`, aborting a
/// crash-looping startup non-deterministically. Serial is the safe default for a
/// startup gate; operators opt into parallelism with `UDB_DDL_CONCURRENCY=N`.
fn default_ddl_concurrency() -> usize {
    1
}

fn bool_env(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .map(|value| parse_bool_env_value(&value))
}

fn parse_bool_env_value(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "off"
    )
}

fn csv_env(key: &str) -> Option<Vec<String>> {
    std::env::var(key).ok().map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .map(ToString::to_string)
            .collect()
    })
}

// ── Object-store streaming tunables (A.6) ─────────────────────────────────────
//
// Defaults for the typed PutObject/GetObject streaming path live here (the
// central settings home), so a missing env var falls back to the value below
// rather than to a literal scattered across the object executors. Each resolver
// reads its `UDB_*` override and applies the default when unset/invalid.

/// DoS ceiling for a single typed object upload/download. Override:
/// `UDB_MAX_OBJECT_BYTES`. Default 5 GiB.
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
pub(crate) const DEFAULT_MAX_OBJECT_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// S3-mandated floor for non-final multipart parts (≥5 MiB).
#[cfg(feature = "s3")]
pub(crate) const S3_MIN_PART_BYTES: usize = 5 * 1024 * 1024;
/// S3 multipart part size. Override: `UDB_S3_MULTIPART_PART_BYTES`. Default 8 MiB.
#[cfg(feature = "s3")]
pub(crate) const DEFAULT_S3_MULTIPART_PART_BYTES: usize = 8 * 1024 * 1024;
/// S3 streaming-download frame size. Override: `UDB_S3_DOWNLOAD_CHUNK_BYTES`.
/// Default 256 KiB.
#[cfg(feature = "s3")]
pub(crate) const DEFAULT_S3_DOWNLOAD_CHUNK_BYTES: usize = 256 * 1024;
/// Azure staged-block size. Override: `UDB_AZURE_BLOCK_BYTES`. Default 8 MiB.
#[cfg(feature = "azureblob")]
pub(crate) const DEFAULT_AZURE_BLOCK_BYTES: usize = 8 * 1024 * 1024;

fn usize_env_or(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(default)
}

// ── Metrics label-cardinality bounds (D.4) ────────────────────────────────────
// Untrusted resource names (table/collection/bucket/…) become Prometheus label
// values; cap their size + distinct count here so a tenant can't blow up
// cardinality. Resolved centrally (raw values still go to traces/logs).

/// Max metric-label length. Override `UDB_METRIC_LABEL_MAX_LEN`. Default 64.
pub(crate) const DEFAULT_METRIC_LABEL_MAX_LEN: usize = 64;
/// Max distinct values admitted per bounded label dimension (others →
/// `"overflow"`). Override `UDB_METRIC_MAX_DISTINCT_LABELS`. Default 512.
pub(crate) const DEFAULT_METRIC_MAX_DISTINCT_LABELS: usize = 512;

pub(crate) fn metric_label_max_len() -> usize {
    usize_env_or("UDB_METRIC_LABEL_MAX_LEN", DEFAULT_METRIC_LABEL_MAX_LEN)
}

pub(crate) fn metric_max_distinct_labels() -> usize {
    usize_env_or(
        "UDB_METRIC_MAX_DISTINCT_LABELS",
        DEFAULT_METRIC_MAX_DISTINCT_LABELS,
    )
}

/// Env-var prefix for the per-backend raw-dispatch opt-out
/// (`UDB_DISPATCH_ALLOW_RAW_<BACKEND>`, e.g. `UDB_DISPATCH_ALLOW_RAW_POSTGRES`).
pub(crate) const RAW_DISPATCH_OPT_OUT_PREFIX: &str = "UDB_DISPATCH_ALLOW_RAW_";

/// True if the operator opted a specific mediated backend out of the raw-dispatch
/// gate (master-plan item 2.1) via `UDB_DISPATCH_ALLOW_RAW_<BACKEND>` set truthy.
/// Resolved ONCE into a static map (never re-read per request). It lives here, in
/// the startup/config boundary, so the hot-path generic-dispatch handler file
/// carries no env reads (the `connection_manager` hot-path/env guards forbid env
/// access in `handlers_*.rs`). `backend` is the canonical lowercase token
/// (e.g. `postgres`, `sqlserver`); the env suffix is its uppercase form.
pub(crate) fn raw_dispatch_opt_out(backend: &str) -> bool {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    static MAP: OnceLock<HashMap<String, bool>> = OnceLock::new();
    let map = MAP.get_or_init(|| {
        let mut map = HashMap::new();
        for (key, value) in std::env::vars() {
            let Some(suffix) = key.strip_prefix(RAW_DISPATCH_OPT_OUT_PREFIX) else {
                continue;
            };
            let truthy = matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
            map.insert(suffix.to_ascii_lowercase(), truthy);
        }
        map
    });
    map.get(&backend.to_ascii_lowercase())
        .copied()
        .unwrap_or(false)
}

/// Typed-object DoS ceiling in bytes: `UDB_MAX_OBJECT_BYTES` or
/// [`DEFAULT_MAX_OBJECT_BYTES`].
#[cfg(any(feature = "s3", feature = "gcs", feature = "azureblob"))]
pub(crate) fn max_object_bytes() -> u64 {
    std::env::var("UDB_MAX_OBJECT_BYTES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&value| value > 0)
        .unwrap_or(DEFAULT_MAX_OBJECT_BYTES)
}

/// S3 multipart part size, clamped up to [`S3_MIN_PART_BYTES`] so a
/// misconfiguration can't produce an upload S3 rejects.
#[cfg(feature = "s3")]
pub(crate) fn s3_multipart_part_bytes() -> usize {
    usize_env_or(
        "UDB_S3_MULTIPART_PART_BYTES",
        DEFAULT_S3_MULTIPART_PART_BYTES,
    )
    .max(S3_MIN_PART_BYTES)
}

/// S3 streaming-download frame size.
#[cfg(feature = "s3")]
pub(crate) fn s3_download_chunk_bytes() -> usize {
    usize_env_or(
        "UDB_S3_DOWNLOAD_CHUNK_BYTES",
        DEFAULT_S3_DOWNLOAD_CHUNK_BYTES,
    )
}

/// Azure staged-block size.
#[cfg(feature = "azureblob")]
pub(crate) fn azure_block_bytes() -> usize {
    usize_env_or("UDB_AZURE_BLOCK_BYTES", DEFAULT_AZURE_BLOCK_BYTES)
}

/// Assemble a PostgreSQL DSN from libpq-style component env vars
/// (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE`,
/// `PGCHANNELBINDING`).
///
/// Managed Postgres providers (Neon, Supabase, RDS, …) and the `psql` client
/// emit these standard variables rather than a single URL. Previously UDB only
/// read `UDB_PG_DSN` / `DATABASE_URL`, so a `.env` populated straight from a
/// provider's dashboard left `postgres_configured()` false and startup aborted
/// with "PostgreSQL is required". This fallback builds an equivalent DSN so
/// those deployments work without hand-assembling a URL.
///
/// Returns `None` unless at least `PGHOST` and `PGDATABASE` are present, so it
/// never fabricates a half-formed DSN. User and password are percent-encoded so
/// credentials containing reserved characters survive URL parsing.
fn pg_dsn_from_libpq_env() -> Option<String> {
    let host = env_first(&["PGHOST"])?;
    let database = env_first(&["PGDATABASE"])?;
    let port = env_first(&["PGPORT"]).unwrap_or_else(|| "5432".to_string());

    let mut authority = String::new();
    if let Some(user) = env_first(&["PGUSER"]) {
        authority.push_str(&urlencoding::encode(&user));
        if let Some(password) = env_first(&["PGPASSWORD"]) {
            authority.push(':');
            authority.push_str(&urlencoding::encode(&password));
        }
        authority.push('@');
    }

    let mut dsn = format!("postgresql://{authority}{host}:{port}/{database}");

    // libpq `sslmode` maps directly onto the sqlx/libpq query parameter.
    // `channel_binding` is negotiated automatically by SCRAM over TLS, so it is
    // intentionally not appended (sqlx rejects unknown query parameters).
    if let Some(sslmode) = env_first(&["PGSSLMODE"]) {
        dsn.push_str("?sslmode=");
        dsn.push_str(&sslmode);
    }
    Some(dsn)
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TlsSettings {
    pub cert_pem: Option<String>,
    pub cert_path: Option<String>,
    pub key_pem: Option<String>,
    pub key_path: Option<String>,
    pub client_ca_pem: Option<String>,
    pub client_ca_path: Option<String>,
}

// Manual `Debug` redacting the inline TLS private key (`key_pem`). Certificates
// and CA bundles are public material and file paths are not secret, so those
// print normally; only the private key is masked.
impl std::fmt::Debug for TlsSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsSettings")
            .field("cert_pem", &self.cert_pem)
            .field("cert_path", &self.cert_path)
            .field("key_pem", &self.key_pem.as_ref().map(|_| "[redacted]"))
            .field("key_path", &self.key_path)
            .field("client_ca_pem", &self.client_ca_pem)
            .field("client_ca_path", &self.client_ca_path)
            .finish()
    }
}

impl Default for TlsSettings {
    fn default() -> Self {
        Self {
            cert_pem: None,
            cert_path: None,
            key_pem: None,
            key_path: None,
            client_ca_pem: None,
            client_ca_path: None,
        }
    }
}

impl TlsSettings {
    pub fn has_server_identity(&self) -> bool {
        (has_non_empty(&self.cert_pem) || has_non_empty(&self.cert_path))
            && (has_non_empty(&self.key_pem) || has_non_empty(&self.key_path))
    }

    pub fn has_client_ca(&self) -> bool {
        has_non_empty(&self.client_ca_pem) || has_non_empty(&self.client_ca_path)
    }

    pub fn merge_env(&mut self) {
        if let Ok(value) = std::env::var("UDB_TLS_CERT_PEM") {
            self.cert_pem = Some(value);
        }
        if let Ok(value) = std::env::var("UDB_TLS_CERT_PATH") {
            self.cert_path = Some(value);
        }
        if let Ok(value) = std::env::var("UDB_TLS_KEY_PEM") {
            self.key_pem = Some(value);
        }
        if let Ok(value) = std::env::var("UDB_TLS_KEY_PATH") {
            self.key_path = Some(value);
        }
        if let Ok(value) = std::env::var("UDB_MTLS_CLIENT_CA_PEM") {
            self.client_ca_pem = Some(value);
        }
        if let Ok(value) = std::env::var("UDB_MTLS_CLIENT_CA_PATH") {
            self.client_ca_path = Some(value);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceSettings {
    pub metrics_addr: String,
    pub metrics_allowed_cidr: Option<String>,
    pub abac_refresh_secs: u64,
    pub abac_default_allow: bool,
    pub grpc_timeout_secs: u64,
    pub grpc_max_concurrent: usize,
    pub allow_degraded_backends: bool,
    pub catalog_compat_warn_only: bool,
    pub catalog_compatibility_level: String,
    pub rate_limit_enabled: bool,
    pub rate_limit_window_secs: u64,
    pub rate_limit_max_per_window: u32,
    /// Per-key data-plane rate limiting (`UDB_RATE_LIMIT_PER_KEY`, default off).
    /// OFF: the DataBroker limiter keys one bucket per `(tenant, operation)` and
    /// every principal in the tenant shares `rate_limit_max_per_window`.
    /// ON: the bucket becomes per-principal `(tenant, credential_id, operation)`
    /// and a credential's `api_keys.rate_limit_per_minute` column RAISES its own
    /// budget above the tenant default (converted to the window). It is
    /// **raise-only** — a low/default column value never throttles a key below
    /// the tenant default (the column is `NOT NULL DEFAULT 60`, so honoring it as
    /// an absolute ceiling would silently strangle every default key).
    pub rate_limit_per_key_enabled: bool,
    /// W4 (consumer tip 4): the DECLARED limiter-outage posture. What happens
    /// to data-plane requests when the rate-limit Redis is unreachable (after
    /// the in-call re-dial):
    ///   "closed" (default) — typed retryable UNAVAILABLE; Redis is a hard
    ///     data-plane dependency (today's fail-closed stance, now explicit).
    ///   "local" — degrade to an in-process fixed window (enforcement loosens
    ///     to N×limit across N replicas instead of vanishing), with a
    ///     rate_limit_degraded metric + health-report flag.
    ///   "open" — allow, with the same degraded metric/flag.
    pub rate_limit_failure_mode: String,
    pub require_secure_transport: bool,
    pub mtls_required: bool,
    pub broker_to_broker_mtls_required: bool,
    pub internal_control_mtls_required: bool,
    pub tls: TlsSettings,
}

impl Default for ServiceSettings {
    fn default() -> Self {
        Self {
            metrics_addr: "0.0.0.0:50052".to_string(),
            metrics_allowed_cidr: None,
            abac_refresh_secs: 60,
            abac_default_allow: false,
            grpc_timeout_secs: 30,
            grpc_max_concurrent: 200,
            allow_degraded_backends: false,
            catalog_compat_warn_only: false,
            catalog_compatibility_level: "backward".to_string(),
            rate_limit_enabled: true,
            rate_limit_window_secs: 60,
            rate_limit_max_per_window: 1000,
            rate_limit_per_key_enabled: false,
            rate_limit_failure_mode: "closed".to_string(),
            require_secure_transport: false,
            mtls_required: false,
            broker_to_broker_mtls_required: false,
            internal_control_mtls_required: false,
            tls: TlsSettings::default(),
        }
    }
}

impl ServiceSettings {
    fn apply_security_posture(
        &mut self,
        production_env: bool,
        explicit_secure: Option<bool>,
        explicit_mtls: Option<bool>,
    ) {
        if production_env {
            // UDB_FRICTION §1: production mandates TLS + mTLS. If the operator
            // explicitly asked to turn either OFF, that request is being
            // overridden — say so loudly instead of silently re-forcing it (the
            // silent re-force previously cost real debugging time).
            if explicit_secure == Some(false) {
                tracing::warn!(
                    "UDB_ENV=production overrides UDB_TLS_REQUIRED/UDB_REQUIRE_SECURE_TRANSPORT=false: \
                     secure transport is FORCED ON in production (provide UDB_TLS_CERT_PATH + \
                     UDB_TLS_KEY_PATH, or unset UDB_ENV=production to opt out)"
                );
            }
            if explicit_mtls == Some(false) {
                tracing::warn!(
                    "UDB_ENV=production overrides UDB_MTLS_REQUIRED=false: mTLS is FORCED ON in \
                     production (provide UDB_MTLS_CLIENT_CA_PEM or _PATH, or unset \
                     UDB_ENV=production to opt out)"
                );
            }
            self.require_secure_transport = true;
            self.mtls_required = true;
            self.broker_to_broker_mtls_required = true;
            self.internal_control_mtls_required = true;
        }
        if self.mtls_required {
            self.broker_to_broker_mtls_required = true;
            self.internal_control_mtls_required = true;
        }
    }

    pub fn merge_env(&mut self) {
        let production_env = std::env::var("UDB_ENV")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "production" | "prod"
                )
            })
            .unwrap_or(false);
        if let Ok(value) = std::env::var("UDB_METRICS_ADDR")
            && !value.trim().is_empty()
        {
            self.metrics_addr = value;
        }
        if let Ok(value) = std::env::var("UDB_METRICS_ALLOWED_CIDR")
            && !value.trim().is_empty()
        {
            self.metrics_allowed_cidr = Some(value);
        }
        if let Some(value) = env_u32("UDB_ABAC_REFRESH_SECS") {
            self.abac_refresh_secs = value as u64;
        }
        if let Some(value) = bool_env("UDB_ABAC_DEFAULT_ALLOW") {
            self.abac_default_allow = value;
        }
        if let Some(value) = env_u32("UDB_GRPC_TIMEOUT_SECS") {
            self.grpc_timeout_secs = value as u64;
        }
        if let Some(value) = env_u32("UDB_GRPC_MAX_CONCURRENT") {
            self.grpc_max_concurrent = value.max(1) as usize;
        }
        if let Some(value) = bool_env("UDB_ALLOW_DEGRADED_BACKENDS") {
            self.allow_degraded_backends = value;
        }
        if let Some(value) = bool_env("UDB_CATALOG_COMPATIBILITY_WARN_ONLY")
            .or_else(|| bool_env("UDB_CATALOG_COMPAT_WARN_ONLY"))
        {
            self.catalog_compat_warn_only = value;
        }
        if let Ok(value) = std::env::var("UDB_CATALOG_COMPATIBILITY_LEVEL")
            && !value.trim().is_empty()
        {
            self.catalog_compatibility_level = normalize_catalog_compatibility_level(&value);
        }
        if let Some(value) = bool_env("UDB_RATE_LIMIT_ENABLED") {
            self.rate_limit_enabled = value;
        }
        // Accept the canonical `_SECS` name plus the bare `UDB_RATE_LIMIT_WINDOW`
        // alias; the canonical name wins when both are set.
        if let Some(value) = env_first(&["UDB_RATE_LIMIT_WINDOW_SECS", "UDB_RATE_LIMIT_WINDOW"])
            .and_then(|value| value.parse::<u32>().ok())
        {
            self.rate_limit_window_secs = value.max(1) as u64;
        }
        // The data-plane per-tenant budget. Accept the canonical
        // `UDB_RATE_LIMIT_MAX_PER_WINDOW` AND the short `UDB_RATE_LIMIT_MAX` name
        // operators reach for first; the canonical `_PER_WINDOW` name wins when
        // both are set. Before this alias, `UDB_RATE_LIMIT_MAX` silently did
        // nothing and the budget stayed pinned at the 1000/window default —
        // a discoverability trap that read as an un-overridable hardcoded limit.
        if let Some(value) = env_first(&["UDB_RATE_LIMIT_MAX_PER_WINDOW", "UDB_RATE_LIMIT_MAX"])
            .and_then(|value| value.parse::<u32>().ok())
        {
            self.rate_limit_max_per_window = value;
        }
        if let Some(value) = bool_env("UDB_RATE_LIMIT_PER_KEY") {
            self.rate_limit_per_key_enabled = value;
        }
        if let Ok(value) = std::env::var("UDB_RATE_LIMIT_FAILURE_MODE") {
            let normalized = value.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "closed" | "local" | "open" => self.rate_limit_failure_mode = normalized,
                "" => {}
                other => tracing::warn!(
                    "UDB_RATE_LIMIT_FAILURE_MODE '{other}' is not one of closed|local|open; \
                     keeping '{}'",
                    self.rate_limit_failure_mode
                ),
            }
        }
        let explicit_secure =
            bool_env("UDB_REQUIRE_SECURE_TRANSPORT").or_else(|| bool_env("UDB_TLS_REQUIRED"));
        if let Some(value) = explicit_secure {
            self.require_secure_transport = value;
        }
        let explicit_mtls = bool_env("UDB_MTLS_REQUIRED");
        if let Some(value) = explicit_mtls {
            self.mtls_required = value;
        }
        if let Some(value) = bool_env("UDB_BROKER_MTLS_REQUIRED")
            .or_else(|| bool_env("UDB_BROKER_TO_BROKER_MTLS_REQUIRED"))
        {
            self.broker_to_broker_mtls_required = value;
        }
        if let Some(value) = bool_env("UDB_INTERNAL_CONTROL_MTLS_REQUIRED")
            .or_else(|| bool_env("UDB_WORKER_CONTROL_MTLS_REQUIRED"))
        {
            self.internal_control_mtls_required = value;
        }
        self.tls.merge_env();
        self.apply_security_posture(production_env, explicit_secure, explicit_mtls);
    }
}

fn has_non_empty(value: &Option<String>) -> bool {
    value
        .as_deref()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn normalize_catalog_compatibility_level(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "exact" => "exact".to_string(),
        "none" | "off" | "disabled" => "none".to_string(),
        _ => "backward".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SagaSettings {
    pub recovery_enabled: bool,
    pub recovery_interval_secs: u64,
    pub stale_threshold_secs: u64,
    pub worker_lease_ttl_secs: u64,
    pub poison_threshold: i64,
    pub recovery_batch_size: i64,
}

impl Default for SagaSettings {
    fn default() -> Self {
        Self {
            recovery_enabled: true,
            recovery_interval_secs: 60,
            stale_threshold_secs: 300,
            worker_lease_ttl_secs: 120,
            poison_threshold: 3,
            recovery_batch_size: 100,
        }
    }
}

impl SagaSettings {
    pub fn merge_env(&mut self) {
        if let Some(value) = bool_env("UDB_SAGA_RECOVERY_ENABLED") {
            self.recovery_enabled = value;
        }
        if let Some(value) = env_u32("UDB_SAGA_RECOVERY_INTERVAL_SECONDS") {
            self.recovery_interval_secs = value.max(1) as u64;
        }
        if let Some(value) = env_u32("UDB_SAGA_STALE_THRESHOLD_SECONDS") {
            self.stale_threshold_secs = value.max(1) as u64;
        }
        if let Some(value) = env_u32("UDB_SAGA_WORKER_LEASE_TTL_SECS") {
            self.worker_lease_ttl_secs = value.max(1) as u64;
        }
        if let Some(value) = env_i32("UDB_SAGA_POISON_THRESHOLD") {
            self.poison_threshold = i64::from(value.max(1));
        }
        if let Some(value) = env_i32("UDB_SAGA_RECOVERY_BATCH_SIZE") {
            self.recovery_batch_size = i64::from(value.max(1));
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CircuitBreakerSettings {
    pub failure_threshold: u32,
    pub cooldown_secs: u64,
}

impl Default for CircuitBreakerSettings {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            cooldown_secs: 30,
        }
    }
}

impl CircuitBreakerSettings {
    pub fn merge_env(&mut self) {
        if let Some(value) = env_u32("UDB_CIRCUIT_FAILURE_THRESHOLD") {
            self.failure_threshold = value.max(1);
        }
        if let Some(value) = env_u32("UDB_CIRCUIT_COOLDOWN_SECS") {
            self.cooldown_secs = value.max(1) as u64;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SystemCatalogSettings {
    pub saga_table: String,
    pub xa_ledger_table: String,
    pub catalog_versions_table: String,
    pub catalog_activation_log_table: String,
    pub project_catalog_bindings_table: String,
    pub catalog_reload_log_table: String,
    pub migration_runs_table: String,
    pub migration_op_ledger_table: String,
    pub cdc_journal_table: String,
    pub dlq_table: String,
    pub cdc_control_table: String,
    pub topic_policy_table: String,
    pub admin_audit_log_table: String,
    pub projects_table: String,
    // U3 — Projection materialization engine
    pub projection_tasks_table: String,
}

impl Default for SystemCatalogSettings {
    fn default() -> Self {
        Self {
            saga_table: "udb_saga_coordinator".to_string(),
            xa_ledger_table: "udb_xa_ledger".to_string(),
            catalog_versions_table: "udb_catalog_versions".to_string(),
            catalog_activation_log_table: "udb_catalog_activation_log".to_string(),
            project_catalog_bindings_table: "udb_project_catalog_bindings".to_string(),
            catalog_reload_log_table: "udb_catalog_reload_log".to_string(),
            migration_runs_table: "udb_migration_runs".to_string(),
            migration_op_ledger_table: "udb_migration_op_ledger".to_string(),
            cdc_journal_table: "udb_cdc_event_journal".to_string(),
            dlq_table: "udb_cdc_dlq_events".to_string(),
            cdc_control_table: "udb_cdc_control".to_string(),
            topic_policy_table: "udb_topic_policy".to_string(),
            admin_audit_log_table: "udb_admin_audit_log".to_string(),
            projects_table: "udb_projects".to_string(),
            projection_tasks_table: "udb_projection_tasks".to_string(),
        }
    }
}

impl SystemCatalogSettings {
    pub fn merge_env(&mut self) {
        merge_identifier_env("UDB_SAGA_TABLE", &mut self.saga_table);
        merge_identifier_env("UDB_XA_LEDGER_TABLE", &mut self.xa_ledger_table);
        merge_identifier_env(
            "UDB_CATALOG_VERSIONS_TABLE",
            &mut self.catalog_versions_table,
        );
        merge_identifier_env(
            "UDB_CATALOG_ACTIVATION_LOG_TABLE",
            &mut self.catalog_activation_log_table,
        );
        merge_identifier_env(
            "UDB_PROJECT_CATALOG_BINDINGS_TABLE",
            &mut self.project_catalog_bindings_table,
        );
        merge_identifier_env(
            "UDB_CATALOG_RELOAD_LOG_TABLE",
            &mut self.catalog_reload_log_table,
        );
        merge_identifier_env("UDB_MIGRATION_RUNS_TABLE", &mut self.migration_runs_table);
        merge_identifier_env(
            "UDB_MIGRATION_OP_LEDGER_TABLE",
            &mut self.migration_op_ledger_table,
        );
        merge_identifier_env("UDB_CDC_JOURNAL_TABLE", &mut self.cdc_journal_table);
        merge_identifier_env("UDB_DLQ_TABLE", &mut self.dlq_table);
        merge_identifier_env("UDB_CDC_CONTROL_TABLE", &mut self.cdc_control_table);
        merge_identifier_env("UDB_TOPIC_POLICY_TABLE", &mut self.topic_policy_table);
        merge_identifier_env("UDB_ADMIN_AUDIT_LOG_TABLE", &mut self.admin_audit_log_table);
        merge_identifier_env("UDB_PROJECTS_TABLE", &mut self.projects_table);
        merge_identifier_env(
            "UDB_PROJECTION_TASKS_TABLE",
            &mut self.projection_tasks_table,
        );
    }
}

fn merge_identifier_env(key: &str, target: &mut String) {
    if let Ok(value) = std::env::var(key) {
        let sanitized = sanitize_identifier_value(&value);
        if !sanitized.is_empty() {
            *target = sanitized;
        }
    }
}

fn sanitize_identifier_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect()
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EncryptionSettings {
    /// Field-level encryption keys by numeric version.
    pub keys: BTreeMap<u8, String>,
    /// Active key version. Defaults to the highest configured key version.
    pub active_version: Option<u8>,
    /// Vault address used for Transit key export when inline keys are absent.
    pub vault_addr: Option<String>,
    /// Vault token used for Transit key export.
    pub vault_token: Option<String>,
    /// Vault Transit key name.
    pub vault_transit_key_name: String,
    /// Vault Transit mount.
    pub vault_transit_mount: String,
    /// Vault HTTP request timeout.
    pub vault_timeout_secs: u64,
    /// Permit plain-HTTP Vault endpoints for local development.
    pub dev_mode: bool,
    /// Require at-rest encryption for UDB-owned object metadata/native state.
    pub object_native_state_required: bool,
}

// Manual `Debug` redacting the field-level encryption key material (`keys`
// values) and the `vault_token`. Configured key *versions* and the Vault
// address/mount remain visible for debuggability — mirrors the
// `RedactedKeyVersions` pattern in `encryption.rs` (versions shown, material
// masked).
impl std::fmt::Debug for EncryptionSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionSettings")
            .field(
                "key_versions",
                &self.keys.keys().copied().collect::<Vec<u8>>(),
            )
            .field("key_material", &"[redacted]")
            .field("active_version", &self.active_version)
            .field("vault_addr", &self.vault_addr)
            .field(
                "vault_token",
                &self.vault_token.as_ref().map(|_| "[redacted]"),
            )
            .field("vault_transit_key_name", &self.vault_transit_key_name)
            .field("vault_transit_mount", &self.vault_transit_mount)
            .field("vault_timeout_secs", &self.vault_timeout_secs)
            .field("dev_mode", &self.dev_mode)
            .field(
                "object_native_state_required",
                &self.object_native_state_required,
            )
            .finish()
    }
}

impl Default for EncryptionSettings {
    fn default() -> Self {
        Self {
            keys: BTreeMap::new(),
            active_version: None,
            vault_addr: None,
            vault_token: None,
            vault_transit_key_name: "udb/pii".to_string(),
            vault_transit_mount: "transit".to_string(),
            vault_timeout_secs: 10,
            dev_mode: false,
            object_native_state_required: false,
        }
    }
}

impl EncryptionSettings {
    pub fn merge_env(&mut self) {
        if let Some(value) = env_first(&["UDB_ENCRYPTION_KEY", "UDB_ENCRYPTION_KEY_V1"]) {
            self.keys.insert(1, value);
        }
        for version in 2..=9u8 {
            let key = format!("UDB_ENCRYPTION_KEY_V{version}");
            if let Ok(value) = std::env::var(key)
                && !value.trim().is_empty()
            {
                self.keys.insert(version, value.trim().to_string());
            }
        }
        if let Ok(value) = std::env::var("UDB_ENCRYPTION_ACTIVE_VERSION")
            && let Ok(version) = value.parse::<u8>()
        {
            self.active_version = Some(version);
        }
        if let Some(value) = env_first(&["UDB_VAULT_ADDR", "VAULT_ADDR"]) {
            self.vault_addr = Some(value);
        }
        if let Some(value) = env_first(&["UDB_VAULT_TOKEN", "VAULT_TOKEN"]) {
            self.vault_token = Some(value);
        }
        if let Some(value) = env_first(&["UDB_VAULT_TRANSIT_KEY_NAME"]) {
            self.vault_transit_key_name = value;
        }
        if let Some(value) = env_first(&["UDB_VAULT_TRANSIT_MOUNT"]) {
            self.vault_transit_mount = value;
        }
        if let Some(value) = env_u32("UDB_VAULT_TIMEOUT_SECS") {
            self.vault_timeout_secs = value.max(1) as u64;
        }
        if let Some(value) = bool_env("UDB_DEV_MODE") {
            self.dev_mode = value;
        }
        if let Some(value) = bool_env("UDB_OBJECT_NATIVE_STATE_ENCRYPTION_REQUIRED")
            .or_else(|| bool_env("UDB_NATIVE_STATE_ENCRYPTION_REQUIRED"))
        {
            self.object_native_state_required = value;
        }
    }

    pub fn has_key_source(&self) -> bool {
        !self.keys.is_empty()
            || (self
                .vault_addr
                .as_deref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
                && self
                    .vault_token
                    .as_deref()
                    .map(|value| !value.trim().is_empty())
                    .unwrap_or(false))
    }

    pub fn validate_required_key_source(&self) -> Result<(), String> {
        if self.object_native_state_required && !self.has_key_source() {
            return Err(
                "object/native-state encryption is required but no UDB_ENCRYPTION_KEY/Vault key source is configured"
                    .to_string(),
            );
        }
        Ok(())
    }
}

// ── Deploy mode ───────────────────────────────────────────────────────────────

/// Per-run options consumed by the migration engine.
///
/// The Go UDB service builds this from `DatabaseConfig` + env overrides and
/// passes it to `NewEngine()`. Mirrors the legacy Go service migration options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MigrationOptions {
    // ── Paths ─────────────────────────────────────────────────────────────────
    /// Directory containing SQL migration files, e.g. `"db/migration"`.
    pub migrations_path: String,
    /// Directory for seeder SQL files, e.g. `"db/seeders"`.
    pub seeders_path: String,
    /// Proto source directory, e.g. `"proto"`.
    pub proto_dir: String,
    /// Optional proto annotation namespace, e.g. `"acme.platform.udb.v1"`.
    pub proto_namespace: String,
    /// Directory for generated bootstrap SQL artifacts, e.g. `"db/bootstrap"`.
    pub bootstrap_dir: String,
    /// Root directory for generated db_ops artifacts.
    pub db_ops_root: String,
    /// Directory for the proto AST manifest JSON files, e.g. `"db/migration_manifest"`.
    pub manifest_dir: String,
    /// PostgreSQL schema that stores UDB's migration ledger tables.
    pub ledger_schema: String,

    // ── Behaviour flags ───────────────────────────────────────────────────────
    /// Re-generate delta SQL files when the proto manifest checksum changes.
    pub generate_sql: bool,
    /// Re-generate bootstrap SQL snapshots when the proto manifest changes.
    pub generate_bootstrap: bool,
    /// Apply safe DDL automatically without writing a SQL file (dev-only).
    pub emergency_auto_alter: bool,
    /// Hard-fail the run if lint errors remain after applying migrations.
    pub strict_verify: bool,
    /// Simulate the full migration run without writing any SQL or applying DDL.
    pub dry_run: bool,
    /// Log every SQL statement executed against the database.
    pub log_queries: bool,
    /// Force a bootstrap re-apply even when the manifest checksum is unchanged.
    pub force_reseed: bool,
    /// Explicitly skip live PostgreSQL verification when the manifest checksum is unchanged.
    pub skip_unchanged_verify: bool,
    /// UDB_FRICTION §4: when the persisted manifest checksum equals the current
    /// one, skip the ENTIRE expensive generate/apply/provision/verify suite and
    /// go straight to serve (a ~2-min remote-DB re-bootstrap on every restart
    /// otherwise). Opt-in; `force_sync`/`force_reseed`/`dry_run` bypass it. The
    /// tradeoff is the same as `skip_unchanged_verify`: external drift is not
    /// re-checked on a fast start (`udb admin force-sync` is the escape hatch).
    pub skip_if_unchanged: bool,

    // ── Timeouts ──────────────────────────────────────────────────────────────
    /// PostgreSQL `lock_timeout` SET at the head of every generated SQL file.
    /// e.g. `"5s"`, `"10s"`.
    pub lock_timeout: String,
    /// PostgreSQL `statement_timeout` SET at the head of every generated SQL file.
    /// e.g. `"120s"`, `"5min"`.
    pub statement_timeout: String,
    /// Per-file Go context deadline in seconds.  Default: `300` (5 minutes).
    pub exec_timeout_secs: u64,

    // ── Approval workflow ─────────────────────────────────────────────────────
    /// Path to a previously exported `MigrationPlan` JSON file.  When set, the
    /// engine verifies that the current diff exactly matches this approved plan
    /// before applying any changes.
    pub require_approval_plan: String,

    // ── Notifications ─────────────────────────────────────────────────────────
    /// HTTP(S) endpoint to POST a JSON payload to on completion / failure / blocked.
    pub notification_url: String,
    /// Events that trigger a notification: `"completed"`, `"failed"`, `"blocked"`.
    pub notification_on: Vec<String>,

    // ── Hooks ─────────────────────────────────────────────────────────────────
    /// Path to a SQL file executed immediately before migrations (pre-migrate hook).
    /// Failure aborts the run.
    pub pre_migrate_sql: String,
    /// Path to a SQL file executed after all migrations (post-migrate hook).
    /// Failure is non-fatal (logged as a warning).
    pub post_migrate_sql: String,

    // ── Retry / recovery ─────────────────────────────────────────────────────
    /// Maximum number of auto-recovery retries.  Defaults to `MAX_RETRIES` (3).
    pub max_retries: u32,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            migrations_path: "db/migration".to_string(),
            seeders_path: "db/seeders".to_string(),
            proto_dir: "proto".to_string(),
            proto_namespace: String::new(),
            bootstrap_dir: "db/bootstrap".to_string(),
            db_ops_root: String::new(),
            manifest_dir: "db/migration_manifest".to_string(),
            ledger_schema: DEFAULT_LEDGER_SCHEMA.to_string(),
            generate_sql: true,
            generate_bootstrap: false,
            emergency_auto_alter: false,
            strict_verify: false,
            dry_run: false,
            log_queries: false,
            force_reseed: false,
            skip_unchanged_verify: false,
            skip_if_unchanged: false,
            lock_timeout: "5s".to_string(),
            statement_timeout: "120s".to_string(),
            exec_timeout_secs: 300,
            require_approval_plan: String::new(),
            notification_url: String::new(),
            notification_on: Vec::new(),
            pre_migrate_sql: String::new(),
            post_migrate_sql: String::new(),
            max_retries: crate::engine::MAX_RETRIES,
        }
    }
}

impl MigrationOptions {
    /// Build migration options from environment variables.
    ///
    /// Currently honours:
    /// - `UDB_SEEDERS_PATH`
    /// - `UDB_SEEDER_PATH` (legacy alias)
    pub fn from_env() -> Self {
        let mut opts = Self::default();
        opts.merge_env();
        opts
    }

    /// Overlay env-provided fields onto an existing (e.g. file-loaded) config,
    /// touching ONLY fields whose env var is present — so file-configured safety
    /// flags (`require_approval_plan`, hooks, `strict_verify`, `dry_run`) survive.
    pub fn merge_env(&mut self) {
        if let Some(path) = std::env::var("UDB_SEEDERS_PATH")
            .ok()
            .or_else(|| std::env::var("UDB_SEEDER_PATH").ok())
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            self.seeders_path = path;
        }
        if let Some(path) = std::env::var("UDB_DB_OPS_ROOT")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            self.db_ops_root = path;
        }
        if let Some(schema) = std::env::var("UDB_LEDGER_SCHEMA")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        {
            self.ledger_schema = schema;
        }
        if let Some(value) = bool_env("UDB_FORCE_RESEED") {
            self.force_reseed = value;
        }
        if let Some(value) = bool_env("UDB_SKIP_UNCHANGED_VERIFY") {
            self.skip_unchanged_verify = value;
        }
        if let Some(value) = bool_env("UDB_STARTUP_SKIP_IF_UNCHANGED") {
            self.skip_if_unchanged = value;
        }
    }

    /// Returns `true` when any notification events are configured.
    pub fn has_notifications(&self) -> bool {
        !self.notification_url.is_empty() && !self.notification_on.is_empty()
    }

    /// Returns `true` when the pre-migrate SQL hook is configured.
    pub fn has_pre_hook(&self) -> bool {
        !self.pre_migrate_sql.is_empty()
    }

    /// Returns `true` when the post-migrate SQL hook is configured.
    pub fn has_post_hook(&self) -> bool {
        !self.post_migrate_sql.is_empty()
    }

    /// Returns `true` when a plan approval file is required.
    pub fn requires_approval(&self) -> bool {
        !self.require_approval_plan.is_empty()
    }
}

// ── DB connection config ──────────────────────────────────────────────────────

// ── Tier 1: PostgreSQL connection config ─────────────────────────────────────

/// Top-level configuration for the Universal Data Broker (UDB).
///
/// Covers all four storage tiers mandated by spec §16.1:
///   - Tier 1 (SQL)    : `primary` / `backup` / `signal`  (all PostgreSQL)
///   - Tier 2 (Cache)  : `redis`
///   - Tier 3 (Vector) : `qdrant`
///   - Tier 4 (Object) : `minio`
///
/// Produced by loading `configs/database.yaml` and expanding env vars.
/// The legacy Go reference (`Config`) only covers Tier 1; UDB adds the
/// optional cache, vector, and object tiers.
///
/// The `deploy` field captures the detected deployment mode for each backend
/// (Docker / self-hosted vs. cloud-managed service).  It is derived from
/// `UDB_<BACKEND>_DEPLOY_MODE` env-vars or DSN heuristics at startup.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UdbConfig {
    // ── Deployment profile ────────────────────────────────────────────────────
    /// Per-backend deployment mode resolved from environment.
    /// Auto-detected at startup; can be overridden per-backend via
    /// `UDB_<BACKEND>_DEPLOY_MODE` env-vars.
    pub deploy: DeployProfile,

    // ── Tier 1: PostgreSQL ────────────────────────────────────────────────────
    /// Primary PostgreSQL connection.  Hosts the migration ledger.
    pub primary: DbConfig,
    /// Optional backup PostgreSQL connection (failover + sync).
    pub backup: Option<DbConfig>,
    /// Optional signal / analytics PostgreSQL connection (high-volume event writes).
    pub signal: Option<DbConfig>,

    // ── Tier 2: Cache ─────────────────────────────────────────────────────────
    /// Redis cache tier configuration.
    pub redis: Option<RedisConfig>,

    // ── Tier 3: Vector ────────────────────────────────────────────────────────
    /// Qdrant vector store configuration.
    pub qdrant: Option<QdrantConfig>,

    // ── Tier 4: Object ────────────────────────────────────────────────────────
    /// MinIO / S3-compatible object store configuration.
    pub minio: Option<MinioConfig>,

    // ── Cross-tier settings ───────────────────────────────────────────────────
    /// Tier 1 failover settings (primary → backup).
    pub failover: FailoverConfig,
    /// Migration engine settings (SQL + non-SQL provisioning).
    pub migration: MigrationOptions,
    /// Audit event sink (structured events for every mutating RPC).
    pub audit_sink: AuditSinkConfig,
    /// Service/server settings such as gRPC, metrics, TLS, and degraded mode.
    pub service: ServiceSettings,
    /// Native UDB services such as authn/authz/storage/asset/WebRTC.
    pub native_services: NativeServicesSettings,
    /// Operation channel limits/timeouts.
    pub channels: ChannelSettings,
    /// CDC/outbox settings.
    pub cdc: CdcConfig,
    /// Request/security settings.
    pub security: SecurityConfig,
    /// Field-level encryption key/Vault settings.
    pub encryption: EncryptionSettings,
    /// Saga recovery worker settings.
    pub saga: SagaSettings,
    /// Circuit breaker thresholds.
    pub circuit_breaker: CircuitBreakerSettings,
    /// Project/backend instance routing strictness. Empty or `permissive`
    /// preserves legacy behavior; `strict` fails closed for unlabeled
    /// instances outside the default project; `strict_with_default:<name>`
    /// binds unlabeled instances to the named project.
    #[serde(default)]
    pub project_routing_mode: String,
    /// UDB-owned system catalog table names.
    pub system_catalog: SystemCatalogSettings,

    /// Generic named backend instances used by the broker registry and portal.
    ///
    /// This is the portable replacement for one-off per-backend instance lists.
    /// Legacy fields such as `primary`, `redis`, `qdrant`, and `minio` remain
    /// supported, but new deployments should declare instances here.
    #[serde(default)]
    pub backend_instances: BackendInstanceConfig,

    // ── Runtime tuning ────────────────────────────────────────────────────────
    /// Application name used in PostgreSQL connection strings and logs.
    #[serde(default)]
    pub app_name: String,
    /// Default row limit for SELECT queries.
    #[serde(default = "hundred_i32")]
    pub default_limit: i32,
    /// Maximum allowed row limit for SELECT queries.
    #[serde(default = "thousand_i32")]
    pub max_limit: i32,
    /// Startup/force-sync DDL concurrency. Defaults to **1 (serial)** — parallel
    /// DDL on shared catalog rows races with `tuple concurrently updated`. Set
    /// `UDB_DDL_CONCURRENCY=N` to opt into parallelism.
    #[serde(default = "default_ddl_concurrency")]
    pub ddl_concurrency: usize,
    /// PostgreSQL read-replica DSNs.
    #[serde(default)]
    pub pg_replica_dsns: Vec<String>,
    /// PostgreSQL read-replica routing strategy.
    #[serde(default)]
    pub pg_replica_strategy: String,
    /// Maximum tolerated read replica lag in seconds.
    #[serde(default)]
    pub pg_replica_max_lag_secs: u64,
    /// Whether replica reads may fall back open when health is unknown.
    #[serde(default)]
    pub pg_replica_fail_open: bool,
    /// Optional replica pool minimum connection override.
    #[serde(default)]
    pub pg_replica_min_connections: u32,
    /// Optional replica pool maximum connection override.
    #[serde(default)]
    pub pg_replica_max_connections: u32,
    /// Replica health refresh interval in seconds.
    #[serde(default)]
    pub pg_replica_health_interval_secs: u64,
    /// Materialized view refresh TTL in days. Zero disables scheduled refresh.
    #[serde(default)]
    pub materialized_view_ttl_days: i32,
    /// ABAC schema name.
    #[serde(default)]
    pub abac_schema: String,
    /// ABAC table name.
    #[serde(default)]
    pub abac_table: String,
    /// Kafka brokers for CDC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_brokers: Option<String>,
}

// Manual `Debug` for the top-level config. Every embedded backend config
// (`primary`, `redis`, `qdrant`, `minio`, `security`, `encryption`, …) carries
// its own redacting `Debug`, so they print safely here. The one secret held
// *directly* on this struct is `pg_replica_dsns` (replica connection strings
// that embed credentials); its count is shown but the strings are masked.
impl std::fmt::Debug for UdbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdbConfig")
            .field("deploy", &self.deploy)
            .field("primary", &self.primary)
            .field("backup", &self.backup)
            .field("signal", &self.signal)
            .field("redis", &self.redis)
            .field("qdrant", &self.qdrant)
            .field("minio", &self.minio)
            .field("failover", &self.failover)
            .field("migration", &self.migration)
            .field("audit_sink", &self.audit_sink)
            .field("service", &self.service)
            .field("native_services", &self.native_services)
            .field("channels", &self.channels)
            .field("cdc", &self.cdc)
            .field("security", &self.security)
            .field("encryption", &self.encryption)
            .field("saga", &self.saga)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("project_routing_mode", &self.project_routing_mode)
            .field("system_catalog", &self.system_catalog)
            .field("backend_instances", &self.backend_instances)
            .field("app_name", &self.app_name)
            .field("default_limit", &self.default_limit)
            .field("max_limit", &self.max_limit)
            .field("ddl_concurrency", &self.ddl_concurrency)
            .field(
                "pg_replica_dsns",
                &format_args!("[{} redacted]", self.pg_replica_dsns.len()),
            )
            .field("pg_replica_strategy", &self.pg_replica_strategy)
            .field("pg_replica_max_lag_secs", &self.pg_replica_max_lag_secs)
            .field("pg_replica_fail_open", &self.pg_replica_fail_open)
            .field(
                "pg_replica_min_connections",
                &self.pg_replica_min_connections,
            )
            .field(
                "pg_replica_max_connections",
                &self.pg_replica_max_connections,
            )
            .field(
                "pg_replica_health_interval_secs",
                &self.pg_replica_health_interval_secs,
            )
            .field(
                "materialized_view_ttl_days",
                &self.materialized_view_ttl_days,
            )
            .field("abac_schema", &self.abac_schema)
            .field("abac_table", &self.abac_table)
            .field("kafka_brokers", &self.kafka_brokers)
            .finish()
    }
}

#[cfg(test)]
mod secret_no_leak {
    use super::*;

    const CANARY: &str = "udb-canary-SECRET";

    #[test]
    fn tls_settings_debug_redacts_private_key() {
        let cfg = TlsSettings {
            key_pem: Some(CANARY.to_string()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains(CANARY), "TlsSettings leaked key_pem: {dbg}");
    }

    #[test]
    fn encryption_settings_debug_redacts_keys_and_token() {
        let mut keys = BTreeMap::new();
        keys.insert(1u8, CANARY.to_string());
        let cfg = EncryptionSettings {
            keys,
            vault_token: Some(CANARY.to_string()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains(CANARY),
            "EncryptionSettings leaked key/token: {dbg}"
        );
        // Key versions remain visible for debuggability.
        assert!(dbg.contains("key_versions"));
    }

    #[test]
    fn udb_config_debug_redacts_replica_dsns() {
        let cfg = UdbConfig {
            pg_replica_dsns: vec![format!("postgres://u:{CANARY}@h/db")],
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(
            !dbg.contains(CANARY),
            "UdbConfig leaked pg_replica_dsns: {dbg}"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConfigValidationReport {
    pub passed: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub health_gates: Vec<StartupHealthGate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StartupHealthGate {
    pub tier: String,
    pub backend: String,
    pub required: bool,
    pub configured: bool,
    pub degraded_mode_allowed: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NativeServicesSettings {
    pub enabled: bool,
    pub migrate_enabled: bool,
    pub control_plane_enabled: bool,
    pub control_plane_addr: String,
    pub webrtc_peer_enabled: bool,
    pub webrtc_peer_addr: String,
    pub default_enabled: bool,
    pub services: BTreeMap<String, NativeServiceConfig>,
}

impl Default for NativeServicesSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            migrate_enabled: true,
            control_plane_enabled: true,
            control_plane_addr: String::new(),
            webrtc_peer_enabled: true,
            webrtc_peer_addr: String::new(),
            default_enabled: true,
            services: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct NativeServiceConfig {
    pub enabled: Option<bool>,
    pub migrate: Option<bool>,
    pub required_backends: Vec<String>,
    pub capability_overrides: Vec<String>,
    pub dependency_overrides: Vec<String>,
    pub worker_config: BTreeMap<String, String>,
}

impl NativeServicesSettings {
    pub fn merge_env(&mut self) {
        let canonical_enabled = std::env::var("UDB_NATIVE_SERVICES_ENABLED").ok();
        let legacy_enabled = std::env::var("UDB_NATIVE_AUTH").ok();
        self.merge_enabled_env_values(canonical_enabled.as_deref(), legacy_enabled.as_deref());
        if let Some(value) = bool_env("UDB_NATIVE_SERVICES_MIGRATE_ENABLED") {
            self.migrate_enabled = value;
        }
        if let Some(value) = bool_env("UDB_NATIVE_CONTROL_PLANE_ENABLED") {
            self.control_plane_enabled = value;
        }
        if let Ok(value) = std::env::var("UDB_AUTH_GRPC_ADDR")
            && !value.trim().is_empty()
        {
            self.control_plane_addr = value.trim().to_string();
        }
        if let Some(value) = bool_env("UDB_NATIVE_WEBRTC_PEER_ENABLED") {
            self.webrtc_peer_enabled = value;
        }
        if let Ok(value) = std::env::var("UDB_WEBRTC_GRPC_ADDR")
            && !value.trim().is_empty()
        {
            self.webrtc_peer_addr = value.trim().to_string();
        }
        if let Some(selected) = csv_env("UDB_NATIVE_SERVICES")
            && !selected.is_empty()
        {
            self.default_enabled = false;
            for service in selected {
                self.services.entry(service).or_default().enabled = Some(true);
            }
        }
        for id in crate::runtime::service::native_registry::native_service_ids() {
            let env_id = crate::runtime::service::native_registry::canonical_service_id(&id)
                .to_ascii_uppercase();
            if let Some(value) = bool_env(&format!("UDB_NATIVE_{env_id}_ENABLED")) {
                self.services.entry(id.clone()).or_default().enabled = Some(value);
            }
            if let Some(value) = bool_env(&format!("UDB_NATIVE_{env_id}_MIGRATE_ENABLED")) {
                self.services.entry(id.clone()).or_default().migrate = Some(value);
            }
        }
    }

    pub(crate) fn merge_enabled_env_values(
        &mut self,
        canonical_enabled: Option<&str>,
        legacy_enabled: Option<&str>,
    ) {
        if canonical_enabled.is_some() && legacy_enabled.is_some() {
            tracing::warn!(
                "both UDB_NATIVE_SERVICES_ENABLED and deprecated UDB_NATIVE_AUTH are set; UDB_NATIVE_SERVICES_ENABLED wins"
            );
        }
        if let Some(value) = canonical_enabled
            .map(parse_bool_env_value)
            .or_else(|| legacy_enabled.map(parse_bool_env_value))
        {
            self.enabled = value;
        }
    }

    pub fn merge_over(self, base: Self) -> Self {
        let mut services = base.services;
        for (id, source) in self.services {
            let entry = services.entry(id).or_default();
            if source.enabled.is_some() {
                entry.enabled = source.enabled;
            }
            if source.migrate.is_some() {
                entry.migrate = source.migrate;
            }
            if !source.required_backends.is_empty() {
                entry.required_backends = source.required_backends;
            }
            if !source.capability_overrides.is_empty() {
                entry.capability_overrides = source.capability_overrides;
            }
            if !source.dependency_overrides.is_empty() {
                entry.dependency_overrides = source.dependency_overrides;
            }
            if !source.worker_config.is_empty() {
                entry.worker_config = source.worker_config;
            }
        }
        Self {
            enabled: self.enabled,
            migrate_enabled: self.migrate_enabled,
            control_plane_enabled: self.control_plane_enabled,
            control_plane_addr: if self.control_plane_addr.is_empty() {
                base.control_plane_addr
            } else {
                self.control_plane_addr
            },
            webrtc_peer_enabled: self.webrtc_peer_enabled,
            webrtc_peer_addr: if self.webrtc_peer_addr.is_empty() {
                base.webrtc_peer_addr
            } else {
                self.webrtc_peer_addr
            },
            default_enabled: self.default_enabled,
            services,
        }
    }
}

/// B (2026-05-30): operator opt-in for live two-phase commit.
/// When `UDB_2PC_ENABLED=true`, the gRPC handler routes
/// `tx_strategy=two_phase` requests through PREPARE TRANSACTION +
/// COMMIT PREPARED (the live XA path) instead of the historical
/// fail-closed "disabled" error. Default is off so existing
/// deployments keep their previous behaviour until the operator
/// explicitly enables the path.
///
/// Accepted truthy values: `1`, `true`, `yes`, `on`
/// (case-insensitive). Anything else (and missing) is false.
pub fn two_phase_runtime_enabled() -> bool {
    parse_two_phase_toggle(std::env::var("UDB_2PC_ENABLED").ok().as_deref())
}

/// Pure parsing helper for `UDB_2PC_ENABLED`. Split out so tests
/// can exercise truthy/falsy detection without racing on the
/// process env (every other test in the suite is running in
/// parallel and may set or clear the same var concurrently).
pub fn parse_two_phase_toggle(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

/// Merge `source` (file config) into `base` (defaults), returning the result.
/// File values only overwrite base values when they are non-empty / explicitly set.
pub fn merge_udb_config(base: UdbConfig, source: UdbConfig) -> UdbConfig {
    UdbConfig {
        deploy: source.deploy,
        primary: if source.primary.is_configured() || !source.primary.direct_dsn.is_empty() {
            source.primary
        } else {
            base.primary
        },
        backup: source.backup.or(base.backup),
        signal: source.signal.or(base.signal),
        redis: source.redis.or(base.redis),
        qdrant: source.qdrant.or(base.qdrant),
        minio: source.minio.or(base.minio),
        failover: source.failover,
        migration: source.migration,
        audit_sink: source.audit_sink,
        service: source.service,
        native_services: source.native_services.merge_over(base.native_services),
        channels: source.channels,
        cdc: source.cdc,
        security: source.security,
        encryption: if source.encryption == EncryptionSettings::default() {
            base.encryption
        } else {
            source.encryption
        },
        saga: source.saga,
        circuit_breaker: source.circuit_breaker,
        project_routing_mode: if source.project_routing_mode.is_empty() {
            base.project_routing_mode
        } else {
            source.project_routing_mode
        },
        system_catalog: if source.system_catalog == SystemCatalogSettings::default() {
            base.system_catalog
        } else {
            source.system_catalog
        },
        backend_instances: if source.backend_instances.instances.is_empty() {
            base.backend_instances
        } else {
            source.backend_instances
        },
        app_name: if source.app_name.is_empty() {
            base.app_name
        } else {
            source.app_name
        },
        default_limit: if source.default_limit == 0 {
            base.default_limit
        } else {
            source.default_limit
        },
        max_limit: if source.max_limit == 0 {
            base.max_limit
        } else {
            source.max_limit
        },
        ddl_concurrency: if source.ddl_concurrency == 0 {
            base.ddl_concurrency
        } else {
            source.ddl_concurrency
        },
        pg_replica_dsns: if source.pg_replica_dsns.is_empty() {
            base.pg_replica_dsns
        } else {
            source.pg_replica_dsns
        },
        pg_replica_strategy: if source.pg_replica_strategy.is_empty() {
            base.pg_replica_strategy
        } else {
            source.pg_replica_strategy
        },
        pg_replica_max_lag_secs: if source.pg_replica_max_lag_secs == 0 {
            base.pg_replica_max_lag_secs
        } else {
            source.pg_replica_max_lag_secs
        },
        pg_replica_fail_open: source.pg_replica_fail_open || base.pg_replica_fail_open,
        pg_replica_min_connections: if source.pg_replica_min_connections == 0 {
            base.pg_replica_min_connections
        } else {
            source.pg_replica_min_connections
        },
        pg_replica_max_connections: if source.pg_replica_max_connections == 0 {
            base.pg_replica_max_connections
        } else {
            source.pg_replica_max_connections
        },
        pg_replica_health_interval_secs: if source.pg_replica_health_interval_secs == 0 {
            base.pg_replica_health_interval_secs
        } else {
            source.pg_replica_health_interval_secs
        },
        materialized_view_ttl_days: if source.materialized_view_ttl_days == 0 {
            base.materialized_view_ttl_days
        } else {
            source.materialized_view_ttl_days
        },
        abac_schema: if source.abac_schema.is_empty() {
            base.abac_schema
        } else {
            source.abac_schema
        },
        abac_table: if source.abac_table.is_empty() {
            base.abac_table
        } else {
            source.abac_table
        },
        kafka_brokers: source.kafka_brokers.or(base.kafka_brokers),
    }
}

impl UdbConfig {
    /// Returns `true` when the backup PostgreSQL DB is configured.
    pub fn has_backup(&self) -> bool {
        self.backup
            .as_ref()
            .map(|b| b.is_configured())
            .unwrap_or(false)
    }

    /// Returns `true` when the signal PostgreSQL DB is configured.
    pub fn has_signal(&self) -> bool {
        self.signal
            .as_ref()
            .map(|s| s.is_configured())
            .unwrap_or(false)
    }

    /// Returns `true` when the Redis cache tier is configured.
    pub fn has_redis(&self) -> bool {
        self.redis
            .as_ref()
            .map(|r| r.is_configured())
            .unwrap_or(false)
    }

    /// Returns `true` when the Qdrant vector tier is configured.
    pub fn has_qdrant(&self) -> bool {
        self.qdrant
            .as_ref()
            .map(|q| q.is_configured())
            .unwrap_or(false)
    }

    /// Returns `true` when the MinIO object tier is configured.
    pub fn has_minio(&self) -> bool {
        self.minio
            .as_ref()
            .map(|m| m.is_configured())
            .unwrap_or(false)
    }

    /// Returns a list of active (configured) backend tier labels.
    /// Used for log output and metrics initialisation.
    pub fn active_tiers(&self) -> Vec<&'static str> {
        let mut tiers = vec!["postgres"]; // Tier 1 is always required
        if self.has_redis() {
            tiers.push("redis");
        }
        if self.has_qdrant() {
            tiers.push("qdrant");
        }
        if self.has_minio() {
            tiers.push("minio");
        }
        for instance in self.backend_instances.active() {
            if let Some(kind) =
                crate::planning::backend::BackendKind::from_store_kind("", &instance.backend)
            {
                let tier_backend = kind.as_str();
                if !tiers.contains(&tier_backend) {
                    tiers.push(tier_backend);
                }
            }
        }
        tiers
    }

    /// Build a `UdbConfig` skeleton that carries the deploy profile resolved
    /// from environment variables.
    ///
    /// This is the recommended startup call.  The resulting `UdbConfig` has all
    /// connection-level fields set to their defaults; callers must populate the
    /// `primary`, `redis`, `qdrant`, and `minio` blocks from a YAML file or
    /// additional env-vars, then call [`validate`] before connecting.
    ///
    /// The deploy profile is written into the top-level `deploy` field **and**
    /// propagated into the per-backend `deploy` field on each config struct so
    /// that TLS and endpoint defaults are immediately available.
    /// Load a `UdbConfig` from a YAML, JSON, or TOML file.
    pub fn load_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let config: Self = match ext.to_lowercase().as_str() {
            "toml" => toml::from_str(&content)
                .map_err(|e| format!("failed to parse TOML config {}: {e}", path.display()))?,
            "yaml" | "yml" | "json" => serde_yaml::from_str(&content)
                .map_err(|e| format!("failed to parse YAML config {}: {e}", path.display()))?,
            _ => serde_yaml::from_str(&content)
                .or_else(|_| toml::from_str(&content))
                .map_err(|e| format!("failed to parse config {}: {e}", path.display()))?,
        };
        Ok(config)
    }

    /// Build config by deterministic merging: defaults → file → env vars.
    pub fn from_merged(file_path: Option<&Path>) -> Self {
        let mut cfg = Self::default();
        if let Some(path) = file_path
            && path.exists()
        {
            match Self::load_file(path) {
                Ok(file_cfg) => cfg = merge_udb_config(cfg, file_cfg),
                Err(err) => eprintln!("udb config warning: {err}"),
            }
        }
        cfg.merge_env();
        cfg
    }

    /// Build config from the standard env-selected config path plus env overlay.
    ///
    /// This is the compatibility boundary for legacy env-first startup and
    /// reload flows. Runtime hot paths should consume the resolved `UdbConfig`
    /// instead of reading process env directly.
    pub fn from_merged_env() -> Self {
        let config_path = std::env::var("UDB_CONFIG_PATH")
            .ok()
            .map(std::path::PathBuf::from);
        Self::from_merged(config_path.as_deref())
    }

    /// Overlay environment variables onto this config. OS env wins over file values.
    #[allow(clippy::field_reassign_with_default)]
    pub fn merge_env(&mut self) {
        let production_env = std::env::var("UDB_ENV")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "production" | "prod"
                )
            })
            .unwrap_or(false);
        // Deployment profile
        let profile = DeployProfile::from_env();
        self.deploy = profile.clone();

        // PostgreSQL primary. Prefer an explicit URL, then fall back to
        // libpq-style component vars (PGHOST/PGUSER/PGPASSWORD/…) emitted by
        // managed providers and the psql client.
        if let Some(dsn) = env_first(&["UDB_PG_DSN", "DATABASE_URL"]).or_else(pg_dsn_from_libpq_env)
        {
            self.primary.direct_dsn = dsn;
        }
        self.primary.deploy = profile.postgres.clone();
        if let Some(v) = env_u32("UDB_PG_MIN_CONNECTIONS") {
            self.primary.min_connections = v as i32;
        }
        if let Some(v) = env_u32("UDB_PG_MAX_CONNECTIONS") {
            self.primary.max_open_conns = v as i32;
        }
        if let Some(v) = env_u32("UDB_PG_ACQUIRE_TIMEOUT") {
            self.primary.acquire_timeout_secs = v as u64;
        }
        if let Some(v) = env_u32("UDB_PG_IDLE_TIMEOUT") {
            self.primary.conn_max_idle_secs = v as u64;
        }
        if let Some(v) = env_u32("UDB_PG_MAX_LIFETIME") {
            self.primary.conn_max_lifetime_secs = v as u64;
        }

        // Redis
        if let Some(dsn) = env_first(&["UDB_REDIS_DSN", "REDIS_URL"]) {
            let mut redis = self.redis.clone().unwrap_or_default();
            redis.dsn = Some(dsn);
            redis.deploy = profile.redis.clone();
            self.redis = Some(redis);
        }

        // Qdrant
        if let Some(url) = env_first(&["UDB_QDRANT_URL", "QDRANT_URL"]) {
            let mut qdrant = self.qdrant.clone().unwrap_or_default();
            qdrant.url = Some(url);
            qdrant.deploy = profile.qdrant.clone();
            if let Some(v) = env_first(&["UDB_QDRANT_API_KEY", "QDRANT_API_KEY"]) {
                qdrant.api_key = v;
            }
            if let Some(v) = env_u32("UDB_QDRANT_TIMEOUT_SECS") {
                qdrant.max_retries = v;
            }
            self.qdrant = Some(qdrant);
        }

        // MinIO / S3
        if let Some(endpoint) = env_first(&["UDB_MINIO_ENDPOINT", "S3_ENDPOINT"]) {
            let mut minio = self.minio.clone().unwrap_or_default();
            minio.endpoint = endpoint;
            minio.deploy = profile.minio.clone();
            if let Some(v) = env_first(&["UDB_MINIO_ACCESS_KEY", "AWS_ACCESS_KEY_ID"]) {
                minio.access_key = v;
            }
            if let Some(v) = env_first(&["UDB_MINIO_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"]) {
                minio.secret_key = v;
            }
            if let Some(v) = env_first(&["UDB_MINIO_REGION", "AWS_REGION"]) {
                minio.region = v;
            }
            self.minio = Some(minio);
        }

        // Backend instances. Explicit env descriptors override file instances;
        // legacy env-derived instances fill the catalog only when the file did
        // not define any portable instances.
        let env_instances = BackendInstanceConfig::from_env();
        if std::env::var("UDB_BACKEND_INSTANCES")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
            || self.backend_instances.instances.is_empty()
        {
            self.backend_instances = env_instances;
        }
        self.backend_instances.resolve_env_dsns();

        // Runtime tuning
        if let Ok(v) = std::env::var("UDB_APP_NAME")
            && !v.trim().is_empty()
        {
            self.app_name = v;
        }
        if let Some(v) = env_i32("UDB_DEFAULT_LIMIT") {
            self.default_limit = v;
        }
        if let Some(v) = env_i32("UDB_MAX_LIMIT") {
            self.max_limit = v;
        }
        if let Some(v) = env_u32("UDB_DDL_CONCURRENCY") {
            self.ddl_concurrency = v.max(1) as usize;
        }
        if let Ok(v) = std::env::var("UDB_PG_REPLICA_DSNS")
            && !v.trim().is_empty()
        {
            self.pg_replica_dsns = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if self.pg_replica_dsns.is_empty()
            && let Ok(v) = std::env::var("UDB_PG_REPLICA_DSN")
            && !v.trim().is_empty()
        {
            self.pg_replica_dsns = vec![v.trim().to_string()];
        }
        if let Ok(v) = std::env::var("UDB_PG_REPLICA_STRATEGY")
            && !v.trim().is_empty()
        {
            self.pg_replica_strategy = v;
        }
        if let Some(v) = env_u32("UDB_PG_REPLICA_MAX_LAG_SECS") {
            self.pg_replica_max_lag_secs = v as u64;
        }
        if let Some(v) = bool_env("UDB_PG_REPLICA_FAIL_OPEN") {
            self.pg_replica_fail_open = v;
        }
        if let Some(v) = env_u32("UDB_PG_REPLICA_MIN_CONNECTIONS") {
            self.pg_replica_min_connections = v;
        }
        if let Some(v) = env_u32("UDB_PG_REPLICA_MAX_CONNECTIONS") {
            self.pg_replica_max_connections = v;
        }
        if let Some(v) = env_u32("UDB_PG_REPLICA_HEALTH_INTERVAL_SECS") {
            self.pg_replica_health_interval_secs = v as u64;
        }
        if let Some(v) = env_i32("UDB_MATERIALIZED_VIEW_TTL_DAYS") {
            self.materialized_view_ttl_days = v;
        }
        if let Ok(v) = std::env::var("UDB_ABAC_SCHEMA")
            && !v.trim().is_empty()
        {
            self.abac_schema = v;
        }
        if let Ok(v) = std::env::var("UDB_ABAC_TABLE")
            && !v.trim().is_empty()
        {
            self.abac_table = v;
        }
        if let Some(v) = env_first(&["UDB_KAFKA_BROKERS", "KAFKA_BROKERS"]) {
            self.kafka_brokers = Some(v);
        }

        // Migration options — overlay env onto file-loaded config (do NOT
        // replace, which would discard YAML-configured hooks/approval/sinks).
        self.migration.merge_env();
        self.audit_sink.merge_env();
        self.service.merge_env();
        self.native_services.merge_env();
        self.channels.merge_env();
        self.cdc.merge_env();
        self.security.merge_env();
        self.encryption.merge_env();
        if production_env {
            self.encryption.object_native_state_required = true;
        }
        self.saga.merge_env();
        self.circuit_breaker.merge_env();
        if let Ok(v) = std::env::var("UDB_PROJECT_ROUTING_MODE")
            && !v.trim().is_empty()
        {
            self.project_routing_mode = v;
        }
        self.system_catalog.merge_env();
    }

    /// Build a `UdbConfig` skeleton entirely from environment variables.
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_env() -> Self {
        let mut cfg = Self::default();
        cfg.merge_env();
        cfg
    }

    pub fn validate(&self) -> ConfigValidationReport {
        validate_udb_config(self, None)
    }

    pub fn validate_with_dsn_catalog(&self, catalog: &UnifiedDsnCatalog) -> ConfigValidationReport {
        validate_udb_config(self, Some(catalog))
    }
}

pub fn validate_udb_config(
    config: &UdbConfig,
    catalog: Option<&UnifiedDsnCatalog>,
) -> ConfigValidationReport {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut health_gates = Vec::new();

    if !config.primary.is_configured()
        && config.primary.direct_dsn.trim().is_empty()
        && config.primary.pooler_dsn.trim().is_empty()
    {
        errors.push("primary PostgreSQL config is required".to_string());
    }

    if config.failover.enabled && !config.has_backup() {
        errors.push("failover.enabled=true requires a configured backup database".to_string());
    }

    if let Some(redis) = &config.redis
        && redis.is_configured()
        && redis.mode == "sentinel"
        && redis.sentinel_master.is_empty()
    {
        errors.push("redis.mode=sentinel requires sentinel_master".to_string());
    }
    if let Some(qdrant) = &config.qdrant
        && qdrant.is_configured()
        && qdrant.default_dimension == 0
    {
        warnings.push(
            "qdrant.default_dimension is 0; vector manifests must provide explicit dimensions"
                .to_string(),
        );
    }
    if let Some(minio) = &config.minio
        && minio.is_configured()
        && (minio.access_key.is_empty() || minio.secret_key.is_empty())
    {
        warnings.push(
            "minio endpoint is configured but access_key/secret_key are incomplete".to_string(),
        );
    }

    // DSN validity checks
    if !config.primary.direct_dsn.is_empty() && !looks_like_dsn(&config.primary.direct_dsn) {
        errors.push(format!(
            "primary.direct_dsn does not look like a valid connection string: {}",
            config.primary.direct_dsn
        ));
    }
    if let Some(redis) = &config.redis
        && let Some(dsn) = &redis.dsn
        && !looks_like_dsn(dsn)
    {
        errors.push(format!("redis dsn does not look valid: {dsn}"));
    }
    if let Some(qdrant) = &config.qdrant
        && let Some(url) = &qdrant.url
        && !looks_like_dsn(url)
    {
        errors.push(format!("qdrant url does not look valid: {url}"));
    }

    // TLS mismatch checks
    if config.primary.deploy.mode == DeployMode::Cloud
        && !config.primary.direct_dsn.is_empty()
        && !config
            .primary
            .direct_dsn
            .to_lowercase()
            .contains("sslmode=require")
        && !config.primary.ssl_mode.eq_ignore_ascii_case("require")
    {
        warnings.push(
            "primary PostgreSQL deploy mode is Cloud but TLS/SSL is not explicitly required"
                .to_string(),
        );
    }
    if let Some(redis) = &config.redis
        && redis.deploy.mode == DeployMode::Cloud
        && !redis.effective_tls()
    {
        warnings.push("redis deploy mode is Cloud but TLS is not enabled".to_string());
    }
    if let Some(qdrant) = &config.qdrant
        && qdrant.deploy.mode == DeployMode::Cloud
        && !qdrant.effective_tls()
    {
        warnings.push("qdrant deploy mode is Cloud but TLS is not enabled".to_string());
    }
    if let Some(minio) = &config.minio
        && minio.deploy.mode == DeployMode::Cloud
        && !minio.effective_ssl()
    {
        warnings.push("minio deploy mode is Cloud but SSL/TLS is not enabled".to_string());
    }

    errors.extend(config.backend_instances.validate());
    errors.extend(config.audit_sink.validate());
    if let Err(err) = config.encryption.validate_required_key_source() {
        errors.push(err);
    }

    health_gates.push(health_gate(
        "sql",
        "postgres",
        true,
        config.primary.is_configured()
            || !config.primary.direct_dsn.is_empty()
            || !config.primary.pooler_dsn.is_empty(),
    ));
    health_gates.push(health_gate("cache", "redis", false, config.has_redis()));
    health_gates.push(health_gate("vector", "qdrant", false, config.has_qdrant()));
    health_gates.push(health_gate("object", "minio", false, config.has_minio()));
    for status in crate::runtime::service::native_registry::resolved_native_service_statuses(config)
        .into_iter()
        .filter(|status| status.enabled && !status.required_backends.is_empty())
    {
        for backend in &status.required_backends {
            health_gates.push(StartupHealthGate {
                tier: "native_service".to_string(),
                backend: format!("{}:{backend}", status.service_id),
                required: true,
                configured: !status
                    .missing_dependencies
                    .iter()
                    .any(|missing| missing == backend),
                degraded_mode_allowed: true,
                message: if status
                    .missing_dependencies
                    .iter()
                    .any(|missing| missing == backend)
                {
                    format!(
                        "native service {} requires backend {}",
                        status.service_id, backend
                    )
                } else {
                    format!(
                        "native service {} dependency {} configured",
                        status.service_id, backend
                    )
                },
            });
        }
    }

    if let Some(catalog) = catalog {
        let active_backends = config.active_tiers().into_iter().collect::<BTreeSet<_>>();
        for entry in &catalog.entries {
            match parse_unified_dsn(&entry.dsn) {
                Ok(parsed) => {
                    if !active_backends.contains(parsed.backend.as_str())
                        && parsed.backend != "postgres"
                    {
                        warnings.push(format!(
                            "DSN entry {} targets backend '{}' that is not configured",
                            entry.id, parsed.backend
                        ));
                    }
                    if parsed.env_key != entry.env_key {
                        errors.push(format!(
                            "DSN entry {} env mismatch: entry={} dsn={}",
                            entry.id, entry.env_key, parsed.env_key
                        ));
                    }
                }
                Err(err) => errors.push(format!("DSN entry {} invalid: {}", entry.id, err)),
            }
        }
    }

    ConfigValidationReport {
        passed: errors.is_empty(),
        errors,
        warnings,
        health_gates,
    }
}

// ── Generic named backend instances ──────────────────────────────────────────

/// Heuristic: does a string look like a connection string / URL?
fn looks_like_dsn(value: &str) -> bool {
    let value = value.trim();
    value.contains("://")
        || value.starts_with("postgres://")
        || value.starts_with("redis://")
        || value.starts_with("mongodb://")
        || value.starts_with("mongodb+srv://")
        || value.starts_with("bolt://")
        || value.starts_with("clickhouse://")
        || value.starts_with("http://")
        || value.starts_with("https://")
        // SQL Server / MSSQL ADO.NET connection string (parsed by tiberius'
        // `from_ado_string`), e.g. `Server=host,1433;User=sa;Password=...;Database=udb`.
        // It is schemeless by design, so the `://` checks above never match it.
        || value.to_ascii_lowercase().contains("server=")
        // Cassandra / ScyllaDB contact points: a schemeless `host:port` list, e.g.
        // `127.0.0.1:9042` or `n1:9042,n2:9042` (the form the cassandra executor
        // and the live conformance suite both use).
        || is_host_port_list(value)
}

/// True when `value` is a non-empty, comma-separated list where every element is
/// `host:port` with a numeric port — the schemeless contact-point form used by
/// the Cassandra/ScyllaDB backend. Keeps `looks_like_dsn` from rejecting a valid
/// `UDB_CASSANDRA_DSN` just because it carries no URL scheme.
fn is_host_port_list(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value
            .split(',')
            .all(|part| match part.trim().rsplit_once(':') {
                Some((host, port)) => {
                    !host.trim().is_empty()
                        && !port.is_empty()
                        && port.chars().all(|c| c.is_ascii_digit())
                }
                None => false,
            })
}

fn health_gate(tier: &str, backend: &str, required: bool, configured: bool) -> StartupHealthGate {
    StartupHealthGate {
        tier: tier.to_string(),
        backend: backend.to_string(),
        required,
        configured,
        degraded_mode_allowed: !required,
        message: if configured {
            "configured".to_string()
        } else if required {
            "required backend is not configured".to_string()
        } else {
            "optional backend not configured; degraded mode allowed".to_string()
        },
    }
}

#[cfg(test)]
mod tests;

// Phase G: config.rs split into grouped child modules.
mod backends;
mod deploy;
mod instances;
pub use backends::*;
pub use deploy::*;
pub use instances::*;
