//! config.rs split — backends group (Phase G).
use super::*;

/// Configuration for a single PostgreSQL connection role (primary / backup / signal).
///
/// PostgreSQL is the only backend that hosts the migration ledger tables.
/// The Go UDB service resolves DSN strings from these fields.
/// Mirrors the legacy Go service `DBConfig` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DbConfig {
    /// Deployment mode: `SelfHosted` (Docker / on-premise) or `Cloud` (Neon,
    /// Supabase, RDS, Aiven, etc.).  Drives TLS defaults and pooler DSN format.
    pub deploy: BackendDeployConfig,
    /// PostgreSQL host (e.g. `"localhost"`, Neon project hostname).
    pub host: String,
    /// TCP port, defaults to `5432`.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Role / user name.
    pub role: String,
    /// Password (resolved from env; never logged or serialised in cleartext).
    pub password: String,
    /// SSL mode: `"require"`, `"disable"`, `"prefer"`.  When empty, derived
    /// from `deploy.mode`: Cloud → `"require"`, SelfHosted → `"prefer"`.
    pub ssl_mode: String,
    /// Pooler DSN (PgBouncer / Neon pooled endpoint) for application traffic.
    pub pooler_dsn: String,
    /// Direct DSN (non-pooled) for migrations and DDL.
    pub direct_dsn: String,
    /// Maximum open connections in the pooler.
    pub max_open_conns: i32,
    /// Maximum idle connections in the pooler.
    pub max_idle_conns: i32,
    /// Connection max lifetime in seconds.
    pub conn_max_lifetime_secs: u64,
    /// Connection max idle time in seconds.
    pub conn_max_idle_secs: u64,
    /// Minimum connections in the pool. Default: 5.
    #[serde(default = "five_i32")]
    pub min_connections: i32,
    /// Connection acquire timeout in seconds. Default: 10.
    #[serde(default = "ten_u64")]
    pub acquire_timeout_secs: u64,
}

impl DbConfig {
    /// Returns `true` when the minimum required fields (`host`, `database`, `role`) are set.
    pub fn is_configured(&self) -> bool {
        !self.host.is_empty() && !self.database.is_empty() && !self.role.is_empty()
    }

    /// Returns the effective SSL mode, falling back to a deploy-mode default when
    /// `ssl_mode` is empty:  Cloud → `"require"`, SelfHosted → `"prefer"`.
    pub fn effective_ssl_mode(&self) -> &str {
        if !self.ssl_mode.is_empty() {
            return &self.ssl_mode;
        }
        match self.deploy.mode {
            DeployMode::Cloud => "require",
            DeployMode::SelfHosted => "prefer",
        }
    }

    /// Returns a log-safe masked representation (host + role; password redacted).
    pub fn masked_log(&self) -> String {
        format!(
            "host={} db={} role={} ssl={} deploy={}",
            self.host,
            self.database,
            self.role,
            self.effective_ssl_mode(),
            self.deploy.mode.as_str(),
        )
    }
}

// ── Tier 2: Redis (Cache) ─────────────────────────────────────────────────────

/// Connection configuration for the Redis cache tier.
///
/// Used for: session cache, rate-limit counters, read-through cache, feature flags.
/// Spec §16.1 Tier 2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct RedisConfig {
    /// Deployment mode: `SelfHosted` (Docker / on-premise) or `Cloud`
    /// (Upstash, Redis Enterprise Cloud, ElastiCache, etc.).
    /// Cloud mode enables TLS by default on the connection.
    pub deploy: BackendDeployConfig,
    /// Redis host.  Defaults to `"localhost"`.
    pub host: String,
    /// Redis TCP port.  Defaults to `6379`.
    pub port: u16,
    /// Authentication password (ACL or `requirepass`; env-resolved).
    pub password: String,
    /// Logical database index (0–15).  Default: `0`.
    pub database: u8,
    /// Maximum number of connections in the pool.  Default: `20`.
    pub pool_size: u32,
    /// Minimum number of idle connections kept open.  Default: `2`.
    pub min_idle_conns: u32,
    /// Maximum number of retries on failure.  Default: `3`.
    pub max_retries: u32,
    /// Raw connection string (DSN). When set, takes precedence over host/port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<String>,
    /// Enable TLS for the connection.  When not set explicitly, derived from
    /// `deploy.mode`: Cloud → `true`, SelfHosted → `false`.
    pub tls_enabled: bool,
    /// Default key TTL in seconds.  `0` = no expiry.  Default: `3600` (1 h).
    pub default_ttl_secs: u64,
    /// Sentinel / Cluster mode: `"standalone"`, `"sentinel"`, `"cluster"`.
    pub mode: String,
    /// Sentinel master name (only used when `mode = "sentinel"`).
    pub sentinel_master: String,
}

impl RedisConfig {
    /// Returns `true` when host or dsn is non-empty.
    pub fn is_configured(&self) -> bool {
        !self.host.is_empty() || self.dsn.as_ref().map(|d| !d.is_empty()).unwrap_or(false)
    }

    /// Returns whether TLS is active, honouring the explicit field first,
    /// then falling back to the deploy-mode default (Cloud → true).
    pub fn effective_tls(&self) -> bool {
        if self.tls_enabled {
            return true;
        }
        self.deploy.tls_required()
    }

    /// Returns a log-safe representation.
    pub fn masked_log(&self) -> String {
        format!(
            "host={}:{} db={} tls={} mode={} deploy={}",
            self.host,
            self.port,
            self.database,
            self.effective_tls(),
            self.mode,
            self.deploy.mode.as_str(),
        )
    }

    /// Returns the effective TCP port, defaulting to `6379`.
    pub fn effective_port(&self) -> u16 {
        if self.port > 0 { self.port } else { 6379 }
    }
}

// ── Tier 3: Qdrant (Vector store) ────────────────────────────────────────────

/// Connection configuration for the Qdrant vector store tier.
///
/// Used for: embedding similarity search, OCR correction history,
/// template clustering.  Spec §16.1 Tier 3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct QdrantConfig {
    /// Deployment mode: `SelfHosted` (Docker / on-premise) or `Cloud`
    /// (Qdrant Cloud).  Cloud mode requires an API key and TLS.
    pub deploy: BackendDeployConfig,
    /// Qdrant host.  Defaults to `"localhost"`.
    pub host: String,
    /// gRPC port (preferred for high-throughput).  Defaults to `6334`.
    pub grpc_port: u16,
    /// HTTP/REST port (health checks, admin UI).  Defaults to `6333`.
    pub http_port: u16,
    /// API key for Qdrant Cloud or secured on-premise deployments.
    pub api_key: String,
    /// Enable TLS on the gRPC channel.  When not set explicitly, derived from
    /// `deploy.mode`: Cloud → `true`, SelfHosted → `false`.
    pub tls_enabled: bool,
    /// Default vector dimension (must match embedding model output).
    /// e.g. `1536` for OpenAI `text-embedding-3-small`.
    pub default_dimension: u32,
    /// Default distance metric: `"Cosine"`, `"Euclid"`, `"Dot"`.  Default: `"Cosine"`.
    pub default_distance: String,
    /// Maximum retries on transient gRPC errors.  Default: `3`.
    pub max_retries: u32,
    /// Raw URL / DSN. When set, takes precedence over host/port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl QdrantConfig {
    /// Returns `true` when host or url is non-empty.
    pub fn is_configured(&self) -> bool {
        !self.host.is_empty() || self.url.as_ref().map(|u| !u.is_empty()).unwrap_or(false)
    }

    /// Returns whether TLS is active, honouring the explicit field first,
    /// then falling back to the deploy-mode default (Cloud → true).
    pub fn effective_tls(&self) -> bool {
        if self.tls_enabled {
            return true;
        }
        self.deploy.tls_required()
    }

    /// Returns a log-safe representation.
    pub fn masked_log(&self) -> String {
        format!(
            "host={} grpc={} http={} tls={} dim={} dist={} deploy={}",
            self.host,
            self.effective_grpc_port(),
            self.effective_http_port(),
            self.effective_tls(),
            self.default_dimension,
            self.default_distance,
            self.deploy.mode.as_str(),
        )
    }

    /// Returns the effective gRPC port, defaulting to `6334`.
    pub fn effective_grpc_port(&self) -> u16 {
        if self.grpc_port > 0 {
            self.grpc_port
        } else {
            6334
        }
    }

    /// Returns the effective HTTP port, defaulting to `6333`.
    pub fn effective_http_port(&self) -> u16 {
        if self.http_port > 0 {
            self.http_port
        } else {
            6333
        }
    }

    /// Returns the effective distance metric, defaulting to `"Cosine"`.
    pub fn effective_distance(&self) -> &str {
        if self.default_distance.is_empty() {
            "Cosine"
        } else {
            &self.default_distance
        }
    }
}

// ── Tier 4: MinIO (Object store) ──────────────────────────────────────────────

/// Connection configuration for the MinIO / S3-compatible object store tier.
///
/// Used for: OCR artifact storage, processed document exports, model archives.
/// Spec §16.1 Tier 4.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MinioConfig {
    /// Deployment mode: `SelfHosted` (Docker MinIO / on-premise) or `Cloud`
    /// (AWS S3, GCS, Azure Blob, DigitalOcean Spaces, Tigris, etc.).
    /// Cloud mode enables TLS by default and may affect endpoint resolution.
    pub deploy: BackendDeployConfig,
    /// MinIO / S3 endpoint URL, e.g. `"http://minio:9000"` or `"https://s3.amazonaws.com"`.
    pub endpoint: String,
    /// Access key ID (env-resolved).
    pub access_key: String,
    /// Secret access key (env-resolved; never logged).
    pub secret_key: String,
    /// Enable SSL/TLS for the endpoint.  When not set explicitly, derived from
    /// `deploy.mode`: Cloud → `true`, SelfHosted → `false`.
    pub use_ssl: bool,
    /// Optional prefix prepended to all bucket names.  e.g. `"tenant-a-"`.
    pub bucket_prefix: String,
    /// AWS/MinIO region.  Default: `"us-east-1"`.
    pub region: String,
    /// Maximum upload/download retries.  Default: `3`.
    pub max_retries: u32,
    /// Part size (bytes) for multipart uploads.  Default: `64 MiB`.
    pub multipart_part_size_bytes: u64,
}

impl MinioConfig {
    /// Returns `true` when endpoint is non-empty.
    pub fn is_configured(&self) -> bool {
        !self.endpoint.is_empty()
    }

    /// Returns whether SSL/TLS is active, honouring the explicit `use_ssl` field
    /// first, then falling back to the deploy-mode default (Cloud → true).
    pub fn effective_ssl(&self) -> bool {
        if self.use_ssl {
            return true;
        }
        self.deploy.tls_required()
    }

    /// Returns a log-safe representation (secret key redacted).
    pub fn masked_log(&self) -> String {
        format!(
            "endpoint={} region={} ssl={} prefix={} deploy={}",
            self.endpoint,
            self.effective_region(),
            self.effective_ssl(),
            self.bucket_prefix,
            self.deploy.mode.as_str(),
        )
    }

    /// Returns the effective region, defaulting to `"us-east-1"`.
    pub fn effective_region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }

    /// Returns the qualified bucket name for a given logical bucket.
    pub fn qualified_bucket(&self, logical_name: &str) -> String {
        format!("{}{}", self.bucket_prefix, logical_name)
    }
}

// ── Failover config ───────────────────────────────────────────────────────────

/// Controls automatic failover from primary to backup DB.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct FailoverConfig {
    /// Enable automatic failover when the primary DB is unhealthy.
    pub enabled: bool,
    /// Number of consecutive health-check failures before triggering failover.
    pub failure_threshold: u32,
    /// Minimum interval between failovers, e.g. `"5m"`.
    pub cooldown: String,
}

// ── Audit event sink ─────────────────────────────────────────────────────────

/// Destination type for the audit event sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AuditSinkKind {
    /// No audit sink — events are discarded (default, backward compatible).
    #[default]
    None,
    /// Emit audit events as structured JSON lines to stdout.
    Stdout,
    /// Append audit events to a file.
    File,
    /// Publish audit events to a Kafka topic.
    Kafka,
    /// Insert audit events into a PostgreSQL table.
    Postgres,
}

/// Configuration for the UDB audit event sink.
///
/// Loaded from env-vars at startup:
///   `UDB_AUDIT_SINK`             — sink kind (`none`|`stdout`|`file`|`kafka`|`postgres`)
///   `UDB_AUDIT_FILE_PATH`        — path when sink=file
///   `UDB_AUDIT_KAFKA_TOPIC`      — Kafka topic when sink=kafka
///   `UDB_AUDIT_KAFKA_BROKERS`    — comma-separated brokers when sink=kafka
///   `UDB_AUDIT_PG_TABLE`         — fully-qualified table name when sink=postgres
///   `UDB_AUDIT_MIN_SEVERITY`     — minimum severity to emit (`info`|`warn`|`error`); default `info`
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AuditSinkConfig {
    pub kind: AuditSinkKind,
    /// Target file path (used when `kind == File`).
    pub file_path: Option<String>,
    /// Kafka topic (used when `kind == Kafka`).
    pub kafka_topic: Option<String>,
    /// Comma-separated Kafka broker list (used when `kind == Kafka`).
    pub kafka_brokers: Option<String>,
    /// Fully-qualified PostgreSQL table for audit rows (used when `kind == Postgres`).
    pub pg_table: Option<String>,
    /// Minimum severity to emit.  Accepts `"info"`, `"warn"`, `"error"`.
    pub min_severity: String,
}

impl AuditSinkConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let kind = match std::env::var("UDB_AUDIT_SINK")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "stdout" => AuditSinkKind::Stdout,
            "file" => AuditSinkKind::File,
            "kafka" => AuditSinkKind::Kafka,
            "postgres" | "pg" => AuditSinkKind::Postgres,
            _ => AuditSinkKind::None,
        };
        let min_severity =
            std::env::var("UDB_AUDIT_MIN_SEVERITY").unwrap_or_else(|_| "info".to_string());
        Self {
            kind,
            file_path: std::env::var("UDB_AUDIT_FILE_PATH")
                .ok()
                .filter(|v| !v.is_empty()),
            kafka_topic: std::env::var("UDB_AUDIT_KAFKA_TOPIC")
                .ok()
                .filter(|v| !v.is_empty()),
            kafka_brokers: std::env::var("UDB_AUDIT_KAFKA_BROKERS")
                .ok()
                .filter(|v| !v.is_empty()),
            pg_table: std::env::var("UDB_AUDIT_PG_TABLE")
                .ok()
                .filter(|v| !v.is_empty()),
            min_severity,
        }
    }

    /// Returns `true` when the sink is active (i.e. not `None`).
    pub fn is_active(&self) -> bool {
        self.kind != AuditSinkKind::None
    }

    /// Returns a validation error if the configuration is inconsistent.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        match self.kind {
            AuditSinkKind::File if self.file_path.is_none() => {
                errors
                    .push("UDB_AUDIT_SINK=file requires UDB_AUDIT_FILE_PATH to be set".to_string());
            }
            AuditSinkKind::Kafka if self.kafka_topic.is_none() => {
                errors.push(
                    "UDB_AUDIT_SINK=kafka requires UDB_AUDIT_KAFKA_TOPIC to be set".to_string(),
                );
            }
            AuditSinkKind::Kafka if self.kafka_brokers.is_none() => {
                errors.push(
                    "UDB_AUDIT_SINK=kafka requires UDB_AUDIT_KAFKA_BROKERS to be set".to_string(),
                );
            }
            AuditSinkKind::Postgres if self.pg_table.is_none() => {
                errors.push(
                    "UDB_AUDIT_SINK=postgres requires UDB_AUDIT_PG_TABLE to be set".to_string(),
                );
            }
            _ => {}
        }
        match self.min_severity.as_str() {
            "info" | "warn" | "error" => {}
            other => errors.push(format!(
                "UDB_AUDIT_MIN_SEVERITY='{other}' is not valid; use 'info', 'warn', or 'error'"
            )),
        }
        errors
    }
}

// ── Top-level UDB config ──────────────────────────────────────────────────────
