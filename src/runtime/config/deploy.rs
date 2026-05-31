//! config.rs split — deploy group (Phase G).
use super::*;

/// Whether a backend instance is self-hosted (Docker / on-premise) or a
/// managed cloud service.
///
/// The mode affects TLS requirements, endpoint format, default ports, auth
/// mechanisms, and health-check conventions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeployMode {
    /// Docker container, bare-metal, or on-premise server.
    /// Typically no TLS by default; uses standard ports; auth via local
    /// password or network isolation.
    #[default]
    SelfHosted,
    /// Managed cloud service (Neon, Supabase, Upstash, Qdrant Cloud,
    /// AWS RDS, Atlas, etc.).
    /// TLS required; often uses non-standard pooler ports; auth via API key
    /// or connection string with embedded credentials.
    Cloud,
}

impl DeployMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SelfHosted => "self_hosted",
            Self::Cloud => "cloud",
        }
    }

    /// Parse from a string.  Accepts `self_hosted`, `selfhosted`, `docker`,
    /// `local`, and `cloud`, `managed`, `saas`.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(value: &str) -> Self {
        match value.to_lowercase().replace('-', "_").as_str() {
            "self_hosted" | "selfhosted" | "docker" | "local" | "on_prem" | "on_premise" => {
                Self::SelfHosted
            }
            "cloud" | "managed" | "saas" | "hosted" => Self::Cloud,
            _ => Self::SelfHosted,
        }
    }
}

/// Connection profile that combines a raw DSN (or its components) with a
/// deploy mode.  Used as the canonical input for building backend connections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BackendDeployConfig {
    /// Deploy mode: `SelfHosted` or `Cloud`.
    pub mode: DeployMode,
    /// Raw connection string / DSN (if provided directly).
    /// When set, takes precedence over individual host/port/credential fields.
    pub dsn: Option<String>,
    /// Override the default TLS requirement (derived from `mode` if absent).
    /// `None` means "use the mode default".
    pub tls_override: Option<bool>,
    /// Optional display label (e.g. `"primary"`, `"qdrant-cloud-eu"`) for logs.
    pub label: String,
}

impl BackendDeployConfig {
    /// Returns whether TLS should be enabled for this backend.
    ///
    /// Cloud mode requires TLS unless explicitly overridden.
    /// Self-hosted mode defaults to no TLS.
    pub fn tls_required(&self) -> bool {
        match self.tls_override {
            Some(v) => v,
            None => self.mode == DeployMode::Cloud,
        }
    }

    /// Returns `true` when a raw DSN has been supplied.
    pub fn has_dsn(&self) -> bool {
        self.dsn
            .as_ref()
            .map(|d| !d.trim().is_empty())
            .unwrap_or(false)
    }
}

// ── Cloud-provider heuristics ─────────────────────────────────────────────────

/// Heuristically determine the deploy mode for a DSN / host string.
///
/// Returns `Cloud` when the string contains a well-known cloud-managed
/// hostname suffix or scheme indicator; `SelfHosted` otherwise.
pub fn detect_deploy_mode_from_dsn(dsn: &str) -> DeployMode {
    let lower = dsn.to_lowercase();
    // Cloud PostgreSQL providers
    if lower.contains(".neon.tech")
        || lower.contains(".supabase.co")
        || lower.contains(".supabase.com")
        || lower.contains("rds.amazonaws.com")
        || lower.contains("cloudsql")
        || lower.contains("azure.com")
        || lower.contains("planetscale.com")
        || lower.contains("cockroachlabs.cloud")
        || lower.contains("railway.app")
        || lower.contains("fly.io")
    {
        return DeployMode::Cloud;
    }
    // Cloud Redis providers
    if lower.contains(".upstash.io")
        || lower.contains(".redislabs.com")
        || lower.contains(".elasticache.amazonaws.com")
    {
        return DeployMode::Cloud;
    }
    // Cloud Qdrant
    if lower.contains(".qdrant.io") || lower.contains("cloud.qdrant.io") {
        return DeployMode::Cloud;
    }
    // Cloud S3 / object storage
    if lower.contains(".amazonaws.com")
        || lower.contains(".blob.core.windows.net")
        || lower.contains(".storage.googleapis.com")
        || lower.contains(".digitaloceanspaces.com")
        || lower.contains(".backblazeb2.com")
    {
        return DeployMode::Cloud;
    }
    // Cloud MongoDB
    if lower.contains(".mongodb.net") || lower.contains("atlas.mongodb.com") {
        return DeployMode::Cloud;
    }
    // Cloud Neo4j / Aura
    if lower.contains(".databases.neo4j.io") || lower.contains("neo4j+s://") {
        return DeployMode::Cloud;
    }
    // Cloud ClickHouse
    if lower.contains(".clickhouse.cloud") {
        return DeployMode::Cloud;
    }
    DeployMode::SelfHosted
}

/// Read the deploy mode for a specific backend from env-vars, falling back to
/// DSN-based heuristic detection.
///
/// * `mode_env_key` — e.g. `"UDB_PG_DEPLOY_MODE"`
/// * `dsn_env_keys` — ordered list of DSN env keys to check for heuristic, e.g.
///   `&["UDB_PG_DSN", "DATABASE_URL"]`
pub fn resolve_deploy_mode(mode_env_key: &str, dsn_env_keys: &[&str]) -> DeployMode {
    // 1. Explicit override wins
    if let Ok(val) = std::env::var(mode_env_key)
        && !val.trim().is_empty()
    {
        return DeployMode::from_str(&val);
    }
    // 2. Heuristic from the first non-empty DSN found
    for key in dsn_env_keys {
        if let Ok(dsn) = std::env::var(key)
            && !dsn.trim().is_empty()
        {
            let mode = detect_deploy_mode_from_dsn(&dsn);
            if mode == DeployMode::Cloud {
                return mode;
            }
        }
    }
    // 3. Default: self-hosted
    DeployMode::SelfHosted
}

/// Build a `BackendDeployConfig` from environment for a given backend.
pub fn backend_deploy_config_from_env(
    mode_env_key: &str,
    dsn_env_keys: &[&str],
    label: &str,
) -> BackendDeployConfig {
    // Collect the first non-empty DSN from the env key list
    let dsn = dsn_env_keys
        .iter()
        .find_map(|key| std::env::var(key).ok().filter(|v| !v.trim().is_empty()));
    let mode = resolve_deploy_mode(mode_env_key, dsn_env_keys);
    BackendDeployConfig {
        mode,
        dsn,
        tls_override: None,
        label: label.to_string(),
    }
}

/// Resolve deploy configs for all backends from the environment.
///
/// Returns a `DeployProfile` describing the mode of every backend the UDB
/// can connect to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DeployProfile {
    pub postgres: BackendDeployConfig,
    pub redis: BackendDeployConfig,
    pub qdrant: BackendDeployConfig,
    pub minio: BackendDeployConfig,
    pub mongodb: BackendDeployConfig,
    pub neo4j: BackendDeployConfig,
    pub clickhouse: BackendDeployConfig,
}

impl DeployProfile {
    /// Build the full deploy profile from environment variables.
    ///
    /// Each backend checks its `UDB_<BACKEND>_DEPLOY_MODE` override first,
    /// then falls back to cloud-pattern heuristics on the raw DSN env vars.
    pub fn from_env() -> Self {
        Self {
            postgres: backend_deploy_config_from_env(
                "UDB_PG_DEPLOY_MODE",
                &["UDB_PG_DSN", "DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN"],
                "postgres",
            ),
            redis: backend_deploy_config_from_env(
                "UDB_REDIS_DEPLOY_MODE",
                &["UDB_REDIS_DSN", "REDIS_URL", "REDIS_DSN"],
                "redis",
            ),
            qdrant: backend_deploy_config_from_env(
                "UDB_QDRANT_DEPLOY_MODE",
                &["UDB_QDRANT_DSN", "QDRANT_URL", "QDRANT_DSN"],
                "qdrant",
            ),
            minio: backend_deploy_config_from_env(
                "UDB_MINIO_DEPLOY_MODE",
                &[
                    "UDB_MINIO_DSN",
                    "MINIO_ENDPOINT",
                    "AWS_ENDPOINT_URL",
                    "S3_ENDPOINT",
                ],
                "minio",
            ),
            mongodb: backend_deploy_config_from_env(
                "UDB_MONGO_DEPLOY_MODE",
                &["UDB_NOSQL_DSN", "MONGODB_URI", "MONGO_DSN"],
                "mongodb",
            ),
            neo4j: backend_deploy_config_from_env(
                "UDB_NEO4J_DEPLOY_MODE",
                &["UDB_GRAPH_DSN", "NEO4J_URI", "NEO4J_DSN"],
                "neo4j",
            ),
            clickhouse: backend_deploy_config_from_env(
                "UDB_CH_DEPLOY_MODE",
                &["UDB_COLUMN_DSN", "CLICKHOUSE_URL", "CLICKHOUSE_DSN"],
                "clickhouse",
            ),
        }
    }

    /// Returns a compact summary for structured startup logging.
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "postgres":    { "mode": self.postgres.mode.as_str(),    "tls": self.postgres.tls_required(),    "dsn_set": self.postgres.has_dsn() },
            "redis":       { "mode": self.redis.mode.as_str(),       "tls": self.redis.tls_required(),       "dsn_set": self.redis.has_dsn() },
            "qdrant":      { "mode": self.qdrant.mode.as_str(),      "tls": self.qdrant.tls_required(),      "dsn_set": self.qdrant.has_dsn() },
            "minio":       { "mode": self.minio.mode.as_str(),       "tls": self.minio.tls_required(),       "dsn_set": self.minio.has_dsn() },
            "mongodb":     { "mode": self.mongodb.mode.as_str(),     "tls": self.mongodb.tls_required(),     "dsn_set": self.mongodb.has_dsn() },
            "neo4j":       { "mode": self.neo4j.mode.as_str(),       "tls": self.neo4j.tls_required(),       "dsn_set": self.neo4j.has_dsn() },
            "clickhouse":  { "mode": self.clickhouse.mode.as_str(),  "tls": self.clickhouse.tls_required(),  "dsn_set": self.clickhouse.has_dsn() },
        })
    }

    /// Returns `true` when every configured backend is running in the same mode
    /// (all self-hosted or all cloud).
    pub fn is_uniform(&self) -> bool {
        let modes = [
            &self.postgres.mode,
            &self.redis.mode,
            &self.qdrant.mode,
            &self.minio.mode,
            &self.mongodb.mode,
            &self.neo4j.mode,
            &self.clickhouse.mode,
        ];
        let first = modes[0];
        modes.iter().all(|m| *m == first)
    }

    /// Return the list of backends that are in `Cloud` mode.
    pub fn cloud_backends(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if self.postgres.mode == DeployMode::Cloud {
            out.push("postgres");
        }
        if self.redis.mode == DeployMode::Cloud {
            out.push("redis");
        }
        if self.qdrant.mode == DeployMode::Cloud {
            out.push("qdrant");
        }
        if self.minio.mode == DeployMode::Cloud {
            out.push("minio");
        }
        if self.mongodb.mode == DeployMode::Cloud {
            out.push("mongodb");
        }
        if self.neo4j.mode == DeployMode::Cloud {
            out.push("neo4j");
        }
        if self.clickhouse.mode == DeployMode::Cloud {
            out.push("clickhouse");
        }
        out
    }

    /// Return the list of backends that are in `SelfHosted` mode.
    pub fn self_hosted_backends(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if self.postgres.mode == DeployMode::SelfHosted {
            out.push("postgres");
        }
        if self.redis.mode == DeployMode::SelfHosted {
            out.push("redis");
        }
        if self.qdrant.mode == DeployMode::SelfHosted {
            out.push("qdrant");
        }
        if self.minio.mode == DeployMode::SelfHosted {
            out.push("minio");
        }
        if self.mongodb.mode == DeployMode::SelfHosted {
            out.push("mongodb");
        }
        if self.neo4j.mode == DeployMode::SelfHosted {
            out.push("neo4j");
        }
        if self.clickhouse.mode == DeployMode::SelfHosted {
            out.push("clickhouse");
        }
        out
    }
}

// ── Migration options ─────────────────────────────────────────────────────────
