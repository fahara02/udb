//! config.rs split — instances group (Phase G).
use super::*;

/// Runtime role for a backend instance.
///
/// A single physical backend can participate in reads, writes, admin/resource
/// operations, or all of them. Routing will use this role alongside backend
/// capabilities and health probes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackendInstanceRole {
    Read,
    Write,
    #[default]
    ReadWrite,
    Admin,
}

impl BackendInstanceRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::ReadWrite => "read_write",
            Self::Admin => "admin",
        }
    }

    pub fn supports_read(&self) -> bool {
        matches!(self, Self::Read | Self::ReadWrite | Self::Admin)
    }

    pub fn supports_write(&self) -> bool {
        matches!(self, Self::Write | Self::ReadWrite | Self::Admin)
    }
}

/// Generic named backend instance.
///
/// This is intentionally backend-agnostic: the same structure can describe
/// PostgreSQL primaries/replicas, Qdrant clusters, MinIO/S3 buckets endpoints,
/// MongoDB databases, Neo4j graphs, and ClickHouse analytics stores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendInstance {
    /// Stable instance name within a backend, for example `primary`,
    /// `replica_1`, `vector_a`, or `object_eu`.
    pub name: String,
    /// Canonical backend name, for example `postgres`, `redis`, `qdrant`,
    /// `minio`, `s3`, `mongodb`, `neo4j`, or `clickhouse`.
    pub backend: String,
    /// Runtime routing role for the instance.
    #[serde(default)]
    pub role: BackendInstanceRole,
    /// Inline DSN or endpoint. Prefer `dsn_env` for production secrets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn: Option<String>,
    /// Environment variable that supplies the DSN or endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dsn_env: Option<String>,
    /// Whether this instance participates in routing and admin commands.
    #[serde(default = "backend_instance_default_enabled")]
    pub enabled: bool,
    /// Relative read routing weight. Zero disables read routing.
    #[serde(default = "backend_instance_default_weight")]
    pub read_weight: u32,
    /// Relative write routing weight. Zero disables write routing.
    #[serde(default = "backend_instance_default_weight")]
    pub write_weight: u32,
    /// Free-form labels for routing policy, ownership, region, service, etc.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Optional capability overrides/additions advertised by this instance.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
}

fn backend_instance_default_enabled() -> bool {
    true
}

fn backend_instance_default_weight() -> u32 {
    1
}

impl Default for BackendInstance {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            backend: "postgres".to_string(),
            role: BackendInstanceRole::ReadWrite,
            dsn: None,
            dsn_env: Some("UDB_PG_DSN".to_string()),
            enabled: true,
            read_weight: 1,
            write_weight: 1,
            labels: BTreeMap::new(),
            capabilities: BTreeSet::new(),
        }
    }
}

impl BackendInstance {
    pub fn canonical_backend(&self) -> Option<crate::planning::backend::BackendKind> {
        crate::planning::backend::BackendKind::from_store_kind("", &self.backend)
    }

    pub fn routing_key(&self) -> String {
        let backend = self
            .canonical_backend()
            .map(|kind| kind.as_str().to_string())
            .unwrap_or_else(|| self.backend.to_ascii_lowercase());
        format!("{backend}:{}", self.name)
    }

    pub fn resolve_dsn(&self) -> Option<String> {
        self.dsn
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                self.dsn_env
                    .as_ref()
                    .and_then(|key| std::env::var(key).ok())
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
    }

    pub fn is_configured(&self) -> bool {
        self.resolve_dsn().is_some()
    }

    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.name.trim().is_empty() {
            errors.push("backend instance name cannot be empty".to_string());
        }
        match self.canonical_backend() {
            Some(kind) => {
                let state = crate::backend::support_state_for_kind(&kind);
                if !state.is_runtime_supported() {
                    errors.push(format!(
                        "backend instance '{}' uses backend '{}': {}",
                        self.name,
                        self.backend,
                        state.diagnostic(kind.as_str())
                    ));
                }
            }
            None => {
                errors.push(format!(
                    "backend instance '{}' uses unsupported backend '{}'",
                    self.name, self.backend
                ));
            }
        }
        if self.enabled && self.dsn.is_none() && self.dsn_env.is_none() {
            errors.push(format!(
                "backend instance '{}' must define dsn or dsn_env",
                self.routing_key()
            ));
        }
        if let Some(dsn) = &self.dsn
            && !dsn.trim().is_empty()
            && !looks_like_dsn(dsn)
        {
            errors.push(format!(
                "backend instance '{}' dsn does not look valid: {}",
                self.routing_key(),
                dsn
            ));
        }
        if self.enabled
            && matches!(
                self.canonical_backend(),
                Some(crate::planning::backend::BackendKind::Mongodb)
            )
        {
            let transport = self
                .labels
                .get("transport")
                .map(|value| value.trim().to_ascii_lowercase());
            let wants_native = transport.as_deref() == Some("native")
                || self.dsn.as_ref().is_some_and(|dsn| {
                    let dsn = dsn.trim().to_ascii_lowercase();
                    dsn.starts_with("mongodb://") || dsn.starts_with("mongodb+srv://")
                });
            let has_api_base = self
                .labels
                .get("api_base")
                .is_some_and(|value| !value.trim().is_empty())
                || self
                    .labels
                    .get("api_url")
                    .is_some_and(|value| !value.trim().is_empty())
                || self
                    .labels
                    .get("api_base_env")
                    .is_some_and(|value| !value.trim().is_empty())
                || self
                    .labels
                    .get("api_url_env")
                    .is_some_and(|value| !value.trim().is_empty());
            if !has_api_base && !wants_native {
                errors.push(format!(
                    "backend instance '{}' uses mongodb Data API transport but does not define labels.api_url/api_base or labels.api_url_env/api_base_env",
                    self.routing_key()
                ));
            }
            #[cfg(not(feature = "mongodb-native"))]
            {
                if let Some(dsn) = &self.dsn {
                    let dsn = dsn.trim().to_ascii_lowercase();
                    if dsn.starts_with("mongodb+srv://") {
                        errors.push(format!(
                            "backend instance '{}' uses mongodb+srv:// but native MongoDB wire protocol is not supported in this build",
                            self.routing_key()
                        ));
                    } else if dsn.starts_with("mongodb://") && !has_api_base {
                        errors.push(format!(
                            "backend instance '{}' uses a native MongoDB DSN without a Data API URL; native MongoDB wire protocol is not supported in this build",
                            self.routing_key()
                        ));
                    }
                }
                if transport.as_deref() == Some("native") {
                    errors.push(format!(
                        "backend instance '{}' requests native MongoDB transport but this build does not enable the mongodb-native feature",
                        self.routing_key()
                    ));
                }
            }
            #[cfg(feature = "mongodb-native")]
            if wants_native && self.resolve_dsn().is_none() {
                errors.push(format!(
                    "backend instance '{}' requests native MongoDB transport but does not define a mongodb:// or mongodb+srv:// dsn/dsn_env",
                    self.routing_key()
                ));
            }
        }
        if let Some(kind) = self.canonical_backend() {
            let cap = kind.capabilities();
            if self.role.supports_read() && self.capabilities.contains("write_only") {
                errors.push(format!(
                    "backend instance '{}' is read-routable but declares write_only",
                    self.routing_key()
                ));
            }
            if self.role.supports_write()
                && matches!(
                    kind,
                    crate::planning::backend::BackendKind::Clickhouse
                        | crate::planning::backend::BackendKind::S3
                )
                && !cap.supports_transactions
                && self.capabilities.contains("transaction_required")
            {
                errors.push(format!(
                    "backend instance '{}' declares unsupported transactional writes",
                    self.routing_key()
                ));
            }
        }
        if self.enabled
            && self.role.supports_read()
            && self.read_weight == 0
            && !matches!(self.role, BackendInstanceRole::Write)
        {
            errors.push(format!(
                "backend instance '{}' has read-capable role but read_weight=0",
                self.routing_key()
            ));
        }
        if self.enabled
            && self.role.supports_write()
            && self.write_weight == 0
            && !matches!(self.role, BackendInstanceRole::Read)
        {
            errors.push(format!(
                "backend instance '{}' has write-capable role but write_weight=0",
                self.routing_key()
            ));
        }
        errors
    }
}

/// Portable named backend instance config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BackendInstanceConfig {
    #[serde(default)]
    pub instances: Vec<BackendInstance>,
}

impl BackendInstanceConfig {
    /// Build a generic instance catalog from environment variables.
    ///
    /// `UDB_BACKEND_INSTANCES` accepts comma-separated descriptors:
    /// `backend:name[:role]`, for example
    /// `postgres:primary:read_write,postgres:replica:read,qdrant:default`.
    ///
    /// Without this variable, UDB derives a backward-compatible catalog from
    /// conventional DSN env vars such as `UDB_PG_DSN`, `UDB_REDIS_DSN`,
    /// `UDB_QDRANT_URL`, and `UDB_MINIO_ENDPOINT`.
    pub fn from_env() -> Self {
        let raw = std::env::var("UDB_BACKEND_INSTANCES").unwrap_or_default();
        if raw.trim().is_empty() {
            return Self::from_legacy_env();
        }

        let instances = raw
            .split(',')
            .filter_map(|descriptor| Self::instance_from_descriptor(descriptor.trim()))
            .collect();
        Self { instances }
    }

    fn from_legacy_env() -> Self {
        let pg_dsn_env = if std::env::var("UDB_PG_DSN")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
        {
            "UDB_PG_DSN"
        } else {
            "DATABASE_URL"
        };
        let mut instances = Vec::new();
        push_legacy_instance(
            &mut instances,
            "postgres",
            "primary",
            pg_dsn_env,
            BackendInstanceRole::ReadWrite,
            &[],
        );
        push_legacy_instance(
            &mut instances,
            "redis",
            "default",
            "UDB_REDIS_DSN",
            BackendInstanceRole::ReadWrite,
            &[],
        );
        push_legacy_instance(
            &mut instances,
            "qdrant",
            "default",
            "UDB_QDRANT_URL",
            BackendInstanceRole::ReadWrite,
            &[("api_key", "UDB_QDRANT_API_KEY")],
        );
        push_legacy_instance(
            &mut instances,
            "minio",
            "default",
            "UDB_MINIO_ENDPOINT",
            BackendInstanceRole::ReadWrite,
            &[
                ("access_key", "UDB_MINIO_ACCESS_KEY"),
                ("secret_key", "UDB_MINIO_SECRET_KEY"),
                ("region", "UDB_MINIO_REGION"),
            ],
        );
        push_legacy_instance(
            &mut instances,
            "mongodb",
            "default",
            "UDB_NOSQL_DSN",
            BackendInstanceRole::ReadWrite,
            &[
                ("api_url", "UDB_NOSQL_API_URL"),
                ("api_key", "UDB_NOSQL_API_KEY"),
                ("database", "UDB_NOSQL_DATABASE"),
                ("deploy_mode", "UDB_MONGO_DEPLOY_MODE"),
                ("timeout_secs", "UDB_NOSQL_TIMEOUT_SECS"),
                ("dev_mode", "UDB_DEV_MODE"),
            ],
        );
        push_legacy_instance(
            &mut instances,
            "neo4j",
            "default",
            "UDB_GRAPH_DSN",
            BackendInstanceRole::ReadWrite,
            &[
                ("http_url", "UDB_GRAPH_HTTP_URL"),
                ("username", "UDB_GRAPH_USER"),
                ("password", "UDB_GRAPH_PASSWORD"),
                ("database", "UDB_GRAPH_DATABASE"),
                ("deploy_mode", "UDB_NEO4J_DEPLOY_MODE"),
                ("timeout_secs", "UDB_GRAPH_TIMEOUT_SECS"),
                ("dev_mode", "UDB_DEV_MODE"),
            ],
        );
        push_legacy_instance(
            &mut instances,
            "clickhouse",
            "default",
            "UDB_COLUMN_DSN",
            BackendInstanceRole::Read,
            &[
                ("http_url", "UDB_COLUMN_HTTP_URL"),
                ("username", "UDB_COLUMN_USER"),
                ("password", "UDB_COLUMN_PASSWORD"),
                ("database", "UDB_COLUMN_DATABASE"),
                ("deploy_mode", "UDB_CH_DEPLOY_MODE"),
                ("connect_timeout_secs", "UDB_CH_CONNECT_TIMEOUT_SECS"),
                ("query_timeout_secs", "UDB_CH_QUERY_TIMEOUT_SECS"),
            ],
        );
        push_legacy_instance(
            &mut instances,
            "mssql",
            "primary",
            "UDB_MSSQL_DSN",
            BackendInstanceRole::ReadWrite,
            &[],
        );
        push_legacy_instance(
            &mut instances,
            "cassandra",
            "primary",
            "UDB_CASSANDRA_DSN",
            BackendInstanceRole::ReadWrite,
            &[],
        );
        Self { instances }
    }

    fn instance_from_descriptor(descriptor: &str) -> Option<BackendInstance> {
        if descriptor.is_empty() {
            return None;
        }
        let mut parts = descriptor.split(':').map(str::trim);
        let backend = parts.next()?.to_ascii_lowercase();
        let name = parts.next().unwrap_or("default").to_string();
        let role = match parts.next().unwrap_or("read_write") {
            "read" | "reader" | "replica" => BackendInstanceRole::Read,
            "write" | "writer" | "primary" => BackendInstanceRole::Write,
            "admin" => BackendInstanceRole::Admin,
            _ => BackendInstanceRole::ReadWrite,
        };
        let upper_backend = backend_env_segment(&backend);
        let upper_name = env_segment(&name);
        Some(BackendInstance {
            backend,
            name,
            role,
            dsn: None,
            dsn_env: Some(format!("UDB_{upper_backend}_DSN_{upper_name}")),
            enabled: true,
            read_weight: 1,
            write_weight: 1,
            labels: BTreeMap::new(),
            capabilities: BTreeSet::new(),
        })
    }

    pub fn active(&self) -> impl Iterator<Item = &BackendInstance> {
        self.instances.iter().filter(|instance| instance.enabled)
    }

    pub fn resolve_env_dsns(&mut self) {
        for instance in &mut self.instances {
            if instance
                .dsn
                .as_ref()
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
            {
                continue;
            }
            let Some(env_key) = instance.dsn_env.as_ref() else {
                continue;
            };
            if let Ok(value) = std::env::var(env_key) {
                let value = value.trim().to_string();
                if !value.is_empty() {
                    instance.dsn = Some(value);
                }
            }
        }
    }

    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        let mut seen = BTreeSet::new();
        for instance in &self.instances {
            errors.extend(instance.validate());
            let key = instance.routing_key();
            if !seen.insert(key.clone()) {
                errors.push(format!("duplicate backend instance '{key}'"));
            }
        }
        errors
    }
}

fn push_legacy_instance(
    instances: &mut Vec<BackendInstance>,
    backend: &str,
    name: &str,
    dsn_env: &str,
    role: BackendInstanceRole,
    label_envs: &[(&str, &str)],
) {
    let Some(dsn) = std::env::var(dsn_env)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let labels = label_envs
        .iter()
        .filter_map(|(label, env_key)| {
            std::env::var(env_key)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(|value| ((*label).to_string(), value))
        })
        .collect();
    instances.push(BackendInstance {
        backend: backend.to_string(),
        name: name.to_string(),
        role,
        dsn: Some(dsn),
        dsn_env: None,
        enabled: true,
        read_weight: 1,
        write_weight: 1,
        labels,
        capabilities: BTreeSet::new(),
    });
}

fn env_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn backend_env_segment(backend: &str) -> String {
    match backend {
        "postgres" | "postgresql" | "pg" => "PG".to_string(),
        "qdrant" => "QDRANT".to_string(),
        "minio" | "s3" => "MINIO".to_string(),
        "mongodb" | "mongo" => "NOSQL".to_string(),
        "neo4j" => "GRAPH".to_string(),
        "clickhouse" => "COLUMN".to_string(),
        "mssql" | "sqlserver" => "MSSQL".to_string(),
        "cassandra" | "scylla" => "CASSANDRA".to_string(),
        other => env_segment(other),
    }
}

// ── Multi-instance PostgreSQL configuration ───────────────────────────────────

/// A named PostgreSQL backend instance (primary, backup, read-replica, …).
///
/// Instances are discovered from `UDB_PG_INSTANCES` and each instance's DSN
/// is read from `UDB_PG_DSN_<UPPERCASE_NAME>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgInstance {
    /// Short identifier: `"primary"`, `"backup"`, `"analytics"`, …
    pub name: String,
    /// Environment variable that supplies this instance's DSN.
    /// Default convention: `UDB_PG_DSN_<UPPERCASE_NAME>`.
    pub dsn_env: String,
    /// Whether this instance participates in batch commands.
    #[serde(default = "pg_instance_default_enabled")]
    pub enabled: bool,
    /// Optional human-readable label for logs and reports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

fn pg_instance_default_enabled() -> bool {
    true
}

impl PgInstance {
    /// Resolve the DSN for this instance from the environment.
    pub fn resolve_dsn(&self) -> Option<String> {
        std::env::var(&self.dsn_env).ok().filter(|v| !v.is_empty())
    }

    /// Display label: uses `label` if set, otherwise falls back to `name`.
    pub fn display_label(&self) -> &str {
        self.label.as_deref().unwrap_or(&self.name)
    }
}

/// Multi-instance PostgreSQL configuration for batch UDB operations.
///
/// Loaded from environment variables:
///   `UDB_PG_INSTANCES=primary,backup`    — comma-separated instance names
///   `UDB_PG_DSN_PRIMARY=postgresql://…`   — DSN for the "primary" instance
///   `UDB_PG_DSN_BACKUP=postgresql://…`    — DSN for the "backup" instance
///   `UDB_PG_PRIMARY_ENABLED=false`        — opt-out a specific instance
///
/// When `UDB_PG_INSTANCES` is not set, falls back to a single "primary"
/// instance reading `UDB_PG_DSN` / `DATABASE_URL` (backward-compatible).
///
/// Used by CLI commands (`admin force-sync`, `admin dry-run`) to apply
/// operations against all configured PostgreSQL databases in sequence.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiPgConfig {
    pub instances: Vec<PgInstance>,
}

impl MultiPgConfig {
    /// Build from environment variables.
    pub fn from_env() -> Self {
        let raw = std::env::var("UDB_PG_INSTANCES").unwrap_or_default();
        if raw.trim().is_empty() {
            // Backward-compat: single primary instance from UDB_PG_DSN / DATABASE_URL.
            return Self {
                instances: vec![PgInstance {
                    name: "primary".to_string(),
                    dsn_env: "UDB_PG_DSN".to_string(),
                    enabled: true,
                    label: Some("Primary (default)".to_string()),
                }],
            };
        }

        let instances = raw
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|name| {
                let upper = name.to_ascii_uppercase();
                let enabled = std::env::var(format!("UDB_PG_{upper}_ENABLED"))
                    .map(|v| !matches!(v.to_ascii_lowercase().trim(), "0" | "false" | "no" | "off"))
                    .unwrap_or(true);
                PgInstance {
                    name: name.to_string(),
                    dsn_env: format!("UDB_PG_DSN_{upper}"),
                    enabled,
                    label: std::env::var(format!("UDB_PG_{upper}_LABEL")).ok(),
                }
            })
            .collect();

        Self { instances }
    }

    /// Return only enabled instances that have a resolvable DSN.
    pub fn active(&self) -> Vec<&PgInstance> {
        self.instances
            .iter()
            .filter(|i| i.enabled && i.resolve_dsn().is_some())
            .collect()
    }

    /// Return a summary string for logging: `"primary, backup (2 active)"`.
    pub fn active_summary(&self) -> String {
        let active = self.active();
        let names: Vec<&str> = active.iter().map(|i| i.name.as_str()).collect();
        format!("{} ({} active)", names.join(", "), active.len())
    }
}

#[cfg(test)]
mod b4_target_routing_tests {
    use super::*;

    #[test]
    fn mssql_and_cassandra_instances_map_to_canonical_kinds() {
        for (backend, kind) in [
            ("mssql", crate::backend::BackendKind::Mssql),
            ("sqlserver", crate::backend::BackendKind::Mssql),
            ("cassandra", crate::backend::BackendKind::Cassandra),
            ("scylla", crate::backend::BackendKind::Cassandra),
        ] {
            let inst = BackendInstance {
                backend: backend.to_string(),
                ..BackendInstance::default()
            };
            assert_eq!(inst.canonical_backend(), Some(kind), "backend {backend}");
        }
    }

    #[test]
    fn descriptor_env_keys_for_target_backends_are_conventional() {
        assert_eq!(backend_env_segment("mssql"), "MSSQL");
        assert_eq!(backend_env_segment("cassandra"), "CASSANDRA");
    }
}
