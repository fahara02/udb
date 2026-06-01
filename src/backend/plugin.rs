//! Per-backend plugin trait — the U2 seam.
//!
//! `Backend` is the single, object-safe trait that a backend module implements
//! to declare its identity, capabilities, and (in later steps) its generation
//! and executor factories. The `all_plugins()` registry is the **one** place
//! where backends are enumerated; everything else iterates that list.
//!
//! Per §9.1 the executor returned by a plugin must be a *stateless leaf I/O*
//! adapter — replica routing, cache, encryption, channels, and circuit breakers
//! stay in `DataBrokerRuntime` orchestration, not in this trait.
//!
//! ## Object safety
//!
//! All trait methods are `&self` and return owned values or sized types, so
//! `&dyn Backend` and `Box<dyn Backend>` both work. The plugin structs in
//! `backend::plugins::*` are zero-sized, so each has a `pub static` instance
//! and the registry is a slice of `'static` references.
//!
//! The trait currently covers identity + capability + DSN scheme methods, with
//! defaults that delegate to `BackendKind`. Generation and executor hooks can
//! be added behind this same seam when those call sites are folded into the
//! plugin inventory.

use crate::backend::{BackendCapability, BackendCapabilityMatrixEntry, BackendKind};
use crate::runtime::config::{BackendInstanceConfig, UdbConfig};
use crate::runtime::core::{DataBrokerRuntime, RuntimeInitReport};
use serde::{Deserialize, Serialize};

/// Runtime implementation state for a known backend token.
///
/// `BackendKind` is intentionally broader than the current runtime. This type
/// keeps operators from mistaking "UDB knows the name" for "this binary can
/// execute it".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendSupportState {
    /// The token does not map to any known backend.
    Unknown,
    /// UDB has metadata for this backend, but no runtime implementation yet.
    KnownUnsupported,
    /// UDB has a runtime implementation, but this binary was built without it.
    DisabledByFeature,
    /// This binary includes the runtime plugin for the backend.
    RuntimeSupported,
}

impl BackendSupportState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::KnownUnsupported => "known_unsupported",
            Self::DisabledByFeature => "disabled_by_feature",
            Self::RuntimeSupported => "runtime_supported",
        }
    }

    pub fn is_runtime_supported(self) -> bool {
        matches!(self, Self::RuntimeSupported)
    }

    pub fn diagnostic(self, backend: &str) -> String {
        match self {
            Self::Unknown => format!("backend '{backend}' is not known to UDB"),
            Self::KnownUnsupported => format!(
                "backend '{backend}' is known to UDB metadata but has no runtime executor in this version"
            ),
            Self::DisabledByFeature => format!(
                "backend '{backend}' is not available in this binary; rebuild with the matching Cargo feature"
            ),
            Self::RuntimeSupported => format!("backend '{backend}' is runtime-supported"),
        }
    }
}

/// One stable surface a backend plugin must provide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendPluginSurface {
    pub name: String,
    pub description: String,
    pub required: bool,
}

impl BackendPluginSurface {
    pub fn required(name: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            required: true,
        }
    }
}

/// Machine-readable contract for adding or validating a backend plugin.
///
/// This is deliberately higher-level than the Rust trait methods. It describes
/// the public surfaces a plugin owns so external plugin crates can conform
/// without learning `runtime/core` internals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendPluginContract {
    pub backend: String,
    pub tier: String,
    pub config_schema: BackendPluginSurface,
    pub connection_factory: BackendPluginSurface,
    pub logical_ir_compiler: BackendPluginSurface,
    pub executor: BackendPluginSurface,
    pub migration_resource_applier: BackendPluginSurface,
    pub health_probe: BackendPluginSurface,
    pub metrics_labels: Vec<String>,
    pub capability_matrix: BackendCapabilityMatrixEntry,
}

impl BackendPluginContract {
    pub fn default_for(kind: BackendKind) -> Self {
        Self {
            backend: kind.as_str().to_string(),
            tier: kind.tier().as_str().to_string(),
            config_schema: BackendPluginSurface::required(
                "BackendInstanceConfig",
                "Declarative config schema for DSN, labels, pool limits, routing, and feature gates",
            ),
            connection_factory: BackendPluginSurface::required(
                "Backend::register",
                "Startup-time connection/client factory owned by the plugin",
            ),
            logical_ir_compiler: BackendPluginSurface::required(
                "ir::compile",
                "Compiler from neutral logical operations into backend wire requests",
            ),
            executor: BackendPluginSurface::required(
                "runtime::executors::DispatchFactory",
                "Runtime executor for generic query/mutate/search/object/resource operations",
            ),
            migration_resource_applier: BackendPluginSurface::required(
                "Backend::generate_artifacts",
                "Migration/resource artifact generator and applier ownership point",
            ),
            health_probe: BackendPluginSurface::required(
                "probe",
                "Backend-specific health probe used by startup, reload, and admin APIs",
            ),
            metrics_labels: vec![
                "project".to_string(),
                "backend".to_string(),
                "tier".to_string(),
                "instance".to_string(),
                "operation".to_string(),
            ],
            capability_matrix: kind.capability_matrix_entry(),
        }
    }

    pub fn conformance_report(&self) -> BackendConformanceReport {
        let mut failures = Vec::new();
        let required_surfaces = [
            &self.config_schema,
            &self.connection_factory,
            &self.logical_ir_compiler,
            &self.executor,
            &self.migration_resource_applier,
            &self.health_probe,
        ];
        for surface in required_surfaces {
            if surface.required
                && (surface.name.trim().is_empty() || surface.description.trim().is_empty())
            {
                failures.push(format!("required surface '{}' is incomplete", surface.name));
            }
        }
        for label in ["backend", "tier", "instance", "operation"] {
            if !self.metrics_labels.iter().any(|item| item == label) {
                failures.push(format!("missing required metrics label '{label}'"));
            }
        }
        if self.capability_matrix.backend != self.backend {
            failures.push("capability matrix backend does not match contract backend".to_string());
        }
        if self.capability_matrix.tier != self.tier {
            failures.push("capability matrix tier does not match contract tier".to_string());
        }
        if self.capability_matrix.operations.is_empty() {
            failures.push("capability matrix must declare at least one operation".to_string());
        }
        BackendConformanceReport {
            backend: self.backend.clone(),
            passed: failures.is_empty(),
            failures,
        }
    }
}

/// Result of validating one plugin against the stable contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendConformanceReport {
    pub backend: String,
    pub passed: bool,
    pub failures: Vec<String>,
}

/// Backends that have a runtime implementation in this source tree.
///
/// Feature gates decide whether each implementation is compiled into a given
/// binary; unsupported-but-known metadata-only backends stay out of this set.
pub fn has_runtime_implementation(kind: &BackendKind) -> bool {
    matches!(
        kind,
        BackendKind::Postgres
            // P2P: MySQL + SQLite are now first-class canonical stores
            // (via sqlx-mysql and sqlx-sqlite). See
            // `runtime/canonical_store/{mysql,sqlite}.rs`.
            | BackendKind::Mysql
            | BackendKind::Sqlite
            | BackendKind::Redis
            | BackendKind::Qdrant
            | BackendKind::Minio
            | BackendKind::S3
            | BackendKind::Mongodb
            | BackendKind::Neo4j
            | BackendKind::Clickhouse
            // These all have runtime executors under `runtime/executors/`; they
            // were missing here, so feature-disabled builds wrongly reported
            // KnownUnsupported (a hard lint error) instead of DisabledByFeature.
            | BackendKind::Mssql
            | BackendKind::Memcached
            | BackendKind::Elasticsearch
            | BackendKind::Weaviate
            | BackendKind::Pinecone
            | BackendKind::Cassandra
            | BackendKind::AzureBlob
            | BackendKind::Gcs
    )
}

/// Implementation state for a parsed backend kind.
pub fn support_state_for_kind(kind: &BackendKind) -> BackendSupportState {
    if all_plugins()
        .into_iter()
        .any(|plugin| plugin.kind() == *kind)
    {
        BackendSupportState::RuntimeSupported
    } else if has_runtime_implementation(kind) {
        BackendSupportState::DisabledByFeature
    } else {
        BackendSupportState::KnownUnsupported
    }
}

/// Implementation state for a backend token or alias.
pub fn support_state_for_token(token: &str) -> BackendSupportState {
    let Some(kind) =
        BackendKind::from_store_kind("", token).or_else(|| BackendKind::from_token(token))
    else {
        return BackendSupportState::Unknown;
    };
    support_state_for_kind(&kind)
}

/// Mutable context passed to [`Backend::register`].
///
/// Holds borrows of the partially-built runtime/report being assembled by
/// `DataBrokerRuntime::from_config` so each plugin can wire its own pools,
/// instance maps, and report flags without that function knowing the backend
/// list.
pub struct RegisterCtx<'a> {
    /// User-supplied configuration (read-only).
    pub config: &'a UdbConfig,
    /// Derived per-instance config (read-only) — labels, DSNs, etc.
    pub instance_config: &'a BackendInstanceConfig,
    /// Effective application name used in DSN labels.
    pub app_name: &'a str,
    /// The runtime being built. Plugins write their executor/client into this.
    pub runtime: &'a mut DataBrokerRuntime,
    /// The init report being built. Plugins set their `*_configured` flag and
    /// push any warnings encountered during setup.
    pub report: &'a mut RuntimeInitReport,
}

/// A plugin module describing one backend.
///
/// Implementors are typically zero-sized structs with one `pub static`
/// instance per backend (see `backend::plugins`).
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    /// The canonical identity of this backend.
    ///
    /// All other defaulted methods derive their value from this; an override
    /// is only needed if a plugin wants to deviate from the enum's metadata.
    fn kind(&self) -> BackendKind;

    /// Whether this backend is compiled into the current build.
    ///
    /// Defaulted to `true`. Plugins for feature-gated backends (s3, redis,
    /// kafka, …) override this by sitting behind `#[cfg(feature = "…")]`
    /// themselves; the slim build simply doesn't include them in
    /// [`all_plugins`].
    fn enabled(&self) -> bool {
        true
    }

    /// Capability flags (delegates to `BackendKind`).
    fn capabilities(&self) -> BackendCapability {
        self.kind().capabilities()
    }

    /// `udb+<tier>+<backend>` URI scheme prefix (delegates to `BackendKind`).
    fn dsn_scheme(&self) -> String {
        self.kind().dsn_scheme()
    }

    /// Default environment variable holding the connection DSN
    /// (delegates to `BackendKind`).
    fn default_dsn_env(&self) -> &'static str {
        self.kind().default_env_key()
    }

    /// Stable plugin contract advertised by this backend.
    fn contract(&self) -> BackendPluginContract {
        BackendPluginContract::default_for(self.kind())
    }

    /// Validate this plugin's contract.
    fn conformance_report(&self) -> BackendConformanceReport {
        self.contract().conformance_report()
    }

    /// Wire this backend into the runtime under construction.
    ///
    /// Default: no-op. Plugins override this to move their per-backend setup
    /// block out of `DataBrokerRuntime::from_config` (pool creation, HTTP
    /// client, instance map, `*_configured` flag, any warnings). Per §9.1 this
    /// is the *connection* layer only — replica routing, channels, breakers,
    /// cache, and encryption stay in orchestration.
    async fn register(&self, _ctx: &mut RegisterCtx<'_>) {}

    /// Generate the migration artifacts this backend ships for the given AST.
    ///
    /// Default: empty. Plugins for backends with a generator
    /// (`generation::<backend>::generate_<backend>_artifacts`) override this to
    /// delegate. `BackendKind::Postgres` keeps the default — its bootstrap SQL
    /// is produced via the separate `generate_bootstrap_sql` path because it
    /// also takes a `CatalogManifest`.
    fn generate_artifacts(
        &self,
        _manifest: &crate::generation::CatalogManifest,
        _sql_config: &crate::generation::sql::SqlGenerationConfig,
    ) -> Result<Vec<crate::generation::GeneratedArtifact>, String> {
        Ok(Vec::new())
    }

    /// Subdirectory name under `db_ops/` where this backend's artifacts are
    /// written by `sync_all_backends`. Defaults to the canonical token; only
    /// override when the directory layout deviates (none today).
    fn sync_subdir(&self) -> &'static str {
        self.kind().as_str()
    }
}

/// All compiled-in backend plugins, in canonical order.
///
/// Cargo features that drop a backend (`s3`, `redis`, `kafka`, …) also drop
/// its plugin via `#[cfg]` in `backend::plugins::mod`. This is the **single**
/// enumeration point: registry build, generation dispatch, and resolver lookup
/// all iterate this list rather than matching on `BackendKind` themselves.
pub fn all_plugins() -> Vec<&'static dyn Backend> {
    crate::backend::plugins::all()
}

/// Find a plugin by canonical token (alias of `BackendKind::from_token`).
///
/// Returns `None` for unknown tokens or for backends whose feature is disabled.
pub fn plugin_for(token: &str) -> Option<&'static dyn Backend> {
    let kind = BackendKind::from_token(token)?;
    all_plugins().into_iter().find(|p| p.kind() == kind)
}

/// Find a compiled runtime plugin by parsed backend kind.
pub fn plugin_for_kind(kind: &BackendKind) -> Option<&'static dyn Backend> {
    all_plugins().into_iter().find(|p| p.kind() == *kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected_runtime_plugin_kinds() -> Vec<BackendKind> {
        vec![
            BackendKind::Postgres,
            // NW3-1 + NW3-2: MySQL + SQLite sit at the SQL tier
            // next to Postgres.
            #[cfg(feature = "mysql")]
            BackendKind::Mysql,
            #[cfg(feature = "sqlite")]
            BackendKind::Sqlite,
            #[cfg(feature = "redis")]
            BackendKind::Redis,
            #[cfg(feature = "qdrant")]
            BackendKind::Qdrant,
            #[cfg(feature = "s3")]
            BackendKind::Minio,
            #[cfg(feature = "s3")]
            BackendKind::S3,
            #[cfg(feature = "mongodb")]
            BackendKind::Mongodb,
            #[cfg(feature = "neo4j")]
            BackendKind::Neo4j,
            #[cfg(feature = "clickhouse")]
            BackendKind::Clickhouse,
            // C9: Elasticsearch promoted from metadata-only.
            #[cfg(feature = "elasticsearch")]
            BackendKind::Elasticsearch,
            // C9: Memcached promoted from metadata-only via the
            // `memcache` crate.
            #[cfg(feature = "memcached")]
            BackendKind::Memcached,
            // C9: SQL Server promoted via the tiberius driver.
            #[cfg(feature = "mssql")]
            BackendKind::Mssql,
            // C9: Weaviate (REST + GraphQL via reqwest).
            #[cfg(feature = "weaviate")]
            BackendKind::Weaviate,
            // C9: Pinecone (REST via reqwest).
            #[cfg(feature = "pinecone")]
            BackendKind::Pinecone,
            // C9: Cassandra / ScyllaDB via the `scylla` driver.
            #[cfg(feature = "cassandra")]
            BackendKind::Cassandra,
            // C9: Azure Blob Storage via the Azure SDK.
            #[cfg(feature = "azureblob")]
            BackendKind::AzureBlob,
            // C9: Google Cloud Storage via the google-cloud-storage crate.
            #[cfg(feature = "gcs")]
            BackendKind::Gcs,
        ]
    }

    #[test]
    fn registry_is_non_empty_and_contains_postgres() {
        let plugins = all_plugins();
        assert!(!plugins.is_empty(), "registry must not be empty");
        assert!(
            plugins.iter().any(|p| p.kind() == BackendKind::Postgres),
            "postgres plugin must always be registered"
        );
    }

    #[test]
    fn registry_matches_compiled_runtime_backends() {
        let actual: Vec<_> = all_plugins().into_iter().map(|p| p.kind()).collect();
        assert_eq!(actual, expected_runtime_plugin_kinds());
    }

    #[test]
    fn plugin_metadata_matches_backend_kind() {
        // Defaulted methods must agree with the underlying BackendKind for
        // every registered plugin — anything else would split the source of
        // truth in two.
        for plugin in all_plugins() {
            let kind = plugin.kind();
            assert_eq!(plugin.dsn_scheme(), kind.dsn_scheme(), "{kind:?}");
            assert_eq!(plugin.default_dsn_env(), kind.default_env_key(), "{kind:?}");
            assert_eq!(plugin.capabilities(), kind.capabilities(), "{kind:?}");
            assert_eq!(plugin.contract().backend, kind.as_str(), "{kind:?}");
        }
    }

    #[test]
    fn registered_plugins_satisfy_stable_contract() {
        for plugin in all_plugins() {
            let report = plugin.conformance_report();
            assert!(
                report.passed,
                "{} plugin contract failed: {:?}",
                report.backend, report.failures
            );
        }
    }

    struct ToyBackend;

    #[async_trait::async_trait]
    impl Backend for ToyBackend {
        fn kind(&self) -> BackendKind {
            BackendKind::Sqlite
        }
    }

    #[test]
    fn toy_backend_can_conform_without_runtime_core_edits() {
        let plugin = ToyBackend;
        let contract = plugin.contract();
        assert_eq!(contract.backend, "sqlite");
        assert!(plugin.conformance_report().passed);
        assert!(
            contract
                .capability_matrix
                .operations
                .contains(&"ping".to_string())
        );
    }

    #[test]
    fn plugin_for_round_trips_through_canonical_token() {
        for plugin in all_plugins() {
            let token = plugin.kind().as_str();
            let resolved = plugin_for(token).unwrap_or_else(|| {
                panic!("plugin_for({token}) returned None for registered plugin")
            });
            assert_eq!(resolved.kind(), plugin.kind());
        }
        assert!(plugin_for("not_a_backend").is_none());
    }

    #[test]
    fn support_state_distinguishes_metadata_from_runtime() {
        assert_eq!(
            support_state_for_kind(&BackendKind::Postgres),
            BackendSupportState::RuntimeSupported
        );
        // C9: MSSQL, Memcached, Elasticsearch are now RuntimeSupported
        // (plugin entries shipped). The KnownUnsupported anchor is
        // one of the remaining metadata-only backends.
        #[cfg(feature = "mysql")]
        assert_eq!(
            support_state_for_kind(&BackendKind::Mysql),
            BackendSupportState::RuntimeSupported
        );
        #[cfg(feature = "sqlite")]
        assert_eq!(
            support_state_for_kind(&BackendKind::Sqlite),
            BackendSupportState::RuntimeSupported
        );
        #[cfg(feature = "mssql")]
        assert_eq!(
            support_state_for_kind(&BackendKind::Mssql),
            BackendSupportState::RuntimeSupported
        );
        // C9 complete: every BackendKind is wired. There's no
        // KnownUnsupported in-tree anchor anymore — verify via
        // token lookup that unknown strings still map correctly.
        #[cfg(feature = "cassandra")]
        assert_eq!(
            support_state_for_kind(&BackendKind::Cassandra),
            BackendSupportState::RuntimeSupported
        );
        assert_eq!(
            support_state_for_token("not_a_backend"),
            BackendSupportState::Unknown
        );
        assert!(support_state_for_token("postgres").is_runtime_supported());
    }
}
