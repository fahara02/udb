//! db_ops_sync — Align db_ops/{backend}/migrations with the proto AST across all backends.
//!
//! Inspired by the legacy_sql `legacycore/db` migration pattern. This module:
//!
//! 1. Discovers the `db_ops` root at **sibling level** relative to the UDB crate
//!    root (nearest `Cargo.toml`).  Override with `UDB_DB_OPS_ROOT` env var.
//!
//! 2. Creates `db_ops/<backend>/migrations` and `db_ops/<backend>/bootstrap` for
//!    each backend selected.  Also creates the configured seeders directory
//!    (`UDB_SEEDERS_PATH` / `UDB_SEEDER_PATH`, default: `db_ops/seeds`) with
//!    a `.gitkeep`.
//!
//! 3. If `<backend>/migrations` is **empty** → writes baseline artifacts derived
//!    from the current proto AST.  These become the canonical audit trail.
//!
//! 4. If `<backend>/migrations` is **non-empty** → runs a double-checksum check:
//!    - Computes the current proto manifest checksum (SHA-256 over sorted AST).
//!    - Reads each artifact header for its `-- UDB:proto_manifest_checksum=<sha>`
//!      (or `_udb_meta.proto_manifest_checksum` for JSON, `# UDB:proto_manifest_checksum:`
//!      for YAML, `// UDB:proto_manifest_checksum=` for Cypher).
//!    - Files whose embedded checksum **does not match** the current proto checksum
//!      are marked `Stale`.  Proto always wins.
//!    - By default, stale or headerless artifacts are refreshed **in place** so the
//!      checked-in baseline converges on the current proto state without manual steps.
//!    - When `force_bootstrap` is enabled, refreshed artifacts are written to
//!      `<backend>/bootstrap` for operator review instead of overwriting `migrations/`.
//!
//! 5. Returns a `MigrationSyncReport` per backend, aggregated into
//!    `MultiBackendSyncReport` by `sync_all_backends`.
//!
//! # Directory layout produced
//!
//! ```text
//! <db_ops_root>/
//! ├── postgres/
//! │   ├── migrations/          ← baseline SQL (proto-derived, written once)
//! │   └── bootstrap/           ← updated SQL when proto changes
//! ├── qdrant/
//! │   ├── migrations/          ← JSON collection definitions
//! │   └── bootstrap/
//! ├── minio/
//! │   ├── migrations/          ← JSON bucket policies
//! │   └── bootstrap/
//! ├── redis/
//! │   ├── migrations/          ← YAML key-schema docs
//! │   └── bootstrap/
//! ├── mongodb/
//! │   ├── migrations/          ← JSON collection validators
//! │   └── bootstrap/
//! ├── neo4j/
//! │   ├── migrations/          ← Cypher constraint scripts
//! │   └── bootstrap/
//! ├── clickhouse/
//! │   ├── migrations/          ← ClickHouse DDL SQL
//! │   └── bootstrap/
//! └── seeds/
//!     └── .gitkeep
//!
//! When `UDB_SEEDERS_PATH` is set, the seeders directory is created at that
//! absolute path or, for relative values, relative to the repository root.
//! ```

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::ast::ProtoSchema;
use crate::generation::{
    CatalogManifest, SqlGenerationConfig, generate_bootstrap_sql, generate_delta_sql,
};
// Per-backend `generate_<backend>_artifacts` are reached via the plugin's
// `Backend::generate_artifacts` override (U2 step 4) rather than direct imports.
use crate::migration::diff::diff_manifests;

const ARTIFACT_EXTENSIONS: &[&str] = &["sql", "json", "yaml", "cypher"];

// ── Backend selector ─────────────────────────────────────────────────────────

/// Which backend(s) to sync during a `sync_db_ops` / `sync_all_backends` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendSyncTarget {
    /// PostgreSQL (default — maintains backward compat with `sync_db_ops`).
    Postgres,
    /// Qdrant vector collections.
    Qdrant,
    /// MinIO / S3 bucket policies.
    Minio,
    /// Redis key-schema YAML docs.
    Redis,
    /// MongoDB collection validators.
    Mongodb,
    /// Neo4j constraints and indexes as Cypher.
    Neo4j,
    /// ClickHouse DDL SQL.
    Clickhouse,
    /// Any backend known to the plugin inventory / BackendKind parser.
    Other(crate::backend::BackendKind),
    /// All backends that have at least one relevant `ManifestStore` entry.
    All,
}

impl BackendSyncTarget {
    pub fn from_token(token: &str) -> Option<Self> {
        let token = token.trim();
        if token.eq_ignore_ascii_case("all") {
            return Some(Self::All);
        }
        let kind = crate::backend::BackendKind::from_token(token).or_else(|| match token {
            "mongo" => Some(crate::backend::BackendKind::Mongodb),
            "mssql" => Some(crate::backend::BackendKind::Mssql),
            _ => None,
        })?;
        Some(match kind {
            crate::backend::BackendKind::Postgres => Self::Postgres,
            crate::backend::BackendKind::Qdrant => Self::Qdrant,
            crate::backend::BackendKind::Minio => Self::Minio,
            crate::backend::BackendKind::Redis => Self::Redis,
            crate::backend::BackendKind::Mongodb => Self::Mongodb,
            crate::backend::BackendKind::Neo4j => Self::Neo4j,
            crate::backend::BackendKind::Clickhouse => Self::Clickhouse,
            other => Self::Other(other),
        })
    }

    /// True when this target includes the given backend kind. `All` includes
    /// every backend that has a matching variant; other targets match only
    /// their named kind. Used by `sync_all_backends` to drive the plugin loop.
    pub fn includes(&self, kind: &crate::backend::BackendKind) -> bool {
        use crate::backend::BackendKind as K;
        match (self, kind) {
            (Self::All, _) => true,
            (Self::Postgres, K::Postgres) => true,
            (Self::Qdrant, K::Qdrant) => true,
            (Self::Minio, K::Minio) => true,
            (Self::Redis, K::Redis) => true,
            (Self::Mongodb, K::Mongodb) => true,
            (Self::Neo4j, K::Neo4j) => true,
            (Self::Clickhouse, K::Clickhouse) => true,
            (Self::Other(target), kind) if target == kind => true,
            _ => false,
        }
    }
}

/// Aggregate report from `sync_all_backends`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiBackendSyncReport {
    /// Proto manifest checksum shared by all backends.
    pub proto_checksum: String,
    /// Per-backend reports keyed by backend name (e.g. `"postgres"`, `"qdrant"`).
    pub backends: BTreeMap<String, MigrationSyncReport>,
    /// `true` when every included backend is clean.
    pub clean: bool,
}

// ── Public types ──────────────────────────────────────────────────────────────

/// Per-file sync status after a `sync_db_ops` run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSyncStatus {
    /// File was written for the first time (no prior file on disk).
    Written,
    /// File exists and its embedded proto checksum matches the current AST.
    Verified,
    /// File exists but its embedded checksum differs from the current proto checksum.
    /// Proto takes priority — by default the canonical artifact is refreshed in place.
    Stale,
    /// File exists but contains no `-- UDB:proto_manifest_checksum=` header.
    /// By default this is refreshed in place so the artifact becomes verifiable.
    NoHeader,
}

/// Record for one SQL file processed during sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationFileRecord {
    /// Path relative to the db_ops root (e.g. `migrations/app/001_users.sql`).
    pub rel_path: String,
    pub status: FileSyncStatus,
    /// Checksum embedded in the SQL file header (empty when absent).
    pub file_checksum: String,
    /// Proto manifest checksum at the time of this sync.
    pub proto_checksum: String,
}

/// Full report returned by `sync_db_ops`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationSyncReport {
    /// Absolute path to the resolved `db_ops` root.
    pub db_ops_root: String,
    /// Proto manifest checksum used for this sync.
    pub proto_checksum: String,
    pub total_files: usize,
    pub written: usize,
    pub verified: usize,
    pub stale: usize,
    pub no_header: usize,
    /// Number of bootstrap artifacts written to `db_ops/bootstrap`.
    pub bootstrap_written: usize,
    /// All migration file records.
    pub files: Vec<MigrationFileRecord>,
    /// Bootstrap files written (non-empty when proto changed).
    pub bootstrap_files: Vec<MigrationFileRecord>,
    pub warnings: Vec<String>,
    /// `true` when every migration file is `verified` (no stale, no missing headers).
    pub clean: bool,
}

/// Configuration for `sync_db_ops` and `sync_all_backends`.
#[derive(Debug, Clone)]
pub struct DbOpsSyncConfig {
    /// Explicit db_ops root override.  When `None`, the root is discovered by
    /// walking up from the current working directory to the nearest `Cargo.toml`
    /// and taking its sibling `db_ops` directory.
    ///
    /// This is also populated from the `UDB_DB_OPS_ROOT` environment variable by
    /// `DbOpsSyncConfig::from_env()`.
    pub db_ops_root: Option<PathBuf>,
    /// SQL generation settings (lock_timeout, statement_timeout, etc.).
    pub sql_config: SqlGenerationConfig,
    /// When `true`, write refreshed artifacts to `<backend>/bootstrap` instead of
    /// overwriting `migrations/`. Useful for explicit operator review mode.
    pub force_bootstrap: bool,
    /// Which backend(s) to sync.  Defaults to `BackendSyncTarget::Postgres`.
    pub backend: BackendSyncTarget,
}

impl Default for DbOpsSyncConfig {
    fn default() -> Self {
        Self {
            db_ops_root: None,
            sql_config: SqlGenerationConfig::default(),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
        }
    }
}

impl DbOpsSyncConfig {
    /// Build a config honouring environment variables.
    ///
    /// - `UDB_DB_OPS_ROOT` → `db_ops_root`
    /// - `UDB_DB_OPS_FORCE_BOOTSTRAP=1` → `force_bootstrap`
    /// - `UDB_DB_OPS_BACKEND` → `backend` (any `BackendKind::from_token` value or `all`)
    pub fn from_env() -> Self {
        let backend = env::var("UDB_DB_OPS_BACKEND")
            .ok()
            .and_then(|raw| BackendSyncTarget::from_token(&raw))
            .unwrap_or(BackendSyncTarget::Postgres);
        Self {
            db_ops_root: env::var("UDB_DB_OPS_ROOT").ok().map(PathBuf::from),
            sql_config: SqlGenerationConfig::default(),
            force_bootstrap: env::var("UDB_DB_OPS_FORCE_BOOTSTRAP")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            backend,
        }
    }
}

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum SyncError {
    Io(PathBuf, io::Error),
    Checksum(serde_json::Error),
    SqlGeneration(String),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Io(path, err) => write!(f, "I/O error at {}: {err}", path.display()),
            SyncError::Checksum(err) => write!(f, "checksum serialisation error: {err}"),
            SyncError::SqlGeneration(err) => write!(f, "SQL generation error: {err}"),
        }
    }
}

impl std::error::Error for SyncError {}

// ── Main entry point ──────────────────────────────────────────────────────────

/// Synchronise `db_ops/postgres/{migrations,bootstrap}` with the current proto AST.
///
/// This is the PostgreSQL-only entry point retained for backward compatibility.
/// For multi-backend sync use `sync_all_backends`.
pub fn sync_db_ops(
    schemas: &[ProtoSchema],
    config: &DbOpsSyncConfig,
) -> Result<MigrationSyncReport, SyncError> {
    let manifest = CatalogManifest::from_schemas(schemas)
        .map_err(|err| SyncError::SqlGeneration(err.to_string()))?;
    let proto_checksum = manifest.checksum_sha256.clone();

    // Resolve the db_ops root directory.
    let db_ops_root = match &config.db_ops_root {
        Some(p) => p.clone(),
        None => discover_db_ops_root()?,
    };

    // Seeds directory is shared across all backends.
    let seeds_dir = resolve_seeders_dir(&db_ops_root);
    ensure_seeds_dir(&seeds_dir)?;

    // Postgres lives under a `postgres/` subdirectory.
    let pg_root = db_ops_root.join("postgres");
    let artifacts = generate_bootstrap_sql(schemas, &config.sql_config)
        .map_err(|err| SyncError::SqlGeneration(err.to_string()))?;

    sync_backend_artifacts(
        &artifacts,
        Some(&manifest),
        &pg_root,
        &proto_checksum,
        &config.sql_config,
        config.force_bootstrap,
    )
}

/// Synchronise all configured backends (or a specific one) with the current proto AST.
///
/// - `BackendSyncTarget::All` runs every backend that produces at least one artifact.
/// - Other variants run exactly the named backend.
///
/// Returns a `MultiBackendSyncReport` that aggregates per-backend `MigrationSyncReport`s.
pub fn sync_all_backends(
    schemas: &[ProtoSchema],
    config: &DbOpsSyncConfig,
) -> Result<MultiBackendSyncReport, SyncError> {
    let manifest = CatalogManifest::from_schemas(schemas)
        .map_err(|err| SyncError::SqlGeneration(err.to_string()))?;
    let proto_checksum = manifest.checksum_sha256.clone();

    let db_ops_root = match &config.db_ops_root {
        Some(p) => p.clone(),
        None => discover_db_ops_root()?,
    };

    let seeds_dir = resolve_seeders_dir(&db_ops_root);
    ensure_seeds_dir(&seeds_dir)?;

    let run_postgres = config
        .backend
        .includes(&crate::backend::BackendKind::Postgres);

    let mut reports: BTreeMap<String, MigrationSyncReport> = BTreeMap::new();

    if run_postgres {
        let artifacts = generate_bootstrap_sql(schemas, &config.sql_config)
            .map_err(|err| SyncError::SqlGeneration(err.to_string()))?;
        if !artifacts.is_empty() {
            let report = sync_backend_artifacts(
                &artifacts,
                Some(&manifest),
                &db_ops_root.join("postgres"),
                &proto_checksum,
                &config.sql_config,
                config.force_bootstrap,
            )?;
            reports.insert("postgres".to_string(), report);
        }
    }

    // U2 step 4: every non-postgres backend dispatches through its plugin's
    // `generate_artifacts`. Postgres stays special-cased above because its
    // bootstrap path also wants the `CatalogManifest`. Plugins for backends
    // without a generator return an empty Vec (default `Backend` impl) and the
    // `is_empty` guard below skips the sync step for them.
    for plugin in crate::backend::all_plugins() {
        let kind = plugin.kind();
        if kind == crate::backend::BackendKind::Postgres {
            continue;
        }
        if !config.backend.includes(&kind) {
            continue;
        }
        let artifacts = plugin
            .generate_artifacts(&manifest, &config.sql_config)
            .map_err(SyncError::SqlGeneration)?;
        if artifacts.is_empty() {
            // A backend that advertises schema-migration support but produces no
            // artifacts has an unimplemented generator — surface it rather than
            // silently skipping, which reads as "nothing to migrate".
            if kind.capabilities().supports_schema_migration {
                tracing::warn!(
                    backend = %kind.as_str(),
                    "backend advertises schema-migration support but generated zero artifacts; \
                     no DDL was synced (generate_artifacts is not implemented for this backend)"
                );
            }
            continue;
        }
        let subdir = plugin.sync_subdir();
        let report = sync_backend_artifacts(
            &artifacts,
            None,
            &db_ops_root.join(subdir),
            &proto_checksum,
            &config.sql_config,
            config.force_bootstrap,
        )?;
        reports.insert(subdir.to_string(), report);
    }

    let clean = reports.values().all(|r| r.clean);
    Ok(MultiBackendSyncReport {
        proto_checksum,
        backends: reports,
        clean,
    })
}

/// Core sync algorithm shared by all backends.
///
/// - `backend_root` is `<db_ops_root>/<backend_name>/`; this function creates
///   `backend_root/migrations` and `backend_root/bootstrap` as needed.
/// - `manifest` is `Some` only for the Postgres backend; it is used to compute
///   precise ALTER TABLE delta SQL when the proto changes.  Pass `None` for
///   non-SQL backends (Qdrant, MinIO, etc.) where baseline artifacts are idempotent.
fn sync_backend_artifacts(
    artifacts: &[crate::generation::GeneratedArtifact],
    manifest: Option<&CatalogManifest>,
    backend_root: &Path,
    proto_checksum: &str,
    sql_config: &SqlGenerationConfig,
    force_bootstrap: bool,
) -> Result<MigrationSyncReport, SyncError> {
    let migrations_dir = backend_root.join("migrations");
    let bootstrap_dir = backend_root.join("bootstrap");
    // Sidecar that persists the last successfully-written manifest for diff.
    let sidecar_path = migrations_dir.join(".last_manifest.json");

    for dir in [&migrations_dir, &bootstrap_dir] {
        fs::create_dir_all(dir).map_err(|e| SyncError::Io(dir.clone(), e))?;
    }

    let existing = scan_artifact_files_recursive(&migrations_dir)?;
    let expected_rel_paths = artifacts
        .iter()
        .map(|artifact| artifact.rel_path.clone())
        .collect::<BTreeSet<_>>();

    let mut warnings: Vec<String> = Vec::new();
    let mut file_records: Vec<MigrationFileRecord> = Vec::new();
    let mut bootstrap_records: Vec<MigrationFileRecord> = Vec::new();

    if existing.is_empty() {
        // ── Case A: migrations folder is empty (first-ever run) ───────────────
        // Write the baseline CREATE TABLE artifacts and snapshot the manifest.
        for artifact in artifacts {
            let dest = migrations_dir.join(&artifact.rel_path);
            ensure_parent(&dest)?;
            fs::write(&dest, artifact.content.as_bytes())
                .map_err(|e| SyncError::Io(dest.clone(), e))?;
            file_records.push(MigrationFileRecord {
                rel_path: format!("migrations/{}", artifact.rel_path),
                status: FileSyncStatus::Written,
                file_checksum: String::new(),
                proto_checksum: proto_checksum.to_string(),
            });
        }
        // Persist the current manifest so that the next run can diff against it.
        if let Some(mf) = manifest
            && let Ok(json) = serde_json::to_string_pretty(mf)
        {
            let _ = fs::write(&sidecar_path, json.as_bytes());
        }
    } else {
        // ── Case B: migrations folder has content ─────────────────────────────
        // Check checksums on existing migration files to detect proto changes.
        let mut stale_schemas: BTreeSet<String> = BTreeSet::new();
        let mut pending_new_artifacts = Vec::new();

        for artifact in artifacts {
            let dest = migrations_dir.join(&artifact.rel_path);
            if dest.exists() {
                let content =
                    fs::read_to_string(&dest).map_err(|e| SyncError::Io(dest.clone(), e))?;
                let embedded = extract_proto_checksum_header(&content);
                let body_checksum = artifact_body_checksum(&content);
                let expected_body_checksum = artifact_body_checksum(&artifact.content);
                let status = if embedded.is_empty() {
                    warnings.push(format!(
                        "migrations/{}: missing proto_manifest_checksum header — cannot verify",
                        artifact.rel_path
                    ));
                    FileSyncStatus::NoHeader
                } else if embedded == proto_checksum {
                    if body_checksum == expected_body_checksum {
                        FileSyncStatus::Verified
                    } else {
                        warnings.push(format!(
                            "migrations/{}: artifact body checksum mismatch — expected {}, found {}",
                            artifact.rel_path, expected_body_checksum, body_checksum
                        ));
                        stale_schemas.insert(artifact.schema.clone());
                        FileSyncStatus::Stale
                    }
                } else {
                    stale_schemas.insert(artifact.schema.clone());
                    FileSyncStatus::Stale
                };
                file_records.push(MigrationFileRecord {
                    rel_path: format!("migrations/{}", artifact.rel_path),
                    status,
                    file_checksum: embedded,
                    proto_checksum: proto_checksum.to_string(),
                });
            } else {
                pending_new_artifacts.push(artifact.clone());
                file_records.push(MigrationFileRecord {
                    rel_path: format!("migrations/{}", artifact.rel_path),
                    status: FileSyncStatus::Written,
                    file_checksum: String::new(),
                    proto_checksum: proto_checksum.to_string(),
                });
            }
        }

        let extra_artifacts = existing
            .iter()
            .filter_map(|path| relative_artifact_path(&migrations_dir, path))
            .filter(|rel_path| !expected_rel_paths.contains(rel_path))
            .collect::<Vec<_>>();

        let has_stale = file_records
            .iter()
            .any(|r| r.status == FileSyncStatus::Stale);
        let has_no_header = file_records
            .iter()
            .any(|r| r.status == FileSyncStatus::NoHeader);
        let has_noncanonical = has_stale || has_no_header || !extra_artifacts.is_empty();

        if has_noncanonical && !force_bootstrap {
            warnings.push(
                "non-canonical migrations detected; refreshing canonical migrations in place"
                    .to_string(),
            );
            reset_artifact_dir(&migrations_dir)?;
            reset_artifact_dir(&bootstrap_dir)?;

            file_records.clear();
            for artifact in artifacts {
                let dest = migrations_dir.join(&artifact.rel_path);
                ensure_parent(&dest)?;
                fs::write(&dest, artifact.content.as_bytes())
                    .map_err(|e| SyncError::Io(dest.clone(), e))?;
                file_records.push(MigrationFileRecord {
                    rel_path: format!("migrations/{}", artifact.rel_path),
                    status: FileSyncStatus::Written,
                    file_checksum: proto_checksum.to_string(),
                    proto_checksum: proto_checksum.to_string(),
                });
            }

            if let Some(mf) = manifest
                && let Ok(json) = serde_json::to_string_pretty(mf)
            {
                let _ = fs::write(&sidecar_path, json.as_bytes());
            }
        } else {
            for artifact in &pending_new_artifacts {
                let dest = migrations_dir.join(&artifact.rel_path);
                ensure_parent(&dest)?;
                fs::write(&dest, artifact.content.as_bytes())
                    .map_err(|e| SyncError::Io(dest.clone(), e))?;
            }

            if has_noncanonical || force_bootstrap {
                // ── Compute delta SQL from the prior manifest sidecar ─────────────
                // For the Postgres backend, generate ALTER TABLE / ADD COLUMN delta
                // SQL so operators get exact change scripts rather than the full
                // baseline CREATE TABLE, which is useless when tables already exist.
                let delta_artifacts: Vec<crate::generation::GeneratedArtifact> = if let Some(
                    new_manifest,
                ) = manifest
                {
                    let prior_manifest = fs::read_to_string(&sidecar_path)
                        .ok()
                        .and_then(|json| serde_json::from_str::<CatalogManifest>(&json).ok());

                    if let Some(ref prior) = prior_manifest {
                        let changes = diff_manifests(Some(prior), new_manifest);
                        let delta = generate_delta_sql(new_manifest, &changes, sql_config);
                        if delta.is_empty() {
                            warnings.push(
                                    "proto changed but no auto-safe ALTER statements were generated; \
                                     writing baseline bootstrap for review (may contain blocked ops)"
                                        .to_string(),
                                );
                            artifacts
                                .iter()
                                .filter(|a| {
                                    force_bootstrap
                                        || stale_schemas.contains(&a.schema)
                                        || a.schema.is_empty()
                                })
                                .cloned()
                                .collect()
                        } else {
                            delta
                        }
                    } else {
                        warnings.push(
                            "no prior manifest sidecar found; writing baseline bootstrap \
                                 for first-time stale detection"
                                .to_string(),
                        );
                        artifacts
                            .iter()
                            .filter(|a| {
                                force_bootstrap
                                    || stale_schemas.contains(&a.schema)
                                    || a.schema.is_empty()
                            })
                            .cloned()
                            .collect()
                    }
                } else {
                    artifacts
                        .iter()
                        .filter(|a| {
                            force_bootstrap
                                || stale_schemas.contains(&a.schema)
                                || a.schema.is_empty()
                        })
                        .cloned()
                        .collect()
                };

                reset_artifact_dir(&bootstrap_dir)?;
                for artifact in &delta_artifacts {
                    let dest = bootstrap_dir.join(&artifact.rel_path);
                    ensure_parent(&dest)?;
                    fs::write(&dest, artifact.content.as_bytes())
                        .map_err(|e| SyncError::Io(dest.clone(), e))?;
                    bootstrap_records.push(MigrationFileRecord {
                        rel_path: format!("bootstrap/{}", artifact.rel_path),
                        status: FileSyncStatus::Written,
                        file_checksum: String::new(),
                        proto_checksum: proto_checksum.to_string(),
                    });
                }

                if let Some(mf) = manifest
                    && let Ok(json) = serde_json::to_string_pretty(mf)
                {
                    let _ = fs::write(&sidecar_path, json.as_bytes());
                }
            }
        }
    }

    let written = file_records
        .iter()
        .filter(|r| r.status == FileSyncStatus::Written)
        .count();
    let verified = file_records
        .iter()
        .filter(|r| r.status == FileSyncStatus::Verified)
        .count();
    let stale = file_records
        .iter()
        .filter(|r| r.status == FileSyncStatus::Stale)
        .count();
    let no_header = file_records
        .iter()
        .filter(|r| r.status == FileSyncStatus::NoHeader)
        .count();
    let clean = stale == 0 && no_header == 0;

    Ok(MigrationSyncReport {
        db_ops_root: backend_root.display().to_string(),
        proto_checksum: proto_checksum.to_string(),
        total_files: file_records.len(),
        written,
        verified,
        stale,
        no_header,
        bootstrap_written: bootstrap_records.len(),
        files: file_records,
        bootstrap_files: bootstrap_records,
        warnings,
        clean,
    })
}

// ── Directory / path helpers ──────────────────────────────────────────────────

/// Resolve the shared seeders directory.
///
/// Resolution order:
/// 1. `UDB_SEEDERS_PATH`
/// 2. `UDB_SEEDER_PATH`
/// 3. `<db_ops_root>/seeds`
///
/// Relative env values are resolved against the repository root, which is the
/// parent directory of `db_ops_root`.
pub(crate) fn resolve_seeders_dir(db_ops_root: &Path) -> PathBuf {
    let repo_root = db_ops_root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| db_ops_root.to_path_buf());

    env::var("UDB_SEEDERS_PATH")
        .ok()
        .or_else(|| env::var("UDB_SEEDER_PATH").ok())
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                repo_root.join(path)
            }
        })
        .unwrap_or_else(|| db_ops_root.join("seeds"))
}

/// Ensure the shared seeders directory exists with a `.gitkeep`.
fn ensure_seeds_dir(seeds_dir: &Path) -> Result<(), SyncError> {
    fs::create_dir_all(seeds_dir).map_err(|e| SyncError::Io(seeds_dir.to_path_buf(), e))?;
    let gitkeep = seeds_dir.join(".gitkeep");
    if !gitkeep.exists() {
        fs::write(
            &gitkeep,
            b"# Place seed files here (e.g. 001_roles.sql).\n\
              # These run after migrations during test/staging bootstraps.\n",
        )
        .map_err(|e| SyncError::Io(gitkeep.clone(), e))?;
    }
    Ok(())
}

/// Recursively collect all artifact files under `dir`.
/// Returns an empty `Vec` (not an error) when the directory is empty.
fn scan_artifact_files_recursive(dir: &Path) -> Result<Vec<PathBuf>, SyncError> {
    let mut out = Vec::new();
    // Exclude the sidecar manifest file from artifact scanning.
    collect_artifacts_filtered(dir, &mut out)?;
    Ok(out)
}

fn collect_artifacts_filtered(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), SyncError> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(SyncError::Io(dir.to_path_buf(), e)),
    };
    for entry in entries {
        // Fail closed: a per-entry `read_dir` error must abort the scan, not be
        // silently dropped by `.flatten()` — otherwise a sync could miss
        // artifacts and report success (#135).
        let entry = entry.map_err(|e| SyncError::Io(dir.to_path_buf(), e))?;
        let path = entry.path();
        if path.is_dir() {
            collect_artifacts_filtered(&path, out)?;
        } else if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n == ".last_manifest.json")
        {
            // Skip the sidecar file — it is metadata, not a migration artifact.
            continue;
        } else if path
            .extension()
            .and_then(|s| s.to_str())
            .is_some_and(|ext| ARTIFACT_EXTENSIONS.contains(&ext))
        {
            out.push(path);
        }
    }
    Ok(())
}

/// Create parent directories for `dest` if they do not already exist.
fn ensure_parent(dest: &Path) -> Result<(), SyncError> {
    if let Some(parent) = dest.parent()
        && !parent.exists()
    {
        fs::create_dir_all(parent).map_err(|e| SyncError::Io(parent.to_path_buf(), e))?;
    }
    Ok(())
}

fn reset_artifact_dir(dir: &Path) -> Result<(), SyncError> {
    match fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(SyncError::Io(dir.to_path_buf(), err)),
    }
    fs::create_dir_all(dir).map_err(|e| SyncError::Io(dir.to_path_buf(), e))?;
    Ok(())
}

fn relative_artifact_path(base: &Path, path: &Path) -> Option<String> {
    path.strip_prefix(base)
        .ok()
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
}

// ── Proto-checksum header extraction ─────────────────────────────────────────

/// Extract the proto manifest checksum from an artifact header.
///
/// Supported formats:
/// - SQL / Cypher comment:  `-- UDB:proto_manifest_checksum=<sha>`  or  `// UDB:proto_manifest_checksum=<sha>`
/// - YAML comment:           `# UDB:proto_manifest_checksum: <sha>`
/// - JSON object field:      `"proto_manifest_checksum": "<sha>"` inside `_udb_meta`
///
/// Returns an empty string when the header is absent.
fn extract_proto_checksum_header(content: &str) -> String {
    for line in content.lines() {
        let line = line.trim();
        // SQL / Cypher: -- UDB:proto_manifest_checksum=<sha>
        if let Some(val) = line.strip_prefix("-- UDB:proto_manifest_checksum=") {
            return val.trim().to_string();
        }
        // Cypher: // UDB:proto_manifest_checksum=<sha>
        if let Some(val) = line.strip_prefix("// UDB:proto_manifest_checksum=") {
            return val.trim().to_string();
        }
        // YAML: # UDB:proto_manifest_checksum: <sha>
        if let Some(rest) = line.strip_prefix("# UDB:proto_manifest_checksum:") {
            return rest.trim().to_string();
        }
        // JSON: "proto_manifest_checksum": "<sha>"
        if let Some(key_pos) = line.find("\"proto_manifest_checksum\"") {
            let after_key = &line[key_pos + "\"proto_manifest_checksum\"".len()..];
            if let Some(colon_pos) = after_key.find(':') {
                let after_colon = after_key[colon_pos + 1..].trim_start();
                if let Some(rest) = after_colon.strip_prefix('"')
                    && let Some(end_quote) = rest.find('"')
                {
                    let value = &rest[..end_quote];
                    if !value.is_empty() {
                        return value.to_string();
                    }
                }
            }
        }
        // Stop as soon as we hit a non-comment, non-blank, non-JSON line.
        if !line.is_empty()
            && !line.starts_with("--")
            && !line.starts_with("//")
            && !line.starts_with('#')
            && !line.starts_with('{')
            && !line.starts_with('"')
            && !line.starts_with('_')
        {
            break;
        }
    }
    String::new()
}

fn artifact_body_checksum(content: &str) -> String {
    let digest = Sha256::digest(content.as_bytes());
    format!("sha256:{digest:x}")
}

// ── db_ops root discovery ─────────────────────────────────────────────────────

/// Discover the `db_ops` root directory by walking up from the current working
/// directory to the nearest `Cargo.toml` file.
///
/// Resolution order:
/// 1. `UDB_DB_OPS_ROOT` env var (handled by caller via `DbOpsSyncConfig`).
/// 2. Walk CWD upward; when `Cargo.toml` is found the **parent** directory is
///    the workspace root; return `<workspace_root>/db_ops`.
/// 3. Fallback: `<CWD>/../db_ops`.
pub fn discover_db_ops_root() -> Result<PathBuf, SyncError> {
    let cwd = env::current_dir().map_err(|e| SyncError::Io(PathBuf::from("<cwd>"), e))?;

    let mut dir = cwd.clone();
    loop {
        if dir.join("Cargo.toml").exists() {
            // Go one level up to get the workspace / repository root so that
            // `db_ops` is a sibling of the `udb` crate directory.
            let parent = dir
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| dir.clone());
            return Ok(parent.join("db_ops"));
        }
        // Invoked from the repo root directly (e.g. `<repo>/`):
        // `Cargo.toml` lives at `udb/Cargo.toml`, not at the root.
        // Detect this by checking for a `.git` directory or `udb/Cargo.toml`.
        if dir.join(".git").exists() || dir.join("udb").join("Cargo.toml").exists() {
            return Ok(dir.join("db_ops"));
        }
        match dir.parent() {
            Some(p) if p != dir => dir = p.to_path_buf(),
            _ => break,
        }
    }

    // Fallback: sibling of CWD.
    let fallback = cwd
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| cwd.clone())
        .join("db_ops");
    Ok(fallback)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{ParserConfig, parse_file};
    use std::fs;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("lock env mutex")
    }

    fn parse_source(source: &str, _filename: &str) -> Vec<ProtoSchema> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_file = std::env::temp_dir().join(format!("udb_test_parse_{}.proto", id));
        fs::write(&tmp_file, source.as_bytes()).unwrap();
        let schemas = parse_file(&tmp_file, &ParserConfig::default()).expect("parse proto source");
        let _ = fs::remove_file(&tmp_file);
        schemas
    }

    // ── Header extraction ──────────────────────────────────────────────────────

    #[test]
    fn extract_checksum_from_header() {
        let sql = "-- UDB:migration_kind=bootstrap\n\
                   -- UDB:schema=app\n\
                   -- UDB:table=users\n\
                   -- UDB:proto_manifest_checksum=abc123\n\
                   -- UDB:generator=udb\n\n\
                   CREATE TABLE \"app\".\"users\" ();\n";
        assert_eq!(extract_proto_checksum_header(sql), "abc123");
    }

    #[test]
    fn extract_checksum_missing() {
        let sql = "CREATE TABLE \"app\".\"users\" ();\n";
        assert_eq!(extract_proto_checksum_header(sql), "");
    }

    #[test]
    fn extract_checksum_stops_at_non_comment() {
        let sql = "SET lock_timeout = '5s';\n-- UDB:proto_manifest_checksum=never\n";
        assert_eq!(extract_proto_checksum_header(sql), "");
    }

    #[test]
    fn extract_checksum_from_yaml_header() {
        let yaml = "# UDB:migration_kind: bootstrap\n\
                    # UDB:backend: redis\n\
                    # UDB:proto_manifest_checksum: sha256-deadbeef\n\
                    namespace: ocr\n";
        assert_eq!(extract_proto_checksum_header(yaml), "sha256-deadbeef");
    }

    #[test]
    fn extract_checksum_from_cypher_header() {
        let cypher = "// UDB:migration_kind=bootstrap\n\
                      // UDB:proto_manifest_checksum=cafebabe\n\
                      CREATE CONSTRAINT;\n";
        assert_eq!(extract_proto_checksum_header(cypher), "cafebabe");
    }

    #[test]
    fn extract_checksum_from_json_udb_meta() {
        let json =
            r#"{"_udb_meta":{"proto_manifest_checksum":"sha256-1234"},"collection_name":"docs"}"#;
        assert_eq!(extract_proto_checksum_header(json), "sha256-1234");
    }

    // ── sync_db_ops: empty migrations dir writes baseline ─────────────────────

    const SIMPLE_PROTO: &str = r#"
syntax = "proto3";
package example.test.entity.v1;
import "udb/core/common/v1/db.proto";
message Widget {
  option (udb.core.common.v1.pg_table) = {
    table_name: "widgets"
    schema_name: "app"
    migration_order: 10
    is_table: true
  };
  string widget_id = 1 [(udb.core.common.v1.pg_column) = {
    column_name: "widget_id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
    default_value: "gen_random_uuid()"
  }];
}
"#;

    fn parse_simple() -> Vec<ProtoSchema> {
        parse_source(SIMPLE_PROTO, "udb_test_simple.proto")
    }

    #[test]
    fn sync_db_ops_writes_baseline_on_empty_dir() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_sync_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_simple();
        let report = sync_db_ops(&schemas, &cfg).expect("sync_db_ops");
        assert!(
            report.written > 0,
            "expected baseline artifacts to be written"
        );
        assert!(report.clean, "fresh run should be clean");

        // migrations dir must exist with at least one file
        let migrations_dir = tmp.join("postgres").join("migrations");
        assert!(
            migrations_dir.exists(),
            "postgres/migrations/ must be created"
        );
        let entries: Vec<_> = fs::read_dir(&migrations_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(
            !entries.is_empty(),
            "at least one migration file must be written"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_db_ops_verifies_matching_checksum() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_verify_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_simple();

        // First run writes the baseline.
        sync_db_ops(&schemas, &cfg).expect("first sync");

        // Second run on the same proto must be fully clean (all Verified).
        let report2 = sync_db_ops(&schemas, &cfg).expect("second sync");
        assert_eq!(report2.stale, 0, "no stale files on identical re-run");
        assert!(report2.clean, "second run must be clean");
        assert!(
            report2.verified > 0,
            "at least one file verified on second run"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_db_ops_marks_preserved_header_body_edit_stale() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_body_stale_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_simple();
        let report1 = sync_db_ops(&schemas, &cfg).expect("first sync");

        let migrations_dir = tmp.join("postgres").join("migrations");
        let target = scan_artifact_files_recursive(&migrations_dir)
            .expect("scan migrations")
            .into_iter()
            .find(|path| {
                fs::read_to_string(path)
                    .map(|content| !extract_proto_checksum_header(&content).is_empty())
                    .unwrap_or(false)
            })
            .expect("migration with checksum header");
        let rel_path = format!(
            "migrations/{}",
            relative_artifact_path(&migrations_dir, &target).expect("relative path")
        );
        let original = fs::read_to_string(&target).unwrap();
        assert_eq!(
            extract_proto_checksum_header(&original),
            report1.proto_checksum,
            "test setup must preserve the current header checksum"
        );
        fs::write(
            &target,
            format!("{original}\n-- hand-edited body with preserved header\n"),
        )
        .unwrap();

        let review_cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: true,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let report2 = sync_db_ops(&schemas, &review_cfg).expect("second sync");
        let record = report2
            .files
            .iter()
            .find(|record| record.rel_path == rel_path)
            .expect("edited migration record");
        assert_eq!(record.status, FileSyncStatus::Stale);
        assert_eq!(
            record.file_checksum, report2.proto_checksum,
            "the preserved header still matches the current proto checksum"
        );
        assert!(!report2.clean, "body edit must make review-mode sync dirty");
        assert!(
            report2
                .warnings
                .iter()
                .any(|warning| warning.contains("artifact body checksum mismatch")),
            "body checksum mismatch should be reported"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_db_ops_refreshes_stale_in_place() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_stale_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_simple();

        // Write baseline.
        sync_db_ops(&schemas, &cfg).expect("first sync");

        // Make every migration non-canonical while preserving the checksum header.
        let migrations_dir = tmp.join("postgres").join("migrations");
        fn patch_sql_files(dir: &std::path::Path) {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_dir() {
                        patch_sql_files(&path);
                    } else if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                        let content = fs::read_to_string(&path).unwrap();
                        let patched = content.replace(
                            "-- UDB:proto_manifest_checksum=",
                            "-- UDB:proto_manifest_checksum=STALE_",
                        );
                        fs::write(&path, patched).unwrap();
                    }
                }
            }
        }
        patch_sql_files(&migrations_dir);

        // Second run should refresh the canonical baseline in place.
        let report2 = sync_db_ops(&schemas, &cfg).expect("second sync");
        assert_eq!(report2.stale, 0, "stale files must be refreshed in place");
        assert_eq!(report2.no_header, 0, "refreshed files must be verifiable");
        assert!(report2.clean, "run must converge on a clean baseline");
        assert!(
            report2.written > 0,
            "refresh must rewrite canonical artifacts"
        );

        // Default mode should not require bootstrap review artifacts.
        let bootstrap_dir = tmp.join("postgres").join("bootstrap");
        assert!(
            scan_artifact_files_recursive(&bootstrap_dir)
                .expect("scan bootstrap dir")
                .is_empty(),
            "bootstrap/ should remain empty during in-place refresh"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_db_ops_force_bootstrap_preserves_review_mode() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_bootstrap_review_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_simple();
        sync_db_ops(&schemas, &cfg).expect("first sync");

        let migrations_dir = tmp.join("postgres").join("migrations");
        fn patch_sql_files(dir: &std::path::Path) {
            if let Ok(entries) = fs::read_dir(dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_dir() {
                        patch_sql_files(&path);
                    } else if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                        let content = fs::read_to_string(&path).unwrap();
                        let patched = content.replace(
                            "-- UDB:proto_manifest_checksum=",
                            "-- UDB:proto_manifest_checksum=STALE_",
                        );
                        fs::write(&path, patched).unwrap();
                    }
                }
            }
        }
        patch_sql_files(&migrations_dir);

        let review_cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: true,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let report2 = sync_db_ops(&schemas, &review_cfg).expect("second sync");
        assert!(
            report2.stale > 0,
            "review mode should preserve stale detection"
        );
        assert!(!report2.clean, "review mode should require operator action");
        assert!(
            report2.bootstrap_written > 0,
            "review mode should emit bootstrap artifacts"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    // ── sync_all_backends: multi-dir layout ──────────────────────────────────

    const MULTI_BACKEND_PROTO: &str = r#"
syntax = "proto3";
package example.test.multi.v1;
import "udb/core/common/v1/db.proto";
message Embedding {
  option (udb.core.common.v1.pg_table) = {
    table_name: "embeddings"
    schema_name: "app"
    migration_order: 1
    is_table: true
  };
  option (udb.core.common.v1.vector_store) = {
    backend: VECTOR_BACKEND_QDRANT
    collection_name: "embeddings"
    dimension: 768
    distance: VECTOR_DISTANCE_COSINE
  };
  option (udb.core.common.v1.cache) = {
    backend: CACHE_BACKEND_REDIS
    key_pattern: "app:embed:{id}"
    ttl_seconds: 3600
  };
  string id = 1 [(udb.core.common.v1.pg_column) = {
    column_name: "id"
    sql_type: "UUID"
    primary_key: true
    not_null: true
  }];
  string s3_uri = 2 [
    (udb.core.common.v1.pg_column) = { column_name: "s3_uri" sql_type: "TEXT" },
    (udb.core.common.v1.storage) = {
      backend: STORAGE_BACKEND_MINIO
      bucket_env_key: "EMBED_BUCKET"
      key_prefix: "embeddings/"
    }
  ];
}
"#;

    #[test]
    fn sync_all_backends_creates_multi_dir_layout() {
        let _env_guard = env_lock();
        let prior_seeders = std::env::var("UDB_SEEDERS_PATH").ok();
        let prior_seeder = std::env::var("UDB_SEEDER_PATH").ok();
        #[allow(unused_unsafe)]
        unsafe {
            std::env::remove_var("UDB_SEEDERS_PATH");
            std::env::remove_var("UDB_SEEDER_PATH");
        }
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_multi_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::All,
            ..Default::default()
        };
        let schemas = parse_source(MULTI_BACKEND_PROTO, "udb_test_multi.proto");
        let report = sync_all_backends(&schemas, &cfg).expect("sync_all_backends");
        assert!(report.clean, "fresh multi-backend run must be clean");

        // At least postgres and qdrant should appear (proto has both).
        assert!(
            report.backends.contains_key("postgres"),
            "postgres backend must be synced"
        );
        assert!(
            report.backends.contains_key("qdrant"),
            "qdrant backend must be synced"
        );

        // The shared seeds dir must always exist. In developer environments
        // UDB_SEEDERS_PATH may intentionally point outside db_ops_root, so use
        // the same resolver as production code rather than hardcoding tmp/seeds.
        assert!(tmp.join("seeds").exists(), "seeds/ must be created");

        // Every backend directory that was synced must have a migrations/ subdir.
        for (backend, backend_report) in &report.backends {
            let migrations = tmp.join(backend).join("migrations");
            assert!(
                migrations.exists(),
                "{backend}/migrations/ must exist; report={backend_report:?}"
            );
        }

        #[allow(unused_unsafe)]
        unsafe {
            match prior_seeders {
                Some(value) => std::env::set_var("UDB_SEEDERS_PATH", value),
                None => std::env::remove_var("UDB_SEEDERS_PATH"),
            }
            match prior_seeder {
                Some(value) => std::env::set_var("UDB_SEEDER_PATH", value),
                None => std::env::remove_var("UDB_SEEDER_PATH"),
            }
        }
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_all_backends_uses_env_seeders_path() {
        let _env_guard = env_lock();
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_seeders_env_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let db_ops_root = tmp.join("db_ops");
        fs::create_dir_all(&db_ops_root).unwrap();

        let prior_seeders = std::env::var("UDB_SEEDERS_PATH").ok();
        #[allow(unused_unsafe)]
        unsafe {
            std::env::set_var("UDB_SEEDERS_PATH", "database/seeders/postgress");
        }

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(db_ops_root.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::All,
            ..Default::default()
        };

        let schemas = parse_source(MULTI_BACKEND_PROTO, "udb_test_seeders_env.proto");
        let report = sync_all_backends(&schemas, &cfg).expect("sync_all_backends");
        assert!(report.clean, "fresh multi-backend run must be clean");

        let custom_seeders_dir = tmp.join("database").join("seeders").join("postgress");
        assert!(
            custom_seeders_dir.exists(),
            "custom seeders path must be created"
        );
        assert!(
            custom_seeders_dir.join(".gitkeep").exists(),
            "custom seeders path must receive .gitkeep"
        );
        assert!(
            !db_ops_root.join("seeds").exists(),
            "default db_ops/seeds path should not be used when env override is set"
        );

        #[allow(unused_unsafe)]
        unsafe {
            match prior_seeders {
                Some(value) => std::env::set_var("UDB_SEEDERS_PATH", value),
                None => std::env::remove_var("UDB_SEEDERS_PATH"),
            }
        }
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn sync_all_backends_postgres_only_skips_other_dirs() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_pg_only_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            force_bootstrap: false,
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };

        let schemas = parse_source(MULTI_BACKEND_PROTO, "udb_test_multi_pg.proto");
        let report = sync_all_backends(&schemas, &cfg).expect("sync_all_backends postgres");

        // Only postgres backend should appear.
        assert!(report.backends.contains_key("postgres"));
        assert!(
            !report.backends.contains_key("qdrant"),
            "qdrant should not be synced when backend=postgres"
        );
        assert!(
            !tmp.join("qdrant").exists(),
            "qdrant/ dir must not be created"
        );

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn multi_backend_report_proto_checksum_is_stable() {
        let tmp = std::env::temp_dir().join(format!(
            "udb_test_chksum_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();

        let cfg = DbOpsSyncConfig {
            db_ops_root: Some(tmp.clone()),
            backend: BackendSyncTarget::Postgres,
            ..Default::default()
        };
        let schemas = parse_simple();

        let r1 = sync_all_backends(&schemas, &cfg).unwrap();
        let r2 = sync_all_backends(&schemas, &cfg).unwrap();

        assert_eq!(
            r1.proto_checksum, r2.proto_checksum,
            "proto_checksum must be deterministic"
        );
        assert!(
            !r1.proto_checksum.is_empty(),
            "proto_checksum must not be empty"
        );

        let _ = fs::remove_dir_all(&tmp);
    }
}
