mod backend_safety;
pub mod backends;
pub mod drift_report;
pub mod dsn;
pub mod lint;
pub mod manifest;
pub mod manifest_index;
pub mod sql;

pub use backends::{
    generate_clickhouse_artifacts, generate_minio_artifacts, generate_mongodb_artifacts,
    generate_mssql_artifacts, generate_mysql_artifacts, generate_neo4j_artifacts,
    generate_qdrant_artifacts, generate_redis_artifacts, generate_sqlite_artifacts,
};
pub use drift_report::{DriftItem, DriftReport, DriftSeverity, build_drift_report};
pub use dsn::{
    DsnGenerationConfig, DsnValidationReport, ParsedUnifiedDsn, ResolvedUnifiedDsn, UnifiedDsn,
    UnifiedDsnCatalog, generate_unified_dsn_catalog, parse_unified_dsn, redact_dsn,
    resolve_unified_dsn, resolve_unified_dsn_catalog, validate_unified_dsn,
};
pub use lint::{LintItem, LintReport, LintSeverity, lint_catalog};
pub use manifest::{
    CatalogManifest, GENERATOR_VERSION, ManifestCheck, ManifestColumn, ManifestColumnSecurity,
    ManifestExtension, ManifestForeignKey, ManifestIndex, ManifestMaterializedView,
    ManifestMigrationReport, ManifestSchemaChecksum, ManifestStore, ManifestStoreOption,
    ManifestTable, ManifestTableSecurity, ManifestTrigger, migrate_manifest_to_current,
};
pub use sql::{GeneratedArtifact, SqlGenerationConfig, generate_bootstrap_sql, generate_delta_sql};
