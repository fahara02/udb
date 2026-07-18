// ── Crate lint gates ──────────────────────────────────────────────────────────
// Warn-level for now. The glob re-exports below currently mask dead code;
// promote these to `deny` only after the public surface is explicit.
#![warn(unused)]
#![warn(dead_code)]

// ── Domain modules ────────────────────────────────────────────────────────────
pub mod backend;
pub mod control;
pub mod embedded;
pub mod generation;
pub mod init;
pub mod ir;
pub mod migration;
pub mod parser;
pub mod planning;
pub mod proto_redaction;
pub mod protocol;
pub mod runtime;
pub mod schema;
pub mod tui;

// D.2: hidden, feature-gated benchmarking shims over hot `pub(crate)` helpers.
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_internals;

// ── Compatibility module aliases ─────────────────────────────────────────────
pub use control::{
    auto_alter, engine, executor, lifecycle, notification, plan_approval, status, sync_config,
    tracker,
};
pub use parser::lexer;
// `planning::registry` was folded into `crate::backend` in U2 step 6:
// `BackendDispatchHook` had zero callers/impls and is gone; the support-state
// check (the only real `BackendRegistry` use) now lives in
// `backend::support_state_for_kind`. Status/Registration enums are dropped — they
// were a parallel identity model and have no remaining consumer.
pub use backend::{BackendSupportState, has_runtime_implementation, support_state_for_kind};
pub use planning::{broker, provisioning};
pub use protocol as proto;
#[cfg(feature = "clickhouse")]
pub use runtime::{ClickHouseConfig, ClickHouseExecutor};
pub use runtime::{
    ConfigReloadMode, ConfigReloadOptions, ConfigReloadReport, PostgresMigrationAuditSink,
};
#[cfg(feature = "mongodb")]
pub use runtime::{MongoDbConfig, MongoDbExecutor};
#[cfg(feature = "neo4j")]
pub use runtime::{Neo4jConfig, Neo4jExecutor};
pub use runtime::{
    cdc, config, metrics, observability, replica, security, service, signal, system,
};
pub use schema::{ast, checksum};

// ── Re-exports ────────────────────────────────────────────────────────────────
pub use ast::{
    CacheStore, ColumnStore, DbExtension, DbTrigger, DocumentStore, GenericStore, GraphStore,
    MaterializedView, ModelRegistry, ProtoCatalog, ProtoColumn, ProtoColumnSecurity,
    ProtoDefinition, ProtoEnumAst, ProtoEnumValueAst, ProtoFieldAst, ProtoFileAst, ProtoForeignKey,
    ProtoImport, ProtoIndex, ProtoMessageAst, ProtoOptionAssignment, ProtoOptionLiteral,
    ProtoReservedRange, ProtoRpcAst, ProtoSchema, ProtoSecurity, ProtoServiceAst, RlsPolicy,
    SourceSpan, SqlArtifact, StorageField, StoreOption, TimeSeriesStore, VectorStore,
};
pub use checksum::schema_checksum;
// Phase H: db_ops_sync.rs relocated to migration/sync.rs; this alias preserves
// the `crate::db_ops_sync::…` / `udb::db_ops_sync::…` paths (§9.4).
pub use crate::migration::sync as db_ops_sync;
pub use db_ops_sync::{
    BackendSyncTarget, DbOpsSyncConfig, FileSyncStatus, MigrationFileRecord, MigrationSyncReport,
    MultiBackendSyncReport, SyncError, discover_db_ops_root, sync_all_backends, sync_db_ops,
};
pub use generation::{
    CatalogManifest, DriftItem, DriftReport, DriftSeverity, DsnGenerationConfig,
    DsnValidationReport, GENERATOR_VERSION, GeneratedArtifact, LintItem, LintReport, LintSeverity,
    ManifestCheck, ManifestColumn, ManifestExtension, ManifestForeignKey, ManifestIndex,
    ManifestMaterializedView, ManifestMigrationReport, ManifestSchemaChecksum, ManifestStore,
    ManifestStoreOption, ManifestTable, ManifestTrigger, ParsedUnifiedDsn, ResolvedUnifiedDsn,
    SqlGenerationConfig, UnifiedDsn, UnifiedDsnCatalog, build_drift_report, generate_bootstrap_sql,
    generate_clickhouse_artifacts, generate_delta_sql, generate_minio_artifacts,
    generate_mongodb_artifacts, generate_mssql_artifacts, generate_mysql_artifacts,
    generate_neo4j_artifacts, generate_qdrant_artifacts, generate_redis_artifacts,
    generate_sqlite_artifacts, generate_unified_dsn_catalog, lint_catalog,
    migrate_manifest_to_current, parse_unified_dsn, redact_dsn, resolve_unified_dsn,
    resolve_unified_dsn_catalog, validate_unified_dsn,
};
pub use migration::{
    ApplyError, ApplyFuture, ApplyTarget, ArtifactApplyResult, ChangeKind, ChangeOperation,
    ChangeSafety, LedgerEntry, MigrationAuditSink, MigrationFsmState, MigrationPlan,
    MigrationPlanConfig, NoopMigrationAuditSink, ResourceAction, apply_artifacts,
    apply_artifacts_audited, build_migration_plan, diff_manifests,
};
pub use parser::{
    AnnotationParserMode, ParseReport, ParserConfig, ParserDiagnostic, UDB_ANNOTATION_VERSION,
    parse_ast_file, parse_ast_source, parse_directory, parse_directory_report, parse_file,
    parse_file_report, parse_proto_source,
};

pub use auto_alter::{LintInput, RepairDecision, RepairKind, RepairPlan, plan_repairs};
pub use backend::{
    Backend, BackendCapability, BackendCapabilityMatrixEntry, BackendConformanceReport,
    BackendKind, BackendPluginContract, BackendPluginSurface, BackendTier, CORE_BACKENDS,
};
pub use broker::{
    AuditEvent, CachePolicyPlan, CachePolicyRequest, DeletePlanRequest, GenericDispatchPlan,
    GenericDispatchRequest, ObjectAccessDecision, ObjectAccessRequest, ObjectStreamPlan,
    ObjectStreamPlanRequest, QueryPlan, RequestContext, SelectPlanRequest, SortSpec,
    SqlOperationPlan, TransactionMutation, TransactionPlan, TransactionPlanRequest,
    UpsertPlanRequest, VectorQueryPlan, VectorSearchPlanRequest, VectorUpsertPlan,
    VectorUpsertPlanRequest, build_audit_event, build_cache_policy_plan, build_delete_plan,
    build_generic_dispatch_plan, build_object_stream_plan, build_select_query_plan,
    build_transaction_plan, build_upsert_plan, build_vector_search_plan, build_vector_upsert_plan,
    evaluate_object_access, table_for_message,
};
pub use config::{
    AuditSinkConfig, AuditSinkKind, ConfigValidationReport, DbConfig, FailoverConfig,
    MigrationOptions, MinioConfig, MultiPgConfig, PgInstance, QdrantConfig, RedisConfig,
    StartupHealthGate, UdbConfig, validate_udb_config,
};
pub use embedded::EmbeddedRuntime;
pub use engine::{Engine, EngineError, FsmState, MAX_RETRIES, RuntimeStateSnapshot};
pub use executor::{ExecutionReport, FakeProvisioningExecutor, ProvisioningExecutor};
pub use lifecycle::{StartupLifecycleReport, run_startup_lifecycle};
pub use metrics::{MetricsRecorder, NoopMetrics, PrometheusMetrics, metrics_or_noop};
pub use notification::{NotificationConfig, NotificationEvent, WebhookPayload};
pub use observability::{MetricLabels, TraceContext, init_observability};
pub use plan_approval::{
    ExportedPlan, PlanMatchResult, PlanOperation, build_exported_plan, op_fingerprint,
    plan_matches_current_diff,
};
pub use provisioning::{
    BackendRequirement, ProvisioningAction, ProvisioningParameter, ProvisioningPlan,
    build_provisioning_plan, required_backends, try_build_provisioning_plan,
};
pub use runtime::core::{BackendProbeResult, EnqueueOutboxEventResult, PostgresPrivilegeReport};
pub use runtime::{DataBrokerRuntime, RuntimeInitReport};
pub use security::{
    LogSafeSecurityContext, PolicyEffect, PolicyLintFinding, SecurityContext,
    lint_policies,
};
pub use runtime::authz::{AuthzPolicy, Effect as AuthzEffect};
pub use service::{DataBrokerService, context_from_metadata, serve};
pub use signal::{AnalyticsEvent, DDL_ANALYTICS_EVENTS_DAILY, SignalConfig, common_events};
pub use status::{ErrorLogEntry, ErrorSummary, MigrationStatus};
pub use sync_config::{SyncConfig, SyncResult, SyncStatus};
pub use system::{
    SystemCatalogConfig, SystemCatalogInspection, SystemCatalogRelationStatus, SystemCatalogReport,
    default_system_catalog_ddl, system_catalog_ddl,
};
pub use tracker::{
    DDL_MIGRATION_ERROR_LOG, DDL_MIGRATION_RUNTIME_STATE, DDL_PROTO_SCHEMA_VERSIONS,
    DDL_SCHEMA_MIGRATIONS, DEFAULT_LEDGER_SCHEMA, all_tracker_ddl_sql,
    all_tracker_ddl_sql_for_schema,
};
