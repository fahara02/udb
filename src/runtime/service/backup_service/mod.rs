//! Native `BackupService` (master-plan 9.10) — tenant-level logical backup and
//! restore.
//!
//! Mirrors `lock_service`/`tenant_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. The retention policy and the per-run journal are real
//! proto entities persisted through native-entity dispatch; the backup/restore
//! ROW movement is raw, RLS-bypassing SQL on the tenant's owned tables, so it is
//! gated through the SHARED fail-closed movement validator and uses the SHARED
//! tenant-table enumeration — never a second copy of either.
//!
//! Doctrine (Phase 9 / movement-safety):
//!   * Backup/restore RPCs are gated through
//!     [`crate::runtime::tenant_movement::validate_tenant_movement_scope`] with
//!     `TenantMovementOperation::{BackupExport, RestoreImport}` — the SAME
//!     validator the startup gate uses; no bespoke scope check here.
//!   * The set of tenant-owned tables comes from the SHARED
//!     [`crate::runtime::core::tenant_purge::plan_tenant_purge`] planner (which
//!     resolves the tenant column via `generation::sql::resolve_tenant_column_ref`)
//!     — the identical enumeration `PurgeTenant` uses. Tables WITHOUT a resolvable
//!     tenant column are REPORTED as excluded in the manifest, never silently
//!     skipped (no capability lie).
//!   * Row payloads are encrypted at rest via
//!     [`DataBrokerRuntime::encrypt_secret_at_rest`] and written through the
//!     existing object-store helpers — no new crypto, no new object layer.
//!   * A restore refuses to write over a live (non-empty) target tenant
//!     (`failed_precondition`) and rewrites the tenant column to the target on
//!     insert. Cross-tenant restores require explicit privileged approval through
//!     the shared validator.
//!   * Every mutation appends a durable journal row and emits a versioned
//!     dot-topic outbox event.
//!
//! Retention pruning runs as a SchedulerService (9.3) job: a leader-elected tick
//! fires `udb.scheduler.job.fired.v1` for a due `BackupPolicy`, which a worker
//! turns into a StartTenantBackup and prunes per the policy's retention window.
//! The scheduling wiring itself lives in the scheduler lane; this module owns the
//! durable policy contract and the backup/restore mechanics.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder,
    SortDirection,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::backup::services::v1 as backup_pb;
use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::core::tenant_purge::plan_tenant_purge;
use crate::runtime::executor_utils::qi_runtime;
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

pub use crate::proto::udb::core::backup::services::v1::backup_service_server::BackupServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, NativeEventContext, admit_on as native_admit_on,
    enqueue_outbox_event_with_context, native_service_context, non_empty_json, parse_uuid,
    storage_object_defaults, validate_request_tenant,
};

const BACKUP_RUN_MSG: &str = "udb.core.backup.entity.v1.BackupRun";
const BACKUP_POLICY_MSG: &str = "udb.core.backup.entity.v1.BackupPolicy";

const TOPIC_BACKUP_COMPLETED: &str = "udb.backup.run.completed.v1";
const TOPIC_BACKUP_RESTORED: &str = "udb.backup.run.restored.v1";
const TOPIC_POLICY_UPSERTED: &str = "udb.backup.policy.upserted.v1";
const TOPIC_POLICY_DELETED: &str = "udb.backup.policy.deleted.v1";

const KIND_BACKUP: &str = "BACKUP";
const KIND_RESTORE: &str = "RESTORE";

const STATUS_COMPLETED: &str = "COMPLETED";

/// The full BackupRun column set read back into a [`backup_pb::BackupRunSummary`].
fn run_summary_fields() -> Vec<String> {
    [
        "backup_id",
        "tenant_id",
        "kind",
        "status",
        "object_prefix",
        "manifest_checksum",
        "table_count",
        "total_rows",
        "excluded_count",
        "source_tenant_id",
        "target_tenant_id",
        "created_at",
        "completed_at",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// Upper bound on rows returned by a list RPC so one call can't unbound the store.
const MAX_LIST_ROWS: u32 = 500;

/// The object suffix for the (plaintext, non-secret) run manifest: it carries
/// only schema/table names, row counts, and ciphertext checksums — the integrity
/// anchor restore reads to find and verify each encrypted table artifact.
const MANIFEST_SUFFIX: &str = "manifest.json";

fn required_backup_field(
    field: &'static str,
    value: &str,
    description: &'static str,
) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("{field} is required"),
            [(field, description)],
        ));
    }
    Ok(value.to_string())
}

/// Postgres-backed `BackupService` handler.
pub struct BackupServiceImpl {
    /// Pool for the raw tenant-row movement SQL and the outbox. Resolved from this
    /// service's `native_service` binding (the data-plane Postgres in a shared
    /// deployment), mirroring `tenant_service::PurgeTenant`.
    pg_pool: Option<PgPool>,
    /// Runtime handle: typed native-entity dispatch (journal + policy), the
    /// encrypt/decrypt-at-rest envelope, and the object-store helpers.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` disables event emission (best-effort).
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
    /// Active catalog manifest — the table set the backup/restore ripples over,
    /// enumerated via the SHARED purge planner. `None` only in bare unit-test
    /// construction (the data-movement RPCs fail closed then).
    manifest: Option<CatalogManifest>,
    /// Default object-store target when neither the request nor a policy overrides.
    object_backend: String,
    object_bucket: String,
}

fn backup_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "backup",
        operation,
        capability_required,
        message,
    )
}

fn backup_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn backup_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "backup",
        operation,
        schema_code,
        message,
    )
}

fn backup_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("backup", operation, message)
}

impl BackupServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            manifest: None,
            object_backend: DEFAULT_OBJECT_BACKEND.to_string(),
            object_bucket: DEFAULT_OBJECT_BUCKET.to_string(),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_manifest(mut self, manifest: Option<CatalogManifest>) -> Self {
        self.manifest = manifest;
        self
    }

    pub(crate) fn with_object(mut self, backend: String, bucket: String) -> Self {
        if !backend.trim().is_empty() {
            self.object_backend = backend;
        }
        if !bucket.trim().is_empty() {
            self.object_bucket = bucket;
        }
        self
    }

    /// Typed journal/policy entities + encryption + object IO all live on the
    /// runtime; fail closed when no runtime is configured.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            backup_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "backup service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    /// Raw tenant-row movement is durable-only: fail closed when no PG pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            backup_capability_status(
                "postgres_store",
                "postgres_store",
                "backup service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    /// The backup/restore ripple enumerates the same manifest table set the purge
    /// planner walks; fail closed when the manifest is absent.
    fn require_manifest(&self) -> Result<&CatalogManifest, Status> {
        self.manifest.as_ref().ok_or_else(|| {
            backup_capability_status(
                "tenant_table_enumeration",
                "catalog_manifest",
                "backup service requires the catalog manifest to enumerate tenant tables",
            )
        })
    }
}

impl Default for BackupServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

/// `"schema"."table"`, quoted via the SAME runtime identifier-quoter the purge
/// path uses. This is identifier quoting, not tenant-column resolution — the
/// resolver stays single-sourced in `plan_tenant_purge`.
fn qualified_relation(schema: &str, table: &str) -> String {
    format!("{}.{}", qi_runtime(schema), qi_runtime(table))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RestoreColumnKey {
    schema: String,
    table: String,
    column: String,
}

type RestoreValueRemaps = HashMap<RestoreColumnKey, HashMap<String, String>>;

fn manifest_table_by_relation<'a>(
    manifest: &'a CatalogManifest,
    schema: &str,
    table: &str,
) -> Option<&'a ManifestTable> {
    manifest
        .tables
        .iter()
        .find(|candidate| candidate.schema == schema && candidate.table == table)
}

fn manifest_column<'a>(table: &'a ManifestTable, column: &str) -> Option<&'a ManifestColumn> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
}

fn unique_restore_columns(table: &ManifestTable, tenant_column: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    let fk_columns: HashSet<&str> = table
        .foreign_keys
        .iter()
        .flat_map(|fk| fk.columns.iter().map(String::as_str))
        .collect();
    let mut add_column = |column: &str| {
        if column != tenant_column
            && !fk_columns.contains(column)
            && seen.insert(column.to_string())
        {
            columns.push(column.to_string());
        }
    };

    if !table
        .primary_key
        .iter()
        .any(|column| column == tenant_column)
    {
        for column in &table.primary_key {
            add_column(column);
        }
    }
    for column in &table.columns {
        if column.unique {
            add_column(&column.column_name);
        }
    }
    for index in &table.indexes {
        if index.unique && !index.columns.iter().any(|column| column == tenant_column) {
            for column in &index.columns {
                add_column(column);
            }
        }
    }

    columns
}

fn restored_unique_value(
    column: Option<&ManifestColumn>,
    target_tenant_id: &str,
    old_value: &str,
) -> String {
    let sql_type = column
        .map(|column| column.sql_type.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if sql_type.contains("uuid") && Uuid::parse_str(old_value).is_ok() {
        return Uuid::new_v4().to_string();
    }

    let target = target_tenant_id.replace('-', "");
    let nonce = Uuid::new_v4().simple().to_string();
    let mut value = format!(
        "restored-{}-{}",
        &target[..12.min(target.len())],
        &nonce[..16]
    );
    if let Some(limit) = varchar_limit(&sql_type)
        && value.len() > limit
    {
        value.truncate(limit);
    }
    value
}

fn varchar_limit(sql_type: &str) -> Option<usize> {
    let open = sql_type.find('(')?;
    let close = sql_type[open + 1..].find(')')? + open + 1;
    sql_type[open + 1..close].trim().parse().ok()
}

fn apply_parent_restore_remaps(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    remaps: &RestoreValueRemaps,
) {
    for fk in &table.foreign_keys {
        for (column, ref_column) in fk.columns.iter().zip(fk.ref_columns.iter()) {
            let Some(serde_json::Value::String(old_value)) = row.get(column) else {
                continue;
            };
            let key = RestoreColumnKey {
                schema: fk.ref_schema.clone(),
                table: fk.ref_table.clone(),
                column: ref_column.clone(),
            };
            let Some(new_value) = remaps.get(&key).and_then(|values| values.get(old_value)) else {
                continue;
            };
            row.insert(column.clone(), serde_json::Value::String(new_value.clone()));
        }
    }
}

fn apply_cross_tenant_restore_remaps(
    row: &mut serde_json::Map<String, serde_json::Value>,
    table: &ManifestTable,
    tenant_column: &str,
    target_tenant_id: &str,
    remaps: &mut RestoreValueRemaps,
) {
    for column in unique_restore_columns(table, tenant_column) {
        let Some(serde_json::Value::String(old_value)) = row.get(&column) else {
            continue;
        };
        let key = RestoreColumnKey {
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.clone(),
        };
        let values = remaps.entry(key).or_default();
        let new_value = values
            .entry(old_value.clone())
            .or_insert_with(|| {
                restored_unique_value(manifest_column(table, &column), target_tenant_id, old_value)
            })
            .clone();
        row.insert(column, serde_json::Value::String(new_value));
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

/// Refuse to restore over a live tenant: a target that already owns ANY row
/// fails closed. Pure — unit-tested without Postgres (the handler supplies the
/// probed row count).
fn ensure_target_is_fresh(existing_rows: u64) -> Result<(), Status> {
    if existing_rows > 0 {
        return Err(restore_target_not_fresh_status(existing_rows));
    }
    Ok(())
}

fn restore_target_not_fresh_status(existing_rows: u64) -> Status {
    backup_policy_status(
        "restore_tenant",
        "restore_target_not_fresh",
        format!(
            "restore target tenant already holds {existing_rows} row(s); restoring over a live tenant is refused — use a fresh tenant id"
        ),
    )
}

fn backup_run_missing_object_prefix_status() -> Status {
    backup_policy_status(
        "restore_tenant",
        "backup_run_missing_object_prefix",
        "backup run has no object prefix to restore from",
    )
}

// ── JSON row decode helpers (native read rows arrive as serde_json::Value) ─────

fn row_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    match row.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::String(value)) => {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "true" | "1" | "t"
            )
        }
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0) != 0,
        _ => false,
    }
}

/// Best-effort RFC3339/string → unix seconds (timestamps round-trip as strings).
fn json_unix_ts(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => DateTime::parse_from_rfc3339(value.trim())
            .map(|dt| dt.timestamp())
            .unwrap_or(0),
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        _ => 0,
    }
}

fn run_summary_from_json(row: &serde_json::Value) -> backup_pb::BackupRunSummary {
    let m = row_object(row);
    backup_pb::BackupRunSummary {
        backup_id: json_str(m, "backup_id"),
        tenant_id: json_str(m, "tenant_id"),
        kind: json_str(m, "kind"),
        status: json_str(m, "status"),
        object_prefix: json_str(m, "object_prefix"),
        manifest_checksum: json_str(m, "manifest_checksum"),
        table_count: json_i64(m, "table_count") as i32,
        total_rows: json_i64(m, "total_rows"),
        excluded_count: json_i64(m, "excluded_count") as i32,
        source_tenant_id: json_str(m, "source_tenant_id"),
        target_tenant_id: json_str(m, "target_tenant_id"),
        created_at_unix: json_unix_ts(m, "created_at"),
        completed_at_unix: json_unix_ts(m, "completed_at"),
    }
}

fn policy_view_from_json(row: &serde_json::Value) -> backup_pb::BackupPolicyView {
    let m = row_object(row);
    backup_pb::BackupPolicyView {
        policy_id: json_str(m, "policy_id"),
        tenant_id: json_str(m, "tenant_id"),
        policy_name: json_str(m, "policy_name"),
        schedule_cron: json_str(m, "schedule_cron"),
        retention_days: json_i64(m, "retention_days") as i32,
        max_retained_backups: json_i64(m, "max_retained_backups") as i32,
        enabled: json_bool(m, "enabled"),
        object_backend: json_str(m, "object_backend"),
        object_bucket: json_str(m, "object_bucket"),
        created_at_unix: json_unix_ts(m, "created_at"),
        updated_at_unix: json_unix_ts(m, "updated_at"),
    }
}

// ── native-entity reads (journal + policy) ───────────────────────────────────

fn run_read_by_id(tenant_id: &str, backup_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_RUN_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "backup_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(backup_id),
            },
        ])),
        projection: Some(LogicalProjection::fields(run_summary_fields())),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn runs_list_read(tenant_id: &str, kind: Option<&str>, limit: u32, offset: u64) -> LogicalRead {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(kind) = kind {
        filters.push(LogicalFilter::Comparison {
            field: "kind".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(kind),
        });
    }
    LogicalRead {
        message_type: BACKUP_RUN_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields(run_summary_fields())),
        sort: vec![LogicalSort {
            field: "created_at".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination {
            limit: Some(limit),
            offset: Some(offset),
            ..LogicalPagination::default()
        }),
    }
}

/// The full BackupPolicy column set read back into a [`backup_pb::BackupPolicyView`].
fn policy_view_fields() -> Vec<String> {
    [
        "policy_id",
        "tenant_id",
        "policy_name",
        "schedule_cron",
        "retention_days",
        "max_retained_backups",
        "enabled",
        "object_backend",
        "object_bucket",
        "created_at",
        "updated_at",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn policy_read_by_name(tenant_id: &str, policy_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_POLICY_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "policy_name".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(policy_name),
            },
        ])),
        projection: Some(LogicalProjection::fields(policy_view_fields())),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn policies_list_read(tenant_id: &str, limit: u32, offset: u64) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_POLICY_MSG.to_string(),
        filter: Some(LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        }),
        projection: Some(LogicalProjection::fields(policy_view_fields())),
        sort: vec![LogicalSort {
            field: "policy_name".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination {
            limit: Some(limit),
            offset: Some(offset),
            ..LogicalPagination::default()
        }),
    }
}

fn policy_conflict() -> ConflictStrategy {
    ConflictStrategy::update_on(
        vec![
            "schedule_cron".to_string(),
            "retention_days".to_string(),
            "max_retained_backups".to_string(),
            "enabled".to_string(),
            "object_backend".to_string(),
            "object_bucket".to_string(),
            "updated_at".to_string(),
            "metadata_json".to_string(),
        ],
        vec!["tenant_id".to_string(), "policy_name".to_string()],
    )
}

impl BackupServiceImpl {
    /// Resolve the object-store target for a run: an explicit request override
    /// wins, else the service default (which `build_backup_service` seeds from
    /// `UDB_STORAGE_OBJECT_BACKEND`/`UDB_STORAGE_BUCKET`).
    fn resolve_object_target(&self, backend: &str, bucket: &str) -> (String, String) {
        let backend = if backend.trim().is_empty() {
            self.object_backend.clone()
        } else {
            backend.trim().to_string()
        };
        let bucket = if bucket.trim().is_empty() {
            self.object_bucket.clone()
        } else {
            bucket.trim().to_string()
        };
        (backend, bucket)
    }

    /// Append a journal row (best-effort durability via native-entity dispatch).
    #[allow(clippy::too_many_arguments)]
    async fn journal_run(
        &self,
        runtime: &DataBrokerRuntime,
        context: &crate::RequestContext,
        backup_id: &str,
        tenant_id: &str,
        kind: &str,
        object_prefix: &str,
        manifest_checksum: &str,
        table_count: i64,
        total_rows: i64,
        excluded_count: i64,
        source_tenant_id: &str,
        target_tenant_id: &str,
    ) -> Result<(), Status> {
        let now = Utc::now();
        let mut record = LogicalRecord::new();
        record.insert("backup_id".to_string(), logical_string(backup_id));
        record.insert("tenant_id".to_string(), logical_string(tenant_id));
        record.insert("kind".to_string(), logical_string(kind));
        record.insert("status".to_string(), logical_string(STATUS_COMPLETED));
        record.insert("object_prefix".to_string(), logical_string(object_prefix));
        record.insert(
            "manifest_checksum".to_string(),
            logical_string(manifest_checksum),
        );
        record.insert("table_count".to_string(), LogicalValue::Int(table_count));
        record.insert("total_rows".to_string(), LogicalValue::Int(total_rows));
        record.insert(
            "excluded_count".to_string(),
            LogicalValue::Int(excluded_count),
        );
        record.insert(
            "source_tenant_id".to_string(),
            logical_string(source_tenant_id),
        );
        record.insert(
            "target_tenant_id".to_string(),
            logical_string(target_tenant_id),
        );
        record.insert("error_message".to_string(), logical_string(""));
        record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
        record.insert("completed_at".to_string(), LogicalValue::Timestamp(now));
        record.insert("metadata_json".to_string(), logical_string("{}"));
        runtime
            .native_entity_write_for_service(
                "backup",
                context,
                BACKUP_RUN_MSG,
                record,
                ConflictStrategy::update(vec![
                    "status".to_string(),
                    "object_prefix".to_string(),
                    "manifest_checksum".to_string(),
                    "table_count".to_string(),
                    "total_rows".to_string(),
                    "excluded_count".to_string(),
                    "completed_at".to_string(),
                ]),
            )
            .await
            .map(|_| ())
    }

    /// Emit a per-mutation versioned dot-topic outbox event (best-effort).
    async fn emit_event(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
        project_id: &str,
        target_resource: &str,
        payload: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                target_resource: target_resource.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

#[tonic::async_trait]
impl BackupService for BackupServiceImpl {
    async fn start_tenant_backup(
        &self,
        request: Request<backup_pb::StartTenantBackupRequest>,
    ) -> Result<Response<backup_pb::StartTenantBackupResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes the body value IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        if tenant_id.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "tenant_id is required",
                [("tenant_id", "must be a non-empty tenant id")],
            ));
        }

        // SHARED fail-closed movement validator — never a bespoke scope check. A
        // backup always filters by the verified tenant (tenant_filter_present),
        // and is single-tenant (no target), so this fails closed on an unscoped
        // export exactly as the broker startup gate does.
        let movement = TenantMovementRequest {
            operation: TenantMovementOperation::BackupExport,
            tenant_id: &tenant_id,
            target_tenant_id: None,
            tenant_filter_present: true,
            privileged_cross_tenant: false,
        };
        validate_tenant_movement_scope(&movement)
            .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let pool = self.require_pool()?;
        let manifest = self.require_manifest()?;
        // Canonical tenant id (and a value that matches the `::text` filter).
        let _ = parse_uuid("tenant_id", &tenant_id)?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let (object_backend, object_bucket) =
            self.resolve_object_target(&req.object_backend, &req.object_bucket);

        // SHARED enumeration: the same planner PurgeTenant uses. Tenant-owned
        // tables are `targets`; tenant-less tables are REPORTED in `excluded`.
        let plan = plan_tenant_purge(manifest);
        let excluded_count = plan.excluded.len() as i64;

        let backup_id = Uuid::new_v4().to_string();
        let started = Utc::now();
        let object_prefix = format!(
            "backups/{}/{}-{}/",
            tenant_id,
            started.format("%Y%m%dT%H%M%SZ"),
            &backup_id[..8.min(backup_id.len())]
        );

        let mut table_entries: Vec<backup_pb::BackupTableEntry> = Vec::new();
        let mut total_rows: i64 = 0;
        for target in &plan.targets {
            let rel = qualified_relation(&target.schema, &target.table);
            // Compare the tenant column as text so a UUID column and a VARCHAR
            // tenant column both match the bound tenant id (no text-cast trap).
            let select_sql = format!(
                "SELECT row_to_json(t)::text FROM {rel} t WHERE {col}::text = $1",
                col = qi_runtime(&target.tenant_column),
            );
            let rows: Vec<String> = sqlx::query_scalar(&select_sql)
                .bind(&tenant_id)
                .fetch_all(pool)
                .await
                .map_err(|err| {
                    backup_internal_status(
                        "start_backup_read_table",
                        format!(
                            "backup failed reading {}.{}: {err}",
                            target.schema, target.table
                        ),
                    )
                })?;
            let row_count = rows.len() as i64;
            total_rows += row_count;
            let jsonl = rows.join("\n");
            // Encrypt the table's rows at rest via the SHARED envelope helper.
            let ciphertext = runtime.encrypt_secret_at_rest(&jsonl).map_err(|err| {
                backup_internal_status(
                    "start_backup_encrypt_artifact",
                    format!("backup encryption failed: {err}"),
                )
            })?;
            let bytes = ciphertext.into_bytes();
            let checksum = sha256_hex(&bytes);
            let object_key = format!(
                "{object_prefix}{}.{}.jsonl.enc",
                target.schema, target.table
            );
            let put_req = crate::runtime::core::setup_data::object_request_json(
                "put",
                &object_bucket,
                &object_key,
                "application/octet-stream",
            );
            runtime
                .put_object_backend_target_for_project(
                    &object_backend,
                    None,
                    &context.project_id,
                    &put_req,
                    bytes,
                )
                .await?;
            table_entries.push(backup_pb::BackupTableEntry {
                schema: target.schema.clone(),
                table: target.table.clone(),
                tenant_column: target.tenant_column.clone(),
                object_key,
                row_count,
                checksum_sha256: checksum,
            });
        }

        let excluded: Vec<backup_pb::BackupExcludedTable> = plan
            .excluded
            .iter()
            .map(|e| backup_pb::BackupExcludedTable {
                schema: e.schema.clone(),
                table: e.table.clone(),
                reason: e.reason.clone(),
            })
            .collect();

        // Checksummed run manifest (plaintext: schema/table names + ciphertext
        // checksums, no row data). Excluded tables are recorded here so an
        // operator sees exactly what the backup does NOT cover — never a silent
        // skip.
        let manifest_value = serde_json::json!({
            "backup_id": backup_id,
            "tenant_id": tenant_id,
            "created_at": started.to_rfc3339(),
            "object_prefix": object_prefix,
            "object_backend": object_backend,
            "object_bucket": object_bucket,
            "encrypted": true,
            "fk_ordered": plan.fk_ordered,
            "tables": table_entries.iter().map(|t| serde_json::json!({
                "schema": t.schema,
                "table": t.table,
                "tenant_column": t.tenant_column,
                "object_key": t.object_key,
                "row_count": t.row_count,
                "checksum_sha256": t.checksum_sha256,
            })).collect::<Vec<_>>(),
            "excluded": excluded.iter().map(|e| serde_json::json!({
                "schema": e.schema,
                "table": e.table,
                "reason": e.reason,
            })).collect::<Vec<_>>(),
        });
        let manifest_bytes = serde_json::to_vec(&manifest_value).map_err(|err| {
            backup_internal_status(
                "start_backup_serialize_manifest",
                format!("backup manifest serialize failed: {err}"),
            )
        })?;
        let manifest_checksum = sha256_hex(&manifest_bytes);
        let manifest_key = format!("{object_prefix}{MANIFEST_SUFFIX}");
        let manifest_put = crate::runtime::core::setup_data::object_request_json(
            "put",
            &object_bucket,
            &manifest_key,
            "application/json",
        );
        runtime
            .put_object_backend_target_for_project(
                &object_backend,
                None,
                &context.project_id,
                &manifest_put,
                manifest_bytes,
            )
            .await?;

        let table_count = table_entries.len() as i64;
        self.journal_run(
            runtime,
            &context,
            &backup_id,
            &tenant_id,
            KIND_BACKUP,
            &object_prefix,
            &manifest_checksum,
            table_count,
            total_rows,
            excluded_count,
            "",
            "",
        )
        .await?;

        self.emit_event(
            TOPIC_BACKUP_COMPLETED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            &backup_id,
            serde_json::json!({
                "backup_id": backup_id,
                "tenant_id": tenant_id,
                "object_prefix": object_prefix,
                "manifest_checksum": manifest_checksum,
                "table_count": table_count,
                "total_rows": total_rows,
                "excluded_count": excluded_count,
            }),
        )
        .await;

        Ok(Response::new(backup_pb::StartTenantBackupResponse {
            backup_id,
            object_prefix,
            manifest_checksum,
            table_count: table_count as i32,
            total_rows,
            excluded_count: excluded_count as i32,
            tables: table_entries,
            excluded,
            message: "tenant backup completed".to_string(),
            error: None,
        }))
    }

    async fn restore_tenant(
        &self,
        request: Request<backup_pb::RestoreTenantRequest>,
    ) -> Result<Response<backup_pb::RestoreTenantResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let source_tenant_id = req.source_tenant_id.trim().to_string();
        let target_tenant_id = req.target_tenant_id.trim().to_string();
        let backup_id = req.backup_id.trim().to_string();
        if source_tenant_id.is_empty() || target_tenant_id.is_empty() || backup_id.is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "source_tenant_id, target_tenant_id and backup_id are required",
                [
                    ("source_tenant_id", "must be a non-empty source tenant id"),
                    ("target_tenant_id", "must be a non-empty target tenant id"),
                    ("backup_id", "must be a non-empty backup id"),
                ],
            ));
        }
        // Same-tenant restore acts on the target tenant. Cross-tenant restore
        // into a fresh target has no target principal yet, so authorize the
        // source tenant and require the explicit movement gate below.
        let auth_tenant_id = if req.allow_cross_tenant {
            &source_tenant_id
        } else {
            &target_tenant_id
        };
        validate_request_tenant(&metadata, auth_tenant_id)?;
        // DESTRUCTIVE: a missing confirmation token fails CLOSED.
        if req.confirmation_token.trim().is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "RestoreTenant overwrites a tenant's data; confirmation_token is required",
                [(
                    "confirmation_token",
                    "must be present to restore over tenant data",
                )],
            ));
        }

        // SHARED fail-closed movement validator — RestoreImport. A target that
        // differs from the source is a cross-tenant move and requires explicit
        // privileged approval; otherwise this returns an error and we fail closed.
        let movement = TenantMovementRequest {
            operation: TenantMovementOperation::RestoreImport,
            tenant_id: &source_tenant_id,
            target_tenant_id: Some(&target_tenant_id),
            tenant_filter_present: true,
            privileged_cross_tenant: req.allow_cross_tenant,
        };
        validate_tenant_movement_scope(&movement)
            .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

        let runtime = self.require_runtime()?;
        let pool = self.require_pool()?;
        let manifest = self.require_manifest()?;
        let _ = parse_uuid("source_tenant_id", &source_tenant_id)?;
        let _ = parse_uuid("target_tenant_id", &target_tenant_id)?;
        let context = native_service_context(&metadata, &target_tenant_id, "");

        // Resolve the source run's object prefix from the durable journal.
        let source_ctx = native_service_context(&metadata, &source_tenant_id, "");
        let run = runtime
            .native_entity_read_for_service(
                "backup",
                &source_ctx,
                run_read_by_id(&source_tenant_id, &backup_id),
            )
            .await?
            .first()
            .map(run_summary_from_json)
            .ok_or_else(|| {
                backup_not_found_status(
                    "restore_tenant",
                    "backup_run_not_found",
                    "backup run not found for source tenant",
                )
            })?;
        let object_prefix = run.object_prefix.trim().to_string();
        if object_prefix.is_empty() {
            return Err(backup_run_missing_object_prefix_status());
        }

        // Enumerate the tenant-owned tables via the SHARED planner.
        let plan = plan_tenant_purge(manifest);

        // FRESH-target guard: refuse to write over a live tenant. Probe each
        // tenant-owned table for ANY existing row under the target tenant.
        let mut existing_rows: u64 = 0;
        for target in &plan.targets {
            let rel = qualified_relation(&target.schema, &target.table);
            let probe_sql = format!(
                "SELECT 1 FROM {rel} WHERE {col}::text = $1 LIMIT 1",
                col = qi_runtime(&target.tenant_column),
            );
            let present: Option<i32> = sqlx::query_scalar(&probe_sql)
                .bind(&target_tenant_id)
                .fetch_optional(pool)
                .await
                .map_err(|err| {
                    backup_internal_status(
                        "restore_freshness_probe",
                        format!(
                            "restore freshness probe failed on {}.{}: {err}",
                            target.schema, target.table
                        ),
                    )
                })?;
            if present.is_some() {
                existing_rows += 1;
            }
        }
        ensure_target_is_fresh(existing_rows)?;

        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Admin,
            &target_tenant_id,
            None,
        )
        .await?;

        // Load + verify the run manifest (object backend/bucket are recorded in
        // it). The manifest itself lives under the service default object target
        // (where the backup wrote it); the per-table artifacts then use whatever
        // backend/bucket the manifest records.
        let manifest_key = format!("{object_prefix}{MANIFEST_SUFFIX}");
        let run_backend = self.object_backend.clone();
        let run_bucket = self.object_bucket.clone();
        let manifest_get = crate::runtime::core::setup_data::object_request_json(
            "get",
            &run_bucket,
            &manifest_key,
            "",
        );
        let manifest_bytes = runtime
            .get_object_backend_target_for_project(
                &run_backend,
                None,
                &context.project_id,
                &manifest_get,
            )
            .await?;
        let manifest_value: serde_json::Value =
            serde_json::from_slice(&manifest_bytes).map_err(|err| {
                backup_internal_status(
                    "restore_manifest_parse",
                    format!("restore manifest parse failed: {err}"),
                )
            })?;
        let object_backend = manifest_value
            .get("object_backend")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string)
            .unwrap_or(run_backend);
        let object_bucket = manifest_value
            .get("object_bucket")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string)
            .unwrap_or(run_bucket);
        let manifest_tables = manifest_value
            .get("tables")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        // Restore in REVERSE planner order: the planner emits children→parents
        // (safe for delete); inserting parents→children satisfies FK constraints.
        let mut ordered_tables = manifest_tables;
        ordered_tables.reverse();

        let mut restored_rows: i64 = 0;
        let mut restored_table_count: i32 = 0;
        let cross_tenant_restore = source_tenant_id != target_tenant_id;
        let mut restore_remaps: RestoreValueRemaps = HashMap::new();
        let mut tx = pool.begin().await.map_err(|err| {
            backup_internal_status(
                "restore_transaction_begin",
                format!("failed to begin restore transaction: {err}"),
            )
        })?;
        for entry in &ordered_tables {
            let schema = entry.get("schema").and_then(|v| v.as_str()).unwrap_or("");
            let table = entry.get("table").and_then(|v| v.as_str()).unwrap_or("");
            let tenant_column = entry
                .get("tenant_column")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let object_key = entry
                .get("object_key")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let expected_checksum = entry
                .get("checksum_sha256")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if schema.is_empty() || table.is_empty() || object_key.is_empty() {
                continue;
            }
            let manifest_table = manifest_table_by_relation(manifest, schema, table);
            let get_req = crate::runtime::core::setup_data::object_request_json(
                "get",
                &object_bucket,
                object_key,
                "",
            );
            let bytes = runtime
                .get_object_backend_target_for_project(
                    &object_backend,
                    None,
                    &context.project_id,
                    &get_req,
                )
                .await?;
            // Integrity: the encrypted artifact must match the manifest checksum.
            if !expected_checksum.is_empty() && sha256_hex(&bytes) != expected_checksum {
                return Err(Status::data_loss(format!(
                    "restore integrity check failed for {schema}.{table} (checksum mismatch)"
                )));
            }
            let ciphertext = String::from_utf8(bytes).map_err(|err| {
                backup_internal_status(
                    "restore_artifact_utf8",
                    format!("restore artifact is not valid UTF-8: {err}"),
                )
            })?;
            let jsonl = runtime.decrypt_secret_at_rest(&ciphertext).map_err(|err| {
                backup_internal_status(
                    "restore_decrypt_artifact",
                    format!("restore decryption failed: {err}"),
                )
            })?;
            let rel = qualified_relation(schema, table);
            // `jsonb_populate_record` casts the row JSON into the table's row type,
            // so every column type is handled by Postgres without hand-mapping.
            let insert_sql = format!(
                "INSERT INTO {rel} SELECT (jsonb_populate_record(NULL::{rel}, $1::jsonb)).*"
            );
            for line in jsonl.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let mut value: serde_json::Value = serde_json::from_str(line).map_err(|err| {
                    backup_internal_status(
                        "restore_row_parse",
                        format!("restore row parse failed for {schema}.{table}: {err}"),
                    )
                })?;
                // Rewrite the tenant column to the target on insert.
                if let Some(obj) = value.as_object_mut()
                    && !tenant_column.is_empty()
                {
                    if cross_tenant_restore && let Some(table_meta) = manifest_table {
                        apply_parent_restore_remaps(obj, table_meta, &restore_remaps);
                    }
                    obj.insert(
                        tenant_column.to_string(),
                        serde_json::Value::String(target_tenant_id.clone()),
                    );
                    if cross_tenant_restore && let Some(table_meta) = manifest_table {
                        apply_cross_tenant_restore_remaps(
                            obj,
                            table_meta,
                            tenant_column,
                            &target_tenant_id,
                            &mut restore_remaps,
                        );
                    }
                }
                // Bind the row as text and cast to jsonb in SQL ($1::jsonb), so the
                // bind never depends on the sqlx json feature being enabled.
                let row_json = serde_json::to_string(&value).map_err(|err| {
                    backup_internal_status(
                        "restore_row_reserialize",
                        format!("restore row reserialize failed: {err}"),
                    )
                })?;
                sqlx::query(&insert_sql)
                    .bind(row_json)
                    .execute(&mut *tx)
                    .await
                    .map_err(|err| {
                        backup_internal_status(
                            "restore_insert_row",
                            format!("restore insert failed for {schema}.{table}: {err}"),
                        )
                    })?;
                restored_rows += 1;
            }
            restored_table_count += 1;
        }
        tx.commit().await.map_err(|err| {
            backup_internal_status(
                "restore_transaction_commit",
                format!("failed to commit restore transaction: {err}"),
            )
        })?;

        let restore_id = Uuid::new_v4().to_string();
        self.journal_run(
            runtime,
            &context,
            &restore_id,
            &target_tenant_id,
            KIND_RESTORE,
            &object_prefix,
            &run.manifest_checksum,
            restored_table_count as i64,
            restored_rows,
            0,
            &source_tenant_id,
            &target_tenant_id,
        )
        .await?;

        self.emit_event(
            TOPIC_BACKUP_RESTORED,
            &target_tenant_id,
            &target_tenant_id,
            &context.project_id,
            &restore_id,
            serde_json::json!({
                "backup_id": restore_id,
                "source_backup_id": backup_id,
                "source_tenant_id": source_tenant_id,
                "target_tenant_id": target_tenant_id,
                "object_prefix": object_prefix,
                "restored_table_count": restored_table_count,
                "restored_rows": restored_rows,
            }),
        )
        .await;

        Ok(Response::new(backup_pb::RestoreTenantResponse {
            backup_id: restore_id,
            source_object_prefix: object_prefix,
            restored_table_count,
            restored_rows,
            message: "tenant restored".to_string(),
            error: None,
        }))
    }

    async fn list_backups(
        &self,
        request: Request<backup_pb::ListBackupsRequest>,
    ) -> Result<Response<backup_pb::ListBackupsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let limit = clamp_limit(req.page_size);
        let offset = parse_offset(&req.page_token);
        let kind = match req.kind.trim() {
            "" => None,
            other => Some(other),
        };
        let rows = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                runs_list_read(&tenant_id, kind, limit, offset),
            )
            .await?;
        let backups: Vec<backup_pb::BackupRunSummary> =
            rows.iter().map(run_summary_from_json).collect();
        let next = next_page_token(offset, limit, backups.len());
        Ok(Response::new(backup_pb::ListBackupsResponse {
            backups,
            next_page_token: next,
            error: None,
        }))
    }

    async fn get_backup(
        &self,
        request: Request<backup_pb::GetBackupRequest>,
    ) -> Result<Response<backup_pb::GetBackupResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let backup_id =
            required_backup_field("backup_id", &req.backup_id, "must be a non-empty backup id")?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let run = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                run_read_by_id(&tenant_id, &backup_id),
            )
            .await?
            .first()
            .map(run_summary_from_json)
            .ok_or_else(|| {
                backup_not_found_status(
                    "get_backup",
                    "backup_run_not_found",
                    "backup run not found",
                )
            })?;

        // Best-effort: read the run manifest for per-table detail. A missing
        // manifest (e.g. object store offline) still returns the journal summary.
        let (mut tables, mut excluded) = (Vec::new(), Vec::new());
        if !run.object_prefix.trim().is_empty() {
            let manifest_key = format!("{}{MANIFEST_SUFFIX}", run.object_prefix);
            let get_req = crate::runtime::core::setup_data::object_request_json(
                "get",
                &self.object_bucket,
                &manifest_key,
                "",
            );
            if let Ok(bytes) = runtime
                .get_object_backend_target_for_project(
                    &self.object_backend,
                    None,
                    &context.project_id,
                    &get_req,
                )
                .await
                && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                if let Some(arr) = value.get("tables").and_then(|v| v.as_array()) {
                    tables = arr
                        .iter()
                        .map(|t| backup_pb::BackupTableEntry {
                            schema: t
                                .get("schema")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            table: t
                                .get("table")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            tenant_column: t
                                .get("tenant_column")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            object_key: t
                                .get("object_key")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            row_count: t.get("row_count").and_then(|v| v.as_i64()).unwrap_or(0),
                            checksum_sha256: t
                                .get("checksum_sha256")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        })
                        .collect();
                }
                if let Some(arr) = value.get("excluded").and_then(|v| v.as_array()) {
                    excluded = arr
                        .iter()
                        .map(|e| backup_pb::BackupExcludedTable {
                            schema: e
                                .get("schema")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            table: e
                                .get("table")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            reason: e
                                .get("reason")
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                        })
                        .collect();
                }
            }
        }

        Ok(Response::new(backup_pb::GetBackupResponse {
            backup: Some(run),
            tables,
            excluded,
            error: None,
        }))
    }

    async fn put_backup_policy(
        &self,
        request: Request<backup_pb::PutBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::PutBackupPolicyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let policy_name = required_backup_field(
            "policy_name",
            &req.policy_name,
            "must be a non-empty policy name",
        )?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        // Reuse the existing policy id on update so the upsert is in place.
        let existing = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                policy_read_by_name(&tenant_id, &policy_name),
            )
            .await?;
        let policy_id = existing
            .first()
            .map(|row| json_str(row_object(row), "policy_id"))
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let now = Utc::now();
        let mut record = LogicalRecord::new();
        record.insert("policy_id".to_string(), logical_string(&policy_id));
        record.insert("tenant_id".to_string(), logical_string(&tenant_id));
        record.insert("policy_name".to_string(), logical_string(&policy_name));
        record.insert(
            "schedule_cron".to_string(),
            logical_string(req.schedule_cron.trim()),
        );
        record.insert(
            "retention_days".to_string(),
            LogicalValue::Int(i64::from(req.retention_days.max(0))),
        );
        record.insert(
            "max_retained_backups".to_string(),
            LogicalValue::Int(i64::from(req.max_retained_backups.max(0))),
        );
        record.insert("enabled".to_string(), LogicalValue::Bool(req.enabled));
        record.insert(
            "object_backend".to_string(),
            logical_string(req.object_backend.trim()),
        );
        record.insert(
            "object_bucket".to_string(),
            logical_string(req.object_bucket.trim()),
        );
        record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
        record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
        record.insert(
            "metadata_json".to_string(),
            logical_string(non_empty_json(&req.metadata_json)),
        );

        runtime
            .native_entity_write_for_service(
                "backup",
                &context,
                BACKUP_POLICY_MSG,
                record,
                policy_conflict(),
            )
            .await?;

        self.emit_event(
            TOPIC_POLICY_UPSERTED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            &policy_name,
            serde_json::json!({
                "tenant_id": tenant_id,
                "policy_id": policy_id,
                "policy_name": policy_name,
                "enabled": req.enabled,
                "retention_days": req.retention_days,
            }),
        )
        .await;

        Ok(Response::new(backup_pb::PutBackupPolicyResponse {
            policy_id,
            message: "backup policy saved".to_string(),
            error: None,
        }))
    }

    async fn get_backup_policy(
        &self,
        request: Request<backup_pb::GetBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::GetBackupPolicyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let policy_name = required_backup_field(
            "policy_name",
            &req.policy_name,
            "must be a non-empty policy name",
        )?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let policy = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                policy_read_by_name(&tenant_id, &policy_name),
            )
            .await?
            .first()
            .map(policy_view_from_json)
            .ok_or_else(|| {
                backup_not_found_status(
                    "get_backup_policy",
                    "backup_policy_not_found",
                    "backup policy not found",
                )
            })?;
        Ok(Response::new(backup_pb::GetBackupPolicyResponse {
            policy: Some(policy),
            error: None,
        }))
    }

    async fn list_backup_policies(
        &self,
        request: Request<backup_pb::ListBackupPoliciesRequest>,
    ) -> Result<Response<backup_pb::ListBackupPoliciesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let limit = clamp_limit(req.page_size);
        let offset = parse_offset(&req.page_token);
        let rows = runtime
            .native_entity_read_for_service(
                "backup",
                &context,
                policies_list_read(&tenant_id, limit, offset),
            )
            .await?;
        let policies: Vec<backup_pb::BackupPolicyView> =
            rows.iter().map(policy_view_from_json).collect();
        let next = next_page_token(offset, limit, policies.len());
        Ok(Response::new(backup_pb::ListBackupPoliciesResponse {
            policies,
            next_page_token: next,
            error: None,
        }))
    }

    async fn delete_backup_policy(
        &self,
        request: Request<backup_pb::DeleteBackupPolicyRequest>,
    ) -> Result<Response<backup_pb::DeleteBackupPolicyResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let policy_name = required_backup_field(
            "policy_name",
            &req.policy_name,
            "must be a non-empty policy name",
        )?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "backup",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        runtime
            .native_entity_delete_for_service(
                "backup",
                &context,
                LogicalDelete {
                    message_type: BACKUP_POLICY_MSG.to_string(),
                    filter: LogicalFilter::And(vec![
                        LogicalFilter::Comparison {
                            field: "tenant_id".to_string(),
                            op: ComparisonOp::Eq,
                            value: logical_string(&tenant_id),
                        },
                        LogicalFilter::Comparison {
                            field: "policy_name".to_string(),
                            op: ComparisonOp::Eq,
                            value: logical_string(&policy_name),
                        },
                    ]),
                    return_fields: Vec::new(),
                },
            )
            .await?;

        self.emit_event(
            TOPIC_POLICY_DELETED,
            &tenant_id,
            &tenant_id,
            &context.project_id,
            &policy_name,
            serde_json::json!({ "tenant_id": tenant_id, "policy_name": policy_name }),
        )
        .await;

        Ok(Response::new(backup_pb::DeleteBackupPolicyResponse {
            deleted: true,
            message: "backup policy deleted".to_string(),
            error: None,
        }))
    }
}

fn clamp_limit(page_size: i32) -> u32 {
    if page_size <= 0 {
        100
    } else {
        (page_size as u32).min(MAX_LIST_ROWS)
    }
}

fn parse_offset(page_token: &str) -> u64 {
    page_token.trim().parse::<u64>().unwrap_or(0)
}

fn next_page_token(offset: u64, limit: u32, returned: usize) -> String {
    if returned as u32 >= limit {
        (offset + returned as u64).to_string()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod backup_tests {
    use super::*;
    use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable};
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_schema_not_found_detail(
        status: &Status,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "backup");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "backup");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn column(name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            ..ManifestColumn::default()
        }
    }

    fn table(schema: &str, name: &str, columns: Vec<ManifestColumn>) -> ManifestTable {
        ManifestTable {
            schema: schema.to_string(),
            table: name.to_string(),
            columns,
            ..ManifestTable::default()
        }
    }

    /// The backup PLAN must reuse the SHARED purge planner: tenant-columned
    /// tables become targets, and a tenant-less table is REPORTED as excluded
    /// with a human reason — never silently skipped (the capability-lie pattern).
    #[test]
    fn backup_plan_reports_excluded_not_skipped() {
        let mut owned = table("app", "invoice", vec![column("id"), column("tenant_id")]);
        owned.table_security.tenant_column = "tenant_id".to_string();
        let tenantless = table("app", "currency_rate", vec![column("id"), column("rate")]);
        let manifest = CatalogManifest {
            tables: vec![owned, tenantless],
            ..CatalogManifest::default()
        };

        let plan = plan_tenant_purge(&manifest);
        let targets: Vec<&str> = plan.targets.iter().map(|t| t.table.as_str()).collect();
        assert!(
            targets.contains(&"invoice"),
            "tenant table is a backup target"
        );
        assert!(
            !targets.contains(&"currency_rate"),
            "a tenant-less table is never backed up as tenant data"
        );
        assert_eq!(plan.excluded.len(), 1, "the tenant-less table is reported");
        assert_eq!(plan.excluded[0].table, "currency_rate");
        assert!(
            !plan.excluded[0].reason.trim().is_empty(),
            "excluded tables must carry a human reason, not be silently dropped"
        );
    }

    /// Restoring over a live tenant fails closed; a fresh (empty) target passes.
    /// Pure check — no Postgres.
    #[test]
    fn restore_over_existing_tenant_is_rejected() {
        let err = ensure_target_is_fresh(3).expect_err("a non-empty target must be refused");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "restore target tenant already holds 3 row(s); restoring over a live tenant is refused — use a fresh tenant id"
        );
        assert_policy_detail(&err, "restore_tenant", "restore_target_not_fresh");
        ensure_target_is_fresh(0).expect("a fresh target must be allowed");
    }

    #[test]
    fn backup_run_missing_object_prefix_carries_policy_detail() {
        let err = backup_run_missing_object_prefix_status();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "backup run has no object prefix to restore from"
        );
        assert_policy_detail(&err, "restore_tenant", "backup_run_missing_object_prefix");
    }

    #[test]
    fn backup_not_found_statuses_carry_schema_detail() {
        for (operation, schema_code, message) in [
            (
                "restore_tenant",
                "backup_run_not_found",
                "backup run not found for source tenant",
            ),
            ("get_backup", "backup_run_not_found", "backup run not found"),
            (
                "get_backup_policy",
                "backup_policy_not_found",
                "backup policy not found",
            ),
        ] {
            assert_schema_not_found_detail(
                &backup_not_found_status(operation, schema_code, message),
                operation,
                schema_code,
                message,
            );
        }
    }

    #[test]
    fn backup_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &backup_internal_status(
                "restore_manifest_parse",
                "restore manifest parse failed: malformed manifest",
            ),
            "restore_manifest_parse",
            "restore manifest parse failed: malformed manifest",
        );
    }

    /// A caller scoped to tenant-a must not back up tenant-b by putting a foreign
    /// tenant_id in the request BODY; the scope guard rejects it before any store
    /// access (no Postgres needed). Mirrors `lock_service`.
    #[tokio::test]
    async fn start_backup_rejects_cross_tenant_body() {
        let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(backup_pb::StartTenantBackupRequest {
            tenant_id: "tenant-b".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .start_tenant_backup(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    /// A cross-tenant restore body is rejected by the shared movement validator
    /// (target differs from the verified claim, and from the source, without a
    /// privileged ack). No Postgres needed.
    #[tokio::test]
    async fn restore_rejects_cross_tenant_body() {
        let svc = BackupServiceImpl::new();
        let mut request = Request::new(backup_pb::RestoreTenantRequest {
            source_tenant_id: "tenant-a".to_string(),
            target_tenant_id: "tenant-b".to_string(),
            backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
            confirmation_token: "yes".to_string(),
            allow_cross_tenant: false,
            ..Default::default()
        });
        // Claim is tenant-a but the body targets tenant-b: the cross-tenant body
        // guard rejects it first.
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .restore_tenant(request)
            .await
            .expect_err("cross-tenant restore body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn restore_cross_tenant_movement_carries_policy_detail() {
        let svc = BackupServiceImpl::new();
        let mut request = Request::new(backup_pb::RestoreTenantRequest {
            source_tenant_id: "tenant-a".to_string(),
            target_tenant_id: "tenant-b".to_string(),
            backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
            confirmation_token: "yes".to_string(),
            allow_cross_tenant: false,
            ..Default::default()
        });
        // The claim matches the TARGET tenant, so the body guard passes and the
        // shared movement validator owns the cross-tenant source->target refusal.
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-b"));
        let err = svc
            .restore_tenant(request)
            .await
            .expect_err("cross-tenant movement must need explicit approval");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message()
                .contains("cannot move tenant 'tenant-a' data into tenant 'tenant-b'")
        );
        assert_policy_detail(
            &err,
            "tenant_movement_restore_import",
            "tenant_movement_scope_required",
        );
    }

    /// RestoreTenant is DESTRUCTIVE: an empty confirmation token fails closed
    /// before any store access (claim matches the target so the body guard
    /// passes). No Postgres needed.
    #[tokio::test]
    async fn restore_requires_confirmation_token() {
        let svc = BackupServiceImpl::new();
        let mut request = Request::new(backup_pb::RestoreTenantRequest {
            source_tenant_id: "tenant-a".to_string(),
            target_tenant_id: "tenant-a".to_string(),
            backup_id: "11111111-1111-1111-1111-111111111111".to_string(),
            confirmation_token: String::new(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .restore_tenant(request)
            .await
            .expect_err("missing confirmation token must fail closed");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "RestoreTenant overwrites a tenant's data; confirmation_token is required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "confirmation_token");
        assert_eq!(
            detail.field_violations[0].description,
            "must be present to restore over tenant data"
        );
    }

    #[tokio::test]
    async fn start_backup_missing_tenant_id_carries_field_violation() {
        let svc = BackupServiceImpl::new();
        let err = svc
            .start_tenant_backup(Request::new(backup_pb::StartTenantBackupRequest {
                tenant_id: "  ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing tenant_id must fail before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "tenant_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "tenant_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty tenant id"
        );
    }

    #[tokio::test]
    async fn restore_tenant_missing_identity_carries_field_violations() {
        let svc = BackupServiceImpl::new();
        let err = svc
            .restore_tenant(Request::new(backup_pb::RestoreTenantRequest {
                source_tenant_id: " ".to_string(),
                target_tenant_id: " ".to_string(),
                backup_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing restore identity must fail before tenant guard");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "source_tenant_id, target_tenant_id and backup_id are required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 3);
        assert_eq!(detail.field_violations[0].field, "source_tenant_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty source tenant id"
        );
        assert_eq!(detail.field_violations[1].field, "target_tenant_id");
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty target tenant id"
        );
        assert_eq!(detail.field_violations[2].field, "backup_id");
        assert_eq!(
            detail.field_violations[2].description,
            "must be a non-empty backup id"
        );
    }

    #[tokio::test]
    async fn get_backup_missing_backup_id_carries_field_violation() {
        let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(backup_pb::GetBackupRequest {
            tenant_id: "tenant-a".to_string(),
            backup_id: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_backup(request)
            .await
            .expect_err("missing backup_id must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "backup_id is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "backup_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty backup id"
        );
    }

    #[tokio::test]
    async fn get_backup_policy_missing_policy_name_carries_field_violation() {
        let svc = BackupServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(backup_pb::GetBackupPolicyRequest {
            tenant_id: "tenant-a".to_string(),
            policy_name: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_backup_policy(request)
            .await
            .expect_err("missing policy_name must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "policy_name is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "policy_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty policy name"
        );
    }

    #[test]
    fn backup_missing_setup_capabilities_carry_typed_detail() {
        for (operation, capability, message) in [
            (
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "backup service requires runtime native-entity dispatch (no runtime configured)",
            ),
            (
                "postgres_store",
                "postgres_store",
                "backup service requires a Postgres-backed store (no PG pool configured)",
            ),
            (
                "tenant_table_enumeration",
                "catalog_manifest",
                "backup service requires the catalog manifest to enumerate tenant tables",
            ),
        ] {
            let err = backup_capability_status(operation, capability, message);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "backup");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, capability);
            assert!(!detail.retryable);
        }
    }
}

impl DataBrokerService {
    /// Build the native `BackupService`, wired to the broker's Postgres pool (for
    /// the raw tenant-row movement + outbox), the runtime (native-entity journal/
    /// policy dispatch + encrypt-at-rest + object IO), the active manifest (the
    /// tenant-table enumeration source), and the default object-store target.
    pub(crate) fn build_backup_service(&self) -> BackupServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then
        // a health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("backup", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        let (object_backend, object_bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        BackupServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_manifest(Some(self.manifest.clone()))
            .with_object(object_backend, object_bucket)
    }
}
