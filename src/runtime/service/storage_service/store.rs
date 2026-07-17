//! Neutral-IR query/record builders, the per-tenant quota lease/aggregate helpers,
//! and the logical-value helpers for the native `StorageService`. Extracted
//! verbatim — the `LogicalRead`/`LogicalRecord`/`LogicalFilter` shapes, the
//! tenant-scoped live-file predicate, and the RLS-GUC-scoped quota SUM are
//! byte-for-byte identical to the former god file.

use std::time::Duration;

use tonic::Status;

use crate::ir::{
    ComparisonOp, LogicalFilter, LogicalPagination, LogicalProjection, LogicalRead, LogicalRecord,
    LogicalValue,
};
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::runtime::DataBrokerRuntime;

use super::StorageServiceImpl;
use super::config::FILE_MSG;
use super::errors::{storage_capability_status, storage_internal_status};
use super::model::{file_status_to_short, file_type_to_short, register_is_public_bind};

// ── typed data-plane path (extend_udb.md P4) ─────────────────────────────────
//
// File metadata persists through the neutral-IR compiler + the backend bound to
// the `storage` native service (per its proto `native_service` annotation),
// instead of hand-written Postgres SQL. Mirrors the `tenant_service` reference
// migration: build `LogicalRead`/`LogicalRecord`/`LogicalFilter`, dispatch via
// `runtime.native_entity_*_for_service("storage", …)`, and map rows back from
// the JSON the executor/native driver returns.

/// Name of the cluster-wide advisory lease serializing a tenant's storage quota
/// check-then-write (one lease per tenant).
pub(crate) fn quota_lease_name(tenant_id: &str) -> String {
    format!("storage_quota:{tenant_id}")
}

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

/// UUID-typed columns reject `''::uuid`, so an absent optional UUID binds SQL
/// NULL (Mongo `null` / etc.) rather than an empty string.
pub(crate) fn logical_uuid_or_null(value: &str) -> LogicalValue {
    let value = value.trim();
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

/// Nullable text/varchar column (e.g. `file_type`): empty → SQL NULL, matching
/// the old `NULLIF($n, '')` binds.
pub(crate) fn logical_text_or_null(value: &str) -> LogicalValue {
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

pub(crate) fn file_eq(field: &str, value: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

fn file_uuid_eq(field: &str, value: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_uuid_or_null(value),
    }
}

/// The base clauses every tenant-scoped live-file read shares: tenant match +
/// not soft-deleted. Single source for `file_active_by_id_filter` and
/// `file_list_filter` so the "live file" definition can never diverge.
fn file_tenant_active_clauses(tenant_id: &str) -> Vec<LogicalFilter> {
    vec![
        file_uuid_eq("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ]
}

/// A single live (non-soft-deleted) file scoped to its tenant.
fn file_active_by_id_filter(tenant_id: &str, file_id: &str) -> LogicalFilter {
    let mut clauses = file_tenant_active_clauses(tenant_id);
    clauses.push(file_uuid_eq("file_id", file_id));
    LogicalFilter::And(clauses)
}

/// `list_files` filter: tenant + live + optional metadata facets. Each facet is
/// applied only when supplied (mirrors the old `$n = '' OR col = $n` guards).
pub(crate) fn file_list_filter(
    tenant_id: &str,
    file_type: &str,
    reference_id: &str,
    reference_type: &str,
    uploaded_by: &str,
) -> LogicalFilter {
    let mut filters = file_tenant_active_clauses(tenant_id);
    if !file_type.is_empty() {
        filters.push(file_eq("file_type", file_type));
    }
    if !reference_id.trim().is_empty() {
        filters.push(file_uuid_eq("reference_id", reference_id.trim()));
    }
    if !reference_type.trim().is_empty() {
        filters.push(file_eq("reference_type", reference_type.trim()));
    }
    if !uploaded_by.trim().is_empty() {
        filters.push(file_uuid_eq("uploaded_by", uploaded_by.trim()));
    }
    LogicalFilter::And(filters)
}

pub(crate) fn file_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "file_id",
            "tenant_id",
            "project_id",
            "filename",
            "content_type",
            "size_bytes",
            "backend",
            "bucket",
            "object_key",
            "url",
            "cdn_url",
            "file_type",
            "reference_id",
            "reference_type",
            "is_public",
            "status",
            "checksum",
            "uploaded_by",
            "deleted_by",
        ]
        .into_iter()
        .map(str::to_string),
    )
}

pub(crate) fn file_read_by_id(tenant_id: &str, file_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: FILE_MSG.to_string(),
        filter: Some(file_active_by_id_filter(tenant_id, file_id)),
        projection: Some(file_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

/// Full INSERT record for a freshly registered (`PENDING`) upload.
pub(crate) fn file_register_record(
    file_id: &str,
    tenant_id: &str,
    req: &storage_pb::RegisterUploadRequest,
    file_type: &str,
    object_backend: &str,
    object_bucket: &str,
    object_key: &str,
    declared_size: i64,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("file_id".to_string(), logical_string(file_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert(
        "project_id".to_string(),
        logical_uuid_or_null(&req.project_id),
    );
    record.insert("filename".to_string(), logical_string(req.filename.clone()));
    record.insert(
        "content_type".to_string(),
        logical_string(req.content_type.clone()),
    );
    record.insert("file_type".to_string(), logical_text_or_null(file_type));
    record.insert("status".to_string(), logical_string("PENDING"));
    record.insert(
        "reference_id".to_string(),
        logical_uuid_or_null(&req.reference_id),
    );
    record.insert(
        "reference_type".to_string(),
        logical_string(req.reference_type.clone()),
    );
    record.insert(
        "is_public".to_string(),
        LogicalValue::Bool(register_is_public_bind(req.is_public)),
    );
    record.insert("backend".to_string(), logical_string(object_backend));
    record.insert("bucket".to_string(), logical_string(object_bucket));
    record.insert("object_key".to_string(), logical_string(object_key));
    record.insert("size_bytes".to_string(), LogicalValue::Int(declared_size));
    record
}

/// A full column record reflecting a file's current state. The typed upsert
/// (`ConflictStrategy::Update`) compiles to `INSERT … ON CONFLICT DO UPDATE`, so
/// the record must carry every column to satisfy NOT-NULL on the insert arm even
/// though an existing row always takes the update arm (mirrors
/// `tenant_config_record`). Callers override the fields they mutate, then list
/// exactly those in `ConflictStrategy::Update { fields }`. `deleted_at` is
/// intentionally omitted (lifecycle-only; set explicitly by `delete_file`).
pub(crate) fn file_full_record(file: &storage_entity_pb::File) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("file_id".to_string(), logical_string(file.file_id.clone()));
    record.insert(
        "tenant_id".to_string(),
        logical_uuid_or_null(&file.tenant_id),
    );
    record.insert(
        "project_id".to_string(),
        logical_uuid_or_null(&file.project_id),
    );
    record.insert(
        "filename".to_string(),
        logical_string(file.filename.clone()),
    );
    record.insert(
        "content_type".to_string(),
        logical_string(file.content_type.clone()),
    );
    record.insert("size_bytes".to_string(), LogicalValue::Int(file.size_bytes));
    record.insert("backend".to_string(), logical_string(file.backend.clone()));
    record.insert("bucket".to_string(), logical_string(file.bucket.clone()));
    record.insert(
        "object_key".to_string(),
        logical_string(file.object_key.clone()),
    );
    record.insert("url".to_string(), logical_string(file.url.clone()));
    record.insert("cdn_url".to_string(), logical_string(file.cdn_url.clone()));
    record.insert(
        "file_type".to_string(),
        logical_text_or_null(file_type_to_short(file.file_type)),
    );
    record.insert(
        "reference_id".to_string(),
        logical_uuid_or_null(&file.reference_id),
    );
    record.insert(
        "reference_type".to_string(),
        logical_string(file.reference_type.clone()),
    );
    record.insert("is_public".to_string(), LogicalValue::Bool(file.is_public));
    record.insert(
        "status".to_string(),
        logical_string(file_status_to_short(file.status)),
    );
    record.insert(
        "checksum".to_string(),
        logical_string(file.checksum.clone()),
    );
    record.insert(
        "uploaded_by".to_string(),
        logical_uuid_or_null(&file.uploaded_by),
    );
    record.insert(
        "deleted_by".to_string(),
        logical_uuid_or_null(&file.deleted_by),
    );
    record
}

impl StorageServiceImpl {
    /// Serialize per-tenant quota check-then-write across the cluster via the
    /// canonical advisory lease — the backend-agnostic analog of the old
    /// `pg_advisory_xact_lock`. Bounded retry (≈5s) then fail closed so a
    /// contended quota gate never silently lets a concurrent over-quota write
    /// slip through. Returns `true` when the lease is held (caller MUST release).
    /// Only invoked when a quota is configured (`quota > 0`).
    pub(crate) async fn acquire_quota_lease(
        &self,
        runtime: &DataBrokerRuntime,
        tenant_id: &str,
        owner: &str,
    ) -> Result<bool, Status> {
        let name = quota_lease_name(tenant_id);
        for _ in 0..50 {
            if runtime
                .try_acquire_native_lease(&name, owner, Duration::from_secs(30))
                .await?
            {
                return Ok(true);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(crate::runtime::executor_utils::retryable_status(
            "storage",
            "quota_lock",
            crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
            "storage quota lock contended; retry shortly",
        ))
    }

    /// Read the per-tenant byte quota from the environment. `0` = unlimited.
    /// Sum this tenant's active (non-deleted) file bytes for quota enforcement.
    /// `udb_storage.files` is RLS-scoped by `app.current_tenant_id`; the generic
    /// aggregate dispatch does not install that GUC on its read connection, so the
    /// quota SUM under-reported as 0 (quota never enforced). Install the tenant
    /// scope in a read transaction, then SUM the active rows in the same tx (mirrors
    /// the metering QueryUsage fix).
    pub(crate) async fn tenant_scoped_size_sum(&self, tenant_id: &str) -> Result<i64, Status> {
        let Some(pool) = self.pg_pool.as_ref() else {
            return Err(storage_capability_status(
                "tenant_size_sum",
                "postgres_store",
                "storage usage pool is not configured",
            ));
        };
        let mut tx = pool.begin().await.map_err(|err| {
            storage_internal_status(
                "tenant_size_sum_begin",
                format!("storage usage tx begin failed: {err}"),
            )
        })?;
        sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "tenant_size_sum_tenant_scope",
                    format!("storage tenant scope set failed: {err}"),
                )
            })?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0)::bigint FROM udb_storage.files              WHERE tenant_id::text = $1 AND deleted_at IS NULL",
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| {
            storage_internal_status(
                "tenant_size_sum_aggregate",
                format!("storage usage aggregate failed: {err}"),
            )
        })?;
        tx.commit().await.map_err(|err| {
            storage_internal_status(
                "tenant_size_sum_commit",
                format!("storage usage tx commit failed: {err}"),
            )
        })?;
        Ok(total)
    }

    pub(crate) fn tenant_quota_bytes() -> i64 {
        std::env::var("UDB_STORAGE_TENANT_QUOTA_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }
}
