//! Neutral-IR query/record builders, the per-tenant quota lease/aggregate helpers,
//! and the logical-value helpers for the native `StorageService`. Extracted
//! verbatim — the `LogicalRead`/`LogicalRecord`/`LogicalFilter` shapes, the
//! tenant-scoped live-file predicate, and the RLS-GUC-scoped quota SUM are
//! byte-for-byte identical to the former god file.

use std::time::Duration;

use sqlx::Row;
use tonic::Status;

use crate::ir::{
    ComparisonOp, LogicalFilter, LogicalPagination, LogicalProjection, LogicalRead, LogicalRecord,
    LogicalValue,
};
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::runtime::DataBrokerRuntime;

use super::StorageServiceImpl;
use super::config::{FILE_MSG, GC_INTENT_DEFAULT_MAX_ATTEMPTS, GC_INTENTS_RELATION};
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

/// The base clauses every live-file read shares: tenant match, optional
/// project ownership, and not soft-deleted. Storage metadata remains physically
/// tenant-placed; `project_id` is an authorization predicate, not a routing key.
/// An empty project intentionally preserves tenant-wide credentials.
fn file_tenant_active_clauses(tenant_id: &str, project_id: &str) -> Vec<LogicalFilter> {
    let mut clauses = vec![
        file_uuid_eq("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ];
    if !project_id.trim().is_empty() {
        clauses.push(file_eq("project_id", project_id.trim()));
    }
    clauses
}

/// A single live (non-soft-deleted) file scoped to its tenant and, when the
/// verified caller carries one, its owning project.
fn file_active_by_id_filter(tenant_id: &str, project_id: &str, file_id: &str) -> LogicalFilter {
    let mut clauses = file_tenant_active_clauses(tenant_id, project_id);
    clauses.push(file_uuid_eq("file_id", file_id));
    LogicalFilter::And(clauses)
}

/// `list_files` filter: tenant + live + optional metadata facets. Each facet is
/// applied only when supplied (mirrors the old `$n = '' OR col = $n` guards).
pub(crate) fn file_list_filter(
    tenant_id: &str,
    project_id: &str,
    file_type: &str,
    reference_id: &str,
    reference_type: &str,
    uploaded_by: &str,
) -> LogicalFilter {
    let mut filters = file_tenant_active_clauses(tenant_id, project_id);
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

pub(crate) fn file_read_by_id(tenant_id: &str, project_id: &str, file_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: FILE_MSG.to_string(),
        filter: Some(file_active_by_id_filter(tenant_id, project_id, file_id)),
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
        logical_text_or_null(req.project_id.trim()),
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
        logical_text_or_null(file.project_id.trim()),
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

// ── durable object-GC intent ledger (HARD DeleteFile) ────────────────────────
//
// A HARD `DeleteFile` records a PENDING intent here ATOMICALLY with the metadata
// tombstone (one Postgres tx), so an object whose bytes fail to delete inline can
// never leak silently: the leader-elected sweep drives every PENDING intent to
// convergence, or dead-letters it (`status = 'FAILED'`) after the attempt cap. The
// same row is the idempotency ledger — a partial unique index on
// `(tenant_id, idempotency_key)` makes an exact-key replay return the ORIGINAL
// outcome while a same-key/different-target reuse conflicts fail-closed.
//
// This is a broker-owned operational table (NOT a proto entity): tenant isolation
// is enforced by the per-tenant handler queries + the tenant-scoped tombstone (RLS
// GUC set in-tx), and the cross-tenant sweep is a broker-admin maintenance path
// (the same posture the orphan reaper uses).

/// Stable fingerprint of a delete's semantic target, stored on the GC intent and
/// re-derived on an idempotency-key replay. A replay that hashes to the SAME
/// fingerprint is an identical retry (replay the outcome); a DIFFERENT fingerprint
/// under the same key is a conflict. Deliberately covers only the target identity
/// (file + mode), not advisory fields (reason/expected_status).
pub(crate) fn gc_intent_fingerprint(file_id: &str, mode: &str) -> String {
    format!("{}|{}", file_id.trim(), mode.trim())
}

/// A durable GC-intent ledger row, decoded for the idempotency-replay and sweep
/// paths. `intent_id`/`tenant_id` are carried as text (UUIDs never
/// leave Postgres typed here) to avoid depending on sqlx's uuid feature.
pub(crate) struct GcIntentRow {
    pub(crate) intent_id: String,
    #[allow(dead_code)]
    pub(crate) tenant_id: String,
    pub(crate) project_id: String,
    pub(crate) backend: String,
    pub(crate) bucket: String,
    pub(crate) object_key: String,
    pub(crate) status: String,
    pub(crate) outcome_success: Option<bool>,
    pub(crate) fingerprint: String,
    #[allow(dead_code)]
    pub(crate) attempts: i64,
}

/// Column list shared by the by-key lookup and the sweep scan so their decodes
/// cannot drift. `attempts` is widened to bigint for a uniform i64 decode.
const GC_INTENT_SELECT_COLUMNS: &str = "intent_id::text AS intent_id, \
     tenant_id::text AS tenant_id, \
     COALESCE(project_id::text, '') AS project_id, \
     backend, bucket, object_key, status, outcome_success, \
     request_fingerprint AS fingerprint, attempts::bigint AS attempts";

fn gc_intent_from_row(row: &sqlx::postgres::PgRow) -> Result<GcIntentRow, Status> {
    let decode = |field: &'static str, err: sqlx::Error| {
        storage_internal_status(
            "gc_intent_decode",
            format!("storage GC intent decode failed for {field}: {err}"),
        )
    };
    Ok(GcIntentRow {
        intent_id: row
            .try_get("intent_id")
            .map_err(|e| decode("intent_id", e))?,
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|e| decode("tenant_id", e))?,
        project_id: row
            .try_get("project_id")
            .map_err(|e| decode("project_id", e))?,
        backend: row.try_get("backend").map_err(|e| decode("backend", e))?,
        bucket: row.try_get("bucket").map_err(|e| decode("bucket", e))?,
        object_key: row
            .try_get("object_key")
            .map_err(|e| decode("object_key", e))?,
        status: row.try_get("status").map_err(|e| decode("status", e))?,
        outcome_success: row
            .try_get("outcome_success")
            .map_err(|e| decode("outcome_success", e))?,
        fingerprint: row
            .try_get("fingerprint")
            .map_err(|e| decode("fingerprint", e))?,
        attempts: row.try_get("attempts").map_err(|e| decode("attempts", e))?,
    })
}

/// Idempotent DDL for the GC-intent ledger, applied once per service/pool instance
/// (guarded by `StorageServiceImpl::gc_intents_ready`).
const GC_INTENTS_DDL: &[&str] = &[
    "CREATE SCHEMA IF NOT EXISTS udb_storage",
    "CREATE TABLE IF NOT EXISTS udb_storage.gc_intents ( \
        intent_id           UUID PRIMARY KEY, \
        tenant_id           UUID NOT NULL, \
        project_id          VARCHAR(120), \
        file_id             UUID NOT NULL, \
        backend             TEXT NOT NULL DEFAULT '', \
        bucket              TEXT NOT NULL DEFAULT '', \
        object_key          TEXT NOT NULL, \
        mode                TEXT NOT NULL DEFAULT 'HARD', \
        reason              TEXT NOT NULL DEFAULT '', \
        status              TEXT NOT NULL DEFAULT 'PENDING', \
        attempts            INTEGER NOT NULL DEFAULT 0, \
        last_error          TEXT NOT NULL DEFAULT '', \
        idempotency_key     TEXT, \
        request_fingerprint TEXT NOT NULL DEFAULT '', \
        outcome_success     BOOLEAN, \
        outcome_code        TEXT NOT NULL DEFAULT '', \
        created_at          TIMESTAMPTZ NOT NULL DEFAULT now(), \
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(), \
        completed_at        TIMESTAMPTZ )",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_udb_storage_gc_intents_idem \
        ON udb_storage.gc_intents (tenant_id, idempotency_key) \
        WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''",
    "CREATE INDEX IF NOT EXISTS idx_udb_storage_gc_intents_pending \
        ON udb_storage.gc_intents (status, created_at) \
        WHERE status = 'PENDING'",
    // Migrate a ledger created before project ids were opaque. `CREATE TABLE IF
    // NOT EXISTS` above only fixes NEW deployments, so an existing table would
    // keep its UUID column and keep rejecting a project such as `ambulife`.
    // Guarded on the current column type so it is a no-op after the first run
    // rather than rewriting the table on every startup, and rollback-safe: a
    // UUID's text form is a valid opaque project id, so no value changes
    // meaning and the reverse cast still parses.
    "DO $$ \
     BEGIN \
       IF EXISTS ( \
         SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'udb_storage' \
           AND table_name = 'gc_intents' \
           AND column_name = 'project_id' \
           AND data_type = 'uuid' \
       ) THEN \
         ALTER TABLE udb_storage.gc_intents \
           ALTER COLUMN project_id TYPE VARCHAR(120) USING project_id::text; \
       END IF; \
     END $$",
];

impl StorageServiceImpl {
    /// Cap on inline+sweep object-delete attempts before a GC intent is
    /// dead-lettered. `UDB_STORAGE_GC_MAX_ATTEMPTS` overrides; clamped to `>= 1`.
    pub(crate) fn gc_max_attempts() -> i64 {
        std::env::var("UDB_STORAGE_GC_MAX_ATTEMPTS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(GC_INTENT_DEFAULT_MAX_ATTEMPTS)
            .max(1)
    }

    fn gc_intent_pool(&self) -> Result<&sqlx::PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            storage_capability_status(
                "gc_intent_store",
                "postgres_store",
                "storage GC-intent ledger requires a Postgres pool",
            )
        })
    }

    /// Create the durable GC-intent ledger if absent (idempotent DDL, run once per
    /// service/pool instance). Fails closed when no Postgres pool is wired
    /// (metadata-only mode cannot durably track HARD deletes).
    pub(crate) async fn ensure_gc_intents_table(&self) -> Result<(), Status> {
        let pool = self.gc_intent_pool()?;
        self.gc_intents_ready
            .get_or_try_init(|| async {
                for stmt in GC_INTENTS_DDL {
                    sqlx::query(stmt).execute(pool).await.map_err(|err| {
                        storage_internal_status(
                            "gc_intent_ddl",
                            format!("storage GC-intent ledger DDL failed: {err}"),
                        )
                    })?;
                }
                Ok::<(), Status>(())
            })
            .await
            .map(|_| ())
    }

    /// Look up a tenant's GC intent by idempotency key (the idempotency-replay
    /// probe). Tenant-scoped so a key is never resolved across tenants.
    pub(crate) async fn gc_intent_by_key(
        &self,
        tenant_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<GcIntentRow>, Status> {
        let pool = self.gc_intent_pool()?;
        let sql = format!(
            "SELECT {GC_INTENT_SELECT_COLUMNS} FROM {GC_INTENTS_RELATION} \
             WHERE tenant_id = $1::uuid AND idempotency_key = $2 LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(idempotency_key)
            .fetch_optional(pool)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "gc_intent_lookup",
                    format!("storage GC-intent lookup failed: {err}"),
                )
            })?;
        row.as_ref().map(gc_intent_from_row).transpose()
    }

    /// Commit the durable GC intent (PENDING) TOGETHER with the metadata tombstone
    /// in ONE Postgres transaction, so the two can never diverge (bytes tracked for
    /// GC ⇔ metadata marked deleted). The tenant RLS GUC is set in-tx and the
    /// tombstone is tenant+file scoped, preserving isolation.
    ///
    /// Returns `Ok(Some(intent_id))` on a fresh commit, or `Ok(None)` when a
    /// concurrent same-key delete already claimed the idempotency key (unique
    /// violation) — the caller then replays that intent's outcome.
    pub(crate) async fn insert_gc_intent_and_tombstone(
        &self,
        tenant_id: &str,
        file_id: &str,
        project_id: &str,
        backend: &str,
        bucket: &str,
        object_key: &str,
        reason: &str,
        idempotency_key: Option<&str>,
        fingerprint: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.gc_intent_pool()?;
        let intent_id = uuid::Uuid::new_v4().to_string();
        let mut tx = pool.begin().await.map_err(|err| {
            storage_internal_status(
                "gc_intent_tx_begin",
                format!("storage GC-intent tx begin failed: {err}"),
            )
        })?;
        // Scope the tombstone to the tenant for the File table's force_rls policy
        // (same GUC install as tenant_scoped_size_sum).
        sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "gc_intent_tenant_scope",
                    format!("storage GC-intent tenant scope set failed: {err}"),
                )
            })?;
        let insert_sql = format!(
            "INSERT INTO {GC_INTENTS_RELATION} \
               (intent_id, tenant_id, project_id, file_id, backend, bucket, object_key, mode, \
                reason, status, idempotency_key, request_fingerprint) \
             VALUES ($1::uuid, $2::uuid, NULLIF($3,'')::uuid, $4::uuid, $5, $6, $7, 'HARD', $8, \
                'PENDING', NULLIF($9,''), $10)"
        );
        let insert = sqlx::query(&insert_sql)
            .bind(&intent_id)
            .bind(tenant_id)
            .bind(project_id)
            .bind(file_id)
            .bind(backend)
            .bind(bucket)
            .bind(object_key)
            .bind(reason)
            .bind(idempotency_key.unwrap_or(""))
            .bind(fingerprint)
            .execute(&mut *tx)
            .await;
        if let Err(err) = insert {
            // A unique violation on (tenant_id, idempotency_key) means a concurrent
            // same-key delete won the race: roll back and signal replay.
            let is_unique_violation = err
                .as_database_error()
                .and_then(|db| db.code())
                .is_some_and(|code| code.as_ref() == "23505");
            drop(tx);
            if is_unique_violation && idempotency_key.is_some() {
                return Ok(None);
            }
            return Err(storage_internal_status(
                "gc_intent_insert",
                format!("storage GC-intent insert failed: {err}"),
            ));
        }
        // Metadata tombstone: same soft-delete transition the SOFT path applies,
        // but here it is atomic with the durable intent. Not requiring an affected
        // row keeps a concurrently-deleted file convergent (the intent still GCs
        // the bytes).
        sqlx::query(
            "UPDATE udb_storage.files SET deleted_at = now(), status = 'DELETED' \
             WHERE tenant_id = $1::uuid AND file_id = $2::uuid AND deleted_at IS NULL",
        )
        .bind(tenant_id)
        .bind(file_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            storage_internal_status(
                "gc_intent_tombstone",
                format!("storage GC-intent tombstone failed: {err}"),
            )
        })?;
        tx.commit().await.map_err(|err| {
            storage_internal_status(
                "gc_intent_tx_commit",
                format!("storage GC-intent tx commit failed: {err}"),
            )
        })?;
        Ok(Some(intent_id))
    }

    /// Record the immutable success outcome once the bytes are confirmed removed.
    /// Guarded `status <> 'DONE'` so a resolved outcome is never rewritten.
    pub(crate) async fn mark_gc_intent_done(&self, intent_id: &str) -> Result<(), Status> {
        let pool = self.gc_intent_pool()?;
        let sql = format!(
            "UPDATE {GC_INTENTS_RELATION} \
                SET status = 'DONE', outcome_success = true, outcome_code = 'OK', \
                    attempts = attempts + 1, updated_at = now(), completed_at = now() \
              WHERE intent_id = $1::uuid AND status <> 'DONE'"
        );
        sqlx::query(&sql)
            .bind(intent_id)
            .execute(pool)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "gc_intent_mark_done",
                    format!("storage GC-intent mark-done failed: {err}"),
                )
            })?;
        Ok(())
    }

    /// Record a failed byte-delete attempt: bump `attempts`, keep the intent
    /// PENDING for the sweep, and dead-letter (`status = 'FAILED'`, immutable
    /// failure outcome) once the attempt cap is reached. Guarded `status =
    /// 'PENDING'` so a DONE intent is never regressed.
    pub(crate) async fn record_gc_intent_failure(
        &self,
        intent_id: &str,
        error_message: &str,
        max_attempts: i64,
    ) -> Result<(), Status> {
        let pool = self.gc_intent_pool()?;
        let truncated: String = error_message.chars().take(500).collect();
        let sql = format!(
            "UPDATE {GC_INTENTS_RELATION} \
                SET attempts = attempts + 1, \
                    last_error = $2, \
                    status = CASE WHEN attempts + 1 >= $3 THEN 'FAILED' ELSE 'PENDING' END, \
                    outcome_success = CASE WHEN attempts + 1 >= $3 THEN false ELSE outcome_success END, \
                    outcome_code = CASE WHEN attempts + 1 >= $3 THEN 'OBJECT_DELETE_FAILED' ELSE outcome_code END, \
                    completed_at = CASE WHEN attempts + 1 >= $3 THEN now() ELSE completed_at END, \
                    updated_at = now() \
              WHERE intent_id = $1::uuid AND status = 'PENDING'"
        );
        sqlx::query(&sql)
            .bind(intent_id)
            .bind(truncated)
            .bind(max_attempts)
            .execute(pool)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "gc_intent_record_failure",
                    format!("storage GC-intent failure record failed: {err}"),
                )
            })?;
        Ok(())
    }

    /// Bounded, oldest-first batch of PENDING GC intents across all tenants, for the
    /// leader-elected sweep. Cross-tenant by design (broker-admin maintenance, same
    /// posture as the orphan reaper).
    pub(crate) async fn select_pending_gc_intents(
        &self,
        batch_size: i64,
    ) -> Result<Vec<GcIntentRow>, Status> {
        let pool = self.gc_intent_pool()?;
        let batch_size = batch_size.clamp(1, 10_000);
        let sql = format!(
            "SELECT {GC_INTENT_SELECT_COLUMNS} FROM {GC_INTENTS_RELATION} \
             WHERE status = 'PENDING' ORDER BY created_at ASC LIMIT $1"
        );
        let rows = sqlx::query(&sql)
            .bind(batch_size)
            .fetch_all(pool)
            .await
            .map_err(|err| {
                storage_internal_status(
                    "gc_intent_scan",
                    format!("storage GC-intent scan failed: {err}"),
                )
            })?;
        rows.iter().map(gc_intent_from_row).collect()
    }
}
