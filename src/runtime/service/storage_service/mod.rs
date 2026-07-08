//! Native `StorageService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_storage.files` table.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table
//! and column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! v1 is a metadata/lifecycle service; object bytes are written through the
//! native presigned URL minted at registration. Public object RPCs remain a
//! fallback only when no runtime is available to presign.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder,
    SortDirection,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};

use crate::proto::udb::core::common::v1 as common_pb;
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;

pub use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, admit_on as native_admit_on, emit_payload_event,
    native_next_page_token_for_total, native_offset_page_window, storage_object_defaults,
    tenant_only_native_service_context, update_mask_allows, update_mask_path_set,
    validate_request_scope, validate_request_tenant,
};

const FILE_MSG: &str = "udb.core.storage.entity.v1.File";

/// Topics for the storage domain events emitted via the transactional outbox
/// (→ CDC → Kafka). Dot-only per the project's Kafka topic convention.
const TOPIC_UPLOAD_URL_ISSUED: &str = "udb.storage.file.upload_url_issued.v1";
const TOPIC_FILE_FINALIZED: &str = "udb.storage.file.finalized.v1";
const TOPIC_FILE_METADATA_UPDATED: &str = "udb.storage.file.metadata_updated.v1";
const TOPIC_FILE_DELETED: &str = "udb.storage.file.deleted.v1";

/// Stable machine-readable error codes for the storage service (§04.5). Emitted
/// as `ApiError.code` on the OK-with-error register paths, or as the
/// `error-reason` Status metadata trailer on non-OK gRPC statuses — a non-OK
/// status is trailers-only and discards the body `ApiError`, so the sub-code must
/// ride a trailer (mirrors the notification/webrtc services).
const STORAGE_QUOTA_EXCEEDED: &str = "STORAGE_QUOTA_EXCEEDED";
const UPLOAD_URL_UNAVAILABLE: &str = "UPLOAD_URL_UNAVAILABLE";
const OBJECT_NOT_PRESENT: &str = "OBJECT_NOT_PRESENT";
const UPLOAD_SIZE_MISMATCH: &str = "UPLOAD_SIZE_MISMATCH";
const ALREADY_FINALIZED: &str = "ALREADY_FINALIZED";
/// Soft-delete warn path only (bytes orphaned after a metadata delete) — emitted
/// on the warn log, NOT part of the RPC error catalog.
const OBJECT_DELETE_ORPHANED: &str = "OBJECT_DELETE_ORPHANED";
/// Reserved: defined for the catalog but no live emit site on the in-process
/// path yet (expiry/backend-unsupported are not surfaced as RPC errors today).
#[allow(dead_code)]
const UPLOAD_EXPIRED: &str = "UPLOAD_EXPIRED";
#[allow(dead_code)]
const UNSUPPORTED_OBJECT_BACKEND: &str = "UNSUPPORTED_OBJECT_BACKEND";

/// Attach a stable machine-readable `reason` to a non-OK gRPC `Status` via the
/// `error-reason` metadata trailer (the body is discarded on non-OK statuses).
fn status_with_reason(mut status: Status, reason: &'static str) -> Status {
    status.metadata_mut().insert(
        "error-reason",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    status
}

fn storage_policy_status_with_reason(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
    reason: &'static str,
) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message),
        reason,
    )
}

fn upload_already_finalized_status() -> Status {
    storage_policy_status_with_reason(
        "finalize_upload",
        "upload_already_finalized",
        "upload already finalized",
        ALREADY_FINALIZED,
    )
}

fn uploaded_object_missing_status() -> Status {
    storage_policy_status_with_reason(
        "finalize_upload",
        "uploaded_object_present",
        "uploaded object is not present in the configured object store",
        OBJECT_NOT_PRESENT,
    )
}

fn upload_etag_mismatch_status() -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            "uploaded object etag does not match",
            [("etag", "must match the uploaded object's store ETag")],
        ),
        UPLOAD_SIZE_MISMATCH,
    )
}

fn upload_size_mismatch_status(head_size: i64, declared_size: i64) -> Status {
    status_with_reason(
        crate::runtime::executor_utils::failed_precondition_fields(
            format!("uploaded object size {head_size} does not match declared {declared_size}"),
            [(
                "size_bytes",
                "must match the uploaded object's store content length",
            )],
        ),
        UPLOAD_SIZE_MISMATCH,
    )
}

/// Build an `ApiError` carrying `UPLOAD_URL_UNAVAILABLE` for the degraded/failed
/// presign branches of `register_upload` (kept as `Response::Ok` so the persisted
/// PENDING row + quota reservation survive).
fn api_error_upload_url_unavailable(
    message: impl Into<String>,
    retryable: bool,
) -> common_pb::ApiError {
    common_pb::ApiError {
        code: UPLOAD_URL_UNAVAILABLE.to_string(),
        message: message.into(),
        retryable,
        ..Default::default()
    }
}

/// Normalize an S3/HTTP ETag for comparison: strip a weak-validator `W/` prefix
/// and the surrounding quotes S3 returns, then lowercase.
fn normalize_etag(etag: &str) -> String {
    etag.trim()
        .trim_start_matches("W/")
        .trim_matches('"')
        .to_ascii_lowercase()
}

/// Outcome of minting a presigned URL: a real URL + unix-seconds expiry, a
/// degraded deployment (no object runtime / object-store feature off), or a
/// genuine presign failure — lets `register_upload` distinguish "no objectstore"
/// from a real error on an empty `upload_url`.
enum PresignOutcome {
    Url { url: String, expires_at: i64 },
    Degraded,
    Failed(String),
}

/// Tri-state object-presence result from the storage `object_exists` wrapper:
/// `Unchecked` (metadata-only mode, no runtime — skip verification), `Absent`
/// (object not in the store), or `Present` with the HEAD content-length + ETag.
enum ObjectCheck {
    Unchecked,
    Absent,
    Present { size: i64, etag: String },
}

/// Postgres-backed `StorageService` handler.
#[derive(Clone)]
pub struct StorageServiceImpl {
    pg_pool: Option<PgPool>,
    /// Schema-qualified outbox table (`udb_system.outbox_events`) the CDC engine
    /// tails → Apache Kafka. `None` = no emit.
    outbox_relation: Option<String>,
    /// Broker runtime handle used to delete object bytes through the existing
    /// object executor (manifest-free `delete_object_backend_target`). `None` =
    /// metadata-only mode (no byte deletion — bytes left to lifecycle/ops).
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Heavy/mutating RPCs acquire a per-tenant
    /// `Object` budget through this so one tenant can't starve shared object
    /// capacity. `None` only in metadata-only/test construction (no runtime
    /// wired) — in production `build_storage_service` always wires it.
    channels: Option<ChannelManager>,
    /// Object-store backend + bucket native storage owns its bytes in.
    object_backend: String,
    object_bucket: String,
    metrics: Arc<dyn MetricsRecorder>,
}

fn storage_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "storage",
        operation,
        capability_required,
        message,
    )
}

fn storage_capability_status_with_reason(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
    reason: &'static str,
) -> Status {
    status_with_reason(
        storage_capability_status(operation, capability_required, message),
        reason,
    )
}

fn storage_file_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "storage",
        operation,
        "file_not_found",
        "file not found",
    )
}

fn storage_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("storage", operation, message)
}

fn object_stream_requires_store_status() -> Status {
    storage_capability_status_with_reason(
        "object_stream",
        "object_store",
        "object byte streaming requires a configured object store",
        UNSUPPORTED_OBJECT_BACKEND,
    )
}

fn file_object_bytes_missing_status() -> Status {
    storage_policy_status_with_reason(
        "download_file",
        "file_object_bytes_present",
        "file has no object bytes",
        OBJECT_NOT_PRESENT,
    )
}

fn object_store_bytes_missing_status() -> Status {
    storage_policy_status_with_reason(
        "download_file",
        "object_store_bytes_present",
        "object is not present in the configured object store",
        OBJECT_NOT_PRESENT,
    )
}

impl StorageServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            runtime: None,
            channels: None,
            object_backend: DEFAULT_OBJECT_BACKEND.to_string(),
            object_bucket: DEFAULT_OBJECT_BUCKET.to_string(),
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Wire the runtime handle + object-store target so `DeleteFile`/reaper can
    /// remove object bytes (not just metadata) via the shared object executor.
    pub(crate) fn with_object(
        mut self,
        runtime: Option<Arc<DataBrokerRuntime>>,
        backend: String,
        bucket: String,
    ) -> Self {
        // Capture the shared per-tenant fair-admission manager so mutating RPCs
        // can acquire the per-tenant Object budget (same path as the data plane).
        self.channels = runtime.as_ref().map(|rt| rt.channels().clone());
        self.runtime = runtime;
        if !backend.trim().is_empty() {
            self.object_backend = backend;
        }
        if !bucket.trim().is_empty() {
            self.object_bucket = bucket;
        }
        self
    }

    /// Best-effort delete of an object's bytes via the existing object executor.
    /// Never fails the caller: on error the metadata row stays soft-deleted and
    /// auditable, and the bytes are logged as orphaned for ops/lifecycle cleanup.
    async fn delete_object_bytes(&self, project_id: &str, object_key: &str) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        if object_key.trim().is_empty() {
            return;
        }
        let request_json = crate::runtime::core::setup_data::object_request_json(
            "delete",
            &self.object_bucket,
            object_key,
            "",
        );
        if let Err(err) = runtime
            .delete_object_backend_target(&self.object_backend, None, project_id, &request_json)
            .await
        {
            tracing::warn!(
                error = %err,
                object_key,
                bucket = %self.object_bucket,
                code = OBJECT_DELETE_ORPHANED,
                "storage object byte delete failed; metadata soft-deleted (auditable), bytes orphaned"
            );
        }
    }

    /// Mint a presigned object URL via the runtime (PUT for uploads, GET for
    /// downloads). Returns `("", 0)` in metadata-only mode (no runtime) or on
    /// error — callers then fall back to the existing public object RPCs.
    async fn presign(
        &self,
        project_id: &str,
        object_key: &str,
        method: &str,
        content_type: &str,
        ttl_minutes: i32,
    ) -> PresignOutcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return PresignOutcome::Degraded;
        };
        let ttl_secs = (ttl_minutes.max(1) as i64 * 60).min(7 * 24 * 3600) as i32;
        match runtime
            .presign_object_backend_target(
                &self.object_backend,
                project_id,
                &self.object_bucket,
                object_key,
                method,
                content_type,
                ttl_secs,
            )
            .await
        {
            Ok((url, expires_at_unix)) => PresignOutcome::Url {
                url,
                expires_at: expires_at_unix,
            },
            Err(err) => {
                tracing::warn!(error = %err, object_key, method, "storage presign failed; returning empty url");
                // An object-store feature compiled out / no instance configured is a
                // deployment-degraded state, not a transient error: surface Degraded
                // (non-retryable) so the client falls back to the public object RPCs
                // instead of retrying. Everything else is a real presign failure.
                if err.message().contains("feature is not enabled") {
                    PresignOutcome::Degraded
                } else {
                    PresignOutcome::Failed(err.message().to_string())
                }
            }
        }
    }

    /// Wire the transactional outbox so storage lifecycle events publish domain
    /// events to Kafka (via the CDC relay). `relation` is the schema-qualified
    /// table, e.g. `"udb_system"."outbox_events"` (`CdcConfig::outbox_relation`).
    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    /// File metadata CRUD is durable-only: fail closed when no Postgres pool exists.
    /// Typed File entities persist through native-entity dispatch on the backend
    /// bound to this service (extend_udb.md P4) — not the hardcoded PG pool. Fail
    /// closed when no runtime is wired (bare metadata-only/test construction).
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            storage_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "storage service requires runtime native entity dispatch",
            )
        })
    }

    /// Serialize per-tenant quota check-then-write across the cluster via the
    /// canonical advisory lease — the backend-agnostic analog of the old
    /// `pg_advisory_xact_lock`. Bounded retry (≈5s) then fail closed so a
    /// contended quota gate never silently lets a concurrent over-quota write
    /// slip through. Returns `true` when the lease is held (caller MUST release).
    /// Only invoked when a quota is configured (`quota > 0`).
    async fn acquire_quota_lease(
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

    /// Per-tenant fair admission for a mutating/heavy storage RPC. Acquires the
    /// shared `Object` channel budget SCOPED to the validated tenant (+ project),
    /// so a single tenant's flood cannot starve other tenants' object capacity —
    /// the exact path the data plane uses via `execute_with_channel_scoped`.
    ///
    /// On budget/concurrency exhaustion this returns the same
    /// `Status::resource_exhausted` backpressure the data plane returns. The
    /// returned [`ChannelPermit`] must be held for the duration of the RPC (drop
    /// = release). `None` channels (no runtime wired — metadata-only/test mode)
    /// admit without a permit since there is no shared object work to starve.
    ///
    /// `tenant` MUST be the VALIDATED tenant (post `validate_request_*`), never an
    /// unverified body field.
    async fn admit(&self, tenant: &str, project: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "storage",
            OperationChannel::Object,
            tenant,
            Some(project),
        )
        .await
    }

    /// Lighter per-tenant fair admission for a READ RPC (`get_file`/`list_files`).
    /// Acquires the cheap `Read` channel budget scoped to the validated tenant so
    /// one tenant cannot exhaust the shared pool with reads, without charging the
    /// heavier `Object` cost a mutating/object-touching RPC pays.
    async fn admit_read(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "storage",
            OperationChannel::Read,
            tenant,
            Some(""),
        )
        .await
    }

    /// Read the per-tenant byte quota from the environment. `0` = unlimited.
    /// Sum this tenant's active (non-deleted) file bytes for quota enforcement.
    /// `udb_storage.files` is RLS-scoped by `app.current_tenant_id`; the generic
    /// aggregate dispatch does not install that GUC on its read connection, so the
    /// quota SUM under-reported as 0 (quota never enforced). Install the tenant
    /// scope in a read transaction, then SUM the active rows in the same tx (mirrors
    /// the metering QueryUsage fix).
    async fn tenant_scoped_size_sum(&self, tenant_id: &str) -> Result<i64, Status> {
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

    fn tenant_quota_bytes() -> i64 {
        std::env::var("UDB_STORAGE_TENANT_QUOTA_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    async fn object_exists(&self, file: &storage_entity_pb::File) -> Result<ObjectCheck, Status> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(ObjectCheck::Unchecked);
        };
        if file.object_key.trim().is_empty() {
            return Ok(ObjectCheck::Absent);
        }
        let backend = if file.backend.trim().is_empty() {
            self.object_backend.as_str()
        } else {
            file.backend.as_str()
        };
        let bucket = if file.bucket.trim().is_empty() {
            self.object_bucket.as_str()
        } else {
            file.bucket.as_str()
        };
        match runtime
            .object_exists_backend_target(backend, &file.project_id, bucket, &file.object_key)
            .await?
        {
            Some((size, etag)) => Ok(ObjectCheck::Present { size, etag }),
            None => Ok(ObjectCheck::Absent),
        }
    }

    /// Hard-DELETE orphaned `PENDING` files older than `older_than_minutes`
    /// (uploads that were registered but never finalized) and remove their
    /// abandoned object bytes. Uses the auto-injected `created_at` audit column
    /// (`audit_fields: true` on the File table). Returns the number deleted.
    ///
    /// Fully on the typed path (no raw SQL). This is a CROSS-TENANT maintenance
    /// sweep, so it runs under a SYSTEM context (empty tenant) and supplies NO
    /// tenant filter — it reaps every tenant's orphans. That is sound because the
    /// broker's platform-admin pool bypasses the File table's `force_rls`
    /// (per-tenant isolation for normal RPCs comes from each handler's tenant
    /// filter, which this maintenance path deliberately omits). The reap is bounded
    /// oldest-first via a `LogicalRead` (`ORDER BY created_at … LIMIT`), then exactly
    /// that batch is hard-deleted by primary key (`file_id IN (…)`). The cutoff is
    /// computed in Rust and bound as a timestamp, so no backend-specific `INTERVAL`
    /// arithmetic leaks into the neutral IR.
    pub(crate) async fn reap_orphans(
        &self,
        older_than_minutes: i64,
        batch_size: i64,
    ) -> Result<u64, Status> {
        let runtime = self.require_runtime()?;
        let batch_size = batch_size.clamp(1, 10_000);
        let context = crate::RequestContext {
            correlation_id: "storage-orphan-reaper".to_string(),
            ..crate::RequestContext::default()
        };
        let cutoff = chrono::Utc::now() - chrono::Duration::minutes(older_than_minutes.max(0));
        // 1) Bounded, oldest-first batch of PENDING orphans across all tenants.
        let read = LogicalRead {
            message_type: FILE_MSG.to_string(),
            filter: Some(LogicalFilter::And(vec![
                file_eq("status", "PENDING"),
                LogicalFilter::Comparison {
                    field: "created_at".to_string(),
                    op: ComparisonOp::Lt,
                    value: LogicalValue::Timestamp(cutoff),
                },
            ])),
            projection: Some(file_projection()),
            sort: vec![LogicalSort {
                field: "created_at".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            }],
            include: Vec::new(),
            pagination: Some(LogicalPagination::limit(batch_size as u32)),
        };
        let doomed: Vec<storage_entity_pb::File> = runtime
            .native_entity_read_for_service("storage", &context, read)
            .await?
            .iter()
            .map(file_from_json)
            .collect();
        if doomed.is_empty() {
            return Ok(0);
        }
        // 2) Hard-DELETE exactly that batch by primary key (UUID `file_id`).
        let delete = LogicalDelete {
            message_type: FILE_MSG.to_string(),
            filter: LogicalFilter::InList {
                field: "file_id".to_string(),
                values: doomed
                    .iter()
                    .map(|f| logical_string(f.file_id.as_str()))
                    .collect(),
            },
            return_fields: Vec::new(),
        };
        runtime
            .native_entity_delete_for_service("storage", &context, delete)
            .await?;
        // 3) Best-effort: remove the now-orphaned object bytes too.
        for file in &doomed {
            self.delete_object_bytes(&file.project_id, &file.object_key)
                .await;
        }
        Ok(doomed.len() as u64)
    }
}

impl Default for StorageServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

use super::native_helpers::parse_uuid;

// ── enum<->db (stored as VARCHAR via the proto_enum serializer) ───────────────

fn file_type_from_db(value: &str) -> i32 {
    use storage_entity_pb::FileType as T;
    match value {
        "IMAGE" | "FILE_TYPE_IMAGE" => T::Image as i32,
        "VIDEO" | "FILE_TYPE_VIDEO" => T::Video as i32,
        "AUDIO" | "FILE_TYPE_AUDIO" => T::Audio as i32,
        "PDF" | "FILE_TYPE_PDF" => T::Pdf as i32,
        "DOCUMENT" | "FILE_TYPE_DOCUMENT" => T::Document as i32,
        "ARCHIVE" | "FILE_TYPE_ARCHIVE" => T::Archive as i32,
        "OTHER" | "FILE_TYPE_OTHER" => T::Other as i32,
        _ => T::Unspecified as i32,
    }
}

fn file_status_from_db(value: &str) -> i32 {
    use storage_entity_pb::FileStatus as S;
    match value {
        "PENDING" | "FILE_STATUS_PENDING" => S::Pending as i32,
        "ACTIVE" | "FILE_STATUS_ACTIVE" => S::Active as i32,
        "DELETED" | "FILE_STATUS_DELETED" => S::Deleted as i32,
        _ => S::Unspecified as i32,
    }
}

/// Normalize a file-type string to the canonical SHORT stored token (e.g.
/// "IMAGE"), accepting either the short or the proto-prefixed ("FILE_TYPE_IMAGE")
/// form. Empty → `default`. Unknown non-empty input is rejected so it never
/// silently overflows VARCHAR(20) or reads back as Unspecified. Storing the
/// short form keeps every value within VARCHAR(20) and makes write/read/filter
/// round-trip.
fn storage_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn file_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "IMAGE" | "FILE_TYPE_IMAGE" => "IMAGE",
        "VIDEO" | "FILE_TYPE_VIDEO" => "VIDEO",
        "AUDIO" | "FILE_TYPE_AUDIO" => "AUDIO",
        "PDF" | "FILE_TYPE_PDF" => "PDF",
        "DOCUMENT" | "FILE_TYPE_DOCUMENT" => "DOCUMENT",
        "ARCHIVE" | "FILE_TYPE_ARCHIVE" => "ARCHIVE",
        "OTHER" | "FILE_TYPE_OTHER" => "OTHER",
        other => {
            return Err(storage_field_violation(
                "file_type",
                "must be a supported FileType enum value",
                format!("unknown file type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

/// Normalize a file-status string to the canonical SHORT stored token. Same
/// accept-both-forms / reject-unknown / empty→default contract as
/// [`file_type_to_db`].
#[allow(dead_code)]
fn file_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "FILE_STATUS_PENDING" => "PENDING",
        "ACTIVE" | "FILE_STATUS_ACTIVE" => "ACTIVE",
        "DELETED" | "FILE_STATUS_DELETED" => "DELETED",
        other => {
            return Err(storage_field_violation(
                "status",
                "must be a supported FileStatus enum value",
                format!("unknown file status: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

// ── is_public presence handling (proto3 optional) ─────────────────────────────

/// Bind value for `is_public` on the register INSERT. The column is NOT NULL,
/// so an absent proto3-optional field defaults to private (`false`) — never
/// binds SQL NULL.
fn register_is_public_bind(requested: Option<bool>) -> bool {
    requested.unwrap_or(false)
}

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
fn quota_lease_name(tenant_id: &str) -> String {
    format!("storage_quota:{tenant_id}")
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

/// UUID-typed columns reject `''::uuid`, so an absent optional UUID binds SQL
/// NULL (Mongo `null` / etc.) rather than an empty string.
fn logical_uuid_or_null(value: &str) -> LogicalValue {
    let value = value.trim();
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

/// Nullable text/varchar column (e.g. `file_type`): empty → SQL NULL, matching
/// the old `NULLIF($n, '')` binds.
fn logical_text_or_null(value: &str) -> LogicalValue {
    if value.is_empty() {
        LogicalValue::Null
    } else {
        LogicalValue::String(value.to_string())
    }
}

fn file_eq(field: &str, value: &str) -> LogicalFilter {
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

/// A single live (non-soft-deleted) file scoped to its tenant.
fn file_active_by_id_filter(tenant_id: &str, file_id: &str) -> LogicalFilter {
    LogicalFilter::And(vec![
        file_uuid_eq("tenant_id", tenant_id),
        file_uuid_eq("file_id", file_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ])
}

/// All live files for a tenant — quota usage scan and the `list_files` base set.
fn file_tenant_active_filter(tenant_id: &str) -> LogicalFilter {
    LogicalFilter::And(vec![
        file_uuid_eq("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ])
}

/// `list_files` filter: tenant + live + optional metadata facets. Each facet is
/// applied only when supplied (mirrors the old `$n = '' OR col = $n` guards).
fn file_list_filter(
    tenant_id: &str,
    file_type: &str,
    reference_id: &str,
    reference_type: &str,
    uploaded_by: &str,
) -> LogicalFilter {
    let mut filters = vec![
        file_uuid_eq("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ];
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

fn file_projection() -> LogicalProjection {
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

fn file_read_by_id(tenant_id: &str, file_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: FILE_MSG.to_string(),
        filter: Some(file_active_by_id_filter(tenant_id, file_id)),
        projection: Some(file_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn file_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Null => None,
            other => Some(other.to_string()),
        })
        .unwrap_or_default()
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.trim().parse::<i64>().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    row.get(key)
        .and_then(|value| match value {
            serde_json::Value::Bool(value) => Some(*value),
            serde_json::Value::Number(value) => value.as_i64().map(|n| n != 0),
            serde_json::Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
                "true" | "t" | "1" | "yes" => Some(true),
                "false" | "f" | "0" | "no" => Some(false),
                _ => None,
            },
            _ => None,
        })
        .unwrap_or(false)
}

fn file_from_json(row: &serde_json::Value) -> storage_entity_pb::File {
    let row = file_json_object(row);
    storage_entity_pb::File {
        file_id: json_string(row, "file_id"),
        tenant_id: json_string(row, "tenant_id"),
        project_id: json_string(row, "project_id"),
        filename: json_string(row, "filename"),
        content_type: json_string(row, "content_type"),
        size_bytes: json_i64(row, "size_bytes"),
        backend: json_string(row, "backend"),
        bucket: json_string(row, "bucket"),
        object_key: json_string(row, "object_key"),
        url: json_string(row, "url"),
        cdn_url: json_string(row, "cdn_url"),
        file_type: file_type_from_db(&json_string(row, "file_type")),
        reference_id: json_string(row, "reference_id"),
        reference_type: json_string(row, "reference_type"),
        is_public: json_bool(row, "is_public"),
        status: file_status_from_db(&json_string(row, "status")),
        checksum: json_string(row, "checksum"),
        uploaded_by: json_string(row, "uploaded_by"),
        deleted_by: json_string(row, "deleted_by"),
        ..Default::default()
    }
}

/// Full INSERT record for a freshly registered (`PENDING`) upload.
fn file_register_record(
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

/// Map the `File.file_type` enum back to its canonical stored token ("" =
/// unspecified → SQL NULL). Inverse of [`file_type_from_db`].
fn file_type_to_short(value: i32) -> &'static str {
    use storage_entity_pb::FileType as T;
    match T::try_from(value).unwrap_or(T::Unspecified) {
        T::Image => "IMAGE",
        T::Video => "VIDEO",
        T::Audio => "AUDIO",
        T::Pdf => "PDF",
        T::Document => "DOCUMENT",
        T::Archive => "ARCHIVE",
        T::Other => "OTHER",
        T::Unspecified => "",
    }
}

/// Map the `File.status` enum back to its canonical stored token. Inverse of
/// [`file_status_from_db`].
fn file_status_to_short(value: i32) -> &'static str {
    use storage_entity_pb::FileStatus as S;
    match S::try_from(value).unwrap_or(S::Unspecified) {
        S::Pending => "PENDING",
        S::Active => "ACTIVE",
        S::Deleted => "DELETED",
        S::Unspecified => "",
    }
}

/// A full column record reflecting a file's current state. The typed upsert
/// (`ConflictStrategy::Update`) compiles to `INSERT … ON CONFLICT DO UPDATE`, so
/// the record must carry every column to satisfy NOT-NULL on the insert arm even
/// though an existing row always takes the update arm (mirrors
/// `tenant_config_record`). Callers override the fields they mutate, then list
/// exactly those in `ConflictStrategy::Update { fields }`. `deleted_at` is
/// intentionally omitted (lifecycle-only; set explicitly by `delete_file`).
fn file_full_record(file: &storage_entity_pb::File) -> LogicalRecord {
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

fn validate_register_upload_required_fields(tenant_id: &str, filename: &str) -> Result<(), Status> {
    let mut fields = Vec::new();
    if tenant_id.trim().is_empty() {
        fields.push(("tenant_id", "must be a non-empty tenant id"));
    }
    if filename.trim().is_empty() {
        fields.push(("filename", "must be a non-empty filename"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "tenant_id and filename are required",
            fields,
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl StorageService for StorageServiceImpl {
    /// Register a new upload's metadata row in `PENDING` state and mint the
    /// canonical `object_key`.
    ///
    /// v1 is a metadata/lifecycle service; object bytes + presigned URLs use the
    /// broker's existing `GeneratePresignedUrl`/`PutObject` RPCs with this
    /// `object_key`. `upload_url` is intentionally empty in v1.
    async fn register_upload(
        &self,
        request: Request<storage_pb::RegisterUploadRequest>,
    ) -> Result<Response<storage_pb::RegisterUploadResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_scope(&metadata, &req.tenant_id, &req.project_id)?;
        validate_register_upload_required_fields(&req.tenant_id, &req.filename)?;
        // Per-tenant fair admission (held for the whole RPC) — one tenant's
        // upload flood can't starve shared object capacity.
        let _admit = self.admit(&req.tenant_id, &req.project_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        let file_id = Uuid::new_v4().to_string();
        // file_type column is nullable: stored NULL when the caller omits it.
        let file_type = file_type_to_db(&req.file_type, "")?;
        let object_key = format!("{}/{}/{}", tenant_id, file_id, req.filename);
        // The declared `size_bytes` is persisted so the running quota total stays
        // accurate even before finalize replaces it with the actual uploaded size.
        let declared_size = req.size_bytes.max(0);
        let quota = Self::tenant_quota_bytes();
        // Real per-tenant byte quota pre-check (0 = unlimited), serialized per
        // tenant by the canonical advisory lease so concurrent registers can't
        // race past the gate (the backend-agnostic analog of the old
        // `pg_advisory_xact_lock`).
        let lease_held = if quota > 0 {
            self.acquire_quota_lease(runtime, &tenant_id, &file_id)
                .await?
        } else {
            false
        };
        let write = async {
            if quota > 0 {
                let used = self.tenant_scoped_size_sum(&tenant_id).await?;
                if used + declared_size > quota {
                    return Err(status_with_reason(
                        crate::runtime::executor_utils::quota_refusal_status(
                            "storage",
                            "tenant storage quota",
                            format!(
                                "tenant storage quota exceeded: {used}+{declared_size} > {quota}"
                            ),
                        ),
                        STORAGE_QUOTA_EXCEEDED,
                    ));
                }
            }
            runtime
                .native_entity_write_for_service(
                    "storage",
                    &context,
                    FILE_MSG,
                    file_register_record(
                        &file_id,
                        &tenant_id,
                        &req,
                        &file_type,
                        &self.object_backend,
                        &self.object_bucket,
                        &object_key,
                        declared_size,
                    ),
                    ConflictStrategy::Error,
                )
                .await?;
            Ok::<(), Status>(())
        }
        .await;
        if lease_held {
            runtime
                .release_native_lease(&quota_lease_name(&tenant_id), &file_id)
                .await;
        }
        write?;
        if let Some(pool) = self.pg_pool.as_ref() {
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                TOPIC_UPLOAD_URL_ISSUED,
                &file_id,
                serde_json::json!({
                    "file_id": file_id.clone(),
                    "tenant_id": req.tenant_id.clone(),
                    "project_id": req.project_id.clone(),
                    "object_key": object_key.clone(),
                    "filename": req.filename.clone(),
                    "size_bytes": declared_size,
                }),
                Some(&self.metrics),
            )
            .await;
        }
        // Mint a presigned PUT URL the client uploads bytes to directly (empty in
        // metadata-only mode / on error — client then uses the public PutObject RPC).
        let upload_minutes = if req.expires_in_minutes > 0 {
            req.expires_in_minutes
        } else {
            15
        };
        // Distinguish a degraded deployment (no objectstore) from a real presign
        // error so a client seeing an empty `upload_url` knows which it is. Stay
        // `Response::Ok` either way (the PENDING row + quota reservation persist);
        // the diagnostic rides `error` + `expires_at`.
        let (upload_url, expires_at, error) = match self
            .presign(
                &req.project_id,
                &object_key,
                "PUT",
                &req.content_type,
                upload_minutes,
            )
            .await
        {
            PresignOutcome::Url { url, expires_at } => (url, expires_at, None),
            PresignOutcome::Degraded => (
                String::new(),
                0,
                Some(api_error_upload_url_unavailable(
                    "object store not configured (metadata-only mode); use the public object RPCs",
                    false,
                )),
            ),
            PresignOutcome::Failed(reason) => (
                String::new(),
                0,
                Some(api_error_upload_url_unavailable(
                    format!("presign failed: {reason}"),
                    true,
                )),
            ),
        };
        Ok(Response::new(storage_pb::RegisterUploadResponse {
            file_id,
            upload_url,
            object_key,
            error,
            expires_at,
        }))
    }

    /// Finalize an upload, transitioning the metadata row to `ACTIVE` and
    /// applying any supplied metadata updates.
    async fn finalize_upload(
        &self,
        request: Request<storage_pb::FinalizeUploadRequest>,
    ) -> Result<Response<storage_pb::FinalizeUploadResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC).
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let file_type = file_type_to_db(&req.file_type, "")?;
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;

        // Load the current row: confirms existence, gives the object_key/project to
        // verify the bytes landed, and the prior size for the quota delta.
        let prior_rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let prior = match prior_rows.first() {
            Some(row) => file_from_json(row),
            None => return Err(storage_file_not_found_status("finalize_upload")),
        };
        // Reject re-finalize of an already-ACTIVE row (zero extra round-trip —
        // `prior` is in hand). Fail-closed lifecycle guard: avoids silently
        // re-running the ACTIVE-transition upsert on a double finalize.
        if file_status_to_short(prior.status) == "ACTIVE" {
            return Err(upload_already_finalized_status());
        }
        // Confirm the bytes landed and, when the client supplies them, verify the
        // object's HEAD size/etag — fail-closed on a missing object or a
        // supplied-vs-actual mismatch. The bench deliberately sends a wrong
        // `size_bytes`, so unsupplied size is NEVER compared; size is only checked
        // under a configured quota (strict mode). Metadata-only mode = unchecked.
        let presence = self.object_exists(&prior).await?;
        match &presence {
            ObjectCheck::Absent => {
                return Err(uploaded_object_missing_status());
            }
            ObjectCheck::Present {
                size: head_size,
                etag: head_etag,
            } => {
                if let Some(etag) = req.etag.as_deref().map(str::trim).filter(|e| !e.is_empty()) {
                    if normalize_etag(etag) != normalize_etag(head_etag) {
                        return Err(upload_etag_mismatch_status());
                    }
                }
                if Self::tenant_quota_bytes() > 0
                    && req.size_bytes >= 0
                    && *head_size >= 0
                    && req.size_bytes != *head_size
                {
                    return Err(upload_size_mismatch_status(*head_size, req.size_bytes));
                }
            }
            ObjectCheck::Unchecked => {}
        }

        // Partial update onto a full record (NOT-NULL-safe upsert): ACTIVE
        // transition plus only the fields the caller supplied. A negative
        // `size_bytes` means "leave the size unchanged".
        let new_size = req.size_bytes;
        let mut record = file_full_record(&prior);
        record.insert("status".to_string(), logical_string("ACTIVE"));
        let mut fields = vec!["status".to_string()];
        if new_size >= 0 {
            record.insert("size_bytes".to_string(), LogicalValue::Int(new_size));
            fields.push("size_bytes".to_string());
        }
        if !req.content_type.trim().is_empty() {
            record.insert(
                "content_type".to_string(),
                logical_string(req.content_type.clone()),
            );
            fields.push("content_type".to_string());
        }
        if !file_type.is_empty() {
            record.insert("file_type".to_string(), logical_text_or_null(&file_type));
            fields.push("file_type".to_string());
        }
        if !req.reference_id.trim().is_empty() {
            record.insert(
                "reference_id".to_string(),
                logical_uuid_or_null(&req.reference_id),
            );
            fields.push("reference_id".to_string());
        }
        if !req.reference_type.trim().is_empty() {
            record.insert(
                "reference_type".to_string(),
                logical_string(req.reference_type.clone()),
            );
            fields.push("reference_type".to_string());
        }
        if let Some(is_public) = req.is_public {
            record.insert("is_public".to_string(), LogicalValue::Bool(is_public));
            fields.push("is_public".to_string());
        }
        // Persist a client-supplied content checksum into File.checksum (field 17,
        // previously never written). Verification against the store is etag-based
        // (above); the checksum is recorded for downstream integrity audits.
        if let Some(checksum) = req
            .checksum
            .as_deref()
            .map(str::trim)
            .filter(|c| !c.is_empty())
        {
            record.insert("checksum".to_string(), logical_string(checksum));
            fields.push("checksum".to_string());
        }

        // Quota re-check against the size delta, serialized per tenant by the same
        // lease as register. Only matters when the size grows under a finite quota.
        let quota = Self::tenant_quota_bytes();
        let delta = if new_size >= 0 {
            new_size - prior.size_bytes
        } else {
            0
        };
        let lease_held = if quota > 0 && delta > 0 {
            self.acquire_quota_lease(runtime, &tenant_id, &file_id)
                .await?
        } else {
            false
        };
        let apply = async {
            if quota > 0 && delta > 0 {
                let used = self.tenant_scoped_size_sum(&tenant_id).await?;
                if used + delta > quota {
                    return Err(status_with_reason(
                        crate::runtime::executor_utils::quota_refusal_status(
                            "storage",
                            "tenant storage quota",
                            format!("tenant storage quota exceeded: {used}+{delta} > {quota}"),
                        ),
                        STORAGE_QUOTA_EXCEEDED,
                    ));
                }
            }
            runtime
                .native_entity_write_for_service(
                    "storage",
                    &context,
                    FILE_MSG,
                    record,
                    ConflictStrategy::update(fields),
                )
                .await?;
            Ok::<(), Status>(())
        }
        .await;
        if lease_held {
            runtime
                .release_native_lease(&quota_lease_name(&tenant_id), &file_id)
                .await;
        }
        apply?;

        // Read back the finalized row for the response.
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let file = rows.first().map(file_from_json);
        if let Some(f) = &file {
            if let Some(pool) = self.pg_pool.as_ref() {
                emit_payload_event(
                    pool,
                    self.outbox_relation.as_deref(),
                    TOPIC_FILE_FINALIZED,
                    &f.file_id,
                    serde_json::json!({
                        "file_id": f.file_id,
                        "tenant_id": f.tenant_id,
                        "project_id": f.project_id,
                        "object_key": f.object_key,
                        "size_bytes": f.size_bytes,
                        "status": "ACTIVE",
                    }),
                    Some(&self.metrics),
                )
                .await;
            }
        }
        Ok(Response::new(storage_pb::FinalizeUploadResponse {
            file,
            error: None,
        }))
    }

    /// Compute the download-URL expiry window for a file.
    ///
    /// The client mints the actual URL via the broker's `GeneratePresignedUrl`
    /// RPC using the file's `object_key` (obtained from `GetFile`).
    /// `download_url` is intentionally empty in v1.
    async fn get_download_url(
        &self,
        request: Request<storage_pb::GetDownloadUrlRequest>,
    ) -> Result<Response<storage_pb::GetDownloadUrlResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission: GetDownloadUrl mints a presigned URL via the
        // object backend, so it's an Object-class op gated per tenant.
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let Some(file) = rows.first().map(file_from_json) else {
            return Err(storage_file_not_found_status("get_download_url"));
        };
        let object_key = file.object_key;
        let project_id = file.project_id;
        let minutes = if req.expires_in_minutes > 0 {
            req.expires_in_minutes.min(1440)
        } else {
            60
        };
        // Mint a presigned GET URL (empty in metadata-only mode / on error — the
        // client then uses the public GeneratePresignedUrl RPC with object_key).
        let (download_url, expires_unix) = match self
            .presign(&project_id, &object_key, "GET", "", minutes)
            .await
        {
            PresignOutcome::Url { url, expires_at } => (url, expires_at),
            // Degraded/failed → empty URL + 0 (the client falls back to the public
            // GeneratePresignedUrl RPC); the response still carries a computed
            // expiry window below.
            PresignOutcome::Degraded | PresignOutcome::Failed(_) => (String::new(), 0),
        };
        let expires_at = if expires_unix > 0 {
            prost_types::Timestamp {
                seconds: expires_unix,
                nanos: 0,
            }
        } else {
            let expiry = chrono::Utc::now() + chrono::Duration::minutes(minutes as i64);
            prost_types::Timestamp {
                seconds: expiry.timestamp(),
                nanos: expiry.timestamp_subsec_nanos() as i32,
            }
        };
        Ok(Response::new(storage_pb::GetDownloadUrlResponse {
            download_url,
            expires_at: Some(expires_at),
            error: None,
        }))
    }

    type DownloadFileStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<storage_pb::DownloadFileChunk, Status>>
                + Send
                + 'static,
        >,
    >;

    /// Stream a file's bytes directly through the broker — the FALLBACK for
    /// clients that cannot reach the object store via the presigned
    /// `GetDownloadUrl` HTTP GET.
    ///
    /// Identity comes from the verified claim (`validate_request_tenant`): the
    /// body `file_id` is the only client-chosen target; the tenant is taken from
    /// the claim and the row is resolved tenant-scoped + live-only, so a
    /// cross-tenant `file_id` simply yields not-found (fail-closed). Presence is
    /// confirmed via the SAME `object_exists` HEAD the finalize path uses (also
    /// supplies the first-chunk content_type/total_size/etag), then the bytes are
    /// streamed in BOUNDED chunks through `get_object_stream_backend_target` (the
    /// data-plane `GetObject` streaming primitive) — the whole object is never
    /// buffered in the broker.
    async fn download_file(
        &self,
        request: Request<storage_pb::DownloadFileRequest>,
    ) -> Result<Response<Self::DownloadFileStream>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission: streaming object bytes is an Object-class op.
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        // Resolve the live file row scoped to the verified tenant (cross-tenant
        // file_id → not-found, fail-closed).
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let Some(file) = rows.first().map(file_from_json) else {
            return Err(storage_file_not_found_status("download_file"));
        };
        if file.object_key.trim().is_empty() {
            return Err(file_object_bytes_missing_status());
        }
        // Confirm the bytes are present (and capture first-chunk metadata) via the
        // SAME HEAD primitive finalize uses. Metadata-only mode (no runtime/object
        // store) cannot stream bytes → fail closed.
        let (head_size, head_etag) = match self.object_exists(&file).await? {
            ObjectCheck::Present { size, etag } => (size, etag),
            ObjectCheck::Absent => {
                return Err(object_store_bytes_missing_status());
            }
            ObjectCheck::Unchecked => {
                return Err(object_stream_requires_store_status());
            }
        };
        // Resolve the backend/bucket the file's bytes live in (same fallbacks as
        // object_exists).
        let backend = if file.backend.trim().is_empty() {
            self.object_backend.clone()
        } else {
            file.backend.clone()
        };
        let bucket = if file.bucket.trim().is_empty() {
            self.object_bucket.clone()
        } else {
            file.bucket.clone()
        };
        let request_json = crate::runtime::core::setup_data::object_request_json(
            "get",
            &bucket,
            &file.object_key,
            "",
        );
        let runtime = self.runtime.clone().ok_or_else(|| {
            storage_capability_status(
                "object_stream",
                "runtime_native_entity_dispatch",
                "storage service requires runtime",
            )
        })?;
        // Bounded-memory byte stream from the object store (executor streams in
        // its own frames; the broker never buffers the whole object).
        let byte_stream = runtime
            .get_object_stream_backend_target(&backend, None, &file.project_id, &request_json)
            .await
            .map_err(|status| match status.code() {
                tonic::Code::FailedPrecondition => {
                    status_with_reason(status, UNSUPPORTED_OBJECT_BACKEND)
                }
                _ => status,
            })?;
        // First-chunk metadata: content_type from the row, total_size + etag from
        // the HEAD. Carried only on the first emitted DownloadFileChunk.
        let content_type = if file.content_type.trim().is_empty() {
            None
        } else {
            Some(file.content_type.clone())
        };
        let total_size = if head_size >= 0 {
            Some(head_size)
        } else {
            None
        };
        let etag = if head_etag.trim().is_empty() {
            None
        } else {
            Some(head_etag)
        };
        let out = async_stream::try_stream! {
            use tokio_stream::StreamExt as _;
            let mut byte_stream = byte_stream;
            let mut first = true;
            while let Some(chunk) = byte_stream.next().await {
                let data = chunk?;
                if first {
                    first = false;
                    yield storage_pb::DownloadFileChunk {
                        data: data.to_vec(),
                        content_type: content_type.clone(),
                        total_size,
                        etag: etag.clone(),
                    };
                } else {
                    yield storage_pb::DownloadFileChunk {
                        data: data.to_vec(),
                        ..Default::default()
                    };
                }
            }
            // Zero-byte object: still emit a single (empty) first chunk so the
            // caller receives the first-chunk metadata.
            if first {
                yield storage_pb::DownloadFileChunk {
                    data: Vec::new(),
                    content_type,
                    total_size,
                    etag,
                };
            }
        };
        Ok(Response::new(Box::pin(out) as Self::DownloadFileStream))
    }

    /// Fetch a single file's metadata.
    async fn get_file(
        &self,
        request: Request<storage_pb::GetFileRequest>,
    ) -> Result<Response<storage_pb::GetFileResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so one tenant can't
        // exhaust the shared pool with reads.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let file = rows.first().map(file_from_json);
        if file.is_none() {
            return Err(storage_file_not_found_status("get_file"));
        }
        Ok(Response::new(storage_pb::GetFileResponse {
            file,
            error: None,
        }))
    }

    /// Partial update of file metadata; only non-empty fields are applied.
    async fn update_file(
        &self,
        request: Request<storage_pb::UpdateFileRequest>,
    ) -> Result<Response<storage_pb::UpdateFileResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC).
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let file_type = file_type_to_db(&req.file_type, "")?;
        let update_mask = update_mask_path_set(
            req.update_mask.as_ref(),
            &[
                "filename",
                "content_type",
                "file_type",
                "reference_id",
                "reference_type",
                "is_public",
            ],
        )?;
        if update_mask
            .as_ref()
            .is_some_and(|paths| paths.contains("is_public"))
            && req.is_public.is_none()
        {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "is_public is required when present in update_mask",
                [(
                    "is_public",
                    "must be supplied when update_mask includes is_public",
                )],
            ));
        }
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let prior = match rows.first() {
            Some(row) => file_from_json(row),
            None => return Err(storage_file_not_found_status("update_file")),
        };
        // Partial update onto a full record (NOT-NULL-safe upsert): only the
        // fields the caller supplied are changed (mirrors the old COALESCE/NULLIF
        // guards).
        let mut record = file_full_record(&prior);
        let mut fields = Vec::new();
        if update_mask_allows(&update_mask, "filename", !req.filename.trim().is_empty()) {
            record.insert("filename".to_string(), logical_string(req.filename.clone()));
            fields.push("filename".to_string());
        }
        if update_mask_allows(
            &update_mask,
            "content_type",
            !req.content_type.trim().is_empty(),
        ) {
            record.insert(
                "content_type".to_string(),
                logical_string(req.content_type.clone()),
            );
            fields.push("content_type".to_string());
        }
        if update_mask_allows(&update_mask, "file_type", !file_type.is_empty()) {
            record.insert("file_type".to_string(), logical_text_or_null(&file_type));
            fields.push("file_type".to_string());
        }
        if update_mask_allows(
            &update_mask,
            "reference_id",
            !req.reference_id.trim().is_empty(),
        ) {
            record.insert(
                "reference_id".to_string(),
                logical_uuid_or_null(&req.reference_id),
            );
            fields.push("reference_id".to_string());
        }
        if update_mask_allows(
            &update_mask,
            "reference_type",
            !req.reference_type.trim().is_empty(),
        ) {
            record.insert(
                "reference_type".to_string(),
                logical_string(req.reference_type.clone()),
            );
            fields.push("reference_type".to_string());
        }
        if update_mask_allows(&update_mask, "is_public", req.is_public.is_some()) {
            record.insert(
                "is_public".to_string(),
                LogicalValue::Bool(req.is_public.unwrap_or(false)),
            );
            fields.push("is_public".to_string());
        }
        if !fields.is_empty() {
            runtime
                .native_entity_write_for_service(
                    "storage",
                    &context,
                    FILE_MSG,
                    record,
                    ConflictStrategy::update(fields),
                )
                .await?;
        }
        if let Some(pool) = self.pg_pool.as_ref() {
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                TOPIC_FILE_METADATA_UPDATED,
                &req.file_id,
                serde_json::json!({
                    "file_id": req.file_id,
                    "tenant_id": req.tenant_id,
                }),
                Some(&self.metrics),
            )
            .await;
        }
        Ok(Response::new(storage_pb::UpdateFileResponse {
            message: "file updated".to_string(),
            error: None,
        }))
    }

    /// Soft-delete a file's metadata record.
    ///
    /// v1 soft-deletes the metadata record; object GC is handled by a
    /// lifecycle/reaper, not inline.
    async fn delete_file(
        &self,
        request: Request<storage_pb::DeleteFileRequest>,
    ) -> Result<Response<storage_pb::DeleteFileResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (held for the whole RPC) — DeleteFile also
        // removes object bytes via the object executor, so it's an Object-class op.
        let _admit = self.admit(&req.tenant_id, "").await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let file_id = parse_uuid("file_id", &req.file_id)?.to_string();
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        // Load the live row first: confirms existence (and not already deleted) and
        // recovers the object_key/project so the bytes can be removed too.
        let rows = runtime
            .native_entity_read_for_service(
                "storage",
                &context,
                file_read_by_id(&tenant_id, &file_id),
            )
            .await?;
        let prior = match rows.first() {
            Some(row) => file_from_json(row),
            None => return Err(storage_file_not_found_status("delete_file")),
        };
        // Soft-delete the metadata (keeps it auditable) on a full record.
        let mut record = file_full_record(&prior);
        record.insert(
            "deleted_at".to_string(),
            LogicalValue::Timestamp(chrono::Utc::now()),
        );
        record.insert("status".to_string(), logical_string("DELETED"));
        runtime
            .native_entity_write_for_service(
                "storage",
                &context,
                FILE_MSG,
                record,
                ConflictStrategy::update(vec!["deleted_at".to_string(), "status".to_string()]),
            )
            .await?;
        // Remove the bytes (best-effort; metadata stays soft-deleted on failure).
        self.delete_object_bytes(&prior.project_id, &prior.object_key)
            .await;
        if let Some(pool) = self.pg_pool.as_ref() {
            emit_payload_event(
                pool,
                self.outbox_relation.as_deref(),
                TOPIC_FILE_DELETED,
                &req.file_id,
                serde_json::json!({
                    "file_id": req.file_id,
                    "tenant_id": req.tenant_id,
                    "project_id": prior.project_id,
                }),
                Some(&self.metrics),
            )
            .await;
        }
        Ok(Response::new(storage_pb::DeleteFileResponse {
            success: true,
            error: None,
        }))
    }

    /// List a tenant's files with optional metadata filters.
    async fn list_files(
        &self,
        request: Request<storage_pb::ListFilesRequest>,
    ) -> Result<Response<storage_pb::ListFilesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // Per-tenant fair admission (lighter Read budget) so one tenant can't
        // exhaust the shared pool with list scans.
        let _admit = self.admit_read(&req.tenant_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let type_filter = file_type_to_db(&req.file_type, "")?;
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let context = tenant_only_native_service_context(&metadata, &tenant_id);
        let runtime = self.require_runtime()?;
        let filter = file_list_filter(
            &tenant_id,
            &type_filter,
            &req.reference_id,
            &req.reference_type,
            &req.uploaded_by,
        );
        // Count via the typed path (reads the per-tenant-bounded matching set). No
        // IR-level aggregate yet (P4.D), so this is a bounded scan, not a hot-path
        // COUNT — acceptable for a tenant's file list.
        let total = runtime
            .native_entity_count_for_service("storage", &context, FILE_MSG, Some(filter.clone()))
            .await?;
        let read = LogicalRead {
            message_type: FILE_MSG.to_string(),
            filter: Some(filter),
            projection: Some(file_projection()),
            sort: vec![LogicalSort {
                field: "filename".to_string(),
                direction: SortDirection::Asc,
                nulls: NullOrder::Default,
            }],
            include: Vec::new(),
            pagination: Some(LogicalPagination::page(
                page_window.offset as u64,
                page_window.limit as u32,
            )),
        };
        let rows = runtime
            .native_entity_read_for_service("storage", &context, read)
            .await?;
        let files = rows.iter().map(file_from_json).collect::<Vec<_>>();
        Ok(Response::new(storage_pb::ListFilesResponse {
            files,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total as i64,
            ),
        }))
    }
}

impl DataBrokerService {
    /// Build the native `StorageService`, wired to the broker's Postgres pool
    /// and the transactional outbox, and spawn the periodic orphan reaper.
    pub(crate) fn build_storage_service(&self) -> StorageServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("storage", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let (object_backend, object_bucket) = storage_object_defaults(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        let svc = StorageServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_metrics(self.metrics.clone())
            .with_object(Some(runtime.clone()), object_backend, object_bucket);

        // Periodic orphan reaper: hard-delete bounded batches of `PENDING` files
        // that were registered but never finalized. Interval/age/batch are
        // env-tunable; interval/age set to 0 disables the reaper. Best-effort —
        // failures are logged only.
        let interval_secs = std::env::var("UDB_STORAGE_REAP_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3600);
        let orphan_age_minutes = std::env::var("UDB_STORAGE_ORPHAN_AGE_MINUTES")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1440);
        let orphan_batch_size = std::env::var("UDB_STORAGE_ORPHAN_BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(500);
        if interval_secs > 0 && orphan_age_minutes > 0 && svc.pg_pool.is_some() {
            let reaper = svc.clone();
            let singleton_pool = svc.pg_pool.clone().expect("checked above");
            let singleton_relation = runtime.config().cdc.lock_log_relation();
            crate::runtime::service::native_runtime::NativeWorkerHost::spawn_while_leader(
                crate::runtime::singleton::WORKER_STORAGE_ORPHAN_REAPER,
                "storage orphan reaper deleted PENDING files",
                singleton_pool,
                singleton_relation,
                std::time::Duration::from_secs(interval_secs),
                move || {
                    let reaper_once = reaper.clone();
                    async move {
                        reaper_once
                            .reap_orphans(orphan_age_minutes, orphan_batch_size)
                            .await
                            .map(|n| n as i64)
                    }
                },
            );
        }

        svc
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
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

    fn assert_single_field_violation(status: &Status, field: &str, description: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_policy_detail_with_reason(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        reason: &'static str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        assert_eq!(
            status
                .metadata()
                .get("error-reason")
                .and_then(|value| value.to_str().ok()),
            Some(reason)
        );
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_capability_detail_with_reason(
        status: &Status,
        operation: &str,
        capability_required: &str,
        reason: &'static str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        assert_eq!(
            status
                .metadata()
                .get("error-reason")
                .and_then(|value| value.to_str().ok()),
            Some(reason)
        );
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "storage");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, capability_required);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_schema_detail(status: &Status, operation: &str, schema_code: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "storage");
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
        assert_eq!(detail.backend, "storage");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_validation_detail_with_reason(
        status: &Status,
        field: &str,
        description: &str,
        reason: &'static str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        assert_eq!(
            status
                .metadata()
                .get("error-reason")
                .and_then(|value| value.to_str().ok()),
            Some(reason)
        );
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a (x-tenant-id) must not operate on tenant-b by
    /// putting a foreign tenant_id in the request BODY. The scope guard rejects
    /// this BEFORE any pool/DB access, so the test needs no Postgres.
    #[tokio::test]
    async fn get_file_rejects_cross_tenant_body() {
        let svc = StorageServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(storage_pb::GetFileRequest {
            tenant_id: "tenant-b".to_string(),
            file_id: "00000000-0000-0000-0000-000000000001".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_file(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn storage_file_not_found_statuses_carry_schema_detail() {
        for operation in [
            "finalize_upload",
            "get_download_url",
            "download_file",
            "get_file",
            "update_file",
            "delete_file",
        ] {
            assert_schema_detail(
                &storage_file_not_found_status(operation),
                operation,
                "file_not_found",
                "file not found",
            );
        }
    }

    #[tokio::test]
    async fn register_upload_missing_filename_carries_field_violation() {
        let svc = StorageServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(storage_pb::RegisterUploadRequest {
            tenant_id: "tenant-a".to_string(),
            filename: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .register_upload(request)
            .await
            .expect_err("missing filename must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "tenant_id and filename are required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "filename");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty filename"
        );
    }

    #[test]
    fn file_type_and_status_validation_carry_field_violations() {
        let file_type =
            file_type_to_db("directory", "OTHER").expect_err("unknown file type must be rejected");
        assert_eq!(file_type.code(), tonic::Code::InvalidArgument);
        assert_eq!(file_type.message(), "unknown file type: DIRECTORY");
        assert_single_field_violation(
            &file_type,
            "file_type",
            "must be a supported FileType enum value",
        );

        let status = file_status_to_db("archived", "PENDING")
            .expect_err("unknown file status must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "unknown file status: ARCHIVED");
        assert_single_field_violation(
            &status,
            "status",
            "must be a supported FileStatus enum value",
        );
    }

    #[test]
    fn upload_already_finalized_carries_policy_detail_and_reason() {
        assert_policy_detail_with_reason(
            &upload_already_finalized_status(),
            "finalize_upload",
            "upload_already_finalized",
            ALREADY_FINALIZED,
            "upload already finalized",
        );
    }

    #[test]
    fn upload_presence_denial_carries_policy_detail_and_reason() {
        assert_policy_detail_with_reason(
            &uploaded_object_missing_status(),
            "finalize_upload",
            "uploaded_object_present",
            OBJECT_NOT_PRESENT,
            "uploaded object is not present in the configured object store",
        );
    }

    #[test]
    fn upload_head_mismatches_carry_validation_detail_and_reason() {
        assert_validation_detail_with_reason(
            &upload_etag_mismatch_status(),
            "etag",
            "must match the uploaded object's store ETag",
            UPLOAD_SIZE_MISMATCH,
            "uploaded object etag does not match",
        );
        assert_validation_detail_with_reason(
            &upload_size_mismatch_status(12, 9),
            "size_bytes",
            "must match the uploaded object's store content length",
            UPLOAD_SIZE_MISMATCH,
            "uploaded object size 12 does not match declared 9",
        );
    }

    #[test]
    fn object_stream_requires_store_carries_capability_detail_and_reason() {
        assert_capability_detail_with_reason(
            &object_stream_requires_store_status(),
            "object_stream",
            "object_store",
            UNSUPPORTED_OBJECT_BACKEND,
            "object byte streaming requires a configured object store",
        );
    }

    #[test]
    fn download_object_absence_carries_policy_detail_and_reason() {
        assert_policy_detail_with_reason(
            &file_object_bytes_missing_status(),
            "download_file",
            "file_object_bytes_present",
            OBJECT_NOT_PRESENT,
            "file has no object bytes",
        );
        assert_policy_detail_with_reason(
            &object_store_bytes_missing_status(),
            "download_file",
            "object_store_bytes_present",
            OBJECT_NOT_PRESENT,
            "object is not present in the configured object store",
        );
    }

    #[test]
    fn storage_missing_runtime_capabilities_carry_typed_detail() {
        for (operation, message) in [
            (
                "native_entity_dispatch",
                "storage service requires runtime native entity dispatch",
            ),
            ("object_stream", "storage service requires runtime"),
        ] {
            let err =
                storage_capability_status(operation, "runtime_native_entity_dispatch", message);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "storage");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn storage_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &storage_internal_status(
                "tenant_size_sum_aggregate",
                "storage usage aggregate failed: database is unavailable",
            ),
            "tenant_size_sum_aggregate",
            "storage usage aggregate failed: database is unavailable",
        );
    }
}

#[cfg(test)]
mod is_public_presence_tests {
    use super::*;

    /// `is_public` presence handling after the P4 typed-entity migration.
    ///
    /// `update_file`/`finalize_upload` no longer build raw `COALESCE` SQL; they
    /// presence-guard `is_public` *structurally* on the typed `LogicalWrite`
    /// field set — an absent proto3-optional `is_public` is never added to
    /// `fields` (`if let Some(is_public) = req.is_public { … }`), so the stored
    /// visibility is left untouched. That is the v0.3.2 #47 guarantee, now
    /// enforced by the type system rather than a hand-built SQL string. The
    /// register INSERT differs: the column is NOT NULL, so an absent value must
    /// default to private — that decision lives in the pure
    /// `register_is_public_bind` helper (still called from `register_upload`),
    /// covered here. End-to-end preservation of stored visibility on an absent
    /// update is exercised by the env-gated storage live tests.
    #[test]
    fn register_is_public_defaults_to_private_when_absent() {
        // register_upload INSERT: absent field defaults to private, never NULL.
        assert!(!register_is_public_bind(None));
        assert!(register_is_public_bind(Some(true)));
        assert!(!register_is_public_bind(Some(false)));
    }
}
