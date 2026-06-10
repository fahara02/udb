//! Native `StorageService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_storage.files` table.
//!
//! Mirrors `tenant_service`: no in-memory store, no hand-mapped schema. Table
//! and column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.
//!
//! v1 is a metadata/lifecycle service; object bytes + presigned URLs use the
//! broker's existing `GeneratePresignedUrl`/`PutObject` RPCs with the
//! `object_key` minted here. The URL fields returned by this service are
//! intentionally empty in v1 — clients mint the actual URLs separately.

use std::sync::Arc;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};

use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::proto::udb::core::storage::services::v1 as storage_pb;
use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageService;
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::storage::services::v1::storage_service_server::StorageServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    DEFAULT_OBJECT_BACKEND, DEFAULT_OBJECT_BUCKET, admit_on as native_admit_on, emit_payload_event,
    storage_object_defaults, validate_request_scope, validate_request_tenant,
};

const FILE_MSG: &str = "udb.core.storage.entity.v1.File";

/// Topics for the storage domain events emitted via the transactional outbox
/// (→ CDC → Kafka). Dot-only per the project's Kafka topic convention.
const TOPIC_UPLOAD_URL_ISSUED: &str = "udb.storage.file.upload_url_issued.v1";
const TOPIC_FILE_FINALIZED: &str = "udb.storage.file.finalized.v1";
const TOPIC_FILE_METADATA_UPDATED: &str = "udb.storage.file.metadata_updated.v1";
const TOPIC_FILE_DELETED: &str = "udb.storage.file.deleted.v1";

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
    ) -> (String, i64) {
        let Some(runtime) = self.runtime.as_ref() else {
            return (String::new(), 0);
        };
        let ttl_secs = (ttl_minutes.max(1) as i64 * 60).min(7 * 24 * 3600) as i32;
        match runtime
            .presign_object_backend_target(
                None,
                project_id,
                &self.object_bucket,
                object_key,
                method,
                content_type,
                ttl_secs,
            )
            .await
        {
            Ok((url, expires_at_unix)) => (url, expires_at_unix),
            Err(err) => {
                tracing::warn!(error = %err, object_key, method, "storage presign failed; returning empty url");
                (String::new(), 0)
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
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "storage service requires a Postgres-backed store (no PG pool configured)",
            )
        })
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
    fn tenant_quota_bytes() -> i64 {
        std::env::var("UDB_STORAGE_TENANT_QUOTA_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    /// Sum the live (non-soft-deleted) byte usage for a tenant.
    async fn tenant_used_bytes_on<'e, E>(
        executor: E,
        m: &NativeModel,
        tenant_id: Uuid,
    ) -> Result<i64, Status>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let rel = m.relation.clone();
        sqlx::query_scalar(&format!(
            "SELECT COALESCE(SUM({size_bytes}), 0) FROM {rel} \
             WHERE {tenant_id} = $1::UUID AND {deleted_at} IS NULL",
            size_bytes = m.q("size_bytes"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(tenant_id)
        .fetch_one(executor)
        .await
        .map_err(|err| Status::internal(format!("tenant usage query failed: {err}")))
    }

    /// Serialize quota checks and writes per tenant. This closes the classic
    /// check-then-insert race without adding a durable counter table.
    async fn lock_tenant_quota(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        tenant_id: Uuid,
    ) -> Result<(), Status> {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(tenant_id.to_string())
            .execute(&mut **tx)
            .await
            .map_err(|err| Status::internal(format!("tenant quota lock failed: {err}")))?;
        Ok(())
    }

    async fn object_exists(&self, project_id: &str, object_key: &str) -> Result<bool, Status> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(true);
        };
        if object_key.trim().is_empty() {
            return Ok(false);
        }
        runtime
            .object_exists_backend_target(
                &self.object_backend,
                project_id,
                &self.object_bucket,
                object_key,
            )
            .await
    }

    /// Hard-DELETE orphaned `PENDING` files older than `older_than_minutes`
    /// (uploads that were registered but never finalized). Uses the
    /// auto-injected `created_at` audit column (`audit_fields: true` on the File
    /// table). Returns the number of rows deleted. Best-effort caller.
    pub(crate) async fn reap_orphans(
        &self,
        older_than_minutes: i64,
        batch_size: i64,
    ) -> Result<u64, Status> {
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let batch_size = batch_size.clamp(1, 10_000);
        // Delete a bounded batch and recover object_keys so abandoned bytes are
        // cleaned up too. The CTE gives Postgres an ordered LIMIT for DELETE.
        let rows = sqlx::query(&format!(
            "WITH doomed AS ( \
                SELECT ctid, {object_key}::TEXT AS object_key, \
                       COALESCE({project_id}::TEXT, '') AS project_id FROM {rel} \
                WHERE {status} = 'PENDING' \
                  AND {created_at} < NOW() - ($1 * INTERVAL '1 minute') \
                ORDER BY {created_at} \
                LIMIT $2 \
             ) \
             DELETE FROM {rel} f USING doomed d \
             WHERE f.ctid = d.ctid \
             RETURNING d.object_key, d.project_id",
            status = m.q("status"),
            created_at = m.q("created_at"),
            object_key = m.q("object_key"),
            project_id = m.q("project_id"),
        ))
        .bind(older_than_minutes as f64)
        .bind(batch_size)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("reap orphans failed: {err}")))?;
        for row in &rows {
            let object_key: String = row.try_get("object_key").unwrap_or_default();
            let project_id: String = row.try_get("project_id").unwrap_or_default();
            self.delete_object_bytes(&project_id, &object_key).await;
        }
        Ok(rows.len() as u64)
    }
}

impl Default for StorageServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn file_model() -> NativeModel {
    native_model(
        FILE_MSG,
        &[
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
            "deleted_at",
            "deleted_by",
            // Auto-injected audit column (File has `audit_fields: true`); used by
            // the orphan reaper and not a proto field on the File message.
            "created_at",
        ],
    )
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
            return Err(Status::invalid_argument(format!(
                "unknown file type: {other}"
            )));
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
            return Err(Status::invalid_argument(format!(
                "unknown file status: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

// ── projections + row mappers ─────────────────────────────────────────────────

fn file_select_projection(m: &NativeModel) -> String {
    [
        m.text("file_id"),
        m.text("tenant_id"),
        m.text_or_empty("project_id"),
        m.select("filename"),
        m.text_or_empty("content_type"),
        m.select("size_bytes"),
        m.text_or_empty("backend"),
        m.text_or_empty("bucket"),
        m.text("object_key"),
        m.text_or_empty("url"),
        m.text_or_empty("cdn_url"),
        m.text_or_empty("file_type"),
        m.text_or_empty("reference_id"),
        m.text_or_empty("reference_type"),
        m.select("is_public"),
        m.text_or_empty("status"),
        m.text_or_empty("checksum"),
        m.text_or_empty("uploaded_by"),
        m.text_or_empty("deleted_by"),
    ]
    .join(", ")
}

// ── is_public presence handling (proto3 optional) ─────────────────────────────

/// Bind value for `is_public` on the register INSERT. The column is NOT NULL,
/// so an absent proto3-optional field defaults to private (`false`) — never
/// binds SQL NULL.
fn register_is_public_bind(requested: Option<bool>) -> bool {
    requested.unwrap_or(false)
}

/// Bind value for `is_public` on the presence-guarded UPDATEs
/// ([`finalize_upload_sql`]/[`update_file_sql`]): `None` binds SQL NULL so the
/// `COALESCE($n, is_public)` SET clause keeps the stored visibility; `Some(v)`
/// applies `v`. A partial update can therefore never silently flip a file
/// public/private.
fn update_is_public_bind(requested: Option<bool>) -> Option<bool> {
    requested
}

/// UPDATE statement used by `finalize_upload`. `$7` is the proto3-optional
/// `is_public` (bound via [`update_is_public_bind`]): COALESCE keeps the
/// stored visibility when the field is absent, like the string fields.
fn finalize_upload_sql(m: &NativeModel) -> String {
    format!(
        "UPDATE {rel} SET \
           {status} = 'ACTIVE', \
           {size_bytes} = CASE WHEN $8 >= 0 THEN $8 ELSE {size_bytes} END, \
           {content_type} = COALESCE(NULLIF($3, ''), {content_type}), \
           {file_type} = CASE WHEN $4 = '' THEN {file_type} ELSE $4 END, \
           {reference_id} = CASE WHEN $5 = '' THEN {reference_id} ELSE $5::UUID END, \
           {reference_type} = COALESCE(NULLIF($6, ''), {reference_type}), \
           {is_public} = COALESCE($7, {is_public}) \
         WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
        rel = m.relation,
        status = m.q("status"),
        size_bytes = m.q("size_bytes"),
        content_type = m.q("content_type"),
        file_type = m.q("file_type"),
        reference_id = m.q("reference_id"),
        reference_type = m.q("reference_type"),
        is_public = m.q("is_public"),
        file_id = m.q("file_id"),
        tenant_id = m.q("tenant_id"),
        deleted_at = m.q("deleted_at"),
    )
}

/// UPDATE statement used by `update_file`. `$8` is the proto3-optional
/// `is_public` (bound via [`update_is_public_bind`]): COALESCE keeps the
/// stored visibility when the field is absent, like the string fields.
fn update_file_sql(m: &NativeModel) -> String {
    format!(
        "UPDATE {rel} SET \
           {filename} = COALESCE(NULLIF($3, ''), {filename}), \
           {content_type} = COALESCE(NULLIF($4, ''), {content_type}), \
           {file_type} = CASE WHEN $5 = '' THEN {file_type} ELSE $5 END, \
           {reference_id} = CASE WHEN $6 = '' THEN {reference_id} ELSE $6::UUID END, \
           {reference_type} = COALESCE(NULLIF($7, ''), {reference_type}), \
           {is_public} = COALESCE($8, {is_public}) \
         WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
        rel = m.relation,
        filename = m.q("filename"),
        content_type = m.q("content_type"),
        file_type = m.q("file_type"),
        reference_id = m.q("reference_id"),
        reference_type = m.q("reference_type"),
        is_public = m.q("is_public"),
        file_id = m.q("file_id"),
        tenant_id = m.q("tenant_id"),
        deleted_at = m.q("deleted_at"),
    )
}

fn file_from_row(row: &sqlx::postgres::PgRow) -> Result<storage_entity_pb::File, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode file failed: {e}"));
    Ok(storage_entity_pb::File {
        file_id: row.try_get("file_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        filename: row.try_get("filename").map_err(map)?,
        content_type: row.try_get("content_type").map_err(map)?,
        size_bytes: row.try_get::<i64, _>("size_bytes").map_err(map)?,
        backend: row.try_get("backend").map_err(map)?,
        bucket: row.try_get("bucket").map_err(map)?,
        object_key: row.try_get("object_key").map_err(map)?,
        url: row.try_get("url").map_err(map)?,
        cdn_url: row.try_get("cdn_url").map_err(map)?,
        file_type: file_type_from_db(&row.try_get::<String, _>("file_type").map_err(map)?),
        reference_id: row.try_get("reference_id").map_err(map)?,
        reference_type: row.try_get("reference_type").map_err(map)?,
        is_public: row.try_get::<bool, _>("is_public").map_err(map)?,
        status: file_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        checksum: row.try_get("checksum").map_err(map)?,
        uploaded_by: row.try_get("uploaded_by").map_err(map)?,
        deleted_by: row.try_get("deleted_by").map_err(map)?,
        ..Default::default()
    })
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
        if req.tenant_id.trim().is_empty() || req.filename.trim().is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id and filename are required",
            ));
        }
        // Per-tenant fair admission (held for the whole RPC) — one tenant's
        // upload flood can't starve shared object capacity.
        let _admit = self.admit(&req.tenant_id, &req.project_id).await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| Status::internal(format!("register upload tx begin failed: {err}")))?;
        Self::lock_tenant_quota(&mut tx, tenant_id).await?;
        let file_id = Uuid::new_v4().to_string();
        // file_type column is nullable: NULLIF stores NULL when caller omits it.
        let file_type = file_type_to_db(&req.file_type, "")?;
        let object_key = format!("{}/{}/{}", req.tenant_id, file_id, req.filename);
        // Real per-tenant byte quota pre-check (0 = unlimited). The declared
        // `size_bytes` is persisted so the running total stays accurate even
        // before finalize replaces it with the actual uploaded size.
        let declared_size = req.size_bytes.max(0);
        let quota = Self::tenant_quota_bytes();
        if quota > 0 {
            let used = Self::tenant_used_bytes_on(&mut *tx, &m, tenant_id).await?;
            if used + declared_size > quota {
                return Err(Status::resource_exhausted(format!(
                    "tenant storage quota exceeded: {used}+{} > {quota}",
                    declared_size
                )));
            }
        }
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({file_id}, {tenant_id}, {project_id}, {filename}, {content_type}, {file_type}, \
              {status}, {reference_id}, {reference_type}, {is_public}, {object_key}, {size_bytes}) \
             VALUES ($1::UUID, $2::UUID, NULLIF($3, '')::UUID, $4, $5, NULLIF($6, ''), \
              'PENDING', NULLIF($7, '')::UUID, $8, $9, $10, $11)",
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            filename = m.q("filename"),
            content_type = m.q("content_type"),
            file_type = m.q("file_type"),
            status = m.q("status"),
            reference_id = m.q("reference_id"),
            reference_type = m.q("reference_type"),
            is_public = m.q("is_public"),
            object_key = m.q("object_key"),
            size_bytes = m.q("size_bytes"),
        ))
        .bind(&file_id)
        .bind(tenant_id)
        .bind(&req.project_id)
        .bind(&req.filename)
        .bind(&req.content_type)
        .bind(&file_type)
        .bind(&req.reference_id)
        .bind(&req.reference_type)
        .bind(register_is_public_bind(req.is_public))
        .bind(&object_key)
        .bind(declared_size)
        .execute(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("register upload failed: {err}")))?;
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("register upload tx commit failed: {err}")))?;
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
        // Mint a presigned PUT URL the client uploads bytes to directly (empty in
        // metadata-only mode / on error — client then uses the public PutObject RPC).
        let upload_minutes = if req.expires_in_minutes > 0 {
            req.expires_in_minutes
        } else {
            15
        };
        let (upload_url, _) = self
            .presign(
                &req.project_id,
                &object_key,
                "PUT",
                &req.content_type,
                upload_minutes,
            )
            .await;
        Ok(Response::new(storage_pb::RegisterUploadResponse {
            file_id,
            upload_url,
            object_key,
            error: None,
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let file_id = parse_uuid("file_id", &req.file_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let file_type = file_type_to_db(&req.file_type, "")?;
        let mut tx = pool
            .begin()
            .await
            .map_err(|err| Status::internal(format!("finalize upload tx begin failed: {err}")))?;
        Self::lock_tenant_quota(&mut tx, tenant_id).await?;

        // Real per-tenant byte quota re-check against the size delta (actual
        // uploaded size vs the declared size persisted at register time).
        // A negative `size_bytes` means "leave the size unchanged".
        let new_size = req.size_bytes;
        let prior: Option<(i64, String, String)> = sqlx::query_as(&format!(
            "SELECT {size_bytes}, {object_key}::TEXT AS object_key, \
                    COALESCE({project_id}::TEXT, '') AS project_id FROM {rel} \
             WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
            size_bytes = m.q("size_bytes"),
            object_key = m.q("object_key"),
            project_id = m.q("project_id"),
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("finalize upload failed: {err}")))?;
        let (prior_size, object_key, project_id) = match prior {
            Some(row) => row,
            None => return Err(Status::not_found("file not found")),
        };
        if !self.object_exists(&project_id, &object_key).await? {
            return Err(Status::failed_precondition(
                "uploaded object is not present in the configured object store",
            ));
        }
        let quota = Self::tenant_quota_bytes();
        if quota > 0 && new_size >= 0 {
            let delta = new_size - prior_size;
            if delta > 0 {
                let used = Self::tenant_used_bytes_on(&mut *tx, &m, tenant_id).await?;
                if used + delta > quota {
                    return Err(Status::resource_exhausted(format!(
                        "tenant storage quota exceeded: {used}+{delta} > {quota}"
                    )));
                }
            }
        }

        let result = sqlx::query(&finalize_upload_sql(&m))
            .bind(file_id)
            .bind(tenant_id)
            .bind(&req.content_type)
            .bind(&file_type)
            .bind(&req.reference_id)
            .bind(&req.reference_type)
            .bind(update_is_public_bind(req.is_public))
            .bind(new_size)
            .execute(&mut *tx)
            .await
            .map_err(|err| Status::internal(format!("finalize upload failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("file not found"));
        }
        let projection = file_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| Status::internal(format!("finalize upload failed: {err}")))?;
        let file = match row {
            Some(row) => Some(file_from_row(&row)?),
            None => return Err(Status::not_found("file not found")),
        };
        tx.commit()
            .await
            .map_err(|err| Status::internal(format!("finalize upload tx commit failed: {err}")))?;
        if let Some(f) = &file {
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let file_id = parse_uuid("file_id", &req.file_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {object_key}, {project_id} FROM {rel} \
             WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
            object_key = m.text("object_key"),
            project_id = m.text_or_empty("project_id"),
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get download url failed: {err}")))?;
        let Some(row) = row else {
            return Err(Status::not_found("file not found"));
        };
        let object_key: String = row.try_get("object_key").unwrap_or_default();
        let project_id: String = row.try_get("project_id").unwrap_or_default();
        let minutes = if req.expires_in_minutes > 0 {
            req.expires_in_minutes.min(1440)
        } else {
            60
        };
        // Mint a presigned GET URL (empty in metadata-only mode / on error — the
        // client then uses the public GeneratePresignedUrl RPC with object_key).
        let (download_url, expires_unix) = self
            .presign(&project_id, &object_key, "GET", "", minutes)
            .await;
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let file_id = parse_uuid("file_id", &req.file_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let projection = file_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL",
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get file failed: {err}")))?;
        let file = match row {
            Some(row) => Some(file_from_row(&row)?),
            None => return Err(Status::not_found("file not found")),
        };
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let file_id = parse_uuid("file_id", &req.file_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let file_type = file_type_to_db(&req.file_type, "")?;
        let result = sqlx::query(&update_file_sql(&m))
            .bind(file_id)
            .bind(tenant_id)
            .bind(&req.filename)
            .bind(&req.content_type)
            .bind(&file_type)
            .bind(&req.reference_id)
            .bind(&req.reference_type)
            .bind(update_is_public_bind(req.is_public))
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("update file failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("file not found"));
        }
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let file_id = parse_uuid("file_id", &req.file_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        // Soft-delete the metadata (keeps it auditable) and recover the object_key
        // so we can remove the bytes too.
        let row = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {deleted_at} = CURRENT_TIMESTAMP, \
               {status} = 'DELETED' \
             WHERE {file_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted_at} IS NULL \
             RETURNING {object_key}::TEXT AS object_key, COALESCE({project_id}::TEXT, '') AS project_id",
            deleted_at = m.q("deleted_at"),
            status = m.q("status"),
            file_id = m.q("file_id"),
            tenant_id = m.q("tenant_id"),
            object_key = m.q("object_key"),
            project_id = m.q("project_id"),
        ))
        .bind(file_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("delete file failed: {err}")))?;
        let Some(row) = row else {
            return Err(Status::not_found("file not found"));
        };
        let object_key: String = row.try_get("object_key").unwrap_or_default();
        let project_id: String = row.try_get("project_id").unwrap_or_default();
        // Remove the bytes (best-effort; metadata stays auditable on failure).
        self.delete_object_bytes(&project_id, &object_key).await;
        emit_payload_event(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_FILE_DELETED,
            &req.file_id,
            serde_json::json!({
                "file_id": req.file_id,
                "tenant_id": req.tenant_id,
                "project_id": project_id,
            }),
            Some(&self.metrics),
        )
        .await;
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
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let pool = self.require_pool()?;
        let m = file_model();
        let rel = m.relation.clone();
        let projection = file_select_projection(&m);
        let type_filter = file_type_to_db(&req.file_type, "")?;
        let page_size = if req.page_size > 0 { req.page_size } else { 50 }.min(500) as i64;
        let page = if req.page > 0 { req.page } else { 1 } as i64;
        let offset = (page - 1) * page_size;
        let where_clause = format!(
            "WHERE {deleted} IS NULL AND {tenant_id} = $1::UUID \
             AND ($2 = '' OR {file_type} = $2) \
             AND ($3 = '' OR {reference_id} = $3::UUID) \
             AND ($4 = '' OR {reference_type} = $4) \
             AND ($5 = '' OR {uploaded_by} = $5::UUID)",
            deleted = m.q("deleted_at"),
            tenant_id = m.q("tenant_id"),
            file_type = m.q("file_type"),
            reference_id = m.q("reference_id"),
            reference_type = m.q("reference_type"),
            uploaded_by = m.q("uploaded_by"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(tenant_id)
            .bind(&type_filter)
            .bind(&req.reference_id)
            .bind(&req.reference_type)
            .bind(&req.uploaded_by)
            .fetch_one(pool)
            .await
            .map_err(|err| Status::internal(format!("count files failed: {err}")))?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {filename} LIMIT $6 OFFSET $7",
            filename = m.q("filename"),
        ))
        .bind(tenant_id)
        .bind(&type_filter)
        .bind(&req.reference_id)
        .bind(&req.reference_type)
        .bind(&req.uploaded_by)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list files failed: {err}")))?;
        let mut files = Vec::with_capacity(rows.len());
        for row in &rows {
            files.push(file_from_row(row)?);
        }
        Ok(Response::new(storage_pb::ListFilesResponse {
            files,
            total_count: total as i32,
            error: None,
        }))
    }
}

impl DataBrokerService {
    /// Build the native `StorageService`, wired to the broker's Postgres pool
    /// and the transactional outbox, and spawn the periodic orphan reaper.
    pub(crate) fn build_storage_service(&self) -> StorageServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime.pg_pool().ok().cloned();
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
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                loop {
                    ticker.tick().await;
                    let reaper_once = reaper.clone();
                    match crate::runtime::singleton::run_while_leader(
                        &singleton_pool,
                        &singleton_relation,
                        crate::runtime::singleton::WORKER_STORAGE_ORPHAN_REAPER,
                        crate::runtime::singleton::WORKER_SINGLETON_LEASE_TTL,
                        || async move {
                            reaper_once
                                .reap_orphans(orphan_age_minutes, orphan_batch_size)
                                .await
                        },
                    )
                    .await
                    {
                        Ok(Some(Ok(n))) if n > 0 => {
                            tracing::info!(
                                reaped = n,
                                "storage orphan reaper deleted PENDING files"
                            )
                        }
                        Ok(Some(Ok(_))) => {}
                        Ok(Some(Err(err))) => {
                            tracing::warn!(error = %err, "storage orphan reaper failed")
                        }
                        Ok(None) => tracing::debug!(
                            "storage orphan reaper skipped: singleton lease held by peer"
                        ),
                        Err(err) => tracing::warn!(
                            error = %err,
                            "storage orphan reaper singleton lease failed"
                        ),
                    }
                }
            });
        }

        svc
    }
}

#[cfg(test)]
mod tenant_scope_tests {
    use super::*;
    use tonic::metadata::MetadataValue;

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
}

#[cfg(test)]
mod is_public_presence_tests {
    use super::*;

    /// The REAL UPDATE statements the handlers execute (`update_file_sql` /
    /// `finalize_upload_sql`) must presence-guard `is_public` with COALESCE so
    /// an absent proto3-optional field (bound as SQL NULL) leaves the stored
    /// visibility unchanged. Uses the embedded proto manifest — no DB needed.
    #[test]
    fn update_and_finalize_sql_presence_guard_is_public() {
        let m = file_model();
        let col = m.q("is_public");

        let update = update_file_sql(&m);
        assert!(
            update.contains(&format!("{col} = COALESCE($8, {col})")),
            "update_file SQL must keep stored is_public when $8 is NULL: {update}"
        );
        assert!(
            !update.contains(&format!("{col} = $8")),
            "update_file SQL must not bind is_public unconditionally: {update}"
        );

        let finalize = finalize_upload_sql(&m);
        assert!(
            finalize.contains(&format!("{col} = COALESCE($7, {col})")),
            "finalize_upload SQL must keep stored is_public when $7 is NULL: {finalize}"
        );
        assert!(
            !finalize.contains(&format!("{col} = $7 ")),
            "finalize_upload SQL must not bind is_public unconditionally: {finalize}"
        );
    }

    /// The bind helpers the handlers feed those statements: absent → NULL bind
    /// (COALESCE keeps the stored value), present → applied. The INSERT-side
    /// helper never produces NULL because the column is NOT NULL.
    #[test]
    fn is_public_binds_only_when_present() {
        // update_file / finalize_upload: absent field binds NULL → unchanged.
        assert_eq!(update_is_public_bind(None), None);
        assert_eq!(update_is_public_bind(Some(true)), Some(true));
        assert_eq!(update_is_public_bind(Some(false)), Some(false));
        // register_upload INSERT: absent field defaults to private, never NULL.
        assert!(!register_is_public_bind(None));
        assert!(register_is_public_bind(Some(true)));
        assert!(!register_is_public_bind(Some(false)));
    }
}
