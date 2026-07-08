//! Native `TenantService` — proto-driven Postgres CRUD over the UDB-owned
//! `udb_tenant.{tenants,tenant_configs}` tables.
//!
//! Mirrors `auth_service`: no in-memory store, no hand-mapped schema. Table and
//! column identifiers are resolved from the embedded proto manifest via
//! [`NativeModel`] (see `runtime::native_catalog`), so the SQL here follows the
//! same single-source-of-truth rule as the rest of the native services.

use std::sync::Arc;

use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::generation::CatalogManifest;
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::tenant::entity::v1 as tenant_entity_pb;
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

pub use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    MAX_LIST_ROWS, NativeEventContext, admit_on as native_admit_on,
    enqueue_outbox_event_with_context, native_next_page_token_for_total, native_offset_page_window,
    native_service_context, non_empty_json, parse_uuid, update_mask_allows, update_mask_path_set,
    validate_request_tenant,
};

const TENANT_MSG: &str = "udb.core.tenant.entity.v1.Tenant";
const TENANT_CONFIG_MSG: &str = "udb.core.tenant.entity.v1.TenantConfig";
const TOPIC_TENANT_PURGED: &str = "udb.tenant.purged.v1";

/// Postgres-backed `TenantService` handler.
pub struct TenantServiceImpl {
    pg_pool: Option<PgPool>,
    /// Runtime handle for P4 native-entity data-plane operations. Tenant config is
    /// stored as the real `TenantConfig` proto entity through the native catalog,
    /// neutral IR compiler, and selected backend executor/native driver.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Per-tenant fair-admission manager (the SAME one the data plane uses via
    /// `execute_with_channel_scoped`). Control-plane mutating/listing RPCs acquire
    /// a per-tenant budget through this so one tenant can't starve the shared
    /// control plane. `None` only in bare unit-test construction (no runtime
    /// wired) — `build_tenant_service` always wires it in production.
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
    /// Configured outbox relation; `None` disables best-effort purge audit emission.
    outbox_relation: Option<String>,
    /// Redis-backed cluster cutoff denylist for fast post-purge token rejection.
    #[cfg(feature = "redis")]
    jti_denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    /// Active catalog manifest — used by `PurgeTenant` to enumerate the tenant's
    /// owned tables for the ripple hard-delete. Set by `build_tenant_service`;
    /// `None` only in bare unit-test construction (PurgeTenant fails closed then).
    manifest: Option<CatalogManifest>,
}

fn tenant_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "tenant",
        operation,
        capability_required,
        message,
    )
}

fn tenant_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "tenant",
        operation,
        "tenant_not_found",
        "tenant not found",
    )
}

fn tenant_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("tenant", operation, message)
}

impl TenantServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
            outbox_relation: None,
            #[cfg(feature = "redis")]
            jti_denylist: None,
            manifest: None,
        }
    }

    /// Wire the active catalog manifest (the table set `PurgeTenant` ripples over).
    pub(crate) fn with_manifest(mut self, manifest: Option<CatalogManifest>) -> Self {
        self.manifest = manifest;
        self
    }

    /// PurgeTenant needs the manifest to enumerate tenant-owned tables; fail closed when absent.
    fn require_manifest(&self) -> Result<&CatalogManifest, Status> {
        self.manifest.as_ref().ok_or_else(|| {
            tenant_capability_status(
                "purge_tenant",
                "catalog_manifest",
                "tenant service requires the catalog manifest for purge",
            )
        })
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    /// Wire the runtime used for typed native-entity tenant config persistence.
    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    /// Typed tenant entities persist through native entity dispatch; fail closed when absent.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            tenant_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "tenant service requires runtime native entity dispatch",
            )
        })
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    #[cfg(feature = "redis")]
    pub(crate) fn with_jti_denylist(
        mut self,
        denylist: Option<crate::runtime::authn::revocation::JtiDenylist>,
    ) -> Self {
        self.jti_denylist = denylist;
        self
    }

    /// Wire the shared per-tenant fair-admission manager (same one the data plane
    /// uses) so control-plane RPCs are bounded per tenant. No-op (`None`) leaves
    /// admission disabled for bare unit-test construction.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Tenant CRUD is durable-only: fail closed when no Postgres pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            tenant_capability_status(
                "postgres_store",
                "postgres_store",
                "tenant service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }

    async fn emit_event(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
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
            "",
            payload,
            NativeEventContext {
                operation: "tenant.purge".to_string(),
                target_resource: tenant_id.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

impl Default for TenantServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

fn tenant_model() -> NativeModel {
    native_model(
        TENANT_MSG,
        &[
            "tenant_id",
            "code",
            "name",
            "type",
            "status",
            "parent_tenant_id",
            "config",
            "branding",
            "deleted_at",
            "deleted_by",
        ],
    )
}

// ── enum<->db (stored as VARCHAR via the proto_enum serializer) ───────────────

fn tenant_type_from_db(value: &str) -> i32 {
    use tenant_entity_pb::TenantType as T;
    match value {
        "PLATFORM" | "TENANT_TYPE_PLATFORM" => T::Platform as i32,
        "PARTNER" | "TENANT_TYPE_PARTNER" => T::Partner as i32,
        "ORGANIZATION" | "TENANT_TYPE_ORGANIZATION" => T::Organization as i32,
        "WORKSPACE" | "TENANT_TYPE_WORKSPACE" => T::Workspace as i32,
        "CUSTOMER_ACCOUNT" | "TENANT_TYPE_CUSTOMER_ACCOUNT" => T::CustomerAccount as i32,
        "DEPARTMENT" | "TENANT_TYPE_DEPARTMENT" => T::Department as i32,
        "SANDBOX" | "TENANT_TYPE_SANDBOX" => T::Sandbox as i32,
        _ => T::Unspecified as i32,
    }
}

fn tenant_status_from_db(value: &str) -> i32 {
    use tenant_entity_pb::TenantStatus as S;
    match value {
        "ACTIVE" | "TENANT_STATUS_ACTIVE" => S::Active as i32,
        "SUSPENDED" | "TENANT_STATUS_SUSPENDED" => S::Suspended as i32,
        "INACTIVE" | "TENANT_STATUS_INACTIVE" => S::Inactive as i32,
        _ => S::Unspecified as i32,
    }
}

fn config_type_from_db(value: &str) -> i32 {
    use tenant_entity_pb::ConfigType as C;
    match value {
        "STRING" | "CONFIG_TYPE_STRING" => C::String as i32,
        "NUMBER" | "CONFIG_TYPE_NUMBER" => C::Number as i32,
        "BOOLEAN" | "CONFIG_TYPE_BOOLEAN" => C::Boolean as i32,
        "JSON" | "CONFIG_TYPE_JSON" => C::Json as i32,
        _ => C::Unspecified as i32,
    }
}

/// Normalize a tenant-type string to the canonical SHORT stored token (e.g.
/// "ORGANIZATION"), accepting either the short or the proto-prefixed
/// ("TENANT_TYPE_ORGANIZATION") form. Empty → `default`. Unknown non-empty input
/// is rejected so it never silently overflows VARCHAR(20) or reads back as
/// Unspecified. Storing the short form keeps every value within VARCHAR(20) (the
/// prefixed forms are too long) and makes write/read/filter round-trip.
fn tenant_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PLATFORM" | "TENANT_TYPE_PLATFORM" => "PLATFORM",
        "PARTNER" | "TENANT_TYPE_PARTNER" => "PARTNER",
        "ORGANIZATION" | "TENANT_TYPE_ORGANIZATION" => "ORGANIZATION",
        "WORKSPACE" | "TENANT_TYPE_WORKSPACE" => "WORKSPACE",
        "CUSTOMER_ACCOUNT" | "TENANT_TYPE_CUSTOMER_ACCOUNT" => "CUSTOMER_ACCOUNT",
        "DEPARTMENT" | "TENANT_TYPE_DEPARTMENT" => "DEPARTMENT",
        "SANDBOX" | "TENANT_TYPE_SANDBOX" => "SANDBOX",
        other => {
            return Err(tenant_field_violation(
                "type",
                format!("unsupported tenant type {other}"),
                format!("unknown tenant type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

/// Normalize a tenant-status string to the canonical SHORT stored token. Same
/// accept-both-forms / reject-unknown / empty→default contract as
/// [`tenant_type_to_db`].
fn tenant_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "ACTIVE" | "TENANT_STATUS_ACTIVE" => "ACTIVE",
        "SUSPENDED" | "TENANT_STATUS_SUSPENDED" => "SUSPENDED",
        "INACTIVE" | "TENANT_STATUS_INACTIVE" => "INACTIVE",
        other => {
            return Err(tenant_field_violation(
                "status",
                format!("unsupported tenant status {other}"),
                format!("unknown tenant status: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

/// Normalize a config-type string to the canonical SHORT stored token.
fn config_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "STRING" | "CONFIG_TYPE_STRING" => "STRING",
        "NUMBER" | "CONFIG_TYPE_NUMBER" => "NUMBER",
        "BOOLEAN" | "CONFIG_TYPE_BOOLEAN" => "BOOLEAN",
        "JSON" | "CONFIG_TYPE_JSON" => "JSON",
        other => {
            return Err(tenant_field_violation(
                "type",
                format!("unsupported config type {other}"),
                format!("unknown config type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn tenant_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn tenant_field_violation(
    field: &'static str,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.to_string(), description.into())],
    )
}

fn validate_create_tenant_required_fields(code: &str, name: &str) -> Result<(), Status> {
    let mut fields = Vec::new();
    if code.trim().is_empty() {
        fields.push(("code", "must be a non-empty tenant code"));
    }
    if name.trim().is_empty() {
        fields.push(("name", "must be a non-empty tenant name"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "code and name are required",
            fields,
        ));
    }
    Ok(())
}

fn active_tenant_filter(tenant_id: &str) -> LogicalFilter {
    LogicalFilter::And(vec![
        LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        },
        LogicalFilter::IsNull("deleted_at".to_string()),
    ])
}

fn tenant_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "tenant_id".to_string(),
        "code".to_string(),
        "name".to_string(),
        "type".to_string(),
        "status".to_string(),
        "parent_tenant_id".to_string(),
        "config".to_string(),
        "branding".to_string(),
        "deleted_by".to_string(),
    ])
}

fn tenant_read_by_id(tenant_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: TENANT_MSG.to_string(),
        filter: Some(active_tenant_filter(tenant_id)),
        projection: Some(tenant_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

fn tenant_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_string_field(
    row: &serde_json::Map<String, serde_json::Value>,
    logical: &str,
    column: &str,
) -> String {
    row.get(logical)
        .or_else(|| row.get(column))
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Some(value.to_string()),
            serde_json::Value::Null => None,
        })
        .unwrap_or_default()
}

fn tenant_from_json(row: &serde_json::Value) -> tenant_entity_pb::Tenant {
    let row = tenant_json_object(row);
    tenant_entity_pb::Tenant {
        tenant_id: json_string_field(row, "tenant_id", "tenant_id"),
        code: json_string_field(row, "code", "code"),
        name: json_string_field(row, "name", "name"),
        r#type: tenant_type_from_db(&json_string_field(row, "type", "type")),
        status: tenant_status_from_db(&json_string_field(row, "status", "status")),
        parent_tenant_id: json_string_field(row, "parent_tenant_id", "parent_tenant_id"),
        config: json_string_field(row, "config", "config"),
        branding: json_string_field(row, "branding", "branding"),
        deleted_by: json_string_field(row, "deleted_by", "deleted_by"),
        ..Default::default()
    }
}

fn tenant_config_filter(tenant_id: &str, config_key: Option<&str>) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(config_key) = config_key.filter(|value| !value.trim().is_empty()) {
        filters.push(LogicalFilter::Comparison {
            field: "config_key".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(config_key.to_string()),
        });
    }
    LogicalFilter::And(filters)
}

fn tenant_config_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "id".to_string(),
        "tenant_id".to_string(),
        "config_key".to_string(),
        "config_value".to_string(),
        "type".to_string(),
        "description".to_string(),
    ])
}

fn tenant_config_read(tenant_id: &str, config_key: Option<&str>, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: TENANT_CONFIG_MSG.to_string(),
        filter: Some(tenant_config_filter(tenant_id, config_key)),
        projection: Some(tenant_config_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(limit)),
    }
}

fn tenant_config_from_json(
    row: &serde_json::Value,
    fallback_tenant_id: &str,
) -> tenant_entity_pb::TenantConfig {
    let row = tenant_json_object(row);
    let tenant_id = json_string_field(row, "tenant_id", "tenant_id");
    tenant_entity_pb::TenantConfig {
        id: json_string_field(row, "id", "config_id"),
        tenant_id: if tenant_id.is_empty() {
            fallback_tenant_id.to_string()
        } else {
            tenant_id
        },
        config_key: json_string_field(row, "config_key", "config_key"),
        config_value: json_string_field(row, "config_value", "config_value"),
        r#type: config_type_from_db(&json_string_field(row, "type", "type")),
        description: json_string_field(row, "description", "description"),
        ..Default::default()
    }
}

fn tenant_config_record(
    id: String,
    tenant_id: &str,
    req: &tenant_pb::UpdateTenantConfigRequest,
    kind: String,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("id".to_string(), logical_string(id));
    record.insert(
        "tenant_id".to_string(),
        logical_string(tenant_id.to_string()),
    );
    record.insert(
        "config_key".to_string(),
        logical_string(req.config_key.trim().to_string()),
    );
    record.insert(
        "config_value".to_string(),
        logical_string(req.config_value.clone()),
    );
    record.insert("type".to_string(), logical_string(kind));
    record.insert("description".to_string(), logical_string(String::new()));
    record
}

// ── projections + row mappers ─────────────────────────────────────────────────

fn tenant_select_projection(m: &NativeModel) -> String {
    [
        m.text("tenant_id"),
        m.select("code"),
        m.select("name"),
        m.text_or_empty("type"),
        m.text_or_empty("status"),
        m.text_or_empty("parent_tenant_id"),
        m.text_or_empty("config"),
        m.text_or_empty("branding"),
        m.text_or_empty("deleted_by"),
    ]
    .join(", ")
}

fn tenant_from_row(row: &sqlx::postgres::PgRow) -> Result<tenant_entity_pb::Tenant, Status> {
    let map = |e: sqlx::Error| {
        tenant_internal_status("decode_tenant", format!("decode tenant failed: {e}"))
    };
    Ok(tenant_entity_pb::Tenant {
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        code: row.try_get("code").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        r#type: tenant_type_from_db(&row.try_get::<String, _>("type").map_err(map)?),
        status: tenant_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        parent_tenant_id: row.try_get("parent_tenant_id").map_err(map)?,
        config: row.try_get("config").map_err(map)?,
        branding: row.try_get("branding").map_err(map)?,
        deleted_by: row.try_get("deleted_by").map_err(map)?,
        ..Default::default()
    })
}

#[tonic::async_trait]
impl TenantService for TenantServiceImpl {
    async fn create_tenant(
        &self,
        request: Request<tenant_pb::CreateTenantRequest>,
    ) -> Result<Response<tenant_pb::CreateTenantResponse>, Status> {
        let req = request.into_inner();
        validate_create_tenant_required_fields(&req.code, &req.name)?;
        // Per-tenant fair admission. CreateTenant has no body tenant_id yet, so it
        // scopes to the parent tenant when supplied (else the shared base budget).
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Admin,
            &req.parent_tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = tenant_model();
        let rel = m.relation.clone();
        let tenant_id = Uuid::new_v4().to_string();
        let kind = tenant_type_to_db(&req.r#type, "ORGANIZATION")?;
        let config = non_empty_json(&req.config);
        let branding = non_empty_json(&req.branding);
        // Idempotent on the unique `code`: a repeated CreateTenant with the same
        // code is a no-op insert (ON CONFLICT DO NOTHING) and returns the EXISTING
        // canonical id rather than erroring on the unique index. This keeps tenant
        // provisioning safe to re-run (matching the offline `ensure_tenant` path).
        //
        // P4 transitional path: the current native LogicalWrite conflict target is
        // the message primary key (`tenant_id`), not the alternate unique `code`.
        // Keep this bespoke insert until alternate-conflict/upsert-by-code is
        // expressible in the IR; falling back to primary-key conflict would break
        // CreateTenant idempotency.
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({tenant_id}, {code}, {name}, {type_col}, {status}, {parent}, {config}, {branding}) \
             VALUES ($1::UUID, $2, $3, $4, 'ACTIVE', NULLIF($5, '')::UUID, $6::JSONB, $7::JSONB) \
             ON CONFLICT ({code}) DO NOTHING",
            tenant_id = m.q("tenant_id"),
            code = m.q("code"),
            name = m.q("name"),
            type_col = m.q("type"),
            status = m.q("status"),
            parent = m.q("parent_tenant_id"),
            config = m.q("config"),
            branding = m.q("branding"),
        ))
        .bind(&tenant_id)
        .bind(&req.code)
        .bind(&req.name)
        .bind(&kind)
        .bind(&req.parent_tenant_id)
        .bind(&config)
        .bind(&branding)
        .execute(pool)
        .await
        .map_err(|err| {
            tenant_internal_status("create_tenant", format!("create tenant failed: {err}"))
        })?;
        // Re-resolve by code so a conflict returns the surviving row's canonical id.
        let canonical_id: String = sqlx::query_scalar(&format!(
            "SELECT {tenant_id}::text FROM {rel} WHERE {code} = $1 AND {deleted_at} IS NULL",
            tenant_id = m.q("tenant_id"),
            code = m.q("code"),
            deleted_at = m.q("deleted_at"),
        ))
        .bind(&req.code)
        .fetch_one(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "resolve_tenant_after_create",
                format!("resolve tenant after create failed: {err}"),
            )
        })?;
        Ok(Response::new(tenant_pb::CreateTenantResponse {
            tenant_id: canonical_id,
            message: "tenant created".to_string(),
            error: None,
        }))
    }

    /// Account/tenant HARD-DELETE with ripple (GDPR right-to-be-forgotten). Hard-
    /// deletes every row the tenant owns across all manifest entity tables in one
    /// transaction (children->parents), reports tenant-less tables as excluded,
    /// then records the tenant-level revocation cutoff so pre-delete tokens are
    /// rejected. DESTRUCTIVE + irreversible: requires an explicit confirmation
    /// token and a body tenant_id matching the verified claim.
    async fn purge_tenant(
        &self,
        request: Request<tenant_pb::PurgeTenantRequest>,
    ) -> Result<Response<tenant_pb::PurgeTenantResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        let tenant_id = req.tenant_id.trim().to_string();
        if tenant_id.is_empty() {
            return Err(tenant_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        // DESTRUCTIVE: an empty/missing confirmation token fails CLOSED — the purge
        // is irreversible, so it never runs without explicit confirmation.
        if req.confirmation_token.trim().is_empty() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "PurgeTenant is an irreversible hard delete; confirmation_token is required",
                [("confirmation_token", "must be present to purge tenant data")],
            ));
        }
        // No cross-tenant purge: the body tenant_id must match the verified claim.
        validate_request_tenant(&metadata, &tenant_id)?;
        let movement = TenantMovementRequest {
            operation: TenantMovementOperation::TenantPurge,
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
            "tenant",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let manifest = self.require_manifest()?;
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // The hard delete of the tenant's principal/session/token-family rows
        // makes validation's persisted-state checks reject their tokens; when
        // Redis is wired, the tenant cutoff denylist accelerates that cluster-wide
        // before each node reaches the durable read.
        let report = {
            #[cfg(feature = "redis")]
            {
                crate::runtime::core::purge_tenant(
                    pool,
                    manifest,
                    &tenant_id,
                    &[],
                    self.jti_denylist.as_ref(),
                    now_unix,
                )
                .await?
            }
            #[cfg(not(feature = "redis"))]
            {
                crate::runtime::core::purge_tenant(pool, manifest, &tenant_id, &[], now_unix)
                    .await?
            }
        };
        let purged_payload = report
            .purged
            .iter()
            .map(|p| {
                serde_json::json!({
                    "schema": &p.schema,
                    "table": &p.table,
                    "tenant_column": &p.tenant_column,
                    "deleted": p.deleted,
                })
            })
            .collect::<Vec<_>>();
        let excluded_payload = report
            .excluded
            .iter()
            .map(|e| {
                serde_json::json!({
                    "schema": &e.schema,
                    "table": &e.table,
                    "reason": &e.reason,
                })
            })
            .collect::<Vec<_>>();
        self.emit_event(
            TOPIC_TENANT_PURGED,
            &tenant_id,
            &tenant_id,
            serde_json::json!({
                "tenant_id": &tenant_id,
                "purged": purged_payload,
                "excluded": excluded_payload,
                "total_deleted": report.total_deleted,
                "tenant_denylisted": report.tenant_denylisted,
                "principals_denylisted": report.principals_denylisted,
            }),
        )
        .await;
        Ok(Response::new(tenant_pb::PurgeTenantResponse {
            tenant_id: report.tenant_id,
            purged: report
                .purged
                .into_iter()
                .map(|p| tenant_pb::PurgedTableCount {
                    schema: p.schema,
                    table: p.table,
                    tenant_column: p.tenant_column,
                    deleted: p.deleted,
                })
                .collect(),
            excluded: report
                .excluded
                .into_iter()
                .map(|e| tenant_pb::PurgeExcludedTable {
                    schema: e.schema,
                    table: e.table,
                    reason: e.reason,
                })
                .collect(),
            total_deleted: report.total_deleted,
            tenant_denylisted: report.tenant_denylisted,
            principals_denylisted: report.principals_denylisted as u32,
            message: "tenant purged".to_string(),
            error: None,
        }))
    }

    async fn get_tenant(
        &self,
        request: Request<tenant_pb::GetTenantRequest>,
    ) -> Result<Response<tenant_pb::GetTenantResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        let runtime = self.require_runtime()?;
        let mut rows = runtime
            .native_entity_read_for_service("tenant", &context, tenant_read_by_id(&tenant_id))
            .await?;
        let tenant = rows
            .pop()
            .map(|row| tenant_from_json(&row))
            .ok_or_else(|| tenant_not_found_status("get_tenant"))?;
        Ok(Response::new(tenant_pb::GetTenantResponse {
            tenant: Some(tenant),
            error: None,
        }))
    }

    async fn list_tenants(
        &self,
        request: Request<tenant_pb::ListTenantsRequest>,
    ) -> Result<Response<tenant_pb::ListTenantsResponse>, Status> {
        let req = request.into_inner();
        // Platform-scope listing (no body tenant to spoof); bound it on the shared
        // base Read budget so a list flood can't starve the control plane.
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Read,
            "",
            None,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = tenant_model();
        let rel = m.relation.clone();
        let projection = tenant_select_projection(&m);
        let type_filter = tenant_type_to_db(&req.r#type, "")?;
        let status_filter = tenant_status_to_db(&req.status, "")?;
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        // P4 transitional path: `ListTenants` returns an exact `total_count`.
        // The service helper currently exposes typed `LogicalRead`, not aggregate
        // count, so keep the existing SQL list/count path rather than deriving an
        // approximate count from the current page.
        let where_clause = format!(
            "WHERE {deleted} IS NULL AND ($1 = '' OR {type_col} = $1) AND ($2 = '' OR {status} = $2)",
            deleted = m.q("deleted_at"),
            type_col = m.q("type"),
            status = m.q("status"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(&type_filter)
            .bind(&status_filter)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                tenant_internal_status("list_tenants_count", format!("count tenants failed: {err}"))
            })?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {code} LIMIT $3 OFFSET $4",
            code = m.q("code"),
        ))
        .bind(&type_filter)
        .bind(&status_filter)
        .bind(page_window.limit_i64())
        .bind(page_window.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| {
            tenant_internal_status("list_tenants", format!("list tenants failed: {err}"))
        })?;
        let mut tenants = Vec::with_capacity(rows.len());
        for row in &rows {
            tenants.push(tenant_from_row(row)?);
        }
        Ok(Response::new(tenant_pb::ListTenantsResponse {
            tenants,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }

    async fn update_tenant(
        &self,
        request: Request<tenant_pb::UpdateTenantRequest>,
    ) -> Result<Response<tenant_pb::UpdateTenantResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?;
        let update_mask = update_mask_path_set(
            req.update_mask.as_ref(),
            &["name", "status", "config", "branding"],
        )?;
        let update_name = update_mask_allows(&update_mask, "name", !req.name.trim().is_empty());
        let update_status =
            update_mask_allows(&update_mask, "status", !req.status.trim().is_empty());
        let update_config =
            update_mask_allows(&update_mask, "config", !req.config.trim().is_empty());
        let update_branding =
            update_mask_allows(&update_mask, "branding", !req.branding.trim().is_empty());
        let status = tenant_status_to_db(&req.status, "")?;
        let pool = self.require_pool()?;
        let m = tenant_model();
        let rel = m.relation.clone();
        // P4 transitional path: native LogicalWrite is currently upsert-by-primary-key,
        // while this RPC is update-only and must not create or revive a deleted row.
        // Keep the predicate-bearing SQL until the IR/service helper can express an
        // update with `WHERE tenant_id = ? AND deleted_at IS NULL`.
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {name} = CASE WHEN $2 THEN $3 ELSE {name} END, \
               {status} = CASE WHEN $4 THEN $5 ELSE {status} END, \
               {config} = CASE WHEN $6 THEN $7::JSONB ELSE {config} END, \
               {branding} = CASE WHEN $8 THEN $9::JSONB ELSE {branding} END \
             WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL",
            name = m.q("name"),
            status = m.q("status"),
            config = m.q("config"),
            branding = m.q("branding"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(tenant_id)
        .bind(update_name)
        .bind(&req.name)
        .bind(update_status)
        .bind(&status)
        .bind(update_config)
        .bind(req.config.trim())
        .bind(update_branding)
        .bind(req.branding.trim())
        .execute(pool)
        .await
        .map_err(|err| {
            tenant_internal_status("update_tenant", format!("update tenant failed: {err}"))
        })?;
        if result.rows_affected() == 0 {
            return Err(tenant_not_found_status("update_tenant"));
        }
        Ok(Response::new(tenant_pb::UpdateTenantResponse {
            message: "tenant updated".to_string(),
            error: None,
        }))
    }

    async fn get_tenant_config(
        &self,
        request: Request<tenant_pb::GetTenantConfigRequest>,
    ) -> Result<Response<tenant_pb::GetTenantConfigResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let context = native_service_context(&metadata, &tenant_id, "");
        let runtime = self.require_runtime()?;
        let rows = runtime
            .native_entity_read_for_service(
                "tenant",
                &context,
                tenant_config_read(&tenant_id, None, MAX_LIST_ROWS as u32),
            )
            .await?;
        let mut configs = rows
            .iter()
            .map(|row| tenant_config_from_json(row, &tenant_id))
            .collect::<Vec<_>>();
        configs.sort_by(|a, b| a.config_key.cmp(&b.config_key));
        Ok(Response::new(tenant_pb::GetTenantConfigResponse {
            configs,
            error: None,
        }))
    }

    async fn update_tenant_config(
        &self,
        request: Request<tenant_pb::UpdateTenantConfigRequest>,
    ) -> Result<Response<tenant_pb::UpdateTenantConfigResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "tenant",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        if req.config_key.trim().is_empty() {
            return Err(tenant_required_field(
                "config_key",
                "must be a non-empty config key",
                "config_key is required",
            ));
        }
        let kind = config_type_to_db(&req.r#type, "STRING")?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let runtime = self.require_runtime()?;
        let existing = runtime
            .native_entity_read_for_service(
                "tenant",
                &context,
                tenant_config_read(&tenant_id, Some(req.config_key.trim()), 1),
            )
            .await?;
        let id = existing
            .first()
            .map(|row| tenant_config_from_json(row, &tenant_id).id)
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        runtime
            .native_entity_write_for_service(
                "tenant",
                &context,
                TENANT_CONFIG_MSG,
                tenant_config_record(id, &tenant_id, &req, kind),
                ConflictStrategy::update(vec![
                    "tenant_id".to_string(),
                    "config_key".to_string(),
                    "config_value".to_string(),
                    "type".to_string(),
                    "description".to_string(),
                ]),
            )
            .await?;
        Ok(Response::new(tenant_pb::UpdateTenantConfigResponse {
            message: "tenant config updated".to_string(),
            error: None,
        }))
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
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_schema_not_found_detail(status: &Status, operation: &str) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "tenant not found");
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "tenant");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "tenant_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "tenant");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not read another tenant by putting a
    /// foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any pool/DB access (no Postgres needed).
    #[tokio::test]
    async fn get_tenant_rejects_cross_tenant_body() {
        let svc = TenantServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(tenant_pb::GetTenantRequest {
            tenant_id: "tenant-b".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_tenant(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_tenant_missing_code_and_name_carries_field_violations() {
        let svc = TenantServiceImpl::new(); // no pool, no channels (admit no-op)
        let request = Request::new(tenant_pb::CreateTenantRequest {
            code: "  ".to_string(),
            name: String::new(),
            ..Default::default()
        });
        let err = svc
            .create_tenant(request)
            .await
            .expect_err("missing create fields must be rejected before pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "code and name are required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "code");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty tenant code"
        );
        assert_eq!(detail.field_violations[1].field, "name");
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty tenant name"
        );
    }

    #[tokio::test]
    async fn purge_tenant_missing_tenant_id_carries_field_violation() {
        let svc = TenantServiceImpl::new(); // no pool/manifest; validation must fire first
        let request = Request::new(tenant_pb::PurgeTenantRequest {
            tenant_id: "  ".to_string(),
            confirmation_token: "confirm".to_string(),
            ..Default::default()
        });
        let err = svc
            .purge_tenant(request)
            .await
            .expect_err("missing tenant_id must be rejected before manifest/pool access");
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
    async fn purge_tenant_missing_confirmation_token_carries_field_violation() {
        let svc = TenantServiceImpl::new(); // no pool/manifest; validation must fire first
        let tenant_id = "11111111-1111-1111-1111-111111111111";
        let mut request = Request::new(tenant_pb::PurgeTenantRequest {
            tenant_id: tenant_id.to_string(),
            confirmation_token: " ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static(tenant_id));
        let err = svc
            .purge_tenant(request)
            .await
            .expect_err("missing confirmation_token must be rejected before manifest/pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            err.message(),
            "PurgeTenant is an irreversible hard delete; confirmation_token is required"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "confirmation_token");
        assert_eq!(
            detail.field_violations[0].description,
            "must be present to purge tenant data"
        );
    }

    #[tokio::test]
    async fn update_tenant_config_missing_key_carries_field_violation() {
        let svc = TenantServiceImpl::new(); // no runtime, no channels (admit no-op)
        let tenant_id = "11111111-1111-1111-1111-111111111111";
        let mut request = Request::new(tenant_pb::UpdateTenantConfigRequest {
            tenant_id: tenant_id.to_string(),
            config_key: "  ".to_string(),
            config_value: "on".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static(tenant_id));
        let err = svc
            .update_tenant_config(request)
            .await
            .expect_err("missing config_key must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "config_key is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "config_key");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty config key"
        );
    }

    #[test]
    fn tenant_enum_normalizers_carry_field_violations() {
        let tenant_type = tenant_type_to_db("enterprise", "ORGANIZATION")
            .expect_err("unknown tenant type must fail");
        assert_eq!(tenant_type.message(), "unknown tenant type: ENTERPRISE");
        assert_single_field_violation(&tenant_type, "type", "unsupported tenant type ENTERPRISE");

        let tenant_status =
            tenant_status_to_db("paused", "ACTIVE").expect_err("unknown tenant status must fail");
        assert_eq!(tenant_status.message(), "unknown tenant status: PAUSED");
        assert_single_field_violation(&tenant_status, "status", "unsupported tenant status PAUSED");

        let config_type =
            config_type_to_db("object", "STRING").expect_err("unknown config type must fail");
        assert_eq!(config_type.message(), "unknown config type: OBJECT");
        assert_single_field_violation(&config_type, "type", "unsupported config type OBJECT");
    }

    #[test]
    fn tenant_missing_setup_capabilities_carry_typed_detail() {
        for (operation, capability, message) in [
            (
                "purge_tenant",
                "catalog_manifest",
                "tenant service requires the catalog manifest for purge",
            ),
            (
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "tenant service requires runtime native entity dispatch",
            ),
            (
                "postgres_store",
                "postgres_store",
                "tenant service requires a Postgres-backed store (no PG pool configured)",
            ),
        ] {
            let err = tenant_capability_status(operation, capability, message);
            assert_eq!(err.code(), tonic::Code::FailedPrecondition);
            assert_eq!(err.message(), message);
            let detail = decode_detail(&err);
            assert_eq!(detail.kind, ErrorKind::Capability as i32);
            assert_eq!(detail.backend, "tenant");
            assert_eq!(detail.operation, operation);
            assert_eq!(detail.capability_required, capability);
            assert!(!detail.retryable);
        }
    }

    #[test]
    fn tenant_not_found_statuses_carry_schema_detail() {
        for operation in ["get_tenant", "update_tenant"] {
            assert_schema_not_found_detail(&tenant_not_found_status(operation), operation);
        }
    }

    #[test]
    fn tenant_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &tenant_internal_status(
                "resolve_tenant_after_create",
                "resolve tenant after create failed: database is unavailable",
            ),
            "resolve_tenant_after_create",
            "resolve tenant after create failed: database is unavailable",
        );
    }
}

impl DataBrokerService {
    /// Build the native `TenantService`, wired to the broker's Postgres pool.
    pub(crate) fn build_tenant_service(&self) -> TenantServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam (extend_udb.md):
        // the backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("tenant", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        #[cfg(feature = "redis")]
        let jti_denylist = runtime.redis_clone().map(|redis| {
            crate::runtime::authn::revocation::JtiDenylist::new(
                redis,
                crate::runtime::security::SecurityConfig::current().jwt_access_ttl_secs,
            )
        });
        let service = TenantServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
            .with_outbox(Some(outbox))
            .with_manifest(Some(self.manifest.clone()));
        #[cfg(feature = "redis")]
        let service = service.with_jti_denylist(jti_denylist);
        service
    }
}
