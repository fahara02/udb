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

pub use crate::proto::udb::core::tenant::services::v1::tenant_service_server::TenantServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    MAX_LIST_ROWS, admit_on as native_admit_on, native_service_context, non_empty_json, parse_uuid,
    validate_request_tenant,
};

const TENANT_MSG: &str = "udb.core.tenant.entity.v1.Tenant";
const TENANT_CONFIG_MSG: &str = "udb.core.tenant.entity.v1.TenantConfig";

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
}

impl TenantServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
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
            Status::failed_precondition("tenant service requires runtime native entity dispatch")
        })
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
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
            Status::failed_precondition(
                "tenant service requires a Postgres-backed store (no PG pool configured)",
            )
        })
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
            return Err(Status::invalid_argument(format!(
                "unknown tenant type: {other}"
            )));
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
            return Err(Status::invalid_argument(format!(
                "unknown tenant status: {other}"
            )));
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
            return Err(Status::invalid_argument(format!(
                "unknown config type: {other}"
            )));
        }
    };
    Ok(short.to_string())
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
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
    let map = |e: sqlx::Error| Status::internal(format!("decode tenant failed: {e}"));
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
        if req.code.trim().is_empty() || req.name.trim().is_empty() {
            return Err(Status::invalid_argument("code and name are required"));
        }
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
        .map_err(|err| Status::internal(format!("create tenant failed: {err}")))?;
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
        .map_err(|err| Status::internal(format!("resolve tenant after create failed: {err}")))?;
        Ok(Response::new(tenant_pb::CreateTenantResponse {
            tenant_id: canonical_id,
            message: "tenant created".to_string(),
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
            .ok_or_else(|| Status::not_found("tenant not found"))?;
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
        let page_size =
            if req.page_size > 0 { req.page_size } else { 50 }.min(MAX_LIST_ROWS as i32) as i64;
        let page = if req.page > 0 { req.page } else { 1 } as i64;
        let offset = (page - 1) * page_size;
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
            .map_err(|err| Status::internal(format!("count tenants failed: {err}")))?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {code} LIMIT $3 OFFSET $4",
            code = m.q("code"),
        ))
        .bind(&type_filter)
        .bind(&status_filter)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list tenants failed: {err}")))?;
        let mut tenants = Vec::with_capacity(rows.len());
        for row in &rows {
            tenants.push(tenant_from_row(row)?);
        }
        Ok(Response::new(tenant_pb::ListTenantsResponse {
            tenants,
            total_count: total as i32,
            error: None,
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
        let pool = self.require_pool()?;
        let m = tenant_model();
        let rel = m.relation.clone();
        let status = tenant_status_to_db(&req.status, "")?;
        // P4 transitional path: native LogicalWrite is currently upsert-by-primary-key,
        // while this RPC is update-only and must not create or revive a deleted row.
        // Keep the predicate-bearing SQL until the IR/service helper can express an
        // update with `WHERE tenant_id = ? AND deleted_at IS NULL`.
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {name} = COALESCE(NULLIF($2, ''), {name}), \
               {status} = COALESCE(NULLIF($3, ''), {status}), \
               {config} = CASE WHEN $4 = '' THEN {config} ELSE $4::JSONB END, \
               {branding} = CASE WHEN $5 = '' THEN {branding} ELSE $5::JSONB END \
             WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL",
            name = m.q("name"),
            status = m.q("status"),
            config = m.q("config"),
            branding = m.q("branding"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(tenant_id)
        .bind(&req.name)
        .bind(&status)
        .bind(req.config.trim())
        .bind(req.branding.trim())
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("update tenant failed: {err}")))?;
        if result.rows_affected() == 0 {
            return Err(Status::not_found("tenant not found"));
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
            return Err(Status::invalid_argument("config_key is required"));
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
    use tonic::metadata::MetadataValue;

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
        let channels = Some(runtime.channels().clone());
        TenantServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
