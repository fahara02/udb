//! Manifest model, enum<->db token converters, and the proto row / JSON decoders
//! for the native `TenantService`. Extracted verbatim from the former god file —
//! the SQL identifiers still resolve from the embedded proto manifest via
//! [`NativeModel`], and every stored-token normalization is byte-for-byte stable.

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::tenant::entity::v1 as tenant_entity_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::TENANT_MSG;
use super::errors::{tenant_field_violation, tenant_internal_status};

pub(crate) fn tenant_model() -> NativeModel {
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

pub(crate) fn tenant_type_from_db(value: &str) -> i32 {
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

pub(crate) fn tenant_status_from_db(value: &str) -> i32 {
    use tenant_entity_pb::TenantStatus as S;
    match value {
        "ACTIVE" | "TENANT_STATUS_ACTIVE" => S::Active as i32,
        "SUSPENDED" | "TENANT_STATUS_SUSPENDED" => S::Suspended as i32,
        "INACTIVE" | "TENANT_STATUS_INACTIVE" => S::Inactive as i32,
        _ => S::Unspecified as i32,
    }
}

pub(crate) fn config_type_from_db(value: &str) -> i32 {
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
pub(crate) fn tenant_type_to_db(value: &str, default: &str) -> Result<String, Status> {
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
pub(crate) fn tenant_status_to_db(value: &str, default: &str) -> Result<String, Status> {
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
pub(crate) fn config_type_to_db(value: &str, default: &str) -> Result<String, Status> {
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

pub(crate) fn tenant_json_object(
    row: &serde_json::Value,
) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

pub(crate) fn json_string_field(
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

pub(crate) fn tenant_from_json(row: &serde_json::Value) -> tenant_entity_pb::Tenant {
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

pub(crate) fn tenant_config_from_json(
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

// ── projections + row mappers ─────────────────────────────────────────────────

pub(crate) fn tenant_select_projection(m: &NativeModel) -> String {
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

pub(crate) fn tenant_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<tenant_entity_pb::Tenant, Status> {
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
