//! Model layer for the native `AssetService`: the proto-manifest-driven
//! [`NativeModel`] table/column resolvers, the enum<->db SHORT-token converters,
//! the native-JSON row decoders (`asset_from_json` / `pipeline_definition_from_json`),
//! and the PgRow decoders + select projections for pipeline instances/steps.
//! Extracted verbatim from the former god file.

use sqlx::Row;
use tonic::Status;

use crate::proto::udb::core::asset::entity::v1 as asset_entity_pb;
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{ASSET_MSG, PIPELINE_DEFINITION_MSG, PIPELINE_INSTANCE_MSG, PIPELINE_STEP_MSG};
use super::errors::{asset_internal_status, asset_invalid_field};

// ── native models (table + column resolution from the embedded proto manifest) ─

pub(crate) fn asset_model() -> NativeModel {
    native_model(
        ASSET_MSG,
        &[
            "asset_id",
            "tenant_id",
            "project_id",
            "file_id",
            "name",
            "media_type",
            "status",
            "metadata",
        ],
    )
}

pub(crate) fn pipeline_definition_model() -> NativeModel {
    native_model(
        PIPELINE_DEFINITION_MSG,
        &[
            "definition_id",
            "tenant_id",
            "name",
            "description",
            "media_type",
            "steps",
            "version",
            "status",
            "trigger_topic",
        ],
    )
}

pub(crate) fn pipeline_instance_model() -> NativeModel {
    native_model(
        PIPELINE_INSTANCE_MSG,
        &[
            "instance_id",
            "definition_id",
            "asset_id",
            "tenant_id",
            "status",
            "current_step",
            "context",
            "correlation_id",
            "started_at",
            "completed_at",
        ],
    )
}

pub(crate) fn pipeline_step_model() -> NativeModel {
    native_model(
        PIPELINE_STEP_MSG,
        &[
            "step_id",
            "instance_id",
            "tenant_id",
            "step_name",
            "step_type",
            "status",
            "result",
            "error",
            "params",
            "retry_count",
            "started_at",
            "completed_at",
        ],
    )
}

pub(crate) fn native_json_object(
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
) -> String {
    row.get(logical)
        .and_then(|value| match value {
            serde_json::Value::String(value) => Some(value.clone()),
            serde_json::Value::Number(value) => Some(value.to_string()),
            serde_json::Value::Bool(value) => Some(value.to_string()),
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => Some(value.to_string()),
            serde_json::Value::Null => None,
        })
        .unwrap_or_default()
}

pub(crate) fn json_i32_field(
    row: &serde_json::Map<String, serde_json::Value>,
    logical: &str,
) -> i32 {
    row.get(logical)
        .and_then(|value| value.as_i64())
        .unwrap_or_default() as i32
}

pub(crate) fn asset_from_json(row: &serde_json::Value) -> asset_entity_pb::Asset {
    let row = native_json_object(row);
    asset_entity_pb::Asset {
        asset_id: json_string_field(row, "asset_id"),
        tenant_id: json_string_field(row, "tenant_id"),
        project_id: json_string_field(row, "project_id"),
        file_id: json_string_field(row, "file_id"),
        name: json_string_field(row, "name"),
        media_type: json_string_field(row, "media_type"),
        status: asset_status_from_db(&json_string_field(row, "status")),
        metadata: json_string_field(row, "metadata"),
        ..Default::default()
    }
}

pub(crate) fn pipeline_definition_from_json(
    row: &serde_json::Value,
) -> asset_entity_pb::PipelineDefinition {
    let row = native_json_object(row);
    asset_entity_pb::PipelineDefinition {
        definition_id: json_string_field(row, "definition_id"),
        tenant_id: json_string_field(row, "tenant_id"),
        name: json_string_field(row, "name"),
        description: json_string_field(row, "description"),
        media_type: json_string_field(row, "media_type"),
        steps: json_string_field(row, "steps"),
        version: json_i32_field(row, "version"),
        status: json_string_field(row, "status"),
        trigger_topic: json_string_field(row, "trigger_topic"),
        ..Default::default()
    }
}

// ── enum<->db (stored as SHORT tokens in VARCHAR(20) via the proto_enum serializer) ─

pub(crate) fn asset_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::AssetStatus as S;
    match value {
        "PENDING" | "ASSET_STATUS_PENDING" => S::Pending as i32,
        "READY" | "ASSET_STATUS_READY" => S::Ready as i32,
        "FAILED" | "ASSET_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

pub(crate) fn asset_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "ASSET_STATUS_PENDING" => "PENDING",
        "READY" | "ASSET_STATUS_READY" => "READY",
        "FAILED" | "ASSET_STATUS_FAILED" => "FAILED",
        other => {
            return Err(asset_invalid_field(
                "status",
                "must be a supported AssetStatus enum value",
                format!("unknown asset status: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

pub(crate) fn pipeline_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::PipelineStatus as S;
    match value {
        "PENDING" | "PIPELINE_STATUS_PENDING" => S::Pending as i32,
        "RUNNING" | "PIPELINE_STATUS_RUNNING" => S::Running as i32,
        "COMPLETED" | "PIPELINE_STATUS_COMPLETED" => S::Completed as i32,
        "FAILED" | "PIPELINE_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

pub(crate) fn step_status_from_db(value: &str) -> i32 {
    use asset_entity_pb::StepStatus as S;
    match value {
        "PENDING" | "STEP_STATUS_PENDING" => S::Pending as i32,
        "RUNNING" | "STEP_STATUS_RUNNING" => S::Running as i32,
        "COMPLETED" | "STEP_STATUS_COMPLETED" => S::Completed as i32,
        "SKIPPED" | "STEP_STATUS_SKIPPED" => S::Skipped as i32,
        "FAILED" | "STEP_STATUS_FAILED" => S::Failed as i32,
        _ => S::Unspecified as i32,
    }
}

/// Normalize a step-status string to the canonical SHORT stored token. Accepts
/// the short or proto-prefixed form, empty→`default`, rejects unknown input so
/// it never overflows VARCHAR(20) or reads back as Unspecified.
pub(crate) fn step_status_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "PENDING" | "STEP_STATUS_PENDING" => "PENDING",
        "RUNNING" | "STEP_STATUS_RUNNING" => "RUNNING",
        "COMPLETED" | "STEP_STATUS_COMPLETED" => "COMPLETED",
        "SKIPPED" | "STEP_STATUS_SKIPPED" => "SKIPPED",
        "FAILED" | "STEP_STATUS_FAILED" => "FAILED",
        other => {
            return Err(asset_invalid_field(
                "status",
                "must be a supported StepStatus enum value",
                format!("unknown step status: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

pub(crate) fn step_type_from_db(value: &str) -> i32 {
    use asset_entity_pb::StepType as T;
    match value {
        "EMBED" | "STEP_TYPE_EMBED" => T::Embed as i32,
        "THUMBNAIL" | "STEP_TYPE_THUMBNAIL" => T::Thumbnail as i32,
        "RESIZE" | "STEP_TYPE_RESIZE" => T::Resize as i32,
        "TRANSCODE" | "STEP_TYPE_TRANSCODE" => T::Transcode as i32,
        "CAPTION" | "STEP_TYPE_CAPTION" => T::Caption as i32,
        "EXTRACT" | "STEP_TYPE_EXTRACT" => T::Extract as i32,
        _ => T::Unspecified as i32,
    }
}

/// Normalize a step-type string to the canonical SHORT stored token. Same
/// accept-both-forms / reject-unknown / empty→default contract as
/// [`step_status_to_db`].
pub(crate) fn step_type_to_db(value: &str, default: &str) -> Result<String, Status> {
    let v = value.trim();
    if v.is_empty() {
        return Ok(default.to_string());
    }
    let short = match v.to_ascii_uppercase().as_str() {
        "EMBED" | "STEP_TYPE_EMBED" => "EMBED",
        "THUMBNAIL" | "STEP_TYPE_THUMBNAIL" => "THUMBNAIL",
        "RESIZE" | "STEP_TYPE_RESIZE" => "RESIZE",
        "TRANSCODE" | "STEP_TYPE_TRANSCODE" => "TRANSCODE",
        "CAPTION" | "STEP_TYPE_CAPTION" => "CAPTION",
        "EXTRACT" | "STEP_TYPE_EXTRACT" => "EXTRACT",
        other => {
            return Err(asset_invalid_field(
                "step_type",
                "must be a supported StepType enum value",
                format!("unknown step type: {other}"),
            ));
        }
    };
    Ok(short.to_string())
}

pub(crate) fn pipeline_instance_select_projection(m: &NativeModel) -> String {
    [
        m.text("instance_id"),
        m.text("definition_id"),
        m.text("asset_id"),
        m.text("tenant_id"),
        m.text_or_empty("status"),
        m.text_or_empty("current_step"),
        m.text_or_empty("context"),
        m.text_or_empty("correlation_id"),
    ]
    .join(", ")
}

pub(crate) fn pipeline_instance_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineInstance, Status> {
    let map = |e: sqlx::Error| {
        asset_internal_status(
            "decode_pipeline_instance",
            format!("decode pipeline instance failed: {e}"),
        )
    };
    Ok(asset_entity_pb::PipelineInstance {
        instance_id: row.try_get("instance_id").map_err(map)?,
        definition_id: row.try_get("definition_id").map_err(map)?,
        asset_id: row.try_get("asset_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        status: pipeline_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        current_step: row.try_get("current_step").map_err(map)?,
        context: row.try_get("context").map_err(map)?,
        correlation_id: row.try_get("correlation_id").map_err(map)?,
        ..Default::default()
    })
}

pub(crate) fn pipeline_step_select_projection(m: &NativeModel) -> String {
    [
        m.text("step_id"),
        m.text("instance_id"),
        m.text("tenant_id"),
        m.text_or_empty("step_name"),
        m.text_or_empty("step_type"),
        m.text_or_empty("status"),
        m.text_or_empty("result"),
        m.text_or_empty("error"),
        m.text_or_empty("params"),
        m.select("retry_count"),
    ]
    .join(", ")
}

pub(crate) fn pipeline_step_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<asset_entity_pb::PipelineStep, Status> {
    let map = |e: sqlx::Error| {
        asset_internal_status(
            "decode_pipeline_step",
            format!("decode pipeline step failed: {e}"),
        )
    };
    Ok(asset_entity_pb::PipelineStep {
        step_id: row.try_get("step_id").map_err(map)?,
        instance_id: row.try_get("instance_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        step_name: row.try_get("step_name").map_err(map)?,
        step_type: step_type_from_db(&row.try_get::<String, _>("step_type").map_err(map)?),
        status: step_status_from_db(&row.try_get::<String, _>("status").map_err(map)?),
        result: row.try_get("result").map_err(map)?,
        error: row.try_get("error").map_err(map)?,
        params: row.try_get("params").map_err(map)?,
        retry_count: row.try_get::<i32, _>("retry_count").map_err(map)?,
        ..Default::default()
    })
}
