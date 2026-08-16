//! Neutral-IR query/record builders for the native `AssetService`: the
//! `LogicalValue`/filter helpers, the asset + pipeline-definition projections and
//! reads, and the insert records. Extracted verbatim from the former god file so
//! the mediated native-entity path builds the exact same IR as before.

use tonic::Status;

use crate::ir::{
    ComparisonOp, LogicalFilter, LogicalPagination, LogicalProjection, LogicalRead, LogicalRecord,
    LogicalSort, LogicalValue, SortDirection,
};
use crate::proto::udb::core::asset::services::v1 as asset_pb;

use super::config::{ASSET_MSG, PIPELINE_DEFINITION_MSG};
use super::errors::asset_invalid_field;

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn logical_json_text(value: &str) -> Result<LogicalValue, Status> {
    serde_json::from_str::<serde_json::Value>(value)
        .map(LogicalValue::Json)
        .map_err(|err| {
            asset_invalid_field(
                "json",
                "must be valid native JSON",
                format!("native JSON field is invalid: {err}"),
            )
        })
}

pub(crate) fn eq_filter(field: &str, value: impl Into<String>) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

pub(crate) fn and_filter(filters: Vec<LogicalFilter>) -> LogicalFilter {
    LogicalFilter::And(filters)
}

pub(crate) fn asset_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "asset_id".to_string(),
        "tenant_id".to_string(),
        "project_id".to_string(),
        "file_id".to_string(),
        "name".to_string(),
        "media_type".to_string(),
        "status".to_string(),
        "metadata".to_string(),
    ])
}

pub(crate) fn pipeline_definition_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "definition_id".to_string(),
        "tenant_id".to_string(),
        "name".to_string(),
        "description".to_string(),
        "media_type".to_string(),
        "steps".to_string(),
        "version".to_string(),
        "status".to_string(),
        "trigger_topic".to_string(),
    ])
}

/// Asset reads are scoped by tenant and, when the verified caller carries one,
/// by the owning project. An empty `project_id` is an intentionally tenant-wide
/// caller and adds no clause — exactly the shape `file_tenant_active_clauses`
/// uses in `storage_service`. The clause lives here rather than relying on a
/// compiler-injected context predicate because these two RPCs read through a
/// tenant-only context.
pub(crate) fn asset_read(
    tenant_id: &str,
    project_id: &str,
    asset_id: Option<&str>,
    media_type: Option<&str>,
    status: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    let mut filters = vec![
        eq_filter("tenant_id", tenant_id),
        LogicalFilter::IsNull("deleted_at".to_string()),
    ];
    if !project_id.trim().is_empty() {
        filters.push(eq_filter("project_id", project_id.trim()));
    }
    if let Some(asset_id) = asset_id.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("asset_id", asset_id));
    }
    if let Some(media_type) = media_type.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("media_type", media_type));
    }
    if let Some(status) = status.filter(|value| !value.trim().is_empty()) {
        filters.push(eq_filter("status", status));
    }
    LogicalRead {
        message_type: ASSET_MSG.to_string(),
        filter: Some(and_filter(filters)),
        projection: Some(asset_projection()),
        sort: vec![LogicalSort {
            field: "name".to_string(),
            direction: SortDirection::Asc,
            nulls: Default::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

pub(crate) fn pipeline_definition_read(tenant_id: &str, definition_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: PIPELINE_DEFINITION_MSG.to_string(),
        filter: Some(and_filter(vec![
            eq_filter("definition_id", definition_id),
            eq_filter("tenant_id", tenant_id),
        ])),
        projection: Some(pipeline_definition_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn asset_record(
    asset_id: &str,
    tenant_id: &str,
    project_id: &str,
    req: &asset_pb::RegisterAssetRequest,
    metadata_json: &str,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert("asset_id".to_string(), logical_string(asset_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert(
        "project_id".to_string(),
        if project_id.trim().is_empty() {
            LogicalValue::Null
        } else {
            logical_string(project_id)
        },
    );
    record.insert("file_id".to_string(), logical_string(req.file_id.trim()));
    record.insert("name".to_string(), logical_string(req.name.clone()));
    record.insert(
        "media_type".to_string(),
        logical_string(req.media_type.clone()),
    );
    record.insert("status".to_string(), logical_string("PENDING"));
    record.insert("metadata".to_string(), logical_json_text(metadata_json)?);
    Ok(record)
}

pub(crate) fn pipeline_definition_record(
    definition_id: &str,
    tenant_id: &str,
    req: &asset_pb::CreatePipelineDefinitionRequest,
    steps_json: &str,
    version: i32,
) -> Result<LogicalRecord, Status> {
    let mut record = LogicalRecord::new();
    record.insert("definition_id".to_string(), logical_string(definition_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("name".to_string(), logical_string(req.name.clone()));
    record.insert(
        "description".to_string(),
        logical_string(req.description.clone()),
    );
    record.insert(
        "media_type".to_string(),
        logical_string(req.media_type.clone()),
    );
    record.insert("steps".to_string(), logical_json_text(steps_json)?);
    record.insert("version".to_string(), LogicalValue::Int(version as i64));
    record.insert("status".to_string(), logical_string("ACTIVE"));
    Ok(record)
}
