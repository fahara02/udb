//! Neutral-IR query/record builders and the manifest model for the native
//! `EmbeddingService`. Extracted verbatim — the `LogicalRead`/`LogicalRecord`
//! shapes, the tenant-scoped source filter, the active-source read, the upsert
//! record/conflict, and the native SQL model are byte-for-byte identical to the
//! former god file.

use chrono::Utc;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{EMBEDDING_SOURCE_MSG, STATUS_ACTIVE};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn source_filter(
    tenant_id: &str,
    source_name: Option<&str>,
    status: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(source_name) = source_name {
        filters.push(LogicalFilter::Comparison {
            field: "source_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(source_name),
        });
    }
    if let Some(status) = status {
        filters.push(LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(status),
        });
    }
    LogicalFilter::And(filters)
}

pub(crate) fn source_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "source_id".to_string(),
        "tenant_id".to_string(),
        "source_name".to_string(),
        "source_message_type".to_string(),
        "text_fields_json".to_string(),
        "target_collection".to_string(),
        "model_id".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
    ])
}

pub(crate) fn embedding_source_model() -> NativeModel {
    native_model(
        EMBEDDING_SOURCE_MSG,
        &[
            "source_id",
            "tenant_id",
            "source_name",
            "source_message_type",
            "text_fields_json",
            "target_collection",
            "model_id",
            "tenant_column",
            "source_cdc_topic",
            "status",
        ],
    )
}

pub(crate) fn source_read_by_name(tenant_id: &str, source_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: EMBEDDING_SOURCE_MSG.to_string(),
        filter: Some(source_filter(tenant_id, Some(source_name), None)),
        projection: Some(source_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn active_sources_read(tenant_id: &str, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: EMBEDDING_SOURCE_MSG.to_string(),
        filter: Some(source_filter(tenant_id, None, Some(STATUS_ACTIVE))),
        projection: Some(source_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn source_record(
    source_id: &str,
    tenant_id: &str,
    source_name: &str,
    source_message_type: &str,
    text_fields_json: &str,
    target_collection: &str,
    model_id: &str,
    tenant_column: &str,
    source_cdc_topic: &str,
    status: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("source_id".to_string(), logical_string(source_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("source_name".to_string(), logical_string(source_name));
    record.insert(
        "source_message_type".to_string(),
        logical_string(source_message_type),
    );
    record.insert(
        "text_fields_json".to_string(),
        logical_string(text_fields_json),
    );
    record.insert(
        "target_collection".to_string(),
        logical_string(target_collection),
    );
    record.insert("model_id".to_string(), logical_string(model_id));
    record.insert("tenant_column".to_string(), logical_string(tenant_column));
    record.insert(
        "source_cdc_topic".to_string(),
        logical_string(source_cdc_topic),
    );
    record.insert("status".to_string(), logical_string(status));
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// Mutable columns an upsert may overwrite on re-register / status change. The
/// conflict target is the message primary key (`source_id`), reused for an
/// existing (tenant, source_name) row so a re-register never violates the unique
/// index.
pub(crate) fn source_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "source_message_type".to_string(),
        "text_fields_json".to_string(),
        "target_collection".to_string(),
        "model_id".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
        "updated_at".to_string(),
        "metadata_json".to_string(),
    ])
}
