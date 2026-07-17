//! Neutral-IR query/record builders and the manifest model for the native
//! `SearchService`. Extracted verbatim — the `LogicalRead`/`LogicalRecord`
//! shapes, the tenant-scoped index filter, the active-index read, the upsert
//! record/conflict, and the native SQL model are byte-for-byte identical to the
//! former god file.

use chrono::Utc;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{SEARCH_INDEX_MSG, STATUS_ACTIVE};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn index_filter(
    tenant_id: &str,
    index_name: Option<&str>,
    status: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(index_name) = index_name {
        filters.push(LogicalFilter::Comparison {
            field: "index_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(index_name),
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

pub(crate) fn index_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "index_id".to_string(),
        "index_name".to_string(),
        "source_message_type".to_string(),
        "backend".to_string(),
        "resource_name".to_string(),
        "vector_dims".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
    ])
}

pub(crate) fn index_read_by_name(tenant_id: &str, index_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: SEARCH_INDEX_MSG.to_string(),
        filter: Some(index_filter(tenant_id, Some(index_name), None)),
        projection: Some(index_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn active_indexes_read(tenant_id: &str, offset: u64, limit: u32) -> LogicalRead {
    LogicalRead {
        message_type: SEARCH_INDEX_MSG.to_string(),
        filter: Some(index_filter(tenant_id, None, Some(STATUS_ACTIVE))),
        projection: Some(index_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn index_record(
    index_id: &str,
    tenant_id: &str,
    index_name: &str,
    source_message_type: &str,
    backend: &str,
    resource_name: &str,
    vector_dims: i32,
    tenant_column: &str,
    source_cdc_topic: &str,
    status: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("index_id".to_string(), logical_string(index_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("index_name".to_string(), logical_string(index_name));
    record.insert(
        "source_message_type".to_string(),
        logical_string(source_message_type),
    );
    record.insert("backend".to_string(), logical_string(backend));
    record.insert("resource_name".to_string(), logical_string(resource_name));
    record.insert(
        "vector_dims".to_string(),
        LogicalValue::Int(i64::from(vector_dims)),
    );
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
/// conflict target is the message primary key (`index_id`), reused for an
/// existing (tenant, index_name) row so a re-register never violates the unique
/// index.
pub(crate) fn index_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "source_message_type".to_string(),
        "backend".to_string(),
        "resource_name".to_string(),
        "vector_dims".to_string(),
        "tenant_column".to_string(),
        "source_cdc_topic".to_string(),
        "status".to_string(),
        "updated_at".to_string(),
        "metadata_json".to_string(),
    ])
}

/// The search-index registration as a native SQL model (relation + quoted
/// columns), resolved from the embedded native manifest — never a hand-mapped
/// schema.
pub(crate) fn search_index_model() -> NativeModel {
    native_model(
        SEARCH_INDEX_MSG,
        &[
            "index_id",
            "tenant_id",
            "index_name",
            "source_message_type",
            "backend",
            "resource_name",
            "vector_dims",
            "tenant_column",
            "source_cdc_topic",
            "status",
        ],
    )
}
