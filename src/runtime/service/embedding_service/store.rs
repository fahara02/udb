//! Neutral-IR query/record builders and the manifest model for the native
//! `EmbeddingService`. Extracted verbatim — the `LogicalRead`/`LogicalRecord`
//! shapes, the tenant-scoped source filter, the active-source read, the upsert
//! record/conflict, and the native SQL model are byte-for-byte identical to the
//! former god file.

use chrono::{TimeZone, Utc};

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{
    EMBEDDING_DOCUMENT_MSG, EMBEDDING_JOB_MSG, EMBEDDING_MODEL_MSG, EMBEDDING_SOURCE_MSG,
    EMBEDDING_WORK_ITEM_MSG, STATUS_ACTIVE,
};

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

fn tenant_filter(tenant_id: &str) -> Vec<LogicalFilter> {
    vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }]
}

pub(crate) fn model_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "model_id",
            "tenant_id",
            "provider",
            "model_name",
            "version",
            "dimensions",
            "matryoshka_dims_json",
            "distance_metric",
            "normalize",
            "output_dtype",
            "rescore",
            "max_input_tokens",
            "tokenizer",
            "task_type",
            "asymmetric",
            "provider_endpoint_ref",
            "status",
            "replacement_model_id",
            "retire_after",
            "vector_backend",
            "vector_instance",
            "collection_alias",
            "active_collection",
            "chunking_strategy",
            "chunk_tokens",
            "chunk_overlap_tokens",
            "contextual_retrieval",
            "late_chunking",
            "tenant_state",
            "metadata_json",
        ]
        .into_iter()
        .map(str::to_string),
    )
}

pub(crate) fn model_read_by_id(tenant_id: &str, model_id: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "model_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(model_id),
    });
    LogicalRead {
        message_type: EMBEDDING_MODEL_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(model_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn model_read_by_collection(tenant_id: &str, collection: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "active_collection".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(collection),
    });
    LogicalRead {
        message_type: EMBEDDING_MODEL_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(model_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(2)),
    }
}

pub(crate) fn models_read(
    tenant_id: &str,
    status: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    if let Some(status) = status.filter(|value| !value.is_empty()) {
        filters.push(LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(status),
        });
    }
    LogicalRead {
        message_type: EMBEDDING_MODEL_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(model_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn model_record(
    model_id: &str,
    tenant_id: &str,
    provider: &str,
    model_name: &str,
    version: &str,
    dimensions: i32,
    matryoshka_dims_json: &str,
    distance_metric: &str,
    normalize: bool,
    output_dtype: &str,
    rescore: bool,
    max_input_tokens: i32,
    tokenizer: &str,
    task_type: &str,
    asymmetric: bool,
    provider_endpoint_ref: &str,
    status: &str,
    replacement_model_id: &str,
    vector_backend: &str,
    vector_instance: &str,
    collection_alias: &str,
    active_collection: &str,
    chunking_strategy: &str,
    chunk_tokens: i32,
    chunk_overlap_tokens: i32,
    contextual_retrieval: bool,
    late_chunking: bool,
    tenant_state: &str,
    retire_after_unix_ms: i64,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    for (key, value) in [
        ("model_id", model_id),
        ("tenant_id", tenant_id),
        ("provider", provider),
        ("model_name", model_name),
        ("version", version),
        ("matryoshka_dims_json", matryoshka_dims_json),
        ("distance_metric", distance_metric),
        ("output_dtype", output_dtype),
        ("tokenizer", tokenizer),
        ("task_type", task_type),
        ("provider_endpoint_ref", provider_endpoint_ref),
        ("status", status),
        ("vector_backend", vector_backend),
        ("vector_instance", vector_instance),
        ("collection_alias", collection_alias),
        ("active_collection", active_collection),
        ("chunking_strategy", chunking_strategy),
        ("tenant_state", tenant_state),
        ("metadata_json", metadata_json),
    ] {
        record.insert(key.to_string(), logical_string(value));
    }
    record.insert(
        "dimensions".to_string(),
        LogicalValue::Int(i64::from(dimensions)),
    );
    record.insert(
        "replacement_model_id".to_string(),
        if replacement_model_id.trim().is_empty() {
            LogicalValue::Null
        } else {
            logical_string(replacement_model_id)
        },
    );
    record.insert("normalize".to_string(), LogicalValue::Bool(normalize));
    record.insert("rescore".to_string(), LogicalValue::Bool(rescore));
    record.insert(
        "max_input_tokens".to_string(),
        LogicalValue::Int(i64::from(max_input_tokens)),
    );
    record.insert("asymmetric".to_string(), LogicalValue::Bool(asymmetric));
    record.insert(
        "chunk_tokens".to_string(),
        LogicalValue::Int(i64::from(chunk_tokens)),
    );
    record.insert(
        "chunk_overlap_tokens".to_string(),
        LogicalValue::Int(i64::from(chunk_overlap_tokens)),
    );
    record.insert(
        "contextual_retrieval".to_string(),
        LogicalValue::Bool(contextual_retrieval),
    );
    record.insert(
        "late_chunking".to_string(),
        LogicalValue::Bool(late_chunking),
    );
    let retire_after = if retire_after_unix_ms > 0 {
        Utc.timestamp_millis_opt(retire_after_unix_ms)
            .single()
            .map(LogicalValue::Timestamp)
            .unwrap_or(LogicalValue::Null)
    } else {
        LogicalValue::Null
    };
    record.insert("retire_after".to_string(), retire_after);
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record
}

pub(crate) fn model_conflict() -> ConflictStrategy {
    ConflictStrategy::update(
        vec![
            "provider_endpoint_ref",
            "status",
            "replacement_model_id",
            "collection_alias",
            "chunking_strategy",
            "chunk_tokens",
            "chunk_overlap_tokens",
            "contextual_retrieval",
            "late_chunking",
            "tenant_state",
            "retire_after",
            "metadata_json",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    )
}

pub(crate) fn job_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "job_id",
            "tenant_id",
            "project_id",
            "source_name",
            "document_id",
            "job_type",
            "mode",
            "status",
            "rows_enumerated",
            "chunks_emitted",
            "vectors_stored",
            "failed",
            "error",
            "created_at",
            "started_at",
            "finished_at",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string),
    )
}

pub(crate) fn job_read_by_id(tenant_id: &str, job_id: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "job_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(job_id),
    });
    LogicalRead {
        message_type: EMBEDDING_JOB_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(job_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn job_record(
    job_id: &str,
    tenant_id: &str,
    project_id: &str,
    source_name: &str,
    document_id: &str,
    job_type: &str,
    mode: &str,
    status: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    for (key, value) in [
        ("job_id", job_id),
        ("tenant_id", tenant_id),
        ("project_id", project_id),
        ("source_name", source_name),
        ("document_id", document_id),
        ("job_type", job_type),
        ("mode", mode),
        ("status", status),
        ("error", ""),
        ("metadata_json", "{}"),
    ] {
        record.insert(key.to_string(), logical_string(value));
    }
    for key in [
        "rows_enumerated",
        "chunks_emitted",
        "vectors_stored",
        "failed",
    ] {
        record.insert(key.to_string(), LogicalValue::Int(0));
    }
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record
}

pub(crate) fn job_conflict() -> ConflictStrategy {
    ConflictStrategy::update(
        vec![
            "status",
            "rows_enumerated",
            "chunks_emitted",
            "vectors_stored",
            "failed",
            "error",
            "started_at",
            "finished_at",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    )
}

pub(crate) fn work_items_read(
    tenant_id: &str,
    job_id: &str,
    status: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    if !job_id.trim().is_empty() {
        filters.push(LogicalFilter::Comparison {
            field: "job_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(job_id),
        });
    }
    if let Some(status) = status.filter(|value| !value.is_empty()) {
        filters.push(LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(status),
        });
    }
    LogicalRead {
        message_type: EMBEDDING_WORK_ITEM_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields(
            [
                "work_item_id",
                "point_id",
                "source_name",
                "parent_pk",
                "chunk_seq",
                "chunk_hash",
                "status",
                "attempt_count",
                "max_attempts",
                "last_error",
                "next_attempt_at",
            ]
            .into_iter()
            .map(str::to_string),
        )),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

pub(crate) fn work_item_read_by_id(tenant_id: &str, work_item_id: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "work_item_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(work_item_id),
    });
    LogicalRead {
        message_type: EMBEDDING_WORK_ITEM_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields(
            [
                "work_item_id",
                "tenant_id",
                "project_id",
                "job_id",
                "source_name",
                "parent_pk",
                "point_id",
                "document_id",
                "doc_version",
                "chunk_seq",
                "chunk_count",
                "chunk_hash",
                "chunk_text",
                "parent_text",
                "char_start",
                "char_end",
                "token_start",
                "token_end",
                "model_id",
                "target_collection",
                "status",
                "attempt_count",
                "max_attempts",
                "last_error",
                "retryable",
                "token_count",
                "next_attempt_at",
                "last_emitted_at",
                "acked_at",
                "updated_at",
            ]
            .into_iter()
            .map(str::to_string),
        )),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn work_item_record(
    work_item_id: &str,
    tenant_id: &str,
    project_id: &str,
    job_id: &str,
    source_name: &str,
    parent_pk: &str,
    point_id: &str,
    document_id: &str,
    doc_version: &str,
    chunk_seq: i32,
    chunk_count: i32,
    chunk_hash: &str,
    chunk_text: &str,
    model_id: &str,
    target_collection: &str,
    status: &str,
    attempt_count: i32,
    max_attempts: i32,
    last_error: &str,
    retryable: bool,
    token_count: i64,
    parent_text: &str,
    char_start: i64,
    char_end: i64,
    token_start: i64,
    token_end: i64,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    for (key, value) in [
        ("work_item_id", work_item_id),
        ("tenant_id", tenant_id),
        ("project_id", project_id),
        ("source_name", source_name),
        ("parent_pk", parent_pk),
        ("point_id", point_id),
        ("document_id", document_id),
        ("doc_version", doc_version),
        ("chunk_hash", chunk_hash),
        ("chunk_text", chunk_text),
        ("parent_text", parent_text),
        ("model_id", model_id),
        ("target_collection", target_collection),
        ("status", status),
        ("last_error", last_error),
    ] {
        record.insert(key.to_string(), logical_string(value));
    }
    record.insert(
        "job_id".to_string(),
        if job_id.trim().is_empty() {
            LogicalValue::Null
        } else {
            logical_string(job_id)
        },
    );
    record.insert(
        "chunk_seq".to_string(),
        LogicalValue::Int(i64::from(chunk_seq)),
    );
    record.insert(
        "chunk_count".to_string(),
        LogicalValue::Int(i64::from(chunk_count)),
    );
    record.insert(
        "attempt_count".to_string(),
        LogicalValue::Int(i64::from(attempt_count)),
    );
    record.insert(
        "max_attempts".to_string(),
        LogicalValue::Int(i64::from(max_attempts)),
    );
    record.insert("retryable".to_string(), LogicalValue::Bool(retryable));
    record.insert("token_count".to_string(), LogicalValue::Int(token_count));
    record.insert("char_start".to_string(), LogicalValue::Int(char_start));
    record.insert("char_end".to_string(), LogicalValue::Int(char_end));
    record.insert("token_start".to_string(), LogicalValue::Int(token_start));
    record.insert("token_end".to_string(), LogicalValue::Int(token_end));
    record.insert("next_attempt_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record
}

pub(crate) fn work_item_conflict() -> ConflictStrategy {
    ConflictStrategy::update(
        vec![
            "job_id",
            "chunk_count",
            "status",
            "attempt_count",
            "max_attempts",
            "last_error",
            "retryable",
            "token_count",
            "parent_text",
            "char_start",
            "char_end",
            "token_start",
            "token_end",
            "next_attempt_at",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    )
}

pub(crate) fn document_record(
    document_id: &str,
    tenant_id: &str,
    project_id: &str,
    external_id: &str,
    title: &str,
    raw_text: &str,
    storage_object_ref: &str,
    content_type: &str,
    doc_version: &str,
    model_id: &str,
    target_collection: &str,
    status: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    for (key, value) in [
        ("document_id", document_id),
        ("tenant_id", tenant_id),
        ("project_id", project_id),
        ("external_id", external_id),
        ("title", title),
        ("raw_text", raw_text),
        ("storage_object_ref", storage_object_ref),
        ("content_type", content_type),
        ("doc_version", doc_version),
        ("model_id", model_id),
        ("target_collection", target_collection),
        ("status", status),
        ("metadata_json", metadata_json),
    ] {
        record.insert(key.to_string(), logical_string(value));
    }
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("updated_at".to_string(), LogicalValue::Timestamp(now));
    record
}

fn document_projection() -> LogicalProjection {
    LogicalProjection::fields(
        [
            "document_id",
            "tenant_id",
            "project_id",
            "external_id",
            "title",
            "raw_text",
            "storage_object_ref",
            "content_type",
            "doc_version",
            "model_id",
            "target_collection",
            "status",
            "metadata_json",
            "created_at",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string),
    )
}

pub(crate) fn document_read_by_external(tenant_id: &str, external_id: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "external_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(external_id),
    });
    LogicalRead {
        message_type: EMBEDDING_DOCUMENT_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(document_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn document_read_by_id(tenant_id: &str, document_id: &str) -> LogicalRead {
    let mut filters = tenant_filter(tenant_id);
    filters.push(LogicalFilter::Comparison {
        field: "document_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(document_id),
    });
    LogicalRead {
        message_type: EMBEDDING_DOCUMENT_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(document_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn document_conflict() -> ConflictStrategy {
    ConflictStrategy::update(
        vec![
            "title",
            "raw_text",
            "storage_object_ref",
            "content_type",
            "doc_version",
            "model_id",
            "target_collection",
            "status",
            "metadata_json",
            "updated_at",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
    )
}
