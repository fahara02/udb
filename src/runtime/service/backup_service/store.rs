//! Neutral-IR query/record builders, projection field lists, the durable journal
//! write, and the list-pagination helpers for the native `BackupService`.
//! Extracted verbatim from the former god file — the `LogicalRead`/`LogicalRecord`
//! shapes, the tenant-scoped filters, and the conflict strategies are
//! byte-for-byte identical.

use chrono::Utc;
use tonic::Status;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder, SortDirection,
};
use crate::runtime::DataBrokerRuntime;

use super::config::{
    BACKUP_POLICY_MSG, BACKUP_RUN_MSG, MAX_LIST_ROWS, STATUS_COMPLETED, STATUS_RUNNING,
};

/// The full BackupRun column set read back into a [`backup_pb::BackupRunSummary`].
pub(crate) fn run_summary_fields() -> Vec<String> {
    [
        "backup_id",
        "tenant_id",
        "kind",
        "status",
        "object_prefix",
        "manifest_checksum",
        "table_count",
        "total_rows",
        "excluded_count",
        "source_tenant_id",
        "target_tenant_id",
        "created_at",
        "completed_at",
        "metadata_json",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

// ── native-entity reads (journal + policy) ───────────────────────────────────

pub(crate) fn run_read_by_id(tenant_id: &str, backup_id: &str) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_RUN_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "backup_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(backup_id),
            },
        ])),
        projection: Some(LogicalProjection::fields(run_summary_fields())),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn runs_list_read(
    tenant_id: &str,
    kind: Option<&str>,
    limit: u32,
    offset: u64,
) -> LogicalRead {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(kind) = kind {
        filters.push(LogicalFilter::Comparison {
            field: "kind".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(kind),
        });
    }
    LogicalRead {
        message_type: BACKUP_RUN_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields(run_summary_fields())),
        sort: vec![LogicalSort {
            field: "created_at".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination {
            limit: Some(limit),
            offset: Some(offset),
            ..LogicalPagination::default()
        }),
    }
}

/// The full BackupPolicy column set read back into a [`backup_pb::BackupPolicyView`].
pub(crate) fn policy_view_fields() -> Vec<String> {
    [
        "policy_id",
        "tenant_id",
        "policy_name",
        "schedule_cron",
        "retention_days",
        "max_retained_backups",
        "enabled",
        "object_backend",
        "object_bucket",
        "created_at",
        "updated_at",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

pub(crate) fn policy_read_by_name(tenant_id: &str, policy_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_POLICY_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "policy_name".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(policy_name),
            },
        ])),
        projection: Some(LogicalProjection::fields(policy_view_fields())),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

pub(crate) fn policies_list_read(tenant_id: &str, limit: u32, offset: u64) -> LogicalRead {
    LogicalRead {
        message_type: BACKUP_POLICY_MSG.to_string(),
        filter: Some(LogicalFilter::Comparison {
            field: "tenant_id".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(tenant_id),
        }),
        projection: Some(LogicalProjection::fields(policy_view_fields())),
        sort: vec![LogicalSort {
            field: "policy_name".to_string(),
            direction: SortDirection::Asc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination {
            limit: Some(limit),
            offset: Some(offset),
            ..LogicalPagination::default()
        }),
    }
}

pub(crate) fn policy_conflict() -> ConflictStrategy {
    ConflictStrategy::update_on(
        vec![
            "schedule_cron".to_string(),
            "retention_days".to_string(),
            "max_retained_backups".to_string(),
            "enabled".to_string(),
            "object_backend".to_string(),
            "object_bucket".to_string(),
            "updated_at".to_string(),
            "metadata_json".to_string(),
        ],
        vec!["tenant_id".to_string(), "policy_name".to_string()],
    )
}

/// Persist the durable RUNNING identity before any backup artifact is written.
/// A collision is an error: operation ids are immutable journal identities, not
/// replaceable aliases.
pub(crate) async fn journal_run_started(
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    backup_id: &str,
    tenant_id: &str,
    kind: &str,
    object_prefix: &str,
    metadata: &serde_json::Value,
) -> Result<(), Status> {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("backup_id".to_string(), logical_string(backup_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("kind".to_string(), logical_string(kind));
    record.insert("status".to_string(), logical_string(STATUS_RUNNING));
    record.insert("object_prefix".to_string(), logical_string(object_prefix));
    record.insert("manifest_checksum".to_string(), logical_string(""));
    record.insert("table_count".to_string(), LogicalValue::Int(0));
    record.insert("total_rows".to_string(), LogicalValue::Int(0));
    record.insert("excluded_count".to_string(), LogicalValue::Int(0));
    record.insert("source_tenant_id".to_string(), logical_string(""));
    record.insert("target_tenant_id".to_string(), logical_string(""));
    record.insert("error_message".to_string(), logical_string(""));
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("completed_at".to_string(), LogicalValue::Null);
    record.insert(
        "metadata_json".to_string(),
        logical_string(metadata.to_string()),
    );
    runtime
        .native_entity_write_for_service(
            "backup",
            context,
            BACKUP_RUN_MSG,
            record,
            ConflictStrategy::Error,
        )
        .await
        .map(|_| ())
}

/// Transition an existing run to COMPLETED after its side effects and immutable
/// integrity metadata are durable.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn journal_run(
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    backup_id: &str,
    tenant_id: &str,
    kind: &str,
    object_prefix: &str,
    manifest_checksum: &str,
    table_count: i64,
    total_rows: i64,
    excluded_count: i64,
    source_tenant_id: &str,
    target_tenant_id: &str,
    metadata: &serde_json::Value,
) -> Result<(), Status> {
    let now = Utc::now();
    let mut record = LogicalRecord::new();
    record.insert("backup_id".to_string(), logical_string(backup_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("kind".to_string(), logical_string(kind));
    record.insert("status".to_string(), logical_string(STATUS_COMPLETED));
    record.insert("object_prefix".to_string(), logical_string(object_prefix));
    record.insert(
        "manifest_checksum".to_string(),
        logical_string(manifest_checksum),
    );
    record.insert("table_count".to_string(), LogicalValue::Int(table_count));
    record.insert("total_rows".to_string(), LogicalValue::Int(total_rows));
    record.insert(
        "excluded_count".to_string(),
        LogicalValue::Int(excluded_count),
    );
    record.insert(
        "source_tenant_id".to_string(),
        logical_string(source_tenant_id),
    );
    record.insert(
        "target_tenant_id".to_string(),
        logical_string(target_tenant_id),
    );
    record.insert("error_message".to_string(), logical_string(""));
    record.insert("created_at".to_string(), LogicalValue::Timestamp(now));
    record.insert("completed_at".to_string(), LogicalValue::Timestamp(now));
    record.insert(
        "metadata_json".to_string(),
        logical_string(metadata.to_string()),
    );
    runtime
        .native_entity_write_for_service(
            "backup",
            context,
            BACKUP_RUN_MSG,
            record,
            ConflictStrategy::update(vec![
                "status".to_string(),
                "object_prefix".to_string(),
                "manifest_checksum".to_string(),
                "table_count".to_string(),
                "total_rows".to_string(),
                "excluded_count".to_string(),
                "completed_at".to_string(),
                "metadata_json".to_string(),
            ]),
        )
        .await
        .map(|_| ())
}

pub(crate) fn clamp_limit(page_size: i32) -> u32 {
    if page_size <= 0 {
        100
    } else {
        (page_size as u32).min(MAX_LIST_ROWS)
    }
}

pub(crate) fn parse_offset(page_token: &str) -> u64 {
    page_token.trim().parse::<u64>().unwrap_or(0)
}

pub(crate) fn next_page_token(offset: u64, limit: u32, returned: usize) -> String {
    if returned as u32 >= limit {
        (offset + returned as u64).to_string()
    } else {
        String::new()
    }
}
