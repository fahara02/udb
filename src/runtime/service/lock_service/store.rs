//! Neutral-IR query/record builders and the manifest model for the native
//! `LockService`. Extracted verbatim — the `LogicalRead`/`LogicalRecord` shapes,
//! the tenant-scoped filter, the quota-count read, and the inventory projection
//! are byte-for-byte identical to the former god file.

use chrono::{DateTime, Utc};

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{LOCK_MSG, STATUS_HELD};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn lock_filter(
    tenant_id: &str,
    lock_name: Option<&str>,
    status: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(lock_name) = lock_name {
        filters.push(LogicalFilter::Comparison {
            field: "lock_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(lock_name),
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

pub(crate) fn lock_read_by_name(tenant_id: &str, lock_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: LOCK_MSG.to_string(),
        filter: Some(lock_filter(tenant_id, Some(lock_name), None)),
        projection: Some(LogicalProjection::fields([
            "lock_id".to_string(),
            "owner_id".to_string(),
            "fencing_token".to_string(),
            "status".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

/// Quota-count read: HELD rows whose lease has NOT lapsed. A lock past its
/// `expires_at` no longer excludes other holders (the advisory lease is gone),
/// so it must not exhaust `MAX_ACTIVE_LOCKS_PER_TENANT` between reaper sweeps
/// (16.5.1). The mediated PG path binds `LogicalValue::Timestamp` with a
/// `$N::TIMESTAMPTZ` cast, so the time comparison is expressible in the filter —
/// no in-memory post-filtering needed.
pub(crate) fn held_locks_read(tenant_id: &str, limit: u32, now: DateTime<Utc>) -> LogicalRead {
    LogicalRead {
        message_type: LOCK_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            lock_filter(tenant_id, None, Some(STATUS_HELD)),
            LogicalFilter::Comparison {
                field: "expires_at".to_string(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Timestamp(now),
            },
        ])),
        projection: Some(LogicalProjection::fields(["lock_id".to_string()])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(limit)),
    }
}

/// Full-projection tenant-scoped read for the inventory RPCs (GetLock/ListLocks).
/// `lock_name` narrows to one row; `status` narrows by operational state; newest
/// acquisitions first. `offset`/`limit` drive opaque pagination for ListLocks
/// (GetLock passes offset 0, limit 1).
pub(crate) fn lock_inventory_read(
    tenant_id: &str,
    lock_name: Option<&str>,
    status: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    let mut pagination = LogicalPagination::limit(limit);
    pagination.offset = (offset > 0).then_some(offset);
    LogicalRead {
        message_type: LOCK_MSG.to_string(),
        filter: Some(lock_filter(tenant_id, lock_name, status)),
        projection: Some(LogicalProjection::fields([
            "lock_id".to_string(),
            "tenant_id".to_string(),
            "lock_name".to_string(),
            "owner_id".to_string(),
            "fencing_token".to_string(),
            "lease_ttl_seconds".to_string(),
            "status".to_string(),
            "acquired_at".to_string(),
            "expires_at".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: vec![crate::ir::LogicalSort {
            field: "acquired_at".to_string(),
            direction: crate::ir::SortDirection::Desc,
            nulls: crate::ir::NullOrder::Default,
        }],
        include: Vec::new(),
        pagination: Some(pagination),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn lock_record(
    lock_id: &str,
    tenant_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
    ttl_seconds: i64,
    status: &str,
    acquired_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("lock_id".to_string(), logical_string(lock_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("lock_name".to_string(), logical_string(lock_name));
    record.insert("owner_id".to_string(), logical_string(owner_id));
    record.insert(
        "fencing_token".to_string(),
        LogicalValue::Int(fencing_token),
    );
    record.insert(
        "lease_ttl_seconds".to_string(),
        LogicalValue::Int(ttl_seconds),
    );
    record.insert("status".to_string(), logical_string(status));
    record.insert(
        "acquired_at".to_string(),
        LogicalValue::Timestamp(acquired_at),
    );
    record.insert(
        "expires_at".to_string(),
        LogicalValue::Timestamp(expires_at),
    );
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// The mutable columns an upsert may overwrite on re-grant/renew/release. The
/// conflict target is the message primary key (`lock_id`); the same `lock_id` is
/// reused for an existing (tenant, lock_name) row so a re-acquire never violates
/// the unique index.
pub(crate) fn lock_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "owner_id".to_string(),
        "fencing_token".to_string(),
        "lease_ttl_seconds".to_string(),
        "status".to_string(),
        "acquired_at".to_string(),
        "expires_at".to_string(),
        "metadata_json".to_string(),
    ])
}

/// Manifest-derived model for the durable lock table, so the reaper SQL below
/// follows the same single-source-of-truth rule as the scheduler tick (no
/// hand-maintained schema copies).
pub(crate) fn lock_model() -> NativeModel {
    native_model(
        LOCK_MSG,
        &[
            "lock_id",
            "tenant_id",
            "lock_name",
            "owner_id",
            "fencing_token",
            "status",
            "expires_at",
        ],
    )
}
