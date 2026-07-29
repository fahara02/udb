//! Neutral-IR read / record / conflict builders for the native `VaultService`:
//! the per-path secret version scan, the list projection, the transit-key scan,
//! and the `LogicalRecord` shapes for secrets, transit keys, and dynamic
//! DB-credential leases. Extracted verbatim — the `LogicalRead`/`LogicalRecord`
//! shapes and conflict targets are byte-for-byte identical to the former god file.

use chrono::{DateTime, Utc};

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};

use super::config::{VAULT_SECRET_MSG, VAULT_TRANSIT_KEY_MSG, max_versions_scan};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn secret_path_read(tenant_id: &str, secret_path: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_SECRET_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "secret_path".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(secret_path),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "secret_id".to_string(),
            "version".to_string(),
            "ciphertext".to_string(),
            "data_key_wrapped".to_string(),
            "state".to_string(),
            "metadata_json".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(max_versions_scan())),
    }
}

pub(crate) fn secret_list_read(tenant_id: &str, prefix: &str) -> LogicalRead {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if !prefix.trim().is_empty() {
        filters.push(LogicalFilter::Comparison {
            field: "secret_path".to_string(),
            op: ComparisonOp::StartsWith,
            value: logical_string(prefix.trim()),
        });
    }
    LogicalRead {
        message_type: VAULT_SECRET_MSG.to_string(),
        filter: Some(LogicalFilter::And(filters)),
        projection: Some(LogicalProjection::fields([
            "secret_path".to_string(),
            "version".to_string(),
            "state".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(max_versions_scan())),
    }
}

pub(crate) fn transit_key_read(tenant_id: &str, key_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_TRANSIT_KEY_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "key_name".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(key_name),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "key_id".to_string(),
            "version".to_string(),
            "algorithm".to_string(),
            "wrapped_key_material".to_string(),
            "state".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(max_versions_scan())),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn secret_record(
    secret_id: &str,
    tenant_id: &str,
    secret_path: &str,
    version: i64,
    ciphertext: &str,
    data_key_wrapped: &str,
    state: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("secret_id".to_string(), logical_string(secret_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("secret_path".to_string(), logical_string(secret_path));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("ciphertext".to_string(), logical_string(ciphertext));
    record.insert(
        "data_key_wrapped".to_string(),
        logical_string(data_key_wrapped),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

pub(crate) fn secret_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "ciphertext".to_string(),
        "data_key_wrapped".to_string(),
        "state".to_string(),
        "metadata_json".to_string(),
    ])
}

pub(crate) fn transit_key_record(
    key_id: &str,
    tenant_id: &str,
    key_name: &str,
    version: i64,
    algorithm: &str,
    wrapped_key_material: &str,
    state: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("key_id".to_string(), logical_string(key_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("key_name".to_string(), logical_string(key_name));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("algorithm".to_string(), logical_string(algorithm));
    record.insert(
        "wrapped_key_material".to_string(),
        logical_string(wrapped_key_material),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string("{}"));
    record
}

pub(crate) fn transit_key_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "algorithm".to_string(),
        "wrapped_key_material".to_string(),
        "state".to_string(),
    ])
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn db_credential_lease_record(
    lease_id: &str,
    tenant_id: &str,
    role_name: &str,
    username: &str,
    parent_role: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("lease_id".to_string(), logical_string(lease_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("role_name".to_string(), logical_string(role_name));
    record.insert("username".to_string(), logical_string(username));
    record.insert("parent_role".to_string(), logical_string(parent_role));
    record.insert("backend".to_string(), logical_string("postgres"));
    record.insert("issued_at".to_string(), LogicalValue::Timestamp(issued_at));
    record.insert(
        "expires_at".to_string(),
        LogicalValue::Timestamp(expires_at),
    );
    record.insert("state".to_string(), logical_string("ACTIVE"));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}
