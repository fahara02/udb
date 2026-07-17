//! Typed `Status` constructors and request validators for the native
//! `EmbeddingService`: the capability / policy / schema envelopes, the
//! field-violation helpers, the register-source / report-embedding validators,
//! the reported-vector shape gate, and the fail-closed source-tenant-column gate.
//! Extracted verbatim; each returns the same typed `ErrorDetail` trailer as the
//! former god file.

use tonic::Status;

pub(crate) fn embedding_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "embedding",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn embedding_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

pub(crate) fn embedding_source_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "embedding",
        operation,
        "embedding_source_not_found",
        "embedding source not found",
    )
}

pub(crate) fn embedding_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    embedding_field_violation(field, description, message)
}

pub(crate) fn embedding_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn validate_register_source_required_fields(
    source_name: &str,
    source_message_type: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if source_name.trim().is_empty() {
        fields.push(("source_name", "must be a non-empty embedding source name"));
    }
    if source_message_type.trim().is_empty() {
        fields.push((
            "source_message_type",
            "must be a non-empty source message type",
        ));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_name and source_message_type are required",
            fields,
        ));
    }
    Ok(())
}

pub(crate) fn validate_report_embedding_required_fields(
    source_name: &str,
    row_pk: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if source_name.trim().is_empty() {
        fields.push(("source_name", "must be a non-empty embedding source name"));
    }
    if row_pk.trim().is_empty() {
        fields.push(("row_pk", "must be a non-empty source row primary key"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "source_name and row_pk are required",
            fields,
        ));
    }
    Ok(())
}

/// Reported-vector shape gate: an empty vector, or a `dims` claim that
/// contradicts the actual vector length, is rejected with an
/// `invalid_argument` field violation BEFORE any admission, store read, or
/// vector-seam call — a mis-declared dimension would otherwise poison the
/// collection's dimension at ensure-time. Pure — unit-tested.
pub(crate) fn validate_reported_vector(dims: i32, vector: &[f32]) -> Result<(), Status> {
    if vector.is_empty() {
        return Err(embedding_required_field(
            "vector",
            "must contain at least one embedding dimension",
            "vector is required",
        ));
    }
    if dims > 0 && dims as usize != vector.len() {
        return Err(embedding_field_violation(
            "dims",
            "must equal the reported vector length when set",
            format!(
                "dims ({dims}) does not match the reported vector length ({})",
                vector.len()
            ),
        ));
    }
    Ok(())
}

/// The fail-closed source-tenant-column gate (mirrors `search_service`). An
/// EmbeddingSource MUST be scoped by the source table's tenant column; if the
/// shared catalog resolver returns none, the source is not tenant-isolated and we
/// refuse to register it (a reported vector / Retrieve would otherwise have no
/// tenant predicate). Pure — unit-tested without a manifest.
pub(crate) fn require_source_tenant_column(
    resolved: Option<String>,
    message_type: &str,
) -> Result<String, Status> {
    match resolved.map(|column| column.trim().to_string()) {
        Some(column) if !column.is_empty() => Ok(column),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "source entity '{message_type}' has no resolvable tenant column; refusing to register a \
                 tenant-scoped embedding source over it (fail closed)"
            ),
            [(
                "source_message_type",
                "must resolve to a tenant-scoped source entity",
            )],
        )),
    }
}
