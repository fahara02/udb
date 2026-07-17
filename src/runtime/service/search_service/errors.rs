//! Typed `Status` constructors and request validators for the native
//! `SearchService`: the capability / schema envelopes, the field-violation
//! helpers, the create-index / search-query validators, and the fail-closed
//! source-tenant-column gate. Extracted verbatim; each returns the same typed
//! `ErrorDetail` trailer as the former god file.

use tonic::Status;

use crate::proto::udb::core::search::services::v1 as search_pb;

pub(crate) fn search_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "search",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn full_text_only_requires_mediated_ir_status() -> Status {
    search_capability_status(
        "full_text_only_search",
        "mediated_ir_full_text_path",
        "full-text-only search requires the mediated IR full-text path (P2.2, pending); \
         supply query_vector for vector or hybrid search",
    )
}

pub(crate) fn search_index_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "search",
        operation,
        "search_index_not_found",
        "search index not found",
    )
}

pub(crate) fn search_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    search_field_violation(field, description, message)
}

pub(crate) fn search_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn validate_create_index_required_fields(
    index_name: &str,
    source_message_type: &str,
) -> Result<(), Status> {
    let mut fields = Vec::new();
    if index_name.trim().is_empty() {
        fields.push(("index_name", "must be a non-empty search index name"));
    }
    if source_message_type.trim().is_empty() {
        fields.push((
            "source_message_type",
            "must be a non-empty source message type",
        ));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "index_name and source_message_type are required",
            fields,
        ));
    }
    Ok(())
}

pub(crate) fn validate_search_query(req: &search_pb::SearchRequest) -> Result<(), Status> {
    let has_vector = !req.query_vector.is_empty();
    let has_text = !req.query_text.trim().is_empty();
    if !has_vector && !has_text {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "search requires query_text and/or query_vector",
            [
                ("query_text", "must be non-empty when query_vector is empty"),
                ("query_vector", "must be non-empty when query_text is empty"),
            ],
        ));
    }
    Ok(())
}

/// The fail-closed source-tenant-column gate. A SearchIndex MUST be scoped by the
/// source table's tenant column; if the shared catalog resolver returns none, the
/// source entity is not tenant-isolated and we refuse to register an index over
/// it (a Search would otherwise have no tenant predicate to inject). Pure —
/// unit-tested without a manifest.
pub(crate) fn require_source_tenant_column(
    resolved: Option<String>,
    message_type: &str,
) -> Result<String, Status> {
    match resolved.map(|column| column.trim().to_string()) {
        Some(column) if !column.is_empty() => Ok(column),
        _ => Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!(
                "source entity '{message_type}' has no resolvable tenant column; refusing to register a \
                 tenant-scoped search index over it (fail closed)"
            ),
            [(
                "source_message_type",
                "must resolve to a tenant-scoped source entity",
            )],
        )),
    }
}
