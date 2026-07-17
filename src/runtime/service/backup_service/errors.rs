//! Typed `Status` constructors for the native `BackupService`: capability /
//! policy / schema-not-found / internal envelopes, the required-field validator,
//! and the restore-specific fresh-target guard plus its policy refusals.
//! Extracted verbatim; each returns the same typed `ErrorDetail` trailer as
//! before.

use tonic::Status;

pub(crate) fn required_backup_field(
    field: &'static str,
    value: &str,
    description: &'static str,
) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("{field} is required"),
            [(field, description)],
        ));
    }
    Ok(value.to_string())
}

pub(crate) fn backup_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "backup",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn backup_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

pub(crate) fn backup_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "backup",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn backup_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("backup", operation, message)
}

/// Refuse to restore over a live tenant: a target that already owns ANY row
/// fails closed. Pure — unit-tested without Postgres (the handler supplies the
/// probed row count).
pub(crate) fn ensure_target_is_fresh(existing_rows: u64) -> Result<(), Status> {
    if existing_rows > 0 {
        return Err(restore_target_not_fresh_status(existing_rows));
    }
    Ok(())
}

pub(crate) fn restore_target_not_fresh_status(existing_rows: u64) -> Status {
    backup_policy_status(
        "restore_tenant",
        "restore_target_not_fresh",
        format!(
            "restore target tenant already holds {existing_rows} row(s); restoring over a live tenant is refused — use a fresh tenant id"
        ),
    )
}

pub(crate) fn backup_run_missing_object_prefix_status() -> Status {
    backup_policy_status(
        "restore_tenant",
        "backup_run_missing_object_prefix",
        "backup run has no object prefix to restore from",
    )
}
