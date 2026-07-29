//! Typed `Status` constructors and field-violation helpers for the native
//! `VaultService`: the capability / schema / internal envelopes, the seal-gate and
//! master-key refusals, the dynamic DB-credential capability refusals, the
//! destructive-confirmation guard, and the required-field builders. Extracted
//! verbatim from the former god file — each returns the same typed `ErrorDetail`
//! trailer as before.

use tonic::Status;

pub(crate) fn vault_capability_status(
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "vault",
        operation,
        capability_required,
        message,
    )
}

pub(crate) fn vault_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("vault", operation, message)
}

pub(crate) fn vault_master_key_unavailable_status(message: impl Into<String>) -> Status {
    vault_capability_status("seal_gate", "vault_master_key", message)
}

pub(crate) fn vault_master_key_operation_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    vault_capability_status(operation, "vault_master_key", message)
}

pub(crate) fn vault_db_credentials_config_status(message: impl Into<String>) -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "database_credentials_config",
        message,
    )
}

pub(crate) fn vault_db_native_store_required_status() -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_native_store",
        "vault dynamic database credentials require a Postgres native store",
    )
}

pub(crate) fn vault_db_role_creation_status(message: impl Into<String>) -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_role_management",
        message,
    )
}

pub(crate) fn vault_schema_not_found_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "vault",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn vault_schema_already_exists_status(
    operation: &'static str,
    schema_code: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "vault",
        operation,
        schema_code,
        message,
    )
}

pub(crate) fn vault_confirmation_token_required_status() -> Status {
    crate::runtime::executor_utils::failed_precondition_fields(
        "DestroySecret crypto-shreds the secret; confirmation_token is required",
        [(
            "confirmation_token",
            "must be provided to confirm destructive secret shredding",
        )],
    )
}

/// The confirmation token was provided but did not match the `secret_path`.
/// DestroySecret is an irreversible crypto-shred, so the caller must name the
/// exact path being destroyed (Vault's own UX) — a non-empty token is not enough.
pub(crate) fn vault_confirmation_token_mismatch_status() -> Status {
    vault_field_violation(
        "confirmation_token",
        "must exactly equal the secret_path being destroyed",
        "DestroySecret confirmation_token must match the secret_path to authorize the irreversible crypto-shred",
    )
}

pub(crate) fn vault_field_violation<F, D, M>(field: F, description: D, message: M) -> Status
where
    F: Into<String>,
    D: Into<String>,
    M: Into<String>,
{
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

pub(crate) fn vault_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    vault_field_violation(field, description, message)
}

pub(crate) fn vault_required_secret_path() -> Status {
    vault_required_field(
        "secret_path",
        "must be a non-empty vault secret path",
        "secret_path is required",
    )
}

pub(crate) fn vault_required_key_name() -> Status {
    vault_required_field(
        "key_name",
        "must be a non-empty vault transit key name",
        "key_name is required",
    )
}
