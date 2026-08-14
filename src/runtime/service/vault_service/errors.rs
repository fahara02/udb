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

pub(crate) fn vault_db_credentials_authority_status() -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "tenant_bound_database_credential_authority",
        "dynamic database credential issuance is disabled: direct Postgres roles cannot enforce \
         immutable tenant/project scope; configure a trusted tenant-bound credential broker \
         before enabling this capability",
    )
}

pub(crate) fn vault_db_native_store_required_status() -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_native_store",
        "vault dynamic database credentials require a Postgres native store",
    )
}

/// The transactional multi-write handlers (`rotate_transit_key`,
/// `destroy_secret`, `list_secrets`) run a raw `sqlx` transaction directly on the
/// configured Postgres native store; without a wired pool they fail closed with a
/// typed capability refusal (rather than degrading to a non-atomic path).
pub(crate) fn vault_native_store_required_status(operation: &'static str) -> Status {
    vault_capability_status(
        operation,
        "postgres_native_store",
        "vault requires a Postgres native store for this operation",
    )
}

/// A PutSecret lost the compare-and-swap: a concurrent writer already committed
/// the next version of this `(tenant_id, secret_path)`, so the unique
/// `(tenant_id, secret_path, version)` index rejected this insert. Surface the
/// typed `ABORTED` the proto promises (retryable) instead of the raw Postgres
/// `23505`/`AlreadyExists` the executor would otherwise return.
pub(crate) fn vault_secret_cas_conflict_status(attempted_version: i64) -> Status {
    crate::runtime::executor_utils::retryable_aborted_status(
        "vault",
        "secret version CAS",
        0,
        format!(
            "CAS conflict: version {attempted_version} for this secret_path was written \
             concurrently; re-read the latest version and retry"
        ),
    )
}

/// Whether a native-write `Status` is the unique-constraint collision the CAS
/// insert (`ConflictStrategy::Error`) raises — the executor maps `23505` to
/// `AlreadyExists`. PutSecret only inserts a fresh version row, so the sole
/// possible unique collision is the `(tenant_id, secret_path, version)` tuple.
pub(crate) fn is_duplicate_conflict(status: &Status) -> bool {
    status.code() == tonic::Code::AlreadyExists
}

pub(crate) fn vault_db_role_creation_status(message: impl Into<String>) -> Status {
    vault_capability_status(
        "generate_database_credentials",
        "postgres_role_management",
        message,
    )
}

pub(crate) fn vault_db_idempotency_conflict_status() -> Status {
    crate::runtime::executor_utils::retryable_aborted_status(
        "vault",
        "generate_database_credentials",
        0,
        "idempotency_key was already used with different database-credential inputs",
    )
}

pub(crate) fn vault_db_lease_not_found_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "vault",
        "revoke_database_credentials",
        "vault_db_credential_lease_not_found",
        "database credential lease was not found in the authenticated tenant/project",
    )
}

pub(crate) fn vault_db_reconciliation_status(message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::retryable_aborted_status(
        "vault",
        "database_credential_reconciliation",
        250,
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
