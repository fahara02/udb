//! The stable machine reasons a refusal carries in `ErrorDetail.reason`.
//!
//! Every reason the broker sends is declared here once, with its kind and the
//! fix hint a caller sees. `docs/error-reasons.md` is rendered from this table
//! (a test fails when the two drift), so SDKs and users branch on a documented,
//! never-renamed code instead of the message text.

use crate::proto::{ErrorDetail, ErrorFieldViolation, ErrorKind};

/// One stable refusal reason.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ErrorReason {
    /// `UDB_` + upper snake case. Never renamed once shipped.
    pub(crate) code: &'static str,
    pub(crate) kind: ErrorKind,
    /// The gRPC status code the reason travels with.
    pub(crate) status: tonic::Code,
    /// When the broker sends it.
    pub(crate) summary: &'static str,
    /// What the caller should do.
    pub(crate) fix_hint: &'static str,
}

macro_rules! reasons {
    ($($name:ident = $code:literal, $kind:ident, $status:ident, $summary:literal, $hint:literal;)*) => {
        $(
            pub(crate) const $name: ErrorReason = ErrorReason {
                code: $code,
                kind: ErrorKind::$kind,
                status: tonic::Code::$status,
                summary: $summary,
                fix_hint: $hint,
            };
        )*
        /// Every declared reason, in documentation order.
        pub(crate) const ALL: &[ErrorReason] = &[$($name),*];
    };
}

reasons! {
    CAS_CONFLICT = "UDB_CAS_CONFLICT", Conflict, FailedPrecondition,
        "A compare-and-swap precondition did not hold: the row changed since it was read.",
        "Read the row again, reapply your change to the fresh value, and retry.";
    CAS_ROW_MISSING = "UDB_CAS_ROW_MISSING", Conflict, FailedPrecondition,
        "A compare-and-swap named a row that does not exist (or is not visible to the caller).",
        "Create the row with a plain write, or check the key.";
    CAS_KEY_NOT_PK = "UDB_CAS_KEY_NOT_PK", Validation, FailedPrecondition,
        "A conditional write's filter is not an equality on every primary-key column or on every column of a declared unique key.",
        "Address the row by its full primary key or by one complete unique key; the generated clients do this from the entity's Key type.";
    REVISION_CONFLICT = "UDB_REVISION_CONFLICT", Conflict, FailedPrecondition,
        "An expected_revision did not match the row's current revision.",
        "Read the row again to get its current revision and retry.";
    IDEMPOTENCY_REUSE = "UDB_IDEMPOTENCY_REUSE", Conflict, FailedPrecondition,
        "An idempotency key was already used with different authoritative inputs; the new write was refused.",
        "Reuse a key only to retry identical inputs; use a new key for a different write.";
    NO_ROWS_AFFECTED = "UDB_NO_ROWS_AFFECTED", NotFound, NotFound,
        "require_affected was set and the write matched a different number of rows; nothing was changed.",
        "Check the filter or key; read the row first if it may have been deleted.";
    UNIQUE_VIOLATION = "UDB_UNIQUE_VIOLATION", Unique, AlreadyExists,
        "The write would duplicate a value a unique constraint covers; ErrorDetail.constraint names it.",
        "Use the existing row (read it by that unique key) or change the conflicting value.";
    NOT_NULL_VIOLATION = "UDB_NOT_NULL_VIOLATION", NotNull, InvalidArgument,
        "A NOT NULL column received no value; ErrorDetail.column names it.",
        "Send a value for the column.";
    UPSERT_INCOMPLETE_ROW = "UDB_UPSERT_INCOMPLETE_ROW", NotNull, InvalidArgument,
        "An Upsert inserts a full row, and the record left out a NOT NULL column that has no default.",
        "Send the whole row to Upsert, or use Update (the generated clients' Patch) to change only some columns of an existing row.";
    FOREIGN_KEY_VIOLATION = "UDB_FOREIGN_KEY_VIOLATION", ForeignKey, FailedPrecondition,
        "The write references a row that does not exist, or deletes a row others still reference.",
        "Create the referenced row first, or remove the references before deleting.";
    TYPE_MISMATCH = "UDB_TYPE_MISMATCH", Validation, InvalidArgument,
        "A value does not fit its column's type; ErrorDetail.column names it.",
        "Send the value in the column's wire form (see docs/wire-types.md).";
    DECODE_FAILED = "UDB_DECODE_FAILED", Internal, Internal,
        "A stored value could not be decoded into its wire form; ErrorDetail.column names it.",
        "Report this with the column's SQL type; it is a broker defect, not a request error.";
    UNSUPPORTED_FILTER_OPERATOR = "UDB_UNSUPPORTED_FILTER_OPERATOR", Validation, InvalidArgument,
        "A filter used an operator the broker does not support.",
        "Use one of $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $between, $like, $ilike, $is_null, $not_null, $not, $and, $or.";
    NULL_COMPARISON = "UDB_NULL_COMPARISON", Validation, InvalidArgument,
        "A filter compared a column with NULL using equality, which matches no row.",
        "Use {\"$is_null\": true} (or $not_null) to match missing values.";
    TENANT_MISMATCH = "UDB_TENANT_MISMATCH", Permission, PermissionDenied,
        "The request named a tenant other than the caller's verified tenant.",
        "Leave the tenant out (the broker uses the caller's tenant) or authenticate as the right tenant.";
    TABLE_NOT_TENANT_SCOPED = "UDB_TABLE_NOT_TENANT_SCOPED", Validation, InvalidArgument,
        "A filter named a tenant column on a table that has none.",
        "Drop the tenant filter for this entity; it is not tenant-scoped.";
    SCOPE_MISSING = "UDB_SCOPE_MISSING", Permission, PermissionDenied,
        "The caller lacks a scope the operation needs; ErrorDetail.missing names it.",
        "Add the scope to the service account's grant and request it when connecting.";
    POLICY_DENIED = "UDB_POLICY_DENIED", Permission, PermissionDenied,
        "No policy rule allows this action for the caller; ErrorDetail.missing names the rule that would.",
        "Add the rule (`udb authz explain` shows the closest candidate and the attribute that failed).";
    REDACTED_VALUE_WRITE = "UDB_REDACTED_VALUE_WRITE", Redacted, InvalidArgument,
        "A write tried to store the redaction placeholder in a protected column, which would erase the real value.",
        "Read the row with the PII read scope before writing it back, or leave the column out of the write.";
    RATE_LIMITED = "UDB_RATE_LIMITED", RateLimited, ResourceExhausted,
        "A rate limit bucket is exhausted; ErrorDetail.missing describes the bucket and retry_after_ms when to retry.",
        "Retry after retry_after_ms, batch lookups with $in, or raise the bucket's limit.";
    UNKNOWN_MESSAGE_TYPE = "UDB_UNKNOWN_MESSAGE_TYPE", Schema, InvalidArgument,
        "The message type is not in the project's active catalog.",
        "Stage and activate a catalog that contains the entity (`udb catalog stage` / `udb catalog activate`).";
    GRANT_OWNED_BY_OTHER = "UDB_GRANT_OWNED_BY_OTHER", Conflict, FailedPrecondition,
        "The service identity is already granted to another service account; a grant is never moved implicitly.",
        "Use a distinct service identity, or move the grant on purpose with `udb auth grant transfer` (or `udb identity apply --allow-transfer`).";
    ENVELOPE_VERSION_UNSUPPORTED = "UDB_ENVELOPE_VERSION_UNSUPPORTED", Schema, FailedPrecondition,
        "An event's envelope_version is newer than the consumer understands.",
        "Upgrade the consumer's SDK to the broker's version.";
    POLICY_WRONG_SURFACE = "UDB_POLICY_WRONG_SURFACE", Validation, FailedPrecondition,
        "DataBroker.PutPolicy writes the legacy ABAC table, which does not authorize requests.",
        "Use AuthzService.PutAuthzPolicy or udb policy apply; UDB_ALLOW_LEGACY_PUT_POLICY=true is a temporary migration override.";
}

/// Builds a refusal status carrying a typed [`ErrorDetail`] with `reason`.
#[derive(Debug, Clone)]
pub(crate) struct Refusal {
    reason: ErrorReason,
    message: String,
    detail: ErrorDetail,
}

impl Refusal {
    pub(crate) fn new(reason: ErrorReason, message: impl Into<String>) -> Self {
        Self {
            reason,
            message: message.into(),
            detail: ErrorDetail {
                kind: reason.kind as i32,
                reason: reason.code.to_string(),
                fix_hint: reason.fix_hint.to_string(),
                ..ErrorDetail::default()
            },
        }
    }

    pub(crate) fn column(mut self, column: impl Into<String>) -> Self {
        self.detail.column = column.into();
        self
    }

    pub(crate) fn constraint(mut self, constraint: impl Into<String>) -> Self {
        self.detail.constraint = constraint.into();
        self
    }

    pub(crate) fn missing(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.detail.missing.insert(key.into(), value.into());
        self
    }

    pub(crate) fn field(
        mut self,
        field: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.detail.field_violations.push(ErrorFieldViolation {
            field: field.into(),
            description: description.into(),
        });
        self
    }

    pub(crate) fn retry_after_ms(mut self, retry_after_ms: i64) -> Self {
        self.detail.retryable = true;
        self.detail.retry_after_ms = retry_after_ms;
        self
    }

    pub(crate) fn into_status(self) -> tonic::Status {
        crate::runtime::executor_utils::status_with_typed_detail(
            self.reason.status,
            self.message,
            self.detail,
        )
    }
}

impl From<Refusal> for tonic::Status {
    fn from(refusal: Refusal) -> Self {
        refusal.into_status()
    }
}

/// Adds `reason` (with its fix hint, and the column / constraint when known) to
/// an existing refusal, keeping its code, message, kind and every other detail
/// field: callers that branch on today's kinds keep working while the reason
/// becomes the stable contract. A status without a typed detail gets one built
/// from the reason.
pub(crate) fn annotate(
    status: tonic::Status,
    reason: ErrorReason,
    column: Option<&str>,
    constraint: Option<&str>,
) -> tonic::Status {
    annotate_with(status, reason, column, constraint, &[])
}

/// [`annotate`] that also records what the caller is missing
/// (`ErrorDetail.missing`), for example `[("scope", "udb:pii:read")]`.
pub(crate) fn annotate_with(
    status: tonic::Status,
    reason: ErrorReason,
    column: Option<&str>,
    constraint: Option<&str>,
    missing: &[(&str, &str)],
) -> tonic::Status {
    use prost::Message as _;
    let mut detail = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .and_then(|value| value.to_bytes().ok())
        .and_then(|raw| ErrorDetail::decode(raw.as_ref()).ok())
        .unwrap_or_else(|| ErrorDetail {
            kind: reason.kind as i32,
            ..ErrorDetail::default()
        });
    detail.reason = reason.code.to_string();
    detail.fix_hint = reason.fix_hint.to_string();
    if let Some(column) = column {
        detail.column = column.to_string();
    }
    if let Some(constraint) = constraint {
        detail.constraint = constraint.to_string();
    }
    for (key, value) in missing {
        detail
            .missing
            .insert((*key).to_string(), (*value).to_string());
    }
    crate::runtime::executor_utils::status_with_typed_detail(
        status.code(),
        status.message().to_string(),
        detail,
    )
}

/// The reason code a status carries, if any.
pub(crate) fn reason_of(status: &tonic::Status) -> Option<String> {
    use prost::Message as _;
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)?
        .to_bytes()
        .ok()?;
    let detail = ErrorDetail::decode(raw.as_ref()).ok()?;
    (!detail.reason.is_empty()).then_some(detail.reason)
}

/// `docs/error-reasons.md`, rendered from [`ALL`].
pub(crate) fn render_reasons_markdown() -> String {
    let mut out = String::from(
        "# Error reasons\n\n\
         <!-- Generated from src/runtime/error_reasons.rs; edit the table there. -->\n\n\
         Every refusal the broker sends carries a typed `ErrorDetail` (the \
         `udb-error-detail-bin` trailer). Its `reason` is one of the codes below. \
         A code is never renamed once shipped: branch on it, never on the message \
         text. The SDKs expose it as `Reason()` / `reason`.\n\n\
         | Reason | gRPC code | Kind | When | Fix |\n\
         |---|---|---|---|---|\n",
    );
    for reason in ALL {
        out.push_str(&format!(
            "| `{}` | {:?} | {} | {} | {} |\n",
            reason.code,
            reason.status,
            reason.kind.as_str_name(),
            reason.summary,
            reason.fix_hint
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reason_codes_are_unique_and_well_formed() {
        let mut seen = std::collections::BTreeSet::new();
        for reason in ALL {
            assert!(seen.insert(reason.code), "{} declared twice", reason.code);
            assert!(reason.code.starts_with("UDB_"), "{}", reason.code);
            assert!(
                reason
                    .code
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'),
                "{}",
                reason.code
            );
            assert!(reason.code.len() <= 64, "{}", reason.code);
            assert!(!reason.summary.is_empty() && !reason.fix_hint.is_empty());
        }
    }

    #[test]
    fn error_reasons_doc_is_current() {
        let committed = include_str!("../../docs/error-reasons.md").replace("\r\n", "\n");
        assert_eq!(
            committed,
            render_reasons_markdown(),
            "docs/error-reasons.md is stale; regenerate it from src/runtime/error_reasons.rs \
             (cargo test prints the expected content above)"
        );
    }

    #[test]
    fn a_refusal_carries_its_reason_kind_and_hint_through_the_sanitizer() {
        let status = Refusal::new(UNIQUE_VIOLATION, "resource already exists")
            .constraint("uq_active_patient_trip")
            .into_status();
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        let raw = status
            .metadata()
            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
            .unwrap()
            .to_bytes()
            .unwrap();
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(detail.reason, "UDB_UNIQUE_VIOLATION");
        assert_eq!(detail.kind, ErrorKind::Unique as i32);
        assert_eq!(detail.constraint, "uq_active_patient_trip");
        assert!(!detail.fix_hint.is_empty());
        assert_eq!(reason_of(&status).as_deref(), Some("UDB_UNIQUE_VIOLATION"));

        let scope = Refusal::new(SCOPE_MISSING, "missing scope")
            .missing("scope", "udb:pii:read")
            .into_status();
        let raw = scope
            .metadata()
            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
            .unwrap()
            .to_bytes()
            .unwrap();
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(
            detail.missing.get("scope").map(String::as_str),
            Some("udb:pii:read")
        );
    }
}
