#![allow(clippy::result_large_err)]

//! Pure utility helpers shared across executor modules and the core runtime.
//! Nothing in this module depends on `DataBrokerRuntime` directly.

#[cfg(feature = "s3")]
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    env,
    future::Future,
    sync::Mutex,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use prost_types::{ListValue, Struct, Value as ProstValue, value::Kind};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::broker::RequestContext;
use crate::generation::{CatalogManifest, ManifestColumn, ManifestMaterializedView, ManifestStore};
use crate::proto::{RecordSet, RequestContext as ProtoRequestContext, Row as ProtoRow};

// ── Object-store limits ──────────────────────────────────────────────────────

/// Generic inline object writes are capped; larger uploads should use
/// presigned/resumable upload flows rather than the generic RPC body.
pub(crate) const INLINE_OBJECT_LIMIT_BYTES: usize = 1_048_576;

/// Suggested client backoff (ms) advertised on transient backend failures
/// (HTTP 5xx / server errors) via [`retryable_status`]. SDKs read it from the
/// typed `ErrorDetail.retry_after_ms` to pace automatic retries.
pub(crate) const HTTP_RETRYABLE_BACKOFF_MS: i64 = 250;

pub(crate) fn executor_timeout_duration() -> Duration {
    let millis = env::var("UDB_EXECUTOR_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(30_000)
        .max(1);
    Duration::from_millis(millis)
}

// ── Log rate-limiting ─────────────────────────────────────────────────────────

/// Collapses a repeating log line so a persistent failure — e.g. an unreachable
/// Kafka broker hit on every CDC poll (200 events × ~4 polls/sec) — logs once,
/// then at most once per `cooldown` carrying the count of occurrences suppressed
/// in between, instead of flooding the log many times per second and burying
/// real errors. Cheap and `&self`: the lock/atomic are touched only on the
/// (cold) error path, so it can live behind a shared/`static` reference.
pub(crate) struct LogRateGate {
    cooldown: Duration,
    last_logged: Mutex<Option<Instant>>,
    suppressed: AtomicU64,
}

impl LogRateGate {
    /// `const` so a gate can be stored in a `static` without a lazy init.
    pub(crate) const fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last_logged: Mutex::new(None),
            suppressed: AtomicU64::new(0),
        }
    }

    /// Decide whether the caller should emit its log line now. Returns
    /// `Some(suppressed)` when it should — where `suppressed` is how many
    /// occurrences were collapsed since the last emit (0 on the first). Returns
    /// `None` while inside the cooldown; the caller stays silent and the
    /// occurrence is counted toward the next emit's suppressed total.
    pub(crate) fn check(&self) -> Option<u64> {
        let now = Instant::now();
        let mut last = self.last_logged.lock().unwrap_or_else(|e| e.into_inner());
        match *last {
            Some(prev) if now.duration_since(prev) < self.cooldown => {
                self.suppressed.fetch_add(1, Ordering::Relaxed);
                None
            }
            _ => {
                *last = Some(now);
                Some(self.suppressed.swap(0, Ordering::Relaxed))
            }
        }
    }
}

pub(crate) async fn with_executor_timeout<T, F>(
    backend: &str,
    operation: &str,
    future: F,
) -> Result<T, tonic::Status>
where
    F: Future<Output = Result<T, tonic::Status>>,
{
    tokio::time::timeout(executor_timeout_duration(), future)
        .await
        .map_err(|_| {
            deadline_exceeded_status(
                backend,
                format!("generic_{operation}"),
                HTTP_RETRYABLE_BACKOFF_MS,
                format!("{backend} generic {operation} exceeded UDB_EXECUTOR_TIMEOUT_MS"),
            )
        })?
}

pub(crate) fn reject_oversized_object(len: usize) -> Result<(), tonic::Status> {
    if len > INLINE_OBJECT_LIMIT_BYTES {
        Err(quota_refusal_status(
            "object",
            "inline_object_size",
            "generic object writes are limited to 1MB; use presigned upload for larger files",
        ))
    } else {
        Ok(())
    }
}

// ── Typed error details (A.3) ────────────────────────────────────────────────
//
// SDKs should branch on a machine-readable `ErrorDetail` instead of parsing the
// human-readable status message. We prost-encode an `ErrorDetail` and attach it
// to the `tonic::Status` as a binary trailer metadata value under
// `udb-error-detail-bin` (the `-bin` suffix tells gRPC the value is raw bytes,
// base64-transcoded on the wire). The status message stays the same human text,
// so existing string-based behavior is unaffected — the typed detail is purely
// additive.

/// Binary trailer metadata key carrying the prost-encoded `ErrorDetail`.
/// The `-bin` suffix is required by gRPC for binary metadata values.
pub(crate) const ERROR_DETAIL_METADATA_KEY: &str = "udb-error-detail-bin";
const MAX_ERROR_DETAIL_STRING_BYTES: usize = 8 * 1024;

/// Build a `tonic::Status` with the given code + human message and attach a
/// prost-encoded [`ErrorDetail`] under [`ERROR_DETAIL_METADATA_KEY`]. Clean
/// messages are preserved, while malformed public text is bounded and stripped
/// of control characters before SDKs or REST gateways can expose it.
fn status_with_error_detail(
    code: tonic::Code,
    message: impl Into<String>,
    detail: crate::proto::ErrorDetail,
) -> tonic::Status {
    use prost::Message as _;
    let message = bounded_error_detail_string(message.into(), "error");
    let detail = sanitized_error_detail(detail);
    let mut metadata = tonic::metadata::MetadataMap::new();
    let encoded = detail.encode_to_vec();
    let value = tonic::metadata::MetadataValue::from_bytes(&encoded);
    metadata.insert_bin(ERROR_DETAIL_METADATA_KEY, value);
    tonic::Status::with_metadata(code, message, metadata)
}

#[cfg(test)]
pub(crate) trait ErrorDetailRawBytes {
    fn raw_error_detail_bytes(&self) -> bytes::Bytes;
}

#[cfg(test)]
impl ErrorDetailRawBytes for tonic::metadata::MetadataValue<tonic::metadata::Binary> {
    fn raw_error_detail_bytes(&self) -> bytes::Bytes {
        self.to_bytes()
            .expect("typed detail metadata decodes to bytes")
    }
}

#[cfg(test)]
impl ErrorDetailRawBytes for bytes::Bytes {
    fn raw_error_detail_bytes(&self) -> bytes::Bytes {
        self.clone()
    }
}

#[cfg(test)]
impl<T> ErrorDetailRawBytes for &T
where
    T: ErrorDetailRawBytes + ?Sized,
{
    fn raw_error_detail_bytes(&self) -> bytes::Bytes {
        (*self).raw_error_detail_bytes()
    }
}

#[cfg(test)]
pub(crate) fn decode_error_detail_from_raw<T>(raw: &T) -> crate::proto::ErrorDetail
where
    T: ErrorDetailRawBytes + ?Sized,
{
    use prost::Message as _;
    let bytes = raw.raw_error_detail_bytes();
    crate::proto::ErrorDetail::decode(bytes.as_ref()).expect("typed detail decodes")
}

fn non_negative_retry_after_ms(retry_after_ms: i64) -> i64 {
    retry_after_ms.max(0)
}

fn bounded_error_detail_string(value: String, fallback: &str) -> String {
    let trimmed = value.trim();
    let mut sanitized = String::with_capacity(trimmed.len().min(MAX_ERROR_DETAIL_STRING_BYTES));
    for ch in trimmed.chars().filter(|ch| !ch.is_control()) {
        if sanitized.len() + ch.len_utf8() > MAX_ERROR_DETAIL_STRING_BYTES {
            break;
        }
        sanitized.push(ch);
    }
    if sanitized.is_empty() && !fallback.is_empty() {
        fallback.to_string()
    } else {
        sanitized
    }
}

fn bounded_error_detail_field_path(value: String) -> String {
    let field = bounded_error_detail_string(value, "field");
    if field.chars().any(char::is_whitespace) {
        "field".to_string()
    } else {
        field
    }
}

fn bounded_error_detail_token(value: String, fallback: &str) -> String {
    let value = bounded_error_detail_string(value, fallback);
    let token = value.split_whitespace().collect::<Vec<_>>().join("_");
    if token.is_empty() {
        fallback.to_string()
    } else {
        token
    }
}

fn sanitized_error_detail(mut detail: crate::proto::ErrorDetail) -> crate::proto::ErrorDetail {
    detail.backend = bounded_error_detail_string(detail.backend, "");
    detail.operation = bounded_error_detail_string(detail.operation, "");
    detail.capability_required = bounded_error_detail_string(detail.capability_required, "");
    detail.policy_decision_id = bounded_error_detail_string(detail.policy_decision_id, "");
    detail.correlation_id = bounded_error_detail_string(detail.correlation_id, "");
    detail.retry_after_ms = non_negative_retry_after_ms(detail.retry_after_ms);
    if !detail.retryable {
        detail.retry_after_ms = 0;
    }
    if detail.kind == crate::proto::ErrorKind::Validation as i32 {
        detail.backend.clear();
        detail.operation.clear();
        detail.capability_required.clear();
        detail.retryable = false;
        detail.retry_after_ms = 0;
        if detail.field_violations.is_empty() {
            detail
                .field_violations
                .push(crate::proto::ErrorFieldViolation {
                    field: "field".to_string(),
                    description: "invalid field".to_string(),
                });
        }
    }
    if detail.kind != crate::proto::ErrorKind::Validation as i32
        && detail.kind != crate::proto::ErrorKind::Quota as i32
        && detail.kind != crate::proto::ErrorKind::Retryable as i32
    {
        detail.retryable = false;
        detail.retry_after_ms = 0;
    }
    if detail.kind != crate::proto::ErrorKind::Validation as i32 {
        detail.field_violations.clear();
    }
    if detail.kind != crate::proto::ErrorKind::Policy as i32 {
        detail.policy_decision_id.clear();
    }
    if detail.kind != crate::proto::ErrorKind::Capability as i32
        && detail.kind != crate::proto::ErrorKind::Schema as i32
    {
        detail.capability_required.clear();
    }
    if detail.kind == crate::proto::ErrorKind::Quota as i32
        || detail.kind == crate::proto::ErrorKind::Retryable as i32
    {
        detail.backend = bounded_error_detail_token(std::mem::take(&mut detail.backend), "backend");
        detail.operation =
            bounded_error_detail_token(std::mem::take(&mut detail.operation), "operation");
    }
    for violation in &mut detail.field_violations {
        violation.field = bounded_error_detail_field_path(std::mem::take(&mut violation.field));
        violation.description = bounded_error_detail_string(
            std::mem::take(&mut violation.description),
            "invalid field",
        );
    }
    detail
}

/// Backend-capability refusal: the target backend does not support the
/// requested operation. `FailedPrecondition`, `kind = CAPABILITY`, not
/// retryable. The caller supplies the same human text it would otherwise pass
/// to `Status::failed_precondition` so messages are unchanged.
pub(crate) fn capability_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: capability_required.into(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Capability as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::FailedPrecondition, message, detail)
}

/// Authentication failure carrying a typed `ErrorDetail` (kind = POLICY) with a
/// stable machine reason in `policy_decision_id`, so a consumer can branch on
/// WHY auth failed — rotate a key vs re-login vs supply a missing claim — instead
/// of pattern-matching a prose message. `Unauthenticated`, never retryable.
///
/// This is the first error most integrations hit, and it previously shipped bare
/// (no `ErrorDetail` at all), contradicting the SDK guidance to read typed fields
/// rather than message strings (T-4).
pub(crate) fn unauthenticated_status(
    reason: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: String::new(),
        operation: String::new(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: reason.into(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Policy as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::Unauthenticated, message, detail)
}

/// Like [`capability_status`] but preserves a caller-chosen gRPC code. Some
/// dispatch paths select a context-specific code (Unimplemented vs Internal vs
/// NotFound) yet still need a typed capability `ErrorDetail` on the wire so SDKs
/// see `kind`/`capability_required` instead of a bare status (R-7).
pub(crate) fn capability_status_with_code(
    code: tonic::Code,
    backend: impl Into<String>,
    operation: impl Into<String>,
    capability_required: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: capability_required.into(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Capability as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(code, message, detail)
}

/// Policy-decision refusal: the request was syntactically valid, but a local or
/// external policy rejected it. `FailedPrecondition`, `kind = POLICY`, not
/// retryable. `policy_decision_id` is the stable machine-readable reason SDKs
/// can branch on; callers supply the same human message they would otherwise
/// pass to `Status::failed_precondition`.
pub(crate) fn policy_status(
    operation: impl Into<String>,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    policy_status_with_code(
        tonic::Code::FailedPrecondition,
        operation,
        policy_decision_id,
        message,
    )
}

pub(crate) fn policy_status_with_code(
    code: tonic::Code,
    operation: impl Into<String>,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: String::new(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: policy_decision_id.into(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Policy as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(code, message, detail)
}

/// Transient-failure refusal the caller may retry. `Unavailable`,
/// `kind = RETRYABLE`, `retryable = true`, with an optional suggested backoff.
pub(crate) fn retryable_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    retry_after_ms: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: true,
        retry_after_ms: non_negative_retry_after_ms(retry_after_ms),
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Retryable as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::Unavailable, message, detail)
}

/// Deadline/timeout refusal the caller may retry. Preserves
/// `DeadlineExceeded` while carrying the same typed retryable detail shape as
/// transient unavailable transport failures.
pub(crate) fn deadline_exceeded_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    retry_after_ms: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: true,
        retry_after_ms: non_negative_retry_after_ms(retry_after_ms),
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Retryable as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::DeadlineExceeded, message, detail)
}

/// Transient backend transport failure. Keeps the old public
/// "{backend} {operation} failed: ..." message shape while adding the typed
/// retryable detail SDKs use for bounded automatic retry decisions.
pub(crate) fn backend_transport_status(
    backend: impl std::fmt::Display,
    operation: impl std::fmt::Display,
    error: impl std::fmt::Display,
) -> tonic::Status {
    // Accept owned/borrowed runtime backend names (e.g. the dispatch target's
    // backend string), not just static literals: callers in the dispatch and
    // cache paths pass values that live only for the request.
    let backend = backend.to_string();
    let operation = operation.to_string();
    let message = format!("{backend} {operation} failed: {error}");
    retryable_status(backend, operation, HTTP_RETRYABLE_BACKOFF_MS, message)
}

/// Optimistic-concurrency or conflict retry. `Aborted`, `kind = RETRYABLE`,
/// `retryable = true`, with an optional suggested backoff.
pub(crate) fn retryable_aborted_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    retry_after_ms: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: true,
        retry_after_ms: non_negative_retry_after_ms(retry_after_ms),
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Retryable as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::Aborted, message, detail)
}

/// Quota/backpressure refusal the caller may retry after the server-advised
/// delay. `ResourceExhausted`, `kind = QUOTA`, `retryable = true`.
pub(crate) fn quota_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    retry_after_ms: i64,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: true,
        retry_after_ms: non_negative_retry_after_ms(retry_after_ms),
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Quota as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::ResourceExhausted, message, detail)
}

/// Hard quota/capacity refusal. `ResourceExhausted`, `kind = QUOTA`, not
/// retryable by itself because the caller must free capacity or change quota
/// before retrying.
pub(crate) fn quota_refusal_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Quota as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::ResourceExhausted, message, detail)
}

/// Validation refusal: one or more request fields are malformed or semantically
/// invalid. `InvalidArgument`, `kind = VALIDATION`, not retryable. The message
/// stays human-readable; SDKs read the structured field list from the typed
/// detail.
pub(crate) fn invalid_argument_fields<I, F, D>(
    message: impl Into<String>,
    fields: I,
) -> tonic::Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    let detail = crate::proto::ErrorDetail {
        backend: String::new(),
        operation: String::new(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Validation as i32,
        field_violations: fields
            .into_iter()
            .map(|(field, description)| crate::proto::ErrorFieldViolation {
                field: field.into(),
                description: description.into(),
            })
            .collect(),
    };
    status_with_error_detail(tonic::Code::InvalidArgument, message, detail)
}

/// Field-validation refusal for state-dependent checks that already use
/// `FailedPrecondition` on the public wire contract. This keeps the status code
/// stable while still giving SDKs structured `field_violations`.
pub(crate) fn failed_precondition_fields<I, F, D>(
    message: impl Into<String>,
    fields: I,
) -> tonic::Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    let detail = crate::proto::ErrorDetail {
        backend: String::new(),
        operation: String::new(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Validation as i32,
        field_violations: fields
            .into_iter()
            .map(|(field, description)| crate::proto::ErrorFieldViolation {
                field: field.into(),
                description: description.into(),
            })
            .collect(),
    };
    status_with_error_detail(tonic::Code::FailedPrecondition, message, detail)
}

fn executor_utils_invalid_field(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn referential_constraint_status() -> tonic::Status {
    schema_status(
        tonic::Code::FailedPrecondition,
        "database",
        "referential_constraint",
        "foreign_key_violation",
        "operation violates a referential constraint",
    )
}

/// Central classifier for `sqlx::Error` -> typed `tonic::Status` (bug_report.md
/// B1/B2/B3). When the failure is a database error carrying a SQLSTATE we
/// recognise, map it to the correct gRPC code with a clean, leak-free message;
/// otherwise fall back to `Internal` with the supplied `context` (the raw DB
/// string is intentionally NOT echoed for the recognised classes).
///
/// SQLSTATE mappings (PostgreSQL codes; the same standard codes are emitted by
/// the other SQL backends UDB drives):
///   - `23505` unique_violation        -> `AlreadyExists` ("resource already exists")
///   - `23502` not_null_violation       -> `InvalidArgument` ("required field <col> is missing")
///   - `22P02` invalid_text_representation / `22007` invalid_datetime_format
///                                      -> `InvalidArgument` ("invalid identifier format ...")
///   - `23503` foreign_key_violation    -> `FailedPrecondition`
///   - everything else                  -> `Internal` ("{context}") — raw DB text suppressed
pub(crate) fn sqlx_error_to_status(context: &str, err: &sqlx::Error) -> tonic::Status {
    if let Some(db) = err.as_database_error() {
        if let Some(code) = db.code() {
            match code.as_ref() {
                "23505" => {
                    // Unique violation. Map to AlreadyExists. We optionally name
                    // the constraint (schema metadata, not row data) but never the
                    // raw DB text/values.
                    let msg = match db.constraint() {
                        Some(name) => {
                            format!("resource already exists (conflicting constraint: {name})")
                        }
                        None => "resource already exists".to_string(),
                    };
                    return schema_status(
                        tonic::Code::AlreadyExists,
                        "database",
                        "unique_constraint",
                        "unique_violation",
                        msg,
                    );
                }
                "23502" => {
                    // NOT NULL violation. Name the column when the driver exposes
                    // it so the client knows which required field is missing.
                    let msg = match not_null_column(db.message()) {
                        Some(c) => format!("required field '{c}' is missing"),
                        None => "a required field is missing".to_string(),
                    };
                    let field =
                        not_null_column(db.message()).unwrap_or_else(|| "database".to_string());
                    return executor_utils_invalid_field(
                        field,
                        "required database field is missing",
                        msg,
                    );
                }
                "22P02" | "22007" => {
                    return executor_utils_invalid_field(
                        "parameter",
                        "must be a valid UUID/value for the target column",
                        "invalid identifier format (a parameter must be a valid UUID/value)",
                    );
                }
                "42804" => {
                    // datatype_mismatch — a bound value's type does not match the
                    // target column (e.g. a text param meeting a typed column with
                    // no cast). Surface a clear client-facing InvalidArgument naming
                    // the column when the driver message exposes it, instead of an
                    // opaque Internal. (bug_report 2026-07-16 minor-obs #5)
                    let column = not_null_column(db.message());
                    let msg = match &column {
                        Some(c) => format!("value type does not match column '{c}'"),
                        None => "a value's type does not match the target column type".to_string(),
                    };
                    return executor_utils_invalid_field(
                        column.unwrap_or_else(|| "parameter".to_string()),
                        "value type must match the target column type",
                        msg,
                    );
                }
                "42846" => {
                    // cannot_coerce — a value cannot be cast/coerced to the target
                    // column's type (e.g. a CAST the backend rejects, reached on a
                    // non-bind path where 42804's datatype_mismatch doesn't fire).
                    // Like 42804 this is a client-side type error, not an internal
                    // fault, so surface a typed InvalidArgument naming the column
                    // when the driver message exposes it, instead of an opaque
                    // Internal.
                    let column = not_null_column(db.message());
                    let msg = match &column {
                        Some(c) => format!("value cannot be coerced to column '{c}' type"),
                        None => "a value cannot be coerced to the target column type".to_string(),
                    };
                    return executor_utils_invalid_field(
                        column.unwrap_or_else(|| "parameter".to_string()),
                        "value must be coercible to the target column type",
                        msg,
                    );
                }
                "23503" => {
                    return referential_constraint_status();
                }
                // Connection loss surfaced AS a database error (Postgres class 08
                // "connection exception" and the 57P0x shutdown/cannot-connect
                // states). These are infrastructure outages, not logic errors, so
                // they must be a retryable UNAVAILABLE — otherwise a mid-request PG
                // restart reaches the client as an opaque Internal (or, on the auth
                // path, as a bogus Unauthenticated). See UDB-DB-READINESS-001.
                "08000" | "08003" | "08006" | "08001" | "08004" | "08007" | "08P01" | "57P01"
                | "57P02" | "57P03" => {
                    tracing::warn!(
                        context = %context,
                        sqlstate = %code.as_ref(),
                        "database connection lost; returning retryable Unavailable"
                    );
                    return retryable_status(
                        "database",
                        context,
                        HTTP_RETRYABLE_BACKOFF_MS,
                        format!("{context}: database temporarily unavailable"),
                    );
                }
                _ => {}
            }
        }
        // Unrecognised (or code-less) DATABASE error. Unlike the classes above —
        // which are normal application outcomes (already-exists / bad-arg) and
        // are deliberately quiet — this is a genuinely UNEXPECTED DB failure the
        // operator must be able to diagnose. The broker's own log is an
        // operator-only channel, so emit the FULL detail (SQLSTATE + driver
        // message + constraint + relation) at ERROR; without this an opaque
        // `INTERNAL` reaches the client with nothing in the logs to explain it.
        let sqlstate = db.code().map(|c| c.into_owned()).unwrap_or_default();
        let constraint = db.constraint().unwrap_or_default().to_string();
        let relation = db.table().unwrap_or_default().to_string();
        tracing::error!(
            context = %context,
            sqlstate = %sqlstate,
            constraint = %constraint,
            relation = %relation,
            db_message = %db.message(),
            "database operation failed with an unhandled error"
        );
        // Surface the schema-level metadata (SQLSTATE / constraint / relation —
        // never raw row VALUES) to the client so the error is no longer a dead
        // end. Opt in to ALSO echo the driver message with
        // UDB_VERBOSE_DB_ERRORS=true (off by default to preserve the no-leak
        // posture, since a few PG messages can quote row values).
        let mut detail = String::new();
        if !sqlstate.is_empty() {
            detail.push_str(&format!(" [SQLSTATE {sqlstate}]"));
        }
        if !constraint.is_empty() {
            detail.push_str(&format!(" (constraint: {constraint})"));
        } else if !relation.is_empty() {
            detail.push_str(&format!(" (relation: {relation})"));
        }
        if verbose_db_errors() {
            detail.push_str(&format!(": {}", db.message()));
        }
        return internal_status("database", context, format!("{context}{detail}"));
    }
    // Not a database error at all (pool acquire / IO / protocol). A transient
    // TRANSPORT failure (pool exhausted/closed, socket IO) is an outage the caller
    // may retry — surface it as a retryable UNAVAILABLE so a dropped pool never
    // masquerades as a server bug or a bad credential (UDB-DB-READINESS-001).
    if is_transient_transport_error(err) {
        tracing::warn!(
            context = %context,
            error = %err,
            "database transport unavailable (pool/IO); returning retryable Unavailable"
        );
        return retryable_status(
            "database",
            context,
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("{context}: database temporarily unavailable"),
        );
    }
    // A genuinely non-transient non-database error (decode/column/protocol bug).
    // Still log it — these were equally invisible before — and keep Internal.
    tracing::error!(context = %context, error = %err, "database call failed (non-database error)");
    internal_status("database", context, context.to_string())
}

/// Whether a `sqlx::Error` is a transient TRANSPORT failure — a connection-pool
/// or socket-level outage the caller may retry — as opposed to a logic/schema
/// error. Used by [`sqlx_error_to_status`] to route outages to a retryable
/// `UNAVAILABLE` instead of an opaque `Internal` (UDB-DB-READINESS-001). Kept
/// conservative: only pool exhaustion/closure and IO are classified transient;
/// ambiguous protocol/decode errors stay `Internal`.
fn is_transient_transport_error(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_)
    )
}

/// Whether to ALSO echo the raw driver message in client-facing `Internal` DB
/// errors (the server-side ERROR log in [`sqlx_error_to_status`] always carries
/// it). Off by default so row values a driver message might quote never leave
/// the server; set `UDB_VERBOSE_DB_ERRORS=true` to opt in. Resolved once.
fn verbose_db_errors() -> bool {
    static VERBOSE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *VERBOSE.get_or_init(|| {
        env::var("UDB_VERBOSE_DB_ERRORS")
            .ok()
            .map(|value| {
                let value = value.trim();
                value == "1"
                    || value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false)
    })
}

/// Re-contextualise a downstream `tonic::Status` by prefixing its message with
/// `context`, WITHOUT changing its code (bug_report.md B4). Use this at handler
/// wrap sites where the downstream value is already a typed `Status` — wrapping
/// it in `Status::internal(format!(...))` would clobber the correct code (e.g.
/// turn an `InvalidArgument`/`AlreadyExists` into `Internal`).
pub(crate) fn prefix_status(context: &str, status: tonic::Status) -> tonic::Status {
    tonic::Status::with_details_and_metadata(
        status.code(),
        format!("{context}: {}", status.message()),
        bytes::Bytes::copy_from_slice(status.details()),
        status.metadata().clone(),
    )
}

/// Prefix used to smuggle a typed gRPC code through the `Result<_, String>`
/// boundaries in the runtime `authn` store layer (whose trait methods return
/// `String`, not `Status`). The leaf sqlx write classifies its error via
/// [`sqlx_error_to_status`] then tags the resulting message so the RPC boundary
/// can reconstruct the correct code instead of forcing every store String into
/// `Status::internal`. Format: `"<TAG><code-number>:<message>"`. bug_report.md B2/B3.
const STATUS_TAG_PREFIX: &str = "\u{1}udb-status:";

/// Render a typed gRPC code + message as a tagged String (see
/// [`STATUS_TAG_PREFIX`]) so a `Result<_, String>` leaf can carry the code
/// across a String boundary, to be reconstructed by [`status_from_store_string`].
#[cfg(feature = "mongodb-native")]
pub(crate) fn tagged_status_string(code: tonic::Code, message: &str) -> String {
    format!("{STATUS_TAG_PREFIX}{}:{}", code as i32, message)
}

/// Classify a leaf `sqlx::Error` and render it as a tagged String carrying the
/// typed gRPC code, for the `String`-returning authn store layer. Recognised
/// SQLSTATEs become a tagged code; anything else is the plain `{context}: {err}`
/// (untagged → decodes to `Internal`).
pub(crate) fn sqlx_error_to_tagged_string(context: &str, err: &sqlx::Error) -> String {
    let status = sqlx_error_to_status(context, err);
    if status.code() == tonic::Code::Internal {
        // Preserve existing behaviour/diagnostics for unrecognised errors.
        return format!("{context}: {err}");
    }
    format!(
        "{STATUS_TAG_PREFIX}{}:{}",
        status.code() as i32,
        status.message()
    )
}

/// Render a downstream `tonic::Status` as a String for the `Result<_, String>`
/// authn resolver boundary, PRESERVING a retryable `Unavailable` code via the
/// [`STATUS_TAG_PREFIX`] tag so [`status_from_store_string`] can reconstruct it.
/// A durable-store OUTAGE (classified `Unavailable` by [`sqlx_error_to_status`])
/// must not be flattened into an opaque string the enforcement layer then treats
/// as a bad credential — the tag keeps the outage a retryable `Unavailable`
/// (UDB-DB-READINESS-001). Non-retryable statuses render as the plain
/// `{context}: {message}` (untagged → the enforcement layer fails closed as an
/// authentication denial exactly as before).
pub(crate) fn store_string_from_status(context: &str, status: &tonic::Status) -> String {
    if status.code() == tonic::Code::Unavailable {
        format!(
            "{STATUS_TAG_PREFIX}{}:{context}: {}",
            tonic::Code::Unavailable as i32,
            status.message()
        )
    } else {
        format!("{context}: {}", status.message())
    }
}

/// Decode a String produced by [`sqlx_error_to_tagged_string`] (or any other
/// store String) into a typed `Status`. Tagged strings restore their original
/// gRPC code + clean message; untagged strings become typed internal store
/// failures instead of losing ErrorDetail at the String boundary.
pub(crate) fn status_from_store_string(s: String) -> tonic::Status {
    if let Some(rest) = s.strip_prefix(STATUS_TAG_PREFIX) {
        if let Some((code_str, msg)) = rest.split_once(':') {
            if let Ok(code_num) = code_str.parse::<i32>() {
                let code = tonic::Code::from(code_num);
                return tagged_status_to_typed_status(code, msg);
            }
        }
    }
    internal_status("store", "string_status", s)
}

fn tagged_status_to_typed_status(code: tonic::Code, message: &str) -> tonic::Status {
    match code {
        tonic::Code::InvalidArgument => invalid_argument_fields(
            message,
            [("database", "database store rejected the request")],
        ),
        tonic::Code::FailedPrecondition
            if message == "operation violates a referential constraint" =>
        {
            referential_constraint_status()
        }
        tonic::Code::AlreadyExists => schema_status(
            tonic::Code::AlreadyExists,
            "database",
            "unique_constraint",
            "unique_violation",
            message,
        ),
        tonic::Code::Unavailable => {
            retryable_status("store", "tagged_status", HTTP_RETRYABLE_BACKOFF_MS, message)
        }
        _ => tonic::Status::new(code, message.to_string()),
    }
}

/// Extract the offending column name from a Postgres NOT NULL violation message
/// of the form: `null value in column "lookup_key" of relation "..." violates
/// not-null constraint`. Returns the bare column name when present.
fn not_null_column(message: &str) -> Option<String> {
    let after = message.split("column \"").nth(1)?;
    let name = after.split('"').next()?;
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

/// Schema/catalog compatibility refusal. The caller selects the public gRPC code
/// already used by that boundary; the detail carries `kind = SCHEMA` and stores a
/// stable schema code in `capability_required` for SDK branching.
pub(crate) fn schema_status(
    code: tonic::Code,
    backend: impl Into<String>,
    operation: impl Into<String>,
    schema_code: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: schema_code.into(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Schema as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(code, message, detail)
}

/// Unexpected internal failure with typed detail. Keeps the public gRPC code as
/// `Internal` while adding stable backend/operation identity for SDK and REST
/// diagnostics.
pub(crate) fn internal_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: String::new(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Internal as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::Internal, message, detail)
}

/// Compiler refusal: map a `CompileError::code()` token onto a typed
/// `ErrorDetail` (`kind = SCHEMA`, code carried in `capability_required`).
/// `InvalidArgument` matches the existing compile-error mapping. Additive — the
/// human message is preserved.
///
/// Currently exercised only by unit tests: the IR compiler's `CompileError` is
/// not yet surfaced through a request handler (the data plane compiles IR deeper
/// in the stack, where its error is propagated directly), so there is no
/// production call site to wire into. Scoped to `#[cfg(test)]` so it is honestly
/// not-dead in the shipped binary; un-gate it when an IR-compile step lands in a
/// handler that must map `CompileError` -> `tonic::Status` (mirror of the
/// already-wired `retryable_status`).
#[cfg(test)]
pub(crate) fn compile_error_status(
    backend: impl Into<String>,
    operation: impl Into<String>,
    compile_code: &str,
    message: impl Into<String>,
) -> tonic::Status {
    let detail = crate::proto::ErrorDetail {
        backend: backend.into(),
        operation: operation.into(),
        capability_required: compile_code.to_string(),
        retryable: false,
        retry_after_ms: 0,
        policy_decision_id: String::new(),
        correlation_id: String::new(),
        kind: crate::proto::ErrorKind::Schema as i32,
        field_violations: Vec::new(),
    };
    status_with_error_detail(tonic::Code::InvalidArgument, message, detail)
}

// ── Struct / prost ────────────────────────────────────────────────────────────

pub(crate) fn struct_to_json(value: &Struct) -> JsonValue {
    JsonValue::Object(
        value
            .fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

pub(crate) fn prost_value_to_json(value: &ProstValue) -> JsonValue {
    match &value.kind {
        Some(Kind::NullValue(_)) | None => JsonValue::Null,
        // google.protobuf.Value has only one numeric wire kind (f64). Recover
        // integer semantics when the value is exactly representable in JSON's
        // interoperable safe-integer range. Without this boundary repair an
        // SDK Update of an INTEGER column sends `0`, prost carries `0.0`, and
        // the relational binder correctly rejects it as a floating-point
        // value. Values outside the safe range remain floats and therefore
        // fail closed for integer columns; callers can use the documented
        // decimal-string form when they need the full i64 range.
        Some(Kind::NumberValue(value))
            if value.is_finite()
                && value.fract() == 0.0
                && value.abs() <= 9_007_199_254_740_991.0 =>
        {
            JsonValue::from(*value as i64)
        }
        Some(Kind::NumberValue(value)) => JsonValue::from(*value),
        Some(Kind::StringValue(value)) => JsonValue::String(value.clone()),
        Some(Kind::BoolValue(value)) => JsonValue::Bool(*value),
        Some(Kind::StructValue(value)) => struct_to_json(value),
        Some(Kind::ListValue(value)) => JsonValue::Array(
            value
                .values
                .iter()
                .map(prost_value_to_json)
                .collect::<Vec<_>>(),
        ),
    }
}

pub(crate) fn json_to_struct(value: &JsonValue) -> Option<Struct> {
    let JsonValue::Object(map) = value else {
        return None;
    };
    Some(Struct {
        fields: map
            .iter()
            .filter_map(|(key, value)| json_to_prost_value(value).map(|v| (key.clone(), v)))
            .collect(),
    })
}

pub(crate) fn json_to_prost_value(value: &JsonValue) -> Option<ProstValue> {
    Some(ProstValue {
        kind: Some(match value {
            JsonValue::Null => Kind::NullValue(0),
            JsonValue::Bool(value) => Kind::BoolValue(*value),
            JsonValue::Number(value) => Kind::NumberValue(value.as_f64()?),
            JsonValue::String(value) => Kind::StringValue(value.clone()),
            JsonValue::Array(items) => Kind::ListValue(ListValue {
                values: items.iter().filter_map(json_to_prost_value).collect(),
            }),
            JsonValue::Object(_) => Kind::StructValue(json_to_struct(value)?),
        }),
    })
}

// D.3: consuming (`_into_`) variants for callers that OWN the `JsonValue` and
// drop it afterwards. They MOVE keys/strings/arrays into the proto value instead
// of cloning every field — byte-for-byte equivalent output (proven by
// `conversion_tests`), fewer allocations (quantified by `hotpath_bench`'s
// move-vs-clone case).
//
// Wired into the qdrant response/upsert paths, which own each point JSON and
// discard it after building the proto point — so the payload Struct is MOVED out
// (`Value::take` + `json_into_struct`) instead of cloned. Keep the borrowing
// variants for inspect-only / reused sub-value paths (filters, the dual
// JSON+proto row build in `core::mod`).
pub(crate) fn json_into_struct(value: JsonValue) -> Option<Struct> {
    let JsonValue::Object(map) = value else {
        return None;
    };
    Some(Struct {
        fields: map
            .into_iter()
            .filter_map(|(key, value)| json_into_prost_value(value).map(|v| (key, v)))
            .collect(),
    })
}

pub(crate) fn json_into_prost_value(value: JsonValue) -> Option<ProstValue> {
    let kind = match value {
        JsonValue::Null => Kind::NullValue(0),
        JsonValue::Bool(value) => Kind::BoolValue(value),
        JsonValue::Number(value) => Kind::NumberValue(value.as_f64()?),
        JsonValue::String(value) => Kind::StringValue(value), // moved, not cloned
        JsonValue::Array(items) => Kind::ListValue(ListValue {
            values: items
                .into_iter()
                .filter_map(json_into_prost_value)
                .collect(),
        }),
        JsonValue::Object(map) => Kind::StructValue(Struct {
            fields: map
                .into_iter()
                .filter_map(|(key, value)| json_into_prost_value(value).map(|v| (key, v)))
                .collect(),
        }),
    };
    Some(ProstValue { kind: Some(kind) })
}

// ── RequestContext merging ────────────────────────────────────────────────────

pub(crate) fn merge_context(
    proto_context: Option<&ProtoRequestContext>,
    metadata_context: RequestContext,
) -> RequestContext {
    let Some(proto) = proto_context else {
        return metadata_context;
    };
    let proto_typed_consistency =
        crate::runtime::consistency::ConsistencyMode::from_proto_i32(proto.consistency_mode)
            .map(|mode| mode.as_str().to_string())
            .unwrap_or_default();
    let proto_consistency = first_non_empty(&proto.consistency, &proto_typed_consistency);
    let proto_typed_read_fence_json = proto
        .read_fence
        .as_ref()
        .map(crate::runtime::consistency::ReadFence::from_proto)
        .and_then(|fence| serde_json::to_string(&fence).ok())
        .unwrap_or_default();
    let proto_read_fence_json =
        first_non_empty(&proto.read_fence_json, &proto_typed_read_fence_json);
    RequestContext {
        // SECURITY: tenant_id is the authenticated isolation boundary — take it
        // from the request metadata context, NOT a caller-supplied per-op proto
        // context (which could target a foreign tenant). Mirrors project_id below.
        tenant_id: metadata_context.tenant_id,
        user_id: first_non_empty(&proto.user_id, &metadata_context.user_id),
        correlation_id: first_non_empty(&proto.correlation_id, &metadata_context.correlation_id),
        purpose: first_non_empty(&proto.purpose, &metadata_context.purpose),
        project_id: metadata_context.project_id,
        consistency: first_non_empty(&metadata_context.consistency, &proto_consistency),
        client_catalog_version: metadata_context.client_catalog_version,
        target_backend: first_non_empty(&proto.target_backend, &metadata_context.target_backend),
        target_instance: first_non_empty(&proto.target_instance, &metadata_context.target_instance),
        routing_policy: first_non_empty(&proto.routing_policy, &metadata_context.routing_policy),
        primary_read: proto.primary_read || metadata_context.primary_read,
        max_replica_lag_ms: if proto.max_replica_lag_ms > 0 {
            proto.max_replica_lag_ms
        } else {
            metadata_context.max_replica_lag_ms
        },
        eventual_consistency_allowed: proto.eventual_consistency_allowed
            || metadata_context.eventual_consistency_allowed,
        // R3.2: the read fence is a freshness/consistency control sourced from the
        // verified `x-udb-read-fence` metadata header. Make it metadata-wins (only
        // falling back to the per-op proto context when the header is absent) so it
        // matches the metadata-wins precedence of every other authority field above
        // (tenant_id / project_id / scopes / service_identity / decision_id). A
        // caller-supplied per-op proto fence can no longer override a fence the
        // transport already pinned; it is honored only when the header sets none.
        read_fence_json: first_non_empty(&metadata_context.read_fence_json, &proto_read_fence_json),
        // SECURITY (01.1.1.1): scopes are an authenticated capability — they drive
        // PII unmasking, the materialized-view admin gate, and seed the cache key.
        // Take them ONLY from the verified metadata/claim side; a caller-supplied
        // per-op proto context must never assert authoritative scopes (it could
        // self-grant `udb:pii:read`/`udb:admin`). Mirrors the tenant_id/project_id
        // metadata-wins rule above.
        scopes: metadata_context.scopes,
        // The broker stamps these from the verified SecurityContext / decision;
        // the wire `RequestContext` cannot assert them, so the metadata side wins.
        service_identity: metadata_context.service_identity,
        decision_id: metadata_context.decision_id,
    }
}

/// OBJ1/2/3 (tenant object isolation): physically namespace every object key by
/// the VERIFIED tenant so two tenants that PUT/GET/DELETE the same `bucket`+`key`
/// can never read, overwrite, or delete each other's objects. `context.tenant_id`
/// is the claim-derived isolation boundary — [`merge_context`] refuses a
/// body-supplied tenant — so the prefix is unforgeable. Applied identically on
/// every object entrypoint (PUT/GET/presign/multipart) so a tenant always maps to
/// the same physical key. A blank tenant (single-tenant / no-isolation build)
/// keeps the unscoped key: with no tenant boundary there is nothing to isolate,
/// and pre-existing unscoped objects stay reachable.
pub(crate) fn tenant_scoped_object_key(context: &RequestContext, object_key: &str) -> String {
    let tenant = context.tenant_id.trim();
    if tenant.is_empty() {
        object_key.to_string()
    } else {
        format!("__udb_t/{tenant}/{}", object_key.trim_start_matches('/'))
    }
}

pub(crate) fn first_non_empty(left: &str, right: &str) -> String {
    if left.trim().is_empty() {
        right.to_string()
    } else {
        left.to_string()
    }
}

// ── RecordSet helpers ─────────────────────────────────────────────────────────

pub(crate) fn reject_plan(errors: &[String]) -> Result<(), tonic::Status> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(executor_utils_invalid_field(
            "plan",
            "planner validation errors must be resolved",
            errors.join("; "),
        ))
    }
}

pub(crate) fn cached_record_set(records_json: Vec<Vec<u8>>) -> RecordSet {
    // Deserialise the cached JSON blobs back into proto rows (best-effort).
    let rows: Vec<ProtoRow> = records_json
        .iter()
        .filter_map(|blob| {
            let value: JsonValue = serde_json::from_slice(blob).ok()?;
            let fields = value
                .as_object()?
                .iter()
                .filter_map(|(key, val)| json_to_prost_value(val).map(|v| (key.clone(), v)))
                .collect();
            Some(ProtoRow { fields })
        })
        .collect();
    RecordSet {
        total_count: rows.len() as i32,
        rows,
        records_json,
        ..RecordSet::default()
    }
}

// ── RecordBatchV2 (A.4) ────────────────────────────────────────────────────────

fn proto_row_to_json(row: &ProtoRow) -> JsonValue {
    JsonValue::Object(
        row.fields
            .iter()
            .map(|(key, value)| (key.clone(), prost_value_to_json(value)))
            .collect(),
    )
}

fn decode_base64_cell(value: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let raw = value.strip_prefix("base64:")?;
    base64::engine::general_purpose::STANDARD.decode(raw).ok()
}

/// Infer a column's [`ColumnType`] and pack the per-row values into the matching
/// typed array, recording NULLs in the bitmap. Bytes columns are recognised via
/// the `base64:` cell prefix SQL executors emit (see [`base64_cell`]); columns
/// whose values mix scalar kinds — or contain arrays/objects — fall back to a
/// JSON-encoded string column so nothing is lost.
fn build_column_batch(name: &str, rows: &[JsonValue]) -> crate::proto::ColumnBatch {
    use crate::proto::{ColumnBatch, ColumnType};

    let cells: Vec<Option<&JsonValue>> = rows
        .iter()
        .map(|row| row.as_object().and_then(|obj| obj.get(name)))
        .collect();

    let (mut saw_bool, mut saw_int, mut saw_float) = (false, false, false);
    let (mut saw_str_plain, mut saw_str_b64, mut saw_nested, mut any_value) =
        (false, false, false, false);
    for cell in &cells {
        match cell {
            None | Some(JsonValue::Null) => {}
            Some(JsonValue::Bool(_)) => {
                saw_bool = true;
                any_value = true;
            }
            Some(JsonValue::Number(n)) => {
                any_value = true;
                if n.is_i64() || n.is_u64() {
                    saw_int = true;
                } else {
                    saw_float = true;
                }
            }
            Some(JsonValue::String(s)) => {
                any_value = true;
                if s.starts_with("base64:") {
                    saw_str_b64 = true;
                } else {
                    saw_str_plain = true;
                }
            }
            Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
                saw_nested = true;
                any_value = true;
            }
        }
    }

    let categories = [
        saw_bool,
        saw_int || saw_float,
        saw_str_plain || saw_str_b64,
        saw_nested,
    ]
    .iter()
    .filter(|present| **present)
    .count();

    let col_type = if !any_value {
        ColumnType::Null
    } else if categories > 1 {
        ColumnType::Json
    } else if saw_bool {
        ColumnType::Bool
    } else if saw_int && !saw_float {
        ColumnType::Int64
    } else if saw_int || saw_float {
        ColumnType::Double
    } else if saw_str_b64 && !saw_str_plain {
        ColumnType::Bytes
    } else if saw_str_plain || saw_str_b64 {
        ColumnType::String
    } else {
        ColumnType::Json
    };

    let mut column = ColumnBatch {
        name: name.to_string(),
        r#type: col_type as i32,
        nulls: Vec::with_capacity(cells.len()),
        ..ColumnBatch::default()
    };
    for cell in &cells {
        let is_null = matches!(cell, None | Some(JsonValue::Null));
        column.nulls.push(is_null);
        match col_type {
            ColumnType::Bool => column
                .bool_values
                .push(cell.and_then(|v| v.as_bool()).unwrap_or(false)),
            ColumnType::Int64 => column
                .int64_values
                .push(cell.and_then(|v| v.as_i64()).unwrap_or(0)),
            ColumnType::Double => column
                .double_values
                .push(cell.and_then(|v| v.as_f64()).unwrap_or(0.0)),
            ColumnType::String => column.string_values.push(match cell {
                Some(JsonValue::String(s)) => s.clone(),
                Some(other) if !other.is_null() => other.to_string(),
                _ => String::new(),
            }),
            ColumnType::Bytes => column.bytes_values.push(
                cell.and_then(|v| v.as_str())
                    .and_then(decode_base64_cell)
                    .unwrap_or_default(),
            ),
            ColumnType::Json => column.json_values.push(match cell {
                Some(v) if !v.is_null() => v.to_string(),
                _ => String::new(),
            }),
            ColumnType::Null | ColumnType::Unspecified => {}
        }
    }
    column
}

/// Build a columnar [`crate::proto::RecordBatchV2`] from JSON row objects. The
/// rows are the same masked/encrypted JSON the V1 `RecordSet` carries, so the
/// V2 encoding inherits the V1 serializer's PII masking by construction. Column
/// order is the first-seen key order across rows.
pub(crate) fn record_batch_v2_from_json_rows(
    rows: &[JsonValue],
    schema_version: &str,
    next_page_token: String,
    total_count: i32,
) -> crate::proto::RecordBatchV2 {
    let mut field_order: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for key in obj.keys() {
                if seen.insert(key.clone()) {
                    field_order.push(key.clone());
                }
            }
        }
    }
    let columns = field_order
        .iter()
        .map(|name| build_column_batch(name, rows))
        .collect::<Vec<_>>();
    crate::proto::RecordBatchV2 {
        columns,
        row_count: rows.len() as i32,
        schema_version: schema_version.to_string(),
        field_order,
        next_page_token,
        total_count,
    }
}

/// Convert a V1 [`RecordSet`] into the additive [`crate::proto::RecordBatchV2`]
/// columnar encoding, preferring the serialized `records_json` blobs (the masked
/// canonical form) and falling back to the proto `rows`.
pub(crate) fn record_batch_v2_from_record_set(
    set: &RecordSet,
    schema_version: &str,
) -> crate::proto::RecordBatchV2 {
    let rows: Vec<JsonValue> = if !set.records_json.is_empty() {
        set.records_json
            .iter()
            .filter_map(|blob| serde_json::from_slice(blob).ok())
            .collect()
    } else {
        set.rows.iter().map(proto_row_to_json).collect()
    };
    record_batch_v2_from_json_rows(
        &rows,
        schema_version,
        set.next_page_token.clone(),
        set.total_count,
    )
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

pub(crate) fn cache_key(
    kind: &str,
    message_type: &str,
    context: &RequestContext,
    manifest_checksum: &str,
    filter: &JsonValue,
    fields: &[String],
    limit: i32,
    sort_repr: &str,
) -> String {
    let mut scopes = context.scopes.clone();
    scopes.sort();
    let mut fields = fields.to_vec();
    fields.sort();
    // `limit` and `sort` are part of a read's identity: `LIMIT 10` and
    // `LIMIT 1000` (and ASC vs DESC) are different result sets and must not share
    // a cache entry (X-1 — the planner memo key `select_plan_cache_key` already
    // folds these in; the two caches previously disagreed on query identity).
    // Appended at the END so `cache_invalidation_pattern`'s trailing `:*` still
    // globs the whole tail.
    format!(
        "udb:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        kind,
        sanitize_cache_part(&context.tenant_id),
        sanitize_cache_part(&context.purpose),
        checksum_str(&scopes.join(",")),
        message_type,
        sanitize_cache_part(manifest_checksum),
        checksum_json(filter),
        checksum_str(&fields.join(",")),
        limit,
        checksum_str(sort_repr),
    )
}

pub(crate) fn cache_invalidation_pattern(kind: &str, message_type: &str) -> String {
    format!("udb:{}:*:*:*:{}:*", kind, message_type)
}

pub(crate) fn checksum_json(value: &JsonValue) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.to_string().as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

pub(crate) fn checksum_str(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("sha256:{:x}", hasher.finalize())
}

fn sanitize_cache_part(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn json_scalar_to_string(value: &JsonValue) -> String {
    match value {
        JsonValue::String(value) => value.clone(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::Bool(value) => value.to_string(),
        JsonValue::Null => String::new(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
pub(crate) fn json_i64(value: &JsonValue) -> Result<i64, tonic::Status> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| {
            executor_utils_invalid_field(
                "value",
                "must be an integer or integer string",
                format!("expected integer, got {value}"),
            )
        })
}

#[cfg(test)]
pub(crate) fn json_f64(value: &JsonValue) -> Result<f64, tonic::Status> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse().ok())
        .ok_or_else(|| {
            executor_utils_invalid_field(
                "value",
                "must be a number or numeric string",
                format!("expected number, got {value}"),
            )
        })
}

pub(crate) fn json_is_ciphertext(value: &JsonValue) -> bool {
    value.as_str().is_some_and(is_ciphertext)
}

pub(crate) fn is_ciphertext(value: &str) -> bool {
    value.starts_with("udb-aead:v")
}

// ── Time helpers ──────────────────────────────────────────────────────────────

#[cfg(feature = "s3")]
pub(crate) fn bounded_ttl(ttl_seconds: i32) -> u64 {
    if ttl_seconds <= 0 {
        900
    } else {
        (ttl_seconds as u64).min(3600)
    }
}

#[cfg(feature = "s3")]
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

// ── Manifest helpers ──────────────────────────────────────────────────────────

pub(crate) fn is_encrypted_column(column: &ManifestColumn) -> bool {
    column.encrypted || column.security.is_encrypted
}

pub(crate) fn declared_materialized_view<'a>(
    manifest: &'a CatalogManifest,
    schema: &str,
    name: &str,
) -> Option<&'a ManifestMaterializedView> {
    manifest
        .tables
        .iter()
        .flat_map(|table| table.materialized_views.iter())
        .find(|view| view.schema == schema && view.name == name)
}

pub(crate) fn store_option(store: &ManifestStore, key: &str) -> String {
    store
        .options
        .iter()
        .find(|option| option.key == key)
        .map(|option| option.value.clone())
        .unwrap_or_default()
}

pub(crate) fn store_option_i32(store: &ManifestStore, key: &str) -> i32 {
    store_option(store, key).parse().unwrap_or_default()
}

// ── SQL / identifier helpers ──────────────────────────────────────────────────

pub(crate) fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Quote an identifier for use in SQL as a double-quoted identifier.
/// Delegates to the canonical implementation in `generation::sql::qi`.
pub(crate) fn qi_runtime(value: &str) -> String {
    crate::generation::sql::qi(value)
}

// ── Generic-dispatch JSON request helpers ───────────────────────────────────────
// Shared by the runtime God-impl and per-backend executors (e.g. Qdrant).

pub(crate) fn json_required_str<'a>(
    value: &'a JsonValue,
    key: &str,
) -> Result<&'a str, tonic::Status> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|raw| !raw.trim().is_empty())
        .ok_or_else(|| {
            executor_utils_invalid_field(
                key,
                "must be a non-empty string",
                format!("{key} is required"),
            )
        })
}

pub(crate) fn json_required_f32_vec(
    value: &JsonValue,
    key: &str,
) -> Result<Vec<f32>, tonic::Status> {
    let values = value
        .get(key)
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            executor_utils_invalid_field(key, "must be an array", format!("{key} must be an array"))
        })?;
    if values.is_empty() {
        return Err(executor_utils_invalid_field(
            key,
            "must not be empty",
            format!("{key} must not be empty"),
        ));
    }
    values
        .iter()
        .map(|value| {
            value.as_f64().map(|number| number as f32).ok_or_else(|| {
                executor_utils_invalid_field(
                    key,
                    "must contain only numbers",
                    format!("{key} must contain only numbers"),
                )
            })
        })
        .collect()
}

pub(crate) fn json_i32(value: &JsonValue, key: &str) -> Option<i32> {
    value
        .get(key)
        .and_then(JsonValue::as_i64)
        .and_then(|number| i32::try_from(number).ok())
}

pub(crate) fn json_bool(value: &JsonValue, key: &str) -> Option<bool> {
    value.get(key).and_then(JsonValue::as_bool)
}

/// Decode inline object bytes from a generic-dispatch request body
/// (`data_base64`/`content_base64` or `data_text`/`content_text`).
pub(crate) fn object_bytes_from_json(value: &JsonValue) -> Result<Vec<u8>, tonic::Status> {
    if let Some(base64_value) = value
        .get("data_base64")
        .or_else(|| value.get("content_base64"))
        .and_then(JsonValue::as_str)
    {
        // D.5: through the accel layer (scalar today; SIMD swap-in point for D.6).
        return crate::runtime::accel::base64_decode(base64_value).map_err(|err| {
            executor_utils_invalid_field(
                "data_base64",
                "must be valid base64 object bytes",
                format!("invalid object base64: {err}"),
            )
        });
    }
    if let Some(text) = value
        .get("data_text")
        .or_else(|| value.get("content_text"))
        .and_then(JsonValue::as_str)
    {
        return Ok(text.as_bytes().to_vec());
    }
    Err(executor_utils_invalid_field(
        "object_bytes",
        "data_base64, content_base64, data_text, or content_text is required",
        "object bytes are required as data_base64, content_base64, data_text, or content_text",
    ))
}

#[cfg(test)]
pub(crate) fn validate_identifier(value: &str, label: &str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || value.starts_with(|ch: char| ch.is_ascii_digit())
    {
        return Err(executor_utils_invalid_field(
            label,
            "must be a valid SQL identifier",
            format!("{label} '{value}' is not a valid SQL identifier"),
        ));
    }
    Ok(())
}

// ── Environment helpers ───────────────────────────────────────────────────────

pub(crate) fn env_first(keys: &[&str]) -> Option<String> {
    // Trim the returned value (not just the emptiness check): a trailing CRLF `\r`
    // from a CRLF-encoded `.env` survives `env::var` and, e.g., poisons an HTTP
    // header value (Qdrant `api-key`) as an opaque reqwest "builder error". Trim
    // once here so every backend DSN/key/token consumer is clean.
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

/// Fill a fresh 32-byte key/secret from the OS CSPRNG. Cleaner than the
/// concatenated-`Uuid::new_v4()` idiom (which fixes the version/variant nibbles);
/// used for vault DEKs, nonces, and dynamic passwords. Panics only if the OS RNG
/// is unavailable — the same failure mode `uuid`'s getrandom-backed v4 has.
pub(crate) fn random_32_bytes() -> [u8; 32] {
    let mut buf = [0u8; 32];
    getrandom::getrandom(&mut buf).expect("OS CSPRNG unavailable");
    buf
}

/// Fill `n` bytes from the OS CSPRNG (e.g. a 12-byte AEAD nonce).
pub(crate) fn random_bytes(n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    getrandom::getrandom(&mut buf).expect("OS CSPRNG unavailable");
    buf
}

/// Constant-time byte-slice equality (no early-exit timing leak). A length
/// mismatch returns false. Shared so MAC/secret comparisons don't each hand-roll
/// their own (the `security.rs`/`authn` string variants exist for `&str`).
pub(crate) fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

pub(crate) fn env_identifier(key: &str, fallback: &str) -> String {
    env::var(key)
        .ok()
        .filter(|value| is_identifier(value))
        .unwrap_or_else(|| fallback.to_string())
}

pub(crate) fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(crate) fn env_u32(key: &str) -> Option<u32> {
    env::var(key).ok()?.parse().ok()
}

pub(crate) fn env_i32(key: &str) -> Option<i32> {
    env::var(key).ok()?.parse().ok()
}

// ── Generic-dispatch SQL request helpers ────────────────────────────────────────
// Shared by the SQL/CQL generic-dispatch executors (mysql, sqlite, mssql,
// cassandra). The dispatch JSON shape is `{"sql":"…","params":[…]}` (the
// `parameters` key is accepted as an alias). Pulled out of the per-backend
// modules so the byte-identical parser lives in exactly one place.

/// Parse `{"sql":"…","params":[…]}` (or `"parameters"`) out of a generic
/// dispatch request. Returns the SQL string plus the positional parameter
/// array (defaulting to empty when absent).
pub(crate) fn parse_sql_dispatch(
    request_json: &str,
) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    let value: JsonValue = serde_json::from_str(request_json).map_err(|e| {
        executor_utils_invalid_field(
            "request_json",
            "must be valid dispatch JSON",
            format!("invalid dispatch JSON: {e}"),
        )
    })?;
    let sql = value
        .get("sql")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            executor_utils_invalid_field(
                "sql",
                "dispatch request must include sql",
                "missing `sql` in dispatch request",
            )
        })?
        .to_string();
    let params = value
        .get("params")
        .or_else(|| value.get("parameters"))
        .and_then(JsonValue::as_array)
        .cloned()
        .unwrap_or_default();
    Ok((sql, params))
}

/// Encode a binary cell as the `base64:<STANDARD>` JSON string the SQL
/// executors (mysql / sqlite / mssql) emit for `BLOB`/`VARBINARY` columns.
#[cfg(any(feature = "mysql", feature = "sqlite", feature = "mssql"))]
pub(crate) fn base64_cell(bytes: &[u8]) -> JsonValue {
    // D.5: through the accel layer (scalar today; SIMD swap-in point for D.6).
    JsonValue::String(format!(
        "base64:{}",
        crate::runtime::accel::base64_encode(bytes)
    ))
}

/// Render a generic `sqlx::Row` into a JSON object. Each column becomes a
/// key; values are coerced by probing `i64 → f64 → bool → String → Vec<u8>`
/// (bytes → `base64:` string) and falling back to JSON null. Shared by the
/// mysql + sqlite executors, whose row converters were byte-identical save
/// for the concrete `Row` type.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) fn sqlx_row_to_json<R>(row: &R) -> JsonValue
where
    R: sqlx::Row,
    usize: sqlx::ColumnIndex<R>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> f64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> bool: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Vec<u8>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    // W3: temporal probes for MySQL DATETIME/TIMESTAMP/DATE (the concrete mysql +
    // sqlite callers both satisfy these via sqlx's `chrono` feature).
    for<'a> chrono::DateTime<chrono::Utc>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> chrono::NaiveDateTime: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> chrono::NaiveDate: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    // `Column` is needed for `col.name()`; `Row`'s methods (`columns`,
    // `try_get`) are callable directly on the `R: sqlx::Row`-bounded param
    // without importing the trait into scope.
    use sqlx::Column as _;
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        // Try common types in order; fall back to NULL.
        let value: JsonValue = if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<bool>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<chrono::NaiveDateTime>, _>(i) {
            // W3: MySQL native TIMESTAMP/DATETIME/DATE decode as chrono types, NOT
            // String — without these branches every temporal column read back NULL
            // (silent data loss). Probe temporals before String (a real VARCHAR
            // fails the chrono decode and falls through).
            //
            // NaiveDateTime is probed BEFORE DateTime<Utc>: MySQL DATETIME is
            // zone-less, but sqlx can also decode it as DateTime<Utc>, inventing
            // a `+00:00` offset that the stored value never carried.
            v.map(|dt| JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(i) {
            v.map(|dt| JsonValue::String(dt.to_rfc3339()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<chrono::NaiveDate>, _>(i) {
            v.map(|d| JsonValue::String(d.format("%Y-%m-%d").to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<String>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(i) {
            v.map(|bytes| base64_cell(&bytes))
                .unwrap_or(JsonValue::Null)
        } else {
            JsonValue::Null
        };
        obj.insert(name, value);
    }
    JsonValue::Object(obj)
}

/// Bind a slice of JSON values as positional parameters onto an `sqlx`
/// query. Numbers go to i64/f64, strings/bool/null map directly, and
/// objects/arrays serialise to JSON-text so the driver stores them in a
/// JSON column. Shared by the mysql + sqlite executors.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) fn bind_json_params<'q, DB>(
    mut q: sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>,
    params: &'q [JsonValue],
) -> sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments<'q>>
where
    DB: sqlx::Database,
    for<'a> Option<i64>: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> i64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> f64: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> bool: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
    for<'a> String: sqlx::Encode<'a, DB> + sqlx::Type<DB>,
{
    for p in params {
        q = match p {
            JsonValue::Null => q.bind(Option::<i64>::None),
            JsonValue::Bool(b) => q.bind(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    q.bind(i)
                } else if let Some(f) = n.as_f64() {
                    q.bind(f)
                } else {
                    q.bind(n.to_string())
                }
            }
            JsonValue::String(s) => q.bind(s.clone()),
            other => q.bind(other.to_string()),
        };
    }
    q
}

/// Apply a batch of context SET / INSERT statements (rendered by
/// `render_sql_session_settings`) on an open `sqlx` transaction. Shared by
/// the mysql + sqlite session-context application paths, which differed only
/// in the connection type and the error-message prefix.
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) async fn apply_context_statements<'c, DB>(
    tx: &mut sqlx::Transaction<'c, DB>,
    statements: &[String],
    error_prefix: &str,
) -> Result<(), tonic::Status>
where
    DB: sqlx::Database,
    for<'e> &'e mut <DB as sqlx::Database>::Connection: sqlx::Executor<'e, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
{
    for stmt in statements {
        sqlx::query(stmt).execute(&mut **tx).await.map_err(|err| {
            internal_status("database", error_prefix, format!("{error_prefix}: {err}"))
        })?;
    }
    Ok(())
}

// ── Object-store dispatch helper ─────────────────────────────────────────────
// Shared by the GCS + Azure-Blob executors, whose `parse_dispatch` differed
// only in which field name is primary and the missing-field error label.

/// Parse an object-store dispatch request of the shape
/// `{"op":"…","<bucket>":"…","<key>":"…","content_type":"…"}`. The
/// `bucket_keys` / `object_keys` slices list the accepted field aliases in
/// priority order (e.g. `["bucket","container"]`). `bucket_label` names the
/// bucket field in the missing-field error. Returns
/// `(op, bucket, object, content_type)`.
#[cfg(any(feature = "gcs", feature = "azureblob"))]
pub(crate) fn parse_object_dispatch(
    req: &str,
    bucket_keys: &[&str],
    object_keys: &[&str],
    bucket_label: &str,
) -> Result<(String, String, String, Option<String>), tonic::Status> {
    let v: JsonValue = serde_json::from_str(req).map_err(|e| {
        executor_utils_invalid_field(
            "request_json",
            "must be valid dispatch JSON",
            format!("invalid dispatch JSON: {e}"),
        )
    })?;
    let op = v
        .get("op")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| executor_utils_invalid_field("op", "operation is required", "missing `op`"))?
        .to_string();
    let bucket = bucket_keys
        .iter()
        .find_map(|key| v.get(*key).and_then(JsonValue::as_str))
        .ok_or_else(|| {
            executor_utils_invalid_field(
                bucket_label,
                "bucket/container field is required",
                format!("missing `{bucket_label}`"),
            )
        })?
        .to_string();
    let object = object_keys
        .iter()
        .find_map(|key| v.get(*key).and_then(JsonValue::as_str))
        .unwrap_or("")
        .to_string();
    let content_type = v
        .get("content_type")
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    Ok((op, bucket, object, content_type))
}

// ── REST dispatch + HTTP-status helpers ──────────────────────────────────────
// Shared by the REST-backed executors (pinecone, weaviate, elasticsearch).

/// Parse a REST dispatch request of the shape
/// `{"path":"…","method":"…","body":{…}}`. `method` defaults to `POST` and
/// `body` defaults to JSON null. Byte-identical across pinecone + weaviate +
/// elasticsearch.
#[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
pub(crate) fn parse_rest_dispatch(
    req: &str,
) -> Result<(reqwest::Method, String, JsonValue), tonic::Status> {
    let mut v: JsonValue = serde_json::from_str(req).map_err(|e| {
        executor_utils_invalid_field(
            "request_json",
            "must be valid dispatch JSON",
            format!("invalid dispatch JSON: {e}"),
        )
    })?;
    let path = v
        .get("path")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| executor_utils_invalid_field("path", "path is required", "missing `path`"))?
        .to_string();
    let method = v
        .get("method")
        .and_then(JsonValue::as_str)
        .unwrap_or("POST")
        .parse::<reqwest::Method>()
        .map_err(|e| {
            executor_utils_invalid_field(
                "method",
                "must be a valid HTTP method",
                format!("bad method: {e}"),
            )
        })?;
    // D.3: move the (possibly large, nested) body out via `Value::take` instead of
    // deep-cloning it. `path`/`method` are already extracted, so replacing `body`
    // with Null is harmless; the returned body is byte-for-byte the same value.
    let body = v
        .get_mut("body")
        .map(JsonValue::take)
        .unwrap_or(JsonValue::Null);
    Ok((method, path, body))
}

/// Map an unsuccessful HTTP status + (already-extracted) detail message to a
/// typed `tonic::Status`, tagging the message with `backend`. This is the
/// shared mapping for the REST backends (pinecone, weaviate, elasticsearch).
/// The caller supplies the human-readable `detail` (truncated body, or a parsed
/// `error.reason`) so each backend keeps control of how it summarises bodies.
#[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
pub(crate) fn http_status_to_tonic(
    status: reqwest::StatusCode,
    detail: &str,
    backend: &str,
) -> tonic::Status {
    let code = status.as_u16();
    match code {
        400 | 422 => executor_utils_invalid_field(
            "request",
            "backend rejected the request as invalid",
            format!("{backend} {code}: {detail}"),
        ),
        401 | 403 => policy_status_with_code(
            tonic::Code::PermissionDenied,
            "backend_http_authz",
            format!("{}_http_{code}", backend),
            format!("{backend} {code}: {detail}"),
        ),
        404 => schema_status(
            tonic::Code::NotFound,
            backend,
            "request",
            "backend_http_not_found",
            format!("{backend} 404: {detail}"),
        ),
        409 => schema_status(
            tonic::Code::AlreadyExists,
            backend,
            "request",
            "backend_http_conflict",
            format!("{backend} 409: {detail}"),
        ),
        429 => quota_status(
            backend,
            "request",
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("{backend} 429: {detail}"),
        ),
        // 5xx is a transient server-side failure: emit the typed retryable detail
        // (Unavailable, retryable=true, suggested backoff) so SDK clients retry.
        500..=599 => retryable_status(
            backend,
            "request",
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("{backend} {code}: {detail}"),
        ),
        _ => internal_status(
            backend,
            format!("http_{code}"),
            format!("{backend} {code}: {detail}"),
        ),
    }
}

// ── Probe builder ─────────────────────────────────────────────────────────────

/// Build a `BackendProbe` from a backend name and a health-check result.
/// Every backend (except S3, which is special-cased to always report ok)
/// constructed its probe with this exact `match` — pulled into one place.
pub(crate) fn build_probe(
    name: &str,
    health: Result<(), String>,
) -> crate::runtime::executors::BackendProbe {
    let (ok, error) = match health {
        Ok(()) => (true, None),
        Err(err) => (false, Some(err)),
    };
    crate::runtime::executors::BackendProbe {
        backend: name.to_string(),
        instance: None,
        ok,
        error,
    }
}

// ── HTTP / Qdrant helpers ─────────────────────────────────────────────────────

#[cfg(feature = "qdrant")]
pub(crate) fn qdrant_status(status: reqwest::StatusCode) -> Result<(), tonic::Status> {
    if status.is_success() {
        Ok(())
    } else if status.is_server_error() {
        // 5xx from Qdrant is transient: typed retryable detail + backoff.
        Err(retryable_status(
            "qdrant",
            "request",
            HTTP_RETRYABLE_BACKOFF_MS,
            format!("Qdrant returned HTTP {status}"),
        ))
    } else {
        Err(schema_status(
            tonic::Code::FailedPrecondition,
            "qdrant",
            "request",
            "qdrant_http_rejected",
            format!("Qdrant returned HTTP {status}"),
        ))
    }
}

// ── Typed error detail tests (A.3) ────────────────────────────────────────────

#[cfg(test)]
mod conversion_tests {
    use super::{json_into_prost_value, json_into_struct, json_to_prost_value, json_to_struct};
    use serde_json::json;

    fn sample() -> serde_json::Value {
        json!({
            "id": "abc", "n": 42, "f": 3.5, "b": true, "nil": null,
            "tags": ["x", "y", 1, false, null],
            "nested": {"k": "v", "deep": {"arr": [1, 2.5, "z"], "flag": true}}
        })
    }

    #[test]
    fn json_into_struct_matches_borrowing_variant() {
        let v = sample();
        let borrowed = json_to_struct(&v).expect("object");
        let moved = json_into_struct(v).expect("object");
        assert_eq!(
            borrowed, moved,
            "consuming variant must yield an identical Struct"
        );
    }

    #[test]
    fn json_into_prost_value_matches_borrowing_variant() {
        let v = sample();
        let borrowed = json_to_prost_value(&v).expect("value");
        let moved = json_into_prost_value(v).expect("value");
        assert_eq!(borrowed, moved);
    }

    #[test]
    fn json_into_struct_rejects_non_object() {
        assert!(json_into_struct(json!([1, 2, 3])).is_none());
        assert!(json_into_struct(json!("scalar")).is_none());
    }
}

#[cfg(test)]
mod error_detail_tests {
    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    use super::http_status_to_tonic;
    #[cfg(feature = "qdrant")]
    use super::qdrant_status;
    use super::{
        ERROR_DETAIL_METADATA_KEY, HTTP_RETRYABLE_BACKOFF_MS, INLINE_OBJECT_LIMIT_BYTES,
        STATUS_TAG_PREFIX, backend_transport_status, capability_status, compile_error_status,
        deadline_exceeded_status, failed_precondition_fields, internal_status,
        invalid_argument_fields, json_f64, json_i64, json_required_f32_vec, json_required_str,
        object_bytes_from_json, parse_sql_dispatch, policy_status, policy_status_with_code,
        prefix_status, prost_value_to_json, quota_refusal_status, quota_status,
        referential_constraint_status, reject_oversized_object, reject_plan,
        retryable_aborted_status, retryable_status, schema_status, sqlx_error_to_status,
        status_from_store_string, status_with_error_detail, validate_identifier,
    };
    use crate::proto::{ErrorDetail, ErrorKind};
    use prost_types::Value as ProstValue;
    use serde_json::json;

    /// Decode the prost `ErrorDetail` from the binary trailer the way an SDK
    /// would, so the test proves the typed detail is recoverable end-to-end.
    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn capability_status_carries_typed_detail_and_preserves_message() {
        let status = capability_status(
            "cassandra",
            "ensure_resource",
            "supports_resource_lifecycle",
            "backend 'cassandra' does not support operation 'ensure_resource'",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        // Human-readable message is preserved verbatim (additive detail only).
        assert!(status.message().contains("does not support operation"));
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "cassandra");
        assert_eq!(detail.operation, "ensure_resource");
        assert_eq!(detail.capability_required, "supports_resource_lifecycle");
        assert!(!detail.retryable);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
    }

    #[test]
    fn policy_status_carries_typed_detail_and_preserves_message() {
        let status = policy_status(
            "webauthn_registration_policy",
            "resident_key_required",
            "WebAuthn policy denied registration",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "WebAuthn policy denied registration");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "webauthn_registration_policy");
        assert_eq!(detail.policy_decision_id, "resident_key_required");
        assert!(detail.backend.is_empty());
        assert!(detail.capability_required.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn policy_status_with_code_preserves_permission_denied_code() {
        let status = policy_status_with_code(
            tonic::Code::PermissionDenied,
            "backend_http_authz",
            "pinecone_http_403",
            "pinecone 403: forbidden",
        );
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), "pinecone 403: forbidden");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "backend_http_authz");
        assert_eq!(detail.policy_decision_id, "pinecone_http_403");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    #[test]
    fn http_status_to_tonic_authz_rejection_carries_policy_detail() {
        let status = http_status_to_tonic(reqwest::StatusCode::FORBIDDEN, "forbidden", "pinecone");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), "pinecone 403: forbidden");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "backend_http_authz");
        assert_eq!(detail.policy_decision_id, "pinecone_http_403");
        assert!(detail.field_violations.is_empty());
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    #[test]
    fn http_status_to_tonic_429_carries_quota_retry_after_detail() {
        let status = http_status_to_tonic(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "rate limited",
            "pinecone",
        );
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(status.message(), "pinecone 429: rate limited");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, "request");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, HTTP_RETRYABLE_BACKOFF_MS);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn retryable_status_is_unavailable_with_backoff() {
        let status = retryable_status("postgres", "query", 250, "temporarily unavailable");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let detail = decode_detail(&status);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 250);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    }

    #[test]
    fn deadline_exceeded_status_preserves_deadline_code_with_retry_detail() {
        let status =
            deadline_exceeded_status("channel", "read channel", 500, "read channel timeout");
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
        assert_eq!(status.message(), "read channel timeout");
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "channel");
        assert_eq!(detail.operation, "read_channel");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 500);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn backend_transport_status_is_typed_retryable() {
        let status = backend_transport_status("Qdrant", "collection check", "connection reset");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            status.message(),
            "Qdrant collection check failed: connection reset"
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "Qdrant");
        assert_eq!(detail.operation, "collection_check");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, HTTP_RETRYABLE_BACKOFF_MS);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    }

    #[cfg(feature = "qdrant")]
    #[test]
    fn qdrant_client_http_rejections_carry_schema_detail() {
        let status = qdrant_status(reqwest::StatusCode::BAD_REQUEST)
            .expect_err("4xx Qdrant response should be rejected");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "Qdrant returned HTTP 400 Bad Request");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "qdrant");
        assert_eq!(detail.operation, "request");
        assert_eq!(detail.capability_required, "qdrant_http_rejected");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn retryable_aborted_status_preserves_aborted_code() {
        let status = retryable_aborted_status(
            "authz",
            "optimistic concurrency",
            0,
            "policy changed concurrently",
        );
        assert_eq!(status.code(), tonic::Code::Aborted);
        assert_eq!(status.message(), "policy changed concurrently");
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "authz");
        assert_eq!(detail.operation, "optimistic_concurrency");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
    }

    #[test]
    fn error_detail_builder_clears_retryable_field_violations() {
        let status = status_with_error_detail(
            tonic::Code::Unavailable,
            "temporarily unavailable",
            ErrorDetail {
                backend: "postgres".to_string(),
                operation: "query".to_string(),
                capability_required: String::new(),
                retryable: true,
                retry_after_ms: 250,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Retryable as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "tenant_id".to_string(),
                    description: "required".to_string(),
                }],
            },
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
        assert!(detail.field_violations.is_empty());
        assert_eq!(detail.backend, "postgres");
        assert_eq!(detail.operation, "query");
    }

    #[test]
    fn quota_status_is_resource_exhausted_with_backoff() {
        let status = quota_status(
            "channel",
            "admin fair admission",
            1_000,
            "admin fair queue budget exhausted; retry after 1s",
        );
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "channel");
        assert_eq!(detail.operation, "admin_fair_admission");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 1_000);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
    }

    #[test]
    fn error_detail_builder_clears_quota_field_violations() {
        let status = status_with_error_detail(
            tonic::Code::ResourceExhausted,
            "quota exhausted",
            ErrorDetail {
                backend: "channel".to_string(),
                operation: "fair_admission".to_string(),
                capability_required: String::new(),
                retryable: true,
                retry_after_ms: 250,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Quota as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "tenant_id".to_string(),
                    description: "required".to_string(),
                }],
            },
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert!(detail.field_violations.is_empty());
        assert_eq!(detail.backend, "channel");
        assert_eq!(detail.operation, "fair_admission");
    }

    #[test]
    fn error_detail_builder_clears_non_validation_field_violations() {
        let capability_status = status_with_error_detail(
            tonic::Code::FailedPrecondition,
            "unsupported operation",
            ErrorDetail {
                backend: "cassandra".to_string(),
                operation: "ensure_resource".to_string(),
                capability_required: "supports_resource_lifecycle".to_string(),
                retryable: false,
                retry_after_ms: 0,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Capability as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "sql".to_string(),
                    description: "unsupported".to_string(),
                }],
            },
        );
        let capability_detail = decode_detail(&capability_status);
        assert_eq!(capability_detail.kind, ErrorKind::Capability as i32);
        assert!(capability_detail.field_violations.is_empty());
        assert_eq!(capability_detail.backend, "cassandra");
        assert_eq!(
            capability_detail.capability_required,
            "supports_resource_lifecycle"
        );

        let policy_status = status_with_error_detail(
            tonic::Code::PermissionDenied,
            "policy denied request",
            ErrorDetail {
                backend: String::new(),
                operation: "authorize".to_string(),
                capability_required: String::new(),
                retryable: false,
                retry_after_ms: 0,
                policy_decision_id: "decision-123".to_string(),
                correlation_id: String::new(),
                kind: ErrorKind::Policy as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "principal".to_string(),
                    description: "denied".to_string(),
                }],
            },
        );
        let policy_detail = decode_detail(&policy_status);
        assert_eq!(policy_detail.kind, ErrorKind::Policy as i32);
        assert!(policy_detail.field_violations.is_empty());
        assert_eq!(policy_detail.operation, "authorize");
        assert_eq!(policy_detail.policy_decision_id, "decision-123");

        let schema_status = status_with_error_detail(
            tonic::Code::InvalidArgument,
            "schema compile failed",
            ErrorDetail {
                backend: "postgres".to_string(),
                operation: "compile".to_string(),
                capability_required: "operation_not_supported".to_string(),
                retryable: false,
                retry_after_ms: 0,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Schema as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "table_name".to_string(),
                    description: "bad schema".to_string(),
                }],
            },
        );
        let schema_detail = decode_detail(&schema_status);
        assert_eq!(schema_detail.kind, ErrorKind::Schema as i32);
        assert!(schema_detail.field_violations.is_empty());
        assert_eq!(schema_detail.backend, "postgres");
        assert_eq!(schema_detail.capability_required, "operation_not_supported");

        for kind in [
            ErrorKind::Internal as i32,
            ErrorKind::Unspecified as i32,
            99,
        ] {
            let status = status_with_error_detail(
                tonic::Code::Internal,
                "internal failure",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: String::new(),
                    retryable: false,
                    retry_after_ms: 0,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: vec![crate::proto::ErrorFieldViolation {
                        field: "request".to_string(),
                        description: "not validation".to_string(),
                    }],
                },
            );
            let detail = decode_detail(&status);
            assert_eq!(detail.kind, kind);
            assert!(detail.field_violations.is_empty());
            assert_eq!(detail.backend, "broker");
            assert_eq!(detail.operation, "dispatch");
        }
    }

    #[test]
    fn error_detail_builder_canonicalizes_non_retryable_error_kinds() {
        for kind in [
            ErrorKind::Capability as i32,
            ErrorKind::Policy as i32,
            ErrorKind::Schema as i32,
            ErrorKind::Internal as i32,
            ErrorKind::Unspecified as i32,
            99,
        ] {
            let status = status_with_error_detail(
                tonic::Code::FailedPrecondition,
                "not retryable",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: "not_retryable".to_string(),
                    retryable: true,
                    retry_after_ms: 250,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert_eq!(detail.kind, kind);
            assert!(!detail.retryable);
            assert_eq!(detail.retry_after_ms, 0);
        }
    }

    #[test]
    fn retryable_details_never_expose_negative_backoff() {
        let unavailable = retryable_status("postgres", "query", -250, "retry now");
        let unavailable_detail = decode_detail(&unavailable);
        assert_eq!(unavailable_detail.retry_after_ms, 0);

        let aborted = retryable_aborted_status("authz", "policy update", -1, "retry now");
        let aborted_detail = decode_detail(&aborted);
        assert_eq!(aborted_detail.retry_after_ms, 0);

        let deadline = deadline_exceeded_status("channel", "read", -500, "retry now");
        let deadline_detail = decode_detail(&deadline);
        assert_eq!(deadline_detail.retry_after_ms, 0);

        let quota = quota_status("channel", "fair_admission", -1_000, "retry now");
        let quota_detail = decode_detail(&quota);
        assert_eq!(quota_detail.retry_after_ms, 0);
    }

    #[test]
    fn error_detail_builder_never_exposes_negative_backoff() {
        let status = status_with_error_detail(
            tonic::Code::Unavailable,
            "retry later",
            ErrorDetail {
                backend: "custom".to_string(),
                operation: "custom retry".to_string(),
                capability_required: String::new(),
                retryable: true,
                retry_after_ms: -750,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Retryable as i32,
                field_violations: Vec::new(),
            },
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn error_detail_builder_canonicalizes_retryable_identity_tokens() {
        for kind in [ErrorKind::Retryable as i32, ErrorKind::Quota as i32] {
            let status = status_with_error_detail(
                tonic::Code::ResourceExhausted,
                "retry later",
                ErrorDetail {
                    backend: " fair admission ".to_string(),
                    operation: "worker queue".to_string(),
                    capability_required: String::new(),
                    retryable: true,
                    retry_after_ms: 250,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert_eq!(detail.backend, "fair_admission");
            assert_eq!(detail.operation, "worker_queue");
        }

        let status = status_with_error_detail(
            tonic::Code::ResourceExhausted,
            "retry later",
            ErrorDetail {
                backend: String::new(),
                operation: String::new(),
                capability_required: String::new(),
                retryable: true,
                retry_after_ms: 250,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Quota as i32,
                field_violations: Vec::new(),
            },
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "backend");
        assert_eq!(detail.operation, "operation");
    }

    #[test]
    fn error_detail_builder_clears_non_retryable_backoff() {
        for kind in [
            ErrorKind::Retryable as i32,
            ErrorKind::Quota as i32,
            ErrorKind::Capability as i32,
            ErrorKind::Policy as i32,
            ErrorKind::Schema as i32,
        ] {
            let status = status_with_error_detail(
                tonic::Code::ResourceExhausted,
                "not retryable",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: String::new(),
                    retryable: false,
                    retry_after_ms: 2_500,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert!(!detail.retryable);
            assert_eq!(detail.retry_after_ms, 0);
        }
    }

    #[test]
    fn error_detail_builder_clears_non_policy_decision_ids() {
        let policy_status = status_with_error_detail(
            tonic::Code::PermissionDenied,
            "policy denied request",
            ErrorDetail {
                backend: String::new(),
                operation: "authorize".to_string(),
                capability_required: String::new(),
                retryable: false,
                retry_after_ms: 0,
                policy_decision_id: "decision-123".to_string(),
                correlation_id: String::new(),
                kind: ErrorKind::Policy as i32,
                field_violations: Vec::new(),
            },
        );
        let policy_detail = decode_detail(&policy_status);
        assert_eq!(policy_detail.policy_decision_id, "decision-123");

        for kind in [
            ErrorKind::Validation as i32,
            ErrorKind::Retryable as i32,
            ErrorKind::Quota as i32,
            ErrorKind::Capability as i32,
            ErrorKind::Schema as i32,
            ErrorKind::Internal as i32,
            ErrorKind::Unspecified as i32,
            99,
        ] {
            let status = status_with_error_detail(
                tonic::Code::ResourceExhausted,
                "not policy",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: String::new(),
                    retryable: false,
                    retry_after_ms: 0,
                    policy_decision_id: "decision-123".to_string(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert!(detail.policy_decision_id.is_empty());
        }
    }

    #[test]
    fn error_detail_builder_clears_non_capability_required_fields() {
        for (kind, expected) in [
            (ErrorKind::Capability as i32, "native_resource_lifecycle"),
            (ErrorKind::Schema as i32, "operation_not_supported"),
        ] {
            let status = status_with_error_detail(
                tonic::Code::FailedPrecondition,
                "capability or schema",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: expected.to_string(),
                    retryable: false,
                    retry_after_ms: 0,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert_eq!(detail.capability_required, expected);
        }

        for kind in [
            ErrorKind::Validation as i32,
            ErrorKind::Retryable as i32,
            ErrorKind::Quota as i32,
            ErrorKind::Policy as i32,
            ErrorKind::Internal as i32,
            ErrorKind::Unspecified as i32,
            99,
        ] {
            let status = status_with_error_detail(
                tonic::Code::ResourceExhausted,
                "not capability",
                ErrorDetail {
                    backend: "broker".to_string(),
                    operation: "dispatch".to_string(),
                    capability_required: "native_resource_lifecycle".to_string(),
                    retryable: false,
                    retry_after_ms: 0,
                    policy_decision_id: String::new(),
                    correlation_id: String::new(),
                    kind,
                    field_violations: Vec::new(),
                },
            );
            let detail = decode_detail(&status);
            assert!(detail.capability_required.is_empty());
        }
    }

    #[test]
    fn error_detail_builder_canonicalizes_validation_retry_shape() {
        let status_with_field = status_with_error_detail(
            tonic::Code::InvalidArgument,
            "tenant_id is required",
            ErrorDetail {
                backend: "admission".to_string(),
                operation: "fair_queue".to_string(),
                capability_required: "quota".to_string(),
                retryable: true,
                retry_after_ms: 250,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Validation as i32,
                field_violations: vec![crate::proto::ErrorFieldViolation {
                    field: "tenant_id".to_string(),
                    description: "required".to_string(),
                }],
            },
        );
        let detail = decode_detail(&status_with_field);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(detail.backend.is_empty());
        assert!(detail.operation.is_empty());
        assert!(detail.capability_required.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert_eq!(detail.field_violations[0].field, "tenant_id");

        let status_without_fields = status_with_error_detail(
            tonic::Code::InvalidArgument,
            "invalid request",
            ErrorDetail {
                backend: String::new(),
                operation: String::new(),
                capability_required: String::new(),
                retryable: false,
                retry_after_ms: 0,
                policy_decision_id: String::new(),
                correlation_id: String::new(),
                kind: ErrorKind::Validation as i32,
                field_violations: Vec::new(),
            },
        );
        let fallback_detail = decode_detail(&status_without_fields);
        assert_eq!(fallback_detail.field_violations.len(), 1);
        assert_eq!(fallback_detail.field_violations[0].field, "field");
        assert_eq!(
            fallback_detail.field_violations[0].description,
            "invalid field"
        );
    }

    #[test]
    fn error_detail_public_strings_are_bounded_and_control_free() {
        let long_backend = "b".repeat(super::MAX_ERROR_DETAIL_STRING_BYTES + 8);
        let long_message = "m".repeat(super::MAX_ERROR_DETAIL_STRING_BYTES + 8);
        let long_description = "d".repeat(super::MAX_ERROR_DETAIL_STRING_BYTES + 8);
        let status = retryable_status(
            format!("{long_backend}\n"),
            "query\tpath",
            1,
            format!(" {long_message}\n"),
        );
        assert_eq!(status.message().len(), super::MAX_ERROR_DETAIL_STRING_BYTES);
        assert!(status.message().chars().all(|ch| !ch.is_control()));
        let detail = decode_detail(&status);
        assert_eq!(detail.backend.len(), super::MAX_ERROR_DETAIL_STRING_BYTES);
        assert!(detail.backend.chars().all(|ch| !ch.is_control()));
        assert_eq!(detail.operation, "querypath");

        let fallback_status = retryable_status("postgres", "query", 1, "\n\t");
        assert_eq!(fallback_status.message(), "error");

        let field_status = invalid_argument_fields(
            "bad request",
            [
                ("\n", "bad\rfield"),
                ("field\nname", "\t"),
                (" tenant_id ", " required "),
                ("tenant id", "not canonical"),
                ("description", long_description.as_str()),
            ],
        );
        let field_detail = decode_detail(&field_status);
        assert_eq!(field_detail.field_violations[0].field, "field");
        assert_eq!(field_detail.field_violations[0].description, "badfield");
        assert_eq!(field_detail.field_violations[1].field, "fieldname");
        assert_eq!(
            field_detail.field_violations[1].description,
            "invalid field"
        );
        assert_eq!(field_detail.field_violations[2].field, "tenant_id");
        assert_eq!(field_detail.field_violations[2].description, "required");
        assert_eq!(field_detail.field_violations[3].field, "field");
        assert_eq!(
            field_detail.field_violations[3].description,
            "not canonical"
        );
        assert_eq!(
            field_detail.field_violations[4].description.len(),
            super::MAX_ERROR_DETAIL_STRING_BYTES
        );
        assert!(
            field_detail.field_violations[4]
                .description
                .chars()
                .all(|ch| !ch.is_control())
        );
    }

    #[test]
    fn quota_refusal_status_is_resource_exhausted_without_retry() {
        let status = quota_refusal_status(
            "storage",
            "tenant_storage_quota",
            "tenant storage quota exceeded",
        );
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.backend, "storage");
        assert_eq!(detail.operation, "tenant_storage_quota");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
    }

    #[test]
    fn reject_oversized_object_carries_typed_quota_detail() {
        let status =
            reject_oversized_object(INLINE_OBJECT_LIMIT_BYTES + 1).expect_err("oversized object");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert_eq!(detail.backend, "object");
        assert_eq!(detail.operation, "inline_object_size");
        assert!(!detail.retryable);
    }

    #[test]
    fn invalid_argument_fields_carries_structured_field_violations() {
        let status = invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        );
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "tenant_id is required");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "tenant_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty tenant id"
        );
    }

    #[test]
    fn failed_precondition_fields_carries_validation_detail_without_changing_code() {
        let status = failed_precondition_fields(
            "uploaded object etag does not match",
            [("etag", "must match the uploaded object's store ETag")],
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "uploaded object etag does not match");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "etag");
        assert_eq!(
            detail.field_violations[0].description,
            "must match the uploaded object's store ETag"
        );
    }

    #[test]
    fn prefix_status_preserves_typed_error_detail_metadata() {
        let status = invalid_argument_fields(
            "tenant_id is required",
            [("tenant_id", "must be a non-empty tenant id")],
        );
        let prefixed = prefix_status("create user failed", status);
        assert_eq!(prefixed.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            prefixed.message(),
            "create user failed: tenant_id is required"
        );
        let detail = decode_detail(&prefixed);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "tenant_id");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty tenant id"
        );
    }

    fn assert_single_field_violation(status: &tonic::Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert!(!detail.retryable);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    #[test]
    fn shared_json_validation_helpers_carry_field_violations() {
        assert_single_field_violation(
            &json_i64(&json!("abc")).expect_err("invalid integer"),
            "value",
            "must be an integer or integer string",
        );
        assert_single_field_violation(
            &json_f64(&json!("abc")).expect_err("invalid number"),
            "value",
            "must be a number or numeric string",
        );
        assert_single_field_violation(
            &json_required_str(&json!({}), "collection").expect_err("missing string"),
            "collection",
            "must be a non-empty string",
        );
        assert_single_field_violation(
            &json_required_f32_vec(&json!({"vector": []}), "vector").expect_err("empty vector"),
            "vector",
            "must not be empty",
        );
        assert_single_field_violation(
            &json_required_f32_vec(&json!({"vector": ["nan"]}), "vector")
                .expect_err("non-numeric vector"),
            "vector",
            "must contain only numbers",
        );
    }

    #[test]
    fn protobuf_numbers_recover_only_safe_integer_semantics() {
        use prost_types::value::Kind;

        let value = |number| ProstValue {
            kind: Some(Kind::NumberValue(number)),
        };

        assert_eq!(prost_value_to_json(&value(0.0)), json!(0));
        assert_eq!(
            prost_value_to_json(&value(9_007_199_254_740_991.0)),
            json!(9_007_199_254_740_991_i64)
        );
        assert_eq!(prost_value_to_json(&value(1.5)), json!(1.5));
        assert!(
            prost_value_to_json(&value(9_007_199_254_740_992.0))
                .as_i64()
                .is_none(),
            "an unsafe f64 must not be reinterpreted as an exact integer"
        );
    }

    #[test]
    fn shared_dispatch_validation_helpers_carry_field_violations() {
        assert_single_field_violation(
            &object_bytes_from_json(&json!({"data_base64": "!"}))
                .expect_err("invalid object base64"),
            "data_base64",
            "must be valid base64 object bytes",
        );
        assert_single_field_violation(
            &object_bytes_from_json(&json!({})).expect_err("missing object bytes"),
            "object_bytes",
            "data_base64, content_base64, data_text, or content_text is required",
        );
        assert_single_field_violation(
            &validate_identifier("1bad", "schema").expect_err("invalid identifier"),
            "schema",
            "must be a valid SQL identifier",
        );
        assert_single_field_violation(
            &parse_sql_dispatch("{").expect_err("invalid dispatch json"),
            "request_json",
            "must be valid dispatch JSON",
        );
        assert_single_field_violation(
            &parse_sql_dispatch(r#"{"params":[]}"#).expect_err("missing sql"),
            "sql",
            "dispatch request must include sql",
        );
        assert_single_field_violation(
            &reject_plan(&["missing field".to_string()]).expect_err("plan errors"),
            "plan",
            "planner validation errors must be resolved",
        );
    }

    #[test]
    fn compile_error_status_carries_code_as_schema_kind() {
        let status = compile_error_status(
            "mssql",
            "mutate",
            "operation_not_supported",
            "compile failed",
        );
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(&status);
        assert_eq!(detail.capability_required, "operation_not_supported");
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert!(!detail.retryable);
    }

    #[test]
    fn schema_status_carries_schema_kind_without_changing_code() {
        let status = schema_status(
            tonic::Code::FailedPrecondition,
            "catalog",
            "LookupMessageSchema",
            "catalog_version_incompatible",
            "client catalog version is incompatible",
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(status.message(), "client catalog version is incompatible");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "catalog");
        assert_eq!(detail.operation, "LookupMessageSchema");
        assert_eq!(detail.capability_required, "catalog_version_incompatible");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn internal_status_carries_internal_kind_with_identity() {
        let status = internal_status("pinecone", "http_418", "pinecone 418: teapot");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "pinecone 418: teapot");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, "http_418");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.capability_required.is_empty());
    }

    #[test]
    fn sqlx_transient_transport_error_maps_to_retryable_unavailable() {
        // Pool/connection loss is an infrastructure outage, not a logic error, so
        // it must be a retryable UNAVAILABLE — an outage can never masquerade as a
        // bad credential or a server bug (UDB-DB-READINESS-001).
        // Callers pass a snake_case operation token, matching every real call site.
        for err in [sqlx::Error::PoolClosed, sqlx::Error::PoolTimedOut] {
            let status = sqlx_error_to_status("pool_acquire", &err);
            assert_eq!(status.code(), tonic::Code::Unavailable, "{err:?}");
            let detail = decode_detail(&status);
            assert_eq!(detail.kind, ErrorKind::Retryable as i32, "{err:?}");
            assert_eq!(detail.backend, "database");
            assert_eq!(detail.operation, "pool_acquire");
            assert!(detail.retryable, "{err:?}");
            assert!(detail.retry_after_ms > 0, "{err:?}");
            assert!(detail.field_violations.is_empty());
        }
    }

    // `operation` is a STABLE MACHINE TOKEN, not prose: the sanitizer collapses
    // whitespace into `_` so a consumer can switch on it. Pinned here because the
    // transient-transport path forwards its caller-supplied context straight into
    // that field, and prose there would silently become an unstable token.
    #[test]
    fn error_detail_operation_is_normalized_to_a_token() {
        let status = sqlx_error_to_status("pool acquire failed", &sqlx::Error::PoolClosed);
        assert_eq!(decode_detail(&status).operation, "pool_acquire_failed");
    }

    #[test]
    fn sqlx_non_transient_non_database_error_preserves_internal_code_with_detail() {
        // A code/schema-level non-database error (NOT a transport outage) stays a
        // non-retryable Internal so real bugs are not silently marked retryable.
        let err = sqlx::Error::ColumnNotFound("missing_col".to_string());
        let status = sqlx_error_to_status("decode row", &err);
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "decode row");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "database");
        assert_eq!(detail.operation, "decode row");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn untagged_store_string_preserves_internal_code_with_detail() {
        let status = status_from_store_string("native store decode failed".to_string());
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "native store decode failed");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "store");
        assert_eq!(detail.operation, "string_status");
        assert!(!detail.retryable);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn unique_constraint_status_preserves_already_exists_with_schema_detail() {
        let status = schema_status(
            tonic::Code::AlreadyExists,
            "database",
            "unique_constraint",
            "unique_violation",
            "resource already exists",
        );
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        assert_eq!(status.message(), "resource already exists");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "database");
        assert_eq!(detail.operation, "unique_constraint");
        assert_eq!(detail.capability_required, "unique_violation");
        assert!(!detail.retryable);
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    #[test]
    fn http_status_to_tonic_conflict_carries_schema_detail() {
        let status = http_status_to_tonic(reqwest::StatusCode::CONFLICT, "exists", "pinecone");
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        assert_eq!(status.message(), "pinecone 409: exists");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, "request");
        assert_eq!(detail.capability_required, "backend_http_conflict");
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    #[test]
    fn http_status_to_tonic_not_found_carries_schema_detail() {
        let status = http_status_to_tonic(reqwest::StatusCode::NOT_FOUND, "missing", "pinecone");
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "pinecone 404: missing");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, "request");
        assert_eq!(detail.capability_required, "backend_http_not_found");
    }

    #[cfg(any(feature = "pinecone", feature = "weaviate", feature = "elasticsearch"))]
    #[test]
    fn http_status_to_tonic_unexpected_status_carries_internal_detail() {
        let status = http_status_to_tonic(reqwest::StatusCode::IM_A_TEAPOT, "teapot", "pinecone");
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "pinecone 418: teapot");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, "http_418");
        assert!(!detail.retryable);
    }

    #[test]
    fn referential_constraint_status_carries_schema_detail() {
        let status = referential_constraint_status();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            status.message(),
            "operation violates a referential constraint"
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "database");
        assert_eq!(detail.operation, "referential_constraint");
        assert_eq!(detail.capability_required, "foreign_key_violation");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn tagged_store_invalid_argument_preserves_validation_detail() {
        let status = status_from_store_string(format!(
            "{STATUS_TAG_PREFIX}{}:required field 'email' is missing",
            tonic::Code::InvalidArgument as i32
        ));
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(status.message(), "required field 'email' is missing");
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "database");
        assert!(!detail.retryable);
    }

    #[test]
    fn tagged_store_referential_constraint_preserves_schema_detail() {
        let status = status_from_store_string(format!(
            "{STATUS_TAG_PREFIX}{}:operation violates a referential constraint",
            tonic::Code::FailedPrecondition as i32
        ));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "database");
        assert_eq!(detail.operation, "referential_constraint");
        assert_eq!(detail.capability_required, "foreign_key_violation");
    }

    #[test]
    fn tagged_store_already_exists_preserves_schema_detail() {
        let status = status_from_store_string(format!(
            "{STATUS_TAG_PREFIX}{}:resource already exists",
            tonic::Code::AlreadyExists as i32
        ));
        assert_eq!(status.code(), tonic::Code::AlreadyExists);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "database");
        assert_eq!(detail.operation, "unique_constraint");
        assert_eq!(detail.capability_required, "unique_violation");
    }

    #[test]
    fn tagged_store_unavailable_preserves_retryable_detail() {
        let status = status_from_store_string(format!(
            "{STATUS_TAG_PREFIX}{}:backend temporarily unavailable (not primary)",
            tonic::Code::Unavailable as i32
        ));
        assert_eq!(status.code(), tonic::Code::Unavailable);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Retryable as i32);
        assert_eq!(detail.backend, "store");
        assert_eq!(detail.operation, "tagged_status");
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, HTTP_RETRYABLE_BACKOFF_MS);
    }
}

// ── RecordBatchV2 conversion tests (A.4) ──────────────────────────────────────

#[cfg(test)]
mod record_batch_tests {
    use super::{record_batch_v2_from_json_rows, record_batch_v2_from_record_set};
    use crate::proto::{ColumnType, RecordBatchV2, RecordSet};
    use serde_json::json;

    fn col<'a>(batch: &'a RecordBatchV2, name: &str) -> &'a crate::proto::ColumnBatch {
        batch
            .columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} present"))
    }

    #[test]
    fn infers_typed_columns_with_nulls_bytes_and_json_fallback() {
        let rows = vec![
            json!({"id": 1, "name": "alice", "active": true, "score": 9.5,
                   "blob": "base64:aGk=", "tags": ["a","b"], "maybe": null}),
            json!({"id": 2, "name": "bob", "active": false, "score": 3.0,
                   "blob": "base64:Ynll", "tags": ["c"], "maybe": 7}),
        ];
        let batch = record_batch_v2_from_json_rows(&rows, "v3", "tok".into(), 2);

        assert_eq!(batch.row_count, 2);
        assert_eq!(batch.schema_version, "v3");
        assert_eq!(batch.next_page_token, "tok");
        assert_eq!(batch.total_count, 2);
        // field_order carries every key (order-independent: serde_json Map is sorted).
        let mut got = batch.field_order.clone();
        got.sort();
        assert_eq!(
            got,
            vec!["active", "blob", "id", "maybe", "name", "score", "tags"]
        );

        let id = col(&batch, "id");
        assert_eq!(id.r#type, ColumnType::Int64 as i32);
        assert_eq!(id.int64_values, vec![1, 2]);
        assert_eq!(id.nulls, vec![false, false]);

        assert_eq!(col(&batch, "name").r#type, ColumnType::String as i32);
        assert_eq!(col(&batch, "name").string_values, vec!["alice", "bob"]);

        assert_eq!(col(&batch, "active").r#type, ColumnType::Bool as i32);
        assert_eq!(col(&batch, "active").bool_values, vec![true, false]);

        assert_eq!(col(&batch, "score").r#type, ColumnType::Double as i32);
        assert_eq!(col(&batch, "score").double_values, vec![9.5, 3.0]);

        // bytes detected via the `base64:` cell prefix and decoded.
        let blob = col(&batch, "blob");
        assert_eq!(blob.r#type, ColumnType::Bytes as i32);
        assert_eq!(blob.bytes_values, vec![b"hi".to_vec(), b"bye".to_vec()]);

        // nested arrays fall back to a JSON-encoded string column.
        let tags = col(&batch, "tags");
        assert_eq!(tags.r#type, ColumnType::Json as i32);
        assert_eq!(tags.json_values[0], "[\"a\",\"b\"]");

        // a single scalar category (int) with a NULL → Int64 + null bitmap.
        let maybe = col(&batch, "maybe");
        assert_eq!(maybe.r#type, ColumnType::Int64 as i32);
        assert_eq!(maybe.nulls, vec![true, false]);
        assert_eq!(maybe.int64_values, vec![0, 7]);
    }

    #[test]
    fn all_null_column_is_null_typed() {
        let rows = vec![json!({"x": null}), json!({"x": null})];
        let batch = record_batch_v2_from_json_rows(&rows, "", String::new(), 0);
        let x = col(&batch, "x");
        assert_eq!(x.r#type, ColumnType::Null as i32);
        assert_eq!(x.nulls, vec![true, true]);
        assert!(x.int64_values.is_empty());
    }

    #[test]
    fn mixed_scalar_kinds_fall_back_to_json() {
        let rows = vec![json!({"v": true}), json!({"v": "hello"})];
        let batch = record_batch_v2_from_json_rows(&rows, "", String::new(), 0);
        let v = col(&batch, "v");
        assert_eq!(v.r#type, ColumnType::Json as i32);
        assert_eq!(v.json_values, vec!["true", "\"hello\""]);
    }

    #[test]
    fn from_record_set_uses_records_json() {
        let blob = serde_json::to_vec(&json!({"a": 1})).unwrap();
        let set = RecordSet {
            records_json: vec![blob],
            total_count: 1,
            next_page_token: "n".into(),
            ..RecordSet::default()
        };
        let batch = record_batch_v2_from_record_set(&set, "v1");
        assert_eq!(batch.row_count, 1);
        assert_eq!(batch.total_count, 1);
        assert_eq!(batch.next_page_token, "n");
        assert_eq!(col(&batch, "a").int64_values, vec![1]);
    }

    /// A.5 — compile-time proof that the selected bytes fields are generated as
    /// `bytes::Bytes` (not `Vec<u8>`). Fails to compile if the build.rs
    /// `.bytes([...])` config regresses.
    #[test]
    fn selected_proto_bytes_fields_are_bytes_type() {
        let chunk = crate::proto::Chunk::default();
        let _: bytes::Bytes = chunk.data;
        let row = crate::proto::ProjectionDriftDivergentRow::default();
        let _: bytes::Bytes = row.row_key_json;
    }

    /// A.7 — the V2 columnar frames must carry exactly the same logical values
    /// as the V1 `RecordSet`, and must NOT resurrect any field the V1 serializer
    /// masked out. Because the converter reads the already-masked `records_json`,
    /// a field masked away (absent from the blob) can never appear in V2.
    #[test]
    fn v2_batch_matches_v1_rows_and_inherits_masking() {
        // The V1 serializer masked "ssn" out, so it is absent from the blob.
        let blob = serde_json::to_vec(&json!({"id": 1, "email": "a@b.com"})).unwrap();
        let set = RecordSet {
            records_json: vec![blob],
            total_count: 1,
            ..RecordSet::default()
        };
        let batch = record_batch_v2_from_record_set(&set, "v9");

        let mut names: Vec<String> = batch.columns.iter().map(|c| c.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["email", "id"], "V2 columns mirror the V1 row");
        assert!(
            !batch.field_order.contains(&"ssn".to_string()),
            "a field masked out of records_json must not leak into the V2 batch"
        );
        assert_eq!(col(&batch, "id").int64_values, vec![1]);
        assert_eq!(col(&batch, "email").string_values, vec!["a@b.com"]);
        assert_eq!(batch.schema_version, "v9");
    }
}

// ── merge_context scope-authority guards (01.1.1.1 / 01.1.2.1 / 01.1.3.1) ─────
//
// `merge_context` is the single authority boundary for `RequestContext.scopes`.
// Both downstream gates that 01.1.2.1 (PII unmask in `rows_to_record_set`) and
// 01.1.3.1 (the materialized-view `udb:admin` gate) protect read `context.scopes`
// AFTER this merge — so proving body scopes never survive the merge proves neither
// sink can be satisfied by caller-supplied body scopes. These tests fail if
// 01.1.1.1 is reverted (i.e. body scopes once again override metadata scopes).
#[cfg(test)]
mod merge_context_scope_authority_tests {
    use super::{ProtoRequestContext, RequestContext, cache_key, merge_context};
    use serde_json::json;

    fn metadata_ctx(scopes: &[&str]) -> RequestContext {
        RequestContext {
            tenant_id: "tenant-a".into(),
            project_id: "proj-a".into(),
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            ..RequestContext::default()
        }
    }

    fn proto_ctx(scopes: &[&str]) -> ProtoRequestContext {
        ProtoRequestContext {
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
            ..ProtoRequestContext::default()
        }
    }

    fn can_unmask_pii(context: &RequestContext) -> bool {
        context
            .scopes
            .iter()
            .any(|scope| scope == "udb:pii:read" || scope == "udb:*" || scope == "*")
    }

    #[test]
    fn body_scopes_never_override_metadata_scopes() {
        // Caller smuggles privileged scopes in the per-op proto body; the verified
        // metadata side carries none of them.
        let proto = proto_ctx(&["udb:pii:read", "udb:admin", "*"]);
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(
            merged.scopes,
            vec!["udb:read".to_string()],
            "body scopes must not reach the merged RequestContext"
        );
        // The PII-unmask predicate (rows_to_record_set) and the MV admin gate both
        // read exactly this vector, so neither can be satisfied by the body.
        assert!(!can_unmask_pii(&merged));
        assert!(!merged.scopes.iter().any(|s| s == "udb:admin" || s == "*"));
    }

    #[test]
    fn body_scopes_cannot_elevate_or_unmask_when_metadata_has_no_scopes() {
        let proto = proto_ctx(&["udb:pii:read", "udb:admin", "*"]);
        let merged = merge_context(Some(&proto), metadata_ctx(&[]));

        assert!(
            merged.scopes.is_empty(),
            "metadata scopes are authoritative even when empty"
        );
        assert!(!can_unmask_pii(&merged));
        assert!(!merged.scopes.iter().any(|scope| scope == "udb:admin"));
    }

    #[test]
    fn empty_body_scopes_inherit_metadata_scopes() {
        let proto = proto_ctx(&[]);
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read", "udb:admin"]));
        assert_eq!(
            merged.scopes,
            vec!["udb:read".to_string(), "udb:admin".to_string()]
        );
    }

    #[test]
    fn metadata_read_fence_wins_over_body_read_fence() {
        // R3.2: the read fence follows the same metadata-wins precedence as the
        // other authority/freshness fields. When the verified metadata header
        // pins a fence, a per-op proto body fence must NOT override it.
        let mut meta = metadata_ctx(&["udb:read"]);
        meta.read_fence_json = r#"{"seq":42}"#.into();
        let proto = ProtoRequestContext {
            read_fence_json: r#"{"seq":1}"#.into(),
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), meta);
        assert_eq!(
            merged.read_fence_json, r#"{"seq":42}"#,
            "metadata read fence must win over a body-supplied read fence"
        );
    }

    #[test]
    fn body_read_fence_used_only_when_metadata_absent() {
        // When the transport pinned no fence, the per-op proto body fence is the
        // honored fallback (so a body-only fence is not silently dropped).
        let proto = ProtoRequestContext {
            read_fence_json: r#"{"seq":7}"#.into(),
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(merged.read_fence_json, r#"{"seq":7}"#);
    }

    #[test]
    fn typed_body_read_fence_used_only_when_metadata_absent() {
        let proto = ProtoRequestContext {
            read_fence: Some(crate::proto::ReadFence {
                min_outbox_lsn: "0/9".into(),
                projection_task_ids: Vec::new(),
                max_wait_ms: 500,
            }),
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(
            merged.read_fence_json,
            r#"{"min_outbox_lsn":"0/9","max_wait_ms":500}"#
        );
    }

    #[test]
    fn metadata_read_fence_wins_over_typed_body_read_fence() {
        let mut meta = metadata_ctx(&["udb:read"]);
        meta.read_fence_json = r#"{"min_outbox_lsn":"0/metadata"}"#.into();
        let proto = ProtoRequestContext {
            read_fence: Some(crate::proto::ReadFence {
                min_outbox_lsn: "0/body".into(),
                projection_task_ids: Vec::new(),
                max_wait_ms: 500,
            }),
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), meta);
        assert_eq!(merged.read_fence_json, r#"{"min_outbox_lsn":"0/metadata"}"#);
    }

    #[test]
    fn legacy_body_read_fence_json_wins_over_typed_body_read_fence() {
        let proto = ProtoRequestContext {
            read_fence_json: r#"{"min_outbox_lsn":"0/legacy"}"#.into(),
            read_fence: Some(crate::proto::ReadFence {
                min_outbox_lsn: "0/typed".into(),
                projection_task_ids: Vec::new(),
                max_wait_ms: 500,
            }),
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(merged.read_fence_json, r#"{"min_outbox_lsn":"0/legacy"}"#);
    }

    #[test]
    fn typed_body_consistency_used_only_when_metadata_absent() {
        let proto = ProtoRequestContext {
            consistency_mode: 2,
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(merged.consistency, "read_your_writes");
    }

    #[test]
    fn legacy_body_consistency_string_wins_over_typed_body_consistency() {
        let proto = ProtoRequestContext {
            consistency: "eventual".into(),
            consistency_mode: 2,
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), metadata_ctx(&["udb:read"]));
        assert_eq!(merged.consistency, "eventual");
    }

    #[test]
    fn metadata_consistency_wins_over_body_consistency_fields() {
        let mut meta = metadata_ctx(&["udb:read"]);
        meta.consistency = "strong".into();
        let proto = ProtoRequestContext {
            consistency: "eventual".into(),
            consistency_mode: 2,
            ..ProtoRequestContext::default()
        };
        let merged = merge_context(Some(&proto), meta);
        assert_eq!(merged.consistency, "strong");
    }

    #[test]
    fn cache_key_is_independent_of_body_scopes() {
        // Same request, once with hostile body scopes and once without: the cache
        // key (which folds in context.scopes) must be identical, proving body
        // scopes can neither widen access nor poison/segment the cache.
        let with_body = merge_context(
            Some(&proto_ctx(&["udb:pii:read"])),
            metadata_ctx(&["udb:read"]),
        );
        let without_body = merge_context(Some(&proto_ctx(&[])), metadata_ctx(&["udb:read"]));
        let filter = json!({"eq": {"id": 1}});
        let fields = vec!["id".to_string(), "email".to_string()];
        let key_with = cache_key(
            "select",
            "udb.Person",
            &with_body,
            "chk",
            &filter,
            &fields,
            100,
            "id:false",
        );
        let key_without = cache_key(
            "select",
            "udb.Person",
            &without_body,
            "chk",
            &filter,
            &fields,
            100,
            "id:false",
        );
        assert_eq!(
            key_with, key_without,
            "cache_key must not depend on caller-supplied body scopes"
        );
        // X-1: limit and sort ARE part of the key — differing values must not collide.
        let key_limit_1000 = cache_key(
            "select",
            "udb.Person",
            &with_body,
            "chk",
            &filter,
            &fields,
            1000,
            "id:false",
        );
        let key_sort_desc = cache_key(
            "select",
            "udb.Person",
            &with_body,
            "chk",
            &filter,
            &fields,
            100,
            "id:true",
        );
        assert_ne!(
            key_with, key_limit_1000,
            "different LIMIT must not share a cache entry"
        );
        assert_ne!(
            key_with, key_sort_desc,
            "different sort direction must not share a cache entry"
        );
    }
}
