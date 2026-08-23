//! Typed errors and retry policy.
//!
//! The broker attaches a structured [`ErrorDetail`] to failures in the
//! `udb-error-detail-bin` trailer. Without decoding it a caller sees only a gRPC
//! code and a message, and has to pattern-match on error strings to learn whether
//! a failure is retryable, which capability was missing, or which field was
//! rejected — exactly the things the broker already said precisely.

use std::time::Duration;

use tonic::{Code, Status};

use crate::proto::udb::entity::v1::ErrorDetail;

/// The metadata key the broker uses. The `-bin` suffix tells gRPC the value is
/// raw bytes rather than ASCII, so tonic base64-decodes it for us.
pub const ERROR_DETAIL_TRAILER: &str = "udb-error-detail-bin";

/// A `Status` plus the broker's structured detail, when it sent one.
#[derive(Debug, Clone)]
pub struct UdbError {
    pub status: Status,
    pub detail: Option<ErrorDetail>,
}

impl UdbError {
    /// Decode the trailer, if present and well-formed.
    ///
    /// A malformed trailer is treated as absent rather than as a new failure: the
    /// call already failed and the caller needs the original status, not a
    /// decoding complaint layered over it.
    pub fn from_status(status: Status) -> Self {
        let detail = status
            .metadata()
            .get_bin(ERROR_DETAIL_TRAILER)
            .and_then(|v| v.to_bytes().ok())
            .and_then(|bytes| <ErrorDetail as prost::Message>::decode(bytes).ok());
        Self { status, detail }
    }

    pub fn code(&self) -> Code {
        self.status.code()
    }

    pub fn message(&self) -> &str {
        self.status.message()
    }

    /// Whether the BROKER said this is retryable.
    ///
    /// Trusted over any code-based guess: the broker knows whether it got far
    /// enough to have side effects, and a client cannot infer that from
    /// `UNAVAILABLE` alone.
    pub fn is_retryable(&self) -> bool {
        match &self.detail {
            Some(d) => d.retryable,
            // No detail: fall back to codes that cannot have applied a mutation.
            None => matches!(
                self.status.code(),
                Code::Unavailable | Code::ResourceExhausted
            ),
        }
    }

    /// How long the broker asked us to wait, if it said.
    pub fn retry_after(&self) -> Option<Duration> {
        let ms = self.detail.as_ref()?.retry_after_ms;
        (ms > 0).then(|| Duration::from_millis(ms as u64))
    }

    /// The capability the deployment is missing, for a capability refusal.
    pub fn capability_required(&self) -> Option<&str> {
        let c = self.detail.as_ref()?.capability_required.as_str();
        (!c.is_empty()).then_some(c)
    }

    /// The correlation id to quote in a bug report or a support ticket.
    pub fn correlation_id(&self) -> Option<&str> {
        let c = self.detail.as_ref()?.correlation_id.as_str();
        (!c.is_empty()).then_some(c)
    }

    /// Per-field rejections, for a validation failure.
    pub fn field_violations(&self) -> &[crate::proto::udb::entity::v1::ErrorFieldViolation] {
        self.detail
            .as_ref()
            .map(|d| d.field_violations.as_slice())
            .unwrap_or_default()
    }
}

impl std::fmt::Display for UdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status.code(), self.status.message())?;
        if let Some(detail) = &self.detail {
            if !detail.backend.is_empty() {
                write!(f, " [backend={}", detail.backend)?;
                if !detail.operation.is_empty() {
                    write!(f, " op={}", detail.operation)?;
                }
                write!(f, "]")?;
            }
            if !detail.capability_required.is_empty() {
                write!(f, " (requires {})", detail.capability_required)?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for UdbError {}

impl From<Status> for UdbError {
    fn from(status: Status) -> Self {
        Self::from_status(status)
    }
}

/// Per-call deadline and retry policy.
///
/// Retries are bounded and only ever applied where the caller has said the
/// operation is safe to repeat — see [`CallPolicy::idempotent`].
#[derive(Debug, Clone, Copy)]
pub struct CallPolicy {
    pub deadline: Option<Duration>,
    pub max_attempts: u32,
    pub base_backoff: Duration,
    /// Whether the operation may be safely repeated. Defaults to `false`: a
    /// blanket retry on a mutation is how one payment becomes two.
    pub idempotent: bool,
}

impl Default for CallPolicy {
    fn default() -> Self {
        Self {
            deadline: Some(Duration::from_secs(30)),
            max_attempts: 3,
            base_backoff: Duration::from_millis(100),
            idempotent: false,
        }
    }
}

impl CallPolicy {
    /// A read: safe to repeat.
    pub fn idempotent() -> Self {
        Self {
            idempotent: true,
            ..Default::default()
        }
    }

    /// The policy the CONTRACT implies for one RPC path.
    ///
    /// This replaced a hand-written per-method judgement, which was wrong in both
    /// directions: it refused to retry `Upsert`, `Update` and `Delete` even
    /// though the broker declares them replayable, costing availability on a
    /// transient failure the broker was happy to see again.
    ///
    /// Deciding from the descriptor instead of a method name is the point of the
    /// `operation_kind` annotation. An unknown path yields the conservative
    /// default (no retries).
    pub fn from_contract(path: &str) -> Self {
        if crate::generated_rpcs::is_retry_safe(path) {
            Self::idempotent()
        } else {
            Self::default()
        }
    }

    /// No retries at all.
    pub fn once() -> Self {
        Self {
            max_attempts: 1,
            ..Default::default()
        }
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Backoff before `attempt` (1-based), honouring the broker's `retry_after`
    /// when it gave one.
    pub(crate) fn backoff_for(&self, attempt: u32, err: &UdbError) -> Duration {
        if let Some(after) = err.retry_after() {
            return after;
        }
        // Exponential, capped. No jitter here: callers that need decorrelated
        // retries across a fleet should drive their own loop, and inventing
        // randomness inside a client makes failures harder to reproduce.
        let exp = self
            .base_backoff
            .saturating_mul(1u32 << attempt.min(6).saturating_sub(1));
        exp.min(Duration::from_secs(5))
    }

    /// Whether another attempt is permitted.
    pub(crate) fn should_retry(&self, attempt: u32, err: &UdbError) -> bool {
        self.idempotent && attempt < self.max_attempts && err.is_retryable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_without_detail(code: Code) -> UdbError {
        UdbError::from_status(Status::new(code, "boom"))
    }

    #[test]
    fn missing_trailer_falls_back_to_safe_codes() {
        assert!(status_without_detail(Code::Unavailable).is_retryable());
        assert!(status_without_detail(Code::ResourceExhausted).is_retryable());
        assert!(
            !status_without_detail(Code::InvalidArgument).is_retryable(),
            "a rejected argument will be rejected again"
        );
        assert!(
            !status_without_detail(Code::Internal).is_retryable(),
            "INTERNAL may have applied a mutation; never assume repeatable"
        );
    }

    #[test]
    fn malformed_trailer_is_treated_as_absent() {
        let mut status = Status::new(Code::Internal, "boom");
        status.metadata_mut().insert_bin(
            ERROR_DETAIL_TRAILER,
            tonic::metadata::MetadataValue::from_bytes(b"not-a-protobuf-at-all"),
        );
        let err = UdbError::from_status(status);
        // Decoding failure must not mask the original status.
        assert_eq!(err.code(), Code::Internal);
        assert_eq!(err.message(), "boom");
    }

    #[test]
    fn broker_detail_overrides_the_code_guess() {
        let detail = ErrorDetail {
            retryable: true,
            retry_after_ms: 250,
            capability_required: "postgres_backend".into(),
            correlation_id: "corr-9".into(),
            ..Default::default()
        };
        let mut status = Status::new(Code::Internal, "boom");
        let mut buf = Vec::new();
        prost::Message::encode(&detail, &mut buf).expect("encode");
        status.metadata_mut().insert_bin(
            ERROR_DETAIL_TRAILER,
            tonic::metadata::MetadataValue::from_bytes(&buf),
        );

        let err = UdbError::from_status(status);
        assert!(
            err.is_retryable(),
            "INTERNAL, but the broker said retryable"
        );
        assert_eq!(err.retry_after(), Some(Duration::from_millis(250)));
        assert_eq!(err.capability_required(), Some("postgres_backend"));
        assert_eq!(err.correlation_id(), Some("corr-9"));
    }

    #[test]
    fn mutations_are_not_retried_by_default() {
        let err = status_without_detail(Code::Unavailable);
        assert!(
            !CallPolicy::default().should_retry(1, &err),
            "default policy must not repeat a possible mutation"
        );
        assert!(CallPolicy::idempotent().should_retry(1, &err));
    }

    #[test]
    fn retry_stops_at_max_attempts() {
        let err = status_without_detail(Code::Unavailable);
        let policy = CallPolicy::idempotent().with_max_attempts(2);
        assert!(policy.should_retry(1, &err));
        assert!(!policy.should_retry(2, &err), "attempt 2 of 2 is the last");
    }

    #[test]
    fn backoff_prefers_the_brokers_retry_after() {
        let detail = ErrorDetail {
            retryable: true,
            retry_after_ms: 1234,
            ..Default::default()
        };
        let err = UdbError {
            status: Status::new(Code::Unavailable, "boom"),
            detail: Some(detail),
        };
        assert_eq!(
            CallPolicy::idempotent().backoff_for(3, &err),
            Duration::from_millis(1234)
        );
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let err = status_without_detail(Code::Unavailable);
        let p = CallPolicy::idempotent();
        assert_eq!(p.backoff_for(1, &err), Duration::from_millis(100));
        assert_eq!(p.backoff_for(2, &err), Duration::from_millis(200));
        assert_eq!(p.backoff_for(3, &err), Duration::from_millis(400));
        assert!(p.backoff_for(20, &err) <= Duration::from_secs(5));
    }
}
