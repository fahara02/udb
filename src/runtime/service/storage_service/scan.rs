//! Content-scan verdict gate (V050-3).
//!
//! A consumer accepting user-supplied files had no authoritative place to record
//! "this was scanned and is clean", so it could not gate a download on it. Worse,
//! whatever gate it built in its own application was bypassed by a direct native
//! `DownloadFile`, which reads the bytes straight from the object store.
//!
//! The verdict lives on the `File` row, is written only by `SetScanVerdict`
//! (privileged scanner scope), and is checked here by BOTH download paths.

use tonic::Status;

use super::config::{SCAN_OVERRIDE_SCOPE, SCAN_VERDICT_NOT_CLEAN, storage_require_clean_scan};
use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;
use crate::runtime::service::method_security::current_claim_context;

/// True when the caller holds the explicit override scope.
///
/// Read from the VERIFIED principal, never from the request, so a client cannot
/// grant itself quarantine access by asking for it.
fn caller_may_override() -> bool {
    let ctx = current_claim_context();
    ctx.scopes
        .iter()
        .any(|scope| scope.trim() == SCAN_OVERRIDE_SCOPE)
}

/// Refuse a download whose scan verdict does not permit it.
///
/// Two rules, and they are deliberately different:
///
/// * INFECTED is refused whether or not enforcement is enabled. Something looked
///   at these bytes and called them malicious; serving them anyway is not a
///   configurable choice.
/// * Anything else that is not CLEAN — never scanned, scan pending, scan failed
///   — is refused only when the operator has enabled enforcement. Refusing by
///   default would deny every file that predates scanning the moment the column
///   appeared, which is an outage, not a security improvement.
///
/// The override scope reaches both, so quarantine review and incident response
/// stay possible.
pub(crate) fn enforce_scan_gate(
    verdict: i32,
    file_id: &str,
    operation: &'static str,
) -> Result<(), Status> {
    decide_scan_gate(
        verdict,
        file_id,
        operation,
        storage_require_clean_scan(),
        caller_may_override(),
    )
}

/// The decision itself, with both ambient inputs - the enforcement flag and the
/// caller's override scope - passed in. Keeping them as parameters means the
/// rules are testable without mutating process env or installing a claim
/// context, which is what let the table below be exercised exhaustively.
pub(crate) fn decide_scan_gate(
    verdict: i32,
    file_id: &str,
    operation: &'static str,
    require_clean: bool,
    may_override: bool,
) -> Result<(), Status> {
    use storage_entity_pb::ScanVerdict as V;
    let decoded = V::try_from(verdict).unwrap_or(V::Unspecified);
    if matches!(decoded, V::Clean) {
        return Ok(());
    }
    let infected = matches!(decoded, V::Infected);
    if !infected && !require_clean {
        return Ok(());
    }
    if may_override {
        tracing::warn!(
            file_id = %file_id,
            verdict = ?decoded,
            operation,
            "scan gate overridden by an explicitly scoped caller"
        );
        return Ok(());
    }
    Err(super::errors::storage_scan_verdict_status(
        operation,
        SCAN_VERDICT_NOT_CLEAN,
        decoded,
    ))
}

/// The identity credited with a verdict: the verified service identity, falling
/// back to the subject. Never the request body — a scanner must not be able to
/// file a verdict under another scanner's name.
pub(crate) fn verified_scanner_identity() -> String {
    let ctx = current_claim_context();
    if !ctx.service_identity.trim().is_empty() {
        return ctx.service_identity.trim().to_string();
    }
    ctx.subject.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::decide_scan_gate;
    use crate::proto::udb::core::storage::entity::v1::ScanVerdict as V;

    fn allowed(verdict: V, require_clean: bool, may_override: bool) -> bool {
        decide_scan_gate(
            verdict as i32,
            "f-1",
            "download_file",
            require_clean,
            may_override,
        )
        .is_ok()
    }

    /// The whole rule set, stated as a table so the asymmetry is explicit:
    /// INFECTED does not depend on the operator flag, everything else does.
    #[test]
    fn scan_gate_rules() {
        // CLEAN always passes.
        assert!(allowed(V::Clean, false, false));
        assert!(allowed(V::Clean, true, false));

        // INFECTED is refused even with enforcement OFF. Something read these
        // bytes and called them malicious; serving them is not a config choice.
        assert!(!allowed(V::Infected, false, false));
        assert!(!allowed(V::Infected, true, false));

        // Not-yet-scanned states pass while enforcement is off, so enabling the
        // column does not deny every pre-existing file at upgrade.
        for verdict in [V::Unspecified, V::Pending, V::Failed] {
            assert!(
                allowed(verdict, false, false),
                "{verdict:?} must pass while enforcement is off"
            );
            assert!(
                !allowed(verdict, true, false),
                "{verdict:?} must be refused once enforcement is on"
            );
        }

        // The override scope reaches everything, including INFECTED, so
        // quarantine review and incident response stay possible.
        for verdict in [V::Unspecified, V::Pending, V::Failed, V::Infected] {
            assert!(
                allowed(verdict, true, true),
                "{verdict:?} must be reachable with the override scope"
            );
        }
    }

    /// An unknown/garbage stored token must read as NOT scanned, never as clean.
    #[test]
    fn an_unrecognised_verdict_is_not_clean() {
        assert!(!decide_scan_gate(9999, "f-1", "download_file", true, false).is_ok());
        assert!(decide_scan_gate(9999, "f-1", "download_file", false, false).is_ok());
    }

    /// The refusal names why, so an operator debugging a quarantined upload is
    /// not left with an indistinguishable "missing file".
    #[test]
    fn the_refusal_explains_itself() {
        let err = decide_scan_gate(V::Infected as i32, "f-1", "download_file", false, false)
            .expect_err("infected must refuse");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("malicious"),
            "the refusal must say why: {}",
            err.message()
        );
    }
}
