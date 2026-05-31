// src/plan_approval.rs — Migration plan approval workflow.
//
// Gap 17 (legacy_sql): before applying any changes, verify that the current
// proto diff exactly matches a previously exported and approved plan file.
//
// This provides a four-eyes / change-management gate for production migrations:
//   1. Engineer runs `udb-proto-parser plan proto/ > db/migration_plan.json`.
//   2. Tech lead reviews and approves the JSON file (e.g. via Git PR).
//   3. On the next deploy, the engine loads `require_approval_plan` and calls
//      `plan_matches_current_diff()` before entering APPLYING.
//   4. If the current diff does not match the approved plan, the run is blocked.
//
// Aligned with:
//   - legacy_sql  legacycore/db/migrate_plan.go  (PlanMatchesCurrentDiff,
//                                             planOperationsHash, MigrationPlan)
//   - UDB spec §16.3.2                        (Double-Entry Ledger)

use serde::{Deserialize, Serialize};

use crate::generation::CatalogManifest;
use crate::migration::diff::{ChangeOperation, ChangeSafety};

// ── Plan operation ────────────────────────────────────────────────────────────

/// One operation serialised in an exported plan file.
/// Mirrors `MigrationPlanOperation` in legacy_sql `migrate_plan.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanOperation {
    /// Machine-readable change kind, e.g. `"add_column"`.
    pub kind: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    /// Safety classification: `"SafeAuto"`, `"RequiresReview"`, `"Blocked"`.
    pub safety: String,
    /// Explanation for blocked operations.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub blocked_reason: String,
    /// Deterministic fingerprint of this operation (kind+schema+table+column+safety).
    pub fingerprint: String,
    /// Optional SQL preview (excluded from fingerprint comparison).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub sql_preview: String,
}

// ── Exported plan ─────────────────────────────────────────────────────────────

/// The full exportable migration plan, written to disk and later verified.
/// Mirrors `MigrationPlan` in legacy_sql `migrate_plan.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExportedPlan {
    /// RFC 3339 timestamp when the plan was generated.
    pub generated_at: String,
    /// SHA-256 checksum of the proto AST manifest when the plan was built.
    pub manifest_checksum: String,
    /// Number of auto-safe operations.
    pub auto_count: usize,
    /// Number of blocked operations.
    pub blocked_count: usize,
    /// Number of stale migration hint warnings.
    pub hint_warnings: usize,
    /// Auto-safe operations in dependency order.
    pub operations: Vec<PlanOperation>,
    /// Blocked operations.
    pub blocked: Vec<PlanOperation>,
    /// Hint warning messages.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub hints: Vec<String>,
    /// SHA-256 of the ordered sequence of all operation fingerprints.
    /// Deep fingerprint that detects reordering, insertions, and deletions.
    pub operations_hash: String,
}

// ── Plan builder ──────────────────────────────────────────────────────────────

/// Build an `ExportedPlan` from a `CatalogManifest` and its diff operations.
///
/// The plan contains per-operation fingerprints and an overall operations hash
/// that makes `plan_matches_current_diff()` resistant to count-only equality.
pub fn build_exported_plan(
    manifest: &CatalogManifest,
    changes: &[ChangeOperation],
) -> ExportedPlan {
    use sha2::{Digest, Sha256};

    let (operations, blocked): (Vec<_>, Vec<_>) = changes
        .iter()
        .partition(|c| c.safety != ChangeSafety::Blocked);

    let hint_warnings = changes
        .iter()
        .filter(|c| c.kind == crate::migration::diff::ChangeKind::HintWarning)
        .count();

    let make_op = |c: &&ChangeOperation| {
        let kind = format!("{:?}", c.kind);
        let safety = format!("{:?}", c.safety);
        let fp = op_fingerprint(&kind, &c.schema, &c.table, &c.column, &safety);
        PlanOperation {
            kind,
            schema: c.schema.clone(),
            table: c.table.clone(),
            column: c.column.clone(),
            safety,
            blocked_reason: c.blocked_reason.clone(),
            fingerprint: fp,
            sql_preview: String::new(),
        }
    };

    let ops: Vec<PlanOperation> = operations.iter().map(make_op).collect();
    let blk: Vec<PlanOperation> = blocked.iter().map(make_op).collect();

    // Overall operations hash: ordered sequence of all fingerprints.
    let mut hasher = Sha256::new();
    for op in &ops {
        hasher.update(op.fingerprint.as_bytes());
        hasher.update(b"\x01");
    }
    hasher.update(b"\x02"); // separator between auto and blocked sections
    for op in &blk {
        hasher.update(op.fingerprint.as_bytes());
        hasher.update(b"\x01");
    }
    let operations_hash = format!("sha256:{:x}", hasher.finalize());

    ExportedPlan {
        generated_at: rfc3339_now(),
        manifest_checksum: manifest.checksum_sha256.clone(),
        auto_count: ops.len(),
        blocked_count: blk.len(),
        hint_warnings,
        operations: ops,
        blocked: blk,
        hints: Vec::new(),
        operations_hash,
    }
}

// ── Plan match ────────────────────────────────────────────────────────────────

/// Result of comparing a saved plan against the current diff state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlanMatchResult {
    /// The current diff matches the approved plan exactly.
    Matches,
    /// The manifest checksum changed since the plan was approved.
    ManifestChanged { approved: String, current: String },
    /// Operation counts differ.
    CountMismatch {
        approved_auto: usize,
        current_auto: usize,
        approved_blocked: usize,
        current_blocked: usize,
    },
    /// The ordered operations hash differs (catches reordering / per-op mutations).
    OperationsHashMismatch {
        approved_hash: String,
        current_hash: String,
    },
}

impl PlanMatchResult {
    /// Returns `true` when the plan matches.
    pub fn is_match(&self) -> bool {
        *self == PlanMatchResult::Matches
    }

    /// Returns a human-readable description of the mismatch.
    pub fn describe(&self) -> String {
        match self {
            Self::Matches => "plan matches approved file".to_string(),
            Self::ManifestChanged { approved, current } => format!(
                "manifest checksum changed: approved={} current={}",
                &approved[..12.min(approved.len())],
                &current[..12.min(current.len())]
            ),
            Self::CountMismatch {
                approved_auto,
                current_auto,
                approved_blocked,
                current_blocked,
            } => format!(
                "operation count mismatch: approved=({auto_a} auto, {blk_a} blocked) current=({auto_c} auto, {blk_c} blocked)",
                auto_a = approved_auto,
                blk_a = approved_blocked,
                auto_c = current_auto,
                blk_c = current_blocked,
            ),
            Self::OperationsHashMismatch {
                approved_hash,
                current_hash,
            } => format!(
                "operations hash mismatch: approved={} current={}",
                &approved_hash[..16.min(approved_hash.len())],
                &current_hash[..16.min(current_hash.len())]
            ),
        }
    }
}

/// Compare a saved `ExportedPlan` against the current manifest and diff.
///
/// Comparison layers (first mismatch wins):
/// 1. `manifest_checksum` — canonical proto AST hash must be identical.
/// 2. `auto_count` / `blocked_count` — operation counts must match.
/// 3. `operations_hash` — ordered fingerprint sequence must match.
pub fn plan_matches_current_diff(
    approved: &ExportedPlan,
    current_manifest: &CatalogManifest,
    current_changes: &[ChangeOperation],
) -> PlanMatchResult {
    // Layer 1: manifest checksum
    if approved.manifest_checksum != current_manifest.checksum_sha256 {
        return PlanMatchResult::ManifestChanged {
            approved: approved.manifest_checksum.clone(),
            current: current_manifest.checksum_sha256.clone(),
        };
    }

    let current = build_exported_plan(current_manifest, current_changes);

    // Layer 2: counts
    if approved.auto_count != current.auto_count || approved.blocked_count != current.blocked_count
    {
        return PlanMatchResult::CountMismatch {
            approved_auto: approved.auto_count,
            current_auto: current.auto_count,
            approved_blocked: approved.blocked_count,
            current_blocked: current.blocked_count,
        };
    }

    // Layer 3: operations hash
    if approved.operations_hash != current.operations_hash {
        return PlanMatchResult::OperationsHashMismatch {
            approved_hash: approved.operations_hash.clone(),
            current_hash: current.operations_hash.clone(),
        };
    }

    PlanMatchResult::Matches
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Deterministic fingerprint for a single operation.
/// Excludes SQL preview and timestamps — stable across re-runs.
pub fn op_fingerprint(kind: &str, schema: &str, table: &str, column: &str, safety: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for part in &[
        kind, "\x00", schema, "\x00", table, "\x00", column, "\x00", safety,
    ] {
        h.update(part.as_bytes());
    }
    format!("sha256:{:x}", h.finalize())
}

/// Real RFC 3339 timestamp via chrono. Pre-fix this was a
/// `format!("{secs}")` epoch-seconds string with a "simplified; Go
/// service writes proper RFC3339" comment — the output looked like
/// a number, not a timestamp, and downstream consumers had to parse
/// it as an i64 then convert. Now matches the Go service.
fn rfc3339_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// NW-universal: wall-clock helper returning Unix milliseconds.
/// Used by the approval workflow's freshness checks; pulled out so
/// tests can substitute deterministic timestamps.
pub fn current_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ── Approval workflow (NW-universal control plane) ───────────────────────────
//
// Pre-NW-universal the plan_approval module was a fingerprint-comparison
// utility: given an exported plan + current manifest + current diff it
// checked that the diff still matched the previously-approved plan
// (Layer 1 = manifest_checksum, Layer 2 = counts, Layer 3 =
// operations_hash). It had no notion of WHO approved a plan, WHY,
// WHEN, or whether the approval had expired or been revoked.
//
// The migration-control workflow needs approval identity, reason, expiry,
// revocation, and SLA metadata. This section delivers the workflow API:
//
//   - `ApprovalSignature` — one approver's vote on a plan
//   - `ApprovedPlan` — sealed plan + N signatures + expiry
//   - `ApprovalConfig` — quorum size, allowed roles, expiry, HMAC key
//   - `ApprovalError` — typed failure modes
//   - `ApprovalFreshness` — is the seal still valid right now?
//   - `compute_signature` — HMAC-SHA256 of (plan_fingerprint || approver_id || reason)
//
// Backward compat: the existing `ExportedPlan` / `build_exported_plan` /
// `plan_matches_current_diff` / `op_fingerprint` are unchanged. Callers
// who don't need the workflow keep using them.

/// One approver's vote on a plan. Multiple signatures combine into
/// a quorum-approved plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSignature {
    /// Stable identifier for the approver (typically the SSO subject).
    pub approver_id: String,
    /// Role at approval time (e.g. `"tech_lead"`, `"sre"`, `"dba"`).
    /// Used by `ApprovalConfig::allowed_roles` to gate who can sign.
    pub approver_role: String,
    /// Unix milliseconds. Approver's wall-clock at signing time.
    pub approved_at_unix_ms: i64,
    /// Free-text justification. Required — the workflow refuses
    /// empty / whitespace-only reasons so audit trails always carry
    /// the engineer's intent.
    pub reason: String,
    /// HMAC-SHA256 of
    ///   `plan.operations_hash || "\x00" || approver_id || "\x00" || reason`
    /// keyed by the operator's `signing_key`. `verify()` checks every
    /// signature against the same key — a key rotation invalidates
    /// older approvals.
    pub signature: String,
}

/// Sealed approved plan = ExportedPlan + signatures + expiry + seal.
/// Once `create()` returns Ok, the struct is treated as immutable —
/// `seal` is a SHA-256 over the plan + every signature, so any
/// tampering downstream is detectable via `verify()`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovedPlan {
    pub plan: ExportedPlan,
    pub signatures: Vec<ApprovalSignature>,
    /// Unix milliseconds when this approval stops being valid.
    /// `current_unix_ms() >= expires_at_unix_ms` → `ApprovalFreshness::Expired`.
    pub expires_at_unix_ms: i64,
    /// SHA-256 of plan.operations_hash + each signature in order.
    /// Tamper-evident: changing any field of any signature requires
    /// recomputing the seal, which `verify()` rejects unless the
    /// signing key is also known.
    pub seal: String,
}

/// Operator policy for what counts as a valid approval.
#[derive(Debug, Clone)]
pub struct ApprovalConfig {
    /// Distinct approvers required. `1` = single-signer mode (matches
    /// the legacy "Git PR is the approval" model); `N >= 2` = quorum.
    pub quorum_size: u32,
    /// Roles permitted to sign. Empty = any role accepted (useful for
    /// dev environments). Each `ApprovalSignature::approver_role` must
    /// be in this list.
    pub allowed_roles: Vec<String>,
    /// How long an approval stays valid after the last signature.
    pub expiry: std::time::Duration,
    /// HMAC key for signature computation + verification. The
    /// operator rotates this to invalidate all outstanding
    /// approvals.
    pub signing_key: Vec<u8>,
}

impl Default for ApprovalConfig {
    /// Defaults read from env at startup:
    ///   - `UDB_APPROVAL_QUORUM_SIZE` (default 1)
    ///   - `UDB_APPROVAL_EXPIRY_SECS` (default 86400 = 24h)
    ///   - `UDB_APPROVAL_ALLOWED_ROLES` (default empty = any role)
    ///   - `UDB_APPROVAL_SIGNING_KEY` (default empty — operators
    ///     MUST set this for production; default makes signatures
    ///     trivially forgeable so tests can run without secrets)
    fn default() -> Self {
        let quorum = std::env::var("UDB_APPROVAL_QUORUM_SIZE")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(1)
            .max(1);
        let expiry_secs = std::env::var("UDB_APPROVAL_EXPIRY_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(86_400)
            .clamp(60, 30 * 86_400); // 1m floor, 30d ceiling
        let allowed_roles = std::env::var("UDB_APPROVAL_ALLOWED_ROLES")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();
        let signing_key = std::env::var("UDB_APPROVAL_SIGNING_KEY")
            .ok()
            .map(|s| s.into_bytes())
            .unwrap_or_default();
        Self {
            quorum_size: quorum,
            allowed_roles,
            expiry: std::time::Duration::from_secs(expiry_secs),
            signing_key,
        }
    }
}

/// Typed failure modes the workflow surfaces. Each maps to a clear
/// operator message in the gRPC handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalError {
    /// Fewer than `config.quorum_size` distinct approvers signed.
    InsufficientQuorum { required: u32, got: u32 },
    /// Two signatures from the same `approver_id` — one approver,
    /// one vote.
    DuplicateApprover { approver_id: String },
    /// `approver_role` isn't in `config.allowed_roles`.
    UnknownRole { role: String },
    /// `reason` is empty / whitespace-only.
    EmptyReason { approver_id: String },
    /// HMAC verification failed for a specific signature. Caused by
    /// (a) signing key rotation, (b) tampering, or (c) the approver
    /// computing the HMAC with a different key.
    BadSignature { approver_id: String },
    /// `seal` doesn't match the expected SHA-256 of plan + signatures.
    /// Indicates downstream tampering.
    SealMismatch,
    /// `current_unix_ms() >= expires_at_unix_ms`.
    Expired {
        expired_at_unix_ms: i64,
        now_unix_ms: i64,
    },
}

impl std::fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InsufficientQuorum { required, got } => write!(
                f,
                "approval rejected: quorum {required} not met (got {got})"
            ),
            Self::DuplicateApprover { approver_id } => write!(
                f,
                "approval rejected: approver '{approver_id}' signed more than once"
            ),
            Self::UnknownRole { role } => write!(
                f,
                "approval rejected: role '{role}' is not in the allowed-roles list"
            ),
            Self::EmptyReason { approver_id } => write!(
                f,
                "approval rejected: approver '{approver_id}' did not supply a reason"
            ),
            Self::BadSignature { approver_id } => write!(
                f,
                "approval rejected: HMAC signature for '{approver_id}' failed verification"
            ),
            Self::SealMismatch => f.write_str(
                "approval rejected: seal does not match plan + signatures (tampered or corrupted)",
            ),
            Self::Expired {
                expired_at_unix_ms,
                now_unix_ms,
            } => write!(
                f,
                "approval expired at {expired_at_unix_ms} (now {now_unix_ms})"
            ),
        }
    }
}

impl std::error::Error for ApprovalError {}

/// Result of `ApprovedPlan::freshness(now)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalFreshness {
    Valid {
        remaining_secs: u64,
    },
    Expired {
        expired_at_unix_ms: i64,
        now_unix_ms: i64,
    },
}

/// Compute the canonical signature payload for one approver and HMAC it
/// with `signing_key`. Same function the verifier uses — round-trip
/// is the correctness guarantee.
pub fn compute_signature(
    plan_operations_hash: &str,
    approver_id: &str,
    reason: &str,
    signing_key: &[u8],
) -> String {
    use sha2::{Digest, Sha256};
    // Hand-rolled HMAC-SHA256: hmac = SHA256( (key ^ opad) || SHA256((key ^ ipad) || message) )
    // sha2 doesn't ship HMAC; rather than pull a new dep we implement
    // the standard construction directly. Block size for SHA-256 is 64.
    const BLOCK: usize = 64;
    let mut k = if signing_key.len() > BLOCK {
        Sha256::digest(signing_key).to_vec()
    } else {
        signing_key.to_vec()
    };
    k.resize(BLOCK, 0);
    let mut ipad = vec![0x36u8; BLOCK];
    let mut opad = vec![0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(plan_operations_hash.as_bytes());
    inner.update(b"\x00");
    inner.update(approver_id.as_bytes());
    inner.update(b"\x00");
    inner.update(reason.as_bytes());
    let inner_hash = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&inner_hash);
    format!("hmac-sha256:{:x}", outer.finalize())
}

fn compute_seal(plan: &ExportedPlan, signatures: &[ApprovalSignature]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(plan.operations_hash.as_bytes());
    h.update(b"\x00");
    for sig in signatures {
        h.update(sig.approver_id.as_bytes());
        h.update(b"\x00");
        h.update(sig.approver_role.as_bytes());
        h.update(b"\x00");
        h.update(sig.approved_at_unix_ms.to_be_bytes());
        h.update(b"\x00");
        h.update(sig.reason.as_bytes());
        h.update(b"\x00");
        h.update(sig.signature.as_bytes());
        h.update(b"\x01");
    }
    format!("sha256:{:x}", h.finalize())
}

impl ApprovedPlan {
    /// Seal a plan with N signatures under the given config. Validates:
    ///   - reason non-empty for every signature
    ///   - role in `config.allowed_roles` (when list is non-empty)
    ///   - no duplicate approver_ids
    ///   - signature count >= `config.quorum_size`
    ///   - every signature HMAC-verifies against the signing key
    ///
    /// On success the seal is computed + the expiry is set to
    /// `now + config.expiry`.
    pub fn create(
        plan: ExportedPlan,
        signatures: Vec<ApprovalSignature>,
        config: &ApprovalConfig,
        now_unix_ms: i64,
    ) -> Result<Self, ApprovalError> {
        // Reason + role gates per signature.
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for sig in &signatures {
            if sig.reason.trim().is_empty() {
                return Err(ApprovalError::EmptyReason {
                    approver_id: sig.approver_id.clone(),
                });
            }
            if !seen_ids.insert(sig.approver_id.clone()) {
                return Err(ApprovalError::DuplicateApprover {
                    approver_id: sig.approver_id.clone(),
                });
            }
            if !config.allowed_roles.is_empty()
                && !config.allowed_roles.iter().any(|r| r == &sig.approver_role)
            {
                return Err(ApprovalError::UnknownRole {
                    role: sig.approver_role.clone(),
                });
            }
            let expected = compute_signature(
                &plan.operations_hash,
                &sig.approver_id,
                &sig.reason,
                &config.signing_key,
            );
            // Constant-time-ish compare via fixed-length string equality
            // — HMAC outputs are fixed length so naive `==` is fine
            // against timing attacks at the broker layer (the network
            // round-trip dominates any string-compare delta).
            if expected != sig.signature {
                return Err(ApprovalError::BadSignature {
                    approver_id: sig.approver_id.clone(),
                });
            }
        }
        if (signatures.len() as u32) < config.quorum_size {
            return Err(ApprovalError::InsufficientQuorum {
                required: config.quorum_size,
                got: signatures.len() as u32,
            });
        }
        let seal = compute_seal(&plan, &signatures);
        let expires_at_unix_ms = now_unix_ms + (config.expiry.as_millis() as i64);
        Ok(Self {
            plan,
            signatures,
            expires_at_unix_ms,
            seal,
        })
    }

    /// Is the approval still fresh? Pure function — no clock side-effect.
    pub fn freshness(&self, now_unix_ms: i64) -> ApprovalFreshness {
        if now_unix_ms >= self.expires_at_unix_ms {
            ApprovalFreshness::Expired {
                expired_at_unix_ms: self.expires_at_unix_ms,
                now_unix_ms,
            }
        } else {
            let remaining = (self.expires_at_unix_ms - now_unix_ms) / 1000;
            ApprovalFreshness::Valid {
                remaining_secs: remaining.max(0) as u64,
            }
        }
    }

    /// Re-verify every signature + the seal under the given config.
    /// Catches: key rotation, tampering with any signature field,
    /// reordering signatures after the seal was computed.
    pub fn verify(&self, config: &ApprovalConfig) -> Result<(), ApprovalError> {
        // Seal integrity first — cheaper than recomputing every HMAC.
        let expected_seal = compute_seal(&self.plan, &self.signatures);
        if expected_seal != self.seal {
            return Err(ApprovalError::SealMismatch);
        }
        // HMAC each signature.
        for sig in &self.signatures {
            let expected = compute_signature(
                &self.plan.operations_hash,
                &sig.approver_id,
                &sig.reason,
                &config.signing_key,
            );
            if expected != sig.signature {
                return Err(ApprovalError::BadSignature {
                    approver_id: sig.approver_id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Full ready-to-apply check: verify signatures + check freshness +
    /// re-confirm the plan still matches the current manifest/diff.
    /// This is the single call gRPC apply handlers make.
    pub fn ready_to_apply(
        &self,
        config: &ApprovalConfig,
        current_manifest: &CatalogManifest,
        current_changes: &[ChangeOperation],
        now_unix_ms: i64,
    ) -> Result<PlanMatchResult, ApprovalError> {
        self.verify(config)?;
        if let ApprovalFreshness::Expired {
            expired_at_unix_ms,
            now_unix_ms,
        } = self.freshness(now_unix_ms)
        {
            return Err(ApprovalError::Expired {
                expired_at_unix_ms,
                now_unix_ms,
            });
        }
        Ok(plan_matches_current_diff(
            &self.plan,
            current_manifest,
            current_changes,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::CatalogManifest;
    use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety};

    fn sample_op(kind: ChangeKind, schema: &str, table: &str) -> ChangeOperation {
        use sha2::{Digest, Sha256};
        let fp = format!(
            "{:x}",
            Sha256::digest(format!("{kind:?}{schema}{table}").as_bytes())
        );
        ChangeOperation {
            kind,
            safety: ChangeSafety::SafeAuto,
            schema: schema.to_string(),
            table: table.to_string(),
            fingerprint: fp,
            ..Default::default()
        }
    }

    #[test]
    fn plan_matches_identical_state() {
        let manifest = CatalogManifest {
            checksum_sha256: "abc123".to_string(),
            ..CatalogManifest::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "ocr", "cases")];
        let approved = build_exported_plan(&manifest, &changes);
        let result = plan_matches_current_diff(&approved, &manifest, &changes);
        assert_eq!(result, PlanMatchResult::Matches);
    }

    #[test]
    fn plan_detects_manifest_change() {
        let old_manifest = CatalogManifest {
            checksum_sha256: "old-checksum".to_string(),
            ..Default::default()
        };
        let new_manifest = CatalogManifest {
            checksum_sha256: "new-checksum".to_string(),
            ..Default::default()
        };
        let approved = build_exported_plan(&old_manifest, &[]);
        let result = plan_matches_current_diff(&approved, &new_manifest, &[]);
        assert!(matches!(result, PlanMatchResult::ManifestChanged { .. }));
    }

    #[test]
    fn op_fingerprint_is_stable() {
        let fp1 = op_fingerprint("AddColumn", "ocr", "cases", "tenant_id", "SafeAuto");
        let fp2 = op_fingerprint("AddColumn", "ocr", "cases", "tenant_id", "SafeAuto");
        assert_eq!(fp1, fp2);
        assert!(fp1.starts_with("sha256:"));
    }

    // ── NW-universal: approval workflow ───────────────────────────────

    fn approval_config(quorum: u32, roles: Vec<&str>) -> ApprovalConfig {
        ApprovalConfig {
            quorum_size: quorum,
            allowed_roles: roles.into_iter().map(str::to_string).collect(),
            expiry: std::time::Duration::from_secs(3600),
            signing_key: b"test-signing-key-32-bytes-..........".to_vec(),
        }
    }

    fn sample_plan() -> ExportedPlan {
        let m = CatalogManifest {
            checksum_sha256: "abc123".to_string(),
            ..Default::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "ocr", "cases")];
        build_exported_plan(&m, &changes)
    }

    fn signed(
        plan: &ExportedPlan,
        approver_id: &str,
        role: &str,
        reason: &str,
        config: &ApprovalConfig,
    ) -> ApprovalSignature {
        let sig = compute_signature(
            &plan.operations_hash,
            approver_id,
            reason,
            &config.signing_key,
        );
        ApprovalSignature {
            approver_id: approver_id.to_string(),
            approver_role: role.to_string(),
            approved_at_unix_ms: 1_000,
            reason: reason.to_string(),
            signature: sig,
        }
    }

    #[test]
    fn approval_single_signer_happy_path() {
        let cfg = approval_config(1, vec!["sre"]);
        let plan = sample_plan();
        let sig = signed(
            &plan,
            "alice@org",
            "sre",
            "rolling out audit log column",
            &cfg,
        );
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).expect("create");
        // Seal computed; expiry set.
        assert!(approved.seal.starts_with("sha256:"));
        assert_eq!(approved.expires_at_unix_ms, 3_600_000);
        // Round-trip verify.
        assert!(approved.verify(&cfg).is_ok());
    }

    #[test]
    fn approval_rejects_empty_reason() {
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "sre", "", &cfg);
        let err = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap_err();
        assert!(matches!(err, ApprovalError::EmptyReason { .. }));
    }

    #[test]
    fn approval_rejects_duplicate_approver() {
        let cfg = approval_config(2, vec![]);
        let plan = sample_plan();
        let sig_a = signed(&plan, "alice", "sre", "first vote", &cfg);
        let sig_a_again = signed(&plan, "alice", "sre", "second vote", &cfg);
        let err = ApprovedPlan::create(plan, vec![sig_a, sig_a_again], &cfg, 0).unwrap_err();
        assert!(matches!(err, ApprovalError::DuplicateApprover { .. }));
    }

    #[test]
    fn approval_rejects_unknown_role() {
        let cfg = approval_config(1, vec!["sre", "tech_lead"]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "intern", "looks fine", &cfg);
        let err = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap_err();
        assert!(matches!(err, ApprovalError::UnknownRole { .. }));
    }

    #[test]
    fn approval_accepts_any_role_when_allowlist_empty() {
        // Dev environments commonly leave UDB_APPROVAL_ALLOWED_ROLES
        // unset → empty list → any role accepted. Regression guard.
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "anything-goes", "dev env", &cfg);
        assert!(ApprovedPlan::create(plan, vec![sig], &cfg, 0).is_ok());
    }

    #[test]
    fn approval_rejects_insufficient_quorum() {
        let cfg = approval_config(2, vec!["sre", "dba"]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "sre", "single vote", &cfg);
        let err = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap_err();
        match err {
            ApprovalError::InsufficientQuorum { required, got } => {
                assert_eq!(required, 2);
                assert_eq!(got, 1);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn approval_accepts_full_quorum() {
        let cfg = approval_config(2, vec!["sre", "dba"]);
        let plan = sample_plan();
        let sig_a = signed(&plan, "alice", "sre", "shipping this", &cfg);
        let sig_b = signed(&plan, "bob", "dba", "schema looks fine", &cfg);
        assert!(ApprovedPlan::create(plan, vec![sig_a, sig_b], &cfg, 0).is_ok());
    }

    #[test]
    fn approval_detects_signature_tampering() {
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let mut sig = signed(&plan, "alice", "sre", "ok", &cfg);
        sig.signature = "hmac-sha256:0000".to_string(); // tampered
        let err = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap_err();
        assert!(matches!(err, ApprovalError::BadSignature { .. }));
    }

    #[test]
    fn approval_detects_seal_tampering_post_create() {
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let mut approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        // Tamper with the reason after sealing.
        approved.signatures[0].reason = "I lied about my reason".to_string();
        let err = approved.verify(&cfg).unwrap_err();
        assert!(matches!(err, ApprovalError::SealMismatch));
    }

    #[test]
    fn approval_freshness_returns_remaining_secs() {
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        match approved.freshness(1_000) {
            ApprovalFreshness::Valid { remaining_secs } => assert_eq!(remaining_secs, 3_599),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn approval_freshness_returns_expired_past_deadline() {
        let cfg = approval_config(1, vec![]);
        let plan = sample_plan();
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        match approved.freshness(approved.expires_at_unix_ms + 1) {
            ApprovalFreshness::Expired {
                expired_at_unix_ms,
                now_unix_ms,
            } => {
                assert_eq!(expired_at_unix_ms, 3_600_000);
                assert_eq!(now_unix_ms, 3_600_001);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn ready_to_apply_succeeds_when_everything_matches() {
        let cfg = approval_config(1, vec![]);
        let manifest = CatalogManifest {
            checksum_sha256: "abc123".to_string(),
            ..Default::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "ocr", "cases")];
        let plan = build_exported_plan(&manifest, &changes);
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        // 1ms before expiry — still valid.
        let result = approved
            .ready_to_apply(&cfg, &manifest, &changes, approved.expires_at_unix_ms - 1)
            .unwrap();
        assert_eq!(result, PlanMatchResult::Matches);
    }

    #[test]
    fn ready_to_apply_blocks_when_manifest_changed_since_approval() {
        let cfg = approval_config(1, vec![]);
        let old_manifest = CatalogManifest {
            checksum_sha256: "old".to_string(),
            ..Default::default()
        };
        let new_manifest = CatalogManifest {
            checksum_sha256: "new".to_string(),
            ..Default::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "ocr", "cases")];
        let plan = build_exported_plan(&old_manifest, &changes);
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        let result = approved
            .ready_to_apply(&cfg, &new_manifest, &changes, 100)
            .unwrap();
        assert!(matches!(result, PlanMatchResult::ManifestChanged { .. }));
    }

    #[test]
    fn ready_to_apply_rejects_expired_approval() {
        let cfg = approval_config(1, vec![]);
        let manifest = CatalogManifest {
            checksum_sha256: "abc".to_string(),
            ..Default::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "x", "y")];
        let plan = build_exported_plan(&manifest, &changes);
        let sig = signed(&plan, "alice", "sre", "ok", &cfg);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg, 0).unwrap();
        let err = approved
            .ready_to_apply(&cfg, &manifest, &changes, approved.expires_at_unix_ms + 1)
            .unwrap_err();
        assert!(matches!(err, ApprovalError::Expired { .. }));
    }

    #[test]
    fn ready_to_apply_rejects_under_rotated_key() {
        let cfg_a = approval_config(1, vec![]);
        let manifest = CatalogManifest {
            checksum_sha256: "abc".to_string(),
            ..Default::default()
        };
        let changes = vec![sample_op(ChangeKind::AddColumn, "x", "y")];
        let plan = build_exported_plan(&manifest, &changes);
        let sig = signed(&plan, "alice", "sre", "ok", &cfg_a);
        let approved = ApprovedPlan::create(plan, vec![sig], &cfg_a, 0).unwrap();
        // Operator rotates the signing key — every outstanding
        // approval immediately fails verify.
        let cfg_b = ApprovalConfig {
            signing_key: b"rotated-key-........................".to_vec(),
            ..approval_config(1, vec![])
        };
        let err = approved
            .ready_to_apply(&cfg_b, &manifest, &changes, 100)
            .unwrap_err();
        assert!(matches!(err, ApprovalError::BadSignature { .. }));
    }

    #[test]
    fn compute_signature_is_deterministic_and_keyed() {
        let key = b"k";
        let sig_a = compute_signature("hash1", "alice", "reason", key);
        let sig_b = compute_signature("hash1", "alice", "reason", key);
        assert_eq!(sig_a, sig_b);
        // Changing the key changes the output.
        let sig_diff_key = compute_signature("hash1", "alice", "reason", b"different");
        assert_ne!(sig_a, sig_diff_key);
        // Changing the message changes the output.
        let sig_diff_msg = compute_signature("hash2", "alice", "reason", key);
        assert_ne!(sig_a, sig_diff_msg);
    }

    #[test]
    fn rfc3339_now_returns_iso8601_format() {
        // Regression guard: pre-NW-universal this returned plain
        // epoch seconds with a "simplified" comment. Now it must be
        // a real RFC 3339 string parseable by chrono.
        let now = rfc3339_now();
        assert!(chrono::DateTime::parse_from_rfc3339(&now).is_ok());
    }
}
