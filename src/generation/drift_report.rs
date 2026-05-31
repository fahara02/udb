use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::generation::manifest::CatalogManifest;
use crate::migration::diff::{ChangeKind, ChangeSafety, diff_manifests};

// ── Severity ─────────────────────────────────────────────────────────────────

/// Severity of a single drift item, aligned with the legacycore/db conventions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DriftSeverity {
    /// Data loss risk or migration is blocked without remediation.
    Critical,
    /// Schema gap — functionality impaired but migration can be staged.
    Warning,
    /// Informational — no immediate action required.
    #[default]
    Info,
}

// ── Item ─────────────────────────────────────────────────────────────────────

/// One detected schema drift issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DriftItem {
    pub severity: DriftSeverity,
    /// Machine-readable kind matching the diff `ChangeKind` or a special value.
    pub kind: String,
    pub schema: String,
    pub table: String,
    /// Empty for table-level or schema-level drift.
    pub column: String,
    /// The object name (index name, FK name, policy name, etc.) if relevant.
    pub object_name: String,
    pub description: String,
    pub suggestion: String,
    pub blocked_reason: String,
    /// Deterministic SHA-256 fingerprint copied from the underlying `ChangeOperation`.
    pub fingerprint: String,
}

// ── Report ───────────────────────────────────────────────────────────────────

/// Complete drift analysis produced by comparing two `CatalogManifest` snapshots.
///
/// Inspired by the legacycore `DriftReport` struct and the UDB spec
/// (§16.3 Double-Entry Migration & Dual FSM).  The Go UDB service builds this
/// report after the `PLAN_PROTO_DIFF` FSM state to decide whether to auto-apply
/// or halt for manual review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DriftReport {
    pub generated_at_unix: u64,
    /// Checksum of the previously applied manifest (empty on first bootstrap).
    pub old_checksum: String,
    /// Checksum of the newly-parsed proto AST manifest.
    pub new_checksum: String,
    /// True when `old_checksum != new_checksum` (or old is absent).
    pub has_drift: bool,
    pub total_issues: usize,
    pub critical: usize,
    pub warnings: usize,
    pub info: usize,
    /// Operations that can be applied automatically without human approval.
    pub auto_safe_count: usize,
    /// Operations that require explicit engineer sign-off before proceeding.
    pub requires_review_count: usize,
    /// Operations that are unconditionally blocked (e.g. DROP without allow_drop).
    pub blocked_count: usize,
    /// Stale migration hint warnings (previous_table_name / previous_column_name).
    pub hint_warnings: usize,
    /// Total number of proto-declared tables in the new manifest.
    pub proto_table_count: usize,
    /// Total number of external store resources (vector, graph, cache, etc.).
    pub store_count: usize,
    pub items: Vec<DriftItem>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

/// Build a structured `DriftReport` by diffing `old` (previously applied
/// manifest, `None` on first bootstrap) against `new` (freshly parsed proto AST).
///
/// This is a **pure static** operation — no database connection is required.
///
/// ```text
/// FSM state: PLAN_PROTO_DIFF
///   old_manifest  = load from proto_schema_versions ledger (if any)
///   new_manifest  = CatalogManifest::from_schemas(parsed_protos)
///   report        = build_drift_report(old.as_ref(), &new)
///   if report.blocked_count > 0 → FSM → ERROR
///   else                        → FSM → GENERATE_SQL → APPLYING
/// ```
pub fn build_drift_report(old: Option<&CatalogManifest>, new: &CatalogManifest) -> DriftReport {
    let changes = diff_manifests(old, new);
    let old_checksum = old.map(|m| m.checksum_sha256.clone()).unwrap_or_default();
    let has_drift = old
        .map(|o| o.checksum_sha256 != new.checksum_sha256)
        .unwrap_or(true);

    let auto_safe_count = changes
        .iter()
        .filter(|c| c.safety == ChangeSafety::SafeAuto && c.kind != ChangeKind::HintWarning)
        .count();
    let requires_review_count = changes
        .iter()
        .filter(|c| c.safety == ChangeSafety::RequiresReview && c.kind != ChangeKind::HintWarning)
        .count();
    let blocked_count = changes
        .iter()
        .filter(|c| c.safety == ChangeSafety::Blocked)
        .count();
    let hint_warnings = changes
        .iter()
        .filter(|c| c.kind == ChangeKind::HintWarning)
        .count();

    let mut items: Vec<DriftItem> = changes
        .iter()
        .map(|change| {
            let severity = classify_severity(change);
            let suggestion = suggest_for_kind(&change.kind);
            DriftItem {
                severity,
                kind: kind_label(&change.kind),
                schema: change.schema.clone(),
                table: change.table.clone(),
                column: change.column.clone(),
                object_name: change.object_name.clone(),
                description: change.reason.clone(),
                suggestion,
                blocked_reason: change.blocked_reason.clone(),
                fingerprint: change.fingerprint.clone(),
            }
        })
        .collect();

    // Promote manifest validation errors as CRITICAL drift items
    for error in &new.validation_errors {
        items.push(DriftItem {
            severity: DriftSeverity::Critical,
            kind: "manifest_validation_error".to_string(),
            description: error.clone(),
            suggestion: "Fix the referenced schema issue before applying migrations.".to_string(),
            ..DriftItem::default()
        });
    }

    // Sort for deterministic output: Critical → Warning → Info, then schema/table
    items.sort_by(|a, b| {
        severity_ord(&a.severity)
            .cmp(&severity_ord(&b.severity))
            .then(a.schema.cmp(&b.schema))
            .then(a.table.cmp(&b.table))
            .then(a.kind.cmp(&b.kind))
    });

    let critical = items
        .iter()
        .filter(|i| i.severity == DriftSeverity::Critical)
        .count();
    let warnings = items
        .iter()
        .filter(|i| i.severity == DriftSeverity::Warning)
        .count();
    let info = items
        .iter()
        .filter(|i| i.severity == DriftSeverity::Info)
        .count();

    DriftReport {
        generated_at_unix: generated_at_unix(),
        old_checksum,
        new_checksum: new.checksum_sha256.clone(),
        has_drift,
        total_issues: items.len(),
        critical,
        warnings,
        info,
        auto_safe_count,
        requires_review_count,
        blocked_count,
        hint_warnings,
        proto_table_count: new.tables.len(),
        store_count: new.stores.len(),
        items,
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn classify_severity(change: &crate::migration::diff::ChangeOperation) -> DriftSeverity {
    use ChangeKind::*;
    match (&change.kind, &change.safety) {
        (ValidationError, _) => DriftSeverity::Critical,
        (_, ChangeSafety::Blocked) => DriftSeverity::Critical,
        (HintWarning, _) => DriftSeverity::Warning,
        (_, ChangeSafety::RequiresReview) => DriftSeverity::Warning,
        _ => DriftSeverity::Info,
    }
}

fn suggest_for_kind(kind: &ChangeKind) -> String {
    use ChangeKind::*;
    match kind {
        DropTable => {
            "Set `allow_drop: true` on the table option after explicit review, then re-run the migration planner.".to_string()
        }
        DropColumn => {
            "Set `allow_drop: true` on the column option after explicit review, then re-run the migration planner.".to_string()
        }
        ChangeColumnType => {
            "Add `using_expression` to the column option to provide a safe USING cast expression.".to_string()
        }
        HintWarning => {
            "Verify migration hints; remove stale `previous_table_name` / `previous_column_name` after the migration is applied.".to_string()
        }
        DisableRls => {
            "Disabling RLS requires explicit migration approval — confirm no active tenant-isolation policies depend on it.".to_string()
        }
        SetTableUnlogged | SetTableLogged => {
            "Changing table persistence (LOGGED/UNLOGGED) requires explicit review — data may be lost on crash for UNLOGGED tables.".to_string()
        }
        _ => String::new(),
    }
}

fn kind_label(kind: &ChangeKind) -> String {
    // Convert CamelCase ChangeKind variant to snake_case label for JSON output.
    let s = format!("{kind:?}");
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(ch.to_lowercase());
    }
    out
}

fn severity_ord(s: &DriftSeverity) -> u8 {
    match s {
        DriftSeverity::Critical => 0,
        DriftSeverity::Warning => 1,
        DriftSeverity::Info => 2,
    }
}

fn generated_at_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
