//! Shared SQL-dialect adapter + SQL-template helpers for the
//! per-backend system-store impls.
//!
//! ## Why this module exists
//!
//! `postgres_*`, `mysql_*`, and `sqlite_*` carry three hand-copied
//! per-dialect impls of the same trait surfaces (`SagaStore`,
//! `ProjectionTaskStore`, `AdminAuditStore`, `MigrationAuditStore`).
//! The SQL bodies were near-identical; they diverged only in a handful
//! of mechanical dialect details:
//!
//! - **placeholder style** — Postgres numbers them (`$1`, `$2`, …);
//!   MySQL and SQLite use positional `?`.
//! - **`NOW()` vs `CURRENT_TIMESTAMP`** — and the microsecond /
//!   strftime variants.
//! - **JSON casts** — `$N::jsonb` vs `CAST(? AS JSON)` vs none.
//!
//! The *logic* around those details — assembling a WHERE clause from an
//! optional-filter list, normalising `limit`/`offset`, mapping a
//! `GROUP BY status` result into a summary struct — was genuinely
//! identical and is centralised here so the three impls can keep only
//! their `bind` / row-decode code (the parts that are irreducibly
//! per-dialect because the sqlx row type differs).
//!
//! The admin-audit chain-verify control loop is *also* identical across
//! the three impls, but its inner page-fetch wraps a per-dialect
//! `sqlx::query(...).fetch_all(...)` + `row_to_audit` decode whose row
//! type differs; sharing the loop would require an async-closure
//! page-fetcher (boxed futures), which adds more plumbing than the
//! ~20-line loop it would remove. Left per-dialect on purpose.
//!
//! Nothing here changes emitted SQL semantics: each helper reproduces
//! exactly the string each impl produced by hand.

use super::system_store::{
    ProjectionTaskSummary, SagaStatus, SagaSummary, SystemStoreError, SystemStoreResult,
};

// ── Placeholder style ─────────────────────────────────────────────────────────

/// How a dialect renders bound-parameter placeholders.
///
/// Postgres uses numbered placeholders (`$1`, `$2`, …) that must match
/// the bind order. MySQL and SQLite both accept positional `?` (SQLite
/// also accepts `?1` but the system-store impls bind positionally, so
/// `?` is the shared form).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placeholder {
    /// `$1`, `$2`, … (Postgres).
    Numbered,
    /// `?` (MySQL, SQLite).
    Positional,
}

impl Placeholder {
    /// Render the placeholder for the 1-based bind position `n`.
    /// `Numbered` returns `$n`; `Positional` returns `?` (ignoring `n`).
    pub fn render(self, n: usize) -> String {
        match self {
            Self::Numbered => format!("${n}"),
            Self::Positional => "?".to_string(),
        }
    }
}

// ── SQL dialect adapter ───────────────────────────────────────────────────────

/// The mechanical dialect knobs the shared SQL-template helpers need.
/// One value per backend; the per-backend impls pass their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlDialect {
    /// Placeholder rendering style.
    pub placeholder: Placeholder,
}

// Per-dialect constants: under a single-feature build only one is
// referenced, so suppress the dead-code warning for the unused ones.
#[allow(dead_code)]
impl SqlDialect {
    /// Postgres: `$N` placeholders.
    pub const POSTGRES: Self = Self {
        placeholder: Placeholder::Numbered,
    };
    /// MySQL: `?` placeholders.
    pub const MYSQL: Self = Self {
        placeholder: Placeholder::Positional,
    };
    /// SQLite: `?` placeholders.
    pub const SQLITE: Self = Self {
        placeholder: Placeholder::Positional,
    };

    /// Render placeholder for 1-based bind position `n`.
    pub fn placeholder(self, n: usize) -> String {
        self.placeholder.render(n)
    }
}

// ── Equality-filter WHERE builder ──────────────────────────────────────────────

/// Result of assembling an optional-equality WHERE clause plus the
/// trailing `LIMIT`/`OFFSET` placeholders.
///
/// The caller still binds the values itself (in the same order it
/// passed the `present` flags), then binds `limit` and `offset` last —
/// exactly the bind sequence the hand-written impls used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhereClause {
    /// `WHERE col = <ph> AND …`, or empty string when no filter is
    /// present. Always has a leading space-free `WHERE` (no trailing
    /// whitespace) so it can be dropped straight into a query template.
    pub where_sql: String,
    /// Placeholder string for the `LIMIT` bind (e.g. `$3` or `?`).
    pub limit_placeholder: String,
    /// Placeholder string for the `OFFSET` bind (e.g. `$4` or `?`).
    pub offset_placeholder: String,
}

/// Assemble `WHERE col = <ph> AND …` from a list of
/// `(column, is_present)` pairs, skipping absent filters, and compute
/// the `LIMIT`/`OFFSET` placeholders that follow the present filters in
/// bind order.
///
/// This reproduces the exact clause the saga / admin-audit /
/// migration-runs list methods built by hand. The caller binds the
/// present filter values in the same order, then binds `limit` and
/// `offset`.
///
/// Postgres → `WHERE a = $1 AND b = $2`, limit `$3`, offset `$4`.
/// MySQL/SQLite → `WHERE a = ? AND b = ?`, limit `?`, offset `?`.
pub fn build_eq_where(dialect: SqlDialect, filters: &[(&str, bool)]) -> WhereClause {
    let mut clauses: Vec<String> = Vec::new();
    let mut bind_index: usize = 0;
    for (column, present) in filters {
        if *present {
            bind_index += 1;
            clauses.push(format!("{column} = {}", dialect.placeholder(bind_index)));
        }
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    WhereClause {
        where_sql,
        limit_placeholder: dialect.placeholder(bind_index + 1),
        offset_placeholder: dialect.placeholder(bind_index + 2),
    }
}

/// Normalise a list filter's `(limit, offset)` to the shared default:
/// non-positive limit becomes 100, negative offset becomes 0. Every
/// list method applied this same rule.
pub fn normalize_limit_offset(limit: i64, offset: i64) -> (i64, i64) {
    let limit = if limit <= 0 { 100 } else { limit };
    (limit, offset.max(0))
}

// ── Summary bucket mappers ─────────────────────────────────────────────────────

/// Fold one `(status, count)` row from a saga `GROUP BY status` query
/// into the running [`SagaSummary`]. Returns a `SchemaMismatch` error
/// for an unrecognised status token (the CHECK constraint should make
/// this unreachable, but the impls all guarded it identically).
pub fn apply_saga_summary_bucket(
    summary: &mut SagaSummary,
    backend: &'static str,
    status: &str,
    n: i64,
) -> SystemStoreResult<()> {
    match SagaStatus::parse(status) {
        Some(SagaStatus::Indeterminate) => summary.indeterminate = n,
        Some(SagaStatus::InProgress) => summary.in_progress = n,
        Some(SagaStatus::Pending) => summary.pending = n,
        Some(SagaStatus::Committed) => summary.committed = n,
        Some(SagaStatus::Compensated) => summary.compensated = n,
        Some(SagaStatus::Failed) => summary.failed = n,
        Some(SagaStatus::InDoubt) => summary.in_doubt = n,
        Some(SagaStatus::FailedCompensation) => summary.failed_compensation = n,
        Some(SagaStatus::ManualReview) => summary.manual_review = n,
        None => {
            return Err(SystemStoreError::SchemaMismatch {
                backend,
                detail: format!(
                    "unexpected saga status '{status}' (CHECK constraint should prevent this)"
                ),
            });
        }
    }
    Ok(())
}

/// Fold one `(status, count)` row from a projection-task
/// `GROUP BY status` query into the running [`ProjectionTaskSummary`].
/// Returns a `SchemaMismatch` error for an unrecognised status token.
pub fn apply_projection_summary_bucket(
    summary: &mut ProjectionTaskSummary,
    backend: &'static str,
    status: &str,
    n: i64,
) -> SystemStoreResult<()> {
    match status {
        "PENDING" => summary.pending = n,
        "IN_PROGRESS" => summary.in_progress = n,
        "COMPLETED" => summary.completed = n,
        "FAILED" => summary.failed = n,
        "DEAD_LETTER" => summary.dead_letter = n,
        _ => {
            return Err(SystemStoreError::SchemaMismatch {
                backend,
                detail: format!(
                    "unexpected status '{status}' (CHECK constraint should prevent this)"
                ),
            });
        }
    }
    Ok(())
}

/// Page size for streaming admin-audit chain verification (#212). The verify
/// loop pulls the chain one page at a time; this bounds the rows (incl. JSONB
/// payloads) buffered per `fetch_all`. Override via
/// `UDB_ADMIN_AUDIT_VERIFY_PAGE_SIZE` (default 1000, minimum 1).
pub(super) fn admin_audit_verify_page_size() -> i64 {
    std::env::var("UDB_ADMIN_AUDIT_VERIFY_PAGE_SIZE")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbered_placeholders_match_bind_order() {
        let d = SqlDialect::POSTGRES;
        let w = build_eq_where(d, &[("tenant_id", true), ("status", true)]);
        assert_eq!(w.where_sql, "WHERE tenant_id = $1 AND status = $2");
        assert_eq!(w.limit_placeholder, "$3");
        assert_eq!(w.offset_placeholder, "$4");
    }

    #[test]
    fn positional_placeholders_use_question_marks() {
        let w = build_eq_where(SqlDialect::MYSQL, &[("tenant_id", true), ("status", true)]);
        assert_eq!(w.where_sql, "WHERE tenant_id = ? AND status = ?");
        assert_eq!(w.limit_placeholder, "?");
        assert_eq!(w.offset_placeholder, "?");
    }

    #[test]
    fn absent_filters_are_skipped_and_renumbered() {
        let d = SqlDialect::POSTGRES;
        // Only the 2nd and 4th filters present.
        let w = build_eq_where(
            d,
            &[
                ("tenant_id", false),
                ("status", true),
                ("tx_id", false),
                ("correlation_id", true),
            ],
        );
        assert_eq!(w.where_sql, "WHERE status = $1 AND correlation_id = $2");
        assert_eq!(w.limit_placeholder, "$3");
        assert_eq!(w.offset_placeholder, "$4");
    }

    #[test]
    fn no_filters_yields_empty_where_and_first_placeholders() {
        let d = SqlDialect::POSTGRES;
        let w = build_eq_where(d, &[("a", false), ("b", false)]);
        assert_eq!(w.where_sql, "");
        assert_eq!(w.limit_placeholder, "$1");
        assert_eq!(w.offset_placeholder, "$2");

        let w2 = build_eq_where(SqlDialect::SQLITE, &[("a", false)]);
        assert_eq!(w2.where_sql, "");
        assert_eq!(w2.limit_placeholder, "?");
        assert_eq!(w2.offset_placeholder, "?");
    }

    #[test]
    fn normalize_limit_offset_applies_defaults() {
        assert_eq!(normalize_limit_offset(0, -5), (100, 0));
        assert_eq!(normalize_limit_offset(-1, 3), (100, 3));
        assert_eq!(normalize_limit_offset(50, 10), (50, 10));
    }

    #[test]
    fn saga_summary_bucket_maps_every_status() {
        let mut s = SagaSummary::default();
        for st in SagaStatus::all() {
            apply_saga_summary_bucket(&mut s, "postgres", st.as_str(), 1).unwrap();
        }
        assert_eq!(s.total(), 8);
    }

    #[test]
    fn saga_summary_bucket_rejects_unknown_status() {
        let mut s = SagaSummary::default();
        let err = apply_saga_summary_bucket(&mut s, "mysql", "bogus", 1).unwrap_err();
        assert!(matches!(err, SystemStoreError::SchemaMismatch { .. }));
    }

    #[test]
    fn projection_summary_bucket_maps_every_status() {
        let mut s = ProjectionTaskSummary::default();
        for st in [
            "PENDING",
            "IN_PROGRESS",
            "COMPLETED",
            "FAILED",
            "DEAD_LETTER",
        ] {
            apply_projection_summary_bucket(&mut s, "sqlite", st, 1).unwrap();
        }
        assert_eq!(s.total(), 5);
        let err = apply_projection_summary_bucket(&mut s, "sqlite", "WAT", 1).unwrap_err();
        assert!(matches!(err, SystemStoreError::SchemaMismatch { .. }));
    }
}
