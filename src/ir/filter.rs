//! Neutral filter / predicate AST (U2 step 1).
//!
//! `LogicalFilter` is a tree of boolean-valued operations the compiler walks
//! to produce backend-specific predicates (`WHERE` in SQL, `find()` in Mongo,
//! `WHERE` in Cypher, payload filter in Qdrant). The variants intentionally
//! stay flat — no aggregate predicates, no subqueries — because anything
//! richer becomes a `LogicalRead` join or a separate aggregation IR step
//! (deferred to a later U2 sub-task).

use serde::{Deserialize, Serialize};

use super::value::LogicalValue;

/// A boolean-valued filter expression.
///
/// `And` / `Or` are n-ary (not binary) so compilers can emit flat
/// `WHERE a AND b AND c` instead of nested parentheses, and so callers don't
/// have to fold pairwise. Empty `And([])` reduces to TRUE, empty `Or([])`
/// to FALSE — both useful as the identity element while building filters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalFilter {
    /// Conjunction of all branches. Empty vector = TRUE.
    And(Vec<LogicalFilter>),
    /// Disjunction of any branch. Empty vector = FALSE.
    Or(Vec<LogicalFilter>),
    /// Logical NOT of the inner predicate.
    Not(Box<LogicalFilter>),
    /// `field <op> value`. Field is a dotted path (`address.city`).
    Comparison {
        field: String,
        op: ComparisonOp,
        value: LogicalValue,
    },
    /// `field IS NULL`. Distinct from `Comparison(_, Eq, Null)` because
    /// SQL's three-valued logic makes those non-equivalent.
    IsNull(String),
    /// `field IN (v1, v2, …)`. Empty `values` reduces to FALSE.
    InList {
        field: String,
        values: Vec<LogicalValue>,
    },
}

/// Comparison operators supported by `LogicalFilter::Comparison`.
///
/// Not every operator works on every backend — `Like` / `ILike` are
/// SQL-shaped, `Contains` / `StartsWith` / `EndsWith` are higher-level.
/// The compiler decides whether to lower (e.g. Mongo `$regex` for
/// `StartsWith`) or reject (`CompileError::OperatorUnsupported`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    /// SQL-style case-sensitive pattern. Value must be `String` containing
    /// `%`/`_` wildcards; compiler validates.
    Like,
    /// Case-insensitive SQL `ILIKE` / Mongo `$regex` with `/i`.
    ILike,
    /// Substring match. Lowered to SQL `LIKE '%v%'` or Mongo `$regex`.
    Contains,
    /// Prefix match.
    StartsWith,
    /// Suffix match.
    EndsWith,
}

impl LogicalFilter {
    /// Identity element for an `And`-fold. Use this as the starting point
    /// when collecting filter fragments from a request builder.
    pub fn always() -> Self {
        Self::And(Vec::new())
    }

    /// Identity element for an `Or`-fold.
    pub fn never() -> Self {
        Self::Or(Vec::new())
    }

    /// Append a clause to `self`, conjoining if already an `And` so the
    /// caller doesn't accidentally end up with `And(And(...), ...)`.
    pub fn and(self, other: Self) -> Self {
        match self {
            Self::And(mut clauses) => {
                clauses.push(other);
                Self::And(clauses)
            }
            existing => Self::And(vec![existing, other]),
        }
    }

    /// Walk every field name referenced (for projection-coverage lint and
    /// for the post-fold "bind these tenant predicates" pass).
    pub fn referenced_fields<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            Self::And(clauses) | Self::Or(clauses) => {
                for c in clauses {
                    c.referenced_fields(out);
                }
            }
            Self::Not(inner) => inner.referenced_fields(out),
            Self::Comparison { field, .. } | Self::IsNull(field) | Self::InList { field, .. } => {
                out.push(field.as_str());
            }
        }
    }
}

impl ComparisonOp {
    /// Stable token for error messages, traces, and capability matrices.
    pub fn token(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::Ne => "ne",
            Self::Lt => "lt",
            Self::Le => "le",
            Self::Gt => "gt",
            Self::Ge => "ge",
            Self::Like => "like",
            Self::ILike => "ilike",
            Self::Contains => "contains",
            Self::StartsWith => "starts_with",
            Self::EndsWith => "ends_with",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_constructors_are_neutral() {
        // `always()` AND any predicate == that predicate (when compiler folds).
        let p = LogicalFilter::Comparison {
            field: "x".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::Int(1),
        };
        let combined = LogicalFilter::always().and(p.clone());
        match combined {
            LogicalFilter::And(clauses) => assert_eq!(clauses, vec![p]),
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn and_chains_flatten() {
        let p1 = LogicalFilter::Comparison {
            field: "a".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::Int(1),
        };
        let p2 = LogicalFilter::Comparison {
            field: "b".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::Int(2),
        };
        let p3 = LogicalFilter::Comparison {
            field: "c".into(),
            op: ComparisonOp::Eq,
            value: LogicalValue::Int(3),
        };
        let combined = p1.clone().and(p2.clone()).and(p3.clone());
        // Must be And([p1, p2, p3]) — flat, not nested.
        match combined {
            LogicalFilter::And(clauses) => assert_eq!(clauses, vec![p1, p2, p3]),
            other => panic!("expected flat And, got {other:?}"),
        }
    }

    #[test]
    fn referenced_fields_walks_all_branches() {
        let f = LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "a".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::Int(1),
            },
            LogicalFilter::Or(vec![
                LogicalFilter::IsNull("b".into()),
                LogicalFilter::InList {
                    field: "c".into(),
                    values: vec![LogicalValue::Int(2)],
                },
            ]),
            LogicalFilter::Not(Box::new(LogicalFilter::Comparison {
                field: "d".into(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Int(3),
            })),
        ]);
        let mut out: Vec<&str> = Vec::new();
        f.referenced_fields(&mut out);
        out.sort();
        assert_eq!(out, vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn op_tokens_are_pinned() {
        // Tokens leak into error messages and SDK enums; pin them.
        assert_eq!(ComparisonOp::Eq.token(), "eq");
        assert_eq!(ComparisonOp::ILike.token(), "ilike");
        assert_eq!(ComparisonOp::StartsWith.token(), "starts_with");
    }
}
