//! The five neutral logical operations (U2 step 1).
//!
//! Every gRPC RPC on the broker that talks to a backend lowers to one of
//! these — `LogicalRead` for `Select`/`GenericDispatch query`, `LogicalWrite`
//! for `Upsert`/`mutate`, `LogicalDelete` for `Delete`, `LogicalSearch` for
//! `VectorSearch`/`HybridSearch`, `LogicalResourceOp` for
//! `EnsureResource`/`DropResource`/`ListResources`. Multi-leg plans
//! (Postgres + Qdrant fan-out) are a `Vec<LogicalLeg>` introduced in U2
//! step 6.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::filter::LogicalFilter;
use super::projection::{LogicalPagination, LogicalProjection, LogicalSort};
use super::value::LogicalValue;

/// A row/document, used by `LogicalWrite::records`. `BTreeMap` (not
/// `HashMap`) so field-order is deterministic — every compiler emits the
/// same SQL/JSON for the same logical record, which makes the canonical
/// cache key in U2 step 8 stable.
pub type LogicalRecord = BTreeMap<String, LogicalValue>;

/// A neutral read.
///
/// `message_type` is the catalog message name (the canonical proto name);
/// the compiler maps it to the backend's table/collection/index using the
/// active `CatalogManifest`. Nothing about this struct mentions a backend.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogicalRead {
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LogicalFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<LogicalProjection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<LogicalSort>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<LogicalInclude>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<LogicalPagination>,
}

/// Eager relation include request for `LogicalRead`.
///
/// The client-facing ORM can now express eager relation loading in the neutral
/// envelope, but compilers must explicitly lower it. Until a backend implements
/// JOIN/batched eager loading, the shared compiler gate rejects non-empty include
/// lists with `OperatorUnsupported` instead of silently ignoring them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalInclude {
    pub relation: String,
}

/// A neutral write — insert or upsert depending on `conflict`.
///
/// Compiler responsibilities:
/// - Validate every record has the same key set (or that missing keys are
///   intentional NULLs).
/// - Apply the manifest's primary-key list as the ON CONFLICT target.
/// - Honor `return_fields` (Postgres `RETURNING`, Mongo `findOneAndReplace`,
///   no-op + warning where unsupported).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalWrite {
    pub message_type: String,
    pub records: Vec<LogicalRecord>,
    #[serde(default)]
    pub conflict: ConflictStrategy,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub return_fields: Vec<String>,
}

/// A field assignment in a conditional update.
///
/// This is deliberately expression-shaped rather than "partial record" shaped:
/// native services need safe updates such as counters, server timestamps, and
/// presence-aware patches without hand-written SQL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LogicalAssignment {
    /// `field = value`
    Set { value: LogicalValue },
    /// `field = CURRENT_TIMESTAMP`
    ServerNow,
    /// `field = field + by` for numeric counters.
    Increment { by: LogicalValue },
    /// `field = COALESCE(value, field)` for proto optional partial updates.
    Coalesce { value: LogicalValue },
}

/// A neutral conditional update.
///
/// `filter` is required; there is no unbounded update path through this IR.
/// Callers that rely on optimistic concurrency set `require_affected = true`
/// and must treat zero affected rows as a failed precondition/not-found at the
/// service boundary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalUpdate {
    pub message_type: String,
    pub filter: LogicalFilter,
    pub assignments: BTreeMap<String, LogicalAssignment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub return_fields: Vec<String>,
    #[serde(default)]
    pub require_affected: bool,
}

/// How to handle a uniqueness collision on insert.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConflictStrategy {
    /// Fail the write with a typed `DuplicateKey` error. The default — the
    /// safest behaviour for a neutral broker.
    #[default]
    Error,
    /// Silently skip the conflicting row (SQL `ON CONFLICT DO NOTHING`).
    Ignore,
    /// Replace every column with the new value (full upsert). The standard
    /// Mongo `replaceOne(upsert: true)` shape.
    Replace,
    /// Update only the listed columns; leave others unchanged. Lowers to
    /// SQL `ON CONFLICT DO UPDATE SET col = EXCLUDED.col` for each.
    Update {
        fields: Vec<String>,
        /// The unique columns that arbitrate the conflict — the
        /// `ON CONFLICT (cols)` / `MERGE … ON` target / Mongo upsert filter.
        /// `None` ⇒ the manifest **primary key** (the historical default).
        /// `Some` ⇒ an **alternate** unique constraint (e.g. `key_hash` for api
        /// keys, `code` for tenants), so an upsert can collide on a non-PK
        /// unique index. The named columns must back a real unique/PK index on
        /// the target backend; the engine enforces this (fail-closed) if not.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conflict_on: Option<Vec<String>>,
    },
}

impl ConflictStrategy {
    /// Build a partial-update upsert keyed on the manifest primary key.
    pub fn update(fields: Vec<String>) -> Self {
        Self::Update {
            fields,
            conflict_on: None,
        }
    }

    /// Build a partial-update upsert keyed on an explicit alternate-unique
    /// column set (the `ON CONFLICT (conflict_on)` target).
    pub fn update_on(fields: Vec<String>, conflict_on: Vec<String>) -> Self {
        Self::Update {
            fields,
            conflict_on: Some(conflict_on),
        }
    }

    /// The explicit alternate-unique conflict target, if one was specified and
    /// non-empty. `None` means "use the manifest primary key" — every SQL
    /// compiler reads this to choose the `ON CONFLICT` / `MERGE` arbiter.
    pub fn conflict_target(&self) -> Option<&[String]> {
        match self {
            Self::Update {
                conflict_on: Some(cols),
                ..
            } if !cols.is_empty() => Some(cols),
            _ => None,
        }
    }
}

/// A neutral delete. `filter` is **required** — there is no
/// "delete everything" path through the IR. A caller that genuinely wants
/// to truncate has to use the resource-op surface (`Drop`+`Ensure`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalDelete {
    pub message_type: String,
    pub filter: LogicalFilter,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub return_fields: Vec<String>,
}

/// A neutral vector / hybrid search.
///
/// At least one of `vector` (dense embedding) or `text_query` (sparse /
/// lexical) must be non-empty. Backends that don't support hybrid lower
/// to whichever channel they support and surface the unused-channel as a
/// `CompileError::CapabilityRejected` if both were supplied with `require_hybrid`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalSearch {
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LogicalFilter>,
    pub top_k: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_threshold: Option<f32>,
    /// When both `vector` and `text_query` are set, refuse to compile to a
    /// backend that doesn't support true hybrid (i.e., don't silently lower).
    #[serde(default)]
    pub require_hybrid: bool,
    /// Include the vector itself in the response (large; off by default).
    #[serde(default)]
    pub with_vector: bool,
    /// Include the stored payload alongside the score (on by default —
    /// callers rarely want score-only results).
    #[serde(default = "default_true")]
    pub with_payload: bool,
}

fn default_true() -> bool {
    true
}

/// A neutral aggregation — GROUP BY + aggregate-function pipeline (NW2).
///
/// Aggregation is a first-class neutral op, not a search-with-extras: the
/// compiler lowers a `LogicalAggregate` to backend-native group-by syntax
/// (Postgres `GROUP BY`, Mongo `$group`, ClickHouse `GROUP BY` with
/// `arrayJoin`, Cassandra `GROUP BY` per-partition, etc.) so the broker
/// can route OLAP shapes without each caller hand-rolling the dialect.
///
/// Contract:
/// - `group_by` may be empty (single-row aggregate, e.g. `COUNT(*)`).
/// - `aggregates` must be non-empty — at least one measure to compute.
///   The compiler rejects empty aggregates with `Malformed` rather than
///   emit a no-op `SELECT FROM table GROUP BY …`.
/// - Each `AggregateExpr.alias` becomes the output column name; the broker
///   uses this as the cursor / result-row key. Aliases must be unique
///   within a single `LogicalAggregate`.
/// - `having` filters operate on aggregate output (post-GROUP BY) — its
///   field references resolve to aliases declared in `aggregates`, NOT
///   to manifest columns. The compiler keeps that distinction explicit
///   so a typo doesn't silently fall through to a column lookup.
/// - `sort` and `pagination` apply to the aggregated result set; sort
///   fields likewise reference aggregate aliases or group-by columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalAggregate {
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<LogicalFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    pub aggregates: Vec<AggregateExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<LogicalFilter>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort: Vec<LogicalSort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<LogicalPagination>,
}

/// One aggregate measure: `func(field) AS alias`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    /// Source field. For `Count` with no specific field, set this to `*`
    /// or leave it as the literal `"*"` — the compiler treats both as
    /// `COUNT(*)`. Every other function requires a real field.
    pub field: String,
    /// Output column name. Must be unique within the parent
    /// `LogicalAggregate.aggregates`. Compilers use it verbatim as the
    /// SQL `AS` alias / Mongo `$group._id` sibling key.
    pub alias: String,
}

/// Aggregate function the broker compiles into backend-native syntax.
///
/// Kept deliberately small — only operations every analytical SQL +
/// document store supports. Backend-specific extras (PG `percentile_cont`,
/// Mongo `$stdDevSamp`) go through a future `Custom { token }` arm so
/// the IR stays portable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunc {
    /// `COUNT(field)` (NULL-skipping) or `COUNT(*)` when `field == "*"`.
    Count,
    /// `COUNT(DISTINCT field)`. Refuses `field == "*"`.
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateFunc {
    /// Canonical SQL function token. Compilers that emit SQL use this
    /// directly so trace and cache-key shapes stay stable.
    pub fn sql_token(self) -> &'static str {
        match self {
            Self::Count | Self::CountDistinct => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

impl LogicalAggregate {
    pub fn count_all(message_type: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            message_type: message_type.into(),
            filter: None,
            group_by: Vec::new(),
            aggregates: vec![AggregateExpr {
                func: AggregateFunc::Count,
                field: "*".into(),
                alias: alias.into(),
            }],
            having: None,
            sort: Vec::new(),
            pagination: None,
        }
    }

    pub fn with_filter(mut self, f: LogicalFilter) -> Self {
        self.filter = Some(f);
        self
    }

    pub fn with_group_by(mut self, fields: Vec<String>) -> Self {
        self.group_by = fields;
        self
    }
}

/// A resource lifecycle op — ensure/drop/list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogicalResourceOp {
    pub op: ResourceOpKind,
    pub resource_kind: ResourceKind,
    pub resource_name: String,
    /// Resource-specific JSON spec (Qdrant `vectors_config`, S3
    /// `bucket_policy`, Postgres column list, Neo4j constraint shape, etc.).
    /// Validated by the compiler against the backend's capability matrix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceOpKind {
    Ensure,
    Drop,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// Relational table.
    Table,
    /// Mongo collection, Qdrant collection.
    Collection,
    /// S3 / MinIO bucket.
    Bucket,
    /// Any index.
    Index,
    /// Neo4j constraint.
    Constraint,
    /// Kafka topic (CDC sink lifecycle).
    Topic,
}

impl LogicalRead {
    pub fn message(message_type: impl Into<String>) -> Self {
        Self {
            message_type: message_type.into(),
            ..Self::default()
        }
    }

    pub fn with_filter(mut self, f: LogicalFilter) -> Self {
        self.filter = Some(f);
        self
    }

    pub fn with_sort(mut self, s: Vec<LogicalSort>) -> Self {
        self.sort = s;
        self
    }

    pub fn with_include(mut self, relation: impl Into<String>) -> Self {
        self.include.push(LogicalInclude {
            relation: relation.into(),
        });
        self
    }

    pub fn with_pagination(mut self, p: LogicalPagination) -> Self {
        self.pagination = Some(p);
        self
    }

    pub fn with_projection(mut self, p: LogicalProjection) -> Self {
        self.projection = Some(p);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::filter::ComparisonOp;

    #[test]
    fn read_builder_chains() {
        let r = LogicalRead::message("Customer")
            .with_filter(LogicalFilter::Comparison {
                field: "active".into(),
                op: ComparisonOp::Eq,
                value: LogicalValue::Bool(true),
            })
            .with_pagination(LogicalPagination::limit(50));
        assert_eq!(r.message_type, "Customer");
        assert!(r.filter.is_some());
        assert_eq!(r.pagination.unwrap().limit, Some(50));
    }

    #[test]
    fn conflict_default_is_error() {
        let w = LogicalWrite {
            message_type: "X".into(),
            records: vec![],
            conflict: ConflictStrategy::default(),
            return_fields: vec![],
        };
        assert_eq!(w.conflict, ConflictStrategy::Error);
    }

    #[test]
    fn conflict_on_is_back_compat_and_targeted() {
        // Legacy payload (no conflict_on) deserializes to the PK-keyed default.
        let legacy: ConflictStrategy =
            serde_json::from_str(r#"{"kind":"update","fields":["name"]}"#).expect("parse legacy");
        assert_eq!(legacy, ConflictStrategy::update(vec!["name".into()]));
        assert_eq!(
            legacy.conflict_target(),
            None,
            "no conflict_on ⇒ use the PK"
        );

        // Explicit alternate-key target round-trips and is exposed.
        let alt = ConflictStrategy::update_on(vec!["name".into()], vec!["email".into()]);
        assert_eq!(alt.conflict_target(), Some(&["email".to_string()][..]));
        let json = serde_json::to_string(&alt).expect("serialize");
        assert_eq!(
            serde_json::from_str::<ConflictStrategy>(&json).expect("round-trip"),
            alt
        );

        // An empty conflict_on is treated as "unset" (falls back to the PK).
        let empty = ConflictStrategy::Update {
            fields: vec!["name".into()],
            conflict_on: Some(vec![]),
        };
        assert_eq!(empty.conflict_target(), None);
    }

    #[test]
    fn search_defaults_to_payload_on() {
        // Round-trip an empty-ish search through serde and confirm with_payload
        // defaults to true (the documented "callers rarely want score-only").
        let json = r#"{"message_type":"Doc","top_k":5}"#;
        let s: LogicalSearch = serde_json::from_str(json).expect("parse");
        assert!(s.with_payload, "with_payload must default to true");
        assert!(!s.with_vector, "with_vector must default to false");
        assert!(!s.require_hybrid);
    }
}
