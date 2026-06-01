//! Shared compiler utilities — value conversion, regex escaping, LIKE
//! translation. Used by `mongodb`, `neo4j`, and `clickhouse` compilers.
//!
//! Kept `pub(super)` (i.e. visible only inside `crate::ir::compile`)
//! because these are intentionally not part of the IR contract — they
//! live and die with the compilers.

use serde_json::{Value as Json, json};

use super::{CompileContext, CompileError};
use crate::generation::ManifestTable;
use crate::ir::operations::AggregateExpr;
use crate::ir::value::LogicalValue;

/// Convert a `LogicalValue` to a JSON value the HTTP-shaped backends can
/// transmit. Mongo uses this for filter operands and document fields;
/// Neo4j uses it for Cypher parameter values; ClickHouse uses it for the
/// JSON-row INSERT format.
pub(super) fn value_to_json(v: &LogicalValue) -> Json {
    match v {
        LogicalValue::Null => Json::Null,
        LogicalValue::Bool(b) => Json::Bool(*b),
        LogicalValue::Int(i) => Json::Number((*i).into()),
        LogicalValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        LogicalValue::String(s) => Json::String(s.clone()),
        LogicalValue::Bytes(b) => {
            use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
            // Atlas Data API / Neo4j accept base64-encoded bytes (with
            // Mongo's `$binary` envelope) — Neo4j just takes the string
            // and stores it; the executor decides whether to wrap.
            json!({ "$binary": { "base64": B64.encode(b), "subType": "00" } })
        }
        LogicalValue::Timestamp(t) => json!({ "$date": t.to_rfc3339() }),
        LogicalValue::Json(j) => j.clone(),
        LogicalValue::Array(values) => Json::Array(values.iter().map(value_to_json).collect()),
    }
}

/// Translate a SQL LIKE pattern (`%`, `_`) into an anchored regex string.
/// Used by Mongo (`$regex`), Neo4j (`=~`), and any other backend whose
/// pattern matching is regex-shaped rather than LIKE-shaped.
pub(super) fn like_to_regex(pattern: &str) -> String {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            '.' | '\\' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' => {
                out.push('\\');
                out.push(ch);
            }
            other => out.push(other),
        }
    }
    out.push('$');
    out
}

/// Escape regex metacharacters in a literal substring (for `Contains` /
/// `StartsWith` / `EndsWith` ops, which don't accept `%`/`_` wildcards).
/// Used by compilers that fall back to regex predicates (Mongo, Neo4j).
pub(super) fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if matches!(
            ch,
            '.' | '\\' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
        ) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Reject duplicate aggregate aliases. Every backend that lowers a
/// `LogicalAggregate` needs the result row to have unambiguous keys, so the
/// alias set must be unique. Previously this exact check was copy-pasted into
/// ~11 backends; it now lives here and each backend calls it once.
pub(super) fn validate_aggregate_aliases(aggregates: &[AggregateExpr]) -> Result<(), CompileError> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for agg in aggregates {
        if !seen.insert(agg.alias.as_str()) {
            return Err(CompileError::Malformed {
                reason: format!("duplicate aggregate alias '{}'", agg.alias),
            });
        }
    }
    Ok(())
}

/// Reject a GROUP BY column whose resolved name collides with an aggregate
/// alias (#151). Group-by columns are emitted in the SELECT keyed by their
/// resolved column name and aggregates by their alias; a collision yields two
/// identically-named result columns, so callers reading by key get an
/// ambiguous/duplicated value. `group_column_names` are the RESOLVED (unquoted)
/// manifest column names for the group-by fields.
pub(super) fn validate_no_groupby_alias_collision(
    group_column_names: &[&str],
    aggregates: &[AggregateExpr],
) -> Result<(), CompileError> {
    if let Some(agg) = aggregates
        .iter()
        .find(|a| group_column_names.iter().any(|g| *g == a.alias.as_str()))
    {
        return Err(CompileError::Malformed {
            reason: format!(
                "aggregate alias '{}' collides with a GROUP BY column of the same name; \
                 rename the aggregate",
                agg.alias
            ),
        });
    }
    Ok(())
}

/// Resolve the tenant-isolation column for a table under ONE naming policy
/// shared by every compiler-layer-RLS backend (ClickHouse, Cassandra, …).
///
/// Policy (in order):
/// 1. the authoritative `is_tenant_column` manifest flag (proto source of
///    truth) — the first column carrying it wins;
/// 2. fall back to the conventional name list, matching either `tenant_id`
///    or `_tenant_id` against `column_name` or `field_name`
///    (case-insensitive). The `_`-prefixed form marks "system" columns.
///
/// Returns the physical `column_name` to emit. Accepting both `tenant_id`
/// and `_tenant_id` everywhere fixes the previous divergence where Cassandra
/// silently lost isolation for a `_tenant_id`-named key.
pub(super) fn resolve_tenant_column(table: &ManifestTable) -> Option<&str> {
    if let Some(c) = table.columns.iter().find(|c| c.is_tenant_column) {
        return Some(c.column_name.as_str());
    }
    table
        .columns
        .iter()
        .find(|c| {
            let cn = c.column_name.as_str();
            let fnm = c.field_name.as_str();
            cn.eq_ignore_ascii_case("tenant_id")
                || cn.eq_ignore_ascii_case("_tenant_id")
                || fnm.eq_ignore_ascii_case("tenant_id")
                || fnm.eq_ignore_ascii_case("_tenant_id")
        })
        .map(|c| c.column_name.as_str())
}

/// Resolve the project-isolation column. Same naming policy as
/// [`resolve_tenant_column`] but for `project_id` / `_project_id`. There is
/// no manifest flag for project columns, so this is name-list only.
pub(super) fn resolve_project_column(table: &ManifestTable) -> Option<&str> {
    table
        .columns
        .iter()
        .find(|c| {
            let cn = c.column_name.as_str();
            let fnm = c.field_name.as_str();
            cn.eq_ignore_ascii_case("project_id")
                || cn.eq_ignore_ascii_case("_project_id")
                || fnm.eq_ignore_ascii_case("project_id")
                || fnm.eq_ignore_ascii_case("_project_id")
        })
        .map(|c| c.column_name.as_str())
}

/// Tenant system-field for SCHEMALESS backends (MongoDB, Elasticsearch,
/// Neo4j, Pinecone, Qdrant, Weaviate). Returns the manifest-declared tenant
/// column when one exists (honoring `is_tenant_column` / the name list), else
/// falls back to the conventional `_tenant_id` system field. Unlike the
/// SQL/columnar backends (ClickHouse, Cassandra) — which must NOT reference a
/// column the table does not declare — these document/graph/vector stores
/// stamp and filter on a system field that is not a declared manifest column,
/// so resolution must always yield a name.
pub(super) fn tenant_system_field(table: &ManifestTable) -> &str {
    resolve_tenant_column(table).unwrap_or("_tenant_id")
}

/// Project system-field counterpart of [`tenant_system_field`]; falls back to
/// `_project_id`.
pub(super) fn project_system_field(table: &ManifestTable) -> &str {
    resolve_project_column(table).unwrap_or("_project_id")
}

/// Append tenant + project equality predicates to a WHERE/clause body for
/// the compiler-layer-RLS backends. `quote` is the dialect's identifier
/// quote char (`` ` `` for ClickHouse, `"` for Cassandra); each injected
/// predicate is `<quote>col<quote> = ?` and pushes one positional `?`
/// placeholder value into `params`.
///
/// Returns `None` only when there is no existing body AND nothing to inject.
pub(super) fn append_context_predicates(
    body: Option<String>,
    table: &ManifestTable,
    ctx: &CompileContext<'_>,
    params: &mut Vec<LogicalValue>,
    quote: char,
) -> Option<String> {
    let mut parts: Vec<String> = body.into_iter().collect();
    if let Some(tid) = ctx.tenant_id
        && !tid.is_empty()
        && let Some(col) = resolve_tenant_column(table)
    {
        params.push(LogicalValue::String(tid.to_string()));
        parts.push(format!("{quote}{col}{quote} = ?"));
    }
    if let Some(pid) = ctx.project_id
        && !pid.is_empty()
        && let Some(col) = resolve_project_column(table)
    {
        params.push(LogicalValue::String(pid.to_string()));
        parts.push(format!("{quote}{col}{quote} = ?"));
    }
    if parts.is_empty() {
        None
    } else if parts.len() == 1 {
        parts.pop()
    } else {
        Some(parts.join(" AND "))
    }
}
