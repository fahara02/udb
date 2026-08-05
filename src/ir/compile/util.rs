//! Shared compiler utilities — value conversion, regex escaping, LIKE
//! translation. Used by `mongodb`, `neo4j`, and `clickhouse` compilers.
//!
//! Kept `pub(super)` (i.e. visible only inside `crate::ir::compile`)
//! because these are intentionally not part of the IR contract — they
//! live and die with the compilers.

use serde_json::{Value as Json, json};

use super::{CompileContext, CompileError};
use crate::generation::sql::{
    resolve_project_column as shared_resolve_project_column,
    resolve_tenant_column as shared_resolve_tenant_column,
};
use crate::generation::{CatalogManifest, ManifestForeignKey, ManifestTable};
use crate::ir::operations::{AggregateExpr, LogicalInclude};
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
        // INJ1 (identifier injection): the alias is client-controlled and every
        // backend interpolates it into an *identifier* position (`AS "<alias>"`,
        // `` AS `<alias>` ``, MSSQL `AS [<alias>]`, Cypher `AS \`<alias>\``) via a
        // naive quoter that does NOT double the closing delimiter. Reject any
        // alias that is not a plain identifier so a `"` / `` ` `` / `]` / `--` /
        // whitespace / `//` payload cannot break out of the quotes — which on the
        // no-RLS SQL backends (mysql/sqlite/mssql) would comment out the
        // compiler-injected tenant/project predicate (cross-tenant read), and on
        // Neo4j would inject arbitrary Cypher.
        if !is_safe_result_alias(&agg.alias) {
            return Err(CompileError::Malformed {
                reason: format!(
                    "invalid aggregate alias '{}': aliases must be a plain identifier \
                     matching [A-Za-z_][A-Za-z0-9_]* of at most 64 characters",
                    agg.alias
                ),
            });
        }
        if !seen.insert(agg.alias.as_str()) {
            return Err(CompileError::Malformed {
                reason: format!("duplicate aggregate alias '{}'", agg.alias),
            });
        }
    }
    Ok(())
}

/// A client-supplied result alias is safe to interpolate into any backend's
/// identifier-quoting only when it is a plain identifier: a leading ASCII letter
/// or `_`, then ASCII alphanumerics/`_`, bounded length. This is deliberately
/// stricter than "quotable" because the backends' alias quoters do not escape
/// their delimiter (see INJ1 in `validate_aggregate_aliases`).
pub(super) fn is_safe_result_alias(alias: &str) -> bool {
    if alias.is_empty() || alias.len() > 64 {
        return false;
    }
    let mut chars = alias.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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

pub(super) fn resolve_tenant_column(table: &ManifestTable) -> Option<&str> {
    shared_resolve_tenant_column(table)
}

pub(super) fn resolve_project_column(table: &ManifestTable) -> Option<&str> {
    shared_resolve_project_column(table)
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

/// The regconfig used when compiling text-search queries against a table's
/// generated tsvector column: the first tsvector column's declared language
/// with the SAME empty→`'simple'` default and unsafe-character fallback the
/// DDL renderer applies (`generation::sql::render_ext`). Emitting the config
/// explicitly keeps `plainto_tsquery` aligned with the indexed configuration
/// instead of depending on the session `default_text_search_config` (which is
/// commonly `english` and silently stems query terms into non-matching lexemes).
pub(super) fn tsvector_query_language(table: &ManifestTable) -> &str {
    let raw = table
        .columns
        .iter()
        .find(|c| c.is_tsvector || c.sql_type.eq_ignore_ascii_case("tsvector"))
        .map(|c| c.tsvector_language.trim())
        .unwrap_or("");
    if raw.is_empty() {
        return "simple";
    }
    crate::generation::sql::safe_ts_language(raw).unwrap_or("simple")
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

pub(super) struct IncludeRelation<'a> {
    pub(super) name: String,
    pub(super) target: &'a ManifestTable,
    pub(super) local_columns: Vec<&'a str>,
    pub(super) target_columns: Vec<&'a str>,
    pub(super) many: bool,
}

pub(super) fn resolve_include_relation<'a>(
    table: &'a ManifestTable,
    manifest: &'a CatalogManifest,
    include: &LogicalInclude,
    message_type: &str,
) -> Result<IncludeRelation<'a>, CompileError> {
    let requested = include.relation.trim();
    if !is_safe_relation_name(requested) {
        return Err(CompileError::Malformed {
            reason: format!(
                "include relation '{}' on message '{}' is not a safe relation name",
                include.relation, message_type
            ),
        });
    }

    let mut matches: Vec<IncludeRelation<'a>> = Vec::new();
    for fk in &table.foreign_keys {
        let Some(target) = target_table_for_fk(manifest, fk) else {
            continue;
        };
        let relation_name = relation_name_for_fk(table, target, fk);
        if relation_name != requested {
            continue;
        }
        let target_columns = if fk.ref_columns.is_empty() {
            target.primary_key.as_slice()
        } else {
            fk.ref_columns.as_slice()
        };
        if fk.columns.is_empty() || fk.columns.len() != target_columns.len() {
            return Err(CompileError::Malformed {
                reason: format!(
                    "include relation '{requested}' on message '{message_type}' has invalid FK column mapping"
                ),
            });
        }
        let mut local = Vec::with_capacity(fk.columns.len());
        let mut remote = Vec::with_capacity(target_columns.len());
        for (local_column, target_column) in fk.columns.iter().zip(target_columns.iter()) {
            local.push(resolve_physical_column(table, local_column, message_type)?);
            remote.push(resolve_physical_column(
                target,
                target_column,
                &manifest_message_type(target),
            )?);
        }
        matches.push(IncludeRelation {
            name: relation_name,
            target,
            local_columns: local,
            target_columns: remote,
            many: false,
        });
    }

    for candidate in &manifest.tables {
        for fk in &candidate.foreign_keys {
            if fk.columns.is_empty() || fk.ref_table != table.table {
                continue;
            }
            if !fk.ref_schema.trim().is_empty() && fk.ref_schema != table.schema {
                continue;
            }
            let relation_name = has_many_relation_name(candidate);
            if relation_name != requested {
                continue;
            }
            let local_columns = if fk.ref_columns.is_empty() {
                table.primary_key.as_slice()
            } else {
                fk.ref_columns.as_slice()
            };
            if local_columns.is_empty() || local_columns.len() != fk.columns.len() {
                return Err(CompileError::Malformed {
                    reason: format!(
                        "include relation '{requested}' on message '{message_type}' has invalid inverse FK column mapping"
                    ),
                });
            }
            let mut local = Vec::with_capacity(local_columns.len());
            let mut remote = Vec::with_capacity(fk.columns.len());
            for (local_column, target_column) in local_columns.iter().zip(fk.columns.iter()) {
                local.push(resolve_physical_column(table, local_column, message_type)?);
                remote.push(resolve_physical_column(
                    candidate,
                    target_column,
                    &manifest_message_type(candidate),
                )?);
            }
            matches.push(IncludeRelation {
                name: relation_name,
                target: candidate,
                local_columns: local,
                target_columns: remote,
                many: true,
            });
        }
    }

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(CompileError::Malformed {
            reason: format!("unknown include relation '{requested}' on message '{message_type}'"),
        }),
        _ => Err(CompileError::Malformed {
            reason: format!("ambiguous include relation '{requested}' on message '{message_type}'"),
        }),
    }
}

fn target_table_for_fk<'a>(
    manifest: &'a CatalogManifest,
    fk: &ManifestForeignKey,
) -> Option<&'a ManifestTable> {
    manifest.tables.iter().find(|candidate| {
        candidate.table == fk.ref_table
            && (fk.ref_schema.trim().is_empty() || candidate.schema == fk.ref_schema)
    })
}

fn resolve_physical_column<'a>(
    table: &'a ManifestTable,
    column: &str,
    message_type: &str,
) -> Result<&'a str, CompileError> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.column_name == column)
        .map(|candidate| candidate.column_name.as_str())
        .ok_or_else(|| CompileError::UnknownField {
            message_type: message_type.to_string(),
            field: column.to_string(),
        })
}

fn relation_name_for_fk(
    table: &ManifestTable,
    target: &ManifestTable,
    fk: &ManifestForeignKey,
) -> String {
    fk.columns
        .first()
        .and_then(|column| manifest_field_for_column(table, column))
        .map(|field| {
            field
                .strip_suffix("_id")
                .or_else(|| field.strip_suffix("_uuid"))
                .unwrap_or(&field)
                .to_string()
        })
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| alias_snake_case(&target.message_name))
}

fn has_many_relation_name(table: &ManifestTable) -> String {
    let name = alias_snake_case(&table.table).trim_matches('_').to_string();
    if !name.is_empty() {
        return name;
    }
    let alias = alias_snake_case(&table.message_name);
    if alias.ends_with('s') {
        alias
    } else if let Some(stem) = alias.strip_suffix('y') {
        format!("{stem}ies")
    } else {
        format!("{alias}s")
    }
}

fn manifest_field_for_column(table: &ManifestTable, column_name: &str) -> Option<String> {
    table
        .columns
        .iter()
        .find(|column| column.column_name == column_name)
        .map(|column| column.field_name.clone())
}

fn manifest_message_type(table: &ManifestTable) -> String {
    if table.proto_package.trim().is_empty() {
        table.message_name.clone()
    } else {
        format!("{}.{}", table.proto_package, table.message_name)
    }
}

fn alias_snake_case(value: &str) -> String {
    let mut out = String::new();
    for (idx, ch) in value.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if idx > 0 && !out.ends_with('_') {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn is_safe_relation_name(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}
