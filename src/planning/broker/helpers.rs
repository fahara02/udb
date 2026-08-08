//! broker.rs split — helpers (Phase I).
use super::*;

pub(crate) fn sql_operator(value: &str) -> Option<&'static str> {
    match value.to_ascii_lowercase().as_str() {
        "$eq" | "=" => Some("="),
        "$ne" | "!=" => Some("!="),
        "$gt" | ">" => Some(">"),
        "$gte" | ">=" => Some(">="),
        "$lt" | "<" => Some("<"),
        "$lte" | "<=" => Some("<="),
        "$in" | "in" => Some("IN"),
        "$like" | "like" => Some("LIKE"),
        "$is_null" | "is_null" => Some("IS NULL"),
        // GAP 6: PostgreSQL-specific operators
        "$not_null" | "is_not_null" => Some("IS NOT NULL"),
        "$ilike" | "ilike" => Some("ILIKE"),
        "$contains" | "contains" => Some("@>"), // JSONB / array contains
        "$contained_by" | "contained_by" => Some("<@"), // JSONB / array contained-by
        "$has_key" | "has_key" => Some("?"),    // JSONB key exists
        "$overlaps" | "overlaps" => Some("&&"), // array overlap
        "$matches" | "matches" => Some("@@"),   // tsvector / jsonpath match
        _ => None,
    }
}

pub(crate) fn validate_write_context(context: &RequestContext) -> Vec<String> {
    let mut errors = Vec::new();
    if context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if context.purpose.trim().is_empty() {
        errors.push("purpose is required".to_string());
    }
    if !has_scope(context, "udb:write") {
        errors.push("scope udb:write is required".to_string());
    }
    errors
}

/// C22: per-table ABAC scope check. A table declaring `required_scope` demands it
/// in addition to the coarse verb scope (`udb:read`/`udb:write`). Returns the
/// error string when the caller lacks it, else `None`. Admin wildcard scopes
/// satisfy `has_scope`, so cross-tenant admins are unaffected.
pub(crate) fn table_required_scope_error(
    context: &RequestContext,
    table: &ManifestTable,
) -> Option<String> {
    let required = table.required_scope.trim();
    if !required.is_empty() && !has_scope(context, required) {
        Some(format!("scope {required} is required for this table"))
    } else {
        None
    }
}

pub(crate) fn validate_stream_context(context: &RequestContext) -> Vec<String> {
    let mut errors = Vec::new();
    if context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if !has_scope(context, "udb:stream") {
        errors.push("scope udb:stream is required".to_string());
    }
    errors
}

pub(crate) fn allowed_columns(table: &ManifestTable) -> BTreeSet<String> {
    table
        .columns
        .iter()
        .map(|column| column.column_name.clone())
        .collect()
}

/// #117: map every accepted field reference (proto `field_name` AND physical
/// `column_name`, lowercased) to the physical `column_name`. The IR compilers
/// accept proto `field_name` aliases; the broker planner must resolve them too,
/// or a column override / identifier normalization (`field_name != column_name`)
/// makes a valid request reject or bind the wrong column.
/// W2 (consumer tip 3, stage 1): the encrypted columns of a table mapped to
/// their blind-index sibling (`<col>_idx` declared with `is_blind_index`), or
/// "" when none exists. Filter compilation FAILS CLOSED on any predicate that
/// references one of these — AEAD ciphertext is randomized, so equality on the
/// encrypted column silently matches nothing, which is worse than an error.
pub(crate) fn encrypted_filter_columns(
    table: &ManifestTable,
) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for column in &table.columns {
        if !column.security.is_encrypted {
            continue;
        }
        let idx_name = format!("{}_idx", column.column_name);
        let sibling = table
            .columns
            .iter()
            .find(|c| c.column_name == idx_name && c.security.is_blind_index)
            .map(|c| c.column_name.clone())
            .unwrap_or_default();
        out.insert(column.column_name.clone(), sibling);
    }
    out
}

pub(crate) fn column_resolver(table: &ManifestTable) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for column in &table.columns {
        map.insert(
            column.column_name.to_ascii_lowercase(),
            column.column_name.clone(),
        );
        if !column.field_name.is_empty() {
            map.entry(column.field_name.to_ascii_lowercase())
                .or_insert_with(|| column.column_name.clone());
        }
    }
    map
}

/// Resolve one field reference to its physical column name, falling back to the
/// lowercased input when unknown so downstream validation still surfaces it (#117).
pub(crate) fn resolve_column(
    resolver: &std::collections::HashMap<String, String>,
    name: &str,
) -> String {
    let key = name.to_ascii_lowercase();
    resolver.get(&key).cloned().unwrap_or(key)
}

/// Rewrite the TOP-LEVEL keys of an upsert record (proto `field_name`s) to their
/// physical `column_name`s so binding by column name finds each value. Only
/// top-level keys are columns — nested objects are JSONB column *values* and are
/// left untouched (#117).
pub(crate) fn normalize_record_keys(table: &ManifestTable, record: &Value) -> Value {
    let resolver = column_resolver(table);
    match record {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (resolve_column(&resolver, key), value.clone()))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Rewrite filter object keys (proto `field_name`s) to physical `column_name`s,
/// preserving logical (`$and`/`$or`) and comparison (`$eq`/`$in`/…) operator keys
/// and recursing into nested groups (#117).
pub(crate) fn normalize_filter_keys(
    resolver: &std::collections::HashMap<String, String>,
    value: &Value,
) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, nested)| {
                    let lower = key.to_ascii_lowercase();
                    if matches!(lower.as_str(), "$and" | "$or" | "and" | "or")
                        || is_operator(&lower)
                    {
                        // Logical / comparison operator: keep the key, recurse the value.
                        (key.clone(), normalize_filter_keys(resolver, nested))
                    } else {
                        // Field reference: resolve to the physical column name.
                        (
                            resolve_column(resolver, key),
                            normalize_filter_keys(resolver, nested),
                        )
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| normalize_filter_keys(resolver, item))
                .collect(),
        ),
        other => other.clone(),
    }
}

pub(crate) fn is_server_owned_column(table: &ManifestTable, column_name: &str) -> bool {
    table.columns.iter().any(|column| {
        column.column_name == column_name
            && (column.generated
                || column.is_identity
                || column.auto_increment
                || column.exclude_from_insert)
    })
}

pub(crate) fn is_update_excluded_column(table: &ManifestTable, column_name: &str) -> bool {
    table.columns.iter().any(|column| {
        column.column_name == column_name
            && (column.generated
                || column.is_identity
                || column.is_primary
                || column.auto_increment
                || column.exclude_from_update)
    })
}

pub(crate) fn conflict_target_is_unique(table: &ManifestTable, columns: &[String]) -> bool {
    let normalized = sorted_columns(columns);
    if !table.primary_key.is_empty() && sorted_columns(&table.primary_key) == normalized {
        return true;
    }
    table.indexes.iter().any(|index| {
        index.unique
            && index.where_clause.trim().is_empty()
            && index
                .columns
                .iter()
                .all(|column| !column.contains('(') && !column.contains('\''))
            && sorted_columns(&index.columns) == normalized
    })
}

/// Human-readable list of the conflict targets an upsert may legally use: the
/// primary key and every full-column (non-partial, non-expression) unique index.
/// Mirrors the acceptance rule in [`conflict_target_is_unique`] so a rejected
/// `conflict_fields` set names exactly what the caller could have matched instead
/// of the bare "must match the primary key or a declared unique index".
pub(crate) fn describe_valid_conflict_targets(table: &ManifestTable) -> String {
    let mut targets = Vec::new();
    if !table.primary_key.is_empty() {
        targets.push(format!("primary key ({})", table.primary_key.join(", ")));
    }
    for index in &table.indexes {
        if index.unique
            && index.where_clause.trim().is_empty()
            && index
                .columns
                .iter()
                .all(|column| !column.contains('(') && !column.contains('\''))
        {
            targets.push(format!("unique index ({})", index.columns.join(", ")));
        }
    }
    if targets.is_empty() {
        "this entity declares no primary key or full-column unique index".to_string()
    } else {
        targets.join("; ")
    }
}

pub(crate) fn sorted_columns(columns: &[String]) -> Vec<String> {
    let mut out = columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<Vec<_>>();
    out.sort();
    out
}

pub(crate) fn store_option(store: &crate::generation::ManifestStore, key: &str) -> String {
    store
        .options
        .iter()
        .find(|option| option.key == key)
        .map(|option| option.value.clone())
        .unwrap_or_default()
}

pub(crate) fn store_option_i32(store: &crate::generation::ManifestStore, key: &str) -> i32 {
    store_option(store, key).parse::<i32>().unwrap_or_default()
}

pub(crate) fn store_option_bool(store: &crate::generation::ManifestStore, key: &str) -> bool {
    matches!(store_option(store, key).as_str(), "true" | "TRUE" | "1")
}

pub(crate) fn collect_payload_fields(value: &Value, out: &mut Vec<String>) {
    if let Value::Object(map) = value {
        for (key, nested) in map {
            if !key.starts_with('$') {
                out.push(key.to_ascii_lowercase());
            }
            collect_payload_fields(nested, out);
        }
    }
}

pub(crate) fn normalize_store_kind(value: &str) -> String {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "document" | "doc" | "nosql" | "no_sql" => "document".to_string(),
        "timeseries" | "time_series" => "timeseries".to_string(),
        "columnar" | "column_store" | "wide_column" => "column".to_string(),
        "blob" | "storage" => "object".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn quote_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| qi(value))
        .collect::<Vec<_>>()
        .join(", ")
}

// `qi` is imported from the canonical `generation::sql::qi` (single-sourced).
