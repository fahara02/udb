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
