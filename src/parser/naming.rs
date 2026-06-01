pub(super) fn infer_sql_type(proto_type: &str) -> String {
    // `map<K, V>` (and bare `map`) store as JSONB. Checked on the full type
    // before the dot-split, since a dotted value type (e.g. `map<string,
    // foo.Bar>`) would otherwise leave `base` as "Bar>".
    if proto_type.to_ascii_lowercase().starts_with("map<") {
        return "JSONB".to_string();
    }
    let base = proto_type.rsplit('.').next().unwrap_or(proto_type);
    match base.to_ascii_lowercase().as_str() {
        "string" => "TEXT",
        "bool" => "BOOLEAN",
        "int32" | "sint32" | "sfixed32" | "fixed32" | "uint32" => "INTEGER",
        "int64" | "sint64" | "sfixed64" | "fixed64" | "uint64" => "BIGINT",
        "float" => "REAL",
        "double" => "DOUBLE PRECISION",
        "bytes" => "BYTEA",
        "timestamp" => "TIMESTAMPTZ",
        "duration" => "INTERVAL",
        "any" | "struct" | "value" | "listvalue" | "map" => "JSONB",
        "fieldmask" | "bytesvalue" | "stringvalue" => "TEXT",
        "int32value" | "uint32value" => "INTEGER",
        "int64value" | "uint64value" => "BIGINT",
        "boolvalue" => "BOOLEAN",
        "floatvalue" => "REAL",
        "doublevalue" => "DOUBLE PRECISION",
        _ if base.chars().next().is_some_and(|ch| ch.is_uppercase()) => "JSONB",
        _ => "TEXT",
    }
    .to_string()
}

pub(super) fn normalize_referential_action(value: &str) -> String {
    normalize_enum(value)
        .trim_start_matches("REFERENTIAL_ACTION_")
        .replace('_', " ")
}

pub(super) fn normalize_index_type(value: &str) -> String {
    let value = normalize_enum(value);
    value
        .trim_start_matches("INDEX_TYPE_")
        .replace("UNSPECIFIED", "")
        .replace("NONE", "")
}

pub(super) fn normalize_enum(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

pub(super) fn normalize_store_kind(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("STORE_KIND_")
        .replace('_', "-")
        .to_ascii_lowercase()
}

pub(super) fn normalize_backend(value: &str) -> String {
    let value = normalize_enum(value);
    for prefix in [
        "SQL_BACKEND_",
        "NOSQL_BACKEND_",
        "VECTOR_BACKEND_",
        "GRAPH_BACKEND_",
        "TIMESERIES_BACKEND_",
        "TIME_SERIES_BACKEND_",
        "COLUMN_BACKEND_",
        "CACHE_BACKEND_",
        "STORAGE_BACKEND_",
        "OBJECT_BACKEND_",
        "MODEL_BACKEND_",
    ] {
        if let Some(stripped) = value.strip_prefix(prefix) {
            return stripped.to_ascii_lowercase();
        }
    }
    value.to_ascii_lowercase()
}

pub(super) fn parse_bool(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

pub(super) fn parse_i32(value: &str) -> i32 {
    value.parse::<i32>().unwrap_or_default()
}

pub(super) fn to_snake_case(value: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = value.trim().chars().collect();
    for (idx, ch) in chars.iter().copied().enumerate() {
        if ch.is_uppercase() {
            if idx > 0
                && (!chars[idx - 1].is_uppercase()
                    || chars.get(idx + 1).is_some_and(|next| next.is_lowercase()))
            {
                out.push('_');
            }
            out.extend(ch.to_lowercase());
        } else if ch == '-' || ch == ' ' {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

pub(super) fn to_plural(value: &str) -> String {
    if value.is_empty() {
        return String::new();
    }
    if value.ends_with('s')
        || value.ends_with('x')
        || value.ends_with("sh")
        || value.ends_with("ch")
    {
        if value.ends_with("ss") {
            return format!("{value}es");
        }
        if value.ends_with("us") {
            return value.to_string();
        }
        return value.to_string();
    }
    if value.ends_with("ey") {
        return format!("{value}s");
    }
    if let Some(stem) = value.strip_suffix('y') {
        let previous = value.chars().rev().nth(1).unwrap_or_default();
        if !"aeiou".contains(previous) {
            return format!("{stem}ies");
        }
    }
    if let Some(stem) = value.strip_suffix("fe") {
        return format!("{stem}ves");
    }
    if value.ends_with('f') && !value.ends_with("ff") {
        let stem = &value[..value.len() - 1];
        return format!("{stem}ves");
    }
    format!("{value}s")
}
