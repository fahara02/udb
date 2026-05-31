use crate::generation::manifest::ManifestStore;

pub(super) fn store_opt_str_any<'a>(store: &'a ManifestStore, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        store
            .options
            .iter()
            .find(|option| option.key == *key)
            .map(|option| option.value.trim())
            .filter(|value| !value.is_empty())
    })
}

pub(super) fn store_opt_i64_any(store: &ManifestStore, keys: &[&str], default: i64) -> i64 {
    store_opt_str_any(store, keys)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
}

pub(super) fn store_opt_bool_any(store: &ManifestStore, keys: &[&str], default: bool) -> bool {
    store_opt_str_any(store, keys)
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}

pub(super) fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(super) fn safe_identifier(value: &str, fallback: &str) -> String {
    let candidate = value.trim();
    if is_safe_identifier(candidate) {
        return candidate.to_string();
    }

    let mut out = String::new();
    for ch in candidate.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
        } else if ch == '-' || ch == '.' || ch == ':' || ch.is_whitespace() {
            out.push('_');
        }
    }
    if out.is_empty() || !is_safe_identifier(&out) {
        fallback.to_string()
    } else {
        out
    }
}

pub(super) fn safe_resource_name(value: &str, fallback: &str) -> String {
    let mut out = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        fallback.to_string()
    } else {
        out
    }
}

pub(super) fn safe_comment_value(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch == '\r' || ch == '\n' { ' ' } else { ch })
        .collect()
}

pub(super) fn quote_clickhouse_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

pub(super) fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}
