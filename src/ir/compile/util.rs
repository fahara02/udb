//! Shared compiler utilities — value conversion, regex escaping, LIKE
//! translation. Used by `mongodb`, `neo4j`, and `clickhouse` compilers.
//!
//! Kept `pub(super)` (i.e. visible only inside `crate::ir::compile`)
//! because these are intentionally not part of the IR contract — they
//! live and die with the compilers.

use serde_json::{Value as Json, json};

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
/// Used by compilers that fall back to regex predicates; marked allow
/// dead_code because not every compiler that pulls in `util` uses it.
#[allow(dead_code)]
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
