//! Keyset (cursor) pagination for the relational read path (P-1).
//!
//! `SelectRequest.page_token` / `RecordSet.next_page_token` existed in the proto
//! but were dead wire — the executor read neither and set no cursor, so a large
//! result set could not be walked deterministically. This module is the
//! mechanism: an opaque, tenant-bound page token plus the lexicographic
//! "after this cursor" predicate, expressed in the SAME wire filter grammar the
//! planner already compiles (so it reuses the validated predicate path and
//! tenant/RLS scoping rather than emitting new SQL).
//!
//! A cursor requires a TOTAL order — the caller's `sort` keys plus the manifest
//! primary key appended as tiebreakers — so no two rows share a cursor position.

use base64::Engine as _;
use serde_json::{Value as JsonValue, json};

/// One ordered key participating in the cursor: physical column + direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorKey {
    pub column: String,
    pub descending: bool,
}

/// Encode the last row's ordered key values into an opaque, tenant+entity-bound
/// page token. The token is bound to `(tenant_id, message_type)` and validated on
/// decode, so a cursor minted for one tenant/entity is refused for another — the
/// query is already tenant-scoped by RLS + the mandatory tenant predicate, so
/// this is defense-in-depth, not the isolation boundary itself.
pub(crate) fn encode_page_token(
    tenant_id: &str,
    message_type: &str,
    keys: &[(String, JsonValue)],
) -> String {
    let payload = json!({
        "t": tenant_id,
        "m": message_type,
        "k": keys
            .iter()
            .map(|(col, val)| json!([col, val]))
            .collect::<Vec<_>>(),
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Decode + validate a page token against the request's `(tenant_id,
/// message_type)`. Returns the ordered `(column, value)` cursor, or a stable
/// error string the caller maps to `INVALID_ARGUMENT`.
pub(crate) fn decode_page_token(
    token: &str,
    tenant_id: &str,
    message_type: &str,
) -> Result<Vec<(String, JsonValue)>, String> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token.trim())
        .map_err(|_| "page_token is malformed".to_string())?;
    let payload: JsonValue =
        serde_json::from_slice(&bytes).map_err(|_| "page_token is malformed".to_string())?;
    if payload.get("t").and_then(JsonValue::as_str) != Some(tenant_id) {
        return Err("page_token was issued for a different tenant".to_string());
    }
    if payload.get("m").and_then(JsonValue::as_str) != Some(message_type) {
        return Err("page_token was issued for a different entity".to_string());
    }
    let entries = payload
        .get("k")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| "page_token is malformed".to_string())?;
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let arr = entry
            .as_array()
            .ok_or_else(|| "page_token is malformed".to_string())?;
        let column = arr
            .first()
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "page_token is malformed".to_string())?;
        let value = arr
            .get(1)
            .cloned()
            .ok_or_else(|| "page_token is malformed".to_string())?;
        // A NULL cursor component would compile to `col < NULL` → UNKNOWN → zero
        // rows silently. Reject it: nullable columns are not valid cursor keys.
        if value.is_null() {
            return Err("page_token contains a null cursor value".to_string());
        }
        out.push((column.to_string(), value));
    }
    Ok(out)
}

/// Build the lexicographic "strictly after this cursor" predicate in the wire
/// filter grammar, as an `$or` of `$and` prefixes:
///
/// ```text
/// (k0 > v0)
///   OR (k0 = v0 AND k1 > v1)
///   OR (k0 = v0 AND k1 = v1 AND k2 > v2) ...
/// ```
///
/// A descending key uses `<` instead of `>`. `keys` and `values` must be the same
/// length and aligned; the caller guarantees non-null values (see `decode`).
/// The result is combined with the caller filter under `$and`, keeping the tenant
/// predicate in the top-level conjunction (X-4).
pub(crate) fn build_cursor_predicate(keys: &[CursorKey], values: &[JsonValue]) -> JsonValue {
    let mut branches = Vec::with_capacity(keys.len());
    for i in 0..keys.len().min(values.len()) {
        let mut clause = serde_json::Map::new();
        for j in 0..i {
            clause.insert(keys[j].column.clone(), json!({ "$eq": values[j].clone() }));
        }
        let op = if keys[i].descending { "$lt" } else { "$gt" };
        clause.insert(keys[i].column.clone(), json!({ op: values[i].clone() }));
        branches.push(JsonValue::Object(clause));
    }
    json!({ "$or": branches })
}

/// Resolve `(physical_column, descending)` sort keys into cursor keys, then append
/// any primary-key columns not already present as ascending tiebreakers, so the
/// order is TOTAL — required for a stable cursor (no two rows share a position).
pub(crate) fn total_order_keys(sort: &[(String, bool)], primary_key: &[String]) -> Vec<CursorKey> {
    let mut keys: Vec<CursorKey> = sort
        .iter()
        .map(|(col, desc)| CursorKey {
            column: col.clone(),
            descending: *desc,
        })
        .collect();
    for pk in primary_key {
        if !keys.iter().any(|k| &k.column == pk) {
            keys.push(CursorKey {
                column: pk.clone(),
                descending: false,
            });
        }
    }
    keys
}

/// Align decoded token entries to the current total-order keys, erroring if they
/// diverge — changing the sort mid-pagination invalidates the cursor (AIP-158).
pub(crate) fn cursor_values_for_keys(
    keys: &[CursorKey],
    decoded: &[(String, JsonValue)],
) -> Result<Vec<JsonValue>, String> {
    if decoded.len() != keys.len() {
        return Err("page_token does not match the current sort".to_string());
    }
    let mut out = Vec::with_capacity(keys.len());
    for (key, (col, val)) in keys.iter().zip(decoded.iter()) {
        if &key.column != col {
            return Err("page_token does not match the current sort".to_string());
        }
        out.push(val.clone());
    }
    Ok(out)
}

/// Extract the next-cursor `(column, value)` pairs (aligned to `keys`) from a
/// decoded row object. Returns `None` if any key column is absent or null, so the
/// caller omits the token rather than mint a broken cursor.
pub(crate) fn cursor_values_from_row(
    keys: &[CursorKey],
    row: &serde_json::Map<String, JsonValue>,
) -> Option<Vec<(String, JsonValue)>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let value = row.get(&key.column)?;
        if value.is_null() {
            return None;
        }
        out.push((key.column.clone(), value.clone()));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_round_trips_and_is_tenant_and_entity_bound() {
        let keys = vec![
            ("created_at".to_string(), json!("2026-07-23T00:00:00Z")),
            ("id".to_string(), json!("row-9")),
        ];
        let token = encode_page_token("tenant-a", "acme.Order", &keys);

        // Correct tenant+entity round-trips.
        let decoded = decode_page_token(&token, "tenant-a", "acme.Order").expect("decode");
        assert_eq!(decoded, keys);

        // Wrong tenant / entity is refused.
        assert!(decode_page_token(&token, "tenant-b", "acme.Order").is_err());
        assert!(decode_page_token(&token, "tenant-a", "acme.Other").is_err());
        // Garbage is refused, not panicked.
        assert!(decode_page_token("!!!not-base64!!!", "tenant-a", "acme.Order").is_err());
    }

    #[test]
    fn null_cursor_value_is_rejected() {
        let token = encode_page_token("t", "E", &[("c".to_string(), JsonValue::Null)]);
        let err = decode_page_token(&token, "t", "E").unwrap_err();
        assert!(err.contains("null"), "got: {err}");
    }

    #[test]
    fn total_order_appends_pk_tiebreakers() {
        let keys = total_order_keys(&[("created_at".to_string(), true)], &["id".to_string()]);
        assert_eq!(
            keys,
            vec![
                CursorKey {
                    column: "created_at".to_string(),
                    descending: true
                },
                CursorKey {
                    column: "id".to_string(),
                    descending: false
                },
            ]
        );
        // A PK already in the sort is not duplicated.
        let keys2 = total_order_keys(&[("id".to_string(), false)], &["id".to_string()]);
        assert_eq!(keys2.len(), 1);
    }

    #[test]
    fn cursor_values_align_or_error_on_sort_change() {
        let keys = total_order_keys(&[("ts".to_string(), false)], &["id".to_string()]);
        let decoded = vec![("ts".to_string(), json!(1)), ("id".to_string(), json!(2))];
        assert_eq!(
            cursor_values_for_keys(&keys, &decoded).unwrap(),
            vec![json!(1), json!(2)]
        );
        // Different columns → the sort changed mid-pagination → error.
        let bad = vec![
            ("other".to_string(), json!(1)),
            ("id".to_string(), json!(2)),
        ];
        assert!(cursor_values_for_keys(&keys, &bad).is_err());
    }

    #[test]
    fn cursor_values_from_row_extracts_or_omits() {
        let keys = total_order_keys(&[("ts".to_string(), false)], &["id".to_string()]);
        let row: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"ts": "2026", "id": "r1", "other": 9})).unwrap();
        assert_eq!(
            cursor_values_from_row(&keys, &row).unwrap(),
            vec![
                ("ts".to_string(), json!("2026")),
                ("id".to_string(), json!("r1"))
            ]
        );
        // A null key value → None (don't mint a broken cursor).
        let row_null: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"ts": null, "id": "r1"})).unwrap();
        assert!(cursor_values_from_row(&keys, &row_null).is_none());
    }

    #[test]
    fn single_key_predicate_is_a_bare_comparison() {
        let keys = vec![CursorKey {
            column: "id".to_string(),
            descending: false,
        }];
        let pred = build_cursor_predicate(&keys, &[json!(10)]);
        assert_eq!(pred, json!({ "$or": [ { "id": { "$gt": 10 } } ] }));
    }

    #[test]
    fn composite_key_predicate_is_lexicographic_or_of_and_prefixes() {
        let keys = vec![
            CursorKey {
                column: "created_at".to_string(),
                descending: false,
            },
            CursorKey {
                column: "id".to_string(),
                descending: true,
            },
        ];
        let pred = build_cursor_predicate(&keys, &[json!("2026"), json!(5)]);
        assert_eq!(
            pred,
            json!({ "$or": [
                { "created_at": { "$gt": "2026" } },
                { "created_at": { "$eq": "2026" }, "id": { "$lt": 5 } },
            ] })
        );
    }
}
