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

/// One ordered key participating in the cursor: physical column + direction,
/// plus whether the column can hold NULL (see [`build_cursor_predicate`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorKey {
    pub column: String,
    pub descending: bool,
    pub nullable: bool,
}

/// Version tag for the query-shape digest carried in a page token. Bump this to
/// invalidate every outstanding token when the set of query inputs that affect a
/// cursor's validity changes.
const PAGE_TOKEN_QUERY_VERSION: u32 = 1;

/// Versioned digest binding a page token to the QUERY that minted it — the
/// normalized filter, the resolved (physical) sort, and the caller's projection.
/// A token minted for one query must be refused for a different one: otherwise
/// the decoded "after this cursor" predicate is silently AND-ed onto an unrelated
/// filter and rows before the foreign cursor are dropped from the page (an
/// incomplete result set returned with NO error — see the P-1 result-integrity
/// defect). The filter is canonicalized (object keys sorted recursively) and the
/// projection is treated as a SET (sorted, de-duplicated), so only a *semantic*
/// change to the query — not incidental key/field ordering — rotates the digest.
pub(crate) fn query_digest(
    normalized_filter: &JsonValue,
    resolved_sort: &[(String, bool)],
    projection: &[String],
) -> String {
    let mut projection_sorted: Vec<&str> = projection.iter().map(String::as_str).collect();
    projection_sorted.sort_unstable();
    projection_sorted.dedup();
    let payload = json!({
        "v": PAGE_TOKEN_QUERY_VERSION,
        "f": normalized_filter,
        "s": resolved_sort
            .iter()
            .map(|(col, desc)| json!([col, desc]))
            .collect::<Vec<_>>(),
        "p": projection_sorted,
    });
    // Canonicalize the WHOLE payload (recursively sorting object keys) before
    // hashing so structurally-equal queries share a digest regardless of key
    // insertion order — robust even if serde_json's `preserve_order` feature is
    // enabled elsewhere in the workspace.
    let canonical = serde_json::to_string(&canonicalize(&payload)).unwrap_or_default();
    crate::runtime::executor_utils::checksum_str(&canonical)
}

/// Recursively rebuild a JSON value with object keys sorted, so two structurally
/// equal values serialize identically regardless of key insertion order. Array
/// order is PRESERVED — element order is semantic.
fn canonicalize(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut entries: Vec<(&String, &JsonValue)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            JsonValue::Object(
                entries
                    .into_iter()
                    .map(|(key, val)| (key.clone(), canonicalize(val)))
                    .collect(),
            )
        }
        JsonValue::Array(items) => JsonValue::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Encode the last row's ordered key values into an opaque, tenant+entity+query-
/// bound page token. The token is bound to `(tenant_id, message_type,
/// query_digest)` and validated on decode, so a cursor minted for one
/// tenant/entity/query is refused for another. Tenant/entity binding is
/// defense-in-depth on top of RLS + the mandatory tenant predicate; the
/// `query_digest` binding is the actual correctness guard against continuing a
/// walk against a *different* filter/sort/projection (P-1).
pub(crate) fn encode_page_token(
    tenant_id: &str,
    message_type: &str,
    query_digest: &str,
    keys: &[(String, JsonValue)],
) -> String {
    let payload = json!({
        "t": tenant_id,
        "m": message_type,
        "q": query_digest,
        "k": keys
            .iter()
            .map(|(col, val)| json!([col, val]))
            .collect::<Vec<_>>(),
    });
    let bytes = serde_json::to_vec(&payload).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Decode + validate a page token against the request's `(tenant_id,
/// message_type, query_digest)`. Returns the ordered `(column, value)` cursor, or
/// a stable error string the caller maps to `INVALID_ARGUMENT`. A token minted
/// for a different query (or a pre-upgrade token that carries no `q` field) is
/// rejected, so a walk can never silently continue from an unrelated cursor.
pub(crate) fn decode_page_token(
    token: &str,
    tenant_id: &str,
    message_type: &str,
    query_digest: &str,
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
    if payload.get("q").and_then(JsonValue::as_str) != Some(query_digest) {
        return Err("page_token was issued for a different query".to_string());
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
        // A NULL component is a legitimate position on a nullable sort key;
        // `build_cursor_predicate` renders it with IS NULL / IS NOT NULL, never
        // as a `col < NULL` comparison.
        let value = arr
            .get(1)
            .cloned()
            .ok_or_else(|| "page_token is malformed".to_string())?;
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
/// length and aligned. The result is combined with the caller filter under
/// `$and`, keeping the tenant predicate in the top-level conjunction (X-4).
///
/// NULL sort keys follow Postgres' default ordering (the ORDER BY emits no NULLS
/// clause): NULL sorts as the LARGEST value — last when ascending, first when
/// descending. So for a `nullable` key:
/// - ascending, cursor value `v`: after = `k > v OR k IS NULL`;
/// - ascending, cursor NULL: nothing on this key sorts after it (branch omitted);
/// - descending, cursor `v`: after = `k < v` (the NULLs came first);
/// - descending, cursor NULL: after = `k IS NOT NULL`;
/// - a NULL prefix component matches with `IS NULL`, never `= NULL`.
///
/// Without this, a full page ending on a NULL key could mint no cursor (silently
/// ending the walk) and an ascending walk never reached the NULL rows at all.
pub(crate) fn build_cursor_predicate(keys: &[CursorKey], values: &[JsonValue]) -> JsonValue {
    let mut branches = Vec::with_capacity(keys.len());
    for i in 0..keys.len().min(values.len()) {
        let mut clause = serde_json::Map::new();
        for j in 0..i {
            let prefix = if values[j].is_null() {
                json!({ "$is_null": true })
            } else {
                json!({ "$eq": values[j].clone() })
            };
            clause.insert(keys[j].column.clone(), prefix);
        }
        let key = &keys[i];
        let value = &values[i];
        match (key.descending, value.is_null()) {
            // Ascending past NULL: NULL is the largest value, nothing follows.
            (false, true) => continue,
            (true, true) => {
                clause.insert(key.column.clone(), json!({ "$not_null": true }));
            }
            (false, false) if key.nullable => {
                clause.insert(
                    "$or".to_string(),
                    json!([
                        { key.column.clone(): { "$gt": value.clone() } },
                        { key.column.clone(): { "$is_null": true } },
                    ]),
                );
            }
            (descending, false) => {
                let op = if descending { "$lt" } else { "$gt" };
                clause.insert(key.column.clone(), json!({ op: value.clone() }));
            }
        }
        branches.push(JsonValue::Object(clause));
    }
    json!({ "$or": branches })
}

/// Refine the nullability of each cursor key from the table's column metadata:
/// a key is nullable unless `non_null(column)` says the column can never hold
/// NULL (declared NOT NULL or part of the primary key).
pub(crate) fn with_nullability(
    keys: Vec<CursorKey>,
    non_null: impl Fn(&str) -> bool,
) -> Vec<CursorKey> {
    keys.into_iter()
        .map(|key| CursorKey {
            nullable: key.nullable && !non_null(&key.column),
            ..key
        })
        .collect()
}

/// Resolve `(physical_column, descending)` sort keys into cursor keys, then append
/// any primary-key columns not already present as ascending tiebreakers, so the
/// order is TOTAL — required for a stable cursor (no two rows share a position).
pub(crate) fn total_order_keys(sort: &[(String, bool)], primary_key: &[String]) -> Vec<CursorKey> {
    // A caller sort key is conservatively nullable unless it is a primary-key
    // column; `with_nullability` refines it from the manifest.
    let mut keys: Vec<CursorKey> = sort
        .iter()
        .map(|(col, desc)| CursorKey {
            column: col.clone(),
            descending: *desc,
            nullable: !primary_key.contains(col),
        })
        .collect();
    for pk in primary_key {
        if !keys.iter().any(|k| &k.column == pk) {
            keys.push(CursorKey {
                column: pk.clone(),
                descending: false,
                nullable: false,
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
/// decoded row object. A NULL value is a valid position (see
/// [`build_cursor_predicate`]); returns `None` only when a key column is absent
/// from the row.
pub(crate) fn cursor_values_from_row(
    keys: &[CursorKey],
    row: &serde_json::Map<String, JsonValue>,
) -> Option<Vec<(String, JsonValue)>> {
    let mut out = Vec::with_capacity(keys.len());
    for key in keys {
        let value = row.get(&key.column)?;
        out.push((key.column.clone(), value.clone()));
    }
    Some(out)
}

/// The cursor for the page AFTER a FULL page whose last row is `row`.
///
/// A full page means more rows may follow, so a missing cursor must never be
/// read as "last page" (an empty token): that would silently truncate the walk.
/// The read forces every key column into its projection, so an absent key is a
/// broken invariant and is refused, naming the column.
pub(crate) fn next_page_cursor(
    keys: &[CursorKey],
    row: &serde_json::Map<String, JsonValue>,
) -> Result<Vec<(String, JsonValue)>, String> {
    cursor_values_from_row(keys, row).ok_or_else(|| {
        let column = keys
            .iter()
            .find(|key| !row.contains_key(&key.column))
            .map(|key| key.column.as_str())
            .unwrap_or_default();
        format!(
            "cannot mint the next page token: sort key '{column}' is missing from the \
             returned row"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // A full page ending on a NULL sort key still mints a cursor (the walk does
    // not silently end); only a key absent from the row is refused.
    #[test]
    fn next_page_cursor_keeps_walking_past_a_null_sort_key() {
        let keys = total_order_keys(&[("ts".to_string(), false)], &["id".to_string()]);
        let row_null: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"ts": null, "id": "r1"})).unwrap();
        assert_eq!(
            next_page_cursor(&keys, &row_null).unwrap(),
            vec![
                ("ts".to_string(), JsonValue::Null),
                ("id".to_string(), json!("r1"))
            ]
        );
        let row_missing: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"id": "r1"})).unwrap();
        let err = next_page_cursor(&keys, &row_missing).expect_err("absent key");
        assert!(err.contains("sort key 'ts' is missing"), "{err}");
    }

    // Ascending nullable key: NULLs sort last, so a non-null cursor must still
    // reach them, and a NULL cursor continues only within the NULL run.
    #[test]
    fn ascending_nullable_key_predicate_reaches_and_walks_the_null_rows() {
        let keys = total_order_keys(&[("ts".to_string(), false)], &["id".to_string()]);
        assert!(keys[0].nullable && !keys[1].nullable);
        let after_value = build_cursor_predicate(&keys, &[json!("2026"), json!("r1")]);
        assert_eq!(
            after_value,
            json!({ "$or": [
                { "$or": [ { "ts": { "$gt": "2026" } }, { "ts": { "$is_null": true } } ] },
                { "ts": { "$eq": "2026" }, "id": { "$gt": "r1" } },
            ] })
        );
        let after_null = build_cursor_predicate(&keys, &[JsonValue::Null, json!("r1")]);
        assert_eq!(
            after_null,
            json!({ "$or": [ { "ts": { "$is_null": true }, "id": { "$gt": "r1" } } ] })
        );
    }

    // Descending nullable key: NULLs sort first, so a NULL cursor continues with
    // the rest of the NULL run and then every non-null row.
    #[test]
    fn descending_nullable_key_predicate_leaves_the_null_run_first() {
        let keys = total_order_keys(&[("ts".to_string(), true)], &["id".to_string()]);
        let after_null = build_cursor_predicate(&keys, &[JsonValue::Null, json!("r1")]);
        assert_eq!(
            after_null,
            json!({ "$or": [
                { "ts": { "$not_null": true } },
                { "ts": { "$is_null": true }, "id": { "$gt": "r1" } },
            ] })
        );
        let after_value = build_cursor_predicate(&keys, &[json!("2026"), json!("r1")]);
        assert_eq!(
            after_value,
            json!({ "$or": [
                { "ts": { "$lt": "2026" } },
                { "ts": { "$eq": "2026" }, "id": { "$gt": "r1" } },
            ] })
        );
    }

    #[test]
    fn with_nullability_marks_not_null_columns() {
        let keys = total_order_keys(&[("ts".to_string(), false)], &["id".to_string()]);
        let keys = with_nullability(keys, |column| column == "ts");
        assert!(!keys[0].nullable, "a NOT NULL sort column is not nullable");
        let pred = build_cursor_predicate(&keys, &[json!(1), json!(2)]);
        assert_eq!(
            pred,
            json!({ "$or": [
                { "ts": { "$gt": 1 } },
                { "ts": { "$eq": 1 }, "id": { "$gt": 2 } },
            ] })
        );
    }

    #[test]
    fn token_round_trips_and_is_tenant_and_entity_bound() {
        let keys = vec![
            ("created_at".to_string(), json!("2026-07-23T00:00:00Z")),
            ("id".to_string(), json!("row-9")),
        ];
        let q = "digest-1";
        let token = encode_page_token("tenant-a", "acme.Order", q, &keys);

        // Correct tenant+entity+query round-trips.
        let decoded = decode_page_token(&token, "tenant-a", "acme.Order", q).expect("decode");
        assert_eq!(decoded, keys);

        // Wrong tenant / entity is refused.
        assert!(decode_page_token(&token, "tenant-b", "acme.Order", q).is_err());
        assert!(decode_page_token(&token, "tenant-a", "acme.Other", q).is_err());
        // Garbage is refused, not panicked.
        assert!(decode_page_token("!!!not-base64!!!", "tenant-a", "acme.Order", q).is_err());
    }

    #[test]
    fn token_is_bound_to_query_digest() {
        let keys = vec![("id".to_string(), json!(100))];
        let token = encode_page_token("t", "E", "digest-A", &keys);
        // Same query digest round-trips.
        assert_eq!(
            decode_page_token(&token, "t", "E", "digest-A").expect("decode"),
            keys
        );
        // A DIFFERENT query (e.g. the filter changed) is refused, not silently
        // continued from an unrelated cursor.
        let err = decode_page_token(&token, "t", "E", "digest-B").unwrap_err();
        assert!(err.contains("different query"), "got: {err}");
    }

    #[test]
    fn query_digest_is_stable_and_change_sensitive() {
        let filter_a = json!({"status": "A", "region": "us"});
        // Same filter with keys in a different order canonicalizes to the SAME
        // digest — only a semantic change rotates it.
        let filter_a_reordered = json!({"region": "us", "status": "A"});
        let sort = vec![("id".to_string(), false)];
        let projection = vec!["id".to_string(), "status".to_string()];
        let base = query_digest(&filter_a, &sort, &projection);
        assert_eq!(base, query_digest(&filter_a_reordered, &sort, &projection));
        // Projection order (a SET) does not matter.
        assert_eq!(
            base,
            query_digest(&filter_a, &sort, &["status".to_string(), "id".to_string()])
        );
        // A changed FILTER rotates the digest.
        assert_ne!(
            base,
            query_digest(&json!({"status": "B"}), &sort, &projection)
        );
        // A changed SORT direction rotates the digest.
        assert_ne!(
            base,
            query_digest(&filter_a, &[("id".to_string(), true)], &projection)
        );
        // A changed PROJECTION rotates the digest.
        assert_ne!(base, query_digest(&filter_a, &sort, &["id".to_string()]));
    }

    #[test]
    fn null_cursor_value_round_trips() {
        // A NULL component is a valid position on a nullable sort key; the
        // predicate renders it with IS NULL, never `col < NULL`.
        let keys = vec![
            ("c".to_string(), JsonValue::Null),
            ("id".to_string(), json!(1)),
        ];
        let token = encode_page_token("t", "E", "q", &keys);
        assert_eq!(decode_page_token(&token, "t", "E", "q").unwrap(), keys);
    }

    #[test]
    fn total_order_appends_pk_tiebreakers() {
        let keys = total_order_keys(&[("created_at".to_string(), true)], &["id".to_string()]);
        assert_eq!(
            keys,
            vec![
                CursorKey {
                    column: "created_at".to_string(),
                    descending: true,
                    nullable: true,
                },
                CursorKey {
                    column: "id".to_string(),
                    descending: false,
                    nullable: false,
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
        // A null key value is a valid cursor position (kept as NULL).
        let row_null: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"ts": null, "id": "r1"})).unwrap();
        assert_eq!(
            cursor_values_from_row(&keys, &row_null).unwrap()[0],
            ("ts".to_string(), JsonValue::Null)
        );
        // An absent key column → None.
        let row_missing: serde_json::Map<String, JsonValue> =
            serde_json::from_value(json!({"id": "r1"})).unwrap();
        assert!(cursor_values_from_row(&keys, &row_missing).is_none());
    }

    #[test]
    fn single_key_predicate_is_a_bare_comparison() {
        let keys = vec![CursorKey {
            column: "id".to_string(),
            descending: false,
            nullable: false,
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
                nullable: false,
            },
            CursorKey {
                column: "id".to_string(),
                descending: true,
                nullable: false,
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
