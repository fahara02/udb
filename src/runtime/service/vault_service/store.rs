//! Neutral-IR read / record / conflict builders for the native `VaultService`:
//! the per-path secret version scan, the list projection, the transit-key scan,
//! and the `LogicalRecord` shapes for secrets and transit keys. Extracted
//! verbatim — the `LogicalRead`/`LogicalRecord` shapes and conflict targets are
//! byte-for-byte identical to the former god file.

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalSort, LogicalValue, NullOrder, SortDirection,
};
use crate::runtime::native_catalog::native_model;

use super::config::{
    KEY_STATE_ACTIVE, KEY_STATE_VERIFYING, STATE_DESTROYED, VAULT_SECRET_MSG,
    VAULT_TRANSIT_KEY_MSG, max_versions_scan,
};

pub(crate) fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

pub(crate) fn secret_path_read(tenant_id: &str, secret_path: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_SECRET_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "secret_path".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(secret_path),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "secret_id".to_string(),
            "version".to_string(),
            "ciphertext".to_string(),
            "data_key_wrapped".to_string(),
            "state".to_string(),
            "metadata_json".to_string(),
        ])),
        // The scan is deliberately bounded, so newest-first ordering is a
        // correctness boundary: latest/CAS/delete callers must never receive an
        // arbitrary old subset once the path exceeds max_versions_scan().
        sort: vec![LogicalSort {
            field: "version".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(max_versions_scan())),
    }
}

pub(crate) fn transit_key_read(tenant_id: &str, key_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: VAULT_TRANSIT_KEY_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(tenant_id),
            },
            LogicalFilter::Comparison {
                field: "key_name".to_string(),
                op: ComparisonOp::Eq,
                value: logical_string(key_name),
            },
        ])),
        projection: Some(LogicalProjection::fields([
            "key_id".to_string(),
            "version".to_string(),
            "algorithm".to_string(),
            "wrapped_key_material".to_string(),
            "state".to_string(),
        ])),
        // Keep ACTIVE/current versions inside the bounded latest-N window.
        sort: vec![LogicalSort {
            field: "version".to_string(),
            direction: SortDirection::Desc,
            nulls: NullOrder::default(),
        }],
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(max_versions_scan())),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn secret_record(
    secret_id: &str,
    tenant_id: &str,
    secret_path: &str,
    version: i64,
    ciphertext: &str,
    data_key_wrapped: &str,
    state: &str,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("secret_id".to_string(), logical_string(secret_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("secret_path".to_string(), logical_string(secret_path));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("ciphertext".to_string(), logical_string(ciphertext));
    record.insert(
        "data_key_wrapped".to_string(),
        logical_string(data_key_wrapped),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

pub(crate) fn secret_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "ciphertext".to_string(),
        "data_key_wrapped".to_string(),
        "state".to_string(),
        "metadata_json".to_string(),
    ])
}

pub(crate) fn transit_key_record(
    key_id: &str,
    tenant_id: &str,
    key_name: &str,
    version: i64,
    algorithm: &str,
    wrapped_key_material: &str,
    state: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("key_id".to_string(), logical_string(key_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("key_name".to_string(), logical_string(key_name));
    record.insert("version".to_string(), LogicalValue::Int(version));
    record.insert("algorithm".to_string(), logical_string(algorithm));
    record.insert(
        "wrapped_key_material".to_string(),
        logical_string(wrapped_key_material),
    );
    record.insert("state".to_string(), logical_string(state));
    record.insert("metadata_json".to_string(), logical_string("{}"));
    record
}

pub(crate) fn transit_key_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "algorithm".to_string(),
        "wrapped_key_material".to_string(),
        "state".to_string(),
    ])
}

// ── Transactional raw-SQL builders (rotate / destroy / keyset list) ────────────
//
// These three handlers need multi-row atomicity (or an accurate cross-page total)
// that the single-record neutral-IR write cannot express, so — like the lease
// reaper in `workers.rs` — they run raw `sqlx` against the vault Postgres store.
// Column/relation names are resolved from the embedded native manifest via
// `native_model` (never hand-typed), so a proto rename is caught at build time.
// The builders are pure `String` producers so their shape is unit-testable
// without a live DB. State tokens are byte-stable module consts, safe to inline.

/// `rotate_transit_key` step 1: demote every currently-ACTIVE version of one
/// `(tenant_id, key_name)` to VERIFYING. Runs inside the rotation transaction so
/// no reader ever observes zero ACTIVE versions between the demote and the insert.
pub(crate) fn transit_demote_active_sql() -> String {
    let m = native_model(VAULT_TRANSIT_KEY_MSG, &["tenant_id", "key_name", "state"]);
    format!(
        "UPDATE {rel} SET {state} = '{verifying}' \
         WHERE {tenant} = $1 AND {key_name} = $2 AND {state} = '{active}'",
        rel = m.relation,
        state = m.q("state"),
        verifying = KEY_STATE_VERIFYING,
        tenant = m.q("tenant_id"),
        key_name = m.q("key_name"),
        active = KEY_STATE_ACTIVE,
    )
}

/// `rotate_transit_key` step 2: insert the new ACTIVE version, computing the next
/// version number atomically from the same table (`MAX(version)+1`) so a
/// concurrent rotation cannot mint a duplicate. Params: `$1` key_id, `$2`
/// tenant_id, `$3` key_name, `$4` algorithm, `$5` wrapped_key_material. Returns
/// the new version.
pub(crate) fn transit_insert_rotated_sql() -> String {
    let m = native_model(
        VAULT_TRANSIT_KEY_MSG,
        &[
            "key_id",
            "tenant_id",
            "key_name",
            "version",
            "algorithm",
            "wrapped_key_material",
            "state",
            "metadata_json",
        ],
    );
    format!(
        "INSERT INTO {rel} \
         ({key_id}, {tenant}, {key_name}, {version}, {algorithm}, {wrapped}, {state}, {metadata}) \
         SELECT $1::uuid, $2, $3, COALESCE(MAX({version}), 0) + 1, $4, $5, '{active}', '{{}}'::jsonb \
         FROM {rel} WHERE {tenant} = $2 AND {key_name} = $3 \
         RETURNING {version}",
        rel = m.relation,
        key_id = m.q("key_id"),
        tenant = m.q("tenant_id"),
        key_name = m.q("key_name"),
        version = m.q("version"),
        algorithm = m.q("algorithm"),
        wrapped = m.q("wrapped_key_material"),
        state = m.q("state"),
        metadata = m.q("metadata_json"),
        active = KEY_STATE_ACTIVE,
    )
}

/// `destroy_secret`: crypto-shred every non-DESTROYED version of one
/// `(tenant_id, secret_path)` in a single statement — clear the ciphertext and
/// the wrapped DEK, flip to DESTROYED — so a mid-loop failure can no longer leave
/// a partial shred or an undercounted result. `rows_affected` is the accurate
/// destroyed count. Params: `$1` tenant_id, `$2` secret_path.
pub(crate) fn secret_shred_all_sql() -> String {
    let m = native_model(
        VAULT_SECRET_MSG,
        &[
            "tenant_id",
            "secret_path",
            "ciphertext",
            "data_key_wrapped",
            "state",
            "metadata_json",
        ],
    );
    format!(
        "UPDATE {rel} SET {ciphertext} = '', {wrapped} = '', {state} = '{destroyed}', \
         {metadata} = '{{}}'::jsonb \
         WHERE {tenant} = $1 AND {secret_path} = $2 AND {state} <> '{destroyed}'",
        rel = m.relation,
        ciphertext = m.q("ciphertext"),
        wrapped = m.q("data_key_wrapped"),
        state = m.q("state"),
        destroyed = STATE_DESTROYED,
        metadata = m.q("metadata_json"),
        tenant = m.q("tenant_id"),
        secret_path = m.q("secret_path"),
    )
}

/// `list_secrets` total: the correct count of DISTINCT secret paths for a tenant
/// (optionally prefix-filtered), computed in the store instead of the former
/// in-memory aggregate that silently truncated at the version-scan cap. Params:
/// `$1` tenant_id, `$2` LIKE pattern (only when `has_prefix`).
pub(crate) fn secret_count_distinct_paths_sql(has_prefix: bool) -> String {
    let m = native_model(VAULT_SECRET_MSG, &["tenant_id", "secret_path"]);
    let mut sql = format!(
        "SELECT COUNT(DISTINCT {path}) FROM {rel} WHERE {tenant} = $1",
        path = m.q("secret_path"),
        rel = m.relation,
        tenant = m.q("tenant_id"),
    );
    if has_prefix {
        sql.push_str(&format!(" AND {} LIKE $2 ESCAPE '\\'", m.q("secret_path")));
    }
    sql
}

/// `list_secrets` page: a keyset (seek) scan returning, per path, the latest
/// version and its state, ordered by path so `next_page_token` is the last path
/// of the page. Params in order: `$1` tenant_id, then — when present — the LIKE
/// pattern, then the keyset cursor, and finally the page limit (their positions
/// depend on which optional predicates are enabled).
pub(crate) fn secret_list_page_sql(has_prefix: bool, has_cursor: bool) -> String {
    let m = native_model(
        VAULT_SECRET_MSG,
        &["tenant_id", "secret_path", "version", "state"],
    );
    let path = m.q("secret_path");
    let version = m.q("version");
    let state = m.q("state");
    let mut sql = format!(
        "SELECT DISTINCT ON ({path}) {path} AS secret_path, {version} AS latest_version, \
         {state} AS state FROM {rel} WHERE {tenant} = $1",
        path = path,
        version = version,
        state = state,
        rel = m.relation,
        tenant = m.q("tenant_id"),
    );
    let mut next = 2;
    if has_prefix {
        sql.push_str(&format!(" AND {path} LIKE ${next} ESCAPE '\\'"));
        next += 1;
    }
    if has_cursor {
        sql.push_str(&format!(" AND {path} > ${next}"));
        next += 1;
    }
    // DISTINCT ON needs the leading ORDER BY key to match; `version DESC` then
    // picks the highest version per path.
    sql.push_str(&format!(
        " ORDER BY {path} ASC, {version} DESC LIMIT ${next}"
    ));
    sql
}

/// Escape a raw secret-path prefix into a Postgres `LIKE ... ESCAPE '\'` pattern
/// (`\` is the escape char), appending `%` so it matches the prefix. Keeps a path
/// containing `%`/`_`/`\` from being interpreted as wildcards.
pub(crate) fn like_prefix_pattern(prefix: &str) -> String {
    let mut out = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('%');
    out
}
