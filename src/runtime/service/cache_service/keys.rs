//! Pure key/namespacing + byte-budget helpers (always compiled; unit-tested
//! without Redis). CLAIM-derived `tenant` is always the first path segment, so
//! two tenants share a namespace safely and a sweep of one tenant's prefix can
//! never reach another's.

use super::config::{
    DEFAULT_NAMESPACE_MAX_BYTES, KEY_ROOT, MAX_SCAN_PAGE_LIMIT, META_SUFFIX, SWEEP_COUNT,
};

/// The data-key for a namespace-local key. CLAIM-derived `tenant` is the first
/// path segment, so tenant-a and tenant-b never collide and a sweep of one
/// tenant's prefix can never reach another's.
pub(crate) fn data_key(tenant: &str, namespace: &str, key: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:k:{key}")
}

/// The SCAN `MATCH` pattern for data keys under a namespace, optionally narrowed
/// by a namespace-local key prefix. Only `:k:` data keys match — the byte counter
/// and meta blob are excluded.
pub(crate) fn data_match(tenant: &str, namespace: &str, key_prefix: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:k:{key_prefix}*")
}

/// The SCAN `MATCH` pattern for the ENTIRE namespace (data + counter + meta). Used
/// by the namespace flush and the CDC invalidation worker.
pub(crate) fn namespace_match_all(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:*")
}

/// The per-namespace used-bytes counter key (`INCRBY` target).
pub(crate) fn bytes_counter_key(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:__bytes__")
}

/// The per-namespace meta key (JSON: `max_bytes`, `default_ttl_seconds`).
pub(crate) fn meta_key(tenant: &str, namespace: &str) -> String {
    format!("{KEY_ROOT}:{tenant}:{namespace}:{META_SUFFIX}")
}

/// The SCAN `MATCH` pattern for EVERY namespace meta key across tenants. Used
/// ONLY by the leader-elected reconciliation pass, which re-derives the tenant
/// from each matched key itself — no caller-supplied value ever reaches this
/// pattern, so it cannot be steered cross-tenant.
pub(crate) fn meta_match_all() -> String {
    format!("{KEY_ROOT}:*:{META_SUFFIX}")
}

/// The bookkeeping key persisting the reconciliation rotation cursor across
/// worker passes. Lives directly under [`KEY_ROOT`] with a reserved segment and
/// NO namespace shape, so it never matches a data-key/meta SCAN pattern and no
/// tenant sweep can reach it.
pub(crate) fn reconcile_cursor_key() -> String {
    format!("{KEY_ROOT}:__reconcile__:cursor")
}

/// Recover `(tenant, namespace)` from a full meta key
/// (`udb:cache:<tenant>:<ns>:__meta__`). [`validate_namespace`] rejects `:` in a
/// namespace, so the namespace is always the LAST segment before the suffix and
/// everything in between belongs to the tenant. `None` for anything that is not
/// a well-formed meta key (data keys, counters, the rotation cursor).
pub(crate) fn parse_meta_key(full_key: &str) -> Option<(String, String)> {
    let rest = full_key.strip_prefix(&format!("{KEY_ROOT}:"))?;
    let rest = rest.strip_suffix(&format!(":{META_SUFFIX}"))?;
    let (tenant, namespace) = rest.rsplit_once(':')?;
    (!tenant.trim().is_empty() && !namespace.trim().is_empty())
        .then(|| (tenant.to_string(), namespace.to_string()))
}

/// Pure reconciliation math: the recomputed namespace usage is the saturating
/// sum of the (non-negative) `STRLEN` of every live data key the bounded SCAN
/// observed. Defensive: a bogus negative length never subtracts real usage, and
/// adversarially huge values saturate instead of wrapping.
pub(crate) fn reconciled_sum(lengths: &[i64]) -> i64 {
    lengths
        .iter()
        .fold(0i64, |acc, len| acc.saturating_add((*len).max(0)))
}

/// Clamp a caller-supplied `Scan` page limit into the SCAN `COUNT` hint: a
/// non-positive limit falls back to [`SWEEP_COUNT`]; a positive limit is capped
/// at [`MAX_SCAN_PAGE_LIMIT`] — never a raw pass-through.
pub(crate) fn clamped_scan_count(limit: i32) -> u32 {
    if limit > 0 {
        (limit as u32).min(MAX_SCAN_PAGE_LIMIT)
    } else {
        SWEEP_COUNT
    }
}

/// Recover the namespace-local key from a full data key (strip the
/// `udb:cache:<tenant>:<ns>:k:` prefix). `None` if the key is not a data key for
/// this (tenant, namespace).
pub(crate) fn strip_data_prefix<'a>(
    full_key: &'a str,
    tenant: &str,
    namespace: &str,
) -> Option<&'a str> {
    let prefix = format!("{KEY_ROOT}:{tenant}:{namespace}:k:");
    full_key.strip_prefix(&prefix)
}

/// Resolve a namespace's effective byte budget: a declared positive `max_bytes`
/// wins, otherwise the service default. Always returns a positive bound.
pub(crate) fn effective_max_bytes(declared: i64) -> i64 {
    if declared > 0 {
        declared
    } else {
        DEFAULT_NAMESPACE_MAX_BYTES
    }
}

/// The budget gate, pure and unit-tested without Redis: writing `delta` more bytes
/// to a namespace currently holding `used` bytes exceeds `max_bytes`. `delta <= 0`
/// (a shrink or same-size overwrite) never exceeds the budget.
pub(crate) fn would_exceed_budget(used: i64, delta: i64, max_bytes: i64) -> bool {
    delta > 0 && used.saturating_add(delta) > max_bytes
}
