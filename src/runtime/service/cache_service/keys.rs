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

/// Atomic budgeted write, run server-side as ONE Lua script so the size probe,
/// the budget gate, the write, and the counter bump cannot interleave with a
/// concurrent writer. Issuing them as separate round-trips let N concurrent
/// writers each read the same `used`, each pass the gate, and together overrun
/// the namespace budget by up to N-1 values.
///
/// `KEYS[1]` = data key, `KEYS[2]` = byte counter.
/// `ARGV[1]` = value, `ARGV[2]` = effective max bytes, `ARGV[3]` = TTL seconds
/// (`<= 0` = no expiry).
/// Returns `{stored(0|1), used_bytes, delta}`; on refusal `used_bytes` is the
/// unchanged counter and nothing was written. The gate mirrors
/// [`would_exceed_budget`] exactly (only a positive delta can exceed).
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const BUDGETED_SET_LUA: &str = "\
local old = redis.call('STRLEN', KEYS[1]) \
local delta = string.len(ARGV[1]) - old \
local used = tonumber(redis.call('GET', KEYS[2]) or '0') or 0 \
if used < 0 then used = 0 end \
if delta > 0 and used + delta > tonumber(ARGV[2]) then \
  return {0, used, delta} \
end \
local ttl = tonumber(ARGV[3]) or 0 \
if ttl > 0 then \
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ttl) \
else \
  redis.call('SET', KEYS[1], ARGV[1]) \
end \
local after = used \
if delta ~= 0 then after = redis.call('INCRBY', KEYS[2], delta) end \
return {1, after, delta}";

/// Atomic counter increment for `Increment` (same single-script rationale as
/// [`BUDGETED_SET_LUA`]): the bound check, the `INCRBY`, the TTL of a created
/// key, the byte-budget charge and the remaining-TTL read cannot interleave
/// with another writer. The budget is charged by the exact `STRLEN` change; an
/// increment that would overrun it is reverted before the script returns.
///
/// `KEYS[1]` = data key, `KEYS[2]` = byte counter.
/// `ARGV[1]` = delta, `ARGV[2]` = max (`<= 0` = no bound), `ARGV[3]` = TTL
/// seconds for a created key (`<= 0` = no expiry), `ARGV[4]` = effective max
/// bytes.
/// Returns `{status, value, created(0|1), ttl_remaining_seconds}` where status
/// is [`INCREMENT_APPLIED`], [`INCREMENT_REFUSED`] (over `max`; nothing
/// written), [`INCREMENT_NOT_INTEGER`], [`INCREMENT_OVERFLOW`] or
/// [`INCREMENT_OVER_BUDGET`] (reverted). A missing key reports TTL 0.
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const BUDGETED_INCREMENT_LUA: &str = "\
local function remaining() \
  local p = redis.call('PTTL', KEYS[1]) \
  if p > 0 then return math.ceil(p / 1000) end \
  return 0 \
end \
local raw = redis.call('GET', KEYS[1]) \
local created = 0 \
local current = 0 \
local old_len = 0 \
if raw then \
  current = tonumber(raw) \
  if current == nil or current ~= math.floor(current) then return {-1, 0, 0, remaining()} end \
  old_len = string.len(raw) \
else \
  created = 1 \
end \
local cap = tonumber(ARGV[2]) or 0 \
if cap > 0 and current + tonumber(ARGV[1]) > cap then \
  return {0, current, 0, remaining()} \
end \
if tonumber(ARGV[1]) == 0 then return {1, current, 0, remaining()} end \
local after = redis.pcall('INCRBY', KEYS[1], ARGV[1]) \
if type(after) == 'table' and after.err then \
  if string.find(after.err, 'overflow') then return {-2, current, 0, remaining()} end \
  return {-1, 0, 0, remaining()} \
end \
local len_delta = redis.call('STRLEN', KEYS[1]) - old_len \
local used = tonumber(redis.call('GET', KEYS[2]) or '0') or 0 \
if used < 0 then used = 0 end \
if len_delta > 0 and used + len_delta > tonumber(ARGV[4]) then \
  if created == 1 then redis.call('DEL', KEYS[1]) else redis.call('SET', KEYS[1], raw, 'KEEPTTL') end \
  return {-3, current, 0, remaining()} \
end \
local ttl = tonumber(ARGV[3]) or 0 \
if created == 1 and ttl > 0 then redis.call('EXPIRE', KEYS[1], ttl) end \
if len_delta ~= 0 then redis.call('INCRBY', KEYS[2], len_delta) end \
return {1, after, created, remaining()}";

/// [`BUDGETED_INCREMENT_LUA`] statuses.
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const INCREMENT_APPLIED: i64 = 1;
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const INCREMENT_REFUSED: i64 = 0;
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const INCREMENT_NOT_INTEGER: i64 = -1;
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const INCREMENT_OVERFLOW: i64 = -2;
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const INCREMENT_OVER_BUDGET: i64 = -3;

/// Atomic delete + counter release (same single-script rationale as
/// [`BUDGETED_SET_LUA`]): the released length is the length of the value that
/// was actually removed, so a concurrent overwrite between a separate STRLEN
/// and DEL can no longer leak or double-release counter bytes.
///
/// `KEYS[1]` = data key, `KEYS[2]` = byte counter. Returns
/// `{removed(0|1), used_bytes}`.
#[cfg_attr(not(feature = "redis"), allow(dead_code))]
pub(crate) const BUDGETED_DELETE_LUA: &str = "\
local old = redis.call('STRLEN', KEYS[1]) \
local removed = redis.call('DEL', KEYS[1]) \
if removed == 0 then \
  local used = tonumber(redis.call('GET', KEYS[2]) or '0') or 0 \
  return {0, used} \
end \
local after = redis.call('INCRBY', KEYS[2], -old) \
return {1, after}";
