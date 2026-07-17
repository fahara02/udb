//! Redis engine — all `redis::` access lives here; the module is gated on the
//! `redis` feature. Every prefix sweep uses Redis `SCAN` ([`SWEEP_COMMAND`]),
//! NEVER `KEYS`, so a large keyspace never blocks the server.

use tonic::Status;

use crate::proto::udb::core::cache::services::v1 as cache_pb;

use super::config::{
    RECONCILE_DISCOVERY_MAX_ROUNDS, RECONCILE_MAX_KEYS_PER_NAMESPACE,
    RECONCILE_NAMESPACES_PER_PASS, SWEEP_COMMAND, SWEEP_COUNT,
};
use super::keys::{
    bytes_counter_key, clamped_scan_count, data_key, data_match, effective_max_bytes, meta_key,
    meta_match_all, namespace_match_all, parse_meta_key, reconcile_cursor_key, reconciled_sum,
    strip_data_prefix, would_exceed_budget,
};

fn map_err(context: &str, err: redis::RedisError) -> Status {
    crate::runtime::executor_utils::backend_transport_status("redis", context, err)
}

async fn connect(client: &redis::Client) -> Result<redis::aio::MultiplexedConnection, Status> {
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|err| {
            crate::runtime::executor_utils::backend_transport_status("redis", "connection", err)
        })
}

/// The decoded namespace meta blob (budget + default TTL).
#[derive(Default)]
struct NamespaceMeta {
    max_bytes: i64,
    default_ttl_seconds: i64,
}

async fn load_meta(
    conn: &mut redis::aio::MultiplexedConnection,
    tenant: &str,
    namespace: &str,
) -> Result<NamespaceMeta, Status> {
    let raw: Option<String> = redis::cmd("GET")
        .arg(meta_key(tenant, namespace))
        .query_async(conn)
        .await
        .map_err(|err| map_err("GET meta", err))?;
    let Some(raw) = raw else {
        return Ok(NamespaceMeta::default());
    };
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap_or_default();
    Ok(NamespaceMeta {
        max_bytes: value
            .get("max_bytes")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        default_ttl_seconds: value
            .get("default_ttl_seconds")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
    })
}

async fn read_counter(
    conn: &mut redis::aio::MultiplexedConnection,
    tenant: &str,
    namespace: &str,
) -> Result<i64, Status> {
    let used: Option<i64> = redis::cmd("GET")
        .arg(bytes_counter_key(tenant, namespace))
        .query_async(conn)
        .await
        .map_err(|err| map_err("GET counter", err))?;
    Ok(used.unwrap_or(0).max(0))
}

pub(crate) async fn get(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
    key: &str,
) -> Result<(bool, Vec<u8>, i64), Status> {
    let mut conn = connect(client).await?;
    let full = data_key(tenant, namespace, key);
    let value: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&full)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("GET", err))?;
    match value {
        Some(value) => {
            let ttl: i64 = redis::cmd("TTL")
                .arg(&full)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("TTL", err))?;
            Ok((true, value, ttl))
        }
        None => Ok((false, Vec::new(), -2)),
    }
}

pub(crate) struct SetOutcome {
    pub used_bytes: i64,
    pub max_bytes: i64,
}

pub(crate) async fn set(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
    key: &str,
    value: &[u8],
    ttl_seconds: i64,
) -> Result<SetOutcome, Status> {
    let mut conn = connect(client).await?;
    let full = data_key(tenant, namespace, key);
    let meta = load_meta(&mut conn, tenant, namespace).await?;
    let max_bytes = effective_max_bytes(meta.max_bytes);

    // Replacing an existing key only charges the size DELTA against the budget.
    let old_len: i64 = redis::cmd("STRLEN")
        .arg(&full)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("STRLEN", err))?;
    let new_len = value.len() as i64;
    let delta = new_len - old_len;
    let used = read_counter(&mut conn, tenant, namespace).await?;
    if would_exceed_budget(used, delta, max_bytes) {
        return Err(crate::runtime::executor_utils::quota_refusal_status(
            "cache",
            "namespace byte budget",
            format!(
                "cache namespace '{namespace}' byte budget exhausted \
                 (used {used} + {delta} > max {max_bytes})"
            ),
        ));
    }

    // Resolve TTL: explicit request value wins; else the namespace default; else
    // no expiry.
    let ttl = if ttl_seconds > 0 {
        ttl_seconds
    } else {
        meta.default_ttl_seconds.max(0)
    };
    let mut set_cmd = redis::cmd("SET");
    set_cmd.arg(&full).arg(value);
    if ttl > 0 {
        set_cmd.arg("EX").arg(ttl);
    }
    set_cmd
        .query_async::<()>(&mut conn)
        .await
        .map_err(|err| map_err("SET", err))?;

    let used_after: i64 = if delta != 0 {
        redis::cmd("INCRBY")
            .arg(bytes_counter_key(tenant, namespace))
            .arg(delta)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("INCRBY", err))?
    } else {
        used
    };
    Ok(SetOutcome {
        used_bytes: used_after.max(0),
        max_bytes,
    })
}

pub(crate) async fn delete(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
    key: &str,
) -> Result<(bool, i64), Status> {
    let mut conn = connect(client).await?;
    let full = data_key(tenant, namespace, key);
    let old_len: i64 = redis::cmd("STRLEN")
        .arg(&full)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("STRLEN", err))?;
    let removed: i64 = redis::cmd("DEL")
        .arg(&full)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("DEL", err))?;
    if removed == 0 {
        let used = read_counter(&mut conn, tenant, namespace).await?;
        return Ok((false, used));
    }
    let used_after: i64 = redis::cmd("INCRBY")
        .arg(bytes_counter_key(tenant, namespace))
        .arg(-old_len)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("INCRBY", err))?;
    Ok((true, used_after.max(0)))
}

pub(crate) async fn scan(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
    key_prefix: &str,
    limit: i32,
    page_token: &str,
) -> Result<(Vec<cache_pb::CacheItem>, String), Status> {
    let mut conn = connect(client).await?;
    let pattern = data_match(tenant, namespace, key_prefix);
    let cursor: u64 = page_token.parse().unwrap_or(0);
    // Named clamp: the caller limit is capped at MAX_SCAN_PAGE_LIMIT, never
    // passed through raw as the SCAN COUNT hint.
    let count = clamped_scan_count(limit);
    // SCAN, never KEYS: cursor-paged so a large keyspace never blocks Redis.
    let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
        .arg(cursor)
        .arg("MATCH")
        .arg(&pattern)
        .arg("COUNT")
        .arg(count)
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("SCAN", err))?;

    let mut items = Vec::with_capacity(keys.len());
    for full in &keys {
        let Some(local) = strip_data_prefix(full, tenant, namespace) else {
            continue;
        };
        let value: Option<Vec<u8>> = redis::cmd("GET")
            .arg(full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("GET", err))?;
        let Some(value) = value else { continue };
        let ttl: i64 = redis::cmd("TTL")
            .arg(full)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("TTL", err))?;
        items.push(cache_pb::CacheItem {
            key: local.to_string(),
            value,
            ttl_remaining_seconds: ttl,
        });
    }
    // Redis SCAN signals end-of-iteration with cursor 0; surface "" then.
    let next_token = if next == 0 {
        String::new()
    } else {
        next.to_string()
    };
    Ok((items, next_token))
}

pub(crate) async fn create_namespace(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
    max_bytes: i64,
    default_ttl_seconds: i64,
) -> Result<(), Status> {
    let mut conn = connect(client).await?;
    let meta = serde_json::json!({
        "max_bytes": max_bytes,
        "default_ttl_seconds": default_ttl_seconds,
    });
    redis::cmd("SET")
        .arg(meta_key(tenant, namespace))
        .arg(meta.to_string())
        .query_async::<()>(&mut conn)
        .await
        .map_err(|err| map_err("SET meta", err))?;
    Ok(())
}

/// SCAN+DEL sweep of the ENTIRE namespace (data keys + counter + meta).
pub(crate) async fn flush_namespace(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
) -> Result<u64, Status> {
    let mut conn = connect(client).await?;
    let pattern = namespace_match_all(tenant, namespace);
    let mut cursor: u64 = 0;
    let mut deleted: u64 = 0;
    loop {
        // SCAN, never KEYS.
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SWEEP_COUNT)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("SCAN", err))?;
        // Count only data keys toward the reported invalidation total; the
        // counter/meta bookkeeping keys are swept but not "cache entries".
        let data_keys: Vec<&String> = keys
            .iter()
            .filter(|k| strip_data_prefix(k, tenant, namespace).is_some())
            .collect();
        if !keys.is_empty() {
            let removed: u64 = redis::cmd("DEL")
                .arg(&keys)
                .query_async(&mut conn)
                .await
                .map_err(|err| map_err("DEL", err))?;
            deleted = deleted.saturating_add(removed.min(data_keys.len() as u64));
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(deleted)
}

pub(crate) struct NamespaceStats {
    pub used_bytes: i64,
    pub max_bytes: i64,
    pub item_count: u64,
}

pub(crate) async fn stats(
    client: &redis::Client,
    tenant: &str,
    namespace: &str,
) -> Result<NamespaceStats, Status> {
    let mut conn = connect(client).await?;
    let used_bytes = read_counter(&mut conn, tenant, namespace).await?;
    let meta = load_meta(&mut conn, tenant, namespace).await?;
    let pattern = data_match(tenant, namespace, "");
    let mut cursor: u64 = 0;
    let mut item_count: u64 = 0;
    loop {
        // SCAN, never KEYS.
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SWEEP_COUNT)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("SCAN", err))?;
        item_count = item_count.saturating_add(keys.len() as u64);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(NamespaceStats {
        used_bytes,
        max_bytes: effective_max_bytes(meta.max_bytes),
        item_count,
    })
}

/// One bounded byte-counter reconciliation pass (leader-elected; called from
/// [`run_cache_invalidation_worker_once`]). Returns how many namespaces had
/// their counter recomputed.
///
/// Design + bounds:
///   - Namespaces are DISCOVERED by SCANning meta keys ([`meta_match_all`],
///     SCAN never KEYS) from a rotation cursor persisted under
///     [`reconcile_cursor_key`], so every namespace is eventually visited
///     round-robin across passes. Per pass: at most
///     [`RECONCILE_DISCOVERY_MAX_ROUNDS`] discovery SCAN round-trips and
///     [`RECONCILE_NAMESPACES_PER_PASS`] namespaces recomputed, each capped
///     at [`RECONCILE_MAX_KEYS_PER_NAMESPACE`] data keys.
///   - A cursor persisted across passes is still a valid SCAN cursor (Redis
///     cursors are stateless reverse-binary-iteration positions); a
///     concurrent keyspace resize can at worst skip or duplicate a namespace
///     within ONE rotation — the next rotation covers it, and the pass is
///     idempotent (it SETs an absolute recomputed value), so duplicates are
///     harmless.
///   - Eventual-consistency window: a namespace's counter is exact only at
///     the moment it is reconciled. Between visits, TTL expiries inflate it
///     (over-count = fails CLOSED toward `resource_exhausted`), and a
///     Set/Delete racing the recompute can leave the written sum stale by
///     that in-flight delta until the namespace's next turn. Both converge
///     on re-run.
pub(crate) async fn reconcile_bytes_counters_once(client: &redis::Client) -> Result<u64, Status> {
    let mut conn = connect(client).await?;
    // Resume the rotation where the previous pass stopped (0 = keyspace start).
    let stored: Option<String> = redis::cmd("GET")
        .arg(reconcile_cursor_key())
        .query_async(&mut conn)
        .await
        .map_err(|err| map_err("GET reconcile cursor", err))?;
    let mut cursor: u64 = stored.and_then(|v| v.parse().ok()).unwrap_or(0);
    let pattern = meta_match_all();
    let mut namespaces: Vec<(String, String)> = Vec::new();
    for _ in 0..RECONCILE_DISCOVERY_MAX_ROUNDS {
        // SCAN, never KEYS.
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SWEEP_COUNT)
            .query_async(&mut conn)
            .await
            .map_err(|err| map_err("SCAN meta", err))?;
        namespaces.extend(keys.iter().filter_map(|key| parse_meta_key(key)));
        cursor = next;
        if cursor == 0 || namespaces.len() >= RECONCILE_NAMESPACES_PER_PASS {
            break;
        }
    }
    // SCAN may surface the same key more than once within an iteration; the
    // recompute is idempotent, but dedupe anyway so the per-pass namespace
    // budget is spent on DISTINCT namespaces.
    namespaces.sort();
    namespaces.dedup();
    namespaces.truncate(RECONCILE_NAMESPACES_PER_PASS);
    // Persist the rotation position so the NEXT pass continues from here.
    redis::cmd("SET")
        .arg(reconcile_cursor_key())
        .arg(cursor.to_string())
        .query_async::<()>(&mut conn)
        .await
        .map_err(|err| map_err("SET reconcile cursor", err))?;

    let mut reconciled: u64 = 0;
    for (tenant, namespace) in &namespaces {
        if reconcile_namespace_bytes(&mut conn, tenant, namespace)
            .await?
            .is_some()
        {
            reconciled = reconciled.saturating_add(1);
        }
    }
    Ok(reconciled)
}

/// Recompute ONE namespace's `__bytes__` counter from ground truth: SCAN the
/// namespace's data keys (only `:k:` keys — bookkeeping is excluded by the
/// pattern), STRLEN each, and SET the counter to the saturating sum
/// ([`reconciled_sum`]). Returns `Ok(None)` — counter left untouched — when
/// the namespace exceeds [`RECONCILE_MAX_KEYS_PER_NAMESPACE`]: a partial sum
/// would UNDER-count and fail open, whereas the stale counter only
/// over-counts. A key expiring between SCAN and STRLEN reads as length 0,
/// which is exactly its live usage.
async fn reconcile_namespace_bytes(
    conn: &mut redis::aio::MultiplexedConnection,
    tenant: &str,
    namespace: &str,
) -> Result<Option<i64>, Status> {
    let pattern = data_match(tenant, namespace, "");
    let mut cursor: u64 = 0;
    let mut lengths: Vec<i64> = Vec::new();
    loop {
        // SCAN, never KEYS.
        let (next, keys): (u64, Vec<String>) = redis::cmd(SWEEP_COMMAND)
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(SWEEP_COUNT)
            .query_async(&mut *conn)
            .await
            .map_err(|err| map_err("SCAN reconcile", err))?;
        for full in &keys {
            if strip_data_prefix(full, tenant, namespace).is_none() {
                continue;
            }
            if lengths.len() >= RECONCILE_MAX_KEYS_PER_NAMESPACE {
                tracing::debug!(
                    tenant = %tenant,
                    namespace = %namespace,
                    cap = RECONCILE_MAX_KEYS_PER_NAMESPACE,
                    "cache byte-counter reconciliation: namespace over the \
                     per-pass key cap; skipping (stale counter fails closed)"
                );
                return Ok(None);
            }
            let len: i64 = redis::cmd("STRLEN")
                .arg(full)
                .query_async(&mut *conn)
                .await
                .map_err(|err| map_err("STRLEN reconcile", err))?;
            lengths.push(len);
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    let sum = reconciled_sum(&lengths);
    let current = read_counter(conn, tenant, namespace).await?;
    if lengths.is_empty() && current == 0 {
        // Nothing stored and no drift recorded: leave no ghost counter key
        // behind (e.g. right after a namespace flush swept the counter).
        return Ok(Some(0));
    }
    if current != sum {
        redis::cmd("SET")
            .arg(bytes_counter_key(tenant, namespace))
            .arg(sum)
            .query_async::<()>(&mut *conn)
            .await
            .map_err(|err| map_err("SET reconcile counter", err))?;
    }
    Ok(Some(sum))
}
