//! Static configuration for the native `ConfigService`: the canonical entity and
//! outbox-topic names, list/evaluate caps, and the resolve-once evaluation TTL
//! (never a per-request env read).

use std::sync::OnceLock;

/// Canonical proto message backing a durable flag row.
pub(crate) const CONFIG_MSG: &str = "udb.core.config.entity.v1.Flag";
/// Outbox topic emitted once per flag mutation.
pub(crate) const TOPIC_FLAG_CHANGED: &str = "udb.config.flag.changed.v1";

/// Default server-authoritative evaluation cache TTL (seconds). Overridable once
/// via `UDB_CONFIG_EVAL_TTL_SECONDS`, resolved through a `OnceLock` — never read
/// per request.
const DEFAULT_EVAL_TTL_SECONDS: i64 = 30;
/// Cap on scope rows scanned per key during evaluation (env/project/tenant
/// arms). The batched `EvaluateFlags` read multiplies this by the key count for
/// its overall row cap (see `store::flag_candidates_batch_read`).
pub(crate) const MAX_FLAGS_PER_KEY_SCAN: u32 = 64;
/// List defaults/caps so one tenant cannot scan an unbounded table.
pub(crate) const DEFAULT_LIST_LIMIT: u32 = 100;
pub(crate) const MAX_LIST_LIMIT: u32 = 500;
/// Cap on keys evaluated in a single EvaluateFlags call.
pub(crate) const MAX_EVALUATE_KEYS: usize = 256;

/// Resolve the evaluation TTL exactly once (no per-request env reads).
pub(crate) fn eval_ttl_seconds() -> i64 {
    static TTL: OnceLock<i64> = OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("UDB_CONFIG_EVAL_TTL_SECONDS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_EVAL_TTL_SECONDS)
    })
}
