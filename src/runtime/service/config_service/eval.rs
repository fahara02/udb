//! PURE evaluation core — NO I/O. The SDK ships the identical algorithm so client
//! and server agree bit-for-bit. Everything here is unit-tested with fixed
//! inputs -> fixed outputs (see `tests`). Scope precedence is deterministic
//! (environment > project > tenant-default) and percentage rollout uses a STABLE
//! FNV-1a hash of `flag_key + ":" + subject`, so a given subject is sticky across
//! nodes and releases.

use std::collections::HashMap;

/// A typed flag value (the oneof arm), independent of proto/storage encodings.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FlagVal {
    Bool(bool),
    Number(f64),
    Str(String),
    Json(String),
}

impl FlagVal {
    /// The value returned when a flag is disabled or a subject falls outside the
    /// rollout bucket: the type's neutral "off"/control value. Deterministic.
    fn off_value(&self) -> FlagVal {
        match self {
            FlagVal::Bool(_) => FlagVal::Bool(false),
            FlagVal::Number(_) => FlagVal::Number(0.0),
            FlagVal::Str(_) => FlagVal::Str(String::new()),
            FlagVal::Json(_) => FlagVal::Json("null".to_string()),
        }
    }
}

/// A flag definition reduced to exactly what evaluation needs (no DB metadata).
#[derive(Clone, Debug)]
pub(crate) struct EvalFlag {
    pub flag_id: String,
    pub flag_key: String,
    pub project_id: String,
    pub environment: String,
    pub value: FlagVal,
    pub enabled: bool,
    pub rollout_percentage: i32,
    pub rollout_context_key: String,
    pub revision: i64,
}

/// The evaluation context: which scope to resolve against and the subject
/// attributes the rollout hash keys on.
#[derive(Clone, Debug, Default)]
pub(crate) struct EvalContext {
    pub project_id: String,
    pub environment: String,
    pub attributes: HashMap<String, String>,
}

/// FNV-1a 64-bit. A tiny, dependency-free, fully-specified hash so the SDK can
/// reproduce the exact bucket in any language.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Stable rollout bucket in `0..100` for `(flag_key, subject)`. Same inputs always
/// produce the same bucket, on every node and every release.
pub(crate) fn rollout_bucket(flag_key: &str, subject: &str) -> i32 {
    let combined = format!("{flag_key}:{subject}");
    (fnv1a64(combined.as_bytes()) % 100) as i32
}

/// Specificity rank of a flag's scope against the context, or `-1` if the flag is
/// not a candidate (its non-empty project/environment does not match). Precedence:
/// environment (2) + project (1); a tenant-default row (both empty) ranks 0.
fn scope_rank(flag: &EvalFlag, ctx: &EvalContext) -> i32 {
    let env_ok = flag.environment.is_empty() || flag.environment == ctx.environment;
    let proj_ok = flag.project_id.is_empty() || flag.project_id == ctx.project_id;
    if !env_ok || !proj_ok {
        return -1;
    }
    let mut rank = 0;
    if !flag.environment.is_empty() {
        rank += 2;
    }
    if !flag.project_id.is_empty() {
        rank += 1;
    }
    rank
}

/// Pick the most specific matching flag among same-key candidates at different
/// scopes (environment > project > tenant-default). Ties (impossible under the
/// unique scope index, but defended anyway) break deterministically by flag_id.
/// PURE — the SDK resolves identically.
pub(crate) fn resolve_flag<'a>(
    candidates: &'a [EvalFlag],
    ctx: &EvalContext,
) -> Option<&'a EvalFlag> {
    candidates
        .iter()
        .filter_map(|f| {
            let rank = scope_rank(f, ctx);
            (rank >= 0).then_some((rank, f))
        })
        .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.flag_id.cmp(&b.1.flag_id)))
        .map(|(_, f)| f)
}

/// Evaluate a single resolved flag to its typed value. PURE and deterministic:
/// disabled -> off; `pct >= 100` -> on; `pct <= 0` -> off; otherwise the stable
/// `(flag_key, subject)` bucket gates against `rollout_percentage`. The SDK ships
/// this identical algorithm.
pub(crate) fn evaluate_flag(flag: &EvalFlag, ctx: &EvalContext) -> FlagVal {
    if !flag.enabled {
        return flag.value.off_value();
    }
    let pct = flag.rollout_percentage.clamp(0, 100);
    if pct >= 100 {
        return flag.value.clone();
    }
    if pct <= 0 {
        return flag.value.off_value();
    }
    let subject = ctx
        .attributes
        .get(&flag.rollout_context_key)
        .map(String::as_str)
        .unwrap_or("");
    if rollout_bucket(&flag.flag_key, subject) < pct {
        flag.value.clone()
    } else {
        flag.value.off_value()
    }
}

/// The config-domain revision bump: monotone, saturating. PURE so the "a change
/// bumps the revision" invariant is unit-testable without Postgres.
pub(crate) fn bump_revision(current: i64) -> i64 {
    current.saturating_add(1)
}
