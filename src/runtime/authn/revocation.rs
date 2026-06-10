//! Token-revocation domain decisions independent of storage and gRPC adapters.
//!
//! The durable `token_revocations` table (Postgres) is the source of truth. To
//! make revocation propagate *fast* across nodes — instead of every node waiting
//! on a DB read before its access-token TTL elapses — a short-TTL cluster jti
//! denylist ([`JtiDenylist`], Redis-backed) caches the revoked `jti_hash` keys.
//! It is purely an acceleration layer: when Redis is absent the behavior is the
//! unchanged DB-only lookup.

/// Reason surfaced when a token is denied because the revocation store was
/// unreachable in fail-closed mode.
pub const REVOCATION_STORE_UNAVAILABLE_REASON: &str = "revocation store unavailable (fail-closed)";

/// Reason surfaced when the fast cluster denylist reports a jti as revoked.
pub const REVOCATION_DENYLIST_HIT_REASON: &str = "token revoked (cluster denylist)";

/// Namespace for the cluster jti denylist keys. The stored value is the same
/// keyed-HMAC `jti_hash` the durable table stores — NEVER a raw jti/token.
pub const REVOKED_JTI_KEY_PREFIX: &str = "udb:revoked_jti";

/// Pure decision for what token validation should return when the revocation
/// store lookup itself errors.
///
/// In fail-closed mode the token is treated as revoked; otherwise dev/test keeps
/// the legacy fail-open behavior.
pub fn revocation_lookup_error_outcome(fail_closed: bool) -> (bool, String) {
    if fail_closed {
        (true, REVOCATION_STORE_UNAVAILABLE_REASON.to_string())
    } else {
        (false, String::new())
    }
}

/// Outcome of a single denylist consultation, before any DB fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenylistDecision {
    /// The jti is present in the fast denylist → revoked (skip the DB read).
    Revoked,
    /// The jti is absent → fall through to the durable DB read (source of truth).
    NotFound,
    /// The denylist itself errored. In a hardened/production posture the caller
    /// must FAIL CLOSED (treat as revoked); in dev it should fall through to the
    /// DB read. Carried as a variant so the caller applies its own posture.
    Error,
}

/// Pure decision for a cluster denylist consultation result, given whether the
/// node is in fail-closed mode. Returns `Some((revoked, reason))` when the
/// denylist alone decides the outcome (a hit, or an error under fail-closed);
/// returns `None` when the caller must fall through to the durable DB read (a
/// miss, or an error in dev fail-open mode).
pub fn denylist_check_outcome(
    decision: DenylistDecision,
    fail_closed: bool,
) -> Option<(bool, String)> {
    match decision {
        DenylistDecision::Revoked => Some((true, REVOCATION_DENYLIST_HIT_REASON.to_string())),
        DenylistDecision::NotFound => None,
        DenylistDecision::Error => {
            if fail_closed {
                Some((true, REVOCATION_STORE_UNAVAILABLE_REASON.to_string()))
            } else {
                None
            }
        }
    }
}

/// Build the namespaced Redis key for a `jti_hash` (keyed-HMAC digest, never a
/// raw jti). Shared by the populate (SET) and check (EXISTS) paths so the key
/// layout lives in one place.
pub fn revoked_jti_key(jti_hash: &str) -> String {
    format!("{REVOKED_JTI_KEY_PREFIX}:{jti_hash}")
}

/// Short-TTL cluster jti denylist backed by Redis (cluster cache). Stores
/// revoked `jti_hash` keys with a TTL equal to the access-token's remaining
/// lifetime, so an entry naturally expires when the token would anyway (no
/// unbounded growth). Best-effort writes: a Redis failure is logged by the
/// caller and never fails the durable revoke. Feature-gated behind `redis` so
/// the slim build compiles without it.
/// `set_ex` / `exists` on the multiplexed connection are provided by this trait.
#[cfg(feature = "redis")]
use redis::AsyncCommands;

#[cfg(feature = "redis")]
#[derive(Clone)]
pub struct JtiDenylist {
    client: redis::Client,
    /// Floor/fallback TTL (seconds) used when a per-token remaining lifetime is
    /// not supplied — set to the access-token max lifetime so the entry outlives
    /// any token it shadows.
    default_ttl_secs: u64,
}

#[cfg(feature = "redis")]
impl JtiDenylist {
    pub fn new(client: redis::Client, default_ttl_secs: u64) -> Self {
        Self {
            client,
            default_ttl_secs: default_ttl_secs.max(1),
        }
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, String> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|err| format!("jti denylist connection failed: {err}"))
    }

    /// Add a revoked `jti_hash` to the denylist with a TTL. `ttl_secs == 0`
    /// falls back to the configured default (access-token max lifetime). The
    /// value is the hash itself; the key is namespaced via [`revoked_jti_key`].
    pub async fn add(&self, jti_hash: &str, ttl_secs: u64) -> Result<(), String> {
        let ttl = if ttl_secs == 0 {
            self.default_ttl_secs
        } else {
            ttl_secs
        }
        .max(1);
        let mut conn = self.connection().await?;
        let key = revoked_jti_key(jti_hash);
        let _: () = conn
            .set_ex(&key, "1", ttl)
            .await
            .map_err(|err| format!("jti denylist SETEX failed: {err}"))?;
        Ok(())
    }

    /// Is this `jti_hash` on the fast denylist? Translates the Redis result into
    /// a [`DenylistDecision`] so the caller can apply its fail-closed posture via
    /// [`denylist_check_outcome`].
    pub async fn check(&self, jti_hash: &str) -> DenylistDecision {
        let mut conn = match self.connection().await {
            Ok(conn) => conn,
            Err(_) => return DenylistDecision::Error,
        };
        let key = revoked_jti_key(jti_hash);
        let exists: Result<bool, _> = conn.exists(&key).await;
        match exists {
            Ok(true) => DenylistDecision::Revoked,
            Ok(false) => DenylistDecision::NotFound,
            Err(_) => DenylistDecision::Error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_error_fails_closed_when_hardened() {
        let (revoked, reason) = revocation_lookup_error_outcome(true);
        assert!(revoked, "fail-closed must treat a store error as revoked");
        assert_eq!(reason, REVOCATION_STORE_UNAVAILABLE_REASON);
    }

    #[test]
    fn lookup_error_fails_open_when_not_hardened() {
        let (revoked, reason) = revocation_lookup_error_outcome(false);
        assert!(!revoked, "dev fail-open must not report revoked");
        assert!(reason.is_empty());
    }

    #[test]
    fn denylist_hit_is_revoked_without_db_read() {
        // A denylist hit decides the outcome on its own (no DB fallthrough),
        // regardless of fail-closed posture.
        for fail_closed in [false, true] {
            let out = denylist_check_outcome(DenylistDecision::Revoked, fail_closed);
            let (revoked, reason) = out.expect("a hit must decide the outcome");
            assert!(revoked, "denylist hit must be treated as revoked");
            assert_eq!(reason, REVOCATION_DENYLIST_HIT_REASON);
        }
    }

    #[test]
    fn denylist_miss_falls_through_to_db() {
        // A miss never decides on its own — the caller must read the durable DB
        // (source of truth) in both postures.
        assert!(denylist_check_outcome(DenylistDecision::NotFound, false).is_none());
        assert!(denylist_check_outcome(DenylistDecision::NotFound, true).is_none());
    }

    #[test]
    fn denylist_error_fails_closed_when_hardened_else_falls_through() {
        // Redis error in hardened mode → fail closed (revoked). In dev → fall
        // through to the DB read (None), preserving the DB as source of truth.
        let hardened = denylist_check_outcome(DenylistDecision::Error, true)
            .expect("hardened denylist error must fail closed");
        assert!(hardened.0, "hardened denylist error must be revoked");
        assert_eq!(hardened.1, REVOCATION_STORE_UNAVAILABLE_REASON);
        assert!(
            denylist_check_outcome(DenylistDecision::Error, false).is_none(),
            "dev denylist error must fall through to the DB read"
        );
    }

    #[test]
    fn revoked_jti_key_is_namespaced_and_carries_only_the_hash() {
        let key = revoked_jti_key("abc123hash");
        assert_eq!(key, "udb:revoked_jti:abc123hash");
        assert!(key.starts_with(REVOKED_JTI_KEY_PREFIX));
    }
}
