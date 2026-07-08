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

/// Namespace for the cluster TENANT denylist keys. The value is a cutoff unix
/// timestamp: every token for the tenant whose `iat` is at/before the cutoff is
/// revoked. This is the tenant-level half of the cluster kill switch (the jti
/// half above is per-token); like the jti denylist it is purely an accelerator
/// over the durable Postgres source of truth.
pub const REVOKED_TENANT_KEY_PREFIX: &str = "udb:denylist:tenant";

/// Reason surfaced when the fast cluster denylist reports a token revoked
/// because its tenant was killed at/before the token's issue time.
pub const REVOCATION_TENANT_DENYLIST_HIT_REASON: &str = "token revoked (tenant cluster denylist)";

/// Namespace for the cluster PRINCIPAL denylist keys. The value is a cutoff unix
/// timestamp: every token whose `sub` (principal id) matches and whose `iat` is
/// at/before the cutoff is revoked. This is the principal-level half of the
/// cluster kill switch — fired on account/principal hard-delete — and reuses the
/// tenant denylist's decision machinery ([`TenantDenylistDecision`] /
/// [`tenant_denylist_check_outcome`]) since the cutoff-vs-`iat` logic is identical.
pub const REVOKED_PRINCIPAL_KEY_PREFIX: &str = "udb:denylist:principal";

/// Reason surfaced when the fast cluster denylist reports a token revoked because
/// its principal was killed (account deleted) at/before the token's issue time.
pub const REVOCATION_PRINCIPAL_DENYLIST_HIT_REASON: &str =
    "token revoked (principal cluster denylist)";

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

/// Build the namespaced Redis key for a tenant's revoke-cutoff. Shared by the
/// populate (SET) and check (GET) paths so the layout lives in one place.
pub fn revoked_tenant_key(tenant_id: &str) -> String {
    format!("{REVOKED_TENANT_KEY_PREFIX}:{tenant_id}")
}

/// Build the namespaced Redis key for a principal's revoke-cutoff. Shared by the
/// populate (SET) and check (GET) paths so the layout lives in one place.
pub fn revoked_principal_key(principal_id: &str) -> String {
    format!("{REVOKED_PRINCIPAL_KEY_PREFIX}:{principal_id}")
}

/// Result of consulting the tenant denylist for a single token, before any DB
/// fallback. Mirrors [`DenylistDecision`] but carries the cutoff so the caller
/// can compare it to the token's `iat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantDenylistDecision {
    /// A cutoff exists for the tenant (unix seconds): tokens issued at/before it
    /// are revoked.
    Cutoff(u64),
    /// No cutoff recorded → fall through to the durable DB read.
    NotFound,
    /// The denylist itself errored. Posture (fail-closed vs fall-through) is
    /// applied by the caller, exactly like [`DenylistDecision::Error`].
    Error,
}

/// Pure decision for a tenant denylist consultation, given the token's `iat`
/// and the node's fail-closed posture. Returns `Some((revoked, reason))` when
/// the tenant denylist alone decides the outcome:
///   - a cutoff that is `>= iat` (token issued at/before the kill) → revoked;
///   - an error under fail-closed → revoked (store outage must not let a
///     possibly-revoked token through).
/// Returns `None` (fall through to the durable DB read) when:
///   - no cutoff is recorded;
///   - a cutoff exists but the token was issued AFTER it (a fresh token);
///   - an error in dev/fail-open mode.
///
/// Availability-first: a Redis MISS or (in dev) a Redis OUTAGE NEVER decides on
/// its own — Postgres stays authoritative.
pub fn tenant_denylist_check_outcome(
    decision: TenantDenylistDecision,
    iat: u64,
    fail_closed: bool,
) -> Option<(bool, String)> {
    match decision {
        TenantDenylistDecision::Cutoff(cutoff) => {
            // Token issued at/before the cutoff → killed. A strictly-newer token
            // (iat > cutoff) is unaffected and falls through to the DB.
            if iat <= cutoff {
                Some((true, REVOCATION_TENANT_DENYLIST_HIT_REASON.to_string()))
            } else {
                None
            }
        }
        TenantDenylistDecision::NotFound => None,
        TenantDenylistDecision::Error => {
            if fail_closed {
                Some((true, REVOCATION_STORE_UNAVAILABLE_REASON.to_string()))
            } else {
                None
            }
        }
    }
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

    // ── Tenant-level cluster kill (3.3) ──────────────────────────────────────

    /// Record that every token for `tenant_id` issued at/before `cutoff_unix` is
    /// revoked. Sets `udb:denylist:tenant:<tenant>` → the cutoff timestamp with
    /// the same default TTL the jti denylist uses (access-token max lifetime), so
    /// the entry outlives any access token it can shadow but does not grow
    /// unbounded — once all tokens minted before the cutoff have expired the kill
    /// is moot and the key can lapse. If a later (larger) cutoff is written the
    /// newer value wins (`SETEX` overwrites), monotonically widening the kill.
    /// Best-effort: a Redis failure is surfaced as `Err` for the caller to log,
    /// and NEVER fails the durable revoke (Postgres remains the source of truth).
    pub async fn deny_tenant_after(&self, tenant_id: &str, cutoff_unix: u64) -> Result<(), String> {
        let mut conn = self.connection().await?;
        let key = revoked_tenant_key(tenant_id);
        let _: () = conn
            .set_ex(&key, cutoff_unix, self.default_ttl_secs)
            .await
            .map_err(|err| format!("tenant denylist SETEX failed: {err}"))?;
        Ok(())
    }

    /// Read the tenant's revoke cutoff. Translates the Redis result into a
    /// [`TenantDenylistDecision`] so the caller can apply its fail-closed posture
    /// (and `iat` comparison) via [`tenant_denylist_check_outcome`]. A missing key
    /// is `NotFound` (fall through to the DB); a connection/read failure is
    /// `Error` (caller decides posture — availability-first defers to the DB).
    pub async fn tenant_denied_after(&self, tenant_id: &str) -> TenantDenylistDecision {
        let mut conn = match self.connection().await {
            Ok(conn) => conn,
            Err(_) => return TenantDenylistDecision::Error,
        };
        let key = revoked_tenant_key(tenant_id);
        let cutoff: Result<Option<u64>, _> = conn.get(&key).await;
        match cutoff {
            Ok(Some(cutoff)) => TenantDenylistDecision::Cutoff(cutoff),
            Ok(None) => TenantDenylistDecision::NotFound,
            Err(_) => TenantDenylistDecision::Error,
        }
    }

    // ── Principal-level cluster kill (account hard-delete) ───────────────────

    /// Record that every token for principal `principal_id` (the JWT `sub`) issued
    /// at/before `cutoff_unix` is revoked — fired when an account/principal is
    /// hard-deleted, so the deleted user is logged out everywhere immediately
    /// rather than at natural TTL. Same key/TTL/overwrite semantics as
    /// [`deny_tenant_after`]; best-effort over the durable Postgres source of truth.
    pub async fn deny_principal_after(
        &self,
        principal_id: &str,
        cutoff_unix: u64,
    ) -> Result<(), String> {
        let mut conn = self.connection().await?;
        let key = revoked_principal_key(principal_id);
        let _: () = conn
            .set_ex(&key, cutoff_unix, self.default_ttl_secs)
            .await
            .map_err(|err| format!("principal denylist SETEX failed: {err}"))?;
        Ok(())
    }

    /// Read the principal's revoke cutoff. Reuses [`TenantDenylistDecision`] (the
    /// cutoff-vs-`iat` semantics are identical) so the caller applies posture via
    /// [`tenant_denylist_check_outcome`]. A miss → `NotFound` (fall through to DB);
    /// a connection/read failure → `Error` (availability-first defers to the DB).
    pub async fn principal_denied_after(&self, principal_id: &str) -> TenantDenylistDecision {
        let mut conn = match self.connection().await {
            Ok(conn) => conn,
            Err(_) => return TenantDenylistDecision::Error,
        };
        let key = revoked_principal_key(principal_id);
        let cutoff: Result<Option<u64>, _> = conn.get(&key).await;
        match cutoff {
            Ok(Some(cutoff)) => TenantDenylistDecision::Cutoff(cutoff),
            Ok(None) => TenantDenylistDecision::NotFound,
            Err(_) => TenantDenylistDecision::Error,
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

    #[test]
    fn revoked_tenant_key_is_namespaced() {
        let key = revoked_tenant_key("tenant-uuid");
        assert_eq!(key, "udb:denylist:tenant:tenant-uuid");
        assert!(key.starts_with(REVOKED_TENANT_KEY_PREFIX));
    }

    #[test]
    fn revoked_principal_key_is_namespaced() {
        let key = revoked_principal_key("user-uuid");
        assert_eq!(key, "udb:denylist:principal:user-uuid");
        assert!(key.starts_with(REVOKED_PRINCIPAL_KEY_PREFIX));
    }

    #[test]
    fn principal_cutoff_reuses_tenant_decision_semantics() {
        // Account hard-delete sets deny_principal_after(p, now); a token with
        // iat <= now is killed (inclusive boundary), a strictly-newer one is not —
        // identical cutoff-vs-iat logic to the tenant kill it reuses.
        let killed =
            tenant_denylist_check_outcome(TenantDenylistDecision::Cutoff(1000), 1000, false)
                .expect("iat == cutoff is revoked");
        assert!(killed.0);
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::Cutoff(1000), 1001, false)
                .is_none(),
            "a token issued after the principal kill survives → DB read"
        );
    }

    #[test]
    fn tenant_cutoff_revokes_tokens_issued_at_or_before_it() {
        // deny_tenant_after(t, 1000): a token with iat <= 1000 is killed; the
        // boundary (iat == cutoff) is inclusive.
        for iat in [0u64, 500, 1000] {
            let out =
                tenant_denylist_check_outcome(TenantDenylistDecision::Cutoff(1000), iat, false);
            let (revoked, reason) = out.expect("a token issued at/before the cutoff is revoked");
            assert!(revoked, "iat {iat} <= cutoff must be revoked");
            assert_eq!(reason, REVOCATION_TENANT_DENYLIST_HIT_REASON);
        }
    }

    #[test]
    fn tenant_cutoff_spares_tokens_issued_after_it() {
        // A token minted AFTER the kill (iat > cutoff) is a fresh credential and
        // must fall through to the DB read (None), not be killed.
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::Cutoff(1000), 1001, false)
                .is_none(),
            "iat strictly after the cutoff must fall through"
        );
        // Posture does not change this: a newer token is never killed by an older
        // cutoff even when hardened.
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::Cutoff(1000), 1001, true)
                .is_none()
        );
    }

    #[test]
    fn tenant_denylist_miss_falls_through_to_db() {
        // No cutoff recorded → DB is the source of truth, in both postures.
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::NotFound, 1000, false).is_none()
        );
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::NotFound, 1000, true).is_none()
        );
    }

    #[test]
    fn tenant_denylist_redis_outage_defers_to_db_in_fail_open() {
        // Availability-first: a Redis outage in dev/fail-open mode must NOT error
        // or deny spuriously — it falls through to the durable PG read (None).
        assert!(
            tenant_denylist_check_outcome(TenantDenylistDecision::Error, 1000, false).is_none(),
            "a Redis outage must defer to PG, never deny spuriously"
        );
        // In hardened mode the same outage fails closed (treat as revoked), exactly
        // like the jti denylist's `Error` handling.
        let hardened = tenant_denylist_check_outcome(TenantDenylistDecision::Error, 1000, true)
            .expect("hardened tenant denylist error must fail closed");
        assert!(hardened.0);
        assert_eq!(hardened.1, REVOCATION_STORE_UNAVAILABLE_REASON);
    }
}
