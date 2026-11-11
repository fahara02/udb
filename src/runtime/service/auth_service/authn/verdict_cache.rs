//! Remembered answers to "is this UDB-issued token still backed by live state?".
//!
//! Every bearer call asks it, and a gateway asks it again through `Authenticate`
//! for every request it serves. Answering costs several round trips (user row,
//! token revocation, session, refresh family or service grant), so a page that
//! makes many calls paid them many times. A verdict for the same claims is reused
//! for a short time instead.
//!
//! Correctness bounds:
//! - Any auth event this node emits (revocation, logout, status change, grant,
//!   role or membership change, API key change, IdP provisioning) drops every
//!   remembered verdict, so such a change made on this node is seen by the next
//!   call.
//! - A change made on another broker node is seen within the cache lifetime,
//!   `UDB_AUTH_VERDICT_CACHE_MS` (default 2000 ms; 0 turns the cache off).
//! - Errors are never remembered: a store that cannot answer is asked again.
//! - The key covers every claim the verdict depends on, so two different tokens
//!   share an entry only when they carry the same claims.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use super::JwtPersistedState;
use crate::runtime::security::SecurityClaims;

/// Default lifetime of a remembered verdict.
const DEFAULT_TTL_MS: u64 = 2_000;
/// Entries kept at most; past this the cache is emptied, never grown.
const MAX_ENTRIES: usize = 50_000;

pub(super) type Key = [u8; 32];

struct Entry {
    stored_at: Instant,
    generation: u64,
    state: JwtPersistedState,
}

/// A verdict cache. The broker uses one process-wide instance ([`shared`]).
pub(super) struct VerdictCache {
    ttl: Duration,
    generation: AtomicU64,
    entries: Mutex<HashMap<Key, Entry>>,
}

impl VerdictCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            generation: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// The current generation, read before computing a verdict: a verdict
    /// computed while an auth event was emitted carries the older generation and
    /// is never stored or served.
    pub(super) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Drop every remembered verdict.
    pub(super) fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn get(&self, key: &Key) -> Option<JwtPersistedState> {
        if self.ttl.is_zero() {
            return None;
        }
        let current = self.generation();
        let mut map = self.entries.lock().ok()?;
        match map.get(key) {
            Some(entry) if entry.generation == current && entry.stored_at.elapsed() < self.ttl => {
                Some(entry.state.clone())
            }
            Some(_) => {
                map.remove(key);
                None
            }
            None => None,
        }
    }

    pub(super) fn put(&self, key: Key, generation: u64, state: JwtPersistedState) {
        if self.ttl.is_zero() || generation != self.generation() {
            return;
        }
        let Ok(mut map) = self.entries.lock() else {
            return;
        };
        if map.len() >= MAX_ENTRIES {
            map.clear();
        }
        map.insert(
            key,
            Entry {
                stored_at: Instant::now(),
                generation,
                state,
            },
        );
    }
}

/// The process-wide cache every service instance in this broker shares.
pub(super) fn shared() -> &'static VerdictCache {
    static SHARED: OnceLock<VerdictCache> = OnceLock::new();
    SHARED.get_or_init(|| {
        let millis = std::env::var("UDB_AUTH_VERDICT_CACHE_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_TTL_MS);
        VerdictCache::new(Duration::from_millis(millis))
    })
}

/// Drop every remembered verdict: called by every auth event emitter.
pub(crate) fn invalidate() {
    shared().invalidate();
}

/// The cache key: a digest over every claim the verdict depends on.
pub(super) fn key(claims: &SecurityClaims) -> Key {
    let mut scopes = claims.resolved_scopes();
    scopes.sort();
    let iat = claims.iat.map(|v| v.to_string()).unwrap_or_default();
    let exp = claims.exp.map(|v| v.to_string()).unwrap_or_default();
    let joined_scopes = scopes.join(" ");
    let parts: [&str; 11] = [
        claims.sub.as_deref().unwrap_or_default(),
        claims.tenant_id.as_deref().unwrap_or_default(),
        claims.project_id.as_deref().unwrap_or_default(),
        claims.service_identity.as_deref().unwrap_or_default(),
        claims.mbr.as_deref().unwrap_or_default(),
        claims.home_tenant_id.as_deref().unwrap_or_default(),
        claims.jti.as_deref().unwrap_or_default(),
        claims.iss.as_deref().unwrap_or_default(),
        &iat,
        &exp,
        &joined_scopes,
    ];
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims(sub: &str, jti: &str) -> SecurityClaims {
        SecurityClaims {
            sub: Some(sub.to_string()),
            jti: Some(jti.to_string()),
            tenant_id: Some("00000000-0000-0000-0000-0000000d0001".to_string()),
            ..Default::default()
        }
    }

    fn cache() -> VerdictCache {
        VerdictCache::new(Duration::from_secs(60))
    }

    #[test]
    fn a_remembered_verdict_is_served_until_an_auth_event_drops_it() {
        let cache = cache();
        let key = key(&claims("11111111-1111-1111-1111-111111111111", "sess_a"));
        cache.put(
            key,
            cache.generation(),
            JwtPersistedState::Valid { user: None },
        );
        assert!(matches!(
            cache.get(&key),
            Some(JwtPersistedState::Valid { .. })
        ));
        cache.invalidate();
        assert!(
            cache.get(&key).is_none(),
            "an auth event drops the remembered verdict"
        );
    }

    #[test]
    fn a_verdict_computed_across_an_auth_event_is_never_stored() {
        let cache = cache();
        let key = key(&claims("22222222-2222-2222-2222-222222222222", "sess_b"));
        let before = cache.generation();
        cache.invalidate();
        cache.put(key, before, JwtPersistedState::Valid { user: None });
        assert!(
            cache.get(&key).is_none(),
            "a verdict read before the event is stale"
        );
    }

    #[test]
    fn a_verdict_expires_after_its_lifetime() {
        let cache = VerdictCache::new(Duration::from_millis(30));
        let key = key(&claims("66666666-6666-6666-6666-666666666666", "sess_f"));
        cache.put(
            key,
            cache.generation(),
            JwtPersistedState::Valid { user: None },
        );
        assert!(cache.get(&key).is_some());
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            cache.get(&key).is_none(),
            "an expired verdict is asked again"
        );
    }

    #[test]
    fn a_zero_lifetime_turns_the_cache_off() {
        let cache = VerdictCache::new(Duration::ZERO);
        let key = key(&claims("77777777-7777-7777-7777-777777777777", "sess_g"));
        cache.put(
            key,
            cache.generation(),
            JwtPersistedState::Valid { user: None },
        );
        assert!(cache.get(&key).is_none());
    }

    #[test]
    fn different_claims_never_share_a_verdict() {
        let a = key(&claims("33333333-3333-3333-3333-333333333333", "sess_c"));
        let b = key(&claims("33333333-3333-3333-3333-333333333333", "sess_d"));
        let c = key(&claims("44444444-4444-4444-4444-444444444444", "sess_c"));
        assert_ne!(a, b);
        assert_ne!(a, c);
        let mut scoped = claims("33333333-3333-3333-3333-333333333333", "sess_c");
        scoped.scopes = Some(vec!["udb:read".to_string()]);
        assert_ne!(a, key(&scoped), "scopes are part of the key");
    }

    #[test]
    fn a_rejected_verdict_is_remembered_too() {
        let cache = cache();
        let key = key(&claims("55555555-5555-5555-5555-555555555555", "sess_e"));
        cache.put(key, cache.generation(), JwtPersistedState::Rejected);
        assert!(matches!(cache.get(&key), Some(JwtPersistedState::Rejected)));
    }
}
