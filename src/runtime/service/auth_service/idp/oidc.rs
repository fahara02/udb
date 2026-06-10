//! Per-tenant OIDC provider registry + bounded-TTL discovery/JWKS cache (J2.1).
//!
//! Moves OIDC configuration out of process globals: providers are resolved by
//! `(tenant_id, provider_id)` from the `udb_idp.identity_providers` table.
//! `UDB_OIDC_*` env values remain as development defaults consulted only when a
//! request/provider does not supply a value (so existing single-tenant
//! deployments keep working).
//!
//! Discovery metadata + JWKS are cached per provider with a bounded TTL and a
//! forced-refresh entry point. The cache holds only the JWKS bytes (key
//! material is public); provider health + last-refresh status are persisted to
//! the provider row by the handler via [`super::store::record_jwks_refresh`].

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Cached JWKS for one provider, with the unix instant it was fetched.
#[derive(Clone)]
struct JwksEntry {
    jwks_json: String,
    kids: Vec<String>,
    fetched_at: Instant,
}

/// Process-wide OIDC discovery/JWKS cache keyed by `(tenant_id, provider_id)`.
/// Bounded TTL; `force_refresh` bypasses the TTL on demand.
#[derive(Clone, Default)]
pub struct OidcJwksCache {
    inner: Arc<RwLock<HashMap<String, JwksEntry>>>,
}

/// Outcome of a discovery/JWKS resolution attempt.
#[derive(Debug, Clone)]
pub struct JwksResolution {
    pub jwks_json: String,
    pub kids: Vec<String>,
    pub from_cache: bool,
}

fn cache_key(tenant_id: &str, provider_id: &str) -> String {
    format!("{tenant_id}\u{1f}{provider_id}")
}

/// JWKS cache TTL (seconds). `UDB_OIDC_JWKS_TTL_SECS`, default 3600.
fn jwks_ttl_secs() -> u64 {
    std::env::var("UDB_OIDC_JWKS_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3600)
}

impl OidcJwksCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the cached JWKS for a provider when present and within TTL.
    fn cached(&self, tenant_id: &str, provider_id: &str) -> Option<JwksEntry> {
        let ttl = Duration::from_secs(jwks_ttl_secs());
        let guard = self.inner.read().ok()?;
        let entry = guard.get(&cache_key(tenant_id, provider_id))?;
        if entry.fetched_at.elapsed() <= ttl {
            Some(entry.clone())
        } else {
            None
        }
    }

    fn store(&self, tenant_id: &str, provider_id: &str, jwks_json: String, kids: Vec<String>) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(
                cache_key(tenant_id, provider_id),
                JwksEntry {
                    jwks_json,
                    kids,
                    fetched_at: Instant::now(),
                },
            );
        }
    }

    /// Drop any cached entry for a provider (used before a forced refresh).
    pub fn invalidate(&self, tenant_id: &str, provider_id: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(&cache_key(tenant_id, provider_id));
        }
    }

    /// Resolve a provider's JWKS, honoring the bounded TTL cache. When
    /// `force_refresh` is set the cache is bypassed and re-populated. Returns the
    /// JWKS JSON + the `kid`s it advertises.
    ///
    /// `jwks_url` is the resolved endpoint (either the provider's stored
    /// `jwks_url`, or derived from discovery against `issuer`).
    pub async fn resolve(
        &self,
        tenant_id: &str,
        provider_id: &str,
        issuer: &str,
        jwks_url: &str,
        force_refresh: bool,
    ) -> Result<JwksResolution, String> {
        if !force_refresh {
            if let Some(entry) = self.cached(tenant_id, provider_id) {
                return Ok(JwksResolution {
                    jwks_json: entry.jwks_json,
                    kids: entry.kids,
                    from_cache: true,
                });
            }
        }
        let resolved_url = if !jwks_url.trim().is_empty() {
            jwks_url.trim().to_string()
        } else {
            discover_jwks_url(issuer).await?
        };
        let jwks_json = fetch_url(&resolved_url).await?;
        let kids = parse_jwks_kids(&jwks_json);
        self.store(tenant_id, provider_id, jwks_json.clone(), kids.clone());
        Ok(JwksResolution {
            jwks_json,
            kids,
            from_cache: false,
        })
    }
}

/// Resolve the JWKS endpoint from an issuer's OIDC discovery document.
async fn discover_jwks_url(issuer: &str) -> Result<String, String> {
    if issuer.trim().is_empty() {
        return Err("OIDC issuer is required to discover the JWKS endpoint".to_string());
    }
    let well_known = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let body = fetch_url(&well_known).await?;
    let doc: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| format!("invalid discovery document: {e}"))?;
    doc.get("jwks_uri")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| "discovery document has no jwks_uri".to_string())
}

/// Extract the advertised `kid`s from a JWKS document (for health/rotation
/// reporting and missing-kid diagnostics).
pub fn parse_jwks_kids(jwks_json: &str) -> Vec<String> {
    let doc: serde_json::Value = match serde_json::from_str(jwks_json) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    doc.get("keys")
        .and_then(|k| k.as_array())
        .map(|keys| {
            keys.iter()
                .filter_map(|k| {
                    k.get("kid")
                        .and_then(|v| v.as_str())
                        .map(ToString::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Best-effort HTTP GET that works whether or not the `http-client` feature is
/// built. Without it, JWKS resolution fails closed with a clear message.
#[cfg(feature = "http-client")]
async fn fetch_url(url: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url} returned HTTP {}", resp.status()));
    }
    resp.text()
        .await
        .map_err(|e| format!("reading {url} body failed: {e}"))
}

#[cfg(not(feature = "http-client"))]
async fn fetch_url(_url: &str) -> Result<String, String> {
    Err("OIDC discovery/JWKS requires the `http-client` feature".to_string())
}

/// Public HTTP GET helper for sibling modules (e.g. SAML metadata-URL import).
pub async fn fetch_text(url: &str) -> Result<String, String> {
    fetch_url(url).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_jwks_kids_and_handles_garbage() {
        let jwks = r#"{"keys":[{"kid":"k1","kty":"RSA"},{"kid":"k2"},{"kty":"oct"}]}"#;
        assert_eq!(parse_jwks_kids(jwks), vec!["k1", "k2"]);
        assert!(parse_jwks_kids("not json").is_empty());
        assert!(parse_jwks_kids("{}").is_empty());
    }

    #[test]
    fn cache_round_trips_within_ttl() {
        let cache = OidcJwksCache::new();
        cache.store("t1", "p1", "{\"keys\":[]}".into(), vec!["a".into()]);
        let hit = cache.cached("t1", "p1").expect("cache hit");
        assert_eq!(hit.kids, vec!["a".to_string()]);
        cache.invalidate("t1", "p1");
        assert!(cache.cached("t1", "p1").is_none());
    }
}
