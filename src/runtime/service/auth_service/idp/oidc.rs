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

// `bytes_stream()` on a reqwest response is a Stream; bounding the body
// requires its adapter.
#[cfg(feature = "http-client")]
use futures_util::StreamExt as _;

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

/// Discovery metadata an OIDC sign-in needs from
/// `<issuer>/.well-known/openid-configuration`.
#[derive(Debug, Clone, Default)]
pub struct OidcDiscovery {
    pub issuer: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    /// `token_endpoint_auth_methods_supported` (empty = not advertised, which
    /// OIDC Discovery defines as `client_secret_basic`).
    pub token_endpoint_auth_methods: Vec<String>,
}

#[derive(Clone)]
struct DiscoveryEntry {
    discovery: OidcDiscovery,
    fetched_at: Instant,
}

/// The shortest interval between two JWKS fetches forced by an unknown `kid`.
/// An ID token with a garbage `kid` must not turn every request into an
/// outbound fetch; a genuine key rotation is still picked up after this.
const JWKS_MIN_FORCED_REFRESH: Duration = Duration::from_secs(10);

static SHARED_CACHE: std::sync::OnceLock<OidcJwksCache> = std::sync::OnceLock::new();

static SHARED_DISCOVERY: std::sync::OnceLock<RwLock<HashMap<String, DiscoveryEntry>>> =
    std::sync::OnceLock::new();

fn discovery_cache() -> &'static RwLock<HashMap<String, DiscoveryEntry>> {
    SHARED_DISCOVERY.get_or_init(|| RwLock::new(HashMap::new()))
}

impl OidcJwksCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// The process-wide cache. `IdentityProviderService` and the AuthnService
    /// OIDC sign-in share it, so an UpdateProvider or ForceJwksRefresh
    /// invalidation is seen by sign-in immediately.
    pub fn shared() -> Self {
        SHARED_CACHE.get_or_init(Self::new).clone()
    }

    /// Resolve the JWKS that should hold `kid`. A cached set that already
    /// advertises `kid` is used as is; otherwise the set is refetched, but at
    /// most once per [`JWKS_MIN_FORCED_REFRESH`] per provider, so an unknown
    /// `kid` then fails cleanly against the cached set.
    pub async fn resolve_for_kid(
        &self,
        tenant_id: &str,
        provider_id: &str,
        issuer: &str,
        jwks_url: &str,
        kid: &str,
    ) -> Result<JwksResolution, String> {
        if let Some(entry) = self.cached(tenant_id, provider_id) {
            if entry.kids.iter().any(|k| k == kid)
                || entry.fetched_at.elapsed() < JWKS_MIN_FORCED_REFRESH
            {
                return Ok(JwksResolution {
                    jwks_json: entry.jwks_json,
                    kids: entry.kids,
                    from_cache: true,
                });
            }
        }
        self.resolve(tenant_id, provider_id, issuer, jwks_url, true)
            .await
    }

    /// Discovery metadata for a provider, cached with the JWKS TTL. The
    /// document's `issuer` must equal the configured issuer exactly (OpenID
    /// Connect Discovery 4.3); a mismatch is refused, never cached.
    pub async fn discovery(
        &self,
        tenant_id: &str,
        provider_id: &str,
        issuer: &str,
    ) -> Result<OidcDiscovery, String> {
        let key = cache_key(tenant_id, provider_id);
        let ttl = Duration::from_secs(jwks_ttl_secs());
        if let Ok(guard) = discovery_cache().read()
            && let Some(entry) = guard.get(&key)
            && entry.fetched_at.elapsed() <= ttl
            && entry.discovery.issuer == issuer.trim()
        {
            return Ok(entry.discovery.clone());
        }
        let discovery = fetch_discovery(issuer).await?;
        if let Ok(mut guard) = discovery_cache().write() {
            guard.insert(
                key,
                DiscoveryEntry {
                    discovery: discovery.clone(),
                    fetched_at: Instant::now(),
                },
            );
        }
        Ok(discovery)
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
    /// Discovery metadata is dropped with it: issuer and endpoints may have
    /// changed together.
    pub fn invalidate(&self, tenant_id: &str, provider_id: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(&cache_key(tenant_id, provider_id));
        }
        if let Ok(mut guard) = discovery_cache().write() {
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

/// Fetch and check an issuer's discovery document for a sign-in: the
/// authorization, token and JWKS endpoints must all be present, and the
/// document's own `issuer` must equal the configured one.
async fn fetch_discovery(issuer: &str) -> Result<OidcDiscovery, String> {
    let issuer = issuer.trim();
    if issuer.is_empty() {
        return Err("OIDC issuer is required for discovery".to_string());
    }
    let well_known = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let body = fetch_url(&well_known).await?;
    parse_discovery(issuer, &body)
}

/// Parse a discovery document (split out so the issuer/endpoint checks are
/// unit-tested without a network).
pub fn parse_discovery(issuer: &str, body: &str) -> Result<OidcDiscovery, String> {
    let doc: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("invalid discovery document: {e}"))?;
    let field = |name: &str| -> Result<String, String> {
        doc.get(name)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .ok_or_else(|| format!("discovery document has no {name}"))
    };
    let advertised = field("issuer")?;
    if advertised != issuer.trim() {
        return Err(format!(
            "discovery issuer {advertised} does not match the configured issuer {}",
            issuer.trim()
        ));
    }
    Ok(OidcDiscovery {
        issuer: advertised,
        authorization_endpoint: field("authorization_endpoint")?,
        token_endpoint: field("token_endpoint")?,
        token_endpoint_auth_methods: doc
            .get("token_endpoint_auth_methods_supported")
            .and_then(|v| v.as_array())
            .map(|methods| {
                methods
                    .iter()
                    .filter_map(|m| m.as_str().map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default(),
    })
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
/// Largest IdP document we will read. A discovery doc or JWKS is a few KiB; a
/// SAML metadata blob is larger but still small. Anything past this is either
/// misconfiguration or an endpoint trying to exhaust broker memory, and the
/// previous unbounded `resp.text()` gave it a free hand.
#[cfg(feature = "http-client")]
const MAX_IDP_DOCUMENT_BYTES: usize = 2 * 1024 * 1024;

/// Fetch an IdP document under the SAME SSRF policy webhook delivery uses.
///
/// These URLs (`issuer`, `jwks_url`, `saml_metadata_url`) are persisted by a
/// tenant administrator and then fetched from the BROKER's network, so they are
/// attacker-influenced input to a server-side request. The previous
/// implementation built a default client: redirects followed, any scheme or
/// host accepted, no address-range check, no DNS pinning, and an unbounded
/// body. A stored public URL could redirect - or DNS-rebind after validation -
/// onto loopback, RFC1918, or the cloud metadata endpoint.
///
/// Rather than grow a second partial check, this reuses
/// `webhook_service::resolve_and_pin_target`, which already enforces
/// https-only and rejects private, loopback, link-local, CGNAT, unspecified,
/// multicast and reserved ranges. Its resolved addresses are PINNED onto the
/// client here, which closes the check-to-connect rebinding window - the
/// pinning that struct was written for and that nothing had wired up.
#[cfg(feature = "http-client")]
async fn fetch_url(url: &str) -> Result<String, String> {
    let client = pinned_client(url).await?;
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("GET {url} returned HTTP {}", resp.status()));
    }
    read_bounded_body(url, resp).await
}

/// TEST-ONLY: lets the IdP fetches (discovery, JWKS, token endpoint) reach a
/// cleartext `http://` LOOPBACK socket (`http://127.0.0.1:<port>/...`) so a
/// live test can run a local fake OpenID provider. Compiled only into test
/// builds; production keeps the full https + SSRF guard for every IdP URL.
#[cfg(all(feature = "http-client", test))]
pub(crate) static ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// The SSRF-validated, address-pinned target of an IdP URL. In test builds
/// only, a loopback `http://ip:port` URL is pinned directly when
/// [`ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST`] is set.
#[cfg(feature = "http-client")]
async fn idp_target(url: &str) -> Result<(String, Vec<std::net::SocketAddr>), String> {
    #[cfg(test)]
    if ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST.load(std::sync::atomic::Ordering::SeqCst)
        && let Some(addr) = url
            .strip_prefix("http://")
            .and_then(|rest| rest.split(['/', '?']).next())
            .and_then(|hostport| hostport.parse::<std::net::SocketAddr>().ok())
        && addr.ip().is_loopback()
    {
        return Ok((addr.ip().to_string(), vec![addr]));
    }
    let target = crate::runtime::service::webhook_service::resolve_and_pin_target(url)
        .await
        .map_err(|status| format!("refused to fetch {url}: {}", status.message()))?;
    Ok((target.host, target.addrs))
}

/// A client bound to the SSRF-checked addresses of `url`: no redirects (a
/// redirect is a second, unvalidated URL; IdP endpoints answer directly), a
/// 10 second timeout, and the resolved addresses PINNED so a DNS rebind after
/// the check cannot move the connection onto an internal address.
#[cfg(feature = "http-client")]
async fn pinned_client(url: &str) -> Result<reqwest::Client, String> {
    let (host, addrs) = idp_target(url).await?;
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(&host, &addrs)
        .build()
        .map_err(|e| format!("http client build failed: {e}"))
}

/// Read an IdP response body under [`MAX_IDP_DOCUMENT_BYTES`].
#[cfg(feature = "http-client")]
async fn read_bounded_body(url: &str, resp: reqwest::Response) -> Result<String, String> {
    // Refuse a DECLARED oversize body before reading a byte...
    if let Some(len) = resp.content_length()
        && len as usize > MAX_IDP_DOCUMENT_BYTES
    {
        return Err(format!(
            "{url} declared {len} bytes, over the {MAX_IDP_DOCUMENT_BYTES}-byte IdP document limit"
        ));
    }
    // ...and bound the STREAMED body too, since Content-Length is optional under
    // chunked encoding and is supplied by the same untrusted endpoint.
    let mut body = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("reading {url} body failed: {e}"))?;
        if body.len() + chunk.len() > MAX_IDP_DOCUMENT_BYTES {
            return Err(format!(
                "{url} exceeded the {MAX_IDP_DOCUMENT_BYTES}-byte IdP document limit"
            ));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|e| format!("reading {url} body failed: {e}"))
}

/// The outcome of a token-endpoint POST: the HTTP status and the bounded body.
/// A 4xx carries the provider's OAuth error JSON, which the caller reduces to
/// its `error` code.
#[derive(Debug, Clone)]
pub struct TokenEndpointResponse {
    pub status: u16,
    pub body: String,
}

/// POST an `application/x-www-form-urlencoded` token request (RFC 6749 4.1.3)
/// under the same SSRF policy as the document fetches. `basic_auth` sends the
/// client credentials as HTTP Basic (`client_secret_basic`); the caller puts
/// them in `form` instead for `client_secret_post`.
#[cfg(feature = "http-client")]
pub async fn post_token_request(
    url: &str,
    form: &[(&str, &str)],
    basic_auth: Option<(&str, &str)>,
) -> Result<TokenEndpointResponse, String> {
    let client = pinned_client(url).await?;
    let mut request = client
        .post(url)
        .header("Accept", "application/json")
        .form(form);
    if let Some((client_id, client_secret)) = basic_auth {
        // RFC 6749 2.3.1: both halves are form-urlencoded before Basic encoding.
        request = request.basic_auth(
            urlencoding::encode(client_id).into_owned(),
            Some(urlencoding::encode(client_secret).into_owned()),
        );
    }
    let resp = request
        .send()
        .await
        .map_err(|e| format!("POST {url} failed: {e}"))?;
    let status = resp.status().as_u16();
    let body = read_bounded_body(url, resp).await?;
    Ok(TokenEndpointResponse { status, body })
}

#[cfg(not(feature = "http-client"))]
pub async fn post_token_request(
    _url: &str,
    _form: &[(&str, &str)],
    _basic_auth: Option<(&str, &str)>,
) -> Result<TokenEndpointResponse, String> {
    Err("the OIDC token endpoint requires the `http-client` feature".to_string())
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
    fn discovery_requires_matching_issuer_and_all_endpoints() {
        let doc = r#"{"issuer":"https://accounts.google.com",
            "authorization_endpoint":"https://accounts.google.com/o/oauth2/v2/auth",
            "token_endpoint":"https://oauth2.googleapis.com/token",
            "jwks_uri":"https://www.googleapis.com/oauth2/v3/certs",
            "token_endpoint_auth_methods_supported":["client_secret_post","client_secret_basic"]}"#;
        let parsed = parse_discovery("https://accounts.google.com", doc).expect("valid document");
        assert_eq!(parsed.token_endpoint, "https://oauth2.googleapis.com/token");
        assert_eq!(parsed.token_endpoint_auth_methods.len(), 2);
        // OpenID Connect Discovery 4.3: a document for another issuer is refused.
        assert!(parse_discovery("https://evil.example", doc).is_err());
        assert!(
            parse_discovery(
                "https://idp.example",
                r#"{"issuer":"https://idp.example","jwks_uri":"https://idp.example/jwks"}"#
            )
            .is_err(),
            "a document without authorization/token endpoints cannot drive a sign-in"
        );
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
