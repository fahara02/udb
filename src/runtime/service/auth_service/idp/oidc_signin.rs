//! What AuthnService's OIDC sign-in (StartOidcLogin / FinishOidcLogin /
//! LoginWithOidcIdToken, and invitation acceptance over OIDC) needs from the
//! IdP plane, kept here so the provider registry, its sealed client secret,
//! the discovery/JWKS cache, the SSRF-pinned HTTP client and the
//! external-identity resolution each have ONE implementation:
//!
//!   * [`load_sign_in_provider`] — an enabled OIDC provider of the tenant, with
//!     its client secret unsealed exactly the way the SCIM connector does it.
//!   * [`discovery`] / [`exchange_code`] — the provider's endpoints and the
//!     authorization-code + PKCE token request.
//!   * [`verify_id_token`] — signature (provider JWKS), `iss`, `aud`, `azp`,
//!     `exp`/`nbf`/`iat` and nonce checks. Pure core:
//!     [`verify_id_token_with_jwks`].
//!   * [`resolve_user`] — the ResolveExternalIdentity logic (existing link,
//!     account-linking policy, JIT provisioning).
//!
//! Every IdP URL is fetched under the webhook SSRF policy (https only,
//! private/loopback ranges refused, resolved addresses pinned, no redirects,
//! bounded bodies); see `oidc::fetch_url`.

use std::sync::Arc;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde_json::Value;
use sqlx::PgPool;
use tonic::Status;

use super::super::events::AuthEventSink;
pub(crate) use super::oidc::OidcDiscovery;
use super::oidc::OidcJwksCache;
use super::store::{self, ProviderRow};
use super::{
    FederatedIdentity, IdentityProviderServiceImpl, idp_internal_status, json_array_to_vec,
};
use crate::proto::udb::core::idp::entity::v1 as idp_entity_pb;
use crate::runtime::core::DataBrokerRuntime;

#[cfg(all(feature = "http-client", test))]
pub(crate) use super::oidc::ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST;

/// Google's issuer. Google documents that its ID tokens carry either this
/// value or the scheme-less `accounts.google.com` in `iss`; both are accepted
/// for a provider configured with this issuer, and only for it.
pub(crate) const GOOGLE_ISSUER: &str = "https://accounts.google.com";
const GOOGLE_LEGACY_ISSUER: &str = "accounts.google.com";

/// Clock skew tolerated on `exp`, `nbf` and `iat` (seconds).
const ID_TOKEN_LEEWAY_SECS: u64 = 60;

/// An ID token presented WITHOUT a nonce (a native app's Google Sign-In token)
/// is accepted only this many seconds after its `iat`, which bounds how long a
/// leaked token can be replayed.
pub(crate) const MAX_UNBOUND_ID_TOKEN_AGE_SECS: i64 = 600;

const MAX_REDIRECT_URIS: usize = 20;
const MAX_REDIRECT_URI_LEN: usize = 512;

/// An enabled OIDC provider of one tenant, as a sign-in uses it.
#[derive(Clone)]
pub(crate) struct OidcSignInProvider {
    pub provider_id: String,
    pub tenant_id: String,
    pub issuer: String,
    /// Accepted OAuth client ids; the first is the one the broker itself
    /// uses in authorization and token requests.
    pub client_ids: Vec<String>,
    /// Extra accepted `aud` values beyond `client_ids`.
    pub audiences: Vec<String>,
    pub redirect_uris: Vec<String>,
    /// Unsealed confidential-client secret ('' = public client, PKCE only).
    client_secret: String,
    row: ProviderRow,
}

impl std::fmt::Debug for OidcSignInProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcSignInProvider")
            .field("provider_id", &self.provider_id)
            .field("tenant_id", &self.tenant_id)
            .field("issuer", &self.issuer)
            .field("client_ids", &self.client_ids)
            .field("redirect_uris", &self.redirect_uris)
            .field("client_secret", &"[redacted]")
            .finish()
    }
}

impl OidcSignInProvider {
    /// The client id the broker presents to the provider.
    pub fn primary_client_id(&self) -> &str {
        self.client_ids
            .first()
            .map(String::as_str)
            .unwrap_or_default()
    }

    /// Exact-match check of a redirect URI against the allowlist.
    pub fn redirect_uri_allowed(&self, redirect_uri: &str) -> bool {
        !redirect_uri.is_empty() && self.redirect_uris.iter().any(|u| u == redirect_uri)
    }

    /// `iss` values accepted for this provider.
    fn accepted_issuers(&self) -> Vec<String> {
        let mut issuers = vec![self.issuer.clone()];
        if self.issuer == GOOGLE_ISSUER {
            issuers.push(GOOGLE_LEGACY_ISSUER.to_string());
        }
        issuers
    }

    /// `aud` values accepted for this provider: every client id plus the
    /// configured extra audiences.
    fn accepted_audiences(&self) -> Vec<String> {
        let mut audiences = self.client_ids.clone();
        for audience in &self.audiences {
            if !audiences.contains(audience) {
                audiences.push(audience.clone());
            }
        }
        audiences
    }

    /// The subject / email / email_verified the provider's claim mapping
    /// yields for verified claims: the same values ResolveExternalIdentity
    /// keys the external-identity link on.
    pub fn mapped_identity(&self, claims: &Value) -> (String, String, bool) {
        let mapped = super::mapping::apply_claim_mapping(&self.row.claim_mapping_json, claims);
        (mapped.subject, mapped.email, mapped.email_verified)
    }
}

/// Validate an OIDC redirect-URI allowlist (CreateProvider / UpdateProvider).
/// Each entry must be an absolute https URI, an http URI on a loopback IP
/// literal (native apps, RFC 8252 7.3), or a private-use scheme containing a
/// dot (RFC 8252 7.1); no fragment, no whitespace.
pub(crate) fn validate_redirect_uris(uris: &[String]) -> Result<(), Status> {
    if uris.len() > MAX_REDIRECT_URIS {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("at most {MAX_REDIRECT_URIS} redirect_uris are allowed"),
            [(
                "redirect_uris",
                format!("must list at most {MAX_REDIRECT_URIS} URIs"),
            )],
        ));
    }
    for (index, uri) in uris.iter().enumerate() {
        if let Err(reason) = check_redirect_uri(uri) {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                format!("redirect_uris[{index}] is not an allowed redirect URI: {reason}"),
                [(format!("redirect_uris[{index}]"), reason.to_string())],
            ));
        }
    }
    Ok(())
}

fn check_redirect_uri(uri: &str) -> Result<(), &'static str> {
    if uri.is_empty() || uri.len() > MAX_REDIRECT_URI_LEN {
        return Err("must be 1 to 512 characters");
    }
    if uri.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("must not contain whitespace or control characters");
    }
    if uri.contains('#') {
        return Err("must not contain a fragment");
    }
    let Some((scheme, rest)) = uri.split_once(':') else {
        return Err("must be an absolute URI");
    };
    let scheme_lower = scheme.to_ascii_lowercase();
    match scheme_lower.as_str() {
        "https" => {
            let host = authority_host(rest).ok_or("must be https://<host>/...")?;
            if host.is_empty() {
                return Err("must name a host");
            }
            Ok(())
        }
        "http" => {
            let host = authority_host(rest).ok_or("must be http://<loopback ip>/...")?;
            let host = host.trim_start_matches('[').trim_end_matches(']');
            match host.parse::<std::net::IpAddr>() {
                Ok(ip) if ip.is_loopback() => Ok(()),
                _ => Err("http is allowed only on a loopback IP literal (127.0.0.1 or [::1])"),
            }
        }
        "javascript" | "data" | "file" | "vbscript" | "blob" | "about" => {
            Err("this URI scheme is never an allowed redirect")
        }
        _ => {
            let valid_scheme = scheme
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic())
                && scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
            if !valid_scheme || !scheme.contains('.') {
                return Err(
                    "must be https, http on a loopback IP, or a private-use scheme containing a dot (com.example.app:/callback)",
                );
            }
            if rest.is_empty() {
                return Err("must include a path after the scheme");
            }
            Ok(())
        }
    }
}

/// The host of `//host[:port]/...` (IPv6 kept in brackets); `None` when the
/// remainder has no `//` authority.
fn authority_host(rest: &str) -> Option<&str> {
    let after = rest.strip_prefix("//")?;
    let authority = &after[..after.find(['/', '?']).unwrap_or(after.len())];
    if authority.contains('@') {
        return None;
    }
    if authority.starts_with('[') {
        let close = authority.find(']')?;
        return Some(&authority[..=close]);
    }
    Some(authority.split(':').next().unwrap_or_default())
}

/// Load an enabled OIDC provider of `tenant_id` for sign-in. `Ok(None)` for
/// every reason a sign-in cannot use it (unknown or malformed id, deleted,
/// disabled, not OIDC, no client id), so callers answer all of them the same
/// way. The client secret is unsealed with the runtime's at-rest key, exactly
/// like the SCIM connector bearer; an AEAD envelope whose key is unavailable
/// is an operational error, never used as the secret.
pub(crate) async fn load_sign_in_provider(
    pool: &PgPool,
    runtime: Option<&DataBrokerRuntime>,
    tenant_id: &str,
    provider_id: &str,
) -> Result<Option<OidcSignInProvider>, Status> {
    if tenant_id.trim().is_empty() || uuid::Uuid::parse_str(provider_id.trim()).is_err() {
        return Ok(None);
    }
    let Some(row) = store::get_provider(pool, tenant_id.trim(), provider_id.trim()).await? else {
        return Ok(None);
    };
    if !row.enabled
        || super::kind_to_pb(&row.kind) != idp_entity_pb::IdpKind::Oidc as i32
        || row.issuer.trim().is_empty()
    {
        return Ok(None);
    }
    let client_ids = json_array_to_vec(&row.client_ids_json)
        .into_iter()
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect::<Vec<_>>();
    if client_ids.is_empty() {
        return Ok(None);
    }
    let stored = store::get_active_provider_client_secret(pool, &row.tenant_id, &row.provider_id)
        .await?
        .unwrap_or_default();
    let client_secret = unseal_client_secret(runtime, &stored)?;
    Ok(Some(OidcSignInProvider {
        provider_id: row.provider_id.clone(),
        tenant_id: row.tenant_id.clone(),
        issuer: row.issuer.trim().to_string(),
        client_ids,
        audiences: json_array_to_vec(&row.audiences_json),
        redirect_uris: json_array_to_vec(&row.redirect_uris_json),
        client_secret,
        row,
    }))
}

fn unseal_client_secret(
    runtime: Option<&DataBrokerRuntime>,
    stored: &str,
) -> Result<String, Status> {
    if stored.is_empty() {
        return Ok(String::new());
    }
    let plaintext = match runtime {
        Some(runtime) => runtime.decrypt_secret_at_rest(stored).map_err(|err| {
            idp_internal_status(
                "oidc_client_secret_decrypt",
                format!("OIDC client secret decryption failed: {err}"),
            )
        })?,
        None => stored.to_string(),
    };
    if stored.starts_with("udb-aead:") && plaintext == stored {
        return Err(idp_internal_status(
            "oidc_client_secret_decrypt",
            "OIDC client secret is encrypted but no matching decryption key is available",
        ));
    }
    Ok(plaintext)
}

/// The provider's discovery metadata (process-wide cache, issuer-checked). The
/// dev OpenID provider (see [`DEV_TEST_ISSUER`]) answers in-process.
pub(crate) async fn discovery(provider: &OidcSignInProvider) -> Result<OidcDiscovery, String> {
    if dev_provider(provider) {
        return Ok(dev_discovery());
    }
    OidcJwksCache::shared()
        .discovery(&provider.tenant_id, &provider.provider_id, &provider.issuer)
        .await
}

/// Why a token-endpoint exchange produced no ID token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TokenExchangeError {
    /// The provider refused the grant (OAuth `error` code, e.g.
    /// `invalid_grant` for a reused or expired code).
    Rejected(String),
    /// The provider could not be reached or answered something unusable.
    Unavailable(String),
}

/// Redeem an authorization code at the provider's token endpoint (RFC 6749
/// 4.1.3 + RFC 7636 4.5) and return the ID token. A confidential client
/// authenticates with `client_secret_basic` unless the provider advertises
/// only `client_secret_post`; a public client sends just its `client_id`.
pub(crate) async fn exchange_code(
    provider: &OidcSignInProvider,
    discovery: &OidcDiscovery,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
) -> Result<String, TokenExchangeError> {
    let client_id = provider.primary_client_id();
    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("code_verifier", code_verifier),
    ];
    let methods = &discovery.token_endpoint_auth_methods;
    let use_post = !provider.client_secret.is_empty()
        && !methods.is_empty()
        && !methods.iter().any(|m| m == "client_secret_basic")
        && methods.iter().any(|m| m == "client_secret_post");
    let basic_auth = if provider.client_secret.is_empty() {
        form.push(("client_id", client_id));
        None
    } else if use_post {
        form.push(("client_id", client_id));
        form.push(("client_secret", provider.client_secret.as_str()));
        None
    } else {
        Some((client_id, provider.client_secret.as_str()))
    };
    let response = super::oidc::post_token_request(&discovery.token_endpoint, &form, basic_auth)
        .await
        .map_err(TokenExchangeError::Unavailable)?;
    parse_token_response(response.status, &response.body)
}

/// Reduce a token-endpoint answer to its ID token (split out for unit tests).
pub(crate) fn parse_token_response(status: u16, body: &str) -> Result<String, TokenExchangeError> {
    let doc: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    if !(200..300).contains(&status) {
        let error = doc
            .get("error")
            .and_then(Value::as_str)
            .filter(|e| !e.is_empty() && e.len() <= 64)
            .unwrap_or("token_request_failed");
        return if (400..500).contains(&status) {
            Err(TokenExchangeError::Rejected(error.to_string()))
        } else {
            Err(TokenExchangeError::Unavailable(format!(
                "token endpoint returned HTTP {status} ({error})"
            )))
        };
    }
    doc.get("id_token")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| {
            TokenExchangeError::Unavailable(
                "token endpoint response carries no id_token (is the openid scope enabled?)"
                    .to_string(),
            )
        })
}

/// Why an ID token was refused. Callers turn every variant except
/// `KeysUnavailable` into one opaque "invalid credential" answer and log the
/// variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdTokenRejection {
    /// Not a JWS compact token.
    Malformed,
    /// `alg` outside the asymmetric set (never `none` or HMAC).
    UnsupportedAlgorithm,
    /// No JWKS key matches the token's `kid`.
    UnknownKey,
    /// Signature, `iss`, `aud`, `exp`, `nbf`, `iat` or `sub` check failed.
    Invalid(String),
    /// Multi-audience token whose `azp` is not one of the provider's clients.
    AuthorizedPartyMismatch,
    /// The expected nonce differs from the token's.
    NonceMismatch,
    /// A nonce was expected but the token has none.
    NonceMissing,
    /// The token is nonce-bound but the caller supplied no nonce.
    UnexpectedNonce,
    /// An unbound (no-nonce) token older than [`MAX_UNBOUND_ID_TOKEN_AGE_SECS`].
    Stale,
    /// The provider's keys could not be fetched (operational, not the token).
    KeysUnavailable(String),
}

/// What [`verify_id_token_with_jwks`] checks besides the signature.
#[derive(Debug, Clone)]
pub(crate) struct IdTokenExpectations<'a> {
    pub issuers: Vec<String>,
    pub audiences: Vec<String>,
    pub client_ids: Vec<String>,
    /// `Some` = the token's `nonce` must equal it; `None` = the token must
    /// carry no nonce and be at most [`MAX_UNBOUND_ID_TOKEN_AGE_SECS`] old.
    pub nonce: Option<&'a str>,
    pub now_unix: i64,
}

/// Verify `id_token` against the provider's JWKS (resolved through the shared
/// discovery/JWKS cache; an unknown `kid` refetches at most every 10 s) and
/// return its claims.
pub(crate) async fn verify_id_token(
    provider: &OidcSignInProvider,
    id_token: &str,
    nonce: Option<&str>,
    now_unix: i64,
) -> Result<Value, IdTokenRejection> {
    let header = jsonwebtoken::decode_header(id_token).map_err(|_| IdTokenRejection::Malformed)?;
    let expectations = IdTokenExpectations {
        issuers: provider.accepted_issuers(),
        audiences: provider.accepted_audiences(),
        client_ids: provider.client_ids.clone(),
        nonce,
        now_unix,
    };
    // The dev OpenID provider publishes its keys in-process; its tokens then
    // take exactly the checks below.
    if dev_provider(provider) {
        let jwks_json = dev_jwks_json().map_err(IdTokenRejection::KeysUnavailable)?;
        return verify_id_token_with_jwks(&jwks_json, id_token, &expectations);
    }
    let kid = header.kid.clone().unwrap_or_default();
    let jwks = OidcJwksCache::shared()
        .resolve_for_kid(
            &provider.tenant_id,
            &provider.provider_id,
            &provider.issuer,
            &provider.row.jwks_url,
            &kid,
        )
        .await
        .map_err(IdTokenRejection::KeysUnavailable)?;
    verify_id_token_with_jwks(&jwks.jwks_json, id_token, &expectations)
}

/// The pure ID-token check (OpenID Connect Core 3.1.3.7): asymmetric `alg`,
/// signature by the JWKS key named by `kid`, `iss` in `issuers`, `aud`
/// intersecting `audiences`, `azp` among the client ids when `aud` has several
/// values, `exp`/`nbf`/`iat` within skew, a non-empty `sub`, then the nonce
/// rule of [`IdTokenExpectations::nonce`]. Returns the verified claims.
pub(crate) fn verify_id_token_with_jwks(
    jwks_json: &str,
    id_token: &str,
    expect: &IdTokenExpectations<'_>,
) -> Result<Value, IdTokenRejection> {
    let header = jsonwebtoken::decode_header(id_token).map_err(|_| IdTokenRejection::Malformed)?;
    if !matches!(
        header.alg,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::PS256
            | Algorithm::PS384
            | Algorithm::PS512
            | Algorithm::ES256
            | Algorithm::ES384
    ) {
        return Err(IdTokenRejection::UnsupportedAlgorithm);
    }
    let jwks: JwkSet = serde_json::from_str(jwks_json).map_err(|_| IdTokenRejection::UnknownKey)?;
    let jwk = match header.kid.as_deref().filter(|kid| !kid.trim().is_empty()) {
        Some(kid) => jwks
            .keys
            .iter()
            .find(|jwk| jwk.common.key_id.as_deref() == Some(kid)),
        // A provider publishing exactly one key may omit `kid`.
        None if jwks.keys.len() == 1 => jwks.keys.first(),
        None => None,
    }
    .ok_or(IdTokenRejection::UnknownKey)?;
    let key = DecodingKey::from_jwk(jwk).map_err(|_| IdTokenRejection::UnknownKey)?;
    if expect.issuers.is_empty() || expect.audiences.is_empty() {
        return Err(IdTokenRejection::Invalid(
            "provider has no issuer or audience to check".to_string(),
        ));
    }
    let mut validation = Validation::new(header.alg);
    validation.leeway = ID_TOKEN_LEEWAY_SECS;
    validation.validate_nbf = true;
    validation.set_issuer(&expect.issuers);
    validation.set_audience(&expect.audiences);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    let claims = jsonwebtoken::decode::<Value>(id_token, &key, &validation)
        .map_err(|err| IdTokenRejection::Invalid(err.to_string()))?
        .claims;

    let iat = claims
        .get("iat")
        .and_then(Value::as_i64)
        .ok_or_else(|| IdTokenRejection::Invalid("missing iat".to_string()))?;
    if iat > expect.now_unix + ID_TOKEN_LEEWAY_SECS as i64 {
        return Err(IdTokenRejection::Invalid(
            "iat is in the future".to_string(),
        ));
    }
    if claims
        .get("sub")
        .and_then(Value::as_str)
        .is_none_or(|sub| sub.trim().is_empty())
    {
        return Err(IdTokenRejection::Invalid("missing sub".to_string()));
    }
    if let Some(audiences) = claims.get("aud").and_then(Value::as_array)
        && audiences.len() > 1
    {
        let azp = claims
            .get("azp")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !expect.client_ids.iter().any(|id| id == azp) {
            return Err(IdTokenRejection::AuthorizedPartyMismatch);
        }
    }
    let token_nonce = claims.get("nonce").and_then(Value::as_str);
    match (expect.nonce, token_nonce) {
        (Some(expected), Some(actual)) => {
            if !crate::runtime::authn::constant_time_eq(expected, actual) {
                return Err(IdTokenRejection::NonceMismatch);
            }
        }
        (Some(_), None) => return Err(IdTokenRejection::NonceMissing),
        (None, Some(_)) => return Err(IdTokenRejection::UnexpectedNonce),
        (None, None) => {
            if iat < expect.now_unix - MAX_UNBOUND_ID_TOKEN_AGE_SECS {
                return Err(IdTokenRejection::Stale);
            }
        }
    }
    Ok(claims)
}

// ── dev-only in-process OpenID provider (UDB_OIDC_TEST_MODE) ─────────────────
//
// Mirrors the dev self-asserted SAML IdP (`saml::DEV_SENTINEL`): a headless
// harness (the SDK benchmark, served CI) has no reachable https OpenID
// provider, and the SSRF guard rightly refuses a loopback fake. When
// `UDB_OIDC_TEST_MODE` is on, a provider whose issuer is EXACTLY
// [`DEV_TEST_ISSUER`] needs no network: its discovery document, JWKS and token
// endpoint are answered in-process with a dev-only RS256 key. The token
// endpoint accepts the code `__UDB_OIDC_TEST__:<subject>[:<email>]` and mints a
// REAL signed ID token carrying the nonce stored with the sign-in state;
// LoginWithOidcIdToken (and invitation acceptance) accept the same sentinel as
// `id_token` and get a real signed token minted for it. Every minted token then
// goes through the normal checks (`verify_id_token_with_jwks`: signature, `iss`,
// `aud`, expiry, nonce) — never an accept-any bypass. Refused in a production
// posture; a provider with any other issuer is untouched.

/// The issuer that selects the dev OpenID provider in test mode. `.invalid`
/// (RFC 2606) never resolves, so outside test mode such a provider fails
/// closed at discovery.
pub(crate) const DEV_TEST_ISSUER: &str = "https://oidc.udb-test.invalid";

/// Code / ID-token sentinel a test harness sends, optionally suffixed with
/// `:<subject>` and `:<email>`.
pub(crate) const DEV_SENTINEL: &str = "__UDB_OIDC_TEST__";

/// `kid` of the dev provider's one signing key.
const DEV_KEY_ID: &str = "udb-oidc-test-1";

/// Lifetime of a minted dev ID token (seconds).
const DEV_ID_TOKEN_TTL_SECS: i64 = 300;

/// Subject used when the sentinel names none.
const DEV_DEFAULT_SUBJECT: &str = "udb-oidc-test-user";

/// Whether the dev OpenID provider is enabled (resolved once). Fail-closed:
/// `UDB_OIDC_TEST_MODE` is REFUSED in a production posture (same stance as
/// `UDB_SAML_TEST_MODE` and the WebAuthn soft-authenticator) — the dev provider
/// stays disabled and the refusal is logged at error level.
pub(crate) fn dev_test_mode_enabled() -> bool {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        resolve_dev_test_mode(
            dev_test_mode_requested(std::env::var("UDB_OIDC_TEST_MODE").ok().as_deref()),
            crate::runtime::security::SecurityConfig::current().is_production(),
        )
    })
}

/// `UDB_OIDC_TEST_MODE` asks for the dev provider only when set to 1/true/yes;
/// unset (the default) or anything else leaves it off.
fn dev_test_mode_requested(raw: Option<&str>) -> bool {
    raw.map(str::trim).is_some_and(|v| {
        v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
    })
}

/// Pure gate for [`dev_test_mode_enabled`]: production never runs the dev
/// provider.
fn resolve_dev_test_mode(requested: bool, production: bool) -> bool {
    if requested && production {
        tracing::error!(
            "UDB_OIDC_TEST_MODE is set in production; the dev in-process OpenID provider is \
             refused and stays disabled (unset UDB_OIDC_TEST_MODE)"
        );
        return false;
    }
    requested
}

/// Pure check: the dev provider answers for `issuer` only in test mode and
/// only for exactly [`DEV_TEST_ISSUER`].
fn dev_provider_active(issuer: &str, test_mode: bool) -> bool {
    test_mode && issuer.trim() == DEV_TEST_ISSUER
}

/// Whether `provider` is served by the in-process dev OpenID provider.
fn dev_provider(provider: &OidcSignInProvider) -> bool {
    dev_provider_active(&provider.issuer, dev_test_mode_enabled())
}

/// The dev provider's discovery document.
fn dev_discovery() -> OidcDiscovery {
    OidcDiscovery {
        issuer: DEV_TEST_ISSUER.to_string(),
        authorization_endpoint: format!("{DEV_TEST_ISSUER}/authorize"),
        token_endpoint: format!("{DEV_TEST_ISSUER}/token"),
        token_endpoint_auth_methods: vec!["client_secret_basic".to_string(), "none".to_string()],
    }
}

/// The dev provider's signing key and published JWKS (built once).
fn dev_signing_material() -> Result<&'static (jsonwebtoken::EncodingKey, String), String> {
    use std::sync::OnceLock;
    static MATERIAL: OnceLock<Result<(jsonwebtoken::EncodingKey, String), String>> =
        OnceLock::new();
    MATERIAL
        .get_or_init(|| {
            use base64::Engine as _;
            use rsa::pkcs8::DecodePrivateKey;
            use rsa::traits::PublicKeyParts;
            let pem = super::saml::DEV_IDP_KEY_PKCS8_PEM;
            let private = rsa::RsaPrivateKey::from_pkcs8_pem(pem)
                .map_err(|err| format!("dev OIDC key: {err}"))?;
            let encoding = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes())
                .map_err(|err| format!("dev OIDC signing key: {err}"))?;
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
            let jwks = serde_json::json!({
                "keys": [{
                    "kty": "RSA",
                    "use": "sig",
                    "alg": "RS256",
                    "kid": DEV_KEY_ID,
                    "n": b64.encode(private.n().to_bytes_be()),
                    "e": b64.encode(private.e().to_bytes_be()),
                }]
            })
            .to_string();
            Ok((encoding, jwks))
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// The dev provider's JWKS document.
fn dev_jwks_json() -> Result<String, String> {
    dev_signing_material().map(|(_, jwks)| jwks.clone())
}

/// `(subject, email)` named by a sentinel `__UDB_OIDC_TEST__[:<subject>[:<email>]]`,
/// or `None` when `raw` is not one.
fn dev_sentinel_identity(raw: &str) -> Option<(String, String)> {
    let rest = raw.trim().strip_prefix(DEV_SENTINEL)?;
    if !rest.is_empty() && !rest.starts_with(':') {
        return None;
    }
    let rest = rest.strip_prefix(':').unwrap_or(rest);
    let (subject, email) = rest.split_once(':').unwrap_or((rest, ""));
    let subject = match subject.trim() {
        "" => DEV_DEFAULT_SUBJECT.to_string(),
        subject => subject.to_string(),
    };
    Some((subject, email.trim().to_string()))
}

/// Sign a dev ID token for `subject` the way a real provider would: issued by
/// [`DEV_TEST_ISSUER`] to the provider's primary client id, valid for
/// [`DEV_ID_TOKEN_TTL_SECS`], with `nonce` when one is bound and a fresh `jti`
/// so two tokens are never identical. An email is reported verified.
fn dev_mint_id_token(
    client_id: &str,
    subject: &str,
    email: &str,
    nonce: Option<&str>,
    now_unix: i64,
) -> Result<String, String> {
    let (key, _) = dev_signing_material()?;
    let mut claims = serde_json::json!({
        "iss": DEV_TEST_ISSUER,
        "aud": client_id,
        "sub": subject,
        "iat": now_unix,
        "exp": now_unix + DEV_ID_TOKEN_TTL_SECS,
        "jti": uuid::Uuid::new_v4().to_string(),
    });
    if !email.is_empty() {
        claims["email"] = Value::String(email.to_string());
        claims["email_verified"] = Value::Bool(true);
    }
    if let Some(nonce) = nonce.filter(|nonce| !nonce.is_empty()) {
        claims["nonce"] = Value::String(nonce.to_string());
    }
    let mut header = jsonwebtoken::Header::new(Algorithm::RS256);
    header.kid = Some(DEV_KEY_ID.to_string());
    jsonwebtoken::encode(&header, &claims, key).map_err(|err| format!("dev ID token: {err}"))
}

/// The dev provider's token endpoint: `None` unless `provider` is the dev
/// provider (the caller then redeems the code at the real endpoint). A code
/// that is not the sentinel is refused like a provider's `invalid_grant`; the
/// sentinel yields a signed ID token bound to the sign-in's stored `nonce`.
pub(crate) fn dev_exchange_code(
    provider: &OidcSignInProvider,
    code: &str,
    nonce: &str,
    now_unix: i64,
) -> Option<Result<String, TokenExchangeError>> {
    if !dev_provider(provider) {
        return None;
    }
    let Some((subject, email)) = dev_sentinel_identity(code) else {
        return Some(Err(TokenExchangeError::Rejected(
            "invalid_grant".to_string(),
        )));
    };
    Some(
        dev_mint_id_token(
            provider.primary_client_id(),
            &subject,
            &email,
            Some(nonce),
            now_unix,
        )
        .map_err(TokenExchangeError::Unavailable),
    )
}

/// The ID token to verify for a presented `id_token`: the presented value
/// itself, except that for the dev provider a sentinel is replaced by a real
/// signed token (bound to `nonce` when the caller supplied one), which then
/// takes the normal checks.
pub(crate) fn presented_id_token(
    provider: &OidcSignInProvider,
    id_token: &str,
    nonce: Option<&str>,
    now_unix: i64,
) -> Result<String, IdTokenRejection> {
    if !dev_provider(provider) {
        return Ok(id_token.to_string());
    }
    match dev_sentinel_identity(id_token) {
        Some((subject, email)) => dev_mint_id_token(
            provider.primary_client_id(),
            &subject,
            &email,
            nonce,
            now_unix,
        )
        .map_err(IdTokenRejection::KeysUnavailable),
        None => Ok(id_token.to_string()),
    }
}

/// Resolve verified claims to a UDB user with the ResolveExternalIdentity
/// logic (existing link, account-linking policy, JIT provisioning), emitting
/// its events through `event_sink`.
pub(crate) async fn resolve_user(
    pool: &PgPool,
    event_sink: Arc<dyn AuthEventSink>,
    provider: &OidcSignInProvider,
    claims: &Value,
) -> Result<FederatedIdentity, Status> {
    IdentityProviderServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_event_sink(event_sink)
        .resolve_federated_identity(pool, &provider.row, claims)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_uri_rules() {
        for ok in [
            "https://app.example.com/oidc/callback",
            "https://app.example.com:8443/cb?x=1",
            "http://127.0.0.1:53682/callback",
            "http://[::1]:8080/cb",
            "com.example.app:/oauth2redirect",
            "com.googleusercontent.apps.123-abc:/oauth2redirect",
        ] {
            assert!(check_redirect_uri(ok).is_ok(), "{ok} should be allowed");
        }
        for bad in [
            "",
            "http://app.example.com/cb",
            "http://localhost:8080/cb",
            "http://10.0.0.5/cb",
            "https://app.example.com/cb#frag",
            "https:///cb",
            "https://user@evil.example/cb",
            "javascript:alert(1)",
            "myapp:/cb",
            "/relative/cb",
            "https://app.example.com/a b",
        ] {
            assert!(check_redirect_uri(bad).is_err(), "{bad} should be refused");
        }
        let many = vec!["https://a.example/cb".to_string(); MAX_REDIRECT_URIS + 1];
        let err = validate_redirect_uris(&many).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let err = validate_redirect_uris(&["http://evil.example/cb".to_string()]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("redirect_uris[0]"));
    }

    #[test]
    fn token_response_parsing() {
        assert_eq!(
            parse_token_response(
                200,
                r#"{"access_token":"a","id_token":"x.y.z","token_type":"Bearer"}"#
            ),
            Ok("x.y.z".to_string())
        );
        assert_eq!(
            parse_token_response(400, r#"{"error":"invalid_grant"}"#),
            Err(TokenExchangeError::Rejected("invalid_grant".to_string()))
        );
        assert!(matches!(
            parse_token_response(200, r#"{"access_token":"a"}"#),
            Err(TokenExchangeError::Unavailable(_))
        ));
        assert!(matches!(
            parse_token_response(503, "upstream down"),
            Err(TokenExchangeError::Unavailable(_))
        ));
    }

    #[test]
    fn google_issuer_accepts_both_documented_forms() {
        let provider = OidcSignInProvider {
            provider_id: "p".into(),
            tenant_id: "t".into(),
            issuer: GOOGLE_ISSUER.into(),
            client_ids: vec!["web".into(), "android".into()],
            audiences: vec!["android".into(), "extra".into()],
            redirect_uris: vec!["https://app.example.com/cb".into()],
            client_secret: "s3cret".into(),
            row: ProviderRow::default(),
        };
        assert_eq!(
            provider.accepted_issuers(),
            vec![GOOGLE_ISSUER.to_string(), GOOGLE_LEGACY_ISSUER.to_string()]
        );
        assert_eq!(
            provider.accepted_audiences(),
            vec!["web", "android", "extra"]
        );
        assert!(provider.redirect_uri_allowed("https://app.example.com/cb"));
        assert!(!provider.redirect_uri_allowed("https://app.example.com/cb/"));
        assert!(!format!("{provider:?}").contains("s3cret"));
    }

    #[test]
    fn oidc_test_mode_is_off_by_default() {
        assert!(
            !dev_test_mode_requested(None),
            "an unset UDB_OIDC_TEST_MODE leaves the dev provider off"
        );
        for off in ["", "0", "false", "no", "off", "2"] {
            assert!(
                !dev_test_mode_requested(Some(off)),
                "{off:?} must not enable it"
            );
        }
        for on in ["1", "true", "TRUE", "yes", " 1 "] {
            assert!(dev_test_mode_requested(Some(on)), "{on:?} enables it");
        }
        // Even an issuer naming the dev provider is not served in-process
        // unless test mode is on.
        assert!(!dev_provider_active(DEV_TEST_ISSUER, false));
    }

    #[test]
    fn oidc_test_mode_is_refused_in_production() {
        assert!(
            resolve_dev_test_mode(true, false),
            "dev posture honors the flag"
        );
        assert!(
            !resolve_dev_test_mode(true, true),
            "production must refuse the dev OpenID provider"
        );
        assert!(!resolve_dev_test_mode(false, true));
        assert!(!resolve_dev_test_mode(false, false));
    }

    #[test]
    fn dev_provider_answers_only_for_the_test_issuer() {
        assert!(dev_provider_active(DEV_TEST_ISSUER, true));
        for other in [
            GOOGLE_ISSUER,
            "https://oidc.udb-test.invalid/",
            "https://oidc.udb-test.invalid.example.com",
            "http://oidc.udb-test.invalid",
            "",
        ] {
            assert!(
                !dev_provider_active(other, true),
                "{other:?} must keep the real network path"
            );
        }
        let discovery = dev_discovery();
        assert_eq!(discovery.issuer, DEV_TEST_ISSUER);
        assert!(discovery.authorization_endpoint.starts_with("https://"));
    }

    #[test]
    fn dev_sentinel_names_subject_and_email() {
        assert_eq!(
            dev_sentinel_identity("__UDB_OIDC_TEST__:alice:alice@example.test"),
            Some(("alice".to_string(), "alice@example.test".to_string()))
        );
        assert_eq!(
            dev_sentinel_identity(" __UDB_OIDC_TEST__:bob "),
            Some(("bob".to_string(), String::new()))
        );
        assert_eq!(
            dev_sentinel_identity("__UDB_OIDC_TEST__"),
            Some((DEV_DEFAULT_SUBJECT.to_string(), String::new()))
        );
        assert_eq!(dev_sentinel_identity("__UDB_OIDC_TEST__x"), None);
        assert_eq!(dev_sentinel_identity("real-authorization-code"), None);
        assert_eq!(dev_sentinel_identity("eyJhbGciOiJSUzI1NiJ9.e30.sig"), None);
    }

    /// A minted dev token is a real RS256 JWS that the normal verifier checks:
    /// it passes with the right issuer/audience/nonce and is refused on any
    /// mismatch, so test mode is never an accept-any bypass.
    #[test]
    fn dev_minted_tokens_take_the_normal_id_token_checks() {
        let now = chrono::Utc::now().timestamp();
        let jwks = dev_jwks_json().expect("dev JWKS");
        let expect = |nonce, audiences: &[&str]| IdTokenExpectations {
            issuers: vec![DEV_TEST_ISSUER.to_string()],
            audiences: audiences.iter().map(ToString::to_string).collect(),
            client_ids: vec!["bench-client".to_string()],
            nonce,
            now_unix: now,
        };
        let bound = dev_mint_id_token(
            "bench-client",
            "alice",
            "alice@example.test",
            Some("n-1"),
            now,
        )
        .expect("mint");
        let claims =
            verify_id_token_with_jwks(&jwks, &bound, &expect(Some("n-1"), &["bench-client"]))
                .expect("a minted token verifies");
        assert_eq!(claims["sub"], "alice");
        assert_eq!(claims["email"], "alice@example.test");
        assert_eq!(claims["email_verified"], true);
        assert_eq!(
            verify_id_token_with_jwks(&jwks, &bound, &expect(Some("n-2"), &["bench-client"])),
            Err(IdTokenRejection::NonceMismatch)
        );
        assert!(matches!(
            verify_id_token_with_jwks(&jwks, &bound, &expect(Some("n-1"), &["other-client"])),
            Err(IdTokenRejection::Invalid(_))
        ));
        let unbound =
            dev_mint_id_token("bench-client", "bob", "", None, now).expect("mint unbound");
        assert_ne!(
            unbound,
            dev_mint_id_token("bench-client", "bob", "", None, now).expect("mint again"),
            "every minted token is distinct (single-use replay ledger)"
        );
        assert!(
            verify_id_token_with_jwks(&jwks, &unbound, &expect(None, &["bench-client"])).is_ok()
        );
        // The dev key is not any other provider's key.
        let foreign = r#"{"keys":[{"kty":"RSA","kid":"udb-oidc-test-1","n":"AQAB","e":"AQAB"}]}"#;
        assert!(
            verify_id_token_with_jwks(foreign, &bound, &expect(Some("n-1"), &["bench-client"]))
                .is_err()
        );
    }

    #[test]
    fn sealed_secret_without_key_is_refused() {
        assert!(unseal_client_secret(None, "udb-aead:v1:abc").is_err());
        assert_eq!(unseal_client_secret(None, "plain").unwrap(), "plain");
        assert_eq!(unseal_client_secret(None, "").unwrap(), "");
    }
}
