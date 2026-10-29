//! 0.6.6 OIDC sign-in: "Sign in with Google / Microsoft / any OpenID provider"
//! through a tenant `IdentityProvider` of kind OIDC.
//!
//! * `StartOidcLogin` — authorization code + PKCE (S256) + nonce. The state,
//!   nonce and (sealed) PKCE verifier are persisted in
//!   `udb_authn.oidc_login_states` for 10 minutes; only a keyed hash of the
//!   state is stored. The redirect URI must exactly match the provider's
//!   allowlist.
//! * `FinishOidcLogin` — consumes the state row exactly once, redeems the code
//!   at the provider's token endpoint (client secret unsealed from the
//!   provider row), verifies the ID token (provider JWKS, issuer, audience,
//!   expiry, nonce), resolves the user with the ResolveExternalIdentity logic
//!   and signs them in through `complete_sign_in` ("oidc").
//! * `LoginWithOidcIdToken` — the same for an ID token a native app obtained
//!   itself (Google Sign-In); each token signs in at most once.
//!
//! Provider config, discovery, the SSRF-pinned HTTP client, ID-token checks
//! and the external-identity resolution live in the IdP plane
//! (`idp::oidc_signin`); this module owns only the sign-in state machine.
//! For invitations it provides [`AuthnServiceImpl::verify_oidc_id_token_for_tenant`],
//! [`AuthnServiceImpl::start_oidc_authorization`] (with an invitation id) and
//! [`AuthnServiceImpl::redeem_oidc_invitation_code`].

use super::*;

use super::super::idp::oidc_signin::{
    self, IdTokenRejection, OidcSignInProvider, TokenExchangeError,
};
use super::login::SignIn;

/// Manifest key `native_model` resolves the state table from.
const OIDC_LOGIN_STATE_MSG: &str = "udb.core.authn.entity.v1.OidcLoginState";

/// How long a started sign-in can be finished.
pub(super) const OIDC_LOGIN_STATE_TTL_SECS: u64 = 10 * 60;

/// Scopes requested from the provider: an ID token with the email and name
/// claims the claim mapping and JIT policy read.
const OIDC_SCOPES: &str = "openid email profile";

const MAX_STATE_LEN: usize = 256;
const MAX_CODE_LEN: usize = 4096;
const MAX_ID_TOKEN_LEN: usize = 16 * 1024;
const MAX_LOGIN_HINT_LEN: usize = 256;

/// A verified OIDC identity, as invitation acceptance consumes it.
/// `subject` / `email` / `email_verified` are the provider's claim mapping
/// applied to the verified claims (the same values its external-identity link
/// is keyed on); `claims_json` is the full verified claim set.
#[derive(Debug, Clone)]
#[allow(dead_code)] // `claims_json` is for callers that need more than the mapped fields
pub(super) struct VerifiedOidcIdentity {
    pub provider_id: String,
    pub subject: String,
    pub email: String,
    pub email_verified: bool,
    pub claims_json: String,
}

/// What a started authorization hands back.
#[derive(Debug, Clone)]
pub(super) struct OidcAuthorizationStart {
    pub authorization_url: String,
    pub state: String,
    pub expires_at_unix: i64,
}

/// One consumed `oidc_login_states` row.
struct OidcLoginStateRecord {
    tenant_id: String,
    provider_id: String,
    redirect_uri: String,
    nonce: String,
    code_verifier: String,
}

/// Device/audit fields every sign-in path forwards to `complete_sign_in`.
struct OidcSignInDevice<'a> {
    device_name: &'a str,
    device_id: &'a str,
    ip_address: &'a str,
    user_agent: &'a str,
}

// ── status helpers ───────────────────────────────────────────────────────────

/// Unknown, disabled, non-OIDC or client-less provider: one answer for all.
fn oidc_provider_unavailable_status() -> Status {
    crate::runtime::executor_utils::policy_status(
        "oidc_login",
        "oidc_provider_unavailable",
        "OIDC sign-in is not available for this tenant and provider",
    )
}

fn oidc_redirect_uri_not_allowed_status() -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        "redirect_uri is not registered for this identity provider",
        [(
            "redirect_uri",
            "must exactly match one of the provider's redirect_uris",
        )],
    )
}

fn oidc_required_field_status(field: &'static str, description: &'static str) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("{field} is required"),
        [(field, description)],
    )
}

fn oidc_field_too_long_status(field: &'static str, max: usize) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        format!("{field} is too long"),
        [(field, format!("must be at most {max} characters"))],
    )
}

/// Expired, unknown, already used, or bound to an invitation: one answer.
fn oidc_login_state_invalid_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_oidc_state",
        "the sign-in expired or was already completed; start it again",
    )
}

fn oidc_invalid_credential_status() -> Status {
    crate::runtime::executor_utils::unauthenticated_status(
        "invalid_credential",
        "invalid credential",
    )
}

fn oidc_provider_unreachable_status(detail: impl std::fmt::Display) -> Status {
    crate::runtime::executor_utils::capability_status_with_code(
        tonic::Code::Unavailable,
        "authn",
        "oidc_login",
        "oidc_provider_reachable",
        format!("the identity provider could not be used: {detail}"),
    )
}

fn oidc_login_user_not_active_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "oidc_login",
        "user_not_active",
        "user is not active",
    )
}

fn oidc_login_account_kind_status() -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        "oidc_login",
        "oidc_login_person_accounts_only",
        "only person accounts sign in through an identity provider",
    )
}

fn oidc_internal_status(operation: &'static str, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authn", operation, message)
}

fn oidc_store_status(operation: &'static str, err: sqlx::Error) -> Status {
    crate::runtime::executor_utils::sqlx_error_to_status(operation, &err)
}

/// Map an ID-token refusal to the caller-facing status. Every token problem is
/// the same opaque `invalid credential`; only an unreachable key endpoint is
/// reported as such. The reason is logged, never returned.
fn id_token_rejection_status(rejection: IdTokenRejection) -> Status {
    match rejection {
        IdTokenRejection::KeysUnavailable(detail) => oidc_provider_unreachable_status(detail),
        other => {
            tracing::info!(reason = ?other, "OIDC ID token refused");
            oidc_invalid_credential_status()
        }
    }
}

// ── pure helpers ─────────────────────────────────────────────────────────────

/// 32 CSPRNG bytes, base64url without padding (43 characters). Used for the
/// state, the nonce and the PKCE verifier (RFC 7636 4.1 allows 43..128
/// characters of `[A-Za-z0-9-._~]`).
pub(super) fn random_url_token() -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(crate::runtime::executor_utils::random_32_bytes())
}

/// RFC 7636 4.2 `S256`: `BASE64URL(SHA256(ASCII(code_verifier)))`.
pub(super) fn pkce_challenge_s256(code_verifier: &str) -> String {
    use base64::Engine as _;
    use sha2::{Digest, Sha256};
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(code_verifier.as_bytes()))
}

/// The authorization request URL (OpenID Connect Core 3.1.2.1 + RFC 7636).
pub(super) fn build_authorization_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    state: &str,
    nonce: &str,
    code_challenge: &str,
    login_hint: &str,
) -> String {
    let mut params: Vec<(&str, &str)> = vec![
        ("response_type", "code"),
        ("client_id", client_id),
        ("redirect_uri", redirect_uri),
        ("scope", OIDC_SCOPES),
        ("state", state),
        ("nonce", nonce),
        ("code_challenge", code_challenge),
        ("code_challenge_method", "S256"),
    ];
    if !login_hint.is_empty() {
        params.push(("login_hint", login_hint));
    }
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let separator = if authorization_endpoint.contains('?') {
        '&'
    } else {
        '?'
    };
    format!("{authorization_endpoint}{separator}{query}")
}

/// The browser is only ever sent to an https authorization endpoint (test
/// builds may allow the loopback fake provider).
fn authorization_endpoint_allowed(url: &str) -> bool {
    if url
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
    {
        return true;
    }
    #[cfg(all(feature = "http-client", test))]
    if oidc_signin::ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST.load(std::sync::atomic::Ordering::SeqCst)
        && url.starts_with("http://127.0.0.1:")
    {
        return true;
    }
    false
}

/// Copy the shared sign-in credentials (LoginResponse fields 1-11).
macro_rules! oidc_login_response {
    ($ty:ident, $login:expr, $provisioned:expr) => {{
        let login: authn_pb::LoginResponse = $login;
        authn_pb::$ty {
            user_id: login.user_id,
            session_id: login.session_id,
            access_token: login.access_token,
            refresh_token: login.refresh_token,
            access_token_expires_in: login.access_token_expires_in,
            session_token: login.session_token,
            csrf_token: login.csrf_token,
            mfa_required: login.mfa_required,
            mfa_otp_id: login.mfa_otp_id,
            refresh_token_expires_in: login.refresh_token_expires_in,
            session_public_id: login.session_public_id,
            provisioned: $provisioned,
        }
    }};
}

impl AuthnServiceImpl {
    // ── RPC handlers ─────────────────────────────────────────────────────────

    pub(super) async fn start_oidc_login_impl(
        &self,
        request: Request<authn_pb::StartOidcLoginRequest>,
    ) -> Result<Response<authn_pb::StartOidcLoginResponse>, Status> {
        let req = request.into_inner();
        let started = self
            .start_oidc_authorization(
                req.tenant_hint.trim(),
                req.provider_id.trim(),
                &req.redirect_uri,
                req.invitation_id.trim(),
                req.login_hint.trim(),
            )
            .await?;
        Ok(Response::new(authn_pb::StartOidcLoginResponse {
            authorization_url: started.authorization_url,
            state: started.state,
            expires_at_unix: started.expires_at_unix,
        }))
    }

    pub(super) async fn finish_oidc_login_impl(
        &self,
        request: Request<authn_pb::FinishOidcLoginRequest>,
    ) -> Result<Response<authn_pb::FinishOidcLoginResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let (provider, claims) = self
            .redeem_oidc_code(req.state.trim(), req.code.trim(), "", now)
            .await?;
        let (login, provisioned) = self
            .complete_oidc_sign_in(
                &provider,
                &claims,
                OidcSignInDevice {
                    device_name: &req.device_name,
                    device_id: &req.device_id,
                    ip_address: &req.ip_address,
                    user_agent: &req.user_agent,
                },
                "oidc_code_exchanged",
                now,
            )
            .await?;
        Ok(Response::new(oidc_login_response!(
            FinishOidcLoginResponse,
            login,
            provisioned
        )))
    }

    pub(super) async fn login_with_oidc_id_token_impl(
        &self,
        request: Request<authn_pb::LoginWithOidcIdTokenRequest>,
    ) -> Result<Response<authn_pb::LoginWithOidcIdTokenResponse>, Status> {
        let req = request.into_inner();
        let now = now_unix();
        let id_token = req.id_token.trim();
        if id_token.is_empty() {
            return Err(oidc_required_field_status(
                "id_token",
                "must carry the ID token the provider issued to the app",
            ));
        }
        if id_token.len() > MAX_ID_TOKEN_LEN {
            return Err(oidc_field_too_long_status("id_token", MAX_ID_TOKEN_LEN));
        }
        let pool = self.require_pool()?;
        let provider = self
            .load_oidc_provider(pool, req.tenant_hint.trim(), req.provider_id.trim())
            .await?;
        let nonce = req.nonce.trim();
        let id_token = oidc_signin::presented_id_token(
            &provider,
            id_token,
            (!nonce.is_empty()).then_some(nonce),
            now as i64,
        )
        .map_err(id_token_rejection_status)?;
        let id_token = id_token.as_str();
        let claims = oidc_signin::verify_id_token(
            &provider,
            id_token,
            (!nonce.is_empty()).then_some(nonce),
            now as i64,
        )
        .await
        .map_err(id_token_rejection_status)?;
        // Each ID token signs in once: a replay of a captured token (no
        // server-held nonce protects a native token) is refused.
        let expires_at_unix = claims
            .get("exp")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(now as i64 + OIDC_LOGIN_STATE_TTL_SECS as i64);
        if !self
            .record_id_token_use(pool, &provider, id_token, nonce, expires_at_unix)
            .await?
        {
            tracing::info!(
                provider_id = %provider.provider_id,
                "OIDC ID token replay refused"
            );
            return Err(oidc_invalid_credential_status());
        }
        let (login, provisioned) = self
            .complete_oidc_sign_in(
                &provider,
                &claims,
                OidcSignInDevice {
                    device_name: &req.device_name,
                    device_id: &req.device_id,
                    ip_address: &req.ip_address,
                    user_agent: &req.user_agent,
                },
                "oidc_id_token_verified",
                now,
            )
            .await?;
        Ok(Response::new(oidc_login_response!(
            LoginWithOidcIdTokenResponse,
            login,
            provisioned
        )))
    }

    // ── functions other areas call ───────────────────────────────────────────

    /// Begin an authorization-code sign-in with `provider_id` of `tenant_id`:
    /// check the redirect URI against the provider allowlist, persist a fresh
    /// state / nonce / PKCE verifier for [`OIDC_LOGIN_STATE_TTL_SECS`], and
    /// return the provider authorization URL. A non-empty `invitation_id` binds
    /// the state to that invitation: only
    /// [`Self::redeem_oidc_invitation_code`] can then redeem it.
    pub(super) async fn start_oidc_authorization(
        &self,
        tenant_id: &str,
        provider_id: &str,
        redirect_uri: &str,
        invitation_id: &str,
        login_hint: &str,
    ) -> Result<OidcAuthorizationStart, Status> {
        if tenant_id.is_empty() {
            return Err(oidc_required_field_status(
                "tenant_hint",
                "must name the tenant whose identity provider to use",
            ));
        }
        if provider_id.is_empty() {
            return Err(oidc_required_field_status(
                "provider_id",
                "must name the tenant's OIDC identity provider",
            ));
        }
        if redirect_uri.is_empty() {
            return Err(oidc_required_field_status(
                "redirect_uri",
                "must be one of the provider's registered redirect_uris",
            ));
        }
        if login_hint.chars().count() > MAX_LOGIN_HINT_LEN {
            return Err(oidc_field_too_long_status("login_hint", MAX_LOGIN_HINT_LEN));
        }
        if invitation_id.len() > 64 {
            return Err(oidc_field_too_long_status("invitation_id", 64));
        }
        let pool = self.require_pool()?;
        let provider = self
            .load_oidc_provider(pool, tenant_id, provider_id)
            .await?;
        if !provider.redirect_uri_allowed(redirect_uri) {
            return Err(oidc_redirect_uri_not_allowed_status());
        }
        let discovery = oidc_signin::discovery(&provider)
            .await
            .map_err(oidc_provider_unreachable_status)?;
        if !authorization_endpoint_allowed(&discovery.authorization_endpoint) {
            return Err(oidc_provider_unreachable_status(
                "the provider's authorization_endpoint is not https",
            ));
        }
        let state = random_url_token();
        let nonce = random_url_token();
        let code_verifier = random_url_token();
        let expires_at_unix = (now_unix() + OIDC_LOGIN_STATE_TTL_SECS) as i64;
        self.insert_oidc_login_state(
            pool,
            &self.oidc_state_hash(&state),
            &provider,
            redirect_uri,
            &nonce,
            &self.seal_pkce_verifier(&code_verifier)?,
            invitation_id,
            expires_at_unix,
        )
        .await?;
        Ok(OidcAuthorizationStart {
            authorization_url: build_authorization_url(
                &discovery.authorization_endpoint,
                provider.primary_client_id(),
                redirect_uri,
                &state,
                &nonce,
                &pkce_challenge_s256(&code_verifier),
                login_hint,
            ),
            state,
            expires_at_unix,
        })
    }

    /// Redeem the provider callback (`state`, `code`) of a sign-in started for
    /// `invitation_id` and return the verified identity plus its tenant. The
    /// state is single use and must have been started for exactly this
    /// invitation. Invitation acceptance calls this for the OIDC channel when
    /// the browser comes back from the provider.
    pub(super) async fn redeem_oidc_invitation_code(
        &self,
        invitation_id: &str,
        state: &str,
        code: &str,
    ) -> Result<(String, VerifiedOidcIdentity), Status> {
        if invitation_id.trim().is_empty() {
            return Err(oidc_required_field_status(
                "invitation_id",
                "must name the invitation the sign-in was started for",
            ));
        }
        let (provider, claims) = self
            .redeem_oidc_code(state.trim(), code.trim(), invitation_id.trim(), now_unix())
            .await?;
        let identity = verified_identity(&provider, &claims);
        Ok((provider.tenant_id.clone(), identity))
    }

    /// Verify an ID token issued by `provider_id` of `tenant_id` and return
    /// the identity it proves. With a non-empty `expected_nonce` the token's
    /// nonce must equal it; with an empty one the token must carry no nonce and
    /// be at most 10 minutes old. Unknown/disabled providers, bad signatures,
    /// wrong issuer/audience, expiry and nonce failures all return the same
    /// `invalid credential` / provider-unavailable statuses as sign-in.
    pub(super) async fn verify_oidc_id_token_for_tenant(
        &self,
        tenant_id: &str,
        provider_id: &str,
        id_token: &str,
        expected_nonce: &str,
    ) -> Result<VerifiedOidcIdentity, Status> {
        let id_token = id_token.trim();
        if id_token.is_empty() {
            return Err(oidc_required_field_status(
                "oidc_id_token",
                "must carry the ID token the provider issued",
            ));
        }
        if id_token.len() > MAX_ID_TOKEN_LEN {
            return Err(oidc_field_too_long_status(
                "oidc_id_token",
                MAX_ID_TOKEN_LEN,
            ));
        }
        let pool = self.require_pool()?;
        let provider = self
            .load_oidc_provider(pool, tenant_id.trim(), provider_id.trim())
            .await?;
        let nonce = expected_nonce.trim();
        let now = now_unix() as i64;
        let id_token = oidc_signin::presented_id_token(
            &provider,
            id_token,
            (!nonce.is_empty()).then_some(nonce),
            now,
        )
        .map_err(id_token_rejection_status)?;
        let claims = oidc_signin::verify_id_token(
            &provider,
            &id_token,
            (!nonce.is_empty()).then_some(nonce),
            now,
        )
        .await
        .map_err(id_token_rejection_status)?;
        Ok(verified_identity(&provider, &claims))
    }

    // ── internals ────────────────────────────────────────────────────────────

    async fn load_oidc_provider(
        &self,
        pool: &PgPool,
        tenant_id: &str,
        provider_id: &str,
    ) -> Result<OidcSignInProvider, Status> {
        if tenant_id.is_empty() || provider_id.is_empty() {
            return Err(oidc_provider_unavailable_status());
        }
        oidc_signin::load_sign_in_provider(pool, self.runtime.as_deref(), tenant_id, provider_id)
            .await?
            .ok_or_else(oidc_provider_unavailable_status)
    }

    /// Consume the state row bound to `invitation_id` ('' = plain sign-in),
    /// redeem the code and verify the ID token against the stored nonce.
    async fn redeem_oidc_code(
        &self,
        state: &str,
        code: &str,
        invitation_id: &str,
        now: u64,
    ) -> Result<(OidcSignInProvider, serde_json::Value), Status> {
        if state.is_empty() {
            return Err(oidc_required_field_status(
                "state",
                "must be the state the provider returned to the redirect URI",
            ));
        }
        if code.is_empty() {
            return Err(oidc_required_field_status(
                "code",
                "must be the authorization code the provider returned to the redirect URI",
            ));
        }
        if state.len() > MAX_STATE_LEN {
            return Err(oidc_login_state_invalid_status());
        }
        if code.len() > MAX_CODE_LEN {
            return Err(oidc_field_too_long_status("code", MAX_CODE_LEN));
        }
        let pool = self.require_pool()?;
        let record = self
            .consume_oidc_login_state(pool, &self.oidc_state_hash(state), invitation_id)
            .await?
            .ok_or_else(oidc_login_state_invalid_status)?;
        let provider = self
            .load_oidc_provider(pool, &record.tenant_id, &record.provider_id)
            .await?;
        // The allowlist may have changed since the sign-in started.
        if !provider.redirect_uri_allowed(&record.redirect_uri) {
            return Err(oidc_redirect_uri_not_allowed_status());
        }
        let discovery = oidc_signin::discovery(&provider)
            .await
            .map_err(oidc_provider_unreachable_status)?;
        let code_verifier = self.unseal_pkce_verifier(&record.code_verifier)?;
        // The dev OpenID provider (UDB_OIDC_TEST_MODE, never in production)
        // answers the token request in-process, minting a real signed ID token
        // bound to the stored nonce; every other provider is called over https.
        let exchanged =
            match oidc_signin::dev_exchange_code(&provider, code, &record.nonce, now as i64) {
                Some(minted) => minted,
                None => {
                    oidc_signin::exchange_code(
                        &provider,
                        &discovery,
                        code,
                        &record.redirect_uri,
                        &code_verifier,
                    )
                    .await
                }
            };
        let id_token = exchanged.map_err(|err| match err {
            TokenExchangeError::Rejected(error) => {
                tracing::info!(
                    provider_id = %provider.provider_id,
                    oauth_error = %error,
                    "OIDC authorization code refused by the provider"
                );
                oidc_invalid_credential_status()
            }
            TokenExchangeError::Unavailable(detail) => oidc_provider_unreachable_status(detail),
        })?;
        let claims =
            oidc_signin::verify_id_token(&provider, &id_token, Some(&record.nonce), now as i64)
                .await
                .map_err(id_token_rejection_status)?;
        Ok((provider, claims))
    }

    /// Resolve verified claims to the UDB user (existing link, account-linking
    /// policy, JIT) and issue the Login credentials. Returns the response and
    /// whether the account was just provisioned.
    async fn complete_oidc_sign_in(
        &self,
        provider: &OidcSignInProvider,
        claims: &serde_json::Value,
        device: OidcSignInDevice<'_>,
        audit_reason_code: &'static str,
        now: u64,
    ) -> Result<(authn_pb::LoginResponse, bool), Status> {
        let pool = self.require_pool()?;
        let federated =
            oidc_signin::resolve_user(pool, self.event_sink.clone(), provider, claims).await?;
        if !federated.bound_roles.is_empty() {
            self.reload_oidc_role_bindings().await;
        }
        let mut user = self
            .users
            .get_user_by_id_in_tenant(&federated.user_id, &provider.tenant_id)
            .await
            .map_err(|err| oidc_internal_status("oidc_login_user_lookup", err))?
            .ok_or_else(|| {
                oidc_internal_status(
                    "oidc_login_user_lookup",
                    "the resolved external identity names no user in this tenant",
                )
            })?;
        // A workload credential never signs in through a browser identity
        // provider, even when an email link points at one.
        if !matches!(
            user.account_kind,
            AccountKind::Person | AccountKind::ExternalIdentity | AccountKind::Unspecified
        ) {
            return Err(oidc_login_account_kind_status());
        }
        // Suspended, locked, deactivated and invite-pending accounts are refused
        // exactly as password Login refuses them.
        if user.locked_until_unix > now
            || (user.status != AccountStatus::Active
                && user.status != AccountStatus::PendingVerification)
        {
            self.metrics.record_auth_login(false);
            return Err(oidc_login_user_not_active_status());
        }
        let prior = user.clone();
        user.last_login_at_unix = now;
        user.updated_at_unix = now;
        self.users
            .put_user(user.clone())
            .await
            .map_err(|err| oidc_internal_status("oidc_login_success_update", err))?;
        let login = self
            .complete_sign_in(
                user,
                SignIn {
                    device_name: device.device_name,
                    ip_address: device.ip_address,
                    user_agent: device.user_agent,
                    device_id: device.device_id,
                    // Person accounts take their grants from role bindings;
                    // requested scopes only narrow service accounts, which
                    // never reach this path.
                    requested_scopes: &[],
                    token_auth_method: "oidc",
                    audit_auth_method: "oidc",
                    audit_reason_code,
                    restore_on_failure: Some(prior),
                },
                now,
            )
            .await?;
        Ok((login, federated.provisioned))
    }

    /// Reload the shared authz snapshot after the IdP mapping bound new roles,
    /// so the token this sign-in issues already carries them (other nodes
    /// reload on the appended authz revision).
    async fn reload_oidc_role_bindings(&self) {
        let (Some(cell), Some(pool)) = (self.authz_snapshot.as_ref(), self.pg_pool.as_ref()) else {
            return;
        };
        super::super::AuthzServiceImpl::shared(cell.clone())
            .with_postgres(Some(pool.clone()))
            .warm_shared_snapshot()
            .await;
    }

    fn oidc_state_hash(&self, state: &str) -> String {
        authn::hash_secret(&format!("oidc_state:{state}"), &self.hash_key())
    }

    fn seal_pkce_verifier(&self, verifier: &str) -> Result<String, Status> {
        match self.runtime.as_deref() {
            Some(runtime) => runtime.encrypt_secret_at_rest(verifier).map_err(|err| {
                oidc_internal_status(
                    "oidc_pkce_verifier_seal",
                    format!("sealing the PKCE verifier failed: {err}"),
                )
            }),
            None => Ok(verifier.to_string()),
        }
    }

    fn unseal_pkce_verifier(&self, stored: &str) -> Result<String, Status> {
        let plaintext = match self.runtime.as_deref() {
            Some(runtime) => runtime.decrypt_secret_at_rest(stored).map_err(|err| {
                oidc_internal_status(
                    "oidc_pkce_verifier_unseal",
                    format!("unsealing the PKCE verifier failed: {err}"),
                )
            })?,
            None => stored.to_string(),
        };
        if stored.starts_with("udb-aead:") && plaintext == stored {
            return Err(oidc_internal_status(
                "oidc_pkce_verifier_unseal",
                "the PKCE verifier is sealed but no matching key is available",
            ));
        }
        Ok(plaintext)
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_oidc_login_state(
        &self,
        pool: &PgPool,
        state_hash: &str,
        provider: &OidcSignInProvider,
        redirect_uri: &str,
        nonce: &str,
        sealed_verifier: &str,
        invitation_id: &str,
        expires_at_unix: i64,
    ) -> Result<(), Status> {
        let m = crate::runtime::native_catalog::native_model(
            OIDC_LOGIN_STATE_MSG,
            &[
                "oidc_login_state_id",
                "state_hash",
                "tenant_id",
                "provider_id",
                "redirect_uri",
                "nonce",
                "code_verifier",
                "invitation_id",
                "expires_at",
            ],
        );
        self.purge_expired_oidc_login_states(pool).await;
        let sql = format!(
            "INSERT INTO {rel} ({id}, {hash}, {tenant}, {pid}, {redirect}, {nonce}, {verifier}, \
                {inv}, {expires}) \
             VALUES (gen_random_uuid(), $1, $2, $3::UUID, $4, $5, $6, $7, \
                to_timestamp($8::DOUBLE PRECISION))",
            rel = m.relation,
            id = m.q("oidc_login_state_id"),
            hash = m.q("state_hash"),
            tenant = m.q("tenant_id"),
            pid = m.q("provider_id"),
            redirect = m.q("redirect_uri"),
            nonce = m.q("nonce"),
            verifier = m.q("code_verifier"),
            inv = m.q("invitation_id"),
            expires = m.q("expires_at"),
        );
        sqlx::query(&sql)
            .bind(state_hash)
            .bind(&provider.tenant_id)
            .bind(&provider.provider_id)
            .bind(redirect_uri)
            .bind(nonce)
            .bind(sealed_verifier)
            .bind(invitation_id)
            .bind(expires_at_unix as f64)
            .execute(pool)
            .await
            .map_err(|err| oidc_store_status("oidc login state insert failed", err))?;
        Ok(())
    }

    /// Atomically mark the unexpired, unconsumed row for `state_hash` (bound
    /// to exactly `invitation_id`) consumed and return it. `None` for every
    /// miss, so a replayed, expired or cross-flow state is indistinguishable.
    async fn consume_oidc_login_state(
        &self,
        pool: &PgPool,
        state_hash: &str,
        invitation_id: &str,
    ) -> Result<Option<OidcLoginStateRecord>, Status> {
        use sqlx::Row as _;
        let m = crate::runtime::native_catalog::native_model(
            OIDC_LOGIN_STATE_MSG,
            &[
                "state_hash",
                "tenant_id",
                "provider_id",
                "redirect_uri",
                "nonce",
                "code_verifier",
                "invitation_id",
                "expires_at",
                "consumed_at",
            ],
        );
        let sql = format!(
            "UPDATE {rel} SET {consumed} = now() \
             WHERE {hash} = $1 AND {inv} = $2 AND {consumed} IS NULL AND {expires} > now() \
             RETURNING {tenant}::TEXT AS tenant_id, {pid}::TEXT AS provider_id, \
                {redirect}::TEXT AS redirect_uri, {nonce}::TEXT AS nonce, \
                {verifier}::TEXT AS code_verifier",
            rel = m.relation,
            consumed = m.q("consumed_at"),
            hash = m.q("state_hash"),
            inv = m.q("invitation_id"),
            expires = m.q("expires_at"),
            tenant = m.q("tenant_id"),
            pid = m.q("provider_id"),
            redirect = m.q("redirect_uri"),
            nonce = m.q("nonce"),
            verifier = m.q("code_verifier"),
        );
        let row = sqlx::query(&sql)
            .bind(state_hash)
            .bind(invitation_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| oidc_store_status("oidc login state consume failed", err))?;
        Ok(row.map(|row| OidcLoginStateRecord {
            tenant_id: row.try_get("tenant_id").unwrap_or_default(),
            provider_id: row.try_get("provider_id").unwrap_or_default(),
            redirect_uri: row.try_get("redirect_uri").unwrap_or_default(),
            nonce: row.try_get("nonce").unwrap_or_default(),
            code_verifier: row.try_get("code_verifier").unwrap_or_default(),
        }))
    }

    /// Record that `id_token` was used to sign in; `false` when it already
    /// was. The row is keyed by a keyed hash of the token and kept until the
    /// token itself expires.
    async fn record_id_token_use(
        &self,
        pool: &PgPool,
        provider: &OidcSignInProvider,
        id_token: &str,
        nonce: &str,
        expires_at_unix: i64,
    ) -> Result<bool, Status> {
        let m = crate::runtime::native_catalog::native_model(
            OIDC_LOGIN_STATE_MSG,
            &[
                "oidc_login_state_id",
                "state_hash",
                "tenant_id",
                "provider_id",
                "redirect_uri",
                "nonce",
                "code_verifier",
                "expires_at",
                "consumed_at",
            ],
        );
        self.purge_expired_oidc_login_states(pool).await;
        let sql = format!(
            "INSERT INTO {rel} ({id}, {hash}, {tenant}, {pid}, {redirect}, {nonce}, {verifier}, \
                {expires}, {consumed}) \
             VALUES (gen_random_uuid(), $1, $2, $3::UUID, '', $4, '', \
                to_timestamp($5::DOUBLE PRECISION), now()) \
             ON CONFLICT ({hash}) DO NOTHING \
             RETURNING {id}::TEXT AS oidc_login_state_id",
            rel = m.relation,
            id = m.q("oidc_login_state_id"),
            hash = m.q("state_hash"),
            tenant = m.q("tenant_id"),
            pid = m.q("provider_id"),
            redirect = m.q("redirect_uri"),
            nonce = m.q("nonce"),
            verifier = m.q("code_verifier"),
            expires = m.q("expires_at"),
            consumed = m.q("consumed_at"),
        );
        let token_hash = authn::hash_secret(&format!("oidc_id_token:{id_token}"), &self.hash_key());
        let inserted = sqlx::query(&sql)
            .bind(token_hash)
            .bind(&provider.tenant_id)
            .bind(&provider.provider_id)
            .bind(nonce)
            .bind(expires_at_unix as f64)
            .fetch_optional(pool)
            .await
            .map_err(|err| oidc_store_status("oidc id token use record failed", err))?;
        Ok(inserted.is_some())
    }

    /// Best-effort removal of rows past their lifetime (plus a margin larger
    /// than the ID-token clock skew, so a replay guard row outlives its token).
    async fn purge_expired_oidc_login_states(&self, pool: &PgPool) {
        let m = crate::runtime::native_catalog::native_model(OIDC_LOGIN_STATE_MSG, &["expires_at"]);
        let sql = format!(
            "DELETE FROM {rel} WHERE {expires} < now() - interval '5 minutes'",
            rel = m.relation,
            expires = m.q("expires_at"),
        );
        if let Err(err) = sqlx::query(&sql).execute(pool).await {
            tracing::warn!(error = %err, "purging expired OIDC login states failed");
        }
    }
}

/// The invitation-facing view of verified claims.
fn verified_identity(
    provider: &OidcSignInProvider,
    claims: &serde_json::Value,
) -> VerifiedOidcIdentity {
    let (subject, email, email_verified) = provider.mapped_identity(claims);
    VerifiedOidcIdentity {
        provider_id: provider.provider_id.clone(),
        subject,
        email,
        email_verified,
        claims_json: claims.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::idp::oidc_signin::{
        IdTokenExpectations, MAX_UNBOUND_ID_TOKEN_AGE_SECS, verify_id_token_with_jwks,
    };
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    const PRIVATE_PEM: &str = include_str!("../../../testdata/jwt_rs256_private.pem");
    const PUBLIC_PEM: &str = include_str!("../../../testdata/jwt_rs256_public.pem");
    const ISSUER: &str = "https://idp.example.test";
    const CLIENT: &str = "client-web";
    const KID: &str = "oidc-test-kid";

    fn jwks() -> String {
        let jwk = super::super::tokens::rsa_jwk_value_from_pem(PUBLIC_PEM, KID, "RS256")
            .expect("test public key yields a JWK");
        serde_json::json!({ "keys": [jwk] }).to_string()
    }

    fn now() -> i64 {
        now_unix() as i64
    }

    fn sign(claims: serde_json::Value) -> String {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(PRIVATE_PEM.as_bytes()).expect("test key"),
        )
        .expect("sign test token")
    }

    fn claims(nonce: Option<&str>) -> serde_json::Value {
        let mut claims = serde_json::json!({
            "iss": ISSUER,
            "aud": CLIENT,
            "sub": "subject-1",
            "email": "person@example.test",
            "email_verified": true,
            "iat": now(),
            "exp": now() + 300,
        });
        if let Some(nonce) = nonce {
            claims["nonce"] = serde_json::Value::String(nonce.to_string());
        }
        claims
    }

    fn expect(nonce: Option<&str>) -> IdTokenExpectations<'_> {
        IdTokenExpectations {
            issuers: vec![ISSUER.to_string()],
            audiences: vec![CLIENT.to_string(), "client-android".to_string()],
            client_ids: vec![CLIENT.to_string(), "client-android".to_string()],
            nonce,
            now_unix: now(),
        }
    }

    #[test]
    fn pkce_s256_matches_rfc7636_appendix_b() {
        assert_eq!(
            pkce_challenge_s256("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn random_tokens_are_valid_pkce_verifiers_and_unique() {
        let a = random_url_token();
        let b = random_url_token();
        assert_eq!(a.len(), 43);
        assert_ne!(a, b);
        assert!(
            a.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        );
    }

    #[test]
    fn authorization_url_carries_pkce_nonce_state_and_encodes_values() {
        let url = build_authorization_url(
            "https://accounts.google.com/o/oauth2/v2/auth",
            "123.apps.googleusercontent.com",
            "https://app.example.com/cb?x=1",
            "st",
            "nn",
            "ch",
            "a b@example.com",
        );
        assert!(
            url.starts_with("https://accounts.google.com/o/oauth2/v2/auth?response_type=code&")
        );
        for part in [
            "client_id=123.apps.googleusercontent.com",
            "redirect_uri=https%3A%2F%2Fapp.example.com%2Fcb%3Fx%3D1",
            "scope=openid%20email%20profile",
            "state=st",
            "nonce=nn",
            "code_challenge=ch",
            "code_challenge_method=S256",
            "login_hint=a%20b%40example.com",
        ] {
            assert!(url.contains(part), "{part} missing from {url}");
        }
        let with_query = build_authorization_url(
            "https://idp.example/auth?tenant=x",
            "c",
            "r",
            "s",
            "n",
            "h",
            "",
        );
        assert!(with_query.starts_with("https://idp.example/auth?tenant=x&response_type=code"));
        assert!(!with_query.contains("login_hint"));
    }

    #[test]
    fn authorization_endpoint_must_be_https() {
        assert!(authorization_endpoint_allowed("https://idp.example/auth"));
        assert!(!authorization_endpoint_allowed("http://idp.example/auth"));
        assert!(!authorization_endpoint_allowed("javascript:alert(1)"));
    }

    #[test]
    fn id_token_with_matching_nonce_verifies() {
        let token = sign(claims(Some("n-1")));
        let verified = verify_id_token_with_jwks(&jwks(), &token, &expect(Some("n-1")))
            .expect("valid token verifies");
        assert_eq!(verified["sub"], "subject-1");
        assert_eq!(verified["email"], "person@example.test");
    }

    #[test]
    fn id_token_nonce_rules() {
        let bound = sign(claims(Some("n-1")));
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &bound, &expect(Some("other"))),
            Err(IdTokenRejection::NonceMismatch)
        );
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &bound, &expect(None)),
            Err(IdTokenRejection::UnexpectedNonce)
        );
        let unbound = sign(claims(None));
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &unbound, &expect(Some("n-1"))),
            Err(IdTokenRejection::NonceMissing)
        );
        assert!(verify_id_token_with_jwks(&jwks(), &unbound, &expect(None)).is_ok());
        let mut old = claims(None);
        old["iat"] = serde_json::json!(now() - MAX_UNBOUND_ID_TOKEN_AGE_SECS - 120);
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &sign(old), &expect(None)),
            Err(IdTokenRejection::Stale)
        );
    }

    #[test]
    fn id_token_issuer_audience_expiry_and_azp_are_enforced() {
        let mut wrong_iss = claims(Some("n"));
        wrong_iss["iss"] = serde_json::json!("https://evil.example");
        assert!(matches!(
            verify_id_token_with_jwks(&jwks(), &sign(wrong_iss), &expect(Some("n"))),
            Err(IdTokenRejection::Invalid(_))
        ));
        let mut wrong_aud = claims(Some("n"));
        wrong_aud["aud"] = serde_json::json!("someone-else");
        assert!(matches!(
            verify_id_token_with_jwks(&jwks(), &sign(wrong_aud), &expect(Some("n"))),
            Err(IdTokenRejection::Invalid(_))
        ));
        let mut expired = claims(Some("n"));
        expired["exp"] = serde_json::json!(now() - 3600);
        assert!(matches!(
            verify_id_token_with_jwks(&jwks(), &sign(expired), &expect(Some("n"))),
            Err(IdTokenRejection::Invalid(_))
        ));
        let mut multi = claims(Some("n"));
        multi["aud"] = serde_json::json!([CLIENT, "third-party"]);
        multi["azp"] = serde_json::json!("third-party");
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &sign(multi.clone()), &expect(Some("n"))),
            Err(IdTokenRejection::AuthorizedPartyMismatch)
        );
        multi["azp"] = serde_json::json!(CLIENT);
        assert!(verify_id_token_with_jwks(&jwks(), &sign(multi), &expect(Some("n"))).is_ok());
    }

    #[test]
    fn id_token_key_and_algorithm_are_enforced() {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
        header.kid = Some(KID.to_string());
        let hmac = jsonwebtoken::encode(
            &header,
            &claims(Some("n")),
            &jsonwebtoken::EncodingKey::from_secret(PUBLIC_PEM.as_bytes()),
        )
        .expect("sign HS256 token");
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), &hmac, &expect(Some("n"))),
            Err(IdTokenRejection::UnsupportedAlgorithm)
        );
        let other_kid = jwks().replace(KID, "rotated-away");
        assert_eq!(
            verify_id_token_with_jwks(&other_kid, &sign(claims(Some("n"))), &expect(Some("n"))),
            Err(IdTokenRejection::UnknownKey)
        );
        let mut tampered = sign(claims(Some("n")));
        tampered.push('A');
        assert!(matches!(
            verify_id_token_with_jwks(&jwks(), &tampered, &expect(Some("n"))),
            Err(IdTokenRejection::Invalid(_))
        ));
        assert_eq!(
            verify_id_token_with_jwks(&jwks(), "not-a-jwt", &expect(Some("n"))),
            Err(IdTokenRejection::Malformed)
        );
    }

    fn detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present")
            .to_bytes()
            .expect("typed detail trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn refusals_are_opaque_and_typed() {
        let status = id_token_rejection_status(IdTokenRejection::NonceMismatch);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "invalid credential");
        let status = id_token_rejection_status(IdTokenRejection::KeysUnavailable("down".into()));
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(detail(&status).kind, ErrorKind::Capability as i32);
        let status = oidc_redirect_uri_not_allowed_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(detail(&status).kind, ErrorKind::Validation as i32);
        let status = oidc_login_state_invalid_status();
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }
}
