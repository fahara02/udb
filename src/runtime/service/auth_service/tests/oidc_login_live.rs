//! Live Postgres acceptance for OIDC sign-in (StartOidcLogin / FinishOidcLogin
//! / LoginWithOidcIdToken) against a loopback fake OpenID provider that serves
//! a real discovery document, a JWKS built from the RS256 test key, and a token
//! endpoint that checks client authentication, the redirect URI, single-use
//! codes and the PKCE S256 verifier. The provider is a real
//! `udb_idp.identity_providers` row created through IdentityProviderService;
//! users resolve through the JIT / external-identity path.

use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::idp::entity::v1 as idp_entity_pb;
use crate::proto::udb::core::idp::services::v1 as idp_pb;
use crate::proto::udb::core::idp::services::v1::identity_provider_service_server::IdentityProviderService;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use tonic::Request;
use uuid::Uuid;

const PRIVATE_PEM: &str = include_str!("../../../testdata/jwt_rs256_private.pem");
const PUBLIC_PEM: &str = include_str!("../../../testdata/jwt_rs256_public.pem");
const KID: &str = "fake-oidc-kid-1";
const CLIENT_ID: &str = "udb-web-client";
const CLIENT_SECRET: &str = "fake-client-secret";
const REDIRECT_URI: &str = "https://app.example.test/oidc/callback";

/// One authorization the fake provider will honour at its token endpoint.
#[derive(Clone)]
struct CodeGrant {
    redirect_uri: String,
    code_challenge: String,
    nonce: String,
    subject: String,
    email: String,
}

struct FakeProvider {
    issuer: String,
    codes: HashMap<String, CodeGrant>,
    token_requests: usize,
    /// `groups` claim put into issued ID tokens (omitted when empty).
    groups: Vec<String>,
}

impl FakeProvider {
    fn jwks() -> serde_json::Value {
        use base64::Engine as _;
        use rsa::RsaPublicKey;
        use rsa::pkcs8::DecodePublicKey;
        use rsa::traits::PublicKeyParts;
        let key = RsaPublicKey::from_public_key_pem(PUBLIC_PEM).expect("test public key");
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        serde_json::json!({ "keys": [{
            "kty": "RSA", "use": "sig", "alg": "RS256", "kid": KID,
            "n": b64.encode(key.n().to_bytes_be()),
            "e": b64.encode(key.e().to_bytes_be()),
        }]})
    }

    fn id_token(&self, subject: &str, email: &str, nonce: Option<&str>) -> String {
        let now = now_unix() as i64;
        let mut claims = serde_json::json!({
            "iss": self.issuer,
            "aud": CLIENT_ID,
            "sub": subject,
            "email": email,
            "email_verified": true,
            "name": "Oidc Person",
            "iat": now,
            "exp": now + 600,
            // Distinct per token: two sign-ins in one second must not mint
            // byte-identical tokens, which the replay guard rightly refuses.
            "jti": Uuid::new_v4().to_string(),
        });
        if let Some(nonce) = nonce {
            claims["nonce"] = serde_json::Value::String(nonce.to_string());
        }
        if !self.groups.is_empty() {
            claims["groups"] = serde_json::json!(self.groups);
        }
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(KID.to_string());
        jsonwebtoken::encode(
            &header,
            &claims,
            &jsonwebtoken::EncodingKey::from_rsa_pem(PRIVATE_PEM.as_bytes()).expect("test key"),
        )
        .expect("sign fake id token")
    }

    fn handle(
        &mut self,
        method: &str,
        path: &str,
        authorization: &str,
        body: &str,
    ) -> (u16, serde_json::Value) {
        match (method, path) {
            ("GET", "/.well-known/openid-configuration") => (
                200,
                serde_json::json!({
                    "issuer": self.issuer,
                    "authorization_endpoint": format!("{}/authorize", self.issuer),
                    "token_endpoint": format!("{}/token", self.issuer),
                    "jwks_uri": format!("{}/jwks", self.issuer),
                    "token_endpoint_auth_methods_supported": ["client_secret_basic", "client_secret_post"],
                }),
            ),
            ("GET", "/jwks") => (200, Self::jwks()),
            ("POST", "/token") => self.token(authorization, body),
            _ => (404, serde_json::json!({ "error": "not_found" })),
        }
    }

    fn token(&mut self, authorization: &str, body: &str) -> (u16, serde_json::Value) {
        use base64::Engine as _;
        self.token_requests += 1;
        let invalid = |error: &str| (400, serde_json::json!({ "error": error }));
        let expected_basic = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD
                .encode(format!("{CLIENT_ID}:{CLIENT_SECRET}"))
        );
        if authorization != expected_basic {
            return invalid("invalid_client");
        }
        let form: HashMap<String, String> = body
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .map(|(k, v)| {
                let decode = |s: &str| {
                    urlencoding::decode(&s.replace('+', " "))
                        .map(|c| c.into_owned())
                        .unwrap_or_default()
                };
                (decode(k), decode(v))
            })
            .collect();
        if form.get("grant_type").map(String::as_str) != Some("authorization_code") {
            return invalid("unsupported_grant_type");
        }
        // Codes are single use, like a real provider's.
        let Some(grant) = form.get("code").and_then(|code| self.codes.remove(code)) else {
            return invalid("invalid_grant");
        };
        if form.get("redirect_uri") != Some(&grant.redirect_uri) {
            return invalid("invalid_grant");
        }
        let verifier = form.get("code_verifier").cloned().unwrap_or_default();
        let challenge = {
            use sha2::{Digest, Sha256};
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(verifier.as_bytes()))
        };
        if challenge != grant.code_challenge {
            return invalid("invalid_grant");
        }
        (
            200,
            serde_json::json!({
                "access_token": "fake-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "id_token": self.id_token(&grant.subject, &grant.email, Some(&grant.nonce)),
            }),
        )
    }
}

/// Serve the fake provider on a loopback port; returns its issuer URL.
async fn spawn_fake_provider() -> (String, Arc<Mutex<FakeProvider>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake OIDC provider");
    let issuer = format!("http://{}", listener.local_addr().expect("local addr"));
    let provider = Arc::new(Mutex::new(FakeProvider {
        issuer: issuer.clone(),
        codes: HashMap::new(),
        token_requests: 0,
        groups: Vec::new(),
    }));
    let shared = provider.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let provider = shared.clone();
            tokio::spawn(async move {
                let _ = serve_one(&mut socket, provider).await;
            });
        }
    });
    (issuer, provider)
}

async fn serve_one(
    socket: &mut tokio::net::TcpStream,
    provider: Arc<Mutex<FakeProvider>>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let header = |name: &str| {
        head.lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .find(|(key, _)| key.trim().eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_string())
            .unwrap_or_default()
    };
    let content_length = header("content-length").parse::<usize>().unwrap_or(0);
    while buf.len() < header_end + content_length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let body_end = (header_end + content_length).min(buf.len());
    let body = String::from_utf8_lossy(&buf[header_end..body_end]).to_string();
    let mut request_line = head.lines().next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default().to_string();
    let path = request_line.next().unwrap_or_default().to_string();
    let (status, json) =
        provider
            .lock()
            .await
            .handle(&method, &path, &header("authorization"), &body);
    let payload = json.to_string();
    let reason = if status == 200 { "OK" } else { "Error" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await
}

/// Restores the loopback switch even when an assertion fails.
struct LoopbackIdpSwitch;

impl LoopbackIdpSwitch {
    fn on() -> Self {
        super::super::idp::oidc_signin::ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Self
    }
}

impl Drop for LoopbackIdpSwitch {
    fn drop(&mut self) {
        super::super::idp::oidc_signin::ALLOW_LOOPBACK_HTTP_IDP_FOR_TEST
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

fn query_param(url: &str, name: &str) -> String {
    let query = url.split_once('?').map(|(_, q)| q).unwrap_or_default();
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| {
            urlencoding::decode(value)
                .map(|v| v.into_owned())
                .unwrap_or_default()
        })
        .unwrap_or_default()
}

async fn start(
    svc: &super::super::AuthnServiceImpl,
    tenant: &str,
    provider_id: &str,
    invitation_id: &str,
) -> authn_pb::StartOidcLoginResponse {
    svc.start_oidc_login(Request::new(authn_pb::StartOidcLoginRequest {
        tenant_hint: tenant.to_string(),
        provider_id: provider_id.to_string(),
        redirect_uri: REDIRECT_URI.to_string(),
        invitation_id: invitation_id.to_string(),
        login_hint: "person@example.test".to_string(),
    }))
    .await
    .expect("start OIDC sign-in")
    .into_inner()
}

/// Play the browser + provider: authorize `subject` for the started sign-in
/// and return the code the provider would redirect back with.
async fn authorize(
    fake: &Arc<Mutex<FakeProvider>>,
    started: &authn_pb::StartOidcLoginResponse,
    subject: &str,
    nonce_override: Option<&str>,
) -> String {
    let url = &started.authorization_url;
    assert_eq!(query_param(url, "state"), started.state);
    assert_eq!(query_param(url, "redirect_uri"), REDIRECT_URI);
    let code = format!("code-{}", Uuid::new_v4().simple());
    fake.lock().await.codes.insert(
        code.clone(),
        CodeGrant {
            redirect_uri: query_param(url, "redirect_uri"),
            code_challenge: query_param(url, "code_challenge"),
            nonce: nonce_override
                .map(ToString::to_string)
                .unwrap_or_else(|| query_param(url, "nonce")),
            subject: subject.to_string(),
            email: "person@example.test".to_string(),
        },
    );
    code
}

async fn finish(
    svc: &super::super::AuthnServiceImpl,
    state: &str,
    code: &str,
) -> Result<authn_pb::FinishOidcLoginResponse, tonic::Status> {
    svc.finish_oidc_login(Request::new(authn_pb::FinishOidcLoginRequest {
        state: state.to_string(),
        code: code.to_string(),
        device_name: "oidc live test".to_string(),
        ip_address: "203.0.113.7".to_string(),
        user_agent: "oidc-live/1.0".to_string(),
        ..Default::default()
    }))
    .await
    .map(|response| response.into_inner())
}

async fn login_with_id_token(
    svc: &super::super::AuthnServiceImpl,
    tenant: &str,
    provider_id: &str,
    id_token: &str,
    nonce: &str,
) -> Result<authn_pb::LoginWithOidcIdTokenResponse, tonic::Status> {
    svc.login_with_oidc_id_token(Request::new(authn_pb::LoginWithOidcIdTokenRequest {
        tenant_hint: tenant.to_string(),
        provider_id: provider_id.to_string(),
        id_token: id_token.to_string(),
        nonce: nonce.to_string(),
        device_name: "oidc native app".to_string(),
        ..Default::default()
    }))
    .await
    .map(|response| response.into_inner())
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_oidc_sign_in_code_flow_native_id_token_and_refusals() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let _loopback = LoopbackIdpSwitch::on();
    let (issuer, fake) = spawn_fake_provider().await;
    let tenant = Uuid::new_v4().to_string();
    // One keyed runtime seals the client secret (IdP) and unseals it at the
    // token exchange (sign-in), in any posture the test process runs in.
    let keyed = keyed_live_runtime().await;
    let idp = super::super::idp::IdentityProviderServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_runtime(keyed.clone());

    // The redirect allowlist is validated when the provider is configured.
    let refused = idp
        .create_provider(Request::new(idp_pb::CreateProviderRequest {
            tenant_id: tenant.clone(),
            kind: idp_entity_pb::IdpKind::Oidc as i32,
            display_name: "Bad redirect".to_string(),
            issuer: issuer.clone(),
            client_ids: vec![CLIENT_ID.to_string()],
            enabled: true,
            redirect_uris: vec!["http://app.example.test/callback".to_string()],
            ..Default::default()
        }))
        .await
        .expect_err("a cleartext non-loopback redirect URI is refused");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);

    let provider_id = idp
        .create_provider(Request::new(idp_pb::CreateProviderRequest {
            tenant_id: tenant.clone(),
            kind: idp_entity_pb::IdpKind::Oidc as i32,
            display_name: "Fake Google".to_string(),
            issuer: issuer.clone(),
            client_ids: vec![CLIENT_ID.to_string()],
            client_secret: CLIENT_SECRET.to_string(),
            jit_policy_json: r#"{"default_project":"default"}"#.to_string(),
            enabled: true,
            redirect_uris: vec![REDIRECT_URI.to_string()],
            ..Default::default()
        }))
        .await
        .expect("create OIDC provider")
        .into_inner()
        .provider
        .expect("created provider")
        .provider_id;

    let svc = authn_service_with_jwt(pool.clone()).with_runtime(Some(keyed.clone()));

    // ── refusals before any redirect ────────────────────────────────────────
    let not_allowlisted = svc
        .start_oidc_login(Request::new(authn_pb::StartOidcLoginRequest {
            tenant_hint: tenant.clone(),
            provider_id: provider_id.clone(),
            redirect_uri: "https://attacker.example/steal".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a redirect URI outside the allowlist is refused");
    assert_eq!(not_allowlisted.code(), tonic::Code::InvalidArgument);
    let unknown = svc
        .start_oidc_login(Request::new(authn_pb::StartOidcLoginRequest {
            tenant_hint: tenant.clone(),
            provider_id: Uuid::new_v4().to_string(),
            redirect_uri: REDIRECT_URI.to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("an unknown provider is refused");
    assert_eq!(unknown.code(), tonic::Code::FailedPrecondition);
    let other_tenant = svc
        .start_oidc_login(Request::new(authn_pb::StartOidcLoginRequest {
            tenant_hint: Uuid::new_v4().to_string(),
            provider_id: provider_id.clone(),
            redirect_uri: REDIRECT_URI.to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("another tenant cannot use this tenant's provider");
    assert_eq!(other_tenant.code(), tonic::Code::FailedPrecondition);

    // ── authorization code + PKCE: JIT-provisioned first sign-in ────────────
    let started = start(&svc, &tenant, &provider_id, "").await;
    let url = &started.authorization_url;
    assert!(url.starts_with(&format!("{issuer}/authorize?")), "{url}");
    assert_eq!(query_param(url, "client_id"), CLIENT_ID);
    assert_eq!(query_param(url, "response_type"), "code");
    assert_eq!(query_param(url, "code_challenge_method"), "S256");
    assert_eq!(query_param(url, "scope"), "openid email profile");
    assert_eq!(query_param(url, "login_hint"), "person@example.test");
    assert_eq!(query_param(url, "code_challenge").len(), 43);
    assert!(!query_param(url, "nonce").is_empty());
    assert!(started.expires_at_unix > now_unix() as i64);
    // Only a keyed hash of the state is stored, never the state itself.
    let raw_state_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_authn.oidc_login_states WHERE state_hash = $1",
    )
    .bind(&started.state)
    .fetch_one(&pool)
    .await
    .expect("count raw state rows");
    assert_eq!(raw_state_rows, 0);

    let code = authorize(&fake, &started, "google-sub-1", None).await;
    let first = finish(&svc, &started.state, &code)
        .await
        .expect("finish OIDC sign-in");
    assert!(!first.user_id.is_empty());
    assert!(!first.session_id.is_empty());
    assert!(!first.access_token.is_empty(), "JWT access token issued");
    assert!(
        first.refresh_token.starts_with("rt_"),
        "refresh family minted"
    );
    assert!(first.refresh_token_expires_in > 0);
    assert!(!first.session_public_id.is_empty());
    assert!(!first.mfa_required);
    assert!(
        first.provisioned,
        "first sign-in JIT-provisions the account"
    );
    let linked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_idp.external_identities WHERE subject = $1 AND user_id = $2::UUID",
    )
    .bind("google-sub-1")
    .bind(&first.user_id)
    .fetch_one(&pool)
    .await
    .expect("count external identity links");
    assert_eq!(linked, 1, "the external identity is linked to the new user");

    // ── the state is single use ────────────────────────────────────────────
    let replay = finish(&svc, &started.state, &code)
        .await
        .expect_err("a consumed state cannot finish twice");
    assert_eq!(replay.code(), tonic::Code::Unauthenticated);
    let unknown_state = finish(&svc, "not-a-real-state", "whatever")
        .await
        .expect_err("an unknown state is refused");
    assert_eq!(unknown_state.code(), tonic::Code::Unauthenticated);

    // ── nonce mismatch: the provider's ID token carries another nonce ──────
    let started = start(&svc, &tenant, &provider_id, "").await;
    let code = authorize(&fake, &started, "google-sub-1", Some("some-other-nonce")).await;
    let mismatch = finish(&svc, &started.state, &code)
        .await
        .expect_err("an ID token with the wrong nonce is refused");
    assert_eq!(mismatch.code(), tonic::Code::Unauthenticated);
    assert_eq!(mismatch.message(), "invalid credential");

    // ── a wrong PKCE verifier (code redeemed against another sign-in) ──────
    let started_a = start(&svc, &tenant, &provider_id, "").await;
    let started_b = start(&svc, &tenant, &provider_id, "").await;
    let code_for_a = authorize(&fake, &started_a, "google-sub-1", None).await;
    let crossed = finish(&svc, &started_b.state, &code_for_a)
        .await
        .expect_err("a code bound to another PKCE challenge is refused");
    assert_eq!(crossed.code(), tonic::Code::Unauthenticated);

    // ── returning user: same account, not re-provisioned ───────────────────
    let started = start(&svc, &tenant, &provider_id, "").await;
    let code = authorize(&fake, &started, "google-sub-1", None).await;
    let again = finish(&svc, &started.state, &code)
        .await
        .expect("returning user signs in");
    assert_eq!(again.user_id, first.user_id);
    assert!(!again.provisioned);

    // ── an expired state is refused ─────────────────────────────────────────
    let started = start(&svc, &tenant, &provider_id, "").await;
    let code = authorize(&fake, &started, "google-sub-1", None).await;
    sqlx::query(
        "UPDATE udb_authn.oidc_login_states SET expires_at = now() - interval '1 minute' \
         WHERE consumed_at IS NULL",
    )
    .execute(&pool)
    .await
    .expect("expire pending states");
    let expired = finish(&svc, &started.state, &code)
        .await
        .expect_err("an expired sign-in is refused");
    assert_eq!(expired.code(), tonic::Code::Unauthenticated);

    // ── an invitation-bound sign-in is not redeemable by FinishOidcLogin ───
    let invitation_id = Uuid::new_v4().to_string();
    let started = start(&svc, &tenant, &provider_id, &invitation_id).await;
    let code = authorize(&fake, &started, "google-sub-invitee", None).await;
    let bound = finish(&svc, &started.state, &code)
        .await
        .expect_err("FinishOidcLogin refuses an invitation-bound state");
    assert_eq!(bound.code(), tonic::Code::Unauthenticated);
    let still_pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM udb_authn.oidc_login_states \
         WHERE invitation_id = $1 AND consumed_at IS NULL",
    )
    .bind(&invitation_id)
    .fetch_one(&pool)
    .await
    .expect("count invitation-bound states");
    assert_eq!(
        still_pending, 1,
        "the invitation's sign-in stays redeemable"
    );

    // ── native app: LoginWithOidcIdToken ────────────────────────────────────
    let unbound = fake
        .lock()
        .await
        .id_token("google-sub-1", "person@example.test", None);
    let native = login_with_id_token(&svc, &tenant, &provider_id, &unbound, "")
        .await
        .expect("a fresh unbound ID token signs in");
    assert_eq!(native.user_id, first.user_id);
    assert!(native.refresh_token.starts_with("rt_"));
    let replayed = login_with_id_token(&svc, &tenant, &provider_id, &unbound, "")
        .await
        .expect_err("the same ID token signs in only once");
    assert_eq!(replayed.code(), tonic::Code::Unauthenticated);

    let bound_token =
        fake.lock()
            .await
            .id_token("google-sub-1", "person@example.test", Some("app-nonce"));
    let wrong_nonce = login_with_id_token(&svc, &tenant, &provider_id, &bound_token, "other")
        .await
        .expect_err("a nonce mismatch is refused");
    assert_eq!(wrong_nonce.code(), tonic::Code::Unauthenticated);
    login_with_id_token(&svc, &tenant, &provider_id, &bound_token, "app-nonce")
        .await
        .expect("the matching nonce signs in");

    let forged = {
        let mut token = fake
            .lock()
            .await
            .id_token("google-sub-1", "person@example.test", None);
        token.push('A');
        token
    };
    let forged = login_with_id_token(&svc, &tenant, &provider_id, &forged, "")
        .await
        .expect_err("a tampered ID token is refused");
    assert_eq!(forged.code(), tonic::Code::Unauthenticated);

    // ── a suspended account is refused like password Login ─────────────────
    sqlx::query("UPDATE udb_authn.users SET status = 'SUSPENDED' WHERE user_id = $1::UUID")
        .bind(&first.user_id)
        .execute(&pool)
        .await
        .expect("suspend the federated user");
    let fresh = fake
        .lock()
        .await
        .id_token("google-sub-1", "person@example.test", None);
    let suspended = login_with_id_token(&svc, &tenant, &provider_id, &fresh, "")
        .await
        .expect_err("a suspended account cannot sign in");
    assert_eq!(suspended.code(), tonic::Code::PermissionDenied);

    // ── a disabled provider stops sign-in ───────────────────────────────────
    idp.disable_provider(Request::new(idp_pb::DisableProviderRequest {
        tenant_id: tenant.clone(),
        provider_id: provider_id.clone(),
        updated_by: "oidc-live".to_string(),
        ..Default::default()
    }))
    .await
    .expect("disable provider");
    let disabled = svc
        .start_oidc_login(Request::new(authn_pb::StartOidcLoginRequest {
            tenant_hint: tenant.clone(),
            provider_id: provider_id.clone(),
            redirect_uri: REDIRECT_URI.to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a disabled provider cannot start a sign-in");
    assert_eq!(disabled.code(), tonic::Code::FailedPrecondition);

    assert!(
        fake.lock().await.token_requests == 4,
        "every code redemption reached the provider's token endpoint"
    );
    cleanup_native_auth_db(&pool).await;
}

/// Seed an active tenant role with `code` (role_code and name).
async fn seed_tenant_role(pool: &sqlx::PgPool, tenant: &str, code: &str) {
    use crate::runtime::native_catalog::native_model;
    let m = native_model(
        "udb.core.authz.entity.v1.Role",
        &[
            "role_id",
            "name",
            "description",
            "is_system",
            "is_active",
            "tenant_id",
            "project_id",
            "role_code",
            "scope_type",
        ],
    );
    sqlx::query(&format!(
        "INSERT INTO {rel} ({rid}, {name}, {description}, {is_system}, {is_active}, {tenant}, {project}, {code}, {scope}) \
         VALUES ($1, $2, 'oidc live role', FALSE, TRUE, $3, '', $2, 'TENANT')",
        rel = m.relation,
        rid = m.q("role_id"),
        name = m.q("name"),
        description = m.q("description"),
        is_system = m.q("is_system"),
        is_active = m.q("is_active"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        code = m.q("role_code"),
        scope = m.q("scope_type"),
    ))
    .bind(Uuid::new_v4())
    .bind(code)
    .bind(tenant)
    .execute(pool)
    .await
    .expect("seed tenant role");
}

/// Role codes bound to `user_id` in `tenant`, sorted.
async fn bound_role_codes(pool: &sqlx::PgPool, tenant: &str, user_id: &str) -> Vec<String> {
    use crate::runtime::native_catalog::native_model;
    let ur = native_model(
        "udb.core.authz.entity.v1.UserRole",
        &["user_id", "role_id", "tenant_id"],
    );
    let r = native_model("udb.core.authz.entity.v1.Role", &["role_id", "role_code"]);
    let mut codes: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT r.{code} FROM {ur_rel} ur JOIN {r_rel} r ON r.{rid} = ur.{ur_rid} \
         WHERE ur.{uid} = $1 AND ur.{tenant} = $2",
        code = r.q("role_code"),
        ur_rel = ur.relation,
        r_rel = r.relation,
        rid = r.q("role_id"),
        ur_rid = ur.q("role_id"),
        uid = ur.q("user_id"),
        tenant = ur.q("tenant_id"),
    ))
    .bind(Uuid::parse_str(user_id).expect("user id is a UUID"))
    .bind(tenant)
    .fetch_all(pool)
    .await
    .expect("read role bindings");
    codes.sort();
    codes
}

async fn token_roles(svc: &super::super::AuthnServiceImpl, access_token: &str) -> Vec<String> {
    use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
    let validated = svc
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: access_token.to_string(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate the issued access token")
        .into_inner();
    assert!(validated.valid, "the issued access token validates");
    validated.roles
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_oidc_jit_user_gets_mapped_roles_bound_and_never_reserved_ones() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let _loopback = LoopbackIdpSwitch::on();
    let (issuer, fake) = spawn_fake_provider().await;
    let tenant = Uuid::new_v4().to_string();
    for code in ["doctor", "nurse"] {
        seed_tenant_role(&pool, &tenant, code).await;
    }
    let broker = native_broker_service().await;
    let keyed = keyed_live_runtime().await;
    let svc = authn_service_with_jwt(pool.clone())
        .with_authz_snapshot(Some(broker.authz_snapshot()))
        .with_runtime(Some(keyed.clone()));
    let idp = super::super::idp::IdentityProviderServiceImpl::new()
        .with_postgres(Some(pool.clone()))
        .with_runtime(keyed.clone());
    let provider_id = idp
        .create_provider(Request::new(idp_pb::CreateProviderRequest {
            tenant_id: tenant.clone(),
            kind: idp_entity_pb::IdpKind::Oidc as i32,
            display_name: "Fake Google with roles".to_string(),
            issuer: issuer.clone(),
            client_ids: vec![CLIENT_ID.to_string()],
            client_secret: CLIENT_SECRET.to_string(),
            // A provider mapping that names reserved roles and an unknown one:
            // only `doctor` may be bound.
            jit_policy_json: serde_json::json!({
                "default_project": "default",
                "default_roles": ["doctor", "organization_owner", "platform_admin", "ghost_role"],
            })
            .to_string(),
            group_mapping_json: serde_json::json!({
                "clinicians": ["nurse", "organization_owner"],
            })
            .to_string(),
            enabled: true,
            redirect_uris: vec![REDIRECT_URI.to_string()],
            ..Default::default()
        }))
        .await
        .expect("create OIDC provider with role mappings")
        .into_inner()
        .provider
        .expect("created provider")
        .provider_id;

    // ── JIT sign-in: default_roles bound, reserved codes skipped ───────────
    let token = fake
        .lock()
        .await
        .id_token("google-doctor-1", "doctor@example.test", None);
    let first = login_with_id_token(&svc, &tenant, &provider_id, &token, "")
        .await
        .expect("JIT sign-in");
    assert!(first.provisioned);
    assert_eq!(
        bound_role_codes(&pool, &tenant, &first.user_id).await,
        vec!["doctor".to_string()],
        "only the non-reserved, existing default role is bound"
    );
    let roles = token_roles(&svc, &first.access_token).await;
    assert!(
        roles.contains(&"doctor".to_string()),
        "the first sign-in's token already carries the bound role, got {roles:?}"
    );
    assert!(
        !roles
            .iter()
            .any(|r| r == "organization_owner" || r == "platform_admin")
    );

    // ── an administrator removes `doctor`; a later sign-in does not re-add a
    //    default role, but adds the missing group-mapped `nurse` ───────────
    sqlx::query(
        "DELETE FROM udb_authz.user_roles WHERE user_id = $1::UUID \
           AND role_id IN (SELECT role_id FROM udb_authz.roles WHERE role_code = 'doctor')",
    )
    .bind(&first.user_id)
    .execute(&pool)
    .await
    .expect("administrator removes the doctor binding");
    fake.lock().await.groups = vec!["clinicians".to_string()];
    let token = fake
        .lock()
        .await
        .id_token("google-doctor-1", "doctor@example.test", None);
    let again = login_with_id_token(&svc, &tenant, &provider_id, &token, "")
        .await
        .expect("returning sign-in with a group claim");
    assert_eq!(again.user_id, first.user_id);
    assert!(!again.provisioned);
    assert_eq!(
        bound_role_codes(&pool, &tenant, &first.user_id).await,
        vec!["nurse".to_string()],
        "the group-mapped role is added; the removed default role and the reserved owner role are not"
    );
    let roles = token_roles(&svc, &again.access_token).await;
    assert!(roles.contains(&"nurse".to_string()), "got {roles:?}");
    assert!(!roles.contains(&"organization_owner".to_string()));

    // ── idempotent: the same claims bind nothing new ────────────────────────
    let token = fake
        .lock()
        .await
        .id_token("google-doctor-1", "doctor@example.test", None);
    login_with_id_token(&svc, &tenant, &provider_id, &token, "")
        .await
        .expect("third sign-in");
    assert_eq!(
        bound_role_codes(&pool, &tenant, &first.user_id).await,
        vec!["nurse".to_string()]
    );
    cleanup_native_auth_db(&pool).await;
}
