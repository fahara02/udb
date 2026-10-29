//! 0.6.6 passkeys, live against Postgres with the dev soft authenticator
//! (`UDB_WEBAUTHN_TEST_MODE`): a real P-256 passkey is registered and real ES256
//! assertions are verified by webauthn-rs — nothing is mocked.
//!
//! * a passkey sign-in returns a refresh token that RefreshToken accepts, and
//!   its challenge cannot be replayed;
//! * usernameless sign-in resolves the user from the passkey's user handle and
//!   stays inside the challenge's tenant;
//! * a passkey completes password Login as the second factor, while a malformed,
//!   foreign, usernameless or replayed assertion does not.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use tonic::Request;

/// What the soft authenticator answers with: the plain sentinel completes a
/// per-user challenge; `SENTINEL:<user_id>` names the user for a usernameless one.
const SOFT_AUTHENTICATOR: &str = "__UDB_WEBAUTHN_TEST__";
const PASSWORD: &str = "CorrectHorse1!";

fn enable_soft_authenticator() {
    // SAFETY: live auth tests run serially under `live_auth_db_lock`; the RP
    // config is read per ceremony and the soft-authenticator gate is resolved
    // once per process, before the first ceremony below.
    unsafe {
        std::env::set_var("UDB_WEBAUTHN_RP_ID", "localhost");
        std::env::set_var("UDB_WEBAUTHN_ORIGIN", "http://localhost");
        std::env::set_var("UDB_WEBAUTHN_TEST_MODE", "1");
    }
}

async fn register_passkey(svc: &super::super::AuthnServiceImpl, user: &authn_entity_pb::User) {
    let started = svc
        .start_web_authn_registration(Request::new(authn_pb::StartWebAuthnRegistrationRequest {
            user_id: user.user_id.clone(),
            tenant_id: user.tenant_id.clone(),
            project_id: user.project_id.clone(),
            label: "live laptop".to_string(),
            ..Default::default()
        }))
        .await
        .expect("start passkey registration")
        .into_inner();
    let finished = svc
        .finish_web_authn_registration(Request::new(authn_pb::FinishWebAuthnRegistrationRequest {
            challenge_id: started.challenge_id,
            public_key_credential_json: SOFT_AUTHENTICATOR.to_string(),
            ..Default::default()
        }))
        .await
        .expect("finish passkey registration")
        .into_inner();
    assert!(finished.registered);
    assert_eq!(finished.user_id, user.user_id);
}

async fn start_assertion(
    svc: &super::super::AuthnServiceImpl,
    user_id: &str,
    tenant_id: &str,
) -> authn_pb::StartWebAuthnAuthenticationResponse {
    svc.start_web_authn_authentication(Request::new(authn_pb::StartWebAuthnAuthenticationRequest {
        user_id: user_id.to_string(),
        tenant_id: tenant_id.to_string(),
        ..Default::default()
    }))
    .await
    .expect("start passkey authentication")
    .into_inner()
}

async fn finish_assertion(
    svc: &super::super::AuthnServiceImpl,
    challenge_id: &str,
    credential: &str,
) -> Result<authn_pb::FinishWebAuthnAuthenticationResponse, tonic::Status> {
    svc.finish_web_authn_authentication(Request::new(
        authn_pb::FinishWebAuthnAuthenticationRequest {
            challenge_id: challenge_id.to_string(),
            public_key_credential_json: credential.to_string(),
            device_name: "passkey live".to_string(),
            ip_address: "203.0.113.7".to_string(),
            user_agent: "passkeys-live".to_string(),
            device_id: format!("dev-{challenge_id}"),
            ..Default::default()
        },
    ))
    .await
    .map(tonic::Response::into_inner)
}

fn password_login(username: &str) -> authn_pb::LoginRequest {
    authn_pb::LoginRequest {
        username: username.to_string(),
        password: PASSWORD.to_string(),
        device_name: "passkey second factor".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_passkey_sign_in_issues_a_refresh_token_refresh_token_accepts() {
    let _guard = live_auth_db_lock().lock().await;
    enable_soft_authenticator();
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&svc, "passkey_signin", PASSWORD).await;
    register_passkey(&svc, &user).await;

    let challenge = start_assertion(&svc, &user.user_id, &user.tenant_id).await;
    let signed_in = finish_assertion(&svc, &challenge.challenge_id, SOFT_AUTHENTICATOR)
        .await
        .expect("passkey sign-in");
    assert_eq!(signed_in.user_id, user.user_id);
    assert!(!signed_in.credential_id.is_empty());
    assert!(!signed_in.access_token.is_empty(), "access token issued");
    assert!(!signed_in.session_id.is_empty());
    assert_eq!(signed_in.session_token, signed_in.session_id);
    assert!(!signed_in.csrf_token.is_empty(), "CSRF token issued");
    assert!(signed_in.session_public_id.starts_with("sesspub_"));
    assert!(
        signed_in.refresh_token.starts_with("rt_"),
        "a passkey sign-in mints a refresh family, got {:?}",
        signed_in.refresh_token
    );
    assert!(signed_in.refresh_token_expires_in > 0);
    assert!(signed_in.access_token_expires_in > 0);
    assert!(signed_in.expires_at_unix > now_unix() as i64);
    let principal = signed_in.principal.as_ref().expect("principal");
    assert_eq!(principal.user_id, user.user_id);
    assert_eq!(principal.tenant_id, user.tenant_id);

    let refreshed = svc
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: signed_in.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("RefreshToken accepts the passkey sign-in's refresh token")
        .into_inner();
    assert!(!refreshed.access_token.is_empty());
    assert!(!refreshed.refresh_token.is_empty());
    assert_ne!(
        refreshed.refresh_token, signed_in.refresh_token,
        "the refresh token rotates"
    );

    // The challenge is single use: replaying it never mints a second sign-in.
    let replay = finish_assertion(&svc, &challenge.challenge_id, SOFT_AUTHENTICATOR)
        .await
        .expect_err("a consumed challenge cannot sign in again");
    assert_eq!(replay.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_usernameless_passkey_sign_in_resolves_the_user() {
    let _guard = live_auth_db_lock().lock().await;
    enable_soft_authenticator();
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&svc, "passkey_usernameless", PASSWORD).await;
    register_passkey(&svc, &user).await;
    let named = format!("{SOFT_AUTHENTICATOR}:{}", user.user_id);

    // No user named: the options carry no allowCredentials list.
    let challenge = start_assertion(&svc, "", &user.tenant_id).await;
    let options: serde_json::Value =
        serde_json::from_str(&challenge.public_key_credential_request_options_json)
            .expect("request options JSON");
    let allowed = options["publicKey"]["allowCredentials"]
        .as_array()
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(allowed, 0, "usernameless options must not list credentials");

    let signed_in = finish_assertion(&svc, &challenge.challenge_id, &named)
        .await
        .expect("usernameless passkey sign-in");
    assert_eq!(
        signed_in.user_id, user.user_id,
        "user resolved from the user handle"
    );
    assert_eq!(
        signed_in.principal.as_ref().expect("principal").user_id,
        user.user_id
    );
    assert!(signed_in.refresh_token.starts_with("rt_"));
    svc.refresh_token(Request::new(authn_pb::RefreshTokenRequest {
        refresh_token: signed_in.refresh_token,
        ..Default::default()
    }))
    .await
    .expect("RefreshToken accepts the usernameless sign-in's refresh token");

    // A challenge for another tenant cannot be completed by this tenant's user.
    let foreign = start_assertion(&svc, "", "passkeys-live-other-tenant").await;
    let err = finish_assertion(&svc, &foreign.challenge_id, &named)
        .await
        .expect_err("a passkey of another tenant must not sign in");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // A user without a passkey gets the same refusal as any unknown handle.
    let bare = create_verified_user(&svc, "passkey_none", PASSWORD).await;
    let challenge = start_assertion(&svc, "", &bare.tenant_id).await;
    let err = finish_assertion(
        &svc,
        &challenge.challenge_id,
        &format!("{SOFT_AUTHENTICATOR}:{}", bare.user_id),
    )
    .await
    .expect_err("a user without a passkey cannot sign in");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_password_login_accepts_a_passkey_as_the_second_factor() {
    let _guard = live_auth_db_lock().lock().await;
    enable_soft_authenticator();
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&svc, "passkey_mfa", PASSWORD).await;
    register_passkey(&svc, &user).await;
    let other = create_verified_user(&svc, "passkey_mfa_other", PASSWORD).await;
    register_passkey(&svc, &other).await;

    // Registering a passkey enrols MFA: the password alone asks for a factor.
    let step_one = svc
        .login(Request::new(password_login(&user.username)))
        .await
        .expect("password step")
        .into_inner();
    assert!(step_one.mfa_required);
    assert!(step_one.refresh_token.is_empty());
    assert_eq!(step_one.user_id, user.user_id);

    let challenge = start_assertion(&svc, &step_one.user_id, "").await;
    let with_passkey = |challenge_id: &str, assertion: &str| authn_pb::LoginRequest {
        webauthn_challenge_id: challenge_id.to_string(),
        webauthn_assertion_json: assertion.to_string(),
        ..password_login(&user.username)
    };

    // A malformed assertion is a failed second factor, not a server error.
    let err = svc
        .login(Request::new(with_passkey(&challenge.challenge_id, "{}")))
        .await
        .expect_err("a malformed assertion must not complete the login");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Another user's challenge, even answered by that user's passkey, is refused.
    let foreign = start_assertion(&svc, &other.user_id, "").await;
    let err = svc
        .login(Request::new(with_passkey(
            &foreign.challenge_id,
            SOFT_AUTHENTICATOR,
        )))
        .await
        .expect_err("a challenge issued for another user must not complete the login");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // A usernameless challenge names no user, so it is no second factor either.
    let usernameless = start_assertion(&svc, "", &user.tenant_id).await;
    let err = svc
        .login(Request::new(with_passkey(
            &usernameless.challenge_id,
            &format!("{SOFT_AUTHENTICATOR}:{}", user.user_id),
        )))
        .await
        .expect_err("a usernameless challenge must not complete the login");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // The user's own challenge and passkey complete the login.
    let logged_in = svc
        .login(Request::new(with_passkey(
            &challenge.challenge_id,
            SOFT_AUTHENTICATOR,
        )))
        .await
        .expect("password + passkey login")
        .into_inner();
    assert!(!logged_in.mfa_required);
    assert_eq!(logged_in.user_id, user.user_id);
    assert!(!logged_in.access_token.is_empty());
    assert!(logged_in.refresh_token.starts_with("rt_"));

    // Single use: the same challenge cannot complete a second login.
    let err = svc
        .login(Request::new(with_passkey(
            &challenge.challenge_id,
            SOFT_AUTHENTICATOR,
        )))
        .await
        .expect_err("a consumed challenge must not complete another login");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}
