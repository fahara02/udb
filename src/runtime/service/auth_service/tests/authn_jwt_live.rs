//! Phase 3 regression: UDB-issued JWT validation is bound to live persisted user
//! state, so a signature-valid, unexpired access token is rejected once its
//! subject user is suspended/deactivated — revocation before token expiry.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_jwt_validation_binds_user_status -- --ignored --nocapture"]
async fn live_postgres_jwt_validation_binds_user_status() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "jwtbind", "CorrectHorse1!").await;

    // Password login issues a UDB-signed access token (a JWT) because the service
    // is configured with a signing key.
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "jwt".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();
    assert!(
        !login.access_token.is_empty(),
        "login must issue a UDB JWT when a signing key is configured"
    );

    // While the user is active, the JWT validates.
    let ok = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate jwt")
        .into_inner();
    assert!(ok.valid, "an active user's JWT must validate");
    assert_eq!(ok.user_id, user.user_id);

    // Suspend the user. The same token is still signature-valid and unexpired,
    // but must now be rejected because validation is bound to user status.
    authn
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("suspend user");

    let after = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token,
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate jwt after suspend")
        .into_inner();
    assert!(
        !after.valid,
        "a suspended user's JWT must be rejected before its natural expiry"
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_jwt_invalidated_by_session_revocation -- --ignored --nocapture"]
async fn live_postgres_jwt_invalidated_by_session_revocation() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "jwtsess", "CorrectHorse1!").await;

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "jwt-sess".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();
    assert!(!login.access_token.is_empty());

    // The access token (jti = issuing session id) validates while the session lives.
    let ok = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate jwt")
        .into_inner();
    assert!(ok.valid, "JWT must validate while its session is active");

    // Revoke the issuing session (logout). The user is still active, but the
    // access token must now be rejected — revocation before token expiry.
    let revoked = authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: login.session_id.clone(),
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke session")
        .into_inner();
    assert_eq!(revoked.revoked_count, 1);

    let after = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token,
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate jwt after session revoke")
        .into_inner();
    assert!(
        !after.valid,
        "a JWT whose session was revoked must be rejected before its natural expiry"
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_refresh_blocked_after_user_suspended -- --ignored --nocapture"]
async fn live_postgres_refresh_blocked_after_user_suspended() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "jwtrefresh", "CorrectHorse1!").await;

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "refresh".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();

    // Refresh works while the user is active.
    let refreshed = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            session_id: login.session_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("refresh while active")
        .into_inner();
    assert!(!refreshed.access_token.is_empty());

    // Suspend the user; the session still lives, but refresh must now be blocked
    // (a suspended user cannot keep minting access tokens).
    authn
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("suspend user");

    let err = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            session_id: login.session_id.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("refresh must be blocked after the user is suspended");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}

/// I2.2: login issues a token-family refresh token (rt_…, NOT the session id);
/// refreshing rotates it; presenting the rotated-away (old) value is reuse, which
/// revokes the family so neither the old nor the new refresh token works after.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_refresh_token_family_rotation_and_reuse -- --ignored --nocapture"]
async fn live_postgres_refresh_token_family_rotation_and_reuse() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "rtfamily", "CorrectHorse1!").await;

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "rtf".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();
    // The refresh token is a token-family credential, never the raw session id.
    assert!(
        login.refresh_token.starts_with("rt_"),
        "login must issue a token-family refresh token, got {:?}",
        login.refresh_token
    );
    assert_ne!(login.refresh_token, login.session_id);

    // First rotation succeeds and returns a fresh, different refresh token.
    let first = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: login.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("first refresh")
        .into_inner();
    assert!(!first.access_token.is_empty());
    assert!(first.refresh_token.starts_with("rt_"));
    assert_ne!(first.refresh_token, login.refresh_token);

    // Replaying the ORIGINAL (now rotated-away) refresh token is reuse: rejected,
    // and it revokes the whole family.
    let reuse = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: login.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("reused refresh token must be rejected");
    assert_eq!(reuse.code(), tonic::Code::Unauthenticated);

    // After reuse detection the family is revoked, so even the legitimately
    // rotated token no longer works.
    let after = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: first.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("rotated token must be dead once the family is revoked");
    assert_eq!(after.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

/// Logout must kill the refresh-token family bound to the session — otherwise a
/// logged-out session's refresh token keeps minting access tokens (the family
/// path only checks the user is active, not whether the session was revoked).
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_refresh_token_dies_with_session_logout -- --ignored --nocapture"]
async fn live_postgres_refresh_token_dies_with_session_logout() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "rtlogout", "CorrectHorse1!").await;

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "rtl".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login")
        .into_inner();
    assert!(login.refresh_token.starts_with("rt_"));

    // Sanity: the refresh token works BEFORE logout (and rotates).
    let ok = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: login.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("refresh before logout")
        .into_inner();
    assert!(!ok.access_token.is_empty());

    // Log the session out.
    authn
        .logout(Request::new(authn_pb::LogoutRequest {
            session_id: login.session_id.clone(),
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("logout");

    // The refresh token must now be DEAD: logout revoked the family, so the
    // logged-out session can no longer mint access tokens via RefreshToken.
    let denied = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: ok.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect_err("refresh must fail after the session is logged out");
    assert_eq!(denied.code(), tonic::Code::Unauthenticated);

    // bug_report #3 (second path): RefreshSession must ALSO fail after logout — a
    // revoked session can't be refreshed, and the RPC must ERROR (not return a
    // success envelope a caller would read as "still active").
    let session_denied = authn
        .refresh_session(Request::new(authn_pb::RefreshSessionRequest {
            session_id: login.session_id.clone(),
            ttl_seconds: 120,
        }))
        .await
        .expect_err("refresh_session must fail after logout");
    assert_eq!(session_denied.code(), tonic::Code::Unauthenticated);

    // bug_report §3 UPDATE 2: the ACCESS TOKEN itself must die with logout — it
    // must neither validate nor introspect Active afterwards. The JWT `jti` is the
    // issuing session id, so the durable session/jti revocation logout performs
    // must be visible to both `ValidateToken` and `IntrospectToken`.
    let validated = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate access token after logout")
        .into_inner();
    assert!(
        !validated.valid,
        "access token must NOT validate after logout (bug_report §3)"
    );
    let introspected = authn
        .introspect_token(Request::new(authn_pb::IntrospectTokenRequest {
            token: login.access_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("introspect access token after logout")
        .into_inner();
    assert!(
        !introspected.active,
        "access token must NOT introspect Active after logout (bug_report §3)"
    );

    cleanup_native_auth_db(&pool).await;
}
