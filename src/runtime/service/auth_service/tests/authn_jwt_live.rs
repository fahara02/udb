//! Phase 3 regression: UDB-issued JWT validation is bound to live persisted user
//! state, so a signature-valid, unexpired access token is rejected once its
//! subject user is suspended/deactivated — revocation before token expiry.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use tonic::Request;

fn revocation_fixture_context() -> crate::proto::udb::core::common::v1::RequestContext {
    crate::proto::udb::core::common::v1::RequestContext {
        tenant: Some(crate::proto::udb::core::common::v1::TenantContext {
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_revoke_session_closes_refresh_family_and_partial_retry() {
    use crate::runtime::authn::{PostgresSessionStore, SessionStore};

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    for session_already_revoked in [false, true] {
        let user = create_verified_user(&authn, "revoke-family", "CorrectHorse1!").await;
        let login = authn
            .login(Request::new(authn_pb::LoginRequest {
                username: user.email.clone(),
                password: "CorrectHorse1!".to_string(),
                device_name: "revoke-family".to_string(),
                ..Default::default()
            }))
            .await
            .expect("login before session revocation")
            .into_inner();
        assert!(login.refresh_token.starts_with("rt_"));
        let rotated = authn
            .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
                refresh_token: login.refresh_token.clone(),
                ..Default::default()
            }))
            .await
            .expect("rotate a live family before revocation")
            .into_inner();
        let active = authn
            .validate_token(Request::new(authn_pb::ValidateTokenRequest {
                token: rotated.access_token.clone(),
                token_type: authn_entity_pb::TokenType::JwtAccess as i32,
            }))
            .await
            .expect("validate refreshed bearer before revocation")
            .into_inner();
        assert!(active.valid, "a live family's bearer must validate");
        if session_already_revoked {
            // Model a partial previous request: the durable session change
            // committed, but the family side effect did not complete.
            let hash = crate::runtime::authn::hash_secret(&login.session_id, &authn.hash_key());
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_secs();
            assert!(
                PostgresSessionStore::new(pool.clone(), "")
                    .revoke(&hash, now)
                    .await
                    .expect("prepare partially revoked session")
            );
        }
        let request = authn_pb::RevokeSessionRequest {
            session_id: login.session_id.clone(),
            revoke_reason: "live family revocation regression".to_string(),
            context: Some(revocation_fixture_context()),
            ..Default::default()
        };
        let revoked = authn
            .revoke_session(Request::new(request.clone()))
            .await
            .expect("revoke session and its family")
            .into_inner();
        assert_eq!(revoked.revoked_count, i32::from(!session_already_revoked));
        let validated = authn
            .validate_token(Request::new(authn_pb::ValidateTokenRequest {
                token: rotated.access_token.clone(),
                token_type: authn_entity_pb::TokenType::JwtAccess as i32,
            }))
            .await
            .expect("validate previously refreshed bearer")
            .into_inner();
        assert!(
            !validated.valid,
            "family-issued bearer must also be revoked"
        );
        let bearer_denied = authn
            .authenticate(Request::new(authn_pb::AuthnRequest {
                bearer_token: rotated.access_token,
                ..Default::default()
            }))
            .await
            .expect_err("ordinary bearer authentication must see family revocation");
        assert_eq!(bearer_denied.code(), tonic::Code::Unauthenticated);
        let denied = authn
            .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
                refresh_token: rotated.refresh_token.clone(),
                ..Default::default()
            }))
            .await
            .expect_err("a revoked session family cannot mint another bearer");
        assert_eq!(denied.code(), tonic::Code::Unauthenticated);
        let retry = authn
            .revoke_session(Request::new(request))
            .await
            .expect("repeat complete revocation")
            .into_inner();
        assert_eq!(retry.revoked_count, 0);
    }
    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_revoke_principal_closes_every_family_preserves_other_user() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let target = create_verified_user(&authn, "revoke-principal", "CorrectHorse1!").await;
    let survivor = create_verified_user(&authn, "surviving-principal", "CorrectHorse1!").await;
    let mut target_refresh = Vec::new();
    let mut survivor_refresh = String::new();
    for user in [&target, &target, &survivor] {
        let login = authn
            .login(Request::new(authn_pb::LoginRequest {
                username: user.email.clone(),
                password: "CorrectHorse1!".to_string(),
                device_name: uuid::Uuid::new_v4().to_string(),
                ..Default::default()
            }))
            .await
            .expect("login before principal revocation")
            .into_inner();
        assert!(login.refresh_token.starts_with("rt_"));
        if user.user_id == target.user_id {
            target_refresh.push(login.refresh_token);
        } else {
            survivor_refresh = login.refresh_token;
        }
    }
    let request = authn_pb::RevokeSessionRequest {
        all_for_principal: true,
        principal_id: target.user_id,
        context: Some(revocation_fixture_context()),
        ..Default::default()
    };
    let revoked = authn
        .revoke_session(Request::new(request.clone()))
        .await
        .expect("revoke both target sessions and families")
        .into_inner();
    assert_eq!(revoked.revoked_count, 2);
    for refresh_token in target_refresh {
        let denied = authn
            .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
                refresh_token,
                ..Default::default()
            }))
            .await
            .expect_err("every target family must refuse refresh");
        assert_eq!(denied.code(), tonic::Code::Unauthenticated);
    }
    let survivor_rotated = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: survivor_refresh,
            ..Default::default()
        }))
        .await
        .expect("another principal's family remains active")
        .into_inner();
    assert!(!survivor_rotated.access_token.is_empty());
    let retry = authn
        .revoke_session(Request::new(request))
        .await
        .expect("repeat principal revocation")
        .into_inner();
    assert_eq!(retry.revoked_count, 0);
    authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: survivor_rotated.refresh_token,
            ..Default::default()
        }))
        .await
        .expect("idempotent retry preserves the unrelated family");
    cleanup_native_auth_db(&pool).await;
}

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

    // One stable, non-secret public session id: Login, RefreshToken, and
    // ValidateToken (for the login token AND the refreshed token) all agree, and
    // it is never the session handle itself.
    assert!(
        login.session_public_id.starts_with("sesspub_"),
        "login must report a public session id, got {:?}",
        login.session_public_id
    );
    assert_ne!(login.session_public_id, login.session_id);
    assert_eq!(first.session_public_id, login.session_public_id);
    for (label, token) in [
        ("login", &login.access_token),
        ("refreshed", &first.access_token),
    ] {
        let validated = authn
            .validate_token(Request::new(authn_pb::ValidateTokenRequest {
                token: token.clone(),
                token_type: authn_entity_pb::TokenType::JwtAccess as i32,
            }))
            .await
            .unwrap_or_else(|err| panic!("validate {label} token: {err}"))
            .into_inner();
        assert!(validated.valid, "{label} token must validate");
        assert_eq!(
            validated.session_public_id, login.session_public_id,
            "{label} token must report the login session's public id"
        );
        assert!(
            validated.expires_at.is_some(),
            "{label} token reports its expiry"
        );
    }
    let login_validated = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate login token")
        .into_inner();
    assert_eq!(
        login_validated.session_id, login.session_id,
        "the login token reports the session handle login returned"
    );

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
            context: Some(crate::proto::udb::core::common::v1::RequestContext {
                tenant: Some(crate::proto::udb::core::common::v1::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
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

/// The per-request verdict is remembered, but a revocation made on this node ends it
/// at once, and a change made elsewhere (written straight to the store, as another
/// broker node would) ends it within the cache lifetime.
#[tokio::test]
#[ignore = "requires live Postgres; run in the unfiltered native live lane"]
async fn live_postgres_cached_bearer_verdict_honours_revocation() {
    use crate::runtime::authn::{PostgresSessionStore, SessionStore, hash_secret};
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service_with_jwt(pool.clone());
    let user = create_verified_user(&authn, "jwtverdict", "CorrectHorse1!").await;
    let security = crate::runtime::security::SecurityConfig {
        jwt_public_key: Some(include_str!("../../../testdata/jwt_rs256_public.pem").to_string()),
        ..crate::runtime::security::SecurityConfig::default()
    };
    let login = |device: &'static str| {
        let authn = &authn;
        let email = user.email.clone();
        async move {
            authn
                .login(Request::new(authn_pb::LoginRequest {
                    username: email,
                    password: "CorrectHorse1!".to_string(),
                    device_name: device.to_string(),
                    ..Default::default()
                }))
                .await
                .expect("login")
                .into_inner()
        }
    };
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };

    // 1. Revoked on this node: refused by the very next call.
    let first = login("verdict-local").await;
    let claims = crate::runtime::security::validate_bearer_token(&security, &first.access_token)
        .expect("decode bearer");
    for _ in 0..3 {
        assert!(
            authn
                .jwt_persisted_state_valid(&claims, now())
                .await
                .unwrap()
        );
    }
    let revoked = authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: first.session_id.clone(),
            revoke_reason: "verdict_cache_live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke session")
        .into_inner();
    assert_eq!(revoked.revoked_count, 1);
    assert!(
        !authn
            .jwt_persisted_state_valid(&claims, now())
            .await
            .unwrap(),
        "a revocation on this node is seen by the next call"
    );

    // 2. Revoked without an event on this node (as another broker node, or an
    // operator in SQL, would): the auth-state version moves at commit and is
    // published to every node, so the change is refused within milliseconds,
    // with no wait for the cache lifetime.
    let second = login("verdict-remote").await;
    let claims = crate::runtime::security::validate_bearer_token(&security, &second.access_token)
        .expect("decode bearer");
    assert!(
        authn
            .jwt_persisted_state_valid(&claims, now())
            .await
            .unwrap()
    );
    let store = PostgresSessionStore::new(pool.clone(), "");
    let jti = claims.jti.clone().expect("session jti");
    let hash = hash_secret(&jti, b"live-auth-test-secret");
    assert!(store.revoke(&hash, now()).await.expect("store revoke"));
    // The change reaches the in-memory version by notification at commit:
    // seen within milliseconds, never after the cache lifetime.
    let changed_at = std::time::Instant::now();
    while authn
        .jwt_persisted_state_valid(&claims, now())
        .await
        .unwrap()
    {
        assert!(
            changed_at.elapsed() < std::time::Duration::from_millis(500),
            "a session revoked elsewhere is refused by the next call (within 500 ms, before the cache lifetime)"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    eprintln!("auth change seen after {:?}", changed_at.elapsed());

    // 3. A user suspended straight in SQL: refused by the next call.
    let third = login("verdict-sql").await;
    let claims = crate::runtime::security::validate_bearer_token(&security, &third.access_token)
        .expect("decode bearer");
    assert!(
        authn
            .jwt_persisted_state_valid(&claims, now())
            .await
            .unwrap()
    );
    let version = || async {
        sqlx::query_scalar::<_, i64>("SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM \"udb_authn\".\"udb_auth_state_version\"")
            .fetch_one(&pool)
            .await
            .expect("read auth-state version")
    };
    // A last_active_at stamp is not a change of auth state: the version stays.
    let before_touch = version().await;
    sqlx::query("UPDATE \"udb_authn\".\"sessions\" SET \"last_active_at\" = now()")
        .execute(&pool)
        .await
        .expect("stamp sessions");
    assert_eq!(
        version().await,
        before_touch,
        "a session touch must not drop every remembered verdict"
    );
    let users = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "status"],
    );
    sqlx::query(&format!(
        "UPDATE {} SET {}='SUSPENDED' WHERE {}=$1::UUID",
        users.relation,
        users.q("status"),
        users.q("user_id")
    ))
    .bind(&user.user_id)
    .execute(&pool)
    .await
    .expect("suspend user in SQL");
    assert!(
        version().await > before_touch,
        "a status change moves the version"
    );
    // The SECURITY DEFINER bump function is not callable by PUBLIC (vault's
    // bounded database credentials refuse a database where one is): the same
    // ACL test vault's issuance audit runs.
    let (installed, public_can_execute): (bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
                        WHERE p.prosecdef AND n.nspname = 'udb_authn' \
                          AND p.proname = 'udb_auth_state_bump'), \
                EXISTS (SELECT 1 FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace \
         CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) acl \
         WHERE p.prosecdef AND n.nspname = 'udb_authn' AND p.proname = 'udb_auth_state_bump' \
           AND acl.grantee = 0 AND acl.privilege_type = 'EXECUTE')",
    )
    .fetch_one(&pool)
    .await
    .expect("read the bump function's ACL");
    assert!(installed, "the auth-state bump function is installed");
    assert!(
        !public_can_execute,
        "PUBLIC must not be able to execute the SECURITY DEFINER auth-state bump"
    );
    // The change reaches the in-memory version by notification at commit:
    // seen within milliseconds, never after the cache lifetime.
    let changed_at = std::time::Instant::now();
    while authn
        .jwt_persisted_state_valid(&claims, now())
        .await
        .unwrap()
    {
        assert!(
            changed_at.elapsed() < std::time::Duration::from_millis(500),
            "a user suspended in SQL is refused by the next call (within 500 ms, before the cache lifetime)"
        );
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    eprintln!("auth change seen after {:?}", changed_at.elapsed());

    cleanup_native_auth_db(&pool).await;
}
