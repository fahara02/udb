use super::support::*;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::authn::AuthnConfig;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_session_token_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authn_session_token_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let apikey = api_key_service(pool.clone());
    let user = create_verified_user(&authn, "session", "CorrectHorse1!").await;

    let session = authn
        .create_session(Request::new(authn_pb::CreateSessionRequest {
            principal: Some(authn_pb::Principal {
                principal_id: user.user_id.clone(),
                subject: user.user_id.clone(),
                user_id: user.user_id.clone(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                scopes: vec!["data:read".to_string()],
                auth_method: "session".to_string(),
                ..Default::default()
            }),
            ttl_seconds: 120,
            client_fingerprint: "live-session".to_string(),
        }))
        .await
        .expect("create direct session")
        .into_inner();
    assert!(session.session_id.starts_with("sess_"));
    assert!(session.expires_at_unix > now_unix() as i64);

    let got_session = authn
        .get_session(Request::new(authn_pb::GetSessionRequest {
            session_id: session.session_id.clone(),
        }))
        .await
        .expect("get direct session")
        .into_inner()
        .session
        .expect("session record");
    assert_eq!(got_session.user_id, user.user_id);

    let validated = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: session.session_id.clone(),
            token_type: authn_entity_pb::TokenType::Session as i32,
        }))
        .await
        .expect("validate session token")
        .into_inner();
    assert!(validated.valid);
    assert_eq!(validated.user_id, user.user_id);
    assert_eq!(validated.project_id, "billing");

    let refreshed = authn
        .refresh_session(Request::new(authn_pb::RefreshSessionRequest {
            session_id: session.session_id.clone(),
            ttl_seconds: 240,
        }))
        .await
        .expect("refresh direct session")
        .into_inner();
    assert!(refreshed.active);
    assert!(refreshed.expires_at_unix >= session.expires_at_unix);

    let refreshed_token = authn
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            session_id: session.session_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("refresh token using server-side session")
        .into_inner();
    assert_eq!(
        refreshed_token.access_token_expires_in,
        AuthnConfig::default().session_ttl_secs as i32
    );

    let (key_owner, _) = create_service_account_with_grant(
        &authn,
        "session_api_key",
        "CorrectHorse1!",
        &["authn:test"],
    )
    .await;
    let created_key = scope_claim_context_for_test(
        test_claim_context(&key_owner.user_id, "acme", "billing", &[], &[]),
        apikey.create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "authn-token-key".to_string(),
            owner_id: key_owner.user_id.clone(),
            scopes: vec!["authn:test".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: key_owner.user_id.clone(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await
    .expect("create API key for authn token validation")
    .into_inner();
    let api_key_token = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: created_key.plain_key,
            token_type: authn_entity_pb::TokenType::ApiKey as i32,
        }))
        .await
        .expect("validate API-key token through authn")
        .into_inner();
    assert!(api_key_token.valid);
    assert_eq!(
        api_key_token
            .principal
            .as_ref()
            .expect("api-key principal")
            .principal_id,
        key_owner.user_id
    );
    assert!(
        api_key_token
            .scopes
            .iter()
            .any(|scope| scope == "authn:test")
    );

    let revoked = authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: session.session_id.clone(),
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke direct session")
        .into_inner();
    assert_eq!(revoked.revoked_count, 1);
    let after_revoke = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: session.session_id,
            token_type: authn_entity_pb::TokenType::Session as i32,
        }))
        .await
        .expect("validate revoked session token")
        .into_inner();
    assert!(!after_revoke.valid);

    let first = authn
        .create_session(Request::new(authn_pb::CreateSessionRequest {
            principal: Some(authn_pb::Principal {
                principal_id: user.user_id.clone(),
                subject: user.user_id.clone(),
                user_id: user.user_id.clone(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                ..Default::default()
            }),
            ttl_seconds: 120,
            client_fingerprint: "bulk-1".to_string(),
        }))
        .await
        .expect("create first bulk session")
        .into_inner();
    let second = authn
        .create_session(Request::new(authn_pb::CreateSessionRequest {
            principal: Some(authn_pb::Principal {
                principal_id: user.user_id.clone(),
                subject: user.user_id.clone(),
                user_id: user.user_id.clone(),
                tenant_id: "acme".to_string(),
                project_id: "billing".to_string(),
                ..Default::default()
            }),
            ttl_seconds: 120,
            client_fingerprint: "bulk-2".to_string(),
        }))
        .await
        .expect("create second bulk session")
        .into_inner();
    assert_ne!(first.session_id, second.session_id);
    let revoked_all = authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            principal_id: user.user_id.clone(),
            all_for_principal: true,
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke all sessions for principal")
        .into_inner();
    assert_eq!(revoked_all.revoked_count, 2);

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "csrf-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login for CSRF token")
        .into_inner();
    assert!(!login.csrf_token.is_empty());
    let csrf_valid = authn
        .validate_csrf(Request::new(authn_pb::ValidateCsrfRequest {
            session_id: login.session_id.clone(),
            csrf_token: login.csrf_token.clone(),
        }))
        .await
        .expect("validate live CSRF token")
        .into_inner();
    assert!(csrf_valid.valid);
    let csrf_invalid = authn
        .validate_csrf(Request::new(authn_pb::ValidateCsrfRequest {
            session_id: login.session_id.clone(),
            csrf_token: "csrf".to_string(),
        }))
        .await
        .expect("reject bad CSRF token")
        .into_inner();
    assert!(!csrf_invalid.valid);
    authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: login.session_id,
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke CSRF login session");

    cleanup_native_auth_db(&pool).await;
}
