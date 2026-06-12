use super::support::*;
use crate::proto::udb::core::apikey::entity::v1 as apikey_entity_pb;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use crate::runtime::service::method_security::{scope_claim_context_for_test, test_claim_context};
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_login_lockout_after_failures -- --ignored --nocapture"]
async fn live_postgres_login_lockout_after_failures() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let user = create_verified_user(&svc, "lockout", "CorrectHorse1!").await;

    for _ in 0..5 {
        let err = svc
            .login(Request::new(authn_pb::LoginRequest {
                username: user.email.clone(),
                password: "WrongPassword9!".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("wrong password must fail");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    let locked = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("correct password must be rejected while locked");
    assert_eq!(locked.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_logout_all_sessions -- --ignored --nocapture"]
async fn live_postgres_logout_all_sessions() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let user = create_verified_user(&svc, "multisession", "CorrectHorse1!").await;

    let mut session_ids = Vec::new();
    for _ in 0..2 {
        let login = svc
            .login(Request::new(authn_pb::LoginRequest {
                username: user.email.clone(),
                password: "CorrectHorse1!".to_string(),
                device_name: "multi".to_string(),
                ..Default::default()
            }))
            .await
            .expect("login")
            .into_inner();
        session_ids.push(login.session_id);
    }

    let active = svc
        .list_sessions(Request::new(authn_pb::ListSessionsRequest {
            user_id: user.user_id.clone(),
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list sessions")
        .into_inner();
    assert_eq!(active.sessions.len(), 2);

    svc.logout(Request::new(authn_pb::LogoutRequest {
        session_id: session_ids[0].clone(),
        all_sessions: true,
        revoke_reason: "live_test".to_string(),
        context: Some(common_pb::RequestContext {
            principal_id: user.user_id.clone(),
            ..Default::default()
        }),
        ..Default::default()
    }))
    .await
    .expect("logout all sessions");

    for session_id in session_ids {
        let err = svc
            .authenticate(Request::new(authn_pb::AuthnRequest {
                session_id,
                ..Default::default()
            }))
            .await
            .expect_err("revoked session must not authenticate");
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authenticate_with_api_key -- --ignored --nocapture"]
async fn live_postgres_authenticate_with_api_key() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let apikeys = api_key_service(pool.clone());
    let owner_id = Uuid::new_v4().to_string();

    let created = apikeys
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "svc-key".to_string(),
            owner_type: apikey_entity_pb::ApiKeyOwnerType::ServiceAccount as i32,
            owner_id: owner_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: owner_id.clone(),
                tenant: Some(common_pb::TenantContext {
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("create api key")
        .into_inner();

    let principal = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: created.plain_key.clone(),
            ..Default::default()
        }))
        .await
        .expect("authenticate with api key")
        .into_inner()
        .principal
        .expect("principal");
    assert_eq!(principal.principal_id, owner_id);

    // Revoke as the authenticated tenant-`acme` admin (validated claim installed
    // by the transport layer over the wire); the tenant guard stays strict.
    scope_claim_context_for_test(
        test_claim_context(&owner_id, "acme", "billing", &[], &[]),
        apikeys.revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
            key_id: created.key.expect("key").key_id,
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("revoke api key");

    let err = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: created.plain_key,
            ..Default::default()
        }))
        .await
        .expect_err("revoked api key must not authenticate");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}
