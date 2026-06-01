use super::support::*;
use crate::proto::udb::core::apikey::services::v1 as apikey_pb;
use crate::proto::udb::core::apikey::services::v1::api_key_service_server::ApiKeyService;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::common::v1 as common_pb;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_roundtrip -- --ignored --nocapture"]
async fn live_postgres_authn_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let suffix = Uuid::new_v4().simple().to_string();

    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("alice_{suffix}"),
            email: format!("alice_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Alice Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create user through Postgres stores")
        .into_inner();
    let user = created.user.expect("created user");
    assert!(Uuid::parse_str(&user.user_id).is_ok());

    let verified = verify_issued_otp(&svc, &created.otp_id).await;
    assert!(verified.verified);

    let login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "live-db".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login through Postgres stores")
        .into_inner();
    assert_eq!(login.user_id, user.user_id);
    assert!(login.session_id.starts_with("sess_"));

    let authed = svc
        .authenticate(Request::new(authn_pb::AuthnRequest {
            session_id: login.session_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("authenticate Postgres-backed session")
        .into_inner()
        .principal
        .expect("principal");
    assert_eq!(authed.user_id, user.user_id);
    assert_eq!(authed.tenant_id, "acme");

    let sessions = svc
        .list_sessions(Request::new(authn_pb::ListSessionsRequest {
            user_id: user.user_id.clone(),
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list Postgres sessions")
        .into_inner();
    assert_eq!(sessions.sessions.len(), 1);

    svc.logout(Request::new(authn_pb::LogoutRequest {
        session_id: login.session_id.clone(),
        revoke_reason: "live_test".to_string(),
        ..Default::default()
    }))
    .await
    .expect("logout Postgres-backed session");

    let err = svc
        .authenticate(Request::new(authn_pb::AuthnRequest {
            session_id: login.session_id,
            ..Default::default()
        }))
        .await
        .expect_err("revoked Postgres session must not authenticate");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_authenticate_api_key -- --ignored --nocapture"]
async fn live_postgres_authn_authenticate_api_key() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let apikey = api_key_service(pool.clone());
    let user = create_verified_user(&authn, "authn_apikey", "CorrectHorse1!").await;

    let created = apikey
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "authenticate-key".to_string(),
            owner_id: user.user_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: user.user_id.clone(),
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
        .expect("create API key for authenticate")
        .into_inner();

    let principal = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: created.plain_key,
            ..Default::default()
        }))
        .await
        .expect("authenticate via API key")
        .into_inner()
        .principal
        .expect("api-key principal");
    assert_eq!(principal.principal_id, user.user_id);
    assert!(principal.scopes.iter().any(|scope| scope == "data:read"));

    let err = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: "udbk_unknown.bogus".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("unknown API key must be rejected");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_revoked_session_cannot_obtain_broker_principal -- --ignored --nocapture"]
async fn live_postgres_revoked_session_cannot_obtain_broker_principal() {
    // Item 129: a live server-side session mints a broker-ready principal via
    // AuthnService.Authenticate; once the session is revoked, the same session id
    // resolves to no principal (Unauthenticated), so a revoked session can no
    // longer obtain a principal to call the broker.
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "revoked_session", "CorrectHorse1!").await;

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
            client_fingerprint: "broker-principal".to_string(),
        }))
        .await
        .expect("create live session")
        .into_inner();

    // Pre-revocation: the live session resolves to a broker-ready principal.
    let principal = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            session_id: session.session_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("authenticate live session")
        .into_inner()
        .principal
        .expect("live session must yield a broker principal");
    assert_eq!(principal.user_id, user.user_id);
    assert_eq!(principal.tenant_id, "acme");

    authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: session.session_id.clone(),
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke live session");

    let err = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            session_id: session.session_id,
            ..Default::default()
        }))
        .await
        .expect_err("revoked session must not yield a broker principal");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_revoked_api_key_cannot_obtain_broker_principal -- --ignored --nocapture"]
async fn live_postgres_revoked_api_key_cannot_obtain_broker_principal() {
    // Item 130: a live API key authenticates to a broker-ready principal; once
    // revoked through RevokeApiKey, the same key resolves to no principal. This
    // exercises an actually-revoked key (distinct from the bogus-key rejection
    // above), proving revocation — not just non-existence — closes broker access.
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let apikey = api_key_service(pool.clone());
    let user = create_verified_user(&authn, "revoked_apikey", "CorrectHorse1!").await;

    let created = apikey
        .create_api_key(Request::new(apikey_pb::CreateApiKeyRequest {
            name: "revoke-broker-key".to_string(),
            owner_id: user.user_id.clone(),
            scopes: vec!["data:read".to_string()],
            context: Some(common_pb::RequestContext {
                principal_id: user.user_id.clone(),
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
        .expect("create live API key")
        .into_inner();
    let key_id = created.key.as_ref().expect("created key").key_id.clone();

    // Pre-revocation: the live key resolves to a broker-ready principal.
    let principal = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: created.plain_key.clone(),
            ..Default::default()
        }))
        .await
        .expect("authenticate live API key")
        .into_inner()
        .principal
        .expect("live API key must yield a broker principal");
    assert_eq!(principal.principal_id, user.user_id);

    apikey
        .revoke_api_key(Request::new(apikey_pb::RevokeApiKeyRequest {
            key_id,
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke live API key");

    let err = authn
        .authenticate(Request::new(authn_pb::AuthnRequest {
            api_key: created.plain_key,
            ..Default::default()
        }))
        .await
        .expect_err("revoked API key must not yield a broker principal");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}
