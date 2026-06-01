use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{
    AuthnConfig, PostgresApiKeyStore, PostgresUserStore, RedisSessionStore, SessionStore,
};
use crate::runtime::security::SecurityConfig;
use std::sync::Arc;
use tonic::Request;

fn live_redis_url() -> String {
    std::env::var("UDB_LIVE_AUTH_REDIS_URL")
        .or_else(|_| std::env::var("UDB_INTEGRATION_REDIS_URL"))
        .unwrap_or_else(|_| "redis://127.0.0.1:56379".to_string())
}

async fn flush_live_redis(url: &str) {
    let client = redis::Client::open(url).expect("create live redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect live redis");
    let _: String = redis::cmd("FLUSHDB")
        .query_async(&mut conn)
        .await
        .expect("flush live redis db");
}

#[tokio::test]
#[ignore = "requires live Postgres + Redis; run with UDB_LIVE_AUTH_TESTS=1 UDB_INTEGRATION_REDIS_URL=redis://127.0.0.1:56379 cargo test --lib live_redis_authn_session_store_roundtrip -- --ignored --nocapture"]
async fn live_redis_authn_session_store_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let redis_url = live_redis_url();
    flush_live_redis(&redis_url).await;

    let config = AuthnConfig {
        session_enabled: true,
        session_hash_secret: "live-auth-test-secret".to_string(),
        session_backend: "redis".to_string(),
        session_ttl_secs: 120,
        ..AuthnConfig::default()
    };
    let sessions: Arc<dyn SessionStore> = Arc::new(
        RedisSessionStore::from_url(&redis_url, "udb:authn:live-test", 120)
            .expect("build Redis session store"),
    );
    let authn = super::super::AuthnServiceImpl::with_stores(
        config,
        SecurityConfig::current(),
        sessions,
        Arc::new(PostgresApiKeyStore::new(pool.clone(), "")),
        Arc::new(PostgresUserStore::new(pool.clone(), "")),
    );
    let user = create_verified_user(&authn, "redis_session", "CorrectHorse1!").await;

    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "redis-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login with Redis sessions")
        .into_inner();
    assert!(login.session_id.starts_with("sess_"));

    let valid = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.session_id.clone(),
            token_type: authn_entity_pb::TokenType::Session as i32,
        }))
        .await
        .expect("validate Redis-backed session")
        .into_inner();
    assert!(valid.valid);
    assert_eq!(valid.user_id, user.user_id);

    let listed = authn
        .list_sessions(Request::new(authn_pb::ListSessionsRequest {
            user_id: user.user_id.clone(),
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list Redis-backed sessions")
        .into_inner();
    assert_eq!(listed.sessions.len(), 1);

    let sessions_model = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.Session",
        &["session_token_lookup"],
    );
    let postgres_session_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*)::bigint FROM {}",
        sessions_model.relation
    ))
    .fetch_one(&pool)
    .await
    .expect("count Postgres sessions");
    assert_eq!(
        postgres_session_count, 0,
        "Redis-backed authn must not persist sessions in Postgres"
    );

    authn
        .revoke_session(Request::new(authn_pb::RevokeSessionRequest {
            session_id: login.session_id.clone(),
            revoke_reason: "live_test".to_string(),
            ..Default::default()
        }))
        .await
        .expect("revoke Redis-backed session");
    let after_revoke = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.session_id,
            token_type: authn_entity_pb::TokenType::Session as i32,
        }))
        .await
        .expect("validate revoked Redis-backed session")
        .into_inner();
    assert!(!after_revoke.valid);

    flush_live_redis(&redis_url).await;
    cleanup_native_auth_db(&pool).await;
}
