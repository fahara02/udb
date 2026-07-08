//! Live fault-injection tests for native auth services.
//!
//! These tests deliberately break one dependency after the generated native DDL is
//! applied, then drive the normal served handler. The point is to prove the handler
//! fails closed and does not leave a partially committed row behind.

use super::support::*;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::native_catalog::{native_model, native_relation};
use tonic::Request;

fn relation(message_type: &str) -> String {
    let (schema, table) = native_relation(message_type)
        .unwrap_or_else(|| panic!("missing native relation for {message_type}"));
    format!("{schema}.{table}")
}

async fn stored_last_login_at_unix(pool: &sqlx::PgPool, user_id: &str) -> i64 {
    let users_rel = relation("udb.core.authn.entity.v1.User");
    let users_model = native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "last_login_at"],
    );
    let user_id_col = users_model.column("user_id");
    let last_login_col = users_model.column("last_login_at");
    sqlx::query_scalar(&format!(
        "SELECT COALESCE(EXTRACT(EPOCH FROM {last_login_col})::BIGINT, 0) \
         FROM {users_rel} WHERE {user_id_col} = $1"
    ))
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("read persisted last_login_at")
}

/// Fault injection: drop the OTP table after startup migration, then call the
/// real `CreateUser` handler. `CreateUser` writes the user and verification OTP
/// in one transaction; losing the OTP table must make the RPC fail and roll back
/// the user insert rather than leaving an unverified orphan user.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_create_user_rolls_back_when_otp_store_fails -- --ignored --nocapture"]
async fn live_postgres_create_user_rolls_back_when_otp_store_fails() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let users_rel = relation("udb.core.authn.entity.v1.User");
    let users_model = native_model("udb.core.authn.entity.v1.User", &["email"]);
    let email_col = users_model.column("email");
    let otps_rel = relation("udb.core.authn.entity.v1.OTP");

    sqlx::query(&format!("DROP TABLE {otps_rel} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop OTP table to inject store fault");

    let svc = authn_service(pool.clone());
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let email = format!("fault_{suffix}@example.com");
    let err = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("fault_{suffix}"),
            email: email.clone(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Fault Injection".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("create_user must fail when the OTP store is unavailable");
    assert_eq!(
        err.code(),
        tonic::Code::Internal,
        "OTP-store fault should surface as a server-side failure"
    );

    let persisted: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {users_rel} WHERE {email_col} = $1"
    ))
    .bind(&email)
    .fetch_one(&pool)
    .await
    .expect("count users after injected OTP-store fault");
    assert_eq!(
        persisted, 0,
        "CreateUser must roll back the user row when OTP persistence fails"
    );

    cleanup_native_auth_db(&pool).await;
}

/// Fault injection: after a verified user exists, drop the generated Session
/// table and call the real `Login` handler. Login stamps successful state on the
/// user before minting a session; if the session store fails, those user changes
/// must be compensated so the failed login leaves no successful-login marker.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_login_restores_user_state_when_session_store_fails -- --ignored --nocapture"]
async fn live_postgres_login_restores_user_state_when_session_store_fails() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    let healthy = authn_service(pool.clone());
    let user = create_verified_user(&healthy, "faultsession", "CorrectHorse1!").await;
    let before = stored_last_login_at_unix(&pool, &user.user_id).await;

    let sessions_rel = relation("udb.core.authn.entity.v1.Session");
    sqlx::query(&format!("DROP TABLE {sessions_rel} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop Session table to inject store fault");

    let err = healthy
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            tenant_hint: "acme".to_string(),
            device_name: "fault-session".to_string(),
            ip_address: "127.0.0.1".to_string(),
            user_agent: "udb-fault-injection".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("login must fail when the session store is unavailable");
    assert_eq!(
        err.code(),
        tonic::Code::Internal,
        "session-store fault should surface as a server-side failure"
    );

    let after = stored_last_login_at_unix(&pool, &user.user_id).await;
    assert_eq!(
        after, before,
        "failed Login must restore the user's successful-login timestamp"
    );

    cleanup_native_auth_db(&pool).await;
}
