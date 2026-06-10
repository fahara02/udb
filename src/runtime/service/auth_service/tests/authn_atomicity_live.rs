//! Live Postgres tests proving enterprise auth-mutation atomicity (final_task.md
//! §7 / Phase 6): an auth mutation and its audit/outbox event commit together, or
//! neither does. Each test injects a [`FailingAuthEventSink`] whose transactional
//! write always fails, then asserts the handler propagated the error AND that the
//! durable mutation was rolled back (the DB is exactly as it was before the call).

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::native_catalog::{native_model, native_relation};
use tonic::Request;

/// Resolve `schema.table` for a native message type (proto-driven, not hardcoded).
fn relation(message_type: &str) -> String {
    let (schema, table) = native_relation(message_type)
        .unwrap_or_else(|| panic!("missing native relation for {message_type}"));
    format!("{schema}.{table}")
}

/// `create_user`: a forced audit-event write failure inside the creation
/// transaction must abort the user upsert AND the email-verification OTP insert —
/// no orphaned user row, no orphaned OTP row.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_create_user_event_failure_is_atomic -- --ignored --nocapture"]
async fn live_postgres_authn_create_user_event_failure_is_atomic() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_failing_sink(pool.clone());

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let email = format!("atomic_{suffix}@example.com");
    let result = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("atomic_{suffix}"),
            email: email.clone(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Atomic Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await;
    // The handler must surface the audit-write failure rather than committing.
    assert!(
        result.is_err(),
        "create_user must fail when its in-tx audit write fails"
    );

    // No user row may persist — the upsert rolled back with the event.
    let users_rel = relation("udb.core.authn.entity.v1.User");
    let user_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {users_rel} WHERE email = $1"
    ))
    .bind(&email)
    .fetch_one(&pool)
    .await
    .expect("count users after failed create");
    assert_eq!(
        user_count, 0,
        "no user row may persist when the create_user audit write failed"
    );

    // No OTP row may have leaked from the rolled-back transaction. The OTP's
    // delivery_address is the user's email (email-verification channel).
    let otps_rel = relation("udb.core.authn.entity.v1.OTP");
    let otp_model = native_model("udb.core.authn.entity.v1.OTP", &["delivery_address"]);
    let addr_col = otp_model.column("delivery_address");
    let otp_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {otps_rel} WHERE {addr_col} = $1"
    ))
    .bind(&email)
    .fetch_one(&pool)
    .await
    .expect("count otps after failed create");
    assert_eq!(
        otp_count, 0,
        "no OTP row may persist when create_user rolled back"
    );
}

/// `change_user_status`: a forced audit-event write failure must roll back the
/// status UPDATE — the user keeps its prior status.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_change_status_event_failure_is_atomic -- --ignored --nocapture"]
async fn live_postgres_authn_change_status_event_failure_is_atomic() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // Create + verify a user with the healthy service so it lands ACTIVE.
    let healthy = authn_service(pool.clone());
    let user = create_verified_user(&healthy, "atomicstatus", "CorrectHorse1!").await;
    assert_eq!(user.status, authn_entity_pb::UserStatus::Active as i32);

    // Attempt a status change with the failing-sink service.
    let failing = authn_service_with_failing_sink(pool.clone());
    let result = failing
        .change_user_status(Request::new(authn_pb::ChangeUserStatusRequest {
            user_id: user.user_id.clone(),
            new_status: authn_entity_pb::UserStatus::Suspended as i32,
            reason: "atomicity_test".to_string(),
            ..Default::default()
        }))
        .await;
    assert!(
        result.is_err(),
        "change_user_status must fail when its in-tx audit write fails"
    );

    // The status UPDATE must have rolled back: the user is still ACTIVE.
    let reread = healthy
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("re-read user after failed status change")
        .into_inner()
        .user
        .expect("user still present");
    assert_eq!(
        reread.status,
        authn_entity_pb::UserStatus::Active as i32,
        "status change must roll back when its audit write fails"
    );
}

/// `emergency_revoke`: a forced audit-event write failure must roll back every
/// durable break-glass mutation. Here we revoke a principal's sessions; after the
/// failed call the user's session must still be active.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_emergency_revoke_event_failure_is_atomic -- --ignored --nocapture"]
async fn live_postgres_authn_emergency_revoke_event_failure_is_atomic() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;

    // Establish a logged-in principal with an active session.
    let healthy = authn_service(pool.clone());
    let user = create_verified_user(&healthy, "atomicrevoke", "CorrectHorse1!").await;
    healthy
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            tenant_hint: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login");

    let sessions_rel = relation("udb.core.authn.entity.v1.Session");
    let session_model = native_model(
        "udb.core.authn.entity.v1.Session",
        &["principal_id", "is_active"],
    );
    let principal_col = session_model.column("principal_id");
    let active_col = session_model.column("is_active");
    let count_active = format!(
        "SELECT COUNT(*) FROM {sessions_rel} WHERE {principal_col} = $1 AND {active_col} = TRUE"
    );
    let active_before: i64 = sqlx::query_scalar(&count_active)
        .bind(&user.user_id)
        .fetch_one(&pool)
        .await
        .expect("count active sessions before emergency revoke");
    assert!(
        active_before > 0,
        "expected at least one active session before emergency revoke"
    );

    // Emergency-revoke the principal with the failing-sink service.
    let failing = authn_service_with_failing_sink(pool.clone());
    let result = failing
        .emergency_revoke(Request::new(authn_pb::EmergencyRevokeRequest {
            principal_id: user.user_id.clone(),
            reason: "atomicity_test".to_string(),
            ..Default::default()
        }))
        .await;
    assert!(
        result.is_err(),
        "emergency_revoke must fail when its in-tx audit write fails"
    );

    // The session-revoke UPDATE must have rolled back with the event.
    let active_after: i64 = sqlx::query_scalar(&count_active)
        .bind(&user.user_id)
        .fetch_one(&pool)
        .await
        .expect("count active sessions after failed emergency revoke");
    assert_eq!(
        active_after, active_before,
        "emergency_revoke must roll back the session revoke when its audit write fails"
    );
}
