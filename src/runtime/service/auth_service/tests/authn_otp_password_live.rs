use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_otp_password_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authn_otp_password_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service(pool.clone());
    let user = create_verified_user(&svc, "password", "CorrectHorse1!").await;

    let sent = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "password-change".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send sensitive-operation OTP")
        .into_inner();
    let wrong = svc
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: "000000".to_string(),
        }))
        .await
        .expect("wrong OTP check")
        .into_inner();
    assert!(!wrong.verified);
    let verified = verify_issued_otp(&svc, &sent.otp_id).await;
    assert!(verified.verified);

    let changed = svc
        .change_password(Request::new(authn_pb::ChangePasswordRequest {
            user_id: user.user_id.clone(),
            current_password: "CorrectHorse1!".to_string(),
            new_password: "NewCorrectHorse1!".to_string(),
            otp_id: sent.otp_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("change password with verified OTP")
        .into_inner();
    assert_eq!(changed.user_id, user.user_id);

    let reused = svc
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: issued_test_otp_code(&sent.otp_id),
        }))
        .await
        .expect("used OTP check")
        .into_inner();
    assert!(!reused.verified);

    let old_login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("old password must fail");
    assert_eq!(old_login.code(), tonic::Code::Unauthenticated);

    let new_login = svc
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email,
            password: "NewCorrectHorse1!".to_string(),
            device_name: "password-lifecycle".to_string(),
            ..Default::default()
        }))
        .await
        .expect("new password login")
        .into_inner();
    assert_eq!(new_login.user_id, user.user_id);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_resend_otp_and_admin_reset -- --ignored --nocapture"]
async fn live_postgres_authn_resend_otp_and_admin_reset() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let suffix = Uuid::new_v4().simple().to_string();

    let created = authn
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("resend_{suffix}"),
            email: format!("resend_{suffix}@example.com"),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            full_name: "Resend Live".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create user")
        .into_inner();
    let user = created.user.expect("created user");

    let resent = authn
        .resend_otp(Request::new(authn_pb::ResendOtpRequest {
            original_otp_id: created.otp_id.clone(),
            reason: "not_received".to_string(),
        }))
        .await
        .expect("resend OTP")
        .into_inner();
    assert!(!resent.otp_id.is_empty());
    assert_ne!(resent.otp_id, created.otp_id);

    let verified = verify_issued_otp(&authn, &resent.otp_id).await;
    assert!(verified.verified);

    let reset = authn
        .admin_reset_password(Request::new(authn_pb::AdminResetPasswordRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("admin reset password")
        .into_inner();
    assert!(!reset.otp_id.is_empty());

    cleanup_native_auth_db(&pool).await;
}
