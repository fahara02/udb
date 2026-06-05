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
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_otp_cooldown -- --ignored --nocapture"]
async fn live_postgres_authn_otp_cooldown() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let svc = authn_service_with_cooldown(pool.clone(), 300);
    let user = create_verified_user(&svc, "cooldown", "CorrectHorse1!").await;

    // First sensitive-operation OTP succeeds.
    let first = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "cooldown-1".to_string(),
            ..Default::default()
        }))
        .await
        .expect("first OTP")
        .into_inner();
    assert!(!first.otp_id.is_empty());

    // An immediate second OTP of the same type is throttled by the cooldown.
    let throttled = svc
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "cooldown-2".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("second OTP within cooldown must be rejected");
    assert_eq!(throttled.code(), tonic::Code::ResourceExhausted);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_phone_verification -- --ignored --nocapture"]
async fn live_postgres_phone_verification() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "phone", "CorrectHorse1!").await;

    let sent = authn
        .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
            user_id: user.user_id.clone(),
            phone: "+15551234567".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send phone verification")
        .into_inner();
    assert!(!sent.otp_id.is_empty());

    let wrong = authn
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: sent.otp_id.clone(),
            code: "000000".to_string(),
        }))
        .await
        .expect("wrong phone code")
        .into_inner();
    assert!(!wrong.verified);

    let verified = verify_issued_otp(&authn, &sent.otp_id).await;
    assert!(verified.verified);
    assert_eq!(
        verified.otp_type,
        authn_entity_pb::OtpType::PhoneVerification as i32
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_forgot_and_reset_password -- --ignored --nocapture"]
async fn live_postgres_forgot_and_reset_password() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "forgot", "CorrectHorse1!").await;

    let forgot = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    assert!(!forgot.otp_id.is_empty());

    let reset = authn
        .reset_password(Request::new(authn_pb::ResetPasswordRequest {
            otp_id: forgot.otp_id.clone(),
            code: issued_test_otp_code(&forgot.otp_id),
            new_password: "NewCorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect("reset password")
        .into_inner();
    assert_eq!(reset.user_id, user.user_id);

    // The old password no longer works; the new one does.
    let old = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("old password must fail after reset");
    assert_eq!(old.code(), tonic::Code::Unauthenticated);
    let new = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "NewCorrectHorse1!".to_string(),
            device_name: "reset".to_string(),
            ..Default::default()
        }))
        .await
        .expect("new password login")
        .into_inner();
    assert_eq!(new.user_id, user.user_id);

    // An unknown identifier returns a uniform empty otp_id (no enumeration).
    let unknown = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: "does-not-exist@example.com".to_string(),
            ..Default::default()
        }))
        .await
        .expect("forgot password for unknown account")
        .into_inner();
    assert!(unknown.otp_id.is_empty());

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
