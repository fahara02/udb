use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::{ErrorDetail, ErrorKind};
use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
use tonic::Request;
use uuid::Uuid;

fn decode_detail(status: &tonic::Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

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
    let detail = decode_detail(&throttled);
    assert_eq!(detail.kind, ErrorKind::Quota as i32);
    assert!(detail.retryable);
    assert_eq!(detail.backend, "authn");
    assert_eq!(detail.operation, "otp_cooldown");
    assert!(detail.retry_after_ms > 0);

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

    // Enumeration safety: an unknown identifier returns a response with the SAME
    // shape as a known one — a non-empty otp_id (a throwaway handle that maps to
    // no real OTP). Returning an *empty* otp_id for unknown accounts while known
    // accounts get a non-empty one would itself be an enumeration oracle, so the
    // handler emits a uniform non-empty otp_id either way.
    let unknown = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: "does-not-exist@example.com".to_string(),
            ..Default::default()
        }))
        .await
        .expect("forgot password for unknown account")
        .into_inner();
    assert!(
        !unknown.otp_id.is_empty(),
        "unknown-account forgot_password must return a uniform non-empty otp_id (no enumeration)"
    );

    cleanup_native_auth_db(&pool).await;
}

/// §8A acceptance: production builds do not expose bypass material. Pins that the
/// single dev-echo chokepoint (`mfa::otp_dev_echo_enabled`) is prod-closed for ALL
/// three OTP issuance sites and, when the dev gate is on, every site DOES surface
/// the proof so a headless conformance harness can complete verification.
///
/// The prod-closed property is asserted deterministically through the pure
/// `otp_dev_echo_resolved(env_opt_in, is_production)` decision (the process-wide
/// `OnceLock` posture cannot be toggled mid-run). The dev-on, served-path half runs
/// only when the gate actually resolved enabled in this process
/// (`UDB_OTP_DEV_ECHO=1` + non-production posture), exactly mirroring how a live
/// conformance harness is launched.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_OTP_DEV_ECHO=1 UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_otp_dev_echo_prod_closed -- --ignored --nocapture"]
async fn live_postgres_otp_dev_echo_prod_closed() {
    use crate::runtime::service::auth_service::authn::{
        otp_dev_echo_enabled, otp_dev_echo_resolved,
    };

    // Production posture closes the echo for every site regardless of the env
    // opt-in; dev posture honours it. Reverting 13.1.1.1 (dropping the
    // `!is_production` AND) flips the first assertion and fails the test.
    assert!(
        !otp_dev_echo_resolved(true, true),
        "production posture must close the OTP dev-echo even with UDB_OTP_DEV_ECHO=1 (proof-material leak)"
    );
    assert!(
        otp_dev_echo_resolved(true, false),
        "non-production posture with the env opt-in must permit the dev-echo"
    );
    assert!(
        !otp_dev_echo_resolved(false, false),
        "without the env opt-in the dev-echo stays closed"
    );

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());

    if !otp_dev_echo_enabled() {
        // The process resolved the gate OFF (no env opt-in or production posture):
        // every served site must return an EMPTY echo — proving prod/no-opt-in
        // builds never leak proof material on the served path.
        let user = create_verified_user(&authn, "echo_off", "CorrectHorse1!").await;
        let sent = authn
            .send_otp(Request::new(authn_pb::SendOtpRequest {
                user_id: user.user_id.clone(),
                otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
                correlation_id: "echo-off".to_string(),
                ..Default::default()
            }))
            .await
            .expect("send OTP")
            .into_inner();
        assert!(
            sent.dev_otp_code.is_empty(),
            "SendOTP must NOT echo when the dev gate is closed"
        );
        let forgot = authn
            .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
                identifier: user.email.clone(),
                ..Default::default()
            }))
            .await
            .expect("forgot password")
            .into_inner();
        assert!(
            forgot.dev_otp_code.is_empty(),
            "ForgotPassword must NOT echo when the dev gate is closed"
        );
        let phone = authn
            .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
                user_id: user.user_id.clone(),
                phone: "+15557654321".to_string(),
                ..Default::default()
            }))
            .await
            .expect("send phone verification")
            .into_inner();
        assert!(
            phone.dev_otp_code.is_empty(),
            "SendPhoneVerification must NOT echo when the dev gate is closed"
        );
        cleanup_native_auth_db(&pool).await;
        return;
    }

    // Dev gate ON: every site must surface the real plaintext code AND that code
    // must verify, proving the echo is genuine conformance proof, not noise.
    let user = create_verified_user(&authn, "echo_on", "CorrectHorse1!").await;

    let sent = authn
        .send_otp(Request::new(authn_pb::SendOtpRequest {
            user_id: user.user_id.clone(),
            otp_type: authn_entity_pb::OtpType::SensitiveOperation as i32,
            correlation_id: "echo-on".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send OTP")
        .into_inner();
    assert!(
        !sent.dev_otp_code.is_empty(),
        "SendOTP must echo the code under the dev gate"
    );
    assert_eq!(sent.dev_otp_code, issued_test_otp_code(&sent.otp_id));

    let forgot = authn
        .forgot_password(Request::new(authn_pb::ForgotPasswordRequest {
            identifier: user.email.clone(),
            ..Default::default()
        }))
        .await
        .expect("forgot password")
        .into_inner();
    assert!(
        !forgot.dev_otp_code.is_empty(),
        "ForgotPassword must echo the reset code under the dev gate"
    );
    assert_eq!(forgot.dev_otp_code, issued_test_otp_code(&forgot.otp_id));

    let phone = authn
        .send_phone_verification(Request::new(authn_pb::SendPhoneVerificationRequest {
            user_id: user.user_id.clone(),
            phone: "+15557654321".to_string(),
            ..Default::default()
        }))
        .await
        .expect("send phone verification")
        .into_inner();
    assert!(
        !phone.dev_otp_code.is_empty(),
        "SendPhoneVerification must echo the code under the dev gate"
    );
    assert_eq!(phone.dev_otp_code, issued_test_otp_code(&phone.otp_id));

    // The echoed phone code actually verifies the PHONE_VERIFICATION row.
    let verified = authn
        .verify_otp(Request::new(authn_pb::VerifyOtpRequest {
            otp_id: phone.otp_id.clone(),
            code: phone.dev_otp_code.clone(),
        }))
        .await
        .expect("verify echoed phone code")
        .into_inner();
    assert!(verified.verified);

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
