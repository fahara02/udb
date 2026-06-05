use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::totp;
use tonic::Request;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_mfa_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authn_mfa_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "mfa", "CorrectHorse1!").await;

    let enroll = authn
        .enroll_mfa(Request::new(authn_pb::EnrollMfaRequest {
            user_id: user.user_id.clone(),
            mfa_type: authn_entity_pb::AuthFactorKind::Totp as i32,
            ..Default::default()
        }))
        .await
        .expect("enroll MFA")
        .into_inner();
    assert!(enroll.totp_qr_uri.starts_with("otpauth://"));
    assert!(enroll.verify_otp_id.is_empty());
    let current_totp =
        totp::current_code(&enroll.totp_secret, now_unix()).expect("current TOTP code");
    let wrong_confirm = authn
        .confirm_mfa_enrollment(Request::new(authn_pb::ConfirmMfaEnrollmentRequest {
            user_id: user.user_id.clone(),
            otp_id: enroll.verify_otp_id.clone(),
            code: "000000".to_string(),
            ..Default::default()
        }))
        .await
        .expect("wrong MFA confirm")
        .into_inner();
    assert!(!wrong_confirm.enrolled);
    let confirmed = authn
        .confirm_mfa_enrollment(Request::new(authn_pb::ConfirmMfaEnrollmentRequest {
            user_id: user.user_id.clone(),
            otp_id: enroll.verify_otp_id.clone(),
            code: current_totp.clone(),
            ..Default::default()
        }))
        .await
        .expect("confirm MFA")
        .into_inner();
    assert!(confirmed.enrolled);

    let mfa_prompt = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "mfa".to_string(),
            ..Default::default()
        }))
        .await
        .expect("MFA login prompt")
        .into_inner();
    assert!(mfa_prompt.mfa_required);
    assert!(mfa_prompt.mfa_otp_id.is_empty());
    let mfa_login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            totp_code: current_totp,
            device_name: "mfa-complete".to_string(),
            ..Default::default()
        }))
        .await
        .expect("complete MFA login")
        .into_inner();
    assert_eq!(mfa_login.user_id, user.user_id);
    assert!(mfa_login.session_id.starts_with("sess_"));

    // Regression (MFA bypass): a non-empty mfa_otp_id must NOT skip the password
    // first factor. Both a wrong password and an empty password must be rejected
    // even when an MFA code field is supplied.
    let wrong_pw_with_otp = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "WrongHorse9!".to_string(),
            mfa_otp_id: "any-otp-id".to_string(),
            totp_code: "000000".to_string(),
            device_name: "bypass-attempt".to_string(),
            ..Default::default()
        }))
        .await;
    assert!(
        wrong_pw_with_otp.is_err(),
        "mfa_otp_id must not bypass a wrong password"
    );
    let empty_pw_with_otp = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: String::new(),
            mfa_otp_id: "any-otp-id".to_string(),
            totp_code: "000000".to_string(),
            device_name: "bypass-attempt".to_string(),
            ..Default::default()
        }))
        .await;
    assert!(
        empty_pw_with_otp.is_err(),
        "mfa_otp_id must not bypass an empty password"
    );

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authn_recovery_codes -- --ignored --nocapture"]
async fn live_postgres_authn_recovery_codes() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "recovery", "CorrectHorse1!").await;

    // Enroll + confirm TOTP so the account is MFA-enabled (recovery codes are an
    // alternative second factor).
    let enroll = authn
        .enroll_mfa(Request::new(authn_pb::EnrollMfaRequest {
            user_id: user.user_id.clone(),
            mfa_type: authn_entity_pb::AuthFactorKind::Totp as i32,
            ..Default::default()
        }))
        .await
        .expect("enroll MFA")
        .into_inner();
    let totp_code = totp::current_code(&enroll.totp_secret, now_unix()).expect("totp");
    authn
        .confirm_mfa_enrollment(Request::new(authn_pb::ConfirmMfaEnrollmentRequest {
            user_id: user.user_id.clone(),
            otp_id: enroll.verify_otp_id.clone(),
            code: totp_code,
            ..Default::default()
        }))
        .await
        .expect("confirm MFA");

    let generated = authn
        .generate_recovery_codes(Request::new(authn_pb::GenerateRecoveryCodesRequest {
            user_id: user.user_id.clone(),
            count: 8,
            ..Default::default()
        }))
        .await
        .expect("generate recovery codes")
        .into_inner();
    assert_eq!(generated.generated, 8);
    assert_eq!(generated.codes.len(), 8);
    let code = generated.codes[0].clone();

    // A recovery code satisfies the MFA second factor (password still required).
    let ok = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            recovery_code: code.clone(),
            device_name: "recovery".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login with recovery code")
        .into_inner();
    assert_eq!(ok.user_id, user.user_id);

    // Single-use: the same code cannot be replayed.
    let reuse = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            recovery_code: code,
            device_name: "recovery-reuse".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a consumed recovery code must not log in again");
    assert_eq!(reuse.code(), tonic::Code::Unauthenticated);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_mfa_enforcement_policy -- --ignored --nocapture"]
async fn live_postgres_mfa_enforcement_policy() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let user = create_verified_user(&authn, "mfapolicy", "CorrectHorse1!").await;

    // Tenant requires MFA: a not-yet-enrolled user cannot log in with a password.
    authn
        .put_mfa_policy(Request::new(authn_pb::PutMfaPolicyRequest {
            tenant_id: user.tenant_id.clone(),
            require_mfa: true,
            ..Default::default()
        }))
        .await
        .expect("set require_mfa");
    let blocked = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "policy".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("password-only login must be blocked when the tenant requires MFA");
    assert_eq!(blocked.code(), tonic::Code::FailedPrecondition);

    // Relaxing the policy restores password login.
    authn
        .put_mfa_policy(Request::new(authn_pb::PutMfaPolicyRequest {
            tenant_id: user.tenant_id.clone(),
            require_mfa: false,
            ..Default::default()
        }))
        .await
        .expect("clear require_mfa");
    let ok = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: user.email.clone(),
            password: "CorrectHorse1!".to_string(),
            device_name: "policy-relaxed".to_string(),
            ..Default::default()
        }))
        .await
        .expect("login after policy relaxed")
        .into_inner();
    assert_eq!(ok.user_id, user.user_id);

    cleanup_native_auth_db(&pool).await;
}
