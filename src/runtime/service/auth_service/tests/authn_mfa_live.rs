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

    cleanup_native_auth_db(&pool).await;
}
