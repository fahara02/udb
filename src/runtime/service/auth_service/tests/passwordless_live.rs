//! 0.6.6 passwordless sign-in against live Postgres: the tenant auth policy,
//! SendLoginOtp / LoginWithOtp end to end over SMS (the operator webhook is
//! the SMS channel) and by magic link, single use, the uniform answer for an
//! unknown address, and nothing sent for a channel the tenant did not enable.

use super::super::AuthnServiceImpl;
use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::authn::{PostgresUserStore, UserStore};
use crate::runtime::service::live_tests::ops_seams_live::EnvRestore;
use tonic::Request;
use uuid::Uuid;

/// Whether the test build captured a delivered code for `otp_id` (every real
/// delivery records one; a uniform miss records none).
fn code_was_delivered(otp_id: &str) -> bool {
    super::super::authn::test_otp_codes()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains_key(otp_id)
}

/// The stored phone and whether it is verified, read from the user row.
async fn stored_phone(pool: &sqlx::PgPool, user_id: &str) -> (String, bool) {
    let users = crate::runtime::native_catalog::native_model(
        "udb.core.authn.entity.v1.User",
        &["user_id", "phone", "phone_verified_at"],
    );
    let row: (Option<String>, bool) = sqlx::query_as(&format!(
        "SELECT {phone}, {verified} IS NOT NULL FROM {rel} WHERE {uid} = $1::UUID",
        phone = users.q("phone"),
        verified = users.q("phone_verified_at"),
        rel = users.relation,
        uid = users.q("user_id"),
    ))
    .bind(user_id)
    .fetch_one(pool)
    .await
    .expect("read stored phone");
    (row.0.unwrap_or_default(), row.1)
}

async fn put_policy(
    svc: &AuthnServiceImpl,
    tenant_id: &str,
    password_required: bool,
    channels: &[&str],
    magic_link_base_url: &str,
) -> authn_entity_pb::TenantAuthPolicy {
    svc.put_tenant_auth_policy(Request::new(authn_pb::PutTenantAuthPolicyRequest {
        tenant_id: tenant_id.to_string(),
        password_required: Some(password_required),
        passwordless_channels: channels.iter().map(|c| c.to_string()).collect(),
        magic_link_base_url: magic_link_base_url.to_string(),
        ..Default::default()
    }))
    .await
    .expect("put tenant auth policy")
    .into_inner()
    .policy
    .expect("stored policy")
}

async fn send_code(
    svc: &AuthnServiceImpl,
    tenant_hint: &str,
    channel: &str,
    address: &str,
    magic_link: bool,
) -> authn_pb::SendLoginOtpResponse {
    svc.send_login_otp(Request::new(authn_pb::SendLoginOtpRequest {
        tenant_hint: tenant_hint.to_string(),
        channel: channel.to_string(),
        address: address.to_string(),
        magic_link,
        ..Default::default()
    }))
    .await
    .expect("SendLoginOtp always answers a well-formed request")
    .into_inner()
}

async fn login_with(
    svc: &AuthnServiceImpl,
    otp_id: &str,
    code: &str,
) -> Result<authn_pb::LoginWithOtpResponse, tonic::Status> {
    svc.login_with_otp(Request::new(authn_pb::LoginWithOtpRequest {
        otp_id: otp_id.to_string(),
        code: code.to_string(),
        device_name: "passwordless-live".to_string(),
        ip_address: "203.0.113.7".to_string(),
        user_agent: "passwordless-live/1".to_string(),
        ..Default::default()
    }))
    .await
    .map(tonic::Response::into_inner)
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_postgres_sms_login_otp_end_to_end() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    // The operator webhook is the SMS channel. Delivery is best-effort
    // (nothing listens on the discard port); the test build captures each
    // delivered code.
    let _webhook = EnvRestore::set("UDB_OTP_DELIVERY_WEBHOOK_URL", "http://127.0.0.1:9/otp");
    let svc = authn_service_with_jwt(pool.clone());
    let store = PostgresUserStore::new(pool.clone(), "");

    // A tenant with no policy reads the defaults.
    let default = svc
        .get_tenant_auth_policy(Request::new(authn_pb::GetTenantAuthPolicyRequest {
            tenant_id: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("get default policy")
        .into_inner();
    assert!(default.is_default);
    let default = default.policy.expect("default policy");
    assert!(default.password_required);
    assert_eq!(default.revision, 0);

    // Passwords off, SMS sign-in on.
    let policy = put_policy(&svc, "acme", false, &["sms"], "").await;
    assert_eq!(policy.revision, 1);
    assert!(!policy.password_required);
    let stored = svc
        .get_tenant_auth_policy(Request::new(authn_pb::GetTenantAuthPolicyRequest {
            tenant_id: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("get stored policy")
        .into_inner();
    assert!(!stored.is_default);
    let channels: Vec<String> = serde_json::from_str(
        &stored
            .policy
            .expect("stored policy")
            .passwordless_channels_json,
    )
    .expect("channels json");
    assert_eq!(channels, vec!["SMS".to_string()]);

    // A stale expected_revision is refused; nothing changes.
    let conflict = svc
        .put_tenant_auth_policy(Request::new(authn_pb::PutTenantAuthPolicyRequest {
            tenant_id: "acme".to_string(),
            password_required: Some(true),
            expected_revision: 7,
            ..Default::default()
        }))
        .await
        .expect_err("stale revision");
    assert_eq!(conflict.code(), tonic::Code::Aborted);

    // CreateUser with no password: accepted because the tenant allows it.
    let phone = "+8801712345601";
    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("otp_only_{}", Uuid::new_v4().simple()),
            phone: phone.to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("passwordless CreateUser")
        .into_inner();
    let user = created.user.expect("created user");
    assert_eq!(
        user.status,
        authn_entity_pb::UserStatus::PendingVerification as i32
    );
    assert_eq!(
        stored_phone(&pool, &user.user_id).await,
        (phone.to_string(), false)
    );

    // Send the code by SMS and sign in with it.
    let sent = send_code(&svc, "acme", "SMS", phone, false).await;
    assert!(
        code_was_delivered(&sent.otp_id),
        "a real account gets a code"
    );
    assert!(sent.expires_at_unix > now_unix() as i64);
    let otp = store
        .get_otp(&sent.otp_id)
        .await
        .expect("otp lookup")
        .expect("the code is persisted");
    assert_eq!(otp.otp_type, authn_entity_pb::OtpType::Login as i32);
    assert_eq!(otp.delivery_channel, "sms");
    assert_eq!(otp.delivery_address, phone);
    let code = issued_test_otp_code(&sent.otp_id);

    // A wrong guess is refused with the one opaque answer.
    let wrong = login_with(&svc, &sent.otp_id, "000000x")
        .await
        .expect_err("wrong code");
    assert_eq!(wrong.code(), tonic::Code::Unauthenticated);

    let signed_in = login_with(&svc, &sent.otp_id, &code)
        .await
        .expect("sign in with the SMS code");
    assert_eq!(signed_in.user_id, user.user_id);
    assert!(!signed_in.access_token.is_empty());
    assert!(signed_in.refresh_token.starts_with("rt_"));
    assert!(signed_in.refresh_token_expires_in > 0);
    assert!(signed_in.session_public_id.starts_with("sesspub_"));
    assert!(!signed_in.csrf_token.is_empty());
    assert!(!signed_in.mfa_required);

    // The issued bearer validates, and the refresh family rotates.
    let validated = svc
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: signed_in.access_token.clone(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate the OTP sign-in bearer")
        .into_inner();
    assert!(validated.valid);
    assert_eq!(validated.user_id, user.user_id);
    let rotated = svc
        .refresh_token(Request::new(authn_pb::RefreshTokenRequest {
            refresh_token: signed_in.refresh_token.clone(),
            ..Default::default()
        }))
        .await
        .expect("rotate the OTP sign-in refresh token")
        .into_inner();
    assert!(!rotated.access_token.is_empty());

    // Proving the phone verified it and activated the phone-only account.
    assert!(stored_phone(&pool, &user.user_id).await.1);
    let activated = svc
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("re-read user")
        .into_inner()
        .user
        .expect("user present");
    assert_eq!(activated.status, authn_entity_pb::UserStatus::Active as i32);

    // Single use: the same code never signs in twice.
    let replay = login_with(&svc, &sent.otp_id, &code)
        .await
        .expect_err("a used code is refused");
    assert_eq!(replay.code(), tonic::Code::Unauthenticated);
    assert_eq!(replay.message(), wrong.message());

    // Unknown address: the same answer shape, nothing persisted or sent, and
    // its id signs no one in.
    let unknown = send_code(&svc, "acme", "SMS", "+8801712345699", false).await;
    assert!(Uuid::parse_str(&unknown.otp_id).is_ok());
    assert!(unknown.expires_at_unix > now_unix() as i64);
    assert!(unknown.dev_otp_code.is_empty());
    if !super::super::authn::otp_dev_echo_enabled() {
        assert!(sent.dev_otp_code.is_empty());
    }
    assert!(!code_was_delivered(&unknown.otp_id));
    assert!(store.get_otp(&unknown.otp_id).await.unwrap().is_none());
    let fake = login_with(&svc, &unknown.otp_id, "123456")
        .await
        .expect_err("a miss id never verifies");
    assert_eq!(fake.code(), tonic::Code::Unauthenticated);
    assert_eq!(fake.message(), wrong.message());

    // An unknown tenant has the default policy: nothing is sent.
    let other_tenant = send_code(&svc, "globex", "SMS", phone, false).await;
    assert!(!code_was_delivered(&other_tenant.otp_id));
    assert!(store.get_otp(&other_tenant.otp_id).await.unwrap().is_none());

    cleanup_native_auth_db(&pool).await;
}

#[cfg(feature = "http-client")]
#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_postgres_login_otp_respects_tenant_channels_and_magic_links() {
    use crate::runtime::service::live_tests::notification_http_live::spawn_provider;
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    // A real loopback relay captures every webhook body, so the magic link it
    // must deliver is asserted on the wire.
    let (relay, received) = spawn_provider("200 OK").await;
    let _webhook = EnvRestore::set(
        "UDB_OTP_DELIVERY_WEBHOOK_URL",
        &format!("http://{relay}/otp"),
    );
    let svc = authn_service_with_jwt(pool.clone());
    let store = PostgresUserStore::new(pool.clone(), "");

    // Passwords stay required: CreateUser still refuses an empty password.
    let refused = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("needs_pw_{}", Uuid::new_v4().simple()),
            email: format!("needs_pw_{}@example.com", Uuid::new_v4().simple()),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("password required by default");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);

    // An account with a password, an email and a phone.
    let suffix = Uuid::new_v4().simple().to_string();
    let phone = "+8801712345602";
    let created = svc
        .create_user(Request::new(authn_pb::CreateUserRequest {
            username: format!("both_{suffix}"),
            email: format!("both_{suffix}@example.com"),
            phone: phone.to_string(),
            password: "CorrectHorse1!".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create user")
        .into_inner();
    let user = created.user.expect("created user");
    assert!(verify_issued_otp(&svc, &created.otp_id).await.verified);

    // Only EMAIL is enabled: an SMS request sends nothing.
    put_policy(&svc, "acme", true, &["EMAIL"], "").await;
    let sms = send_code(&svc, "acme", "SMS", phone, false).await;
    assert!(Uuid::parse_str(&sms.otp_id).is_ok());
    assert!(!code_was_delivered(&sms.otp_id));
    assert!(store.get_otp(&sms.otp_id).await.unwrap().is_none());

    // A magic link needs the tenant's link page: without one nothing is sent.
    let no_page = send_code(&svc, "acme", "EMAIL", &user.email, true).await;
    assert!(!code_was_delivered(&no_page.otp_id));
    assert!(store.get_otp(&no_page.otp_id).await.unwrap().is_none());

    // With the page configured the link is sent and signs in as a magic link.
    let policy = put_policy(
        &svc,
        "acme",
        true,
        &["EMAIL"],
        "https://app.example.test/auth/link",
    )
    .await;
    assert_eq!(policy.revision, 2);
    let link = send_code(&svc, "acme", "email", &user.email.to_uppercase(), true).await;
    assert!(code_was_delivered(&link.otp_id));
    let code = issued_test_otp_code(&link.otp_id);
    let signed_in = login_with(&svc, &link.otp_id, &code)
        .await
        .expect("sign in with the magic link");
    assert_eq!(signed_in.user_id, user.user_id);
    assert!(signed_in.refresh_token.starts_with("rt_"));

    // Turning the channel off stops a code already sent from signing in.
    let pending = send_code(&svc, "acme", "EMAIL", &user.email, false).await;
    assert!(code_was_delivered(&pending.otp_id));
    let pending_code = issued_test_otp_code(&pending.otp_id);

    // The relay received the magic link next to its code; a plain sign-in
    // code carries no link.
    let bodies: Vec<serde_json::Value> = received
        .lock()
        .await
        .iter()
        .map(|request| serde_json::from_slice(&request.body).expect("JSON webhook body"))
        .collect();
    let magic = bodies
        .iter()
        .find(|body| body["code"].as_str() == Some(code.as_str()))
        .expect("the magic link reached the relay");
    assert_eq!(magic["channel"], "email");
    assert_eq!(magic["address"], user.email.as_str());
    assert_eq!(magic["otp_type"], authn_entity_pb::OtpType::Login as i32);
    assert_eq!(
        magic["link"].as_str(),
        Some(
            format!(
                "https://app.example.test/auth/link?otp_id={}&code={code}",
                link.otp_id
            )
            .as_str()
        )
    );
    let plain = bodies
        .iter()
        .find(|body| body["code"].as_str() == Some(pending_code.as_str()))
        .expect("the plain code reached the relay");
    assert!(plain.get("link").is_none());
    put_policy(&svc, "acme", true, &[], "").await;
    let disabled = login_with(&svc, &pending.otp_id, &pending_code)
        .await
        .expect_err("the channel is off");
    assert_eq!(disabled.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}
