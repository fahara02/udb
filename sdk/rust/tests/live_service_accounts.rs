//! Live service-account conformance through the Rust SDK.
//!
//! Proves, against the served broker:
//!
//! 1. `Authenticate` names the account (`Principal.username`);
//! 2. a service-account bearer carries its bound roles, never a withheld
//!    admin-segment role, and its typed-grant scopes;
//! 3. a service account holding `udb:authz:check-access-for` evaluates
//!    `CheckAccess` for a person of its tenant (`evaluated_subject`), and is
//!    refused for an id that is not one;
//! 4. `ExchangeServiceTokenForTenant` mints a short-lived target-tenant token
//!    that authenticates and authorizes IN the target tenant, and is refused
//!    for a tenant with no approved membership;
//! 5. a phone-only `CreateUser` is verified by a `PHONE_VERIFICATION` OTP and
//!    the account becomes ACTIVE.
//!
//! Gated on `UDB_LIVE_SDK_TESTS=1`; without it the test passes without touching
//! the network. CI's "SDK service accounts (live)" job runs it against the
//! broker it just built, after `scripts/provision-service-account-live.py` has
//! created the identities it reads from the environment.
//!
//! Run: `cargo test --test live_service_accounts -- --nocapture`

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};
use udb_client::proto::udb::core::authn::entity::v1::{AccountKind, OtpType, UserStatus};
use udb_client::proto::udb::core::authn::services::v1 as authn;
use udb_client::proto::udb::core::authn::services::v1::authn_service_client::AuthnServiceClient;
use udb_client::proto::udb::core::authz::services::v1 as authz;
use udb_client::proto::udb::core::authz::services::v1::authz_service_client::AuthzServiceClient;
use udb_client::Metadata;

const OBJECT: &str = "sdk-live-chart";
const ACTION: &str = "read";
const EXCHANGED_SCOPE: &str = "udb:authz:check-access";

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} is required when UDB_LIVE_SDK_TESTS=1"))
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn endpoint(address: &str) -> String {
    if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    }
}

fn request<T>(meta: &Metadata, message: T) -> Request<T> {
    let mut request = Request::new(message);
    meta.apply(&mut request).expect("metadata");
    request
}

/// Metadata asserting `tenant` and `project`, with a correlation id:
/// authenticated native calls require a request-context header.
fn meta(tenant: &str, project: &str, role: &str, bearer: &str) -> Metadata {
    let mut meta = Metadata::new(tenant).with_project(project);
    meta.purpose = "rust.live.service-accounts".into();
    meta.correlation_id = format!("rust-live-service-accounts-{role}");
    meta.service_identity = "rust.sdk.live".into();
    meta.bearer_token = bearer.to_string();
    meta
}

fn first_non_empty(primary: &str, fallback: &str) -> String {
    if primary.is_empty() {
        fallback.to_string()
    } else {
        primary.to_string()
    }
}

/// Random bytes without a dependency: `RandomState` is seeded per instance
/// from the OS, mixed with the clock.
fn random_bytes(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default(),
        );
        hasher.write_usize(out.len());
        out.extend_from_slice(&hasher.finish().to_le_bytes());
    }
    out.truncate(n);
    out
}

fn random_hex(n: usize) -> String {
    random_bytes(n).iter().map(|b| format!("{b:02x}")).collect()
}

fn random_digits(n: usize) -> String {
    random_bytes(n)
        .iter()
        .map(|b| char::from(b'0' + b % 10))
        .collect()
}

/// A random RFC 4122 version-4 UUID.
fn random_uuid_v4() -> String {
    let mut b = random_bytes(16);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

async fn login(
    authn_client: &mut AuthnServiceClient<Channel>,
    public: &Metadata,
    step: &str,
    username: String,
    password: String,
    device: &str,
) -> String {
    let login = authn_client
        .login(request(
            public,
            authn::LoginRequest {
                username,
                password,
                tenant_hint: public.tenant_id.clone(),
                project_hint: public.project_id.clone(),
                device_name: device.into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("{step}: login failed: {status}"))
        .into_inner();
    assert!(
        !login.access_token.is_empty(),
        "{step}: login returned no access token"
    );
    login.access_token
}

async fn authenticate(
    authn_client: &mut AuthnServiceClient<Channel>,
    public: &Metadata,
    step: &str,
    bearer: &str,
) -> authn::Principal {
    authn_client
        .authenticate(request(
            public,
            authn::AuthnRequest {
                bearer_token: bearer.to_string(),
                tenant_hint: public.tenant_id.clone(),
                project_hint: public.project_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("{step}: Authenticate failed: {status}"))
        .into_inner()
        .principal
        .unwrap_or_else(|| panic!("{step}: Authenticate returned no principal"))
}

#[tokio::test(flavor = "multi_thread")]
async fn live_service_account_delegation_conformance() {
    if std::env::var("UDB_LIVE_SDK_TESTS").as_deref() != Ok("1") {
        eprintln!("skipped: the live service-account test needs UDB_LIVE_SDK_TESTS=1");
        return;
    }
    let auth_target =
        optional("UDB_AUTH_GRPC_TARGET").unwrap_or_else(|| required("UDB_GRPC_TARGET"));
    let tenant = required("UDB_LIVE_TENANT");
    let project = required("UDB_LIVE_PROJECT");
    let human_username = required("UDB_LIVE_USERNAME");
    let service_username = required("UDB_LIVE_SERVICE_USERNAME");
    let service_user_id = required("UDB_LIVE_SERVICE_USER_ID");
    let service_role = required("UDB_LIVE_SERVICE_ROLE");
    let withheld_role = required("UDB_LIVE_SERVICE_WITHHELD_ROLE");
    let delegated_user_id = required("UDB_LIVE_DELEGATED_USER_ID");
    let target_tenant_id = required("UDB_LIVE_TARGET_TENANT_ID");
    let target_project = required("UDB_LIVE_TARGET_PROJECT");
    let unapproved_tenant_id = required("UDB_LIVE_UNAPPROVED_TENANT_ID");

    let channel = Endpoint::from_shared(endpoint(&auth_target))
        .expect("endpoint")
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .connect()
        .await
        .expect("connect the native listener");
    let mut authn_client = AuthnServiceClient::new(channel.clone());
    let mut authz_client = AuthzServiceClient::new(channel);

    // Login and Authenticate are public: they carry the human tenant code.
    let public = meta(&tenant, &project, "public", "");

    // ── 1. Principal.username ──────────────────────────────────────────────
    let human_bearer = login(
        &mut authn_client,
        &public,
        "step 1",
        human_username.clone(),
        required("UDB_LIVE_PASSWORD"),
        "rust-sdk-service-accounts-human",
    )
    .await;
    let human = authenticate(&mut authn_client, &public, "step 1", &human_bearer).await;
    assert!(
        human.username.eq_ignore_ascii_case(&human_username),
        "step 1: principal.username = {:?}, want {:?}",
        human.username,
        human_username
    );
    assert!(
        !human.tenant_id.is_empty(),
        "step 1: the human principal has no canonical tenant_id"
    );
    let home_tenant_id = human.tenant_id.clone();
    let human_project = first_non_empty(&human.project_id, &project);
    let human_meta = meta(&home_tenant_id, &human_project, "human", &human_bearer);

    // ── 2. service bearer roles + username ─────────────────────────────────
    let service_bearer = login(
        &mut authn_client,
        &public,
        "step 2",
        service_username.clone(),
        required("UDB_LIVE_SERVICE_PASSWORD"),
        "rust-sdk-service-accounts-service",
    )
    .await;
    let svc = authenticate(&mut authn_client, &public, "step 2", &service_bearer).await;
    assert!(
        svc.username.eq_ignore_ascii_case(&service_username),
        "step 2: principal.username = {:?}, want {:?}",
        svc.username,
        service_username
    );
    assert_eq!(
        svc.user_id, service_user_id,
        "step 2: principal.user_id = {:?}, want {:?}",
        svc.user_id, service_user_id
    );
    assert_eq!(
        svc.account_kind,
        AccountKind::ServiceAccount as i32,
        "step 2: principal.account_kind = {}, want ACCOUNT_KIND_SERVICE_ACCOUNT",
        svc.account_kind
    );
    assert_eq!(
        svc.tenant_id, home_tenant_id,
        "step 2: principal.tenant_id = {:?}, want the home tenant {:?}",
        svc.tenant_id, home_tenant_id
    );
    assert!(
        svc.roles.iter().any(|r| r == &service_role),
        "step 2: principal.roles = {:?}, missing the bound role {:?}",
        svc.roles,
        service_role
    );
    for forbidden in [
        withheld_role.as_str(),
        "platform_admin",
        "organization_owner",
    ] {
        assert!(
            !svc.roles.iter().any(|r| r == forbidden),
            "step 2: principal.roles = {:?} carries the withheld role {:?}",
            svc.roles,
            forbidden
        );
    }
    assert!(
        svc.scopes.iter().any(|s| s == "udb:authz:check-access-for"),
        "step 2: principal.scopes = {:?}, missing udb:authz:check-access-for",
        svc.scopes
    );
    let service_meta = meta(
        &home_tenant_id,
        &first_non_empty(&svc.project_id, &project),
        "service",
        &service_bearer,
    );

    // ── 3. delegated CheckAccess ───────────────────────────────────────────
    let delegated = authz_client
        .check_access(request(
            &service_meta,
            authz::CheckAccessRequest {
                user_id: delegated_user_id.clone(),
                object: OBJECT.into(),
                action: ACTION.into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| {
            panic!("step 3: delegated CheckAccess for {delegated_user_id} failed: {status}")
        })
        .into_inner();
    assert_eq!(
        delegated.evaluated_subject, delegated_user_id,
        "step 3: evaluated_subject = {:?}, want the delegated user {:?}",
        delegated.evaluated_subject, delegated_user_id
    );
    let stranger = random_uuid_v4();
    let refused = authz_client
        .check_access(request(
            &service_meta,
            authz::CheckAccessRequest {
                user_id: stranger.clone(),
                object: OBJECT.into(),
                action: ACTION.into(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("step 3: delegated CheckAccess for an unknown id was accepted");
    assert_eq!(
        refused.code(),
        Code::PermissionDenied,
        "step 3: delegated CheckAccess for the unknown id {stranger} must be PERMISSION_DENIED: {refused}"
    );

    // ── 4. tenant token exchange ───────────────────────────────────────────
    let exchanged = authn_client
        .exchange_service_token_for_tenant(request(
            &service_meta,
            authn::ExchangeServiceTokenForTenantRequest {
                target_tenant_id: target_tenant_id.clone(),
                target_project_id: target_project.clone(),
                requested_scopes: vec![EXCHANGED_SCOPE.into()],
            },
        ))
        .await
        .unwrap_or_else(|status| {
            panic!("step 4: ExchangeServiceTokenForTenant({target_tenant_id}) failed: {status}")
        })
        .into_inner();
    assert!(
        !exchanged.access_token.is_empty(),
        "step 4: the exchange returned no access token"
    );
    assert_eq!(
        exchanged.tenant_id, target_tenant_id,
        "step 4: exchanged tenant_id = {:?}, want {:?}",
        exchanged.tenant_id, target_tenant_id
    );
    assert_eq!(
        exchanged.project_id, target_project,
        "step 4: exchanged project_id = {:?}, want {:?}",
        exchanged.project_id, target_project
    );
    assert_eq!(
        exchanged.scopes,
        vec![EXCHANGED_SCOPE.to_string()],
        "step 4: exchanged scopes = {:?}, want [{EXCHANGED_SCOPE}]",
        exchanged.scopes
    );
    assert!(
        !exchanged.membership_id.is_empty(),
        "step 4: the exchange returned no membership_id"
    );
    let now = now_unix();
    assert!(
        exchanged.expires_at_unix > now && exchanged.expires_at_unix <= now + 15 * 60 + 60,
        "step 4: expires_at_unix = {}, want in ({now}, {}]",
        exchanged.expires_at_unix,
        now + 15 * 60 + 60
    );
    let target_public = meta(&target_tenant_id, &target_project, "exchanged-authn", "");
    let in_target = authenticate(
        &mut authn_client,
        &target_public,
        "step 4",
        &exchanged.access_token,
    )
    .await;
    assert_eq!(
        in_target.tenant_id, target_tenant_id,
        "step 4: exchanged principal.tenant_id = {:?}, want {:?}",
        in_target.tenant_id, target_tenant_id
    );
    assert_eq!(
        in_target.user_id, service_user_id,
        "step 4: exchanged principal.user_id = {:?}, want {:?}",
        in_target.user_id, service_user_id
    );
    let exchanged_meta = meta(
        &target_tenant_id,
        &target_project,
        "exchanged",
        &exchanged.access_token,
    );
    let own_check = authz_client
        .check_access(request(
            &exchanged_meta,
            // CheckAccess requires a user_id; naming the caller itself is a
            // self-check.
            authz::CheckAccessRequest {
                user_id: service_user_id.clone(),
                object: OBJECT.into(),
                action: ACTION.into(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| {
            panic!("step 4: CheckAccess in the target tenant with the exchanged bearer failed: {status}")
        })
        .into_inner();
    assert_eq!(
        own_check.evaluated_subject, service_user_id,
        "step 4: target-tenant evaluated_subject = {:?}, want the service account {:?}",
        own_check.evaluated_subject, service_user_id
    );
    let unapproved = authn_client
        .exchange_service_token_for_tenant(request(
            &service_meta,
            authn::ExchangeServiceTokenForTenantRequest {
                target_tenant_id: unapproved_tenant_id.clone(),
                target_project_id: target_project.clone(),
                requested_scopes: Vec::new(),
            },
        ))
        .await
        .expect_err("step 4: the exchange for an unapproved tenant was accepted");
    assert_eq!(
        unapproved.code(),
        Code::PermissionDenied,
        "step 4: exchange for the unapproved tenant {unapproved_tenant_id} must be PERMISSION_DENIED: {unapproved}"
    );

    // ── 5. phone-only CreateUser ───────────────────────────────────────────
    let phone = format!("+1555{}", random_digits(7));
    let created = authn_client
        .create_user(request(
            &human_meta,
            authn::CreateUserRequest {
                username: format!("sdk-live-phone-rust-{}", random_hex(6)),
                phone: phone.clone(),
                password: "SdkLivePhone#2026".into(),
                tenant_id: home_tenant_id.clone(),
                project_id: human_project.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("step 5: phone-only CreateUser failed: {status}"))
        .into_inner();
    let user = created
        .user
        .clone()
        .expect("step 5: CreateUser returned no user");
    assert!(
        !user.user_id.is_empty(),
        "step 5: CreateUser returned no user_id"
    );
    assert!(
        user.email.is_empty(),
        "step 5: user.email = {:?}, want empty",
        user.email
    );
    assert_eq!(
        user.phone, phone,
        "step 5: user.phone = {:?}, want {:?}",
        user.phone, phone
    );
    assert_eq!(
        user.status,
        UserStatus::PendingVerification as i32,
        "step 5: user.status = {}, want USER_STATUS_PENDING_VERIFICATION",
        user.status
    );
    assert!(
        !created.otp_id.is_empty(),
        "step 5: CreateUser returned no otp_id"
    );
    let sent = authn_client
        .send_otp(request(
            &human_meta,
            authn::SendOtpRequest {
                user_id: user.user_id.clone(),
                otp_type: OtpType::PhoneVerification as i32,
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("step 5: SendOTP(PHONE_VERIFICATION) failed: {status}"))
        .into_inner();
    assert!(
        !sent.dev_otp_code.is_empty(),
        "step 5: SendOTP returned no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set on the broker?)"
    );
    // The echoed code belongs to the OTP SendOTP just issued.
    let otp_id = first_non_empty(&sent.otp_id, &created.otp_id);
    let verified = authn_client
        .verify_otp(request(
            &human_meta,
            authn::VerifyOtpRequest {
                otp_id: otp_id.clone(),
                code: sent.dev_otp_code.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("step 5: VerifyOTP({otp_id}) failed: {status}"))
        .into_inner();
    assert!(verified.verified, "step 5: VerifyOTP verified = false");
    assert_eq!(
        verified.otp_type,
        OtpType::PhoneVerification as i32,
        "step 5: VerifyOTP otp_type = {}, want OTP_TYPE_PHONE_VERIFICATION",
        verified.otp_type
    );
    let fetched = authn_client
        .get_user(request(
            &human_meta,
            authn::GetUserRequest {
                user_id: user.user_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .unwrap_or_else(|status| panic!("step 5: GetUser({}) failed: {status}", user.user_id))
        .into_inner()
        .user
        .expect("step 5: GetUser returned no user");
    assert_eq!(
        fetched.status,
        UserStatus::Active as i32,
        "step 5: after VerifyOTP user.status = {}, want USER_STATUS_ACTIVE",
        fetched.status
    );
    assert_eq!(
        fetched.phone, phone,
        "step 5: after VerifyOTP user.phone = {:?}, want {:?}",
        fetched.phone, phone
    );
}
