//! Live Postgres acceptance for invitations: an administrator invites a person
//! into a tenant with roles; the invitee proves the channel with a one-time
//! code and gets an ACTIVE, verified, role-bound account that is signed in and
//! can sign in again. Revoked and expired invitations are refused uniformly,
//! and the role guards hold.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::native_catalog::native_model;
use crate::runtime::service::method_security::{
    VerifiedClaimContext, scope_claim_context_for_test, test_claim_context,
};
use tonic::Request;
use uuid::Uuid;

const PASSWORD: &str = "CorrectHorse1!";
const TENANT: &str = "acme";
const PROJECT: &str = "billing";
const MANAGE: &str = "udb:authn:manage-invitations";

fn admin(roles: &[&str]) -> VerifiedClaimContext {
    test_claim_context("invite-admin", TENANT, PROJECT, &[MANAGE], roles)
}

fn decode_policy_id(status: &tonic::Status) -> String {
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw).policy_decision_id
}

fn assert_unavailable(status: &tonic::Status) {
    assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
    assert_eq!(decode_policy_id(status), "invitation_unavailable");
}

fn random_phone() -> String {
    let digits = Uuid::new_v4().as_u128() % 1_000_000_000;
    format!("+8801{digits:09}")
}

/// A tenant role the invitations bind by code.
async fn seed_role(pool: &sqlx::PgPool, code: &str) {
    let m = native_model(
        "udb.core.authz.entity.v1.Role",
        &[
            "role_id",
            "name",
            "description",
            "is_system",
            "is_active",
            "tenant_id",
            "project_id",
            "role_code",
            "scope_type",
        ],
    );
    sqlx::query(&format!(
        "INSERT INTO {rel} ({rid}, {name}, {description}, {is_system}, {is_active}, {tenant}, {project}, {code}, {scope}) \
         VALUES ($1, $2, 'live invitation role', FALSE, TRUE, $3, '', $2, 'TENANT')",
        rel = m.relation,
        rid = m.q("role_id"),
        name = m.q("name"),
        description = m.q("description"),
        is_system = m.q("is_system"),
        is_active = m.q("is_active"),
        tenant = m.q("tenant_id"),
        project = m.q("project_id"),
        code = m.q("role_code"),
        scope = m.q("scope_type"),
    ))
    .bind(Uuid::new_v4())
    .bind(code)
    .bind(TENANT)
    .execute(pool)
    .await
    .expect("seed tenant role");
}

/// Role codes bound to `user_id` in the tenant.
async fn bound_role_codes(pool: &sqlx::PgPool, user_id: &str) -> Vec<String> {
    let ur = native_model(
        "udb.core.authz.entity.v1.UserRole",
        &["user_id", "role_id", "tenant_id"],
    );
    let r = native_model("udb.core.authz.entity.v1.Role", &["role_id", "role_code"]);
    let mut codes: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT r.{code} FROM {ur_rel} ur JOIN {r_rel} r ON r.{rid} = ur.{ur_rid} \
         WHERE ur.{uid} = $1 AND ur.{tenant} = $2",
        code = r.q("role_code"),
        ur_rel = ur.relation,
        r_rel = r.relation,
        rid = r.q("role_id"),
        ur_rid = ur.q("role_id"),
        uid = ur.q("user_id"),
        tenant = ur.q("tenant_id"),
    ))
    .bind(Uuid::parse_str(user_id).expect("user id is a UUID"))
    .bind(TENANT)
    .fetch_all(pool)
    .await
    .expect("read role bindings");
    codes.sort();
    codes
}

async fn create_invitation(
    authn: &super::super::AuthnServiceImpl,
    channel: &str,
    address: &str,
    roles: &[&str],
) -> authn_pb::CreateInvitationResponse {
    scope_claim_context_for_test(
        admin(&[]),
        authn.create_invitation(Request::new(authn_pb::CreateInvitationRequest {
            tenant_id: TENANT.to_string(),
            project_id: PROJECT.to_string(),
            channel: channel.to_string(),
            address: address.to_string(),
            roles: roles.iter().map(|role| role.to_string()).collect(),
            full_name: "Rina Akter".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("create invitation")
    .into_inner()
}

fn invitation_id(created: &authn_pb::CreateInvitationResponse) -> String {
    created
        .invitation
        .as_ref()
        .expect("invitation")
        .invitation_id
        .clone()
}

async fn start(
    authn: &super::super::AuthnServiceImpl,
    invitation_id: &str,
    token: &str,
) -> Result<authn_pb::StartInvitationAcceptanceResponse, tonic::Status> {
    authn
        .start_invitation_acceptance(Request::new(authn_pb::StartInvitationAcceptanceRequest {
            invitation_id: invitation_id.to_string(),
            token: token.to_string(),
            ..Default::default()
        }))
        .await
        .map(tonic::Response::into_inner)
}

fn accept_request(
    invitation_id: &str,
    token: &str,
    username: &str,
    otp_id: &str,
    code: &str,
) -> Request<authn_pb::AcceptInvitationRequest> {
    Request::new(authn_pb::AcceptInvitationRequest {
        invitation_id: invitation_id.to_string(),
        token: token.to_string(),
        username: username.to_string(),
        password: PASSWORD.to_string(),
        otp_id: otp_id.to_string(),
        code: code.to_string(),
        device_name: "invitation-live".to_string(),
        ..Default::default()
    })
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_postgres_sms_invitation_creates_an_active_verified_role_bound_account() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    seed_role(&pool, "nurse").await;
    let phone = random_phone();

    let created = create_invitation(&authn, "SMS", &phone, &["nurse"]).await;
    let info = created.invitation.clone().expect("invitation");
    assert_eq!(info.status, "PENDING");
    assert_eq!(info.channel, "SMS");
    assert_eq!(info.address, phone);
    assert_eq!(info.roles, vec!["nurse".to_string()]);
    assert_eq!(info.send_count, 1);
    assert!(created.accept_token.starts_with("inv_"));
    let id = invitation_id(&created);
    let token = created.accept_token.clone();

    // The token is stored hashed, never verbatim.
    let stored: String = sqlx::query_scalar(&format!(
        "SELECT token_hash FROM {} WHERE invitation_id = $1",
        native_model("udb.core.authn.entity.v1.Invitation", &["token_hash"]).relation
    ))
    .bind(Uuid::parse_str(&id).expect("uuid"))
    .fetch_one(&pool)
    .await
    .expect("read stored token hash");
    assert_ne!(stored, token);
    assert!(!stored.contains(&token));

    // What the invitee sees: no PII beyond the masked address.
    let preview = authn
        .get_invitation(Request::new(authn_pb::GetInvitationRequest {
            invitation_id: id.clone(),
            token: token.clone(),
        }))
        .await
        .expect("invitee preview")
        .into_inner();
    assert_eq!(preview.channel, "SMS");
    assert_eq!(preview.roles, vec!["nurse".to_string()]);
    assert_ne!(preview.masked_address, phone);
    assert!(preview.masked_address.ends_with(&phone[phone.len() - 3..]));
    assert!(preview.password_required);

    // A wrong token is the uniform refusal.
    let wrong = authn
        .get_invitation(Request::new(authn_pb::GetInvitationRequest {
            invitation_id: id.clone(),
            token: format!("{token}x"),
        }))
        .await
        .expect_err("wrong token");
    assert_unavailable(&wrong);

    // Start: an SMS code goes to the invitation phone.
    let started = start(&authn, &id, &token).await.expect("start acceptance");
    assert!(!started.otp_id.is_empty());
    let code = issued_test_otp_code(&started.otp_id);

    // A wrong code does not accept.
    let bad = authn
        .accept_invitation(accept_request(
            &id,
            &token,
            "rina_live",
            &started.otp_id,
            "000000x",
        ))
        .await
        .expect_err("wrong code");
    assert_eq!(bad.code(), tonic::Code::Unauthenticated);

    let username = format!("rina_{}", Uuid::new_v4().simple());
    let accepted = authn
        .accept_invitation(accept_request(
            &id,
            &token,
            &username,
            &started.otp_id,
            &code,
        ))
        .await
        .expect("accept invitation")
        .into_inner();
    assert!(!accepted.user_id.is_empty());
    assert!(!accepted.session_id.is_empty());
    assert!(!accepted.mfa_required);

    // The account: ACTIVE, the proven phone verified, in the invitation's
    // tenant and project, with the invited role bound.
    let user = authn
        .get_user(Request::new(authn_pb::GetUserRequest {
            user_id: accepted.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("read accepted account")
        .into_inner()
        .user
        .expect("user");
    assert_eq!(user.status, authn_entity_pb::UserStatus::Active as i32);
    assert_eq!(user.username, username);
    assert_eq!(user.phone, phone);
    assert!(user.phone_verified_at.is_some());
    assert_eq!(user.tenant_id, TENANT);
    assert_eq!(user.project_id, PROJECT);
    assert_eq!(
        bound_role_codes(&pool, &accepted.user_id).await,
        vec!["nurse".to_string()]
    );

    // The invitation is ACCEPTED by that account, and cannot be used again.
    let listed = scope_claim_context_for_test(
        admin(&[]),
        authn.list_invitations(Request::new(authn_pb::ListInvitationsRequest {
            tenant_id: TENANT.to_string(),
            status: "ACCEPTED".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("list accepted invitations")
    .into_inner();
    let row = listed
        .invitations
        .iter()
        .find(|invitation| invitation.invitation_id == id)
        .expect("accepted invitation listed");
    assert_eq!(row.accepted_user_id, accepted.user_id);
    assert_unavailable(
        &authn
            .accept_invitation(accept_request(
                &id,
                &token,
                "rina_again",
                &started.otp_id,
                &code,
            ))
            .await
            .expect_err("an accepted invitation is closed"),
    );

    // The account signs in with its password.
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: username.clone(),
            password: PASSWORD.to_string(),
            device_name: "invitation-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("accepted account logs in")
        .into_inner();
    assert_eq!(login.user_id, accepted.user_id);
    assert!(!login.mfa_required);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_postgres_revoked_and_expired_invitations_are_refused_uniformly() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());

    // ── Revoke after the invitee started: blocked, invitee account removed ──
    let email = format!("invitee_{}@example.com", Uuid::new_v4().simple());
    let created = create_invitation(&authn, "EMAIL", &email, &[]).await;
    let id = invitation_id(&created);
    let token = created.accept_token.clone();
    let started = start(&authn, &id, &token).await.expect("start acceptance");
    let code = issued_test_otp_code(&started.otp_id);
    let invitee: String = sqlx::query_scalar(&format!(
        "SELECT invitee_user_id::TEXT FROM {} WHERE invitation_id = $1",
        native_model("udb.core.authn.entity.v1.Invitation", &["invitee_user_id"]).relation
    ))
    .bind(Uuid::parse_str(&id).expect("uuid"))
    .fetch_one(&pool)
    .await
    .expect("invitee account recorded");

    // An immediate resend is throttled.
    let throttled = scope_claim_context_for_test(
        admin(&[]),
        authn.resend_invitation(Request::new(authn_pb::ResendInvitationRequest {
            invitation_id: id.clone(),
        })),
    )
    .await
    .expect_err("resend within the interval");
    assert_eq!(throttled.code(), tonic::Code::ResourceExhausted);

    // Another tenant's admin cannot see or revoke it.
    let foreign = test_claim_context("other-admin", "other-tenant", "", &[MANAGE], &[]);
    let hidden = scope_claim_context_for_test(
        foreign,
        authn.revoke_invitation(Request::new(authn_pb::RevokeInvitationRequest {
            invitation_id: id.clone(),
            reason: "not yours".to_string(),
        })),
    )
    .await
    .expect_err("foreign tenant");
    assert_eq!(hidden.code(), tonic::Code::NotFound);

    let revoked = scope_claim_context_for_test(
        admin(&[]),
        authn.revoke_invitation(Request::new(authn_pb::RevokeInvitationRequest {
            invitation_id: id.clone(),
            reason: "sent to the wrong person".to_string(),
        })),
    )
    .await
    .expect("revoke")
    .into_inner();
    assert!(revoked.revoked);
    assert_eq!(revoked.invitation.expect("invitation").status, "REVOKED");
    assert!(
        authn
            .get_user(Request::new(authn_pb::GetUserRequest {
                user_id: invitee,
                ..Default::default()
            }))
            .await
            .is_err(),
        "the unused invitee account is deleted with the invitation"
    );
    assert_unavailable(
        &authn
            .accept_invitation(accept_request(
                &id,
                &token,
                "late_rina",
                &started.otp_id,
                &code,
            ))
            .await
            .expect_err("revoked"),
    );
    assert_unavailable(&start(&authn, &id, &token).await.expect_err("revoked"));
    let again = scope_claim_context_for_test(
        admin(&[]),
        authn.revoke_invitation(Request::new(authn_pb::RevokeInvitationRequest {
            invitation_id: id.clone(),
            reason: String::new(),
        })),
    )
    .await
    .expect("revoking twice is a no-op")
    .into_inner();
    assert!(!again.revoked);

    // ── Expired: blocked, listed as EXPIRED, replaceable ─────────────────────
    let phone = random_phone();
    let created = create_invitation(&authn, "SMS", &phone, &[]).await;
    let id = invitation_id(&created);
    let token = created.accept_token.clone();
    sqlx::query(&format!(
        "UPDATE {} SET expires_at = now() - interval '1 minute' WHERE invitation_id = $1",
        native_model("udb.core.authn.entity.v1.Invitation", &["expires_at"]).relation
    ))
    .bind(Uuid::parse_str(&id).expect("uuid"))
    .execute(&pool)
    .await
    .expect("expire the invitation");
    let expired = authn
        .get_invitation(Request::new(authn_pb::GetInvitationRequest {
            invitation_id: id.clone(),
            token: token.clone(),
        }))
        .await
        .expect_err("expired");
    assert_unavailable(&expired);
    // Identical to the refusal for an invitation that never existed.
    let unknown = authn
        .get_invitation(Request::new(authn_pb::GetInvitationRequest {
            invitation_id: Uuid::new_v4().to_string(),
            token: token.clone(),
        }))
        .await
        .expect_err("unknown");
    assert_eq!(unknown.message(), expired.message());
    assert_unavailable(&start(&authn, &id, &token).await.expect_err("expired"));
    let listed = scope_claim_context_for_test(
        admin(&[]),
        authn.list_invitations(Request::new(authn_pb::ListInvitationsRequest {
            status: "EXPIRED".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect("list expired")
    .into_inner();
    assert!(
        listed
            .invitations
            .iter()
            .any(|invitation| invitation.invitation_id == id && invitation.status == "EXPIRED")
    );
    // A lapsed invitation does not block a new one for the same phone.
    let replacement = create_invitation(&authn, "SMS", &phone, &[]).await;
    assert_ne!(invitation_id(&replacement), id);

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn live_postgres_invitation_role_guards() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    super::super::seed_system_authz_defaults(&pool)
        .await
        .expect("seed organization_owner");
    let authn = authn_service(pool.clone());
    let request = |roles: &[&str]| {
        Request::new(authn_pb::CreateInvitationRequest {
            channel: "PASSKEY".to_string(),
            roles: roles.iter().map(|role| role.to_string()).collect(),
            ..Default::default()
        })
    };

    // organization_owner needs an inviter who holds it.
    let refused = scope_claim_context_for_test(
        admin(&["tenant_admin"]),
        authn.create_invitation(request(&["organization_owner"])),
    )
    .await
    .expect_err("an admin without organization_owner cannot invite one");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_policy_id(&refused),
        "organization_owner_authority_required"
    );
    let owner_invite = scope_claim_context_for_test(
        admin(&["organization_owner"]),
        authn.create_invitation(request(&["organization_owner"])),
    )
    .await
    .expect("an owner may invite an owner")
    .into_inner();
    assert_eq!(
        owner_invite.invitation.expect("invitation").roles,
        vec!["organization_owner".to_string()]
    );

    // Reserved platform roles are never granted by invitation, even by an owner.
    let reserved = scope_claim_context_for_test(
        admin(&["organization_owner"]),
        authn.create_invitation(request(&["platform_admin"])),
    )
    .await
    .expect_err("platform roles are reserved");
    assert_eq!(reserved.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_policy_id(&reserved),
        "reserved_platform_role_authority_required"
    );

    // A role code the tenant does not have is refused up front.
    let unknown = scope_claim_context_for_test(
        admin(&[]),
        authn.create_invitation(request(&["no_such_role"])),
    )
    .await
    .expect_err("unknown role");
    assert_eq!(unknown.code(), tonic::Code::FailedPrecondition);

    // Nothing is written outside the caller's tenant.
    let cross = scope_claim_context_for_test(
        admin(&[]),
        authn.create_invitation(Request::new(authn_pb::CreateInvitationRequest {
            tenant_id: "other-tenant".to_string(),
            channel: "PASSKEY".to_string(),
            ..Default::default()
        })),
    )
    .await
    .expect_err("another tenant");
    assert_eq!(cross.code(), tonic::Code::PermissionDenied);

    cleanup_native_auth_db(&pool).await;
}
