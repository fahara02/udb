//! Live Postgres acceptance for cross-tenant service tokens: a target-tenant
//! admin approves a service account homed in another tenant
//! (ServiceAccountTenantMembership); the account exchanges its home bearer for
//! a short-lived target-tenant token that is re-validated on every use.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::runtime::service::method_security::{
    VerifiedClaimContext, enforce_body_tenant_matches_claim, scope_claim_context_for_test,
    test_claim_context,
};
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const PASSWORD: &str = "CorrectHorse1!";
const OUTBOX_RELATION: &str = "udb_system.outbox_events";

fn decode_policy_id(status: &tonic::Status) -> String {
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("error-detail trailer present")
        .to_bytes()
        .expect("trailer decodes to bytes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw).policy_decision_id
}

async fn validate(
    authn: &super::super::AuthnServiceImpl,
    token: &str,
) -> authn_pb::ValidateTokenResponse {
    authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: token.to_string(),
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate token")
        .into_inner()
}

fn membership_request(
    target: &str,
    user_id: &str,
) -> Request<authn_pb::CreateServiceAccountTenantMembershipRequest> {
    let expires = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs() as i64
        + 3_600;
    Request::new(authn_pb::CreateServiceAccountTenantMembershipRequest {
        target_tenant_id: target.to_string(),
        user_id: user_id.to_string(),
        home_tenant_id: "acme".to_string(),
        target_project_id: "clinic".to_string(),
        approved_scopes: vec!["udb:read".to_string()],
        reason: "live exchange test".to_string(),
        expires_at: Some(prost_types::Timestamp {
            seconds: expires,
            nanos: 0,
        }),
    })
}

fn exchange_request(target: &str) -> Request<authn_pb::ExchangeServiceTokenForTenantRequest> {
    Request::new(authn_pb::ExchangeServiceTokenForTenantRequest {
        target_tenant_id: target.to_string(),
        target_project_id: "clinic".to_string(),
        requested_scopes: Vec::new(),
    })
}

/// The tenants (`payload.tenant_id`) an outbox topic was published in for one
/// membership.
async fn event_tenants(pool: &sqlx::PgPool, topic: &str, membership_id: &str) -> Vec<String> {
    let mut tenants: Vec<String> = sqlx::query_scalar(
        "SELECT payload->>'tenant_id' FROM udb_system.outbox_events \
         WHERE topic = $1 AND payload->'payload'->>'membership_id' = $2",
    )
    .bind(topic)
    .bind(membership_id)
    .fetch_all(pool)
    .await
    .expect("read membership events");
    tenants.sort();
    tenants.dedup();
    tenants
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_service_tenant_token_exchange -- --ignored --nocapture"]
async fn live_postgres_service_tenant_token_exchange() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    ensure_outbox_table(&pool).await;
    let authn = authn_service_with_jwt(pool.clone()).with_event_sink(Arc::new(
        super::super::events::OutboxAuthEventSink::new(pool.clone(), OUTBOX_RELATION),
    ));
    let (account, grant) = create_service_account_with_grant(
        &authn,
        "tenant_exchange",
        PASSWORD,
        &["udb:read", "udb:authn:exchange-tenant-token"],
    )
    .await;
    let target = Uuid::new_v4().to_string();
    let third = Uuid::new_v4().to_string();

    // The service account's HOME bearer (tenant acme), as the transport would
    // install it for the exchange call.
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: account.email.clone(),
            password: PASSWORD.to_string(),
            device_name: "tenant-exchange-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("service-account login")
        .into_inner();
    let home_bearer = validate(&authn, &login.access_token).await;
    assert!(home_bearer.valid);
    let home_ctx = VerifiedClaimContext {
        subject: home_bearer.user_id.clone(),
        service_identity: grant.service_identity.clone(),
        tenant_id: home_bearer.tenant_id.clone(),
        project_id: home_bearer.project_id.clone(),
        scopes: home_bearer.scopes.clone(),
        roles: home_bearer.roles.clone(),
        // CREDENTIAL_TYPE_SERVICE_ACCOUNT
        credential_type: 4,
        credential_id: home_bearer.token_id.clone(),
        auth_method: "pwd".to_string(),
        authenticated: true,
    };

    // No membership yet: refused.
    let denied = scope_claim_context_for_test(
        home_ctx.clone(),
        authn.exchange_service_token_for_tenant(exchange_request(&target)),
    )
    .await
    .expect_err("no membership, no target-tenant token");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        decode_policy_id(&denied),
        "service_tenant_membership_required"
    );

    // The home tenant's admin cannot approve its account into another tenant.
    let home_admin =
        test_claim_context("home-admin", "acme", "", &["udb:authn:manage-grants"], &[]);
    let refused = scope_claim_context_for_test(
        home_admin,
        authn.create_service_account_tenant_membership(membership_request(
            &target,
            &account.user_id,
        )),
    )
    .await
    .expect_err("a home-tenant admin alone cannot create a membership");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    // The target tenant's admin approves it.
    let target_admin = test_claim_context(
        "target-admin",
        &target,
        "",
        &["udb:authn:manage-grants", "udb:authn:read-grants"],
        &[],
    );
    let membership = scope_claim_context_for_test(
        target_admin.clone(),
        authn.create_service_account_tenant_membership(membership_request(
            &target,
            &account.user_id,
        )),
    )
    .await
    .expect("target-tenant admin approves the membership")
    .into_inner()
    .membership
    .expect("membership");
    assert_eq!(membership.status, "ACTIVE");
    assert_eq!(membership.home_tenant_id, "acme");
    let listed = scope_claim_context_for_test(
        target_admin.clone(),
        authn.list_service_account_tenant_memberships(Request::new(
            authn_pb::ListServiceAccountTenantMembershipsRequest {
                target_tenant_id: target.clone(),
                ..Default::default()
            },
        )),
    )
    .await
    .expect("list target memberships")
    .into_inner();
    assert!(
        listed
            .memberships
            .iter()
            .any(|listed| listed.membership_id == membership.membership_id)
    );

    // Exchange: a target-tenant token pinned to the membership.
    let exchanged = scope_claim_context_for_test(
        home_ctx.clone(),
        authn.exchange_service_token_for_tenant(exchange_request(&target)),
    )
    .await
    .expect("exchange the home bearer")
    .into_inner();
    assert_eq!(exchanged.tenant_id, target);
    assert_eq!(exchanged.project_id, "clinic");
    assert_eq!(exchanged.scopes, vec!["udb:read"]);
    assert_eq!(exchanged.membership_id, membership.membership_id);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs() as i64;
    assert!(exchanged.expires_at_unix > now);
    assert!(exchanged.expires_at_unix <= now + 15 * 60 + 5);

    let in_target = validate(&authn, &exchanged.access_token).await;
    assert!(
        in_target.valid,
        "the exchanged token works in the target tenant"
    );
    assert_eq!(in_target.user_id, account.user_id);
    assert_eq!(in_target.tenant_id, target);
    assert_eq!(in_target.project_id, "clinic");
    assert_eq!(in_target.scopes, vec!["udb:read"]);
    assert!(in_target.token_id.starts_with("mbx_"));
    let claims = crate::runtime::security::validate_bearer_token(
        &crate::runtime::security::SecurityConfig {
            jwt_public_key: Some(
                include_str!("../../../testdata/jwt_rs256_public.pem").to_string(),
            ),
            ..Default::default()
        },
        &exchanged.access_token,
    )
    .expect("exchanged token verifies");
    assert_eq!(claims.home_tenant_id.as_deref(), Some("acme"));
    assert_eq!(
        claims.mbr.as_deref(),
        Some(format!("{}:1", membership.membership_id).as_str())
    );
    assert_eq!(
        claims.service_identity.as_deref(),
        Some(grant.service_identity.as_str())
    );

    // The token is bound to the target tenant: neither the home tenant nor a
    // third tenant accepts it, and the third tenant cannot be exchanged for.
    let token_ctx = VerifiedClaimContext {
        subject: in_target.user_id.clone(),
        service_identity: grant.service_identity.clone(),
        tenant_id: in_target.tenant_id.clone(),
        project_id: in_target.project_id.clone(),
        scopes: in_target.scopes.clone(),
        roles: in_target.roles.clone(),
        credential_type: 4,
        credential_id: in_target.token_id.clone(),
        auth_method: "membership_exchange".to_string(),
        authenticated: true,
    };
    // The guard is a no-op without an installed claim context (in-process
    // callers), so each check runs as the exchanged token's request would.
    let body_tenant_check = |tenant: String, project: &'static str| {
        let ctx = token_ctx.clone();
        scope_claim_context_for_test(token_ctx.clone(), async move {
            enforce_body_tenant_matches_claim(&ctx, &tenant, project)
        })
    };
    body_tenant_check(target.clone(), "clinic")
        .await
        .expect("the target tenant accepts the token");
    assert!(body_tenant_check("acme".to_string(), "").await.is_err());
    assert!(body_tenant_check(third.clone(), "").await.is_err());
    let third_denied = scope_claim_context_for_test(
        home_ctx.clone(),
        authn.exchange_service_token_for_tenant(exchange_request(&third)),
    )
    .await
    .expect_err("no membership in a third tenant");
    assert_eq!(third_denied.message(), denied.message());
    // An exchanged token never exchanges again.
    let chained = scope_claim_context_for_test(
        token_ctx.clone(),
        authn.exchange_service_token_for_tenant(exchange_request(&target)),
    )
    .await
    .expect_err("an exchanged token is not a home bearer");
    assert_eq!(chained.code(), tonic::Code::PermissionDenied);

    // Audited in BOTH tenants: the approval and the exchange.
    let mut both = vec!["acme".to_string(), target.clone()];
    both.sort();
    assert_eq!(
        event_tenants(
            &pool,
            super::super::memberships::TOPIC_MEMBERSHIP_CHANGED,
            &membership.membership_id
        )
        .await,
        both
    );
    assert_eq!(
        event_tenants(
            &pool,
            super::super::memberships::TOPIC_TENANT_TOKEN_EXCHANGED,
            &membership.membership_id
        )
        .await,
        both
    );

    // Revoking the membership kills the token already issued.
    let revoked = scope_claim_context_for_test(
        target_admin.clone(),
        authn.revoke_service_account_tenant_membership(Request::new(
            authn_pb::RevokeServiceAccountTenantMembershipRequest {
                target_tenant_id: target.clone(),
                membership_id: membership.membership_id.clone(),
                reason: "live revoke".to_string(),
            },
        )),
    )
    .await
    .expect("revoke membership")
    .into_inner();
    assert!(revoked.revoked);
    assert!(
        !validate(&authn, &exchanged.access_token).await.valid,
        "a revoked membership's token stops validating"
    );

    // A fresh approval, a fresh token — then revoking the HOME grant kills it.
    scope_claim_context_for_test(
        target_admin.clone(),
        authn.create_service_account_tenant_membership(membership_request(
            &target,
            &account.user_id,
        )),
    )
    .await
    .expect("re-approve after revoke");
    let second = scope_claim_context_for_test(
        home_ctx.clone(),
        authn.exchange_service_token_for_tenant(exchange_request(&target)),
    )
    .await
    .expect("exchange under the new membership")
    .into_inner();
    assert!(validate(&authn, &second.access_token).await.valid);
    authn
        .revoke_service_account_grant(Request::new(authn_pb::RevokeServiceAccountGrantRequest {
            tenant_id: "acme".to_string(),
            user_id: account.user_id.clone(),
            reason: "live revoke home grant".to_string(),
        }))
        .await
        .expect("revoke the home grant");
    assert!(
        !validate(&authn, &second.access_token).await.valid,
        "revoking the home grant kills the exchanged token"
    );

    cleanup_native_auth_db(&pool).await;
}
