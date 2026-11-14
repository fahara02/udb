use super::support::*;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_relationship_tuple_roundtrip -- --ignored --nocapture"]
async fn live_postgres_authz_relationship_tuple_roundtrip() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "rebac", "CorrectHorse1!").await;
    let object = format!("invoice:{}", Uuid::new_v4().simple());
    let policy_id = Uuid::new_v4().to_string();

    authz
        .put_relationship(Request::new(authz_pb::PutRelationshipRequest {
            tuple: Some(authz_pb::RelationshipTuple {
                subject: user.user_id.clone(),
                relation: "owner".to_string(),
                object: object.clone(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                version: 1,
                expires_at_unix: now_unix() as i64 + 3600,
                source: "live_test".to_string(),
            }),
        }))
        .await
        .expect("put relationship tuple");

    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: policy_id.clone(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                action: "data.select".to_string(),
                resource: object.clone(),
                relationship: "owner".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put ReBAC policy");

    let allowed = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: object.clone(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check relationship-owned object")
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.matched_rule, policy_id);

    let other_object = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: "invoice:not-owned".to_string(),
            action: "data.select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check non-owned object")
        .into_inner();
    assert!(!other_object.allowed);

    let listed = authz
        .list_policy_rules(Request::new(authz_pb::ListPolicyRulesRequest {
            domain: "acme".to_string(),
            object,
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list ReBAC policy")
        .into_inner();
    assert_eq!(listed.policies.len(), 1);
    assert_eq!(listed.policies[0].policy_id, policy_id);

    cleanup_native_auth_db(&pool).await;
}

/// The reported shape end to end: a tenant-domain rule written by
/// PutAuthzPolicy for a service account's user id (role "", purpose set, no
/// scopes, bound to a project) matches that service account acting in the
/// tenant through a membership-exchanged (`mbx_`) token; a non-admin caller
/// asking about the account is answered for itself (`evaluated_subject`), and
/// the same token writes a relationship tuple in the tenant.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_membership_service_account_rule_and_tuple_write -- --ignored --nocapture"]
async fn live_postgres_membership_service_account_rule_and_tuple_write() {
    use crate::runtime::service::method_security::{
        VerifiedClaimContext, scope_claim_context_for_test,
    };

    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authz = authz_service(pool.clone()).await;
    let tenant = "acme".to_string();
    let project = format!("meddesk-{}", Uuid::new_v4().simple());
    let service_account = Uuid::new_v4().to_string();
    let policy_id = Uuid::new_v4().to_string();

    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: policy_id.clone(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: tenant.clone(),
                project: project.clone(),
                subject: service_account.clone(),
                role: String::new(),
                action: "seed-persona".to_string(),
                resource: "list:roots".to_string(),
                purpose: "meddesk.persona".to_string(),
                required_scopes: Vec::new(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put the service account's tenant rule");

    let membership_ctx = VerifiedClaimContext {
        subject: service_account.clone(),
        service_identity: "meddesk.seeder".to_string(),
        tenant_id: tenant.clone(),
        project_id: project.clone(),
        scopes: vec![
            "udb:authz:check-access".to_string(),
            "udb:authz:put-relationship".to_string(),
        ],
        roles: Vec::new(),
        credential_type: 4, // CREDENTIAL_TYPE_SERVICE_ACCOUNT
        credential_id: format!("mbx_{}", Uuid::new_v4().simple()),
        auth_method: "token_exchange".to_string(),
        authenticated: true,
    };
    let check = |user_id: &str| {
        Request::new(authz_pb::CheckAccessRequest {
            user_id: user_id.to_string(),
            tenant_id: tenant.clone(),
            project_id: project.clone(),
            object: "list:roots".to_string(),
            action: "seed-persona".to_string(),
            purpose: "meddesk.persona".to_string(),
            ..Default::default()
        })
    };

    let allowed = scope_claim_context_for_test(
        membership_ctx.clone(),
        authz.check_access(check(&service_account)),
    )
    .await
    .expect("the service account's own check")
    .into_inner();
    assert!(allowed.allowed, "{}", allowed.reason);
    assert_eq!(allowed.matched_rule, policy_id);
    assert_eq!(allowed.evaluated_subject, service_account);

    // A tenant-bound human asking about the account is answered for itself:
    // the denial names the account's rule (closest), not the owner rule.
    let human = Uuid::new_v4().to_string();
    let rebound = scope_claim_context_for_test(
        VerifiedClaimContext {
            subject: human.clone(),
            service_identity: String::new(),
            scopes: vec!["udb:authz:check-access".to_string()],
            credential_type: 1, // CREDENTIAL_TYPE_BEARER_JWT
            credential_id: String::new(),
            auth_method: "pwd".to_string(),
            ..membership_ctx.clone()
        },
        authz.check_access(check(&service_account)),
    )
    .await
    .expect("rebound check")
    .into_inner();
    assert!(!rebound.allowed);
    assert_eq!(rebound.evaluated_subject, human);
    assert!(
        rebound.reason.contains(&policy_id),
        "the diagnosis names the closest rule: {}",
        rebound.reason
    );

    // The membership token writes a relationship tuple in its tenant.
    scope_claim_context_for_test(
        membership_ctx,
        authz.put_relationship(Request::new(authz_pb::PutRelationshipRequest {
            tuple: Some(authz_pb::RelationshipTuple {
                subject: service_account.clone(),
                relation: "seeder".to_string(),
                object: format!("persona:{}", Uuid::new_v4().simple()),
                tenant: tenant.clone(),
                project: project.clone(),
                version: 1,
                expires_at_unix: 0,
                source: "live_test".to_string(),
            }),
        })),
    )
    .await
    .expect("a service account writes a relationship tuple in its tenant");
}
