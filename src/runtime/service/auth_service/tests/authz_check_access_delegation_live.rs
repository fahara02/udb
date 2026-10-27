//! Live Postgres acceptance for delegated CheckAccess ("check access for"): a
//! service account holding `udb:authz:check-access-for` evaluates a person of
//! its own tenant; every delegated decision, allow AND deny, lands in the
//! access-decision audit with the caller in `evaluated_by`.

use super::support::*;
use crate::proto::udb::core::authn::entity::v1 as authn_entity_pb;
use crate::proto::udb::core::authn::services::v1 as authn_pb;
use crate::proto::udb::core::authn::services::v1::authn_service_server::AuthnService;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use crate::runtime::service::method_security::{
    VerifiedClaimContext, scope_claim_context_for_test,
};
use tonic::Request;

const PASSWORD: &str = "CorrectHorse1!";

fn chart_check(user_id: &str, action: &str) -> Request<authz_pb::CheckAccessRequest> {
    Request::new(authz_pb::CheckAccessRequest {
        user_id: user_id.to_string(),
        object: "chart".to_string(),
        action: action.to_string(),
        ..Default::default()
    })
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_delegated_check_access_evaluates_and_audits_the_subject -- --ignored --nocapture"]
async fn live_postgres_delegated_check_access_evaluates_and_audits_the_subject() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let broker = native_broker_service().await;
    let (_, authz, _) = broker.build_auth_services();
    let authn = authn_service_with_jwt(pool.clone());

    let (service_account, grant) = create_service_account_with_grant(
        &authn,
        "delegating_svc",
        PASSWORD,
        &["udb:authz:check-access", "udb:authz:check-access-for"],
    )
    .await;
    let patient = create_verified_user(&authn, "delegated_patient", PASSWORD).await;
    let foreign =
        create_verified_user_in(&authn, "foreign_patient", PASSWORD, "globex", "billing").await;

    // Only the patient may read charts; nothing grants delete.
    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: uuid::Uuid::new_v4().to_string(),
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                subject: patient.user_id.clone(),
                action: "read".to_string(),
                resource: "chart".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put patient chart policy");

    // The service account's own bearer, exactly as it would present it.
    let login = authn
        .login(Request::new(authn_pb::LoginRequest {
            username: service_account.email.clone(),
            password: PASSWORD.to_string(),
            device_name: "delegation-live".to_string(),
            ..Default::default()
        }))
        .await
        .expect("service-account login")
        .into_inner();
    let validated = authn
        .validate_token(Request::new(authn_pb::ValidateTokenRequest {
            token: login.access_token,
            token_type: authn_entity_pb::TokenType::JwtAccess as i32,
        }))
        .await
        .expect("validate service bearer")
        .into_inner();
    assert!(validated.valid);
    assert!(
        validated
            .scopes
            .iter()
            .any(|scope| scope == "udb:authz:check-access-for")
    );
    let ctx = VerifiedClaimContext {
        subject: validated.user_id.clone(),
        service_identity: grant.service_identity.clone(),
        tenant_id: validated.tenant_id.clone(),
        project_id: validated.project_id.clone(),
        scopes: validated.scopes.clone(),
        roles: validated.roles.clone(),
        // CREDENTIAL_TYPE_SERVICE_ACCOUNT, as method security classifies a
        // bearer carrying a grant service identity.
        credential_type: 4,
        credential_id: String::new(),
        auth_method: "pwd".to_string(),
        authenticated: true,
    };

    let allowed = scope_claim_context_for_test(
        ctx.clone(),
        authz.check_access(chart_check(&patient.user_id, "read")),
    )
    .await
    .expect("delegated allow")
    .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.evaluated_subject, patient.user_id);

    let denied = scope_claim_context_for_test(
        ctx.clone(),
        authz.check_access(chart_check(&patient.user_id, "delete")),
    )
    .await
    .expect("delegated deny is a decision")
    .into_inner();
    assert!(!denied.allowed);
    assert_eq!(denied.evaluated_subject, patient.user_id);

    // A person of another tenant and an id that does not exist get the SAME
    // refusal, so the call cannot tell them apart.
    let cross_tenant = scope_claim_context_for_test(
        ctx.clone(),
        authz.check_access(chart_check(&foreign.user_id, "read")),
    )
    .await
    .expect_err("another tenant's person is refused");
    let missing = scope_claim_context_for_test(
        ctx.clone(),
        authz.check_access(chart_check(&uuid::Uuid::new_v4().to_string(), "read")),
    )
    .await
    .expect_err("an unknown id is refused");
    assert_eq!(cross_tenant.code(), tonic::Code::PermissionDenied);
    assert_eq!(missing.code(), tonic::Code::PermissionDenied);
    assert_eq!(cross_tenant.message(), missing.message());

    // Without the delegation scope the same body evaluates the caller.
    let rebound = scope_claim_context_for_test(
        VerifiedClaimContext {
            scopes: vec!["udb:authz:check-access".to_string()],
            ..ctx.clone()
        },
        authz.check_access(chart_check(&patient.user_id, "read")),
    )
    .await
    .expect("rebinding check")
    .into_inner();
    assert_eq!(rebound.evaluated_subject, service_account.user_id);
    assert!(!rebound.allowed);

    // Both delegated decisions — the allow included — are audited with the
    // service caller recorded as the evaluator.
    let audits = authz
        .list_access_decision_audits(Request::new(authz_pb::ListAccessDecisionAuditsRequest {
            user_id: patient.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("list the patient's decision audits")
        .into_inner()
        .audits;
    let delegated: Vec<_> = audits
        .iter()
        .filter(|audit| audit.evaluated_by == service_account.user_id)
        .collect();
    assert!(
        delegated.iter().any(|audit| audit.object == "chart"
            && audit.action == "read"
            && audit.effect == authz_entity_pb::PolicyEffect::Allow as i32),
        "the delegated allow must be audited: {audits:?}"
    );
    assert!(
        delegated.iter().any(|audit| audit.object == "chart"
            && audit.action == "delete"
            && audit.effect == authz_entity_pb::PolicyEffect::Deny as i32),
        "the delegated deny must be audited: {audits:?}"
    );
    assert!(
        delegated.iter().all(
            |audit| audit.evaluated_by_service_identity == grant.service_identity
                && audit.tenant_id == "acme"
        ),
        "delegated rows name the caller's service identity and stay in its tenant"
    );

    cleanup_native_auth_db(&pool).await;
}
