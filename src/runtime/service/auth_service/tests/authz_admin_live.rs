use super::support::*;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
use crate::proto::udb::core::common::v1 as common_pb;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::Request;
use uuid::Uuid;

fn authz_tenant_request<T>(message: T, tenant_id: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "x-tenant-id",
        tenant_id.parse().expect("valid tenant metadata"),
    );
    request
}

fn live_governance_actor(subject: &str) -> authz_pb::GovernanceActor {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64;
    authz_pb::GovernanceActor {
        subject: subject.to_string(),
        tenant_id: "acme".to_string(),
        project_id: "billing".to_string(),
        break_glass: true,
        break_glass_reason: "live governance read-after-write proof".to_string(),
        break_glass_expires_at_unix: now + 900,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_admin_crud_and_audit_lifecycle -- --ignored --nocapture"]
async fn live_postgres_authz_admin_crud_and_audit_lifecycle() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "authz_admin", "CorrectHorse1!").await;
    let suffix = Uuid::new_v4().simple().to_string();
    let role_code = format!("auditor_{suffix}");

    let role = authz
        .create_role(Request::new(authz_pb::CreateRoleRequest {
            name: format!("Auditor {suffix}"),
            description: "Live admin CRUD role".to_string(),
            created_by: user.user_id.clone(),
            role_code: role_code.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            scope_type: authz_entity_pb::RoleScopeType::Project as i32,
            access_surface: "native-authz".to_string(),
            metadata: [("suite".to_string(), "live".to_string())].into(),
        }))
        .await
        .expect("create role for admin lifecycle")
        .into_inner()
        .role
        .expect("created role");

    let got_by_id = authz
        .get_role(Request::new(authz_pb::GetRoleRequest {
            role_id: role.role_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("get role by id")
        .into_inner()
        .role
        .expect("role by id");
    assert_eq!(got_by_id.role_code, role_code);

    let got_by_code = authz
        .get_role(Request::new(authz_pb::GetRoleRequest {
            role_code: role_code.clone(),
            domain: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("get role by code")
        .into_inner()
        .role
        .expect("role by code");
    assert_eq!(got_by_code.role_id, role.role_id);

    let listed_roles = authz
        .list_roles(Request::new(authz_pb::ListRolesRequest {
            domain: "acme".to_string(),
            active_only: true,
            page: Some(common_pb::PageRequest {
                page_size: 20,
                ..Default::default()
            }),
        }))
        .await
        .expect("list roles")
        .into_inner();
    assert!(
        listed_roles
            .roles
            .iter()
            .any(|listed| listed.role_id == role.role_id)
    );

    let updated_role = authz
        .update_role(Request::new(authz_pb::UpdateRoleRequest {
            role_id: role.role_id.clone(),
            name: "Auditor Updated".to_string(),
            description: "Updated by live admin test".to_string(),
            is_active: Some(true),
            updated_by: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("update role")
        .into_inner()
        .role
        .expect("updated role");
    assert_eq!(updated_role.name, "Auditor Updated");

    let direct_policy = authz
        .create_policy_rule(Request::new(authz_pb::CreatePolicyRuleRequest {
            subject: user.user_id.clone(),
            domain: "acme".to_string(),
            object: "report".to_string(),
            action: "data.export".to_string(),
            effect: authz_entity_pb::PolicyEffect::Allow as i32,
            description: "Direct live permission".to_string(),
            created_by: user.user_id.clone(),
            tenant_id: "acme".to_string(),
            resource_type: "report".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create direct policy rule")
        .into_inner()
        .policy
        .expect("direct policy");

    let got_policy = authz
        .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
            policy_id: direct_policy.policy_id.clone(),
        }))
        .await
        .expect("get policy rule")
        .into_inner()
        .policy
        .expect("got policy");
    assert_eq!(got_policy.subject, user.user_id);
    assert_eq!(
        got_policy.effect,
        authz_entity_pb::PolicyEffect::Allow as i32
    );

    let listed_policies = authz
        .list_policy_rules(Request::new(authz_pb::ListPolicyRulesRequest {
            domain: "acme".to_string(),
            subject: user.user_id.clone(),
            object: "report".to_string(),
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list policy rules")
        .into_inner();
    assert_eq!(listed_policies.policies.len(), 1);

    let allowed = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            object: "report".to_string(),
            action: "data.export".to_string(),
            ..Default::default()
        }))
        .await
        .expect("direct policy check")
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.matched_rule, direct_policy.policy_id);

    let batch = authz
        .batch_check_permissions(Request::new(authz_pb::BatchCheckPermissionsRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            checks: vec![
                authz_pb::PermissionCheck {
                    object: "report".to_string(),
                    action: "data.export".to_string(),
                },
                authz_pb::PermissionCheck {
                    object: "report".to_string(),
                    action: "data.delete".to_string(),
                },
            ],
            ..Default::default()
        }))
        .await
        .expect("batch permission checks")
        .into_inner();
    assert_eq!(batch.results.get("report:data.export"), Some(&true));
    assert_eq!(batch.results.get("report:data.delete"), Some(&false));

    let effective = authz
        .list_user_permissions(Request::new(authz_pb::ListUserPermissionsRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            ..Default::default()
        }))
        .await
        .expect("list effective user permissions")
        .into_inner();
    assert!(
        effective
            .permissions
            .iter()
            .any(|permission| permission.object == "report" && permission.action == "data.export")
    );

    let denied = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            tenant_id: "acme".to_string(),
            object: "secret".to_string(),
            action: "data.delete".to_string(),
            ..Default::default()
        }))
        .await
        .expect("denied access check")
        .into_inner();
    assert!(!denied.allowed);

    let audits = authz
        .list_access_decision_audits(Request::new(authz_pb::ListAccessDecisionAuditsRequest {
            user_id: user.user_id.clone(),
            domain: "acme".to_string(),
            page: Some(common_pb::PageRequest {
                page_size: 10,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("list decision audits")
        .into_inner();
    assert!(audits.audits.iter().any(|audit| audit.object == "secret"));

    let foreign_policy_delete = authz
        .delete_policy_rule(authz_tenant_request(
            authz_pb::DeletePolicyRuleRequest {
                policy_id: direct_policy.policy_id.clone(),
                deleted_by: user.user_id.clone(),
            },
            "other-tenant",
        ))
        .await
        .expect("foreign tenant delete remains an idempotent no-op")
        .into_inner();
    assert!(
        !foreign_policy_delete.deleted,
        "a foreign tenant must not delete a policy by identifier"
    );

    let deleted_policy = authz
        .delete_policy_rule(authz_tenant_request(
            authz_pb::DeletePolicyRuleRequest {
                policy_id: direct_policy.policy_id.clone(),
                deleted_by: user.user_id.clone(),
            },
            "acme",
        ))
        .await
        .expect("delete policy rule")
        .into_inner();
    assert!(deleted_policy.deleted);
    let missing_policy = authz
        .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
            policy_id: direct_policy.policy_id,
        }))
        .await
        .expect_err("deleted policy should not be returned");
    assert_eq!(missing_policy.code(), tonic::Code::NotFound);

    let foreign_role_delete = authz
        .delete_role(authz_tenant_request(
            authz_pb::DeleteRoleRequest {
                role_id: role.role_id.clone(),
                deleted_by: user.user_id.clone(),
            },
            "other-tenant",
        ))
        .await
        .expect("foreign tenant role delete remains an idempotent no-op")
        .into_inner();
    assert!(
        !foreign_role_delete.deleted,
        "a foreign tenant must not delete a role by identifier"
    );

    let deleted_role = authz
        .delete_role(authz_tenant_request(
            authz_pb::DeleteRoleRequest {
                role_id: role.role_id.clone(),
                deleted_by: user.user_id,
            },
            "acme",
        ))
        .await
        .expect("delete role")
        .into_inner();
    assert!(deleted_role.deleted);

    let active_roles = authz
        .list_roles(Request::new(authz_pb::ListRolesRequest {
            domain: "acme".to_string(),
            active_only: true,
            ..Default::default()
        }))
        .await
        .expect("list roles after delete")
        .into_inner();
    assert!(
        !active_roles
            .roles
            .iter()
            .any(|listed| listed.role_id == role.role_id)
    );

    cleanup_native_auth_db(&pool).await;
}

/// §1 read-after-write served-path contract (13.7.1.1): the `policy_id`
/// `CreatePolicyRule` returns is IMMEDIATELY readable by `GetPolicyRule` on the SAME
/// served path with the SAME tenant metadata a client uses. Reverting that broker
/// guarantee (a create that returns an id not gettable) fails this test.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_create_policy_rule_read_after_write -- --ignored --nocapture"]
async fn live_postgres_authz_create_policy_rule_read_after_write() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "ryw_policy", "CorrectHorse1!").await;

    let created = authz
        .create_policy_rule(Request::new(authz_pb::CreatePolicyRuleRequest {
            subject: user.user_id.clone(),
            domain: "acme".to_string(),
            object: "ledger".to_string(),
            action: "data.read".to_string(),
            effect: authz_entity_pb::PolicyEffect::Allow as i32,
            description: "RYW live permission".to_string(),
            created_by: user.user_id.clone(),
            tenant_id: "acme".to_string(),
            resource_type: "ledger".to_string(),
            ..Default::default()
        }))
        .await
        .expect("create_policy_rule")
        .into_inner()
        .policy
        .expect("created policy");

    // The id create returned must be NON-EMPTY and immediately gettable on the
    // SAME served path, round-tripping the SAME id.
    assert!(
        !created.policy_id.is_empty(),
        "CreatePolicyRule must return a non-empty policy_id"
    );
    let got = authz
        .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
            policy_id: created.policy_id.clone(),
        }))
        .await
        .expect("CreatePolicyRule→GetPolicyRule must resolve the returned id")
        .into_inner()
        .policy
        .expect("got policy");
    assert_eq!(
        got.policy_id, created.policy_id,
        "GetPolicyRule must return the SAME id CreatePolicyRule issued (read-after-write)"
    );
    assert_eq!(got.subject, user.user_id);
    assert_eq!(got.object, "ledger");

    cleanup_native_auth_db(&pool).await;
}

/// §1 read-after-write served-path contract (13.7.1.1), governed path: a frozen
/// governance policy document must preserve each policy's own id through
/// CreatePolicyDraft → SubmitPolicyDraft → ApprovePolicyDraft → ActivatePolicyVersion
/// and make that SAME id immediately readable by GetPolicyRule. Reverting activation
/// to re-mint ids with `gen_random_uuid()` fails this test.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_governance_activate_policy_read_after_write -- --ignored --nocapture"]
async fn live_postgres_authz_governance_activate_policy_read_after_write() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let author = create_verified_user(&authn, "gov_author", "CorrectHorse1!").await;
    let reviewer = create_verified_user(&authn, "gov_reviewer", "CorrectHorse1!").await;
    let suffix = Uuid::new_v4().simple().to_string();
    let policy_id = Uuid::new_v4().to_string();
    let governed_resource = format!("governed-ledger-{suffix}");

    let draft = authz
        .create_policy_draft(Request::new(authz_pb::CreatePolicyDraftRequest {
            actor: Some(live_governance_actor(&author.user_id)),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            policy_set_name: format!("ryw-governance-{suffix}"),
            title: "Governed RYW live permission".to_string(),
            change_reason: "prove activation preserves policy id".to_string(),
            high_risk: true,
            document: Some(authz_pb::PolicyDocument {
                policies: vec![authz_pb::AuthzPolicyRecord {
                    id: policy_id.clone(),
                    enabled: true,
                    effect: "allow".to_string(),
                    tenant: "acme".to_string(),
                    project: "billing".to_string(),
                    subject: reviewer.user_id.clone(),
                    action: "data.read".to_string(),
                    resource: governed_resource.clone(),
                    required_scopes: vec!["ledger:read".to_string()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            branch_from_active: false,
        }))
        .await
        .expect("CreatePolicyDraft must accept the frozen policy document")
        .into_inner()
        .draft
        .expect("created policy draft");

    authz
        .submit_policy_draft(Request::new(authz_pb::SubmitPolicyDraftRequest {
            actor: Some(live_governance_actor(&author.user_id)),
            draft_id: draft.draft_id.clone(),
            expected_updated_at_unix: draft.updated_at.as_ref().map(|ts| ts.seconds).unwrap_or(0),
        }))
        .await
        .expect("SubmitPolicyDraft must move the draft into review");

    let version = authz
        .approve_policy_draft(Request::new(authz_pb::ApprovePolicyDraftRequest {
            actor: Some(live_governance_actor(&reviewer.user_id)),
            draft_id: draft.draft_id,
            reviewer: reviewer.user_id.clone(),
            reason: "distinct reviewer approves governed RYW proof".to_string(),
        }))
        .await
        .expect("ApprovePolicyDraft must promote the approved draft")
        .into_inner()
        .version
        .expect("approved draft must produce a policy version");

    authz
        .activate_policy_version(Request::new(authz_pb::ActivatePolicyVersionRequest {
            actor: Some(live_governance_actor(&reviewer.user_id)),
            policy_version_id: version.policy_version_id.clone(),
            expected_revision: version.revision,
            ..Default::default()
        }))
        .await
        .expect("ActivatePolicyVersion must apply the frozen document");

    let got = authz
        .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
            policy_id: policy_id.clone(),
        }))
        .await
        .expect("ActivatePolicyVersion→GetPolicyRule must resolve the original document id")
        .into_inner()
        .policy
        .expect("activated policy must be readable by id");
    assert_eq!(
        got.policy_id, policy_id,
        "activated governance policy must be readable by its original id"
    );
    assert_eq!(got.subject, reviewer.user_id);
    assert_eq!(got.object, governed_resource);
    assert_eq!(got.action, "data.read");

    cleanup_native_auth_db(&pool).await;
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_role_binding_authorize_and_lint -- --ignored --nocapture"]
async fn live_postgres_authz_role_binding_authorize_and_lint() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "rolebind", "CorrectHorse1!").await;
    let suffix = Uuid::new_v4().simple().to_string();
    let role_code = format!("binder_{suffix}");

    // put_role_binding writes a grouping tuple (subject -> role) to Postgres.
    authz
        .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest {
            binding: Some(authz_pb::RoleBinding {
                subject: user.user_id.clone(),
                role: role_code.clone(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                expires_at_unix: 0,
                source: "live_test".to_string(),
            }),
        }))
        .await
        .expect("put_role_binding");

    // A role-scoped allow policy that the binding satisfies.
    let policy_id = Uuid::new_v4().to_string();
    authz
        .put_authz_policy(Request::new(authz_pb::PutAuthzPolicyRequest {
            policy: Some(authz_pb::AuthzPolicyRecord {
                id: policy_id,
                enabled: true,
                effect: "allow".to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                role: role_code,
                action: "data.update".to_string(),
                resource: "invoice".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("put role policy");

    let principal = || {
        Some(authz_pb::Principal {
            principal_id: user.user_id.clone(),
            subject: user.user_id.clone(),
            user_id: user.user_id.clone(),
            tenant_id: "acme".to_string(),
            project_id: "billing".to_string(),
            ..Default::default()
        })
    };
    let authz_req = |action: &str| authz_pb::AuthzRequest {
        principal: principal(),
        tenant_id: "acme".to_string(),
        project_id: "billing".to_string(),
        domain: "acme".to_string(),
        resource: Some(authz_pb::ResourceRef {
            resource_name: "invoice".to_string(),
            ..Default::default()
        }),
        action: action.to_string(),
        ..Default::default()
    };

    // Direct Authorize (not proxied through CheckAccess) resolves the binding.
    let allowed = authz
        .authorize(Request::new(authz_req("data.update")))
        .await
        .expect("authorize allow")
        .into_inner()
        .decision
        .expect("decision");
    assert!(allowed.allowed, "role-bound principal must be authorized");

    let denied = authz
        .authorize(Request::new(authz_req("data.delete")))
        .await
        .expect("authorize deny")
        .into_inner()
        .decision
        .expect("decision");
    assert!(!denied.allowed, "unbound action must be denied");

    // lint_authz_policies runs over the live policy set without error.
    authz
        .lint_authz_policies(Request::new(authz_pb::LintAuthzPoliciesRequest {}))
        .await
        .expect("lint_authz_policies");

    cleanup_native_auth_db(&pool).await;
}
