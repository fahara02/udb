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

    let revision_model = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.AuthzRevision",
        &["tenant_id"],
    );
    let revision_count_sql = format!("SELECT COUNT(*) FROM {}", revision_model.relation);
    let revisions_before: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions before foreign policy delete");
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
    let revisions_after: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions after foreign policy delete");
    assert_eq!(
        revisions_after, revisions_before,
        "a foreign policy delete must not append an authz revision"
    );
    let retained_policy = authz
        .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
            policy_id: direct_policy.policy_id.clone(),
        }))
        .await
        .expect("foreign delete must retain the owner's policy")
        .into_inner()
        .policy
        .expect("retained policy");
    assert_eq!(retained_policy.policy_id, direct_policy.policy_id);

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

    let revisions_before: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions before foreign role delete");
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
    let revisions_after: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions after foreign role delete");
    assert_eq!(
        revisions_after, revisions_before,
        "a foreign role delete must not append an authz revision"
    );
    let retained_role = authz
        .get_role(Request::new(authz_pb::GetRoleRequest {
            role_id: role.role_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("foreign delete must retain the owner's role")
        .into_inner()
        .role
        .expect("retained role");
    assert!(retained_role.is_active);

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

/// Raw SQL read adapters must retain the shared retryable database refusal.
/// Closing the actual configured PG pool exercises the handler/store boundary;
/// reverting a mapper to generic Internal fails the corresponding assertion.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_reads_preserve_retryable_store_refusals -- --ignored --nocapture"]
async fn live_postgres_authz_reads_preserve_retryable_store_refusals() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authz = authz_service(pool.clone())
        .await
        .with_postgres(Some(pool.clone()));
    let id = Uuid::new_v4().to_string();
    pool.close().await;
    let refusals = [
        (
            "GetRole",
            authz
                .get_role(Request::new(authz_pb::GetRoleRequest {
                    role_id: id.clone(),
                    ..Default::default()
                }))
                .await
                .expect_err("closed role store must refuse the actual read"),
        ),
        (
            "ListRoles",
            authz
                .list_roles(Request::new(authz_pb::ListRolesRequest::default()))
                .await
                .expect_err("closed role store must refuse the actual list"),
        ),
        (
            "GetPolicyRule",
            authz
                .get_policy_rule(Request::new(authz_pb::GetPolicyRuleRequest {
                    policy_id: id.clone(),
                }))
                .await
                .expect_err("closed policy store must refuse the actual read"),
        ),
        (
            "ListPolicyRules",
            authz
                .list_policy_rules(Request::new(authz_pb::ListPolicyRulesRequest::default()))
                .await
                .expect_err("closed policy store must refuse the actual list"),
        ),
        (
            "ListUserRoles",
            authz
                .list_user_roles(Request::new(authz_pb::ListUserRolesRequest {
                    user_id: id.clone(),
                    ..Default::default()
                }))
                .await
                .expect_err("closed assignment store must refuse the actual list"),
        ),
        (
            "ListAccessDecisionAudits",
            authz
                .list_access_decision_audits(Request::new(
                    authz_pb::ListAccessDecisionAuditsRequest::default(),
                ))
                .await
                .expect_err("closed audit store must refuse the actual list"),
        ),
        (
            "CheckAccess",
            authz
                .check_access(Request::new(authz_pb::CheckAccessRequest {
                    user_id: id.clone(),
                    tenant_id: "acme".to_string(),
                    domain: "acme".to_string(),
                    object: "invoice".to_string(),
                    action: "data.select".to_string(),
                    ..Default::default()
                }))
                .await
                .expect_err("closed snapshot store must refuse the actual decision"),
        ),
        (
            "UpdateRole",
            authz
                .update_role(Request::new(authz_pb::UpdateRoleRequest {
                    role_id: id.clone(),
                    name: "Updated role".to_string(),
                    updated_by: id,
                    ..Default::default()
                }))
                .await
                .expect_err("closed authority store must refuse before any mutation"),
        ),
    ];
    let cleanup_pool = live_pg_pool().await;
    cleanup_native_auth_db(&cleanup_pool).await;
    for (rpc, refusal) in refusals {
        assert_eq!(
            refusal.code(),
            tonic::Code::Unavailable,
            "{rpc} must preserve the store's retryable code"
        );
        let raw = refusal
            .metadata()
            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
            .expect("actual refusal retains its typed trailer");
        let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
        assert_eq!(
            detail.reason,
            crate::runtime::error_reasons::BACKEND_UNAVAILABLE.code,
            "{rpc} must preserve the stable store reason"
        );
        assert_eq!(detail.kind, crate::proto::ErrorKind::Retryable as i32);
        assert!(detail.retryable, "{rpc} must retain retry advice");
        assert_eq!(
            detail.retry_after_ms,
            crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS
        );
        assert_eq!(detail.backend, "database");
        assert!(!detail.operation.is_empty());
    }
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

    // Incoming documents must use the same effect validation as direct policy
    // writes. Exercise all four governance handlers, including the update of
    // this actual stored draft, before continuing the successful lifecycle.
    let rejected_set_name = format!("invalid-effect-{suffix}");
    for effect in ["", "maybe", "ALLOW "] {
        let document = authz_pb::PolicyDocument {
            policies: vec![authz_pb::AuthzPolicyRecord {
                id: policy_id.clone(),
                enabled: true,
                effect: effect.to_string(),
                tenant: "acme".to_string(),
                project: "billing".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let refusals = [
            authz
                .create_policy_draft(Request::new(authz_pb::CreatePolicyDraftRequest {
                    actor: Some(live_governance_actor(&author.user_id)),
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    policy_set_name: rejected_set_name.clone(),
                    document: Some(document.clone()),
                    ..Default::default()
                }))
                .await
                .expect_err("unknown effect must refuse draft creation"),
            authz
                .update_policy_draft(Request::new(authz_pb::UpdatePolicyDraftRequest {
                    actor: Some(live_governance_actor(&author.user_id)),
                    draft_id: draft.draft_id.clone(),
                    document: Some(document.clone()),
                    ..Default::default()
                }))
                .await
                .expect_err("unknown effect must refuse stored draft update"),
            authz
                .simulate_policy(Request::new(authz_pb::SimulatePolicyRequest {
                    actor: Some(live_governance_actor(&author.user_id)),
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    candidate: Some(document.clone()),
                    ..Default::default()
                }))
                .await
                .expect_err("unknown effect must refuse policy simulation"),
            authz
                .explain_policy(Request::new(authz_pb::ExplainPolicyRequest {
                    actor: Some(live_governance_actor(&author.user_id)),
                    tenant_id: "acme".to_string(),
                    project_id: "billing".to_string(),
                    candidate: Some(document),
                    ..Default::default()
                }))
                .await
                .expect_err("unknown effect must refuse policy explanation"),
        ];
        for err in refusals {
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(
                err.metadata()
                    .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
                    .expect("typed invalid effect detail"),
            );
            assert_eq!(detail.field_violations.len(), 1);
            assert_eq!(detail.field_violations[0].field, "policy.effect");
        }
    }
    let policy_sets = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicySet",
        &["name"],
    );
    let rejected_sets: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {} WHERE {} = $1",
        policy_sets.relation,
        policy_sets.q("name"),
    ))
    .bind(&rejected_set_name)
    .fetch_one(&pool)
    .await
    .expect("inspect rejected draft policy sets");
    assert_eq!(
        rejected_sets, 0,
        "invalid effect must not create a policy set"
    );
    let drafts = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyDraft",
        &["draft_id", "proposed_policies_json", "updated_at"],
    );
    let unchanged: (String, i64) = sqlx::query_as(&format!(
        "SELECT {}, EXTRACT(EPOCH FROM {})::BIGINT FROM {} WHERE {} = $1::UUID",
        drafts.json_text_as("proposed_policies_json", "proposed_policies_json"),
        drafts.q("updated_at"),
        drafts.relation,
        drafts.q("draft_id"),
    ))
    .bind(&draft.draft_id)
    .fetch_one(&pool)
    .await
    .expect("read stored draft after invalid effect refusals");
    assert_eq!(
        unchanged.0, draft.proposed_policies_json,
        "invalid effect must not replace stored draft policies"
    );
    assert_eq!(
        unchanged.1,
        draft
            .updated_at
            .as_ref()
            .expect("draft update time")
            .seconds,
        "invalid effect must not advance draft update time"
    );

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

    // Corrupt the actual frozen store value, then call the served activation
    // handler. No partial document may activate or advance durable revisions.
    let versions = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyVersion",
        &["policy_version_id", "payload_json", "state"],
    );
    let revisions = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.AuthzRevision",
        &["tenant_id"],
    );
    let policies = authz.policies_model();
    let revision_count_sql = format!("SELECT COUNT(*) FROM {}", revisions.relation);
    let state_sql = format!(
        "SELECT {} FROM {} WHERE {} = $1::UUID",
        versions.q("state"),
        versions.relation,
        versions.q("policy_version_id"),
    );
    let policy_count_sql = format!(
        "SELECT COUNT(*) FROM {} WHERE {} = $1::UUID",
        policies.relation,
        policies.q("policy_id"),
    );
    let payload_sql = format!(
        "UPDATE {} SET {} = $1::JSONB WHERE {} = $2::UUID",
        versions.relation,
        versions.q("payload_json"),
        versions.q("policy_version_id"),
    );
    let before_revisions: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions before corrupt activation");
    let before_state: String = sqlx::query_scalar(&state_sql)
        .bind(&version.policy_version_id)
        .fetch_one(&pool)
        .await
        .expect("read approved version state");
    let canonical: serde_json::Value =
        serde_json::from_str(&version.payload_json).expect("canonical frozen policy document");
    let mut bad_effect = canonical.clone();
    bad_effect["policies"][0]["effect"] = serde_json::json!("private-invalid-effect");
    let mut bad_condition = canonical.clone();
    bad_condition["policies"][0]["conditions"] = serde_json::json!({"classification": 42});
    let mut bad_array = canonical;
    bad_array["policies"] = serde_json::json!({});
    let mut refusals = Vec::new();
    for malformed in [
        serde_json::Value::Null,
        bad_effect,
        bad_condition,
        bad_array,
    ] {
        sqlx::query(&payload_sql)
            .bind(malformed.to_string())
            .bind(&version.policy_version_id)
            .execute(&pool)
            .await
            .expect("write actual corrupt frozen document");
        refusals.push(
            authz
                .activate_policy_version(Request::new(authz_pb::ActivatePolicyVersionRequest {
                    actor: Some(live_governance_actor(&reviewer.user_id)),
                    policy_version_id: version.policy_version_id.clone(),
                    expected_revision: version.revision,
                    ..Default::default()
                }))
                .await,
        );
    }
    let after_revisions: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions after corrupt activation");
    let after_state: String = sqlx::query_scalar(&state_sql)
        .bind(&version.policy_version_id)
        .fetch_one(&pool)
        .await
        .expect("read version after corrupt activation");
    let applied_policies: i64 = sqlx::query_scalar(&policy_count_sql)
        .bind(&policy_id)
        .fetch_one(&pool)
        .await
        .expect("count policies after corrupt activation");
    sqlx::query(&payload_sql)
        .bind(&version.payload_json)
        .bind(&version.policy_version_id)
        .execute(&pool)
        .await
        .expect("restore canonical frozen document");
    let all_typed_refusals = refusals.iter().all(|result| {
        let Err(err) = result else {
            return false;
        };
        let detail = err
            .metadata()
            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
            .and_then(|raw| raw.to_bytes().ok())
            .map(|raw| crate::runtime::executor_utils::decode_error_detail_from_raw(&raw));
        err.code() == crate::runtime::error_reasons::DECODE_FAILED.status
            && !err.message().contains("private-invalid-effect")
            && detail.is_some_and(|detail| {
                detail.reason == crate::runtime::error_reasons::DECODE_FAILED.code
                    && detail.column == "payload_json"
            })
    });
    if !all_typed_refusals
        || after_revisions != before_revisions
        || after_state != before_state
        || applied_policies != 0
    {
        cleanup_native_auth_db(&pool).await;
    }
    assert!(
        all_typed_refusals,
        "corrupt frozen policy document must refuse activation"
    );
    assert_eq!(
        after_revisions, before_revisions,
        "corrupt activation must not append a revision"
    );
    assert_eq!(
        after_state, before_state,
        "corrupt activation must retain the approved version"
    );
    assert_eq!(
        applied_policies, 0,
        "corrupt activation must not apply any policy"
    );

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

/// Raw privileged read of one stored policy row: `(tenant_id, domain)`.
async fn raw_policy_tenant_and_domain(
    pool: &sqlx::PgPool,
    policy_id: &str,
) -> Option<(String, String)> {
    let model = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &["policy_id", "domain", "tenant_id"],
    );
    sqlx::query_as(&format!(
        "SELECT COALESCE({tenant_id}, ''), COALESCE({domain}, '') FROM {rel} \
         WHERE {policy_id} = $1::UUID",
        tenant_id = model.q("tenant_id"),
        domain = model.q("domain"),
        rel = model.relation,
        policy_id = model.q("policy_id"),
    ))
    .bind(policy_id)
    .fetch_optional(pool)
    .await
    .expect("raw policy row read")
}

/// B3 — a tenant-bound policy admin (tenant A) cannot overwrite a policy id
/// that tenant B owns through PutAuthzPolicy: PermissionDenied, and the stored
/// row keeps tenant B.
///
/// Revert-proof: drop `check_policy_overwrite_boundary` from put_authz_policy
/// and the upsert on `policy_id` re-homes the row into tenant A.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_put_authz_policy_refuses_foreign_policy_id_overwrite -- --ignored --nocapture"]
async fn live_postgres_put_authz_policy_refuses_foreign_policy_id_overwrite() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authz = authz_service(pool.clone()).await;
    let tenant_a = Uuid::new_v4().to_string();
    let tenant_b = Uuid::new_v4().to_string();
    let policy_id = Uuid::new_v4().to_string();
    let record = |tenant: &str, project: &str, resource: &str| authz_pb::PutAuthzPolicyRequest {
        policy: Some(authz_pb::AuthzPolicyRecord {
            id: policy_id.clone(),
            enabled: true,
            effect: "allow".to_string(),
            tenant: tenant.to_string(),
            project: project.to_string(),
            subject: "svc-b3".to_string(),
            action: "Select".to_string(),
            resource: resource.to_string(),
            ..Default::default()
        }),
    };

    // Tenant B's policy, written in-process (no claim = unrestricted).
    authz
        .put_authz_policy(Request::new(record(
            &tenant_b,
            "owner-project",
            "acme.b3.v1.Invoice",
        )))
        .await
        .expect("seed the tenant B policy");
    assert_eq!(
        raw_policy_tenant_and_domain(&pool, &policy_id)
            .await
            .map(|(tenant, _)| tenant),
        Some(tenant_b.clone())
    );

    // Tenant A's admin aims the same id at its own tenant.
    let ctx = crate::runtime::service::method_security::test_claim_context(
        "policy-admin-a",
        &tenant_a,
        "",
        &["udb:authz:admin"],
        &[],
    );
    let err = crate::runtime::service::method_security::scope_claim_context_for_test(
        ctx,
        authz.put_authz_policy(Request::new(record(&tenant_a, "", "acme.b3.v1.Payroll"))),
    )
    .await
    .expect_err("a tenant-bound admin must not overwrite another tenant's policy id");
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err:?}");
    assert_eq!(
        raw_policy_tenant_and_domain(&pool, &policy_id)
            .await
            .map(|(tenant, _)| tenant),
        Some(tenant_b.clone()),
        "the stored policy must keep tenant B"
    );

    // The tenant ownership pre-read allows this caller. The shared compiler
    // must still skip the conflicting row because its project belongs to a
    // different scope; that skip must roll back before any revision append.
    let revisions = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.AuthzRevision",
        &["tenant_id"],
    );
    let revision_count_sql = format!("SELECT COUNT(*) FROM {}", revisions.relation);
    let revisions_before: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions before scope-skipped upsert");
    let project_context = crate::runtime::service::method_security::test_claim_context(
        "policy-admin-b",
        &tenant_b,
        "other-project",
        &["udb:authz:admin"],
        &[],
    );
    let skipped = crate::runtime::service::method_security::scope_claim_context_for_test(
        project_context,
        authz.put_authz_policy(Request::new(record(
            &tenant_b,
            "other-project",
            "acme.b3.v1.Payroll",
        ))),
    )
    .await;
    let revisions_after: i64 = sqlx::query_scalar(&revision_count_sql)
        .fetch_one(&pool)
        .await
        .expect("count revisions after scope-skipped upsert");
    let policy = authz.policies_model();
    let retained: (String, String, String) = sqlx::query_as(&format!(
        "SELECT {tenant}::TEXT, {project}, {resource} FROM {rel} WHERE {id} = $1::UUID",
        tenant = policy.q("tenant_id"),
        project = policy.q("project_id"),
        resource = policy.q("object"),
        rel = policy.relation,
        id = policy.q("policy_id"),
    ))
    .bind(Uuid::parse_str(&policy_id).expect("valid policy id"))
    .fetch_one(&pool)
    .await
    .expect("read back the original owner policy");
    cleanup_native_auth_db(&pool).await;
    let err = skipped.expect_err("scope-skipped policy replacement must refuse");
    assert_eq!(
        err.code(),
        crate::runtime::error_reasons::NO_ROWS_AFFECTED.status
    );
    let raw = err
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("scope refusal must carry the actual ErrorDetail")
        .to_bytes()
        .expect("valid detail metadata");
    let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
    assert_eq!(
        detail.reason,
        crate::runtime::error_reasons::NO_ROWS_AFFECTED.code
    );
    assert_eq!(
        revisions_after, revisions_before,
        "scope-skipped upsert must not append a revision"
    );
    assert_eq!(
        retained,
        (
            tenant_b,
            "owner-project".to_string(),
            "acme.b3.v1.Invoice".to_string()
        ),
        "owner policy must retain its project and resource",
    );
}

/// B7 — CreatePolicyRule with a `tenant:<uuid>` domain (any spacing) stores
/// BOTH `tenant_id` and the `domain` column normalized, and the rule then
/// authorizes the bare tenant id the caller's claim carries.
///
/// Revert-proof: store `req.domain` verbatim and the raw domain column reads
/// back as `" tenant: <uuid> "`.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_create_policy_rule_stores_domain_normalized -- --ignored --nocapture"]
async fn live_postgres_create_policy_rule_stores_domain_normalized() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let authn = authn_service(pool.clone());
    let authz = authz_service(pool.clone()).await;
    let user = create_verified_user(&authn, "authz_b7", "CorrectHorse1!").await;
    let tenant = Uuid::new_v4().to_string();

    let created = authz
        .create_policy_rule(Request::new(authz_pb::CreatePolicyRuleRequest {
            subject: user.user_id.clone(),
            domain: format!(" tenant: {tenant} "),
            object: "acme.b7.v1.Invoice".to_string(),
            action: "Select".to_string(),
            effect: authz_entity_pb::PolicyEffect::Allow as i32,
            description: "B7 normalized domain".to_string(),
            created_by: user.user_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("create policy rule with a tenant domain")
        .into_inner()
        .policy
        .expect("created policy");
    assert_eq!(created.domain, format!("tenant:{tenant}"));
    assert_eq!(
        raw_policy_tenant_and_domain(&pool, &created.policy_id).await,
        Some((tenant.clone(), format!("tenant:{tenant}"))),
        "tenant_id AND domain must be stored normalized"
    );

    let decision = authz
        .check_access(Request::new(authz_pb::CheckAccessRequest {
            user_id: user.user_id.clone(),
            domain: tenant.clone(),
            tenant_id: tenant.clone(),
            object: "acme.b7.v1.Invoice".to_string(),
            action: "Select".to_string(),
            ..Default::default()
        }))
        .await
        .expect("check access under the normalized tenant")
        .into_inner();
    assert!(
        decision.allowed,
        "the normalized rule must authorize the bare tenant id: {}",
        decision.reason
    );

    cleanup_native_auth_db(&pool).await;
}

/// B10 — the policy ids `udb authz seed --emit` writes are exactly the ids the
/// offline seed stores: both derive them through `seed_authz_policy_id`, and
/// the stored rows carry exactly that set (empty subject, role in attributes).
///
/// Revert-proof: give the seed its own id derivation again (or emit a
/// synthetic `udb-authz-seed:` label) and the stored set no longer equals the
/// derived set.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_authz_seed_ids_match_emitted_ids -- --ignored --nocapture"]
async fn live_postgres_authz_seed_ids_match_emitted_ids() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_auth_db(&pool).await;
    let tenant = Uuid::new_v4().to_string();
    let project = "b10-project";
    let role = "app_rw";
    let objects = vec!["*".to_string(), "acme.b10.v1.Invoice".to_string()];
    let actions = vec!["Select".to_string(), "Upsert".to_string()];

    let inserted = crate::runtime::service::auth_service::seed_project_authz_policies(
        &pool, &tenant, project, role, &actions, &objects,
    )
    .await
    .expect("seed project authz policies");
    assert_eq!(inserted, objects.len() * actions.len());

    let mut expected: Vec<String> = objects
        .iter()
        .flat_map(|object| {
            actions.iter().map(|action| {
                crate::runtime::service::seed_authz_policy_id(
                    &tenant, project, role, object, action,
                )
                .to_string()
            })
        })
        .collect();
    expected.sort();

    let model = crate::runtime::native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &["policy_id", "subject", "tenant_id", "project_id"],
    );
    let mut stored: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT {policy_id}::TEXT, COALESCE({subject}, '') FROM {rel} \
         WHERE {tenant_id} = $1 AND {project_id} = $2",
        policy_id = model.q("policy_id"),
        subject = model.q("subject"),
        rel = model.relation,
        tenant_id = model.q("tenant_id"),
        project_id = model.q("project_id"),
    ))
    .bind(&tenant)
    .bind(project)
    .fetch_all(&pool)
    .await
    .expect("read seeded policy rows");
    stored.sort();
    assert!(
        stored.iter().all(|(_, subject)| subject.is_empty()),
        "the seed stores an empty subject (the role carries the grant)"
    );
    let stored_ids: Vec<String> = stored.into_iter().map(|(id, _)| id).collect();
    assert_eq!(
        stored_ids, expected,
        "the stored policy ids must be exactly the ids `authz seed --emit` writes"
    );

    // Re-seeding is a no-op because the ids are stable.
    let again = crate::runtime::service::auth_service::seed_project_authz_policies(
        &pool, &tenant, project, role, &actions, &objects,
    )
    .await
    .expect("re-seed");
    assert_eq!(again, 0);

    cleanup_native_auth_db(&pool).await;
}
