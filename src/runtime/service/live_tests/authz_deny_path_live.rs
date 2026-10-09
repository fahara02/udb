//! LIVE served deny-path profile for the data-plane authorization gate.
//!
//! Every other served live test builds the broker with default-allow on, so no
//! test ever proved that a NARROW policy set is enforced as narrow. This one
//! builds the broker the way production does — deny-by-default snapshot, the
//! AuthzService PG-warming that SAME shared cell from `udb_authz.policy_rules`
//! — seeds ONE narrow allow row, and drives `DataBrokerService::authorize` (the
//! gate every served data RPC calls) to assert:
//!
//! * the narrow grant allows exactly its (subject, tenant, project, object, action);
//! * a different action / table / subject / tenant is denied;
//! * a different PROJECT is denied (project-bound allow rules are honored);
//! * a caller-supplied `"*"` / empty message type is refused, never a bypass.
//!
//! Run with a live Postgres:
//!   UDB_LIVE_NATIVE_PG_DSN=postgres://udb:udb@127.0.0.1:55432/udb \
//!     cargo test --lib authz_deny_path -- --ignored --nocapture

use super::support::{
    live_native_service_db_lock, live_pg_dsn, live_pg_pool, migrate_native_service_db,
};
use crate::runtime::catalog::DEFAULT_PROJECT_ID;
use crate::runtime::config::{DbConfig, UdbConfig};
use crate::runtime::security::SecurityContext;
use crate::runtime::service::DataBrokerService;
use crate::runtime::{DataBrokerRuntime, native_catalog};
use uuid::Uuid;

const INVOICE: &str = "acme.b11.v1.Invoice";
const SALARY: &str = "acme.b11.v1.Salary";
const SUBJECT: &str = "svc-b11-reader";
const OTHER_PROJECT: &str = "b11-other-project";

async fn deny_path_broker() -> DataBrokerService {
    let config = UdbConfig {
        primary: DbConfig {
            direct_dsn: live_pg_dsn(),
            ..DbConfig::default()
        },
        ..UdbConfig::default()
    };
    DataBrokerService::with_runtime(
        native_catalog::native_manifest().clone(),
        DataBrokerRuntime::from_config(config).await,
    )
}

/// Insert one ALLOW row the way the AuthzService stores it: a direct subject,
/// a tenant + project scope, an exact object and an RPC-method action.
pub(super) async fn insert_allow_rule(
    pool: &sqlx::PgPool,
    tenant: &str,
    project: &str,
    subject: &str,
    object: &str,
    action: &str,
) -> String {
    let policy = native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &[
            "policy_id",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "condition",
            "description",
            "is_active",
            "tenant_id",
            "project_id",
            "attributes_json",
        ],
    );
    let policy_id = Uuid::new_v4();
    let sql = format!(
        "INSERT INTO {rel} \
           ({policy_id}, {subject}, {domain}, {object}, {action}, {effect}, {condition}, {description}, {is_active}, {tenant_id}, {project_id}, {attributes_json}) \
         VALUES ($1::UUID, $2, $3, $4, $5, 'ALLOW', '', 'b11 deny-path live', TRUE, $3, $6, '{{}}'::JSONB)",
        rel = policy.relation,
        policy_id = policy.q("policy_id"),
        subject = policy.q("subject"),
        domain = policy.q("domain"),
        object = policy.q("object"),
        action = policy.q("action"),
        effect = policy.q("effect"),
        condition = policy.q("condition"),
        description = policy.q("description"),
        is_active = policy.q("is_active"),
        tenant_id = policy.q("tenant_id"),
        project_id = policy.q("project_id"),
        attributes_json = policy.q("attributes_json"),
    );
    sqlx::query(&sql)
        .bind(policy_id)
        .bind(subject)
        .bind(tenant)
        .bind(object)
        .bind(action)
        .bind(project)
        .execute(pool)
        .await
        .unwrap_or_else(|err| panic!("insert narrow allow policy: {err}"));
    policy_id.to_string()
}

fn caller(tenant: &str, project: &str, subject: &str) -> SecurityContext {
    SecurityContext {
        tenant_id: tenant.to_string(),
        project_id: project.to_string(),
        purpose: "b11-deny-path".to_string(),
        service_identity: subject.to_string(),
        scopes: vec!["udb:read".to_string(), "udb:write".to_string()],
        ..SecurityContext::default()
    }
}

async fn set_rule_attributes(pool: &sqlx::PgPool, policy_id: &str, attributes: serde_json::Value) {
    let model = native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &["policy_id", "attributes_json"],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {attributes} = $2 WHERE {policy_id} = $1::UUID",
        rel = model.relation,
        attributes = model.q("attributes_json"),
        policy_id = model.q("policy_id"),
    ))
    .bind(policy_id)
    .bind(attributes)
    .execute(pool)
    .await
    .expect("set actual durable policy predicates");
}

fn denial_detail(status: &tonic::Status) -> crate::proto::ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("actual denial carries typed detail")
        .to_bytes()
        .expect("denial metadata decodes");
    crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
}

async fn assert_denied(
    svc: &DataBrokerService,
    ctx: &SecurityContext,
    message_type: &str,
    action: &str,
    why: &str,
    expected: &[(&str, &str)],
) -> tonic::Status {
    let err = svc
        .authorize(ctx, message_type, action)
        .await
        .expect_err(why);
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{why}: {err:?}");
    let detail = denial_detail(&err);
    assert_eq!(detail.kind, crate::proto::ErrorKind::Permission as i32);
    assert_eq!(detail.reason, "UDB_POLICY_DENIED");
    assert_eq!(
        detail.missing.get("rule"),
        Some(&format!("{action} {message_type}"))
    );
    for (attribute, expected) in expected {
        assert_eq!(
            detail.missing.get(*attribute).map(String::as_str),
            Some(*expected),
            "{why}: failed attribute {attribute}",
        );
    }
    let item = DataBrokerService::authorize_message_item(
        &svc.current_authz_snapshot(),
        ctx,
        message_type,
        action,
    )
    .await
    .expect_err("the batch item must refuse the same policy tuple");
    assert_eq!(item.code(), err.code());
    let item_detail = denial_detail(&item);
    assert_eq!(item_detail.reason, detail.reason);
    assert_eq!(item_detail.missing, detail.missing);
    assert_eq!(item_detail.policy_decision_id, detail.policy_decision_id);
    err
}

#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_NATIVE_PG_DSN=... -- --ignored"]
async fn authz_deny_path_narrow_policy_is_enforced_as_narrow_live() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;

    let tenant = Uuid::new_v4().to_string();
    let foreign_tenant = Uuid::new_v4().to_string();
    let policy_id = insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        INVOICE,
        "Select",
    )
    .await;
    set_rule_attributes(
        &pool,
        &policy_id,
        serde_json::json!({"priority": 10, "purpose": "b11-deny-path", "required_scopes": "udb:read"}),
    )
    .await;
    // An unrelated but ABAC-applicable policy must not suppress the diagnosis
    // of the closest rule that failed the caller's real tuple.
    insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        "acme.b11.v1.Unrelated",
        "Select",
    )
    .await;

    // Production wiring: the broker's shared snapshot cell starts deny-by-default
    // and the AuthzService PG-warms THAT cell.
    let svc = deny_path_broker().await;
    assert!(
        svc.authz_snapshot().load().policies.is_empty(),
        "the broker must start from an empty deny-by-default snapshot"
    );
    let (_authn, authz, _keys) = svc.build_auth_services();
    authz.warm_shared_snapshot().await;
    assert!(
        svc.authz_snapshot()
            .load()
            .policies
            .iter()
            .any(|p| p.id == policy_id),
        "the PG-warmed snapshot must carry the seeded narrow rule"
    );

    // A second ACTIVE project, so the cross-project probe reaches the policy
    // decision instead of the catalog gate.
    let checksum = svc
        .catalog
        .stage_catalog(
            svc.manifest.clone(),
            OTHER_PROJECT.to_string(),
            "b11-deny-path".to_string(),
            "exact".to_string(),
        )
        .await
        .unwrap_or_else(|err| panic!("stage catalog for {OTHER_PROJECT}: {err}"));
    svc.catalog
        .activate_catalog_for(OTHER_PROJECT, &checksum)
        .await
        .unwrap_or_else(|err| panic!("activate catalog for {OTHER_PROJECT}: {err}"));

    let granted = caller(&tenant, DEFAULT_PROJECT_ID, SUBJECT);

    // Narrow allow.
    svc.authorize(&granted, INVOICE, "Select")
        .await
        .expect("the narrow grant must allow exactly its own tuple");

    // Everything outside the tuple is denied.
    for action in ["Upsert", "Delete"] {
        assert_denied(
            &svc,
            &granted,
            INVOICE,
            action,
            "another action",
            &[("candidate_rule", &policy_id), ("action", "Select")],
        )
        .await;
    }
    assert_denied(
        &svc,
        &granted,
        SALARY,
        "Select",
        "another table",
        &[("candidate_rule", &policy_id), ("object", INVOICE)],
    )
    .await;
    assert_denied(
        &svc,
        &caller(&tenant, DEFAULT_PROJECT_ID, "svc-b11-intruder"),
        INVOICE,
        "Select",
        "another subject",
        &[
            ("candidate_rule", &policy_id),
            ("identity", "subject or role binding"),
        ],
    )
    .await;
    let foreign = assert_denied(
        &svc,
        &caller(&foreign_tenant, DEFAULT_PROJECT_ID, SUBJECT),
        INVOICE,
        "Select",
        "another tenant",
        &[("tenant", &foreign_tenant)],
    )
    .await;
    assert!(
        !denial_detail(&foreign)
            .missing
            .contains_key("candidate_rule")
            && !foreign.message().contains(&policy_id)
            && !foreign.message().contains(&tenant),
        "denial diagnostics cannot enumerate another tenant's rule",
    );
    assert_denied(
        &svc,
        &caller(&tenant, OTHER_PROJECT, SUBJECT),
        INVOICE,
        "Select",
        "another project (project-bound allow rules must be honored)",
        &[
            ("candidate_rule", &policy_id),
            ("project", DEFAULT_PROJECT_ID),
        ],
    )
    .await;

    let mut purpose = granted.clone();
    purpose.purpose = "different-purpose".to_string();
    assert_denied(
        &svc,
        &purpose,
        INVOICE,
        "Select",
        "another purpose",
        &[("candidate_rule", &policy_id), ("purpose", "b11-deny-path")],
    )
    .await;
    let mut without_scope = granted.clone();
    without_scope.scopes.clear();
    assert_denied(
        &svc,
        &without_scope,
        INVOICE,
        "Select",
        "missing scope",
        &[("candidate_rule", &policy_id), ("scope", "udb:read")],
    )
    .await;
    let mut empty_purpose = granted.clone();
    empty_purpose.purpose.clear();
    assert_denied(
        &svc,
        &empty_purpose,
        INVOICE,
        "Select",
        "missing purpose",
        &[("purpose", "")],
    )
    .await;

    // Purpose '*' grants arbitrary nonempty purposes through the real loader
    // and gate. It does not bypass required-scope predicates.
    set_rule_attributes(
        &pool,
        &policy_id,
        serde_json::json!({"priority": 10, "purpose": "*", "required_scopes": "udb:read"}),
    )
    .await;
    authz.warm_shared_snapshot().await;
    svc.authorize(&purpose, INVOICE, "Select")
        .await
        .expect("wildcard purpose allows the actual alternate purpose");
    assert_denied(
        &svc,
        &without_scope,
        INVOICE,
        "Select",
        "wildcard purpose keeps scope checks",
        &[("candidate_rule", &policy_id), ("scope", "udb:read")],
    )
    .await;

    let deny_id = insert_allow_rule(
        &pool,
        &tenant,
        DEFAULT_PROJECT_ID,
        SUBJECT,
        INVOICE,
        "Select",
    )
    .await;
    set_rule_attributes(
        &pool,
        &deny_id,
        serde_json::json!({"required_scopes": "udb:admin"}),
    )
    .await;
    let model = native_catalog::native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &["policy_id", "effect"],
    );
    sqlx::query(&format!(
        "UPDATE {rel} SET {effect} = 'DENY' WHERE {policy_id} = $1::UUID",
        rel = model.relation,
        effect = model.q("effect"),
        policy_id = model.q("policy_id"),
    ))
    .bind(&deny_id)
    .execute(&pool)
    .await
    .expect("persist explicit deny");
    authz.warm_shared_snapshot().await;
    assert_denied(
        &svc,
        &granted,
        INVOICE,
        "Select",
        "missing scope cannot cancel an explicit deny",
        &[("candidate_rule", &deny_id), ("effect", "deny")],
    )
    .await;

    // A caller-supplied wildcard / empty message type is refused, never a bypass.
    for wildcard in ["*", ""] {
        let err = svc
            .authorize(&granted, wildcard, "Select")
            .await
            .expect_err("a wildcard message type must be refused on a data RPC");
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{wildcard:?}");
    }

    pool.close().await;
}
