use super::*;

fn principal(subject: &str, tenant: &str, scopes: &[&str], roles: &[&str]) -> Principal {
    Principal {
        principal_id: subject.to_string(),
        subject: subject.to_string(),
        user_id: subject.to_string(),
        service_identity: subject.to_string(),
        tenant_id: tenant.to_string(),
        project_id: String::new(),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        roles: roles.iter().map(|s| s.to_string()).collect(),
        provider_id: String::new(),
        auth_method: "test".to_string(),
    }
}

fn query<'a>(
    principal: &'a Principal,
    resource: &'a ResourceRef,
    action: &'a str,
    purpose: &'a str,
    attrs: &'a BTreeMap<String, String>,
) -> AuthzQuery<'a> {
    AuthzQuery {
        principal,
        resource,
        action,
        purpose,
        attributes: attrs,
    }
}

fn no_attrs() -> BTreeMap<String, String> {
    BTreeMap::new()
}

#[tokio::test]
async fn rbac_role_policy_allows_and_default_denies() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            role: "admin".into(),
            action: "data.*".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();

    // Has the role → allowed.
    let admin = principal("svc", "acme", &[], &["admin"]);
    let d = snap
        .casbin_authorize(&query(&admin, &res, "data.select", "billing", &attrs))
        .await;
    assert!(d.allowed);
    assert_eq!(d.matched_policy_ids, vec!["p1"]);

    // No role → no match → default deny.
    let nobody = principal("svc", "acme", &[], &[]);
    let d = snap
        .casbin_authorize(&query(&nobody, &res, "data.select", "billing", &attrs))
        .await;
    assert!(!d.allowed);
    assert!(d.audit_required);
}

#[tokio::test]
async fn role_binding_grants_role() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            role: "writer".into(),
            action: "data.upsert".into(),
            ..Default::default()
        }],
        role_bindings: vec![RoleBinding {
            subject: "svc".into(),
            role: "writer".into(),
            tenant: "acme".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]); // no inline roles
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();
    let d = snap
        .casbin_authorize(&query(&p, &res, "data.upsert", "billing", &attrs))
        .await;
    assert!(d.allowed, "role binding should grant the writer role");
}

#[tokio::test]
async fn explicit_deny_wins_at_equal_priority() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![
            AuthzPolicy {
                id: "allow".into(),
                priority: 5,
                effect: Effect::Allow,
                action: "data.select".into(),
                ..Default::default()
            },
            AuthzPolicy {
                id: "deny".into(),
                priority: 5,
                effect: Effect::Deny,
                action: "data.select".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();
    let d = snap
        .casbin_authorize(&query(&p, &res, "data.select", "billing", &attrs))
        .await;
    assert!(!d.allowed, "deny must win at equal priority");
    assert_eq!(d.effect, Effect::Deny);
}

#[tokio::test]
async fn explicit_deny_overrides_higher_priority_allow() {
    // Deny-override: an explicit matched Deny wins even over a higher-priority
    // Allow. This matches the Casbin engine's deny-override semantics so the
    // two authz engines never disagree (previously the higher-priority Allow
    // won here, diverging from Casbin).
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![
            AuthzPolicy {
                id: "allow".into(),
                priority: 10,
                effect: Effect::Allow,
                action: "*".into(),
                ..Default::default()
            },
            AuthzPolicy {
                id: "deny".into(),
                priority: 1,
                effect: Effect::Deny,
                action: "*".into(),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();
    let d = snap
        .casbin_authorize(&query(&p, &res, "data.select", "billing", &attrs))
        .await;
    assert!(
        !d.allowed,
        "explicit deny must override a higher-priority allow"
    );
}

#[tokio::test]
async fn disabled_policy_is_ignored() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            enabled: false,
            action: "*".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();
    let d = snap
        .casbin_authorize(&query(&p, &res, "data.select", "billing", &attrs))
        .await;
    assert!(!d.allowed, "disabled policy must not grant access");
}

#[tokio::test]
async fn abac_condition_gates_match() {
    let mut conditions = BTreeMap::new();
    conditions.insert("region".to_string(), "eu".to_string());
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            action: "*".into(),
            conditions,
            ..Default::default()
        }],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let res = ResourceRef::message("acme.Invoice");

    let mut eu = BTreeMap::new();
    eu.insert("region".to_string(), "eu".to_string());
    assert!(
        snap.casbin_authorize(&query(&p, &res, "data.select", "x", &eu))
            .await
            .allowed,
        "matching attribute should allow"
    );

    let mut us = BTreeMap::new();
    us.insert("region".to_string(), "us".to_string());
    assert!(
        !snap
            .casbin_authorize(&query(&p, &res, "data.select", "x", &us))
            .await
            .allowed,
        "mismatched attribute should deny"
    );
}

#[tokio::test]
async fn rebac_relationship_required() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            action: "data.select".into(),
            relationship: "owner".into(),
            ..Default::default()
        }],
        tuples: vec![RelationshipTuple {
            subject: "svc".into(),
            relation: "owner".into(),
            object: "inv_001".into(),
            tenant: "acme".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let mut res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();

    // Owns inv_001 → allowed.
    res.resource_name = "inv_001".into();
    assert!(
        snap.casbin_authorize(&query(&p, &res, "data.select", "x", &attrs))
            .await
            .allowed
    );

    // Does not own inv_002 → denied.
    res.resource_name = "inv_002".into();
    assert!(
        !snap
            .casbin_authorize(&query(&p, &res, "data.select", "x", &attrs))
            .await
            .allowed
    );
}

#[tokio::test]
async fn required_scope_must_be_present() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            action: "*".into(),
            required_scopes: vec!["udb:write".into()],
            ..Default::default()
        }],
        ..Default::default()
    };
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();

    let with = principal("svc", "acme", &["udb:write"], &[]);
    assert!(
        snap.casbin_authorize(&query(&with, &res, "data.upsert", "x", &attrs))
            .await
            .allowed
    );

    let without = principal("svc", "acme", &["udb:read"], &[]);
    assert!(
        !snap
            .casbin_authorize(&query(&without, &res, "data.upsert", "x", &attrs))
            .await
            .allowed
    );
}

#[tokio::test]
async fn decision_id_is_stable_for_same_inputs() {
    let snap = AuthzSnapshot {
        version: "v1".into(),
        policies: vec![AuthzPolicy {
            id: "p1".into(),
            action: "*".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let p = principal("svc", "acme", &[], &[]);
    let res = ResourceRef::message("acme.Invoice");
    let attrs = no_attrs();
    let a = snap
        .casbin_authorize(&query(&p, &res, "data.select", "x", &attrs))
        .await;
    let b = snap
        .casbin_authorize(&query(&p, &res, "data.select", "x", &attrs))
        .await;
    assert_eq!(a.decision_id, b.decision_id);
    assert!(a.decision_id.starts_with("authz_"));

    // Different action → different id.
    let c = snap
        .casbin_authorize(&query(&p, &res, "data.delete", "x", &attrs))
        .await;
    assert_ne!(a.decision_id, c.decision_id);
}

#[test]
fn rpc_action_mapping_covers_core_rpcs() {
    assert_eq!(rpc_action("Select"), "data.select");
    assert_eq!(rpc_action("BatchUpsert"), "data.upsert");
    assert_eq!(rpc_action("VectorSearch"), "vector.search");
    assert_eq!(rpc_action("GetObject"), "object.read");
    assert_eq!(rpc_action("GeneratePresignedUrl"), "object.presign");
    // Unmapped/admin RPCs collapse to the admin action.
    assert_eq!(rpc_action("ListPolicies"), "admin.manage");
    assert_eq!(rpc_action("ApplyMigration"), "admin.manage");
}

#[test]
fn principal_from_security_context_prefers_user_then_service() {
    let mut ctx = SecurityContext::default();
    ctx.service_identity = "billing-api".into();
    ctx.tenant_id = "acme".into();
    ctx.scopes = vec!["udb:read".into()];
    // No user id → subject falls back to the service identity.
    let p = Principal::from_security_context(&ctx, vec!["reader".into()]);
    assert_eq!(p.subject, "billing-api");
    assert_eq!(p.tenant_id, "acme");
    assert_eq!(p.roles, vec!["reader"]);

    ctx.user_id = "user_1".into();
    let p = Principal::from_security_context(&ctx, vec![]);
    assert_eq!(p.subject, "user_1");
}

#[test]
fn pattern_match_handles_dotted_prefix() {
    assert!(pattern_match("data.*", "data.select"));
    assert!(pattern_match("data.*", "data"));
    assert!(!pattern_match("data.*", "vector.search"));
    assert!(pattern_match("*", "anything"));
    assert!(pattern_match("", "anything"));
    assert!(pattern_match("data.select", "data.select"));
    assert!(!pattern_match("data.select", "data.delete"));
}

#[test]
fn authz_catalog_ddl_covers_v2_tables_and_is_idempotent() {
    let ddl = authz_catalog_ddl("udb_system");
    let joined = ddl.join("\n");
    for fragment in [
        "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"policy_tuples\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"roles\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"user_roles\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"policy_rules\"",
        "CREATE TABLE IF NOT EXISTS \"udb_authz\".\"access_decision_audits\"",
    ] {
        assert!(
            joined.contains(fragment),
            "missing generated DDL fragment {fragment}"
        );
    }
    assert!(
        joined.contains("UDB:proto_manifest_checksum"),
        "native authz DDL must carry proto manifest metadata"
    );
}
