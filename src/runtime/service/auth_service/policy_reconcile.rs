//! Policy as code (`udb policy diff|apply`): reconcile one tenant's active
//! authorization policies in `udb_authz.policy_rules` against a declared file.
//!
//! Policies used to be kept in hand-synced JSON files pushed with `udb
//! policy-seed` (removed in 0.6.0), which deleted EVERY tenant's rows before re-inserting; one
//! diff script compared rows without their purpose and skipped 17 of them, and a
//! development file once loaded 302 rows into production. This reconcile:
//!
//! - touches ONE tenant: a declared policy naming another tenant is refused, and
//!   only that tenant's rows are read or retired. The platform-wide domain `*`
//!   (rules that apply in every tenant) is a target of its own: `--tenant '*'`.
//!   Reaching it takes the database DSN, which is platform authority already;
//!   the served PutAuthzPolicy keeps requiring a cross-tenant admin for it;
//! - compares the whole policy (purpose, role, conditions, scopes and priority
//!   included), so any difference is a retire + add, never a silent skip;
//! - retires rows by soft delete (`deleted_at`, `is_active = false`) so the
//!   audit trail keeps them;
//! - writes in one transaction and appends an authz revision so every replica's
//!   cached policy bundle invalidates;
//! - with `additive`, only adds: rows the file does not declare are reported
//!   as kept and never retired, so a partial file can be applied safely;
//! - never retires UDB's seeded `organization_owner` rule.

use std::collections::{BTreeMap, BTreeSet};

use crate::runtime::authz::{AuthzPolicy, Effect};
use crate::runtime::native_catalog::native_model;

use super::mappings::policy_from_pg_row;

/// The platform-wide policy domain: a rule stored under it applies in every
/// tenant.
pub const PLATFORM_POLICY_DOMAIN: &str = "*";

/// How a reconcile runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PolicyReconcileOptions {
    /// Write the changes (`apply`); otherwise only report them (`diff`).
    pub apply: bool,
    /// Only add missing rules: never retire a row the file does not declare,
    /// nor an exact duplicate.
    pub additive: bool,
}

/// What `udb policy diff|apply` found (and, when applied, changed).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PolicyReconcileReport {
    pub tenant_id: String,
    pub applied: bool,
    /// Declared policies not present yet.
    pub added: Vec<AuthzPolicy>,
    /// Active rows the file no longer declares (retired on apply).
    pub removed: Vec<AuthzPolicy>,
    /// Active rows that duplicate another active row exactly (retired on apply).
    pub duplicates: Vec<AuthzPolicy>,
    /// Active rows the file does not declare that stay in place: everything an
    /// additive run would otherwise retire, and UDB's seeded owner rule.
    pub kept: Vec<AuthzPolicy>,
    /// The run only added rules (`udb policy apply --additive`).
    pub additive: bool,
    pub unchanged: usize,
    /// The policy revision appended per touched project (apply only).
    pub revisions: BTreeMap<String, i64>,
}

/// The comparison key: everything that changes what a policy decides. The row
/// id is not part of it, so the same rule loaded under another id is unchanged.
fn policy_key(policy: &AuthzPolicy) -> String {
    let conditions: Vec<String> = policy
        .conditions
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect();
    let mut scopes: Vec<&str> = policy
        .required_scopes
        .iter()
        .map(|scope| scope.trim())
        .filter(|scope| !scope.is_empty())
        .collect();
    scopes.sort_unstable();
    scopes.dedup();
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        policy.tenant,
        policy.project,
        policy.subject,
        policy.role,
        policy.action,
        policy.resource,
        if matches!(policy.effect, Effect::Deny) {
            "DENY"
        } else {
            "ALLOW"
        },
        policy.purpose,
        policy.relationship,
        policy.priority,
        conditions.join("&"),
        scopes.join(","),
    )
}

/// The stable row id a declared policy is stored under, derived from its key so
/// applying the same file twice is a no-op even before the diff runs.
fn declared_policy_id(key: &str) -> uuid::Uuid {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("udb-policy-apply|{key}").as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Uuid::from_bytes(bytes)
}

/// Check and normalise the declared policies for `tenant_id`: an empty tenant
/// becomes the target, any other tenant is refused, disabled entries are
/// dropped (absence is how a file retires a rule), and exact repeats collapse.
/// The target may be [`PLATFORM_POLICY_DOMAIN`] (`*`), the platform-wide
/// domain; it is reconciled on its own, never mixed into a tenant's file.
pub fn normalize_declared_policies(
    tenant_id: &str,
    declared: Vec<AuthzPolicy>,
) -> Result<Vec<AuthzPolicy>, String> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Err(
            "UDB_POLICY_TENANT_REQUIRED: pass --tenant <canonical tenant UUID>".to_string(),
        );
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(declared.len());
    for (index, mut policy) in declared.into_iter().enumerate() {
        let declared_tenant = policy.tenant.trim();
        if declared_tenant.is_empty() {
            policy.tenant = tenant_id.to_string();
        } else if declared_tenant != tenant_id {
            let hint = if declared_tenant == PLATFORM_POLICY_DOMAIN {
                "; platform-wide rules (tenant '*') go in their own file, applied with --tenant '*' \
                 (that needs the database DSN, i.e. platform authority)"
            } else {
                ""
            };
            return Err(format!(
                "UDB_POLICY_TENANT_MISMATCH: policy #{index} ({} {} on {}) names tenant '{declared_tenant}' but the target is '{tenant_id}'; \
                 a policy file applies to exactly one tenant{hint}",
                if policy.role.is_empty() {
                    policy.subject.as_str()
                } else {
                    policy.role.as_str()
                },
                policy.action,
                policy.resource,
            ));
        }
        if policy.action.trim().is_empty() || policy.resource.trim().is_empty() {
            return Err(format!(
                "UDB_POLICY_INVALID: policy #{index} needs an action (the RPC name, e.g. Select) and a resource (a message type or *)"
            ));
        }
        if !policy.enabled {
            continue;
        }
        if seen.insert(policy_key(&policy)) {
            out.push(policy);
        }
    }
    Ok(out)
}

/// Diff (and with `apply`, reconcile) the tenant's active policies against
/// `declared` (already passed through [`normalize_declared_policies`]).
pub async fn reconcile_authz_policies_offline(
    dsn: &str,
    tenant_id: &str,
    declared: Vec<AuthzPolicy>,
    apply: bool,
    changed_by: &str,
) -> Result<PolicyReconcileReport, String> {
    reconcile_authz_policies_offline_with(
        dsn,
        tenant_id,
        declared,
        PolicyReconcileOptions {
            apply,
            additive: false,
        },
        changed_by,
    )
    .await
}

/// [`reconcile_authz_policies_offline`] with every option, `additive` included.
pub async fn reconcile_authz_policies_offline_with(
    dsn: &str,
    tenant_id: &str,
    declared: Vec<AuthzPolicy>,
    options: PolicyReconcileOptions,
    changed_by: &str,
) -> Result<PolicyReconcileReport, String> {
    let pool = connect_offline(dsn).await?;
    let result = reconcile(&pool, tenant_id.trim(), declared, options, changed_by).await;
    pool.close().await;
    result
}

/// The tenant's (or, for `*`, the platform domain's) active policies, in the
/// shape `udb policy apply` reads back (see [`exportable_policies`]).
pub async fn export_authz_policies_offline(
    dsn: &str,
    tenant_id: &str,
) -> Result<Vec<AuthzPolicy>, String> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() {
        return Err(
            "UDB_POLICY_TENANT_REQUIRED: pass --tenant <canonical tenant UUID> (or '*')"
                .to_string(),
        );
    }
    let pool = connect_offline(dsn).await?;
    let result = read_policies_once(&pool, tenant_id).await;
    pool.close().await;
    Ok(exportable_policies(result?))
}

async fn read_policies_once(
    pool: &sqlx::PgPool,
    tenant_id: &str,
) -> Result<Vec<AuthzPolicy>, String> {
    // `set_config(.., true)` is transaction-local: read inside one.
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| format!("begin transaction failed: {err}"))?;
    let rows = read_active_policies(&mut tx, tenant_id).await?;
    tx.rollback()
        .await
        .map_err(|err| format!("end read transaction failed: {err}"))?;
    Ok(rows)
}

/// Shape stored rules as a policy file: the per-policy tenant is dropped (the
/// document's `tenant` supplies it, so the file round-trips through `udb
/// policy apply` unchanged), ids are kept for reference (apply compares
/// rules, not ids), and the order is the rule key's so two exports of the same
/// state are identical.
fn exportable_policies(rows: Vec<AuthzPolicy>) -> Vec<AuthzPolicy> {
    let mut keyed: Vec<(String, AuthzPolicy)> = rows
        .into_iter()
        .map(|policy| (policy_key(&policy), policy))
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.id.cmp(&b.1.id)));
    keyed
        .into_iter()
        .map(|(_, mut policy)| {
            policy.tenant = String::new();
            policy
        })
        .collect()
}

async fn connect_offline(dsn: &str) -> Result<sqlx::PgPool, String> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(dsn)
        .await
        .map_err(|err| format!("connect to Postgres failed: {err}"))
}

fn policy_rule_columns() -> crate::runtime::native_catalog::NativeModel {
    native_model(
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
            "deleted_at",
        ],
    )
}

/// Scope the transaction to `tenant_id` and read its active rules.
async fn read_active_policies(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
) -> Result<Vec<AuthzPolicy>, String> {
    let policy = policy_rule_columns();
    // Row-level security on policy_rules keys on this setting; scope the
    // session to the target tenant so nothing else is visible or writable.
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| format!("set tenant scope failed: {err}"))?;
    let rows = sqlx::query(&format!(
        "SELECT {policy_id}::TEXT AS id, COALESCE(NULLIF({attributes_json}->>'priority', '')::INT, 0) AS priority, {is_active} AS enabled, {effect}, {tenant_col} AS tenant, COALESCE({project_id}, '') AS project, {subject}, \
                COALESCE({attributes_json}->>'role', '') AS role, {action}, {object_col} AS resource, COALESCE({attributes_json}->>'purpose', '') AS purpose, \
                COALESCE({attributes_json}->>'relationship', '') AS relationship, {attributes_json} AS conditions, COALESCE({attributes_json}->>'required_scopes', '') AS required_scopes \
         FROM {rel} \
         WHERE {tenant_col} = $1 AND {deleted_at} IS NULL AND {is_active} = TRUE \
         ORDER BY {policy_id} ASC",
        rel = policy.relation,
        policy_id = policy.q("policy_id"),
        attributes_json = policy.q("attributes_json"),
        is_active = policy.q("is_active"),
        effect = policy.q("effect"),
        tenant_col = policy.q("tenant_id"),
        project_id = policy.q("project_id"),
        subject = policy.q("subject"),
        action = policy.q("action"),
        object_col = policy.q("object"),
        deleted_at = policy.q("deleted_at"),
    ))
    .bind(tenant_id)
    .fetch_all(&mut *conn)
    .await
    .map_err(|err| format!("read the tenant's policies failed (is the authz schema bootstrapped?): {err}"))?;
    rows.iter()
        .map(|row| policy_from_pg_row(row).map_err(|err| format!("decode policy failed: {err}")))
        .collect()
}

/// Compare the live rows with the declared policies (keyed by
/// [`policy_key`]). Pure, so the additive and owner-rule rules are tested
/// without Postgres.
fn plan_reconcile(
    tenant_id: &str,
    live: Vec<AuthzPolicy>,
    declared: &BTreeMap<String, AuthzPolicy>,
    options: PolicyReconcileOptions,
) -> PolicyReconcileReport {
    let mut existing: BTreeMap<String, AuthzPolicy> = BTreeMap::new();
    let mut report = PolicyReconcileReport {
        tenant_id: tenant_id.to_string(),
        applied: options.apply,
        additive: options.additive,
        ..Default::default()
    };
    for current in live {
        let key = policy_key(&current);
        if !existing.contains_key(&key) {
            existing.insert(key, current);
        } else if options.additive {
            report.kept.push(current);
        } else {
            report.duplicates.push(current);
        }
    }
    for (key, policy) in declared {
        if existing.contains_key(key) {
            report.unchanged += 1;
        } else {
            report.added.push(policy.clone());
        }
    }
    for (key, policy) in &existing {
        if declared.contains_key(key) {
            continue;
        }
        // UDB seeds this rule into the platform domain; no policy file owns
        // it, so no reconcile retires it.
        if options.additive || policy.id == super::SYSTEM_ORG_OWNER_POLICY_ID {
            report.kept.push(policy.clone());
        } else {
            report.removed.push(policy.clone());
        }
    }
    report
}

async fn reconcile(
    pool: &sqlx::PgPool,
    tenant_id: &str,
    declared: Vec<AuthzPolicy>,
    options: PolicyReconcileOptions,
    changed_by: &str,
) -> Result<PolicyReconcileReport, String> {
    let policy = policy_rule_columns();
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| format!("begin transaction failed: {err}"))?;
    let live = read_active_policies(&mut tx, tenant_id).await?;
    let declared: BTreeMap<String, AuthzPolicy> = declared
        .into_iter()
        .map(|policy| (policy_key(&policy), policy))
        .collect();
    let mut report = plan_reconcile(tenant_id, live, &declared, options);
    if !options.apply {
        return Ok(report);
    }
    if report.added.is_empty() && report.removed.is_empty() && report.duplicates.is_empty() {
        return Ok(report);
    }

    let insert = format!(
        "INSERT INTO {rel} \
           ({policy_id}, {subject}, {domain}, {object}, {action}, {effect}, {condition}, {description}, {is_active}, {tenant_col}, {project_id}, {attributes_json}) \
         VALUES ($1::UUID, $2, '*', $3, $4, $5, '', $6, TRUE, $7, $8, $9::JSONB) \
         ON CONFLICT ({policy_id}) DO UPDATE SET {is_active} = TRUE, {deleted_at} = NULL",
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
        tenant_col = policy.q("tenant_id"),
        project_id = policy.q("project_id"),
        attributes_json = policy.q("attributes_json"),
        deleted_at = policy.q("deleted_at"),
    );
    let mut touched_projects = BTreeSet::new();
    for added in &report.added {
        let key = policy_key(added);
        let mut attributes: serde_json::Map<String, serde_json::Value> = added
            .conditions
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        for (name, value) in [
            ("priority", added.priority.to_string()),
            ("role", added.role.clone()),
            ("purpose", added.purpose.clone()),
            ("relationship", added.relationship.clone()),
            ("required_scopes", added.required_scopes.join(",")),
        ] {
            attributes.insert(name.to_string(), serde_json::Value::String(value));
        }
        sqlx::query(&insert)
            .bind(declared_policy_id(&key).to_string())
            .bind(added.subject.as_str())
            .bind(added.resource.as_str())
            .bind(added.action.as_str())
            .bind(if matches!(added.effect, Effect::Deny) {
                "DENY"
            } else {
                "ALLOW"
            })
            .bind(format!("udb policy apply by {changed_by}"))
            .bind(tenant_id)
            .bind(added.project.as_str())
            .bind(serde_json::Value::Object(attributes).to_string())
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                format!(
                    "insert policy {} {} failed: {err}",
                    added.action, added.resource
                )
            })?;
        touched_projects.insert(added.project.clone());
    }
    let retire = format!(
        "UPDATE {rel} SET {is_active} = FALSE, {deleted_at} = CURRENT_TIMESTAMP \
         WHERE {policy_id} = $1::UUID AND {tenant_col} = $2",
        rel = policy.relation,
        is_active = policy.q("is_active"),
        deleted_at = policy.q("deleted_at"),
        policy_id = policy.q("policy_id"),
        tenant_col = policy.q("tenant_id"),
    );
    for retired in report.removed.iter().chain(report.duplicates.iter()) {
        sqlx::query(&retire)
            .bind(retired.id.as_str())
            .bind(tenant_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| format!("retire policy {} failed: {err}", retired.id))?;
        touched_projects.insert(retired.project.clone());
    }

    let revision = native_model(
        "udb.core.authz.entity.v1.AuthzRevision",
        &[
            "revision_id",
            "tenant_id",
            "project_id",
            "policy_revision",
            "relationship_revision",
            "content_hash",
            "changed_by",
            "change_type",
        ],
    );
    let content_hash = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for key in declared.keys() {
            hasher.update(key.as_bytes());
            hasher.update([0]);
        }
        format!("{:x}", hasher.finalize())
    };
    for project in touched_projects {
        // Next policy revision for (tenant, project), relationship revision
        // carried over; computed and appended in one statement.
        let next: i64 = sqlx::query_scalar(&format!(
            "INSERT INTO {rel} ({revision_id}, {tenant_col}, {project_id}, {policy_revision}, {relationship_revision}, {content_hash}, {changed_by}, {change_type}) \
             SELECT $1::UUID, $2, $3, COALESCE(MAX({policy_revision}), 0) + 1, COALESCE(MAX({relationship_revision}), 0), $4, $5, 'AUTHZ_CHANGE_TYPE_POLICY' \
             FROM {rel} WHERE {tenant_col} = $2 AND {project_id} = $3 \
             RETURNING {policy_revision}",
            rel = revision.relation,
            revision_id = revision.q("revision_id"),
            tenant_col = revision.q("tenant_id"),
            project_id = revision.q("project_id"),
            policy_revision = revision.q("policy_revision"),
            relationship_revision = revision.q("relationship_revision"),
            content_hash = revision.q("content_hash"),
            changed_by = revision.q("changed_by"),
            change_type = revision.q("change_type"),
        ))
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(tenant_id)
        .bind(project.as_str())
        .bind(content_hash.as_str())
        .bind(changed_by)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| format!("append authz revision failed: {err}"))?;
        report.revisions.insert(project, next);
    }
    tx.commit()
        .await
        .map_err(|err| format!("commit failed: {err}"))?;
    Ok(report)
}

#[cfg(test)]
mod policy_reconcile_tests {
    use super::{
        PLATFORM_POLICY_DOMAIN, PolicyReconcileOptions, declared_policy_id, exportable_policies,
        normalize_declared_policies, plan_reconcile, policy_key,
    };
    use crate::runtime::authz::AuthzPolicy;
    use std::collections::BTreeMap;

    fn rule(tenant: &str, action: &str, purpose: &str) -> AuthzPolicy {
        AuthzPolicy {
            tenant: tenant.to_string(),
            role: "app_rw".to_string(),
            action: action.to_string(),
            resource: "*".to_string(),
            purpose: purpose.to_string(),
            ..Default::default()
        }
    }

    /// A file applies to one tenant: blanks are filled, a foreign tenant is
    /// refused by name, disabled rows and exact repeats drop out.
    #[test]
    fn normalize_fills_and_guards_the_tenant() {
        let out = normalize_declared_policies(
            "t-1",
            vec![rule("", "Select", "care"), rule("t-1", "Select", "care")],
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].tenant, "t-1");
        let err = normalize_declared_policies("t-1", vec![rule("t-2", "Select", "")]).unwrap_err();
        assert!(err.starts_with("UDB_POLICY_TENANT_MISMATCH"), "{err}");
        let mut disabled = rule("", "Upsert", "");
        disabled.enabled = false;
        assert!(
            normalize_declared_policies("t-1", vec![disabled])
                .unwrap()
                .is_empty()
        );
        assert!(normalize_declared_policies(" ", vec![]).is_err());
    }

    fn keyed(policies: Vec<AuthzPolicy>) -> BTreeMap<String, AuthzPolicy> {
        policies
            .into_iter()
            .map(|policy| (policy_key(&policy), policy))
            .collect()
    }

    fn live(id: &str, tenant: &str, action: &str) -> AuthzPolicy {
        AuthzPolicy {
            id: id.to_string(),
            ..rule(tenant, action, "")
        }
    }

    /// The default plan retires undeclared rows and exact duplicates; an
    /// additive plan adds the same rows and retires nothing.
    #[test]
    fn additive_plan_adds_and_never_retires() {
        let rows = vec![
            live("a", "t-1", "Select"),
            live("b", "t-1", "Upsert"),
            live("b-copy", "t-1", "Upsert"),
        ];
        let declared = keyed(vec![rule("t-1", "Select", ""), rule("t-1", "Delete", "")]);

        let full = plan_reconcile(
            "t-1",
            rows.clone(),
            &declared,
            PolicyReconcileOptions {
                apply: true,
                additive: false,
            },
        );
        assert_eq!(full.added.len(), 1);
        assert_eq!(full.added[0].action, "Delete");
        assert_eq!(full.unchanged, 1);
        assert_eq!(full.removed.len(), 1);
        assert_eq!(full.removed[0].id, "b");
        assert_eq!(full.duplicates.len(), 1);
        assert!(full.kept.is_empty());
        assert!(!full.additive);

        let additive = plan_reconcile(
            "t-1",
            rows,
            &declared,
            PolicyReconcileOptions {
                apply: true,
                additive: true,
            },
        );
        assert_eq!(additive.added, full.added);
        assert_eq!(additive.unchanged, 1);
        assert!(additive.removed.is_empty(), "additive never retires");
        assert!(additive.duplicates.is_empty(), "additive never retires");
        let mut kept: Vec<&str> = additive.kept.iter().map(|p| p.id.as_str()).collect();
        kept.sort_unstable();
        assert_eq!(kept, vec!["b", "b-copy"]);
        assert!(additive.additive);
    }

    /// The platform domain is a target of its own; the seeded owner rule there
    /// is never retired, even by a full (non-additive) apply.
    #[test]
    fn platform_domain_target_keeps_the_seeded_owner_rule() {
        let declared = normalize_declared_policies(
            PLATFORM_POLICY_DOMAIN,
            vec![rule("", "Select", ""), rule("*", "Upsert", "")],
        )
        .unwrap();
        assert!(declared.iter().all(|p| p.tenant == "*"));
        let owner = AuthzPolicy {
            id: super::super::SYSTEM_ORG_OWNER_POLICY_ID.to_string(),
            tenant: "*".to_string(),
            role: super::super::ORG_OWNER_ROLE_CODE.to_string(),
            action: "*".to_string(),
            resource: "*".to_string(),
            ..Default::default()
        };
        let plan = plan_reconcile(
            "*",
            vec![owner.clone()],
            &keyed(declared),
            PolicyReconcileOptions {
                apply: true,
                additive: false,
            },
        );
        assert_eq!(plan.added.len(), 2);
        assert!(plan.removed.is_empty(), "{:?}", plan.removed);
        assert_eq!(plan.kept, vec![owner]);

        // A tenant file naming the platform domain is still refused, with the
        // way to apply it.
        let err = normalize_declared_policies("t-1", vec![rule("*", "Select", "")]).unwrap_err();
        assert!(err.starts_with("UDB_POLICY_TENANT_MISMATCH"), "{err}");
        assert!(err.contains("--tenant '*'"), "{err}");
    }

    /// An export is a policy file `udb policy apply` reads back as unchanged:
    /// per-policy tenants are dropped (the document supplies them) and the
    /// order is stable.
    #[test]
    fn export_round_trips_through_normalize_as_unchanged() {
        let rows = vec![live("z", "t-1", "Upsert"), live("y", "t-1", "Select")];
        let exported = exportable_policies(rows.clone());
        assert!(exported.iter().all(|p| p.tenant.is_empty()));
        assert_eq!(exportable_policies(rows.clone()), exported, "stable order");
        let document = serde_json::json!({"tenant": "t-1", "policies": exported});
        let reread: Vec<AuthzPolicy> =
            serde_json::from_value(document["policies"].clone()).unwrap();
        let declared = normalize_declared_policies("t-1", reread).unwrap();
        let plan = plan_reconcile(
            "t-1",
            rows,
            &keyed(declared),
            PolicyReconcileOptions::default(),
        );
        assert!(plan.added.is_empty() && plan.removed.is_empty(), "{plan:?}");
        assert_eq!(plan.unchanged, 2);
    }

    /// Purpose is part of the comparison: two rules differing only in purpose
    /// are different rules (the old diff skipped exactly these).
    #[test]
    fn key_includes_purpose_and_ignores_scope_order() {
        assert_ne!(
            policy_key(&rule("t", "Select", "care")),
            policy_key(&rule("t", "Select", "billing"))
        );
        let mut a = rule("t", "Select", "");
        a.required_scopes = vec!["b".into(), "a".into()];
        let mut b = rule("t", "Select", "");
        b.required_scopes = vec!["a".into(), " b ".into()];
        assert_eq!(policy_key(&a), policy_key(&b));
        assert_eq!(
            declared_policy_id(&policy_key(&a)),
            declared_policy_id(&policy_key(&b))
        );
    }
}
