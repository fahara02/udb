//! Real Casbin-driven authorization.
//!
//! The [`AuthzSnapshot`] is enforced through an actual [`casbin::Enforcer`]
//! running an advanced **PERM** model (request / policy / role / effect /
//! matchers): RBAC with tenant domains and glob resource/action matching via
//! `keyMatch2`. Deny override is enforced before Casbin because Rust Casbin's
//! built-in effector does not support the combined deny/allow expression.
//!
//! ABAC gates that the Casbin RBAC matcher does not express — required scopes,
//! attribute conditions, ReBAC relationship tuples, and purpose — pre-filter
//! which policies enter the enforcer. Casbin then drives subject/role/resource/
//! action/domain matching and the allow/deny effect.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use casbin::{CoreApi, DefaultModel, Enforcer, MemoryAdapter, MgmtApi};
use sha2::{Digest, Sha256};

use super::{AuthzPolicy, AuthzQuery, AuthzSnapshot, Decision, Effect, conditions_match, wildcard};

/// The Casbin PERM model. RBAC-with-domains request + policy, a `g` role
/// grouping, allow effect, and `keyMatch2` glob matching for resources and
/// actions. Explicit deny is checked before enforcement. `p.sub == "*"`
/// (wildcard subject) and empty/`*` domains are honored in the matcher so
/// legacy broad policies keep working.
pub(crate) const CASBIN_MODEL: &str = r#"[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act, eft

[role_definition]
g = _, _

[policy_effect]
e = some(where (p_eft == allow)) && !some(where (p_eft == deny))

[matchers]
m = (p.sub == "*" || g(r.sub, p.sub)) && (p.dom == "*" || p.dom == "" || r.dom == p.dom) && (p.obj == "*" || keyMatch2(r.obj, p.obj)) && (p.act == "*" || r.act == p.act || keyMatch2(r.act, p.act))
"#;

/// Empty selector → Casbin wildcard token.
fn slot(value: &str) -> String {
    if value.trim().is_empty() {
        "*".to_string()
    } else {
        value.to_string()
    }
}

impl AuthzSnapshot {
    /// Authorize `req` through a real Casbin enforcer built from this snapshot.
    /// This is the production decision path for the native `AuthzService`.
    pub(crate) async fn casbin_authorize(&self, req: &AuthzQuery<'_>) -> Decision {
        let decision_id = self.decision_id(req);

        // Dev-only default-allow applies only when there is genuinely no policy.
        if self.policies.is_empty() {
            let allowed = self.default_allow;
            return Decision {
                decision_id,
                allowed,
                effect: if allowed { Effect::Allow } else { Effect::Deny },
                deny_reason: if allowed {
                    String::new()
                } else {
                    "no authz policy (default deny)".to_string()
                },
                policy_version: self.version.clone(),
                relationship_version: self.relationship_version.clone(),
                audit_required: !allowed,
                ..Default::default()
            };
        }

        let principal = req.principal;
        let roles = self.effective_roles(principal);

        // ABAC pre-filter: keep only policies whose purpose / attribute conditions
        // / ReBAC relationship / required-scope gates the request satisfies. Scopes
        // refine an Allow only (a Deny still applies — never fail-open).
        let applicable: Vec<&AuthzPolicy> = self
            .policies
            .iter()
            .filter(|p| {
                p.enabled
                    && wildcard(&p.purpose, req.purpose)
                    && conditions_match(&p.conditions, req.attributes)
                    && (p.relationship.is_empty()
                        || self.has_tuple(principal, &p.relationship, &req.resource.resource_name))
                    && match p.effect {
                        Effect::Allow => p.required_scopes.iter().all(|s| principal.has_scope(s)),
                        Effect::Deny => true,
                    }
            })
            .collect();

        let deny_matches: Vec<&AuthzPolicy> = applicable
            .iter()
            .copied()
            .filter(|p| p.effect == Effect::Deny && self.policy_matches(p, &roles, req))
            .collect();
        if let Some(policy) = deny_matches.first() {
            return Decision {
                decision_id,
                allowed: false,
                effect: Effect::Deny,
                deny_reason: format!("denied by policy {}", policy.id),
                matched_policy_ids: deny_matches.iter().map(|p| p.id.clone()).collect(),
                required_scopes: policy.required_scopes.clone(),
                policy_version: self.version.clone(),
                relationship_version: self.relationship_version.clone(),
                cache_ttl_seconds: 0,
                audit_required: true,
                via_role: false,
            };
        }

        static MODEL: tokio::sync::OnceCell<DefaultModel> = tokio::sync::OnceCell::const_new();
        let model = match MODEL
            .get_or_try_init(|| async { DefaultModel::from_str(CASBIN_MODEL).await })
            .await
        {
            Ok(model) => model.clone(),
            Err(err) => return self.casbin_error(decision_id, &format!("model parse: {err}")),
        };
        let subject = if principal.subject.trim().is_empty() {
            principal.principal_id.clone()
        } else {
            principal.subject.clone()
        };
        let identities: Vec<String> = principal
            .identities()
            .into_iter()
            .filter(|id| !id.trim().is_empty() && *id != subject)
            .map(ToString::to_string)
            .collect();
        let enforcer =
            match cached_enforcer(model, &applicable, &roles, &subject, &identities).await {
                Ok(enforcer) => enforcer,
                Err(err) => return self.casbin_error(decision_id, &err),
            };

        let dom = slot(&principal.tenant_id);
        // Match AuthzSnapshot::resource_match, which tests the policy resource
        // pattern against EVERY selector (resource_name, message_type, table,
        // resource_type) and matches if any does. Enforce once per non-empty
        // selector and allow if any enforce() succeeds, so the two engines agree
        // (the prior single-`obj` check could deny a request the snapshot engine
        // would allow via a different selector).
        let mut selectors: Vec<String> = [
            req.resource.resource_name.trim(),
            req.resource.message_type.trim(),
            req.resource.table.trim(),
            req.resource.resource_type.trim(),
        ]
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
        if selectors.is_empty() {
            selectors.push("*".to_string());
        }
        let allowed = selectors.iter().any(|obj| {
            enforcer
                .enforce((
                    subject.clone(),
                    dom.clone(),
                    obj.clone(),
                    req.action.to_string(),
                ))
                .unwrap_or(false)
        });

        // Resolve the actual granting policy once: the highest-priority
        // applicable Allow whose full selector set matches the request. Both
        // required_scopes and the ROLE_POLICY audit classification derive from
        // it (not an arbitrary `applicable.first()`).
        let granting: Option<&AuthzPolicy> = if allowed {
            let mut grantors: Vec<&AuthzPolicy> = applicable
                .iter()
                .copied()
                .filter(|p| p.effect == Effect::Allow && self.policy_matches(p, &roles, req))
                .collect();
            grantors.sort_by_key(|p| std::cmp::Reverse(p.priority));
            grantors.first().copied()
        } else {
            None
        };

        Decision {
            decision_id,
            allowed,
            effect: if allowed { Effect::Allow } else { Effect::Deny },
            deny_reason: if allowed {
                String::new()
            } else {
                "denied by Casbin PERM model".to_string()
            },
            // Candidate policy set Casbin evaluated (post ABAC pre-filter).
            matched_policy_ids: applicable.iter().map(|p| p.id.clone()).collect(),
            required_scopes: granting
                .map(|p| p.required_scopes.clone())
                .unwrap_or_default(),
            policy_version: self.version.clone(),
            relationship_version: self.relationship_version.clone(),
            cache_ttl_seconds: 0,
            audit_required: !allowed,
            via_role: granting.map(|p| !p.role.trim().is_empty()).unwrap_or(false),
        }
    }

    /// Fail closed on a Casbin engine error.
    fn casbin_error(&self, decision_id: String, reason: &str) -> Decision {
        Decision {
            decision_id,
            allowed: false,
            effect: Effect::Deny,
            deny_reason: format!("casbin engine error: {reason}"),
            policy_version: self.version.clone(),
            audit_required: true,
            ..Default::default()
        }
    }
}

fn enforcer_cache() -> &'static tokio::sync::Mutex<HashMap<String, Arc<Enforcer>>> {
    static CACHE: OnceLock<tokio::sync::Mutex<HashMap<String, Arc<Enforcer>>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

async fn cached_enforcer(
    model: DefaultModel,
    applicable: &[&AuthzPolicy],
    roles: &[String],
    subject: &str,
    identities: &[String],
) -> Result<Arc<Enforcer>, String> {
    let key = casbin_policy_set_hash(applicable, roles, subject, identities);
    if let Some(enforcer) = enforcer_cache().lock().await.get(&key).cloned() {
        return Ok(enforcer);
    }

    let mut enforcer = Enforcer::new(model, MemoryAdapter::default())
        .await
        .map_err(|err| format!("enforcer init: {err}"))?;
    for p in applicable.iter().filter(|p| p.effect == Effect::Allow) {
        let sub = if !p.role.trim().is_empty() {
            p.role.clone()
        } else {
            slot(&p.subject)
        };
        let rule = vec![
            sub,
            slot(&p.tenant),
            slot(&p.resource),
            slot(&p.action),
            p.effect.as_str().to_string(),
        ];
        enforcer
            .add_policy(rule)
            .await
            .map_err(|err| format!("policy load: {err}"))?;
    }
    for role in roles {
        if !role.trim().is_empty() {
            enforcer
                .add_grouping_policy(vec![subject.to_string(), role.clone()])
                .await
                .map_err(|err| format!("role link load: {err}"))?;
        }
    }
    for id in identities {
        enforcer
            .add_grouping_policy(vec![subject.to_string(), id.clone()])
            .await
            .map_err(|err| format!("identity link load: {err}"))?;
    }

    let enforcer = Arc::new(enforcer);
    let mut cache = enforcer_cache().lock().await;
    if cache.len() > 256 {
        cache.clear();
    }
    Ok(cache.entry(key).or_insert_with(|| enforcer.clone()).clone())
}

fn casbin_policy_set_hash(
    applicable: &[&AuthzPolicy],
    roles: &[String],
    subject: &str,
    identities: &[String],
) -> String {
    let mut parts = Vec::new();
    for p in applicable.iter().filter(|p| p.effect == Effect::Allow) {
        parts.push(format!(
            "p|{}|{}|{}|{}|{}|{}",
            p.id,
            p.priority,
            if p.role.trim().is_empty() {
                slot(&p.subject)
            } else {
                p.role.clone()
            },
            slot(&p.tenant),
            slot(&p.resource),
            slot(&p.action)
        ));
    }
    for role in roles {
        if !role.trim().is_empty() {
            parts.push(format!("g|{subject}|{role}"));
        }
    }
    for id in identities {
        parts.push(format!("g|{subject}|{id}"));
    }
    parts.sort();

    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\n");
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::authz::{Principal, ResourceRef, RoleBinding};
    use std::collections::BTreeMap;

    fn query<'a>(
        principal: &'a Principal,
        resource: &'a ResourceRef,
        action: &'a str,
        attrs: &'a BTreeMap<String, String>,
    ) -> AuthzQuery<'a> {
        AuthzQuery {
            principal,
            resource,
            action,
            purpose: "",
            attributes: attrs,
        }
    }

    #[tokio::test]
    async fn casbin_rbac_role_grants_via_binding_and_default_denies() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        // A reader role may select invoices in tenant acme.
        snap.policies.push(AuthzPolicy {
            id: "p1".to_string(),
            effect: Effect::Allow,
            tenant: "acme".to_string(),
            role: "reader".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        snap.role_bindings.push(RoleBinding {
            subject: "alice".to_string(),
            role: "reader".to_string(),
            tenant: "acme".to_string(),
            project: String::new(),
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");

        let alice = Principal {
            subject: "alice".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        let allow = snap
            .casbin_authorize(&query(&alice, &resource, "data.select", &attrs))
            .await;
        assert!(
            allow.allowed,
            "role-bound principal must be allowed by Casbin"
        );

        // A different action is denied (default deny / deny-override).
        let deny = snap
            .casbin_authorize(&query(&alice, &resource, "data.delete", &attrs))
            .await;
        assert!(!deny.allowed);

        // An unbound principal is denied.
        let bob = Principal {
            subject: "bob".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        };
        let bob_deny = snap
            .casbin_authorize(&query(&bob, &resource, "data.select", &attrs))
            .await;
        assert!(!bob_deny.allowed);
    }

    #[tokio::test]
    async fn casbin_explicit_deny_overrides_allow() {
        let mut snap = AuthzSnapshot::default();
        snap.version = "v1".to_string();
        snap.policies.push(AuthzPolicy {
            id: "allow".to_string(),
            effect: Effect::Allow,
            subject: "*".to_string(),
            action: "*".to_string(),
            resource: "*".to_string(),
            ..Default::default()
        });
        snap.policies.push(AuthzPolicy {
            id: "deny".to_string(),
            effect: Effect::Deny,
            subject: "carol".to_string(),
            action: "data.delete".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let attrs = BTreeMap::new();
        let resource = ResourceRef::message("invoice");
        let carol = Principal {
            subject: "carol".to_string(),
            ..Default::default()
        };
        let decision = snap
            .casbin_authorize(&query(&carol, &resource, "data.delete", &attrs))
            .await;
        assert!(
            !decision.allowed,
            "explicit deny must override the broad allow"
        );
    }
}
