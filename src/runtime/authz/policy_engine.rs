//! Policy-engine trait seam for the authz serving path.
//!
//! [`PolicyEngine`] abstracts the policy *decision surface* that the native
//! `AuthzService` consumes today directly off [`AuthzSnapshot`]'s Casbin path.
//! It exists so a future Cedar / OPA engine can be slotted in behind the same
//! surface without touching the service boundary. **Casbin stays the default**:
//! the bundled [`AuthzSnapshot`] implements this trait over its existing
//! `casbin_authorize` / `lint` / bundle paths, and nothing in the default build
//! selects an alternative engine.
//!
//! The trait deliberately reuses the *existing* authz request/decision/finding/
//! bundle types ([`AuthzQuery`], [`Decision`], [`PolicyLintFinding`],
//! [`SignedBundle`], [`PolicyBundleConfig`]) rather than introducing parallel
//! shapes — the seam is a re-grouping of the real call surface, not a new model.

use async_trait::async_trait;

use crate::runtime::security::PolicyLintFinding;

use super::bundle::{PolicyBundleConfig, SignedBundle};
use super::{AuthzQuery, AuthzSnapshot, Decision};

/// The pluggable policy-decision surface. Implementations evaluate a request to
/// a structured [`Decision`], surface an audit-grade explanation of how that
/// decision was reached, lint the loaded policy set for likely mistakes, expose
/// a signed projection of the policy state for SDK-local caches, and report the
/// authority's current policy version so callers can detect drift.
///
/// All methods are `&self` so an engine is shareable behind an `Arc` and may be
/// swapped atomically (`arc-swap`) the same way the snapshot is today.
#[async_trait]
pub trait PolicyEngine: Send + Sync {
    /// Evaluate `req` and return the structured allow/deny [`Decision`]
    /// (carrying the deterministic `decision_id`, matched policy ids, required
    /// scopes, and policy/relationship versions). This is the hot enforcement
    /// path — equivalent to `AuthzSnapshot::casbin_authorize`.
    async fn decide(&self, req: &AuthzQuery<'_>) -> Decision;

    /// Audit-grade explanation of a decision for the same request. The default
    /// re-runs [`PolicyEngine::decide`]; engines that can attach a richer trace
    /// (matched rule chain, evaluated conditions) override this. The returned
    /// [`Decision`] already exposes `matched_policy_ids` and `deny_reason`.
    async fn explain(&self, req: &AuthzQuery<'_>) -> Decision {
        self.decide(req).await
    }

    /// Lint the loaded policy set for likely-misconfigurations (deny-by-default,
    /// shadowed allows, overly broad wildcard denies), returning the existing
    /// [`PolicyLintFinding`] records the `lint_policies` admin RPC already
    /// surfaces. Synchronous in spirit but `async` for engines that fetch state.
    async fn lint(&self) -> Vec<PolicyLintFinding>;

    /// Produce a signed, time-boxed projection of the policy state scoped to a
    /// tenant/project for SDK-local authorization caches, using the operator's
    /// [`PolicyBundleConfig`]. Returns `None` when bundle signing is disabled
    /// (no secret configured) — mirrors `PolicyBundleConfig::sign`.
    async fn bundle(
        &self,
        cfg: &PolicyBundleConfig,
        tenant_id: &str,
        project_id: &str,
        now_unix: i64,
    ) -> Option<SignedBundle>;

    /// The authority's current policy bundle version (the snapshot `version`),
    /// so callers can detect drift / invalidate caches. Pairs with
    /// [`PolicyEngine::relationship_version`] for the ReBAC tuple generation.
    fn bundle_version(&self) -> String;

    /// The relationship-tuple (ReBAC) version of the current state.
    fn relationship_version(&self) -> String {
        String::new()
    }
}

/// Casbin is the default engine: [`AuthzSnapshot`] already owns the real
/// decision (`casbin_authorize`), lint, and bundle paths, so the seam is a
/// trivial forwarding impl over the existing surface. Kept here so the default
/// build always has a working `PolicyEngine` without selecting any feature.
#[async_trait]
impl PolicyEngine for AuthzSnapshot {
    #[tracing::instrument(skip_all, name = "authz.decide")]
    async fn decide(&self, req: &AuthzQuery<'_>) -> Decision {
        self.casbin_authorize(req).await
    }

    async fn lint(&self) -> Vec<PolicyLintFinding> {
        lint_snapshot(self)
    }

    async fn bundle(
        &self,
        cfg: &PolicyBundleConfig,
        tenant_id: &str,
        project_id: &str,
        now_unix: i64,
    ) -> Option<SignedBundle> {
        cfg.sign(self, tenant_id, project_id, now_unix)
    }

    fn bundle_version(&self) -> String {
        self.version.clone()
    }

    fn relationship_version(&self) -> String {
        self.relationship_version.clone()
    }
}

/// True when the deny-side wildcard selector covers the allow-side one:
/// empty/`*` covers anything; otherwise exact match. Mirrors the engine's
/// `wildcard`/`subject_match`/`role_match` semantics (no prefix patterns).
fn wild_covers(deny: &str, allow: &str) -> bool {
    let deny = deny.trim();
    deny.is_empty() || deny == "*" || deny == allow.trim()
}

/// True when a deny action/resource pattern covers the allow's: wildcard,
/// exact, or a `prefix.*` deny covering an equal-or-narrower allow pattern.
/// Mirrors the engine's `pattern_match`, lifted to pattern-vs-pattern.
fn selector_covers(deny: &str, allow: &str) -> bool {
    let deny = deny.trim();
    let allow = allow.trim();
    if deny.is_empty() || deny == "*" || deny == allow {
        return true;
    }
    if let Some(prefix) = deny.strip_suffix(".*") {
        let allow_base = allow.strip_suffix(".*").unwrap_or(allow);
        return allow_base == prefix || allow_base.starts_with(&format!("{prefix}."));
    }
    false
}

/// Real v2 snapshot lint (item 82): checks provable from the snapshot model
/// alone, surfaced through the same [`PolicyLintFinding`] shape the legacy
/// `lint_policies` admin path uses. `policy_index` is the index into
/// `snapshot.policies` (None for tuple/global findings).
fn lint_snapshot(snapshot: &AuthzSnapshot) -> Vec<PolicyLintFinding> {
    use super::Effect;

    // Empty policy set → the existing deny-by-default signal.
    if snapshot.policies.is_empty() {
        return crate::runtime::security::lint_policies(&[]);
    }

    let mut findings = Vec::new();

    // Per-policy hygiene: ids, disabled rules, overly broad allows.
    let mut seen_ids: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (idx, p) in snapshot.policies.iter().enumerate() {
        if p.id.trim().is_empty() {
            findings.push(PolicyLintFinding {
                severity: "error".to_string(),
                category: "empty_policy_id".to_string(),
                message: format!("Policy #{idx} has an empty id; audits cannot reference it."),
                policy_index: Some(idx),
            });
        } else if !seen_ids.insert(p.id.as_str()) {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "duplicate_policy_id".to_string(),
                message: format!(
                    "Policy #{idx} duplicates id '{}'; decision audits become ambiguous.",
                    p.id
                ),
                policy_index: Some(idx),
            });
        }
        if !p.enabled {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "disabled_policy".to_string(),
                message: format!("Policy {} (#{idx}) is disabled and will be ignored.", p.id),
                policy_index: Some(idx),
            });
        }
        if p.effect == Effect::Allow
            && wild_covers(&p.subject, "?")
            && wild_covers(&p.role, "?")
            && selector_covers(&p.action, "?")
            && selector_covers(&p.resource, "?")
        {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "broad_wildcard".to_string(),
                message: format!(
                    "Policy {} (#{idx}) allows any subject/role/action/resource (overly broad).",
                    p.id
                ),
                policy_index: Some(idx),
            });
        }
    }

    // Unreachable allows: deny-override means an enabled, UNCONDITIONAL Deny
    // whose every selector covers an Allow's makes that Allow unable to ever
    // grant (a conditional/relationship-gated deny does not always fire, so it
    // does not prove shadowing).
    for (idx, allow) in snapshot.policies.iter().enumerate() {
        if allow.effect != Effect::Allow || !allow.enabled {
            continue;
        }
        let shadowing = snapshot.policies.iter().enumerate().find(|(_, deny)| {
            deny.effect == Effect::Deny
                && deny.enabled
                && deny.conditions.is_empty()
                && deny.relationship.trim().is_empty()
                && wild_covers(&deny.tenant, &allow.tenant)
                && wild_covers(&deny.project, &allow.project)
                && wild_covers(&deny.subject, &allow.subject)
                && wild_covers(&deny.role, &allow.role)
                && wild_covers(&deny.purpose, &allow.purpose)
                && selector_covers(&deny.action, &allow.action)
                && selector_covers(&deny.resource, &allow.resource)
        });
        if let Some((deny_idx, deny)) = shadowing {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "shadowed_allow".to_string(),
                message: format!(
                    "Allow policy {} (#{idx}) is unreachable: deny-override policy {} \
                     (#{deny_idx}) covers its entire subject/role/domain/action/resource scope.",
                    allow.id, deny.id
                ),
                policy_index: Some(idx),
            });
        }
    }

    // Dangling role refs: a role-gated policy with no role binding granting
    // that role. Principals may still carry the role via token claims, so this
    // is a warning, not an error.
    let bound_roles: std::collections::HashSet<&str> = snapshot
        .role_bindings
        .iter()
        .map(|b| b.role.as_str())
        .collect();
    for (idx, p) in snapshot.policies.iter().enumerate() {
        let role = p.role.trim();
        if !role.is_empty() && role != "*" && !bound_roles.contains(role) {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "dangling_role".to_string(),
                message: format!(
                    "Policy {} (#{idx}) requires role '{role}' but no role binding grants it \
                     (only token-claim roles could ever match).",
                    p.id
                ),
                policy_index: Some(idx),
            });
        }
    }

    // Duplicate relationship tuples: exact duplicates add nothing and usually
    // indicate a double-write in the tuple store.
    let mut seen_tuples: std::collections::HashSet<String> = std::collections::HashSet::new();
    for t in &snapshot.tuples {
        let key = format!(
            "{}|{}|{}|{}|{}",
            t.subject, t.relation, t.object, t.tenant, t.project
        );
        if !seen_tuples.insert(key) {
            findings.push(PolicyLintFinding {
                severity: "warning".to_string(),
                category: "duplicate_tuple".to_string(),
                message: format!(
                    "Duplicate relationship tuple ({} {} {}) in tenant '{}' project '{}'.",
                    t.subject, t.relation, t.object, t.tenant, t.project
                ),
                policy_index: None,
            });
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use super::super::{AuthzPolicy, Effect, RelationshipTuple, RoleBinding};
    use super::*;

    fn categories(findings: &[PolicyLintFinding]) -> Vec<&str> {
        findings.iter().map(|f| f.category.as_str()).collect()
    }

    #[tokio::test]
    async fn lint_empty_snapshot_reports_deny_by_default() {
        let snap = AuthzSnapshot::default();
        let findings = PolicyEngine::lint(&snap).await;
        assert!(categories(&findings).contains(&"deny_by_default"));
    }

    #[tokio::test]
    async fn lint_flags_allow_shadowed_by_unconditional_deny() {
        let mut snap = AuthzSnapshot::default();
        snap.policies.push(AuthzPolicy {
            id: "allow-select".to_string(),
            effect: Effect::Allow,
            tenant: "acme".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        snap.policies.push(AuthzPolicy {
            id: "deny-all-data".to_string(),
            effect: Effect::Deny,
            action: "data.*".to_string(),
            ..Default::default()
        });
        let findings = PolicyEngine::lint(&snap).await;
        let shadowed: Vec<_> = findings
            .iter()
            .filter(|f| f.category == "shadowed_allow")
            .collect();
        assert_eq!(shadowed.len(), 1, "exactly the covered Allow is flagged");
        assert_eq!(shadowed[0].policy_index, Some(0));

        // A conditional deny does not prove shadowing: no finding.
        snap.policies[1]
            .conditions
            .insert("env".to_string(), "prod".to_string());
        let findings = PolicyEngine::lint(&snap).await;
        assert!(
            !categories(&findings).contains(&"shadowed_allow"),
            "conditional denies must not flag allows as unreachable"
        );
    }

    #[tokio::test]
    async fn lint_flags_dangling_role_and_duplicate_tuple() {
        let mut snap = AuthzSnapshot::default();
        snap.policies.push(AuthzPolicy {
            id: "role-gated".to_string(),
            effect: Effect::Allow,
            role: "auditor".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let tuple = RelationshipTuple {
            subject: "alice".to_string(),
            relation: "owner".to_string(),
            object: "invoice/1".to_string(),
            tenant: "acme".to_string(),
            project: String::new(),
        };
        snap.tuples.push(tuple.clone());
        snap.tuples.push(tuple);

        let findings = PolicyEngine::lint(&snap).await;
        let cats = categories(&findings);
        assert!(cats.contains(&"dangling_role"), "unbound role is flagged");
        assert!(cats.contains(&"duplicate_tuple"), "exact dup is flagged");

        // Binding the role clears the dangling finding.
        snap.role_bindings.push(RoleBinding {
            subject: "alice".to_string(),
            role: "auditor".to_string(),
            tenant: "acme".to_string(),
            project: String::new(),
        });
        let findings = PolicyEngine::lint(&snap).await;
        assert!(!categories(&findings).contains(&"dangling_role"));
    }

    #[tokio::test]
    async fn lint_flags_ids_disabled_and_broad_allow() {
        let mut snap = AuthzSnapshot::default();
        snap.policies.push(AuthzPolicy {
            id: "p1".to_string(),
            effect: Effect::Allow,
            ..Default::default() // all-wildcard allow
        });
        snap.policies.push(AuthzPolicy {
            id: "p1".to_string(), // duplicate id
            effect: Effect::Allow,
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            enabled: false, // disabled
            ..Default::default()
        });
        snap.policies.push(AuthzPolicy {
            id: "  ".to_string(), // empty id
            effect: Effect::Allow,
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            ..Default::default()
        });
        let findings = PolicyEngine::lint(&snap).await;
        let cats = categories(&findings);
        for expect in [
            "broad_wildcard",
            "duplicate_policy_id",
            "disabled_policy",
            "empty_policy_id",
        ] {
            assert!(cats.contains(&expect), "missing finding {expect}");
        }
    }

    #[test]
    fn deny_selector_coverage_rules() {
        // Wildcard deny covers everything; specific deny covers only itself.
        assert!(selector_covers("*", "data.select"));
        assert!(selector_covers("", "data.select"));
        assert!(selector_covers("data.select", "data.select"));
        assert!(!selector_covers("data.select", "data.delete"));
        // Prefix deny covers equal-or-narrower patterns, not broader ones.
        assert!(selector_covers("data.*", "data.select"));
        assert!(selector_covers("data.*", "data.*"));
        assert!(!selector_covers("data.*", "*"));
        assert!(!selector_covers("data.*", "vector.search"));
        // Wild (exact-only) selectors.
        assert!(wild_covers("*", "alice"));
        assert!(wild_covers("", "alice"));
        assert!(wild_covers("alice", "alice"));
        assert!(!wild_covers("alice", "bob"));
    }
}
