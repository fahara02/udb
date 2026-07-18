//! UDB-owned authorization model + Casbin enforcement.
//!
//! Runtime `Principal` / `ResourceRef` / `Decision` types over an immutable
//! [`AuthzSnapshot`] supporting RBAC (roles + role bindings), ABAC (attribute
//! conditions), simple ReBAC (relationship tuples), explicit-deny and required
//! scopes. The single decision path is [`AuthzSnapshot::casbin_authorize`]
//! (see [`casbin_engine`]): a real Casbin PERM model drives subject/role/
//! resource/action/domain matching, with the non-Casbin gates (scopes,
//! conditions, relationships, purpose) pre-filtering which policies enter the
//! enforcer and an explicit Deny short-circuiting before enforcement.
//!
//! It is intentionally decoupled from gRPC: callers map a `Decision` to a
//! `tonic::Status` only at the service boundary.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::runtime::security::{PolicyEffect, SecurityContext};

/// Real Casbin-driven enforcement of the snapshot via a PERM model.
mod casbin_engine;
pub(crate) use casbin_engine::validate_casbin_model;

/// Stage 2: signed policy bundles for local SDK authorization caches (item 139).
pub mod bundle;
/// Stage 2: native Postgres restricted-role / DSN contract (item 134).
pub mod native_access;
/// C.4: pluggable policy-engine trait seam (Casbin stays the default impl).
pub mod policy_engine;

pub use policy_engine::PolicyEngine;

/// Map a broker RPC name to a canonical authorization action. Pure so the
/// broker-integration layer (Milestone 7) and tests share one table.
pub fn rpc_action(rpc_name: &str) -> &'static str {
    match rpc_name {
        "Select" | "BatchSelect" => "data.select",
        "Upsert" | "BatchUpsert" => "data.upsert",
        "Delete" => "data.delete",
        "VectorSearch" | "VectorHybridSearch" => "vector.search",
        "VectorUpsert" | "VectorBatchUpsert" => "vector.upsert",
        "PutObject" | "InitiateMultipartUpload" => "object.write",
        "GetObject" => "object.read",
        "GeneratePresignedUrl" => "object.presign",
        "GenericDispatch" => "backend.dispatch",
        "PublishCDC" | "EnqueueOutboxEvent" => "cdc.publish",
        // Catalog / migration / policy / DLQ / saga / project / health admin RPCs.
        _ => "admin.manage",
    }
}

/// Back-compat wrapper for older call sites. Native authz DDL is generated from
/// `proto/udb/core/authz/entity/**` through the normal UDB proto migration path.
pub fn authz_catalog_ddl(_schema: &str) -> Vec<String> {
    crate::runtime::native_catalog::native_service_catalog_ddl()
        .into_iter()
        .filter(|sql| sql.contains("udb_authz"))
        .collect()
}

/// Allow / Deny effect for a policy or a decision.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Effect {
    #[default]
    Allow,
    Deny,
}

impl Effect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Effect::Allow => "allow",
            Effect::Deny => "deny",
        }
    }
}

impl From<PolicyEffect> for Effect {
    fn from(value: PolicyEffect) -> Self {
        match value {
            PolicyEffect::Allow => Effect::Allow,
            PolicyEffect::Deny => Effect::Deny,
        }
    }
}

/// The authenticated caller, normalized to what authorization needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
    pub principal_id: String,
    pub subject: String,
    pub user_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub roles: Vec<String>,
    pub provider_id: String,
    pub auth_method: String,
}

impl Principal {
    /// True if the principal carries `scope` (exact, `udb:*`, or `*`).
    pub fn has_scope(&self, scope: &str) -> bool {
        if scope.trim().is_empty() {
            return true;
        }
        self.scopes
            .iter()
            .any(|s| s == scope || s == "*" || s == "udb:*")
    }

    /// Bridge the existing per-request `SecurityContext` into a `Principal`.
    /// `roles` come from a JWT `roles` claim / role bindings (not yet stored on
    /// `SecurityContext`), so they are passed in explicitly for now.
    pub fn from_security_context(ctx: &SecurityContext, roles: Vec<String>) -> Self {
        let subject = if !ctx.user_id.trim().is_empty() {
            ctx.user_id.clone()
        } else {
            ctx.service_identity.clone()
        };
        Self {
            principal_id: subject.clone(),
            subject,
            user_id: ctx.user_id.clone(),
            service_identity: ctx.service_identity.clone(),
            tenant_id: ctx.tenant_id.clone(),
            project_id: ctx.project_id.clone(),
            scopes: ctx.scopes.clone(),
            roles,
            provider_id: String::new(),
            auth_method: String::new(),
        }
    }

    /// Any identity string a policy `subject` selector may match against.
    fn identities(&self) -> [&str; 4] {
        [
            self.subject.as_str(),
            self.service_identity.as_str(),
            self.user_id.as_str(),
            self.principal_id.as_str(),
        ]
    }
}

/// The thing being acted upon.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceRef {
    pub resource_type: String,
    pub resource_name: String,
    pub message_type: String,
    pub schema: String,
    pub table: String,
    pub backend: String,
    pub instance: String,
}

impl ResourceRef {
    /// Message-type resource (the common broker case).
    pub fn message(message_type: impl Into<String>) -> Self {
        let message_type = message_type.into();
        Self {
            resource_type: "message".to_string(),
            resource_name: message_type.clone(),
            message_type,
            ..Self::default()
        }
    }

    /// Candidate strings a policy `resource_pattern` may match against.
    fn selectors(&self) -> [&str; 4] {
        [
            self.resource_name.as_str(),
            self.message_type.as_str(),
            self.table.as_str(),
            self.resource_type.as_str(),
        ]
    }
}

/// One v2 authorization policy. Empty string selectors are wildcards.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct AuthzPolicy {
    pub id: String,
    pub priority: i32,
    pub enabled: bool,
    pub effect: Effect,
    /// Tenant domain — empty/`*` matches any tenant.
    pub tenant: String,
    /// Project domain — empty/`*` matches any project.
    pub project: String,
    /// Subject selector matched against the principal's identities.
    pub subject: String,
    /// Role selector — empty/`*` ignored, else the principal must hold the role.
    pub role: String,
    /// Action selector (`data.select`, `data.*`, `*`, …).
    pub action: String,
    /// Resource selector matched against the `ResourceRef` selectors.
    pub resource: String,
    /// Purpose selector.
    pub purpose: String,
    /// Required relationship — empty = none. When set, an ReBAC tuple
    /// `(principal.subject, <relationship>, resource.resource_name)` must exist
    /// in the same tenant/project for the policy to match.
    pub relationship: String,
    /// Attribute equality conditions; every entry must equal a request attribute.
    pub conditions: BTreeMap<String, String>,
    /// Scopes the principal must hold for the policy to match.
    pub required_scopes: Vec<String>,
}

impl Default for AuthzPolicy {
    fn default() -> Self {
        Self {
            id: String::new(),
            priority: 0,
            enabled: true,
            effect: Effect::Allow,
            tenant: String::new(),
            project: String::new(),
            subject: String::new(),
            role: String::new(),
            action: String::new(),
            resource: String::new(),
            purpose: String::new(),
            relationship: String::new(),
            conditions: BTreeMap::new(),
            required_scopes: Vec::new(),
        }
    }
}

/// RBAC: a principal (by subject) holds a role within a tenant/project domain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleBinding {
    pub subject: String,
    pub role: String,
    pub tenant: String,
    pub project: String,
}

/// ReBAC: `subject <relation> object` within a tenant/project domain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationshipTuple {
    pub subject: String,
    pub relation: String,
    pub object: String,
    pub tenant: String,
    pub project: String,
}

/// The structured authorization result. Mapped to a gRPC status only at the
/// service boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decision {
    pub decision_id: String,
    pub allowed: bool,
    pub effect: Effect,
    pub deny_reason: String,
    pub matched_policy_ids: Vec<String>,
    pub required_scopes: Vec<String>,
    pub policy_version: String,
    pub relationship_version: String,
    pub cache_ttl_seconds: u64,
    pub audit_required: bool,
    /// True when the granting Allow policy matched via a role binding (vs a
    /// direct subject match). Drives the `ROLE_POLICY` decision-source audit
    /// classification. Only meaningful when `allowed`.
    pub via_role: bool,
}

/// A single authorization question.
#[derive(Debug, Clone)]
pub struct AuthzQuery<'a> {
    pub principal: &'a Principal,
    pub resource: &'a ResourceRef,
    pub action: &'a str,
    pub purpose: &'a str,
    pub attributes: &'a BTreeMap<String, String>,
}

/// An immutable, atomically-swappable view of all authorization inputs.
#[derive(Debug, Clone, Default)]
pub struct AuthzSnapshot {
    pub version: String,
    pub relationship_version: String,
    pub policies: Vec<AuthzPolicy>,
    pub role_bindings: Vec<RoleBinding>,
    pub tuples: Vec<RelationshipTuple>,
    /// Fail-open only when no policy exists AND this is true (dev/local). In
    /// production this must be false (deny by default).
    pub default_allow: bool,
}

impl AuthzSnapshot {
    /// Effective roles = principal-carried roles ∪ role bindings matching the
    /// principal's identities within the request domain.
    fn effective_roles(&self, principal: &Principal) -> Vec<String> {
        let mut roles = principal.roles.clone();
        for binding in &self.role_bindings {
            if !domain_match(&binding.tenant, &principal.tenant_id)
                || !domain_match(&binding.project, &principal.project_id)
            {
                continue;
            }
            if principal
                .identities()
                .iter()
                .any(|id| *id == binding.subject)
                && !roles.contains(&binding.role)
            {
                roles.push(binding.role.clone());
            }
        }
        roles
    }

    fn has_tuple(&self, principal: &Principal, relation: &str, object: &str) -> bool {
        self.tuples.iter().any(|t| {
            t.relation == relation
                && t.object == object
                && principal.identities().iter().any(|id| *id == t.subject)
                && domain_match(&t.tenant, &principal.tenant_id)
                && domain_match(&t.project, &principal.project_id)
        })
    }

    fn policy_matches(&self, policy: &AuthzPolicy, roles: &[String], req: &AuthzQuery<'_>) -> bool {
        let p = req.principal;
        let base = domain_match(&policy.tenant, &p.tenant_id)
            && domain_match(&policy.project, &p.project_id)
            && subject_match(&policy.subject, &p.identities())
            && role_match(&policy.role, roles)
            && pattern_match(&policy.action, req.action)
            && resource_match(&policy.resource, &req.resource.selectors())
            && wildcard(&policy.purpose, req.purpose)
            && conditions_match(&policy.conditions, req.attributes)
            && (policy.relationship.is_empty()
                || self.has_tuple(p, &policy.relationship, &req.resource.resource_name));
        if !base {
            return false;
        }
        // `required_scopes` REFINES an Allow — the caller must hold every listed
        // scope to be granted. It must NOT gate a Deny: scopes are caller-asserted
        // capabilities, so letting a missing scope cancel a Deny is fail-open (a
        // broader, lower-priority Allow could then win). A Deny applies once the
        // subject/role/action/resource/domain/relationship predicate matches.
        match policy.effect {
            Effect::Allow => policy
                .required_scopes
                .iter()
                .all(|scope| p.has_scope(scope)),
            Effect::Deny => true,
        }
    }
}

impl AuthzSnapshot {
    /// Deterministic, auditable id over (policy version, principal, resource,
    /// action, purpose). Stable for identical inputs so audit logs can join
    /// decisions across retries without a clock.
    fn decision_id(&self, req: &AuthzQuery<'_>) -> String {
        let mut hasher = Sha256::new();
        for part in [
            self.version.as_str(),
            req.principal.principal_id.as_str(),
            req.principal.subject.as_str(),
            req.principal.tenant_id.as_str(),
            req.principal.project_id.as_str(),
            req.resource.resource_name.as_str(),
            req.resource.message_type.as_str(),
            req.action,
            req.purpose,
        ] {
            hasher.update(part.as_bytes());
            hasher.update([0u8]); // domain separator
        }
        let digest = hasher.finalize();
        // Take 16 hex chars of the digest after the `authz_` prefix. Build from a
        // char iterator rather than a byte slice so this can never panic on a
        // non-ASCII boundary if the format ever changes.
        let id: String = format!("{digest:x}").chars().take(16).collect();
        format!("authz_{id}")
    }
}

// ── Matching helpers ───────────────────────────────────────────────────────

/// Empty / `*` / exact match.
fn wildcard(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || pattern == value
}

/// Domain match: empty / `*` matches any value.
fn domain_match(pattern: &str, value: &str) -> bool {
    wildcard(pattern, value)
}

/// Action/resource pattern: exact, `*`, or dotted prefix wildcard `data.*`.
fn pattern_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() || pattern == "*" || pattern == value {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return value == prefix || value.starts_with(&format!("{prefix}."));
    }
    false
}

fn subject_match(pattern: &str, identities: &[&str]) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || identities.iter().any(|id| *id == pattern)
}

fn role_match(pattern: &str, roles: &[String]) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || roles.iter().any(|r| r == pattern)
}

fn resource_match(pattern: &str, selectors: &[&str]) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() || pattern == "*" {
        return true;
    }
    selectors.iter().any(|sel| pattern_match(pattern, sel))
}

fn conditions_match(
    conditions: &BTreeMap<String, String>,
    attrs: &BTreeMap<String, String>,
) -> bool {
    conditions
        .iter()
        .all(|(key, want)| attrs.get(key).map(String::as_str) == Some(want.as_str()))
}

#[cfg(test)]
mod tests;
