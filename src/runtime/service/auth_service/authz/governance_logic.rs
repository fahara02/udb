//! Pure governance logic: document <-> snapshot conversion, content hashing,
//! diffing, and draft/version state-machine validation.
//!
//! Everything here is DB-free and synchronous so the Phase K3 unit tests can
//! exercise the governance invariants (draft isolation, SoD, simulation diff,
//! rollback, revision bumps) without a live database. The DB handlers in
//! `governance.rs` call into these helpers.

use std::collections::BTreeMap;

use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot, Effect, RelationshipTuple, RoleBinding};

/// A candidate authorization document: the policies, role bindings, and
/// relationship tuples that make up one draft/version. The in-memory form the
/// Casbin engine evaluates and the JSON payload stored on a `PolicyVersion`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolicyDocument {
    pub policies: Vec<AuthzPolicy>,
    pub role_bindings: Vec<RoleBinding>,
    pub tuples: Vec<RelationshipTuple>,
}

impl PolicyDocument {
    /// Build a document from a snapshot, scoped to the given tenant/project
    /// (wildcard rows included). Used when branching a draft from the active
    /// snapshot.
    pub fn from_snapshot(snap: &AuthzSnapshot, tenant: &str, project: &str) -> Self {
        let scope = |t: &str, p: &str| {
            (t.trim().is_empty() || t == "*" || t == tenant)
                && (p.trim().is_empty() || p == "*" || p == project)
        };
        Self {
            policies: snap
                .policies
                .iter()
                .filter(|p| scope(&p.tenant, &p.project))
                .cloned()
                .collect(),
            role_bindings: snap
                .role_bindings
                .iter()
                // Platform authority is an out-of-band, system-provenanced
                // binding. A governed document materializes bindings as literal
                // tuples, so carrying it into a draft would erase that provenance.
                .filter(|b| {
                    scope(&b.tenant, &b.project)
                        && !crate::runtime::service::method_security::is_platform_authority_role(
                            &b.role,
                        )
                })
                .cloned()
                .collect(),
            tuples: snap
                .tuples
                .iter()
                .filter(|t| scope(&t.tenant, &t.project))
                .cloned()
                .collect(),
        }
    }

    /// Materialize an in-memory snapshot from this document for simulation /
    /// explain. Versions are content-addressed so a changed document yields a
    /// changed `policy_version`/`relationship_version` (matches the live store).
    pub fn to_snapshot(&self) -> AuthzSnapshot {
        AuthzSnapshot {
            version: content_version("doc-pol", policy_keys(&self.policies)),
            relationship_version: content_version("doc-rel", tuple_keys(&self.tuples)),
            policies: self.policies.clone(),
            role_bindings: self.role_bindings.clone(),
            tuples: self.tuples.clone(),
            default_allow: false,
        }
    }

    /// Canonical content hash of the whole document (order-independent), used to
    /// detect no-op edits and to stamp `content_hash` on a version.
    pub fn content_hash(&self) -> String {
        let mut keys = policy_keys(&self.policies);
        keys.extend(binding_keys(&self.role_bindings));
        keys.extend(tuple_keys(&self.tuples));
        content_version("doc", keys)
    }

    /// Serialize to the JSON payload stored on a `PolicyVersion`.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "policies": self.policies.iter().map(policy_to_json).collect::<Vec<_>>(),
            "role_bindings": self.role_bindings.iter().map(binding_to_json).collect::<Vec<_>>(),
            "relationship_tuples": self.tuples.iter().map(tuple_to_json).collect::<Vec<_>>(),
        })
    }

    /// Parse the JSON payload stored on a `PolicyVersion`.
    pub fn from_json(value: &serde_json::Value) -> Self {
        let policies = value
            .get("policies")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(policy_from_json).collect())
            .unwrap_or_default();
        let role_bindings = value
            .get("role_bindings")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(binding_from_json).collect())
            .unwrap_or_default();
        let tuples = value
            .get("relationship_tuples")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(tuple_from_json).collect())
            .unwrap_or_default();
        Self {
            policies,
            role_bindings,
            tuples,
        }
    }

    /// Build a document from the governance `PolicyDocument` proto message.
    pub fn from_proto(doc: &authz_pb::PolicyDocument) -> Self {
        Self {
            policies: doc.policies.iter().map(record_to_policy).collect(),
            role_bindings: doc
                .role_bindings
                .iter()
                .map(pb_binding_to_runtime)
                .collect(),
            tuples: doc
                .relationship_tuples
                .iter()
                .map(pb_tuple_to_runtime)
                .collect(),
        }
    }
}

/// Convert a governance `AuthzPolicyRecord` to a runtime policy. Mirrors the
/// PutAuthzPolicy mapping so CreatePolicyRule/PutAuthzPolicy parity holds (K8):
/// role, purpose, relationship, priority, required scopes, conditions, tenant,
/// project are all carried.
pub fn record_to_policy(r: &authz_pb::AuthzPolicyRecord) -> AuthzPolicy {
    let effect = if r.effect.eq_ignore_ascii_case("deny") {
        Effect::Deny
    } else {
        Effect::Allow
    };
    AuthzPolicy {
        id: if r.id.trim().is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            r.id.clone()
        },
        priority: r.priority,
        enabled: r.enabled,
        effect,
        tenant: r.tenant.clone(),
        project: r.project.clone(),
        subject: r.subject.clone(),
        role: r.role.clone(),
        action: r.action.clone(),
        resource: r.resource.clone(),
        purpose: r.purpose.clone(),
        relationship: r.relationship.clone(),
        conditions: r.conditions.clone().into_iter().collect(),
        required_scopes: r.required_scopes.clone(),
    }
}

pub fn pb_binding_to_runtime(b: &authz_pb::RoleBinding) -> RoleBinding {
    RoleBinding {
        subject: b.subject.clone(),
        role: b.role.clone(),
        tenant: b.tenant.clone(),
        project: b.project.clone(),
    }
}

pub fn pb_tuple_to_runtime(t: &authz_pb::RelationshipTuple) -> RelationshipTuple {
    RelationshipTuple {
        subject: t.subject.clone(),
        relation: t.relation.clone(),
        object: t.object.clone(),
        tenant: t.tenant.clone(),
        project: t.project.clone(),
    }
}

// ── Diffing (K2.3 machine-readable diff) ───────────────────────────────────

/// A structural diff between two documents (before -> after), keyed by stable
/// element id. Returned as proto `PolicyDiffEntry`s and a JSON document.
pub fn diff_documents(
    before: &PolicyDocument,
    after: &PolicyDocument,
) -> Vec<authz_pb::PolicyDiffEntry> {
    let mut entries = Vec::new();
    diff_set(
        "policy",
        before
            .policies
            .iter()
            .map(|p| (p.id.clone(), policy_to_json(p))),
        after
            .policies
            .iter()
            .map(|p| (p.id.clone(), policy_to_json(p))),
        &mut entries,
    );
    diff_set(
        "role_binding",
        before
            .role_bindings
            .iter()
            .map(|b| (binding_key(b), binding_to_json(b))),
        after
            .role_bindings
            .iter()
            .map(|b| (binding_key(b), binding_to_json(b))),
        &mut entries,
    );
    diff_set(
        "relationship_tuple",
        before
            .tuples
            .iter()
            .map(|t| (tuple_key(t), tuple_to_json(t))),
        after
            .tuples
            .iter()
            .map(|t| (tuple_key(t), tuple_to_json(t))),
        &mut entries,
    );
    entries
}

fn diff_set(
    kind: &str,
    before: impl Iterator<Item = (String, serde_json::Value)>,
    after: impl Iterator<Item = (String, serde_json::Value)>,
    out: &mut Vec<authz_pb::PolicyDiffEntry>,
) {
    let before: BTreeMap<String, serde_json::Value> = before.collect();
    let after: BTreeMap<String, serde_json::Value> = after.collect();
    for (id, av) in &after {
        match before.get(id) {
            None => out.push(authz_pb::PolicyDiffEntry {
                change: "added".to_string(),
                kind: kind.to_string(),
                id: id.clone(),
                before_json: String::new(),
                after_json: av.to_string(),
            }),
            Some(bv) if bv != av => out.push(authz_pb::PolicyDiffEntry {
                change: "changed".to_string(),
                kind: kind.to_string(),
                id: id.clone(),
                before_json: bv.to_string(),
                after_json: av.to_string(),
            }),
            _ => {}
        }
    }
    for (id, bv) in &before {
        if !after.contains_key(id) {
            out.push(authz_pb::PolicyDiffEntry {
                change: "removed".to_string(),
                kind: kind.to_string(),
                id: id.clone(),
                before_json: bv.to_string(),
                after_json: String::new(),
            });
        }
    }
}

/// JSON form of a diff for machine-readable export.
pub fn diff_to_json(entries: &[authz_pb::PolicyDiffEntry]) -> serde_json::Value {
    serde_json::json!({
        "schema": "udb.authz.policy-diff.v1",
        "entries": entries
            .iter()
            .map(|e| serde_json::json!({
                "change": e.change,
                "kind": e.kind,
                "id": e.id,
                "before": e.before_json,
                "after": e.after_json,
            }))
            .collect::<Vec<_>>(),
    })
}

// ── Draft / version state machine (K2.1, K2.2) ─────────────────────────────

/// Draft lifecycle status strings (match `PolicyDraft.status` defaults).
pub const DRAFT_OPEN: &str = "OPEN";
pub const DRAFT_IN_REVIEW: &str = "IN_REVIEW";
pub const DRAFT_APPROVED: &str = "APPROVED";
pub const DRAFT_REJECTED: &str = "REJECTED";

/// Whether a draft in `status` may still be edited (UpdatePolicyDraft).
pub fn draft_editable(status: &str) -> bool {
    matches!(status, DRAFT_OPEN | DRAFT_REJECTED)
}

/// Whether a draft in `status` may be submitted for review.
pub fn draft_submittable(status: &str) -> bool {
    matches!(status, DRAFT_OPEN | DRAFT_REJECTED)
}

/// Whether a draft in `status` is awaiting an approve/reject decision.
pub fn draft_reviewable(status: &str) -> bool {
    status == DRAFT_IN_REVIEW
}

/// Separation of duties (K2.2): the author of a high-risk draft may NOT approve
/// it. Reviewer must differ from author for high-risk changes; low-risk changes
/// may be self-approved (configurable upstream, but high-risk is hard-blocked).
pub fn approval_allowed(high_risk: bool, author: &str, reviewer: &str) -> bool {
    if !high_risk {
        return true;
    }
    !author.trim().eq_ignore_ascii_case(reviewer.trim()) && !reviewer.trim().is_empty()
}

// ── JSON encoders for the document elements ────────────────────────────────

pub fn policy_to_json(p: &AuthzPolicy) -> serde_json::Value {
    serde_json::json!({
        "id": p.id,
        "priority": p.priority,
        "enabled": p.enabled,
        "effect": p.effect.as_str(),
        "tenant": p.tenant,
        "project": p.project,
        "subject": p.subject,
        "role": p.role,
        "action": p.action,
        "resource": p.resource,
        "purpose": p.purpose,
        "relationship": p.relationship,
        "conditions": p.conditions,
        "required_scopes": p.required_scopes,
    })
}

fn policy_from_json(value: &serde_json::Value) -> Option<AuthzPolicy> {
    let s = |k: &str| {
        value
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    let effect = if s("effect").eq_ignore_ascii_case("deny") {
        Effect::Deny
    } else {
        Effect::Allow
    };
    let conditions = value
        .get("conditions")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();
    let required_scopes = value
        .get("required_scopes")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    Some(AuthzPolicy {
        id: s("id"),
        priority: value.get("priority").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        enabled: value
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        effect,
        tenant: s("tenant"),
        project: s("project"),
        subject: s("subject"),
        role: s("role"),
        action: s("action"),
        resource: s("resource"),
        purpose: s("purpose"),
        relationship: s("relationship"),
        conditions,
        required_scopes,
    })
}

fn binding_to_json(b: &RoleBinding) -> serde_json::Value {
    serde_json::json!({
        "subject": b.subject,
        "role": b.role,
        "tenant": b.tenant,
        "project": b.project,
    })
}

fn binding_from_json(value: &serde_json::Value) -> Option<RoleBinding> {
    let s = |k: &str| {
        value
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    Some(RoleBinding {
        subject: s("subject"),
        role: s("role"),
        tenant: s("tenant"),
        project: s("project"),
    })
}

fn tuple_to_json(t: &RelationshipTuple) -> serde_json::Value {
    serde_json::json!({
        "subject": t.subject,
        "relation": t.relation,
        "object": t.object,
        "tenant": t.tenant,
        "project": t.project,
    })
}

fn tuple_from_json(value: &serde_json::Value) -> Option<RelationshipTuple> {
    let s = |k: &str| {
        value
            .get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    Some(RelationshipTuple {
        subject: s("subject"),
        relation: s("relation"),
        object: s("object"),
        tenant: s("tenant"),
        project: s("project"),
    })
}

// ── Content-addressing helpers ─────────────────────────────────────────────

fn policy_keys(policies: &[AuthzPolicy]) -> Vec<String> {
    policies
        .iter()
        .map(|p| {
            let mut scopes = p.required_scopes.clone();
            scopes.sort();
            format!(
                "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{:?}\u{1f}{}\u{1f}{:?}",
                p.id, p.priority, p.enabled, p.effect.as_str(), p.tenant, p.project, p.subject,
                p.role, p.action, p.resource, p.purpose, p.conditions, p.relationship, scopes,
            )
        })
        .collect()
}

fn binding_keys(bindings: &[RoleBinding]) -> Vec<String> {
    bindings.iter().map(binding_key).collect()
}

fn binding_key(b: &RoleBinding) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        b.subject, b.role, b.tenant, b.project
    )
}

fn tuple_keys(tuples: &[RelationshipTuple]) -> Vec<String> {
    tuples.iter().map(tuple_key).collect()
}

fn tuple_key(t: &RelationshipTuple) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        t.subject, t.relation, t.object, t.tenant, t.project
    )
}

fn content_version(prefix: &str, mut keys: Vec<String>) -> String {
    use std::hash::{Hash, Hasher};
    keys.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    keys.len().hash(&mut hasher);
    for k in &keys {
        k.hash(&mut hasher);
    }
    format!("{prefix}-{}-{:016x}", keys.len(), hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::authz::{AuthzQuery, Principal, ResourceRef};

    fn allow_policy(id: &str, role: &str, action: &str, resource: &str) -> AuthzPolicy {
        AuthzPolicy {
            id: id.to_string(),
            effect: Effect::Allow,
            tenant: "acme".to_string(),
            role: role.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
            ..Default::default()
        }
    }

    fn reader_binding(subject: &str) -> RoleBinding {
        RoleBinding {
            subject: subject.to_string(),
            role: "reader".to_string(),
            tenant: "acme".to_string(),
            project: String::new(),
        }
    }

    fn alice() -> Principal {
        Principal {
            subject: "alice".to_string(),
            tenant_id: "acme".to_string(),
            ..Default::default()
        }
    }

    /// Evaluate `action` for `alice` on `invoice` against `snap`.
    async fn decide_invoice(snap: &AuthzSnapshot, action: &str) -> bool {
        let principal = alice();
        let resource = ResourceRef::message("invoice");
        let attrs = BTreeMap::new();
        snap.casbin_authorize(&AuthzQuery {
            principal: &principal,
            resource: &resource,
            action,
            purpose: "",
            attributes: &attrs,
        })
        .await
        .allowed
    }

    // K3: a draft must not affect live decisions before activation. We model the
    // "live" snapshot as the active document and the draft as a separate
    // document; only the draft snapshot reflects the change.
    #[tokio::test]
    async fn draft_does_not_affect_live_decisions_before_activation() {
        let active = PolicyDocument {
            policies: vec![allow_policy("p1", "reader", "data.select", "invoice")],
            role_bindings: vec![reader_binding("alice")],
            tuples: vec![],
        };
        // Draft additionally allows delete.
        let mut draft = active.clone();
        draft
            .policies
            .push(allow_policy("p2", "reader", "data.delete", "invoice"));

        let active_snap = active.to_snapshot();
        let draft_snap = draft.to_snapshot();

        // Active (live) snapshot still denies delete.
        assert!(
            !decide_invoice(&active_snap, "data.delete").await,
            "draft must not change live decisions"
        );
        // Draft snapshot allows it (proving the change is real but isolated).
        assert!(
            decide_invoice(&draft_snap, "data.delete").await,
            "draft snapshot reflects the candidate change"
        );
        // Snapshot versions differ -> activation would be a real change.
        assert_ne!(active_snap.version, draft_snap.version);
    }

    // K3: approval flow blocks self-approval for high-risk changes.
    #[test]
    fn approval_blocks_self_approval_for_high_risk() {
        assert!(!approval_allowed(true, "alice", "alice"));
        assert!(!approval_allowed(true, "alice", "ALICE")); // case-insensitive
        assert!(approval_allowed(true, "alice", "bob"));
        // Low-risk may be self-approved.
        assert!(approval_allowed(false, "alice", "alice"));
        // High-risk with empty reviewer is rejected.
        assert!(!approval_allowed(true, "alice", ""));
    }

    // K3: simulation shows active-vs-draft differences as a diff.
    #[test]
    fn simulation_diff_shows_active_vs_draft() {
        let before = PolicyDocument {
            policies: vec![allow_policy("p1", "reader", "data.select", "invoice")],
            ..Default::default()
        };
        let mut after = before.clone();
        after
            .policies
            .push(allow_policy("p2", "reader", "data.delete", "invoice"));
        let diff = diff_documents(&before, &after);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].change, "added");
        assert_eq!(diff[0].kind, "policy");
        assert_eq!(diff[0].id, "p2");

        // Changing a policy in place is a "changed" entry.
        let mut changed = before.clone();
        changed.policies[0].effect = Effect::Deny;
        let diff2 = diff_documents(&before, &changed);
        assert_eq!(diff2.len(), 1);
        assert_eq!(diff2[0].change, "changed");
        assert_eq!(diff2[0].id, "p1");
    }

    // K3: a no-op edit (same content, possibly reordered) does not change the
    // content hash; any real edit does — so the revision bump is content-driven.
    #[test]
    fn content_hash_is_order_independent_and_change_sensitive() {
        let a = PolicyDocument {
            policies: vec![
                allow_policy("p1", "reader", "data.select", "invoice"),
                allow_policy("p2", "writer", "data.upsert", "invoice"),
            ],
            ..Default::default()
        };
        let b = PolicyDocument {
            policies: vec![
                allow_policy("p2", "writer", "data.upsert", "invoice"),
                allow_policy("p1", "reader", "data.select", "invoice"),
            ],
            ..Default::default()
        };
        assert_eq!(a.content_hash(), b.content_hash(), "order must not matter");
        let mut c = a.clone();
        c.policies[0].effect = Effect::Deny;
        assert_ne!(
            a.content_hash(),
            c.content_hash(),
            "an edit must change the hash"
        );
    }

    // K3: rollback restores prior decisions. Model: active=v2 (delete allowed),
    // rollback target=v1 (delete denied). After rollback the v1 document decides.
    #[tokio::test]
    async fn rollback_restores_prior_decisions() {
        let v1 = PolicyDocument {
            policies: vec![allow_policy("p1", "reader", "data.select", "invoice")],
            role_bindings: vec![reader_binding("alice")],
            tuples: vec![],
        };
        let mut v2 = v1.clone();
        v2.policies
            .push(allow_policy("p2", "reader", "data.delete", "invoice"));

        let v2_snap = v2.to_snapshot();
        assert!(
            decide_invoice(&v2_snap, "data.delete").await,
            "v2 allows delete"
        );
        // Rolling back to v1 restores the original deny.
        let v1_snap = v1.to_snapshot();
        assert!(
            !decide_invoice(&v1_snap, "data.delete").await,
            "rollback to v1 must restore the prior deny"
        );
    }

    // K3 (K8 parity): an AuthzPolicyRecord round-trips its full field richness
    // (role, purpose, relationship, priority, required scopes, conditions).
    #[test]
    fn policy_record_carries_full_richness() {
        let rec = authz_pb::AuthzPolicyRecord {
            id: "p1".to_string(),
            priority: 7,
            enabled: true,
            effect: "allow".to_string(),
            tenant: "acme".to_string(),
            project: "proj".to_string(),
            subject: "u1".to_string(),
            role: "reader".to_string(),
            action: "data.select".to_string(),
            resource: "invoice".to_string(),
            purpose: "ops".to_string(),
            relationship: "owner".to_string(),
            conditions: std::collections::HashMap::from([("k".to_string(), "v".to_string())]),
            required_scopes: vec!["udb:read".to_string()],
        };
        let p = record_to_policy(&rec);
        assert_eq!(p.priority, 7);
        assert_eq!(p.role, "reader");
        assert_eq!(p.purpose, "ops");
        assert_eq!(p.relationship, "owner");
        assert_eq!(p.required_scopes, vec!["udb:read".to_string()]);
        assert_eq!(p.conditions.get("k"), Some(&"v".to_string()));
        // Document JSON round-trips losslessly.
        let doc = PolicyDocument {
            policies: vec![p.clone()],
            ..Default::default()
        };
        let parsed = PolicyDocument::from_json(&doc.to_json());
        assert_eq!(parsed.policies[0], p);
    }

    // K3: draft state-machine guards.
    #[test]
    fn draft_state_machine_guards() {
        assert!(draft_editable(DRAFT_OPEN));
        assert!(draft_editable(DRAFT_REJECTED));
        assert!(!draft_editable(DRAFT_IN_REVIEW));
        assert!(!draft_editable("MERGED"));
        assert!(draft_submittable(DRAFT_OPEN));
        assert!(!draft_submittable(DRAFT_IN_REVIEW));
        assert!(draft_reviewable(DRAFT_IN_REVIEW));
        assert!(!draft_reviewable(DRAFT_OPEN));
    }
}
