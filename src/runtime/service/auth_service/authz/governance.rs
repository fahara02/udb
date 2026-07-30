//! Phase K: Authz Policy Governance.
//!
//! Draft/version/approval workflow, separation of duties, simulation/explain,
//! durable cluster invalidation, built-in role seeding, and legacy ABAC
//! migration — all layered over the existing Postgres-backed authz store and
//! the [`crate::runtime::authz::PolicyEngine`] decision surface.
//!
//! Governance invariants:
//!   * Drafts and simulations NEVER mutate the live authorization snapshot.
//!     They are evaluated in-memory through the Casbin engine.
//!   * Only `ActivatePolicyVersion` / `RollbackPolicyVersion` change the active
//!     policy/role/tuple rows, and each bumps the durable [`AuthzRevision`].
//!   * Every authz admin mutation is itself authorized under
//!     `native.authz.governance` with an explicit governance scope (or a
//!     short-TTL break-glass grant) — never a bare bearer token.
//!   * Separation of duties: the author of a high-risk draft cannot approve it.

use super::*;
use crate::runtime::authz::PolicyEngine;

// ── Governance scopes / SoD constants ──────────────────────────────────────

/// Scope granting full authz governance authority (activate/rollback/seed).
pub(super) const SCOPE_AUTHZ_ADMIN: &str = "authz:admin";
/// Scope to author/edit/submit policy drafts and write policy rows.
pub(super) const SCOPE_POLICY_WRITE: &str = "authz:policy:write";
/// Scope to approve/reject submitted drafts (distinct from authoring).
pub(super) const SCOPE_POLICY_APPROVE: &str = "authz:policy:approve";
/// Scope to read governance state (drafts, versions, diffs, simulations).
pub(super) const SCOPE_POLICY_READ: &str = "authz:policy:read";
/// Scope to write/assign roles.
pub(super) const SCOPE_ROLE_WRITE: &str = "authz:role:write";
/// Scope explicitly permitting a verified caller to act AS a different governance
/// subject (impersonation). Without it (and absent a cross-tenant admin identity)
/// a claim acting under a different `actor.subject` is rejected (fail closed).
pub(super) const SCOPE_AUTHZ_IMPERSONATE: &str = "authz:impersonate";

/// Role that authorizes a caller to exercise an emergency break-glass governance
/// bypass. Break-glass skips the standing-scope / separation-of-duties gate, so it
/// is gated on this VERIFIED role on the authenticated principal — never on the
/// request-supplied `actor.break_glass` bool alone (which is fully caller
/// controlled). Mirrors the seeded `break_glass_admin` [`BUILTIN_ROLES`] entry.
pub(super) const ROLE_BREAK_GLASS_ADMIN: &str = "break_glass_admin";

/// Maximum lifetime of a break-glass grant. A break-glass actor whose
/// `break_glass_expires_at_unix` is unset or further out than this is rejected,
/// so emergency bypasses are always short-lived.
pub(super) const BREAK_GLASS_MAX_TTL_SECS: i64 = 900; // 15 minutes

fn governance_permission_status(
    rpc: &str,
    policy_decision_id: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        tonic::Code::PermissionDenied,
        rpc,
        policy_decision_id,
        message,
    )
}

fn governance_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authz", operation, message)
}

fn governance_actor_required_status(rpc: &str) -> Status {
    governance_permission_status(
        rpc,
        "governance_actor_required",
        format!("{rpc} requires a governance actor with an explicit authz scope"),
    )
}

fn governance_impersonation_denied_status(
    rpc: &str,
    claim_subject: &str,
    actor_subject: &str,
) -> Status {
    governance_permission_status(
        rpc,
        "governance_impersonation_scope_required",
        format!(
            "{rpc}: claim subject {claim_subject:?} may not act as {actor_subject:?} without the {SCOPE_AUTHZ_IMPERSONATE} scope or a cross-tenant admin identity"
        ),
    )
}

fn break_glass_reason_required_status(rpc: &str) -> Status {
    governance_permission_status(
        rpc,
        "break_glass_reason_required",
        "break-glass requires a reason",
    )
}

fn break_glass_ttl_invalid_status(rpc: &str) -> Status {
    governance_permission_status(
        rpc,
        "break_glass_ttl_invalid",
        format!(
            "break-glass grant must expire within {BREAK_GLASS_MAX_TTL_SECS}s and be in the future"
        ),
    )
}

fn break_glass_role_required_status(rpc: &str) -> Status {
    governance_permission_status(
        rpc,
        "break_glass_role_required",
        format!(
            "{rpc}: break-glass requires the {ROLE_BREAK_GLASS_ADMIN} role on the authenticated principal; a request-asserted break-glass flag is not sufficient"
        ),
    )
}

/// Whether `roles` contains the break-glass admin role (case-insensitive, trimmed).
fn holds_break_glass_admin_role<S: AsRef<str>>(roles: &[S]) -> bool {
    roles.iter().any(|r| {
        r.as_ref()
            .trim()
            .eq_ignore_ascii_case(ROLE_BREAK_GLASS_ADMIN)
    })
}

/// Outcome of evaluating whether a request may exercise an emergency break-glass
/// grant. Break-glass skips the standing-scope / SoD gate, so it is authorized
/// against the VERIFIED principal, never the request-supplied `actor.break_glass`
/// bool alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BreakGlassEval {
    /// Not a break-glass request; fall through to the standing-scope/SoD gate.
    NotAsserted,
    /// Break-glass asserted AND the verified identity is authorized to use it.
    Authorized,
    /// Break-glass asserted but the verified principal lacks the break-glass role.
    /// Fail closed: deny (do NOT ignore-and-continue).
    RoleDenied,
}

/// Decide whether an asserted break-glass grant is authorized.
///
/// Over an authenticated transport (`claim_present`) the caller may exercise
/// break-glass ONLY if the VERIFIED claim actually holds the `break_glass_admin`
/// role (or a genuine cross-tenant/platform-admin identity). A caller that clears
/// the coarse transport scope and self-asserts `actor.break_glass=true` without
/// that role is DENIED — the request bool can never bypass the SoD gate.
///
/// Absent a validated claim (in-process / test / trusted internal caller) the
/// actor body is trusted exactly as the standing-scope gate trusts
/// `actor.scopes` in the same fallback, so break-glass is permitted there.
fn evaluate_break_glass(
    break_glass: bool,
    claim_present: bool,
    claim_roles: &[String],
    claim_cross_tenant_admin: bool,
) -> BreakGlassEval {
    if !break_glass {
        return BreakGlassEval::NotAsserted;
    }
    if !claim_present {
        return BreakGlassEval::Authorized;
    }
    if claim_cross_tenant_admin || holds_break_glass_admin_role(claim_roles) {
        BreakGlassEval::Authorized
    } else {
        BreakGlassEval::RoleDenied
    }
}

fn governance_scope_required_status(rpc: &str, required_scopes: &[&str]) -> Status {
    governance_permission_status(
        rpc,
        "governance_scope_required",
        format!(
            "{rpc} requires one of the authz governance scopes {:?} (or break-glass); a bearer token alone is insufficient",
            required_scopes
        ),
    )
}

fn governance_policy_denied_status(rpc: &str, deny_reason: &str) -> Status {
    governance_permission_status(
        rpc,
        "governance_policy_denied",
        format!("{rpc} denied by native.authz.governance policy: {deny_reason}"),
    )
}

/// Built-in role definitions seeded per tenant (Phase K5). `(role_code, name)`.
/// These cover the distinct security-admin / policy-author / policy-approver /
/// auditor separation-of-duties roles plus operational admin roles.
pub(super) const BUILTIN_ROLES: &[(&str, &str, &str)] = &[
    (
        "organization_owner",
        "Organization Owner",
        "Full control across the organization",
    ),
    (
        "tenant_admin",
        "Tenant Admin",
        "Administers a single tenant",
    ),
    (
        "security_admin",
        "Security Admin",
        "Manages authn/authz security configuration",
    ),
    (
        "policy_author",
        "Policy Author",
        "Drafts and submits authorization policy changes",
    ),
    (
        "policy_approver",
        "Policy Approver",
        "Reviews and approves authorization policy changes",
    ),
    (
        "auditor",
        "Auditor",
        "Read-only access to audit and governance state",
    ),
    (
        "service_account_manager",
        "Service Account Manager",
        "Manages service accounts",
    ),
    ("api_key_manager", "API Key Manager", "Manages API keys"),
    (
        "billing_admin",
        "Billing Admin",
        "Manages billing configuration",
    ),
    (
        "break_glass_admin",
        "Break-Glass Admin",
        "Emergency break-glass operations",
    ),
];

impl AuthzServiceImpl {
    // ── Native models for the governance tables ────────────────────────────

    pub(super) fn policy_sets_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicySet",
            &[
                "policy_set_id",
                "tenant_id",
                "project_id",
                "name",
                "active_version_id",
                "rollback_version_id",
                "description",
                "created_by",
                "deleted_at",
            ],
        )
    }

    pub(super) fn policy_drafts_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicyDraft",
            &[
                "draft_id",
                "tenant_id",
                "project_id",
                "title",
                "description",
                "proposed_policies_json",
                "proposed_tuples_json",
                "base_version_id",
                "status",
                "author",
                "high_risk",
            ],
        )
    }

    pub(super) fn policy_versions_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicyVersion",
            &[
                "policy_version_id",
                "policy_set_id",
                "version_number",
                "state",
                "snapshot_hash",
                "created_by",
                "activated_by",
                "rollback_of",
                "change_reason",
                "revision",
                "content_hash",
                "tenant_id",
                "project_id",
                "payload_json",
                "high_risk",
                "submitted_by",
                "source_draft_id",
            ],
        )
    }

    pub(super) fn policy_approvals_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicyApproval",
            &[
                "approval_id",
                "draft_id",
                "tenant_id",
                "actor",
                "role",
                "decision",
                "reason",
            ],
        )
    }

    pub(super) fn authz_revisions_model(&self) -> NativeModel {
        native_model(
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
        )
    }

    pub(super) fn policy_simulations_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicySimulation",
            &[
                "simulation_id",
                "policy_version_id",
                "principal_json",
                "resource_json",
                "action",
                "purpose",
                "active_decision_json",
                "draft_decision_json",
                "diff_json",
                "tenant_id",
                "project_id",
            ],
        )
    }

    // ── Governance authorization gate (K2.2) ───────────────────────────────

    /// Authorize a governance mutation as a resource under
    /// `native.authz.governance` and require an explicit governance scope (or a
    /// valid short-TTL break-glass grant). Fails closed: a bare bearer token
    /// with no governance scope is rejected. Returns the canonical actor
    /// subject used for audit/`changed_by`.
    ///
    /// `resource_action` is the governance verb (e.g. `governance.activate`);
    /// `required_scopes` lists scopes that satisfy the gate (any one suffices).
    pub(super) async fn authorize_governance(
        &self,
        actor: Option<&authz_pb::GovernanceActor>,
        rpc: &str,
        resource_action: &str,
        required_scopes: &[&str],
        now: i64,
    ) -> Result<String, Status> {
        let actor = actor.ok_or_else(|| governance_actor_required_status(rpc))?;
        // Identity is bound to the VERIFIED claim when this call arrived over the
        // authenticated control-plane transport. The `actor.*` body fields are
        // request-supplied intent and MUST NOT be used to attribute audit/events
        // or to satisfy the standing-scope gate when a claim is present (D1/D2).
        // The `!claim_context_present()` fallback (in-process / test / trusted
        // internal caller) keeps the legacy `actor.*`-derived behavior, matching
        // `authorize_action`'s posture.
        let claim_present = crate::runtime::service::method_security::claim_context_present();

        // Claim-derived identity, computed ONCE and reused across the break-glass
        // branch, the standing-scope gate, and the Casbin Principal build below.
        let claim_subject = if claim_present {
            crate::runtime::service::method_security::current_claim_context().subject
        } else {
            String::new()
        };

        // Impersonation gate (shared by both branches): a claim acting as a
        // DIFFERENT body subject is rejected unless the claim is a cross-tenant
        // admin or explicitly carries the `authz:impersonate` scope. When allowed,
        // `actor.subject` stays the impersonation target and the audit stamps both
        // the real and impersonated identities below.
        let impersonating = if claim_present && !actor.subject.trim().is_empty() {
            let ctx = crate::runtime::service::method_security::current_claim_context();
            if !claim_subject.is_empty() && actor.subject != claim_subject {
                let allowed = ctx.is_cross_tenant_admin()
                    || ctx
                        .scopes
                        .iter()
                        .any(|s| s.trim() == SCOPE_AUTHZ_IMPERSONATE);
                if !allowed {
                    return Err(governance_impersonation_denied_status(
                        rpc,
                        &claim_subject,
                        &actor.subject,
                    ));
                }
                true
            } else {
                false
            }
        } else {
            false
        };

        // The effective audit/`changed_by` subject + tenant: the claim subject when
        // present (impersonation keeps `actor.subject` as the target — see below),
        // else the actor body fallback.
        let subject = if claim_present {
            // Under impersonation, attribute the action to the impersonated target
            // (`actor.subject`); the real `claim_subject` is also stamped on the
            // audit/event. Otherwise attribute to the claim subject directly.
            let effective = if impersonating {
                actor.subject.clone()
            } else if claim_subject.trim().is_empty() {
                "anonymous".to_string()
            } else {
                claim_subject.clone()
            };
            effective
        } else if actor.subject.trim().is_empty() {
            "anonymous".to_string()
        } else {
            actor.subject.clone()
        };
        let tenant = if claim_present {
            crate::runtime::service::method_security::current_claim_context().tenant_id
        } else {
            actor.tenant_id.clone()
        };

        // Break-glass: a short-TTL, reason-bearing emergency bypass. Still
        // audited as a governance resource; just not blocked on a standing scope.
        //
        // The bypass itself is authorized against the VERIFIED principal — NOT the
        // request-supplied `actor.break_glass` bool. Over an authenticated
        // transport the claim must actually hold the `break_glass_admin` role (or a
        // genuine cross-tenant/platform-admin identity); a caller that cleared the
        // coarse transport scope and self-asserted break-glass without that role is
        // denied fail-closed, so the request bool can never skip the SoD gate.
        let break_glass_eval = if actor.break_glass {
            let (claim_roles, claim_cross_admin) = if claim_present {
                let ctx = crate::runtime::service::method_security::current_claim_context();
                // Evaluate the cross-tenant check BEFORE moving `roles` out of `ctx`
                // (`roles` is a non-Copy Vec, so the move would otherwise partially
                // move `ctx` and forbid the subsequent method borrow).
                let claim_cross_admin = ctx.is_cross_tenant_admin();
                (ctx.roles, claim_cross_admin)
            } else {
                (Vec::new(), false)
            };
            evaluate_break_glass(true, claim_present, &claim_roles, claim_cross_admin)
        } else {
            BreakGlassEval::NotAsserted
        };
        if break_glass_eval == BreakGlassEval::RoleDenied {
            self.audit_governance(&subject, &tenant, rpc, resource_action, false, now)
                .await;
            return Err(break_glass_role_required_status(rpc));
        }
        if break_glass_eval == BreakGlassEval::Authorized {
            if actor.break_glass_reason.trim().is_empty() {
                return Err(break_glass_reason_required_status(rpc));
            }
            let exp = actor.break_glass_expires_at_unix;
            if exp <= now || exp - now > BREAK_GLASS_MAX_TTL_SECS {
                return Err(break_glass_ttl_invalid_status(rpc));
            }
            self.audit_governance(&subject, &tenant, rpc, resource_action, true, now)
                .await;
            // Break-glass bypass GRANTED (security-sensitive ops event): a
            // short-TTL, reason-bearing emergency authorization that skipped the
            // standing-scope gate. Always audited so security review sees every
            // break-glass use.
            self.emit_event(
                AuthEvent::new(
                    topics::OPS_BREAK_GLASS_GRANT,
                    subject.clone(),
                    tenant.clone(),
                    serde_json::json!({
                        "subject": subject.clone(),
                        "tenant_id": tenant.clone(),
                        // When impersonating, `subject` is the impersonated target;
                        // record the REAL authenticated claim subject too so security
                        // review sees who actually exercised the break-glass grant.
                        "claim_subject": claim_subject.clone(),
                        "impersonated_subject": if impersonating { actor.subject.clone() } else { String::new() },
                        "impersonation": impersonating,
                        "rpc": rpc,
                        "resource": format!("native.authz.governance/{resource_action}"),
                        "reason": actor.break_glass_reason.clone(),
                        "expires_at_unix": actor.break_glass_expires_at_unix,
                        "granted_at_unix": now,
                    }),
                )
                .with_correlation(format!("break_glass:{subject}:{rpc}"))
                .with_compliance(events::ComplianceEnvelope {
                    // Compliance attributes the action to the REAL authenticated
                    // identity (the claim subject) when present, even under
                    // impersonation; `subject`/event payload carry the target.
                    actor: if claim_present && !claim_subject.trim().is_empty() {
                        claim_subject.clone()
                    } else {
                        subject.clone()
                    },
                    target_resource: format!("native.authz.governance/{resource_action}"),
                    operation: "break_glass_grant".to_string(),
                    outcome: "allow".to_string(),
                    reason_code: if actor.break_glass_reason.trim().is_empty() {
                        "break_glass".to_string()
                    } else {
                        actor.break_glass_reason.clone()
                    },
                    auth_method: "break_glass".to_string(),
                    ..events::ComplianceEnvelope::default()
                }),
            )
            .await;
            return Ok(subject);
        }

        // Standing authority: the caller must carry at least one of the required
        // governance scopes. `authz:admin` is a superset that satisfies any gate.
        // When a verified claim is present the gate is evaluated against the CLAIM
        // scopes (never request-supplied `actor.scopes`); the actor body scopes are
        // the in-process/test fallback only.
        let gate_scopes: Vec<String> = if claim_present {
            crate::runtime::service::method_security::current_claim_context().scopes
        } else {
            actor.scopes.clone()
        };
        let has_scope = gate_scopes.iter().any(|s| {
            let s = s.trim();
            s == SCOPE_AUTHZ_ADMIN || required_scopes.iter().any(|req| *req == s)
        });
        if !has_scope {
            self.audit_governance(&subject, &tenant, rpc, resource_action, false, now)
                .await;
            return Err(governance_scope_required_status(rpc, required_scopes));
        }

        // Authorize the governance action itself through the live snapshot, so an
        // operator may additionally gate governance with Casbin policy over
        // `native.authz.governance` resources. When no governance policy exists,
        // the scope check above is authoritative (scopes are the floor).
        if let Ok(snap) = self.current_snapshot().await {
            if snap
                .policies
                .iter()
                .any(|p| p.resource.starts_with("native.authz.governance"))
            {
                // Build the Casbin Principal from the VERIFIED claim when present
                // (its subject/tenant/project/scopes/roles), so policy is evaluated
                // against the authenticated identity — not request-supplied
                // `actor.*` fields. The in-process/test fallback keeps the actor
                // body identity.
                let principal = if claim_present {
                    crate::runtime::service::method_security::current_claim_context().to_principal()
                } else {
                    Principal {
                        principal_id: subject.clone(),
                        subject: subject.clone(),
                        user_id: subject.clone(),
                        tenant_id: tenant.clone(),
                        project_id: actor.project_id.clone(),
                        scopes: actor.scopes.clone(),
                        roles: actor.roles.clone(),
                        ..Default::default()
                    }
                };
                let resource = ResourceRef {
                    resource_type: "governance".to_string(),
                    resource_name: format!("native.authz.governance/{resource_action}"),
                    message_type: "native.authz.governance".to_string(),
                    ..Default::default()
                };
                let attrs = BTreeMap::new();
                let decision = PolicyEngine::decide(
                    snap.as_ref(),
                    &AuthzQuery {
                        principal: &principal,
                        resource: &resource,
                        action: resource_action,
                        purpose: "governance",
                        attributes: &attrs,
                    },
                )
                .await;
                if !decision.allowed {
                    self.audit_governance(&subject, &tenant, rpc, resource_action, false, now)
                        .await;
                    return Err(governance_policy_denied_status(rpc, &decision.deny_reason));
                }
            }
        }

        self.audit_governance(&subject, &tenant, rpc, resource_action, true, now)
            .await;
        Ok(subject)
    }

    /// Audit a governance authorization decision as a resource under
    /// `native.authz.governance`. Denials are published to the access-denied
    /// stream so security dashboards see attempted governance bypasses; allowed
    /// governance mutations are audited by each handler's own domain event, so we
    /// only emit here on denial to avoid duplicating allow events on read RPCs.
    async fn audit_governance(
        &self,
        subject: &str,
        tenant: &str,
        rpc: &str,
        resource_action: &str,
        allowed: bool,
        now: i64,
    ) {
        if allowed {
            tracing::debug!(
                subject,
                tenant,
                rpc,
                resource = %format!("native.authz.governance/{resource_action}"),
                "governance action authorized"
            );
            return;
        }
        self.emit_event(
            AuthEvent::new(
                topics::ACCESS_DENIED,
                subject.to_string(),
                tenant.to_string(),
                serde_json::json!({
                    "kind": "governance_decision",
                    "rpc": rpc,
                    "resource": format!("native.authz.governance/{resource_action}"),
                    "subject": subject,
                    "tenant_id": tenant,
                    "allowed": false,
                    "decided_at_unix": now,
                }),
            )
            .with_correlation(format!("governance_deny:{subject}:{rpc}"))
            .with_compliance(events::ComplianceEnvelope {
                actor: subject.to_string(),
                target_resource: format!("native.authz.governance/{resource_action}"),
                operation: resource_action.to_string(),
                outcome: "deny".to_string(),
                reason_code: "governance_scope_denied".to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
    }
}

impl AuthzServiceImpl {
    // ── AuthzRevision (K2.1): one durable revision spanning policies, roles,
    //    assignments, and tuples. The latest row per tenant/project is current. ─

    /// Read the current `(policy_revision, relationship_revision, content_hash)`
    /// for a tenant/project. Returns zeros when no revision row exists yet.
    pub(super) async fn current_authz_revision(
        &self,
        tenant: &str,
        project: &str,
    ) -> Result<(i64, i64, String), Status> {
        let pool = self.require_pool()?;
        let m = self.authz_revisions_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {policy_revision}, {relationship_revision}, COALESCE({content_hash}, '') AS content_hash \
             FROM {rel} WHERE {tenant_id} = $1 AND {project_id} = $2 \
             ORDER BY {policy_revision} DESC, {relationship_revision} DESC, {changed_at} DESC LIMIT 1",
            policy_revision = m.q("policy_revision"),
            relationship_revision = m.q("relationship_revision"),
            content_hash = m.q("content_hash"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            changed_at = m.q("changed_at"),
        ))
        .bind(tenant)
        .bind(project)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            governance_internal_status(
                "read_authz_revision",
                format!("read authz revision failed: {err}"),
            )
        })?;
        match row {
            Some(row) => Ok((
                row.try_get("policy_revision").unwrap_or(0),
                row.try_get("relationship_revision").unwrap_or(0),
                row.try_get("content_hash").unwrap_or_default(),
            )),
            None => Ok((0, 0, String::new())),
        }
    }

    /// Append a new `AuthzRevision` row, bumping the policy and/or relationship
    /// counter. Returns the new `(policy_revision, relationship_revision)`. This
    /// is the single revision-bump point invoked by EVERY active authz mutation
    /// — policy edits, role create/update/delete, role assignment/revocation,
    /// relationship/grouping tuple writes, activation, and rollback — so cached
    /// bundles invalidate on any of them, not only policy/tuple edits.
    pub(super) async fn bump_authz_revision(
        &self,
        tenant: &str,
        project: &str,
        change_type: authz_entity_pb::AuthzChangeType,
        content_hash: &str,
        changed_by: &str,
    ) -> Result<(i64, i64), Status> {
        let (cur_policy, cur_rel, _) = self.current_authz_revision(tenant, project).await?;
        let bumps_relationship = matches!(
            change_type,
            authz_entity_pb::AuthzChangeType::Relationship
                | authz_entity_pb::AuthzChangeType::Activation
                | authz_entity_pb::AuthzChangeType::Rollback
        );
        // Policy/role/assignment/activation/rollback all bump the policy counter;
        // relationship + activation + rollback also bump the relationship counter.
        let new_policy = cur_policy + 1;
        let new_rel = if bumps_relationship {
            cur_rel + 1
        } else {
            cur_rel
        };
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            authz_capability_status(
                "revision_persistence",
                "runtime_native_entity_dispatch",
                "native authz requires runtime-backed revision persistence",
            )
        })?;
        // P6.10 Wave 0: revision append via the typed native write (no raw SQL).
        // `revision_id` is server-generated in Rust (was `gen_random_uuid()`);
        // `ConflictStrategy::Error` preserves the immutable append-only contract.
        let mut record = LogicalRecord::new();
        record.insert(
            "revision_id".to_string(),
            LogicalValue::String(Uuid::new_v4().to_string()),
        );
        record.insert(
            "tenant_id".to_string(),
            LogicalValue::String(tenant.to_string()),
        );
        record.insert(
            "project_id".to_string(),
            LogicalValue::String(project.to_string()),
        );
        record.insert("policy_revision".to_string(), LogicalValue::Int(new_policy));
        record.insert(
            "relationship_revision".to_string(),
            LogicalValue::Int(new_rel),
        );
        record.insert(
            "content_hash".to_string(),
            LogicalValue::String(content_hash.to_string()),
        );
        record.insert(
            "changed_by".to_string(),
            LogicalValue::String(changed_by.to_string()),
        );
        record.insert(
            "change_type".to_string(),
            LogicalValue::String(change_type_to_db(change_type).to_string()),
        );
        let context = crate::RequestContext {
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            ..crate::RequestContext::default()
        };
        runtime
            .native_entity_write_for_service(
                "authz",
                &context,
                "udb.core.authz.entity.v1.AuthzRevision",
                record,
                ConflictStrategy::Error,
            )
            .await
            .map_err(|err| {
                governance_internal_status(
                    "bump_authz_revision",
                    format!("bump authz revision failed: {err}"),
                )
            })?;
        Ok((new_policy, new_rel))
    }

    /// Emit the durable cluster-invalidation event (K2.4). Every node tailing the
    /// outbox observes this and reloads its snapshot + revokes cached bundles
    /// below `policy_revision`, rather than waiting for the local TTL.
    pub(super) async fn emit_bundle_invalidation(
        &self,
        tenant: &str,
        project: &str,
        policy_revision: i64,
        relationship_revision: i64,
        reason: &str,
        changed_by: &str,
    ) {
        self.emit_event(AuthEvent::new(
            topics::POLICY_BUNDLE_INVALIDATED,
            format!("{tenant}/{project}"),
            tenant.to_string(),
            serde_json::json!({
                "tenant_id": tenant,
                "project_id": project,
                "policy_revision": policy_revision,
                "relationship_revision": relationship_revision,
                "reason": reason,
                "changed_by": changed_by,
            }),
        ))
        .await;
    }
}

fn change_type_to_db(ct: authz_entity_pb::AuthzChangeType) -> &'static str {
    use authz_entity_pb::AuthzChangeType as T;
    match ct {
        T::Unspecified => "AUTHZ_CHANGE_TYPE_UNSPECIFIED",
        T::Policy => "AUTHZ_CHANGE_TYPE_POLICY",
        T::Role => "AUTHZ_CHANGE_TYPE_ROLE",
        T::RoleAssignment => "AUTHZ_CHANGE_TYPE_ROLE_ASSIGNMENT",
        T::Relationship => "AUTHZ_CHANGE_TYPE_RELATIONSHIP",
        T::Activation => "AUTHZ_CHANGE_TYPE_ACTIVATION",
        T::Rollback => "AUTHZ_CHANGE_TYPE_ROLLBACK",
    }
}

/// Reject a direct role mutation when governed mode is on. Role CRUD /
/// assignment RPCs carry no `GovernanceActor`, so in governed mode they fail
/// closed (callers must use a governed policy draft, which carries role bindings,
/// or a break-glass governance RPC). `required` names the scope that would be
/// needed (`authz:role:write`) in the rejection message. Off by default.
pub(super) fn guard_governed_role_mutation(rpc: &str) -> Result<(), tonic::Status> {
    if governed_mode_enabled() {
        return Err(governed_role_mutation_status(rpc));
    }
    Ok(())
}

fn governed_role_mutation_status(rpc: &str) -> tonic::Status {
    crate::runtime::executor_utils::policy_status(
        "authz_governed_role_mutation",
        "role_mutation_requires_governance",
        format!(
            "governed mode: direct {rpc} is disabled; bind roles through a governed policy draft \
             (carrying role bindings) and activate it, or use a break-glass governance RPC with {SCOPE_ROLE_WRITE}"
        ),
    )
}

/// Whether governed mode is active. In governed mode the direct active-mutation
/// RPCs (PutAuthzPolicy, CreatePolicyRule, role mutations, role binding,
/// relationship writes) require an explicit governance/break-glass grant rather
/// than only a bearer token; otherwise they create drafts. Off by default so
/// existing deployments keep working; opt in with `UDB_AUTHZ_GOVERNED_MODE=1`.
pub(super) fn governed_mode_enabled() -> bool {
    std::env::var("UDB_AUTHZ_GOVERNED_MODE")
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_permission_policy_detail(
        status: &tonic::Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "authz");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn governance_internal_status_carries_typed_detail() {
        let status = governance_internal_status(
            "bump_authz_revision",
            "bump authz revision failed: store closed",
        );
        assert_internal_detail(
            &status,
            "bump_authz_revision",
            "bump authz revision failed: store closed",
        );
    }

    #[test]
    fn governed_role_mutation_denial_carries_policy_detail() {
        let err = governed_role_mutation_status("CreateRole");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "governed mode: direct CreateRole is disabled; bind roles through a governed policy draft \
             (carrying role bindings) and activate it, or use a break-glass governance RPC with authz:role:write"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "authz_governed_role_mutation");
        assert_eq!(
            detail.policy_decision_id,
            "role_mutation_requires_governance"
        );
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn governance_authorization_denials_carry_policy_detail() {
        let actor_required = governance_actor_required_status("ActivatePolicyVersion");
        assert_permission_policy_detail(
            &actor_required,
            "ActivatePolicyVersion",
            "governance_actor_required",
            "ActivatePolicyVersion requires a governance actor with an explicit authz scope",
        );

        let impersonation =
            governance_impersonation_denied_status("SubmitPolicyDraft", "alice", "bob");
        assert_permission_policy_detail(
            &impersonation,
            "SubmitPolicyDraft",
            "governance_impersonation_scope_required",
            "SubmitPolicyDraft: claim subject \"alice\" may not act as \"bob\" without the authz:impersonate scope or a cross-tenant admin identity",
        );

        let break_glass_reason = break_glass_reason_required_status("RollbackPolicyVersion");
        assert_permission_policy_detail(
            &break_glass_reason,
            "RollbackPolicyVersion",
            "break_glass_reason_required",
            "break-glass requires a reason",
        );

        let break_glass_ttl = break_glass_ttl_invalid_status("RollbackPolicyVersion");
        assert_permission_policy_detail(
            &break_glass_ttl,
            "RollbackPolicyVersion",
            "break_glass_ttl_invalid",
            "break-glass grant must expire within 900s and be in the future",
        );

        let scope_required =
            governance_scope_required_status("ApprovePolicyDraft", &[SCOPE_POLICY_APPROVE]);
        assert_permission_policy_detail(
            &scope_required,
            "ApprovePolicyDraft",
            "governance_scope_required",
            "ApprovePolicyDraft requires one of the authz governance scopes [\"authz:policy:approve\"] (or break-glass); a bearer token alone is insufficient",
        );

        let policy_denied = governance_policy_denied_status("SimulatePolicy", "deny-by-rule");
        assert_permission_policy_detail(
            &policy_denied,
            "SimulatePolicy",
            "governance_policy_denied",
            "SimulatePolicy denied by native.authz.governance policy: deny-by-rule",
        );
    }

    #[test]
    fn holds_break_glass_admin_role_matches_case_insensitively() {
        assert!(holds_break_glass_admin_role(&["break_glass_admin"]));
        assert!(holds_break_glass_admin_role(&[" Break_Glass_Admin "]));
        assert!(holds_break_glass_admin_role(&[
            "auditor",
            "break_glass_admin"
        ]));
        assert!(!holds_break_glass_admin_role(&["auditor", "policy_author"]));
        assert!(!holds_break_glass_admin_role::<&str>(&[]));
    }

    // (a) Break-glass asserted over an authenticated transport WITHOUT the
    // `break_glass_admin` role is DENIED — the request-supplied bool cannot skip
    // the standing-scope / SoD gate. A caller who cleared the coarse transport
    // scope stays denied.
    #[test]
    fn break_glass_without_role_is_denied_over_authenticated_transport() {
        let eval = evaluate_break_glass(
            true,
            /* claim_present */ true,
            &["auditor".to_string()],
            /* cross_tenant_admin */ false,
        );
        assert_eq!(eval, BreakGlassEval::RoleDenied);

        // No roles at all → still denied (fail closed).
        let eval_no_roles = evaluate_break_glass(true, true, &[], false);
        assert_eq!(eval_no_roles, BreakGlassEval::RoleDenied);

        // And the denial carries the typed policy detail.
        let status = break_glass_role_required_status("RollbackPolicyVersion");
        assert_permission_policy_detail(
            &status,
            "RollbackPolicyVersion",
            "break_glass_role_required",
            "RollbackPolicyVersion: break-glass requires the break_glass_admin role on the authenticated principal; a request-asserted break-glass flag is not sufficient",
        );
    }

    // (b) With the verified `break_glass_admin` role the caller may use break-glass;
    // a genuine cross-tenant/platform-admin identity is also accepted as a superset.
    #[test]
    fn break_glass_with_role_is_authorized() {
        let eval = evaluate_break_glass(true, true, &["break_glass_admin".to_string()], false);
        assert_eq!(eval, BreakGlassEval::Authorized);

        let eval_cross_admin = evaluate_break_glass(
            true,
            true,
            /* roles */ &[],
            /* cross_tenant_admin */ true,
        );
        assert_eq!(eval_cross_admin, BreakGlassEval::Authorized);
    }

    // (c) break_glass=false is never a break-glass path, so the normal
    // standing-scope / SoD gate applies unaffected regardless of identity.
    #[test]
    fn break_glass_not_asserted_falls_through_to_standing_gate() {
        assert_eq!(
            evaluate_break_glass(false, true, &["break_glass_admin".to_string()], true),
            BreakGlassEval::NotAsserted
        );
        assert_eq!(
            evaluate_break_glass(false, false, &[], false),
            BreakGlassEval::NotAsserted
        );
    }

    // In-process / test / trusted-internal caller (no validated claim): the actor
    // body is trusted exactly as the standing-scope gate trusts `actor.scopes` in
    // the same fallback, so break-glass is permitted without a claim role.
    #[test]
    fn break_glass_without_claim_is_trusted_in_process_fallback() {
        assert_eq!(
            evaluate_break_glass(true, /* claim_present */ false, &[], false),
            BreakGlassEval::Authorized
        );
    }
}
