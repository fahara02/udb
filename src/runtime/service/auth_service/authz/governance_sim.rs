//! Phase K simulation / explain / cluster-invalidation / seed / legacy-migration
//! handlers. Simulations and explains run entirely in memory through the policy
//! engine and never mutate durable authorization state (K2.3).

use super::governance::BUILTIN_ROLES;
use super::governance_logic::{self as glogic, PolicyDocument};
use super::*;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use crate::runtime::authz::PolicyEngine;

fn simulation_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn governance_sim_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("authz", operation, message)
}

impl AuthzServiceImpl {
    pub(super) async fn simulate_policy_impl(
        &self,
        request: Request<authz_pb::SimulatePolicyRequest>,
    ) -> Result<Response<authz_pb::SimulatePolicyResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "SimulatePolicy",
                "governance.simulate",
                &[super::governance::SCOPE_POLICY_READ],
                now,
            )
            .await?;
        let tenant = req.tenant_id.clone();
        let project = req.project_id.clone();

        // Active snapshot scoped to the tenant/project.
        let active_snap = self.current_snapshot().await?;
        let active_doc = PolicyDocument::from_snapshot(&active_snap, &tenant, &project);
        let active_eval = active_doc.to_snapshot();

        // Draft document: an explicit draft, or the supplied ad-hoc candidate.
        // Either way it is evaluated in-memory and NEVER persisted to live tables.
        let draft_doc = if !req.draft_id.trim().is_empty() {
            self.load_draft_document(&req.draft_id).await?
        } else if let Some(candidate) = req.candidate.as_ref() {
            PolicyDocument::from_proto(candidate)
        } else {
            active_doc.clone()
        };
        let draft_eval = draft_doc.to_snapshot();

        let mut results = Vec::with_capacity(req.cases.len());
        let mut aggregate = Vec::new();
        for case in &req.cases {
            let (principal, resource, action, purpose, attrs) = simulation_inputs(case);
            let active = PolicyEngine::decide(
                &active_eval,
                &AuthzQuery {
                    principal: &principal,
                    resource: &resource,
                    action: &action,
                    purpose: &purpose,
                    attributes: &attrs,
                },
            )
            .await;
            let draft = PolicyEngine::decide(
                &draft_eval,
                &AuthzQuery {
                    principal: &principal,
                    resource: &resource,
                    action: &action,
                    purpose: &purpose,
                    attributes: &attrs,
                },
            )
            .await;
            let changed = active.allowed != draft.allowed;
            let diff = serde_json::json!({
                "label": case.label,
                "action": action,
                "active_allowed": active.allowed,
                "draft_allowed": draft.allowed,
                "changed": changed,
            });
            aggregate.push(diff.clone());

            if req.persist {
                self.persist_simulation(
                    &tenant,
                    &project,
                    &req.policy_version_id,
                    case,
                    &action,
                    &purpose,
                    &active,
                    &draft,
                    &diff,
                )
                .await;
            }

            results.push(authz_pb::SimulationResult {
                label: case.label.clone(),
                active_decision: Some(
                    crate::runtime::service::auth_service::mappings::decision_to_pb(&active),
                ),
                draft_decision: Some(
                    crate::runtime::service::auth_service::mappings::decision_to_pb(&draft),
                ),
                changed,
                diff_json: diff.to_string(),
            });
        }
        // Audit the simulation (security-sensitive governance read): records WHO
        // simulated WHICH draft against the active policy, and how many cases
        // changed — never any decision-input credential material.
        let changed_count = results.iter().filter(|r| r.changed).count();
        self.emit_event(
            AuthEvent::new(
                topics::POLICY_SIMULATED,
                if req.draft_id.trim().is_empty() {
                    format!("{tenant}/{project}")
                } else {
                    req.draft_id.clone()
                },
                tenant.clone(),
                serde_json::json!({
                    "tenant_id": tenant.clone(),
                    "project_id": project.clone(),
                    "draft_id": req.draft_id.clone(),
                    "case_count": results.len(),
                    "changed_count": changed_count,
                    "persisted": req.persist,
                }),
            )
            .with_correlation(format!("policy_sim:{tenant}/{project}"))
            .with_compliance(events::ComplianceEnvelope {
                actor: actor.clone(),
                target_resource: if req.draft_id.trim().is_empty() {
                    format!("policy:{tenant}/{project}")
                } else {
                    format!("draft:{}", req.draft_id)
                },
                operation: "policy_simulate".to_string(),
                outcome: "success".to_string(),
                reason_code: "simulation_evaluated".to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authz_pb::SimulatePolicyResponse {
            results,
            diff_json: serde_json::json!({ "cases": aggregate }).to_string(),
        }))
    }

    pub(super) async fn explain_policy_impl(
        &self,
        request: Request<authz_pb::ExplainPolicyRequest>,
    ) -> Result<Response<authz_pb::ExplainPolicyResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        self.authorize_governance(
            req.actor.as_ref(),
            "ExplainPolicy",
            "governance.explain",
            &[super::governance::SCOPE_POLICY_READ],
            now,
        )
        .await?;
        let tenant = req.tenant_id.clone();
        let project = req.project_id.clone();
        let doc = if !req.draft_id.trim().is_empty() {
            self.load_draft_document(&req.draft_id).await?
        } else if let Some(candidate) = req.candidate.as_ref() {
            PolicyDocument::from_proto(candidate)
        } else {
            let snap = self.current_snapshot().await?;
            PolicyDocument::from_snapshot(&snap, &tenant, &project)
        };
        let eval = doc.to_snapshot();
        let case = req.test_case.as_ref().ok_or_else(|| {
            simulation_required_field(
                "test_case",
                "must include a simulation case to explain",
                "test_case is required",
            )
        })?;
        let (principal, resource, action, purpose, attrs) = simulation_inputs(case);
        let decision = PolicyEngine::explain(
            &eval,
            &AuthzQuery {
                principal: &principal,
                resource: &resource,
                action: &action,
                purpose: &purpose,
                attributes: &attrs,
            },
        )
        .await;
        // Explanation: which candidate policies matched the request predicate.
        let explanation: Vec<String> = decision
            .matched_policy_ids
            .iter()
            .map(|id| {
                doc.policies
                    .iter()
                    .find(|p| &p.id == id)
                    .map(|p| {
                        format!(
                            "policy {} [{}] subject='{}' role='{}' action='{}' resource='{}'",
                            p.id,
                            p.effect.as_str(),
                            p.subject,
                            p.role,
                            p.action,
                            p.resource
                        )
                    })
                    .unwrap_or_else(|| format!("policy {id}"))
            })
            .collect();
        Ok(Response::new(authz_pb::ExplainPolicyResponse {
            matched_policy_ids: decision.matched_policy_ids.clone(),
            deny_reason: decision.deny_reason.clone(),
            decision: Some(
                crate::runtime::service::auth_service::mappings::decision_to_pb(&decision),
            ),
            explanation,
        }))
    }

    pub(super) async fn list_policy_versions_impl(
        &self,
        request: Request<authz_pb::ListPolicyVersionsRequest>,
    ) -> Result<Response<authz_pb::ListPolicyVersionsResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        self.authorize_governance(
            req.actor.as_ref(),
            "ListPolicyVersions",
            "governance.version.list",
            &[super::governance::SCOPE_POLICY_READ],
            now,
        )
        .await?;
        let pool = self.require_pool()?;
        let m = self.policy_versions_model();
        let rel = m.relation.clone();
        let state_filter = if req.state == 0 {
            String::new()
        } else {
            super::governance_store::version_state_to_db(
                authz_entity_pb::PolicyVersionState::try_from(req.state).unwrap_or_default(),
            )
            .to_string()
        };
        let rows = sqlx::query(&format!(
            "SELECT {policy_version_id}::TEXT AS policy_version_id, {policy_set_id}::TEXT AS policy_set_id, \
                    {version_number}, {state}, COALESCE({snapshot_hash}, '') AS snapshot_hash, COALESCE({created_by}, '') AS created_by, \
                    COALESCE({activated_by}, '') AS activated_by, COALESCE({rollback_of}::TEXT, '') AS rollback_of, \
                    COALESCE({change_reason}, '') AS change_reason, {revision}, COALESCE({content_hash}, '') AS content_hash, \
                    {tenant_id}::TEXT AS tenant_id, COALESCE({project_id}, '') AS project_id, {payload_json}::TEXT AS payload_json, \
                    {high_risk}, COALESCE({submitted_by}, '') AS submitted_by, COALESCE({source_draft_id}::TEXT, '') AS source_draft_id \
             FROM {rel} \
             WHERE {tenant_id} = $1 AND ($2 = '' OR COALESCE({project_id}, '') = $2) \
               AND ($3 = '' OR {policy_set_id}::TEXT = $3) AND ($4 = '' OR {state} = $4) \
             ORDER BY {version_number} DESC",
            policy_version_id = m.q("policy_version_id"),
            policy_set_id = m.q("policy_set_id"),
            version_number = m.q("version_number"),
            state = m.q("state"),
            snapshot_hash = m.q("snapshot_hash"),
            created_by = m.q("created_by"),
            activated_by = m.q("activated_by"),
            rollback_of = m.q("rollback_of"),
            change_reason = m.q("change_reason"),
            revision = m.q("revision"),
            content_hash = m.q("content_hash"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            payload_json = m.q("payload_json"),
            high_risk = m.q("high_risk"),
            submitted_by = m.q("submitted_by"),
            source_draft_id = m.q("source_draft_id"),
        ))
        .bind(&req.tenant_id)
        .bind(&req.project_id)
        .bind(&req.policy_set_id)
        .bind(&state_filter)
        .fetch_all(pool)
        .await
        .map_err(|err| {
            governance_sim_internal_status(
                "list_policy_versions",
                format!("list policy versions failed: {err}"),
            )
        })?;
        let all: Vec<_> = rows
            .iter()
            .map(super::governance_store::version_from_row)
            .collect();
        let page = req.page.as_ref();
        let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1) as usize;
        let page_size = page
            .map(|p| p.page_size)
            .filter(|s| *s > 0)
            .unwrap_or(all.len().max(1) as i32) as usize;
        let start = page_number.saturating_sub(1).saturating_mul(page_size);
        let total = all.len();
        let versions = all.into_iter().skip(start).take(page_size).collect();
        Ok(Response::new(authz_pb::ListPolicyVersionsResponse {
            page: Some(crate::runtime::service::auth_service::mappings::page_response(total, page)),
            versions,
        }))
    }

    pub(super) async fn get_authz_revision_impl(
        &self,
        request: Request<authz_pb::GetAuthzRevisionRequest>,
    ) -> Result<Response<authz_pb::GetAuthzRevisionResponse>, Status> {
        let req = request.into_inner();
        let (policy_rev, rel_rev, content_hash) = self
            .current_authz_revision(&req.tenant_id, &req.project_id)
            .await?;
        Ok(Response::new(authz_pb::GetAuthzRevisionResponse {
            policy_revision: policy_rev,
            relationship_revision: rel_rev,
            content_hash,
            changed_at: None,
        }))
    }

    pub(super) async fn invalidate_policy_bundles_impl(
        &self,
        request: Request<authz_pb::InvalidatePolicyBundlesRequest>,
    ) -> Result<Response<authz_pb::InvalidatePolicyBundlesResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "InvalidatePolicyBundles",
                "governance.bundles.invalidate",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        // Bumping the policy revision without changing rows is enough to revoke
        // every cached bundle below the new revision (bundles are version-pinned).
        let (policy_rev, rel_rev) = self
            .bump_authz_revision(
                &req.tenant_id,
                &req.project_id,
                authz_entity_pb::AuthzChangeType::Policy,
                "manual-invalidation",
                &actor,
            )
            .await?;
        self.invalidate_snapshot_cache();
        let reason = if req.reason.trim().is_empty() {
            "manual bundle invalidation".to_string()
        } else {
            req.reason.clone()
        };
        self.emit_bundle_invalidation(
            &req.tenant_id,
            &req.project_id,
            policy_rev,
            rel_rev,
            &reason,
            &actor,
        )
        .await;
        // Distinct revocation audit (security-sensitive): every cached SDK bundle
        // below the new revision is revoked. `POLICY_BUNDLE_INVALIDATED` above is
        // the cluster reload signal; this is the compliance-plane revocation record.
        self.emit_event(
            AuthEvent::new(
                topics::POLICY_BUNDLE_REVOKED,
                format!("{}/{}", req.tenant_id, req.project_id),
                req.tenant_id.clone(),
                serde_json::json!({
                    "tenant_id": req.tenant_id.clone(),
                    "project_id": req.project_id.clone(),
                    "policy_revision": policy_rev,
                    "relationship_revision": rel_rev,
                    "reason": reason.clone(),
                    "revoked_by": actor.clone(),
                }),
            )
            .with_correlation(format!(
                "policy_bundle_revoke:{}/{}",
                req.tenant_id, req.project_id
            ))
            .with_compliance(events::ComplianceEnvelope {
                actor: actor.clone(),
                target_resource: format!("policy_bundle:{}/{}", req.tenant_id, req.project_id),
                operation: "policy_bundle_revoke".to_string(),
                outcome: "success".to_string(),
                reason_code: "bundles_invalidated".to_string(),
                policy_version: policy_rev.to_string(),
                relationship_version: rel_rev.to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
        Ok(Response::new(authz_pb::InvalidatePolicyBundlesResponse {
            ok: true,
            policy_revision: policy_rev,
            relationship_revision: rel_rev,
        }))
    }

    pub(super) async fn seed_builtin_roles_impl(
        &self,
        request: Request<authz_pb::SeedBuiltinRolesRequest>,
    ) -> Result<Response<authz_pb::SeedBuiltinRolesResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "SeedBuiltinRoles",
                "governance.roles.seed",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        if req.tenant_id.trim().is_empty() {
            return Err(simulation_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        let pool = self.require_pool()?;
        let m = self.roles_model();
        let rel = m.relation.clone();
        let mut created = 0;
        let mut existing = 0;
        let mut seeded = Vec::new();
        for (code, name, description) in BUILTIN_ROLES {
            let result = sqlx::query(&format!(
                "INSERT INTO {rel} ({role_id}, {name}, {description}, {is_system}, {is_active}, {tenant_id}, {project_id}, {role_code}, {scope_type}) \
                 VALUES (gen_random_uuid(), $1, $2, TRUE, TRUE, $3, $4, $5, 'TENANT') \
                 ON CONFLICT DO NOTHING",
                role_id = m.q("role_id"),
                name = m.q("name"),
                description = m.q("description"),
                is_system = m.q("is_system"),
                is_active = m.q("is_active"),
                tenant_id = m.q("tenant_id"),
                project_id = m.q("project_id"),
                role_code = m.q("role_code"),
                scope_type = m.q("scope_type"),
            ))
            .bind(name)
            .bind(description)
            .bind(&req.tenant_id)
            .bind(&req.project_id)
            .bind(code)
            .execute(pool)
            .await
            .map_err(|err| {
                governance_sim_internal_status(
                    "seed_builtin_role",
                    format!("seed role failed: {err}"),
                )
            })?;
            if result.rows_affected() > 0 {
                created += 1;
            } else {
                existing += 1;
            }
            seeded.push((*code).to_string());
        }
        // Bump the authz revision so any caches refresh after seeding.
        let _ = self
            .bump_authz_revision(
                &req.tenant_id,
                &req.project_id,
                authz_entity_pb::AuthzChangeType::Role,
                "builtin-role-seed",
                &actor,
            )
            .await;
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::SeedBuiltinRolesResponse {
            seeded_role_codes: seeded,
            created,
            existing,
        }))
    }

    pub(super) async fn migrate_legacy_policies_impl(
        &self,
        request: Request<authz_pb::MigrateLegacyPoliciesRequest>,
    ) -> Result<Response<authz_pb::MigrateLegacyPoliciesResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "MigrateLegacyPolicies",
                "governance.legacy.migrate",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        let tenant = req.tenant_id.clone();
        let project = req.project_id.clone();

        // Convert the legacy ABAC seed (docs/abac_seed.json) into governed policy
        // records. Embedded so the migration is deterministic and host-independent.
        let legacy = legacy_abac_document(&tenant);
        let snap = self.current_snapshot().await?;
        let active_doc = PolicyDocument::from_snapshot(&snap, &tenant, &project);
        let diff = glogic::diff_documents(&active_doc, &legacy);

        // Parity simulation: evaluate a sample of the legacy rules against both
        // the active and migrated documents.
        let active_eval = active_doc.to_snapshot();
        let migrated_eval = legacy.to_snapshot();
        let mut simulation = Vec::new();
        for p in legacy.policies.iter().take(8) {
            let principal = Principal {
                principal_id: p.subject.clone(),
                subject: p.subject.clone(),
                service_identity: p.subject.clone(),
                tenant_id: tenant.clone(),
                scopes: p.required_scopes.clone(),
                ..Default::default()
            };
            let resource = ResourceRef::message(&p.resource);
            let attrs = BTreeMap::new();
            let action = if p.action.trim().is_empty() {
                "*".to_string()
            } else {
                p.action.clone()
            };
            let active = PolicyEngine::decide(
                &active_eval,
                &AuthzQuery {
                    principal: &principal,
                    resource: &resource,
                    action: &action,
                    purpose: &p.purpose,
                    attributes: &attrs,
                },
            )
            .await;
            let migrated = PolicyEngine::decide(
                &migrated_eval,
                &AuthzQuery {
                    principal: &principal,
                    resource: &resource,
                    action: &action,
                    purpose: &p.purpose,
                    attributes: &attrs,
                },
            )
            .await;
            simulation.push(authz_pb::SimulationResult {
                label: p.id.clone(),
                active_decision: Some(
                    crate::runtime::service::auth_service::mappings::decision_to_pb(&active),
                ),
                draft_decision: Some(
                    crate::runtime::service::auth_service::mappings::decision_to_pb(&migrated),
                ),
                changed: active.allowed != migrated.allowed,
                diff_json: serde_json::json!({
                    "active_allowed": active.allowed,
                    "migrated_allowed": migrated.allowed,
                })
                .to_string(),
            });
        }

        let report = serde_json::json!({
            "schema": "udb.authz.legacy-migration.v1",
            "tenant_id": tenant,
            "project_id": project,
            "policy_count": legacy.policies.len(),
            "diff_entries": diff.len(),
            "applied": req.apply,
        });

        // When apply=true, create a governed draft carrying the migrated policies
        // (kept behind the draft/activation flow until parity is proven — the old
        // ABAC path stays live until the draft is activated).
        let mut draft = None;
        if req.apply {
            let create = authz_pb::CreatePolicyDraftRequest {
                actor: req.actor.clone(),
                tenant_id: tenant.clone(),
                project_id: project.clone(),
                policy_set_name: if req.policy_set_name.trim().is_empty() {
                    "legacy-migration".to_string()
                } else {
                    req.policy_set_name.clone()
                },
                title: "legacy ABAC migration".to_string(),
                change_reason: "migrate legacy ABAC policies to governed records".to_string(),
                high_risk: true,
                document: Some(super::governance_drafts::document_to_proto(&legacy)),
                branch_from_active: false,
            };
            let resp = self
                .create_policy_draft_impl(Request::new(create))
                .await?
                .into_inner();
            draft = resp.draft;
            let _ = actor; // actor already recorded via governance audit
        }

        Ok(Response::new(authz_pb::MigrateLegacyPoliciesResponse {
            draft,
            diff,
            simulation,
            report_json: report.to_string(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_simulation(
        &self,
        tenant: &str,
        project: &str,
        policy_version_id: &str,
        case: &authz_pb::SimulationCase,
        action: &str,
        purpose: &str,
        active: &crate::runtime::authz::Decision,
        draft: &crate::runtime::authz::Decision,
        diff: &serde_json::Value,
    ) {
        let Ok(pool) = self.require_pool() else {
            return;
        };
        let m = self.policy_simulations_model();
        let rel = m.relation.clone();
        let principal_json = case
            .principal
            .as_ref()
            .map(|p| serde_json::json!({ "subject": p.subject, "user_id": p.user_id }))
            .unwrap_or_else(|| serde_json::json!({}));
        let resource_json = case
            .resource
            .as_ref()
            .map(|r| serde_json::json!({ "resource_name": r.resource_name }))
            .unwrap_or_else(|| serde_json::json!({}));
        let active_json =
            serde_json::json!({ "allowed": active.allowed, "reason": active.deny_reason });
        let draft_json =
            serde_json::json!({ "allowed": draft.allowed, "reason": draft.deny_reason });
        let version_bind = if policy_version_id.trim().is_empty() {
            None
        } else {
            Some(policy_version_id.to_string())
        };
        let result = sqlx::query(&format!(
            "INSERT INTO {rel} ({simulation_id}, {policy_version_id}, {principal_json}, {resource_json}, {action}, {purpose}, {active_decision_json}, {draft_decision_json}, {diff_json}, {tenant_id}, {project_id}) \
             VALUES (gen_random_uuid(), NULLIF($1, '')::UUID, $2::JSONB, $3::JSONB, $4, $5, $6::JSONB, $7::JSONB, $8::JSONB, $9, $10)",
            simulation_id = m.q("simulation_id"),
            policy_version_id = m.q("policy_version_id"),
            principal_json = m.q("principal_json"),
            resource_json = m.q("resource_json"),
            action = m.q("action"),
            purpose = m.q("purpose"),
            active_decision_json = m.q("active_decision_json"),
            draft_decision_json = m.q("draft_decision_json"),
            diff_json = m.q("diff_json"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
        ))
        .bind(version_bind.unwrap_or_default())
        .bind(principal_json.to_string())
        .bind(resource_json.to_string())
        .bind(action)
        .bind(purpose)
        .bind(active_json.to_string())
        .bind(draft_json.to_string())
        .bind(diff.to_string())
        .bind(tenant)
        .bind(project)
        .execute(pool)
        .await;
        if let Err(err) = result {
            tracing::warn!(error = %err, "failed to persist policy simulation");
        }
    }
}

/// Build the (principal, resource, action, purpose, attributes) tuple for a
/// simulation case, defaulting sensibly when fields are absent.
fn simulation_inputs(
    case: &authz_pb::SimulationCase,
) -> (
    Principal,
    ResourceRef,
    String,
    String,
    BTreeMap<String, String>,
) {
    let principal = case
        .principal
        .as_ref()
        .map(crate::runtime::service::auth_service::mappings::authz_principal_to_runtime)
        .unwrap_or_default();
    let resource = case
        .resource
        .as_ref()
        .map(crate::runtime::service::auth_service::mappings::resource_to_runtime)
        .unwrap_or_default();
    let attrs: BTreeMap<String, String> = case.attributes.clone().into_iter().collect();
    (
        principal,
        resource,
        case.action.clone(),
        case.purpose.clone(),
        attrs,
    )
}

/// Convert the legacy `docs/abac_seed.json` ABAC seed into a governed
/// `PolicyDocument`. The file is embedded so the conversion is deterministic and
/// runs without host filesystem access.
fn legacy_abac_document(tenant: &str) -> PolicyDocument {
    use crate::runtime::authz::{AuthzPolicy, Effect};
    const SEED: &str = include_str!("../../../../../docs/abac_seed.json");
    let entries: Vec<serde_json::Value> = serde_json::from_str(SEED).unwrap_or_default();
    let policies = entries
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let s = |k: &str| e.get(k).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let effect = if s("effect").eq_ignore_ascii_case("deny") {
                Effect::Deny
            } else {
                Effect::Allow
            };
            let required_scope = s("required_scope");
            let required_scopes = if required_scope.trim().is_empty() {
                Vec::new()
            } else {
                vec![required_scope]
            };
            let raw_tenant = s("tenant_id");
            AuthzPolicy {
                id: format!("legacy-{i}"),
                priority: 0,
                enabled: true,
                effect,
                tenant: if raw_tenant == "*" || raw_tenant.is_empty() {
                    String::new()
                } else {
                    raw_tenant
                },
                project: String::new(),
                subject: s("service_identity"),
                role: String::new(),
                action: {
                    let op = s("operation");
                    if op == "*" { String::new() } else { op }
                },
                resource: {
                    let mt = s("message_type");
                    if mt == "*" { String::new() } else { mt }
                },
                purpose: {
                    let p = s("purpose");
                    if p == "*" { String::new() } else { p }
                },
                relationship: String::new(),
                conditions: BTreeMap::new(),
                required_scopes,
            }
        })
        .collect();
    let _ = tenant;
    PolicyDocument {
        policies,
        role_bindings: Vec::new(),
        tuples: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::governance::{SCOPE_AUTHZ_ADMIN, SCOPE_POLICY_READ};
    use super::*;
    use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::authz::AuthzSnapshot;
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Request, Status};

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "authz");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn actor_with_scope(scope: &str) -> authz_pb::GovernanceActor {
        authz_pb::GovernanceActor {
            subject: "policy-operator".to_string(),
            tenant_id: "tenant-a".to_string(),
            scopes: vec![scope.to_string()],
            ..Default::default()
        }
    }

    fn svc() -> AuthzServiceImpl {
        AuthzServiceImpl::new(AuthzSnapshot::default())
    }

    #[test]
    fn governance_sim_internal_status_carries_typed_detail() {
        let status = governance_sim_internal_status(
            "list_policy_versions",
            "list policy versions failed: pool closed",
        );
        assert_internal_detail(
            &status,
            "list_policy_versions",
            "list policy versions failed: pool closed",
        );
    }

    #[tokio::test]
    async fn explain_policy_missing_test_case_carries_field_violation() {
        let err = svc()
            .explain_policy(Request::new(authz_pb::ExplainPolicyRequest {
                actor: Some(actor_with_scope(SCOPE_POLICY_READ)),
                tenant_id: "tenant-a".to_string(),
                test_case: None,
                ..Default::default()
            }))
            .await
            .expect_err("missing test_case must fail before decision evaluation");

        assert_eq!(err.message(), "test_case is required");
        assert_validation_field(
            &err,
            "test_case",
            "must include a simulation case to explain",
        );
    }

    #[tokio::test]
    async fn seed_builtin_roles_missing_tenant_id_carries_field_violation() {
        let err = svc()
            .seed_builtin_roles(Request::new(authz_pb::SeedBuiltinRolesRequest {
                actor: Some(actor_with_scope(SCOPE_AUTHZ_ADMIN)),
                tenant_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing tenant_id must fail before Postgres availability");

        assert_eq!(err.message(), "tenant_id is required");
        assert_validation_field(&err, "tenant_id", "must be a non-empty tenant id");
    }
}
