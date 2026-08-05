//! Phase K draft/version/approval workflow handlers.
//!
//! Drafts are stored in `policy_drafts`; approvals in `policy_approvals`; an
//! approved draft is promoted into an immutable `policy_versions` row. None of
//! these touch the live authorization snapshot — only `activate`/`rollback`
//! (in `governance_activate.rs`) do.

use super::governance::{SCOPE_POLICY_APPROVE, SCOPE_POLICY_WRITE};
use super::governance_logic::{
    self as glogic, DRAFT_APPROVED, DRAFT_IN_REVIEW, DRAFT_OPEN, DRAFT_REJECTED, PolicyDocument,
};
use super::*;
use crate::proto::udb::core::authz::services::v1 as authz_pb;

fn governance_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn governance_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn governance_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

fn governance_draft_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status("authz", operation, message)
}

fn draft_not_editable_status(status: &str) -> Status {
    governance_policy_status(
        "policy_draft_update",
        "draft_not_editable",
        format!("draft in status {status} is not editable"),
    )
}

fn draft_not_editable_static_status() -> Status {
    governance_policy_status(
        "policy_draft_update",
        "draft_not_editable",
        "draft is not editable",
    )
}

fn draft_not_submittable_status(status: &str) -> Status {
    governance_policy_status(
        "policy_draft_submit",
        "draft_not_submittable",
        format!("draft in status {status} cannot be submitted"),
    )
}

fn draft_not_reviewable_status(status: &str) -> Status {
    governance_policy_status(
        "policy_draft_review",
        "draft_not_reviewable",
        format!("draft in status {status} is not awaiting review"),
    )
}

fn draft_separation_of_duties_status() -> Status {
    governance_policy_status_with_code(
        tonic::Code::PermissionDenied,
        "policy_draft_review",
        "separation_of_duties",
        "separation of duties: the author of a high-risk draft cannot approve it",
    )
}

impl AuthzServiceImpl {
    /// Resolve (or create) a `PolicySet` for a tenant/project/name, returning its
    /// id. Idempotent on (tenant, project, name).
    pub(super) async fn ensure_policy_set(
        &self,
        tenant: &str,
        project: &str,
        name: &str,
        created_by: &str,
    ) -> Result<String, Status> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            authz_capability_status(
                "policy_set_persistence",
                "runtime_native_entity_dispatch",
                "native authz requires runtime-backed policy-set persistence",
            )
        })?;
        let name = if name.trim().is_empty() {
            "default"
        } else {
            name.trim()
        };
        // P6.10 Wave 4: typed upsert on the alternate-unique (tenant_id,project_id,name)
        // [manifest idx uq_policy_sets_tenant_project_name] + RETURNING policy_set_id.
        // policy_set_id is server-generated in Rust (was gen_random_uuid()).
        let mut record = LogicalRecord::new();
        record.insert(
            "policy_set_id".to_string(),
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
        record.insert("name".to_string(), LogicalValue::String(name.to_string()));
        record.insert(
            "created_by".to_string(),
            LogicalValue::String(created_by.to_string()),
        );
        let context = crate::RequestContext {
            tenant_id: tenant.to_string(),
            project_id: project.to_string(),
            ..crate::RequestContext::default()
        };
        let returned = runtime
            .native_entity_write_for_service_returning(
                "authz",
                &context,
                "udb.core.authz.entity.v1.PolicySet",
                record,
                ConflictStrategy::update_on(
                    vec!["name".to_string()],
                    vec![
                        "tenant_id".to_string(),
                        "project_id".to_string(),
                        "name".to_string(),
                    ],
                ),
                vec!["policy_set_id".to_string()],
            )
            .await
            .map_err(|err| {
                governance_draft_internal_status(
                    "ensure_policy_set",
                    format!("ensure policy set failed: {err}"),
                )
            })?;
        returned
            .first()
            .and_then(|r| r.get("policy_set_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| {
                governance_draft_internal_status(
                    "ensure_policy_set",
                    "ensure policy set returned no id",
                )
            })
    }

    pub(super) async fn create_policy_draft_impl(
        &self,
        request: Request<authz_pb::CreatePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "CreatePolicyDraft",
                "governance.draft.create",
                &[SCOPE_POLICY_WRITE],
                now,
            )
            .await?;
        let tenant = req.tenant_id.trim().to_string();
        if tenant.is_empty() {
            return Err(governance_required_field(
                "tenant_id",
                "must be a non-empty tenant id",
                "tenant_id is required",
            ));
        }
        let project = req.project_id.clone();
        // A6: bind the body tenant/project to the VALIDATED claim BEFORE any DB
        // write — a tenant-A `authz:policy:write` caller must not create a draft /
        // policy-set under tenant B. Cross-tenant admins still pass; no-op
        // in-process. (Mirrors the read guards in authz/mod.rs.)
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &tenant,
            &project,
        )?;

        // Build the initial document: explicit document, or a branch of the
        // active snapshot when branch_from_active is set.
        let mut document = req
            .document
            .as_ref()
            .map(PolicyDocument::from_proto)
            .unwrap_or_default();
        if req.branch_from_active && document.policies.is_empty() {
            let snap = self.current_snapshot().await?;
            document = PolicyDocument::from_snapshot(&snap, &tenant, &project);
        }

        let policy_set_id = self
            .ensure_policy_set(&tenant, &project, &req.policy_set_name, &actor)
            .await?;
        let pool = self.require_pool()?;
        let m = self.policy_drafts_model();
        let rel = m.relation.clone();
        let draft_id = Uuid::new_v4().to_string();
        let title = if req.title.trim().is_empty() {
            "policy draft".to_string()
        } else {
            req.title.clone()
        };
        let policies_json = serde_json::to_string(
            &document
                .policies
                .iter()
                .map(glogic::policy_to_json)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());
        let tuples_json = serde_json::to_string(&document.to_json()["relationship_tuples"])
            .unwrap_or_else(|_| "[]".to_string());

        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({draft_id}, {tenant_id}, {project_id}, {title}, {description}, {proposed_policies_json}, {proposed_tuples_json}, {status}, {author}, {high_risk}) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            draft_id = m.q("draft_id"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            title = m.q("title"),
            description = m.q("description"),
            proposed_policies_json = m.q("proposed_policies_json"),
            proposed_tuples_json = m.q("proposed_tuples_json"),
            status = m.q("status"),
            author = m.q("author"),
            high_risk = m.q("high_risk"),
        ))
        .bind(&draft_id)
        .bind(&tenant)
        .bind(&project)
        .bind(&title)
        .bind(&req.change_reason)
        .bind(&policies_json)
        .bind(&tuples_json)
        .bind(DRAFT_OPEN)
        .bind(&actor)
        .bind(req.high_risk)
        .execute(pool)
        .await
        .map_err(|err| {
            governance_draft_internal_status(
                "create_policy_draft",
                format!("create policy draft failed: {err}"),
            )
        })?;

        // Store the full document (incl. role bindings) on the draft payload via a
        // dedicated frozen version slot is deferred to promotion; the draft itself
        // carries policies + tuples. Persist the whole document as the draft's
        // proposed_policies payload extension if role bindings are present.
        self.persist_draft_document(&draft_id, &document).await?;

        self.emit_event(AuthEvent::new(
            topics::POLICY_DRAFT_CREATED,
            draft_id.clone(),
            tenant.clone(),
            serde_json::json!({
                "draft_id": draft_id,
                "tenant_id": tenant,
                "project_id": project,
                "policy_set_id": policy_set_id,
                "author": actor,
                "high_risk": req.high_risk,
            }),
        ))
        .await;

        let draft = self.load_draft(&draft_id).await?;
        Ok(Response::new(authz_pb::PolicyDraftResponse {
            draft: Some(draft),
            policy_set: self.load_policy_set(&policy_set_id).await.ok().flatten(),
            document: Some(document_to_proto(&document)),
        }))
    }

    pub(super) async fn update_policy_draft_impl(
        &self,
        request: Request<authz_pb::UpdatePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "UpdatePolicyDraft",
                "governance.draft.update",
                &[SCOPE_POLICY_WRITE],
                now,
            )
            .await?;
        if req.draft_id.trim().is_empty() {
            return Err(governance_required_field(
                "draft_id",
                "must be a non-empty policy draft id",
                "draft_id is required",
            ));
        }
        let draft = self.load_draft(&req.draft_id).await?;
        if !glogic::draft_editable(&draft.status) {
            return Err(draft_not_editable_status(&draft.status));
        }
        // Optimistic concurrency over the draft's updated_at epoch.
        let cur_updated = draft.updated_at.as_ref().map(|t| t.seconds).unwrap_or(0);
        if req.expected_updated_at_unix != 0 && req.expected_updated_at_unix != cur_updated {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "draft expected updated_at",
                0,
                "draft was modified concurrently (expected_updated_at_unix mismatch)",
            ));
        }
        let document = req
            .document
            .as_ref()
            .map(PolicyDocument::from_proto)
            .unwrap_or_default();
        let pool = self.require_pool()?;
        let m = self.policy_drafts_model();
        let rel = m.relation.clone();
        let policies_json = serde_json::to_string(
            &document
                .policies
                .iter()
                .map(glogic::policy_to_json)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_string());
        let tuples_json = serde_json::to_string(&document.to_json()["relationship_tuples"])
            .unwrap_or_else(|_| "[]".to_string());
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {proposed_policies_json} = $2, {proposed_tuples_json} = $3, \
               {title} = COALESCE(NULLIF($4, ''), {title}), \
               {description} = COALESCE(NULLIF($5, ''), {description}), \
               {high_risk} = $6, {updated_at} = NOW() \
             WHERE {draft_id} = $1::UUID AND {status} IN ('OPEN', 'REJECTED')",
            proposed_policies_json = m.q("proposed_policies_json"),
            proposed_tuples_json = m.q("proposed_tuples_json"),
            title = m.q("title"),
            description = m.q("description"),
            high_risk = m.q("high_risk"),
            updated_at = m.q("updated_at"),
            draft_id = m.q("draft_id"),
            status = m.q("status"),
        ))
        .bind(&req.draft_id)
        .bind(&policies_json)
        .bind(&tuples_json)
        .bind(&req.title)
        .bind(&req.change_reason)
        .bind(req.high_risk)
        .execute(pool)
        .await
        .map_err(|err| {
            governance_draft_internal_status(
                "update_policy_draft",
                format!("update policy draft failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(draft_not_editable_static_status());
        }
        self.persist_draft_document(&req.draft_id, &document)
            .await?;
        self.emit_event(AuthEvent::new(
            topics::POLICY_DRAFT_UPDATED,
            req.draft_id.clone(),
            draft.tenant_id.clone(),
            serde_json::json!({ "draft_id": req.draft_id, "updated_by": actor }),
        ))
        .await;
        let updated = self.load_draft(&req.draft_id).await?;
        Ok(Response::new(authz_pb::PolicyDraftResponse {
            draft: Some(updated),
            policy_set: None,
            document: Some(document_to_proto(&document)),
        }))
    }

    pub(super) async fn diff_policy_draft_impl(
        &self,
        request: Request<authz_pb::DiffPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::DiffPolicyDraftResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        self.authorize_governance(
            req.actor.as_ref(),
            "DiffPolicyDraft",
            "governance.draft.diff",
            &[super::governance::SCOPE_POLICY_READ],
            now,
        )
        .await?;
        if req.draft_id.trim().is_empty() {
            return Err(governance_required_field(
                "draft_id",
                "must be a non-empty policy draft id",
                "draft_id is required",
            ));
        }
        let draft = self.load_draft(&req.draft_id).await?;
        let after = self.load_draft_document(&req.draft_id).await?;
        // Base = the named against_version document, else the active snapshot
        // scoped to the draft's tenant/project.
        let before = if req.against_version_id.trim().is_empty() {
            let snap = self.current_snapshot().await?;
            PolicyDocument::from_snapshot(&snap, &draft.tenant_id, &draft.project_id)
        } else {
            self.load_version_document(&req.against_version_id).await?
        };
        let entries = glogic::diff_documents(&before, &after);
        let diff_json = glogic::diff_to_json(&entries).to_string();
        Ok(Response::new(authz_pb::DiffPolicyDraftResponse {
            entries,
            diff_json,
        }))
    }

    pub(super) async fn submit_policy_draft_impl(
        &self,
        request: Request<authz_pb::SubmitPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "SubmitPolicyDraft",
                "governance.draft.submit",
                &[SCOPE_POLICY_WRITE],
                now,
            )
            .await?;
        if req.draft_id.trim().is_empty() {
            return Err(governance_required_field(
                "draft_id",
                "must be a non-empty policy draft id",
                "draft_id is required",
            ));
        }
        let draft = self.load_draft(&req.draft_id).await?;
        if !glogic::draft_submittable(&draft.status) {
            return Err(draft_not_submittable_status(&draft.status));
        }
        let pool = self.require_pool()?;
        let m = self.policy_drafts_model();
        let rel = m.relation.clone();
        sqlx::query(&format!(
            "UPDATE {rel} SET {status} = '{in_review}', {updated_at} = NOW() WHERE {draft_id} = $1::UUID",
            status = m.q("status"),
            in_review = DRAFT_IN_REVIEW,
            updated_at = m.q("updated_at"),
            draft_id = m.q("draft_id"),
        ))
        .bind(&req.draft_id)
        .execute(pool)
        .await
        .map_err(|err| {
            governance_draft_internal_status(
                "submit_policy_draft",
                format!("submit policy draft failed: {err}"),
            )
        })?;
        self.emit_event(AuthEvent::new(
            topics::POLICY_DRAFT_SUBMITTED,
            req.draft_id.clone(),
            draft.tenant_id.clone(),
            serde_json::json!({ "draft_id": req.draft_id, "submitted_by": actor }),
        ))
        .await;
        Ok(Response::new(authz_pb::PolicyDraftResponse {
            draft: Some(self.load_draft(&req.draft_id).await?),
            policy_set: None,
            document: None,
        }))
    }

    pub(super) async fn approve_policy_draft_impl(
        &self,
        request: Request<authz_pb::ApprovePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyApprovalResponse>, Status> {
        self.decide_policy_draft(request.into_inner(), true).await
    }

    pub(super) async fn reject_policy_draft_impl(
        &self,
        request: Request<authz_pb::RejectPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyApprovalResponse>, Status> {
        let r = request.into_inner();
        // Reuse the approve path with approve=false by repacking into the approve
        // request shape (same fields).
        let approve = authz_pb::ApprovePolicyDraftRequest {
            actor: r.actor,
            draft_id: r.draft_id,
            reviewer: r.reviewer,
            reason: r.reason,
        };
        self.decide_policy_draft(approve, false).await
    }

    async fn decide_policy_draft(
        &self,
        req: authz_pb::ApprovePolicyDraftRequest,
        approve: bool,
    ) -> Result<Response<authz_pb::PolicyApprovalResponse>, Status> {
        let now = now_unix() as i64;
        let rpc = if approve {
            "ApprovePolicyDraft"
        } else {
            "RejectPolicyDraft"
        };
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                rpc,
                if approve {
                    "governance.draft.approve"
                } else {
                    "governance.draft.reject"
                },
                &[SCOPE_POLICY_APPROVE],
                now,
            )
            .await?;
        if req.draft_id.trim().is_empty() {
            return Err(governance_required_field(
                "draft_id",
                "must be a non-empty policy draft id",
                "draft_id is required",
            ));
        }
        // Separation of duties: derive the approving reviewer from the
        // claim-derived actor (claim subject) so a single bearer cannot
        // author-as-X then approve-as-Y. The body `reviewer` is only honored
        // in the in-process fallback where no validated claim context exists.
        let reviewer = if crate::runtime::service::method_security::claim_context_present() {
            crate::runtime::service::method_security::current_claim_context().subject
        } else if req.reviewer.trim().is_empty() {
            actor.clone()
        } else {
            req.reviewer.clone()
        };
        if req.reason.trim().is_empty() {
            return Err(governance_required_field(
                "reason",
                "must be a non-empty approval decision reason",
                "a reason is required for an approval decision",
            ));
        }
        let draft = self.load_draft(&req.draft_id).await?;
        if !glogic::draft_reviewable(&draft.status) {
            return Err(draft_not_reviewable_status(&draft.status));
        }
        // Separation of duties (K2.2): the author of a high-risk draft cannot
        // approve it.
        if approve && !glogic::approval_allowed(draft.high_risk, &draft.author, &reviewer) {
            return Err(draft_separation_of_duties_status());
        }

        let pool = self.require_pool()?;
        // Record the approval decision.
        let am = self.policy_approvals_model();
        let arel = am.relation.clone();
        let decision = if approve { "APPROVE" } else { "REJECT" };
        let approval_id = Uuid::new_v4().to_string();
        sqlx::query(&format!(
            "INSERT INTO {arel} ({approval_id}, {draft_id}, {tenant_id}, {actor}, {role}, {decision}, {reason}) \
             VALUES ($1::UUID, $2::UUID, $3, $4, 'APPROVER', $5, $6)",
            approval_id = am.q("approval_id"),
            draft_id = am.q("draft_id"),
            tenant_id = am.q("tenant_id"),
            actor = am.q("actor"),
            role = am.q("role"),
            decision = am.q("decision"),
            reason = am.q("reason"),
        ))
        .bind(&approval_id)
        .bind(&req.draft_id)
        .bind(&draft.tenant_id)
        .bind(&reviewer)
        .bind(decision)
        .bind(&req.reason)
        .execute(pool)
        .await
        .map_err(|err| {
            governance_draft_internal_status(
                "record_policy_approval",
                format!("record approval failed: {err}"),
            )
        })?;

        let new_status = if approve {
            DRAFT_APPROVED
        } else {
            DRAFT_REJECTED
        };
        let dm = self.policy_drafts_model();
        let drel = dm.relation.clone();
        sqlx::query(&format!(
            "UPDATE {drel} SET {status} = $2, {updated_at} = NOW() WHERE {draft_id} = $1::UUID",
            status = dm.q("status"),
            updated_at = dm.q("updated_at"),
            draft_id = dm.q("draft_id"),
        ))
        .bind(&req.draft_id)
        .bind(new_status)
        .execute(pool)
        .await
        .map_err(|err| {
            governance_draft_internal_status(
                "update_draft_status",
                format!("update draft status failed: {err}"),
            )
        })?;

        // On approval, promote the draft into an immutable APPROVED PolicyVersion
        // ready for activation. Rejection leaves no version.
        let mut version = None;
        if approve {
            version = Some(self.promote_draft_to_version(&draft, &actor).await?);
        }

        self.emit_event(AuthEvent::new(
            if approve {
                topics::POLICY_DRAFT_APPROVED
            } else {
                topics::POLICY_DRAFT_REJECTED
            },
            req.draft_id.clone(),
            draft.tenant_id.clone(),
            serde_json::json!({
                "draft_id": req.draft_id,
                "reviewer": reviewer,
                "decision": decision,
                "reason": req.reason,
            }),
        ))
        .await;

        let approval = self.load_approval(&approval_id).await.ok().flatten();
        Ok(Response::new(authz_pb::PolicyApprovalResponse {
            draft: Some(self.load_draft(&req.draft_id).await?),
            approval,
            version,
        }))
    }
}

/// Convert a runtime document into the proto `PolicyDocument` for responses.
pub(super) fn document_to_proto(doc: &PolicyDocument) -> authz_pb::PolicyDocument {
    authz_pb::PolicyDocument {
        policies: doc
            .policies
            .iter()
            .map(|p| authz_pb::AuthzPolicyRecord {
                id: p.id.clone(),
                priority: p.priority,
                enabled: p.enabled,
                effect: p.effect.as_str().to_string(),
                tenant: p.tenant.clone(),
                project: p.project.clone(),
                subject: p.subject.clone(),
                role: p.role.clone(),
                action: p.action.clone(),
                resource: p.resource.clone(),
                purpose: p.purpose.clone(),
                relationship: p.relationship.clone(),
                conditions: p.conditions.clone().into_iter().collect(),
                required_scopes: p.required_scopes.clone(),
            })
            .collect(),
        role_bindings: doc
            .role_bindings
            .iter()
            .map(|b| authz_pb::RoleBinding {
                subject: b.subject.clone(),
                role: b.role.clone(),
                tenant: b.tenant.clone(),
                project: b.project.clone(),
                expires_at_unix: 0,
                source: "draft".to_string(),
            })
            .collect(),
        relationship_tuples: doc
            .tuples
            .iter()
            .map(|t| authz_pb::RelationshipTuple {
                subject: t.subject.clone(),
                relation: t.relation.clone(),
                object: t.object.clone(),
                tenant: t.tenant.clone(),
                project: t.project.clone(),
                version: 0,
                expires_at_unix: 0,
                source: "draft".to_string(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::governance::SCOPE_POLICY_READ;
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

    fn assert_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::FailedPrecondition);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_permission_policy_detail(
        status: &Status,
        operation: &str,
        policy_decision_id: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
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
            subject: "policy-author".to_string(),
            tenant_id: "tenant-a".to_string(),
            scopes: vec![scope.to_string()],
            ..Default::default()
        }
    }

    fn svc() -> AuthzServiceImpl {
        AuthzServiceImpl::new(AuthzSnapshot::default())
    }

    #[test]
    fn governance_draft_internal_status_carries_typed_detail() {
        let status = governance_draft_internal_status(
            "create_policy_draft",
            "create policy draft failed: store closed",
        );
        assert_internal_detail(
            &status,
            "create_policy_draft",
            "create policy draft failed: store closed",
        );
    }

    #[tokio::test]
    async fn update_policy_draft_missing_draft_id_carries_field_violation() {
        let err = svc()
            .update_policy_draft(Request::new(authz_pb::UpdatePolicyDraftRequest {
                actor: Some(actor_with_scope(SCOPE_POLICY_WRITE)),
                draft_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing draft_id must fail before draft loading");

        assert_eq!(err.message(), "draft_id is required");
        assert_validation_field(&err, "draft_id", "must be a non-empty policy draft id");
    }

    #[tokio::test]
    async fn diff_policy_draft_missing_draft_id_carries_field_violation() {
        let err = svc()
            .diff_policy_draft(Request::new(authz_pb::DiffPolicyDraftRequest {
                actor: Some(actor_with_scope(SCOPE_POLICY_READ)),
                draft_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing draft_id must fail before draft loading");

        assert_eq!(err.message(), "draft_id is required");
        assert_validation_field(&err, "draft_id", "must be a non-empty policy draft id");
    }

    #[tokio::test]
    async fn submit_policy_draft_missing_draft_id_carries_field_violation() {
        let err = svc()
            .submit_policy_draft(Request::new(authz_pb::SubmitPolicyDraftRequest {
                actor: Some(actor_with_scope(SCOPE_POLICY_WRITE)),
                draft_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing draft_id must fail before draft loading");

        assert_eq!(err.message(), "draft_id is required");
        assert_validation_field(&err, "draft_id", "must be a non-empty policy draft id");
    }

    #[tokio::test]
    async fn approve_policy_draft_missing_reason_carries_field_violation() {
        let err = svc()
            .approve_policy_draft(Request::new(authz_pb::ApprovePolicyDraftRequest {
                actor: Some(actor_with_scope(SCOPE_POLICY_APPROVE)),
                draft_id: "00000000-0000-0000-0000-000000000001".to_string(),
                reason: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing reason must fail before draft loading");

        assert_eq!(
            err.message(),
            "a reason is required for an approval decision"
        );
        assert_validation_field(
            &err,
            "reason",
            "must be a non-empty approval decision reason",
        );
    }

    #[test]
    fn policy_draft_lifecycle_denials_carry_policy_detail() {
        assert_policy_detail(
            &draft_not_editable_status(DRAFT_IN_REVIEW),
            "policy_draft_update",
            "draft_not_editable",
            "draft in status IN_REVIEW is not editable",
        );
        assert_policy_detail(
            &draft_not_editable_static_status(),
            "policy_draft_update",
            "draft_not_editable",
            "draft is not editable",
        );
        assert_policy_detail(
            &draft_not_submittable_status(DRAFT_APPROVED),
            "policy_draft_submit",
            "draft_not_submittable",
            "draft in status APPROVED cannot be submitted",
        );
        assert_policy_detail(
            &draft_not_reviewable_status(DRAFT_OPEN),
            "policy_draft_review",
            "draft_not_reviewable",
            "draft in status OPEN is not awaiting review",
        );
    }

    #[test]
    fn policy_draft_separation_of_duties_carries_permission_policy_detail() {
        assert_permission_policy_detail(
            &draft_separation_of_duties_status(),
            "policy_draft_review",
            "separation_of_duties",
            "separation of duties: the author of a high-risk draft cannot approve it",
        );
    }
}
