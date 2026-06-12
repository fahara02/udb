//! ReBAC tuples: role bindings (grouping `g` links) and relationship tuples.

use super::*;

fn tuple_scope_tenant(tenant: &str, project: &str) -> String {
    if !tenant.trim().is_empty() {
        tenant.to_string()
    } else {
        project.to_string()
    }
}

fn policy_tuple_record(
    tuple_kind: &str,
    subject: &str,
    domain: &str,
    object: &str,
    action: &str,
    effect: &str,
    condition: &str,
    tenant_id: &str,
    project_id: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert(
        "tuple_kind".to_string(),
        LogicalValue::String(tuple_kind.to_string()),
    );
    record.insert(
        "subject".to_string(),
        LogicalValue::String(subject.to_string()),
    );
    record.insert(
        "domain".to_string(),
        LogicalValue::String(domain.to_string()),
    );
    record.insert(
        "object".to_string(),
        LogicalValue::String(object.to_string()),
    );
    record.insert(
        "action".to_string(),
        LogicalValue::String(action.to_string()),
    );
    record.insert(
        "effect".to_string(),
        LogicalValue::String(effect.to_string()),
    );
    record.insert(
        "condition".to_string(),
        LogicalValue::String(condition.to_string()),
    );
    record.insert(
        "tenant_id".to_string(),
        LogicalValue::String(tenant_id.to_string()),
    );
    record.insert(
        "project_id".to_string(),
        LogicalValue::String(project_id.to_string()),
    );
    record
}

fn policy_tuple_conflict() -> ConflictStrategy {
    ConflictStrategy::update_on(
        vec![
            "condition".to_string(),
            "tenant_id".to_string(),
            "project_id".to_string(),
        ],
        vec![
            "tuple_kind".to_string(),
            "subject".to_string(),
            "domain".to_string(),
            "object".to_string(),
            "action".to_string(),
            "effect".to_string(),
        ],
    )
}

impl AuthzServiceImpl {
    pub(super) async fn put_role_binding_impl(
        &self,
        request: Request<authz_pb::PutRoleBindingRequest>,
    ) -> Result<Response<authz_pb::AuthMutationResponse>, Status> {
        // K2.2: governed mode disables direct active mutation (use the draft flow).
        if super::governance::governed_mode_enabled() {
            return Err(Status::failed_precondition(
                "governed mode: direct PutRoleBinding is disabled; create a policy draft and activate it (or use break-glass governance)",
            ));
        }
        let binding = request
            .into_inner()
            .binding
            .ok_or_else(|| Status::invalid_argument("binding is required"))?;
        if binding.subject.trim().is_empty() || binding.role.trim().is_empty() {
            return Err(Status::invalid_argument(
                "binding subject and role are required",
            ));
        }
        let scope_tenant = tuple_scope_tenant(&binding.tenant, &binding.project);
        if scope_tenant.trim().is_empty() {
            return Err(Status::invalid_argument(
                "binding tenant or project is required",
            ));
        }
        let condition = serde_json::json!({
            "source": binding.source,
            "expires_at_unix": binding.expires_at_unix,
        })
        .to_string();
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition("native authz requires runtime-backed tuple persistence")
        })?;
        let context = crate::RequestContext {
            tenant_id: scope_tenant.clone(),
            project_id: binding.project.clone(),
            ..crate::RequestContext::default()
        };
        runtime
            .native_entity_write_for_service(
                "authz",
                &context,
                "udb.core.authz.entity.v1.PolicyTuple",
                policy_tuple_record(
                    "grouping",
                    &binding.subject,
                    &scope_tenant,
                    "",
                    &binding.role,
                    "",
                    &condition,
                    &scope_tenant,
                    &binding.project,
                ),
                policy_tuple_conflict(),
            )
            .await
            .map_err(|err| Status::internal(format!("store role binding failed: {err}")))?;
        let _ = self
            .bump_authz_revision(
                &scope_tenant,
                &binding.project,
                authz_entity_pb::AuthzChangeType::RoleAssignment,
                "role-binding-put",
                &binding.source,
            )
            .await;
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::AuthMutationResponse {
            ok: true,
            message: "role binding stored".to_string(),
        }))
    }

    pub(super) async fn put_relationship_impl(
        &self,
        request: Request<authz_pb::PutRelationshipRequest>,
    ) -> Result<Response<authz_pb::AuthMutationResponse>, Status> {
        // K2.2: governed mode disables direct active mutation (use the draft flow).
        if super::governance::governed_mode_enabled() {
            return Err(Status::failed_precondition(
                "governed mode: direct PutRelationship is disabled; create a policy draft and activate it (or use break-glass governance)",
            ));
        }
        let tuple = request
            .into_inner()
            .tuple
            .ok_or_else(|| Status::invalid_argument("tuple is required"))?;
        if tuple.subject.trim().is_empty()
            || tuple.relation.trim().is_empty()
            || tuple.object.trim().is_empty()
        {
            return Err(Status::invalid_argument(
                "tuple subject, relation, and object are required",
            ));
        }
        let scope_tenant = tuple_scope_tenant(&tuple.tenant, &tuple.project);
        if scope_tenant.trim().is_empty() {
            return Err(Status::invalid_argument(
                "tuple tenant or project is required",
            ));
        }
        let condition = serde_json::json!({
            "source": tuple.source,
            "version": tuple.version,
            "expires_at_unix": tuple.expires_at_unix,
        })
        .to_string();
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            Status::failed_precondition("native authz requires runtime-backed tuple persistence")
        })?;
        let context = crate::RequestContext {
            tenant_id: scope_tenant.clone(),
            project_id: tuple.project.clone(),
            ..crate::RequestContext::default()
        };
        runtime
            .native_entity_write_for_service(
                "authz",
                &context,
                "udb.core.authz.entity.v1.PolicyTuple",
                policy_tuple_record(
                    "relationship",
                    &tuple.subject,
                    &scope_tenant,
                    &tuple.object,
                    &tuple.relation,
                    "",
                    &condition,
                    &scope_tenant,
                    &tuple.project,
                ),
                policy_tuple_conflict(),
            )
            .await
            .map_err(|err| Status::internal(format!("store relationship tuple failed: {err}")))?;
        let _ = self
            .bump_authz_revision(
                &scope_tenant,
                &tuple.project,
                authz_entity_pb::AuthzChangeType::Relationship,
                "relationship-put",
                &tuple.source,
            )
            .await;
        self.invalidate_snapshot_cache();
        // Phase L2/L3 task5: publish the relationship-tuple change so security
        // dashboards and the audit plane observe ReBAC edits (no raw condition
        // material in the body — the outbox sink scrubs credential-shaped keys).
        self.emit_event(
            AuthEvent::new(
                topics::RELATIONSHIP_TUPLE_CHANGED,
                format!("{}:{}:{}", tuple.subject, tuple.relation, tuple.object),
                scope_tenant.clone(),
                serde_json::json!({
                    "subject": tuple.subject,
                    "relation": tuple.relation,
                    "object": tuple.object,
                    "tenant_id": scope_tenant,
                    "project_id": tuple.project,
                    "verb": "upsert",
                }),
            )
            .with_compliance(events::ComplianceEnvelope {
                actor: tuple.subject.clone(),
                target_resource: tuple.object.clone(),
                actor_project: tuple.project.clone(),
                operation: "upsert".to_string(),
                outcome: "success".to_string(),
                reason_code: "relationship_tuple_upsert".to_string(),
                ..Default::default()
            })
            .with_correlation(format!("reltuple:{}:{}", tuple.subject, tuple.object)),
        )
        .await;
        Ok(Response::new(authz_pb::AuthMutationResponse {
            ok: true,
            message: "relationship tuple stored".to_string(),
        }))
    }
}
