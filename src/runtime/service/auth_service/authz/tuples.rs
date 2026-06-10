//! ReBAC tuples: role bindings (grouping `g` links) and relationship tuples.

use super::*;

fn tuple_scope_tenant(tenant: &str, project: &str) -> String {
    if !tenant.trim().is_empty() {
        tenant.to_string()
    } else {
        project.to_string()
    }
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
        if let Some(pool) = &self.pg_pool {
            let tuple = self.relationship_tuples_model();
            let rel = tuple.relation.clone();
            sqlx::query(&format!(
                "INSERT INTO {rel} ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {tenant_id}, {project_id}) \
                 VALUES ('grouping', $1, $3, '', $2, '', $5, $3, $4) \
                 ON CONFLICT ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}) \
                 DO UPDATE SET {condition} = EXCLUDED.{condition}, {tenant_id} = EXCLUDED.{tenant_id}, {project_id} = EXCLUDED.{project_id}",
                tuple_kind = tuple.q("tuple_kind"),
                subject = tuple.q("subject"),
                domain_col = tuple.q("domain"),
                object_col = tuple.q("object"),
                action_col = tuple.q("action"),
                effect_col = tuple.q("effect"),
                condition = tuple.q("condition"),
                tenant_id = tuple.q("tenant_id"),
                project_id = tuple.q("project_id"),
            ))
            .bind(&binding.subject)
            .bind(&binding.role)
            .bind(&scope_tenant)
            .bind(&binding.project)
            .bind(&condition)
            .execute(pool)
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
        } else {
            self.require_snapshot_fallback()?;
        }
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
        if let Some(pool) = &self.pg_pool {
            let tuple_model = self.relationship_tuples_model();
            let rel = tuple_model.relation.clone();
            sqlx::query(&format!(
                "INSERT INTO {rel} ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {tenant_id}, {project_id}) \
                 VALUES ('relationship', $1, $4, $3, $2, '', $6, $4, $5) \
                 ON CONFLICT ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}) \
                 DO UPDATE SET {condition} = EXCLUDED.{condition}, {tenant_id} = EXCLUDED.{tenant_id}, {project_id} = EXCLUDED.{project_id}",
                tuple_kind = tuple_model.q("tuple_kind"),
                subject = tuple_model.q("subject"),
                domain_col = tuple_model.q("domain"),
                object_col = tuple_model.q("object"),
                action_col = tuple_model.q("action"),
                effect_col = tuple_model.q("effect"),
                condition = tuple_model.q("condition"),
                tenant_id = tuple_model.q("tenant_id"),
                project_id = tuple_model.q("project_id"),
            ))
            .bind(&tuple.subject)
            .bind(&tuple.relation)
            .bind(&tuple.object)
            .bind(&scope_tenant)
            .bind(&tuple.project)
            .bind(&condition)
            .execute(pool)
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
        } else {
            self.require_snapshot_fallback()?;
        }
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
