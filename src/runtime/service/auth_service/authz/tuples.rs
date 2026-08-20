//! ReBAC tuples: role bindings (grouping `g` links) and relationship tuples.

use super::*;

fn tuple_scope_tenant(tenant: &str, project: &str) -> String {
    if !tenant.trim().is_empty() {
        tenant.to_string()
    } else {
        project.to_string()
    }
}

fn authz_tuple_invalid_fields<const N: usize>(
    message: &'static str,
    fields: [(&'static str, &'static str); N],
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, fields)
}

fn authz_tuple_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authz", operation, message)
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
            return Err(governed_direct_mutation_status(
                "PutRoleBinding",
                "put_role_binding_disabled",
            ));
        }
        let binding = request.into_inner().binding.ok_or_else(|| {
            authz_tuple_invalid_fields(
                "binding is required",
                [("binding", "must include a role binding")],
            )
        })?;
        if binding.subject.trim().is_empty() || binding.role.trim().is_empty() {
            return Err(authz_tuple_invalid_fields(
                "binding subject and role are required",
                [
                    ("binding.subject", "must be a non-empty binding subject"),
                    ("binding.role", "must be a non-empty role"),
                ],
            ));
        }
        if is_reserved_platform_role(&binding.role) {
            return Err(reserved_platform_role_status(
                "put_role_binding",
                "reserved platform roles require a trusted system-role assignment and cannot be granted by a literal tuple",
            ));
        }
        let scope_tenant = tuple_scope_tenant(&binding.tenant, &binding.project);
        if scope_tenant.trim().is_empty() {
            return Err(authz_tuple_invalid_fields(
                "binding tenant or project is required",
                [
                    (
                        "binding.tenant",
                        "must include tenant or project scope for the binding",
                    ),
                    (
                        "binding.project",
                        "must include tenant or project scope for the binding",
                    ),
                ],
            ));
        }
        // Bind the BINDING's scope to the caller's claim. `tuple_scope_tenant`
        // substitutes the project for an empty tenant, which collapses two
        // different security dimensions, so the tenant and project are checked as
        // they were supplied rather than after that fallback.
        super::enforce_authz_body_scope(&binding.tenant, &binding.project)?;
        let condition = serde_json::json!({
            "source": binding.source,
            "expires_at_unix": binding.expires_at_unix,
        })
        .to_string();
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            authz_capability_status(
                "tuple_persistence",
                "runtime_native_entity_dispatch",
                "native authz requires runtime-backed tuple persistence",
            )
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
            .map_err(|err| {
                authz_tuple_internal_status(
                    "store_role_binding",
                    format!("store role binding failed: {err}"),
                )
            })?;
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
            return Err(governed_direct_mutation_status(
                "PutRelationship",
                "put_relationship_disabled",
            ));
        }
        let tuple = request.into_inner().tuple.ok_or_else(|| {
            authz_tuple_invalid_fields(
                "tuple is required",
                [("tuple", "must include a relationship tuple")],
            )
        })?;
        if tuple.subject.trim().is_empty()
            || tuple.relation.trim().is_empty()
            || tuple.object.trim().is_empty()
        {
            return Err(authz_tuple_invalid_fields(
                "tuple subject, relation, and object are required",
                [
                    ("tuple.subject", "must be a non-empty tuple subject"),
                    ("tuple.relation", "must be a non-empty tuple relation"),
                    ("tuple.object", "must be a non-empty tuple object"),
                ],
            ));
        }
        let scope_tenant = tuple_scope_tenant(&tuple.tenant, &tuple.project);
        if scope_tenant.trim().is_empty() {
            return Err(authz_tuple_invalid_fields(
                "tuple tenant or project is required",
                [
                    (
                        "tuple.tenant",
                        "must include tenant or project scope for the tuple",
                    ),
                    (
                        "tuple.project",
                        "must include tenant or project scope for the tuple",
                    ),
                ],
            ));
        }
        // Bind the TUPLE's scope to the caller's claim: a relationship tuple is a
        // grant, so writing one into another tenant grants access there.
        super::enforce_authz_body_scope(&tuple.tenant, &tuple.project)?;
        let condition = serde_json::json!({
            "source": tuple.source,
            "version": tuple.version,
            "expires_at_unix": tuple.expires_at_unix,
        })
        .to_string();
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            authz_capability_status(
                "tuple_persistence",
                "runtime_native_entity_dispatch",
                "native authz requires runtime-backed tuple persistence",
            )
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
            .map_err(|err| {
                authz_tuple_internal_status(
                    "store_relationship_tuple",
                    format!("store relationship tuple failed: {err}"),
                )
            })?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::authz::AuthzSnapshot;
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Request};

    fn svc() -> AuthzServiceImpl {
        AuthzServiceImpl::new(AuthzSnapshot::default())
    }

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_validation_fields(status: &Status, expected: &[(&str, &str)]) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), expected.len());
        for (actual, (field, description)) in detail.field_violations.iter().zip(expected) {
            assert_eq!(actual.field, *field);
            assert_eq!(actual.description, *description);
        }
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

    #[test]
    fn authz_tuple_internal_status_carries_typed_detail() {
        let status = authz_tuple_internal_status(
            "store_relationship_tuple",
            "store relationship tuple failed: dispatch down",
        );
        assert_internal_detail(
            &status,
            "store_relationship_tuple",
            "store relationship tuple failed: dispatch down",
        );
    }

    #[tokio::test]
    async fn put_role_binding_missing_binding_carries_field_violation() {
        let err = svc()
            .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest::default()))
            .await
            .expect_err("missing binding must fail before runtime access");

        assert_eq!(err.message(), "binding is required");
        assert_validation_fields(&err, &[("binding", "must include a role binding")]);
    }

    #[tokio::test]
    async fn put_role_binding_missing_identity_carries_field_violations() {
        let err = svc()
            .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest {
                binding: Some(authz_pb::RoleBinding {
                    tenant: "tenant-1".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("missing binding identity must fail before runtime access");

        assert_eq!(err.message(), "binding subject and role are required");
        assert_validation_fields(
            &err,
            &[
                ("binding.subject", "must be a non-empty binding subject"),
                ("binding.role", "must be a non-empty role"),
            ],
        );
    }

    #[tokio::test]
    async fn put_role_binding_missing_scope_carries_field_violations() {
        let err = svc()
            .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest {
                binding: Some(authz_pb::RoleBinding {
                    subject: "user-1".to_string(),
                    role: "reader".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("missing binding scope must fail before runtime access");

        assert_eq!(err.message(), "binding tenant or project is required");
        assert_validation_fields(
            &err,
            &[
                (
                    "binding.tenant",
                    "must include tenant or project scope for the binding",
                ),
                (
                    "binding.project",
                    "must include tenant or project scope for the binding",
                ),
            ],
        );
    }

    #[tokio::test]
    async fn put_role_binding_rejects_reserved_platform_authority() {
        let err = svc()
            .put_role_binding(Request::new(authz_pb::PutRoleBindingRequest {
                binding: Some(authz_pb::RoleBinding {
                    subject: "user-1".to_string(),
                    role: "platform_admin".to_string(),
                    tenant: "tenant-1".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("a literal tuple must not mint platform authority");

        assert_eq!(err.code(), Code::PermissionDenied);
        assert_eq!(
            err.message(),
            "reserved platform roles require a trusted system-role assignment and cannot be granted by a literal tuple"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "put_role_binding");
        assert_eq!(
            detail.policy_decision_id,
            "reserved_platform_role_authority_required"
        );
    }

    #[tokio::test]
    async fn put_relationship_missing_tuple_carries_field_violation() {
        let err = svc()
            .put_relationship(Request::new(authz_pb::PutRelationshipRequest::default()))
            .await
            .expect_err("missing tuple must fail before runtime access");

        assert_eq!(err.message(), "tuple is required");
        assert_validation_fields(&err, &[("tuple", "must include a relationship tuple")]);
    }

    #[tokio::test]
    async fn put_relationship_missing_identity_carries_field_violations() {
        let err = svc()
            .put_relationship(Request::new(authz_pb::PutRelationshipRequest {
                tuple: Some(authz_pb::RelationshipTuple {
                    tenant: "tenant-1".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("missing tuple identity must fail before runtime access");

        assert_eq!(
            err.message(),
            "tuple subject, relation, and object are required"
        );
        assert_validation_fields(
            &err,
            &[
                ("tuple.subject", "must be a non-empty tuple subject"),
                ("tuple.relation", "must be a non-empty tuple relation"),
                ("tuple.object", "must be a non-empty tuple object"),
            ],
        );
    }

    #[tokio::test]
    async fn put_relationship_missing_scope_carries_field_violations() {
        let err = svc()
            .put_relationship(Request::new(authz_pb::PutRelationshipRequest {
                tuple: Some(authz_pb::RelationshipTuple {
                    subject: "user-1".to_string(),
                    relation: "viewer".to_string(),
                    object: "doc-1".to_string(),
                    ..Default::default()
                }),
            }))
            .await
            .expect_err("missing tuple scope must fail before runtime access");

        assert_eq!(err.message(), "tuple tenant or project is required");
        assert_validation_fields(
            &err,
            &[
                (
                    "tuple.tenant",
                    "must include tenant or project scope for the tuple",
                ),
                (
                    "tuple.project",
                    "must include tenant or project scope for the tuple",
                ),
            ],
        );
    }
}
