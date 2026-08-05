//! Phase K activation / rollback: the ONLY paths that mutate the live
//! authorization snapshot in governed mode. Applying a version replaces the
//! tenant/project-scoped policy / role-binding / relationship rows with the
//! version's frozen document, bumps the durable [`AuthzRevision`], invalidates
//! the local snapshot cache, and emits a durable cluster-invalidation event so
//! every node reloads immediately instead of waiting for the TTL (K2.4).

use super::governance_logic::PolicyDocument;
use super::governance_store::version_state_to_db;
use super::*;
use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;

fn activation_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn activation_field_violation(
    field: &'static str,
    description: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [(field.to_string(), description.into())],
    )
}

fn activation_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn activation_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("authz", operation, message)
}

fn policy_version_not_activatable_status(state: authz_entity_pb::PolicyVersionState) -> Status {
    activation_policy_status(
        "policy_version_activate",
        "policy_version_not_activatable",
        format!(
            "only an approved (or previously-active) version may be activated; version is {state:?}"
        ),
    )
}

fn rollback_target_required_status() -> Status {
    activation_policy_status(
        "policy_version_rollback",
        "rollback_target_required",
        "no rollback target: supply target_version_id or activate a second version first",
    )
}

fn policy_version_not_canariable_status(state: authz_entity_pb::PolicyVersionState) -> Status {
    activation_policy_status(
        "policy_canary_activate",
        "policy_version_not_canariable",
        format!(
            "only an approved (or previously-active) version may be canaried; version is {state:?}"
        ),
    )
}

fn canary_not_active_status(state: authz_entity_pb::CanaryState) -> Status {
    activation_policy_status(
        "policy_canary_promote",
        "canary_not_active",
        format!("only an ACTIVE canary can be promoted; canary is {state:?}"),
    )
}

fn canary_not_promote_eligible_status() -> Status {
    activation_policy_status(
        "policy_canary_promote",
        "canary_not_promote_eligible",
        "canary is not promote-eligible yet: its success window has not elapsed",
    )
}

fn validate_unique_document_policy_ids(document: &PolicyDocument) -> Result<(), Status> {
    let mut seen_policy_ids = std::collections::HashSet::new();
    for p in &document.policies {
        if !p.id.trim().is_empty() && !seen_policy_ids.insert(p.id.as_str()) {
            return Err(activation_field_violation(
                "policies.id",
                format!("duplicate policy id {}", p.id),
                format!("duplicate policy id in version document: {}", p.id),
            ));
        }
    }
    Ok(())
}

fn validate_canary_scope(scope: &CanaryScope) -> Result<(), Status> {
    match scope {
        CanaryScope::Nodes(ids) | CanaryScope::Tenants(ids) if ids.is_empty() => {
            Err(activation_field_violation(
                "scope_values",
                "must be non-empty for NODE or TENANT canary scope",
                "canary scope_values must be non-empty for NODE/TENANT scope",
            ))
        }
        CanaryScope::Percent(0) => Err(activation_field_violation(
            "scope_percent",
            "must be in the range 1..=100",
            "canary PERCENT scope must be 1..=100 (0 includes nobody)",
        )),
        _ => Ok(()),
    }
}

impl AuthzServiceImpl {
    pub(super) async fn activate_policy_version_impl(
        &self,
        request: Request<authz_pb::ActivatePolicyVersionRequest>,
    ) -> Result<Response<authz_pb::ActivationResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "ActivatePolicyVersion",
                "governance.version.activate",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        if req.policy_version_id.trim().is_empty() {
            return Err(activation_required_field(
                "policy_version_id",
                "must be a non-empty policy version id",
                "policy_version_id is required",
            ));
        }
        let version = self.load_version(&req.policy_version_id).await?;
        // A3: bind the caller claim to the VERSION's tenant/project before the
        // replace-all (canary) activation — a tenant-A `authz:admin` must not
        // activate/replace tenant B's live policy set. Cross-tenant admins still
        // pass; no-op in-process.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &version.tenant_id,
            &version.project_id,
        )?;
        let state =
            authz_entity_pb::PolicyVersionState::try_from(version.state).unwrap_or_default();
        if !matches!(
            state,
            authz_entity_pb::PolicyVersionState::Approved
                | authz_entity_pb::PolicyVersionState::Superseded
                | authz_entity_pb::PolicyVersionState::RolledBack
        ) {
            return Err(policy_version_not_activatable_status(state));
        }
        // Optimistic concurrency over the version's own revision.
        if req.expected_revision != 0 && req.expected_revision != version.revision {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "policy version expected revision",
                0,
                "policy version changed concurrently (expected_revision mismatch)",
            ));
        }
        // Optimistic concurrency over the live authz revision counters.
        let (cur_policy, cur_rel, _) = self
            .current_authz_revision(&version.tenant_id, &version.project_id)
            .await?;
        if req.expected_policy_revision != 0 && req.expected_policy_revision != cur_policy {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "live policy revision",
                0,
                "live authz policy revision changed concurrently",
            ));
        }
        if req.expected_relationship_revision != 0 && req.expected_relationship_revision != cur_rel
        {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "live relationship revision",
                0,
                "live authz relationship revision changed concurrently",
            ));
        }

        let document = self.load_version_document(&req.policy_version_id).await?;
        let (policy_rev, rel_rev) = self
            .apply_document_and_activate(&version, &document, &actor, false)
            .await?;

        self.emit_event(AuthEvent::new(
            topics::POLICY_VERSION_ACTIVATED,
            version.policy_version_id.clone(),
            version.tenant_id.clone(),
            serde_json::json!({
                "policy_version_id": version.policy_version_id,
                "policy_set_id": version.policy_set_id,
                "version_number": version.version_number,
                "activated_by": actor,
                "policy_revision": policy_rev,
            }),
        ))
        .await;
        self.emit_bundle_invalidation(
            &version.tenant_id,
            &version.project_id,
            policy_rev,
            rel_rev,
            "policy version activated",
            &actor,
        )
        .await;

        let activated = self.load_version(&req.policy_version_id).await?;
        Ok(Response::new(authz_pb::ActivationResponse {
            version: Some(activated),
            policy_set: self
                .load_policy_set(&version.policy_set_id)
                .await
                .ok()
                .flatten(),
            policy_revision: policy_rev,
            relationship_revision: rel_rev,
            content_hash: document.content_hash(),
        }))
    }

    pub(super) async fn rollback_policy_version_impl(
        &self,
        request: Request<authz_pb::RollbackPolicyVersionRequest>,
    ) -> Result<Response<authz_pb::ActivationResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "RollbackPolicyVersion",
                "governance.version.rollback",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        if req.policy_set_id.trim().is_empty() {
            return Err(activation_required_field(
                "policy_set_id",
                "must be a non-empty policy set id",
                "policy_set_id is required",
            ));
        }
        let policy_set = self
            .load_policy_set(&req.policy_set_id)
            .await?
            .ok_or_else(|| {
                authz_not_found_status(
                    "load_policy_set",
                    "policy_set_not_found",
                    "policy set not found",
                )
            })?;
        // Target = explicit target, else the set's recorded rollback_version_id.
        let target_id = if req.target_version_id.trim().is_empty() {
            policy_set.rollback_version_id.clone()
        } else {
            req.target_version_id.clone()
        };
        if target_id.trim().is_empty() {
            return Err(rollback_target_required_status());
        }
        let target = self.load_version(&target_id).await?;
        // A3: bind the caller claim to the TARGET version's tenant/project before
        // rollback replaces the live policy set (cross-tenant admins still pass).
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &target.tenant_id,
            &target.project_id,
        )?;
        let document = self.load_version_document(&target_id).await?;
        // Rollback restores the target document and records the rollback lineage.
        let (policy_rev, rel_rev) = self
            .apply_document_and_activate(&target, &document, &actor, true)
            .await?;

        self.emit_event(AuthEvent::new(
            topics::POLICY_VERSION_ROLLED_BACK,
            target.policy_version_id.clone(),
            target.tenant_id.clone(),
            serde_json::json!({
                "policy_set_id": req.policy_set_id,
                "restored_version_id": target.policy_version_id,
                "rolled_back_by": actor,
                "reason": req.change_reason,
                "policy_revision": policy_rev,
            }),
        ))
        .await;
        self.emit_bundle_invalidation(
            &target.tenant_id,
            &target.project_id,
            policy_rev,
            rel_rev,
            "policy version rolled back",
            &actor,
        )
        .await;

        Ok(Response::new(authz_pb::ActivationResponse {
            version: Some(self.load_version(&target_id).await?),
            policy_set: self
                .load_policy_set(&req.policy_set_id)
                .await
                .ok()
                .flatten(),
            policy_revision: policy_rev,
            relationship_revision: rel_rev,
            content_hash: document.content_hash(),
        }))
    }

    /// Replace the tenant/project-scoped live authz rows with `document`, flip
    /// version/state bookkeeping, bump the durable authz revision, and
    /// invalidate the local snapshot cache. `is_rollback` records the lineage.
    ///
    /// Invariant: a `PolicyVersion` payload is the COMPLETE authz document for
    /// its tenant/project (drafts branch from the full active snapshot via
    /// `branch_from_active`, and the legacy migration carries the whole rule
    /// set). Activation is therefore a transactional replace of every
    /// tenant/project-scoped policy + role-binding + relationship row, not a
    /// merge — so a single governed policy set owns the tenant/project authz
    /// state. Run one policy set per tenant/project under governed mode.
    async fn apply_document_and_activate(
        &self,
        version: &authz_entity_pb::PolicyVersion,
        document: &PolicyDocument,
        actor: &str,
        is_rollback: bool,
    ) -> Result<(i64, i64), Status> {
        let pool = self.require_pool()?;
        let tenant = version.tenant_id.clone();
        let project = version.project_id.clone();

        // Record the currently-active version (for rollback bookkeeping) before
        // we replace it.
        let prior_active = self
            .policy_set_active_version(&version.policy_set_id)
            .await?;

        let mut tx = pool.begin().await.map_err(|err| {
            activation_internal_status(
                "activation_tx_begin",
                format!("activation tx begin failed: {err}"),
            )
        })?;
        // P6.10 carve-out: install request-local tenant/project context as the FIRST
        // tx statement so the replace-all legs run with the same RLS/audit context the
        // typed path would set (defense-in-depth — the legs are already explicitly
        // tenant-scoped, and authz tables are enable_rls-not-force so the broker owner
        // bypasses RLS, but this keeps the context correct and audit-attributable).
        crate::runtime::core::set_request_local_settings(
            &mut tx,
            &crate::RequestContext {
                tenant_id: tenant.clone(),
                project_id: project.clone(),
                ..crate::RequestContext::default()
            },
        )
        .await?;

        // 1. Replace policy_rules for this tenant/project with the version's.
        let pm = self.policies_model();
        sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {tenant_id} = $1 AND COALESCE({project_id}, '') = $2",
            rel = pm.relation,
            tenant_id = pm.q("tenant_id"),
            project_id = pm.q("project_id"),
        ))
        .bind(&tenant)
        .bind(&project)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            activation_internal_status("clear_policies", format!("clear policies failed: {err}"))
        })?;
        // Fail closed on duplicate non-empty policy ids within the document: each
        // policy's own id is bound on re-insert (below), so two policies sharing
        // a non-empty id would collide on the primary key / lose identity.
        validate_unique_document_policy_ids(document)?;
        // NOTE: the atomic DELETE-all above is load-bearing and intentionally
        // retained — it preserves the "live set == version document" invariant, so
        // a manual rule NOT present in the activated document is still (correctly)
        // removed here even though we now preserve each document policy's own id.
        for p in &document.policies {
            let mut attrs = serde_json::Map::new();
            for (k, v) in &p.conditions {
                attrs.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            attrs.insert(
                "priority".into(),
                serde_json::Value::String(p.priority.to_string()),
            );
            attrs.insert("role".into(), serde_json::Value::String(p.role.clone()));
            attrs.insert(
                "purpose".into(),
                serde_json::Value::String(p.purpose.clone()),
            );
            attrs.insert(
                "relationship".into(),
                serde_json::Value::String(p.relationship.clone()),
            );
            attrs.insert(
                "required_scopes".into(),
                serde_json::Value::String(
                    crate::runtime::service::auth_service::mappings::scopes_to_db(
                        &p.required_scopes,
                    ),
                ),
            );
            sqlx::query(&format!(
                "INSERT INTO {rel} ({policy_id}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {description}, {is_active}, {tenant_id}, {project_id}, {attributes_json}) \
                 VALUES (COALESCE(NULLIF($1,'')::uuid, gen_random_uuid()), $2, $3, $4, $5, $6, '', '', $7, $3, $8, $9::JSONB)",
                rel = pm.relation,
                policy_id = pm.q("policy_id"),
                subject = pm.q("subject"),
                domain_col = pm.q("domain"),
                object_col = pm.q("object"),
                action_col = pm.q("action"),
                effect_col = pm.q("effect"),
                condition = pm.q("condition"),
                description = pm.q("description"),
                is_active = pm.q("is_active"),
                tenant_id = pm.q("tenant_id"),
                project_id = pm.q("project_id"),
                attributes_json = pm.q("attributes_json"),
            ))
            .bind(&p.id)
            .bind(&p.subject)
            .bind(if p.tenant.trim().is_empty() { &tenant } else { &p.tenant })
            .bind(&p.resource)
            .bind(&p.action)
            .bind(effect_to_db(p.effect))
            .bind(p.enabled)
            .bind(if p.project.trim().is_empty() { &project } else { &p.project })
            .bind(serde_json::Value::Object(attrs))
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                activation_internal_status(
                    "insert_policy",
                    format!("insert policy failed: {err}"),
                )
            })?;
        }

        // 2. Replace relationship/grouping tuples for this tenant/project.
        let tm = self.relationship_tuples_model();
        sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {tenant_id} = $1 AND COALESCE({project_id}, '') = $2",
            rel = tm.relation,
            tenant_id = tm.q("tenant_id"),
            project_id = tm.q("project_id"),
        ))
        .bind(&tenant)
        .bind(&project)
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            activation_internal_status("clear_tuples", format!("clear tuples failed: {err}"))
        })?;
        for b in &document.role_bindings {
            let scope_tenant = if b.tenant.trim().is_empty() {
                &tenant
            } else {
                &b.tenant
            };
            sqlx::query(&format!(
                "INSERT INTO {rel} ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {tenant_id}, {project_id}) \
                 VALUES ('grouping', $1, $2, '', $3, '', '{{}}', $2, $4) \
                 ON CONFLICT ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}) DO NOTHING",
                rel = tm.relation,
                tuple_kind = tm.q("tuple_kind"),
                subject = tm.q("subject"),
                domain_col = tm.q("domain"),
                object_col = tm.q("object"),
                action_col = tm.q("action"),
                effect_col = tm.q("effect"),
                condition = tm.q("condition"),
                tenant_id = tm.q("tenant_id"),
                project_id = tm.q("project_id"),
            ))
            .bind(&b.subject)
            .bind(scope_tenant)
            .bind(&b.role)
            .bind(if b.project.trim().is_empty() { &project } else { &b.project })
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                activation_internal_status(
                    "insert_grouping_tuple",
                    format!("insert grouping tuple failed: {err}"),
                )
            })?;
        }
        for t in &document.tuples {
            let scope_tenant = if t.tenant.trim().is_empty() {
                &tenant
            } else {
                &t.tenant
            };
            sqlx::query(&format!(
                "INSERT INTO {rel} ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {tenant_id}, {project_id}) \
                 VALUES ('relationship', $1, $2, $3, $4, '', '{{}}', $2, $5) \
                 ON CONFLICT ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}) DO NOTHING",
                rel = tm.relation,
                tuple_kind = tm.q("tuple_kind"),
                subject = tm.q("subject"),
                domain_col = tm.q("domain"),
                object_col = tm.q("object"),
                action_col = tm.q("action"),
                effect_col = tm.q("effect"),
                condition = tm.q("condition"),
                tenant_id = tm.q("tenant_id"),
                project_id = tm.q("project_id"),
            ))
            .bind(&t.subject)
            .bind(scope_tenant)
            .bind(&t.object)
            .bind(&t.relation)
            .bind(if t.project.trim().is_empty() { &project } else { &t.project })
            .execute(&mut *tx)
            .await
            .map_err(|err| {
                activation_internal_status(
                    "insert_relationship_tuple",
                    format!("insert relationship tuple failed: {err}"),
                )
            })?;
        }

        // 3. Version state bookkeeping: supersede the previously-active version,
        //    flip this version ACTIVE, set rollback lineage.
        let vm = self.policy_versions_model();
        if let Some(prior) = &prior_active {
            if prior != &version.policy_version_id {
                sqlx::query(&format!(
                    "UPDATE {rel} SET {state} = '{superseded}' WHERE {policy_version_id} = $1::UUID",
                    rel = vm.relation,
                    state = vm.q("state"),
                    superseded = version_state_to_db(authz_entity_pb::PolicyVersionState::Superseded),
                    policy_version_id = vm.q("policy_version_id"),
                ))
                .bind(prior)
                .execute(&mut *tx)
                .await
                .map_err(|err| {
                    activation_internal_status(
                        "supersede_prior_version",
                        format!("supersede prior version failed: {err}"),
                    )
                })?;
            }
        }
        let new_state = if is_rollback {
            authz_entity_pb::PolicyVersionState::RolledBack
        } else {
            authz_entity_pb::PolicyVersionState::Active
        };
        sqlx::query(&format!(
            "UPDATE {rel} SET {state} = $2, {activated_by} = $3, {activated_at} = NOW(), {revision} = {revision} + 1, {rollback_of} = $4::UUID \
             WHERE {policy_version_id} = $1::UUID",
            rel = vm.relation,
            state = vm.q("state"),
            activated_by = vm.q("activated_by"),
            activated_at = vm.q("activated_at"),
            revision = vm.q("revision"),
            rollback_of = vm.q("rollback_of"),
            policy_version_id = vm.q("policy_version_id"),
        ))
        .bind(&version.policy_version_id)
        .bind(version_state_to_db(new_state))
        .bind(actor)
        // K1-EXPANDED (bug_report.md): rollback_of is a nullable UUID column.
        // prior_active is Option<String> (the prior active version id, or None) —
        // bind it as Option<&str> so None→NULL and a real id→text, with $4::UUID
        // casting it into the uuid column instead of leaking "column is uuid but
        // expression is text".
        .bind(prior_active.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            activation_internal_status(
                "activate_version",
                format!("activate version failed: {err}"),
            )
        })?;

        // 4. PolicySet pointers: active = this version, rollback = prior active.
        let sm = self.policy_sets_model();
        sqlx::query(&format!(
            "UPDATE {rel} SET {active_version_id} = $2::UUID, {rollback_version_id} = $3::UUID WHERE {policy_set_id} = $1::UUID",
            rel = sm.relation,
            active_version_id = sm.q("active_version_id"),
            rollback_version_id = sm.q("rollback_version_id"),
            policy_set_id = sm.q("policy_set_id"),
        ))
        .bind(&version.policy_set_id)
        .bind(&version.policy_version_id)
        .bind(prior_active.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(|err| {
            activation_internal_status(
                "update_policy_set_pointers",
                format!("update policy set pointers failed: {err}"),
            )
        })?;

        tx.commit().await.map_err(|err| {
            activation_internal_status(
                "activation_tx_commit",
                format!("activation tx commit failed: {err}"),
            )
        })?;

        // 5. Bump the durable authz revision (covers policy + tuples) and force
        //    a local snapshot reload so this node enforces immediately.
        let change_type = if is_rollback {
            authz_entity_pb::AuthzChangeType::Rollback
        } else {
            authz_entity_pb::AuthzChangeType::Activation
        };
        let (policy_rev, rel_rev) = self
            .bump_authz_revision(
                &tenant,
                &project,
                change_type,
                &document.content_hash(),
                actor,
            )
            .await?;
        self.invalidate_snapshot_cache();
        Ok((policy_rev, rel_rev))
    }

    /// The policy set's currently-active version id, if any.
    async fn policy_set_active_version(
        &self,
        policy_set_id: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_sets_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT COALESCE({active_version_id}::TEXT, '') AS active FROM {rel} WHERE {policy_set_id} = $1::UUID",
            active_version_id = m.q("active_version_id"),
            policy_set_id = m.q("policy_set_id"),
        ))
        .bind(policy_set_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            activation_internal_status(
                "read_active_version",
                format!("read active version failed: {err}"),
            )
        })?;
        Ok(row
            .and_then(|r| r.try_get::<String, _>("active").ok())
            .filter(|s| !s.trim().is_empty()))
    }
}

// ── Phase 9: progressive rollout (canary + metric-based auto-rollback) ──────
//
// A canary activates a candidate `PolicyVersion` to a SUBSET of the fleet (node
// ids / tenant ids / a percentage) and is watched by the background evaluator
// (`control_plane::canary::spawn_canary_evaluator`). A metric breach inside the
// bake window auto-rolls back to the policy set's prior version BEFORE the bad
// policy reaches the whole fleet; an inconclusive signal pauses; a healthy bake
// makes the canary promote-eligible (an explicit `PromoteCanary` flips it
// fleet-wide). Every transition is audited.

use crate::runtime::service::auth_service::control_plane::canary as canarylib;
use canarylib::{CanaryExecutor, CanaryMetricSource, CanaryScope};

impl AuthzServiceImpl {
    /// The shared metrics recorder (so the parent can spawn the canary evaluator
    /// against the SAME recorder this service uses, without duplicating it).
    pub(crate) fn canary_metrics(&self) -> Arc<dyn crate::metrics::MetricsRecorder> {
        self.metrics.clone()
    }

    /// Build the production node-state-backed metric source for the canary
    /// evaluator, when a Postgres pool is configured. Returns `None` (→ caller
    /// falls back to [`canarylib::NoSignalSource`], i.e. zero-sample canaries
    /// keep baking and remain explicitly promotable) when there is no pool.
    pub(crate) fn build_canary_metric_source(&self) -> Option<Arc<dyn CanaryMetricSource>> {
        self.pg_pool
            .clone()
            .map(|pool| Arc::new(NackRateMetricSource::new(pool)) as Arc<dyn CanaryMetricSource>)
    }

    /// Spawn the Phase-9 canary evaluator background task. It periodically reads
    /// each ACTIVE canary's success signal and — per `evaluate_canary` —
    /// auto-rolls-back breaching canaries (restoring the prior version BEFORE
    /// fleet-wide promotion), PAUSES low-sample inconclusive ones, and leaves
    /// healthy or zero-sample canaries to bake until an operator promotes.
    /// Detached; runs for the process lifetime.
    /// The parent calls this once in `serve()` against the built `AuthzServiceImpl`.
    pub(crate) fn spawn_canary_evaluator(&self) -> tokio::task::JoinHandle<()> {
        let executor: Arc<dyn CanaryExecutor> = Arc::new(self.clone());
        let recorder = self.canary_metrics();
        let source = self
            .build_canary_metric_source()
            .unwrap_or_else(|| Arc::new(canarylib::NoSignalSource) as Arc<dyn CanaryMetricSource>);
        canarylib::spawn_canary_evaluator(
            executor,
            source,
            recorder,
            canarylib::CANARY_EVAL_INTERVAL,
            Arc::new(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0)
            }),
        )
    }

    /// Native model for the `policy_canaries` table (proto is source of truth).
    pub(super) fn policy_canaries_model(&self) -> NativeModel {
        native_model(
            "udb.core.authz.entity.v1.PolicyCanary",
            &[
                "canary_id",
                "policy_set_id",
                "policy_version_id",
                "scope_kind",
                "scope_values",
                "state",
                "success_window_secs",
                "metric_threshold",
                "created_by",
                "tenant_id",
                "project_id",
                "min_samples",
                "rollback_version_id",
                "outcome_reason",
                "revision",
            ],
        )
    }

    // ── ActivateCanary ──────────────────────────────────────────────────────

    pub(super) async fn activate_canary_impl(
        &self,
        request: Request<authz_pb::ActivateCanaryRequest>,
    ) -> Result<Response<authz_pb::CanaryResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "ActivateCanary",
                "governance.canary.activate",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        if req.policy_version_id.trim().is_empty() {
            return Err(activation_required_field(
                "policy_version_id",
                "must be a non-empty policy version id",
                "policy_version_id is required",
            ));
        }
        let version = self.load_version(&req.policy_version_id).await?;
        // A3: bind the caller claim to the VERSION's tenant/project before the
        // replace-all (canary) activation — a tenant-A `authz:admin` must not
        // activate/replace tenant B's live policy set. Cross-tenant admins still
        // pass; no-op in-process.
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &version.tenant_id,
            &version.project_id,
        )?;
        let state =
            authz_entity_pb::PolicyVersionState::try_from(version.state).unwrap_or_default();
        if !matches!(
            state,
            authz_entity_pb::PolicyVersionState::Approved
                | authz_entity_pb::PolicyVersionState::Superseded
                | authz_entity_pb::PolicyVersionState::RolledBack
        ) {
            return Err(policy_version_not_canariable_status(state));
        }
        if req.expected_revision != 0 && req.expected_revision != version.revision {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "canary policy version expected revision",
                0,
                "policy version changed concurrently (expected_revision mismatch)",
            ));
        }

        // Decode + validate the requested scope (fails closed on an empty/garbage
        // scope so a canary is never silently fleet-wide).
        let scope_kind = authz_entity_pb::CanaryScopeKind::try_from(req.scope_kind)
            .unwrap_or(authz_entity_pb::CanaryScopeKind::Unspecified);
        let scope = CanaryScope::from_row(scope_kind, &req.scope_values);
        validate_canary_scope(&scope)?;

        // The version a breach rolls back to = the policy set's current active
        // version (the prior good state). May be empty for a first-ever activation.
        let rollback_target = self
            .policy_set_active_version(&version.policy_set_id)
            .await?
            .unwrap_or_default();

        let window = if req.success_window_secs > 0 {
            req.success_window_secs
        } else {
            DEFAULT_CANARY_WINDOW_SECS
        };
        let threshold = if req.metric_threshold.is_finite() && req.metric_threshold >= 0.0 {
            req.metric_threshold
        } else {
            DEFAULT_CANARY_THRESHOLD
        };
        let min_samples = req.min_samples.max(1);

        let scope_values_json =
            serde_json::to_string(&scope.values()).unwrap_or_else(|_| "[]".to_string());

        let pool = self.require_pool()?;
        let m = self.policy_canaries_model();
        let canary_id: String = sqlx::query(&format!(
            "INSERT INTO {rel} \
                ({policy_set_id}, {policy_version_id}, {scope_kind}, {scope_values}, {state}, \
                 {success_window_secs}, {metric_threshold}, {created_by}, {tenant_id}, {project_id}, \
                 {min_samples}, {rollback_version_id}, {outcome_reason}) \
             VALUES ($1::UUID, $2::UUID, $3, $4::JSONB, $5, $6, $7, $8, $9, $10, $11, $12::UUID, '') \
             RETURNING {canary_id}::TEXT AS canary_id",
            rel = m.relation,
            policy_set_id = m.q("policy_set_id"),
            policy_version_id = m.q("policy_version_id"),
            scope_kind = m.q("scope_kind"),
            scope_values = m.q("scope_values"),
            state = m.q("state"),
            success_window_secs = m.q("success_window_secs"),
            metric_threshold = m.q("metric_threshold"),
            created_by = m.q("created_by"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            min_samples = m.q("min_samples"),
            rollback_version_id = m.q("rollback_version_id"),
            outcome_reason = m.q("outcome_reason"),
            canary_id = m.q("canary_id"),
        ))
        .bind(&version.policy_set_id)
        .bind(&version.policy_version_id)
        .bind(canary_scope_kind_to_db(scope.kind()))
        .bind(scope_values_json)
        .bind(canary_state_to_db(authz_entity_pb::CanaryState::Active))
        .bind(window)
        .bind(threshold)
        .bind(&actor)
        .bind(&version.tenant_id)
        .bind(&version.project_id)
        .bind(min_samples)
        .bind(if rollback_target.is_empty() {
            None
        } else {
            Some(rollback_target.clone())
        })
        .fetch_one(pool)
        .await
        .map_err(|err| {
            activation_internal_status("create_canary", format!("create canary failed: {err}"))
        })?
        .try_get("canary_id")
        .map_err(|err| {
            activation_internal_status(
                "create_canary_id_decode",
                format!("create canary returned no id: {err}"),
            )
        })?;

        let canary = self.load_canary(&canary_id).await?;
        self.emit_canary_event(
            topics::POLICY_CANARY_ACTIVATED,
            &canary,
            &actor,
            "canary activated to subset scope",
        )
        .await;

        Ok(Response::new(authz_pb::CanaryResponse {
            canary: Some(canary),
            version: Some(self.load_version(&req.policy_version_id).await?),
            policy_set: self
                .load_policy_set(&version.policy_set_id)
                .await
                .ok()
                .flatten(),
        }))
    }

    // ── PromoteCanary ───────────────────────────────────────────────────────

    pub(super) async fn promote_canary_impl(
        &self,
        request: Request<authz_pb::PromoteCanaryRequest>,
    ) -> Result<Response<authz_pb::CanaryResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        let actor = self
            .authorize_governance(
                req.actor.as_ref(),
                "PromoteCanary",
                "governance.canary.promote",
                &[super::governance::SCOPE_AUTHZ_ADMIN],
                now,
            )
            .await?;
        if req.canary_id.trim().is_empty() {
            return Err(activation_required_field(
                "canary_id",
                "must be a non-empty policy canary id",
                "canary_id is required",
            ));
        }
        let canary = self.load_canary(&req.canary_id).await?;
        if req.expected_revision != 0 && req.expected_revision != canary.revision {
            return Err(crate::runtime::executor_utils::retryable_aborted_status(
                "authz",
                "canary expected revision",
                0,
                "canary changed concurrently (expected_revision mismatch)",
            ));
        }
        if canary.state != authz_entity_pb::CanaryState::Active as i32 {
            let st = authz_entity_pb::CanaryState::try_from(canary.state).unwrap_or_default();
            return Err(canary_not_active_status(st));
        }
        if !canarylib::promote_eligible(&canary, now) {
            return Err(canary_not_promote_eligible_status());
        }

        // Promote = the real Phase-K fleet-wide activation of the canaried
        // version. Reuse the governance activation path so PolicySet pointers,
        // version state, authz revision, and cluster invalidation all flip.
        let version = self.load_version(&canary.policy_version_id).await?;
        // A3: bind the caller claim to the version's tenant/project before promote
        // runs the fleet-wide activation (cross-tenant admins still pass).
        crate::runtime::service::method_security::enforce_body_tenant_matches_claim(
            &crate::runtime::service::method_security::current_claim_context(),
            &version.tenant_id,
            &version.project_id,
        )?;
        let document = self
            .load_version_document(&canary.policy_version_id)
            .await?;
        let (policy_rev, rel_rev) = self
            .apply_document_and_activate(&version, &document, &actor, false)
            .await?;

        self.set_canary_state(
            &canary.canary_id,
            authz_entity_pb::CanaryState::Promoted,
            "promoted fleet-wide after healthy bake",
        )
        .await?;
        let promoted = self.load_canary(&canary.canary_id).await?;
        self.emit_canary_event(
            topics::POLICY_CANARY_PROMOTED,
            &promoted,
            &actor,
            "canary promoted fleet-wide",
        )
        .await;
        self.emit_bundle_invalidation(
            &version.tenant_id,
            &version.project_id,
            policy_rev,
            rel_rev,
            "canary promoted fleet-wide",
            &actor,
        )
        .await;

        Ok(Response::new(authz_pb::CanaryResponse {
            canary: Some(promoted),
            version: Some(self.load_version(&canary.policy_version_id).await?),
            policy_set: self
                .load_policy_set(&version.policy_set_id)
                .await
                .ok()
                .flatten(),
        }))
    }

    // ── GetCanaryStatus ─────────────────────────────────────────────────────

    pub(super) async fn get_canary_status_impl(
        &self,
        request: Request<authz_pb::GetCanaryStatusRequest>,
    ) -> Result<Response<authz_pb::GetCanaryStatusResponse>, Status> {
        let now = now_unix() as i64;
        let req = request.into_inner();
        self.authorize_governance(
            req.actor.as_ref(),
            "GetCanaryStatus",
            "governance.canary.read",
            &[
                super::governance::SCOPE_POLICY_READ,
                super::governance::SCOPE_AUTHZ_ADMIN,
            ],
            now,
        )
        .await?;
        if req.canary_id.trim().is_empty() {
            return Err(activation_required_field(
                "canary_id",
                "must be a non-empty policy canary id",
                "canary_id is required",
            ));
        }
        let canary = self.load_canary(&req.canary_id).await?;
        let promote_eligible = canarylib::promote_eligible(&canary, now);
        let window_remaining_secs = canarylib::window_remaining_secs(&canary, now);
        Ok(Response::new(authz_pb::GetCanaryStatusResponse {
            canary: Some(canary),
            promote_eligible,
            window_remaining_secs,
        }))
    }

    // ── durable load / state transition ─────────────────────────────────────

    /// Load one canary row as the proto entity.
    pub(super) async fn load_canary(
        &self,
        canary_id: &str,
    ) -> Result<authz_entity_pb::PolicyCanary, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_canaries_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT {canary_id}::TEXT AS canary_id, {policy_set_id}::TEXT AS policy_set_id, \
                    {policy_version_id}::TEXT AS policy_version_id, {scope_kind}, \
                    {scope_values}::TEXT AS scope_values, {state}, {success_window_secs}, \
                    {metric_threshold}, COALESCE({created_by}, '') AS created_by, \
                    {tenant_id}::TEXT AS tenant_id, COALESCE({project_id}, '') AS project_id, \
                    {min_samples}, COALESCE({rollback_version_id}::TEXT, '') AS rollback_version_id, \
                    COALESCE({outcome_reason}, '') AS outcome_reason, {revision}, \
                    EXTRACT(EPOCH FROM {started_at})::BIGINT AS started_at_unix \
             FROM {rel} WHERE {canary_id} = $1::UUID",
            canary_id = m.q("canary_id"),
            policy_set_id = m.q("policy_set_id"),
            policy_version_id = m.q("policy_version_id"),
            scope_kind = m.q("scope_kind"),
            scope_values = m.q("scope_values"),
            state = m.q("state"),
            success_window_secs = m.q("success_window_secs"),
            metric_threshold = m.q("metric_threshold"),
            created_by = m.q("created_by"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            min_samples = m.q("min_samples"),
            rollback_version_id = m.q("rollback_version_id"),
            outcome_reason = m.q("outcome_reason"),
            revision = m.q("revision"),
            started_at = m.q("started_at"),
            rel = rel,
        ))
        .bind(canary_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            activation_internal_status("load_canary", format!("load canary failed: {err}"))
        })?
        .ok_or_else(|| {
            authz_not_found_status(
                "load_canary",
                "policy_canary_not_found",
                "canary not found",
            )
        })?;
        Ok(canary_from_row(&row))
    }

    /// Every ACTIVE canary (evaluator input).
    pub(super) async fn load_active_canaries(
        &self,
    ) -> Result<Vec<authz_entity_pb::PolicyCanary>, Status> {
        let pool = self.require_pool()?;
        let m = self.policy_canaries_model();
        let rel = m.relation.clone();
        let rows = sqlx::query(&format!(
            "SELECT {canary_id}::TEXT AS canary_id, {policy_set_id}::TEXT AS policy_set_id, \
                    {policy_version_id}::TEXT AS policy_version_id, {scope_kind}, \
                    {scope_values}::TEXT AS scope_values, {state}, {success_window_secs}, \
                    {metric_threshold}, COALESCE({created_by}, '') AS created_by, \
                    {tenant_id}::TEXT AS tenant_id, COALESCE({project_id}, '') AS project_id, \
                    {min_samples}, COALESCE({rollback_version_id}::TEXT, '') AS rollback_version_id, \
                    COALESCE({outcome_reason}, '') AS outcome_reason, {revision}, \
                    EXTRACT(EPOCH FROM {started_at})::BIGINT AS started_at_unix \
             FROM {rel} WHERE {state} = $1 ORDER BY {started_at} ASC",
            canary_id = m.q("canary_id"),
            policy_set_id = m.q("policy_set_id"),
            policy_version_id = m.q("policy_version_id"),
            scope_kind = m.q("scope_kind"),
            scope_values = m.q("scope_values"),
            state = m.q("state"),
            success_window_secs = m.q("success_window_secs"),
            metric_threshold = m.q("metric_threshold"),
            created_by = m.q("created_by"),
            tenant_id = m.q("tenant_id"),
            project_id = m.q("project_id"),
            min_samples = m.q("min_samples"),
            rollback_version_id = m.q("rollback_version_id"),
            outcome_reason = m.q("outcome_reason"),
            revision = m.q("revision"),
            started_at = m.q("started_at"),
            rel = rel,
        ))
        .bind(canary_state_to_db(authz_entity_pb::CanaryState::Active))
        .fetch_all(pool)
        .await
        .map_err(|err| {
            activation_internal_status(
                "list_active_canaries",
                format!("list active canaries failed: {err}"),
            )
        })?;
        Ok(rows.iter().map(canary_from_row).collect())
    }

    /// Transition a canary to `new_state` (revision-bumped, records the reason).
    /// Only honored while the canary is still ACTIVE so two evaluator cycles (or
    /// an evaluator + a promote) cannot double-transition.
    pub(super) async fn set_canary_state(
        &self,
        canary_id: &str,
        new_state: authz_entity_pb::CanaryState,
        reason: &str,
    ) -> Result<bool, Status> {
        let runtime = self.runtime.as_ref().ok_or_else(|| {
            authz_capability_status(
                "canary_persistence",
                "runtime_native_entity_dispatch",
                "native authz requires runtime-backed canary persistence",
            )
        })?;
        // P6.10 Wave 1: typed conditional UPDATE (was raw). `state=$4` (ACTIVE) guard
        // keeps the single-transition semantic; revision bumps via Increment.
        let mut assignments = std::collections::BTreeMap::new();
        assignments.insert(
            "state".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String(canary_state_to_db(new_state).to_string()),
            },
        );
        assignments.insert(
            "outcome_reason".to_string(),
            LogicalAssignment::Set {
                value: LogicalValue::String(reason.to_string()),
            },
        );
        assignments.insert(
            "revision".to_string(),
            LogicalAssignment::Increment {
                by: LogicalValue::Int(1),
            },
        );
        let (affected, _) = runtime
            .native_entity_update_for_service(
                "authz",
                &crate::RequestContext::default(),
                LogicalUpdate {
                    message_type: "udb.core.authz.entity.v1.PolicyCanary".to_string(),
                    filter: LogicalFilter::And(vec![
                        LogicalFilter::Comparison {
                            field: "canary_id".to_string(),
                            op: ComparisonOp::Eq,
                            value: LogicalValue::String(canary_id.to_string()),
                        },
                        LogicalFilter::Comparison {
                            field: "state".to_string(),
                            op: ComparisonOp::Eq,
                            value: LogicalValue::String(
                                canary_state_to_db(authz_entity_pb::CanaryState::Active)
                                    .to_string(),
                            ),
                        },
                    ]),
                    assignments,
                    return_fields: Vec::new(),
                    require_affected: false,
                },
            )
            .await
            .map_err(|err| {
                activation_internal_status(
                    "update_canary_state",
                    format!("update canary state failed: {err}"),
                )
            })?;
        Ok(affected > 0)
    }

    /// Emit a governance audit event for a canary transition.
    pub(super) async fn emit_canary_event(
        &self,
        topic: &'static str,
        canary: &authz_entity_pb::PolicyCanary,
        actor: &str,
        reason: &str,
    ) {
        let severity = if topic == topics::POLICY_CANARY_ROLLED_BACK {
            "high"
        } else {
            "info"
        };
        self.emit_event(AuthEvent::new(
            topic,
            canary.canary_id.clone(),
            canary.tenant_id.clone(),
            serde_json::json!({
                "canary_id": canary.canary_id,
                "policy_set_id": canary.policy_set_id,
                "policy_version_id": canary.policy_version_id,
                "rollback_version_id": canary.rollback_version_id,
                "state": canary.state,
                "scope_kind": canary.scope_kind,
                "tenant_id": canary.tenant_id,
                "project_id": canary.project_id,
                "actor": actor,
                "reason": reason,
                "severity": severity,
            }),
        ))
        .await;
    }

    /// The auto-rollback executed by the evaluator on a metric breach: flip the
    /// canary to ROLLED_BACK, restore the policy set's prior version through the
    /// real governance rollback path, and emit the high-severity audit +
    /// cluster-invalidation events. Idempotent (no-op once not ACTIVE).
    pub(super) async fn canary_auto_rollback(
        &self,
        canary: &authz_entity_pb::PolicyCanary,
        reason: &str,
    ) -> Result<(), Status> {
        // Claim the transition first so a second evaluator cycle can't double-roll.
        if !self
            .set_canary_state(
                &canary.canary_id,
                authz_entity_pb::CanaryState::RolledBack,
                reason,
            )
            .await?
        {
            return Ok(());
        }
        let actor = "system:canary-evaluator";

        // Restore the prior version. When the policy set had a prior active
        // version we restore it; otherwise the canary never reached fleet-wide
        // (subset only) and there is nothing to restore beyond marking it rolled
        // back — the bad version simply never gets promoted.
        if !canary.rollback_version_id.trim().is_empty() {
            let target = self.load_version(&canary.rollback_version_id).await?;
            let document = self
                .load_version_document(&canary.rollback_version_id)
                .await?;
            let (policy_rev, rel_rev) = self
                .apply_document_and_activate(&target, &document, actor, true)
                .await?;
            self.emit_event(AuthEvent::new(
                topics::POLICY_VERSION_ROLLED_BACK,
                target.policy_version_id.clone(),
                target.tenant_id.clone(),
                serde_json::json!({
                    "policy_set_id": canary.policy_set_id,
                    "restored_version_id": target.policy_version_id,
                    "rolled_back_by": actor,
                    "reason": reason,
                    "policy_revision": policy_rev,
                    "trigger": "canary_auto_rollback",
                }),
            ))
            .await;
            self.emit_bundle_invalidation(
                &target.tenant_id,
                &target.project_id,
                policy_rev,
                rel_rev,
                "canary auto-rollback",
                actor,
            )
            .await;
        }

        let rolled = self.load_canary(&canary.canary_id).await?;
        self.emit_canary_event(topics::POLICY_CANARY_ROLLED_BACK, &rolled, actor, reason)
            .await;
        Ok(())
    }

    /// The pause executed by the evaluator on an inconclusive signal: flip the
    /// canary to PAUSED + audit, only on the first transition out of ACTIVE.
    pub(super) async fn canary_pause(
        &self,
        canary: &authz_entity_pb::PolicyCanary,
        reason: &str,
    ) -> Result<(), Status> {
        if !self
            .set_canary_state(
                &canary.canary_id,
                authz_entity_pb::CanaryState::Paused,
                reason,
            )
            .await?
        {
            return Ok(());
        }
        let paused = self.load_canary(&canary.canary_id).await?;
        self.emit_canary_event(
            topics::POLICY_CANARY_PAUSED,
            &paused,
            "system:canary-evaluator",
            reason,
        )
        .await;
        Ok(())
    }
}

/// Service-default bake window / breach threshold (used when the request leaves
/// them zero). Named constants — no magic numbers at the call site.
const DEFAULT_CANARY_WINDOW_SECS: i64 = 300;
const DEFAULT_CANARY_THRESHOLD: f64 = 0.05;

/// `AuthzServiceImpl` is the production [`CanaryExecutor`]: it drives the real
/// governance rollback path and durable transitions. The background evaluator
/// holds it as `Arc<dyn CanaryExecutor>`.
#[async_trait::async_trait]
impl CanaryExecutor for AuthzServiceImpl {
    async fn list_active_canaries(&self) -> Vec<authz_entity_pb::PolicyCanary> {
        match self.load_active_canaries().await {
            Ok(canaries) => canaries,
            Err(err) => {
                tracing::warn!(error = %err, "canary evaluator: list_active_canaries failed");
                Vec::new()
            }
        }
    }

    async fn auto_rollback(&self, canary: &authz_entity_pb::PolicyCanary, reason: &str) {
        if let Err(err) = self.canary_auto_rollback(canary, reason).await {
            tracing::error!(
                canary_id = %canary.canary_id,
                error = %err,
                "canary auto-rollback failed"
            );
        }
    }

    async fn pause(&self, canary: &authz_entity_pb::PolicyCanary, reason: &str) {
        if let Err(err) = self.canary_pause(canary, reason).await {
            tracing::warn!(canary_id = %canary.canary_id, error = %err, "canary pause failed");
        }
    }
}

/// A [`CanaryMetricSource`] backed by the control-plane node-state NACK ledger:
/// the success metric for a canary is the fraction of its in-scope nodes that
/// currently hold a NACK (rejected the pushed policy). A high NACK fraction is a
/// real, conservative breach signal ("nodes are rejecting this policy"). When no
/// in-scope node has reported yet, `samples == 0` → the evaluator PAUSES rather
/// than promoting on no evidence.
#[derive(Clone)]
pub struct NackRateMetricSource {
    pool: PgPool,
}

impl NackRateMetricSource {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl CanaryMetricSource for NackRateMetricSource {
    async fn read(
        &self,
        canary: &authz_entity_pb::PolicyCanary,
        _window_secs: i64,
    ) -> canarylib::CanarySignal {
        let kind = authz_entity_pb::CanaryScopeKind::try_from(canary.scope_kind)
            .unwrap_or(authz_entity_pb::CanaryScopeKind::Unspecified);
        let values: Vec<String> =
            serde_json::from_str(canary.scope_values.trim()).unwrap_or_default();
        let scope = CanaryScope::from_row(kind, &values);

        // Pull the node-state ledger (admin view) and project onto in-scope nodes.
        self.nack_fraction_in_scope(&scope)
            .await
            .unwrap_or(canarylib::CanarySignal {
                value: 0.0,
                samples: 0,
            })
    }
}

impl NackRateMetricSource {
    async fn nack_fraction_in_scope(
        &self,
        scope: &CanaryScope,
    ) -> Result<canarylib::CanarySignal, Status> {
        // The node-state ledger lives under control_plane; read it via its model
        // so we never reach into private store internals.
        let m = native_model(
            "udb.core.control.entity.v1.ControlPlaneNodeState",
            &["node_id", "nack_error_detail"],
        );
        let rel = m.relation.clone();
        let rows = sqlx::query(&format!(
            "SELECT {node_id} AS node_id, \
                    (COALESCE({nack}, '') <> '') AS nacked \
             FROM {rel}",
            node_id = m.q("node_id"),
            nack = m.q("nack_error_detail"),
            rel = rel,
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|err| {
            activation_internal_status(
                "read_node_state_ledger",
                format!("read node-state ledger failed: {err}"),
            )
        })?;

        let mut samples = 0i64;
        let mut nacked = 0i64;
        for row in &rows {
            let node_id: String = row.try_get("node_id").unwrap_or_default();
            // Percent/Node scoping selects nodes; tenant-scoped canaries have no
            // node projection here, so they yield no node samples (→ PAUSE until a
            // metric backend with tenant-addressed signal is wired). The
            // `ControlPlaneNodeState` ledger is node-addressed (no tenant column),
            // so `CanaryScope::tenant_in_scope` cannot be applied against these
            // rows; it is reached only by a future tenant-addressed signal source.
            if matches!(scope, CanaryScope::Tenants(_)) {
                continue;
            }
            if !scope.node_in_scope(&node_id) {
                continue;
            }
            samples += 1;
            if row.try_get::<bool, _>("nacked").unwrap_or(false) {
                nacked += 1;
            }
        }
        let value = if samples > 0 {
            nacked as f64 / samples as f64
        } else {
            0.0
        };
        Ok(canarylib::CanarySignal { value, samples })
    }
}

// ── row decode / enum mapping ───────────────────────────────────────────────

fn canary_from_row(row: &sqlx::postgres::PgRow) -> authz_entity_pb::PolicyCanary {
    let started_at_unix: i64 = row.try_get("started_at_unix").unwrap_or(0);
    authz_entity_pb::PolicyCanary {
        canary_id: row.try_get("canary_id").unwrap_or_default(),
        policy_set_id: row.try_get("policy_set_id").unwrap_or_default(),
        policy_version_id: row.try_get("policy_version_id").unwrap_or_default(),
        scope_kind: canary_scope_kind_from_db(
            &row.try_get::<String, _>("scope_kind").unwrap_or_default(),
        ),
        scope_values: row
            .try_get("scope_values")
            .unwrap_or_else(|_| "[]".to_string()),
        state: canary_state_from_db(&row.try_get::<String, _>("state").unwrap_or_default()),
        started_at: timestamp_from_unix(started_at_unix.max(0) as u64),
        success_window_secs: row.try_get("success_window_secs").unwrap_or(0),
        metric_threshold: row.try_get("metric_threshold").unwrap_or(0.0),
        created_by: row.try_get("created_by").unwrap_or_default(),
        tenant_id: row.try_get("tenant_id").unwrap_or_default(),
        project_id: row.try_get("project_id").unwrap_or_default(),
        min_samples: row.try_get("min_samples").unwrap_or(1),
        rollback_version_id: row.try_get("rollback_version_id").unwrap_or_default(),
        outcome_reason: row.try_get("outcome_reason").unwrap_or_default(),
        revision: row.try_get("revision").unwrap_or(1),
    }
}

fn canary_state_to_db(state: authz_entity_pb::CanaryState) -> &'static str {
    use authz_entity_pb::CanaryState as S;
    match state {
        S::Unspecified => "CANARY_STATE_UNSPECIFIED",
        S::Active => "CANARY_STATE_ACTIVE",
        S::Promoted => "CANARY_STATE_PROMOTED",
        S::RolledBack => "CANARY_STATE_ROLLED_BACK",
        S::Paused => "CANARY_STATE_PAUSED",
    }
}

fn canary_state_from_db(value: &str) -> i32 {
    use authz_entity_pb::CanaryState as S;
    let v = match value {
        "CANARY_STATE_ACTIVE" | "ACTIVE" => S::Active,
        "CANARY_STATE_PROMOTED" | "PROMOTED" => S::Promoted,
        "CANARY_STATE_ROLLED_BACK" | "ROLLED_BACK" => S::RolledBack,
        "CANARY_STATE_PAUSED" | "PAUSED" => S::Paused,
        _ => S::Unspecified,
    };
    v as i32
}

fn canary_scope_kind_to_db(kind: authz_entity_pb::CanaryScopeKind) -> &'static str {
    use authz_entity_pb::CanaryScopeKind as K;
    match kind {
        K::Unspecified => "CANARY_SCOPE_KIND_UNSPECIFIED",
        K::Node => "CANARY_SCOPE_KIND_NODE",
        K::Tenant => "CANARY_SCOPE_KIND_TENANT",
        K::Percent => "CANARY_SCOPE_KIND_PERCENT",
    }
}

fn canary_scope_kind_from_db(value: &str) -> i32 {
    use authz_entity_pb::CanaryScopeKind as K;
    let v = match value {
        "CANARY_SCOPE_KIND_NODE" | "NODE" => K::Node,
        "CANARY_SCOPE_KIND_TENANT" | "TENANT" => K::Tenant,
        "CANARY_SCOPE_KIND_PERCENT" | "PERCENT" => K::Percent,
        _ => K::Unspecified,
    };
    v as i32
}

#[cfg(test)]
mod tests {
    use super::super::governance::SCOPE_AUTHZ_ADMIN;
    use super::*;
    use crate::proto::udb::core::authz::services::v1::authz_service_server::AuthzService;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::authz::{AuthzPolicy, AuthzSnapshot};
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

    fn admin_actor() -> authz_pb::GovernanceActor {
        authz_pb::GovernanceActor {
            subject: "policy-admin".to_string(),
            tenant_id: "tenant-a".to_string(),
            scopes: vec![SCOPE_AUTHZ_ADMIN.to_string()],
            ..Default::default()
        }
    }

    fn svc() -> AuthzServiceImpl {
        AuthzServiceImpl::new(AuthzSnapshot::default())
    }

    #[test]
    fn activation_internal_status_carries_typed_detail() {
        let status =
            activation_internal_status("activate_version", "activate version failed: store closed");
        assert_internal_detail(
            &status,
            "activate_version",
            "activate version failed: store closed",
        );
    }

    #[tokio::test]
    async fn activate_policy_version_missing_policy_version_id_carries_field_violation() {
        let err = svc()
            .activate_policy_version(Request::new(authz_pb::ActivatePolicyVersionRequest {
                actor: Some(admin_actor()),
                policy_version_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing policy_version_id must fail before version loading");

        assert_eq!(err.message(), "policy_version_id is required");
        assert_validation_field(
            &err,
            "policy_version_id",
            "must be a non-empty policy version id",
        );
    }

    #[tokio::test]
    async fn rollback_policy_version_missing_policy_set_id_carries_field_violation() {
        let err = svc()
            .rollback_policy_version(Request::new(authz_pb::RollbackPolicyVersionRequest {
                actor: Some(admin_actor()),
                policy_set_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing policy_set_id must fail before policy-set loading");

        assert_eq!(err.message(), "policy_set_id is required");
        assert_validation_field(&err, "policy_set_id", "must be a non-empty policy set id");
    }

    #[tokio::test]
    async fn activate_canary_missing_policy_version_id_carries_field_violation() {
        let err = svc()
            .activate_canary(Request::new(authz_pb::ActivateCanaryRequest {
                actor: Some(admin_actor()),
                policy_version_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing policy_version_id must fail before version loading");

        assert_eq!(err.message(), "policy_version_id is required");
        assert_validation_field(
            &err,
            "policy_version_id",
            "must be a non-empty policy version id",
        );
    }

    #[tokio::test]
    async fn promote_canary_missing_canary_id_carries_field_violation() {
        let err = svc()
            .promote_canary(Request::new(authz_pb::PromoteCanaryRequest {
                actor: Some(admin_actor()),
                canary_id: " ".to_string(),
                ..Default::default()
            }))
            .await
            .expect_err("missing canary_id must fail before canary loading");

        assert_eq!(err.message(), "canary_id is required");
        assert_validation_field(&err, "canary_id", "must be a non-empty policy canary id");
    }

    #[tokio::test]
    async fn get_canary_status_missing_canary_id_carries_field_violation() {
        let err = svc()
            .get_canary_status(Request::new(authz_pb::GetCanaryStatusRequest {
                actor: Some(admin_actor()),
                canary_id: " ".to_string(),
            }))
            .await
            .expect_err("missing canary_id must fail before canary loading");

        assert_eq!(err.message(), "canary_id is required");
        assert_validation_field(&err, "canary_id", "must be a non-empty policy canary id");
    }

    #[test]
    fn activation_lifecycle_denials_carry_policy_detail() {
        assert_policy_detail(
            &policy_version_not_activatable_status(authz_entity_pb::PolicyVersionState::Draft),
            "policy_version_activate",
            "policy_version_not_activatable",
            "only an approved (or previously-active) version may be activated; version is Draft",
        );
        assert_policy_detail(
            &rollback_target_required_status(),
            "policy_version_rollback",
            "rollback_target_required",
            "no rollback target: supply target_version_id or activate a second version first",
        );
        assert_policy_detail(
            &policy_version_not_canariable_status(authz_entity_pb::PolicyVersionState::Rejected),
            "policy_canary_activate",
            "policy_version_not_canariable",
            "only an approved (or previously-active) version may be canaried; version is Rejected",
        );
        assert_policy_detail(
            &canary_not_active_status(authz_entity_pb::CanaryState::Paused),
            "policy_canary_promote",
            "canary_not_active",
            "only an ACTIVE canary can be promoted; canary is Paused",
        );
        assert_policy_detail(
            &canary_not_promote_eligible_status(),
            "policy_canary_promote",
            "canary_not_promote_eligible",
            "canary is not promote-eligible yet: its success window has not elapsed",
        );
    }

    #[test]
    fn duplicate_policy_id_validation_carries_field_violation() {
        let document = PolicyDocument {
            policies: vec![
                AuthzPolicy {
                    id: "11111111-1111-1111-1111-111111111111".to_string(),
                    ..Default::default()
                },
                AuthzPolicy {
                    id: "11111111-1111-1111-1111-111111111111".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let err = validate_unique_document_policy_ids(&document)
            .expect_err("duplicate policy IDs must fail before activation writes");
        assert_eq!(
            err.message(),
            "duplicate policy id in version document: 11111111-1111-1111-1111-111111111111"
        );
        assert_validation_field(
            &err,
            "policies.id",
            "duplicate policy id 11111111-1111-1111-1111-111111111111",
        );
    }

    #[test]
    fn canary_scope_validation_carries_field_violations() {
        let empty_nodes = validate_canary_scope(&CanaryScope::Nodes(Vec::new()))
            .expect_err("empty node canary scope must fail");
        assert_eq!(
            empty_nodes.message(),
            "canary scope_values must be non-empty for NODE/TENANT scope"
        );
        assert_validation_field(
            &empty_nodes,
            "scope_values",
            "must be non-empty for NODE or TENANT canary scope",
        );

        let percent_zero = validate_canary_scope(&CanaryScope::Percent(0))
            .expect_err("zero-percent canary scope must fail");
        assert_eq!(
            percent_zero.message(),
            "canary PERCENT scope must be 1..=100 (0 includes nobody)"
        );
        assert_validation_field(
            &percent_zero,
            "scope_percent",
            "must be in the range 1..=100",
        );
    }
}
