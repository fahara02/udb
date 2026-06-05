//! `AuthzService` handler over an atomically-swappable [`AuthzSnapshot`], with
//! an optional Postgres-backed policy/role-binding/relationship store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::authz::entity::v1 as authz_entity_pb;
use crate::proto::udb::core::authz::services::v1 as authz_pb;
use authz_pb::authz_service_server::AuthzService;

use super::mappings::{
    authz_principal_to_runtime, decision_to_pb, effect_to_entity, entity_effect_to_runtime,
    page_response, policy_from_pg_row, policy_to_rule_pb, resource_to_runtime, scopes_to_db,
    timestamp_from_unix,
};
use super::now_unix;
use crate::runtime::authz::Effect;
use crate::runtime::authz::{
    Authorizer, AuthzPolicy, AuthzQuery, AuthzSnapshot, Decision, Principal, RelationshipTuple,
    ResourceRef, RoleBinding,
};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::events::{self, AuthEvent, AuthEventSink, topics};

// Topic submodules: inherent `impl AuthzServiceImpl` blocks (+ helpers); the
// single `impl AuthzService` trait block below delegates to them.
mod audit;
mod tuples;

fn policy_rule_model() -> NativeModel {
    native_model(
        "udb.core.authz.entity.v1.PolicyRule",
        &[
            "policy_id",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "condition",
            "description",
            "is_active",
            "created_by",
            "deleted_at",
            "tenant_id",
            "deleted_by",
            "project_id",
            "resource_type",
            "attributes_json",
        ],
    )
}

fn policy_tuple_model() -> NativeModel {
    native_model(
        "udb.core.authz.entity.v1.PolicyTuple",
        &[
            "tuple_kind",
            "subject",
            "domain",
            "object",
            "action",
            "effect",
            "condition",
            "tenant_id",
            "project_id",
        ],
    )
}

fn role_model() -> NativeModel {
    native_model(
        "udb.core.authz.entity.v1.Role",
        &[
            "role_id",
            "name",
            "description",
            "is_system",
            "is_active",
            "created_by",
            "deleted_at",
            "tenant_id",
            "deleted_by",
            "role_code",
            "domain",
            "project_id",
            "scope_type",
            "access_surface",
            "metadata_json",
        ],
    )
}

fn user_role_model() -> NativeModel {
    native_model(
        "udb.core.authz.entity.v1.UserRole",
        &[
            "user_role_id",
            "user_id",
            "role_id",
            "domain",
            "assigned_by",
            "expires_at",
            "created_by",
            "tenant_id",
        ],
    )
}

fn access_decision_audit_model() -> NativeModel {
    native_model(
        "udb.core.authz.entity.v1.AccessDecisionAudit",
        &[
            "decision_audit_id",
            "user_id",
            "domain",
            "object",
            "action",
            "effect",
            "decision_source",
            "matched_rule",
            "reason",
            "ip_address",
            "correlation_id",
            "decided_at",
            "tenant_id",
        ],
    )
}

fn role_select_projection(model: &NativeModel) -> String {
    [
        model.text("role_id"),
        model.select("name"),
        model.text_or_empty("description"),
        model.select("is_system"),
        model.select("is_active"),
        model.text_or_empty("created_by"),
        model.text_or_empty_as("tenant_id", "tenant"),
        model.text_or_empty_as("project_id", "project"),
        model.text_or_empty("role_code"),
        model.text_or_empty("domain"),
        model.select("scope_type"),
        model.text_or_empty("access_surface"),
        model.json_text_as("metadata_json", "metadata_json"),
        model.text_or_empty("deleted_by"),
    ]
    .join(", ")
}

fn user_role_select_projection(model: &NativeModel) -> String {
    [
        model.text("user_role_id"),
        model.text("user_id"),
        model.text("role_id"),
        model.text_or_empty("domain"),
        model.text_or_empty("assigned_by"),
        model.timestamp_unix_as("expires_at", "expires_at_unix"),
        model.text_or_empty_as("tenant_id", "tenant"),
        model.text_or_empty("created_by"),
    ]
    .join(", ")
}

fn policy_rule_select_projection(model: &NativeModel) -> String {
    [
        model.text("policy_id"),
        model.text_or_empty("subject"),
        model.text_or_empty("domain"),
        model.text_or_empty("object"),
        model.text_or_empty("action"),
        model.select("effect"),
        model.text_or_empty("condition"),
        model.text_or_empty("description"),
        model.select("is_active"),
        model.text_or_empty("created_by"),
        model.text_or_empty("tenant_id"),
        model.text_or_empty("deleted_by"),
        model.text_or_empty("project_id"),
        model.text_or_empty("resource_type"),
        model.json_text_as("attributes_json", "attributes_json"),
    ]
    .join(", ")
}

fn stable_audit_user_uuid(principal: &Principal) -> Uuid {
    let subject = [
        principal.subject.as_str(),
        principal.user_id.as_str(),
        principal.principal_id.as_str(),
        principal.service_identity.as_str(),
    ]
    .into_iter()
    .find(|value| !value.trim().is_empty())
    .unwrap_or("anonymous");
    stable_uuid_from_subject(subject)
}

pub(super) fn stable_uuid_from_subject(subject: &str) -> Uuid {
    if let Ok(uuid) = Uuid::parse_str(subject) {
        return uuid;
    }
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(subject.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

pub(super) fn parse_uuid_field(field_name: &str, value: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(value)
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a UUID")))
}

pub(super) fn timestamp_unix_field(
    field_name: &str,
    value: Option<prost_types::Timestamp>,
) -> Result<Option<i64>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.seconds <= 0 {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be a positive unix timestamp"
        )));
    }
    Ok(Some(value.seconds))
}

fn tenant_from_domain(tenant_id: &str, domain: &str) -> String {
    if !tenant_id.trim().is_empty() {
        tenant_id.to_string()
    } else if let Some((prefix, suffix)) = domain.split_once(':') {
        if matches!(prefix, "tenant" | "project" | "resource") && !suffix.trim().is_empty() {
            suffix.to_string()
        } else {
            domain.to_string()
        }
    } else {
        domain.to_string()
    }
}

fn tuple_condition_expired(condition: &str, now: u64) -> bool {
    if condition.trim().is_empty() {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(condition) else {
        return false;
    };
    let Some(expires_at) = value
        .get("expires_at_unix")
        .and_then(serde_json::Value::as_i64)
    else {
        return false;
    };
    expires_at > 0 && (expires_at as u64) <= now
}

/// Item 81: enrich a `ResourceRef` with the concrete manifest schema/table/store
/// resolved from its message type, so policies can match on
/// `schema`/`table`/`backend` and audits record the real relation. Looks the
/// message type up in the native manifest; leaves the ref untouched when it is
/// already populated or the type is not a known native relation.
fn enrich_resource(resource: &mut ResourceRef) {
    if !resource.table.trim().is_empty() {
        return;
    }
    let key = if !resource.message_type.trim().is_empty() {
        resource.message_type.clone()
    } else {
        resource.resource_name.clone()
    };
    if key.trim().is_empty() {
        return;
    }
    if let Some((schema, table)) = crate::runtime::native_catalog::native_relation(&key) {
        resource.schema = schema;
        resource.table = table;
        if resource.backend.trim().is_empty() {
            resource.backend = "postgres".to_string();
        }
    }
}

fn role_scope_type_to_db(scope_type: i32) -> &'static str {
    match authz_entity_pb::RoleScopeType::try_from(scope_type).unwrap_or_default() {
        authz_entity_pb::RoleScopeType::Global => "GLOBAL",
        authz_entity_pb::RoleScopeType::Tenant => "TENANT",
        authz_entity_pb::RoleScopeType::Project => "PROJECT",
        authz_entity_pb::RoleScopeType::Resource => "RESOURCE",
        authz_entity_pb::RoleScopeType::External => "EXTERNAL",
        authz_entity_pb::RoleScopeType::Unspecified => "UNSPECIFIED",
    }
}

fn role_scope_type_from_db(value: &str) -> i32 {
    match value {
        "GLOBAL" | "ROLE_SCOPE_TYPE_GLOBAL" => authz_entity_pb::RoleScopeType::Global as i32,
        "TENANT" | "ROLE_SCOPE_TYPE_TENANT" => authz_entity_pb::RoleScopeType::Tenant as i32,
        "PROJECT" | "ROLE_SCOPE_TYPE_PROJECT" => authz_entity_pb::RoleScopeType::Project as i32,
        "RESOURCE" | "ROLE_SCOPE_TYPE_RESOURCE" => authz_entity_pb::RoleScopeType::Resource as i32,
        "EXTERNAL" | "ROLE_SCOPE_TYPE_EXTERNAL" => authz_entity_pb::RoleScopeType::External as i32,
        _ => authz_entity_pb::RoleScopeType::Unspecified as i32,
    }
}

fn effect_to_db(effect: Effect) -> &'static str {
    match effect {
        Effect::Allow => "ALLOW",
        Effect::Deny => "DENY",
    }
}

pub(super) fn effect_from_db(value: &str) -> i32 {
    match value {
        "ALLOW" | "allow" | "POLICY_EFFECT_ALLOW" => authz_entity_pb::PolicyEffect::Allow as i32,
        "DENY" | "deny" | "POLICY_EFFECT_DENY" => authz_entity_pb::PolicyEffect::Deny as i32,
        _ => authz_entity_pb::PolicyEffect::Unspecified as i32,
    }
}

/// Map a `roles` row to the `Role` entity. Timestamps are not read back
/// (left `None`); the durable columns carry the role's logical state.
fn role_from_row(row: &sqlx::postgres::PgRow) -> Result<authz_entity_pb::Role, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode role failed: {e}"));
    Ok(authz_entity_pb::Role {
        role_id: row.try_get("role_id").map_err(map)?,
        name: row.try_get("name").map_err(map)?,
        description: row.try_get("description").map_err(map)?,
        is_system: row.try_get("is_system").map_err(map)?,
        is_active: row.try_get("is_active").map_err(map)?,
        created_by: row.try_get("created_by").map_err(map)?,
        created_at: None,
        updated_at: None,
        deleted_at: None,
        tenant_id: row.try_get("tenant").map_err(map)?,
        deleted_by: row.try_get("deleted_by").map_err(map)?,
        role_code: row.try_get("role_code").map_err(map)?,
        domain: row.try_get("domain").map_err(map)?,
        project_id: row.try_get("project").map_err(map)?,
        scope_type: role_scope_type_from_db(&row.try_get::<String, _>("scope_type").map_err(map)?),
        access_surface: row.try_get("access_surface").map_err(map)?,
        metadata_json: row.try_get("metadata_json").map_err(map)?,
    })
}

/// Map a `user_roles` row to the `UserRole` entity.
fn user_role_from_row(row: &sqlx::postgres::PgRow) -> Result<authz_entity_pb::UserRole, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode user role failed: {e}"));
    Ok(authz_entity_pb::UserRole {
        user_role_id: row.try_get("user_role_id").map_err(map)?,
        user_id: row.try_get("user_id").map_err(map)?,
        role_id: row.try_get("role_id").map_err(map)?,
        domain: row.try_get("domain").map_err(map)?,
        assigned_by: row.try_get("assigned_by").map_err(map)?,
        assigned_at: None,
        expires_at: timestamp_from_unix(
            row.try_get::<i64, _>("expires_at_unix")
                .map_err(map)?
                .max(0) as u64,
        ),
        created_at: None,
        updated_at: None,
        created_by: row.try_get("created_by").map_err(map)?,
        tenant_id: row.try_get("tenant").map_err(map)?,
    })
}

/// Map a `policy_rules` row to the `PolicyRule` entity without going through
/// the evaluation snapshot. The snapshot intentionally drops management fields
/// such as description/deleted_by; admin read APIs must return the durable row.
fn policy_rule_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<authz_entity_pb::PolicyRule, Status> {
    let map = |e: sqlx::Error| Status::internal(format!("decode policy rule failed: {e}"));
    Ok(authz_entity_pb::PolicyRule {
        policy_id: row.try_get("policy_id").map_err(map)?,
        subject: row.try_get("subject").map_err(map)?,
        domain: row.try_get("domain").map_err(map)?,
        object: row.try_get("object").map_err(map)?,
        action: row.try_get("action").map_err(map)?,
        effect: effect_from_db(&row.try_get::<String, _>("effect").map_err(map)?),
        condition: row.try_get("condition").map_err(map)?,
        description: row.try_get("description").map_err(map)?,
        is_active: row.try_get("is_active").map_err(map)?,
        created_by: row.try_get("created_by").map_err(map)?,
        created_at: None,
        updated_at: None,
        deleted_at: None,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        deleted_by: row.try_get("deleted_by").map_err(map)?,
        project_id: row.try_get("project_id").map_err(map)?,
        resource_type: row.try_get("resource_type").map_err(map)?,
        attributes_json: row.try_get("attributes_json").map_err(map)?,
    })
}

/// `AuthzService` handler over an atomically-swappable [`AuthzSnapshot`].
pub struct AuthzServiceImpl {
    snapshot: Arc<ArcSwap<AuthzSnapshot>>,
    snapshot_loaded_at: Arc<Mutex<Option<Instant>>>,
    snapshot_ttl: Duration,
    pg_pool: Option<PgPool>,
    event_sink: Arc<dyn AuthEventSink>,
}

impl AuthzServiceImpl {
    pub fn new(snapshot: AuthzSnapshot) -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
            snapshot_loaded_at: Arc::new(Mutex::new(Some(Instant::now()))),
            snapshot_ttl: authz_snapshot_ttl(),
            pg_pool: None,
            event_sink: events::noop_sink(),
        }
    }

    /// Share an externally-owned snapshot cell (so reloads are visible here).
    pub fn shared(snapshot: Arc<ArcSwap<AuthzSnapshot>>) -> Self {
        Self {
            snapshot,
            snapshot_loaded_at: Arc::new(Mutex::new(None)),
            snapshot_ttl: authz_snapshot_ttl(),
            pg_pool: None,
            event_sink: events::noop_sink(),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        let has_pool = pool.is_some();
        self.pg_pool = pool;
        // A durable Postgres pool is the source of truth. The constructor seeds
        // `snapshot_loaded_at = Some(now)` over the (possibly empty) in-memory
        // snapshot, which would otherwise be served as "fresh" for the whole TTL
        // and suppress the first load from Postgres. Force the next read to load
        // the real snapshot from PG.
        if has_pool {
            self.invalidate_snapshot_cache();
        }
        self
    }

    /// Invalidate the cached authz snapshot so the next `current_snapshot()`
    /// reloads from Postgres. Called after every authz mutation so the writing
    /// node enforces its own writes immediately (read-your-writes) instead of
    /// serving a stale snapshot until the TTL elapses.
    pub(super) fn invalidate_snapshot_cache(&self) {
        if let Ok(mut guard) = self.snapshot_loaded_at.lock() {
            *guard = None;
        }
    }

    pub(crate) fn with_event_sink(mut self, sink: Arc<dyn AuthEventSink>) -> Self {
        self.event_sink = sink;
        self
    }

    pub(super) async fn emit_event(&self, event: AuthEvent) {
        let topic = event.topic;
        if let Err(err) = self.event_sink.emit(event).await {
            tracing::warn!(topic, error = %err, "failed to publish authz event");
        }
    }

    pub(super) async fn decide_with_snapshot(
        &self,
        snapshot: &AuthzSnapshot,
        principal: &Principal,
        resource: &ResourceRef,
        action: &str,
        purpose: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Decision {
        // Drive the decision through a real Casbin enforcer running the PERM model
        // (`runtime::authz::casbin_engine`).
        snapshot
            .casbin_authorize(&AuthzQuery {
                principal,
                resource,
                action,
                purpose,
                attributes,
            })
            .await
    }

    pub(super) fn policies_model(&self) -> NativeModel {
        policy_rule_model()
    }

    pub(super) fn relationship_tuples_model(&self) -> NativeModel {
        policy_tuple_model()
    }

    pub(super) fn roles_model(&self) -> NativeModel {
        role_model()
    }

    pub(super) fn user_roles_model(&self) -> NativeModel {
        user_role_model()
    }

    pub(super) fn audits_model(&self) -> NativeModel {
        access_decision_audit_model()
    }

    /// Role/assignment/audit management is durable-only: fail closed when no
    /// Postgres pool is configured.
    pub(super) fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this operation requires a Postgres-backed auth store (no PG pool configured)",
            )
        })
    }

    /// Best-effort access-decision audit write (items 84–86): records denies and
    /// audit-flagged allows into the proto-defined `access_decision_audits`
    /// table. Errors are logged, never surfaced — auditing must not block the
    /// decision path. The UUID `user_id` column accepts any principal subject by
    /// deriving a stable UUID from it (service/external identities aren't UUIDs).
    pub(super) async fn write_decision_audit(
        &self,
        principal: &Principal,
        resource: &ResourceRef,
        action: &str,
        decision: &Decision,
    ) {
        let Some(pool) = &self.pg_pool else {
            return;
        };
        if decision.allowed && !decision.audit_required {
            return;
        }
        let audit = self.audits_model();
        let rel = audit.relation.clone();
        let user_uuid = stable_audit_user_uuid(principal);
        let effect = if decision.allowed { "ALLOW" } else { "DENY" };
        let source = if decision.matched_policy_ids.is_empty() {
            "NO_MATCH"
        } else if decision.allowed && decision.via_role {
            "ROLE_POLICY"
        } else {
            "DIRECT_POLICY"
        };
        let domain = if principal.project_id.trim().is_empty() {
            principal.tenant_id.clone()
        } else {
            principal.project_id.clone()
        };
        let result = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({decision_audit_id}, {user_id}, {domain_col}, {object_col}, {action_col}, {effect_col}, {decision_source}, {matched_rule}, {reason}, {ip_address}, {correlation_id}, {tenant_id}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '', '', $10)",
            decision_audit_id = audit.q("decision_audit_id"),
            user_id = audit.q("user_id"),
            domain_col = audit.q("domain"),
            object_col = audit.q("object"),
            action_col = audit.q("action"),
            effect_col = audit.q("effect"),
            decision_source = audit.q("decision_source"),
            matched_rule = audit.q("matched_rule"),
            reason = audit.q("reason"),
            ip_address = audit.q("ip_address"),
            correlation_id = audit.q("correlation_id"),
            tenant_id = audit.q("tenant_id"),
        ))
        .bind(Uuid::new_v4())
        .bind(user_uuid)
        .bind(domain)
        .bind(&resource.resource_name)
        .bind(action)
        .bind(effect)
        .bind(source)
        .bind(decision.matched_policy_ids.first().cloned().unwrap_or_default())
        .bind(&decision.deny_reason)
        .bind(&principal.tenant_id)
        .execute(pool)
        .await;
        if let Err(err) = result {
            tracing::warn!(error = %err, "failed to write access-decision audit");
        }
    }

    /// Native authz reads and mutations require a Postgres-backed store. There is
    /// no in-memory fallback, so a missing pool fails closed.
    pub(super) fn require_snapshot_fallback(&self) -> Result<(), Status> {
        Err(Status::failed_precondition(
            "native authz requires a Postgres-backed auth store",
        ))
    }

    async fn load_snapshot_from_postgres(&self) -> Result<Option<AuthzSnapshot>, Status> {
        let Some(pool) = &self.pg_pool else {
            return Ok(None);
        };
        let policy = self.policies_model();
        let binding = self.user_roles_model();
        let role = self.roles_model();
        let tuple = self.relationship_tuples_model();

        let policy_rows = sqlx::query(&format!(
            "SELECT {policy_id_text} AS id, COALESCE(NULLIF({attributes_json}->>'priority', '')::INT, 0) AS priority, {is_active} AS enabled, {effect}, {tenant_id} AS tenant, COALESCE({project_id}, '') AS project, {subject}, \
                    COALESCE({attributes_json}->>'role', '') AS role, {action}, {object_col} AS resource, COALESCE({attributes_json}->>'purpose', '') AS purpose, \
                    COALESCE({attributes_json}->>'relationship', '') AS relationship, {attributes_json} AS conditions, COALESCE({attributes_json}->>'required_scopes', '') AS required_scopes \
             FROM {policy_rel} \
             WHERE {deleted_at} IS NULL AND {is_active} = TRUE \
             ORDER BY priority DESC, {policy_id} ASC",
            policy_rel = policy.relation.clone(),
            policy_id_text = format!("{}::TEXT", policy.q("policy_id")),
            policy_id = policy.q("policy_id"),
            attributes_json = policy.q("attributes_json"),
            is_active = policy.q("is_active"),
            effect = policy.q("effect"),
            tenant_id = policy.q("tenant_id"),
            project_id = policy.q("project_id"),
            subject = policy.q("subject"),
            action = policy.q("action"),
            object_col = policy.q("object"),
            deleted_at = policy.q("deleted_at"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("load authz policies failed: {err}")))?;

        let mut policies = Vec::with_capacity(policy_rows.len());
        for row in &policy_rows {
            policies.push(
                policy_from_pg_row(row).map_err(|err| {
                    Status::internal(format!("decode authz policy failed: {err}"))
                })?,
            );
        }

        let binding_rows = sqlx::query(&format!(
            "SELECT ur.{user_id}::TEXT AS subject, COALESCE(NULLIF(r.{role_code}, ''), NULLIF(r.{name}, ''), ur.{role_id}::TEXT) AS role, ur.{tenant_id} AS tenant, COALESCE(r.{project_id}, '') AS project \
             FROM {binding_rel} ur \
             LEFT JOIN {role_rel} r ON r.{role_role_id} = ur.{role_id} \
             WHERE (ur.{expires_at} IS NULL OR ur.{expires_at} > NOW()) \
               AND r.{deleted_at} IS NULL \
               AND (r.{is_active} IS NULL OR r.{is_active} = TRUE)",
            binding_rel = binding.relation.clone(),
            role_rel = role.relation.clone(),
            user_id = binding.q("user_id"),
            role_id = binding.q("role_id"),
            tenant_id = binding.q("tenant_id"),
            expires_at = binding.q("expires_at"),
            role_role_id = role.q("role_id"),
            role_code = role.q("role_code"),
            name = role.q("name"),
            project_id = role.q("project_id"),
            deleted_at = role.q("deleted_at"),
            is_active = role.q("is_active"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("load role bindings failed: {err}")))?;
        let mut role_bindings = Vec::with_capacity(binding_rows.len());
        for row in binding_rows {
            role_bindings.push(RoleBinding {
                subject: row.try_get("subject").map_err(|err| {
                    Status::internal(format!("decode role binding failed: {err}"))
                })?,
                role: row.try_get("role").map_err(|err| {
                    Status::internal(format!("decode role binding failed: {err}"))
                })?,
                tenant: row.try_get("tenant").map_err(|err| {
                    Status::internal(format!("decode role binding failed: {err}"))
                })?,
                project: row.try_get("project").map_err(|err| {
                    Status::internal(format!("decode role binding failed: {err}"))
                })?,
            });
        }

        let grouping_rows = sqlx::query(&format!(
            "SELECT {subject}, {action} AS role, {tenant_id} AS tenant, COALESCE({project_id}, '') AS project, {condition} \
             FROM {tuple_rel} \
             WHERE {tuple_kind} = 'grouping'",
            tuple_rel = tuple.relation.clone(),
            subject = tuple.q("subject"),
            action = tuple.q("action"),
            tenant_id = tuple.q("tenant_id"),
            project_id = tuple.q("project_id"),
            condition = tuple.q("condition"),
            tuple_kind = tuple.q("tuple_kind"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("load grouping tuples failed: {err}")))?;
        let now = now_unix();
        for row in grouping_rows {
            let condition: String = row
                .try_get("condition")
                .map_err(|err| Status::internal(format!("decode grouping tuple failed: {err}")))?;
            if tuple_condition_expired(&condition, now) {
                continue;
            }
            role_bindings.push(RoleBinding {
                subject: row.try_get("subject").map_err(|err| {
                    Status::internal(format!("decode grouping tuple failed: {err}"))
                })?,
                role: row.try_get("role").map_err(|err| {
                    Status::internal(format!("decode grouping tuple failed: {err}"))
                })?,
                tenant: row.try_get("tenant").map_err(|err| {
                    Status::internal(format!("decode grouping tuple failed: {err}"))
                })?,
                project: row.try_get("project").map_err(|err| {
                    Status::internal(format!("decode grouping tuple failed: {err}"))
                })?,
            });
        }

        let tuple_rows = sqlx::query(&format!(
            "SELECT {subject}, {action} AS relation, {object_col}, {tenant_id} AS tenant, COALESCE({project_id}, '') AS project, {condition} FROM {tuple_rel} \
             WHERE {tuple_kind} = 'relationship'",
            tuple_rel = tuple.relation.clone(),
            subject = tuple.q("subject"),
            action = tuple.q("action"),
            object_col = tuple.q("object"),
            tenant_id = tuple.q("tenant_id"),
            project_id = tuple.q("project_id"),
            condition = tuple.q("condition"),
            tuple_kind = tuple.q("tuple_kind"),
        ))
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("load relationship tuples failed: {err}")))?;
        let mut tuples = Vec::with_capacity(tuple_rows.len());
        for row in tuple_rows {
            let condition: String = row.try_get("condition").map_err(|err| {
                Status::internal(format!("decode relationship tuple failed: {err}"))
            })?;
            if tuple_condition_expired(&condition, now) {
                continue;
            }
            tuples.push(RelationshipTuple {
                subject: row.try_get("subject").map_err(|err| {
                    Status::internal(format!("decode relationship tuple failed: {err}"))
                })?,
                relation: row.try_get("relation").map_err(|err| {
                    Status::internal(format!("decode relationship tuple failed: {err}"))
                })?,
                object: row.try_get("object").map_err(|err| {
                    Status::internal(format!("decode relationship tuple failed: {err}"))
                })?,
                tenant: row.try_get("tenant").map_err(|err| {
                    Status::internal(format!("decode relationship tuple failed: {err}"))
                })?,
                project: row.try_get("project").map_err(|err| {
                    Status::internal(format!("decode relationship tuple failed: {err}"))
                })?,
            });
        }

        Ok(Some(AuthzSnapshot {
            version: format!("pg-{}", policies.len()),
            relationship_version: format!("pg-{}", tuples.len()),
            policies,
            role_bindings,
            tuples,
            default_allow: false,
        }))
    }

    pub(super) async fn current_snapshot(&self) -> Result<Arc<AuthzSnapshot>, Status> {
        let cached_is_fresh = self
            .snapshot_loaded_at
            .lock()
            .map(|guard| {
                guard
                    .map(|t| t.elapsed() < self.snapshot_ttl)
                    .unwrap_or(false)
            })
            .unwrap_or(false);
        if cached_is_fresh {
            return Ok(self.snapshot.load_full());
        }
        if let Some(snapshot) = self.load_snapshot_from_postgres().await? {
            self.snapshot.store(Arc::new(snapshot));
            if let Ok(mut guard) = self.snapshot_loaded_at.lock() {
                *guard = Some(Instant::now());
            }
            return Ok(self.snapshot.load_full());
        }
        self.require_snapshot_fallback()?;
        if let Ok(mut guard) = self.snapshot_loaded_at.lock() {
            *guard = Some(Instant::now());
        }
        Ok(self.snapshot.load_full())
    }
}

fn authz_snapshot_ttl() -> Duration {
    let secs = std::env::var("UDB_AUTHZ_SNAPSHOT_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(5)
        .max(1);
    Duration::from_secs(secs)
}

#[tonic::async_trait]
impl AuthzService for AuthzServiceImpl {
    async fn authorize(
        &self,
        request: Request<authz_pb::AuthzRequest>,
    ) -> Result<Response<authz_pb::AuthzResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let mut principal = req
            .principal
            .as_ref()
            .map(authz_principal_to_runtime)
            .unwrap_or_default();
        if principal.tenant_id.trim().is_empty() {
            principal.tenant_id = if req.tenant_id.trim().is_empty() {
                req.domain.clone()
            } else {
                req.tenant_id.clone()
            };
        }
        if principal.project_id.trim().is_empty() {
            principal.project_id = req.project_id.clone();
        }
        if principal.subject.trim().is_empty() {
            principal.subject = if !principal.user_id.trim().is_empty() {
                principal.user_id.clone()
            } else {
                principal.principal_id.clone()
            };
        }
        if principal.principal_id.trim().is_empty() {
            principal.principal_id = principal.subject.clone();
        }
        let mut resource = req
            .resource
            .as_ref()
            .map(resource_to_runtime)
            .unwrap_or_default();
        enrich_resource(&mut resource);
        let mut attributes: BTreeMap<String, String> = req.attributes.into_iter().collect();
        if let Some(ctx) = req.context {
            attributes.extend(ctx.attributes.into_iter());
        }
        let snap = self.current_snapshot().await?;
        let decision = self
            .decide_with_snapshot(
                &snap,
                &principal,
                &resource,
                &req.action,
                &req.purpose,
                &attributes,
            )
            .await;
        // Milestone 6: decision-evaluation latency.
        tracing::debug!(
            decision_id = %decision.decision_id,
            allowed = decision.allowed,
            action = %req.action,
            latency_us = started.elapsed().as_micros() as u64,
            "authz decision",
        );
        // Items 84–86: persist denies / audit-flagged allows to the proto audit table.
        self.write_decision_audit(&principal, &resource, &req.action, &decision)
            .await;
        // Publish a denial event so security dashboards (Kafka → Spark) see
        // deny rates in near-real-time, alongside the durable audit row.
        if !decision.allowed {
            let subject = if principal.user_id.trim().is_empty() {
                principal.subject.clone()
            } else {
                principal.user_id.clone()
            };
            self.emit_event(
                AuthEvent::new(
                    topics::ACCESS_DENIED,
                    subject.clone(),
                    principal.tenant_id.clone(),
                    serde_json::json!({
                        "user_id": principal.user_id.clone(),
                        "subject": subject,
                        "tenant_id": principal.tenant_id.clone(),
                        "resource": resource.resource_name.clone(),
                        "action": req.action.clone(),
                        "deny_reason": decision.deny_reason.clone(),
                        "decision_id": decision.decision_id.clone(),
                    }),
                )
                .with_correlation(decision.decision_id.clone()),
            )
            .await;
        }
        Ok(Response::new(authz_pb::AuthzResponse {
            decision: Some(decision_to_pb(&decision)),
        }))
    }

    async fn put_role_binding(
        &self,
        request: Request<authz_pb::PutRoleBindingRequest>,
    ) -> Result<Response<authz_pb::AuthMutationResponse>, Status> {
        self.put_role_binding_impl(request).await
    }

    async fn put_relationship(
        &self,
        request: Request<authz_pb::PutRelationshipRequest>,
    ) -> Result<Response<authz_pb::AuthMutationResponse>, Status> {
        self.put_relationship_impl(request).await
    }

    async fn put_authz_policy(
        &self,
        request: Request<authz_pb::PutAuthzPolicyRequest>,
    ) -> Result<Response<authz_pb::AuthMutationResponse>, Status> {
        let p = request
            .into_inner()
            .policy
            .ok_or_else(|| Status::invalid_argument("policy is required"))?;
        if p.id.trim().is_empty() {
            return Err(Status::invalid_argument("policy id is required"));
        }
        // Reject unknown effect strings rather than silently defaulting to Allow:
        // a typo'd effect must never become a permissive policy.
        let effect = if p.effect.eq_ignore_ascii_case("deny") {
            Effect::Deny
        } else if p.effect.eq_ignore_ascii_case("allow") {
            Effect::Allow
        } else {
            return Err(Status::invalid_argument(format!(
                "policy effect must be 'allow' or 'deny', got '{}'",
                p.effect
            )));
        };
        let policy = AuthzPolicy {
            id: p.id,
            priority: p.priority,
            enabled: p.enabled,
            effect,
            tenant: p.tenant,
            project: p.project,
            subject: p.subject,
            role: p.role,
            action: p.action,
            resource: p.resource,
            purpose: p.purpose,
            relationship: p.relationship,
            conditions: p.conditions.into_iter().collect(),
            required_scopes: p.required_scopes,
        };
        if let Some(pool) = &self.pg_pool {
            let policy_id = parse_uuid_field("policy.id", &policy.id)?;
            let policy_model = self.policies_model();
            let rel = policy_model.relation.clone();
            let mut attributes = serde_json::Map::new();
            for (key, value) in &policy.conditions {
                attributes.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
            attributes.insert(
                "priority".to_string(),
                serde_json::Value::String(policy.priority.to_string()),
            );
            attributes.insert(
                "role".to_string(),
                serde_json::Value::String(policy.role.clone()),
            );
            attributes.insert(
                "purpose".to_string(),
                serde_json::Value::String(policy.purpose.clone()),
            );
            attributes.insert(
                "relationship".to_string(),
                serde_json::Value::String(policy.relationship.clone()),
            );
            attributes.insert(
                "required_scopes".to_string(),
                serde_json::Value::String(scopes_to_db(&policy.required_scopes)),
            );
            sqlx::query(&format!(
                "INSERT INTO {rel} \
                 ({policy_id}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {description}, {is_active}, {tenant_id}, {project_id}, {attributes_json}) \
                 VALUES ($1::UUID, $2, $3, $4, $5, $6, '', '', $7, $3, $8, $9::JSONB) \
                 ON CONFLICT ({policy_id}) DO UPDATE SET \
                   {subject} = EXCLUDED.{subject}, {domain_col} = EXCLUDED.{domain_col}, {object_col} = EXCLUDED.{object_col}, {action_col} = EXCLUDED.{action_col}, {effect_col} = EXCLUDED.{effect_col}, \
                   {is_active} = EXCLUDED.{is_active}, {tenant_id} = EXCLUDED.{tenant_id}, {project_id} = EXCLUDED.{project_id}, {attributes_json} = EXCLUDED.{attributes_json}",
                policy_id = policy_model.q("policy_id"),
                subject = policy_model.q("subject"),
                domain_col = policy_model.q("domain"),
                object_col = policy_model.q("object"),
                action_col = policy_model.q("action"),
                effect_col = policy_model.q("effect"),
                condition = policy_model.q("condition"),
                description = policy_model.q("description"),
                is_active = policy_model.q("is_active"),
                tenant_id = policy_model.q("tenant_id"),
                project_id = policy_model.q("project_id"),
                attributes_json = policy_model.q("attributes_json"),
            ))
            .bind(policy_id)
            .bind(&policy.subject)
            .bind(&policy.tenant)
            .bind(&policy.resource)
            .bind(&policy.action)
            .bind(effect_to_db(policy.effect))
            .bind(policy.enabled)
            .bind(&policy.project)
            .bind(serde_json::Value::Object(attributes))
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("store authz policy failed: {err}")))?;
        } else {
            self.require_snapshot_fallback()?;
        }
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::AuthMutationResponse {
            ok: true,
            message: "authz policy stored".to_string(),
        }))
    }

    async fn lint_authz_policies(
        &self,
        _request: Request<authz_pb::LintAuthzPoliciesRequest>,
    ) -> Result<Response<authz_pb::LintAuthzPoliciesResponse>, Status> {
        let snap = self.current_snapshot().await?;
        let mut findings = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for p in &snap.policies {
            if p.id.trim().is_empty() {
                findings.push("policy with empty id".to_string());
            } else if !seen.insert(p.id.clone()) {
                findings.push(format!("duplicate policy id: {}", p.id));
            }
            if !p.enabled {
                findings.push(format!("policy {} is disabled and will be ignored", p.id));
            }
            if p.effect == Effect::Allow
                && p.subject.trim().is_empty()
                && p.action.trim().is_empty()
                && p.resource.trim().is_empty()
            {
                findings.push(format!(
                    "policy {} allows any subject/action/resource (overly broad)",
                    p.id
                ));
            }
        }
        Ok(Response::new(authz_pb::LintAuthzPoliciesResponse {
            findings,
        }))
    }

    // Snapshot-backed authz helpers. Role entity CRUD + audits remain DB-backed
    // surfaces for later milestones.

    async fn check_access(
        &self,
        request: Request<authz_pb::CheckAccessRequest>,
    ) -> Result<Response<authz_pb::CheckAccessResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        if req.object.trim().is_empty() {
            return Err(Status::invalid_argument("object is required"));
        }
        if req.action.trim().is_empty() {
            return Err(Status::invalid_argument("action is required"));
        }

        let mut principal = req
            .principal
            .as_ref()
            .map(authz_principal_to_runtime)
            .unwrap_or_default();
        if principal.principal_id.trim().is_empty() {
            principal.principal_id = req.user_id.clone();
        }
        if principal.subject.trim().is_empty() {
            principal.subject = req.user_id.clone();
        }
        if principal.user_id.trim().is_empty() {
            principal.user_id = req.user_id.clone();
        }
        if principal.tenant_id.trim().is_empty() {
            principal.tenant_id = if req.tenant_id.trim().is_empty() {
                req.domain.clone()
            } else {
                req.tenant_id.clone()
            };
        }
        if principal.project_id.trim().is_empty() {
            principal.project_id = req.project_id.clone();
        }

        let mut resource = req
            .resource
            .as_ref()
            .map(resource_to_runtime)
            .unwrap_or_default();
        if resource.resource_name.trim().is_empty() {
            resource.resource_name = req.object.clone();
        }
        if resource.message_type.trim().is_empty() {
            resource.message_type = req.object.clone();
        }
        enrich_resource(&mut resource);

        let mut attributes: BTreeMap<String, String> = req.attributes.into_iter().collect();
        if let Some(ctx) = req.context {
            attributes.extend(ctx.attributes.into_iter());
        }
        let snap = self.current_snapshot().await?;
        let decision = self
            .decide_with_snapshot(
                &snap,
                &principal,
                &resource,
                &req.action,
                &req.purpose,
                &attributes,
            )
            .await;
        self.write_decision_audit(&principal, &resource, &req.action, &decision)
            .await;
        Ok(Response::new(authz_pb::CheckAccessResponse {
            allowed: decision.allowed,
            effect: effect_to_entity(decision.effect),
            matched_rule: decision
                .matched_policy_ids
                .first()
                .cloned()
                .unwrap_or_default(),
            reason: decision.deny_reason.clone(),
            decision: Some(decision_to_pb(&decision)),
        }))
    }
    async fn create_role(
        &self,
        request: Request<authz_pb::CreateRoleRequest>,
    ) -> Result<Response<authz_pb::CreateRoleResponse>, Status> {
        let req = request.into_inner();
        if req.name.trim().is_empty() {
            return Err(Status::invalid_argument("name is required"));
        }
        if req.created_by.trim().is_empty() {
            return Err(Status::invalid_argument("created_by is required"));
        }
        let created_by = parse_uuid_field("created_by", &req.created_by)?;
        let pool = self.require_pool()?;
        let role_model = self.roles_model();
        let rel = role_model.relation.clone();
        let role_id = Uuid::new_v4().to_string();
        let tenant_id = tenant_from_domain(&req.tenant_id, &req.domain);
        if tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id or domain is required"));
        }
        let metadata_json =
            serde_json::to_string(&req.metadata).unwrap_or_else(|_| "{}".to_string());
        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({role_id}, {name}, {description}, {is_system}, {is_active}, {created_by}, {tenant_id}, {project_id}, {role_code}, {domain_col}, {scope_type}, {access_surface}, {metadata_json}) \
             VALUES ($1::UUID, $2, $3, FALSE, TRUE, NULLIF($4, '')::UUID, $5, $6, $7, $8, $9, $10, $11::JSONB)",
            role_id = role_model.q("role_id"),
            name = role_model.q("name"),
            description = role_model.q("description"),
            is_system = role_model.q("is_system"),
            is_active = role_model.q("is_active"),
            created_by = role_model.q("created_by"),
            tenant_id = role_model.q("tenant_id"),
            project_id = role_model.q("project_id"),
            role_code = role_model.q("role_code"),
            domain_col = role_model.q("domain"),
            scope_type = role_model.q("scope_type"),
            access_surface = role_model.q("access_surface"),
            metadata_json = role_model.q("metadata_json"),
        ))
        .bind(&role_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(created_by.to_string())
        .bind(&tenant_id)
        .bind(&req.project_id)
        .bind(&req.role_code)
        .bind(&req.domain)
        .bind(role_scope_type_to_db(req.scope_type))
        .bind(&req.access_surface)
        .bind(&metadata_json)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("create role failed: {err}")))?;
        self.emit_event(AuthEvent::new(
            topics::ROLE_CREATED,
            role_id.clone(),
            tenant_id.clone(),
            serde_json::json!({
                "role_id": role_id.clone(),
                "role_code": req.role_code.clone(),
                "tenant_id": tenant_id.clone(),
                "project_id": req.project_id.clone(),
                "created_by": req.created_by.clone(),
            }),
        ))
        .await;
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::CreateRoleResponse {
            role: Some(authz_entity_pb::Role {
                role_id,
                name: req.name,
                description: req.description,
                is_system: false,
                is_active: true,
                created_by: req.created_by,
                created_at: None,
                updated_at: None,
                deleted_at: None,
                tenant_id,
                deleted_by: String::new(),
                role_code: req.role_code,
                domain: req.domain,
                project_id: req.project_id,
                scope_type: req.scope_type,
                access_surface: req.access_surface,
                metadata_json,
            }),
        }))
    }
    async fn assign_role(
        &self,
        request: Request<authz_pb::AssignRoleRequest>,
    ) -> Result<Response<authz_pb::AssignRoleResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() || req.role_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id and role_id are required"));
        }
        if req.assigned_by.trim().is_empty() {
            return Err(Status::invalid_argument("assigned_by is required"));
        }
        let user_id = parse_uuid_field("user_id", &req.user_id)?;
        let role_id = parse_uuid_field("role_id", &req.role_id)?;
        let assigned_by = parse_uuid_field("assigned_by", &req.assigned_by)?;
        let expires_at_unix = timestamp_unix_field("expires_at", req.expires_at.clone())?;
        let expires_at_bind = expires_at_unix.map(|seconds| seconds as f64);
        let tenant_id = tenant_from_domain(&req.tenant_id, &req.domain);
        if tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id or domain is required"));
        }
        let pool = self.require_pool()?;
        let user_role_model = self.user_roles_model();
        let rel = user_role_model.relation.clone();
        let new_user_role_id = Uuid::new_v4().to_string();
        // Idempotent: re-assigning an already-held (user, role, domain) binding
        // refreshes its assigner/tenant/expiry instead of raising the
        // `uq_user_roles_user_role_domain` unique violation (which previously
        // surfaced as a 500). RETURNING yields the row's real id (existing on
        // conflict, new on insert).
        let row = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({user_role_id}, {user_id}, {role_id}, {domain_col}, {assigned_by}, {tenant_id}, {created_by}, {expires_at}) \
             VALUES ($1::UUID, $2::UUID, $3::UUID, $4, $5::UUID, $6, $7, CASE WHEN $8::DOUBLE PRECISION IS NULL OR $8 <= 0.0 THEN NULL ELSE to_timestamp($8) END) \
             ON CONFLICT ({user_id}, {role_id}, {domain_col}) DO UPDATE SET \
               {assigned_by} = EXCLUDED.{assigned_by}, \
               {tenant_id} = EXCLUDED.{tenant_id}, \
               {created_by} = EXCLUDED.{created_by}, \
               {expires_at} = EXCLUDED.{expires_at} \
             RETURNING {user_role_id}",
            user_role_id = user_role_model.q("user_role_id"),
            user_id = user_role_model.q("user_id"),
            role_id = user_role_model.q("role_id"),
            domain_col = user_role_model.q("domain"),
            assigned_by = user_role_model.q("assigned_by"),
            tenant_id = user_role_model.q("tenant_id"),
            created_by = user_role_model.q("created_by"),
            expires_at = user_role_model.q("expires_at"),
        ))
        .bind(&new_user_role_id)
        .bind(user_id)
        .bind(role_id)
        .bind(&req.domain)
        .bind(assigned_by)
        .bind(&tenant_id)
        .bind(&req.assigned_by)
        .bind(expires_at_bind)
        .fetch_one(pool)
        .await
        .map_err(|err| Status::internal(format!("assign role failed: {err}")))?;
        let user_role_id = row
            .try_get::<Uuid, _>(user_role_model.column("user_role_id"))
            .map(|id| id.to_string())
            .unwrap_or(new_user_role_id);
        self.emit_event(AuthEvent::new(
            topics::ROLE_ASSIGNED,
            req.user_id.clone(),
            tenant_id.clone(),
            serde_json::json!({
                "user_role_id": user_role_id.clone(),
                "user_id": req.user_id.clone(),
                "role_id": req.role_id.clone(),
                "tenant_id": tenant_id.clone(),
                "domain": req.domain.clone(),
                "assigned_by": req.assigned_by.clone(),
            }),
        ))
        .await;
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::AssignRoleResponse {
            user_role: Some(authz_entity_pb::UserRole {
                user_role_id,
                user_id: req.user_id,
                role_id: req.role_id,
                domain: req.domain,
                assigned_by: req.assigned_by.clone(),
                assigned_at: None,
                expires_at: req.expires_at,
                created_at: None,
                updated_at: None,
                created_by: req.assigned_by,
                tenant_id,
            }),
        }))
    }
    async fn create_policy_rule(
        &self,
        request: Request<authz_pb::CreatePolicyRuleRequest>,
    ) -> Result<Response<authz_pb::CreatePolicyRuleResponse>, Status> {
        let req = request.into_inner();
        if req.subject.trim().is_empty() {
            return Err(Status::invalid_argument("subject is required"));
        }
        if req.domain.trim().is_empty() {
            return Err(Status::invalid_argument("domain is required"));
        }
        if req.object.trim().is_empty() {
            return Err(Status::invalid_argument("object is required"));
        }
        if req.action.trim().is_empty() {
            return Err(Status::invalid_argument("action is required"));
        }
        if req.created_by.trim().is_empty() {
            return Err(Status::invalid_argument("created_by is required"));
        }
        let created_by = parse_uuid_field("created_by", &req.created_by)?;
        let effect = entity_effect_to_runtime(req.effect)?;
        let policy = AuthzPolicy {
            id: Uuid::new_v4().to_string(),
            priority: 0,
            enabled: true,
            effect,
            tenant: if req.tenant_id.trim().is_empty() {
                req.domain.clone()
            } else {
                req.tenant_id.clone()
            },
            project: req.project_id.clone(),
            subject: req.subject.clone(),
            role: String::new(),
            action: req.action.clone(),
            resource: req.object.clone(),
            purpose: String::new(),
            relationship: String::new(),
            conditions: req.attributes.into_iter().collect(),
            required_scopes: Vec::new(),
        };
        let policy_rule = policy_to_rule_pb(&policy);
        if let Some(pool) = &self.pg_pool {
            let policy_model = self.policies_model();
            let rel = policy_model.relation.clone();
            let attributes = serde_json::to_value(&policy.conditions).map_err(|err| {
                Status::internal(format!("encode policy conditions failed: {err}"))
            })?;
            sqlx::query(&format!(
                "INSERT INTO {rel} \
                 ({policy_id}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {description}, {is_active}, {created_by}, {tenant_id}, {project_id}, {resource_type}, {attributes_json}) \
                 VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, TRUE, $9::UUID, $10, $11, $12, $13::JSONB)",
                policy_id = policy_model.q("policy_id"),
                subject = policy_model.q("subject"),
                domain_col = policy_model.q("domain"),
                object_col = policy_model.q("object"),
                action_col = policy_model.q("action"),
                effect_col = policy_model.q("effect"),
                condition = policy_model.q("condition"),
                description = policy_model.q("description"),
                is_active = policy_model.q("is_active"),
                created_by = policy_model.q("created_by"),
                tenant_id = policy_model.q("tenant_id"),
                project_id = policy_model.q("project_id"),
                resource_type = policy_model.q("resource_type"),
                attributes_json = policy_model.q("attributes_json"),
            ))
            .bind(&policy.id)
            .bind(&policy.subject)
            .bind(&req.domain)
            .bind(&policy.resource)
            .bind(&policy.action)
            .bind(effect_to_db(policy.effect))
            .bind(&req.condition)
            .bind(&req.description)
            .bind(created_by)
            .bind(&policy.tenant)
            .bind(&policy.project)
            .bind(&req.resource_type)
            .bind(attributes)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("create policy rule failed: {err}")))?;
        } else {
            self.require_snapshot_fallback()?;
        }
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::CreatePolicyRuleResponse {
            policy: Some(authz_entity_pb::PolicyRule {
                domain: req.domain,
                description: req.description,
                created_by: req.created_by,
                resource_type: req.resource_type,
                condition: req.condition,
                ..policy_rule
            }),
        }))
    }
    async fn list_user_permissions(
        &self,
        request: Request<authz_pb::ListUserPermissionsRequest>,
    ) -> Result<Response<authz_pb::ListUserPermissionsResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let snap = self.current_snapshot().await?;
        let mut roles = Vec::new();
        for binding in &snap.role_bindings {
            if binding.subject == req.user_id
                && (req.domain.trim().is_empty()
                    || binding.tenant == req.domain
                    || binding.project == req.domain)
                && !roles.contains(&binding.role)
            {
                roles.push(binding.role.clone());
            }
        }
        let mut permissions = Vec::new();
        for policy in &snap.policies {
            if !policy.enabled || policy.effect != Effect::Allow {
                continue;
            }
            let domain_matches = req.domain.trim().is_empty()
                || policy.tenant == req.domain
                || policy.project == req.domain;
            let subject_matches =
                policy.subject.is_empty() || policy.subject == "*" || policy.subject == req.user_id;
            let role_matches =
                !policy.role.trim().is_empty() && roles.iter().any(|r| r == &policy.role);
            if domain_matches && (subject_matches || role_matches) {
                permissions.push(authz_pb::EffectivePermission {
                    object: policy.resource.clone(),
                    action: policy.action.clone(),
                    via_role: if role_matches {
                        policy.role.clone()
                    } else {
                        String::new()
                    },
                    resource_type: String::new(),
                    domain: if policy.tenant.trim().is_empty() {
                        policy.project.clone()
                    } else {
                        policy.tenant.clone()
                    },
                });
            }
        }
        Ok(Response::new(authz_pb::ListUserPermissionsResponse {
            permissions,
        }))
    }
    async fn list_access_decision_audits(
        &self,
        request: Request<authz_pb::ListAccessDecisionAuditsRequest>,
    ) -> Result<Response<authz_pb::ListAccessDecisionAuditsResponse>, Status> {
        self.list_access_decision_audits_impl(request).await
    }
    async fn revoke_role(
        &self,
        request: Request<authz_pb::RevokeRoleRequest>,
    ) -> Result<Response<authz_pb::RevokeRoleResponse>, Status> {
        let req = request.into_inner();
        if req.user_role_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_role_id is required"));
        }
        let user_role_id = parse_uuid_field("user_role_id", &req.user_role_id)?;
        let pool = self.require_pool()?;
        let user_role_model = self.user_roles_model();
        let rel = user_role_model.relation.clone();
        let result = sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {user_role_id} = $1::UUID",
            user_role_id = user_role_model.q("user_role_id"),
        ))
        .bind(user_role_id)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("revoke role failed: {err}")))?;
        let revoked = result.rows_affected() > 0;
        if revoked {
            self.emit_event(AuthEvent::new(
                topics::ROLE_REVOKED,
                req.user_id.clone(),
                String::new(),
                serde_json::json!({
                    "user_role_id": req.user_role_id.clone(),
                    "user_id": req.user_id.clone(),
                    "reason": req.reason.clone(),
                    "revoked_by": req.revoked_by.clone(),
                }),
            ))
            .await;
        }
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::RevokeRoleResponse { revoked }))
    }
    async fn list_user_roles(
        &self,
        request: Request<authz_pb::ListUserRolesRequest>,
    ) -> Result<Response<authz_pb::ListUserRolesResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let user_id = parse_uuid_field("user_id", &req.user_id)?;
        let pool = self.require_pool()?;
        let user_role_model = self.user_roles_model();
        let rel = user_role_model.relation.clone();
        let projection = user_role_select_projection(&user_role_model);
        let rows = sqlx::query(&format!(
            "SELECT {projection} \
             FROM {rel} \
             WHERE {user_id} = $1::UUID \
               AND ($2 = '' OR {domain_col} = $2) \
               AND (NOT $3 OR {expires_at} IS NULL OR {expires_at} > NOW())",
            user_id = user_role_model.q("user_id"),
            domain_col = user_role_model.q("domain"),
            expires_at = user_role_model.q("expires_at"),
        ))
        .bind(user_id)
        .bind(&req.domain)
        .bind(req.active_only)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list user roles failed: {err}")))?;
        let mut user_roles = Vec::with_capacity(rows.len());
        for row in &rows {
            user_roles.push(user_role_from_row(row)?);
        }
        Ok(Response::new(authz_pb::ListUserRolesResponse {
            user_roles,
        }))
    }
    async fn get_role(
        &self,
        request: Request<authz_pb::GetRoleRequest>,
    ) -> Result<Response<authz_pb::GetRoleResponse>, Status> {
        let req = request.into_inner();
        if req.role_id.trim().is_empty() && req.role_code.trim().is_empty() {
            return Err(Status::invalid_argument("role_id or role_code is required"));
        }
        let role_id_filter = if req.role_id.trim().is_empty() {
            None
        } else {
            Some(parse_uuid_field("role_id", &req.role_id)?)
        };
        let pool = self.require_pool()?;
        let role_model = self.roles_model();
        let rel = role_model.relation.clone();
        let projection = role_select_projection(&role_model);
        let row = sqlx::query(&format!(
            "SELECT {projection} \
             FROM {rel} \
             WHERE {deleted_at} IS NULL \
               AND (($1::UUID IS NOT NULL AND {role_id} = $1) OR ($1::UUID IS NULL AND {role_code} = $2)) \
               AND ($3 = '' OR {domain_col} = $3) \
             LIMIT 1",
            role_id = role_model.q("role_id"),
            role_code = role_model.q("role_code"),
            domain_col = role_model.q("domain"),
            deleted_at = role_model.q("deleted_at"),
        ))
        .bind(role_id_filter)
        .bind(&req.role_code)
        .bind(&req.domain)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("get role failed: {err}")))?;
        match row {
            Some(row) => Ok(Response::new(authz_pb::GetRoleResponse {
                role: Some(role_from_row(&row)?),
            })),
            None => Err(Status::not_found("role not found")),
        }
    }
    async fn list_roles(
        &self,
        request: Request<authz_pb::ListRolesRequest>,
    ) -> Result<Response<authz_pb::ListRolesResponse>, Status> {
        let req = request.into_inner();
        let pool = self.require_pool()?;
        let role_model = self.roles_model();
        let rel = role_model.relation.clone();
        let projection = role_select_projection(&role_model);
        let rows = sqlx::query(&format!(
            "SELECT {projection} \
             FROM {rel} \
             WHERE {deleted_at} IS NULL \
               AND ($1 = '' OR {domain_col} = $1) \
               AND (NOT $2 OR {is_active} = TRUE) \
             ORDER BY {name} ASC",
            domain_col = role_model.q("domain"),
            is_active = role_model.q("is_active"),
            name = role_model.q("name"),
            deleted_at = role_model.q("deleted_at"),
        ))
        .bind(&req.domain)
        .bind(req.active_only)
        .fetch_all(pool)
        .await
        .map_err(|err| Status::internal(format!("list roles failed: {err}")))?;
        let mut all = Vec::with_capacity(rows.len());
        for row in &rows {
            all.push(role_from_row(row)?);
        }
        let page = req.page.as_ref();
        let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1) as usize;
        let page_size = page
            .map(|p| p.page_size)
            .filter(|s| *s > 0)
            .unwrap_or(all.len().max(1) as i32) as usize;
        let start = page_number.saturating_sub(1).saturating_mul(page_size);
        let total = all.len();
        let roles = all.into_iter().skip(start).take(page_size).collect();
        Ok(Response::new(authz_pb::ListRolesResponse {
            page: Some(page_response(total, page)),
            roles,
        }))
    }
    async fn batch_check_permissions(
        &self,
        request: Request<authz_pb::BatchCheckPermissionsRequest>,
    ) -> Result<Response<authz_pb::BatchCheckPermissionsResponse>, Status> {
        let req = request.into_inner();
        if req.user_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_id is required"));
        }
        let attributes: BTreeMap<String, String> = req
            .context
            .map(|ctx| ctx.attributes.into_iter().collect())
            .unwrap_or_default();
        let principal = Principal {
            principal_id: req.user_id.clone(),
            subject: req.user_id.clone(),
            user_id: req.user_id.clone(),
            tenant_id: req.domain.clone(),
            ..Default::default()
        };
        let snap = self.current_snapshot().await?;
        let mut results = std::collections::HashMap::new();
        for check in req.checks {
            let mut resource = ResourceRef {
                resource_name: check.object.clone(),
                message_type: check.object.clone(),
                ..Default::default()
            };
            enrich_resource(&mut resource);
            let decision = self
                .decide_with_snapshot(&snap, &principal, &resource, &check.action, "", &attributes)
                .await;
            self.write_decision_audit(&principal, &resource, &check.action, &decision)
                .await;
            results.insert(
                format!("{}:{}", check.object, check.action),
                decision.allowed,
            );
        }
        Ok(Response::new(authz_pb::BatchCheckPermissionsResponse {
            results,
        }))
    }
    async fn update_role(
        &self,
        request: Request<authz_pb::UpdateRoleRequest>,
    ) -> Result<Response<authz_pb::UpdateRoleResponse>, Status> {
        let req = request.into_inner();
        if req.role_id.trim().is_empty() {
            return Err(Status::invalid_argument("role_id is required"));
        }
        let role_id = parse_uuid_field("role_id", &req.role_id)?;
        if req.updated_by.trim().is_empty() {
            return Err(Status::invalid_argument("updated_by is required"));
        }
        let pool = self.require_pool()?;
        let role_model = self.roles_model();
        let rel = role_model.relation.clone();
        let projection = role_select_projection(&role_model);
        let row = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {name} = COALESCE(NULLIF($2, ''), {name}), \
               {description} = COALESCE(NULLIF($3, ''), {description}), \
               {is_active} = COALESCE($4, {is_active}) \
             WHERE {role_id} = $1::UUID AND {deleted_at} IS NULL \
             RETURNING {projection}",
            name = role_model.q("name"),
            description = role_model.q("description"),
            is_active = role_model.q("is_active"),
            role_id = role_model.q("role_id"),
            deleted_at = role_model.q("deleted_at"),
        ))
        .bind(role_id)
        .bind(&req.name)
        .bind(&req.description)
        .bind(req.is_active)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("update role failed: {err}")))?;
        let Some(row) = row else {
            return Err(Status::not_found("role not found"));
        };
        let role = role_from_row(&row)?;
        self.emit_event(AuthEvent::new(
            topics::ROLE_UPDATED,
            role.role_id.clone(),
            role.tenant_id.clone(),
            serde_json::json!({
                "role_id": role.role_id.clone(),
                "role_code": role.role_code.clone(),
                "tenant_id": role.tenant_id.clone(),
                "updated_by": req.updated_by.clone(),
            }),
        ))
        .await;
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::UpdateRoleResponse {
            role: Some(role),
        }))
    }
    async fn delete_role(
        &self,
        request: Request<authz_pb::DeleteRoleRequest>,
    ) -> Result<Response<authz_pb::DeleteRoleResponse>, Status> {
        let req = request.into_inner();
        if req.role_id.trim().is_empty() {
            return Err(Status::invalid_argument("role_id is required"));
        }
        if req.deleted_by.trim().is_empty() {
            return Err(Status::invalid_argument("deleted_by is required"));
        }
        let role_id = parse_uuid_field("role_id", &req.role_id)?;
        let deleted_by = parse_uuid_field("deleted_by", &req.deleted_by)?;
        let pool = self.require_pool()?;
        let role_model = self.roles_model();
        let user_role_model = self.user_roles_model();
        let role_rel = role_model.relation.clone();
        let user_role_rel = user_role_model.relation.clone();
        let result = sqlx::query(&format!(
            "UPDATE {role_rel} SET {deleted_at} = NOW(), {deleted_by} = $2, {is_active} = FALSE \
             WHERE {role_id} = $1::UUID AND {deleted_at} IS NULL",
            deleted_at = role_model.q("deleted_at"),
            deleted_by = role_model.q("deleted_by"),
            is_active = role_model.q("is_active"),
            role_id = role_model.q("role_id"),
        ))
        .bind(role_id)
        .bind(deleted_by)
        .execute(pool)
        .await
        .map_err(|err| Status::internal(format!("delete role failed: {err}")))?;
        if result.rows_affected() > 0 {
            sqlx::query(&format!(
                "DELETE FROM {user_role_rel} WHERE {role_id} = $1::UUID",
                role_id = user_role_model.q("role_id"),
            ))
            .bind(role_id)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("delete role assignments failed: {err}")))?;
        }
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::DeleteRoleResponse {
            deleted: result.rows_affected() > 0,
        }))
    }
    async fn get_policy_rule(
        &self,
        request: Request<authz_pb::GetPolicyRuleRequest>,
    ) -> Result<Response<authz_pb::GetPolicyRuleResponse>, Status> {
        let req = request.into_inner();
        if req.policy_id.trim().is_empty() {
            return Err(Status::invalid_argument("policy_id is required"));
        }
        if let Some(pool) = &self.pg_pool {
            let policy_id = parse_uuid_field("policy_id", &req.policy_id)?;
            let policy_model = self.policies_model();
            let rel = policy_model.relation.clone();
            let projection = policy_rule_select_projection(&policy_model);
            let row = sqlx::query(&format!(
                "SELECT {projection} \
                 FROM {rel} \
                 WHERE {policy_id} = $1::UUID AND {deleted_at} IS NULL \
                 LIMIT 1",
                policy_id = policy_model.q("policy_id"),
                deleted_at = policy_model.q("deleted_at"),
            ))
            .bind(policy_id)
            .fetch_optional(pool)
            .await
            .map_err(|err| Status::internal(format!("get policy rule failed: {err}")))?;
            return match row {
                Some(row) => Ok(Response::new(authz_pb::GetPolicyRuleResponse {
                    policy: Some(policy_rule_from_row(&row)?),
                })),
                None => Err(Status::not_found("policy rule not found")),
            };
        }
        let snap = self.current_snapshot().await?;
        let policy = snap
            .policies
            .iter()
            .find(|p| p.id == req.policy_id)
            .map(policy_to_rule_pb);
        Ok(Response::new(authz_pb::GetPolicyRuleResponse { policy }))
    }
    async fn list_policy_rules(
        &self,
        request: Request<authz_pb::ListPolicyRulesRequest>,
    ) -> Result<Response<authz_pb::ListPolicyRulesResponse>, Status> {
        let req = request.into_inner();
        if let Some(pool) = &self.pg_pool {
            let policy_model = self.policies_model();
            let rel = policy_model.relation.clone();
            let projection = policy_rule_select_projection(&policy_model);
            let rows = sqlx::query(&format!(
                "SELECT {projection} \
                 FROM {rel} \
                 WHERE {deleted_at} IS NULL \
                   AND ($1 = '' OR {domain_col} = $1 OR {tenant_id} = $1 OR {project_id} = $1) \
                   AND ($2 = '' OR {subject} = $2) \
                   AND ($3 = '' OR {object_col} = $3) \
                   AND (NOT $4 OR {is_active} = TRUE) \
                 ORDER BY {is_active} DESC, {policy_id} ASC",
                deleted_at = policy_model.q("deleted_at"),
                domain_col = policy_model.q("domain"),
                tenant_id = policy_model.q("tenant_id"),
                project_id = policy_model.q("project_id"),
                subject = policy_model.q("subject"),
                object_col = policy_model.q("object"),
                is_active = policy_model.q("is_active"),
                policy_id = policy_model.q("policy_id"),
            ))
            .bind(&req.domain)
            .bind(&req.subject)
            .bind(&req.object)
            .bind(req.active_only)
            .fetch_all(pool)
            .await
            .map_err(|err| Status::internal(format!("list policy rules failed: {err}")))?;
            let all = rows
                .iter()
                .map(policy_rule_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            let page = req.page.as_ref();
            let page_number = page.map(|p| p.page).filter(|p| *p > 0).unwrap_or(1) as usize;
            let page_size = page
                .map(|p| p.page_size)
                .filter(|s| *s > 0)
                .unwrap_or(all.len().max(1) as i32) as usize;
            let start = page_number.saturating_sub(1).saturating_mul(page_size);
            let policies = all.into_iter().skip(start).take(page_size).collect();
            return Ok(Response::new(authz_pb::ListPolicyRulesResponse {
                page: Some(page_response(rows.len(), page)),
                policies,
            }));
        }
        let snap = self.current_snapshot().await?;
        let policies: Vec<_> = snap
            .policies
            .iter()
            .filter(|p| !req.active_only || p.enabled)
            .filter(|p| {
                req.domain.trim().is_empty() || p.tenant == req.domain || p.project == req.domain
            })
            .filter(|p| req.subject.trim().is_empty() || p.subject == req.subject)
            .filter(|p| req.object.trim().is_empty() || p.resource == req.object)
            .map(policy_to_rule_pb)
            .collect();
        Ok(Response::new(authz_pb::ListPolicyRulesResponse {
            page: Some(page_response(policies.len(), req.page.as_ref())),
            policies,
        }))
    }
    async fn delete_policy_rule(
        &self,
        request: Request<authz_pb::DeletePolicyRuleRequest>,
    ) -> Result<Response<authz_pb::DeletePolicyRuleResponse>, Status> {
        let req = request.into_inner();
        if req.policy_id.trim().is_empty() {
            return Err(Status::invalid_argument("policy_id is required"));
        }
        let mut deleted = false;
        if let Some(pool) = &self.pg_pool {
            let policy_id = parse_uuid_field("policy_id", &req.policy_id)?;
            let deleted_by = if req.deleted_by.trim().is_empty() {
                None
            } else {
                Some(parse_uuid_field("deleted_by", &req.deleted_by)?)
            };
            let policy_model = self.policies_model();
            let rel = policy_model.relation.clone();
            let result = sqlx::query(&format!(
                "UPDATE {rel} SET {deleted_at} = NOW(), {deleted_by} = $2, {is_active} = FALSE \
                 WHERE {policy_id} = $1::UUID AND {deleted_at} IS NULL",
                policy_id = policy_model.q("policy_id"),
                deleted_at = policy_model.q("deleted_at"),
                deleted_by = policy_model.q("deleted_by"),
                is_active = policy_model.q("is_active"),
            ))
            .bind(policy_id)
            .bind(deleted_by)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("delete policy rule failed: {err}")))?;
            deleted = result.rows_affected() > 0;
        } else {
            self.require_snapshot_fallback()?;
        }
        self.invalidate_snapshot_cache();
        Ok(Response::new(authz_pb::DeletePolicyRuleResponse {
            deleted,
        }))
    }

    /// Stage 2 (item 133): authorize through the same engine and, when allowed,
    /// mint a short-lived native-access contract (restricted role + scoped DSN +
    /// RLS session variables). The decision is always returned; the grant is
    /// present only on allow and only when native access is configured.
    async fn get_native_access(
        &self,
        request: Request<authz_pb::NativeAccessRequest>,
    ) -> Result<Response<authz_pb::NativeAccessResponse>, Status> {
        use crate::runtime::authz::native_access::NativeAccessConfig;

        let req = request.into_inner();
        let mut principal = req
            .principal
            .as_ref()
            .map(authz_principal_to_runtime)
            .unwrap_or_default();
        if principal.tenant_id.trim().is_empty() {
            principal.tenant_id = req.tenant_id.clone();
        }
        if principal.project_id.trim().is_empty() {
            principal.project_id = req.project_id.clone();
        }
        if principal.scopes.is_empty() {
            principal.scopes = req.requested_scopes.clone();
        }
        if principal.subject.trim().is_empty() {
            principal.subject = if !principal.user_id.trim().is_empty() {
                principal.user_id.clone()
            } else {
                principal.principal_id.clone()
            };
        }
        if principal.principal_id.trim().is_empty() {
            principal.principal_id = principal.subject.clone();
        }
        if principal.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id is required"));
        }

        let mut resource = req
            .resource
            .as_ref()
            .map(resource_to_runtime)
            .unwrap_or_default();
        if !req.backend.trim().is_empty() && resource.backend.trim().is_empty() {
            resource.backend = req.backend.clone();
        }
        enrich_resource(&mut resource);

        let mut attributes: BTreeMap<String, String> = req.attributes.into_iter().collect();
        if let Some(ctx) = req.context {
            attributes.extend(ctx.attributes.into_iter());
        }

        let snap = self.current_snapshot().await?;
        let decision = self
            .decide_with_snapshot(
                &snap,
                &principal,
                &resource,
                &req.action,
                &req.purpose,
                &attributes,
            )
            .await;
        self.write_decision_audit(&principal, &resource, &req.action, &decision)
            .await;

        let grant = NativeAccessConfig::from_env()
            .mint(
                &principal,
                &resource,
                &req.action,
                &req.purpose,
                &decision,
                now_unix() as i64,
            )
            .map(|g| authz_pb::NativeAccessGrant {
                dsn: g.dsn,
                role: g.role,
                backend: g.backend,
                database: g.database,
                schema: g.schema,
                session_variables: g.session_variables.into_iter().collect(),
                expires_at_unix: g.expires_at_unix,
                ttl_seconds: g.ttl_seconds,
            });

        Ok(Response::new(authz_pb::NativeAccessResponse {
            decision: Some(decision_to_pb(&decision)),
            grant,
        }))
    }

    /// Stage 2 (item 139): return a signed, tenant-scoped projection of the live
    /// authorization snapshot so SDKs can cache it and answer `can()` locally.
    async fn get_policy_bundle(
        &self,
        request: Request<authz_pb::PolicyBundleRequest>,
    ) -> Result<Response<authz_pb::PolicyBundleResponse>, Status> {
        use crate::runtime::authz::bundle::PolicyBundleConfig;

        let req = request.into_inner();
        // A bundle is always tenant-scoped; an empty tenant must not fall through
        // to "all tenants" (that would sign + return every tenant's policies).
        if req.tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "tenant_id is required for a policy bundle",
            ));
        }
        let cfg = PolicyBundleConfig::from_env();
        if !cfg.enabled() {
            return Err(Status::failed_precondition(
                "policy bundle signing is not configured; set UDB_POLICY_BUNDLE_SECRET \
                 (or UDB_SESSION_HASH_SECRET)",
            ));
        }
        let snap = self.current_snapshot().await?;
        let tenant = if req.tenant_id.trim().is_empty() {
            req.domain.clone()
        } else {
            req.tenant_id.clone()
        };
        let signed = cfg
            .sign(&snap, &tenant, &req.project_id, now_unix() as i64)
            .ok_or_else(|| Status::internal("failed to sign policy bundle"))?;

        Ok(Response::new(authz_pb::PolicyBundleResponse {
            bundle: Some(authz_pb::SignedPolicyBundle {
                bundle: signed.bundle,
                signature: signed.signature,
                key_id: signed.key_id,
                algorithm: signed.algorithm,
                policy_version: signed.policy_version,
                relationship_version: signed.relationship_version,
                issued_at_unix: signed.issued_at_unix,
                expires_at_unix: signed.expires_at_unix,
                ttl_seconds: signed.ttl_seconds,
            }),
        }))
    }
}
