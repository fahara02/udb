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
    AuthzPolicy, AuthzQuery, AuthzSnapshot, Decision, PolicyEngine, Principal, RelationshipTuple,
    ResourceRef, RoleBinding,
};
use crate::runtime::channels::{ChannelManager, ChannelPermit, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

use super::events::{self, AuthEvent, AuthEventSink, topics};

// Topic submodules: inherent `impl AuthzServiceImpl` blocks (+ helpers); the
// single `impl AuthzService` trait block below delegates to them.
mod audit;
mod tuples;

// Phase K: Authz Policy Governance. Pure logic in `governance_logic`; DB-backed
// handlers split across topic submodules (drafts, activation, simulation, store).
mod governance;
mod governance_activate;
mod governance_drafts;
mod governance_logic;
mod governance_sim;
mod governance_store;

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
            // Phase L3 task4 expanded compliance columns.
            "decision_id",
            "policy_version",
            "relationship_version",
            "purpose",
            "scopes",
            "matched_policy_ids",
            "project_id",
            "actor_kind",
            "resource_type",
            "trace_id",
            "span_id",
            "user_agent_hash",
            "decision_input",
        ],
    )
}

/// Request context threaded into the access-decision audit (Phase L3 task4): the
/// source IP, correlation/trace ids, user-agent, declared purpose, and redacted
/// decision-input attributes — populated from `AccessContext` instead of the
/// empty strings the audit-write previously bound.
#[derive(Default, Clone)]
pub(super) struct AuditContext {
    pub source_ip: String,
    pub correlation_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub user_agent: String,
    pub purpose: String,
    /// Redacted decision-input attributes as a JSON object string.
    pub decision_input: String,
}

impl AuditContext {
    /// Build from the request attributes/context map. Pulls IP, user-agent,
    /// correlation/trace ids out of the attribute bag (the broker merges
    /// `AccessContext` fields into `attributes`), masks/hashes the sensitive
    /// ones, and captures the remaining attributes as a redacted JSON blob.
    pub(super) fn from_attributes(
        attributes: &std::collections::BTreeMap<String, String>,
        purpose: &str,
    ) -> Self {
        let get = |k: &str| attributes.get(k).cloned().unwrap_or_default();
        let source_ip = {
            let raw = get("ip_address");
            let raw = if raw.is_empty() {
                get("source_ip")
            } else {
                raw
            };
            events::mask_source_ip(&raw)
        };
        let user_agent = events::hash_user_agent(&get("user_agent"));
        let correlation_id = {
            let c = get("correlation_id");
            if c.is_empty() {
                get("x-correlation-id")
            } else {
                c
            }
        };
        // Capture non-sensitive attributes as a redacted JSON object so the audit
        // row records WHAT was evaluated without leaking credentials.
        let mut input = serde_json::Map::new();
        for (k, v) in attributes {
            if matches!(
                k.as_str(),
                "ip_address" | "source_ip" | "user_agent" | "correlation_id" | "x-correlation-id"
            ) {
                continue;
            }
            input.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        let mut input_value = serde_json::Value::Object(input);
        events::redact_auth_payload(&mut input_value);
        Self {
            source_ip,
            correlation_id,
            trace_id: get("trace_id"),
            span_id: get("span_id"),
            user_agent,
            purpose: purpose.to_string(),
            decision_input: input_value.to_string(),
        }
    }
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
/// `Clone` is cheap (every field is an `Arc`/`Duration`/`Option<PgPool>`) and is
/// used to hand a detached executor to the Phase-9 canary evaluator task.
#[derive(Clone)]
pub struct AuthzServiceImpl {
    snapshot: Arc<ArcSwap<AuthzSnapshot>>,
    snapshot_loaded_at: Arc<Mutex<Option<Instant>>>,
    snapshot_ttl: Duration,
    pg_pool: Option<PgPool>,
    event_sink: Arc<dyn AuthEventSink>,
    /// Records authz metrics (deny count). Defaults to a no-op; production wires
    /// the broker's shared recorder via [`AuthzServiceImpl::with_metrics`].
    metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    /// Per-tenant fair-admission manager (the SAME one the data/media planes use).
    /// The hot authz decision RPCs (`authorize`, `check_access`,
    /// `batch_check_permissions`) acquire a per-tenant `Admin` budget through this
    /// so one tenant flooding authz checks cannot starve others or exhaust the
    /// shared PG pool / snapshot path. `None` only in bare/test construction (no
    /// runtime wired) — production wires it via [`AuthzServiceImpl::with_channels`].
    channels: Option<ChannelManager>,
}

impl AuthzServiceImpl {
    /// Owning constructor over an in-memory snapshot. Production wires the
    /// service through [`AuthzServiceImpl::shared`] (a shared `ArcSwap` cell so
    /// reloads are visible across handlers); `new` is the convenience
    /// constructor used by the unit/integration tests, which exercise the
    /// service over a self-owned snapshot.
    #[cfg(test)]
    pub fn new(snapshot: AuthzSnapshot) -> Self {
        Self {
            snapshot: Arc::new(ArcSwap::from_pointee(snapshot)),
            snapshot_loaded_at: Arc::new(Mutex::new(Some(Instant::now()))),
            snapshot_ttl: authz_snapshot_ttl(),
            pg_pool: None,
            event_sink: events::noop_sink(),
            metrics: Arc::new(crate::metrics::NoopMetrics),
            channels: None,
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
            metrics: Arc::new(crate::metrics::NoopMetrics),
            channels: None,
        }
    }

    /// Wire the broker's shared per-tenant fair-admission manager (mirror of
    /// `StorageServiceImpl::with_object`'s channel wiring). The parent builder
    /// (`build_auth_services`) passes `Some(runtime.channels().clone())`. `None`
    /// keeps the bare/test path admitting without a permit.
    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    /// Per-tenant fair admission for the hot authz decision RPCs, keyed by the
    /// VALIDATED caller tenant (never an unverified body field). Acquires the
    /// `Admin` (control-plane) channel budget scoped to the tenant so one tenant
    /// cannot exhaust the shared PG pool / snapshot path with authz checks. The
    /// returned [`ChannelPermit`] MUST be held across the decision (drop =
    /// release). `None` channels (bare/test construction — no runtime wired) admit
    /// without a permit, since there is no shared admission budget to starve.
    ///
    /// On budget/concurrency exhaustion this returns the same
    /// `Status::resource_exhausted` backpressure the data plane returns.
    async fn admit(&self, tenant: &str) -> Result<Option<ChannelPermit>, Status> {
        let Some(channels) = self.channels.as_ref() else {
            return Ok(None);
        };
        let op = OperationChannel::Admin;
        // Local tenant hash for the metric label (the parent module's
        // `tenant_hash_label` is private to `service/mod.rs`); never logs the raw
        // tenant id.
        let tenant_hash = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(tenant.as_bytes());
            digest[..8]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        };
        match channels
            .acquire_fair_with_backpressure(op, Some(tenant), None, None, None, op.default_cost())
            .await
        {
            Ok(permit) => {
                self.metrics.record_fair_admission(
                    "default",
                    &tenant_hash,
                    "authz",
                    "default",
                    op.as_str(),
                    "accepted",
                );
                self.metrics.add_fair_cost(
                    "default",
                    &tenant_hash,
                    "authz",
                    "default",
                    op.as_str(),
                    f64::from(op.default_cost()),
                );
                Ok(Some(permit))
            }
            Err(err) => {
                self.metrics.inc_channel_rejected(op.as_str());
                self.metrics.record_fair_admission(
                    "default",
                    &tenant_hash,
                    "authz",
                    "default",
                    op.as_str(),
                    "rejected",
                );
                Err(err)
            }
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

    /// Attach the metrics recorder (the broker's shared recorder in production).
    /// Defaults to a no-op.
    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
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
        // Item 82: the service consumes the pluggable `PolicyEngine` seam (the
        // snapshot's impl drives the real Casbin enforcer in
        // `runtime::authz::casbin_engine`), so a future engine slots in here
        // without touching the service boundary.
        PolicyEngine::decide(
            snapshot,
            &AuthzQuery {
                principal,
                resource,
                action,
                purpose,
                attributes,
            },
        )
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

    /// Resolve a role's stable `role_code` (falling back to `name`) for use as
    /// the grouping-tuple role token when binding a non-UUID principal. Returns
    /// `None` when the role is missing.
    pub(super) async fn role_code_for(
        &self,
        role_id: Uuid,
        domain: &str,
    ) -> Result<Option<String>, Status> {
        let pool = self.require_pool()?;
        let m = self.roles_model();
        let rel = m.relation.clone();
        let row = sqlx::query(&format!(
            "SELECT COALESCE(NULLIF({role_code}, ''), {name}) AS role FROM {rel} \
             WHERE {role_id} = $1::UUID AND {deleted_at} IS NULL AND ($2 = '' OR {domain_col} = $2) LIMIT 1",
            role_code = m.q("role_code"),
            name = m.q("name"),
            role_id = m.q("role_id"),
            deleted_at = m.q("deleted_at"),
            domain_col = m.q("domain"),
        ))
        .bind(role_id)
        .bind(domain)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("resolve role code failed: {err}")))?;
        Ok(row.and_then(|r| r.try_get::<String, _>("role").ok()))
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
        ctx: &AuditContext,
    ) {
        // Phase 5: count every deny decision regardless of audit-sink/pool state
        // (this is the chokepoint all decision paths route through).
        if !decision.allowed {
            self.metrics.record_authz_deny();
        }
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
        // Phase L3 task4: classify the actor and join the scope/policy lists.
        let actor_kind = if !principal.user_id.trim().is_empty() {
            "user"
        } else if !principal.service_identity.trim().is_empty() {
            "service"
        } else {
            "external"
        };
        let scopes = decision.required_scopes.join(",");
        let result = sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({decision_audit_id}, {user_id}, {domain_col}, {object_col}, {action_col}, {effect_col}, {decision_source}, {matched_rule}, {reason}, {ip_address}, {correlation_id}, {tenant_id}, \
              {decision_id}, {policy_version}, {relationship_version}, {purpose}, {scopes}, {matched_policy_ids}, {project_id}, {actor_kind}, {resource_type}, {trace_id}, {span_id}, {user_agent_hash}, {decision_input}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25::JSONB)",
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
            decision_id = audit.q("decision_id"),
            policy_version = audit.q("policy_version"),
            relationship_version = audit.q("relationship_version"),
            purpose = audit.q("purpose"),
            scopes = audit.q("scopes"),
            matched_policy_ids = audit.q("matched_policy_ids"),
            project_id = audit.q("project_id"),
            actor_kind = audit.q("actor_kind"),
            resource_type = audit.q("resource_type"),
            trace_id = audit.q("trace_id"),
            span_id = audit.q("span_id"),
            user_agent_hash = audit.q("user_agent_hash"),
            decision_input = audit.q("decision_input"),
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
        // Phase L3 task4: populate source IP + correlation id from AccessContext
        // instead of binding empty strings.
        .bind(&ctx.source_ip)
        .bind(&ctx.correlation_id)
        .bind(&principal.tenant_id)
        .bind(&decision.decision_id)
        .bind(&decision.policy_version)
        .bind(&decision.relationship_version)
        .bind(&ctx.purpose)
        .bind(scopes)
        .bind(&decision.matched_policy_ids)
        .bind(&principal.project_id)
        .bind(actor_kind)
        .bind(&resource.resource_type)
        .bind(&ctx.trace_id)
        .bind(&ctx.span_id)
        .bind(&ctx.user_agent)
        .bind(&ctx.decision_input)
        .execute(pool)
        .await;
        if let Err(err) = result {
            tracing::warn!(error = %err, "failed to write access-decision audit");
        }

        // Phase L2: an ALLOW that the policy flagged `audit_required` is a
        // security-sensitive event in its own right (e.g. access to a regulated
        // resource that is permitted but must be reviewed). Publish it so the
        // compliance plane sees the audited-allow, distinct from the typed audit
        // row above. Denies already surface via `ACCESS_DENIED` at the decision
        // sites, so we only emit here for the allow+audit_required case.
        if decision.allowed && decision.audit_required {
            let actor = if principal.subject.trim().is_empty() {
                principal.principal_id.clone()
            } else {
                principal.subject.clone()
            };
            let correlation = if ctx.correlation_id.trim().is_empty() {
                format!("audit_allow:{}:{}", actor, resource.resource_name)
            } else {
                ctx.correlation_id.clone()
            };
            self.emit_event(
                AuthEvent::new(
                    topics::ACCESS_AUDIT_REQUIRED_ALLOW,
                    actor.clone(),
                    principal.tenant_id.clone(),
                    serde_json::json!({
                        "subject": actor.clone(),
                        "resource": resource.resource_name.clone(),
                        "action": action,
                        "purpose": ctx.purpose.clone(),
                        "matched_policy_ids": decision.matched_policy_ids.clone(),
                    }),
                )
                .with_correlation(correlation)
                .with_compliance(events::ComplianceEnvelope {
                    actor: actor.clone(),
                    actor_project: principal.project_id.clone(),
                    target_resource: resource.resource_name.clone(),
                    operation: action.to_string(),
                    outcome: "allow".to_string(),
                    reason_code: "audit_required_allow".to_string(),
                    decision_id: decision.decision_id.clone(),
                    policy_version: decision.policy_version.clone(),
                    relationship_version: decision.relationship_version.clone(),
                    trace_id: ctx.trace_id.clone(),
                    span_id: ctx.span_id.clone(),
                    ..events::ComplianceEnvelope::default()
                }),
            )
            .await;
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
            // Content-addressed versions (not count-only): any in-place policy/
            // tuple edit changes the version even when the row count is unchanged,
            // so SDK-cached bundles and cross-node snapshot caches invalidate
            // correctly. See `policy_content_version` / `tuple_content_version`.
            version: policy_content_version(&policies),
            relationship_version: tuple_content_version(&tuples),
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
        // H1: time the policy-snapshot (re)load so operators can alert on a slow
        // authz reload path. The histogram + recorder already exist; this is the
        // missing call site. Covers the real DB round-trip + decode in
        // `load_snapshot_from_postgres`.
        let reload_started = Instant::now();
        let loaded = self.load_snapshot_from_postgres().await?;
        self.metrics
            .observe_policy_reload_seconds(reload_started.elapsed().as_secs_f64());
        if let Some(snapshot) = loaded {
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
        // Tier-0 #1: per-tenant fair admission keyed by the resolved caller
        // tenant. Held across the snapshot load + decision so a single tenant
        // flooding `Authorize` can't starve others / exhaust the PG pool.
        let _admit = self.admit(&principal.tenant_id).await?;
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
        let audit_ctx = AuditContext::from_attributes(&attributes, &req.purpose);
        self.write_decision_audit(&principal, &resource, &req.action, &decision, &audit_ctx)
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
                        "subject": subject.clone(),
                        "tenant_id": principal.tenant_id.clone(),
                        "resource": resource.resource_name.clone(),
                        "action": req.action.clone(),
                        "deny_reason": decision.deny_reason.clone(),
                        "decision_id": decision.decision_id.clone(),
                    }),
                )
                .with_correlation(if decision.decision_id.trim().is_empty() {
                    format!("deny:{}:{}", subject, resource.resource_name)
                } else {
                    decision.decision_id.clone()
                })
                .with_compliance(events::ComplianceEnvelope {
                    actor: subject.clone(),
                    actor_project: principal.project_id.clone(),
                    target_resource: resource.resource_name.clone(),
                    operation: req.action.clone(),
                    outcome: "deny".to_string(),
                    reason_code: if decision.deny_reason.trim().is_empty() {
                        "access_denied".to_string()
                    } else {
                        decision.deny_reason.clone()
                    },
                    decision_id: decision.decision_id.clone(),
                    policy_version: decision.policy_version.clone(),
                    relationship_version: decision.relationship_version.clone(),
                    source_ip: audit_ctx.source_ip.clone(),
                    trace_id: audit_ctx.trace_id.clone(),
                    span_id: audit_ctx.span_id.clone(),
                    ..events::ComplianceEnvelope::default()
                }),
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
        // K2.2: in governed mode a direct active mutation is rejected — callers
        // must go through the draft -> approve -> activate flow (or use a
        // break-glass governance RPC). Off by default so legacy deployments keep
        // the direct path.
        if governance::governed_mode_enabled() {
            return Err(Status::failed_precondition(
                "governed mode: direct PutAuthzPolicy is disabled; create a policy draft and activate it (or use break-glass governance)",
            ));
        }
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
            let _ = self
                .bump_authz_revision(
                    &policy.tenant,
                    &policy.project,
                    authz_entity_pb::AuthzChangeType::Policy,
                    "policy-put",
                    "",
                )
                .await;
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
        // Item 82: lint through the `PolicyEngine` seam — the snapshot's v2
        // linter covers the previous inline checks (empty/duplicate ids,
        // disabled rules, overly broad allows) plus shadowed allows, dangling
        // role refs, and duplicate relationship tuples.
        let snap = self.current_snapshot().await?;
        let findings = PolicyEngine::lint(snap.as_ref())
            .await
            .into_iter()
            .map(|f| format!("[{}] {}: {}", f.severity, f.category, f.message))
            .collect();
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
        // Tier-0 #1: per-tenant fair admission keyed by the resolved caller
        // tenant, held across the snapshot load + decision (same backpressure the
        // data plane returns on exhaustion).
        let _admit = self.admit(&principal.tenant_id).await?;
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
        let audit_ctx = AuditContext::from_attributes(&attributes, &req.purpose);
        self.write_decision_audit(&principal, &resource, &req.action, &decision, &audit_ctx)
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
        governance::guard_governed_role_mutation("CreateRole")?;
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
        self.emit_event(
            AuthEvent::new(
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
            )
            .with_correlation(format!("role_create:{role_id}"))
            .with_compliance(events::ComplianceEnvelope {
                actor: if req.created_by.trim().is_empty() {
                    created_by.to_string()
                } else {
                    req.created_by.clone()
                },
                actor_project: req.project_id.clone(),
                target_resource: format!("role:{role_id}"),
                operation: "role_create".to_string(),
                outcome: "success".to_string(),
                reason_code: "role_created".to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
        let _ = self
            .bump_authz_revision(
                &tenant_id,
                &req.project_id,
                authz_entity_pb::AuthzChangeType::Role,
                "role-created",
                &req.created_by,
            )
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
        governance::guard_governed_role_mutation("AssignRole")?;
        let req = request.into_inner();
        // K7 principal-kind parity: a binding identifies a principal by either a
        // UUID user_id (USER compat) or a non-UUID principal_id for service
        // accounts / workloads / groups / external subjects. The `user_roles`
        // table keys on a UUID column, so non-UUID principals are mapped to a
        // STABLE UUID derived from their canonical principal id (deterministic,
        // so the same principal always resolves to the same row). Groups may be
        // bound only through an explicit principal_id (IdP/SCIM mapping policy).
        use authz_entity_pb::PrincipalKind;
        let principal_kind = PrincipalKind::try_from(req.principal_kind).unwrap_or_default();
        let principal_ref = if !req.principal_id.trim().is_empty() {
            req.principal_id.clone()
        } else {
            req.user_id.clone()
        };
        if principal_ref.trim().is_empty() || req.role_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "user_id (or principal_id) and role_id are required",
            ));
        }
        if matches!(principal_kind, PrincipalKind::Group) && req.principal_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "group role bindings require an explicit principal_id (IdP/SCIM group mapping)",
            ));
        }
        if req.assigned_by.trim().is_empty() {
            return Err(Status::invalid_argument("assigned_by is required"));
        }
        // USER with a UUID keeps its UUID; every other kind (and non-UUID users)
        // gets a stable derived UUID so service/workload/group/external principals
        // are first-class without forcing them through a real user_id.
        let user_id = match principal_kind {
            PrincipalKind::User | PrincipalKind::Unspecified => {
                Uuid::parse_str(principal_ref.trim())
                    .unwrap_or_else(|_| stable_uuid_from_subject(principal_ref.trim()))
            }
            _ => stable_uuid_from_subject(principal_ref.trim()),
        };
        let role_id = parse_uuid_field("role_id", &req.role_id)?;
        let assigned_by = parse_uuid_field("assigned_by", &req.assigned_by)?;
        let expires_at_unix = timestamp_unix_field("expires_at", req.expires_at.clone())?;
        let expires_at_bind = expires_at_unix.map(|seconds| seconds as f64);
        let tenant_id = tenant_from_domain(&req.tenant_id, &req.domain);
        if tenant_id.trim().is_empty() {
            return Err(Status::invalid_argument("tenant_id or domain is required"));
        }
        let pool = self.require_pool()?;

        // Non-USER principals (service account / workload / group / external)
        // bind via a grouping tuple keyed on the LITERAL principal id so the
        // snapshot loader exposes the real subject string (a derived UUID in
        // user_roles would never match at decision time). USER (UUID) keeps the
        // user_roles path for backward compatibility.
        let is_literal_principal = matches!(
            principal_kind,
            PrincipalKind::ServiceAccount
                | PrincipalKind::Workload
                | PrincipalKind::Group
                | PrincipalKind::ExternalSubject
        ) || (matches!(
            principal_kind,
            PrincipalKind::Unspecified | PrincipalKind::User
        ) && Uuid::parse_str(principal_ref.trim()).is_err());
        if is_literal_principal {
            let role_code = self
                .role_code_for(role_id, &req.domain)
                .await?
                .unwrap_or_else(|| req.role_id.clone());
            let tuple = self.relationship_tuples_model();
            let trel = tuple.relation.clone();
            let condition = serde_json::json!({
                "source": "assign_role",
                "principal_kind": req.principal_kind,
                "expires_at_unix": expires_at_unix.unwrap_or(0),
            })
            .to_string();
            sqlx::query(&format!(
                "INSERT INTO {trel} ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}, {condition}, {tenant_id}, {project_id}) \
                 VALUES ('grouping', $1, $3, '', $2, '', $4, $3, $5) \
                 ON CONFLICT ({tuple_kind}, {subject}, {domain_col}, {object_col}, {action_col}, {effect_col}) \
                 DO UPDATE SET {condition} = EXCLUDED.{condition}, {tenant_id} = EXCLUDED.{tenant_id}",
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
            .bind(principal_ref.trim())
            .bind(&role_code)
            .bind(&tenant_id)
            .bind(&condition)
            .bind(&req.project_id)
            .execute(pool)
            .await
            .map_err(|err| Status::internal(format!("assign role (principal) failed: {err}")))?;
            self.emit_event(
                AuthEvent::new(
                    topics::ROLE_ASSIGNED,
                    principal_ref.clone(),
                    tenant_id.clone(),
                    serde_json::json!({
                        "principal_id": principal_ref.clone(),
                        "principal_kind": req.principal_kind,
                        "role_id": req.role_id.clone(),
                        "role_code": role_code.clone(),
                        "tenant_id": tenant_id.clone(),
                        "domain": req.domain.clone(),
                        "assigned_by": req.assigned_by.clone(),
                    }),
                )
                .with_correlation(format!("role_assign:{principal_ref}:{}", req.role_id))
                .with_compliance(events::ComplianceEnvelope {
                    actor: if req.assigned_by.trim().is_empty() {
                        principal_ref.clone()
                    } else {
                        req.assigned_by.clone()
                    },
                    actor_project: req.project_id.clone(),
                    target_resource: principal_ref.clone(),
                    operation: "role_assign".to_string(),
                    outcome: "success".to_string(),
                    reason_code: "role_assigned".to_string(),
                    ..events::ComplianceEnvelope::default()
                }),
            )
            .await;
            let _ = self
                .bump_authz_revision(
                    &tenant_id,
                    &req.project_id,
                    authz_entity_pb::AuthzChangeType::RoleAssignment,
                    "role-assignment",
                    &req.assigned_by,
                )
                .await;
            self.invalidate_snapshot_cache();
            return Ok(Response::new(authz_pb::AssignRoleResponse {
                user_role: Some(authz_entity_pb::UserRole {
                    user_role_id: stable_uuid_from_subject(&format!(
                        "{principal_ref}:{}:{}",
                        req.role_id, req.domain
                    ))
                    .to_string(),
                    user_id: principal_ref,
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
            }));
        }

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
        self.emit_event(
            AuthEvent::new(
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
            )
            .with_correlation(format!("role_assign:{}:{}", req.user_id, req.role_id))
            .with_compliance(events::ComplianceEnvelope {
                actor: if req.assigned_by.trim().is_empty() {
                    req.user_id.clone()
                } else {
                    req.assigned_by.clone()
                },
                actor_project: req.project_id.clone(),
                target_resource: req.user_id.clone(),
                operation: "role_assign".to_string(),
                outcome: "success".to_string(),
                reason_code: "role_assigned".to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
        // K2.1: role assignment bumps the active authz revision so cached bundles
        // invalidate (not only policy/tuple edits).
        let _ = self
            .bump_authz_revision(
                &tenant_id,
                &req.project_id,
                authz_entity_pb::AuthzChangeType::RoleAssignment,
                "role-assignment",
                &req.assigned_by,
            )
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
        // K2.2: governed mode disables direct active mutation (use the draft flow).
        if governance::governed_mode_enabled() {
            return Err(Status::failed_precondition(
                "governed mode: direct CreatePolicyRule is disabled; create a policy draft and activate it (or use break-glass governance)",
            ));
        }
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
            let _ = self
                .bump_authz_revision(
                    &policy.tenant,
                    &policy.project,
                    authz_entity_pb::AuthzChangeType::Policy,
                    "policy-created",
                    &req.created_by,
                )
                .await;
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
        governance::guard_governed_role_mutation("RevokeRole")?;
        let req = request.into_inner();
        if req.user_role_id.trim().is_empty() {
            return Err(Status::invalid_argument("user_role_id is required"));
        }
        let user_role_id = parse_uuid_field("user_role_id", &req.user_role_id)?;
        let pool = self.require_pool()?;
        let user_role_model = self.user_roles_model();
        let rel = user_role_model.relation.clone();
        // RETURNING the tenant/project so the revocation can bump the
        // tenant-scoped authz revision (K2.1).
        let row = sqlx::query(&format!(
            "DELETE FROM {rel} WHERE {user_role_id} = $1::UUID \
             RETURNING {tenant_id}::TEXT AS tenant, '' AS project",
            user_role_id = user_role_model.q("user_role_id"),
            tenant_id = user_role_model.q("tenant_id"),
        ))
        .bind(user_role_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("revoke role failed: {err}")))?;
        let revoked = row.is_some();
        if let Some(row) = row {
            let tenant: String = row.try_get("tenant").unwrap_or_default();
            let project: String = row.try_get("project").unwrap_or_default();
            self.emit_event(
                AuthEvent::new(
                    topics::ROLE_REVOKED,
                    req.user_id.clone(),
                    tenant.clone(),
                    serde_json::json!({
                        "user_role_id": req.user_role_id.clone(),
                        "user_id": req.user_id.clone(),
                        "reason": req.reason.clone(),
                        "revoked_by": req.revoked_by.clone(),
                    }),
                )
                .with_correlation(format!("role_revoke:{}", req.user_role_id))
                .with_compliance(events::ComplianceEnvelope {
                    actor: if req.revoked_by.trim().is_empty() {
                        req.user_id.clone()
                    } else {
                        req.revoked_by.clone()
                    },
                    target_resource: req.user_id.clone(),
                    operation: "role_revoke".to_string(),
                    outcome: "success".to_string(),
                    reason_code: if req.reason.trim().is_empty() {
                        "role_revoked".to_string()
                    } else {
                        req.reason.clone()
                    },
                    ..events::ComplianceEnvelope::default()
                }),
            )
            .await;
            if !tenant.trim().is_empty() {
                let _ = self
                    .bump_authz_revision(
                        &tenant,
                        &project,
                        authz_entity_pb::AuthzChangeType::RoleAssignment,
                        "role-revoked",
                        &req.revoked_by,
                    )
                    .await;
            }
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
        // Tier-0 #1: acquire ONE per-tenant fair-admission permit for the whole
        // batch (bounded cost) — not per-check — keyed by the resolved tenant.
        // Held across all checks in the batch (same backpressure on exhaustion).
        let _admit = self.admit(&principal.tenant_id).await?;
        let snap = self.current_snapshot().await?;
        let audit_ctx = AuditContext::from_attributes(&attributes, "");
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
            self.write_decision_audit(&principal, &resource, &check.action, &decision, &audit_ctx)
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
        governance::guard_governed_role_mutation("UpdateRole")?;
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
        self.emit_event(
            AuthEvent::new(
                topics::ROLE_UPDATED,
                role.role_id.clone(),
                role.tenant_id.clone(),
                serde_json::json!({
                    "role_id": role.role_id.clone(),
                    "role_code": role.role_code.clone(),
                    "tenant_id": role.tenant_id.clone(),
                    "updated_by": req.updated_by.clone(),
                }),
            )
            .with_correlation(format!("role_update:{}", role.role_id))
            .with_compliance(events::ComplianceEnvelope {
                actor: if req.updated_by.trim().is_empty() {
                    role.role_id.clone()
                } else {
                    req.updated_by.clone()
                },
                actor_project: role.project_id.clone(),
                target_resource: format!("role:{}", role.role_id),
                operation: "role_update".to_string(),
                outcome: "success".to_string(),
                reason_code: "role_updated".to_string(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;
        let _ = self
            .bump_authz_revision(
                &role.tenant_id,
                &role.project_id,
                authz_entity_pb::AuthzChangeType::Role,
                "role-updated",
                &req.updated_by,
            )
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
        governance::guard_governed_role_mutation("DeleteRole")?;
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
        // Capture the role's tenant/project before the soft delete so the authz
        // revision bump is scoped correctly.
        let scope_row = sqlx::query(&format!(
            "SELECT {tenant_id}::TEXT AS tenant, COALESCE({project_id}, '') AS project FROM {role_rel} WHERE {role_id} = $1::UUID",
            tenant_id = role_model.q("tenant_id"),
            project_id = role_model.q("project_id"),
            role_id = role_model.q("role_id"),
        ))
        .bind(role_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| Status::internal(format!("read role scope failed: {err}")))?;
        let (role_tenant, role_project) = scope_row
            .map(|r| {
                (
                    r.try_get::<String, _>("tenant").unwrap_or_default(),
                    r.try_get::<String, _>("project").unwrap_or_default(),
                )
            })
            .unwrap_or_default();
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
        if result.rows_affected() > 0 && !role_tenant.trim().is_empty() {
            let _ = self
                .bump_authz_revision(
                    &role_tenant,
                    &role_project,
                    authz_entity_pb::AuthzChangeType::Role,
                    "role-deleted",
                    &req.deleted_by,
                )
                .await;
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
        let audit_ctx = AuditContext::from_attributes(&attributes, &req.purpose);
        self.write_decision_audit(&principal, &resource, &req.action, &decision, &audit_ctx)
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

        // Audit the native-access grant outcome (security-sensitive). A grant is
        // only ISSUED when the decision allowed AND a DSN was minted; otherwise it
        // is DENIED. The DSN/role NEVER enter the event body (credential material).
        let actor = if principal.subject.trim().is_empty() {
            principal.principal_id.clone()
        } else {
            principal.subject.clone()
        };
        let correlation = if audit_ctx.correlation_id.trim().is_empty() {
            format!("native_access:{}:{}", actor, resource.resource_name)
        } else {
            audit_ctx.correlation_id.clone()
        };
        let issued = decision.allowed && grant.is_some();
        let (grant_topic, grant_op, grant_outcome, grant_reason) = if issued {
            (
                topics::NATIVE_ACCESS_GRANT_ISSUED,
                "native_access_grant",
                "allow",
                "grant_issued".to_string(),
            )
        } else {
            (
                topics::NATIVE_ACCESS_GRANT_DENIED,
                "native_access_grant",
                "deny",
                if decision.deny_reason.trim().is_empty() {
                    "no_grant_minted".to_string()
                } else {
                    decision.deny_reason.clone()
                },
            )
        };
        self.emit_event(
            AuthEvent::new(
                grant_topic,
                actor.clone(),
                principal.tenant_id.clone(),
                serde_json::json!({
                    "subject": actor.clone(),
                    "resource": resource.resource_name.clone(),
                    "backend": resource.backend.clone(),
                    "action": req.action.clone(),
                    "purpose": req.purpose.clone(),
                    "allowed": decision.allowed,
                    "granted": issued,
                }),
            )
            .with_correlation(correlation)
            .with_compliance(events::ComplianceEnvelope {
                actor: actor.clone(),
                actor_project: principal.project_id.clone(),
                target_resource: resource.resource_name.clone(),
                operation: grant_op.to_string(),
                outcome: grant_outcome.to_string(),
                reason_code: grant_reason,
                decision_id: decision.decision_id.clone(),
                policy_version: decision.policy_version.clone(),
                relationship_version: decision.relationship_version.clone(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;

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
        // Item 82: signing goes through the `PolicyEngine` seam (the snapshot's
        // impl forwards to `PolicyBundleConfig::sign`).
        let now = now_unix() as i64;
        let signed = PolicyEngine::bundle(snap.as_ref(), &cfg, &tenant, &req.project_id, now)
            .await
            .ok_or_else(|| Status::internal("failed to sign policy bundle"))?;

        // Audit the bundle issuance (security-sensitive). Only the bundle metadata
        // (key id, versions) is recorded — never the signed bundle bytes.
        // `PolicyBundleRequest` is a server-only RPC carrying no principal, so the
        // requesting tenant is the audited actor.
        let bundle_actor = format!("tenant:{tenant}");
        self.emit_event(
            AuthEvent::new(
                topics::POLICY_BUNDLE_ISSUED,
                format!("{tenant}/{}", req.project_id),
                tenant.clone(),
                serde_json::json!({
                    "tenant_id": tenant.clone(),
                    "project_id": req.project_id.clone(),
                    "key_id": signed.key_id.clone(),
                    "policy_version": signed.policy_version.clone(),
                    "relationship_version": signed.relationship_version.clone(),
                }),
            )
            .with_correlation(format!("policy_bundle:{tenant}/{}", req.project_id))
            .with_compliance(events::ComplianceEnvelope {
                actor: bundle_actor,
                target_resource: format!("policy_bundle:{tenant}/{}", req.project_id),
                operation: "policy_bundle_issue".to_string(),
                outcome: "success".to_string(),
                reason_code: "bundle_signed".to_string(),
                policy_version: signed.policy_version.clone(),
                relationship_version: signed.relationship_version.clone(),
                ..events::ComplianceEnvelope::default()
            }),
        )
        .await;

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

    // ── Phase K: Authz Policy Governance — delegate to topic submodules ──────

    async fn create_policy_draft(
        &self,
        request: Request<authz_pb::CreatePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        self.create_policy_draft_impl(request).await
    }

    async fn update_policy_draft(
        &self,
        request: Request<authz_pb::UpdatePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        self.update_policy_draft_impl(request).await
    }

    async fn diff_policy_draft(
        &self,
        request: Request<authz_pb::DiffPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::DiffPolicyDraftResponse>, Status> {
        self.diff_policy_draft_impl(request).await
    }

    async fn submit_policy_draft(
        &self,
        request: Request<authz_pb::SubmitPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyDraftResponse>, Status> {
        self.submit_policy_draft_impl(request).await
    }

    async fn approve_policy_draft(
        &self,
        request: Request<authz_pb::ApprovePolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyApprovalResponse>, Status> {
        self.approve_policy_draft_impl(request).await
    }

    async fn reject_policy_draft(
        &self,
        request: Request<authz_pb::RejectPolicyDraftRequest>,
    ) -> Result<Response<authz_pb::PolicyApprovalResponse>, Status> {
        self.reject_policy_draft_impl(request).await
    }

    async fn activate_policy_version(
        &self,
        request: Request<authz_pb::ActivatePolicyVersionRequest>,
    ) -> Result<Response<authz_pb::ActivationResponse>, Status> {
        self.activate_policy_version_impl(request).await
    }

    async fn rollback_policy_version(
        &self,
        request: Request<authz_pb::RollbackPolicyVersionRequest>,
    ) -> Result<Response<authz_pb::ActivationResponse>, Status> {
        self.rollback_policy_version_impl(request).await
    }

    async fn activate_canary(
        &self,
        request: Request<authz_pb::ActivateCanaryRequest>,
    ) -> Result<Response<authz_pb::CanaryResponse>, Status> {
        self.activate_canary_impl(request).await
    }

    async fn promote_canary(
        &self,
        request: Request<authz_pb::PromoteCanaryRequest>,
    ) -> Result<Response<authz_pb::CanaryResponse>, Status> {
        self.promote_canary_impl(request).await
    }

    async fn get_canary_status(
        &self,
        request: Request<authz_pb::GetCanaryStatusRequest>,
    ) -> Result<Response<authz_pb::GetCanaryStatusResponse>, Status> {
        self.get_canary_status_impl(request).await
    }

    async fn list_policy_versions(
        &self,
        request: Request<authz_pb::ListPolicyVersionsRequest>,
    ) -> Result<Response<authz_pb::ListPolicyVersionsResponse>, Status> {
        self.list_policy_versions_impl(request).await
    }

    async fn simulate_policy(
        &self,
        request: Request<authz_pb::SimulatePolicyRequest>,
    ) -> Result<Response<authz_pb::SimulatePolicyResponse>, Status> {
        self.simulate_policy_impl(request).await
    }

    async fn explain_policy(
        &self,
        request: Request<authz_pb::ExplainPolicyRequest>,
    ) -> Result<Response<authz_pb::ExplainPolicyResponse>, Status> {
        self.explain_policy_impl(request).await
    }

    async fn get_authz_revision(
        &self,
        request: Request<authz_pb::GetAuthzRevisionRequest>,
    ) -> Result<Response<authz_pb::GetAuthzRevisionResponse>, Status> {
        self.get_authz_revision_impl(request).await
    }

    async fn invalidate_policy_bundles(
        &self,
        request: Request<authz_pb::InvalidatePolicyBundlesRequest>,
    ) -> Result<Response<authz_pb::InvalidatePolicyBundlesResponse>, Status> {
        self.invalidate_policy_bundles_impl(request).await
    }

    async fn seed_builtin_roles(
        &self,
        request: Request<authz_pb::SeedBuiltinRolesRequest>,
    ) -> Result<Response<authz_pb::SeedBuiltinRolesResponse>, Status> {
        self.seed_builtin_roles_impl(request).await
    }

    async fn migrate_legacy_policies(
        &self,
        request: Request<authz_pb::MigrateLegacyPoliciesRequest>,
    ) -> Result<Response<authz_pb::MigrateLegacyPoliciesResponse>, Status> {
        self.migrate_legacy_policies_impl(request).await
    }
}

/// Derive a content-addressed snapshot version for the policy set. Unlike a
/// count-only `pg-{len}` version, this changes whenever any policy's *content*
/// changes (effect flip, condition edit, scope change) even if the row count is
/// unchanged — which is exactly the case a count-only version missed, letting
/// SDK-cached bundles and cross-node snapshot caches serve stale policy. The
/// hash is computed over a sorted canonical form, so DB row order never affects
/// it; the `pg-{len}-…` shape keeps the version human-readable. `DefaultHasher`
/// is seeded with fixed keys, so it is deterministic across nodes running the
/// same binary (which is all snapshot-cache invalidation requires).
fn policy_content_version(policies: &[AuthzPolicy]) -> String {
    use std::hash::{Hash, Hasher};
    let mut entries: Vec<String> = policies
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
        .collect();
    entries.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    entries.len().hash(&mut hasher);
    for entry in &entries {
        entry.hash(&mut hasher);
    }
    format!("pg-{}-{:016x}", policies.len(), hasher.finish())
}

/// Content-addressed version for the relationship-tuple set (see
/// [`policy_content_version`] for the rationale and determinism guarantees).
fn tuple_content_version(tuples: &[RelationshipTuple]) -> String {
    use std::hash::{Hash, Hasher};
    let mut entries: Vec<String> = tuples
        .iter()
        .map(|t| {
            format!(
                "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                t.subject, t.relation, t.object, t.tenant, t.project,
            )
        })
        .collect();
    entries.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    entries.len().hash(&mut hasher);
    for entry in &entries {
        entry.hash(&mut hasher);
    }
    format!("pg-{}-{:016x}", tuples.len(), hasher.finish())
}

#[cfg(test)]
mod version_tests {
    use super::*;

    fn policy(id: &str, effect: Effect) -> AuthzPolicy {
        AuthzPolicy {
            id: id.to_string(),
            priority: 0,
            enabled: true,
            effect,
            tenant: "acme".to_string(),
            project: String::new(),
            subject: "u1".to_string(),
            role: String::new(),
            action: "read".to_string(),
            resource: "doc".to_string(),
            purpose: String::new(),
            relationship: String::new(),
            conditions: Default::default(),
            required_scopes: Vec::new(),
        }
    }

    #[test]
    fn version_changes_on_in_place_edit_same_count() {
        let before = vec![policy("p1", Effect::Allow)];
        // Same row count, but the effect flipped — a count-only version would NOT
        // change; the content version MUST.
        let after = vec![policy("p1", Effect::Deny)];
        assert_ne!(
            policy_content_version(&before),
            policy_content_version(&after),
            "an in-place effect change must bump the version"
        );
    }

    #[test]
    fn version_is_order_independent_and_stable() {
        let a = vec![policy("p1", Effect::Allow), policy("p2", Effect::Deny)];
        let b = vec![policy("p2", Effect::Deny), policy("p1", Effect::Allow)];
        // DB row order must not change the version.
        assert_eq!(policy_content_version(&a), policy_content_version(&b));
        // And it is stable across calls.
        assert_eq!(policy_content_version(&a), policy_content_version(&a));
    }
}

#[cfg(test)]
mod fair_admission_tests {
    use super::*;

    /// Tier-0 #1: with a wired `ChannelManager`, the hot authz decision path
    /// acquires a per-tenant `Admin` fair-admission permit (held across the
    /// decision). Without channels (bare/test construction) it degrades to a
    /// no-op permit so callers are admitted unchanged.
    #[tokio::test]
    async fn admit_acquires_permit_when_channels_wired() {
        // Bare construction: no channels → admit without a permit (no-op).
        let bare = AuthzServiceImpl::new(AuthzSnapshot::default());
        let none = bare
            .admit("tenant-a")
            .await
            .expect("bare admit must never reject");
        assert!(
            none.is_none(),
            "no channels wired ⇒ admit must return None (no-op, callers still admitted)"
        );

        // Wired construction: a real ChannelManager → admit acquires a permit.
        let svc = AuthzServiceImpl::new(AuthzSnapshot::default())
            .with_channels(Some(ChannelManager::from_env()));
        let permit = svc
            .admit("tenant-a")
            .await
            .expect("Admin channel has capacity ⇒ permit must be granted");
        assert!(
            permit.is_some(),
            "channels wired + capacity available ⇒ admit must hold a ChannelPermit"
        );
    }
}
