//! UDB-owned authorization model + Casbin enforcement.
//!
//! Runtime `Principal` / `ResourceRef` / `Decision` types over an immutable
//! [`AuthzSnapshot`] supporting RBAC (roles + role bindings), ABAC (attribute
//! conditions), simple ReBAC (relationship tuples), explicit-deny and required
//! scopes. The single decision path is [`AuthzSnapshot::casbin_authorize`]
//! (see [`casbin_engine`]): a real Casbin PERM model drives subject/role/
//! resource/action/domain matching, with the non-Casbin gates (scopes,
//! conditions, relationships, purpose) pre-filtering which policies enter the
//! enforcer and an explicit Deny short-circuiting before enforcement.
//!
//! It is intentionally decoupled from gRPC: callers map a `Decision` to a
//! `tonic::Status` only at the service boundary.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};

use crate::runtime::security::{PolicyEffect, SecurityContext};

/// Real Casbin-driven enforcement of the snapshot via a PERM model.
mod casbin_engine;
pub(crate) use casbin_engine::validate_casbin_model;

/// Stage 2: signed policy bundles for local SDK authorization caches (item 139).
pub mod bundle;
/// Stage 2: native Postgres restricted-role / DSN contract (item 134).
pub mod native_access;
/// C.4: pluggable policy-engine trait seam (Casbin stays the default impl).
pub mod policy_engine;

pub use policy_engine::PolicyEngine;

// ── Snapshot staleness bound ───────────────────────────────────────────────
//
// The live policy snapshot is PG-warmed on an interval and a failed reload
// RETAINS the last good snapshot (so a PG blip does not blank authz). Without a
// bound, a revoked grant would keep being honored for as long as PG stays
// unreachable. The bound: once a snapshot has been loaded from the durable store
// at least once, a last-good snapshot older than the configured maximum fails
// data-plane decisions closed until a reload succeeds.

/// Unix seconds of the last SUCCESSFUL durable snapshot load; 0 = never loaded
/// from the durable store (in-memory/dev snapshot — no bound applies).
static LAST_GOOD_SNAPSHOT_UNIX: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Default maximum age of a last-good snapshot before decisions fail closed.
pub const DEFAULT_SNAPSHOT_MAX_STALENESS_SECS: u64 = 600;

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// Record a successful durable snapshot load (called by the snapshot loader).
pub(crate) fn record_snapshot_reload_success() {
    LAST_GOOD_SNAPSHOT_UNIX.store(unix_now_secs(), std::sync::atomic::Ordering::Relaxed);
}

// ── Snapshot-behind-revision hint ──────────────────────────────────────────
//
// A deny that lands between a durable policy/role change (for example an
// offline `udb policy apply`, which appends an authz revision) and the next
// timed snapshot reload looks exactly like a missing grant. On a deny the
// broker compares the revision fingerprint the snapshot was loaded at with the
// durable one and, when the durable one is newer, says so with a retry time.
// The durable probe runs at most once per `REVISION_PROBE_MIN_INTERVAL` and is
// bounded by `REVISION_PROBE_TIMEOUT`, so a deny storm cannot turn into a
// query storm and a slow store cannot slow a refusal.

type RevisionProbeFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send>>;

/// Reads the durable authz revision fingerprint (`None` when unreadable).
pub(crate) type RevisionProbe = std::sync::Arc<dyn Fn() -> RevisionProbeFuture + Send + Sync>;

struct RevisionState {
    /// Fingerprint of the durable revisions the shared snapshot was loaded at.
    loaded: Option<String>,
    /// Snapshot reload cadence, for the retry hint.
    reload_every_secs: u64,
    probe: Option<RevisionProbe>,
    /// Last probe time and the durable fingerprint it read.
    last_probe: Option<(std::time::Instant, Option<String>)>,
}

const REVISION_PROBE_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const REVISION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);

fn revision_state() -> &'static std::sync::Mutex<RevisionState> {
    static STATE: std::sync::OnceLock<std::sync::Mutex<RevisionState>> = std::sync::OnceLock::new();
    STATE.get_or_init(|| {
        std::sync::Mutex::new(RevisionState {
            loaded: None,
            reload_every_secs: 5,
            probe: None,
            last_probe: None,
        })
    })
}

/// Register how to read the durable revision fingerprint and the snapshot
/// reload cadence (called once the authz service has its durable store).
pub(crate) fn register_revision_probe(probe: RevisionProbe, reload_every_secs: u64) {
    if let Ok(mut state) = revision_state().lock() {
        state.probe = Some(probe);
        state.reload_every_secs = reload_every_secs.max(1);
        state.last_probe = None;
    }
}

/// Record the durable revision fingerprint a successful snapshot load saw.
pub(crate) fn record_loaded_revision(fingerprint: &str) {
    if let Ok(mut state) = revision_state().lock() {
        state.loaded = Some(fingerprint.to_string());
    }
}

/// The message suffix for a deny decided on a snapshot older than the durable
/// revision. Pure for testing: `None` unless both fingerprints are known and
/// differ.
pub(crate) fn stale_snapshot_message(
    loaded: Option<&str>,
    durable: Option<&str>,
    reload_every_secs: u64,
    loaded_age_secs: u64,
) -> Option<String> {
    let (loaded, durable) = (loaded?, durable?);
    if loaded.is_empty() || durable.is_empty() || loaded == durable {
        return None;
    }
    let retry_after = reload_every_secs
        .saturating_sub(loaded_age_secs)
        .clamp(1, reload_every_secs.max(1));
    Some(format!(
        "Note: this broker decided with an authz snapshot that predates the latest durable \
         policy/role revision (a change such as `udb policy apply` or a role binding landed after \
         the last reload; snapshots reload every {reload_every_secs}s, UDB_AUTHZ_SNAPSHOT_TTL_SECS). \
         Retry after {retry_after}s before changing policy"
    ))
}

/// Probe the durable revision (rate-limited, time-bounded) and return the
/// stale-snapshot note when the loaded snapshot is behind it.
pub(crate) async fn stale_snapshot_hint() -> Option<String> {
    let (probe, loaded, reload_every_secs, cached) = {
        let state = revision_state().lock().ok()?;
        let cached = state
            .last_probe
            .as_ref()
            .filter(|(at, _)| at.elapsed() < REVISION_PROBE_MIN_INTERVAL)
            .map(|(_, durable)| durable.clone());
        (
            state.probe.clone(),
            state.loaded.clone(),
            state.reload_every_secs,
            cached,
        )
    };
    loaded.as_ref()?;
    let durable = match cached {
        Some(durable) => durable,
        None => {
            let probe = probe?;
            let durable = tokio::time::timeout(REVISION_PROBE_TIMEOUT, probe())
                .await
                .ok()
                .flatten();
            if let Ok(mut state) = revision_state().lock() {
                state.last_probe = Some((std::time::Instant::now(), durable.clone()));
            }
            durable
        }
    };
    let last_good = LAST_GOOD_SNAPSHOT_UNIX.load(std::sync::atomic::Ordering::Relaxed);
    let age = u64::try_from(unix_now_secs().saturating_sub(last_good)).unwrap_or(0);
    stale_snapshot_message(
        loaded.as_deref(),
        durable.as_deref(),
        reload_every_secs,
        age,
    )
}

/// Parse `UDB_AUTHZ_SNAPSHOT_MAX_STALENESS_SECS` (`0` disables the bound).
fn resolve_snapshot_max_staleness_secs(raw: Option<&str>) -> u64 {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_SNAPSHOT_MAX_STALENESS_SECS)
}

fn snapshot_max_staleness_secs() -> u64 {
    static CACHE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHE.get_or_init(|| {
        resolve_snapshot_max_staleness_secs(
            std::env::var("UDB_AUTHZ_SNAPSHOT_MAX_STALENESS_SECS")
                .ok()
                .as_deref(),
        )
    })
}

/// Pure staleness rule: `Some(age_secs)` when a durable snapshot was loaded
/// (`last_good > 0`), the bound is enabled (`max_secs > 0`) and the last good
/// load is older than the bound.
pub(crate) fn snapshot_staleness(last_good: i64, now: i64, max_secs: u64) -> Option<u64> {
    if last_good <= 0 || max_secs == 0 {
        return None;
    }
    let age = u64::try_from(now.saturating_sub(last_good)).unwrap_or(0);
    (age > max_secs).then_some(age)
}

/// `Some(age_secs)` when the live snapshot is past its staleness bound and
/// data-plane decisions must fail closed.
pub(crate) fn snapshot_stale_age_secs() -> Option<u64> {
    snapshot_staleness(
        LAST_GOOD_SNAPSHOT_UNIX.load(std::sync::atomic::Ordering::Relaxed),
        unix_now_secs(),
        snapshot_max_staleness_secs(),
    )
}

/// Map a broker RPC name to a canonical authorization action. Pure so the
/// broker-integration layer (Milestone 7) and tests share one table.
pub fn rpc_action(rpc_name: &str) -> &'static str {
    match rpc_name {
        "Select" | "BatchSelect" => "data.select",
        "Upsert" | "BatchUpsert" => "data.upsert",
        "Delete" => "data.delete",
        "VectorSearch" | "VectorHybridSearch" => "vector.search",
        "VectorUpsert" | "VectorBatchUpsert" => "vector.upsert",
        "PutObject" | "InitiateMultipartUpload" => "object.write",
        "GetObject" => "object.read",
        "GeneratePresignedUrl" => "object.presign",
        "GenericDispatch" => "backend.dispatch",
        "PublishCDC" | "EnqueueOutboxEvent" => "cdc.publish",
        // Catalog / migration / policy / DLQ / saga / project / health admin RPCs.
        _ => "admin.manage",
    }
}

/// Back-compat wrapper for older call sites. Native authz DDL is generated from
/// `proto/udb/core/authz/entity/**` through the normal UDB proto migration path.
pub fn authz_catalog_ddl(_schema: &str) -> Vec<String> {
    crate::runtime::native_catalog::native_service_catalog_ddl()
        .into_iter()
        .filter(|sql| sql.contains("udb_authz"))
        .collect()
}

/// Allow / Deny effect for a policy or a decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Effect {
    #[default]
    Allow,
    Deny,
}

impl Effect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Effect::Allow => "allow",
            Effect::Deny => "deny",
        }
    }
}

impl From<PolicyEffect> for Effect {
    fn from(value: PolicyEffect) -> Self {
        match value {
            PolicyEffect::Allow => Effect::Allow,
            PolicyEffect::Deny => Effect::Deny,
        }
    }
}

/// The authenticated caller, normalized to what authorization needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Principal {
    pub principal_id: String,
    pub subject: String,
    pub user_id: String,
    pub service_identity: String,
    pub tenant_id: String,
    pub project_id: String,
    pub scopes: Vec<String>,
    pub roles: Vec<String>,
    pub provider_id: String,
    pub auth_method: String,
}

impl Principal {
    /// True if the principal carries `scope` (exact, `udb:*`, or `*`).
    pub fn has_scope(&self, scope: &str) -> bool {
        if scope.trim().is_empty() {
            return true;
        }
        self.scopes
            .iter()
            .any(|s| s == scope || s == "*" || s == "udb:*")
    }

    /// Bridge the existing per-request `SecurityContext` into a `Principal`.
    /// `roles` come from a JWT `roles` claim / role bindings (not yet stored on
    /// `SecurityContext`), so they are passed in explicitly for now.
    pub fn from_security_context(ctx: &SecurityContext, roles: Vec<String>) -> Self {
        let subject = if !ctx.user_id.trim().is_empty() {
            ctx.user_id.clone()
        } else {
            ctx.service_identity.clone()
        };
        Self {
            principal_id: subject.clone(),
            subject,
            user_id: ctx.user_id.clone(),
            service_identity: ctx.service_identity.clone(),
            tenant_id: ctx.tenant_id.clone(),
            project_id: ctx.project_id.clone(),
            scopes: ctx.scopes.clone(),
            roles,
            provider_id: String::new(),
            auth_method: String::new(),
        }
    }

    /// Any identity string a policy `subject` selector may match against.
    fn identities(&self) -> [&str; 4] {
        [
            self.subject.as_str(),
            self.service_identity.as_str(),
            self.user_id.as_str(),
            self.principal_id.as_str(),
        ]
    }
}

/// The thing being acted upon.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResourceRef {
    pub resource_type: String,
    pub resource_name: String,
    pub message_type: String,
    pub schema: String,
    pub table: String,
    pub backend: String,
    pub instance: String,
}

impl ResourceRef {
    /// Message-type resource (the common broker case).
    pub fn message(message_type: impl Into<String>) -> Self {
        let message_type = message_type.into();
        Self {
            resource_type: "message".to_string(),
            resource_name: message_type.clone(),
            message_type,
            ..Self::default()
        }
    }

    /// Candidate strings a policy `resource_pattern` may match against.
    ///
    /// The generic `message` resource type is NOT a selector: every data
    /// request carries it, so a policy whose object is the literal `message`
    /// would otherwise grant (or deny) every data RPC on every resource, like
    /// `*`. Specific resource types still match.
    fn selectors(&self) -> [&str; 4] {
        let resource_type = if self.resource_type.trim() == "message" {
            ""
        } else {
            self.resource_type.as_str()
        };
        [
            self.resource_name.as_str(),
            self.message_type.as_str(),
            self.table.as_str(),
            resource_type,
        ]
    }
}

/// One v2 authorization policy. Empty string selectors are wildcards.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct AuthzPolicy {
    pub id: String,
    pub priority: i32,
    pub enabled: bool,
    pub effect: Effect,
    /// Tenant domain — empty/`*` matches any tenant.
    pub tenant: String,
    /// Project domain — empty/`*` matches any project.
    pub project: String,
    /// Subject selector matched against the principal's identities.
    pub subject: String,
    /// Role selector — empty/`*` ignored, else the principal must hold the role.
    pub role: String,
    /// Action selector (`data.select`, `data.*`, `*`, …).
    pub action: String,
    /// Resource selector matched against the `ResourceRef` selectors.
    pub resource: String,
    /// Purpose selector.
    pub purpose: String,
    /// Required relationship — empty = none. When set, an ReBAC tuple
    /// `(principal.subject, <relationship>, resource.resource_name)` must exist
    /// in the same tenant/project for the policy to match.
    pub relationship: String,
    /// Attribute equality conditions; every entry must equal a request attribute.
    pub conditions: BTreeMap<String, String>,
    /// Scopes the principal must hold for the policy to match.
    pub required_scopes: Vec<String>,
}

impl Default for AuthzPolicy {
    fn default() -> Self {
        Self {
            id: String::new(),
            priority: 0,
            enabled: true,
            effect: Effect::Allow,
            tenant: String::new(),
            project: String::new(),
            subject: String::new(),
            role: String::new(),
            action: String::new(),
            resource: String::new(),
            purpose: String::new(),
            relationship: String::new(),
            conditions: BTreeMap::new(),
            required_scopes: Vec::new(),
        }
    }
}

/// RBAC: a principal (by subject) holds a role within a tenant/project domain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoleBinding {
    pub subject: String,
    pub role: String,
    pub tenant: String,
    pub project: String,
}

/// ReBAC: `subject <relation> object` within a tenant/project domain.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationshipTuple {
    pub subject: String,
    pub relation: String,
    pub object: String,
    pub tenant: String,
    pub project: String,
}

/// `deny_reason` prefix of a decision denied by an EXPLICIT matched Deny
/// policy. `matched_policy_ids` cannot tell the two apart: a default-deny also
/// lists every candidate policy it evaluated.
pub const EXPLICIT_DENY_REASON_PREFIX: &str = "denied by policy ";

/// `deny_reason` prefix of a decision the engine could not evaluate.
pub const ENGINE_ERROR_REASON_PREFIX: &str = "casbin engine error: ";

/// The structured authorization result. Mapped to a gRPC status only at the
/// service boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Decision {
    pub decision_id: String,
    pub allowed: bool,
    pub effect: Effect,
    pub deny_reason: String,
    /// Safe denial diagnostics: closest caller-tenant rule and failed
    /// attributes. Empty on allow; never contains another tenant's rule.
    pub missing: BTreeMap<String, String>,
    pub matched_policy_ids: Vec<String>,
    pub required_scopes: Vec<String>,
    pub policy_version: String,
    pub relationship_version: String,
    pub cache_ttl_seconds: u64,
    pub audit_required: bool,
    /// True when the granting Allow policy matched via a role binding (vs a
    /// direct subject match). Drives the `ROLE_POLICY` decision-source audit
    /// classification. Only meaningful when `allowed`.
    pub via_role: bool,
}

impl Decision {
    /// Denied by an explicit matched Deny policy, or by an engine failure
    /// (fail closed) — as opposed to a default-deny where no Allow matched.
    pub fn is_explicit_deny(&self) -> bool {
        !self.allowed
            && (self.deny_reason.starts_with(EXPLICIT_DENY_REASON_PREFIX)
                || self.deny_reason.starts_with(ENGINE_ERROR_REASON_PREFIX))
    }
}

/// A single authorization question.
#[derive(Debug, Clone)]
pub struct AuthzQuery<'a> {
    pub principal: &'a Principal,
    pub resource: &'a ResourceRef,
    pub action: &'a str,
    pub purpose: &'a str,
    pub attributes: &'a BTreeMap<String, String>,
}

/// An immutable, atomically-swappable view of all authorization inputs.
#[derive(Debug, Clone, Default)]
pub struct AuthzSnapshot {
    pub version: String,
    pub relationship_version: String,
    pub policies: Vec<AuthzPolicy>,
    pub role_bindings: Vec<RoleBinding>,
    pub tuples: Vec<RelationshipTuple>,
    /// Fail-open only when no policy exists AND this is true (dev/local). In
    /// production this must be false (deny by default).
    pub default_allow: bool,
}

impl AuthzSnapshot {
    /// Effective roles = principal-carried roles ∪ role bindings matching the
    /// principal's identities within the request domain.
    fn effective_roles(&self, principal: &Principal) -> Vec<String> {
        let mut roles = principal.roles.clone();
        for binding in &self.role_bindings {
            if !domain_match(&binding.tenant, &principal.tenant_id)
                || !domain_match(&binding.project, &principal.project_id)
            {
                continue;
            }
            if principal
                .identities()
                .iter()
                .any(|id| *id == binding.subject)
                && !roles.contains(&binding.role)
            {
                roles.push(binding.role.clone());
            }
        }
        roles
    }

    fn has_tuple(&self, principal: &Principal, relation: &str, object: &str) -> bool {
        self.tuples.iter().any(|t| {
            t.relation == relation
                && t.object == object
                && principal.identities().iter().any(|id| *id == t.subject)
                && domain_match(&t.tenant, &principal.tenant_id)
                && domain_match(&t.project, &principal.project_id)
        })
    }

    fn policy_matches(&self, policy: &AuthzPolicy, roles: &[String], req: &AuthzQuery<'_>) -> bool {
        let p = req.principal;
        let base = domain_match(&policy.tenant, &p.tenant_id)
            && domain_match(&policy.project, &p.project_id)
            && subject_match(&policy.subject, &p.identities())
            && role_match(&policy.role, roles)
            && pattern_match(&policy.action, req.action)
            && resource_match(&policy.resource, &req.resource.selectors())
            && wildcard(&policy.purpose, req.purpose)
            && conditions_match(&policy.conditions, req.attributes)
            && (policy.relationship.is_empty()
                || self.has_tuple(p, &policy.relationship, &req.resource.resource_name));
        if !base {
            return false;
        }
        // `required_scopes` REFINES an Allow — the caller must hold every listed
        // scope to be granted. It must NOT gate a Deny: scopes are caller-asserted
        // capabilities, so letting a missing scope cancel a Deny is fail-open (a
        // broader, lower-priority Allow could then win). A Deny applies once the
        // subject/role/action/resource/domain/relationship predicate matches.
        match policy.effect {
            Effect::Allow => policy
                .required_scopes
                .iter()
                .all(|scope| p.has_scope(scope)),
            Effect::Deny => true,
        }
    }
}

impl AuthzSnapshot {
    /// Deterministic, auditable id over (policy version, principal, resource,
    /// action, purpose). Stable for identical inputs so audit logs can join
    /// decisions across retries without a clock.
    fn decision_id(&self, req: &AuthzQuery<'_>) -> String {
        let mut hasher = Sha256::new();
        for part in [
            self.version.as_str(),
            req.principal.principal_id.as_str(),
            req.principal.subject.as_str(),
            req.principal.tenant_id.as_str(),
            req.principal.project_id.as_str(),
            req.resource.resource_name.as_str(),
            req.resource.message_type.as_str(),
            req.action,
            req.purpose,
        ] {
            hasher.update(part.as_bytes());
            hasher.update([0u8]); // domain separator
        }
        let digest = hasher.finalize();
        // Take 16 hex chars of the digest after the `authz_` prefix. Build from a
        // char iterator rather than a byte slice so this can never panic on a
        // non-ASCII boundary if the format ever changes.
        let id: String = format!("{digest:x}").chars().take(16).collect();
        format!("authz_{id}")
    }
}

// ── Grant hints ────────────────────────────────────────────────────────────

/// Who a suggested grant names: a role the caller holds, or the caller itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grantee<'a> {
    Role(&'a str),
    Subject(&'a str),
}

impl<'a> Grantee<'a> {
    /// The caller's first effective role, else its most readable identity
    /// (service identity, then subject, then principal id).
    pub fn for_principal(principal: &'a Principal, roles: &'a [String]) -> Self {
        if let Some(role) = roles
            .iter()
            .map(|role| role.trim())
            .find(|role| !role.is_empty())
        {
            return Grantee::Role(role);
        }
        let subject = [
            principal.service_identity.as_str(),
            principal.subject.as_str(),
            principal.user_id.as_str(),
            principal.principal_id.as_str(),
        ]
        .into_iter()
        .map(str::trim)
        .find(|id| !id.is_empty())
        .unwrap_or("<subject>");
        Grantee::Subject(subject)
    }
}

fn yaml_scalar(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("\"{value}\""))
}

/// One policies-file entry, YAML flow style, that allows `action` on
/// `resource` for `grantee`. Values are JSON-quoted, which YAML accepts.
pub fn grant_policy_entry(grantee: Grantee<'_>, action: &str, resource: &str) -> String {
    let (key, who) = match grantee {
        Grantee::Role(role) => ("role", role),
        Grantee::Subject(subject) => ("subject", subject),
    };
    format!(
        "{{{key}: {}, action: {}, resource: {}, effect: allow}}",
        yaml_scalar(who),
        yaml_scalar(action),
        yaml_scalar(resource)
    )
}

/// The exact `udb policy apply` step that would grant `action` on `resource`.
/// `udb policy apply` reconciles the tenant to the WHOLE file (rules it does
/// not declare are soft-deleted), so the hint says to add the entry to the
/// existing file rather than apply a file holding only this entry.
pub fn grant_command_hint(
    tenant: &str,
    grantee: Grantee<'_>,
    action: &str,
    resource: &str,
) -> String {
    let tenant = if tenant.trim().is_empty() {
        "<tenant-uuid>"
    } else {
        tenant.trim()
    };
    format!(
        "To grant it, add `- {}` under `policies:` in the tenant's policies file and run \
         `udb policy apply -f <policies.yaml> --tenant {tenant}` (apply reconciles the whole \
         file, so keep the existing entries in it)",
        grant_policy_entry(grantee, action, resource)
    )
}

// ── Matching helpers ───────────────────────────────────────────────────────

/// Conservative offline coverage for a declared identity and call site.
/// Uses the runtime's subject, role, domain, resource and scope predicates.
/// An ALLOW requiring runtime attributes or relationships is not proof. A
/// potentially matching conditional DENY prevents a static grant. A missing
/// purpose cannot pass because the data plane requires it on every request.
pub fn policies_allow_statically(
    policies: &[AuthzPolicy],
    principal: &Principal,
    resource: &ResourceRef,
    action: &str,
    purpose: Option<&str>,
) -> bool {
    let Some(purpose) = purpose.filter(|purpose| !purpose.trim().is_empty()) else {
        return false;
    };
    let snapshot = AuthzSnapshot::default();
    let attributes = BTreeMap::new();
    let request = AuthzQuery {
        principal,
        resource,
        action,
        purpose,
        attributes: &attributes,
    };
    let denied = policies.iter().any(|policy| {
        if !policy.enabled || policy.effect != Effect::Deny {
            return false;
        }
        let mut potential = policy.clone();
        potential.conditions.clear();
        potential.relationship.clear();
        if principal.project_id.is_empty() {
            potential.project.clear();
        }
        snapshot.policy_matches(&potential, &principal.roles, &request)
    });
    !denied
        && policies.iter().any(|policy| {
            policy.enabled
                && policy.effect == Effect::Allow
                && policy.conditions.is_empty()
                && policy.relationship.is_empty()
                && snapshot.policy_matches(policy, &principal.roles, &request)
        })
}

fn wildcard(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || pattern == value
}

/// Domain match: empty / `*` matches any value.
fn domain_match(pattern: &str, value: &str) -> bool {
    wildcard(pattern, value)
}

/// Action/resource pattern: exact, `*`, or dotted prefix wildcard `data.*`.
fn pattern_match(pattern: &str, value: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() || pattern == "*" || pattern == value {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(".*") {
        return value == prefix || value.starts_with(&format!("{prefix}."));
    }
    false
}

fn subject_match(pattern: &str, identities: &[&str]) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || identities.iter().any(|id| *id == pattern)
}

fn role_match(pattern: &str, roles: &[String]) -> bool {
    let pattern = pattern.trim();
    pattern.is_empty() || pattern == "*" || roles.iter().any(|r| r == pattern)
}

fn resource_match(pattern: &str, selectors: &[&str]) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() || pattern == "*" {
        return true;
    }
    selectors.iter().any(|sel| pattern_match(pattern, sel))
}

fn conditions_match(
    conditions: &BTreeMap<String, String>,
    attrs: &BTreeMap<String, String>,
) -> bool {
    conditions
        .iter()
        .all(|(key, want)| attrs.get(key).map(String::as_str) == Some(want.as_str()))
}

#[cfg(test)]
mod tests;
