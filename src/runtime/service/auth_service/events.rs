//! Native auth domain-event emission.
//!
//! The authn/authz/apikey services publish domain events (defined in
//! `proto/udb/core/{authn,authz,apikey}/events/v1`) into the shared
//! transactional outbox (`udb_system.outbox_events`). The CDC engine tails that
//! table and relays each row to Apache Kafka; downstream consumers — notably the
//! Apache Spark streaming jobs under `analytics/spark/` — read the per-domain
//! topics. This module is the bridge that turns those previously-dead event
//! protos into real Kafka traffic.
//!
//! Emission is best-effort relative to the mutation (the mutation has already
//! committed when we enqueue): an enqueue failure is logged, never fails the
//! RPC. A fully transactional outbox (mutation + enqueue in one tx) is tracked
//! as a hardening follow-up; the wire contract here is already what the CDC
//! tailer and Spark consumers expect.

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// Canonical Kafka topic names for native auth events. These constants are the
/// registry of record — the matching `// Kafka topic:` comments in the event
/// protos document the same names for SDK consumers.
pub(crate) mod topics {
    // Topic taxonomy: domain.entity.verb.version — PERIODS ONLY, no underscores.
    // The matching `// Kafka topic:` comments in the event protos are kept in sync.
    // authn
    pub const USER_REGISTERED: &str = "udb.authn.user.registered.v1";
    pub const USER_LOGGED_IN: &str = "udb.authn.user.login.v1";
    pub const SESSION_REVOKED: &str = "udb.authn.session.revoked.v1";
    pub const USER_LOCKED: &str = "udb.authn.user.locked.v1";
    pub const PASSWORD_CHANGED: &str = "udb.authn.user.password.changed.v1";
    pub const OTP_SENT: &str = "udb.authn.otp.sent.v1";
    pub const USER_STATUS_CHANGED: &str = "udb.authn.user.status.changed.v1";
    pub const EMAIL_VERIFIED: &str = "udb.authn.user.email.verified.v1";
    // authz
    pub const ROLE_CREATED: &str = "udb.authz.role.created.v1";
    pub const ROLE_ASSIGNED: &str = "udb.authz.role.assigned.v1";
    pub const ROLE_REVOKED: &str = "udb.authz.role.revoked.v1";
    pub const ROLE_UPDATED: &str = "udb.authz.role.updated.v1";
    pub const ACCESS_DENIED: &str = "udb.authz.access.denied.v1";
    // authz policy governance (Phase K)
    pub const POLICY_DRAFT_CREATED: &str = "udb.authz.policy.draft.created.v1";
    pub const POLICY_DRAFT_UPDATED: &str = "udb.authz.policy.draft.updated.v1";
    pub const POLICY_DRAFT_SUBMITTED: &str = "udb.authz.policy.draft.submitted.v1";
    pub const POLICY_DRAFT_APPROVED: &str = "udb.authz.policy.draft.approved.v1";
    pub const POLICY_DRAFT_REJECTED: &str = "udb.authz.policy.draft.rejected.v1";
    pub const POLICY_VERSION_ACTIVATED: &str = "udb.authz.policy.version.activated.v1";
    pub const POLICY_VERSION_ROLLED_BACK: &str = "udb.authz.policy.version.rolledback.v1";
    // Progressive rollout: canary + metric-based auto-rollback (Phase 9).
    pub const POLICY_CANARY_ACTIVATED: &str = "udb.authz.policy.canary.activated.v1";
    pub const POLICY_CANARY_PROMOTED: &str = "udb.authz.policy.canary.promoted.v1";
    pub const POLICY_CANARY_ROLLED_BACK: &str = "udb.authz.policy.canary.rolledback.v1";
    pub const POLICY_CANARY_PAUSED: &str = "udb.authz.policy.canary.paused.v1";
    /// Durable cluster invalidation signal: a node observing this reloads its
    /// authz snapshot immediately and revokes cached SDK bundles below the new
    /// revision, instead of waiting for the local snapshot TTL to elapse.
    pub const POLICY_BUNDLE_INVALIDATED: &str = "udb.authz.policy.bundle.invalidated.v1";
    pub const DEVICE_REVOKED: &str = "udb.authn.device.revoked.v1";
    pub const TOKEN_REVOKED: &str = "udb.authn.token.revoked.v1";
    pub const REFRESH_REUSE_DETECTED: &str = "udb.authn.token.reuse.detected.v1";
    pub const MFA_FACTOR_DISABLED: &str = "udb.authn.mfa.factor.disabled.v1";
    pub const MFA_RESET: &str = "udb.authn.mfa.reset.v1";
    pub const SIGNING_KEY_ROTATED: &str = "udb.authn.signing.key.rotated.v1";
    // apikey
    pub const API_KEY_CREATED: &str = "udb.apikey.created.v1";
    pub const API_KEY_REVOKED: &str = "udb.apikey.revoked.v1";
    pub const API_KEY_UPDATED: &str = "udb.apikey.updated.v1";
    pub const API_KEY_ROTATED: &str = "udb.apikey.rotated.v1";

    // ── Phase L additions (compliance audit / required-events list) ───────────
    // authn — additional security-sensitive lifecycle events
    pub const LOGIN_FAILED: &str = "udb.authn.user.login.failed.v1";
    pub const MFA_ENROLLED: &str = "udb.authn.mfa.enrolled.v1";
    pub const MFA_CHANGED: &str = "udb.authn.mfa.changed.v1";
    pub const RECOVERY_CODES_GENERATED: &str = "udb.authn.recovery.codes.generated.v1";
    pub const RECOVERY_CODE_USED: &str = "udb.authn.recovery.code.used.v1";
    pub const OTP_VERIFIED: &str = "udb.authn.otp.verified.v1";
    pub const OTP_FAILED: &str = "udb.authn.otp.failed.v1";
    pub const PASSWORD_RESET_REQUESTED: &str = "udb.authn.password.reset.requested.v1";
    pub const PASSWORD_RESET_COMPLETED: &str = "udb.authn.password.reset.completed.v1";
    pub const PHONE_VERIFIED: &str = "udb.authn.user.phone.verified.v1";
    pub const SESSION_REFRESHED: &str = "udb.authn.session.refreshed.v1";
    // Emitted only from the `#[cfg(feature = "webauthn")]` registration/auth
    // ceremonies (authn/mod.rs); gate the definitions to match so the default
    // (webauthn-off) build doesn't see them as dead.
    #[cfg(feature = "webauthn")]
    pub const WEBAUTHN_REGISTERED: &str = "udb.authn.webauthn.registered.v1";
    #[cfg(feature = "webauthn")]
    pub const WEBAUTHN_AUTHENTICATED: &str = "udb.authn.webauthn.authenticated.v1";
    // apikey — additional events
    pub const API_KEY_VALIDATE_FAILED: &str = "udb.apikey.validate.failed.v1";
    pub const API_KEY_RATE_LIMITED: &str = "udb.apikey.rate.limited.v1";
    pub const API_KEY_ANOMALOUS_USE: &str = "udb.apikey.anomalous.use.v1";
    // authz — governance/mutation events (K-aligned; additive)
    pub const POLICY_SIMULATED: &str = "udb.authz.policy.simulated.v1";
    pub const POLICY_BUNDLE_ISSUED: &str = "udb.authz.policy.bundle.issued.v1";
    pub const POLICY_BUNDLE_REVOKED: &str = "udb.authz.policy.bundle.revoked.v1";
    pub const RELATIONSHIP_TUPLE_CHANGED: &str = "udb.authz.relationship.tuple.changed.v1";
    pub const NATIVE_ACCESS_GRANT_ISSUED: &str = "udb.authz.native.access.issued.v1";
    pub const NATIVE_ACCESS_GRANT_DENIED: &str = "udb.authz.native.access.denied.v1";
    pub const ACCESS_AUDIT_REQUIRED_ALLOW: &str = "udb.authz.access.audit.allow.v1";
    // operations — emergency / break-glass / readiness
    pub const OPS_KEY_ROTATION: &str = "udb.ops.key.rotation.v1";
    pub const OPS_EMERGENCY_REVOKE: &str = "udb.ops.emergency.revoke.v1";
    pub const OPS_EMERGENCY_DENY_ALL: &str = "udb.ops.emergency.denyall.v1";
    pub const OPS_TENANT_SUSPENDED: &str = "udb.ops.tenant.suspended.v1";
    pub const OPS_BREAK_GLASS_GRANT: &str = "udb.ops.breakglass.grant.v1";
    pub const OPS_READINESS_FAILURE: &str = "udb.ops.readiness.failure.v1";

    /// Wildcard patterns covering every native auth topic — for
    /// `UDB_CDC_VALID_TOPICS` allowlists and Kafka consumer subscriptions.
    ///
    /// `udb.idp.*` is included here so the CDC allowlist covers the IdP plane's
    /// local topics (see `auth_service::idp::events`) without folding the IdP
    /// constants into this module — the lower-risk consolidation (Phase L): the
    /// IdP handlers keep referencing their local consts, and this single shared
    /// allowlist guarantees `udb.idp.*` outbox rows are relayed to Kafka.
    /// `udb.ops.*` covers the operations-plane emergency/break-glass events.
    pub const AUTH_TOPIC_PATTERNS: &[&str] = &[
        "udb.authn.*",
        "udb.authz.*",
        "udb.apikey.*",
        "udb.idp.*",
        "udb.ops.*",
    ];

    /// The set of topics that are security-sensitive enough to require a fully
    /// populated compliance envelope (tenant, actor, target, operation, result,
    /// correlation id, reason, redaction profile) when running in enterprise
    /// audit mode. Matched by prefix so versioned topics stay covered.
    pub const SECURITY_SENSITIVE_PREFIXES: &[&str] = &[
        "udb.authn.",
        "udb.authz.",
        "udb.apikey.",
        "udb.idp.",
        "udb.ops.",
        // Phase 10: the unified compliance envelope covers the native data-plane
        // services too; in enterprise audit mode their events must carry
        // actor/operation/correlation (validation is gated on enterprise mode).
        "udb.tenant.",
        "udb.notification.",
        "udb.storage.",
        "udb.asset.",
        "udb.webrtc.",
    ];
}

/// Credential/secret JSON key names that must never reach the durable outbox or
/// Kafka. Matched case-insensitively against every key in an event payload.
const BANNED_PAYLOAD_KEYS: &[&str] = &[
    "password",
    "password_hash",
    "totp_secret",
    "totp_secret_enc",
    "secret",
    "code_hash",
    "session_id",
    "session_token",
    "session_token_hash",
    "session_token_lookup",
    "csrf_token",
    "csrf_token_hash",
    "key_hash",
    "api_key",
    "recovery_code",
    "recovery_code_hash",
    "otp_code",
    "private_key",
    "refresh_token",
    "access_token",
    "bearer_token",
];

fn auth_payload_contains_banned_key(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, child)| {
            BANNED_PAYLOAD_KEYS
                .iter()
                .any(|banned| key.eq_ignore_ascii_case(banned))
                || auth_payload_contains_banned_key(child)
        }),
        serde_json::Value::Array(items) => items.iter().any(auth_payload_contains_banned_key),
        _ => false,
    }
}

fn looks_like_raw_credential_or_lookup(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.starts_with("sess_")
        || trimmed.starts_with("hmac-sha256:")
        || (trimmed.starts_with("udbk_") && trimmed.contains('.'))
}

fn validate_auth_event_surface(event: &AuthEvent) -> Result<(), String> {
    if auth_payload_contains_banned_key(&event.body) {
        return Err(format!(
            "auth event {} payload contains credential-shaped field names",
            event.topic
        ));
    }
    if looks_like_raw_credential_or_lookup(&event.document_id) {
        return Err(format!(
            "auth event {} document_id contains credential material",
            event.topic
        ));
    }
    if looks_like_raw_credential_or_lookup(&event.correlation_id) {
        return Err(format!(
            "auth event {} correlation_id contains credential material",
            event.topic
        ));
    }
    Ok(())
}

/// Recursively replace the value of any credential/secret-named key with
/// `"[REDACTED]"` (Phase 1 / plan G2.4). This is defense-in-depth behind the
/// Phase 0 DTO redaction: even if a handler mistakenly stuffs a hash or token
/// into an event body, it can never reach the durable audit trail. The authz
/// access-decision audit is written as typed columns (no free-form payload), so
/// it has no equivalent blob to scrub.
pub(crate) fn redact_auth_payload(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if BANNED_PAYLOAD_KEYS
                    .iter()
                    .any(|banned| key.eq_ignore_ascii_case(banned))
                {
                    *child = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_auth_payload(child);
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items.iter_mut() {
                redact_auth_payload(item);
            }
        }
        _ => {}
    }
}

/// The current redaction-profile version. Bumped whenever the banned-key set or
/// the masking algorithm changes, so downstream consumers can detect a
/// re-redaction of historical events.
pub(crate) const REDACTION_PROFILE_VERSION: &str = "v1";

/// Mask a source IP for the compliance envelope: keep the network portion and
/// zero the host portion (IPv4 → /24, IPv6 → /48). Non-IP strings pass through a
/// length-bounded passthrough so we never store a raw client identifier when the
/// caller already masked it. Empty input stays empty.
pub(crate) fn mask_source_ip(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    if let Ok(v4) = raw.parse::<std::net::Ipv4Addr>() {
        let o = v4.octets();
        return format!("{}.{}.{}.0/24", o[0], o[1], o[2]);
    }
    if let Ok(v6) = raw.parse::<std::net::Ipv6Addr>() {
        let s = v6.segments();
        return format!("{:x}:{:x}:{:x}::/48", s[0], s[1], s[2]);
    }
    // Already-masked or hostname form: keep it but never longer than 64 chars.
    raw.chars().take(64).collect()
}

/// Stable, non-reversible hash of a user-agent string (FNV-1a 64) so the
/// compliance envelope can correlate clients without storing the raw UA.
pub(crate) fn hash_user_agent(raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return String::new();
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in raw.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("ua_{hash:016x}")
}

/// True when this topic is security-sensitive and therefore subject to the
/// full compliance-envelope validation in enterprise audit mode.
pub(crate) fn is_security_sensitive_topic(topic: &str) -> bool {
    topics::SECURITY_SENSITIVE_PREFIXES
        .iter()
        .any(|p| topic.starts_with(p))
}

/// The structured compliance envelope (Phase L1). Every security-sensitive auth
/// event carries this metadata alongside its domain `body`. Field names are
/// aligned with CloudEvents core attributes (`source`, `subject`,
/// `datacontenttype`) and OpenTelemetry log/event fields (`trace_id`,
/// `span_id`) where that does not weaken UDB-specific compliance data.
///
/// Phase 10 (telemetry coherence): this is the ONE compliance envelope shape for
/// every security-sensitive lane. The auth sink populates it inline; the native
/// data-plane services (storage/asset/webrtc/notification/tenant) populate it via
/// [`build_native_compliance_envelope`] so a native audit row carries the SAME
/// field set as an auth row (no second divergent shape).
#[derive(Clone, Default)]
pub struct ComplianceEnvelope {
    /// Acting principal subject (CloudEvents-style actor).
    pub actor: String,
    /// Acting principal's project (tenant lives on `AuthEvent::tenant_id`).
    pub actor_project: String,
    /// Resource being acted upon (CloudEvents `subject`).
    pub target_resource: String,
    /// Target tenant/project when different from the actor's.
    pub target_tenant: String,
    pub target_project: String,
    /// Operation performed (verb), e.g. "create", "revoke", "deny".
    pub operation: String,
    /// Outcome: "success" | "failure" | "deny" | "allow".
    pub outcome: String,
    /// Machine-readable reason / deny code.
    pub reason_code: String,
    /// Authentication method used (e.g. "password", "mfa_totp", "passkey").
    pub auth_method: String,
    /// Verified `udb.core.common.v1.CredentialType` and its non-secret durable
    /// lineage (`jti`, API-key prefix, or certificate-binding id).
    pub credential_type: i32,
    pub credential_id: String,
    /// Immutable service identity carried by the verified credential/grant.
    pub service_identity: String,
    /// Identity-assurance level (e.g. "aal1", "aal2").
    pub assurance_level: String,
    /// Identity-provider id, when the event originated from an IdP flow.
    pub provider_id: String,
    /// Raw source IP — masked into the envelope, never stored verbatim.
    pub source_ip: String,
    /// Raw user-agent — hashed into the envelope, never stored verbatim.
    pub user_agent: String,
    /// Distributed-trace ids (OTel) for the originating RPC.
    pub trace_id: String,
    pub span_id: String,
    /// Authz decision linkage.
    pub decision_id: String,
    pub policy_version: String,
    pub relationship_version: String,
}

/// Resolve the OTel `(trace_id, span_id)` to stamp on an envelope. Prefers values
/// already on the [`ComplianceEnvelope`] (a caller may have set them explicitly);
/// otherwise falls back to the ambient request trace context populated by
/// [`crate::runtime::otel::TraceExtractLayer`] from the inbound `traceparent`.
/// This is a read-only call into the trace scope — no restructuring of the
/// builders — so an inbound trace flows into the audit row instead of the prior
/// empty strings (Phase 10).
fn resolve_trace_ids(env: &ComplianceEnvelope) -> (String, String) {
    if !env.trace_id.is_empty() || !env.span_id.is_empty() {
        return (env.trace_id.clone(), env.span_id.clone());
    }
    let ctx = crate::runtime::otel::current_trace_context();
    // The local span we own is `span_id` when assigned, else the inbound parent
    // span (so the audit row links to a real span in the inbound trace).
    let span = if !ctx.span_id.is_empty() {
        ctx.span_id.clone()
    } else {
        ctx.parent_span_id.clone()
    };
    (ctx.trace_id, span)
}

/// One domain event to publish. `topic` routes it on Kafka; `document_id` is the
/// aggregate id used as the outbox/Kafka partition key (per-aggregate ordering);
/// `body` carries the proto event's domain fields; `compliance` carries the
/// Phase L1 audit envelope.
pub(crate) struct AuthEvent {
    pub topic: &'static str,
    pub document_id: String,
    pub tenant_id: String,
    pub correlation_id: String,
    pub body: serde_json::Value,
    pub compliance: ComplianceEnvelope,
    /// Whether a handler attached a `ComplianceEnvelope` via [`Self::with_compliance`]
    /// that carried an EXPLICIT non-empty `actor`. The validator checks this BEFORE
    /// the `document_id` fallback so an attached-but-empty actor is rejected in
    /// enterprise audit mode instead of being silently masked by the fallback.
    actor_explicit: bool,
    /// As `actor_explicit`, for `target_resource`.
    target_explicit: bool,
}

impl AuthEvent {
    pub(crate) fn new(
        topic: &'static str,
        document_id: impl Into<String>,
        tenant_id: impl Into<String>,
        body: serde_json::Value,
    ) -> Self {
        let document_id = document_id.into();
        let tenant_id = tenant_id.into();
        Self {
            topic,
            // Default the envelope actor/target from the routing fields so a
            // handler that only fills the legacy `new(...)` shape still produces
            // a partially-populated envelope (full population happens when a
            // handler attaches a `ComplianceEnvelope`). `*_explicit` stay false:
            // a bare `new(...)` did not assert an audited actor/target.
            compliance: ComplianceEnvelope {
                actor: document_id.clone(),
                target_resource: document_id.clone(),
                ..ComplianceEnvelope::default()
            },
            document_id,
            tenant_id,
            correlation_id: String::new(),
            body,
            actor_explicit: false,
            target_explicit: false,
        }
    }

    pub(crate) fn with_correlation(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = correlation_id.into();
        self
    }

    /// Attach (or overwrite) the full compliance envelope (Phase L1). Records
    /// whether the supplied envelope carried an explicit `actor`/`target_resource`
    /// (so the validator can enforce them before the `document_id` fallback), then
    /// falls those fields back to `document_id` for the persisted envelope so a
    /// partially-populated handler still produces a routable audit row.
    pub(crate) fn with_compliance(mut self, mut env: ComplianceEnvelope) -> Self {
        self.actor_explicit = !env.actor.trim().is_empty();
        self.target_explicit = !env.target_resource.trim().is_empty();
        if env.actor.trim().is_empty() {
            env.actor = self.document_id.clone();
        }
        if env.target_resource.trim().is_empty() {
            env.target_resource = self.document_id.clone();
        }
        if env.credential_type == 0 {
            env.credential_type = crate::runtime::otel::current_credential_type();
        }
        if env.credential_id.trim().is_empty() {
            env.credential_id = crate::runtime::otel::current_credential_id();
        }
        if env.service_identity.trim().is_empty() {
            env.service_identity = crate::runtime::otel::current_service_identity();
        }
        self.compliance = env;
        self
    }
}

/// Validate the compliance envelope (Phase L1/L3-task1). In enterprise audit
/// mode the sink rejects a security-sensitive event before enqueue when it is
/// missing any required compliance field. Returns the first missing-field name.
pub(crate) fn validate_compliance_envelope(event: &AuthEvent) -> Result<(), String> {
    if !is_security_sensitive_topic(event.topic) {
        return Ok(());
    }
    let env = &event.compliance;
    let mut missing: Vec<&str> = Vec::new();
    if event.tenant_id.trim().is_empty() {
        missing.push("tenant_id");
    }
    // Validate the EXPLICIT actor/target the handler attached, BEFORE the
    // `document_id` fallback applied in `with_compliance`/`new`. A security-
    // sensitive event must carry an actor/target the handler asserted on a
    // compliance envelope: a bare `new(...)` (no envelope) or an attached
    // envelope with an empty actor/target is rejected in enterprise mode rather
    // than silently audited with the routing id standing in for a real subject.
    if !event.actor_explicit || env.actor.trim().is_empty() {
        missing.push("actor");
    }
    if !event.target_explicit || env.target_resource.trim().is_empty() {
        missing.push("target");
    }
    if env.operation.trim().is_empty() {
        missing.push("operation");
    }
    if env.outcome.trim().is_empty() {
        missing.push("result");
    }
    if event.correlation_id.trim().is_empty() {
        missing.push("correlation_id");
    }
    if env.reason_code.trim().is_empty() {
        missing.push("reason");
    }
    // Redaction-profile presence check (parity with the native validator's
    // `redaction` field). The auth lane stamps a compile-time profile id on every
    // persisted envelope; assert it is configured so a build that blanked it can
    // never produce an unattributable audit row.
    if REDACTION_PROFILE_VERSION.trim().is_empty() {
        missing.push("redaction");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "auth event {} is missing required compliance fields: {}",
            event.topic,
            missing.join(", ")
        ))
    }
}

/// Whether the process runs in enterprise audit mode. In this mode the sink
/// enforces the full compliance envelope (Phase L1). Off by default so existing
/// best-effort handlers keep working; flip on with `UDB_ENTERPRISE_AUDIT=1`.
pub(crate) fn enterprise_audit_mode() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("UDB_ENTERPRISE_AUDIT")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

/// Required compliance fields for a security-sensitive NATIVE data-plane event
/// (storage/asset/webrtc/notification/tenant). The native lane fails closed in
/// enterprise audit mode when any of these is empty for a `udb.*` topic; this
/// mirrors [`validate_compliance_envelope`] for the auth lane. Returns the first
/// missing field name (so the caller can log/reject before enqueue).
///
/// `redaction_present` reflects whether the native envelope carries redaction
/// metadata (it always does — `redaction_mode`/`redaction_version`/`redacted_fields`),
/// so the only fields that can actually be missing are tenant/actor/operation/
/// correlation. Kept as a parameter so the validator stays pure and testable.
pub fn validate_native_compliance(
    topic: &str,
    tenant_id: &str,
    actor: &str,
    operation: &str,
    correlation_id: &str,
    redaction_present: bool,
) -> Result<(), String> {
    if !is_security_sensitive_topic(topic) {
        return Ok(());
    }
    let mut missing: Vec<&str> = Vec::new();
    if tenant_id.trim().is_empty() {
        missing.push("tenant_id");
    }
    if actor.trim().is_empty() {
        missing.push("actor");
    }
    if operation.trim().is_empty() {
        missing.push("operation");
    }
    if correlation_id.trim().is_empty() {
        missing.push("correlation_id");
    }
    if !redaction_present {
        missing.push("redaction");
    }
    if missing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "native event {topic} is missing required compliance fields: {}",
            missing.join(", ")
        ))
    }
}

/// Build the shared compliance envelope JSON for a NATIVE data-plane event so it
/// carries the SAME field set as [`OutboxAuthEventSink::envelope`]
/// (tenant/project/actor/action/resource/correlation_id/decision_id/policy_version/
/// auth_method/outcome/redaction + CloudEvents `specversion/source/subject/...`).
///
/// `event_id`/`topic`/`partition_key` are the routing fields; `tenant_id`/
/// `project_id` scope it; `env` carries the available compliance context (a
/// native service supplies actor/operation/outcome at minimum; decision_id/
/// policy_version/auth_method are optional). `redaction_*` come from the native
/// CDC redaction profile (the native lane already redacts via manifest metadata
/// downstream, so the envelope records the declared mode/version/fields). The
/// `payload` is the already-built domain payload (the caller owns its redaction).
#[allow(clippy::too_many_arguments)]
pub fn build_native_compliance_envelope(
    event_id: &str,
    topic: &str,
    partition_key: &str,
    tenant_id: &str,
    project_id: &str,
    env: &ComplianceEnvelope,
    correlation_id: &str,
    redaction_mode: &str,
    redaction_version: u32,
    redacted_fields: &[String],
    payload: serde_json::Value,
) -> serde_json::Value {
    let now = Utc::now().to_rfc3339();
    let (trace_id, span_id) = resolve_trace_ids(env);
    let actor = if env.actor.trim().is_empty() {
        partition_key
    } else {
        env.actor.as_str()
    };
    let target = if env.target_resource.trim().is_empty() {
        partition_key
    } else {
        env.target_resource.as_str()
    };
    let mut out = json!({
        // Core envelope (CDC `EventEnvelope` contract — unchanged so existing
        // rows and the CDC tailer keep decoding).
        "event_id": event_id,
        "event_type": topic,
        "timestamp": now,
        "correlation_id": correlation_id,
        "document_id": partition_key,
        "payload": payload,
        // Native CDC redaction metadata (decoded by `EventEnvelope`).
        "redaction_mode": redaction_mode,
        "redaction_version": redaction_version,
        "redacted_fields": redacted_fields,
        // ── Phase L1 / Phase 10 compliance envelope (additive optional fields) ──
        "specversion": "1.0",
        "source": format!("udb.native/{tenant_id}"),
        "subject": target,
        "datacontenttype": "application/json",
        "time": now,
        // CDC routing + tenant-scope validation: emit the top-level `tenant_id`/
        // `project_id` the CDC `EventEnvelope` decodes, else every `udb.*` event
        // is rejected as TenantScopeMissing and never published.
        "tenant_id": tenant_id,
        "project_id": project_id,
        "actor": actor,
        "actor_tenant": tenant_id,
        "actor_project": if env.actor_project.is_empty() { project_id } else { env.actor_project.as_str() },
        "target_resource": target,
        "target_tenant": if env.target_tenant.is_empty() { tenant_id } else { env.target_tenant.as_str() },
        "target_project": if env.target_project.is_empty() { project_id } else { env.target_project.as_str() },
        "auth_method": env.auth_method,
        "credential_type": if env.credential_type == 0 {
            crate::runtime::otel::current_credential_type()
        } else {
            env.credential_type
        },
        "credential_id": if env.credential_id.is_empty() {
            crate::runtime::otel::current_credential_id()
        } else {
            env.credential_id.clone()
        },
        "service_identity": if env.service_identity.is_empty() {
            crate::runtime::otel::current_service_identity()
        } else {
            env.service_identity.clone()
        },
        "assurance_level": env.assurance_level,
        "provider_id": env.provider_id,
        "source_ip": mask_source_ip(&env.source_ip),
        "user_agent_hash": hash_user_agent(&env.user_agent),
        "trace_id": trace_id,
        "span_id": span_id,
        "decision_id": env.decision_id,
        "policy_version": env.policy_version,
        "relationship_version": env.relationship_version,
        "operation": env.operation,
        "outcome": env.outcome,
        "reason_code": env.reason_code,
        // String redaction-PROFILE id ships alongside the numeric CDC version
        // (same split the auth envelope uses to avoid a serde type clash).
        "redaction_profile": REDACTION_PROFILE_VERSION,
        "occurred_at": now,
    });
    // Only attach tenant_id/project_id at the envelope top level when present, so
    // the CDC tenant-scope classifier behaves exactly as the prior native helper
    // (a non-tenant-scoped topic stays unscoped).
    if let Some(obj) = out.as_object_mut() {
        if !tenant_id.trim().is_empty() {
            obj.insert("tenant_id".to_string(), json!(tenant_id));
        }
        if !project_id.trim().is_empty() {
            obj.insert("project_id".to_string(), json!(project_id));
        }
    }
    out
}

/// Publishes native auth domain events. Implementations either write to the
/// shared outbox (production) or discard (when no Postgres pool is wired).
#[async_trait]
pub(crate) trait AuthEventSink: Send + Sync {
    async fn emit(&self, event: AuthEvent) -> Result<(), String>;

    /// Persist the audit/outbox row for `event` on an existing transaction
    /// connection, so the event is durable in the SAME transaction as the
    /// caller's mutation (enterprise atomicity — final_task.md §7: "auth mutations
    /// either commit with their event or fail"). The default falls back to a
    /// non-atomic `emit` for sinks without a transactional store (e.g. the no-op
    /// sink); `OutboxAuthEventSink` overrides it to write on `conn`.
    async fn write_in_tx(
        &self,
        _conn: &mut sqlx::PgConnection,
        event: AuthEvent,
    ) -> Result<(), String> {
        self.emit(event).await
    }
}

/// No-op sink for services constructed without a durable backend (fail-closed
/// `new()` constructors, snapshot-only authz). Native serving always wires the
/// outbox sink in `build_auth_services`.
pub(crate) struct NoopAuthEventSink;

#[async_trait]
impl AuthEventSink for NoopAuthEventSink {
    async fn emit(&self, event: AuthEvent) -> Result<(), String> {
        validate_auth_event_surface(&event)?;
        if enterprise_audit_mode() {
            validate_compliance_envelope(&event)?;
        }
        Ok(())
    }
}

/// Default sink: a no-op. Use `Arc<dyn AuthEventSink>` fields so production can
/// swap in the outbox sink without changing handler code.
pub(crate) fn noop_sink() -> Arc<dyn AuthEventSink> {
    Arc::new(NoopAuthEventSink)
}

/// Outbox-backed sink. Inserts the enriched event envelope into the same
/// `udb_system.outbox_events` relation the CDC engine tails, so auth events
/// reach Kafka through the established relay (no second publish path).
pub(crate) struct OutboxAuthEventSink {
    pool: sqlx::PgPool,
    outbox_relation: String,
    /// Additional immutable export targets (Phase L3 task2): Postgres durable
    /// audit table, stdout/file, SIEM webhook. Empty = outbox-only.
    export_sinks: Vec<Arc<dyn super::audit_export::AuditExportSink>>,
    /// Records `audit_sink_failures` so a degraded export pipeline is alertable.
    metrics: Arc<dyn crate::metrics::MetricsRecorder>,
}

impl OutboxAuthEventSink {
    /// `outbox_relation` is the schema-qualified table name, e.g.
    /// `"udb_system"."outbox_events"` (from `CdcConfig::outbox_relation`).
    pub(crate) fn new(pool: sqlx::PgPool, outbox_relation: impl Into<String>) -> Self {
        Self {
            pool,
            outbox_relation: outbox_relation.into(),
            export_sinks: Vec::new(),
            metrics: Arc::new(crate::metrics::NoopMetrics),
        }
    }

    /// Attach the configured immutable-export sinks (Phase L3 task2).
    pub(crate) fn with_exports(
        mut self,
        export_sinks: Vec<Arc<dyn super::audit_export::AuditExportSink>>,
    ) -> Self {
        self.export_sinks = export_sinks;
        self
    }

    /// Wire the shared metrics recorder so audit-sink failures are counted.
    pub(crate) fn with_metrics(
        mut self,
        metrics: Arc<dyn crate::metrics::MetricsRecorder>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    /// Build the consumer-facing envelope. This MUST match the CDC engine's
    /// `EventEnvelope` (`src/runtime/cdc/mod.rs`) so `process_outbox_event`
    /// deserializes it and forwards to Kafka — same shape the broker's
    /// `prepare_outbox_envelope` produces. Required fields: event_id, event_type,
    /// correlation_id. `timestamp` is the envelope's event time; the domain
    /// event's own fields (including tenant_id) live under `payload`.
    fn envelope(event_id: &Uuid, event: &AuthEvent) -> serde_json::Value {
        // Scrub credential/secret-named keys from the payload before it is
        // persisted (defense-in-depth behind Phase 0 DTO redaction).
        let mut payload = event.body.clone();
        redact_auth_payload(&mut payload);
        let env = &event.compliance;
        let now = Utc::now().to_rfc3339();
        let (trace_id, span_id) = resolve_trace_ids(env);
        json!({
            // Core envelope (CDC `EventEnvelope` contract — unchanged).
            "event_id": event_id.to_string(),
            "event_type": event.topic,
            "timestamp": now,
            "correlation_id": event.correlation_id,
            "document_id": event.document_id,
            "payload": payload,
            // CDC routing + tenant-scope validation: the envelope MUST carry the
            // top-level `tenant_id` the CDC `EventEnvelope` decodes. Without it
            // every `udb.*` (tenant-scoped) event deserializes with an empty
            // tenant and is rejected by `validate_topic_tenant_scope` as
            // TenantScopeMissing — DLQ'd, never published.
            "tenant_id": event.tenant_id,
            "project_id": env.actor_project,
            // ── Phase L1 compliance envelope ──────────────────────────────────
            // CloudEvents-compatible core attributes.
            "specversion": "1.0",
            "source": format!("udb.auth/{}", event.tenant_id),
            "subject": env.target_resource,
            "datacontenttype": "application/json",
            "time": now,
            // Actor / target identity.
            "actor": env.actor,
            "actor_tenant": event.tenant_id,
            "actor_project": env.actor_project,
            "target_resource": env.target_resource,
            "target_tenant": if env.target_tenant.is_empty() { event.tenant_id.clone() } else { env.target_tenant.clone() },
            "target_project": env.target_project,
            // Auth context.
            "auth_method": env.auth_method,
            "credential_type": env.credential_type,
            "credential_id": env.credential_id,
            "service_identity": env.service_identity,
            "assurance_level": env.assurance_level,
            "provider_id": env.provider_id,
            "source_ip": mask_source_ip(&env.source_ip),
            "user_agent_hash": hash_user_agent(&env.user_agent),
            // OpenTelemetry trace correlation (from the envelope, else the inbound
            // `traceparent` scoped by TraceExtractLayer).
            "trace_id": trace_id,
            "span_id": span_id,
            // Authz decision linkage.
            "decision_id": env.decision_id,
            "policy_version": env.policy_version,
            "relationship_version": env.relationship_version,
            // Outcome + compliance bookkeeping.
            "operation": env.operation,
            "outcome": env.outcome,
            "reason_code": env.reason_code,
            // NOTE: the CDC `EventEnvelope` reserves a numeric `redaction_version`
            // (u32). The compliance redaction-PROFILE id is a string, so it ships
            // under `redaction_profile` to avoid a serde type clash on decode.
            "redaction_profile": REDACTION_PROFILE_VERSION,
            "occurred_at": now,
            // Set by the export pipeline once the row is durably exported.
            "immutable_export_status": "pending",
        })
    }

    /// Writes the audit/outbox row for `event` on the provided executor and
    /// returns the persisted envelope. Pass `&self.pool` for a standalone write,
    /// or `&mut *tx` so the row is durable in the **same transaction** as the
    /// caller's mutation — the enterprise-mode atomicity primitive (final_task.md
    /// §7: "auth mutations either commit with their event or fail"). Validation
    /// and the compliance-envelope gate run here, before the insert. Does NOT run
    /// the export-sink fan-out (that is post-durability; the caller runs it after
    /// committing).
    pub(crate) async fn write_outbox_row<'c, E>(
        &self,
        executor: E,
        event: &AuthEvent,
    ) -> Result<serde_json::Value, String>
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        validate_auth_event_surface(event)?;
        // Phase L1: reject a security-sensitive event lacking a complete
        // compliance envelope BEFORE it is enqueued.
        if enterprise_audit_mode() {
            validate_compliance_envelope(event)?;
        }
        let event_id = Uuid::new_v4();
        let envelope = Self::envelope(&event_id, event);
        // ONE shared insert path — EVERY outbox writer uses this same function, so the
        // SQL + bind types can never drift again (§4).
        crate::runtime::cdc::insert_outbox_row(
            executor,
            &self.outbox_relation,
            event_id,
            event.topic,
            &event.document_id,
            &envelope,
        )
        .await
        .map_err(|e| format!("auth outbox enqueue failed for topic {}: {e}", event.topic))?;
        Ok(envelope)
    }

    async fn record_auth_outbox_lag<'c, E>(&self, executor: E)
    where
        E: sqlx::Executor<'c, Database = sqlx::Postgres>,
    {
        let sql = format!(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))), 0)::DOUBLE PRECISION \
             FROM {rel} \
             WHERE delivery_state IN ('pending', 'publishing') \
               AND (topic LIKE 'udb.authn.%' \
                    OR topic LIKE 'udb.authz.%' \
                    OR topic LIKE 'udb.apikey.%' \
                    OR topic LIKE 'udb.idp.%' \
                    OR topic LIKE 'udb.ops.%')",
            rel = self.outbox_relation
        );
        match sqlx::query_scalar::<_, f64>(&sql).fetch_one(executor).await {
            Ok(seconds) => self.metrics.set_auth_outbox_lag_seconds(seconds.max(0.0)),
            Err(err) => tracing::warn!(
                error = %err,
                "failed to sample auth outbox lag"
            ),
        }
    }
}

#[async_trait]
impl AuthEventSink for OutboxAuthEventSink {
    async fn emit(&self, event: AuthEvent) -> Result<(), String> {
        // Durability anchor: validation + the compliance gate + the outbox INSERT
        // all run inside `write_outbox_row` (here on the sink's own pool). A
        // handler that needs the event atomic with its mutation calls
        // `write_outbox_row` with its own transaction instead of the sink.
        let envelope = self.write_outbox_row(&self.pool, &event).await?;
        self.record_auth_outbox_lag(&self.pool).await;
        // Phase L3 task2: fan the already-redacted envelope out to the additional
        // immutable export sinks. The outbox write above is the durability
        // anchor, so an export-sink failure is recorded (audit_sink_failures)
        // and logged but does NOT fail the RPC — the event is already durable.
        for sink in &self.export_sinks {
            if let Err(err) = sink.export(event.topic, &envelope).await {
                self.metrics.record_audit_sink_failure(sink.name());
                tracing::warn!(sink = sink.name(), topic = event.topic, error = %err,
                    "auth audit export sink failed");
            }
        }
        Ok(())
    }

    async fn write_in_tx(
        &self,
        conn: &mut sqlx::PgConnection,
        event: AuthEvent,
    ) -> Result<(), String> {
        // Atomic path: persist ONLY the durable outbox row on the caller's
        // transaction connection so it commits with the mutation. Export-sink
        // fan-out is skipped here (post-durability; the CDC relay forwards the
        // committed row).
        //
        // The lag-metric sample is DELIBERATELY NOT run on this connection: it is
        // a best-effort observability query, and ANY failure of it on the tx
        // connection (a schema/shape mismatch, a transient error) aborts the
        // caller's transaction in Postgres — silently turning the subsequent
        // commit into a rollback that drops the mutation AND this outbox row while
        // the handler still returns Ok. Lag is sampled off-tx by the `emit` path
        // and the background poller instead.
        self.write_outbox_row(&mut *conn, &event).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_banned_keys_recursively() {
        let mut value = json!({
            "user_id": "u1",
            "password_hash": "argon2id$secret",
            "nested": { "totp_secret_enc": "ciphertext", "ok": "keep" },
            "list": [{ "api_key": "k_live_xxx" }, { "email": "a@b.com" }],
        });
        redact_auth_payload(&mut value);
        assert_eq!(value["password_hash"], json!("[REDACTED]"));
        assert_eq!(value["nested"]["totp_secret_enc"], json!("[REDACTED]"));
        assert_eq!(value["nested"]["ok"], json!("keep"));
        assert_eq!(value["list"][0]["api_key"], json!("[REDACTED]"));
        // Non-credential fields are preserved.
        assert_eq!(value["user_id"], json!("u1"));
        assert_eq!(value["list"][1]["email"], json!("a@b.com"));
    }

    #[test]
    fn envelope_scrubs_payload_before_persisting() {
        let event = AuthEvent::new(
            topics::USER_REGISTERED,
            "doc-1",
            "acme",
            json!({ "password": "p4ssw0rd", "session_token_hash": "h", "user_id": "u1" }),
        );
        let envelope = OutboxAuthEventSink::envelope(&Uuid::nil(), &event);
        assert_eq!(envelope["payload"]["password"], json!("[REDACTED]"));
        assert_eq!(
            envelope["payload"]["session_token_hash"],
            json!("[REDACTED]")
        );
        assert_eq!(envelope["payload"]["user_id"], json!("u1"));
        assert_eq!(envelope["event_type"], json!(topics::USER_REGISTERED));
    }

    #[test]
    fn rejects_sensitive_event_payload_keys_and_partition_keys() {
        let payload_event = AuthEvent::new(
            topics::SESSION_REVOKED,
            "sesspub_abc",
            "acme",
            json!({ "session_id": "sess_raw", "user_id": "u1" }),
        );
        assert!(validate_auth_event_surface(&payload_event).is_err());

        let partition_event = AuthEvent::new(
            topics::SESSION_REVOKED,
            "sess_raw",
            "acme",
            json!({ "session_public_id": "sesspub_abc", "user_id": "u1" }),
        );
        assert!(validate_auth_event_surface(&partition_event).is_err());

        let safe_event = AuthEvent::new(
            topics::SESSION_REVOKED,
            "sesspub_abc",
            "acme",
            json!({ "session_public_id": "sesspub_abc", "user_id": "u1" }),
        );
        assert!(validate_auth_event_surface(&safe_event).is_ok());
    }

    fn complete_envelope() -> ComplianceEnvelope {
        ComplianceEnvelope {
            actor: "u1".to_string(),
            target_resource: "messages/orders".to_string(),
            operation: "deny".to_string(),
            outcome: "deny".to_string(),
            reason_code: "no_matching_policy".to_string(),
            ..ComplianceEnvelope::default()
        }
    }

    #[test]
    fn compliance_validation_rejects_incomplete_security_event() {
        // Missing operation/outcome/reason/correlation → rejected.
        let incomplete = AuthEvent::new(
            topics::ACCESS_DENIED,
            "u1",
            "acme",
            json!({ "user_id": "u1" }),
        );
        let err = validate_compliance_envelope(&incomplete).unwrap_err();
        assert!(err.contains("operation"), "{err}");
        assert!(err.contains("correlation_id"), "{err}");
        assert!(err.contains("reason"), "{err}");
    }

    #[test]
    fn compliance_validation_accepts_complete_security_event() {
        let complete = AuthEvent::new(
            topics::ACCESS_DENIED,
            "u1",
            "acme",
            json!({ "user_id": "u1" }),
        )
        .with_correlation("corr-123")
        .with_compliance(complete_envelope());
        assert!(validate_compliance_envelope(&complete).is_ok());
    }

    #[test]
    fn compliance_validation_requires_explicit_actor_and_target() {
        // A handler that attached a compliance envelope WITHOUT an actor/target
        // (so it would have relied on the `document_id` fallback) must be rejected
        // for a security-sensitive topic — the fallback no longer masks the gap.
        let no_actor = AuthEvent::new(
            topics::ACCESS_DENIED,
            "u1",
            "acme",
            json!({ "user_id": "u1" }),
        )
        .with_correlation("corr-123")
        .with_compliance(ComplianceEnvelope {
            // actor + target_resource left empty → fall back to document_id, but
            // `*_explicit` stays false so the validator still flags them.
            operation: "deny".to_string(),
            outcome: "deny".to_string(),
            reason_code: "no_policy".to_string(),
            ..ComplianceEnvelope::default()
        });
        let err = validate_compliance_envelope(&no_actor).unwrap_err();
        assert!(err.contains("actor"), "{err}");
        assert!(err.contains("target"), "{err}");

        // A bare `new(...)` with no envelope attached is likewise rejected even
        // though the routing id populated actor/target on the persisted envelope.
        let bare = AuthEvent::new(topics::ACCESS_DENIED, "u1", "acme", json!({}))
            .with_correlation("corr-1");
        let err = validate_compliance_envelope(&bare).unwrap_err();
        assert!(err.contains("actor"), "{err}");
        assert!(err.contains("target"), "{err}");
    }

    #[test]
    fn compliance_validation_skips_non_security_topics() {
        // A non-prefixed topic is not subject to envelope enforcement.
        let event = AuthEvent::new("analytics.rollup.v1", "doc", "acme", json!({}));
        assert!(validate_compliance_envelope(&event).is_ok());
    }

    #[test]
    fn source_ip_is_masked_in_envelope() {
        assert_eq!(mask_source_ip("203.0.113.42"), "203.0.113.0/24");
        assert_eq!(mask_source_ip("2001:db8:abcd:1::1"), "2001:db8:abcd::/48");
        assert_eq!(mask_source_ip(""), "");
        // A full IP must never survive verbatim.
        assert!(!mask_source_ip("198.51.100.7").contains(".7"));
    }

    #[test]
    fn user_agent_is_hashed_not_stored_raw() {
        let h = hash_user_agent("Mozilla/5.0 (secret-build 1234)");
        assert!(h.starts_with("ua_"));
        assert!(!h.contains("Mozilla"));
        assert!(!h.contains("secret"));
        // Stable across calls.
        assert_eq!(h, hash_user_agent("Mozilla/5.0 (secret-build 1234)"));
        assert_eq!(hash_user_agent(""), "");
    }

    #[test]
    fn envelope_carries_cloudevents_and_otel_attributes() {
        let event = AuthEvent::new(
            topics::ACCESS_DENIED,
            "u1",
            "acme",
            json!({ "user_id": "u1" }),
        )
        .with_correlation("corr-123")
        .with_compliance(ComplianceEnvelope {
            source_ip: "203.0.113.42".to_string(),
            user_agent: "curl/8".to_string(),
            decision_id: "dec-1".to_string(),
            policy_version: "pv-7".to_string(),
            credential_type: 3,
            credential_id: "key_abc123".to_string(),
            service_identity: "svc:orders".to_string(),
            ..complete_envelope()
        });
        let env = OutboxAuthEventSink::envelope(&Uuid::nil(), &event);
        assert_eq!(env["specversion"], json!("1.0"));
        assert_eq!(env["datacontenttype"], json!("application/json"));
        assert_eq!(env["subject"], json!("messages/orders"));
        assert_eq!(env["source_ip"], json!("203.0.113.0/24"));
        assert!(env["user_agent_hash"].as_str().unwrap().starts_with("ua_"));
        assert_eq!(env["decision_id"], json!("dec-1"));
        assert_eq!(env["policy_version"], json!("pv-7"));
        assert_eq!(env["credential_type"], json!(3));
        assert_eq!(env["credential_id"], json!("key_abc123"));
        assert_eq!(env["service_identity"], json!("svc:orders"));
        assert_eq!(env["redaction_profile"], json!(REDACTION_PROFILE_VERSION));
        assert_eq!(env["immutable_export_status"], json!("pending"));
        assert_eq!(env["outcome"], json!("deny"));
    }

    #[test]
    fn native_envelope_carries_full_compliance_field_set() {
        // Phase 10 telemetry coherence: a native event must carry the SAME field
        // set as the auth envelope (actor/operation/outcome/trace + CloudEvents).
        let env = ComplianceEnvelope {
            actor: "svc-storage".to_string(),
            operation: "register_upload".to_string(),
            outcome: "success".to_string(),
            target_resource: "object/abc".to_string(),
            trace_id: "0af7651916cd43dd8448eb211c80319c".to_string(),
            span_id: "b7ad6b7169203331".to_string(),
            credential_type: 5,
            credential_id: "binding-abc".to_string(),
            service_identity: "spiffe://udb.test/storage".to_string(),
            ..ComplianceEnvelope::default()
        };
        let envelope = build_native_compliance_envelope(
            "11111111-1111-4111-8111-111111111111",
            "udb.storage.upload.registered.v1",
            "object-1",
            "acme",
            "proj-1",
            &env,
            "corr-9",
            "none",
            1,
            &[],
            json!({ "object_key": "k1" }),
        );
        // Routing/CDC contract preserved.
        assert_eq!(
            envelope["event_type"],
            json!("udb.storage.upload.registered.v1")
        );
        assert_eq!(envelope["correlation_id"], json!("corr-9"));
        assert_eq!(envelope["document_id"], json!("object-1"));
        assert_eq!(envelope["tenant_id"], json!("acme"));
        assert_eq!(envelope["project_id"], json!("proj-1"));
        assert_eq!(envelope["redaction_mode"], json!("none"));
        // Compliance fields (same names as the auth envelope).
        assert_eq!(envelope["specversion"], json!("1.0"));
        assert_eq!(envelope["actor"], json!("svc-storage"));
        assert_eq!(envelope["actor_tenant"], json!("acme"));
        assert_eq!(envelope["operation"], json!("register_upload"));
        assert_eq!(envelope["outcome"], json!("success"));
        assert_eq!(envelope["subject"], json!("object/abc"));
        assert_eq!(
            envelope["trace_id"],
            json!("0af7651916cd43dd8448eb211c80319c")
        );
        assert_eq!(envelope["span_id"], json!("b7ad6b7169203331"));
        assert_eq!(envelope["credential_type"], json!(5));
        assert_eq!(envelope["credential_id"], json!("binding-abc"));
        assert_eq!(
            envelope["service_identity"],
            json!("spiffe://udb.test/storage")
        );
        assert_eq!(
            envelope["redaction_profile"],
            json!(REDACTION_PROFILE_VERSION)
        );
        assert_eq!(envelope["payload"]["object_key"], json!("k1"));
    }

    #[tokio::test]
    async fn native_envelope_inherits_verified_credential_lineage() {
        let principal = crate::runtime::otel::RequestPrincipal {
            subject: "service-account-a".to_string(),
            service_identity: "spiffe://udb.test/orders".to_string(),
            credential_type: 5,
            credential_id: "binding-a".to_string(),
            auth_method: "mtls".to_string(),
            decision_id: "decision-a".to_string(),
            policy_revision: "contract-a".to_string(),
        };
        crate::runtime::otel::scope_principal(principal, async {
            let envelope = build_native_compliance_envelope(
                "11111111-1111-4111-8111-111111111111",
                "udb.storage.upload.registered.v1",
                "object-1",
                "acme",
                "proj-1",
                &ComplianceEnvelope::default(),
                "corr-9",
                "none",
                1,
                &[],
                json!({}),
            );
            assert_eq!(envelope["credential_type"], json!(5));
            assert_eq!(envelope["credential_id"], json!("binding-a"));
            assert_eq!(
                envelope["service_identity"],
                json!("spiffe://udb.test/orders")
            );
        })
        .await;
    }

    #[test]
    fn native_validation_rejects_incomplete_security_event() {
        // A security-sensitive native topic missing actor/operation/correlation
        // is rejected (enterprise-mode fail-closed parity with the auth lane).
        let err = validate_native_compliance(
            "udb.storage.upload.registered.v1",
            "acme",
            "", // actor missing
            "", // operation missing
            "", // correlation missing
            true,
        )
        .unwrap_err();
        assert!(err.contains("actor"), "{err}");
        assert!(err.contains("operation"), "{err}");
        assert!(err.contains("correlation_id"), "{err}");

        // Complete → ok.
        assert!(
            validate_native_compliance(
                "udb.storage.upload.registered.v1",
                "acme",
                "svc",
                "register_upload",
                "corr-1",
                true,
            )
            .is_ok()
        );
        // Non-security topic → skipped.
        assert!(
            validate_native_compliance("billing.invoice.created", "", "", "", "", false).is_ok()
        );
    }

    #[test]
    fn banned_field_names_cover_known_credential_tokens() {
        // Regression: the banned-key set must catch every credential family the
        // redactor is responsible for (Phase L3 task3).
        for key in [
            "password",
            "totp_secret",
            "session_token",
            "api_key",
            "recovery_code",
            "refresh_token",
            "access_token",
        ] {
            let mut v = json!({ key: "leak", "ok": "keep" });
            redact_auth_payload(&mut v);
            assert_eq!(v[key], json!("[REDACTED]"), "{key} not redacted");
            assert_eq!(v["ok"], json!("keep"));
        }
    }
}
