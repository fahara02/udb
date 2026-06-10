//! IdP-plane Kafka topic constants and a thin emit helper.
//!
//! Defined locally (not in the shared `auth_service::events`) so the IdP work
//! does not contend with the events/audit agent that owns `events.rs`. The
//! topics follow the same `domain.entity.verb.version` taxonomy (PERIODS ONLY).
//! Emission reuses the shared [`AuthEventSink`] (outbox → CDC → Kafka).
//!
//! Phase L consolidation decision: these `udb.idp.*` topic constants stay LOCAL
//! (the lower-risk option). The shared CDC allowlist in
//! `auth_service/events.rs::topics::AUTH_TOPIC_PATTERNS` now includes
//! `udb.idp.*`, so IdP outbox rows are relayed to Kafka without touching any IdP
//! emit call site, and the shared `SECURITY_SENSITIVE_PREFIXES` covers them for
//! enterprise-mode envelope validation.

#![allow(dead_code)]

use serde_json::Value;

use super::super::events::{AuthEvent, AuthEventSink};

// ── IdP topic taxonomy: domain.entity.verb.version ──────────────────────────
pub const PROVIDER_CREATED: &str = "udb.idp.provider.created.v1";
pub const PROVIDER_UPDATED: &str = "udb.idp.provider.updated.v1";
pub const PROVIDER_DISABLED: &str = "udb.idp.provider.disabled.v1";
pub const JWKS_REFRESHED: &str = "udb.idp.provider.jwks.refreshed.v1";
/// Tier-7 #32: discovery/JWKS force-refresh FAILURE (a degraded federation edge).
/// Previously only a metric was recorded; the event makes the failure auditable.
pub const PROVIDER_REFRESH_FAILED: &str = "udb.idp.provider.refresh.failed.v1";
/// Tier-7 #32: SAML metadata import/update (entity_id / SSO URL / signing certs).
pub const SAML_METADATA_UPDATED: &str = "udb.idp.saml.metadata.updated.v1";
pub const IDENTITY_LINKED: &str = "udb.idp.identity.linked.v1";
pub const IDENTITY_UNLINKED: &str = "udb.idp.identity.unlinked.v1";
pub const IDENTITY_PROVISIONED: &str = "udb.idp.identity.provisioned.v1";
pub const SAML_ASSERTION_CONSUMED: &str = "udb.idp.saml.assertion.consumed.v1";
pub const SAML_REPLAY_REJECTED: &str = "udb.idp.saml.replay.rejected.v1";
pub const SCIM_USER_PROVISIONED: &str = "udb.idp.scim.user.provisioned.v1";
/// Tier-7 #32: SCIM user UPDATE (Replace/Patch) on the active-user path. The
/// create + deactivate paths already emit; this covers the in-place update.
pub const SCIM_USER_UPDATED: &str = "udb.idp.scim.user.updated.v1";
pub const SCIM_USER_DEACTIVATED: &str = "udb.idp.scim.user.deactivated.v1";
pub const SCIM_GROUP_CHANGED: &str = "udb.idp.scim.group.changed.v1";

/// Wildcard pattern covering every IdP topic, for `UDB_CDC_VALID_TOPICS`
/// allowlists and Kafka consumer subscriptions.
pub const IDP_TOPIC_PATTERNS: &[&str] = &["udb.idp.*"];

/// Best-effort emit: never fails the RPC. The payload is scrubbed of
/// credential-named keys by the underlying outbox sink (defense in depth).
pub async fn emit(
    sink: &dyn AuthEventSink,
    topic: &'static str,
    document_id: impl Into<String>,
    tenant_id: impl Into<String>,
    body: Value,
) {
    let event = AuthEvent::new(topic, document_id, tenant_id, body);
    if let Err(err) = sink.emit(event).await {
        tracing::warn!(topic, error = %err, "failed to publish idp event");
    }
}
