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
    // apikey
    pub const API_KEY_CREATED: &str = "udb.apikey.created.v1";
    pub const API_KEY_REVOKED: &str = "udb.apikey.revoked.v1";
    pub const API_KEY_UPDATED: &str = "udb.apikey.updated.v1";

    /// Wildcard patterns covering every native auth topic — for
    /// `UDB_CDC_VALID_TOPICS` allowlists and Kafka consumer subscriptions.
    pub const AUTH_TOPIC_PATTERNS: &[&str] = &["udb.authn.*", "udb.authz.*", "udb.apikey.*"];
}

/// One domain event to publish. `topic` routes it on Kafka; `document_id` is the
/// aggregate id used as the outbox/Kafka partition key (per-aggregate ordering);
/// `body` carries the proto event's domain fields.
pub(crate) struct AuthEvent {
    pub topic: &'static str,
    pub document_id: String,
    pub tenant_id: String,
    pub correlation_id: String,
    pub body: serde_json::Value,
}

impl AuthEvent {
    pub(crate) fn new(
        topic: &'static str,
        document_id: impl Into<String>,
        tenant_id: impl Into<String>,
        body: serde_json::Value,
    ) -> Self {
        Self {
            topic,
            document_id: document_id.into(),
            tenant_id: tenant_id.into(),
            correlation_id: String::new(),
            body,
        }
    }

    pub(crate) fn with_correlation(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = correlation_id.into();
        self
    }
}

/// Publishes native auth domain events. Implementations either write to the
/// shared outbox (production) or discard (when no Postgres pool is wired).
#[async_trait]
pub(crate) trait AuthEventSink: Send + Sync {
    async fn emit(&self, event: AuthEvent) -> Result<(), String>;
}

/// No-op sink for services constructed without a durable backend (fail-closed
/// `new()` constructors, snapshot-only authz). Native serving always wires the
/// outbox sink in `build_auth_services`.
pub(crate) struct NoopAuthEventSink;

#[async_trait]
impl AuthEventSink for NoopAuthEventSink {
    async fn emit(&self, _event: AuthEvent) -> Result<(), String> {
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
}

impl OutboxAuthEventSink {
    /// `outbox_relation` is the schema-qualified table name, e.g.
    /// `"udb_system"."outbox_events"` (from `CdcConfig::outbox_relation`).
    pub(crate) fn new(pool: sqlx::PgPool, outbox_relation: impl Into<String>) -> Self {
        Self {
            pool,
            outbox_relation: outbox_relation.into(),
        }
    }

    /// Build the consumer-facing envelope. This MUST match the CDC engine's
    /// `EventEnvelope` (`src/runtime/cdc/mod.rs`) so `process_outbox_event`
    /// deserializes it and forwards to Kafka — same shape the broker's
    /// `prepare_outbox_envelope` produces. Required fields: event_id, event_type,
    /// correlation_id. `timestamp` is the envelope's event time; the domain
    /// event's own fields (including tenant_id) live under `payload`.
    fn envelope(event_id: &Uuid, event: &AuthEvent) -> serde_json::Value {
        json!({
            "event_id": event_id.to_string(),
            "event_type": event.topic,
            "timestamp": Utc::now().to_rfc3339(),
            "correlation_id": event.correlation_id,
            "document_id": event.document_id,
            "payload": event.body,
        })
    }
}

#[async_trait]
impl AuthEventSink for OutboxAuthEventSink {
    async fn emit(&self, event: AuthEvent) -> Result<(), String> {
        let event_id = Uuid::new_v4();
        let envelope = Self::envelope(&event_id, &event);
        // Same column tuple the CDC tailer reads and the broker's own
        // enqueue_outbox_event writes.
        let sql = format!(
            "INSERT INTO {rel} (event_id, topic, partition_key, payload, created_at) \
             VALUES ($1::UUID, $2, $3, $4::JSONB, NOW())",
            rel = self.outbox_relation
        );
        sqlx::query(&sql)
            .bind(event_id)
            .bind(event.topic)
            .bind(&event.document_id)
            .bind(envelope.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| format!("auth outbox enqueue failed for topic {}: {e}", event.topic))?;
        Ok(())
    }
}
