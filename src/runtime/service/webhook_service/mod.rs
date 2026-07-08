//! Native `WebhookService` (master-plan 9.4) — events delivered to the outside
//! world.
//!
//! Mirrors `tenant_service`/`scheduler_service`: proto-driven, no in-memory store,
//! no hand-mapped schema. A tenant registers an external HTTPS endpoint with a
//! topic subscription; the leader-elected delivery worker
//! ([`run_webhook_delivery_once`]) consumes the tenant-BOUND CDC stream, signs each
//! event body with the per-endpoint secret (HMAC-SHA256 → `X-Udb-Signature`),
//! POSTs it with retries/backoff, dead-letters after `max_attempts`
//! (`udb.webhook.delivery.dead.v1`), and writes the durable delivery journal.
//!
//! Security core (fail closed everywhere):
//!   - **SSRF guard** ([`validate_webhook_target_url`] / [`ip_is_blocked`]): every
//!     external target is https-only and must NOT resolve to a
//!     private/loopback/link-local/CGNAT/unspecified range. Enforced at endpoint
//!     WRITE (Create/Update) AND again at DELIVERY time
//!     ([`resolve_and_validate_target`]) to defeat DNS rebinding.
//!   - **Tenant-bound subscription** ([`webhook_event_matches_endpoint_scope`]):
//!     an event is delivered to an endpoint only when its payload tenant matches
//!     the endpoint's verified `tenant_id`. The subscription is NEVER pattern-only
//!     — a tenant-less or cross-tenant event is dropped (mirrors the CDC stream
//!     fail-closed rule `engine_tail::payload_value_matches_stream_scope`).
//!   - **Claim-scoped CRUD**: the body `tenant_id` must match the verified claim
//!     (`validate_request_tenant`); the signing secret is OUTPUT_VIEW_STORAGE_ONLY
//!     and is returned exactly once at creation, never surfaced by reads.
//!
//! The HMAC is built from the already-vendored `sha2` via
//! [`crate::runtime::security::hmac_sha256`] (the same manual HMAC the Vault/TOTP
//! lanes use) — no new crypto crate is introduced.
//!
//! NOTE: `build_webhook_service` + the constructor/wiring helpers are reachable
//! from `serve()`. When `http-client` is enabled, `serve()` also spawns
//! [`run_webhook_delivery_worker_once`] under
//! `run_while_leader(singleton::WORKER_WEBHOOK_DELIVERY)`.
#![allow(dead_code)]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::webhook::entity::v1 as webhook_entity_pb;
use crate::proto::udb::core::webhook::services::v1 as webhook_pb;
use crate::proto::udb::core::webhook::services::v1::webhook_service_server::WebhookService;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::webhook::services::v1::webhook_service_server::WebhookServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token_for_total, native_offset_page_window, non_empty_json, parse_uuid,
    update_mask_allows, update_mask_path_set, validate_request_tenant,
};

const ENDPOINT_MSG: &str = "udb.core.webhook.entity.v1.WebhookEndpoint";
const DELIVERY_MSG: &str = "udb.core.webhook.entity.v1.WebhookDelivery";

// ── outbox topics ─────────────────────────────────────────────────────────────
const TOPIC_ENDPOINT_CREATED: &str = "udb.webhook.endpoint.created.v1";
const TOPIC_ENDPOINT_UPDATED: &str = "udb.webhook.endpoint.updated.v1";
const TOPIC_ENDPOINT_DELETED: &str = "udb.webhook.endpoint.deleted.v1";
/// Successful external delivery (one per delivered event/endpoint).
const TOPIC_DELIVERY_SUCCEEDED: &str = "udb.webhook.delivery.succeeded.v1";
/// Dead-letter: a delivery exhausted `max_attempts` and was abandoned.
const TOPIC_DELIVERY_DEAD: &str = "udb.webhook.delivery.dead.v1";

/// The signature header the broker stamps every delivery with.
pub(crate) const SIGNATURE_HEADER: &str = "x-udb-signature";

/// Default delivery attempt budget when an endpoint declares none.
const DEFAULT_MAX_ATTEMPTS: i32 = 5;
/// Hard ceiling so a misconfigured endpoint can't pin the worker forever.
const MAX_MAX_ATTEMPTS: i32 = 20;
/// Base backoff between delivery attempts; doubled per attempt up to the cap.
const DELIVERY_BACKOFF_BASE: Duration = Duration::from_secs(2);
const DELIVERY_BACKOFF_CAP: Duration = Duration::from_secs(300);
pub(crate) const WEBHOOK_DELIVERY_BATCH: i64 = 200;
const DEFAULT_WEBHOOK_DELIVERY_INTERVAL_SECS: u64 = 30;
const WEBHOOK_DELIVERY_INTERVAL_ENV: &str = "UDB_WEBHOOK_DELIVERY_INTERVAL_SECS";

// ── delivery statuses (stored as VARCHAR) ─────────────────────────────────────
const STATUS_DELIVERED: &str = "DELIVERED";
const STATUS_DEAD: &str = "DEAD";
const TOPIC_WEBHOOK_DELIVERY_CDC: &str = "udb.webhook.webhook_deliveries.cdc";

/// Postgres-backed `WebhookService` handler.
pub struct WebhookServiceImpl {
    pg_pool: Option<PgPool>,
    /// Transactional-outbox relation; mutations enqueue events here.
    outbox_relation: Option<String>,
    /// Per-tenant fair-admission manager (same one the data plane uses). `None`
    /// only in bare unit-test construction (admit becomes a no-op).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn webhook_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "webhook",
        operation,
        capability_required,
        message,
    )
}

fn webhook_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status(operation, policy_decision_id, message)
}

fn webhook_endpoint_not_found_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "webhook",
        operation,
        "webhook_endpoint_not_found",
        "webhook endpoint not found",
    )
}

fn webhook_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("webhook", operation, message)
}

impl WebhookServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Webhook state is durable-only: fail closed when no Postgres pool exists.
    fn require_pool(&self) -> Result<&PgPool, Status> {
        self.pg_pool.as_ref().ok_or_else(|| {
            webhook_capability_status(
                "postgres_store",
                "postgres_store",
                "webhook service requires a Postgres-backed store (no PG pool configured)",
            )
        })
    }
}

impl Default for WebhookServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

// ── SSRF guard (the security core) ────────────────────────────────────────────

/// Reject an IP that a webhook must never be allowed to reach. Covers the full
/// private/loopback/link-local/CGNAT/unspecified set in BOTH families, so neither
/// a raw-IP literal at write time nor a rebound DNS answer at delivery time can
/// point the broker at an internal address (cloud metadata, service mesh, etc.).
///
/// IPv4: `0.0.0.0/8`, `10.0.0.0/8`, `127.0.0.0/8`, `169.254.0.0/16`,
/// `172.16.0.0/12`, `192.168.0.0/16`, `100.64.0.0/10` (CGNAT), plus multicast and
/// reserved (`>= 224.0.0.0`). IPv6: `::` (unspecified), `::1` (loopback),
/// `fc00::/7` (ULA), `fe80::/10` (link-local), and any IPv4-mapped address that is
/// itself blocked.
pub(crate) fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            v4.is_loopback()            // 127.0.0.0/8
                || v4.is_private()      // 10/8, 172.16/12, 192.168/16
                || v4.is_link_local()   // 169.254.0.0/16
                || v4.is_unspecified()  // 0.0.0.0
                || v4.is_broadcast()    // 255.255.255.255
                || o[0] == 0            // 0.0.0.0/8 "this network"
                || (o[0] == 100 && (o[1] & 0xc0) == 0x40) // 100.64.0.0/10 CGNAT
                || o[0] >= 224 // 224/4 multicast + 240/4 reserved
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return ip_is_blocked(IpAddr::V4(mapped));
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()                 // ::1
                || v6.is_unspecified()       // ::
                || (seg0 & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (seg0 & 0xffc0) == 0xfe80 // fe80::/10 link-local
        }
    }
}

/// Dependency-free parse of a webhook target into `(host, port)`. The host is
/// returned with IPv6 brackets stripped. Enforces the `https://` scheme (the only
/// allowed one) and extracts the authority's host the way a URL parser does — host
/// is whatever follows the LAST `@` (userinfo is dropped), up to the path/query/
/// fragment, with `\` treated as a path separator (WHATWG special-scheme rule) so
/// a `https://evil.com\@127.0.0.1` style trick cannot smuggle an internal host
/// past the check. No `url`/`reqwest` crate is used, so this stays compiled (and
/// unit-tested) on every feature set.
fn parse_webhook_target(url: &str) -> Result<(String, u16), Status> {
    let raw = url.trim();
    // `get(..8)` is None when shorter than 8 bytes OR when byte 8 is not a UTF-8
    // char boundary, so a malformed multi-byte input is rejected, never panics.
    let scheme_ok = raw
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"));
    if !scheme_ok {
        return Err(webhook_url_violation(
            "must use https scheme",
            "webhook url must use https (cleartext http and non-http schemes are rejected)",
        ));
    }
    let rest = &raw[8..];
    let authority_end = rest.find(['/', '?', '#', '\\']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    // Host is everything after the LAST '@' (userinfo, if any, is discarded).
    let hostport = authority.rsplit('@').next().unwrap_or(authority);
    let (host, port) = if let Some(after_bracket) = hostport.strip_prefix('[') {
        // IPv6 literal: `[addr]` optionally followed by `:port`.
        let close = after_bracket.find(']').ok_or_else(|| {
            webhook_url_violation(
                "must contain a well-formed bracketed IPv6 host",
                "webhook url has a malformed IPv6 host",
            )
        })?;
        let addr = &after_bracket[..close];
        let port = after_bracket[close + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(443);
        (addr.to_string(), port)
    } else if let Some(idx) = hostport.find(':') {
        let port = hostport[idx + 1..].parse::<u16>().unwrap_or(443);
        (hostport[..idx].to_string(), port)
    } else {
        (hostport.to_string(), 443)
    };
    if host.trim().is_empty() || host.chars().any(char::is_whitespace) {
        return Err(webhook_url_violation(
            "must include a valid external host",
            "webhook url must include a valid host",
        ));
    }
    Ok((host, port))
}

/// Pure SSRF validator enforced at endpoint WRITE time (Create/Update): the URL
/// must be `https://`, carry a host, and — when the host is a raw IP literal — not
/// fall in a blocked range. A hostname literal cannot be resolved here (that is
/// the delivery-time job in [`resolve_and_validate_target`]), but obvious local
/// names (`localhost`) are rejected up front. Maps every failure to
/// `invalid_argument` so a caller learns exactly why the endpoint was refused.
pub(crate) fn validate_webhook_target_url(url: &str) -> Result<(), Status> {
    let (host, _port) = parse_webhook_target(url)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        if ip_is_blocked(ip) {
            return Err(webhook_url_violation(
                "must not target private, loopback, link-local, CGNAT, unspecified, multicast, or reserved IP ranges",
                format!(
                    "webhook url host {host} resolves to a private/loopback/link-local address (SSRF blocked)"
                ),
            ));
        }
    } else {
        let lower = host.to_ascii_lowercase();
        if lower == "localhost" || lower.ends_with(".localhost") {
            return Err(webhook_url_violation(
                "must not target localhost hostnames",
                "webhook url host localhost is not an allowed external target (SSRF blocked)",
            ));
        }
    }
    Ok(())
}

/// Delivery-time SSRF guard (defeats DNS rebinding): re-run the write-time check,
/// then — for a hostname target — resolve it and reject if ANY resolved address is
/// blocked. Fail closed: a resolution error is treated as a rejection rather than
/// proceeding to a possibly-internal address. Literal-IP targets were already
/// validated by [`validate_webhook_target_url`].
#[allow(dead_code)]
pub(crate) async fn resolve_and_validate_target(url: &str) -> Result<(), Status> {
    validate_webhook_target_url(url)?;
    let (host, port) = parse_webhook_target(url)?;
    if host.parse::<IpAddr>().is_ok() {
        // A literal IP was already range-checked by `validate_webhook_target_url`.
        return Ok(());
    }
    let mut resolved = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|err| {
            // Fail closed: if we cannot prove where the host points, do not deliver.
            webhook_host_unresolved_status(&host, err)
        })?;
    let mut saw_any = false;
    for addr in &mut resolved {
        saw_any = true;
        if ip_is_blocked(addr.ip()) {
            return Err(webhook_host_blocked_address_status(&host, addr.ip()));
        }
    }
    if !saw_any {
        return Err(webhook_host_no_addresses_status(&host));
    }
    Ok(())
}

fn webhook_host_unresolved_status(host: &str, err: impl std::fmt::Display) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_unresolved",
        format!("webhook host {host} did not resolve: {err}"),
    )
}

fn webhook_host_blocked_address_status(host: &str, ip: IpAddr) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_blocked_address",
        format!(
            "webhook host {host} resolved to a blocked address {ip} (SSRF/DNS-rebinding blocked)"
        ),
    )
}

fn webhook_host_no_addresses_status(host: &str) -> Status {
    webhook_policy_status(
        "webhook_delivery_ssrf",
        "webhook_host_no_addresses",
        format!("webhook host {host} resolved to no addresses"),
    )
}

// ── HMAC signing (reuses the vendored sha2-based HMAC; no new crypto crate) ────

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The `X-Udb-Signature` value for a body: `sha256=<hex(HMAC-SHA256(secret,body))>`.
/// Computed with [`crate::runtime::security::hmac_sha256`] (the manual sha2 HMAC
/// the Vault/TOTP lanes share) so no `hmac` crate is added.
pub(crate) fn sign_webhook_body(secret: &str, body: &[u8]) -> String {
    let mac = crate::runtime::security::hmac_sha256(secret.as_bytes(), body);
    format!("sha256={}", hex_lower(&mac))
}

/// Mint a fresh per-endpoint signing secret from two UUIDv4s (256 bits), matching
/// the no-extra-RNG-dependency convention used for TOTP secrets.
fn generate_signing_secret() -> String {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

// ── tenant-bound subscription matching (fail closed) ──────────────────────────

/// Glob match for a topic subscription pattern. `*` matches everything; a trailing
/// `*` (e.g. `udb.*`) is a prefix match; anything else is an exact match. An empty
/// pattern matches NOTHING (fail closed) so a misconfigured endpoint never
/// silently subscribes to the whole bus.
pub(crate) fn topic_matches_pattern(pattern: &str, topic: &str) -> bool {
    let pattern = pattern.trim();
    let topic = topic.trim();
    if pattern.is_empty() || topic.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return topic.starts_with(prefix);
    }
    pattern == topic
}

/// The fail-closed delivery gate: an event is delivered to an endpoint ONLY when
/// (1) the event payload's `tenant_id` is non-empty and equals the endpoint's
/// verified `endpoint_tenant`, and (2) the topic matches the endpoint's
/// subscription pattern. The subscription is BOUND to the endpoint tenant — it is
/// never pattern-only — so a tenant-less event or a cross-tenant event is dropped,
/// mirroring `engine_tail::payload_value_matches_stream_scope` (#8).
pub(crate) fn webhook_event_matches_endpoint_scope(
    endpoint_tenant: &str,
    endpoint_pattern: &str,
    topic: &str,
    payload: &serde_json::Value,
) -> bool {
    let endpoint_tenant = endpoint_tenant.trim();
    if endpoint_tenant.is_empty() {
        return false;
    }
    let event_tenant = payload
        .get("tenant_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    // Tenant BOUND to the endpoint: a tenant-less or foreign-tenant event is
    // never delivered (no pattern-only fan-out — the fail-open trap).
    if event_tenant.is_empty() || event_tenant != endpoint_tenant {
        return false;
    }
    topic_matches_pattern(endpoint_pattern, topic)
}

/// Exponential backoff for delivery attempt `attempt` (1-based), doubled per
/// attempt and capped. Pure and unit-tested.
pub(crate) fn delivery_backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(8);
    let scaled = DELIVERY_BACKOFF_BASE
        .checked_mul(1u32 << shift)
        .unwrap_or(DELIVERY_BACKOFF_CAP);
    scaled.min(DELIVERY_BACKOFF_CAP)
}

pub(crate) fn webhook_delivery_interval() -> Duration {
    Duration::from_secs(
        std::env::var(WEBHOOK_DELIVERY_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_WEBHOOK_DELIVERY_INTERVAL_SECS),
    )
}

fn clamp_max_attempts(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        requested.min(MAX_MAX_ATTEMPTS)
    }
}

fn webhook_required_field(
    field: &'static str,
    description: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(message, [(field, description)])
}

fn webhook_url_violation(description: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::invalid_argument_fields(
        message,
        [("url".to_string(), description.into())],
    )
}

// ── native models / projections ───────────────────────────────────────────────

fn endpoint_model() -> NativeModel {
    native_model(
        ENDPOINT_MSG,
        &[
            "endpoint_id",
            "tenant_id",
            "url",
            "topic_pattern",
            "signing_secret",
            "active",
            "description",
            "max_attempts",
            "metadata_json",
            "deleted_at",
            "deleted_by",
        ],
    )
}

fn delivery_model() -> NativeModel {
    native_model(
        DELIVERY_MSG,
        &[
            "delivery_id",
            "tenant_id",
            "endpoint_id",
            "event_id",
            "topic",
            "status",
            "attempt_count",
            "response_status",
            "signature",
            "last_error",
            "payload_json",
            "delivered_at",
        ],
    )
}

/// Projection for endpoint reads. The signing secret is NEVER selected — it is
/// OUTPUT_VIEW_STORAGE_ONLY and must never leave the store after creation.
fn endpoint_select_projection(m: &NativeModel) -> String {
    [
        m.text("endpoint_id"),
        m.text("tenant_id"),
        m.text("url"),
        m.text_or_empty("topic_pattern"),
        m.text_or_empty("description"),
        m.select("active"),
        m.select("max_attempts"),
        m.text_or_empty("metadata_json"),
    ]
    .join(", ")
}

fn endpoint_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<webhook_entity_pb::WebhookEndpoint, Status> {
    let map = |e: sqlx::Error| {
        webhook_internal_status(
            "decode_webhook_endpoint",
            format!("decode webhook endpoint failed: {e}"),
        )
    };
    Ok(webhook_entity_pb::WebhookEndpoint {
        endpoint_id: row.try_get("endpoint_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        url: row.try_get("url").map_err(map)?,
        topic_pattern: row.try_get("topic_pattern").map_err(map)?,
        description: row.try_get("description").map_err(map)?,
        active: row.try_get("active").map_err(map)?,
        max_attempts: row.try_get("max_attempts").map_err(map)?,
        metadata_json: row.try_get("metadata_json").map_err(map)?,
        // signing_secret intentionally left empty: STORAGE_ONLY, never surfaced.
        signing_secret: String::new(),
        ..Default::default()
    })
}

fn delivery_select_projection(m: &NativeModel) -> String {
    [
        m.text("delivery_id"),
        m.text("tenant_id"),
        m.text("endpoint_id"),
        m.text_or_empty("event_id"),
        m.text_or_empty("topic"),
        m.text_or_empty("status"),
        m.select("attempt_count"),
        m.select("response_status"),
        m.text_or_empty("signature"),
        m.text_or_empty("last_error"),
        m.text_or_empty("payload_json"),
        format!(
            "EXTRACT(EPOCH FROM {})::BIGINT AS delivered_at_epoch",
            m.q("delivered_at")
        ),
    ]
    .join(", ")
}

fn epoch_to_ts(epoch: Option<i64>) -> Option<prost_types::Timestamp> {
    epoch.map(|seconds| prost_types::Timestamp { seconds, nanos: 0 })
}

fn delivery_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<webhook_entity_pb::WebhookDelivery, Status> {
    let map = |e: sqlx::Error| {
        webhook_internal_status(
            "decode_webhook_delivery",
            format!("decode webhook delivery failed: {e}"),
        )
    };
    Ok(webhook_entity_pb::WebhookDelivery {
        delivery_id: row.try_get("delivery_id").map_err(map)?,
        tenant_id: row.try_get("tenant_id").map_err(map)?,
        endpoint_id: row.try_get("endpoint_id").map_err(map)?,
        event_id: row.try_get("event_id").map_err(map)?,
        topic: row.try_get("topic").map_err(map)?,
        status: row.try_get("status").map_err(map)?,
        attempt_count: row.try_get("attempt_count").map_err(map)?,
        response_status: row.try_get("response_status").map_err(map)?,
        signature: row.try_get("signature").map_err(map)?,
        last_error: row.try_get("last_error").map_err(map)?,
        payload_json: row.try_get("payload_json").map_err(map)?,
        delivered_at: epoch_to_ts(
            row.try_get::<Option<i64>, _>("delivered_at_epoch")
                .map_err(map)?,
        ),
        ..Default::default()
    })
}

impl WebhookServiceImpl {
    /// Best-effort versioned dot-topic outbox event (mirrors `lock_service`).
    async fn emit_event(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
        payload: serde_json::Value,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            tenant_id,
            "",
            payload,
            NativeEventContext {
                target_resource: partition_key.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

#[tonic::async_trait]
impl WebhookService for WebhookServiceImpl {
    async fn create_endpoint(
        &self,
        request: Request<webhook_pb::CreateEndpointRequest>,
    ) -> Result<Response<webhook_pb::CreateEndpointResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes, the body value IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        if req.url.trim().is_empty() {
            return Err(webhook_required_field(
                "url",
                "must be a non-empty HTTPS webhook URL",
                "url is required",
            ));
        }
        // SSRF guard at WRITE time: https-only, no private/loopback/CGNAT host.
        validate_webhook_target_url(&req.url)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let pool = self.require_pool()?;
        let m = endpoint_model();
        let rel = m.relation.clone();
        let endpoint_id = Uuid::new_v4().to_string();
        let secret = if req.signing_secret.trim().is_empty() {
            generate_signing_secret()
        } else {
            req.signing_secret.trim().to_string()
        };
        let topic_pattern = {
            let p = req.topic_pattern.trim();
            if p.is_empty() {
                "*".to_string()
            } else {
                p.to_string()
            }
        };
        let max_attempts = clamp_max_attempts(req.max_attempts);
        let metadata_json = non_empty_json(&req.metadata_json);

        sqlx::query(&format!(
            "INSERT INTO {rel} \
             ({endpoint_id}, {tenant_id}, {url}, {topic_pattern}, {signing_secret}, {active}, {description}, {max_attempts}, {metadata_json}) \
             VALUES ($1::UUID, $2::UUID, $3, $4, $5, true, NULLIF($6, ''), $7, $8::JSONB)",
            endpoint_id = m.q("endpoint_id"),
            tenant_id = m.q("tenant_id"),
            url = m.q("url"),
            topic_pattern = m.q("topic_pattern"),
            signing_secret = m.q("signing_secret"),
            active = m.q("active"),
            description = m.q("description"),
            max_attempts = m.q("max_attempts"),
            metadata_json = m.q("metadata_json"),
        ))
        .bind(&endpoint_id)
        .bind(&tenant_id)
        .bind(req.url.trim())
        .bind(&topic_pattern)
        .bind(&secret)
        .bind(req.description.trim())
        .bind(max_attempts)
        .bind(&metadata_json)
        .execute(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "create_webhook_endpoint",
                format!("create webhook endpoint failed: {err}"),
            )
        })?;

        self.emit_event(
            TOPIC_ENDPOINT_CREATED,
            &endpoint_id,
            &tenant_id,
            serde_json::json!({
                "tenant_id": tenant_id,
                "endpoint_id": endpoint_id,
                "topic_pattern": topic_pattern,
            }),
        )
        .await;

        Ok(Response::new(webhook_pb::CreateEndpointResponse {
            endpoint_id,
            // Returned ONCE here; never surfaced by reads again.
            signing_secret: secret,
            message: "webhook endpoint created".to_string(),
            error: None,
        }))
    }

    async fn get_endpoint(
        &self,
        request: Request<webhook_pb::GetEndpointRequest>,
    ) -> Result<Response<webhook_pb::GetEndpointResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
        let pool = self.require_pool()?;
        let m = endpoint_model();
        let rel = m.relation.clone();
        let projection = endpoint_select_projection(&m);
        let row = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} \
             WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            endpoint_id = m.q("endpoint_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&endpoint_id)
        .bind(&tenant_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "get_webhook_endpoint",
                format!("get webhook endpoint failed: {err}"),
            )
        })?;
        let endpoint = row
            .as_ref()
            .map(endpoint_from_row)
            .transpose()?
            .ok_or_else(|| webhook_endpoint_not_found_status("get_endpoint"))?;
        Ok(Response::new(webhook_pb::GetEndpointResponse {
            endpoint: Some(endpoint),
            error: None,
        }))
    }

    async fn list_endpoints(
        &self,
        request: Request<webhook_pb::ListEndpointsRequest>,
    ) -> Result<Response<webhook_pb::ListEndpointsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let pool = self.require_pool()?;
        let m = endpoint_model();
        let rel = m.relation.clone();
        let projection = endpoint_select_projection(&m);
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let active_clause = if req.active_only {
            format!("AND {active} = true", active = m.q("active"))
        } else {
            String::new()
        };
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL {active_clause}",
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(&tenant_id)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                webhook_internal_status(
                    "list_webhook_endpoints_count",
                    format!("count webhook endpoints failed: {err}"),
                )
            })?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {endpoint_id} LIMIT $2 OFFSET $3",
            endpoint_id = m.q("endpoint_id"),
        ))
        .bind(&tenant_id)
        .bind(page_window.limit_i64())
        .bind(page_window.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "list_webhook_endpoints",
                format!("list webhook endpoints failed: {err}"),
            )
        })?;
        let mut endpoints = Vec::with_capacity(rows.len());
        for row in &rows {
            endpoints.push(endpoint_from_row(row)?);
        }
        Ok(Response::new(webhook_pb::ListEndpointsResponse {
            endpoints,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }

    async fn update_endpoint(
        &self,
        request: Request<webhook_pb::UpdateEndpointRequest>,
    ) -> Result<Response<webhook_pb::UpdateEndpointResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        // A changed URL is SSRF-revalidated before it can be stored.
        if !req.url.trim().is_empty() {
            validate_webhook_target_url(&req.url)?;
        }
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
        let pool = self.require_pool()?;
        let m = endpoint_model();
        let rel = m.relation.clone();
        let update_mask = update_mask_path_set(
            req.update_mask.as_ref(),
            &[
                "url",
                "topic_pattern",
                "description",
                "active",
                "max_attempts",
            ],
        )?;
        let update_url = update_mask_allows(&update_mask, "url", !req.url.trim().is_empty());
        let update_topic_pattern = update_mask_allows(
            &update_mask,
            "topic_pattern",
            !req.topic_pattern.trim().is_empty(),
        );
        let update_description = update_mask_allows(
            &update_mask,
            "description",
            !req.description.trim().is_empty(),
        );
        let update_active = update_mask_allows(&update_mask, "active", true);
        let update_max_attempts =
            update_mask_allows(&update_mask, "max_attempts", req.max_attempts > 0);
        let max_attempts = if update_max_attempts && req.max_attempts > 0 {
            clamp_max_attempts(req.max_attempts)
        } else {
            0
        };
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET \
               {url} = CASE WHEN $2 THEN $3 ELSE {url} END, \
               {topic_pattern} = CASE WHEN $4 THEN $5 ELSE {topic_pattern} END, \
               {description} = CASE WHEN $6 THEN $7 ELSE {description} END, \
               {active} = CASE WHEN $8 THEN $9 ELSE {active} END, \
               {max_attempts} = CASE WHEN $10 THEN $11 ELSE {max_attempts} END \
             WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $12::UUID AND {deleted} IS NULL",
            url = m.q("url"),
            topic_pattern = m.q("topic_pattern"),
            description = m.q("description"),
            active = m.q("active"),
            max_attempts = m.q("max_attempts"),
            endpoint_id = m.q("endpoint_id"),
            tenant_id = m.q("tenant_id"),
            deleted = m.q("deleted_at"),
        ))
        .bind(&endpoint_id)
        .bind(update_url)
        .bind(req.url.trim())
        .bind(update_topic_pattern)
        .bind(req.topic_pattern.trim())
        .bind(update_description)
        .bind(req.description.trim())
        .bind(update_active)
        .bind(req.active)
        .bind(update_max_attempts)
        .bind(max_attempts)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "update_webhook_endpoint",
                format!("update webhook endpoint failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(webhook_endpoint_not_found_status("update_endpoint"));
        }
        self.emit_event(
            TOPIC_ENDPOINT_UPDATED,
            &endpoint_id,
            &tenant_id,
            serde_json::json!({ "tenant_id": tenant_id, "endpoint_id": endpoint_id }),
        )
        .await;
        Ok(Response::new(webhook_pb::UpdateEndpointResponse {
            message: "webhook endpoint updated".to_string(),
            error: None,
        }))
    }

    async fn delete_endpoint(
        &self,
        request: Request<webhook_pb::DeleteEndpointRequest>,
    ) -> Result<Response<webhook_pb::DeleteEndpointResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Admin,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        let endpoint_id = parse_uuid("endpoint_id", &req.endpoint_id)?.to_string();
        let pool = self.require_pool()?;
        let m = endpoint_model();
        let rel = m.relation.clone();
        // Soft delete: stop delivering to the endpoint, keep the audit row.
        let result = sqlx::query(&format!(
            "UPDATE {rel} SET {deleted} = now(), {active} = false \
             WHERE {endpoint_id} = $1::UUID AND {tenant_id} = $2::UUID AND {deleted} IS NULL",
            deleted = m.q("deleted_at"),
            active = m.q("active"),
            endpoint_id = m.q("endpoint_id"),
            tenant_id = m.q("tenant_id"),
        ))
        .bind(&endpoint_id)
        .bind(&tenant_id)
        .execute(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "delete_webhook_endpoint",
                format!("delete webhook endpoint failed: {err}"),
            )
        })?;
        if result.rows_affected() == 0 {
            return Err(webhook_endpoint_not_found_status("delete_endpoint"));
        }
        self.emit_event(
            TOPIC_ENDPOINT_DELETED,
            &endpoint_id,
            &tenant_id,
            serde_json::json!({ "tenant_id": tenant_id, "endpoint_id": endpoint_id }),
        )
        .await;
        Ok(Response::new(webhook_pb::DeleteEndpointResponse {
            message: "webhook endpoint deleted".to_string(),
            error: None,
        }))
    }

    async fn list_deliveries(
        &self,
        request: Request<webhook_pb::ListDeliveriesRequest>,
    ) -> Result<Response<webhook_pb::ListDeliveriesResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "webhook",
            OperationChannel::Read,
            &req.tenant_id,
            None,
        )
        .await?;
        let tenant_id = parse_uuid("tenant_id", &req.tenant_id)?.to_string();
        // Optional endpoint filter; an empty value lists all endpoints' deliveries.
        let endpoint_filter = if req.endpoint_id.trim().is_empty() {
            String::new()
        } else {
            parse_uuid("endpoint_id", &req.endpoint_id)?.to_string()
        };
        let status_filter = req.status.trim().to_ascii_uppercase();
        let pool = self.require_pool()?;
        let m = delivery_model();
        let rel = m.relation.clone();
        let projection = delivery_select_projection(&m);
        let page_window = native_offset_page_window(req.page, req.page_size, &req.page_token, 50);
        let where_clause = format!(
            "WHERE {tenant_id} = $1::UUID \
             AND ($2 = '' OR {endpoint_id} = $2::UUID) \
             AND ($3 = '' OR {status} = $3)",
            tenant_id = m.q("tenant_id"),
            endpoint_id = m.q("endpoint_id"),
            status = m.q("status"),
        );
        let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {rel} {where_clause}"))
            .bind(&tenant_id)
            .bind(&endpoint_filter)
            .bind(&status_filter)
            .fetch_one(pool)
            .await
            .map_err(|err| {
                webhook_internal_status(
                    "list_webhook_deliveries_count",
                    format!("count webhook deliveries failed: {err}"),
                )
            })?;
        let rows = sqlx::query(&format!(
            "SELECT {projection} FROM {rel} {where_clause} \
             ORDER BY {delivered_at} DESC NULLS LAST, {delivery_id} LIMIT $4 OFFSET $5",
            delivered_at = m.q("delivered_at"),
            delivery_id = m.q("delivery_id"),
        ))
        .bind(&tenant_id)
        .bind(&endpoint_filter)
        .bind(&status_filter)
        .bind(page_window.limit_i64())
        .bind(page_window.offset_i64())
        .fetch_all(pool)
        .await
        .map_err(|err| {
            webhook_internal_status(
                "list_webhook_deliveries",
                format!("list webhook deliveries failed: {err}"),
            )
        })?;
        let mut deliveries = Vec::with_capacity(rows.len());
        for row in &rows {
            deliveries.push(delivery_from_row(row)?);
        }
        Ok(Response::new(webhook_pb::ListDeliveriesResponse {
            deliveries,
            total_count: total as i32,
            error: None,
            next_page_token: native_next_page_token_for_total(
                page_window.offset,
                page_window.limit,
                total,
            ),
        }))
    }
}

// ── leader-elected delivery worker (design; leader spawn = follow-up) ──────────

/// One active webhook endpoint the delivery worker may deliver to. Tenant is the
/// VERIFIED endpoint tenant the subscription is BOUND to (never a request body).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct WebhookDeliveryTarget {
    pub endpoint_id: String,
    pub tenant_id: String,
    pub url: String,
    pub topic_pattern: String,
    pub signing_secret: String,
    pub max_attempts: i32,
}

/// One CDC/outbox event the worker considers for delivery. The tenant is read
/// ONLY from the payload and matched against each endpoint's bound tenant.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct WebhookEventCandidate {
    pub topic: String,
    pub event_id: String,
    pub payload: serde_json::Value,
}

#[cfg(feature = "http-client")]
struct WebhookDeliveryJob {
    target: WebhookDeliveryTarget,
    event: WebhookEventCandidate,
}

#[cfg(feature = "http-client")]
async fn terminal_delivery_exists(
    pool: &PgPool,
    endpoint_id: &str,
    event_id: &str,
) -> Result<bool, String> {
    let m = delivery_model();
    let rel = m.relation.clone();
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {rel} \
         WHERE {endpoint_id} = $1::UUID \
           AND {event_id} = $2 \
           AND {status} IN ($3, $4)",
        endpoint_id = m.q("endpoint_id"),
        event_id = m.q("event_id"),
        status = m.q("status"),
    ))
    .bind(endpoint_id)
    .bind(event_id)
    .bind(STATUS_DELIVERED)
    .bind(STATUS_DEAD)
    .fetch_one(pool)
    .await
    .map_err(|err| format!("check webhook terminal delivery failed: {err}"))?;
    Ok(count > 0)
}

#[cfg(feature = "http-client")]
async fn load_webhook_delivery_jobs(
    pool: &PgPool,
    journal_relation: &str,
    batch: i64,
) -> Result<Vec<WebhookDeliveryJob>, String> {
    let endpoint = endpoint_model();
    let delivery = delivery_model();
    let endpoint_rel = endpoint.relation.clone();
    let delivery_rel = delivery.relation.clone();
    let limit = batch.max(1);
    let rows = sqlx::query(&format!(
        "SELECT \
            e.{endpoint_id}::TEXT AS endpoint_id, \
            e.{tenant_id}::TEXT AS tenant_id, \
            e.{url} AS url, \
            e.{topic_pattern} AS topic_pattern, \
            e.{signing_secret} AS signing_secret, \
            e.{max_attempts} AS max_attempts, \
            j.event_id::TEXT AS event_id, \
            j.topic AS topic, \
            j.payload::TEXT AS payload_json \
         FROM {journal_relation} j \
         JOIN {endpoint_rel} e \
           ON e.{tenant_id}::TEXT = COALESCE(j.payload->>'tenant_id', '') \
          AND e.{active} = true \
          AND e.{deleted_at} IS NULL \
          AND ( \
              e.{topic_pattern} = '*' \
              OR (right(e.{topic_pattern}, 1) = '*' \
                  AND left(j.topic, GREATEST(length(e.{topic_pattern}) - 1, 0)) = \
                      left(e.{topic_pattern}, GREATEST(length(e.{topic_pattern}) - 1, 0))) \
              OR e.{topic_pattern} = j.topic \
          ) \
         WHERE j.delivery_state IN ('published', 'acked') \
           AND j.topic <> $1 \
           AND j.topic <> $2 \
           AND j.topic <> $3 \
           AND NOT EXISTS ( \
               SELECT 1 FROM {delivery_rel} d \
               WHERE d.{delivery_endpoint_id} = e.{endpoint_id} \
                 AND d.{delivery_event_id} = j.event_id::TEXT \
                 AND d.{delivery_status} IN ($4, $5) \
           ) \
         ORDER BY j.published_at ASC, e.{endpoint_id} ASC \
         LIMIT $6",
        endpoint_id = endpoint.q("endpoint_id"),
        tenant_id = endpoint.q("tenant_id"),
        url = endpoint.q("url"),
        topic_pattern = endpoint.q("topic_pattern"),
        signing_secret = endpoint.q("signing_secret"),
        max_attempts = endpoint.q("max_attempts"),
        active = endpoint.q("active"),
        deleted_at = endpoint.q("deleted_at"),
        delivery_endpoint_id = delivery.q("endpoint_id"),
        delivery_event_id = delivery.q("event_id"),
        delivery_status = delivery.q("status"),
    ))
    .bind(TOPIC_DELIVERY_SUCCEEDED)
    .bind(TOPIC_DELIVERY_DEAD)
    .bind(TOPIC_WEBHOOK_DELIVERY_CDC)
    .bind(STATUS_DELIVERED)
    .bind(STATUS_DEAD)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load webhook delivery jobs failed: {err}"))?;

    let mut jobs = Vec::with_capacity(rows.len());
    for row in rows {
        let payload_json: String = row
            .try_get("payload_json")
            .map_err(|err| format!("decode webhook event payload failed: {err}"))?;
        let payload = serde_json::from_str(&payload_json)
            .map_err(|err| format!("decode webhook event payload JSON failed: {err}"))?;
        jobs.push(WebhookDeliveryJob {
            target: WebhookDeliveryTarget {
                endpoint_id: row
                    .try_get("endpoint_id")
                    .map_err(|err| format!("decode webhook endpoint id failed: {err}"))?,
                tenant_id: row
                    .try_get("tenant_id")
                    .map_err(|err| format!("decode webhook tenant id failed: {err}"))?,
                url: row
                    .try_get("url")
                    .map_err(|err| format!("decode webhook url failed: {err}"))?,
                topic_pattern: row
                    .try_get("topic_pattern")
                    .map_err(|err| format!("decode webhook topic pattern failed: {err}"))?,
                signing_secret: row
                    .try_get("signing_secret")
                    .map_err(|err| format!("decode webhook signing secret failed: {err}"))?,
                max_attempts: row
                    .try_get("max_attempts")
                    .map_err(|err| format!("decode webhook max attempts failed: {err}"))?,
            },
            event: WebhookEventCandidate {
                topic: row
                    .try_get("topic")
                    .map_err(|err| format!("decode webhook event topic failed: {err}"))?,
                event_id: row
                    .try_get("event_id")
                    .map_err(|err| format!("decode webhook event id failed: {err}"))?,
                payload,
            },
        });
    }
    Ok(jobs)
}

/// Insert one terminal delivery-journal row (best-effort; logged on failure).
#[cfg(feature = "http-client")]
#[allow(clippy::too_many_arguments)]
async fn insert_delivery_journal(
    pool: &PgPool,
    tenant_id: &str,
    endpoint_id: &str,
    event_id: &str,
    topic: &str,
    status: &str,
    attempt_count: i32,
    response_status: i32,
    signature: &str,
    last_error: &str,
    payload: &serde_json::Value,
) {
    let m = delivery_model();
    let rel = m.relation.clone();
    let delivery_id = Uuid::new_v4().to_string();
    let payload_text = serde_json::to_string(payload).unwrap_or_else(|_| "{}".to_string());
    let result = sqlx::query(&format!(
        "INSERT INTO {rel} \
         ({delivery_id}, {tenant_id}, {endpoint_id}, {event_id}, {topic}, {status}, {attempt_count}, {response_status}, {signature}, {last_error}, {payload_json}, {delivered_at}) \
         VALUES ($1::UUID, $2::UUID, $3::UUID, $4, $5, $6, $7, $8, NULLIF($9, ''), NULLIF($10, ''), $11::JSONB, now())",
        delivery_id = m.q("delivery_id"),
        tenant_id = m.q("tenant_id"),
        endpoint_id = m.q("endpoint_id"),
        event_id = m.q("event_id"),
        topic = m.q("topic"),
        status = m.q("status"),
        attempt_count = m.q("attempt_count"),
        response_status = m.q("response_status"),
        signature = m.q("signature"),
        last_error = m.q("last_error"),
        payload_json = m.q("payload_json"),
        delivered_at = m.q("delivered_at"),
    ))
    .bind(&delivery_id)
    .bind(tenant_id)
    .bind(endpoint_id)
    .bind(event_id)
    .bind(topic)
    .bind(status)
    .bind(attempt_count)
    .bind(response_status)
    .bind(signature)
    .bind(last_error)
    .bind(&payload_text)
    .execute(pool)
    .await;
    if let Err(err) = result {
        tracing::warn!(endpoint_id, event_id, error = %err, "webhook delivery journal insert failed");
    }
}

/// Run ONE delivery pass over supplied candidates. For each
/// candidate event and each endpoint whose BOUND tenant + subscription matches
/// ([`webhook_event_matches_endpoint_scope`]): re-validate the target against the
/// SSRF guard at delivery time (DNS rebinding), sign the body
/// ([`sign_webhook_body`]), POST it with retries/backoff, journal the terminal
/// outcome, and on attempt exhaustion emit the dead-letter event
/// (`udb.webhook.delivery.dead.v1`). Returns the count of events delivered
/// successfully. Best-effort per (event, endpoint): one failing target never
/// aborts the pass.
///
#[cfg(feature = "http-client")]
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
pub(crate) async fn run_webhook_delivery_once(
    http: &reqwest::Client,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    endpoints: &[WebhookDeliveryTarget],
    events: &[WebhookEventCandidate],
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<u64, String> {
    let mut delivered: u64 = 0;
    for event in events {
        let body = serde_json::to_vec(&event.payload).unwrap_or_default();
        for endpoint in endpoints {
            // Fail-closed tenant-bound scope: a tenant-less / cross-tenant event,
            // or a non-subscribed topic, is never delivered.
            if !webhook_event_matches_endpoint_scope(
                &endpoint.tenant_id,
                &endpoint.topic_pattern,
                &event.topic,
                &event.payload,
            ) {
                continue;
            }
            if terminal_delivery_exists(pool, &endpoint.endpoint_id, &event.event_id).await? {
                continue;
            }
            let signature = sign_webhook_body(&endpoint.signing_secret, &body);
            let max_attempts = clamp_max_attempts(endpoint.max_attempts).max(1) as u32;
            let mut last_status: i32 = 0;
            let mut last_error = String::new();
            let mut ok = false;
            let mut attempts_observed: i32 = 0;
            for attempt in 1..=max_attempts {
                attempts_observed = attempt as i32;
                // SSRF re-check at DELIVERY time (DNS rebinding).
                if let Err(err) = resolve_and_validate_target(&endpoint.url).await {
                    last_error = err.message().to_string();
                    break; // a blocked target never becomes deliverable on retry
                }
                match http
                    .post(endpoint.url.as_str())
                    .header(SIGNATURE_HEADER, signature.as_str())
                    .header("content-type", "application/json")
                    .body(body.clone())
                    .send()
                    .await
                {
                    Ok(resp) => {
                        last_status = i32::from(resp.status().as_u16());
                        if resp.status().is_success() {
                            ok = true;
                            break;
                        }
                        last_error = format!("non-success status {last_status}");
                    }
                    Err(err) => {
                        last_error = err.to_string();
                    }
                }
                if attempt < max_attempts {
                    tokio::time::sleep(delivery_backoff(attempt)).await;
                }
            }

            let status = if ok { STATUS_DELIVERED } else { STATUS_DEAD };
            let attempts = attempts_observed.max(1);
            insert_delivery_journal(
                pool,
                &endpoint.tenant_id,
                &endpoint.endpoint_id,
                &event.event_id,
                &event.topic,
                status,
                attempts,
                last_status,
                &signature,
                &last_error,
                &event.payload,
            )
            .await;

            if ok {
                delivered = delivered.saturating_add(1);
                enqueue_outbox_event_with_context(
                    pool,
                    outbox_relation,
                    TOPIC_DELIVERY_SUCCEEDED,
                    &endpoint.endpoint_id,
                    &endpoint.tenant_id,
                    "",
                    serde_json::json!({
                        "tenant_id": endpoint.tenant_id,
                        "endpoint_id": endpoint.endpoint_id,
                        "event_id": event.event_id,
                        "topic": event.topic,
                        "response_status": last_status,
                    }),
                    NativeEventContext {
                        operation: "webhook.deliver".to_string(),
                        target_resource: endpoint.endpoint_id.clone(),
                        ..NativeEventContext::default()
                    },
                    metrics,
                )
                .await;
            } else {
                // Dead-letter after max attempts (fail closed, durably recorded).
                enqueue_outbox_event_with_context(
                    pool,
                    outbox_relation,
                    TOPIC_DELIVERY_DEAD,
                    &endpoint.endpoint_id,
                    &endpoint.tenant_id,
                    "",
                    serde_json::json!({
                        "tenant_id": endpoint.tenant_id,
                        "endpoint_id": endpoint.endpoint_id,
                        "event_id": event.event_id,
                        "topic": event.topic,
                        "attempts": attempts,
                        "last_error": last_error,
                    }),
                    NativeEventContext {
                        operation: "webhook.dead_letter".to_string(),
                        outcome: "failure".to_string(),
                        target_resource: endpoint.endpoint_id.clone(),
                        ..NativeEventContext::default()
                    },
                    metrics,
                )
                .await;
            }
        }
    }
    Ok(delivered)
}

/// Run one leader-owned worker tick from the durable CDC journal. The loader
/// returns endpoint/event pairs that still lack a terminal delivery journal row,
/// so repeated ticks are idempotent and bounded.
#[cfg(feature = "http-client")]
pub(crate) async fn run_webhook_delivery_worker_once(
    http: &reqwest::Client,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let jobs = load_webhook_delivery_jobs(pool, journal_relation, batch).await?;
    let mut delivered = 0u64;
    for job in &jobs {
        delivered = delivered.saturating_add(
            run_webhook_delivery_once(
                http,
                pool,
                outbox_relation,
                std::slice::from_ref(&job.target),
                std::slice::from_ref(&job.event),
                metrics,
            )
            .await?,
        );
    }
    Ok(i64::try_from(delivered).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod webhook_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_url_field_violation(status: &Status, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "url");
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn assert_policy_detail(status: &Status, policy_decision_id: &str) {
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, "webhook_delivery_ssrf");
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_schema_not_found_detail(status: &Status, operation: &str) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "webhook endpoint not found");
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "webhook");
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, "webhook_endpoint_not_found");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    fn assert_internal_detail(status: &Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "webhook");
        assert_eq!(detail.operation, operation);
        assert!(detail.capability_required.is_empty());
        assert!(detail.policy_decision_id.is_empty());
        assert!(detail.field_violations.is_empty());
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// Every private/loopback/link-local/CGNAT/unspecified range — in both IP
    /// families and as raw-IP literal targets — is rejected by the WRITE-time SSRF
    /// guard; a public https target with a hostname is accepted.
    #[test]
    fn ssrf_guard_rejects_private_ranges() {
        let blocked = [
            "https://127.0.0.1/hook",         // 127/8 loopback
            "https://127.255.255.254/hook",   // 127/8 (upper)
            "https://10.0.0.1/hook",          // 10/8
            "https://10.255.255.255/hook",    // 10/8 (upper)
            "https://172.16.0.1/hook",        // 172.16/12 (lower)
            "https://172.31.255.254/hook",    // 172.16/12 (upper)
            "https://192.168.1.1/hook",       // 192.168/16
            "https://169.254.169.254/hook",   // 169.254/16 (cloud metadata)
            "https://100.64.0.1/hook",        // 100.64/10 CGNAT (lower)
            "https://100.127.255.254/hook",   // 100.64/10 CGNAT (upper)
            "https://0.0.0.0/hook",           // 0/8 + unspecified
            "https://0.1.2.3/hook",           // 0/8 "this network"
            "https://[::1]/hook",             // IPv6 loopback
            "https://[::]/hook",              // IPv6 unspecified
            "https://[fc00::1]/hook",         // fc00::/7 ULA
            "https://[fd12:3456::1]/hook",    // fc00::/7 ULA (fd)
            "https://[fe80::1]/hook",         // fe80::/10 link-local
            "https://[::ffff:10.0.0.1]/hook", // IPv4-mapped private
            "https://localhost/hook",         // local hostname
        ];
        for url in blocked {
            let err = validate_webhook_target_url(url)
                .expect_err(&format!("SSRF guard must reject {url}"));
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "wrong code for {url}"
            );
        }
        // Non-https is rejected even for a public host.
        assert_eq!(
            validate_webhook_target_url("http://hooks.example.com/x")
                .expect_err("cleartext http must be rejected")
                .code(),
            tonic::Code::InvalidArgument
        );
        // A public https hostname target is accepted (DNS is checked at delivery).
        validate_webhook_target_url("https://hooks.example.com/events")
            .expect("public https target should be accepted at write time");
    }

    #[test]
    fn webhook_url_validation_carries_field_violations() {
        let non_https = validate_webhook_target_url("http://hooks.example.com/x")
            .expect_err("cleartext http must fail");
        assert_eq!(
            non_https.message(),
            "webhook url must use https (cleartext http and non-http schemes are rejected)"
        );
        assert_url_field_violation(&non_https, "must use https scheme");

        let malformed_ipv6 = validate_webhook_target_url("https://[::1/hook")
            .expect_err("malformed IPv6 host must fail");
        assert_eq!(
            malformed_ipv6.message(),
            "webhook url has a malformed IPv6 host"
        );
        assert_url_field_violation(
            &malformed_ipv6,
            "must contain a well-formed bracketed IPv6 host",
        );

        let missing_host =
            validate_webhook_target_url("https:///hook").expect_err("missing host must fail");
        assert_eq!(
            missing_host.message(),
            "webhook url must include a valid host"
        );
        assert_url_field_violation(&missing_host, "must include a valid external host");

        let private = validate_webhook_target_url("https://10.0.0.1/hook")
            .expect_err("private IP target must fail");
        assert_eq!(
            private.message(),
            "webhook url host 10.0.0.1 resolves to a private/loopback/link-local address (SSRF blocked)"
        );
        assert_url_field_violation(
            &private,
            "must not target private, loopback, link-local, CGNAT, unspecified, multicast, or reserved IP ranges",
        );

        let localhost = validate_webhook_target_url("https://localhost/hook")
            .expect_err("localhost target must fail");
        assert_eq!(
            localhost.message(),
            "webhook url host localhost is not an allowed external target (SSRF blocked)"
        );
        assert_url_field_violation(&localhost, "must not target localhost hostnames");
    }

    #[test]
    fn delivery_time_ssrf_denials_carry_policy_detail() {
        let unresolved = webhook_host_unresolved_status("hooks.example.test", "dns timeout");
        assert_eq!(
            unresolved.message(),
            "webhook host hooks.example.test did not resolve: dns timeout"
        );
        assert_policy_detail(&unresolved, "webhook_host_unresolved");

        let blocked =
            webhook_host_blocked_address_status("hooks.example.test", "10.0.0.1".parse().unwrap());
        assert_eq!(
            blocked.message(),
            "webhook host hooks.example.test resolved to a blocked address 10.0.0.1 (SSRF/DNS-rebinding blocked)"
        );
        assert_policy_detail(&blocked, "webhook_host_blocked_address");

        let empty = webhook_host_no_addresses_status("hooks.example.test");
        assert_eq!(
            empty.message(),
            "webhook host hooks.example.test resolved to no addresses"
        );
        assert_policy_detail(&empty, "webhook_host_no_addresses");
    }

    /// Direct range coverage of the pure IP classifier (boundaries included).
    #[test]
    fn ip_classifier_boundaries() {
        let blocked: &[&str] = &[
            "10.0.0.0",
            "10.255.255.255",
            "172.16.0.0",
            "172.31.255.255",
            "192.168.0.0",
            "192.168.255.255",
            "127.0.0.1",
            "169.254.0.1",
            "100.64.0.0",
            "100.127.255.255",
            "0.0.0.0",
            "::1",
            "::",
            "fc00::",
            "fdff::1",
            "fe80::abcd",
        ];
        for ip in blocked {
            assert!(ip_is_blocked(ip.parse().unwrap()), "{ip} should be blocked");
        }
        let allowed: &[&str] = &[
            "8.8.8.8",
            "1.1.1.1",
            "203.0.113.10",
            "100.63.255.255",
            "100.128.0.0",
            "172.15.255.255",
            "172.32.0.1",
            "2606:4700:4700::1111",
        ];
        for ip in allowed {
            assert!(
                !ip_is_blocked(ip.parse().unwrap()),
                "{ip} should be allowed"
            );
        }
    }

    /// A tenant-B event never matches a tenant-A endpoint scope (fail closed,
    /// tenant BOUND to the endpoint — never pattern-only). Same tenant + matching
    /// topic delivers; a tenant-less event is dropped.
    #[test]
    fn delivery_scope_is_tenant_bound() {
        let pattern = "udb.*";
        let tenant_a_event = serde_json::json!({ "tenant_id": "tenant-a", "x": 1 });
        let tenant_b_event = serde_json::json!({ "tenant_id": "tenant-b", "x": 1 });
        let tenantless = serde_json::json!({ "x": 1 });

        // Same tenant + subscribed topic → delivered.
        assert!(webhook_event_matches_endpoint_scope(
            "tenant-a",
            pattern,
            "udb.invoice.created.v1",
            &tenant_a_event
        ));
        // Cross-tenant event must NEVER reach tenant-a's endpoint.
        assert!(!webhook_event_matches_endpoint_scope(
            "tenant-a",
            pattern,
            "udb.invoice.created.v1",
            &tenant_b_event
        ));
        // Tenant-less event is dropped (no pattern-only fan-out, the fail-open trap).
        assert!(!webhook_event_matches_endpoint_scope(
            "tenant-a",
            pattern,
            "udb.invoice.created.v1",
            &tenantless
        ));
        // Subscribed tenant but non-matching topic → not delivered.
        assert!(!webhook_event_matches_endpoint_scope(
            "tenant-a",
            "udb.payment.*",
            "udb.invoice.created.v1",
            &tenant_a_event
        ));
        // An empty pattern subscribes to nothing (fail closed).
        assert!(!webhook_event_matches_endpoint_scope(
            "tenant-a",
            "",
            "udb.invoice.created.v1",
            &tenant_a_event
        ));
    }

    /// The `X-Udb-Signature` verifies against an in-test receiver recomputing the
    /// HMAC with the shared secret; a wrong secret or tampered body does not.
    #[test]
    fn hmac_signature_round_trips() {
        let secret = "shhh-per-endpoint-secret";
        let body = br#"{"tenant_id":"tenant-a","event":"invoice.created"}"#;
        let signature = sign_webhook_body(secret, body);
        assert!(signature.starts_with("sha256="));

        // Receiver side: recompute and compare (constant-form string compare here;
        // the header value is the canonical hex digest).
        let expected = sign_webhook_body(secret, body);
        assert_eq!(signature, expected, "same secret + body must verify");

        // Wrong secret does not verify.
        assert_ne!(signature, sign_webhook_body("wrong-secret", body));
        // Tampered body does not verify.
        let tampered = br#"{"tenant_id":"tenant-b","event":"invoice.created"}"#;
        assert_ne!(signature, sign_webhook_body(secret, tampered));
    }

    /// A caller scoped to tenant-a must not register/read an endpoint for tenant-b
    /// by putting a foreign tenant_id in the request BODY; the scope guard rejects
    /// this before any pool/DB access — mirrors `tenant_service`/`lock_service`.
    #[tokio::test]
    async fn create_endpoint_rejects_cross_tenant_body() {
        let svc = WebhookServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(webhook_pb::CreateEndpointRequest {
            tenant_id: "tenant-b".to_string(),
            url: "https://hooks.example.com/x".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_endpoint(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn create_endpoint_missing_url_carries_field_violation() {
        let svc = WebhookServiceImpl::new(); // no pool, no channels (admit no-op)
        let mut request = Request::new(webhook_pb::CreateEndpointRequest {
            tenant_id: "tenant-a".to_string(),
            url: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .create_endpoint(request)
            .await
            .expect_err("missing url must be rejected before pool access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "url is required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "url");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty HTTPS webhook URL"
        );
    }

    #[test]
    fn webhook_missing_postgres_capability_carries_typed_detail() {
        let err = webhook_capability_status(
            "postgres_store",
            "postgres_store",
            "webhook service requires a Postgres-backed store (no PG pool configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "webhook service requires a Postgres-backed store (no PG pool configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "webhook");
        assert_eq!(detail.operation, "postgres_store");
        assert_eq!(detail.capability_required, "postgres_store");
        assert!(!detail.retryable);
    }

    #[test]
    fn webhook_endpoint_not_found_statuses_carry_schema_detail() {
        for operation in ["get_endpoint", "update_endpoint", "delete_endpoint"] {
            assert_schema_not_found_detail(
                &webhook_endpoint_not_found_status(operation),
                operation,
            );
        }
    }

    #[test]
    fn webhook_internal_status_carries_typed_detail() {
        assert_internal_detail(
            &webhook_internal_status(
                "list_webhook_deliveries",
                "list webhook deliveries failed: database is unavailable",
            ),
            "list_webhook_deliveries",
            "list webhook deliveries failed: database is unavailable",
        );
    }

    /// Backoff grows monotonically and is capped.
    #[test]
    fn backoff_is_bounded() {
        assert_eq!(delivery_backoff(1), DELIVERY_BACKOFF_BASE);
        assert!(delivery_backoff(2) > delivery_backoff(1));
        assert!(delivery_backoff(100) <= DELIVERY_BACKOFF_CAP);
    }
}

impl DataBrokerService {
    /// Build the native `WebhookService`, wired to the broker's Postgres pool, the
    /// shared outbox, and the per-tenant fair-admission manager.
    pub(crate) fn build_webhook_service(&self) -> WebhookServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime
            .native_store_pool_for_service("webhook", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        WebhookServiceImpl::new()
            .with_postgres(pg_pool)
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
