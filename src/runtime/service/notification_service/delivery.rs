//! The leader-elected notification delivery worker + generic provider adapter for
//! the native `NotificationService` (master-plan 9.13), extracted verbatim. The
//! provider/credential/intent value types are always compiled (the `ReportDelivery`
//! unit guards exercise the redaction doctrine); the provider-config parsing, the
//! durable intent loader, the vault-decrypt + SSRF-revalidating HTTP dispatch, and
//! the leader-tick entry points are `#[cfg(feature = "http-client")]` — no
//! `http-client` feature means no external HTTP delivery worker.
//!
//! `serve()` spawns [`run_notification_delivery_worker_once`] under
//! `singleton::WORKER_NOTIFICATION_DELIVERY` when `http-client` is built. Provider
//! configuration is intentionally generic and envelope-based:
//! `UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON` is resolved once and contains only
//! {channel, provider, endpoint_url, wrapped_credential}; provider-specific SDKs
//! stay in sidecars. The SSRF guard at delivery time REUSES the webhook lane's
//! `webhook_service::resolve_and_validate_target` (never re-implemented).

#[cfg(feature = "http-client")]
use std::sync::{Arc, OnceLock};

#[cfg(feature = "http-client")]
use sqlx::{PgPool, Row};
#[cfg(feature = "http-client")]
use uuid::Uuid;

#[cfg(feature = "http-client")]
use crate::metrics::MetricsRecorder;
#[cfg(feature = "http-client")]
use crate::proto::udb::core::notification::entity::v1 as notif_entity_pb;
#[cfg(feature = "http-client")]
use crate::runtime::DataBrokerRuntime;

#[cfg(feature = "http-client")]
use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
#[cfg(feature = "http-client")]
use super::config::{
    NOTIFICATION_DELIVERY_PROVIDERS_ENV, TOPIC_NOTIFICATION_DEAD_LETTERED, delivery_event_topic,
    delivery_exhausted_retries, max_delivery_attempts, notification_backoff_base_secs,
    notification_backoff_cap_secs,
};
#[cfg(feature = "http-client")]
use super::model::{channel_from_db, channel_to_db, delivery_attempt_model, log_model};
#[cfg(feature = "http-client")]
use super::store::{log_transition_allowed_prev, transition_log_status, write_delivery_attempt};

/// A decrypted provider credential in flight. Redacting `Debug` from day one so
/// the secret never leaks into a log line or panic (the redaction doctrine,
/// mirroring `vault_service::PlaintextSecret`).
#[allow(dead_code)]
pub(crate) struct ProviderCredential(pub(crate) String);

impl std::fmt::Debug for ProviderCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProviderCredential")
            .field(&"[redacted]")
            .finish()
    }
}

/// How a delivery provider authenticates each POST. The actual secret is ALWAYS
/// the vault-sealed `wrapped_credential` (decrypted only at delivery time); only
/// the NON-secret scheme parameters (header/field names, basic username) live
/// here, so the redaction doctrine holds — no variant ever carries a plaintext
/// secret and `Debug` is safe to derive.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum ProviderAuth {
    /// `Authorization: Bearer <credential>` (the default; back-compatible with the
    /// previous bearer-only adapter).
    #[default]
    Bearer,
    /// A custom API-key header: `<header>: <credential>` (e.g. `X-API-Key`).
    ApiKey { header: String },
    /// HTTP Basic — `<username>:<credential>` (the credential is the password).
    Basic { username: String },
    /// HMAC-SHA256 over the exact request body, keyed by `<credential>`, hex-encoded
    /// into `<header>` (e.g. `X-Signature`).
    Hmac { header: String },
}

/// One configured delivery provider for a channel. `wrapped_credential` is the
/// vault-sealed API key (the `udb-aead:` envelope `encrypt_secret_at_rest`
/// produces); it is decrypted ONLY at the moment of use and never stored
/// plaintext or logged.
#[allow(dead_code)]
#[derive(Clone)]
pub(crate) struct NotificationDeliveryProvider {
    pub channel: i32,
    pub provider: String,
    /// The provider's HTTPS API endpoint (SSRF-validated at delivery time).
    pub endpoint_url: String,
    pub wrapped_credential: String,
    /// E-1: optional JSON request-body template so a provider whose API is not the
    /// default `{to,subject,body}` shape (e.g. a regional SMS gateway) works from
    /// config, without a translating sidecar. Placeholders `{{to}}`, `{{subject}}`,
    /// `{{body}}` are substituted with JSON-escaped values, then parsed as JSON.
    /// `None`/empty keeps the default body shape.
    pub body_template: Option<String>,
    /// Per-provider auth scheme applied to the POST (default `Bearer`).
    pub auth: ProviderAuth,
    /// Header name carrying the stable idempotency key so a crash-retry POST can't
    /// double-send (the provider dedupes on it). Empty disables the header; the
    /// default is `Idempotency-Key`.
    pub idempotency_header: String,
    /// Response header to read the provider's real message-id from (preferred, no
    /// body read). `None` falls back to `message_id_json_path`.
    pub message_id_header: Option<String>,
    /// Dot-separated JSON path into the response body for the provider's message-id
    /// (e.g. `data.id`, `messages.0.id`) when it isn't returned in a header.
    pub message_id_json_path: Option<String>,
}

/// The default idempotency header when a provider config doesn't override it.
#[allow(dead_code)]
pub(crate) const DEFAULT_IDEMPOTENCY_HEADER: &str = "Idempotency-Key";

/// Render a provider's `body_template` into the JSON request body by substituting
/// the JSON-escaped recipient/subject/body, then parsing the result. Returns an
/// error (never a silent default) when the template does not produce valid JSON,
/// so a misconfigured template is caught rather than silently mis-sending.
pub(crate) fn render_provider_body(
    template: &str,
    to: &str,
    subject: &str,
    body: &str,
) -> Result<serde_json::Value, String> {
    // JSON-escaped inner form (no surrounding quotes), safe to splice inside the
    // template's existing string quotes.
    let esc = |s: &str| -> String {
        let quoted = serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string());
        quoted[1..quoted.len().saturating_sub(1)].to_string()
    };
    let rendered = template
        .replace("{{to}}", &esc(to))
        .replace("{{subject}}", &esc(subject))
        .replace("{{body}}", &esc(body));
    serde_json::from_str(&rendered)
        .map_err(|err| format!("notification body_template did not render to valid JSON: {err}"))
}

// Redacting Debug so a provider config never leaks its wrapped credential.
impl std::fmt::Debug for NotificationDeliveryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationDeliveryProvider")
            .field("channel", &self.channel)
            .field("provider", &self.provider)
            .field("endpoint_url", &self.endpoint_url)
            .field("wrapped_credential", &"[redacted]")
            .field("auth", &self.auth)
            .field("idempotency_header", &self.idempotency_header)
            .field("message_id_header", &self.message_id_header)
            .field("message_id_json_path", &self.message_id_json_path)
            .finish()
    }
}

/// The concrete authentication to apply to a provider POST, computed purely from
/// the scheme + decrypted credential (+ body, for HMAC). Kept separate from the
/// reqwest builder so it is unit-testable without a live client. Never
/// `Debug`-printed — it carries the live credential / HMAC signature.
#[allow(dead_code)]
pub(crate) enum ProviderAuthApplication {
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// `Authorization: Basic base64(username:password)`.
    Basic { username: String, password: String },
    /// A single explicit header (api-key value or HMAC signature).
    Header { name: String, value: String },
}

/// Compute the auth application for a provider POST. `credential` is the decrypted
/// secret (used, never logged); `body` is the exact request-body string (read only
/// to sign the HMAC scheme). Pure + unit-tested per scheme.
#[allow(dead_code)]
pub(crate) fn build_auth_application(
    auth: &ProviderAuth,
    credential: &str,
    body: &str,
) -> ProviderAuthApplication {
    match auth {
        ProviderAuth::Bearer => ProviderAuthApplication::Bearer(credential.to_string()),
        ProviderAuth::ApiKey { header } => ProviderAuthApplication::Header {
            name: header.clone(),
            value: credential.to_string(),
        },
        ProviderAuth::Basic { username } => ProviderAuthApplication::Basic {
            username: username.clone(),
            password: credential.to_string(),
        },
        ProviderAuth::Hmac { header } => {
            // Sign the exact bytes we send, keyed by the decrypted credential; reuse
            // the shared HMAC-SHA256 primitive (no hand-rolled crypto).
            let mac = crate::runtime::security::hmac_sha256(credential.as_bytes(), body.as_bytes());
            let hex: String = mac.iter().map(|byte| format!("{byte:02x}")).collect();
            ProviderAuthApplication::Header {
                name: header.clone(),
                value: hex,
            }
        }
    }
}

/// A stable idempotency key for a (log, channel) delivery so a crash-retry POST
/// can't double-send (the provider dedupes on it): `<log_id>:<channel_db>`. Pure +
/// unit-tested.
#[allow(dead_code)]
pub(crate) fn idempotency_key(log_id: &str, channel_db: &str) -> String {
    format!("{log_id}:{channel_db}")
}

/// Extract a provider's real message-id from a dot-separated JSON path
/// (e.g. `data.id`, `messages.0.id`). Object keys and array indices are both
/// resolved; a scalar (string/number/bool) is stringified, anything else (missing
/// key, non-scalar terminus) yields "". Pure + unit-tested.
#[allow(dead_code)]
pub(crate) fn extract_json_path(value: &serde_json::Value, path: &str) -> String {
    let mut current = value;
    for segment in path.split('.').filter(|part| !part.is_empty()) {
        current = match current {
            serde_json::Value::Object(map) => match map.get(segment) {
                Some(next) => next,
                None => return String::new(),
            },
            serde_json::Value::Array(items) => {
                match segment.parse::<usize>().ok().and_then(|idx| items.get(idx)) {
                    Some(next) => next,
                    None => return String::new(),
                }
            }
            _ => return String::new(),
        };
    }
    match current {
        serde_json::Value::String(text) => text.clone(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::Bool(flag) => flag.to_string(),
        _ => String::new(),
    }
}

/// Whether a queued intent can NEVER be delivered because its channel requires a
/// recipient address but none was provided. EMAIL/SMS/PUSH require an address;
/// IN_APP legitimately has none (delivered in-app) and must never be failed for
/// this — otherwise a valid in-app notification would be wrongly marked FAILED.
/// A permanent condition: the worker fails such a row out of the PENDING queue
/// instead of leaving it to loop forever. Pure + unit-tested.
#[allow(dead_code)]
pub(crate) fn missing_recipient_should_fail(channel: i32, recipient_address: &str) -> bool {
    use crate::proto::udb::core::notification::entity::v1::NotificationChannel as C;
    if !recipient_address.trim().is_empty() {
        return false;
    }
    matches!(
        C::try_from(channel).unwrap_or(C::Unspecified),
        C::Email | C::Sms | C::Push
    )
}

/// Read the provider's real message-id from the response: a configured response
/// header first (cheap, no body read), else a configured JSON path into the
/// response body. Returns "" when neither is configured or present. Replaces the
/// made-up `x-provider-message-id` header no real provider returns.
#[cfg(feature = "http-client")]
async fn extract_provider_message_id(
    provider: &NotificationDeliveryProvider,
    resp: reqwest::Response,
) -> String {
    if let Some(name) = provider
        .message_id_header
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if let Some(value) = resp
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
        {
            let value = value.trim();
            if !value.is_empty() {
                return value.to_string();
            }
        }
    }
    if let Some(path) = provider
        .message_id_json_path
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        if let Ok(body) = resp.json::<serde_json::Value>().await {
            return extract_json_path(&body, path);
        }
    }
    String::new()
}

#[cfg(feature = "http-client")]
fn parse_delivery_channel(value: &serde_json::Value) -> Option<i32> {
    match value {
        serde_json::Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
        serde_json::Value::String(raw) => {
            let raw = raw.trim();
            if let Ok(n) = raw.parse::<i32>() {
                return Some(n);
            }
            let normalized = raw.to_ascii_uppercase().replace('-', "_");
            let channel = match normalized.as_str() {
                "EMAIL" => notif_entity_pb::NotificationChannel::Email,
                "SMS" => notif_entity_pb::NotificationChannel::Sms,
                "PUSH" => notif_entity_pb::NotificationChannel::Push,
                "IN_APP" => notif_entity_pb::NotificationChannel::InApp,
                "WEBHOOK" => notif_entity_pb::NotificationChannel::Webhook,
                _ => return None,
            };
            Some(channel as i32)
        }
        _ => None,
    }
}

/// Parse the per-provider `auth` config into a [`ProviderAuth`]. Accepts either a
/// bare scheme string (`"bearer"`) or an object `{ "scheme": ..., ... }`. Only the
/// non-secret scheme parameters are read here; the secret is always the sealed
/// `wrapped_credential`. An absent/unknown scheme falls back to `Bearer`.
#[cfg(feature = "http-client")]
fn parse_provider_auth(value: Option<&serde_json::Value>) -> ProviderAuth {
    let Some(value) = value else {
        return ProviderAuth::Bearer;
    };
    let (scheme, obj) = match value {
        serde_json::Value::String(raw) => (raw.trim().to_ascii_lowercase(), None),
        serde_json::Value::Object(map) => {
            let scheme = map
                .get("scheme")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("bearer")
                .trim()
                .to_ascii_lowercase();
            (scheme, Some(map))
        }
        _ => return ProviderAuth::Bearer,
    };
    let field = |key: &str| -> Option<String> {
        obj.and_then(|map| map.get(key))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    match scheme.as_str() {
        "api_key" | "apikey" | "api-key" => ProviderAuth::ApiKey {
            header: field("header").unwrap_or_else(|| "X-API-Key".to_string()),
        },
        "basic" => ProviderAuth::Basic {
            username: field("username").unwrap_or_default(),
        },
        "hmac" => ProviderAuth::Hmac {
            header: field("header").unwrap_or_else(|| "X-Signature".to_string()),
        },
        // "bearer" and anything unrecognized fail safe to the bearer default.
        _ => ProviderAuth::Bearer,
    }
}

#[cfg(feature = "http-client")]
pub(crate) fn parse_notification_delivery_providers_json(
    raw: &str,
) -> Vec<NotificationDeliveryProvider> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw.trim()) else {
        return Vec::new();
    };
    let Some(items) = value.as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let obj = item.as_object()?;
            let channel = parse_delivery_channel(obj.get("channel")?)?;
            let provider = obj
                .get("provider")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let endpoint_url = obj
                .get("endpoint_url")
                .or_else(|| obj.get("url"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let wrapped_credential = obj
                .get("wrapped_credential")
                .or_else(|| obj.get("credential"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            if provider.is_empty() || endpoint_url.is_empty() || wrapped_credential.is_empty() {
                return None;
            }
            let body_template = obj
                .get("body_template")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let auth = parse_provider_auth(obj.get("auth"));
            let optional_string = |key: &str| -> Option<String> {
                obj.get(key)
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
            };
            // Default the idempotency header on; an explicit "" opts out.
            let idempotency_header = obj
                .get("idempotency_header")
                .and_then(serde_json::Value::as_str)
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|| DEFAULT_IDEMPOTENCY_HEADER.to_string());
            let message_id_header = optional_string("message_id_header");
            let message_id_json_path = optional_string("message_id_json_path");
            Some(NotificationDeliveryProvider {
                channel,
                provider,
                endpoint_url,
                wrapped_credential,
                body_template,
                auth,
                idempotency_header,
                message_id_header,
                message_id_json_path,
            })
        })
        .collect()
}

#[cfg(feature = "http-client")]
fn notification_delivery_providers() -> &'static Vec<NotificationDeliveryProvider> {
    static PROVIDERS: OnceLock<Vec<NotificationDeliveryProvider>> = OnceLock::new();
    PROVIDERS.get_or_init(|| {
        std::env::var(NOTIFICATION_DELIVERY_PROVIDERS_ENV)
            .ok()
            .map(|raw| parse_notification_delivery_providers_json(&raw))
            .unwrap_or_default()
    })
}

/// One queued notification intent (a PENDING NotificationLog) the worker drains.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct NotificationDeliveryIntent {
    pub log_id: String,
    pub tenant_id: String,
    pub channel: i32,
    pub recipient_address: String,
    pub rendered_subject: String,
    pub rendered_body: String,
}

#[cfg(feature = "http-client")]
fn intent_from_row(row: &sqlx::postgres::PgRow) -> Result<NotificationDeliveryIntent, String> {
    let channel_db: String = row
        .try_get("channel")
        .map_err(|err| format!("decode notification delivery channel failed: {err}"))?;
    Ok(NotificationDeliveryIntent {
        log_id: row
            .try_get("log_id")
            .map_err(|err| format!("decode notification delivery log id failed: {err}"))?,
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|err| format!("decode notification delivery tenant failed: {err}"))?,
        channel: channel_from_db(&channel_db),
        recipient_address: row
            .try_get("recipient_address")
            .map_err(|err| format!("decode notification delivery recipient failed: {err}"))?,
        rendered_subject: row
            .try_get("rendered_subject")
            .map_err(|err| format!("decode notification delivery subject failed: {err}"))?,
        rendered_body: row
            .try_get("rendered_body")
            .map_err(|err| format!("decode notification delivery body failed: {err}"))?,
    })
}

#[cfg(feature = "http-client")]
async fn load_notification_delivery_intents(
    pool: &PgPool,
    batch: i64,
) -> Result<Vec<NotificationDeliveryIntent>, String> {
    let log = log_model();
    let attempt = delivery_attempt_model();
    let log_rel = log.relation.clone();
    let attempt_rel = attempt.relation.clone();
    let limit = batch.max(1);
    // Exponential-backoff knobs (resolved once). The delay after the latest FAILED
    // attempt is `min(cap, base * 2^(attempt_count - 1))` — replicating the pure
    // `next_retry_delay_secs` — so a failing provider is re-POSTed with a growing gap
    // instead of every interval (the retry storm). Passed as binds so the SQL, not a
    // hardcode, carries the tunables.
    let backoff_base = notification_backoff_base_secs();
    let backoff_cap = notification_backoff_cap_secs();
    let rows = sqlx::query(&format!(
        "SELECT \
            l.{log_id}::TEXT AS log_id, \
            l.{tenant_id}::TEXT AS tenant_id, \
            l.{channel}::TEXT AS channel, \
            COALESCE(l.{recipient_address}::TEXT, '') AS recipient_address, \
            COALESCE(l.{rendered_subject}::TEXT, '') AS rendered_subject, \
            COALESCE(l.{rendered_body}::TEXT, '') AS rendered_body \
         FROM {log_rel} l \
         LEFT JOIN LATERAL ( \
             SELECT a.{attempt_count} AS attempt_count, \
                    a.{attempt_updated_at} AS updated_at, \
                    LEAST( \
                        $5::double precision, \
                        $4::double precision \
                            * power(2::double precision, LEAST(a.{attempt_count} - 1, 12)::double precision) \
                    ) AS delay_cap \
             FROM {attempt_rel} a \
             WHERE a.{attempt_notification_id} = l.{log_id} \
               AND a.{attempt_channel} = l.{channel} \
               AND a.{attempt_tenant} = l.{tenant_id} \
               AND a.{attempt_status} = 'FAILED' \
             ORDER BY a.{attempt_updated_at} DESC \
             LIMIT 1 \
         ) last_fail ON TRUE \
         WHERE l.{status} = $1 \
           AND COALESCE(l.{tenant_id}::TEXT, '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {attempt_rel} a \
               WHERE a.{attempt_notification_id} = l.{log_id} \
                 AND a.{attempt_channel} = l.{channel} \
                 AND a.{attempt_tenant} = l.{tenant_id} \
                 AND a.{attempt_status} IN ($2, $3) \
           ) \
           AND ( \
               last_fail.updated_at IS NULL \
               OR now() >= last_fail.updated_at \
                    + make_interval(secs => (0.5 + 0.5 * random()) * last_fail.delay_cap) \
           ) \
         ORDER BY l.{created_at} ASC, l.{log_id} ASC \
         LIMIT $6",
        log_id = log.q("log_id"),
        tenant_id = log.q("tenant_id"),
        channel = log.q("channel"),
        recipient_address = log.q("recipient_address"),
        rendered_subject = log.q("rendered_subject"),
        rendered_body = log.q("rendered_body"),
        status = log.q("status"),
        created_at = log.q("created_at"),
        attempt_notification_id = attempt.q("notification_id"),
        attempt_channel = attempt.q("channel"),
        attempt_tenant = attempt.q("tenant_id"),
        attempt_status = attempt.q("status"),
        attempt_count = attempt.q("attempt_count"),
        attempt_updated_at = attempt.q("updated_at"),
    ))
    .bind("PENDING")
    .bind("SENT")
    .bind("DELIVERED")
    .bind(backoff_base)
    .bind(backoff_cap)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|err| format!("load notification delivery intents failed: {err}"))?;

    rows.iter().map(intent_from_row).collect()
}

/// Write the terminal `NotificationDeliveryAttempt` (via the shared
/// `write_delivery_attempt` upsert) and emit `udb.notification.delivery.<status>.v1`.
/// Best-effort: a journal/emit failure is logged, never aborts the pass. The
/// `last_error` argument is a delivery diagnostic — NEVER a credential.
#[cfg(feature = "http-client")]
#[allow(clippy::too_many_arguments)]
async fn record_attempt_outcome(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
    notification_id: Uuid,
    intent: &NotificationDeliveryIntent,
    provider: &str,
    status_db: &str,
    last_error: &str,
    provider_message_id: &str,
    // A permanent failure (e.g. a missing recipient address) moves the log to the
    // terminal FAILED state on THIS attempt rather than waiting for the bounded
    // retry budget to exhaust — retrying a permanent error just loops.
    force_terminal: bool,
) {
    let channel_db = channel_to_db(intent.channel);
    // Capture the stored attempt row: its `attempt_count` (bumped by the upsert on
    // every attempt) drives the bounded-retry FAILED transition below.
    let attempt_row = match write_delivery_attempt(
        pool,
        notification_id,
        &intent.tenant_id,
        channel_db,
        provider,
        status_db,
        last_error,
        provider_message_id,
    )
    .await
    {
        Ok(row) => row,
        Err(err) => {
            tracing::warn!(log_id = %intent.log_id, error = %err, "notification delivery attempt write failed");
            return;
        }
    };
    // Reflect a successful delivery on the parent log (PENDING→SENT→DELIVERED) so
    // GetNotification/GetDeliveryStats stop reporting a delivered notification as
    // PENDING. Best-effort: a transition failure is logged, never aborts the pass.
    let allowed_prev = log_transition_allowed_prev(status_db);
    if !allowed_prev.is_empty() {
        if let Err(err) = transition_log_status(
            pool,
            notification_id,
            &intent.tenant_id,
            status_db,
            allowed_prev,
        )
        .await
        {
            tracing::warn!(log_id = %intent.log_id, error = %err, "notification log status transition failed");
        }
    } else if status_db == "FAILED" {
        // Bounded-retry (F7): a FAILED attempt leaves the log PENDING (the loader
        // only excludes SENT/DELIVERED), so a permanently-failing provider would be
        // re-POSTed every interval forever. Once the stored `attempt_count` reaches
        // `max_delivery_attempts()`, move the log to the terminal FAILED state so it
        // drops out of the PENDING queue. Below the ceiling it stays PENDING for the
        // next bounded auto-retry. FAILED can be reached from PENDING or SENT (never
        // downgrade a DELIVERED). Best-effort: a transition failure is logged.
        let attempt_count = attempt_row
            .as_ref()
            .and_then(|row| row.try_get::<i32, _>("attempt_count").ok())
            .unwrap_or(0);
        if force_terminal || delivery_exhausted_retries(attempt_count, max_delivery_attempts()) {
            match transition_log_status(
                pool,
                notification_id,
                &intent.tenant_id,
                "FAILED",
                &["PENDING", "SENT"],
            )
            .await
            {
                // DLQ: the log just crossed into the terminal FAILED (dead-letter)
                // state — either the bounded-retry budget is exhausted or this is a
                // permanent failure. `transition_log_status` moves a row out of
                // PENDING/SENT exactly once, so `moved == true` fires the dead-letter
                // event ONCE (a later scan can't re-select a non-PENDING log). Reuse
                // the outbox path the worker already uses — no new table — so operators
                // can alert on DLQ depth via `TOPIC_NOTIFICATION_DEAD_LETTERED`.
                Ok(true) => {
                    let reason = if force_terminal {
                        "permanent_failure"
                    } else {
                        "max_attempts_exhausted"
                    };
                    enqueue_outbox_event_with_context(
                        pool,
                        outbox_relation,
                        TOPIC_NOTIFICATION_DEAD_LETTERED,
                        &intent.log_id,
                        &intent.tenant_id,
                        "",
                        serde_json::json!({
                            "log_id": intent.log_id,
                            "tenant_id": intent.tenant_id,
                            "channel": channel_db,
                            "provider": provider,
                            "status": "FAILED",
                            "reason": reason,
                            "attempt_count": attempt_count,
                            "last_error": last_error,
                        }),
                        NativeEventContext {
                            operation: "notification.dead_letter".to_string(),
                            outcome: "failure".to_string(),
                            target_resource: intent.log_id.clone(),
                            ..NativeEventContext::default()
                        },
                        metrics,
                    )
                    .await;
                }
                // Already terminal / not owned by this tenant — no dead-letter event
                // (avoids a duplicate DLQ notification on a repeated scan).
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(log_id = %intent.log_id, error = %err, "notification log FAILED transition failed");
                }
            }
        }
    }
    let outcome = if status_db == "FAILED" {
        "failure"
    } else {
        "allow"
    };
    enqueue_outbox_event_with_context(
        pool,
        outbox_relation,
        &delivery_event_topic(status_db),
        &intent.log_id,
        &intent.tenant_id,
        "",
        serde_json::json!({
            "log_id": intent.log_id,
            "tenant_id": intent.tenant_id,
            "channel": channel_db,
            "provider": provider,
            "status": status_db,
            "provider_message_id": provider_message_id,
        }),
        NativeEventContext {
            operation: "notification.deliver".to_string(),
            outcome: outcome.to_string(),
            target_resource: intent.log_id.clone(),
            ..NativeEventContext::default()
        },
        metrics,
    )
    .await;
}

/// Run ONE leader pass of notification delivery (the consumer shape `serve()`
/// spawns under `NativeWorkerHost::spawn_while_leader(WORKER_NOTIFICATION_DELIVERY)`).
/// For each queued intent: find the channel's provider, SSRF-revalidate its URL at
/// delivery time (DNS rebinding), decrypt its credential through the vault transit
/// seam (used, never logged), attempt delivery, and write a
/// `NotificationDeliveryAttempt` + emit the terminal event. Returns the count
/// delivered. Best-effort per intent: one failing provider never aborts the pass.
#[cfg(feature = "http-client")]
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
pub(crate) async fn run_notification_delivery_once(
    http: &reqwest::Client,
    runtime: &DataBrokerRuntime,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    providers: &[NotificationDeliveryProvider],
    intents: &[NotificationDeliveryIntent],
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<u64, String> {
    let mut delivered: u64 = 0;
    for intent in intents {
        let Ok(notification_id) = Uuid::parse_str(intent.log_id.trim()) else {
            continue;
        };
        // An address-bearing channel (EMAIL/SMS/PUSH) with an empty
        // `recipient_address` can NEVER be delivered — without this it would sit
        // PENDING and be re-scanned forever. Fail it out of the queue with a clear
        // reason (permanent → terminal FAILED on this attempt). IN_APP has no
        // address by design and is never failed here.
        if missing_recipient_should_fail(intent.channel, &intent.recipient_address) {
            record_attempt_outcome(
                pool,
                outbox_relation,
                metrics,
                notification_id,
                intent,
                "",
                "FAILED",
                "missing recipient address",
                "",
                true,
            )
            .await;
            continue;
        }
        // The provider configured for this channel.
        let Some(provider) = providers.iter().find(|p| p.channel == intent.channel) else {
            record_attempt_outcome(
                pool,
                outbox_relation,
                metrics,
                notification_id,
                intent,
                "",
                "FAILED",
                "no delivery provider configured for channel",
                "",
                false,
            )
            .await;
            continue;
        };
        // SSRF guard at DELIVERY time (reuse the webhook resolver; defeats DNS
        // rebinding). A blocked target never becomes deliverable, so fail closed.
        if let Err(err) = crate::runtime::service::webhook_service::resolve_and_validate_target(
            &provider.endpoint_url,
        )
        .await
        {
            record_attempt_outcome(
                pool,
                outbox_relation,
                metrics,
                notification_id,
                intent,
                &provider.provider,
                "FAILED",
                err.message(),
                "",
                false,
            )
            .await;
            continue;
        }
        // Decrypt the provider credential ONLY at use; never log it.
        let credential = match runtime.decrypt_secret_at_rest(&provider.wrapped_credential) {
            Ok(secret) => ProviderCredential(secret),
            Err(_) => {
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    "provider credential unavailable (vault sealed?)",
                    "",
                    false,
                )
                .await;
                continue;
            }
        };
        // E-1: build the request body from the provider's template when set, else
        // the default `{to,subject,body}` shape. A template that renders to invalid
        // JSON fails the attempt (recorded) rather than silently mis-sending.
        let request_body = match provider
            .body_template
            .as_deref()
            .filter(|t| !t.trim().is_empty())
        {
            Some(template) => match render_provider_body(
                template,
                &intent.recipient_address,
                &intent.rendered_subject,
                &intent.rendered_body,
            ) {
                Ok(body) => body,
                Err(err) => {
                    record_attempt_outcome(
                        pool,
                        outbox_relation,
                        metrics,
                        notification_id,
                        intent,
                        &provider.provider,
                        "FAILED",
                        &err,
                        "",
                        false,
                    )
                    .await;
                    continue;
                }
            },
            None => serde_json::json!({
                "to": intent.recipient_address,
                "subject": intent.rendered_subject,
                "body": intent.rendered_body,
            }),
        };
        // Serialize the body ONCE so the HMAC scheme signs exactly the bytes sent.
        let body_bytes = match serde_json::to_string(&request_body) {
            Ok(body) => body,
            Err(err) => {
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    &format!("notification body serialization failed: {err}"),
                    "",
                    false,
                )
                .await;
                continue;
            }
        };
        // Attempt delivery: POST the rendered message to the provider API. A stable
        // idempotency key (log_id:channel) lets the provider dedupe a crash-retry so
        // it can't double-send, and the per-provider auth scheme is applied with the
        // decrypted credential (used, never logged).
        let mut builder = http
            .post(provider.endpoint_url.as_str())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_bytes.clone());
        if !provider.idempotency_header.trim().is_empty() {
            builder = builder.header(
                provider.idempotency_header.as_str(),
                idempotency_key(&intent.log_id, channel_to_db(intent.channel)).as_str(),
            );
        }
        builder = match build_auth_application(&provider.auth, credential.0.as_str(), &body_bytes) {
            ProviderAuthApplication::Bearer(token) => builder.bearer_auth(token),
            ProviderAuthApplication::Basic { username, password } => {
                builder.basic_auth(username, Some(password))
            }
            ProviderAuthApplication::Header { name, value } => {
                builder.header(name.as_str(), value.as_str())
            }
        };
        let outcome = builder.send().await;
        match outcome {
            Ok(resp) if resp.status().is_success() => {
                // Parse the provider's REAL message-id from its configured response
                // header / JSON path (not a made-up header) into provider_message_id.
                let provider_message_id = extract_provider_message_id(provider, resp).await;
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "SENT",
                    "",
                    &provider_message_id,
                    false,
                )
                .await;
                delivered = delivered.saturating_add(1);
            }
            Ok(resp) => {
                let status = resp.status();
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    &format!("provider returned status {status}"),
                    "",
                    false,
                )
                .await;
            }
            Err(err) => {
                record_attempt_outcome(
                    pool,
                    outbox_relation,
                    metrics,
                    notification_id,
                    intent,
                    &provider.provider,
                    "FAILED",
                    &err.to_string(),
                    "",
                    false,
                )
                .await;
            }
        }
    }
    Ok(delivered)
}

/// Run one leader-owned delivery pass from durable `NotificationLog` rows.
/// Provider config is resolved once from `UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON`.
/// If no providers are configured, the worker leaves queued intents untouched so a
/// later sidecar/provider rollout can drain them instead of poisoning attempts.
#[cfg(feature = "http-client")]
pub(crate) async fn run_notification_delivery_worker_once(
    http: &reqwest::Client,
    runtime: Arc<DataBrokerRuntime>,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
) -> Result<i64, String> {
    let providers = notification_delivery_providers();
    if providers.is_empty() {
        return Ok(0);
    }
    let intents = load_notification_delivery_intents(pool, batch).await?;
    let deliverable = intents
        .into_iter()
        .filter(|intent| {
            // Keep intents a configured provider can serve, PLUS undeliverable
            // empty-address rows on an address-bearing channel so the worker can
            // fail them out of the queue instead of leaving them to loop forever.
            missing_recipient_should_fail(intent.channel, &intent.recipient_address)
                || providers
                    .iter()
                    .any(|provider| provider.channel == intent.channel)
        })
        .collect::<Vec<_>>();
    let delivered = run_notification_delivery_once(
        http,
        runtime.as_ref(),
        pool,
        outbox_relation,
        providers,
        &deliverable,
        metrics,
    )
    .await?;
    Ok(i64::try_from(delivered).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod delivery_tests {
    use super::{
        ProviderAuth, ProviderAuthApplication, build_auth_application, extract_json_path,
        idempotency_key, missing_recipient_should_fail, render_provider_body,
    };
    use crate::proto::udb::core::notification::entity::v1::NotificationChannel as C;

    // ── item 1: per-provider auth scheme ────────────────────────────────────
    #[test]
    fn bearer_auth_uses_the_credential_as_the_token() {
        match build_auth_application(&ProviderAuth::Bearer, "sekret", "{}") {
            ProviderAuthApplication::Bearer(token) => assert_eq!(token, "sekret"),
            _ => panic!("bearer scheme must produce a bearer token"),
        }
    }

    #[test]
    fn api_key_auth_sets_the_configured_header_to_the_credential() {
        match build_auth_application(
            &ProviderAuth::ApiKey {
                header: "X-API-Key".to_string(),
            },
            "sekret",
            "{}",
        ) {
            ProviderAuthApplication::Header { name, value } => {
                assert_eq!(name, "X-API-Key");
                assert_eq!(value, "sekret");
            }
            _ => panic!("api_key scheme must produce a header"),
        }
    }

    #[test]
    fn basic_auth_uses_the_credential_as_the_password() {
        match build_auth_application(
            &ProviderAuth::Basic {
                username: "svc-user".to_string(),
            },
            "sekret",
            "{}",
        ) {
            ProviderAuthApplication::Basic { username, password } => {
                assert_eq!(username, "svc-user");
                assert_eq!(password, "sekret");
            }
            _ => panic!("basic scheme must produce basic credentials"),
        }
    }

    #[test]
    fn hmac_auth_signs_the_body_into_the_configured_header() {
        let body = r#"{"to":"a@example.com"}"#;
        let app = build_auth_application(
            &ProviderAuth::Hmac {
                header: "X-Signature".to_string(),
            },
            "sekret",
            body,
        );
        let expected = {
            let mac = crate::runtime::security::hmac_sha256(b"sekret", body.as_bytes());
            mac.iter().map(|b| format!("{b:02x}")).collect::<String>()
        };
        match app {
            ProviderAuthApplication::Header { name, value } => {
                assert_eq!(name, "X-Signature");
                assert_eq!(value.len(), 64, "hex HMAC-SHA256 is 64 chars: {value}");
                assert_eq!(
                    value, expected,
                    "signature must be HMAC-SHA256(credential, body)"
                );
                // The body must change the signature (it is over the body, not fixed).
                let other = build_auth_application(
                    &ProviderAuth::Hmac {
                        header: "X-Signature".to_string(),
                    },
                    "sekret",
                    r#"{"to":"b@example.com"}"#,
                );
                if let ProviderAuthApplication::Header { value: v2, .. } = other {
                    assert_ne!(
                        value, v2,
                        "a different body must yield a different signature"
                    );
                }
            }
            _ => panic!("hmac scheme must produce a header"),
        }
    }

    // ── item 2: idempotency key + JSON-path message-id ──────────────────────
    #[test]
    fn idempotency_key_is_stable_per_log_and_channel() {
        assert_eq!(idempotency_key("log-abc", "EMAIL"), "log-abc:EMAIL");
        // Same inputs → same key (a crash-retry POST reuses it, so the provider
        // dedupes); a different channel is a distinct delivery, hence a distinct key.
        assert_eq!(
            idempotency_key("log-abc", "EMAIL"),
            idempotency_key("log-abc", "EMAIL")
        );
        assert_ne!(
            idempotency_key("log-abc", "EMAIL"),
            idempotency_key("log-abc", "SMS")
        );
    }

    #[test]
    fn extract_json_path_walks_objects_and_array_indices() {
        let body = serde_json::json!({
            "data": { "id": "msg-42" },
            "messages": [ { "id": "m0" }, { "id": "m1" } ],
            "count": 7
        });
        assert_eq!(extract_json_path(&body, "data.id"), "msg-42");
        assert_eq!(extract_json_path(&body, "messages.1.id"), "m1");
        assert_eq!(extract_json_path(&body, "count"), "7");
        // A missing key or a non-scalar terminus yields "" (never panics).
        assert_eq!(extract_json_path(&body, "data.missing"), "");
        assert_eq!(extract_json_path(&body, "data"), "");
    }

    // ── item 3: empty recipient_address decision ────────────────────────────
    #[test]
    fn empty_address_fails_email_but_never_in_app() {
        // EMAIL/SMS/PUSH with no address can never be delivered → fail out.
        assert!(missing_recipient_should_fail(C::Email as i32, ""));
        assert!(missing_recipient_should_fail(C::Email as i32, "   "));
        assert!(missing_recipient_should_fail(C::Sms as i32, ""));
        assert!(missing_recipient_should_fail(C::Push as i32, ""));
        // IN_APP legitimately has no address → must NOT be failed.
        assert!(!missing_recipient_should_fail(C::InApp as i32, ""));
        // A present address is always fine, regardless of channel.
        assert!(!missing_recipient_should_fail(
            C::Email as i32,
            "a@example.com"
        ));
    }

    #[test]
    fn template_substitutes_and_parses_a_non_default_shape() {
        // A regional SMS gateway whose API is NOT {to,subject,body}.
        let tmpl = r#"{"msisdn": "{{to}}", "text": "{{body}}", "sender": "UDB"}"#;
        let body =
            render_provider_body(tmpl, "+8801700000000", "ignored", "your code is 4242").unwrap();
        assert_eq!(body["msisdn"], "+8801700000000");
        assert_eq!(body["text"], "your code is 4242");
        assert_eq!(body["sender"], "UDB");
    }

    #[test]
    fn template_escapes_values_to_stay_valid_json() {
        // A body with quotes/newlines must not break the JSON.
        let body = render_provider_body(r#"{"text": "{{body}}"}"#, "x", "y", "he said \"hi\"\nl2")
            .unwrap();
        assert_eq!(body["text"], "he said \"hi\"\nl2");
    }

    #[test]
    fn invalid_template_errors_rather_than_silently_sending() {
        let err = render_provider_body(r#"{"text": "{{body}}""#, "x", "y", "z").unwrap_err();
        assert!(err.contains("valid JSON"), "got: {err}");
    }
}
