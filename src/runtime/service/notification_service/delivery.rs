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
use super::config::{NOTIFICATION_DELIVERY_PROVIDERS_ENV, delivery_event_topic};
#[cfg(feature = "http-client")]
use super::model::{channel_from_db, channel_to_db, delivery_attempt_model, log_model};
#[cfg(feature = "http-client")]
use super::store::write_delivery_attempt;

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
}

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
            .finish()
    }
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
            Some(NotificationDeliveryProvider {
                channel,
                provider,
                endpoint_url,
                wrapped_credential,
                body_template,
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
    let rows = sqlx::query(&format!(
        "SELECT \
            l.{log_id}::TEXT AS log_id, \
            l.{tenant_id}::TEXT AS tenant_id, \
            l.{channel}::TEXT AS channel, \
            COALESCE(l.{recipient_address}::TEXT, '') AS recipient_address, \
            COALESCE(l.{rendered_subject}::TEXT, '') AS rendered_subject, \
            COALESCE(l.{rendered_body}::TEXT, '') AS rendered_body \
         FROM {log_rel} l \
         WHERE l.{status} = $1 \
           AND COALESCE(l.{tenant_id}::TEXT, '') <> '' \
           AND COALESCE(l.{recipient_address}::TEXT, '') <> '' \
           AND NOT EXISTS ( \
               SELECT 1 FROM {attempt_rel} a \
               WHERE a.{attempt_notification_id} = l.{log_id} \
                 AND a.{attempt_channel} = l.{channel} \
                 AND a.{attempt_status} IN ($2, $3) \
           ) \
         ORDER BY l.{created_at} ASC, l.{log_id} ASC \
         LIMIT $4",
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
        attempt_status = attempt.q("status"),
    ))
    .bind("PENDING")
    .bind("SENT")
    .bind("DELIVERED")
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
) {
    let channel_db = channel_to_db(intent.channel);
    if let Err(err) = write_delivery_attempt(
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
        tracing::warn!(log_id = %intent.log_id, error = %err, "notification delivery attempt write failed");
        return;
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
        // Attempt delivery: POST the rendered message to the provider API,
        // authenticated with the decrypted credential (used, never logged).
        let outcome = http
            .post(provider.endpoint_url.as_str())
            .bearer_auth(credential.0.as_str())
            .json(&request_body)
            .send()
            .await;
        match outcome {
            Ok(resp) if resp.status().is_success() => {
                let provider_message_id = resp
                    .headers()
                    .get("x-provider-message-id")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string();
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
            providers
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
    use super::render_provider_body;

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
