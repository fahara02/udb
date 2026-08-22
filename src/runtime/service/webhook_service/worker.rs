//! The leader-elected webhook delivery worker for the native `WebhookService`,
//! extracted verbatim. The delivery targets/candidates are always compiled; the
//! durable-journal loaders, the SSRF-re-validating HTTP dispatch, and the
//! leader-tick entry points are `#[cfg(feature = "http-client")]` — no
//! `http-client` feature means no external HTTP worker.

#[cfg(feature = "http-client")]
use std::sync::Arc;

#[cfg(feature = "http-client")]
use sqlx::{PgPool, Row};
#[cfg(feature = "http-client")]
use uuid::Uuid;

#[cfg(feature = "http-client")]
use super::super::native_helpers::{NativeEventContext, enqueue_outbox_event_with_context};
#[cfg(feature = "http-client")]
use futures::StreamExt as _;

#[cfg(feature = "http-client")]
use super::config::{
    SIGNATURE_HEADER, STATUS_DEAD, STATUS_DELIVERED, TIMESTAMP_HEADER, TOPIC_DELIVERY_DEAD,
    TOPIC_DELIVERY_SUCCEEDED, TOPIC_WEBHOOK_DELIVERY_CDC, clamp_max_attempts, delivery_backoff,
    webhook_delivery_concurrency, webhook_delivery_timeout,
};
#[cfg(feature = "http-client")]
use super::model::{delivery_model, endpoint_model};
#[cfg(feature = "http-client")]
use super::security::{
    resolve_and_pin_target, sign_webhook_body_with_timestamp, webhook_event_matches_endpoint_scope,
};
#[cfg(feature = "http-client")]
use crate::metrics::MetricsRecorder;

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

/// Bounded retries for the delivery-journal write. Small and quick: the goal
/// is to survive a transient blip, not to hold the worker while a database is
/// down.
#[cfg(feature = "http-client")]
const JOURNAL_INSERT_MAX_ATTEMPTS: u32 = 3;

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
    // Hoisted so the retry below reuses the EXACT same statement.
    let sql = format!(
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
    );
    let result = sqlx::query(&sql)
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
        // This is NOT a cosmetic logging failure. The loader selects outbox events
        // that have no DELIVERED/DEAD delivery row, so an unwritten journal entry
        // means the very same webhook is POSTed again on the next pass, and the
        // next — an unbounded redelivery loop against a receiver that already got
        // the event, caused by our own bookkeeping.
        //
        // The POST has already happened and cannot be un-sent, so the only correct
        // response is to try harder to record it, then say plainly what will
        // happen if we still cannot.
        for attempt in 1..=JOURNAL_INSERT_MAX_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(100 * attempt as u64)).await;
            let retry = sqlx::query(&sql)
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
            if retry.is_ok() {
                tracing::warn!(
                    endpoint_id,
                    event_id,
                    attempt,
                    "webhook delivery journal insert succeeded on retry"
                );
                return;
            }
        }
        tracing::error!(
            endpoint_id,
            event_id,
            error = %err,
            "webhook delivery journal insert failed after retries; this event has ALREADY been delivered but is not recorded, so the worker will redeliver it on every subsequent pass until the row can be written"
        );
    }
}

/// Current unix time in whole seconds (saturating), the timestamp bound into the
/// delivery signature and emitted in the `X-Udb-Timestamp` header.
#[cfg(feature = "http-client")]
fn current_unix_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Run ONE delivery pass over supplied candidates. For each
/// candidate event and each endpoint whose BOUND tenant + subscription matches
/// ([`webhook_event_matches_endpoint_scope`]): re-validate AND PIN the target
/// against the SSRF guard at delivery time (defeats DNS rebinding at the connect
/// layer — [`resolve_and_pin_target`]), sign the timestamped body
/// ([`sign_webhook_body_with_timestamp`]), POST it (redirects DISABLED so a 3xx to
/// an internal host can never be followed past the guard; a bounded per-delivery
/// timeout so one black-holed endpoint cannot pin the worker) with retries/backoff,
/// journal the terminal outcome, and on attempt exhaustion emit the dead-letter
/// event (`udb.webhook.delivery.dead.v1`). Returns the count of events delivered
/// successfully. Best-effort per (event, endpoint): one failing target never
/// aborts the pass.
///
#[cfg(feature = "http-client")]
#[cfg_attr(test, allow(dead_code))]
#[allow(dead_code)]
pub(crate) async fn run_webhook_delivery_once(
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
            // One timestamp per delivery, bound into the signed material and sent
            // in the header so receivers can reject stale/replayed deliveries.
            let timestamp = current_unix_timestamp();
            let timestamp_header = timestamp.to_string();
            let signature =
                sign_webhook_body_with_timestamp(&endpoint.signing_secret, timestamp, &body);
            let max_attempts = clamp_max_attempts(endpoint.max_attempts).max(1) as u32;
            let mut last_status: i32 = 0;
            let mut last_error = String::new();
            let mut ok = false;
            let mut attempts_observed: i32 = 0;
            for attempt in 1..=max_attempts {
                attempts_observed = attempt as i32;
                // SSRF re-check AND address PIN at DELIVERY time: resolve+validate
                // once here, then connect to exactly those addresses so reqwest
                // never re-resolves the hostname (closing the DNS-rebinding TOCTOU).
                let target = match resolve_and_pin_target(&endpoint.url).await {
                    Ok(target) => target,
                    Err(err) => {
                        last_error = err.message().to_string();
                        break; // a blocked target never becomes deliverable on retry
                    }
                };
                // Per-request client pinned to the validated address(es), with
                // redirects DISABLED (a 302 to 169.254.169.254 / any internal host
                // is returned as a non-success status, never followed) and a
                // bounded timeout so a black-holed endpoint cannot pin the worker.
                //
                // TODO(leader-wire): the SHARED delivery client built in
                // `src/runtime/service/mod.rs` (`reqwest::Client::new()`) must also
                // be constructed as
                //   `reqwest::Client::builder()
                //        .redirect(reqwest::redirect::Policy::none())
                //        .timeout(webhook_service::webhook_delivery_timeout())
                //        .build()`
                // as defense-in-depth for any caller that still uses it; this
                // worker no longer relies on it (it pins per request below).
                let client = match reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(webhook_delivery_timeout())
                    .resolve_to_addrs(&target.host, &target.addrs)
                    .build()
                {
                    Ok(client) => client,
                    Err(err) => {
                        last_error = format!("webhook delivery client build failed: {err}");
                        break;
                    }
                };
                match client
                    .post(endpoint.url.as_str())
                    .header(SIGNATURE_HEADER, signature.as_str())
                    .header(TIMESTAMP_HEADER, timestamp_header.as_str())
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
///
/// `_http` is the process-shared client built in `serve()`; the worker no longer
/// uses it (each delivery builds a per-request client pinned to the SSRF-validated
/// address — see [`run_webhook_delivery_once`]). It is kept in the signature only
/// to avoid touching the shared `serve()` call site.
#[cfg(feature = "http-client")]
pub(crate) async fn run_webhook_delivery_worker_once(
    _http: &reqwest::Client,
    pool: &PgPool,
    outbox_relation: Option<&str>,
    journal_relation: &str,
    batch: i64,
    metrics: Option<&Arc<dyn MetricsRecorder>>,
    runtime: &crate::runtime::DataBrokerRuntime,
) -> Result<i64, String> {
    let mut jobs = load_webhook_delivery_jobs(pool, journal_relation, batch).await?;
    // NTF1: the loaded signing_secret is sealed at rest (see create_endpoint);
    // decrypt it to the plaintext HMAC key before signing. `decrypt_secret_at_rest`
    // returns a plaintext value unchanged, so a mixed plaintext/ciphertext rollout
    // (pre-fix rows + newly-sealed rows) both sign correctly.
    for job in &mut jobs {
        if let Ok(plain) = runtime.decrypt_secret_at_rest(&job.target.signing_secret) {
            job.target.signing_secret = plain;
        }
    }
    // Bounded-concurrency fan-out: each job is an independent (endpoint, event)
    // delivery, so one slow/black-holed endpoint (capped by the per-delivery
    // timeout) never head-of-line-blocks other tenants' deliveries in this tick.
    let concurrency = webhook_delivery_concurrency();
    // Materialize the per-job delivery futures eagerly so the bounded-concurrency
    // stream owns concrete futures. A lazy `Map` closure returning a future that
    // borrows its `&job` argument can't satisfy the `for<'a>` bound the
    // leader-spawned task requires ("FnOnce is not general enough").
    let deliveries: Vec<_> = jobs
        .iter()
        .map(|job| {
            run_webhook_delivery_once(
                pool,
                outbox_relation,
                std::slice::from_ref(&job.target),
                std::slice::from_ref(&job.event),
                metrics,
            )
        })
        .collect();
    let results = futures::stream::iter(deliveries)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    let mut delivered = 0u64;
    for result in results {
        delivered = delivered.saturating_add(result?);
    }
    Ok(i64::try_from(delivered).unwrap_or(i64::MAX))
}
