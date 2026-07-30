//! Statics and resolve-once knobs for the native `WebhookService`: entity message
//! ids, versioned outbox topics, delivery statuses, the attempt/backoff budget,
//! and the pure derivations over them (`clamp_max_attempts`, `delivery_backoff`,
//! `webhook_delivery_interval`). Extracted verbatim from the former god file.

use std::time::Duration;

pub(crate) const ENDPOINT_MSG: &str = "udb.core.webhook.entity.v1.WebhookEndpoint";
pub(crate) const DELIVERY_MSG: &str = "udb.core.webhook.entity.v1.WebhookDelivery";

// ── outbox topics ─────────────────────────────────────────────────────────────
pub(crate) const TOPIC_ENDPOINT_CREATED: &str = "udb.webhook.endpoint.created.v1";
pub(crate) const TOPIC_ENDPOINT_UPDATED: &str = "udb.webhook.endpoint.updated.v1";
pub(crate) const TOPIC_ENDPOINT_DELETED: &str = "udb.webhook.endpoint.deleted.v1";
/// Successful external delivery (one per delivered event/endpoint).
pub(crate) const TOPIC_DELIVERY_SUCCEEDED: &str = "udb.webhook.delivery.succeeded.v1";
/// Dead-letter: a delivery exhausted `max_attempts` and was abandoned.
pub(crate) const TOPIC_DELIVERY_DEAD: &str = "udb.webhook.delivery.dead.v1";

/// The signature header the broker stamps every delivery with.
pub(crate) const SIGNATURE_HEADER: &str = "x-udb-signature";
/// The delivery timestamp (unix seconds) header stamped alongside the signature.
/// The signature covers `"<timestamp>.<body>"`, so a receiver rejects a stale or
/// replayed delivery by bounding `now - timestamp` before it trusts the payload.
pub(crate) const TIMESTAMP_HEADER: &str = "x-udb-timestamp";

/// Default delivery attempt budget when an endpoint declares none.
pub(crate) const DEFAULT_MAX_ATTEMPTS: i32 = 5;
/// Hard ceiling so a misconfigured endpoint can't pin the worker forever.
pub(crate) const MAX_MAX_ATTEMPTS: i32 = 20;
/// Base backoff between delivery attempts; doubled per attempt up to the cap.
pub(crate) const DELIVERY_BACKOFF_BASE: Duration = Duration::from_secs(2);
pub(crate) const DELIVERY_BACKOFF_CAP: Duration = Duration::from_secs(300);
pub(crate) const WEBHOOK_DELIVERY_BATCH: i64 = 200;
pub(crate) const DEFAULT_WEBHOOK_DELIVERY_INTERVAL_SECS: u64 = 30;
pub(crate) const WEBHOOK_DELIVERY_INTERVAL_ENV: &str = "UDB_WEBHOOK_DELIVERY_INTERVAL_SECS";

// ── per-delivery request timeout (bounds head-of-line blocking) ────────────────
/// Default hard timeout for a single delivery POST. Without it, one black-holed
/// endpoint can pin the worker (and — before bounded concurrency — starve every
/// other tenant's deliveries in the tick). Bounded to a sane floor/ceiling.
pub(crate) const DEFAULT_WEBHOOK_DELIVERY_TIMEOUT_MS: u64 = 10_000;
pub(crate) const MIN_WEBHOOK_DELIVERY_TIMEOUT_MS: u64 = 1_000;
pub(crate) const MAX_WEBHOOK_DELIVERY_TIMEOUT_MS: u64 = 120_000;
pub(crate) const WEBHOOK_DELIVERY_TIMEOUT_ENV: &str = "UDB_WEBHOOK_DELIVERY_TIMEOUT_MS";

// ── bounded per-tick delivery concurrency (per-tenant fairness) ────────────────
/// Independent `(endpoint, event)` deliveries run concurrently up to this bound so
/// a slow/black-holed endpoint for one tenant (already capped by the per-delivery
/// timeout) never head-of-line-blocks other tenants' deliveries in the same tick.
pub(crate) const DEFAULT_WEBHOOK_DELIVERY_CONCURRENCY: usize = 8;
pub(crate) const MAX_WEBHOOK_DELIVERY_CONCURRENCY: usize = 64;
pub(crate) const WEBHOOK_DELIVERY_CONCURRENCY_ENV: &str = "UDB_WEBHOOK_DELIVERY_CONCURRENCY";

// ── delivery statuses (stored as VARCHAR) ─────────────────────────────────────
pub(crate) const STATUS_DELIVERED: &str = "DELIVERED";
pub(crate) const STATUS_DEAD: &str = "DEAD";
pub(crate) const TOPIC_WEBHOOK_DELIVERY_CDC: &str = "udb.webhook.webhook_deliveries.cdc";

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

/// Resolve-once per-delivery request timeout, clamped to `[MIN, MAX]` so a bad
/// env value can neither disable the timeout nor set an absurd one.
pub(crate) fn webhook_delivery_timeout() -> Duration {
    let ms = std::env::var(WEBHOOK_DELIVERY_TIMEOUT_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_WEBHOOK_DELIVERY_TIMEOUT_MS)
        .clamp(
            MIN_WEBHOOK_DELIVERY_TIMEOUT_MS,
            MAX_WEBHOOK_DELIVERY_TIMEOUT_MS,
        );
    Duration::from_millis(ms)
}

/// Resolve-once bounded delivery fan-out width (at least 1, capped).
pub(crate) fn webhook_delivery_concurrency() -> usize {
    std::env::var(WEBHOOK_DELIVERY_CONCURRENCY_ENV)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_WEBHOOK_DELIVERY_CONCURRENCY)
        .clamp(1, MAX_WEBHOOK_DELIVERY_CONCURRENCY)
}

pub(crate) fn clamp_max_attempts(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        requested.min(MAX_MAX_ATTEMPTS)
    }
}
