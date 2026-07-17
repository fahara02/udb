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

pub(crate) fn clamp_max_attempts(requested: i32) -> i32 {
    if requested <= 0 {
        DEFAULT_MAX_ATTEMPTS
    } else {
        requested.min(MAX_MAX_ATTEMPTS)
    }
}
