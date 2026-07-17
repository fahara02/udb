//! Statics and resolve-once knobs for the native `NotificationService`: entity
//! message ids, the delivery batch/interval + provider-config env keys, the stable
//! machine-readable error reasons, the test-only sentinel/gate, the versioned
//! sent/delivery dot-topics, and the pure derivations over them
//! (`notification_delivery_interval`, `delivery_event_topic`). Extracted verbatim
//! from the former god file.

use std::sync::OnceLock;
use std::time::Duration;

pub(crate) const LOG_MSG: &str = "udb.core.notification.entity.v1.NotificationLog";
pub(crate) const TEMPLATE_MSG: &str = "udb.core.notification.entity.v1.NotificationTemplate";
pub(crate) const PREFERENCE_MSG: &str = "udb.core.notification.entity.v1.NotificationPreference";
/// Native message id for the master-plan 9.13 delivery-status record.
pub(crate) const DELIVERY_ATTEMPT_MSG: &str =
    "udb.core.notification.entity.v1.NotificationDeliveryAttempt";
pub(crate) const TEMPLATE_LOCALE_MAX_CHARS: usize = 10;
pub(crate) const NOTIFICATION_DELIVERY_BATCH: i64 = 200;
pub(crate) const DEFAULT_NOTIFICATION_DELIVERY_INTERVAL_SECS: u64 = 30;
pub(crate) const NOTIFICATION_DELIVERY_INTERVAL_ENV: &str =
    "UDB_NOTIFICATION_DELIVERY_INTERVAL_SECS";
pub(crate) const NOTIFICATION_DELIVERY_PROVIDERS_ENV: &str =
    "UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON";

// Stable machine-readable error reasons (google.rpc.ErrorInfo-style `reason`),
// attached to the returned `Status` metadata under `error-reason` so SDK clients
// can branch on a fixed code instead of free-text messages. Only reasons with a
// LIVE failure path are fired here. `TEMPLATE_INACTIVE`/`PROVIDER_UNAVAILABLE`
// have no reachable site and are intentionally NOT defined.
/// No template matched the (event_type, channel, locale, tenant) scope on send.
pub(crate) const TEMPLATE_NOT_FOUND: &str = "TEMPLATE_NOT_FOUND";
/// A `{{placeholder}}` in the matched template had no value in `req.variables`.
pub(crate) const VARIABLE_MISSING: &str = "VARIABLE_MISSING";
/// Reserved: recipient opted out of the (channel, event) — currently handled as a
/// SUPPRESSED log row, not an error, so no live site fires this reason yet.
#[allow(dead_code)]
pub(crate) const RECIPIENT_OPTED_OUT: &str = "RECIPIENT_OPTED_OUT";
/// Retry was requested for a notification that is not in a retryable state.
pub(crate) const NOT_RETRYABLE_STATE: &str = "NOT_RETRYABLE_STATE";

/// Test-only sentinel (TODO 04.4.2.2): when `test_mode_enabled()` is true AND a
/// `SendNotification` request carries this exact value in `resource_type`, each
/// per-channel log is written as FAILED so `retry_notification` has a real
/// retryable row to exercise. Unreachable in production: the env gate defaults
/// false, so production sends ignore this value entirely.
pub(crate) const TEST_FORCE_FAILED_SENTINEL: &str = "__perf_force_failed__";

/// Kafka topic for the "notification sent" domain event.
pub(crate) const NOTIFICATION_SENT_TOPIC: &str = "udb.notification.sent.v1";

/// Whether the notification test harness is enabled (resolved once). Fail-closed
/// default: production never sets `UDB_NOTIFICATION_TEST_MODE`, so the test-only
/// forced-FAILED send path below is unreachable in production. Mirrors the exact
/// shape of `auth_service::authn::webauthn_softauth::test_mode_enabled`.
pub(crate) fn test_mode_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("UDB_NOTIFICATION_TEST_MODE")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false)
    })
}

pub(crate) fn notification_delivery_interval() -> Duration {
    static INTERVAL: OnceLock<Duration> = OnceLock::new();
    *INTERVAL.get_or_init(|| {
        Duration::from_secs(
            std::env::var(NOTIFICATION_DELIVERY_INTERVAL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_NOTIFICATION_DELIVERY_INTERVAL_SECS),
        )
    })
}

/// The versioned dot-topic a terminal delivery outcome is published on
/// (master-plan 9.13): `udb.notification.delivery.<status>.v1`, e.g.
/// `udb.notification.delivery.delivered.v1`. Pure + unit-tested.
pub(crate) fn delivery_event_topic(status_db: &str) -> String {
    format!(
        "udb.notification.delivery.{}.v1",
        status_db.to_ascii_lowercase()
    )
}
