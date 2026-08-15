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
/// Default page size for the list handlers when a request omits `page.page_size`.
/// One const in place of the literal `50` that was duplicated across the six
/// notification/template/preference list read + response call sites.
pub(crate) const DEFAULT_PAGE_SIZE: i32 = 50;
pub(crate) const NOTIFICATION_DELIVERY_BATCH: i64 = 200;
pub(crate) const NOTIFICATION_DELIVERY_BATCH_ENV: &str = "UDB_NOTIFICATION_DELIVERY_BATCH";
pub(crate) const DEFAULT_NOTIFICATION_DELIVERY_INTERVAL_SECS: u64 = 30;
pub(crate) const NOTIFICATION_DELIVERY_INTERVAL_ENV: &str =
    "UDB_NOTIFICATION_DELIVERY_INTERVAL_SECS";
/// Bounded-retry ceiling (F7): once a delivery's stored `attempt_count` reaches
/// this value the worker moves the log to the terminal FAILED state, removing it
/// from the PENDING queue so a permanently-failing provider stops being re-POSTed
/// every interval. Default 6 (the middle of the 5–8 best-practice range).
pub(crate) const DEFAULT_NOTIFICATION_DELIVERY_MAX_ATTEMPTS: i64 = 6;
pub(crate) const NOTIFICATION_DELIVERY_MAX_ATTEMPTS_ENV: &str =
    "UDB_NOTIFICATION_DELIVERY_MAX_ATTEMPTS";
pub(crate) const NOTIFICATION_DELIVERY_PROVIDERS_ENV: &str =
    "UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON";
/// Per-POST HTTP client timeout for the delivery worker (bounds one hung provider
/// so it can't stall a whole sequential batch). Default 30s.
pub(crate) const DEFAULT_NOTIFICATION_DELIVERY_TIMEOUT_SECS: u64 = 30;
pub(crate) const NOTIFICATION_DELIVERY_TIMEOUT_ENV: &str = "UDB_NOTIFICATION_DELIVERY_TIMEOUT_SECS";
/// Exponential-backoff base (seconds): the auto-retry delay after the FIRST failed
/// delivery attempt (`attempt_count == 1`). Each further failed attempt doubles it,
/// up to the cap. Reads `UDB_NOTIFICATION_BACKOFF_BASE_SECS`, default
/// `DEFAULT_NOTIFICATION_BACKOFF_BASE_SECS` (5). Replaces the old fixed-interval
/// re-POST-every-30s retry storm.
pub(crate) const DEFAULT_NOTIFICATION_BACKOFF_BASE_SECS: i64 = 5;
pub(crate) const NOTIFICATION_BACKOFF_BASE_SECS_ENV: &str = "UDB_NOTIFICATION_BACKOFF_BASE_SECS";
/// Exponential-backoff ceiling (seconds): the auto-retry delay never grows past
/// this no matter how many attempts have failed (bounded backoff). Reads
/// `UDB_NOTIFICATION_BACKOFF_CAP_SECS`, default
/// `DEFAULT_NOTIFICATION_BACKOFF_CAP_SECS` (1800 = 30 min).
pub(crate) const DEFAULT_NOTIFICATION_BACKOFF_CAP_SECS: i64 = 1800;
pub(crate) const NOTIFICATION_BACKOFF_CAP_SECS_ENV: &str = "UDB_NOTIFICATION_BACKOFF_CAP_SECS";

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
/// Kafka topic for a notification intentionally withheld from delivery.
pub(crate) const NOTIFICATION_SUPPRESSED_TOPIC: &str = "udb.notification.suppressed.v1";

/// Dead-letter (DLQ) topic: emitted ONCE when a notification log crosses into the
/// terminal FAILED state after exhausting its bounded-retry budget (or on a
/// permanent failure such as a missing recipient). The terminal FAILED state IS the
/// dead-letter — no separate DLQ table — and this event is the entry notification so
/// operators can alert on DLQ depth. Distinct from the per-attempt
/// `udb.notification.delivery.failed.v1` (which fires on every failed attempt).
pub(crate) const TOPIC_NOTIFICATION_DEAD_LETTERED: &str =
    "udb.notification.delivery.dead_lettered.v1";

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

/// Pure positive-integer env parse shared by the delivery batch / max-attempts
/// knobs: a present, parseable, strictly-positive value wins; anything else
/// (absent / unparseable / `<= 0`) falls back to `default`. Mirrors the filter
/// shape of `notification_delivery_interval`. Pure + unit-tested.
pub(crate) fn parse_positive_i64(value: Option<&str>, default: i64) -> i64 {
    value
        .and_then(|raw| raw.trim().parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

/// Max delivery attempts before the worker transitions a still-failing log to
/// the terminal FAILED state (bounding the retry storm — F7). Reads
/// `UDB_NOTIFICATION_DELIVERY_MAX_ATTEMPTS`, default
/// `DEFAULT_NOTIFICATION_DELIVERY_MAX_ATTEMPTS` (6). Mirrors the
/// `notification_delivery_interval` resolve-once pattern.
pub(crate) fn max_delivery_attempts() -> i64 {
    static MAX_ATTEMPTS: OnceLock<i64> = OnceLock::new();
    *MAX_ATTEMPTS.get_or_init(|| {
        parse_positive_i64(
            std::env::var(NOTIFICATION_DELIVERY_MAX_ATTEMPTS_ENV)
                .ok()
                .as_deref(),
            DEFAULT_NOTIFICATION_DELIVERY_MAX_ATTEMPTS,
        )
    })
}

/// Whether a delivery has exhausted its bounded-retry budget: the stored attempt
/// row's `attempt_count` has reached `max_attempts`, so the worker moves the log
/// to terminal FAILED (dropping it from the PENDING queue) instead of leaving it
/// for another bounded auto-retry. Pure + unit-tested; `max_attempts <= 0` means
/// "never exhaust" (unbounded), never a divide/underflow.
pub(crate) fn delivery_exhausted_retries(attempt_count: i32, max_attempts: i64) -> bool {
    max_attempts > 0 && i64::from(attempt_count) >= max_attempts
}

/// Per-pass delivery batch size (the worker's `LIMIT`). Reads
/// `UDB_NOTIFICATION_DELIVERY_BATCH`, default `NOTIFICATION_DELIVERY_BATCH` (200).
/// Mirrors the `notification_delivery_interval` resolve-once pattern.
// TODO(leader-wire): re-export this from notification_service/mod.rs and swap the
// hardcoded `NOTIFICATION_DELIVERY_BATCH` spawn arg for `notification_delivery_batch()`
// at the notification delivery worker spawn in service/mod.rs. Unused until then
// (the spawn still passes the const), hence the allow.
#[allow(dead_code)]
pub(crate) fn notification_delivery_batch() -> i64 {
    static BATCH: OnceLock<i64> = OnceLock::new();
    *BATCH.get_or_init(|| {
        parse_positive_i64(
            std::env::var(NOTIFICATION_DELIVERY_BATCH_ENV)
                .ok()
                .as_deref(),
            NOTIFICATION_DELIVERY_BATCH,
        )
    })
}

/// Per-POST HTTP client timeout for the delivery worker. Reads
/// `UDB_NOTIFICATION_DELIVERY_TIMEOUT_SECS`, default
/// `DEFAULT_NOTIFICATION_DELIVERY_TIMEOUT_SECS` (30). Mirrors the
/// `notification_delivery_interval` resolve-once pattern.
// TODO(leader-wire): the reqwest client is built in `service/mod.rs`
// (`let http = reqwest::Client::new();` at the notification delivery worker spawn,
// ~line 2980, which THIS module must not edit); apply
// `.timeout(notification_delivery_timeout())` there so one hung provider can't
// stall a batch of intents processed sequentially. Re-exported from
// notification_service/mod.rs; unused until the leader wires it, hence the allow.
#[allow(dead_code)]
pub(crate) fn notification_delivery_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        Duration::from_secs(
            std::env::var(NOTIFICATION_DELIVERY_TIMEOUT_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(DEFAULT_NOTIFICATION_DELIVERY_TIMEOUT_SECS),
        )
    })
}

/// Exponential-backoff base (seconds) resolved once from
/// `UDB_NOTIFICATION_BACKOFF_BASE_SECS`, default
/// `DEFAULT_NOTIFICATION_BACKOFF_BASE_SECS` (5). Mirrors the
/// `notification_delivery_interval` resolve-once pattern.
pub(crate) fn notification_backoff_base_secs() -> i64 {
    static BASE: OnceLock<i64> = OnceLock::new();
    *BASE.get_or_init(|| {
        parse_positive_i64(
            std::env::var(NOTIFICATION_BACKOFF_BASE_SECS_ENV)
                .ok()
                .as_deref(),
            DEFAULT_NOTIFICATION_BACKOFF_BASE_SECS,
        )
    })
}

/// Exponential-backoff ceiling (seconds) resolved once from
/// `UDB_NOTIFICATION_BACKOFF_CAP_SECS`, default
/// `DEFAULT_NOTIFICATION_BACKOFF_CAP_SECS` (1800). Mirrors the
/// `notification_delivery_interval` resolve-once pattern.
pub(crate) fn notification_backoff_cap_secs() -> i64 {
    static CAP: OnceLock<i64> = OnceLock::new();
    *CAP.get_or_init(|| {
        parse_positive_i64(
            std::env::var(NOTIFICATION_BACKOFF_CAP_SECS_ENV)
                .ok()
                .as_deref(),
            DEFAULT_NOTIFICATION_BACKOFF_CAP_SECS,
        )
    })
}

/// Pure exponential backoff for a failed delivery's next auto-retry:
/// `min(cap, base * 2^(attempt_count - 1))` for `attempt_count >= 1`. Bounded (never
/// exceeds `cap`) and overflow-safe — the `(attempt_count - 1)` exponent is clamped
/// to `[0, 32]` so `2^exp` can't overflow `i64`, and the multiply saturates. An
/// `attempt_count <= 1` yields `base` (the exponent floors at 0). Jitter is applied
/// at the SQL comparison site, not here, so this stays deterministic + unit-tested.
/// The loader's SQL replicates this exact shape via
/// `base * power(2, LEAST(attempt_count - 1, 12))` capped by `cap`; this pure
/// version is the unit-test oracle for that shape, hence unused in non-test builds.
#[allow(dead_code)]
pub(crate) fn next_retry_delay_secs(attempt_count: i64, base: i64, cap: i64) -> i64 {
    let base = base.max(0);
    let cap = cap.max(0);
    let exp = attempt_count.saturating_sub(1).clamp(0, 32) as u32;
    let factor = 1_i64.checked_shl(exp).unwrap_or(i64::MAX);
    base.saturating_mul(factor).min(cap)
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

/// Compatibility topic declared by the older public notification event
/// messages. The delivery worker later introduced the more precise
/// `udb.notification.delivery.<status>.v1` family without migrating those two
/// consumer contracts. Publish both while the v1 messages remain supported so
/// an existing customer group cannot silently miss a delivery outcome.
pub(crate) fn legacy_delivery_event_topic(status_db: &str) -> Option<&'static str> {
    match status_db {
        "FAILED" => Some("udb.notification.failed.v1"),
        "DELIVERED" => Some("udb.notification.delivered.v1"),
        _ => None,
    }
}

#[cfg(test)]
mod config_tests {
    use super::{
        DEFAULT_NOTIFICATION_DELIVERY_MAX_ATTEMPTS, NOTIFICATION_DELIVERY_BATCH,
        delivery_exhausted_retries, next_retry_delay_secs, parse_positive_i64,
    };

    #[test]
    fn parse_positive_i64_falls_back_on_absent_or_non_positive() {
        // Absent / unparseable / non-positive all take the default.
        assert_eq!(parse_positive_i64(None, 200), 200);
        assert_eq!(parse_positive_i64(Some("not-a-number"), 200), 200);
        assert_eq!(parse_positive_i64(Some("0"), 200), 200);
        assert_eq!(parse_positive_i64(Some("-5"), 200), 200);
        assert_eq!(parse_positive_i64(Some("   "), 200), 200);
    }

    #[test]
    fn parse_positive_i64_uses_a_valid_positive_override() {
        assert_eq!(
            parse_positive_i64(Some("500"), NOTIFICATION_DELIVERY_BATCH),
            500
        );
        assert_eq!(parse_positive_i64(Some("  9  "), 6), 9);
    }

    #[test]
    fn delivery_exhausted_retries_is_true_at_or_above_the_ceiling() {
        let max = DEFAULT_NOTIFICATION_DELIVERY_MAX_ATTEMPTS; // 6
        // Below the ceiling: keep retrying (stays PENDING).
        assert!(!delivery_exhausted_retries(1, max));
        assert!(!delivery_exhausted_retries(5, max));
        // At and above the ceiling: exhaust → terminal FAILED.
        assert!(delivery_exhausted_retries(6, max));
        assert!(delivery_exhausted_retries(7, max));
    }

    #[test]
    fn delivery_exhausted_retries_treats_non_positive_max_as_unbounded() {
        assert!(!delivery_exhausted_retries(1_000, 0));
        assert!(!delivery_exhausted_retries(1_000, -1));
    }

    #[test]
    fn next_retry_delay_attempt_one_is_the_base() {
        // attempt_count == 1 → base * 2^0 == base (the delay after the first failure).
        assert_eq!(next_retry_delay_secs(1, 5, 1800), 5);
        assert_eq!(next_retry_delay_secs(1, 8, 1800), 8);
        // attempt_count <= 1 floors the exponent at 0 (never negative / underflow).
        assert_eq!(next_retry_delay_secs(0, 5, 1800), 5);
        assert_eq!(next_retry_delay_secs(-4, 5, 1800), 5);
    }

    #[test]
    fn next_retry_delay_grows_exponentially() {
        let (base, cap) = (5, 1800);
        assert_eq!(next_retry_delay_secs(2, base, cap), 10); // 5 * 2^1
        assert_eq!(next_retry_delay_secs(3, base, cap), 20); // 5 * 2^2
        assert_eq!(next_retry_delay_secs(4, base, cap), 40); // 5 * 2^3
        assert_eq!(next_retry_delay_secs(9, base, cap), 1280); // 5 * 2^8 (< cap)
    }

    #[test]
    fn next_retry_delay_is_bounded_by_the_cap() {
        let (base, cap) = (5, 1800);
        // 5 * 2^9 == 2560 > cap → clamps to the ceiling.
        assert_eq!(next_retry_delay_secs(10, base, cap), 1800);
        // A far-out attempt count stays capped and never overflows.
        assert_eq!(next_retry_delay_secs(1_000, base, cap), 1800);
        assert_eq!(next_retry_delay_secs(i64::MAX, base, cap), 1800);
        // A cap below the base still clamps to the cap (min semantics hold).
        assert_eq!(next_retry_delay_secs(1, 100, 30), 30);
    }
}
