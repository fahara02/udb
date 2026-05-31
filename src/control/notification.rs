// src/notification.rs — Webhook notification types and config.
//
// When a migration run completes, fails, or is blocked, the Go UDB service
// POSTs a `WebhookPayload` to the configured `notification_url`.
//
// This module defines the data types and helpers used by both the Go service
// and the Rust CLI tool to reason about notification configuration.
//
// Aligned with:
//   - legacy_sql  legacycore/db/migrate_hooks.go  (NotificationEvent, WebhookPayload,
//                                              SendNotification)
//   - UDB spec §16.3.1                        (FSM Lifecycle → completion hooks)

use serde::{Deserialize, Serialize};

// ── Event kinds ───────────────────────────────────────────────────────────────

/// The type of migration lifecycle event that triggers a webhook call.
/// Mirrors `NotificationEvent` constants in legacy_sql `migrate_hooks.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NotificationEvent {
    /// The migration run completed successfully.
    Completed,
    /// The migration run failed with an unrecoverable error.
    Failed,
    /// The diff contained blocked (non-auto-applicable) operations.
    Blocked,
}

impl NotificationEvent {
    /// Returns the canonical lowercase string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
        }
    }

    /// Parse from a string; returns `None` for unrecognised values.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "blocked" => Some(Self::Blocked),
            _ => None,
        }
    }
}

impl std::fmt::Display for NotificationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ── Webhook payload ───────────────────────────────────────────────────────────

/// JSON body POSTed to `notification_url` on a lifecycle event.
/// Mirrors `WebhookPayload` in legacy_sql `migrate_hooks.go`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WebhookPayload {
    /// Event kind: `"completed"`, `"failed"`, `"blocked"`.
    pub event: String,
    /// Run ID of the migration that produced the event.
    pub run_id: String,
    /// RFC 3339 timestamp when the event was produced.
    pub timestamp: String,
    /// Human-readable summary message.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Error message (only present on `"failed"` events).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// Target PostgreSQL schema (if the event is schema-specific).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub schema: String,
    /// SQL file being applied when the error occurred (if applicable).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub file: String,
    /// Number of blocked operations (only present on `"blocked"` events).
    #[serde(skip_serializing_if = "is_zero")]
    pub blocked_count: usize,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

// ── Notification config ───────────────────────────────────────────────────────

/// Resolved notification configuration for one migration run.
/// Built from `MigrationOptions.notification_url` + `notification_on`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NotificationConfig {
    /// HTTP(S) webhook endpoint.  Empty string disables notifications entirely.
    pub url: String,
    /// Events for which a notification is sent.
    pub events: Vec<NotificationEvent>,
}

impl NotificationConfig {
    /// Build a `NotificationConfig` from raw config strings.
    ///
    /// `notification_on` is a list of event strings; unrecognised values are silently ignored.
    pub fn new(url: impl Into<String>, notification_on: &[String]) -> Self {
        let events = notification_on
            .iter()
            .filter_map(|s| NotificationEvent::from_str(s))
            .collect();
        Self {
            url: url.into(),
            events,
        }
    }

    /// Returns `true` when the given event should trigger a notification.
    pub fn wants_event(&self, event: &NotificationEvent) -> bool {
        !self.url.is_empty() && self.events.contains(event)
    }

    /// Build a "completed" payload.
    pub fn completed_payload(&self, run_id: &str) -> WebhookPayload {
        WebhookPayload {
            event: NotificationEvent::Completed.as_str().to_string(),
            run_id: run_id.to_string(),
            timestamp: rfc3339_now(),
            message: "Migration run completed successfully.".to_string(),
            ..WebhookPayload::default()
        }
    }

    /// Build a "failed" payload.
    pub fn failed_payload(&self, run_id: &str, error: &str, file: &str) -> WebhookPayload {
        WebhookPayload {
            event: NotificationEvent::Failed.as_str().to_string(),
            run_id: run_id.to_string(),
            timestamp: rfc3339_now(),
            message: "Migration run failed.".to_string(),
            error: error.to_string(),
            file: file.to_string(),
            ..WebhookPayload::default()
        }
    }

    /// Build a "blocked" payload.
    pub fn blocked_payload(&self, run_id: &str, count: usize) -> WebhookPayload {
        WebhookPayload {
            event: NotificationEvent::Blocked.as_str().to_string(),
            run_id: run_id.to_string(),
            timestamp: rfc3339_now(),
            message: format!("{count} blocked operation(s) require manual intervention."),
            blocked_count: count,
            ..WebhookPayload::default()
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn rfc3339_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    // Minimal RFC 3339 UTC timestamp — the Go service uses time.Now().UTC().Format(time.RFC3339).
    let (y, mo, d, h, mi, s) = epoch_to_utc(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

/// Converts a Unix timestamp to (year, month, day, hour, minute, second) in UTC.
/// Minimal implementation that avoids pulling in a time crate.
fn epoch_to_utc(secs: u64) -> (u64, u64, u64, u64, u64, u64) {
    let s = secs % 60;
    let mins = secs / 60;
    let mi = mins % 60;
    let hours = mins / 60;
    let h = hours % 24;
    let days = hours / 24;

    // Gregorian calendar approximation from days since epoch (1970-01-01).
    let mut y = 1970u64;
    let mut remaining = days;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }
    let month_days: &[u64] = if is_leap(y) {
        &[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        &[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut mo = 1u64;
    for &md in month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        mo += 1;
    }
    (y, mo, remaining + 1, h, mi, s)
}

fn is_leap(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notification_event_round_trip() {
        for (s, expected) in [
            ("completed", NotificationEvent::Completed),
            ("failed", NotificationEvent::Failed),
            ("blocked", NotificationEvent::Blocked),
        ] {
            assert_eq!(NotificationEvent::from_str(s).unwrap(), expected);
            assert_eq!(expected.as_str(), s);
        }
    }

    #[test]
    fn notification_config_wants_event() {
        let cfg = NotificationConfig::new(
            "https://hooks.example.com/xyz",
            &["completed".to_string(), "failed".to_string()],
        );
        assert!(cfg.wants_event(&NotificationEvent::Completed));
        assert!(cfg.wants_event(&NotificationEvent::Failed));
        assert!(!cfg.wants_event(&NotificationEvent::Blocked));
    }

    #[test]
    fn notification_config_empty_url_returns_false() {
        let cfg = NotificationConfig::new("", &["completed".to_string()]);
        assert!(!cfg.wants_event(&NotificationEvent::Completed));
    }

    #[test]
    fn completed_payload_has_correct_event() {
        let cfg = NotificationConfig::new("https://example.com", &[]);
        let p = cfg.completed_payload("run-001");
        assert_eq!(p.event, "completed");
        assert_eq!(p.run_id, "run-001");
    }

    #[test]
    fn blocked_payload_has_count() {
        let cfg = NotificationConfig::new("https://example.com", &[]);
        let p = cfg.blocked_payload("run-002", 3);
        assert_eq!(p.event, "blocked");
        assert_eq!(p.blocked_count, 3);
    }
}
