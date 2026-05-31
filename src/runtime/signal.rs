// src/signal.rs — Signal / analytics DB types.
//
// The signal DB is an optional third database connection (distinct from the
// primary and backup) used for high-volume analytics event writes that must not
// compete with primary application traffic.
//
// Typical pattern (Go UDB service):
//   if let Some(sig) = manager.signal() {
//       udb.write_analytics_event(sig, event).await?;
//   } else {
//       udb.write_analytics_event(manager.primary(), event).await?;
//   }
//
// The Rust library exposes the event struct and config types so the CLI and
// codegen tools can reason about analytics routing without a live DB connection.
//
// Aligned with:
//   - legacy_sql  legacycore/db/signal.go   (AnalyticsEvent, WriteAnalyticsEvent,
//                                        SignalHealth, LogSignalStatus)
//   - UDB spec §16.4                    (Unified CRUD Operations)

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ── Analytics event ───────────────────────────────────────────────────────────

/// A structured analytics event written to the signal DB (or primary fallback).
///
/// The Go UDB service inserts rows into `analytics.events_daily` using
/// `INSERT … ON CONFLICT (event_id) DO NOTHING` so duplicate IDs are idempotent.
/// Mirrors `AnalyticsEvent` in legacy_sql `signal.go`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct AnalyticsEvent {
    /// Globally unique event identifier (UUID v4 recommended).
    pub event_id: String,
    /// Property / site ID — identifies the originating application instance.
    pub property_id: String,
    /// Tenant or company identifier for multi-tenant deployments.
    pub tenant_id: String,
    /// Domain event name in `<domain>.<action>.v1` format,
    /// e.g. `"document.processed.v1"`.
    pub event_name: String,
    /// Source system: `"internal"`, `"webhook"`, `"grpc"`, `"kafka"`.
    pub event_source: String,
    /// Arbitrary string dimensions (searchable categorical attributes).
    pub dimensions: BTreeMap<String, String>,
    /// Arbitrary numeric metrics (e.g. `"confidence": 0.97`).
    pub metrics: BTreeMap<String, f64>,
    /// Unix timestamp when the business event occurred.
    pub occurred_at_unix: u64,
    /// Unix timestamp when the event was received by the UDB.
    pub received_at_unix: u64,
}

impl AnalyticsEvent {
    /// Returns `true` when the event has the minimum required fields.
    pub fn is_valid(&self) -> bool {
        !self.event_id.is_empty() && !self.event_name.is_empty()
    }

    /// Returns a validation error message, or `None` when the event is valid.
    pub fn validate(&self) -> Option<String> {
        if self.event_id.is_empty() {
            return Some("event_id is required".to_string());
        }
        if self.event_name.is_empty() {
            return Some("event_name is required".to_string());
        }
        None
    }
}

// ── DDL for analytics.events_daily ───────────────────────────────────────────

/// DDL for the `analytics.events_daily` table in the signal DB.
///
/// This table receives `AnalyticsEvent` rows from the UDB service.
/// Partitioned by day for efficient time-range queries and retention management.
pub const DDL_ANALYTICS_EVENTS_DAILY: &str = r#"
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events_daily (
    event_id      TEXT        NOT NULL,
    property_id   TEXT        NOT NULL DEFAULT '',
    tenant_id     TEXT        NOT NULL DEFAULT '',
    event_name    TEXT        NOT NULL,
    event_source  TEXT        NOT NULL DEFAULT 'internal',
    dimensions    JSONB       NULL,
    metrics       JSONB       NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_events_daily PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- Default partition for any day not yet explicitly partitioned.
CREATE TABLE IF NOT EXISTS analytics.events_daily_default
    PARTITION OF analytics.events_daily DEFAULT;

CREATE INDEX IF NOT EXISTS idx_events_daily_event_name
    ON analytics.events_daily (event_name, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_daily_tenant
    ON analytics.events_daily (tenant_id, occurred_at DESC);
"#;

// ── Signal config ─────────────────────────────────────────────────────────────

/// Configuration for the signal / analytics DB routing.
///
/// The Go UDB service reads this from `UdbConfig.signal` and uses it to decide
/// whether analytics writes should go to the signal DB or fall back to primary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SignalConfig {
    /// Timeout for analytics query context in seconds.  Default: 30.
    pub query_timeout_secs: u64,
    /// Maximum batch size for bulk analytics inserts.  Default: 500.
    pub batch_size: usize,
    /// Whether to fall back to primary DB when signal DB is unavailable.
    /// Default: `true`.
    pub fallback_to_primary: bool,
}

impl SignalConfig {
    /// Returns the configured query timeout, or the default of 30 seconds.
    pub fn effective_query_timeout_secs(&self) -> u64 {
        if self.query_timeout_secs > 0 {
            self.query_timeout_secs
        } else {
            30
        }
    }

    /// Returns the effective batch size with fallback to 500.
    pub fn effective_batch_size(&self) -> usize {
        if self.batch_size > 0 {
            self.batch_size
        } else {
            500
        }
    }
}

// ── Generic analytics event names ────────────────────────────────────────────

/// Optional, generic event-name helpers. Applications may ignore these and use
/// their own `<domain>.<action>.vN` taxonomy.
pub mod common_events {
    /// A document-like work item was submitted.
    pub const DOCUMENT_SUBMITTED: &str = "document.submitted.v1";
    /// A document-like work item was processed successfully.
    pub const DOCUMENT_PROCESSED: &str = "document.processed.v1";
    /// A document-like work item failed processing.
    pub const DOCUMENT_FAILED: &str = "document.failed.v1";
    /// A field was corrected by a human reviewer.
    pub const FIELD_CORRECTED: &str = "field.corrected.v1";
    /// A form template was published.
    pub const TEMPLATE_PUBLISHED: &str = "template.published.v1";
    /// A learning cycle completed.
    pub const LEARNING_CYCLE_COMPLETED: &str = "learning.cycle_completed.v1";
    /// A migration run completed successfully.
    pub const MIGRATION_COMPLETED: &str = "udb.migration.completed.v1";
    /// A migration run failed.
    pub const MIGRATION_FAILED: &str = "udb.migration.failed.v1";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analytics_event_validate_empty_id() {
        let evt = AnalyticsEvent::default();
        assert!(evt.validate().is_some());
    }

    #[test]
    fn analytics_event_validate_valid() {
        let evt = AnalyticsEvent {
            event_id: "evt-001".to_string(),
            event_name: common_events::DOCUMENT_PROCESSED.to_string(),
            ..Default::default()
        };
        assert!(evt.validate().is_none());
        assert!(evt.is_valid());
    }

    #[test]
    fn signal_config_defaults() {
        let cfg = SignalConfig::default();
        assert_eq!(cfg.effective_query_timeout_secs(), 30);
        assert_eq!(cfg.effective_batch_size(), 500);
    }

    #[test]
    fn ddl_analytics_events_daily_nonempty() {
        assert!(!DDL_ANALYTICS_EVENTS_DAILY.trim().is_empty());
        assert!(DDL_ANALYTICS_EVENTS_DAILY.contains("analytics.events_daily"));
    }

    #[test]
    fn common_event_names_are_versioned() {
        for name in [
            common_events::DOCUMENT_SUBMITTED,
            common_events::DOCUMENT_PROCESSED,
            common_events::DOCUMENT_FAILED,
            common_events::FIELD_CORRECTED,
            common_events::TEMPLATE_PUBLISHED,
            common_events::MIGRATION_COMPLETED,
            common_events::MIGRATION_FAILED,
        ] {
            assert!(
                name.ends_with(".v1"),
                "event name '{name}' must end with .v1"
            );
        }
    }
}
