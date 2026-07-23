//! Data-plane audit emitter (F-1).
//!
//! `build_audit_event` produced an `AuditEvent` that NOTHING consumed — its only
//! caller was a test — so `AuditSinkConfig` (stdout/file/kafka/postgres) was
//! fully configured and production-validated while every data-plane mutation went
//! unaudited on every configuration. This wires the emitter: a successful
//! mutation now produces a structured JSON audit line to the configured sink.
//!
//! Stdout and File are implemented and unit-tested here (no external infra). The
//! Kafka and Postgres sinks are not yet wired to their transports; rather than
//! silently drop their events (the very lie this fixes), they fall back to stdout
//! with a one-time warning, so events are never lost silently.

use std::io::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::planning::broker::AuditEvent;
use crate::runtime::config::{AuditSinkConfig, AuditSinkKind};

/// Serialize an audit event as a single JSON line (newline-terminated).
pub(crate) fn audit_event_line(event: &AuditEvent) -> String {
    let value = serde_json::json!({
        "event_type": event.event_type,
        "tenant_id": event.tenant_id,
        "user_id": event.user_id,
        "correlation_id": event.correlation_id,
        "purpose": event.purpose,
        "resource_uri": event.resource_uri,
        "checksum_sha256": event.checksum_sha256,
    });
    // `to_string` on a serde_json::Value never fails; append exactly one newline.
    format!("{value}\n")
}

static UNWIRED_SINK_WARNED: AtomicBool = AtomicBool::new(false);

/// Emit `event` to the configured audit sink. Best-effort by design: a mutation
/// has already committed, so an audit-transport hiccup logs and is dropped rather
/// than failing the write (the write itself is journaled via CDC/outbox). `None`
/// is a no-op.
pub(crate) fn emit_audit(config: &AuditSinkConfig, event: &AuditEvent) {
    match config.kind {
        AuditSinkKind::None => {}
        AuditSinkKind::Stdout => {
            print!("{}", audit_event_line(event));
        }
        AuditSinkKind::File => {
            let Some(path) = config.file_path.as_deref().filter(|p| !p.trim().is_empty()) else {
                tracing::warn!(
                    "audit sink kind=file but UDB_AUDIT_FILE_PATH is unset; dropping event"
                );
                return;
            };
            if let Err(err) = append_line(path, &audit_event_line(event)) {
                tracing::warn!(path = %path, error = %err, "audit file append failed");
            }
        }
        // Not yet wired to their transports — fall back to stdout (never silently
        // drop) and warn once so an operator notices the configuration gap.
        AuditSinkKind::Kafka | AuditSinkKind::Postgres => {
            if !UNWIRED_SINK_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "audit sink kind={:?} is configured but its transport is not yet wired; \
                     audit events are being written to stdout as a fallback",
                    config.kind
                );
            }
            print!("{}", audit_event_line(event));
        }
    }
}

fn append_line(path: &str, line: &str) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(line.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AuditEvent {
        AuditEvent {
            event_type: "upsert".to_string(),
            tenant_id: "tenant-a".to_string(),
            user_id: "user-1".to_string(),
            correlation_id: "corr-1".to_string(),
            purpose: "billing".to_string(),
            resource_uri: "udb://tenant-a/acme.Order/o-9".to_string(),
            checksum_sha256: "sha256:abc".to_string(),
        }
    }

    #[test]
    fn audit_line_is_one_json_line_with_all_fields() {
        let line = audit_event_line(&sample());
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches('\n').count(), 1);
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).expect("valid json");
        assert_eq!(parsed["event_type"], "upsert");
        assert_eq!(parsed["tenant_id"], "tenant-a");
        assert_eq!(parsed["resource_uri"], "udb://tenant-a/acme.Order/o-9");
        assert_eq!(parsed["checksum_sha256"], "sha256:abc");
    }

    #[test]
    fn file_sink_appends_a_line_per_event() {
        let dir = std::env::temp_dir().join(format!("udb-audit-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("audit.log");
        let cfg = AuditSinkConfig {
            kind: AuditSinkKind::File,
            file_path: Some(path.to_string_lossy().to_string()),
            ..Default::default()
        };
        emit_audit(&cfg, &sample());
        emit_audit(&cfg, &sample());
        let contents = std::fs::read_to_string(&path).expect("read audit file");
        assert_eq!(contents.lines().count(), 2, "one line per event");
        assert!(contents.contains("\"event_type\":\"upsert\""));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn none_sink_is_a_noop() {
        // No panic, no file — the default backward-compatible behavior.
        emit_audit(&AuditSinkConfig::default(), &sample());
    }
}
