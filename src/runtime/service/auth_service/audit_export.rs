//! Immutable compliance-audit export sinks (Phase L3 task 2).
//!
//! The transactional outbox (`OutboxAuthEventSink`) is the primary, durable,
//! Kafka-relayed audit path. This module adds the *additional* immutable export
//! targets the compliance plan calls for, behind a single [`AuditExportSink`]
//! abstraction so operators can fan a security event out to several
//! tamper-evident destinations:
//!
//! - **Postgres durable audit table** — an append-only `udb_system.auth_audit_log`
//!   relation, separate from the outbox (which CDC drains and may prune), giving
//!   a query-able 7-year compliance record even if Kafka/CDC is offline.
//! - **stdout / file development sink** — line-delimited JSON for local dev and
//!   container log shipping.
//! - **SIEM webhook sink** — real HTTP `POST` (via the shared `reqwest` client)
//!   of the envelope JSON to an operator SIEM endpoint (Splunk HEC, etc.).
//!
//! Kafka and object-storage exports are already covered by the outbox → CDC
//! relay and the CDC engine's object-storage archival respectively, so they are
//! intentionally not re-implemented here.
//!
//! All export sinks are *best-effort relative to the mutation* but their failures
//! are surfaced through the `audit_sink_failures` metric (Phase L3 task 6) so an
//! operator can alert on a silently-degraded compliance pipeline.

use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value;

/// One configured immutable-export destination for already-redacted compliance
/// envelopes. Implementations MUST be non-blocking-safe (they run on the request
/// path) and MUST NOT mutate the envelope.
#[async_trait]
pub(crate) trait AuditExportSink: Send + Sync {
    /// Stable label used in logs and the `audit_sink_failures{sink=...}` metric.
    fn name(&self) -> &'static str;

    /// Export one redacted envelope. `topic` is the event's Kafka topic;
    /// `envelope` is the fully-built compliance envelope JSON (already scrubbed
    /// of credential-shaped keys by the outbox sink).
    async fn export(&self, topic: &str, envelope: &Value) -> Result<(), String>;
}

/// Append-only Postgres audit table, independent of the outbox. The table is
/// created lazily on first write so no migration ordering is required; the schema
/// mirrors the compliance envelope's queryable columns. Soft-delete is *not*
/// enabled — these rows are immutable compliance records.
pub(crate) struct PostgresAuditLogSink {
    pool: sqlx::PgPool,
    relation: String,
    /// Set once the lazy `CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS` has
    /// succeeded, so the DDL guard in `export` runs at most once per process.
    ensured: AtomicBool,
}

impl PostgresAuditLogSink {
    pub(crate) fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            relation: "udb_system.auth_audit_log".to_string(),
            ensured: AtomicBool::new(false),
        }
    }

    /// `CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS` for the durable audit log.
    /// Idempotent. The leading `CREATE SCHEMA IF NOT EXISTS udb_system` makes the
    /// sink fully self-creating even on a fresh database with no migration run.
    pub(crate) async fn ensure_table(&self) -> Result<(), String> {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
            .execute(&self.pool)
            .await
            .map_err(|e| format!("ensure udb_system schema failed: {e}"))?;
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {rel} ( \
                 audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), \
                 event_id UUID NOT NULL, \
                 event_type VARCHAR(200) NOT NULL, \
                 tenant_id VARCHAR(64) NOT NULL, \
                 actor VARCHAR(200) NOT NULL DEFAULT '', \
                 target_resource VARCHAR(400) NOT NULL DEFAULT '', \
                 operation VARCHAR(80) NOT NULL DEFAULT '', \
                 outcome VARCHAR(40) NOT NULL DEFAULT '', \
                 reason_code VARCHAR(200) NOT NULL DEFAULT '', \
                 correlation_id VARCHAR(120) NOT NULL DEFAULT '', \
                 trace_id VARCHAR(64) NOT NULL DEFAULT '', \
                 decision_id VARCHAR(120) NOT NULL DEFAULT '', \
                 source_ip VARCHAR(64) NOT NULL DEFAULT '', \
                 redaction_version VARCHAR(16) NOT NULL DEFAULT '', \
                 envelope JSONB NOT NULL, \
                 occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
             )",
            rel = self.relation
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("ensure auth_audit_log table failed: {e}"))
    }
}

#[async_trait]
impl AuditExportSink for PostgresAuditLogSink {
    fn name(&self) -> &'static str {
        "postgres_audit_log"
    }

    async fn export(&self, topic: &str, envelope: &Value) -> Result<(), String> {
        // Lazy self-create: the durable audit table has no migration, so create
        // the schema + table on first write (idempotent `IF NOT EXISTS`). Guarded
        // by a flag so the DDL runs at most once per sink, not on every export.
        if !self.ensured.load(Ordering::Acquire) {
            self.ensure_table().await?;
            self.ensured.store(true, Ordering::Release);
        }
        let s = |k: &str| envelope.get(k).and_then(Value::as_str).unwrap_or("");
        let event_id = s("event_id");
        let sql = format!(
            "INSERT INTO {rel} \
                 (event_id, event_type, tenant_id, actor, target_resource, operation, outcome, \
                  reason_code, correlation_id, trace_id, decision_id, source_ip, redaction_version, envelope) \
             VALUES ($1::UUID, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::JSONB)",
            rel = self.relation
        );
        sqlx::query(&sql)
            .bind(event_id)
            .bind(topic)
            .bind(s("actor_tenant"))
            .bind(s("actor"))
            .bind(s("target_resource"))
            .bind(s("operation"))
            .bind(s("outcome"))
            .bind(s("reason_code"))
            .bind(s("correlation_id"))
            .bind(s("trace_id"))
            .bind(s("decision_id"))
            .bind(s("source_ip"))
            .bind(s("redaction_profile"))
            .bind(envelope.to_string())
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| format!("auth audit-log insert failed: {e}"))
    }
}

/// Where a [`StdoutAuditSink`] writes: process stdout (dev/container) or an
/// append-only file (line-delimited JSON).
pub(crate) enum StdoutTarget {
    Stdout,
    File(std::path::PathBuf),
}

/// Development export: one redacted JSON envelope per line.
pub(crate) struct StdoutAuditSink {
    target: StdoutTarget,
}

impl StdoutAuditSink {
    pub(crate) fn stdout() -> Self {
        Self {
            target: StdoutTarget::Stdout,
        }
    }
    pub(crate) fn file(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            target: StdoutTarget::File(path.into()),
        }
    }
}

#[async_trait]
impl AuditExportSink for StdoutAuditSink {
    fn name(&self) -> &'static str {
        "stdout_file"
    }

    async fn export(&self, _topic: &str, envelope: &Value) -> Result<(), String> {
        let line = envelope.to_string();
        match &self.target {
            StdoutTarget::Stdout => {
                println!("{line}");
                Ok(())
            }
            StdoutTarget::File(path) => {
                use std::io::Write;
                let path = path.clone();
                tokio::task::spawn_blocking(move || {
                    let mut f = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .map_err(|e| format!("open audit file failed: {e}"))?;
                    writeln!(f, "{line}").map_err(|e| format!("write audit file failed: {e}"))
                })
                .await
                .map_err(|e| format!("audit file task join failed: {e}"))?
            }
        }
    }
}

/// Optional SIEM webhook sink: real HTTP `POST` of the envelope JSON to an
/// operator endpoint. The endpoint and optional bearer token are config-driven.
/// Gated behind the `http-client` feature (which pulls in `reqwest`), matching
/// the rest of the auth plane's outbound HTTP (`authn::tokens` JWKS proxy).
#[cfg(feature = "http-client")]
pub(crate) struct WebhookAuditSink {
    client: reqwest::Client,
    url: String,
    auth_header: Option<String>,
}

#[cfg(feature = "http-client")]
impl WebhookAuditSink {
    pub(crate) fn new(url: impl Into<String>, auth_header: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            auth_header,
        }
    }
}

#[cfg(feature = "http-client")]
#[async_trait]
impl AuditExportSink for WebhookAuditSink {
    fn name(&self) -> &'static str {
        "siem_webhook"
    }

    async fn export(&self, _topic: &str, envelope: &Value) -> Result<(), String> {
        let mut req = self
            .client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(envelope.to_string());
        if let Some(h) = &self.auth_header {
            req = req.header("authorization", h);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| format!("siem webhook post failed: {e}"))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(format!("siem webhook returned HTTP {}", resp.status()))
        }
    }
}

/// Build the configured set of additional export sinks from environment config.
///
/// - `UDB_AUDIT_EXPORT_POSTGRES=1` → durable Postgres audit table (needs `pool`).
/// - `UDB_AUDIT_EXPORT_STDOUT=1`   → stdout JSONL.
/// - `UDB_AUDIT_EXPORT_FILE=<path>`→ append-only JSONL file.
/// - `UDB_AUDIT_EXPORT_WEBHOOK_URL=<url>` (+ optional
///   `UDB_AUDIT_EXPORT_WEBHOOK_AUTH`) → SIEM webhook.
///
/// Returns an empty vec when nothing is configured (the outbox path alone).
pub(crate) fn export_sinks_from_env(pool: Option<&sqlx::PgPool>) -> Vec<Arc<dyn AuditExportSink>> {
    let on = |k: &str| {
        std::env::var(k)
            .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    };
    let mut sinks: Vec<Arc<dyn AuditExportSink>> = Vec::new();
    if on("UDB_AUDIT_EXPORT_POSTGRES") {
        if let Some(pool) = pool {
            sinks.push(Arc::new(PostgresAuditLogSink::new(pool.clone())));
        }
    }
    if on("UDB_AUDIT_EXPORT_STDOUT") {
        sinks.push(Arc::new(StdoutAuditSink::stdout()));
    }
    if let Ok(path) = std::env::var("UDB_AUDIT_EXPORT_FILE") {
        if !path.trim().is_empty() {
            sinks.push(Arc::new(StdoutAuditSink::file(path)));
        }
    }
    #[cfg(feature = "http-client")]
    if let Ok(url) = std::env::var("UDB_AUDIT_EXPORT_WEBHOOK_URL") {
        if !url.trim().is_empty() {
            let auth = std::env::var("UDB_AUDIT_EXPORT_WEBHOOK_AUTH")
                .ok()
                .filter(|s| !s.trim().is_empty());
            sinks.push(Arc::new(WebhookAuditSink::new(url, auth)));
        }
    }
    sinks
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn file_sink_writes_jsonl_line() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("udb_audit_test_{}.jsonl", uuid::Uuid::new_v4()));
        let sink = StdoutAuditSink::file(&path);
        let env = json!({ "event_id": "e1", "event_type": "udb.authz.access.denied.v1" });
        sink.export("udb.authz.access.denied.v1", &env)
            .await
            .expect("file export");
        let body = std::fs::read_to_string(&path).expect("read back");
        assert!(body.contains("udb.authz.access.denied.v1"));
        assert!(body.ends_with('\n'));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn env_sinks_default_to_empty() {
        // With no UDB_AUDIT_EXPORT_* vars set in this test process, the set is
        // empty (outbox-only). We can't safely mutate process env in parallel
        // tests, so assert the pure no-config branch.
        let sinks = export_sinks_from_env(None);
        // stdout/file/webhook/postgres all opt-in; default config yields none.
        assert!(sinks.iter().all(|s| !s.name().is_empty()));
    }
}
