//! Data-plane audit emitter (F-1).
//!
//! `build_audit_event` produced an `AuditEvent` that NOTHING consumed — its only
//! caller was a test — so `AuditSinkConfig` (stdout/file/kafka/postgres) was
//! fully configured and production-validated while every data-plane mutation went
//! unaudited on every configuration. This wires the emitter: a successful
//! mutation now produces a structured JSON audit line to the configured sink.
//!
//! Stdout and File write inline. **Postgres** persists durably: `emit_audit`
//! hands the event to a lazily-started, bounded background writer that
//! self-creates the configured `UDB_AUDIT_PG_TABLE` (`CREATE SCHEMA`/`CREATE TABLE
//! IF NOT EXISTS`, mirroring the auth-plane `PostgresAuditLogSink`) and INSERTs
//! each event off the request path. It is best-effort by design — the mutation has
//! already committed (and is journaled via CDC/outbox) — so a full queue or a
//! broken/unreachable audit DB falls back to stdout with a warning rather than
//! blocking or failing the write, and never drops an event silently. **Kafka**
//! remains unwired and still falls back to stdout with a one-time warning.

use std::io::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::PgPool;
use tokio::sync::mpsc;

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

static UNWIRED_KAFKA_WARNED: AtomicBool = AtomicBool::new(false);
static PG_AUDIT_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

/// Bounded queue depth for the durable Postgres audit writer. Best-effort: on a
/// sustained audit-DB stall the queue caps and further events warn-and-fall-back
/// to stdout rather than growing memory unbounded or blocking the (already
/// committed) mutation.
const PG_AUDIT_QUEUE_DEPTH: usize = 8192;

/// Emit `event` to the configured audit sink. Best-effort by design: a mutation
/// has already committed, so an audit-transport hiccup logs and falls back to
/// stdout rather than failing the write (the write itself is journaled via
/// CDC/outbox). `pg_pool` is the data-plane Postgres pool the caller already
/// holds, used only by the Postgres sink. `None` kind is a no-op.
pub(crate) fn emit_audit(config: &AuditSinkConfig, event: &AuditEvent, pg_pool: Option<&PgPool>) {
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
        AuditSinkKind::Postgres => {
            let table = config.pg_table.as_deref().unwrap_or("").trim();
            match pg_pool {
                Some(pool) if !table.is_empty() => match pg_audit_writer(pool, table) {
                    // Enqueue off the request path. `try_send` fails only when the
                    // bounded queue is full OR the writer task has exited (a
                    // table-create failure / dead audit DB), in which case we fall
                    // back to stdout so the event is never silently lost.
                    Some(writer) => {
                        if writer.tx.try_send(event.clone()).is_err() {
                            pg_audit_fallback_warn();
                            print!("{}", audit_event_line(event));
                        }
                    }
                    // Unsafe / unusable table identifier — disabled, stdout instead.
                    None => print!("{}", audit_event_line(event)),
                },
                // No pool or no table configured — can't persist; stdout fallback.
                _ => {
                    pg_audit_fallback_warn();
                    print!("{}", audit_event_line(event));
                }
            }
        }
        // Not yet wired to its transport — fall back to stdout (never silently
        // drop) and warn once so an operator notices the configuration gap.
        AuditSinkKind::Kafka => {
            if !UNWIRED_KAFKA_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "audit sink kind=Kafka is configured but its transport is not yet wired; \
                     audit events are being written to stdout as a fallback"
                );
            }
            print!("{}", audit_event_line(event));
        }
    }
}

fn pg_audit_fallback_warn() {
    if !PG_AUDIT_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            "durable Postgres audit sink is unavailable (queue full, unreachable DB, or missing \
             UDB_AUDIT_PG_TABLE / Postgres pool); audit events are falling back to stdout"
        );
    }
}

/// The lazily-started durable Postgres audit writer. Owns the send half of a
/// bounded channel drained by a background task that INSERTs each event.
struct PgAuditWriter {
    tx: mpsc::Sender<AuditEvent>,
}

/// Return the process-wide durable Postgres audit writer, starting it (once) with
/// the given pool + table on first use. `None` when the configured table is not a
/// safe qualified identifier (the writer is permanently disabled → stdout). The
/// first caller's pool/table win; both are env-derived and stable per process.
fn pg_audit_writer(pool: &PgPool, table: &str) -> Option<&'static PgAuditWriter> {
    static WRITER: OnceLock<Option<PgAuditWriter>> = OnceLock::new();
    WRITER
        .get_or_init(|| {
            let Some(relation) = sanitize_audit_relation(table) else {
                tracing::error!(
                    table,
                    "UDB_AUDIT_PG_TABLE is not a safe schema-qualified identifier; the durable \
                     Postgres audit sink is disabled (stdout fallback)"
                );
                return None;
            };
            let (tx, mut rx) = mpsc::channel::<AuditEvent>(PG_AUDIT_QUEUE_DEPTH);
            let pool = pool.clone();
            // Drain the queue on a background task so the request path never blocks
            // on a DB write. Started inside the caller's async runtime context.
            tokio::spawn(async move {
                let insert = pg_audit_insert_sql(&relation);
                let mut table_ready = false;
                // NEVER exit: a transient/ensure error must not permanently disable
                // the sink (the 0.4.34 defect — the task returned, dropped `rx`, and
                // every later audit silently fell back to stdout). Instead lazily
                // (re)create the table until it sticks, and on ANY failure fall back
                // to stdout (visible, never silently dropped) + retry next event.
                while let Some(event) = rx.recv().await {
                    if !table_ready {
                        match ensure_pg_audit_table(&pool, &relation).await {
                            Ok(()) => table_ready = true,
                            Err(err) => {
                                if !PG_AUDIT_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                                    tracing::error!(
                                        error = %err, table = %relation,
                                        "durable Postgres audit table unavailable; audit events fall back to stdout until it can be created"
                                    );
                                }
                                print!("{}", audit_event_line(&event));
                                continue;
                            }
                        }
                    }
                    if let Err(err) = sqlx::query(&insert)
                        .bind(event.event_type.as_str())
                        .bind(event.tenant_id.as_str())
                        .bind(event.user_id.as_str())
                        .bind(event.correlation_id.as_str())
                        .bind(event.purpose.as_str())
                        .bind(event.resource_uri.as_str())
                        .bind(event.checksum_sha256.as_str())
                        .execute(&pool)
                        .await
                    {
                        tracing::warn!(error = %err, "durable Postgres audit insert failed; falling back to stdout, will re-check the table");
                        table_ready = false;
                        print!("{}", audit_event_line(&event));
                    }
                }
            });
            Some(PgAuditWriter { tx })
        })
        .as_ref()
}

/// Validate a fully-qualified audit table name (`schema.table` or `table`) as a
/// safe Postgres identifier before it is interpolated into DDL/DML. Each segment
/// must be a plain unquoted identifier (`[A-Za-z_][A-Za-z0-9_]*`), 1 or 2 segments
/// only. Returns the normalized `schema.table` (or bare `table`) on success, `None`
/// on anything unsafe — closing SQL injection via `UDB_AUDIT_PG_TABLE`.
fn sanitize_audit_relation(table: &str) -> Option<String> {
    let parts: Vec<&str> = table.trim().split('.').collect();
    if parts.is_empty() || parts.len() > 2 {
        return None;
    }
    let is_ident = |s: &str| {
        !s.is_empty()
            && s.len() <= 63
            && s.chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    if parts.iter().all(|p| is_ident(p)) {
        Some(parts.join("."))
    } else {
        None
    }
}

/// Optional schema segment of a validated `schema.table` relation.
fn audit_relation_schema(relation: &str) -> Option<&str> {
    relation.split_once('.').map(|(schema, _)| schema)
}

/// `CREATE SCHEMA`/`CREATE TABLE IF NOT EXISTS` for the durable data-plane audit
/// table. Idempotent + self-creating (no migration needed), mirroring the
/// auth-plane `PostgresAuditLogSink::ensure_table`. `relation` is pre-validated by
/// [`sanitize_audit_relation`], so interpolation is safe.
async fn ensure_pg_audit_table(pool: &PgPool, relation: &str) -> Result<(), String> {
    if let Some(schema) = audit_relation_schema(relation) {
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(pool)
            .await
            .map_err(|e| format!("ensure audit schema failed: {e}"))?;
    }
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {relation} ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             event_type VARCHAR(80) NOT NULL DEFAULT '', \
             tenant_id VARCHAR(64) NOT NULL DEFAULT '', \
             user_id VARCHAR(200) NOT NULL DEFAULT '', \
             correlation_id VARCHAR(120) NOT NULL DEFAULT '', \
             purpose VARCHAR(120) NOT NULL DEFAULT '', \
             resource_uri VARCHAR(400) NOT NULL DEFAULT '', \
             checksum_sha256 VARCHAR(80) NOT NULL DEFAULT '', \
             occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW() \
         )"
    );
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| format!("ensure audit table failed: {e}"))
}

/// Parameterized INSERT for one audit event into the validated `relation`.
fn pg_audit_insert_sql(relation: &str) -> String {
    format!(
        "INSERT INTO {relation} \
             (event_type, tenant_id, user_id, correlation_id, purpose, resource_uri, checksum_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    )
}

/// Startup readiness check for the durable Postgres audit sink, called from
/// `serve()`. When `UDB_AUDIT_SINK=postgres`, actually create the configured table
/// NOW (on the data-plane pool) so a broken sink — a bad `UDB_AUDIT_PG_TABLE`, a
/// missing CREATE privilege, or no Postgres pool — surfaces LOUDLY at boot with the
/// exact reason, instead of a detached writer task dying quietly and every audit
/// silently falling back to stdout (the failure mode that made 0.4.34 look broken).
/// Returns `Ok(())` for a non-Postgres sink or once the table is confirmed creatable.
pub(crate) async fn ensure_pg_audit_sink_ready(
    config: &AuditSinkConfig,
    pg_pool: Option<&PgPool>,
) -> Result<(), String> {
    if config.kind != AuditSinkKind::Postgres {
        return Ok(());
    }
    let table = config.pg_table.as_deref().unwrap_or("").trim();
    if table.is_empty() {
        return Err(
            "UDB_AUDIT_SINK=postgres requires UDB_AUDIT_PG_TABLE=<schema.table>".to_string(),
        );
    }
    let relation = sanitize_audit_relation(table).ok_or_else(|| {
        format!("UDB_AUDIT_PG_TABLE='{table}' is not a safe schema-qualified identifier")
    })?;
    let pool = pg_pool.ok_or_else(|| {
        "UDB_AUDIT_SINK=postgres but no data-plane Postgres pool is configured (set UDB_PG_DSN)"
            .to_string()
    })?;
    ensure_pg_audit_table(pool, &relation).await
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
        emit_audit(&cfg, &sample(), None);
        emit_audit(&cfg, &sample(), None);
        let contents = std::fs::read_to_string(&path).expect("read audit file");
        assert_eq!(contents.lines().count(), 2, "one line per event");
        assert!(contents.contains("\"event_type\":\"upsert\""));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn none_sink_is_a_noop() {
        // No panic, no file — the default backward-compatible behavior.
        emit_audit(&AuditSinkConfig::default(), &sample(), None);
    }

    #[test]
    fn audit_relation_accepts_only_safe_identifiers() {
        assert_eq!(
            sanitize_audit_relation("udb_system.data_audit_log").as_deref(),
            Some("udb_system.data_audit_log")
        );
        assert_eq!(
            sanitize_audit_relation("audit_log").as_deref(),
            Some("audit_log")
        );
        assert_eq!(
            sanitize_audit_relation("  public.a1  ").as_deref(),
            Some("public.a1")
        );
        // Injection / malformed shapes are rejected → sink disabled, never
        // interpolated into DDL/DML.
        assert!(sanitize_audit_relation("a.b.c").is_none());
        assert!(sanitize_audit_relation("audit; DROP TABLE users").is_none());
        assert!(sanitize_audit_relation("audit log").is_none());
        assert!(sanitize_audit_relation("\"audit\"").is_none());
        assert!(sanitize_audit_relation("1audit").is_none());
        assert!(sanitize_audit_relation("").is_none());
        assert!(sanitize_audit_relation("schema.").is_none());
    }

    #[test]
    fn pg_audit_ddl_and_insert_reference_the_validated_relation() {
        let rel = sanitize_audit_relation("udb_system.data_audit_log").expect("valid");
        assert_eq!(audit_relation_schema(&rel), Some("udb_system"));
        let insert = pg_audit_insert_sql(&rel);
        assert!(insert.starts_with("INSERT INTO udb_system.data_audit_log"));
        // All seven event fields are bound positionally.
        assert!(insert.contains("$7"));
        assert!(!insert.contains("$8"));
        assert!(audit_relation_schema("audit_log").is_none());
    }

    /// EMPIRICAL end-to-end repro against a live Postgres (gated on `RUN_AUDIT_REPRO`).
    /// Exercises the REAL `UdbConfig::from_env()` merge (does it populate
    /// `audit_sink.pg_table`?) and the REAL `emit_audit` — the exact call the
    /// data-plane upsert makes — and asserts a row PERSISTS rather than going to
    /// stdout. Run with:
    ///   RUN_AUDIT_REPRO=1 REPRO_DSN=postgres://udb:udb@127.0.0.1:55999/udb \
    ///     cargo test --lib runtime::core::audit::tests::repro_real_config_and_emit_persists -- --nocapture
    #[tokio::test]
    async fn repro_real_config_and_emit_persists() {
        if std::env::var("RUN_AUDIT_REPRO").is_err() {
            return;
        }
        // SAFETY: gated single-run repro; edition-2024 requires unsafe env mutation.
        unsafe {
            std::env::set_var("UDB_AUDIT_SINK", "postgres");
            std::env::set_var("UDB_AUDIT_PG_TABLE", "udb_system.data_audit_repro2");
        }
        // The REAL config path used by serve()/live_native_config().
        let cfg = crate::runtime::config::UdbConfig::from_env();
        eprintln!(
            "REAL from_env audit_sink: kind={:?} pg_table={:?}",
            cfg.audit_sink.kind, cfg.audit_sink.pg_table
        );
        assert_eq!(
            cfg.audit_sink.kind,
            AuditSinkKind::Postgres,
            "from_env must set kind=Postgres"
        );
        assert_eq!(
            cfg.audit_sink.pg_table.as_deref(),
            Some("udb_system.data_audit_repro2"),
            "from_env must populate pg_table from UDB_AUDIT_PG_TABLE"
        );
        let dsn = std::env::var("REPRO_DSN").expect("set REPRO_DSN");
        let pool = sqlx::PgPool::connect(&dsn).await.expect("connect repro pg");
        sqlx::query("DROP TABLE IF EXISTS udb_system.data_audit_repro2")
            .execute(&pool)
            .await
            .ok();
        // EXACT call shape the data-plane upsert makes.
        emit_audit(&cfg.audit_sink, &sample(), Some(&pool));
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM udb_system.data_audit_repro2")
            .fetch_one(&pool)
            .await
            .unwrap_or(-1);
        eprintln!("REAL EMIT persisted rows = {count} (expected 1)");
        assert_eq!(
            count, 1,
            "real config + emit must persist to Postgres, not stdout"
        );
    }
}
