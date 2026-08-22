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
//! broken/unreachable audit DB falls back to stdout rather than blocking or
//! failing the write. **Kafka** remains unwired and falls back the same way.
//!
//! # The degradation invariant
//!
//! Falling back to stdout means the operator asked for a durable audit trail and
//! is not getting one. That has now silently reached production twice: in 0.4.34
//! because the writer task exited, and again because `CREATE TABLE IF NOT EXISTS`
//! accepted a pre-existing table of the wrong shape. Each was fixed as its own
//! mechanism, and the outcome came back through the next door.
//!
//! The reason it could come back is that this module had no telemetry at all — no
//! metric, no health signal, no count — while the auth plane's sink had all three.
//! So the rule here is structural rather than per-mechanism: **every fallback path
//! calls [`note_audit_degraded`]**, which counts the event, records the reason,
//! logs on a throttle, and always writes the event to stdout so none is dropped.
//! [`audit_degradation_snapshot`] then surfaces it in `GetHealthReport`, which
//! turns a silent audit gap into a failing probe that takes the broker out of
//! rotation. A new failure mechanism is visible the moment it appears, whether or
//! not anyone remembered to instrument it.

use std::io::Write as _;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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

/// Cumulative count of audit events that could NOT be durably stored.
static AUDIT_DEGRADED_EVENTS: AtomicU64 = AtomicU64::new(0);
/// Unix seconds of the most recent degraded event.
static AUDIT_DEGRADED_LAST_UNIX: AtomicI64 = AtomicI64::new(0);
/// Unix seconds of the most recent degradation log line (throttle state).
static AUDIT_DEGRADED_LAST_LOG_UNIX: AtomicI64 = AtomicI64::new(0);
/// The most recent degradation reason, for the health report to quote.
static AUDIT_DEGRADED_REASON: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

/// How often to repeat the degradation log while it persists.
///
/// The two prior shapes were both wrong. A one-shot `AtomicBool` warned once per
/// PROCESS, so every later failure — including an unrelated one days afterwards —
/// was silent. A per-event `warn!` produced 4,326 identical lines in six hours.
/// Neither told an operator "this is still happening, and here is how much has
/// been lost".
const AUDIT_DEGRADED_LOG_INTERVAL_SECS: i64 = 60;

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// THE chokepoint for "the configured durable audit sink could not take this
/// event". Every fallback path funnels through here.
///
/// This exists because of how the failure recurred rather than because of any one
/// mechanism. 0.4.34 fixed the writer task exiting; the shape check in
/// [`verify_pg_audit_columns`] fixes a pre-existing table of the wrong shape. Both
/// are single doors. What let the SAME outcome return through a new door is that
/// this module recorded no metric, exposed no health signal, and counted nothing —
/// so a broker lost every audit event for days while reporting itself ready.
///
/// Routing every path through one function makes the next mechanism observable
/// the moment it appears, without anyone remembering to wire it up.
pub(crate) fn note_audit_degraded(reason: &str, event: &AuditEvent) {
    // NEVER drop. Two File-sink paths used to return without writing the event
    // anywhere at all, against this module's own documented promise.
    print!("{}", audit_event_line(event));

    let total = AUDIT_DEGRADED_EVENTS.fetch_add(1, Ordering::Relaxed) + 1;
    let now = unix_now_secs();
    AUDIT_DEGRADED_LAST_UNIX.store(now, Ordering::Relaxed);
    if let Ok(mut slot) = AUDIT_DEGRADED_REASON.lock()
        && *slot != reason
    {
        slot.clear();
        slot.push_str(reason);
    }

    let last = AUDIT_DEGRADED_LAST_LOG_UNIX.load(Ordering::Relaxed);
    if last == 0 || now.saturating_sub(last) >= AUDIT_DEGRADED_LOG_INTERVAL_SECS {
        AUDIT_DEGRADED_LAST_LOG_UNIX.store(now, Ordering::Relaxed);
        tracing::error!(
            reason,
            degraded_events = total,
            "durable audit sink is DEGRADED: events are going to stdout and are NOT durably \
             stored. This is an audit-trail gap, not a transport detail"
        );
    }
}

/// Snapshot of durable-audit degradation for the health report:
/// `(events_degraded, reason, last_unix_secs)`, or `None` if audit has never
/// degraded in this process.
pub(crate) fn audit_degradation_snapshot() -> Option<(u64, String, i64)> {
    let events = AUDIT_DEGRADED_EVENTS.load(Ordering::Relaxed);
    if events == 0 {
        return None;
    }
    let reason = AUDIT_DEGRADED_REASON
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    Some((
        events,
        reason,
        AUDIT_DEGRADED_LAST_UNIX.load(Ordering::Relaxed),
    ))
}

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
            // Both arms below used to log and return WITHOUT writing the event
            // anywhere - a silent drop, in a module whose own docs promise it
            // never drops an event. They now degrade like every other path.
            let Some(path) = config.file_path.as_deref().filter(|p| !p.trim().is_empty()) else {
                note_audit_degraded("file_sink_path_unset", event);
                return;
            };
            if let Err(err) = append_line(path, &audit_event_line(event)) {
                tracing::warn!(path = %path, error = %err, "audit file append failed");
                note_audit_degraded("file_sink_append_failed", event);
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
                            note_audit_degraded("pg_writer_queue_full_or_dead", event);
                        }
                    }
                    // Unsafe / unusable table identifier — disabled, stdout
                    // instead. This arm carried no warning of its own at all.
                    None => note_audit_degraded("pg_table_identifier_unusable", event),
                },
                // No pool or no table configured — can't persist; stdout fallback.
                _ => note_audit_degraded("pg_pool_or_table_missing", event),
            }
        }
        // Not yet wired to its transport.
        AuditSinkKind::Kafka => {
            // The operator asked for durable Kafka audit and is not getting it,
            // which is a degradation like any other - not merely a startup note.
            note_audit_degraded("kafka_transport_unwired", event);
        }
    }
}

/// How long to wait before re-attempting `ensure_pg_audit_table` after it fails.
///
/// Re-checking on every event turned a permanent misconfiguration into three
/// failed round trips PER AUDIT EVENT - 4,326 of them in six hours on one live
/// broker, against a table whose shape was never going to change on its own. A
/// transient outage still recovers, on this interval rather than instantly; events
/// keep going to stdout in the meantime, so none are lost either way.
const PG_AUDIT_ENSURE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

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
                // Set ONLY when `ensure` itself fails. An insert that fails after a
                // successful ensure is the transient case, and still retries at once.
                let mut ensure_failed_at: Option<std::time::Instant> = None;
                // NEVER exit: a transient/ensure error must not permanently disable
                // the sink (the 0.4.34 defect — the task returned, dropped `rx`, and
                // every later audit silently fell back to stdout). Instead lazily
                // (re)create the table until it sticks, and on ANY failure fall back
                // to stdout (visible, never silently dropped) + retry, throttled by
                // PG_AUDIT_ENSURE_RETRY_INTERVAL so a permanent misconfiguration
                // cannot re-run the ensure on every single event.
                while let Some(event) = rx.recv().await {
                    if !table_ready {
                        if ensure_failed_at
                            .is_some_and(|at| at.elapsed() < PG_AUDIT_ENSURE_RETRY_INTERVAL)
                        {
                            note_audit_degraded("pg_table_unusable_backoff", &event);
                            continue;
                        }
                        match ensure_pg_audit_table(&pool, &relation).await {
                            Ok(()) => {
                                ensure_failed_at = None;
                                table_ready = true;
                            }
                            Err(err) => {
                                ensure_failed_at = Some(std::time::Instant::now());
                                // The reason string; the chokepoint below owns
                                // counting and throttling.
                                tracing::warn!(
                                    error = %err, table = %relation,
                                    "durable Postgres audit table is unusable. If this names                                      missing columns it is a configuration error that will NOT                                      heal on its own - fix UDB_AUDIT_PG_TABLE"
                                );
                                note_audit_degraded("pg_ensure_table_failed", &event);
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
                        note_audit_degraded("pg_insert_failed", &event);
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
        .map_err(|e| format!("ensure audit table failed: {e}"))?;
    // The CREATE above is a no-op when the relation already exists, whatever
    // shape it has, so it cannot be the whole check.
    verify_pg_audit_columns(pool, relation).await
}

/// The columns [`pg_audit_insert_sql`] binds, and therefore the columns the
/// configured relation must actually have. One list, so the INSERT and the boot
/// check cannot drift apart.
const PG_AUDIT_BOUND_COLUMNS: [&str; 7] = [
    "event_type",
    "tenant_id",
    "user_id",
    "correlation_id",
    "purpose",
    "resource_uri",
    "checksum_sha256",
];

/// Parameterized INSERT for one audit event into the validated `relation`.
fn pg_audit_insert_sql(relation: &str) -> String {
    let cols = PG_AUDIT_BOUND_COLUMNS.join(", ");
    let params = (1..=PG_AUDIT_BOUND_COLUMNS.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {relation} ({cols}) VALUES ({params})")
}

/// The event sink and the admin-audit CHAIN both answer to `udb_admin_audit_log`,
/// and the chain's is the documented default - so it is precisely the name an
/// operator reaches for when pointing `UDB_AUDIT_PG_TABLE` somewhere. The shapes
/// are incompatible and the chain table is UDB-owned with its own writer, so say
/// that outright rather than letting a generic "missing columns" message imply the
/// remedy is to ALTER TABLE it.
fn admin_audit_collision_hint(relation: &str) -> &'static str {
    let table = relation.rsplit('.').next().unwrap_or(relation);
    if table.eq_ignore_ascii_case("udb_admin_audit_log") {
        "That is UDB's own hash-chained admin-audit table: a different shape with a different \
         writer, so do NOT add columns to it. "
    } else {
        ""
    }
}

/// Prove the configured relation is WRITABLE in the shape the sink inserts.
///
/// `CREATE TABLE IF NOT EXISTS` is a no-op against a pre-existing table of ANY
/// shape, so creatability was never evidence of writability: a relation that
/// already existed passed the boot check and then failed every insert, sending
/// audit to stdout permanently while the broker reported itself ready - the exact
/// outcome the readiness check was written to prevent. Read-only on purpose; a
/// trial insert would burn a sequence value and could fire triggers on a table UDB
/// does not own.
async fn verify_pg_audit_columns(pool: &PgPool, relation: &str) -> Result<(), String> {
    verify_table_columns(pool, relation, &PG_AUDIT_BOUND_COLUMNS).await
}

/// The shape check itself, over an arbitrary bound-column list.
///
/// Shared with the auth-plane `PostgresAuditLogSink`, which self-creates its table
/// the same way and therefore had the identical hole. One implementation so a fix
/// to either plane cannot leave the other behind — the asymmetry between these two
/// sinks is what let the same defect live on in one of them.
pub(crate) async fn verify_table_columns(
    pool: &PgPool,
    relation: &str,
    bound_columns: &[&str],
) -> Result<(), String> {
    // The relation may arrive quoted (`"udb_system"."auth_audit_log"`); compare on
    // the bare identifiers, which is what information_schema stores.
    let unquote = |s: &str| s.trim().trim_matches('"').to_string();
    let (schema, table) = match relation.split_once('.') {
        Some((s, t)) => (Some(unquote(s)), unquote(t)),
        None => (None, unquote(relation)),
    };
    let (schema, table) = (schema.as_deref(), table.as_str());
    // `information_schema` rather than `pg_attribute`: the identity and generated
    // flags read below are standard there across every supported server version.
    let rows: Vec<(String, bool, bool)> = sqlx::query_as::<_, (String, bool, bool)>(
        "SELECT column_name::TEXT, \
                COALESCE(is_nullable = 'NO', FALSE), \
                COALESCE( \
                    column_default IS NOT NULL \
                        OR is_identity = 'YES' \
                        OR is_generated = 'ALWAYS', \
                    FALSE \
                ) \
         FROM information_schema.columns \
         WHERE table_schema = COALESCE($1, current_schema()) AND table_name = $2",
    )
    .bind(schema)
    .bind(table)
    .fetch_all(pool)
    .await
    .map_err(|e| format!("audit table column check failed: {e}"))?;

    if rows.is_empty() {
        return Err(format!(
            "audit table {relation} reports no columns - it does not resolve under the current \
             search_path, or this role holds no privileges on it"
        ));
    }

    let missing: Vec<&str> = bound_columns
        .iter()
        .copied()
        .filter(|c| !rows.iter().any(|(name, _, _)| name.as_str() == *c))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "audit table {relation} exists but lacks the column(s) {}; the sink inserts ({}), so \
             every audit write fails and falls back to stdout. {}Point UDB_AUDIT_PG_TABLE at a \
             dedicated table and let the sink create it.",
            missing.join(", "),
            bound_columns.join(", "),
            admin_audit_collision_hint(relation),
        ));
    }

    // The same defect wearing a different hat: a NOT NULL column the sink never
    // populates, with nothing to fill it, fails every insert just as surely as a
    // missing one - and would otherwise reach production the same silent way.
    let unwritable: Vec<&str> = rows
        .iter()
        .filter(|(name, not_null, has_default)| {
            *not_null && !*has_default && !bound_columns.contains(&name.as_str())
        })
        .map(|(name, _, _)| name.as_str())
        .collect();
    if !unwritable.is_empty() {
        return Err(format!(
            "audit table {relation} has NOT NULL column(s) the sink does not populate and that \
             carry no default: {}; every audit write fails. {}",
            unwritable.join(", "),
            admin_audit_collision_hint(relation),
        ));
    }
    Ok(())
}

/// Startup readiness check for the durable Postgres audit sink, called from
/// `serve()`. When `UDB_AUDIT_SINK=postgres`, actually create the configured table
/// NOW (on the data-plane pool) so a broken sink — a bad `UDB_AUDIT_PG_TABLE`, a
/// missing CREATE privilege, or no Postgres pool — surfaces LOUDLY at boot with the
/// exact reason, instead of a detached writer task dying quietly and every audit
/// silently falling back to stdout (the failure mode that made 0.4.34 look broken).
/// Returns `Ok(())` for a non-Postgres sink, or once the table is confirmed
/// WRITABLE — creatable was not enough, and proving only that reproduced the very
/// 0.4.34 outcome this check exists to prevent (see [`verify_pg_audit_columns`]).
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

    /// The INSERT is now built from `PG_AUDIT_BOUND_COLUMNS` rather than written
    /// out longhand. Pin the exact SQL so that refactor cannot have changed what
    /// goes on the wire, and so adding a column to the const without teaching the
    /// binder about it fails here rather than at runtime.
    #[test]
    fn insert_sql_is_unchanged_by_the_const_driven_builder() {
        let expected = concat!(
            "INSERT INTO udb_system.data_audit ",
            "(event_type, tenant_id, user_id, correlation_id, purpose, resource_uri, ",
            "checksum_sha256) VALUES ($1, $2, $3, $4, $5, $6, $7)"
        );
        assert_eq!(pg_audit_insert_sql("udb_system.data_audit"), expected);
        assert_eq!(
            PG_AUDIT_BOUND_COLUMNS.len(),
            7,
            "the writer binds exactly seven values; update both together"
        );
    }

    /// `udb_admin_audit_log` is the admin-audit CHAIN's documented default, so an
    /// operator who points `UDB_AUDIT_PG_TABLE` at it must be told it is UDB-owned
    /// rather than left to conclude the remedy is to ALTER TABLE it.
    #[test]
    fn admin_audit_chain_table_is_called_out_by_name() {
        assert!(
            admin_audit_collision_hint("udb_system.udb_admin_audit_log")
                .contains("do NOT add columns"),
            "the chain table must be named as UDB-owned"
        );
        assert!(
            admin_audit_collision_hint("UDB_ADMIN_AUDIT_LOG").contains("do NOT add columns"),
            "identifier comparison is case-insensitive in Postgres"
        );
        assert!(
            admin_audit_collision_hint("udb_system.data_audit").is_empty(),
            "an ordinary table gets no chain-specific hint"
        );
    }
}
