//! Live conformance for the durable audit sinks (data plane and auth plane).
//!
//! These exist because the SAME outcome — every audit event falling back to
//! stdout while the broker reported itself healthy — reached production twice,
//! through two different mechanisms. It was fixed both times against real
//! Postgres only by hand, never in a test CI runs, so nothing stopped the second
//! occurrence.
//!
//! Every assertion below therefore runs against a REAL Postgres in `live-quick`.
//! In particular `live_audit_sink_refuses_foreign_shaped_table` proves the
//! creatability-vs-writability distinction empirically: it asserts that the old
//! check (`CREATE TABLE IF NOT EXISTS` alone) still SUCCEEDS against the bad
//! table, and that readiness nonetheless refuses it. A future refactor that
//! reverts to "creatable is good enough" fails here rather than in a customer's
//! six-hour log.
//!
//! Run (after `docker compose ... up -d --wait postgres`):
//!   UDB_LIVE_AUTH_TESTS=1 cargo test --lib audit_sink_live -- --ignored --nocapture

use super::support::*;
use crate::planning::broker::AuditEvent;
use crate::runtime::config::{AuditSinkConfig, AuditSinkKind};
use crate::runtime::core::audit::{
    audit_degradation_snapshot, emit_audit, ensure_pg_audit_sink_ready,
};

fn sample_event(tag: &str) -> AuditEvent {
    AuditEvent {
        event_type: format!("upsert-{tag}"),
        tenant_id: "tenant-live".to_string(),
        user_id: "user-live".to_string(),
        correlation_id: format!("corr-{tag}"),
        purpose: "live-audit-conformance".to_string(),
        resource_uri: format!("udb://live/{tag}"),
        checksum_sha256: "0".repeat(64),
    }
}

fn pg_sink(table: &str) -> AuditSinkConfig {
    AuditSinkConfig {
        kind: AuditSinkKind::Postgres,
        pg_table: Some(table.to_string()),
        ..Default::default()
    }
}

/// Count of degraded audit events so far. The counter is process-global, so every
/// assertion below is a DELTA — absolute values would depend on test ordering.
fn degraded_count() -> u64 {
    audit_degradation_snapshot().map(|(n, _, _)| n).unwrap_or(0)
}

/// THE reported defect. A table that already exists in the wrong shape was
/// accepted by the readiness check, because `CREATE TABLE IF NOT EXISTS` is a
/// no-op against it — so the broker booted healthy and then failed every insert.
#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_audit_sink_refuses_foreign_shaped_table -- --ignored --nocapture"]
async fn live_audit_sink_refuses_foreign_shaped_table() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(&pool)
        .await
        .expect("create udb_system schema");
    sqlx::query("DROP TABLE IF EXISTS udb_system.audit_foreign_shape")
        .execute(&pool)
        .await
        .expect("drop stale fixture");
    // The reported shape: UDB's own hash-chained admin-audit table. It exists, and
    // it has none of the columns the event sink binds.
    sqlx::query(
        "CREATE TABLE udb_system.audit_foreign_shape ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             actor TEXT NOT NULL DEFAULT '', \
             operation TEXT NOT NULL DEFAULT '', \
             previous_hash TEXT NOT NULL DEFAULT '', \
             current_hash TEXT NOT NULL DEFAULT '' )",
    )
    .execute(&pool)
    .await
    .expect("create foreign-shaped table");

    // ── The regression anchor ────────────────────────────────────────────────
    // Prove empirically that the OLD check would still pass today: creatability
    // is not writability. If someone reverts readiness to just this statement,
    // the assertion below it fails and says why.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS udb_system.audit_foreign_shape ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             event_type VARCHAR(80) NOT NULL DEFAULT '' )",
    )
    .execute(&pool)
    .await
    .expect("CREATE TABLE IF NOT EXISTS must SUCCEED against the wrong-shaped table - that is precisely why it was never proof of writability");

    // ── The fix ──────────────────────────────────────────────────────────────
    let err = ensure_pg_audit_sink_ready(&pg_sink("udb_system.audit_foreign_shape"), Some(&pool))
        .await
        .expect_err("readiness must REFUSE a table the sink cannot write to");
    eprintln!("readiness refusal = {err}");
    assert!(
        err.contains("event_type"),
        "the refusal must name the missing column, not just fail: {err}"
    );
    assert!(
        err.contains("UDB_AUDIT_PG_TABLE"),
        "the refusal must name the knob that fixes it: {err}"
    );

    // And prove the insert really would have failed, so the refusal is not
    // over-strict: this is the exact SQL the writer runs.
    let insert_err = sqlx::query(
        "INSERT INTO udb_system.audit_foreign_shape \
             (event_type, tenant_id, user_id, correlation_id, purpose, resource_uri, checksum_sha256) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind("upsert")
    .bind("t")
    .bind("u")
    .bind("c")
    .bind("p")
    .bind("r")
    .bind("s")
    .execute(&pool)
    .await
    .expect_err("the sink INSERT must genuinely fail against this table");
    eprintln!("confirmed insert failure = {insert_err}");

    sqlx::query("DROP TABLE IF EXISTS udb_system.audit_foreign_shape")
        .execute(&pool)
        .await
        .ok();

    // ── The sibling case ─────────────────────────────────────────────────────
    // A table that HAS all seven bound columns but also carries a NOT NULL column
    // the sink never populates fails every insert just as permanently. Same
    // defect, different shape — so it is proven here too, not just reasoned about.
    sqlx::query("DROP TABLE IF EXISTS udb_system.audit_extra_notnull")
        .execute(&pool)
        .await
        .ok();
    sqlx::query(
        "CREATE TABLE udb_system.audit_extra_notnull ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             event_type VARCHAR(80) NOT NULL DEFAULT '', \
             tenant_id VARCHAR(64) NOT NULL DEFAULT '', \
             user_id VARCHAR(200) NOT NULL DEFAULT '', \
             correlation_id VARCHAR(120) NOT NULL DEFAULT '', \
             purpose VARCHAR(120) NOT NULL DEFAULT '', \
             resource_uri VARCHAR(400) NOT NULL DEFAULT '', \
             checksum_sha256 VARCHAR(80) NOT NULL DEFAULT '', \
             mandatory_extra TEXT NOT NULL )",
    )
    .execute(&pool)
    .await
    .expect("create table with an unpopulated NOT NULL column");

    let err = ensure_pg_audit_sink_ready(&pg_sink("udb_system.audit_extra_notnull"), Some(&pool))
        .await
        .expect_err("an unpopulated NOT NULL column must NOT report ready");
    eprintln!("extra-NOT-NULL refusal = {err}");
    assert!(
        err.contains("mandatory_extra"),
        "the refusal must name the offending column: {err}"
    );

    sqlx::query("DROP TABLE IF EXISTS udb_system.audit_extra_notnull")
        .execute(&pool)
        .await
        .ok();
}

/// The positive path: a table the sink creates itself passes readiness, and a real
/// `emit_audit` call actually PERSISTS a row rather than degrading. Guards against
/// a shape check so strict it breaks the working configuration.
#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_audit_sink_persists_to_its_own_table -- --ignored --nocapture"]
async fn live_audit_sink_persists_to_its_own_table() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(&pool)
        .await
        .expect("create udb_system schema");
    // NOTE: the writer is a process-wide `OnceLock`, so exactly ONE test may drive
    // `emit_audit` with a live pool. This is that test.
    let table = "udb_system.audit_live_ok";
    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&pool)
        .await
        .ok();

    let cfg = pg_sink(table);
    ensure_pg_audit_sink_ready(&cfg, Some(&pool))
        .await
        .expect("a table the sink creates itself must pass readiness");

    let before = degraded_count();
    emit_audit(&cfg, &sample_event("persist"), Some(&pool));
    // The writer drains off the request path.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM {table} WHERE event_type = 'upsert-persist'"
    ))
    .fetch_one(&pool)
    .await
    .unwrap_or(-1);
    assert_eq!(
        count, 1,
        "the audit event must be DURABLY stored, not printed"
    );
    assert_eq!(
        degraded_count(),
        before,
        "a healthy sink must not register any degradation"
    );

    sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
        .execute(&pool)
        .await
        .ok();
}

/// THE invariant that would have caught both occurrences. Degradation must be
/// COUNTED and readable, so it can reach the health report — previously this
/// module recorded nothing at all and a broker could lose every audit event for
/// days while reporting ready.
#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_audit_degradation_is_counted_and_readable -- --ignored --nocapture"]
async fn live_audit_degradation_is_counted_and_readable() {
    let _guard = live_native_service_db_lock().lock().await;

    // Postgres sink configured, but no pool available: the operator asked for a
    // durable trail and is not getting one. Does NOT touch the writer OnceLock.
    let before = degraded_count();
    emit_audit(
        &pg_sink("udb_system.audit_never_reached"),
        &sample_event("nopool"),
        None,
    );
    let after = degraded_count();
    assert_eq!(
        after,
        before + 1,
        "a fallback to stdout MUST be counted - being uncounted is why this defect recurred"
    );

    let (events, reason, last_unix) =
        audit_degradation_snapshot().expect("degradation must be readable once it has happened");
    assert!(events >= 1, "counter must be non-zero: {events}");
    assert_eq!(
        reason, "pg_pool_or_table_missing",
        "the snapshot must carry an actionable reason, not just a flag"
    );
    assert!(last_unix > 0, "the snapshot must carry a timestamp");
    eprintln!("degradation snapshot = ({events}, {reason}, {last_unix})");
}

/// The File sink had TWO paths that logged and returned WITHOUT writing the event
/// anywhere — a genuine silent drop, in a module documented as never dropping one.
#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_audit_file_sink_no_longer_drops_events -- --ignored --nocapture"]
async fn live_audit_file_sink_no_longer_drops_events() {
    let _guard = live_native_service_db_lock().lock().await;
    let before = degraded_count();
    // kind=file with no path configured: previously warn-and-return, event gone.
    emit_audit(
        &AuditSinkConfig {
            kind: AuditSinkKind::File,
            file_path: None,
            ..Default::default()
        },
        &sample_event("filedrop"),
        None,
    );
    assert_eq!(
        degraded_count(),
        before + 1,
        "an unconfigured file sink must degrade (counted, event to stdout), not silently drop"
    );
    assert_eq!(
        audit_degradation_snapshot()
            .map(|(_, r, _)| r)
            .unwrap_or_default(),
        "file_sink_path_unset",
        "the reason must identify which path degraded"
    );
}

/// The sibling: the auth-plane sink self-creates its table exactly the same way
/// and therefore had the identical hole — and latches `ensured = true` on success,
/// so it would never re-check.
#[tokio::test]
#[ignore = "requires live Postgres; UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_auth_audit_export_refuses_foreign_shaped_table -- --ignored --nocapture"]
async fn live_auth_audit_export_refuses_foreign_shaped_table() {
    use crate::runtime::service::auth_service::audit_export::PostgresAuditLogSink;

    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    sqlx::query("CREATE SCHEMA IF NOT EXISTS udb_system")
        .execute(&pool)
        .await
        .expect("create udb_system schema");
    // Use a fixture relation, NOT the real `auth_audit_log` other live tests write
    // to — dropping that would make this test corrupt its neighbours.
    let fixture = "udb_system.auth_audit_log_shapecheck";
    sqlx::query(&format!("DROP TABLE IF EXISTS {fixture} CASCADE"))
        .execute(&pool)
        .await
        .ok();
    sqlx::query(&format!(
        "CREATE TABLE {fixture} ( \
             audit_id BIGSERIAL PRIMARY KEY, \
             unrelated TEXT NOT NULL DEFAULT '' )"
    ))
    .execute(&pool)
    .await
    .expect("create foreign-shaped auth audit table");

    let err = PostgresAuditLogSink::new(pool.clone())
        .with_relation(fixture)
        .ensure_table()
        .await
        .expect_err("the auth-plane sink must refuse a table it cannot write to");
    eprintln!("auth-plane refusal = {err}");
    assert!(
        err.contains("event_id") || err.contains("envelope"),
        "the refusal must name a missing bound column: {err}"
    );

    // The sink's OWN shape must pass its own check — otherwise this guard would
    // break every deployment rather than only the misconfigured ones.
    sqlx::query(&format!("DROP TABLE IF EXISTS {fixture} CASCADE"))
        .execute(&pool)
        .await
        .ok();
    PostgresAuditLogSink::new(pool.clone())
        .with_relation(fixture)
        .ensure_table()
        .await
        .expect("the sink's own table shape must pass its own check");
    sqlx::query(&format!("DROP TABLE IF EXISTS {fixture} CASCADE"))
        .execute(&pool)
        .await
        .ok();
}
