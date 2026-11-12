//! Live Postgres tests for the auth event sink's request-path cost and its
//! durable audit trail:
//!
//! - an auth event written inside a transaction (`write_in_tx`) lands in the
//!   durable audit table in that same transaction (commit → row, rollback → no
//!   row), and a failed audit write refuses the event and is counted;
//! - emitting an event never runs the outbox lag query on the request path; the
//!   gauge is fed by the background sampler only.
//!
//! Each test uses its own schema (outbox + audit table), so it never touches the
//! shared `udb_system` relations other live tests write to.
//!
//! Run: UDB_LIVE_AUTH_TESTS=1 cargo test --lib events_audit_tx_live -- --ignored --nocapture

use super::super::audit_export::{AuditExportSink, PostgresAuditLogSink};
use super::super::events::{
    AuthEvent, AuthEventSink, ComplianceEnvelope, OutboxAuthEventSink, topics,
};
use super::support::*;
use crate::runtime::metrics::MetricsRecorder;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Counts the two signals these tests assert on.
#[derive(Debug, Default)]
struct CountingMetrics {
    lag_samples: AtomicUsize,
    audit_failures: AtomicUsize,
}

impl MetricsRecorder for CountingMetrics {
    crate::runtime::metrics::noop_required_metrics!();
    fn set_auth_outbox_lag_seconds(&self, _seconds: f64) {
        self.lag_samples.fetch_add(1, Ordering::SeqCst);
    }
    fn record_audit_sink_failure(&self, _sink: &str) {
        self.audit_failures.fetch_add(1, Ordering::SeqCst);
    }
}

/// A private schema holding an outbox table of the CDC shape (including the
/// `delivery_state` column the lag query reads). Returns the schema name.
async fn fresh_schema(pool: &sqlx::PgPool) -> String {
    let schema = format!("p4_auth_audit_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(pool)
        .await
        .expect("create test schema");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.outbox_events ( \
            event_seq      BIGSERIAL PRIMARY KEY, \
            event_id       UUID NOT NULL UNIQUE, \
            topic          TEXT NOT NULL, \
            partition_key  TEXT NOT NULL DEFAULT '', \
            payload        JSONB NOT NULL, \
            delivery_state TEXT NOT NULL DEFAULT 'pending', \
            created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW() )"
    ))
    .execute(pool)
    .await
    .expect("create test outbox table");
    schema
}

async fn drop_schema(pool: &sqlx::PgPool, schema: &str) {
    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(pool)
        .await;
}

/// A security-sensitive event with a complete compliance envelope, so the sink
/// accepts it in enterprise audit mode as well.
fn status_event(document_id: &str) -> AuthEvent {
    AuthEvent::new(
        topics::USER_STATUS_CHANGED,
        document_id,
        "acme",
        serde_json::json!({ "user_id": document_id, "status": "suspended" }),
    )
    .with_correlation(format!("corr-{document_id}"))
    .with_compliance(ComplianceEnvelope {
        actor: "admin-1".to_string(),
        target_resource: format!("users/{document_id}"),
        operation: "change_status".to_string(),
        outcome: "success".to_string(),
        reason_code: "admin_action".to_string(),
        ..ComplianceEnvelope::default()
    })
}

fn sink_with_audit(
    pool: &sqlx::PgPool,
    schema: &str,
    audit_relation: &str,
    metrics: Arc<CountingMetrics>,
) -> OutboxAuthEventSink {
    let audit: Arc<dyn AuditExportSink> =
        Arc::new(PostgresAuditLogSink::new(pool.clone()).with_relation(audit_relation));
    OutboxAuthEventSink::new(pool.clone(), format!("{schema}.outbox_events"))
        .with_exports(vec![audit])
        .with_metrics(metrics)
        // Long enough that no background sample can land during the test.
        .with_lag_sample_interval(Duration::from_secs(3600))
}

async fn count_where(pool: &sqlx::PgPool, sql: &str, bind: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(bind)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

/// A transactional auth event reaches the durable audit table in the same
/// transaction: N commits give N audit rows each matching its outbox row; a
/// rolled-back transaction leaves neither row behind.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_transactional_auth_event_lands_in_audit_log -- --ignored --nocapture"]
async fn live_postgres_transactional_auth_event_lands_in_audit_log() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    let schema = fresh_schema(&pool).await;
    let audit_relation = format!("{schema}.auth_audit_log");
    let metrics = Arc::new(CountingMetrics::default());
    let sink = sink_with_audit(&pool, &schema, &audit_relation, metrics.clone());
    let outbox_on = crate::runtime::cdc::cdc_delivery_enabled();

    // Repeat the action: every committed transactional event gets its row.
    const N: usize = 3;
    for i in 0..N {
        let doc = format!("user-commit-{i}");
        let mut tx = pool.begin().await.expect("begin");
        sink.write_in_tx(&mut *tx, status_event(&doc))
            .await
            .expect("transactional event write");
        tx.commit().await.expect("commit");

        let audit_rows = count_where(
            &pool,
            &format!("SELECT COUNT(*) FROM {audit_relation} WHERE target_resource = $1"),
            &format!("users/{doc}"),
        )
        .await;
        assert_eq!(
            audit_rows, 1,
            "a committed transactional auth event must have exactly one audit row ({doc})"
        );
        if outbox_on {
            // The audit row and the outbox row describe the same event.
            let joined = count_where(
                &pool,
                &format!(
                    "SELECT COUNT(*) FROM {audit_relation} a \
                     JOIN {schema}.outbox_events o ON o.event_id = a.event_id \
                     WHERE o.partition_key = $1"
                ),
                &doc,
            )
            .await;
            assert_eq!(
                joined, 1,
                "audit row must carry the outbox event id ({doc})"
            );
        }
    }

    // Negative case: a rolled-back transaction leaves no audit row and no
    // outbox row.
    let doc = "user-rollback";
    let mut tx = pool.begin().await.expect("begin");
    sink.write_in_tx(&mut *tx, status_event(doc))
        .await
        .expect("transactional event write before rollback");
    tx.rollback().await.expect("rollback");
    assert_eq!(
        count_where(
            &pool,
            &format!("SELECT COUNT(*) FROM {audit_relation} WHERE target_resource = $1"),
            &format!("users/{doc}"),
        )
        .await,
        0,
        "a rolled-back transaction must leave no audit row"
    );
    assert_eq!(
        count_where(
            &pool,
            &format!("SELECT COUNT(*) FROM {schema}.outbox_events WHERE partition_key = $1"),
            doc,
        )
        .await,
        0,
        "a rolled-back transaction must leave no outbox row"
    );
    assert_eq!(metrics.audit_failures.load(Ordering::SeqCst), 0);

    drop_schema(&pool, &schema).await;
}

/// A transactional event whose audit write cannot land is refused (so the
/// caller's mutation rolls back), counted in `audit_sink_failures`, and leaves
/// no outbox row behind.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_transactional_audit_failure_refuses_event -- --ignored --nocapture"]
async fn live_postgres_transactional_audit_failure_refuses_event() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    let schema = fresh_schema(&pool).await;
    // A pre-existing audit table of the wrong shape: the sink's column check
    // must reject it rather than drop the audit record.
    let audit_relation = format!("{schema}.auth_audit_log");
    sqlx::query(&format!(
        "CREATE TABLE {audit_relation} (audit_id UUID PRIMARY KEY)"
    ))
    .execute(&pool)
    .await
    .expect("create wrong-shaped audit table");
    let metrics = Arc::new(CountingMetrics::default());
    let sink = sink_with_audit(&pool, &schema, &audit_relation, metrics.clone());

    let doc = "user-audit-broken";
    let mut tx = pool.begin().await.expect("begin");
    let result = sink.write_in_tx(&mut *tx, status_event(doc)).await;
    assert!(
        result.is_err(),
        "an event whose audit row cannot be written must be refused"
    );
    tx.rollback().await.expect("rollback");
    assert_eq!(
        metrics.audit_failures.load(Ordering::SeqCst),
        1,
        "the refused audit write must be counted"
    );
    assert_eq!(
        count_where(
            &pool,
            &format!("SELECT COUNT(*) FROM {schema}.outbox_events WHERE partition_key = $1"),
            doc,
        )
        .await,
        0,
        "a refused event must leave no outbox row"
    );

    drop_schema(&pool, &schema).await;
}

/// Emitting events (both paths) never runs the outbox lag query on the request
/// path; a background sampler still feeds the gauge.
#[tokio::test]
#[ignore = "requires live Postgres; run with UDB_LIVE_AUTH_TESTS=1 cargo test --lib live_postgres_auth_emit_does_not_sample_lag_on_request_path -- --ignored --nocapture"]
async fn live_postgres_auth_emit_does_not_sample_lag_on_request_path() {
    let _guard = live_auth_db_lock().lock().await;
    let pool = live_pg_pool().await;
    let schema = fresh_schema(&pool).await;
    let audit_relation = format!("{schema}.auth_audit_log");

    // Sampler period of an hour: any lag sample seen during the test could only
    // have come from the request path.
    let metrics = Arc::new(CountingMetrics::default());
    let sink = sink_with_audit(&pool, &schema, &audit_relation, metrics.clone());
    const N: usize = 5;
    for i in 0..N {
        sink.emit(status_event(&format!("user-emit-{i}")))
            .await
            .expect("emit");
        let mut tx = pool.begin().await.expect("begin");
        sink.write_in_tx(&mut *tx, status_event(&format!("user-tx-{i}")))
            .await
            .expect("write_in_tx");
        tx.commit().await.expect("commit");
    }
    assert_eq!(
        metrics.lag_samples.load(Ordering::SeqCst),
        0,
        "no event emit may run the outbox lag query on the request path"
    );
    // Every one of the 2N events still has its durable audit row.
    let audit_rows: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {audit_relation}"))
        .fetch_one(&pool)
        .await
        .expect("count audit rows");
    assert_eq!(
        audit_rows,
        (2 * N) as i64,
        "every event must have an audit row"
    );

    // The gauge is still fed — by the background sampler, off the request path.
    let sampled_metrics = Arc::new(CountingMetrics::default());
    let sampled = OutboxAuthEventSink::new(pool.clone(), format!("{schema}.outbox_events"))
        .with_metrics(sampled_metrics.clone())
        .with_lag_sample_interval(Duration::from_millis(100));
    sampled
        .emit(status_event("user-sampled"))
        .await
        .expect("emit on sampled sink");
    let mut seen = false;
    for _ in 0..50 {
        if sampled_metrics.lag_samples.load(Ordering::SeqCst) > 0 {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        seen,
        "the background sampler must set the auth outbox lag gauge"
    );

    drop(sampled);
    drop(sink);
    drop_schema(&pool, &schema).await;
}
