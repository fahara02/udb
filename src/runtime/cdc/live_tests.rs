//! Env-gated live-Postgres tests for the CDC delivery state machine.
//!
//! In-memory doubles are forbidden — these tests drive the REAL serving-path
//! functions (`indoubt_recovery::reset_indoubt_publishing_row`,
//! `CdcEngine::route_to_dlq`, `CdcEngine::run_journal_retention_sweep`)
//! against a live Postgres, following the repo's env-gated live-test pattern
//! (see `runtime::authn::tests`). They are `#[ignore]`d so the default
//! `cargo test` needs no database; run them with:
//!
//! ```text
//! UDB_LIVE_CDC_TESTS=1 cargo test --lib cdc::live_tests -- --ignored --nocapture
//! ```
//!
//! or point at a specific database with `UDB_LIVE_CDC_PG_DSN=postgres://…`.

use super::*;

const LIVE_GATE_HINT: &str = "skipping: set UDB_LIVE_CDC_TESTS=1 (or UDB_LIVE_CDC_PG_DSN=postgres://…) to run live CDC tests";

fn live_pg_dsn() -> Option<String> {
    std::env::var("UDB_LIVE_CDC_PG_DSN")
        .or_else(|_| std::env::var("UDB_LIVE_NATIVE_PG_DSN"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_PG_DSN"))
        .ok()
        .or_else(|| {
            std::env::var("UDB_LIVE_CDC_TESTS")
                .ok()
                .filter(|v| matches!(v.as_str(), "1" | "true" | "yes"))
                .map(|_| "postgres://udb:udb@localhost:55432/udb".to_string())
        })
}

/// Serialize the live CDC tests — they share the `udb_system` catalog tables.
fn live_cdc_db_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

/// Connect to the live Postgres and bootstrap the REAL UDB system catalog
/// (outbox, journal, DLQ, …) with the production DDL, or `None` when no live
/// DB is configured.
async fn live_cdc_pool() -> Option<PgPool> {
    let dsn = live_pg_dsn()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&dsn)
        .await
        .unwrap_or_else(|err| panic!("connect live CDC postgres at {dsn}: {err}"));
    crate::runtime::system::ensure_system_catalog(&pool)
        .await
        .expect("ensure live UDB system catalog");
    Some(pool)
}

async fn insert_outbox_publishing_row(
    pool: &PgPool,
    event_id: Uuid,
    producer_epoch: i64,
    kafka_offset: Option<i64>,
) {
    sqlx::query(
        "INSERT INTO udb_system.outbox_events \
         (event_id, topic, partition_key, payload, delivery_state, publishing_started_at, producer_epoch, kafka_offset) \
         VALUES ($1, 'udb.cdc.live.indoubt.v1', 'live-test', '{}'::JSONB, 'publishing', NOW(), $2, $3)",
    )
    .bind(event_id)
    .bind(producer_epoch)
    .bind(kafka_offset)
    .execute(pool)
    .await
    .expect("insert publishing outbox row");
}

async fn outbox_delivery_state(pool: &PgPool, event_id: Uuid) -> String {
    sqlx::query_scalar(
        "SELECT delivery_state FROM udb_system.outbox_events WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await
    .expect("read outbox delivery_state")
}

async fn delete_outbox_rows(pool: &PgPool, event_ids: &[Uuid]) {
    sqlx::query("DELETE FROM udb_system.outbox_events WHERE event_id = ANY($1)")
        .bind(event_ids)
        .execute(pool)
        .await
        .expect("clean up live outbox rows");
}

/// FIX-9C: the real per-event in-doubt reset — the exact function
/// `CdcEngine::reset_timed_out_publishing_row` calls when a delivery-timeout
/// drops an in-flight publish future — moves a `publishing` row back to
/// `pending` (no recorded offset), finalizes a commit-confirmed row as
/// `acked` (offset recorded), and never touches a row owned by a different
/// producer epoch. The periodic sweep (`reset_indoubt_publishing_rows`) then
/// recovers the prior-epoch row with matching semantics.
#[tokio::test]
#[ignore = "requires live Postgres: UDB_LIVE_CDC_TESTS=1 cargo test --lib cdc::live_tests -- --ignored --nocapture"]
async fn live_indoubt_reset_returns_publishing_row_to_pending() {
    let _guard = live_cdc_db_lock().lock().await;
    let Some(pool) = live_cdc_pool().await else {
        eprintln!("{LIVE_GATE_HINT}");
        return;
    };
    let outbox_relation = CdcConfig::default().outbox_relation();
    let current_epoch = 7_i64;

    let dropped_id = Uuid::new_v4(); // in-flight future dropped, no offset
    let committed_id = Uuid::new_v4(); // commit confirmed before the drop
    let prior_epoch_id = Uuid::new_v4(); // owned by an older producer epoch
    insert_outbox_publishing_row(&pool, dropped_id, current_epoch, None).await;
    insert_outbox_publishing_row(&pool, committed_id, current_epoch, Some(42)).await;
    insert_outbox_publishing_row(&pool, prior_epoch_id, current_epoch - 1, None).await;

    // The publish-unproven row transitions publishing -> pending.
    let outcome = super::indoubt_recovery::reset_indoubt_publishing_row(
        &pool,
        &outbox_relation,
        dropped_id,
        current_epoch,
    )
    .await
    .expect("per-event in-doubt reset");
    assert_eq!(outcome, super::indoubt_recovery::IndoubtRowOutcome::Pending);
    assert_eq!(outbox_delivery_state(&pool, dropped_id).await, "pending");
    let started_at: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT publishing_started_at FROM udb_system.outbox_events WHERE event_id = $1",
    )
    .bind(dropped_id)
    .fetch_one(&pool)
    .await
    .expect("read publishing_started_at");
    assert!(
        started_at.is_none(),
        "pending reset must clear publishing_started_at"
    );

    // A recorded kafka_offset is commit proof: finalize as acked, never
    // re-publish (would duplicate).
    let outcome = super::indoubt_recovery::reset_indoubt_publishing_row(
        &pool,
        &outbox_relation,
        committed_id,
        current_epoch,
    )
    .await
    .expect("per-event in-doubt ack");
    assert_eq!(outcome, super::indoubt_recovery::IndoubtRowOutcome::Acked);
    assert_eq!(outbox_delivery_state(&pool, committed_id).await, "acked");

    // A row stamped with a different producer epoch is not this caller's to
    // reset — the epoch-aware sweep owns it.
    let outcome = super::indoubt_recovery::reset_indoubt_publishing_row(
        &pool,
        &outbox_relation,
        prior_epoch_id,
        current_epoch,
    )
    .await
    .expect("per-event reset of foreign-epoch row");
    assert_eq!(
        outcome,
        super::indoubt_recovery::IndoubtRowOutcome::Untouched
    );
    assert_eq!(
        outbox_delivery_state(&pool, prior_epoch_id).await,
        "publishing"
    );

    // …and the startup/periodic sweep recovers it with the same
    // ack-vs-pending decision (no offset recorded -> pending).
    let swept = super::indoubt_recovery::reset_indoubt_publishing_rows(
        &pool,
        &outbox_relation,
        current_epoch,
        300,
    )
    .await
    .expect("epoch sweep");
    assert!(swept >= 1, "sweep should reset the prior-epoch row");
    assert_eq!(
        outbox_delivery_state(&pool, prior_epoch_id).await,
        "pending"
    );

    delete_outbox_rows(&pool, &[dropped_id, committed_id, prior_epoch_id]).await;
}

/// Build a CdcEngine for live tests. The broker address is unroutable on
/// purpose: rdkafka connects lazily and the paths under test fail (or
/// finish) before any Kafka I/O is needed.
#[cfg(feature = "kafka")]
fn live_engine(pool: PgPool, dsn: String, config: CdcConfig) -> CdcEngine {
    let metrics: std::sync::Arc<dyn MetricsRecorder> =
        std::sync::Arc::new(crate::metrics::NoopMetrics);
    #[cfg(feature = "redis")]
    {
        CdcEngine::new(pool, None, "127.0.0.1:1", dsn, metrics, config)
            .expect("build live CDC engine")
    }
    #[cfg(not(feature = "redis"))]
    {
        CdcEngine::new(pool, "127.0.0.1:1", dsn, metrics, config).expect("build live CDC engine")
    }
}

/// Item 25: `route_to_dlq`'s failure path. When the durable Postgres DLQ
/// insert fails, the event must NOT be acked away — `route_to_dlq` returns
/// `false` and resets the outbox row to `pending` so the next poll retries
/// it after the operator fixes the DLQ. This calls the real
/// `CdcEngine::route_to_dlq`, forcing the insert failure by hiding the DLQ
/// table for the duration of the call.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires live Postgres: UDB_LIVE_CDC_TESTS=1 cargo test --lib cdc::live_tests -- --ignored --nocapture"]
async fn live_route_to_dlq_insert_failure_resets_row_to_pending() {
    let _guard = live_cdc_db_lock().lock().await;
    let Some(pool) = live_cdc_pool().await else {
        eprintln!("{LIVE_GATE_HINT}");
        return;
    };
    let dsn = live_pg_dsn().expect("live dsn present when pool connected");
    // StateMachine mode so delivery_state transitions are tracked (the
    // 'pending' reset under test is a state-machine write).
    let config = CdcConfig {
        exactly_once_mode: CdcExactlyOnceMode::StateMachine,
        ..CdcConfig::default()
    };
    let engine = live_engine(pool.clone(), dsn, config);

    let event_id = Uuid::new_v4();
    insert_outbox_publishing_row(&pool, event_id, 0, None).await;

    // Force the durable DLQ insert to fail by renaming the table away for
    // the duration of the call (clean any leftover from an aborted run
    // first; `ensure_system_catalog` above recreated the canonical table).
    sqlx::query("DROP TABLE IF EXISTS udb_system.udb_cdc_dlq_events_hidden_live_test")
        .execute(&pool)
        .await
        .expect("drop leftover hidden DLQ table");
    sqlx::query(
        "ALTER TABLE udb_system.udb_cdc_dlq_events RENAME TO udb_cdc_dlq_events_hidden_live_test",
    )
    .execute(&pool)
    .await
    .expect("hide DLQ table");

    let routed = engine
        .route_to_dlq(
            event_id,
            serde_json::json!({
                "event_id": event_id.to_string(),
                "event_type": "udb.cdc.live.dlq.v1",
                "tenant_id": "live-test",
            }),
            "LiveTestForcedFailure",
            "live test: DLQ table hidden to force the insert failure",
        )
        .await;

    // Restore the table before asserting so a failed assert can't leave the
    // shared catalog broken for the next test.
    sqlx::query(
        "ALTER TABLE udb_system.udb_cdc_dlq_events_hidden_live_test RENAME TO udb_cdc_dlq_events",
    )
    .execute(&pool)
    .await
    .expect("restore DLQ table");

    assert!(
        !routed,
        "route_to_dlq must report failure when the durable DLQ insert fails"
    );
    assert_eq!(
        outbox_delivery_state(&pool, event_id).await,
        "pending",
        "a DLQ-routing failure must return the row to 'pending' for retry"
    );

    delete_outbox_rows(&pool, &[event_id]).await;
}

/// Item 26: the real `run_journal_retention_sweep` against a live journal.
/// Only `acked` rows older than `idempotency_ttl_secs` are pruned; fresh
/// acked rows and unacked `published`/`dlq` rows (durable replay/operator
/// evidence) survive. Complements the SQL-shape unit test in `engine_tail`.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires live Postgres: UDB_LIVE_CDC_TESTS=1 cargo test --lib cdc::live_tests -- --ignored --nocapture"]
async fn live_journal_retention_sweep_removes_only_old_acked_rows() {
    let _guard = live_cdc_db_lock().lock().await;
    let Some(pool) = live_cdc_pool().await else {
        eprintln!("{LIVE_GATE_HINT}");
        return;
    };
    let dsn = live_pg_dsn().expect("live dsn present when pool connected");
    let config = CdcConfig {
        idempotency_ttl_secs: 3_600, // 1h TTL: "old" rows are 2h past
        ..CdcConfig::default()
    };
    let engine = live_engine(pool.clone(), dsn, config);

    async fn insert_journal_row(pool: &PgPool, event_id: Uuid, state: &str, age_secs: f64) {
        sqlx::query(
            "INSERT INTO udb_system.udb_cdc_event_journal \
             (event_id, topic, partition_key, payload, published_at, delivery_state) \
             VALUES ($1, 'udb.cdc.live.retention.v1', 'live-test', '{}'::JSONB, \
                     NOW() - make_interval(secs => $2), $3)",
        )
        .bind(event_id)
        .bind(age_secs)
        .bind(state)
        .execute(pool)
        .await
        .expect("insert live journal row");
    }
    async fn journal_row_count(pool: &PgPool, event_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM udb_system.udb_cdc_event_journal WHERE event_id = $1",
        )
        .bind(event_id)
        .fetch_one(pool)
        .await
        .expect("count live journal row")
    }

    let old_acked = Uuid::new_v4();
    let fresh_acked = Uuid::new_v4();
    let old_published = Uuid::new_v4();
    let old_dlq = Uuid::new_v4();
    insert_journal_row(&pool, old_acked, "acked", 7_200.0).await;
    insert_journal_row(&pool, fresh_acked, "acked", 0.0).await;
    insert_journal_row(&pool, old_published, "published", 7_200.0).await;
    insert_journal_row(&pool, old_dlq, "dlq", 7_200.0).await;

    engine.run_journal_retention_sweep().await;

    assert_eq!(
        journal_row_count(&pool, old_acked).await,
        0,
        "acked row older than the TTL must be pruned"
    );
    assert_eq!(
        journal_row_count(&pool, fresh_acked).await,
        1,
        "acked row inside the TTL must survive"
    );
    assert_eq!(
        journal_row_count(&pool, old_published).await,
        1,
        "unacked 'published' rows are durable evidence and survive retention"
    );
    assert_eq!(
        journal_row_count(&pool, old_dlq).await,
        1,
        "'dlq' rows are durable operator evidence and survive retention"
    );

    sqlx::query("DELETE FROM udb_system.udb_cdc_event_journal WHERE event_id = ANY($1)")
        .bind([old_acked, fresh_acked, old_published, old_dlq].as_slice())
        .execute(&pool)
        .await
        .expect("clean up live journal rows");
}
