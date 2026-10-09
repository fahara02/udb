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
    sqlx::query_scalar("SELECT delivery_state FROM udb_system.outbox_events WHERE event_id = $1")
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
        None,
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

/// The cross-replica LiveQuery tail: `journal_head_event_id` anchors a new
/// subscriber at the journal head, and `journal_scan_for_scope` then returns
/// only the subscriber's tenant's LATER events while advancing the cursor past
/// every scanned row (foreign-tenant rows included), so a busy shared topic
/// never pins the cursor.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires live Postgres: UDB_LIVE_CDC_TESTS=1 cargo test --lib cdc::live_tests -- --ignored --nocapture"]
async fn live_journal_tail_anchors_at_head_and_scopes_by_tenant() {
    let _guard = live_cdc_db_lock().lock().await;
    let Some(pool) = live_cdc_pool().await else {
        eprintln!("{LIVE_GATE_HINT}");
        return;
    };
    let dsn = live_pg_dsn().expect("live dsn present when pool connected");
    let engine = live_engine(pool.clone(), dsn, CdcConfig::default());
    let topic = format!("udb.cdc.live.tail.{}.v1", Uuid::new_v4().simple());

    async fn journal(pool: &PgPool, topic: &str, tenant: &str, age_secs: f64) -> Uuid {
        let event_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO udb_system.udb_cdc_event_journal \
             (event_id, topic, partition_key, payload, published_at, delivery_state) \
             VALUES ($1, $2, 'live-test', $3::JSONB, \
                     NOW() - make_interval(secs => $4), 'published')",
        )
        .bind(event_id)
        .bind(topic)
        .bind(
            serde_json::json!({"tenant_id": tenant, "project_id": "default", "payload": {}})
                .to_string(),
        )
        .bind(age_secs)
        .execute(pool)
        .await
        .expect("insert live journal row");
        event_id
    }

    // History from before the subscription.
    let before = journal(&pool, &topic, "tenant-a", 60.0).await;
    let head = engine
        .journal_head_event_id(&topic)
        .await
        .expect("journal head readable")
        .expect("topic has a head");
    assert_eq!(head, before.to_string());

    // After subscribing: one foreign-tenant event, then the subscriber's own.
    let foreign = journal(&pool, &topic, "tenant-b", 30.0).await;
    let own = journal(&pool, &topic, "tenant-a", 10.0).await;

    let (events, last_scanned) = engine
        .journal_scan_for_scope(&topic, "tenant-a", "default", &head, 100)
        .await;
    let ids: Vec<_> = events.iter().map(|e| e.event_id.clone()).collect();
    assert_eq!(
        ids,
        vec![own.to_string()],
        "only tenant-a's post-head event"
    );
    assert_eq!(last_scanned, Some(own.to_string()));

    // Polling again from the advanced cursor yields nothing new.
    let (again, unchanged) = engine
        .journal_scan_for_scope(&topic, "tenant-a", "default", &own.to_string(), 100)
        .await;
    assert!(again.is_empty());
    assert_eq!(unchanged, None);

    sqlx::query("DELETE FROM udb_system.udb_cdc_event_journal WHERE event_id = ANY($1)")
        .bind([before, foreign, own].as_slice())
        .execute(&pool)
        .await
        .expect("clean up live journal rows");
}

/// Brokers for the live tail test: CI's live lane exports
/// `UDB_INTEGRATION_KAFKA_BROKERS`.
#[cfg(feature = "kafka")]
fn live_kafka_brokers() -> String {
    std::env::var("UDB_INTEGRATION_KAFKA_BROKERS")
        .or_else(|_| std::env::var("UDB_KAFKA_BROKERS"))
        .unwrap_or_else(|_| "localhost:59192".to_string())
}

/// Create `topic` (1 partition, RF 1) so the produce does not depend on broker
/// auto-creation.
#[cfg(feature = "kafka")]
async fn ensure_live_kafka_topic(brokers: &str, topic: &str) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;

    let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap_or_else(|err| panic!("create Kafka admin client for {brokers}: {err}"));
    match admin
        .create_topics(
            &[NewTopic::new(topic, 1, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
    {
        Ok(results) => {
            for result in results {
                if let Err((name, code)) = result
                    && !format!("{code:?}").contains("TopicAlreadyExists")
                {
                    panic!("create Kafka topic {name} failed: {code:?}");
                }
            }
        }
        Err(err) => panic!("create Kafka topic {topic} request failed: {err}"),
    }
}

/// D14: the production tail loop itself — `CdcEngine::tail_outbox`, the relay
/// the leader runs — drains a PENDING outbox row to Kafka, journals it (the
/// replay / cross-replica LiveQuery source) and acks the outbox row. Until
/// this test only the per-event helpers were exercised, never the loop.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires live Postgres + Kafka (CI live lane: UDB_INTEGRATION_PG_DSN + UDB_INTEGRATION_KAFKA_BROKERS) -- --ignored"]
async fn live_tail_outbox_publishes_journals_and_acks_a_pending_row() {
    let _guard = live_cdc_db_lock().lock().await;
    let Some(pool) = live_cdc_pool().await else {
        if std::env::var("UDB_LIVE_AUTH_TESTS").is_ok_and(|v| v.trim() == "1") {
            panic!(
                "UDB_LIVE_AUTH_TESTS=1 but no live CDC Postgres DSN is set \
                 (UDB_LIVE_CDC_PG_DSN / UDB_LIVE_NATIVE_PG_DSN / UDB_INTEGRATION_PG_DSN)"
            );
        }
        eprintln!("{LIVE_GATE_HINT}");
        return;
    };
    let dsn = live_pg_dsn().expect("live dsn present when pool connected");
    let brokers = live_kafka_brokers();
    let topic = format!("udb.cdc.live.tail.{}.v1", Uuid::new_v4().simple());
    ensure_live_kafka_topic(&brokers, &topic).await;

    // The tail only publishes topics the active topic policy allows once any
    // policy exists (other live tests leave some behind), so allow this one.
    let policy_relation =
        crate::runtime::system::SystemCatalogConfig::current().topic_policy_relation();
    sqlx::query(&format!(
        "INSERT INTO {policy_relation} (topic, owning_project, owning_service, schema_uri, enabled) \
         VALUES ($1, 'default', 'cdc-live-tail', '', TRUE) \
         ON CONFLICT (topic) DO UPDATE SET enabled = TRUE, updated_at = NOW()"
    ))
    .bind(&topic)
    .execute(&pool)
    .await
    .expect("allow the live tail topic");

    let config = CdcConfig::default();
    let outbox = config.outbox_relation();
    let metrics: std::sync::Arc<dyn MetricsRecorder> =
        std::sync::Arc::new(crate::metrics::NoopMetrics);
    #[cfg(feature = "redis")]
    let engine = CdcEngine::new(pool.clone(), None, &brokers, dsn, metrics, config)
        .expect("build live CDC engine against real Kafka");
    #[cfg(not(feature = "redis"))]
    let engine = CdcEngine::new(pool.clone(), &brokers, dsn, metrics, config)
        .expect("build live CDC engine against real Kafka");
    engine
        .load_topic_policies()
        .await
        .expect("load live topic policies");
    let engine = std::sync::Arc::new(engine);

    let event_id = Uuid::new_v4();
    let payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "event_type": topic,
        "correlation_id": format!("cdc-tail:{event_id}"),
        "document_id": "row-1",
        "tenant_id": "tenant-a",
        "project_id": "default",
        "timestamp": Utc::now().to_rfc3339(),
        "payload": {"id": "row-1", "tenant_id": "tenant-a"}
    });
    sqlx::query(&format!(
        "INSERT INTO {outbox} (event_id, topic, partition_key, payload, created_at) \
         VALUES ($1, $2, 'row-1', $3::JSONB, NOW())"
    ))
    .bind(event_id)
    .bind(&topic)
    .bind(payload.to_string())
    .execute(&pool)
    .await
    .expect("insert pending outbox row");

    let tailer = {
        let engine = engine.clone();
        tokio::spawn(async move {
            if let Err(err) = engine.tail_outbox().await {
                eprintln!("tail_outbox exited: {err}");
            }
        })
    };

    let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let (journal_state, outbox_state) = loop {
        let journal_state: Option<String> = sqlx::query_scalar(&format!(
            "SELECT delivery_state FROM {journal} WHERE event_id = $1"
        ))
        .bind(event_id)
        .fetch_optional(&pool)
        .await
        .expect("read journal row");
        let outbox_state: Option<String> = sqlx::query_scalar(&format!(
            "SELECT delivery_state FROM {outbox} WHERE event_id = $1"
        ))
        .bind(event_id)
        .fetch_optional(&pool)
        .await
        .expect("read outbox row");
        let acked = outbox_state
            .as_deref()
            .is_none_or(|state| matches!(state, "published" | "acked"));
        if (journal_state.is_some() && acked) || std::time::Instant::now() >= deadline {
            break (journal_state, outbox_state);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    };
    tailer.abort();

    let dlq: Option<String> = sqlx::query_scalar(
        "SELECT error_message FROM udb_system.udb_cdc_dlq_events WHERE event_id = $1",
    )
    .bind(event_id)
    .fetch_optional(&pool)
    .await
    .unwrap_or(None);
    assert_eq!(dlq, None, "the tail routed the event to the DLQ");
    assert!(
        matches!(journal_state.as_deref(), Some("published" | "acked")),
        "tail_outbox must journal the published event, got journal {journal_state:?} \
         (outbox state: {outbox_state:?})"
    );
    assert!(
        outbox_state
            .as_deref()
            .is_none_or(|state| matches!(state, "published" | "acked")),
        "tail_outbox must ack the outbox row after publishing, got {outbox_state:?}"
    );

    let _ = sqlx::query(&format!("DELETE FROM {outbox} WHERE event_id = $1"))
        .bind(event_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DELETE FROM {journal} WHERE event_id = $1"))
        .bind(event_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query(&format!("DELETE FROM {policy_relation} WHERE topic = $1"))
        .bind(&topic)
        .execute(&pool)
        .await;
}

mod journal_prefix_controls {
    use super::*;
    use crate::runtime::system::SystemCatalogConfig;
    use sqlx::Executor as _;

    async fn legacy_catalog(pool: &PgPool) -> SystemCatalogConfig {
        let schema = format!("prefix_{}$udb_prefix$'\"", Uuid::new_v4().simple());
        let mut config = SystemCatalogConfig::with_schema(&schema);
        config.cdc_journal_table = "journal$udb_allocate$'\".history".into();
        sqlx::query(&format!("CREATE SCHEMA {}", qi(&schema)))
            .execute(pool)
            .await
            .unwrap();
        for relation in [
            config.cdc_journal_relation(),
            config.cdc_consumer_cursors_relation(),
        ] {
            let create = format!("CREATE TABLE IF NOT EXISTS {relation} (");
            let statements = config.system_catalog_ddl();
            let sql = statements
                .iter()
                .find(|stmt| stmt.starts_with(&create))
                .expect("canonical legacy table DDL");
            pool.execute(sql.as_str()).await.unwrap();
        }
        config
    }
    async fn migrate(pool: &PgPool, config: &SystemCatalogConfig) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;
        for statement in super::super::journal::schema_statements(config) {
            tx.execute(statement.as_str()).await?;
        }
        tx.commit().await
    }
    async fn remove(pool: &PgPool, config: &SystemCatalogConfig) {
        pool.execute(format!("DROP SCHEMA {} CASCADE", qi(&config.cdc.system_schema)).as_str())
            .await
            .unwrap();
    }
    async fn publish(
        pool: &PgPool,
        config: &SystemCatalogConfig,
        id: Uuid,
    ) -> super::super::journal::JournalEntry {
        let mut tx = pool.begin().await.unwrap();
        let entry = super::super::journal::insert(
            &mut tx,
            config,
            id,
            "udb.prefix.control",
            "fixture",
            "{\"tenant_id\":\"prefix-control\",\"project_id\":\"default\"}",
            None,
            None,
            0,
            "",
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        entry
    }

    #[tokio::test]
    #[ignore = "requires live Postgres; runs in native CI"]
    async fn live_journal_prefix_backfills_quoted_catalog_and_preserves_retry_retention() {
        let _guard = live_cdc_db_lock().lock().await;
        let pool = live_cdc_pool()
            .await
            .expect("live PostgreSQL fixture is mandatory");
        let config = legacy_catalog(&pool).await;
        let journal = config.cdc_journal_relation();
        let cursors = config.cdc_consumer_cursors_relation();
        // Missing retained IDs must exercise both sides of the UUID tie-break.
        let before = Uuid::from_u128(1);
        let a = Uuid::from_u128(2);
        let b = Uuid::from_u128(3);
        let missing = Uuid::from_u128(4);
        for (id, second) in [(b, 2i32), (a, 1)] {
            sqlx::query(&format!("INSERT INTO {journal} (event_id,topic,payload,published_at) VALUES ($1,'udb.prefix.control','{{}}','2026-01-01'::TIMESTAMPTZ + make_interval(secs=>$2::DOUBLE PRECISION))"))
                .bind(id).bind(f64::from(second)).execute(&pool).await.unwrap();
        }
        for (name, id, second, owner) in [
            ("retained", b, 2i32, "user:canonical-ci"),
            ("pruned", missing, 1, "user:canonical-ci"),
            ("pruned-before", before, 1, "user:canonical-ci"),
            ("legacy", a, 1, ""),
        ] {
            sqlx::query(&format!("INSERT INTO {cursors} (tenant_id,project_id,consumer_name,topic_pattern,last_event_id,owner_identity,last_published_at) VALUES ('prefix-control','default',$1,'udb.prefix.control',$2,$3,'2026-01-01'::TIMESTAMPTZ + make_interval(secs=>$4::DOUBLE PRECISION))"))
                .bind(name).bind(id).bind(owner).bind(f64::from(second)).execute(&pool).await.unwrap();
        }
        migrate(&pool, &config)
            .await
            .expect("canonical quoted legacy migration");
        let positions: Vec<(Uuid, i64)> = sqlx::query_as(&format!(
            "SELECT event_id,journal_position FROM {journal} ORDER BY journal_position"
        ))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            positions,
            vec![(a, 1), (b, 2)],
            "legacy backfill preserves stable prior time/UUID order"
        );
        let owner_positions: Vec<(String,i64,String)> = sqlx::query_as(&format!("SELECT consumer_name,last_journal_position,owner_identity FROM {cursors} ORDER BY consumer_name"))
            .fetch_all(&pool).await.unwrap();
        assert_eq!(
            owner_positions,
            vec![
                ("legacy".into(), 1, "".into()),
                ("pruned".into(), 1, "user:canonical-ci".into()),
                ("pruned-before".into(), 0, "user:canonical-ci".into()),
                ("retained".into(), 2, "user:canonical-ci".into())
            ]
        );
        let original: DateTime<Utc> = sqlx::query_scalar(&format!(
            "SELECT published_at FROM {journal} WHERE event_id=$1"
        ))
        .bind(a)
        .fetch_one(&pool)
        .await
        .unwrap();
        let retried = publish(&pool, &config, a).await;
        assert_eq!(
            retried.position, 1,
            "retry UUID must retain immutable publication position"
        );
        assert_eq!(
            retried.envelope.published_at, original,
            "retry must retain the actual original timestamp"
        );
        let invalid = sqlx::query(&format!(
            "UPDATE {journal} SET journal_position=journal_position+100 WHERE event_id=$1"
        ))
        .bind(a)
        .execute(&pool)
        .await;
        assert!(
            invalid.is_err(),
            "database rejects moving a retained event behind/above a durable cursor"
        );
        let id = Uuid::new_v4();
        let mut tx = pool.begin().await.unwrap();
        let rolled = super::super::journal::insert(
            &mut tx,
            &config,
            id,
            "udb.prefix.control",
            "fixture",
            "{}",
            None,
            None,
            0,
            "",
        )
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let absent: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {journal} WHERE event_id=$1"))
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            absent, 0,
            "rolled back publication is never retained evidence"
        );
        let committed = publish(&pool, &config, id).await;
        assert_eq!(
            committed.position, rolled.position,
            "transactional head rolls back with the unpublished row"
        );
        let head = super::super::journal::head(&pool, &config).await.unwrap();
        pool.execute(format!("DELETE FROM {journal}").as_str())
            .await
            .unwrap();
        pool.execute(format!("DELETE FROM {cursors}").as_str())
            .await
            .unwrap();
        migrate(&pool, &config).await.unwrap();
        assert_eq!(
            super::super::journal::head(&pool, &config).await.unwrap(),
            head,
            "empty retention must preserve the durable head"
        );
        assert!(publish(&pool, &config, Uuid::new_v4()).await.position > head);
        remove(&pool, &config).await;
    }

    #[tokio::test]
    #[ignore = "requires live Postgres; runs in native CI"]
    async fn live_journal_prefix_refuses_wrong_storage_and_missing_modern_head() {
        let _guard = live_cdc_db_lock().lock().await;
        let pool = live_cdc_pool()
            .await
            .expect("live PostgreSQL fixture is mandatory");
        for case in [
            "journal-type",
            "cursor-type",
            "head-type",
            "head-key",
            "head-negative",
            "head-behind",
            "position-index",
        ] {
            let config = legacy_catalog(&pool).await;
            let journal = config.cdc_journal_relation();
            let cursors = config.cdc_consumer_cursors_relation();
            let heads = format!(
                "{}.{}",
                qi(&config.cdc.system_schema),
                qi("udb_cdc_journal_heads")
            );
            match case {
                "journal-type" => {
                    pool.execute(
                        format!("ALTER TABLE {journal} ADD COLUMN journal_position INTEGER")
                            .as_str(),
                    )
                    .await
                    .unwrap();
                }
                "cursor-type" => {
                    pool.execute(
                        format!("ALTER TABLE {cursors} ADD COLUMN last_journal_position NUMERIC")
                            .as_str(),
                    )
                    .await
                    .unwrap();
                }
                "head-type" => {
                    pool.execute(format!("CREATE TABLE {heads} (journal_schema TEXT NOT NULL,journal_table TEXT NOT NULL,last_position INTEGER NOT NULL,PRIMARY KEY(journal_schema,journal_table))").as_str()).await.unwrap();
                }
                "head-key" => {
                    pool.execute(format!("CREATE TABLE {heads} (journal_schema TEXT NOT NULL,journal_table TEXT NOT NULL,last_position BIGINT NOT NULL)").as_str()).await.unwrap();
                }
                "head-negative" => {
                    sqlx::query(&format!("CREATE TABLE {heads} (journal_schema TEXT NOT NULL,journal_table TEXT NOT NULL,last_position BIGINT NOT NULL,PRIMARY KEY(journal_schema,journal_table))"))
                        .execute(&pool).await.unwrap();
                    sqlx::query(&format!("INSERT INTO {heads} (journal_schema,journal_table,last_position) VALUES ($1,$2,-1)"))
                        .bind(&config.cdc.system_schema).bind(&config.cdc_journal_table).execute(&pool).await.unwrap();
                }
                "head-behind" => {
                    sqlx::query(&format!("CREATE TABLE {heads} (journal_schema TEXT NOT NULL,journal_table TEXT NOT NULL,last_position BIGINT NOT NULL,PRIMARY KEY(journal_schema,journal_table))"))
                        .execute(&pool).await.unwrap();
                    sqlx::query(&format!("INSERT INTO {heads} (journal_schema,journal_table,last_position) VALUES ($1,$2,0)"))
                        .bind(&config.cdc.system_schema).bind(&config.cdc_journal_table).execute(&pool).await.unwrap();
                    pool.execute(
                        format!("ALTER TABLE {journal} ADD COLUMN journal_position BIGINT")
                            .as_str(),
                    )
                    .await
                    .unwrap();
                    sqlx::query(&format!("INSERT INTO {journal} (event_id,topic,payload,journal_position) VALUES ($1,'udb.prefix.control','{{}}',1)"))
                        .bind(Uuid::new_v4()).execute(&pool).await.unwrap();
                }
                "position-index" => {
                    let suffix = format!("{:016x}", fnv1a_64(journal.as_bytes()));
                    pool.execute(
                        format!(
                            "CREATE INDEX {} ON {journal} (topic)",
                            qi(&format!("udb_cdc_position_{suffix}"))
                        )
                        .as_str(),
                    )
                    .await
                    .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                migrate(&pool, &config).await.is_err(),
                "{case}: bootstrap must reject invalid durable position authority"
            );
            remove(&pool, &config).await;
        }
        for case in [
            "journal-ascii",
            "journal-unicode",
            "schema-ascii",
            "schema-unicode",
        ] {
            let mut config = legacy_catalog(&pool).await;
            // PostgreSQL truncates identifiers by bytes, including quoted UTF-8 names.
            let suffix = if case.ends_with("unicode") {
                "界".repeat(20)
            } else {
                "j".repeat(48)
            };
            let long_name = format!("{}_{}", Uuid::new_v4().simple(), suffix);
            if case.starts_with("schema") {
                let old = qi(&config.cdc.system_schema);
                pool.execute(format!("ALTER SCHEMA {old} RENAME TO {}", qi(&long_name)).as_str())
                    .await
                    .unwrap();
                config.cdc.system_schema = long_name;
            } else {
                let old = config.cdc_journal_relation();
                pool.execute(format!("ALTER TABLE {old} RENAME TO {}", qi(&long_name)).as_str())
                    .await
                    .unwrap();
                config.cdc_journal_table = long_name;
            }
            assert!(
                migrate(&pool, &config).await.is_err(),
                "{case}: truncated configured identifiers must be rejected before claiming head authority"
            );
            remove(&pool, &config).await;
        }
        for entire_table in [false, true] {
            let config = legacy_catalog(&pool).await;
            migrate(&pool, &config).await.unwrap();
            let id = Uuid::new_v4();
            publish(&pool, &config, id).await;
            pool.execute(format!("DELETE FROM {}", config.cdc_journal_relation()).as_str())
                .await
                .unwrap();
            let heads = format!(
                "{}.{}",
                qi(&config.cdc.system_schema),
                qi("udb_cdc_journal_heads")
            );
            let sql = if entire_table {
                format!("DROP TABLE {heads}")
            } else {
                format!("DELETE FROM {heads}")
            };
            pool.execute(sql.as_str()).await.unwrap();
            assert!(
                migrate(&pool, &config).await.is_err(),
                "even empty journal/no cursor must refuse a lost modern durable head"
            );
            remove(&pool, &config).await;
        }
    }

    #[tokio::test]
    #[ignore = "requires live Postgres; runs in native CI"]
    async fn live_journal_prefix_concurrent_producers_expose_only_committed_prefixes() {
        let _guard = live_cdc_db_lock().lock().await;
        let pool = live_cdc_pool()
            .await
            .expect("live PostgreSQL fixture is mandatory");
        let config = legacy_catalog(&pool).await;
        migrate(&pool, &config).await.unwrap();
        for producers in [1usize, 4, 22] {
            let before = super::super::journal::head(&pool, &config).await.unwrap();
            let started = std::time::Instant::now();
            let mut tasks = tokio::task::JoinSet::new();
            for producer in 0..producers {
                let pool = pool.clone();
                let config = config.clone();
                tasks.spawn(async move {
                    let mut positions = Vec::new();
                    for _ in (producer..96).step_by(producers) {
                        let publication_started = std::time::Instant::now();
                        let item = publish(&pool, &config, Uuid::new_v4()).await;
                        positions.push((item.position, publication_started.elapsed().as_micros()));
                    }
                    positions
                });
            }
            let mut samples = 0usize;
            let mut positions = Vec::new();
            let mut publication_us = Vec::new();
            while !tasks.is_empty() {
                let journal = config.cdc_journal_relation();
                let (count,max):(i64,i64)=sqlx::query_as(&format!("SELECT COUNT(*),COALESCE(MAX(journal_position),$1) FROM {journal} WHERE journal_position>$1"))
                    .bind(before).fetch_one(&pool).await.unwrap();
                assert_eq!(
                    max - before,
                    count,
                    "visible publication positions must form a committed prefix"
                );
                samples += 1;
                while let Some(result) = tasks.try_join_next() {
                    for (position, elapsed_us) in result.unwrap() {
                        positions.push(position);
                        publication_us.push(elapsed_us);
                    }
                }
                tokio::task::yield_now().await;
            }
            positions.sort_unstable();
            positions.dedup();
            assert_eq!(
                positions.len(),
                96,
                "all actual producer publications retained once"
            );
            assert_eq!(positions[0], before + 1);
            assert_eq!(positions[95], before + 96);
            publication_us.sort_unstable();
            let journal = config.cdc_journal_relation();
            let drain_started = std::time::Instant::now();
            let drained: Vec<i64> = sqlx::query_scalar(&format!("SELECT journal_position FROM {journal} WHERE journal_position>$1 ORDER BY journal_position"))
                .bind(before).fetch_all(&pool).await.unwrap();
            assert_eq!(
                drained, positions,
                "durable reader drains the same committed prefix"
            );
            eprintln!(
                "cdc-prefix-load producers={producers} publications=96 prefix_samples={samples} elapsed_ms={} publish_p50_us={} publish_p95_us={} publish_max_us={} drain_us={}",
                started.elapsed().as_millis(),
                publication_us[47],
                publication_us[91],
                publication_us[95],
                drain_started.elapsed().as_micros()
            );
        }
        remove(&pool, &config).await;
    }
}

/// Uses the actual PostgreSQL CdcSource adapter and real Kafka delivery. The
/// wrapper changes only its stable slot label to isolate this fixture.
#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires live Postgres + Kafka; runs in native CI"]
async fn live_generic_source_durable_failure_never_advances_a_later_offset() {
    use super::source::{CdcEvent, CdcSource, PostgresCdcSource};
    use crate::generation::sql::ql;
    struct IsolatedSource {
        label: String,
        postgres: PostgresCdcSource,
    }
    #[async_trait::async_trait]
    impl CdcSource for IsolatedSource {
        fn backend_label(&self) -> &str {
            &self.label
        }
        async fn open(
            &self,
            after: &str,
        ) -> Result<Pin<Box<dyn futures::Stream<Item = Result<CdcEvent, String>> + Send>>, String>
        {
            self.postgres.open(after).await
        }
        async fn health(&self) -> Result<(), String> {
            self.postgres.health().await
        }
    }
    let _guard = live_cdc_db_lock().lock().await;
    let pool = live_cdc_pool()
        .await
        .expect("live PostgreSQL fixture is mandatory");
    let dsn = live_pg_dsn().unwrap();
    let brokers = live_kafka_brokers();
    let catalog = crate::runtime::system::SystemCatalogConfig::current();
    for failure in ["journal", "offset"] {
        let suffix = Uuid::new_v4().simple().to_string();
        let label = format!("prefix_{suffix}");
        let slot = format!("cdc_source:{label}");
        let topic = format!("udb.cdc.{label}.rows");
        ensure_live_kafka_topic(&brokers, &topic).await;
        let source_relation = format!(
            "{}.{}",
            qi(&catalog.cdc.system_schema),
            qi(&format!("prefix_source_{suffix}"))
        );
        sqlx::query(&format!("CREATE TABLE {source_relation} (event_seq BIGSERIAL PRIMARY KEY,topic TEXT NOT NULL,payload JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"))
            .execute(&pool).await.unwrap();
        for row in [1, 2] {
            let payload = serde_json::json!({"id":format!("row-{row}"),"tenant_id":"prefix-source-ci","project_id":"default"});
            sqlx::query(&format!(
                "INSERT INTO {source_relation} (topic,payload) VALUES ('rows',$1)"
            ))
            .bind(payload)
            .execute(&pool)
            .await
            .unwrap();
        }
        let config = CdcConfig {
            valid_topics: vec![topic.clone()],
            ..CdcConfig::default()
        };
        let offsets = config.offsets_relation();
        let journal = catalog.cdc_journal_relation();
        let constraint = format!("prefix_fail_{suffix}");
        let failed_relation = if failure == "journal" {
            &journal
        } else {
            &offsets
        };
        let predicate = if failure == "journal" {
            format!(
                "topic <> {} OR payload->>'source_offset' IS DISTINCT FROM '1'",
                ql(&topic)
            )
        } else {
            format!(
                "slot_name <> {} OR last_offset IS DISTINCT FROM '1'",
                ql(&slot)
            )
        };
        sqlx::query(&format!(
            "ALTER TABLE {failed_relation} ADD CONSTRAINT {} CHECK ({predicate})",
            qi(&constraint)
        ))
        .execute(&pool)
        .await
        .unwrap();
        let metrics: Arc<dyn MetricsRecorder> = Arc::new(crate::metrics::NoopMetrics);
        #[cfg(feature = "redis")]
        let engine =
            CdcEngine::new(pool.clone(), None, &brokers, dsn.clone(), metrics, config).unwrap();
        #[cfg(not(feature = "redis"))]
        let engine = CdcEngine::new(pool.clone(), &brokers, dsn.clone(), metrics, config).unwrap();
        let source = Arc::new(IsolatedSource {
            label,
            postgres: PostgresCdcSource {
                dsn: dsn.clone(),
                publication: source_relation.clone(),
                slot: slot.clone(),
            },
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            engine.tail_source(source),
        )
        .await;
        let persisted: Option<String> = sqlx::query_scalar(&format!(
            "SELECT last_offset FROM {offsets} WHERE slot_name=$1"
        ))
        .bind(&slot)
        .fetch_optional(&pool)
        .await
        .unwrap();
        let retained: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {journal} WHERE topic=$1"))
                .bind(&topic)
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query(&format!(
            "ALTER TABLE {failed_relation} DROP CONSTRAINT {}",
            qi(&constraint)
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!("DELETE FROM {offsets} WHERE slot_name=$1"))
            .bind(&slot)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(&format!("DELETE FROM {journal} WHERE topic=$1"))
            .bind(&topic)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP TABLE {source_relation}"))
            .execute(&pool)
            .await
            .unwrap();
        let error = result
            .expect("durable failure must abort the actual source tail")
            .expect_err("actual canonical persistence must fail");
        assert!(
            error.contains("source durable journal/offset commit failed"),
            "must reach actual durable failure after Kafka acknowledgement: {error}"
        );
        assert_eq!(
            persisted, None,
            "no later source offset may pass failed durable publication"
        );
        assert_eq!(
            retained, 0,
            "journal and source offset must roll back together on either failure"
        );
    }
}
