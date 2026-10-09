//! D15 seam test: a LiveQuery subscriber on a replica that does NOT hold the
//! CDC tailer lease. On such a replica the in-process broadcast is silent (it
//! is fed only by the tailer's leader), so every delta must arrive through the
//! shared CDC journal tail — and the journal tail must keep the subscriber's
//! tenant scope.
//!
//! The test drives the REAL served `LiveQueryService::subscribe` handler (the
//! method tonic dispatches) with a real `CdcEngine` whose tailer is never
//! started, against a live Postgres with the migrated native schema. A change
//! event is written straight into the journal the way the leader's tailer
//! journals it, for a foreign tenant first and then for the subscriber's own
//! tenant: the subscriber must receive exactly its own row.
//!
//! Gated on `kafka` (in `mod.rs`): building a `CdcEngine` requires it.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tonic::Request;
use uuid::Uuid;

use super::support::*;
use crate::proto::udb::core::livequery::services::v1 as lq_pb;
use crate::proto::udb::core::livequery::services::v1::live_query_service_server::LiveQueryService;
use crate::runtime::service::livequery_service::LiveQueryServiceImpl;

/// The native Lock entity: tenant-scoped (`tenant_id`) with a CDC topic.
const LOCK_MSG: &str = crate::runtime::service::lock_service::LOCK_MSG;
const PROJECT: &str = "default";

fn lock_cdc_topic() -> String {
    let table = crate::broker::resolve_table_for_message(
        crate::runtime::native_catalog::native_manifest(),
        LOCK_MSG,
    )
    .expect("the native manifest declares the Lock entity");
    let topic = table.cdc_topic.trim().to_string();
    assert!(
        !topic.is_empty(),
        "the Lock entity must declare a cdc_topic"
    );
    topic
}

/// Journal one change event exactly as the CDC tailer's leader does (the
/// envelope nests the row image under `payload`).
async fn journal_lock_change(
    pool: &sqlx::PgPool,
    topic: &str,
    tenant_id: &str,
    lock_name: &str,
) -> Uuid {
    let event_id = Uuid::new_v4();
    let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
    let payload = serde_json::json!({
        "event_id": event_id.to_string(),
        "event_type": topic,
        "topic": topic,
        "tenant_id": tenant_id,
        "project_id": PROJECT,
        "operation": "upsert",
        "message_type": LOCK_MSG,
        "payload": {
            "lock_id": Uuid::new_v4().to_string(),
            "tenant_id": tenant_id,
            "lock_name": lock_name,
        },
    });
    sqlx::query(&format!(
        "INSERT INTO {journal} (event_id, topic, partition_key, payload, published_at, delivery_state) \
         VALUES ($1, $2, $3, $4::JSONB, NOW(), 'published')"
    ))
    .bind(event_id)
    .bind(topic)
    .bind(lock_name)
    .bind(payload.to_string())
    .execute(pool)
    .await
    .expect("insert live journal change event");
    event_id
}

fn idle_cdc_engine(pool: sqlx::PgPool) -> Arc<crate::cdc::CdcEngine> {
    // Never started: no tailer lease, no Kafka consumption. The brokers are
    // unreachable on purpose — nothing on this replica may feed the
    // broadcast, so a delta can only come from the journal.
    let metrics: Arc<dyn crate::metrics::MetricsRecorder> = Arc::new(crate::metrics::NoopMetrics);
    let config = crate::runtime::cdc::CdcConfig::default();
    #[cfg(feature = "redis")]
    let engine =
        crate::cdc::CdcEngine::new(pool, None, "127.0.0.1:1", live_pg_dsn(), metrics, config);
    #[cfg(not(feature = "redis"))]
    let engine = crate::cdc::CdcEngine::new(pool, "127.0.0.1:1", live_pg_dsn(), metrics, config);
    Arc::new(engine.expect("build idle CDC engine"))
}

fn subscribe_request(tenant_id: &str) -> Request<lq_pb::SubscribeRequest> {
    let mut request = Request::new(lq_pb::SubscribeRequest {
        tenant_id: tenant_id.to_string(),
        message_type: LOCK_MSG.to_string(),
        project_id: PROJECT.to_string(),
        ..Default::default()
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", tenant_id.parse().expect("tenant metadata"));
    metadata.insert(
        "x-udb-project-id",
        PROJECT.parse().expect("project metadata"),
    );
    metadata.insert(
        "x-request-id",
        Uuid::new_v4()
            .to_string()
            .parse()
            .expect("request id metadata"),
    );
    request
}

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live lane (UDB_LIVE_AUTH_TESTS=1 ... -- --ignored)"]
async fn live_livequery_non_leader_streams_own_tenant_deltas_from_the_journal() {
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = live_runtime().await;
    let topic = lock_cdc_topic();

    let service = LiveQueryServiceImpl::new()
        .with_runtime(Some(runtime.clone()))
        .with_cdc_engine(Some(idle_cdc_engine(pool.clone())))
        .with_channels(Some(runtime.channels().clone()));

    let tenant = Uuid::new_v4().to_string();
    let foreign = Uuid::new_v4().to_string();
    let mut stream = service
        .subscribe(subscribe_request(&tenant))
        .await
        .expect("served Subscribe on a non-leader replica must open (journal readable)")
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("the snapshot frame must arrive")
        .expect("the stream must not end before the snapshot")
        .expect("the snapshot frame must not be an error");
    assert!(
        matches!(
            first.payload,
            Some(lq_pb::subscribe_response::Payload::Snapshot(_))
        ),
        "the first frame is the snapshot: {first:?}"
    );

    // Exercise the real idle forwarder before any data is journalled. The CI
    // native lane uses a one-second cadence; local live invocations retain the
    // configured cadence. An old empty Change must fail this assertion.
    let idle_period = super::super::livequery_service::livequery_keepalive_interval()
        .expect("the live heartbeat proof requires keepalives enabled");
    let heartbeat = tokio::time::timeout(idle_period + Duration::from_secs(10), stream.next())
        .await
        .expect("the idle subscription must receive a heartbeat")
        .expect("the idle subscription must stay open")
        .expect("the heartbeat must not be an error");
    assert!(
        matches!(
            heartbeat.payload,
            Some(lq_pb::subscribe_response::Payload::Heartbeat(_))
        ),
        "an idle frame must be an explicit Heartbeat, not a data change: {heartbeat:?}"
    );

    // Journalled AFTER the subscription anchored at the journal head: the
    // foreign tenant's change first, so a scope leak would be delivered first.
    let foreign_event = journal_lock_change(&pool, &topic, &foreign, "foreign-lock").await;
    let own_event = journal_lock_change(&pool, &topic, &tenant, "own-lock").await;

    let change = loop {
        let frame = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect(
                "the own-tenant delta must arrive through the journal tail on a replica \
                 whose broadcast is silent",
            )
            .expect("the stream must stay open")
            .expect("a delta frame, not an error");
        match frame.payload {
            Some(lq_pb::subscribe_response::Payload::Change(change))
                if !change.event_id.is_empty() =>
            {
                break change;
            }
            // Heartbeats carry no event id; skip them.
            _ => continue,
        }
    };
    assert_eq!(
        change.event_id,
        own_event.to_string(),
        "the first delta must be the subscriber's own event, never the foreign one ({foreign_event})"
    );
    let row: serde_json::Value = serde_json::from_str(&change.row_json).expect("delta row is JSON");
    assert_eq!(row["tenant_id"], tenant, "{row}");
    assert_eq!(row["lock_name"], "own-lock", "{row}");

    drop(stream);
    let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
    let _ = sqlx::query(&format!("DELETE FROM {journal} WHERE event_id = ANY($1)"))
        .bind([foreign_event, own_event].as_slice())
        .execute(&pool)
        .await;
}

fn journal_scan_count(metrics: &crate::metrics::PrometheusMetrics, source: &str) -> u64 {
    metric_sum(
        metrics,
        &format!("udb_livequery_journal_scans_total{{source=\"{source}\"}} "),
    )
}

fn metric_sum(metrics: &crate::metrics::PrometheusMetrics, prefix: &str) -> u64 {
    metrics
        .gather_text("")
        .lines()
        .filter(|line| line.starts_with(prefix))
        .map(|line| {
            line.split_whitespace()
                .last()
                .expect("metric value")
                .parse::<u64>()
                .expect("non-negative integer counter or gauge")
        })
        .sum()
}

fn filtered_watch_request(tenant: &str, watcher: usize) -> Request<lq_pb::SubscribeRequest> {
    let mut request = subscribe_request(tenant);
    let predicate =
        |op: lq_pb::LiveQueryComparison, value: &str, values: &[&str]| lq_pb::LiveQueryPredicate {
            field: "lock_name".into(),
            op: op as i32,
            value: value.into(),
            values: values.iter().map(|value| (*value).to_string()).collect(),
        };
    if watcher % 2 == 0 {
        // IN(alpha, beta) AND (beta OR gamma) matches only beta. Applying just
        // one of the two clauses leaks alpha or gamma into the stream.
        request.get_mut().filters = vec![predicate(
            lq_pb::LiveQueryComparison::In,
            "",
            &["alpha", "beta"],
        )];
        request.get_mut().any_of = vec![lq_pb::LiveQueryAnyOf {
            predicates: vec![
                predicate(lq_pb::LiveQueryComparison::Eq, "beta", &[]),
                predicate(lq_pb::LiveQueryComparison::Eq, "gamma", &[]),
            ],
        }];
    } else {
        request.get_mut().filters = vec![predicate(lq_pb::LiveQueryComparison::Eq, "gamma", &[])];
    }
    request
}

async fn next_data_change<S>(stream: &mut S) -> lq_pb::LiveQueryChange
where
    S: futures::Stream<Item = Result<lq_pb::SubscribeResponse, tonic::Status>> + Unpin,
{
    loop {
        let frame = stream
            .next()
            .await
            .expect("the live subscription must stay open")
            .expect("the live subscription must not fail");
        match frame.payload {
            Some(lq_pb::subscribe_response::Payload::Change(change)) => return change,
            Some(lq_pb::subscribe_response::Payload::Heartbeat(_)) => {}
            other => panic!("unexpected frame after the snapshot: {other:?}"),
        }
    }
}

/// LQ3/LQ4: 1,000 actual served subscriptions share a real PostgreSQL journal
/// poll, retain each watcher's IN/OR filter on snapshots and deltas, and stop
/// polling after the clients disconnect. No CDC leader or in-memory store can
/// supply these deltas. The scan count is measured at the production SQL calls.
#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live lane"]
async fn live_livequery_thousand_watchers_share_scans_and_keep_filter_parity() {
    const WATCHERS: usize = 1_000;
    let _guard = live_native_service_db_lock().lock().await;
    let pool = live_pg_pool().await;
    migrate_native_service_db(&pool).await;
    let runtime = live_runtime().await;
    let topic = lock_cdc_topic();
    let tenant = Uuid::new_v4().to_string();
    let foreign = Uuid::new_v4().to_string();
    // Fixture rows exercise the mediated snapshot read, independently of the
    // journal images that later exercise the delta evaluator.
    for (scope, name) in [
        (&tenant, "alpha"),
        (&tenant, "beta"),
        (&tenant, "gamma"),
        (&foreign, "beta"),
    ] {
        sqlx::query(
            "INSERT INTO udb_lock.locks (tenant_id, lock_name, owner_id) VALUES ($1, $2, $3)",
        )
        .bind(scope)
        .bind(name)
        .bind("live-query-watch-proof")
        .execute(&pool)
        .await
        .expect("seed real snapshot row");
    }
    let metrics = Arc::new(crate::metrics::PrometheusMetrics::new().expect("metrics registry"));
    let service = LiveQueryServiceImpl::new()
        .with_runtime(Some(runtime.clone()))
        .with_cdc_engine(Some(idle_cdc_engine(pool.clone())))
        .with_channels(Some(runtime.channels().clone()))
        .with_metrics(metrics.clone());
    let mut streams = Vec::with_capacity(WATCHERS);
    tokio::time::timeout(Duration::from_secs(120), async {
        for watcher in 0..WATCHERS {
            let mut stream = loop {
                match service
                    .subscribe(filtered_watch_request(&tenant, watcher))
                    .await
                {
                    Ok(response) => break response.into_inner(),
                    Err(status) => {
                        // Connection rate and concurrent stream capacity are
                        // separate budgets. Respect the real admission delay
                        // while still requiring all 1,000 active subscriptions.
                        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                        let raw = status
                            .metadata()
                            .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
                            .expect("typed admission refusal")
                            .to_bytes()
                            .expect("binary admission detail");
                        let detail =
                            crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
                        assert_eq!(detail.backend, "channel");
                        assert_eq!(detail.operation, "read_fair_admission");
                        assert!(detail.retryable && detail.retry_after_ms > 0);
                        tokio::time::sleep(Duration::from_millis(detail.retry_after_ms as u64))
                            .await;
                    }
                }
            };
            let first = stream
                .next()
                .await
                .expect("snapshot")
                .expect("snapshot read");
            let Some(lq_pb::subscribe_response::Payload::Snapshot(snapshot)) = first.payload else {
                panic!("watcher {watcher} must receive its snapshot first");
            };
            assert_eq!(snapshot.row_count, 1, "watcher {watcher}: {snapshot:?}");
            assert_eq!(snapshot.rows_json.len(), 1);
            let row: serde_json::Value = serde_json::from_str(&snapshot.rows_json[0]).unwrap();
            let expected = if watcher % 2 == 0 { "beta" } else { "gamma" };
            assert_eq!(row["lock_name"], expected, "watcher {watcher}: {row}");
            assert_eq!(row["tenant_id"], tenant, "snapshot tenant leak");
            streams.push(stream);
        }
    })
    .await
    .expect("all 1,000 subscriptions must open within the setup budget");
    tokio::time::timeout(Duration::from_secs(5), async {
        while metric_sum(&metrics, "udb_livequery_active_streams{") != WATCHERS as u64 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all admitted stream tasks must publish their active gauge");

    // Measure only steady-state idle SQL, excluding the necessary snapshot/head
    // reads during admission. A poll per watcher would issue thousands here.
    let before = journal_scan_count(&metrics, "shared");
    let started = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let elapsed = started.elapsed();
    let scans = journal_scan_count(&metrics, "shared") - before;
    let ceiling = elapsed.as_millis() as u64 / 500 + 2;
    assert!(
        scans > 0 && scans <= ceiling,
        "{WATCHERS} watchers: {scans} scans in {elapsed:?}"
    );
    assert_eq!(
        journal_scan_count(&metrics, "catch_up"),
        0,
        "idle watchers must not scan privately"
    );

    let mut events = vec![journal_lock_change(&pool, &topic, &foreign, "beta").await];
    events.push(journal_lock_change(&pool, &topic, &tenant, "alpha").await);
    let beta = journal_lock_change(&pool, &topic, &tenant, "beta").await;
    let gamma = journal_lock_change(&pool, &topic, &tenant, "gamma").await;
    events.extend([beta, gamma]);
    tokio::time::timeout(Duration::from_secs(30), async {
        for (watcher, stream) in streams.iter_mut().enumerate() {
            let change = next_data_change(stream).await;
            let (expected_event, expected_name) = if watcher % 2 == 0 {
                (beta, "beta")
            } else {
                (gamma, "gamma")
            };
            assert_eq!(
                change.event_id,
                expected_event.to_string(),
                "watcher {watcher}"
            );
            let row: serde_json::Value = serde_json::from_str(&change.row_json).unwrap();
            assert_eq!(row["lock_name"], expected_name, "delta predicate mismatch");
            assert_eq!(row["tenant_id"], tenant, "delta tenant leak");
        }
    })
    .await
    .expect("every watcher must receive its own matching journal delta");
    // Keep reading all watchers concurrently: an omitted filter clause or a
    // duplicate journal delivery must fail, even when its first delta matched.
    let unexpected = tokio::time::timeout(
        Duration::from_secs(1),
        futures::future::select_all(
            streams
                .iter_mut()
                .map(|stream| Box::pin(next_data_change(stream))),
        ),
    )
    .await;
    assert!(
        unexpected.is_err(),
        "a watcher received an extra or duplicate data change"
    );
    drop(unexpected);
    assert_eq!(
        metric_sum(&metrics, "udb_livequery_delta_forwarded_total{"),
        WATCHERS as u64
    );

    drop(streams);
    tokio::time::timeout(Duration::from_secs(5), async {
        while metric_sum(&metrics, "udb_livequery_active_streams{") != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("disconnecting every watcher must release every stream slot");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let retired = journal_scan_count(&metrics, "shared");
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        journal_scan_count(&metrics, "shared"),
        retired,
        "an idle feed must retire"
    );

    let journal = crate::runtime::system::SystemCatalogConfig::current().cdc_journal_relation();
    sqlx::query(&format!("DELETE FROM {journal} WHERE event_id = ANY($1)"))
        .bind(events.as_slice())
        .execute(&pool)
        .await
        .expect("remove proof journal rows");
    sqlx::query("DELETE FROM udb_lock.locks WHERE tenant_id = ANY($1)")
        .bind([tenant, foreign].as_slice())
        .execute(&pool)
        .await
        .expect("remove snapshot fixture rows");
}
