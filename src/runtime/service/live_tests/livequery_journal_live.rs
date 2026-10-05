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
            // Keepalive frames carry no event id; skip them.
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
