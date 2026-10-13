//! Real journal → pipeline → mutation/DLQ proof, across runtime instances.

use super::data_plane_live::{
    create_schema, dp_live_pg_dsn, dp_pool, dp_service, teardown, widget_manifest,
};
use crate::runtime::core::pipelines::PipelineSpec;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};
use serde_json::json;
use std::collections::BTreeMap;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires live Postgres; CI runs every ignored lib test"]
async fn pipeline_retries_survive_replica_changes_and_keep_project_scope() {
    // Surface the pipeline's own retry/dead-letter warnings in the test output.
    #[cfg(feature = "runtime-logging")]
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_env_filter("udb=warn")
        .try_init();
    let Some(dsn) = dp_live_pg_dsn() else { return };
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let schema = format!("udb_pipeline_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let topic = format!("pipeline.live.{}", Uuid::new_v4().simple());
    let project = "pipeline-project";
    create_schema(&pool, &schema).await;
    sqlx::query(&format!("CREATE TABLE \"{schema}\".summary (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT CHECK (status <> 'FAIL'))"))
        .execute(&pool).await.expect("target table");
    let service = dp_service(
        &dsn,
        widget_manifest(&schema, "summary", "PipelineSummary", false),
    )
    .await;
    let manifest = service.catalog.active_for("default").manifest.clone();
    let spec = PipelineSpec {
        name: topic.clone(),
        source_topic: topic.clone(),
        target_message_type: "acme.dp.v1.PipelineSummary".into(),
        fields: BTreeMap::from([
            ("id".into(), "id".into()),
            ("status".into(), "status".into()),
        ]),
        conflict_fields: vec!["id".into()],
        project: project.into(),
        max_attempts: 2,
    };
    let config = SystemCatalogConfig::current();
    let journal = config.cdc_journal_relation();
    let cursors = config.pipeline_cursors_relation();
    let dlq = config.dlq_relation();
    let mut ids = Vec::new();
    for (index, event_project, id, status) in [
        (0, project, "bad", "FAIL"),
        (1, "foreign-project", "foreign", "LIVE"),
        (2, project, "good", "LIVE"),
    ] {
        let event_id = Uuid::new_v4().to_string();
        sqlx::query(&format!("INSERT INTO {journal} (event_id, topic, payload, published_at) VALUES ($1::UUID, $2, $3, NOW() + $4::INTEGER * INTERVAL '1 second')"))
            .bind(&event_id).bind(&topic)
            .bind(json!({"tenant_id": tenant, "project_id": event_project, "event_type": "pipeline.live", "payload": {"id": id, "status": status}}))
            .bind(index as i32).execute(&pool).await.expect("journal event");
        ids.push(event_id);
    }
    assert_eq!(
        service
            .runtime_snapshot()
            .run_pipeline_tick(&spec, &manifest, 10)
            .await
            .expect("first attempt"),
        0
    );
    let tries: i32 = sqlx::query_scalar(&format!(
        "SELECT retry_count FROM {cursors} WHERE project_id = $1 AND pipeline_name = $2"
    ))
    .bind(project)
    .bind(&spec.name)
    .fetch_one(&pool)
    .await
    .expect("durable retry state");
    assert_eq!(tries, 1);
    let other_replica = dp_service(&dsn, (*manifest).clone()).await;
    assert_eq!(
        other_replica
            .runtime_snapshot()
            .run_pipeline_tick(&spec, &manifest, 10)
            .await
            .expect("second replica attempt"),
        2,
        "one exhausted event and one applied event"
    );
    let rows: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT id, tenant_id FROM \"{schema}\".summary ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .expect("pipeline target read-back");
    assert_eq!(
        rows,
        [("good".into(), tenant.clone())],
        "foreign-project events cannot write this target"
    );
    let dead: Vec<(String, i32)> = sqlx::query_as(&format!(
        "SELECT event_id::TEXT, retry_count FROM {dlq} WHERE topic = $1"
    ))
    .bind(&topic)
    .fetch_all(&pool)
    .await
    .expect("dead letters");
    assert_eq!(
        dead,
        [(ids[0].clone(), 2)],
        "retry budget persists across replicas and DLQ insert succeeds"
    );
    sqlx::query(&format!("DELETE FROM {journal} WHERE event_id = $1::UUID"))
        .bind(&ids[2])
        .execute(&pool)
        .await
        .expect("expire the acknowledged event");
    assert_eq!(
        service
            .runtime_snapshot()
            .run_pipeline_tick(&spec, &manifest, 10)
            .await
            .expect("tick after journal expiry"),
        0,
        "expiry cannot reset the cursor and replay an earlier retained event"
    );
    for relation in [&journal, &dlq] {
        sqlx::query(&format!("DELETE FROM {relation} WHERE topic = $1"))
            .bind(&topic)
            .execute(&pool)
            .await
            .expect("clean pipeline events");
    }
    sqlx::query(&format!("DELETE FROM {cursors} WHERE pipeline_name = $1"))
        .bind(&spec.name)
        .execute(&pool)
        .await
        .expect("clean cursor");
    teardown(&pool, &schema, &tenant).await;
}
