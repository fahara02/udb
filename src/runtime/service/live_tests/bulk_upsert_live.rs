//! Live served-path proof for `BulkUpsert` (audited bulk load).
//!
//! Drives the real `DataBroker::bulk_upsert` handler against a live Postgres:
//! 10 000 rows in ten batches of 1 000, then checks what a large reference
//! load depends on — every row stored, ONE change event and ONE audit record
//! per batch (not per row), a bounded number of SQL statements per batch, the
//! row revisions bumped, and all-or-nothing on a bad row (a database refusal
//! and the broker's own duplicate-key / batch-size refusals).
//!
//! `#[ignore]`d like every live test; the CI live lane runs it with
//! `UDB_INTEGRATION_PG_DSN` (and CDC delivery left on, its default).

use serde_json::json;
use tonic::Code;
use uuid::Uuid;

use super::data_plane_live::{
    create_schema, dp_live_pg_dsn, dp_pool, dp_service_with, install_dp_security, teardown,
    widget_manifest, with_ctx,
};
use crate::proto::BulkUpsertRequest;
use crate::proto::data_broker_server::DataBroker;
use crate::runtime::config::AuditSinkKind;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::{SystemCatalogConfig, ensure_system_catalog};

const MSG: &str = "acme.dp.v1.Widget";
const BATCHES: usize = 10;
const BATCH_ROWS: usize = 1_000;

async fn served_bulk(
    svc: &DataBrokerService,
    tenant: &str,
    rows: &[serde_json::Value],
    batch_id: &str,
    idempotency_key: &str,
) -> Result<crate::proto::BulkUpsertResponse, tonic::Status> {
    let request = BulkUpsertRequest {
        message_type: MSG.to_string(),
        records_json: rows
            .iter()
            .map(|row| serde_json::to_vec(row).expect("encode row"))
            .collect(),
        batch_id: batch_id.to_string(),
        idempotency_key: idempotency_key.to_string(),
        ..BulkUpsertRequest::default()
    };
    svc.bulk_upsert(with_ctx(request, tenant))
        .await
        .map(tonic::Response::into_inner)
}

fn rows(tenant: &str, prefix: &str, count: usize) -> Vec<serde_json::Value> {
    (0..count)
        .map(|index| json!({"id": format!("{prefix}-{index:05}"), "tenant_id": tenant, "status": "ok"}))
        .collect()
}

async fn table_rows(pool: &sqlx::PgPool, schema: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".widgets"))
        .fetch_one(pool)
        .await
        .expect("count widgets")
}

async fn outbox_events(pool: &sqlx::PgPool, topic: &str) -> Vec<serde_json::Value> {
    let outbox = SystemCatalogConfig::current().cdc.outbox_relation();
    sqlx::query_scalar(&format!(
        "SELECT payload FROM {outbox} WHERE topic = $1 ORDER BY created_at, event_id"
    ))
    .bind(topic)
    .fetch_all(pool)
    .await
    .expect("read outbox events")
}

/// This run's bulk audit records. The file sink is used because it is
/// per-config and synchronous; the Postgres sink's writer is process-wide and
/// owned by `audit_sink_live`.
fn audit_records(path: &std::path::Path, batch_prefix: &str) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|record| {
            record["event_type"] == "bulk_upsert"
                && record["resource_uri"]
                    .as_str()
                    .is_some_and(|uri| uri.contains(&format!("batch_id={batch_prefix}")))
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_bulk_upsert_loads_in_audited_batches_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping bulk upsert");
        return;
    };
    assert!(
        crate::runtime::cdc::cdc_delivery_enabled(),
        "this proof counts change events; run it with CDC delivery on (UDB_CDC_ENABLED unset or true)"
    );
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let run = Uuid::new_v4().simple().to_string();
    let schema = format!("udb_dp_{run}");
    let tenant = Uuid::new_v4().to_string();
    let topic = format!("acme.bulk.{run}.cdc");
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status VARCHAR(8))"
    ))
    .execute(&pool)
    .await
    .expect("create widgets");
    let mut manifest = widget_manifest(&schema, "widgets", "Widget", false);
    manifest.tables[0].cdc_topic = topic.clone();
    for column in &mut manifest.tables[0].columns {
        if column.column_name == "status" {
            column.sql_type = "VARCHAR(8)".to_string();
        }
    }
    let audit_path = std::env::temp_dir().join(format!("udb-bulk-audit-{run}.jsonl"));
    let svc = dp_service_with(&dsn, manifest, |config| {
        config.audit_sink.kind = AuditSinkKind::File;
        config.audit_sink.file_path = Some(audit_path.to_string_lossy().into_owned());
        config.service.bulk_upsert_max_rows = BATCH_ROWS;
    })
    .await;

    // ── 10 000 rows in ten batches ───────────────────────────────────────────
    let batch_prefix = format!("bulk-{run}");
    for batch in 0..BATCHES {
        let batch_rows = rows(&tenant, &format!("w{batch:02}"), BATCH_ROWS);
        let batch_id = format!("{batch_prefix}/{batch:06}");
        let response = served_bulk(&svc, &tenant, &batch_rows, &batch_id, "")
            .await
            .unwrap_or_else(|err| panic!("batch {batch} must commit: {err:?}"));
        assert_eq!(response.batch_id, batch_id);
        assert_eq!(response.row_count as usize, BATCH_ROWS);
        assert_eq!(response.affected_rows as usize, BATCH_ROWS);
        assert!(
            !response.event_id.is_empty(),
            "batch {batch} emitted no event"
        );
        assert_eq!(response.keys_sha256.len(), 64);
        assert_eq!(response.first_key, format!("s:w{batch:02}-00000"));
        // The bound that keeps a native bulk load from becoming the bottleneck:
        // request context + ONE multi-row INSERT (3 columns x 1000 rows fits one
        // statement) + ONE revision bump + ONE outbox row. Per-row writes would
        // be ~4 000 statements here.
        assert_eq!(
            response.statement_count, 4,
            "batch {batch} ran {} statements; a 1000-row batch must run 4",
            response.statement_count
        );
    }
    assert_eq!(
        table_rows(&pool, &schema).await,
        (BATCHES * BATCH_ROWS) as i64
    );

    // ONE change event per batch, summarising it.
    let events = outbox_events(&pool, &topic).await;
    assert_eq!(
        events.len(),
        BATCHES,
        "one change event per batch: {events:?}"
    );
    for (batch, event) in events.iter().enumerate() {
        assert_eq!(event["operation"], "bulk_upsert");
        assert_eq!(event["tenant_id"], tenant.as_str());
        assert_eq!(
            event["document_id"],
            format!("{batch_prefix}/{batch:06}").as_str()
        );
        assert_eq!(event["payload"]["row_count"], BATCH_ROWS);
        assert_eq!(event["payload"]["affected_rows"], BATCH_ROWS);
    }

    // Every loaded row's revision was bumped (a later conditional write sees
    // the load), in one statement per batch.
    let revisions: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {} WHERE tenant_id = $1",
        SystemCatalogConfig::current().row_revisions_relation()
    ))
    .bind(&tenant)
    .fetch_one(&pool)
    .await
    .expect("count row revisions");
    assert_eq!(revisions, (BATCHES * BATCH_ROWS) as i64);

    // ONE audit record per batch, carrying the row count and the key digest.
    let audits = audit_records(&audit_path, &batch_prefix);
    assert_eq!(
        audits.len(),
        BATCHES,
        "one audit record per batch: {audits:?}"
    );
    for record in &audits {
        assert_eq!(record["tenant_id"], tenant.as_str());
        assert!(
            record["resource_uri"]
                .as_str()
                .is_some_and(|uri| uri.ends_with(&format!("&rows={BATCH_ROWS}"))),
            "{record}"
        );
        assert_eq!(record["checksum_sha256"].as_str().map(str::len), Some(64));
    }

    // ── All or nothing ───────────────────────────────────────────────────────
    // A row the database refuses (status longer than VARCHAR(8)) as the LAST
    // row of a fresh batch: the 999 good rows before it are rolled back too.
    let mut bad = rows(&tenant, "bad", BATCH_ROWS);
    bad[BATCH_ROWS - 1]["status"] = json!("far-too-long-for-the-column");
    let err = served_bulk(&svc, &tenant, &bad, "", "")
        .await
        .expect_err("a batch with a refused row must fail");
    assert_ne!(err.code(), Code::Ok);
    let leaked: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".widgets WHERE id LIKE 'bad-%'"
    ))
    .fetch_one(&pool)
    .await
    .expect("count rolled-back rows");
    assert_eq!(leaked, 0, "a failed batch must write no row");

    // The broker's own refusals write nothing either.
    let mut duplicate = rows(&tenant, "dup", 3);
    duplicate[2]["id"] = duplicate[0]["id"].clone();
    let err = served_bulk(&svc, &tenant, &duplicate, "", "")
        .await
        .expect_err("a duplicate key must be refused");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("same conflict key"), "{err:?}");
    let err = served_bulk(&svc, &tenant, &rows(&tenant, "big", BATCH_ROWS + 1), "", "")
        .await
        .expect_err("a batch over the row maximum must be refused");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(
        table_rows(&pool, &schema).await,
        (BATCHES * BATCH_ROWS) as i64,
        "refused batches wrote rows"
    );
    assert_eq!(
        outbox_events(&pool, &topic).await.len(),
        BATCHES,
        "refused batches emitted events"
    );

    // ── Keyed retry replays, writes nothing, emits nothing ───────────────────
    let key = format!("bulk-key-{run}");
    let keyed = rows(&tenant, "keyed", 10);
    let first = served_bulk(&svc, &tenant, &keyed, "", &key)
        .await
        .expect("keyed batch commits");
    assert!(!first.was_duplicate);
    let replay = served_bulk(&svc, &tenant, &keyed, "", &key)
        .await
        .expect("keyed retry replays");
    assert!(replay.was_duplicate);
    assert_eq!(replay.batch_id, first.batch_id);
    assert_eq!(replay.event_id, first.event_id);
    assert_eq!(
        outbox_events(&pool, &topic).await.len(),
        BATCHES + 1,
        "a replay must not emit a second event"
    );
    let mut changed = keyed.clone();
    changed[0]["status"] = json!("other");
    let err = served_bulk(&svc, &tenant, &changed, "", &key)
        .await
        .expect_err("the key reused for a different batch is refused");
    assert_eq!(err.code(), Code::FailedPrecondition);

    let outbox = SystemCatalogConfig::current().cdc.outbox_relation();
    let _ = sqlx::query(&format!("DELETE FROM {outbox} WHERE topic = $1"))
        .bind(&topic)
        .execute(&pool)
        .await;
    let _ = std::fs::remove_file(&audit_path);
    teardown(&pool, &schema, &tenant).await;
}
