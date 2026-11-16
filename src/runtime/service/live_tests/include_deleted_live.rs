//! `SelectRequest.include_deleted`, proven against a live Postgres through the
//! served handlers: a caller holding `udb:deleted:read` sees its tenant's
//! soft-deleted rows marked by their `deleted_at`, every such read is on the
//! durable audit log, and every other caller or entity is refused by name.

use futures::StreamExt;
use serde_json::json;
use tonic::{Code, Request};
use uuid::Uuid;

use super::data_plane_live::{
    create_schema, dp_live_pg_dsn, dp_pool, dp_service, install_dp_security, serve_data_broker,
    served_select_rows, served_upsert, teardown, widget_manifest, with_ctx,
};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{DeleteRequest, SelectRequest};
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::system::ensure_system_catalog;

const MSG: &str = "acme.dp.v1.SoftWidget";

/// A Select as a caller with the given scopes, tagged with `correlation` so
/// its audit record can be found.
fn scoped<T>(message: T, tenant: &str, scopes: &str, correlation: &str) -> Request<T> {
    let mut request = with_ctx(message, tenant);
    let metadata = request.metadata_mut();
    metadata.insert("x-scopes", scopes.parse().expect("scopes header"));
    metadata.insert(
        "x-correlation-id",
        correlation.parse().expect("correlation"),
    );
    request
}

fn include_deleted_read(tenant: &str, fields: &[&str]) -> SelectRequest {
    SelectRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"tenant_id": tenant})),
        fields: fields.iter().map(|field| field.to_string()).collect(),
        include_deleted: true,
        include_total: true,
        ..SelectRequest::default()
    }
}

fn reason(status: &tonic::Status) -> String {
    crate::runtime::error_reasons::reason_of(status).unwrap_or_default()
}

/// The audit records for one correlation id: (row_count, target_resource, tenant).
async fn deleted_read_records(
    pool: &sqlx::PgPool,
    correlation: &str,
) -> Vec<(String, String, String)> {
    sqlx::query_as(
        "SELECT envelope ->> 'row_count', target_resource, tenant_id \
         FROM udb_system.auth_audit_log \
         WHERE event_type = 'udb.data.deleted_read' AND correlation_id = $1",
    )
    .bind(correlation)
    .fetch_all(pool)
    .await
    .expect("read the audit log")
}

/// Each returned row as (id, deleted_at is set).
fn marked_rows(set: &crate::proto::RecordSet) -> Vec<(String, bool)> {
    let mut rows = set
        .records_json
        .iter()
        .map(|raw| {
            let row: serde_json::Value = serde_json::from_slice(raw).expect("row json");
            let id = row["id"].as_str().expect("id").to_string();
            let deleted = row
                .get("deleted_at")
                .expect("an include_deleted row always carries deleted_at")
                .is_string();
            (id, deleted)
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

/// The owner's include_deleted read returns the live and the deleted row (the
/// deleted one marked by a non-null `deleted_at`, added to an explicit field
/// list), never another tenant's tombstone, is audited once with its row count,
/// and leaves plain reads (and the read cache) showing live rows only. Select,
/// SelectV2 and BatchSelect all behave the same; without the scope each is
/// refused with UDB_DELETED_READ_SCOPE_MISSING and nothing is recorded.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn include_deleted_read_returns_marked_tombstones_audits_and_stays_in_tenant_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping include_deleted");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let other = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".sd_widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT, deleted_at TIMESTAMPTZ)"
    ))
    .execute(&pool)
    .await
    .expect("create sd_widgets");
    let manifest = widget_manifest(&schema, "sd_widgets", "SoftWidget", true);
    let svc = dp_service(&dsn, manifest.clone()).await;

    let (kept, gone, foreign) = ("kept", "gone", "foreign");
    for (owner, id) in [(&tenant, kept), (&tenant, gone), (&other, foreign)] {
        served_upsert(
            &svc,
            owner,
            MSG,
            json!({"id": id, "tenant_id": owner, "status": "LIVE"}),
            "",
        )
        .await
        .expect("seed");
    }
    for (owner, id) in [(&tenant, gone), (&other, foreign)] {
        svc.delete(with_ctx(
            DeleteRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"id": id, "tenant_id": owner})),
                ..DeleteRequest::default()
            },
            owner,
        ))
        .await
        .expect("soft delete");
    }
    // A plain read first, so a cached live-only answer exists to be (not) reused.
    let plain = served_select_rows(&svc, &tenant, MSG, json!({"tenant_id": tenant}), false).await;
    assert_eq!(
        plain.records_json.len(),
        1,
        "a plain read hides the tombstone"
    );

    // Select, with an explicit field list that leaves deleted_at out.
    let allowed = "udb:read,udb:deleted:read";
    let correlation = format!("incdel-{}", Uuid::new_v4().simple());
    let set = svc
        .select(scoped(
            include_deleted_read(&tenant, &["id", "status"]),
            &tenant,
            allowed,
            &correlation,
        ))
        .await
        .expect("an include_deleted read with the scope")
        .into_inner();
    assert_eq!(
        marked_rows(&set),
        vec![(gone.to_string(), true), (kept.to_string(), false)],
        "both of the tenant's rows, the deleted one marked; never the other tenant's tombstone"
    );
    assert_eq!(set.exact_total, 2, "the total counts deleted rows too");
    assert_eq!(
        deleted_read_records(&pool, &correlation).await,
        vec![("2".to_string(), MSG.to_string(), tenant.clone())],
        "one audit record naming the entity, the tenant and the row count"
    );

    // The plain read still answers live rows only (not from an include_deleted cache).
    let plain = served_select_rows(&svc, &tenant, MSG, json!({"tenant_id": tenant}), false).await;
    assert_eq!(
        plain.records_json.len(),
        1,
        "include_deleted never feeds the read cache"
    );

    // SelectV2 reads the same rows and is audited the same way.
    let correlation_v2 = format!("incdel-v2-{}", Uuid::new_v4().simple());
    let mut batches = svc
        .select_v2(scoped(
            include_deleted_read(&tenant, &[]),
            &tenant,
            allowed,
            &correlation_v2,
        ))
        .await
        .expect("SelectV2 with include_deleted")
        .into_inner();
    let batch = batches.next().await.expect("a batch").expect("no error");
    assert_eq!(batch.row_count, 2);
    assert_eq!(deleted_read_records(&pool, &correlation_v2).await.len(), 1);

    // BatchSelect: the item's own include_deleted is honoured and audited.
    let (mut client, shutdown, handle) =
        serve_data_broker(dp_service(&dsn, manifest.clone()).await).await;
    let correlation_batch = format!("incdel-batch-{}", Uuid::new_v4().simple());
    let mut sets = client
        .batch_select(scoped(
            futures::stream::iter(vec![include_deleted_read(&tenant, &["id"])]),
            &tenant,
            allowed,
            &correlation_batch,
        ))
        .await
        .expect("BatchSelect with include_deleted")
        .into_inner();
    let item = sets.message().await.expect("item").expect("one record set");
    assert_eq!(
        marked_rows(&item),
        vec![(gone.to_string(), true), (kept.to_string(), false)]
    );
    assert_eq!(
        deleted_read_records(&pool, &correlation_batch).await.len(),
        1
    );

    // Without the scope: refused by name on every path, and nothing is recorded.
    // `udb:admin` is not the deleted-row scope.
    let unscoped = "udb:admin,udb:read,udb:write";
    let correlation_denied = format!("incdel-denied-{}", Uuid::new_v4().simple());
    let refused = svc
        .select(scoped(
            include_deleted_read(&tenant, &[]),
            &tenant,
            unscoped,
            &correlation_denied,
        ))
        .await
        .expect_err("Select without udb:deleted:read");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert_eq!(reason(&refused), "UDB_DELETED_READ_SCOPE_MISSING");
    let refused = svc
        .select_v2(scoped(
            include_deleted_read(&tenant, &[]),
            &tenant,
            unscoped,
            &correlation_denied,
        ))
        .await
        .err()
        .expect("SelectV2 without udb:deleted:read");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    let mut sets = client
        .batch_select(scoped(
            futures::stream::iter(vec![include_deleted_read(&tenant, &[])]),
            &tenant,
            unscoped,
            &correlation_denied,
        ))
        .await
        .expect("the batch stream opens")
        .into_inner();
    let refused = sets
        .message()
        .await
        .expect_err("BatchSelect item without udb:deleted:read");
    assert_eq!(refused.code(), Code::PermissionDenied, "{refused:?}");
    assert_eq!(reason(&refused), "UDB_DELETED_READ_SCOPE_MISSING");
    assert!(
        deleted_read_records(&pool, &correlation_denied)
            .await
            .is_empty(),
        "a refused read returns no rows and records no read"
    );

    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
    teardown(&pool, &schema, &other).await;
}

/// An entity without soft delete has no deleted rows to include: the read is
/// refused with INVALID_ARGUMENT naming `include_deleted`, even for a caller
/// holding the scope, instead of quietly answering as a plain Select.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn include_deleted_on_an_entity_without_soft_delete_is_refused_by_name_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping include_deleted refusal");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dp_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".hard_widgets \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create hard_widgets");
    const HARD: &str = "acme.dp.v1.HardWidget";
    let svc = dp_service(
        &dsn,
        widget_manifest(&schema, "hard_widgets", "HardWidget", false),
    )
    .await;
    let refused = svc
        .select(scoped(
            SelectRequest {
                message_type: HARD.to_string(),
                filter: json_to_struct(&json!({"tenant_id": tenant})),
                include_deleted: true,
                ..SelectRequest::default()
            },
            &tenant,
            "udb:read,udb:deleted:read",
            &format!("incdel-hard-{}", Uuid::new_v4().simple()),
        ))
        .await
        .expect_err("no soft delete, nothing to include");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    let raw = refused
        .metadata()
        .get_bin(crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY)
        .expect("typed detail")
        .to_bytes()
        .expect("detail bytes");
    let detail = crate::runtime::executor_utils::decode_error_detail_from_raw(&raw);
    assert!(
        detail
            .field_violations
            .iter()
            .any(|violation| violation.field == "include_deleted"),
        "{detail:?}"
    );
    teardown(&pool, &schema, &tenant).await;
}
