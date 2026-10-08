//! Env-gated LIVE tests for the served data-plane contract a client relies on
//! without a wrapper: the verified tenant fills in, single-row writes can insist
//! on touching one row, a capped read says it was capped, reads have a total
//! order, and the filter grammar covers NOT IN / BETWEEN / NOT / IS NOT NULL.
//!
//! Every test is `#[ignore]`d; the CI live lane runs every ignored lib test with
//! a DSN exported (see `data_plane_live` for the gating contract).

use serde_json::json;
use tonic::Code;
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service, install_dp_security,
    served_select_rows, served_upsert, teardown, with_ctx,
};
use crate::generation::{CatalogManifest, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::{DeleteRequest, PolicyRecord, PutPolicyRequest, SelectRequest, UpdateRequest};
use crate::runtime::error_reasons::reason_of;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::ensure_system_catalog;

const MSG: &str = "acme.dc.v1.Task";

/// The served RPC must refuse a write that cannot grant authorization, and the
/// refusal must leave the legacy ABAC table untouched.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_put_policy_refuses_the_legacy_authorization_surface_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let svc = dp_service(&dsn, CatalogManifest::default()).await;
    let tenant = Uuid::new_v4().to_string();
    let err = svc
        .put_policy(with_ctx(
            PutPolicyRequest {
                policy: Some(PolicyRecord {
                    effect: "allow".into(),
                    tenant_id: tenant.clone(),
                    purpose: "admin".into(),
                    message_type: MSG.into(),
                    operation: "Select".into(),
                    enabled: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect_err("the wrong policy surface must refuse rather than report success");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(reason_of(&err).as_deref(), Some("UDB_POLICY_WRONG_SURFACE"));
    assert!(err.message().contains("AuthzService.PutAuthzPolicy"));
    let rows = svc
        .runtime_snapshot()
        .list_policies(true)
        .await
        .expect("legacy policy list");
    assert!(
        rows.iter()
            .all(|row| row["tenant_id"].as_str() != Some(tenant.as_str())),
        "refusal must not insert a legacy policy"
    );
}

/// A tenant-scoped `tasks` table in a throwaway schema, served by a live broker.
async fn task_service(dsn: &str, pool: &sqlx::PgPool, schema: &str) -> DataBrokerService {
    create_schema(pool, schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".tasks \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT NOT NULL, \
          rank INTEGER NOT NULL, closed_at TIMESTAMPTZ)"
    ))
    .execute(pool)
    .await
    .expect("create tasks");
    let mut table = ManifestTable {
        proto_package: "acme.dc.v1".to_string(),
        message_name: "Task".to_string(),
        schema: schema.to_string(),
        table: "tasks".to_string(),
        primary_key: vec!["id".to_string()],
        table_security: ManifestTableSecurity {
            tenant_column: "tenant_id".to_string(),
            ..ManifestTableSecurity::default()
        },
        ..ManifestTable::default()
    };
    table.columns = vec![
        col("id", "TEXT", true),
        col("tenant_id", "TEXT", false),
        col("status", "TEXT", false),
        col("rank", "INTEGER", false),
        col("closed_at", "TIMESTAMPTZ", false),
    ];
    dp_service(
        dsn,
        CatalogManifest {
            tables: vec![table],
            ..CatalogManifest::default()
        },
    )
    .await
}

async fn ids_matching(
    svc: &DataBrokerService,
    tenant: &str,
    filter: serde_json::Value,
) -> Vec<String> {
    let rows = served_select_rows(svc, tenant, MSG, filter, false).await;
    rows.records_json
        .iter()
        .map(|bytes| {
            serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

/// The verified tenant fills in wherever the caller leaves it out, on every
/// verb; a different tenant is refused by name; a delete whose filter is only
/// scope is never widened to the whole tenant.
///
/// Revert-proof: removing the `scope_autofill` calls makes the unscoped
/// upsert fail ("tenant isolation requires record field tenant_id") and the
/// unscoped select fail ("tenant isolation requires filter on tenant_id").
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_verbs_fill_in_the_verified_tenant_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping tenant autofill");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dc_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let other = Uuid::new_v4().to_string();
    let svc = task_service(&dsn, &pool, &schema).await;

    // Upsert without tenant_id: stamped with the caller's tenant.
    for (id, rank) in [("a", 1), ("b", 2)] {
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "status": "OPEN", "rank": rank}),
            "",
        )
        .await
        .unwrap_or_else(|err| panic!("unscoped upsert {id}: {err:?}"));
    }
    served_upsert(
        &svc,
        &other,
        MSG,
        json!({"id": "z", "status": "OPEN", "rank": 9}),
        "",
    )
    .await
    .expect("other tenant's row");
    let stored: Vec<(String, String)> = sqlx::query_as(&format!(
        "SELECT id, tenant_id FROM \"{schema}\".tasks ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        stored,
        vec![
            ("a".to_string(), tenant.clone()),
            ("b".to_string(), tenant.clone()),
            ("z".to_string(), other.clone())
        ]
    );

    // Select without a tenant filter sees only the caller's rows.
    assert_eq!(ids_matching(&svc, &tenant, json!({})).await, vec!["a", "b"]);

    // Update / delete by key without tenant_id are scoped to the caller.
    svc.update(with_ctx(
        UpdateRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(&json!({"id": "a"})),
            changes: json_to_struct(&json!({"status": "DONE"})),
            ..UpdateRequest::default()
        },
        &tenant,
    ))
    .await
    .expect("unscoped update by key");
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"status": "DONE"})).await,
        vec!["a"]
    );

    // Naming another tenant is refused by name, not answered with zero rows.
    let foreign = svc
        .select(with_ctx(
            SelectRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"tenant_id": other})),
                ..SelectRequest::default()
            },
            &tenant,
        ))
        .await
        .expect_err("a foreign tenant filter must be refused");
    assert_eq!(foreign.code(), Code::PermissionDenied, "{foreign:?}");
    assert_eq!(reason_of(&foreign).as_deref(), Some("UDB_TENANT_MISMATCH"));

    // A delete whose filter is empty is not widened into "every row of the tenant".
    let mass = svc
        .delete(with_ctx(
            DeleteRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({})),
                ..DeleteRequest::default()
            },
            &tenant,
        ))
        .await;
    assert!(
        mass.is_err(),
        "an empty delete filter must stay refused: {mass:?}"
    );
    assert_eq!(ids_matching(&svc, &tenant, json!({})).await, vec!["a", "b"]);

    teardown(&pool, &schema, &tenant).await;
    teardown(&pool, &schema, &other).await;
}

/// `require_affected` makes a single-row write fail loudly when it matches
/// nothing, and changes nothing when the count is off.
///
/// Revert-proof: dropping `enforce_require_affected` makes the update of the
/// missing row succeed with `affected_rows: 0`.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_require_affected_refuses_a_write_that_misses_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping require_affected");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dc_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let svc = task_service(&dsn, &pool, &schema).await;
    for (id, rank) in [("a", 1), ("b", 2)] {
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "status": "OPEN", "rank": rank}),
            "",
        )
        .await
        .expect("seed");
    }

    let missing = svc
        .update(with_ctx(
            UpdateRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"id": "gone"})),
                changes: json_to_struct(&json!({"status": "DONE"})),
                require_affected: 1,
                ..UpdateRequest::default()
            },
            &tenant,
        ))
        .await
        .expect_err("an update that matches no row must fail with require_affected");
    assert_eq!(missing.code(), Code::NotFound, "{missing:?}");
    assert_eq!(reason_of(&missing).as_deref(), Some("UDB_NO_ROWS_AFFECTED"));

    // Two rows matched while one was required: nothing changes.
    let wide = svc
        .update(with_ctx(
            UpdateRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"status": "OPEN"})),
                changes: json_to_struct(&json!({"status": "DONE"})),
                require_affected: 1,
                ..UpdateRequest::default()
            },
            &tenant,
        ))
        .await
        .expect_err("a two-row update must fail with require_affected = 1");
    assert_eq!(reason_of(&wide).as_deref(), Some("UDB_NO_ROWS_AFFECTED"));
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"status": "OPEN"})).await,
        vec!["a", "b"],
        "a refused update must change nothing"
    );

    let gone = svc
        .delete(with_ctx(
            DeleteRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"id": "gone"})),
                require_affected: 1,
                ..DeleteRequest::default()
            },
            &tenant,
        ))
        .await
        .expect_err("a delete of a missing row must fail with require_affected");
    assert_eq!(reason_of(&gone).as_deref(), Some("UDB_NO_ROWS_AFFECTED"));

    svc.delete(with_ctx(
        DeleteRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(&json!({"id": "a"})),
            require_affected: 1,
            ..DeleteRequest::default()
        },
        &tenant,
    ))
    .await
    .expect("a delete of exactly one row passes require_affected");
    assert_eq!(ids_matching(&svc, &tenant, json!({})).await, vec!["b"]);

    teardown(&pool, &schema, &tenant).await;
}

/// A capped read says so (`has_more`), `include_total` counts every match, and
/// reads come back in a total order (the primary key) without a caller sort.
///
/// Revert-proof: without `has_more` the capped page looks complete; without the
/// primary-key default order the ids come back in heap order (shuffled by the
/// updates below).
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_select_reports_truncation_total_and_order_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping truncation/total");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dc_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let svc = task_service(&dsn, &pool, &schema).await;
    let ids = ["g", "c", "e", "a", "f", "b", "d"];
    for (rank, id) in ids.iter().enumerate() {
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "status": "OPEN", "rank": rank}),
            "",
        )
        .await
        .expect("seed");
    }
    // Rewrite a few rows so physical (heap) order no longer matches insert order.
    sqlx::query(&format!(
        "UPDATE \"{schema}\".tasks SET rank = rank + 100 WHERE id IN ('a', 'g')"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let page = svc
        .select(with_ctx(
            SelectRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"status": "OPEN"})),
                limit: 5,
                include_total: true,
                ..SelectRequest::default()
            },
            &tenant,
        ))
        .await
        .expect("capped select")
        .into_inner();
    assert_eq!(page.records_json.len(), 5);
    assert!(page.has_more, "a full page must say more rows may match");
    assert_eq!(page.exact_total, 7);
    let page_ids: Vec<String> = page
        .records_json
        .iter()
        .map(|bytes| {
            serde_json::from_slice::<serde_json::Value>(bytes).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(
        page_ids,
        vec!["a", "b", "c", "d", "e"],
        "default order is the primary key"
    );

    let all = svc
        .select(with_ctx(
            SelectRequest {
                message_type: MSG.to_string(),
                filter: json_to_struct(&json!({"status": "OPEN"})),
                limit: 50,
                ..SelectRequest::default()
            },
            &tenant,
        ))
        .await
        .expect("uncapped select")
        .into_inner();
    assert_eq!(all.records_json.len(), 7);
    assert!(!all.has_more, "a short page is the whole result");
    assert_eq!(
        all.exact_total, 0,
        "no total unless include_total asked for it"
    );

    teardown(&pool, &schema, &tenant).await;
}

/// NOT IN, BETWEEN, NOT and `$is_null: false` select exactly the rows they
/// describe, on the served read and on a served update.
///
/// Revert-proof: removing the `$between` bind-value split desynchronises the
/// placeholders and the read fails; ignoring the `$is_null` flag returns the
/// open rows instead of the closed one.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_filter_grammar_covers_nin_between_not_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping filter grammar");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");
    let schema = format!("udb_dc_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    let svc = task_service(&dsn, &pool, &schema).await;
    for (id, status, rank) in [
        ("a", "OPEN", 1),
        ("b", "DONE", 2),
        ("c", "VOID", 3),
        ("d", "OPEN", 4),
    ] {
        served_upsert(
            &svc,
            &tenant,
            MSG,
            json!({"id": id, "status": status, "rank": rank}),
            "",
        )
        .await
        .expect("seed");
    }
    served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({"id": "b", "status": "DONE", "rank": 2, "closed_at": "2026-10-07T09:00:00Z"}),
        "",
    )
    .await
    .expect("close b");

    assert_eq!(
        ids_matching(&svc, &tenant, json!({"status": {"$nin": ["DONE", "VOID"]}})).await,
        vec!["a", "d"]
    );
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"rank": {"$between": [2, 3]}})).await,
        vec!["b", "c"]
    );
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"id": {"$not": {"$in": ["a", "b"]}}})).await,
        vec!["c", "d"]
    );
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"closed_at": {"$is_null": false}})).await,
        vec!["b"]
    );

    // The same grammar on the write path (planner SQL).
    svc.update(with_ctx(
        UpdateRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(
                &json!({"rank": {"$between": [3, 4]}, "status": {"$nin": ["VOID"]}}),
            ),
            changes: json_to_struct(&json!({"status": "LATE"})),
            ..UpdateRequest::default()
        },
        &tenant,
    ))
    .await
    .expect("update through $between/$nin");
    assert_eq!(
        ids_matching(&svc, &tenant, json!({"status": "LATE"})).await,
        vec!["d"]
    );

    teardown(&pool, &schema, &tenant).await;
}
