//! Opaque revision and keyed-retry proofs over the real gRPC transport and SQL.

use serde_json::json;
use tonic::Code;
use uuid::Uuid;

use super::data_plane_live::{
    create_schema, dp_live_pg_dsn, dp_pool, dp_service, drive_begin_tx, install_dp_security,
    serve_data_broker, served_row_revision, served_status_of, teardown, widget_manifest, with_ctx,
};
use crate::proto::{DeleteRequest, Mutation, UpdateRequest, UpsertRequest};
use crate::runtime::error_reasons::reason_of;
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::system::ensure_system_catalog;

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live step"]
async fn served_transactions_and_keyed_guards_preserve_opaque_revision_safety_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let schema = format!("revision_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT)"
    ))
    .execute(&pool).await.expect("widgets");
    const MSG: &str = "acme.dp.v1.Widget";
    let manifest = widget_manifest(&schema, "widgets", "Widget", false);
    let reader = dp_service(&dsn, manifest.clone()).await;
    let (mut client, shutdown, handle) = serve_data_broker(dp_service(&dsn, manifest).await).await;
    let id = Uuid::new_v4().to_string();
    let seeded = client
        .upsert(with_ctx(
            UpsertRequest {
                message_type: MSG.into(),
                record_json: serde_json::to_vec(&json!({"id": id, "status": "A"})).unwrap(),
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("served seed")
        .into_inner();
    assert!(!seeded.revision.is_empty());
    let filter = json_to_struct(&json!({"id": id}));
    let tx_update = |status: &str, expected: Option<&str>, commit| Mutation {
        message_type: MSG.into(),
        operation: "update".into(),
        filter: filter.clone(),
        changes: json_to_struct(&json!({"status": status})),
        expected: expected.and_then(|value| json_to_struct(&json!({"status": value}))),
        commit,
        ..Default::default()
    };
    let (committed, error) =
        drive_begin_tx(&mut client, &tenant, vec![tx_update("B", Some("A"), true)]).await;
    assert!(committed && error.is_none(), "valid transaction: {error:?}");
    let after_tx = served_row_revision(&reader, &tenant, MSG, &id)
        .await
        .expect("revision slot");
    assert_ne!(
        after_tx, seeded.revision,
        "transaction must invalidate the old token"
    );
    let stale = client
        .update(with_ctx(
            UpdateRequest {
                message_type: MSG.into(),
                filter: filter.clone(),
                changes: json_to_struct(&json!({"status": "STALE"})),
                expected_revision: seeded.revision.clone(),
                require_affected: 1,
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect_err("pre-transaction token must refuse");
    assert_eq!(stale.code(), Code::FailedPrecondition);
    assert_eq!(reason_of(&stale).as_deref(), Some("UDB_REVISION_CONFLICT"));

    // Both an inserted prefix and an updated revision must roll back when a
    // later mutation fails; the new row must not leave a revision either.
    let prefix_id = Uuid::new_v4().to_string();
    let (committed, error) = drive_begin_tx(
        &mut client,
        &tenant,
        vec![
            Mutation {
                message_type: MSG.into(),
                operation: "upsert".into(),
                record_json: serde_json::to_vec(&json!({"id": prefix_id, "status": "PREFIX"}))
                    .unwrap(),
                ..Default::default()
            },
            tx_update("C", None, false),
            tx_update("D", Some("WRONG"), true),
        ],
    )
    .await;
    assert!(
        !committed && error.is_some(),
        "invalid transaction committed"
    );
    assert_eq!(
        served_row_revision(&reader, &tenant, MSG, &id)
            .await
            .as_deref(),
        Some(after_tx.as_str())
    );
    assert_eq!(
        served_status_of(&reader, &tenant, MSG, &id)
            .await
            .as_deref(),
        Some("B")
    );
    assert!(
        served_row_revision(&reader, &tenant, MSG, &prefix_id)
            .await
            .is_none()
    );
    let revisions = crate::runtime::system::SystemCatalogConfig::current().row_revisions_relation();
    let abandoned: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {revisions} WHERE tenant_id = $1 AND message_type = $2 AND row_key LIKE $3"
    ))
    .bind(&tenant)
    .bind(MSG)
    .bind(format!("%{prefix_id}%"))
    .fetch_one(&pool)
    .await
    .expect("inspect rolled-back revision ledger");
    assert_eq!(abandoned, 0, "rolled-back insert left a durable revision");
    let (committed, error) = drive_begin_tx(
        &mut client,
        &tenant,
        vec![Mutation {
            message_type: MSG.into(),
            operation: "upsert".into(),
            record_json: serde_json::to_vec(&json!({"id": id, "status": "UPSERTED"})).unwrap(),
            commit: true,
            ..Default::default()
        }],
    )
    .await;
    assert!(
        committed && error.is_none(),
        "transactional upsert: {error:?}"
    );
    let upsert_revision = served_row_revision(&reader, &tenant, MSG, &id)
        .await
        .expect("upsert revision");
    assert_ne!(
        upsert_revision, after_tx,
        "transactional upsert must advance the token"
    );

    // An identical retry must preserve the full original response, including
    // revision. Changing either concurrency guard is a different operation.
    let update = UpdateRequest {
        message_type: MSG.into(),
        filter: filter.clone(),
        changes: json_to_struct(&json!({"status": "KEYED"})),
        expected_revision: upsert_revision.clone(),
        require_affected: 1,
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let first = client
        .update(with_ctx(update.clone(), &tenant))
        .await
        .expect("guarded write")
        .into_inner();
    assert_ne!(first.revision, upsert_revision);
    let replay = client
        .update(with_ctx(update.clone(), &tenant))
        .await
        .expect("identical guarded retry")
        .into_inner();
    let mut expected_replay = first.clone();
    expected_replay.was_duplicate = true;
    assert_eq!(
        replay, expected_replay,
        "replay must retain the original revision/receipt"
    );
    let mut changed_revision = update.clone();
    changed_revision.expected_revision = first.revision.clone();
    let mut changed_count = update.clone();
    changed_count.require_affected = 2;
    for changed in [changed_revision, changed_count] {
        let err = client
            .update(with_ctx(changed, &tenant))
            .await
            .expect_err("changed guard must not replay");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(reason_of(&err).as_deref(), Some("UDB_IDEMPOTENCY_REUSE"));
        assert_eq!(
            served_row_revision(&reader, &tenant, MSG, &id)
                .await
                .as_deref(),
            Some(first.revision.as_str())
        );
    }
    assert_eq!(
        served_status_of(&reader, &tenant, MSG, &id)
            .await
            .as_deref(),
        Some("KEYED")
    );
    let delete = DeleteRequest {
        message_type: MSG.into(),
        filter,
        expected_revision: first.revision.clone(),
        require_affected: 1,
        idempotency_key: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let deleted = client
        .delete(with_ctx(delete.clone(), &tenant))
        .await
        .expect("guarded delete")
        .into_inner();
    let replay = client
        .delete(with_ctx(delete.clone(), &tenant))
        .await
        .expect("delete retry after row gone")
        .into_inner();
    let mut expected_replay = deleted.clone();
    expected_replay.was_duplicate = true;
    assert_eq!(replay, expected_replay);
    let mut changed_revision = delete.clone();
    changed_revision.expected_revision = upsert_revision;
    let mut changed_count = delete;
    changed_count.require_affected = 2;
    for changed in [changed_revision, changed_count] {
        let err = client
            .delete(with_ctx(changed, &tenant))
            .await
            .expect_err("changed delete guard must not replay");
        assert_eq!(reason_of(&err).as_deref(), Some("UDB_IDEMPOTENCY_REUSE"));
    }
    assert!(served_status_of(&reader, &tenant, MSG, &id).await.is_none());
    let _ = shutdown.send(());
    let _ = handle.await;
    teardown(&pool, &schema, &tenant).await;
}
