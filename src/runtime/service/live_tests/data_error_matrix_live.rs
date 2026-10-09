//! Unary/BeginTx refusal parity and durable relational replay over served gRPC.

use serde_json::{Value, json};
use tonic::{Code, Status};
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service, install_dp_security,
    serve_data_broker, served_row_revision, teardown, widget_manifest, with_ctx,
};
use crate::proto::{
    DeleteRequest, ErrorDetail, ErrorKind, Mutation, TxStatus, UpdateRequest, UpsertRequest,
    tx_status,
};
use crate::runtime::executor_utils::{
    ERROR_DETAIL_METADATA_KEY, decode_error_detail_from_raw, json_to_struct,
};
use crate::runtime::system::ensure_system_catalog;

const MSG: &str = "acme.dp.v1.Widget";

fn detail(status: &Status) -> ErrorDetail {
    let raw = status
        .metadata()
        .get_bin(ERROR_DETAIL_METADATA_KEY)
        .expect("served typed refusal")
        .to_bytes()
        .expect("binary typed refusal");
    decode_error_detail_from_raw(&raw)
}

async fn transaction(
    client: &mut crate::proto::data_broker_client::DataBrokerClient<tonic::transport::Channel>,
    tenant: &str,
    mutations: Vec<Mutation>,
) -> (Vec<TxStatus>, Option<Status>) {
    let mut stream = client
        .begin_tx(with_ctx(futures::stream::iter(mutations), tenant))
        .await
        .expect("served BeginTx accepted")
        .into_inner();
    let mut frames = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(frame)) => frames.push(frame),
            Ok(None) => return (frames, None),
            Err(status) => return (frames, Some(status)),
        }
    }
}

fn record(id: &str, unique_key: &str) -> Value {
    json!({"id":id,"status":"A","unique_key":unique_key,"immediate_unique":unique_key,"parent_id":"parent","counter":0})
}

fn upsert(id: &str, unique_key: &str) -> Mutation {
    Mutation {
        operation: "upsert".into(),
        message_type: MSG.into(),
        record_json: serde_json::to_vec(&record(id, unique_key)).unwrap(),
        ..Default::default()
    }
}

fn update(changes: Value) -> Mutation {
    Mutation {
        operation: "update".into(),
        message_type: MSG.into(),
        filter: json_to_struct(&json!({"id":"seed"})),
        changes: json_to_struct(&changes),
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires live Postgres; runs in the CI native live step"]
async fn served_unary_and_transaction_errors_preserve_every_reason_and_original_code_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool).await.expect("system catalog");
    let schema = format!("errors_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".parents (id TEXT PRIMARY KEY)"
    ))
    .execute(&pool)
    .await
    .expect("parents");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".parents VALUES ('parent')"
    ))
    .execute(&pool)
    .await
    .expect("parent");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".widgets (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, \
         status TEXT NOT NULL, unique_key TEXT NOT NULL, immediate_unique TEXT NOT NULL, \
         parent_id TEXT, parent_deferred TEXT, counter BIGINT NOT NULL, \
         CONSTRAINT widgets_unique_key UNIQUE (unique_key) DEFERRABLE INITIALLY DEFERRED, \
         CONSTRAINT widgets_immediate_unique UNIQUE (immediate_unique), \
         CONSTRAINT widgets_parent_fk FOREIGN KEY (parent_id) REFERENCES \"{schema}\".parents(id), \
         CONSTRAINT widgets_parent_deferred_fk FOREIGN KEY (parent_deferred) \
         REFERENCES \"{schema}\".parents(id) DEFERRABLE INITIALLY DEFERRED)"
    ))
    .execute(&pool)
    .await
    .expect("widgets with real constraints");
    let mut manifest = widget_manifest(&schema, "widgets", "Widget", false);
    manifest.tables[0]
        .columns
        .iter_mut()
        .find(|column| column.column_name == "status")
        .unwrap()
        .not_null = true;
    manifest.tables[0].columns.extend([
        col("unique_key", "TEXT", false),
        col("immediate_unique", "TEXT", false),
        col("parent_id", "TEXT", false),
        col("parent_deferred", "TEXT", false),
        col("counter", "BIGINT", false),
    ]);
    for column in &mut manifest.tables[0].columns {
        if matches!(
            column.column_name.as_str(),
            "unique_key" | "immediate_unique" | "counter"
        ) {
            column.not_null = true;
        }
        if column.column_name == "counter" {
            column.proto_type = "int64".into();
        }
    }
    let reader = dp_service(&dsn, manifest.clone()).await;
    let (mut client, shutdown, handle) = serve_data_broker(dp_service(&dsn, manifest).await).await;
    for (id, key) in [("seed", "seed-key"), ("other", "taken")] {
        client
            .upsert(with_ctx(
                UpsertRequest {
                    message_type: MSG.into(),
                    record_json: serde_json::to_vec(&record(id, key)).unwrap(),
                    ..Default::default()
                },
                &tenant,
            ))
            .await
            .expect("served seed");
    }
    // Real store leaves return String: prove the driver's complete detail
    // survives that boundary for immediate and deferred constraints.
    for (changes, code, reason, column, constraint) in [
        (
            "immediate_unique = 'taken'",
            Code::AlreadyExists,
            "UDB_UNIQUE_VIOLATION",
            "immediate_unique",
            "widgets_immediate_unique",
        ),
        (
            "unique_key = 'taken'",
            Code::AlreadyExists,
            "UDB_UNIQUE_VIOLATION",
            "unique_key",
            "widgets_unique_key",
        ),
        (
            "status = NULL",
            Code::InvalidArgument,
            "UDB_NOT_NULL_VIOLATION",
            "status",
            "",
        ),
        (
            "parent_id = 'absent-parent'",
            Code::FailedPrecondition,
            "UDB_FOREIGN_KEY_VIOLATION",
            "parent_id",
            "widgets_parent_fk",
        ),
        (
            "parent_deferred = 'absent-parent'",
            Code::FailedPrecondition,
            "UDB_FOREIGN_KEY_VIOLATION",
            "parent_deferred",
            "widgets_parent_deferred_fk",
        ),
    ] {
        let error = sqlx::query(&format!(
            "UPDATE \"{schema}\".widgets SET {changes} WHERE id = 'seed'"
        ))
        .execute(&pool)
        .await
        .expect_err("real store leaf must report the constraint");
        let original =
            crate::runtime::executor_utils::sqlx_error_to_status("store boundary", &error);
        let tagged =
            crate::runtime::executor_utils::sqlx_error_to_tagged_string("store boundary", &error);
        let restored = crate::runtime::executor_utils::status_from_store_string(tagged);
        assert_eq!(restored.code(), code);
        assert_eq!(restored.message(), original.message());
        let restored_detail = detail(&restored);
        assert_eq!(restored_detail.reason, reason);
        assert_eq!(restored_detail.column, column);
        assert_eq!(restored_detail.constraint, constraint);
        assert_eq!(
            restored_detail,
            detail(&original),
            "complete SQL refusal survives String leaf"
        );
        let participant = crate::runtime::xa_postgres::PostgresXaParticipant::new(
            crate::runtime::xa::XaParticipantHandle::new("postgres", "constraint-proof"),
            pool.clone(),
            vec![format!(
                "UPDATE \"{schema}\".widgets SET {changes} WHERE id = 'seed'"
            )],
        );
        let vote = crate::runtime::xa::XaParticipant::prepare(
            &participant,
            &crate::runtime::xa::XaCoordinator::new_xid(),
        )
        .await;
        let buffered_refusal = vote
            .refusal_status()
            .expect("buffered PostgreSQL participant preserves real refusal");
        assert_eq!(buffered_refusal.code(), code);
        let buffered_detail = detail(&buffered_refusal);
        assert_eq!(buffered_detail.reason, reason);
        assert_eq!(buffered_detail.column, column);
        assert_eq!(buffered_detail.constraint, constraint);
    }
    client
        .update(with_ctx(
            UpdateRequest {
                message_type: MSG.into(),
                filter: json_to_struct(&json!({"id":"seed"})),
                changes: json_to_struct(&json!({"status":"A"})),
                idempotency_key: "unary-claimed".into(),
                ..Default::default()
            },
            &tenant,
        ))
        .await
        .expect("claim unary replay key");
    let mut claimed = update(json!({"status":"A"}));
    claimed.idempotency_key = "tx-claimed".into();
    claimed.commit = true;
    let (frames, error) = transaction(&mut client, &tenant, vec![claimed]).await;
    assert!(error.is_none(), "initial keyed transaction: {error:?}");
    assert!(
        frames
            .iter()
            .any(|frame| frame.state == tx_status::State::TxStateCommitted as i32)
    );

    let not_found = Mutation {
        operation: "delete".into(),
        message_type: MSG.into(),
        filter: json_to_struct(&json!({"id":"absent"})),
        require_affected: 1,
        ..Default::default()
    };
    let mut cas = update(json!({"status":"bad-cas"}));
    cas.expected = json_to_struct(&json!({"status":"stale"}));
    let mut reused = update(json!({"status":"changed-input"}));
    reused.idempotency_key = "tx-claimed".into();
    let cases = [
        (
            update(json!({"immediate_unique":"taken"})),
            Code::AlreadyExists,
            ErrorKind::Unique,
            "UDB_UNIQUE_VIOLATION",
            "immediate_unique",
            "widgets_immediate_unique",
        ),
        (
            update(json!({"unique_key":"taken"})),
            Code::AlreadyExists,
            ErrorKind::Unique,
            "UDB_UNIQUE_VIOLATION",
            "unique_key",
            "widgets_unique_key",
        ),
        (
            update(json!({"status":null})),
            Code::InvalidArgument,
            ErrorKind::NotNull,
            "UDB_NOT_NULL_VIOLATION",
            "status",
            "",
        ),
        (
            update(json!({"parent_id":"absent-parent"})),
            Code::FailedPrecondition,
            ErrorKind::ForeignKey,
            "UDB_FOREIGN_KEY_VIOLATION",
            "parent_id",
            "widgets_parent_fk",
        ),
        (
            update(json!({"parent_deferred":"absent-parent"})),
            Code::FailedPrecondition,
            ErrorKind::ForeignKey,
            "UDB_FOREIGN_KEY_VIOLATION",
            "parent_deferred",
            "widgets_parent_deferred_fk",
        ),
        (
            not_found,
            Code::NotFound,
            ErrorKind::NotFound,
            "UDB_NO_ROWS_AFFECTED",
            "",
            "",
        ),
        (
            cas,
            Code::FailedPrecondition,
            ErrorKind::Conflict,
            "UDB_CAS_CONFLICT",
            "",
            "",
        ),
        (
            reused,
            Code::FailedPrecondition,
            ErrorKind::Conflict,
            "UDB_IDEMPOTENCY_REUSE",
            "",
            "",
        ),
    ];
    for (index, (mut mutation, code, kind, reason, column, constraint)) in
        cases.into_iter().enumerate()
    {
        let unary_error = if mutation.operation == "delete" {
            client
                .delete(with_ctx(
                    DeleteRequest {
                        message_type: MSG.into(),
                        filter: mutation.filter.clone(),
                        require_affected: mutation.require_affected,
                        ..Default::default()
                    },
                    &tenant,
                ))
                .await
                .map(|_| ())
                .expect_err("served unary delete must refuse")
        } else {
            client
                .update(with_ctx(
                    UpdateRequest {
                        message_type: MSG.into(),
                        filter: mutation.filter.clone(),
                        changes: mutation.changes.clone(),
                        expected: mutation.expected.clone(),
                        idempotency_key: if reason == "UDB_IDEMPOTENCY_REUSE" {
                            "unary-claimed".into()
                        } else {
                            String::new()
                        },
                        ..Default::default()
                    },
                    &tenant,
                ))
                .await
                .map(|_| ())
                .expect_err("served unary update must refuse")
        };
        assert_eq!(unary_error.code(), code, "{reason}");
        let unary_detail = detail(&unary_error);
        assert_eq!(unary_detail.reason, reason);
        assert_eq!(unary_detail.kind, kind as i32);
        assert_eq!(unary_detail.column, column);
        assert_eq!(unary_detail.constraint, constraint);
        assert!(!unary_detail.fix_hint.is_empty());

        let marker = format!("rolled-back-{index}");
        let marker_unique = format!("marker-key-{index}");
        mutation.commit = true;
        let (frames, error) = transaction(
            &mut client,
            &tenant,
            vec![upsert(&marker, &marker_unique), mutation],
        )
        .await;
        let error = error.expect("served transaction must terminate with original refusal");
        assert_eq!(error.code(), code, "{reason}");
        let terminal_detail = detail(&error);
        assert_eq!(terminal_detail.reason, reason);
        assert_eq!(terminal_detail.kind, kind as i32);
        assert_eq!(terminal_detail.column, column);
        assert_eq!(terminal_detail.constraint, constraint);
        let refusal = frames
            .iter()
            .find(|frame| frame.state == tx_status::State::TxStateError as i32)
            .expect("actual error frame reaches the gRPC client before its trailer");
        assert_eq!(refusal.code, code as i32);
        assert_eq!(refusal.error_detail.as_ref(), Some(&terminal_detail));
        assert!(!refusal.tx_id.is_empty());
        assert!(
            !frames
                .iter()
                .any(|frame| frame.state == tx_status::State::TxStateCommitted as i32)
        );
        let markers: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM \"{schema}\".widgets WHERE id = $1"
        ))
        .bind(&marker)
        .fetch_one(&pool)
        .await
        .expect("read rollback result");
        assert_eq!(
            markers, 0,
            "an earlier write must also roll back for {reason}"
        );
    }

    // Deferred constraints also fire at PREPARE, not only at plain COMMIT.
    // Drive the same real gRPC stream through the configured 2PC protocol.
    {
        let _two_phase = super::ops_seams_live::EnvRestore::set("UDB_2PC_ENABLED", "true");
        let prepared_capacity: i32 =
            sqlx::query_scalar("SELECT current_setting('max_prepared_transactions')::int")
                .fetch_one(&pool)
                .await
                .expect("PostgreSQL prepared transaction capacity");
        assert!(
            prepared_capacity > 0,
            "CI must enable real PREPARE TRANSACTION"
        );
        let ledger_relation =
            crate::runtime::system::SystemCatalogConfig::current().xa_ledger_relation();
        for (index, changes, code, kind, reason, column, constraint) in [
            (
                0,
                json!({"unique_key":"taken"}),
                Code::AlreadyExists,
                ErrorKind::Unique,
                "UDB_UNIQUE_VIOLATION",
                "unique_key",
                "widgets_unique_key",
            ),
            (
                1,
                json!({"parent_deferred":"absent-parent"}),
                Code::FailedPrecondition,
                ErrorKind::ForeignKey,
                "UDB_FOREIGN_KEY_VIOLATION",
                "parent_deferred",
                "widgets_parent_deferred_fk",
            ),
        ] {
            let mut mutation = update(changes);
            mutation.context = Some(crate::proto::RequestContext {
                routing_policy: "tx_strategy=two_phase".into(),
                ..Default::default()
            });
            mutation.commit = true;
            let marker = format!("prepare-rolled-back-{index}");
            let (frames, error) = transaction(
                &mut client,
                &tenant,
                vec![
                    upsert(&marker, &format!("prepare-marker-key-{index}")),
                    mutation,
                ],
            )
            .await;
            let error = error.expect("real PREPARE must report the deferred refusal");
            assert_eq!(error.code(), code);
            let terminal_detail = detail(&error);
            assert_eq!(terminal_detail.reason, reason);
            assert_eq!(terminal_detail.kind, kind as i32);
            assert_eq!(terminal_detail.column, column);
            assert_eq!(terminal_detail.constraint, constraint);
            assert!(!terminal_detail.fix_hint.is_empty());
            assert!(
                !terminal_detail.retryable,
                "constraint failure is not transient"
            );
            let refusal = frames
                .iter()
                .find(|frame| frame.state == tx_status::State::TxStateError as i32)
                .expect("PREPARE refusal frame reaches the served client");
            assert_eq!(refusal.code, code as i32);
            assert_eq!(refusal.error_detail.as_ref(), Some(&terminal_detail));
            assert!(
                !frames
                    .iter()
                    .any(|frame| { frame.state == tx_status::State::TxStateCommitted as i32 })
            );
            let markers: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM \"{schema}\".widgets WHERE id = $1"
            ))
            .bind(&marker)
            .fetch_one(&pool)
            .await
            .expect("read PREPARE rollback");
            assert_eq!(markers, 0, "PREPARE refusal rolls back earlier writes");
            let ledgers: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {ledger_relation} WHERE tenant_id = $1 AND decision = 'rolled_back'"
            ))
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("read actual phase-one rollback ledger");
            assert_eq!(ledgers, index + 1, "proof must reach the XA coordinator");
            let prepared: i64 = sqlx::query_scalar(&format!(
                "SELECT count(*) FROM pg_prepared_xacts p JOIN {ledger_relation} l ON p.gid = l.xid WHERE l.tenant_id = $1"
            ))
            .bind(&tenant)
            .fetch_one(&pool)
            .await
            .expect("check prepared transaction cleanup");
            assert_eq!(prepared, 0, "refusal must not leak a prepared transaction");
        }
    }

    // A replay must skip CAS, increments, revisions and SQL side effects. The
    // first execution changes A to B, so repeating the original A precondition
    // is safe only when the durable claim is checked before CAS.
    let mut keyed = update(json!({"status":"B"}));
    keyed.expected = json_to_struct(&json!({"status":"A"}));
    keyed
        .increments
        .push(crate::proto::update_request::Increment {
            column: "counter".into(),
            delta: 1.0,
        });
    keyed.require_affected = 1;
    keyed.idempotency_key = "exact-once".into();
    keyed.commit = true;
    let (first, error) = transaction(&mut client, &tenant, vec![keyed.clone()]).await;
    assert!(error.is_none(), "first execution: {error:?}");
    let first_id = first
        .iter()
        .find(|frame| frame.state == tx_status::State::TxStateOpen as i32)
        .unwrap()
        .mutation_id
        .clone();
    let revision = served_row_revision(&reader, &tenant, MSG, "seed")
        .await
        .expect("first revision");
    let (replayed, error) = transaction(&mut client, &tenant, vec![keyed]).await;
    assert!(error.is_none(), "retry must replay before CAS: {error:?}");
    assert!(
        replayed
            .iter()
            .any(|frame| frame.state == tx_status::State::TxStateCommitted as i32)
    );
    assert_eq!(
        replayed
            .iter()
            .find(|frame| frame.state == tx_status::State::TxStateOpen as i32)
            .unwrap()
            .mutation_id,
        first_id
    );
    assert_eq!(
        served_row_revision(&reader, &tenant, MSG, "seed")
            .await
            .unwrap(),
        revision
    );
    let counter: i64 = sqlx::query_scalar(&format!(
        "SELECT counter FROM \"{schema}\".widgets WHERE id = 'seed'"
    ))
    .fetch_one(&pool)
    .await
    .expect("counter after retry");
    assert_eq!(counter, 1, "retry must not apply the increment twice");

    // A constraint failure at COMMIT must roll back its fresh receipt as well
    // as its rows. Keep the immediate unique constraint valid so this actually
    // exercises commit-time classification and receipt rollback.
    let mut rolled_back = update(json!({"status":"C"}));
    rolled_back
        .increments
        .push(crate::proto::update_request::Increment {
            column: "counter".into(),
            delta: 5.0,
        });
    rolled_back.idempotency_key = "retry-after-rollback".into();
    let mut duplicate = upsert("duplicate", "taken");
    let mut duplicate_record = record("duplicate", "taken");
    duplicate_record["immediate_unique"] = json!("duplicate-immediate-unused");
    duplicate.record_json = serde_json::to_vec(&duplicate_record).unwrap();
    duplicate.commit = true;
    let (_, error) = transaction(&mut client, &tenant, vec![rolled_back.clone(), duplicate]).await;
    assert_eq!(detail(&error.unwrap()).reason, "UDB_UNIQUE_VIOLATION");
    rolled_back.commit = true;
    let (_, error) = transaction(&mut client, &tenant, vec![rolled_back]).await;
    assert!(
        error.is_none(),
        "retry after rollback must remain fresh: {error:?}"
    );
    let counter: i64 = sqlx::query_scalar(&format!(
        "SELECT counter FROM \"{schema}\".widgets WHERE id = 'seed'"
    ))
    .fetch_one(&pool)
    .await
    .expect("counter after recovered transaction");
    assert_eq!(counter, 6);

    // Concurrent identical requests must serialize on the durable claim, not
    // execute two increments while each believes it owns the replay key.
    let mut concurrent = update(json!({"status":"D"}));
    concurrent.expected = json_to_struct(&json!({"status":"C"}));
    concurrent
        .increments
        .push(crate::proto::update_request::Increment {
            column: "counter".into(),
            delta: 2.0,
        });
    concurrent.idempotency_key = "concurrent-retry".into();
    concurrent.commit = true;
    let mut left_client = client.clone();
    let mut right_client = client.clone();
    let (left, right) = tokio::join!(
        transaction(&mut left_client, &tenant, vec![concurrent.clone()]),
        transaction(&mut right_client, &tenant, vec![concurrent]),
    );
    for (_, error) in [&left, &right] {
        assert!(error.is_none(), "concurrent retry: {error:?}");
    }
    let original_id = left
        .0
        .iter()
        .find(|frame| frame.state == tx_status::State::TxStateOpen as i32)
        .unwrap()
        .mutation_id
        .clone();
    assert_eq!(
        right
            .0
            .iter()
            .find(|frame| frame.state == tx_status::State::TxStateOpen as i32)
            .unwrap()
            .mutation_id,
        original_id
    );
    let counter: i64 = sqlx::query_scalar(&format!(
        "SELECT counter FROM \"{schema}\".widgets WHERE id = 'seed'"
    ))
    .fetch_one(&pool)
    .await
    .expect("counter after concurrent retries");
    assert_eq!(counter, 8);

    let mut keyed_upsert = upsert("keyed-upsert", "keyed-upsert-key");
    keyed_upsert.idempotency_key = "upsert-replay".into();
    keyed_upsert.commit = true;
    let (original, error) = transaction(&mut client, &tenant, vec![keyed_upsert.clone()]).await;
    assert!(error.is_none(), "keyed upsert: {error:?}");
    let revision = served_row_revision(&reader, &tenant, MSG, "keyed-upsert")
        .await
        .unwrap();
    let (retry, error) = transaction(&mut client, &tenant, vec![keyed_upsert]).await;
    assert!(error.is_none(), "upsert replay: {error:?}");
    assert_eq!(retry[0].mutation_id, original[0].mutation_id);
    assert_eq!(
        served_row_revision(&reader, &tenant, MSG, "keyed-upsert")
            .await
            .unwrap(),
        revision
    );

    let keyed_delete = Mutation {
        operation: "delete".into(),
        message_type: MSG.into(),
        filter: json_to_struct(&json!({"id":"keyed-upsert"})),
        expected: json_to_struct(&json!({"status":"A"})),
        require_affected: 1,
        idempotency_key: "delete-replay".into(),
        commit: true,
        ..Default::default()
    };
    let (original, error) = transaction(&mut client, &tenant, vec![keyed_delete.clone()]).await;
    assert!(error.is_none(), "keyed delete: {error:?}");
    let (retry, error) = transaction(&mut client, &tenant, vec![keyed_delete]).await;
    assert!(
        error.is_none(),
        "delete replay must succeed after its row is gone: {error:?}"
    );
    assert_eq!(retry[0].mutation_id, original[0].mutation_id);

    let foreign_tenant = Uuid::new_v4().to_string();
    let mut other_scope = upsert("other-tenant", "other-tenant-key");
    other_scope.idempotency_key = "upsert-replay".into();
    other_scope.commit = true;
    let (_, error) = transaction(&mut client, &foreign_tenant, vec![other_scope]).await;
    assert!(
        error.is_none(),
        "another tenant owns its own key: {error:?}"
    );

    let _ = shutdown.send(());
    handle.await.expect("served broker stops");
    teardown(&pool, &schema, &tenant).await;
    teardown(&pool, &schema, &foreign_tenant).await;
}
