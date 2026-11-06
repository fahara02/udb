//! The data-plane contract, proven against a live Postgres: a written value is stored
//! exactly or refused loudly, on every verb, whatever the caller sends. Each test drives
//! the served handlers and then reads the database itself.

use futures::stream;
use serde_json::json;
use tonic::{Code, Request};
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service, install_dp_security,
    serve_data_broker, served_select_rows, served_update, served_upsert, teardown, with_ctx,
};
use crate::generation::{CatalogManifest, ManifestColumn, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_server::DataBroker;
use crate::proto::update_request::Increment;
use crate::proto::{DeleteRequest, Mutation, UpdateRequest, tx_status};
use crate::runtime::executor_utils::json_to_struct;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::ensure_system_catalog;

const MSG: &str = "acme.contract.v1.Row";

struct Fx {
    pool: sqlx::PgPool,
    svc: DataBrokerService,
    manifest: CatalogManifest,
    schema: String,
    tenant: String,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Fx {
    /// A `rows` table with the given extra columns. `ddl` lists them for CREATE TABLE.
    async fn new(ddl: &str, columns: Vec<ManifestColumn>) -> Option<Self> {
        let dsn = dp_live_pg_dsn()?;
        let guard = super::support::live_native_service_db_lock().lock().await;
        install_dp_security();
        let pool = dp_pool(&dsn).await;
        ensure_system_catalog(&pool)
            .await
            .expect("bootstrap system catalog");
        let schema = format!("udb_ct_{}", Uuid::new_v4().simple());
        let tenant = Uuid::new_v4().to_string();
        create_schema(&pool, &schema).await;
        sqlx::raw_sql(&format!(
            "CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL{ddl})"
        ))
        .execute(&pool)
        .await
        .expect("create rows");
        let mut all = vec![col("id", "TEXT", true), col("tenant_id", "TEXT", false)];
        all.extend(columns);
        let manifest = CatalogManifest {
            tables: vec![ManifestTable {
                proto_package: "acme.contract.v1".to_string(),
                message_name: "Row".to_string(),
                schema: schema.clone(),
                table: "rows".to_string(),
                primary_key: vec!["id".to_string()],
                table_security: ManifestTableSecurity {
                    tenant_column: "tenant_id".to_string(),
                    ..ManifestTableSecurity::default()
                },
                columns: all,
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        };
        let svc = dp_service(&dsn, manifest.clone()).await;
        Some(Self {
            pool,
            svc,
            manifest,
            schema,
            tenant,
            _guard: guard,
        })
    }

    fn row(&self, id: &str, fields: serde_json::Value) -> serde_json::Value {
        let mut row = json!({"id": id, "tenant_id": self.tenant});
        for (key, value) in fields.as_object().expect("fields") {
            row[key] = value.clone();
        }
        row
    }

    async fn upsert(
        &self,
        id: &str,
        fields: serde_json::Value,
    ) -> Result<crate::proto::MutationResponse, tonic::Status> {
        served_upsert(&self.svc, &self.tenant, MSG, self.row(id, fields), "").await
    }

    async fn update(
        &self,
        id: &str,
        changes: serde_json::Value,
    ) -> Result<crate::proto::MutationResponse, tonic::Status> {
        served_update(
            &self.svc,
            &self.tenant,
            MSG,
            json!({"id": id, "tenant_id": self.tenant}),
            changes,
        )
        .await
    }

    /// One text value from the row as the database holds it.
    async fn stored(&self, expression: &str, id: &str) -> Option<String> {
        sqlx::query_scalar(&format!(
            "SELECT ({expression})::TEXT FROM \"{}\".rows WHERE id = $1",
            self.schema
        ))
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .unwrap_or_else(|err| panic!("read {expression}: {err}"))
    }

    /// Run mutations in one BeginTx over a real client stream. Returns (committed, error).
    async fn tx(&self, mutations: Vec<Mutation>) -> (bool, Option<tonic::Status>) {
        let dsn = dp_live_pg_dsn().expect("dsn");
        let (mut client, shutdown, handle) =
            serve_data_broker(dp_service(&dsn, self.manifest.clone()).await).await;
        let request: Request<_> = with_ctx(stream::iter(mutations), &self.tenant);
        let mut committed = false;
        let mut failure = None;
        match client.begin_tx(request).await {
            Ok(response) => {
                let mut stream = response.into_inner();
                loop {
                    match stream.message().await {
                        Ok(Some(status)) => {
                            committed |= status.state == tx_status::State::TxStateCommitted as i32;
                        }
                        Ok(None) => break,
                        Err(status) => {
                            failure = Some(status);
                            break;
                        }
                    }
                }
            }
            Err(status) => failure = Some(status),
        }
        let _ = shutdown.send(());
        let _ = handle.await;
        (committed, failure)
    }

    fn tx_upsert(&self, id: &str, fields: serde_json::Value) -> Mutation {
        Mutation {
            message_type: MSG.to_string(),
            operation: "upsert".to_string(),
            record_json: serde_json::to_vec(&self.row(id, fields)).unwrap(),
            commit: true,
            tx_id: Uuid::new_v4().to_string(),
            ..Mutation::default()
        }
    }

    async fn finish(self) {
        teardown(&self.pool, &self.schema, &self.tenant).await;
    }
}

fn typed(name: &str, sql_type: &str) -> ManifestColumn {
    col(name, sql_type, false)
}

/// Rows found by their encrypted value, as a caller allowed to read that column.
async fn secret_rows(fx: &Fx, value: &str) -> crate::proto::RecordSet {
    let request = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"secret": value})),
        ..Default::default()
    };
    let mut request = with_ctx(request, &fx.tenant);
    request.metadata_mut().insert(
        "x-scopes",
        "udb:admin,udb:read,udb:write,udb:pii:read".parse().unwrap(),
    );
    fx.svc
        .select(request)
        .await
        .expect("a lookup by encrypted value")
        .into_inner()
}

fn encrypted_columns() -> (&'static str, Vec<ManifestColumn>) {
    let mut secret = typed("secret", "TEXT");
    secret.encrypted = true;
    let mut secret_idx = typed("secret_idx", "TEXT");
    secret_idx.security.is_blind_index = true;
    (", secret TEXT, secret_idx TEXT", vec![secret, secret_idx])
}

fn refused(result: Result<impl Sized, tonic::Status>, label: &str) {
    match result {
        Ok(_) => panic!("{label}: must be refused, and was accepted"),
        Err(status) => assert_eq!(
            status.code(),
            Code::InvalidArgument,
            "{label}: {}",
            status.message()
        ),
    }
}

macro_rules! fixture {
    ($ddl:expr, $columns:expr) => {
        match Fx::new($ddl, $columns).await {
            Some(fx) => fx,
            None => return,
        }
    };
}

macro_rules! live {
    ($name:ident, $body:block) => {
        #[tokio::test]
        #[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
        async fn $name() $body
    };
}

// ── Values are stored exactly or refused ───────────────────────────────────

live!(begin_tx_stores_json_text_as_the_document_it_encodes_live, {
    let fx = fixture!(", doc JSONB", vec![typed("doc", "JSONB")]);
    let (committed, error) = fx
        .tx(vec![fx.tx_upsert("a", json!({"doc": "{\"a\": 1}"}))])
        .await;
    assert!(committed, "{error:?}");
    assert_eq!(
        fx.stored("jsonb_typeof(doc)", "a").await.as_deref(),
        Some("object")
    );
    fx.finish().await;
});

live!(
    a_fraction_for_an_integer_column_is_refused_on_every_verb_live,
    {
        let fx = fixture!(", n INTEGER", vec![typed("n", "INTEGER")]);
        fx.upsert("a", json!({"n": 1})).await.expect("seed");
        refused(fx.upsert("a", json!({"n": 2.5})).await, "Upsert");
        refused(fx.update("a", json!({"n": 2.5})).await, "Update");
        let (committed, _) = fx.tx(vec![fx.tx_upsert("a", json!({"n": 2.5}))]).await;
        assert!(!committed, "BeginTx must refuse a fraction");
        assert_eq!(fx.stored("n", "a").await.as_deref(), Some("1"));
        fx.finish().await;
    }
);

live!(
    an_integer_beyond_double_precision_is_exact_or_refused_live,
    {
        let fx = fixture!(", big BIGINT", vec![typed("big", "BIGINT")]);
        fx.upsert("a", json!({"big": "9007199254740993"}))
            .await
            .expect("a decimal string is the exact form on Upsert");
        assert_eq!(
            fx.stored("big", "a").await.as_deref(),
            Some("9007199254740993")
        );
        refused(
            fx.update("a", json!({"big": 9_007_199_254_740_994.0_f64}))
                .await,
            "Update with a whole double above 2^53",
        );
        fx.update("a", json!({"big": "9007199254740995"}))
            .await
            .expect("a decimal string is the exact form on Update");
        assert_eq!(
            fx.stored("big", "a").await.as_deref(),
            Some("9007199254740995")
        );
        fx.finish().await;
    }
);

live!(a_non_boolean_is_refused_and_never_stored_as_false_live, {
    let fx = fixture!(", active BOOLEAN", vec![typed("active", "BOOLEAN")]);
    fx.upsert("a", json!({"active": true})).await.expect("seed");
    refused(fx.upsert("a", json!({"active": "true"})).await, "Upsert");
    refused(fx.update("a", json!({"active": "true"})).await, "Update");
    assert_eq!(fx.stored("active", "a").await.as_deref(), Some("true"));
    let request = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"active": "true"})),
        ..Default::default()
    };
    refused(
        fx.svc.select(with_ctx(request, &fx.tenant)).await,
        "a filter must not read it as false",
    );
    fx.finish().await;
});

live!(a_null_array_element_is_refused_on_every_verb_live, {
    let fx = fixture!(
        ", tags TEXT[], flags BOOLEAN[]",
        vec![typed("tags", "TEXT[]"), typed("flags", "BOOLEAN[]")]
    );
    fx.upsert("a", json!({"tags": ["x"]})).await.expect("seed");
    refused(fx.upsert("a", json!({"tags": ["a", null]})).await, "Upsert");
    refused(fx.update("a", json!({"tags": ["a", null]})).await, "Update");
    refused(
        fx.update("a", json!({"flags": [null]})).await,
        "Update flags",
    );
    assert_eq!(fx.stored("tags", "a").await.as_deref(), Some("{x}"));
    fx.finish().await;
});

live!(
    an_empty_string_for_a_typed_column_is_refused_on_every_verb_live,
    {
        let fx = fixture!(
            ", d DATE, ts TIMESTAMPTZ, u UUID, ip INET",
            vec![
                typed("d", "DATE"),
                typed("ts", "TIMESTAMPTZ"),
                typed("u", "UUID"),
                typed("ip", "INET"),
            ]
        );
        fx.upsert("a", json!({"d": "2026-01-02"}))
            .await
            .expect("seed");
        for column in ["d", "ts", "u", "ip"] {
            refused(
                fx.upsert("a", json!({column: ""})).await,
                &format!("Upsert {column}"),
            );
            refused(
                fx.update("a", json!({column: ""})).await,
                &format!("Update {column}"),
            );
        }
        assert_eq!(fx.stored("d", "a").await.as_deref(), Some("2026-01-02"));
        fx.finish().await;
    }
);

// ── Encrypted columns and their blind index ────────────────────────────────

live!(
    begin_tx_writes_the_blind_index_so_the_row_can_be_found_live,
    {
        let (ddl, columns) = encrypted_columns();
        let fx = fixture!(ddl, columns);
        let (committed, error) = fx
            .tx(vec![fx.tx_upsert("a", json!({"secret": "one"}))])
            .await;
        assert!(committed, "{error:?}");
        assert!(
            fx.stored("secret_idx", "a").await.is_some(),
            "no blind index"
        );
        let rows = secret_rows(&fx, "one").await;
        assert_eq!(
            rows.affected_rows_or_len(),
            1,
            "the row must be found by value"
        );
        let (committed, error) = fx
            .tx(vec![fx.tx_upsert("a", json!({"secret": "two"}))])
            .await;
        assert!(committed, "{error:?}");
        let old = secret_rows(&fx, "one").await;
        assert_eq!(
            old.affected_rows_or_len(),
            0,
            "the old value must not match"
        );
        fx.finish().await;
    }
);

live!(clearing_an_encrypted_column_clears_its_blind_index_live, {
    let (ddl, columns) = encrypted_columns();
    let fx = fixture!(ddl, columns);
    fx.upsert("a", json!({"secret": "one"}))
        .await
        .expect("seed");
    fx.update("a", json!({"secret": null}))
        .await
        .expect("clear");
    assert_eq!(fx.stored("secret", "a").await, None);
    assert_eq!(fx.stored("secret_idx", "a").await, None, "a stale token");
    let rows = secret_rows(&fx, "one").await;
    assert_eq!(rows.affected_rows_or_len(), 0);
    fx.finish().await;
});

live!(
    a_tenant_named_only_by_a_column_flag_scopes_the_blind_index_live,
    {
        let (ddl, columns) = encrypted_columns();
        let mut fx = fixture!(ddl, columns);
        fx.manifest.tables[0].table_security.tenant_column.clear();
        let tenant_column = fx.manifest.tables[0]
            .columns
            .iter_mut()
            .find(|column| column.column_name == "tenant_id")
            .expect("tenant column");
        tenant_column.is_tenant_column = true;
        let dsn = dp_live_pg_dsn().expect("dsn");
        fx.svc = dp_service(&dsn, fx.manifest.clone()).await;
        fx.upsert("a", json!({"secret": "one"}))
            .await
            .expect("write");
        let rows = secret_rows(&fx, "one").await;
        assert_eq!(rows.affected_rows_or_len(), 1);
        fx.finish().await;
    }
);

live!(begin_tx_delete_finds_a_row_by_its_encrypted_value_live, {
    let (ddl, columns) = encrypted_columns();
    let fx = fixture!(ddl, columns);
    fx.upsert("a", json!({"secret": "one"}))
        .await
        .expect("seed");
    let delete = Mutation {
        message_type: MSG.to_string(),
        operation: "delete".to_string(),
        filter: json_to_struct(&json!({"secret": "one"})),
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    };
    let (committed, error) = fx.tx(vec![delete]).await;
    assert!(committed, "{error:?}");
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\".rows", fx.schema))
        .fetch_one(&fx.pool)
        .await
        .expect("count");
    assert_eq!(left, 0);
    fx.finish().await;
});

// ── Counters ───────────────────────────────────────────────────────────────

fn increment(fx: &Fx, id: &str, column: &str, delta: f64) -> UpdateRequest {
    UpdateRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"id": id, "tenant_id": fx.tenant})),
        increments: vec![Increment {
            column: column.to_string(),
            delta,
        }],
        ..UpdateRequest::default()
    }
}

live!(incrementing_a_null_counter_counts_from_zero_live, {
    let fx = fixture!(", n BIGINT", vec![typed("n", "BIGINT")]);
    fx.upsert("a", json!({})).await.expect("seed");
    fx.svc
        .update(with_ctx(increment(&fx, "a", "n", 2.0), &fx.tenant))
        .await
        .expect("increment");
    assert_eq!(fx.stored("n", "a").await.as_deref(), Some("2"));
    fx.finish().await;
});

live!(a_non_finite_increment_is_refused_live, {
    let fx = fixture!(", n BIGINT", vec![typed("n", "BIGINT")]);
    fx.upsert("a", json!({"n": 5})).await.expect("seed");
    for delta in [f64::NAN, f64::INFINITY] {
        refused(
            fx.svc
                .update(with_ctx(increment(&fx, "a", "n", delta), &fx.tenant))
                .await,
            "a non-finite delta",
        );
    }
    assert_eq!(fx.stored("n", "a").await.as_deref(), Some("5"));
    fx.finish().await;
});

// ── Writes stay inside their limits ────────────────────────────────────────

live!(update_refuses_a_column_marked_exclude_from_update_live, {
    let mut locked = typed("locked", "TEXT");
    locked.exclude_from_update = true;
    let fx = fixture!(", locked TEXT", vec![locked]);
    fx.upsert("a", json!({"locked": "first"}))
        .await
        .expect("seed");
    refused(fx.update("a", json!({"locked": "second"})).await, "Update");
    assert_eq!(fx.stored("locked", "a").await.as_deref(), Some("first"));
    fx.finish().await;
});

live!(a_non_string_tenant_cannot_write_into_another_tenant_live, {
    let fx = fixture!("", Vec::new());
    let foreign = json!({"id": "a", "tenant_id": 12345});
    let error = served_upsert(&fx.svc, &fx.tenant, MSG, foreign.clone(), "")
        .await
        .expect_err("a numeric tenant value must not bypass the scope check");
    assert_eq!(error.code(), Code::PermissionDenied, "{}", error.message());
    let write = Mutation {
        message_type: MSG.to_string(),
        operation: "upsert".to_string(),
        record_json: serde_json::to_vec(&foreign).unwrap(),
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    };
    let (committed, _) = fx.tx(vec![write]).await;
    assert!(!committed, "BeginTx must refuse it too");
    let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\".rows", fx.schema))
        .fetch_one(&fx.pool)
        .await
        .expect("count");
    assert_eq!(rows, 0);
    fx.finish().await;
});

live!(
    a_range_update_moves_the_revision_of_the_rows_it_changed_live,
    {
        let fx = fixture!(", status TEXT", vec![typed("status", "TEXT")]);
        fx.upsert("a", json!({"status": "old"}))
            .await
            .expect("seed");
        let rows = served_select_rows(&fx.svc, &fx.tenant, MSG, json!({"id": "a"}), true).await;
        let revision = rows.record_revisions.first().cloned().expect("a revision");
        served_update(
            &fx.svc,
            &fx.tenant,
            MSG,
            json!({"status": "old"}),
            json!({"status": "new"}),
        )
        .await
        .expect("range update");
        let stale = UpdateRequest {
            message_type: MSG.to_string(),
            filter: json_to_struct(&json!({"id": "a", "tenant_id": fx.tenant})),
            changes: json_to_struct(&json!({"status": "lost-update"})),
            expected_revision: revision,
            ..UpdateRequest::default()
        };
        let error = fx
            .svc
            .update(with_ctx(stale, &fx.tenant))
            .await
            .expect_err("a stale revision must not write");
        assert_eq!(
            error.code(),
            Code::FailedPrecondition,
            "{}",
            error.message()
        );
        assert_eq!(fx.stored("status", "a").await.as_deref(), Some("new"));
        fx.finish().await;
    }
);

// ── A failure names itself ─────────────────────────────────────────────────

live!(database_refusals_map_to_their_own_codes_live, {
    let fx = fixture!(
        ", n SMALLINT CHECK (n < 100), name VARCHAR(3)",
        vec![typed("n", "SMALLINT"), typed("name", "VARCHAR(3)")]
    );
    fx.upsert("a", json!({"n": 1})).await.expect("seed");
    for (label, changes) in [
        ("CHECK", json!({"n": 200})),
        ("out of range", json!({"n": 100000})),
        ("too long", json!({"name": "toolong"})),
    ] {
        let error = fx
            .update("a", changes)
            .await
            .expect_err("the database refuses it");
        assert_ne!(error.code(), Code::Internal, "{label}: {}", error.message());
        assert_ne!(error.code(), Code::Ok, "{label}");
    }
    fx.finish().await;
});

live!(a_delete_blocked_by_a_foreign_key_is_not_internal_live, {
    let fx = fixture!("", Vec::new());
    sqlx::raw_sql(&format!(
        "CREATE TABLE \"{0}\".kids (id TEXT PRIMARY KEY, parent TEXT REFERENCES \"{0}\".rows (id))",
        fx.schema
    ))
    .execute(&fx.pool)
    .await
    .expect("child table");
    fx.upsert("a", json!({})).await.expect("seed");
    sqlx::query(&format!(
        "INSERT INTO \"{}\".kids VALUES ('k', 'a')",
        fx.schema
    ))
    .execute(&fx.pool)
    .await
    .expect("seed child");
    let request = DeleteRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"id": "a", "tenant_id": fx.tenant})),
        ..DeleteRequest::default()
    };
    let error = fx
        .svc
        .delete(with_ctx(request, &fx.tenant))
        .await
        .expect_err("the foreign key blocks it");
    assert_eq!(
        error.code(),
        Code::FailedPrecondition,
        "{}",
        error.message()
    );
    assert!(
        !error.message().contains("violates foreign key"),
        "driver text must not reach the client: {}",
        error.message()
    );
    fx.finish().await;
});

// ── Filters ────────────────────────────────────────────────────────────────

async fn select(
    fx: &Fx,
    filter: serde_json::Value,
) -> Result<crate::proto::RecordSet, tonic::Status> {
    let request = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&filter),
        ..Default::default()
    };
    fx.svc
        .select(with_ctx(request, &fx.tenant))
        .await
        .map(tonic::Response::into_inner)
}

live!(a_bigint_filter_never_matches_a_rounded_neighbour_live, {
    let fx = fixture!(", big BIGINT", vec![typed("big", "BIGINT")]);
    fx.upsert("a", json!({"big": "9007199254740992"}))
        .await
        .expect("a");
    fx.upsert("b", json!({"big": "9007199254740993"}))
        .await
        .expect("b");
    refused(
        select(&fx, json!({"big": 9_007_199_254_740_993_i64})).await,
        "a double above 2^53",
    );
    let exact = select(&fx, json!({"big": "9007199254740993"}))
        .await
        .expect("exact");
    assert_eq!(exact.records_json.len(), 1);
    fx.finish().await;
});

live!(an_operator_outside_a_column_is_refused_not_ignored_live, {
    let fx = fixture!(", status TEXT", vec![typed("status", "TEXT")]);
    fx.upsert("a", json!({"status": "closed"}))
        .await
        .expect("seed");
    refused(
        select(&fx, json!({"$not": {"status": "closed"}})).await,
        "$not",
    );
    fx.finish().await;
});

live!(comparing_with_null_is_refused_and_is_null_works_live, {
    let fx = fixture!(", status TEXT", vec![typed("status", "TEXT")]);
    fx.upsert("a", json!({})).await.expect("null row");
    fx.upsert("b", json!({"status": "x"}))
        .await
        .expect("value row");
    refused(
        select(&fx, json!({"status": {"$eq": null}})).await,
        "$eq null",
    );
    refused(
        select(&fx, json!({"status": {"$ne": null}})).await,
        "$ne null",
    );
    let nulls = select(&fx, json!({"status": {"$is_null": true}}))
        .await
        .expect("is_null");
    assert_eq!(nulls.records_json.len(), 1);
    fx.finish().await;
});

live!(
    a_filter_value_the_column_cannot_hold_is_not_internal_live,
    {
        let fx = fixture!(", d DATE", vec![typed("d", "DATE")]);
        fx.upsert("a", json!({"d": "2026-01-02"}))
            .await
            .expect("seed");
        refused(select(&fx, json!({"d": "not-a-date"})).await, "a bad date");
        fx.finish().await;
    }
);

// ── Every column type round-trips ──────────────────────────────────────────

/// Upsert, Update and BeginTx store the same value, and it reads back unchanged.
async fn round_trip(sql_type: &str, ddl_type: &str, value: serde_json::Value, expected: &str) {
    let ddl = format!(", v {ddl_type}");
    let Some(fx) = Fx::new(&ddl, vec![typed("v", sql_type)]).await else {
        return;
    };
    fx.upsert("up", json!({"v": value}))
        .await
        .unwrap_or_else(|err| panic!("{sql_type} Upsert: {err:?}"));
    assert_eq!(
        fx.stored("v", "up").await.as_deref(),
        Some(expected),
        "{sql_type} Upsert"
    );

    fx.upsert("upd", json!({})).await.expect("seed");
    fx.update("upd", json!({"v": value}))
        .await
        .unwrap_or_else(|err| panic!("{sql_type} Update: {err:?}"));
    assert_eq!(
        fx.stored("v", "upd").await.as_deref(),
        Some(expected),
        "{sql_type} Update"
    );

    let (committed, error) = fx.tx(vec![fx.tx_upsert("tx", json!({"v": value}))]).await;
    assert!(committed, "{sql_type} BeginTx: {error:?}");
    assert_eq!(
        fx.stored("v", "tx").await.as_deref(),
        Some(expected),
        "{sql_type} BeginTx"
    );
    fx.finish().await;
}

macro_rules! round_trips {
    ($($name:ident: $sql:expr, $ddl:expr, $value:expr, $expected:expr;)*) => {
        $(live!($name, {
            round_trip($sql, $ddl, $value, $expected).await;
        });)*
    };
}

round_trips! {
    round_trip_bytea_live: "BYTEA", "BYTEA", json!("aGk="), "\\x6869";
    round_trip_bigint_string_live: "BIGINT", "BIGINT", json!("9007199254740993"), "9007199254740993";
    round_trip_numeric_live: "NUMERIC(20,6)", "NUMERIC(20,6)", json!("12345678901234.123456"), "12345678901234.123456";
    round_trip_timestamptz3_live: "TIMESTAMPTZ(3)", "TIMESTAMPTZ(3)", json!("2026-01-02T03:04:05.123Z"), "2026-01-02 03:04:05.123+00";
    round_trip_timestamp3_live: "TIMESTAMP(3)", "TIMESTAMP(3)", json!("2026-01-02T03:04:05.123"), "2026-01-02 03:04:05.123";
    round_trip_date_live: "DATE", "DATE", json!("2026-01-02"), "2026-01-02";
    round_trip_time_live: "TIME", "TIME", json!("10:00:00"), "10:00:00";
    round_trip_timetz_live: "TIMETZ", "TIMETZ", json!("10:00:00+00"), "10:00:00+00";
    round_trip_interval_live: "INTERVAL", "INTERVAL", json!("1 day"), "1 day";
    round_trip_inet_live: "INET", "INET", json!("10.0.0.1"), "10.0.0.1/32";
    round_trip_cidr_live: "CIDR", "CIDR", json!("10.0.0.0/8"), "10.0.0.0/8";
    round_trip_macaddr_live: "MACADDR", "MACADDR", json!("08:00:2b:01:02:03"), "08:00:2b:01:02:03";
    round_trip_uuid_live: "UUID", "UUID", json!("11111111-1111-1111-1111-111111111111"), "11111111-1111-1111-1111-111111111111";
    round_trip_json_object_live: "JSON", "JSON", json!({"a": 1}), "{\"a\": 1}";
    round_trip_jsonb_array_live: "JSONB", "JSONB", json!([1, 2]), "[1, 2]";
    round_trip_text_array_live: "TEXT[]", "TEXT[]", json!(["a", "b"]), "{a,b}";
    round_trip_empty_text_array_live: "TEXT[]", "TEXT[]", json!([]), "{}";
    round_trip_int_array_live: "INTEGER[]", "INTEGER[]", json!([1, 2]), "{1,2}";
    round_trip_real_live: "REAL", "REAL", json!(1.5), "1.5";
    round_trip_double_live: "DOUBLE PRECISION", "DOUBLE PRECISION", json!(0.1), "0.1";
    round_trip_empty_text_live: "TEXT", "TEXT", json!(""), "";
    round_trip_boolean_live: "BOOLEAN", "BOOLEAN", json!(false), "false";
}

// ── Column kinds in filters ────────────────────────────────────────────────

live!(a_json_key_filter_finds_the_row_live, {
    let fx = fixture!(", doc JSONB", vec![typed("doc", "JSONB")]);
    fx.upsert("a", json!({"doc": {"k": 1}})).await.expect("a");
    fx.upsert("b", json!({"doc": {"other": 1}}))
        .await
        .expect("b");
    let rows = select(&fx, json!({"doc": {"$has_key": "k"}}))
        .await
        .expect("has_key");
    assert_eq!(rows.records_json.len(), 1);
    fx.finish().await;
});

live!(an_array_column_filters_by_containment_live, {
    let fx = fixture!(", tags TEXT[]", vec![typed("tags", "TEXT[]")]);
    fx.upsert("a", json!({"tags": ["x", "y"]}))
        .await
        .expect("a");
    fx.upsert("b", json!({"tags": ["z"]})).await.expect("b");
    let rows = select(&fx, json!({"tags": {"$contains": ["x"]}}))
        .await
        .expect("$contains on an array");
    assert_eq!(rows.records_json.len(), 1);
    fx.finish().await;
});

live!(a_json_column_filters_by_containment_live, {
    let fx = fixture!(", doc JSONB", vec![typed("doc", "JSONB")]);
    fx.upsert("a", json!({"doc": {"k": 1}})).await.expect("a");
    fx.upsert("b", json!({"doc": {"k": 2}})).await.expect("b");
    let rows = select(&fx, json!({"doc": {"$contains": {"k": 1}}}))
        .await
        .expect("$contains on JSONB");
    assert_eq!(rows.records_json.len(), 1);
    fx.finish().await;
});

live!(typed_columns_compare_with_a_text_value_live, {
    let fx = fixture!(
        ", t TIME, iv INTERVAL, ip INET",
        vec![
            typed("t", "TIME"),
            typed("iv", "INTERVAL"),
            typed("ip", "INET")
        ]
    );
    fx.upsert(
        "a",
        json!({"t": "10:00:00", "iv": "1 day", "ip": "10.0.0.1"}),
    )
    .await
    .expect("seed");
    for filter in [
        json!({"t": "10:00:00"}),
        json!({"iv": "1 day"}),
        json!({"ip": "10.0.0.1"}),
    ] {
        let rows = select(&fx, filter.clone())
            .await
            .unwrap_or_else(|err| panic!("{filter}: {err:?}"));
        assert_eq!(rows.records_json.len(), 1, "{filter}");
    }
    fx.finish().await;
});

// ── Paged reads of protected columns ───────────────────────────────────────

live!(a_paged_sort_on_an_encrypted_column_is_refused_live, {
    let (ddl, columns) = encrypted_columns();
    let fx = fixture!(ddl, columns);
    fx.upsert("a", json!({"secret": "one"}))
        .await
        .expect("seed");
    let request = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        limit: 1,
        sort: vec![crate::proto::Sort {
            field: "secret".to_string(),
            descending: false,
        }],
        ..Default::default()
    };
    let mut request = with_ctx(request, &fx.tenant);
    request.metadata_mut().insert(
        "x-scopes",
        "udb:admin,udb:read,udb:write,udb:pii:read".parse().unwrap(),
    );
    refused(fx.svc.select(request).await, "sort by ciphertext");
    fx.finish().await;
});

// ── Masked values do not come back ─────────────────────────────────────────

live!(a_redaction_placeholder_is_never_stored_live, {
    let mut pii = typed("email", "TEXT");
    pii.security.is_pii = true;
    let fx = fixture!(", email TEXT", vec![pii]);
    fx.upsert("a", json!({"email": "real@x"}))
        .await
        .expect("seed");
    refused(
        fx.update("a", json!({"email": "***MASKED***"})).await,
        "a masked value written back",
    );
    refused(
        fx.upsert("a", json!({"email": "***MASKED***"})).await,
        "a masked value upserted",
    );
    assert_eq!(fx.stored("email", "a").await.as_deref(), Some("real@x"));
    fx.finish().await;
});

// ── Joins ──────────────────────────────────────────────────────────────────

/// `notes` joined to `authors` on `author_id`, with a PII and an encrypted author column.
async fn join_fixture() -> Option<Fx> {
    let mut fx = Fx::new(", author_id TEXT", vec![typed("author_id", "TEXT")]).await?;
    sqlx::raw_sql(&format!(
        "CREATE TABLE \"{0}\".authors (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, \
         name TEXT, email TEXT, secret TEXT)",
        fx.schema
    ))
    .execute(&fx.pool)
    .await
    .expect("authors");
    let mut email = typed("email", "TEXT");
    email.security.is_pii = true;
    let mut secret = typed("secret", "TEXT");
    secret.encrypted = true;
    secret.security.is_encrypted = true;
    fx.manifest.tables[0].foreign_keys = vec![crate::generation::ManifestForeignKey {
        name: "fk_rows_authors".to_string(),
        columns: vec!["author_id".to_string()],
        ref_schema: fx.schema.clone(),
        ref_table: "authors".to_string(),
        ref_columns: vec!["id".to_string()],
        ..Default::default()
    }];
    fx.manifest.tables.push(ManifestTable {
        proto_package: "acme.contract.v1".to_string(),
        message_name: "Author".to_string(),
        schema: fx.schema.clone(),
        table: "authors".to_string(),
        primary_key: vec!["id".to_string()],
        table_security: ManifestTableSecurity {
            tenant_column: "tenant_id".to_string(),
            ..ManifestTableSecurity::default()
        },
        columns: vec![
            col("id", "TEXT", true),
            col("tenant_id", "TEXT", false),
            typed("name", "TEXT"),
            email,
            secret,
        ],
        ..ManifestTable::default()
    });
    let dsn = dp_live_pg_dsn().expect("dsn");
    fx.svc = dp_service(&dsn, fx.manifest.clone()).await;
    for index in 0..3 {
        sqlx::query(&format!(
            "INSERT INTO \"{0}\".authors VALUES ($1, $2, 'n', 'real@x', 'udb-aead:v1:c'); \
             ",
            fx.schema
        ))
        .bind(format!("author-{index}"))
        .bind(&fx.tenant)
        .execute(&fx.pool)
        .await
        .expect("author");
        sqlx::query(&format!(
            "INSERT INTO \"{0}\".rows (id, tenant_id, author_id) VALUES ($1, $2, $3)",
            fx.schema
        ))
        .bind(format!("note-{index}"))
        .bind(&fx.tenant)
        .bind(format!("author-{index}"))
        .execute(&fx.pool)
        .await
        .expect("note");
    }
    Some(fx)
}

fn join_request(fields: &[&str], limit: i32) -> crate::proto::SelectRequest {
    crate::proto::SelectRequest {
        message_type: "acme.contract.v1.Row,acme.contract.v1.Author".to_string(),
        fields: fields.iter().map(|field| field.to_string()).collect(),
        limit,
        ..Default::default()
    }
}

live!(a_join_masks_a_pii_field_however_it_is_spelled_live, {
    let Some(fx) = join_fixture().await else {
        return;
    };
    for field in ["acme.contract.v1.Author.email", "Author.email"] {
        let response = fx
            .svc
            .select(with_ctx(join_request(&[field], 10), &fx.tenant))
            .await;
        if let Ok(rows) = response {
            for raw in &rows.into_inner().records_json {
                let text = String::from_utf8_lossy(raw);
                assert!(!text.contains("real@x"), "{field} leaked: {text}");
            }
        }
    }
    fx.finish().await;
});

live!(
    a_join_refuses_an_encrypted_field_rather_than_return_ciphertext_live,
    {
        let Some(fx) = join_fixture().await else {
            return;
        };
        let mut request = with_ctx(join_request(&["Author.secret"], 10), &fx.tenant);
        request.metadata_mut().insert(
            "x-scopes",
            "udb:admin,udb:read,udb:write,udb:pii:read".parse().unwrap(),
        );
        refused(fx.svc.select(request).await, "an encrypted join field");
        fx.finish().await;
    }
);

live!(a_join_says_when_it_stopped_at_its_limit_live, {
    let Some(fx) = join_fixture().await else {
        return;
    };
    let rows = fx
        .svc
        .select(with_ctx(join_request(&[], 2), &fx.tenant))
        .await
        .expect("join")
        .into_inner();
    assert_eq!(rows.records_json.len(), 2);
    assert!(rows.has_more, "more rows match than the limit");
    let mut sorted = join_request(&[], 2);
    sorted.sort = vec![crate::proto::Sort {
        field: "id".to_string(),
        descending: true,
    }];
    refused(
        fx.svc.select(with_ctx(sorted, &fx.tenant)).await,
        "a join cannot honour a sort",
    );
    fx.finish().await;
});

// ── Field names that differ from their column ──────────────────────────────

live!(begin_tx_delete_binds_values_to_their_own_columns_live, {
    let mut zone = typed("zone", "TEXT");
    zone.field_name = "Zone".to_string();
    let mut alias = typed("region", "TEXT");
    alias.field_name = "alias_aaa".to_string();
    let fx = fixture!(", zone TEXT, region TEXT", vec![zone, alias]);
    sqlx::query(&format!(
        "INSERT INTO \"{}\".rows VALUES ('a', $1, 'x', 'y')",
        fx.schema
    ))
    .bind(&fx.tenant)
    .execute(&fx.pool)
    .await
    .expect("seed");
    let delete = Mutation {
        message_type: MSG.to_string(),
        operation: "delete".to_string(),
        filter: json_to_struct(&json!({"Zone": "x", "alias_aaa": "y"})),
        commit: true,
        tx_id: Uuid::new_v4().to_string(),
        ..Mutation::default()
    };
    let (committed, error) = fx.tx(vec![delete]).await;
    assert!(committed, "{error:?}");
    let left: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM \"{}\".rows", fx.schema))
        .fetch_one(&fx.pool)
        .await
        .expect("count");
    assert_eq!(left, 0, "the delete must match its row");
    fx.finish().await;
});

// ── Paging ─────────────────────────────────────────────────────────────────

live!(a_page_token_continues_the_walk_without_a_limit_live, {
    let fx = fixture!("", Vec::new());
    for id in ["a", "b", "c"] {
        fx.upsert(id, json!({})).await.expect("seed");
    }
    let first = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        limit: 2,
        ..Default::default()
    };
    let page = fx
        .svc
        .select(with_ctx(first, &fx.tenant))
        .await
        .expect("page one")
        .into_inner();
    assert_eq!(page.records_json.len(), 2);
    assert!(!page.next_page_token.is_empty());
    let next = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        page_token: page.next_page_token.clone(),
        ..Default::default()
    };
    let page_two = fx
        .svc
        .select(with_ctx(next, &fx.tenant))
        .await
        .expect("a token alone continues")
        .into_inner();
    assert_eq!(page_two.records_json.len(), 1);
    fx.finish().await;
});

live!(an_unprefixed_is_null_keeps_the_total_working_live, {
    let fx = fixture!(", status TEXT", vec![typed("status", "TEXT")]);
    fx.upsert("a", json!({})).await.expect("seed");
    let request = crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        filter: json_to_struct(&json!({"status": {"is_null": true}})),
        include_total: true,
        ..Default::default()
    };
    let rows = fx
        .svc
        .select(with_ctx(request, &fx.tenant))
        .await
        .expect("rows and a total")
        .into_inner();
    assert_eq!(rows.exact_total, 1);
    fx.finish().await;
});

// ── What was stored is what a read returns ─────────────────────────────────

/// Insert through SQL, read through the served Select, and compare the JSON value.
async fn read_back(ddl_type: &str, literal: &str, expected: serde_json::Value) {
    let ddl = format!(", v {ddl_type}");
    let Some(fx) = Fx::new(&ddl, vec![typed("v", ddl_type)]).await else {
        return;
    };
    sqlx::query(&format!(
        "INSERT INTO \"{}\".rows (id, tenant_id, v) VALUES ('a', '{}', {literal})",
        fx.schema, fx.tenant
    ))
    .execute(&fx.pool)
    .await
    .unwrap_or_else(|err| panic!("{ddl_type} insert: {err}"));
    let rows = select(&fx, json!({"id": "a"}))
        .await
        .unwrap_or_else(|err| panic!("{ddl_type} read: {err:?}"));
    let record: serde_json::Value =
        serde_json::from_slice(rows.records_json.first().expect("one row")).expect("record");
    assert_eq!(record["v"], expected, "{ddl_type}");
    fx.finish().await;
}

macro_rules! read_backs {
    ($($name:ident: $ddl:expr, $literal:expr, $expected:expr;)*) => {
        $(live!($name, {
            read_back($ddl, $literal, $expected).await;
        });)*
    };
}

read_backs! {
    read_time_live: "TIME", "'12:30:00'", json!("12:30:00");
    read_time_fraction_live: "TIME", "'12:30:00.25'", json!("12:30:00.25");
    read_timetz_live: "TIMETZ", "'12:30:00+02'", json!("12:30:00+02");
    read_interval_live: "INTERVAL", "'1 year 2 mons 3 days 04:05:06'", json!("1 year 2 mons 3 days 04:05:06");
    read_interval_zero_live: "INTERVAL", "'0'", json!("00:00:00");
    read_money_live: "MONEY", "'12.34'", json!("12.34");
    read_bit_live: "BIT(4)", "B'1010'", json!("1010");
    read_varbit_live: "VARBIT", "B'101'", json!("101");
    read_xml_live: "XML", "'<a>1</a>'", json!("<a>1</a>");
    read_tsvector_live: "TSVECTOR", "'cat:1 dog:2'::tsvector", json!("'cat':1 'dog':2");
    read_real_live: "REAL", "0.1", json!(0.1);
    read_real_nan_live: "REAL", "'NaN'", json!("NaN");
    read_double_infinity_live: "DOUBLE PRECISION", "'Infinity'", json!("Infinity");
    read_ipv6_live: "INET", "'2001:db8::1'", json!("2001:db8::1");
    read_inet_array_live: "INET[]", "ARRAY['10.0.0.1'::inet, NULL]", json!(["10.0.0.1", null]);
    read_jsonb_array_live: "JSONB[]", "ARRAY['{\"a\":1}'::jsonb]", json!([{"a": 1}]);
    read_interval_array_live: "INTERVAL[]", "ARRAY['1 day'::interval]", json!(["1 day"]);
    read_bytea_array_live: "BYTEA[]", "ARRAY['hi'::bytea]", json!(["aGk="]);
    read_empty_inet_array_live: "INET[]", "'{}'", json!([]);
}

live!(a_custom_enum_column_round_trips_on_every_verb_live, {
    let fx = fixture!("", Vec::new());
    let type_name = format!("mood_{}", Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE TYPE \"{0}\".\"{type_name}\" AS ENUM ('happy','sad'); \
         ALTER TABLE \"{0}\".rows ADD COLUMN mood \"{0}\".\"{type_name}\"",
        fx.schema
    ))
    .execute(&fx.pool)
    .await
    .expect("enum column");
    let mut manifest = fx.manifest.clone();
    manifest.tables[0]
        .columns
        .push(typed("mood", &format!("\"{}\".\"{type_name}\"", fx.schema)));
    let dsn = dp_live_pg_dsn().expect("dsn");
    let svc = dp_service(&dsn, manifest).await;
    served_upsert(
        &svc,
        &fx.tenant,
        MSG,
        fx.row("a", json!({"mood": "happy"})),
        "",
    )
    .await
    .expect("Upsert an enum value");
    assert_eq!(fx.stored("mood", "a").await.as_deref(), Some("happy"));
    served_update(
        &svc,
        &fx.tenant,
        MSG,
        json!({"id": "a", "tenant_id": fx.tenant}),
        json!({"mood": "sad"}),
    )
    .await
    .expect("Update an enum value");
    assert_eq!(fx.stored("mood", "a").await.as_deref(), Some("sad"));
    fx.finish().await;
});

trait RowCount {
    fn affected_rows_or_len(&self) -> usize;
}

impl RowCount for crate::proto::RecordSet {
    fn affected_rows_or_len(&self) -> usize {
        self.records_json.len()
    }
}

// ── Replays, totals and routing ────────────────────────────────────────────

live!(a_replayed_bulk_cas_returns_its_per_item_results_live, {
    let fx = fixture!(", note TEXT", vec![typed("note", "TEXT")]);
    fx.upsert("a", json!({"note": "old"})).await.expect("seed");
    let key = format!("bulk-{}", Uuid::new_v4());
    let request = || crate::proto::BulkCasRequest {
        message_type: MSG.to_string(),
        idempotency_key: key.clone(),
        items: vec![
            crate::proto::BulkCasItem {
                filter: json_to_struct(&json!({"id": "a", "tenant_id": fx.tenant})),
                changes: json_to_struct(&json!({"note": "new"})),
                expected: json_to_struct(&json!({"note": "old"})),
                ..Default::default()
            },
            crate::proto::BulkCasItem {
                filter: json_to_struct(&json!({"id": "missing", "tenant_id": fx.tenant})),
                changes: json_to_struct(&json!({"note": "new"})),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let first = fx
        .svc
        .bulk_cas(with_ctx(request(), &fx.tenant))
        .await
        .expect("the batch")
        .into_inner();
    let replay = fx
        .svc
        .bulk_cas(with_ctx(request(), &fx.tenant))
        .await
        .expect("the replay")
        .into_inner();
    assert_eq!(first.results.len(), 2);
    assert_eq!(replay.results, first.results, "the replay lost its results");
    assert_eq!(replay.changed, first.changed);
    fx.finish().await;
});

live!(select_v2_reports_the_total_and_the_next_page_live, {
    use futures::StreamExt;
    let fx = fixture!(", note TEXT", vec![typed("note", "TEXT")]);
    for id in ["a", "b", "c"] {
        fx.upsert(id, json!({"note": "x"})).await.expect("seed");
    }
    let mut batches = fx
        .svc
        .select_v2(with_ctx(
            crate::proto::SelectRequest {
                message_type: MSG.to_string(),
                limit: 2,
                include_total: true,
                ..Default::default()
            },
            &fx.tenant,
        ))
        .await
        .expect("SelectV2")
        .into_inner();
    let batch = batches.next().await.expect("a batch").expect("no error");
    assert_eq!(batch.row_count, 2);
    assert_eq!(batch.total_count, 3, "the total counts every match");
    assert!(!batch.next_page_token.is_empty(), "there is a next page");
    fx.finish().await;
});

live!(
    routing_policy_primary_reads_the_primary_while_eventual_reads_a_replica_live,
    {
        let fx = fixture!(", note TEXT", vec![typed("note", "TEXT")]);
        fx.upsert("a", json!({"note": "on primary"}))
            .await
            .expect("seed");
        // A second database stands in for a replica: the same table, holding another value.
        let dsn = dp_live_pg_dsn().expect("dsn");
        let database = format!("udb_ct_replica_{}", Uuid::new_v4().simple());
        sqlx::raw_sql(&format!("CREATE DATABASE \"{database}\""))
            .execute(&fx.pool)
            .await
            .expect("a database for the replica");
        let (base, _) = dsn.rsplit_once('/').expect("a DSN with a database");
        let replica_dsn = format!("{base}/{database}");
        let replica = dp_pool(&replica_dsn).await;
        sqlx::raw_sql(&format!(
            "CREATE SCHEMA \"{schema}\"; \
         CREATE TABLE \"{schema}\".rows (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, note TEXT); \
         INSERT INTO \"{schema}\".rows VALUES ('a', '{tenant}', 'on replica')",
            schema = fx.schema,
            tenant = fx.tenant,
        ))
        .execute(&replica)
        .await
        .expect("the replica's copy");
        let svc = super::data_plane_live::dp_service_with(&dsn, fx.manifest.clone(), |cfg| {
            cfg.pg_replica_dsns = vec![replica_dsn.clone()];
            cfg.pg_replica_fail_open = true;
        })
        .await;
        let read = |routing: &'static str| {
            let svc = &svc;
            let tenant = fx.tenant.clone();
            async move {
                let set = svc
                    .select(with_ctx(
                        crate::proto::SelectRequest {
                            context: Some(crate::proto::RequestContext {
                                tenant_id: tenant.clone(),
                                consistency: "eventual".to_string(),
                                eventual_consistency_allowed: true,
                                routing_policy: routing.to_string(),
                                ..Default::default()
                            }),
                            message_type: MSG.to_string(),
                            filter: json_to_struct(&json!({"id": "a"})),
                            ..Default::default()
                        },
                        &tenant,
                    ))
                    .await
                    .expect("a read")
                    .into_inner();
                let row: serde_json::Value =
                    serde_json::from_slice(&set.records_json[0]).expect("a JSON row");
                row["note"].as_str().unwrap_or_default().to_string()
            }
        };
        assert_eq!(read("").await, "on replica");
        assert_eq!(read("primary").await, "on primary");
        drop(svc);
        replica.close().await;
        let _ = sqlx::raw_sql(&format!("DROP DATABASE \"{database}\" WITH (FORCE)"))
            .execute(&fx.pool)
            .await;
        fx.finish().await;
    }
);

// ── Audited cross-tenant reads ─────────────────────────────────────────────

fn cross_tenant_request(target: &str, reason: &str) -> Request<crate::proto::SelectRequest> {
    let mut request = Request::new(crate::proto::SelectRequest {
        message_type: MSG.to_string(),
        limit: 10,
        ..Default::default()
    });
    let metadata = request.metadata_mut();
    metadata.insert("x-tenant-id", Uuid::new_v4().to_string().parse().unwrap());
    metadata.insert("x-purpose", "admin".parse().unwrap());
    metadata.insert(
        "x-scopes",
        "udb:admin,udb:read,udb:cross_tenant:read".parse().unwrap(),
    );
    metadata.insert(
        crate::runtime::service::cross_tenant_read::TARGET_HEADER,
        target.parse().unwrap(),
    );
    metadata.insert(
        crate::runtime::service::cross_tenant_read::REASON_HEADER,
        reason.parse().unwrap(),
    );
    request
}

live!(
    a_platform_operator_reads_one_other_tenant_and_the_read_is_on_record_live,
    {
        use crate::runtime::service::method_security::{
            scope_claim_context_for_test, test_claim_context,
        };
        let fx = fixture!(", note TEXT", vec![typed("note", "TEXT")]);
        fx.upsert("mine", json!({"note": "theirs"}))
            .await
            .expect("seed");
        let other = Uuid::new_v4().to_string();
        served_upsert(
            &fx.svc,
            &other,
            MSG,
            json!({"id": "elsewhere", "tenant_id": other, "note": "not this tenant"}),
            "",
        )
        .await
        .expect("another tenant's row");
        let reason = format!("ticket {}", Uuid::new_v4());
        let operator = test_claim_context(
            "ops",
            "",
            "",
            &["udb:platform_admin", "udb:admin", "udb:cross_tenant:read"],
            &[],
        );
        let set = scope_claim_context_for_test(
            operator,
            fx.svc.select(cross_tenant_request(&fx.tenant, &reason)),
        )
        .await
        .expect("an operator's read")
        .into_inner();
        assert_eq!(set.records_json.len(), 1, "only the named tenant's rows");
        let row: serde_json::Value = serde_json::from_slice(&set.records_json[0]).unwrap();
        assert_eq!(row["id"], "mine");
        let recorded: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM udb_system.auth_audit_log \
         WHERE event_type = 'udb.data.cross_tenant_read' AND outcome = 'allowed' \
         AND envelope ->> 'reason' = $1 AND envelope ->> 'target_tenant' = $2",
        )
        .bind(&reason)
        .bind(&fx.tenant)
        .fetch_one(&fx.pool)
        .await
        .expect("the audit log");
        assert_eq!(recorded, 1, "the read is on record");

        // Authority alone, without the scope.
        let plain_admin = test_claim_context("ops", "", "", &["udb:platform_admin"], &[]);
        let refused = scope_claim_context_for_test(
            plain_admin,
            fx.svc.select(cross_tenant_request(&fx.tenant, &reason)),
        )
        .await
        .expect_err("the scope is required");
        assert_eq!(refused.code(), Code::PermissionDenied);
        // The scope alone, on a tenant-bound credential.
        let tenant_user = test_claim_context("user", &other, "", &["udb:cross_tenant:read"], &[]);
        let refused = scope_claim_context_for_test(
            tenant_user,
            fx.svc.select(cross_tenant_request(&fx.tenant, &reason)),
        )
        .await
        .expect_err("platform authority is required");
        assert_eq!(refused.code(), Code::PermissionDenied);
        // Metadata cannot stand in for a verified credential.
        let refused = fx
            .svc
            .select(cross_tenant_request(&fx.tenant, &reason))
            .await
            .expect_err("a verified credential is required");
        assert_eq!(refused.code(), Code::PermissionDenied);
        let denied: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM udb_system.auth_audit_log \
         WHERE event_type = 'udb.data.cross_tenant_read' AND outcome = 'denied' \
         AND envelope ->> 'reason' = $1",
        )
        .bind(&reason)
        .fetch_one(&fx.pool)
        .await
        .expect("the audit log");
        assert_eq!(denied, 3, "every refused attempt is on record");
        let _ = sqlx::query(&format!(
            "DELETE FROM \"{}\".rows WHERE tenant_id = $1",
            fx.schema
        ))
        .bind(&other)
        .execute(&fx.pool)
        .await;
        fx.finish().await;
    }
);
