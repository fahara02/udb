//! Env-gated LIVE tests for the wire shape of every PostgreSQL column type on
//! the served read path (`DataBrokerService::select`): one JSON shape per SQL
//! type, the same on every path, never a silent NULL.
//!
//! Every test is `#[ignore]`d; the CI live lane runs every ignored lib test with
//! a DSN exported (see `data_plane_live` for the gating contract).

use serde_json::json;
use uuid::Uuid;

use super::data_plane_live::{
    col, create_schema, dp_live_pg_dsn, dp_pool, dp_service, install_dp_security,
    served_select_rows, served_upsert, teardown,
};
use crate::generation::{CatalogManifest, ManifestTable, ManifestTableSecurity};
use crate::proto::data_broker_server::DataBroker;
use crate::runtime::service::DataBrokerService;
use crate::runtime::system::ensure_system_catalog;

/// Reads exactly one row by `id` within `tenant` through the served Select.
async fn read_one(
    svc: &DataBrokerService,
    tenant: &str,
    message: &str,
    id: &str,
) -> serde_json::Value {
    let rows = served_select_rows(
        svc,
        tenant,
        message,
        json!({"id": id, "tenant_id": tenant}),
        false,
    )
    .await;
    assert_eq!(rows.records_json.len(), 1, "row {id} must be readable");
    serde_json::from_slice(&rows.records_json[0]).expect("decode row")
}

/// NUMERIC read back as NULL for every value: sqlx's String and f64 decoders
/// both refuse the NUMERIC OID and the read path swallowed the error. Values
/// must come back as exact decimal strings keeping the declared scale, for
/// scalars and arrays, for rows written raw and through the served Upsert.
///
/// Revert-proof: restoring the String/f64 probe in `row_value_to_json` makes
/// every `amount` read back as `null`.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_numeric_reads_back_as_exact_decimal_strings_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping NUMERIC wire shape");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_wt_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".payments \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, amount NUMERIC(12,2), \
          precise NUMERIC(40,10), amounts NUMERIC[])"
    ))
    .execute(&pool)
    .await
    .expect("create payments");

    const MSG: &str = "acme.wt.v1.Payment";
    let mut table = ManifestTable {
        proto_package: "acme.wt.v1".to_string(),
        message_name: "Payment".to_string(),
        schema: schema.clone(),
        table: "payments".to_string(),
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
        col("amount", "NUMERIC(12,2)", false),
        col("precise", "NUMERIC(40,10)", false),
        col("amounts", "NUMERIC[]", false),
    ];
    let svc = dp_service(
        &dsn,
        CatalogManifest {
            tables: vec![table],
            ..CatalogManifest::default()
        },
    )
    .await;

    // Raw rows isolate the read path from the write binder.
    let raw_cases = [
        (
            "p-raw-1",
            "12.50",
            "123456789012345678901234567890.0123456789",
            "{1.50,NULL,-2}",
        ),
        ("p-raw-2", "0", "-0.0000000001", "{}"),
    ];
    for (id, amount, precise, amounts) in raw_cases {
        sqlx::query(&format!(
            "INSERT INTO \"{schema}\".payments (id, tenant_id, amount, precise, amounts) \
             VALUES ($1, $2, $3::numeric, $4::numeric, $5::numeric[])"
        ))
        .bind(id)
        .bind(&tenant)
        .bind(amount)
        .bind(precise)
        .bind(amounts)
        .execute(&pool)
        .await
        .expect("seed raw numeric row");
    }
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".payments (id, tenant_id) VALUES ('p-null', $1)"
    ))
    .bind(&tenant)
    .execute(&pool)
    .await
    .expect("seed null numeric row");

    // A served write of a JSON number reads back at the column's scale.
    served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({"id": "p-served", "tenant_id": tenant, "amount": -3.25}),
        "",
    )
    .await
    .expect("served upsert of a NUMERIC value");

    let one = read_one(&svc, &tenant, MSG, "p-raw-1").await;
    assert_eq!(one["amount"], json!("12.50"));
    assert_eq!(
        one["precise"],
        json!("123456789012345678901234567890.0123456789")
    );
    assert_eq!(one["amounts"], json!(["1.50", null, "-2"]));

    let two = read_one(&svc, &tenant, MSG, "p-raw-2").await;
    assert_eq!(two["amount"], json!("0.00"));
    assert_eq!(two["precise"], json!("-0.0000000001"));
    assert_eq!(two["amounts"], json!([]));

    let null_row = read_one(&svc, &tenant, MSG, "p-null").await;
    assert_eq!(null_row["amount"], serde_json::Value::Null);
    assert_eq!(null_row["amounts"], serde_json::Value::Null);

    let served = read_one(&svc, &tenant, MSG, "p-served").await;
    assert_eq!(served["amount"], json!("-3.25"));

    // Writes are exact too: a numeric string beyond f64 precision survives the
    // served Upsert (bridged IR path) and the served Update (planner path), and
    // a NUMERIC filter compares numerically.
    const WIDE: &str = "123456789012345678901234567890.0123456789";
    served_upsert(
        &svc,
        &tenant,
        MSG,
        json!({"id": "p-exact", "tenant_id": tenant, "precise": WIDE, "amounts": ["0.10", null, "7"]}),
        "",
    )
    .await
    .expect("served upsert of a wide NUMERIC string");
    let exact = read_one(&svc, &tenant, MSG, "p-exact").await;
    assert_eq!(exact["precise"], json!(WIDE));
    // NUMERIC[] has no typmod, so each element keeps the scale it was written with.
    assert_eq!(exact["amounts"], json!(["0.10", null, "7"]));
    super::data_plane_live::served_update(
        &svc,
        &tenant,
        MSG,
        json!({"id": "p-exact", "tenant_id": tenant}),
        json!({"precise": "-98765432109876543210.0000000001"}),
    )
    .await
    .expect("served update of a wide NUMERIC string");
    let updated = read_one(&svc, &tenant, MSG, "p-exact").await;
    assert_eq!(
        updated["precise"],
        json!("-98765432109876543210.0000000001")
    );
    let by_amount = served_select_rows(
        &svc,
        &tenant,
        MSG,
        json!({"tenant_id": tenant, "amount": "12.5"}),
        false,
    )
    .await;
    assert_eq!(
        by_amount.records_json.len(),
        1,
        "a NUMERIC filter must match 12.50 numerically"
    );

    teardown(&pool, &schema, &tenant).await;
}

/// One JSON shape per SQL type on the served read path (docs/wire-types.md):
/// integers and floats as numbers, CHAR(n) without padding, user enums and
/// enum arrays as labels, typed arrays as JSON arrays, bytea as base64.
///
/// Revert-proof: dropping the `_<type>` enum-array branch in
/// `row_value_to_json` makes `moods` come back as raw text instead of a JSON
/// array; dropping the CHAR branch keeps the padding in `code`.
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_select_returns_one_json_shape_per_sql_type_live() {
    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping wire-type matrix");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_wt_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TYPE \"{schema}\".mood AS ENUM ('happy', 'very sad')"
    ))
    .execute(&pool)
    .await
    .expect("create enum");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".samples (\
           id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, \
           small SMALLINT, regular INTEGER, big BIGINT, ratio REAL, precise DOUBLE PRECISION, \
           flag BOOLEAN, ref UUID, doc JSONB, blob BYTEA, at TIMESTAMPTZ, day DATE, \
           code CHAR(4), mood \"{schema}\".mood, moods \"{schema}\".mood[], \
           tags TEXT[], counts INTEGER[])"
    ))
    .execute(&pool)
    .await
    .expect("create samples");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".samples VALUES (\
           's1', $1, -7, 42, 9007199254740993, 1.5, 2.25, true, \
           '0192f0c4-0000-7000-8000-000000000001', '{{\"a\": [1, \"x\"]}}', '\\xdeadbeef', \
           '2026-10-07 09:30:00.123+00', '2026-10-07', 'AB', 'very sad', \
           ARRAY['happy', 'very sad', NULL]::\"{schema}\".mood[], \
           ARRAY['a', 'b c'], ARRAY[1, NULL, 3])"
    ))
    .bind(&tenant)
    .execute(&pool)
    .await
    .expect("seed sample row");

    const MSG: &str = "acme.wt.v1.Sample";
    let mut table = ManifestTable {
        proto_package: "acme.wt.v1".to_string(),
        message_name: "Sample".to_string(),
        schema: schema.clone(),
        table: "samples".to_string(),
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
        col("small", "SMALLINT", false),
        col("regular", "INTEGER", false),
        col("big", "BIGINT", false),
        col("ratio", "REAL", false),
        col("precise", "DOUBLE PRECISION", false),
        col("flag", "BOOLEAN", false),
        col("ref", "UUID", false),
        col("doc", "JSONB", false),
        col("blob", "BYTEA", false),
        col("at", "TIMESTAMPTZ", false),
        col("day", "DATE", false),
        col("code", "CHAR(4)", false),
        col("mood", "TEXT", false),
        col("moods", "TEXT[]", false),
        col("tags", "TEXT[]", false),
        col("counts", "INTEGER[]", false),
    ];
    let svc = dp_service(
        &dsn,
        CatalogManifest {
            tables: vec![table],
            ..CatalogManifest::default()
        },
    )
    .await;

    let row = read_one(&svc, &tenant, MSG, "s1").await;
    assert_eq!(row["small"], json!(-7));
    assert_eq!(row["regular"], json!(42));
    assert_eq!(row["big"], json!(9_007_199_254_740_993_i64));
    assert_eq!(row["ratio"], json!(1.5));
    assert_eq!(row["precise"], json!(2.25));
    assert_eq!(row["flag"], json!(true));
    assert_eq!(row["ref"], json!("0192f0c4-0000-7000-8000-000000000001"));
    assert_eq!(row["doc"], json!({"a": [1, "x"]}));
    assert_eq!(row["blob"], json!("3q2+7w=="));
    assert_eq!(row["at"], json!("2026-10-07T09:30:00.123Z"));
    assert_eq!(row["day"], json!("2026-10-07"));
    assert_eq!(row["code"], json!("AB"));
    assert_eq!(row["mood"], json!("very sad"));
    assert_eq!(row["moods"], json!(["happy", "very sad", null]));
    assert_eq!(row["tags"], json!(["a", "b c"]));
    assert_eq!(row["counts"], json!([1, null, 3]));

    teardown(&pool, &schema, &tenant).await;
}

/// Compare-and-swap compares values by column type, not by their text: a
/// timestamp asserted with a different fraction width or offset spelling, and a
/// NUMERIC asserted as a JSON number, both match the stored value; a real
/// difference still refuses with FAILED_PRECONDITION.
///
/// Revert-proof: comparing with `json_values_match` again makes the first
/// conditional upsert fail (`.12Z` != `.120000+00:00`, `"12.50"` != `12.5`).
#[tokio::test]
#[ignore = "requires live Postgres (UDB_INTEGRATION_PG_DSN); runs in the CI --ignored live step"]
async fn served_cas_compares_timestamps_and_numerics_as_values_live() {
    use crate::proto::UpsertRequest;
    use crate::runtime::executor_utils::json_to_struct;

    let Some(dsn) = dp_live_pg_dsn() else {
        eprintln!("data-plane live DSN unset — skipping typed CAS");
        return;
    };
    let _guard = super::support::live_native_service_db_lock().lock().await;
    install_dp_security();
    let pool = dp_pool(&dsn).await;
    ensure_system_catalog(&pool)
        .await
        .expect("bootstrap system catalog");

    let schema = format!("udb_wt_{}", Uuid::new_v4().simple());
    let tenant = Uuid::new_v4().to_string();
    create_schema(&pool, &schema).await;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".shifts \
         (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, status TEXT NOT NULL, \
          started_at TIMESTAMPTZ NOT NULL, rate NUMERIC(12,2) NOT NULL)"
    ))
    .execute(&pool)
    .await
    .expect("create shifts");
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".shifts VALUES ('s1', $1, 'ACTIVE', '2026-10-07 09:30:00.12+00', 12.5)"
    ))
    .bind(&tenant)
    .execute(&pool)
    .await
    .expect("seed shift");

    const MSG: &str = "acme.wt.v1.Shift";
    let mut table = ManifestTable {
        proto_package: "acme.wt.v1".to_string(),
        message_name: "Shift".to_string(),
        schema: schema.clone(),
        table: "shifts".to_string(),
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
        col("started_at", "TIMESTAMPTZ", false),
        col("rate", "NUMERIC(12,2)", false),
    ];
    let svc = dp_service(
        &dsn,
        CatalogManifest {
            tables: vec![table],
            ..CatalogManifest::default()
        },
    )
    .await;

    let conditional = |status: &str, expected: serde_json::Value| {
        super::data_plane_live::with_ctx(
            UpsertRequest {
                message_type: MSG.to_string(),
                record_json: serde_json::to_vec(&json!({
                    "id": "s1",
                    "tenant_id": tenant,
                    "status": status,
                    "started_at": "2026-10-07T09:30:00.12Z",
                    "rate": "12.50"
                }))
                .unwrap(),
                expected: json_to_struct(&expected),
                ..UpsertRequest::default()
            },
            &tenant,
        )
    };

    svc.upsert(conditional(
        "ENDED",
        json!({"started_at": "2026-10-07T09:30:00.120000+00:00", "rate": 12.5, "status": "ACTIVE"}),
    ))
    .await
    .expect("a CAS asserting the same instant and amount in other spellings must apply");
    let row = read_one(&svc, &tenant, MSG, "s1").await;
    assert_eq!(row["status"], json!("ENDED"));

    let refused = svc
        .upsert(conditional(
            "REOPENED",
            json!({"started_at": "2026-10-07T09:30:00.13Z"}),
        ))
        .await
        .expect_err("a different instant must refuse the CAS");
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );
    let row = read_one(&svc, &tenant, MSG, "s1").await;
    assert_eq!(row["status"], json!("ENDED"));

    teardown(&pool, &schema, &tenant).await;
}
