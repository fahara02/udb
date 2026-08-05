//! SERVED round-trip live tests for the PostgreSQL wire-codec fixes:
//!
//!   * W1  — `float4`/`REAL` scalar read (probe f32 after f64).
//!   * W2  — non-text array read (`bigint[]/int[]/bool[]/float[]/uuid[]`) —
//!           element-typed decode instead of the old text-only `Vec<String>`
//!           decode that silently collapsed every non-text array to `[]`.
//!   * #2  — protobuf `bytes` → `BYTEA` bind/read (base64-decode on write,
//!           base64-encode on read).
//!   * #10 — structured JSON → `JSONB` bind/read (no double-encoding; JSON `null`
//!           vs SQL `NULL` stay distinct; `jsonb_typeof` correct).
//!
//! WRITE goes through the real data-plane bind (`postgres_helpers::bind_one`, the
//! function the legacy Upsert RPC binds every column with) executed against the
//! live pool; READ goes through the real served query executor
//! (`PostgresExecutor::query` → `pg_rows_to_json` → `row_value_to_json`). Gated on
//! `UDB_PG_DSN` (falling back to `DATABASE_URL`); runtime-skips when unset.

#![allow(unused_imports)]

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde_json::{Value as JsonValue, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

use crate::generation::ManifestColumn;
use crate::runtime::executors::QueryExecutor;
use crate::runtime::executors::postgres::PostgresExecutor;
use crate::runtime::postgres_helpers::bind_one;

fn pg_dsn() -> Option<String> {
    std::env::var("UDB_PG_DSN")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

fn col(name: &str, sql_type: &str) -> ManifestColumn {
    ManifestColumn {
        column_name: name.to_string(),
        sql_type: sql_type.to_string(),
        ..ManifestColumn::default()
    }
}

/// Read rows back through the REAL served query executor — the same
/// `pg_rows_to_json` → `row_value_to_json` serializer the Select RPC emits.
async fn served_rows(pool: &PgPool, sql: &str) -> Vec<JsonValue> {
    let exec = PostgresExecutor::with_pool(pool.clone());
    let req = json!({ "sql": sql, "params": [] }).to_string();
    let resp = QueryExecutor::query(&exec, &req)
        .await
        .expect("served PostgreSQL query must succeed");
    serde_json::from_str::<Vec<JsonValue>>(&resp).expect("served query returns a JSON row array")
}

async fn served_one(pool: &PgPool, sql: &str) -> JsonValue {
    let mut rows = served_rows(pool, sql).await;
    assert_eq!(rows.len(), 1, "expected exactly one row for {sql}");
    rows.remove(0)
}

/// W1 + W2 — a `REAL` scalar and the full non-text array matrix, written through
/// the data-plane bind and read back through the served query serializer. Before
/// the fix the `REAL` column read back `null` and every non-text array read back
/// `[]`; this asserts each survives the round-trip EXACTLY.
#[tokio::test]
async fn postgres_real_and_non_text_arrays_round_trip_served_live() {
    let Some(dsn) = pg_dsn() else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset — skipping PG real/array wire-codec round-trip");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres (UDB_PG_DSN)");
    let schema = format!("udb_wire_arr_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create throwaway schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".matrix (\
            id text PRIMARY KEY, \
            real_col real, double_col double precision, \
            arr_i64 bigint[], arr_i32 integer[], arr_bool boolean[], \
            arr_f64 double precision[], arr_f32 real[], arr_uuid uuid[], arr_text text[])"
    ))
    .execute(&pool)
    .await
    .expect("create matrix table");

    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();
    let insert_sql = format!(
        "INSERT INTO \"{schema}\".matrix \
        (id, real_col, double_col, arr_i64, arr_i32, arr_bool, arr_f64, arr_f32, arr_uuid, arr_text) \
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
    );
    // Positional binds in column order. Scalars + the type-matching arrays go
    // through the served `bind_one` data-plane bind; the narrow (`int4[]`/`real[]`)
    // and `uuid[]` columns are populated with an element-exact native typed bind
    // (the data-plane's filter-array bind widens int4→int8 / real→float8 and
    // text-encodes uuid, which a narrow/uuid ARRAY column rejects on write — a
    // write-side typing detail orthogonal to the W2 READ-decode fix under test).
    let mut q = sqlx::query(&insert_sql);
    q = bind_one(q, Some(&col("id", "TEXT")), &json!("m1")).expect("bind id");
    q = bind_one(q, Some(&col("real_col", "REAL")), &json!(3.5)).expect("bind real_col");
    q = bind_one(
        q,
        Some(&col("double_col", "DOUBLE PRECISION")),
        &json!(2.25),
    )
    .expect("bind double_col");
    q = bind_one(q, Some(&col("arr_i64", "BIGINT[]")), &json!([1, 2, 3])).expect("bind arr_i64");
    q = q.bind(vec![10_i32, 20, 30]); // arr_i32 (integer[])
    q = bind_one(
        q,
        Some(&col("arr_bool", "BOOLEAN[]")),
        &json!([true, false, true]),
    )
    .expect("bind arr_bool");
    q = bind_one(
        q,
        Some(&col("arr_f64", "DOUBLE PRECISION[]")),
        &json!([1.5, 2.5]),
    )
    .expect("bind arr_f64");
    q = q.bind(vec![1.25_f32, 2.75]); // arr_f32 (real[])
    q = q.bind(vec![u1, u2]); // arr_uuid (uuid[])
    q = bind_one(q, Some(&col("arr_text", "TEXT[]")), &json!(["a", "b"])).expect("bind arr_text");
    q.execute(&pool)
        .await
        .expect("data-plane insert of matrix row");

    let row = served_one(
        &pool,
        &format!(
            "SELECT real_col, double_col, arr_i64, arr_i32, arr_bool, arr_f64, arr_f32, arr_uuid, arr_text \
             FROM \"{schema}\".matrix WHERE id = 'm1'"
        ),
    )
    .await;

    // Table-driven so a newly-broken type in the matrix fails loudly. W1: `real`
    // must not read back null (revert = single f64 probe → null). W2: every
    // non-text array must retain its EXACT elements — revert collapses each of the
    // non-text arrays to `[]`. `double precision` and `text[]` are the controls the
    // pre-fix code already handled.
    let expected: [(&str, JsonValue, &str); 9] = [
        ("real_col", json!(3.5), "W1: REAL scalar (revert → null)"),
        ("double_col", json!(2.25), "float8 control"),
        ("arr_i64", json!([1, 2, 3]), "W2: bigint[] (revert → [])"),
        (
            "arr_i32",
            json!([10, 20, 30]),
            "W2: integer[] (revert → [])",
        ),
        (
            "arr_bool",
            json!([true, false, true]),
            "W2: boolean[] (revert → [])",
        ),
        (
            "arr_f64",
            json!([1.5, 2.5]),
            "W2: double precision[] (revert → [])",
        ),
        ("arr_f32", json!([1.25, 2.75]), "W2: real[] (revert → [])"),
        (
            "arr_uuid",
            json!([u1.to_string(), u2.to_string()]),
            "W2: uuid[] (revert → [])",
        ),
        ("arr_text", json!(["a", "b"]), "text[] control"),
    ];
    for (column, want, label) in &expected {
        assert_eq!(&row[*column], want, "{label}");
    }

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

/// #2 — protobuf `bytes` → `BYTEA`. The SDK renders `bytes` as a base64 STRING;
/// the data-plane bind must base64-DECODE it into native `bytea`, and the served
/// read must base64-ENCODE it back. Covers the empty, NUL-containing, and
/// non-UTF-8 payloads. Reverting the `bind_one` BYTEA branch makes the base64
/// string fall through to the scalar text bind, which PostgreSQL rejects (SQLSTATE
/// 42804) — the write `.expect` below then fails — or stores the raw base64 ASCII,
/// which the read-back comparison catches.
#[tokio::test]
async fn postgres_bytea_bytes_round_trip_served_live() {
    let Some(dsn) = pg_dsn() else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset — skipping PG bytea wire-codec round-trip");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres (UDB_PG_DSN)");
    let schema = format!("udb_wire_bytea_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create throwaway schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".blobs (id text PRIMARY KEY, blob bytea)"
    ))
    .execute(&pool)
    .await
    .expect("create blobs table");

    // (id, raw bytes) — the exact payload classes that broke the base64 round-trip.
    let cases: [(&str, Vec<u8>); 3] = [
        ("empty", Vec::new()),
        ("nul", vec![0x00, 0x00, 0x00]),
        ("non_utf8", vec![0xFF, 0xFE, 0x00, 0x01, 0x80]),
    ];
    let insert_sql = format!("INSERT INTO \"{schema}\".blobs (id, blob) VALUES ($1, $2)");
    for (id, raw) in &cases {
        // Served bind path: the SDK sends `bytes` as a base64 string.
        let encoded = B64.encode(raw);
        let mut q = sqlx::query(&insert_sql);
        q = bind_one(q, Some(&col("id", "TEXT")), &json!(id)).expect("bind id");
        q = bind_one(q, Some(&col("blob", "BYTEA")), &json!(encoded))
            .expect("bind bytea from base64");
        q.execute(&pool)
            .await
            .expect("data-plane insert of bytea (pre-fix: SQLSTATE 42804)");
    }

    for (id, raw) in &cases {
        let row = served_one(
            &pool,
            &format!("SELECT blob FROM \"{schema}\".blobs WHERE id = '{id}'"),
        )
        .await;
        // The served read serializes bytea back to base64 — it must equal the
        // base64 of the ORIGINAL bytes (empty → "").
        assert_eq!(
            row["blob"],
            json!(B64.encode(raw)),
            "#2: bytea round-trip for case '{id}'"
        );
    }

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}

/// #10 — structured JSON → `JSONB`. A structured value must round-trip as
/// canonical STRUCTURED JSON (not a double-encoded string), `jsonb_typeof` must
/// report the true type, and JSON `null` must stay distinct from SQL `NULL`.
/// Reverting the read decode (to `String`) makes the object read back as a quoted
/// string — the `is_object()` assertion fails; reverting the bind (double-encode)
/// makes `jsonb_typeof` report `string` instead of `object`.
#[tokio::test]
async fn postgres_jsonb_structured_round_trip_served_live() {
    let Some(dsn) = pg_dsn() else {
        eprintln!("UDB_PG_DSN / DATABASE_URL unset — skipping PG jsonb wire-codec round-trip");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres (UDB_PG_DSN)");
    let schema = format!("udb_wire_jsonb_{}", Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&pool)
        .await
        .expect("create throwaway schema");
    sqlx::query(&format!(
        "CREATE TABLE \"{schema}\".docs (id text PRIMARY KEY, doc jsonb)"
    ))
    .execute(&pool)
    .await
    .expect("create docs table");

    let structured = json!({
        "a": 1,
        "b": [1, 2, 3],
        "c": null,
        "nested": { "x": true, "y": "z" }
    });
    let insert_sql = format!("INSERT INTO \"{schema}\".docs (id, doc) VALUES ($1, $2)");
    // obj / arr / json-null all go through the served JSONB bind.
    for (id, value) in [
        ("obj", structured.clone()),
        ("arr", json!([1, 2, 3])),
        ("jnull", json!(null)), // a JSON null VALUE → stored as jsonb 'null'
    ] {
        let mut q = sqlx::query(&insert_sql);
        q = bind_one(q, Some(&col("id", "TEXT")), &json!(id)).expect("bind id");
        q = bind_one(q, Some(&col("doc", "JSONB")), &value).expect("bind jsonb");
        q.execute(&pool).await.expect("data-plane insert of jsonb");
    }
    // A genuine SQL NULL: omit the column so the row's doc is SQL NULL, not a
    // jsonb 'null' — the distinction #10 must preserve.
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".docs (id) VALUES ('sqlnull')"
    ))
    .execute(&pool)
    .await
    .expect("insert sql-null row");

    // Structured object round-trips as an OBJECT (no double-encoding).
    let obj_row = served_one(
        &pool,
        &format!("SELECT doc FROM \"{schema}\".docs WHERE id = 'obj'"),
    )
    .await;
    assert!(
        obj_row["doc"].is_object(),
        "#10: JSONB object must read back structured, not a double-encoded string (got {})",
        obj_row["doc"]
    );
    assert_eq!(
        obj_row["doc"], structured,
        "#10: JSONB object exact round-trip"
    );

    let arr_row = served_one(
        &pool,
        &format!("SELECT doc FROM \"{schema}\".docs WHERE id = 'arr'"),
    )
    .await;
    assert!(
        arr_row["doc"].is_array(),
        "#10: JSONB array reads back as array"
    );
    assert_eq!(
        arr_row["doc"],
        json!([1, 2, 3]),
        "#10: JSONB array exact round-trip"
    );

    // `jsonb_typeof` proves the stored physical type is the structured type — a
    // double-encoded write would report 'string'. This SELECT is itself served
    // through the query executor.
    let types = served_rows(
        &pool,
        &format!("SELECT id, jsonb_typeof(doc) AS t FROM \"{schema}\".docs ORDER BY id"),
    )
    .await;
    let type_of = |id: &str| -> JsonValue {
        types
            .iter()
            .find(|r| r["id"] == json!(id))
            .map(|r| r["t"].clone())
            .unwrap_or(JsonValue::Null)
    };
    assert_eq!(
        type_of("obj"),
        json!("object"),
        "#10: jsonb_typeof(obj) = object"
    );
    assert_eq!(
        type_of("arr"),
        json!("array"),
        "#10: jsonb_typeof(arr) = array"
    );
    // JSON null vs SQL NULL: the JSON-null value is stored as jsonb 'null'
    // (jsonb_typeof = 'null'); the omitted column is SQL NULL (jsonb_typeof =
    // SQL NULL → serialized as JSON null). The two are distinguishable here.
    assert_eq!(
        type_of("jnull"),
        json!("null"),
        "#10: a JSON null VALUE is stored as jsonb 'null', distinct from SQL NULL"
    );
    assert_eq!(
        type_of("sqlnull"),
        JsonValue::Null,
        "#10: an absent column is SQL NULL (jsonb_typeof is SQL NULL), not jsonb 'null'"
    );

    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
        .execute(&pool)
        .await;
    pool.close().await;
}
