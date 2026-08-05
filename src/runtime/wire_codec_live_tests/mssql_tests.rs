//! SERVED round-trip live test for the SQL Server wire-codec fix (W4):
//! `DATETIME`/`DATETIME2` decode as `chrono::NaiveDateTime`, `UNIQUEIDENTIFIER`
//! decodes as `Uuid`, and `REAL` decodes as `f32` (widened to f64) in the MSSQL
//! `row_to_json` serializer. Before the fix a lone `f64` probe failed on REAL and
//! there were no uuid/datetime branches, so every such column read back `null`
//! (silent data loss).
//!
//! WRITE goes through the real served mutation executor (`MssqlExecutor::mutate`);
//! READ goes through the real served query executor (`MssqlExecutor::query` →
//! `row_to_json`). Gated on `UDB_MSSQL_DSN` (ADO-style); runtime-skips when unset.
//!
//! NOTE: SQL Server `DATE` (as opposed to DATETIME/DATETIME2) has NO branch in
//! `row_to_json` (tiberius decodes it as `chrono::NaiveDate`, which the serializer
//! does not probe), so a bare `DATE` column still reads back `null` on the served
//! path — a residual gap flagged separately, deliberately NOT asserted here.

#![allow(unused_imports)]

use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::runtime::executors::mssql::{MssqlClient, MssqlExecutor};
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

#[tokio::test]
async fn mssql_datetime_uuid_real_round_trip_served_live() {
    let Ok(dsn) = std::env::var("UDB_MSSQL_DSN") else {
        eprintln!("UDB_MSSQL_DSN unset — skipping SQL Server temporal/uuid/real wire-codec round-trip");
        return;
    };
    let table = format!("udb_wire_mssql_{}", Uuid::new_v4().simple());
    let client = MssqlClient::new(dsn);
    client
        .simple_batch(&format!(
            "CREATE TABLE {table} (\
                id NVARCHAR(64) PRIMARY KEY, \
                dt2 DATETIME2(3), \
                dt DATETIME, \
                guid UNIQUEIDENTIFIER, \
                rl REAL)"
        ))
        .await
        .expect("create throwaway SQL Server table");

    let exec = MssqlExecutor::new(client.clone());
    let guid = Uuid::new_v4();

    // Served WRITE via the mutation executor. SQL Server parses the ISO-8601
    // datetime string literals and implicitly converts the GUID string literal to
    // UNIQUEIDENTIFIER.
    let insert = json!({
        "sql": format!(
            "INSERT INTO {table} (id, dt2, dt, guid, rl) VALUES \
             ('x1', '2026-08-04T12:34:56.123', '2026-08-04T12:34:56', '{guid}', 3.5)"
        ),
        "params": []
    })
    .to_string();
    MutationExecutor::mutate(&exec, &insert)
        .await
        .expect("served SQL Server insert of datetime/uuid/real row");

    // Served READ via the query executor → `row_to_json` (W4).
    let read = json!({
        "sql": format!("SELECT id, dt2, dt, guid, rl FROM {table} WHERE id = 'x1'"),
        "params": []
    })
    .to_string();
    let resp = QueryExecutor::query(&exec, &read)
        .await
        .expect("served SQL Server query");
    let rows: Vec<JsonValue> = serde_json::from_str(&resp).expect("MSSQL query returns row array");
    assert_eq!(rows.len(), 1, "one row expected");
    let row = &rows[0];

    // Revert of W4 = each of these reads back null (datetime) or a base64 blob
    // (uuid/real). The fix decodes them exactly.
    let dt2 = row["dt2"].as_str().unwrap_or_default();
    assert!(
        dt2.starts_with("2026-08-04T12:34:56.123"),
        "W4: DATETIME2 round-trip (revert → null), got {}",
        row["dt2"]
    );
    assert_eq!(
        row["dt"],
        json!("2026-08-04T12:34:56"),
        "W4: DATETIME exact round-trip (revert → null)"
    );
    assert_eq!(
        row["guid"],
        json!(guid.to_string()),
        "W4: UNIQUEIDENTIFIER round-trip (revert → base64 blob / null)"
    );
    assert_eq!(
        row["rl"],
        json!(3.5),
        "W4: REAL (f32) round-trip (revert → f64 probe fails → null)"
    );

    let _ = client
        .simple_batch(&format!(
            "IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"
        ))
        .await;
}
