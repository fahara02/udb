//! SERVED round-trip live test for the MySQL wire-codec fix (W3):
//! `DATETIME` / `TIMESTAMP` / `DATE` columns decode as chrono types in the shared
//! `sqlx_row_to_json` serializer. Before the fix none of the temporal branches
//! existed, so every temporal column read back `null` (silent data loss).
//!
//! WRITE goes through the real served mutation executor
//! (`MysqlExecutor::mutate`); READ goes through the real served query executor
//! (`MysqlExecutor::query` → `sqlx_row_to_json`). Gated on `UDB_MYSQL_DSN`;
//! runtime-skips when unset.

#![allow(unused_imports)]

use serde_json::{Value as JsonValue, json};
use sqlx::mysql::MySqlPoolOptions;
use uuid::Uuid;

use crate::runtime::executors::mysql::MysqlExecutor;
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

#[tokio::test]
async fn mysql_temporal_columns_round_trip_served_live() {
    let Ok(dsn) = std::env::var("UDB_MYSQL_DSN") else {
        eprintln!("UDB_MYSQL_DSN unset — skipping MySQL temporal wire-codec round-trip");
        return;
    };
    let pool = MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect to live MySQL (UDB_MYSQL_DSN)");
    let table = format!("udb_wire_dt_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE TABLE `{table}` (\
            id VARCHAR(64) PRIMARY KEY, \
            dt DATETIME(6), \
            ts TIMESTAMP(6) NULL DEFAULT NULL, \
            d DATE)"
    ))
    .execute(&pool)
    .await
    .expect("create throwaway temporal table");

    let exec = MysqlExecutor::with_pool(pool.clone());

    // Served WRITE via the mutation executor (literal INSERT — MySQL parses the
    // datetime/date literals directly).
    let insert = json!({
        "sql": format!(
            "INSERT INTO `{table}` (id, dt, ts, d) VALUES \
             ('m1', '2026-08-04 12:34:56.123456', '2026-08-04 12:34:56.000000', '2026-08-04')"
        ),
        "params": []
    })
    .to_string();
    MutationExecutor::mutate(&exec, &insert)
        .await
        .expect("served MySQL insert of temporal row");

    // Served READ via the query executor → `sqlx_row_to_json` (W3).
    let read = json!({
        "sql": format!("SELECT id, dt, ts, d FROM `{table}` WHERE id = 'm1'"),
        "params": []
    })
    .to_string();
    let resp = QueryExecutor::query(&exec, &read)
        .await
        .expect("served MySQL query of temporal row");
    let rows: Vec<JsonValue> = serde_json::from_str(&resp).expect("MySQL query returns row array");
    assert_eq!(rows.len(), 1, "one temporal row expected");
    let row = &rows[0];

    // Revert of W3 = these read back `null`. The fix decodes them as chrono types.
    // DATETIME(6) → NaiveDateTime, exact to the microsecond.
    assert_eq!(
        row["dt"],
        json!("2026-08-04T12:34:56.123456"),
        "W3: DATETIME(6) exact round-trip (revert → null)"
    );
    // DATE → NaiveDate, exact.
    assert_eq!(
        row["d"],
        json!("2026-08-04"),
        "W3: DATE exact round-trip (revert → null)"
    );
    // TIMESTAMP → DateTime<Utc>; session-timezone conversion makes the exact
    // rendering deployment-dependent, so assert it is a non-null datetime string
    // for the written day (revert → null → both assertions fail).
    assert!(
        row["ts"].is_string(),
        "W3: TIMESTAMP(6) must read back a non-null datetime string, got {}",
        row["ts"]
    );
    assert!(
        row["ts"].as_str().unwrap_or_default().contains('T'),
        "W3: TIMESTAMP(6) must decode to an RFC3339 datetime (session-tz may shift the \
         wall-clock, so assert shape not exact value), got {}",
        row["ts"]
    );

    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS `{table}`"))
        .execute(&pool)
        .await;
    pool.close().await;
}
