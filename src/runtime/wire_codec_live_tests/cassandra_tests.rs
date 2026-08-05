//! SERVED round-trip live test for the Cassandra wire-codec fix (W5): the read
//! serializer's old `_ => Null` catch-all silently dropped every column below the
//! handful of scalar arms to `null` — a data-loss capability lie. The fix handles
//! every `CqlValue` arm, so `smallint` / `tinyint` / `counter` / `timestamp` /
//! `date` / `time` / `inet` / `list` / `set` / `map` / UDT columns all survive a
//! SELECT.
//!
//! WRITE goes through the real served mutation executor (`CassandraExecutor::mutate`,
//! literal CQL); DDL uses the low-level client; READ goes through the real served
//! query executor (`CassandraExecutor::query` → `cql_to_json`). Gated on
//! `UDB_CASSANDRA_DSN` (contact-point form `host:port`); runtime-skips when unset.

#![allow(unused_imports)]

use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::runtime::executors::cassandra::{CassandraClient, CassandraExecutor};
use crate::runtime::executors::{MutationExecutor, QueryExecutor};

#[tokio::test]
async fn cassandra_typed_columns_round_trip_served_live() {
    let Ok(dsn) = std::env::var("UDB_CASSANDRA_DSN") else {
        eprintln!("UDB_CASSANDRA_DSN unset — skipping Cassandra typed-column wire-codec round-trip");
        return;
    };
    let client = CassandraClient::connect(&dsn)
        .await
        .expect("connect to live Cassandra (UDB_CASSANDRA_DSN)");
    let ks = format!("udb_wire_{}", Uuid::new_v4().simple());

    // DDL through the low-level client (CREATE is not a served mutation verb).
    client
        .cql_execute(
            &format!(
                "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = \
                 {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
            ),
            (),
        )
        .await
        .expect("create throwaway keyspace");
    client
        .cql_execute(
            &format!("CREATE TYPE {ks}.addr (street text, zip int)"),
            (),
        )
        .await
        .expect("create UDT");
    client
        .cql_execute(
            &format!(
                "CREATE TABLE {ks}.wc (\
                    id text PRIMARY KEY, si smallint, ti tinyint, ts timestamp, dt date, \
                    tm time, ip inet, li list<int>, st set<text>, mp map<text, int>, \
                    ad frozen<addr>)"
            ),
            (),
        )
        .await
        .expect("create typed table");
    client
        .cql_execute(
            &format!("CREATE TABLE {ks}.wc_counter (id text PRIMARY KEY, cnt counter)"),
            (),
        )
        .await
        .expect("create counter table");

    let exec = CassandraExecutor::new(client.clone());

    // Served WRITE via the mutation executor (literal CQL — Cassandra's typed
    // values cannot be expressed through the coarse JSON param binder, so the
    // record is written as a CQL literal, exactly as the compiled mutation would).
    let insert = json!({
        "sql": format!(
            "INSERT INTO {ks}.wc (id, si, ti, ts, dt, tm, ip, li, st, mp, ad) VALUES (\
             'k1', 7, 3, 1754310896789, '2026-08-04', '12:34:56', '10.1.2.3', \
             [1, 2, 3], {{'a', 'b'}}, {{'x': 1, 'y': 2}}, {{street: 'Main', zip: 12345}})"
        ),
        "params": []
    })
    .to_string();
    MutationExecutor::mutate(&exec, &insert)
        .await
        .expect("served Cassandra insert of typed row");
    let bump = json!({
        "sql": format!("UPDATE {ks}.wc_counter SET cnt = cnt + 5 WHERE id = 'k1'"),
        "params": []
    })
    .to_string();
    MutationExecutor::mutate(&exec, &bump)
        .await
        .expect("served Cassandra counter update");

    // Served READ via the query executor → `cql_to_json` (W5).
    let read = json!({
        "sql": format!(
            "SELECT id, si, ti, ts, dt, tm, ip, li, st, mp, ad FROM {ks}.wc WHERE id = 'k1'"
        ),
        "params": []
    })
    .to_string();
    let resp = QueryExecutor::query(&exec, &read)
        .await
        .expect("served Cassandra query of typed row");
    let rows: Vec<JsonValue> = serde_json::from_str(&resp).expect("Cassandra query returns rows");
    assert_eq!(rows.len(), 1, "one typed row expected");
    let row = &rows[0];

    // Every one of these read back `null` before the fix (the `_ => Null` arm).
    assert_eq!(row["si"], json!(7), "W5: smallint");
    assert_eq!(row["ti"], json!(3), "W5: tinyint");
    assert_eq!(row["ts"], json!(1754310896789_i64), "W5: timestamp (epoch ms)");
    // date surfaces as the driver's raw day offset — assert non-null (revert → null).
    assert!(row["dt"].is_number(), "W5: date decodes to a number, got {}", row["dt"]);
    assert_eq!(row["tm"], json!(45296000000000_i64), "W5: time (ns since midnight)");
    assert_eq!(row["ip"], json!("10.1.2.3"), "W5: inet");
    assert_eq!(row["li"], json!([1, 2, 3]), "W5: list<int>");
    assert_eq!(row["st"], json!(["a", "b"]), "W5: set<text>");
    assert_eq!(
        row["mp"],
        json!([["x", 1], ["y", 2]]),
        "W5: map<text,int> as key/value pairs"
    );
    assert_eq!(
        row["ad"],
        json!({ "street": "Main", "zip": 12345 }),
        "W5: UDT decodes to an object"
    );

    let read_counter = json!({
        "sql": format!("SELECT id, cnt FROM {ks}.wc_counter WHERE id = 'k1'"),
        "params": []
    })
    .to_string();
    let resp = QueryExecutor::query(&exec, &read_counter)
        .await
        .expect("served Cassandra counter query");
    let counter_rows: Vec<JsonValue> = serde_json::from_str(&resp).expect("counter rows");
    assert_eq!(counter_rows.len(), 1, "one counter row expected");
    assert_eq!(counter_rows[0]["cnt"], json!(5), "W5: counter");

    // Best-effort teardown of the throwaway keyspace.
    let _ = client
        .cql_execute(&format!("DROP KEYSPACE IF EXISTS {ks}"), ())
        .await;
}
