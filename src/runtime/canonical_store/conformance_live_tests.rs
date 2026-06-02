//! B.11 / B.7 safety net — env-gated LIVE canonical-store conformance.
//!
//! These tests reuse the exact contract functions from [`super::conformance`]
//! against real Postgres / MySQL engines (host-published Docker containers, see
//! `docker-compose.canonical.yml`). They are runtime-skipped when the matching
//! DSN env var is unset, so default `cargo test --lib` (and CI) stay green with
//! zero external dependencies; set `UDB_PG_DSN` / `UDB_MYSQL_DSN` to exercise.
//!
//! Why this exists: before refactoring the shared SQL canonical core (B.7) we
//! need proof the *current* Postgres/MySQL stores satisfy the same contract the
//! SQLite store does — on a real engine, not just typeless in-memory SQLite.
//! Each run provisions a throwaway database (`udb_conf_<uuid>`) so the
//! contracts' freshness/count assertions hold and runs never collide; the
//! database is dropped on success. (The file is named `*_tests.rs` so the
//! `connection_manager::runtime_env_reads_are_confined…` guardrail treats its
//! `std::env::var` reads as test config.)

#![allow(unused_imports)]

use std::sync::Arc;

use super::CanonicalStore;
use super::conformance::{
    run_admin_audit_store_contract, run_contract, run_migration_audit_store_contract,
    run_projection_task_contract, run_saga_store_contract,
};
use super::system_store::{AdminAuditStore, MigrationAuditStore, ProjectionTaskStore, SagaStore};

/// Swap the database segment of a DSN of the form `scheme://…@host:port/db`.
/// The canonical DSNs used here carry no query string, so a single rsplit on
/// `/` isolates the database name.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn swap_database(dsn: &str, new_db: &str) -> String {
    match dsn.rsplit_once('/') {
        Some((prefix, _db)) => format!("{prefix}/{new_db}"),
        None => format!("{dsn}/{new_db}"),
    }
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "clickhouse"))]
fn fresh_db_name() -> String {
    format!("udb_conf_{}", uuid::Uuid::new_v4().simple())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_PG_DSN") else {
        eprintln!("UDB_PG_DSN unset — skipping live Postgres conformance");
        return;
    };
    use super::postgres::PostgresCanonicalStore;
    use sqlx::postgres::PgPoolOptions;

    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to live Postgres (UDB_PG_DSN)");
    let db = fresh_db_name();
    // CREATE DATABASE cannot run inside a transaction; sqlx issues it directly.
    sqlx::query(&format!("CREATE DATABASE \"{db}\""))
        .execute(&admin)
        .await
        .expect("create throwaway database");

    let conf_dsn = swap_database(&dsn, &db);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&conf_dsn)
        .await
        .expect("connect to throwaway database");
    let store = Arc::new(PostgresCanonicalStore::new(
        pool.clone(),
        "conformance-live",
        "udb_outbox_events",
    ));

    // Every contract the SQLite store satisfies, now on real Postgres. These
    // exercise the projection/saga/admin/migration INSERTs (all `uuid` columns)
    // that in-memory SQLite never type-checked.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Teardown: drop our connections, then the throwaway database.
    drop(store);
    pool.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{db}\" WITH (FORCE)"))
        .execute(&admin)
        .await;
}

#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_MYSQL_DSN") else {
        eprintln!("UDB_MYSQL_DSN unset — skipping live MySQL conformance");
        return;
    };
    use super::mysql::MysqlCanonicalStore;
    use sqlx::mysql::MySqlPoolOptions;

    let admin = MySqlPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect to live MySQL (UDB_MYSQL_DSN)");
    let db = fresh_db_name();
    sqlx::query(&format!("CREATE DATABASE `{db}`"))
        .execute(&admin)
        .await
        .expect("create throwaway database");

    let conf_dsn = swap_database(&dsn, &db);
    let pool = MySqlPoolOptions::new()
        .max_connections(4)
        .connect(&conf_dsn)
        .await
        .expect("connect to throwaway database");
    let store = Arc::new(MysqlCanonicalStore::new(
        pool.clone(),
        "conformance-live",
        "udb_outbox_events",
    ));

    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    drop(store);
    pool.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS `{db}`"))
        .execute(&admin)
        .await;
}

/// B.14 — env-gated LIVE Redis conformance. Runs ALL FIVE contracts against
/// the native Redis `SystemStores` implementation. The store refuses to start
/// unless Redis reports `aof_enabled:1`, so a provided DSN with a non-durable
/// Redis instance fails here instead of silently passing as a projection cache.
///
/// Reads `UDB_REDIS_DSN`, falling back to `REDIS_URL` and
/// `UDB_INTEGRATION_REDIS_URL`; runtime-skips when all are unset. Each run uses
/// a unique Redis key prefix (`udb:system:conformance-live-<uuid>`) and deletes
/// only that prefix best-effort after the contracts finish.
#[cfg(feature = "redis")]
#[tokio::test]
async fn redis_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_REDIS_DSN")
        .or_else(|_| std::env::var("REDIS_URL"))
        .or_else(|_| std::env::var("UDB_INTEGRATION_REDIS_URL"))
    else {
        eprintln!(
            "UDB_REDIS_DSN / REDIS_URL / UDB_INTEGRATION_REDIS_URL unset — skipping live Redis conformance"
        );
        return;
    };
    use super::redis::RedisCanonicalStore;

    let instance = format!("conformance-live-{}", uuid::Uuid::new_v4().simple());
    let client = redis::Client::open(dsn).expect("create live Redis client");
    let store = Arc::new(RedisCanonicalStore::new(client.clone(), instance.clone()));

    // Every SystemStores contract, now on native Redis. `run_contract` calls
    // `ensure_system_tables`, which enforces the AOF durability profile.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    drop(store);
    let prefix = format!("udb:system:{instance}:*");
    if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
        if let Ok(keys) = redis::cmd("KEYS")
            .arg(prefix)
            .query_async::<Vec<String>>(&mut conn)
            .await
            && !keys.is_empty()
        {
            let _ = redis::cmd("DEL")
                .arg(keys)
                .query_async::<i64>(&mut conn)
                .await;
        }
    }
}

/// B.8 — env-gated LIVE SQL Server conformance. Runs ALL FIVE contracts
/// (base CanonicalStore + the four system-store traits), mirroring the
/// Postgres/MySQL all-contracts tests above now that PHASE 2 implements
/// `ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
/// `MigrationAuditStore` for the MSSQL store.
///
/// Runtime-skips when `UDB_MSSQL_DSN` is unset, so default
/// `cargo test` stays green with zero external dependencies. The DSN
/// is ADO-style (`Server=…;User=sa;Password=…;Database=udb;TrustServerCertificate=true`).
///
/// ## Isolation without per-run CREATE DATABASE
///
/// SQL Server's `CREATE DATABASE` semantics differ from PG/MySQL and the
/// harness has no throwaway-database swap, so instead every contract gets
/// a UNIQUE set of table names (outbox / projection / saga / admin-audit /
/// migration-runs / migration-ledger), all suffixed with one per-run UUID.
/// That keeps each contract's freshness/count assertions honest (the
/// admin-audit contract asserts an empty chain on entry; the projection /
/// migration contracts assert empty summaries) and lets parallel runs
/// against the shared `udb` database never collide. Best-effort DROPs at
/// the end clean the tables up; a crashed run leaves uniquely-named
/// leftovers rather than corrupting a shared table.
#[cfg(feature = "mssql")]
#[tokio::test]
async fn mssql_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_MSSQL_DSN") else {
        eprintln!("UDB_MSSQL_DSN unset — skipping live SQL Server conformance");
        return;
    };
    use super::mssql::MssqlCanonicalStore;
    use crate::runtime::executors::mssql::MssqlClient;

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let outbox_rel = format!("udb_outbox_events_conf_{suffix}");
    let projection_rel = format!("udb_projection_tasks_conf_{suffix}");
    let saga_rel = format!("udb_sagas_conf_{suffix}");
    let audit_rel = format!("udb_admin_audit_log_conf_{suffix}");
    let runs_rel = format!("udb_migration_runs_conf_{suffix}");
    let ledger_rel = format!("udb_migration_op_ledger_conf_{suffix}");

    let client = MssqlClient::new(dsn);
    let store = Arc::new(
        MssqlCanonicalStore::new(client.clone(), "conformance-live", outbox_rel.clone())
            .with_projection_relation(projection_rel.clone())
            .with_saga_relation(saga_rel.clone())
            .with_admin_audit_relation(audit_rel.clone())
            .with_migration_relations(runs_rel.clone(), ledger_rel.clone()),
    );

    // Every contract the Postgres/MySQL stores satisfy, now on real SQL Server.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Best-effort cleanup of the throwaway tables (ledger before runs — the
    // ledger FK references the runs table).
    drop(store);
    for rel in [
        &ledger_rel,
        &runs_rel,
        &audit_rel,
        &saga_rel,
        &projection_rel,
        &outbox_rel,
    ] {
        let _ = client
            .simple_batch(&format!(
                "IF OBJECT_ID(N'{rel}', N'U') IS NOT NULL DROP TABLE {rel}"
            ))
            .await;
    }
}

/// B.9 phase 2 — env-gated LIVE MongoDB conformance. Runs ALL FIVE contracts
/// (base `CanonicalStore` + the four system-store traits), mirroring the
/// Postgres/MySQL/MSSQL all-contracts tests now that PHASE 2 implements
/// `ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
/// `MigrationAuditStore` for the MongoDB store.
///
/// Reads `UDB_MONGODB_DSN` (falling back to `UDB_NOSQL_DSN`); runtime-skips when
/// neither is set, so default `cargo test` stays green with zero external
/// dependencies. The system-store contracts' `append_admin_audit` and
/// `claim_recoverable_sagas` run multi-document transactions, so the test
/// asserts a replica-set / sharded topology via `hello` before running (a
/// standalone mongod cannot start a session transaction, so the system-store
/// contracts genuinely require this deployment shape; the assertion matches the
/// CDC source's check).
///
/// Each run targets a UNIQUE database (`udb_conf_<uuid>`) so each contract's
/// freshness/count assertions hold and parallel runs never collide; the
/// database is dropped best-effort at the end.
#[cfg(feature = "mongodb-native")]
#[tokio::test]
async fn mongodb_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_MONGODB_DSN").or_else(|_| std::env::var("UDB_NOSQL_DSN"))
    else {
        eprintln!("UDB_MONGODB_DSN / UDB_NOSQL_DSN unset — skipping live MongoDB conformance");
        return;
    };
    use super::mongodb::MongoDbCanonicalStore;
    use crate::runtime::executors::mongodb::{MongoDbExecutor, MongoDbNativeConfig};
    use mongodb_driver::bson::doc;

    let db_name = format!("udb_conf_{}", uuid::Uuid::new_v4().simple());
    let executor = MongoDbExecutor::new_native(MongoDbNativeConfig {
        dsn: dsn.clone(),
        database: db_name.clone(),
        timeout_secs: 10,
        app_name: Some("udb-mongo-conformance".to_string()),
        max_pool_size: Some(4),
        direct_connection: None,
        retry_writes: None,
    })
    .await
    .expect("construct native MongoDbExecutor from UDB_MONGODB_DSN");

    let database = executor
        .native_database()
        .expect("native executor must expose its Database handle");

    // Topology check: change streams / multi-doc transactions (used by later
    // phases) require a replica set or sharded cluster. Mirror the CDC source's
    // `hello` probe (`setName` for replica sets, `msg == "isdbgrid"` for shards).
    let hello = database
        .run_command(doc! { "hello": 1 })
        .await
        .expect("mongodb hello command");
    let is_replica = hello.contains_key("setName");
    let is_sharded = hello
        .get_str("msg")
        .map(|m| m == "isdbgrid")
        .unwrap_or(false);
    assert!(
        is_replica || is_sharded,
        "mongodb conformance expects a replica set or sharded cluster topology"
    );

    let store = Arc::new(MongoDbCanonicalStore::new(
        database.clone(),
        "conformance-live",
        "udb_outbox_events",
    ));

    // Every contract the SQL stores satisfy, now on real MongoDB. The
    // system-store contracts exercise the saga/admin-audit session transactions
    // and the migration-ledger monotone counter.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Best-effort teardown of the throwaway database.
    drop(store);
    let _ = database.run_command(doc! { "dropDatabase": 1 }).await;
}

/// B.10a phase 2 — env-gated LIVE Cassandra / ScyllaDB conformance. Runs ALL
/// FIVE contracts (base `CanonicalStore` + the four system-store traits), now
/// that PHASE 2 implements `ProjectionTaskStore` / `SagaStore` /
/// `AdminAuditStore` / `MigrationAuditStore` for the Cassandra store in CQL
/// (per-row LWT where atomic compare-and-set is needed; the admin-audit chain
/// serialises on a single chain-head LWT row; the migration-op ledger id is a
/// monotone LWT-CAS counter).
///
/// Reads `UDB_CASSANDRA_DSN` (contact-point form `host:port`, e.g.
/// `127.0.0.1:59042`); runtime-skips when unset so default `cargo test` stays
/// green with zero external dependencies. Each run targets a UNIQUE keyspace
/// (`udb_conf_<uuid>`) so each contract's freshness/count assertions hold
/// (fresh outbox max_seq == 0, empty projection/saga/migration summaries, empty
/// audit chain) and parallel runs never collide; the keyspace is dropped
/// best-effort at the end.
#[cfg(feature = "cassandra")]
#[tokio::test]
async fn cassandra_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_CASSANDRA_DSN") else {
        eprintln!("UDB_CASSANDRA_DSN unset — skipping live Cassandra conformance");
        return;
    };
    use super::cassandra::CassandraCanonicalStore;
    use crate::runtime::executors::cassandra::CassandraClient;

    let client = CassandraClient::connect(&dsn)
        .await
        .expect("connect to live Cassandra (UDB_CASSANDRA_DSN)");
    let keyspace = format!("udb_conf_{}", uuid::Uuid::new_v4().simple());

    let store = Arc::new(CassandraCanonicalStore::new(
        client.clone(),
        "conformance-live",
        keyspace.clone(),
        "udb_outbox_events",
    ));

    // Every contract the SQL/Mongo stores satisfy, now on real Cassandra. The
    // system-store contracts exercise the per-row LWT batch claim, the
    // chain-head LWT admin-audit append, and the LWT-CAS migration-op counter.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Best-effort teardown of the throwaway keyspace.
    drop(store);
    let _ = client
        .cql_execute(&format!("DROP KEYSPACE IF EXISTS \"{keyspace}\""), ())
        .await;
}

/// B.10b phase 2 — env-gated LIVE Neo4j conformance. Runs ALL FIVE contracts
/// (the base `CanonicalStore` contract plus the four system-store contracts:
/// projection / saga / admin-audit / migration-audit) against real Neo4j 5.
/// The system-store contracts exercise the run_tag-scoped Cypher claims, the
/// chain-serialised audit append (read-head-then-guarded-create in one HTTP
/// tx), and the `(:UdbCounter)`-backed migration-op id sequence.
///
/// Reads `UDB_NEO4J_DSN` (HTTP base, e.g. `http://127.0.0.1:57474`), falling
/// back to `UDB_GRAPH_DSN` / `UDB_GRAPH_HTTP_URL`; runtime-skips when all are
/// unset so default `cargo test` stays green with zero external dependencies.
/// Credentials come from `UDB_NEO4J_USER` / `UDB_NEO4J_PASSWORD` /
/// `UDB_NEO4J_DATABASE` (falling back to the executor's `UDB_GRAPH_*` env names
/// and to `neo4j` / empty / `neo4j`).
///
/// ## Isolation without a per-run database
///
/// Neo4j Community Edition exposes a single database, so unlike the PG / MySQL /
/// Mongo / Cassandra tests there is no throwaway DB to provision. Instead the
/// store is constructed `.with_run_tag(<uuid>)`, which stamps every
/// counter / outbox / lease node it writes with that tag and ANDs the tag into
/// every MATCH/MERGE. That keeps the contract's freshness assertions honest
/// (a fresh tag sees `outbox_max_seq == 0` and no leases) and lets parallel
/// runs against the shared `neo4j` database never collide. All tagged nodes are
/// `DETACH DELETE`d best-effort at the end.
#[cfg(feature = "neo4j")]
#[tokio::test]
async fn neo4j_canonical_store_satisfies_all_contracts_live() {
    let Ok(http_base) = std::env::var("UDB_NEO4J_DSN")
        .or_else(|_| std::env::var("UDB_GRAPH_HTTP_URL"))
        .or_else(|_| std::env::var("UDB_GRAPH_DSN"))
    else {
        eprintln!(
            "UDB_NEO4J_DSN / UDB_GRAPH_HTTP_URL / UDB_GRAPH_DSN unset — skipping live Neo4j conformance"
        );
        return;
    };
    use super::neo4j::Neo4jCanonicalStore;
    use crate::runtime::executors::neo4j::{Neo4jConfig, Neo4jExecutor};

    // Normalise a bolt:// DSN to the HTTP base the executor speaks; pass an
    // http(s):// base through unchanged.
    let http_base = if http_base.starts_with("http://") || http_base.starts_with("https://") {
        http_base
    } else {
        Neo4jConfig::http_base_from_dsn(&http_base)
    };
    let username = std::env::var("UDB_NEO4J_USER")
        .or_else(|_| std::env::var("UDB_GRAPH_USER"))
        .unwrap_or_else(|_| "neo4j".to_string());
    let password = std::env::var("UDB_NEO4J_PASSWORD")
        .or_else(|_| std::env::var("UDB_GRAPH_PASSWORD"))
        .unwrap_or_default();
    let database = std::env::var("UDB_NEO4J_DATABASE")
        .or_else(|_| std::env::var("UDB_GRAPH_DATABASE"))
        .unwrap_or_else(|_| "neo4j".to_string());

    let executor = Neo4jExecutor::new(Neo4jConfig {
        http_base,
        username,
        password,
        database,
        is_cloud: false,
        // dev_mode silences the plaintext-HTTP credential warning for the local
        // test container.
        dev_mode: true,
        timeout_secs: 15,
    });

    let run_tag = uuid::Uuid::new_v4().simple().to_string();
    let store = Arc::new(
        Neo4jCanonicalStore::new(executor.clone(), "conformance-live")
            .with_run_tag(run_tag.clone()),
    );

    // Every contract the SQL/Mongo/Cassandra stores satisfy, now on real
    // Neo4j 5 (phase 2). The system-store contracts exercise the run_tag-scoped
    // Cypher claims, the chain-serialised admin-audit append, and the
    // `(:UdbCounter)`-backed migration-op id sequence.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Best-effort teardown: delete every node this run tagged.
    drop(store);
    let _ = executor
        .cypher_rows(
            "MATCH (n) WHERE n.run_tag = $tag DETACH DELETE n",
            serde_json::json!({ "tag": run_tag }),
        )
        .await;
}

/// B.10c phase 2 — env-gated LIVE ClickHouse conformance. Runs ALL FIVE
/// contracts (the base `CanonicalStore` contract plus the four system-store
/// contracts: projection / saga / admin-audit / migration-audit), now that
/// PHASE 2 implements `ProjectionTaskStore` / `SagaStore` / `AdminAuditStore` /
/// `MigrationAuditStore` for the ClickHouse store. Mutable state
/// (projection_tasks / sagas / migration_runs / op-seq counter) uses a
/// `ReplacingMergeTree(version)` read-insert-reread versioned-CAS read with
/// `SELECT … FINAL`; append-only data (admin-audit log / migration op-ledger)
/// uses plain `MergeTree` INSERTs.
///
/// Reads `UDB_COLUMN_DSN` (falling back to `UDB_CLICKHOUSE_DSN`); runtime-skips
/// when both are unset so default `cargo test` stays green with zero external
/// dependencies. The live container publishes the ClickHouse HTTP interface at
/// `http://127.0.0.1:58123` (user `udb`, password `udb`, database `udb`).
///
/// ## Isolation via a per-run database
///
/// ClickHouse supports `CREATE DATABASE`, so each run provisions a UNIQUE
/// throwaway database (`udb_conf_<uuid>`) and points the store's executor at it.
/// That keeps every contract's freshness assertions honest (a fresh database
/// sees `outbox_max_seq == 0`, no leases, empty projection/saga/migration
/// summaries, and an empty audit chain) and lets parallel runs never collide.
/// The database is dropped best-effort at the end.
///
/// ## ClickHouse correctness notes exercised here
///
/// The contracts write/read outbox events, advisory leases, projection tasks,
/// sagas, admin-audit rows and migration runs/ops, which drive the
/// ReplacingMergeTree(version) read-insert-reread CAS emulation and every
/// `SELECT … FINAL` collapse / aggregate path against a real engine — the parts
/// no unit test can cover (FINAL merge-at-read-time, FINAL over GROUP BY
/// aggregates, JSONCompact UInt64-as-string version decode, epoch-millis Int64
/// timestamp round-trip incl. the sentinel-0 None discipline, and the
/// versioned-CAS mutation visibility after each INSERT). The contracts are
/// single-threaded, matching the store's documented single-writer assumption.
#[cfg(feature = "clickhouse")]
#[tokio::test]
async fn clickhouse_canonical_store_satisfies_all_contracts_live() {
    let Ok(dsn) = std::env::var("UDB_COLUMN_DSN").or_else(|_| std::env::var("UDB_CLICKHOUSE_DSN"))
    else {
        eprintln!(
            "UDB_COLUMN_DSN / UDB_CLICKHOUSE_DSN unset — skipping live ClickHouse conformance"
        );
        return;
    };
    use super::clickhouse::ClickHouseCanonicalStore;
    use crate::runtime::executors::clickhouse::{ClickHouseConfig, ClickHouseExecutor};

    // Resolve the HTTP base from the DSN (accepts both `clickhouse://…` and a
    // bare `http(s)://…` base), and the credentials from the standard
    // `UDB_COLUMN_*` env names (falling back to the live container's defaults).
    let http_base = if dsn.starts_with("http://") || dsn.starts_with("https://") {
        dsn.split('/').take(3).collect::<Vec<_>>().join("/")
    } else {
        ClickHouseConfig::http_base_from_dsn(&dsn)
    };
    let username = std::env::var("UDB_COLUMN_USER").unwrap_or_else(|_| "udb".to_string());
    let password = std::env::var("UDB_COLUMN_PASSWORD").unwrap_or_else(|_| "udb".to_string());
    let base_db = std::env::var("UDB_COLUMN_DATABASE")
        .ok()
        .or_else(|| ClickHouseConfig::db_from_dsn(&dsn))
        .unwrap_or_else(|| "udb".to_string());

    let mk_cfg = |database: String| ClickHouseConfig {
        http_base: http_base.clone(),
        username: username.clone(),
        password: password.clone(),
        database,
        is_cloud: false,
        connect_timeout_secs: 10,
        query_timeout_secs: 30,
    };

    // Provision a UNIQUE throwaway database via an executor on the base database.
    let admin = ClickHouseExecutor::new(mk_cfg(base_db.clone()));
    let conf_db = fresh_db_name();
    admin
        .execute_ddl(&format!("CREATE DATABASE IF NOT EXISTS `{conf_db}`"))
        .await
        .expect("create throwaway ClickHouse database");

    // The store's executor targets the throwaway database (the
    // `X-ClickHouse-Database` header is set from `config.database`).
    let executor = ClickHouseExecutor::new(mk_cfg(conf_db.clone()));
    let store = Arc::new(ClickHouseCanonicalStore::new(
        executor,
        "conformance-live",
        conf_db.clone(),
    ));

    // PHASE 2: every contract the SQL/Mongo/Cassandra/Neo4j stores satisfy, now
    // on real ClickHouse. The system-store contracts exercise the
    // ReplacingMergeTree(version) versioned-CAS claim/flip, the append-only
    // admin-audit chain, the FINAL group-by summaries, and the
    // ReplacingMergeTree-counter migration-op id sequence.
    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;

    // Best-effort teardown of the throwaway database.
    drop(store);
    let _ = admin
        .execute_ddl(&format!("DROP DATABASE IF EXISTS `{conf_db}`"))
        .await;
}

/// B.11 — env-gated LIVE Qdrant conformance. Qdrant is a vector store with no
/// transactions; the canonical store (`qdrant.rs`) models every system row as a
/// point in a per-instance `udb_system_<instance>` collection (deterministic
/// point IDs + an in-process op-lock for the audit chain / claim CAS). Runs ALL
/// FIVE contracts. A unique instance name per run → a fresh, isolated system
/// collection, so freshness/count assertions hold and runs never collide.
#[cfg(feature = "qdrant")]
#[tokio::test]
async fn qdrant_canonical_store_satisfies_all_contracts_live() {
    let Ok(url) = std::env::var("UDB_QDRANT_URL") else {
        eprintln!("UDB_QDRANT_URL unset — skipping live Qdrant conformance");
        return;
    };
    use super::qdrant::QdrantCanonicalStore;
    use crate::runtime::executors::qdrant::QdrantHttpClient;

    let client = QdrantHttpClient {
        base_url: url.trim_end_matches('/').to_string(),
        api_key: std::env::var("UDB_QDRANT_API_KEY").ok(),
        http: crate::runtime::executors::http::HttpClientSpec::with_timeout(
            std::time::Duration::from_secs(30),
        )
        .build(),
    };
    let instance = format!("conf_{}", uuid::Uuid::new_v4().simple());
    let store = Arc::new(QdrantCanonicalStore::new(client, instance.clone()));

    run_contract(store.clone()).await;
    run_projection_task_contract(store.clone()).await;
    run_saga_store_contract(store.clone()).await;
    run_admin_audit_store_contract(store.clone()).await;
    run_migration_audit_store_contract(store.clone()).await;
    // The per-run `udb_system_<instance>` collection is left in the dev Qdrant
    // (unique name → no cross-run interference); cleanup is best-effort and not
    // required for the contract.
}
