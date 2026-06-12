//! Live round-trip tests for the multi-backend [`NativeEntityStore`] (extend_udb.md
//! P2). SQLite runs in-process (a real embedded SQL engine, no infra); Postgres and
//! MySQL are gated on `UDB_PG_DSN` / `UDB_MYSQL_DSN` (the Docker fleet) and skip when
//! unset — directive #8's live env-gated DSN chain. Kept in a `_tests.rs` file so the
//! runtime env-confinement guard excludes the DSN reads (test-only).

use std::sync::Arc;

use crate::runtime::service::native_entity_store::{NativeEntityStore, PostgresNativeEntityStore};

/// ensure → put → get → upsert → get → delete → get, asserting values at each step.
/// Proves a backend's native persistence actually round-trips (directive #9 — the
/// served path) against a real database engine (directive #8).
async fn assert_roundtrip(store: Arc<dyn NativeEntityStore>) {
    let key = format!("native-store-test-{}", store.backend());
    store.ensure_kv_table().await.expect("ensure_kv_table");
    store.kv_delete(&key).await.expect("pre-clean"); // idempotent reset
    store.kv_put(&key, "v1").await.expect("kv_put v1");
    assert_eq!(
        store.kv_get(&key).await.expect("kv_get v1"),
        Some("v1".to_string())
    );
    store.kv_put(&key, "v2").await.expect("kv_put v2 (upsert)");
    assert_eq!(
        store.kv_get(&key).await.expect("kv_get v2"),
        Some("v2".to_string())
    );

    store.kv_delete(&key).await.expect("kv_delete");
    assert_eq!(store.kv_get(&key).await.expect("kv_get after delete"), None);
}

/// SQLite is a real embedded SQL engine (not an in-memory fake map) and needs no
/// external infrastructure, so it always runs. `max_connections(1)` keeps the shared
/// in-memory database alive across the round-trip.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_native_store_roundtrip() {
    use crate::runtime::service::native_entity_store::SqliteNativeEntityStore;
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    assert_roundtrip(SqliteNativeEntityStore::new(pool)).await;
}

/// Live Postgres round-trip — gated on `UDB_PG_DSN` (the Docker fleet).
#[tokio::test]
async fn postgres_native_store_roundtrip() {
    let Ok(dsn) = std::env::var("UDB_PG_DSN") else {
        eprintln!("skip postgres_native_store_roundtrip: UDB_PG_DSN unset");
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect postgres");
    assert_roundtrip(PostgresNativeEntityStore::new(pool)).await;
}

/// Live MySQL round-trip — gated on `UDB_MYSQL_DSN` (the Docker fleet). P2 acceptance:
/// native-service persistence working on a NON-Postgres SQL backend.
#[cfg(feature = "mysql")]
#[tokio::test]
async fn mysql_native_store_roundtrip() {
    use crate::runtime::service::native_entity_store::MySqlNativeEntityStore;
    let Ok(dsn) = std::env::var("UDB_MYSQL_DSN") else {
        eprintln!("skip mysql_native_store_roundtrip: UDB_MYSQL_DSN unset");
        return;
    };
    let pool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect mysql");
    assert_roundtrip(MySqlNativeEntityStore::new(pool)).await;
}

/// Live Neo4j round-trip — gated on `UDB_GRAPH_DSN` (the Docker fleet). P3 acceptance:
/// native persistence working on a GRAPH backend via HTTP Cypher.
#[cfg(feature = "neo4j")]
#[tokio::test]
async fn neo4j_native_store_roundtrip() {
    use crate::runtime::executors::neo4j::{Neo4jConfig, Neo4jExecutor};
    use crate::runtime::service::native_entity_store::Neo4jNativeEntityStore;
    let Some(config) = Neo4jConfig::from_env() else {
        eprintln!("skip neo4j_native_store_roundtrip: UDB_GRAPH_DSN unset");
        return;
    };
    assert_roundtrip(Neo4jNativeEntityStore::new(Neo4jExecutor::new(config))).await;
}

/// Live MongoDB round-trip — gated on `UDB_NOSQL_DSN`/`UDB_MONGODB_DSN` (the Docker
/// fleet) and the `mongodb-native` feature (the wire driver the Docker mongod speaks).
/// P3 acceptance: native persistence working on a DOCUMENT backend.
#[cfg(feature = "mongodb-native")]
#[tokio::test]
async fn mongodb_native_store_roundtrip() {
    use crate::runtime::executors::mongodb::MongoDbExecutor;
    use crate::runtime::service::native_entity_store::MongoDbNativeEntityStore;
    let Some(executor) = MongoDbExecutor::from_env() else {
        eprintln!("skip mongodb_native_store_roundtrip: UDB_NOSQL_DSN unset");
        return;
    };
    assert_roundtrip(MongoDbNativeEntityStore::new(executor)).await;
}
