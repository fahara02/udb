//! Backend-neutral native-service persistence handle (extend_udb.md, P1–P2).
//!
//! Native control-plane services acquire their persistence through this trait rather
//! than a raw `PgPool`, so the concrete backend — resolved from the service's proto
//! `native_service` binding via [`super::native_store_binding`] — can vary without
//! changing service call sites. The trait exposes a small **backend-neutral key/value
//! persistence** surface (`ensure_kv_table`/`kv_put`/`kv_get`/`kv_delete`) implemented
//! per backend with that engine's own dialect (mirroring `runtime::canonical_store`),
//! so native state round-trips identically on Postgres, SQLite and MySQL. P2 ships
//! those three SQL impls; further backends + migrating each service's bespoke SQL onto
//! the neutral surface continue the rollout.
//!
//! The `pg_pool()` accessor is a TRANSITIONAL bridge: today's services still issue
//! Postgres SQL directly, so the Postgres store hands back its pool. Non-Postgres
//! stores return `None`.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::PgPool;
use tonic::Status;

/// The UDB-owned native key/value table (portable across the SQL backends).
const KV_TABLE: &str = "udb_native_kv";
/// The graph node label for the native KV store (Neo4j).
#[cfg(feature = "neo4j")]
const KV_LABEL: &str = "UdbNativeKv";

fn native_store_internal_status(
    backend: &str,
    operation: impl Into<String>,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::internal_status(backend, operation, message)
}

fn store_err(op: &str, backend: &str, err: sqlx::Error) -> Status {
    native_store_internal_status(
        backend,
        op,
        format!("native store {op} on '{backend}' failed: {err}"),
    )
}

#[cfg(any(feature = "neo4j", feature = "mongodb"))]
fn store_err_str(op: &str, backend: &str, err: String) -> Status {
    native_store_internal_status(
        backend,
        op,
        format!("native store {op} on '{backend}' failed: {err}"),
    )
}

/// Backend-neutral persistence handle for a native control-plane service.
#[async_trait]
pub(crate) trait NativeEntityStore: Send + Sync {
    /// The backend kind this store persists to (e.g. `"postgres"`).
    fn backend(&self) -> &'static str;

    /// Idempotent DDL for the native KV table.
    async fn ensure_kv_table(&self) -> Result<(), Status>;

    /// Upsert a string value by key.
    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status>;

    /// Read a value by key (`None` when absent).
    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status>;

    /// Delete a key (idempotent).
    async fn kv_delete(&self, key: &str) -> Result<(), Status>;

    /// The Postgres pool when this store is Postgres-backed; `None` otherwise.
    /// Transitional — see the module docs.
    fn pg_pool(&self) -> Option<&PgPool> {
        None
    }
}

/// Round-trip the KV surface (ensure → put → get → delete) to prove native
/// persistence is actually writable+readable on the bound backend. Used by the
/// runtime's native-store self-check (capability honesty: a backend the binding
/// claims must really persist) and by the live conformance test.
pub(crate) async fn kv_roundtrip_probe(
    store: &dyn NativeEntityStore,
    key: &str,
    value: &str,
) -> Result<(), Status> {
    store.ensure_kv_table().await?;
    store.kv_put(key, value).await?;
    let got = store.kv_get(key).await?;
    if got.as_deref() != Some(value) {
        return Err(native_store_internal_status(
            store.backend(),
            "kv_roundtrip_probe",
            format!(
                "native store '{}' kv round-trip mismatch: wrote {value:?}, read {got:?}",
                store.backend()
            ),
        ));
    }
    store.kv_delete(key).await?;
    Ok(())
}

/// Postgres-backed native entity store (P1 reference impl).
pub(crate) struct PostgresNativeEntityStore {
    pool: PgPool,
}

impl PostgresNativeEntityStore {
    pub(crate) fn new(pool: PgPool) -> Arc<dyn NativeEntityStore> {
        Arc::new(Self { pool })
    }
}

#[async_trait]
impl NativeEntityStore for PostgresNativeEntityStore {
    fn backend(&self) -> &'static str {
        "postgres"
    }

    fn pg_pool(&self) -> Option<&PgPool> {
        Some(&self.pool)
    }

    async fn ensure_kv_table(&self) -> Result<(), Status> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {KV_TABLE} (\
             store_key TEXT PRIMARY KEY, \
             store_value TEXT NOT NULL, \
             updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("ensure_kv_table", "postgres", e))?;
        Ok(())
    }

    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status> {
        sqlx::query(&format!(
            "INSERT INTO {KV_TABLE} (store_key, store_value) VALUES ($1, $2) \
             ON CONFLICT (store_key) DO UPDATE SET store_value = EXCLUDED.store_value, updated_at = now()"
        ))
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("kv_put", "postgres", e))?;
        Ok(())
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status> {
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT store_value FROM {KV_TABLE} WHERE store_key = $1"
        ))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_err("kv_get", "postgres", e))?;
        Ok(row.map(|r| r.0))
    }

    async fn kv_delete(&self, key: &str) -> Result<(), Status> {
        sqlx::query(&format!("DELETE FROM {KV_TABLE} WHERE store_key = $1"))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| store_err("kv_delete", "postgres", e))?;
        Ok(())
    }
}

/// SQLite-backed native entity store (P2). Real embedded SQL backend — no external
/// infrastructure, so it round-trips in-process in CI.
#[cfg(feature = "sqlite")]
pub(crate) struct SqliteNativeEntityStore {
    pool: sqlx::SqlitePool,
}

#[cfg(feature = "sqlite")]
impl SqliteNativeEntityStore {
    pub(crate) fn new(pool: sqlx::SqlitePool) -> Arc<dyn NativeEntityStore> {
        Arc::new(Self { pool })
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl NativeEntityStore for SqliteNativeEntityStore {
    fn backend(&self) -> &'static str {
        "sqlite"
    }

    async fn ensure_kv_table(&self) -> Result<(), Status> {
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {KV_TABLE} (\
             store_key TEXT PRIMARY KEY, \
             store_value TEXT NOT NULL, \
             updated_at TEXT NOT NULL DEFAULT (datetime('now')))"
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("ensure_kv_table", "sqlite", e))?;
        Ok(())
    }

    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status> {
        sqlx::query(&format!(
            "INSERT INTO {KV_TABLE} (store_key, store_value) VALUES (?, ?) \
             ON CONFLICT(store_key) DO UPDATE SET store_value = excluded.store_value, updated_at = datetime('now')"
        ))
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("kv_put", "sqlite", e))?;
        Ok(())
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status> {
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT store_value FROM {KV_TABLE} WHERE store_key = ?"
        ))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_err("kv_get", "sqlite", e))?;
        Ok(row.map(|r| r.0))
    }

    async fn kv_delete(&self, key: &str) -> Result<(), Status> {
        sqlx::query(&format!("DELETE FROM {KV_TABLE} WHERE store_key = ?"))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| store_err("kv_delete", "sqlite", e))?;
        Ok(())
    }
}

/// MySQL-backed native entity store (P2). Verified against the live Docker MySQL.
#[cfg(feature = "mysql")]
pub(crate) struct MySqlNativeEntityStore {
    pool: sqlx::MySqlPool,
}

#[cfg(feature = "mysql")]
impl MySqlNativeEntityStore {
    pub(crate) fn new(pool: sqlx::MySqlPool) -> Arc<dyn NativeEntityStore> {
        Arc::new(Self { pool })
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl NativeEntityStore for MySqlNativeEntityStore {
    fn backend(&self) -> &'static str {
        "mysql"
    }

    async fn ensure_kv_table(&self) -> Result<(), Status> {
        // VARCHAR(191) keeps the PK inside the utf8mb4 index-length limit.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {KV_TABLE} (\
             store_key VARCHAR(191) PRIMARY KEY, \
             store_value LONGTEXT NOT NULL, \
             updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("ensure_kv_table", "mysql", e))?;
        Ok(())
    }

    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status> {
        sqlx::query(&format!(
            "INSERT INTO {KV_TABLE} (store_key, store_value) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE store_value = VALUES(store_value), updated_at = CURRENT_TIMESTAMP"
        ))
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| store_err("kv_put", "mysql", e))?;
        Ok(())
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status> {
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT store_value FROM {KV_TABLE} WHERE store_key = ?"
        ))
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| store_err("kv_get", "mysql", e))?;
        Ok(row.map(|r| r.0))
    }

    async fn kv_delete(&self, key: &str) -> Result<(), Status> {
        sqlx::query(&format!("DELETE FROM {KV_TABLE} WHERE store_key = ?"))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| store_err("kv_delete", "mysql", e))?;
        Ok(())
    }
}

/// Neo4j-backed native entity store (P3 — graph). Reuses the existing
/// [`crate::runtime::executors::neo4j::Neo4jExecutor`] (HTTP transactional Cypher,
/// auth/TLS/timeout handling) rather than re-implementing HTTP (directive #2): the KV
/// surface maps onto graph nodes labelled `UdbNativeKv` keyed by `id`.
#[cfg(feature = "neo4j")]
pub(crate) struct Neo4jNativeEntityStore {
    executor: crate::runtime::executors::neo4j::Neo4jExecutor,
}

#[cfg(feature = "neo4j")]
impl Neo4jNativeEntityStore {
    pub(crate) fn new(
        executor: crate::runtime::executors::neo4j::Neo4jExecutor,
    ) -> Arc<dyn NativeEntityStore> {
        Arc::new(Self { executor })
    }
}

#[cfg(feature = "neo4j")]
#[async_trait]
impl NativeEntityStore for Neo4jNativeEntityStore {
    fn backend(&self) -> &'static str {
        "neo4j"
    }

    async fn ensure_kv_table(&self) -> Result<(), Status> {
        // Graph nodes need no DDL; the MERGE in `kv_put` is idempotent.
        Ok(())
    }

    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status> {
        // create_node MERGEs on `id` and SETs the properties — an idempotent upsert.
        self.executor
            .create_node(KV_LABEL, key, serde_json::json!({ "value": value }))
            .await
            .map_err(|e| store_err_str("kv_put", "neo4j", e))
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status> {
        let rows = self
            .executor
            .cypher_rows(
                &format!("MATCH (n:{KV_LABEL} {{id: $id}}) RETURN n.value AS value"),
                serde_json::json!({ "id": key }),
            )
            .await
            .map_err(|e| store_err_str("kv_get", "neo4j", e))?;
        Ok(rows
            .first()
            .and_then(|row| row.get("value"))
            .and_then(|v| v.as_str())
            .map(String::from))
    }

    async fn kv_delete(&self, key: &str) -> Result<(), Status> {
        self.executor
            .delete_node(KV_LABEL, key)
            .await
            .map_err(|e| store_err_str("kv_delete", "neo4j", e))
    }
}

/// MongoDB-backed native entity store (P3 — document). Reuses the existing
/// [`crate::runtime::executors::mongodb::MongoDbExecutor`] (native wire driver under
/// `mongodb-native`, Atlas Data API otherwise) rather than re-implementing a driver
/// (directive #2): the KV surface maps onto documents `{_id: key, value}` in the
/// `udb_native_kv` collection.
#[cfg(feature = "mongodb")]
pub(crate) struct MongoDbNativeEntityStore {
    executor: crate::runtime::executors::mongodb::MongoDbExecutor,
}

#[cfg(feature = "mongodb")]
impl MongoDbNativeEntityStore {
    pub(crate) fn new(
        executor: crate::runtime::executors::mongodb::MongoDbExecutor,
    ) -> Arc<dyn NativeEntityStore> {
        Arc::new(Self { executor })
    }
}

#[cfg(feature = "mongodb")]
#[async_trait]
impl NativeEntityStore for MongoDbNativeEntityStore {
    fn backend(&self) -> &'static str {
        "mongodb"
    }

    async fn ensure_kv_table(&self) -> Result<(), Status> {
        // Collections auto-create on first write; no DDL needed.
        Ok(())
    }

    async fn kv_put(&self, key: &str, value: &str) -> Result<(), Status> {
        self.executor
            .upsert_document(
                KV_TABLE,
                serde_json::json!({ "_id": key }),
                serde_json::json!({ "$set": { "value": value } }),
            )
            .await
            .map(|_| ())
            .map_err(|e| store_err_str("kv_put", "mongodb", e))
    }

    async fn kv_get(&self, key: &str) -> Result<Option<String>, Status> {
        let docs = self
            .executor
            .find_documents(
                KV_TABLE,
                serde_json::json!({ "_id": key }),
                serde_json::json!({}),
                1,
            )
            .await
            .map_err(|e| store_err_str("kv_get", "mongodb", e))?;
        Ok(docs
            .first()
            .and_then(|doc| doc.get("value"))
            .and_then(|v| v.as_str())
            .map(String::from))
    }

    async fn kv_delete(&self, key: &str) -> Result<(), Status> {
        self.executor
            .delete_document(KV_TABLE, serde_json::json!({ "_id": key }))
            .await
            .map(|_| ())
            .map_err(|e| store_err_str("kv_delete", "mongodb", e))
    }
}

// Live round-trip tests live in `native_entity_store_tests.rs` (a `_tests.rs` file):
// they read `UDB_PG_DSN`/`UDB_MYSQL_DSN` to gate the live backends, which the
// runtime env-confinement guard excludes for test files only.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn native_store_internal_status_carries_typed_detail() {
        let status = native_store_internal_status(
            "postgres",
            "kv_get",
            "native store kv_get on 'postgres' failed: unavailable",
        );
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(
            status.message(),
            "native store kv_get on 'postgres' failed: unavailable"
        );
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "postgres");
        assert_eq!(detail.operation, "kv_get");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }
}
