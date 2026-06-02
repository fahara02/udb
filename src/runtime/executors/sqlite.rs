//! `SqliteExecutor` — sqlx-sqlite-backed generic dispatch executor.
//!
//! Mirror of the MySQL executor for SQLite. Same dispatch-JSON
//! contract (`{"sql": "...", "params": [...]}`), same trait coverage,
//! same honest unsupported errors for vector/object operations.
//!
//! ## SQLite-specific notes
//!
//! - **In-memory mode** is fully supported. A SqliteExecutor wrapping
//!   `SqlitePool::connect("sqlite::memory:")` gives every test a
//!   real generic-dispatch backend with no Docker, no daemon.
//! - **Type coercion**: SQLite is dynamically typed (every value has
//!   a runtime type, not a column type). The row-to-JSON converter
//!   tries i64 → f64 → String → bytes in that order.
//! - **Transactions**: SQLite supports nested SAVEPOINTs but
//!   single-writer at a time. The transaction method here uses sqlx's
//!   `begin`/`commit` which translate to `BEGIN` + `COMMIT`.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use sqlx::Row;
use sqlx::SqlitePool;

use crate::broker::RequestContext;
use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, SqlDialect, enforce_with_mechanism,
    render_sql_session_settings,
};
use crate::runtime::core::{validate_mutation_sql, validate_read_sql};
use crate::runtime::executor_utils::{
    apply_context_statements, bind_json_params, build_probe, parse_sql_dispatch, sqlx_row_to_json,
    with_executor_timeout,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

pub struct SqliteExecutor {
    pub(crate) pool: SqlitePool,
    /// Optional request context. When `Some`, `query`/`mutate` wrap the
    /// SQL in a transaction and populate a temporary `_udb_context`
    /// table with the request's tenant/project/purpose. RLS-style
    /// views can read from that table.
    pub(crate) context: Option<Arc<RequestContext>>,
}

impl SqliteExecutor {
    pub fn with_pool(pool: SqlitePool) -> Self {
        Self {
            pool,
            context: None,
        }
    }

    /// Context-bound constructor — every dispatched query/mutate runs
    /// inside a transaction with the `_udb_context` temp table
    /// populated, so RLS-style views can introspect tenant context.
    pub fn with_context(pool: SqlitePool, context: Arc<RequestContext>) -> Self {
        Self {
            pool,
            context: Some(context),
        }
    }

    async fn apply_context_table(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &RequestContext,
    ) -> Result<(), tonic::Status> {
        // Ensure the temp table exists. Temp tables live for the
        // connection's lifetime; the executor's tx ties them to the
        // active connection.
        sqlx::query(
            "CREATE TEMP TABLE IF NOT EXISTS _udb_context(key TEXT PRIMARY KEY, value TEXT)",
        )
        .execute(&mut **tx)
        .await
        .map_err(|err| {
            tonic::Status::internal(format!("sqlite context table create failed: {err}"))
        })?;
        let applied = AppliedContext::from_request(ctx);
        let stmts = render_sql_session_settings(&applied, SqlDialect::Sqlite);
        apply_context_statements(tx, &stmts, "sqlite context insert failed").await
    }
}

fn validate_sqlite_ident(value: &str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(tonic::Status::invalid_argument(format!(
            "invalid SQLite identifier '{value}'"
        )));
    }
    Ok(())
}

fn quote_sqlite_ident(value: &str) -> Result<String, tonic::Status> {
    validate_sqlite_ident(value)?;
    Ok(format!("\"{value}\""))
}

fn sqlite_create_table_sql(resource_name: &str, spec_json: &str) -> Result<String, tonic::Status> {
    let spec: JsonValue = serde_json::from_str(spec_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid resource spec: {e}")))?;
    let columns = spec
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| tonic::Status::invalid_argument("table resource spec requires columns"))?;
    if columns.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "table resource spec requires at least one column",
        ));
    }
    let mut defs = Vec::with_capacity(columns.len() + 1);
    let mut pk_cols = Vec::new();
    for column in columns {
        let name = column
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| tonic::Status::invalid_argument("column missing name"))?;
        let ty = column
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| tonic::Status::invalid_argument("column missing type"))?;
        if !ty
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '(' | ')' | ',' | ' '))
        {
            return Err(tonic::Status::invalid_argument(format!(
                "invalid SQL type for column '{name}'"
            )));
        }
        let quoted = quote_sqlite_ident(name)?;
        if column
            .get("primary_key")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            pk_cols.push(quoted.clone());
        }
        let null_clause = if column
            .get("not_null")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            " NOT NULL"
        } else {
            ""
        };
        defs.push(format!("{quoted} {ty}{null_clause}"));
    }
    if !pk_cols.is_empty() {
        defs.push(format!("PRIMARY KEY ({})", pk_cols.join(", ")));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_sqlite_ident(resource_name)?,
        defs.join(", ")
    ))
}

impl BackendContextEnforcer for SqliteExecutor {
    fn backend_label(&self) -> &str {
        "sqlite"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        enforce_with_mechanism(
            ctx,
            "_udb_context temp table populated per request-scoped transaction",
        )
    }
}

impl BackendHealth for SqliteExecutor {
    async fn ping(&self) -> Result<(), String> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

impl QueryExecutor for SqliteExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("SQLite", "query", async {
            let (sql, params) = parse_sql_dispatch(request_json)?;
            validate_read_sql(&sql)?;
            let rows = if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    tonic::Status::internal(format!("sqlite transaction start failed: {err}"))
                })?;
                Self::apply_context_table(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let rows = q
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| tonic::Status::internal(format!("sqlite query failed: {e}")))?;
                tx.commit().await.map_err(|err| {
                    tonic::Status::internal(format!("sqlite transaction commit failed: {err}"))
                })?;
                rows
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                q.fetch_all(&self.pool)
                    .await
                    .map_err(|e| tonic::Status::internal(format!("sqlite query failed: {e}")))?
            };
            let json: Vec<JsonValue> = rows.iter().map(sqlx_row_to_json).collect();
            serde_json::to_string(&JsonValue::Array(json))
                .map_err(|e| tonic::Status::internal(format!("response serialise failed: {e}")))
        })
        .await
    }
}

impl MutationExecutor for SqliteExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("SQLite", "mutate", async {
            let (sql, params) = parse_sql_dispatch(request_json)?;
            validate_mutation_sql(&sql)?;
            if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    tonic::Status::internal(format!("sqlite transaction start failed: {err}"))
                })?;
                Self::apply_context_table(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| tonic::Status::internal(format!("sqlite mutate failed: {e}")))?;
                let rows_affected = result.rows_affected();
                let last_insert_rowid = result.last_insert_rowid();
                tx.commit().await.map_err(|err| {
                    tonic::Status::internal(format!("sqlite transaction commit failed: {err}"))
                })?;
                Ok(serde_json::json!({
                    "rows_affected": rows_affected,
                    "last_insert_rowid": last_insert_rowid,
                })
                .to_string())
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q
                    .execute(&self.pool)
                    .await
                    .map_err(|e| tonic::Status::internal(format!("sqlite mutate failed: {e}")))?;
                Ok(serde_json::json!({
                    "rows_affected": result.rows_affected(),
                    "last_insert_rowid": result.last_insert_rowid(),
                })
                .to_string())
            }
        })
        .await
    }
}

impl SearchExecutor for SqliteExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: SQLite backend does not provide native vector search; \
             use `query` with FTS5 or route through a vector backend (Qdrant)",
        ))
    }
}

impl ObjectExecutor for SqliteExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: SQLite backend is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: SQLite backend is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for SqliteExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let ddl = sqlite_create_table_sql(resource_name, spec_json)?;
        sqlx::query(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| tonic::Status::internal(format!("sqlite ensure_resource failed: {e}")))?;
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let ddl = format!(
            "DROP TABLE IF EXISTS {}",
            quote_sqlite_ident(resource_name)?
        );
        sqlx::query(&ddl)
            .execute(&self.pool)
            .await
            .map_err(|e| tonic::Status::internal(format!("sqlite drop_resource failed: {e}")))?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| tonic::Status::internal(format!("sqlite_master query failed: {e}")))?;
        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>(0).ok())
            .collect();
        Ok(names)
    }
}

impl BackendExecutor for SqliteExecutor {
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status> {
        let value: JsonValue = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid tx JSON: {e}")))?;
        let stmts = value
            .get("statements")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                tonic::Status::invalid_argument("missing `statements` array in tx request")
            })?
            .clone();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| tonic::Status::internal(format!("BEGIN failed: {e}")))?;
        let mut total_affected: u64 = 0;
        for stmt in &stmts {
            let sql = stmt
                .get("sql")
                .and_then(|v| v.as_str())
                .ok_or_else(|| tonic::Status::invalid_argument("tx statement missing `sql`"))?;
            let params = stmt
                .get("params")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let q = bind_json_params(sqlx::query(sql), &params);
            let r = q
                .execute(&mut *tx)
                .await
                .map_err(|e| tonic::Status::internal(format!("tx statement failed: {e}")))?;
            total_affected += r.rows_affected();
        }
        tx.commit()
            .await
            .map_err(|e| tonic::Status::internal(format!("COMMIT failed: {e}")))?;
        Ok(serde_json::json!({"rows_affected": total_affected}).to_string())
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("sqlite", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite")
    }

    /// Pin: end-to-end smoke. Create a table, insert via mutate,
    /// select via query, get the row back. Proves the executor
    /// contract works without a network database.
    #[tokio::test]
    async fn end_to_end_round_trip_against_in_memory_sqlite() {
        let exec = SqliteExecutor::with_pool(pool().await);

        // 1. Health check
        exec.ping().await.expect("ping");

        // 2. Create fixture schema directly; generic mutate is intentionally
        // restricted to DML verbs.
        sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)")
            .execute(&exec.pool)
            .await
            .expect("create");

        // 3. INSERT via mutate with parameters
        let resp = exec
            .mutate(
                r#"{"sql":"INSERT INTO items(name, qty) VALUES (?, ?)","params":["widget", 7]}"#,
            )
            .await
            .expect("insert");
        let parsed: JsonValue = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["rows_affected"], 1);
        assert_eq!(parsed["last_insert_rowid"], 1);

        // 4. SELECT via query
        let resp = exec
            .query(r#"{"sql":"SELECT id, name, qty FROM items WHERE qty > ?","params":[5]}"#)
            .await
            .expect("select");
        let rows: Vec<JsonValue> = serde_json::from_str(&resp).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "widget");
        assert_eq!(rows[0]["qty"], 7);

        // 5. list_resources sees the table.
        let resources = exec.list_resources().await.expect("list");
        assert!(resources.iter().any(|n| n == "items"));
    }

    /// Pin: search returns the unsupported-operation error code so
    /// generic dispatch routes the request to the right backend.
    #[tokio::test]
    async fn search_returns_unsupported_operation() {
        let exec = SqliteExecutor::with_pool(pool().await);
        let err = exec.search("{}").await.expect_err("should refuse");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("UDB_UNSUPPORTED_OPERATION"));
        assert!(err.message().contains("SQLite"));
    }

    /// Pin: transaction with two statements commits atomically.
    #[tokio::test]
    async fn transaction_commits_multiple_statements() {
        let exec = SqliteExecutor::with_pool(pool().await);
        sqlx::query("CREATE TABLE t (n INTEGER)")
            .execute(&exec.pool)
            .await
            .unwrap();
        let resp = exec
            .transaction(
                r#"{"statements":[
                {"sql":"INSERT INTO t(n) VALUES (?)","params":[1]},
                {"sql":"INSERT INTO t(n) VALUES (?)","params":[2]}
            ]}"#,
            )
            .await
            .expect("tx");
        let parsed: JsonValue = serde_json::from_str(&resp).unwrap();
        assert_eq!(parsed["rows_affected"], 2);

        let rows = exec
            .query(r#"{"sql":"SELECT COUNT(*) AS c FROM t","params":[]}"#)
            .await
            .unwrap();
        let arr: Vec<JsonValue> = serde_json::from_str(&rows).unwrap();
        assert_eq!(arr[0]["c"], 2);
    }
}
