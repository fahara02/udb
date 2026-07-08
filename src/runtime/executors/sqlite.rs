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
    AppliedContext, BackendContextEnforcer, ContextEffect, SqlDialect, render_sql_session_settings,
};
use crate::runtime::core::{validate_mutation_sql, validate_read_sql};
use crate::runtime::executor_utils::{
    apply_context_statements, bind_json_params, build_probe, capability_status,
    invalid_argument_fields, parse_sql_dispatch, sqlx_row_to_json, with_executor_timeout,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

pub struct SqliteExecutor {
    pub(crate) pool: SqlitePool,
    /// Optional request context. When `Some`, `query`/`mutate` wrap the
    /// SQL in a transaction and populate a temporary `_udb_context`
    /// table with the request's tenant/project/purpose. Operator-installed
    /// RLS-style views can read from that table. NOTE: this populates tenant
    /// session context ONLY; SQLite has no native RLS, so it performs NO
    /// in-engine row filtering — operator views / broker tenant-predicate
    /// injection are REQUIRED for isolation. See `enforce` for the honest posture.
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
            sqlite_internal_status(
                "context_table_create",
                format!("sqlite context table create failed: {err}"),
            )
        })?;
        let applied = AppliedContext::from_request(ctx);
        let stmts = render_sql_session_settings(&applied, SqlDialect::Sqlite);
        apply_context_statements(tx, &stmts, "sqlite context insert failed").await
    }
}

fn sqlite_invalid_field_status(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_sqlite_resource_spec_status(err: serde_json::Error) -> tonic::Status {
    sqlite_invalid_field_status(
        "spec_json",
        "must be valid JSON for SQLite table resource creation",
        format!("invalid resource spec: {err}"),
    )
}

fn invalid_sqlite_tx_json_status(err: serde_json::Error) -> tonic::Status {
    sqlite_invalid_field_status(
        "request_json",
        "must be valid JSON for SQLite transaction dispatch",
        format!("invalid tx JSON: {err}"),
    )
}

fn sqlite_required_field_status(field: &'static str, message: &'static str) -> tonic::Status {
    sqlite_invalid_field_status(field, format!("{field} is required"), message)
}

fn sqlite_identifier_status(field: &'static str, message: String) -> tonic::Status {
    sqlite_invalid_field_status(field, "must be a valid SQLite identifier", message)
}

fn sqlite_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("sqlite", operation, message)
}

fn encode_sqlite_response(
    value: &JsonValue,
    operation: &'static str,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value).map_err(|err| {
        sqlite_internal_status(operation, format!("response serialise failed: {err}"))
    })
}

fn validate_sqlite_ident(value: &str, field: &'static str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(sqlite_identifier_status(
            field,
            format!("invalid SQLite identifier '{value}'"),
        ));
    }
    Ok(())
}

fn quote_sqlite_ident(value: &str, field: &'static str) -> Result<String, tonic::Status> {
    validate_sqlite_ident(value, field)?;
    Ok(format!("\"{value}\""))
}

fn sqlite_create_table_sql(resource_name: &str, spec_json: &str) -> Result<String, tonic::Status> {
    let spec: JsonValue =
        serde_json::from_str(spec_json).map_err(invalid_sqlite_resource_spec_status)?;
    let columns = spec
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            sqlite_required_field_status("columns", "table resource spec requires columns")
        })?;
    if columns.is_empty() {
        return Err(sqlite_required_field_status(
            "columns",
            "table resource spec requires at least one column",
        ));
    }
    let mut defs = Vec::with_capacity(columns.len() + 1);
    let mut pk_cols = Vec::new();
    for column in columns {
        let name = column
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| sqlite_required_field_status("columns.name", "column missing name"))?;
        let ty = column
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| sqlite_required_field_status("columns.type", "column missing type"))?;
        if !ty
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '(' | ')' | ',' | ' '))
        {
            return Err(sqlite_invalid_field_status(
                "columns.type",
                "must contain only SQLite type characters",
                format!("invalid SQL type for column '{name}'"),
            ));
        }
        let quoted = quote_sqlite_ident(name, "columns.name")?;
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
        quote_sqlite_ident(resource_name, "resource_name")?,
        defs.join(", ")
    ))
}

impl BackendContextEnforcer for SqliteExecutor {
    fn backend_label(&self) -> &str {
        "sqlite"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        // HONEST POSTURE (M1): SQLite has NO native row-level-security engine.
        // Populating the `_udb_context` temp table publishes the tenant context,
        // but performs NO in-engine row filtering by itself — rows are only scoped
        // if the OPERATOR installs views that JOIN/filter on `_udb_context` (e.g.
        // `WHERE tenant_id = (SELECT value FROM _udb_context WHERE key=...)`), or
        // if the broker injects a tenant predicate at compile time. Therefore the
        // temp-table population is `Advisory`, not `Enforced`: the broker records
        // the context for operator-side policy to consume, but does not itself
        // constrain row visibility. This matches `supports_rls: false` for SQLite.
        // Without operator views OR broker predicate injection, tenant isolation
        // on SQLite is application-trust.
        if ctx.is_empty() {
            ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            }
        } else {
            ContextEffect::Advisory {
                recorded_in: "_udb_context temp table populated (no native SQLite RLS — \
                              operator-installed views / broker tenant-predicate injection \
                              REQUIRED for row isolation)"
                    .into(),
            }
        }
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
                    sqlite_internal_status(
                        "query_transaction_start",
                        format!("sqlite transaction start failed: {err}"),
                    )
                })?;
                Self::apply_context_table(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let rows = q.fetch_all(&mut *tx).await.map_err(|e| {
                    sqlite_internal_status("query", format!("sqlite query failed: {e}"))
                })?;
                tx.commit().await.map_err(|err| {
                    sqlite_internal_status(
                        "query_transaction_commit",
                        format!("sqlite transaction commit failed: {err}"),
                    )
                })?;
                rows
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                q.fetch_all(&self.pool).await.map_err(|e| {
                    sqlite_internal_status("query", format!("sqlite query failed: {e}"))
                })?
            };
            let json: Vec<JsonValue> = rows.iter().map(sqlx_row_to_json).collect();
            encode_sqlite_response(&JsonValue::Array(json), "query_response_encode")
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
                    sqlite_internal_status(
                        "mutate_transaction_start",
                        format!("sqlite transaction start failed: {err}"),
                    )
                })?;
                Self::apply_context_table(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q.execute(&mut *tx).await.map_err(|e| {
                    sqlite_internal_status("mutate", format!("sqlite mutate failed: {e}"))
                })?;
                let rows_affected = result.rows_affected();
                let last_insert_rowid = result.last_insert_rowid();
                tx.commit().await.map_err(|err| {
                    sqlite_internal_status(
                        "mutate_transaction_commit",
                        format!("sqlite transaction commit failed: {err}"),
                    )
                })?;
                Ok(serde_json::json!({
                    "rows_affected": rows_affected,
                    "last_insert_rowid": last_insert_rowid,
                })
                .to_string())
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q.execute(&self.pool).await.map_err(|e| {
                    sqlite_internal_status("mutate", format!("sqlite mutate failed: {e}"))
                })?;
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
        Err(capability_status(
            "sqlite",
            "search",
            "vector_search",
            "UDB_UNSUPPORTED_OPERATION: SQLite backend does not provide native vector search; \
             use `query` with FTS5 or route through a vector backend (Qdrant)",
        ))
    }
}

impl ObjectExecutor for SqliteExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "sqlite",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: SQLite backend is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "sqlite",
            "put_object",
            "object_store",
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
        sqlx::query(&ddl).execute(&self.pool).await.map_err(|e| {
            sqlite_internal_status(
                "ensure_resource",
                format!("sqlite ensure_resource failed: {e}"),
            )
        })?;
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let ddl = format!(
            "DROP TABLE IF EXISTS {}",
            quote_sqlite_ident(resource_name, "resource_name")?
        );
        sqlx::query(&ddl).execute(&self.pool).await.map_err(|e| {
            sqlite_internal_status("drop_resource", format!("sqlite drop_resource failed: {e}"))
        })?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type = 'table'")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                sqlite_internal_status("list_resources", format!("sqlite_master query failed: {e}"))
            })?;
        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>(0).ok())
            .collect();
        Ok(names)
    }
}

impl BackendExecutor for SqliteExecutor {
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status> {
        let value: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_sqlite_tx_json_status)?;
        let stmts = value
            .get("statements")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                sqlite_required_field_status(
                    "statements",
                    "missing `statements` array in tx request",
                )
            })?
            .clone();
        let mut tx = self.pool.begin().await.map_err(|e| {
            sqlite_internal_status("transaction_begin", format!("BEGIN failed: {e}"))
        })?;
        let mut total_affected: u64 = 0;
        for stmt in &stmts {
            let sql = stmt.get("sql").and_then(|v| v.as_str()).ok_or_else(|| {
                sqlite_required_field_status("statements.sql", "tx statement missing `sql`")
            })?;
            let params = stmt
                .get("params")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let q = bind_json_params(sqlx::query(sql), &params);
            let r = q.execute(&mut *tx).await.map_err(|e| {
                sqlite_internal_status("transaction_statement", format!("tx statement failed: {e}"))
            })?;
            total_affected += r.rows_affected();
        }
        tx.commit().await.map_err(|e| {
            sqlite_internal_status("transaction_commit", format!("COMMIT failed: {e}"))
        })?;
        Ok(serde_json::json!({"rows_affected": total_affected}).to_string())
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("sqlite", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use sqlx::sqlite::SqlitePoolOptions;

    async fn pool() -> SqlitePool {
        SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite")
    }

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_single_field(status: &tonic::Status, field: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "sqlite");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn sqlite_internal_status_carries_typed_detail() {
        let status = sqlite_internal_status(
            "context_table_create",
            "sqlite context table create failed: readonly",
        );
        assert_internal_detail(
            &status,
            "context_table_create",
            "sqlite context table create failed: readonly",
        );

        let status = sqlite_internal_status("transaction_commit", "COMMIT failed: closed");
        assert_internal_detail(&status, "transaction_commit", "COMMIT failed: closed");
    }

    #[test]
    fn sqlite_resource_validation_carries_field_violations() {
        let invalid_json = sqlite_create_table_sql("items", "{").unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid resource spec:"));
        assert_single_field(&invalid_json, "spec_json");

        let missing_columns = sqlite_create_table_sql("items", "{}").unwrap_err();
        assert_eq!(
            missing_columns.message(),
            "table resource spec requires columns"
        );
        assert_single_field(&missing_columns, "columns");

        let empty_columns = sqlite_create_table_sql("items", r#"{"columns":[]}"#).unwrap_err();
        assert_eq!(
            empty_columns.message(),
            "table resource spec requires at least one column"
        );
        assert_single_field(&empty_columns, "columns");

        let missing_name =
            sqlite_create_table_sql("items", r#"{"columns":[{"type":"TEXT"}]}"#).unwrap_err();
        assert_eq!(missing_name.message(), "column missing name");
        assert_single_field(&missing_name, "columns.name");

        let missing_type =
            sqlite_create_table_sql("items", r#"{"columns":[{"name":"name"}]}"#).unwrap_err();
        assert_eq!(missing_type.message(), "column missing type");
        assert_single_field(&missing_type, "columns.type");

        let invalid_type = sqlite_create_table_sql(
            "items",
            r#"{"columns":[{"name":"name","type":"TEXT;DROP"}]}"#,
        )
        .unwrap_err();
        assert_eq!(invalid_type.message(), "invalid SQL type for column 'name'");
        assert_single_field(&invalid_type, "columns.type");

        let invalid_identifier =
            sqlite_create_table_sql("bad-name", r#"{"columns":[{"name":"name","type":"TEXT"}]}"#)
                .unwrap_err();
        assert_eq!(
            invalid_identifier.message(),
            "invalid SQLite identifier 'bad-name'"
        );
        assert_single_field(&invalid_identifier, "resource_name");
    }

    #[tokio::test]
    async fn sqlite_transaction_validation_carries_field_violations() {
        let exec = SqliteExecutor::with_pool(pool().await);

        let invalid_json = BackendExecutor::transaction(&exec, "{").await.unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid tx JSON:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_statements = BackendExecutor::transaction(&exec, "{}").await.unwrap_err();
        assert_eq!(
            missing_statements.message(),
            "missing `statements` array in tx request"
        );
        assert_single_field(&missing_statements, "statements");

        let missing_sql = BackendExecutor::transaction(&exec, r#"{"statements":[{}]}"#)
            .await
            .unwrap_err();
        assert_eq!(missing_sql.message(), "tx statement missing `sql`");
        assert_single_field(&missing_sql, "statements.sql");
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
