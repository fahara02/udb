//! `MysqlExecutor` — sqlx-mysql-backed generic dispatch executor.
//!
//! Mirror of `PostgresExecutor` for MySQL. Implements the same
//! executor traits (`BackendHealth`, `QueryExecutor`, `MutationExecutor`),
//! returns typed capability details for Vector /
//! Object refusals (MySQL is not a vector store or object
//! store — these operations are honestly unsupported).
//!
//! ## Dispatch-JSON contract
//!
//! Same shape as the Postgres executor: requests carry
//! `{"sql": "...", "params": [...]}`. Params are passed positionally
//! as MySQL `?` placeholders (sqlx converts `?` from the SQL string
//! to MySQL-protocol prepared-statement params).
//!
//! ## NW-deep — RequestContext propagation (NOT native RLS)
//!
//! When constructed with `with_context`, the executor wraps every
//! dispatched SQL in a session-scoped transaction and SETs the
//! `@app_current_tenant_id` / `@app_current_project_id` /
//! `@app_current_purpose` user variables before running the SQL.
//!
//! **Limitation (capability-honest):** MySQL has no native row-level-security
//! engine, so these session vars perform NO in-engine row filtering on their
//! own. They merely *publish* the tenant context so that operator-installed
//! views / stored procedures (e.g. `WHERE tenant_id = @app_current_tenant_id`)
//! — or broker-side tenant-predicate injection at compile time — can scope rows.
//! Accordingly `BackendContextEnforcer::enforce` reports `Advisory` (not
//! `Enforced`) and the capability matrix reports `supports_rls: false`. Without
//! operator views OR broker predicate injection, tenant isolation on MySQL is
//! application-trust, not broker-enforced — see `enforce` below.
//!
//! ## What this DOES NOT do yet
//!
//! - **Replica routing**: the canonical-store router needs to be
//!   extended to MySQL's `SHOW REPLICA STATUS` model. Today the
//!   executor talks to whatever pool the caller hands it.
//! - **Capability-matrix joins**: complex multi-table planning is
//!   server-side (MySQL itself) rather than fused; only generic
//!   single-statement dispatch is wired here.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use sqlx::MySqlPool;
use sqlx::Row;

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

/// Public so the dispatcher can construct it. Slim and stateless —
/// the pool is held in the runtime supervisor and cloned in here.
pub struct MysqlExecutor {
    pub(crate) pool: MySqlPool,
    /// Optional request context. When `Some`, `query`/`mutate` wrap the
    /// SQL in a transaction and SET MySQL session vars
    /// (`@app_current_tenant_id` etc.) before running the user
    /// statement. Mirrors `PostgresExecutor::with_context`.
    pub(crate) context: Option<Arc<RequestContext>>,
}

impl MysqlExecutor {
    pub fn with_pool(pool: MySqlPool) -> Self {
        Self {
            pool,
            context: None,
        }
    }

    /// Context-bound constructor — every dispatched query/mutate runs
    /// inside a transaction with session vars set, so operator-installed
    /// RLS-style views can introspect tenant context. NOTE: this sets the
    /// tenant session context ONLY; it performs NO in-engine row filtering —
    /// operator-installed views / broker tenant-predicate injection are REQUIRED
    /// for isolation on MySQL (no native RLS). See the module-level note.
    pub fn with_context(pool: MySqlPool, context: Arc<RequestContext>) -> Self {
        Self {
            pool,
            context: Some(context),
        }
    }

    /// Issue every `SET @key = value` statement that `AppliedContext`
    /// renders for MySQL. Called once at the start of each context-bound
    /// query / mutate transaction.
    async fn apply_session_vars(
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
        ctx: &RequestContext,
    ) -> Result<(), tonic::Status> {
        let applied = AppliedContext::from_request(ctx);
        let stmts = render_sql_session_settings(&applied, SqlDialect::Mysql);
        apply_context_statements(tx, &stmts, "mysql session var set failed").await
    }
}

fn mysql_invalid_field_status(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_mysql_resource_spec_status(err: serde_json::Error) -> tonic::Status {
    mysql_invalid_field_status(
        "spec_json",
        "must be valid JSON for MySQL table resource creation",
        format!("invalid resource spec: {err}"),
    )
}

fn invalid_mysql_tx_json_status(err: serde_json::Error) -> tonic::Status {
    mysql_invalid_field_status(
        "request_json",
        "must be valid JSON for MySQL transaction dispatch",
        format!("invalid tx JSON: {err}"),
    )
}

fn mysql_required_field_status(field: &'static str, message: &'static str) -> tonic::Status {
    mysql_invalid_field_status(field, format!("{field} is required"), message)
}

fn mysql_identifier_status(field: &'static str, message: String) -> tonic::Status {
    mysql_invalid_field_status(field, "must be a valid MySQL identifier", message)
}

fn mysql_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("mysql", operation, message)
}

fn encode_mysql_response(
    value: &JsonValue,
    operation: &'static str,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value).map_err(|err| {
        mysql_internal_status(operation, format!("response serialise failed: {err}"))
    })
}

fn validate_mysql_ident(value: &str, field: &'static str) -> Result<(), tonic::Status> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(mysql_identifier_status(
            field,
            format!("invalid MySQL identifier '{value}'"),
        ));
    }
    Ok(())
}

fn quote_mysql_ident(value: &str, field: &'static str) -> Result<String, tonic::Status> {
    validate_mysql_ident(value, field)?;
    Ok(format!("`{value}`"))
}

fn mysql_create_table_sql(resource_name: &str, spec_json: &str) -> Result<String, tonic::Status> {
    let spec: JsonValue =
        serde_json::from_str(spec_json).map_err(invalid_mysql_resource_spec_status)?;
    let columns = spec
        .get("columns")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            mysql_required_field_status("columns", "table resource spec requires columns")
        })?;
    if columns.is_empty() {
        return Err(mysql_required_field_status(
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
            .ok_or_else(|| mysql_required_field_status("columns.name", "column missing name"))?;
        let ty = column
            .get("type")
            .and_then(|v| v.as_str())
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| mysql_required_field_status("columns.type", "column missing type"))?;
        if !ty
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '(' | ')' | ',' | ' '))
        {
            return Err(mysql_invalid_field_status(
                "columns.type",
                "must contain only MySQL type characters",
                format!("invalid SQL type for column '{name}'"),
            ));
        }
        let quoted = quote_mysql_ident(name, "columns.name")?;
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
    let engine = spec
        .get("engine")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or("InnoDB");
    validate_mysql_ident(engine, "engine")?;
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE={engine} DEFAULT CHARSET=utf8mb4",
        quote_mysql_ident(resource_name, "resource_name")?,
        defs.join(", ")
    ))
}

impl BackendContextEnforcer for MysqlExecutor {
    fn backend_label(&self) -> &str {
        "mysql"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        // HONEST POSTURE (M1): MySQL has NO native row-level-security engine.
        // Setting `@app_current_*` session user-variables publishes the tenant
        // context, but performs NO in-engine row filtering by itself — rows are
        // only scoped if the OPERATOR installs views/stored-procedures that read
        // those user-vars (e.g. `WHERE tenant_id = @app_current_tenant_id`), or if
        // the broker injects a tenant predicate at compile time. Therefore the
        // session-var SET is `Advisory`, not `Enforced`: the broker records the
        // context for operator-side policy to consume, but does not itself
        // constrain row visibility. This matches `supports_rls: false` for MySQL
        // in the capability matrix. Without operator views OR broker-side tenant
        // predicate injection, tenant isolation on MySQL is application-trust.
        if ctx.is_empty() {
            ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            }
        } else {
            ContextEffect::Advisory {
                recorded_in: "SET @app_current_* session variables (no native MySQL RLS — \
                              operator-installed views / broker tenant-predicate injection \
                              REQUIRED for row isolation)"
                    .into(),
            }
        }
    }
}

impl BackendHealth for MysqlExecutor {
    async fn ping(&self) -> Result<(), String> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

impl QueryExecutor for MysqlExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("MySQL", "query", async {
            let (sql, params) = parse_sql_dispatch(request_json)?;
            validate_read_sql(&sql)?;

            let rows = if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    mysql_internal_status(
                        "query_transaction_start",
                        format!("mysql transaction start failed: {err}"),
                    )
                })?;
                Self::apply_session_vars(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let rows = q.fetch_all(&mut *tx).await.map_err(|e| {
                    mysql_internal_status("query", format!("mysql query failed: {e}"))
                })?;
                tx.commit().await.map_err(|err| {
                    mysql_internal_status(
                        "query_transaction_commit",
                        format!("mysql transaction commit failed: {err}"),
                    )
                })?;
                rows
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                q.fetch_all(&self.pool).await.map_err(|e| {
                    mysql_internal_status("query", format!("mysql query failed: {e}"))
                })?
            };
            let json: Vec<JsonValue> = rows.iter().map(sqlx_row_to_json).collect();
            encode_mysql_response(&JsonValue::Array(json), "query_response_encode")
        })
        .await
    }
}

impl MutationExecutor for MysqlExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("MySQL", "mutate", async {
            let (sql, params) = parse_sql_dispatch(request_json)?;
            validate_mutation_sql(&sql)?;

            if let Some(ctx) = &self.context {
                let mut tx = self.pool.begin().await.map_err(|err| {
                    mysql_internal_status(
                        "mutate_transaction_start",
                        format!("mysql transaction start failed: {err}"),
                    )
                })?;
                Self::apply_session_vars(&mut tx, ctx).await?;
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q.execute(&mut *tx).await.map_err(|e| {
                    mysql_internal_status("mutate", format!("mysql mutate failed: {e}"))
                })?;
                // last_insert_id captured before commit (sqlx zeros it on
                // transaction close otherwise).
                let last_insert_id = result.last_insert_id();
                let rows_affected = result.rows_affected();
                tx.commit().await.map_err(|err| {
                    mysql_internal_status(
                        "mutate_transaction_commit",
                        format!("mysql transaction commit failed: {err}"),
                    )
                })?;
                Ok(serde_json::json!({
                    "rows_affected": rows_affected,
                    "last_insert_id": last_insert_id,
                })
                .to_string())
            } else {
                let q = bind_json_params(sqlx::query(&sql), &params);
                let result = q.execute(&self.pool).await.map_err(|e| {
                    mysql_internal_status("mutate", format!("mysql mutate failed: {e}"))
                })?;
                Ok(serde_json::json!({
                    "rows_affected": result.rows_affected(),
                    "last_insert_id": result.last_insert_id(),
                })
                .to_string())
            }
        })
        .await
    }
}

impl SearchExecutor for MysqlExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "mysql",
            "search",
            "vector_search",
            "UDB_UNSUPPORTED_OPERATION: MySQL backend does not provide native vector search; \
             use `query` with FULLTEXT or route through a vector backend (Qdrant)",
        ))
    }
}

impl ObjectExecutor for MysqlExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "mysql",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: MySQL backend is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "mysql",
            "put_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: MySQL backend is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for MysqlExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let ddl = mysql_create_table_sql(resource_name, spec_json)?;
        sqlx::query(&ddl).execute(&self.pool).await.map_err(|e| {
            mysql_internal_status(
                "ensure_resource",
                format!("mysql ensure_resource failed: {e}"),
            )
        })?;
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let ddl = format!(
            "DROP TABLE IF EXISTS {}",
            quote_mysql_ident(resource_name, "resource_name")?
        );
        sqlx::query(&ddl).execute(&self.pool).await.map_err(|e| {
            mysql_internal_status("drop_resource", format!("mysql drop_resource failed: {e}"))
        })?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = sqlx::query("SHOW TABLES")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                mysql_internal_status("list_resources", format!("SHOW TABLES failed: {e}"))
            })?;
        let names: Vec<String> = rows
            .iter()
            .filter_map(|row| row.try_get::<String, _>(0).ok())
            .collect();
        Ok(names)
    }
}

impl BackendExecutor for MysqlExecutor {
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status> {
        // Minimal transaction support: BEGIN, run the list of
        // statements, COMMIT. The request shape is
        // `{"statements": [{"sql": "...", "params": [...]}, ...]}`.
        let value: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_mysql_tx_json_status)?;
        let stmts = value
            .get("statements")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                mysql_required_field_status(
                    "statements",
                    "missing `statements` array in tx request",
                )
            })?
            .clone();
        for stmt in &stmts {
            let _ = stmt.get("sql").and_then(|v| v.as_str()).ok_or_else(|| {
                mysql_required_field_status("statements.sql", "tx statement missing `sql`")
            })?;
        }
        let mut tx = self.pool.begin().await.map_err(|e| {
            mysql_internal_status("transaction_begin", format!("BEGIN failed: {e}"))
        })?;
        let mut total_affected: u64 = 0;
        for stmt in &stmts {
            let sql = stmt.get("sql").and_then(|v| v.as_str()).ok_or_else(|| {
                mysql_required_field_status("statements.sql", "tx statement missing `sql`")
            })?;
            let params = stmt
                .get("params")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            let q = bind_json_params(sqlx::query(sql), &params);
            let r = q.execute(&mut *tx).await.map_err(|e| {
                mysql_internal_status("transaction_statement", format!("tx statement failed: {e}"))
            })?;
            total_affected += r.rows_affected();
        }
        tx.commit().await.map_err(|e| {
            mysql_internal_status("transaction_commit", format!("COMMIT failed: {e}"))
        })?;
        Ok(serde_json::json!({"rows_affected": total_affected}).to_string())
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("mysql", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use sqlx::mysql::MySqlPoolOptions;

    fn lazy_pool() -> MySqlPool {
        MySqlPoolOptions::new()
            .max_connections(1)
            .connect_lazy("mysql://root:password@localhost:3306/udb_test")
            .expect("lazy mysql pool")
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
        assert_eq!(detail.backend, "mysql");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    #[test]
    fn mysql_internal_status_carries_typed_detail() {
        let status = mysql_internal_status(
            "query_transaction_start",
            "mysql transaction start failed: unavailable",
        );
        assert_internal_detail(
            &status,
            "query_transaction_start",
            "mysql transaction start failed: unavailable",
        );

        let status = mysql_internal_status("transaction_commit", "COMMIT failed: closed");
        assert_internal_detail(&status, "transaction_commit", "COMMIT failed: closed");
    }

    #[test]
    fn mysql_resource_validation_carries_field_violations() {
        let invalid_json = mysql_create_table_sql("items", "{").unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid resource spec:"));
        assert_single_field(&invalid_json, "spec_json");

        let missing_columns = mysql_create_table_sql("items", "{}").unwrap_err();
        assert_eq!(
            missing_columns.message(),
            "table resource spec requires columns"
        );
        assert_single_field(&missing_columns, "columns");

        let empty_columns = mysql_create_table_sql("items", r#"{"columns":[]}"#).unwrap_err();
        assert_eq!(
            empty_columns.message(),
            "table resource spec requires at least one column"
        );
        assert_single_field(&empty_columns, "columns");

        let missing_name =
            mysql_create_table_sql("items", r#"{"columns":[{"type":"TEXT"}]}"#).unwrap_err();
        assert_eq!(missing_name.message(), "column missing name");
        assert_single_field(&missing_name, "columns.name");

        let missing_type =
            mysql_create_table_sql("items", r#"{"columns":[{"name":"name"}]}"#).unwrap_err();
        assert_eq!(missing_type.message(), "column missing type");
        assert_single_field(&missing_type, "columns.type");

        let invalid_type = mysql_create_table_sql(
            "items",
            r#"{"columns":[{"name":"name","type":"TEXT;DROP"}]}"#,
        )
        .unwrap_err();
        assert_eq!(invalid_type.message(), "invalid SQL type for column 'name'");
        assert_single_field(&invalid_type, "columns.type");

        let invalid_engine = mysql_create_table_sql(
            "items",
            r#"{"engine":"bad-engine","columns":[{"name":"name","type":"TEXT"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            invalid_engine.message(),
            "invalid MySQL identifier 'bad-engine'"
        );
        assert_single_field(&invalid_engine, "engine");

        let invalid_identifier =
            mysql_create_table_sql("bad-name", r#"{"columns":[{"name":"name","type":"TEXT"}]}"#)
                .unwrap_err();
        assert_eq!(
            invalid_identifier.message(),
            "invalid MySQL identifier 'bad-name'"
        );
        assert_single_field(&invalid_identifier, "resource_name");
    }

    #[tokio::test]
    async fn mysql_transaction_validation_carries_field_violations() {
        let exec = MysqlExecutor::with_pool(lazy_pool());

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
}
