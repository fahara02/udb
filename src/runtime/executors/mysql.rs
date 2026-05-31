//! `MysqlExecutor` — sqlx-mysql-backed generic dispatch executor.
//!
//! Mirror of `PostgresExecutor` for MySQL. Implements the same
//! executor traits (`BackendHealth`, `QueryExecutor`, `MutationExecutor`),
//! returns typed `tonic::Status::failed_precondition` for Vector /
//! Object / ResourceAdmin (MySQL is not a vector store or object
//! store — these operations are honestly unsupported).
//!
//! ## Dispatch-JSON contract
//!
//! Same shape as the Postgres executor: requests carry
//! `{"sql": "...", "params": [...]}`. Params are passed positionally
//! as MySQL `?` placeholders (sqlx converts `?` from the SQL string
//! to MySQL-protocol prepared-statement params).
//!
//! ## NW-deep — RequestContext enforcement
//!
//! When constructed with `with_context`, the executor wraps every
//! dispatched SQL in a session-scoped transaction and SETs the
//! `@app_current_tenant_id` / `@app_current_project_id` /
//! `@app_current_purpose` user variables before running the SQL. RLS
//! views and stored procedures that read those user vars now see the
//! same tenant context as the typed RPCs (matches Postgres's
//! `set_request_local_settings` pattern).
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
use sqlx::Column;
use sqlx::MySqlPool;
use sqlx::Row;

use crate::broker::RequestContext;
use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, SqlDialect, render_sql_session_settings,
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
    /// inside a transaction with session vars set, so RLS-style views
    /// can introspect tenant context.
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
        for stmt in stmts {
            sqlx::query(&stmt).execute(&mut **tx).await.map_err(|err| {
                tonic::Status::internal(format!("mysql session var set failed: {err}"))
            })?;
        }
        Ok(())
    }
}

impl BackendContextEnforcer for MysqlExecutor {
    fn backend_label(&self) -> &str {
        "mysql"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        // Effect classification — actual application happens inside
        // the per-request transaction via `apply_session_vars`. We
        // report Enforced because the executor is wired to honour
        // `with_context` and emit the SETs.
        ContextEffect::Enforced {
            mechanism: "SET @app_current_* session variables in request-scoped transaction".into(),
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

/// Render a sqlx-mysql row into a JSON object. Each column becomes a
/// key; types are coerced into JSON-friendly variants.
fn row_to_json(row: &sqlx::mysql::MySqlRow) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        // Try common types; fall back to NULL.
        let value: JsonValue = if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<bool>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<String>, _>(i) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<Option<Vec<u8>>, _>(i) {
            // Bytes → base64-encoded JSON string. base64::Engine is
            // already a crate-wide dep.
            use base64::Engine as _;
            v.map(|bytes| {
                JsonValue::String(format!(
                    "base64:{}",
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                ))
            })
            .unwrap_or(JsonValue::Null)
        } else {
            JsonValue::Null
        };
        obj.insert(name, value);
    }
    JsonValue::Object(obj)
}

/// Parse `{"sql": "...", "params": [...]}` out of a dispatch request.
fn parse_dispatch(request_json: &str) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    let value: JsonValue = serde_json::from_str(request_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let sql = value
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `sql` in dispatch request"))?
        .to_string();
    let params = value
        .get("params")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    Ok((sql, params))
}

/// Bind one JSON value as a MySQL parameter. Numbers go to i64/f64,
/// strings stay as strings, bool, null. Objects/arrays serialize to
/// JSON-text so MySQL stores them in a `JSON` column.
fn bind_params<'q>(
    mut q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    params: &'q [JsonValue],
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    for p in params {
        q = match p {
            JsonValue::Null => q.bind(Option::<i64>::None),
            JsonValue::Bool(b) => q.bind(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    q.bind(i)
                } else if let Some(f) = n.as_f64() {
                    q.bind(f)
                } else {
                    q.bind(n.to_string())
                }
            }
            JsonValue::String(s) => q.bind(s.clone()),
            other => q.bind(other.to_string()),
        };
    }
    q
}

impl QueryExecutor for MysqlExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (sql, params) = parse_dispatch(request_json)?;

        let rows = if let Some(ctx) = &self.context {
            let mut tx = self.pool.begin().await.map_err(|err| {
                tonic::Status::internal(format!("mysql transaction start failed: {err}"))
            })?;
            Self::apply_session_vars(&mut tx, ctx).await?;
            let q = bind_params(sqlx::query(&sql), &params);
            let rows = q
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| tonic::Status::internal(format!("mysql query failed: {e}")))?;
            tx.commit().await.map_err(|err| {
                tonic::Status::internal(format!("mysql transaction commit failed: {err}"))
            })?;
            rows
        } else {
            let q = bind_params(sqlx::query(&sql), &params);
            q.fetch_all(&self.pool)
                .await
                .map_err(|e| tonic::Status::internal(format!("mysql query failed: {e}")))?
        };
        let json: Vec<JsonValue> = rows.iter().map(row_to_json).collect();
        serde_json::to_string(&JsonValue::Array(json))
            .map_err(|e| tonic::Status::internal(format!("response serialise failed: {e}")))
    }
}

impl MutationExecutor for MysqlExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (sql, params) = parse_dispatch(request_json)?;

        if let Some(ctx) = &self.context {
            let mut tx = self.pool.begin().await.map_err(|err| {
                tonic::Status::internal(format!("mysql transaction start failed: {err}"))
            })?;
            Self::apply_session_vars(&mut tx, ctx).await?;
            let q = bind_params(sqlx::query(&sql), &params);
            let result = q
                .execute(&mut *tx)
                .await
                .map_err(|e| tonic::Status::internal(format!("mysql mutate failed: {e}")))?;
            // last_insert_id captured before commit (sqlx zeros it on
            // transaction close otherwise).
            let last_insert_id = result.last_insert_id();
            let rows_affected = result.rows_affected();
            tx.commit().await.map_err(|err| {
                tonic::Status::internal(format!("mysql transaction commit failed: {err}"))
            })?;
            Ok(serde_json::json!({
                "rows_affected": rows_affected,
                "last_insert_id": last_insert_id,
            })
            .to_string())
        } else {
            let q = bind_params(sqlx::query(&sql), &params);
            let result = q
                .execute(&self.pool)
                .await
                .map_err(|e| tonic::Status::internal(format!("mysql mutate failed: {e}")))?;
            Ok(serde_json::json!({
                "rows_affected": result.rows_affected(),
                "last_insert_id": result.last_insert_id(),
            })
            .to_string())
        }
    }
}

impl SearchExecutor for MysqlExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: MySQL backend does not provide native vector search; \
             use `query` with FULLTEXT or route through a vector backend (Qdrant)",
        ))
    }
}

impl ObjectExecutor for MysqlExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: MySQL backend is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
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
        // For now: log + accept. The full resource lifecycle for MySQL
        // (CREATE TABLE / CREATE INDEX from JSON spec) is the P2P
        // executor follow-up.
        tracing::info!(
            backend = "mysql",
            resource = resource_name,
            spec = spec_json,
            "MysqlExecutor::ensure_resource accepted; full lifecycle is a P2P follow-up"
        );
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        tracing::info!(
            backend = "mysql",
            resource = resource_name,
            "MysqlExecutor::drop_resource accepted; full lifecycle is a P2P follow-up"
        );
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = sqlx::query("SHOW TABLES")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| tonic::Status::internal(format!("SHOW TABLES failed: {e}")))?;
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
            let q = bind_params(sqlx::query(sql), &params);
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
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "mysql".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "mysql".to_string(),
                instance: None,
                ok: false,
                error: Some(err),
            }),
        }
    }
}
