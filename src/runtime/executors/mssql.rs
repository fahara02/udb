//! Microsoft SQL Server executor (C9).
//!
//! Real TDS-protocol implementation via the canonical `tiberius`
//! driver. Tiberius is async-native via the `tokio-util` `compat`
//! shim. The executor holds a `Mutex<Option<Client>>` — single
//! connection per executor instance, lazily initialised on first
//! use, reconnected on transport errors. For production deployments
//! requiring concurrent throughput, operators front the broker with
//! a connection pool (deadpool-tiberius / bb8-tiberius) at the
//! orchestration layer; this executor stays simple + correct.
//!
//! ## Dispatch contract
//!
//! Tiberius accepts the SQL the Mssql compiler emits directly: named
//! `@P1` / `@P2` / `…` parameters bound from a `&[&dyn ToSql]` slice.
//! The dispatch JSON shape mirrors the Postgres / MySQL executors:
//!
//! ```json
//! { "sql": "SELECT * FROM [t] WHERE [id] = @P1",
//!   "params": ["abc"] }
//! ```
//!
//! ## What this does NOT do (yet)
//!
//! - **Pooling** — single connection per executor; auto-reconnects
//!   on `tiberius::error::Error`. For multi-tenant production
//!   deployments, wrap with `deadpool-tiberius` at the broker
//!   orchestration layer.
//! - **AAD / Managed Identity auth** — tiberius supports it via the
//!   `AuthMethod::AADToken` variant; not wired here (operators
//!   typically set the token externally and pass via ADO string).
//! - **MARS** — Multiple Active Result Sets isn't enabled; each
//!   query consumes the connection until completion.

use std::sync::Arc;

use serde_json::Value as JsonValue;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::broker::RequestContext;
use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, SqlDialect, render_sql_session_settings,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

type TiberiusClient = Client<Compat<TcpStream>>;

/// Holds the ADO connection string and a lazily-initialised tiberius
/// client. The client is wrapped in an async mutex so concurrent
/// `query`/`mutate` calls serialise on the single connection (which
/// is what tiberius requires anyway — no MARS).
#[derive(Clone)]
pub struct MssqlClient {
    ado_string: Arc<String>,
    inner: Arc<Mutex<Option<TiberiusClient>>>,
}

impl std::fmt::Debug for MssqlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlClient")
            .field("ado_string", &"<redacted>")
            .finish()
    }
}

impl MssqlClient {
    /// Construct without opening a connection. The first call to
    /// `with_client` triggers the initial TCP + TDS handshake.
    pub fn new(ado_string: impl Into<String>) -> Self {
        Self {
            ado_string: Arc::new(ado_string.into()),
            inner: Arc::new(Mutex::new(None)),
        }
    }

    /// Open a fresh tiberius client from the configured ADO string.
    /// Pulled out so the auto-reconnect path can call it.
    async fn open(&self) -> Result<TiberiusClient, String> {
        let config = Config::from_ado_string(&self.ado_string)
            .map_err(|e| format!("tiberius: bad ADO connection string: {e}"))?;
        // Some Azure SQL deployments need explicit trust toggles;
        // operators control that through the ADO string itself.
        let _ = AuthMethod::None; // placeholder for future AAD wiring
        let addr = config.get_addr();
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| format!("tiberius: TCP connect failed: {e}"))?;
        tcp.set_nodelay(true)
            .map_err(|e| format!("tiberius: set_nodelay failed: {e}"))?;
        Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| format!("tiberius: TDS handshake failed: {e}"))
    }

    /// Run a closure against the live client. Handles lazy init +
    /// auto-reconnect: if the client returns a transport error, the
    /// connection is dropped and a fresh one is opened for the retry.
    async fn with_client<F, R>(&self, f: F) -> Result<R, String>
    where
        F: AsyncFnOnce(&mut TiberiusClient) -> Result<R, tiberius::error::Error>,
    {
        let mut guard = self.inner.lock().await;
        // Ensure a live client.
        if guard.is_none() {
            *guard = Some(self.open().await?);
        }
        let client = guard.as_mut().expect("client just initialised");
        match f(client).await {
            Ok(v) => Ok(v),
            Err(err) => {
                // Drop the connection on any error so the next call
                // reconnects. This is conservative but correct;
                // tiberius doesn't surface a typed "is this a
                // transient connection error" predicate.
                *guard = None;
                Err(format!("tiberius: {err}"))
            }
        }
    }

    /// Healthcheck: open the connection (lazy first time, cached
    /// after) and issue a `SELECT 1`.
    pub async fn ping(&self) -> Result<(), String> {
        self.with_client(async |client| client.simple_query("SELECT 1").await.map(|_| ()))
            .await
    }
}

/// Generic-dispatch executor wrapping an `MssqlClient`.
///
/// A3 (2026-05-30): when constructed with `with_context`, every
/// `query`/`mutate`/`transaction` call prepends
/// `EXEC sp_set_session_context` statements that set
/// `app.current_tenant_id`, `app.current_project_id`, …
/// `@read_only = 1` so the values can't be rewritten mid-request.
/// Operator-installed T-SQL RLS policies read those values via
/// `SESSION_CONTEXT(N'app_current_tenant_id')` to filter rows.
#[derive(Debug, Clone)]
pub struct MssqlExecutor {
    client: MssqlClient,
    context: Option<Arc<RequestContext>>,
}

impl MssqlExecutor {
    pub fn new(client: MssqlClient) -> Self {
        Self {
            client,
            context: None,
        }
    }

    /// A3: context-bound constructor. Every dispatched call runs
    /// after `EXEC sp_set_session_context` has stamped the
    /// per-request tenant/project values.
    pub fn with_context(client: MssqlClient, context: Arc<RequestContext>) -> Self {
        Self {
            client,
            context: Some(context),
        }
    }

    /// Build the `EXEC sp_set_session_context` statements for the
    /// bound context, joined into a single T-SQL batch. Returns
    /// `None` when no context is bound or the context is empty.
    fn session_context_batch(&self) -> Option<String> {
        let ctx = self.context.as_ref()?;
        let applied = AppliedContext::from_request(ctx);
        let stmts = render_sql_session_settings(&applied, SqlDialect::Mssql);
        if stmts.is_empty() {
            None
        } else {
            // Single T-SQL batch — tiberius `simple_query` accepts
            // multi-statement scripts separated by `;`.
            Some(stmts.join("; "))
        }
    }

    /// A3: send the `sp_set_session_context` batch on the live
    /// connection. Called before every user query/mutate when
    /// context is bound. Re-issuing on every call is cheap (one
    /// round-trip) and guarantees correctness across reconnects:
    /// if `with_client` had to open a fresh connection, the prior
    /// session state is gone, and we restore it here.
    async fn apply_session_context(
        client: &mut TiberiusClient,
        batch: &str,
    ) -> Result<(), tiberius::error::Error> {
        client.simple_query(batch).await.map(|_| ())
    }
}

impl BackendContextEnforcer for MssqlExecutor {
    fn backend_label(&self) -> &str {
        "mssql"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        // A3 (2026-05-30): when the executor was constructed via
        // `with_context`, every dispatched call now prepends
        // `EXEC sp_set_session_context` so RLS policies that read
        // `SESSION_CONTEXT(N'app_current_tenant_id')` are
        // automatically scoped to the request. The actual SET
        // happens in `apply_session_context`; we report `Enforced`
        // when the executor is wired with context (the runtime
        // assembles us via `with_context` on the request path).
        if self.context.is_some() {
            ContextEffect::Enforced {
                mechanism: "EXEC sp_set_session_context (T-SQL SESSION_CONTEXT)".into(),
            }
        } else {
            // Reachable from probes / non-request paths; honest
            // about the mode.
            ContextEffect::Advisory {
                recorded_in: "T-SQL SESSION_CONTEXT (no request context bound)".into(),
            }
        }
    }
}

impl BackendHealth for MssqlExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

fn parse_dispatch(request_json: &str) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    let v: JsonValue = serde_json::from_str(request_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let sql = v
        .get("sql")
        .and_then(|x| x.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `sql` in dispatch request"))?
        .to_string();
    let params = v
        .get("params")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    Ok((sql, params))
}

/// Convert a `tiberius::Row` into a `serde_json::Value` object. Each
/// column becomes a key; types are coerced via tiberius's `Row::get`
/// trait machinery.
fn row_to_json(row: &tiberius::Row) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (idx, col) in row.columns().iter().enumerate() {
        let name = col.name().to_string();
        // Try common types in order. Tiberius's Row::get returns
        // Option<T> when the value might be NULL; we surface NULL as
        // JSON null.
        let value: JsonValue = if let Ok(v) = row.try_get::<&str, _>(idx) {
            v.map(|s| JsonValue::String(s.to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<i64, _>(idx) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<i32, _>(idx) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<f64, _>(idx) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<bool, _>(idx) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<&[u8], _>(idx) {
            v.map(|bytes| {
                use base64::Engine as _;
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

/// Bind a JSON value to tiberius's `ToSql` machinery. tiberius's
/// `query()` takes `&[&dyn ToSql]`, and the values must outlive the
/// borrow. We materialise typed scalars in a local `Vec` and hand
/// references to query().
enum SqlParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl SqlParam {
    fn from_json(v: &JsonValue) -> Self {
        match v {
            JsonValue::Null => Self::Null,
            JsonValue::Bool(b) => Self::Bool(*b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Self::Float(f)
                } else {
                    Self::Str(n.to_string())
                }
            }
            JsonValue::String(s) => Self::Str(s.clone()),
            other => Self::Str(other.to_string()),
        }
    }
}

fn bind_refs<'a>(params: &'a [SqlParam]) -> Vec<&'a dyn tiberius::ToSql> {
    params
        .iter()
        .map(|p| -> &dyn tiberius::ToSql {
            match p {
                SqlParam::Null => &Option::<&str>::None,
                SqlParam::Bool(b) => b,
                SqlParam::Int(i) => i,
                SqlParam::Float(f) => f,
                SqlParam::Str(s) => s,
            }
        })
        .collect()
}

impl QueryExecutor for MssqlExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (sql, params_json) = parse_dispatch(request_json)?;
        let params: Vec<SqlParam> = params_json.iter().map(SqlParam::from_json).collect();
        // A3: pre-render the session-context batch (if any) so the
        // hot path inside `with_client` only does I/O.
        let ctx_batch = self.session_context_batch();
        let rows = self
            .client
            .with_client(async |client| {
                if let Some(ref batch) = ctx_batch {
                    Self::apply_session_context(client, batch).await?;
                }
                let refs = bind_refs(&params);
                let stream = client.query(&sql, &refs).await?;
                let rows = stream.into_first_result().await?;
                Ok::<_, tiberius::error::Error>(rows)
            })
            .await
            .map_err(|e| tonic::Status::internal(format!("mssql query failed: {e}")))?;
        let json: Vec<JsonValue> = rows.iter().map(row_to_json).collect();
        serde_json::to_string(&JsonValue::Array(json))
            .map_err(|e| tonic::Status::internal(format!("mssql response serialise failed: {e}")))
    }
}

impl MutationExecutor for MssqlExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (sql, params_json) = parse_dispatch(request_json)?;
        let params: Vec<SqlParam> = params_json.iter().map(SqlParam::from_json).collect();
        let ctx_batch = self.session_context_batch();
        let result = self
            .client
            .with_client(async |client| {
                if let Some(ref batch) = ctx_batch {
                    Self::apply_session_context(client, batch).await?;
                }
                let refs = bind_refs(&params);
                let result = client.execute(&sql, &refs).await?;
                Ok::<_, tiberius::error::Error>(result.rows_affected().iter().sum::<u64>())
            })
            .await
            .map_err(|e| tonic::Status::internal(format!("mssql mutate failed: {e}")))?;
        Ok(serde_json::json!({ "rows_affected": result }).to_string())
    }
}

impl SearchExecutor for MssqlExecutor {
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status> {
        // T-SQL CONTAINS() / FREETEXT() compiles to a SELECT that runs
        // through the standard query path.
        self.query(request_json).await
    }
}

impl ObjectExecutor for MssqlExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: SQL Server is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: SQL Server is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for MssqlExecutor {
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        // The Mssql compiler's compile_resource_op produces a
        // ready-to-run T-SQL statement; ensure_resource just runs it.
        let _ = spec_json; // shape: { "sql": "..." } already executed by compile path
        Err(tonic::Status::unimplemented(
            "MssqlExecutor::ensure_resource is driven via compile_resource_op + mutate",
        ))
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(tonic::Status::unimplemented(
            "MssqlExecutor::drop_resource is driven via compile_resource_op + mutate",
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        // Catalog query — sys.tables is the canonical source.
        let req = serde_json::json!({
            "sql": "SELECT name FROM sys.tables ORDER BY name",
            "params": []
        });
        let body = self.query(&req.to_string()).await?;
        let parsed: JsonValue = serde_json::from_str(&body)
            .map_err(|e| tonic::Status::internal(format!("list_resources parse: {e}")))?;
        let mut out = Vec::new();
        if let JsonValue::Array(rows) = parsed {
            for row in rows {
                if let Some(name) = row.get("name").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for MssqlExecutor {
    async fn transaction(&self, request_json: &str) -> Result<String, tonic::Status> {
        // T-SQL BEGIN TRAN ... COMMIT lifecycle. Single-statement
        // "transaction" support; multi-statement TX needs the saga
        // path. The dispatch JSON carries the SQL the caller wants
        // wrapped — execute inside BEGIN/COMMIT.
        let (sql, params_json) = parse_dispatch(request_json)?;
        let params: Vec<SqlParam> = params_json.iter().map(SqlParam::from_json).collect();
        let ctx_batch = self.session_context_batch();
        let result = self
            .client
            .with_client(async |client| {
                client.simple_query("BEGIN TRANSACTION").await?;
                // A3: apply session context INSIDE the transaction
                // so RLS predicates that reference SESSION_CONTEXT
                // see the request's tenant scoping.
                if let Some(ref batch) = ctx_batch {
                    Self::apply_session_context(client, batch).await?;
                }
                let refs = bind_refs(&params);
                let exec_result = client.execute(&sql, &refs).await;
                match exec_result {
                    Ok(r) => {
                        client.simple_query("COMMIT TRANSACTION").await?;
                        Ok::<u64, tiberius::error::Error>(r.rows_affected().iter().sum())
                    }
                    Err(err) => {
                        let _ = client.simple_query("ROLLBACK TRANSACTION").await;
                        Err(err)
                    }
                }
            })
            .await
            .map_err(|e| tonic::Status::internal(format!("mssql transaction failed: {e}")))?;
        Ok(serde_json::json!({ "rows_affected": result }).to_string())
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "mssql".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "mssql".to_string(),
                instance: None,
                ok: false,
                error: Some(err),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_dispatch_extracts_sql_and_params() {
        let req = r#"{"sql":"SELECT * FROM [t] WHERE [id] = @P1","params":["abc"]}"#;
        let (sql, params) = parse_dispatch(req).unwrap();
        assert_eq!(sql, "SELECT * FROM [t] WHERE [id] = @P1");
        assert_eq!(params, vec![json!("abc")]);
    }

    #[test]
    fn parse_dispatch_defaults_params_to_empty() {
        let req = r#"{"sql":"SELECT 1"}"#;
        let (_, params) = parse_dispatch(req).unwrap();
        assert!(params.is_empty());
    }

    #[test]
    fn parse_dispatch_rejects_missing_sql() {
        let req = r#"{"params":[]}"#;
        let err = parse_dispatch(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn sql_param_from_json_handles_every_scalar() {
        assert!(matches!(SqlParam::from_json(&json!(null)), SqlParam::Null));
        assert!(matches!(
            SqlParam::from_json(&json!(true)),
            SqlParam::Bool(true)
        ));
        assert!(matches!(SqlParam::from_json(&json!(42)), SqlParam::Int(42)));
        assert!(matches!(
            SqlParam::from_json(&json!(2.5)),
            SqlParam::Float(_)
        ));
        match SqlParam::from_json(&json!("hello")) {
            SqlParam::Str(s) => assert_eq!(s, "hello"),
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn mssql_client_new_does_not_connect() {
        // Constructing a client must not open a TCP connection — the
        // connection is opened lazily on first `with_client` call.
        // This lets tests construct executors without a live SQL
        // Server and lets the runtime skip dead instances at startup
        // without crashing.
        let client = MssqlClient::new("Server=localhost,1433;User=sa;Password=x;");
        // No assertions needed — the constructor returning means
        // no TCP attempt happened. Sanity: the Debug impl redacts.
        assert!(format!("{client:?}").contains("<redacted>"));
    }

    #[test]
    fn enforce_without_context_reports_advisory() {
        // A3 (2026-05-30): the plain `new` constructor isn't wired
        // to a request context, so even with an applied tenant_id
        // the executor reports Advisory — honest about the actual
        // mode the connection is in.
        let exec = MssqlExecutor::new(MssqlClient::new("Server=x;"));
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        match exec.enforce(&ctx) {
            ContextEffect::Advisory { recorded_in } => {
                assert!(recorded_in.contains("SESSION_CONTEXT"));
            }
            other => panic!("expected Advisory, got {other:?}"),
        }
    }

    #[test]
    fn a3_enforce_with_context_reports_enforced() {
        // A3: when constructed with `with_context`, the executor
        // now stamps `sp_set_session_context` on every dispatched
        // call — that is row-level enforcement at the
        // protocol layer (the RLS policy reads SESSION_CONTEXT).
        let req = Arc::new(crate::broker::RequestContext {
            tenant_id: "acme".into(),
            project_id: "p1".into(),
            ..Default::default()
        });
        let exec = MssqlExecutor::with_context(MssqlClient::new("Server=x;"), req);
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        match exec.enforce(&ctx) {
            ContextEffect::Enforced { mechanism } => {
                assert!(mechanism.contains("sp_set_session_context"));
            }
            other => panic!("expected Enforced, got {other:?}"),
        }
    }

    #[test]
    fn a3_session_context_batch_renders_when_context_bound() {
        let req = Arc::new(crate::broker::RequestContext {
            tenant_id: "acme".into(),
            project_id: "p1".into(),
            ..Default::default()
        });
        let exec = MssqlExecutor::with_context(MssqlClient::new("Server=x;"), req);
        let batch = exec.session_context_batch().expect("batch");
        assert!(batch.contains("EXEC sp_set_session_context"));
        assert!(batch.contains("@key = N'app_current_tenant_id'"));
        assert!(batch.contains("@value = N'acme'"));
        assert!(batch.contains("@key = N'app_current_project_id'"));
        // Multiple statements joined into a single batch.
        assert!(batch.contains("; "));
    }

    #[test]
    fn a3_session_context_batch_is_none_without_context() {
        let exec = MssqlExecutor::new(MssqlClient::new("Server=x;"));
        assert!(exec.session_context_batch().is_none());
    }
}
