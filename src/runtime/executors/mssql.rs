//! Microsoft SQL Server executor (C9).
//!
//! Real TDS-protocol implementation via the canonical `tiberius`
//! driver. Tiberius is async-native via the `tokio-util` `compat`
//! shim. The executor holds a small round-robin pool of lazily
//! initialised clients. Each client still serialises its own I/O,
//! which is what tiberius requires, but unrelated requests no longer
//! queue behind one global mutex.
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
//! - **AAD / Managed Identity auth** - tiberius supports it via the
//!   `AuthMethod::AADToken` variant; not wired here (operators
//!   typically set the token externally and pass via ADO string).
//! - **MARS** — Multiple Active Result Sets isn't enabled; each
//!   query consumes the connection until completion.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use serde_json::Value as JsonValue;
use tiberius::{AuthMethod, Client, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::broker::RequestContext;
use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, SqlDialect, render_sql_session_settings,
};
use crate::runtime::core::{validate_mutation_sql, validate_read_sql};
use crate::runtime::executor_utils::{
    base64_cell, build_probe, capability_status, parse_sql_dispatch, with_executor_timeout,
};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

type TiberiusClient = Client<Compat<TcpStream>>;
type TiberiusSlot = Arc<Mutex<Option<TiberiusClient>>>;

const DEFAULT_MSSQL_POOL_SIZE: usize = 4;
const MAX_MSSQL_POOL_SIZE: usize = 64;

/// Holds the ADO connection string and a lazily-initialised tiberius
/// client pool. Each slot is wrapped in an async mutex; the pool
/// spreads requests across slots so one long-running query does not
/// block every other SQL Server operation.
#[derive(Clone)]
pub struct MssqlClient {
    ado_string: Arc<String>,
    slots: Arc<Vec<TiberiusSlot>>,
    next_slot: Arc<AtomicUsize>,
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
        let pool_size = std::env::var("UDB_MSSQL_POOL_SIZE")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MSSQL_POOL_SIZE)
            .clamp(1, MAX_MSSQL_POOL_SIZE);
        let slots = (0..pool_size)
            .map(|_| Arc::new(Mutex::new(None)))
            .collect::<Vec<_>>();
        Self {
            ado_string: Arc::new(ado_string.into()),
            slots: Arc::new(slots),
            next_slot: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn next_slot(&self) -> TiberiusSlot {
        let idx = self.next_slot.fetch_add(1, Ordering::Relaxed) % self.slots.len();
        Arc::clone(&self.slots[idx])
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
        let slot = self.next_slot();
        let mut guard = slot.lock().await;
        // Ensure a live client.
        if guard.is_none() {
            *guard = Some(self.open().await?);
        }
        let client = guard.as_mut().expect("client just initialised");
        match f(client).await {
            Ok(v) => Ok(v),
            Err(err) => {
                if is_transport_error(&err) {
                    *guard = None;
                }
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

    /// Ensure the target database named by the ADO string exists. SQL Server
    /// refuses the login handshake when `Database=...` points at a missing DB,
    /// so bootstrap must connect to `master` first and create the configured DB
    /// before the normal lazy client is opened.
    pub(crate) async fn ensure_database_exists(&self) -> Result<(), String> {
        let Some(database) = ado_database_name(&self.ado_string) else {
            return Ok(());
        };
        if database.eq_ignore_ascii_case("master") {
            return Ok(());
        }
        let master_ado = ado_with_database(&self.ado_string, "master");
        let master = MssqlClient::new(master_ado);
        let database_literal = mssql_string_literal(&database);
        let database_ident = mssql_database_identifier(&database)?;
        let sql = format!(
            "IF DB_ID(N{database_literal}) IS NULL BEGIN CREATE DATABASE {database_ident}; END"
        );
        master
            .simple_batch(&sql)
            .await
            .map_err(|err| format!("ensure SQL Server database '{database}' failed: {err}"))
    }

    /// B.8 — run a parameterised query and return the first result
    /// set's rows. Thin wrapper over [`Self::with_client`] so the
    /// canonical store (which lives outside this module and cannot
    /// reach the private `with_client`) can issue `SELECT`s with bound
    /// `@P1`/`@P2`/… parameters. `params` are materialised typed
    /// scalars (see [`SqlParam`]); references are bound via
    /// [`bind_refs`].
    pub(crate) async fn fetch_rows(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<Vec<tiberius::Row>, String> {
        self.with_client(async |client| {
            let refs = bind_refs(params);
            let stream = client.query(sql, &refs).await?;
            let rows = stream.into_first_result().await?;
            Ok::<_, tiberius::error::Error>(rows)
        })
        .await
    }

    /// B.8 — run a parameterised write and return the total rows
    /// affected across all result sets. Thin wrapper over
    /// [`Self::with_client`].
    pub(crate) async fn execute_sql(&self, sql: &str, params: &[SqlParam]) -> Result<u64, String> {
        self.with_client(async |client| {
            let refs = bind_refs(params);
            let result = client.execute(sql, &refs).await?;
            Ok::<_, tiberius::error::Error>(result.rows_affected().iter().sum::<u64>())
        })
        .await
    }

    /// B.8 — run a (possibly multi-statement) T-SQL batch with no
    /// parameters. For DDL / transaction control where tiberius's
    /// `simple_query` is the right primitive (it accepts `;`-separated
    /// scripts). Thin wrapper over [`Self::with_client`].
    pub(crate) async fn simple_batch(&self, sql: &str) -> Result<(), String> {
        self.with_client(async |client| client.simple_query(sql).await.map(|_| ()))
            .await
    }

    /// B.8 phase 2 — run a closure against a SINGLE pinned tiberius
    /// connection for the whole call. The admin-audit chain append needs
    /// `BEGIN TRAN` + `sp_getapplock` (`@LockOwner='Transaction'`) + read
    /// latest hash + INSERT + `COMMIT` to all execute on ONE connection
    /// inside ONE transaction — otherwise the applock serialises nothing
    /// (each `fetch_rows`/`execute_sql` would grab a different pooled
    /// slot) and two concurrent appends could link off the same
    /// `previous_hash`. This exposes the same lazy-init + auto-reconnect
    /// `with_client` machinery to sibling canonical-store modules so they
    /// can pin a connection for a multi-statement critical section. The
    /// closure receives `&mut TiberiusClient` and may freely
    /// `simple_query` / `query` / `execute` on it; transport errors drop
    /// the connection for the next caller exactly like the other helpers.
    pub(crate) async fn with_pinned_client<F, R>(&self, f: F) -> Result<R, String>
    where
        F: AsyncFnOnce(&mut TiberiusClient) -> Result<R, tiberius::error::Error>,
    {
        self.with_client(f).await
    }

    /// B.8 phase 2 — bind `SqlParam` references for a pinned-connection
    /// query. Sibling modules driving [`Self::with_pinned_client`] hold a
    /// raw `&mut TiberiusClient`, so they need the same `&[&dyn ToSql]`
    /// materialisation `fetch_rows` does internally. Re-exported here so
    /// they don't reimplement [`bind_refs`].
    pub(crate) fn bind_param_refs<'a>(params: &'a [SqlParam]) -> Vec<&'a dyn tiberius::ToSql> {
        bind_refs(params)
    }
}

fn is_transport_error(err: &tiberius::error::Error) -> bool {
    let text = err.to_string().to_ascii_lowercase();
    text.contains("io error")
        || text.contains("connection")
        || text.contains("connection reset")
        || text.contains("connection refused")
        || text.contains("connection aborted")
        || text.contains("broken pipe")
        || text.contains("timed out")
        || text.contains("timeout")
        || text.contains("transport")
        || text.contains("tls")
}

fn ado_database_name(ado: &str) -> Option<String> {
    ado.split(';').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        let key = key.trim();
        if key.eq_ignore_ascii_case("database") || key.eq_ignore_ascii_case("initial catalog") {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
        None
    })
}

fn ado_with_database(ado: &str, database: &str) -> String {
    let mut replaced = false;
    let parts = ado
        .split(';')
        .filter(|part| !part.trim().is_empty())
        .map(|part| {
            let Some((key, _)) = part.split_once('=') else {
                return part.to_string();
            };
            let key = key.trim();
            if key.eq_ignore_ascii_case("database") || key.eq_ignore_ascii_case("initial catalog") {
                replaced = true;
                format!("{key}={database}")
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>();
    if replaced {
        format!("{};", parts.join(";"))
    } else {
        let mut out = ado.trim_end_matches(';').to_string();
        if !out.is_empty() {
            out.push(';');
        }
        out.push_str("Database=");
        out.push_str(database);
        out.push(';');
        out
    }
}

fn mssql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn mssql_database_identifier(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("SQL Server database name is empty".to_string());
    }
    if value.chars().any(|ch| ch.is_control() || ch == ';') {
        return Err("SQL Server database name contains an unsafe character".to_string());
    }
    Ok(format!("[{}]", value.replace(']', "]]")))
}

/// Generic-dispatch executor wrapping an `MssqlClient`.
///
/// A3 (2026-05-30): when constructed with `with_context`, every
/// `query`/`mutate`/`transaction` call prepends
/// `EXEC sp_set_session_context` statements that set
/// `app.current_tenant_id`, `app.current_project_id`, …
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
        // A3 (2026-05-30): when the executor was constructed via `with_context`,
        // every dispatched call prepends `EXEC sp_set_session_context` so RLS
        // policies reading `SESSION_CONTEXT(N'app_current_tenant_id')` are
        // scoped to the request (the actual SET happens in
        // `apply_session_context`). On that request-wired path the posture is
        // exactly the shared `is_empty → Advisory else Enforced`, so route it
        // through `enforce_with_mechanism` (#22). Off that path (probes /
        // `new`), stay honest about the unbound mode.
        if self.context.is_some() {
            crate::runtime::backend_context::enforce_with_mechanism(
                ctx,
                "EXEC sp_set_session_context (T-SQL SESSION_CONTEXT)",
            )
        } else if ctx.is_empty() {
            ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            }
        } else {
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
        } else if let Ok(v) = row.try_get::<f32, _>(idx) {
            // W4: SQL Server REAL is a distinct 32-bit type; tiberius decodes it
            // only as f32 (a lone f64 probe fails → NULL). Widen to f64 for JSON.
            v.map(|f| JsonValue::from(f64::from(f)))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<bool, _>(idx) {
            v.map(JsonValue::from).unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<uuid::Uuid, _>(idx) {
            // W4: UNIQUEIDENTIFIER decodes only as Uuid; without this branch every
            // uuid column read back NULL (silent data loss).
            v.map(|u| JsonValue::String(u.to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(idx) {
            // W4: DATETIME/DATETIME2/SMALLDATETIME decode as chrono types (tiberius
            // `chrono` feature); without this every datetime column read back NULL.
            v.map(|dt| JsonValue::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<chrono::NaiveDate, _>(idx) {
            // W4 residual (found by the served round-trip test): a bare DATE column
            // is tiberius `NaiveDate`, which the NaiveDateTime probe above does NOT
            // match — without this branch a DATE read back NULL.
            v.map(|d| JsonValue::String(d.format("%Y-%m-%d").to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<chrono::NaiveTime, _>(idx) {
            // W4 residual: a bare TIME column is tiberius `NaiveTime`, same rationale
            // — otherwise it read back NULL.
            v.map(|t| JsonValue::String(t.format("%H:%M:%S%.f").to_string()))
                .unwrap_or(JsonValue::Null)
        } else if let Ok(v) = row.try_get::<&[u8], _>(idx) {
            v.map(base64_cell).unwrap_or(JsonValue::Null)
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
pub(crate) enum SqlParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl SqlParam {
    pub(crate) fn from_json(v: &JsonValue) -> Self {
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

pub(crate) fn bind_refs<'a>(params: &'a [SqlParam]) -> Vec<&'a dyn tiberius::ToSql> {
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

fn mssql_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("mssql", operation, message)
}

fn encode_mssql_response(
    operation: &'static str,
    value: &JsonValue,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value).map_err(|e| {
        mssql_internal_status(operation, format!("mssql response serialise failed: {e}"))
    })
}

impl QueryExecutor for MssqlExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("SQL Server", "query", async {
            let (sql, params_json) = parse_sql_dispatch(request_json)?;
            validate_read_sql(&sql)?;
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
                .map_err(|e| mssql_internal_status("query", format!("mssql query failed: {e}")))?;
            let json: Vec<JsonValue> = rows.iter().map(row_to_json).collect();
            encode_mssql_response("query_response_encode", &JsonValue::Array(json))
        })
        .await
    }
}

impl MutationExecutor for MssqlExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        with_executor_timeout("SQL Server", "mutate", async {
            let (sql, params_json) = parse_sql_dispatch(request_json)?;
            validate_mutation_sql(&sql)?;
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
                .map_err(|e| {
                    mssql_internal_status("mutate", format!("mssql mutate failed: {e}"))
                })?;
            Ok(serde_json::json!({ "rows_affected": result }).to_string())
        })
        .await
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
        Err(capability_status(
            "mssql",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: SQL Server is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "mssql",
            "put_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: SQL Server is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for MssqlExecutor {
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        Err(crate::runtime::executor_utils::capability_status(
            "mssql",
            "ensure_resource",
            "native_resource_lifecycle",
            "SQL Server resource lifecycle is compiler-mediated: issue CREATE/DROP through the compiled DDL path (compile_resource_op + mutate), not the native ensure_resource executor method",
        ))
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(crate::runtime::executor_utils::capability_status(
            "mssql",
            "drop_resource",
            "native_resource_lifecycle",
            "SQL Server resource lifecycle is compiler-mediated: issue CREATE/DROP through the compiled DDL path (compile_resource_op + mutate), not the native drop_resource executor method",
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        // Catalog query — sys.tables is the canonical source.
        let req = serde_json::json!({
            "sql": "SELECT name FROM sys.tables ORDER BY name",
            "params": []
        });
        let body = self.query(&req.to_string()).await?;
        let parsed: JsonValue = serde_json::from_str(&body).map_err(|e| {
            mssql_internal_status("list_resources_parse", format!("list_resources parse: {e}"))
        })?;
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
        let (sql, params_json) = parse_sql_dispatch(request_json)?;
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
            .map_err(|e| {
                mssql_internal_status("transaction", format!("mssql transaction failed: {e}"))
            })?;
        Ok(serde_json::json!({ "rows_affected": result }).to_string())
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("mssql", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use serde_json::json;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "mssql");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn mssql_internal_status_carries_typed_detail() {
        let status =
            mssql_internal_status("query_response_encode", "mssql response serialise failed");
        assert_internal_detail(
            &status,
            "query_response_encode",
            "mssql response serialise failed",
        );
    }

    #[test]
    fn parse_dispatch_extracts_sql_and_params() {
        let req = r#"{"sql":"SELECT * FROM [t] WHERE [id] = @P1","params":["abc"]}"#;
        let (sql, params) = parse_sql_dispatch(req).unwrap();
        assert_eq!(sql, "SELECT * FROM [t] WHERE [id] = @P1");
        assert_eq!(params, vec![json!("abc")]);
    }

    #[test]
    fn parse_dispatch_defaults_params_to_empty() {
        let req = r#"{"sql":"SELECT 1"}"#;
        let (_, params) = parse_sql_dispatch(req).unwrap();
        assert!(params.is_empty());
    }

    #[test]
    fn parse_dispatch_rejects_missing_sql() {
        let req = r#"{"params":[]}"#;
        let err = parse_sql_dispatch(req).unwrap_err();
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
    fn ado_database_helpers_target_master_for_bootstrap() {
        let ado = "Server=localhost,1433;Database=udb;User=sa;Password=x;";
        assert_eq!(ado_database_name(ado).as_deref(), Some("udb"));
        assert_eq!(
            ado_with_database(ado, "master"),
            "Server=localhost,1433;Database=master;User=sa;Password=x;"
        );

        let catalog = "Server=x;Initial Catalog=appdb;User=sa;";
        assert_eq!(ado_database_name(catalog).as_deref(), Some("appdb"));
        assert_eq!(
            ado_with_database("Server=x;User=sa", "master"),
            "Server=x;User=sa;Database=master;"
        );
    }

    #[test]
    fn mssql_database_identifier_is_escaped_and_rejects_batch_separator() {
        assert_eq!(
            mssql_database_identifier("udb]prod").unwrap(),
            "[udb]]prod]"
        );
        assert!(mssql_database_identifier("udb;DROP DATABASE master").is_err());
        assert_eq!(mssql_string_literal("tenant's db"), "'tenant''s db'");
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
        assert!(
            !batch.contains("@read_only"),
            "pooled MSSQL sessions must be restampable per request; got: {batch}"
        );
        // Multiple statements joined into a single batch.
        assert!(batch.contains("; "));
    }

    #[test]
    fn a3_session_context_batch_is_none_without_context() {
        let exec = MssqlExecutor::new(MssqlClient::new("Server=x;"));
        assert!(exec.session_context_batch().is_none());
    }
}

#[cfg(test)]
mod b5_lifecycle_capability_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use crate::runtime::executors::ResourceAdminExecutor;

    /// Decode the prost `ErrorDetail` from the binary trailer the way an SDK
    /// would — same pattern as `executor_utils::error_detail_tests`.
    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[tokio::test]
    async fn ensure_resource_returns_typed_capability_error_not_unimplemented() {
        let exec = MssqlExecutor::new(MssqlClient::new(
            "Server=localhost,1433;Database=udb;User=sa;Password=x;".to_string(),
        ));
        let err = ResourceAdminExecutor::ensure_resource(&exec, "widgets", "{}")
            .await
            .expect_err("native ensure_resource must refuse — it is compiler-mediated");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_ne!(err.code(), tonic::Code::Unimplemented);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.capability_required, "native_resource_lifecycle");
        assert_eq!(detail.backend, "mssql");
    }
}
