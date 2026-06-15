//! ClickHouse column-store executor.
//!
//! Uses the ClickHouse HTTP interface (port 8123 by default).  All queries are
//! sent as HTTP POST requests with the query in the request body and results
//! returned in `JSONCompact` format.
//!
//! Environment variables
//! ─────────────────────
//! | Variable                  | Purpose                                                 |
//! |---------------------------|---------------------------------------------------------|
//! | `UDB_COLUMN_DSN`          | `clickhouse://[user:pass@]host:8123/database`           |
//! | `UDB_COLUMN_HTTP_URL`     | Override HTTP API base (default: derived from DSN)      |
//! | `UDB_COLUMN_USER`         | Username (default: `default`)                           |
//! | `UDB_COLUMN_PASSWORD`     | Password                                                |
//! | `UDB_COLUMN_DATABASE`     | Database name (default: `default`)                      |
//!
//! Operations implemented
//! ──────────────────────
//! - `ping()`                  → `SELECT 1`
//! - `ensure_resource()`       → `CREATE TABLE IF NOT EXISTS …`
//! - `drop_resource()`         → `DROP TABLE IF EXISTS …`
//! - `list_resources()`        → `SHOW TABLES FROM <database>`
//! - `insert_rows()`           → `INSERT INTO … FORMAT JSONEachRow`
//! - `select_rows()`           → arbitrary `SELECT` returning `JSONCompact`
//! - `execute_ddl()`           → raw DDL statement execution
//! - `table_exists()`          → `EXISTS TABLE …`

use std::env;

use reqwest::Client;
use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::runtime::executor_utils::build_probe;
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

const CLICKHOUSE_MAX_QUERY_LIMIT: i64 = 10_000;

// ── Identifier validation ─────────────────────────────────────────────────────

/// Allowlisted ClickHouse table engines.
const ALLOWED_CH_ENGINES: &[&str] = &[
    "MergeTree",
    "ReplacingMergeTree",
    "AggregatingMergeTree",
    "SummingMergeTree",
    "CollapsingMergeTree",
    "VersionedCollapsingMergeTree",
    "Log",
    "TinyLog",
    "Memory",
];

/// Validate a ClickHouse identifier (database name, table name, or column name).
/// Allowed: ASCII letters, digits, and underscores; must start with a letter or
/// underscore; max 64 characters.
fn validate_ch_identifier(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 64 {
        return Err(format!(
            "ClickHouse identifier '{id}' is invalid: must be 1–64 characters"
        ));
    }
    let first = id.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(format!(
            "ClickHouse identifier '{id}' must start with a letter or underscore"
        ));
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "ClickHouse identifier '{id}' contains invalid characters; \
             only ASCII letters, digits, and underscores are allowed"
        ));
    }
    Ok(())
}

// ── Configuration ─────────────────────────────────────────────────────────────

/// Connection configuration for ClickHouse HTTP interface.
#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    /// HTTP base URL.  e.g. `http://localhost:8123`
    pub http_base: String,
    /// ClickHouse username.
    pub username: String,
    /// ClickHouse password.
    pub password: String,
    /// Default database.
    pub database: String,
    /// Whether this endpoint should enforce HTTPS-only requests.
    pub is_cloud: bool,
    /// HTTP connect timeout in seconds.
    pub connect_timeout_secs: u64,
    /// Per-query timeout in seconds.
    pub query_timeout_secs: u64,
}

impl ClickHouseConfig {
    /// Compatibility-only env constructor.
    ///
    /// New runtime paths should use `runtime::config::UdbConfig` and pass the
    /// resolved backend settings into executors instead of reading process env.
    pub fn from_env() -> Option<Self> {
        let dsn = env::var("UDB_COLUMN_DSN").ok();
        let http_override = env::var("UDB_COLUMN_HTTP_URL").ok();

        let http_base = if let Some(url) = http_override {
            url
        } else if let Some(ref dsn) = dsn {
            Self::http_base_from_dsn(dsn)
        } else {
            return None;
        };

        let username = env::var("UDB_COLUMN_USER").unwrap_or_else(|_| "default".to_string());
        let password = env::var("UDB_COLUMN_PASSWORD").unwrap_or_default();
        let database = env::var("UDB_COLUMN_DATABASE")
            .ok()
            .or_else(|| dsn.as_deref().and_then(Self::db_from_dsn))
            .unwrap_or_else(|| "default".to_string());
        let is_cloud = super::http::is_cloud("UDB_CH_DEPLOY_MODE", &http_base, ".clickhouse.cloud");
        let connect_timeout_secs =
            super::http::env_timeout("UDB_CH_CONNECT_TIMEOUT_SECS", 10).as_secs();
        let query_timeout_secs = env_timeout_secs("UDB_CH_QUERY_TIMEOUT_SECS", 30);

        Some(Self {
            http_base,
            username,
            password,
            database,
            is_cloud,
            connect_timeout_secs,
            query_timeout_secs,
        })
    }

    pub(crate) fn http_base_from_dsn(dsn: &str) -> String {
        // clickhouse://host:8123/db → http://host:8123
        // http://host:8123 → unchanged
        if dsn.starts_with("http://") || dsn.starts_with("https://") {
            return dsn.split('/').take(3).collect::<Vec<_>>().join("/");
        }
        let rest = dsn.strip_prefix("clickhouse://").unwrap_or(dsn);
        let rest = if rest.contains('@') {
            rest.split_once('@').map(|(_, r)| r).unwrap_or(rest)
        } else {
            rest
        };
        let host_port = rest.split('/').next().unwrap_or(rest);
        format!("http://{host_port}")
    }

    pub(crate) fn db_from_dsn(dsn: &str) -> Option<String> {
        let rest = dsn.strip_prefix("clickhouse://").unwrap_or(dsn);
        let rest = if rest.contains('@') {
            rest.split_once('@').map(|(_, r)| r)?
        } else {
            rest
        };
        let db = rest.split('/').nth(1)?;
        let db = db.split('?').next().unwrap_or(db);
        if db.is_empty() {
            None
        } else {
            Some(db.to_string())
        }
    }
}

fn env_timeout_secs(key: &str, default_secs: u64) -> u64 {
    match env::var(key) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(value) if value > 0 => value,
            Ok(_) => {
                tracing::warn!("{key} must be greater than zero; using {default_secs}s");
                default_secs
            }
            Err(err) => {
                tracing::warn!(
                    "{key} is not a valid timeout in seconds: {err}; using {default_secs}s"
                );
                default_secs
            }
        },
        Err(_) => default_secs,
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// ClickHouse column-store executor using the ClickHouse HTTP interface.
#[derive(Debug, Clone)]
pub struct ClickHouseExecutor {
    config: ClickHouseConfig,
    http: Client,
}

impl crate::runtime::backend_context::BackendContextEnforcer for ClickHouseExecutor {
    fn backend_label(&self) -> &str {
        "clickhouse"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // HONEST POSTURE: this generic executor accepts raw SQL and ad-hoc
        // table/filter specs. It can publish tenant context through ClickHouse
        // SETTINGS for operator row policies to consume, but it does not prove
        // that arbitrary SQL/filter dispatch was rewritten with tenant/project
        // predicates. Treat the generic path as advisory unless a higher-level
        // compiler path owns and verifies predicate injection.
        if ctx.is_empty() {
            crate::runtime::backend_context::ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            }
        } else {
            crate::runtime::backend_context::ContextEffect::Advisory {
                recorded_in: "ClickHouse SETTINGS context for generic raw SQL/filter dispatch; \
                              tenant/project predicate injection not verified here"
                    .into(),
            }
        }
    }
}

impl ClickHouseExecutor {
    /// Construct from config.
    ///
    /// GAP 20 fix: the HTTP client is built once with configurable connect and
    /// query timeouts. Credentials are sent as `X-ClickHouse-*` headers (not URL
    /// query params that appear in access logs).
    ///
    /// GAP 39: When `UDB_CH_DEPLOY_MODE=cloud`, the URL must use `https://`
    /// and the client is built with `.https_only(true)` to prevent credential
    /// leakage over plaintext HTTP.
    pub fn new(config: ClickHouseConfig) -> Self {
        if config.is_cloud && config.http_base.starts_with("http://") {
            tracing::error!(
                http_base = %config.http_base,
                "ClickHouse is configured as cloud (tls_required=true) but \
                 UDB_COLUMN_DSN / UDB_COLUMN_HTTP_URL uses http:// — \
                 change to https:// or set UDB_CH_DEPLOY_MODE=self_hosted"
            );
        }
        let connect_timeout = std::time::Duration::from_secs(config.connect_timeout_secs.max(1));
        let http = super::http::HttpClientSpec::with_connect_timeout(connect_timeout)
            .https_only(config.is_cloud)
            .build();
        Self { config, http }
    }

    pub fn kind(&self) -> BackendKind {
        BackendKind::Clickhouse
    }

    pub fn name(&self) -> &str {
        "ClickHouse"
    }

    /// B.10c: the database this executor's queries run against (sent as the
    /// `X-ClickHouse-Database` header on every request). The canonical store
    /// reuses this to build fully-qualified `` `db`.`table` `` identifiers and to
    /// report its `database` to the runtime.
    pub(crate) fn database(&self) -> &str {
        &self.config.database
    }

    /// Construct from environment variables.  Returns `None` when
    /// `UDB_COLUMN_DSN` / `UDB_COLUMN_HTTP_URL` are absent.
    pub fn from_env() -> Option<Self> {
        ClickHouseConfig::from_env().map(Self::new)
    }

    // ── Low-level execution ───────────────────────────────────────────────────

    /// Detect a genuine trailing `FORMAT <name>` clause. A naive
    /// `contains("FORMAT")` matches function names like `formatDateTime(...)`
    /// or `toFormat(...)` in the middle of a query, which would wrongly
    /// suppress the appended output format. ClickHouse's FORMAT clause is
    /// always the final clause, so check that the second-to-last token
    /// (after stripping a trailing `;`) is `FORMAT`.
    fn has_trailing_format_clause(sql: &str) -> bool {
        let trimmed = sql.trim_end().trim_end_matches(';').trim_end();
        let mut tokens = trimmed.split_whitespace().rev();
        // last token = format name; the token before it must be FORMAT
        let _name = tokens.next();
        tokens
            .next()
            .map(|t| t.eq_ignore_ascii_case("FORMAT"))
            .unwrap_or(false)
    }

    /// Execute a SQL query string against ClickHouse and return the raw response body.
    /// Appends `FORMAT <fmt>` unless the query already contains a FORMAT clause.
    async fn execute_raw(&self, sql: &str, format: &str) -> Result<String, String> {
        let full_sql = if format.is_empty() || Self::has_trailing_format_clause(sql) {
            sql.to_string()
        } else {
            format!("{sql} FORMAT {format}")
        };

        // GAP 20: credentials sent as X-ClickHouse-* headers, NOT as URL query
        // parameters which appear in HTTP access logs, nginx logs, and HAProxy logs.
        let query_timeout = std::time::Duration::from_secs(self.config.query_timeout_secs.max(1));
        let resp = self
            .http
            .post(&self.config.http_base)
            .header("X-ClickHouse-User", &self.config.username)
            .header("X-ClickHouse-Key", &self.config.password)
            .header("X-ClickHouse-Database", &self.config.database)
            .header("Content-Type", "text/plain; charset=utf-8")
            .timeout(query_timeout)
            .body(full_sql)
            .send()
            .await
            .map_err(|e| format!("ClickHouse HTTP error: {e}"))?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("ClickHouse query failed [{status}]: {body}"));
        }
        Ok(body)
    }

    /// Execute a `SELECT` and return rows as a `Vec<Json>`.
    /// Uses `JSONCompact` output format.
    pub async fn select_rows(&self, sql: &str) -> Result<Vec<Json>, String> {
        let body = self.execute_raw(sql, "JSONCompact").await?;
        let mut parsed: Json =
            serde_json::from_str(&body).map_err(|e| format!("ClickHouse decode failed: {e}"))?;

        // JSONCompact: { "meta": [...], "data": [[...], ...] }. Take both arrays out
        // of `parsed` and move each cell into its row object — no whole-array,
        // per-row, or per-cell value clones (#103).
        let column_names: Vec<String> = match parsed.get_mut("meta").map(Json::take) {
            Some(Json::Array(cols)) => cols
                .into_iter()
                .filter_map(|col| col.get("name").and_then(|n| n.as_str()).map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };

        let rows = match parsed.get_mut("data").map(Json::take) {
            Some(Json::Array(data)) => data
                .into_iter()
                .map(|row| {
                    let mut obj = serde_json::Map::new();
                    if let Json::Array(cells) = row {
                        for (name, val) in column_names.iter().zip(cells) {
                            obj.insert(name.clone(), val);
                        }
                    }
                    Json::Object(obj)
                })
                .collect(),
            _ => Vec::new(),
        };
        Ok(rows)
    }

    /// Execute a DDL or DML statement (no result rows expected).
    pub async fn execute_ddl(&self, sql: &str) -> Result<(), String> {
        self.execute_raw(sql, "").await?;
        Ok(())
    }

    /// Insert a batch of rows into `table` using `JSONEachRow` format.
    pub async fn insert_rows(&self, table: &str, rows: &[Json]) -> Result<(), String> {
        validate_ch_identifier(table)?;
        if rows.is_empty() {
            return Ok(());
        }
        let ndjson = rows
            .iter()
            .map(|r| serde_json::to_string(r).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n");

        let sql = format!(
            "INSERT INTO `{db}`.`{table}` FORMAT JSONEachRow\n{ndjson}",
            db = self.config.database
        );
        self.execute_raw(&sql, "").await?;
        Ok(())
    }

    fn select_template_sql(&self, spec: &Json) -> Result<String, String> {
        let table = spec
            .get("table")
            .and_then(Json::as_str)
            .ok_or_else(|| "missing required field 'table'".to_string())?;
        validate_ch_identifier(table)?;
        let columns = spec
            .get("columns")
            .and_then(Json::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Json::as_str)
                    .map(|column| {
                        validate_ch_identifier(column)?;
                        Ok(format!("`{column}`"))
                    })
                    .collect::<Result<Vec<_>, String>>()
            })
            .transpose()?
            .filter(|columns| !columns.is_empty())
            .unwrap_or_else(|| vec!["*".to_string()])
            .join(", ");
        let mut sql = format!(
            "SELECT {columns} FROM `{db}`.`{table}`",
            db = self.config.database
        );
        if let Some(filter) = spec.get("filter").and_then(Json::as_object)
            && !filter.is_empty()
        {
            let mut clauses = Vec::new();
            for (field, value) in filter {
                validate_ch_identifier(field)?;
                let literal = match value {
                    Json::String(s) => format!("'{}'", s.replace('\'', "''")),
                    Json::Number(n) => n.to_string(),
                    Json::Bool(b) => {
                        if *b {
                            "1".to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                    Json::Null => "NULL".to_string(),
                    _ => return Err("ClickHouse filter values must be scalar".to_string()),
                };
                clauses.push(format!("`{field}` = {literal}"));
            }
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        if let Some(order_by) = spec.get("order_by").and_then(Json::as_str) {
            validate_ch_identifier(order_by)?;
            sql.push_str(&format!(" ORDER BY `{order_by}`"));
        }
        let limit = spec.get("limit").and_then(Json::as_i64).unwrap_or(100);
        if limit > 0 {
            if limit > CLICKHOUSE_MAX_QUERY_LIMIT {
                tracing::warn!(
                    requested_limit = limit,
                    applied_limit = CLICKHOUSE_MAX_QUERY_LIMIT,
                    "ClickHouse generic query limit capped"
                );
            }
            sql.push_str(&format!(" LIMIT {}", limit.min(CLICKHOUSE_MAX_QUERY_LIMIT)));
        }
        Ok(sql)
    }

    /// Check whether a table exists in the configured database.
    pub async fn table_exists(&self, table: &str) -> Result<bool, String> {
        validate_ch_identifier(table)?;
        let sql = format!("EXISTS TABLE `{db}`.`{table}`", db = self.config.database);
        let body = self.execute_raw(&sql, "").await?;
        Ok(body.trim() == "1")
    }

    /// Build a `CREATE TABLE IF NOT EXISTS` statement from a spec JSON.
    ///
    /// `spec_json` keys (all optional):
    /// - `"engine"` → table engine (default: `MergeTree`; must be in the allowlist)
    /// - `"order_by"` → `ORDER BY` expression (default: `tuple()`)
    /// - `"columns"` → array of `{"name": "...", "type": "..."}` objects
    fn create_table_sql(&self, table: &str, spec: &Json) -> Result<String, String> {
        validate_ch_identifier(table)?;

        let engine = spec
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("MergeTree");
        if !ALLOWED_CH_ENGINES.contains(&engine) {
            return Err(format!(
                "ClickHouse engine '{engine}' is not in the allowlist; \
                 allowed: {}",
                ALLOWED_CH_ENGINES.join(", ")
            ));
        }

        // order_by may contain expressions like `tuple()` or `id` — allow
        // alphanumeric, underscores, parentheses, commas, and spaces only.
        let order_by = spec
            .get("order_by")
            .and_then(|v| v.as_str())
            .unwrap_or("tuple()");
        if !order_by
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || " _(),".contains(c))
        {
            return Err(format!(
                "ClickHouse ORDER BY expression '{order_by}' contains invalid characters"
            ));
        }

        let columns = spec
            .get("columns")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let col_defs = if columns.is_empty() {
            "    _id String,\n    _data String".to_string()
        } else {
            let mut parts = Vec::with_capacity(columns.len());
            for c in &columns {
                let name = c.get("name").and_then(|v| v.as_str()).unwrap_or("col");
                let typ = c.get("type").and_then(|v| v.as_str()).unwrap_or("String");
                validate_ch_identifier(name)?;
                // Type may be composite (e.g. "Nullable(String)", "UInt64") — allow
                // alphanumeric, underscores, parentheses, and spaces only.
                if !typ
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || "_ ()".contains(c))
                {
                    return Err(format!(
                        "ClickHouse column type '{typ}' for '{name}' contains invalid characters"
                    ));
                }
                parts.push(format!("    `{name}` {typ}"));
            }
            parts.join(",\n")
        };

        Ok(format!(
            "CREATE TABLE IF NOT EXISTS `{db}`.`{table}` (\n{col_defs}\n) ENGINE = {engine} ORDER BY {order_by}",
            db = self.config.database
        ))
    }
}

// ── BackendExecutor (runtime trait) ─────────────────────────────────────────────
// Phase D vertical slice: ClickHouseExecutor is a real `BackendExecutor` —
// stateless leaf I/O only. Instance selection, circuit-breaking, and channel
// admission stay in the orchestration layer (`DataBrokerRuntime`); this type just
// performs the leaf operation. Request shapes mirror the former inline arms in
// `core.rs` (`query_backend_target` / `mutate_backend_target`).

impl BackendHealth for ClickHouseExecutor {
    async fn ping(&self) -> Result<(), String> {
        let result = self.select_rows("SELECT 1 AS ok").await?;
        if result.is_empty() {
            return Err("ClickHouse ping returned no rows".to_string());
        }
        Ok(())
    }
}

impl QueryExecutor for ClickHouseExecutor {
    /// `{"sql":"SELECT ..."}` or `{"table":"t","columns":[...],"filter":{}}`.
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let sql = if let Some(sql) = spec.get("sql").and_then(Json::as_str) {
            sql.to_string()
        } else {
            self.select_template_sql(&spec)
                .map_err(tonic::Status::invalid_argument)?
        };
        let rows = self
            .select_rows(&sql)
            .await
            .map_err(tonic::Status::internal)?;
        serde_json::to_string(&rows).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for ClickHouseExecutor {
    /// `{"sql":"CREATE ..."}` → DDL, or `{"table":"t","rows":[...]}` → insert.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        if let Some(sql) = spec.get("sql").and_then(Json::as_str) {
            self.execute_ddl(sql)
                .await
                .map_err(tonic::Status::internal)?;
            return Ok(r#"{"affected_rows":0}"#.to_string());
        }
        let table = spec
            .get("table")
            .and_then(Json::as_str)
            .ok_or_else(|| tonic::Status::invalid_argument("missing required field 'table'"))?;
        // Validate the identifier as a client error BEFORE any I/O, mirroring the
        // read path (`select_template_sql` → `invalid_argument`). Without this an
        // empty/over-long table id escaped `insert_rows` as the catch-all `internal`
        // below, leaking a validation message as a server fault (bug B6).
        validate_ch_identifier(table).map_err(tonic::Status::invalid_argument)?;
        let rows = spec
            .get("rows")
            .and_then(Json::as_array)
            .ok_or_else(|| tonic::Status::invalid_argument("rows must be an array"))?;
        self.insert_rows(table, rows)
            .await
            .map_err(tonic::Status::internal)?;
        Ok(format!(r#"{{"affected_rows":{}}}"#, rows.len()))
    }
}

impl SearchExecutor for ClickHouseExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "clickhouse does not support vector/document search",
        ))
    }
}

impl ObjectExecutor for ClickHouseExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "clickhouse is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "clickhouse is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for ClickHouseExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        validate_ch_identifier(resource_name).map_err(tonic::Status::internal)?;
        let spec: Json = serde_json::from_str(spec_json).unwrap_or(json!({}));
        let ddl = self
            .create_table_sql(resource_name, &spec)
            .map_err(tonic::Status::internal)?;
        self.execute_ddl(&ddl)
            .await
            .map_err(tonic::Status::internal)
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        validate_ch_identifier(resource_name).map_err(tonic::Status::internal)?;
        let ddl = format!(
            "DROP TABLE IF EXISTS `{db}`.`{resource_name}`",
            db = self.config.database
        );
        self.execute_ddl(&ddl)
            .await
            .map_err(tonic::Status::internal)
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let sql = format!("SHOW TABLES FROM `{}`", self.config.database);
        let rows = self
            .select_rows(&sql)
            .await
            .map_err(tonic::Status::internal)?;
        let tables = rows
            .iter()
            .filter_map(|r| {
                r.get("name")
                    .or_else(|| r.get("Tables_in_default"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        Ok(tables)
    }
}

impl BackendExecutor for ClickHouseExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "clickhouse does not support multi-statement transactions",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "clickhouse",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};

    #[test]
    fn clickhouse_executor_kind_and_name() {
        let cfg = ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        };
        let exec = ClickHouseExecutor::new(cfg);
        assert_eq!(exec.kind(), BackendKind::Clickhouse);
        assert_eq!(exec.name(), "ClickHouse");
    }

    #[test]
    fn clickhouse_generic_dispatch_context_is_advisory() {
        let exec = ClickHouseExecutor::new(ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        });
        let ctx = AppliedContext {
            tenant_id: "tenant-a".into(),
            project_id: "project-a".into(),
            ..Default::default()
        };

        match BackendContextEnforcer::enforce(&exec, &ctx) {
            ContextEffect::Advisory { recorded_in } => {
                assert!(recorded_in.contains("generic raw SQL/filter dispatch"));
                assert!(recorded_in.contains("not verified"));
            }
            other => panic!("expected Advisory, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clickhouse_backend_executor_rejects_unsupported_and_malformed() {
        let exec = ClickHouseExecutor::new(ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            database: "default".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        });
        // Unsupported operations fail without any network I/O.
        assert!(SearchExecutor::search(&exec, "{}").await.is_err());
        assert!(ObjectExecutor::get_object(&exec, "{}").await.is_err());
        assert!(
            ObjectExecutor::put_object(&exec, "{}", vec![])
                .await
                .is_err()
        );
        assert!(BackendExecutor::transaction(&exec, "{}").await.is_err());
        // query/mutate validate the request shape before any network call.
        assert!(QueryExecutor::query(&exec, "not json").await.is_err());
        assert!(QueryExecutor::query(&exec, "{}").await.is_err()); // missing sql
        assert!(MutationExecutor::mutate(&exec, "{}").await.is_err()); // missing table
    }

    #[test]
    fn clickhouse_config_parses_dsn() {
        let base = ClickHouseConfig::http_base_from_dsn(
            "clickhouse://user:pass@ch.example.com:8123/analytics",
        );
        assert_eq!(base, "http://ch.example.com:8123");
    }

    #[test]
    fn clickhouse_config_db_from_dsn() {
        let db = ClickHouseConfig::db_from_dsn("clickhouse://localhost:8123/analytics");
        assert_eq!(db.as_deref(), Some("analytics"));
    }

    #[test]
    fn clickhouse_config_from_env_returns_none_without_vars() {
        unsafe {
            env::remove_var("UDB_COLUMN_DSN");
            env::remove_var("UDB_COLUMN_HTTP_URL");
        }
        assert!(ClickHouseConfig::from_env().is_none());
    }

    #[test]
    fn clickhouse_create_table_sql_default_columns() {
        let cfg = ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            database: "mydb".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        };
        let exec = ClickHouseExecutor::new(cfg);
        let sql = exec
            .create_table_sql("events", &json!({}))
            .expect("default spec should be valid");
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `mydb`.`events`"));
        assert!(sql.contains("MergeTree"));
        assert!(sql.contains("ORDER BY tuple()"));
    }

    #[test]
    fn clickhouse_create_table_sql_custom_spec() {
        let cfg = ClickHouseConfig {
            http_base: "http://localhost:8123".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            database: "analytics".to_string(),
            is_cloud: false,
            connect_timeout_secs: 10,
            query_timeout_secs: 30,
        };
        let exec = ClickHouseExecutor::new(cfg);
        let spec = json!({
            "engine": "ReplacingMergeTree",
            "order_by": "event_time",
            "columns": [
                {"name": "event_time", "type": "DateTime"},
                {"name": "user_id",   "type": "UInt64"},
                {"name": "event",     "type": "String"}
            ]
        });
        let sql = exec
            .create_table_sql("page_views", &spec)
            .expect("custom spec should be valid");
        assert!(sql.contains("ReplacingMergeTree"));
        assert!(sql.contains("ORDER BY event_time"));
        assert!(sql.contains("`event_time` DateTime"));
    }
}
