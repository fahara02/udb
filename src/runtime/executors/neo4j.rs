//! Neo4j graph-store executor.
#![allow(clippy::result_large_err)]
//!
//! Uses the Neo4j HTTP Transactional Cypher API (available in Neo4j 3.5+,
//! AuraDB, and Neo4j Desktop).  No native Bolt driver dependency is needed —
//! the HTTP JSON API supports all Cypher queries.
//!
//! Environment variables
//! ─────────────────────
//! | Variable                | Purpose                                               |
//! |-------------------------|-------------------------------------------------------|
//! | `UDB_GRAPH_DSN`         | `bolt://[user:pass@]host:port` or `http://host:7474`   |
//! | `UDB_GRAPH_HTTP_URL`    | Override HTTP API base (default: derived from DSN)    |
//! | `UDB_GRAPH_USER`        | Neo4j username (default: `neo4j`)                     |
//! | `UDB_GRAPH_PASSWORD`    | Neo4j password                                        |
//! | `UDB_GRAPH_DATABASE`    | Neo4j database name (default: `neo4j`)                |
//!
//! Operations implemented
//! ──────────────────────
//! - `ping()`                  → `RETURN 1`
//! - `ensure_resource()`       → `CREATE CONSTRAINT … IF NOT EXISTS` for a node label
//! - `drop_resource()`         → `DROP CONSTRAINT … IF EXISTS`
//! - `list_resources()`        → `SHOW CONSTRAINTS`
//! - `create_node()`           → `MERGE (n:Label {id}) SET n += props`
//! - `find_nodes()`            → `MATCH (n:Label) WHERE … RETURN n`
//! - `update_node()`           → `MATCH (n:Label {id}) SET n += props`
//! - `delete_node()`           → `MATCH (n:Label {id}) DETACH DELETE n`
//! - `create_relationship()`   → `MATCH (a),(b) MERGE (a)-[r:TYPE]->(b) SET r += props`
//! - `query()`                 → arbitrary Cypher execution

use std::env;

use reqwest::Client;
use serde_json::{Value as Json, json};

use crate::backend::BackendKind;
use crate::runtime::executor_utils::{build_probe, capability_status, invalid_argument_fields};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

// ── Identifier validation ─────────────────────────────────────────────────────

/// Validate a Neo4j label, relationship type, or property name.
/// Allowed: ASCII letters, digits, and underscores; must start with a letter or
/// underscore; max 64 characters.  Returns an error string if invalid.
fn validate_neo4j_identifier(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > 64 {
        return Err(format!(
            "Neo4j identifier '{id}' is invalid: must be 1–64 characters"
        ));
    }
    let first = id.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(format!(
            "Neo4j identifier '{id}' must start with a letter or underscore"
        ));
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "Neo4j identifier '{id}' contains invalid characters; \
             only ASCII letters, digits, and underscores are allowed"
        ));
    }
    Ok(())
}

/// Transaction access mode for the HTTP API's `access-mode` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Neo4jAccessMode {
    Read,
    Write,
}

/// Cypher clauses / commands that write data or schema, or administer the
/// DBMS. A read (`query`) statement containing any of them as a bare keyword
/// is refused before it reaches the server.
const CYPHER_WRITE_KEYWORDS: &[&str] = &[
    "CREATE",
    "MERGE",
    "SET",
    "DELETE",
    "DETACH",
    "REMOVE",
    "DROP",
    "FOREACH",
    "LOAD",
    "ALTER",
    "GRANT",
    "DENY",
    "REVOKE",
    "START",
    "STOP",
    "TERMINATE",
    "ENABLE",
    "RENAME",
];

/// Procedures a read statement may `CALL`: read-only index queries and schema
/// introspection. Any other procedure (APOC writers, `dbms.*`, ...) can
/// mutate, so it is refused on the read path.
const CYPHER_READ_PROCEDURE_PREFIXES: &[&str] = &[
    "db.index.fulltext.querynodes",
    "db.index.fulltext.queryrelationships",
    "db.index.vector.querynodes",
    "db.index.vector.queryrelationships",
    "db.labels",
    "db.relationshiptypes",
    "db.propertykeys",
    "db.schema.",
];

/// Conservative read-only check for a Cypher statement run on the query path.
/// Lexes the text (skipping string literals, backtick identifiers, comments,
/// parameters `$x`, property access `n.x` and labels/map keys `:x`) and
/// rejects any write keyword or a `CALL` of a procedure outside the read-only
/// allowlist. A `CALL { ... }` subquery is allowed: its body is lexed like
/// everything else, so a write inside it is still caught. The server-side
/// READ access mode is the second line of defence.
fn cypher_write_clause(cypher: &str) -> Option<String> {
    let chars: Vec<char> = cypher.chars().collect();
    let mut i = 0;
    let mut prev_sig: Option<char> = None;
    while i < chars.len() {
        let ch = chars[i];
        match ch {
            '\'' | '"' => {
                let quote = ch;
                i += 1;
                while i < chars.len() && chars[i] != quote {
                    if chars[i] == '\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
                prev_sig = Some(quote);
            }
            '`' => {
                i += 1;
                while i < chars.len() && chars[i] != '`' {
                    i += 1;
                }
                i += 1;
                prev_sig = Some('`');
            }
            '/' if chars.get(i + 1) == Some(&'/') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                i += 2;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                let upper = word.to_ascii_uppercase();
                // `n.set`, `$delete`, `:Create` (label / map key) are names,
                // not clauses.
                let is_name = matches!(prev_sig, Some('.') | Some('$') | Some(':'));
                let mut j = i;
                while j < chars.len() && chars[j].is_whitespace() {
                    j += 1;
                }
                let is_map_key = chars.get(j) == Some(&':');
                if !is_name && !is_map_key {
                    if CYPHER_WRITE_KEYWORDS.contains(&upper.as_str()) {
                        return Some(upper);
                    }
                    if upper == "CALL" && chars.get(j) != Some(&'{') {
                        // Read the dotted procedure name that follows.
                        let mut k = j;
                        while k < chars.len()
                            && (chars[k].is_ascii_alphanumeric()
                                || chars[k] == '_'
                                || chars[k] == '.')
                        {
                            k += 1;
                        }
                        let procedure: String =
                            chars[j..k].iter().collect::<String>().to_ascii_lowercase();
                        if !CYPHER_READ_PROCEDURE_PREFIXES
                            .iter()
                            .any(|allowed| procedure.starts_with(allowed))
                        {
                            return Some(format!("CALL {procedure}"));
                        }
                    }
                }
                prev_sig = Some('a');
            }
            c if c.is_whitespace() => {
                i += 1;
            }
            other => {
                prev_sig = Some(other);
                i += 1;
            }
        }
    }
    None
}

/// Real affected count from a statement's `includeStats` counters plus its
/// returned rows: nodes/relationships created or deleted, or — for a pure
/// property update (`MATCH ... SET ... RETURN n`) where the counters carry no
/// entity count — the number of rows the statement returned. Zero when the
/// statement changed nothing.
fn neo4j_affected_rows(stats: Option<&Json>, returned_rows: usize) -> u64 {
    let Some(stats) = stats else {
        return 0;
    };
    let counter = |key: &str| stats.get(key).and_then(Json::as_u64).unwrap_or(0);
    let entities = counter("nodes_created")
        + counter("nodes_deleted")
        + counter("relationships_created")
        + counter("relationship_deleted")
        + counter("relationships_deleted");
    let contains_updates = stats
        .get("contains_updates")
        .and_then(Json::as_bool)
        .unwrap_or(entities > 0 || counter("properties_set") > 0);
    if !contains_updates {
        return 0;
    }
    entities.max(returned_rows as u64)
}

/// The `affected` column of a `... RETURN count(*) AS affected` statement.
fn affected_from_count_rows(rows: &[Json]) -> u64 {
    rows.first()
        .and_then(|row| row.get("affected"))
        .and_then(Json::as_u64)
        .unwrap_or(0)
}

fn neo4j_invalid_field_status(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_neo4j_request_json_status(err: serde_json::Error) -> tonic::Status {
    neo4j_invalid_field_status(
        "request_json",
        "must be valid JSON for Neo4j generic dispatch",
        format!("invalid request json: {err}"),
    )
}

fn neo4j_required_field_status(field: &str) -> tonic::Status {
    neo4j_invalid_field_status(
        field.to_string(),
        format!("{field} is required for this Neo4j operation"),
        format!("missing required field '{field}'"),
    )
}

fn unsupported_neo4j_operation_status(operation: &str) -> tonic::Status {
    neo4j_invalid_field_status(
        "operation",
        "unsupported Neo4j mutation operation",
        format!("unsupported Neo4j mutation operation '{operation}'"),
    )
}

/// The optional `scope` (`{_tenant_id, _project_id}`) of a mutation request.
fn request_scope(spec: &Json) -> Result<GraphScope, tonic::Status> {
    GraphScope::from_request(spec.get("scope")).map_err(|message| {
        neo4j_invalid_field_status(
            "scope",
            "must be an object holding only _tenant_id / _project_id strings",
            message,
        )
    })
}

fn neo4j_identifier_status(field: impl Into<String>, message: impl Into<String>) -> tonic::Status {
    neo4j_invalid_field_status(field, "must be a valid Neo4j identifier", message)
}

fn neo4j_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("neo4j", operation, message)
}

fn encode_neo4j_response(rows: &[Json], operation: &'static str) -> Result<String, tonic::Status> {
    serde_json::to_string(rows).map_err(|err| neo4j_internal_status(operation, err.to_string()))
}

// ── Configuration ─────────────────────────────────────────────────────────────

/// Connection configuration for Neo4j HTTP Transactional API.
#[derive(Clone)]
pub struct Neo4jConfig {
    /// HTTP(S) base URL to the Neo4j HTTP API root.
    /// e.g. `http://localhost:7474` or `https://xxxxx.databases.neo4j.io`
    pub http_base: String,
    /// Neo4j username.
    pub username: String,
    /// Neo4j password.
    pub password: String,
    /// Database name (`neo4j` by default for Community Edition).
    pub database: String,
    /// Whether this endpoint should enforce HTTPS-only requests.
    pub is_cloud: bool,
    /// Allow plaintext self-hosted HTTP without warning.
    pub dev_mode: bool,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
}

// 4.6 secrets posture: redact `password` in Debug (host/user/db are non-secret).
impl std::fmt::Debug for Neo4jConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jConfig")
            .field("http_base", &self.http_base)
            .field("username", &self.username)
            .field("password", &"[redacted]")
            .field("database", &self.database)
            .field("is_cloud", &self.is_cloud)
            .field("dev_mode", &self.dev_mode)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

impl Neo4jConfig {
    /// Compatibility-only env constructor.
    ///
    /// New runtime paths should use `runtime::config::UdbConfig` and pass the
    /// resolved backend settings into executors instead of reading process env.
    pub fn from_env() -> Option<Self> {
        let dsn = env::var("UDB_GRAPH_DSN").ok();
        let http_override = env::var("UDB_GRAPH_HTTP_URL").ok();

        let http_base = if let Some(url) = http_override {
            url
        } else if let Some(ref dsn) = dsn {
            Self::http_base_from_dsn(dsn)
        } else {
            return None;
        };

        // Trim: username/password become HTTP Basic-auth header values; a CRLF
        // `\r` from a CRLF `.env` would poison the header (invalid header value).
        let username = env::var("UDB_GRAPH_USER")
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "neo4j".to_string());
        let password = env::var("UDB_GRAPH_PASSWORD")
            .map(|v| v.trim().to_string())
            .unwrap_or_default();
        let database = env::var("UDB_GRAPH_DATABASE")
            .map(|v| v.trim().to_string())
            .unwrap_or_else(|_| "neo4j".to_string());
        let is_cloud =
            super::http::is_cloud("UDB_NEO4J_DEPLOY_MODE", &http_base, ".databases.neo4j.io");
        let dev_mode = std::env::var("UDB_DEV_MODE")
            .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        let timeout_secs = super::http::env_timeout("UDB_GRAPH_TIMEOUT_SECS", 30).as_secs();

        Some(Self {
            http_base,
            username,
            password,
            database,
            is_cloud,
            dev_mode,
            timeout_secs,
        })
    }

    pub(crate) fn http_base_from_dsn(dsn: &str) -> String {
        // bolt://host:7687 → http://host:7474
        // http://host:7474 → unchanged
        if dsn.starts_with("http://") || dsn.starts_with("https://") {
            // strip credentials if present
            if let Some(at) = dsn.find('@') {
                let scheme_end = dsn.find("://").map(|i| i + 3).unwrap_or(0);
                let scheme = &dsn[..scheme_end];
                return format!("{scheme}{}", &dsn[at + 1..]);
            }
            return dsn.to_string();
        }
        // bolt://[user:pass@]host:7687 → http://host:7474
        let rest = dsn.strip_prefix("bolt://").unwrap_or(dsn);
        let rest = if rest.contains('@') {
            rest.split_once('@').map(|(_, r)| r).unwrap_or(rest)
        } else {
            rest
        };
        let host_port = rest.split('/').next().unwrap_or(rest);
        let (host, _) = host_port.split_once(':').unwrap_or((host_port, "7687"));
        format!("http://{host}:7474")
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// Neo4j graph-store executor using the HTTP Transactional Cypher API.
#[derive(Debug, Clone)]
pub struct Neo4jExecutor {
    config: Neo4jConfig,
    http: Client,
}

impl crate::runtime::backend_context::BackendContextEnforcer for Neo4jExecutor {
    fn backend_label(&self) -> &str {
        "neo4j"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // C7/C8: the Neo4j IR compiler ANDs `n._tenant_id = $ctx_tenant_id` /
        // `n._project_id = $ctx_project_id` into every MATCH WHERE clause and
        // stamps them into the MERGE key of every write; projection writes key
        // nodes and edges on the same two properties through a request `scope`.
        // Raw Cypher dispatch carries no scope — the raw-dispatch gate governs it.
        crate::runtime::backend_context::enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id in MERGE key and MATCH WHERE (IR + projection scope); raw Cypher gated",
        )
    }
}

impl Neo4jExecutor {
    /// Construct from config.
    ///
    /// GAP 28 fix: the HTTP client is built with a configurable request timeout
    /// (UDB_GRAPH_TIMEOUT_SECS, default 30 s) so Neo4j calls cannot block
    /// the runtime indefinitely. A warning is emitted when plain HTTP is used,
    /// because Basic Auth credentials will be transmitted unencrypted.
    ///
    /// GAP 39: When `UDB_NEO4J_DEPLOY_MODE=cloud` (or the DSN looks like an
    /// AuraDB / cloud URL), the URL must use `https://` and the client is built
    /// with `.https_only(true)` to prevent credential leakage.
    pub fn new(config: Neo4jConfig) -> Self {
        if config.is_cloud && config.http_base.starts_with("http://") {
            // Log an error instead of panicking — the reqwest client is already
            // built with .https_only(true) below, so every request will fail at
            // runtime with a clear TLS error rather than crashing the process.
            // Panicking here would bring down the entire service if Neo4j is
            // misconfigured, which is worse than a per-request error.
            tracing::error!(
                http_base = %config.http_base,
                "Neo4j is configured as cloud but the HTTP base uses http:// — \
                 all requests will fail. Change to https:// or set UDB_NEO4J_DEPLOY_MODE=self_hosted"
            );
        }
        if !config.dev_mode && !config.is_cloud && config.http_base.starts_with("http://") {
            tracing::warn!(
                http_base = %config.http_base,
                "Neo4j HTTP base uses plain HTTP — Basic Auth credentials will be sent unencrypted"
            );
        }
        let timeout = std::time::Duration::from_secs(config.timeout_secs.max(1));
        let http = super::http::HttpClientSpec::with_timeout(timeout)
            .https_only(config.is_cloud)
            .build();
        Self { config, http }
    }

    pub fn kind(&self) -> BackendKind {
        BackendKind::Neo4j
    }

    pub fn name(&self) -> &str {
        "Neo4j"
    }

    /// Construct from environment variables.  Returns `None` when
    /// `UDB_GRAPH_DSN` / `UDB_GRAPH_HTTP_URL` are absent.
    pub fn from_env() -> Option<Self> {
        Neo4jConfig::from_env().map(Self::new)
    }

    // ── Low-level Cypher execution ────────────────────────────────────────────

    fn tx_url(&self) -> String {
        // Commit transaction in a single request (auto-commit endpoint).
        format!(
            "{}/db/{}/tx/commit",
            self.config.http_base, self.config.database
        )
    }

    /// Execute one or more Cypher statements in a single auto-commit transaction.
    /// `statements` is a slice of `(cypher, parameters)` pairs.
    pub async fn cypher(&self, statements: &[(&str, Json)]) -> Result<Vec<Json>, String> {
        let stmt_array: Vec<Json> = statements
            .iter()
            .map(|(text, params)| json!({ "statement": text, "parameters": params }))
            .collect();
        self.post_statements(stmt_array, Neo4jAccessMode::Write)
            .await
    }

    /// POST a statement list to the auto-commit transaction endpoint.
    /// `access_mode` is sent as the HTTP API's `access-mode` header, so a
    /// READ transaction is refused by the server if any statement writes.
    async fn post_statements(
        &self,
        stmt_array: Vec<Json>,
        access_mode: Neo4jAccessMode,
    ) -> Result<Vec<Json>, String> {
        let body = json!({ "statements": stmt_array });

        let mut request = self
            .http
            .post(self.tx_url())
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json;charset=UTF-8");
        // WRITE is the server default, so the header is only sent for reads.
        if matches!(access_mode, Neo4jAccessMode::Read) {
            request = request.header("access-mode", "READ");
        }
        let resp = request
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Neo4j HTTP error: {e}"))?;

        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let parsed: Json = serde_json::from_str(&text)
            .map_err(|e| format!("Neo4j response decode failed: {e}"))?;

        // Check for Neo4j-level errors (distinct from HTTP errors).
        if let Some(errors) = parsed.get("errors").and_then(|e| e.as_array())
            && !errors.is_empty()
        {
            let msg = errors
                .iter()
                .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(format!("Neo4j Cypher error: {msg}"));
        }

        if !status.is_success() {
            return Err(format!("Neo4j HTTP [{status}]: {text}"));
        }

        // Extract result rows from each statement's result set.
        let results = parsed
            .get("results")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(results)
    }

    /// Execute a single Cypher statement and return all rows as JSON objects.
    async fn run_single(&self, cypher: &str, params: Json) -> Result<Vec<Json>, String> {
        let results = self.cypher(&[(cypher, params)]).await?;
        Ok(results
            .first()
            .map(Self::rows_from_result)
            .unwrap_or_default())
    }

    /// Parse one statement result-set (the `{columns, data:[{row:[...]}]}`
    /// shape the HTTP API returns) into a `Vec` of column-keyed JSON objects.
    /// Shared by `run_single` (above) and the canonical-store helpers below.
    fn rows_from_result(result: &Json) -> Vec<Json> {
        let columns = result
            .get("columns")
            .and_then(|c| c.as_array())
            .cloned()
            .unwrap_or_default();
        let Some(data) = result.get("data").and_then(|d| d.as_array()) else {
            return Vec::new();
        };
        data.iter()
            .map(|row| {
                let row_vals = row
                    .get("row")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut obj = serde_json::Map::new();
                for (col, val) in columns.iter().zip(row_vals.iter()) {
                    if let Some(name) = col.as_str() {
                        obj.insert(name.to_string(), val.clone());
                    }
                }
                Json::Object(obj)
            })
            .collect()
    }

    /// B.10b: run a single auto-commit Cypher statement with parameters and
    /// return all rows as column-keyed JSON objects.
    ///
    /// This is the single-statement entry point the `Neo4jCanonicalStore` uses
    /// (outbox enqueue, lease acquire/release, counter reads). It is a thin
    /// `pub(crate)` wrapper over the existing private `run_single`, so the
    /// canonical store does not depend on internal naming.
    pub(crate) async fn cypher_rows(
        &self,
        cypher: &str,
        params: Json,
    ) -> Result<Vec<Json>, String> {
        self.run_single(cypher, params).await
    }

    /// B.10b: run a LIST of Cypher statements atomically inside ONE HTTP
    /// transaction (the `/db/<db>/tx/commit` endpoint opens, runs every
    /// statement, and commits in a single request — all-or-nothing). Returns
    /// the parsed rows for EACH statement, in order, so a caller that needs the
    /// `RETURN` of a later statement (e.g. the outbox-seq counter) can read it.
    ///
    /// The canonical store needs atomic multi-statement transactions for the
    /// claims / audit-chain writes; this is the foundation. The existing
    /// auto-commit endpoint already provides the atomicity (a single POST of N
    /// statements commits as one transaction), so no new endpoint is needed.
    ///
    /// Phase 1's outbox enqueue is a single statement (the counter bump and the
    /// event CREATE are one Cypher statement), so this multi-statement helper is
    /// the FOUNDATION the phase-2 system-store traits (projection claims /
    /// audit-chain appends) will use — hence `allow(dead_code)` until then.
    #[allow(dead_code)]
    pub(crate) async fn cypher_tx_rows(
        &self,
        statements: &[(&str, Json)],
    ) -> Result<Vec<Vec<Json>>, String> {
        let results = self.cypher(statements).await?;
        Ok(results.iter().map(Self::rows_from_result).collect())
    }

    // ── Public graph API ──────────────────────────────────────────────────────

    /// Create or merge a node with a given label and properties.
    /// Uses MERGE on the `id` property to achieve idempotency.
    pub async fn create_node(&self, label: &str, id: &str, properties: Json) -> Result<(), String> {
        validate_neo4j_identifier(label)?;
        let cypher = format!("MERGE (n:{label} {{id: $id}}) SET n += $props RETURN n",);
        self.run_single(&cypher, json!({ "id": id, "props": properties }))
            .await?;
        Ok(())
    }

    /// Find nodes of a given label that match a filter (key/value pairs in `filter`).
    pub async fn find_nodes(
        &self,
        label: &str,
        filter: Json,
        limit: i64,
    ) -> Result<Vec<Json>, String> {
        validate_neo4j_identifier(label)?;
        let limit_clause = if limit > 0 {
            format!(" LIMIT {limit}")
        } else {
            String::new()
        };
        let cypher = format!(
            "MATCH (n:{label}) WHERE all(k IN keys($f) WHERE n[k] = $f[k]) RETURN n{limit_clause}"
        );
        self.run_single(&cypher, json!({ "f": filter })).await
    }

    /// Update a node (merge properties onto it). Returns how many nodes the
    /// MATCH found and updated (0 when the id does not exist).
    pub async fn update_node(
        &self,
        label: &str,
        id: &str,
        properties: Json,
    ) -> Result<u64, String> {
        validate_neo4j_identifier(label)?;
        let cypher =
            format!("MATCH (n:{label} {{id: $id}}) SET n += $props RETURN count(n) AS affected");
        let rows = self
            .run_single(&cypher, json!({ "id": id, "props": properties }))
            .await?;
        Ok(affected_from_count_rows(&rows))
    }

    /// Delete a node by id (DETACH DELETE removes all relationships).
    pub async fn delete_node(&self, label: &str, id: &str) -> Result<(), String> {
        validate_neo4j_identifier(label)?;
        let cypher = format!("MATCH (n:{label} {{id: $id}}) DETACH DELETE n");
        self.run_single(&cypher, json!({ "id": id })).await?;
        Ok(())
    }

    /// Create or merge a typed relationship between two nodes.
    pub async fn create_relationship(
        &self,
        from_label: &str,
        from_id: &str,
        to_label: &str,
        to_id: &str,
        rel_type: &str,
        properties: Json,
    ) -> Result<u64, String> {
        validate_neo4j_identifier(from_label)?;
        validate_neo4j_identifier(to_label)?;
        validate_neo4j_identifier(rel_type)?;
        let cypher = format!(
            "MATCH (a:{from_label} {{id: $from_id}}), (b:{to_label} {{id: $to_id}}) \
             MERGE (a)-[r:{rel_type}]->(b) SET r += $props RETURN count(r) AS affected"
        );
        let rows = self
            .run_single(
                &cypher,
                json!({ "from_id": from_id, "to_id": to_id, "props": properties }),
            )
            .await?;
        Ok(affected_from_count_rows(&rows))
    }

    /// Tenant/project-scoped node upsert: the MERGE key is `{id}` plus the
    /// scope fields (the same key the IR compiler writes), so two tenants'
    /// rows sharing an id never collapse into one node. A node written before
    /// scoping existed (no `_tenant_id`/`_project_id`) is adopted into the scope
    /// first, in the same transaction; otherwise the MERGE would create a twin
    /// that trips the unique-`id` constraint and the write could never land.
    pub async fn upsert_scoped_node(
        &self,
        label: &str,
        id: &str,
        properties: Json,
        scope: &GraphScope,
    ) -> Result<(), String> {
        validate_neo4j_identifier(label)?;
        if scope.is_empty() {
            return self.create_node(label, id, properties).await;
        }
        let (adopt, merge) = scoped_node_upsert_cypher(label, scope);
        let params = scope.params(json!({ "id": id, "props": properties }));
        self.cypher(&[(&adopt, params.clone()), (&merge, params)])
            .await?;
        Ok(())
    }

    /// Tenant/project-scoped node delete: only the node inside the scope is
    /// removed, never another tenant's node that shares the id.
    pub async fn delete_scoped_node(
        &self,
        label: &str,
        id: &str,
        scope: &GraphScope,
    ) -> Result<u64, String> {
        validate_neo4j_identifier(label)?;
        let cypher = format!(
            "MATCH (n:{label} {{id: $id{key}}}) DETACH DELETE n RETURN count(*) AS affected",
            key = scope.key_clause()
        );
        let rows = self
            .run_single(&cypher, scope.params(json!({ "id": id })))
            .await?;
        Ok(affected_from_count_rows(&rows))
    }

    /// Upsert the edge identified by `id`, from the node whose `id` is
    /// `from_id` to the node whose `id` is `to_id`. Both endpoints must lie in
    /// the edge's own scope, so an edge cannot join two tenants' nodes. When the
    /// row's endpoints changed, the edge that still connects the old endpoints is
    /// removed in the same statement. An endpoint that does not exist (yet) is an
    /// error, not a silent no-op: the projection task retries until the node
    /// projection that creates it has landed.
    pub async fn upsert_scoped_edge(
        &self,
        edge: &GraphEdge,
        scope: &GraphScope,
    ) -> Result<(), String> {
        let cypher = scoped_edge_upsert_cypher(edge, scope)?;
        let rows = self
            .run_single(
                &cypher,
                scope.params(json!({
                    "id": edge.id,
                    "from_id": edge.from_id,
                    "to_id": edge.to_id,
                    "props": edge.properties,
                })),
            )
            .await?;
        if rows.is_empty() {
            return Err(format!(
                "graph edge '{}' ({}) not written: endpoint node '{}' or '{}' does not exist in its tenant/project scope",
                edge.id, edge.rel_type, edge.from_id, edge.to_id
            ));
        }
        Ok(())
    }

    /// Delete the scoped edge identified by `id` (idempotent).
    pub async fn delete_scoped_edge(
        &self,
        rel_type: &str,
        id: &str,
        scope: &GraphScope,
    ) -> Result<u64, String> {
        validate_neo4j_identifier(rel_type)?;
        let cypher = format!(
            "MATCH ()-[r:{rel_type} {{id: $id{key}}}]->() DELETE r RETURN count(*) AS affected",
            key = scope.key_clause()
        );
        let rows = self
            .run_single(&cypher, scope.params(json!({ "id": id })))
            .await?;
        Ok(affected_from_count_rows(&rows))
    }
}

/// The `{_tenant_id, _project_id}` system fields a graph record is scoped by.
/// Only these two names are accepted, so request JSON never reaches the Cypher
/// text; their values travel as parameters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphScope {
    fields: Vec<(&'static str, String)>,
}

const GRAPH_SCOPE_FIELDS: [&str; 2] = ["_tenant_id", "_project_id"];

impl GraphScope {
    /// Parse the optional `scope` object of a mutation request.
    pub fn from_request(scope: Option<&Json>) -> Result<Self, String> {
        let Some(scope) = scope.filter(|s| !s.is_null()) else {
            return Ok(Self::default());
        };
        let map = scope
            .as_object()
            .ok_or_else(|| "graph scope must be an object".to_string())?;
        if let Some(unknown) = map
            .keys()
            .find(|key| !GRAPH_SCOPE_FIELDS.contains(&key.as_str()))
        {
            return Err(format!(
                "graph scope field '{unknown}' is not one of _tenant_id/_project_id"
            ));
        }
        let mut fields = Vec::new();
        for field in GRAPH_SCOPE_FIELDS {
            match map.get(field) {
                None | Some(Json::Null) => {}
                Some(Json::String(value)) if !value.trim().is_empty() => {
                    fields.push((field, value.clone()));
                }
                Some(_) => {
                    return Err(format!(
                        "graph scope field '{field}' must be a non-empty string"
                    ));
                }
            }
        }
        Ok(Self { fields })
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// `, _tenant_id: $scope_tenant_id, ...` appended inside a `{id: $id ...}`
    /// property-map key.
    fn key_clause(&self) -> String {
        self.fields
            .iter()
            .map(|(field, _)| format!(", {field}: $scope{field}"))
            .collect()
    }

    /// `base` plus one `scope<field>` parameter per field and the whole scope as
    /// `$scope` (for `SET n += $scope`).
    fn params(&self, mut base: Json) -> Json {
        if let Json::Object(map) = &mut base {
            let mut scope = serde_json::Map::new();
            for (field, value) in &self.fields {
                map.insert(format!("scope{field}"), Json::String(value.clone()));
                scope.insert((*field).to_string(), Json::String(value.clone()));
            }
            map.insert("scope".to_string(), Json::Object(scope));
        }
        base
    }
}

/// One projected relationship.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphEdge {
    pub rel_type: String,
    pub id: String,
    pub from_id: String,
    pub to_id: String,
    pub from_label: Option<String>,
    pub to_label: Option<String>,
    pub properties: Json,
}

/// Cypher for a label's tenant-composite uniqueness: nodes are unique on
/// `(prop, _tenant_id, _project_id)` — the same key the IR compiler and the
/// scoped projection MERGE on — never on `prop` alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedUniquenessStatements {
    drop_legacy: String,
    composite_unique: String,
    fallback_index: String,
    drop_composite: String,
    drop_fallback_index: String,
}

/// `label` and `prop` must already be validated identifiers.
fn scoped_uniqueness_statements(label: &str, prop: &str) -> ScopedUniquenessStatements {
    let legacy = format!("udb_{label}_{prop}_unique");
    let composite = format!("udb_{label}_{prop}_scope_unique");
    let index = format!("udb_{label}_{prop}_scope_idx");
    ScopedUniquenessStatements {
        drop_legacy: format!("DROP CONSTRAINT {legacy} IF EXISTS"),
        composite_unique: format!(
            "CREATE CONSTRAINT {composite} IF NOT EXISTS \
             FOR (n:{label}) REQUIRE (n.{prop}, n._tenant_id, n._project_id) IS UNIQUE"
        ),
        fallback_index: format!(
            "CREATE INDEX {index} IF NOT EXISTS \
             FOR (n:{label}) ON (n.{prop}, n._tenant_id, n._project_id)"
        ),
        drop_composite: format!("DROP CONSTRAINT {composite} IF EXISTS"),
        drop_fallback_index: format!("DROP INDEX {index} IF EXISTS"),
    }
}

/// `(adopt legacy node, scoped MERGE)` statements for [`Neo4jExecutor::upsert_scoped_node`].
fn scoped_node_upsert_cypher(label: &str, scope: &GraphScope) -> (String, String) {
    let unscoped = GRAPH_SCOPE_FIELDS
        .iter()
        .map(|field| format!("n.{field} IS NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    (
        format!(
            "MATCH (n:{label} {{id: $id}}) WHERE {unscoped} SET n += $scope RETURN count(n) AS adopted"
        ),
        format!(
            "MERGE (n:{label} {{id: $id{key}}}) SET n += $props RETURN n",
            key = scope.key_clause()
        ),
    )
}

fn scoped_edge_upsert_cypher(edge: &GraphEdge, scope: &GraphScope) -> Result<String, String> {
    validate_neo4j_identifier(&edge.rel_type)?;
    let label = |label: &Option<String>| -> Result<String, String> {
        match label.as_deref().map(str::trim).filter(|l| !l.is_empty()) {
            Some(label) => {
                validate_neo4j_identifier(label)?;
                Ok(format!(":{label}"))
            }
            None => Ok(String::new()),
        }
    };
    let key = scope.key_clause();
    Ok(format!(
        "MATCH (a{from} {{id: $from_id{key}}}), (b{to} {{id: $to_id{key}}}) \
         OPTIONAL MATCH ()-[old:{rel} {{id: $id{key}}}]->() \
         WHERE startNode(old) <> a OR endNode(old) <> b \
         DELETE old \
         WITH DISTINCT a, b \
         MERGE (a)-[r:{rel} {{id: $id{key}}}]->(b) SET r += $props RETURN r.id AS id",
        from = label(&edge.from_label)?,
        to = label(&edge.to_label)?,
        rel = edge.rel_type,
    ))
}

// ── BackendExecutor (runtime trait) ─────────────────────────────────────────────
// Phase D: Neo4jExecutor as stateless leaf I/O. Request shapes mirror the former
// inline arms in core.rs (`query_backend_target` / `mutate_backend_target`).

impl BackendHealth for Neo4jExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.run_single("RETURN 1 AS ok", json!({})).await?;
        Ok(())
    }
}

impl QueryExecutor for Neo4jExecutor {
    /// `{"cypher":"MATCH ...","parameters":{...}}` or
    /// `{"label":"L","filter":{...},"limit":N}`.
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json =
            serde_json::from_str(request_json).map_err(invalid_neo4j_request_json_status)?;
        let rows = if let Some(cypher) = spec.get("cypher").and_then(Json::as_str) {
            // The query path is read-only: refuse a write clause up front and
            // run the statement in a READ transaction so the server refuses
            // any write the lexical check could not see.
            if let Some(clause) = cypher_write_clause(cypher) {
                return Err(neo4j_invalid_field_status(
                    "cypher",
                    "query (read) Cypher must not contain write clauses or write procedures; use a mutation",
                    format!("Neo4j query refused: read statement contains write clause '{clause}'"),
                ));
            }
            let params = spec.get("parameters").cloned().unwrap_or_else(|| json!({}));
            let results = self
                .post_statements(
                    vec![json!({ "statement": cypher, "parameters": params })],
                    Neo4jAccessMode::Read,
                )
                .await
                .map_err(|err| neo4j_internal_status("query_cypher", err))?;
            results
                .first()
                .map(Self::rows_from_result)
                .unwrap_or_default()
        } else {
            let label = spec
                .get("label")
                .and_then(Json::as_str)
                .ok_or_else(|| neo4j_required_field_status("label"))?;
            let filter = spec.get("filter").cloned().unwrap_or_else(|| json!({}));
            let limit = spec.get("limit").and_then(Json::as_i64).unwrap_or(100);
            self.find_nodes(label, filter, limit)
                .await
                .map_err(|err| neo4j_internal_status("find_nodes", err))?
        };
        encode_neo4j_response(&rows, "query_response_encode")
    }
}

impl MutationExecutor for Neo4jExecutor {
    /// `{"operation":"cypher|create_node|upsert_node|update_node|delete_node|create_relationship|upsert_relationship|upsert_edge|delete_edge", ...}`.
    /// `create_node`/`upsert_node`/`delete_node`/`upsert_edge`/`delete_edge`
    /// accept an optional `scope` (`{_tenant_id, _project_id}`) that keys the
    /// record, so a scoped write or delete never reaches another tenant's node.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json =
            serde_json::from_str(request_json).map_err(invalid_neo4j_request_json_status)?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .ok_or_else(|| neo4j_required_field_status("operation"))?;
        let req_str = |key: &str| -> Result<String, tonic::Status> {
            spec.get(key)
                .and_then(Json::as_str)
                .map(|s| s.to_string())
                .ok_or_else(|| neo4j_required_field_status(key))
        };
        match operation {
            "cypher" => {
                let cypher = req_str("cypher")?;
                let params = spec.get("parameters").cloned().unwrap_or_else(|| json!({}));
                // `includeStats` makes the server report what the statement
                // changed, so the affected count is real rather than assumed.
                let results = self
                    .post_statements(
                        vec![json!({
                            "statement": cypher,
                            "parameters": params,
                            "includeStats": true,
                        })],
                        Neo4jAccessMode::Write,
                    )
                    .await
                    .map_err(|err| neo4j_internal_status("mutate_cypher", err))?;
                let first = results.first();
                let rows = first.map(Self::rows_from_result).unwrap_or_default();
                let stats = first.and_then(|result| result.get("stats"));
                let affected = neo4j_affected_rows(stats, rows.len());
                serde_json::to_string(&json!({
                    "affected_rows": affected,
                    "stats": stats.cloned().unwrap_or(Json::Null),
                    "results": results,
                }))
                .map_err(|err| neo4j_internal_status("mutate_response_encode", err.to_string()))
            }
            "create_node" | "upsert_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                let properties = spec
                    .get("properties")
                    .or_else(|| spec.get("props"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let scope = request_scope(&spec)?;
                self.upsert_scoped_node(&label, &id, properties, &scope)
                    .await
                    .map_err(|err| neo4j_internal_status("create_node", err))?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            "update_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                let properties = spec.get("properties").cloned().unwrap_or_else(|| json!({}));
                let affected = self
                    .update_node(&label, &id, properties)
                    .await
                    .map_err(|err| neo4j_internal_status("update_node", err))?;
                Ok(json!({ "affected_rows": affected }).to_string())
            }
            "delete_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                let scope = request_scope(&spec)?;
                let affected = self
                    .delete_scoped_node(&label, &id, &scope)
                    .await
                    .map_err(|err| neo4j_internal_status("delete_node", err))?;
                Ok(json!({ "affected_rows": affected }).to_string())
            }
            "upsert_edge" => {
                let opt_str = |key: &str| {
                    spec.get(key)
                        .and_then(Json::as_str)
                        .map(ToString::to_string)
                };
                let edge = GraphEdge {
                    rel_type: req_str("rel_type")?,
                    id: req_str("id")?,
                    from_id: req_str("from_id")?,
                    to_id: req_str("to_id")?,
                    from_label: opt_str("from_label"),
                    to_label: opt_str("to_label"),
                    properties: spec.get("properties").cloned().unwrap_or_else(|| json!({})),
                };
                let scope = request_scope(&spec)?;
                self.upsert_scoped_edge(&edge, &scope)
                    .await
                    .map_err(|err| neo4j_internal_status("upsert_edge", err))?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            "delete_edge" => {
                let rel_type = req_str("rel_type")?;
                let id = req_str("id")?;
                let scope = request_scope(&spec)?;
                let affected = self
                    .delete_scoped_edge(&rel_type, &id, &scope)
                    .await
                    .map_err(|err| neo4j_internal_status("delete_edge", err))?;
                Ok(json!({ "affected_rows": affected }).to_string())
            }
            "create_relationship" | "upsert_relationship" => {
                let from_label = req_str("from_label")?;
                let from_id = req_str("from_id")?;
                let to_label = req_str("to_label")?;
                let to_id = req_str("to_id")?;
                let rel_type = req_str("rel_type")?;
                let properties = spec.get("properties").cloned().unwrap_or_else(|| json!({}));
                let affected = self
                    .create_relationship(
                        &from_label,
                        &from_id,
                        &to_label,
                        &to_id,
                        &rel_type,
                        properties,
                    )
                    .await
                    .map_err(|err| neo4j_internal_status("create_relationship", err))?;
                Ok(json!({ "affected_rows": affected }).to_string())
            }
            other => Err(unsupported_neo4j_operation_status(other)),
        }
    }
}

impl SearchExecutor for Neo4jExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "neo4j",
            "search",
            "vector_search",
            "neo4j does not support generic vector search dispatch",
        ))
    }
}

impl ObjectExecutor for Neo4jExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "neo4j",
            "get_object",
            "object_store",
            "neo4j is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "neo4j",
            "put_object",
            "object_store",
            "neo4j is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for Neo4jExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        validate_neo4j_identifier(resource_name)
            .map_err(|err| neo4j_identifier_status("resource_name", err))?;
        let spec: Json = serde_json::from_str(spec_json).unwrap_or(json!({}));
        let prop = spec
            .get("constraint_property")
            .and_then(|v| v.as_str())
            .unwrap_or("id");
        validate_neo4j_identifier(prop)
            .map_err(|err| neo4j_identifier_status("constraint_property", err))?;
        let statements = scoped_uniqueness_statements(resource_name, prop);
        // A global `id` constraint from an earlier release would reject a
        // second tenant's node with the same id; drop it first.
        self.run_single(&statements.drop_legacy, json!({}))
            .await
            .map_err(|err| neo4j_internal_status("ensure_resource", err))?;
        match self
            .run_single(&statements.composite_unique, json!({}))
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                // Composite property uniqueness needs Neo4j 5 (Community or
                // Enterprise). On an older server keep the scoped MERGE key
                // as the only dedup guard and index the composite key so
                // scoped lookups stay indexed; never fall back to a global
                // `id` constraint, which would break multi-tenant writes.
                tracing::warn!(
                    resource = %resource_name,
                    error = %err,
                    "Neo4j rejected the tenant-composite uniqueness constraint; \
                     falling back to a composite index (uniqueness is then enforced \
                     only by the scoped MERGE key)"
                );
                self.run_single(&statements.fallback_index, json!({}))
                    .await
                    .map(|_| ())
                    .map_err(|err| neo4j_internal_status("ensure_resource", err))
            }
        }
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        validate_neo4j_identifier(resource_name)
            .map_err(|err| neo4j_identifier_status("resource_name", err))?;
        let statements = scoped_uniqueness_statements(resource_name, "id");
        for cypher in [
            statements.drop_legacy,
            statements.drop_composite,
            statements.drop_fallback_index,
        ] {
            self.run_single(&cypher, json!({}))
                .await
                .map_err(|err| neo4j_internal_status("drop_resource", err))?;
        }
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = self
            .run_single("SHOW CONSTRAINTS WHERE name STARTS WITH 'udb_'", json!({}))
            .await
            .map_err(|err| neo4j_internal_status("list_resources", err))?;
        let names = rows
            .iter()
            .filter_map(|r| r.get("name").and_then(|v| v.as_str()))
            .map(|s| s.to_string())
            .collect();
        Ok(names)
    }
}

impl BackendExecutor for Neo4jExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "neo4j",
            "transaction",
            "transactions",
            "neo4j transactions are not exposed via generic dispatch",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "neo4j",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn test_executor() -> Neo4jExecutor {
        Neo4jExecutor::new(Neo4jConfig {
            http_base: "http://localhost:7474".to_string(),
            username: "neo4j".to_string(),
            password: "secret".to_string(),
            database: "neo4j".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        })
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
        assert_eq!(detail.backend, "neo4j");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn tenant_scope() -> GraphScope {
        GraphScope::from_request(Some(&json!({"_tenant_id": "t1", "_project_id": "p1"})))
            .expect("valid scope")
    }

    #[test]
    fn read_gate_rejects_write_clauses_and_write_procedures() {
        for write in [
            "CREATE (n:Customer {id: 1})",
            "MATCH (n) DETACH DELETE n",
            "match (n) set n.x = 1 return n",
            "MERGE (n:A {id: $id}) RETURN n",
            "MATCH (n) REMOVE n:Label",
            "DROP CONSTRAINT c IF EXISTS",
            "MATCH (n) CALL { WITH n DELETE n } RETURN 1",
            "CALL apoc.create.node(['A'], {}) YIELD node RETURN node",
            "CALL dbms.security.createUser('x', 'y', false)",
            "LOAD CSV FROM 'file:///x.csv' AS row RETURN row",
            "MATCH (n) FOREACH (x IN [1] | SET n.y = x)",
        ] {
            assert!(
                cypher_write_clause(write).is_some(),
                "write statement must be refused on the read path: {write}"
            );
        }
        for read in [
            "MATCH (n:Customer) WHERE n.`_tenant_id` = $p0 RETURN n",
            // Write words inside strings, comments, backticks, properties,
            // parameters, labels and map keys are names, not clauses.
            "MATCH (n) WHERE n.name = 'CREATE something; DELETE' RETURN n.set, $delete",
            "MATCH (n:`MERGE`) // DELETE everything\n RETURN n {set: n.x}",
            "MATCH (n) RETURN n.created_at /* SET */ AS created",
            "CALL db.index.fulltext.queryNodes('Customer_fulltext', $p0) YIELD node, score RETURN node",
            "CALL db.labels() YIELD label RETURN label",
            "MATCH (n) CALL { WITH n RETURN count(*) AS c } RETURN c",
        ] {
            assert_eq!(
                cypher_write_clause(read),
                None,
                "read statement refused: {read}"
            );
        }
    }

    #[tokio::test]
    async fn query_path_refuses_write_cypher_before_any_io() {
        let exec = test_executor();
        let err = QueryExecutor::query(
            &exec,
            &json!({"cypher": "MATCH (n) DETACH DELETE n", "parameters": {}}).to_string(),
        )
        .await
        .expect_err("a write on the read path must be refused");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_single_field(&err, "cypher");
    }

    #[test]
    fn affected_rows_come_from_server_counters_and_returned_rows() {
        // Pure property update: counters carry no entity count, rows do.
        let update = json!({"contains_updates": true, "properties_set": 2});
        assert_eq!(neo4j_affected_rows(Some(&update), 1), 1);
        // Deletes report through the counters (DETACH DELETE returns no row).
        let delete =
            json!({"contains_updates": true, "nodes_deleted": 3, "relationship_deleted": 2});
        assert_eq!(neo4j_affected_rows(Some(&delete), 0), 5);
        // A statement that changed nothing is 0, even when it returned rows.
        let noop = json!({"contains_updates": false});
        assert_eq!(neo4j_affected_rows(Some(&noop), 4), 0);
        assert_eq!(neo4j_affected_rows(None, 4), 0);
        assert_eq!(affected_from_count_rows(&[json!({"affected": 0})]), 0);
        assert_eq!(affected_from_count_rows(&[json!({"affected": 2})]), 2);
        assert_eq!(affected_from_count_rows(&[]), 0);
    }

    #[test]
    fn ensure_resource_uniqueness_is_tenant_composite() {
        let s = scoped_uniqueness_statements("Customer", "id");
        assert_eq!(
            s.drop_legacy,
            "DROP CONSTRAINT udb_Customer_id_unique IF EXISTS"
        );
        assert!(
            s.composite_unique.ends_with(
                "FOR (n:Customer) REQUIRE (n.id, n._tenant_id, n._project_id) IS UNIQUE"
            ),
            "{}",
            s.composite_unique
        );
        assert!(!s.composite_unique.contains("REQUIRE n.id IS UNIQUE"));
        assert!(
            s.fallback_index
                .contains("ON (n.id, n._tenant_id, n._project_id)")
        );
    }

    #[test]
    fn graph_scope_accepts_only_the_two_system_fields() {
        assert!(GraphScope::from_request(None).unwrap().is_empty());
        assert!(
            GraphScope::from_request(Some(&json!({"_tenant_id": "t1"})))
                .unwrap()
                .key_clause()
                .contains("_tenant_id: $scope_tenant_id")
        );
        // A caller-chosen key would otherwise be spliced into the Cypher text.
        let err =
            GraphScope::from_request(Some(&json!({"id}) DETACH DELETE n //": "x"}))).unwrap_err();
        assert!(err.contains("not one of _tenant_id/_project_id"), "{err}");
        assert!(GraphScope::from_request(Some(&json!({"_tenant_id": 7}))).is_err());
    }

    #[test]
    fn scoped_node_merge_keys_on_tenant_and_project() {
        let (adopt, merge) = scoped_node_upsert_cypher("Patient", &tenant_scope());
        assert_eq!(
            merge,
            "MERGE (n:Patient {id: $id, _tenant_id: $scope_tenant_id, _project_id: $scope_project_id}) SET n += $props RETURN n"
        );
        // Only an UNSCOPED legacy node is adopted — never another tenant's node.
        assert!(adopt.contains("n._tenant_id IS NULL AND n._project_id IS NULL"));
        let params = tenant_scope().params(json!({"id": "a"}));
        assert_eq!(params["scope_tenant_id"], "t1");
        assert_eq!(
            params["scope"],
            json!({"_tenant_id": "t1", "_project_id": "p1"})
        );
    }

    #[test]
    fn scoped_edge_matches_both_endpoints_inside_the_scope() {
        let edge = GraphEdge {
            rel_type: "TREATS".into(),
            id: "e1".into(),
            from_id: "doc-1".into(),
            to_id: "pat-1".into(),
            from_label: None,
            to_label: Some("Patient".into()),
            properties: json!({}),
        };
        let cypher = scoped_edge_upsert_cypher(&edge, &tenant_scope()).unwrap();
        assert!(cypher.starts_with(
            "MATCH (a {id: $from_id, _tenant_id: $scope_tenant_id, _project_id: $scope_project_id}), \
             (b:Patient {id: $to_id, _tenant_id: $scope_tenant_id, _project_id: $scope_project_id})"
        ));
        assert!(cypher.contains("WHERE startNode(old) <> a OR endNode(old) <> b DELETE old"));
        assert!(cypher.contains(
            "MERGE (a)-[r:TREATS {id: $id, _tenant_id: $scope_tenant_id, _project_id: $scope_project_id}]->(b)"
        ));
        let bad = GraphEdge {
            rel_type: "TREATS]->() DETACH DELETE a //".into(),
            ..edge
        };
        assert!(scoped_edge_upsert_cypher(&bad, &tenant_scope()).is_err());
    }

    #[test]
    fn neo4j_internal_status_carries_typed_detail() {
        let status = neo4j_internal_status("find_nodes", "Neo4j HTTP request failed: closed");
        assert_internal_detail(&status, "find_nodes", "Neo4j HTTP request failed: closed");

        let status = neo4j_internal_status("ensure_resource", "Neo4j returned errors");
        assert_internal_detail(&status, "ensure_resource", "Neo4j returned errors");
    }

    #[test]
    fn neo4j_executor_kind_and_name() {
        let cfg = Neo4jConfig {
            http_base: "http://localhost:7474".to_string(),
            username: "neo4j".to_string(),
            password: "secret".to_string(),
            database: "neo4j".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        };
        let exec = Neo4jExecutor::new(cfg);
        assert_eq!(exec.kind(), BackendKind::Neo4j);
        assert_eq!(exec.name(), "Neo4j");
    }

    #[tokio::test]
    async fn neo4j_query_validation_carries_field_violations() {
        let exec = test_executor();

        let invalid_json = QueryExecutor::query(&exec, "not json").await.unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid request json:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_label = QueryExecutor::query(&exec, "{}").await.unwrap_err();
        assert_eq!(missing_label.message(), "missing required field 'label'");
        assert_single_field(&missing_label, "label");
    }

    #[tokio::test]
    async fn neo4j_mutation_validation_carries_field_violations() {
        let exec = test_executor();

        let invalid_json = MutationExecutor::mutate(&exec, "not json")
            .await
            .unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid request json:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_operation = MutationExecutor::mutate(&exec, "{}").await.unwrap_err();
        assert_eq!(
            missing_operation.message(),
            "missing required field 'operation'"
        );
        assert_single_field(&missing_operation, "operation");

        let missing_cypher = MutationExecutor::mutate(&exec, r#"{"operation":"cypher"}"#)
            .await
            .unwrap_err();
        assert_eq!(missing_cypher.message(), "missing required field 'cypher'");
        assert_single_field(&missing_cypher, "cypher");

        let unsupported = MutationExecutor::mutate(&exec, r#"{"operation":"bogus"}"#)
            .await
            .unwrap_err();
        assert_eq!(
            unsupported.message(),
            "unsupported Neo4j mutation operation 'bogus'"
        );
        assert_single_field(&unsupported, "operation");
    }

    #[tokio::test]
    async fn neo4j_resource_identifier_validation_carries_field_violations() {
        let exec = test_executor();

        let invalid_resource = ResourceAdminExecutor::ensure_resource(
            &exec,
            "bad-name",
            r#"{"constraint_property":"id"}"#,
        )
        .await
        .unwrap_err();
        assert_eq!(
            invalid_resource.message(),
            "Neo4j identifier 'bad-name' contains invalid characters; only ASCII letters, digits, and underscores are allowed"
        );
        assert_single_field(&invalid_resource, "resource_name");

        let invalid_property = ResourceAdminExecutor::ensure_resource(
            &exec,
            "GoodLabel",
            r#"{"constraint_property":"1bad"}"#,
        )
        .await
        .unwrap_err();
        assert_eq!(
            invalid_property.message(),
            "Neo4j identifier '1bad' must start with a letter or underscore"
        );
        assert_single_field(&invalid_property, "constraint_property");
    }

    #[tokio::test]
    async fn neo4j_backend_executor_rejects_unsupported_and_malformed() {
        let exec = test_executor();
        assert!(SearchExecutor::search(&exec, "{}").await.is_err());
        assert!(ObjectExecutor::get_object(&exec, "{}").await.is_err());
        assert!(BackendExecutor::transaction(&exec, "{}").await.is_err());
        assert!(QueryExecutor::query(&exec, "not json").await.is_err());
        // missing operation
        assert!(MutationExecutor::mutate(&exec, "{}").await.is_err());
        // unknown operation
        assert!(
            MutationExecutor::mutate(&exec, r#"{"operation":"bogus"}"#)
                .await
                .is_err()
        );
    }

    #[test]
    fn neo4j_tx_url_correct() {
        let cfg = Neo4jConfig {
            http_base: "http://localhost:7474".to_string(),
            username: "neo4j".to_string(),
            password: "".to_string(),
            database: "neo4j".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        };
        let exec = Neo4jExecutor::new(cfg);
        assert_eq!(exec.tx_url(), "http://localhost:7474/db/neo4j/tx/commit");
    }

    #[test]
    fn neo4j_config_bolt_to_http() {
        let http = Neo4jConfig::http_base_from_dsn("bolt://user:pass@graph.example.com:7687");
        assert_eq!(http, "http://graph.example.com:7474");
    }

    #[test]
    fn neo4j_config_http_passthrough() {
        let http = Neo4jConfig::http_base_from_dsn("http://auradb.neo4j.io:7474");
        assert_eq!(http, "http://auradb.neo4j.io:7474");
    }

    #[test]
    fn neo4j_config_from_env_returns_none_without_vars() {
        unsafe {
            env::remove_var("UDB_GRAPH_DSN");
            env::remove_var("UDB_GRAPH_HTTP_URL");
        }
        assert!(Neo4jConfig::from_env().is_none());
    }
}
