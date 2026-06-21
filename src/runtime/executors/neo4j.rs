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
use crate::runtime::executor_utils::build_probe;
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

// ── Configuration ─────────────────────────────────────────────────────────────

/// Connection configuration for Neo4j HTTP Transactional API.
#[derive(Debug, Clone)]
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
        // C7/C8: the Neo4j IR compiler now ANDs `n._tenant_id = $ctx_tenant_id`
        // / `n._project_id = $ctx_project_id` into every MATCH WHERE
        // clause AND stamps them into the MERGE key of every write.
        // Tenant boundary is protocol-enforced.
        crate::runtime::backend_context::enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id stamped in MERGE key; ANDed into MATCH WHERE",
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
        let body = json!({ "statements": stmt_array });

        let resp = self
            .http
            .post(self.tx_url())
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json;charset=UTF-8")
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

    /// Update a node (merge properties onto it).
    pub async fn update_node(&self, label: &str, id: &str, properties: Json) -> Result<(), String> {
        validate_neo4j_identifier(label)?;
        let cypher = format!("MATCH (n:{label} {{id: $id}}) SET n += $props RETURN n");
        self.run_single(&cypher, json!({ "id": id, "props": properties }))
            .await?;
        Ok(())
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
    ) -> Result<(), String> {
        validate_neo4j_identifier(from_label)?;
        validate_neo4j_identifier(to_label)?;
        validate_neo4j_identifier(rel_type)?;
        let cypher = format!(
            "MATCH (a:{from_label} {{id: $from_id}}), (b:{to_label} {{id: $to_id}}) \
             MERGE (a)-[r:{rel_type}]->(b) SET r += $props RETURN r"
        );
        self.run_single(
            &cypher,
            json!({ "from_id": from_id, "to_id": to_id, "props": properties }),
        )
        .await?;
        Ok(())
    }
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
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let rows = if let Some(cypher) = spec.get("cypher").and_then(Json::as_str) {
            let params = spec.get("parameters").cloned().unwrap_or_else(|| json!({}));
            self.cypher(&[(cypher, params)])
                .await
                .map_err(tonic::Status::internal)?
        } else {
            let label = spec
                .get("label")
                .and_then(Json::as_str)
                .ok_or_else(|| tonic::Status::invalid_argument("missing required field 'label'"))?;
            let filter = spec.get("filter").cloned().unwrap_or_else(|| json!({}));
            let limit = spec.get("limit").and_then(Json::as_i64).unwrap_or(100);
            self.find_nodes(label, filter, limit)
                .await
                .map_err(tonic::Status::internal)?
        };
        serde_json::to_string(&rows).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for Neo4jExecutor {
    /// `{"operation":"cypher|create_node|upsert_node|update_node|delete_node|create_relationship|upsert_relationship", ...}`.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: Json = serde_json::from_str(request_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid request json: {e}")))?;
        let operation = spec
            .get("operation")
            .and_then(Json::as_str)
            .ok_or_else(|| tonic::Status::invalid_argument("missing required field 'operation'"))?;
        let req_str = |key: &str| -> Result<String, tonic::Status> {
            spec.get(key)
                .and_then(Json::as_str)
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    tonic::Status::invalid_argument(format!("missing required field '{key}'"))
                })
        };
        match operation {
            "cypher" => {
                let cypher = req_str("cypher")?;
                let params = spec.get("parameters").cloned().unwrap_or_else(|| json!({}));
                let rows = self
                    .cypher(&[(&cypher, params)])
                    .await
                    .map_err(tonic::Status::internal)?;
                serde_json::to_string(&rows).map_err(|e| tonic::Status::internal(e.to_string()))
            }
            "create_node" | "upsert_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                let properties = spec
                    .get("properties")
                    .or_else(|| spec.get("props"))
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                self.create_node(&label, &id, properties)
                    .await
                    .map_err(tonic::Status::internal)?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            "update_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                let properties = spec.get("properties").cloned().unwrap_or_else(|| json!({}));
                self.update_node(&label, &id, properties)
                    .await
                    .map_err(tonic::Status::internal)?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            "delete_node" => {
                let label = req_str("label")?;
                let id = req_str("id")?;
                self.delete_node(&label, &id)
                    .await
                    .map_err(tonic::Status::internal)?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            "create_relationship" | "upsert_relationship" => {
                let from_label = req_str("from_label")?;
                let from_id = req_str("from_id")?;
                let to_label = req_str("to_label")?;
                let to_id = req_str("to_id")?;
                let rel_type = req_str("rel_type")?;
                let properties = spec.get("properties").cloned().unwrap_or_else(|| json!({}));
                self.create_relationship(
                    &from_label,
                    &from_id,
                    &to_label,
                    &to_id,
                    &rel_type,
                    properties,
                )
                .await
                .map_err(tonic::Status::internal)?;
                Ok(r#"{"affected_rows":1}"#.to_string())
            }
            other => Err(tonic::Status::invalid_argument(format!(
                "unsupported Neo4j mutation operation '{other}'"
            ))),
        }
    }
}

impl SearchExecutor for Neo4jExecutor {
    async fn search(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "neo4j does not support generic vector search dispatch",
        ))
    }
}

impl ObjectExecutor for Neo4jExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "neo4j is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
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
        validate_neo4j_identifier(resource_name).map_err(tonic::Status::internal)?;
        let spec: Json = serde_json::from_str(spec_json).unwrap_or(json!({}));
        let prop = spec
            .get("constraint_property")
            .and_then(|v| v.as_str())
            .unwrap_or("id");
        validate_neo4j_identifier(prop).map_err(tonic::Status::internal)?;
        let constraint_name = format!("udb_{resource_name}_{prop}_unique");
        let cypher = format!(
            "CREATE CONSTRAINT {constraint_name} IF NOT EXISTS \
             FOR (n:{resource_name}) REQUIRE n.{prop} IS UNIQUE"
        );
        self.run_single(&cypher, json!({}))
            .await
            .map(|_| ())
            .map_err(tonic::Status::internal)
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        validate_neo4j_identifier(resource_name).map_err(tonic::Status::internal)?;
        let cypher = format!("DROP CONSTRAINT udb_{resource_name}_id_unique IF EXISTS");
        self.run_single(&cypher, json!({}))
            .await
            .map(|_| ())
            .map_err(tonic::Status::internal)
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let rows = self
            .run_single("SHOW CONSTRAINTS WHERE name STARTS WITH 'udb_'", json!({}))
            .await
            .map_err(tonic::Status::internal)?;
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
        Err(tonic::Status::failed_precondition(
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
    async fn neo4j_backend_executor_rejects_unsupported_and_malformed() {
        let exec = Neo4jExecutor::new(Neo4jConfig {
            http_base: "http://localhost:7474".to_string(),
            username: "neo4j".to_string(),
            password: "secret".to_string(),
            database: "neo4j".to_string(),
            is_cloud: false,
            dev_mode: true,
            timeout_secs: 30,
        });
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
