//! Cassandra / ScyllaDB CQL executor (C9). Real driver via `scylla`
//! crate. The same driver works against both Apache Cassandra and
//! ScyllaDB — both speak CQL over the binary protocol.
//!
//! ## Dispatch contract
//!
//! Same JSON shape as the other SQL backends:
//! ```json
//! { "sql": "SELECT * FROM \"ks\".\"t\" WHERE \"pk\" = ?",
//!   "params": ["abc"] }
//! ```
//!
//! The driver uses positional `?` placeholders. The compiler already
//! emits CQL with the right shape; the executor handles binding +
//! row decoding + error mapping.

use std::sync::Arc;

use scylla::frame::response::result::CqlValue;
use scylla::{Session, SessionBuilder};
use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{build_probe, parse_sql_dispatch};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

/// Wraps a `scylla::Session`. The session manages its own connection
/// pool internally (one connection per shard for ScyllaDB / per node
/// for Cassandra), so we just need a single shared session.
#[derive(Clone)]
pub struct CassandraClient {
    session: Arc<Session>,
}

impl std::fmt::Debug for CassandraClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraClient").finish()
    }
}

impl CassandraClient {
    /// Open a session from the given contact-point DSN. Format:
    ///   `host:9042` (no auth)
    ///   `user:pass@host:9042` (PasswordAuthenticator)
    ///   `host1:9042,host2:9042` (multi-node bootstrap)
    pub async fn connect(dsn: &str) -> Result<Self, String> {
        let (auth, nodes_part) = match dsn.split_once('@') {
            Some((auth, rest)) => (Some(auth), rest),
            None => (None, dsn),
        };
        let nodes: Vec<&str> = nodes_part.split(',').map(str::trim).collect();
        let mut builder = SessionBuilder::new();
        for node in nodes {
            builder = builder.known_node(node);
        }
        if let Some(auth) = auth
            && let Some((user, pass)) = auth.split_once(':')
        {
            builder = builder.user(user, pass);
        }
        let session = builder
            .build()
            .await
            .map_err(|e| format!("cassandra session build failed: {e}"))?;
        Ok(Self {
            session: Arc::new(session),
        })
    }

    pub async fn ping(&self) -> Result<(), String> {
        self.session
            .query("SELECT release_version FROM system.local", ())
            .await
            .map(|_| ())
            .map_err(|e| format!("cassandra ping failed: {e}"))
    }

    // ── B.10a — CQL helpers for the canonical store ─────────────────────────
    //
    // The canonical store (`runtime/canonical_store/cassandra.rs`) needs to run
    // CQL directly — DDL, parameterized reads, and LWTs whose `[applied]`
    // boolean must be inspected. Rather than re-open a second `Session`, it
    // borrows this client's session through these thin `pub(crate)` helpers,
    // mirroring how `MssqlClient` exposed `fetch_rows`/`execute_sql` and
    // `MongoDbExecutor` exposed `native_database()`.

    /// Run a CQL statement (DDL or a write) ignoring any result rows. Used for
    /// `CREATE KEYSPACE/TABLE` and seed `INSERT`s where the outcome isn't an
    /// LWT `[applied]` decision.
    pub(crate) async fn cql_execute<V>(&self, cql: &str, values: V) -> Result<(), String>
    where
        V: scylla::serialize::row::SerializeRow,
    {
        self.session
            .query(cql, values)
            .await
            .map(|_| ())
            .map_err(|e| format!("cassandra cql_execute failed: {e}"))
    }

    /// Run a parameterized CQL SELECT and return the first row's first column
    /// decoded as `i64`, or `None` when no row matched. Used to read the outbox
    /// sequence counter.
    pub(crate) async fn cql_query_first_i64(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Option<i64>, String> {
        let result = self
            .session
            .query(cql, values)
            .await
            .map_err(|e| format!("cassandra cql_query_first_i64 failed: {e}"))?;
        let row = match result.maybe_first_row() {
            Ok(Some(row)) => row,
            Ok(None) => return Ok(None),
            Err(e) => return Err(format!("cassandra cql_query_first_i64 rows error: {e}")),
        };
        match row.columns.first().and_then(|c| c.as_ref()) {
            // Counter is stored as a CQL `bigint`; accept `int` too for safety.
            Some(v) => v
                .as_bigint()
                .or_else(|| v.as_int().map(i64::from))
                .map(Some)
                .ok_or_else(|| {
                    "cassandra cql_query_first_i64: column is not an integer".to_string()
                }),
            None => Ok(None),
        }
    }

    /// Run a parameterized CQL SELECT and return all result rows. Each
    /// [`scylla::frame::response::result::Row`] exposes `columns:
    /// Vec<Option<CqlValue>>` in SELECT order, which the canonical-store row
    /// mappers (`cassandra_projection.rs` et al.) decode by ordinal via the
    /// `CqlValue::as_*` accessors. Used by the B.10a phase-2 system-store
    /// scans (projection / saga / migration tables) where the SQL backends
    /// would issue a multi-column `SELECT … fetch_all`.
    ///
    /// Returns an empty `Vec` when no row matched. `ALLOW FILTERING` scans go
    /// through here too; that is acceptable for the conformance contract (small
    /// data) — production would carry a proper secondary index / MV instead.
    pub(crate) async fn cql_query_rows(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<Vec<scylla::frame::response::result::Row>, String> {
        let result = self
            .session
            .query(cql, values)
            .await
            .map_err(|e| format!("cassandra cql_query_rows failed: {e}"))?;
        match result.rows() {
            Ok(rows) => Ok(rows),
            // A statement that returns no rows section (shouldn't happen for a
            // SELECT, but be defensive) is treated as the empty result set.
            Err(_) => Ok(Vec::new()),
        }
    }

    /// Run a CQL LWT (`... IF ...` / `IF NOT EXISTS`) at the given serial
    /// consistency and return the `[applied]` boolean. Cassandra returns
    /// `[applied]` as the first column of the single result row; on a
    /// not-applied LWT it also returns the conflicting row's current values in
    /// the remaining columns, so we read column 0 directly rather than decoding
    /// a fixed-width tuple (which would fail with a row-size mismatch).
    pub(crate) async fn cql_lwt_applied(
        &self,
        cql: &str,
        values: impl scylla::serialize::row::SerializeRow,
        serial: scylla::statement::SerialConsistency,
    ) -> Result<bool, String> {
        let mut query = scylla::statement::query::Query::new(cql.to_string());
        query.set_serial_consistency(Some(serial));
        let result = self
            .session
            .query(query, values)
            .await
            .map_err(|e| format!("cassandra cql_lwt_applied failed: {e}"))?;
        let row = result
            .first_row()
            .map_err(|e| format!("cassandra LWT returned no result row: {e}"))?;
        row.columns
            .first()
            .and_then(|c| c.as_ref())
            .and_then(|v| v.as_boolean())
            .ok_or_else(|| "cassandra LWT result missing [applied] boolean".to_string())
    }
}

#[derive(Debug, Clone)]
pub struct CassandraExecutor {
    client: CassandraClient,
}

impl CassandraExecutor {
    pub fn new(client: CassandraClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for CassandraExecutor {
    fn backend_label(&self) -> &str {
        "cassandra"
    }
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        // A1 (2026-05-30): Cassandra has no native session context,
        // BUT the CQL compiler now AND-injects `tenant_id = ?` and
        // `project_id = ?` into every read/delete/aggregate when
        // the manifest declares those columns, and stamps them
        // onto writes. That is row-level enforcement at the
        // statement layer — no statement can leak across tenant
        // boundaries. Tables whose manifest omits tenant_id
        // columns are not tenant-scoped by design and the compiler
        // is a no-op for them, which is correct.
        enforce_with_mechanism(ctx, "cql_compiler_tenant_predicate_injection")
    }
}

impl BackendHealth for CassandraExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

/// Convert a JSON parameter to a CQL value. Cassandra's typing is
/// strict; we map common JSON shapes to the obvious CQL counterpart.
fn json_to_cql(v: &JsonValue) -> CqlValue {
    match v {
        JsonValue::Null => CqlValue::Empty,
        JsonValue::Bool(b) => CqlValue::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                CqlValue::BigInt(i)
            } else if let Some(f) = n.as_f64() {
                CqlValue::Double(f)
            } else {
                CqlValue::Text(n.to_string())
            }
        }
        JsonValue::String(s) => CqlValue::Text(s.clone()),
        other => CqlValue::Text(other.to_string()),
    }
}

/// Convert a CQL row column to JSON. The driver returns `CqlValue`
/// from `Row::columns`; we surface common types to JSON-friendly
/// shapes.
fn cql_to_json(v: &CqlValue) -> JsonValue {
    match v {
        CqlValue::Empty => JsonValue::Null,
        CqlValue::Boolean(b) => JsonValue::Bool(*b),
        CqlValue::Int(i) => JsonValue::Number((*i).into()),
        CqlValue::BigInt(i) => JsonValue::Number((*i).into()),
        CqlValue::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        CqlValue::Double(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        CqlValue::Text(s) | CqlValue::Ascii(s) => JsonValue::String(s.clone()),
        CqlValue::Uuid(u) => JsonValue::String(u.to_string()),
        CqlValue::Timeuuid(u) => JsonValue::String(u.to_string()),
        CqlValue::Blob(b) => {
            use base64::Engine as _;
            JsonValue::String(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(b)
            ))
        }
        _ => JsonValue::Null,
    }
}

fn cql_leading_keyword(cql: &str) -> String {
    cql.trim_start()
        .split(|ch: char| ch.is_whitespace() || ch == '(')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase()
}

fn validate_cassandra_query(cql: &str) -> Result<(), tonic::Status> {
    match cql_leading_keyword(cql).as_str() {
        "SELECT" => Ok(()),
        other => Err(tonic::Status::invalid_argument(format!(
            "cassandra query accepts SELECT only, got '{other}'"
        ))),
    }
}

fn validate_cassandra_mutation(cql: &str) -> Result<(), tonic::Status> {
    match cql_leading_keyword(cql).as_str() {
        "INSERT" | "UPDATE" | "DELETE" | "BEGIN" => Ok(()),
        other => Err(tonic::Status::invalid_argument(format!(
            "cassandra mutate accepts INSERT/UPDATE/DELETE/BATCH only, got '{other}'"
        ))),
    }
}

impl QueryExecutor for CassandraExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (cql, params_json) = parse_sql_dispatch(req)?;
        validate_cassandra_query(&cql)?;
        let params: Vec<CqlValue> = params_json.iter().map(json_to_cql).collect();
        let result = self
            .client
            .session
            .query(cql.as_str(), &params[..])
            .await
            .map_err(|e| tonic::Status::internal(format!("cassandra query failed: {e}")))?;
        let rows = result.rows.unwrap_or_default();
        let column_specs: Vec<String> = result.col_specs.iter().map(|c| c.name.clone()).collect();
        let mut out: Vec<JsonValue> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut obj = serde_json::Map::new();
            for (i, col) in row.columns.iter().enumerate() {
                let name = column_specs
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("col_{i}"));
                let v = col.as_ref().map(cql_to_json).unwrap_or(JsonValue::Null);
                obj.insert(name, v);
            }
            out.push(JsonValue::Object(obj));
        }
        serde_json::to_string(&JsonValue::Array(out))
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for CassandraExecutor {
    async fn mutate(&self, req: &str) -> Result<String, tonic::Status> {
        let (cql, params_json) = parse_sql_dispatch(req)?;
        validate_cassandra_mutation(&cql)?;
        let params: Vec<CqlValue> = params_json.iter().map(json_to_cql).collect();
        self.client
            .session
            .query(cql.as_str(), &params[..])
            .await
            .map_err(|e| tonic::Status::internal(format!("cassandra mutate failed: {e}")))?;
        // Cassandra doesn't report rows_affected for writes — INSERT /
        // UPDATE / DELETE are always "applied" at the protocol level
        // unless LWT (`IF NOT EXISTS`) reports `[applied]: false`.
        Ok(serde_json::json!({ "ok": true }).to_string())
    }
}

impl SearchExecutor for CassandraExecutor {
    async fn search(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Cassandra has no native search surface; \
             pair with Elasticsearch for full-text or Qdrant for vector",
        ))
    }
}

impl ObjectExecutor for CassandraExecutor {
    async fn get_object(&self, _: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Cassandra is not an object store",
        ))
    }
    async fn put_object(&self, _: &str, _: Vec<u8>) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Cassandra is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for CassandraExecutor {
    async fn ensure_resource(
        &self,
        _resource_name: &str,
        _spec_json: &str,
    ) -> Result<(), tonic::Status> {
        Err(crate::runtime::executor_utils::capability_status(
            "cassandra",
            "ensure_resource",
            "native_resource_lifecycle",
            "Cassandra resource lifecycle is compiler-mediated: issue CREATE/DROP through the compiled DDL path (compile_resource_op + mutate), not the native ensure_resource executor method",
        ))
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(crate::runtime::executor_utils::capability_status(
            "cassandra",
            "drop_resource",
            "native_resource_lifecycle",
            "Cassandra resource lifecycle is compiler-mediated: issue CREATE/DROP through the compiled DDL path (compile_resource_op + mutate), not the native drop_resource executor method",
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let req = serde_json::json!({
            "sql": "SELECT table_name FROM system_schema.tables",
            "params": []
        });
        let body = self.query(&req.to_string()).await?;
        let v: JsonValue = serde_json::from_str(&body)
            .map_err(|e| tonic::Status::internal(format!("list parse: {e}")))?;
        let mut out = Vec::new();
        if let JsonValue::Array(rows) = v {
            for row in rows {
                if let Some(n) = row.get("table_name").and_then(|v| v.as_str()) {
                    out.push(n.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for CassandraExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Cassandra has no multi-statement transactions; \
             use LWT (IF NOT EXISTS) for per-row atomicity",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("cassandra", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_dispatch_extracts_sql_and_params() {
        let req = r#"{"sql":"SELECT * FROM ks.t WHERE pk = ?","params":["abc"]}"#;
        let (sql, params) = parse_sql_dispatch(req).unwrap();
        assert_eq!(sql, "SELECT * FROM ks.t WHERE pk = ?");
        assert_eq!(params, vec![json!("abc")]);
    }

    #[test]
    fn json_to_cql_maps_scalars() {
        assert!(matches!(json_to_cql(&json!(null)), CqlValue::Empty));
        assert!(matches!(json_to_cql(&json!(true)), CqlValue::Boolean(true)));
        assert!(matches!(json_to_cql(&json!(42)), CqlValue::BigInt(42)));
        match json_to_cql(&json!("x")) {
            CqlValue::Text(s) => assert_eq!(s, "x"),
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn cql_to_json_maps_text() {
        let cql = CqlValue::Text("hello".into());
        assert_eq!(cql_to_json(&cql), json!("hello"));
    }
}
