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

use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};
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
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        // A1 (2026-05-30): Cassandra has no native session context,
        // BUT the CQL compiler now AND-injects `tenant_id = ?` and
        // `project_id = ?` into every read/delete/aggregate when
        // the manifest declares those columns, and stamps them
        // onto writes. That is row-level enforcement at the
        // statement layer — no statement can leak across tenant
        // boundaries. Tables whose manifest omits tenant_id
        // columns are not tenant-scoped by design and the compiler
        // is a no-op for them, which is correct.
        ContextEffect::Enforced {
            mechanism: "cql_compiler_tenant_predicate_injection".into(),
        }
    }
}

impl BackendHealth for CassandraExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

fn parse_dispatch(req: &str) -> Result<(String, Vec<JsonValue>), tonic::Status> {
    let v: JsonValue = serde_json::from_str(req)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let sql = v
        .get("sql")
        .and_then(|x| x.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `sql`"))?
        .to_string();
    let params = v
        .get("params")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default();
    Ok((sql, params))
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

impl QueryExecutor for CassandraExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (cql, params_json) = parse_dispatch(req)?;
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
        let (cql, params_json) = parse_dispatch(req)?;
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
        // DDL goes through compile_resource_op + mutate.
        Err(tonic::Status::unimplemented(
            "CassandraExecutor::ensure_resource — use compile_resource_op + mutate",
        ))
    }
    async fn drop_resource(&self, _resource_name: &str) -> Result<(), tonic::Status> {
        Err(tonic::Status::unimplemented(
            "CassandraExecutor::drop_resource — use compile_resource_op + mutate",
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
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "cassandra".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "cassandra".to_string(),
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
        let req = r#"{"sql":"SELECT * FROM ks.t WHERE pk = ?","params":["abc"]}"#;
        let (sql, params) = parse_dispatch(req).unwrap();
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
