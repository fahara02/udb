//! Elasticsearch HTTP executor (C9).
//!
//! Lightweight reqwest-based client for the Elasticsearch REST API.
//! Mirrors the Qdrant / Mongo Data API pattern: stateless leaf I/O on
//! a fully-resolved client; the orchestration layer
//! (`DataBrokerRuntime`) holds the per-instance client + hands it
//! here on dispatch.
//!
//! ## Auth modes
//!
//! - **Basic auth** when `username` + `password` are set on the
//!   `ElasticsearchConfig` (matches the most common deployment).
//! - **API key** when `api_key` is set — sent as
//!   `Authorization: ApiKey <base64>`. Takes precedence over Basic
//!   when both are configured.
//! - **No auth** when neither is set (dev / unprotected clusters).
//!
//! ## What this DOES NOT do (yet)
//!
//! - **Cluster discovery / sniffing** — single base URL only; HA
//!   needs a load balancer in front. Future enhancement.
//! - **Snapshot / restore** — out of scope for the broker's generic
//!   dispatch path.
//! - **Async search** — every request is synchronous; long-running
//!   aggregations should be issued via the operator's portal.

use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{
    backend_transport_status, build_probe, capability_status, invalid_argument_fields,
};
use crate::runtime::executors::http::{HttpClientSpec, env_timeout};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

// ── HTTP client ────────────────────────────────────────────────────────────

/// Reqwest-backed Elasticsearch client. Holds the base URL + auth.
/// Cheap to clone (the inner `reqwest::Client` is `Arc<_>`-shaped).
#[derive(Debug, Clone)]
pub struct ElasticsearchHttpClient {
    base_url: String,
    auth: ElasticsearchAuth,
    http: reqwest::Client,
}

#[derive(Debug, Clone)]
pub enum ElasticsearchAuth {
    None,
    Basic { username: String, password: String },
    ApiKey(String),
}

impl ElasticsearchHttpClient {
    /// Construct a client from base URL + auth. Pre-resolves the
    /// underlying reqwest client with a configurable default timeout matching
    /// the Qdrant / Mongo patterns.
    pub fn new(base_url: impl Into<String>, auth: ElasticsearchAuth) -> Self {
        Self::with_timeout(
            base_url,
            auth,
            env_timeout("UDB_ELASTICSEARCH_HTTP_TIMEOUT_SECS", 30),
        )
    }

    pub fn with_timeout(
        base_url: impl Into<String>,
        auth: ElasticsearchAuth,
        timeout: Duration,
    ) -> Self {
        let http = HttpClientSpec::with_timeout(timeout).build();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            auth,
            http,
        }
    }

    /// Apply Auth headers to a request builder.
    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.auth {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                req.basic_auth(username, Some(password))
            }
            ElasticsearchAuth::ApiKey(key) => {
                // ES expects `Authorization: ApiKey <base64-encoded
                // id:api_key>`. We accept the already-encoded form
                // from config — operators paste the value Kibana
                // emits when they create the key.
                req.header(reqwest::header::AUTHORIZATION, format!("ApiKey {key}"))
            }
        }
    }

    /// Health probe: GET /. ES returns the cluster identity + version.
    /// We treat any 2xx as healthy.
    pub async fn ping(&self) -> Result<(), String> {
        let url = format!("{}/", self.base_url);
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| format!("Elasticsearch ping failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "Elasticsearch ping returned HTTP {}",
                resp.status()
            ));
        }
        Ok(())
    }

    /// Issue a JSON-body request to the given path and return the
    /// response body as parsed JSON. The path includes the leading
    /// slash — callers pass `/index/_search` etc.
    pub async fn request_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, tonic::Status> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, url);
        req = self.auth(req);
        // Empty body objects → no body (some ES endpoints reject
        // bodies on GET / DELETE).
        if !body.is_null() && !matches!(body, JsonValue::Object(m) if m.is_empty()) {
            req = req.json(body);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| backend_transport_status("Elasticsearch", "request", e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| backend_transport_status("Elasticsearch", "response read", e))?;
        if !status.is_success() {
            return Err(es_status_to_tonic(status, &text));
        }
        if text.is_empty() {
            return Ok(JsonValue::Object(Default::default()));
        }
        serde_json::from_str(&text).map_err(|e| {
            backend_transport_status(
                "Elasticsearch",
                "response parse",
                format!("{e}; body: {}", text.chars().take(200).collect::<String>()),
            )
        })
    }

    /// Issue a `_bulk` request — the body is NDJSON, not JSON.
    /// Content-Type must be `application/x-ndjson`.
    pub async fn request_ndjson(
        &self,
        path: &str,
        ndjson: &str,
    ) -> Result<JsonValue, tonic::Status> {
        let url = format!("{}{}", self.base_url, path);
        let req = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(ndjson.to_string());
        let resp = self
            .auth(req)
            .send()
            .await
            .map_err(|e| backend_transport_status("Elasticsearch", "_bulk", e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| backend_transport_status("Elasticsearch", "_bulk response read", e))?;
        if !status.is_success() {
            return Err(es_status_to_tonic(status, &text));
        }
        serde_json::from_str(&text).map_err(|e| {
            backend_transport_status(
                "Elasticsearch",
                "_bulk response parse",
                format!("{e}; body: {}", text.chars().take(200).collect::<String>()),
            )
        })
    }
}

fn elasticsearch_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("elasticsearch", operation, message)
}

fn encode_elasticsearch_response(
    operation: &'static str,
    value: &JsonValue,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value).map_err(|e| {
        elasticsearch_internal_status(
            operation,
            format!("Elasticsearch response serialise failed: {e}"),
        )
    })
}

/// Translate an HTTP error from ES into a typed `tonic::Status`.
/// ES error responses include a JSON body with `error.type` /
/// `error.reason` that we surface so operators can diagnose.
fn es_status_to_tonic(status: reqwest::StatusCode, body: &str) -> tonic::Status {
    // Extract the ES-specific `error.reason` from the response body (falling
    // back to a truncated body), then delegate the status→code mapping to the
    // shared `http_status_to_tonic` (#21) so pinecone/weaviate/elasticsearch
    // map HTTP codes identically. Note: this folds `422` into invalid_argument
    // (the shared `400 | 422` arm), where the prior ES-local mapping treated
    // 422 as internal — ES does not emit 422 in practice (it uses 400).
    let detail = serde_json::from_str::<JsonValue>(body)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.get("reason"))
                .and_then(|r| r.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.chars().take(200).collect::<String>());
    crate::runtime::executor_utils::http_status_to_tonic(status, &detail, "Elasticsearch")
}

// ── Executor ────────────────────────────────────────────────────────────

/// Generic-dispatch executor wrapping an `ElasticsearchHttpClient`.
/// The dispatch path resolves the client by instance name + hands it
/// to a fresh `ElasticsearchExecutor` for the duration of the request.
#[derive(Debug, Clone)]
pub struct ElasticsearchExecutor {
    client: ElasticsearchHttpClient,
}

impl ElasticsearchExecutor {
    pub fn new(client: ElasticsearchHttpClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for ElasticsearchExecutor {
    fn backend_label(&self) -> &str {
        "elasticsearch"
    }

    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        // C7/C8: the ES IR compiler stamps `_tenant_id` / `_project_id`
        // on every written document and ANDs them into every read /
        // delete / search / aggregate query.
        enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id stamped on writes; ANDed into bool/must on reads",
        )
    }
}

impl BackendHealth for ElasticsearchExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

// Dispatch JSON contract: the broker passes the compiler output
// directly as `{ "path": "...", "method": "POST", "body": {...} }`
// — the compiler already produced the wire shape, the executor just
// forwards it.
// #21: REST dispatch parsing is shared with pinecone/weaviate via
// `executor_utils::parse_rest_dispatch` (byte-identical shape).
use crate::runtime::executor_utils::parse_rest_dispatch as parse_dispatch;

fn invalid_ensure_resource_spec_status(err: serde_json::Error) -> tonic::Status {
    invalid_argument_fields(
        format!("invalid ensure_resource spec: {err}"),
        [(
            "spec_json",
            "must be valid JSON for Elasticsearch ensure_resource",
        )],
    )
}

impl QueryExecutor for ElasticsearchExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (method, path, body) = parse_dispatch(request_json)?;
        let resp = self.client.request_json(method, &path, &body).await?;
        encode_elasticsearch_response("query_response_encode", &resp)
    }
}

impl MutationExecutor for ElasticsearchExecutor {
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (method, path, body) = parse_dispatch(request_json)?;
        // Detect _bulk shape: the compiler wraps the NDJSON string
        // inside `{"ndjson": "..."}` because CompiledRendering's body
        // type is JSON. Unwrap here and post as ndjson.
        if let Some(ndjson) = body.get("ndjson").and_then(|v| v.as_str()) {
            let resp = self.client.request_ndjson(&path, ndjson).await?;
            return encode_elasticsearch_response("bulk_response_encode", &resp);
        }
        let resp = self.client.request_json(method, &path, &body).await?;
        encode_elasticsearch_response("mutate_response_encode", &resp)
    }
}

impl SearchExecutor for ElasticsearchExecutor {
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (method, path, body) = parse_dispatch(request_json)?;
        let resp = self.client.request_json(method, &path, &body).await?;
        encode_elasticsearch_response("search_response_encode", &resp)
    }
}

impl ObjectExecutor for ElasticsearchExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "elasticsearch",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Elasticsearch is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "elasticsearch",
            "put_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Elasticsearch is not an object store; route to S3/MinIO",
        ))
    }
}

impl ResourceAdminExecutor for ElasticsearchExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let raw: JsonValue =
            serde_json::from_str(spec_json).map_err(invalid_ensure_resource_spec_status)?;
        let dimension = raw
            .get("dimension")
            .or_else(|| raw.get("vector_size"))
            .or_else(|| raw.get("size"))
            .and_then(JsonValue::as_i64)
            .unwrap_or(4)
            .max(1);
        let spec = serde_json::json!({
            "mappings": {
                "properties": {
                    "vector": {
                        "type": "dense_vector",
                        "dims": dimension,
                        "index": true,
                        "similarity": "cosine"
                    },
                    "payload": { "type": "object", "enabled": true }
                }
            }
        });
        let index = resource_name.to_ascii_lowercase();
        self.client
            .request_json(reqwest::Method::PUT, &format!("/{index}"), &spec)
            .await?;
        Ok(())
    }

    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let index = resource_name.to_ascii_lowercase();
        self.client
            .request_json(
                reqwest::Method::DELETE,
                &format!("/{index}"),
                &JsonValue::Null,
            )
            .await?;
        Ok(())
    }

    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let resp = self
            .client
            .request_json(
                reqwest::Method::GET,
                "/_cat/indices?format=json",
                &JsonValue::Null,
            )
            .await?;
        let mut out = Vec::new();
        if let JsonValue::Array(rows) = resp {
            for row in rows {
                if let Some(name) = row.get("index").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for ElasticsearchExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "elasticsearch",
            "transaction",
            "transactions",
            "UDB_UNSUPPORTED_OPERATION: Elasticsearch does not provide multi-document \
             transactions; each _bulk request is atomic per-shard but not cross-document",
        ))
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("elasticsearch", self.client.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;
    use serde_json::json;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "elasticsearch");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn elasticsearch_internal_status_carries_typed_detail() {
        let status = elasticsearch_internal_status(
            "query_response_encode",
            "Elasticsearch response serialise failed",
        );
        assert_internal_detail(
            &status,
            "query_response_encode",
            "Elasticsearch response serialise failed",
        );
    }

    #[test]
    fn auth_basic_attaches_credentials() {
        // We can't run a real reqwest send without a server, but we
        // can verify the auth branch by constructing the client and
        // confirming the auth variant.
        let client = ElasticsearchHttpClient::new(
            "http://localhost:9200",
            ElasticsearchAuth::Basic {
                username: "elastic".into(),
                password: "changeme".into(),
            },
        );
        match &client.auth {
            ElasticsearchAuth::Basic { username, .. } => assert_eq!(username, "elastic"),
            _ => panic!("expected Basic auth"),
        }
    }

    #[test]
    fn auth_apikey_takes_precedence_in_match() {
        let client = ElasticsearchHttpClient::new(
            "http://localhost:9200",
            ElasticsearchAuth::ApiKey("abc123".into()),
        );
        assert!(matches!(client.auth, ElasticsearchAuth::ApiKey(_)));
    }

    #[test]
    fn base_url_trims_trailing_slash() {
        let client =
            ElasticsearchHttpClient::new("http://localhost:9200/", ElasticsearchAuth::None);
        assert_eq!(client.base_url, "http://localhost:9200");
    }

    #[test]
    fn parse_dispatch_extracts_method_path_body() {
        let req = r#"{"path":"/orders/_search","method":"POST","body":{"query":{"match_all":{}}}}"#;
        let (method, path, body) = parse_dispatch(req).unwrap();
        assert_eq!(method, reqwest::Method::POST);
        assert_eq!(path, "/orders/_search");
        assert_eq!(body["query"]["match_all"], json!({}));
    }

    #[test]
    fn parse_dispatch_defaults_method_to_post() {
        let req = r#"{"path":"/orders/_search","body":{}}"#;
        let (method, _, _) = parse_dispatch(req).unwrap();
        assert_eq!(method, reqwest::Method::POST);
    }

    #[test]
    fn parse_dispatch_rejects_missing_path() {
        let req = r#"{"method":"GET","body":{}}"#;
        let err = parse_dispatch(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn es_status_to_tonic_extracts_error_reason() {
        let body =
            r#"{"error":{"type":"index_not_found_exception","reason":"no such index [foo]"}}"#;
        let status = es_status_to_tonic(reqwest::StatusCode::NOT_FOUND, body);
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert!(status.message().contains("no such index [foo]"));
    }

    #[test]
    fn es_status_maps_403_to_permission_denied() {
        let status = es_status_to_tonic(reqwest::StatusCode::FORBIDDEN, "{}");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    #[test]
    fn es_status_maps_429_to_resource_exhausted() {
        let status = es_status_to_tonic(reqwest::StatusCode::TOO_MANY_REQUESTS, "{}");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        let detail = decode_detail(&status);
        assert_eq!(detail.kind, ErrorKind::Quota as i32);
        assert!(detail.retryable);
        assert_eq!(detail.retry_after_ms, 250);
        assert_eq!(detail.backend, "Elasticsearch");
        assert_eq!(detail.operation, "request");
    }

    #[test]
    fn ensure_resource_spec_validation_carries_field_violation() {
        let err = serde_json::from_str::<JsonValue>("{")
            .map_err(invalid_ensure_resource_spec_status)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("invalid ensure_resource spec:"));
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "spec_json");
    }

    #[test]
    fn enforce_returns_enforced_when_context_set() {
        let exec = ElasticsearchExecutor::new(ElasticsearchHttpClient::new(
            "http://localhost:9200",
            ElasticsearchAuth::None,
        ));
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        match exec.enforce(&ctx) {
            ContextEffect::Enforced { mechanism } => {
                assert!(mechanism.contains("_tenant_id"));
            }
            other => panic!("expected Enforced, got {other:?}"),
        }
    }

    #[test]
    fn enforce_returns_advisory_when_context_empty() {
        let exec = ElasticsearchExecutor::new(ElasticsearchHttpClient::new(
            "http://localhost:9200",
            ElasticsearchAuth::None,
        ));
        let ctx = AppliedContext::default();
        assert!(matches!(exec.enforce(&ctx), ContextEffect::Advisory { .. }));
    }
}
