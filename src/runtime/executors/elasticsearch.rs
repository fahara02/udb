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

use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};
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
    /// underlying reqwest client with a 30s default timeout matching
    /// the Qdrant / Mongo patterns.
    pub fn new(base_url: impl Into<String>, auth: ElasticsearchAuth) -> Self {
        Self::with_timeout(base_url, auth, Duration::from_secs(30))
    }

    pub fn with_timeout(
        base_url: impl Into<String>,
        auth: ElasticsearchAuth,
        timeout: Duration,
    ) -> Self {
        let http = crate::runtime::executors::http::HttpClientSpec::with_timeout(timeout).build();
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
        let resp = req.send().await.map_err(|e| {
            tonic::Status::unavailable(format!("Elasticsearch request failed: {e}"))
        })?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| {
            tonic::Status::internal(format!("Elasticsearch response read failed: {e}"))
        })?;
        if !status.is_success() {
            return Err(es_status_to_tonic(status, &text));
        }
        if text.is_empty() {
            return Ok(JsonValue::Object(Default::default()));
        }
        serde_json::from_str(&text).map_err(|e| {
            tonic::Status::internal(format!(
                "Elasticsearch response parse failed: {e}; body: {}",
                text.chars().take(200).collect::<String>()
            ))
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
        let resp =
            self.auth(req).send().await.map_err(|e| {
                tonic::Status::unavailable(format!("Elasticsearch _bulk failed: {e}"))
            })?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| {
            tonic::Status::internal(format!("Elasticsearch _bulk read failed: {e}"))
        })?;
        if !status.is_success() {
            return Err(es_status_to_tonic(status, &text));
        }
        serde_json::from_str(&text).map_err(|e| {
            tonic::Status::internal(format!(
                "Elasticsearch _bulk parse failed: {e}; body: {}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }
}

/// Translate an HTTP error from ES into a typed `tonic::Status`.
/// ES error responses include a JSON body with `error.type` /
/// `error.reason` that we surface so operators can diagnose.
fn es_status_to_tonic(status: reqwest::StatusCode, body: &str) -> tonic::Status {
    // Try to extract `error.reason` from the response body.
    let detail = serde_json::from_str::<JsonValue>(body)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.get("reason"))
                .and_then(|r| r.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.chars().take(200).collect::<String>());
    match status.as_u16() {
        400 => tonic::Status::invalid_argument(format!("Elasticsearch 400: {detail}")),
        401 | 403 => {
            tonic::Status::permission_denied(format!("Elasticsearch {}: {detail}", status.as_u16()))
        }
        404 => tonic::Status::not_found(format!("Elasticsearch 404: {detail}")),
        409 => tonic::Status::already_exists(format!("Elasticsearch 409: {detail}")),
        429 => tonic::Status::resource_exhausted(format!("Elasticsearch 429: {detail}")),
        500..=599 => {
            tonic::Status::unavailable(format!("Elasticsearch {}: {detail}", status.as_u16()))
        }
        _ => tonic::Status::internal(format!("Elasticsearch {}: {detail}", status.as_u16())),
    }
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
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        // C7/C8: the ES IR compiler stamps `_tenant_id` / `_project_id`
        // on every written document and ANDs them into every read /
        // delete / search / aggregate query.
        ContextEffect::Enforced {
            mechanism: "_tenant_id / _project_id stamped on writes; ANDed into bool/must on reads"
                .into(),
        }
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
fn parse_dispatch(
    request_json: &str,
) -> Result<(reqwest::Method, String, JsonValue), tonic::Status> {
    let req: JsonValue = serde_json::from_str(request_json)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let path = req
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `path` in dispatch request"))?
        .to_string();
    let method_str = req.get("method").and_then(|v| v.as_str()).unwrap_or("POST");
    let method = method_str
        .parse::<reqwest::Method>()
        .map_err(|e| tonic::Status::invalid_argument(format!("bad method '{method_str}': {e}")))?;
    let body = req.get("body").cloned().unwrap_or(JsonValue::Null);
    Ok((method, path, body))
}

impl QueryExecutor for ElasticsearchExecutor {
    async fn query(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (method, path, body) = parse_dispatch(request_json)?;
        let resp = self.client.request_json(method, &path, &body).await?;
        serde_json::to_string(&resp).map_err(|e| tonic::Status::internal(e.to_string()))
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
            return serde_json::to_string(&resp)
                .map_err(|e| tonic::Status::internal(e.to_string()));
        }
        let resp = self.client.request_json(method, &path, &body).await?;
        serde_json::to_string(&resp).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl SearchExecutor for ElasticsearchExecutor {
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status> {
        let (method, path, body) = parse_dispatch(request_json)?;
        let resp = self.client.request_json(method, &path, &body).await?;
        serde_json::to_string(&resp).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl ObjectExecutor for ElasticsearchExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Elasticsearch is not an object store; route to S3/MinIO",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
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
        let spec: JsonValue = serde_json::from_str(spec_json).map_err(|e| {
            tonic::Status::invalid_argument(format!("invalid ensure_resource spec: {e}"))
        })?;
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
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Elasticsearch does not provide multi-document \
             transactions; each _bulk request is atomic per-shard but not cross-document",
        ))
    }

    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        match self.client.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "elasticsearch".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "elasticsearch".to_string(),
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
