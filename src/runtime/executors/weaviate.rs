//! Weaviate REST executor (C9). reqwest-backed HTTP client; the
//! compiler emits `CompiledRendering::Json` with the full path / method
//! / body already shaped for the Weaviate v1 REST + GraphQL API. The
//! executor's job is auth + transport.

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{build_probe, http_status_to_tonic, parse_rest_dispatch};
use crate::runtime::executors::http::{HttpClientSpec, env_timeout};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Debug, Clone)]
pub struct WeaviateHttpClient {
    base_url: String,
    api_key: Option<String>,
    http: reqwest::Client,
}

impl WeaviateHttpClient {
    pub fn new(base_url: impl Into<String>, api_key: Option<String>) -> Self {
        let http =
            HttpClientSpec::with_timeout(env_timeout("UDB_WEAVIATE_HTTP_TIMEOUT_SECS", 30)).build();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key,
            http,
        }
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(key) = &self.api_key {
            // Weaviate Cloud API keys go in the Authorization Bearer
            // header. Self-hosted Weaviate without auth keeps None.
            req.bearer_auth(key)
        } else {
            req
        }
    }

    pub async fn ping(&self) -> Result<(), String> {
        let url = format!("{}/v1/.well-known/ready", self.base_url);
        let resp = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|e| format!("weaviate ping failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("weaviate ping returned HTTP {}", resp.status()));
        }
        Ok(())
    }

    pub async fn request_json(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &JsonValue,
    ) -> Result<JsonValue, tonic::Status> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self.http.request(method, url);
        req = self.auth(req);
        if !body.is_null() && !matches!(body, JsonValue::Object(m) if m.is_empty()) {
            req = req.json(body);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| tonic::Status::unavailable(format!("weaviate request failed: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| tonic::Status::internal(format!("weaviate response read failed: {e}")))?;
        if !status.is_success() {
            return Err(weaviate_status_to_tonic(status, &text));
        }
        if text.is_empty() {
            return Ok(JsonValue::Object(Default::default()));
        }
        serde_json::from_str(&text)
            .map_err(|e| tonic::Status::internal(format!("weaviate response parse failed: {e}")))
    }
}

fn weaviate_status_to_tonic(status: reqwest::StatusCode, body: &str) -> tonic::Status {
    let detail = body.chars().take(200).collect::<String>();
    http_status_to_tonic(status, &detail, "weaviate")
}

pub(crate) fn weaviate_class_name(resource_name: &str) -> String {
    let mut out = String::new();
    for ch in resource_name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if ch == '_' || ch == '-' {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push_str("UdbVector");
    }
    if !out.chars().next().is_some_and(|ch| ch.is_ascii_uppercase()) {
        out.insert_str(0, "Udb");
    }
    out
}

#[derive(Debug, Clone)]
pub struct WeaviateExecutor {
    client: WeaviateHttpClient,
}

impl WeaviateExecutor {
    pub fn new(client: WeaviateHttpClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for WeaviateExecutor {
    fn backend_label(&self) -> &str {
        "weaviate"
    }
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id stamped on object properties; AND'd into where clauses",
        )
    }
}

impl BackendHealth for WeaviateExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

impl QueryExecutor for WeaviateExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for WeaviateExecutor {
    async fn mutate(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl SearchExecutor for WeaviateExecutor {
    async fn search(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl ObjectExecutor for WeaviateExecutor {
    async fn get_object(&self, _: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Weaviate is not an object store",
        ))
    }
    async fn put_object(&self, _: &str, _: Vec<u8>) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Weaviate is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for WeaviateExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let _spec: JsonValue = serde_json::from_str(spec_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid spec: {e}")))?;
        let spec = serde_json::json!({
            "class": weaviate_class_name(resource_name),
            "vectorizer": "none",
            "properties": [
                { "name": "tag", "dataType": ["text"] },
                { "name": "_tenant_id", "dataType": ["text"] },
                { "name": "_project_id", "dataType": ["text"] }
            ]
        });
        self.client
            .request_json(reqwest::Method::POST, "/v1/schema", &spec)
            .await?;
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        self.client
            .request_json(
                reqwest::Method::DELETE,
                &format!("/v1/schema/{resource_name}"),
                &JsonValue::Null,
            )
            .await?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let r = self
            .client
            .request_json(reqwest::Method::GET, "/v1/schema", &JsonValue::Null)
            .await?;
        let mut out = Vec::new();
        if let Some(classes) = r.get("classes").and_then(|v| v.as_array()) {
            for class in classes {
                if let Some(name) = class.get("class").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for WeaviateExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Weaviate has no transaction primitive",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("weaviate", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dispatch_works() {
        let req = r#"{"path":"/v1/graphql","method":"POST","body":{}}"#;
        let (m, p, _) = parse_rest_dispatch(req).unwrap();
        assert_eq!(m, reqwest::Method::POST);
        assert_eq!(p, "/v1/graphql");
    }

    #[test]
    fn weaviate_status_maps_correctly() {
        assert_eq!(
            weaviate_status_to_tonic(reqwest::StatusCode::FORBIDDEN, "{}").code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            weaviate_status_to_tonic(reqwest::StatusCode::TOO_MANY_REQUESTS, "{}").code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn enforce_reports_enforced_with_context() {
        let exec = WeaviateExecutor::new(WeaviateHttpClient::new("http://localhost:8080", None));
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        assert!(matches!(exec.enforce(&ctx), ContextEffect::Enforced { .. }));
    }
}
