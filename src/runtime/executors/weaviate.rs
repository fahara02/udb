//! Weaviate REST executor (C9). reqwest-backed HTTP client; the
//! compiler emits `CompiledRendering::Json` with the full path / method
//! / body already shaped for the Weaviate v1 REST + GraphQL API. The
//! executor's job is auth + transport.

use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};
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
            crate::runtime::executors::http::HttpClientSpec::with_timeout(Duration::from_secs(30))
                .build();
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
    match status.as_u16() {
        400 | 422 => {
            tonic::Status::invalid_argument(format!("weaviate {}: {detail}", status.as_u16()))
        }
        401 | 403 => {
            tonic::Status::permission_denied(format!("weaviate {}: {detail}", status.as_u16()))
        }
        404 => tonic::Status::not_found(format!("weaviate 404: {detail}")),
        409 => tonic::Status::already_exists(format!("weaviate 409: {detail}")),
        429 => tonic::Status::resource_exhausted(format!("weaviate 429: {detail}")),
        500..=599 => tonic::Status::unavailable(format!("weaviate {}: {detail}", status.as_u16())),
        _ => tonic::Status::internal(format!("weaviate {}: {detail}", status.as_u16())),
    }
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
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        ContextEffect::Enforced {
            mechanism:
                "_tenant_id / _project_id stamped on object properties; AND'd into where clauses"
                    .into(),
        }
    }
}

impl BackendHealth for WeaviateExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

fn parse_dispatch(req: &str) -> Result<(reqwest::Method, String, JsonValue), tonic::Status> {
    let v: JsonValue = serde_json::from_str(req)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid dispatch JSON: {e}")))?;
    let path = v
        .get("path")
        .and_then(|x| x.as_str())
        .ok_or_else(|| tonic::Status::invalid_argument("missing `path`"))?
        .to_string();
    let method = v
        .get("method")
        .and_then(|x| x.as_str())
        .unwrap_or("POST")
        .parse::<reqwest::Method>()
        .map_err(|e| tonic::Status::invalid_argument(format!("bad method: {e}")))?;
    let body = v.get("body").cloned().unwrap_or(JsonValue::Null);
    Ok((method, path, body))
}

impl QueryExecutor for WeaviateExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for WeaviateExecutor {
    async fn mutate(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl SearchExecutor for WeaviateExecutor {
    async fn search(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
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
        let mut spec: JsonValue = serde_json::from_str(spec_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid spec: {e}")))?;
        if let JsonValue::Object(map) = &mut spec {
            map.entry("class".to_string())
                .or_insert_with(|| JsonValue::String(resource_name.to_string()));
            map.entry("vectorizer".to_string())
                .or_insert_with(|| JsonValue::String("none".to_string()));
        }
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
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "weaviate".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "weaviate".to_string(),
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

    #[test]
    fn parse_dispatch_works() {
        let req = r#"{"path":"/v1/graphql","method":"POST","body":{}}"#;
        let (m, p, _) = parse_dispatch(req).unwrap();
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
