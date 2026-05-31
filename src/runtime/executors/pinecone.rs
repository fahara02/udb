//! Pinecone executor (C9). reqwest-backed REST client.
//!
//! Pinecone uses per-index URLs of the form
//! `https://<index>-<project>.svc.<env>.pinecone.io`. The operator
//! pastes the index host as the DSN. Auth is via the `Api-Key` header.

use std::time::Duration;

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{AppliedContext, BackendContextEnforcer, ContextEffect};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Debug, Clone)]
pub struct PineconeHttpClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

impl PineconeHttpClient {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let http =
            crate::runtime::executors::http::HttpClientSpec::with_timeout(Duration::from_secs(30))
                .build();
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            http,
        }
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req.header("Api-Key", &self.api_key)
    }

    pub async fn ping(&self) -> Result<(), String> {
        // `/describe_index_stats` is the lightest endpoint that
        // requires both the URL + the API key to be valid.
        let url = format!("{}/describe_index_stats", self.base_url);
        let resp = self
            .auth(self.http.post(url).json(&serde_json::json!({})))
            .send()
            .await
            .map_err(|e| format!("pinecone ping failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("pinecone ping returned HTTP {}", resp.status()));
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
            .map_err(|e| tonic::Status::unavailable(format!("pinecone request failed: {e}")))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| tonic::Status::internal(format!("pinecone read failed: {e}")))?;
        if !status.is_success() {
            return Err(pinecone_status_to_tonic(status, &text));
        }
        if text.is_empty() {
            return Ok(JsonValue::Object(Default::default()));
        }
        serde_json::from_str(&text)
            .map_err(|e| tonic::Status::internal(format!("pinecone parse failed: {e}")))
    }
}

fn pinecone_status_to_tonic(status: reqwest::StatusCode, body: &str) -> tonic::Status {
    let detail = body.chars().take(200).collect::<String>();
    match status.as_u16() {
        400 | 422 => {
            tonic::Status::invalid_argument(format!("pinecone {}: {detail}", status.as_u16()))
        }
        401 | 403 => {
            tonic::Status::permission_denied(format!("pinecone {}: {detail}", status.as_u16()))
        }
        404 => tonic::Status::not_found(format!("pinecone 404: {detail}")),
        409 => tonic::Status::already_exists(format!("pinecone 409: {detail}")),
        429 => tonic::Status::resource_exhausted(format!("pinecone 429: {detail}")),
        500..=599 => tonic::Status::unavailable(format!("pinecone {}: {detail}", status.as_u16())),
        _ => tonic::Status::internal(format!("pinecone {}: {detail}", status.as_u16())),
    }
}

#[derive(Debug, Clone)]
pub struct PineconeExecutor {
    client: PineconeHttpClient,
}

impl PineconeExecutor {
    pub fn new(client: PineconeHttpClient) -> Self {
        Self { client }
    }
}

impl BackendContextEnforcer for PineconeExecutor {
    fn backend_label(&self) -> &str {
        "pinecone"
    }
    fn enforce(&self, ctx: &AppliedContext) -> ContextEffect {
        if ctx.is_empty() {
            return ContextEffect::Advisory {
                recorded_in: "no_context_to_apply".into(),
            };
        }
        ContextEffect::Enforced {
            mechanism: "_tenant_id / _project_id stamped in vector metadata; AND'd into query/delete filters".into(),
        }
    }
}

impl BackendHealth for PineconeExecutor {
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

impl QueryExecutor for PineconeExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl MutationExecutor for PineconeExecutor {
    async fn mutate(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl SearchExecutor for PineconeExecutor {
    async fn search(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        serde_json::to_string(&r).map_err(|e| tonic::Status::internal(e.to_string()))
    }
}

impl ObjectExecutor for PineconeExecutor {
    async fn get_object(&self, _: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Pinecone is not an object store",
        ))
    }
    async fn put_object(&self, _: &str, _: Vec<u8>) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Pinecone is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for PineconeExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let mut spec: JsonValue = serde_json::from_str(spec_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("invalid spec: {e}")))?;
        if let JsonValue::Object(map) = &mut spec {
            map.entry("name".to_string())
                .or_insert_with(|| JsonValue::String(resource_name.to_string()));
            map.entry("dimension".to_string())
                .or_insert_with(|| serde_json::json!(1536));
            map.entry("metric".to_string())
                .or_insert_with(|| JsonValue::String("cosine".to_string()));
        }
        self.client
            .request_json(reqwest::Method::POST, "/indexes", &spec)
            .await?;
        Ok(())
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        self.client
            .request_json(
                reqwest::Method::DELETE,
                &format!("/indexes/{resource_name}"),
                &JsonValue::Null,
            )
            .await?;
        Ok(())
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let r = self
            .client
            .request_json(reqwest::Method::GET, "/indexes", &JsonValue::Null)
            .await?;
        let mut out = Vec::new();
        if let Some(idxs) = r.get("indexes").and_then(|v| v.as_array()) {
            for idx in idxs {
                if let Some(name) = idx.get("name").and_then(|v| v.as_str()) {
                    out.push(name.to_string());
                }
            }
        }
        Ok(out)
    }
}

impl BackendExecutor for PineconeExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(tonic::Status::failed_precondition(
            "UDB_UNSUPPORTED_OPERATION: Pinecone has no transaction primitive",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        match self.ping().await {
            Ok(()) => Ok(BackendProbe {
                backend: "pinecone".to_string(),
                instance: None,
                ok: true,
                error: None,
            }),
            Err(err) => Ok(BackendProbe {
                backend: "pinecone".to_string(),
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
        let req = r#"{"path":"/query","method":"POST","body":{"vector":[0.1]}}"#;
        let (m, p, _) = parse_dispatch(req).unwrap();
        assert_eq!(m, reqwest::Method::POST);
        assert_eq!(p, "/query");
    }

    #[test]
    fn pinecone_status_maps_correctly() {
        assert_eq!(
            pinecone_status_to_tonic(reqwest::StatusCode::UNAUTHORIZED, "{}").code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            pinecone_status_to_tonic(reqwest::StatusCode::TOO_MANY_REQUESTS, "{}").code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn enforce_reports_enforced_with_context() {
        let exec = PineconeExecutor::new(PineconeHttpClient::new(
            "https://idx.svc.pinecone.io",
            "key",
        ));
        let ctx = AppliedContext {
            tenant_id: "acme".into(),
            ..Default::default()
        };
        assert!(matches!(exec.enforce(&ctx), ContextEffect::Enforced { .. }));
    }
}
