//! Pinecone executor (C9). reqwest-backed REST client.
//!
//! Pinecone uses per-index URLs of the form
//! `https://<index>-<project>.svc.<env>.pinecone.io`. The operator
//! pastes the index host as the DSN. Auth is via the `Api-Key` header.

use serde_json::Value as JsonValue;

use crate::runtime::backend_context::{
    AppliedContext, BackendContextEnforcer, ContextEffect, enforce_with_mechanism,
};
use crate::runtime::executor_utils::{
    backend_transport_status, build_probe, capability_status, http_status_to_tonic,
    invalid_argument_fields, parse_rest_dispatch,
};
use crate::runtime::executors::http::{HttpClientSpec, env_timeout};
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

#[derive(Clone)]
pub struct PineconeHttpClient {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
}

// 4.6 secrets posture: redact `api_key` in Debug.
impl std::fmt::Debug for PineconeHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PineconeHttpClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl PineconeHttpClient {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let http =
            HttpClientSpec::with_timeout(env_timeout("UDB_PINECONE_HTTP_TIMEOUT_SECS", 30)).build();
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
            .map_err(|e| backend_transport_status("pinecone", "request", e))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| backend_transport_status("pinecone", "response read", e))?;
        if !status.is_success() {
            return Err(pinecone_status_to_tonic(status, &text));
        }
        if text.is_empty() {
            return Ok(JsonValue::Object(Default::default()));
        }
        serde_json::from_str(&text)
            .map_err(|e| backend_transport_status("pinecone", "response parse", e))
    }
}

fn pinecone_status_to_tonic(status: reqwest::StatusCode, body: &str) -> tonic::Status {
    let detail = body.chars().take(200).collect::<String>();
    http_status_to_tonic(status, &detail, "pinecone")
}

fn invalid_ensure_resource_spec_status(err: serde_json::Error) -> tonic::Status {
    invalid_argument_fields(
        format!("invalid spec: {err}"),
        [(
            "spec_json",
            "must be valid JSON for Pinecone ensure_resource",
        )],
    )
}

fn pinecone_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("pinecone", operation, message)
}

fn encode_pinecone_response(
    operation: &'static str,
    value: &JsonValue,
) -> Result<String, tonic::Status> {
    serde_json::to_string(value).map_err(|e| pinecone_internal_status(operation, e.to_string()))
}

/// Refusal for an index-lifecycle call: the DSN is a data-plane host, and index
/// lifecycle is a control-plane operation this client is not configured for.
fn pinecone_control_plane_status(operation: &str, resource_name: &str) -> tonic::Status {
    capability_status(
        "pinecone",
        operation,
        "pinecone_control_plane",
        format!(
            "UDB_UNSUPPORTED_OPERATION: the Pinecone DSN is an index data-plane host; \
             '{resource_name}' is a logical collection inside that index and index \
             lifecycle requires the Pinecone control plane"
        ),
    )
}

/// Check that the configured index can hold a logical collection of the
/// requested `dimension` (`describe_index_stats` reports the index dimension).
/// No requested dimension, or no reported dimension, is accepted. Pure.
fn pinecone_index_fits(
    resource_name: &str,
    spec: &JsonValue,
    stats: &JsonValue,
) -> Result<(), tonic::Status> {
    let wanted = spec.get("dimension").and_then(JsonValue::as_i64);
    let actual = stats.get("dimension").and_then(JsonValue::as_i64);
    match (wanted, actual) {
        (Some(wanted), Some(actual)) if wanted > 0 && wanted != actual => Err(capability_status(
            "pinecone",
            "ensure_resource",
            "pinecone_index_dimension",
            format!(
                "Pinecone index dimension is {actual} but collection '{resource_name}' needs \
                 {wanted}; point the DSN at an index of that dimension"
            ),
        )),
        _ => Ok(()),
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
        enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id stamped in vector metadata; AND'd into query/delete filters",
        )
    }
}

impl BackendHealth for PineconeExecutor {
    async fn ping(&self) -> Result<(), String> {
        self.client.ping().await
    }
}

impl QueryExecutor for PineconeExecutor {
    async fn query(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        encode_pinecone_response("query_response_encode", &r)
    }
}

impl MutationExecutor for PineconeExecutor {
    async fn mutate(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        encode_pinecone_response("mutate_response_encode", &r)
    }
}

impl SearchExecutor for PineconeExecutor {
    async fn search(&self, req: &str) -> Result<String, tonic::Status> {
        let (m, p, b) = parse_rest_dispatch(req)?;
        let r = self.client.request_json(m, &p, &b).await?;
        encode_pinecone_response("search_response_encode", &r)
    }
}

impl ObjectExecutor for PineconeExecutor {
    async fn get_object(&self, _: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "pinecone",
            "get_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Pinecone is not an object store",
        ))
    }
    async fn put_object(&self, _: &str, _: Vec<u8>) -> Result<String, tonic::Status> {
        Err(capability_status(
            "pinecone",
            "put_object",
            "object_store",
            "UDB_UNSUPPORTED_OPERATION: Pinecone is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for PineconeExecutor {
    /// The configured DSN is ONE index's data-plane host; index creation lives
    /// on the separate control plane (`api.pinecone.io`), which this client is
    /// not configured for. A logical collection is therefore a metadata
    /// partition of that index (`_collection`, filtered on every query) inside
    /// the project namespace, and "ensure" VERIFIES the index can hold it: the
    /// index must be reachable and its dimension must match the requested one.
    /// A mismatch fails honestly instead of POSTing `/indexes` to the data plane
    /// (which never creates anything).
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(spec_json).map_err(invalid_ensure_resource_spec_status)?;
        let stats = self
            .client
            .request_json(
                reqwest::Method::POST,
                "/describe_index_stats",
                &serde_json::json!({}),
            )
            .await?;
        pinecone_index_fits(resource_name, &spec, &stats)
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        Err(pinecone_control_plane_status(
            "drop_resource",
            resource_name,
        ))
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        Err(pinecone_control_plane_status("list_resources", "*"))
    }
}

impl BackendExecutor for PineconeExecutor {
    async fn transaction(&self, _: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "pinecone",
            "transaction",
            "transactions",
            "UDB_UNSUPPORTED_OPERATION: Pinecone has no transaction primitive",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe("pinecone", self.ping().await))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;

    fn decode_detail(status: &tonic::Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_spec_json_violation(status: &tonic::Status) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, "spec_json");
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "pinecone");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn pinecone_internal_status_carries_typed_detail() {
        let status =
            pinecone_internal_status("query_response_encode", "pinecone response encode failed");
        assert_internal_detail(
            &status,
            "query_response_encode",
            "pinecone response encode failed",
        );
    }

    #[test]
    fn parse_dispatch_works() {
        let req = r#"{"path":"/query","method":"POST","body":{"vector":[0.1]}}"#;
        let (m, p, _) = parse_rest_dispatch(req).unwrap();
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

    #[test]
    fn index_dimension_mismatch_fails_honestly() {
        let stats = serde_json::json!({ "dimension": 768, "namespaces": {} });
        assert!(
            pinecone_index_fits("docs", &serde_json::json!({ "dimension": 768 }), &stats).is_ok()
        );
        assert!(pinecone_index_fits("docs", &serde_json::json!({}), &stats).is_ok());
        let err = pinecone_index_fits("docs", &serde_json::json!({ "dimension": 1536 }), &stats)
            .expect_err("dimension mismatch must refuse");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("768") && err.message().contains("1536"));
    }

    #[test]
    fn index_lifecycle_calls_are_refused_not_sent_to_the_data_plane() {
        let err = pinecone_control_plane_status("drop_resource", "docs");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("control plane"));
    }

    #[test]
    fn ensure_resource_spec_validation_carries_field_violation() {
        let err = serde_json::from_str::<JsonValue>("{")
            .map_err(invalid_ensure_resource_spec_status)
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().starts_with("invalid spec:"));
        assert_spec_json_violation(&err);
    }
}
