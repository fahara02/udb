//! Qdrant HTTP client and vector-store executor.
//! Contains `QdrantHttpClient` (internal) and the `impl DataBrokerRuntime`
//! vector executor methods are kept in `core.rs`; this module owns the
//! HTTP transport layer for the Qdrant API.

// Struct imported via prost_types in method signatures (reqwest bring it in transitively)
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};

use crate::generation::ManifestStore;
use crate::proto::{
    VectorHybridSearchRequest, VectorPoint, VectorPointMutation, VectorSearchRequest, VectorSet,
    VectorUpsertRequest,
};

use crate::runtime::executor_utils::{
    backend_transport_status, build_probe, capability_status, invalid_argument_fields, json_bool,
    json_i32, json_into_struct, json_required_f32_vec, json_required_str, json_scalar_to_string,
    json_to_struct, qdrant_status, store_option, store_option_i32, struct_to_json,
};

/// Build a `VectorPoint` from an OWNED qdrant result point (D.3). The point JSON
/// is discarded right after, so the payload Struct is MOVED out via
/// `Value::take` + `json_into_struct` instead of cloned.
fn point_to_vector_point(mut point: JsonValue) -> VectorPoint {
    let id = point
        .get("id")
        .map(json_scalar_to_string)
        .unwrap_or_default();
    let score = point
        .get("score")
        .and_then(JsonValue::as_f64)
        .unwrap_or_default() as f32;
    let payload = point
        .get_mut("payload")
        .map(JsonValue::take)
        .and_then(json_into_struct);
    VectorPoint { id, score, payload }
}

fn qdrant_point_id(id: &str) -> JsonValue {
    if let Ok(n) = id.parse::<u64>() {
        return json!(n);
    }
    if uuid::Uuid::parse_str(id).is_ok() {
        return json!(id);
    }
    let digest = Sha256::digest(id.as_bytes());
    json!(format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        digest[0],
        digest[1],
        digest[2],
        digest[3],
        digest[4],
        digest[5],
        digest[6] & 0x0f,
        digest[7],
        (digest[8] & 0x3f) | 0x80,
        digest[9],
        digest[10],
        digest[11],
        digest[12],
        digest[13],
        digest[14],
        digest[15],
    ))
}
use crate::runtime::executors::{
    BackendExecutor, BackendHealth, BackendProbe, MutationExecutor, ObjectExecutor, QueryExecutor,
    ResourceAdminExecutor, SearchExecutor,
};

const QDRANT_DEFAULT_SEARCH_LIMIT: i32 = 10;

// ── Collection name validation (GAP 27) ────────────────────────────────

fn qdrant_invalid_field_status(
    field: impl Into<String>,
    description: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    invalid_argument_fields(message, [(field.into(), description.into())])
}

fn invalid_qdrant_request_json_status(err: serde_json::Error) -> tonic::Status {
    qdrant_invalid_field_status(
        "request_json",
        "must be valid JSON for Qdrant generic dispatch",
        format!("invalid request json: {err}"),
    )
}

fn invalid_qdrant_ensure_resource_spec_status(err: serde_json::Error) -> tonic::Status {
    qdrant_invalid_field_status(
        "spec_json",
        "must be valid JSON for Qdrant ensure_resource",
        format!("invalid qdrant ensure_resource spec: {err}"),
    )
}

fn qdrant_required_field_status(field: &'static str, message: &'static str) -> tonic::Status {
    qdrant_invalid_field_status(
        field,
        format!("{field} is required for this Qdrant operation"),
        message,
    )
}

fn unsupported_qdrant_operation_status(operation: &str) -> tonic::Status {
    qdrant_invalid_field_status(
        "operation",
        "unsupported Qdrant mutation operation",
        format!("unsupported Qdrant mutation operation '{operation}'"),
    )
}

fn qdrant_collection_not_found_status(collection: &str) -> tonic::Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "qdrant",
        "collection_check",
        "qdrant_collection_not_found",
        format!("Qdrant collection {collection} is missing"),
    )
}

fn qdrant_internal_status(
    operation: impl Into<String>,
    message: impl Into<String>,
) -> tonic::Status {
    crate::runtime::executor_utils::internal_status("qdrant", operation, message)
}

/// Validate a Qdrant collection name received from a gRPC caller.
///
/// Rejects names that could escape the collections path via path traversal
/// (e.g. `../cluster`, `../snapshots`).  Allowed: ASCII letters, digits,
/// hyphens, and underscores; 1–255 characters; must not start with `.` or `-`.
#[allow(clippy::result_large_err)]
fn validate_collection_name(name: &str) -> Result<(), tonic::Status> {
    if name.is_empty() || name.len() > 255 {
        return Err(qdrant_invalid_field_status(
            "collection",
            "must be 1-255 characters",
            "Qdrant collection name must be 1–255 characters",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        return Err(qdrant_invalid_field_status(
            "collection",
            "may only contain ASCII letters, digits, hyphens, and underscores",
            "Qdrant collection name may only contain ASCII letters, digits, hyphens, and underscores",
        ));
    }
    if name.starts_with('.') || name.starts_with('-') {
        return Err(qdrant_invalid_field_status(
            "collection",
            "may not start with '.' or '-'",
            "Qdrant collection name may not start with '.' or '-'",
        ));
    }
    Ok(())
}

/// URL-percent-encode a validated collection name (defence in depth).
fn encode_collection(name: &str) -> String {
    // After validation only [A-Za-z0-9_-] remain, which are all unreserved
    // characters in RFC 3986 and require no encoding.  We still go through
    // the encoding step so future validators with broader allowed sets stay safe.
    name.chars()
        .flat_map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                vec![c]
            } else {
                format!("%{:02X}", c as u32).chars().collect()
            }
        })
        .collect()
}

// ── Distance normalisation ────────────────────────────────────────────────────

/// Convert a proto-enum distance string to the value expected by the Qdrant REST
/// API.  The manifest stores values like `"VECTOR_DISTANCE_COSINE"` (the raw
/// proto enum name), but Qdrant requires `"Cosine"` / `"Dot"` / `"Euclid"` /
/// `"Manhattan"`.  Also accepts the already-normalised forms so the function is
/// idempotent.
fn normalize_qdrant_distance(raw: &str) -> String {
    let upper = raw.trim().to_ascii_uppercase();
    let canonical = upper
        .trim_start_matches("VECTOR_DISTANCE_")
        .trim_start_matches("DISTANCE_");
    match canonical {
        "COSINE" => "Cosine",
        "DOT" => "Dot",
        "EUCLID" | "EUCLIDEAN" => "Euclid",
        "MANHATTAN" => "Manhattan",
        _ => "Cosine", // safe default for unrecognised or empty
    }
    .to_string()
}

// ── QdrantHttpClient ──────────────────────────────────────────────────────────

/// Lightweight reqwest-based HTTP client for the Qdrant REST API.
#[derive(Debug, Clone)]
pub(crate) struct QdrantHttpClient {
    pub(crate) base_url: String,
    pub(crate) api_key: Option<String>,
    pub(crate) http: reqwest::Client,
}

impl QdrantHttpClient {
    pub(crate) async fn collection_exists(&self, collection: &str) -> Result<(), tonic::Status> {
        validate_collection_name(collection)?;
        let url = format!(
            "{}/collections/{}",
            self.base_url,
            encode_collection(collection)
        );
        let response = self
            .auth(self.http.get(url))
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "collection check", err))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(qdrant_collection_not_found_status(collection));
        }
        qdrant_status(response.status())
    }

    pub(crate) async fn ensure_collection(
        &self,
        store: &ManifestStore,
    ) -> Result<(), tonic::Status> {
        validate_collection_name(&store.resource_name)?;
        let dimension = store_option_i32(store, "dimension").max(1);
        let distance = normalize_qdrant_distance(&store_option(store, "distance"));
        let url = format!(
            "{}/collections/{}",
            self.base_url,
            encode_collection(&store.resource_name)
        );
        let response = self
            .auth(self.http.put(url))
            .json(&json!({
                "vectors": {
                    "size": dimension,
                    "distance": distance,
                }
            }))
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "collection create", err))?;
        let status = response.status();
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(());
        }
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let message = format!("Qdrant collection create failed: HTTP {status}: {body}");
            if status.is_server_error() {
                return Err(crate::runtime::executor_utils::retryable_status(
                    "Qdrant",
                    "collection create",
                    crate::runtime::executor_utils::HTTP_RETRYABLE_BACKOFF_MS,
                    message,
                ));
            }
            return Err(crate::runtime::executor_utils::schema_status(
                tonic::Code::FailedPrecondition,
                "Qdrant",
                "collection create",
                "qdrant_collection_create_rejected",
                message,
            ));
        }
        Ok(())
    }

    pub(crate) async fn search(
        &self,
        request: &VectorSearchRequest,
        filter: JsonValue,
    ) -> Result<VectorSet, tonic::Status> {
        validate_collection_name(&request.collection)?;
        let mut body = json!({
            "vector": request.vector,
            "limit": if request.limit > 0 {
                request.limit
            } else {
                QDRANT_DEFAULT_SEARCH_LIMIT
            },
            "with_payload": request.with_payload,
        });
        if !filter.is_null() {
            body["filter"] = filter;
        }
        if request.score_threshold > 0.0 {
            body["score_threshold"] = json!(request.score_threshold);
        }
        let url = format!(
            "{}/collections/{}/points/search",
            self.base_url,
            encode_collection(&request.collection)
        );
        let response = self
            .auth(self.http.post(url))
            .json(&body)
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "search", err))?;
        qdrant_status(response.status())?;
        let mut payload: JsonValue = response
            .json()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "response decode", err))?;
        let points = payload
            .get_mut("result")
            .and_then(JsonValue::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .map(point_to_vector_point)
            .collect();
        Ok(VectorSet { points })
    }

    pub(crate) async fn upsert(&self, request: &VectorUpsertRequest) -> Result<(), tonic::Status> {
        validate_collection_name(&request.collection)?;
        let points = request
            .points
            .iter()
            .map(|point| {
                json!({
                    "id": qdrant_point_id(&point.id),
                    "vector": point.vector,
                    "payload": point.payload.as_ref().map(struct_to_json).unwrap_or(JsonValue::Null),
                })
            })
            .collect::<Vec<_>>();
        let url = format!(
            "{}/collections/{}/points?wait=true",
            self.base_url,
            encode_collection(&request.collection)
        );
        let response = self
            .auth(self.http.put(url))
            .json(&json!({ "points": points }))
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "upsert", err))?;
        qdrant_status(response.status())?;
        Ok(())
    }

    pub(crate) async fn delete_points(
        &self,
        collection: &str,
        point_ids: &[String],
    ) -> Result<(), tonic::Status> {
        validate_collection_name(collection)?;
        if point_ids.is_empty() {
            return Ok(());
        }
        let url = format!(
            "{}/collections/{}/points/delete?wait=true",
            self.base_url,
            encode_collection(collection)
        );
        let response = self
            .auth(self.http.post(url))
            .json(&json!({
                "points": point_ids.iter().map(|id| qdrant_point_id(id)).collect::<Vec<_>>()
            }))
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "delete", err))?;
        qdrant_status(response.status())?;
        Ok(())
    }

    pub(crate) async fn delete_by_filter(
        &self,
        collection: &str,
        filter: JsonValue,
    ) -> Result<(), tonic::Status> {
        validate_collection_name(collection)?;
        let url = format!(
            "{}/collections/{}/points/delete?wait=true",
            self.base_url,
            encode_collection(collection)
        );
        let response = self
            .auth(self.http.post(url))
            .json(&json!({ "filter": filter }))
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "filtered delete", err))?;
        qdrant_status(response.status())?;
        Ok(())
    }

    pub(crate) async fn set_payload(
        &self,
        collection: &str,
        payload: JsonValue,
        point_ids: Option<Vec<String>>,
        filter: Option<JsonValue>,
    ) -> Result<(), tonic::Status> {
        validate_collection_name(collection)?;
        let mut body = json!({ "payload": payload });
        if let Some(ids) = point_ids {
            body["points"] = json!(ids);
        }
        if let Some(filter) = filter {
            body["filter"] = filter;
        }
        let url = format!(
            "{}/collections/{}/points/payload?wait=true",
            self.base_url,
            encode_collection(collection)
        );
        let response = self
            .auth(self.http.post(url))
            .json(&body)
            .send()
            .await
            .map_err(|err| backend_transport_status("Qdrant", "payload patch", err))?;
        qdrant_status(response.status())?;
        Ok(())
    }

    /// True hybrid search using Qdrant's `/points/query` RRF API (Qdrant ≥ v1.7).
    ///
    /// GAP 27 fix: collection name is validated and URL-encoded before use.
    pub(crate) async fn hybrid_search(
        &self,
        request: &VectorHybridSearchRequest,
        filter: JsonValue,
    ) -> Result<VectorSet, tonic::Status> {
        validate_collection_name(&request.collection)?;
        let limit = if request.limit > 0 {
            request.limit as usize
        } else {
            QDRANT_DEFAULT_SEARCH_LIMIT as usize
        };
        let text_query = request.text_query.trim().to_lowercase();
        let dense_weight = request.fusion_weights.first().copied().unwrap_or(0.7) as f64;
        let sparse_weight = request.fusion_weights.get(1).copied().unwrap_or(0.3) as f64;

        // ── Tier 1: Qdrant native /points/query with RRF ──────────────────────
        if !request.vector.is_empty() {
            let prefetch_limit = (limit * 4).max(50);
            let mut prefetch = vec![json!({
                "query": request.vector,
                "limit": prefetch_limit
            })];

            // Add a text-match prefetch if text_query is provided so Qdrant can
            // also score by lexical relevance in collections that have a
            // payload text index set up.
            if !text_query.is_empty() {
                prefetch.push(json!({
                    "query": request.vector,
                    "filter": {
                        "must": [{
                            "key": "_full_text",
                            "match": { "text": text_query }
                        }]
                    },
                    "limit": prefetch_limit
                }));
            }

            let mut query_body = json!({
                "prefetch": prefetch,
                "query": { "fusion": "rrf" },
                "limit": (limit * 2).max(20),
                "with_payload": true,
            });
            if !filter.is_null() {
                query_body["filter"] = filter.clone();
            }

            let url = format!(
                "{}/collections/{}/points/query",
                self.base_url,
                encode_collection(&request.collection)
            );
            if let Ok(resp) = self
                .auth(self.http.post(&url))
                .json(&query_body)
                .send()
                .await
                && resp.status().is_success()
                && let Ok(payload) = resp.json::<JsonValue>().await
            {
                let points = self.parse_query_response(payload);
                let reranked = if text_query.is_empty() {
                    points.into_iter().take(limit).collect()
                } else {
                    rerank_with_text(points, &text_query, dense_weight, sparse_weight, limit)
                };
                return Ok(VectorSet { points: reranked });
            }
        }

        // ── Tier 2: Dense search + local lexical re-ranking fallback ──────────
        let fetch_limit = if text_query.is_empty() {
            limit as i32
        } else {
            (limit * 4).max(40) as i32
        };
        let dense_req = VectorSearchRequest {
            context: request.context.clone(),
            collection: request.collection.clone(),
            vector: request.vector.clone(),
            filter: request.filter.clone(),
            limit: fetch_limit,
            score_threshold: 0.0,
            with_payload: true,
        };
        let result = self.search(&dense_req, filter).await?;
        if text_query.is_empty() {
            return Ok(result);
        }
        let reranked = rerank_with_text(
            result.points,
            &text_query,
            dense_weight,
            sparse_weight,
            limit,
        );
        Ok(VectorSet { points: reranked })
    }

    /// Parse the result array from a Qdrant `/points/query` response.
    fn parse_query_response(&self, mut payload: JsonValue) -> Vec<VectorPoint> {
        payload
            .get_mut("result")
            .and_then(JsonValue::as_array_mut)
            .map(std::mem::take)
            .unwrap_or_default()
            .into_iter()
            .map(point_to_vector_point)
            .collect()
    }

    pub(crate) fn auth(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let Some(api_key) = &self.api_key else {
            return builder;
        };
        // Validate the value up front so an invalid key fails with a message that
        // NAMES the header and the likely cause, instead of an opaque reqwest
        // "builder error" surfaced later at send() (UDB_FRICTION §3).
        match reqwest::header::HeaderValue::from_str(api_key) {
            Ok(value) => builder.header("api-key", value),
            Err(err) => {
                tracing::error!(
                    "Qdrant 'api-key' header value is invalid ({err}); it likely contains \
                     whitespace/control characters (e.g. a trailing CRLF from a Windows .env). \
                     The request will fail — set UDB_QDRANT_API_KEY to a clean value"
                );
                builder.header("api-key", api_key)
            }
        }
    }
}

// ── Hybrid search helpers ─────────────────────────────────────────────────────

/// Re-rank `points` by combining the normalised dense vector score with a
/// lexical text score computed against `text_query`.
///
/// Scoring: `combined = dw * (dense / max_dense) + sw * lexical_score`
/// where `dw + sw = 1` after normalisation.
fn rerank_with_text(
    points: Vec<VectorPoint>,
    text_query: &str,
    dense_weight: f64,
    sparse_weight: f64,
    limit: usize,
) -> Vec<VectorPoint> {
    if points.is_empty() {
        return vec![];
    }
    let query_tokens: Vec<String> = text_query
        .split_whitespace()
        .map(|t| t.to_lowercase())
        .collect();
    if query_tokens.is_empty() {
        return points.into_iter().take(limit).collect();
    }

    // Normalise weights.
    let total = dense_weight + sparse_weight;
    let (dw, sw) = if total > 0.0 {
        (dense_weight / total, sparse_weight / total)
    } else {
        (0.5, 0.5)
    };

    let max_dense = points
        .iter()
        .map(|p| p.score as f64)
        .fold(0.0_f64, f64::max)
        .max(1e-9);

    let mut scored: Vec<(f32, VectorPoint)> = points
        .into_iter()
        .map(|p| {
            let text_score = p
                .payload
                .as_ref()
                .map(|pl| lexical_score(&payload_to_text(pl), &query_tokens))
                .unwrap_or(0.0);
            let norm_dense = p.score as f64 / max_dense;
            let combined = (dw * norm_dense + sw * text_score) as f32;
            (combined, p)
        })
        .collect();

    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored
        .into_iter()
        .take(limit)
        .map(|(combined_score, mut p)| {
            p.score = combined_score;
            p
        })
        .collect()
}

/// Flatten all string/number fields of a `prost_types::Struct` payload into a
/// single lower-cased string for lexical scoring.
fn payload_to_text(payload: &prost_types::Struct) -> String {
    payload
        .fields
        .values()
        .map(|v| match &v.kind {
            Some(prost_types::value::Kind::StringValue(s)) => s.to_lowercase(),
            Some(prost_types::value::Kind::NumberValue(n)) => n.to_string(),
            _ => String::new(),
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Simple token-overlap lexical score in [0, 1].
///
/// Score = (number of query tokens found in `text`) / (total query tokens).
/// Each token is counted at most once (set intersection semantics).
fn lexical_score(text: &str, tokens: &[String]) -> f64 {
    if text.is_empty() || tokens.is_empty() {
        return 0.0;
    }
    let matched = tokens.iter().filter(|t| text.contains(t.as_str())).count();
    matched as f64 / tokens.len() as f64
}

// ── QdrantExecutor: BackendExecutor over QdrantHttpClient ───────────────────────
// Newtype wrapper so the generic-dispatch trait methods (search/mutate/...) do not
// collide with the low-level inherent methods on QdrantHttpClient. Stateless leaf
// I/O; instance selection / circuit-breaking stay in the orchestration layer.
// Request shapes mirror the former inline arms in core.rs.

pub(crate) struct QdrantExecutor(pub(crate) QdrantHttpClient);

impl crate::runtime::backend_context::BackendContextEnforcer for QdrantExecutor {
    fn backend_label(&self) -> &str {
        "qdrant"
    }

    fn enforce(
        &self,
        ctx: &crate::runtime::backend_context::AppliedContext,
    ) -> crate::runtime::backend_context::ContextEffect {
        // C7/C8: the Qdrant IR compiler now stamps `_tenant_id` /
        // `_project_id` onto every point payload at write time AND
        // ANDs them into the `must` clause of every read/search/
        // delete filter. Tenant boundary is protocol-enforced —
        // a cross-tenant search can't surface points across the
        // boundary.
        crate::runtime::backend_context::enforce_with_mechanism(
            ctx,
            "_tenant_id / _project_id payload stamps; AND'd into must-filters",
        )
    }
}

impl BackendHealth for QdrantExecutor {
    async fn ping(&self) -> Result<(), String> {
        // Loose reachability probe (mirrors the former ping arm: a missing sentinel
        // collection still counts as reachable).
        let _ = self.0.collection_exists("__ping__").await;
        Ok(())
    }
}

impl QueryExecutor for QdrantExecutor {
    async fn query(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "qdrant",
            "query",
            "generic_query",
            "qdrant does not support generic query; use search",
        ))
    }
}

impl SearchExecutor for QdrantExecutor {
    /// `{"collection","vector","filter?","limit?","score_threshold?","with_payload?","text_query?","fusion_weights?"}`.
    async fn search(&self, request_json: &str) -> Result<String, tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_qdrant_request_json_status)?;
        let collection = json_required_str(&spec, "collection")?;
        let vector = json_required_f32_vec(&spec, "vector")?;
        let filter = spec.get("filter").cloned().unwrap_or(JsonValue::Null);
        let limit = json_i32(&spec, "limit").unwrap_or(QDRANT_DEFAULT_SEARCH_LIMIT);
        let with_payload = json_bool(&spec, "with_payload").unwrap_or(true);
        let result = if let Some(text_query) = spec
            .get("text_query")
            .and_then(JsonValue::as_str)
            .filter(|value| !value.trim().is_empty())
        {
            let fusion_weights = spec
                .get("fusion_weights")
                .and_then(JsonValue::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_f64().map(|number| number as f32))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let request = VectorHybridSearchRequest {
                context: None,
                collection: collection.to_string(),
                vector,
                text_query: text_query.to_string(),
                filter: json_to_struct(&filter),
                limit,
                fusion_weights,
                with_payload,
            };
            self.0.hybrid_search(&request, filter).await?
        } else {
            let request = VectorSearchRequest {
                context: None,
                collection: collection.to_string(),
                vector,
                filter: json_to_struct(&filter),
                limit,
                score_threshold: spec
                    .get("score_threshold")
                    .and_then(JsonValue::as_f64)
                    .unwrap_or_default() as f32,
                with_payload,
            };
            self.0.search(&request, filter).await?
        };
        let points = result
            .points
            .iter()
            .map(|point| {
                json!({
                    "id": point.id,
                    "score": point.score,
                    "payload": point.payload.as_ref().map(struct_to_json).unwrap_or(JsonValue::Null)
                })
            })
            .collect::<Vec<_>>();
        serde_json::to_string(&points)
            .map_err(|e| qdrant_internal_status("search_result_encode", e.to_string()))
    }
}

impl MutationExecutor for QdrantExecutor {
    /// `{"operation":"upsert"|"delete", "collection", ...}`.
    async fn mutate(&self, request_json: &str) -> Result<String, tonic::Status> {
        let mut spec: JsonValue =
            serde_json::from_str(request_json).map_err(invalid_qdrant_request_json_status)?;
        // Owned op string so the match doesn't hold a borrow of `spec` (the upsert
        // arm needs `get_mut` to move the points array out).
        let operation = spec
            .get("operation")
            .and_then(JsonValue::as_str)
            .unwrap_or("upsert")
            .to_string();
        match operation.as_str() {
            "upsert" | "upsert_points" => {
                let collection = json_required_str(&spec, "collection")?.to_string();
                let idempotency_key = spec
                    .get("idempotency_key")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string();
                // D.3: take the points array out of `spec` (owned) and move each
                // payload Struct instead of cloning it.
                let point_specs = spec
                    .get_mut("points")
                    .and_then(JsonValue::as_array_mut)
                    .map(std::mem::take)
                    .ok_or_else(|| {
                        qdrant_required_field_status("points", "points must be an array")
                    })?;
                let mut points = Vec::with_capacity(point_specs.len());
                for mut point in point_specs {
                    points.push(VectorPointMutation {
                        id: json_required_str(&point, "id")?.to_string(),
                        vector: json_required_f32_vec(&point, "vector")?,
                        payload: point
                            .get_mut("payload")
                            .map(JsonValue::take)
                            .and_then(json_into_struct),
                    });
                }
                let request = VectorUpsertRequest {
                    context: None,
                    collection: collection.clone(),
                    points,
                    idempotency_key,
                };
                self.0.upsert(&request).await?;
                Ok(json!({
                    "resource_uri": format!("vector://{collection}"),
                    "affected_rows": request.points.len()
                })
                .to_string())
            }
            "delete" | "delete_points" => {
                let collection = json_required_str(&spec, "collection")?;
                if let Some(filter) = spec.get("filter").cloned() {
                    self.0.delete_by_filter(collection, filter).await?;
                    return Ok(json!({
                        "resource_uri": format!("vector://{collection}"),
                        "affected_rows": 0,
                        "matched_by_filter": true
                    })
                    .to_string());
                }
                let point_ids = spec
                    .get("point_ids")
                    .or_else(|| spec.get("ids"))
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        qdrant_required_field_status(
                            "point_ids",
                            "point_ids must be an array when filter is absent",
                        )
                    })?
                    .iter()
                    .map(json_scalar_to_string)
                    .collect::<Vec<_>>();
                self.0.delete_points(collection, &point_ids).await?;
                Ok(json!({
                    "resource_uri": format!("vector://{collection}"),
                    "affected_rows": point_ids.len()
                })
                .to_string())
            }
            "set_payload" | "patch_payload" | "upsert_payload" => {
                let collection = json_required_str(&spec, "collection")?;
                let payload = spec.get("payload").cloned().ok_or_else(|| {
                    qdrant_required_field_status("payload", "payload is required")
                })?;
                let point_ids = spec
                    .get("point_ids")
                    .or_else(|| spec.get("ids"))
                    .and_then(|ids| {
                        ids.as_array().map(|values| {
                            values.iter().map(json_scalar_to_string).collect::<Vec<_>>()
                        })
                    });
                let filter = spec.get("filter").cloned();
                if point_ids.is_none() && filter.is_none() {
                    return Err(qdrant_required_field_status(
                        "point_ids",
                        "set_payload requires point_ids or filter",
                    ));
                }
                self.0
                    .set_payload(collection, payload, point_ids, filter)
                    .await?;
                Ok(json!({
                    "resource_uri": format!("vector://{collection}"),
                    "affected_rows": 0
                })
                .to_string())
            }
            other => Err(unsupported_qdrant_operation_status(other)),
        }
    }
}

impl ObjectExecutor for QdrantExecutor {
    async fn get_object(&self, _request_json: &str) -> Result<Vec<u8>, tonic::Status> {
        Err(capability_status(
            "qdrant",
            "get_object",
            "object_store",
            "qdrant is not an object store",
        ))
    }
    async fn put_object(
        &self,
        _request_json: &str,
        _bytes: Vec<u8>,
    ) -> Result<String, tonic::Status> {
        Err(capability_status(
            "qdrant",
            "put_object",
            "object_store",
            "qdrant is not an object store",
        ))
    }
}

impl ResourceAdminExecutor for QdrantExecutor {
    async fn ensure_resource(
        &self,
        resource_name: &str,
        spec_json: &str,
    ) -> Result<(), tonic::Status> {
        let spec: JsonValue =
            serde_json::from_str(spec_json).map_err(invalid_qdrant_ensure_resource_spec_status)?;
        let dimension = spec
            .get("dimension")
            .or_else(|| spec.get("vector_size"))
            .or_else(|| spec.get("size"))
            .or_else(|| spec.get("vectors").and_then(|vectors| vectors.get("size")))
            .and_then(JsonValue::as_i64)
            .unwrap_or(4)
            .max(1)
            .to_string();
        let distance = spec
            .get("distance")
            .or_else(|| {
                spec.get("vectors")
                    .and_then(|vectors| vectors.get("distance"))
            })
            .and_then(JsonValue::as_str)
            .unwrap_or("cosine")
            .to_string();
        let store = ManifestStore {
            store_kind: "vector".to_string(),
            backend: "qdrant".to_string(),
            logical_name: resource_name.to_string(),
            resource_name: resource_name.to_string(),
            options: vec![
                crate::generation::ManifestStoreOption {
                    key: "dimension".to_string(),
                    value: dimension,
                },
                crate::generation::ManifestStoreOption {
                    key: "distance".to_string(),
                    value: distance,
                },
            ],
            ..ManifestStore::default()
        };
        self.0.ensure_collection(&store).await
    }
    async fn drop_resource(&self, resource_name: &str) -> Result<(), tonic::Status> {
        let url = format!("{}/collections/{}", self.0.base_url, resource_name);
        let resp = self
            .0
            .http
            .delete(url)
            .send()
            .await
            .map_err(|e| backend_transport_status("qdrant", "delete", e))?;
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(qdrant_internal_status(
                "drop_resource",
                format!("qdrant drop_resource status: {}", resp.status()),
            ))
        }
    }
    async fn list_resources(&self) -> Result<Vec<String>, tonic::Status> {
        let url = format!("{}/collections", self.0.base_url);
        let resp = self
            .0
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| backend_transport_status("qdrant", "list", e))?;
        let body: JsonValue = resp
            .json()
            .await
            .map_err(|e| backend_transport_status("qdrant", "list parse", e))?;
        let names = body
            .get("result")
            .and_then(|r| r.get("collections"))
            .and_then(|c| c.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| item.get("name").and_then(|n| n.as_str()))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(names)
    }
}

impl BackendExecutor for QdrantExecutor {
    async fn transaction(&self, _request_json: &str) -> Result<String, tonic::Status> {
        Err(capability_status(
            "qdrant",
            "transaction",
            "transactions",
            "qdrant does not support transactions",
        ))
    }
    async fn probe(&self) -> Result<BackendProbe, tonic::Status> {
        Ok(build_probe(
            "qdrant",
            <Self as BackendHealth>::ping(self).await,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use prost::Message as _;

    fn exec() -> QdrantExecutor {
        QdrantExecutor(QdrantHttpClient {
            base_url: "http://localhost:6333".to_string(),
            api_key: None,
            http: reqwest::Client::new(),
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

    fn assert_schema_detail(
        status: &tonic::Status,
        backend: &str,
        operation: &str,
        schema_code: &str,
        message: &str,
    ) {
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, backend);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.capability_required, schema_code);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
        assert!(detail.field_violations.is_empty());
    }

    fn assert_internal_detail(status: &tonic::Status, operation: &str, message: &str) {
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), message);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Internal as i32);
        assert_eq!(detail.backend, "qdrant");
        assert_eq!(detail.operation, operation);
        assert!(!detail.retryable);
    }

    #[test]
    fn qdrant_internal_status_carries_typed_detail() {
        let status = qdrant_internal_status("drop_resource", "qdrant drop_resource status: 500");
        assert_internal_detail(&status, "drop_resource", "qdrant drop_resource status: 500");
    }

    #[test]
    fn qdrant_collection_not_found_carries_schema_detail() {
        assert_schema_detail(
            &qdrant_collection_not_found_status("items"),
            "qdrant",
            "collection_check",
            "qdrant_collection_not_found",
            "Qdrant collection items is missing",
        );
    }

    #[test]
    fn qdrant_collection_validation_carries_field_violations() {
        for (name, message) in [
            ("", "Qdrant collection name must be 1–255 characters"),
            (
                "bad/name",
                "Qdrant collection name may only contain ASCII letters, digits, hyphens, and underscores",
            ),
            (
                "-bad",
                "Qdrant collection name may not start with '.' or '-'",
            ),
        ] {
            let err = validate_collection_name(name).unwrap_err();
            assert_eq!(err.message(), message);
            assert_single_field(&err, "collection");
        }
    }

    #[tokio::test]
    async fn qdrant_search_validation_carries_field_violations() {
        let e = exec();

        let invalid_json = SearchExecutor::search(&e, "not json").await.unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid request json:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_collection = SearchExecutor::search(&e, r#"{"vector":[1,2,3]}"#)
            .await
            .unwrap_err();
        assert_eq!(missing_collection.message(), "collection is required");
        assert_single_field(&missing_collection, "collection");
    }

    #[tokio::test]
    async fn qdrant_mutation_validation_carries_field_violations() {
        let e = exec();

        let invalid_json = MutationExecutor::mutate(&e, "not json").await.unwrap_err();
        assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
        assert!(invalid_json.message().starts_with("invalid request json:"));
        assert_single_field(&invalid_json, "request_json");

        let missing_points =
            MutationExecutor::mutate(&e, r#"{"operation":"upsert","collection":"items"}"#)
                .await
                .unwrap_err();
        assert_eq!(missing_points.message(), "points must be an array");
        assert_single_field(&missing_points, "points");

        let missing_point_ids =
            MutationExecutor::mutate(&e, r#"{"operation":"delete","collection":"items"}"#)
                .await
                .unwrap_err();
        assert_eq!(
            missing_point_ids.message(),
            "point_ids must be an array when filter is absent"
        );
        assert_single_field(&missing_point_ids, "point_ids");

        let missing_payload =
            MutationExecutor::mutate(&e, r#"{"operation":"set_payload","collection":"items"}"#)
                .await
                .unwrap_err();
        assert_eq!(missing_payload.message(), "payload is required");
        assert_single_field(&missing_payload, "payload");

        let missing_selector = MutationExecutor::mutate(
            &e,
            r#"{"operation":"set_payload","collection":"items","payload":{"a":1}}"#,
        )
        .await
        .unwrap_err();
        assert_eq!(
            missing_selector.message(),
            "set_payload requires point_ids or filter"
        );
        assert_single_field(&missing_selector, "point_ids");

        let unsupported = MutationExecutor::mutate(&e, r#"{"operation":"bogus"}"#)
            .await
            .unwrap_err();
        assert_eq!(
            unsupported.message(),
            "unsupported Qdrant mutation operation 'bogus'"
        );
        assert_single_field(&unsupported, "operation");
    }

    #[tokio::test]
    async fn qdrant_resource_spec_validation_carries_field_violation() {
        let e = exec();
        let err = ResourceAdminExecutor::ensure_resource(&e, "items", "{")
            .await
            .unwrap_err();
        assert!(
            err.message()
                .starts_with("invalid qdrant ensure_resource spec:")
        );
        assert_single_field(&err, "spec_json");
    }

    #[tokio::test]
    async fn qdrant_executor_rejects_unsupported_and_malformed() {
        let e = exec();
        assert!(QueryExecutor::query(&e, "{}").await.is_err());
        assert!(ObjectExecutor::get_object(&e, "{}").await.is_err());
        assert!(BackendExecutor::transaction(&e, "{}").await.is_err());
        assert!(SearchExecutor::search(&e, "not json").await.is_err());
        assert!(SearchExecutor::search(&e, "{}").await.is_err()); // missing collection
        assert!(
            MutationExecutor::mutate(&e, r#"{"operation":"bogus"}"#)
                .await
                .is_err()
        );
    }
}
