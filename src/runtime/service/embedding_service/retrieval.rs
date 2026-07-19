use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::embedding::services::v1 as embedding_pb;
use crate::proto::{VectorHybridSearchRequest, VectorPoint, VectorSearchRequest, VectorSet};
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_service_context, validate_request_tenant,
};
use super::EmbeddingServiceImpl;
use super::config::{
    STATUS_ACTIVE, STATUS_DEPRECATED, TOPIC_RETRIEVAL_EVALUATED, TOPIC_RETRIEVAL_SAMPLED,
    embedding_max_chunks_per_row, resolve_top_k, retrieval_eval_sample_rate,
    retrieve_fusion_weights, retrieve_score_threshold,
};
use super::documents::document_source_name;
use super::errors::{
    embedding_field_violation, embedding_required_field, embedding_source_not_found_status,
};
use super::handlers::{parse_grpc_timeout, remaining_before_deadline};
use super::model::{merge_retrieve_scope_filter, stored_model_from_json, stored_source_from_json};
use super::store::{model_read_by_id, source_read_by_name};
use super::vector_store::VectorStore as _;

fn cosine(left: &[f32], right: &[f32]) -> f32 {
    if left.is_empty() || left.len() != right.len() {
        return 0.0;
    }
    let mut dot = 0.0f64;
    let mut left_norm = 0.0f64;
    let mut right_norm = 0.0f64;
    for (left, right) in left.iter().zip(right) {
        dot += f64::from(*left) * f64::from(*right);
        left_norm += f64::from(*left) * f64::from(*left);
        right_norm += f64::from(*right) * f64::from(*right);
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        0.0
    } else {
        (dot / (left_norm.sqrt() * right_norm.sqrt())) as f32
    }
}

pub(crate) fn mmr_select(
    mut candidates: Vec<VectorPoint>,
    query: &[f32],
    limit: usize,
    lambda: f32,
) -> Vec<VectorPoint> {
    let lambda = lambda.clamp(0.0, 1.0);
    let mut selected = Vec::with_capacity(limit.min(candidates.len()));
    while !candidates.is_empty() && selected.len() < limit {
        let mut best_index = 0usize;
        let mut best_score = f32::NEG_INFINITY;
        for (index, candidate) in candidates.iter().enumerate() {
            let relevance = if candidate.vector.is_empty() {
                candidate.score
            } else {
                cosine(query, &candidate.vector)
            };
            let redundancy = selected
                .iter()
                .map(|chosen: &VectorPoint| {
                    if candidate.vector.is_empty() || chosen.vector.is_empty() {
                        0.0
                    } else {
                        cosine(&candidate.vector, &chosen.vector)
                    }
                })
                .fold(0.0f32, f32::max);
            let score = lambda * relevance - (1.0 - lambda) * redundancy;
            if score > best_score {
                best_score = score;
                best_index = index;
            }
        }
        selected.push(candidates.swap_remove(best_index));
    }
    selected
}

fn payload_json(point: &VectorPoint) -> serde_json::Value {
    point
        .payload
        .as_ref()
        .map(crate::runtime::executor_utils::struct_to_json)
        .unwrap_or_else(|| serde_json::json!({}))
}

fn payload_string(point: &VectorPoint, key: &str) -> String {
    payload_json(point)
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn payload_i32(point: &VectorPoint, key: &str) -> i32 {
    payload_json(point)
        .get(key)
        .and_then(serde_json::Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

fn public_payload(point: &VectorPoint, context_text: Option<&str>) -> String {
    let mut json = payload_json(point);
    let Some(object) = json.as_object_mut() else {
        return String::new();
    };
    for key in ["_tenant_id", "_source", "_model_id", "_indexed_at_unix_ms"] {
        object.remove(key);
    }
    for (internal, public) in [
        ("_parent_pk", "parent_pk"),
        ("_chunk_seq", "chunk_seq"),
        ("_chunk_text", "text"),
        ("_document_id", "document_id"),
        ("_doc_version", "doc_version"),
        ("_chunk_hash", "content_hash"),
    ] {
        if let Some(value) = object.remove(internal) {
            object.insert(public.to_string(), value);
        }
    }
    if let Some(context) = context_text.filter(|value| !value.is_empty()) {
        object.insert(
            "context_text".to_string(),
            serde_json::Value::String(context.to_string()),
        );
    }
    if object.is_empty() {
        String::new()
    } else {
        serde_json::Value::Object(std::mem::take(object)).to_string()
    }
}

async fn rerank_points(
    query: &str,
    config: &embedding_pb::RerankConfig,
    points: Vec<VectorPoint>,
) -> Result<(Vec<VectorPoint>, BTreeMap<String, f64>, bool), Status> {
    if !config.enabled {
        return Ok((points, BTreeMap::new(), false));
    }
    let endpoint = std::env::var("UDB_EMBEDDING_RERANK_URL").unwrap_or_default();
    if endpoint.trim().is_empty() {
        if config.fail_open {
            return Ok((points, BTreeMap::new(), false));
        }
        return Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            "rerank",
            "embedding_rerank_sidecar",
            "rerank requested but UDB_EMBEDDING_RERANK_URL is not configured",
        ));
    }
    if query.trim().is_empty() {
        return Err(embedding_required_field(
            "query_text",
            "is required for sidecar reranking",
            "rerank requires query_text",
        ));
    }
    let candidates = points
        .iter()
        .map(|point| {
            serde_json::json!({
                "id": point.id, "text": payload_string(point, "_chunk_text"), "score": point.score,
            })
        })
        .collect::<Vec<_>>();
    let body = serde_json::json!({
        "query": query, "model": config.model, "strategy": config.strategy,
        "top_n": config.top_n, "candidates": candidates,
    });
    // Slim builds (`--no-default-features --features postgres`) carry no HTTP
    // client at all — honest capability degradation instead of a compile-time
    // reqwest dependency: fail_open configs skip the rerank, strict configs get
    // a typed capability error naming the missing feature.
    #[cfg(not(feature = "http-client"))]
    {
        let _ = body;
        if config.fail_open {
            return Ok((points, BTreeMap::new(), false));
        }
        return Err(crate::runtime::executor_utils::capability_status(
            "embedding",
            "rerank",
            "http_client_feature",
            "rerank requires a broker built with the http-client feature",
        ));
    }
    #[cfg(feature = "http-client")]
    {
        let response = reqwest::Client::new()
            .post(endpoint.trim())
            .json(&body)
            .timeout(Duration::from_secs(10))
            .send()
            .await;
        let decoded = match response {
            Ok(response) if response.status().is_success() => {
                response.json::<serde_json::Value>().await.ok()
            }
            _ => None,
        };
        let Some(decoded) = decoded else {
            if config.fail_open {
                return Ok((points, BTreeMap::new(), false));
            }
            return Err(crate::runtime::executor_utils::retryable_status(
                "embedding",
                "rerank",
                500,
                "embedding reranker sidecar is unavailable",
            ));
        };
        let mut scores = BTreeMap::new();
        for result in decoded
            .get("results")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            if let (Some(id), Some(score)) = (
                result.get("id").and_then(serde_json::Value::as_str),
                result.get("score").and_then(serde_json::Value::as_f64),
            ) {
                scores.insert(id.to_string(), score);
            }
        }
        if scores.is_empty() {
            if config.fail_open {
                return Ok((points, scores, false));
            }
            return Err(crate::runtime::executor_utils::schema_status(
                tonic::Code::FailedPrecondition,
                "embedding",
                "rerank",
                "embedding_rerank_response_invalid",
                "reranker returned no candidate scores",
            ));
        }
        let mut points = points;
        points.sort_by(|left, right| {
            scores
                .get(&right.id)
                .copied()
                .unwrap_or(f64::NEG_INFINITY)
                .total_cmp(&scores.get(&left.id).copied().unwrap_or(f64::NEG_INFINITY))
        });
        Ok((points, scores, true))
    }
}

pub(crate) async fn retrieve(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::RetrieveRequest>,
) -> Result<Response<embedding_pb::RetrieveResponse>, Status> {
    let started = Instant::now();
    let metadata = request.metadata().clone();
    let deadline = metadata
        .get("grpc-timeout")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_grpc_timeout)
        .map(|budget| Instant::now() + budget);
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let source_name = req.source_name.trim().to_string();
    if source_name.is_empty() {
        return Err(embedding_required_field(
            "source_name",
            "must be non-empty",
            "source_name is required",
        ));
    }
    if req.query_vector.is_empty() {
        return Err(embedding_required_field(
            "query_vector",
            "must contain an already-computed query embedding",
            "query_vector is required",
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Vector,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let source = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            source_read_by_name(&tenant_id, &source_name),
        )
        .await?
        .first()
        .map(stored_source_from_json)
        .filter(|source| source.status == STATUS_ACTIVE);
    let model_id = source
        .as_ref()
        .map(|source| source.model_id.clone())
        .or_else(|| source_name.strip_prefix("documents:").map(str::to_string))
        .ok_or_else(|| embedding_source_not_found_status("retrieve"))?;
    let model = runtime
        .native_entity_read_for_service(
            "embedding",
            &context,
            model_read_by_id(&tenant_id, &model_id),
        )
        .await?
        .first()
        .map(stored_model_from_json)
        .filter(|model| {
            model.tenant_state == STATUS_ACTIVE
                && matches!(model.status.as_str(), STATUS_ACTIVE | STATUS_DEPRECATED)
        })
        .ok_or_else(|| {
            embedding_field_violation(
                "source_name",
                "must resolve to an active/deprecated model",
                "embedding model unavailable",
            )
        })?;
    if source.is_none() && source_name != document_source_name(&model.model_id) {
        return Err(embedding_source_not_found_status("retrieve"));
    }
    if req.query_vector.len() != model.dimensions as usize {
        return Err(embedding_field_violation(
            "query_vector",
            format!("must contain exactly {} model dimensions", model.dimensions),
            format!(
                "query vector has {} dimensions; model requires {}",
                req.query_vector.len(),
                model.dimensions
            ),
        ));
    }
    let top_k = resolve_top_k(req.top_k) as usize;
    let mmr = req.mmr.as_ref().filter(|config| config.enabled);
    if let Some(mmr) = mmr
        && !(0.0..=1.0).contains(&mmr.lambda)
    {
        return Err(embedding_field_violation(
            "mmr.lambda",
            "must be between 0 and 1",
            "invalid MMR lambda",
        ));
    }
    let rerank_top = req
        .rerank
        .as_ref()
        .filter(|config| config.enabled)
        .map_or(0usize, |config| config.top_n.max(top_k as i32) as usize);
    let candidate_limit = top_k
        .max(rerank_top)
        .max(if mmr.is_some() {
            top_k.saturating_mul(4)
        } else {
            top_k
        })
        .min(800);
    let score_floor = {
        let configured = retrieve_score_threshold();
        if req.score_threshold > 0.0 {
            (req.score_threshold as f32).max(configured)
        } else {
            configured
        }
    };
    let filter = merge_retrieve_scope_filter(&tenant_id, &source_name, &req.filter_json, None)?;
    let with_vector = req.include_vectors || mmr.is_some();
    let store = svc.vector_store_for_model(&context.project_id, &model)?;
    let remaining = remaining_before_deadline(deadline, Instant::now())?;
    let search = async {
        if req.query_text.trim().is_empty() {
            store
                .search(&VectorSearchRequest {
                    context: None,
                    collection: model.collection().to_string(),
                    vector: req.query_vector.clone(),
                    filter: crate::runtime::executor_utils::json_to_struct(&filter),
                    limit: candidate_limit as i32,
                    score_threshold: score_floor,
                    with_payload: true,
                    with_vector,
                    vector_name: req.vector_name.trim().to_string(),
                    quantization_rescore: model.rescore,
                })
                .await
        } else {
            store
                .hybrid_search(&VectorHybridSearchRequest {
                    context: None,
                    collection: model.collection().to_string(),
                    vector: req.query_vector.clone(),
                    text_query: req.query_text.clone(),
                    filter: crate::runtime::executor_utils::json_to_struct(&filter),
                    limit: candidate_limit as i32,
                    fusion_weights: retrieve_fusion_weights(),
                    with_payload: true,
                    with_vector,
                    vector_name: req.vector_name.trim().to_string(),
                    fusion_strategy: req.fusion,
                    prefetch_limit: req.prefetch_limit.max(candidate_limit as i32),
                    quantization_rescore: model.rescore,
                })
                .await
        }
    };
    let result: VectorSet = match remaining {
        Some(budget) => tokio::time::timeout(budget, search).await.map_err(|_| {
            crate::runtime::executor_utils::deadline_exceeded_status(
                "embedding",
                "retrieve",
                250,
                "retrieve exceeded its deadline",
            )
        })??,
        None => search.await?,
    };
    let mut points = result
        .points
        .into_iter()
        .filter(|point| point.score >= score_floor)
        .collect::<Vec<_>>();
    if req.filter_json.trim().is_empty() {
        let fresh = svc.fresh_vectors.search(
            &tenant_id,
            &source_name,
            &model.model_id,
            req.vector_name.trim(),
            &req.query_vector,
            &model.distance_metric,
            candidate_limit,
        );
        if !fresh.is_empty() {
            svc.metrics.inc_embedding_work("fresh_merged");
            points.extend(fresh.into_iter().filter(|point| point.score >= score_floor));
            points.sort_by(|left, right| right.score.total_cmp(&left.score));
            let mut seen = BTreeSet::new();
            points.retain(|point| seen.insert(point.id.clone()));
            points.truncate(candidate_limit);
        }
    }
    if let Some(config) = mmr {
        points = mmr_select(
            points,
            &req.query_vector,
            candidate_limit,
            config.lambda as f32,
        );
    }
    let rerank = req.rerank.clone().unwrap_or_default();
    let (mut points, rerank_scores, rerank_applied) =
        rerank_points(&req.query_text, &rerank, points).await?;
    points.truncate(top_k);

    let mut parent_context = BTreeMap::<String, String>::new();
    if req.parent_window > 0 {
        let mut parents = BTreeMap::<String, BTreeSet<i32>>::new();
        for point in &points {
            let parent = payload_string(point, "_parent_pk");
            if !parent.is_empty() {
                parents
                    .entry(parent)
                    .or_default()
                    .insert(payload_i32(point, "_chunk_seq"));
            }
        }
        for (parent, selected_sequences) in parents {
            let parent_filter =
                merge_retrieve_scope_filter(&tenant_id, &source_name, "", Some(&parent))?;
            let neighbors = store
                .search(&VectorSearchRequest {
                    context: None,
                    collection: model.collection().to_string(),
                    vector: req.query_vector.clone(),
                    filter: crate::runtime::executor_utils::json_to_struct(&parent_filter),
                    limit: embedding_max_chunks_per_row() as i32,
                    score_threshold: 0.0,
                    with_payload: true,
                    with_vector: false,
                    vector_name: req.vector_name.clone(),
                    quantization_rescore: model.rescore,
                })
                .await?
                .points;
            let mut ordered = neighbors
                .into_iter()
                .map(|point| {
                    (
                        payload_i32(&point, "_chunk_seq"),
                        payload_string(&point, "_chunk_text"),
                    )
                })
                .collect::<Vec<_>>();
            ordered.sort_by_key(|(seq, _)| *seq);
            let window = req.parent_window as i32;
            parent_context.insert(
                parent,
                ordered
                    .into_iter()
                    .filter(|(seq, _)| {
                        selected_sequences
                            .iter()
                            .any(|selected| (seq - selected).abs() <= window)
                    })
                    .map(|(_, text)| text)
                    .filter(|text| !text.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n\n"),
            );
        }
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let mut max_lag = 0i64;
    let hits = points
        .into_iter()
        .map(|point| {
            let parent_pk = payload_string(&point, "_parent_pk");
            let chunk_seq = payload_i32(&point, "_chunk_seq");
            let document_id = payload_string(&point, "_document_id");
            let doc_version = payload_string(&point, "_doc_version");
            let indexed_at = payload_json(&point)
                .get("_indexed_at_unix_ms")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default();
            if indexed_at > 0 {
                max_lag = max_lag.max(now_ms.saturating_sub(indexed_at));
            }
            embedding_pb::RetrieveHit {
                id: point.id.clone(),
                score: f64::from(point.score),
                payload_json: public_payload(
                    &point,
                    parent_context.get(&parent_pk).map(String::as_str),
                ),
                vector: if req.include_vectors {
                    point.vector
                } else {
                    Vec::new()
                },
                source_name: source_name.clone(),
                parent_pk,
                chunk_seq,
                document_id,
                doc_version,
                vector_name: point.vector_name,
                rerank_score: rerank_scores.get(&point.id).copied().unwrap_or_default(),
            }
        })
        .collect::<Vec<_>>();
    if max_lag > 0 {
        svc.metrics
            .observe_embedding_index_lag(max_lag as f64 / 1000.0);
    }
    let sample_rate = retrieval_eval_sample_rate();
    let mut evaluation_id = String::new();
    if sample_rate > 0.0 {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(
            format!("{tenant_id}\u{1f}{source_name}\u{1f}{}", req.query_text).as_bytes(),
        );
        let bucket = u16::from_be_bytes([digest[0], digest[1]]) as f64 / u16::MAX as f64;
        if bucket < sample_rate {
            evaluation_id = Uuid::new_v4().to_string();
            svc.emit_source_event(
                TOPIC_RETRIEVAL_SAMPLED, &tenant_id, &context.project_id, &source_name,
                serde_json::json!({
                    "model_id": model.model_id,
                    "evaluation_id": evaluation_id,
                    "query_hash": digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                    "hit_ids": hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
                    "scores": hits.iter().map(|hit| hit.score).collect::<Vec<_>>(),
                    "latency_ms": started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
                    "rerank_applied": rerank_applied,
                    "fusion": req.fusion,
                }),
            ).await;
        }
    }
    Ok(Response::new(embedding_pb::RetrieveResponse {
        hits,
        message: "ok".to_string(),
        error: None,
        index_lag_ms: max_lag,
        rerank_applied,
        evaluation_id,
    }))
}

pub(crate) async fn report_retrieval_evaluation(
    svc: &EmbeddingServiceImpl,
    request: Request<embedding_pb::ReportRetrievalEvaluationRequest>,
) -> Result<Response<embedding_pb::ReportRetrievalEvaluationResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    if Uuid::parse_str(req.evaluation_id.trim()).is_err() {
        return Err(embedding_field_violation(
            "evaluation_id",
            "must be the sampled retrieval evaluation UUID",
            "invalid retrieval evaluation id",
        ));
    }
    for (field, value) in [
        ("context_relevance", req.context_relevance),
        ("groundedness", req.groundedness),
        ("answer_relevance", req.answer_relevance),
    ] {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(embedding_field_violation(
                field,
                "must be a finite score between 0 and 1",
                "invalid retrieval evaluation score",
            ));
        }
    }
    let tenant_id = req.tenant_id.trim().to_string();
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "embedding",
        OperationChannel::Vector,
        &tenant_id,
        None,
    )
    .await?;
    let context = native_service_context(&metadata, &tenant_id, "");
    let evaluation_failed = !req.error.trim().is_empty();
    svc.emit_source_event(
        TOPIC_RETRIEVAL_EVALUATED,
        &tenant_id,
        &context.project_id,
        req.evaluation_id.trim(),
        serde_json::json!({
            "evaluation_id": req.evaluation_id,
            "context_relevance": req.context_relevance,
            "groundedness": req.groundedness,
            "answer_relevance": req.answer_relevance,
            "evaluator_model": req.evaluator_model,
            "error": req.error,
        }),
    )
    .await;
    svc.metrics.inc_embedding_work(if evaluation_failed {
        "evaluation_failed"
    } else {
        "evaluation_reported"
    });
    Ok(Response::new(
        embedding_pb::ReportRetrievalEvaluationResponse {
            accepted: true,
            message: "retrieval evaluation recorded".to_string(),
            error: None,
        },
    ))
}
