//! service.rs split — cache / document / graph / time-series / analytical RPC
//! handlers.
//!
//! These typed store RPCs are thin adapters: they authorize, translate the
//! typed request into the backend's native operation spec, and run it through
//! the SINGLE shared dispatch core `DataBrokerService::execute_backend_operation`
//! (the same path `GenericDispatch` uses) — there is no second dispatch
//! implementation here. Response mapping follows each executor's actual result
//! JSON:
//!   - redis      query "get" → {"key","hit","value"}; mutate → {"affected_rows"}
//!   - mongo      query        → bare array of docs;    mutate → {"affected_rows"|"inserted_id"}
//!   - neo4j      query        → bare array of records; mutate → {"affected_rows"}
//!   - clickhouse query        → bare array of rows;    mutate → {"affected_rows"}

use super::*;
use crate::runtime::executor_utils::{json_into_struct, struct_to_json};

impl DataBrokerService {
    /// Adapter over the shared dispatch core: pull the backend out of the typed
    /// `StoreResource` and delegate to `execute_backend_operation`. `method` is
    /// the executor method to invoke (`"query"` / `"mutate"` / `"search"`); the
    /// backend sub-operation (e.g. redis `"get"`, mongo `"upsert"`) lives inside
    /// `spec`.
    async fn run_store_op(
        &self,
        security: &SecurityContext,
        resource: Option<&crate::proto::StoreResource>,
        write: bool,
        method: &str,
        spec: serde_json::Value,
    ) -> Result<String, Status> {
        let backend = resource
            .map(|r| r.backend.clone())
            .filter(|b| !b.trim().is_empty())
            .ok_or_else(|| Status::invalid_argument("resource.backend is required"))?;
        let resource_name = resource
            .map(|r| r.resource_name.clone())
            .unwrap_or_default();
        self.execute_backend_operation(
            &security.request_context(),
            &backend,
            write,
            method.to_string(),
            resource_name,
            spec.to_string(),
        )
        .await
    }

    // ── Cache (redis dialect) ─────────────────────────────────────────────────

    pub(crate) async fn cache_get_inner(
        &self,
        request: Request<crate::proto::CacheGetRequest>,
    ) -> Result<Response<crate::proto::CacheGetResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CacheGet", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "cache.get")
            .await
        {
            return self.record_grpc("CacheGet", started, Err(e));
        }
        let spec = serde_json::json!({ "operation": "get", "key": req.key });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| {
                let v = parse_json(&json);
                let found = v
                    .get("hit")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or_else(|| v.get("value").map(|x| !x.is_null()).unwrap_or(false));
                let value = v
                    .get("value")
                    .and_then(serde_json::Value::as_str)
                    .map(|s| s.as_bytes().to_vec())
                    .unwrap_or_default();
                Response::new(crate::proto::CacheGetResponse {
                    found,
                    value,
                    ..Default::default()
                })
            });
        self.record_grpc("CacheGet", started, out)
    }

    pub(crate) async fn cache_set_inner(
        &self,
        request: Request<crate::proto::CacheSetRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CacheSet", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "cache.set")
            .await
        {
            return self.record_grpc("CacheSet", started, Err(e));
        }
        let mut spec = serde_json::json!({
            "operation": "set",
            "key": req.key,
            "value": String::from_utf8_lossy(&req.value),
        });
        if req.ttl_seconds > 0 {
            spec["ttl"] = serde_json::json!(req.ttl_seconds);
        }
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("CacheSet", started, out)
    }

    pub(crate) async fn cache_delete_inner(
        &self,
        request: Request<crate::proto::CacheDeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CacheDelete", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "cache.delete")
            .await
        {
            return self.record_grpc("CacheDelete", started, Err(e));
        }
        let spec = serde_json::json!({ "operation": "delete", "key": req.key });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("CacheDelete", started, out)
    }

    pub(crate) async fn cache_scan_inner(
        &self,
        request: Request<crate::proto::CacheScanRequest>,
    ) -> Result<Response<crate::proto::CacheScanResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("CacheScan", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "cache.scan")
            .await
        {
            return self.record_grpc("CacheScan", started, Err(e));
        }
        let spec = serde_json::json!({
            "operation": "scan",
            "pattern": req.key_pattern,
            "limit": req.limit,
            "cursor": req.page_token,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| {
                let v = parse_json(&json);
                let entries = v
                    .get("entries")
                    .and_then(serde_json::Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .map(|e| crate::proto::CacheEntry {
                                key: e
                                    .get("key")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or_default()
                                    .to_string(),
                                value: e
                                    .get("value")
                                    .and_then(serde_json::Value::as_str)
                                    .map(|s| s.as_bytes().to_vec())
                                    .unwrap_or_default(),
                                ..Default::default()
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let next = v
                    .get("next_page_token")
                    .or_else(|| v.get("cursor"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                Response::new(crate::proto::CacheScanResponse {
                    entries,
                    next_page_token: next,
                    ..Default::default()
                })
            });
        self.record_grpc("CacheScan", started, out)
    }

    // ── Document (mongo dialect) ──────────────────────────────────────────────

    pub(crate) async fn document_get_inner(
        &self,
        request: Request<crate::proto::DocumentGetRequest>,
    ) -> Result<Response<crate::proto::DocumentSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("DocumentGet", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "document.get")
            .await
        {
            return self.record_grpc("DocumentGet", started, Err(e));
        }
        let spec = serde_json::json!({
            "collection": collection_of(&req.resource),
            "filter": { "_id": req.document_id },
            "limit": 1,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| Response::new(document_set_from_json(&json)));
        self.record_grpc("DocumentGet", started, out)
    }

    pub(crate) async fn document_find_inner(
        &self,
        request: Request<crate::proto::DocumentFindRequest>,
    ) -> Result<Response<crate::proto::DocumentSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("DocumentFind", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "document.find")
            .await
        {
            return self.record_grpc("DocumentFind", started, Err(e));
        }
        let spec = serde_json::json!({
            "collection": collection_of(&req.resource),
            "filter": struct_field(&req.filter),
            "limit": req.limit,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| Response::new(document_set_from_json(&json)));
        self.record_grpc("DocumentFind", started, out)
    }

    pub(crate) async fn document_upsert_inner(
        &self,
        request: Request<crate::proto::DocumentUpsertRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("DocumentUpsert", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "document.upsert")
            .await
        {
            return self.record_grpc("DocumentUpsert", started, Err(e));
        }
        let spec = serde_json::json!({
            "collection": collection_of(&req.resource),
            "operation": if req.replace { "update" } else { "upsert" },
            "filter": { "_id": req.document_id },
            "update": struct_field(&req.document),
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("DocumentUpsert", started, out)
    }

    pub(crate) async fn document_delete_inner(
        &self,
        request: Request<crate::proto::DocumentDeleteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("DocumentDelete", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "document.delete")
            .await
        {
            return self.record_grpc("DocumentDelete", started, Err(e));
        }
        let filter = if req.document_id.is_empty() {
            struct_field(&req.filter)
        } else {
            serde_json::json!({ "_id": req.document_id })
        };
        let spec = serde_json::json!({
            "collection": collection_of(&req.resource),
            "operation": "delete",
            "filter": filter,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("DocumentDelete", started, out)
    }

    // ── Graph (neo4j dialect) ─────────────────────────────────────────────────

    pub(crate) async fn graph_query_inner(
        &self,
        request: Request<crate::proto::GraphQueryRequest>,
    ) -> Result<Response<crate::proto::GraphResultSet>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GraphQuery", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "graph.query")
            .await
        {
            return self.record_grpc("GraphQuery", started, Err(e));
        }
        let spec = serde_json::json!({
            "cypher": req.query,
            "parameters": struct_field(&req.parameters),
            "limit": req.limit,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| {
                Response::new(crate::proto::GraphResultSet {
                    records: structs_from_result(&json),
                    ..Default::default()
                })
            });
        self.record_grpc("GraphQuery", started, out)
    }

    pub(crate) async fn graph_mutate_inner(
        &self,
        request: Request<crate::proto::GraphMutationRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("GraphMutate", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "graph.mutate")
            .await
        {
            return self.record_grpc("GraphMutate", started, Err(e));
        }
        let spec = serde_json::json!({
            "operation": "cypher",
            "cypher": req.query,
            "parameters": struct_field(&req.parameters),
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("GraphMutate", started, out)
    }

    // ── Time-series / analytical (clickhouse dialect) ─────────────────────────

    pub(crate) async fn time_series_write_inner(
        &self,
        request: Request<crate::proto::TimeSeriesWriteRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "timeseries.write")
            .await
        {
            return self.record_grpc("TimeSeriesWrite", started, Err(e));
        }
        let rows: Vec<serde_json::Value> = req
            .points
            .iter()
            .map(|p| {
                let mut row = serde_json::Map::new();
                for (k, v) in &p.tags {
                    row.insert(k.clone(), serde_json::json!(v));
                }
                for (k, v) in &p.values {
                    row.insert(k.clone(), serde_json::json!(v));
                }
                if let serde_json::Value::Object(map) = struct_field(&p.fields) {
                    row.extend(map);
                }
                serde_json::Value::Object(row)
            })
            .collect();
        let spec = serde_json::json!({
            "table": collection_of(&req.resource),
            "rows": rows,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("TimeSeriesWrite", started, out)
    }

    pub(crate) async fn time_series_query_inner(
        &self,
        request: Request<crate::proto::TimeSeriesQueryRequest>,
    ) -> Result<Response<crate::proto::TimeSeriesQueryResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("TimeSeriesQuery", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "timeseries.query")
            .await
        {
            return self.record_grpc("TimeSeriesQuery", started, Err(e));
        }
        let spec = serde_json::json!({
            "table": collection_of(&req.resource),
            "filter": struct_field(&req.filter),
            "limit": req.limit,
        });
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| {
                // clickhouse returns a bare array of row objects; expose each as
                // a point's structured `fields`.
                let points = structs_from_result(&json)
                    .into_iter()
                    .map(|fields| crate::proto::TimeSeriesPoint {
                        fields: Some(fields),
                        ..Default::default()
                    })
                    .collect();
                Response::new(crate::proto::TimeSeriesQueryResponse {
                    points,
                    ..Default::default()
                })
            });
        self.record_grpc("TimeSeriesQuery", started, out)
    }

    pub(crate) async fn analytical_query_inner(
        &self,
        request: Request<crate::proto::AnalyticalQueryRequest>,
    ) -> Result<Response<crate::proto::AnalyticalQueryResponse>, Status> {
        let started = Instant::now();
        let security = match security_from_request(&request) {
            Ok(s) => s,
            Err(e) => return self.record_grpc("AnalyticalQuery", started, Err(e)),
        };
        let req = request.into_inner();
        if let Err(e) = self
            .authorize(&security, msg_type(&req.resource), "analytical.query")
            .await
        {
            return self.record_grpc("AnalyticalQuery", started, Err(e));
        }
        let spec = if req.query.trim().is_empty() {
            serde_json::json!({ "table": collection_of(&req.resource), "limit": req.limit })
        } else {
            serde_json::json!({ "sql": req.query })
        };
        let out = self
            .run_store_op(&security, req.resource.as_ref(), false, "query", spec)
            .await
            .map(|json| {
                let rows = structs_from_result(&json)
                    .into_iter()
                    .map(|s| crate::proto::Row {
                        fields: s.fields.into_iter().collect(),
                    })
                    .collect();
                Response::new(crate::proto::AnalyticalQueryResponse {
                    rows,
                    ..Default::default()
                })
            });
        self.record_grpc("AnalyticalQuery", started, out)
    }
}

// ── Shared mapping helpers ──────────────────────────────────────────────────

fn parse_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
}

fn msg_type(resource: &Option<crate::proto::StoreResource>) -> &str {
    resource
        .as_ref()
        .map(|r| r.message_type.as_str())
        .unwrap_or("")
}

fn collection_of(resource: &Option<crate::proto::StoreResource>) -> String {
    resource
        .as_ref()
        .map(|r| {
            if r.resource_name.is_empty() {
                r.message_type.clone()
            } else {
                r.resource_name.clone()
            }
        })
        .unwrap_or_default()
}

/// JSON for an optional protobuf `Struct` field (empty object when absent).
fn struct_field(value: &Option<prost_types::Struct>) -> serde_json::Value {
    value
        .as_ref()
        .map(struct_to_json)
        .unwrap_or_else(|| serde_json::json!({}))
}

/// Build a `MutationResponse` from an executor mutate result, mapping
/// `affected_rows` (or treating an `inserted_id` as one affected row).
fn mutation_from_json(json: &str) -> MutationResponse {
    let v = parse_json(json);
    let affected_rows = v
        .get("affected_rows")
        .and_then(serde_json::Value::as_i64)
        .or_else(|| v.get("inserted_id").map(|_| 1))
        .unwrap_or(0);
    MutationResponse {
        affected_rows,
        ..Default::default()
    }
}

/// A `DocumentSet` from a query result (mongo returns a bare array of docs).
fn document_set_from_json(json: &str) -> crate::proto::DocumentSet {
    crate::proto::DocumentSet {
        documents: structs_from_result(json),
        ..Default::default()
    }
}

/// Parse an executor query result into a list of protobuf `Struct`s. The SQL /
/// document / graph executors return a bare JSON array of row objects; this also
/// tolerates `{"rows":[...]}` / `{"records":[...]}` / `{"documents":[...]}`.
fn structs_from_result(json: &str) -> Vec<prost_types::Struct> {
    let mut v = parse_json(json);
    // The parsed result is discarded after this, so MOVE each row object into its
    // proto Struct (`json_into_struct`) instead of borrow-and-clone-every-field
    // (`json_to_struct`). Take the row array out by value; for the wrapper-object
    // forms, `contains_key` (immutable) picks the slot before the `get_mut` move.
    let array = match &mut v {
        serde_json::Value::Array(items) => Some(std::mem::take(items)),
        serde_json::Value::Object(map) => {
            let slot = if map.contains_key("rows") {
                map.get_mut("rows")
            } else if map.contains_key("records") {
                map.get_mut("records")
            } else {
                map.get_mut("documents")
            };
            slot.and_then(serde_json::Value::as_array_mut)
                .map(std::mem::take)
        }
        _ => None,
    };
    array
        .map(|items| items.into_iter().filter_map(json_into_struct).collect())
        .unwrap_or_default()
}
