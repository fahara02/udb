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
//!
//! ## What is authorized is what is executed
//!
//! Every handler resolves its [`StoreTarget`] BEFORE authorizing: when the
//! request names a manifest entity (`resource.message_type`), the collection it
//! executes on must be that entity's own store resource, so a caller cannot
//! authorize as entity X and execute on collection Y. A resource that is not a
//! manifest entity is authorized on its `resource_name` — the collection
//! actually executed.
//!
//! ## Tenant scoping
//!
//! When the target is a manifest entity and the request shape maps to the
//! neutral IR (document get/find/upsert/delete, time-series write/query,
//! analytical table scans), the handler sends an `ir` envelope: the dispatch
//! core compiles it through the backend's IR compiler with the caller's
//! verified tenant/project, so the executed statement is tenant-scoped (and
//! fails closed on an empty tenant). Free-text shapes (raw Cypher / SQL, or a
//! filter the IR cannot express) stay on the raw path, which the dispatch core
//! gates: refused in production unless the per-backend opt-out is set, and
//! counted + warned in development.

use super::cache_service::scope::{
    CacheAccess, scope_for_caller, shared_cache_write_denied_status,
};
use super::*;
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue, LogicalWrite,
    NullOrder, SortDirection,
};
use crate::runtime::executor_utils::{invalid_argument_fields, json_into_struct, struct_to_json};

fn store_rpc_invalid_fields<I, F, D>(message: impl Into<String>, fields: I) -> Status
where
    I: IntoIterator<Item = (F, D)>,
    F: Into<String>,
    D: Into<String>,
{
    invalid_argument_fields(message, fields)
}

fn require_resource_backend(
    resource: Option<&crate::proto::StoreResource>,
) -> Result<String, Status> {
    resource
        .map(|r| r.backend.clone())
        .filter(|b| !b.trim().is_empty())
        .ok_or_else(|| {
            store_rpc_invalid_fields(
                "resource.backend is required",
                [("resource.backend", "must be a non-empty backend name")],
            )
        })
}

/// What a typed store RPC executes against, resolved before authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StoreTarget {
    /// The object handed to `authorize` — the entity or collection executed.
    authz_object: String,
    /// The manifest entity (message type) the IR compiles against. `None`
    /// means the resource is not a manifest entity, so only the gated raw
    /// path is available.
    entity: Option<String>,
}

/// How a store RPC uses `resource.resource_name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceUse {
    /// The RPC executes on the named collection/table (document, time-series,
    /// analytical table scan): the entity and the collection must agree.
    Collection,
    /// The RPC executes key ops or free-text statements that do not target
    /// `resource_name` (cache keys, raw Cypher, raw SQL).
    Statement,
}

fn has_wildcard(value: &str) -> bool {
    value.contains('*')
}

/// Resolve the executed target of a store RPC against the active manifest.
fn resolve_store_target(
    manifest: &CatalogManifest,
    resource: &Option<crate::proto::StoreResource>,
    usage: ResourceUse,
) -> Result<StoreTarget, Status> {
    let message_type = resource
        .as_ref()
        .map(|r| r.message_type.trim().to_string())
        .unwrap_or_default();
    let resource_name = resource
        .as_ref()
        .map(|r| r.resource_name.trim().to_string())
        .unwrap_or_default();
    if has_wildcard(&message_type) {
        return Err(store_rpc_invalid_fields(
            format!(
                "resource.message_type '{message_type}' is a wildcard; store RPCs must name a concrete entity"
            ),
            [(
                "resource.message_type",
                "must name a concrete entity; wildcards are not accepted on store RPCs",
            )],
        ));
    }
    if has_wildcard(&resource_name) {
        return Err(store_rpc_invalid_fields(
            format!("resource.resource_name '{resource_name}' is a wildcard"),
            [(
                "resource.resource_name",
                "must name a concrete collection; wildcards are not accepted on store RPCs",
            )],
        ));
    }

    if !message_type.is_empty() {
        match crate::broker::table_lookup(manifest, &message_type) {
            crate::broker::TableLookup::Found(table) => {
                if usage == ResourceUse::Collection
                    && !resource_name.is_empty()
                    && resource_name != table.table
                {
                    return Err(store_rpc_invalid_fields(
                        format!(
                            "resource.resource_name '{resource_name}' is not the store resource of \
                             entity '{message_type}' (expected '{}'); a request is authorized on the \
                             entity it names, so it may only execute on that entity's own collection",
                            table.table
                        ),
                        [(
                            "resource.resource_name",
                            "must be empty or equal to the store resource of resource.message_type",
                        )],
                    ));
                }
                Ok(StoreTarget {
                    authz_object: message_type,
                    entity: Some(table.message_fqn()),
                })
            }
            crate::broker::TableLookup::Ambiguous { .. } => Err(store_rpc_invalid_fields(
                crate::broker::describe_table_lookup_miss(manifest, &message_type),
                [(
                    "resource.message_type",
                    "must identify exactly one catalog entity; qualify it with the full protobuf name",
                )],
            )),
            crate::broker::TableLookup::Missing => {
                // Not a manifest entity: it cannot vouch for any other
                // collection, so authorize on the collection actually executed.
                let authz_object = if usage == ResourceUse::Collection && !resource_name.is_empty()
                {
                    resource_name
                } else {
                    message_type
                };
                Ok(StoreTarget {
                    authz_object,
                    entity: None,
                })
            }
        }
    } else {
        // No entity named: authorize on the collection itself, and use the IR
        // when that collection is exactly one manifest entity's table.
        let entity = match (usage, crate::broker::table_lookup(manifest, &resource_name)) {
            (ResourceUse::Collection, crate::broker::TableLookup::Found(table))
                if !resource_name.is_empty() && table.table == resource_name =>
            {
                Some(table.message_fqn())
            }
            _ => None,
        };
        Ok(StoreTarget {
            authz_object: resource_name,
            entity,
        })
    }
}

/// `{"ir": {"op": <op>, ...payload}}` — the neutral-IR envelope the dispatch
/// core compiles with the caller's verified tenant/project.
fn ir_envelope<T: serde::Serialize>(op: &str, payload: &T) -> Result<serde_json::Value, Status> {
    let mut ir = serde_json::to_value(payload).map_err(|err| {
        crate::runtime::executor_utils::internal_status(
            "udb",
            "store_rpc_ir_envelope",
            format!("store RPC IR envelope encode failed: {err}"),
        )
    })?;
    if let serde_json::Value::Object(map) = &mut ir {
        map.insert("op".to_string(), serde_json::json!(op));
    }
    Ok(serde_json::json!({ "ir": ir }))
}

fn logical_value_from_json(value: &serde_json::Value) -> LogicalValue {
    match value {
        serde_json::Value::Null => LogicalValue::Null,
        serde_json::Value::Bool(v) => LogicalValue::Bool(*v),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(LogicalValue::Int)
            .or_else(|| n.as_f64().map(LogicalValue::Float))
            .unwrap_or_else(|| LogicalValue::Json(value.clone())),
        serde_json::Value::String(v) => LogicalValue::String(v.clone()),
        serde_json::Value::Array(values) => {
            LogicalValue::Array(values.iter().map(logical_value_from_json).collect())
        }
        serde_json::Value::Object(_) => LogicalValue::Json(value.clone()),
    }
}

/// A scalar JSON value usable as a comparison operand (objects/arrays are not
/// — their equality semantics differ per backend).
fn scalar_operand(value: &serde_json::Value) -> Option<LogicalValue> {
    match value {
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        other => Some(logical_value_from_json(other)),
    }
}

/// Map a Mongo-style filter document onto the neutral IR filter.
///
/// Supported: `{field: scalar}` (equality), `{field: null}` (is null),
/// `{field: {$eq|$ne|$gt|$gte|$lt|$lte: scalar, $in: [scalars]}}`, and
/// `$and` / `$or` over such documents. `Ok(None)` is "no filter";
/// `Err(())` means the document uses something the IR cannot express, and the
/// caller must take the gated raw path instead.
#[allow(clippy::result_unit_err)]
fn filter_from_document(doc: &serde_json::Value) -> Result<Option<LogicalFilter>, ()> {
    let serde_json::Value::Object(map) = doc else {
        return if doc.is_null() { Ok(None) } else { Err(()) };
    };
    let mut clauses: Vec<LogicalFilter> = Vec::new();
    for (key, value) in map {
        match key.as_str() {
            "$and" | "$or" => {
                let serde_json::Value::Array(items) = value else {
                    return Err(());
                };
                let mut branches = Vec::with_capacity(items.len());
                for item in items {
                    match filter_from_document(item)? {
                        Some(branch) => branches.push(branch),
                        // An empty sub-document is TRUE; in an `$or` that
                        // makes the whole disjunction TRUE — too subtle to
                        // map faithfully, so take the raw path.
                        None if key == "$or" => return Err(()),
                        None => {}
                    }
                }
                if branches.is_empty() {
                    return Err(());
                }
                clauses.push(if key == "$and" {
                    LogicalFilter::And(branches)
                } else {
                    LogicalFilter::Or(branches)
                });
            }
            other if other.starts_with('$') => return Err(()),
            field => match value {
                serde_json::Value::Null => clauses.push(LogicalFilter::IsNull(field.to_string())),
                serde_json::Value::Object(ops) => {
                    if ops.is_empty() || !ops.keys().all(|k| k.starts_with('$')) {
                        return Err(());
                    }
                    for (op, operand) in ops {
                        let comparison = |op: ComparisonOp| -> Result<LogicalFilter, ()> {
                            Ok(LogicalFilter::Comparison {
                                field: field.to_string(),
                                op,
                                value: scalar_operand(operand).filter(|v| !v.is_null()).ok_or(())?,
                            })
                        };
                        clauses.push(match op.as_str() {
                            "$eq" => comparison(ComparisonOp::Eq)?,
                            "$ne" => comparison(ComparisonOp::Ne)?,
                            "$gt" => comparison(ComparisonOp::Gt)?,
                            "$gte" => comparison(ComparisonOp::Ge)?,
                            "$lt" => comparison(ComparisonOp::Lt)?,
                            "$lte" => comparison(ComparisonOp::Le)?,
                            "$in" => {
                                let serde_json::Value::Array(items) = operand else {
                                    return Err(());
                                };
                                let values = items
                                    .iter()
                                    .map(|item| scalar_operand(item).ok_or(()))
                                    .collect::<Result<Vec<_>, _>>()?;
                                LogicalFilter::InList {
                                    field: field.to_string(),
                                    values,
                                }
                            }
                            _ => return Err(()),
                        });
                    }
                }
                serde_json::Value::Array(_) => return Err(()),
                scalar => clauses.push(LogicalFilter::Comparison {
                    field: field.to_string(),
                    op: ComparisonOp::Eq,
                    value: logical_value_from_json(scalar),
                }),
            },
        }
    }
    Ok(match clauses.len() {
        0 => None,
        1 => clauses.pop(),
        _ => Some(LogicalFilter::And(clauses)),
    })
}

/// Positive `limit` as IR pagination.
fn limit_pagination(limit: i64) -> Option<LogicalPagination> {
    u32::try_from(limit)
        .ok()
        .filter(|limit| *limit > 0)
        .map(|limit| LogicalPagination {
            limit: Some(limit),
            ..Default::default()
        })
}

/// The first primary-key field of a manifest entity (`None` when it has none).
fn entity_primary_key(manifest: &CatalogManifest, entity: &str) -> Option<String> {
    match crate::broker::table_lookup(manifest, entity) {
        crate::broker::TableLookup::Found(table) => table.primary_key.first().cloned(),
        _ => None,
    }
}

fn pk_filter(pk: &str, id: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: pk.to_string(),
        op: ComparisonOp::Eq,
        value: LogicalValue::String(id.to_string()),
    }
}

fn record_from_json(value: &serde_json::Value) -> LogicalRecord {
    match value {
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(k, v)| (k.clone(), logical_value_from_json(v)))
            .collect(),
        _ => LogicalRecord::new(),
    }
}

impl DataBrokerService {
    /// Adapter over the shared dispatch core: pull the backend out of the typed
    /// `StoreResource` and delegate to `execute_backend_operation`. `method` is
    /// the executor method to invoke (`"query"` / `"mutate"` / `"search"`); the
    /// backend sub-operation (e.g. redis `"get"`, mongo `"upsert"`) lives inside
    /// `spec`. An `ir` envelope in `spec` is compiled (tenant-scoped) by the
    /// dispatch core, which then picks the executor method itself.
    async fn run_store_op(
        &self,
        security: &SecurityContext,
        resource: Option<&crate::proto::StoreResource>,
        write: bool,
        method: &str,
        spec: serde_json::Value,
    ) -> Result<String, Status> {
        self.run_store_op_keyed(security, None, resource, write, method, spec)
            .await
    }

    /// [`Self::run_store_op`] with the key tenant set apart from the caller
    /// (the shared cache namespace); see
    /// [`DataBrokerService::execute_backend_operation_keyed`].
    async fn run_store_op_keyed(
        &self,
        security: &SecurityContext,
        key_tenant: Option<&str>,
        resource: Option<&crate::proto::StoreResource>,
        write: bool,
        method: &str,
        spec: serde_json::Value,
    ) -> Result<String, Status> {
        let backend = require_resource_backend(resource)?;
        let resource_name = resource
            .map(|r| r.resource_name.clone())
            .unwrap_or_default();
        self.execute_backend_operation_keyed(
            &security.request_context(),
            key_tenant,
            &backend,
            write,
            method.to_string(),
            resource_name,
            spec.to_string(),
        )
        .await
    }

    /// The key tenant of a typed cache RPC: `None` keys under the caller's own
    /// `udb:{project}:{tenant}:` namespace; a `shared` request keys under the
    /// reserved shared-tenant marker (`udb:{project}:*shared*:`). Any caller
    /// may read the shared namespace; writing it takes the shared-table write
    /// authority (`udb:platform_admin` or `udb:global:write`), else
    /// PERMISSION_DENIED `shared_cache_write_denied`. A caller whose tenant is
    /// the marker itself is refused either way. Only the Redis and Memcached
    /// cache backends key by tenant, so `shared` is refused on any other.
    fn cache_key_tenant(
        &self,
        security: &SecurityContext,
        resource: &Option<crate::proto::StoreResource>,
        shared: bool,
        access: CacheAccess,
        operation: &'static str,
    ) -> Result<Option<String>, Status> {
        let key_tenant =
            shared_cache_key_tenant(&security.request_context(), shared, access, operation)?;
        if key_tenant.is_none() {
            return Ok(None);
        }
        let backend = self.store_backend_kind(security, resource);
        shared_cache_backend_supported(backend)?;
        Ok(key_tenant)
    }

    /// Resolve the executed target (see [`resolve_store_target`]) against the
    /// caller's active catalog and authorize the caller on it.
    async fn authorize_store_target(
        &self,
        security: &SecurityContext,
        resource: &Option<crate::proto::StoreResource>,
        usage: ResourceUse,
        operation: &str,
    ) -> Result<(StoreTarget, Arc<crate::runtime::catalog::CatalogState>), Status> {
        let catalog = self
            .catalog
            .active_for(&security.request_context().project_id);
        if usage == ResourceUse::Collection {
            require_collection(resource)?;
        }
        let target = resolve_store_target(&catalog.manifest, resource, usage)?;
        if target.authz_object.is_empty() {
            // Nothing names what is executed, so there is nothing to
            // authorize against — refuse before any policy evaluation.
            return Err(store_rpc_invalid_fields(
                "resource.message_type or resource.resource_name is required",
                [
                    (
                        "resource.message_type",
                        "must name the entity executed when resource.resource_name is empty",
                    ),
                    (
                        "resource.resource_name",
                        "must name the collection executed when resource.message_type is empty",
                    ),
                ],
            ));
        }
        self.authorize(security, &target.authz_object, operation)
            .await?;
        Ok((target, catalog))
    }

    /// Canonical backend token for a store resource (resolves selectors), so
    /// the handler can pick a per-backend IR shape.
    fn store_backend_kind(
        &self,
        security: &SecurityContext,
        resource: &Option<crate::proto::StoreResource>,
    ) -> Option<crate::backend::BackendKind> {
        let backend = require_resource_backend(resource.as_ref()).ok()?;
        let resolved = self
            .runtime_snapshot()
            .resolve_backend_selector_for_project(&backend, &security.request_context().project_id)
            .ok()?;
        crate::backend::BackendKind::from_token(&resolved.backend)
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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "cache.get",
            )
            .await
        {
            return self.record_grpc("CacheGet", started, Err(e));
        }
        if let Err(e) = cache_get_unsupported(&req) {
            return self.record_grpc("CacheGet", started, Err(e));
        }
        let key_tenant = match self.cache_key_tenant(
            &security,
            &req.resource,
            req.shared,
            CacheAccess::Read,
            "cache.get",
        ) {
            Ok(key_tenant) => key_tenant,
            Err(e) => return self.record_grpc("CacheGet", started, Err(e)),
        };
        let spec = serde_json::json!({ "operation": "get", "key": req.key });
        let out = self
            .run_store_op_keyed(
                &security,
                key_tenant.as_deref(),
                req.resource.as_ref(),
                false,
                "query",
                spec,
            )
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
                    .map(cache_value_bytes)
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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "cache.set",
            )
            .await
        {
            return self.record_grpc("CacheSet", started, Err(e));
        }
        if let Err(e) = cache_set_unsupported(&req) {
            return self.record_grpc("CacheSet", started, Err(e));
        }
        let key_tenant = match self.cache_key_tenant(
            &security,
            &req.resource,
            req.shared,
            CacheAccess::Write,
            "cache.set",
        ) {
            Ok(key_tenant) => key_tenant,
            Err(e) => return self.record_grpc("CacheSet", started, Err(e)),
        };
        // The cache stores text: a value that is not UTF-8 would come back
        // changed, so it is refused rather than silently altered.
        let value = match String::from_utf8(req.value.clone()) {
            Ok(value) => value,
            Err(_) => {
                return self.record_grpc(
                    "CacheSet",
                    started,
                    Err(crate::runtime::executor_utils::invalid_argument_fields(
                        "CacheSet stores text; the value is not valid UTF-8 (encode binary values, e.g. base64)",
                        [("value", "must be valid UTF-8")],
                    )),
                );
            }
        };
        let mut spec = serde_json::json!({
            "operation": "set",
            "key": req.key,
            "value": value,
        });
        if req.ttl_seconds > 0 {
            spec["ttl"] = serde_json::json!(req.ttl_seconds);
        }
        if req.only_if_absent {
            spec["nx"] = serde_json::json!(true);
        }
        if req.only_if_present {
            spec["xx"] = serde_json::json!(true);
        }
        let out = self
            .run_store_op_keyed(
                &security,
                key_tenant.as_deref(),
                req.resource.as_ref(),
                true,
                "mutate",
                spec,
            )
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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "cache.delete",
            )
            .await
        {
            return self.record_grpc("CacheDelete", started, Err(e));
        }
        if let Err(e) = refuse_unsupported_fields(
            "CacheDelete",
            &[(
                "idempotency_key",
                has_text(&req.idempotency_key),
                NO_STORE_IDEMPOTENCY,
            )],
        ) {
            return self.record_grpc("CacheDelete", started, Err(e));
        }
        let key_tenant = match self.cache_key_tenant(
            &security,
            &req.resource,
            req.shared,
            CacheAccess::Write,
            "cache.delete",
        ) {
            Ok(key_tenant) => key_tenant,
            Err(e) => return self.record_grpc("CacheDelete", started, Err(e)),
        };
        let spec = serde_json::json!({ "operation": "delete", "key": req.key });
        let out = self
            .run_store_op_keyed(
                &security,
                key_tenant.as_deref(),
                req.resource.as_ref(),
                true,
                "mutate",
                spec,
            )
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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "cache.scan",
            )
            .await
        {
            return self.record_grpc("CacheScan", started, Err(e));
        }
        let key_tenant = match self.cache_key_tenant(
            &security,
            &req.resource,
            req.shared,
            CacheAccess::Read,
            "cache.scan",
        ) {
            Ok(key_tenant) => key_tenant,
            Err(e) => return self.record_grpc("CacheScan", started, Err(e)),
        };
        let spec = serde_json::json!({
            "operation": "scan",
            "pattern": req.key_pattern,
            "limit": req.limit,
            "cursor": req.page_token,
        });
        let out = self
            .run_store_op_keyed(
                &security,
                key_tenant.as_deref(),
                req.resource.as_ref(),
                false,
                "query",
                spec,
            )
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
                                    .map(cache_value_bytes)
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
        let (target, catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "document.get",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("DocumentGet", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("DocumentGet", started, Err(e));
        }
        let ir = target.entity.as_deref().and_then(|entity| {
            let pk = entity_primary_key(&catalog.manifest, entity)?;
            let mut read = LogicalRead::message(entity)
                .with_filter(pk_filter(&pk, &req.document_id))
                .with_pagination(LogicalPagination {
                    limit: Some(1),
                    ..Default::default()
                });
            read.projection = field_projection(&req.fields);
            Some(ir_envelope("read", &read))
        });
        let spec = match ir {
            Some(Ok(spec)) => spec,
            Some(Err(e)) => return self.record_grpc("DocumentGet", started, Err(e)),
            None => {
                let mut spec = serde_json::json!({
                    "collection": collection_of(&req.resource),
                    "filter": { "_id": req.document_id },
                    "limit": 1,
                });
                if let Some(projection) = mongo_projection(&req.fields) {
                    spec["projection"] = projection;
                }
                spec
            }
        };
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
        let (target, _catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "document.find",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("DocumentFind", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("DocumentFind", started, Err(e));
        }
        if let Err(e) = refuse_unsupported_fields(
            "DocumentFind",
            &[(
                "page_token",
                has_text(&req.page_token),
                "document finds return one page and issue no continuation token",
            )],
        ) {
            return self.record_grpc("DocumentFind", started, Err(e));
        }
        let sort = match document_sort(&req.sort) {
            Ok(sort) => sort,
            Err(e) => return self.record_grpc("DocumentFind", started, Err(e)),
        };
        let filter_doc = struct_field(&req.filter);
        let ir = match (target.entity.as_deref(), filter_from_document(&filter_doc)) {
            (Some(entity), Ok(filter)) => {
                let mut read = LogicalRead::message(entity);
                read.filter = filter;
                read.projection = field_projection(&req.fields);
                read.sort = sort.clone();
                read.pagination = limit_pagination(i64::from(req.limit));
                Some(ir_envelope("read", &read))
            }
            _ => None,
        };
        let spec = match ir {
            Some(Ok(spec)) => spec,
            Some(Err(e)) => return self.record_grpc("DocumentFind", started, Err(e)),
            None => {
                // The raw document query takes a projection but no sort, so a
                // sorted find that cannot use the neutral IR is refused rather
                // than returned in an order the caller did not ask for.
                if !sort.is_empty() {
                    return self.record_grpc(
                        "DocumentFind",
                        started,
                        Err(unsupported_store_field(
                            "DocumentFind",
                            "sort",
                            "sorting needs a catalog entity and a filter the neutral IR can express",
                        )),
                    );
                }
                let mut spec = serde_json::json!({
                    "collection": collection_of(&req.resource),
                    "filter": filter_doc,
                    "limit": req.limit,
                });
                if let Some(projection) = mongo_projection(&req.fields) {
                    spec["projection"] = projection;
                }
                spec
            }
        };
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
        let (target, catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "document.upsert",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("DocumentUpsert", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("DocumentUpsert", started, Err(e));
        }
        if let Err(e) = refuse_unsupported_fields(
            "DocumentUpsert",
            &[(
                "idempotency_key",
                has_text(&req.idempotency_key),
                NO_STORE_IDEMPOTENCY,
            )],
        ) {
            return self.record_grpc("DocumentUpsert", started, Err(e));
        }
        let document = struct_field(&req.document);
        let ir = target.entity.as_deref().and_then(|entity| {
            let pk = entity_primary_key(&catalog.manifest, entity)?;
            let mut record = record_from_json(&document);
            if !req.document_id.is_empty() {
                record.insert(pk.clone(), LogicalValue::String(req.document_id.clone()));
            }
            if !record.contains_key(&pk) {
                return None;
            }
            let fields: Vec<String> = record.keys().filter(|k| **k != pk).cloned().collect();
            let conflict = match upsert_conflict(&pk, fields, &req.merge_fields, req.replace) {
                Ok(conflict) => conflict,
                Err(e) => return Some(Err(e)),
            };
            Some(ir_envelope(
                "write",
                &LogicalWrite {
                    message_type: entity.to_string(),
                    records: vec![record],
                    conflict,
                    return_fields: Vec::new(),
                },
            ))
        });
        let spec = match ir {
            Some(Ok(spec)) => spec,
            Some(Err(e)) => return self.record_grpc("DocumentUpsert", started, Err(e)),
            // The raw document write replaces or merges the whole document;
            // it cannot limit the update to `merge_fields`.
            None if !req.merge_fields.is_empty() => {
                return self.record_grpc(
                    "DocumentUpsert",
                    started,
                    Err(unsupported_store_field(
                        "DocumentUpsert",
                        "merge_fields",
                        "a partial merge needs a catalog entity and a document that carries its primary key",
                    )),
                );
            }
            None => serde_json::json!({
                "collection": collection_of(&req.resource),
                "operation": if req.replace { "update" } else { "upsert" },
                "filter": { "_id": req.document_id },
                "update": document,
            }),
        };
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
        let (target, catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "document.delete",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("DocumentDelete", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("DocumentDelete", started, Err(e));
        }
        let filter = if req.document_id.is_empty() {
            struct_field(&req.filter)
        } else {
            serde_json::json!({ "_id": req.document_id })
        };
        let ir_filter = target.entity.as_deref().and_then(|entity| {
            if req.document_id.is_empty() {
                filter_from_document(&filter).ok()
            } else {
                entity_primary_key(&catalog.manifest, entity)
                    .map(|pk| Some(pk_filter(&pk, &req.document_id)))
            }
        });
        let spec = match (target.entity.as_deref(), ir_filter) {
            // A delete must be bounded: the IR has no "delete everything" path.
            (Some(_), Some(None)) => {
                return self.record_grpc(
                    "DocumentDelete",
                    started,
                    Err(store_rpc_invalid_fields(
                        "a document delete needs document_id or a non-empty filter",
                        [("filter", "must be non-empty when document_id is empty")],
                    )),
                );
            }
            (Some(entity), Some(Some(filter))) => match ir_envelope(
                "delete",
                &LogicalDelete {
                    message_type: entity.to_string(),
                    filter,
                    return_fields: Vec::new(),
                },
            ) {
                Ok(spec) => spec,
                Err(e) => return self.record_grpc("DocumentDelete", started, Err(e)),
            },
            _ => serde_json::json!({
                "collection": collection_of(&req.resource),
                "operation": "delete",
                "filter": filter,
            }),
        };
        let out = self
            .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
            .await
            .map(|json| Response::new(mutation_from_json(&json)));
        self.record_grpc("DocumentDelete", started, out)
    }

    // ── Graph (neo4j dialect) ─────────────────────────────────────────────────
    //
    // Free-text Cypher cannot be lowered to the IR, so these stay on the raw
    // path behind the dispatch core's raw-dispatch gate. GraphQuery is
    // additionally read-only: the Neo4j executor's query path refuses write
    // clauses and runs the statement in a READ transaction. A typed
    // `GraphQuery.traversal` is the mediated alternative: the broker builds
    // the Cypher with the verified scope on every node and relationship, so it
    // is not subject to the raw-dispatch gate.

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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "graph.query",
            )
            .await
        {
            return self.record_grpc("GraphQuery", started, Err(e));
        }
        let spec = match req.traversal.as_ref() {
            // Typed traversal: the dispatch core stamps the verified scope and
            // the Neo4j executor builds the scoped Cypher (no raw gate).
            Some(traversal) => {
                if !req.query.trim().is_empty()
                    || req
                        .parameters
                        .as_ref()
                        .is_some_and(|params| !params.fields.is_empty())
                {
                    return self.record_grpc(
                        "GraphQuery",
                        started,
                        Err(store_rpc_invalid_fields(
                            "GraphQuery takes either a free-text query or a typed traversal, not both",
                            [(
                                "traversal",
                                "must not be combined with query or parameters",
                            )],
                        )),
                    );
                }
                if self.store_backend_kind(&security, &req.resource)
                    != Some(crate::backend::BackendKind::Neo4j)
                {
                    return self.record_grpc(
                        "GraphQuery",
                        started,
                        Err(store_rpc_invalid_fields(
                            "GraphQuery.traversal is served by a neo4j graph store only",
                            [(
                                "resource.backend",
                                "must resolve to a neo4j backend for a typed traversal",
                            )],
                        )),
                    );
                }
                match graph_traversal_json(traversal, req.limit) {
                    Ok(traversal) => serde_json::json!({ "traversal": traversal }),
                    Err(e) => return self.record_grpc("GraphQuery", started, Err(e)),
                }
            }
            None => serde_json::json!({
                "cypher": req.query,
                "parameters": struct_field(&req.parameters),
                "limit": req.limit,
            }),
        };
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
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Statement,
                "graph.mutate",
            )
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
        let (target, catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "timeseries.write",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("TimeSeriesWrite", started, Err(e));
        }
        if let Err(e) = refuse_unsupported_fields(
            "TimeSeriesWrite",
            &[(
                "idempotency_key",
                has_text(&req.idempotency_key),
                NO_STORE_IDEMPOTENCY,
            )],
        ) {
            return self.record_grpc("TimeSeriesWrite", started, Err(e));
        }
        // `TimeSeriesPoint.timestamp` is written into the entity's declared
        // time column (`timeseries_store.time_field`). Without one there is no
        // column to put it in, so a stamped point is refused, not dropped.
        let time_field = target
            .entity
            .as_deref()
            .and_then(|entity| timeseries_time_field(&catalog.manifest, entity));
        let stamped_times = match point_times(&req.points, time_field.as_deref()) {
            Ok(times) => times,
            Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
        };
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
        let Some(entity) = target.entity.as_deref().filter(|_| !rows.is_empty()) else {
            let spec = serde_json::json!({
                "table": collection_of(&req.resource),
                "rows": rows,
            });
            let out = self
                .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
                .await
                .map(|json| Response::new(mutation_from_json(&json)));
            return self.record_grpc("TimeSeriesWrite", started, out);
        };
        // ClickHouse ingests a multi-row INSERT in one statement; the other
        // IR backends (Cassandra, Neo4j) compile one record per statement, so
        // each point is its own scoped write and the counts are summed.
        let batch = matches!(
            self.store_backend_kind(&security, &req.resource),
            Some(crate::backend::BackendKind::Clickhouse)
        );
        let records: Vec<LogicalRecord> = match rows
            .iter()
            .zip(&stamped_times)
            .map(|(row, time)| {
                stamp_point_record(record_from_json(row), time_field.as_deref(), *time)
            })
            .collect::<Result<Vec<_>, Status>>()
        {
            Ok(records) => records,
            Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
        };
        let writes: Vec<Vec<LogicalRecord>> = if batch {
            vec![records]
        } else {
            records.into_iter().map(|record| vec![record]).collect()
        };
        let mut affected_rows: i64 = 0;
        for records in writes {
            let spec = match ir_envelope(
                "write",
                &LogicalWrite {
                    message_type: entity.to_string(),
                    records,
                    conflict: ConflictStrategy::Error,
                    return_fields: Vec::new(),
                },
            ) {
                Ok(spec) => spec,
                Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
            };
            match self
                .run_store_op(&security, req.resource.as_ref(), true, "mutate", spec)
                .await
            {
                Ok(json) => affected_rows += mutation_from_json(&json).affected_rows,
                Err(e) => return self.record_grpc("TimeSeriesWrite", started, Err(e)),
            }
        }
        self.record_grpc(
            "TimeSeriesWrite",
            started,
            Ok(Response::new(MutationResponse {
                affected_rows,
                ..Default::default()
            })),
        )
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
        let (target, catalog) = match self
            .authorize_store_target(
                &security,
                &req.resource,
                ResourceUse::Collection,
                "timeseries.query",
            )
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("TimeSeriesQuery", started, Err(e)),
        };
        if let Err(e) = require_collection(&req.resource) {
            return self.record_grpc("TimeSeriesQuery", started, Err(e));
        }
        if let Err(e) = time_series_query_unsupported(&req) {
            return self.record_grpc("TimeSeriesQuery", started, Err(e));
        }
        let time_field = target
            .entity
            .as_deref()
            .and_then(|entity| timeseries_time_field(&catalog.manifest, entity));
        let time_range = match time_range_filter(
            time_field.as_deref(),
            req.from.as_ref().map(|t| (t.seconds, t.nanos)),
            req.to.as_ref().map(|t| (t.seconds, t.nanos)),
        ) {
            Ok(range) => range,
            Err(e) => return self.record_grpc("TimeSeriesQuery", started, Err(e)),
        };
        let filter_doc = struct_field(&req.filter);
        let ir = match (target.entity.as_deref(), filter_from_document(&filter_doc)) {
            (Some(entity), Ok(filter)) => {
                let mut read = LogicalRead::message(entity);
                read.filter = and_filters(filter, time_range);
                read.projection = field_projection(&req.fields);
                read.pagination = limit_pagination(i64::from(req.limit));
                Some(ir_envelope("read", &read))
            }
            _ => None,
        };
        let spec = match ir {
            Some(Ok(spec)) => spec,
            Some(Err(e)) => return self.record_grpc("TimeSeriesQuery", started, Err(e)),
            None => {
                // The raw table read takes a filter document only; it has no
                // time range or column list, so neither may be dropped here.
                let refused = refuse_unsupported_fields(
                    "TimeSeriesQuery",
                    &[
                        ("from", req.from.is_some(), TS_RANGE_NEEDS_IR),
                        ("to", req.to.is_some(), TS_RANGE_NEEDS_IR),
                        ("fields", !req.fields.is_empty(), TS_RANGE_NEEDS_IR),
                    ],
                );
                if let Err(e) = refused {
                    return self.record_grpc("TimeSeriesQuery", started, Err(e));
                }
                serde_json::json!({
                    "table": collection_of(&req.resource),
                    "filter": filter_doc,
                    "limit": req.limit,
                })
            }
        };
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
        let table_scan = req.query.trim().is_empty();
        // A table scan executes on the named collection; raw SQL executes on
        // whatever its text names (and stays behind the raw-dispatch gate).
        let usage = if table_scan {
            ResourceUse::Collection
        } else {
            ResourceUse::Statement
        };
        let (target, _catalog) = match self
            .authorize_store_target(&security, &req.resource, usage, "analytical.query")
            .await
        {
            Ok(resolved) => resolved,
            Err(e) => return self.record_grpc("AnalyticalQuery", started, Err(e)),
        };
        // A table-scan analytical query (no raw SQL) needs a named collection;
        // guard it at the boundary so an empty identifier returns InvalidArgument
        // rather than leaking the backend's identifier error. bug_report.md B4.
        if table_scan {
            if let Err(e) = require_collection(&req.resource) {
                return self.record_grpc("AnalyticalQuery", started, Err(e));
            }
        }
        if let Err(e) = analytical_query_unsupported(&req) {
            return self.record_grpc("AnalyticalQuery", started, Err(e));
        }
        let ir = match target.entity.as_deref() {
            Some(entity) if table_scan => {
                let mut read = LogicalRead::message(entity);
                read.pagination = limit_pagination(i64::from(req.limit));
                Some(ir_envelope("read", &read))
            }
            _ => None,
        };
        let spec = match ir {
            Some(Ok(spec)) => spec,
            Some(Err(e)) => return self.record_grpc("AnalyticalQuery", started, Err(e)),
            None if table_scan => {
                serde_json::json!({ "table": collection_of(&req.resource), "limit": req.limit })
            }
            None => serde_json::json!({ "sql": req.query }),
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

/// The typed `GraphTraversal` as the executor's `traversal` JSON. Map filters
/// are emitted in key order so the built Cypher is deterministic. The request
/// `limit` is the fallback when the traversal sets none.
fn graph_traversal_json(
    traversal: &crate::proto::GraphTraversal,
    request_limit: i32,
) -> Result<serde_json::Value, Status> {
    let direction = match traversal.direction {
        0 | 1 => "outgoing",
        2 => "incoming",
        3 => "both",
        other => {
            return Err(store_rpc_invalid_fields(
                format!("GraphTraversal.direction {other} is not a known direction"),
                [("traversal.direction", "must be OUTGOING, INCOMING or BOTH")],
            ));
        }
    };
    let sorted = |map: &std::collections::HashMap<String, String>| {
        map.iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let limit = if traversal.limit > 0 {
        traversal.limit
    } else {
        request_limit.max(0)
    };
    Ok(serde_json::json!({
        "start_label": traversal.start_label,
        "start_id": traversal.start_id,
        "relationship_types": traversal.relationship_types,
        "direction": direction,
        "min_depth": traversal.min_depth,
        "max_depth": traversal.max_depth,
        "node_labels": traversal.node_labels,
        "node_property_equals": sorted(&traversal.node_property_equals),
        "relationship_property_equals": sorted(&traversal.relationship_property_equals),
        "limit": limit,
        "return_relationships": traversal.return_relationships,
    }))
}

fn parse_json(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap_or(serde_json::Value::Null)
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

/// Boundary guard for store RPCs that target a table/collection: the request
/// must carry a non-empty identifier (`resource.resource_name`, or
/// `message_type` as the fallback). Returns `InvalidArgument` BEFORE dispatch so
/// an empty identifier never reaches a backend — where the read path returns a
/// clean `InvalidArgument` (e.g. ClickHouse `select_template_sql`) but the write
/// path leaked the driver's `identifier '' is invalid` as `Internal`, giving the
/// two siblings divergent codes for identical bad input. bug_report.md B4
/// (TimeSeries/Document read↔write parity). Key-based ops (cache) and free-query
/// ops (graph cypher, analytical raw SQL) do not target a named collection and
/// are not gated here.
fn require_collection(resource: &Option<crate::proto::StoreResource>) -> Result<(), Status> {
    if collection_of(resource).trim().is_empty() {
        return Err(store_rpc_invalid_fields(
            "resource.resource_name (or resource.message_type) is required",
            [
                (
                    "resource.resource_name",
                    "must be non-empty when resource.message_type is empty",
                ),
                (
                    "resource.message_type",
                    "must be non-empty when resource.resource_name is empty",
                ),
            ],
        ));
    }
    Ok(())
}

// ── Request fields a store RPC cannot honour ───────────────────────────────
//
// A field the broker cannot act on is refused with `Unimplemented` naming the
// field, never accepted and ignored: a caller who sets it is relying on it.

/// Why an `idempotency_key` cannot be honoured on a store RPC: the broker's
/// replay ledger commits in the same PostgreSQL transaction as the write it
/// guards, and these writes go to another store.
const NO_STORE_IDEMPOTENCY: &str = "replay protection is only available on the relational data \
     RPCs (Insert/Update/Upsert/Delete), whose ledger commits with the write; retry this call \
     only when a repeat is harmless";

/// Why a time-series range or column list needs the neutral IR path.
const TS_RANGE_NEEDS_IR: &str = "it needs a catalog entity with a timeseries_store time_field and \
     a filter the neutral IR can express";

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}

fn unsupported_store_field(rpc: &str, field: &str, why: &str) -> Status {
    crate::runtime::executor_utils::capability_status_with_code(
        tonic::Code::Unimplemented,
        "udb",
        rpc,
        field,
        format!("{rpc}.{field} is not supported: {why}; leave it unset"),
    )
}

/// Refuses the first `(field, is_set, why)` entry whose field the caller set.
/// The pure shared-namespace decision of a typed cache RPC (see
/// `DataBrokerService::cache_key_tenant`): `None` keys under the caller's own
/// namespace, `Some(marker)` under the shared one. Shared reads are open to any
/// caller; shared writes need `udb:platform_admin` or `udb:global:write`
/// (`broker::may_write_global_table`, the shared-table write rule). The
/// reserved marker is never a caller tenant.
fn shared_cache_key_tenant(
    context: &crate::RequestContext,
    shared: bool,
    access: CacheAccess,
    operation: &'static str,
) -> Result<Option<String>, Status> {
    let scope = scope_for_caller(
        context.tenant_id.trim().to_string(),
        shared,
        access,
        operation,
        || {
            if crate::broker::may_write_global_table(context) {
                Ok(())
            } else {
                Err(shared_cache_write_denied_status(operation))
            }
        },
    )?;
    Ok(scope.shared.then_some(scope.key_tenant))
}

/// Only the Redis and Memcached cache backends key by tenant, so `shared` is
/// refused naming the field on any other backend.
fn shared_cache_backend_supported(
    backend: Option<crate::backend::BackendKind>,
) -> Result<(), Status> {
    match backend {
        Some(crate::backend::BackendKind::Redis | crate::backend::BackendKind::Memcached) => Ok(()),
        _ => Err(store_rpc_invalid_fields(
            "the shared cache namespace is served by the Redis and Memcached cache backends",
            [(
                "shared",
                "is supported only when resource.backend is a Redis or Memcached cache",
            )],
        )),
    }
}

fn refuse_unsupported_fields(rpc: &str, fields: &[(&str, bool, &str)]) -> Result<(), Status> {
    for (field, is_set, why) in fields {
        if *is_set {
            return Err(unsupported_store_field(rpc, field, why));
        }
    }
    Ok(())
}

fn cache_get_unsupported(req: &crate::proto::CacheGetRequest) -> Result<(), Status> {
    refuse_unsupported_fields(
        "CacheGet",
        &[(
            "touch",
            req.touch,
            "the cache read does not refresh a key's expiry; set the key again with ttl_seconds",
        )],
    )
}

/// A content type the cache can honour: the value is stored as UTF-8 text.
fn text_content_type(content_type: &str) -> bool {
    let base = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    base.is_empty()
        || base.starts_with("text/")
        || base == "application/json"
        || base.ends_with("+json")
        || base == "application/xml"
        || base.ends_with("+xml")
}

fn cache_set_unsupported(req: &crate::proto::CacheSetRequest) -> Result<(), Status> {
    if req.only_if_absent && req.only_if_present {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "CacheSet: only_if_absent and only_if_present exclude each other",
            [(
                "only_if_absent",
                "set at most one of only_if_absent / only_if_present",
            )],
        ));
    }
    refuse_unsupported_fields(
        "CacheSet",
        &[
            (
                "idempotency_key",
                has_text(&req.idempotency_key),
                NO_STORE_IDEMPOTENCY,
            ),
            (
                "content_type",
                !text_content_type(&req.content_type),
                "the cache stores text values; a binary content type would not survive",
            ),
            (
                "metadata",
                req.metadata.as_ref().is_some_and(|m| !m.fields.is_empty()),
                "the cache stores the value only, so metadata would not come back on CacheGet",
            ),
        ],
    )
}

fn time_series_query_unsupported(req: &crate::proto::TimeSeriesQueryRequest) -> Result<(), Status> {
    const NO_AGGREGATION: &str =
        "time-series queries return raw points; aggregate with AnalyticalQuery";
    refuse_unsupported_fields(
        "TimeSeriesQuery",
        &[
            ("group_by", has_text(&req.group_by), NO_AGGREGATION),
            ("aggregate", has_text(&req.aggregate), NO_AGGREGATION),
            ("window", has_text(&req.window), NO_AGGREGATION),
            (
                "page_token",
                has_text(&req.page_token),
                "time-series queries return one page and issue no continuation token",
            ),
        ],
    )
}

fn analytical_query_unsupported(req: &crate::proto::AnalyticalQueryRequest) -> Result<(), Status> {
    refuse_unsupported_fields(
        "AnalyticalQuery",
        &[
            (
                "dry_run",
                req.dry_run,
                "the analytical store has no plan-only mode here, and a dry run must not execute",
            ),
            (
                "parameters",
                req.parameters
                    .as_ref()
                    .is_some_and(|p| !p.fields.is_empty()),
                "the analytical store does not bind query parameters",
            ),
            (
                "page_token",
                has_text(&req.page_token),
                "analytical queries return one page and issue no continuation token",
            ),
        ],
    )
}

/// The requested field list as an IR projection; `None` means every field.
fn field_projection(fields: &[String]) -> Option<LogicalProjection> {
    let fields: Vec<String> = fields
        .iter()
        .map(|field| field.trim().to_string())
        .filter(|field| !field.is_empty())
        .collect();
    (!fields.is_empty()).then(|| LogicalProjection::fields(fields))
}

/// The requested field list as a raw document-query projection
/// (`{"field": 1, ...}`); `None` means every field.
fn mongo_projection(fields: &[String]) -> Option<serde_json::Value> {
    let projection = field_projection(fields)?;
    Some(serde_json::Value::Object(
        projection
            .fields
            .into_iter()
            .map(|field| (field, serde_json::json!(1)))
            .collect(),
    ))
}

/// `DocumentFind.sort` as IR sort keys. A sort key with no field is refused.
fn document_sort(sort: &[crate::proto::Sort]) -> Result<Vec<LogicalSort>, Status> {
    sort.iter()
        .map(|key| {
            let field = key.field.trim();
            if field.is_empty() {
                return Err(store_rpc_invalid_fields(
                    "every sort key needs a field",
                    [("sort.field", "must name a document field")],
                ));
            }
            Ok(LogicalSort {
                field: field.to_string(),
                direction: if key.descending {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
                nulls: NullOrder::Default,
            })
        })
        .collect()
}

/// How a document upsert resolves a conflict on its primary key `pk`.
/// `fields` are the non-key fields the document carries. With `merge_fields`
/// only those fields are updated on an existing document; each must be in the
/// document, and a merge cannot be combined with `replace`.
fn upsert_conflict(
    pk: &str,
    fields: Vec<String>,
    merge_fields: &[String],
    replace: bool,
) -> Result<ConflictStrategy, Status> {
    if merge_fields.is_empty() {
        return Ok(if replace || fields.is_empty() {
            ConflictStrategy::Replace
        } else {
            ConflictStrategy::update(fields)
        });
    }
    if replace {
        return Err(store_rpc_invalid_fields(
            "merge_fields and replace cannot both be set",
            [(
                "merge_fields",
                "must be empty when replace is set (replace overwrites the whole document)",
            )],
        ));
    }
    let mut merged = Vec::with_capacity(merge_fields.len());
    for field in merge_fields {
        let field = field.trim();
        if field.is_empty() || field == pk || !fields.iter().any(|f| f == field) {
            return Err(store_rpc_invalid_fields(
                format!("merge field '{field}' is not a non-key field of the document"),
                [(
                    "merge_fields",
                    "must name non-key fields that the document carries",
                )],
            ));
        }
        if !merged.iter().any(|f: &String| f == field) {
            merged.push(field.to_string());
        }
    }
    Ok(ConflictStrategy::update(merged))
}

/// The time column a time-series entity declared with
/// `timeseries_store.time_field`, if any.
fn timeseries_time_field(manifest: &CatalogManifest, entity: &str) -> Option<String> {
    let crate::broker::TableLookup::Found(table) = crate::broker::table_lookup(manifest, entity)
    else {
        return None;
    };
    manifest
        .stores
        .iter()
        .filter(|store| {
            store.store_kind == "timeseries"
                && store.owner_table == table.table
                && store.owner_schema == table.schema
        })
        .flat_map(|store| store.options.iter())
        .find(|option| option.key == "time_field" && !option.value.trim().is_empty())
        .map(|option| option.value.trim().to_string())
}

fn utc_from_proto_time(
    field: &str,
    seconds: i64,
    nanos: i32,
) -> Result<chrono::DateTime<chrono::Utc>, Status> {
    u32::try_from(nanos)
        .ok()
        .and_then(|nanos| chrono::DateTime::from_timestamp(seconds, nanos))
        .ok_or_else(|| {
            store_rpc_invalid_fields(
                format!("{field} is not a valid timestamp"),
                [(field, "must be a valid google.protobuf.Timestamp")],
            )
        })
}

/// Each point's `timestamp` as UTC, in point order. A stamped point needs a
/// time column to land in; without one the request is refused.
fn point_times(
    points: &[crate::proto::TimeSeriesPoint],
    time_field: Option<&str>,
) -> Result<Vec<Option<chrono::DateTime<chrono::Utc>>>, Status> {
    points
        .iter()
        .map(|point| {
            let Some(ts) = point.timestamp.as_ref() else {
                return Ok(None);
            };
            if time_field.is_none() {
                return Err(unsupported_store_field(
                    "TimeSeriesWrite",
                    "points.timestamp",
                    "the target has no declared timeseries_store time_field to hold it; \
                     send the time inside the point's fields",
                ));
            }
            utc_from_proto_time("points.timestamp", ts.seconds, ts.nanos).map(Some)
        })
        .collect()
}

/// Puts a point's timestamp into its time column. A point that also carries
/// that column in its fields/values is refused: two times for one point.
fn stamp_point_record(
    mut record: LogicalRecord,
    time_field: Option<&str>,
    time: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<LogicalRecord, Status> {
    let (Some(field), Some(time)) = (time_field, time) else {
        return Ok(record);
    };
    if record.contains_key(field) {
        return Err(store_rpc_invalid_fields(
            format!("the point sets both timestamp and the time column '{field}'"),
            [(
                "points.timestamp",
                "must be unset when the point's fields or values carry the time column",
            )],
        ));
    }
    record.insert(field.to_string(), LogicalValue::Timestamp(time));
    Ok(record)
}

/// `from` (inclusive) and `to` (exclusive) as a filter on the time column.
fn time_range_filter(
    time_field: Option<&str>,
    from: Option<(i64, i32)>,
    to: Option<(i64, i32)>,
) -> Result<Option<LogicalFilter>, Status> {
    if from.is_none() && to.is_none() {
        return Ok(None);
    }
    let Some(field) = time_field else {
        return Err(unsupported_store_field(
            "TimeSeriesQuery",
            if from.is_some() { "from" } else { "to" },
            TS_RANGE_NEEDS_IR,
        ));
    };
    let from = from
        .map(|(seconds, nanos)| utc_from_proto_time("from", seconds, nanos))
        .transpose()?;
    let to = to
        .map(|(seconds, nanos)| utc_from_proto_time("to", seconds, nanos))
        .transpose()?;
    if let (Some(from), Some(to)) = (from, to)
        && from > to
    {
        return Err(store_rpc_invalid_fields(
            "from is after to",
            [("from", "must not be later than to")],
        ));
    }
    let bound = |op: ComparisonOp, time: chrono::DateTime<chrono::Utc>| LogicalFilter::Comparison {
        field: field.to_string(),
        op,
        value: LogicalValue::Timestamp(time),
    };
    let mut clauses: Vec<LogicalFilter> = Vec::new();
    if let Some(from) = from {
        clauses.push(bound(ComparisonOp::Ge, from));
    }
    if let Some(to) = to {
        clauses.push(bound(ComparisonOp::Lt, to));
    }
    Ok(and_filters(clauses.pop(), clauses.pop()))
}

fn and_filters(left: Option<LogicalFilter>, right: Option<LogicalFilter>) -> Option<LogicalFilter> {
    match (left, right) {
        (Some(left), Some(right)) => Some(LogicalFilter::And(vec![left, right])),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }
}

fn cache_value_bytes(value: &str) -> Vec<u8> {
    if let Some(encoded) = value.strip_prefix("base64:") {
        use base64::Engine as _;
        return base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap_or_default();
    }
    value.as_bytes().to_vec()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{ManifestColumn, ManifestTable};
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::{Code, Status};

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("typed detail trailer is present");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    #[test]
    fn graph_traversal_json_maps_the_typed_request() {
        let traversal = crate::proto::GraphTraversal {
            start_label: "Doc".to_string(),
            start_id: "d1".to_string(),
            relationship_types: vec!["RELATED".to_string()],
            direction: 3,
            min_depth: 1,
            max_depth: 2,
            node_labels: vec!["Doc".to_string()],
            node_property_equals: [("owner".to_string(), "u1".to_string())]
                .into_iter()
                .collect(),
            relationship_property_equals: [("kind".to_string(), "peer".to_string())]
                .into_iter()
                .collect(),
            limit: 0,
            return_relationships: true,
        };
        let json = graph_traversal_json(&traversal, 25).expect("valid traversal");
        assert_eq!(json["direction"], "both");
        assert_eq!(json["limit"], 25, "the request limit is the fallback");
        assert_eq!(json["node_property_equals"]["owner"], "u1");
        assert_eq!(json["relationship_property_equals"]["kind"], "peer");
        assert_eq!(json["relationship_types"][0], "RELATED");
        assert_eq!(json["return_relationships"], true);

        let unknown = crate::proto::GraphTraversal {
            direction: 9,
            ..traversal
        };
        let err = graph_traversal_json(&unknown, 0).expect_err("unknown direction");
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    fn assert_validation_fields(status: &Status, expected: &[(&str, &str)]) {
        assert_eq!(status.code(), Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), expected.len());
        for (actual, (field, description)) in detail.field_violations.iter().zip(expected) {
            assert_eq!(actual.field, *field);
            assert_eq!(actual.description, *description);
        }
    }

    #[test]
    fn store_rpc_missing_backend_carries_field_violation() {
        let err = require_resource_backend(Some(&crate::proto::StoreResource {
            backend: " ".to_string(),
            ..Default::default()
        }))
        .expect_err("missing resource.backend must fail before backend dispatch");

        assert_eq!(err.message(), "resource.backend is required");
        assert_validation_fields(
            &err,
            &[("resource.backend", "must be a non-empty backend name")],
        );
    }

    #[test]
    fn store_rpc_missing_collection_carries_field_violations() {
        let err = require_collection(&Some(crate::proto::StoreResource {
            resource_name: " ".to_string(),
            message_type: " ".to_string(),
            ..Default::default()
        }))
        .expect_err("missing collection identifier must fail before backend dispatch");

        assert_eq!(
            err.message(),
            "resource.resource_name (or resource.message_type) is required"
        );
        assert_validation_fields(
            &err,
            &[
                (
                    "resource.resource_name",
                    "must be non-empty when resource.message_type is empty",
                ),
                (
                    "resource.message_type",
                    "must be non-empty when resource.resource_name is empty",
                ),
            ],
        );
    }

    fn store_manifest() -> CatalogManifest {
        let table = ManifestTable {
            message_name: "acme.docs.v1.Invoice".into(),
            schema: "docs".into(),
            table: "invoices".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                ManifestColumn {
                    field_name: "id".into(),
                    column_name: "id".into(),
                    proto_type: "string".into(),
                    sql_type: "text".into(),
                    is_primary: true,
                    ..Default::default()
                },
                ManifestColumn {
                    field_name: "amount".into(),
                    column_name: "amount".into(),
                    proto_type: "int64".into(),
                    sql_type: "bigint".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        CatalogManifest {
            tables: vec![table],
            ..Default::default()
        }
    }

    fn resource(message_type: &str, resource_name: &str) -> Option<crate::proto::StoreResource> {
        Some(crate::proto::StoreResource {
            backend: "mongodb".into(),
            message_type: message_type.into(),
            resource_name: resource_name.into(),
            ..Default::default()
        })
    }

    #[test]
    fn store_target_cannot_authorize_as_one_entity_and_execute_on_another_collection() {
        let manifest = store_manifest();
        let err = resolve_store_target(
            &manifest,
            &resource("acme.docs.v1.Invoice", "payroll"),
            ResourceUse::Collection,
        )
        .expect_err("entity X + collection Y must be refused");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("expected 'invoices'"),
            "{}",
            err.message()
        );

        // The entity's own collection (or none) is accepted and IR-compiled.
        for name in ["invoices", ""] {
            let target = resolve_store_target(
                &manifest,
                &resource("acme.docs.v1.Invoice", name),
                ResourceUse::Collection,
            )
            .unwrap();
            assert_eq!(target.authz_object, "acme.docs.v1.Invoice");
            assert_eq!(target.entity.as_deref(), Some("acme.docs.v1.Invoice"));
        }
    }

    #[test]
    fn store_target_for_non_entity_authorizes_the_executed_collection() {
        let manifest = store_manifest();
        // A non-manifest message type cannot vouch for another collection:
        // the collection executed is what gets authorized.
        let target = resolve_store_target(
            &manifest,
            &resource("free.Form", "payroll"),
            ResourceUse::Collection,
        )
        .unwrap();
        assert_eq!(target.authz_object, "payroll");
        assert_eq!(target.entity, None);

        // No message type: authorize on the collection; a manifest table name
        // still compiles through the IR.
        let target = resolve_store_target(
            &manifest,
            &resource("", "invoices"),
            ResourceUse::Collection,
        )
        .unwrap();
        assert_eq!(target.authz_object, "invoices");
        assert_eq!(target.entity.as_deref(), Some("acme.docs.v1.Invoice"));

        // Statement-shaped RPCs (cache keys, raw Cypher/SQL) authorize on the
        // named entity and never take the IR path.
        let target = resolve_store_target(
            &manifest,
            &resource("free.Session", "sessions"),
            ResourceUse::Statement,
        )
        .unwrap();
        assert_eq!(target.authz_object, "free.Session");
        assert_eq!(target.entity, None);
    }

    #[test]
    fn store_target_rejects_wildcards() {
        let manifest = store_manifest();
        for (mt, rn) in [("*", "invoices"), ("acme.*", ""), ("", "*")] {
            for usage in [ResourceUse::Collection, ResourceUse::Statement] {
                let err = resolve_store_target(&manifest, &resource(mt, rn), usage)
                    .expect_err("wildcards must never reach authorize/dispatch");
                assert_eq!(err.code(), Code::InvalidArgument);
            }
        }
    }

    #[test]
    fn mongo_filters_map_to_the_ir_or_refuse() {
        let doc = serde_json::json!({
            "status": "open",
            "amount": {"$gte": 10, "$lt": 100},
            "region": {"$in": ["eu", "us"]},
            "closed_at": null,
        });
        let filter = filter_from_document(&doc)
            .unwrap()
            .expect("non-empty filter");
        let LogicalFilter::And(clauses) = filter else {
            panic!("expected a conjunction");
        };
        assert_eq!(clauses.len(), 5);
        assert!(clauses.contains(&LogicalFilter::IsNull("closed_at".into())));
        assert!(clauses.contains(&LogicalFilter::Comparison {
            field: "amount".into(),
            op: ComparisonOp::Ge,
            value: LogicalValue::Int(10),
        }));

        assert_eq!(filter_from_document(&serde_json::json!({})).unwrap(), None);
        let or = filter_from_document(&serde_json::json!({"$or": [{"a": 1}, {"b": 2}]}))
            .unwrap()
            .unwrap();
        assert!(matches!(or, LogicalFilter::Or(ref b) if b.len() == 2));

        // Operators the IR cannot express fall back to the gated raw path.
        for unmappable in [
            serde_json::json!({"$where": "this.a > 1"}),
            serde_json::json!({"a": {"$regex": "x"}}),
            serde_json::json!({"a": [1, 2]}),
            serde_json::json!({"a": {"nested": 1}}),
            serde_json::json!({"a": {"$eq": null}}),
            serde_json::json!({"$or": [{}, {"a": 1}]}),
        ] {
            assert!(
                filter_from_document(&unmappable).is_err(),
                "must not be mapped: {unmappable}"
            );
        }
    }

    #[test]
    fn typed_document_requests_compile_to_tenant_scoped_ir() {
        use crate::ir::compile::{CompileContext, CompileOperation, compile_for_backend};

        let manifest = store_manifest();
        let read = LogicalRead::message("acme.docs.v1.Invoice")
            .with_filter(pk_filter("id", "inv-1"))
            .with_pagination(LogicalPagination {
                limit: Some(1),
                ..Default::default()
            });
        let envelope = ir_envelope("read", &read).unwrap();
        assert_eq!(envelope["ir"]["op"], "read");
        assert_eq!(envelope["ir"]["message_type"], "acme.docs.v1.Invoice");
        // The envelope payload round-trips into the IR the dispatch core
        // compiles, and the compiled statement carries the tenant scope.
        let mut payload = envelope["ir"].clone();
        payload.as_object_mut().unwrap().remove("op");
        let decoded: LogicalRead = serde_json::from_value(payload).unwrap();
        assert_eq!(decoded, read);
        let ctx = CompileContext::new(&manifest)
            .with_tenant("tenant-a")
            .enforcing_tenant_scope(true);
        if let Some(rendering) = compile_for_backend(
            &crate::backend::BackendKind::Mongodb,
            CompileOperation::Read(&decoded),
            &ctx,
        ) {
            let rendered = serde_json::to_string(&rendering.unwrap()).unwrap();
            assert!(rendered.contains("tenant-a"), "{rendered}");
        }
    }
}

#[cfg(test)]
mod store_field_honesty_tests {
    use super::*;
    use tonic::Code;

    fn manifest_with_time_field(time_field: &str) -> CatalogManifest {
        let mut manifest = CatalogManifest::default();
        manifest.tables.push(crate::generation::ManifestTable {
            message_name: "acme.v1.Reading".into(),
            schema: "public".into(),
            table: "readings".into(),
            primary_key: vec!["id".into()],
            ..Default::default()
        });
        manifest.stores.push(crate::generation::ManifestStore {
            store_kind: "timeseries".into(),
            owner_schema: "public".into(),
            owner_table: "readings".into(),
            options: vec![crate::generation::ManifestStoreOption {
                key: "time_field".into(),
                value: time_field.into(),
            }],
            ..Default::default()
        });
        manifest
    }

    #[test]
    fn cache_set_honours_conditions_and_text_types_and_refuses_the_rest() {
        let base = crate::proto::CacheSetRequest::default();
        assert!(cache_set_unsupported(&base).is_ok());
        // Conditional writes and text content types are honoured.
        for honoured in [
            crate::proto::CacheSetRequest {
                only_if_absent: true,
                ..base.clone()
            },
            crate::proto::CacheSetRequest {
                only_if_present: true,
                ..base.clone()
            },
            crate::proto::CacheSetRequest {
                content_type: "text/plain; charset=utf-8".into(),
                ..base.clone()
            },
            crate::proto::CacheSetRequest {
                content_type: "application/problem+json".into(),
                ..base.clone()
            },
        ] {
            assert!(cache_set_unsupported(&honoured).is_ok(), "{honoured:?}");
        }
        let both = crate::proto::CacheSetRequest {
            only_if_absent: true,
            only_if_present: true,
            ..base.clone()
        };
        assert_eq!(
            cache_set_unsupported(&both).unwrap_err().code(),
            Code::InvalidArgument
        );
        // What the cache cannot keep is refused, not ignored.
        let cases = [
            (
                "idempotency_key",
                crate::proto::CacheSetRequest {
                    idempotency_key: "k".into(),
                    ..base.clone()
                },
            ),
            (
                "content_type",
                crate::proto::CacheSetRequest {
                    content_type: "application/octet-stream".into(),
                    ..base.clone()
                },
            ),
        ];
        for (field, req) in cases {
            let err = cache_set_unsupported(&req).unwrap_err();
            assert_eq!(err.code(), Code::Unimplemented, "{field}");
            assert!(err.message().contains(field), "{field}: {}", err.message());
        }
        let touch = crate::proto::CacheGetRequest {
            touch: true,
            ..Default::default()
        };
        assert_eq!(
            cache_get_unsupported(&touch).unwrap_err().code(),
            Code::Unimplemented
        );
    }

    /// The DataBroker cache RPCs' shared namespace: reads open to any caller,
    /// writes only with `udb:platform_admin` or `udb:global:write`, the marker
    /// never a caller, and only Redis/Memcached serve it.
    #[test]
    fn shared_cache_key_tenant_follows_the_shared_namespace_rules() {
        use super::super::cache_service::scope::CacheAccess;
        let ctx = |tenant: &str, scopes: &[&str]| crate::RequestContext {
            tenant_id: tenant.to_string(),
            project_id: "default".to_string(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            ..crate::RequestContext::default()
        };
        let reader = ctx("tenant-a", &["udb:read", "udb:write", "udb:admin"]);
        let marker = crate::broker::SHARED_TENANT_MARKER.to_string();

        for access in [CacheAccess::Read, CacheAccess::Write] {
            assert_eq!(
                shared_cache_key_tenant(&reader, false, access, "cache.set").unwrap(),
                None,
                "a tenant request keys under the caller"
            );
        }
        assert_eq!(
            shared_cache_key_tenant(&reader, true, CacheAccess::Read, "cache.get").unwrap(),
            Some(marker.clone())
        );
        let denied = shared_cache_key_tenant(&reader, true, CacheAccess::Write, "cache.set")
            .expect_err("a tenant token may not write the shared namespace");
        assert_eq!(denied.code(), Code::PermissionDenied);
        assert!(denied.message().contains(crate::broker::GLOBAL_WRITE_SCOPE));

        for scopes in [
            &[crate::broker::GLOBAL_WRITE_SCOPE][..],
            &["udb:platform_admin"][..],
        ] {
            assert_eq!(
                shared_cache_key_tenant(
                    &ctx("tenant-a", scopes),
                    true,
                    CacheAccess::Write,
                    "cache.delete"
                )
                .unwrap(),
                Some(marker.clone()),
                "{scopes:?}"
            );
        }
        for shared in [false, true] {
            let err = shared_cache_key_tenant(
                &ctx(&marker, &[crate::broker::GLOBAL_WRITE_SCOPE]),
                shared,
                CacheAccess::Read,
                "cache.get",
            )
            .expect_err("the marker is not a caller tenant");
            assert_eq!(err.code(), Code::PermissionDenied);
        }

        for backend in [
            crate::backend::BackendKind::Redis,
            crate::backend::BackendKind::Memcached,
        ] {
            shared_cache_backend_supported(Some(backend)).expect("cache backend");
        }
        for backend in [Some(crate::backend::BackendKind::Postgres), None] {
            let err =
                shared_cache_backend_supported(backend).expect_err("not a tenant-keyed cache");
            assert_eq!(err.code(), Code::InvalidArgument);
        }
    }

    #[test]
    fn analytical_dry_run_and_parameters_are_refused_before_execution() {
        let dry = crate::proto::AnalyticalQueryRequest {
            dry_run: true,
            ..Default::default()
        };
        let err = analytical_query_unsupported(&dry).unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
        assert!(err.message().contains("dry_run"));
        let mut params = prost_types::Struct::default();
        params.fields.insert(
            "id".into(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(1.0)),
            },
        );
        let with_params = crate::proto::AnalyticalQueryRequest {
            parameters: Some(params),
            ..Default::default()
        };
        assert!(
            analytical_query_unsupported(&with_params)
                .unwrap_err()
                .message()
                .contains("parameters")
        );
        let empty_params = crate::proto::AnalyticalQueryRequest {
            parameters: Some(prost_types::Struct::default()),
            ..Default::default()
        };
        assert!(analytical_query_unsupported(&empty_params).is_ok());
    }

    #[test]
    fn time_series_aggregation_fields_are_refused() {
        let cases = [
            (
                "group_by",
                crate::proto::TimeSeriesQueryRequest {
                    group_by: "host".into(),
                    ..Default::default()
                },
            ),
            (
                "aggregate",
                crate::proto::TimeSeriesQueryRequest {
                    aggregate: "avg".into(),
                    ..Default::default()
                },
            ),
            (
                "window",
                crate::proto::TimeSeriesQueryRequest {
                    window: "1m".into(),
                    ..Default::default()
                },
            ),
            (
                "page_token",
                crate::proto::TimeSeriesQueryRequest {
                    page_token: "p".into(),
                    ..Default::default()
                },
            ),
        ];
        for (field, req) in cases {
            let err = time_series_query_unsupported(&req).unwrap_err();
            assert_eq!(err.code(), Code::Unimplemented, "{field}");
            assert!(err.message().contains(field), "{field}");
        }
    }

    #[test]
    fn the_time_field_comes_from_the_timeseries_store() {
        let manifest = manifest_with_time_field("observed_at");
        assert_eq!(
            timeseries_time_field(&manifest, "acme.v1.Reading").as_deref(),
            Some("observed_at")
        );
        assert_eq!(
            timeseries_time_field(&CatalogManifest::default(), "acme.v1.Reading"),
            None
        );
    }

    #[test]
    fn a_point_timestamp_lands_in_the_time_column() {
        let point = crate::proto::TimeSeriesPoint {
            timestamp: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let times = point_times(std::slice::from_ref(&point), Some("observed_at")).unwrap();
        let record =
            stamp_point_record(LogicalRecord::new(), Some("observed_at"), times[0]).unwrap();
        assert_eq!(
            record.get("observed_at"),
            Some(&LogicalValue::Timestamp(
                chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
            ))
        );
        // No time column: the stamped point is refused, never dropped.
        let err = point_times(std::slice::from_ref(&point), None).unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
        // A point with no timestamp is unchanged.
        let unstamped = point_times(&[crate::proto::TimeSeriesPoint::default()], None).unwrap();
        assert_eq!(unstamped, vec![None]);
        // Two times for one point: refused.
        let mut both = LogicalRecord::new();
        both.insert("observed_at".into(), LogicalValue::String("x".into()));
        assert_eq!(
            stamp_point_record(both, Some("observed_at"), times[0])
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn from_and_to_become_a_half_open_time_range() {
        let range = time_range_filter(Some("observed_at"), Some((100, 0)), Some((200, 0)))
            .unwrap()
            .expect("a range");
        let LogicalFilter::And(clauses) = range else {
            panic!("expected an AND of two bounds");
        };
        assert!(clauses.iter().any(|c| matches!(
            c,
            LogicalFilter::Comparison { op: ComparisonOp::Ge, field, .. } if field == "observed_at"
        )));
        assert!(clauses.iter().any(|c| matches!(
            c,
            LogicalFilter::Comparison { op: ComparisonOp::Lt, field, .. } if field == "observed_at"
        )));
        assert!(time_range_filter(Some("t"), None, None).unwrap().is_none());
        assert_eq!(
            time_range_filter(None, Some((1, 0)), None)
                .unwrap_err()
                .code(),
            Code::Unimplemented
        );
        assert_eq!(
            time_range_filter(Some("t"), Some((200, 0)), Some((100, 0)))
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn merge_fields_update_only_the_named_fields() {
        let fields = vec!["name".to_string(), "email".to_string()];
        assert_eq!(
            upsert_conflict("id", fields.clone(), &["email".to_string()], false).unwrap(),
            ConflictStrategy::update(vec!["email".to_string()])
        );
        assert_eq!(
            upsert_conflict("id", fields.clone(), &[], false).unwrap(),
            ConflictStrategy::update(fields.clone())
        );
        assert_eq!(
            upsert_conflict("id", fields.clone(), &["missing".to_string()], false)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(
            upsert_conflict("id", fields, &["email".to_string()], true)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn document_fields_and_sort_map_to_projection_and_order() {
        assert!(field_projection(&[]).is_none());
        assert_eq!(
            field_projection(&["a".to_string(), " ".to_string()])
                .unwrap()
                .fields,
            vec!["a".to_string()]
        );
        assert_eq!(
            mongo_projection(&["a".to_string()]).unwrap(),
            serde_json::json!({ "a": 1 })
        );
        let sort = document_sort(&[crate::proto::Sort {
            field: "age".into(),
            descending: true,
        }])
        .unwrap();
        assert_eq!(sort[0].field, "age");
        assert_eq!(sort[0].direction, SortDirection::Desc);
        assert_eq!(
            document_sort(&[crate::proto::Sort::default()])
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
}
