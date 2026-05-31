use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

use crate::generation::sql::qi;
use crate::generation::{CatalogManifest, ManifestTable};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RequestContext {
    pub tenant_id: String,
    pub user_id: String,
    pub correlation_id: String,
    pub purpose: String,
    pub scopes: Vec<String>,
    /// Optional project namespace. Empty = single-project mode.
    pub project_id: String,
    /// Read consistency hint from `x-udb-consistency`.
    ///
    /// Supported values are `strong`, `bounded_staleness`, and `eventual`.
    /// Empty means the runtime default, currently replica-eligible reads.
    #[serde(default)]
    pub consistency: String,
    /// Optional per-request maximum replica lag from `x-udb-max-replica-lag-ms`.
    /// A value of 0 means use the runtime default.
    #[serde(default)]
    pub max_replica_lag_ms: u64,
    /// Optional client-side catalog version from `x-udb-client-catalog-version`.
    /// Service authorization enforces this against the active catalog.
    #[serde(default)]
    pub client_catalog_version: String,
    /// Optional explicit backend target from `x-udb-target-backend`.
    #[serde(default)]
    pub target_backend: String,
    /// Optional explicit backend instance from `x-udb-target-instance`.
    #[serde(default)]
    pub target_instance: String,
    /// Routing policy hint from `x-udb-routing-policy`.
    #[serde(default)]
    pub routing_policy: String,
    /// Force reads to the primary/write backend.
    #[serde(default)]
    pub primary_read: bool,
    /// Permit eventually consistent read routing when the backend supports it.
    #[serde(default)]
    pub eventual_consistency_allowed: bool,
    /// JSON-encoded ReadFence supplied by SDKs/readers for read-your-writes.
    #[serde(default)]
    pub read_fence_json: String,
}

impl RequestContext {
    pub fn requires_primary_read(&self) -> bool {
        if self.primary_read {
            return true;
        }
        matches!(
            self.consistency
                .to_ascii_lowercase()
                .replace('-', "_")
                .as_str(),
            "strong" | "primary" | "linearizable" | "read_your_writes"
        )
    }

    pub fn replica_lag_override(&self) -> Option<std::time::Duration> {
        (self.max_replica_lag_ms > 0)
            .then(|| std::time::Duration::from_millis(self.max_replica_lag_ms))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct SelectPlanRequest {
    pub context: RequestContext,
    pub message_type: String,
    pub filter: Value,
    pub fields: Vec<String>,
    pub limit: i32,
    pub sort: Vec<SortSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct UpsertPlanRequest {
    pub context: RequestContext,
    pub message_type: String,
    pub record: Value,
    pub conflict_fields: Vec<String>,
    pub return_record: bool,
    pub bypass_cache_write: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DeletePlanRequest {
    pub context: RequestContext,
    pub message_type: String,
    pub filter: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CachePolicyRequest {
    pub message_type: String,
    pub operation: String,
    pub bypass_read: bool,
    pub bypass_write: bool,
    pub ttl_seconds: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VectorSearchPlanRequest {
    pub context: RequestContext,
    pub collection: String,
    pub vector_dimension: usize,
    pub filter: Value,
    pub limit: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct VectorUpsertPlanRequest {
    pub context: RequestContext,
    pub collection: String,
    pub point_dimensions: Vec<usize>,
    pub payloads: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VectorQueryPlan {
    pub collection: String,
    pub backend: String,
    pub expected_dimension: i32,
    pub filter_fields: Vec<String>,
    pub errors: Vec<String>,
}

impl VectorQueryPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VectorUpsertPlan {
    pub collection: String,
    pub backend: String,
    pub expected_dimension: i32,
    pub point_count: usize,
    pub payload_fields: Vec<String>,
    pub errors: Vec<String>,
}

impl VectorUpsertPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectAccessRequest {
    pub context: RequestContext,
    pub bucket: String,
    pub object_key: String,
    pub method: String,
    pub presigned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectStreamPlanRequest {
    pub context: RequestContext,
    pub bucket: String,
    pub object_key: String,
    pub method: String,
    pub chunk_count: usize,
    pub final_chunk_seen: bool,
    pub content_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectAccessDecision {
    pub allowed: bool,
    pub resource_uri: String,
    pub pii: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ObjectStreamPlan {
    pub allowed: bool,
    pub resource_uri: String,
    pub backend: String,
    pub method: String,
    pub requires_server_side_encryption: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuditEvent {
    pub event_type: String,
    pub tenant_id: String,
    pub user_id: String,
    pub correlation_id: String,
    pub purpose: String,
    pub resource_uri: String,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CachePolicyPlan {
    pub backend: String,
    pub key_pattern: String,
    pub ttl_seconds: i32,
    pub read_through: bool,
    pub write_through: bool,
    pub bypass_read: bool,
    pub bypass_write: bool,
    pub invalidates_on_mutation: bool,
    pub errors: Vec<String>,
}

impl CachePolicyPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SqlOperationPlan {
    pub operation: String,
    pub resource_uri: String,
    pub sql: String,
    pub parameter_columns: Vec<String>,
    pub selected_columns: Vec<String>,
    pub conflict_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub cache_policy: CachePolicyPlan,
    pub audit_event_type: String,
    pub errors: Vec<String>,
}

impl SqlOperationPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TransactionMutation {
    pub operation: String,
    pub message_type: String,
    pub record: Value,
    pub filter: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TransactionPlanRequest {
    pub context: RequestContext,
    pub tx_id: String,
    pub mutations: Vec<TransactionMutation>,
    pub commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransactionPlan {
    pub tx_id: String,
    pub state: String,
    pub mutation_count: usize,
    pub mutation_plans: Vec<SqlOperationPlan>,
    pub errors: Vec<String>,
}

impl TransactionPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GenericDispatchRequest {
    pub context: RequestContext,
    pub store_kind: String,
    pub resource_name: String,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GenericDispatchPlan {
    pub store_kind: String,
    pub backend: String,
    pub resource_uri: String,
    pub dsn_env_key: String,
    pub operation: String,
    pub errors: Vec<String>,
}

impl GenericDispatchPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SortSpec {
    pub field: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct QueryPlan {
    pub resource_uri: String,
    pub schema: String,
    pub table: String,
    pub selected_columns: Vec<String>,
    pub filter_columns: Vec<String>,
    pub sort_columns: Vec<String>,
    pub tenant_column: String,
    pub masked_columns: Vec<String>,
    pub cache_key_pattern: String,
    pub sql: String,
    pub parameter_columns: Vec<String>,
    pub errors: Vec<String>,
}

impl QueryPlan {
    pub fn passed(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn build_select_query_plan(
    manifest: &CatalogManifest,
    request: &SelectPlanRequest,
) -> QueryPlan {
    let Some(table) = table_for_message(manifest, &request.message_type) else {
        return QueryPlan {
            errors: vec![format!("unknown message_type {}", request.message_type)],
            ..QueryPlan::default()
        };
    };

    let mut errors = Vec::new();
    if request.context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if request.context.purpose.trim().is_empty() {
        errors.push("purpose is required".to_string());
    }
    if !has_scope(&request.context, "udb:read") {
        errors.push("scope udb:read is required".to_string());
    }

    let allowed = allowed_columns(table);
    let selected_columns = if request.fields.is_empty() {
        table
            .columns
            .iter()
            // Exclude PII and encrypted columns from the implicit SELECT *.
            // Callers must explicitly request PII fields; mask_in_logs controls
            // log redaction only, not access control.
            .filter(|column| !column.security.is_pii && !column.security.is_encrypted)
            .map(|column| column.column_name.clone())
            .collect::<Vec<_>>()
    } else {
        request
            .fields
            .iter()
            .map(|field| field.to_ascii_lowercase())
            .inspect(|field| {
                if !allowed.contains(field) {
                    errors.push(format!("unknown selected field {}", field));
                }
            })
            .collect::<Vec<_>>()
    };

    let mut parameter_columns = Vec::new();
    let compiled_filter = compile_filter_predicates(
        &request.filter,
        &allowed,
        &mut errors,
        &mut parameter_columns,
        1,
    );
    let filter_columns = filter_columns(&request.filter, &allowed, &mut errors);
    let sort_columns = request
        .sort
        .iter()
        .map(|sort| sort.field.to_ascii_lowercase())
        .inspect(|field| {
            if !allowed.contains(field) {
                errors.push(format!("unknown sort field {}", field));
            }
        })
        .collect::<Vec<_>>();
    let tenant_column = tenant_column(table);
    if !tenant_column.is_empty() && !filter_columns.contains(&tenant_column) {
        errors.push(format!(
            "tenant isolation requires filter on {}",
            tenant_column
        ));
    }

    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        if selected_columns.is_empty() {
            "*".to_string()
        } else {
            quote_list(&selected_columns)
        },
        qi(&table.schema),
        qi(&table.table)
    );
    if !compiled_filter.sql.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&compiled_filter.sql);
    }
    if !request.sort.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &request
                .sort
                .iter()
                .map(|sort| {
                    format!(
                        "{} {}",
                        qi(&sort.field.to_ascii_lowercase()),
                        if sort.descending { "DESC" } else { "ASC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    // Cap LIMIT to prevent DoS via arbitrarily large result sets.
    // Callers that need full table scans should use pagination or streaming.
    const MAX_QUERY_LIMIT: i32 = 100_000;
    if request.limit > 0 {
        sql.push_str(&format!(" LIMIT {}", request.limit.min(MAX_QUERY_LIMIT)));
    }

    QueryPlan {
        resource_uri: format!("sql://{}/{}", table.schema, table.table),
        schema: table.schema.clone(),
        table: table.table.clone(),
        selected_columns,
        filter_columns,
        sort_columns,
        tenant_column,
        masked_columns: masked_columns(table),
        cache_key_pattern: cache_key_pattern(manifest, table),
        sql,
        parameter_columns,
        errors,
    }
}

pub fn build_upsert_plan(
    manifest: &CatalogManifest,
    request: &UpsertPlanRequest,
) -> SqlOperationPlan {
    let Some(table) = table_for_message(manifest, &request.message_type) else {
        return SqlOperationPlan {
            operation: "upsert".to_string(),
            errors: vec![format!("unknown message_type {}", request.message_type)],
            ..SqlOperationPlan::default()
        };
    };

    let mut errors = validate_write_context(&request.context);
    let allowed = allowed_columns(table);
    let Some(record) = request.record.as_object() else {
        return SqlOperationPlan {
            operation: "upsert".to_string(),
            resource_uri: format!("sql://{}/{}", table.schema, table.table),
            errors: vec!["record must be a JSON object".to_string()],
            ..SqlOperationPlan::default()
        };
    };

    let mut parameter_columns = Vec::new();
    for key in record.keys() {
        let column = key.to_ascii_lowercase();
        if !allowed.contains(&column) {
            errors.push(format!("unknown record field {}", key));
        } else if !is_server_owned_column(table, &column) {
            parameter_columns.push(column);
        }
    }
    parameter_columns.sort();
    parameter_columns.dedup();

    let tenant = tenant_column(table);
    if !tenant.is_empty() && !parameter_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires record field {}", tenant));
    }
    if let Some(tenant_value) = record.get(&tenant).and_then(Value::as_str)
        && tenant_value != request.context.tenant_id
    {
        errors.push("record tenant_id must match RequestContext.tenant_id".to_string());
    }

    let conflict_columns = if request.conflict_fields.is_empty() {
        table.primary_key.clone()
    } else {
        request
            .conflict_fields
            .iter()
            .map(|field| field.to_ascii_lowercase())
            .collect::<Vec<_>>()
    };
    if conflict_columns.is_empty() {
        errors.push("upsert requires conflict_fields or a manifest primary key".to_string());
    }
    for column in &conflict_columns {
        if !allowed.contains(column) {
            errors.push(format!("unknown conflict field {}", column));
        }
    }
    if !conflict_columns.is_empty() && !conflict_target_is_unique(table, &conflict_columns) {
        errors.push(
            "conflict_fields must match the primary key or a declared unique index".to_string(),
        );
    }

    let update_columns = parameter_columns
        .iter()
        .filter(|column| {
            !conflict_columns.contains(column) && !is_update_excluded_column(table, column)
        })
        .cloned()
        .collect::<Vec<_>>();
    let values = (1..=parameter_columns.len())
        .map(|idx| format!("${idx}"))
        .collect::<Vec<_>>()
        .join(", ");
    let assignments = update_columns
        .iter()
        .map(|column| format!("{} = EXCLUDED.{}", qi(column), qi(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_columns.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {assignments}")
    };
    let returning = if request.return_record {
        " RETURNING *"
    } else {
        ""
    };
    let sql = format!(
        "INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) {}{}",
        qi(&table.schema),
        qi(&table.table),
        quote_list(&parameter_columns),
        values,
        quote_list(&conflict_columns),
        on_conflict,
        returning
    );

    SqlOperationPlan {
        operation: "upsert".to_string(),
        resource_uri: format!("sql://{}/{}", table.schema, table.table),
        sql,
        parameter_columns,
        conflict_columns,
        cache_policy: build_cache_policy_plan(
            manifest,
            &CachePolicyRequest {
                message_type: request.message_type.clone(),
                operation: "upsert".to_string(),
                bypass_write: request.bypass_cache_write,
                ..CachePolicyRequest::default()
            },
        ),
        audit_event_type: "udb.sql.upsert".to_string(),
        errors,
        ..SqlOperationPlan::default()
    }
}

pub fn build_delete_plan(
    manifest: &CatalogManifest,
    request: &DeletePlanRequest,
) -> SqlOperationPlan {
    let Some(table) = table_for_message(manifest, &request.message_type) else {
        return SqlOperationPlan {
            operation: "delete".to_string(),
            errors: vec![format!("unknown message_type {}", request.message_type)],
            ..SqlOperationPlan::default()
        };
    };

    let mut errors = validate_write_context(&request.context);
    let allowed = allowed_columns(table);
    let mut parameter_columns = Vec::new();
    let compiled = compile_filter_predicates(
        &request.filter,
        &allowed,
        &mut errors,
        &mut parameter_columns,
        1,
    );
    let filter_columns = filter_columns(&request.filter, &allowed, &mut errors);
    let tenant = tenant_column(table);
    if !tenant.is_empty() && !filter_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires filter on {}", tenant));
    }
    if compiled.sql.is_empty() {
        errors.push("delete requires at least one safe filter predicate".to_string());
    }
    let sql = format!(
        "DELETE FROM {}.{} WHERE {}",
        qi(&table.schema),
        qi(&table.table),
        if compiled.sql.is_empty() {
            "FALSE".to_string()
        } else {
            compiled.sql
        }
    );

    SqlOperationPlan {
        operation: "delete".to_string(),
        resource_uri: format!("sql://{}/{}", table.schema, table.table),
        sql,
        parameter_columns,
        filter_columns,
        cache_policy: build_cache_policy_plan(
            manifest,
            &CachePolicyRequest {
                message_type: request.message_type.clone(),
                operation: "delete".to_string(),
                ..CachePolicyRequest::default()
            },
        ),
        audit_event_type: "udb.sql.delete".to_string(),
        errors,
        ..SqlOperationPlan::default()
    }
}

pub fn build_transaction_plan(
    manifest: &CatalogManifest,
    request: &TransactionPlanRequest,
) -> TransactionPlan {
    let mut errors = validate_stream_context(&request.context);
    if request.tx_id.trim().is_empty() {
        errors.push("tx_id is required".to_string());
    }
    if request.mutations.is_empty() {
        errors.push("transaction stream requires at least one mutation".to_string());
    }

    let mut mutation_plans = Vec::new();
    for mutation in &request.mutations {
        match mutation.operation.as_str() {
            "upsert" => mutation_plans.push(build_upsert_plan(
                manifest,
                &UpsertPlanRequest {
                    context: request.context.clone(),
                    message_type: mutation.message_type.clone(),
                    record: mutation.record.clone(),
                    ..UpsertPlanRequest::default()
                },
            )),
            "delete" => mutation_plans.push(build_delete_plan(
                manifest,
                &DeletePlanRequest {
                    context: request.context.clone(),
                    message_type: mutation.message_type.clone(),
                    filter: mutation.filter.clone(),
                },
            )),
            other => errors.push(format!("unsupported transaction mutation op {}", other)),
        }
    }
    for plan in &mutation_plans {
        errors.extend(plan.errors.iter().cloned());
    }

    TransactionPlan {
        tx_id: request.tx_id.clone(),
        state: if errors.is_empty() {
            if request.commit {
                "TX_STATE_COMMITTED".to_string()
            } else {
                "TX_STATE_OPEN".to_string()
            }
        } else {
            "TX_STATE_ERROR".to_string()
        },
        mutation_count: mutation_plans.len(),
        mutation_plans,
        errors,
    }
}

pub fn build_cache_policy_plan(
    manifest: &CatalogManifest,
    request: &CachePolicyRequest,
) -> CachePolicyPlan {
    let Some(table) = table_for_message(manifest, &request.message_type) else {
        return CachePolicyPlan {
            errors: vec![format!("unknown message_type {}", request.message_type)],
            ..CachePolicyPlan::default()
        };
    };
    let Some(store) = manifest.stores.iter().find(|store| {
        store.store_kind == "cache"
            && store.owner_schema == table.schema
            && store.owner_table == table.table
    }) else {
        return CachePolicyPlan::default();
    };

    CachePolicyPlan {
        backend: store.backend.clone(),
        key_pattern: store_option(store, "key_pattern"),
        ttl_seconds: if request.ttl_seconds > 0 {
            request.ttl_seconds
        } else {
            store_option_i32(store, "ttl_seconds")
        },
        read_through: store_option_bool(store, "read_through") && !request.bypass_read,
        write_through: store_option_bool(store, "write_through") && !request.bypass_write,
        bypass_read: request.bypass_read,
        bypass_write: request.bypass_write,
        invalidates_on_mutation: matches!(request.operation.as_str(), "upsert" | "delete"),
        ..CachePolicyPlan::default()
    }
}

pub fn build_vector_search_plan(
    manifest: &CatalogManifest,
    request: &VectorSearchPlanRequest,
) -> VectorQueryPlan {
    let mut errors = Vec::new();
    if request.context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if !has_scope(&request.context, "udb:vector:read") {
        errors.push("scope udb:vector:read is required".to_string());
    }

    let Some(store) = manifest
        .stores
        .iter()
        .find(|store| store.store_kind == "vector" && store.resource_name == request.collection)
    else {
        return VectorQueryPlan {
            collection: request.collection.clone(),
            errors: vec![format!("unknown vector collection {}", request.collection)],
            ..VectorQueryPlan::default()
        };
    };

    let expected_dimension = store
        .options
        .iter()
        .find(|option| option.key == "dimension")
        .and_then(|option| option.value.parse::<i32>().ok())
        .unwrap_or_default();
    if expected_dimension > 0 && request.vector_dimension as i32 != expected_dimension {
        errors.push(format!(
            "vector dimension mismatch: got {}, expected {}",
            request.vector_dimension, expected_dimension
        ));
    }

    VectorQueryPlan {
        collection: request.collection.clone(),
        backend: store.backend.clone(),
        expected_dimension,
        filter_fields: vector_filter_fields(&request.filter, &mut errors),
        errors,
    }
}

pub fn build_vector_upsert_plan(
    manifest: &CatalogManifest,
    request: &VectorUpsertPlanRequest,
) -> VectorUpsertPlan {
    let mut errors = Vec::new();
    if request.context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if !has_scope(&request.context, "udb:vector:write") {
        errors.push("scope udb:vector:write is required".to_string());
    }
    if request.point_dimensions.is_empty() {
        errors.push("at least one vector point is required".to_string());
    }

    let Some(store) = manifest
        .stores
        .iter()
        .find(|store| store.store_kind == "vector" && store.resource_name == request.collection)
    else {
        return VectorUpsertPlan {
            collection: request.collection.clone(),
            errors: vec![format!("unknown vector collection {}", request.collection)],
            ..VectorUpsertPlan::default()
        };
    };

    let expected_dimension = store_option_i32(store, "dimension");
    for (idx, dimension) in request.point_dimensions.iter().enumerate() {
        if expected_dimension > 0 && *dimension as i32 != expected_dimension {
            errors.push(format!(
                "vector point {} dimension mismatch: got {}, expected {}",
                idx, dimension, expected_dimension
            ));
        }
    }
    let mut payload_fields = Vec::new();
    for payload in &request.payloads {
        collect_payload_fields(payload, &mut payload_fields);
    }
    payload_fields.sort();
    payload_fields.dedup();

    VectorUpsertPlan {
        collection: request.collection.clone(),
        backend: store.backend.clone(),
        expected_dimension,
        point_count: request.point_dimensions.len(),
        payload_fields,
        errors,
    }
}

pub fn evaluate_object_access(
    manifest: &CatalogManifest,
    request: &ObjectAccessRequest,
) -> ObjectAccessDecision {
    let mut errors = Vec::new();
    if request.context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if request.presigned && !has_scope(&request.context, "udb:object:presign") {
        errors.push("scope udb:object:presign is required".to_string());
    }
    let method = request.method.to_ascii_uppercase();
    if !matches!(method.as_str(), "GET" | "PUT") {
        errors.push("object access method must be GET or PUT".to_string());
    }

    let Some(store) = manifest.stores.iter().find(|store| {
        matches!(store.store_kind.as_str(), "object" | "blob" | "storage")
            && store.resource_name == request.bucket
    }) else {
        return ObjectAccessDecision {
            resource_uri: format!("object://{}", request.bucket),
            errors: vec![format!("unknown object bucket {}", request.bucket)],
            ..ObjectAccessDecision::default()
        };
    };

    if request.presigned {
        let allowed_by_annotation = match method.as_str() {
            "GET" => store_option_bool(store, "presigned_read"),
            "PUT" => store_option_bool(store, "presigned_write"),
            _ => false,
        };
        if !allowed_by_annotation {
            errors.push(format!(
                "presigned {} is not enabled for bucket {}",
                method, request.bucket
            ));
        }
    }

    let column_name = store
        .options
        .iter()
        .find(|option| option.key == "column_name")
        .map(|option| option.value.as_str())
        .unwrap_or_default();
    let pii = manifest
        .table(&store.owner_schema, &store.owner_table)
        .and_then(|table| {
            table
                .columns
                .iter()
                .find(|column| column.column_name == column_name)
        })
        .map(|column| column.security.is_pii || column.security.is_encrypted)
        .unwrap_or(false);

    if pii
        && !matches!(
            request.context.purpose.as_str(),
            "export" | "verification" | "audit"
        )
        && !has_scope(&request.context, "udb:object:pii")
    {
        errors.push(
            "PII object access requires export, verification, audit, or udb:object:pii scope"
                .to_string(),
        );
    }

    ObjectAccessDecision {
        allowed: errors.is_empty(),
        resource_uri: format!(
            "object://{}/{}",
            request.bucket,
            request.object_key.trim_start_matches('/')
        ),
        pii,
        errors,
    }
}

pub fn build_object_stream_plan(
    manifest: &CatalogManifest,
    request: &ObjectStreamPlanRequest,
) -> ObjectStreamPlan {
    let mut decision = evaluate_object_access(
        manifest,
        &ObjectAccessRequest {
            context: request.context.clone(),
            bucket: request.bucket.clone(),
            object_key: request.object_key.clone(),
            method: request.method.clone(),
            presigned: false,
        },
    );
    if !has_scope(&request.context, "udb:stream") {
        decision
            .errors
            .push("scope udb:stream is required".to_string());
    }
    if request.object_key.trim().is_empty() {
        decision.errors.push("object_key is required".to_string());
    }
    if request.method.eq_ignore_ascii_case("PUT") {
        if request.chunk_count == 0 {
            decision
                .errors
                .push("PUT stream requires at least one chunk".to_string());
        }
        if !request.final_chunk_seen {
            decision
                .errors
                .push("PUT stream must end with final_chunk=true".to_string());
        }
    }

    let store = manifest.stores.iter().find(|store| {
        matches!(store.store_kind.as_str(), "object" | "blob" | "storage")
            && store.resource_name == request.bucket
    });
    ObjectStreamPlan {
        allowed: decision.errors.is_empty(),
        resource_uri: decision.resource_uri,
        backend: store.map(|store| store.backend.clone()).unwrap_or_default(),
        method: request.method.to_ascii_uppercase(),
        requires_server_side_encryption: store
            .map(|store| store_option_bool(store, "server_side_encryption"))
            .unwrap_or(false),
        errors: decision.errors,
    }
}

pub fn build_audit_event(
    context: &RequestContext,
    event_type: &str,
    resource_uri: &str,
    checksum_sha256: &str,
) -> AuditEvent {
    AuditEvent {
        event_type: event_type.to_string(),
        tenant_id: context.tenant_id.clone(),
        user_id: context.user_id.clone(),
        correlation_id: context.correlation_id.clone(),
        purpose: context.purpose.clone(),
        resource_uri: resource_uri.to_string(),
        checksum_sha256: checksum_sha256.to_string(),
    }
}

pub fn build_generic_dispatch_plan(
    manifest: &CatalogManifest,
    request: &GenericDispatchRequest,
) -> GenericDispatchPlan {
    let mut errors = Vec::new();
    if request.context.tenant_id.trim().is_empty() {
        errors.push("tenant_id is required".to_string());
    }
    if request.context.purpose.trim().is_empty() {
        errors.push("purpose is required".to_string());
    }
    if !has_scope(&request.context, "udb:dispatch") {
        errors.push("scope udb:dispatch is required".to_string());
    }

    let store_kind = normalize_store_kind(&request.store_kind);
    let Some(store) = manifest.stores.iter().find(|store| {
        normalize_store_kind(&store.store_kind) == store_kind
            && (store.resource_name == request.resource_name
                || store.logical_name == request.resource_name)
    }) else {
        return GenericDispatchPlan {
            store_kind,
            operation: request.operation.clone(),
            errors: vec![format!(
                "unknown {} resource {}",
                request.store_kind, request.resource_name
            )],
            ..GenericDispatchPlan::default()
        };
    };

    GenericDispatchPlan {
        store_kind,
        backend: store.backend.clone(),
        resource_uri: format!(
            "{}://{}",
            normalize_store_kind(&store.store_kind),
            if store.namespace.trim().is_empty() {
                store.resource_name.clone()
            } else {
                format!("{}/{}", store.namespace, store.resource_name)
            }
        ),
        dsn_env_key: store.dsn_env_key.clone(),
        operation: request.operation.clone(),
        errors,
    }
}

pub fn table_for_message<'a>(
    manifest: &'a CatalogManifest,
    message_type: &str,
) -> Option<&'a ManifestTable> {
    let leaf = message_type
        .rsplit('.')
        .next()
        .unwrap_or(message_type)
        .to_ascii_lowercase();
    manifest.tables.iter().find(|table| {
        table.message_name.eq_ignore_ascii_case(message_type)
            || table.message_name.to_ascii_lowercase() == leaf
            || table.table == message_type
            || table.table == leaf
    })
}

fn filter_columns(
    value: &Value,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
) -> Vec<String> {
    let mut out = Vec::new();
    collect_filter_columns(value, allowed, errors, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_filter_columns(
    value: &Value,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
    out: &mut Vec<String>,
) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let normalized = key.to_ascii_lowercase();
                if matches!(normalized.as_str(), "$raw" | "raw" | "sql" | "where_sql") {
                    errors.push(format!("raw SQL filter key '{}' is not allowed", key));
                    continue;
                }
                if matches!(normalized.as_str(), "$and" | "$or" | "and" | "or") {
                    collect_filter_columns(nested, allowed, errors, out);
                    continue;
                }
                if allowed.contains(&normalized) {
                    out.push(normalized);
                    collect_filter_columns(nested, allowed, errors, out);
                } else if !is_operator(&normalized) {
                    errors.push(format!("unknown filter field {}", key));
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_filter_columns(item, allowed, errors, out);
            }
        }
        _ => {}
    }
}

fn is_operator(value: &str) -> bool {
    matches!(
        value,
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" | "$in" | "$like" | "$is_null"
            // GAP 6: PostgreSQL-specific operators
            | "$not_null" | "$ilike" | "$contains" | "$contained_by"
            | "$has_key" | "$overlaps" | "$matches"
    )
}

fn tenant_column(table: &ManifestTable) -> String {
    table
        .columns
        .iter()
        .find(|column| column.column_name == "tenant_id")
        .map(|column| column.column_name.clone())
        .unwrap_or_default()
}

fn masked_columns(table: &ManifestTable) -> Vec<String> {
    table
        .columns
        .iter()
        .filter(|column| column.security.is_pii || column.security.mask_in_logs)
        .map(|column| column.column_name.clone())
        .collect()
}

fn cache_key_pattern(manifest: &CatalogManifest, table: &ManifestTable) -> String {
    manifest
        .stores
        .iter()
        .find(|store| {
            store.store_kind == "cache"
                && store.owner_schema == table.schema
                && store.owner_table == table.table
        })
        .and_then(|store| {
            store
                .options
                .iter()
                .find(|option| option.key == "key_pattern")
                .map(|option| option.value.clone())
        })
        .unwrap_or_default()
}

fn has_scope(context: &RequestContext, required: &str) -> bool {
    context
        .scopes
        .iter()
        .any(|scope| scope == required || scope == "udb:*" || scope == "*")
}

fn vector_filter_fields(value: &Value, errors: &mut Vec<String>) -> Vec<String> {
    let mut fields = Vec::new();
    collect_vector_filter_fields(value, errors, &mut fields);
    fields.sort();
    fields.dedup();
    fields
}

fn collect_vector_filter_fields(value: &Value, errors: &mut Vec<String>, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let normalized = key.to_ascii_lowercase();
                if matches!(normalized.as_str(), "$raw" | "raw" | "sql" | "where_sql") {
                    errors.push(format!("raw vector filter key '{}' is not allowed", key));
                    continue;
                }
                if !normalized.starts_with('$') {
                    out.push(normalized);
                }
                collect_vector_filter_fields(nested, errors, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_vector_filter_fields(item, errors, out);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct CompiledFilter {
    sql: String,
    next_param: usize,
}

fn compile_filter_predicates(
    value: &Value,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
    parameter_columns: &mut Vec<String>,
    start_param: usize,
) -> CompiledFilter {
    match value {
        Value::Object(map) => {
            let mut parts = Vec::new();
            let mut next_param = start_param;
            for (key, nested) in map {
                let normalized = key.to_ascii_lowercase();
                if matches!(normalized.as_str(), "$raw" | "raw" | "sql" | "where_sql") {
                    errors.push(format!("raw SQL filter key '{}' is not allowed", key));
                    continue;
                }
                if matches!(normalized.as_str(), "$and" | "and" | "$or" | "or") {
                    let joiner = if normalized.contains("or") {
                        " OR "
                    } else {
                        " AND "
                    };
                    let compiled = compile_filter_group(
                        nested,
                        allowed,
                        errors,
                        parameter_columns,
                        next_param,
                        joiner,
                    );
                    next_param = compiled.next_param;
                    if !compiled.sql.is_empty() {
                        parts.push(format!("({})", compiled.sql));
                    }
                    continue;
                }
                if !allowed.contains(&normalized) {
                    if !is_operator(&normalized) {
                        errors.push(format!("unknown filter field {}", key));
                    }
                    continue;
                }
                let compiled = compile_column_predicate(
                    &normalized,
                    nested,
                    errors,
                    parameter_columns,
                    next_param,
                );
                next_param = compiled.next_param;
                if !compiled.sql.is_empty() {
                    parts.push(compiled.sql);
                }
            }
            CompiledFilter {
                sql: parts.join(" AND "),
                next_param,
            }
        }
        _ => CompiledFilter {
            sql: String::new(),
            next_param: start_param,
        },
    }
}

fn compile_filter_group(
    value: &Value,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
    parameter_columns: &mut Vec<String>,
    start_param: usize,
    joiner: &str,
) -> CompiledFilter {
    let mut parts = Vec::new();
    let mut next_param = start_param;
    if let Value::Array(items) = value {
        for item in items {
            let compiled =
                compile_filter_predicates(item, allowed, errors, parameter_columns, next_param);
            next_param = compiled.next_param;
            if !compiled.sql.is_empty() {
                parts.push(compiled.sql);
            }
        }
    } else {
        errors.push("logical filter operator requires an array".to_string());
    }
    CompiledFilter {
        sql: parts.join(joiner),
        next_param,
    }
}

fn compile_column_predicate(
    column: &str,
    value: &Value,
    errors: &mut Vec<String>,
    parameter_columns: &mut Vec<String>,
    start_param: usize,
) -> CompiledFilter {
    if let Value::Object(map) = value {
        let mut parts = Vec::new();
        let mut next_param = start_param;
        for (op, op_value) in map {
            let Some(sql_op) = sql_operator(op) else {
                errors.push(format!("unsupported filter operator {}", op));
                continue;
            };
            // Operators that need no bound parameter
            if sql_op == "IS NULL" {
                parts.push(format!("{} IS NULL", qi(column)));
                continue;
            }
            if sql_op == "IS NOT NULL" {
                parts.push(format!("{} IS NOT NULL", qi(column)));
                continue;
            }
            if sql_op == "IN" {
                if !op_value.is_array() {
                    errors.push(format!("$in filter on {} requires an array value", column));
                    continue;
                }
                // PostgreSQL: col = ANY($N) where $N is bound as a text array
                parameter_columns.push(column.to_string());
                parts.push(format!("{} = ANY(${})", qi(column), next_param));
                next_param += 1;
                continue;
            }
            // GAP 6: JSONB containment / array overlap — bind value as JSONB/array cast
            if matches!(sql_op, "@>" | "<@") {
                parameter_columns.push(column.to_string());
                parts.push(format!("{} {} ${}::jsonb", qi(column), sql_op, next_param));
                next_param += 1;
                continue;
            }
            // GAP 6: JSONB key-exists operator takes a text key, no cast needed
            if sql_op == "?" {
                parameter_columns.push(column.to_string());
                parts.push(format!("{} ? ${}", qi(column), next_param));
                next_param += 1;
                continue;
            }
            // GAP 6: tsvector @@ operator — wrap RHS with to_tsquery
            if sql_op == "@@" {
                parameter_columns.push(column.to_string());
                parts.push(format!(
                    "{} @@ to_tsquery('simple', ${})",
                    qi(column),
                    next_param
                ));
                next_param += 1;
                continue;
            }
            // GAP 29: Guard against leading-wildcard full-table-scans via $like/$ilike.
            // A pattern beginning with '%' or '_' forces a sequential scan on every
            // row; B-tree indexes cannot be used. Reject such patterns at the query
            // planner layer so DoS via sequential scan is not possible.
            if matches!(sql_op, "LIKE" | "ILIKE")
                && let Value::String(pattern) = op_value
            {
                if pattern.starts_with('%') || pattern.starts_with('_') {
                    errors.push(format!(
                        "$like/$ilike on column '{}' starts with a wildcard — \
                         this forces a full sequential scan; use a full-text search \
                         index ($matches) or add a trigram GIN index instead",
                        column
                    ));
                    continue;
                }
                if pattern.len() > 256 {
                    errors.push(format!(
                        "$like/$ilike pattern on column '{}' exceeds 256 characters",
                        column
                    ));
                    continue;
                }
            }
            parameter_columns.push(column.to_string());
            parts.push(format!("{} {} ${}", qi(column), sql_op, next_param));
            next_param += 1;
        }
        CompiledFilter {
            sql: parts.join(" AND "),
            next_param,
        }
    } else {
        parameter_columns.push(column.to_string());
        CompiledFilter {
            sql: format!("{} = ${}", qi(column), start_param),
            next_param: start_param + 1,
        }
    }
}

// Phase I: broker.rs split into helper modules.
mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_consistency_helpers() {
        let strong = RequestContext {
            consistency: "strong".to_string(),
            max_replica_lag_ms: 250,
            ..RequestContext::default()
        };
        assert!(strong.requires_primary_read());
        assert_eq!(
            strong.replica_lag_override(),
            Some(std::time::Duration::from_millis(250))
        );

        let eventual = RequestContext {
            consistency: "eventual".to_string(),
            ..RequestContext::default()
        };
        assert!(!eventual.requires_primary_read());
        assert_eq!(eventual.replica_lag_override(), None);
    }
}
