use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Mutex, OnceLock};

use crate::backend::BackendKind;
use crate::generation::sql::{
    qi, resolve_project_column, resolve_tenant_column, table_requires_tenant_column,
};
use crate::generation::{CatalogManifest, ManifestTable};
use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalSort, LogicalValue, LogicalWrite,
    SortDirection,
};

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
    /// mTLS/JWT service identity of the caller (`x-service-identity` /
    /// cert SAN). Emitted to the backend as `app.current_service_identity`
    /// so RLS policies / audit triggers can attribute the connection.
    #[serde(default)]
    pub service_identity: String,
    /// Stable id of the authorization decision that admitted this request.
    /// Emitted to the backend as `app.current_decision_id` so row-level
    /// audit records can be joined back to the broker's decision audit.
    #[serde(default)]
    pub decision_id: String,
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

/// Planner input for the partial-update verb: SET the named `changes` columns
/// and/or apply atomic `increments` on the rows matched by `filter`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct UpdatePlanRequest {
    pub context: RequestContext,
    pub message_type: String,
    pub filter: Value,
    /// Columns to SET (proto field or physical column names; JSON null ⇒ SQL NULL).
    pub changes: Value,
    /// Atomic `col = col + delta` deltas applied in the same statement.
    pub increments: Vec<(String, f64)>,
    pub return_record: bool,
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

/// Compose the single deterministic cache key for `build_select_query_plan`.
///
/// The key encodes EVERY input that can influence the emitted SQL / `QueryPlan`,
/// so two requests collide on the key only when they would build a byte-identical
/// plan. Composition (in order):
///   1. `manifest.checksum_sha256` FIRST — the catalog version. Any schema reload
///      changes the checksum, so a manifest change can never reuse a prior plan
///      (the checksum is a strict prefix of the key, mirroring `manifest_index.rs`).
///      When the checksum is empty we fall back to the `ptr:{:p}:{len}` discriminator
///      that `manifest_index.rs:31` uses so an unchecksummed manifest is never
///      conflated with another.
///   2. `message_type` — selects the table/columns/security.
///   3. canonical filter JSON — `serde_json::Map` is a `BTreeMap` here
///      (no `preserve_order` feature), so `to_string()` is key-order-stable.
///   4. `fields` in request order — order is preserved into `selected_columns`,
///      so it is part of the output identity and must NOT be reordered.
///   5. sort specs (field + direction, in order).
///   6. `limit`.
///   7. planning-affecting `RequestContext` bits: effective SQL backend
///      (`target_backend` drives `effective_sql_backend`, which selects the SQL
///      dialect), `tenant_id`, `project_id`, `purpose`, and the sorted `scopes`
///      (scope order does not affect output, so it is normalized for hit-rate).
fn select_plan_cache_key(manifest: &CatalogManifest, request: &SelectPlanRequest) -> String {
    let manifest_key = if manifest.checksum_sha256.is_empty() {
        format!("ptr:{:p}:{}", manifest, manifest.tables.len())
    } else {
        manifest.checksum_sha256.clone()
    };
    let ctx = &request.context;
    let backend = effective_sql_backend(ctx);
    let mut scopes = ctx.scopes.clone();
    scopes.sort();
    let sort_key = request
        .sort
        .iter()
        .map(|sort| format!("{}:{}", sort.field, sort.descending))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{ck}\u{1f}msg={msg}\u{1f}flt={flt}\u{1f}fields={fields}\u{1f}sort={sort}\u{1f}limit={limit}\u{1f}backend={backend:?}\u{1f}tenant={tenant}\u{1f}project={project}\u{1f}purpose={purpose}\u{1f}scopes={scopes}",
        ck = manifest_key,
        msg = request.message_type,
        flt = request.filter,
        fields = request.fields.join(","),
        sort = sort_key,
        limit = request.limit,
        tenant = ctx.tenant_id,
        project = ctx.project_id,
        purpose = ctx.purpose,
        scopes = scopes.join(","),
    )
}

/// Build the IR→SQL read plan for a typed Select, with a transparent, bounded,
/// process-global memoization (#136 pattern, see `select_plan_cache_key` and
/// `manifest_index.rs`). Identical inputs (including the manifest checksum) return
/// a byte-identical `QueryPlan` cloned from the cache; the planning output is
/// unchanged for every input. On a miss it builds exactly as before, caches, and
/// returns. Because `manifest.checksum_sha256` is the key prefix, a manifest
/// change always produces a fresh key and can never serve a stale plan.
pub fn build_select_query_plan(
    manifest: &CatalogManifest,
    request: &SelectPlanRequest,
) -> QueryPlan {
    static PLAN_CACHE: OnceLock<Mutex<HashMap<String, QueryPlan>>> = OnceLock::new();
    let cache_key = select_plan_cache_key(manifest, request);
    let cache = PLAN_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(guard) = cache.lock()
        && let Some(plan) = guard.get(&cache_key)
    {
        return plan.clone();
    }
    let plan = build_select_query_plan_uncached(manifest, request);
    if let Ok(mut guard) = cache.lock() {
        // Bound the process-global cache. The manifest checksum is part of every
        // key, so without a cap the map grows unbounded across schema reloads.
        // When a new key would exceed the cap, drop the whole cache and rebuild
        // lazily (rebuilding one plan is cheap) — #136.
        const PLAN_CACHE_CAP: usize = 512;
        if guard.len() >= PLAN_CACHE_CAP && !guard.contains_key(&cache_key) {
            guard.clear();
        }
        guard.entry(cache_key).or_insert_with(|| plan.clone());
    }
    plan
}

/// Convert the data-plane Select planner input into the canonical neutral read
/// shape. This is the 2.4 merge seam: the data-plane wrapper still owns its
/// value-adds (context/scope checks, PII projection exclusion, tenant/project
/// isolation, cache/audit metadata), while the emitted read can be compiled by
/// the shared IR compilers once live SQL equivalence is proven.
pub(crate) fn build_select_logical_read(
    manifest: &CatalogManifest,
    request: &SelectPlanRequest,
) -> Result<LogicalRead, Vec<String>> {
    let table = resolve_table_for_message(manifest, &request.message_type)
        .map_err(|error| vec![error.to_string()])?;

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
    // C22: per-table ABAC — a table declaring `required_scope` demands it IN
    // ADDITION to the coarse `udb:read` gate.
    errors.extend(table_required_scope_error(&request.context, table));

    let allowed = allowed_columns(table);
    let resolver = column_resolver(table);
    let filter_json = normalize_filter_keys(&resolver, &request.filter);
    // Validate fields (incl. unknowns nested inside $or) via the broad scan, then
    // use the MANDATORY set for isolation (X-4) — the broad set counts a column
    // inside an $or branch, which does not isolate.
    let _ = filter_columns(&filter_json, &allowed, &mut errors);
    let mandatory_columns = mandatory_and_columns(&filter_json, &allowed);
    let tenant = tenant_column(table);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !mandatory_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires filter on {}", tenant));
    }
    let project = project_column(table);
    if !project.is_empty() && !mandatory_columns.contains(&project) {
        errors.push(format!("project isolation requires filter on {}", project));
    }

    let selected_columns = if request.fields.is_empty() {
        table
            .columns
            .iter()
            .filter(|column| !column.security.is_pii && !column.security.is_encrypted)
            .map(|column| column.column_name.clone())
            .collect::<Vec<_>>()
    } else {
        request
            .fields
            .iter()
            .map(|field| {
                let column = resolve_column(&resolver, field);
                if !allowed.contains(&column) {
                    errors.push(format!("unknown selected field {}", column));
                }
                column
            })
            .collect::<Vec<_>>()
    };

    let sort = request
        .sort
        .iter()
        .map(|sort| {
            let column = resolve_column(&resolver, &sort.field);
            if !allowed.contains(&column) {
                errors.push(format!("unknown sort field {}", column));
            }
            LogicalSort {
                field: column,
                direction: if sort.descending {
                    SortDirection::Desc
                } else {
                    SortDirection::Asc
                },
                nulls: Default::default(),
            }
        })
        .collect::<Vec<_>>();

    let mut filter = logical_filter_from_planner_json(&filter_json, &allowed, &mut errors);
    // #4 soft-delete-read-scope-gap: default-exclude tombstoned rows on the
    // neutral-IR read path too (the served SELECT prefers this bridge). Conjoin
    // `<soft_delete_column> IS NULL` with the caller filter (which already carries
    // the tenant/project isolation predicates); `.and()` keeps the tree flat.
    // Malformed soft-delete metadata fails closed via the pushed error below.
    if let Some(column) = resolve_soft_delete_column(table, &allowed, &mut errors) {
        let live_rows = LogicalFilter::IsNull(column);
        filter = Some(match filter {
            Some(existing) => existing.and(live_rows),
            None => live_rows,
        });
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    const MAX_QUERY_LIMIT: i32 = 100_000;
    let pagination = (request.limit > 0).then(|| LogicalPagination {
        limit: Some(request.limit.min(MAX_QUERY_LIMIT) as u32),
        ..Default::default()
    });

    Ok(LogicalRead {
        message_type: request.message_type.clone(),
        filter,
        projection: Some(LogicalProjection::fields(selected_columns)),
        sort,
        include: Vec::new(),
        pagination,
    })
}

fn build_select_query_plan_uncached(
    manifest: &CatalogManifest,
    request: &SelectPlanRequest,
) -> QueryPlan {
    let table = match resolve_table_for_message(manifest, &request.message_type) {
        Ok(table) => table,
        Err(error) => {
            return QueryPlan {
                errors: vec![error.to_string()],
                ..QueryPlan::default()
            };
        }
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
    // C22: per-table ABAC — a table declaring `required_scope` demands it IN
    // ADDITION to the coarse `udb:read` gate.
    errors.extend(table_required_scope_error(&request.context, table));

    let allowed = allowed_columns(table);
    // #117: resolve proto `field_name` aliases to physical `column_name`s before
    // validation/emission, so a column override (`field_name != column_name`)
    // doesn't reject a valid request or build SQL against the wrong column.
    let resolver = column_resolver(table);
    let filter = normalize_filter_keys(&resolver, &request.filter);
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
            .map(|field| resolve_column(&resolver, field))
            .inspect(|field| {
                if !allowed.contains(field) {
                    errors.push(format!("unknown selected field {}", field));
                }
            })
            .collect::<Vec<_>>()
    };

    let mut parameter_columns = Vec::new();
    let backend_kind = effective_sql_backend(&request.context);
    let encrypted = encrypted_filter_columns(table);
    let compiled_filter = compile_filter_predicates(
        &filter,
        &allowed,
        &encrypted,
        &mut errors,
        &mut parameter_columns,
        1,
        &backend_kind,
    );
    let filter_columns = filter_columns(&filter, &allowed, &mut errors);
    let mandatory_columns = mandatory_and_columns(&filter, &allowed);
    let sort_columns = request
        .sort
        .iter()
        .map(|sort| resolve_column(&resolver, &sort.field))
        .inspect(|field| {
            if !allowed.contains(field) {
                errors.push(format!("unknown sort field {}", field));
            }
        })
        .collect::<Vec<_>>();
    let tenant_column = tenant_column(table);
    if tenant_column.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !mandatory_columns.contains(&tenant_column) {
        errors.push(format!(
            "tenant isolation requires filter on {}",
            tenant_column
        ));
    }
    // Project isolation (mirrors tenant): when the table declares a project key,
    // the read must filter on it so a query cannot span projects.
    let project_column = project_column(table);
    if !project_column.is_empty() && !mandatory_columns.contains(&project_column) {
        errors.push(format!(
            "project isolation requires filter on {}",
            project_column
        ));
    }

    // #4 soft-delete-read-scope-gap: exclude tombstoned rows from ordinary reads
    // by default. Inject `<soft_delete_column> IS NULL` into the mandatory
    // conjunction so it composes with the caller filter (including a top-level
    // `$or`, which `compile_filter_predicates` already parenthesizes), the
    // tenant/project mandatory filters, and the LIMIT below. The predicate binds
    // no parameter, so `$N` numbering is unchanged. Malformed soft-delete metadata
    // fails closed via the pushed error rather than leaking deleted rows.
    let soft_delete_predicate = resolve_soft_delete_column(table, &allowed, &mut errors)
        .map(|column| format!("{} IS NULL", qi(&column)));

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
    let mut where_fragments = Vec::new();
    if !compiled_filter.sql.is_empty() {
        where_fragments.push(compiled_filter.sql.clone());
    }
    if let Some(predicate) = soft_delete_predicate {
        where_fragments.push(predicate);
    }
    if !where_fragments.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_fragments.join(" AND "));
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
                        qi(&resolve_column(&resolver, &sort.field)),
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
    let table = match resolve_table_for_message(manifest, &request.message_type) {
        Ok(table) => table,
        Err(error) => {
            return SqlOperationPlan {
                operation: "upsert".to_string(),
                errors: vec![error.to_string()],
                ..SqlOperationPlan::default()
            };
        }
    };

    let mut errors = validate_write_context(&request.context);
    // C22: per-table ABAC on the write path.
    errors.extend(table_required_scope_error(&request.context, table));
    let allowed = allowed_columns(table);
    // #117: resolve proto `field_name` aliases (record keys, conflict fields) to
    // physical `column_name`s. Idempotent on already-physical names; the runtime
    // applies the same `normalize_record_keys` before binding values.
    let resolver = column_resolver(table);
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
        let column = resolve_column(&resolver, key);
        if !allowed.contains(&column) {
            errors.push(format!("unknown record field {}", key));
        } else if !is_server_owned_column(table, &column) {
            parameter_columns.push(column);
        }
    }
    parameter_columns.sort();
    parameter_columns.dedup();

    let tenant = tenant_column(table);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !parameter_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires record field {}", tenant));
    }
    // Look up the tenant value case-insensitively: `record.get(&tenant)` uses the
    // lowercase manifest column name, but JSON keys arrive in original case, so a
    // `{"TenantId": …}` payload would miss the lookup and SKIP this mismatch check
    // — letting a foreign tenant_id through. Match any key whose lowercase equals
    // the tenant column.
    if !tenant.is_empty()
        && let Some(tenant_value) = record
            .iter()
            .find(|(key, _)| resolve_column(&resolver, key) == tenant)
            .and_then(|(_, value)| value.as_str())
        && tenant_value != request.context.tenant_id
    {
        errors.push("record tenant_id must match RequestContext.tenant_id".to_string());
    }

    // Project isolation (mirrors tenant). When the request carries a project_id
    // and the table has a project key, the record must include it and the value
    // must match the request context — preventing cross-project upserts. Gated on
    // a non-empty context project_id so single-project deployments are unaffected.
    let project = project_column(table);
    if !project.is_empty() && !request.context.project_id.is_empty() {
        if !parameter_columns.contains(&project) {
            errors.push(format!(
                "project isolation requires record field {}",
                project
            ));
        }
        if let Some(project_value) = record
            .iter()
            .find(|(key, _)| resolve_column(&resolver, key) == project)
            .and_then(|(_, value)| value.as_str())
            && project_value != request.context.project_id
        {
            errors.push("record project_id must match RequestContext.project_id".to_string());
        }
    }

    let conflict_columns = if request.conflict_fields.is_empty() {
        table.primary_key.clone()
    } else {
        request
            .conflict_fields
            .iter()
            .map(|field| resolve_column(&resolver, field))
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

/// Convert the data-plane Upsert planner input into a neutral write while
/// preserving the wrapper's validation boundary (scope, tenant/project binding,
/// server-owned column exclusion, conflict uniqueness, and return shape).
pub(crate) fn build_upsert_logical_write(
    manifest: &CatalogManifest,
    request: &UpsertPlanRequest,
) -> Result<LogicalWrite, Vec<String>> {
    let table = resolve_table_for_message(manifest, &request.message_type)
        .map_err(|error| vec![error.to_string()])?;

    let mut errors = validate_write_context(&request.context);
    // C22: per-table ABAC on the write path.
    errors.extend(table_required_scope_error(&request.context, table));
    let allowed = allowed_columns(table);
    let resolver = column_resolver(table);
    let Some(record) = request.record.as_object() else {
        return Err(vec!["record must be a JSON object".to_string()]);
    };

    let mut logical_record: LogicalRecord = BTreeMap::new();
    for (key, value) in record {
        let column = resolve_column(&resolver, key);
        if !allowed.contains(&column) {
            errors.push(format!("unknown record field {}", key));
        } else if !is_server_owned_column(table, &column) {
            logical_record.insert(column, logical_value_from_json(value));
        }
    }
    if logical_record.is_empty() {
        errors.push("upsert requires at least one client-writable record field".to_string());
    }

    let record_columns = logical_record.keys().cloned().collect::<Vec<_>>();
    let tenant = tenant_column(table);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !record_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires record field {}", tenant));
    }
    if !tenant.is_empty()
        && let Some(LogicalValue::String(tenant_value)) = logical_record.get(&tenant)
        && tenant_value != &request.context.tenant_id
    {
        errors.push("record tenant_id must match RequestContext.tenant_id".to_string());
    }

    let project = project_column(table);
    if !project.is_empty() && !request.context.project_id.is_empty() {
        if !record_columns.contains(&project) {
            errors.push(format!(
                "project isolation requires record field {}",
                project
            ));
        }
        if let Some(LogicalValue::String(project_value)) = logical_record.get(&project)
            && project_value != &request.context.project_id
        {
            errors.push("record project_id must match RequestContext.project_id".to_string());
        }
    }

    let conflict_columns = if request.conflict_fields.is_empty() {
        table.primary_key.clone()
    } else {
        request
            .conflict_fields
            .iter()
            .map(|field| resolve_column(&resolver, field))
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

    let update_columns = record_columns
        .iter()
        .filter(|column| {
            !conflict_columns.contains(column) && !is_update_excluded_column(table, column)
        })
        .cloned()
        .collect::<Vec<_>>();
    let uses_primary_conflict =
        request.conflict_fields.is_empty() || conflict_columns == table.primary_key;
    let conflict = if update_columns.is_empty() {
        if !uses_primary_conflict {
            errors.push(
                "neutral IR cannot represent alternate-unique ON CONFLICT DO NOTHING yet"
                    .to_string(),
            );
        }
        ConflictStrategy::Ignore
    } else if uses_primary_conflict {
        ConflictStrategy::update(update_columns)
    } else {
        ConflictStrategy::update_on(update_columns, conflict_columns.clone())
    };

    if !errors.is_empty() {
        return Err(errors);
    }

    let return_fields = if request.return_record {
        table
            .columns
            .iter()
            .map(|column| column.column_name.clone())
            .collect()
    } else {
        Vec::new()
    };

    Ok(LogicalWrite {
        message_type: request.message_type.clone(),
        records: vec![logical_record],
        conflict,
        return_fields,
    })
}

/// Build the IR→SQL plan for a typed Delete.
///
/// #3 soft-delete-is-hard-delete: when the resolved table is annotated
/// `soft_delete`, the emitted statement is NOT a physical `DELETE` but an
/// idempotent `UPDATE … SET <soft_delete_column> = NOW()` that stamps the
/// declared tombstone column using the SAME WHERE/tenant/project predicate the
/// physical delete would have used. The SET clause binds no parameter, so
/// `parameter_columns` and the `$N` filter numbering are byte-identical to the
/// physical-delete plan — the compare-and-swap precondition (enforced upstream
/// on the same normalized filter), tenant/project isolation, and the
/// affected-rows contract are all preserved. Malformed soft-delete metadata
/// fails closed (see [`resolve_soft_delete_column`]) and NEVER degrades to a
/// physical `DELETE`.
pub fn build_delete_plan(
    manifest: &CatalogManifest,
    request: &DeletePlanRequest,
) -> SqlOperationPlan {
    let table = match resolve_table_for_message(manifest, &request.message_type) {
        Ok(table) => table,
        Err(error) => {
            return SqlOperationPlan {
                operation: "delete".to_string(),
                errors: vec![error.to_string()],
                ..SqlOperationPlan::default()
            };
        }
    };

    let mut errors = validate_write_context(&request.context);
    // C22: per-table ABAC on the write path.
    errors.extend(table_required_scope_error(&request.context, table));
    let allowed = allowed_columns(table);
    // #117: resolve proto `field_name` aliases in the delete filter.
    let resolver = column_resolver(table);
    let filter = normalize_filter_keys(&resolver, &request.filter);
    let mut parameter_columns = Vec::new();
    let backend_kind = effective_sql_backend(&request.context);
    let encrypted = encrypted_filter_columns(table);
    let compiled = compile_filter_predicates(
        &filter,
        &allowed,
        &encrypted,
        &mut errors,
        &mut parameter_columns,
        1,
        &backend_kind,
    );
    let filter_columns = filter_columns(&filter, &allowed, &mut errors);
    let mandatory_columns = mandatory_and_columns(&filter, &allowed);
    let tenant = tenant_column(table);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !mandatory_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires filter on {}", tenant));
    }
    let project = project_column(table);
    if !project.is_empty() && !mandatory_columns.contains(&project) {
        errors.push(format!("project isolation requires filter on {}", project));
    }
    if compiled.sql.is_empty() {
        errors.push("delete requires at least one safe filter predicate".to_string());
    }
    // #3: resolve the tombstone column (fail-closed on malformed metadata) BEFORE
    // emitting anything, so a soft-delete table can never fall through to a
    // physical DELETE.
    let soft_delete_column = resolve_soft_delete_column(table, &allowed, &mut errors);
    let where_clause = if compiled.sql.is_empty() {
        "FALSE".to_string()
    } else {
        compiled.sql
    };
    let sql = match (table.soft_delete, &soft_delete_column) {
        // Not a soft-delete table: ordinary physical delete (unchanged behavior).
        (false, _) => format!(
            "DELETE FROM {}.{} WHERE {}",
            qi(&table.schema),
            qi(&table.table),
            where_clause
        ),
        // Soft-delete table, valid metadata: idempotent tombstone UPDATE that
        // reuses the exact delete WHERE/tenant/project predicate.
        (true, Some(column)) => format!(
            "UPDATE {}.{} SET {} = NOW() WHERE {}",
            qi(&table.schema),
            qi(&table.table),
            qi(column),
            where_clause
        ),
        // Soft-delete table, MALFORMED metadata: an error is already queued, so
        // fail closed with an inert (never-matching) UPDATE — we must never emit
        // a physical DELETE for a table declared soft-delete.
        (true, None) => {
            let inert = table.soft_delete_column.trim();
            let inert = if inert.is_empty() {
                "deleted_at"
            } else {
                inert
            };
            format!(
                "UPDATE {}.{} SET {} = NOW() WHERE FALSE",
                qi(&table.schema),
                qi(&table.table),
                qi(inert)
            )
        }
    };

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

/// Compile the partial-update verb: `UPDATE s.t SET a=$1[, cnt=cnt+$2] WHERE …`.
/// SET parameters come FIRST ($1..$k in `parameter_columns` order: changes then
/// increments), the filter predicate's parameters start at $k+1 — the executor
/// binds `changes ++ increments ++ filter` values in exactly that order. Shares
/// every safety property with the delete plan: field-name aliasing, tenant AND
/// project isolation on the MANDATORY column set, safe-predicate requirement,
/// and the encrypted-filter posture of `compile_filter_predicates`. Isolation
/// and primary-key columns are IMMUTABLE through this verb (identity changes
/// belong to Upsert), and increment columns must not also appear in `changes`.
pub fn build_update_plan(
    manifest: &CatalogManifest,
    request: &UpdatePlanRequest,
) -> SqlOperationPlan {
    let table = match resolve_table_for_message(manifest, &request.message_type) {
        Ok(table) => table,
        Err(error) => {
            return SqlOperationPlan {
                operation: "update".to_string(),
                errors: vec![error.to_string()],
                ..SqlOperationPlan::default()
            };
        }
    };

    let mut errors = validate_write_context(&request.context);
    // C22: per-table ABAC on the write path.
    errors.extend(table_required_scope_error(&request.context, table));
    let allowed = allowed_columns(table);
    let resolver = column_resolver(table);
    let filter = normalize_filter_keys(&resolver, &request.filter);

    // ── SET clause: changes (typed binds by column) then increments ──────────
    let tenant = tenant_column(table);
    let project = project_column(table);
    let immutable: BTreeSet<String> = table
        .primary_key
        .iter()
        .cloned()
        .chain([tenant.clone(), project.clone()])
        .filter(|column| !column.is_empty())
        .collect();
    let mut set_fragments: Vec<String> = Vec::new();
    let mut parameter_columns: Vec<String> = Vec::new();
    let mut next_param = 1usize;
    // NORMALIZE-then-SORT: the executor rebuilds the bind-value order from the
    // normalized physical column names (BTreeMap iteration), so the plan must
    // order SET parameters by the NORMALIZED name — sorting raw keys would
    // diverge whenever a field alias sorts differently from its column.
    for (normalized, key) in normalized_update_changes(&request.changes, &resolver, &mut errors) {
        if !allowed.contains(&normalized) {
            errors.push(format!("unknown update column {key}"));
            continue;
        }
        if immutable.contains(&normalized) {
            errors.push(format!(
                "column {normalized} is immutable through Update (primary key / tenant / project); use Upsert for identity changes"
            ));
            continue;
        }
        set_fragments.push(format!("{} = ${next_param}", qi(&normalized)));
        parameter_columns.push(normalized);
        next_param += 1;
    }
    let change_columns: BTreeSet<String> = parameter_columns.iter().cloned().collect();
    for (column, _delta) in &request.increments {
        let normalized = resolver
            .get(&column.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| column.to_ascii_lowercase());
        if !allowed.contains(&normalized) {
            errors.push(format!("unknown increment column {column}"));
            continue;
        }
        if immutable.contains(&normalized) {
            errors.push(format!("column {normalized} is immutable through Update"));
            continue;
        }
        if change_columns.contains(&normalized) {
            errors.push(format!(
                "column {normalized} appears in both changes and increments"
            ));
            continue;
        }
        set_fragments.push(format!(
            "{col} = {col} + ${next_param}",
            col = qi(&normalized)
        ));
        parameter_columns.push(normalized);
        next_param += 1;
    }
    if set_fragments.is_empty() {
        errors.push("update requires at least one change or increment".to_string());
    }

    // ── WHERE clause: identical machinery + isolation posture to delete ──────
    let backend_kind = effective_sql_backend(&request.context);
    let encrypted = encrypted_filter_columns(table);
    let compiled = compile_filter_predicates(
        &filter,
        &allowed,
        &encrypted,
        &mut errors,
        &mut parameter_columns,
        next_param,
        &backend_kind,
    );
    let filter_columns = filter_columns(&filter, &allowed, &mut errors);
    let mandatory_columns = mandatory_and_columns(&filter, &allowed);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !mandatory_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires filter on {}", tenant));
    }
    if !project.is_empty() && !mandatory_columns.contains(&project) {
        errors.push(format!("project isolation requires filter on {}", project));
    }
    if compiled.sql.is_empty() {
        errors.push("update requires at least one safe filter predicate".to_string());
    }

    let sql = format!(
        "UPDATE {}.{} SET {} WHERE {}{}",
        qi(&table.schema),
        qi(&table.table),
        set_fragments.join(", "),
        if compiled.sql.is_empty() {
            "FALSE".to_string()
        } else {
            compiled.sql
        },
        if request.return_record {
            " RETURNING *"
        } else {
            ""
        },
    );

    SqlOperationPlan {
        operation: "update".to_string(),
        resource_uri: format!("sql://{}/{}", table.schema, table.table),
        sql,
        parameter_columns,
        filter_columns,
        cache_policy: build_cache_policy_plan(
            manifest,
            &CachePolicyRequest {
                message_type: request.message_type.clone(),
                operation: "update".to_string(),
                ..CachePolicyRequest::default()
            },
        ),
        audit_event_type: "udb.sql.update".to_string(),
        errors,
        ..SqlOperationPlan::default()
    }
}

/// Normalize an Update `changes` object into `(physical_column, raw_key)`
/// pairs SORTED by physical column — the shared ordering contract between
/// [`build_update_plan`] (which numbers the SET parameters) and the executor
/// (which binds the values). Duplicate normalizations (a field alias and its
/// physical column both present) are an error, not a silent overwrite.
pub fn normalized_update_changes(
    changes: &Value,
    resolver: &std::collections::HashMap<String, String>,
    errors: &mut Vec<String>,
) -> Vec<(String, String)> {
    let mut out: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    match changes {
        Value::Object(map) => {
            for key in map.keys() {
                let normalized = resolver
                    .get(&key.to_ascii_lowercase())
                    .cloned()
                    .unwrap_or_else(|| key.to_ascii_lowercase());
                if let Some(previous) = out.insert(normalized.clone(), key.clone()) {
                    errors.push(format!(
                        "update changes name column {normalized} twice ({previous} and {key})"
                    ));
                }
            }
        }
        Value::Null => {}
        _ => errors.push("update changes must be a JSON object of column -> value".to_string()),
    }
    out.into_iter().collect()
}

/// Convert the data-plane Delete planner input into the canonical neutral
/// delete shape. The bridge keeps the data-plane tenant/project/scope checks and
/// refuses unbounded or planner-only Postgres filters rather than downgrading
/// them to a broader delete.
pub(crate) fn build_delete_logical_delete(
    manifest: &CatalogManifest,
    request: &DeletePlanRequest,
) -> Result<LogicalDelete, Vec<String>> {
    let table = resolve_table_for_message(manifest, &request.message_type)
        .map_err(|error| vec![error.to_string()])?;

    // #3: a soft-delete table must NOT lower to a neutral hard `DELETE`. Decline
    // so the served delete path (`bridged_pg_delete_statement` → `.ok()?`) falls
    // back to the planner's soft-delete UPDATE in `build_delete_plan`; direct
    // callers get a clear routing error instead of a silent physical delete.
    if table.soft_delete {
        return Err(vec![format!(
            "soft-delete table {}.{} does not lower to a neutral hard delete; \
             the planner emits a soft-delete UPDATE instead",
            table.schema, table.table
        )]);
    }

    let mut errors = validate_write_context(&request.context);
    // C22: per-table ABAC on the write path.
    errors.extend(table_required_scope_error(&request.context, table));
    let allowed = allowed_columns(table);
    let resolver = column_resolver(table);
    let filter_json = normalize_filter_keys(&resolver, &request.filter);
    // Validate fields (incl. unknowns nested inside $or) via the broad scan, then
    // use the MANDATORY set for isolation (X-4) — the broad set counts a column
    // inside an $or branch, which does not isolate.
    let _ = filter_columns(&filter_json, &allowed, &mut errors);
    let mandatory_columns = mandatory_and_columns(&filter_json, &allowed);

    let tenant = tenant_column(table);
    if tenant.is_empty() {
        if table_requires_tenant_column(table) {
            errors.push(unresolved_tenant_column_error(table));
        }
    } else if !mandatory_columns.contains(&tenant) {
        errors.push(format!("tenant isolation requires filter on {}", tenant));
    }
    let project = project_column(table);
    if !project.is_empty() && !mandatory_columns.contains(&project) {
        errors.push(format!("project isolation requires filter on {}", project));
    }

    let filter = logical_filter_from_planner_json(&filter_json, &allowed, &mut errors);
    if filter.is_none() {
        errors.push("delete requires at least one safe filter predicate".to_string());
    }
    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(LogicalDelete {
        message_type: request.message_type.clone(),
        filter: filter.expect("checked above"),
        return_fields: Vec::new(),
    })
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
    let table = match resolve_table_for_message(manifest, &request.message_type) {
        Ok(table) => table,
        Err(error) => {
            return CachePolicyPlan {
                errors: vec![error.to_string()],
                ..CachePolicyPlan::default()
            };
        }
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

// `table_for_message` moved to `crate::generation::manifest_index` so the
// WASM-portable `udb-portable` crate can resolve `crate::broker::table_for_message`
// (the one broker fn the IR→SQL compilers call) without this native module.
// Re-exported here so server call-sites (`crate::broker::table_for_message`,
// `crate::planning::broker::table_for_message`) are unchanged.
pub use crate::generation::manifest_index::{
    TableLookup, TableLookupError, describe_table_lookup_miss, resolve_table_for_message,
    table_for_message, table_lookup,
};

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

/// Columns that constrain EVERY row a read returns or a delete affects — i.e.
/// those in the top-level AND context, descending through `$and` but NOT `$or`.
///
/// Tenant/project isolation MUST check this set, not the broad `filter_columns`
/// (which collects a column appearing anywhere, including inside an `$or`
/// branch). A predicate inside `$or` does not isolate — `{"$or":[{tenant_id:a},
/// {status:open}]}` returns rows that satisfy EITHER arm, so it can leak rows of
/// other tenants while still "mentioning" `tenant_id` (X-4).
fn mandatory_and_columns(value: &Value, allowed: &BTreeSet<String>) -> Vec<String> {
    let mut out = Vec::new();
    collect_mandatory_and_columns(value, allowed, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_mandatory_and_columns(value: &Value, allowed: &BTreeSet<String>, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let normalized = key.to_ascii_lowercase();
                if matches!(normalized.as_str(), "$and" | "and") {
                    // Every arm of an AND constrains every row → still mandatory.
                    collect_mandatory_and_columns(nested, allowed, out);
                } else if matches!(normalized.as_str(), "$or" | "or") {
                    // An OR arm does NOT constrain every row → not mandatory. Skip.
                    continue;
                } else if allowed.contains(&normalized) {
                    out.push(normalized);
                }
            }
        }
        Value::Array(items) => {
            // Reached only as an `$and`'s array value → every item is mandatory.
            for item in items {
                collect_mandatory_and_columns(item, allowed, out);
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
    resolve_tenant_column(table).unwrap_or_default().to_string()
}

/// Resolve the project-isolation column for a table: the proto-declared
/// `project_column: true` designator first (authoritative), else the well-known
/// `project_id` name. Mirrors [`tenant_column`]. Empty when the table has no
/// project key.
fn project_column(table: &ManifestTable) -> String {
    resolve_project_column(table)
        .unwrap_or_default()
        .to_string()
}

/// Resolve the declared soft-delete (tombstone) column for a table, failing
/// closed on malformed soft-delete metadata (#3 delete rewrite / #4 read scope).
///
/// Returns:
///   * `None` when the table is NOT soft-delete — the caller emits an ordinary
///     physical delete / injects no live-row read predicate;
///   * `Some(column)` — the validated, declared soft-delete column (the physical
///     `column_name`, lowercased to match `allowed`), ready to quote with [`qi`];
///   * `None` **with an error pushed onto `errors`** when the table declares
///     `soft_delete` but its `soft_delete_column` is empty or is not a real
///     column. A soft-delete table that yields `None` MUST be treated as
///     fail-closed by the caller (the plan is rejected) and must NEVER fall
///     through to a physical `DELETE` (#3) or to a read that could surface
///     tombstoned rows (#4).
///
/// `build.rs` (`security_soft_delete_enabled` + the `soft_delete_column`
/// existence check) and `generation::lint` gate this at catalog-build time; the
/// runtime re-check makes a hand-built or corrupted manifest unable to physically
/// delete a soft-delete row or leak a tombstoned one.
fn resolve_soft_delete_column(
    table: &ManifestTable,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    if !table.soft_delete {
        return None;
    }
    let column = table.soft_delete_column.trim().to_ascii_lowercase();
    if column.is_empty() {
        errors.push(format!(
            "soft-delete table {}.{} declares no soft_delete_column",
            table.schema, table.table
        ));
        return None;
    }
    if !allowed.contains(&column) {
        errors.push(format!(
            "soft-delete table {}.{} soft_delete_column '{}' is not a declared column",
            table.schema, table.table, table.soft_delete_column
        ));
        return None;
    }
    Some(column)
}

fn unresolved_tenant_column_error(table: &ManifestTable) -> String {
    format!(
        "tenant-scoped table {}.{} has no resolvable tenant column",
        table.schema, table.table
    )
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
    encrypted: &std::collections::BTreeMap<String, String>,
    errors: &mut Vec<String>,
    parameter_columns: &mut Vec<String>,
    start_param: usize,
    backend_kind: &BackendKind,
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
                        encrypted,
                        errors,
                        parameter_columns,
                        next_param,
                        joiner,
                        backend_kind,
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
                // W2: FAIL CLOSED on encrypted columns — AEAD ciphertext is
                // randomized, so this predicate silently matches nothing.
                if let Some(index_column) = encrypted.get(&normalized) {
                    if index_column.is_empty() {
                        errors.push(format!(
                            "filter on encrypted column \"{normalized}\" cannot match (AEAD \
                             ciphertext is randomized); declare a blind-index column to make \
                             this column filterable"
                        ));
                    } else {
                        errors.push(format!(
                            "filter on encrypted column \"{normalized}\" cannot match (AEAD \
                             ciphertext is randomized); filter on \"{index_column}\" (its blind \
                             index) instead"
                        ));
                    }
                    continue;
                }
                let compiled = compile_column_predicate(
                    &normalized,
                    nested,
                    errors,
                    parameter_columns,
                    next_param,
                    backend_kind,
                );
                next_param = compiled.next_param;
                if !compiled.sql.is_empty() {
                    parts.push(compiled.sql);
                }
            }
            CompiledFilter {
                sql: if parts.len() > 1 {
                    format!("({})", parts.join(" AND "))
                } else {
                    parts.join(" AND ")
                },
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
    encrypted: &std::collections::BTreeMap<String, String>,
    errors: &mut Vec<String>,
    parameter_columns: &mut Vec<String>,
    start_param: usize,
    joiner: &str,
    backend_kind: &BackendKind,
) -> CompiledFilter {
    let mut parts = Vec::new();
    let mut next_param = start_param;
    if let Value::Array(items) = value {
        for item in items {
            let compiled = compile_filter_predicates(
                item,
                allowed,
                encrypted,
                errors,
                parameter_columns,
                next_param,
                backend_kind,
            );
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
    backend_kind: &BackendKind,
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
            if matches!(sql_op, "@>" | "<@" | "?" | "@@" | "&&")
                && !matches!(backend_kind, &BackendKind::Postgres)
            {
                errors.push(format!(
                    "filter operator '{}' on column '{}' is PostgreSQL-only and cannot be used with backend '{}'",
                    sql_op,
                    column,
                    backend_kind.as_str()
                ));
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
            // `$overlaps` (`&&`) is the Postgres array-overlap operator: it needs a
            // matching array cast on the bound parameter, which the planner cannot
            // synthesize without the column's element type. The generic `col && $N`
            // fallthrough below would bind text and fail at execution
            // ("operator does not exist: <type> && text"), so reject it here with a
            // clear message rather than emitting invalid SQL.
            if sql_op == "&&" {
                errors.push(format!(
                    "$overlaps on column '{}' is not supported (array-overlap requires a typed \
                     array cast); use $contains/$contained_by (@>/<@) for JSONB columns",
                    column
                ));
                continue;
            }
            // GAP 29: Guard against leading-wildcard full-table-scans via $like/$ilike.
            // A pattern beginning with '%' or '_' forces a sequential scan on every
            // row; B-tree indexes cannot be used. Reject such patterns at the query
            // planner layer so DoS via sequential scan is not possible.
            if matches!(sql_op, "LIKE" | "ILIKE")
                && let Value::String(pattern) = op_value
            {
                let guard_pattern = unescape_like_pattern(pattern);
                if guard_pattern.starts_with('%') || guard_pattern.starts_with('_') {
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
            if matches!(sql_op, "LIKE" | "ILIKE") {
                parts.push(format!(
                    "{} {} ${} ESCAPE '\\'",
                    qi(column),
                    sql_op,
                    next_param
                ));
            } else {
                parts.push(format!("{} {} ${}", qi(column), sql_op, next_param));
            }
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

fn effective_sql_backend(context: &RequestContext) -> BackendKind {
    if context.target_backend.trim().is_empty() {
        return BackendKind::Postgres;
    }
    BackendKind::from_store_kind("sql", &context.target_backend).unwrap_or(BackendKind::Postgres)
}

fn unescape_like_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\'
            && let Some(next) = chars.next()
        {
            out.push(next);
            continue;
        }
        out.push(ch);
    }
    out
}

fn logical_value_from_json(value: &Value) -> LogicalValue {
    match value {
        Value::Null => LogicalValue::Null,
        Value::Bool(v) => LogicalValue::Bool(*v),
        Value::Number(n) => n
            .as_i64()
            .map(LogicalValue::Int)
            .or_else(|| n.as_f64().map(LogicalValue::Float))
            .unwrap_or_else(|| LogicalValue::Json(value.clone())),
        Value::String(v) => LogicalValue::String(v.clone()),
        Value::Array(values) => LogicalValue::Array(
            values
                .iter()
                .map(logical_value_from_json)
                .collect::<Vec<_>>(),
        ),
        Value::Object(_) => LogicalValue::Json(value.clone()),
    }
}

fn logical_filter_from_planner_json(
    value: &Value,
    allowed: &BTreeSet<String>,
    errors: &mut Vec<String>,
) -> Option<LogicalFilter> {
    let Value::Object(map) = value else {
        return None;
    };
    let mut clauses = Vec::new();
    for (key, nested) in map {
        let normalized = key.to_ascii_lowercase();
        if matches!(normalized.as_str(), "$raw" | "raw" | "sql" | "where_sql") {
            errors.push(format!("raw SQL filter key '{}' is not allowed", key));
            continue;
        }
        if matches!(normalized.as_str(), "$and" | "and" | "$or" | "or") {
            let Some(items) = nested.as_array() else {
                errors.push("logical filter operator requires an array".to_string());
                continue;
            };
            let mut branches = Vec::new();
            for item in items {
                if let Some(branch) = logical_filter_from_planner_json(item, allowed, errors) {
                    branches.push(branch);
                }
            }
            clauses.push(if normalized.contains("or") {
                LogicalFilter::Or(branches)
            } else {
                LogicalFilter::And(branches)
            });
            continue;
        }
        if !allowed.contains(&normalized) {
            if !is_operator(&normalized) {
                errors.push(format!("unknown filter field {}", key));
            }
            continue;
        }
        if let Some(clause) = logical_column_filter_from_json(&normalized, nested, errors) {
            clauses.push(clause);
        }
    }

    match clauses.len() {
        0 => None,
        1 => clauses.into_iter().next(),
        _ => Some(LogicalFilter::And(clauses)),
    }
}

fn logical_column_filter_from_json(
    column: &str,
    value: &Value,
    errors: &mut Vec<String>,
) -> Option<LogicalFilter> {
    let Value::Object(map) = value else {
        return Some(LogicalFilter::Comparison {
            field: column.to_string(),
            op: ComparisonOp::Eq,
            value: logical_value_from_json(value),
        });
    };

    let mut clauses = Vec::new();
    for (op, op_value) in map {
        let normalized = op.to_ascii_lowercase();
        match normalized.as_str() {
            "$eq" | "=" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Eq,
                value: logical_value_from_json(op_value),
            }),
            "$ne" | "!=" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Ne,
                value: logical_value_from_json(op_value),
            }),
            "$gt" | ">" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Gt,
                value: logical_value_from_json(op_value),
            }),
            "$gte" | ">=" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Ge,
                value: logical_value_from_json(op_value),
            }),
            "$lt" | "<" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Lt,
                value: logical_value_from_json(op_value),
            }),
            "$lte" | "<=" => clauses.push(LogicalFilter::Comparison {
                field: column.to_string(),
                op: ComparisonOp::Le,
                value: logical_value_from_json(op_value),
            }),
            "$like" | "like" | "$ilike" | "ilike" => {
                let Some(pattern) = op_value.as_str() else {
                    errors.push(format!(
                        "{} filter on {} requires a string value",
                        op, column
                    ));
                    continue;
                };
                let guard_pattern = unescape_like_pattern(pattern);
                if guard_pattern.starts_with('%') || guard_pattern.starts_with('_') {
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
                clauses.push(LogicalFilter::Comparison {
                    field: column.to_string(),
                    op: if normalized.contains("ilike") {
                        ComparisonOp::ILike
                    } else {
                        ComparisonOp::Like
                    },
                    value: LogicalValue::String(pattern.to_string()),
                });
            }
            "$in" | "in" => {
                let Some(values) = op_value.as_array() else {
                    errors.push(format!("$in filter on {} requires an array value", column));
                    continue;
                };
                clauses.push(LogicalFilter::InList {
                    field: column.to_string(),
                    values: values.iter().map(logical_value_from_json).collect(),
                });
            }
            "$is_null" | "is_null" => clauses.push(LogicalFilter::IsNull(column.to_string())),
            "$not_null" | "is_not_null" => clauses.push(LogicalFilter::Not(Box::new(
                LogicalFilter::IsNull(column.to_string()),
            ))),
            "$contains" | "contains" | "$contained_by" | "contained_by" | "$has_key"
            | "has_key" | "$overlaps" | "overlaps" | "$matches" | "matches" => {
                errors.push(format!(
                    "filter operator '{}' on column '{}' has no neutral IR equivalent yet",
                    op, column
                ));
            }
            _ => errors.push(format!("unsupported filter operator {}", op)),
        }
    }

    match clauses.len() {
        0 => None,
        1 => clauses.into_iter().next(),
        _ => Some(LogicalFilter::And(clauses)),
    }
}

// Phase I: broker.rs split into helper modules.
mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::{
        ManifestColumn, ManifestColumnSecurity, ManifestIndex, ManifestTableSecurity,
    };
    use crate::ir::compile::{
        CompileContext, CompileOperation, CompiledRendering, compile_for_backend,
    };
    use serde_json::json;

    // X-4: a tenant predicate inside `$or` must NOT satisfy isolation, because an
    // OR arm does not constrain every returned row.
    #[test]
    fn mandatory_and_columns_excludes_or_branches() {
        let allowed: BTreeSet<String> = ["tenant_id", "status", "id"]
            .into_iter()
            .map(String::from)
            .collect();

        // Top-level AND context: tenant_id is mandatory.
        let top = json!({"tenant_id": "t1", "status": "open"});
        assert!(mandatory_and_columns(&top, &allowed).contains(&"tenant_id".to_string()));

        // tenant_id ANDed with an $or group: still mandatory.
        let anded = json!({"tenant_id": "t1", "$or": [{"status": "open"}, {"id": 1}]});
        assert!(mandatory_and_columns(&anded, &allowed).contains(&"tenant_id".to_string()));

        // tenant_id buried INSIDE an $or: NOT mandatory (the leak X-4 closes).
        let in_or = json!({"$or": [{"tenant_id": "t1"}, {"status": "open"}]});
        assert!(!mandatory_and_columns(&in_or, &allowed).contains(&"tenant_id".to_string()));

        // Nested $and inside the tree keeps its columns mandatory.
        let nested_and = json!({"$and": [{"tenant_id": "t1"}, {"status": "open"}]});
        assert!(mandatory_and_columns(&nested_and, &allowed).contains(&"tenant_id".to_string()));
    }

    fn test_column(name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            sql_type: "TEXT".to_string(),
            is_primary: name == "id",
            ..ManifestColumn::default()
        }
    }

    // ── W7: partial-update planner ───────────────────────────────────────────

    fn update_test_manifest() -> CatalogManifest {
        let mut attempts = test_column("login_attempts");
        attempts.sql_type = "INTEGER".to_string();
        let table = ManifestTable {
            columns: vec![
                test_column("id"),
                test_column("tenant_id"),
                test_column("status"),
                attempts,
            ],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            ..ManifestTable::default()
        };
        test_manifest(table)
    }

    /// C22: a table declaring `required_scope` must be denied to a caller holding
    /// only the coarse verb scope, and allowed once the caller carries it.
    #[test]
    fn table_required_scope_is_enforced() {
        let mut manifest = update_test_manifest();
        manifest.tables[0].required_scope = "hr:write".to_string();
        let request = |scopes: Vec<String>| UpdatePlanRequest {
            context: RequestContext {
                tenant_id: "t1".to_string(),
                purpose: "unit-test".to_string(),
                scopes,
                ..RequestContext::default()
            },
            message_type: "acme.test.v1.Widget".to_string(),
            filter: json!({"id": "w1", "tenant_id": "t1"}),
            changes: json!({"status": "closed"}),
            increments: vec![],
            return_record: false,
        };
        let denied = build_update_plan(&manifest, &request(vec!["udb:write".to_string()]));
        assert!(
            denied
                .errors
                .iter()
                .any(|e| e.contains("scope hr:write is required")),
            "per-table scope must be required: {:?}",
            denied.errors
        );
        let allowed = build_update_plan(
            &manifest,
            &request(vec!["udb:write".to_string(), "hr:write".to_string()]),
        );
        assert!(
            !allowed.errors.iter().any(|e| e.contains("hr:write")),
            "carrying the table scope must clear the error: {:?}",
            allowed.errors
        );
    }

    #[test]
    fn update_plan_compiles_set_increment_where_with_isolation() {
        let manifest = update_test_manifest();
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                context: RequestContext {
                    tenant_id: "t1".to_string(),
                    purpose: "unit-test".to_string(),
                    scopes: vec!["udb:write".to_string()],
                    ..RequestContext::default()
                },
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"id": "w1", "tenant_id": "t1"}),
                changes: json!({"status": "closed"}),
                increments: vec![("login_attempts".to_string(), 1.0)],
                return_record: false,
            },
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        // SET params first (changes sorted by column, then increments), filter after.
        assert!(
            plan.sql.starts_with(
                "UPDATE \"public\".\"widgets\" SET \"status\" = $1, \"login_attempts\" = \"login_attempts\" + $2 WHERE "
            ),
            "unexpected sql: {}",
            plan.sql
        );
        assert_eq!(
            plan.parameter_columns,
            ["status", "login_attempts", "id", "tenant_id"]
        );
        assert!(!plan.sql.contains("RETURNING"));
        assert_eq!(plan.operation, "update");
        assert_eq!(plan.audit_event_type, "udb.sql.update");
    }

    #[test]
    fn update_plan_return_record_appends_returning() {
        let manifest = update_test_manifest();
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                context: RequestContext {
                    tenant_id: "t1".to_string(),
                    purpose: "unit-test".to_string(),
                    scopes: vec!["udb:write".to_string()],
                    ..RequestContext::default()
                },
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"id": "w1", "tenant_id": "t1"}),
                changes: json!({"status": "open"}),
                increments: Vec::new(),
                return_record: true,
            },
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        assert!(plan.sql.ends_with(" RETURNING *"), "sql: {}", plan.sql);
    }

    #[test]
    fn update_plan_fail_closed_shapes() {
        let manifest = update_test_manifest();
        let base = UpdatePlanRequest {
            context: RequestContext {
                tenant_id: "t1".to_string(),
                purpose: "unit-test".to_string(),
                scopes: vec!["udb:write".to_string()],
                ..RequestContext::default()
            },
            message_type: "acme.test.v1.Widget".to_string(),
            filter: json!({"id": "w1", "tenant_id": "t1"}),
            changes: json!({"status": "x"}),
            increments: Vec::new(),
            return_record: false,
        };

        // Identity/isolation columns are immutable through Update.
        for immutable in ["id", "tenant_id"] {
            let plan = build_update_plan(
                &manifest,
                &UpdatePlanRequest {
                    changes: json!({ immutable: "nope" }),
                    ..base.clone()
                },
            );
            assert!(
                plan.errors.iter().any(|e| e.contains("immutable")),
                "{immutable}: {:?}",
                plan.errors
            );
        }

        // A column cannot be both set and incremented.
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                changes: json!({"login_attempts": 5}),
                increments: vec![("login_attempts".to_string(), 1.0)],
                ..base.clone()
            },
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("both changes and increments")),
            "{:?}",
            plan.errors
        );

        // Unknown columns and empty updates are rejected.
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                changes: json!({"no_such_column": 1}),
                ..base.clone()
            },
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("unknown update column")),
            "{:?}",
            plan.errors
        );
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                changes: json!({}),
                ..base.clone()
            },
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("at least one change or increment")),
            "{:?}",
            plan.errors
        );

        // X-4 posture carries over: tenant inside $or does NOT satisfy isolation.
        let plan = build_update_plan(
            &manifest,
            &UpdatePlanRequest {
                filter: json!({"$or": [{"tenant_id": "t1"}, {"id": "w1"}]}),
                ..base.clone()
            },
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("tenant isolation requires filter on tenant_id")),
            "{:?}",
            plan.errors
        );
    }

    // W2 (tip 3 stage 1): equality on an encrypted column must FAIL CLOSED with
    // a typed error naming the blind-index sibling — never silently match
    // nothing. Applies at every depth of the filter tree and to every verb that
    // compiles filters (select/delete/update share the compiler).
    #[test]
    fn encrypted_filter_fails_closed_naming_blind_index() {
        let mut mobile = test_column("mobile_number");
        mobile.security = ManifestColumnSecurity {
            is_encrypted: true,
            ..ManifestColumnSecurity::default()
        };
        let mut mobile_idx = test_column("mobile_number_idx");
        mobile_idx.security = ManifestColumnSecurity {
            is_blind_index: true,
            ..ManifestColumnSecurity::default()
        };
        let mut ssn = test_column("ssn");
        ssn.security = ManifestColumnSecurity {
            is_encrypted: true,
            ..ManifestColumnSecurity::default()
        };
        let table = ManifestTable {
            columns: vec![
                test_column("id"),
                test_column("tenant_id"),
                mobile,
                mobile_idx,
                ssn,
            ],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            ..ManifestTable::default()
        };
        let manifest = test_manifest(table);
        let request = |filter: Value| DeletePlanRequest {
            context: RequestContext {
                tenant_id: "t1".to_string(),
                purpose: "unit-test".to_string(),
                scopes: vec!["udb:write".to_string()],
                ..RequestContext::default()
            },
            message_type: "acme.test.v1.Widget".to_string(),
            filter,
        };

        // Top-level predicate on the encrypted column: typed error names the idx.
        let plan = build_delete_plan(
            &manifest,
            &request(json!({"tenant_id": "t1", "mobile_number": "+15550100"})),
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("encrypted column \"mobile_number\"")
                    && e.contains("\"mobile_number_idx\"")),
            "{:?}",
            plan.errors
        );

        // Nested inside $or: still caught.
        let plan = build_delete_plan(
            &manifest,
            &request(json!({"tenant_id": "t1", "$or": [{"mobile_number": "x"}, {"id": "w1"}]})),
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("encrypted column \"mobile_number\"")),
            "{:?}",
            plan.errors
        );

        // Encrypted column WITHOUT a blind index: generic guidance.
        let plan = build_delete_plan(&manifest, &request(json!({"tenant_id": "t1", "ssn": "x"})));
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("encrypted column \"ssn\"")
                    && e.contains("declare a blind-index column")),
            "{:?}",
            plan.errors
        );

        // Filtering on the blind-index sibling itself is legal.
        let plan = build_delete_plan(
            &manifest,
            &request(json!({"tenant_id": "t1", "mobile_number_idx": "hmac-token"})),
        );
        assert!(
            !plan.errors.iter().any(|e| e.contains("encrypted column")),
            "{:?}",
            plan.errors
        );
    }

    // ── #3/#4: soft-delete planner behavior ──────────────────────────────────

    fn soft_delete_test_manifest() -> CatalogManifest {
        test_manifest(ManifestTable {
            columns: vec![
                test_column("id"),
                test_column("tenant_id"),
                test_column("status"),
                test_column("deleted_at"),
            ],
            table_security: ManifestTableSecurity {
                tenant_column: "tenant_id".to_string(),
                ..ManifestTableSecurity::default()
            },
            soft_delete: true,
            soft_delete_column: "deleted_at".to_string(),
            ..ManifestTable::default()
        })
    }

    fn soft_delete_delete_request(filter: Value) -> DeletePlanRequest {
        DeletePlanRequest {
            context: RequestContext {
                tenant_id: "tenant-a".to_string(),
                purpose: "unit-test".to_string(),
                scopes: vec!["udb:write".to_string()],
                ..RequestContext::default()
            },
            message_type: "acme.test.v1.Widget".to_string(),
            filter,
        }
    }

    // #3: a Delete on a soft-delete table becomes an idempotent tombstone UPDATE
    // that reuses the exact delete WHERE (tenant isolation + `$N` numbering) and
    // never emits a physical DELETE. The SET binds no parameter, so
    // `parameter_columns` and the verb/audit/cache contract are unchanged.
    #[test]
    fn delete_plan_soft_delete_rewrites_to_tombstone_update() {
        let manifest = soft_delete_test_manifest();
        let plan = build_delete_plan(
            &manifest,
            &soft_delete_delete_request(json!({"id": "w1", "tenant_id": "tenant-a"})),
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        assert_eq!(
            plan.sql,
            "UPDATE \"public\".\"widgets\" SET \"deleted_at\" = NOW() WHERE (\"id\" = $1 AND \"tenant_id\" = $2)"
        );
        assert!(
            !plan.sql.to_ascii_uppercase().contains("DELETE FROM"),
            "soft-delete must never physically DELETE: {}",
            plan.sql
        );
        assert_eq!(plan.parameter_columns, ["id", "tenant_id"]);
        assert_eq!(plan.operation, "delete");
        assert_eq!(plan.audit_event_type, "udb.sql.delete");
    }

    // #3: tenant isolation is still enforced on the soft-delete path, and even a
    // rejected soft-delete plan must not be a physical DELETE.
    #[test]
    fn delete_plan_soft_delete_preserves_tenant_isolation() {
        let manifest = soft_delete_test_manifest();
        let plan = build_delete_plan(&manifest, &soft_delete_delete_request(json!({"id": "w1"})));
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("tenant isolation requires filter on tenant_id")),
            "{:?}",
            plan.errors
        );
        assert!(
            !plan.sql.to_ascii_uppercase().contains("DELETE FROM"),
            "even a rejected soft-delete plan must not physically DELETE: {}",
            plan.sql
        );
    }

    // #3: malformed soft-delete metadata fails closed BEFORE mutating and never
    // degrades to a physical DELETE.
    #[test]
    fn delete_plan_soft_delete_malformed_metadata_fails_closed() {
        // Declared column absent from the table.
        let mut manifest = soft_delete_test_manifest();
        manifest.tables[0].soft_delete_column = "gone".to_string();
        let plan = build_delete_plan(
            &manifest,
            &soft_delete_delete_request(json!({"id": "w1", "tenant_id": "tenant-a"})),
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("soft_delete_column 'gone' is not a declared column")),
            "{:?}",
            plan.errors
        );
        assert!(
            !plan.sql.to_ascii_uppercase().contains("DELETE FROM"),
            "{}",
            plan.sql
        );

        // Empty column with soft_delete set.
        let mut manifest = soft_delete_test_manifest();
        manifest.tables[0].soft_delete_column = String::new();
        let plan = build_delete_plan(
            &manifest,
            &soft_delete_delete_request(json!({"id": "w1", "tenant_id": "tenant-a"})),
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("declares no soft_delete_column")),
            "{:?}",
            plan.errors
        );
        assert!(
            !plan.sql.to_ascii_uppercase().contains("DELETE FROM"),
            "{}",
            plan.sql
        );
    }

    // #3: a non-soft-delete table still emits a physical DELETE (regression).
    #[test]
    fn delete_plan_non_soft_delete_still_physical() {
        let mut manifest = soft_delete_test_manifest();
        manifest.tables[0].soft_delete = false;
        let plan = build_delete_plan(
            &manifest,
            &soft_delete_delete_request(json!({"id": "w1", "tenant_id": "tenant-a"})),
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        assert!(
            plan.sql
                .starts_with("DELETE FROM \"public\".\"widgets\" WHERE "),
            "{}",
            plan.sql
        );
    }

    // #3: the neutral-IR delete bridge declines for soft-delete tables so the
    // served path falls back to the planner's soft-delete UPDATE.
    #[test]
    fn delete_logical_delete_declines_for_soft_delete_table() {
        let manifest = soft_delete_test_manifest();
        let errors = build_delete_logical_delete(
            &manifest,
            &soft_delete_delete_request(json!({"id": "w1", "tenant_id": "tenant-a"})),
        )
        .expect_err("soft-delete table must not lower to a neutral hard delete");
        assert!(
            errors
                .iter()
                .any(|e| e.contains("does not lower to a neutral hard delete")),
            "{:?}",
            errors
        );
    }

    // #4: ordinary reads on a soft-delete table default-exclude tombstoned rows
    // via an injected `IS NULL`, composed with the caller/tenant filter. The
    // injection binds no parameter.
    #[test]
    fn select_plan_soft_delete_injects_live_row_predicate() {
        let mut manifest = soft_delete_test_manifest();
        manifest.checksum_sha256 = "sd-inject".to_string();
        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"tenant_id": "tenant-a"}),
                ..SelectPlanRequest::default()
            },
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        assert!(
            plan.sql
                .ends_with("WHERE \"tenant_id\" = $1 AND \"deleted_at\" IS NULL"),
            "{}",
            plan.sql
        );
        assert_eq!(plan.parameter_columns, ["tenant_id"]);
    }

    // #4: the injected predicate composes with a top-level $or (already
    // parenthesized) as a trailing mandatory conjunct.
    #[test]
    fn select_plan_soft_delete_composes_with_or() {
        let mut manifest = soft_delete_test_manifest();
        manifest.checksum_sha256 = "sd-or".to_string();
        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({
                    "tenant_id": "tenant-a",
                    "$or": [{"status": "open"}, {"status": "queued"}],
                }),
                ..SelectPlanRequest::default()
            },
        );
        assert_eq!(plan.errors, Vec::<String>::new());
        assert!(
            plan.sql.contains(" OR "),
            "OR group preserved: {}",
            plan.sql
        );
        assert!(
            plan.sql.ends_with("AND \"deleted_at\" IS NULL"),
            "live-row predicate is the trailing mandatory conjunct: {}",
            plan.sql
        );
    }

    // #4: a non-soft-delete read is unchanged (no injected predicate).
    #[test]
    fn select_plan_non_soft_delete_has_no_live_row_predicate() {
        let mut manifest = soft_delete_test_manifest();
        manifest.tables[0].soft_delete = false;
        manifest.checksum_sha256 = "sd-nonsoft".to_string();
        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"tenant_id": "tenant-a"}),
                ..SelectPlanRequest::default()
            },
        );
        assert!(!plan.sql.contains("IS NULL"), "{}", plan.sql);
    }

    // #4: the neutral-IR read bridge also default-excludes tombstoned rows.
    #[test]
    fn select_logical_read_soft_delete_injects_is_null() {
        let manifest = soft_delete_test_manifest();
        let read = build_select_logical_read(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"tenant_id": "tenant-a"}),
                ..SelectPlanRequest::default()
            },
        )
        .expect("soft-delete read lowers to neutral read");
        let mut fields = Vec::new();
        read.filter
            .as_ref()
            .expect("filter")
            .referenced_fields(&mut fields);
        fields.sort();
        assert_eq!(fields, vec!["deleted_at", "tenant_id"]);
    }

    // #4: malformed soft-delete metadata fails the read closed rather than
    // leaking tombstoned rows.
    #[test]
    fn select_plan_soft_delete_malformed_metadata_fails_closed() {
        let mut manifest = soft_delete_test_manifest();
        manifest.tables[0].soft_delete_column = "gone".to_string();
        manifest.checksum_sha256 = "sd-read-malformed".to_string();
        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({"tenant_id": "tenant-a"}),
                ..SelectPlanRequest::default()
            },
        );
        assert!(
            plan.errors
                .iter()
                .any(|e| e.contains("soft_delete_column 'gone' is not a declared column")),
            "{:?}",
            plan.errors
        );
    }

    fn test_manifest(mut table: ManifestTable) -> CatalogManifest {
        table.message_name = "acme.test.v1.Widget".to_string();
        table.schema = "public".to_string();
        table.table = "widgets".to_string();
        table.primary_key = vec!["id".to_string()];
        CatalogManifest {
            tables: vec![table],
            ..CatalogManifest::default()
        }
    }

    fn colliding_auth_catalog() -> CatalogManifest {
        let mut tables = Vec::new();
        for (package, schema) in [
            ("acme.authn.entity.v1", "acme_authn"),
            ("udb.core.authn.entity.v1", "udb_authn"),
        ] {
            for (message, physical) in [("OTP", "otps"), ("User", "users"), ("Session", "sessions")]
            {
                tables.push(ManifestTable {
                    message_name: message.to_string(),
                    proto_package: package.to_string(),
                    schema: schema.to_string(),
                    table: physical.to_string(),
                    primary_key: vec!["id".to_string()],
                    columns: vec![test_column("id"), test_column("status")],
                    ..ManifestTable::default()
                });
            }
        }
        CatalogManifest {
            checksum_sha256: "catalog-fqn-planner-acceptance".to_string(),
            tables,
            ..CatalogManifest::default()
        }
    }

    fn read_context() -> RequestContext {
        RequestContext {
            tenant_id: "tenant-a".to_string(),
            purpose: "test".to_string(),
            scopes: vec!["udb:read".to_string()],
            ..RequestContext::default()
        }
    }

    fn write_context() -> RequestContext {
        RequestContext {
            tenant_id: "tenant-a".to_string(),
            purpose: "test".to_string(),
            scopes: vec!["udb:write".to_string()],
            ..RequestContext::default()
        }
    }

    fn compile_pg_sql(
        manifest: &CatalogManifest,
        context: &RequestContext,
        op: CompileOperation<'_>,
    ) -> (String, Vec<LogicalValue>) {
        let compile_ctx = CompileContext::new(manifest)
            .with_tenant(&context.tenant_id)
            .with_project(&context.project_id);
        let rendering = compile_for_backend(&BackendKind::Postgres, op, &compile_ctx)
            .expect("Postgres compiler must be present in this build")
            .expect("data-plane bridge should compile for Postgres");
        match rendering {
            CompiledRendering::Sql {
                backend,
                statement,
                params,
            } => {
                assert_eq!(backend, BackendKind::Postgres);
                (statement, params)
            }
            other => panic!("expected Postgres SQL rendering, got {other:?}"),
        }
    }

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

    #[test]
    fn colliding_catalog_select_and_upsert_route_only_by_exact_fqn() {
        let manifest = colliding_auth_catalog();
        for (message_type, expected_schema) in [
            ("acme.authn.entity.v1.OTP", "acme_authn"),
            ("udb.core.authn.entity.v1.OTP", "udb_authn"),
        ] {
            let select = build_select_query_plan(
                &manifest,
                &SelectPlanRequest {
                    context: read_context(),
                    message_type: message_type.to_string(),
                    filter: json!({"status": "pending"}),
                    ..SelectPlanRequest::default()
                },
            );
            assert!(select.errors.is_empty(), "{:?}", select.errors);
            assert_eq!(select.schema, expected_schema);
            assert_eq!(select.table, "otps");

            let upsert = build_upsert_plan(
                &manifest,
                &UpsertPlanRequest {
                    context: write_context(),
                    message_type: message_type.to_string(),
                    record: json!({"id": "otp-1", "status": "pending"}),
                    ..UpsertPlanRequest::default()
                },
            );
            assert!(upsert.errors.is_empty(), "{:?}", upsert.errors);
            assert_eq!(upsert.resource_uri, format!("sql://{expected_schema}/otps"));
        }

        let ambiguous_select = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "OTP".to_string(),
                ..SelectPlanRequest::default()
            },
        );
        assert!(
            ambiguous_select
                .errors
                .iter()
                .any(|error| error.contains("ambiguous message type 'OTP'")
                    && error.contains("acme.authn.entity.v1.OTP")
                    && error.contains("udb.core.authn.entity.v1.OTP")),
            "{:?}",
            ambiguous_select.errors
        );

        let ambiguous_upsert = build_upsert_plan(
            &manifest,
            &UpsertPlanRequest {
                context: write_context(),
                message_type: "OTP".to_string(),
                record: json!({"id": "otp-1"}),
                ..UpsertPlanRequest::default()
            },
        );
        assert!(
            ambiguous_upsert
                .errors
                .iter()
                .any(|error| error.contains("ambiguous message type 'OTP'")),
            "{:?}",
            ambiguous_upsert.errors
        );
        assert!(ambiguous_upsert.resource_uri.is_empty());
    }

    #[test]
    fn planner_resolves_system_tenant_column() {
        let mut tenant = test_column("_tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            enable_rls: true,
            columns: vec![test_column("id"), tenant, test_column("status")],
            ..ManifestTable::default()
        });

        let plan = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "Widget".to_string(),
                filter: json!({"_tenant_id": "tenant-a"}),
                ..SelectPlanRequest::default()
            },
        );

        assert_eq!(plan.tenant_column, "_tenant_id");
        assert!(plan.errors.is_empty(), "{:?}", plan.errors);
    }

    #[test]
    fn planner_fails_closed_for_scoped_table_without_tenant_column() {
        let manifest = test_manifest(ManifestTable {
            enable_rls: true,
            table_security: ManifestTableSecurity {
                tenant_isolation_mode: "tenant".to_string(),
                ..ManifestTableSecurity::default()
            },
            columns: vec![test_column("id"), test_column("status")],
            ..ManifestTable::default()
        });

        let select = build_select_query_plan(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "Widget".to_string(),
                filter: json!({"status": "open"}),
                ..SelectPlanRequest::default()
            },
        );
        assert!(
            select
                .errors
                .iter()
                .any(|error| error.contains("no resolvable tenant column")),
            "{:?}",
            select.errors
        );

        let upsert = build_upsert_plan(
            &manifest,
            &UpsertPlanRequest {
                context: write_context(),
                message_type: "Widget".to_string(),
                record: json!({"id": "w1", "status": "open"}),
                ..UpsertPlanRequest::default()
            },
        );
        assert!(
            upsert
                .errors
                .iter()
                .any(|error| error.contains("no resolvable tenant column")),
            "{:?}",
            upsert.errors
        );

        let delete = build_delete_plan(
            &manifest,
            &DeletePlanRequest {
                context: write_context(),
                message_type: "Widget".to_string(),
                filter: json!({"status": "open"}),
            },
        );
        assert!(
            delete
                .errors
                .iter()
                .any(|error| error.contains("no resolvable tenant column")),
            "{:?}",
            delete.errors
        );
    }

    #[test]
    fn like_escape_clause_renders_single_backslash_character() {
        let manifest = CatalogManifest {
            tables: vec![ManifestTable {
                message_name: "Doc".to_string(),
                schema: "public".to_string(),
                table: "docs".to_string(),
                columns: vec![
                    ManifestColumn {
                        field_name: "tenant_id".to_string(),
                        column_name: "tenant_id".to_string(),
                        security: ManifestColumnSecurity::default(),
                        ..ManifestColumn::default()
                    },
                    ManifestColumn {
                        field_name: "name".to_string(),
                        column_name: "name".to_string(),
                        security: ManifestColumnSecurity::default(),
                        ..ManifestColumn::default()
                    },
                ],
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        };
        let request = SelectPlanRequest {
            context: RequestContext {
                tenant_id: "tenant-a".to_string(),
                purpose: "test".to_string(),
                scopes: vec!["udb:read".to_string()],
                ..RequestContext::default()
            },
            message_type: "Doc".to_string(),
            filter: json!({
                "tenant_id": "tenant-a",
                "name": {
                    "$like": "abc\\_%"
                }
            }),
            ..SelectPlanRequest::default()
        };

        let plan = build_select_query_plan(&manifest, &request);

        assert!(
            plan.errors.is_empty(),
            "unexpected planner errors: {:?}",
            plan.errors
        );
        assert!(
            plan.sql.contains("ESCAPE '\\'"),
            "rendered SQL should contain one backslash between quotes: {}",
            plan.sql
        );
        assert!(
            !plan.sql.contains("ESCAPE '\\\\'"),
            "rendered SQL should not contain two backslashes between quotes: {}",
            plan.sql
        );
    }

    #[test]
    fn select_planner_logical_read_preserves_wrapper_value_adds() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let mut email = test_column("email");
        email.security = ManifestColumnSecurity {
            is_pii: true,
            is_encrypted: true,
            mask_in_logs: true,
            ..ManifestColumnSecurity::default()
        };
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("status"), email],
            ..ManifestTable::default()
        });

        let read = build_select_logical_read(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({
                    "tenant_id": "tenant-a",
                    "status": {"$in": ["open", "queued"]},
                }),
                sort: vec![SortSpec {
                    field: "status".to_string(),
                    descending: true,
                }],
                limit: 25,
                ..SelectPlanRequest::default()
            },
        )
        .expect("planner request should lower to neutral read");

        assert_eq!(read.message_type, "acme.test.v1.Widget");
        assert_eq!(
            read.projection.expect("projection").fields,
            vec!["id", "tenant_id", "status"],
            "implicit data-plane reads must keep excluding PII/encrypted columns"
        );
        assert_eq!(read.sort.len(), 1);
        assert_eq!(read.sort[0].field, "status");
        assert_eq!(read.sort[0].direction, SortDirection::Desc);
        assert_eq!(read.pagination.expect("limit").limit, Some(25));
        let mut fields = Vec::new();
        read.filter
            .as_ref()
            .expect("filter")
            .referenced_fields(&mut fields);
        fields.sort();
        assert_eq!(fields, vec!["status", "tenant_id"]);
    }

    #[test]
    fn select_planner_bridge_matches_postgres_compiler_for_safe_subset() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("status")],
            ..ManifestTable::default()
        });
        let context = read_context();
        let request = SelectPlanRequest {
            context: context.clone(),
            message_type: "acme.test.v1.Widget".to_string(),
            filter: json!({
                "tenant_id": "tenant-a",
                "status": "open",
            }),
            fields: vec![
                "id".to_string(),
                "tenant_id".to_string(),
                "status".to_string(),
            ],
            sort: vec![SortSpec {
                field: "status".to_string(),
                descending: true,
            }],
            limit: 10,
        };

        let legacy_plan = build_select_query_plan(&manifest, &request);
        assert!(legacy_plan.errors.is_empty(), "{:?}", legacy_plan.errors);
        let read = build_select_logical_read(&manifest, &request)
            .expect("data-plane Select should lower to neutral read");
        let (compiled_sql, compiled_params) =
            compile_pg_sql(&manifest, &context, CompileOperation::Read(&read));

        // The neutral-IR bridge is the SERVED-DEFAULT emitter and carries F6/RLS1's
        // defense-in-depth tenant backstop (`AND "tenant_id" = $N`) that the legacy
        // fallback planner omits — it is intentionally MORE tenant-scoped, not
        // byte-identical. Assert the safe-subset relationship instead of equality:
        // the IR keeps the caller's predicates, appends exactly the tenant backstop
        // as one extra positional param, and preserves projection/order/limit.
        //
        // The ORDER between the two caller predicates (status vs tenant_id) is a
        // `serde_json` object-iteration artifact — alphabetical without the
        // `preserve_order` feature (slim builds) but insertion-order with it (the
        // full-feature build enables it transitively via cloud/otel deps). Assert
        // presence / occurrence-count / last-positional-param, never a hardcoded
        // inter-column order.
        assert!(
            compiled_sql.contains("\"status\" = $"),
            "IR keeps the caller status predicate: {compiled_sql}"
        );
        assert_eq!(
            compiled_sql.matches("\"tenant_id\" = $").count(),
            2,
            "IR keeps the caller tenant_id predicate AND appends the RLS backstop: {compiled_sql}"
        );
        assert!(
            compiled_sql.contains(&format!("AND \"tenant_id\" = ${}", compiled_params.len())),
            "IR appends the tenant backstop as the last positional param: {compiled_sql}"
        );
        assert!(
            compiled_sql.ends_with("ORDER BY \"status\" DESC LIMIT 10"),
            "IR preserves order/limit: {compiled_sql}"
        );
        let mut param_cols = legacy_plan.parameter_columns.clone();
        param_cols.sort();
        assert_eq!(
            param_cols,
            vec!["status".to_string(), "tenant_id".to_string()],
            "legacy planner filters on exactly the caller columns (inter-column order is a map-iteration artifact)"
        );
        // Caller's 2 params + the 1 tenant backstop.
        assert_eq!(
            compiled_params.len(),
            3,
            "2 caller params + tenant backstop"
        );
    }

    #[test]
    fn select_planner_logical_read_rejects_unrepresented_pg_only_ops() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("payload")],
            ..ManifestTable::default()
        });

        let errors = build_select_logical_read(
            &manifest,
            &SelectPlanRequest {
                context: read_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({
                    "tenant_id": "tenant-a",
                    "payload": {"$contains": {"kind": "invoice"}},
                }),
                ..SelectPlanRequest::default()
            },
        )
        .expect_err("jsonb containment has no neutral filter equivalent yet");

        assert!(
            errors.iter().any(|error| error.contains(
                "filter operator '$contains' on column 'payload' has no neutral IR equivalent yet"
            )),
            "{errors:?}"
        );
    }

    #[test]
    fn upsert_planner_logical_write_preserves_wrapper_value_adds() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let mut status = test_column("status");
        status.field_name = "public_status".to_string();
        let mut created_at = test_column("created_at");
        created_at.exclude_from_insert = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![
                test_column("id"),
                tenant,
                test_column("code"),
                status,
                created_at,
            ],
            indexes: vec![ManifestIndex {
                name: "uniq_widget_code".to_string(),
                columns: vec!["code".to_string()],
                unique: true,
                ..ManifestIndex::default()
            }],
            ..ManifestTable::default()
        });

        let write = build_upsert_logical_write(
            &manifest,
            &UpsertPlanRequest {
                context: write_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                record: json!({
                    "id": "w1",
                    "tenant_id": "tenant-a",
                    "code": "external-1",
                    "public_status": "open",
                    "created_at": "server-owned"
                }),
                conflict_fields: vec!["code".to_string()],
                return_record: true,
                ..UpsertPlanRequest::default()
            },
        )
        .expect("planner upsert should lower to neutral write");

        assert_eq!(write.records.len(), 1);
        let record = &write.records[0];
        assert_eq!(
            record.keys().cloned().collect::<Vec<_>>(),
            vec!["code", "id", "status", "tenant_id"],
            "server-owned columns are excluded and proto field aliases resolve to physical columns"
        );
        assert_eq!(
            write.conflict,
            ConflictStrategy::update_on(
                vec!["status".to_string(), "tenant_id".to_string()],
                vec!["code".to_string()]
            )
        );
        assert_eq!(
            write.return_fields,
            vec!["id", "tenant_id", "code", "status", "created_at"]
        );
    }

    #[test]
    fn upsert_planner_logical_write_rejects_unrepresented_alt_unique_do_nothing() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("code")],
            indexes: vec![ManifestIndex {
                name: "uniq_widget_tenant_code".to_string(),
                columns: vec!["tenant_id".to_string(), "code".to_string()],
                unique: true,
                ..ManifestIndex::default()
            }],
            ..ManifestTable::default()
        });

        let errors = build_upsert_logical_write(
            &manifest,
            &UpsertPlanRequest {
                context: write_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                record: json!({
                    "tenant_id": "tenant-a",
                    "code": "external-1"
                }),
                conflict_fields: vec!["tenant_id".to_string(), "code".to_string()],
                ..UpsertPlanRequest::default()
            },
        )
        .expect_err("alternate-unique DO NOTHING is not represented by current IR");

        assert!(
            errors.iter().any(|error| error.contains(
                "neutral IR cannot represent alternate-unique ON CONFLICT DO NOTHING yet"
            )),
            "{errors:?}"
        );
    }

    #[test]
    fn upsert_planner_bridge_matches_postgres_compiler_for_safe_subset() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("status")],
            ..ManifestTable::default()
        });
        let context = write_context();
        let request = UpsertPlanRequest {
            context: context.clone(),
            message_type: "acme.test.v1.Widget".to_string(),
            record: json!({
                "id": "w1",
                "tenant_id": "tenant-a",
                "status": "open",
            }),
            return_record: false,
            ..UpsertPlanRequest::default()
        };

        let legacy_plan = build_upsert_plan(&manifest, &request);
        assert!(legacy_plan.errors.is_empty(), "{:?}", legacy_plan.errors);
        let write = build_upsert_logical_write(&manifest, &request)
            .expect("data-plane Upsert should lower to neutral write");
        let (compiled_sql, compiled_params) =
            compile_pg_sql(&manifest, &context, CompileOperation::Write(&write));

        assert_eq!(legacy_plan.sql, compiled_sql);
        assert_eq!(
            legacy_plan.parameter_columns,
            vec![
                "id".to_string(),
                "status".to_string(),
                "tenant_id".to_string()
            ]
        );
        assert_eq!(
            compiled_params,
            vec![
                LogicalValue::String("w1".to_string()),
                LogicalValue::String("open".to_string()),
                LogicalValue::String("tenant-a".to_string())
            ]
        );
    }

    #[test]
    fn delete_planner_logical_delete_preserves_wrapper_value_adds() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("status")],
            ..ManifestTable::default()
        });

        let delete = build_delete_logical_delete(
            &manifest,
            &DeletePlanRequest {
                context: write_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({
                    "tenant_id": "tenant-a",
                    "status": {"$ne": "archived"}
                }),
            },
        )
        .expect("planner delete should lower to neutral delete");

        assert_eq!(delete.message_type, "acme.test.v1.Widget");
        let mut fields = Vec::new();
        delete.filter.referenced_fields(&mut fields);
        fields.sort();
        assert_eq!(fields, vec!["status", "tenant_id"]);
    }

    #[test]
    fn delete_planner_logical_delete_rejects_unrepresented_pg_only_ops() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("payload")],
            ..ManifestTable::default()
        });

        let errors = build_delete_logical_delete(
            &manifest,
            &DeletePlanRequest {
                context: write_context(),
                message_type: "acme.test.v1.Widget".to_string(),
                filter: json!({
                    "tenant_id": "tenant-a",
                    "payload": {"$contains": {"kind": "invoice"}}
                }),
            },
        )
        .expect_err("jsonb containment has no neutral filter equivalent yet");

        assert!(
            errors.iter().any(|error| error.contains(
                "filter operator '$contains' on column 'payload' has no neutral IR equivalent yet"
            )),
            "{errors:?}"
        );
    }

    #[test]
    fn delete_planner_bridge_matches_postgres_compiler_for_safe_subset() {
        let mut tenant = test_column("tenant_id");
        tenant.is_tenant_column = true;
        let manifest = test_manifest(ManifestTable {
            columns: vec![test_column("id"), tenant, test_column("status")],
            ..ManifestTable::default()
        });
        let context = write_context();
        let request = DeletePlanRequest {
            context: context.clone(),
            message_type: "acme.test.v1.Widget".to_string(),
            filter: json!({
                "tenant_id": "tenant-a",
                "status": "archived",
            }),
        };

        let legacy_plan = build_delete_plan(&manifest, &request);
        assert!(legacy_plan.errors.is_empty(), "{:?}", legacy_plan.errors);
        let delete = build_delete_logical_delete(&manifest, &request)
            .expect("data-plane Delete should lower to neutral delete");
        let (compiled_sql, compiled_params) =
            compile_pg_sql(&manifest, &context, CompileOperation::Delete(&delete));

        // Same safe-subset relationship as the select bridge test: the neutral-IR
        // DELETE (served default) carries RLS1's defense-in-depth tenant backstop
        // (`AND "tenant_id" = $N`) the legacy planner omits — intentionally MORE
        // tenant-scoped, not byte-identical. (`build_delete_logical_delete` also
        // declines for soft-delete tables, but `widgets` here is non-soft, so it
        // is a real physical DELETE, exercising the backstop on the delete path.)
        // The order between the two caller predicates is a `serde_json`
        // object-iteration artifact (see the select bridge test) — assert
        // presence / occurrence-count / last-positional-param, never a hardcoded
        // inter-column order.
        assert!(
            compiled_sql.starts_with("DELETE FROM \"public\".\"widgets\" WHERE "),
            "IR emits a physical DELETE for a non-soft table: {compiled_sql}"
        );
        assert!(
            compiled_sql.contains("\"status\" = $"),
            "IR keeps the caller status predicate: {compiled_sql}"
        );
        assert_eq!(
            compiled_sql.matches("\"tenant_id\" = $").count(),
            2,
            "IR keeps the caller tenant_id predicate AND appends the RLS backstop: {compiled_sql}"
        );
        assert!(
            compiled_sql.contains(&format!("AND \"tenant_id\" = ${}", compiled_params.len())),
            "IR appends the tenant backstop as the last positional param: {compiled_sql}"
        );
        let mut param_cols = legacy_plan.parameter_columns.clone();
        param_cols.sort();
        assert_eq!(
            param_cols,
            vec!["status".to_string(), "tenant_id".to_string()],
            "legacy planner filters on exactly the caller columns (inter-column order is a map-iteration artifact)"
        );
        assert_eq!(
            compiled_params.len(),
            3,
            "2 caller params + tenant backstop"
        );
    }
}
