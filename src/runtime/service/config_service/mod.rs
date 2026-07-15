//! Native `ConfigService` (master-plan 9.8) — feature flags and runtime config.
//!
//! Mirrors `lock_service`/`tenant_service`: proto-driven, no in-memory store, no
//! hand-mapped schema. Flags are durable rows in the canonical store, scoped to
//! (tenant, project, environment) and isolated by RLS; the tenant is always taken
//! from the VERIFIED claim, never the request body.
//!
//! The heart of the service is a PURE evaluation function ([`evaluate_flag`] +
//! [`resolve_flag`]) with NO I/O: the identical algorithm ships in the SDK so a
//! client-side cache and the server compute byte-identical results. Scope
//! precedence is deterministic (environment > project > tenant-default) and
//! percentage rollout uses a STABLE FNV-1a hash of `flag_key + ":" + subject`, so
//! a given subject is sticky across nodes and releases. The unit-test fixtures
//! below are the SDK<->server contract (they should ship via sdk-conformance).
//!
//! Doctrine (Phase 9): claim-derived tenant, durable state, a monotone per-row
//! `revision` bumped on every change, a `udb.config.flag.changed.v1` outbox event
//! per mutation, and a resolve-once (OnceLock) evaluation TTL — never a
//! per-request env read.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use sqlx::PgPool;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalDelete, LogicalFilter, LogicalPagination,
    LogicalProjection, LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::config::services::v1 as config_pb;
use crate::proto::udb::core::config::services::v1::config_service_server::ConfigService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};

pub use crate::proto::udb::core::config::services::v1::config_service_server::ConfigServiceServer;

use super::DataBrokerService;
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_next_page_token, native_offset_page_window, native_service_context, non_empty_json,
    validate_request_tenant,
};

const CONFIG_MSG: &str = "udb.core.config.entity.v1.Flag";
const TOPIC_FLAG_CHANGED: &str = "udb.config.flag.changed.v1";

const VALUE_TYPE_BOOL: &str = "BOOL";
const VALUE_TYPE_STRING: &str = "STRING";
const VALUE_TYPE_NUMBER: &str = "NUMBER";
const VALUE_TYPE_JSON: &str = "JSON";

/// Default server-authoritative evaluation cache TTL (seconds). Overridable once
/// via `UDB_CONFIG_EVAL_TTL_SECONDS`, resolved through a `OnceLock` — never read
/// per request.
const DEFAULT_EVAL_TTL_SECONDS: i64 = 30;
/// Cap on scope rows scanned per key during evaluation (env/project/tenant
/// arms). The batched `EvaluateFlags` read multiplies this by the key count for
/// its overall row cap (see [`flag_candidates_batch_read`]).
const MAX_FLAGS_PER_KEY_SCAN: u32 = 64;
/// List defaults/caps so one tenant cannot scan an unbounded table.
const DEFAULT_LIST_LIMIT: u32 = 100;
const MAX_LIST_LIMIT: u32 = 500;
/// Cap on keys evaluated in a single EvaluateFlags call.
const MAX_EVALUATE_KEYS: usize = 256;

/// Resolve the evaluation TTL exactly once (no per-request env reads).
fn eval_ttl_seconds() -> i64 {
    static TTL: OnceLock<i64> = OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("UDB_CONFIG_EVAL_TTL_SECONDS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_EVAL_TTL_SECONDS)
    })
}

// ---------------------------------------------------------------------------
// PURE evaluation core — NO I/O. The SDK ships the identical algorithm so client
// and server agree bit-for-bit. Everything below is unit-tested with fixed
// inputs -> fixed outputs.
// ---------------------------------------------------------------------------

/// A typed flag value (the oneof arm), independent of proto/storage encodings.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FlagVal {
    Bool(bool),
    Number(f64),
    Str(String),
    Json(String),
}

impl FlagVal {
    /// The value returned when a flag is disabled or a subject falls outside the
    /// rollout bucket: the type's neutral "off"/control value. Deterministic.
    fn off_value(&self) -> FlagVal {
        match self {
            FlagVal::Bool(_) => FlagVal::Bool(false),
            FlagVal::Number(_) => FlagVal::Number(0.0),
            FlagVal::Str(_) => FlagVal::Str(String::new()),
            FlagVal::Json(_) => FlagVal::Json("null".to_string()),
        }
    }
}

/// A flag definition reduced to exactly what evaluation needs (no DB metadata).
#[derive(Clone, Debug)]
pub(crate) struct EvalFlag {
    pub flag_id: String,
    pub flag_key: String,
    pub project_id: String,
    pub environment: String,
    pub value: FlagVal,
    pub enabled: bool,
    pub rollout_percentage: i32,
    pub rollout_context_key: String,
    pub revision: i64,
}

/// The evaluation context: which scope to resolve against and the subject
/// attributes the rollout hash keys on.
#[derive(Clone, Debug, Default)]
pub(crate) struct EvalContext {
    pub project_id: String,
    pub environment: String,
    pub attributes: HashMap<String, String>,
}

/// FNV-1a 64-bit. A tiny, dependency-free, fully-specified hash so the SDK can
/// reproduce the exact bucket in any language.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Stable rollout bucket in `0..100` for `(flag_key, subject)`. Same inputs always
/// produce the same bucket, on every node and every release.
fn rollout_bucket(flag_key: &str, subject: &str) -> i32 {
    let combined = format!("{flag_key}:{subject}");
    (fnv1a64(combined.as_bytes()) % 100) as i32
}

/// Specificity rank of a flag's scope against the context, or `-1` if the flag is
/// not a candidate (its non-empty project/environment does not match). Precedence:
/// environment (2) + project (1); a tenant-default row (both empty) ranks 0.
fn scope_rank(flag: &EvalFlag, ctx: &EvalContext) -> i32 {
    let env_ok = flag.environment.is_empty() || flag.environment == ctx.environment;
    let proj_ok = flag.project_id.is_empty() || flag.project_id == ctx.project_id;
    if !env_ok || !proj_ok {
        return -1;
    }
    let mut rank = 0;
    if !flag.environment.is_empty() {
        rank += 2;
    }
    if !flag.project_id.is_empty() {
        rank += 1;
    }
    rank
}

/// Pick the most specific matching flag among same-key candidates at different
/// scopes (environment > project > tenant-default). Ties (impossible under the
/// unique scope index, but defended anyway) break deterministically by flag_id.
/// PURE — the SDK resolves identically.
pub(crate) fn resolve_flag<'a>(
    candidates: &'a [EvalFlag],
    ctx: &EvalContext,
) -> Option<&'a EvalFlag> {
    candidates
        .iter()
        .filter_map(|f| {
            let rank = scope_rank(f, ctx);
            (rank >= 0).then_some((rank, f))
        })
        .max_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.flag_id.cmp(&b.1.flag_id)))
        .map(|(_, f)| f)
}

/// Evaluate a single resolved flag to its typed value. PURE and deterministic:
/// disabled -> off; `pct >= 100` -> on; `pct <= 0` -> off; otherwise the stable
/// `(flag_key, subject)` bucket gates against `rollout_percentage`. The SDK ships
/// this identical algorithm.
pub(crate) fn evaluate_flag(flag: &EvalFlag, ctx: &EvalContext) -> FlagVal {
    if !flag.enabled {
        return flag.value.off_value();
    }
    let pct = flag.rollout_percentage.clamp(0, 100);
    if pct >= 100 {
        return flag.value.clone();
    }
    if pct <= 0 {
        return flag.value.off_value();
    }
    let subject = ctx
        .attributes
        .get(&flag.rollout_context_key)
        .map(String::as_str)
        .unwrap_or("");
    if rollout_bucket(&flag.flag_key, subject) < pct {
        flag.value.clone()
    } else {
        flag.value.off_value()
    }
}

/// The config-domain revision bump: monotone, saturating. PURE so the "a change
/// bumps the revision" invariant is unit-testable without Postgres.
fn bump_revision(current: i64) -> i64 {
    current.saturating_add(1)
}

// ---------------------------------------------------------------------------
// Value <-> storage/proto encodings.
// ---------------------------------------------------------------------------

fn number_to_json(n: f64) -> String {
    serde_json::Number::from_f64(n)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
        .to_string()
}

/// Encode a typed value to its `(value_type, value_json)` storage pair.
fn flag_val_to_stored(value: &FlagVal) -> (String, String) {
    match value {
        FlagVal::Bool(b) => (
            VALUE_TYPE_BOOL.to_string(),
            serde_json::Value::Bool(*b).to_string(),
        ),
        FlagVal::Number(n) => (VALUE_TYPE_NUMBER.to_string(), number_to_json(*n)),
        FlagVal::Str(s) => (
            VALUE_TYPE_STRING.to_string(),
            serde_json::Value::String(s.clone()).to_string(),
        ),
        FlagVal::Json(j) => (VALUE_TYPE_JSON.to_string(), j.clone()),
    }
}

/// Decode a stored `(value_type, value_json)` pair back to a typed value.
fn stored_to_flag_val(value_type: &str, value_json: &str) -> FlagVal {
    let parsed: serde_json::Value =
        serde_json::from_str(value_json).unwrap_or(serde_json::Value::Null);
    match value_type {
        VALUE_TYPE_NUMBER => FlagVal::Number(parsed.as_f64().unwrap_or(0.0)),
        VALUE_TYPE_STRING => FlagVal::Str(parsed.as_str().unwrap_or("").to_string()),
        VALUE_TYPE_JSON => FlagVal::Json(parsed.to_string()),
        // BOOL is the default/fallback type.
        _ => FlagVal::Bool(parsed.as_bool().unwrap_or(false)),
    }
}

fn require_flag_key(flag_key: &str) -> Result<String, Status> {
    let flag_key = flag_key.trim();
    if flag_key.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "flag_key is required",
            [("flag_key", "must be a non-empty flag key")],
        ));
    }
    Ok(flag_key.to_string())
}

fn ensure_evaluate_key_limit(keys_len: usize) -> Result<(), Status> {
    if keys_len > MAX_EVALUATE_KEYS {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            format!("too many keys (max {MAX_EVALUATE_KEYS})"),
            [("keys", "must contain at most 256 keys")],
        ));
    }
    Ok(())
}

/// Convert the request oneof into a typed value, validating JSON arms.
fn proto_to_flag_val(value: &Option<config_pb::FlagValue>) -> Result<FlagVal, Status> {
    use config_pb::flag_value::Value;
    let inner = value
        .as_ref()
        .and_then(|fv| fv.value.as_ref())
        .ok_or_else(|| {
            crate::runtime::executor_utils::invalid_argument_fields(
                "value is required",
                [("value", "must set one FlagValue arm")],
            )
        })?;
    Ok(match inner {
        Value::BoolValue(b) => FlagVal::Bool(*b),
        Value::NumberValue(n) => FlagVal::Number(*n),
        Value::StringValue(s) => FlagVal::Str(s.clone()),
        Value::JsonValue(j) => {
            let text = if j.trim().is_empty() {
                "null"
            } else {
                j.as_str()
            };
            let parsed: serde_json::Value = serde_json::from_str(text).map_err(|e| {
                crate::runtime::executor_utils::invalid_argument_fields(
                    format!("json_value is not valid JSON: {e}"),
                    [("value.json_value", "must be valid JSON")],
                )
            })?;
            FlagVal::Json(parsed.to_string())
        }
    })
}

fn flag_val_to_proto(value: &FlagVal) -> config_pb::FlagValue {
    use config_pb::flag_value::Value;
    config_pb::FlagValue {
        value: Some(match value {
            FlagVal::Bool(b) => Value::BoolValue(*b),
            FlagVal::Number(n) => Value::NumberValue(*n),
            FlagVal::Str(s) => Value::StringValue(s.clone()),
            FlagVal::Json(j) => Value::JsonValue(j.clone()),
        }),
    }
}

// ---------------------------------------------------------------------------
// IR builders (mirror lock_service): tenant-scoped reads/writes via the neutral
// compiler, never raw SQL.
// ---------------------------------------------------------------------------

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn eq(field: &str, value: &str) -> LogicalFilter {
    LogicalFilter::Comparison {
        field: field.to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(value),
    }
}

fn flag_filter(
    tenant_id: &str,
    project_id: Option<&str>,
    environment: Option<&str>,
    flag_key: Option<&str>,
) -> LogicalFilter {
    let mut filters = vec![eq("tenant_id", tenant_id)];
    if let Some(project_id) = project_id {
        filters.push(eq("project_id", project_id));
    }
    if let Some(environment) = environment {
        filters.push(eq("environment", environment));
    }
    if let Some(flag_key) = flag_key {
        filters.push(eq("flag_key", flag_key));
    }
    LogicalFilter::And(filters)
}

fn flag_projection() -> LogicalProjection {
    LogicalProjection::fields([
        "flag_id".to_string(),
        "flag_key".to_string(),
        "project_id".to_string(),
        "environment".to_string(),
        "value_type".to_string(),
        "value_json".to_string(),
        "enabled".to_string(),
        "rollout_percentage".to_string(),
        "rollout_context_key".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}

fn flag_read_exact(
    tenant_id: &str,
    project_id: &str,
    environment: &str,
    flag_key: &str,
) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(flag_filter(
            tenant_id,
            Some(project_id),
            Some(environment),
            Some(flag_key),
        )),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

/// ONE tenant-scoped candidate read for ALL evaluated keys (the batched
/// `EvaluateFlags` path — replaces the previous read-per-key loop):
/// `tenant_id = ? AND flag_key IN (keys)`. The row cap is
/// `keys.len() × MAX_FLAGS_PER_KEY_SCAN`: each key still has at most
/// [`MAX_FLAGS_PER_KEY_SCAN`] scope rows (env/project/tenant arms, one row per
/// scope under the unique scope index), so the batched read carries the same
/// overall bound the per-key loop enforced — collapsed into a single mediated
/// read. Combined with [`MAX_EVALUATE_KEYS`] the absolute ceiling is
/// 256 × 64 = 16 384 rows.
fn flag_candidates_batch_read(tenant_id: &str, flag_keys: &[String]) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            eq("tenant_id", tenant_id),
            LogicalFilter::InList {
                field: "flag_key".to_string(),
                values: flag_keys
                    .iter()
                    .map(|key| logical_string(key.as_str()))
                    .collect(),
            },
        ])),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(
            (flag_keys.len() as u32).saturating_mul(MAX_FLAGS_PER_KEY_SCAN),
        )),
    }
}

fn flag_list_read(
    tenant_id: &str,
    project_id: Option<&str>,
    environment: Option<&str>,
    offset: u64,
    limit: u32,
) -> LogicalRead {
    LogicalRead {
        message_type: CONFIG_MSG.to_string(),
        filter: Some(flag_filter(tenant_id, project_id, environment, None)),
        projection: Some(flag_projection()),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::page(offset, limit)),
    }
}

#[allow(clippy::too_many_arguments)]
fn flag_record(
    flag_id: &str,
    tenant_id: &str,
    project_id: &str,
    environment: &str,
    flag_key: &str,
    value_type: &str,
    value_json: &str,
    enabled: bool,
    rollout_percentage: i32,
    rollout_context_key: &str,
    revision: i64,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("flag_id".to_string(), logical_string(flag_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("project_id".to_string(), logical_string(project_id));
    record.insert("environment".to_string(), logical_string(environment));
    record.insert("flag_key".to_string(), logical_string(flag_key));
    record.insert("value_type".to_string(), logical_string(value_type));
    record.insert("value_json".to_string(), logical_string(value_json));
    record.insert("enabled".to_string(), LogicalValue::Bool(enabled));
    record.insert(
        "rollout_percentage".to_string(),
        LogicalValue::Int(i64::from(rollout_percentage)),
    );
    record.insert(
        "rollout_context_key".to_string(),
        logical_string(rollout_context_key),
    );
    record.insert("revision".to_string(), LogicalValue::Int(revision));
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// Mutable columns an upsert may overwrite for an existing (tenant, project, env,
/// key) row. The conflict target is the `flag_id` PK (reused for the existing
/// scope), so a re-put never violates the unique scope index.
fn flag_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "value_type".to_string(),
        "value_json".to_string(),
        "enabled".to_string(),
        "rollout_percentage".to_string(),
        "rollout_context_key".to_string(),
        "revision".to_string(),
        "metadata_json".to_string(),
    ])
}

// ---------------------------------------------------------------------------
// JSON row decoding.
// ---------------------------------------------------------------------------

fn flag_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: OnceLock<serde_json::Map<String, serde_json::Value>> = OnceLock::new();
            EMPTY.get_or_init(serde_json::Map::new)
        })
}

fn json_str(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(serde_json::Value::Number(value)) => value.to_string(),
        Some(serde_json::Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

/// Read a JSONB column either as the text the driver returned, or as a re-encoded
/// nested JSON value (some drivers hand back the parsed value, not a string).
fn json_value_text(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    match row.get(key) {
        Some(serde_json::Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
        None => String::new(),
    }
}

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn json_bool(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    match row.get(key) {
        Some(serde_json::Value::Bool(value)) => *value,
        Some(serde_json::Value::String(value)) => {
            value.eq_ignore_ascii_case("true") || value.trim() == "1"
        }
        Some(serde_json::Value::Number(value)) => value.as_i64().map(|v| v != 0).unwrap_or(false),
        _ => false,
    }
}

fn eval_flag_from_json(row: &serde_json::Value) -> EvalFlag {
    let map = flag_json_object(row);
    let value_type = json_str(map, "value_type");
    let value_json = json_value_text(map, "value_json");
    EvalFlag {
        flag_id: json_str(map, "flag_id"),
        flag_key: json_str(map, "flag_key"),
        project_id: json_str(map, "project_id"),
        environment: json_str(map, "environment"),
        value: stored_to_flag_val(&value_type, &value_json),
        enabled: json_bool(map, "enabled"),
        rollout_percentage: json_i64(map, "rollout_percentage") as i32,
        rollout_context_key: json_str(map, "rollout_context_key"),
        revision: json_i64(map, "revision"),
    }
}

fn flag_state_from_json(row: &serde_json::Value, tenant_id: &str) -> config_pb::FlagState {
    let ef = eval_flag_from_json(row);
    let map = flag_json_object(row);
    config_pb::FlagState {
        tenant_id: tenant_id.to_string(),
        project_id: ef.project_id.clone(),
        environment: ef.environment.clone(),
        flag_key: ef.flag_key.clone(),
        value: Some(flag_val_to_proto(&ef.value)),
        enabled: ef.enabled,
        rollout_percentage: ef.rollout_percentage,
        rollout_context_key: ef.rollout_context_key.clone(),
        revision: ef.revision,
        metadata_json: json_value_text(map, "metadata_json"),
    }
}

/// The acting principal for the change event, from the verified claim (falls back
/// to `system` when no claim context is present, e.g. internal callers).
fn event_actor() -> String {
    let subject = crate::runtime::service::method_security::current_claim_context()
        .subject
        .trim()
        .to_string();
    if subject.is_empty() {
        "system".to_string()
    } else {
        subject
    }
}

/// Postgres-backed `ConfigService` handler.
pub struct ConfigServiceImpl {
    pg_pool: Option<PgPool>,
    runtime: Option<Arc<DataBrokerRuntime>>,
    outbox_relation: Option<String>,
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn config_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "config",
        operation,
        capability_required,
        message,
    )
}

impl ConfigServiceImpl {
    pub fn new() -> Self {
        Self {
            pg_pool: None,
            runtime: None,
            outbox_relation: None,
            channels: None,
            metrics: Arc::new(NoopMetrics),
        }
    }

    pub fn with_postgres(mut self, pool: Option<PgPool>) -> Self {
        self.pg_pool = pool;
        self
    }

    pub(crate) fn with_runtime(mut self, runtime: Option<Arc<DataBrokerRuntime>>) -> Self {
        self.runtime = runtime;
        self
    }

    pub(crate) fn with_outbox(mut self, relation: Option<String>) -> Self {
        self.outbox_relation = relation;
        self
    }

    pub(crate) fn with_channels(mut self, channels: Option<ChannelManager>) -> Self {
        self.channels = channels;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<dyn MetricsRecorder>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Flag state is durable-only: fail closed when no canonical/PG store exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            config_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "config service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }

    /// Emit the per-mutation `udb.config.flag.changed.v1` event `{key, actor,
    /// revision}` (best-effort; the durable write already succeeded).
    async fn emit_flag_changed(
        &self,
        tenant_id: &str,
        project_id: &str,
        flag_key: &str,
        actor: &str,
        revision: i64,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            return;
        };
        let payload = serde_json::json!({
            "key": flag_key,
            "actor": actor,
            "revision": revision,
            "tenant_id": tenant_id,
            "project_id": project_id,
        });
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            TOPIC_FLAG_CHANGED,
            flag_key,
            tenant_id,
            project_id,
            payload,
            NativeEventContext {
                actor: actor.to_string(),
                target_resource: flag_key.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

impl Default for ConfigServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl ConfigService for ConfigServiceImpl {
    async fn put_flag(
        &self,
        request: Request<config_pb::PutFlagRequest>,
    ) -> Result<Response<config_pb::PutFlagResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes, the body value IS the verified tenant.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let environment = req.environment.trim().to_string();
        let flag_key = require_flag_key(&req.flag_key)?;
        let value = proto_to_flag_val(&req.value)?;
        let (value_type, value_json) = flag_val_to_stored(&value);
        let rollout_percentage = req.rollout_percentage.clamp(0, 100);
        let rollout_context_key = req.rollout_context_key.trim().to_string();
        let metadata_json = non_empty_json(&req.metadata_json);

        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "config",
            OperationChannel::Write,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        // Existing row at this exact scope (for stable flag_id + revision bump).
        let existing = runtime
            .native_entity_read_for_service(
                "config",
                &context,
                flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
            )
            .await?
            .first()
            .map(eval_flag_from_json);

        let flag_id = existing
            .as_ref()
            .map(|f| f.flag_id.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let revision = bump_revision(existing.as_ref().map(|f| f.revision).unwrap_or(0));

        runtime
            .native_entity_write_for_service(
                "config",
                &context,
                CONFIG_MSG,
                flag_record(
                    &flag_id,
                    &tenant_id,
                    &project_id,
                    &environment,
                    &flag_key,
                    &value_type,
                    &value_json,
                    req.enabled,
                    rollout_percentage,
                    &rollout_context_key,
                    revision,
                    &metadata_json,
                ),
                flag_conflict(),
            )
            .await?;

        let actor = event_actor();
        self.emit_flag_changed(&tenant_id, &project_id, &flag_key, &actor, revision)
            .await;

        Ok(Response::new(config_pb::PutFlagResponse {
            stored: true,
            flag_key,
            revision,
            message: "flag stored".to_string(),
            error: None,
        }))
    }

    async fn get_flag(
        &self,
        request: Request<config_pb::GetFlagRequest>,
    ) -> Result<Response<config_pb::GetFlagResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let environment = req.environment.trim().to_string();
        let flag_key = require_flag_key(&req.flag_key)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "config",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        let found = runtime
            .native_entity_read_for_service(
                "config",
                &context,
                flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
            )
            .await?
            .first()
            .map(|row| flag_state_from_json(row, &tenant_id));

        Ok(Response::new(config_pb::GetFlagResponse {
            found: found.is_some(),
            flag: found,
            message: String::new(),
            error: None,
        }))
    }

    async fn list_flags(
        &self,
        request: Request<config_pb::ListFlagsRequest>,
    ) -> Result<Response<config_pb::ListFlagsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let environment = req.environment.trim().to_string();
        let legacy_limit = if req.limit == 0 {
            DEFAULT_LIST_LIMIT
        } else {
            req.limit.min(MAX_LIST_LIMIT)
        };
        let requested_page_size = if req.page_size > 0 {
            req.page_size
        } else {
            legacy_limit as i32
        };
        let page_window = native_offset_page_window(
            1,
            requested_page_size,
            &req.page_token,
            DEFAULT_LIST_LIMIT as i32,
        );
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "config",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        let project_filter = (!project_id.is_empty()).then_some(project_id.as_str());
        let env_filter = (!environment.is_empty()).then_some(environment.as_str());
        let flags = runtime
            .native_entity_read_for_service(
                "config",
                &context,
                flag_list_read(
                    &tenant_id,
                    project_filter,
                    env_filter,
                    page_window.offset as u64,
                    (page_window.limit as u32).min(MAX_LIST_LIMIT),
                ),
            )
            .await?
            .iter()
            .map(|row| flag_state_from_json(row, &tenant_id))
            .collect::<Vec<_>>();
        let next_page_token =
            native_next_page_token(page_window.offset, page_window.limit, flags.len());

        Ok(Response::new(config_pb::ListFlagsResponse {
            flags,
            message: String::new(),
            error: None,
            next_page_token,
        }))
    }

    async fn delete_flag(
        &self,
        request: Request<config_pb::DeleteFlagRequest>,
    ) -> Result<Response<config_pb::DeleteFlagResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let project_id = req.project_id.trim().to_string();
        let environment = req.environment.trim().to_string();
        let flag_key = require_flag_key(&req.flag_key)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "config",
            OperationChannel::Write,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &project_id);

        let existing = runtime
            .native_entity_read_for_service(
                "config",
                &context,
                flag_read_exact(&tenant_id, &project_id, &environment, &flag_key),
            )
            .await?
            .first()
            .map(eval_flag_from_json);

        let Some(existing) = existing else {
            // Idempotent: nothing to delete.
            return Ok(Response::new(config_pb::DeleteFlagResponse {
                deleted: true,
                revision: 0,
                message: "flag not found".to_string(),
                error: None,
            }));
        };

        runtime
            .native_entity_delete_for_service(
                "config",
                &context,
                LogicalDelete {
                    message_type: CONFIG_MSG.to_string(),
                    filter: flag_filter(
                        &tenant_id,
                        Some(&project_id),
                        Some(&environment),
                        Some(&flag_key),
                    ),
                    return_fields: Vec::new(),
                },
            )
            .await?;

        // A delete is a config change: bump the revision in the emitted event.
        let revision = bump_revision(existing.revision);
        let actor = event_actor();
        self.emit_flag_changed(&tenant_id, &project_id, &flag_key, &actor, revision)
            .await;

        Ok(Response::new(config_pb::DeleteFlagResponse {
            deleted: true,
            revision,
            message: "flag deleted".to_string(),
            error: None,
        }))
    }

    async fn evaluate_flags(
        &self,
        request: Request<config_pb::EvaluateFlagsRequest>,
    ) -> Result<Response<config_pb::EvaluateFlagsResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        ensure_evaluate_key_limit(req.keys.len())?;
        let ctx_pb = req.context.unwrap_or_default();
        let eval_ctx = EvalContext {
            project_id: ctx_pb.project_id.trim().to_string(),
            environment: ctx_pb.environment.trim().to_string(),
            attributes: ctx_pb.attributes,
        };
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "config",
            OperationChannel::Read,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, &eval_ctx.project_id);

        // Trimmed, non-empty, de-duplicated keys — preserves the previous
        // per-key loop's semantics (blank keys skipped; a duplicate key would
        // evaluate identically, so it is read and resolved once). The key count
        // is already gated by `ensure_evaluate_key_limit` above.
        let mut keys: Vec<String> = Vec::with_capacity(req.keys.len());
        for key in &req.keys {
            let key = key.trim();
            if key.is_empty() || keys.iter().any(|seen| seen == key) {
                continue;
            }
            keys.push(key.to_string());
        }

        let mut values: HashMap<String, config_pb::FlagValue> = HashMap::new();
        let mut config_revision: i64 = 0;
        if !keys.is_empty() {
            // ONE tenant-scoped mediated read for every requested key (was one
            // read per key — up to MAX_EVALUATE_KEYS sequential round-trips).
            let rows = runtime
                .native_entity_read_for_service(
                    "config",
                    &context,
                    flag_candidates_batch_read(&tenant_id, &keys),
                )
                .await?;
            // Bucket candidates per key, then resolve in memory through the
            // same PURE core (`resolve_flag` scope precedence + `evaluate_flag`
            // rollout hashing) the per-key loop used — semantics unchanged.
            let mut candidates_by_key: HashMap<String, Vec<EvalFlag>> =
                HashMap::with_capacity(keys.len());
            for row in &rows {
                let flag = eval_flag_from_json(row);
                candidates_by_key
                    .entry(flag.flag_key.clone())
                    .or_default()
                    .push(flag);
            }
            for key in &keys {
                let Some(candidates) = candidates_by_key.get(key.as_str()) else {
                    continue;
                };
                if let Some(flag) = resolve_flag(candidates, &eval_ctx) {
                    config_revision = config_revision.max(flag.revision);
                    let resolved = evaluate_flag(flag, &eval_ctx);
                    values.insert(key.clone(), flag_val_to_proto(&resolved));
                }
            }
        }

        Ok(Response::new(config_pb::EvaluateFlagsResponse {
            values,
            server_ttl_seconds: eval_ttl_seconds(),
            config_revision,
            message: String::new(),
            error: None,
        }))
    }
}

impl DataBrokerService {
    /// Build the native `ConfigService`, wired to the broker's Postgres pool, the
    /// native-entity dispatch runtime, and the shared outbox. Served in `serve()`
    /// via `add_service(ConfigServiceServer::new(...))`.
    pub(crate) fn build_config_service(&self) -> ConfigServiceImpl {
        let runtime = self.runtime.load_full();
        let pg_pool = runtime
            .native_store_pool_for_service("config", true, "")
            .ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        ConfigServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}

#[cfg(test)]
mod config_eval_tests {
    use super::*;
    use crate::proto::{ErrorDetail, ErrorKind};
    use crate::runtime::executor_utils::ERROR_DETAIL_METADATA_KEY;
    use tonic::metadata::MetadataValue;

    fn decode_detail(status: &Status) -> ErrorDetail {
        let raw = status
            .metadata()
            .get_bin(ERROR_DETAIL_METADATA_KEY)
            .expect("error-detail trailer present")
            .to_bytes()
            .expect("trailer decodes to bytes");
        crate::runtime::executor_utils::decode_error_detail_from_raw(&raw)
    }

    fn assert_one_validation_field(status: &Status, field: &str, description: &str) {
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 1);
        assert_eq!(detail.field_violations[0].field, field);
        assert_eq!(detail.field_violations[0].description, description);
    }

    fn flag(
        flag_id: &str,
        project: &str,
        env: &str,
        value: FlagVal,
        enabled: bool,
        pct: i32,
        ctx_key: &str,
        revision: i64,
    ) -> EvalFlag {
        EvalFlag {
            flag_id: flag_id.to_string(),
            flag_key: "feature.x".to_string(),
            project_id: project.to_string(),
            environment: env.to_string(),
            value,
            enabled,
            rollout_percentage: pct,
            rollout_context_key: ctx_key.to_string(),
            revision,
        }
    }

    /// Scope precedence is environment > project > tenant-default, deterministically.
    #[test]
    fn scope_precedence_env_project_tenant_default() {
        let default = flag(
            "d",
            "",
            "",
            FlagVal::Str("default".into()),
            true,
            100,
            "",
            1,
        );
        let tenant_proj = flag(
            "p",
            "proj-1",
            "",
            FlagVal::Str("project".into()),
            true,
            100,
            "",
            2,
        );
        let env = flag(
            "e",
            "",
            "prod",
            FlagVal::Str("env".into()),
            true,
            100,
            "",
            3,
        );
        let env_proj = flag(
            "ep",
            "proj-1",
            "prod",
            FlagVal::Str("env+proj".into()),
            true,
            100,
            "",
            4,
        );

        let ctx = EvalContext {
            project_id: "proj-1".to_string(),
            environment: "prod".to_string(),
            attributes: HashMap::new(),
        };
        // All four are candidates; the most specific (env+project) wins.
        let candidates = vec![
            default.clone(),
            tenant_proj.clone(),
            env.clone(),
            env_proj.clone(),
        ];
        let resolved = resolve_flag(&candidates, &ctx).expect("a candidate must resolve");
        assert_eq!(resolved.flag_id, "ep");

        // Without the env+project row, the env-only row beats the project-only row.
        let candidates = vec![default.clone(), tenant_proj.clone(), env.clone()];
        assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "e");

        // Project-only beats tenant-default.
        let candidates = vec![default.clone(), tenant_proj.clone()];
        assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "p");

        // Only the tenant-default remains.
        let candidates = vec![default.clone()];
        assert_eq!(resolve_flag(&candidates, &ctx).unwrap().flag_id, "d");

        // A non-matching environment row is NOT a candidate.
        let other_env = flag(
            "o",
            "",
            "staging",
            FlagVal::Str("staging".into()),
            true,
            100,
            "",
            5,
        );
        assert!(resolve_flag(std::slice::from_ref(&other_env), &ctx).is_none());
    }

    /// `evaluate_flag` is deterministic: the same (flag, context) yields the same
    /// value on every call, and percentage rollout is stable per subject. These
    /// fixed input -> output pairs are the SDK<->server contract.
    #[test]
    fn evaluate_flag_is_deterministic_and_stable() {
        let ctx = |subject: &str| EvalContext {
            project_id: String::new(),
            environment: String::new(),
            attributes: HashMap::from([("user_id".to_string(), subject.to_string())]),
        };

        // Fully on / fully off / disabled are unconditional.
        let on = flag("a", "", "", FlagVal::Bool(true), true, 100, "user_id", 1);
        assert_eq!(evaluate_flag(&on, &ctx("u-1")), FlagVal::Bool(true));
        let zero = flag("b", "", "", FlagVal::Bool(true), true, 0, "user_id", 1);
        assert_eq!(evaluate_flag(&zero, &ctx("u-1")), FlagVal::Bool(false));
        let disabled = flag(
            "c",
            "",
            "",
            FlagVal::Str("v".into()),
            false,
            100,
            "user_id",
            1,
        );
        assert_eq!(
            evaluate_flag(&disabled, &ctx("u-1")),
            FlagVal::Str(String::new())
        );

        // Stable bucket: the same subject is sticky across many calls.
        let half = flag("h", "", "", FlagVal::Bool(true), true, 50, "user_id", 1);
        let first = evaluate_flag(&half, &ctx("subject-42"));
        for _ in 0..16 {
            assert_eq!(evaluate_flag(&half, &ctx("subject-42")), first);
        }

        // The bucket is a pure function of (flag_key, subject) — assert the exact
        // boundary so any drift in the hash/threshold is a test failure.
        let bucket = rollout_bucket("feature.x", "subject-42");
        let expected = if bucket < 50 {
            FlagVal::Bool(true)
        } else {
            FlagVal::Bool(false)
        };
        assert_eq!(first, expected);
        // Monotonicity: a subject in the 30% bucket is also in the 80% bucket.
        if bucket < 30 {
            let p30 = flag("p30", "", "", FlagVal::Bool(true), true, 30, "user_id", 1);
            let p80 = flag("p80", "", "", FlagVal::Bool(true), true, 80, "user_id", 1);
            assert_eq!(evaluate_flag(&p30, &ctx("subject-42")), FlagVal::Bool(true));
            assert_eq!(evaluate_flag(&p80, &ctx("subject-42")), FlagVal::Bool(true));
        }
    }

    /// The config-domain revision is monotone and bumps on every change.
    #[test]
    fn revision_bumps_monotonically() {
        assert_eq!(bump_revision(0), 1);
        assert_eq!(bump_revision(1), 2);
        let mut rev = 0;
        for expected in 1..=10 {
            rev = bump_revision(rev);
            assert_eq!(rev, expected);
        }
        // Saturating at the ceiling (never panics / wraps).
        assert_eq!(bump_revision(i64::MAX), i64::MAX);
    }

    /// A caller scoped to tenant-a must not write tenant-b's flag by putting a
    /// foreign tenant_id in the request BODY; the guard rejects before any store
    /// access (no Postgres needed) — mirrors `lock_service`/`tenant_service`.
    #[tokio::test]
    async fn put_flag_rejects_cross_tenant_body() {
        let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(config_pb::PutFlagRequest {
            tenant_id: "tenant-b".to_string(),
            flag_key: "feature.x".to_string(),
            value: Some(flag_val_to_proto(&FlagVal::Bool(true))),
            enabled: true,
            rollout_percentage: 100,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_flag(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn put_flag_missing_value_carries_field_violation() {
        let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(config_pb::PutFlagRequest {
            tenant_id: "tenant-a".to_string(),
            flag_key: "feature.x".to_string(),
            value: None,
            enabled: true,
            rollout_percentage: 100,
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .put_flag(request)
            .await
            .expect_err("missing value must be rejected before runtime access");
        assert_eq!(err.message(), "value is required");
        assert_one_validation_field(&err, "value", "must set one FlagValue arm");
    }

    #[tokio::test]
    async fn get_flag_missing_key_carries_field_violation() {
        let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(config_pb::GetFlagRequest {
            tenant_id: "tenant-a".to_string(),
            flag_key: "  ".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .get_flag(request)
            .await
            .expect_err("missing key must be rejected before runtime access");
        assert_eq!(err.message(), "flag_key is required");
        assert_one_validation_field(&err, "flag_key", "must be a non-empty flag key");
    }

    #[tokio::test]
    async fn evaluate_flags_key_limit_carries_field_violation() {
        let svc = ConfigServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(config_pb::EvaluateFlagsRequest {
            tenant_id: "tenant-a".to_string(),
            keys: (0..=MAX_EVALUATE_KEYS)
                .map(|i| format!("feature.{i}"))
                .collect(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .evaluate_flags(request)
            .await
            .expect_err("oversized key set must be rejected before runtime access");
        assert_eq!(err.message(), "too many keys (max 256)");
        assert_one_validation_field(&err, "keys", "must contain at most 256 keys");
    }

    #[test]
    fn json_flag_value_validation_carries_field_violation() {
        let value = Some(config_pb::FlagValue {
            value: Some(config_pb::flag_value::Value::JsonValue("{bad".to_string())),
        });
        let err = proto_to_flag_val(&value).expect_err("invalid JSON flag value");
        assert!(err.message().starts_with("json_value is not valid JSON:"));
        assert_one_validation_field(&err, "value.json_value", "must be valid JSON");
    }

    #[test]
    fn config_missing_runtime_capability_carries_typed_detail() {
        let err = config_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "config service requires runtime native-entity dispatch (no runtime configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "config service requires runtime native-entity dispatch (no runtime configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "config");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }

    /// The batched EvaluateFlags path issues ONE read whose filter ANDs the
    /// tenant scope with a single `flag_key IN (keys)` list, capped at
    /// `keys × MAX_FLAGS_PER_KEY_SCAN` rows — never one read per key.
    #[test]
    fn evaluate_batch_read_builds_single_in_list_filter() {
        let keys = vec![
            "feature.a".to_string(),
            "feature.b".to_string(),
            "feature.c".to_string(),
        ];
        let read = flag_candidates_batch_read("tenant-a", &keys);
        assert_eq!(read.message_type, CONFIG_MSG);

        let Some(LogicalFilter::And(branches)) = read.filter else {
            panic!("batched read must AND the tenant scope with the key In-list");
        };
        assert_eq!(branches.len(), 2, "exactly tenant scope + one In-list");
        assert_eq!(
            branches[0],
            LogicalFilter::Comparison {
                field: "tenant_id".to_string(),
                op: ComparisonOp::Eq,
                value: LogicalValue::String("tenant-a".to_string()),
            },
            "the tenant predicate must always be present",
        );
        assert_eq!(
            branches[1],
            LogicalFilter::InList {
                field: "flag_key".to_string(),
                values: keys
                    .iter()
                    .map(|key| LogicalValue::String(key.clone()))
                    .collect(),
            },
            "all keys must ride ONE In-list filter",
        );

        // Overall row cap = keys × per-key scope cap (documented bound).
        assert_eq!(
            read.pagination,
            Some(LogicalPagination::limit(3 * MAX_FLAGS_PER_KEY_SCAN)),
        );
    }

    /// Round-trip the typed value oneof through storage encoding and back.
    #[test]
    fn value_storage_roundtrip() {
        for value in [
            FlagVal::Bool(true),
            FlagVal::Number(42.5),
            FlagVal::Str("hello".into()),
            FlagVal::Json("{\"a\":1}".into()),
        ] {
            let (ty, json) = flag_val_to_stored(&value);
            assert_eq!(stored_to_flag_val(&ty, &json), value);
        }
    }
}
