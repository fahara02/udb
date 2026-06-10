//! U19 — plugin-aware backend compensators for saga recovery.
//!
//! Pre-U19 the saga recovery worker (`runtime/saga.rs::attempt_compensations`)
//! pulled each compensation step off the saga row, **logged** the
//! `(backend, operation, resource_uri, payload)` fields, and returned
//! `Ok(())`. That was a placeholder — the operator would see the log
//! but no Qdrant delete, no S3 delete, no Mongo rollback ever ran from
//! the recovery path. Inline transaction compensation worked
//! (`runtime/core/tx_object.rs::compensate_external_side_effects` handles
//! Qdrant/S3 immediately when the current transaction fails), but
//! anything that crashed past the inline window was stuck.
//!
//! U19 supplies:
//!
//! - [`CompensationPayload`] — structured form of the JSON row the
//!   saga ledger stores (`backend`, `operation`, `resource_uri`,
//!   `payload`). Parsing logic is here so the recovery worker can swap
//!   from "log a JSON blob" to "dispatch via the registry."
//! - [`BackendCompensator`] — async trait every backend implements.
//!   Object-safe so `CompensatorRegistry` can hold trait objects.
//! - [`CompensatorRegistry`] — keyed by `backend` label
//!   (`"postgres"`, `"qdrant"`, `"s3"`, `"mongodb"`, `"neo4j"`,
//!   `"clickhouse"`). Recovery worker resolves the impl by label
//!   then calls `execute`.
//! - [`QuarantineState`] / [`QuarantinePolicy`] — controls how many
//!   retries a compensation gets before the saga is moved to
//!   `manual_review` instead of being retried on the next sweep.
//! - [`dispatch_compensations`] — the entry point the saga recovery
//!   worker now calls instead of its inline log loop. Reports per-step
//!   outcomes back so the worker can decide whether to advance the
//!   saga to `compensated` or escalate to `manual_review`.
//!
//! The actual Qdrant/S3/Mongo/Neo4j/ClickHouse implementations live
//! next to the executor crates (each backend already exposes its
//! delete/rollback primitives). This module is the **registry + trait
//! contract + dispatcher**, which is the gap the upgrade plan
//! identified.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ── CompensationPayload ───────────────────────────────────────────────────────

/// The structured shape every compensation row uses. Recovery worker
/// reads JSON from the saga ledger and `try_from_json` parses it into
/// this struct. Producers (tx_object.rs) build it from inline
/// compensation tracking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompensationPayload {
    /// Backend label — `"postgres"`, `"qdrant"`, `"s3"`, etc. Drives
    /// the `CompensatorRegistry` lookup.
    pub backend: String,
    /// Operation token — `"delete_points"`, `"delete_object"`,
    /// `"delete_document"`, etc. The compensator switches on this.
    pub operation: String,
    /// Stable resource locator — `qdrant://collection`,
    /// `s3://bucket/key`, `mongodb://db/coll`. Used by the trait
    /// `describe()` for log lines.
    pub resource_uri: String,
    /// Free-form payload the backend impl interprets. For Qdrant this
    /// is `{"point_ids": [...]}`; for S3 it's `{}` (resource_uri carries
    /// everything). Stored as JSON so the saga row can be inspected
    /// without backend knowledge.
    #[serde(default)]
    pub payload: serde_json::Value,
}

impl CompensationPayload {
    /// Parse one step out of the JSON array the saga row stores.
    /// Returns `None` if `backend` and `operation` aren't both
    /// present — a missing field means the producer didn't emit a
    /// real compensation; logging it is the best we can do.
    pub fn try_from_json(value: &serde_json::Value) -> Option<Self> {
        let backend = value.get("backend")?.as_str()?.to_string();
        let operation = value.get("operation")?.as_str()?.to_string();
        if backend.trim().is_empty() || operation.trim().is_empty() {
            return None;
        }
        let resource_uri = value
            .get("resource_uri")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        // Accept both `"payload"` (canonical) and `"compensation"`
        // (legacy key from tx_object.rs's older emissions).
        let payload = value
            .get("payload")
            .or_else(|| value.get("compensation"))
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        Some(Self {
            backend,
            operation,
            resource_uri,
            payload,
        })
    }
}

// ── BackendCompensator ────────────────────────────────────────────────────────

/// What every backend's compensator implements. Object-safe; held as
/// `Arc<dyn BackendCompensator>` in [`CompensatorRegistry`].
#[async_trait]
pub trait BackendCompensator: Send + Sync {
    /// Backend label this implementation handles. Compared
    /// case-insensitively against `CompensationPayload::backend`.
    fn backend_label(&self) -> &str;

    /// Execute the compensation. `Ok(())` on success — the recovery
    /// worker advances the saga past this step. `Err(reason)` pauses
    /// the step; the worker increments the attempt counter and
    /// applies the quarantine policy.
    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String>;

    /// One-line human description for log lines. Default takes the
    /// backend/op/resource_uri; backends can override for richer
    /// detail.
    fn describe(&self, payload: &CompensationPayload) -> String {
        format!(
            "{}::{} on {}",
            payload.backend, payload.operation, payload.resource_uri
        )
    }
}

// ── CompensatorRegistry ───────────────────────────────────────────────────────

/// Keyed by lowercase backend label. The recovery worker constructs
/// one of these at startup (registering Qdrant, S3, Mongo, Neo4j,
/// ClickHouse, Postgres compensators) and re-uses it for every sweep.
#[derive(Default, Clone)]
pub struct CompensatorRegistry {
    by_backend: HashMap<String, Arc<dyn BackendCompensator>>,
}

impl CompensatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an impl. Replaces any previous registration for the
    /// same backend label — last writer wins so a deployment can
    /// override a built-in compensator with a specialised one.
    pub fn register(&mut self, compensator: Arc<dyn BackendCompensator>) {
        let key = compensator.backend_label().to_ascii_lowercase();
        self.by_backend.insert(key, compensator);
    }

    pub fn get(&self, backend_label: &str) -> Option<Arc<dyn BackendCompensator>> {
        self.by_backend
            .get(&backend_label.to_ascii_lowercase())
            .cloned()
    }

    pub fn registered_backends(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.by_backend.keys().cloned().collect();
        keys.sort();
        keys
    }
}

// ── QuarantinePolicy ──────────────────────────────────────────────────────────

/// Per-saga retry policy. The recovery worker bumps `attempts` each
/// time the compensation pipeline fails; once `attempts >= max_attempts`
/// the saga moves to `manual_review` (a terminal state for the worker
/// — operator inspects, fixes, then explicitly retries).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantinePolicy {
    pub max_attempts: u32,
    /// Minimum seconds between two consecutive sweeps' compensation
    /// attempts on the same saga. The worker checks
    /// `last_attempt_at + cooldown_secs <= now` before retrying.
    pub cooldown_secs: u64,
}

impl Default for QuarantinePolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            cooldown_secs: 30,
        }
    }
}

/// What the policy returns when evaluated against the saga's attempt
/// counter. The recovery worker translates each variant into a row
/// update on `udb_sagas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineState {
    /// Retry on the next sweep.
    Retry { next_attempt: u32 },
    /// Quarantined — move the saga to `manual_review` and stop
    /// retrying automatically.
    Quarantine { final_attempts: u32, reason: String },
    /// Cooldown not elapsed — skip this sweep.
    Cooldown { remaining_secs: u64 },
}

impl QuarantinePolicy {
    /// Decide what to do given the current saga state.
    ///
    /// `current_attempts` is the row's `recovery_attempts` column.
    /// `last_attempt_at_unix_secs` is the most recent attempt time
    /// (0 if never attempted). `now_unix_secs` is the wall clock.
    pub fn evaluate(
        &self,
        current_attempts: u32,
        last_attempt_at_unix_secs: i64,
        now_unix_secs: i64,
    ) -> QuarantineState {
        if current_attempts >= self.max_attempts {
            return QuarantineState::Quarantine {
                final_attempts: current_attempts,
                reason: format!(
                    "exceeded max_attempts ({}); escalating to manual_review",
                    self.max_attempts
                ),
            };
        }
        if last_attempt_at_unix_secs > 0 {
            let elapsed = (now_unix_secs - last_attempt_at_unix_secs).max(0) as u64;
            if elapsed < self.cooldown_secs {
                return QuarantineState::Cooldown {
                    remaining_secs: self.cooldown_secs - elapsed,
                };
            }
        }
        QuarantineState::Retry {
            next_attempt: current_attempts.saturating_add(1),
        }
    }
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

/// Per-step result the dispatcher returns. The recovery worker
/// aggregates these into a saga-level outcome (`compensated` /
/// `failed_compensation` / `manual_review`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    /// Compensator ran and returned `Ok(())`.
    Compensated,
    /// No compensator was registered for this backend. The worker
    /// logs and skips — the row stays around for an operator to
    /// inspect.
    NoCompensator { backend: String },
    /// Compensation step was malformed (missing required fields).
    Malformed { index: usize },
    /// Compensator returned `Err(_)`. The worker uses the
    /// `QuarantinePolicy` to decide whether to retry the whole saga.
    Failed { backend: String, reason: String },
}

impl StepOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Compensated)
    }
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }
}

// ── Built-in runtime compensators ─────────────────────────────────────────────

#[cfg(feature = "qdrant")]
pub(crate) struct QdrantPointCompensator {
    client: crate::runtime::executors::qdrant::QdrantHttpClient,
}

#[cfg(feature = "qdrant")]
impl QdrantPointCompensator {
    pub(crate) fn new(client: crate::runtime::executors::qdrant::QdrantHttpClient) -> Self {
        Self { client }
    }
}

#[cfg(feature = "qdrant")]
#[async_trait]
impl BackendCompensator for QdrantPointCompensator {
    fn backend_label(&self) -> &str {
        "qdrant"
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(payload.operation.as_str(), "delete_points" | "delete") {
            return Err(format!(
                "unsupported qdrant compensation operation '{}'",
                payload.operation
            ));
        }
        let collection = payload
            .payload
            .get("collection")
            .and_then(serde_json::Value::as_str)
            .or_else(|| payload.resource_uri.strip_prefix("qdrant://"))
            .ok_or_else(|| "qdrant compensation missing collection".to_string())?;
        let point_ids = payload
            .payload
            .get("point_ids")
            .or_else(|| payload.payload.get("ids"))
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "qdrant compensation missing point_ids".to_string())?
            .iter()
            .filter_map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| value.as_i64().map(|n| n.to_string()))
                    .or_else(|| value.as_u64().map(|n| n.to_string()))
            })
            .collect::<Vec<_>>();
        if point_ids.is_empty() {
            return Ok(());
        }
        self.client
            .delete_points(collection, &point_ids)
            .await
            .map_err(|err| err.to_string())
    }
}

#[cfg(feature = "s3")]
pub(crate) struct S3ObjectCompensator {
    label: String,
    client: aws_sdk_s3::Client,
}

#[cfg(feature = "s3")]
impl S3ObjectCompensator {
    pub(crate) fn new(label: impl Into<String>, client: aws_sdk_s3::Client) -> Self {
        Self {
            label: label.into(),
            client,
        }
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl BackendCompensator for S3ObjectCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if payload.operation != "delete_object" {
            return Err(format!(
                "unsupported s3 compensation operation '{}'",
                payload.operation
            ));
        }
        let (bucket, key) = parse_s3_target(payload)?;
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| format!("S3 delete_object compensation failed: {err}"))?;
        Ok(())
    }
}

#[cfg(feature = "mongodb")]
pub(crate) struct MongoDbCompensator {
    executor: crate::runtime::executors::mongodb::MongoDbExecutor,
}

#[cfg(feature = "mongodb")]
impl MongoDbCompensator {
    pub(crate) fn new(executor: crate::runtime::executors::mongodb::MongoDbExecutor) -> Self {
        Self { executor }
    }
}

#[cfg(feature = "mongodb")]
#[async_trait]
impl BackendCompensator for MongoDbCompensator {
    fn backend_label(&self) -> &str {
        "mongodb"
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(
            payload.operation.as_str(),
            "delete_one" | "delete_document" | "delete"
        ) {
            return Err(format!(
                "unsupported mongodb compensation operation '{}'",
                payload.operation
            ));
        }
        let collection = payload
            .payload
            .get("collection")
            .and_then(serde_json::Value::as_str)
            .or_else(|| payload.resource_uri.strip_prefix("mongodb://"))
            .ok_or_else(|| "mongodb compensation missing collection".to_string())?;
        let filter = payload
            .payload
            .get("filter")
            .or_else(|| payload.payload.get("compensation_filter"))
            .cloned()
            .ok_or_else(|| "mongodb compensation missing filter".to_string())?;
        self.executor
            .delete_document(collection, filter)
            .await
            .map(|_| ())
    }
}

#[cfg(feature = "neo4j")]
pub(crate) struct Neo4jCompensator {
    executor: crate::runtime::executors::neo4j::Neo4jExecutor,
}

#[cfg(feature = "neo4j")]
impl Neo4jCompensator {
    pub(crate) fn new(executor: crate::runtime::executors::neo4j::Neo4jExecutor) -> Self {
        Self { executor }
    }
}

#[cfg(feature = "neo4j")]
#[async_trait]
impl BackendCompensator for Neo4jCompensator {
    fn backend_label(&self) -> &str {
        "neo4j"
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if payload.operation != "delete_node" {
            return Err(format!(
                "unsupported neo4j compensation operation '{}'",
                payload.operation
            ));
        }
        let label = payload
            .payload
            .get("label")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "neo4j compensation missing label".to_string())?;
        let id = payload
            .payload
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "neo4j compensation missing id".to_string())?;
        self.executor.delete_node(label, id).await
    }
}

pub(crate) struct ManualReviewCompensator {
    label: String,
}

impl ManualReviewCompensator {
    pub(crate) fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}

#[async_trait]
impl BackendCompensator for ManualReviewCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        Err(format!(
            "{} compensation '{}' requires manual review",
            payload.backend, payload.operation
        ))
    }
}

#[cfg(feature = "s3")]
fn parse_s3_target(payload: &CompensationPayload) -> Result<(&str, &str), String> {
    let bucket = payload
        .payload
        .get("bucket")
        .and_then(serde_json::Value::as_str);
    let key = payload
        .payload
        .get("key")
        .or_else(|| payload.payload.get("object_key"))
        .and_then(serde_json::Value::as_str);
    if let (Some(bucket), Some(key)) = (bucket, key) {
        return Ok((bucket, key));
    }
    let Some(rest) = payload.resource_uri.strip_prefix("s3://") else {
        return Err("s3 compensation missing bucket/key".to_string());
    };
    let Some((bucket, key)) = rest.split_once('/') else {
        return Err("s3 compensation resource_uri must be s3://bucket/key".to_string());
    };
    if bucket.is_empty() || key.is_empty() {
        return Err("s3 compensation missing bucket/key".to_string());
    }
    Ok((bucket, key))
}

/// Walk the JSON array of compensation steps from the saga row,
/// dispatching each through the registry. Steps are compensated in
/// **reverse (LIFO)** order — the last-executed forward step is undone first,
/// which is required for correct saga semantics (a later step may depend on an
/// earlier one). The reported `index` is still the step's original position.
#[tracing::instrument(skip_all, name = "saga.compensate")]
pub async fn dispatch_compensations(
    registry: &CompensatorRegistry,
    compensations: &serde_json::Value,
) -> Vec<StepOutcome> {
    let steps = compensations.as_array().cloned().unwrap_or_default();
    let mut out = Vec::with_capacity(steps.len());
    for (i, step) in steps.iter().enumerate().rev() {
        let Some(payload) = CompensationPayload::try_from_json(step) else {
            out.push(StepOutcome::Malformed { index: i });
            continue;
        };
        let Some(compensator) = registry.get(&payload.backend) else {
            out.push(StepOutcome::NoCompensator {
                backend: payload.backend.clone(),
            });
            continue;
        };
        match compensator.execute(&payload).await {
            Ok(()) => out.push(StepOutcome::Compensated),
            Err(reason) => out.push(StepOutcome::Failed {
                backend: payload.backend.clone(),
                reason,
            }),
        }
    }
    // Compensators EXECUTE in reverse (undo last-first, above), but the
    // returned outcomes are ordered to match the input `steps` so the recovery
    // worker can map outcome[i] → step[i] (the `out.push` loop above filled
    // `out` in reverse-execution order). Restore input order here.
    out.reverse();
    out
}

/// Convenience: did **every** step succeed? The recovery worker uses this to
/// decide whether to mark the saga `compensated`. An empty set (a saga with no
/// compensation steps. An empty list is not compensated; callers with no
/// compensations should handle that state before asking whether every step
/// finished successfully.
pub fn all_compensated(outcomes: &[StepOutcome]) -> bool {
    !outcomes.is_empty() && outcomes.iter().all(StepOutcome::is_success)
}

// ── SQL canonical-store compensators (NW-deep) ────────────────────────────────
//
// Pre-NW-deep the SQL canonical stores had no saga compensator path.
// A saga that committed a Postgres insert and then failed on a
// downstream Qdrant step would compensate Qdrant (delete points) but
// leave the Postgres row in place — half-committed state surviving
// past the inline transaction window. The recovery worker logged the
// row but never re-ran a DELETE.
//
// These compensators close the gap: each one takes a structured
// `payload` carrying `{table, where_clause, where_params}` and runs a
// parameterised `DELETE FROM table WHERE …` against the canonical
// store's pool. The runtime registers one per SQL canonical store
// during startup.

/// Compensator for Postgres canonical-store rows.
///
/// Payload shape:
/// ```json
/// {
///   "schema": "public",        // optional, defaults to "public"
///   "table": "orders",
///   "where_sql": "id = $1",    // bind placeholders are $1, $2, ...
///   "where_params": ["abc"]    // JSON values; mapped to the pg driver
/// }
/// ```
/// `operation` should be `"delete_row"` or `"delete_rows"`.
pub struct PostgresCompensator {
    pool: sqlx::PgPool,
    label: String,
}

impl PostgresCompensator {
    pub fn new(pool: sqlx::PgPool, label: impl Into<String>) -> Self {
        Self {
            pool,
            label: label.into(),
        }
    }
}

#[async_trait]
impl BackendCompensator for PostgresCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(
            payload.operation.as_str(),
            "delete_row" | "delete_rows" | "delete"
        ) {
            return Err(format!(
                "unsupported postgres compensation operation '{}'",
                payload.operation
            ));
        }
        let table = payload
            .payload
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "postgres compensation missing 'table'".to_string())?;
        let schema = payload
            .payload
            .get("schema")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("public");
        let where_sql = payload
            .payload
            .get("where_sql")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "postgres compensation missing 'where_sql'".to_string())?;
        if where_sql.trim().is_empty() {
            return Err(
                "postgres compensation 'where_sql' is empty; refusing to TRUNCATE table".into(),
            );
        }
        validate_compensation_identifier(schema)?;
        validate_compensation_identifier(table)?;

        let where_params = payload
            .payload
            .get("where_params")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let sql = format!("DELETE FROM \"{schema}\".\"{table}\" WHERE {where_sql}");
        let mut q = sqlx::query(&sql);
        for v in &where_params {
            q = bind_pg_value(q, v);
        }
        q.execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("postgres compensation failed: {err}"))
    }
}

/// Compensator for MySQL canonical-store rows.
///
/// Same payload shape as `PostgresCompensator` but binds via `?`.
#[cfg(feature = "mysql")]
pub struct MysqlCompensator {
    pool: sqlx::MySqlPool,
    label: String,
}

#[cfg(feature = "mysql")]
impl MysqlCompensator {
    pub fn new(pool: sqlx::MySqlPool, label: impl Into<String>) -> Self {
        Self {
            pool,
            label: label.into(),
        }
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl BackendCompensator for MysqlCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(
            payload.operation.as_str(),
            "delete_row" | "delete_rows" | "delete"
        ) {
            return Err(format!(
                "unsupported mysql compensation operation '{}'",
                payload.operation
            ));
        }
        let table = payload
            .payload
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "mysql compensation missing 'table'".to_string())?;
        let schema = payload
            .payload
            .get("schema")
            .and_then(serde_json::Value::as_str);
        let where_sql = payload
            .payload
            .get("where_sql")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "mysql compensation missing 'where_sql'".to_string())?;
        if where_sql.trim().is_empty() {
            return Err(
                "mysql compensation 'where_sql' is empty; refusing to TRUNCATE table".into(),
            );
        }
        validate_compensation_identifier(table)?;
        if let Some(s) = schema {
            validate_compensation_identifier(s)?;
        }
        let where_params = payload
            .payload
            .get("where_params")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let qualified = match schema {
            Some(s) => format!("`{s}`.`{table}`"),
            None => format!("`{table}`"),
        };
        let sql = format!("DELETE FROM {qualified} WHERE {where_sql}");
        let mut q = sqlx::query(&sql);
        for v in &where_params {
            q = bind_mysql_value(q, v);
        }
        q.execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("mysql compensation failed: {err}"))
    }
}

/// Compensator for SQLite canonical-store rows.
#[cfg(feature = "sqlite")]
pub struct SqliteCompensator {
    pool: sqlx::SqlitePool,
    label: String,
}

#[cfg(feature = "sqlite")]
impl SqliteCompensator {
    pub fn new(pool: sqlx::SqlitePool, label: impl Into<String>) -> Self {
        Self {
            pool,
            label: label.into(),
        }
    }
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl BackendCompensator for SqliteCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(
            payload.operation.as_str(),
            "delete_row" | "delete_rows" | "delete"
        ) {
            return Err(format!(
                "unsupported sqlite compensation operation '{}'",
                payload.operation
            ));
        }
        let table = payload
            .payload
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "sqlite compensation missing 'table'".to_string())?;
        let where_sql = payload
            .payload
            .get("where_sql")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "sqlite compensation missing 'where_sql'".to_string())?;
        if where_sql.trim().is_empty() {
            return Err(
                "sqlite compensation 'where_sql' is empty; refusing to TRUNCATE table".into(),
            );
        }
        validate_compensation_identifier(table)?;
        let where_params = payload
            .payload
            .get("where_params")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let sql = format!("DELETE FROM \"{table}\" WHERE {where_sql}");
        let mut q = sqlx::query(&sql);
        for v in &where_params {
            q = bind_sqlite_value(q, v);
        }
        q.execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|err| format!("sqlite compensation failed: {err}"))
    }
}

/// Compensator for Redis cache entries — best-effort `DEL <key>`.
///
/// Payload shape: `{"key": "udb:..."}` or `{"keys": ["k1", "k2"]}`.
#[cfg(feature = "redis")]
pub struct RedisCompensator {
    client: redis::Client,
    label: String,
}

#[cfg(feature = "redis")]
impl RedisCompensator {
    pub fn new(client: redis::Client, label: impl Into<String>) -> Self {
        Self {
            client,
            label: label.into(),
        }
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl BackendCompensator for RedisCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(payload.operation.as_str(), "delete_key" | "del" | "delete") {
            return Err(format!(
                "unsupported redis compensation operation '{}'",
                payload.operation
            ));
        }
        let keys: Vec<String> = if let Some(k) = payload
            .payload
            .get("key")
            .and_then(serde_json::Value::as_str)
        {
            vec![k.to_string()]
        } else if let Some(arr) = payload
            .payload
            .get("keys")
            .and_then(serde_json::Value::as_array)
        {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        } else {
            return Err("redis compensation missing 'key' or 'keys'".into());
        };
        if keys.is_empty() {
            return Ok(());
        }
        use redis::AsyncCommands;
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| format!("redis compensation connect failed: {e}"))?;
        let _: redis::RedisResult<i64> = conn.del(&keys).await;
        // Redis DEL is idempotent — non-existent keys count as "0
        // deleted" which is success from the compensator's viewpoint.
        Ok(())
    }
}

/// Compensator for ClickHouse — emits `ALTER TABLE … DELETE WHERE …`.
///
/// This is a **mutation**, not a transactional rollback — ClickHouse
/// applies it asynchronously by default. The compensator returns Ok
/// after the statement is accepted; the actual deletion happens in
/// background merges. Operators who want sync semantics should set
/// `mutations_sync=2` at the cluster level.
#[cfg(feature = "clickhouse")]
pub struct ClickhouseCompensator {
    executor: crate::runtime::executors::clickhouse::ClickHouseExecutor,
    label: String,
}

#[cfg(feature = "clickhouse")]
impl ClickhouseCompensator {
    pub fn new(
        executor: crate::runtime::executors::clickhouse::ClickHouseExecutor,
        label: impl Into<String>,
    ) -> Self {
        Self {
            executor,
            label: label.into(),
        }
    }
}

#[cfg(feature = "clickhouse")]
#[async_trait]
impl BackendCompensator for ClickhouseCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }

    async fn execute(&self, payload: &CompensationPayload) -> Result<(), String> {
        if !matches!(
            payload.operation.as_str(),
            "delete_row" | "delete_rows" | "delete"
        ) {
            return Err(format!(
                "unsupported clickhouse compensation operation '{}'",
                payload.operation
            ));
        }
        let database = payload
            .payload
            .get("database")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("default");
        let table = payload
            .payload
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "clickhouse compensation missing 'table'".to_string())?;
        let where_sql = payload
            .payload
            .get("where_sql")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "clickhouse compensation missing 'where_sql'".to_string())?;
        if where_sql.trim().is_empty() {
            return Err(
                "clickhouse compensation 'where_sql' is empty; refusing wide mutation".into(),
            );
        }
        validate_compensation_identifier(database)?;
        validate_compensation_identifier(table)?;
        let sql = format!("ALTER TABLE `{database}`.`{table}` DELETE WHERE {where_sql}");
        // ClickHouseExecutor exposes a mutate path that accepts raw
        // SQL via the same dispatch JSON shape the SQL compilers
        // produce. We post the ALTER as a mutation; the executor
        // wraps with the right HTTP headers.
        let req = serde_json::json!({ "sql": sql, "params": [] }).to_string();
        use crate::runtime::executors::MutationExecutor;
        self.executor
            .mutate(&req)
            .await
            .map(|_| ())
            .map_err(|err| format!("clickhouse compensation failed: {err}"))
    }
}

/// Reject identifiers containing characters that would let a malicious
/// payload escape the back-tick / double-quote envelope. Compensators
/// rebuild SQL from the saga ledger, so we can't trust the payload to
/// contain only safe characters even though the saga emitter was
/// trusted at write time.
fn validate_compensation_identifier(ident: &str) -> Result<(), String> {
    if ident.is_empty() {
        return Err("compensation identifier is empty".into());
    }
    if !ident
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "compensation identifier '{ident}' contains disallowed characters"
        ));
    }
    Ok(())
}

/// Bind a JSON value into a sqlx Postgres query in the position
/// matching its `$N` placeholder. Compensation payloads serialise
/// values as JSON; this function maps them back to the appropriate
/// Rust type for the pg driver.
fn bind_pg_value<'a>(
    q: sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: &'a serde_json::Value,
) -> sqlx::query::Query<'a, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        serde_json::Value::Null => q.bind(Option::<&str>::None),
        serde_json::Value::Bool(b) => q.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => q.bind(s.as_str()),
        other => q.bind(other.to_string()),
    }
}

#[cfg(feature = "mysql")]
fn bind_mysql_value<'a>(
    q: sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    v: &'a serde_json::Value,
) -> sqlx::query::Query<'a, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    match v {
        serde_json::Value::Null => q.bind(Option::<&str>::None),
        serde_json::Value::Bool(b) => q.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => q.bind(s.as_str()),
        other => q.bind(other.to_string()),
    }
}

#[cfg(feature = "sqlite")]
fn bind_sqlite_value<'a>(
    q: sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>>,
    v: &'a serde_json::Value,
) -> sqlx::query::Query<'a, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'a>> {
    match v {
        serde_json::Value::Null => q.bind(Option::<&str>::None),
        serde_json::Value::Bool(b) => q.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => q.bind(s.as_str()),
        other => q.bind(other.to_string()),
    }
}

// ── Test compensators ────────────────────────────────────────────────────────

/// Always succeeds. Useful for tests that need a registered backend
/// but don't care about the per-call behaviour.
pub struct AlwaysOkCompensator {
    label: String,
}
impl AlwaysOkCompensator {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
        }
    }
}
#[async_trait]
impl BackendCompensator for AlwaysOkCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }
    async fn execute(&self, _payload: &CompensationPayload) -> Result<(), String> {
        Ok(())
    }
}

/// Always returns `Err(reason)`. Lets tests pin the quarantine path.
pub struct AlwaysFailCompensator {
    label: String,
    reason: String,
}
impl AlwaysFailCompensator {
    pub fn new(label: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            reason: reason.into(),
        }
    }
}
#[async_trait]
impl BackendCompensator for AlwaysFailCompensator {
    fn backend_label(&self) -> &str {
        &self.label
    }
    async fn execute(&self, _payload: &CompensationPayload) -> Result<(), String> {
        Err(self.reason.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn qdrant_step() -> serde_json::Value {
        json!({
            "backend": "qdrant",
            "operation": "delete_points",
            "resource_uri": "qdrant://users",
            "payload": {"point_ids": ["1","2"]}
        })
    }

    fn s3_step() -> serde_json::Value {
        json!({
            "backend": "s3",
            "operation": "delete_object",
            "resource_uri": "s3://bucket/key",
            "payload": {}
        })
    }

    /// Pin: payload parser tolerates the legacy `"compensation"` key
    /// emitted by tx_object.rs before U19, AND the canonical
    /// `"payload"` key. A mixed saga ledger must keep replaying.
    #[test]
    fn payload_parser_accepts_both_canonical_and_legacy_keys() {
        let canonical = json!({
            "backend": "qdrant",
            "operation": "delete_points",
            "resource_uri": "qdrant://x",
            "payload": {"point_ids": ["a"]}
        });
        let legacy = json!({
            "backend": "qdrant",
            "operation": "delete_points",
            "resource_uri": "qdrant://x",
            "compensation": {"point_ids": ["a"]}
        });
        let a = CompensationPayload::try_from_json(&canonical).unwrap();
        let b = CompensationPayload::try_from_json(&legacy).unwrap();
        assert_eq!(a.payload, b.payload);
        // Missing required field rejected.
        assert!(CompensationPayload::try_from_json(&json!({"backend": "x"})).is_none());
        assert!(CompensationPayload::try_from_json(&json!({"operation": "y"})).is_none());
    }

    /// Pin: dispatcher routes each step to the right backend impl
    /// and reports per-step outcomes in order.
    #[tokio::test]
    async fn dispatcher_routes_by_backend_label() {
        let mut registry = CompensatorRegistry::new();
        registry.register(Arc::new(AlwaysOkCompensator::new("qdrant")));
        registry.register(Arc::new(AlwaysOkCompensator::new("s3")));

        let steps = json!([qdrant_step(), s3_step()]);
        let outcomes = dispatch_compensations(&registry, &steps).await;
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes.iter().all(StepOutcome::is_success));
        assert!(all_compensated(&outcomes));
    }

    /// Pin: missing compensator produces a typed `NoCompensator`
    /// outcome instead of silently logging. The worker sees the
    /// gap, the operator can register one and resume.
    #[tokio::test]
    async fn missing_compensator_is_reported_not_silent() {
        let registry = CompensatorRegistry::new(); // empty
        let steps = json!([qdrant_step()]);
        let outcomes = dispatch_compensations(&registry, &steps).await;
        assert_eq!(outcomes.len(), 1);
        match &outcomes[0] {
            StepOutcome::NoCompensator { backend } => assert_eq!(backend, "qdrant"),
            other => panic!("expected NoCompensator, got {:?}", other),
        }
        assert!(!all_compensated(&outcomes));
    }

    /// Pin: failed compensator surfaces the error reason so the
    /// recovery worker can write it to `last_error`. Order is
    /// preserved.
    #[tokio::test]
    async fn failed_compensator_surfaces_error_reason() {
        let mut registry = CompensatorRegistry::new();
        registry.register(Arc::new(AlwaysOkCompensator::new("qdrant")));
        registry.register(Arc::new(AlwaysFailCompensator::new(
            "s3",
            "bucket policy denied delete",
        )));
        let steps = json!([qdrant_step(), s3_step()]);
        let outcomes = dispatch_compensations(&registry, &steps).await;
        assert!(outcomes[0].is_success());
        match &outcomes[1] {
            StepOutcome::Failed { backend, reason } => {
                assert_eq!(backend, "s3");
                assert!(reason.contains("bucket policy denied"));
            }
            other => panic!("expected Failed, got {:?}", other),
        }
        assert!(!all_compensated(&outcomes));
    }

    /// Pin: malformed step is flagged with its index so the operator
    /// can locate it in the saga row.
    #[tokio::test]
    async fn malformed_step_is_indexed() {
        let registry = CompensatorRegistry::new();
        let steps = json!([{"missing": "fields"}]);
        let outcomes = dispatch_compensations(&registry, &steps).await;
        match &outcomes[0] {
            StepOutcome::Malformed { index } => assert_eq!(*index, 0),
            other => panic!("expected Malformed, got {:?}", other),
        }
    }

    /// Pin: backend label is case-insensitive — a saga emitted with
    /// `"QDRANT"` still resolves the registered `"qdrant"` impl.
    /// Prevents a Producer/Recovery casing mismatch from silently
    /// dropping compensation.
    #[tokio::test]
    async fn registry_lookup_is_case_insensitive() {
        let mut registry = CompensatorRegistry::new();
        registry.register(Arc::new(AlwaysOkCompensator::new("Qdrant")));
        let steps = json!([{
            "backend": "QDRANT",
            "operation": "delete_points",
            "resource_uri": "qdrant://x",
            "payload": {}
        }]);
        let outcomes = dispatch_compensations(&registry, &steps).await;
        assert!(outcomes[0].is_success());
    }

    /// Pin: quarantine policy escalates to manual_review at
    /// `max_attempts`. Operator inspects, resolves, restarts.
    #[test]
    fn quarantine_policy_escalates_after_max_attempts() {
        let policy = QuarantinePolicy {
            max_attempts: 3,
            cooldown_secs: 0,
        };
        let now = 1_000_i64;
        // Below threshold → retry, attempt counter bumps.
        match policy.evaluate(2, 990, now) {
            QuarantineState::Retry { next_attempt } => assert_eq!(next_attempt, 3),
            other => panic!("expected Retry, got {:?}", other),
        }
        // At threshold → quarantine.
        match policy.evaluate(3, 990, now) {
            QuarantineState::Quarantine {
                final_attempts,
                reason,
            } => {
                assert_eq!(final_attempts, 3);
                assert!(reason.contains("exceeded max_attempts"));
            }
            other => panic!("expected Quarantine, got {:?}", other),
        }
    }

    /// Pin: quarantine policy enforces cooldown between retries.
    /// Prevents the saga sweep from busy-looping on a flaky backend.
    #[test]
    fn quarantine_policy_enforces_cooldown_between_retries() {
        let policy = QuarantinePolicy {
            max_attempts: 5,
            cooldown_secs: 30,
        };
        // last attempt 10s ago, cooldown 30s → 20s remaining.
        match policy.evaluate(1, 990, 1000) {
            QuarantineState::Cooldown { remaining_secs } => {
                assert_eq!(remaining_secs, 20);
            }
            other => panic!("expected Cooldown, got {:?}", other),
        }
        // After cooldown → retry.
        match policy.evaluate(1, 900, 1000) {
            QuarantineState::Retry { .. } => {}
            other => panic!("expected Retry, got {:?}", other),
        }
        // First attempt (last=0) skips cooldown.
        match policy.evaluate(0, 0, 1000) {
            QuarantineState::Retry { next_attempt } => assert_eq!(next_attempt, 1),
            other => panic!("expected Retry, got {:?}", other),
        }
    }

    /// Pin: `all_compensated` is false on an empty outcome list. A
    /// saga with zero compensation steps should NOT be marked
    /// `compensated` — that would be hiding a missing producer.
    #[test]
    fn all_compensated_requires_at_least_one_step() {
        assert!(!all_compensated(&[]));
        assert!(all_compensated(&[StepOutcome::Compensated]));
        assert!(!all_compensated(&[
            StepOutcome::Compensated,
            StepOutcome::Failed {
                backend: "x".into(),
                reason: "y".into()
            }
        ]));
    }

    #[test]
    fn validate_compensation_identifier_accepts_safe_names() {
        assert!(validate_compensation_identifier("orders").is_ok());
        assert!(validate_compensation_identifier("order_lines_v2").is_ok());
        assert!(validate_compensation_identifier("my-table").is_ok());
    }

    #[test]
    fn validate_compensation_identifier_rejects_sql_injection_shapes() {
        assert!(validate_compensation_identifier("orders; DROP TABLE x").is_err());
        assert!(validate_compensation_identifier("orders\"").is_err());
        assert!(validate_compensation_identifier("orders`").is_err());
        assert!(validate_compensation_identifier("orders'").is_err());
        assert!(validate_compensation_identifier("").is_err());
        assert!(validate_compensation_identifier("orders OR 1=1").is_err());
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_compensator_deletes_row_by_pk() {
        // Build an in-memory SQLite pool, seed a row, run a
        // compensation step, assert the row is gone.
        use sqlx::sqlite::SqlitePoolOptions;
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // :memory: requires single connection
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");

        sqlx::query("CREATE TABLE orders (id TEXT PRIMARY KEY, total INTEGER)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO orders (id, total) VALUES (?, ?)")
            .bind("o1")
            .bind(42)
            .execute(&pool)
            .await
            .unwrap();

        let compensator = SqliteCompensator::new(pool.clone(), "sqlite");
        let payload = CompensationPayload {
            backend: "sqlite".into(),
            operation: "delete_row".into(),
            resource_uri: "sqlite://orders/o1".into(),
            payload: serde_json::json!({
                "table": "orders",
                "where_sql": "id = ?",
                "where_params": ["o1"]
            }),
        };
        compensator.execute(&payload).await.unwrap();

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(remaining, 0);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_compensator_rejects_empty_where_clause() {
        use sqlx::sqlite::SqlitePoolOptions;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();

        let compensator = SqliteCompensator::new(pool.clone(), "sqlite");
        let payload = CompensationPayload {
            backend: "sqlite".into(),
            operation: "delete_row".into(),
            resource_uri: "sqlite://t".into(),
            payload: serde_json::json!({
                "table": "t",
                "where_sql": "",
                "where_params": []
            }),
        };
        let err = compensator.execute(&payload).await.unwrap_err();
        // Critical: empty WHERE must not silently TRUNCATE.
        assert!(err.contains("'where_sql' is empty"));
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_compensator_rejects_unsafe_table_name() {
        use sqlx::sqlite::SqlitePoolOptions;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        let compensator = SqliteCompensator::new(pool.clone(), "sqlite");
        let payload = CompensationPayload {
            backend: "sqlite".into(),
            operation: "delete_row".into(),
            resource_uri: "sqlite://t".into(),
            payload: serde_json::json!({
                "table": "t; DROP TABLE users",
                "where_sql": "id = ?",
                "where_params": [1]
            }),
        };
        let err = compensator.execute(&payload).await.unwrap_err();
        assert!(err.contains("disallowed characters"));
    }
}
