//! NW1-1a — System-table operations on the canonical store.
//!
//! `CanonicalStore` (in `mod.rs`) supplies the **durability** primitives
//! (`current_durability_token`, `wait_for_token`, outbox). This module
//! supplies the **system-table** primitives — projection tasks, saga
//! state, admin audit, migration audit — which UDB previously read and
//! wrote directly through `PgPool`.
//!
//! Phase 1a covers projection tasks. Subsets 1b–1d follow.
//!
//! ## What "production-grade" means in this module
//!
//! - **No `todo!()`, no `unimplemented!()`, no shortcut returns.** Every
//!   method has a real implementation per backend (PG, MySQL, SQLite).
//! - **Schema preserved.** The existing PG projection_tasks table has 18
//!   columns, two CHECK constraints, a UNIQUE on `idempotency_key`, and
//!   two indexes. The trait surface mirrors this exactly so the
//!   call-site migration in NW1 step 3 is a swap with no semantic
//!   change.
//! - **Atomic claim.** The claim operation marks `PENDING|FAILED`
//!   tasks `IN_PROGRESS` in one round-trip:
//!   - PG: `FOR UPDATE SKIP LOCKED` + `UPDATE … RETURNING`.
//!   - MySQL: `FOR UPDATE SKIP LOCKED` (8.0.1+).
//!   - SQLite: `BEGIN IMMEDIATE` (single-writer atomic) + `RETURNING`
//!     (3.35+). Equivalent isolation because SQLite is single-writer
//!     by nature.
//! - **Idempotent enqueue.** Re-enqueueing the same
//!   `idempotency_key` returns the existing `task_id`, not a fresh
//!   one. The producer's saga-step retry can re-emit without
//!   duplicating work.
//! - **Status discipline.** `ProjectionTaskStatus` is the wire
//!   contract. PG/MySQL store it as TEXT with a CHECK constraint;
//!   SQLite as TEXT (no CHECK — SQLite is dynamically typed) plus a
//!   trigger that enforces the same set. The Rust enum is the source
//!   of truth for the parsing logic.
//! - **Tested.** `conformance::projection_task_contract` runs the
//!   same assertions against every store. Today it runs in-memory on
//!   SQLite on every CI; PG/MySQL pick it up under env-gated tests.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ── Status enum ───────────────────────────────────────────────────────────────

/// Pinned status tokens for projection tasks. The string forms appear
/// in the DB CHECK constraint, in metric labels, in audit logs, in
/// the gRPC admin RPCs. Treat them as part of the wire contract —
/// changing one breaks dashboards and triggers a schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectionTaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    DeadLetter,
}

impl ProjectionTaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::DeadLetter => "DEAD_LETTER",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "PENDING" => Some(Self::Pending),
            "IN_PROGRESS" => Some(Self::InProgress),
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "DEAD_LETTER" => Some(Self::DeadLetter),
            _ => None,
        }
    }

    /// The set the worker can claim. PENDING is the normal queue;
    /// FAILED is the retry queue.
    pub fn is_claimable(self) -> bool {
        matches!(self, Self::Pending | Self::Failed)
    }

    /// Terminal — no further processing.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::DeadLetter)
    }
}

// ── Operation enum ────────────────────────────────────────────────────────────

/// Per-row operation kind. Pinned because the DB CHECK constraint
/// enumerates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionOperation {
    Upsert,
    Delete,
}

impl ProjectionOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Upsert => "upsert",
            Self::Delete => "delete",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "upsert" => Some(Self::Upsert),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }
}

// ── Insert payload ────────────────────────────────────────────────────────────

/// Everything the producer supplies on enqueue. Mirrors the existing
/// `INSERT INTO udb_projection_tasks (…) VALUES (…)` column list so
/// the producer-side call sites map field-by-field.
///
/// `task_id` is **assigned by the store**, not by the caller. The
/// idempotency contract is on `idempotency_key`: two enqueues with the
/// same key return the same `task_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionTaskInsert {
    pub idempotency_key: String,
    pub project_id: String,
    pub manifest_checksum: String,
    pub message_type: String,
    pub source_schema: String,
    pub source_table: String,
    /// JSON object describing the source row's identity. PG stores
    /// this as JSONB; MySQL as JSON; SQLite as TEXT. Stored as
    /// `serde_json::Value` here so the producer doesn't worry about
    /// the wire type.
    pub source_row_key: serde_json::Value,
    pub operation: ProjectionOperation,
    pub target_backend: String,
    pub target_instance: String,
    pub projection_kind: String,
    pub resource_name: String,
    /// JSON array of ManifestStoreOption-shaped objects.
    pub target_options: serde_json::Value,
    /// JSON object — the canonical payload to project.
    pub source_payload: serde_json::Value,
    pub source_checksum: String,
}

// ── Row returned on claim ─────────────────────────────────────────────────────

/// What `claim_projection_tasks` returns. Mirrors what the existing
/// claim query selects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionTaskRow {
    pub task_id: Uuid,
    pub idempotency_key: String,
    pub project_id: String,
    pub target_backend: String,
    pub target_instance: String,
    pub projection_kind: String,
    pub resource_name: String,
    pub operation: ProjectionOperation,
    pub source_row_key: serde_json::Value,
    pub target_options: serde_json::Value,
    pub source_payload: serde_json::Value,
    pub source_checksum: String,
    pub status: ProjectionTaskStatus,
    pub retry_count: i32,
    pub last_error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

// ── Claim filter ──────────────────────────────────────────────────────────────

/// Controls one claim batch. Pinning these explicitly (instead of
/// reading globals) keeps the trait pure and the worker testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionClaimFilter {
    /// Cap batch size. The store may return fewer rows.
    pub batch_size: i64,
    /// Skip rows whose `retry_count` is at-or-above this. Used to
    /// keep poison rows out of the active queue.
    pub max_retries: i32,
    /// Restrict to a single backend+instance? `None` means "any".
    /// Used by per-backend worker pools.
    pub target_backend: Option<String>,
    pub target_instance: Option<String>,
    /// Restrict claims to one project. `None` preserves legacy any-project
    /// worker behavior for deployments that have not split workers per project.
    pub project_id: Option<String>,
}

impl Default for ProjectionClaimFilter {
    fn default() -> Self {
        Self {
            batch_size: 50,
            max_retries: 5,
            target_backend: None,
            target_instance: None,
            project_id: None,
        }
    }
}

// ── Summary for /admin and metrics ────────────────────────────────────────────

/// Counts by status. Returned by `projection_task_summary`; emitted
/// every poll cycle as the projection-lag metric and used by the
/// admin RPC.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionTaskSummary {
    pub pending: i64,
    pub in_progress: i64,
    pub completed: i64,
    pub failed: i64,
    pub dead_letter: i64,
}

impl ProjectionTaskSummary {
    pub fn total(&self) -> i64 {
        self.pending + self.in_progress + self.completed + self.failed + self.dead_letter
    }

    /// What the worker can still pick up (active queue depth).
    pub fn claimable(&self) -> i64 {
        self.pending + self.failed
    }
}

// ── Aggregate-metric types ───────────────────────────────────────────────────

/// One row from [`ProjectionTaskStore::pending_task_metrics`].
/// Drives the per-`(project, backend, instance, kind)` gauges the
/// projection worker emits each pass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingTaskMetric {
    pub project_id: String,
    pub target_backend: String,
    pub target_instance: String,
    pub projection_kind: String,
    /// Count of PENDING + FAILED rows in this group.
    pub pending: i64,
    /// Age (seconds) of the oldest PENDING/FAILED row, as a float.
    pub oldest_age_seconds: f64,
}

/// One row from [`ProjectionTaskStore::dead_letter_groups`]. The
/// reconciliation worker scans these groups and requeues each.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeadLetterGroup {
    pub source_table: String,
    pub target_backend: String,
    pub target_instance: String,
    pub dead_count: i64,
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Typed errors so call sites can map to the right gRPC code without
/// stringly comparison. Implementations construct these from the
/// underlying sqlx error.
///
/// Hand-rolled Display/Error so the module stays dep-light.
#[derive(Debug)]
pub enum SystemStoreError {
    /// DB connection / IO / network failure.
    Io {
        backend: &'static str,
        source: String,
    },
    /// SQL execution failed (constraint violation, type error, etc.).
    Query {
        backend: &'static str,
        sql: String,
        source: String,
    },
    /// The store doesn't have the required version (e.g. MySQL < 8.0.1
    /// can't do `FOR UPDATE SKIP LOCKED`; SQLite < 3.35 can't do
    /// RETURNING).
    UnsupportedVersion {
        backend: &'static str,
        required: &'static str,
        detail: String,
    },
    /// Schema check failed — table missing, column missing.
    SchemaMismatch {
        backend: &'static str,
        detail: String,
    },
    /// Caller passed an invalid value (bad status string, malformed
    /// JSON, etc.).
    InvalidInput(String),
}

impl std::fmt::Display for SystemStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { backend, source } => {
                write!(f, "{backend} system store I/O failure: {source}")
            }
            Self::Query {
                backend,
                sql,
                source,
            } => {
                let preview: String = sql.chars().take(80).collect();
                write!(
                    f,
                    "{backend} system store query failed: {source} (sql preview: '{preview}…')"
                )
            }
            Self::UnsupportedVersion {
                backend,
                required,
                detail,
            } => write!(f, "{backend} system store requires {required}: {detail}"),
            Self::SchemaMismatch { backend, detail } => {
                write!(f, "{backend} system store schema mismatch: {detail}")
            }
            Self::InvalidInput(msg) => write!(f, "system store invalid input: {msg}"),
        }
    }
}

impl std::error::Error for SystemStoreError {}

impl SystemStoreError {
    pub fn io(backend: &'static str, err: impl std::fmt::Display) -> Self {
        Self::Io {
            backend,
            source: err.to_string(),
        }
    }

    pub fn query(
        backend: &'static str,
        sql: impl Into<String>,
        err: impl std::fmt::Display,
    ) -> Self {
        Self::Query {
            backend,
            sql: sql.into(),
            source: err.to_string(),
        }
    }
}

/// Convenience alias.
pub type SystemStoreResult<T> = Result<T, SystemStoreError>;

// ── ProjectionTaskStore trait ────────────────────────────────────────────────

/// The system-store operations on projection tasks. Implemented by
/// every canonical store (Postgres, MySQL, SQLite). Object-safe so
/// the runtime can hold `Arc<dyn ProjectionTaskStore>`.
///
/// **Why a sibling trait instead of extending `CanonicalStore` directly?**
/// `CanonicalStore` already has a heavy responsibility (durability +
/// outbox). Bundling 8 more methods on it forces every consumer (even
/// those that only want `current_durability_token`) to know about the
/// projection schema. Splitting keeps each surface focused. The
/// concrete impl types (`PostgresCanonicalStore`, …) implement both
/// traits, so the runtime can hold one `Arc` and use whichever surface
/// it needs.
#[async_trait]
pub trait ProjectionTaskStore: Send + Sync {
    fn backend_label(&self) -> &'static str;

    /// Idempotent — safe to call on every startup. Creates the
    /// `udb_projection_tasks` table + indexes in the right dialect.
    async fn ensure_projection_tables(&self) -> SystemStoreResult<()>;

    /// Insert a task. Returns the assigned `task_id`. Re-enqueueing
    /// the same `idempotency_key` returns the existing `task_id`
    /// without creating a duplicate.
    async fn enqueue_projection_task(&self, task: &ProjectionTaskInsert)
    -> SystemStoreResult<Uuid>;

    /// Atomically claim a batch. Pre-claim status is `PENDING` or
    /// `FAILED` with `retry_count < filter.max_retries`. Post-claim
    /// status is `IN_PROGRESS`.
    ///
    /// Implementations use:
    /// - PG: `FOR UPDATE SKIP LOCKED` + `UPDATE … RETURNING`.
    /// - MySQL: `FOR UPDATE SKIP LOCKED` (requires 8.0.1+).
    /// - SQLite: `BEGIN IMMEDIATE` (single-writer atomicity) +
    ///   `UPDATE … RETURNING` (requires 3.35+).
    async fn claim_projection_tasks(
        &self,
        filter: &ProjectionClaimFilter,
    ) -> SystemStoreResult<Vec<ProjectionTaskRow>>;

    /// Worker reports success. Sets status `COMPLETED`,
    /// `completed_at = NOW()`.
    async fn mark_projection_task_completed(&self, task_id: Uuid) -> SystemStoreResult<()>;

    /// Worker reports failure. `new_retry_count` is the post-failure
    /// counter; `new_status` is whichever of `FAILED` (retry-eligible)
    /// or `DEAD_LETTER` (poison) the caller decided.
    async fn mark_projection_task_failed(
        &self,
        task_id: Uuid,
        new_retry_count: i32,
        new_status: ProjectionTaskStatus,
        error: &str,
    ) -> SystemStoreResult<()>;

    /// Operator-driven: move every dead-letter row matching the
    /// filter back to `PENDING` with `retry_count = 0`. Returns the
    /// number of rows requeued.
    async fn requeue_dead_letter_tasks(
        &self,
        target_backend: Option<&str>,
    ) -> SystemStoreResult<i64>;

    /// Reconciliation: rows stuck in `IN_PROGRESS` longer than
    /// `stale_after` are reset to `PENDING` with a "stale
    /// in-progress reconciliation" note in `last_error`. Returns the
    /// number of rows reset.
    async fn reset_stale_in_progress_tasks(&self, stale_after: Duration) -> SystemStoreResult<i64>;

    /// `(pending, in_progress, completed, failed, dead_letter)`
    /// counts. Used by metrics + admin summary.
    async fn projection_task_summary(&self) -> SystemStoreResult<ProjectionTaskSummary>;

    /// NW1-3b — pending-task metrics grouped by
    /// `(project_id, target_backend, target_instance, projection_kind)`.
    /// Returns the count of PENDING/FAILED rows + the age (seconds)
    /// of the oldest such row in each group. The projection worker
    /// uses this each pass to feed the `projection_tasks_pending`
    /// and `projection_oldest_pending_age_seconds` gauges.
    ///
    /// `limit` caps the group count returned; existing PG path used
    /// 500.
    async fn pending_task_metrics(&self, limit: i64) -> SystemStoreResult<Vec<PendingTaskMetric>>;

    /// NW1-3b — dead-letter task groups by
    /// `(source_table, target_backend, target_instance)`. Returns
    /// the count of DEAD_LETTER rows per group. The reconciliation
    /// worker scans this to know which combinations to repair.
    async fn dead_letter_groups(&self, limit: i64) -> SystemStoreResult<Vec<DeadLetterGroup>>;

    /// NW1-3b — requeue dead-letter rows matching a specific
    /// `(source_table, target_backend, target_instance)` triple back
    /// to PENDING (with `retry_count = 0` and last_error =
    /// 'reconciliation repair'). Returns rows-affected.
    async fn requeue_dead_letter_by_source(
        &self,
        source_table: &str,
        target_backend: &str,
        target_instance: &str,
    ) -> SystemStoreResult<i64>;

    /// NW1-3e — count tasks among `idempotency_keys` whose status is
    /// NOT terminal-or-failed (`COMPLETED` / `DEAD_LETTER` / `FAILED`).
    ///
    /// Used by `consistency_fence::wait_for_fence` to decide whether
    /// a read fence (carrying the projection task IDs the producer
    /// enqueued on the matching write) has cleared. Returning `0`
    /// means every task in the list either succeeded or is in a
    /// terminal failure state that the fence treats as "not coming
    /// back, serve the read." A non-zero count means at least one
    /// task is still in-flight; the fence loops and re-checks.
    async fn pending_projection_task_count(
        &self,
        idempotency_keys: &[String],
    ) -> SystemStoreResult<i64>;
}

// ─────────────────────────────────────────────────────────────────────────────
// NW1-1b — Saga state operations.
// ─────────────────────────────────────────────────────────────────────────────

/// Saga lifecycle states. Pinned to the existing `ALLOWED_STATUSES`
/// list `runtime/saga.rs::list_sagas` validates against — change any
/// of these and the admin RPC breaks.
///
/// Stored as TEXT in PG/MySQL with CHECK constraint, and as TEXT in
/// SQLite (with the same CHECK).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaStatus {
    /// Saga state at process startup recovery: we don't know whether
    /// the saga committed, compensated, or is mid-flight.
    Indeterminate,
    /// Saga is currently executing forward steps.
    InProgress,
    /// Saga was recorded but no step has executed yet.
    Pending,
    /// All forward steps succeeded; saga is finalized.
    Committed,
    /// Compensation completed — every reversible side effect was undone.
    Compensated,
    /// Forward execution failed but compensation hasn't started.
    Failed,
    /// Two-phase commit outcome is unknown.
    InDoubt,
    /// Compensation itself failed. Recovery worker will retry up to
    /// the poison threshold.
    FailedCompensation,
    /// Operator-flagged. Auto-recovery is paused until explicit
    /// retry.
    ManualReview,
}

impl SagaStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Indeterminate => "indeterminate",
            Self::InProgress => "in_progress",
            Self::Pending => "pending",
            Self::Committed => "committed",
            Self::Compensated => "compensated",
            Self::Failed => "failed",
            Self::InDoubt => "in_doubt",
            Self::FailedCompensation => "failed_compensation",
            Self::ManualReview => "manual_review",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "indeterminate" => Some(Self::Indeterminate),
            "in_progress" => Some(Self::InProgress),
            "pending" => Some(Self::Pending),
            "committed" => Some(Self::Committed),
            "compensated" => Some(Self::Compensated),
            "failed" => Some(Self::Failed),
            "in_doubt" => Some(Self::InDoubt),
            "failed_compensation" => Some(Self::FailedCompensation),
            "manual_review" => Some(Self::ManualReview),
            _ => None,
        }
    }

    /// Every valid status token, in canonical order. Used by the
    /// admin RPC's argument validation + DDL CHECK constraint.
    pub fn all() -> &'static [SagaStatus] {
        &[
            Self::Indeterminate,
            Self::InProgress,
            Self::Pending,
            Self::Committed,
            Self::Compensated,
            Self::Failed,
            Self::InDoubt,
            Self::FailedCompensation,
            Self::ManualReview,
        ]
    }

    /// Recovery worker considers these claimable for compensation:
    /// `indeterminate` (rebuild state) or stale `in_progress` (worker
    /// crashed mid-flight).
    pub fn is_recoverable(self) -> bool {
        matches!(self, Self::Indeterminate | Self::InProgress | Self::InDoubt)
    }

    /// `committed` / `compensated` / `manual_review` are terminal —
    /// recovery skips them. `failed_compensation` is non-terminal
    /// (recovery will retry).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Committed | Self::Compensated | Self::ManualReview
        )
    }
}

/// Sub-state describing the compensation phase. Stored separately
/// from the main `status` because a `failed_compensation` saga can
/// still carry useful breadcrumbs (`retry_requested`, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompensationStatus {
    /// No compensation attempted yet (or saga didn't need one).
    None,
    /// Compensation succeeded.
    Completed,
    /// Operator flagged for manual review.
    ManualReview,
    /// Operator requested a retry; recovery worker will pick it up
    /// on the next pass.
    RetryRequested,
}

impl CompensationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Completed => "completed",
            Self::ManualReview => "manual_review",
            Self::RetryRequested => "retry_requested",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "none" => Some(Self::None),
            "completed" => Some(Self::Completed),
            "manual_review" => Some(Self::ManualReview),
            "retry_requested" => Some(Self::RetryRequested),
            _ => None,
        }
    }
}

/// Insert payload — what the producer supplies when recording a new
/// saga. `saga_id` is assigned by the store (UUID v4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaInsert {
    pub tx_id: String,
    pub tenant_id: String,
    pub correlation_id: String,
    pub backend_instance: String,
    pub operation: String,
    pub status: SagaStatus,
    /// JSON array of executed forward steps.
    pub steps: serde_json::Value,
    /// JSON array of executable compensation payloads (matches
    /// `runtime::saga_compensators::CompensationPayload`).
    pub compensations: serde_json::Value,
}

/// One saga row as it lives in the store. Mirrors the existing
/// `SagaAdminRecord` + the columns the recovery worker uses
/// (`recovery_attempts`, `compensation_status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaRow {
    pub saga_id: Uuid,
    pub tx_id: String,
    pub tenant_id: String,
    pub correlation_id: String,
    pub status: SagaStatus,
    pub backend_instance: String,
    pub operation: String,
    pub current_step: i32,
    pub retry_count: i32,
    pub recovery_attempts: i32,
    pub compensation_status: CompensationStatus,
    pub steps: serde_json::Value,
    pub compensations: serde_json::Value,
    pub last_error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Filters for `list_sagas`. Mirrors the admin RPC's argument list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SagaListFilter {
    pub tenant_id: Option<String>,
    pub status: Option<SagaStatus>,
    /// Filtered as exact-match string. The PG impl UUID-validates
    /// this before binding; the SQLite/MySQL impls compare as TEXT.
    pub tx_id: Option<String>,
    pub correlation_id: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

/// Counts by status for the admin summary RPC + metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaSummary {
    pub indeterminate: i64,
    pub in_progress: i64,
    pub pending: i64,
    pub committed: i64,
    pub compensated: i64,
    pub failed: i64,
    pub in_doubt: i64,
    pub failed_compensation: i64,
    pub manual_review: i64,
}

impl SagaSummary {
    pub fn total(&self) -> i64 {
        self.indeterminate
            + self.in_progress
            + self.pending
            + self.committed
            + self.compensated
            + self.failed
            + self.in_doubt
            + self.failed_compensation
            + self.manual_review
    }

    /// What the recovery worker can still pick up.
    pub fn recoverable(&self) -> i64 {
        self.indeterminate + self.in_progress + self.in_doubt + self.failed_compensation
    }
}

/// The saga-store trait. Same pattern as `ProjectionTaskStore`: each
/// canonical-class backend (Postgres / MySQL / SQLite) implements it,
/// and the runtime holds `Arc<dyn SagaStore>`.
#[async_trait]
pub trait SagaStore: Send + Sync {
    fn backend_label(&self) -> &'static str;

    /// Idempotent. Creates the `udb_sagas` table + tenant/status
    /// index using the right dialect.
    async fn ensure_saga_tables(&self) -> SystemStoreResult<()>;

    /// Insert a new saga. Returns the assigned `saga_id`.
    async fn record_saga(&self, saga: &SagaInsert) -> SystemStoreResult<Uuid>;

    /// Fetch a single saga. `None` when no row matches.
    async fn get_saga(&self, saga_id: Uuid) -> SystemStoreResult<Option<SagaRow>>;

    /// List sagas with the given filters. `limit + offset` are the
    /// pagination contract.
    async fn list_sagas(&self, filter: &SagaListFilter) -> SystemStoreResult<Vec<SagaRow>>;

    /// Set the saga's status + compensation_status atomically. Used
    /// by the recovery worker's "happy path" (mark compensated).
    async fn update_saga_status(
        &self,
        saga_id: Uuid,
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()>;

    /// Batch variant of [`update_saga_status`]: apply one
    /// `(status, compensation_status)` to many sagas in a single round-trip. The
    /// recovery worker groups its terminal outcomes and flushes each group once
    /// instead of writing per saga. The default loops over `update_saga_status`;
    /// Postgres overrides it with a set-based `WHERE saga_id = ANY(...)`.
    async fn update_saga_statuses_batch(
        &self,
        saga_ids: &[Uuid],
        status: SagaStatus,
        compensation_status: CompensationStatus,
    ) -> SystemStoreResult<()> {
        for &saga_id in saga_ids {
            self.update_saga_status(saga_id, status, compensation_status)
                .await?;
        }
        Ok(())
    }

    /// Operator-driven: flag a saga for manual review. Recovery
    /// worker will skip it until [`request_saga_recompensation`].
    async fn mark_saga_manual_review(&self, saga_id: Uuid) -> SystemStoreResult<()>;

    /// Operator-driven: ask the recovery worker to retry. Refuses
    /// unless the saga is currently in `failed_compensation` or
    /// `manual_review` — defensive guard.
    async fn request_saga_recompensation(&self, saga_id: Uuid) -> SystemStoreResult<()>;

    /// Recovery worker bumps `recovery_attempts` after a failed
    /// compensation pass. Returns the post-increment count; the
    /// worker compares against the poison threshold to decide
    /// whether to escalate to `manual_review`.
    async fn increment_recovery_attempts(
        &self,
        saga_id: Uuid,
        error: &str,
    ) -> SystemStoreResult<i64>;

    /// Recovery worker reads the recoverable queue: rows in
    /// `indeterminate` status OR `in_progress` whose `updated_at` is
    /// older than `stale_after`. Ordered oldest-first; limit applies.
    async fn claim_recoverable_sagas(
        &self,
        stale_after: std::time::Duration,
        limit: i64,
    ) -> SystemStoreResult<Vec<SagaRow>>;

    /// Startup cleanup: any `in_progress` row older than
    /// `stale_after` is reset to `indeterminate` so the recovery
    /// worker considers it. Returns the rows-affected count.
    async fn mark_stale_in_progress_indeterminate(
        &self,
        stale_after: std::time::Duration,
    ) -> SystemStoreResult<i64>;

    /// `(indeterminate, in_progress, pending, committed, compensated,
    /// failed, failed_compensation, manual_review)` counts.
    async fn saga_summary(&self) -> SystemStoreResult<SagaSummary>;
}

// ─────────────────────────────────────────────────────────────────────────────
// NW1-1c — Admin audit log (tamper-evident hash chain).
// ─────────────────────────────────────────────────────────────────────────────

/// Insert payload for one admin audit row. `audit_id` is assigned by
/// the store; `previous_hash` is read by the store from the latest
/// row inside the same atomic write; `current_hash` is computed by
/// the store as the SHA-256 of the canonical-JSON of the linked
/// fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAuditInsert {
    pub actor: String,
    pub operation: String,
    pub target: String,
    pub request_json: serde_json::Value,
    pub result: String,
    pub tenant_id: String,
    pub project_id: String,
    pub correlation_id: String,
    pub signer_key_id: String,
    pub external_anchor: String,
}

/// One audit row as it lives in the store. Read by `list` / `verify`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminAuditRow {
    pub audit_id: Uuid,
    pub actor: String,
    pub operation: String,
    pub target: String,
    pub request_json: serde_json::Value,
    pub result: String,
    pub tenant_id: String,
    pub project_id: String,
    pub correlation_id: String,
    pub previous_hash: String,
    pub current_hash: String,
    pub signer_key_id: String,
    pub external_anchor: String,
    pub created_at: DateTime<Utc>,
}

/// Filters for `list_admin_audit`. Mirrors the existing
/// `list_admin_audit_logs` RPC's arguments.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AdminAuditListFilter {
    pub operation: Option<String>,
    pub actor: Option<String>,
    pub tenant_id: Option<String>,
    pub project_id: Option<String>,
    pub limit: i64,
    pub offset: i64,
    /// When `true`, the store replaces `request_json` with
    /// `{"redacted": true}` before returning. Used by the admin RPC
    /// when the caller doesn't have the `audit:request_json:read`
    /// scope.
    pub redact_request_json: bool,
}

/// Result of a chain verification pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum AdminAuditChainReport {
    /// Chain is intact end-to-end. `checked_count` = rows verified;
    /// `last_hash` = the final row's `current_hash` (use as anchor
    /// for external timestamp services).
    Passed {
        checked_count: i64,
        last_hash: String,
    },
    /// First broken link encountered. Fields name exactly what
    /// failed so operators can locate the tamper.
    Failed {
        checked_count: i64,
        first_broken_audit_id: Uuid,
        reason: AdminAuditBreakReason,
        expected_previous_hash: String,
        actual_previous_hash: String,
        expected_current_hash: String,
        actual_current_hash: String,
    },
}

impl AdminAuditChainReport {
    pub fn is_passed(&self) -> bool {
        matches!(self, Self::Passed { .. })
    }
    pub fn checked_count(&self) -> i64 {
        match self {
            Self::Passed { checked_count, .. } => *checked_count,
            Self::Failed { checked_count, .. } => *checked_count,
        }
    }
}

/// Why a chain row was rejected. Tokens are stable for the admin
/// dashboard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminAuditBreakReason {
    /// Stored `previous_hash` doesn't match what we just computed
    /// (or, for the first row, doesn't match the empty string).
    PreviousHashMismatch,
    /// Row has no `current_hash`. Either never written (corruption)
    /// or stripped by an attacker.
    MissingCurrentHash,
    /// Stored `current_hash` doesn't match the SHA-256 we just
    /// computed from the canonical-JSON of the row's fields.
    CurrentHashMismatch,
}

impl AdminAuditBreakReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreviousHashMismatch => "previous_hash_mismatch",
            Self::MissingCurrentHash => "missing_current_hash",
            Self::CurrentHashMismatch => "current_hash_mismatch",
        }
    }
}

/// Canonical-JSON SHA-256 hash for one row. Same algorithm as
/// `runtime/core/catalog_admin.rs::admin_audit_hash`; centralised
/// here so PG/MySQL/SQLite impls compute byte-identical chains.
///
/// **DO NOT CHANGE THIS** without versioning the audit log. The
/// hash is part of the durability contract — every existing row's
/// `current_hash` is computed against this exact formula.
pub fn compute_admin_audit_hash(
    previous_hash: &str,
    actor: &str,
    operation: &str,
    target: &str,
    request_json: &serde_json::Value,
    result: &str,
    tenant_id: &str,
    project_id: &str,
    correlation_id: &str,
    signer_key_id: &str,
    external_anchor: &str,
) -> String {
    use sha2::{Digest, Sha256};
    // `serde_json::json!` produces key order matching the macro source,
    // which keeps the canonical form stable across rustc versions.
    let canonical = serde_json::json!({
        "previous_hash": previous_hash,
        "actor": actor,
        "operation": operation,
        "target": target,
        "request_json": request_json,
        "result": result,
        "tenant_id": tenant_id,
        "project_id": project_id,
        "correlation_id": correlation_id,
        "signer_key_id": signer_key_id,
        "external_anchor": external_anchor,
    });
    let encoded = serde_json::to_vec(&canonical).unwrap_or_default();
    format!("{:x}", Sha256::digest(encoded))
}

/// Per-row chain verification helper. The three impls each pull rows
/// in order (created_at ASC, audit_id ASC) and feed them in one at
/// a time; this returns the running `previous_hash` to feed into the
/// next call, or the `Failed` report on first break.
///
/// Shared so PG/MySQL/SQLite verify cannot drift apart on what
/// "tampered" means.
pub fn verify_admin_audit_chain_step(
    row: &AdminAuditRow,
    expected_previous_hash: &str,
    checked_count: i64,
) -> Result<String, AdminAuditChainReport> {
    if row.previous_hash != expected_previous_hash {
        return Err(AdminAuditChainReport::Failed {
            checked_count,
            first_broken_audit_id: row.audit_id,
            reason: AdminAuditBreakReason::PreviousHashMismatch,
            expected_previous_hash: expected_previous_hash.to_string(),
            actual_previous_hash: row.previous_hash.clone(),
            expected_current_hash: String::new(),
            actual_current_hash: String::new(),
        });
    }
    if row.current_hash.is_empty() {
        let expected_current_hash = compute_admin_audit_hash(
            &row.previous_hash,
            &row.actor,
            &row.operation,
            &row.target,
            &row.request_json,
            &row.result,
            &row.tenant_id,
            &row.project_id,
            &row.correlation_id,
            &row.signer_key_id,
            &row.external_anchor,
        );
        return Err(AdminAuditChainReport::Failed {
            checked_count,
            first_broken_audit_id: row.audit_id,
            reason: AdminAuditBreakReason::MissingCurrentHash,
            expected_previous_hash: expected_previous_hash.to_string(),
            actual_previous_hash: row.previous_hash.clone(),
            expected_current_hash,
            actual_current_hash: row.current_hash.clone(),
        });
    }
    let expected_current_hash = compute_admin_audit_hash(
        &row.previous_hash,
        &row.actor,
        &row.operation,
        &row.target,
        &row.request_json,
        &row.result,
        &row.tenant_id,
        &row.project_id,
        &row.correlation_id,
        &row.signer_key_id,
        &row.external_anchor,
    );
    if row.current_hash != expected_current_hash {
        return Err(AdminAuditChainReport::Failed {
            checked_count,
            first_broken_audit_id: row.audit_id,
            reason: AdminAuditBreakReason::CurrentHashMismatch,
            expected_previous_hash: expected_previous_hash.to_string(),
            actual_previous_hash: row.previous_hash.clone(),
            expected_current_hash,
            actual_current_hash: row.current_hash.clone(),
        });
    }
    Ok(row.current_hash.clone())
}

/// The admin-audit-store trait. Same pattern as `SagaStore` /
/// `ProjectionTaskStore`. Each canonical-class backend implements it.
#[async_trait]
pub trait AdminAuditStore: Send + Sync {
    fn backend_label(&self) -> &'static str;

    /// Idempotent DDL.
    async fn ensure_admin_audit_tables(&self) -> SystemStoreResult<()>;

    /// Read the most recent row's `current_hash`. Returns empty string
    /// when the table is empty (the chain starts from `""`).
    async fn latest_admin_audit_hash(&self) -> SystemStoreResult<String>;

    /// Append one row atomically. The store:
    ///
    /// 1. Acquires a chain lock (PG: `pg_advisory_xact_lock`, MySQL:
    ///    `GET_LOCK`, SQLite: `BEGIN IMMEDIATE`) so concurrent
    ///    appends can't produce two rows with the same `previous_hash`.
    /// 2. Reads the latest `current_hash`.
    /// 3. Computes the new row's `current_hash` via
    ///    [`compute_admin_audit_hash`].
    /// 4. Inserts the row with the assigned UUID.
    /// 5. Commits.
    ///
    /// Returns the assigned `audit_id`.
    async fn append_admin_audit(&self, entry: &AdminAuditInsert) -> SystemStoreResult<Uuid>;

    /// Multi-axis list. Filter strings are bound as parameters; no
    /// caller input ever interpolates into SQL. When
    /// `filter.redact_request_json == true`, each returned row's
    /// `request_json` is replaced with `{"redacted": true}`.
    async fn list_admin_audit(
        &self,
        filter: &AdminAuditListFilter,
    ) -> SystemStoreResult<Vec<AdminAuditRow>>;

    /// Stream the chain oldest-to-newest, recomputing every
    /// `current_hash`, returning at the first break. `limit = None`
    /// verifies the whole chain; `limit = Some(n)` verifies the
    /// oldest `n` rows.
    async fn verify_admin_audit_chain(
        &self,
        limit: Option<i64>,
    ) -> SystemStoreResult<AdminAuditChainReport>;
}

// ─────────────────────────────────────────────────────────────────────────────
// NW1-1d — Migration audit ledger.
// ─────────────────────────────────────────────────────────────────────────────

/// State of one migration run. Pinned to the existing PG `udb_migration_runs.state`
/// CHECK constraint values. Changing any string here means a schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MigrationRunState {
    DryRun,
    Preflight,
    Applying,
    Verifying,
    Completed,
    Error,
    DeadLetter,
}

impl MigrationRunState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DryRun => "DRY_RUN",
            Self::Preflight => "PREFLIGHT",
            Self::Applying => "APPLYING",
            Self::Verifying => "VERIFYING",
            Self::Completed => "COMPLETED",
            Self::Error => "ERROR",
            Self::DeadLetter => "DEAD_LETTER",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "DRY_RUN" => Some(Self::DryRun),
            "PREFLIGHT" => Some(Self::Preflight),
            "APPLYING" => Some(Self::Applying),
            "VERIFYING" => Some(Self::Verifying),
            "COMPLETED" => Some(Self::Completed),
            "ERROR" => Some(Self::Error),
            "DEAD_LETTER" => Some(Self::DeadLetter),
            _ => None,
        }
    }

    pub fn all() -> &'static [MigrationRunState] {
        &[
            Self::DryRun,
            Self::Preflight,
            Self::Applying,
            Self::Verifying,
            Self::Completed,
            Self::Error,
            Self::DeadLetter,
        ]
    }

    /// Terminal — no further updates expected on the run row.
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Error | Self::DeadLetter)
    }
}

/// Per-artifact operation status in `udb_migration_op_ledger.status`.
/// Pinned to the existing CHECK constraint values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OpLedgerStatus {
    Pending,
    Applied,
    Verified,
    Skipped,
    Failed,
    RolledBack,
}

impl OpLedgerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Applied => "APPLIED",
            Self::Verified => "VERIFIED",
            Self::Skipped => "SKIPPED",
            Self::Failed => "FAILED",
            Self::RolledBack => "ROLLED_BACK",
        }
    }

    pub fn parse(token: &str) -> Option<Self> {
        match token {
            "PENDING" => Some(Self::Pending),
            "APPLIED" => Some(Self::Applied),
            "VERIFIED" => Some(Self::Verified),
            "SKIPPED" => Some(Self::Skipped),
            "FAILED" => Some(Self::Failed),
            "ROLLED_BACK" => Some(Self::RolledBack),
            _ => None,
        }
    }
}

/// Insert payload for `start_migration_run`. `run_id` is assigned by
/// the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRunInsert {
    pub project_id: String,
    pub catalog_version: String,
    pub operations_hash: String,
    pub approval_token: String,
    pub state: MigrationRunState,
}

/// Insert payload for `record_migration_op`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationOpInsert {
    pub run_id: Uuid,
    pub operation_index: i32,
    pub backend: String,
    pub resource_uri: String,
    pub operation_kind: String,
    pub status: OpLedgerStatus,
    pub rollback_json: serde_json::Value,
    pub error: String,
}

/// Row returned for one migration run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRunRow {
    pub run_id: Uuid,
    pub project_id: String,
    pub catalog_version: String,
    pub state: MigrationRunState,
    pub operations_hash: String,
    pub approval_token: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub error: String,
}

/// Row returned for one op-ledger entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationOpRow {
    pub id: i64,
    pub run_id: Uuid,
    pub operation_index: i32,
    pub backend: String,
    pub resource_uri: String,
    pub operation_kind: String,
    pub status: OpLedgerStatus,
    pub rollback_json: serde_json::Value,
    pub error: String,
    pub applied_at: Option<DateTime<Utc>>,
}

/// Filters for `list_migration_runs`. Mirrors what an admin RPC would
/// ask for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MigrationRunsFilter {
    pub project_id: Option<String>,
    pub state: Option<MigrationRunState>,
    pub catalog_version: Option<String>,
    pub limit: i64,
    pub offset: i64,
}

/// The migration audit store trait. Each canonical-class backend
/// implements it the same way `ProjectionTaskStore`, `SagaStore`, and
/// `AdminAuditStore` are implemented.
///
/// **Schema responsibility split:** the existing
/// `runtime/system.rs::ensure_system_catalog` still creates these
/// tables on PG (because that path is the one operators run today
/// and it already covers many other tables). `ensure_migration_audit_tables`
/// here creates the same tables in MySQL/SQLite, and is idempotent
/// against PG too — running both is safe.
#[async_trait]
pub trait MigrationAuditStore: Send + Sync {
    fn backend_label(&self) -> &'static str;

    /// Idempotent DDL — creates both `udb_migration_runs` and
    /// `udb_migration_op_ledger` with the right dialect.
    async fn ensure_migration_audit_tables(&self) -> SystemStoreResult<()>;

    /// Insert a new run row. Returns the assigned `run_id`.
    async fn start_migration_run(&self, run: &MigrationRunInsert) -> SystemStoreResult<Uuid>;

    /// Insert one op ledger entry. Returns the auto-assigned `id`.
    async fn record_migration_op(&self, op: &MigrationOpInsert) -> SystemStoreResult<i64>;

    /// Update a run's state + error + finished_at. Used at the end
    /// of an apply.
    async fn finish_migration_run(
        &self,
        run_id: Uuid,
        new_state: MigrationRunState,
        error: &str,
    ) -> SystemStoreResult<()>;

    /// Fetch one run + its op ledger entries. `None` when no run
    /// matches.
    async fn get_migration_run(&self, run_id: Uuid) -> SystemStoreResult<Option<MigrationRunRow>>;

    /// List op-ledger entries for a run, ordered by `operation_index`.
    async fn list_migration_ops(&self, run_id: Uuid) -> SystemStoreResult<Vec<MigrationOpRow>>;

    /// List runs with filter + pagination, newest first.
    async fn list_migration_runs(
        &self,
        filter: &MigrationRunsFilter,
    ) -> SystemStoreResult<Vec<MigrationRunRow>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin: status string forms are the wire contract for the DB
    /// CHECK constraint + the dashboard. They MUST NOT change.
    #[test]
    fn projection_status_tokens_are_pinned() {
        assert_eq!(ProjectionTaskStatus::Pending.as_str(), "PENDING");
        assert_eq!(ProjectionTaskStatus::InProgress.as_str(), "IN_PROGRESS");
        assert_eq!(ProjectionTaskStatus::Completed.as_str(), "COMPLETED");
        assert_eq!(ProjectionTaskStatus::Failed.as_str(), "FAILED");
        assert_eq!(ProjectionTaskStatus::DeadLetter.as_str(), "DEAD_LETTER");

        for s in [
            ProjectionTaskStatus::Pending,
            ProjectionTaskStatus::InProgress,
            ProjectionTaskStatus::Completed,
            ProjectionTaskStatus::Failed,
            ProjectionTaskStatus::DeadLetter,
        ] {
            assert_eq!(ProjectionTaskStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(ProjectionTaskStatus::parse("bogus"), None);
    }

    /// Pin: operation tokens are the wire contract.
    #[test]
    fn projection_operation_tokens_are_pinned() {
        assert_eq!(ProjectionOperation::Upsert.as_str(), "upsert");
        assert_eq!(ProjectionOperation::Delete.as_str(), "delete");
        assert_eq!(
            ProjectionOperation::parse("upsert"),
            Some(ProjectionOperation::Upsert)
        );
        assert_eq!(
            ProjectionOperation::parse("delete"),
            Some(ProjectionOperation::Delete)
        );
        assert_eq!(ProjectionOperation::parse("INSERT"), None);
    }

    /// Pin: PENDING + FAILED are the claimable set; everything else
    /// is not. Worker logic relies on this so we pin it.
    #[test]
    fn projection_claimability_pinned() {
        assert!(ProjectionTaskStatus::Pending.is_claimable());
        assert!(ProjectionTaskStatus::Failed.is_claimable());
        assert!(!ProjectionTaskStatus::InProgress.is_claimable());
        assert!(!ProjectionTaskStatus::Completed.is_claimable());
        assert!(!ProjectionTaskStatus::DeadLetter.is_claimable());

        assert!(ProjectionTaskStatus::Completed.is_terminal());
        assert!(ProjectionTaskStatus::DeadLetter.is_terminal());
        assert!(!ProjectionTaskStatus::Pending.is_terminal());
    }

    /// Pin: summary arithmetic. The admin dashboard uses `total` and
    /// `claimable` directly; both must include the right buckets.
    #[test]
    fn projection_summary_arithmetic_is_pinned() {
        let s = ProjectionTaskSummary {
            pending: 5,
            in_progress: 2,
            completed: 100,
            failed: 3,
            dead_letter: 1,
        };
        assert_eq!(s.total(), 111);
        assert_eq!(s.claimable(), 8);
    }

    /// Pin: saga status tokens are the wire contract. The PG admin
    /// RPC's `ALLOWED_STATUSES` validation must match these exactly.
    #[test]
    fn saga_status_tokens_are_pinned() {
        assert_eq!(SagaStatus::Indeterminate.as_str(), "indeterminate");
        assert_eq!(SagaStatus::InProgress.as_str(), "in_progress");
        assert_eq!(SagaStatus::Pending.as_str(), "pending");
        assert_eq!(SagaStatus::Committed.as_str(), "committed");
        assert_eq!(SagaStatus::Compensated.as_str(), "compensated");
        assert_eq!(SagaStatus::Failed.as_str(), "failed");
        assert_eq!(SagaStatus::InDoubt.as_str(), "in_doubt");
        assert_eq!(
            SagaStatus::FailedCompensation.as_str(),
            "failed_compensation"
        );
        assert_eq!(SagaStatus::ManualReview.as_str(), "manual_review");

        // Round-trip every variant.
        for s in SagaStatus::all() {
            assert_eq!(SagaStatus::parse(s.as_str()), Some(*s));
        }
        assert_eq!(SagaStatus::parse("garbage"), None);
        assert_eq!(SagaStatus::all().len(), 9, "exactly 9 saga statuses");
    }

    /// Pin: recoverable + terminal sets. Recovery worker uses these.
    #[test]
    fn saga_recoverability_pinned() {
        assert!(SagaStatus::Indeterminate.is_recoverable());
        assert!(SagaStatus::InProgress.is_recoverable());
        assert!(SagaStatus::InDoubt.is_recoverable());
        assert!(!SagaStatus::Pending.is_recoverable());
        assert!(!SagaStatus::Committed.is_recoverable());

        assert!(SagaStatus::Committed.is_terminal());
        assert!(SagaStatus::Compensated.is_terminal());
        assert!(SagaStatus::ManualReview.is_terminal());
        assert!(!SagaStatus::FailedCompensation.is_terminal());
    }

    /// Pin: compensation status tokens.
    #[test]
    fn compensation_status_tokens_are_pinned() {
        assert_eq!(CompensationStatus::None.as_str(), "none");
        assert_eq!(CompensationStatus::Completed.as_str(), "completed");
        assert_eq!(CompensationStatus::ManualReview.as_str(), "manual_review");
        assert_eq!(
            CompensationStatus::RetryRequested.as_str(),
            "retry_requested"
        );
        for token in ["none", "completed", "manual_review", "retry_requested"] {
            assert_eq!(
                CompensationStatus::parse(token).map(|s| s.as_str()),
                Some(token)
            );
        }
    }

    /// Pin: saga summary arithmetic + recoverable bucket.
    #[test]
    fn saga_summary_arithmetic_is_pinned() {
        let s = SagaSummary {
            indeterminate: 1,
            in_progress: 2,
            pending: 3,
            committed: 10,
            compensated: 5,
            failed: 1,
            in_doubt: 6,
            failed_compensation: 2,
            manual_review: 4,
        };
        assert_eq!(s.total(), 34);
        // Recoverable = indeterminate + in_progress + in_doubt + failed_compensation.
        assert_eq!(s.recoverable(), 1 + 2 + 6 + 2);
    }

    fn sample_audit_row(audit_id: Uuid, previous_hash: &str) -> AdminAuditRow {
        let req = serde_json::json!({"target": "catalog"});
        let current = compute_admin_audit_hash(
            previous_hash,
            "operator-1",
            "ActivateCatalog",
            "project-alpha",
            &req,
            "ok",
            "tenant-1",
            "project-alpha",
            "corr-1",
            "default",
            "",
        );
        AdminAuditRow {
            audit_id,
            actor: "operator-1".to_string(),
            operation: "ActivateCatalog".to_string(),
            target: "project-alpha".to_string(),
            request_json: req,
            result: "ok".to_string(),
            tenant_id: "tenant-1".to_string(),
            project_id: "project-alpha".to_string(),
            correlation_id: "corr-1".to_string(),
            previous_hash: previous_hash.to_string(),
            current_hash: current,
            signer_key_id: "default".to_string(),
            external_anchor: String::new(),
            created_at: Utc::now(),
        }
    }

    /// Pin: canonical hash is stable + non-trivial. Two distinct
    /// inputs produce two distinct hashes; equivalent inputs produce
    /// the same hash. Pin the hex length so anyone changing the
    /// digest algorithm sees the test fail.
    #[test]
    fn admin_audit_hash_is_deterministic_and_collision_resistant() {
        let req = serde_json::json!({"x": 1});
        let h1 = compute_admin_audit_hash(
            "", "actor", "Op", "t", &req, "ok", "tn", "pj", "co", "k", "",
        );
        let h2 = compute_admin_audit_hash(
            "", "actor", "Op", "t", &req, "ok", "tn", "pj", "co", "k", "",
        );
        assert_eq!(h1, h2, "same inputs → same hash");
        assert_eq!(h1.len(), 64, "SHA-256 hex is 64 chars");

        // Flip any field → different hash.
        let h_changed = compute_admin_audit_hash(
            "",
            "actor",
            "Op",
            "t",
            &req,
            "ok",
            "tn",
            "pj",
            "co",
            "DIFFERENT",
            "",
        );
        assert_ne!(h1, h_changed);
    }

    /// Pin: the chain step accepts a well-formed row.
    #[test]
    fn admin_audit_chain_step_accepts_valid_row() {
        let row = sample_audit_row(Uuid::new_v4(), "");
        let next_previous =
            verify_admin_audit_chain_step(&row, "", 0).expect("first row links the empty seed");
        assert_eq!(next_previous, row.current_hash);
    }

    /// Pin: tampering with `previous_hash` is flagged.
    #[test]
    fn admin_audit_chain_step_rejects_previous_hash_mismatch() {
        let mut row = sample_audit_row(Uuid::new_v4(), "abcd");
        row.previous_hash = "wrong".to_string();
        let err = verify_admin_audit_chain_step(&row, "expected-hash", 5).expect_err("must reject");
        match err {
            AdminAuditChainReport::Failed {
                reason,
                checked_count,
                expected_previous_hash,
                actual_previous_hash,
                ..
            } => {
                assert_eq!(reason, AdminAuditBreakReason::PreviousHashMismatch);
                assert_eq!(checked_count, 5);
                assert_eq!(expected_previous_hash, "expected-hash");
                assert_eq!(actual_previous_hash, "wrong");
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    /// Pin: missing current_hash is flagged.
    #[test]
    fn admin_audit_chain_step_rejects_missing_current_hash() {
        let mut row = sample_audit_row(Uuid::new_v4(), "");
        row.current_hash = String::new();
        let err = verify_admin_audit_chain_step(&row, "", 0).expect_err("must reject");
        assert!(matches!(
            err,
            AdminAuditChainReport::Failed {
                reason: AdminAuditBreakReason::MissingCurrentHash,
                ..
            }
        ));
    }

    /// Pin: tampering with row body (after the hash is computed)
    /// is flagged as current_hash mismatch.
    #[test]
    fn admin_audit_chain_step_rejects_tampered_body() {
        let mut row = sample_audit_row(Uuid::new_v4(), "");
        // Modify operation without recomputing current_hash.
        row.operation = "MaliciouslyAlteredOp".to_string();
        let err = verify_admin_audit_chain_step(&row, "", 0).expect_err("must reject");
        match err {
            AdminAuditChainReport::Failed {
                reason,
                expected_current_hash,
                actual_current_hash,
                ..
            } => {
                assert_eq!(reason, AdminAuditBreakReason::CurrentHashMismatch);
                assert_ne!(expected_current_hash, actual_current_hash);
            }
            other => panic!("expected Failed, got: {other:?}"),
        }
    }

    /// Pin: report tokens for the dashboard. Break reasons map
    /// stably to strings.
    #[test]
    fn admin_audit_break_reason_tokens_pinned() {
        assert_eq!(
            AdminAuditBreakReason::PreviousHashMismatch.as_str(),
            "previous_hash_mismatch"
        );
        assert_eq!(
            AdminAuditBreakReason::MissingCurrentHash.as_str(),
            "missing_current_hash"
        );
        assert_eq!(
            AdminAuditBreakReason::CurrentHashMismatch.as_str(),
            "current_hash_mismatch"
        );
    }

    /// Pin: chain report's predicates.
    #[test]
    fn admin_audit_chain_report_predicates() {
        let passed = AdminAuditChainReport::Passed {
            checked_count: 10,
            last_hash: "abc".to_string(),
        };
        let failed = AdminAuditChainReport::Failed {
            checked_count: 5,
            first_broken_audit_id: Uuid::nil(),
            reason: AdminAuditBreakReason::PreviousHashMismatch,
            expected_previous_hash: String::new(),
            actual_previous_hash: String::new(),
            expected_current_hash: String::new(),
            actual_current_hash: String::new(),
        };
        assert!(passed.is_passed());
        assert!(!failed.is_passed());
        assert_eq!(passed.checked_count(), 10);
        assert_eq!(failed.checked_count(), 5);
    }

    /// Pin: migration run state tokens are the wire contract for the
    /// PG CHECK constraint + the admin RPC.
    #[test]
    fn migration_run_state_tokens_pinned() {
        assert_eq!(MigrationRunState::DryRun.as_str(), "DRY_RUN");
        assert_eq!(MigrationRunState::Preflight.as_str(), "PREFLIGHT");
        assert_eq!(MigrationRunState::Applying.as_str(), "APPLYING");
        assert_eq!(MigrationRunState::Verifying.as_str(), "VERIFYING");
        assert_eq!(MigrationRunState::Completed.as_str(), "COMPLETED");
        assert_eq!(MigrationRunState::Error.as_str(), "ERROR");
        assert_eq!(MigrationRunState::DeadLetter.as_str(), "DEAD_LETTER");

        for s in MigrationRunState::all() {
            assert_eq!(MigrationRunState::parse(s.as_str()), Some(*s));
        }
        assert_eq!(MigrationRunState::parse("garbage"), None);
        assert_eq!(MigrationRunState::all().len(), 7);

        // Terminal set: COMPLETED / ERROR / DEAD_LETTER.
        assert!(MigrationRunState::Completed.is_terminal());
        assert!(MigrationRunState::Error.is_terminal());
        assert!(MigrationRunState::DeadLetter.is_terminal());
        assert!(!MigrationRunState::Applying.is_terminal());
        assert!(!MigrationRunState::DryRun.is_terminal());
    }

    /// Pin: op ledger status tokens.
    #[test]
    fn op_ledger_status_tokens_pinned() {
        for (s, token) in [
            (OpLedgerStatus::Pending, "PENDING"),
            (OpLedgerStatus::Applied, "APPLIED"),
            (OpLedgerStatus::Verified, "VERIFIED"),
            (OpLedgerStatus::Skipped, "SKIPPED"),
            (OpLedgerStatus::Failed, "FAILED"),
            (OpLedgerStatus::RolledBack, "ROLLED_BACK"),
        ] {
            assert_eq!(s.as_str(), token);
            assert_eq!(OpLedgerStatus::parse(token), Some(s));
        }
        assert_eq!(OpLedgerStatus::parse("none"), None);
    }

    /// Pin: SystemStoreError formatting includes the backend label
    /// and an SQL preview that won't flood the log. Operators
    /// triaging a failure rely on these.
    #[test]
    fn system_store_error_display_is_useful() {
        let big_sql = format!("SELECT {} FROM t", "x".repeat(500));
        let e = SystemStoreError::query("postgres", big_sql.clone(), "syntax error at $1");
        let s = format!("{e}");
        assert!(s.contains("postgres"));
        assert!(s.contains("syntax error at $1"));
        // Preview is capped to 80 chars; full SQL must NOT leak.
        assert!(!s.contains(&"x".repeat(200)));
        assert!(s.len() < big_sql.len(), "error must not embed full SQL");
    }
}
