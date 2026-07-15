//! Native `LockService` (master-plan 9.2) — distributed application locks.
//!
//! Mirrors `tenant_service`: proto-driven, no in-memory store, no hand-mapped
//! schema. Mutual exclusion is the portable `udb_advisory_leases` primitive taken
//! atomically at the SQL layer — reused via
//! [`DataBrokerRuntime::try_acquire_native_lease`] /
//! [`DataBrokerRuntime::release_native_lease`] — NOT re-implemented here. The
//! durable, tenant-scoped `udb_lock.locks` row records who holds each lock and
//! the monotone fencing token handed out at grant time, so a slow/partitioned
//! holder presenting a stale token is fenced off (`failed_precondition`).
//!
//! Doctrine (Phase 9): the lock name is always derived from the VERIFIED claim
//! tenant (never the request body), per-tenant active-lock quota is enforced,
//! admission is fair (`native_helpers::admit_on`), state is durable in the
//! canonical store, and every mutation emits a versioned dot-topic outbox event.
//!
//! Lifecycle (16.5.1): the leader-elected expiry reaper ([`run_lock_expiry_once`],
//! spawned under `WORKER_LOCK_EXPIRY_REAPER`) flips lapsed `HELD` rows to
//! `EXPIRED` and emits `udb.lock.lock.expired.v1` per lock, transactionally with
//! the flip; independently, the acquire-time quota count excludes lapsed rows so
//! an un-released lock never exhausts the tenant budget between sweeps.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::ir::{
    ComparisonOp, ConflictStrategy, LogicalFilter, LogicalPagination, LogicalProjection,
    LogicalRead, LogicalRecord, LogicalValue,
};
use crate::metrics::{MetricsRecorder, NoopMetrics};
use crate::proto::udb::core::lock::services::v1 as lock_pb;
use crate::proto::udb::core::lock::services::v1::lock_service_server::LockService;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::{ChannelManager, OperationChannel};
use crate::runtime::native_catalog::{NativeModel, native_model};

pub use crate::proto::udb::core::lock::services::v1::lock_service_server::LockServiceServer;

use super::DataBrokerService;
use super::auth_service::events::{ComplianceEnvelope, build_native_compliance_envelope};
use super::native_helpers::{
    NativeEventContext, admit_on as native_admit_on, enqueue_outbox_event_with_context,
    native_service_context, non_empty_json, validate_request_tenant,
};

const LOCK_MSG: &str = "udb.core.lock.entity.v1.Lock";

const TOPIC_ACQUIRED: &str = "udb.lock.lock.acquired.v1";
const TOPIC_RENEWED: &str = "udb.lock.lock.renewed.v1";
const TOPIC_RELEASED: &str = "udb.lock.lock.released.v1";
/// Emitted by the leader-elected expiry reaper when a lapsed HELD row is flipped
/// to EXPIRED (16.5.1).
const TOPIC_EXPIRED: &str = "udb.lock.lock.expired.v1";

const STATUS_HELD: &str = "HELD";
const STATUS_RELEASED: &str = "RELEASED";
/// Terminal state stamped by the expiry reaper on a HELD row whose lease lapsed
/// without a release. The `status` column is a VARCHAR(20); the entity-proto
/// comment currently enumerates only `HELD | RELEASED` (out-of-fence follow-up).
const STATUS_EXPIRED: &str = "EXPIRED";

/// Default lease TTL when the caller does not specify one.
const DEFAULT_LEASE_TTL_SECONDS: i64 = 30;
/// Upper bound on a lease TTL so a caller cannot pin a lock indefinitely.
const MAX_LEASE_TTL_SECONDS: i64 = 3600;
/// Per-tenant active-lock budget. Bounds the durable lock table so one tenant
/// cannot exhaust the shared store; a new acquire beyond this fails closed.
const MAX_ACTIVE_LOCKS_PER_TENANT: usize = 256;
/// Upper bound on rows one expiry-reaper pass claims — bounds the sweep
/// transaction the same way `SCHEDULER_TICK_BATCH` bounds the scheduler tick.
/// `pub(crate)` so the leader spawn site passes the named const (16.11.3).
pub(crate) const LOCK_EXPIRY_SWEEP_BATCH: i64 = 200;
/// Default reaper cadence; overridable via [`LOCK_EXPIRY_INTERVAL_ENV`].
const DEFAULT_LOCK_EXPIRY_INTERVAL_SECS: u64 = 30;
const LOCK_EXPIRY_INTERVAL_ENV: &str = "UDB_LOCK_EXPIRY_INTERVAL_SECS";

/// Reaper cadence for the leader spawn site — env resolved once at spawn,
/// mirroring `cache_service::cache_invalidation_interval`.
pub(crate) fn lock_expiry_interval() -> std::time::Duration {
    // Resolved ONCE via OnceLock (worker-spawn cadence knob), never per-request —
    // matches the sibling native-service startup-config reads.
    static SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let secs = *SECS.get_or_init(|| {
        std::env::var(LOCK_EXPIRY_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_LOCK_EXPIRY_INTERVAL_SECS)
    });
    std::time::Duration::from_secs(secs)
}

/// Postgres-backed `LockService` handler.
pub struct LockServiceImpl {
    /// Outbox-event Postgres pool (the configured native store for `lock`).
    pg_pool: Option<PgPool>,
    /// Runtime handle for the advisory-lease primitive, the monotone fencing-token
    /// source (canonical outbox high-water mark), and typed native-entity dispatch.
    runtime: Option<Arc<DataBrokerRuntime>>,
    /// Configured outbox relation; `None` drops events LOUDLY (error log +
    /// `udb_outbox_enqueue_failures_total`) — see [`Self::emit_lock_event`].
    outbox_relation: Option<String>,
    /// Shared per-tenant fair-admission manager (same one the data plane uses).
    channels: Option<ChannelManager>,
    metrics: Arc<dyn MetricsRecorder>,
}

fn lock_capability_status(
    operation: &'static str,
    capability_required: &'static str,
    message: &'static str,
) -> Status {
    crate::runtime::executor_utils::capability_status(
        "lock",
        operation,
        capability_required,
        message,
    )
}

fn lock_internal_status(operation: impl Into<String>, message: impl Into<String>) -> Status {
    crate::runtime::executor_utils::internal_status("lock", operation, message)
}

/// Fail-closed refusal when the monotone fencing-token source is unavailable
/// (no canonical store registered, or the counter read failed). Wall-clock
/// seconds are NOT an acceptable fallback: they collide within a second and
/// regress across clock steps, so a time-derived token could duplicate or
/// undercut an already-issued token and let a fenced-off holder write again.
fn fencing_token_unavailable_status() -> Status {
    lock_capability_status(
        "lock_fencing",
        "canonical_store_monotone_counter",
        "lock fencing requires the canonical-store monotone token source; \
         refusing to mint a wall-clock fencing token",
    )
}

fn lock_policy_status(
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    lock_policy_status_with_code(
        tonic::Code::FailedPrecondition,
        operation,
        policy_decision_id,
        message,
    )
}

fn lock_policy_status_with_code(
    code: tonic::Code,
    operation: &'static str,
    policy_decision_id: &'static str,
    message: impl Into<String>,
) -> Status {
    crate::runtime::executor_utils::policy_status_with_code(
        code,
        operation,
        policy_decision_id,
        message,
    )
}

fn lock_not_held_status(operation: &'static str) -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::NotFound,
        "lock",
        operation,
        "lock_not_held",
        "lock not held",
    )
}

fn lock_already_held_status() -> Status {
    crate::runtime::executor_utils::schema_status(
        tonic::Code::AlreadyExists,
        "lock",
        "acquire_lock",
        "lock_already_held",
        "lock is already held by another owner",
    )
}

impl LockServiceImpl {
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

    /// Lock state is durable-only: fail closed when no canonical/PG store exists.
    fn require_runtime(&self) -> Result<&DataBrokerRuntime, Status> {
        self.runtime.as_deref().ok_or_else(|| {
            lock_capability_status(
                "native_entity_dispatch",
                "runtime_native_entity_dispatch",
                "lock service requires runtime native-entity dispatch (no runtime configured)",
            )
        })
    }
}

impl Default for LockServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

/// The mutual-exclusion lease name. Tenant-prefixed from the VERIFIED claim so two
/// tenants can hold the same logical name without colliding, and so a caller can
/// never serialize against another tenant's lock.
fn lease_name(tenant_id: &str, lock_name: &str) -> String {
    format!("app_lock:{tenant_id}:{lock_name}")
}

/// Clamp a requested TTL into `[DEFAULT, MAX]`; non-positive → default.
fn resolve_ttl_seconds(requested: i32) -> i64 {
    let requested = i64::from(requested);
    if requested <= 0 {
        DEFAULT_LEASE_TTL_SECONDS
    } else {
        requested.min(MAX_LEASE_TTL_SECONDS)
    }
}

fn validate_lock_identity(lock_name: &str, owner_id: &str) -> Result<(String, String), Status> {
    let lock_name = lock_name.trim();
    let owner_id = owner_id.trim();
    let mut fields = Vec::new();
    if lock_name.is_empty() {
        fields.push(("lock_name", "must be a non-empty lock name"));
    }
    if owner_id.is_empty() {
        fields.push(("owner_id", "must be a non-empty owner id"));
    }
    if !fields.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "lock_name and owner_id are required",
            fields,
        ));
    }
    Ok((lock_name.to_string(), owner_id.to_string()))
}

/// Reject a stale fencing token: a holder presenting a token lower than the
/// stored one has been fenced off (a newer holder superseded it). Equal or
/// greater is accepted (the current holder). Pure — unit-tested without PG.
fn ensure_fencing_token_fresh(provided: i64, stored: i64) -> Result<(), Status> {
    if provided < stored {
        return Err(stale_fencing_token_status(provided, stored));
    }
    Ok(())
}

fn stale_fencing_token_status(provided: i64, stored: i64) -> Status {
    lock_policy_status(
        "lock_fencing",
        "stale_fencing_token",
        format!("stale fencing token {provided}; the lock has been fenced to token {stored}"),
    )
}

fn lock_lease_lost_status() -> Status {
    lock_policy_status(
        "renew_lock",
        "lock_lease_lost",
        "lock lease lost; it is now held by another owner",
    )
}

fn lock_held_by_different_owner_status(operation: &'static str) -> Status {
    lock_policy_status_with_code(
        tonic::Code::PermissionDenied,
        operation,
        "lock_owner_mismatch",
        "lock is held by a different owner",
    )
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn logical_string(value: impl Into<String>) -> LogicalValue {
    LogicalValue::String(value.into())
}

fn lock_filter(tenant_id: &str, lock_name: Option<&str>, status: Option<&str>) -> LogicalFilter {
    let mut filters = vec![LogicalFilter::Comparison {
        field: "tenant_id".to_string(),
        op: ComparisonOp::Eq,
        value: logical_string(tenant_id),
    }];
    if let Some(lock_name) = lock_name {
        filters.push(LogicalFilter::Comparison {
            field: "lock_name".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(lock_name),
        });
    }
    if let Some(status) = status {
        filters.push(LogicalFilter::Comparison {
            field: "status".to_string(),
            op: ComparisonOp::Eq,
            value: logical_string(status),
        });
    }
    LogicalFilter::And(filters)
}

fn lock_read_by_name(tenant_id: &str, lock_name: &str) -> LogicalRead {
    LogicalRead {
        message_type: LOCK_MSG.to_string(),
        filter: Some(lock_filter(tenant_id, Some(lock_name), None)),
        projection: Some(LogicalProjection::fields([
            "lock_id".to_string(),
            "owner_id".to_string(),
            "fencing_token".to_string(),
            "status".to_string(),
        ])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(1)),
    }
}

/// Quota-count read: HELD rows whose lease has NOT lapsed. A lock past its
/// `expires_at` no longer excludes other holders (the advisory lease is gone),
/// so it must not exhaust `MAX_ACTIVE_LOCKS_PER_TENANT` between reaper sweeps
/// (16.5.1). The mediated PG path binds `LogicalValue::Timestamp` with a
/// `$N::TIMESTAMPTZ` cast, so the time comparison is expressible in the filter —
/// no in-memory post-filtering needed.
fn held_locks_read(tenant_id: &str, limit: u32, now: DateTime<Utc>) -> LogicalRead {
    LogicalRead {
        message_type: LOCK_MSG.to_string(),
        filter: Some(LogicalFilter::And(vec![
            lock_filter(tenant_id, None, Some(STATUS_HELD)),
            LogicalFilter::Comparison {
                field: "expires_at".to_string(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Timestamp(now),
            },
        ])),
        projection: Some(LogicalProjection::fields(["lock_id".to_string()])),
        sort: Vec::new(),
        include: Vec::new(),
        pagination: Some(LogicalPagination::limit(limit)),
    }
}

#[allow(clippy::too_many_arguments)]
fn lock_record(
    lock_id: &str,
    tenant_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
    ttl_seconds: i64,
    status: &str,
    acquired_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    metadata_json: &str,
) -> LogicalRecord {
    let mut record = LogicalRecord::new();
    record.insert("lock_id".to_string(), logical_string(lock_id));
    record.insert("tenant_id".to_string(), logical_string(tenant_id));
    record.insert("lock_name".to_string(), logical_string(lock_name));
    record.insert("owner_id".to_string(), logical_string(owner_id));
    record.insert(
        "fencing_token".to_string(),
        LogicalValue::Int(fencing_token),
    );
    record.insert(
        "lease_ttl_seconds".to_string(),
        LogicalValue::Int(ttl_seconds),
    );
    record.insert("status".to_string(), logical_string(status));
    record.insert(
        "acquired_at".to_string(),
        LogicalValue::Timestamp(acquired_at),
    );
    record.insert(
        "expires_at".to_string(),
        LogicalValue::Timestamp(expires_at),
    );
    record.insert("metadata_json".to_string(), logical_string(metadata_json));
    record
}

/// The mutable columns an upsert may overwrite on re-grant/renew/release. The
/// conflict target is the message primary key (`lock_id`); the same `lock_id` is
/// reused for an existing (tenant, lock_name) row so a re-acquire never violates
/// the unique index.
fn lock_conflict() -> ConflictStrategy {
    ConflictStrategy::update(vec![
        "owner_id".to_string(),
        "fencing_token".to_string(),
        "lease_ttl_seconds".to_string(),
        "status".to_string(),
        "acquired_at".to_string(),
        "expires_at".to_string(),
        "metadata_json".to_string(),
    ])
}

fn lock_json_object(row: &serde_json::Value) -> &serde_json::Map<String, serde_json::Value> {
    row.get("n")
        .and_then(serde_json::Value::as_object)
        .or_else(|| row.as_object())
        .unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<serde_json::Map<String, serde_json::Value>> =
                std::sync::OnceLock::new();
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

fn json_i64(row: &serde_json::Map<String, serde_json::Value>, key: &str) -> i64 {
    match row.get(key) {
        Some(serde_json::Value::Number(value)) => value.as_i64().unwrap_or(0),
        Some(serde_json::Value::String(value)) => value.trim().parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

/// A durable lock row decoded from the native read JSON.
struct StoredLock {
    lock_id: String,
    owner_id: String,
    fencing_token: i64,
    status: String,
}

fn stored_lock_from_json(row: &serde_json::Value) -> StoredLock {
    let map = lock_json_object(row);
    StoredLock {
        lock_id: json_str(map, "lock_id"),
        owner_id: json_str(map, "owner_id"),
        fencing_token: json_i64(map, "fencing_token"),
        status: json_str(map, "status"),
    }
}

/// The domain payload every `udb.lock.*` event carries — shared between the
/// RPC emit path ([`LockServiceImpl::emit_lock_event`]) and the expiry reaper
/// ([`run_lock_expiry_once`]) so the two lanes never drift.
fn lock_event_payload(
    tenant_id: &str,
    project_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": tenant_id,
        "project_id": project_id,
        "lock_name": lock_name,
        "owner_id": owner_id,
        "fencing_token": fencing_token,
    })
}

impl LockServiceImpl {
    /// Source the next monotone fencing token from the canonical outbox high-water
    /// mark (the same counter write receipts advance), so each successive grant —
    /// including a takeover after the prior holder's lease expired — receives a
    /// strictly greater token. Fail closed when no canonical store is registered
    /// or the counter read fails (16.5.2): the old wall-clock fallback collided
    /// within a second and regressed across clock steps, breaking the fencing
    /// guarantee the token exists to provide.
    async fn next_fencing_token(&self, runtime: &DataBrokerRuntime) -> Result<i64, Status> {
        let Some(store) = runtime.default_system_stores() else {
            return Err(fencing_token_unavailable_status());
        };
        match store.outbox_max_seq().await {
            Ok(seq) => Ok(seq.saturating_add(1)),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "lock fencing-token source read failed; refusing to grant"
                );
                Err(fencing_token_unavailable_status())
            }
        }
    }

    /// Emit a per-mutation versioned dot-topic outbox event.
    ///
    /// Delivery contract — at-least-once-minus (16.5.3): the durable lock row has
    /// already committed when this runs and the outbox insert is a SEPARATE
    /// statement, so a crash (or a missing pool/relation) between the two loses
    /// the event while keeping the state change. Drops are never silent: both
    /// local drop paths log at error level with the lock id/topic and count in
    /// `udb_outbox_enqueue_failures_total{path="native"}`; an insert failure
    /// inside the shared enqueue helper records the same counter (it does not
    /// surface a `Result` to this call site). A true transactional outbox for
    /// mediated entity writes is follow-up 16.12.4.
    #[allow(clippy::too_many_arguments)]
    async fn emit_lock_event(
        &self,
        topic: &str,
        partition_key: &str,
        tenant_id: &str,
        project_id: &str,
        lock_id: &str,
        lock_name: &str,
        owner_id: &str,
        fencing_token: i64,
    ) {
        let Some(pool) = self.pg_pool.as_ref() else {
            tracing::error!(
                topic,
                lock_id,
                lock_name,
                tenant_id,
                "lock event dropped: no outbox Postgres pool configured for the lock native store"
            );
            self.metrics.inc_outbox_enqueue_failures_total("native");
            return;
        };
        if self.outbox_relation.is_none() {
            tracing::error!(
                topic,
                lock_id,
                lock_name,
                tenant_id,
                "lock event dropped: no outbox relation configured"
            );
            self.metrics.inc_outbox_enqueue_failures_total("native");
            return;
        }
        enqueue_outbox_event_with_context(
            pool,
            self.outbox_relation.as_deref(),
            topic,
            partition_key,
            tenant_id,
            project_id,
            lock_event_payload(tenant_id, project_id, lock_name, owner_id, fencing_token),
            NativeEventContext {
                target_resource: lock_name.to_string(),
                ..NativeEventContext::default()
            },
            Some(&self.metrics),
        )
        .await;
    }
}

#[tonic::async_trait]
impl LockService for LockServiceImpl {
    async fn acquire_lock(
        &self,
        request: Request<lock_pb::AcquireLockRequest>,
    ) -> Result<Response<lock_pb::AcquireLockResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        // Cross-tenant guard FIRST: the body tenant_id must match the verified
        // claim/header. After this passes, the body value IS the verified tenant,
        // so the lease name is derived from the verified claim, never raw body.
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let (lock_name, owner_id) = validate_lock_identity(&req.lock_name, &req.owner_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "lock",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let ttl_seconds = resolve_ttl_seconds(req.lease_ttl_seconds);

        // Existing durable row (if any) for this (tenant, lock_name).
        let existing = runtime
            .native_entity_read_for_service(
                "lock",
                &context,
                lock_read_by_name(&tenant_id, &lock_name),
            )
            .await?
            .first()
            .map(stored_lock_from_json);

        // Per-tenant quota: only a brand-new lock counts against the budget; a
        // re-acquire of an existing row is in-place.
        if existing.is_none() {
            let held = runtime
                .native_entity_read_for_service(
                    "lock",
                    &context,
                    held_locks_read(
                        &tenant_id,
                        (MAX_ACTIVE_LOCKS_PER_TENANT as u32) + 1,
                        Utc::now(),
                    ),
                )
                .await?;
            if held.len() >= MAX_ACTIVE_LOCKS_PER_TENANT {
                return Err(crate::runtime::executor_utils::quota_refusal_status(
                    "lock",
                    "tenant active-lock quota",
                    format!("tenant active-lock quota exhausted ({MAX_ACTIVE_LOCKS_PER_TENANT})"),
                ));
            }
        }

        // Mutual exclusion: the portable advisory lease. A same-owner refresh or an
        // expired-lease takeover returns true; a different live owner returns false.
        let lease = lease_name(&tenant_id, &lock_name);
        let acquired = runtime
            .try_acquire_native_lease(&lease, &owner_id, Duration::from_secs(ttl_seconds as u64))
            .await?;
        if !acquired {
            return Err(lock_already_held_status());
        }

        let fencing_token = self.next_fencing_token(runtime).await?;
        let lock_id = existing
            .as_ref()
            .map(|row| row.lock_id.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let acquired_at = Utc::now();
        let expires_at = acquired_at + chrono::Duration::seconds(ttl_seconds);
        let metadata_json = non_empty_json(&req.metadata_json);

        runtime
            .native_entity_write_for_service(
                "lock",
                &context,
                LOCK_MSG,
                lock_record(
                    &lock_id,
                    &tenant_id,
                    &lock_name,
                    &owner_id,
                    fencing_token,
                    ttl_seconds,
                    STATUS_HELD,
                    acquired_at,
                    expires_at,
                    &metadata_json,
                ),
                lock_conflict(),
            )
            .await?;

        self.emit_lock_event(
            TOPIC_ACQUIRED,
            &lease,
            &tenant_id,
            &context.project_id,
            &lock_id,
            &lock_name,
            &owner_id,
            fencing_token,
        )
        .await;

        Ok(Response::new(lock_pb::AcquireLockResponse {
            acquired: true,
            fencing_token,
            lock_name,
            expires_at_unix: now_unix() + ttl_seconds,
            message: "lock acquired".to_string(),
            error: None,
        }))
    }

    async fn renew_lock(
        &self,
        request: Request<lock_pb::RenewLockRequest>,
    ) -> Result<Response<lock_pb::RenewLockResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let (lock_name, owner_id) = validate_lock_identity(&req.lock_name, &req.owner_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "lock",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");
        let ttl_seconds = resolve_ttl_seconds(req.lease_ttl_seconds);

        let stored = runtime
            .native_entity_read_for_service(
                "lock",
                &context,
                lock_read_by_name(&tenant_id, &lock_name),
            )
            .await?
            .first()
            .map(stored_lock_from_json)
            .filter(|row| row.status == STATUS_HELD)
            .ok_or_else(|| lock_not_held_status("renew_lock"))?;
        if stored.owner_id != owner_id {
            return Err(lock_held_by_different_owner_status("renew_lock"));
        }
        // Fence: a stale token cannot renew.
        ensure_fencing_token_fresh(req.fencing_token, stored.fencing_token)?;

        // Refresh the advisory lease (same owner refreshes its own live row).
        let lease = lease_name(&tenant_id, &lock_name);
        let refreshed = runtime
            .try_acquire_native_lease(&lease, &owner_id, Duration::from_secs(ttl_seconds as u64))
            .await?;
        if !refreshed {
            return Err(lock_lease_lost_status());
        }

        let acquired_at = Utc::now();
        let expires_at = acquired_at + chrono::Duration::seconds(ttl_seconds);
        // Renew keeps the same fencing token (no new grant).
        runtime
            .native_entity_write_for_service(
                "lock",
                &context,
                LOCK_MSG,
                lock_record(
                    &stored.lock_id,
                    &tenant_id,
                    &lock_name,
                    &owner_id,
                    stored.fencing_token,
                    ttl_seconds,
                    STATUS_HELD,
                    acquired_at,
                    expires_at,
                    "{}",
                ),
                lock_conflict(),
            )
            .await?;

        self.emit_lock_event(
            TOPIC_RENEWED,
            &lease,
            &tenant_id,
            &context.project_id,
            &stored.lock_id,
            &lock_name,
            &owner_id,
            stored.fencing_token,
        )
        .await;

        Ok(Response::new(lock_pb::RenewLockResponse {
            renewed: true,
            fencing_token: stored.fencing_token,
            expires_at_unix: now_unix() + ttl_seconds,
            message: "lock renewed".to_string(),
            error: None,
        }))
    }

    async fn release_lock(
        &self,
        request: Request<lock_pb::ReleaseLockRequest>,
    ) -> Result<Response<lock_pb::ReleaseLockResponse>, Status> {
        let metadata = request.metadata().clone();
        let req = request.into_inner();
        validate_request_tenant(&metadata, &req.tenant_id)?;
        let tenant_id = req.tenant_id.trim().to_string();
        let (lock_name, owner_id) = validate_lock_identity(&req.lock_name, &req.owner_id)?;
        let _admit = native_admit_on(
            self.channels.as_ref(),
            &self.metrics,
            "lock",
            OperationChannel::Admin,
            &tenant_id,
            None,
        )
        .await?;
        let runtime = self.require_runtime()?;
        let context = native_service_context(&metadata, &tenant_id, "");

        let stored = runtime
            .native_entity_read_for_service(
                "lock",
                &context,
                lock_read_by_name(&tenant_id, &lock_name),
            )
            .await?
            .first()
            .map(stored_lock_from_json);
        let Some(stored) = stored.filter(|row| row.status == STATUS_HELD) else {
            // Idempotent: nothing to release.
            return Ok(Response::new(lock_pb::ReleaseLockResponse {
                released: true,
                message: "lock not held".to_string(),
                error: None,
            }));
        };
        if stored.owner_id != owner_id {
            return Err(lock_held_by_different_owner_status("release_lock"));
        }
        ensure_fencing_token_fresh(req.fencing_token, stored.fencing_token)?;

        let lease = lease_name(&tenant_id, &lock_name);
        runtime.release_native_lease(&lease, &owner_id).await;

        let now = Utc::now();
        runtime
            .native_entity_write_for_service(
                "lock",
                &context,
                LOCK_MSG,
                lock_record(
                    &stored.lock_id,
                    &tenant_id,
                    &lock_name,
                    &owner_id,
                    stored.fencing_token,
                    0,
                    STATUS_RELEASED,
                    now,
                    now,
                    "{}",
                ),
                lock_conflict(),
            )
            .await?;

        self.emit_lock_event(
            TOPIC_RELEASED,
            &lease,
            &tenant_id,
            &context.project_id,
            &stored.lock_id,
            &lock_name,
            &owner_id,
            stored.fencing_token,
        )
        .await;

        Ok(Response::new(lock_pb::ReleaseLockResponse {
            released: true,
            message: "lock released".to_string(),
            error: None,
        }))
    }
}

// ── leader-elected expiry reaper (16.5.1) ─────────────────────────────────────

/// Manifest-derived model for the durable lock table, so the reaper SQL below
/// follows the same single-source-of-truth rule as the scheduler tick (no
/// hand-maintained schema copies).
fn lock_model() -> NativeModel {
    native_model(
        LOCK_MSG,
        &[
            "lock_id",
            "tenant_id",
            "lock_name",
            "owner_id",
            "fencing_token",
            "status",
            "expires_at",
        ],
    )
}

/// The claim-and-flip statement the expiry reaper runs: lapsed HELD rows are
/// claimed with `FOR UPDATE SKIP LOCKED` (two leaders can never double-expire
/// the same row) and flipped to EXPIRED in the same statement, returning the
/// identifying columns the `udb.lock.lock.expired.v1` event needs. Bind order:
/// `$1` = EXPIRED (flip target), `$2` = HELD (claim filter), `$3` = batch bound.
/// Exposed (and unit-tested) so the no-double-expire contract is asserted on
/// the rendered SQL, mirroring `scheduler_service::due_jobs_claim_sql`.
pub(crate) fn expired_locks_claim_sql(m: &NativeModel) -> String {
    format!(
        "UPDATE {rel} SET {status} = $1 \
         WHERE {lock_id} IN ( \
            SELECT {lock_id} FROM {rel} \
            WHERE {status} = $2 AND {expires_at} < NOW() \
            ORDER BY {expires_at} \
            LIMIT $3 \
            FOR UPDATE SKIP LOCKED) \
         RETURNING {lock_id}::text AS lock_id, {tenant_id}::text AS tenant_id, \
            {lock_name} AS lock_name, {owner_id} AS owner_id, \
            {fencing_token} AS fencing_token",
        rel = m.relation,
        lock_id = m.q("lock_id"),
        tenant_id = m.q("tenant_id"),
        lock_name = m.q("lock_name"),
        owner_id = m.q("owner_id"),
        fencing_token = m.q("fencing_token"),
        status = m.q("status"),
        expires_at = m.q("expires_at"),
    )
}

/// Insert ONE `udb.lock.lock.expired.v1` outbox row inside the sweep transaction
/// (transactional outbox — the flip and its declared event commit atomically),
/// using the SAME shared compliance envelope the auth/native lanes emit so the
/// CDC tailer decodes it identically. The actor is the lock reaper (a system
/// principal), not an end user. Mirrors `scheduler_service::insert_tick_outbox`.
async fn insert_lock_expired_outbox(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    relation: &str,
    lock_id: &str,
    tenant_id: &str,
    lock_name: &str,
    owner_id: &str,
    fencing_token: i64,
) -> Result<(), Status> {
    let env = ComplianceEnvelope {
        actor: "udb:lock".to_string(),
        operation: "expired".to_string(),
        outcome: "success".to_string(),
        auth_method: "system".to_string(),
        target_resource: lock_name.to_string(),
        ..ComplianceEnvelope::default()
    };
    let event_id = Uuid::new_v4();
    // Same partition key the RPC-side lock events use, so per-lock ordering holds
    // across acquire/renew/release/expire.
    let partition_key = lease_name(tenant_id, lock_name);
    let envelope = build_native_compliance_envelope(
        &event_id.to_string(),
        TOPIC_EXPIRED,
        &partition_key,
        tenant_id,
        "", // locks carry no project binding
        &env,
        lock_id, // correlation id
        "none",
        1,
        &[],
        lock_event_payload(tenant_id, "", lock_name, owner_id, fencing_token),
    );
    crate::runtime::cdc::insert_outbox_row(
        &mut **tx,
        relation,
        event_id,
        TOPIC_EXPIRED,
        &partition_key,
        &envelope,
    )
    .await
    .map_err(|err| {
        lock_internal_status(
            "lock_expiry_outbox_insert",
            format!("lock expiry outbox insert failed: {err}"),
        )
    })
}

/// One expiry-reaper pass (leader-elected by the caller — 16.11.3 spawns it
/// under `WORKER_LOCK_EXPIRY_REAPER`). Flips up to `batch_size` lapsed HELD rows
/// to EXPIRED and — within the SAME transaction — durably enqueues one
/// `udb.lock.lock.expired.v1` outbox row per expired lock, so a lapsed lock can
/// never permanently exhaust `MAX_ACTIVE_LOCKS_PER_TENANT` and every expiry is
/// at-least-once via the outbox→CDC pipeline. The advisory-lease primitive needs
/// no sweep of its own: a lapsed lease is superseded atomically at acquire time.
///
/// The sweep is intentionally cross-tenant system work: the native-store pool
/// connects as the table owner, which `enable_rls` (not FORCE) exempts from the
/// tenant RLS policy — the same posture as the scheduler tick.
///
/// Returns the number of locks expired. Fail closed: a missing outbox relation
/// yields `Ok(0)` (nothing flips) rather than expiring without the declared
/// event; an outbox insert failure rolls back the whole batch.
pub(crate) async fn run_lock_expiry_once(
    pool: &PgPool,
    outbox_relation: Option<&str>,
    batch_size: i64,
) -> Result<i64, Status> {
    let Some(outbox_rel) = outbox_relation else {
        tracing::warn!("lock expiry: no outbox relation configured; cannot expire locks");
        return Ok(0);
    };
    let m = lock_model();
    let claim_sql = expired_locks_claim_sql(&m);
    let batch = batch_size.clamp(1, LOCK_EXPIRY_SWEEP_BATCH);

    let mut tx = pool.begin().await.map_err(|err| {
        lock_internal_status(
            "lock_expiry_begin",
            format!("lock expiry begin failed: {err}"),
        )
    })?;
    let rows = sqlx::query(&claim_sql)
        .bind(STATUS_EXPIRED)
        .bind(STATUS_HELD)
        .bind(batch)
        .fetch_all(&mut *tx)
        .await
        .map_err(|err| {
            lock_internal_status(
                "lock_expiry_claim",
                format!("lock expiry claim failed: {err}"),
            )
        })?;

    let mut expired = 0i64;
    for row in &rows {
        let get = |c: &str| -> Result<String, Status> {
            row.try_get::<String, _>(c).map_err(|e| {
                lock_internal_status(
                    "lock_expiry_decode",
                    format!("lock expiry decode {c} failed: {e}"),
                )
            })
        };
        let lock_id = get("lock_id")?;
        let tenant_id = get("tenant_id")?;
        let lock_name = get("lock_name")?;
        let owner_id = get("owner_id")?;
        let fencing_token: i64 = row.try_get("fencing_token").map_err(|e| {
            lock_internal_status(
                "lock_expiry_decode",
                format!("lock expiry decode fencing_token: {e}"),
            )
        })?;
        insert_lock_expired_outbox(
            &mut tx,
            outbox_rel,
            &lock_id,
            &tenant_id,
            &lock_name,
            &owner_id,
            fencing_token,
        )
        .await?;
        expired += 1;
    }

    tx.commit().await.map_err(|err| {
        lock_internal_status(
            "lock_expiry_commit",
            format!("lock expiry commit failed: {err}"),
        )
    })?;
    Ok(expired)
}

#[cfg(test)]
mod lock_scope_tests {
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

    fn assert_policy_detail(status: &Status, operation: &str, policy_decision_id: &str) {
        let detail = decode_detail(status);
        assert_eq!(detail.kind, ErrorKind::Policy as i32);
        assert_eq!(detail.operation, operation);
        assert_eq!(detail.policy_decision_id, policy_decision_id);
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// A caller scoped to tenant-a must not target tenant-b's lock by putting a
    /// foreign tenant_id in the request BODY; the scope guard rejects this before
    /// any lease/store access (no Postgres needed) — mirrors `tenant_service`.
    #[tokio::test]
    async fn acquire_lock_rejects_cross_tenant_body() {
        let svc = LockServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(lock_pb::AcquireLockRequest {
            tenant_id: "tenant-b".to_string(),
            lock_name: "orders".to_string(),
            owner_id: "worker-1".to_string(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .acquire_lock(request)
            .await
            .expect_err("cross-tenant body must be rejected");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn acquire_lock_missing_identity_carries_field_violations() {
        let svc = LockServiceImpl::new(); // no runtime, no channels (admit no-op)
        let mut request = Request::new(lock_pb::AcquireLockRequest {
            tenant_id: "tenant-a".to_string(),
            lock_name: "  ".to_string(),
            owner_id: String::new(),
            ..Default::default()
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", MetadataValue::from_static("tenant-a"));
        let err = svc
            .acquire_lock(request)
            .await
            .expect_err("missing lock identity must be rejected before runtime access");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), "lock_name and owner_id are required");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Validation as i32);
        assert_eq!(detail.field_violations.len(), 2);
        assert_eq!(detail.field_violations[0].field, "lock_name");
        assert_eq!(
            detail.field_violations[0].description,
            "must be a non-empty lock name"
        );
        assert_eq!(detail.field_violations[1].field, "owner_id");
        assert_eq!(
            detail.field_violations[1].description,
            "must be a non-empty owner id"
        );
    }

    /// A stale (lower) fencing token is fenced off; an equal or greater token from
    /// the current holder is accepted. Pure check — no PG.
    #[test]
    fn stale_fencing_token_is_rejected() {
        // Holder advanced to token 7; a partitioned caller still on 5 is fenced.
        let err = ensure_fencing_token_fresh(5, 7).expect_err("stale token must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "stale fencing token 5; the lock has been fenced to token 7"
        );
        assert_policy_detail(&err, "lock_fencing", "stale_fencing_token");
        // The current holder (equal) and a future token (greater) pass.
        ensure_fencing_token_fresh(7, 7).expect("current token must pass");
        ensure_fencing_token_fresh(8, 7).expect("newer token must pass");
    }

    #[test]
    fn lock_lease_lost_carries_policy_detail() {
        let err = lock_lease_lost_status();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "lock lease lost; it is now held by another owner"
        );
        assert_policy_detail(&err, "renew_lock", "lock_lease_lost");
    }

    #[test]
    fn lock_owner_mismatch_carries_permission_denied_policy_detail() {
        for operation in ["renew_lock", "release_lock"] {
            let err = lock_held_by_different_owner_status(operation);
            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            assert_eq!(err.message(), "lock is held by a different owner");
            assert_policy_detail(&err, operation, "lock_owner_mismatch");
        }
    }

    #[test]
    fn lock_not_held_carries_schema_detail() {
        let err = lock_not_held_status("renew_lock");
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert_eq!(err.message(), "lock not held");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "lock");
        assert_eq!(detail.operation, "renew_lock");
        assert_eq!(detail.capability_required, "lock_not_held");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    #[test]
    fn lock_already_held_carries_schema_detail() {
        let err = lock_already_held_status();
        assert_eq!(err.code(), tonic::Code::AlreadyExists);
        assert_eq!(err.message(), "lock is already held by another owner");
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Schema as i32);
        assert_eq!(detail.backend, "lock");
        assert_eq!(detail.operation, "acquire_lock");
        assert_eq!(detail.capability_required, "lock_already_held");
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// 16.5.1a — the quota-count read must exclude rows whose lease already
    /// lapsed: a lapsed lock no longer excludes anyone, so counting it toward
    /// `MAX_ACTIVE_LOCKS_PER_TENANT` would lock the tenant out between sweeps.
    /// Pure filter-shape assertion (no PG).
    #[test]
    fn held_locks_quota_read_excludes_expired_rows() {
        let now = Utc::now();
        let read = held_locks_read("tenant-a", (MAX_ACTIVE_LOCKS_PER_TENANT as u32) + 1, now);
        let expected = LogicalFilter::And(vec![
            lock_filter("tenant-a", None, Some(STATUS_HELD)),
            LogicalFilter::Comparison {
                field: "expires_at".to_string(),
                op: ComparisonOp::Gt,
                value: LogicalValue::Timestamp(now),
            },
        ]);
        assert_eq!(read.filter, Some(expected));
    }

    /// 16.5.2 — with no canonical store (or a failed counter read) the fencing
    /// path must return this typed capability refusal, never a wall-clock token:
    /// `next_fencing_token`'s only non-store exit is
    /// `fencing_token_unavailable_status()` (the `now_unix()` fallback is gone).
    #[test]
    fn missing_fencing_token_source_returns_typed_refusal_not_a_token() {
        let err = fencing_token_unavailable_status();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "lock");
        assert_eq!(detail.operation, "lock_fencing");
        assert_eq!(
            detail.capability_required,
            "canonical_store_monotone_counter"
        );
        assert!(!detail.retryable);
        assert_eq!(detail.retry_after_ms, 0);
    }

    /// 16.5.1b — the expiry-claim SQL must claim with `FOR UPDATE SKIP LOCKED`
    /// (two leaders never double-expire the same row), flip only lapsed HELD
    /// rows, bound the batch, and return the columns the expired event needs.
    #[test]
    fn expired_locks_claim_sql_shape() {
        let sql = expired_locks_claim_sql(&lock_model());
        assert!(
            sql.starts_with("UPDATE"),
            "flip and claim in one statement: {sql}"
        );
        assert!(
            sql.contains("FOR UPDATE SKIP LOCKED"),
            "no-double-expire: {sql}"
        );
        assert!(sql.contains("\"status\" = $1"), "flip target bind: {sql}");
        assert!(
            sql.contains("\"status\" = $2"),
            "HELD claim filter bind: {sql}"
        );
        assert!(
            sql.contains("\"expires_at\" < NOW()"),
            "lapsed-only filter: {sql}"
        );
        assert!(sql.contains("LIMIT $3"), "bounded batch: {sql}");
        for column in [
            "RETURNING",
            "lock_id",
            "tenant_id",
            "lock_name",
            "owner_id",
            "fencing_token",
        ] {
            assert!(sql.contains(column), "event needs {column}: {sql}");
        }
    }

    #[test]
    fn lock_missing_runtime_capability_carries_typed_detail() {
        let err = lock_capability_status(
            "native_entity_dispatch",
            "runtime_native_entity_dispatch",
            "lock service requires runtime native-entity dispatch (no runtime configured)",
        );
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            err.message(),
            "lock service requires runtime native-entity dispatch (no runtime configured)"
        );
        let detail = decode_detail(&err);
        assert_eq!(detail.kind, ErrorKind::Capability as i32);
        assert_eq!(detail.backend, "lock");
        assert_eq!(detail.operation, "native_entity_dispatch");
        assert_eq!(detail.capability_required, "runtime_native_entity_dispatch");
        assert!(!detail.retryable);
    }
}

impl DataBrokerService {
    /// Build the native `LockService`, wired to the broker's Postgres pool, the
    /// advisory-lease/fencing-token runtime, and the shared outbox.
    pub(crate) fn build_lock_service(&self) -> LockServiceImpl {
        let runtime = self.runtime.load_full();
        // Native-service persistence resolves through the discovery seam: the
        // backend is read from this service's proto `native_service` binding, then a
        // health/weight-routed instance is chosen — not the process-global pool.
        let pg_pool = runtime.native_store_pool_for_service("lock", true, "").ok();
        let outbox = runtime.config().cdc.outbox_relation();
        let channels = Some(runtime.channels().clone());
        LockServiceImpl::new()
            .with_postgres(pg_pool)
            .with_runtime(Some(runtime))
            .with_outbox(Some(outbox))
            .with_channels(channels)
            .with_metrics(self.metrics.clone())
    }
}
