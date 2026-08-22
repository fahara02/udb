//! The five `LockService` RPC handlers (acquire/renew/release/get/list) plus the
//! monotone fencing-token source, extracted from the trait impl as free
//! `pub(crate) async fn`s taking `svc` where the trait method took `&self`.
//! `mod.rs` delegates one line to each. Bodies are verbatim — the same admission,
//! cross-tenant guard, advisory-lease mutual exclusion, fencing, durable write,
//! and outbox emission as the former god file.

use std::time::Duration;

use chrono::Utc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::udb::core::lock::services::v1 as lock_pb;
use crate::runtime::DataBrokerRuntime;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{
    admit_on as native_admit_on, native_next_page_token, native_offset_page_window, non_empty_json,
    project_scoped_native_service_context, validate_request_tenant,
};
use super::LockServiceImpl;
use super::config::{
    DEFAULT_LOCK_LIST_LIMIT, LOCK_MSG, MAX_ACTIVE_LOCKS_PER_TENANT, STATUS_HELD, STATUS_RELEASED,
    TOPIC_ACQUIRED, TOPIC_RELEASED, TOPIC_RENEWED, resolve_ttl_seconds,
};
use super::errors::{
    ensure_fencing_token_fresh, lock_already_held_status, lock_held_by_different_owner_status,
    lock_lease_lost_status, lock_not_held_status, validate_lock_identity,
};
use super::events::{emit_lock_event, lock_event_transaction_op};
use super::model::{lease_lapsed, lease_name, lock_dto_from_json, now_unix, stored_lock_from_json};
use super::store::{
    held_locks_read, lock_conflict, lock_inventory_read, lock_read_by_name, lock_record,
};

/// Allocate the next fencing token for THIS lock — per-lock, strictly monotone,
/// and independent of the outbox high-water mark.
///
/// The token lives on the durable lock row and is bumped from the row's own
/// last value: the successor is `current + 1`. This is read UNDER the freshly
/// (re)held advisory lease, which serializes acquirers, so the read+bump is
/// atomic for this lock — the prior stale scheme (`outbox_max_seq()+1`) could
/// hand the next holder the SAME token when the acquire event dropped and the
/// global counter was quiescent, defeating split-brain fencing. Because the
/// durable row is committed before any outbox emit, a dropped event can never
/// let a successor reuse a token.
///
/// Returns the granted token plus the fresh row read (its `lock_id` is reused so
/// a re-acquire keeps the same primary key).
async fn allocate_lock_fencing_token(
    runtime: &DataBrokerRuntime,
    context: &crate::RequestContext,
    tenant_id: &str,
    lock_name: &str,
) -> Result<(i64, Option<super::model::StoredLock>), Status> {
    let current = runtime
        .native_entity_read_for_service("lock", context, lock_read_by_name(tenant_id, lock_name))
        .await?
        .first()
        .map(stored_lock_from_json);
    let prev = current.as_ref().map(|row| row.fencing_token).unwrap_or(0);
    Ok((prev.saturating_add(1), current))
}

pub(crate) async fn acquire_lock(
    svc: &LockServiceImpl,
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
        svc.channels.as_ref(),
        &svc.metrics,
        "lock",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let ttl_seconds = resolve_ttl_seconds(req.lease_ttl_seconds);

    // Existing durable row (if any) for this (tenant, lock_name).
    let existing = runtime
        .native_entity_read_for_service("lock", &context, lock_read_by_name(&tenant_id, &lock_name))
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

    // Per-lock monotone fencing token, allocated UNDER the just-held lease: the
    // fresh read reflects the last committed grant for this lock even after a
    // takeover, so the successor token is strictly greater. Independent of the
    // outbox high-water mark, which could collide when an acquire event dropped.
    let (fencing_token, current) =
        allocate_lock_fencing_token(runtime, &context, &tenant_id, &lock_name).await?;
    let lock_id = current
        .as_ref()
        .or(existing.as_ref())
        .map(|row| row.lock_id.clone())
        .filter(|id| !id.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let acquired_at = Utc::now();
    let expires_at = acquired_at + chrono::Duration::seconds(ttl_seconds);
    let metadata_json = non_empty_json(&req.metadata_json);

    // The lock row and its event commit together: a held/renewed/released state
    // that became durable while its event was lost leaves every fencing-token
    // consumer reasoning from a stale view, with nothing to re-derive it.
    let event_op = lock_event_transaction_op(
        svc,
        TOPIC_ACQUIRED,
        &lease,
        &tenant_id,
        &context.project_id,
        &lock_name,
        &owner_id,
        fencing_token,
    );
    let had_event = event_op.is_some();
    let co_committed = runtime
        .native_entity_write_co_commit_for_service(
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
            event_op,
        )
        .await?;
    if had_event && !co_committed {
        // Target is not Postgres, so the outbox row cannot join the write's
        // transaction. Keep the best-effort emit for that backend.
        emit_lock_event(
            svc,
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
    }

    Ok(Response::new(lock_pb::AcquireLockResponse {
        acquired: true,
        fencing_token,
        lock_name,
        expires_at_unix: now_unix() + ttl_seconds,
        message: "lock acquired".to_string(),
        error: None,
    }))
}

pub(crate) async fn renew_lock(
    svc: &LockServiceImpl,
    request: Request<lock_pb::RenewLockRequest>,
) -> Result<Response<lock_pb::RenewLockResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let (lock_name, owner_id) = validate_lock_identity(&req.lock_name, &req.owner_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "lock",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let ttl_seconds = resolve_ttl_seconds(req.lease_ttl_seconds);

    let stored = runtime
        .native_entity_read_for_service("lock", &context, lock_read_by_name(&tenant_id, &lock_name))
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

    // A continuous renew keeps the same token. But if the lease had already
    // LAPSED (expired) before this renew, re-acquiring it is a fresh grant — a
    // stale token from the lapse window must not stay valid, so bump to a
    // strictly-greater per-lock token. The owner check above guarantees no other
    // owner supplanted us during the lapse (else it would have failed), so the
    // stored token is our own last grant and `+1` is monotone for this lock.
    let lapsed = lease_lapsed(stored.expires_at_unix, now_unix());
    let fencing_token = if lapsed {
        stored.fencing_token.saturating_add(1)
    } else {
        stored.fencing_token
    };

    let acquired_at = Utc::now();
    let expires_at = acquired_at + chrono::Duration::seconds(ttl_seconds);
    // The lock row and its event commit together: a held/renewed/released state
    // that became durable while its event was lost leaves every fencing-token
    // consumer reasoning from a stale view, with nothing to re-derive it.
    let event_op = lock_event_transaction_op(
        svc,
        TOPIC_RENEWED,
        &lease,
        &tenant_id,
        &context.project_id,
        &lock_name,
        &owner_id,
        fencing_token,
    );
    let had_event = event_op.is_some();
    let co_committed = runtime
        .native_entity_write_co_commit_for_service(
            "lock",
            &context,
            LOCK_MSG,
            lock_record(
                &stored.lock_id,
                &tenant_id,
                &lock_name,
                &owner_id,
                fencing_token,
                ttl_seconds,
                STATUS_HELD,
                acquired_at,
                expires_at,
                "{}",
            ),
            lock_conflict(),
            event_op,
        )
        .await?;
    if had_event && !co_committed {
        // Target is not Postgres, so the outbox row cannot join the write's
        // transaction. Keep the best-effort emit for that backend.
        emit_lock_event(
            svc,
            TOPIC_RENEWED,
            &lease,
            &tenant_id,
            &context.project_id,
            &stored.lock_id,
            &lock_name,
            &owner_id,
            fencing_token,
        )
        .await;
    }

    Ok(Response::new(lock_pb::RenewLockResponse {
        renewed: true,
        fencing_token,
        expires_at_unix: now_unix() + ttl_seconds,
        message: "lock renewed".to_string(),
        error: None,
    }))
}

pub(crate) async fn release_lock(
    svc: &LockServiceImpl,
    request: Request<lock_pb::ReleaseLockRequest>,
) -> Result<Response<lock_pb::ReleaseLockResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let (lock_name, owner_id) = validate_lock_identity(&req.lock_name, &req.owner_id)?;
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "lock",
        OperationChannel::Admin,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);

    let stored = runtime
        .native_entity_read_for_service("lock", &context, lock_read_by_name(&tenant_id, &lock_name))
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
    // The lock row and its event commit together: a held/renewed/released state
    // that became durable while its event was lost leaves every fencing-token
    // consumer reasoning from a stale view, with nothing to re-derive it.
    let event_op = lock_event_transaction_op(
        svc,
        TOPIC_RELEASED,
        &lease,
        &tenant_id,
        &context.project_id,
        &lock_name,
        &owner_id,
        stored.fencing_token,
    );
    let had_event = event_op.is_some();
    let co_committed = runtime
        .native_entity_write_co_commit_for_service(
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
            event_op,
        )
        .await?;
    if had_event && !co_committed {
        // Target is not Postgres, so the outbox row cannot join the write's
        // transaction. Keep the best-effort emit for that backend.
        emit_lock_event(
            svc,
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
    }

    Ok(Response::new(lock_pb::ReleaseLockResponse {
        released: true,
        message: "lock released".to_string(),
        error: None,
    }))
}

// ── inventory reads (ch17 / 16.12.1) ──────────────────────────────────────
// Read-only introspection. Tenant is bound to the VERIFIED claim (never the
// body); an absent lock is a normal empty read (found=false), not an error.

pub(crate) async fn get_lock(
    svc: &LockServiceImpl,
    request: Request<lock_pb::GetLockRequest>,
) -> Result<Response<lock_pb::GetLockResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let lock_name = req.lock_name.trim().to_string();
    if lock_name.is_empty() {
        return Err(crate::runtime::executor_utils::invalid_argument_fields(
            "lock_name is required",
            vec![("lock_name", "must be a non-empty lock name")],
        ));
    }
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "lock",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let lock = runtime
        .native_entity_read_for_service(
            "lock",
            &context,
            lock_inventory_read(&tenant_id, Some(&lock_name), None, 0, 1),
        )
        .await?
        .first()
        .map(lock_dto_from_json);
    Ok(Response::new(lock_pb::GetLockResponse {
        found: lock.is_some(),
        lock,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn list_locks(
    svc: &LockServiceImpl,
    request: Request<lock_pb::ListLocksRequest>,
) -> Result<Response<lock_pb::ListLocksResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    validate_request_tenant(&metadata, &req.tenant_id)?;
    let tenant_id = req.tenant_id.trim().to_string();
    let status_filter = req.status_filter.trim().to_string();
    let status = (!status_filter.is_empty()).then_some(status_filter.as_str());
    let window =
        native_offset_page_window(1, req.page_size, &req.page_token, DEFAULT_LOCK_LIST_LIMIT);
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "lock",
        OperationChannel::Read,
        &tenant_id,
        None,
    )
    .await?;
    let runtime = svc.require_runtime()?;
    let context = project_scoped_native_service_context(&metadata, &tenant_id);
    let locks = runtime
        .native_entity_read_for_service(
            "lock",
            &context,
            lock_inventory_read(
                &tenant_id,
                None,
                status,
                window.offset as u64,
                window.limit as u32,
            ),
        )
        .await?
        .iter()
        .map(lock_dto_from_json)
        .collect::<Vec<_>>();
    let next_page_token = native_next_page_token(window.offset, window.limit, locks.len());
    Ok(Response::new(lock_pb::ListLocksResponse {
        locks,
        next_page_token,
        message: String::new(),
        error: None,
    }))
}
