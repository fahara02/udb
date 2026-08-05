//! Bug #2 — PRIVILEGED cross-tenant purge (`TenantService.AdminPurgeTenant`).
//!
//! The self-purge [`super::handlers::purge_tenant`] deliberately forces the body
//! tenant to equal the verified claim (`privileged_cross_tenant:false`), so a
//! delegated operator could never purge a DIFFERENT tenant, and tenant-less /
//! control-plane tables were only ever reported as `excluded`. This SEPARATE RPC
//! closes that gap WITHOUT weakening the self-purge:
//!
//!   * it is gated by a DISTINCT, default-deny scope (`udb:tenant:admin-purge`)
//!     enforced first at the transport gate (descriptor `endpoint_security.scopes`)
//!     and re-checked here (D2 defense-in-depth via [`authorize_action`]);
//!   * it routes the movement with `privileged_cross_tenant:true` through the
//!     shared [`validate_tenant_movement_scope`] fail-closed validator;
//!   * it binds the VERIFIED delegated actor (a forged body actor is rejected for
//!     any caller that is not a genuine cross-tenant admin);
//!   * it treats control-plane / tenant-less tables EXPLICITLY per mode — HARD
//!     hard-deletes tenant-columned tables and RETAINS+REPORTS the tenant-less
//!     ones (never blind-deletes shared control-plane rows); SOFT deactivates the
//!     tenant control record and RETAINS+REPORTS every tenant-owned table;
//!   * it is idempotent + auditable via a durable ledger row bound to a request
//!     hash (a replay with the same key returns the original outcome; the same key
//!     with different inputs is a conflict), and it writes an immutable
//!     audit/outcome record (the ledger row + the compliance outbox event).

use sqlx::Row;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::generation::CatalogManifest;
use crate::proto::udb::core::tenant::services::v1 as tenant_pb;
use crate::runtime::channels::OperationChannel;
use crate::runtime::executor_utils::{
    failed_precondition_fields, policy_status_with_code, qi_runtime, retryable_aborted_status,
};
use crate::runtime::service::method_security::{
    authorize_action, claim_context_present, current_claim_context,
};
use crate::runtime::tenant_movement::{
    TenantMovementOperation, TenantMovementRequest, tenant_movement_policy_status,
    validate_tenant_movement_scope,
};

use super::super::native_helpers::{admit_on as native_admit_on, parse_uuid};
use super::TenantServiceImpl;
use super::config::{
    ACTION_TENANT_ADMIN_PURGE, ADMIN_PURGE_LEDGER_SCHEMA, ADMIN_PURGE_LEDGER_TABLE,
    EVENT_TYPE_TENANT_ADMIN_PURGE, SCOPE_TENANT_ADMIN_PURGE, TENANT_STATUS_INACTIVE_DB,
    TOPIC_TENANT_ADMIN_PURGED,
};
use super::errors::{
    tenant_field_violation, tenant_internal_status, tenant_not_found_status, tenant_required_field,
};
use super::gate;
use super::model::tenant_model;

/// Suggested client backoff for the retryable "purge already in progress" /
/// optimistic-version-conflict refusals (a named const, not a bare literal).
const ADMIN_PURGE_RETRY_BACKOFF_MS: i64 = 250;

/// PRIVILEGED cross-tenant purge. See the module doc for the full contract.
pub(crate) async fn admin_purge_tenant(
    svc: &TenantServiceImpl,
    request: Request<tenant_pb::AdminPurgeTenantRequest>,
) -> Result<Response<tenant_pb::AdminPurgeTenantResponse>, Status> {
    let req = request.into_inner();

    // ── 1. Field validation (fail closed, before any authz / pool / DB access) ──
    let target_raw = req.target_tenant_id.trim().to_string();
    if target_raw.is_empty() {
        return Err(tenant_required_field(
            "target_tenant_id",
            "must be a non-empty tenant id",
            "target_tenant_id is required",
        ));
    }
    let target_uuid = parse_uuid("target_tenant_id", &target_raw)?;
    let target = target_uuid.to_string();

    let mode = tenant_pb::AdminPurgeMode::try_from(req.mode)
        .unwrap_or(tenant_pb::AdminPurgeMode::Unspecified);
    if mode == tenant_pb::AdminPurgeMode::Unspecified {
        return Err(tenant_field_violation(
            "mode",
            "must be ADMIN_PURGE_MODE_HARD or ADMIN_PURGE_MODE_SOFT",
            "mode is required (HARD or SOFT)",
        ));
    }
    let mode_str = admin_purge_mode_str(mode);

    if req.reason.trim().is_empty() {
        return Err(tenant_required_field(
            "reason",
            "must be a non-empty justification (recorded in the audit record)",
            "reason is required",
        ));
    }
    let reason = req.reason.trim().to_string();

    // DESTRUCTIVE cross-tenant confirmation: the token MUST equal target_tenant_id.
    // An empty or non-matching token fails closed so a fat-fingered purge of the
    // wrong tenant cannot proceed (compared as UUIDs so case/format cannot bypass).
    let confirmation = req.confirmation_token.trim();
    if confirmation.is_empty() {
        return Err(tenant_required_field(
            "confirmation_token",
            "must equal target_tenant_id to confirm this irreversible cross-tenant purge",
            "confirmation_token is required",
        ));
    }
    match Uuid::parse_str(confirmation) {
        Ok(confirm_uuid) if confirm_uuid == target_uuid => {}
        _ => {
            return Err(tenant_field_violation(
                "confirmation_token",
                "must equal target_tenant_id",
                "confirmation_token must equal target_tenant_id",
            ));
        }
    }

    let idempotency_key = req.idempotency_key.trim().to_string();
    if idempotency_key.is_empty() {
        return Err(tenant_required_field(
            "idempotency_key",
            "must be a non-empty caller-supplied idempotency key",
            "idempotency_key is required",
        ));
    }

    // ── 2. Default-deny per-action authz (DISTINCT scope, D2 defense-in-depth) ──
    // The transport gate already required `udb:tenant:admin-purge` (or a broad
    // control-plane admin scope); this records the concrete per-action decision and
    // rejects a caller that reached the handler without it. Returns the decision id
    // threaded into the immutable audit record below.
    let claim_ctx = current_claim_context();
    let decision_id = authorize_action(
        &claim_ctx,
        ACTION_TENANT_ADMIN_PURGE,
        &[SCOPE_TENANT_ADMIN_PURGE],
    )?;

    // ── 3. Bind the VERIFIED delegated actor (attribution cannot be forged) ─────
    // Authoritative actor = the validated bearer subject. A body `delegated_actor`
    // that disagrees with the caller is accepted ONLY from a genuine cross-tenant
    // admin; otherwise rejected. Empty inherits the caller. Skipped on the
    // in-process/loopback path (no claim context installed), matching create_user.
    let verified_actor = claim_ctx.subject.trim().to_string();
    let body_actor = req.delegated_actor.trim().to_string();
    let delegated_actor = if !claim_context_present() {
        if body_actor.is_empty() {
            verified_actor.clone()
        } else {
            body_actor
        }
    } else if body_actor.is_empty() {
        verified_actor.clone()
    } else if body_actor != verified_actor && !claim_ctx.is_cross_tenant_admin() {
        return Err(policy_status_with_code(
            tonic::Code::PermissionDenied,
            "admin_purge_tenant",
            "delegated_actor_mismatch",
            "delegated_actor must match the authenticated principal unless the caller is a cross-tenant admin",
        ));
    } else {
        body_actor
    };

    // ── 4. Privileged cross-tenant movement scope (the Bug #2 pivot) ────────────
    // Unlike the self-purge, `privileged_cross_tenant:true` — the shared validator
    // then permits acting on a tenant other than the caller's own claim tenant.
    let movement = TenantMovementRequest {
        operation: TenantMovementOperation::TenantPurge,
        tenant_id: &target,
        target_tenant_id: None,
        tenant_filter_present: true,
        privileged_cross_tenant: true,
    };
    validate_tenant_movement_scope(&movement)
        .map_err(|err| tenant_movement_policy_status(movement.operation, err))?;

    // ── 5. Per-tenant fair admission, charged to the TARGET tenant ──────────────
    let _admit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "tenant",
        OperationChannel::Admin,
        &target,
        None,
    )
    .await?;

    let pool = svc.require_pool()?;
    let manifest = svc.require_manifest()?;
    let now_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // ── 6. Idempotency + immutable audit ledger ─────────────────────────────────
    ensure_admin_purge_ledger(pool).await?;
    let relation = admin_purge_ledger_relation();
    let request_hash = admin_purge_request_hash(
        &target,
        mode_str,
        &reason,
        req.expected_version,
        &delegated_actor,
        &verified_actor,
    );

    // Prior outcome for this key: replay on an identical request; conflict on a
    // reused key with different inputs; in-progress on a concurrent duplicate.
    if let Some(replay) =
        admin_purge_lookup(pool, &relation, &idempotency_key, &request_hash, &target).await?
    {
        return Ok(Response::new(replay));
    }

    // ── 7. Existence + optimistic-concurrency guard (BEFORE claiming) ───────────
    let m = tenant_model();
    let version = read_target_tenant_version(pool, &m, &target)
        .await?
        .ok_or_else(|| tenant_not_found_status("admin_purge_tenant"))?;
    if req.expected_version != 0 && req.expected_version != version {
        return Err(retryable_aborted_status(
            "tenant",
            "admin_purge_tenant",
            ADMIN_PURGE_RETRY_BACKOFF_MS,
            format!(
                "tenant version conflict: expected {}, current {}",
                req.expected_version, version
            ),
        ));
    }

    // ── 8. Claim the idempotency key (fail closed on a lost race) ───────────────
    let outcome_id = Uuid::new_v4().to_string();
    let claimed = admin_purge_try_claim(
        pool,
        &relation,
        &AdminPurgeLedgerRow {
            idempotency_key: &idempotency_key,
            request_hash: &request_hash,
            target_tenant_id: &target,
            mode: mode_str,
            delegated_actor: &delegated_actor,
            verified_actor: &verified_actor,
            reason: &reason,
            decision_id: &decision_id,
            outcome_id: &outcome_id,
        },
    )
    .await?;
    if !claimed {
        // Another request claimed the same key between our lookup and INSERT.
        if let Some(replay) =
            admin_purge_lookup(pool, &relation, &idempotency_key, &request_hash, &target).await?
        {
            return Ok(Response::new(replay));
        }
        return Err(retryable_aborted_status(
            "tenant",
            "admin_purge_tenant",
            ADMIN_PURGE_RETRY_BACKOFF_MS,
            "an admin purge for this idempotency key is already in progress",
        ));
    }

    // ── 9. Execute the chosen treatment ─────────────────────────────────────────
    let exec = match mode {
        tenant_pb::AdminPurgeMode::Hard => {
            execute_hard_purge(svc, pool, manifest, &target, now_unix).await
        }
        tenant_pb::AdminPurgeMode::Soft => {
            let actor_uuid = Uuid::parse_str(&delegated_actor).ok();
            execute_soft_purge(svc, pool, manifest, &m, &target, actor_uuid, now_unix).await
        }
        // UNSPECIFIED was rejected in step 1.
        tenant_pb::AdminPurgeMode::Unspecified => unreachable!("mode validated non-unspecified"),
    };
    let exec = match exec {
        Ok(exec) => exec,
        Err(err) => {
            // Release the PENDING claim so a corrected retry with the same key can
            // proceed (a failed purge must not strand the key forever).
            release_admin_purge_claim(pool, &relation, &idempotency_key).await;
            return Err(err);
        }
    };

    // ── 10. Immutable outcome record: finalize the ledger + emit the audit ──────
    let outcome_json = serde_json::json!({
        "mode": mode_str,
        "purged": exec.purged,
        "excluded": exec.excluded,
        "total_deleted": exec.total_deleted,
        "tenant_denylisted": exec.tenant_denylisted,
        "principals_denylisted": exec.principals_denylisted,
        "soft_deactivated": exec.soft_deactivated,
        "purged_version": version,
        "outcome_id": outcome_id,
        "delegated_actor": delegated_actor,
        "verified_actor": verified_actor,
        "reason": reason,
    });
    finalize_admin_purge(pool, &relation, &idempotency_key, &outcome_json).await?;

    let mut audit_payload = outcome_json.clone();
    if let Some(obj) = audit_payload.as_object_mut() {
        obj.insert(
            "target_tenant_id".to_string(),
            serde_json::Value::String(target.clone()),
        );
    }
    super::events::emit_admin_purge_audit(svc, &target, &verified_actor, &decision_id, audit_payload)
        .await;

    Ok(Response::new(response_from_outcome_json(
        &target,
        &outcome_json,
        false,
    )))
}

/// Fully-qualified, quoted relation for the admin-purge ledger.
fn admin_purge_ledger_relation() -> String {
    format!(
        "{}.{}",
        qi_runtime(ADMIN_PURGE_LEDGER_SCHEMA),
        qi_runtime(ADMIN_PURGE_LEDGER_TABLE)
    )
}

/// Create the durable idempotency + immutable audit/outcome ledger on first use.
/// Same `CREATE TABLE IF NOT EXISTS` bootstrap the platform system tables use;
/// the UDB-owned `udb_tenant` schema already exists (it holds `tenants`). Rows are
/// append-only: a PENDING claim transitions to COMPLETED exactly once and the
/// COMPLETED outcome is never mutated or deleted.
async fn ensure_admin_purge_ledger(pool: &sqlx::PgPool) -> Result<(), Status> {
    let relation = admin_purge_ledger_relation();
    let ddl = format!(
        "CREATE TABLE IF NOT EXISTS {relation} (
            idempotency_key   TEXT PRIMARY KEY,
            request_hash      TEXT NOT NULL,
            target_tenant_id  TEXT NOT NULL,
            mode              TEXT NOT NULL,
            delegated_actor   TEXT NOT NULL DEFAULT '',
            verified_actor    TEXT NOT NULL DEFAULT '',
            reason            TEXT NOT NULL DEFAULT '',
            decision_id       TEXT NOT NULL DEFAULT '',
            status            TEXT NOT NULL,
            outcome_json      JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            outcome_id        UUID NOT NULL,
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            completed_at      TIMESTAMPTZ
        )"
    );
    sqlx::query(&ddl).execute(pool).await.map_err(|err| {
        tenant_internal_status(
            "admin_purge_ledger_ddl",
            format!("failed to ensure admin purge ledger: {err}"),
        )
    })?;
    let index = format!(
        "CREATE INDEX IF NOT EXISTS {} ON {relation} (target_tenant_id, created_at)",
        qi_runtime(&format!("idx_{ADMIN_PURGE_LEDGER_TABLE}_target"))
    );
    sqlx::query(&index).execute(pool).await.map_err(|err| {
        tenant_internal_status(
            "admin_purge_ledger_index",
            format!("failed to ensure admin purge ledger index: {err}"),
        )
    })?;
    Ok(())
}

/// Authoritative-input hash bound to the idempotency claim: a reused key with any
/// different authoritative input (target, mode, reason, expected version, or the
/// bound actor identities) is a CONFLICT, never a bogus replay.
fn admin_purge_request_hash(
    target: &str,
    mode: &str,
    reason: &str,
    expected_version: i64,
    delegated_actor: &str,
    verified_actor: &str,
) -> String {
    let payload = serde_json::json!({
        "v": 1,
        "op": "tenant.admin_purge",
        "target": target,
        "mode": mode,
        "reason": reason,
        "expected_version": expected_version,
        "delegated_actor": delegated_actor,
        "verified_actor": verified_actor,
    });
    crate::runtime::executor_utils::checksum_str(&payload.to_string())
}

/// Look up a prior outcome for `key`. `Ok(None)` = no row (proceed); `Ok(Some)` =
/// a COMPLETED identical request to REPLAY; `Err` = a reused key with different
/// inputs (conflict) or a concurrent PENDING duplicate (retryable in-progress).
async fn admin_purge_lookup(
    pool: &sqlx::PgPool,
    relation: &str,
    key: &str,
    request_hash: &str,
    target: &str,
) -> Result<Option<tenant_pb::AdminPurgeTenantResponse>, Status> {
    let sql = format!(
        "SELECT request_hash, status, outcome_json::text AS outcome_json \
         FROM {relation} WHERE idempotency_key = $1"
    );
    let row = sqlx::query(&sql)
        .bind(key)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "admin_purge_lookup",
                format!("admin purge idempotency lookup failed: {err}"),
            )
        })?;
    let Some(row) = row else {
        return Ok(None);
    };
    let decode = |e: sqlx::Error| {
        tenant_internal_status(
            "admin_purge_lookup",
            format!("decode admin purge ledger row failed: {e}"),
        )
    };
    let stored_hash: String = row.try_get("request_hash").map_err(decode)?;
    if stored_hash != request_hash {
        // Non-disclosing: never echo the prior request's parameters.
        return Err(failed_precondition_fields(
            "idempotency_key was reused with different request parameters",
            [(
                "idempotency_key",
                "already used for a different admin purge request",
            )],
        ));
    }
    let status: String = row.try_get("status").map_err(decode)?;
    if status != "COMPLETED" {
        return Err(retryable_aborted_status(
            "tenant",
            "admin_purge_tenant",
            ADMIN_PURGE_RETRY_BACKOFF_MS,
            "an admin purge for this idempotency key is already in progress",
        ));
    }
    let outcome_text: String = row.try_get("outcome_json").map_err(decode)?;
    let outcome: serde_json::Value = serde_json::from_str(&outcome_text).map_err(|e| {
        tenant_internal_status(
            "admin_purge_lookup",
            format!("decode admin purge outcome json failed: {e}"),
        )
    })?;
    Ok(Some(response_from_outcome_json(target, &outcome, true)))
}

/// The authoritative inputs persisted with an idempotency claim.
struct AdminPurgeLedgerRow<'a> {
    idempotency_key: &'a str,
    request_hash: &'a str,
    target_tenant_id: &'a str,
    mode: &'a str,
    delegated_actor: &'a str,
    verified_actor: &'a str,
    reason: &'a str,
    decision_id: &'a str,
    outcome_id: &'a str,
}

/// Claim the idempotency key by inserting a PENDING row. `Ok(true)` = we own the
/// claim (execute); `Ok(false)` = the key already existed (a concurrent claim).
async fn admin_purge_try_claim(
    pool: &sqlx::PgPool,
    relation: &str,
    row: &AdminPurgeLedgerRow<'_>,
) -> Result<bool, Status> {
    let sql = format!(
        "INSERT INTO {relation} \
         (idempotency_key, request_hash, target_tenant_id, mode, delegated_actor, \
          verified_actor, reason, decision_id, outcome_id, status) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::UUID, 'PENDING') \
         ON CONFLICT (idempotency_key) DO NOTHING \
         RETURNING idempotency_key"
    );
    let claimed = sqlx::query(&sql)
        .bind(row.idempotency_key)
        .bind(row.request_hash)
        .bind(row.target_tenant_id)
        .bind(row.mode)
        .bind(row.delegated_actor)
        .bind(row.verified_actor)
        .bind(row.reason)
        .bind(row.decision_id)
        .bind(row.outcome_id)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "admin_purge_claim",
                format!("admin purge idempotency claim failed: {err}"),
            )
        })?;
    Ok(claimed.is_some())
}

/// Transition the claimed PENDING row to the COMPLETED immutable outcome exactly
/// once (`WHERE status = 'PENDING'` — the outcome is never overwritten).
async fn finalize_admin_purge(
    pool: &sqlx::PgPool,
    relation: &str,
    key: &str,
    outcome_json: &serde_json::Value,
) -> Result<(), Status> {
    let sql = format!(
        "UPDATE {relation} SET status = 'COMPLETED', outcome_json = $2::jsonb, \
         completed_at = NOW() WHERE idempotency_key = $1 AND status = 'PENDING'"
    );
    sqlx::query(&sql)
        .bind(key)
        .bind(outcome_json.to_string())
        .execute(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "admin_purge_finalize",
                format!("admin purge outcome finalize failed: {err}"),
            )
        })?;
    Ok(())
}

/// Best-effort release of a PENDING claim after an execution FAILURE, so a
/// corrected retry with the same key is not stranded. Never fails the caller.
async fn release_admin_purge_claim(pool: &sqlx::PgPool, relation: &str, key: &str) {
    let sql = format!("DELETE FROM {relation} WHERE idempotency_key = $1 AND status = 'PENDING'");
    if let Err(err) = sqlx::query(&sql).bind(key).execute(pool).await {
        tracing::warn!(
            idempotency_key = %key,
            error = %err,
            "admin purge failed and the PENDING idempotency claim could not be released \
             (a retry with the same key will report in-progress until it is cleared)"
        );
    }
}

/// Read the target tenant's optimistic-concurrency version (its `updated_at`
/// epoch seconds; 0 when unset). `None` when the ACTIVE control record is absent
/// (already purged / never existed) → the handler maps that to NotFound.
/// `updated_at` is the injected audit column (audit_fields=true), not a proto
/// field, so it is referenced as a raw quoted identifier.
async fn read_target_tenant_version(
    pool: &sqlx::PgPool,
    m: &crate::runtime::native_catalog::NativeModel,
    target: &str,
) -> Result<Option<i64>, Status> {
    let sql = format!(
        "SELECT COALESCE(EXTRACT(EPOCH FROM {updated})::BIGINT, 0) AS version \
         FROM {rel} WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL",
        updated = qi_runtime("updated_at"),
        rel = m.relation,
        tenant_id = m.q("tenant_id"),
        deleted = m.q("deleted_at"),
    );
    let row = sqlx::query(&sql)
        .bind(target)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "admin_purge_read_state",
                format!("read target tenant state failed: {err}"),
            )
        })?;
    let Some(row) = row else {
        return Ok(None);
    };
    row.try_get("version").map(Some).map_err(|e| {
        tenant_internal_status(
            "admin_purge_read_state",
            format!("decode target tenant version failed: {e}"),
        )
    })
}

/// Per-execution outcome (JSON-shaped so the ledger row + the replay path share a
/// single construction).
struct AdminPurgeExecOutcome {
    purged: Vec<serde_json::Value>,
    excluded: Vec<serde_json::Value>,
    total_deleted: u64,
    tenant_denylisted: bool,
    principals_denylisted: u32,
    soft_deactivated: bool,
}

/// HARD: run the shared tenant hard-delete ripple. Tenant-columned tables are
/// physically deleted (children→parents in one tx); tenant-less / control-plane
/// tables are RETAINED and reported (never blind-deleted — that would corrupt
/// shared/global state). Best-effort token revocation rides the same foundation
/// as the self-purge.
async fn execute_hard_purge(
    svc: &TenantServiceImpl,
    pool: &sqlx::PgPool,
    manifest: &CatalogManifest,
    target: &str,
    now_unix: u64,
) -> Result<AdminPurgeExecOutcome, Status> {
    let report = {
        #[cfg(feature = "redis")]
        {
            crate::runtime::core::purge_tenant(
                pool,
                manifest,
                target,
                &[],
                svc.jti_denylist.as_ref(),
                now_unix,
            )
            .await?
        }
        #[cfg(not(feature = "redis"))]
        {
            let _ = svc;
            crate::runtime::core::purge_tenant(pool, manifest, target, &[], now_unix).await?
        }
    };
    let purged: Vec<serde_json::Value> = report
        .purged
        .iter()
        .map(|p| {
            serde_json::json!({
                "schema": &p.schema,
                "table": &p.table,
                "tenant_column": &p.tenant_column,
                "deleted": p.deleted,
            })
        })
        .collect();
    let excluded: Vec<serde_json::Value> = report
        .excluded
        .iter()
        .map(|e| {
            serde_json::json!({
                "schema": &e.schema,
                "table": &e.table,
                "reason": format!("retained (control-plane / tenant-less): {}", e.reason),
            })
        })
        .collect();
    Ok(AdminPurgeExecOutcome {
        purged,
        excluded,
        total_deleted: report.total_deleted,
        tenant_denylisted: report.tenant_denylisted,
        principals_denylisted: report.principals_denylisted as u32,
        soft_deactivated: false,
    })
}

/// SOFT: no physical deletes. Deactivate the tenant control record (soft-delete +
/// INACTIVE), mark the process suspension gate so the tenant's live tokens are
/// rejected immediately, and record a best-effort cluster denylist cutoff. EVERY
/// tenant-owned table is REPORTED as retained (capability honesty — the operator
/// sees exactly what soft mode preserved and did NOT physically remove).
async fn execute_soft_purge(
    svc: &TenantServiceImpl,
    pool: &sqlx::PgPool,
    manifest: &CatalogManifest,
    m: &crate::runtime::native_catalog::NativeModel,
    target: &str,
    actor_uuid: Option<Uuid>,
    now_unix: u64,
) -> Result<AdminPurgeExecOutcome, Status> {
    let sql = format!(
        "UPDATE {rel} SET {deleted} = NOW(), {deleted_by} = $2, {status} = $3 \
         WHERE {tenant_id} = $1::UUID AND {deleted} IS NULL RETURNING {code} AS code",
        rel = m.relation,
        deleted = m.q("deleted_at"),
        deleted_by = m.q("deleted_by"),
        status = m.q("status"),
        tenant_id = m.q("tenant_id"),
        code = m.q("code"),
    );
    let updated = sqlx::query(&sql)
        .bind(target)
        .bind(actor_uuid)
        .bind(TENANT_STATUS_INACTIVE_DB)
        .fetch_optional(pool)
        .await
        .map_err(|err| {
            tenant_internal_status(
                "admin_purge_soft",
                format!("soft admin purge update failed: {err}"),
            )
        })?;
    if updated.is_none() {
        // Raced with a concurrent delete/soft-purge between the pre-read and here.
        return Err(tenant_not_found_status("admin_purge_tenant"));
    }
    // Immediate local revocation: the shared request gate rejects this tenant's
    // live bearer tokens at once instead of at TTL (same H10 mechanism the status
    // update path uses).
    gate::mark_tenant_status(target, TENANT_STATUS_INACTIVE_DB);
    // Best-effort cluster-wide cutoff (accelerator over the durable state).
    #[cfg(feature = "redis")]
    let tenant_denylisted = if let Some(denylist) = svc.jti_denylist.as_ref() {
        match denylist.deny_tenant_after(target, now_unix).await {
            Ok(()) => true,
            Err(err) => {
                tracing::warn!(
                    tenant_id = %target,
                    error = %err,
                    "soft admin purge committed but tenant denylist cutoff was not recorded"
                );
                false
            }
        }
    } else {
        false
    };
    #[cfg(not(feature = "redis"))]
    let tenant_denylisted = {
        let _ = (svc, now_unix);
        false
    };

    // Report every tenant-owned table as retained (soft mode preserves data), plus
    // the tenant-less / control-plane tables — nothing was physically deleted.
    let plan = crate::runtime::core::plan_tenant_purge(manifest);
    let mut excluded = Vec::with_capacity(plan.targets.len() + plan.excluded.len());
    for target_table in &plan.targets {
        excluded.push(serde_json::json!({
            "schema": &target_table.schema,
            "table": &target_table.table,
            "reason": "retained: SOFT mode preserves tenant-owned data (control record deactivated)",
        }));
    }
    for excluded_table in &plan.excluded {
        excluded.push(serde_json::json!({
            "schema": &excluded_table.schema,
            "table": &excluded_table.table,
            "reason": format!("retained (control-plane / tenant-less): {}", excluded_table.reason),
        }));
    }
    Ok(AdminPurgeExecOutcome {
        purged: Vec::new(),
        excluded,
        total_deleted: 0,
        tenant_denylisted,
        principals_denylisted: 0,
        soft_deactivated: true,
    })
}

fn admin_purge_mode_str(mode: tenant_pb::AdminPurgeMode) -> &'static str {
    match mode {
        tenant_pb::AdminPurgeMode::Hard => "HARD",
        tenant_pb::AdminPurgeMode::Soft => "SOFT",
        tenant_pb::AdminPurgeMode::Unspecified => "UNSPECIFIED",
    }
}

fn admin_purge_mode_from_str(value: &str) -> tenant_pb::AdminPurgeMode {
    match value {
        "HARD" => tenant_pb::AdminPurgeMode::Hard,
        "SOFT" => tenant_pb::AdminPurgeMode::Soft,
        _ => tenant_pb::AdminPurgeMode::Unspecified,
    }
}

fn json_str(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn purged_from_json(value: &serde_json::Value) -> Vec<tenant_pb::PurgedTableCount> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|p| tenant_pb::PurgedTableCount {
                    schema: json_str(p, "schema"),
                    table: json_str(p, "table"),
                    tenant_column: json_str(p, "tenant_column"),
                    deleted: p.get("deleted").and_then(serde_json::Value::as_u64).unwrap_or(0),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn excluded_from_json(value: &serde_json::Value) -> Vec<tenant_pb::PurgeExcludedTable> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|e| tenant_pb::PurgeExcludedTable {
                    schema: json_str(e, "schema"),
                    table: json_str(e, "table"),
                    reason: json_str(e, "reason"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Rebuild the response from the stored outcome json, so a fresh completion and an
/// idempotent replay return byte-identical bodies (only `replayed`/`message` differ).
fn response_from_outcome_json(
    target: &str,
    outcome: &serde_json::Value,
    replayed: bool,
) -> tenant_pb::AdminPurgeTenantResponse {
    tenant_pb::AdminPurgeTenantResponse {
        target_tenant_id: target.to_string(),
        mode: admin_purge_mode_from_str(&json_str(outcome, "mode")) as i32,
        purged: purged_from_json(outcome.get("purged").unwrap_or(&serde_json::Value::Null)),
        excluded: excluded_from_json(outcome.get("excluded").unwrap_or(&serde_json::Value::Null)),
        total_deleted: outcome
            .get("total_deleted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        tenant_denylisted: outcome
            .get("tenant_denylisted")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        principals_denylisted: outcome
            .get("principals_denylisted")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32,
        soft_deactivated: outcome
            .get("soft_deactivated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        purged_version: outcome
            .get("purged_version")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0),
        replayed,
        outcome_id: json_str(outcome, "outcome_id"),
        message: if replayed {
            "tenant admin purge replayed (idempotent)".to_string()
        } else {
            "tenant admin purged".to_string()
        },
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_purge_mode_round_trips_through_string() {
        for (mode, token) in [
            (tenant_pb::AdminPurgeMode::Hard, "HARD"),
            (tenant_pb::AdminPurgeMode::Soft, "SOFT"),
        ] {
            assert_eq!(admin_purge_mode_str(mode), token);
            assert_eq!(admin_purge_mode_from_str(token), mode);
        }
        // Unknown / unspecified tokens fail closed to Unspecified (never Hard/Soft).
        assert_eq!(
            admin_purge_mode_from_str("GARBAGE"),
            tenant_pb::AdminPurgeMode::Unspecified
        );
    }

    /// The request hash must change when ANY authoritative input changes, so a
    /// reused idempotency key with different parameters can never masquerade as a
    /// replay. It must ALSO be stable across identical inputs (real replays work).
    #[test]
    fn request_hash_is_input_sensitive_and_stable() {
        let base = admin_purge_request_hash("t1", "HARD", "gdpr", 0, "actor", "verified");
        assert_eq!(
            base,
            admin_purge_request_hash("t1", "HARD", "gdpr", 0, "actor", "verified"),
            "identical inputs must hash identically (replay)"
        );
        assert!(base.starts_with("sha256:"));
        for changed in [
            admin_purge_request_hash("t2", "HARD", "gdpr", 0, "actor", "verified"),
            admin_purge_request_hash("t1", "SOFT", "gdpr", 0, "actor", "verified"),
            admin_purge_request_hash("t1", "HARD", "other", 0, "actor", "verified"),
            admin_purge_request_hash("t1", "HARD", "gdpr", 7, "actor", "verified"),
            admin_purge_request_hash("t1", "HARD", "gdpr", 0, "other", "verified"),
            admin_purge_request_hash("t1", "HARD", "gdpr", 0, "actor", "other"),
        ] {
            assert_ne!(base, changed, "a changed authoritative input must rotate the hash");
        }
    }

    /// A completed outcome and its idempotent replay reconstruct byte-identical
    /// bodies except the `replayed`/`message` fields.
    #[test]
    fn outcome_json_round_trips_to_response() {
        let outcome = serde_json::json!({
            "mode": "HARD",
            "purged": [{"schema": "app", "table": "invoice", "tenant_column": "tenant_id", "deleted": 3u64}],
            "excluded": [{"schema": "platform_system", "table": "events_outbox", "reason": "retained (control-plane / tenant-less): no resolvable tenant-isolation column"}],
            "total_deleted": 3u64,
            "tenant_denylisted": true,
            "principals_denylisted": 2u64,
            "soft_deactivated": false,
            "purged_version": 42i64,
            "outcome_id": "11111111-1111-1111-1111-111111111111",
        });
        let fresh = response_from_outcome_json("tenant-x", &outcome, false);
        let replay = response_from_outcome_json("tenant-x", &outcome, true);

        assert_eq!(fresh.target_tenant_id, "tenant-x");
        assert_eq!(fresh.mode, tenant_pb::AdminPurgeMode::Hard as i32);
        assert_eq!(fresh.purged.len(), 1);
        assert_eq!(fresh.purged[0].table, "invoice");
        assert_eq!(fresh.purged[0].deleted, 3);
        assert_eq!(fresh.excluded.len(), 1);
        assert_eq!(fresh.excluded[0].table, "events_outbox");
        assert_eq!(fresh.total_deleted, 3);
        assert!(fresh.tenant_denylisted);
        assert_eq!(fresh.principals_denylisted, 2);
        assert!(!fresh.soft_deactivated);
        assert_eq!(fresh.purged_version, 42);
        assert_eq!(fresh.outcome_id, "11111111-1111-1111-1111-111111111111");
        assert!(!fresh.replayed);
        assert!(!replay.message.is_empty());
        assert!(replay.replayed);

        // The load-bearing purge counts/tables are identical across replay.
        assert_eq!(fresh.purged, replay.purged);
        assert_eq!(fresh.excluded, replay.excluded);
        assert_eq!(fresh.total_deleted, replay.total_deleted);
        assert_eq!(fresh.outcome_id, replay.outcome_id);
    }

    #[test]
    fn ledger_relation_is_quoted_and_schema_qualified() {
        assert_eq!(
            admin_purge_ledger_relation(),
            "\"udb_tenant\".\"tenant_admin_purge_outcomes\""
        );
    }
}
