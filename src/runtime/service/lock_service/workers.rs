//! The leader-elected expiry reaper (16.5.1) for the native `LockService`: the
//! no-double-expire claim-and-flip SQL, the transactional expired-event outbox
//! insert, and the one-pass sweep. Extracted verbatim from the former god file —
//! the SQL and the at-least-once outbox contract are byte-for-byte identical.

use sqlx::{PgPool, Row};
use tonic::Status;
use uuid::Uuid;

use crate::runtime::native_catalog::NativeModel;

use super::super::auth_service::events::{ComplianceEnvelope, build_native_compliance_envelope};
use super::config::{LOCK_EXPIRY_SWEEP_BATCH, STATUS_EXPIRED, STATUS_HELD, TOPIC_EXPIRED};
use super::errors::lock_internal_status;
use super::events::lock_event_payload;
use super::model::lease_name;
use super::store::lock_model;

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
