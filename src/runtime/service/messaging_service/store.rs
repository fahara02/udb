//! Postgres store for the native `MessagingService`. Every relation and column
//! comes from the proto manifest through [`native_model`]; no column name is
//! written by hand. Every statement runs inside a transaction that installed
//! `app.current_tenant_id` (the tables are FORCE ROW LEVEL SECURITY) AND names
//! the tenant in its own predicate, so isolation never rests on one mechanism.
//!
//! Concurrency rules the handlers rely on:
//! - a one-time KeyPackage is claimed by `UPDATE ... WHERE id = (SELECT ... FOR
//!   UPDATE SKIP LOCKED)`, so concurrent claimers never receive the same row;
//! - a group row is locked `FOR UPDATE` by SubmitCommit and `FOR SHARE` by
//!   SendMessage, so commits linearize and a message is never accepted for an
//!   epoch a concurrent commit is retiring;
//! - inbox sequence numbers come from the device row (`inbox_head_seq + 1`),
//!   allocated in ascending device-id order so concurrent fan-outs cannot
//!   deadlock.

use std::sync::OnceLock;

use sqlx::{PgConnection, PgPool, Postgres, Row, Transaction};
use tonic::Status;

use crate::runtime::native_catalog::{NativeModel, native_model};

use super::config::{
    BACKUP_MSG, DEVICE_ACTIVE, DEVICE_MSG, DEVICE_REVOKED, GROUP_MEMBER_MSG, GROUP_MSG, INBOX_MSG,
    KEY_PACKAGE_MSG,
};
use super::errors::{messaging_internal_status, messaging_store_status};

pub(crate) struct Models {
    pub(crate) device: NativeModel,
    pub(crate) key_package: NativeModel,
    pub(crate) group: NativeModel,
    pub(crate) member: NativeModel,
    pub(crate) inbox: NativeModel,
    pub(crate) backup: NativeModel,
}

pub(crate) fn models() -> &'static Models {
    static MODELS: OnceLock<Models> = OnceLock::new();
    MODELS.get_or_init(|| Models {
        device: native_model(
            DEVICE_MSG,
            &[
                "id",
                "tenant_id",
                "subject_id",
                "device_id",
                "signature_public_key",
                "credential",
                "key_fingerprint",
                "status",
                "inbox_head_seq",
                "acked_seq",
                "registered_at",
                "revoked_at",
            ],
        ),
        key_package: native_model(
            KEY_PACKAGE_MSG,
            &[
                "id",
                "tenant_id",
                "device_id",
                "key_package",
                "key_package_ref",
                "last_resort",
                "published_at",
                "claimed_at",
                "claimed_by_device_id",
            ],
        ),
        group: native_model(
            GROUP_MSG,
            &[
                "id",
                "tenant_id",
                "group_id",
                "epoch",
                "public_group_state",
                "group_info",
                "group_info_epoch",
                "created_by_device_id",
                "created_at",
                "updated_at",
            ],
        ),
        member: native_model(
            GROUP_MEMBER_MSG,
            &[
                "id",
                "tenant_id",
                "group_id",
                "device_id",
                "subject_id",
                "leaf_index",
                "joined_epoch",
            ],
        ),
        inbox: native_model(
            INBOX_MSG,
            &[
                "id",
                "tenant_id",
                "device_id",
                "seq",
                "kind",
                "group_id",
                "epoch",
                "payload",
                "sender_device_id",
                "client_message_id",
                "created_at",
                "acked_at",
            ],
        ),
        backup: native_model(
            BACKUP_MSG,
            &[
                "id",
                "tenant_id",
                "subject_id",
                "backup_id",
                "ciphertext",
                "size_bytes",
                "ciphertext_sha256",
                "version",
                "created_at",
                "updated_at",
            ],
        ),
    })
}

/// Begin a transaction scoped to `tenant_id` for row-level security.
pub(crate) async fn begin_tenant_tx(
    pool: &PgPool,
    tenant_id: &str,
) -> Result<Transaction<'static, Postgres>, Status> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|err| messaging_store_status("begin", &err))?;
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(|err| messaging_store_status("tenant_scope", &err))?;
    Ok(tx)
}

pub(crate) async fn commit(tx: Transaction<'static, Postgres>) -> Result<(), Status> {
    tx.commit()
        .await
        .map_err(|err| messaging_store_status("commit", &err))
}

fn unix(column: &str, alias: &str) -> String {
    format!("COALESCE(EXTRACT(EPOCH FROM {column})::BIGINT, 0) AS {alias}")
}

fn decode<T>(row: &sqlx::postgres::PgRow, column: &str) -> Result<T, Status>
where
    T: for<'r> sqlx::Decode<'r, Postgres> + sqlx::Type<Postgres>,
{
    row.try_get::<T, _>(column).map_err(|err| {
        messaging_internal_status("decode", format!("messaging row decode {column}: {err}"))
    })
}

// ── devices ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct DeviceRow {
    pub(crate) subject_id: String,
    pub(crate) device_id: String,
    pub(crate) signature_public_key: Vec<u8>,
    pub(crate) credential: Vec<u8>,
    pub(crate) key_fingerprint: String,
    pub(crate) status: String,
    pub(crate) acked_seq: i64,
    pub(crate) registered_at_unix: i64,
    pub(crate) revoked_at_unix: i64,
}

impl DeviceRow {
    pub(crate) fn is_active(&self) -> bool {
        self.status == DEVICE_ACTIVE
    }
}

fn device_columns(m: &NativeModel) -> String {
    format!(
        "{subject} AS subject_id, {device} AS device_id, {key} AS signature_public_key, \
         {credential} AS credential, {fingerprint} AS key_fingerprint, {status} AS status, \
         {acked} AS acked_seq, {registered}, {revoked}",
        subject = m.q("subject_id"),
        device = m.q("device_id"),
        key = m.q("signature_public_key"),
        credential = m.q("credential"),
        fingerprint = m.q("key_fingerprint"),
        status = m.q("status"),
        acked = m.q("acked_seq"),
        registered = unix(&m.q("registered_at"), "registered_at_unix"),
        revoked = unix(&m.q("revoked_at"), "revoked_at_unix"),
    )
}

fn device_from_row(row: &sqlx::postgres::PgRow) -> Result<DeviceRow, Status> {
    Ok(DeviceRow {
        subject_id: decode(row, "subject_id")?,
        device_id: decode(row, "device_id")?,
        signature_public_key: decode(row, "signature_public_key")?,
        credential: decode(row, "credential")?,
        key_fingerprint: decode(row, "key_fingerprint")?,
        status: decode(row, "status")?,
        acked_seq: decode(row, "acked_seq")?,
        registered_at_unix: decode(row, "registered_at_unix")?,
        revoked_at_unix: decode(row, "revoked_at_unix")?,
    })
}

pub(crate) async fn load_device(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
) -> Result<Option<DeviceRow>, Status> {
    let m = &models().device;
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {device} = $2",
        cols = device_columns(m),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("load_device", &err))?;
    row.as_ref().map(device_from_row).transpose()
}

/// Insert a device; `None` when (tenant, device_id) already exists.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_device(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
    device_id: &str,
    signature_public_key: &[u8],
    credential: &[u8],
    key_fingerprint: &str,
) -> Result<Option<DeviceRow>, Status> {
    let m = &models().device;
    let sql = format!(
        "INSERT INTO {rel} ({tenant}, {subject}, {device}, {key}, {credential}, {fingerprint}, {status}) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT ({tenant}, {device}) DO NOTHING \
         RETURNING {cols}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        device = m.q("device_id"),
        key = m.q("signature_public_key"),
        credential = m.q("credential"),
        fingerprint = m.q("key_fingerprint"),
        status = m.q("status"),
        cols = device_columns(m),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .bind(device_id)
        .bind(signature_public_key)
        .bind(credential)
        .bind(key_fingerprint)
        .bind(DEVICE_ACTIVE)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("insert_device", &err))?;
    row.as_ref().map(device_from_row).transpose()
}

pub(crate) async fn list_devices(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
    include_revoked: bool,
) -> Result<Vec<DeviceRow>, Status> {
    let m = &models().device;
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {subject} = $2 \
         AND ($3 OR {status} = $4) ORDER BY {device}",
        cols = device_columns(m),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        status = m.q("status"),
        device = m.q("device_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .bind(include_revoked)
        .bind(DEVICE_ACTIVE)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("list_devices", &err))?;
    rows.iter().map(device_from_row).collect()
}

/// ACTIVE devices of the given subjects, as `(device_id, subject_id)`.
pub(crate) async fn active_devices_of_subjects(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_ids: &[String],
) -> Result<Vec<(String, String)>, Status> {
    if subject_ids.is_empty() {
        return Ok(Vec::new());
    }
    let m = &models().device;
    let sql = format!(
        "SELECT {device} AS device_id, {subject} AS subject_id FROM {rel} \
         WHERE {tenant} = $1 AND {subject} = ANY($2) AND {status} = $3 ORDER BY {device}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        status = m.q("status"),
        device = m.q("device_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_ids)
        .bind(DEVICE_ACTIVE)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("active_devices_of_subjects", &err))?;
    rows.iter()
        .map(|row| Ok((decode(row, "device_id")?, decode(row, "subject_id")?)))
        .collect()
}

/// Devices (any status) by id, keyed by device id.
pub(crate) async fn devices_by_id(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_ids: &[String],
) -> Result<std::collections::BTreeMap<String, DeviceRow>, Status> {
    if device_ids.is_empty() {
        return Ok(Default::default());
    }
    let m = &models().device;
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {device} = ANY($2)",
        cols = device_columns(m),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("devices_by_id", &err))?;
    rows.iter()
        .map(|row| device_from_row(row).map(|device| (device.device_id.clone(), device)))
        .collect()
}

/// Row-lock the given devices in ascending device-id order and return them.
/// Every transaction that touches more than one device row (fan-out, revoke)
/// locks through here first, so two of them always lock in the same order and
/// can never deadlock.
pub(crate) async fn lock_devices(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_ids: &[String],
) -> Result<std::collections::BTreeMap<String, DeviceRow>, Status> {
    if device_ids.is_empty() {
        return Ok(Default::default());
    }
    let m = &models().device;
    let sql = format!(
        "SELECT {cols} FROM {rel} WHERE {tenant} = $1 AND {device} = ANY($2) \
         ORDER BY {device} FOR UPDATE",
        cols = device_columns(m),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_ids)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("lock_devices", &err))?;
    rows.iter()
        .map(|row| device_from_row(row).map(|device| (device.device_id.clone(), device)))
        .collect()
}

/// Flip an ACTIVE device to REVOKED. `false` when it was not active.
pub(crate) async fn revoke_device(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
) -> Result<bool, Status> {
    let m = &models().device;
    let sql = format!(
        "UPDATE {rel} SET {status} = $3, {revoked} = NOW() \
         WHERE {tenant} = $1 AND {device} = $2 AND {status} = $4",
        rel = m.relation,
        status = m.q("status"),
        revoked = m.q("revoked_at"),
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .bind(DEVICE_REVOKED)
        .bind(DEVICE_ACTIVE)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("revoke_device", &err))?;
    Ok(done.rows_affected() == 1)
}

// ── key packages ───────────────────────────────────────────────────────────

/// Insert a one-time package; `false` when the same package was already stored.
pub(crate) async fn insert_key_package(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
    key_package: &[u8],
    key_package_ref: &str,
    last_resort: bool,
) -> Result<bool, Status> {
    let m = &models().key_package;
    let sql = format!(
        "INSERT INTO {rel} ({tenant}, {device}, {kp}, {kp_ref}, {last_resort}) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT ({tenant}, {kp_ref}) DO NOTHING",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        kp = m.q("key_package"),
        kp_ref = m.q("key_package_ref"),
        last_resort = m.q("last_resort"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .bind(key_package)
        .bind(key_package_ref)
        .bind(last_resort)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("insert_key_package", &err))?;
    Ok(done.rows_affected() == 1)
}

/// Delete a device's packages: all unclaimed ones (`last_resort` = None), or
/// only its last-resort package (`Some(true)`).
pub(crate) async fn delete_key_packages(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
    only_last_resort: bool,
) -> Result<u64, Status> {
    let m = &models().key_package;
    let sql = format!(
        "DELETE FROM {rel} WHERE {tenant} = $1 AND {device} = $2 AND {claimed} IS NULL \
         AND (NOT $3 OR {last_resort})",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        claimed = m.q("claimed_at"),
        last_resort = m.q("last_resort"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .bind(only_last_resort)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("delete_key_packages", &err))?;
    Ok(done.rows_affected())
}

/// `(unclaimed one-time packages, has a last-resort package)`.
pub(crate) async fn key_package_stock(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
) -> Result<(i64, bool), Status> {
    let m = &models().key_package;
    let sql = format!(
        "SELECT COUNT(*) FILTER (WHERE NOT {last_resort} AND {claimed} IS NULL) AS one_time, \
                COUNT(*) FILTER (WHERE {last_resort}) AS last_resort \
         FROM {rel} WHERE {tenant} = $1 AND {device} = $2",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        claimed = m.q("claimed_at"),
        last_resort = m.q("last_resort"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("key_package_stock", &err))?;
    let one_time: i64 = decode(&row, "one_time")?;
    let last_resort: i64 = decode(&row, "last_resort")?;
    Ok((one_time, last_resort > 0))
}

/// The claim statement: stamp ONE unclaimed one-time package of the device and
/// return it. `FOR UPDATE SKIP LOCKED` makes concurrent claimers pick different
/// rows, and the outer `claimed_at IS NULL` re-check makes a row that was
/// claimed between the subselect and the update impossible to hand out twice.
/// Binds: `$1` tenant, `$2` device, `$3` claimer. Exposed for the SQL-shape test.
pub(crate) fn claim_one_time_sql(m: &NativeModel) -> String {
    format!(
        "UPDATE {rel} SET {claimed} = NOW(), {claimed_by} = $3 \
         WHERE {id} = ( \
            SELECT {id} FROM {rel} \
            WHERE {tenant} = $1 AND {device} = $2 AND {claimed} IS NULL AND NOT {last_resort} \
            ORDER BY {published}, {id} \
            LIMIT 1 \
            FOR UPDATE SKIP LOCKED) \
           AND {claimed} IS NULL \
         RETURNING {kp} AS key_package, {kp_ref} AS key_package_ref",
        rel = m.relation,
        id = m.q("id"),
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        claimed = m.q("claimed_at"),
        claimed_by = m.q("claimed_by_device_id"),
        last_resort = m.q("last_resort"),
        published = m.q("published_at"),
        kp = m.q("key_package"),
        kp_ref = m.q("key_package_ref"),
    )
}

/// Claim one package for `device_id`: a one-time package (consumed), else the
/// last-resort package (not consumed). `(bytes, ref, last_resort)`.
pub(crate) async fn claim_key_package(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
    claimer_device_id: &str,
) -> Result<Option<(Vec<u8>, String, bool)>, Status> {
    let m = &models().key_package;
    let row = sqlx::query(&claim_one_time_sql(m))
        .bind(tenant_id)
        .bind(device_id)
        .bind(claimer_device_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("claim_key_package", &err))?;
    if let Some(row) = row {
        return Ok(Some((
            decode(&row, "key_package")?,
            decode(&row, "key_package_ref")?,
            false,
        )));
    }
    let sql = format!(
        "SELECT {kp} AS key_package, {kp_ref} AS key_package_ref FROM {rel} \
         WHERE {tenant} = $1 AND {device} = $2 AND {last_resort} \
         ORDER BY {published} DESC LIMIT 1",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        last_resort = m.q("last_resort"),
        published = m.q("published_at"),
        kp = m.q("key_package"),
        kp_ref = m.q("key_package_ref"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("claim_last_resort", &err))?;
    row.map(|row| {
        Ok((
            decode(&row, "key_package")?,
            decode(&row, "key_package_ref")?,
            true,
        ))
    })
    .transpose()
}

// ── groups and members ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct GroupRow {
    pub(crate) epoch: i64,
    pub(crate) public_group_state: Vec<u8>,
    pub(crate) group_info: Option<Vec<u8>>,
    pub(crate) group_info_epoch: i64,
    pub(crate) created_by_device_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowLock {
    None,
    Share,
    Update,
}

pub(crate) async fn load_group(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    lock: RowLock,
) -> Result<Option<GroupRow>, Status> {
    let m = &models().group;
    let lock_clause = match lock {
        RowLock::None => "",
        RowLock::Share => " FOR SHARE",
        RowLock::Update => " FOR UPDATE",
    };
    let sql = format!(
        "SELECT {epoch} AS epoch, {state} AS public_group_state, {info} AS group_info, \
                {info_epoch} AS group_info_epoch, {creator} AS created_by_device_id \
         FROM {rel} WHERE {tenant} = $1 AND {group} = $2{lock_clause}",
        rel = m.relation,
        epoch = m.q("epoch"),
        state = m.q("public_group_state"),
        info = m.q("group_info"),
        info_epoch = m.q("group_info_epoch"),
        creator = m.q("created_by_device_id"),
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(group_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("load_group", &err))?;
    row.map(|row| {
        Ok(GroupRow {
            epoch: decode(&row, "epoch")?,
            public_group_state: decode(&row, "public_group_state")?,
            group_info: decode(&row, "group_info")?,
            group_info_epoch: decode(&row, "group_info_epoch")?,
            created_by_device_id: decode(&row, "created_by_device_id")?,
        })
    })
    .transpose()
}

/// Insert a group; `false` when (tenant, group_id) already exists.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn insert_group(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    epoch: i64,
    state: &[u8],
    group_info: &[u8],
    creator_device_id: &str,
) -> Result<bool, Status> {
    let m = &models().group;
    let sql = format!(
        "INSERT INTO {rel} ({tenant}, {group}, {epoch}, {state}, {info}, {info_epoch}, {creator}) \
         VALUES ($1, $2, $3, $4, $5, $3, $6) \
         ON CONFLICT ({tenant}, {group}) DO NOTHING",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        epoch = m.q("epoch"),
        state = m.q("public_group_state"),
        info = m.q("group_info"),
        info_epoch = m.q("group_info_epoch"),
        creator = m.q("created_by_device_id"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(group_id)
        .bind(epoch)
        .bind(state)
        .bind(group_info)
        .bind(creator_device_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("insert_group", &err))?;
    Ok(done.rows_affected() == 1)
}

/// Persist the post-commit public state. The `epoch = $3` guard makes a write
/// against an epoch that moved underneath impossible even if the row lock were
/// ever dropped from the caller.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn update_group_state(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    expected_epoch: i64,
    new_epoch: i64,
    state: &[u8],
    group_info: Option<&[u8]>,
) -> Result<bool, Status> {
    let m = &models().group;
    let sql = format!(
        "UPDATE {rel} SET {epoch} = $4, {state} = $5, \
            {info} = COALESCE($6, {info}), \
            {info_epoch} = CASE WHEN $6 IS NULL THEN {info_epoch} ELSE $4 END, \
            {updated} = NOW() \
         WHERE {tenant} = $1 AND {group} = $2 AND {epoch} = $3",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        epoch = m.q("epoch"),
        state = m.q("public_group_state"),
        info = m.q("group_info"),
        info_epoch = m.q("group_info_epoch"),
        updated = m.q("updated_at"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(group_id)
        .bind(expected_epoch)
        .bind(new_epoch)
        .bind(state)
        .bind(group_info)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("update_group_state", &err))?;
    Ok(done.rows_affected() == 1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemberRow {
    pub(crate) device_id: String,
    pub(crate) subject_id: String,
    pub(crate) leaf_index: i32,
    pub(crate) joined_epoch: i64,
}

pub(crate) async fn list_members(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
) -> Result<Vec<MemberRow>, Status> {
    let m = &models().member;
    let sql = format!(
        "SELECT {device} AS device_id, {subject} AS subject_id, {leaf} AS leaf_index, \
                {joined} AS joined_epoch \
         FROM {rel} WHERE {tenant} = $1 AND {group} = $2 ORDER BY {device}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        device = m.q("device_id"),
        subject = m.q("subject_id"),
        leaf = m.q("leaf_index"),
        joined = m.q("joined_epoch"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(group_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("list_members", &err))?;
    rows.iter()
        .map(|row| {
            Ok(MemberRow {
                device_id: decode(row, "device_id")?,
                subject_id: decode(row, "subject_id")?,
                leaf_index: decode(row, "leaf_index")?,
                joined_epoch: decode(row, "joined_epoch")?,
            })
        })
        .collect()
}

/// Replace the roster with `members` (derived from the validated tree).
pub(crate) async fn replace_members(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    members: &[MemberRow],
) -> Result<(), Status> {
    let m = &models().member;
    let delete = format!(
        "DELETE FROM {rel} WHERE {tenant} = $1 AND {group} = $2",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
    );
    sqlx::query(&delete)
        .bind(tenant_id)
        .bind(group_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("replace_members_delete", &err))?;
    let insert = format!(
        "INSERT INTO {rel} ({tenant}, {group}, {device}, {subject}, {leaf}, {joined}) \
         SELECT $1, $2, device_id, subject_id, leaf_index, joined_epoch \
         FROM UNNEST($3::TEXT[], $4::TEXT[], $5::INT[], $6::BIGINT[]) \
              AS t(device_id, subject_id, leaf_index, joined_epoch)",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        device = m.q("device_id"),
        subject = m.q("subject_id"),
        leaf = m.q("leaf_index"),
        joined = m.q("joined_epoch"),
    );
    sqlx::query(&insert)
        .bind(tenant_id)
        .bind(group_id)
        .bind(
            members
                .iter()
                .map(|m| m.device_id.clone())
                .collect::<Vec<_>>(),
        )
        .bind(
            members
                .iter()
                .map(|m| m.subject_id.clone())
                .collect::<Vec<_>>(),
        )
        .bind(members.iter().map(|m| m.leaf_index).collect::<Vec<_>>())
        .bind(members.iter().map(|m| m.joined_epoch).collect::<Vec<_>>())
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("replace_members_insert", &err))?;
    Ok(())
}

/// Groups a device belongs to, with its leaf index there.
pub(crate) async fn groups_of_device(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
) -> Result<Vec<(String, i32)>, Status> {
    let m = &models().member;
    let sql = format!(
        "SELECT {group} AS group_id, {leaf} AS leaf_index FROM {rel} \
         WHERE {tenant} = $1 AND {device} = $2 ORDER BY {group}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        device = m.q("device_id"),
        leaf = m.q("leaf_index"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("groups_of_device", &err))?;
    rows.iter()
        .map(|row| Ok((decode(row, "group_id")?, decode(row, "leaf_index")?)))
        .collect()
}

// ── inbox ──────────────────────────────────────────────────────────────────

/// One inbox row to append.
#[derive(Debug, Clone)]
pub(crate) struct InboxAppend<'a> {
    pub(crate) device_id: &'a str,
    pub(crate) kind: &'a str,
    pub(crate) group_id: &'a str,
    pub(crate) epoch: i64,
    pub(crate) payload: &'a [u8],
    pub(crate) sender_device_id: &'a str,
    pub(crate) client_message_id: &'a str,
}

/// Allocate the device's next inbox seq (row-locks the device) and insert the
/// row. Callers append in ascending device-id order.
pub(crate) async fn append_inbox(
    conn: &mut PgConnection,
    tenant_id: &str,
    entry: &InboxAppend<'_>,
) -> Result<i64, Status> {
    let d = &models().device;
    let allocate = format!(
        "UPDATE {rel} SET {head} = {head} + 1 WHERE {tenant} = $1 AND {device} = $2 \
         RETURNING {head} AS seq",
        rel = d.relation,
        head = d.q("inbox_head_seq"),
        tenant = d.q("tenant_id"),
        device = d.q("device_id"),
    );
    let row = sqlx::query(&allocate)
        .bind(tenant_id)
        .bind(entry.device_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("allocate_inbox_seq", &err))?
        .ok_or_else(|| {
            messaging_internal_status(
                "allocate_inbox_seq",
                format!("inbox target device '{}' vanished", entry.device_id),
            )
        })?;
    let seq: i64 = decode(&row, "seq")?;
    let m = &models().inbox;
    let insert = format!(
        "INSERT INTO {rel} ({tenant}, {device}, {seq}, {kind}, {group}, {epoch}, {payload}, \
                            {sender}, {client}) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        seq = m.q("seq"),
        kind = m.q("kind"),
        group = m.q("group_id"),
        epoch = m.q("epoch"),
        payload = m.q("payload"),
        sender = m.q("sender_device_id"),
        client = m.q("client_message_id"),
    );
    sqlx::query(&insert)
        .bind(tenant_id)
        .bind(entry.device_id)
        .bind(seq)
        .bind(entry.kind)
        .bind(entry.group_id)
        .bind(entry.epoch)
        .bind(entry.payload)
        .bind(entry.sender_device_id)
        .bind(entry.client_message_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("append_inbox", &err))?;
    Ok(seq)
}

/// A prior delivery under the same client idempotency key: `(epoch, rows)`.
pub(crate) async fn find_client_message(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    client_message_id: &str,
    kinds: &[&str],
) -> Result<Option<(i64, i64)>, Status> {
    let m = &models().inbox;
    let sql = format!(
        "SELECT MAX({epoch}) AS epoch, COUNT(*) AS rows FROM {rel} \
         WHERE {tenant} = $1 AND {group} = $2 AND {client} = $3 AND {kind} = ANY($4)",
        rel = m.relation,
        epoch = m.q("epoch"),
        tenant = m.q("tenant_id"),
        group = m.q("group_id"),
        client = m.q("client_message_id"),
        kind = m.q("kind"),
    );
    let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(group_id)
        .bind(client_message_id)
        .bind(&kinds)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("find_client_message", &err))?;
    let rows: i64 = decode(&row, "rows")?;
    if rows == 0 {
        return Ok(None);
    }
    let epoch: Option<i64> = decode(&row, "epoch")?;
    Ok(Some((epoch.unwrap_or(0), rows)))
}

/// Serialize concurrent requests that share one client idempotency key, for the
/// rest of the transaction.
pub(crate) async fn lock_client_message_key(
    conn: &mut PgConnection,
    tenant_id: &str,
    group_id: &str,
    client_message_id: &str,
) -> Result<(), Status> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!(
            "udb:messaging:client-message:{tenant_id}:{group_id}:{client_message_id}"
        ))
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("lock_client_message_key", &err))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct InboxRow {
    pub(crate) device_id: String,
    pub(crate) seq: i64,
    pub(crate) kind: String,
    pub(crate) group_id: String,
    pub(crate) epoch: i64,
    pub(crate) payload: Vec<u8>,
    pub(crate) sender_device_id: String,
    pub(crate) client_message_id: String,
    pub(crate) created_at_unix: i64,
}

pub(crate) async fn read_inbox(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
    after_seq: i64,
    limit: i64,
) -> Result<Vec<InboxRow>, Status> {
    let m = &models().inbox;
    let sql = format!(
        "SELECT {device} AS device_id, {seq} AS seq, {kind} AS kind, {group} AS group_id, \
                {epoch} AS epoch, {payload} AS payload, {sender} AS sender_device_id, \
                {client} AS client_message_id, {created} \
         FROM {rel} WHERE {tenant} = $1 AND {device} = $2 AND {seq} > $3 \
         ORDER BY {seq} LIMIT $4",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        seq = m.q("seq"),
        kind = m.q("kind"),
        group = m.q("group_id"),
        epoch = m.q("epoch"),
        payload = m.q("payload"),
        sender = m.q("sender_device_id"),
        client = m.q("client_message_id"),
        created = unix(&m.q("created_at"), "created_at_unix"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(device_id)
        .bind(after_seq)
        .bind(limit)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("read_inbox", &err))?;
    rows.iter()
        .map(|row| {
            Ok(InboxRow {
                device_id: decode(row, "device_id")?,
                seq: decode(row, "seq")?,
                kind: decode(row, "kind")?,
                group_id: decode(row, "group_id")?,
                epoch: decode(row, "epoch")?,
                payload: decode(row, "payload")?,
                sender_device_id: decode(row, "sender_device_id")?,
                client_message_id: decode(row, "client_message_id")?,
                created_at_unix: decode(row, "created_at_unix")?,
            })
        })
        .collect()
}

/// Acknowledge entries up to `up_to_seq` (clamped to the device's allocated
/// head so the cursor can never skip entries that do not exist yet). Returns
/// `(acked cursor after the call, rows newly acknowledged)`.
pub(crate) async fn ack_inbox(
    conn: &mut PgConnection,
    tenant_id: &str,
    device_id: &str,
    up_to_seq: i64,
) -> Result<(i64, i64), Status> {
    let d = &models().device;
    let cursor = format!(
        "UPDATE {rel} SET {acked} = GREATEST({acked}, LEAST($3, {head})) \
         WHERE {tenant} = $1 AND {device} = $2 RETURNING {acked} AS acked_seq",
        rel = d.relation,
        acked = d.q("acked_seq"),
        head = d.q("inbox_head_seq"),
        tenant = d.q("tenant_id"),
        device = d.q("device_id"),
    );
    let row = sqlx::query(&cursor)
        .bind(tenant_id)
        .bind(device_id)
        .bind(up_to_seq)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("ack_inbox_cursor", &err))?;
    let acked_seq: i64 = decode(&row, "acked_seq")?;
    let m = &models().inbox;
    let mark = format!(
        "UPDATE {rel} SET {acked_at} = NOW() \
         WHERE {tenant} = $1 AND {device} = $2 AND {seq} <= $3 AND {acked_at} IS NULL",
        rel = m.relation,
        acked_at = m.q("acked_at"),
        tenant = m.q("tenant_id"),
        device = m.q("device_id"),
        seq = m.q("seq"),
    );
    let done = sqlx::query(&mark)
        .bind(tenant_id)
        .bind(device_id)
        .bind(acked_seq)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("ack_inbox_rows", &err))?;
    Ok((acked_seq, done.rows_affected() as i64))
}

/// Retention statement: delete up to `$2` inbox rows that are acknowledged or
/// older than `$1` seconds. `SKIP LOCKED` keeps it off rows a live transaction
/// holds. Exposed for the SQL-shape test.
pub(crate) fn inbox_retention_sql(m: &NativeModel) -> String {
    format!(
        "DELETE FROM {rel} WHERE {id} IN ( \
            SELECT {id} FROM {rel} \
            WHERE {acked_at} IS NOT NULL OR {created} < NOW() - make_interval(secs => $1) \
            LIMIT $2 \
            FOR UPDATE SKIP LOCKED)",
        rel = m.relation,
        id = m.q("id"),
        acked_at = m.q("acked_at"),
        created = m.q("created_at"),
    )
}

/// Claimed one-time KeyPackages are spent; drop them after the inbox TTL.
pub(crate) fn spent_key_package_retention_sql(m: &NativeModel) -> String {
    format!(
        "DELETE FROM {rel} WHERE {id} IN ( \
            SELECT {id} FROM {rel} \
            WHERE {claimed} IS NOT NULL AND {claimed} < NOW() - make_interval(secs => $1) \
            LIMIT $2 \
            FOR UPDATE SKIP LOCKED)",
        rel = m.relation,
        id = m.q("id"),
        claimed = m.q("claimed_at"),
    )
}

// ── backups ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct BackupRow {
    pub(crate) subject_id: String,
    pub(crate) backup_id: String,
    pub(crate) size_bytes: i64,
    pub(crate) ciphertext_sha256: String,
    pub(crate) version: i64,
    pub(crate) created_at_unix: i64,
    pub(crate) updated_at_unix: i64,
}

fn backup_columns(m: &NativeModel) -> String {
    format!(
        "{subject} AS subject_id, {backup} AS backup_id, {size} AS size_bytes, \
         {digest} AS ciphertext_sha256, {version} AS version, {created}, {updated}",
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
        size = m.q("size_bytes"),
        digest = m.q("ciphertext_sha256"),
        version = m.q("version"),
        created = unix(&m.q("created_at"), "created_at_unix"),
        updated = unix(&m.q("updated_at"), "updated_at_unix"),
    )
}

fn backup_from_row(row: &sqlx::postgres::PgRow) -> Result<BackupRow, Status> {
    Ok(BackupRow {
        subject_id: decode(row, "subject_id")?,
        backup_id: decode(row, "backup_id")?,
        size_bytes: decode(row, "size_bytes")?,
        ciphertext_sha256: decode(row, "ciphertext_sha256")?,
        version: decode(row, "version")?,
        created_at_unix: decode(row, "created_at_unix")?,
        updated_at_unix: decode(row, "updated_at_unix")?,
    })
}

/// The stored backup and its envelope.
pub(crate) async fn load_backup(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
    backup_id: &str,
) -> Result<Option<(BackupRow, Vec<u8>)>, Status> {
    let m = &models().backup;
    let sql = format!(
        "SELECT {cols}, {ciphertext} AS ciphertext FROM {rel} \
         WHERE {tenant} = $1 AND {subject} = $2 AND {backup} = $3",
        cols = backup_columns(m),
        ciphertext = m.q("ciphertext"),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .bind(backup_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("load_backup", &err))?;
    row.map(|row| Ok((backup_from_row(&row)?, decode(&row, "ciphertext")?)))
        .transpose()
}

/// The stored backup's version, 0 when none is stored.
pub(crate) async fn backup_version(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
    backup_id: &str,
) -> Result<i64, Status> {
    let m = &models().backup;
    let sql = format!(
        "SELECT {version} AS version FROM {rel} \
         WHERE {tenant} = $1 AND {subject} = $2 AND {backup} = $3",
        version = m.q("version"),
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
    );
    let row = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .bind(backup_id)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("backup_version", &err))?;
    row.map(|row| decode(&row, "version"))
        .transpose()
        .map(Option::unwrap_or_default)
}

/// One backup write: the envelope, its size and digest.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BackupWrite<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) subject_id: &'a str,
    pub(crate) backup_id: &'a str,
    pub(crate) ciphertext: &'a [u8],
    pub(crate) ciphertext_sha256: &'a str,
}

/// Store or replace a backup unconditionally: `(row, created)`. One statement,
/// so two concurrent first writes cannot both insert.
pub(crate) async fn upsert_backup(
    conn: &mut PgConnection,
    write: BackupWrite<'_>,
) -> Result<(BackupRow, bool), Status> {
    let m = &models().backup;
    let sql = format!(
        "INSERT INTO {rel} AS b ({tenant}, {subject}, {backup}, {ciphertext}, {size}, {digest}) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT ({tenant}, {subject}, {backup}) DO UPDATE SET \
            {ciphertext} = EXCLUDED.{ciphertext}, {size} = EXCLUDED.{size}, \
            {digest} = EXCLUDED.{digest}, {version} = b.{version} + 1, {updated} = NOW() \
         RETURNING {cols}, (xmax = 0) AS created",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
        ciphertext = m.q("ciphertext"),
        size = m.q("size_bytes"),
        digest = m.q("ciphertext_sha256"),
        version = m.q("version"),
        updated = m.q("updated_at"),
        cols = backup_columns(m),
    );
    let row = sqlx::query(&sql)
        .bind(write.tenant_id)
        .bind(write.subject_id)
        .bind(write.backup_id)
        .bind(write.ciphertext)
        .bind(write.ciphertext.len() as i64)
        .bind(write.ciphertext_sha256)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("upsert_backup", &err))?;
    Ok((backup_from_row(&row)?, decode(&row, "created")?))
}

/// Replace a backup only while it is at `expected_version`; `None` when it is
/// not (or no backup is stored).
pub(crate) async fn replace_backup_at_version(
    conn: &mut PgConnection,
    write: BackupWrite<'_>,
    expected_version: i64,
) -> Result<Option<BackupRow>, Status> {
    let m = &models().backup;
    let sql = format!(
        "UPDATE {rel} SET {ciphertext} = $4, {size} = $5, {digest} = $6, \
            {version} = {version} + 1, {updated} = NOW() \
         WHERE {tenant} = $1 AND {subject} = $2 AND {backup} = $3 AND {version} = $7 \
         RETURNING {cols}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
        ciphertext = m.q("ciphertext"),
        size = m.q("size_bytes"),
        digest = m.q("ciphertext_sha256"),
        version = m.q("version"),
        updated = m.q("updated_at"),
        cols = backup_columns(m),
    );
    let row = sqlx::query(&sql)
        .bind(write.tenant_id)
        .bind(write.subject_id)
        .bind(write.backup_id)
        .bind(write.ciphertext)
        .bind(write.ciphertext.len() as i64)
        .bind(write.ciphertext_sha256)
        .bind(expected_version)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("replace_backup_at_version", &err))?;
    row.as_ref().map(backup_from_row).transpose()
}

/// Delete a backup; `false` when none was stored.
pub(crate) async fn delete_backup(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
    backup_id: &str,
) -> Result<bool, Status> {
    let m = &models().backup;
    let sql = format!(
        "DELETE FROM {rel} WHERE {tenant} = $1 AND {subject} = $2 AND {backup} = $3",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        backup = m.q("backup_id"),
    );
    let done = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .bind(backup_id)
        .execute(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("delete_backup", &err))?;
    Ok(done.rows_affected() == 1)
}

// ── subject erasure ────────────────────────────────────────────────────────

/// Whether the service's tables exist in this store (a store the messaging
/// migrations never ran on has none).
pub(crate) async fn messaging_tables_exist(conn: &mut PgConnection) -> Result<bool, Status> {
    let mut relations: Vec<String> = Vec::new();
    let all = models();
    for m in [
        &all.device,
        &all.key_package,
        &all.group,
        &all.member,
        &all.inbox,
        &all.backup,
    ] {
        relations.push(m.relation.clone());
    }
    let present: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM UNNEST($1::TEXT[]) AS r(name) WHERE to_regclass(r.name) IS NOT NULL",
    )
    .bind(&relations)
    .fetch_one(&mut *conn)
    .await
    .map_err(|err| messaging_store_status("messaging_tables_exist", &err))?;
    Ok(present == relations.len() as i64)
}

/// Every device of a subject, any status.
pub(crate) async fn subject_device_ids(
    conn: &mut PgConnection,
    tenant_id: &str,
    subject_id: &str,
) -> Result<Vec<String>, Status> {
    let m = &models().device;
    let sql = format!(
        "SELECT {device} AS device_id FROM {rel} WHERE {tenant} = $1 AND {subject} = $2 \
         ORDER BY {device}",
        rel = m.relation,
        tenant = m.q("tenant_id"),
        subject = m.q("subject_id"),
        device = m.q("device_id"),
    );
    let rows = sqlx::query(&sql)
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| messaging_store_status("subject_device_ids", &err))?;
    rows.iter().map(|row| decode(row, "device_id")).collect()
}

/// Erase (or, with `delete = false`, count) the tenant's rows of `m` whose
/// `column` is one of `values`. Returns the row count.
pub(crate) async fn erase_rows(
    conn: &mut PgConnection,
    m: &NativeModel,
    column: &str,
    tenant_id: &str,
    values: &[String],
    delete: bool,
) -> Result<i64, Status> {
    if values.is_empty() {
        return Ok(0);
    }
    let predicate = format!(
        "{tenant} = $1 AND {column} = ANY($2)",
        tenant = m.q("tenant_id"),
        column = m.q(column),
    );
    if delete {
        let sql = format!("DELETE FROM {rel} WHERE {predicate}", rel = m.relation);
        let done = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(values)
            .execute(&mut *conn)
            .await
            .map_err(|err| messaging_store_status("erase_rows", &err))?;
        Ok(done.rows_affected() as i64)
    } else {
        let sql = format!(
            "SELECT COUNT(*) AS rows FROM {rel} WHERE {predicate}",
            rel = m.relation
        );
        let row = sqlx::query(&sql)
            .bind(tenant_id)
            .bind(values)
            .fetch_one(&mut *conn)
            .await
            .map_err(|err| messaging_store_status("count_rows", &err))?;
        decode(&row, "rows")
    }
}
