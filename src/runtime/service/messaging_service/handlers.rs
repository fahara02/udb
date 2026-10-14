//! The twelve `MessagingService` RPC handlers, as free functions taking `svc`;
//! `mod.rs` delegates one line to each.
//!
//! Every handler: binds the body tenant to the verified claim first, validates
//! the request, admits on the tenant's fair-share channel, resolves the
//! service's Postgres store, and runs its reads/writes in ONE tenant-scoped
//! transaction. Writes that append inbox rows enqueue one
//! `udb.messaging.inbox.appended.v1` outbox event per row in that same
//! transaction.

use std::collections::{BTreeMap, BTreeSet};

use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::runtime::channels::OperationChannel;

use super::super::native_helpers::{admit_on as native_admit_on, validate_request_tenant};
use super::MessagingServiceImpl;
use super::config::{
    DEVICE_ACTIVE, INBOX_PAGE, INBOX_POLL_INTERVAL, KIND_COMMIT, KIND_MESSAGE, KIND_REMOVAL_HINT,
    KIND_WELCOME, MAX_CLAIM_TARGETS, MAX_GROUP_MEMBERS, MAX_KEY_PACKAGE_STOCK,
    MAX_KEY_PACKAGES_PER_PUBLISH,
};
use super::errors::{
    device_not_active_status, device_not_found_status, group_not_found_status, invalid_field,
    messaging_already_exists_status, messaging_internal_status, messaging_policy_status,
    mls_invalid, not_a_member_status, stale_epoch_status,
};
use super::events::emit_inbox_appended;
use super::mls;
use super::model::{
    caller_user_subject, device_dto, ensure_device_owner, inbox_dto, key_fingerprint,
    removal_hint_payload, require_mls_bytes, resolve_subject, validate_client_message_id,
    validate_device_id, validate_group_id,
};
use super::store::{self, DeviceRow, InboxAppend, MemberRow, RowLock, begin_tenant_tx, commit};

fn mls_status(reject: mls::MlsReject) -> Status {
    mls_invalid(reject.field, reject.reason)
}

/// Common prologue: tenant binding + admission.
async fn admit(
    svc: &MessagingServiceImpl,
    metadata: &MetadataMap,
    tenant_id: &str,
    op: OperationChannel,
) -> Result<(String, Option<crate::runtime::channels::ChannelPermit>), Status> {
    validate_request_tenant(metadata, tenant_id)?;
    let tenant_id = tenant_id.trim().to_string();
    let permit = native_admit_on(
        svc.channels.as_ref(),
        &svc.metrics,
        "messaging",
        op,
        &tenant_id,
        None,
    )
    .await?;
    Ok((tenant_id, permit))
}

/// Load a device that must exist, be ACTIVE, and belong to the caller.
async fn require_own_active_device(
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    device_id: &str,
    caller: Option<&str>,
    operation: &'static str,
) -> Result<DeviceRow, Status> {
    let device = store::load_device(conn, tenant_id, device_id)
        .await?
        .ok_or_else(|| device_not_found_status(operation, device_id))?;
    ensure_device_owner(caller, &device, operation)?;
    if !device.is_active() {
        return Err(device_not_active_status(operation, device_id));
    }
    Ok(device)
}

/// Append the given entries (any order) and their events. Entries are sorted by
/// device id and every target row is locked first, in that order, so concurrent
/// fan-outs cannot deadlock. Returns the number of rows appended.
async fn fan_out(
    svc: &MessagingServiceImpl,
    conn: &mut sqlx::PgConnection,
    tenant_id: &str,
    project_id: &str,
    mut entries: Vec<InboxAppend<'_>>,
) -> Result<i32, Status> {
    entries.sort_by(|a, b| a.device_id.cmp(b.device_id));
    let targets: Vec<String> = entries
        .iter()
        .map(|entry| entry.device_id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    store::lock_devices(conn, tenant_id, &targets).await?;
    let mut appended = 0i32;
    for entry in &entries {
        let seq = store::append_inbox(conn, tenant_id, entry).await?;
        emit_inbox_appended(
            conn,
            svc.outbox_relation.as_deref(),
            tenant_id,
            project_id,
            entry.device_id,
            seq,
        )
        .await?;
        appended += 1;
    }
    Ok(appended)
}

// ── device directory ───────────────────────────────────────────────────────

pub(crate) async fn register_device(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::RegisterDeviceRequest>,
) -> Result<Response<msg_pb::RegisterDeviceResponse>, Status> {
    const OP: &str = "register_device";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Admin).await?;
    let caller = caller_user_subject();
    let subject_id = resolve_subject(caller.as_deref(), "subject_id", &req.subject_id, OP)?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    if req.signature_public_key.len() != 32 {
        return Err(invalid_field(
            "signature_public_key",
            "must be a raw 32-byte Ed25519 public key",
        ));
    }
    require_mls_bytes("credential", &req.credential)?;
    let identity =
        mls::basic_credential_identity("credential", &req.credential).map_err(mls_status)?;
    if identity != mls::device_identity(&tenant_id, &subject_id, &device_id).as_bytes() {
        return Err(invalid_field(
            "credential",
            "the BasicCredential identity must be <tenant_id>/<subject_id>/<device_id>",
        ));
    }
    let fingerprint = key_fingerprint(&req.signature_public_key);

    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let inserted = store::insert_device(
        &mut tx,
        &tenant_id,
        &subject_id,
        &device_id,
        &req.signature_public_key,
        &req.credential,
        &fingerprint,
    )
    .await?;
    let (device, created) = match inserted {
        Some(device) => (device, true),
        None => {
            let existing = store::load_device(&mut tx, &tenant_id, &device_id)
                .await?
                .ok_or_else(|| {
                    messaging_internal_status(OP, "device conflicted but could not be read")
                })?;
            let identical = existing.subject_id == subject_id
                && existing.signature_public_key == req.signature_public_key
                && existing.credential == req.credential
                && existing.status == DEVICE_ACTIVE;
            if !identical {
                return Err(messaging_already_exists_status(
                    OP,
                    "messaging_device_exists",
                    format!(
                        "device '{device_id}' is already registered with a different key or owner, \
                         or was revoked; register a new device id"
                    ),
                ));
            }
            (existing, false)
        }
    };
    commit(tx).await?;
    Ok(Response::new(msg_pb::RegisterDeviceResponse {
        device: Some(device_dto(&tenant_id, &device)),
        created,
        message: if created {
            "device registered".to_string()
        } else {
            "device already registered".to_string()
        },
        error: None,
    }))
}

pub(crate) async fn list_devices(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::ListDevicesRequest>,
) -> Result<Response<msg_pb::ListDevicesResponse>, Status> {
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Read).await?;
    // Any principal in the tenant may list another principal's devices: peers
    // need the keys to verify safety numbers. An empty subject means "mine".
    let subject_id = match caller_user_subject() {
        Some(caller) if req.subject_id.trim().is_empty() => caller,
        _ => super::model::validate_subject_id("subject_id", &req.subject_id)?,
    };
    let (_context, pool) = svc.store(&metadata, &tenant_id, false)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let devices =
        store::list_devices(&mut tx, &tenant_id, &subject_id, req.include_revoked).await?;
    commit(tx).await?;
    Ok(Response::new(msg_pb::ListDevicesResponse {
        devices: devices
            .iter()
            .map(|device| device_dto(&tenant_id, device))
            .collect(),
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn revoke_device(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::RevokeDeviceRequest>,
) -> Result<Response<msg_pb::RevokeDeviceResponse>, Status> {
    const OP: &str = "revoke_device";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Admin).await?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    let reason: String = req.reason.trim().chars().take(256).collect();
    let caller = caller_user_subject();
    let (context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let device = store::load_device(&mut tx, &tenant_id, &device_id)
        .await?
        .ok_or_else(|| device_not_found_status(OP, &device_id))?;
    ensure_device_owner(caller.as_deref(), &device, OP)?;
    if !device.is_active() {
        commit(tx).await?;
        return Ok(Response::new(msg_pb::RevokeDeviceResponse {
            revoked: true,
            group_ids: Vec::new(),
            message: "device already revoked".to_string(),
            error: None,
        }));
    }

    // Removal hints go to every OTHER active member of each group the device is in.
    let groups = store::groups_of_device(&mut tx, &tenant_id, &device_id).await?;
    let mut hints: Vec<(String, String, Vec<u8>)> = Vec::new();
    for (group_id, leaf_index) in &groups {
        let payload = removal_hint_payload(group_id, &device_id, *leaf_index, &reason);
        for member in store::list_members(&mut tx, &tenant_id, group_id).await? {
            if member.device_id != device_id {
                hints.push((member.device_id, group_id.clone(), payload.clone()));
            }
        }
    }
    let mut involved: BTreeSet<String> = hints.iter().map(|(d, _, _)| d.clone()).collect();
    involved.insert(device_id.clone());
    let involved: Vec<String> = involved.into_iter().collect();
    let locked = store::lock_devices(&mut tx, &tenant_id, &involved).await?;
    store::revoke_device(&mut tx, &tenant_id, &device_id).await?;
    store::delete_key_packages(&mut tx, &tenant_id, &device_id, false).await?;
    let entries: Vec<InboxAppend<'_>> = hints
        .iter()
        .filter(|(target, _, _)| locked.get(target).is_some_and(DeviceRow::is_active))
        .map(|(target, group_id, payload)| InboxAppend {
            device_id: target,
            kind: KIND_REMOVAL_HINT,
            group_id,
            epoch: 0,
            payload,
            sender_device_id: &device_id,
            client_message_id: "",
        })
        .collect();
    let appended = fan_out(svc, &mut tx, &tenant_id, &context.project_id, entries).await?;
    commit(tx).await?;
    if appended > 0 {
        svc.inbox_wake.notify_waiters();
    }
    Ok(Response::new(msg_pb::RevokeDeviceResponse {
        revoked: true,
        group_ids: groups.into_iter().map(|(group_id, _)| group_id).collect(),
        message: "device revoked".to_string(),
        error: None,
    }))
}

// ── key packages ───────────────────────────────────────────────────────────

pub(crate) async fn publish_key_packages(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::PublishKeyPackagesRequest>,
) -> Result<Response<msg_pb::PublishKeyPackagesResponse>, Status> {
    const OP: &str = "publish_key_packages";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    if req.key_packages.is_empty() && req.last_resort_key_package.is_empty() {
        return Err(invalid_field(
            "key_packages",
            "publish at least one KeyPackage or a last-resort KeyPackage",
        ));
    }
    if req.key_packages.len() > MAX_KEY_PACKAGES_PER_PUBLISH {
        return Err(invalid_field(
            "key_packages",
            format!("at most {MAX_KEY_PACKAGES_PER_PUBLISH} KeyPackages per call"),
        ));
    }
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    // Lock the device row: serializes concurrent publishes so the stock bound holds.
    store::lock_devices(&mut tx, &tenant_id, std::slice::from_ref(&device_id)).await?;
    let device =
        require_own_active_device(&mut tx, &tenant_id, &device_id, caller.as_deref(), OP).await?;
    let expected_identity = mls::device_identity(&tenant_id, &device.subject_id, &device_id);
    let check = |field: &'static str, bytes: &[u8]| -> Result<mls::ValidatedKeyPackage, Status> {
        require_mls_bytes(field, bytes)?;
        let package = mls::validate_key_package(field, bytes).map_err(mls_status)?;
        if package.identity != expected_identity.as_bytes() {
            return Err(invalid_field(
                field,
                "KeyPackage credential does not name this device",
            ));
        }
        if package.signature_key != device.signature_public_key {
            return Err(invalid_field(
                field,
                "KeyPackage signature key is not the device's registered key",
            ));
        }
        Ok(package)
    };
    let mut validated = Vec::with_capacity(req.key_packages.len());
    for bytes in &req.key_packages {
        validated.push((bytes, check("key_packages", bytes)?));
    }
    let last_resort = if req.last_resort_key_package.is_empty() {
        None
    } else {
        Some(check(
            "last_resort_key_package",
            &req.last_resort_key_package,
        )?)
    };

    let (stock, _) = store::key_package_stock(&mut tx, &tenant_id, &device_id).await?;
    if stock + validated.len() as i64 > MAX_KEY_PACKAGE_STOCK {
        return Err(crate::runtime::executor_utils::quota_refusal_status(
            "messaging",
            "device key-package stock",
            format!(
                "device key-package stock would exceed {MAX_KEY_PACKAGE_STOCK} unclaimed packages"
            ),
        ));
    }
    let mut accepted = 0i32;
    for (bytes, package) in &validated {
        if store::insert_key_package(
            &mut tx,
            &tenant_id,
            &device_id,
            bytes,
            &package.key_package_ref,
            false,
        )
        .await?
        {
            accepted += 1;
        }
    }
    if let Some(package) = &last_resort {
        store::delete_key_packages(&mut tx, &tenant_id, &device_id, true).await?;
        if !store::insert_key_package(
            &mut tx,
            &tenant_id,
            &device_id,
            &req.last_resort_key_package,
            &package.key_package_ref,
            true,
        )
        .await?
        {
            return Err(invalid_field(
                "last_resort_key_package",
                "this KeyPackage was already published as a one-time package",
            ));
        }
    }
    let (one_time_remaining, has_last_resort) =
        store::key_package_stock(&mut tx, &tenant_id, &device_id).await?;
    commit(tx).await?;
    Ok(Response::new(msg_pb::PublishKeyPackagesResponse {
        accepted,
        one_time_remaining,
        has_last_resort,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn claim_key_packages(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::ClaimKeyPackagesRequest>,
) -> Result<Response<msg_pb::ClaimKeyPackagesResponse>, Status> {
    const OP: &str = "claim_key_packages";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let requester = validate_device_id("requester_device_id", &req.requester_device_id)?;
    let mut target_ids = BTreeSet::new();
    for device_id in &req.target_device_ids {
        target_ids.insert(validate_device_id("target_device_ids", device_id)?);
    }
    let mut subjects = Vec::new();
    for subject in &req.target_subject_ids {
        subjects.push(super::model::validate_subject_id(
            "target_subject_ids",
            subject,
        )?);
    }
    if target_ids.is_empty() && subjects.is_empty() {
        return Err(invalid_field(
            "target_device_ids",
            "name at least one target device or subject",
        ));
    }
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    require_own_active_device(&mut tx, &tenant_id, &requester, caller.as_deref(), OP).await?;
    for (device_id, _subject) in
        store::active_devices_of_subjects(&mut tx, &tenant_id, &subjects).await?
    {
        target_ids.insert(device_id);
    }
    // A device never claims a package of its own.
    target_ids.remove(&requester);
    if target_ids.len() > MAX_CLAIM_TARGETS {
        return Err(invalid_field(
            "target_device_ids",
            format!("at most {MAX_CLAIM_TARGETS} target devices per claim"),
        ));
    }
    let targets: Vec<String> = target_ids.into_iter().collect();
    let directory = store::devices_by_id(&mut tx, &tenant_id, &targets).await?;
    let mut key_packages = Vec::new();
    let mut missing = Vec::new();
    for device_id in &targets {
        let Some(device) = directory.get(device_id).filter(|d| d.is_active()) else {
            missing.push(device_id.clone());
            continue;
        };
        match store::claim_key_package(&mut tx, &tenant_id, device_id, &requester).await? {
            Some((key_package, key_package_ref, last_resort)) => {
                key_packages.push(msg_pb::ClaimedKeyPackage {
                    subject_id: device.subject_id.clone(),
                    device_id: device_id.clone(),
                    key_package,
                    key_package_ref,
                    last_resort,
                })
            }
            None => missing.push(device_id.clone()),
        }
    }
    commit(tx).await?;
    Ok(Response::new(msg_pb::ClaimKeyPackagesResponse {
        key_packages,
        missing_device_ids: missing,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn get_key_package_stock(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::GetKeyPackageStockRequest>,
) -> Result<Response<msg_pb::GetKeyPackageStockResponse>, Status> {
    const OP: &str = "get_key_package_stock";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Read).await?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, false)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let device = store::load_device(&mut tx, &tenant_id, &device_id)
        .await?
        .ok_or_else(|| device_not_found_status(OP, &device_id))?;
    ensure_device_owner(caller.as_deref(), &device, OP)?;
    let (one_time_remaining, has_last_resort) =
        store::key_package_stock(&mut tx, &tenant_id, &device_id).await?;
    commit(tx).await?;
    Ok(Response::new(msg_pb::GetKeyPackageStockResponse {
        one_time_remaining,
        has_last_resort,
        message: String::new(),
        error: None,
    }))
}

// ── groups ─────────────────────────────────────────────────────────────────

/// Map a validated tree to roster rows. Every leaf must name a device of THIS
/// tenant that is registered with exactly the leaf's signature key; devices in
/// `must_be_active` (newly added ones) must also be ACTIVE.
fn roster_from_tree(
    tenant_id: &str,
    leaves: &[mls::MemberLeaf],
    directory: &BTreeMap<String, DeviceRow>,
    must_be_active: &BTreeSet<String>,
    joined: &BTreeMap<String, i64>,
    epoch: i64,
) -> Result<Vec<MemberRow>, Status> {
    let mut members = Vec::with_capacity(leaves.len());
    for leaf in leaves {
        let device_id = leaf_device_id(tenant_id, leaf)?;
        let device = directory.get(&device_id).ok_or_else(|| {
            invalid_field(
                "commit",
                format!("member device '{device_id}' is not registered in the key directory"),
            )
        })?;
        if mls::device_identity(tenant_id, &device.subject_id, &device_id).as_bytes()
            != leaf.identity.as_slice()
        {
            return Err(invalid_field(
                "commit",
                format!("member '{device_id}' credential names a different principal"),
            ));
        }
        if device.signature_public_key != leaf.signature_key {
            return Err(invalid_field(
                "commit",
                format!("member '{device_id}' signature key is not its registered key"),
            ));
        }
        if must_be_active.contains(&device_id) && !device.is_active() {
            return Err(invalid_field(
                "commit",
                format!("added device '{device_id}' is revoked"),
            ));
        }
        members.push(MemberRow {
            joined_epoch: joined.get(&device_id).copied().unwrap_or(epoch),
            device_id,
            subject_id: device.subject_id.clone(),
            leaf_index: leaf.leaf_index as i32,
        });
    }
    Ok(members)
}

fn leaf_device_id(tenant_id: &str, leaf: &mls::MemberLeaf) -> Result<String, Status> {
    match mls::split_identity(&leaf.identity) {
        Some((tenant, _subject, device)) if tenant == tenant_id => Ok(device),
        Some(_) => Err(invalid_field(
            "commit",
            "a member credential names a different tenant",
        )),
        None => Err(invalid_field(
            "commit",
            "a member credential is not <tenant>/<subject>/<device>",
        )),
    }
}

pub(crate) async fn create_group(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::CreateGroupRequest>,
) -> Result<Response<msg_pb::CreateGroupResponse>, Status> {
    const OP: &str = "create_group";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Admin).await?;
    let group_id = validate_group_id(&req.group_id)?;
    let creator = validate_device_id("creator_device_id", &req.creator_device_id)?;
    require_mls_bytes("group_info", &req.group_info)?;
    require_mls_bytes("ratchet_tree", &req.ratchet_tree)?;
    let state = mls::create_public_group(&group_id, &req.group_info, &req.ratchet_tree)
        .map_err(mls_status)?;
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let device =
        require_own_active_device(&mut tx, &tenant_id, &creator, caller.as_deref(), OP).await?;
    let [leaf] = state.members.as_slice() else {
        return Err(invalid_field(
            "ratchet_tree",
            "a new group must hold exactly one member, its creator; add members with SubmitCommit",
        ));
    };
    let directory = BTreeMap::from([(creator.clone(), device)]);
    let roster = roster_from_tree(
        &tenant_id,
        std::slice::from_ref(leaf),
        &directory,
        &BTreeSet::from([creator.clone()]),
        &BTreeMap::new(),
        state.epoch as i64,
    )?;
    if roster[0].device_id != creator {
        return Err(invalid_field(
            "ratchet_tree",
            "the only member must be the creator device",
        ));
    }
    let epoch = state.epoch as i64;
    let created = store::insert_group(
        &mut tx,
        &tenant_id,
        &group_id,
        epoch,
        &state.state,
        &req.group_info,
        &creator,
    )
    .await?;
    if created {
        store::replace_members(&mut tx, &tenant_id, &group_id, &roster).await?;
    } else {
        let existing = store::load_group(&mut tx, &tenant_id, &group_id, RowLock::None)
            .await?
            .ok_or_else(|| {
                messaging_internal_status(OP, "group conflicted but could not be read")
            })?;
        // Idempotent replay: the same creator registering the same untouched group.
        if existing.created_by_device_id != creator
            || existing.epoch != epoch
            || existing.public_group_state != state.state
        {
            return Err(messaging_already_exists_status(
                OP,
                "messaging_group_exists",
                format!("group '{group_id}' already exists"),
            ));
        }
    }
    commit(tx).await?;
    Ok(Response::new(msg_pb::CreateGroupResponse {
        group_id,
        epoch,
        created,
        message: if created {
            "group created".to_string()
        } else {
            "group already registered".to_string()
        },
        error: None,
    }))
}

pub(crate) async fn submit_commit(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::SubmitCommitRequest>,
) -> Result<Response<msg_pb::SubmitCommitResponse>, Status> {
    const OP: &str = "submit_commit";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let group_id = validate_group_id(&req.group_id)?;
    let sender = validate_device_id("sender_device_id", &req.sender_device_id)?;
    let client_message_id = validate_client_message_id(&req.client_message_id)?;
    require_mls_bytes("commit", &req.commit)?;
    if !req.welcome.is_empty() {
        require_mls_bytes("welcome", &req.welcome)?;
        mls::check_welcome(&req.welcome).map_err(mls_status)?;
    }
    if !req.group_info.is_empty() {
        require_mls_bytes("group_info", &req.group_info)?;
    }
    let (header_group, header_epoch) = mls::commit_epoch(&req.commit).map_err(mls_status)?;
    if header_group.as_slice() != group_id.as_bytes() {
        return Err(invalid_field("commit", "commit is for a different group"));
    }
    if req.epoch < 0 || header_epoch != req.epoch as u64 {
        return Err(invalid_field(
            "epoch",
            format!("must equal the commit's epoch ({header_epoch})"),
        ));
    }
    let caller = caller_user_subject();
    let (context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;

    if !client_message_id.is_empty() {
        store::lock_client_message_key(&mut tx, &tenant_id, &group_id, &client_message_id).await?;
        if store::find_client_message(
            &mut tx,
            &tenant_id,
            &group_id,
            &client_message_id,
            &[KIND_COMMIT, KIND_WELCOME],
        )
        .await?
        .is_some()
        {
            let group = store::load_group(&mut tx, &tenant_id, &group_id, RowLock::None)
                .await?
                .ok_or_else(|| group_not_found_status(OP, &group_id))?;
            let members = store::list_members(&mut tx, &tenant_id, &group_id).await?;
            commit(tx).await?;
            return Ok(Response::new(msg_pb::SubmitCommitResponse {
                epoch: group.epoch,
                member_device_ids: members.into_iter().map(|m| m.device_id).collect(),
                added_device_ids: Vec::new(),
                removed_device_ids: Vec::new(),
                duplicate: true,
                message: "commit already accepted".to_string(),
                error: None,
            }));
        }
    }

    // The group row lock linearizes commits: a concurrent commit at the same
    // epoch waits here and then sees the advanced epoch.
    let group = store::load_group(&mut tx, &tenant_id, &group_id, RowLock::Update)
        .await?
        .ok_or_else(|| group_not_found_status(OP, &group_id))?;
    if req.epoch != group.epoch {
        return Err(stale_epoch_status(OP, req.epoch, group.epoch));
    }
    let sender_device =
        require_own_active_device(&mut tx, &tenant_id, &sender, caller.as_deref(), OP).await?;
    let before = store::list_members(&mut tx, &tenant_id, &group_id).await?;

    let accepted =
        mls::apply_commit(&group.public_group_state, &group_id, &req.commit).map_err(mls_status)?;
    let sender_identity = mls::device_identity(&tenant_id, &sender_device.subject_id, &sender);
    if accepted.sender_identity != sender_identity.as_bytes() {
        return Err(messaging_policy_status(
            tonic::Code::PermissionDenied,
            OP,
            "messaging_commit_sender_mismatch",
            "the commit is not signed by the sender device",
        ));
    }
    if accepted.after.members.len() > MAX_GROUP_MEMBERS {
        return Err(crate::runtime::executor_utils::quota_refusal_status(
            "messaging",
            "group member limit",
            format!("a group may hold at most {MAX_GROUP_MEMBERS} members"),
        ));
    }
    let new_epoch = accepted.after.epoch as i64;

    let before_ids: BTreeSet<String> = before.iter().map(|m| m.device_id.clone()).collect();
    let mut after_ids = BTreeSet::new();
    for leaf in &accepted.after.members {
        after_ids.insert(leaf_device_id(&tenant_id, leaf)?);
    }
    let added: BTreeSet<String> = after_ids.difference(&before_ids).cloned().collect();
    let removed: Vec<String> = before_ids.difference(&after_ids).cloned().collect();
    if !added.is_empty() && req.welcome.is_empty() {
        return Err(invalid_field(
            "welcome",
            "the commit adds members; their Welcome is required",
        ));
    }
    if added.is_empty() && !req.welcome.is_empty() {
        return Err(invalid_field(
            "welcome",
            "the commit adds no members; omit the Welcome",
        ));
    }
    let lookup: Vec<String> = after_ids.union(&before_ids).cloned().collect();
    let directory = store::devices_by_id(&mut tx, &tenant_id, &lookup).await?;
    let joined: BTreeMap<String, i64> = before
        .iter()
        .map(|m| (m.device_id.clone(), m.joined_epoch))
        .collect();
    let roster = roster_from_tree(
        &tenant_id,
        &accepted.after.members,
        &directory,
        &added,
        &joined,
        new_epoch,
    )?;
    let group_info_epoch = if req.group_info.is_empty() {
        None
    } else {
        Some(
            mls::verify_group_info(&accepted.after.state, &group_id, &req.group_info)
                .map_err(mls_status)?,
        )
    };
    if group_info_epoch.is_some_and(|epoch| epoch != accepted.after.epoch) {
        return Err(invalid_field(
            "group_info",
            "the GroupInfo must describe the epoch this commit creates",
        ));
    }

    if !store::update_group_state(
        &mut tx,
        &tenant_id,
        &group_id,
        group.epoch,
        new_epoch,
        &accepted.after.state,
        (!req.group_info.is_empty()).then_some(req.group_info.as_slice()),
    )
    .await?
    {
        return Err(messaging_internal_status(
            OP,
            "group epoch moved while its row was locked",
        ));
    }
    store::replace_members(&mut tx, &tenant_id, &group_id, &roster).await?;

    // COMMIT to everyone who was or still is a member (removed devices learn they
    // were removed), except the added ones, who get the Welcome instead.
    // Revoked devices receive nothing.
    let committed_epoch = accepted.committed_epoch as i64;
    let mut entries = Vec::new();
    for device_id in before_ids.union(&after_ids) {
        if !directory.get(device_id).is_some_and(DeviceRow::is_active) {
            continue;
        }
        let (kind, epoch, payload) = if added.contains(device_id) {
            (KIND_WELCOME, new_epoch, req.welcome.as_slice())
        } else {
            (KIND_COMMIT, committed_epoch, req.commit.as_slice())
        };
        entries.push(InboxAppend {
            device_id,
            kind,
            group_id: &group_id,
            epoch,
            payload,
            sender_device_id: &sender,
            client_message_id: &client_message_id,
        });
    }
    let appended = fan_out(svc, &mut tx, &tenant_id, &context.project_id, entries).await?;
    commit(tx).await?;
    if appended > 0 {
        svc.inbox_wake.notify_waiters();
    }
    Ok(Response::new(msg_pb::SubmitCommitResponse {
        epoch: new_epoch,
        member_device_ids: after_ids.into_iter().collect(),
        added_device_ids: added.into_iter().collect(),
        removed_device_ids: removed,
        duplicate: false,
        message: "commit accepted".to_string(),
        error: None,
    }))
}

pub(crate) async fn get_group_info(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::GetGroupInfoRequest>,
) -> Result<Response<msg_pb::GetGroupInfoResponse>, Status> {
    const OP: &str = "get_group_info";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Read).await?;
    let group_id = validate_group_id(&req.group_id)?;
    let (_context, pool) = svc.store(&metadata, &tenant_id, false)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let group = store::load_group(&mut tx, &tenant_id, &group_id, RowLock::None)
        .await?
        .ok_or_else(|| group_not_found_status(OP, &group_id))?;
    let members = store::list_members(&mut tx, &tenant_id, &group_id).await?;
    commit(tx).await?;
    let ratchet_tree =
        mls::export_ratchet_tree(&group.public_group_state, &group_id).map_err(|reject| {
            messaging_internal_status(OP, format!("stored group state unreadable: {reject}"))
        })?;
    Ok(Response::new(msg_pb::GetGroupInfoResponse {
        group_id,
        epoch: group.epoch,
        ratchet_tree,
        group_info: group.group_info.unwrap_or_default(),
        group_info_epoch: group.group_info_epoch,
        member_device_ids: members.into_iter().map(|m| m.device_id).collect(),
        message: String::new(),
        error: None,
    }))
}

// ── messages and inbox ─────────────────────────────────────────────────────

pub(crate) async fn send_message(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::SendMessageRequest>,
) -> Result<Response<msg_pb::SendMessageResponse>, Status> {
    const OP: &str = "send_message";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let group_id = validate_group_id(&req.group_id)?;
    let sender = validate_device_id("sender_device_id", &req.sender_device_id)?;
    let client_message_id = validate_client_message_id(&req.client_message_id)?;
    require_mls_bytes("ciphertext", &req.ciphertext)?;
    let message_epoch =
        mls::application_message_epoch(&group_id, &req.ciphertext).map_err(mls_status)? as i64;
    let caller = caller_user_subject();
    let (context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;

    if !client_message_id.is_empty() {
        store::lock_client_message_key(&mut tx, &tenant_id, &group_id, &client_message_id).await?;
        if let Some((epoch, rows)) = store::find_client_message(
            &mut tx,
            &tenant_id,
            &group_id,
            &client_message_id,
            &[KIND_MESSAGE],
        )
        .await?
        {
            commit(tx).await?;
            return Ok(Response::new(msg_pb::SendMessageResponse {
                epoch,
                fanout: rows as i32,
                duplicate: true,
                message: "message already delivered".to_string(),
                error: None,
            }));
        }
    }

    // FOR SHARE: concurrent sends proceed together, but a commit retiring this
    // epoch waits until they finish (and a send waits for a running commit).
    let group = store::load_group(&mut tx, &tenant_id, &group_id, RowLock::Share)
        .await?
        .ok_or_else(|| group_not_found_status(OP, &group_id))?;
    if message_epoch != group.epoch {
        return Err(stale_epoch_status(OP, message_epoch, group.epoch));
    }
    require_own_active_device(&mut tx, &tenant_id, &sender, caller.as_deref(), OP).await?;
    let members = store::list_members(&mut tx, &tenant_id, &group_id).await?;
    if !members.iter().any(|member| member.device_id == sender) {
        return Err(not_a_member_status(OP, &sender));
    }
    let member_ids: Vec<String> = members.iter().map(|m| m.device_id.clone()).collect();
    let directory = store::devices_by_id(&mut tx, &tenant_id, &member_ids).await?;
    // Every ACTIVE member device, the sender's other devices and the sender
    // itself included (clients skip entries whose sender_device_id is their own).
    let entries: Vec<InboxAppend<'_>> = member_ids
        .iter()
        .filter(|device_id| directory.get(*device_id).is_some_and(DeviceRow::is_active))
        .map(|device_id| InboxAppend {
            device_id,
            kind: KIND_MESSAGE,
            group_id: &group_id,
            epoch: message_epoch,
            payload: &req.ciphertext,
            sender_device_id: &sender,
            client_message_id: &client_message_id,
        })
        .collect();
    let fanout = fan_out(svc, &mut tx, &tenant_id, &context.project_id, entries).await?;
    commit(tx).await?;
    if fanout > 0 {
        svc.inbox_wake.notify_waiters();
    }
    Ok(Response::new(msg_pb::SendMessageResponse {
        epoch: message_epoch,
        fanout,
        duplicate: false,
        message: "message delivered".to_string(),
        error: None,
    }))
}

pub(crate) async fn stream_inbox(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::StreamInboxRequest>,
) -> Result<Response<super::InboxStream>, Status> {
    const OP: &str = "stream_inbox";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Read).await?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    if req.after_seq < 0 || req.max_entries < 0 {
        return Err(invalid_field(
            "after_seq",
            "after_seq and max_entries must not be negative",
        ));
    }
    // Authorization happens HERE, inside the request's claim scope; the stream
    // body below runs after the handler returns.
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, false)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let device = store::load_device(&mut tx, &tenant_id, &device_id)
        .await?
        .ok_or_else(|| device_not_found_status(OP, &device_id))?;
    ensure_device_owner(caller.as_deref(), &device, OP)?;
    commit(tx).await?;

    let mut cursor = if req.after_seq > 0 {
        req.after_seq
    } else {
        device.acked_seq
    };
    let follow = req.follow;
    let limit = (req.max_entries > 0).then_some(req.max_entries as i64);
    let wake = svc.inbox_wake.clone();
    let stream = async_stream::try_stream! {
        let mut delivered = 0i64;
        loop {
            // Register for the wake BEFORE reading, so an append that lands
            // between the read and the wait is never missed.
            let notified = wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let page = match limit {
                Some(limit) => INBOX_PAGE.min(limit - delivered),
                None => INBOX_PAGE,
            };
            let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
            let rows = store::read_inbox(&mut tx, &tenant_id, &device_id, cursor, page).await?;
            commit(tx).await?;
            let full_page = rows.len() as i64 == page;
            for row in rows {
                cursor = row.seq;
                delivered += 1;
                yield msg_pb::StreamInboxResponse {
                    entry: Some(inbox_dto(row)),
                    error: None,
                };
            }
            if limit.is_some_and(|limit| delivered >= limit) {
                break;
            }
            if full_page {
                continue;
            }
            if !follow {
                break;
            }
            let _ = tokio::time::timeout(INBOX_POLL_INTERVAL, notified).await;
        }
    };
    Ok(Response::new(Box::pin(stream)))
}

pub(crate) async fn ack_inbox(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::AckInboxRequest>,
) -> Result<Response<msg_pb::AckInboxResponse>, Status> {
    const OP: &str = "ack_inbox";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let device_id = validate_device_id("device_id", &req.device_id)?;
    if req.up_to_seq < 0 {
        return Err(invalid_field("up_to_seq", "must not be negative"));
    }
    let caller = caller_user_subject();
    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let device = store::load_device(&mut tx, &tenant_id, &device_id)
        .await?
        .ok_or_else(|| device_not_found_status(OP, &device_id))?;
    ensure_device_owner(caller.as_deref(), &device, OP)?;
    let (acked_seq, acknowledged) =
        store::ack_inbox(&mut tx, &tenant_id, &device_id, req.up_to_seq).await?;
    commit(tx).await?;
    Ok(Response::new(msg_pb::AckInboxResponse {
        acked_seq,
        acknowledged,
        message: String::new(),
        error: None,
    }))
}
