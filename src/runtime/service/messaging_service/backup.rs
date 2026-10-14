//! The opt-in encrypted history backup RPCs (`PutMessagingBackup`,
//! `GetMessagingBackup`, `DeleteMessagingBackup`).
//!
//! The client seals its export with a key derived from a recovery secret
//! (Argon2id, then HKDF-SHA256, then XChaCha20-Poly1305; `udb-mls`
//! `seal_backup`). The broker stores the opaque envelope in
//! `udb_messaging.backups` under tenant RLS and never sees the secret, the key or
//! the plaintext. A backup belongs to one principal: an end user reads and
//! writes only its own, a service account acting for users must name the
//! principal.

use tonic::{Request, Response, Status};

use crate::proto::udb::core::messaging::services::v1 as msg_pb;
use crate::runtime::channels::OperationChannel;

use super::MessagingServiceImpl;
use super::errors::{backup_not_found_status, backup_version_conflict_status, invalid_field};
use super::handlers::admit;
use super::model::{
    backup_dto, caller_user_subject, envelope_digest, require_backup_envelope, resolve_subject,
    validate_backup_id,
};
use super::store::{self, BackupWrite, begin_tenant_tx, commit};

pub(crate) async fn put_backup(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::PutMessagingBackupRequest>,
) -> Result<Response<msg_pb::PutMessagingBackupResponse>, Status> {
    const OP: &str = "put_messaging_backup";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Write).await?;
    let caller = caller_user_subject();
    let subject_id = resolve_subject(caller.as_deref(), "subject_id", &req.subject_id, OP)?;
    let backup_id = validate_backup_id(&req.backup_id)?;
    require_backup_envelope(&req.ciphertext)?;
    if req.expected_version < 0 {
        return Err(invalid_field("expected_version", "must not be negative"));
    }
    let digest = envelope_digest(&req.ciphertext);
    let write = BackupWrite {
        tenant_id: &tenant_id,
        subject_id: &subject_id,
        backup_id: &backup_id,
        ciphertext: &req.ciphertext,
        ciphertext_sha256: &digest,
    };

    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let (row, created) = if req.expected_version == 0 {
        store::upsert_backup(&mut tx, write).await?
    } else {
        match store::replace_backup_at_version(&mut tx, write, req.expected_version).await? {
            Some(row) => (row, false),
            None => {
                let current =
                    store::backup_version(&mut tx, &tenant_id, &subject_id, &backup_id).await?;
                return Err(backup_version_conflict_status(
                    OP,
                    req.expected_version,
                    current,
                ));
            }
        }
    };
    commit(tx).await?;
    Ok(Response::new(msg_pb::PutMessagingBackupResponse {
        backup: Some(backup_dto(&tenant_id, &row)),
        created,
        message: if created {
            "backup stored".to_string()
        } else {
            "backup replaced".to_string()
        },
        error: None,
    }))
}

pub(crate) async fn get_backup(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::GetMessagingBackupRequest>,
) -> Result<Response<msg_pb::GetMessagingBackupResponse>, Status> {
    const OP: &str = "get_messaging_backup";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Read).await?;
    let caller = caller_user_subject();
    let subject_id = resolve_subject(caller.as_deref(), "subject_id", &req.subject_id, OP)?;
    let backup_id = validate_backup_id(&req.backup_id)?;
    let (_context, pool) = svc.store(&metadata, &tenant_id, false)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let stored = store::load_backup(&mut tx, &tenant_id, &subject_id, &backup_id).await?;
    commit(tx).await?;
    let (row, ciphertext) = stored.ok_or_else(|| backup_not_found_status(OP, &backup_id))?;
    Ok(Response::new(msg_pb::GetMessagingBackupResponse {
        backup: Some(backup_dto(&tenant_id, &row)),
        ciphertext,
        message: String::new(),
        error: None,
    }))
}

pub(crate) async fn delete_backup(
    svc: &MessagingServiceImpl,
    request: Request<msg_pb::DeleteMessagingBackupRequest>,
) -> Result<Response<msg_pb::DeleteMessagingBackupResponse>, Status> {
    const OP: &str = "delete_messaging_backup";
    let metadata = request.metadata().clone();
    let req = request.into_inner();
    let (tenant_id, _permit) =
        admit(svc, &metadata, &req.tenant_id, OperationChannel::Admin).await?;
    let caller = caller_user_subject();
    let subject_id = resolve_subject(caller.as_deref(), "subject_id", &req.subject_id, OP)?;
    let backup_id = validate_backup_id(&req.backup_id)?;
    let (_context, pool) = svc.store(&metadata, &tenant_id, true)?;
    let mut tx = begin_tenant_tx(&pool, &tenant_id).await?;
    let deleted = store::delete_backup(&mut tx, &tenant_id, &subject_id, &backup_id).await?;
    commit(tx).await?;
    Ok(Response::new(msg_pb::DeleteMessagingBackupResponse {
        deleted,
        message: if deleted {
            "backup deleted".to_string()
        } else {
            "no backup was stored".to_string()
        },
        error: None,
    }))
}
