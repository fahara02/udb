//! Request validation, caller-identity binding and row → DTO mapping for the
//! native `MessagingService`. Pure, so it is unit-tested without a store.

use sha2::Digest as _;
use tonic::Status;

use crate::proto::udb::core::messaging::services::v1 as msg_pb;

use super::config::{
    BACKUP_ENVELOPE_MAGIC, DEFAULT_BACKUP_ID, MAX_BACKUP_BYTES, MAX_BACKUP_ID_LEN,
    MAX_CLIENT_MESSAGE_ID_LEN, MAX_DEVICE_ID_LEN, MAX_GROUP_ID_LEN, MAX_MLS_MESSAGE_BYTES,
    MAX_SUBJECT_ID_LEN, MIN_BACKUP_BYTES,
};
use super::errors::{device_owner_mismatch_status, invalid_field};
use super::store::{BackupRow, DeviceRow, InboxRow};

fn token_char(c: char, extra: &[char]) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') || extra.contains(&c)
}

fn token(field: &str, value: &str, max: usize, extra: &[char]) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_field(field, "is required"));
    }
    if value.len() > max {
        return Err(invalid_field(
            field,
            format!("is longer than {max} characters"),
        ));
    }
    if !value.chars().all(|c| token_char(c, extra)) {
        return Err(invalid_field(
            field,
            "may contain only ASCII letters, digits and . _ - characters",
        ));
    }
    Ok(value.to_string())
}

/// A device id: 1-128 of `[A-Za-z0-9._-]`. Never contains `/`, so the MLS
/// identity `<tenant>/<subject>/<device>` splits unambiguously.
pub(crate) fn validate_device_id(field: &str, value: &str) -> Result<String, Status> {
    token(field, value, MAX_DEVICE_ID_LEN, &[])
}

/// A group id: 1-255 of `[A-Za-z0-9._:-]`; its UTF-8 bytes are the MLS group id.
pub(crate) fn validate_group_id(value: &str) -> Result<String, Status> {
    token("group_id", value, MAX_GROUP_ID_LEN, &[':'])
}

/// An optional client idempotency key (empty = none).
pub(crate) fn validate_client_message_id(value: &str) -> Result<String, Status> {
    if value.trim().is_empty() {
        return Ok(String::new());
    }
    token(
        "client_message_id",
        value,
        MAX_CLIENT_MESSAGE_ID_LEN,
        &[':'],
    )
}

/// A backup id: 1-128 of `[A-Za-z0-9._-]`; empty means [`DEFAULT_BACKUP_ID`].
pub(crate) fn validate_backup_id(value: &str) -> Result<String, Status> {
    if value.trim().is_empty() {
        return Ok(DEFAULT_BACKUP_ID.to_string());
    }
    token("backup_id", value, MAX_BACKUP_ID_LEN, &[])
}

/// A sealed backup envelope: tagged with the udb-mls envelope magic and within
/// the size bounds. The broker cannot verify the encryption itself.
pub(crate) fn require_backup_envelope(bytes: &[u8]) -> Result<(), Status> {
    if bytes.len() < MIN_BACKUP_BYTES || !bytes.starts_with(BACKUP_ENVELOPE_MAGIC) {
        return Err(invalid_field(
            "ciphertext",
            "must be a sealed backup envelope (udb-mls seal_backup); the broker stores only encrypted backups",
        ));
    }
    if bytes.len() > MAX_BACKUP_BYTES {
        return Err(invalid_field(
            "ciphertext",
            format!("is larger than {MAX_BACKUP_BYTES} bytes"),
        ));
    }
    Ok(())
}

/// A subject id: non-empty, at most 255 characters, no control characters.
pub(crate) fn validate_subject_id(field: &str, value: &str) -> Result<String, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_field(field, "is required"));
    }
    if value.len() > MAX_SUBJECT_ID_LEN {
        return Err(invalid_field(
            field,
            format!("is longer than {MAX_SUBJECT_ID_LEN} characters"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid_field(field, "must not contain control characters"));
    }
    Ok(value.to_string())
}

/// Reject an empty or oversized MLS blob.
pub(crate) fn require_mls_bytes(field: &str, bytes: &[u8]) -> Result<(), Status> {
    if bytes.is_empty() {
        return Err(invalid_field(field, "is required"));
    }
    if bytes.len() > MAX_MLS_MESSAGE_BYTES {
        return Err(invalid_field(
            field,
            format!("is larger than {MAX_MLS_MESSAGE_BYTES} bytes"),
        ));
    }
    Ok(())
}

/// Who may join a group by an external commit, as the group row stores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinPolicy {
    None,
    SamePrincipal,
    Tenant,
}

impl JoinPolicy {
    /// The request enum; UNSPECIFIED is NONE, an unknown value is refused.
    pub(crate) fn from_proto(value: i32) -> Result<Self, Status> {
        use msg_pb::ExternalJoinPolicy as P;
        match P::try_from(value) {
            Ok(P::Unspecified | P::None) => Ok(Self::None),
            Ok(P::SamePrincipal) => Ok(Self::SamePrincipal),
            Ok(P::Tenant) => Ok(Self::Tenant),
            Err(_) => Err(invalid_field(
                "external_join_policy",
                "is not a known ExternalJoinPolicy",
            )),
        }
    }

    pub(crate) fn to_proto(self) -> msg_pb::ExternalJoinPolicy {
        match self {
            Self::None => msg_pb::ExternalJoinPolicy::None,
            Self::SamePrincipal => msg_pb::ExternalJoinPolicy::SamePrincipal,
            Self::Tenant => msg_pb::ExternalJoinPolicy::Tenant,
        }
    }

    /// The `external_join_policy` column value.
    pub(crate) fn as_text(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::SamePrincipal => "SAME_PRINCIPAL",
            Self::Tenant => "TENANT",
        }
    }

    /// The column value; anything unrecognised reads as NONE (fail closed).
    pub(crate) fn from_text(value: &str) -> Self {
        match value.trim() {
            "SAME_PRINCIPAL" => Self::SamePrincipal,
            "TENANT" => Self::Tenant,
            _ => Self::None,
        }
    }
}

/// Lowercase hex SHA-256 of a signature public key: the safety-number input
/// peers compare out of band.
pub(crate) fn key_fingerprint(signature_public_key: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(signature_public_key))
}

/// The END-USER subject of the verified claim, when the request carries one.
/// Service accounts (a non-empty service identity) act for users and are not
/// pinned to one subject; in-process callers carry no claim at all.
pub(crate) fn caller_user_subject() -> Option<String> {
    use crate::runtime::service::method_security::{claim_context_present, current_claim_context};
    if !claim_context_present() {
        return None;
    }
    let claim = current_claim_context();
    if !claim.authenticated || !claim.service_identity.trim().is_empty() {
        return None;
    }
    let subject = claim.subject.trim();
    (!subject.is_empty()).then(|| subject.to_string())
}

/// Resolve the subject a request acts for. An end user may act only for itself
/// (an empty body subject means "me"); a service account or in-process caller
/// must name the subject.
pub(crate) fn resolve_subject(
    caller: Option<&str>,
    field: &str,
    body_subject: &str,
    operation: &'static str,
) -> Result<String, Status> {
    match caller {
        Some(caller) if body_subject.trim().is_empty() => Ok(caller.to_string()),
        Some(caller) => {
            let body = validate_subject_id(field, body_subject)?;
            if body != caller {
                return Err(device_owner_mismatch_status(operation));
            }
            Ok(body)
        }
        None => validate_subject_id(field, body_subject),
    }
}

/// An end user may act only through its own devices.
pub(crate) fn ensure_device_owner(
    caller: Option<&str>,
    device: &DeviceRow,
    operation: &'static str,
) -> Result<(), Status> {
    match caller {
        Some(caller) if caller != device.subject_id => Err(device_owner_mismatch_status(operation)),
        _ => Ok(()),
    }
}

pub(crate) fn device_dto(tenant_id: &str, row: &DeviceRow) -> msg_pb::DirectoryDevice {
    msg_pb::DirectoryDevice {
        tenant_id: tenant_id.to_string(),
        subject_id: row.subject_id.clone(),
        device_id: row.device_id.clone(),
        signature_public_key: row.signature_public_key.clone(),
        credential: row.credential.clone(),
        key_fingerprint: row.key_fingerprint.clone(),
        status: row.status.clone(),
        registered_at_unix: row.registered_at_unix,
        revoked_at_unix: row.revoked_at_unix,
    }
}

pub(crate) fn inbox_dto(row: InboxRow) -> msg_pb::InboxEntry {
    msg_pb::InboxEntry {
        device_id: row.device_id,
        seq: row.seq,
        kind: row.kind,
        group_id: row.group_id,
        epoch: row.epoch,
        payload: row.payload,
        sender_device_id: row.sender_device_id,
        client_message_id: row.client_message_id,
        created_at_unix: row.created_at_unix,
    }
}

/// The JSON removal hint delivered to a revoked device's group peers.
pub(crate) fn removal_hint_payload(
    group_id: &str,
    device_id: &str,
    leaf_index: i32,
    reason: &str,
) -> Vec<u8> {
    serde_json::json!({
        "type": "device_revoked",
        "group_id": group_id,
        "device_id": device_id,
        "leaf_index": leaf_index,
        "reason": reason,
    })
    .to_string()
    .into_bytes()
}

pub(crate) fn backup_dto(tenant_id: &str, row: &BackupRow) -> msg_pb::MessagingBackupInfo {
    msg_pb::MessagingBackupInfo {
        tenant_id: tenant_id.to_string(),
        subject_id: row.subject_id.clone(),
        backup_id: row.backup_id.clone(),
        size_bytes: row.size_bytes,
        ciphertext_sha256: row.ciphertext_sha256.clone(),
        version: row.version,
        created_at_unix: row.created_at_unix,
        updated_at_unix: row.updated_at_unix,
    }
}

/// Lowercase hex SHA-256 of a backup envelope.
pub(crate) fn envelope_digest(bytes: &[u8]) -> String {
    format!("{:x}", sha2::Sha256::digest(bytes))
}
