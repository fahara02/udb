//! Device identity: the ciphersuite, the credential naming scheme the broker
//! enforces, and the key fingerprint peers compare out of band.

use openmls::prelude::Ciphersuite;
use sha2::{Digest as _, Sha256};

use crate::error::MlsError;

/// RFC 9420 mandatory-to-implement ciphersuite; the only one the broker accepts.
pub const CIPHERSUITE: Ciphersuite = Ciphersuite::MLS_128_DHKEMX25519_AES128GCM_SHA256_Ed25519;

/// The MLS identity a device credential carries: `<tenant>/<subject>/<device>`.
pub fn device_identity(tenant_id: &str, subject_id: &str, device_id: &str) -> String {
    format!("{tenant_id}/{subject_id}/{device_id}")
}

/// Split a credential identity into `(tenant, subject, device)`. The tenant ends
/// at the first `/` and the device starts after the last one, so a subject may
/// itself contain `/` (the broker splits the same way). `None` for anything else.
pub fn split_identity(identity: &[u8]) -> Option<(String, String, String)> {
    let text = std::str::from_utf8(identity).ok()?;
    let (tenant, rest) = text.split_once('/')?;
    let (subject, device) = rest.rsplit_once('/')?;
    if tenant.is_empty() || subject.is_empty() || device.is_empty() {
        return None;
    }
    Some((tenant.to_string(), subject.to_string(), device.to_string()))
}

/// Lowercase hex SHA-256 of an Ed25519 signature public key: the
/// `key_fingerprint` the broker's device directory publishes, for safety-number
/// comparison.
pub fn key_fingerprint(signature_public_key: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(signature_public_key);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Who this device is. Validated the way the broker validates registrations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeviceIdentity {
    pub tenant_id: String,
    pub subject_id: String,
    pub device_id: String,
}

impl DeviceIdentity {
    /// A device of `subject_id` in `tenant_id`. The device id is 1-128
    /// characters from `[A-Za-z0-9._-]`; the tenant must not contain `/`.
    pub fn new(
        tenant_id: impl Into<String>,
        subject_id: impl Into<String>,
        device_id: impl Into<String>,
    ) -> Result<Self, MlsError> {
        let identity = Self {
            tenant_id: tenant_id.into().trim().to_string(),
            subject_id: subject_id.into().trim().to_string(),
            device_id: device_id.into().trim().to_string(),
        };
        if identity.tenant_id.is_empty() || identity.tenant_id.contains('/') {
            return Err(MlsError::InvalidIdentifier(
                "tenant_id must be non-empty and contain no '/'".into(),
            ));
        }
        if identity.subject_id.is_empty() {
            return Err(MlsError::InvalidIdentifier(
                "subject_id must be non-empty".into(),
            ));
        }
        validate_device_id(&identity.device_id)?;
        Ok(identity)
    }

    /// The credential identity string, `<tenant>/<subject>/<device>`.
    pub fn credential_identity(&self) -> String {
        device_identity(&self.tenant_id, &self.subject_id, &self.device_id)
    }
}

/// The broker's identifier token rule: 1..=`max` characters from
/// `[A-Za-z0-9._-]` plus `extra`.
fn is_token(value: &str, max: usize, extra: &[u8]) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value.bytes().all(|b| {
            b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-') || extra.contains(&b)
        })
}

pub(crate) fn validate_device_id(device_id: &str) -> Result<(), MlsError> {
    if is_token(device_id, 128, &[]) {
        Ok(())
    } else {
        Err(MlsError::InvalidIdentifier(format!(
            "device id `{device_id}` must be 1-128 characters from [A-Za-z0-9._-]"
        )))
    }
}

pub(crate) fn validate_group_id(group_id: &str) -> Result<(), MlsError> {
    if is_token(group_id, 255, b":") {
        Ok(())
    } else {
        Err(MlsError::InvalidIdentifier(format!(
            "group id `{group_id}` must be 1-255 characters from [A-Za-z0-9._:-]"
        )))
    }
}
