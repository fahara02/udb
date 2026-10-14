//! Opt-in encrypted backup of a device's state and history.
//!
//! History is device-only by default. A user who opts in keeps a backup on the
//! broker that only they can open: the device seals its state snapshot (and any
//! history the application holds) with a key derived from a user recovery
//! secret, and the broker stores the opaque envelope (`PutMessagingBackup`). On a
//! new device the same secret opens it again ([`open_backup`],
//! [`crate::MlsClient::restore_backup`]). The broker never sees the secret, the
//! key, or the plaintext.
//!
//! Key schedule: Argon2id (the cost parameters travel in the envelope) stretches
//! the recovery secret with a random 16-byte salt into 32 bytes of input key
//! material; HKDF-SHA256 (salt = the same salt, info = a fixed label) expands it
//! into the XChaCha20-Poly1305 key. Every envelope has a fresh salt and a fresh
//! random 24-byte nonce. The whole header is authenticated as associated data,
//! so a changed cost parameter, salt or nonce fails decryption like a changed
//! ciphertext byte does.
//!
//! Envelope (big-endian):
//!
//! | bytes | field |
//! |---|---|
//! | 8 | magic `UDBMLSB1` |
//! | 4 / 4 / 4 | Argon2id memory (KiB), iterations, parallelism |
//! | 16 | salt |
//! | 24 | nonce |
//! | rest | XChaCha20-Poly1305 ciphertext of the payload, then the 16-byte tag |
//!
//! The payload is `UDBMLSP1`, then the snapshot and the history, each as a
//! 4-byte length and the bytes.
//!
//! A restored device IS the backed-up device (same identity and keys). It
//! decrypts what its inbox still holds after the backup's cursor and every new
//! message. Back up after processing the inbox: entries the old device
//! acknowledged after the backup was taken are collected by the broker, and a
//! group whose commits were missed must re-add the device.

use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use openmls::prelude::{OpenMlsProvider as _, OpenMlsRand as _};
use openmls_rust_crypto::OpenMlsRustCrypto;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::error::MlsError;

/// The first eight bytes of every sealed backup envelope.
pub const BACKUP_MAGIC: &[u8; 8] = b"UDBMLSB1";
const PAYLOAD_MAGIC: &[u8; 8] = b"UDBMLSP1";
const SALT_LEN: usize = 16;
const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
const HEADER_LEN: usize = BACKUP_MAGIC.len() + 12 + SALT_LEN + NONCE_LEN;
const KEY_LEN: usize = 32;
const HKDF_INFO: &[u8] = b"udb-mls backup v1: xchacha20poly1305 key";

/// Shortest recovery secret accepted.
pub const MIN_RECOVERY_SECRET_LEN: usize = 8;
/// Smallest envelope: the header plus the tag of an empty payload.
pub const MIN_BACKUP_LEN: usize = HEADER_LEN + TAG_LEN;

/// Argon2id cost parameters of a backup key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupParams {
    /// Memory in KiB.
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

impl BackupParams {
    /// The OWASP Argon2id baseline: 19 MiB, 2 iterations, 1 lane.
    pub const RECOMMENDED: Self = Self {
        memory_kib: 19 * 1024,
        iterations: 2,
        parallelism: 1,
    };
    /// The highest costs [`open_backup`] accepts from an envelope, so a crafted
    /// envelope cannot make a device allocate gigabytes or spin for minutes.
    pub const MAX_MEMORY_KIB: u32 = 1024 * 1024;
    pub const MAX_ITERATIONS: u32 = 16;
    pub const MAX_PARALLELISM: u32 = 16;

    fn argon2(self) -> Result<Argon2<'static>, MlsError> {
        if self.memory_kib > Self::MAX_MEMORY_KIB
            || self.iterations > Self::MAX_ITERATIONS
            || self.parallelism > Self::MAX_PARALLELISM
        {
            return Err(MlsError::Backup(format!(
                "Argon2id cost {}KiB/{}/{} exceeds the accepted maximum",
                self.memory_kib, self.iterations, self.parallelism
            )));
        }
        let params = Params::new(
            self.memory_kib,
            self.iterations,
            self.parallelism,
            Some(KEY_LEN),
        )
        .map_err(|err| MlsError::Backup(format!("Argon2id parameters: {err}")))?;
        Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
    }
}

impl Default for BackupParams {
    fn default() -> Self {
        Self::RECOMMENDED
    }
}

/// What a backup holds: the device's state snapshot (SECRET: private keys and
/// group secrets) and the history the application chose to include.
#[derive(Clone, PartialEq, Eq)]
pub struct BackupContents {
    pub snapshot: Vec<u8>,
    pub history: Vec<u8>,
}

impl std::fmt::Debug for BackupContents {
    // Never prints the secret snapshot or the plaintext history.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupContents")
            .field("snapshot_len", &self.snapshot.len())
            .field("history_len", &self.history.len())
            .finish()
    }
}

impl Drop for BackupContents {
    fn drop(&mut self) {
        use zeroize::Zeroize as _;
        self.snapshot.zeroize();
        self.history.zeroize();
    }
}

fn check_secret(recovery_secret: &[u8]) -> Result<(), MlsError> {
    if recovery_secret.len() < MIN_RECOVERY_SECRET_LEN {
        return Err(MlsError::Backup(format!(
            "the recovery secret must be at least {MIN_RECOVERY_SECRET_LEN} bytes"
        )));
    }
    Ok(())
}

fn derive_key(
    recovery_secret: &[u8],
    salt: &[u8],
    params: BackupParams,
) -> Result<Zeroizing<[u8; KEY_LEN]>, MlsError> {
    let mut stretched = Zeroizing::new([0u8; KEY_LEN]);
    params
        .argon2()?
        .hash_password_into(recovery_secret, salt, &mut stretched[..])
        .map_err(|err| MlsError::Backup(format!("Argon2id: {err}")))?;
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    Hkdf::<Sha256>::new(Some(salt), &stretched[..])
        .expand(HKDF_INFO, &mut key[..])
        .map_err(|err| MlsError::Backup(format!("HKDF: {err}")))?;
    Ok(key)
}

fn random(len: usize) -> Result<Vec<u8>, MlsError> {
    OpenMlsRustCrypto::default()
        .rand()
        .random_vec(len)
        .map_err(|err| MlsError::Backup(format!("randomness: {err:?}")))
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) -> Result<(), MlsError> {
    let len = u32::try_from(chunk.len())
        .map_err(|_| MlsError::Backup("a backup part is larger than 4 GiB".into()))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(chunk);
    Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    if bytes.len() < len {
        return None;
    }
    let (head, rest) = bytes.split_at(len);
    *bytes = rest;
    Some(head)
}

fn take_u32(bytes: &mut &[u8]) -> Option<u32> {
    take(bytes, 4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

/// Seal `contents` under `recovery_secret` with the given Argon2id costs.
pub fn seal_backup(
    recovery_secret: &[u8],
    contents: &BackupContents,
    params: BackupParams,
) -> Result<Vec<u8>, MlsError> {
    check_secret(recovery_secret)?;
    let salt = random(SALT_LEN)?;
    let nonce = random(NONCE_LEN)?;
    let key = derive_key(recovery_secret, &salt, params)?;

    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(BACKUP_MAGIC);
    header.extend_from_slice(&params.memory_kib.to_be_bytes());
    header.extend_from_slice(&params.iterations.to_be_bytes());
    header.extend_from_slice(&params.parallelism.to_be_bytes());
    header.extend_from_slice(&salt);
    header.extend_from_slice(&nonce);

    let mut plaintext = Zeroizing::new(Vec::with_capacity(
        PAYLOAD_MAGIC.len() + 8 + contents.snapshot.len() + contents.history.len(),
    ));
    plaintext.extend_from_slice(PAYLOAD_MAGIC);
    push_chunk(&mut plaintext, &contents.snapshot)?;
    push_chunk(&mut plaintext, &contents.history)?;

    let sealed = XChaCha20Poly1305::new(Key::from_slice(&key[..]))
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &header,
            },
        )
        .map_err(|_| MlsError::Backup("encryption failed".into()))?;
    let mut envelope = header;
    envelope.extend_from_slice(&sealed);
    Ok(envelope)
}

/// Open an envelope [`seal_backup`] produced. A wrong recovery secret and a
/// modified envelope are indistinguishable by design: both fail
/// authentication.
pub fn open_backup(recovery_secret: &[u8], envelope: &[u8]) -> Result<BackupContents, MlsError> {
    check_secret(recovery_secret)?;
    let malformed = || MlsError::Backup("not a udb-mls backup envelope".into());
    if envelope.len() < MIN_BACKUP_LEN {
        return Err(malformed());
    }
    let (header, sealed) = envelope.split_at(HEADER_LEN);
    let mut rest = header;
    if take(&mut rest, BACKUP_MAGIC.len()) != Some(BACKUP_MAGIC.as_slice()) {
        return Err(malformed());
    }
    let params = BackupParams {
        memory_kib: take_u32(&mut rest).ok_or_else(malformed)?,
        iterations: take_u32(&mut rest).ok_or_else(malformed)?,
        parallelism: take_u32(&mut rest).ok_or_else(malformed)?,
    };
    let salt = take(&mut rest, SALT_LEN).ok_or_else(malformed)?;
    let nonce = take(&mut rest, NONCE_LEN).ok_or_else(malformed)?;
    let key = derive_key(recovery_secret, salt, params)?;
    let plaintext = Zeroizing::new(
        XChaCha20Poly1305::new(Key::from_slice(&key[..]))
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: sealed,
                    aad: header,
                },
            )
            .map_err(|_| {
                MlsError::Backup(
                    "the backup does not open: wrong recovery secret, or the backup was modified"
                        .into(),
                )
            })?,
    );
    let corrupt = || MlsError::Corrupt("backup payload framing".into());
    let mut rest: &[u8] = &plaintext;
    if take(&mut rest, PAYLOAD_MAGIC.len()) != Some(PAYLOAD_MAGIC.as_slice()) {
        return Err(corrupt());
    }
    let snapshot_len = take_u32(&mut rest).ok_or_else(corrupt)? as usize;
    let snapshot = take(&mut rest, snapshot_len).ok_or_else(corrupt)?.to_vec();
    let history_len = take_u32(&mut rest).ok_or_else(corrupt)? as usize;
    let history = take(&mut rest, history_len).ok_or_else(corrupt)?.to_vec();
    if !rest.is_empty() {
        return Err(corrupt());
    }
    Ok(BackupContents { snapshot, history })
}
