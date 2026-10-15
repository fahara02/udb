//! Pluggable persistence for client state.
//!
//! The client keeps its OpenMLS key/value storage in memory and writes one
//! self-contained snapshot through a [`StateStore`] after every mutation: the
//! device identity and inbox cursor, the per-group commit bookkeeping, and every
//! OpenMLS entry (signature key, KeyPackage private keys, group secrets). A store
//! only ever sees that opaque snapshot, so implementing one for a platform
//! keychain, IndexedDB, or an encrypted database is two methods.
//!
//! The snapshot holds SECRET KEY MATERIAL. Store it where the device's other
//! secrets live; never upload it to the broker (use the opt-in, client-encrypted
//! backup for history instead).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::MlsError;

/// Where an [`crate::MlsClient`] persists its state.
pub trait StateStore {
    /// The last saved snapshot, or `None` for a store that was never written.
    fn load(&self) -> Result<Option<Vec<u8>>, MlsError>;
    /// Replace the stored snapshot. Must be atomic: after a crash the store
    /// holds either the previous snapshot or this one, never a mix.
    fn save(&self, snapshot: &[u8]) -> Result<(), MlsError>;
}

impl<T: StateStore + ?Sized> StateStore for Box<T> {
    fn load(&self) -> Result<Option<Vec<u8>>, MlsError> {
        (**self).load()
    }
    fn save(&self, snapshot: &[u8]) -> Result<(), MlsError> {
        (**self).save(snapshot)
    }
}

impl<T: StateStore + ?Sized> StateStore for std::sync::Arc<T> {
    fn load(&self) -> Result<Option<Vec<u8>>, MlsError> {
        (**self).load()
    }
    fn save(&self, snapshot: &[u8]) -> Result<(), MlsError> {
        (**self).save(snapshot)
    }
}

/// A process-lifetime store. State is lost when the process exits unless the
/// caller exports [`MemoryStore::snapshot`] somewhere durable (the WASM binding
/// hands it to JavaScript this way).
#[derive(Debug, Default)]
pub struct MemoryStore {
    snapshot: Mutex<Option<Vec<u8>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// A store that starts from a previously exported snapshot.
    pub fn from_snapshot(snapshot: Vec<u8>) -> Self {
        Self {
            snapshot: Mutex::new(Some(snapshot)),
        }
    }

    /// The last saved snapshot.
    pub fn snapshot(&self) -> Option<Vec<u8>> {
        self.snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl StateStore for MemoryStore {
    fn load(&self) -> Result<Option<Vec<u8>>, MlsError> {
        Ok(self.snapshot())
    }

    fn save(&self, snapshot: &[u8]) -> Result<(), MlsError> {
        *self
            .snapshot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(snapshot.to_vec());
        Ok(())
    }
}

/// A single file, replaced atomically (write a sibling temp file, flush it to
/// disk, rename over the target). On Unix the file is created owner-only
/// (`0600`) because it holds private keys.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct FileStore {
    path: std::path::PathBuf,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileStore {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl StateStore for FileStore {
    fn load(&self) -> Result<Option<Vec<u8>>, MlsError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(MlsError::Store(format!(
                "read {}: {err}",
                self.path.display()
            ))),
        }
    }

    fn save(&self, snapshot: &[u8]) -> Result<(), MlsError> {
        use std::io::Write as _;
        let fail = |what: &str, err: std::io::Error| {
            MlsError::Store(format!("{what} {}: {err}", self.path.display()))
        };
        let mut temp = self.path.clone().into_os_string();
        temp.push(".tmp");
        let temp = std::path::PathBuf::from(temp);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temp).map_err(|err| fail("create", err))?;
        file.write_all(snapshot).map_err(|err| fail("write", err))?;
        file.sync_all().map_err(|err| fail("sync", err))?;
        drop(file);
        std::fs::rename(&temp, &self.path).map_err(|err| fail("replace", err))?;
        Ok(())
    }
}

// ── snapshot codec ─────────────────────────────────────────────────────────

const SNAPSHOT_MAGIC: &[u8; 8] = b"UDBMLSC1";

/// Encode the client metadata (JSON) and the OpenMLS key/value entries, sorted
/// so the same state always produces the same bytes.
pub(crate) fn encode_snapshot(meta: &[u8], values: &HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
    let mut entries: Vec<(&Vec<u8>, &Vec<u8>)> = values.iter().collect();
    entries.sort();
    let size = SNAPSHOT_MAGIC.len()
        + 8
        + meta.len()
        + entries
            .iter()
            .map(|(k, v)| 8 + k.len() + v.len())
            .sum::<usize>();
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(SNAPSHOT_MAGIC);
    push_chunk(&mut out, meta);
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (key, value) in entries {
        push_chunk(&mut out, key);
        push_chunk(&mut out, value);
    }
    out
}

fn push_chunk(out: &mut Vec<u8>, chunk: &[u8]) {
    out.extend_from_slice(&(chunk.len() as u32).to_be_bytes());
    out.extend_from_slice(chunk);
}

fn read_u32(bytes: &mut &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    let (head, rest) = bytes.split_at(4);
    *bytes = rest;
    Some(u32::from_be_bytes(head.try_into().ok()?))
}

fn read_chunk<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
    let len = read_u32(bytes)? as usize;
    if bytes.len() < len {
        return None;
    }
    let (chunk, rest) = bytes.split_at(len);
    *bytes = rest;
    Some(chunk)
}

/// The inverse of [`encode_snapshot`].
pub(crate) type DecodedSnapshot = (Vec<u8>, HashMap<Vec<u8>, Vec<u8>>);

pub(crate) fn decode_snapshot(snapshot: &[u8]) -> Result<DecodedSnapshot, MlsError> {
    let corrupt = || MlsError::Corrupt("snapshot framing".into());
    let mut rest = snapshot
        .strip_prefix(SNAPSHOT_MAGIC.as_slice())
        .ok_or_else(|| MlsError::Corrupt("not a udb-mls client snapshot".into()))?;
    let meta = read_chunk(&mut rest).ok_or_else(corrupt)?.to_vec();
    let count = read_u32(&mut rest).ok_or_else(corrupt)?;
    let mut values = HashMap::with_capacity(count as usize);
    for _ in 0..count {
        let key = read_chunk(&mut rest).ok_or_else(corrupt)?.to_vec();
        let value = read_chunk(&mut rest).ok_or_else(corrupt)?.to_vec();
        values.insert(key, value);
    }
    if !rest.is_empty() {
        return Err(corrupt());
    }
    Ok((meta, values))
}
