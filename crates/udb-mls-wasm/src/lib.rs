//! JavaScript bindings for `udb-mls` (wasm-bindgen). The TypeScript SDK's
//! `MessagingClient` drives the network; this module holds one device's MLS
//! state and does every cryptographic operation, exactly as the Rust SDK does
//! (it is the same Rust code).
//!
//! State lives in memory; [`MlsDevice::snapshot`] exports it (SECRET: private
//! keys) after every mutation so the host can persist it, and
//! [`MlsDevice::restore`] loads it back. Epochs and sequence numbers cross as
//! JS numbers (exact up to 2^53).

// `#[wasm_bindgen(getter_with_clone)]` generates a `.clone()` getter for every
// field of a struct, including the `f64`/`bool` ones.
#![allow(clippy::clone_on_copy)]

use std::collections::BTreeMap;
use std::sync::Arc;

use js_sys::{Array, Reflect, Uint8Array};
use udb_mls::{
    CommitIntent, DeviceIdentity, InboxEvent, InboxItem, InboxKind, KeySource, MemoryStore,
    MlsClient, MlsError, PendingCommit,
};
use wasm_bindgen::prelude::*;

/// A JSON string literal (the identifiers in a key-change error are tenant
/// subject ids and device ids; escape them anyway).
fn json_string(value: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A changed key crosses as `key_changed:{json}` so the TypeScript SDK can
/// raise a typed `KeyChangedError`; every other error as its message.
fn js_err(err: MlsError) -> JsError {
    if let MlsError::KeyChanged {
        subject_id,
        device_id,
        pinned_fingerprint,
        observed_fingerprint,
    } = &err
    {
        return JsError::new(&format!(
            "key_changed:{{\"subjectId\":{},\"deviceId\":{},\"pinnedFingerprint\":{},\"observedFingerprint\":{}}}",
            json_string(subject_id),
            json_string(device_id),
            json_string(pinned_fingerprint),
            json_string(observed_fingerprint),
        ));
    }
    JsError::new(&err.to_string())
}

/// Unix seconds from the JS host (`SystemTime` is unavailable on
/// wasm32-unknown-unknown).
#[cfg(target_arch = "wasm32")]
fn js_clock() -> u64 {
    (js_sys::Date::now() / 1000.0) as u64
}

/// Install the JS host clock for pin timestamps (wasm32 only; a native build of
/// this crate keeps the system clock).
fn with_host_clock<S: udb_mls::StateStore>(client: MlsClient<S>) -> MlsClient<S> {
    #[cfg(target_arch = "wasm32")]
    {
        client.with_clock(js_clock)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        client
    }
}

fn string_field(value: &JsValue, name: &str) -> Result<String, JsError> {
    Reflect::get(value, &JsValue::from_str(name))
        .ok()
        .and_then(|field| field.as_string())
        .ok_or_else(|| JsError::new(&format!("expected a string `{name}`")))
}

fn bytes_list(list: &Array) -> Result<Vec<Vec<u8>>, JsError> {
    list.iter()
        .map(|value| {
            value
                .dyn_into::<Uint8Array>()
                .map(|array| array.to_vec())
                .map_err(|_| JsError::new("expected an array of Uint8Array"))
        })
        .collect()
}

fn to_js_bytes(list: Vec<Vec<u8>>) -> Array {
    list.iter()
        .map(|bytes| JsValue::from(Uint8Array::from(bytes.as_slice())))
        .collect()
}

/// One device: its identity, keys, groups and inbox cursor.
#[wasm_bindgen]
pub struct MlsDevice {
    client: MlsClient<Arc<MemoryStore>>,
    store: Arc<MemoryStore>,
    /// Intents of commits dropped by `discardCommit`, for `recommit`.
    discarded: BTreeMap<String, CommitIntent>,
}

/// What `RegisterMessagingDevice` needs.
#[wasm_bindgen(getter_with_clone)]
pub struct DeviceRegistration {
    #[wasm_bindgen(js_name = tenantId)]
    pub tenant_id: String,
    #[wasm_bindgen(js_name = subjectId)]
    pub subject_id: String,
    #[wasm_bindgen(js_name = deviceId)]
    pub device_id: String,
    #[wasm_bindgen(js_name = signaturePublicKey)]
    pub signature_public_key: Vec<u8>,
    pub credential: Vec<u8>,
    #[wasm_bindgen(js_name = keyFingerprint)]
    pub key_fingerprint: String,
}

/// What `CreateGroup` needs.
#[wasm_bindgen(getter_with_clone)]
pub struct GroupCreation {
    pub epoch: f64,
    #[wasm_bindgen(js_name = groupInfo)]
    pub group_info: Vec<u8>,
    #[wasm_bindgen(js_name = ratchetTree)]
    pub ratchet_tree: Vec<u8>,
}

/// A commit for `SubmitCommit`; confirm or discard it after the broker answers.
#[wasm_bindgen(getter_with_clone)]
pub struct Commit {
    #[wasm_bindgen(js_name = groupId)]
    pub group_id: String,
    /// The epoch the commit was built on.
    pub epoch: f64,
    pub commit: Vec<u8>,
    /// Empty when the commit adds nobody.
    pub welcome: Vec<u8>,
    /// The signed GroupInfo of the epoch the commit creates
    /// (`SubmitCommitRequest.group_info`); empty when none was produced.
    #[wasm_bindgen(js_name = groupInfo)]
    pub group_info: Vec<u8>,
    #[wasm_bindgen(js_name = addedDeviceIds)]
    pub added_device_ids: Vec<String>,
    #[wasm_bindgen(js_name = removedDeviceIds)]
    pub removed_device_ids: Vec<String>,
}

impl From<PendingCommit> for Commit {
    fn from(pending: PendingCommit) -> Self {
        Self {
            group_id: pending.group_id,
            epoch: pending.epoch as f64,
            commit: pending.commit,
            welcome: pending.welcome.unwrap_or_default(),
            group_info: pending.group_info.unwrap_or_default(),
            added_device_ids: pending.added_device_ids,
            removed_device_ids: pending.removed_device_ids,
        }
    }
}

/// An application message sealed for `SendMessage`.
#[wasm_bindgen(getter_with_clone)]
pub struct Sealed {
    pub epoch: f64,
    pub ciphertext: Vec<u8>,
}

/// The device a verified KeyPackage names.
#[wasm_bindgen(getter_with_clone)]
pub struct KeyPackageInfo {
    #[wasm_bindgen(js_name = tenantId)]
    pub tenant_id: String,
    #[wasm_bindgen(js_name = subjectId)]
    pub subject_id: String,
    #[wasm_bindgen(js_name = deviceId)]
    pub device_id: String,
    #[wasm_bindgen(js_name = lastResort)]
    pub last_resort: bool,
    #[wasm_bindgen(js_name = signaturePublicKey)]
    pub signature_public_key: Vec<u8>,
    #[wasm_bindgen(js_name = keyFingerprint)]
    pub key_fingerprint: String,
}

/// A key event (`MlsDevice.takeKeyEvents`). `kind` is `new_device` or
/// `key_changed`; `source` is `directory`, `key_package`, `welcome`, `commit` or
/// `group_info`. For a new device `observedFingerprint` is its key and
/// `pinnedFingerprint` is empty; `groupId` is empty and `seq` 0 when the key was
/// not seen in an inbox entry.
#[wasm_bindgen(getter_with_clone)]
pub struct KeyEvent {
    pub kind: String,
    #[wasm_bindgen(js_name = subjectId)]
    pub subject_id: String,
    #[wasm_bindgen(js_name = deviceId)]
    pub device_id: String,
    #[wasm_bindgen(js_name = pinnedFingerprint)]
    pub pinned_fingerprint: String,
    #[wasm_bindgen(js_name = observedFingerprint)]
    pub observed_fingerprint: String,
    pub source: String,
    #[wasm_bindgen(js_name = groupId)]
    pub group_id: String,
    pub seq: f64,
}

impl From<udb_mls::KeyEvent> for KeyEvent {
    fn from(event: udb_mls::KeyEvent) -> Self {
        match event {
            udb_mls::KeyEvent::NewDevice {
                subject_id,
                device_id,
                fingerprint,
                source,
                group_id,
                seq,
            } => Self {
                kind: "new_device".into(),
                subject_id,
                device_id,
                pinned_fingerprint: String::new(),
                observed_fingerprint: fingerprint,
                source: source.as_str().into(),
                group_id: group_id.unwrap_or_default(),
                seq: seq.unwrap_or_default() as f64,
            },
            udb_mls::KeyEvent::KeyChanged {
                subject_id,
                device_id,
                pinned_fingerprint,
                observed_fingerprint,
                source,
                group_id,
                seq,
            } => Self {
                kind: "key_changed".into(),
                subject_id,
                device_id,
                pinned_fingerprint,
                observed_fingerprint,
                source: source.as_str().into(),
                group_id: group_id.unwrap_or_default(),
                seq: seq.unwrap_or_default() as f64,
            },
        }
    }
}

/// A pinned device key (`MlsDevice.pinnedDevices`). `pendingFingerprint` is the
/// observed, not yet accepted key (empty when none).
#[wasm_bindgen(getter_with_clone)]
pub struct PinnedDevice {
    #[wasm_bindgen(js_name = subjectId)]
    pub subject_id: String,
    #[wasm_bindgen(js_name = deviceId)]
    pub device_id: String,
    #[wasm_bindgen(js_name = signaturePublicKey)]
    pub signature_public_key: Vec<u8>,
    #[wasm_bindgen(js_name = keyFingerprint)]
    pub key_fingerprint: String,
    #[wasm_bindgen(js_name = firstSeenUnix)]
    pub first_seen_unix: f64,
    pub verified: bool,
    #[wasm_bindgen(js_name = pendingFingerprint)]
    pub pending_fingerprint: String,
}

/// The outcome of processing one inbox entry. `kind` is one of `joined`,
/// `commit`, `own_commit`, `removed`, `message`, `removal_hint`, `skipped`,
/// `undecryptable`; fields that do not apply to a kind are empty.
#[wasm_bindgen(getter_with_clone)]
#[derive(Default)]
pub struct Event {
    pub kind: String,
    pub seq: f64,
    #[wasm_bindgen(js_name = groupId)]
    pub group_id: String,
    pub epoch: f64,
    /// The member that committed, added this device, sent the message, or was
    /// revoked (removal hint).
    #[wasm_bindgen(js_name = deviceId)]
    pub device_id: String,
    #[wasm_bindgen(js_name = senderSubjectId)]
    pub sender_subject_id: String,
    #[wasm_bindgen(js_name = clientMessageId)]
    pub client_message_id: String,
    pub plaintext: Vec<u8>,
    /// `joined`: every member; `commit`: the added devices.
    pub members: Vec<String>,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub merged: bool,
    pub reason: String,
}

impl From<InboxEvent> for Event {
    fn from(event: InboxEvent) -> Self {
        let seq = event.seq() as f64;
        match event {
            InboxEvent::Joined {
                group_id,
                epoch,
                members,
                added_by,
                ..
            } => Self {
                kind: "joined".into(),
                seq,
                group_id,
                epoch: epoch as f64,
                device_id: added_by,
                members,
                ..Self::default()
            },
            InboxEvent::Commit {
                group_id,
                epoch,
                sender_device_id,
                added,
                removed,
                ..
            } => Self {
                kind: "commit".into(),
                seq,
                group_id,
                epoch: epoch as f64,
                device_id: sender_device_id,
                added,
                removed,
                ..Self::default()
            },
            InboxEvent::OwnCommit {
                group_id,
                epoch,
                merged,
                ..
            } => Self {
                kind: "own_commit".into(),
                seq,
                group_id,
                epoch: epoch as f64,
                merged,
                ..Self::default()
            },
            InboxEvent::Removed {
                group_id,
                removed_by,
                ..
            } => Self {
                kind: "removed".into(),
                seq,
                group_id,
                device_id: removed_by,
                ..Self::default()
            },
            InboxEvent::Message {
                group_id,
                epoch,
                sender_device_id,
                sender_subject_id,
                client_message_id,
                plaintext,
                ..
            } => Self {
                kind: "message".into(),
                seq,
                group_id,
                epoch: epoch as f64,
                device_id: sender_device_id,
                sender_subject_id,
                client_message_id,
                plaintext,
                ..Self::default()
            },
            InboxEvent::RemovalHint {
                group_id,
                device_id,
                reason,
                ..
            } => Self {
                kind: "removal_hint".into(),
                seq,
                group_id,
                device_id,
                reason,
                ..Self::default()
            },
            InboxEvent::Skipped { reason, .. } => Self {
                kind: "skipped".into(),
                seq,
                reason,
                ..Self::default()
            },
            InboxEvent::Undecryptable {
                group_id, reason, ..
            } => Self {
                kind: "undecryptable".into(),
                seq,
                group_id,
                reason,
                ..Self::default()
            },
        }
    }
}

/// Verify a KeyPackage and return the device it names.
#[wasm_bindgen(js_name = inspectKeyPackage)]
pub fn inspect_key_package(bytes: &[u8]) -> Result<KeyPackageInfo, JsError> {
    let info = udb_mls::inspect_key_package(bytes).map_err(js_err)?;
    Ok(KeyPackageInfo {
        tenant_id: info.identity.tenant_id,
        subject_id: info.identity.subject_id,
        device_id: info.identity.device_id,
        last_resort: info.last_resort,
        key_fingerprint: udb_mls::key_fingerprint(&info.signature_public_key),
        signature_public_key: info.signature_public_key,
    })
}

/// An opened encrypted backup: the device snapshot (SECRET; `MlsDevice.restore`
/// takes it) and the application history it carried.
#[wasm_bindgen(getter_with_clone)]
pub struct OpenedBackup {
    pub snapshot: Vec<u8>,
    pub history: Vec<u8>,
}

/// Open a backup envelope (as `GetMessagingBackup` returns it) with the user's
/// recovery secret. A wrong secret and a modified envelope both fail.
#[wasm_bindgen(js_name = openBackup)]
pub fn open_backup(recovery_secret: &[u8], envelope: &[u8]) -> Result<OpenedBackup, JsError> {
    let mut contents = udb_mls::open_backup(recovery_secret, envelope).map_err(js_err)?;
    Ok(OpenedBackup {
        snapshot: std::mem::take(&mut contents.snapshot),
        history: std::mem::take(&mut contents.history),
    })
}

/// Lowercase hex SHA-256 of a signature public key (the directory fingerprint).
#[wasm_bindgen(js_name = keyFingerprint)]
pub fn key_fingerprint(signature_public_key: &[u8]) -> String {
    udb_mls::key_fingerprint(signature_public_key)
}

#[wasm_bindgen]
impl MlsDevice {
    /// A new device with fresh keys.
    pub fn create(
        tenant_id: &str,
        subject_id: &str,
        device_id: &str,
    ) -> Result<MlsDevice, JsError> {
        let identity = DeviceIdentity::new(tenant_id, subject_id, device_id).map_err(js_err)?;
        let store = Arc::new(MemoryStore::new());
        let client =
            with_host_clock(MlsClient::create(identity, Arc::clone(&store)).map_err(js_err)?);
        Ok(Self {
            client,
            store,
            discarded: BTreeMap::new(),
        })
    }

    /// The device saved in `snapshot`.
    pub fn restore(snapshot: &[u8]) -> Result<MlsDevice, JsError> {
        let store = Arc::new(MemoryStore::from_snapshot(snapshot.to_vec()));
        let client = with_host_clock(MlsClient::open(Arc::clone(&store)).map_err(js_err)?);
        Ok(Self {
            client,
            store,
            discarded: BTreeMap::new(),
        })
    }

    /// The complete device state (SECRET). Persist it after every call that
    /// changes state; it is what `restore` takes.
    pub fn snapshot(&self) -> Vec<u8> {
        self.store.snapshot().unwrap_or_default()
    }

    /// Seal this device's state and the application's `history` under a key
    /// derived from `recoverySecret` (Argon2id with the recommended costs, then
    /// HKDF-SHA256, then XChaCha20-Poly1305), for `PutMessagingBackup`.
    #[wasm_bindgen(js_name = exportBackup)]
    pub fn export_backup(
        &self,
        recovery_secret: &[u8],
        history: &[u8],
    ) -> Result<Vec<u8>, JsError> {
        self.client
            .export_backup(recovery_secret, history)
            .map_err(js_err)
    }

    /// `exportBackup` with explicit Argon2id costs (memory in KiB).
    #[wasm_bindgen(js_name = exportBackupWith)]
    pub fn export_backup_with(
        &self,
        recovery_secret: &[u8],
        history: &[u8],
        memory_kib: u32,
        iterations: u32,
        parallelism: u32,
    ) -> Result<Vec<u8>, JsError> {
        self.client
            .export_backup_with(
                recovery_secret,
                history,
                udb_mls::BackupParams {
                    memory_kib,
                    iterations,
                    parallelism,
                },
            )
            .map_err(js_err)
    }

    #[wasm_bindgen(getter, js_name = tenantId)]
    pub fn tenant_id(&self) -> String {
        self.client.identity().tenant_id.clone()
    }

    #[wasm_bindgen(getter, js_name = subjectId)]
    pub fn subject_id(&self) -> String {
        self.client.identity().subject_id.clone()
    }

    #[wasm_bindgen(getter, js_name = deviceId)]
    pub fn device_id(&self) -> String {
        self.client.identity().device_id.clone()
    }

    #[wasm_bindgen(getter, js_name = inboxCursor)]
    pub fn inbox_cursor(&self) -> f64 {
        self.client.inbox_cursor() as f64
    }

    pub fn registration(&self) -> Result<DeviceRegistration, JsError> {
        let registration = self.client.registration().map_err(js_err)?;
        Ok(DeviceRegistration {
            tenant_id: registration.tenant_id,
            subject_id: registration.subject_id,
            device_id: registration.device_id,
            signature_public_key: registration.signature_public_key,
            credential: registration.credential,
            key_fingerprint: registration.key_fingerprint,
        })
    }

    /// `count` one-time KeyPackages (an array of Uint8Array).
    #[wasm_bindgen(js_name = keyPackages)]
    pub fn key_packages(&mut self, count: usize) -> Result<Array, JsError> {
        Ok(to_js_bytes(
            self.client.key_packages(count).map_err(js_err)?,
        ))
    }

    #[wasm_bindgen(js_name = lastResortKeyPackage)]
    pub fn last_resort_key_package(&mut self) -> Result<Vec<u8>, JsError> {
        self.client.last_resort_key_package().map_err(js_err)
    }

    #[wasm_bindgen(js_name = createGroup)]
    pub fn create_group(&mut self, group_id: &str) -> Result<GroupCreation, JsError> {
        let creation = self.client.create_group(group_id).map_err(js_err)?;
        Ok(GroupCreation {
            epoch: creation.epoch as f64,
            group_info: creation.group_info,
            ratchet_tree: creation.ratchet_tree,
        })
    }

    #[wasm_bindgen(js_name = forgetGroup)]
    pub fn forget_group(&mut self, group_id: &str) -> Result<(), JsError> {
        self.client.forget_group(group_id).map_err(js_err)
    }

    #[wasm_bindgen(js_name = groupIds)]
    pub fn group_ids(&self) -> Vec<String> {
        self.client.group_ids()
    }

    pub fn epoch(&self, group_id: &str) -> Result<f64, JsError> {
        Ok(self.client.epoch(group_id).map_err(js_err)? as f64)
    }

    /// Member device ids, in leaf order.
    pub fn members(&self, group_id: &str) -> Result<Vec<String>, JsError> {
        Ok(self
            .client
            .members(group_id)
            .map_err(js_err)?
            .into_iter()
            .map(|member| member.device_id)
            .collect())
    }

    #[wasm_bindgen(js_name = hasPendingCommit)]
    pub fn has_pending_commit(&self, group_id: &str) -> bool {
        self.client.has_pending_commit(group_id)
    }

    fn commit(&mut self, group_id: &str, intent: CommitIntent) -> Result<Option<Commit>, JsError> {
        Ok(self
            .client
            .commit(group_id, intent)
            .map_err(js_err)?
            .map(Commit::from))
    }

    /// Add the devices behind these KeyPackages; `undefined` when every one is
    /// already a member.
    #[wasm_bindgen(js_name = commitAdd)]
    pub fn commit_add(
        &mut self,
        group_id: &str,
        key_packages: &Array,
    ) -> Result<Option<Commit>, JsError> {
        let key_packages = bytes_list(key_packages)?;
        self.commit(group_id, CommitIntent::Add { key_packages })
    }

    #[wasm_bindgen(js_name = commitRemove)]
    pub fn commit_remove(
        &mut self,
        group_id: &str,
        device_ids: Vec<String>,
    ) -> Result<Option<Commit>, JsError> {
        self.commit(group_id, CommitIntent::Remove { device_ids })
    }

    /// Rotate this device's leaf keys.
    #[wasm_bindgen(js_name = commitUpdate)]
    pub fn commit_update(&mut self, group_id: &str) -> Result<Option<Commit>, JsError> {
        self.commit(group_id, CommitIntent::Update)
    }

    /// The broker accepted the pending commit. Returns the new epoch.
    #[wasm_bindgen(js_name = confirmCommit)]
    pub fn confirm_commit(&mut self, group_id: &str) -> Result<f64, JsError> {
        Ok(self.client.confirm_commit(group_id).map_err(js_err)?.epoch as f64)
    }

    /// The broker refused the pending commit. Its intent is kept for
    /// `recommit`; returns whether there is one.
    #[wasm_bindgen(js_name = discardCommit)]
    pub fn discard_commit(&mut self, group_id: &str) -> Result<bool, JsError> {
        match self.client.discard_commit(group_id).map_err(js_err)? {
            Some(intent) => {
                self.discarded.insert(group_id.to_string(), intent);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Rebuild the discarded commit on the group's current epoch.
    pub fn recommit(&mut self, group_id: &str) -> Result<Option<Commit>, JsError> {
        let intent = self
            .discarded
            .remove(group_id)
            .ok_or_else(|| JsError::new("no discarded commit to rebuild for this group"))?;
        self.commit(group_id, intent)
    }

    pub fn encrypt(&mut self, group_id: &str, plaintext: &[u8]) -> Result<Sealed, JsError> {
        let sealed = self.client.encrypt(group_id, plaintext).map_err(js_err)?;
        Ok(Sealed {
            epoch: sealed.epoch as f64,
            ciphertext: sealed.ciphertext,
        })
    }

    /// Process one inbox entry (fields of `InboxEntry`), in order.
    #[allow(clippy::too_many_arguments)]
    pub fn process(
        &mut self,
        seq: f64,
        kind: &str,
        group_id: &str,
        epoch: f64,
        payload: &[u8],
        sender_device_id: &str,
        client_message_id: &str,
    ) -> Result<Event, JsError> {
        let item = InboxItem {
            seq: seq as i64,
            kind: InboxKind::parse(kind),
            group_id: group_id.to_string(),
            epoch: if epoch > 0.0 { epoch as u64 } else { 0 },
            payload: payload.to_vec(),
            sender_device_id: sender_device_id.to_string(),
            client_message_id: client_message_id.to_string(),
        };
        Ok(Event::from(self.client.process(&item).map_err(js_err)?))
    }

    // ── key pinning ────────────────────────────────────────────────────────

    /// Record device keys (an array of `{ subjectId, deviceId,
    /// signaturePublicKey }`, as `ListMessagingDevices` returns them) against
    /// the pins; `source` is `directory` (default) or `key_package`. Every key
    /// is recorded; a key that differs from its pin throws `key_changed:{json}`.
    #[wasm_bindgen(js_name = observeDeviceKeys)]
    pub fn observe_device_keys(
        &mut self,
        keys: &Array,
        source: Option<String>,
    ) -> Result<(), JsError> {
        let source = match source.as_deref() {
            None | Some("") => KeySource::Directory,
            Some(name) => KeySource::parse(name)
                .ok_or_else(|| JsError::new(&format!("unknown key source `{name}`")))?,
        };
        let mut owned = Vec::with_capacity(keys.length() as usize);
        for value in keys.iter() {
            let key = Reflect::get(&value, &JsValue::from_str("signaturePublicKey"))
                .ok()
                .and_then(|field| field.dyn_into::<Uint8Array>().ok())
                .ok_or_else(|| JsError::new("expected a Uint8Array `signaturePublicKey`"))?
                .to_vec();
            owned.push((
                string_field(&value, "subjectId")?,
                string_field(&value, "deviceId")?,
                key,
            ));
        }
        let borrowed: Vec<(&str, &str, &[u8])> = owned
            .iter()
            .map(|(subject, device, key)| (subject.as_str(), device.as_str(), key.as_slice()))
            .collect();
        self.client
            .observe_device_keys(&borrowed, source)
            .map_err(js_err)
    }

    /// Key events (new devices, changed keys seen in a Welcome or a commit)
    /// since the last call, oldest first.
    #[wasm_bindgen(js_name = takeKeyEvents)]
    pub fn take_key_events(&mut self) -> Result<Vec<KeyEvent>, JsError> {
        Ok(self
            .client
            .take_key_events()
            .map_err(js_err)?
            .into_iter()
            .map(KeyEvent::from)
            .collect())
    }

    /// The safety number for this device's subject and `peerSubjectId`: 12
    /// groups of 5 digits, equal on both sides when both pinned the same keys.
    #[wasm_bindgen(js_name = safetyNumber)]
    pub fn safety_number(&self, peer_subject_id: &str) -> Result<String, JsError> {
        self.client.safety_number(peer_subject_id).map_err(js_err)
    }

    /// Mark a pinned device verified after comparing its fingerprint out of
    /// band; a different fingerprint throws `key_changed:{json}`.
    #[wasm_bindgen(js_name = verifyDevice)]
    pub fn verify_device(
        &mut self,
        subject_id: &str,
        device_id: &str,
        fingerprint: &str,
    ) -> Result<(), JsError> {
        self.client
            .verify_device(subject_id, device_id, fingerprint)
            .map_err(js_err)
    }

    /// Re-pin a device to its pending key, given that key's compared fingerprint.
    #[wasm_bindgen(js_name = acceptKeyChange)]
    pub fn accept_key_change(
        &mut self,
        subject_id: &str,
        device_id: &str,
        fingerprint: &str,
    ) -> Result<(), JsError> {
        self.client
            .accept_key_change(subject_id, device_id, fingerprint)
            .map_err(js_err)
    }

    /// A subject's pinned devices, sorted by device id.
    #[wasm_bindgen(js_name = pinnedDevices)]
    pub fn pinned_devices(&self, subject_id: &str) -> Vec<PinnedDevice> {
        self.client
            .pinned_devices(subject_id)
            .into_iter()
            .map(|(device_id, pin)| PinnedDevice {
                key_fingerprint: pin.fingerprint(),
                pending_fingerprint: pin.pending_fingerprint().unwrap_or_default(),
                subject_id: pin.subject_id,
                device_id,
                signature_public_key: pin.signature_public_key,
                first_seen_unix: pin.first_seen_unix as f64,
                verified: pin.verified,
            })
            .collect()
    }

    /// Drop a device's pin; false when nothing was pinned for it.
    #[wasm_bindgen(js_name = forgetDevice)]
    pub fn forget_device(&mut self, subject_id: &str, device_id: &str) -> Result<bool, JsError> {
        self.client
            .forget_device(subject_id, device_id)
            .map_err(js_err)
    }
}
