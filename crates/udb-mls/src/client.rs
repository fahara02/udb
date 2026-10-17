//! [`MlsClient`]: one device's MLS state and every operation on it.

use std::collections::{BTreeMap, BTreeSet};

use openmls::prelude::tls_codec::{Deserialize as _, Serialize as _};
use openmls::prelude::{
    BasicCredential, Capabilities, Credential, CredentialWithKey, ExtensionType, GroupId,
    KeyPackage, KeyPackageIn, LeafNodeIndex, LeafNodeParameters, MlsGroup, MlsGroupCreateConfig,
    MlsMessageBodyIn, MlsMessageIn, MlsMessageOut, OpenMlsProvider as _, ProcessedMessageContent,
    ProtocolMessage, ProtocolVersion, RatchetTreeIn, StagedWelcome,
    MIXED_PLAINTEXT_WIRE_FORMAT_POLICY,
};
use openmls_basic_credential::SignatureKeyPair;
use openmls_rust_crypto::OpenMlsRustCrypto;
use serde::{Deserialize, Serialize};

use crate::backup::{open_backup, seal_backup, BackupContents, BackupParams};
use crate::error::{mls, MlsError};
use crate::identity::{
    key_fingerprint, split_identity, validate_device_id, validate_group_id, DeviceIdentity,
    CIPHERSUITE,
};
use crate::inbox::{InboxEvent, InboxItem, InboxKind};
use crate::pins::{self, KeyEvent, KeySource, Observation, ObserveContext, PinnedKey};
use crate::store::{decode_snapshot, encode_snapshot, MemoryStore, StateStore};

/// Past epochs whose message secrets a member keeps, so an application message
/// sealed just before a commit still decrypts after the commit was applied.
const MAX_PAST_EPOCHS: usize = 8;

/// A built commit's messages: the commit, the Welcome (adds only), and the
/// GroupInfo of the epoch it creates.
type CommitMessages = (MlsMessageOut, Option<MlsMessageOut>, Option<MlsMessageOut>);

/// Snapshot metadata format version.
const META_VERSION: u32 = 1;

/// The group configuration every UDB client uses: handshakes go out as
/// `PublicMessage` (the broker validates them), application messages are
/// always `PrivateMessage`, the ratchet tree travels inside the Welcome.
fn group_config() -> MlsGroupCreateConfig {
    MlsGroupCreateConfig::builder()
        .wire_format_policy(MIXED_PLAINTEXT_WIRE_FORMAT_POLICY)
        .ciphersuite(CIPHERSUITE)
        .use_ratchet_tree_extension(true)
        .max_past_epochs(MAX_PAST_EPOCHS)
        .build()
}

/// What a commit does. Kept with a pending commit so a commit that lost an
/// epoch race can be rebuilt on the new epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitIntent {
    /// Add the devices behind these TLS-serialized KeyPackages (as returned by
    /// `ClaimKeyPackages`). Devices that are already members are skipped.
    Add { key_packages: Vec<Vec<u8>> },
    /// Remove these member devices. Devices that are not members are skipped.
    Remove { device_ids: Vec<String> },
    /// Rotate this device's leaf keys (post-compromise security).
    Update,
}

/// The registration this device sends to `RegisterMessagingDevice`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRegistration {
    pub tenant_id: String,
    pub subject_id: String,
    pub device_id: String,
    /// Raw Ed25519 signature public key (32 bytes).
    pub signature_public_key: Vec<u8>,
    /// TLS-serialized MLS `BasicCredential` naming `<tenant>/<subject>/<device>`.
    pub credential: Vec<u8>,
    /// Lowercase hex SHA-256 of the signature key, as the directory shows it.
    pub key_fingerprint: String,
}

/// What `CreateGroup` needs from the creator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCreation {
    pub group_id: String,
    pub epoch: u64,
    /// TLS-serialized `MlsMessage` carrying the signed GroupInfo.
    pub group_info: Vec<u8>,
    /// TLS-serialized ratchet tree.
    pub ratchet_tree: Vec<u8>,
}

/// A commit built but not yet accepted by the broker. Submit `commit` (and
/// `welcome`, when present) with `SubmitCommit` at `epoch`, then
/// [`MlsClient::confirm_commit`] on success or [`MlsClient::discard_commit`] on
/// a definite rejection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCommit {
    pub group_id: String,
    /// The epoch the commit was built on (`SubmitCommitRequest.epoch`).
    pub epoch: u64,
    /// TLS-serialized `MlsMessage` carrying the `PublicMessage` commit.
    pub commit: Vec<u8>,
    /// TLS-serialized Welcome for the added devices (adds only).
    pub welcome: Option<Vec<u8>>,
    /// TLS-serialized `MlsMessage` carrying the signed GroupInfo of the epoch
    /// this commit creates (`SubmitCommitRequest.group_info`; groups that allow
    /// external joins require it).
    pub group_info: Option<Vec<u8>>,
    pub added_device_ids: Vec<String>,
    pub removed_device_ids: Vec<String>,
    pub intent: CommitIntent,
}

/// An external commit that joins a group from its GroupInfo (RFC 9420
/// §12.4.3.2), built by [`MlsClient::join_external`]. Submit `commit` (no
/// Welcome) with `SubmitCommit` at `epoch`, then [`MlsClient::confirm_commit`]
/// on success or [`MlsClient::discard_commit`] on a definite rejection; until
/// then the group is held but refuses to send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalJoin {
    pub group_id: String,
    /// The epoch the commit was built on: the GroupInfo's epoch
    /// (`SubmitCommitRequest.epoch`).
    pub epoch: u64,
    /// TLS-serialized `MlsMessage` carrying the `PublicMessage` external commit.
    pub commit: Vec<u8>,
    /// TLS-serialized `MlsMessage` carrying the signed GroupInfo of the epoch
    /// the join creates.
    pub group_info: Option<Vec<u8>>,
}

/// The group after a confirmed commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitSummary {
    pub group_id: String,
    pub epoch: u64,
    pub members: Vec<String>,
}

/// An application message sealed for `SendMessage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingMessage {
    pub group_id: String,
    /// The epoch the message was sealed in.
    pub epoch: u64,
    /// TLS-serialized `MlsMessage` carrying the `PrivateMessage`.
    pub ciphertext: Vec<u8>,
}

/// One leaf of a group's ratchet tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub leaf_index: u32,
    pub tenant_id: String,
    pub subject_id: String,
    pub device_id: String,
    pub signature_public_key: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingMeta {
    epoch: u64,
    intent: CommitIntent,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct GroupMeta {
    /// The commit this device built and has not resolved yet.
    pending: Option<PendingMeta>,
    /// A commit that lost an epoch race (another member's commit for the same
    /// epoch arrived first); [`MlsClient::discard_commit`] hands it back so it
    /// can be rebuilt.
    lost: Option<CommitIntent>,
    /// This device joined by an external commit the broker has not accepted
    /// yet. The group is already at the new epoch (an external commit needs no
    /// merge); it is kept on acceptance (or when the inbox echoes the commit)
    /// and deleted on refusal.
    #[serde(default)]
    awaiting_external_ack: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    version: u32,
    identity: DeviceIdentity,
    signature_public_key: Vec<u8>,
    inbox_cursor: i64,
    groups: BTreeMap<String, GroupMeta>,
    /// Pinned signature keys by device id (device ids are tenant-unique).
    /// Absent in a 0.6.0 snapshot; [`MlsClient::open`] pins the own key.
    #[serde(default)]
    pins: BTreeMap<String, PinnedKey>,
    /// Key events not yet taken by the caller.
    #[serde(default)]
    key_events: Vec<KeyEvent>,
}

/// Unix seconds from the system clock. `wasm32-unknown-unknown` has no system
/// clock (`SystemTime::now` panics there), so it reads 0 until the host
/// installs one with [`MlsClient::with_clock`].
fn system_clock() -> u64 {
    #[cfg(not(target_arch = "wasm32"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default()
    }
    #[cfg(target_arch = "wasm32")]
    {
        0
    }
}

fn own_pin(signature_public_key: &[u8], subject_id: &str, now: u64) -> PinnedKey {
    PinnedKey {
        subject_id: subject_id.to_string(),
        signature_public_key: signature_public_key.to_vec(),
        first_seen_unix: now,
        // This device's own key needs no out-of-band comparison.
        verified: true,
        pending: None,
        pending_subject_id: None,
        pending_seen_unix: None,
    }
}

/// One device's MLS client. Every mutation is persisted through the
/// [`StateStore`] before the method returns; in particular a ciphertext is only
/// returned after the advanced sending ratchet was saved, so a crash can never
/// make the device reuse a message key.
pub struct MlsClient<S: StateStore> {
    provider: OpenMlsRustCrypto,
    signer: SignatureKeyPair,
    credential: CredentialWithKey,
    meta: Meta,
    store: S,
    /// Unix seconds, for pin timestamps.
    clock: fn() -> u64,
}

impl<S: StateStore> std::fmt::Debug for MlsClient<S> {
    // Never prints key material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MlsClient")
            .field("identity", &self.meta.identity)
            .field("inbox_cursor", &self.meta.inbox_cursor)
            .field("groups", &self.meta.groups.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

fn rejected<E: std::fmt::Display>(context: &str) -> impl FnOnce(E) -> MlsError + '_ {
    move |err| MlsError::Rejected(format!("{context}: {err}"))
}

/// What a verified KeyPackage says about its device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPackageInfo {
    pub identity: DeviceIdentity,
    pub signature_public_key: Vec<u8>,
    pub last_resort: bool,
}

fn parse_key_package(bytes: &[u8]) -> Result<(KeyPackage, KeyPackageInfo), MlsError> {
    let provider = OpenMlsRustCrypto::default();
    let package = KeyPackageIn::tls_deserialize_exact(bytes)
        .map_err(rejected("not an MLS KeyPackage"))?
        .validate(provider.crypto(), ProtocolVersion::Mls10)
        .map_err(rejected("KeyPackage did not verify"))?;
    if package.ciphersuite() != CIPHERSUITE {
        return Err(MlsError::Rejected(
            "KeyPackage uses a ciphersuite other than the service's".into(),
        ));
    }
    let leaf = package.leaf_node();
    let (tenant, subject, device) = credential_identity(leaf.credential()).ok_or_else(|| {
        MlsError::Rejected("KeyPackage credential is not a UDB device identity".into())
    })?;
    let info = KeyPackageInfo {
        identity: DeviceIdentity::new(tenant, subject, device)?,
        signature_public_key: leaf.signature_key().as_slice().to_vec(),
        last_resort: package.last_resort(),
    };
    Ok((package, info))
}

/// Verify a TLS-serialized KeyPackage (signature, lifetime, ciphersuite) and
/// return the device it names, so a caller can check that a package claimed
/// for a device really is that device's before adding it.
pub fn inspect_key_package(bytes: &[u8]) -> Result<KeyPackageInfo, MlsError> {
    parse_key_package(bytes).map(|(_, info)| info)
}

fn credential_identity(credential: &Credential) -> Option<(String, String, String)> {
    BasicCredential::try_from(credential.clone())
        .ok()
        .and_then(|basic| split_identity(basic.identity()))
}

fn group_id_of(group_id: &str) -> GroupId {
    GroupId::from_slice(group_id.as_bytes())
}

fn member_devices(group: &MlsGroup) -> BTreeMap<String, LeafNodeIndex> {
    group
        .members()
        .filter_map(|member| {
            credential_identity(&member.credential).map(|(_, _, device)| (device, member.index))
        })
        .collect()
}

fn protocol_message(payload: &[u8]) -> Result<ProtocolMessage, String> {
    MlsMessageIn::tls_deserialize_exact(payload)
        .map_err(|err| format!("not an MLS message: {err}"))?
        .try_into_protocol_message()
        .map_err(|err| format!("not a protocol message: {err:?}"))
}

impl<S: StateStore> MlsClient<S> {
    /// A new device: generates its Ed25519 signature key and credential and
    /// saves them. Fails if the store already holds a device.
    pub fn create(identity: DeviceIdentity, store: S) -> Result<Self, MlsError> {
        if store.load()?.is_some() {
            let existing = Self::open(store)?;
            return Err(MlsError::AlreadyInitialized(
                existing.meta.identity.credential_identity(),
            ));
        }
        // Re-validate: the fields are public and may have been built by hand.
        let identity =
            DeviceIdentity::new(identity.tenant_id, identity.subject_id, identity.device_id)?;
        let provider = OpenMlsRustCrypto::default();
        let signer = SignatureKeyPair::new(CIPHERSUITE.signature_algorithm())
            .map_err(mls("generate the signature key"))?;
        signer
            .store(provider.storage())
            .map_err(mls("store the signature key"))?;
        let credential = CredentialWithKey {
            credential: BasicCredential::new(identity.credential_identity().into_bytes()).into(),
            signature_key: signer.public().into(),
        };
        let signature_public_key = signer.to_public_vec();
        let pins = BTreeMap::from([(
            identity.device_id.clone(),
            own_pin(&signature_public_key, &identity.subject_id, system_clock()),
        )]);
        let meta = Meta {
            version: META_VERSION,
            identity,
            signature_public_key,
            inbox_cursor: 0,
            groups: BTreeMap::new(),
            pins,
            key_events: Vec::new(),
        };
        let client = Self {
            provider,
            signer,
            credential,
            meta,
            store,
            clock: system_clock,
        };
        client.persist()?;
        Ok(client)
    }

    /// The device saved in `store`.
    pub fn open(store: S) -> Result<Self, MlsError> {
        let snapshot = store.load()?.ok_or(MlsError::NotInitialized)?;
        let (meta, values) = decode_snapshot(&snapshot)?;
        let mut meta: Meta = serde_json::from_slice(&meta)
            .map_err(|err| MlsError::Corrupt(format!("client metadata: {err}")))?;
        if meta.version != META_VERSION {
            return Err(MlsError::Corrupt(format!(
                "client metadata version {} is not {META_VERSION}",
                meta.version
            )));
        }
        let provider = OpenMlsRustCrypto::default();
        *provider
            .storage()
            .values
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = values;
        let signer = SignatureKeyPair::read(
            provider.storage(),
            &meta.signature_public_key,
            CIPHERSUITE.signature_algorithm(),
        )
        .ok_or_else(|| MlsError::Corrupt("the signature key is missing".into()))?;
        let credential = CredentialWithKey {
            credential: BasicCredential::new(meta.identity.credential_identity().into_bytes())
                .into(),
            signature_key: signer.public().into(),
        };
        // A 0.6.0 snapshot has no pins: pin this device's own key (saved with
        // the next mutation).
        if !meta.pins.contains_key(&meta.identity.device_id) {
            let pin = own_pin(
                &meta.signature_public_key,
                &meta.identity.subject_id,
                system_clock(),
            );
            meta.pins.insert(meta.identity.device_id.clone(), pin);
        }
        Ok(Self {
            provider,
            signer,
            credential,
            meta,
            store,
            clock: system_clock,
        })
    }

    /// Use `clock` (Unix seconds) for pin timestamps instead of the system
    /// clock; the WASM binding installs the JS host's clock this way. Pins
    /// recorded while no clock was available (timestamp 0) take its time.
    pub fn with_clock(mut self, clock: fn() -> u64) -> Self {
        self.clock = clock;
        let now = clock();
        for pin in self.meta.pins.values_mut() {
            if pin.first_seen_unix == 0 {
                pin.first_seen_unix = now;
            }
            if pin.pending_seen_unix == Some(0) {
                pin.pending_seen_unix = Some(now);
            }
        }
        self
    }

    /// Open the device in `store`, or create it when the store is empty. A store
    /// holding a DIFFERENT device is an error, never silently replaced.
    pub fn open_or_create(identity: DeviceIdentity, store: S) -> Result<Self, MlsError> {
        if store.load()?.is_none() {
            return Self::create(identity, store);
        }
        let client = Self::open(store)?;
        if client.meta.identity != identity {
            return Err(MlsError::AlreadyInitialized(
                client.meta.identity.credential_identity(),
            ));
        }
        Ok(client)
    }

    fn persist(&self) -> Result<(), MlsError> {
        let meta = serde_json::to_vec(&self.meta)
            .map_err(|err| MlsError::Store(format!("encode client metadata: {err}")))?;
        let snapshot = {
            let values = self
                .provider
                .storage()
                .values
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            encode_snapshot(&meta, &values)
        };
        self.store.save(&snapshot)
    }

    /// The complete client state as the store holds it (SECRET: it contains the
    /// device's private keys).
    pub fn snapshot(&self) -> Result<Vec<u8>, MlsError> {
        self.store.load()?.ok_or(MlsError::NotInitialized)
    }

    /// Seal this device's saved state, plus the application's `history`, as a
    /// backup envelope for `PutMessagingBackup`, under a key derived from
    /// `recovery_secret` with the recommended Argon2id costs. The broker can
    /// store it but never open it.
    pub fn export_backup(
        &self,
        recovery_secret: &[u8],
        history: &[u8],
    ) -> Result<Vec<u8>, MlsError> {
        self.export_backup_with(recovery_secret, history, BackupParams::default())
    }

    /// [`Self::export_backup`] with explicit Argon2id costs.
    pub fn export_backup_with(
        &self,
        recovery_secret: &[u8],
        history: &[u8],
        params: BackupParams,
    ) -> Result<Vec<u8>, MlsError> {
        let contents = BackupContents {
            snapshot: self.snapshot()?,
            history: history.to_vec(),
        };
        seal_backup(recovery_secret, &contents, params)
    }

    /// Restore a device from a backup envelope (as `GetMessagingBackup` returns
    /// it) into an EMPTY `store`, on a new device. Returns the device, which is
    /// the backed-up device with its identity, keys, groups and inbox cursor,
    /// and the history the backup carried. A store that already holds a device
    /// is never overwritten.
    pub fn restore_backup(
        recovery_secret: &[u8],
        envelope: &[u8],
        store: S,
    ) -> Result<(Self, Vec<u8>), MlsError> {
        if store.load()?.is_some() {
            let existing = Self::open(store)?;
            return Err(MlsError::AlreadyInitialized(
                existing.meta.identity.credential_identity(),
            ));
        }
        let mut contents = open_backup(recovery_secret, envelope)?;
        // Prove the snapshot opens before it replaces anything.
        MlsClient::<MemoryStore>::open(MemoryStore::from_snapshot(contents.snapshot.clone()))?;
        store.save(&contents.snapshot)?;
        let client = Self::open(store)?;
        Ok((client, std::mem::take(&mut contents.history)))
    }

    /// The underlying store.
    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn identity(&self) -> &DeviceIdentity {
        &self.meta.identity
    }

    /// The highest inbox sequence number processed so far.
    pub fn inbox_cursor(&self) -> i64 {
        self.meta.inbox_cursor
    }

    /// What `RegisterMessagingDevice` needs.
    pub fn registration(&self) -> Result<DeviceRegistration, MlsError> {
        Ok(DeviceRegistration {
            tenant_id: self.meta.identity.tenant_id.clone(),
            subject_id: self.meta.identity.subject_id.clone(),
            device_id: self.meta.identity.device_id.clone(),
            signature_public_key: self.signer.to_public_vec(),
            credential: self
                .credential
                .credential
                .tls_serialize_detached()
                .map_err(mls("serialize the credential"))?,
            key_fingerprint: key_fingerprint(self.signer.public()),
        })
    }

    // ── key pinning ────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    fn observe_key(
        &mut self,
        subject: &str,
        device: &str,
        key: &[u8],
        known: &BTreeSet<String>,
        source: KeySource,
        group_id: Option<&str>,
        seq: Option<i64>,
    ) -> Observation {
        let own_subject = self.meta.identity.subject_id.clone();
        let ctx = ObserveContext {
            known_subjects: known,
            own_subject: &own_subject,
            source,
            group_id,
            seq,
            now: (self.clock)(),
        };
        pins::observe(
            &mut self.meta.pins,
            &mut self.meta.key_events,
            subject,
            device,
            key,
            &ctx,
        )
    }

    /// Check every leaf of a group against the pins. Changed keys from a source
    /// that does not fail closed become [`KeyEvent`]s (saved with the inbox
    /// cursor); the first changed key is returned as [`MlsError::KeyChanged`]
    /// for the caller that does fail closed.
    fn observe_members(
        &mut self,
        group: &MlsGroup,
        source: KeySource,
        group_id: &str,
        seq: Option<i64>,
    ) -> Option<MlsError> {
        let tenant = self.meta.identity.tenant_id.clone();
        let members: Vec<(String, String, Vec<u8>)> = group
            .members()
            .filter_map(|member| {
                let (member_tenant, subject, device) = credential_identity(&member.credential)?;
                (member_tenant == tenant).then_some((subject, device, member.signature_key))
            })
            .collect();
        let known = pins::known_subjects(&self.meta.pins);
        let mut changed = None;
        for (subject, device, key) in members {
            let observed =
                self.observe_key(&subject, &device, &key, &known, source, Some(group_id), seq);
            if observed == Observation::Changed && changed.is_none() {
                changed = Some(pins::key_changed(&self.meta.pins, &subject, &device, &key));
            }
        }
        changed
    }

    /// Record device keys learned outside MLS (the directory's
    /// `ListMessagingDevices` rows: subject, device id, signature public key)
    /// against the pins and save them once. Every key is recorded; the first
    /// one that differs from its pin is returned as [`MlsError::KeyChanged`]
    /// (the pin is unchanged and the observed key kept pending).
    pub fn observe_device_keys(
        &mut self,
        keys: &[(&str, &str, &[u8])],
        source: KeySource,
    ) -> Result<(), MlsError> {
        for (subject, device, key) in keys {
            validate_device_id(device)?;
            if subject.is_empty() || key.is_empty() {
                return Err(MlsError::Rejected(format!(
                    "device `{device}` has no subject or no signature key"
                )));
            }
        }
        let known = pins::known_subjects(&self.meta.pins);
        let mut changed = None;
        for (subject, device, key) in keys {
            let observed = self.observe_key(subject, device, key, &known, source, None, None);
            if observed == Observation::Changed && changed.is_none() {
                changed = Some(pins::key_changed(&self.meta.pins, subject, device, key));
            }
        }
        self.persist()?;
        match changed {
            Some(err) if source.fails_closed() => Err(err),
            _ => Ok(()),
        }
    }

    /// Key events (new devices, changed keys seen in a Welcome or a commit)
    /// recorded since the last call, oldest first. Taking them is saved.
    pub fn take_key_events(&mut self) -> Result<Vec<KeyEvent>, MlsError> {
        if self.meta.key_events.is_empty() {
            return Ok(Vec::new());
        }
        let events = std::mem::take(&mut self.meta.key_events);
        self.persist()?;
        Ok(events)
    }

    /// Key events not yet taken.
    pub fn key_events(&self) -> &[KeyEvent] {
        &self.meta.key_events
    }

    /// The pinned key of a device, under whatever subject it was pinned.
    pub fn pinned_key(&self, device_id: &str) -> Option<&PinnedKey> {
        self.meta.pins.get(device_id)
    }

    /// A subject's pinned devices `(device id, pin)`, sorted by device id.
    pub fn pinned_devices(&self, subject_id: &str) -> Vec<(String, PinnedKey)> {
        self.meta
            .pins
            .iter()
            .filter(|(_, pin)| pin.subject_id == subject_id)
            .map(|(device, pin)| (device.clone(), pin.clone()))
            .collect()
    }

    /// The safety number for this device's subject and `peer_subject_id`, from
    /// both subjects' pinned devices: 12 groups of 5 digits, the same on both
    /// sides when both pinned the same keys. Compare it out of band (in person,
    /// over a call). `Rejected` while no device of the peer is pinned.
    pub fn safety_number(&self, peer_subject_id: &str) -> Result<String, MlsError> {
        pins::safety_number(
            &self.meta.identity.tenant_id,
            &self.meta.pins,
            &self.meta.identity.subject_id,
            peer_subject_id,
        )
    }

    fn pin_of(&self, subject_id: &str, device_id: &str) -> Result<&PinnedKey, MlsError> {
        self.meta
            .pins
            .get(device_id)
            .filter(|pin| pin.subject_id == subject_id)
            .ok_or_else(|| {
                MlsError::Rejected(format!(
                    "no pinned key for device `{device_id}` of `{subject_id}`"
                ))
            })
    }

    /// Mark a pinned device verified after its owner read `fingerprint` (the
    /// directory's lowercase hex key fingerprint) out of band.
    /// [`MlsError::KeyChanged`] when it is not the pinned key's fingerprint.
    pub fn verify_device(
        &mut self,
        subject_id: &str,
        device_id: &str,
        fingerprint: &str,
    ) -> Result<(), MlsError> {
        let pinned = self.pin_of(subject_id, device_id)?.fingerprint();
        let given = pins::normalize_fingerprint(fingerprint);
        if given != pinned {
            return Err(MlsError::KeyChanged {
                subject_id: subject_id.to_string(),
                device_id: device_id.to_string(),
                pinned_fingerprint: pinned,
                observed_fingerprint: given,
            });
        }
        if let Some(pin) = self.meta.pins.get_mut(device_id) {
            pin.verified = true;
        }
        self.persist()
    }

    /// Re-pin a device to the pending key observed for it under `subject_id`,
    /// after the user compared that key's `fingerprint` with the device's owner.
    /// Only the compared key is accepted: a fingerprint that is not the pending
    /// key's is refused. The new pin is unverified.
    pub fn accept_key_change(
        &mut self,
        subject_id: &str,
        device_id: &str,
        fingerprint: &str,
    ) -> Result<(), MlsError> {
        if device_id == self.meta.identity.device_id {
            return Err(MlsError::Rejected(
                "this device's own key is never replaced".into(),
            ));
        }
        let given = pins::normalize_fingerprint(fingerprint);
        let now = (self.clock)();
        let pin =
            self.meta.pins.get_mut(device_id).ok_or_else(|| {
                MlsError::Rejected(format!("no pinned key for device `{device_id}`"))
            })?;
        let matches = pin.pending_subject_id.as_deref() == Some(subject_id)
            && pin.pending_fingerprint().as_deref() == Some(given.as_str());
        let pending = match pin.pending.take() {
            Some(pending) if matches => pending,
            other => {
                pin.pending = other;
                return Err(MlsError::Rejected(format!(
                    "no pending key with fingerprint {given} for device `{device_id}` of `{subject_id}`"
                )));
            }
        };
        *pin = PinnedKey {
            subject_id: subject_id.to_string(),
            signature_public_key: pending,
            first_seen_unix: pin.pending_seen_unix.unwrap_or(now),
            verified: false,
            pending: None,
            pending_subject_id: None,
            pending_seen_unix: None,
        };
        self.persist()
    }

    /// Drop a device's pin (a device that was revoked and will not return), so
    /// its id can be pinned afresh. `false` when nothing was pinned for it.
    pub fn forget_device(&mut self, subject_id: &str, device_id: &str) -> Result<bool, MlsError> {
        if device_id == self.meta.identity.device_id {
            return Err(MlsError::Rejected(
                "this device's own pin cannot be forgotten".into(),
            ));
        }
        if self.pin_of(subject_id, device_id).is_err() {
            return Ok(false);
        }
        self.meta.pins.remove(device_id);
        self.persist()?;
        Ok(true)
    }

    // ── key packages ───────────────────────────────────────────────────────

    /// `count` one-time KeyPackages for `PublishKeyPackages`. Their private keys
    /// stay on this device until a Welcome consumes them.
    pub fn key_packages(&mut self, count: usize) -> Result<Vec<Vec<u8>>, MlsError> {
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let bundle = KeyPackage::builder()
                .build(
                    CIPHERSUITE,
                    &self.provider,
                    &self.signer,
                    self.credential.clone(),
                )
                .map_err(mls("build a KeyPackage"))?;
            out.push(
                bundle
                    .key_package()
                    .tls_serialize_detached()
                    .map_err(mls("serialize a KeyPackage"))?,
            );
        }
        self.persist()?;
        Ok(out)
    }

    /// A last-resort KeyPackage (RFC 9420 `last_resort` extension): the broker
    /// hands it out only when no one-time package is left, and this device
    /// keeps its private key after a Welcome uses it.
    pub fn last_resort_key_package(&mut self) -> Result<Vec<u8>, MlsError> {
        let bundle = KeyPackage::builder()
            .leaf_node_capabilities(Capabilities::new(
                None,
                None,
                Some(&[ExtensionType::LastResort]),
                None,
                None,
            ))
            .mark_as_last_resort()
            .build(
                CIPHERSUITE,
                &self.provider,
                &self.signer,
                self.credential.clone(),
            )
            .map_err(mls("build the last-resort KeyPackage"))?;
        let bytes = bundle
            .key_package()
            .tls_serialize_detached()
            .map_err(mls("serialize the last-resort KeyPackage"))?;
        self.persist()?;
        Ok(bytes)
    }

    // ── groups ─────────────────────────────────────────────────────────────

    fn load_group(&self, group_id: &str) -> Result<MlsGroup, MlsError> {
        self.find_group(group_id)?
            .ok_or_else(|| MlsError::UnknownGroup(group_id.to_string()))
    }

    fn find_group(&self, group_id: &str) -> Result<Option<MlsGroup>, MlsError> {
        MlsGroup::load(self.provider.storage(), &group_id_of(group_id))
            .map_err(mls("load the group"))
    }

    /// Create a group with this device as its only member, at epoch 0.
    pub fn create_group(&mut self, group_id: &str) -> Result<GroupCreation, MlsError> {
        validate_group_id(group_id)?;
        if self.meta.groups.contains_key(group_id) || self.find_group(group_id)?.is_some() {
            return Err(MlsError::GroupExists(group_id.to_string()));
        }
        let group = MlsGroup::new_with_group_id(
            &self.provider,
            &self.signer,
            &group_config(),
            group_id_of(group_id),
            self.credential.clone(),
        )
        .map_err(mls("create the group"))?;
        let group_info = group
            .export_group_info(self.provider.crypto(), &self.signer, false)
            .map_err(mls("export the GroupInfo"))?
            .tls_serialize_detached()
            .map_err(mls("serialize the GroupInfo"))?;
        let ratchet_tree = group
            .export_ratchet_tree()
            .tls_serialize_detached()
            .map_err(mls("serialize the ratchet tree"))?;
        self.meta
            .groups
            .insert(group_id.to_string(), GroupMeta::default());
        self.persist()?;
        Ok(GroupCreation {
            group_id: group_id.to_string(),
            epoch: group.epoch().as_u64(),
            group_info,
            ratchet_tree,
        })
    }

    /// Delete this device's state for a group (after the broker refused its
    /// creation, or to leave local history behind).
    pub fn forget_group(&mut self, group_id: &str) -> Result<(), MlsError> {
        if let Some(mut group) = self.find_group(group_id)? {
            group
                .delete(self.provider.storage())
                .map_err(mls("delete the group"))?;
        }
        self.meta.groups.remove(group_id);
        self.persist()
    }

    /// Groups this device is a member of.
    pub fn group_ids(&self) -> Vec<String> {
        self.meta.groups.keys().cloned().collect()
    }

    /// The group's current epoch on this device.
    pub fn epoch(&self, group_id: &str) -> Result<u64, MlsError> {
        Ok(self.load_group(group_id)?.epoch().as_u64())
    }

    /// The group's members, in leaf order.
    pub fn members(&self, group_id: &str) -> Result<Vec<GroupMember>, MlsError> {
        let group = self.load_group(group_id)?;
        Ok(group
            .members()
            .filter_map(|member| {
                let (tenant_id, subject_id, device_id) = credential_identity(&member.credential)?;
                Some(GroupMember {
                    leaf_index: member.index.u32(),
                    tenant_id,
                    subject_id,
                    device_id,
                    signature_public_key: member.signature_key.clone(),
                })
            })
            .collect())
    }

    /// Whether a commit this device built (or an external join) awaits
    /// confirmation.
    pub fn has_pending_commit(&self, group_id: &str) -> bool {
        self.meta
            .groups
            .get(group_id)
            .is_some_and(|meta| meta.pending.is_some() || meta.awaiting_external_ack)
    }

    fn awaiting_external_ack(&self, group_id: &str) -> bool {
        self.meta
            .groups
            .get(group_id)
            .is_some_and(|meta| meta.awaiting_external_ack)
    }

    /// Build the commit through the commit builder, asking for the new epoch's
    /// GroupInfo explicitly; used when the convenience API returned none.
    fn commit_with_group_info(
        &self,
        group: &mut MlsGroup,
        adds: Vec<KeyPackage>,
        removals: Vec<LeafNodeIndex>,
        self_update: bool,
    ) -> Result<CommitMessages, MlsError> {
        group
            .clear_pending_commit(self.provider.storage())
            .map_err(mls("clear the commit to rebuild it"))?;
        let bundle = group
            .commit_builder()
            .propose_adds(adds)
            .propose_removals(removals)
            .force_self_update(self_update)
            .load_psks(self.provider.storage())
            .map_err(mls("load the commit's PSKs"))?
            .create_group_info(true)
            .build(
                self.provider.rand(),
                self.provider.crypto(),
                &self.signer,
                |_| true,
            )
            .map_err(mls("build the commit"))?
            .stage_commit(&self.provider)
            .map_err(mls("stage the commit"))?;
        let (commit, welcome, group_info) = bundle.into_messages();
        Ok((commit, welcome, group_info.map(MlsMessageOut::from)))
    }

    /// Build an external commit that joins `group_id` from its signed
    /// `group_info` (TLS-serialized `MlsMessage`, as `GetGroupInfo` serves it)
    /// and its public `ratchet_tree` (may be empty when the GroupInfo carries
    /// the tree). Every member's key is checked against the pins first: a
    /// changed key refuses the join ([`MlsError::KeyChanged`]). The group is
    /// held at the new epoch until [`Self::confirm_commit`] (the broker accepted
    /// the commit) or [`Self::discard_commit`] (it refused; the group is
    /// deleted). Not a [`CommitIntent`]: a lost race is retried by fetching the
    /// GroupInfo again.
    pub fn join_external(
        &mut self,
        group_id: &str,
        group_info: &[u8],
        ratchet_tree: &[u8],
    ) -> Result<ExternalJoin, MlsError> {
        validate_group_id(group_id)?;
        if self.meta.groups.contains_key(group_id) || self.find_group(group_id)?.is_some() {
            return Err(MlsError::GroupExists(group_id.to_string()));
        }
        let verifiable = match MlsMessageIn::tls_deserialize_exact(group_info)
            .map_err(rejected("group_info is not an MLS message"))?
            .extract()
        {
            MlsMessageBodyIn::GroupInfo(info) => info,
            _ => {
                return Err(MlsError::Rejected(
                    "group_info is not an MLS GroupInfo".into(),
                ))
            }
        };
        let mut builder =
            MlsGroup::external_commit_builder().with_config(group_config().join_config().clone());
        if !ratchet_tree.is_empty() {
            let tree = RatchetTreeIn::tls_deserialize_exact(ratchet_tree)
                .map_err(rejected("ratchet_tree is not an MLS ratchet tree"))?;
            builder = builder.with_ratchet_tree(tree);
        }
        // `finalize` returns the group already at the new epoch: an external
        // commit has nothing left to merge.
        let (mut group, bundle) = builder
            .build_group(&self.provider, verifiable, self.credential.clone())
            .map_err(mls("start the external commit"))?
            .load_psks(self.provider.storage())
            .map_err(mls("load the external commit's PSKs"))?
            .build(
                self.provider.rand(),
                self.provider.crypto(),
                &self.signer,
                |_| true,
            )
            .map_err(mls("build the external commit"))?
            .finalize(&self.provider)
            .map_err(mls("finalize the external commit"))?;
        if group.group_id().as_slice() != group_id.as_bytes() {
            group
                .delete(self.provider.storage())
                .map_err(mls("delete a mismatched group"))?;
            return Err(MlsError::Rejected(
                "the GroupInfo is for a different group".into(),
            ));
        }
        if let Some(changed) = self.observe_members(&group, KeySource::GroupInfo, group_id, None) {
            group
                .delete(self.provider.storage())
                .map_err(mls("delete the refused group"))?;
            self.persist()?;
            return Err(changed);
        }
        let (commit, _welcome, _info) = bundle.into_messages();
        let commit = commit
            .tls_serialize_detached()
            .map_err(mls("serialize the external commit"))?;
        // The joined group is already at the new epoch: its own GroupInfo (with
        // the ratchet tree) is the one the broker serves next.
        let new_group_info = group
            .export_group_info(self.provider.crypto(), &self.signer, true)
            .map_err(mls("export the GroupInfo"))?
            .tls_serialize_detached()
            .map_err(mls("serialize the GroupInfo"))?;
        let epoch = group.epoch().as_u64().saturating_sub(1);
        self.meta.groups.insert(
            group_id.to_string(),
            GroupMeta {
                awaiting_external_ack: true,
                ..GroupMeta::default()
            },
        );
        self.persist()?;
        Ok(ExternalJoin {
            group_id: group_id.to_string(),
            epoch,
            commit,
            group_info: Some(new_group_info),
        })
    }

    /// Build a `PublicMessage` commit for `intent` on the group's current epoch.
    /// `Ok(None)` when there is nothing to do (every device to add is already a
    /// member, or none of those to remove is). The commit stays pending until
    /// [`Self::confirm_commit`] or [`Self::discard_commit`].
    pub fn commit(
        &mut self,
        group_id: &str,
        intent: CommitIntent,
    ) -> Result<Option<PendingCommit>, MlsError> {
        let mut group = self.load_group(group_id)?;
        if group.pending_commit().is_some() || self.has_pending_commit(group_id) {
            return Err(MlsError::PendingCommit(group_id.to_string()));
        }
        let epoch = group.epoch().as_u64();
        let members = member_devices(&group);
        let built: (CommitMessages, Vec<String>, Vec<String>, CommitIntent) = match intent {
            CommitIntent::Add { key_packages } => {
                let known = pins::known_subjects(&self.meta.pins);
                let mut packages = Vec::new();
                let mut kept = Vec::new();
                let mut added: Vec<String> = Vec::new();
                for bytes in key_packages {
                    let (package, info) = parse_key_package(&bytes)?;
                    let device = info.identity.device_id;
                    if info.identity.tenant_id != self.meta.identity.tenant_id {
                        return Err(MlsError::Rejected(format!(
                            "KeyPackage for device `{device}` belongs to another tenant"
                        )));
                    }
                    let subject = info.identity.subject_id;
                    let key = info.signature_public_key;
                    let observed = self.observe_key(
                        &subject,
                        &device,
                        &key,
                        &known,
                        KeySource::KeyPackage,
                        None,
                        None,
                    );
                    if observed == Observation::Changed {
                        // Keep the pending key; build nothing.
                        self.persist()?;
                        return Err(pins::key_changed(&self.meta.pins, &subject, &device, &key));
                    }
                    if members.contains_key(&device) || added.contains(&device) {
                        continue;
                    }
                    packages.push(package);
                    kept.push(bytes);
                    added.push(device);
                }
                if packages.is_empty() {
                    // Keys pinned from the packages are kept.
                    self.persist()?;
                    return Ok(None);
                }
                let (commit, welcome, info) = group
                    .add_members(&self.provider, &self.signer, &packages)
                    .map_err(mls("add members"))?;
                let mut messages = (commit, Some(welcome), info.map(MlsMessageOut::from));
                if messages.2.is_none() {
                    messages =
                        self.commit_with_group_info(&mut group, packages, Vec::new(), false)?;
                }
                (
                    messages,
                    added,
                    Vec::new(),
                    CommitIntent::Add { key_packages: kept },
                )
            }
            CommitIntent::Remove { device_ids } => {
                if device_ids.contains(&self.meta.identity.device_id) {
                    return Err(MlsError::Rejected(
                        "a device cannot remove itself; another member commits its removal".into(),
                    ));
                }
                let mut removed = Vec::new();
                let mut leaves = Vec::new();
                for device in device_ids {
                    if let Some(leaf) = members.get(&device) {
                        if !removed.contains(&device) {
                            leaves.push(*leaf);
                            removed.push(device);
                        }
                    }
                }
                if leaves.is_empty() {
                    return Ok(None);
                }
                // A removal adds nobody, so there is no Welcome to route (the
                // broker refuses one on a commit that adds no members).
                let (commit, _welcome, info) = group
                    .remove_members(&self.provider, &self.signer, &leaves)
                    .map_err(mls("remove members"))?;
                let mut messages = (commit, None, info.map(MlsMessageOut::from));
                if messages.2.is_none() {
                    messages =
                        self.commit_with_group_info(&mut group, Vec::new(), leaves, false)?;
                }
                (
                    messages,
                    Vec::new(),
                    removed.clone(),
                    CommitIntent::Remove {
                        device_ids: removed,
                    },
                )
            }
            CommitIntent::Update => {
                let bundle = group
                    .self_update(&self.provider, &self.signer, LeafNodeParameters::default())
                    .map_err(mls("rotate the leaf keys"))?;
                let (commit, _welcome, info) = bundle.into_messages();
                let mut messages = (commit, None, info.map(MlsMessageOut::from));
                if messages.2.is_none() {
                    messages =
                        self.commit_with_group_info(&mut group, Vec::new(), Vec::new(), true)?;
                }
                (messages, Vec::new(), Vec::new(), CommitIntent::Update)
            }
        };
        let ((commit, welcome, group_info), added, removed, intent) = built;
        let commit = commit
            .tls_serialize_detached()
            .map_err(mls("serialize the commit"))?;
        let welcome = welcome
            .map(|welcome| welcome.tls_serialize_detached())
            .transpose()
            .map_err(mls("serialize the Welcome"))?;
        let group_info = group_info
            .map(|info| info.tls_serialize_detached())
            .transpose()
            .map_err(mls("serialize the GroupInfo"))?;
        let meta = self.meta.groups.entry(group_id.to_string()).or_default();
        meta.pending = Some(PendingMeta {
            epoch,
            intent: intent.clone(),
        });
        meta.lost = None;
        self.persist()?;
        Ok(Some(PendingCommit {
            group_id: group_id.to_string(),
            epoch,
            commit,
            welcome,
            group_info,
            added_device_ids: added,
            removed_device_ids: removed,
            intent,
        }))
    }

    /// The broker accepted the pending commit: merge it and enter the new epoch.
    /// For an external join ([`Self::join_external`]) the group is already at
    /// the new epoch and is simply kept.
    pub fn confirm_commit(&mut self, group_id: &str) -> Result<CommitSummary, MlsError> {
        let mut group = self.load_group(group_id)?;
        if self.awaiting_external_ack(group_id) {
            if let Some(meta) = self.meta.groups.get_mut(group_id) {
                meta.awaiting_external_ack = false;
            }
            self.persist()?;
            return Ok(CommitSummary {
                group_id: group_id.to_string(),
                epoch: group.epoch().as_u64(),
                members: member_devices(&group).into_keys().collect(),
            });
        }
        if group.pending_commit().is_none() {
            return Err(MlsError::NoPendingCommit(group_id.to_string()));
        }
        group
            .merge_pending_commit(&self.provider)
            .map_err(mls("merge the pending commit"))?;
        if let Some(meta) = self.meta.groups.get_mut(group_id) {
            meta.pending = None;
        }
        self.persist()?;
        Ok(CommitSummary {
            group_id: group_id.to_string(),
            epoch: group.epoch().as_u64(),
            members: member_devices(&group).into_keys().collect(),
        })
    }

    /// The broker definitively refused the pending commit (a stale epoch, a
    /// validation failure): drop it. Returns its intent — or the intent of a
    /// commit that already lost a race while the inbox was processed — so the
    /// caller can rebuild it on the current epoch with [`Self::commit`]. A
    /// refused external join deletes the group (`None`): join again from a
    /// fresh GroupInfo.
    pub fn discard_commit(&mut self, group_id: &str) -> Result<Option<CommitIntent>, MlsError> {
        if self.awaiting_external_ack(group_id) {
            self.forget_group(group_id)?;
            return Ok(None);
        }
        let mut group = self.load_group(group_id)?;
        group
            .clear_pending_commit(self.provider.storage())
            .map_err(mls("clear the pending commit"))?;
        let intent = self.meta.groups.get_mut(group_id).and_then(|meta| {
            meta.pending
                .take()
                .map(|pending| pending.intent)
                .or_else(|| meta.lost.take())
        });
        self.persist()?;
        Ok(intent)
    }

    // ── application messages ───────────────────────────────────────────────

    /// Seal `plaintext` as a `PrivateMessage` for the group's current epoch.
    /// Not allowed while a commit is pending (its epoch is about to end), nor
    /// while an external join awaits the broker (the epoch may never exist).
    pub fn encrypt(
        &mut self,
        group_id: &str,
        plaintext: &[u8],
    ) -> Result<OutgoingMessage, MlsError> {
        let mut group = self.load_group(group_id)?;
        if group.pending_commit().is_some() || self.awaiting_external_ack(group_id) {
            return Err(MlsError::PendingCommit(group_id.to_string()));
        }
        let ciphertext = group
            .create_message(&self.provider, &self.signer, plaintext)
            .map_err(mls("encrypt the message"))?
            .tls_serialize_detached()
            .map_err(mls("serialize the message"))?;
        // The sending ratchet advanced: save it BEFORE the ciphertext can leave.
        self.persist()?;
        Ok(OutgoingMessage {
            group_id: group_id.to_string(),
            epoch: group.epoch().as_u64(),
            ciphertext,
        })
    }

    // ── inbox ──────────────────────────────────────────────────────────────

    /// Process one inbox entry, in order. Entries at or below the cursor are
    /// skipped, so replaying a stream from an older cursor is harmless. The
    /// cursor and every state change are saved before this returns; acknowledge
    /// the entry to the broker only after that.
    pub fn process(&mut self, item: &InboxItem) -> Result<InboxEvent, MlsError> {
        if item.seq > 0 && item.seq <= self.meta.inbox_cursor {
            return Ok(InboxEvent::Skipped {
                seq: item.seq,
                reason: "already processed".into(),
            });
        }
        let event = match item.kind {
            InboxKind::Welcome => self.process_welcome(item)?,
            InboxKind::Commit => self.process_commit(item)?,
            InboxKind::Message => self.process_message(item)?,
            InboxKind::RemovalHint => removal_hint(item),
            InboxKind::Other => InboxEvent::Skipped {
                seq: item.seq,
                reason: "unknown inbox entry kind".into(),
            },
        };
        self.meta.inbox_cursor = self.meta.inbox_cursor.max(item.seq);
        self.persist()?;
        Ok(event)
    }

    fn own_device(&self) -> &str {
        &self.meta.identity.device_id
    }

    fn process_welcome(&mut self, item: &InboxItem) -> Result<InboxEvent, MlsError> {
        let undecryptable = |reason: String| InboxEvent::Undecryptable {
            seq: item.seq,
            group_id: item.group_id.clone(),
            reason,
        };
        if self.find_group(&item.group_id)?.is_some() {
            return Ok(InboxEvent::Skipped {
                seq: item.seq,
                reason: "this device already holds the group".into(),
            });
        }
        let welcome = match MlsMessageIn::tls_deserialize_exact(item.payload.as_slice()) {
            Ok(message) => match message.extract() {
                MlsMessageBodyIn::Welcome(welcome) => welcome,
                _ => return Ok(undecryptable("WELCOME entry is not a Welcome".into())),
            },
            Err(err) => return Ok(undecryptable(format!("not an MLS message: {err}"))),
        };
        let staged = match StagedWelcome::new_from_welcome(
            &self.provider,
            group_config().join_config(),
            welcome,
            None,
        ) {
            Ok(staged) => staged,
            Err(err) => return Ok(undecryptable(format!("Welcome did not open: {err:?}"))),
        };
        let mut group = match staged.into_group(&self.provider) {
            Ok(group) => group,
            Err(err) => return Ok(undecryptable(format!("joining failed: {err:?}"))),
        };
        if group.group_id().as_slice() != item.group_id.as_bytes() {
            group
                .delete(self.provider.storage())
                .map_err(mls("delete a mismatched group"))?;
            return Ok(undecryptable(
                "the Welcome is for a different group than the entry names".into(),
            ));
        }
        // The group is joined either way; a changed key is reported as an event.
        let _ = self.observe_members(&group, KeySource::Welcome, &item.group_id, Some(item.seq));
        self.meta
            .groups
            .insert(item.group_id.clone(), GroupMeta::default());
        Ok(InboxEvent::Joined {
            seq: item.seq,
            group_id: item.group_id.clone(),
            epoch: group.epoch().as_u64(),
            members: member_devices(&group).into_keys().collect(),
            added_by: item.sender_device_id.clone(),
        })
    }

    fn process_commit(&mut self, item: &InboxItem) -> Result<InboxEvent, MlsError> {
        let undecryptable = |reason: String| InboxEvent::Undecryptable {
            seq: item.seq,
            group_id: item.group_id.clone(),
            reason,
        };
        let Some(mut group) = self.find_group(&item.group_id)? else {
            return Ok(InboxEvent::Skipped {
                seq: item.seq,
                reason: "this device holds no state for the group".into(),
            });
        };
        let current = group.epoch().as_u64();

        if item.sender_device_id == self.own_device() {
            // Our own commit, echoed. Normally it was confirmed already; if the
            // SubmitCommit response was lost it is still pending — the broker
            // accepted it, so merge it now.
            // An external join's echo (built on the epoch before the one the
            // joined group is at) confirms it the same way.
            let joined = self.awaiting_external_ack(&item.group_id)
                && item.epoch.checked_add(1) == Some(current);
            let merged = joined || (group.pending_commit().is_some() && current == item.epoch);
            if joined {
                if let Some(meta) = self.meta.groups.get_mut(&item.group_id) {
                    meta.awaiting_external_ack = false;
                }
            } else if merged {
                group
                    .merge_pending_commit(&self.provider)
                    .map_err(mls("merge the pending commit"))?;
                if let Some(meta) = self.meta.groups.get_mut(&item.group_id) {
                    meta.pending = None;
                }
            }
            return Ok(InboxEvent::OwnCommit {
                seq: item.seq,
                group_id: item.group_id.clone(),
                epoch: group.epoch().as_u64(),
                merged,
            });
        }

        if item.epoch < current {
            return Ok(InboxEvent::Skipped {
                seq: item.seq,
                reason: format!(
                    "commit built on epoch {} but the group is at epoch {current}",
                    item.epoch
                ),
            });
        }
        if item.epoch > current {
            return Ok(undecryptable(format!(
                "commits are missing: the group is at epoch {current}, this commit was built on {}",
                item.epoch
            )));
        }
        let message = match protocol_message(&item.payload) {
            Ok(message) => message,
            Err(reason) => return Ok(undecryptable(reason)),
        };
        // Another member's commit for our epoch won: our pending commit (if
        // any) lost the race. Keep its intent for a rebuild.
        if group.pending_commit().is_some() {
            group
                .clear_pending_commit(self.provider.storage())
                .map_err(mls("clear the lost commit"))?;
        }
        if let Some(meta) = self.meta.groups.get_mut(&item.group_id) {
            if let Some(pending) = meta.pending.take() {
                meta.lost = Some(pending.intent);
            }
        }
        let before = member_devices(&group);
        let processed = match group.process_message(&self.provider, message) {
            Ok(processed) => processed,
            Err(err) => return Ok(undecryptable(format!("commit did not apply: {err:?}"))),
        };
        let sender = credential_identity(processed.credential())
            .map(|(_, _, device)| device)
            .unwrap_or_else(|| item.sender_device_id.clone());
        let staged = match processed.into_content() {
            ProcessedMessageContent::StagedCommitMessage(staged) => staged,
            _ => return Ok(undecryptable("COMMIT entry is not a commit".into())),
        };
        let self_removed = staged.self_removed();
        group
            .merge_staged_commit(&self.provider, *staged)
            .map_err(mls("merge the commit"))?;
        if self_removed || !group.is_active() {
            group
                .delete(self.provider.storage())
                .map_err(mls("delete the group"))?;
            self.meta.groups.remove(&item.group_id);
            return Ok(InboxEvent::Removed {
                seq: item.seq,
                group_id: item.group_id.clone(),
                removed_by: sender,
            });
        }
        // Every leaf, not only the added device ids: a removal and re-add, or an
        // update path, can change a leaf's key under the same device id.
        let _ = self.observe_members(&group, KeySource::Commit, &item.group_id, Some(item.seq));
        let after = member_devices(&group);
        let before: BTreeSet<&String> = before.keys().collect();
        let after_ids: BTreeSet<&String> = after.keys().collect();
        Ok(InboxEvent::Commit {
            seq: item.seq,
            group_id: item.group_id.clone(),
            epoch: group.epoch().as_u64(),
            sender_device_id: sender,
            added: after_ids
                .difference(&before)
                .map(|d| (*d).clone())
                .collect(),
            removed: before
                .difference(&after_ids)
                .map(|d| (*d).clone())
                .collect(),
        })
    }

    fn process_message(&mut self, item: &InboxItem) -> Result<InboxEvent, MlsError> {
        let undecryptable = |reason: String| InboxEvent::Undecryptable {
            seq: item.seq,
            group_id: item.group_id.clone(),
            reason,
        };
        if item.sender_device_id == self.own_device() {
            // A sender cannot decrypt its own PrivateMessage (its ratchet moved
            // on); the caller already holds the plaintext.
            return Ok(InboxEvent::Skipped {
                seq: item.seq,
                reason: "own message".into(),
            });
        }
        let Some(mut group) = self.find_group(&item.group_id)? else {
            return Ok(undecryptable(
                "this device is not a member of the group".into(),
            ));
        };
        let message = match protocol_message(&item.payload) {
            Ok(message) => message,
            Err(reason) => return Ok(undecryptable(reason)),
        };
        let processed = match group.process_message(&self.provider, message) {
            Ok(processed) => processed,
            Err(err) => return Ok(undecryptable(format!("message did not decrypt: {err:?}"))),
        };
        let epoch = processed.epoch().as_u64();
        let (_tenant, subject, device) =
            credential_identity(processed.credential()).unwrap_or_default();
        match processed.into_content() {
            ProcessedMessageContent::ApplicationMessage(app) => Ok(InboxEvent::Message {
                seq: item.seq,
                group_id: item.group_id.clone(),
                epoch,
                sender_device_id: device,
                sender_subject_id: subject,
                client_message_id: item.client_message_id.clone(),
                plaintext: app.into_bytes(),
            }),
            _ => Ok(undecryptable(
                "MESSAGE entry is not an application message".into(),
            )),
        }
    }
}

fn removal_hint(item: &InboxItem) -> InboxEvent {
    let hint: serde_json::Value = serde_json::from_slice(&item.payload).unwrap_or_default();
    let text = |key: &str| {
        hint.get(key)
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let device_id = match text("device_id") {
        device if !device.is_empty() => device,
        _ => item.sender_device_id.clone(),
    };
    if validate_device_id(&device_id).is_err() {
        return InboxEvent::Skipped {
            seq: item.seq,
            reason: "removal hint names no device".into(),
        };
    }
    InboxEvent::RemovalHint {
        seq: item.seq,
        group_id: item.group_id.clone(),
        device_id,
        reason: text("reason"),
    }
}
