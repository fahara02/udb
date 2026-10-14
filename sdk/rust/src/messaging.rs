//! End-to-end encrypted messaging over UDB's `MessagingService` (feature `mls`).
//!
//! [`MessagingClient`] joins the generated `MessagingService` tonic client to a
//! device's [`udb_mls::MlsClient`]: the broker is the MLS Delivery Service and
//! key directory, this device does all cryptography. Plaintext and secret keys
//! never leave the process except to the caller; the broker receives only
//! KeyPackages, `PublicMessage` commits, Welcomes, and `PrivateMessage`
//! ciphertext.
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use udb_client::messaging::{MessagingClient, mls};
//! use udb_client::Metadata;
//!
//! let meta = Metadata::new("tenant-1").with_bearer_token(std::env::var("UDB_TOKEN")?);
//! let device = mls::MlsClient::open_or_create(
//!     mls::DeviceIdentity::new("tenant-1", "alice", "alice-phone")?,
//!     mls::FileStore::new("alice-phone.mls"),
//! )?;
//! // MessagingService is a native service: use the native listener's address.
//! let mut chat = MessagingClient::connect("http://127.0.0.1:50061", meta, device).await?;
//! chat.register_device().await?;
//! chat.ensure_key_packages().await?;
//! chat.create_group("room-1").await?;
//! chat.add_members("room-1", &["bob"], &[]).await?;
//! chat.send("room-1", b"hello bob").await?;
//! while let Some(event) = chat.next_event().await? {
//!     if let mls::InboxEvent::Message { sender_device_id, plaintext, .. } = event {
//!         println!("{sender_device_id}: {}", String::from_utf8_lossy(&plaintext));
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::hash::{BuildHasher as _, Hasher as _};
use std::time::Duration;

use tonic::codec::Streaming;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

use crate::error::UdbError;
use crate::metadata::Metadata;
use crate::proto::udb::core::messaging::services::v1 as pb;
use crate::proto::udb::core::messaging::services::v1::messaging_service_client::MessagingServiceClient;

/// The client-side MLS crate this module drives, re-exported so a consumer
/// needs no direct dependency on it.
pub use udb_mls as mls;

use mls::{CommitIntent, InboxEvent, InboxItem, InboxKind, MlsClient, MlsError, StateStore};

/// How many times a commit is rebuilt after losing an epoch race, and a send
/// re-sealed after the group moved on, before giving up.
const MAX_REBASES: u32 = 5;
/// How many times a mutation is re-sent with the SAME idempotency key after a
/// transport failure (the broker collapses the duplicate).
const MAX_TRANSPORT_RETRIES: u32 = 3;

/// A messaging failure: a broker refusal, a local MLS failure, or a protocol
/// violation (the broker answered something this client cannot reconcile).
// `Rpc` carries the same unboxed `UdbError` every other client call returns
// (see the crate-level `result_large_err` note); boxing it here would make this
// one error type match differently from the rest of the SDK.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum MessagingError {
    Rpc(UdbError),
    Mls(MlsError),
    Protocol(String),
}

impl std::fmt::Display for MessagingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rpc(err) => write!(f, "messaging RPC failed: {err}"),
            Self::Mls(err) => write!(f, "messaging MLS failure: {err}"),
            Self::Protocol(message) => write!(f, "messaging protocol error: {message}"),
        }
    }
}

impl std::error::Error for MessagingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rpc(err) => Some(err),
            Self::Mls(err) => Some(err),
            Self::Protocol(_) => None,
        }
    }
}

impl From<UdbError> for MessagingError {
    fn from(err: UdbError) -> Self {
        Self::Rpc(err)
    }
}

impl From<tonic::Status> for MessagingError {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(UdbError::from_status(status))
    }
}

impl From<MlsError> for MessagingError {
    fn from(err: MlsError) -> Self {
        Self::Mls(err)
    }
}

/// The group epoch a stale-epoch refusal reports (`FAILED_PRECONDITION` with
/// `ErrorDetail.missing.current_epoch`), if this is one.
pub fn stale_epoch(err: &UdbError) -> Option<u64> {
    if err.code() != Code::FailedPrecondition {
        return None;
    }
    err.detail
        .as_ref()?
        .missing
        .get("current_epoch")?
        .parse()
        .ok()
}

fn transient(err: &UdbError) -> bool {
    matches!(
        err.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Aborted
    )
}

/// The result of an accepted commit.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CommitOutcome {
    /// The group's epoch after the commit (unchanged when nothing was to do).
    pub epoch: u64,
    pub added_device_ids: Vec<String>,
    pub removed_device_ids: Vec<String>,
    /// Devices that had no KeyPackage to claim (or are not active); they were
    /// not added.
    pub missing_device_ids: Vec<String>,
    /// How many times the commit was rebuilt because a concurrent commit won.
    pub rebases: u32,
    /// False when there was nothing to commit (every target was already in, or
    /// already out).
    pub committed: bool,
}

/// The device's one-time KeyPackage stock after [`MessagingClient::ensure_key_packages`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeyPackageStock {
    pub one_time_remaining: i64,
    pub has_last_resort: bool,
    /// Packages published by this call.
    pub published: usize,
}

/// One device's messaging session: the generated `MessagingService` client
/// plus that device's MLS state.
///
/// Every inbox entry is processed — and the device state saved — before it is
/// acknowledged, so a crash at any point replays at most entries the cursor
/// then skips.
pub struct MessagingClient<S: StateStore> {
    rpc: MessagingServiceClient<Channel>,
    meta: Metadata,
    mls: MlsClient<S>,
    backlog: VecDeque<InboxEvent>,
    follow: Option<Streaming<pb::StreamInboxResponse>>,
    acked: i64,
    key_package_target: usize,
    key_package_threshold: usize,
    backup_params: mls::BackupParams,
    ids: u64,
}

impl<S: StateStore> std::fmt::Debug for MessagingClient<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessagingClient")
            .field("mls", &self.mls)
            .field("backlog", &self.backlog.len())
            .field("following", &self.follow.is_some())
            .finish_non_exhaustive()
    }
}

impl<S: StateStore> MessagingClient<S> {
    /// Connect to the listener that serves `MessagingService` (the native
    /// control-plane listener, not the DataBroker one).
    pub async fn connect(
        endpoint: impl Into<String>,
        meta: Metadata,
        mls: MlsClient<S>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self::with_channel(channel, meta, mls))
    }

    pub fn with_channel(channel: Channel, meta: Metadata, mls: MlsClient<S>) -> Self {
        Self {
            rpc: MessagingServiceClient::new(channel),
            meta,
            mls,
            backlog: VecDeque::new(),
            follow: None,
            acked: 0,
            key_package_target: 20,
            key_package_threshold: 5,
            backup_params: mls::BackupParams::default(),
            ids: 0,
        }
    }

    /// Keep `target` one-time KeyPackages published, topping up when the stock
    /// falls below `threshold`.
    pub fn with_key_package_stock(mut self, target: usize, threshold: usize) -> Self {
        self.key_package_target = target.max(1);
        self.key_package_threshold = threshold.min(self.key_package_target);
        self
    }

    /// The Argon2id costs [`Self::backup`] derives the backup key with (default
    /// [`mls::BackupParams::RECOMMENDED`]).
    pub fn with_backup_params(mut self, params: mls::BackupParams) -> Self {
        self.backup_params = params;
        self
    }

    /// Replace the bearer token (after a refresh).
    pub fn set_bearer_token(&mut self, token: impl Into<String>) {
        self.meta.bearer_token = token.into();
    }

    pub fn metadata(&self) -> &Metadata {
        &self.meta
    }

    /// This device's MLS state (groups, epochs, members, fingerprints).
    pub fn mls(&self) -> &MlsClient<S> {
        &self.mls
    }

    pub fn mls_mut(&mut self) -> &mut MlsClient<S> {
        &mut self.mls
    }

    pub fn into_mls(self) -> MlsClient<S> {
        self.mls
    }

    fn tenant(&self) -> String {
        self.mls.identity().tenant_id.clone()
    }

    fn device(&self) -> String {
        self.mls.identity().device_id.clone()
    }

    /// A fresh identifier for request ids and idempotency keys: time, a
    /// per-client counter, and per-process random bits.
    fn fresh_id(&mut self, prefix: &str) -> String {
        self.ids += 1;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or_default();
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        hasher.write_u64(nanos ^ self.ids);
        format!("{prefix}-{nanos:x}-{:x}-{:x}", self.ids, hasher.finish())
    }

    fn request<T>(&mut self, message: T) -> Result<Request<T>, MessagingError> {
        let request_id = self.fresh_id("req");
        let mut request = Request::new(message);
        self.meta.apply(&mut request)?;
        // Native RPCs require a request context.
        if let Ok(value) = request_id.parse() {
            request.metadata_mut().insert("x-request-id", value);
        }
        Ok(request)
    }

    // ── device directory ───────────────────────────────────────────────────

    /// Register this device's signature key and credential in the directory.
    /// Idempotent for the same key.
    pub async fn register_device(&mut self) -> Result<pb::DirectoryDevice, MessagingError> {
        let registration = self.mls.registration()?;
        let request = self.request(pb::RegisterMessagingDeviceRequest {
            tenant_id: registration.tenant_id,
            subject_id: registration.subject_id,
            device_id: registration.device_id,
            signature_public_key: registration.signature_public_key,
            credential: registration.credential,
        })?;
        let response = self
            .rpc
            .register_messaging_device(request)
            .await?
            .into_inner();
        let device = response
            .device
            .ok_or_else(|| MessagingError::Protocol("registration returned no device".into()))?;
        if device.key_fingerprint != registration.key_fingerprint {
            return Err(MessagingError::Protocol(format!(
                "the directory holds key {} for this device, not this device's key {}",
                device.key_fingerprint, registration.key_fingerprint
            )));
        }
        Ok(device)
    }

    /// A principal's devices with their key fingerprints (empty subject: this
    /// device's own principal). Compare fingerprints out of band to verify.
    pub async fn list_devices(
        &mut self,
        subject_id: &str,
        include_revoked: bool,
    ) -> Result<Vec<pb::DirectoryDevice>, MessagingError> {
        let request = self.request(pb::ListMessagingDevicesRequest {
            tenant_id: self.tenant(),
            subject_id: subject_id.to_string(),
            include_revoked,
        })?;
        Ok(self
            .rpc
            .list_messaging_devices(request)
            .await?
            .into_inner()
            .devices)
    }

    /// Revoke one of the principal's devices. Its groups' other members get a
    /// removal hint ([`InboxEvent::RemovalHint`]); commit the removal with
    /// [`Self::remove_devices`].
    pub async fn revoke_device(
        &mut self,
        device_id: &str,
        reason: &str,
    ) -> Result<pb::RevokeMessagingDeviceResponse, MessagingError> {
        let request = self.request(pb::RevokeMessagingDeviceRequest {
            tenant_id: self.tenant(),
            device_id: device_id.to_string(),
            reason: reason.to_string(),
        })?;
        Ok(self
            .rpc
            .revoke_messaging_device(request)
            .await?
            .into_inner())
    }

    // ── key packages ───────────────────────────────────────────────────────

    /// Top the one-time KeyPackage stock up to the target when it is below the
    /// threshold, and publish a last-resort package if the broker has none.
    pub async fn ensure_key_packages(&mut self) -> Result<KeyPackageStock, MessagingError> {
        let request = self.request(pb::GetKeyPackageStockRequest {
            tenant_id: self.tenant(),
            device_id: self.device(),
        })?;
        let stock = self.rpc.get_key_package_stock(request).await?.into_inner();
        let remaining = usize::try_from(stock.one_time_remaining.max(0)).unwrap_or(usize::MAX);
        let top_up = if remaining < self.key_package_threshold {
            self.key_package_target - remaining
        } else {
            0
        };
        if top_up == 0 && stock.has_last_resort {
            return Ok(KeyPackageStock {
                one_time_remaining: stock.one_time_remaining,
                has_last_resort: true,
                published: 0,
            });
        }
        let key_packages = self.mls.key_packages(top_up)?;
        let last_resort_key_package = if stock.has_last_resort {
            Vec::new()
        } else {
            self.mls.last_resort_key_package()?
        };
        let request = self.request(pb::PublishKeyPackagesRequest {
            tenant_id: self.tenant(),
            device_id: self.device(),
            key_packages,
            last_resort_key_package,
        })?;
        let published = self.rpc.publish_key_packages(request).await?.into_inner();
        Ok(KeyPackageStock {
            one_time_remaining: published.one_time_remaining,
            has_last_resort: published.has_last_resort,
            published: usize::try_from(published.accepted.max(0)).unwrap_or_default(),
        })
    }

    // ── groups ─────────────────────────────────────────────────────────────

    /// Create a group with this device as its only member. Returns its epoch.
    pub async fn create_group(&mut self, group_id: &str) -> Result<u64, MessagingError> {
        let creation = self.mls.create_group(group_id)?;
        let request = self.request(pb::CreateGroupRequest {
            tenant_id: self.tenant(),
            group_id: group_id.to_string(),
            creator_device_id: self.device(),
            group_info: creation.group_info,
            ratchet_tree: creation.ratchet_tree,
        })?;
        match self.rpc.create_group(request).await {
            Ok(response) => Ok(u64::try_from(response.into_inner().epoch).unwrap_or_default()),
            Err(status) => {
                // The broker did not take the group: drop the local state too.
                self.mls.forget_group(group_id)?;
                Err(status.into())
            }
        }
    }

    /// Add every active device of `subject_ids` plus `device_ids`, claiming one
    /// KeyPackage per device. Devices already in the group (and this device)
    /// are skipped; devices without a package are reported missing.
    pub async fn add_members(
        &mut self,
        group_id: &str,
        subject_ids: &[&str],
        device_ids: &[&str],
    ) -> Result<CommitOutcome, MessagingError> {
        let members: Vec<String> = self
            .mls
            .members(group_id)?
            .into_iter()
            .map(|member| member.device_id)
            .collect();
        let mut targets: Vec<String> = device_ids.iter().map(|d| d.to_string()).collect();
        for subject in subject_ids {
            for device in self.list_devices(subject, false).await? {
                if device.status == "ACTIVE" {
                    targets.push(device.device_id);
                }
            }
        }
        let own = self.device();
        targets.retain(|device| device != &own && !members.contains(device));
        targets.sort();
        targets.dedup();
        if targets.is_empty() {
            return Ok(CommitOutcome {
                epoch: self.mls.epoch(group_id)?,
                ..CommitOutcome::default()
            });
        }
        let request = self.request(pb::ClaimKeyPackagesRequest {
            tenant_id: self.tenant(),
            requester_device_id: own,
            target_device_ids: targets,
            target_subject_ids: Vec::new(),
        })?;
        let claimed = self.rpc.claim_key_packages(request).await?.into_inner();
        let tenant = self.tenant();
        let mut key_packages = Vec::with_capacity(claimed.key_packages.len());
        for package in claimed.key_packages {
            // The package must name exactly the device it was claimed for.
            let info = mls::inspect_key_package(&package.key_package)?;
            if info.identity.tenant_id != tenant
                || info.identity.subject_id != package.subject_id
                || info.identity.device_id != package.device_id
            {
                return Err(MessagingError::Protocol(format!(
                    "the KeyPackage claimed for device `{}` names `{}`",
                    package.device_id,
                    info.identity.credential_identity()
                )));
            }
            key_packages.push(package.key_package);
        }
        let mut outcome = self
            .submit(group_id, CommitIntent::Add { key_packages })
            .await?;
        outcome.missing_device_ids = claimed.missing_device_ids;
        Ok(outcome)
    }

    /// Remove member devices (for example after a [`InboxEvent::RemovalHint`]).
    pub async fn remove_devices(
        &mut self,
        group_id: &str,
        device_ids: &[&str],
    ) -> Result<CommitOutcome, MessagingError> {
        let device_ids = device_ids.iter().map(|d| d.to_string()).collect();
        self.submit(group_id, CommitIntent::Remove { device_ids })
            .await
    }

    /// Rotate this device's leaf keys in the group (post-compromise security).
    pub async fn update_keys(&mut self, group_id: &str) -> Result<CommitOutcome, MessagingError> {
        self.submit(group_id, CommitIntent::Update).await
    }

    /// Build, submit and resolve a commit. A stale epoch (another member's
    /// commit won) is re-based: the commit is dropped, the inbox is processed
    /// up to the winning epoch, and the same intent is rebuilt and resubmitted.
    /// A transport failure is retried with the same idempotency key; a definite
    /// refusal discards the commit.
    async fn submit(
        &mut self,
        group_id: &str,
        mut intent: CommitIntent,
    ) -> Result<CommitOutcome, MessagingError> {
        let mut rebases = 0;
        loop {
            let Some(pending) = self.mls.commit(group_id, intent.clone())? else {
                return Ok(CommitOutcome {
                    epoch: self.mls.epoch(group_id)?,
                    rebases,
                    ..CommitOutcome::default()
                });
            };
            let client_message_id = self.fresh_id("commit");
            let mut transport_retries = 0;
            let result = loop {
                let request = self.request(pb::SubmitCommitRequest {
                    tenant_id: self.tenant(),
                    group_id: group_id.to_string(),
                    sender_device_id: self.device(),
                    epoch: pending.epoch as i64,
                    commit: pending.commit.clone(),
                    welcome: pending.welcome.clone().unwrap_or_default(),
                    group_info: Vec::new(),
                    client_message_id: client_message_id.clone(),
                })?;
                match self.rpc.submit_commit(request).await {
                    Ok(response) => break Ok(response.into_inner()),
                    Err(status) => {
                        let err = UdbError::from_status(status);
                        if transient(&err) && transport_retries < MAX_TRANSPORT_RETRIES {
                            transport_retries += 1;
                            tokio::time::sleep(Duration::from_millis(
                                100 * u64::from(transport_retries),
                            ))
                            .await;
                            continue;
                        }
                        break Err(err);
                    }
                }
            };
            match result {
                Ok(_response) => {
                    // A `duplicate` response means an earlier attempt landed; its
                    // inbox echo may already have merged the commit.
                    let epoch = if self.mls.has_pending_commit(group_id) {
                        self.mls.confirm_commit(group_id)?.epoch
                    } else {
                        self.mls.epoch(group_id)?
                    };
                    return Ok(CommitOutcome {
                        epoch,
                        added_device_ids: pending.added_device_ids,
                        removed_device_ids: pending.removed_device_ids,
                        missing_device_ids: Vec::new(),
                        rebases,
                        committed: true,
                    });
                }
                Err(err) => {
                    if let Some(current) = stale_epoch(&err) {
                        if rebases >= MAX_REBASES {
                            self.mls.discard_commit(group_id)?;
                            return Err(err.into());
                        }
                        rebases += 1;
                        if let Some(kept) = self.mls.discard_commit(group_id)? {
                            intent = kept;
                        }
                        self.catch_up(group_id, current).await?;
                        continue;
                    }
                    if !transient(&err) {
                        // A definite refusal: the commit never lands.
                        self.mls.discard_commit(group_id)?;
                    }
                    // A transport failure that outlived the retries leaves the
                    // commit pending: its inbox echo merges it if it landed;
                    // otherwise `discard_pending_commit` drops it.
                    return Err(err.into());
                }
            }
        }
    }

    /// Drop a commit left pending by a transport failure, after making sure
    /// (through the inbox) that it did not land.
    pub async fn discard_pending_commit(&mut self, group_id: &str) -> Result<(), MessagingError> {
        self.sync_into_backlog().await?;
        if self.mls.has_pending_commit(group_id) {
            self.mls.discard_commit(group_id)?;
        }
        Ok(())
    }

    // ── encrypted backup ───────────────────────────────────────────────────

    /// Seal this device's state and the application's `history` under a key
    /// derived from `recovery_secret` and store the envelope as the principal's
    /// backup `backup_id` (empty: `"default"`), replacing any earlier one. The
    /// broker stores only the envelope; the secret never leaves this process.
    /// Back up after processing the inbox: a restore resumes from the cursor
    /// saved here.
    pub async fn backup(
        &mut self,
        recovery_secret: &[u8],
        history: &[u8],
        backup_id: &str,
    ) -> Result<pb::MessagingBackupInfo, MessagingError> {
        let envelope = self
            .mls
            .export_backup_with(recovery_secret, history, self.backup_params)?;
        let request = self.request(pb::PutMessagingBackupRequest {
            tenant_id: self.tenant(),
            subject_id: self.mls.identity().subject_id.clone(),
            backup_id: backup_id.to_string(),
            ciphertext: envelope,
            expected_version: 0,
        })?;
        self.rpc
            .put_messaging_backup(request)
            .await?
            .into_inner()
            .backup
            .ok_or_else(|| MessagingError::Protocol("the backup write returned no backup".into()))
    }

    /// Erase the principal's backup `backup_id` (empty: `"default"`). `false`
    /// when none was stored.
    pub async fn delete_backup(&mut self, backup_id: &str) -> Result<bool, MessagingError> {
        let request = self.request(pb::DeleteMessagingBackupRequest {
            tenant_id: self.tenant(),
            subject_id: self.mls.identity().subject_id.clone(),
            backup_id: backup_id.to_string(),
        })?;
        Ok(self
            .rpc
            .delete_messaging_backup(request)
            .await?
            .into_inner()
            .deleted)
    }

    /// Restore a device on a new machine: fetch `subject_id`'s backup
    /// `backup_id` (empty: the caller, `"default"`), open it with
    /// `recovery_secret`, and load the device into the EMPTY `store`. Returns the
    /// client (the backed-up device, ready to [`Self::sync`] from the backup's
    /// cursor) and the history the backup carried.
    pub async fn restore(
        channel: Channel,
        meta: Metadata,
        subject_id: &str,
        backup_id: &str,
        recovery_secret: &[u8],
        store: S,
    ) -> Result<(Self, Vec<u8>), MessagingError> {
        let mut rpc = MessagingServiceClient::new(channel.clone());
        let mut request = Request::new(pb::GetMessagingBackupRequest {
            tenant_id: meta.tenant_id.clone(),
            subject_id: subject_id.to_string(),
            backup_id: backup_id.to_string(),
        });
        meta.apply(&mut request)?;
        if let Ok(value) = "messaging-restore".parse() {
            request.metadata_mut().insert("x-request-id", value);
        }
        let stored = rpc.get_messaging_backup(request).await?.into_inner();
        let (mls, history) = MlsClient::restore_backup(recovery_secret, &stored.ciphertext, store)?;
        if mls.identity().tenant_id != meta.tenant_id {
            return Err(MessagingError::Protocol(format!(
                "the backup is of tenant {}, not {}",
                mls.identity().tenant_id,
                meta.tenant_id
            )));
        }
        Ok((Self::with_channel(channel, meta, mls), history))
    }

    // ── messages ───────────────────────────────────────────────────────────

    /// Encrypt and send an application message to the group. If the group moved
    /// to a new epoch first, the inbox is processed and the message re-sealed.
    pub async fn send(
        &mut self,
        group_id: &str,
        plaintext: &[u8],
    ) -> Result<pb::SendMessageResponse, MessagingError> {
        let client_message_id = self.fresh_id("msg");
        let mut rebases = 0;
        loop {
            let sealed = self.mls.encrypt(group_id, plaintext)?;
            let mut transport_retries = 0;
            let result = loop {
                let request = self.request(pb::SendMessageRequest {
                    tenant_id: self.tenant(),
                    group_id: group_id.to_string(),
                    sender_device_id: self.device(),
                    ciphertext: sealed.ciphertext.clone(),
                    client_message_id: client_message_id.clone(),
                })?;
                match self.rpc.send_message(request).await {
                    Ok(response) => break Ok(response.into_inner()),
                    Err(status) => {
                        let err = UdbError::from_status(status);
                        if transient(&err) && transport_retries < MAX_TRANSPORT_RETRIES {
                            transport_retries += 1;
                            tokio::time::sleep(Duration::from_millis(
                                100 * u64::from(transport_retries),
                            ))
                            .await;
                            continue;
                        }
                        break Err(err);
                    }
                }
            };
            match result {
                Ok(response) => return Ok(response),
                Err(err) => match stale_epoch(&err) {
                    Some(current) if rebases < MAX_REBASES => {
                        rebases += 1;
                        self.catch_up(group_id, current).await?;
                    }
                    _ => return Err(err.into()),
                },
            }
        }
    }

    // ── inbox ──────────────────────────────────────────────────────────────

    async fn open_inbox(
        &mut self,
        follow: bool,
    ) -> Result<Streaming<pb::StreamInboxResponse>, MessagingError> {
        let request = self.request(pb::StreamInboxRequest {
            tenant_id: self.tenant(),
            device_id: self.device(),
            after_seq: self.mls.inbox_cursor(),
            follow,
            max_entries: 0,
        })?;
        Ok(self.rpc.stream_inbox(request).await?.into_inner())
    }

    fn item_of(entry: pb::InboxEntry) -> InboxItem {
        InboxItem {
            seq: entry.seq,
            kind: InboxKind::parse(&entry.kind),
            group_id: entry.group_id,
            epoch: u64::try_from(entry.epoch).unwrap_or_default(),
            payload: entry.payload,
            sender_device_id: entry.sender_device_id,
            client_message_id: entry.client_message_id,
        }
    }

    /// Process one delivered entry; the event, unless it needs no action.
    fn handle(
        &mut self,
        response: pb::StreamInboxResponse,
    ) -> Result<Option<InboxEvent>, MessagingError> {
        if let Some(error) = response.error {
            return Err(MessagingError::Protocol(format!(
                "inbox stream reported an error: {}",
                error.message
            )));
        }
        let Some(entry) = response.entry else {
            return Ok(None);
        };
        let event = self.mls.process(&Self::item_of(entry))?;
        Ok((!matches!(event, InboxEvent::Skipped { .. })).then_some(event))
    }

    /// Acknowledge every processed entry to the broker.
    pub async fn ack(&mut self) -> Result<(), MessagingError> {
        let cursor = self.mls.inbox_cursor();
        if cursor <= self.acked {
            return Ok(());
        }
        let request = self.request(pb::AckInboxRequest {
            tenant_id: self.tenant(),
            device_id: self.device(),
            up_to_seq: cursor,
        })?;
        let response = self.rpc.ack_inbox(request).await?.into_inner();
        self.acked = response.acked_seq.max(cursor);
        Ok(())
    }

    /// Drain the inbox (without waiting for new entries) into the backlog.
    async fn sync_into_backlog(&mut self) -> Result<(), MessagingError> {
        let mut stream = self.open_inbox(false).await?;
        while let Some(response) = stream.message().await? {
            if let Some(event) = self.handle(response)? {
                self.backlog.push_back(event);
            }
        }
        self.ack().await
    }

    /// Process the inbox until the group reaches `epoch` (a winning commit).
    async fn catch_up(&mut self, group_id: &str, epoch: u64) -> Result<(), MessagingError> {
        for _ in 0..3 {
            self.sync_into_backlog().await?;
            if self.mls.epoch(group_id)? >= epoch {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(MessagingError::Protocol(format!(
            "group `{group_id}` is at epoch {} after processing the inbox, the broker reports {epoch}",
            self.mls.epoch(group_id)?
        )))
    }

    /// Process everything currently in the inbox and return the events (plus
    /// any processed earlier while re-basing). Does not wait for new entries.
    pub async fn sync(&mut self) -> Result<Vec<InboxEvent>, MessagingError> {
        self.sync_into_backlog().await?;
        Ok(self.backlog.drain(..).collect())
    }

    /// The next inbox event, waiting for one on a follow stream. `Ok(None)` when
    /// the broker closed the stream; the next call reopens it.
    pub async fn next_event(&mut self) -> Result<Option<InboxEvent>, MessagingError> {
        if let Some(event) = self.backlog.pop_front() {
            return Ok(Some(event));
        }
        loop {
            if self.follow.is_none() {
                self.follow = Some(self.open_inbox(true).await?);
            }
            let Some(stream) = self.follow.as_mut() else {
                continue;
            };
            let response = match stream.message().await {
                Ok(Some(response)) => response,
                Ok(None) => {
                    self.follow = None;
                    return Ok(None);
                }
                Err(status) => {
                    self.follow = None;
                    return Err(status.into());
                }
            };
            let event = self.handle(response)?;
            self.ack().await?;
            if let Some(event) = event {
                return Ok(Some(event));
            }
            if let Some(event) = self.backlog.pop_front() {
                return Ok(Some(event));
            }
        }
    }

    /// Wait up to `timeout` for the next event.
    pub async fn next_event_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<InboxEvent>, MessagingError> {
        match tokio::time::timeout(timeout, self.next_event()).await {
            Ok(result) => result,
            Err(_) => Ok(None),
        }
    }
}
