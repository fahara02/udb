//! Client-side MLS for UDB's end-to-end encrypted messaging service.
//!
//! The broker's `MessagingService` is the MLS Delivery Service and key
//! directory: it validates handshake commits against the PUBLIC group state and
//! fans ciphertext out to per-device inboxes, but it never holds a group secret
//! and never sees plaintext. Everything secret happens here, on the device:
//!
//! - a device identity (an Ed25519 signature key and an MLS `BasicCredential`
//!   naming `<tenant>/<subject>/<device_id>`),
//! - KeyPackages (a one-time batch plus a last-resort package),
//! - group creation and `PublicMessage` commits that add, remove, or rekey,
//! - Welcome processing for groups this device was added to,
//! - `PrivateMessage` encryption and decryption of application messages,
//! - in-order processing of the device inbox (commits, welcomes, messages) with
//!   epoch tracking, and the re-base path when a concurrent commit won,
//! - an opt-in encrypted backup of the device state and the application's
//!   history, sealed under a key derived from a user recovery secret
//!   ([`MlsClient::export_backup`], [`MlsClient::restore_backup`]),
//! - key pinning: the first signature key seen for each peer device is pinned
//!   (from the directory, a claimed KeyPackage, a Welcome or a commit), a
//!   different key later is refused ([`MlsError::KeyChanged`]) or reported
//!   ([`KeyEvent`], [`MlsClient::take_key_events`]) and never silently
//!   trusted, and two users compare [`MlsClient::safety_number`] out of band.
//!
//! The crate is sans-IO. [`MlsClient`] returns the bytes each RPC carries and
//! consumes the entries `StreamInbox` delivers; the SDKs drive the network. All
//! client state is persisted after every mutation through a pluggable
//! [`StateStore`] ([`MemoryStore`], or [`FileStore`] on native targets).
//!
//! ```
//! use udb_mls::{CommitIntent, DeviceIdentity, InboxEvent, InboxItem, MemoryStore, MlsClient};
//!
//! # fn main() -> Result<(), udb_mls::MlsError> {
//! let mut alice = MlsClient::create(DeviceIdentity::new("t1", "alice", "alice-phone")?, MemoryStore::new())?;
//! let mut bob = MlsClient::create(DeviceIdentity::new("t1", "bob", "bob-phone")?, MemoryStore::new())?;
//!
//! // Bob publishes KeyPackages; Alice creates a group and adds him.
//! let bob_packages = bob.key_packages(1)?;
//! alice.create_group("room-1")?;
//! let pending = alice
//!     .commit("room-1", CommitIntent::Add { key_packages: bob_packages })?
//!     .expect("bob is not a member yet");
//! // ... SubmitCommit accepted by the broker ...
//! alice.confirm_commit("room-1")?;
//!
//! // Bob's inbox delivers the Welcome; Alice's message decrypts on his side.
//! let joined = bob.process(&InboxItem::welcome(1, "room-1", 1, "alice-phone", pending.welcome.unwrap()))?;
//! assert!(matches!(joined, InboxEvent::Joined { .. }));
//! let sealed = alice.encrypt("room-1", b"hello")?;
//! let event = bob.process(&InboxItem::message(2, "room-1", sealed.epoch, "alice-phone", sealed.ciphertext))?;
//! assert!(matches!(event, InboxEvent::Message { ref plaintext, .. } if plaintext == b"hello"));
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

mod backup;
mod client;
mod error;
mod identity;
mod inbox;
mod pins;
mod store;

pub use backup::{
    open_backup, seal_backup, BackupContents, BackupParams, BACKUP_MAGIC, MIN_BACKUP_LEN,
    MIN_RECOVERY_SECRET_LEN,
};
pub use client::{
    inspect_key_package, CommitIntent, CommitSummary, DeviceRegistration, GroupCreation,
    GroupMember, KeyPackageInfo, MlsClient, OutgoingMessage, PendingCommit,
};
pub use error::MlsError;
pub use identity::{device_identity, key_fingerprint, split_identity, DeviceIdentity, CIPHERSUITE};
pub use inbox::{InboxEvent, InboxItem, InboxKind};
pub use pins::{KeyEvent, KeySource, Observation, PinnedKey};
#[cfg(not(target_arch = "wasm32"))]
pub use store::FileStore;
pub use store::{MemoryStore, StateStore};

#[cfg(test)]
mod tests;
