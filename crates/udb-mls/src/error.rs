//! The crate's error type.
//!
//! Only failures the CALLER must handle are errors: a broken store, an unknown
//! group, a misuse of the commit lifecycle, input that is not a valid MLS
//! object. A single inbox entry that cannot be processed (a message this device
//! cannot decrypt, a Welcome for a KeyPackage it no longer holds) is reported as
//! an [`crate::InboxEvent::Undecryptable`] event instead, so one bad entry
//! never wedges the inbox.

/// Every failure `udb-mls` reports.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MlsError {
    /// The [`crate::StateStore`] could not load or save client state.
    #[error("client state store: {0}")]
    Store(String),
    /// The stored client state could not be decoded.
    #[error("client state is corrupt: {0}")]
    Corrupt(String),
    /// [`crate::MlsClient::open`] found no state in the store.
    #[error("the state store holds no device")]
    NotInitialized,
    /// [`crate::MlsClient::create`] found a device already in the store.
    #[error("the state store already holds device `{0}`")]
    AlreadyInitialized(String),
    /// A tenant, subject, device, or group identifier is not acceptable.
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    /// This device holds no state for the group.
    #[error("unknown group `{0}`")]
    UnknownGroup(String),
    /// The group already exists on this device.
    #[error("group `{0}` already exists on this device")]
    GroupExists(String),
    /// A commit is already pending for the group.
    #[error("group `{0}` has a pending commit; confirm or discard it first")]
    PendingCommit(String),
    /// No commit is pending for the group.
    #[error("group `{0}` has no pending commit")]
    NoPendingCommit(String),
    /// An input is not an acceptable MLS object (a KeyPackage that does not
    /// verify, a member that is not in the group, a foreign tenant's device).
    #[error("rejected: {0}")]
    Rejected(String),
    /// An encrypted backup could not be sealed or opened (a recovery secret
    /// that is too short, a wrong secret, a modified or foreign envelope).
    #[error("backup: {0}")]
    Backup(String),
    /// A device presented a different signature key than the one this client
    /// pinned for it (from the directory or a claimed KeyPackage). The pin is
    /// unchanged; compare `observed_fingerprint` with the device's owner out of
    /// band, then [`crate::MlsClient::accept_key_change`] it or stop.
    #[error(
        "the key of device `{device_id}` of `{subject_id}` changed: pinned {pinned_fingerprint}, observed {observed_fingerprint}"
    )]
    KeyChanged {
        subject_id: String,
        device_id: String,
        pinned_fingerprint: String,
        observed_fingerprint: String,
    },
    /// OpenMLS refused an operation.
    #[error("MLS operation failed: {0}")]
    Mls(String),
}

pub(crate) fn mls<E: std::fmt::Debug>(context: &str) -> impl FnOnce(E) -> MlsError + '_ {
    move |err| MlsError::Mls(format!("{context}: {err:?}"))
}
