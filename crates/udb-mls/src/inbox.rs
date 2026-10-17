//! Inbox entries as the broker delivers them, and what processing one yields.

/// The kind of an inbox entry (`InboxEntry.kind` on the wire).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboxKind {
    /// A `PublicMessage` commit; `epoch` is the epoch it was built on.
    Commit,
    /// A Welcome for a device the commit added; `epoch` is the new epoch.
    Welcome,
    /// An application `PrivateMessage`; `epoch` is the epoch it was sealed in.
    Message,
    /// The broker's JSON hint that a member device was revoked.
    RemovalHint,
    /// A kind this client version does not know.
    Other,
}

impl InboxKind {
    /// Parse the wire `kind` string (`COMMIT`, `WELCOME`, `MESSAGE`, `REMOVAL_HINT`).
    pub fn parse(kind: &str) -> Self {
        match kind {
            "COMMIT" => Self::Commit,
            "WELCOME" => Self::Welcome,
            "MESSAGE" => Self::Message,
            "REMOVAL_HINT" => Self::RemovalHint,
            _ => Self::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Commit => "COMMIT",
            Self::Welcome => "WELCOME",
            Self::Message => "MESSAGE",
            Self::RemovalHint => "REMOVAL_HINT",
            Self::Other => "OTHER",
        }
    }
}

/// One inbox entry, transport-neutral (the SDKs map `InboxEntry` onto this).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxItem {
    /// The device-local sequence number. Entries at or below the client's
    /// cursor were already processed and are skipped.
    pub seq: i64,
    pub kind: InboxKind,
    pub group_id: String,
    pub epoch: u64,
    /// The MLS message (or the JSON hint for [`InboxKind::RemovalHint`]).
    pub payload: Vec<u8>,
    pub sender_device_id: String,
    pub client_message_id: String,
}

impl InboxItem {
    fn new(
        seq: i64,
        kind: InboxKind,
        group_id: &str,
        epoch: u64,
        sender_device_id: &str,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            seq,
            kind,
            group_id: group_id.to_string(),
            epoch,
            payload,
            sender_device_id: sender_device_id.to_string(),
            client_message_id: String::new(),
        }
    }

    pub fn commit(seq: i64, group_id: &str, epoch: u64, sender: &str, payload: Vec<u8>) -> Self {
        Self::new(seq, InboxKind::Commit, group_id, epoch, sender, payload)
    }

    pub fn welcome(seq: i64, group_id: &str, epoch: u64, sender: &str, payload: Vec<u8>) -> Self {
        Self::new(seq, InboxKind::Welcome, group_id, epoch, sender, payload)
    }

    pub fn message(seq: i64, group_id: &str, epoch: u64, sender: &str, payload: Vec<u8>) -> Self {
        Self::new(seq, InboxKind::Message, group_id, epoch, sender, payload)
    }
}

/// The outcome of processing one inbox entry. Plaintext appears only in
/// [`InboxEvent::Message`], and only to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxEvent {
    /// This device joined a group from a Welcome.
    Joined {
        seq: i64,
        group_id: String,
        epoch: u64,
        /// Device ids of every member, this one included.
        members: Vec<String>,
        added_by: String,
    },
    /// Another member's commit was applied: the group moved to `epoch`.
    Commit {
        seq: i64,
        group_id: String,
        epoch: u64,
        sender_device_id: String,
        added: Vec<String>,
        removed: Vec<String>,
    },
    /// This device's own commit came back through the inbox. If it was still
    /// pending (the SubmitCommit response was lost) it has now been merged; for
    /// an external join, the echo confirms the join the same way.
    OwnCommit {
        seq: i64,
        group_id: String,
        epoch: u64,
        merged: bool,
    },
    /// This device was removed from the group; its local state was deleted.
    Removed {
        seq: i64,
        group_id: String,
        removed_by: String,
    },
    /// A decrypted application message.
    Message {
        seq: i64,
        group_id: String,
        epoch: u64,
        sender_device_id: String,
        sender_subject_id: String,
        client_message_id: String,
        plaintext: Vec<u8>,
    },
    /// The broker reported that a member device was revoked; a member should
    /// commit its removal.
    RemovalHint {
        seq: i64,
        group_id: String,
        device_id: String,
        reason: String,
    },
    /// An entry that needs no action: this device's own message echo, an entry
    /// already processed, a commit for an epoch this device already left, or a
    /// kind this version does not know.
    Skipped { seq: i64, reason: String },
    /// An entry this device could not process (it cannot decrypt the message,
    /// the Welcome names a KeyPackage it no longer holds, a commit gap). The
    /// cursor still advances past it.
    Undecryptable {
        seq: i64,
        group_id: String,
        reason: String,
    },
}

impl InboxEvent {
    /// The inbox sequence number the event came from.
    pub fn seq(&self) -> i64 {
        match self {
            Self::Joined { seq, .. }
            | Self::Commit { seq, .. }
            | Self::OwnCommit { seq, .. }
            | Self::Removed { seq, .. }
            | Self::Message { seq, .. }
            | Self::RemovalHint { seq, .. }
            | Self::Skipped { seq, .. }
            | Self::Undecryptable { seq, .. } => *seq,
        }
    }
}
