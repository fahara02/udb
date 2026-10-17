//! Key pinning: trust on first use, safety numbers, and key-change events.
//!
//! The broker's device directory vouches for each device's Ed25519 signature
//! key, and an honest broker never lets a registered device id change its key.
//! A compromised directory could still swap one, so the client remembers every
//! key it has seen, per device id, and treats any later difference as an event
//! the user must look at:
//!
//! - the first key seen for a device is pinned (a device of a subject this
//!   client has never seen is a silent first contact; an unseen device of a
//!   subject it already knows, or of its own subject, is reported as a new
//!   device);
//! - a different key for a pinned device never replaces the pin: it is kept as
//!   `pending` until the user compares it out of band and accepts it;
//! - two users compare a safety number derived from both subjects' pinned keys.
//!
//! Device ids are unique within a tenant, so pins are keyed by device id alone;
//! a device id that shows up under a different subject is a key change too.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::error::MlsError;
use crate::identity::key_fingerprint;

/// Domain separation for the safety-number digest.
const SAFETY_NUMBER_LABEL: &[u8] = b"udb-safety-v1";

/// The key this client trusts for one device.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinnedKey {
    pub subject_id: String,
    /// Raw Ed25519 signature public key.
    pub signature_public_key: Vec<u8>,
    /// When the key was first seen (Unix seconds; 0 when no clock was available).
    pub first_seen_unix: u64,
    /// The user compared the fingerprint out of band
    /// ([`crate::MlsClient::verify_device`]).
    pub verified: bool,
    /// A different key observed for this device and not accepted yet.
    #[serde(default)]
    pub pending: Option<Vec<u8>>,
    /// The subject the pending key was observed under (differs from
    /// `subject_id` when the device id showed up under another subject).
    #[serde(default)]
    pub pending_subject_id: Option<String>,
    #[serde(default)]
    pub pending_seen_unix: Option<u64>,
}

impl PinnedKey {
    /// Lowercase hex SHA-256 of the pinned key.
    pub fn fingerprint(&self) -> String {
        key_fingerprint(&self.signature_public_key)
    }

    /// Fingerprint of the pending (observed, not accepted) key, if any.
    pub fn pending_fingerprint(&self) -> Option<String> {
        self.pending.as_deref().map(key_fingerprint)
    }
}

/// Where a device key was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeySource {
    /// The broker's device directory (`ListMessagingDevices`).
    Directory,
    /// A KeyPackage claimed to add the device to a group.
    KeyPackage,
    /// The ratchet tree of a group this device joined from a Welcome.
    Welcome,
    /// The ratchet tree after another member's commit.
    Commit,
}

impl KeySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Directory => "directory",
            Self::KeyPackage => "key_package",
            Self::Welcome => "welcome",
            Self::Commit => "commit",
        }
    }

    /// Parse [`Self::as_str`].
    pub fn parse(source: &str) -> Option<Self> {
        match source {
            "directory" => Some(Self::Directory),
            "key_package" => Some(Self::KeyPackage),
            "welcome" => Some(Self::Welcome),
            "commit" => Some(Self::Commit),
            _ => None,
        }
    }

    /// Sources the client acts on (listing devices, adding a member) refuse a
    /// changed key with [`MlsError::KeyChanged`]. A Welcome or another member's
    /// commit was already accepted by the group, so a changed key there is
    /// recorded as a [`KeyEvent`] instead.
    pub(crate) fn fails_closed(self) -> bool {
        matches!(self, Self::Directory | Self::KeyPackage)
    }
}

/// Something about a peer's keys the user should see.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyEvent {
    /// A device this client had not seen, of a subject it already knew (or of
    /// its own subject). Its key is now pinned.
    NewDevice {
        subject_id: String,
        device_id: String,
        fingerprint: String,
        source: KeySource,
        group_id: Option<String>,
        /// The inbox entry it came from.
        seq: Option<i64>,
    },
    /// A pinned device presented a different key (or showed up under another
    /// subject). The pin is unchanged; the observed key is pending until
    /// [`crate::MlsClient::accept_key_change`].
    KeyChanged {
        subject_id: String,
        device_id: String,
        pinned_fingerprint: String,
        observed_fingerprint: String,
        source: KeySource,
        group_id: Option<String>,
        seq: Option<i64>,
    },
}

/// What observing one device key found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// The key equals the pin.
    Match,
    /// A device of a subject never seen before: pinned silently.
    FirstContact,
    /// An unseen device of a known subject, or of this client's own subject:
    /// pinned and reported.
    NewDevice,
    /// A different key than the pinned one: kept pending, the pin unchanged.
    Changed,
}

/// Where and when a batch of keys was observed.
pub(crate) struct ObserveContext<'a> {
    /// Subjects that had pins BEFORE the batch, so a batch that lists several
    /// devices of a new subject is one first contact, not a new-device storm.
    pub known_subjects: &'a BTreeSet<String>,
    pub own_subject: &'a str,
    pub source: KeySource,
    pub group_id: Option<&'a str>,
    pub seq: Option<i64>,
    pub now: u64,
}

/// Subjects with at least one pinned device.
pub(crate) fn known_subjects(pins: &BTreeMap<String, PinnedKey>) -> BTreeSet<String> {
    pins.values().map(|pin| pin.subject_id.clone()).collect()
}

/// Record one observed key against the pins. A new event is queued for a new
/// device, and for a changed key from a source that does not fail closed — once
/// per distinct pending key, so a member's every later commit does not repeat it.
pub(crate) fn observe(
    pins: &mut BTreeMap<String, PinnedKey>,
    events: &mut Vec<KeyEvent>,
    subject: &str,
    device: &str,
    key: &[u8],
    ctx: &ObserveContext<'_>,
) -> Observation {
    match pins.get_mut(device) {
        Some(pin) if pin.subject_id == subject && pin.signature_public_key == key => {
            Observation::Match
        }
        Some(pin) => {
            let fresh = pin.pending.as_deref() != Some(key)
                || pin.pending_subject_id.as_deref() != Some(subject);
            if fresh {
                pin.pending = Some(key.to_vec());
                pin.pending_subject_id = Some(subject.to_string());
                pin.pending_seen_unix = Some(ctx.now);
                if !ctx.source.fails_closed() {
                    events.push(KeyEvent::KeyChanged {
                        subject_id: subject.to_string(),
                        device_id: device.to_string(),
                        pinned_fingerprint: pin.fingerprint(),
                        observed_fingerprint: key_fingerprint(key),
                        source: ctx.source,
                        group_id: ctx.group_id.map(str::to_string),
                        seq: ctx.seq,
                    });
                }
            }
            Observation::Changed
        }
        None => {
            pins.insert(
                device.to_string(),
                PinnedKey {
                    subject_id: subject.to_string(),
                    signature_public_key: key.to_vec(),
                    first_seen_unix: ctx.now,
                    verified: false,
                    pending: None,
                    pending_subject_id: None,
                    pending_seen_unix: None,
                },
            );
            if subject == ctx.own_subject || ctx.known_subjects.contains(subject) {
                events.push(KeyEvent::NewDevice {
                    subject_id: subject.to_string(),
                    device_id: device.to_string(),
                    fingerprint: key_fingerprint(key),
                    source: ctx.source,
                    group_id: ctx.group_id.map(str::to_string),
                    seq: ctx.seq,
                });
                Observation::NewDevice
            } else {
                Observation::FirstContact
            }
        }
    }
}

/// The error a fail-closed source reports for a changed key.
pub(crate) fn key_changed(
    pins: &BTreeMap<String, PinnedKey>,
    subject: &str,
    device: &str,
    observed: &[u8],
) -> MlsError {
    MlsError::KeyChanged {
        subject_id: subject.to_string(),
        device_id: device.to_string(),
        pinned_fingerprint: pins
            .get(device)
            .map(PinnedKey::fingerprint)
            .unwrap_or_default(),
        observed_fingerprint: key_fingerprint(observed),
    }
}

/// A fingerprint as the user typed or pasted it.
pub(crate) fn normalize_fingerprint(fingerprint: &str) -> String {
    fingerprint
        .chars()
        .filter(|c| !c.is_whitespace() && *c != ':')
        .collect::<String>()
        .to_ascii_lowercase()
}

/// A subject's pinned devices, sorted by device id.
fn block<'a>(pins: &'a BTreeMap<String, PinnedKey>, subject: &str) -> Vec<(&'a str, &'a [u8])> {
    pins.iter()
        .filter(|(_, pin)| pin.subject_id == subject)
        .map(|(device, pin)| (device.as_str(), pin.signature_public_key.as_slice()))
        .collect()
}

fn put(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u32).to_be_bytes());
    hasher.update(bytes);
}

/// The safety number two users compare: SHA-256 over both subjects' pinned
/// devices (each subject's devices sorted by device id, the two subjects in
/// byte order, every field length-prefixed), rendered as 12 groups of 5 digits.
/// It is the same on both sides and independent of who computes it.
pub(crate) fn safety_number(
    tenant: &str,
    pins: &BTreeMap<String, PinnedKey>,
    own_subject: &str,
    peer_subject: &str,
) -> Result<String, MlsError> {
    let peer = block(pins, peer_subject);
    if peer.is_empty() {
        return Err(MlsError::Rejected(format!(
            "no pinned device of `{peer_subject}`: list its devices first"
        )));
    }
    let mut blocks = [
        (own_subject, block(pins, own_subject)),
        (peer_subject, peer),
    ];
    blocks.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut hasher = Sha256::new();
    hasher.update(SAFETY_NUMBER_LABEL);
    put(&mut hasher, tenant.as_bytes());
    for (subject, devices) in &blocks {
        put(&mut hasher, subject.as_bytes());
        hasher.update((devices.len() as u32).to_be_bytes());
        for (device, key) in devices {
            put(&mut hasher, device.as_bytes());
            put(&mut hasher, key);
        }
    }
    let digest = hasher.finalize();
    let mut groups = Vec::with_capacity(12);
    for chunk in digest[..30].chunks(5) {
        let value = chunk
            .iter()
            .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte))
            % 10_000_000_000;
        groups.push(format!("{:05}", value / 100_000));
        groups.push(format!("{:05}", value % 100_000));
    }
    Ok(groups.join(" "))
}
