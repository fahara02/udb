//! Static configuration for the native `MessagingService`: entity message names,
//! the inbox outbox topic, the durable status/kind tokens, the request bounds,
//! and the retention-worker knobs.

pub(crate) const DEVICE_MSG: &str = "udb.core.messaging.entity.v1.MessagingDevice";
pub(crate) const KEY_PACKAGE_MSG: &str = "udb.core.messaging.entity.v1.MessagingKeyPackage";
pub(crate) const GROUP_MSG: &str = "udb.core.messaging.entity.v1.MessagingGroup";
pub(crate) const GROUP_MEMBER_MSG: &str = "udb.core.messaging.entity.v1.MessagingGroupMember";
pub(crate) const INBOX_MSG: &str = "udb.core.messaging.entity.v1.MessagingInboxEntry";
pub(crate) const BACKUP_MSG: &str = "udb.core.messaging.entity.v1.MessagingBackup";

/// Emitted once per appended inbox row, in the same transaction as the row. The
/// payload names only `{tenant_id, device_id, seq}`: subscribers wake and pull
/// the entry through `StreamInbox`; no content (not even the group) leaves the
/// inbox through the event pipeline.
pub(crate) const TOPIC_INBOX_APPENDED: &str = "udb.messaging.inbox.appended.v1";

pub(crate) const DEVICE_ACTIVE: &str = "ACTIVE";
pub(crate) const DEVICE_REVOKED: &str = "REVOKED";

pub(crate) const KIND_COMMIT: &str = "COMMIT";
pub(crate) const KIND_WELCOME: &str = "WELCOME";
pub(crate) const KIND_MESSAGE: &str = "MESSAGE";
pub(crate) const KIND_REMOVAL_HINT: &str = "REMOVAL_HINT";

/// Longest accepted device / group / client-message identifier.
pub(crate) const MAX_DEVICE_ID_LEN: usize = 128;
pub(crate) const MAX_GROUP_ID_LEN: usize = 255;
pub(crate) const MAX_CLIENT_MESSAGE_ID_LEN: usize = 128;
pub(crate) const MAX_SUBJECT_ID_LEN: usize = 255;

/// One-time KeyPackages accepted per `PublishKeyPackages` call.
pub(crate) const MAX_KEY_PACKAGES_PER_PUBLISH: usize = 100;
/// Unclaimed one-time KeyPackages a device may hold. Bounds the table so one
/// device cannot exhaust the shared store.
pub(crate) const MAX_KEY_PACKAGE_STOCK: i64 = 500;
/// Target devices one `ClaimKeyPackages` call may resolve.
pub(crate) const MAX_CLAIM_TARGETS: usize = 256;
/// Largest MLS message (commit, welcome, ciphertext, group info, tree) accepted.
pub(crate) const MAX_MLS_MESSAGE_BYTES: usize = 1024 * 1024;
/// Members one group may hold; bounds the per-commit fan-out transaction.
pub(crate) const MAX_GROUP_MEMBERS: usize = 1000;

/// The backup a request names when it names none.
pub(crate) const DEFAULT_BACKUP_ID: &str = "default";
/// Longest accepted backup id.
pub(crate) const MAX_BACKUP_ID_LEN: usize = 128;
/// Every sealed backup envelope (udb-mls `seal_backup`) starts with this tag.
/// The broker cannot check the encryption, but it refuses a body that is not
/// an envelope at all, so a client bug cannot store a plaintext export.
pub(crate) const BACKUP_ENVELOPE_MAGIC: &[u8; 8] = b"UDBMLSB1";
/// Smallest possible envelope: magic, Argon2id parameters (3 x u32), 16-byte
/// salt, 24-byte nonce, and the 16-byte Poly1305 tag of an empty payload.
pub(crate) const MIN_BACKUP_BYTES: usize = 8 + 12 + 16 + 24 + 16;
/// Largest accepted envelope. It stays under the 4 MiB default gRPC message
/// limit of the native listener and of the SDK clients, so a backup always
/// round-trips without per-client limit changes.
pub(crate) const MAX_BACKUP_BYTES: usize = 3 * 1024 * 1024;

/// Inbox rows one `StreamInbox` page reads.
pub(crate) const INBOX_PAGE: i64 = 100;
/// Devices per ListMessagingDevices page when the caller sends no page_size.
pub(crate) const DEVICE_PAGE: i64 = 100;
/// How long a following `StreamInbox` waits for an in-process wake before it
/// polls the store again (covers appends made by another broker replica).
pub(crate) const INBOX_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

/// Upper bound on inbox rows one retention pass deletes per store.
pub(crate) const INBOX_RETENTION_BATCH: i64 = 5000;
const DEFAULT_INBOX_TTL_SECS: u64 = 30 * 24 * 60 * 60;
const INBOX_TTL_ENV: &str = "UDB_MESSAGING_INBOX_TTL";
const DEFAULT_RETENTION_INTERVAL_SECS: u64 = 300;
const RETENTION_INTERVAL_ENV: &str = "UDB_MESSAGING_RETENTION_INTERVAL_SECS";

/// Parse a TTL: a bare number of seconds or a number with an `s`/`m`/`h`/`d`
/// suffix (`30d`, `12h`). Zero, negative or malformed values are rejected.
pub(crate) fn parse_ttl_secs(raw: &str) -> Option<u64> {
    let raw = raw.trim();
    let (digits, unit) = match raw.char_indices().last() {
        Some((index, c)) if c.is_ascii_alphabetic() => (&raw[..index], c.to_ascii_lowercase()),
        _ => (raw, 's'),
    };
    let value: u64 = digits.trim().parse().ok().filter(|v| *v > 0)?;
    let scale = match unit {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        _ => return None,
    };
    value.checked_mul(scale)
}

/// Inbox TTL (`UDB_MESSAGING_INBOX_TTL`, default 30 days). Resolved once at
/// worker spawn, never per request.
pub(crate) fn inbox_ttl() -> std::time::Duration {
    static SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let secs = *SECS.get_or_init(|| match std::env::var(INBOX_TTL_ENV) {
        Ok(raw) => parse_ttl_secs(&raw).unwrap_or_else(|| {
            tracing::warn!(
                value = %raw,
                default_secs = DEFAULT_INBOX_TTL_SECS,
                "UDB_MESSAGING_INBOX_TTL is not a positive duration; using the default"
            );
            DEFAULT_INBOX_TTL_SECS
        }),
        Err(_) => DEFAULT_INBOX_TTL_SECS,
    });
    std::time::Duration::from_secs(secs)
}

/// Retention-worker cadence (`UDB_MESSAGING_RETENTION_INTERVAL_SECS`, default 300).
pub(crate) fn inbox_retention_interval() -> std::time::Duration {
    static SECS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let secs = *SECS.get_or_init(|| {
        std::env::var(RETENTION_INTERVAL_ENV)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_RETENTION_INTERVAL_SECS)
    });
    std::time::Duration::from_secs(secs)
}
