//! Static configuration for the native `StorageService`: the entity message
//! name, the versioned outbox topics, and the stable machine-readable error
//! codes. Extracted verbatim from the former god file; every value is
//! byte-stable for downstream audit/CDC/error consumers.

pub(crate) const FILE_MSG: &str = "udb.core.storage.entity.v1.File";

/// Topics for the storage domain events emitted via the transactional outbox
/// (→ CDC → Kafka). Dot-only per the project's Kafka topic convention.
pub(crate) const TOPIC_UPLOAD_URL_ISSUED: &str = "udb.storage.file.upload_url_issued.v1";
pub(crate) const TOPIC_FILE_FINALIZED: &str = "udb.storage.file.finalized.v1";
pub(crate) const TOPIC_FILE_METADATA_UPDATED: &str = "udb.storage.file.metadata_updated.v1";
pub(crate) const TOPIC_FILE_DELETED: &str = "udb.storage.file.deleted.v1";

/// Stable machine-readable error codes for the storage service (§04.5). Emitted
/// as `ApiError.code` on the OK-with-error register paths, or as the
/// `error-reason` Status metadata trailer on non-OK gRPC statuses — a non-OK
/// status is trailers-only and discards the body `ApiError`, so the sub-code must
/// ride a trailer (mirrors the notification/webrtc services).
pub(crate) const STORAGE_QUOTA_EXCEEDED: &str = "STORAGE_QUOTA_EXCEEDED";
pub(crate) const UPLOAD_URL_UNAVAILABLE: &str = "UPLOAD_URL_UNAVAILABLE";
pub(crate) const OBJECT_NOT_PRESENT: &str = "OBJECT_NOT_PRESENT";
pub(crate) const UPLOAD_SIZE_MISMATCH: &str = "UPLOAD_SIZE_MISMATCH";
pub(crate) const ALREADY_FINALIZED: &str = "ALREADY_FINALIZED";
/// Soft-delete warn path only (bytes orphaned after a metadata delete) — emitted
/// on the warn log, NOT part of the RPC error catalog.
pub(crate) const OBJECT_DELETE_ORPHANED: &str = "OBJECT_DELETE_ORPHANED";
/// Reserved: defined for the catalog but no live emit site on the in-process
/// path yet (expiry/backend-unsupported are not surfaced as RPC errors today).
#[allow(dead_code)]
pub(crate) const UPLOAD_EXPIRED: &str = "UPLOAD_EXPIRED";
#[allow(dead_code)]
pub(crate) const UNSUPPORTED_OBJECT_BACKEND: &str = "UNSUPPORTED_OBJECT_BACKEND";
