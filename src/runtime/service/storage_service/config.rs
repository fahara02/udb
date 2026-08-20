//! Static configuration for the native `StorageService`: the entity message
//! name, the versioned outbox topics, and the stable machine-readable error
//! codes. Extracted verbatim from the former god file; every value is
//! byte-stable for downstream audit/CDC/error consumers.

pub(crate) const FILE_MSG: &str = "udb.core.storage.entity.v1.File";

/// Durable object-GC intent ledger (schema-qualified). A HARD `DeleteFile` records
/// one PENDING row here, atomically with the metadata tombstone, so an object that
/// fails to delete inline can never leak: the leader-elected sweep drives it to
/// convergence (or dead-letters it after the attempt cap). This is a broker-owned
/// operational table managed via `store::ensure_gc_intents_table` — it is NOT a
/// proto entity (tenant isolation is enforced by the per-tenant handler filters,
/// not RLS), so ideally the maintainer promotes it to a `file.proto`-style entity
/// + manifest table in a follow-up.
pub(crate) const GC_INTENTS_RELATION: &str = "udb_storage.gc_intents";

/// Leader-election lock name for the GC-intent sweep worker. Kept distinct from
/// `WORKER_STORAGE_ORPHAN_REAPER` so the two workers hold independent leases.
/// Defined locally because `src/runtime/singleton.rs` (the canonical `WORKER_*`
/// registry) is out of this change's fence — the maintainer should add a matching
/// `WORKER_STORAGE_GC_SWEEP` const + distinctness-test entry there.
pub(crate) const WORKER_STORAGE_GC_SWEEP: &str = "udb:storage:gc-sweep";

/// Default cap on inline+sweep object-delete attempts before a GC intent is
/// dead-lettered (`status = 'FAILED'`). Overridable via `UDB_STORAGE_GC_MAX_ATTEMPTS`.
pub(crate) const GC_INTENT_DEFAULT_MAX_ATTEMPTS: i64 = 10;

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
pub(crate) const REISSUE_REQUIRES_PENDING: &str = "REISSUE_REQUIRES_PENDING";
/// Finalize tried to change a registration-established (immutable) field —
/// reference id/type, content/file type, or visibility. Finalize is an
/// ownership-preserving lifecycle transition, not a metadata-update endpoint, so
/// a conflicting value is rejected fail-closed instead of silently rewriting the
/// row (a same-tenant finalize scope must not re-point ownership or escalate
/// visibility).
pub(crate) const FINALIZE_IMMUTABLE_MISMATCH: &str = "FINALIZE_IMMUTABLE_MISMATCH";
/// Soft-delete warn path only (bytes orphaned after a metadata delete) — emitted
/// on the warn log, NOT part of the RPC error catalog.
pub(crate) const OBJECT_DELETE_ORPHANED: &str = "OBJECT_DELETE_ORPHANED";
/// HARD DeleteFile replayed an idempotency key that was first claimed by a delete
/// with a DIFFERENT target (file/mode) — reused fail-closed instead of replaying a
/// mismatched outcome. Mirrors the data-plane idempotency-mismatch contract.
pub(crate) const IDEMPOTENCY_KEY_CONFLICT: &str = "IDEMPOTENCY_KEY_CONFLICT";
/// HARD DeleteFile committed the tombstone + durable GC intent but the object
/// executor could not remove the bytes; success is NOT reported and the PENDING
/// intent is left for the leader-elected sweep to drive to convergence. Retryable.
pub(crate) const OBJECT_DELETE_FAILED: &str = "OBJECT_DELETE_FAILED";
/// DeleteFile's `expected_status` optimistic guard did not match the file's
/// current status token — the delete is refused fail-closed.
pub(crate) const DELETE_PRECONDITION_FAILED: &str = "DELETE_PRECONDITION_FAILED";
/// Reserved: defined for the catalog but no live emit site on the in-process
/// path yet (expiry/backend-unsupported are not surfaced as RPC errors today).
#[allow(dead_code)]
pub(crate) const UPLOAD_EXPIRED: &str = "UPLOAD_EXPIRED";
#[allow(dead_code)]
pub(crate) const UNSUPPORTED_OBJECT_BACKEND: &str = "UNSUPPORTED_OBJECT_BACKEND";
/// A client-supplied `filename` produced an object key that would escape the
/// tenant/file prefix (traversal / absolute / separator) — the register is
/// rejected fail-closed before any row or object is created.
pub(crate) const OBJECT_KEY_TRAVERSAL: &str = "OBJECT_KEY_TRAVERSAL";

/// Env knob: when set truthy, the native storage object store requires
/// server-side encryption (SSE-S3 / AES-256) on every object write — the
/// native-path analog of an object store's `server_side_encryption` catalog
/// annotation, which the data plane enforces on the object PUT. Native services
/// hold no catalog-manifest handle, so the requirement is surfaced through this
/// companion knob alongside `UDB_STORAGE_BUCKET` / `UDB_STORAGE_OBJECT_BACKEND`.
pub(crate) const STORAGE_SSE_ENV: &str = "UDB_STORAGE_SERVER_SIDE_ENCRYPTION";

/// Whether native object writes/presigns into this service's store must request
/// server-side encryption. Parsed with the same truthy set the rest of the
/// runtime uses for boolean env flags.
pub(crate) fn storage_sse_required() -> bool {
    std::env::var(STORAGE_SSE_ENV)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

// ── Content scanning (V050-3) ────────────────────────────────────────────────

/// Topic for the scan-verdict transition, emitted through the same
/// transactional outbox as the other storage events so a consumer can gate on
/// "this file became CLEAN" without polling.
pub(crate) const TOPIC_FILE_SCAN_VERDICT_SET: &str = "udb.storage.file.scan_verdict_set.v1";

/// A download was refused because the file's scan verdict is not CLEAN.
pub(crate) const SCAN_VERDICT_NOT_CLEAN: &str = "SCAN_VERDICT_NOT_CLEAN";
/// `SetScanVerdict` was called with UNSPECIFIED, or tried an illegal transition.
pub(crate) const SCAN_VERDICT_INVALID: &str = "SCAN_VERDICT_INVALID";

/// Require a CLEAN scan verdict before any download path hands out bytes or a
/// presigned URL.
///
/// OFF by default, and that default is deliberate. Turning it on for every
/// deployment at upgrade would refuse every pre-existing file the moment the
/// column appeared, because nothing has scanned them — an availability outage
/// dressed as a security fix. An operator enables this once a scanner is
/// actually writing verdicts.
///
/// INFECTED is refused whether or not this is set: if something looked at the
/// bytes and said they are malicious, serving them is not a configurable
/// choice. Only the explicit override scope reaches an infected object.
pub(crate) const STORAGE_REQUIRE_CLEAN_SCAN_ENV: &str = "UDB_STORAGE_REQUIRE_CLEAN_SCAN";

/// Scope that lets a caller download an object whose verdict is not CLEAN —
/// quarantine review, incident response, or a deliberate operator override. It
/// is separate from every ordinary storage scope so it cannot be held by
/// accident.
pub(crate) const SCAN_OVERRIDE_SCOPE: &str = "udb:storage:download-unscanned";

pub(crate) fn storage_require_clean_scan() -> bool {
    std::env::var(STORAGE_REQUIRE_CLEAN_SCAN_ENV)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}
