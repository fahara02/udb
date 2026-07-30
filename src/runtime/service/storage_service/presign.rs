//! Object-store interaction for the native `StorageService`: the presigned-URL
//! mint, the best-effort byte delete, the HEAD presence probe, and the two
//! result enums the handlers match on. Extracted verbatim from the former god
//! file — the degraded-vs-failed presign distinction and the tri-state presence
//! check are byte-for-byte identical; the methods take `&self` unchanged.

use tonic::Status;

use crate::proto::udb::core::storage::entity::v1 as storage_entity_pb;

use super::StorageServiceImpl;
use super::config::{OBJECT_DELETE_ORPHANED, storage_sse_required};

/// Outcome of minting a presigned URL: a real URL + unix-seconds expiry, a
/// degraded deployment (no object runtime / object-store feature off), or a
/// genuine presign failure — lets `register_upload` distinguish "no objectstore"
/// from a real error on an empty `upload_url`.
pub(crate) enum PresignOutcome {
    Url { url: String, expires_at: i64 },
    Degraded,
    Failed(String),
}

/// Tri-state object-presence result from the storage `object_exists` wrapper:
/// `Unchecked` (metadata-only mode, no runtime — skip verification), `Absent`
/// (object not in the store), or `Present` with the HEAD content-length + ETag.
pub(crate) enum ObjectCheck {
    Unchecked,
    Absent,
    Present { size: i64, etag: String },
}

impl StorageServiceImpl {
    /// Best-effort delete of an object's bytes via the existing object executor.
    /// Never fails the caller: on error the metadata row stays soft-deleted and
    /// auditable, and the bytes are logged as orphaned for ops/lifecycle cleanup.
    pub(crate) async fn delete_object_bytes(&self, project_id: &str, object_key: &str) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        if object_key.trim().is_empty() {
            return;
        }
        let request_json = crate::runtime::core::setup_data::object_request_json(
            "delete",
            &self.object_bucket,
            object_key,
            "",
        );
        if let Err(err) = runtime
            .delete_object_backend_target(&self.object_backend, None, project_id, &request_json)
            .await
        {
            tracing::warn!(
                error = %err,
                object_key,
                bucket = %self.object_bucket,
                code = OBJECT_DELETE_ORPHANED,
                "storage object byte delete failed; metadata soft-deleted (auditable), bytes orphaned"
            );
        }
    }

    /// Mint a presigned object URL via the runtime (PUT for uploads, GET for
    /// downloads). Returns `("", 0)` in metadata-only mode (no runtime) or on
    /// error — callers then fall back to the existing public object RPCs.
    pub(crate) async fn presign(
        &self,
        project_id: &str,
        object_key: &str,
        method: &str,
        content_type: &str,
        ttl_minutes: i32,
    ) -> PresignOutcome {
        let Some(runtime) = self.runtime.as_ref() else {
            return PresignOutcome::Degraded;
        };
        // SSE enforcement on the native presign path (mirrors the data-plane
        // object PUT, which stamps `server_side_encryption` on the executor
        // request). A presigned PUT URL only enforces SSE when the
        // `x-amz-server-side-encryption` header is part of the SIGNED request;
        // that signing lives in the shared `presign_object_backend_target`
        // helper, so until it accepts the flag we fail closed for uploads rather
        // than hand out a URL that would let the client store the object
        // unencrypted. GET/download presign is unaffected — reading an encrypted
        // object is transparent.
        // TODO(leader-wire): extend runtime.presign_object_backend_target
        // (src/runtime/core/setup_data.rs) with a `require_sse` flag that signs
        // `x-amz-server-side-encryption: AES256` into the PUT presign, then
        // replace this fail-closed guard with that signed-header path.
        if method.eq_ignore_ascii_case("PUT") && storage_sse_required() {
            return PresignOutcome::Failed(
                "object store requires server-side encryption; the native presigned PUT cannot \
                 yet sign the encryption header (upload via the broker object PUT RPC instead)"
                    .to_string(),
            );
        }
        let ttl_secs = (ttl_minutes.max(1) as i64 * 60).min(7 * 24 * 3600) as i32;
        match runtime
            .presign_object_backend_target(
                &self.object_backend,
                project_id,
                &self.object_bucket,
                object_key,
                method,
                content_type,
                ttl_secs,
            )
            .await
        {
            Ok((url, expires_at_unix)) => PresignOutcome::Url {
                url,
                expires_at: expires_at_unix,
            },
            Err(err) => {
                tracing::warn!(error = %err, object_key, method, "storage presign failed; returning empty url");
                // An object-store feature compiled out / no instance configured is a
                // deployment-degraded state, not a transient error: surface Degraded
                // (non-retryable) so the client falls back to the public object RPCs
                // instead of retrying. Everything else is a real presign failure.
                if err.message().contains("feature is not enabled") {
                    PresignOutcome::Degraded
                } else {
                    PresignOutcome::Failed(err.message().to_string())
                }
            }
        }
    }

    pub(crate) async fn object_exists(
        &self,
        file: &storage_entity_pb::File,
    ) -> Result<ObjectCheck, Status> {
        let Some(runtime) = self.runtime.as_ref() else {
            return Ok(ObjectCheck::Unchecked);
        };
        if file.object_key.trim().is_empty() {
            return Ok(ObjectCheck::Absent);
        }
        let backend = if file.backend.trim().is_empty() {
            self.object_backend.as_str()
        } else {
            file.backend.as_str()
        };
        let bucket = if file.bucket.trim().is_empty() {
            self.object_bucket.as_str()
        } else {
            file.bucket.as_str()
        };
        match runtime
            .object_exists_backend_target(backend, &file.project_id, bucket, &file.object_key)
            .await?
        {
            Some((size, etag)) => Ok(ObjectCheck::Present { size, etag }),
            None => Ok(ObjectCheck::Absent),
        }
    }
}
