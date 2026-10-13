//! Storage-layout safety (C5). Since 0.5.0 every data-plane object key is
//! namespaced by its tenant (`__udb_t/<tenant>/<key>`); objects written by
//! earlier versions sit at their bare key and the object RPCs no longer find
//! them. Their owner cannot be derived from the key — those deployments were
//! single-tenant — so the operator names the tenant and `udb migrate storage`
//! copies the bytes under its namespace. Copies only: the bare object stays
//! until the operator deletes it, so a wrong tenant is recoverable.
//!
//! The broker checks at startup and WARNS when a bucket still holds bare keys;
//! `UDB_STORAGE_LAYOUT_STRICT=true` turns the warning into a refusal to start.

use super::*;

/// Prefixes that mark a key as already namespaced (typed object RPCs and the
/// IR compiler respectively).
const NAMESPACED_PREFIXES: [&str; 2] = ["__udb_t/", "t:"];

/// Whether `key` was written before tenant namespacing.
pub(crate) fn is_legacy_object_key(key: &str) -> bool {
    if NAMESPACED_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        return false;
    }
    // Native StorageService presigns keys as <tenant UUID>/<file UUID>/<name>.
    // Those already carry their tenant even without the data-plane prefix.
    let mut parts = key.splitn(3, '/');
    !matches!((parts.next(), parts.next(), parts.next()), (Some(tenant), Some(file), Some(name))
        if !name.is_empty() && uuid::Uuid::parse_str(tenant).is_ok() && uuid::Uuid::parse_str(file).is_ok())
}

/// The namespaced key a legacy object moves to for `tenant`.
pub(crate) fn namespaced_object_key(tenant: &str, key: &str) -> String {
    format!("__udb_t/{}/{}", tenant.trim(), key.trim_start_matches('/'))
}

/// What a layout scan or migration found.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct StorageLayoutReport {
    pub bucket: String,
    /// Legacy (un-namespaced) keys seen, up to the scan limit.
    pub legacy_keys: Vec<String>,
    /// Keys copied under the tenant's namespace.
    pub copied: usize,
    /// Keys whose namespaced copy already existed.
    pub already_present: usize,
    pub dry_run: bool,
    /// More legacy keys exist past the limit.
    pub truncated: bool,
}

impl DataBrokerRuntime {
    /// List up to `limit` legacy keys in `bucket` under `prefix`.
    #[cfg(feature = "s3")]
    pub async fn scan_legacy_object_keys(
        &self,
        bucket: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<StorageLayoutReport, tonic::Status> {
        let client = self.s3_for_instance_for_project(None, "")?.clone();
        let mut report = StorageLayoutReport {
            bucket: bucket.to_string(),
            dry_run: true,
            ..Default::default()
        };
        let mut token: Option<String> = None;
        loop {
            let mut request = client.list_objects_v2().bucket(bucket);
            if !prefix.is_empty() {
                request = request.prefix(prefix);
            }
            if let Some(next) = token.take() {
                request = request.continuation_token(next);
            }
            let page = request.send().await.map_err(|err| {
                crate::runtime::executor_utils::backend_transport_status(
                    "S3",
                    "list_objects_v2",
                    err,
                )
            })?;
            for object in page.contents() {
                let Some(key) = object.key() else { continue };
                if is_legacy_object_key(key) {
                    if report.legacy_keys.len() >= limit {
                        report.truncated = true;
                        return Ok(report);
                    }
                    report.legacy_keys.push(key.to_string());
                }
            }
            match page.next_continuation_token() {
                Some(next) if page.is_truncated().unwrap_or(false) => {
                    token = Some(next.to_string());
                }
                _ => return Ok(report),
            }
        }
    }

    /// Copy up to `limit` legacy keys under `tenant`'s namespace. Existing
    /// namespaced copies are left alone; sources are never deleted.
    #[cfg(feature = "s3")]
    pub async fn migrate_legacy_object_keys(
        &self,
        bucket: &str,
        tenant: &str,
        prefix: &str,
        limit: usize,
        dry_run: bool,
    ) -> Result<StorageLayoutReport, tonic::Status> {
        if uuid::Uuid::parse_str(tenant.trim()).is_err() {
            return Err(crate::runtime::executor_utils::invalid_argument_fields(
                "name the tenant that owned the legacy objects",
                [("tenant", "must be the canonical tenant UUID")],
            ));
        }
        let mut report = self.scan_legacy_object_keys(bucket, prefix, limit).await?;
        report.dry_run = dry_run;
        if dry_run {
            return Ok(report);
        }
        let client = self.s3_for_instance_for_project(None, "")?.clone();
        for key in &report.legacy_keys {
            let target = namespaced_object_key(tenant, key);
            let existing = client
                .head_object()
                .bucket(bucket)
                .key(&target)
                .send()
                .await;
            match existing {
                Ok(_) => {
                    report.already_present += 1;
                    continue;
                }
                Err(err)
                    if err
                        .as_service_error()
                        .is_some_and(|error| error.is_not_found()) => {}
                Err(err) => {
                    return Err(crate::runtime::executor_utils::backend_transport_status(
                        "S3",
                        "head_object",
                        err,
                    ));
                }
            }
            client
                .copy_object()
                .bucket(bucket)
                .key(&target)
                .copy_source(format!("{bucket}/{}", urlencoding_path(key)))
                .send()
                .await
                .map_err(|err| {
                    crate::runtime::executor_utils::backend_transport_status(
                        "S3",
                        "copy_object",
                        err,
                    )
                })?;
            report.copied += 1;
        }
        Ok(report)
    }

    /// Startup check: warn (or, with `UDB_STORAGE_LAYOUT_STRICT=true`, refuse)
    /// when the default storage bucket still holds legacy keys.
    #[cfg(feature = "s3")]
    pub async fn check_storage_layout_at_startup(&self) -> Result<(), String> {
        let strict = std::env::var("UDB_STORAGE_LAYOUT_STRICT")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        if let Err(err) = self.s3_for_instance_for_project(None, "") {
            return if strict {
                Err(format!("cannot verify storage layout: {err}"))
            } else {
                Ok(())
            };
        }
        let (_, bucket) = crate::runtime::service::storage_object_defaults_pub(
            std::env::var("UDB_STORAGE_OBJECT_BACKEND").ok(),
            std::env::var("UDB_STORAGE_BUCKET").ok(),
        );
        let report = match self.scan_legacy_object_keys(&bucket, "", 5).await {
            Ok(report) => report,
            Err(err) => {
                if strict {
                    return Err(format!(
                        "cannot verify storage layout in bucket '{bucket}': {err}"
                    ));
                }
                tracing::debug!(bucket = %bucket, error = %err, "storage layout check skipped");
                return Ok(());
            }
        };
        if report.legacy_keys.is_empty() {
            return Ok(());
        }
        let message = format!(
            "bucket '{bucket}' holds objects written before tenant-namespaced keys (e.g. '{}'); \
             the object RPCs no longer find them. Copy them under their tenant with \
             `udb migrate storage --bucket {bucket} --tenant <uuid>`",
            report.legacy_keys[0]
        );
        if strict {
            return Err(message);
        }
        tracing::warn!("{message}");
        Ok(())
    }
}

/// Percent-encode an S3 key for `CopySource` (path segments kept).
#[cfg(feature = "s3")]
fn urlencoding_path(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod storage_layout_tests {
    use super::{is_legacy_object_key, namespaced_object_key};

    #[test]
    fn classifies_and_namespaces_keys() {
        assert!(is_legacy_object_key("docs/a.pdf"));
        assert!(!is_legacy_object_key("__udb_t/t1/docs/a.pdf"));
        assert!(!is_legacy_object_key("t:t1/p:p/docs/a.pdf"));
        assert!(!is_legacy_object_key(
            "8f7e8d75-0af5-443b-b253-a409faf9d22a/3c84bd25-9da7-44f8-9ad3-12f693611a0f/a.pdf"
        ));
        assert_eq!(
            namespaced_object_key(" t1 ", "/docs/a.pdf"),
            "__udb_t/t1/docs/a.pdf"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn copy_source_is_percent_encoded() {
        assert_eq!(super::urlencoding_path("a b/ü.txt"), "a%20b/%C3%BC.txt");
    }
}
