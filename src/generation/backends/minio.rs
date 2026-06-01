//! MinIO / S3 bucket artifact generator.
//!
//! Produces one JSON artifact per `ManifestStore` whose `backend == "s3"` /
//! `"minio"` or `store_kind == "object"`, or whose options include a non-empty
//! `udb.bucket`.
//!
//! Each artifact is a UDB bucket-policy document that can be applied by the
//! MinIO / S3 apply engine.  The `_udb_meta` block embeds the proto manifest
//! checksum for drift detection.
//!
//! Supported `ManifestStoreOption` keys:
//!
//! | Key | Default | Description |
//! |-----|---------|-------------|
//! | `udb.bucket` | `<resource_name>` | Non-empty bucket name override |
//! | `udb.bucket_versioning` | `true` | Enable object versioning |
//! | `udb.bucket_lifecycle_days` | `7` | Expiry days for `tmp/` prefix |
//! | `udb.bucket_region` | `us-east-1` | AWS / MinIO region |

use serde_json::json;

use crate::generation::backend_safety::{
    generated_at_unix, store_opt_bool_any, store_opt_i64_any, store_opt_str_any,
};

use crate::generation::GeneratedArtifact;
use crate::generation::manifest::{CatalogManifest, ManifestStore};
use crate::generation::sql::SqlGenerationConfig;

/// Generate MinIO / S3 bucket artifacts from the proto AST.
pub fn generate_minio_artifacts(
    manifest: &CatalogManifest,
    _config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let checksum = &manifest.checksum_sha256;
    let ts = generated_at_unix();

    let mut out = Vec::new();
    for store in &manifest.stores {
        if !is_minio_store(store) {
            continue;
        }
        let bucket = bucket_name(store);
        let versioning = store_opt_bool_any(store, &["udb.bucket_versioning"], true);
        let lifecycle_days = store_opt_i64_any(store, &["udb.bucket_lifecycle_days"], 7);
        let region = store_opt_str_any(store, &["udb.bucket_region"]).unwrap_or("us-east-1");

        let body = json!({
            "_udb_meta": {
                "migration_kind": "bootstrap",
                "backend": "minio",
                "bucket": bucket,
                "proto_manifest_checksum": checksum,
                "generator": "udb",
                "generated_at": ts
            },
            "bucket": bucket,
            "region": region,
            "versioning": versioning,
            "lifecycle_rules": [
                {
                    "id": "expire-temp",
                    "prefix": "tmp/",
                    "expiration_days": lifecycle_days
                }
            ],
            "public_access_block": true,
            "tags": {
                "udb_managed": "true"
            }
        });

        let content = serde_json::to_string_pretty(&body)?;
        out.push(GeneratedArtifact {
            rel_path: format!("{bucket}.json"),
            kind: "bootstrap_minio".to_string(),
            schema: bucket.clone(),
            table: String::new(),
            content,
        });
    }
    Ok(out)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_minio_store(store: &ManifestStore) -> bool {
    store.backend == "s3"
        || store.backend == "minio"
        || store.store_kind == "object"
        || store_opt_str_any(store, &["udb.bucket"]).is_some()
}

fn bucket_name(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.bucket"])
        .map(ToString::to_string)
        .unwrap_or_else(|| {
            if !store.resource_name.is_empty() {
                store.resource_name.clone()
            } else {
                format!("{}-{}", store.owner_schema, store.owner_table)
                    .trim_start_matches('-')
                    .to_string()
            }
        })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestStore, ManifestStoreOption};

    fn make_store(resource: &str, backend: &str, opts: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: backend.to_string(),
            resource_name: resource.to_string(),
            store_kind: "object".to_string(),
            options: opts
                .iter()
                .map(|(k, v)| ManifestStoreOption {
                    key: k.to_string(),
                    value: v.to_string(),
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn minio_is_minio_store_s3_backend() {
        let store = ManifestStore {
            backend: "s3".to_string(),
            ..Default::default()
        };
        assert!(is_minio_store(&store));
    }

    #[test]
    fn minio_is_minio_store_by_bucket_option() {
        let store = ManifestStore {
            options: vec![ManifestStoreOption {
                key: "udb.bucket".to_string(),
                value: "my-bucket".to_string(),
            }],
            ..Default::default()
        };
        assert!(is_minio_store(&store));
    }

    #[test]
    fn minio_blank_bucket_option_does_not_classify_store() {
        let store = ManifestStore {
            options: vec![ManifestStoreOption {
                key: "udb.bucket".to_string(),
                value: "  ".to_string(),
            }],
            ..Default::default()
        };
        assert!(!is_minio_store(&store));
    }

    #[test]
    fn minio_bucket_name_from_option() {
        let store = make_store("documents", "s3", &[("udb.bucket", "prime-docs")]);
        assert_eq!(bucket_name(&store), "prime-docs");
    }

    #[test]
    fn minio_artifact_lifecycle_days() {
        let store = make_store("exports", "minio", &[("udb.bucket_lifecycle_days", "30")]);
        assert_eq!(
            store_opt_i64_any(&store, &["udb.bucket_lifecycle_days"], 7),
            30
        );
    }

    #[test]
    fn minio_versioning_default_true() {
        let store = make_store("raw", "s3", &[]);
        assert!(store_opt_bool_any(&store, &["udb.bucket_versioning"], true));
    }
}
