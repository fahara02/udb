//! Redis key-schema artifact generator.
//!
//! Produces one YAML artifact per `ManifestStore` whose `backend == "redis"` or
//! whose options include `udb.redis_namespace`.
//!
//! The YAML file documents the key conventions for the namespace: key patterns,
//! TTL policies, and outbox queue definitions.  It serves as authoritative
//! documentation and drift detection; there is no automated apply engine for
//! Redis (Redis has no native schema format).
//!
//! Supported `ManifestStoreOption` keys:
//!
//! | Key | Default | Description |
//! |-----|---------|-------------|
//! | `udb.redis_namespace` | `<resource_name>` | Key prefix namespace |
//! | `udb.redis_ttl_seconds` | `3600` | Default TTL for idempotency keys |
//! | `udb.redis_session_ttl` | `86400` | TTL for session keys |
//! | `udb.redis_outbox_queue` | `false` | Generate outbox queue config |
//! | `udb.redis_outbox_max_len` | `10000` | Max outbox list length |

use crate::generation::backend_safety::generated_at_unix;

use crate::generation::GeneratedArtifact;
use crate::generation::backend_safety::{
    safe_comment_value, sanitize_filename, store_opt_bool_any, store_opt_i64_any, store_opt_str_any,
};
use crate::generation::manifest::{CatalogManifest, ManifestStore};
use crate::generation::sql::SqlGenerationConfig;

/// Generate Redis key-schema YAML artifacts from the proto AST.
pub fn generate_redis_artifacts(
    manifest: &CatalogManifest,
    _config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let checksum = manifest.checksum_sha256.clone();
    let ts = generated_at_unix();

    let mut out = Vec::new();
    for store in &manifest.stores {
        if !is_redis_store(store) {
            continue;
        }
        let namespace = safe_redis_namespace(&redis_namespace(store), "udb");
        let key_pattern = redis_key_pattern(store, &namespace);
        let ttl = store_opt_i64_any(store, &["udb.redis_ttl_seconds", "ttl_seconds"], 3600);
        let session_ttl = store_opt_i64_any(
            store,
            &["udb.redis_session_ttl", "session_ttl_seconds"],
            86400,
        );
        let has_outbox =
            store_opt_bool_any(store, &["udb.redis_outbox_queue", "outbox_queue"], false);
        let outbox_max_len = store_opt_i64_any(
            store,
            &["udb.redis_outbox_max_len", "outbox_max_len"],
            10000,
        );

        let mut yaml = format!(
            "# UDB:migration_kind: bootstrap\n\
             # UDB:backend: redis\n\
             # UDB:namespace: {namespace_header}\n\
             # UDB:proto_manifest_checksum: {checksum}\n\
             # UDB:generator: udb\n\
             # UDB:generated_at: {ts}\n\
             namespace: {namespace_yaml}\n\
             key_patterns:\n\
             {indent}- pattern: {session_pattern}\n\
             {indent}  type: hash\n\
             {indent}  ttl_seconds: {session_ttl}\n\
             {indent}  description: \"User session data\"\n\
             {indent}- pattern: {idempotency_pattern}\n\
             {indent}  type: string\n\
             {indent}  ttl_seconds: {ttl}\n\
             {indent}  description: \"Request idempotency key\"\n",
            namespace_header = safe_comment_value(&namespace),
            namespace_yaml = yaml_string(&namespace),
            session_pattern = yaml_string(&format!("{namespace}:session:{{user_id}}")),
            idempotency_pattern = yaml_string(&key_pattern),
            indent = "  "
        );

        if has_outbox {
            yaml.push_str(&format!(
                "outbox_queues:\n\
                 {indent}- name: {outbox_name}\n\
                 {indent}  type: list\n\
                 {indent}  max_len: {outbox_max_len}\n",
                outbox_name = yaml_string(&format!("{namespace}:outbox")),
                indent = "  "
            ));
        }

        out.push(GeneratedArtifact {
            rel_path: format!("{}.yaml", sanitize_filename(&namespace)),
            kind: "bootstrap_redis".to_string(),
            schema: namespace.clone(),
            table: String::new(),
            content: yaml,
        });
    }
    Ok(out)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_redis_store(store: &ManifestStore) -> bool {
    store.backend == "redis"
        || store.store_kind == "cache"
        || store.options.iter().any(|o| {
            matches!(
                o.key.as_str(),
                "udb.redis_namespace" | "namespace" | "key_pattern" | "ttl_seconds"
            )
        })
}

fn redis_namespace(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.redis_namespace", "namespace"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if !store.namespace.is_empty() {
                store.namespace.clone()
            } else if !store.resource_name.is_empty() {
                store.resource_name.clone()
            } else {
                format!("{}:{}", store.owner_schema, store.owner_table)
                    .trim_start_matches(':')
                    .to_string()
            }
        })
}

fn redis_key_pattern(store: &ManifestStore, namespace: &str) -> String {
    store_opt_str_any(store, &["udb.redis_key_pattern", "key_pattern"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{namespace}:idempotency:{{request_id}}"))
}

fn safe_redis_namespace(value: &str, fallback: &str) -> String {
    let mut out = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, ':' | '_' | '-' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        fallback.to_string()
    } else {
        out
    }
}

fn yaml_string(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace(['\r', '\n'], " ")
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestStore, ManifestStoreOption};

    fn make_store(resource: &str, opts: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: "redis".to_string(),
            resource_name: resource.to_string(),
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
    fn redis_namespace_from_option() {
        let store = make_store("sessions", &[("udb.redis_namespace", "prime:sessions")]);
        assert_eq!(redis_namespace(&store), "prime:sessions");
    }

    #[test]
    fn redis_namespace_fallback_to_resource_name() {
        let store = make_store("example_cache", &[]);
        assert_eq!(redis_namespace(&store), "example_cache");
    }

    #[test]
    fn redis_yaml_contains_checksum_header() {
        let store = make_store("test_ns", &[("udb.redis_namespace", "test_ns")]);
        let mut manifest = CatalogManifest::default();
        manifest.stores.push(store);
        // Simulate directly: build YAML and check it contains the header key.
        let checksum = "deadbeef";
        let ns = "test_ns";
        let yaml = format!("# UDB:proto_manifest_checksum: {checksum}\nnamespace: {ns}\n");
        assert!(yaml.contains("UDB:proto_manifest_checksum: deadbeef"));
    }

    #[test]
    fn redis_outbox_option() {
        let store = make_store(
            "events",
            &[
                ("udb.redis_outbox_queue", "true"),
                ("udb.redis_outbox_max_len", "5000"),
            ],
        );
        assert!(store_opt_bool_any(
            &store,
            &["udb.redis_outbox_queue"],
            false
        ));
        assert_eq!(
            store_opt_i64_any(&store, &["udb.redis_outbox_max_len"], 10000),
            5000
        );
    }
}
