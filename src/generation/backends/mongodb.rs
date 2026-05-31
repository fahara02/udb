//! MongoDB collection artifact generator.
//!
//! Produces one JSON artifact per `ManifestStore` whose `backend == "mongodb"` or
//! `store_kind == "nosql"`, or whose options include `udb.mongo_database`.
//!
//! Each artifact is a UDB collection-policy document containing:
//! - `$jsonSchema` validator derived from the proto message fields
//! - Default indexes (`tenant_id`, `_id`)
//! - `_udb_meta` block for drift detection
//!
//! Supported `ManifestStoreOption` keys:
//!
//! | Key | Default | Description |
//! |-----|---------|-------------|
//! | `udb.mongo_database` | `example` | Database name override |
//! | `udb.mongo_json_schema` | `true` | Emit `$jsonSchema` validator |

use serde_json::{Value as Json, json};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::ast::ProtoSchema;
use crate::generation::GeneratedArtifact;
use crate::generation::backend_safety::{
    safe_comment_value, safe_resource_name, store_opt_bool_any, store_opt_i64_any,
    store_opt_str_any,
};
use crate::generation::manifest::{CatalogManifest, ManifestStore};
use crate::generation::sql::SqlGenerationConfig;

/// Generate MongoDB collection artifacts from the proto AST.
pub fn generate_mongodb_artifacts(
    schemas: &[ProtoSchema],
    _config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let manifest = CatalogManifest::from_schemas(schemas)?;
    let checksum = &manifest.checksum_sha256;
    let ts = generated_at_unix();

    let mut out = Vec::new();
    for store in &manifest.stores {
        if !is_mongo_store(store) {
            continue;
        }
        let database = safe_mongo_name(&mongo_database(store), "example");
        let collection = safe_mongo_name(&collection_name(store), "collection");
        let emit_schema =
            store_opt_bool_any(store, &["udb.mongo_json_schema", "json_schema"], true);
        let id_field = safe_mongo_field(
            store_opt_str_any(store, &["udb.mongo_id_field", "id_field"]).unwrap_or("_id"),
            "_id",
        );
        let tenant_field = safe_mongo_field(
            store_opt_str_any(store, &["udb.mongo_tenant_field", "tenant_field"])
                .unwrap_or("tenant_id"),
            "tenant_id",
        );
        let partition_key = store_opt_str_any(store, &["udb.mongo_partition_key", "partition_key"])
            .map(|value| safe_mongo_field(value, ""))
            .filter(|value| !value.is_empty());
        let ttl_seconds = store_opt_i64_any(store, &["udb.mongo_ttl_seconds", "ttl_seconds"], 0);

        let validator = if emit_schema {
            payload_schema_validator(store).unwrap_or_else(|| {
                json!({
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": [id_field.clone(), tenant_field.clone()],
                        "properties": {
                            (tenant_field.clone()): { "bsonType": "string" },
                            "created_at": { "bsonType": "date" }
                        }
                    }
                })
            })
        } else {
            json!({})
        };

        let mut indexes = vec![json!({
            "key": { (tenant_field.clone()): 1 },
            "name": format!("idx_{collection}_{tenant_field}"),
            "background": true
        })];
        if let Some(partition_key) = &partition_key {
            indexes.push(json!({
                "key": { (partition_key.clone()): 1 },
                "name": format!("idx_{collection}_{partition_key}"),
                "background": true
            }));
        }
        if ttl_seconds > 0 {
            indexes.push(json!({
                "key": { "created_at": 1 },
                "name": format!("idx_{collection}_ttl"),
                "expireAfterSeconds": ttl_seconds,
                "background": true
            }));
        }

        let body = json!({
            "_udb_meta": {
                "migration_kind": "bootstrap",
                "backend": "mongodb",
                "collection": safe_comment_value(&collection),
                "database": safe_comment_value(&database),
                "proto_manifest_checksum": checksum,
                "generator": "udb",
                "generated_at": ts
            },
            "database": database,
            "collection": collection,
            "validator": validator,
            "indexes": indexes
        });

        let content = serde_json::to_string_pretty(&body)?;
        out.push(GeneratedArtifact {
            rel_path: format!(
                "{}/{}.json",
                safe_resource_name(&database, "example"),
                safe_resource_name(&collection, "collection")
            ),
            kind: "bootstrap_mongodb".to_string(),
            schema: database.clone(),
            table: collection.clone(),
            content,
        });
    }
    Ok(out)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_mongo_store(store: &ManifestStore) -> bool {
    store.backend == "mongodb"
        || store.store_kind == "nosql"
        || store.options.iter().any(|o| {
            matches!(
                o.key.as_str(),
                "udb.mongo_database" | "database_name" | "collection_name" | "partition_key"
            )
        })
}

fn mongo_database(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.mongo_database", "database_name"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if !store.database_name.is_empty() {
                store.database_name.clone()
            } else {
                "example".to_string()
            }
        })
}

fn collection_name(store: &ManifestStore) -> String {
    if let Some(collection) = store_opt_str_any(store, &["udb.mongo_collection", "collection_name"])
    {
        collection.to_string()
    } else if !store.resource_name.is_empty() {
        store.resource_name.clone()
    } else {
        store.owner_table.clone()
    }
}

fn payload_schema_validator(store: &ManifestStore) -> Option<Json> {
    let raw = store.payload_schema_json.trim();
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str::<Json>(raw).ok().map(|schema| {
        if schema.get("$jsonSchema").is_some() {
            schema
        } else {
            json!({ "$jsonSchema": schema })
        }
    })
}

fn safe_mongo_name(value: &str, fallback: &str) -> String {
    let mut out = String::new();
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
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

fn safe_mongo_field(value: &str, fallback: &str) -> String {
    let out = safe_mongo_name(value, fallback);
    if out.contains('\0') || out.starts_with('$') {
        fallback.to_string()
    } else {
        out
    }
}

fn generated_at_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestStore, ManifestStoreOption};

    fn make_store(resource: &str, opts: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: "mongodb".to_string(),
            resource_name: resource.to_string(),
            store_kind: "nosql".to_string(),
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
    fn mongodb_is_mongo_store() {
        let s = ManifestStore {
            backend: "mongodb".to_string(),
            ..Default::default()
        };
        assert!(is_mongo_store(&s));
    }

    #[test]
    fn mongodb_is_nosql_store_kind() {
        let s = ManifestStore {
            store_kind: "nosql".to_string(),
            ..Default::default()
        };
        assert!(is_mongo_store(&s));
    }

    #[test]
    fn mongodb_database_from_option() {
        let store = make_store("docs", &[("udb.mongo_database", "example_db")]);
        assert_eq!(mongo_database(&store), "example_db");
    }

    #[test]
    fn mongodb_database_default() {
        let store = make_store("docs", &[]);
        assert_eq!(mongo_database(&store), "example");
    }

    #[test]
    fn mongodb_json_schema_disabled() {
        let store = make_store("raw_docs", &[("udb.mongo_json_schema", "false")]);
        assert!(!store_opt_bool_any(
            &store,
            &["udb.mongo_json_schema"],
            true
        ));
    }
}
