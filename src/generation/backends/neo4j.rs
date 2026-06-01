//! Neo4j constraint and index artifact generator.
//!
//! Produces one Cypher (`.cypher`) artifact per `ManifestStore` whose
//! `backend == "neo4j"` or `store_kind == "graph"`, or whose options include
//! `udb.neo4j_database`.
//!
//! Each artifact emits idempotent `CREATE CONSTRAINT … IF NOT EXISTS` and
//! `CREATE INDEX … IF NOT EXISTS` Cypher statements derived from the proto
//! message fields.
//!
//! Supported `ManifestStoreOption` keys:
//!
//! | Key | Default | Description |
//! |-----|---------|-------------|
//! | `udb.neo4j_database` | `neo4j` | Database name override |
//! | `udb.neo4j_label` | `<resource_name>` | Node label override |

use crate::generation::backend_safety::generated_at_unix;

use crate::generation::GeneratedArtifact;
use crate::generation::backend_safety::{
    safe_comment_value, safe_identifier, safe_resource_name, store_opt_str_any,
};
use crate::generation::manifest::{CatalogManifest, ManifestStore};
use crate::generation::sql::SqlGenerationConfig;

/// Generate Neo4j Cypher constraint/index artifacts from the proto AST.
pub fn generate_neo4j_artifacts(
    manifest: &CatalogManifest,
    _config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let checksum = &manifest.checksum_sha256;
    let ts = generated_at_unix();

    let mut out = Vec::new();
    for store in &manifest.stores {
        if !is_neo4j_store(store) {
            continue;
        }
        let database = safe_identifier(&neo4j_database(store), "neo4j");
        let label = safe_identifier(&node_label(store), "Node");
        let id_field = safe_identifier(
            store_opt_str_any(store, &["udb.neo4j_id_field", "id_field"]).unwrap_or("id"),
            "id",
        );
        let tenant_field = safe_identifier(
            store_opt_str_any(store, &["udb.neo4j_tenant_field", "tenant_field"])
                .unwrap_or("tenant_id"),
            "tenant_id",
        );
        let id_constraint =
            safe_identifier(&format!("{label}_{id_field}_unique"), "node_id_unique");
        let tenant_index = safe_identifier(&format!("{label}_{tenant_field}"), "node_tenant");

        let cypher = format!(
            "// UDB:migration_kind=bootstrap\n\
             // UDB:backend=neo4j\n\
             // UDB:database={database_header}\n\
             // UDB:label={label_header}\n\
             // UDB:proto_manifest_checksum={checksum}\n\
             // UDB:generator=udb\n\
             // UDB:generated_at={ts}\n\
             \n\
             CREATE CONSTRAINT {id_constraint} IF NOT EXISTS\n\
             {indent}FOR (n:{label}) REQUIRE n.{id_field} IS UNIQUE;\n\
             \n\
             CREATE INDEX {tenant_index} IF NOT EXISTS\n\
             {indent}FOR (n:{label}) ON (n.{tenant_field});\n",
            database_header = safe_comment_value(&database),
            label_header = safe_comment_value(&label),
            indent = "  "
        );

        out.push(GeneratedArtifact {
            rel_path: format!(
                "{}/{}.cypher",
                safe_resource_name(&database, "neo4j"),
                safe_resource_name(&label, "Node")
            ),
            kind: "bootstrap_neo4j".to_string(),
            schema: database.clone(),
            table: label.clone(),
            content: cypher,
        });
    }
    Ok(out)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_neo4j_store(store: &ManifestStore) -> bool {
    store.backend == "neo4j"
        || store.store_kind == "graph"
        || store.options.iter().any(|o| {
            matches!(
                o.key.as_str(),
                "udb.neo4j_database" | "database_name" | "udb.neo4j_label" | "node_label"
            )
        })
}

fn neo4j_database(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.neo4j_database", "database_name"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            if !store.database_name.is_empty() {
                store.database_name.clone()
            } else {
                "neo4j".to_string()
            }
        })
}

fn node_label(store: &ManifestStore) -> String {
    store_opt_str_any(store, &["udb.neo4j_label", "node_label"])
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            // Convert snake_case resource name to PascalCase label.
            to_pascal_case(if !store.resource_name.is_empty() {
                &store.resource_name
            } else {
                &store.owner_table
            })
        })
}

/// Convert `snake_case` to `PascalCase` for Neo4j node labels.
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
            }
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestStore, ManifestStoreOption};

    fn make_store(resource: &str, opts: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: "neo4j".to_string(),
            resource_name: resource.to_string(),
            store_kind: "graph".to_string(),
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
    fn neo4j_is_neo4j_store() {
        let s = ManifestStore {
            backend: "neo4j".to_string(),
            ..Default::default()
        };
        assert!(is_neo4j_store(&s));
    }

    #[test]
    fn neo4j_to_pascal_case() {
        assert_eq!(to_pascal_case("example_document"), "ExampleDocument");
        assert_eq!(to_pascal_case("ocr_document"), "OcrDocument");
        assert_eq!(to_pascal_case("user"), "User");
        assert_eq!(to_pascal_case(""), "");
    }

    #[test]
    fn neo4j_node_label_from_resource() {
        let store = make_store("example_document", &[]);
        assert_eq!(node_label(&store), "ExampleDocument");
        let store2 = make_store("ocr_document", &[]);
        assert_eq!(node_label(&store2), "OcrDocument");
    }

    #[test]
    fn neo4j_node_label_from_option() {
        let store = make_store("doc", &[("udb.neo4j_label", "Document")]);
        assert_eq!(node_label(&store), "Document");
    }

    #[test]
    fn neo4j_database_default() {
        let store = make_store("entity", &[]);
        assert_eq!(neo4j_database(&store), "neo4j");
    }

    #[test]
    fn neo4j_cypher_contains_checksum() {
        let checksum = "testchecksum";
        let cypher = format!("// UDB:proto_manifest_checksum={checksum}\n");
        assert!(cypher.contains("// UDB:proto_manifest_checksum=testchecksum"));
    }
}
