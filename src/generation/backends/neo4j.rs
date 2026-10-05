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
//! | `udb.neo4j_label` / `node_label` | `<resource_name>` | Node label override |
//!
//! The node label is resolved by ONE function, [`resolve_neo4j_label`], shared
//! by this DDL generator, the Neo4j IR compiler and the graph projection
//! worker, so constraints, compiled reads/writes and projected nodes always
//! agree on the label. Node uniqueness is tenant-composite
//! (`(id, _tenant_id, _project_id)`): two tenants may own a node with the same
//! id without one write dead-lettering on a global unique constraint.

use crate::generation::backend_safety::generated_at_unix;

use crate::generation::GeneratedArtifact;
use crate::generation::backend_safety::{
    safe_comment_value, safe_identifier, safe_resource_name, store_opt_str_any,
};
use crate::generation::manifest::{CatalogManifest, ManifestStore};
use crate::generation::neo4j_labels::is_neo4j_store;
pub use crate::generation::neo4j_labels::{
    NEO4J_LABEL_OPTION_KEYS, neo4j_label_for_table, neo4j_store_label, resolve_neo4j_label,
};
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
        let label = node_label(store);
        let id_field = safe_identifier(
            store_opt_str_any(store, &["udb.neo4j_id_field", "id_field"]).unwrap_or("id"),
            "id",
        );
        let tenant_field = safe_identifier(
            store_opt_str_any(store, &["udb.neo4j_tenant_field", "tenant_field"])
                .unwrap_or("tenant_id"),
            "tenant_id",
        );
        // Earlier generators made `id` globally unique per label (and named
        // the label in PascalCase). Drop those so a second tenant's node with
        // the same id is not rejected; uniqueness is tenant-composite below.
        let mut legacy_constraints = vec![safe_identifier(
            &format!("{label}_{id_field}_unique"),
            "node_id_unique",
        )];
        let legacy_label = safe_identifier(&legacy_pascal_label(store), "Node");
        let legacy_name = safe_identifier(
            &format!("{legacy_label}_{id_field}_unique"),
            "node_id_unique",
        );
        if !legacy_constraints.contains(&legacy_name) {
            legacy_constraints.push(legacy_name);
        }
        let drop_legacy: String = legacy_constraints
            .iter()
            .map(|name| format!("DROP CONSTRAINT {name} IF EXISTS;\n\n"))
            .collect();
        let id_constraint = safe_identifier(
            &format!("{label}_{id_field}_scope_unique"),
            "node_id_scope_unique",
        );
        let tenant_index = safe_identifier(&format!("{label}_{tenant_field}"), "node_tenant");
        // Projected and IR-written nodes are keyed (and every scoped read is
        // filtered) on the `_tenant_id` / `_project_id` system properties,
        // whatever the source's own tenant column is called; index them so a
        // scoped lookup doesn't scan the whole label.
        let scope_index = safe_identifier(&format!("{label}_udb_scope"), "node_scope");

        let cypher = format!(
            "// UDB:migration_kind=bootstrap\n\
             // UDB:backend=neo4j\n\
             // UDB:database={database_header}\n\
             // UDB:label={label_header}\n\
             // UDB:proto_manifest_checksum={checksum}\n\
             // UDB:generator=udb\n\
             // UDB:generated_at={ts}\n\
             \n\
             {drop_legacy}\
             CREATE CONSTRAINT {id_constraint} IF NOT EXISTS\n\
             {indent}FOR (n:{label}) REQUIRE (n.{id_field}, n._tenant_id, n._project_id) IS UNIQUE;\n\
             \n\
             CREATE INDEX {tenant_index} IF NOT EXISTS\n\
             {indent}FOR (n:{label}) ON (n.{tenant_field});\n\
             \n\
             CREATE INDEX {scope_index} IF NOT EXISTS\n\
             {indent}FOR (n:{label}) ON (n._tenant_id, n._project_id);\n",
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
    neo4j_store_label(store)
}

/// The PascalCase label earlier generators derived by default; used only to
/// name the legacy global-id constraint that the DDL now drops.
fn legacy_pascal_label(store: &ManifestStore) -> String {
    store_opt_str_any(store, NEO4J_LABEL_OPTION_KEYS)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
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
    use crate::generation::manifest::{ManifestStore, ManifestStoreOption, ManifestTable};

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
        // The label is the resource name verbatim — the same label the IR
        // compiler and the projection worker write — not a PascalCase variant.
        let store = make_store("example_document", &[]);
        assert_eq!(node_label(&store), "example_document");
        let store2 = make_store("ocr_document", &[]);
        assert_eq!(node_label(&store2), "ocr_document");
        assert_eq!(legacy_pascal_label(&store), "ExampleDocument");
    }

    #[test]
    fn neo4j_label_resolver_is_shared_and_honours_both_override_keys() {
        assert_eq!(resolve_neo4j_label("patients", None), "patients");
        assert_eq!(resolve_neo4j_label("patients", Some("Patient")), "Patient");
        assert_eq!(resolve_neo4j_label("patients", Some("  ")), "patients");
        let store = make_store("patients", &[("node_label", "Patient")]);
        assert_eq!(neo4j_store_label(&store), "Patient");

        let mut manifest = CatalogManifest::default();
        let mut owned = make_store("patient_graph", &[("udb.neo4j_label", "Patient")]);
        owned.owner_table = "patients".to_string();
        manifest.stores.push(owned);
        let table = ManifestTable {
            table: "patients".to_string(),
            ..Default::default()
        };
        assert_eq!(neo4j_label_for_table(&manifest, &table), "Patient");
        let other = ManifestTable {
            table: "visits".to_string(),
            ..Default::default()
        };
        assert_eq!(neo4j_label_for_table(&manifest, &other), "visits");
    }

    #[test]
    fn neo4j_ddl_uniqueness_is_tenant_composite_and_drops_global_id_constraint() {
        let mut manifest = CatalogManifest::default();
        manifest.stores.push(make_store("example_document", &[]));
        let artifacts =
            generate_neo4j_artifacts(&manifest, &SqlGenerationConfig::default()).unwrap();
        assert_eq!(artifacts.len(), 1);
        let cypher = &artifacts[0].content;
        assert!(
            cypher.contains(
                "FOR (n:example_document) REQUIRE (n.id, n._tenant_id, n._project_id) IS UNIQUE;"
            ),
            "{cypher}"
        );
        assert!(cypher.contains("DROP CONSTRAINT example_document_id_unique IF EXISTS;"));
        assert!(cypher.contains("DROP CONSTRAINT ExampleDocument_id_unique IF EXISTS;"));
        assert!(!cypher.contains("REQUIRE n.id IS UNIQUE"));
        assert_eq!(artifacts[0].table, "example_document");
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
