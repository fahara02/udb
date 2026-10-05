//! Shared Neo4j label resolution for DDL, projections and portable IR compilers.

use super::backend_safety::{safe_identifier, store_opt_str_any};
use super::manifest::{CatalogManifest, ManifestStore, ManifestTable};

pub(super) fn is_neo4j_store(store: &ManifestStore) -> bool {
    store.backend == "neo4j"
        || store.store_kind == "graph"
        || store.options.iter().any(|o| {
            matches!(
                o.key.as_str(),
                "udb.neo4j_database" | "database_name" | "udb.neo4j_label" | "node_label"
            )
        })
}

/// Store option keys that override a graph store's node label (first wins).
pub const NEO4J_LABEL_OPTION_KEYS: &[&str] = &["udb.neo4j_label", "node_label"];

/// The label override, or resource name, sanitised to a plain identifier.
pub fn resolve_neo4j_label(resource_name: &str, label_override: Option<&str>) -> String {
    let raw = label_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| resource_name.trim());
    safe_identifier(raw, "Node")
}

/// Resolve a manifest store's label, falling back to its owner table.
pub fn neo4j_store_label(store: &ManifestStore) -> String {
    let resource = if store.resource_name.trim().is_empty() {
        &store.owner_table
    } else {
        &store.resource_name
    };
    resolve_neo4j_label(resource, store_opt_str_any(store, NEO4J_LABEL_OPTION_KEYS))
}

/// Resolve the owning graph store's label, falling back to the table name.
pub fn neo4j_label_for_table(manifest: &CatalogManifest, table: &ManifestTable) -> String {
    manifest
        .stores
        .iter()
        .filter(|store| is_neo4j_store(store))
        .find(|store| {
            (store.owner_table == table.table
                && (store.owner_schema.trim().is_empty() || store.owner_schema == table.schema))
                || store.resource_name == table.table
        })
        .map(neo4j_store_label)
        .unwrap_or_else(|| resolve_neo4j_label(&table.table, None))
}
