pub mod clickhouse;
pub mod minio;
pub mod mongodb;
pub mod neo4j;
pub mod qdrant;
pub mod redis_schema;
pub mod sql_ddl;

pub use clickhouse::generate_clickhouse_artifacts;
pub use minio::generate_minio_artifacts;
pub use mongodb::generate_mongodb_artifacts;
pub use neo4j::generate_neo4j_artifacts;
pub use qdrant::generate_qdrant_artifacts;
pub use redis_schema::generate_redis_artifacts;
pub use sql_ddl::{generate_mssql_artifacts, generate_mysql_artifacts, generate_sqlite_artifacts};

/// The spec a backend executor provisions a store from: the store as declared, plus the
/// fields that executor reads. Without them it creates a default shape and still reports
/// the resource as ensured.
pub fn store_ensure_spec(
    store: &crate::generation::manifest::ManifestStore,
    manifest: &crate::generation::manifest::CatalogManifest,
) -> serde_json::Value {
    let backend = store.backend.to_ascii_lowercase();
    let fields = if backend.contains("clickhouse") {
        clickhouse::ensure_spec(
            store,
            manifest.table(&store.owner_schema, &store.owner_table),
        )
    } else if backend.contains("mongo") {
        mongodb::ensure_spec(store)
    } else if backend.contains("neo4j") {
        neo4j::ensure_spec(store)
    } else {
        serde_json::json!({})
    };
    let mut spec = serde_json::to_value(store).unwrap_or_else(|_| serde_json::json!({}));
    if let (Some(spec), Some(fields)) = (spec.as_object_mut(), fields.as_object()) {
        spec.extend(fields.clone());
    }
    spec
}

#[cfg(test)]
mod ensure_spec_tests {
    use super::store_ensure_spec;
    use crate::generation::manifest::{
        CatalogManifest, ManifestColumn, ManifestStore, ManifestStoreOption, ManifestTable,
    };

    fn store(backend: &str, options: &[(&str, &str)]) -> ManifestStore {
        ManifestStore {
            backend: backend.to_string(),
            resource_name: "events".to_string(),
            owner_schema: "s".to_string(),
            owner_table: "events".to_string(),
            options: options
                .iter()
                .map(|(key, value)| ManifestStoreOption {
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            ..ManifestStore::default()
        }
    }

    fn manifest() -> CatalogManifest {
        CatalogManifest {
            tables: vec![ManifestTable {
                schema: "s".to_string(),
                table: "events".to_string(),
                primary_key: vec!["id".to_string()],
                columns: vec![ManifestColumn {
                    column_name: "id".to_string(),
                    sql_type: "TEXT".to_string(),
                    not_null: true,
                    ..ManifestColumn::default()
                }],
                ..ManifestTable::default()
            }],
            ..CatalogManifest::default()
        }
    }

    #[test]
    fn clickhouse_gets_the_declared_engine_order_and_columns() {
        let spec = store_ensure_spec(
            &store("clickhouse", &[("udb.ch_engine", "SummingMergeTree")]),
            &manifest(),
        );
        assert_eq!(spec["engine"], "SummingMergeTree");
        assert_eq!(spec["order_by"], "id");
        assert_eq!(spec["columns"][0]["name"], "id");
        assert!(spec["columns"][0]["type"].is_string());
    }

    #[test]
    fn mongodb_gets_its_indexes_and_ttl_in_the_executors_shape() {
        let spec = store_ensure_spec(
            &store("mongodb", &[("udb.mongo_ttl_seconds", "60")]),
            &manifest(),
        );
        let indexes = spec["indexes"].as_array().expect("indexes");
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[1]["expire_after_seconds"], 60);
    }

    #[test]
    fn neo4j_gets_the_declared_id_property() {
        let spec = store_ensure_spec(
            &store("neo4j", &[("udb.neo4j_id_field", "uuid")]),
            &manifest(),
        );
        assert_eq!(spec["constraint_property"], "uuid");
    }
}
