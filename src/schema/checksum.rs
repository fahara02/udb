use sha2::{Digest, Sha256};

use crate::ast::ProtoSchema;

pub fn schema_checksum(schemas: &[ProtoSchema]) -> Result<String, serde_json::Error> {
    let mut canonical = schemas.to_vec();
    canonical.sort_by(|a, b| {
        (
            a.schema_name.as_str(),
            a.table_name.as_str(),
            a.message_name.as_str(),
            a.file.as_str(),
        )
            .cmp(&(
                b.schema_name.as_str(),
                b.table_name.as_str(),
                b.message_name.as_str(),
                b.file.as_str(),
            ))
    });

    for schema in &mut canonical {
        // Sort every collection field so that the checksum is deterministic
        // regardless of the order in which proto options were declared.
        // Any collection not sorted here would produce a different hash for
        // two semantically identical schemas that differ only in declaration order,
        // causing spurious drift detection and unnecessary re-migrations.
        schema.columns.sort_by_key(|col| col.field_number);
        schema.indexes.sort_by(|a, b| a.name.cmp(&b.name));
        schema.foreign_keys.sort_by(|a, b| a.name.cmp(&b.name));
        schema.rls_policies.sort_by(|a, b| a.name.cmp(&b.name));
        schema.triggers.sort_by(|a, b| a.name.cmp(&b.name));
        schema
            .materialized_views
            .sort_by(|a, b| a.name.cmp(&b.name));
        schema.extensions.sort_by(|a, b| a.name.cmp(&b.name));
        schema.sql_artifacts.sort_by(|a, b| a.name.cmp(&b.name));
    }

    let json = serde_json::to_vec(&canonical)?;
    let digest = Sha256::digest(json);
    Ok(format!("{digest:x}"))
}
