use sha2::{Digest, Sha256};

use crate::ast::{ProtoSchema, SourceSpan};

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
        schema.generic_stores.sort_by(|a, b| {
            (
                a.store_kind.as_str(),
                a.backend.as_str(),
                a.logical_name.as_str(),
                a.resource_name.as_str(),
            )
                .cmp(&(
                    b.store_kind.as_str(),
                    b.backend.as_str(),
                    b.logical_name.as_str(),
                    b.resource_name.as_str(),
                ))
        });
        schema.reserved_names.sort();
        schema
            .reserved_numbers
            .sort_by(|a, b| (a.start, a.end).cmp(&(b.start, b.end)));
        // Source positions are not semantically meaningful for drift — only the
        // declared (start, end) slots are. Zero the spans so a whitespace/line
        // move of a `reserved` declaration does not change the checksum.
        for range in &mut schema.reserved_numbers {
            range.span = SourceSpan::default();
        }
        schema.nested_enums.sort_by(|a, b| a.name.cmp(&b.name));
        for nested in &mut schema.nested_enums {
            // Enum values carry explicit numbers, so order is not semantic —
            // sort for a stable checksum regardless of declaration order.
            nested
                .values
                .sort_by(|a, b| a.number.cmp(&b.number).then_with(|| a.name.cmp(&b.name)));
        }
    }

    let json = serde_json::to_vec(&canonical)?;
    let digest = Sha256::digest(json);
    Ok(format!("{digest:x}"))
}
