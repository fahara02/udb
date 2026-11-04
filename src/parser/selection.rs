use std::collections::{HashMap, HashSet};

use crate::ast::ProtoSchema;

use super::facade::ParserDiagnostic;
use super::paths::extract_proto_domain;

/// The diagnostic for a message dropped because another one declares its table.
pub const DUPLICATE_TABLE_DIAGNOSTIC_CODE: &str = "duplicate_table_name";

/// Keep one schema per physical table. Copies of one message across a tree (the
/// same package and message name) collapse quietly to the canonical copy. Two
/// DIFFERENT messages on one table are an authoring mistake: one of them would
/// silently have no table, so each dropped message also gets a diagnostic.
pub(super) fn dedupe_canonical_table_schemas(
    schemas: Vec<ProtoSchema>,
) -> (Vec<ProtoSchema>, Vec<ParserDiagnostic>) {
    let mut winners: HashMap<String, usize> = HashMap::new();
    for (idx, schema) in schemas.iter().enumerate() {
        if !schema.is_table {
            continue;
        }
        match winners.get(&table_key(schema)).copied() {
            None => {
                winners.insert(table_key(schema), idx);
            }
            Some(current) if prefer_proto_schema(schema, &schemas[current]) => {
                winners.insert(table_key(schema), idx);
            }
            _ => {}
        }
    }

    let diagnostics = schemas
        .iter()
        .enumerate()
        .filter(|(_, schema)| schema.is_table)
        .filter_map(|(idx, schema)| {
            let winner = &schemas[*winners.get(&table_key(schema))?];
            (idx != winners[&table_key(schema)]
                && message_identity(schema) != message_identity(winner))
            .then(|| ParserDiagnostic {
                file: schema.file.clone(),
                line: 1,
                column: 1,
                code: DUPLICATE_TABLE_DIAGNOSTIC_CODE.to_string(),
                message: format!(
                    "messages {} ({}) and {} ({}) declare the same table {}.{}; {} was ignored \
                     and has no table",
                    message_identity(schema),
                    schema.file,
                    message_identity(winner),
                    winner.file,
                    schema.schema_name,
                    schema.table_name,
                    message_identity(schema),
                ),
            })
        })
        .collect();

    let selected: HashSet<usize> = winners.values().copied().collect();
    let schemas = schemas
        .into_iter()
        .enumerate()
        .filter_map(|(idx, schema)| {
            if !schema.is_table || selected.contains(&idx) {
                Some(schema)
            } else {
                None
            }
        })
        .collect();
    (schemas, diagnostics)
}

fn table_key(schema: &ProtoSchema) -> String {
    format!(
        "{}.{}",
        schema.schema_name.to_lowercase(),
        schema.table_name.to_lowercase()
    )
}

fn message_identity(schema: &ProtoSchema) -> String {
    if schema.proto_package.is_empty() {
        schema.message_name.clone()
    } else {
        format!("{}.{}", schema.proto_package, schema.message_name)
    }
}

fn prefer_proto_schema(next: &ProtoSchema, current: &ProtoSchema) -> bool {
    let next_score = canonicality_score(next);
    let current_score = canonicality_score(current);
    if next_score != current_score {
        return next_score > current_score;
    }
    let next_richness = next.columns.len() + next.indexes.len() + next.foreign_keys.len();
    let current_richness =
        current.columns.len() + current.indexes.len() + current.foreign_keys.len();
    if next_richness != current_richness {
        return next_richness > current_richness;
    }
    next.file < current.file
}

fn canonicality_score(schema: &ProtoSchema) -> i32 {
    if extract_proto_domain(&schema.file) == schema.schema_name {
        100
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(package: &str, message: &str, file: &str) -> ProtoSchema {
        let mut schema = ProtoSchema::new(message);
        schema.proto_package = package.to_string();
        schema.schema_name = "notes".to_string();
        schema.table_name = "notes".to_string();
        schema.file = file.to_string();
        schema.is_table = true;
        schema
    }

    #[test]
    fn two_messages_on_one_table_keep_one_and_report_the_other() {
        let (kept, diagnostics) = dedupe_canonical_table_schemas(vec![
            table("acme.notes.v1", "Draft", "proto/b.proto"),
            table("acme.notes.v1", "Note", "proto/a.proto"),
        ]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].message_name, "Note");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, DUPLICATE_TABLE_DIAGNOSTIC_CODE);
        assert_eq!(diagnostics[0].file, "proto/b.proto");
        assert!(diagnostics[0].message.contains("acme.notes.v1.Draft"));
        assert!(diagnostics[0].message.contains("acme.notes.v1.Note"));
        assert!(diagnostics[0].message.contains("notes.notes"));
    }

    #[test]
    fn copies_of_one_message_collapse_quietly() {
        let (kept, diagnostics) = dedupe_canonical_table_schemas(vec![
            table("acme.notes.v1", "Note", "proto/a.proto"),
            table("acme.notes.v1", "Note", "vendor/a.proto"),
        ]);
        assert_eq!(kept.len(), 1);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    #[test]
    fn the_same_table_name_in_another_schema_is_legal() {
        let mut other = table("acme.drafts.v1", "Draft", "proto/b.proto");
        other.schema_name = "drafts".to_string();
        let (kept, diagnostics) = dedupe_canonical_table_schemas(vec![
            table("acme.notes.v1", "Note", "proto/a.proto"),
            other,
        ]);
        assert_eq!(kept.len(), 2);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }
}
