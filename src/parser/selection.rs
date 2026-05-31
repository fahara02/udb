use std::collections::{HashMap, HashSet};

use crate::ast::ProtoSchema;

use super::paths::extract_proto_domain;

pub(super) fn dedupe_canonical_table_schemas(schemas: Vec<ProtoSchema>) -> Vec<ProtoSchema> {
    let mut winners: HashMap<String, usize> = HashMap::new();
    for (idx, schema) in schemas.iter().enumerate() {
        if !schema.is_table {
            continue;
        }
        let key = format!(
            "{}.{}",
            schema.schema_name.to_lowercase(),
            schema.table_name.to_lowercase()
        );
        match winners.get(&key).copied() {
            None => {
                winners.insert(key, idx);
            }
            Some(current) if prefer_proto_schema(schema, &schemas[current]) => {
                winners.insert(key, idx);
            }
            _ => {}
        }
    }

    let selected: HashSet<usize> = winners.values().copied().collect();
    schemas
        .into_iter()
        .enumerate()
        .filter_map(|(idx, schema)| {
            if !schema.is_table || selected.contains(&idx) {
                Some(schema)
            } else {
                None
            }
        })
        .collect()
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
