use std::collections::HashMap;

use crate::ast::{
    CacheStore, ColumnStore, DbExtension, DbTrigger, DocumentStore, GenericStore, GraphStore,
    MaterializedView, ModelRegistry, ProtoColumn, ProtoColumnSecurity, ProtoForeignKey, ProtoIndex,
    ProtoSchema, ProtoSecurity, RlsPolicy, SqlArtifact, StorageField, StoreOption, TimeSeriesStore,
    VectorStore,
};

use super::naming::{
    normalize_backend, normalize_enum, normalize_index_type, normalize_referential_action,
    normalize_store_kind, parse_bool, parse_i32, to_snake_case,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OptionKind {
    Table,
    Column,
    VectorStore,
    GraphStore,
    DocumentStore,
    TimeSeriesStore,
    Cache,
    Storage,
    Security,
    ModelRegistry,
    ColumnStore,
    SqlStore,
    NoSqlStore,
    GenericStore,
    ObjectStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OptionValue {
    Scalar(String),
    Block(HashMap<String, Vec<OptionValue>>),
}

pub(super) fn option_kind(option_name: &str, namespace: &str) -> Option<OptionKind> {
    let option_name = normalize_option_name(option_name);
    let namespace = namespace.trim().trim_matches('.');
    let needle = |name: &str| {
        if namespace.is_empty() {
            option_name == name || option_name.ends_with(&format!(".{name}"))
        } else {
            option_name == format!("{namespace}.{name}")
        }
    };
    if needle("table") || needle("pg_table") {
        Some(OptionKind::Table)
    } else if needle("vector_store") {
        Some(OptionKind::VectorStore)
    } else if needle("graph_store") {
        Some(OptionKind::GraphStore)
    } else if needle("document_store") {
        Some(OptionKind::DocumentStore)
    } else if needle("nosql_store") || needle("no_sql_store") || needle("kv_store") {
        Some(OptionKind::NoSqlStore)
    } else if needle("timeseries_store") || needle("time_series_store") {
        Some(OptionKind::TimeSeriesStore)
    } else if needle("column_store") || needle("wide_column_store") || needle("columnar_store") {
        Some(OptionKind::ColumnStore)
    } else if needle("cache") {
        Some(OptionKind::Cache)
    } else if needle("storage") {
        Some(OptionKind::Storage)
    } else if needle("object_store") || needle("blob_store") {
        Some(OptionKind::ObjectStore)
    } else if needle("security") {
        Some(OptionKind::Security)
    } else if needle("model_registry") {
        Some(OptionKind::ModelRegistry)
    } else if needle("sql_store") || needle("relational_store") {
        Some(OptionKind::SqlStore)
    } else if needle("data_store") || needle("external_store") || needle("search_store") {
        Some(OptionKind::GenericStore)
    } else if needle("column") || needle("pg_column") {
        Some(OptionKind::Column)
    } else {
        None
    }
}

fn normalize_option_name(option_name: &str) -> String {
    option_name
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_start_matches('.')
        .to_string()
}

pub(super) fn apply_table_values(
    schema: &mut ProtoSchema,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "table_name" => schema.table_name = value,
            "schema_name" => schema.declared_schema_name = value,
            "migration_order" => schema.migration_order = parse_i32(&value),
            "is_table" => schema.is_table = parse_bool(&value),
            "comment" => schema.table_comment = value,
            "soft_delete" => schema.soft_delete = parse_bool(&value),
            "soft_delete_column" => schema.soft_delete_column = to_snake_case(&value),
            "audit_fields" => schema.audit_fields = parse_bool(&value),
            "enable_rls" => schema.enable_rls = parse_bool(&value),
            "force_rls" => schema.force_rls = parse_bool(&value),
            "unlogged" => schema.unlogged = parse_bool(&value),
            "tablespace" => schema.tablespace = value,
            "partition_strategy" => schema.partition_strategy = normalize_enum(&value),
            "partition_by" => schema.partition_strategy = normalize_enum(&value),
            "partition_column" => schema.partition_column = value,
            "retention_days" => schema.retention_days = parse_i32(&value),
            "partition_interval" => schema.partition_interval = normalize_enum(&value),
            "partition_premake" => schema.partition_premake = parse_i32(&value),
            "partition_default" => schema.partition_default = parse_bool(&value),
            "partition_retention_months" => schema.partition_retention_months = parse_i32(&value),
            "replica_hint" => schema.replica_hint = normalize_enum(&value).to_ascii_lowercase(),
            "cdc_topic" => schema.cdc_topic = value.trim().to_string(),
            "required_scope" => schema.required_scope = value.trim().to_string(),
            "previous_table_name" => schema.previous_table_name = to_snake_case(&value),
            "allow_drop" => schema.allow_drop = parse_bool(&value),
            _ => {}
        }
    }

    for (key, list) in values {
        match key.as_str() {
            "indexes" | "index" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.indexes.push(index_from_values(None, block));
                    }
                }
            }
            "foreign_keys" | "foreign_key" | "fk" | "fks" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.foreign_keys.push(foreign_key_from_values("", block));
                    }
                }
            }
            "rls_policies" | "rls_policy" | "policy" | "policies" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.rls_policies.push(rls_policy_from_values(block));
                    }
                }
            }
            "extensions" | "extension" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.extensions.push(extension_from_values(block));
                    }
                }
            }
            "materialized_views" | "materialized_view" | "matview" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema
                            .materialized_views
                            .push(materialized_view_from_values(block));
                    }
                }
            }
            "triggers" | "trigger" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.triggers.push(trigger_from_values(block));
                    }
                }
            }
            "sql_artifacts" | "sql_artifact" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.sql_artifacts.push(sql_artifact_from_values(block));
                    }
                }
            }
            "vector_store" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.vector_store = Some(vector_from_values(block));
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn apply_column_values(
    column: &mut ProtoColumn,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    for (key, list) in values {
        if matches!(key.as_str(), "foreign_key" | "index") {
            continue;
        }
        for value in list {
            if let OptionValue::Scalar(value) = value {
                apply_column_value(column, key, value);
            }
        }
    }

    for (key, list) in values {
        match key.as_str() {
            "foreign_key" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        column.foreign_key =
                            Some(foreign_key_from_values(column.column_name.as_str(), block));
                    }
                }
            }
            "index" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        column
                            .indexes
                            .push(index_from_values(Some(column.column_name.as_str()), block));
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn apply_column_value(column: &mut ProtoColumn, key: &str, value: &str) {
    match key {
        "column_name" => column.column_name = to_snake_case(value),
        "sql_type" => column.sql_type = value.to_string(),
        "not_null" => column.not_null = parse_bool(value),
        "unique" => column.unique = parse_bool(value),
        "primary_key" | "is_primary_key" => column.is_primary = parse_bool(value),
        "auto_increment" => column.auto_increment = parse_bool(value),
        "default_value" => column.default_value = value.to_string(),
        "check_constraint" => column.check_constraint = value.to_string(),
        "collation" => column.collation = value.to_string(),
        "enum_values" | "enum_value" => column.enum_values.push(value.to_string()),
        "comment" => column.comment = value.to_string(),
        "exclude_from_insert" => column.exclude_from_insert = parse_bool(value),
        "exclude_from_update" => column.exclude_from_update = parse_bool(value),
        "encrypted" | "encrypt" => column.encrypted = parse_bool(value),
        "is_json" => column.is_json = parse_bool(value),
        "is_jsonb" => column.is_jsonb = parse_bool(value),
        "json_path_ops" => column.json_path_ops = parse_bool(value),
        "is_tsvector" => column.is_tsvector = parse_bool(value),
        "tsvector_language" => column.tsvector_language = value.to_string(),
        "tsvector_source_columns" | "tsvector_source_column" => {
            column.tsvector_source_columns.push(value.to_string())
        }
        "trigram_index" => column.trigram_index = parse_bool(value),
        "references" | "foreign_key" => column.references = value.to_string(),
        "tenant_column" => {}
        "on_delete" => column.on_delete = normalize_referential_action(value),
        "on_update" => column.on_update = normalize_referential_action(value),
        "previous_column_name" => column.previous_column_name = to_snake_case(value),
        "backfill_sql" => column.backfill_sql = value.to_string(),
        "using_expression" => column.using_expression = value.to_string(),
        "allow_drop" => column.allow_drop = parse_bool(value),
        "generated" => column.generated = parse_bool(value),
        "generated_expr" => column.generated_expr = value.to_string(),
        "identity" | "is_identity" => column.is_identity = parse_bool(value),
        _ => {}
    }
}

fn foreign_key_from_values(
    local_column: &str,
    values: &HashMap<String, Vec<OptionValue>>,
) -> ProtoForeignKey {
    let mut fk = ProtoForeignKey::default();
    if !local_column.trim().is_empty() {
        fk.columns.push(local_column.to_string());
    }
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "columns" | "column" | "local_column" | "local_columns" => {
                fk.columns.push(to_snake_case(&value))
            }
            "references_table" => fk.ref_table = to_snake_case(&value),
            "references_column" | "references_columns" => {
                fk.ref_columns.push(to_snake_case(&value))
            }
            "references_schema" => fk.ref_schema = value.to_lowercase(),
            "on_delete" => fk.on_delete = normalize_referential_action(&value),
            "on_update" => fk.on_update = normalize_referential_action(&value),
            "constraint_name" => fk.name = value,
            "not_valid" => fk.not_valid = parse_bool(&value),
            "deferrable" => fk.deferrable = parse_bool(&value),
            "initially_deferred" => fk.initially_deferred = parse_bool(&value),
            _ => {}
        }
    }
    // Preserve parse-order so column[i] corresponds to ref_column[i].
    // A sort here would desync local columns from ref_columns, producing incorrect FK SQL.
    let mut seen = std::collections::HashSet::new();
    fk.columns
        .retain(|col| !col.trim().is_empty() && seen.insert(col.clone()));
    fk
}

pub(super) fn foreign_key_from_reference(column: &ProtoColumn) -> Option<ProtoForeignKey> {
    let reference = column.references.trim();
    if reference.is_empty() {
        return None;
    }
    let (target, ref_col) = if let Some(start) = reference.find('(') {
        let end = reference[start + 1..]
            .find(')')
            .map(|idx| start + 1 + idx)
            .unwrap_or(reference.len());
        (&reference[..start], reference[start + 1..end].trim())
    } else {
        (reference, "")
    };
    let parts = target
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (ref_schema, ref_table) = match parts.as_slice() {
        [table] => ("", *table),
        [schema, table] => (*schema, *table),
        _ => return None,
    };
    let ref_column = if ref_col.is_empty() {
        column.column_name.as_str()
    } else {
        ref_col
    };
    Some(ProtoForeignKey {
        columns: vec![column.column_name.clone()],
        ref_schema: ref_schema.to_ascii_lowercase(),
        ref_table: to_snake_case(ref_table),
        ref_columns: vec![to_snake_case(ref_column)],
        on_delete: column.on_delete.clone(),
        on_update: column.on_update.clone(),
        ..ProtoForeignKey::default()
    })
}

fn rls_policy_from_values(values: &HashMap<String, Vec<OptionValue>>) -> RlsPolicy {
    let mut policy = RlsPolicy {
        permissive: true,
        ..RlsPolicy::default()
    };
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "policy_name" | "name" => policy.name = value,
            "command" => policy.command = normalize_enum(&value),
            "using" | "using_expression" => policy.using_expression = value,
            "with_check" | "check_expression" => policy.with_check = value,
            "permissive" => policy.permissive = parse_bool(&value),
            _ => {}
        }
    }
    policy
}

pub(super) fn apply_schema_security_values(
    security: &mut ProtoSecurity,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "classification_level" => security.classification_level = normalize_enum(&value),
            "audit_writes" => security.audit_writes = parse_bool(&value),
            "audit_reads" => security.audit_reads = parse_bool(&value),
            "retention_days" => security.retention_days = parse_i32(&value),
            "encryption_required" => security.encryption_required = parse_bool(&value),
            _ => {}
        }
    }
}

pub(super) fn apply_column_security_values(
    security: &mut ProtoColumnSecurity,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "is_pii" => security.is_pii = parse_bool(&value),
            "pii_kind" => {
                security.is_pii = !value.trim().is_empty();
                security.data_class = normalize_enum(&value);
            }
            "is_encrypted" => security.is_encrypted = parse_bool(&value),
            "is_blind_index" => security.is_blind_index = parse_bool(&value),
            "mask_in_logs" => security.mask_in_logs = parse_bool(&value),
            "data_class" => security.data_class = normalize_enum(&value),
            "consent_required" => security.consent_required = parse_bool(&value),
            "retention_days" => security.retention_days = parse_i32(&value),
            _ => {}
        }
    }
}

fn extension_from_values(values: &HashMap<String, Vec<OptionValue>>) -> DbExtension {
    let mut out = DbExtension::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "name" | "extension_name" => out.name = value,
            "schema" | "schema_name" => out.schema = value,
            "version" => out.version = value,
            _ => {}
        }
    }
    out
}

fn materialized_view_from_values(values: &HashMap<String, Vec<OptionValue>>) -> MaterializedView {
    let mut out = MaterializedView::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "name" | "view_name" => out.name = to_snake_case(&value),
            "schema" | "schema_name" => out.schema = value.to_ascii_lowercase(),
            "query" | "sql" => out.query = value,
            "with_data" => out.with_data = parse_bool(&value),
            _ => {}
        }
    }
    out
}

fn trigger_from_values(values: &HashMap<String, Vec<OptionValue>>) -> DbTrigger {
    let mut out = DbTrigger {
        for_each: "ROW".to_string(),
        ..DbTrigger::default()
    };
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "name" | "trigger_name" => out.name = to_snake_case(&value),
            "event" => out.event = normalize_enum(&value),
            "timing" => out.timing = normalize_enum(&value),
            "function" | "function_name" => out.function = value,
            "for_each" => out.for_each = normalize_enum(&value),
            "when" | "when_clause" => out.when_clause = value,
            _ => {}
        }
    }
    out
}

fn sql_artifact_from_values(values: &HashMap<String, Vec<OptionValue>>) -> SqlArtifact {
    let mut out = SqlArtifact::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "name" | "artifact_name" => out.name = to_snake_case(&value),
            "backend" => out.backend = normalize_backend(&value),
            "phase" => out.phase = value.to_ascii_lowercase(),
            "sql" | "body" => out.sql = value,
            "file" | "path" => out.file = value,
            "checksum_sha256" | "sha256" => out.checksum_sha256 = value,
            "requires_review" => out.requires_review = parse_bool(&value),
            _ => {}
        }
    }
    out
}

fn index_from_values(
    local_column: Option<&str>,
    values: &HashMap<String, Vec<OptionValue>>,
) -> ProtoIndex {
    let mut index = ProtoIndex {
        columns: local_column
            .map(|col| vec![col.to_string()])
            .unwrap_or_default(),
        index_type: "BTREE".to_string(),
        ..ProtoIndex::default()
    };
    let local = local_column.unwrap_or_default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "index_name" => index.name = value,
            "index_type" => index.index_type = normalize_index_type(&value),
            "unique" => index.unique = parse_bool(&value),
            "composite_fields" | "columns" | "column" => {
                // composite_fields entries may be:
                //   (a) simple column names: "col_a, col_b" or separate repeated fields
                //   (b) SQL expressions with function calls that contain commas
                //       themselves: "date_trunc('day', usage_date)"
                // Only split on commas that are at top-level (depth 0),
                // i.e. not inside parentheses or single-quoted strings.
                for part in split_top_level_commas(&value) {
                    let part = part.trim();
                    if part.is_empty() {
                        continue;
                    }
                    // Preserve SQL expressions as-is; only snake_case plain identifiers.
                    let col = if part.contains('(') || part.contains('\'') {
                        part.to_string()
                    } else {
                        to_snake_case(part)
                    };
                    if !col.is_empty() && col != local {
                        index.columns.push(col);
                    }
                }
            }
            "include_columns" => index.include_columns.push(to_snake_case(&value)),
            "index_method" => index.index_method = value,
            "where_clause" => index.where_clause = value,
            "operator_class" => index.operator_class = value,
            "concurrent" | "concurrently" => index.concurrent = parse_bool(&value),
            _ => {}
        }
    }
    for (key, list) in values {
        if key == "index_params" || key == "index_param" {
            for value in list {
                if let OptionValue::Block(block) = value {
                    let param_key = first_scalar(block, "key");
                    let param_value = first_scalar(block, "value");
                    if !param_key.trim().is_empty() {
                        index.index_params.push(StoreOption {
                            key: param_key,
                            value: param_value,
                        });
                    }
                }
            }
        }
    }
    index
}

/// Split `s` on commas that are at nesting depth 0 (not inside parentheses
/// or single-quoted strings). This lets composite_fields entries contain SQL
/// function calls such as `date_trunc('day', requested_at)` without being
/// incorrectly split at the comma inside the function call.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Toggle single-quote context (naively; good enough for identifiers).
                in_single_quote = !in_single_quote;
            }
            b'(' if !in_single_quote => depth += 1,
            b')' if !in_single_quote => depth -= 1,
            b',' if !in_single_quote && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

fn first_scalar(values: &HashMap<String, Vec<OptionValue>>, key: &str) -> String {
    values
        .get(key)
        .and_then(|list| {
            list.iter().find_map(|value| match value {
                OptionValue::Scalar(value) => Some(value.clone()),
                OptionValue::Block(_) => None,
            })
        })
        .unwrap_or_default()
}

pub(super) fn vector_from_values(values: &HashMap<String, Vec<OptionValue>>) -> VectorStore {
    let mut out = VectorStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "collection_name" => out.collection_name = value,
            "dimension" => out.dimension = parse_i32(&value),
            "distance" => out.distance = normalize_enum(&value),
            "shard_count" => out.shard_count = parse_i32(&value),
            "replica_count" => out.replica_count = parse_i32(&value),
            "on_disk" => out.on_disk = parse_bool(&value),
            "payload_schema_json" => out.payload_schema_json = value,
            "hnsw_m" => out.hnsw_m = parse_i32(&value),
            "hnsw_ef_construction" => out.hnsw_ef_construction = parse_i32(&value),
            _ => {}
        }
    }
    out
}

pub(super) fn graph_from_values(values: &HashMap<String, Vec<OptionValue>>) -> GraphStore {
    let mut out = GraphStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "graph_name" => out.graph_name = value,
            "node_label" => out.node_label = value,
            "id_field" => out.id_field = to_snake_case(&value),
            "tenant_field" => out.tenant_field = to_snake_case(&value),
            "edge_source_field" => out.edge_source_field = to_snake_case(&value),
            "edge_target_field" => out.edge_target_field = to_snake_case(&value),
            "payload_schema_json" => out.payload_schema_json = value,
            _ => {}
        }
    }
    out
}

pub(super) fn document_store_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> DocumentStore {
    let mut out = DocumentStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "database_name" => out.database_name = value,
            "collection_name" => out.collection_name = value,
            "partition_key" => out.partition_key = to_snake_case(&value),
            "id_field" => out.id_field = to_snake_case(&value),
            "tenant_field" => out.tenant_field = to_snake_case(&value),
            "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
            "payload_schema_json" => out.payload_schema_json = value,
            _ => {}
        }
    }
    out
}

pub(super) fn timeseries_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> TimeSeriesStore {
    let mut out = TimeSeriesStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "database_name" => out.database_name = value,
            "measurement_name" => out.measurement_name = value,
            "time_field" => out.time_field = to_snake_case(&value),
            "tenant_field" => out.tenant_field = to_snake_case(&value),
            "tag_fields" => out.tag_fields.push(to_snake_case(&value)),
            "value_fields" => out.value_fields.push(to_snake_case(&value)),
            "retention_days" => out.retention_days = parse_i32(&value),
            "downsample_policy" => out.downsample_policy = value,
            _ => {}
        }
    }
    out
}

pub(super) fn cache_from_values(values: &HashMap<String, Vec<OptionValue>>) -> CacheStore {
    let mut out = CacheStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "key_pattern" => out.key_pattern = value,
            "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
            "write_through" => out.write_through = parse_bool(&value),
            "read_through" => out.read_through = parse_bool(&value),
            "eviction_policy" => out.eviction_policy = value,
            "cluster_env_key" => out.cluster_env_key = value,
            "namespace" => out.namespace = value,
            _ => {}
        }
    }
    out
}

pub(super) fn storage_from_values(values: &HashMap<String, Vec<OptionValue>>) -> StorageField {
    let mut out = StorageField::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "bucket_env_key" => out.bucket_env_key = value,
            "key_prefix" => out.key_prefix = value,
            "presigned_read" => out.presigned_read = parse_bool(&value),
            "presigned_write" => out.presigned_write = parse_bool(&value),
            "presigned_ttl_seconds" => out.presigned_ttl_seconds = parse_i32(&value),
            "server_side_encryption" => out.server_side_encryption = parse_bool(&value),
            "kms_key_id" => out.kms_key_id = value,
            "acl" => out.acl = value,
            _ => {}
        }
    }
    out
}

pub(super) fn model_registry_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> ModelRegistry {
    let mut out = ModelRegistry::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "experiment_name" => out.experiment_name = value,
            "artifact_path" => out.artifact_path = value,
            "auto_register" => out.auto_register = parse_bool(&value),
            "stage" => out.stage = value,
            "metric_keys" => out.metric_keys.push(value),
            "param_keys" => out.param_keys.push(value),
            "storage_uri_env" => out.storage_uri_env = value,
            _ => {}
        }
    }
    out
}

pub(super) fn column_store_from_values(values: &HashMap<String, Vec<OptionValue>>) -> ColumnStore {
    let mut out = ColumnStore::default();
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "backend" => out.backend = normalize_enum(&value),
            "database_name" => out.database_name = value,
            "table_name" => out.table_name = to_snake_case(&value),
            "partition_key" => out.partition_key = to_snake_case(&value),
            "sort_key" => out.sort_key = to_snake_case(&value),
            "compression" => out.compression = value,
            "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
            "payload_schema_json" => out.payload_schema_json = value,
            _ => {}
        }
    }
    out
}

pub(super) fn generic_store_from_values(
    default_kind: &str,
    values: &HashMap<String, Vec<OptionValue>>,
) -> GenericStore {
    let mut out = GenericStore {
        store_kind: default_kind.to_string(),
        ..GenericStore::default()
    };
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "store_kind" | "kind" => out.store_kind = normalize_store_kind(&value),
            "backend" | "driver" => out.backend = normalize_backend(&value),
            "logical_name" | "name" => out.logical_name = value,
            "database_name" | "database" => out.database_name = value,
            "namespace" | "schema_name" => out.namespace = value,
            "resource_name" | "table_name" | "collection_name" | "graph_name"
            | "measurement_name" | "bucket" => out.resource_name = value,
            "dsn_env_key" | "env_key" => out.dsn_env_key = value,
            "dsn" | "uri" => out.dsn = value,
            "payload_schema_json" | "options_json" => out.payload_schema_json = value,
            _ => out.options.push(StoreOption { key, value }),
        }
    }
    out
}

fn scalar_entries(values: &HashMap<String, Vec<OptionValue>>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (key, list) in values {
        for value in list {
            if let OptionValue::Scalar(value) = value {
                out.push((key.clone(), value.clone()));
            }
        }
    }
    out
}
