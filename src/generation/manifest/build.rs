//! manifest.rs split — build (Phase I).
use super::*;

pub(crate) fn migrate_v1_to_v2(manifest: &mut CatalogManifest) {
    for store in &mut manifest.stores {
        if store.store_kind == "storage" || store.store_kind == "blob" {
            store.store_kind = "object".to_string();
        }
        if store.store_kind == "document" {
            store.store_kind = "nosql".to_string();
        }
        store.options.sort_by(|a, b| a.key.cmp(&b.key));
        store
            .options
            .dedup_by(|a, b| a.key == b.key && a.value == b.value);
    }
}

pub(crate) fn migrate_v2_to_v3(manifest: &mut CatalogManifest) {
    for table in &mut manifest.tables {
        table.columns.sort_by_key(|col| col.field_number);
        table.indexes.sort_by_key(index_key);
        table.foreign_keys.sort_by_key(fk_key);
        table.checks.sort_by_key(check_key);
    }
}

pub(crate) fn table_from_schema(schema: &ProtoSchema) -> ManifestTable {
    let mut columns: Vec<_> = schema.columns.iter().map(column_from_proto).collect();
    columns.sort_by_key(|col| col.field_number);
    if schema.audit_fields {
        append_missing_audit_columns(&mut columns);
    }

    let primary_key = columns
        .iter()
        .filter(|col| col.is_primary)
        .map(|col| col.column_name.clone())
        .collect();

    let mut indexes: Vec<_> = schema.indexes.iter().map(index_from_proto).collect();
    for column in &schema.columns {
        for index in &column.indexes {
            indexes.push(index_from_proto(index));
        }
    }
    indexes.sort_by_key(index_key);
    indexes.dedup_by(|a, b| index_key(a) == index_key(b));

    let mut foreign_keys: Vec<_> = schema.foreign_keys.iter().map(fk_from_proto).collect();
    for column in &schema.columns {
        if let Some(fk) = &column.foreign_key {
            foreign_keys.push(fk_from_proto(fk));
        }
    }
    foreign_keys.sort_by_key(fk_key);
    foreign_keys.dedup_by(|a, b| fk_key(a) == fk_key(b));

    let mut checks: Vec<_> = columns
        .iter()
        .filter(|col| !col.check_constraint.trim().is_empty())
        .map(|col| ManifestCheck {
            name: format!("chk_{}_{}", schema.table_name, col.column_name),
            expression: col.check_constraint.clone(),
        })
        .collect();
    for column in &columns {
        if column.check_constraint.trim().is_empty() && !column.enum_values.is_empty() {
            let mut values = column.enum_values.clone();
            values.sort();
            values.dedup();
            checks.push(ManifestCheck {
                name: format!("chk_{}_{}_enum", schema.table_name, column.column_name),
                expression: format!(
                    "{} IN ({})",
                    column.column_name,
                    values
                        .iter()
                        .map(|value| format!("'{}'", value.replace('\'', "''")))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
            });
        }
    }
    checks.sort_by(|a, b| a.expression.cmp(&b.expression));

    let mut rls_policies = schema
        .rls_policies
        .iter()
        .map(|policy| ManifestPolicy {
            name: policy.name.trim().to_string(),
            command: normalize_policy_command(&policy.command),
            using_expression: policy.using_expression.trim().to_string(),
            with_check: policy.with_check.trim().to_string(),
            permissive: policy.permissive,
        })
        .collect::<Vec<_>>();
    rls_policies.sort_by(|a, b| a.name.cmp(&b.name));

    let mut warnings = Vec::new();
    warnings.extend(validate_table_shape(
        &schema.schema_name,
        &schema.table_name,
        &columns,
    ));
    if schema.audit_fields {
        warnings.extend(validate_audit_fields(
            &schema.schema_name,
            &schema.table_name,
            &columns,
        ));
    }

    ManifestTable {
        message_name: schema.message_name.clone(),
        proto_package: schema.proto_package.clone(),
        php_namespace: schema.php_namespace.clone(),
        php_class_prefix: schema.php_class_prefix.clone(),
        php_class: php_class_name(
            &schema.php_namespace,
            &schema.php_class_prefix,
            &schema.message_name,
        ),
        php_metadata_namespace: schema.php_metadata_namespace.clone(),
        php_metadata_class: php_class_name(
            &schema.php_metadata_namespace,
            "",
            &format!("{}Entry", schema.message_name),
        ),
        // NW-universal: propagate every language option through the
        // manifest unchanged. Codegen consumers (Java/C#/Go/etc.)
        // read this map.
        language_options: schema.language_options.clone(),
        // NW-universal: derive the fully qualified class / type name
        // per language at manifest build time, so codegen consumers
        // never have to reapply separators.
        language_classes: build_language_classes(schema),
        // NW-universal: propagate reserved field numbers + names so
        // the migration safety checker can refuse new fields that
        // collide with reserved slots.
        reserved_numbers: schema
            .reserved_numbers
            .iter()
            .map(|r| ManifestReservedRange {
                start: r.start,
                end: r.end,
            })
            .collect(),
        reserved_names: schema.reserved_names.clone(),
        schema: normalize_ident_or(&schema.schema_name, "public"),
        table: normalize_ident(&schema.table_name),
        migration_order: schema.migration_order,
        columns,
        primary_key,
        indexes,
        foreign_keys,
        checks,
        partition_strategy: schema.partition_strategy.clone(),
        partition_column: normalize_ident(&schema.partition_column),
        partition_interval: schema.partition_interval.clone(),
        partition_premake: schema.partition_premake,
        partition_default: schema.partition_default,
        retention_days: schema.retention_days,
        replica_hint: normalize_replica_hint(&schema.replica_hint),
        cdc_topic: schema.cdc_topic.trim().to_string(),
        required_scope: schema.required_scope.trim().to_string(),
        enable_rls: schema.enable_rls,
        // force_rls (FORCE ROW LEVEL SECURITY) makes even the table owner subject
        // to RLS policies.  It must only be set when the proto explicitly requests
        // it.  Deriving it from enable_rls would silently promote every
        // enable_rls table to the far more restrictive FORCE setting.
        force_rls: schema.force_rls,
        rls_policies,
        soft_delete: schema.soft_delete,
        soft_delete_column: defaulted(&normalize_ident(&schema.soft_delete_column), "deleted_at"),
        audit_fields: schema.audit_fields,
        security: ManifestSecurity {
            classification_level: schema.security.classification_level.clone(),
            audit_writes: schema.security.audit_writes,
            audit_reads: schema.security.audit_reads,
            retention_days: schema.security.retention_days,
            encryption_required: schema.security.encryption_required,
        },
        unlogged: schema.unlogged,
        tablespace: schema.tablespace.trim().to_string(),
        extensions: schema
            .extensions
            .iter()
            .filter(|ext| !ext.name.trim().is_empty())
            .map(|ext| ManifestExtension {
                name: ext.name.trim().to_string(),
                schema: defaulted(&ext.schema, "public"),
                version: ext.version.trim().to_string(),
            })
            .collect(),
        materialized_views: schema
            .materialized_views
            .iter()
            .filter(|view| !view.name.trim().is_empty() && !view.query.trim().is_empty())
            .map(|view| ManifestMaterializedView {
                name: normalize_ident(&view.name),
                schema: normalize_ident_or(&view.schema, &schema.schema_name),
                query: view.query.trim().to_string(),
                with_data: view.with_data,
            })
            .collect(),
        triggers: schema
            .triggers
            .iter()
            .filter(|trigger| {
                !trigger.name.trim().is_empty() && !trigger.function.trim().is_empty()
            })
            .map(|trigger| ManifestTrigger {
                name: normalize_ident(&trigger.name),
                schema: normalize_ident_or(&schema.schema_name, "public"),
                table: normalize_ident(&schema.table_name),
                event: normalize_policy_command(&trigger.event),
                timing: normalize_policy_command(&trigger.timing),
                function: trigger.function.trim().to_string(),
                for_each: defaulted(&normalize_policy_command(&trigger.for_each), "ROW"),
                when_clause: trigger.when_clause.trim().to_string(),
            })
            .collect(),
        sql_artifacts: schema
            .sql_artifacts
            .iter()
            .filter(|artifact| {
                !artifact.name.trim().is_empty()
                    && (!artifact.sql.trim().is_empty() || !artifact.file.trim().is_empty())
            })
            .map(|artifact| ManifestSqlArtifact {
                name: normalize_ident(&artifact.name),
                backend: artifact.backend.trim().to_string(),
                phase: artifact.phase.trim().to_ascii_lowercase(),
                sql: artifact.sql.trim().to_string(),
                file: artifact.file.trim().to_string(),
                checksum_sha256: artifact.checksum_sha256.trim().to_string(),
                requires_review: artifact.requires_review,
            })
            .collect(),
        comment: schema.table_comment.clone(),
        source_file: schema.file.clone(),
        previous_table_name: normalize_ident(&schema.previous_table_name),
        allow_drop: schema.allow_drop,
        warnings,
        ..ManifestTable::default()
    }
}

fn php_class_name(namespace: &str, prefix: &str, message_name: &str) -> String {
    let class = format!("{prefix}{message_name}");
    if namespace.trim().is_empty() {
        class
    } else {
        format!("{}\\{}", namespace.trim_matches('\\'), class)
    }
}

/// NW-universal: derive `(language_short_name → fully_qualified_class)`
/// for every language the schema declared a namespace / package /
/// prefix for. Replaces the pre-fix pattern of "PHP-only class name
/// derivation in `php_class_name`". Each language's separator is
/// applied via `ProtoSchema::fully_qualified_name`.
///
/// Example: with `option java_package = "com.acme.billing";` plus
/// `option csharp_namespace = "Acme.Billing";` plus
/// `option go_package = "acme.com/billing";` on a `Customer` message,
/// the result is:
/// ```text
/// { "java" → "com.acme.billing.Customer",
///   "csharp" → "Acme.Billing.Customer",
///   "go" → "acme.com/billing.Customer" }
/// ```
fn build_language_classes(schema: &ProtoSchema) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    for lang in schema.declared_languages() {
        let fqn = schema.fully_qualified_name(lang, &schema.message_name);
        if fqn != schema.message_name {
            // Only insert when a namespace was applied — bare
            // message names aren't worth recording per-language.
            out.insert(lang.to_string(), fqn);
        }
    }
    out
}

pub(crate) fn append_missing_audit_columns(columns: &mut Vec<ManifestColumn>) {
    let mut next_field_number = columns
        .iter()
        .map(|column| column.field_number)
        .max()
        .unwrap_or(0)
        + 1;

    for column in [
        audit_column(
            "created_at",
            "google.protobuf.Timestamp",
            "TIMESTAMPTZ",
            true,
            "CURRENT_TIMESTAMP",
            "Record creation timestamp",
        ),
        audit_column(
            "updated_at",
            "google.protobuf.Timestamp",
            "TIMESTAMPTZ",
            true,
            "CURRENT_TIMESTAMP",
            "Record update timestamp",
        ),
        audit_column(
            "created_by",
            "string",
            "VARCHAR(120)",
            false,
            "",
            "Record creator",
        ),
    ] {
        if columns
            .iter()
            .any(|existing| existing.column_name == column.column_name)
        {
            continue;
        }
        let mut column = column;
        column.field_number = next_field_number;
        next_field_number += 1;
        columns.push(column);
    }
}

pub(crate) fn build_manifest_projections(
    tables: &mut [ManifestTable],
    stores: &[ManifestStore],
) -> Vec<ManifestProjection> {
    let mut projections = Vec::new();
    for table in tables.iter_mut() {
        let mut table_projections = Vec::new();
        table_projections.push(ManifestProjection {
            message_type: table.message_name.clone(),
            projection_kind: "relational".to_string(),
            backend: "postgres".to_string(),
            instance: String::new(),
            resource_name: format!("{}.{}", table.schema, table.table),
            read_policy: if table.replica_hint.eq_ignore_ascii_case("primary") {
                "primary".to_string()
            } else {
                "replica".to_string()
            },
            write_policy: "primary".to_string(),
            fanout_policy: "primary_only".to_string(),
            consistency: ManifestConsistency {
                model: "strong".to_string(),
                read_your_writes: true,
                max_replica_lag_ms: 0,
                eventual_allowed: false,
            },
            write_owner: true,
            options: Vec::new(),
        });

        for store in stores
            .iter()
            .filter(|store| store.owner_schema == table.schema && store.owner_table == table.table)
        {
            let projection_kind = projection_kind_for_store(store);
            table_projections.push(ManifestProjection {
                message_type: table.message_name.clone(),
                projection_kind: projection_kind.clone(),
                backend: store.backend.clone(),
                instance: store_option(store, "instance")
                    .or_else(|| store_option(store, "target_instance"))
                    .unwrap_or_default(),
                resource_name: store.resource_name.clone(),
                read_policy: default_read_policy(&projection_kind),
                write_policy: store_option(store, "write_policy")
                    .unwrap_or_else(|| "projection".to_string()),
                fanout_policy: store_option(store, "fanout_policy")
                    .unwrap_or_else(|| "async_projection".to_string()),
                consistency: ManifestConsistency {
                    model: store_option(store, "consistency")
                        .unwrap_or_else(|| "eventual".to_string()),
                    read_your_writes: store_option(store, "read_your_writes")
                        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
                        .unwrap_or(false),
                    max_replica_lag_ms: store_option(store, "max_replica_lag_ms")
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0),
                    eventual_allowed: true,
                },
                write_owner: store_option(store, "write_owner")
                    .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
                    .unwrap_or(false),
                options: store.options.clone(),
            });
        }
        projections.extend(table_projections.clone());
        table.projections = table_projections;
    }
    projections
}

pub(crate) fn projection_kind_for_store(store: &ManifestStore) -> String {
    match store.store_kind.as_str() {
        "nosql" | "document" => "document",
        "column" | "columnar" | "timeseries" => "columnar",
        "blob" | "storage" => "object",
        other => other,
    }
    .to_string()
}

pub(crate) fn default_read_policy(projection_kind: &str) -> String {
    match projection_kind {
        "relational" => "replica",
        "cache" => "cache_first",
        "vector" => "vector",
        "document" => "document",
        "graph" => "graph",
        "columnar" => "analytics",
        "object" => "object",
        _ => "projection",
    }
    .to_string()
}

pub(crate) fn store_option(store: &ManifestStore, key: &str) -> Option<String> {
    store
        .options
        .iter()
        .find(|option| option.key == key || option.key == format!("udb.{key}"))
        .map(|option| option.value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(crate) fn audit_column(
    name: &str,
    proto_type: &str,
    sql_type: &str,
    not_null: bool,
    default_value: &str,
    comment: &str,
) -> ManifestColumn {
    ManifestColumn {
        field_name: name.to_string(),
        column_name: name.to_string(),
        proto_type: proto_type.to_string(),
        sql_type: sql_type.to_string(),
        not_null,
        default_value: default_value.to_string(),
        comment: comment.to_string(),
        field_number: 0,
        ..ManifestColumn::default()
    }
}

pub(crate) fn column_from_proto(column: &ProtoColumn) -> ManifestColumn {
    let mut sql_type = normalize_sql_type(&column.sql_type);
    // Only append [] for standard SQL arrays. Skip when:
    //  (a) the type already ends with []
    //  (b) an explicit sql_type with dimension/modifier is provided (e.g. vector(384),
    //      geometry, etc.) — these are custom types where the proto `repeated` keyword
    //      is used to represent multi-valued data but the SQL column itself is a single
    //      typed value, not a PG array.
    if column.is_array && !sql_type.ends_with("[]") && !sql_type.contains('(') {
        sql_type.push_str("[]");
    }
    ManifestColumn {
        field_name: column.field_name.clone(),
        column_name: normalize_ident(&column.column_name),
        proto_type: column.proto_type.clone(),
        sql_type,
        not_null: column.not_null,
        unique: column.unique,
        is_primary: column.is_primary,
        auto_increment: column.auto_increment,
        is_array: column.is_array,
        default_value: column.default_value.trim().to_string(),
        check_constraint: column.check_constraint.trim().to_string(),
        collation: column.collation.trim().to_string(),
        enum_values: column.enum_values.clone(),
        comment: column.comment.trim().to_string(),
        exclude_from_insert: column.exclude_from_insert,
        exclude_from_update: column.exclude_from_update,
        encrypted: column.encrypted,
        is_json: column.is_json,
        is_jsonb: column.is_jsonb,
        json_path_ops: column.json_path_ops,
        is_tsvector: column.is_tsvector,
        tsvector_language: column.tsvector_language.clone(),
        tsvector_source_columns: column.tsvector_source_columns.clone(),
        trigram_index: column.trigram_index,
        references: column.references.trim().to_string(),
        security: ManifestColumnSecurity {
            is_pii: column.security.is_pii,
            is_encrypted: column.security.is_encrypted,
            is_blind_index: column.security.is_blind_index,
            mask_in_logs: column.security.mask_in_logs,
            data_class: column.security.data_class.clone(),
            consent_required: column.security.consent_required,
            retention_days: column.security.retention_days,
        },
        field_number: column.field_number,
        oneof_group: column.oneof_group.clone(),
        previous_column_name: normalize_ident(&column.previous_column_name),
        backfill_sql: column.backfill_sql.trim().to_string(),
        using_expression: column.using_expression.trim().to_string(),
        allow_drop: column.allow_drop,
        generated: column.generated,
        generated_expr: column.generated_expr.trim().to_string(),
        is_identity: column.is_identity,
    }
}

pub(crate) fn index_from_proto(index: &ProtoIndex) -> ManifestIndex {
    let mut columns = index
        .columns
        .iter()
        .map(|col| normalize_ident(col))
        .filter(|col| !col.is_empty())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        columns = Vec::new();
    }
    let mut include_columns = index
        .include_columns
        .iter()
        .map(|col| normalize_ident(col))
        .filter(|col| !col.is_empty())
        .collect::<Vec<_>>();
    include_columns.sort();
    ManifestIndex {
        name: index.name.trim().to_string(),
        columns,
        unique: index.unique,
        method: first_non_empty(&index.index_method, &index.index_type, "BTREE")
            .to_ascii_uppercase(),
        where_clause: index.where_clause.trim().to_string(),
        include_columns,
        operator_class: index.operator_class.trim().to_string(),
        index_params: index
            .index_params
            .iter()
            .map(|param| ManifestStoreOption {
                key: param.key.trim().to_string(),
                value: param.value.trim().to_string(),
            })
            .filter(|param| !param.key.is_empty())
            .collect(),
        concurrent: index.concurrent,
    }
}

pub(crate) fn fk_from_proto(fk: &ProtoForeignKey) -> ManifestForeignKey {
    let columns = fk
        .columns
        .iter()
        .map(|col| normalize_ident(col))
        .filter(|col| !col.is_empty())
        .collect::<Vec<_>>();
    let ref_columns = if fk.ref_columns.is_empty() {
        columns.clone()
    } else {
        fk.ref_columns
            .iter()
            .map(|col| normalize_ident(col))
            .filter(|col| !col.is_empty())
            .collect()
    };
    ManifestForeignKey {
        name: fk.name.trim().to_string(),
        columns,
        ref_schema: normalize_ident_or(&fk.ref_schema, "public"),
        ref_table: normalize_ident(&fk.ref_table),
        ref_columns,
        on_delete: normalize_action(&fk.on_delete),
        on_update: normalize_action(&fk.on_update),
        not_valid: fk.not_valid,
        deferrable: fk.deferrable,
        initially_deferred: fk.initially_deferred,
    }
}

pub(crate) fn stores_from_schema(schema: &ProtoSchema) -> Vec<ManifestStore> {
    let mut stores = Vec::new();
    let owner_schema = normalize_ident_or(&schema.schema_name, "public");
    let owner_table = normalize_ident(&schema.table_name);

    if let Some(store) = &schema.vector_store {
        stores.push(ManifestStore {
            store_kind: "vector".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.collection_name, &owner_table),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            payload_schema_json: store.payload_schema_json.clone(),
            options: present_options([
                ("dimension", store.dimension.to_string()),
                ("distance", store.distance.clone()),
                ("shard_count", store.shard_count.to_string()),
                ("replica_count", store.replica_count.to_string()),
                ("on_disk", store.on_disk.to_string()),
                ("hnsw_m", store.hnsw_m.to_string()),
                (
                    "hnsw_ef_construction",
                    store.hnsw_ef_construction.to_string(),
                ),
            ]),
            ..ManifestStore::default()
        });
    }

    if let Some(store) = &schema.graph_store {
        stores.push(ManifestStore {
            store_kind: "graph".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.graph_name, &owner_table),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            payload_schema_json: store.payload_schema_json.clone(),
            options: present_options([
                ("node_label", store.node_label.clone()),
                ("id_field", store.id_field.clone()),
                ("tenant_field", store.tenant_field.clone()),
                ("edge_source_field", store.edge_source_field.clone()),
                ("edge_target_field", store.edge_target_field.clone()),
            ]),
            ..ManifestStore::default()
        });
    }

    if let Some(store) = &schema.document_store {
        stores.push(ManifestStore {
            store_kind: "nosql".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            database_name: store.database_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.collection_name, &owner_table),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            payload_schema_json: store.payload_schema_json.clone(),
            options: present_options([
                ("partition_key", store.partition_key.clone()),
                ("id_field", store.id_field.clone()),
                ("tenant_field", store.tenant_field.clone()),
                ("ttl_seconds", store.ttl_seconds.to_string()),
            ]),
            ..ManifestStore::default()
        });
    }

    if let Some(store) = &schema.timeseries_store {
        stores.push(ManifestStore {
            store_kind: "timeseries".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            database_name: store.database_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.measurement_name, &owner_table),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            options: present_options([
                ("time_field", store.time_field.clone()),
                ("tenant_field", store.tenant_field.clone()),
                ("tag_fields", store.tag_fields.join(",")),
                ("value_fields", store.value_fields.join(",")),
                ("retention_days", store.retention_days.to_string()),
                ("downsample_policy", store.downsample_policy.clone()),
            ]),
            ..ManifestStore::default()
        });
    }

    if let Some(store) = &schema.column_store {
        stores.push(ManifestStore {
            store_kind: "column".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            database_name: store.database_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.table_name, &owner_table),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            payload_schema_json: store.payload_schema_json.clone(),
            options: present_options([
                ("partition_key", store.partition_key.clone()),
                ("sort_key", store.sort_key.clone()),
                ("compression", store.compression.clone()),
                ("ttl_seconds", store.ttl_seconds.to_string()),
            ]),
            ..ManifestStore::default()
        });
    }

    if let Some(store) = &schema.cache {
        stores.push(ManifestStore {
            store_kind: "cache".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            namespace: defaulted(&store.namespace, &owner_schema),
            resource_name: defaulted(&store.key_pattern, &owner_table),
            dsn_env_key: store.cluster_env_key.clone(),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            options: present_options([
                ("key_pattern", store.key_pattern.clone()),
                ("ttl_seconds", store.ttl_seconds.to_string()),
                ("write_through", store.write_through.to_string()),
                ("read_through", store.read_through.to_string()),
                ("eviction_policy", store.eviction_policy.clone()),
            ]),
            ..ManifestStore::default()
        });
    }

    for column in &schema.columns {
        if let Some(storage) = &column.storage {
            let bucket = if storage.bucket_env_key.trim().is_empty() {
                format!("{}_{}", owner_table, column.column_name)
            } else {
                storage.bucket_env_key.to_ascii_lowercase()
            };
            stores.push(ManifestStore {
                store_kind: "object".to_string(),
                backend: normalize_backend(&storage.backend),
                logical_name: format!("{}.{}", schema.message_name, column.field_name),
                namespace: owner_schema.clone(),
                resource_name: bucket,
                owner_schema: owner_schema.clone(),
                owner_table: owner_table.clone(),
                options: present_options([
                    ("field_name", column.field_name.clone()),
                    ("column_name", column.column_name.clone()),
                    ("bucket_env_key", storage.bucket_env_key.clone()),
                    ("key_prefix", storage.key_prefix.clone()),
                    ("presigned_read", storage.presigned_read.to_string()),
                    ("presigned_write", storage.presigned_write.to_string()),
                    (
                        "presigned_ttl_seconds",
                        storage.presigned_ttl_seconds.to_string(),
                    ),
                    (
                        "server_side_encryption",
                        storage.server_side_encryption.to_string(),
                    ),
                    ("kms_key_id", storage.kms_key_id.clone()),
                    ("acl", storage.acl.clone()),
                ]),
                ..ManifestStore::default()
            });
        }
    }

    if let Some(store) = &schema.model_registry {
        stores.push(ManifestStore {
            store_kind: "model_registry".to_string(),
            backend: normalize_backend(&store.backend),
            logical_name: schema.message_name.clone(),
            namespace: owner_schema.clone(),
            resource_name: defaulted(&store.experiment_name, &owner_table),
            dsn_env_key: store.storage_uri_env.clone(),
            owner_schema: owner_schema.clone(),
            owner_table: owner_table.clone(),
            options: present_options([
                ("experiment_name", store.experiment_name.clone()),
                ("artifact_path", store.artifact_path.clone()),
                ("auto_register", store.auto_register.to_string()),
                ("stage", store.stage.clone()),
                ("metric_keys", store.metric_keys.join(",")),
                ("param_keys", store.param_keys.join(",")),
                ("storage_uri_env", store.storage_uri_env.clone()),
            ]),
            ..ManifestStore::default()
        });
    }

    for store in &schema.generic_stores {
        stores.push(generic_store_from_proto(
            store,
            schema,
            &owner_schema,
            &owner_table,
        ));
    }

    stores
}

pub(crate) fn generic_store_from_proto(
    store: &GenericStore,
    schema: &ProtoSchema,
    owner_schema: &str,
    owner_table: &str,
) -> ManifestStore {
    ManifestStore {
        store_kind: defaulted(&store.store_kind, "generic"),
        backend: normalize_backend(&store.backend),
        logical_name: defaulted(&store.logical_name, &schema.message_name),
        database_name: store.database_name.clone(),
        namespace: defaulted(&store.namespace, owner_schema),
        resource_name: defaulted(&store.resource_name, owner_table),
        dsn_env_key: store.dsn_env_key.clone(),
        dsn: store.dsn.clone(),
        owner_schema: owner_schema.to_string(),
        owner_table: owner_table.to_string(),
        payload_schema_json: store.payload_schema_json.clone(),
        options: store
            .options
            .iter()
            .map(|option| ManifestStoreOption {
                key: option.key.clone(),
                value: option.value.clone(),
            })
            .collect(),
    }
}

pub(crate) fn present_options<const N: usize>(
    options: [(&str, String); N],
) -> Vec<ManifestStoreOption> {
    options
        .into_iter()
        .filter(|(_, value)| !value.trim().is_empty() && value != "0" && value != "false")
        .map(|(key, value)| ManifestStoreOption {
            key: key.to_string(),
            value,
        })
        .collect()
}

pub(crate) fn compute_schema_checksums(
    tables: &[ManifestTable],
) -> Result<Vec<ManifestSchemaChecksum>, serde_json::Error> {
    let mut by_schema: BTreeMap<String, Vec<TableDdl>> = BTreeMap::new();
    for table in tables {
        by_schema
            .entry(table.schema.clone())
            .or_default()
            .push(table_ddl(table));
    }
    by_schema
        .into_iter()
        .map(|(schema, tables)| {
            Ok(ManifestSchemaChecksum {
                schema,
                checksum_sha256: checksum_hex(&tables)?,
            })
        })
        .collect()
}

pub(crate) fn compute_schema_order(tables: &[ManifestTable]) -> Vec<String> {
    let mut min_order = BTreeMap::<String, i32>::new();
    let mut deps = BTreeMap::<String, BTreeSet<String>>::new();

    for table in tables {
        min_order
            .entry(table.schema.clone())
            .and_modify(|current| *current = (*current).min(table.migration_order))
            .or_insert(table.migration_order);
        deps.entry(table.schema.clone()).or_default();
        for fk in &table.foreign_keys {
            if fk.not_valid || fk.ref_schema == table.schema || fk.ref_schema.is_empty() {
                continue;
            }
            deps.entry(table.schema.clone())
                .or_default()
                .insert(fk.ref_schema.clone());
            deps.entry(fk.ref_schema.clone()).or_default();
            min_order.entry(fk.ref_schema.clone()).or_insert(9999);
        }
    }

    let mut indegree = deps
        .keys()
        .map(|schema| (schema.clone(), 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<String, BTreeSet<String>>::new();
    for (schema, schema_deps) in &deps {
        for dep in schema_deps {
            *indegree.entry(schema.clone()).or_default() += 1;
            dependents
                .entry(dep.clone())
                .or_default()
                .insert(schema.clone());
        }
    }

    let mut ready = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(schema, _)| schema.clone())
        .collect::<Vec<_>>();
    sort_schemas(&mut ready, &min_order);

    let mut out = Vec::new();
    while let Some(schema) = ready.first().cloned() {
        ready.remove(0);
        out.push(schema.clone());
        if let Some(children) = dependents.get(&schema) {
            for child in children {
                if let Some(count) = indegree.get_mut(child) {
                    *count -= 1;
                    if *count == 0 {
                        ready.push(child.clone());
                    }
                }
            }
            sort_schemas(&mut ready, &min_order);
        }
    }

    if out.len() < indegree.len() {
        let mut remaining = indegree
            .keys()
            .filter(|schema| !out.contains(schema))
            .cloned()
            .collect::<Vec<_>>();
        sort_schemas(&mut remaining, &min_order);
        out.extend(remaining);
    }
    out
}

pub(crate) fn validate_manifest_tables(tables: &[ManifestTable]) -> Vec<String> {
    let mut errors = Vec::new();
    let table_keys = tables
        .iter()
        .map(|table| format!("{}.{}", table.schema, table.table))
        .collect::<BTreeSet<_>>();
    for table in tables {
        for fk in &table.foreign_keys {
            if fk.ref_table.is_empty() {
                errors.push(format!(
                    "{}.{} FK {} has no referenced table",
                    table.schema, table.table, fk.name
                ));
                continue;
            }
            let key = format!("{}.{}", fk.ref_schema, fk.ref_table);
            if !table_keys.contains(&key) {
                errors.push(format!(
                    "{}.{} FK {} references missing table {}",
                    table.schema, table.table, fk.name, key
                ));
            }
        }
    }
    errors.extend(detect_fk_cycles(tables));
    errors
}
