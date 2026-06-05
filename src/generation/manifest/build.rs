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

fn validate_rls_expression(value: &str) -> Result<(), &'static str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if trimmed.contains('\0') {
        return Err("contains a NUL byte");
    }
    if trimmed.contains("$$")
        || trimmed.contains(';')
        || trimmed.contains("--")
        || trimmed.contains("/*")
        || trimmed.contains("*/")
    {
        return Err("contains SQL block/comment/statement separator tokens");
    }
    let mut single_quote_open = false;
    let mut double_quote_open = false;
    let mut paren_depth = 0i32;
    let mut chars = trimmed.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !double_quote_open => {
                if single_quote_open && matches!(chars.peek(), Some('\'')) {
                    chars.next();
                } else {
                    single_quote_open = !single_quote_open;
                }
            }
            '"' if !single_quote_open => {
                double_quote_open = !double_quote_open;
            }
            '(' if !single_quote_open && !double_quote_open => paren_depth += 1,
            ')' if !single_quote_open && !double_quote_open => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err("has unbalanced parentheses");
                }
            }
            _ => {}
        }
    }
    if single_quote_open || double_quote_open {
        return Err("has an unterminated quoted string or identifier");
    }
    if paren_depth != 0 {
        return Err("has unbalanced parentheses");
    }
    Ok(())
}

/// Same safety contract as [`validate_rls_expression`]: reject any
/// user-supplied SQL fragment that could break out of the surrounding
/// statement (statement separators, comment/block delimiters, NUL bytes,
/// unbalanced quotes/parens). Applied to column `check_constraint` values,
/// which are otherwise embedded RAW into `ADD CONSTRAINT ... CHECK (...)`
/// (render_ext.rs) and inline `CONSTRAINT ... CHECK (...)` on CREATE TABLE
/// (sql/mod.rs) — neither of which quotes or escapes the expression.
fn validate_check_expression(value: &str) -> Result<(), &'static str> {
    validate_rls_expression(value)
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

    let mut warnings = Vec::new();
    // Column-level CHECK expressions are author-supplied SQL embedded raw into
    // DDL. Validate each the same way RLS expressions are validated; drop (with
    // a warning) any that carries a statement-breaking token rather than emit
    // an injectable constraint. This closes the gap where enum-derived CHECKs
    // and RLS expressions were guarded but free-form check_constraint was not.
    let mut checks: Vec<ManifestCheck> = Vec::new();
    for col in &columns {
        if col.check_constraint.trim().is_empty() {
            continue;
        }
        if let Err(reason) = validate_check_expression(&col.check_constraint) {
            warnings.push(format!(
                "{}.{} check_constraint omitted: {}",
                schema.table_name, col.column_name, reason
            ));
            tracing::warn!(
                table = %schema.table_name,
                column = %col.column_name,
                reason = %reason,
                "dropping unsafe check_constraint from generated DDL"
            );
            continue;
        }
        checks.push(ManifestCheck {
            name: format!("chk_{}_{}", schema.table_name, col.column_name),
            expression: col.check_constraint.clone(),
        });
    }
    for column in &columns {
        if column.check_constraint.trim().is_empty() && !column.enum_values.is_empty() {
            let mut values = column.enum_values.clone();
            values.sort();
            values.dedup();
            // Same guard as render_enum_types (GAP 31): drop any value that
            // could break out of the SQL literal or a surrounding DO $$…$$
            // block (quote, backslash, dollar, null, newline, over-length).
            // The bare `replace('\'', "''")` here only handled single quotes.
            let literals = values
                .iter()
                .filter(
                    |value| match crate::generation::sql::validate_enum_value(value) {
                        Ok(()) => true,
                        Err(reason) => {
                            warnings.push(format!(
                                "{}.{} enum value '{}' omitted from generated CHECK: {}",
                                schema.table_name, column.column_name, value, reason
                            ));
                            tracing::warn!(
                                table = %schema.table_name,
                                column = %column.column_name,
                                value = %value,
                                reason = %reason,
                                "skipping unsafe enum value in CHECK constraint generation"
                            );
                            false
                        }
                    },
                )
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>();
            // Only emit the CHECK when at least one safe value remains;
            // an empty `IN ()` is invalid SQL.
            if !literals.is_empty() {
                checks.push(ManifestCheck {
                    name: format!("chk_{}_{}_enum", schema.table_name, column.column_name),
                    expression: format!("{} IN ({})", column.column_name, literals.join(",")),
                });
            }
        }
    }
    // oneof XOR enforcement: each member of a proto `oneof` maps to a nullable
    // column, so emit one CHECK per group asserting at most one member is set.
    // Flows through bootstrap (CREATE TABLE constraint), delta (AddCheck), and
    // the checksum via the manifest, using the existing check machinery.
    {
        let mut groups: std::collections::BTreeMap<String, Vec<String>> =
            std::collections::BTreeMap::new();
        for column in &columns {
            let group = column.oneof_group.trim();
            if !group.is_empty() {
                groups
                    .entry(group.to_string())
                    .or_default()
                    .push(column.column_name.clone());
            }
        }
        for (group, mut member_cols) in groups {
            // A single-member oneof has no XOR to enforce.
            if member_cols.len() < 2 {
                continue;
            }
            member_cols.sort();
            checks.push(ManifestCheck {
                name: format!("chk_{}_{}_oneof", schema.table_name, group),
                expression: format!("num_nonnulls({}) <= 1", member_cols.join(", ")),
            });
        }
    }
    checks.sort_by(|a, b| a.expression.cmp(&b.expression));

    let mut rls_policies = schema
        .rls_policies
        .iter()
        .map(|policy| {
            let mut using_expression = policy.using_expression.trim().to_string();
            let mut with_check = policy.with_check.trim().to_string();
            if let Err(reason) = validate_rls_expression(&using_expression) {
                warnings.push(format!(
                    "{} RLS policy '{}' USING expression omitted: {}",
                    schema.table_name, policy.name, reason
                ));
                using_expression.clear();
            }
            if let Err(reason) = validate_rls_expression(&with_check) {
                warnings.push(format!(
                    "{} RLS policy '{}' WITH CHECK expression omitted: {}",
                    schema.table_name, policy.name, reason
                ));
                with_check.clear();
            }
            ManifestPolicy {
                name: normalize_ident(&policy.name),
                command: normalize_policy_command(&policy.command),
                using_expression,
                with_check,
                permissive: policy.permissive,
            }
        })
        .collect::<Vec<_>>();
    rls_policies.sort_by(|a, b| a.name.cmp(&b.name));

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
                POLICY_PRIMARY.to_string()
            } else {
                POLICY_REPLICA.to_string()
            },
            write_policy: POLICY_PRIMARY.to_string(),
            fanout_policy: POLICY_PRIMARY_ONLY.to_string(),
            consistency: ManifestConsistency {
                model: POLICY_STRONG.to_string(),
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
                    .unwrap_or_else(|| POLICY_PROJECTION.to_string()),
                fanout_policy: store_option(store, "fanout_policy")
                    .unwrap_or_else(|| POLICY_ASYNC_PROJECTION.to_string()),
                consistency: ManifestConsistency {
                    model: store_option(store, "consistency")
                        .unwrap_or_else(|| POLICY_EVENTUAL.to_string()),
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
        "relational" => POLICY_REPLICA,
        "cache" => POLICY_CACHE_FIRST,
        "vector" => "vector",
        "document" => "document",
        "graph" => "graph",
        "columnar" => "analytics",
        "object" => "object",
        _ => POLICY_PROJECTION,
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
    //  (a) the type already ends with `]` (e.g. `text[]`, `int[][]`)
    //  (b) the base type is a custom multi-valued-but-scalar type (vector,
    //      geometry, …) where the proto `repeated` keyword represents the
    //      multi-valued payload but the SQL column itself is a single value.
    //
    // A previous heuristic treated *any* `(` modifier as case (b), which
    // wrongly dropped the array suffix for standard parametric types — e.g.
    // `repeated numeric(10,2)` compiled to a scalar `numeric(10,2)` instead
    // of `numeric(10,2)[]`. Match only the known scalar custom types by their
    // base (the token before any `(`).
    const SCALAR_REPEATED_BASES: &[&str] = &[
        "vector",
        "halfvec",
        "sparsevec",
        "geometry",
        "geography",
        "tsvector",
    ];
    let base = sql_type
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let is_scalar_custom = SCALAR_REPEATED_BASES.contains(&base.as_str());
    if column.is_array && !sql_type.ends_with(']') && !is_scalar_custom {
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
        is_tenant_column: column.is_tenant,
        is_project_column: column.is_project,
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
            min_order
                .entry(fk.ref_schema.clone())
                .or_insert(DEFAULT_MIGRATION_ORDER);
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

    let ready_vec = indegree
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(schema, _)| schema.clone())
        .collect::<Vec<_>>();
    let mut ready = ready_vec;
    sort_schemas(&mut ready, &min_order);
    let mut ready = std::collections::VecDeque::from(ready);

    let mut out = Vec::new();
    let mut out_seen = BTreeSet::new();
    while let Some(schema) = ready.pop_front() {
        out_seen.insert(schema.clone());
        out.push(schema.clone());
        if let Some(children) = dependents.get(&schema) {
            let mut newly_ready = Vec::new();
            for child in children {
                if let Some(count) = indegree.get_mut(child) {
                    *count -= 1;
                    if *count == 0 {
                        newly_ready.push(child.clone());
                    }
                }
            }
            let mut merged = ready.into_iter().collect::<Vec<_>>();
            merged.extend(newly_ready);
            sort_schemas(&mut merged, &min_order);
            ready = std::collections::VecDeque::from(merged);
        }
    }

    if out.len() < indegree.len() {
        let mut remaining = indegree
            .keys()
            .filter(|schema| !out_seen.contains(*schema))
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

#[cfg(test)]
mod oneof_check_tests {
    use super::*;
    use crate::ast::{ProtoColumn, ProtoSchema};

    fn col(name: &str, field_number: i32, oneof: &str) -> ProtoColumn {
        ProtoColumn {
            field_name: name.to_string(),
            column_name: name.to_string(),
            proto_type: "string".to_string(),
            sql_type: "TEXT".to_string(),
            field_number,
            oneof_group: oneof.to_string(),
            ..ProtoColumn::default()
        }
    }

    #[test]
    fn oneof_group_emits_xor_check() {
        let mut schema = ProtoSchema::new("Payment");
        schema.table_name = "payments".to_string();
        schema.schema_name = "billing".to_string();
        schema.is_table = true;
        schema.columns = vec![
            col("id", 1, ""),
            col("card", 2, "method"),
            col("bank", 3, "method"),
            col("wallet", 4, "method"),
            col("solo", 5, "single"), // single-member oneof → no XOR check
        ];

        let table = table_from_schema(&schema);
        let oneof: Vec<&ManifestCheck> = table
            .checks
            .iter()
            .filter(|c| c.name.ends_with("_oneof"))
            .collect();

        assert_eq!(oneof.len(), 1, "only the multi-member group gets a check");
        assert_eq!(oneof[0].name, "chk_payments_method_oneof");
        // Member columns are sorted for a deterministic expression.
        assert_eq!(oneof[0].expression, "num_nonnulls(bank, card, wallet) <= 1");
    }

    #[test]
    fn two_oneof_groups_emit_two_checks() {
        let mut schema = ProtoSchema::new("Multi");
        schema.table_name = "multi".to_string();
        schema.columns = vec![
            col("a1", 1, "alpha"),
            col("a2", 2, "alpha"),
            col("b1", 3, "beta"),
            col("b2", 4, "beta"),
        ];
        let table = table_from_schema(&schema);
        let mut names: Vec<&str> = table
            .checks
            .iter()
            .filter(|c| c.name.ends_with("_oneof"))
            .map(|c| c.name.as_str())
            .collect();
        names.sort();
        assert_eq!(names, vec!["chk_multi_alpha_oneof", "chk_multi_beta_oneof"]);
    }

    #[test]
    fn no_oneof_emits_no_xor_check() {
        let mut schema = ProtoSchema::new("Plain");
        schema.table_name = "plain".to_string();
        schema.columns = vec![col("id", 1, ""), col("name", 2, "")];
        let table = table_from_schema(&schema);
        assert!(table.checks.iter().all(|c| !c.name.ends_with("_oneof")));
    }

    fn col_check(name: &str, field_number: i32, check: &str) -> ProtoColumn {
        ProtoColumn {
            check_constraint: check.to_string(),
            ..col(name, field_number, "")
        }
    }

    #[test]
    fn malicious_check_constraint_is_dropped_not_emitted() {
        // A statement-breaking CHECK expression must never reach the raw
        // `ADD CONSTRAINT ... CHECK (...)` emitter.
        let mut schema = ProtoSchema::new("Acct");
        schema.table_name = "accounts".to_string();
        schema.columns = vec![
            col("id", 1, ""),
            col_check("bal", 2, "bal > 0); DROP TABLE accounts; --"),
        ];
        let table = table_from_schema(&schema);
        assert!(
            !table.checks.iter().any(|c| c.name == "chk_accounts_bal"),
            "unsafe column check_constraint must be omitted from the manifest"
        );
    }

    #[test]
    fn benign_check_constraint_survives() {
        // Quotes and parentheses are legitimate in CHECK expressions and must
        // pass — the guard rejects only statement-breaking tokens.
        let mut schema = ProtoSchema::new("Acct");
        schema.table_name = "accounts".to_string();
        schema.columns = vec![
            col("id", 1, ""),
            col_check("status", 2, "status IN ('active', 'closed')"),
        ];
        let table = table_from_schema(&schema);
        let chk = table
            .checks
            .iter()
            .find(|c| c.name == "chk_accounts_status")
            .expect("benign check_constraint should be emitted");
        assert_eq!(chk.expression, "status IN ('active', 'closed')");
    }
}
