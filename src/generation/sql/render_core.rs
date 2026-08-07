//! sql.rs split — render_core (Phase I).
use super::*;

pub(crate) fn render_header(
    table: &ManifestTable,
    manifest_checksum: &str,
    config: &SqlGenerationConfig,
) -> String {
    // NOTE: do NOT include any time-varying values (timestamps, run IDs, etc.)
    // in this header. The header text is part of the artifact content that gets
    // SHA-256 hashed and stored in public.schema_migrations. Any value that
    // changes between runs would produce a different checksum, causing every
    // artifact to appear un-applied on every run even when the schema is
    // identical.
    format!(
        "-- UDB:migration_kind=bootstrap\n-- UDB:schema={}\n-- UDB:table={}\n-- UDB:proto_manifest_checksum={}\n-- UDB:source_proto={}\n-- UDB:generator={}\n\nSET lock_timeout = '{}';\nSET statement_timeout = '{}';\n",
        table.schema,
        table.table,
        manifest_checksum,
        table.source_file,
        config.generator_name,
        config.lock_timeout.replace('\'', "''"),
        config.statement_timeout.replace('\'', "''")
    )
}

pub(crate) fn render_extensions(manifest: &CatalogManifest) -> String {
    let mut extensions = manifest
        .tables
        .iter()
        .flat_map(|table| table.extensions.iter())
        .filter(|extension| !extension.name.trim().is_empty())
        .map(|extension| {
            (
                extension.name.clone(),
                if extension.schema.trim().is_empty() {
                    "public".to_string()
                } else {
                    extension.schema.clone()
                },
                extension.version.clone(),
            )
        })
        .collect::<Vec<_>>();
    extensions.sort();
    extensions.dedup();
    if extensions.is_empty() {
        return String::new();
    }
    // NOTE: do NOT include generated_at_unix() or any time-varying value here.
    // The artifact content is SHA-256 hashed and stored in schema_migrations;
    // a timestamp would produce a new checksum on every run, defeating the
    // idempotency skip logic that prevents re-applying already-applied artifacts.
    let mut sql = format!(
        "-- UDB:migration_kind=bootstrap\n-- UDB:extensions=true\n-- UDB:proto_manifest_checksum={}\n\n",
        manifest.checksum_sha256,
    );
    for (name, schema, version) in extensions {
        sql.push_str(&render_create_extension(&ManifestExtension {
            name,
            schema,
            version,
        }));
        sql.push('\n');
    }
    sql
}

pub(crate) fn render_foreign_keys_header(
    manifest_checksum: &str,
    config: &SqlGenerationConfig,
) -> String {
    format!(
        "-- UDB:migration_kind=bootstrap\n-- UDB:foreign_keys=true\n-- UDB:proto_manifest_checksum={}\n-- UDB:generator={}\n\nSET lock_timeout = '{}';\nSET statement_timeout = '{}';\n\n",
        manifest_checksum,
        config.generator_name,
        config.lock_timeout.replace('\'', "''"),
        config.statement_timeout.replace('\'', "''")
    )
}

pub(crate) fn render_delta_table(
    manifest: &CatalogManifest,
    schema: &str,
    table: &str,
    ops: &[&ChangeOperation],
    config: &SqlGenerationConfig,
) -> String {
    let mut sql = String::new();
    // NOTE: do NOT include generated_at_unix() here — see render_extensions for
    // the same rationale. Delta artifact checksums must be stable across runs.
    sql.push_str(&format!(
        "-- UDB:migration_kind=proto_delta\n-- UDB:schema={schema}\n-- UDB:table={table}\n-- UDB:operations={}\n-- UDB:proto_manifest_checksum={}\n-- UDB:generator={}\n\nSET lock_timeout = '{}';\nSET statement_timeout = '{}';\n\n",
        ops.iter()
            .map(|op| format!("{:?}", op.kind))
            .collect::<Vec<_>>()
            .join(","),
        manifest.checksum_sha256,
        config.generator_name,
        config.lock_timeout.replace('\'', "''"),
        config.statement_timeout.replace('\'', "''")
    ));
    sql.push_str("BEGIN;\n\n");

    let table_manifest = manifest.table(schema, table);

    // When the delta includes a new trigger, emit the trigger-support functions
    // (sql_artifacts with phase "before_triggers") first so that the subsequent
    // CREATE TRIGGER can resolve the function reference.  Using
    // CREATE OR REPLACE makes this idempotent on re-runs.
    let has_create_trigger = ops
        .iter()
        .any(|op| matches!(op.kind, ChangeKind::CreateTrigger));
    let has_explicit_sql_artifact = ops
        .iter()
        .any(|op| matches!(op.kind, ChangeKind::ApplySqlArtifact));
    if has_create_trigger && !has_explicit_sql_artifact {
        if let Some(t) = table_manifest {
            let artifact_sql = render_sql_artifacts(t, "before_triggers");
            if !artifact_sql.trim().is_empty() {
                sql.push_str("-- sql_artifacts: trigger support functions\n");
                sql.push_str(&artifact_sql);
            }
        }
    }

    for op in ops {
        let stmt = render_delta_operation(manifest, table_manifest, op);
        if !stmt.trim().is_empty() {
            sql.push_str(&format!("-- {:?}\n{}\n", op.kind, stmt));
        }
    }

    sql.push_str("COMMIT;\n");
    sql
}

/// Header for a **standalone, NON-transactional** artifact — a single statement
/// PostgreSQL forbids inside a `BEGIN/COMMIT` block (`CREATE INDEX CONCURRENTLY`,
/// `ALTER TYPE … ADD VALUE`). It is comments-only on purpose: the applier runs a
/// small artifact with one `pool.execute(content)`, and Postgres treats a
/// multi-statement string as an implicit transaction — so emitting `SET` or
/// `BEGIN` alongside the DDL would re-wrap it and defeat the point. The
/// `UDB:no_transaction=true` marker records the intent for tooling. (#116/#120)
pub(crate) fn render_non_tx_header(
    manifest_checksum: &str,
    schema: &str,
    table: &str,
    kind: &str,
    config: &SqlGenerationConfig,
) -> String {
    format!(
        "-- UDB:migration_kind={kind}\n-- UDB:no_transaction=true\n-- UDB:schema={schema}\n-- UDB:table={table}\n-- UDB:proto_manifest_checksum={manifest_checksum}\n-- UDB:generator={}\n-- This artifact MUST run outside a transaction (single statement, autocommit).\n\n",
        config.generator_name,
    )
}

/// True when a delta op must run as its own standalone, non-transactional
/// artifact because PostgreSQL forbids it inside a `BEGIN/COMMIT` block:
/// `ALTER TYPE … ADD VALUE` (#116) and concurrent non-unique index creation
/// (#120). Everything else stays in the grouped transactional delta artifact.
pub(crate) fn op_requires_standalone(manifest: &CatalogManifest, op: &ChangeOperation) -> bool {
    match op.kind {
        ChangeKind::AlterEnumAddValue => true,
        ChangeKind::CreateIndex => manifest
            .table(&op.schema, &op.table)
            .and_then(|t| find_index(t, &op.object_name))
            .map(|idx| idx.concurrent && !idx.unique)
            .unwrap_or(false),
        _ => false,
    }
}

/// Render a single delta op as a standalone, non-transactional artifact body:
/// a comments-only header plus exactly one statement (no `BEGIN/COMMIT`/`SET`),
/// so the applier executes it in autocommit. (#116/#120)
pub(crate) fn render_standalone_delta(
    manifest: &CatalogManifest,
    op: &ChangeOperation,
    config: &SqlGenerationConfig,
) -> String {
    let table_manifest = manifest.table(&op.schema, &op.table);
    let stmt = match op.kind {
        // CreateIndex via the normal delta path downgrades CONCURRENTLY (it
        // assumes a surrounding transaction); render the concurrent form here.
        ChangeKind::CreateIndex => table_manifest
            .and_then(|t| find_index(t, &op.object_name).map(|idx| render_index_standalone(t, idx)))
            .unwrap_or_default(),
        _ => render_delta_operation(manifest, table_manifest, op),
    };
    if stmt.trim().is_empty() {
        return String::new();
    }
    format!(
        "{}{}\n",
        render_non_tx_header(
            &manifest.checksum_sha256,
            &op.schema,
            &op.table,
            "proto_delta_non_transactional",
            config,
        ),
        stmt,
    )
}

/// Visible placeholder emitted when a column-dependent delta operation
/// references a column absent from the new manifest. These arms previously
/// returned an empty string, silently dropping the change with no trace.
fn missing_object_note(op: &ChangeOperation, what: &str, name: &str) -> String {
    format!(
        "-- UDB:skipped {:?} on {}.{}: {} '{}' not found in new manifest",
        op.kind, op.schema, op.table, what, name
    )
}

fn missing_column_note(op: &ChangeOperation) -> String {
    missing_object_note(op, "column", &op.column)
}

pub(crate) fn render_delta_operation(
    manifest: &CatalogManifest,
    table: Option<&ManifestTable>,
    op: &ChangeOperation,
) -> String {
    match op.kind {
        ChangeKind::ApplySqlArtifact => table
            .and_then(|table| find_sql_artifact(table, &op.object_name))
            .and_then(|artifact| render_sql_artifact(artifact, &artifact.phase))
            .unwrap_or_else(|| missing_object_note(op, "sql_artifact", &op.object_name)),
        ChangeKind::CreateExtension => find_extension(manifest, &op.schema, &op.object_name)
            .map(render_create_extension)
            .unwrap_or_default(),
        ChangeKind::DropExtension => {
            format!("DROP EXTENSION IF EXISTS {};", qi(&op.object_name))
        }
        ChangeKind::CreateTable => table
            .map(|table| render_bootstrap_table(table, &manifest.checksum_sha256, &SqlGenerationConfig::default()))
            .unwrap_or_default(),
        ChangeKind::RenameTable => format!(
            "ALTER TABLE {}.{} RENAME TO {};",
            qi(&op.schema),
            qi(&op.object_name),
            qi(&op.table)
        ),
        ChangeKind::DropTable => {
            format!("DROP TABLE IF EXISTS {}.{};", qi(&op.schema), qi(&op.table))
        }
        ChangeKind::SetTableUnlogged => format!(
            "ALTER TABLE {}.{} SET UNLOGGED;",
            qi(&op.schema),
            qi(&op.table)
        ),
        ChangeKind::SetTableLogged => format!(
            "ALTER TABLE {}.{} SET LOGGED;",
            qi(&op.schema),
            qi(&op.table)
        ),
        ChangeKind::SetTablespace => format!(
            "ALTER TABLE {}.{} SET TABLESPACE {};",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.object_name)
        ),
        ChangeKind::AddColumn => table
            .and_then(|table| find_column(table, &op.column))
            .map(|column| {
                let mut nullable = column.clone();
                if column.not_null && column.default_value.is_empty() && !column.backfill_sql.is_empty() {
                    nullable.not_null = false;
                    format!(
                        "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {};\nUPDATE {}.{} SET {} = {} WHERE {} IS NULL;\nALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL;",
                        qi(&op.schema),
                        qi(&op.table),
                        render_column(&nullable),
                        qi(&op.schema),
                        qi(&op.table),
                        qi(&column.column_name),
                        column.backfill_sql,
                        qi(&column.column_name),
                        qi(&op.schema),
                        qi(&op.table),
                        qi(&column.column_name)
                    )
                } else {
                    format!(
                        "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {};",
                        qi(&op.schema),
                        qi(&op.table),
                        render_column(column)
                    )
                }
            })
            .unwrap_or_else(|| missing_column_note(op)),
        ChangeKind::RenameColumn => format!(
            "ALTER TABLE {}.{} RENAME COLUMN {} TO {};",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.object_name),
            qi(&op.column)
        ),
        ChangeKind::ChangeColumnType => table
            .and_then(|table| find_column(table, &op.column))
            .map(|column| {
                let using = if column.using_expression.trim().is_empty() {
                    String::new()
                } else {
                    format!(" USING {}", column.using_expression)
                };
                format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}{};",
                    qi(&op.schema),
                    qi(&op.table),
                    qi(&column.column_name),
                    column.sql_type,
                    using
                )
            })
            .unwrap_or_else(|| missing_column_note(op)),
        ChangeKind::SetColumnCollation => table
            .and_then(|table| find_column(table, &op.column))
            .map(|column| {
                format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {} COLLATE {};",
                    qi(&op.schema),
                    qi(&op.table),
                    qi(&column.column_name),
                    column.sql_type,
                    qi(&column.collation)
                )
            })
            .unwrap_or_else(|| missing_column_note(op)),
        ChangeKind::SetDefault => table
            .and_then(|table| find_column(table, &op.column))
            .map(|column| {
                format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {};",
                    qi(&op.schema),
                    qi(&op.table),
                    qi(&column.column_name),
                    column.default_value
                )
            })
            .unwrap_or_else(|| missing_column_note(op)),
        ChangeKind::DropDefault => format!(
            "ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT;",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.column)
        ),
        ChangeKind::SetNotNull => table
            .and_then(|table| find_column(table, &op.column))
            .map(|column| {
                let backfill = first_non_empty(&column.backfill_sql, &column.default_value, "");
                let prefix = if backfill.is_empty() {
                    String::new()
                } else {
                    format!(
                        "UPDATE {}.{} SET {} = {} WHERE {} IS NULL;\n",
                        qi(&op.schema),
                        qi(&op.table),
                        qi(&column.column_name),
                        backfill,
                        qi(&column.column_name)
                    )
                };
                format!(
                    "{}ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL;",
                    prefix,
                    qi(&op.schema),
                    qi(&op.table),
                    qi(&column.column_name)
                )
            })
            .unwrap_or_else(|| missing_column_note(op)),
        ChangeKind::DropNotNull => format!(
            "ALTER TABLE {}.{} ALTER COLUMN {} DROP NOT NULL;",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.column)
        ),
        ChangeKind::DropColumn => format!(
            "ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.column)
        ),
        ChangeKind::AddCheck => table
            .and_then(|table| find_check(table, &op.object_name))
            .map(|check| render_add_check(&op.schema, &op.table, check))
            .unwrap_or_else(|| missing_object_note(op, "check constraint", &op.object_name)),
        ChangeKind::AddUnique => {
            let columns = table
                .map(|table| {
                    partition_aware_unique_columns(table, std::slice::from_ref(&op.column))
                })
                .unwrap_or_else(|| vec![op.column.clone()]);
            table
                .map(|table| {
                    let column_sql = columns.iter().map(|column| qi(column)).collect::<Vec<_>>();
                    render_partitioned_unique_index_create(
                        table,
                        &format!("uidx_{}_{}_{}", op.schema, op.table, op.column),
                        &columns,
                        &column_sql,
                        "BTREE",
                        "",
                        "",
                    )
                    .trim()
                    .to_string()
                })
                .unwrap_or_else(|| {
                    format!(
                        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {}.{} ({});",
                        qi(&format!("uidx_{}_{}_{}", op.schema, op.table, op.column)),
                        qi(&op.schema),
                        qi(&op.table),
                        quote_list(&columns)
                    )
                })
        }
        ChangeKind::CreateIndex => table
            .and_then(|table| find_index(table, &op.object_name).map(|index| render_index_in_tx(table, index)))
            .unwrap_or_default(),
        ChangeKind::DropIndex => {
            // A named index: object_name IS the real index identifier.
            format!("DROP INDEX IF EXISTS {}.{};", qi(&op.schema), qi(&op.object_name))
        }
        ChangeKind::DropUnique => {
            // The unique index UDB created for a column-level `unique: true` is
            // named `uidx_<schema>_<table>_<column>` (see the AddUnique arm above),
            // NOT the bare column. `object_name` on a DropUnique is the COLUMN, so
            // dropping it under `DROP INDEX IF EXISTS` matched nothing — the drop
            // silently no-op'd yet the artifact recorded as `applied`, leaving the
            // unique index (and its 409-on-second-row) in place forever. Emit the
            // index identifier AddUnique actually generated.
            format!(
                "DROP INDEX IF EXISTS {}.{};",
                qi(&op.schema),
                qi(&format!("uidx_{}_{}_{}", op.schema, op.table, op.column))
            )
        }
        ChangeKind::AddForeignKey => table
            .and_then(|table| find_fk(table, &op.object_name).map(|fk| (fk, is_partitioned(table))))
            .map(|(fk, partitioned)| render_add_fk(&op.schema, &op.table, fk, partitioned))
            .unwrap_or_default(),
        ChangeKind::DropForeignKey => format!(
            "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {};",
            qi(&op.schema),
            qi(&op.table),
            qi(&op.object_name)
        ),
        ChangeKind::EnableRls => format!(
            "ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY;",
            qi(&op.schema),
            qi(&op.table)
        ),
        ChangeKind::DisableRls => format!(
            "ALTER TABLE {}.{} DISABLE ROW LEVEL SECURITY;",
            qi(&op.schema),
            qi(&op.table)
        ),
        ChangeKind::CreatePolicy => table
            .and_then(|table| find_policy(table, &op.object_name))
            .map(|policy| render_policy(&op.schema, &op.table, policy))
            .unwrap_or_default(),
        ChangeKind::DropPolicy => format!(
            "DROP POLICY IF EXISTS {} ON {}.{};",
            qi(&op.object_name),
            qi(&op.schema),
            qi(&op.table)
        ),
        ChangeKind::CreateMaterializedView => table
            .and_then(|table| find_materialized_view(table, &op.object_name))
            .map(render_materialized_view)
            .unwrap_or_default(),
        ChangeKind::DropMaterializedView => {
            let (schema, view) = split_qualified_name(&op.object_name, &op.schema);
            format!("DROP MATERIALIZED VIEW IF EXISTS {}.{};", qi(&schema), qi(&view))
        }
        ChangeKind::CreateTrigger => table
            .and_then(|table| find_trigger(table, &op.object_name))
            .map(render_trigger)
            .unwrap_or_default(),
        ChangeKind::DropTrigger => format!(
            "DROP TRIGGER IF EXISTS {} ON {}.{};",
            qi(&op.object_name),
            qi(&op.schema),
            qi(&op.table)
        ),
        // GAP 4: ENUM type additions
        ChangeKind::CreateEnum => table
            .and_then(|table| find_column(table, &op.column))
            .and_then(|column| {
                let mut values = column.enum_values.clone();
                values.sort();
                values.dedup();
                let type_name = format!(
                    "{}.{}_{}_enum",
                    qi(&op.schema),
                    op.table,
                    column.column_name
                );
                let value_list = values
                    .iter()
                    .try_fold(Vec::new(), |mut acc, v| {
                        validate_enum_value(v).map(|_| {
                            acc.push(format!("'{}'", v.replace('\'', "''")));
                            acc
                        })
                    })
                    .ok()?
                    .into_iter()
                    .collect::<Vec<_>>()
                    .join(", ");
                Some(format!(
                    "DO $$BEGIN\n  CREATE TYPE {type_name} AS ENUM ({value_list});\nEXCEPTION WHEN duplicate_object THEN NULL;\nEND$$;"
                ))
            })
            .unwrap_or_default(),
        // GAP 4: GAP 12 — ALTER TYPE ... ADD VALUE must run outside a transaction
        // The generate_delta_sql function emits AlterEnumAddValue ops as standalone
        // (non-BEGIN/COMMIT) artifacts to satisfy PostgreSQL's restriction.
        //
        // The added value (op.column) is embedded into a SQL string literal via
        // ql() (single-quote escaping only). Validate it against the same rules
        // CreateEnum/build.rs apply (reject backslash/dollar/NUL/newline) so a
        // poisoned value can't break out of the literal — and unlike CreateEnum
        // this artifact runs with no surrounding DO block / transaction.
        ChangeKind::AlterEnumAddValue => {
            if validate_enum_value(&op.column).is_err() {
                String::new()
            } else {
                format!(
                    "-- NOTE: run outside a transaction (no BEGIN/COMMIT)\n\
                     ALTER TYPE {}.{} ADD VALUE IF NOT EXISTS {};",
                    qi(&op.schema),
                    qi(&op.object_name),
                    ql(&op.column)
                )
            }
        }
        ChangeKind::DropEnum => format!(
            "DROP TYPE IF EXISTS {}.{};",
            qi(&op.schema),
            qi(&op.object_name)
        ),
        // GAP 3: Partition management (informational stubs — actual partition
        // creation is handled by pg_partman at runtime)
        ChangeKind::CreatePartition | ChangeKind::AttachPartition
        | ChangeKind::DetachPartition | ChangeKind::DropPartition => String::new(),
        _ => String::new(),
    }
}

pub(crate) fn render_column(column: &ManifestColumn) -> String {
    let mut sql_type = if column.sql_type.trim().is_empty() {
        "TEXT".to_string()
    } else {
        column.sql_type.clone()
    };
    if column.auto_increment {
        sql_type = match sql_type.to_ascii_uppercase().as_str() {
            "BIGINT" | "INT8" => "BIGSERIAL".to_string(),
            "INTEGER" | "INT" | "INT4" => "SERIAL".to_string(),
            "SMALLINT" | "INT2" => "SMALLSERIAL".to_string(),
            _ => sql_type,
        };
    }

    let mut parts = vec![qi(&column.column_name), sql_type];
    if !column.collation.trim().is_empty() {
        parts.push(format!("COLLATE {}", qi(&column.collation)));
    }
    if column.is_identity {
        parts.push("GENERATED ALWAYS AS IDENTITY".to_string());
    } else if column.generated && !column.generated_expr.trim().is_empty() {
        parts.push(format!(
            "GENERATED ALWAYS AS ({}) STORED",
            column.generated_expr
        ));
    } else {
        if !column.default_value.trim().is_empty() {
            parts.push(format!("DEFAULT {}", column.default_value));
        }
        if column.not_null {
            parts.push("NOT NULL".to_string());
        }
    }
    if !column.check_constraint.trim().is_empty() {
        parts.push(format!("CHECK ({})", column.check_constraint));
    }
    parts.join(" ")
}

pub(crate) fn render_create_extension(extension: &ManifestExtension) -> String {
    let schema = if extension.schema.trim().is_empty() {
        "public"
    } else {
        &extension.schema
    };
    let is_optional_pg_partman = extension.name.trim() == "pg_partman";
    let mut sql = String::new();
    if is_optional_pg_partman {
        sql.push_str(&format!(
            "DO $$\n\
             BEGIN\n\
             \tIF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = {}) THEN\n",
            ql(extension.name.trim())
        ));
    }
    // Extensions installed outside the public schema require the schema to
    // exist first — PostgreSQL does NOT auto-create it during CREATE EXTENSION.
    // Guard this bootstrap block because `CREATE SCHEMA IF NOT EXISTS` is not
    // race-proof across concurrent migration runners: two sessions can both
    // observe the schema missing and one loses on `pg_namespace_nspname_index`.
    if schema != "public" {
        sql.push_str(&format!(
            "{}{} pg_advisory_xact_lock(hashtext({}));\n",
            if is_optional_pg_partman { "\t\t" } else { "" },
            if is_optional_pg_partman {
                "PERFORM"
            } else {
                "SELECT"
            },
            ql(&format!(
                "udb:create_extension:{}:{}",
                extension.name.trim(),
                schema
            ))
        ));
        sql.push_str(&format!(
            "{}CREATE SCHEMA IF NOT EXISTS {};\n",
            if is_optional_pg_partman { "\t\t" } else { "" },
            qi(schema)
        ));
    }
    sql.push_str(&format!(
        "{}CREATE EXTENSION IF NOT EXISTS {} SCHEMA {}{};",
        if is_optional_pg_partman { "\t\t" } else { "" },
        qi(&extension.name),
        qi(schema),
        if extension.version.trim().is_empty() {
            String::new()
        } else {
            format!(" VERSION {}", ql(&extension.version))
        }
    ));
    if is_optional_pg_partman {
        sql.push_str("\n\tEND IF;\nEND$$;");
    }
    sql
}

pub(crate) fn render_tablespace(table: &ManifestTable) -> String {
    if table.tablespace.trim().is_empty() || table.tablespace == "pg_default" {
        String::new()
    } else {
        format!(" TABLESPACE {}", qi(&table.tablespace))
    }
}

pub(crate) fn partition_aware_unique_columns(
    table: &ManifestTable,
    columns: &[String],
) -> Vec<String> {
    let mut effective_columns = columns.to_vec();
    if is_partitioned(table) && !table.partition_column.trim().is_empty() {
        let part_col = table.partition_column.trim().to_string();
        if !effective_columns.iter().any(|column| column == &part_col) {
            effective_columns.push(part_col);
        }
    }
    effective_columns
}

pub(crate) fn same_column_set(left: &[String], right: &[String]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .all(|column| right.iter().any(|other| other == column))
}

pub(crate) fn has_explicit_unique_index_for_columns(
    table: &ManifestTable,
    columns: &[String],
) -> bool {
    let generated = partition_aware_unique_columns(table, columns);
    table.indexes.iter().any(|index| {
        index.unique
            && same_column_set(
                &partition_aware_unique_columns(table, &index.columns),
                &generated,
            )
    })
}

pub(crate) fn sql_text_array(values: &[String]) -> String {
    if values.is_empty() {
        return "ARRAY[]::TEXT[]".to_string();
    }
    format!(
        "ARRAY[{}]::TEXT[]",
        values
            .iter()
            .map(|value| ql(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(crate) fn render_partitioned_unique_index_create(
    table: &ManifestTable,
    name: &str,
    declared_columns: &[String],
    column_sql_parts: &[String],
    method: &str,
    suffix: &str,
    where_clause: &str,
) -> String {
    if !is_partitioned(table) || table.partition_column.trim().is_empty() {
        return format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {}\n    ON {}.{} USING {} ({}){}{};\n\n",
            qi(name),
            qi(&table.schema),
            qi(&table.table),
            method,
            column_sql_parts.join(", "),
            suffix,
            where_clause
        );
    }

    let original_declared_columns = declared_columns.to_vec();
    let declared_columns = partition_aware_unique_columns(table, declared_columns);
    let mut effective_column_sql_parts = column_sql_parts.to_vec();
    for column in &declared_columns {
        if !original_declared_columns
            .iter()
            .any(|declared| declared == column)
        {
            effective_column_sql_parts.push(qi(column));
        }
    }

    format!(
        "DO $$\n\
         DECLARE\n\
         \t_rel REGCLASS := to_regclass({relation});\n\
         \t_part_col TEXT;\n\
         \t_declared_columns TEXT[] := {declared_columns};\n\
         \t_column_sql_parts TEXT[] := {column_sql_parts};\n\
         BEGIN\n\
         \tIF _rel IS NOT NULL THEN\n\
         \t\tFOR _part_col IN\n\
         \t\t\tSELECT a.attname\n\
         \t\t\tFROM pg_partitioned_table p\n\
         \t\t\tJOIN LATERAL unnest(p.partattrs) WITH ORDINALITY AS key(attnum, ord) ON TRUE\n\
         \t\t\tJOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = key.attnum\n\
         \t\t\tWHERE p.partrelid = _rel\n\
         \t\t\tORDER BY key.ord\n\
         \t\tLOOP\n\
         \t\t\tIF NOT (_part_col = ANY(_declared_columns)) THEN\n\
         \t\t\t\t_declared_columns := _declared_columns || _part_col;\n\
         \t\t\t\t_column_sql_parts := _column_sql_parts || format('%I', _part_col);\n\
         \t\t\tEND IF;\n\
         \t\tEND LOOP;\n\
         \tEND IF;\n\
         \n\
         \tEXECUTE format(\n\
         \t\t'CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I USING %s (%s)',\n\
         \t\t{name_lit}, {schema_lit}, {table_lit}, {method_lit}, array_to_string(_column_sql_parts, ', ')\n\
         \t) || {extra_sql};\n\
         END$$;\n\n",
        relation = ql(&format!("{}.{}", table.schema, table.table)),
        declared_columns = sql_text_array(&declared_columns),
        column_sql_parts = sql_text_array(&effective_column_sql_parts),
        name_lit = ql(name),
        schema_lit = ql(&table.schema),
        table_lit = ql(&table.table),
        method_lit = ql(method),
        extra_sql = ql(&format!("{suffix}{where_clause}")),
    )
}

pub(crate) fn render_partition_unique_constraint_repair(table: &ManifestTable) -> String {
    render_partition_unique_constraint_repair_block(table, true)
}

pub(crate) fn render_partition_unique_constraint_cleanup(table: &ManifestTable) -> String {
    render_partition_unique_constraint_repair_block(table, false)
}

pub(crate) fn render_partition_unique_constraint_repair_block(
    table: &ManifestTable,
    recreate_primary_key: bool,
) -> String {
    if !is_partitioned(table) || table.partition_column.trim().is_empty() {
        return String::new();
    }

    let part_col = table.partition_column.trim();
    let pk_columns = partition_aware_unique_columns(table, &table.primary_key);
    let pk_column_sql_parts = pk_columns
        .iter()
        .map(|column| qi(column))
        .collect::<Vec<_>>();
    let add_pk = if !recreate_primary_key || pk_columns.is_empty() {
        String::new()
    } else {
        format!(
            "\nIF NOT EXISTS (\n\
             \tSELECT 1 FROM pg_constraint\n\
             \tWHERE conrelid = {relation}::regclass\n\
             \t  AND contype = 'p'\n\
             \t  AND conname = {pk_name}\n\
             ) THEN\n\
             \t_pk_declared_columns := {pk_declared_columns};\n\
             \t_pk_column_sql_parts := {pk_column_sql_parts};\n\
             \tFOREACH _part_col IN ARRAY _required_partition_cols LOOP\n\
             \t\tIF NOT (_part_col = ANY(_pk_declared_columns)) THEN\n\
             \t\t\t_pk_declared_columns := _pk_declared_columns || _part_col;\n\
             \t\t\t_pk_column_sql_parts := _pk_column_sql_parts || format('%I', _part_col);\n\
             \t\tEND IF;\n\
             \tEND LOOP;\n\
             \tEXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I PRIMARY KEY (%s)', {schema_lit}, {table_lit}, {pk_name}, array_to_string(_pk_column_sql_parts, ', '));\n\
             END IF;",
            relation = ql(&format!("{}.{}", table.schema, table.table)),
            pk_name = ql(&format!("pk_{}", table.table)),
            schema_lit = ql(&table.schema),
            table_lit = ql(&table.table),
            pk_declared_columns = sql_text_array(&pk_columns),
            pk_column_sql_parts = sql_text_array(&pk_column_sql_parts),
        )
    };

    format!(
        "\nDO $$\n\
         DECLARE\n\
         \t_constraint RECORD;\n\
         \t_index RECORD;\n\
         \t_part_col TEXT;\n\
         \t_required_partition_cols TEXT[] := {required_partition_cols};\n\
         \t_pk_declared_columns TEXT[] := ARRAY[]::TEXT[];\n\
         \t_pk_column_sql_parts TEXT[] := ARRAY[]::TEXT[];\n\
         BEGIN\n\
         \tIF to_regclass({relation}) IS NULL THEN\n\
         \t\tRETURN;\n\
         \tEND IF;\n\
         \n\
         \tFOR _part_col IN\n\
         \t\tSELECT a.attname\n\
         \t\tFROM pg_partitioned_table p\n\
         \t\tJOIN LATERAL unnest(p.partattrs) WITH ORDINALITY AS key(attnum, ord) ON TRUE\n\
         \t\tJOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = key.attnum\n\
         \t\tWHERE p.partrelid = {relation}::regclass\n\
         \t\tORDER BY key.ord\n\
         \tLOOP\n\
         \t\tIF NOT (_part_col = ANY(_required_partition_cols)) THEN\n\
         \t\t\t_required_partition_cols := _required_partition_cols || _part_col;\n\
         \t\tEND IF;\n\
         \tEND LOOP;\n\
         \n\
         \tFOR _constraint IN\n\
         \t\tSELECT c.conname\n\
         \t\tFROM pg_constraint c\n\
         \t\tWHERE c.conrelid = {relation}::regclass\n\
         \t\t  AND c.contype IN ('p', 'u')\n\
         \t\t  AND EXISTS (\n\
         \t\t\tSELECT 1\n\
         \t\t\tFROM unnest(_required_partition_cols) AS required(attname)\n\
         \t\t\tWHERE NOT EXISTS (\n\
         \t\t\t\tSELECT 1\n\
         \t\t\t\tFROM unnest(c.conkey) AS key(attnum)\n\
         \t\t\t\tJOIN pg_attribute a\n\
         \t\t\t\t  ON a.attrelid = c.conrelid\n\
         \t\t\t\t AND a.attnum = key.attnum\n\
         \t\t\t\tWHERE a.attname = required.attname\n\
         \t\t\t)\n\
         \t\t  )\n\
         \tLOOP\n\
         \t\tEXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I CASCADE', {schema_lit}, {table_lit}, _constraint.conname);\n\
         \tEND LOOP;\n\
         \n\
         \tFOR _index IN\n\
         \t\tSELECT ic.relname\n\
         \t\tFROM pg_index i\n\
         \t\tJOIN pg_class ic ON ic.oid = i.indexrelid\n\
         \t\tWHERE i.indrelid = {relation}::regclass\n\
         \t\t  AND i.indisunique\n\
         \t\t  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.indexrelid)\n\
         \t\t  AND EXISTS (\n\
         \t\t\tSELECT 1\n\
         \t\t\tFROM unnest(_required_partition_cols) AS required(attname)\n\
         \t\t\tWHERE NOT EXISTS (\n\
         \t\t\t\tSELECT 1\n\
         \t\t\t\tFROM unnest(i.indkey) AS key(attnum)\n\
         \t\t\t\tJOIN pg_attribute a\n\
         \t\t\t\t  ON a.attrelid = i.indrelid\n\
         \t\t\t\t AND a.attnum = key.attnum\n\
         \t\t\t\tWHERE a.attname = required.attname\n\
         \t\t\t)\n\
         \t\t  )\n\
         \tLOOP\n\
         \t\tEXECUTE format('DROP INDEX IF EXISTS %I.%I', {schema_lit}, _index.relname);\n\
         \tEND LOOP;{add_pk}\n\
         END$$;\n\n",
        relation = ql(&format!("{}.{}", table.schema, table.table)),
        required_partition_cols = sql_text_array(&[part_col.to_string()]),
        schema_lit = ql(&table.schema),
        table_lit = ql(&table.table),
        add_pk = add_pk,
    )
}
