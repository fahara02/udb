//! sql.rs split — render_ext (Phase I).
use super::*;

pub(crate) fn render_materialized_view(view: &ManifestMaterializedView) -> String {
    let with_data = if view.with_data {
        "WITH DATA"
    } else {
        "WITH NO DATA"
    };
    format!(
        "\nCREATE SCHEMA IF NOT EXISTS {};\nCREATE MATERIALIZED VIEW IF NOT EXISTS {}.{} AS\n{}\n{};\n",
        qi(&view.schema),
        qi(&view.schema),
        qi(&view.name),
        view.query,
        with_data
    )
}

pub(crate) fn render_sql_artifacts(table: &ManifestTable, phase: &str) -> String {
    let mut out = String::new();
    for artifact in &table.sql_artifacts {
        if !artifact_applies_to_phase(artifact, phase) {
            continue;
        }
        if !artifact.backend.trim().is_empty() && artifact.backend != "postgres" {
            continue;
        }
        if !artifact.sql.trim().is_empty() {
            out.push_str(&format!(
                "\n-- UDB:sql_artifact={}\n-- UDB:sql_artifact_phase={}\n",
                artifact.name, phase
            ));
            if !artifact.checksum_sha256.trim().is_empty() {
                out.push_str(&format!(
                    "-- UDB:sql_artifact_sha256={}\n",
                    artifact.checksum_sha256
                ));
            }
            out.push_str(artifact.sql.trim());
            out.push_str("\n\n");
        } else if !artifact.file.trim().is_empty() {
            out.push_str(&format!(
                "\n-- UDB:sql_artifact={} file={} phase={} sha256={}\n",
                artifact.name, artifact.file, phase, artifact.checksum_sha256
            ));
        }
    }
    out
}

pub(crate) fn artifact_applies_to_phase(artifact: &ManifestSqlArtifact, phase: &str) -> bool {
    let artifact_phase = artifact.phase.trim();
    if artifact_phase.is_empty() {
        return phase == "before_triggers";
    }
    artifact_phase == phase
}

pub(crate) fn render_trigger(trigger: &ManifestTrigger) -> String {
    let timing = if trigger.timing.trim().is_empty() {
        "AFTER"
    } else {
        trigger.timing.as_str()
    };
    let event = if trigger.event.trim().is_empty() {
        "INSERT"
    } else {
        trigger.event.as_str()
    };
    let for_each = if trigger.for_each.trim().eq_ignore_ascii_case("STATEMENT") {
        "STATEMENT"
    } else {
        "ROW"
    };
    let when_clause = if trigger.when_clause.trim().is_empty() {
        String::new()
    } else {
        format!("\n    WHEN ({})", trigger.when_clause)
    };
    format!(
        "\nDROP TRIGGER IF EXISTS {} ON {}.{};\nCREATE TRIGGER {}\n    {} {} ON {}.{}\n    FOR EACH {}{}\n    EXECUTE FUNCTION {};\n",
        qi(&trigger.name),
        qi(&trigger.schema),
        qi(&trigger.table),
        qi(&trigger.name),
        timing,
        event,
        qi(&trigger.schema),
        qi(&trigger.table),
        for_each,
        when_clause,
        trigger.function
    )
}

pub(crate) fn render_add_fk(
    schema: &str,
    table: &str,
    fk: &ManifestForeignKey,
    table_is_partitioned: bool,
) -> String {
    let name = derive_fk_name(table, fk);
    let mut inner = format!(
        "ALTER TABLE {}.{}\n    ADD CONSTRAINT {}\n    FOREIGN KEY ({}) REFERENCES {}.{} ({})",
        qi(schema),
        qi(table),
        qi(&name),
        quote_list(&fk.columns),
        qi(&fk.ref_schema),
        qi(&fk.ref_table),
        quote_list(&fk.ref_columns)
    );
    if !fk.on_delete.trim().is_empty() && fk.on_delete != "NO ACTION" {
        inner.push_str(&format!("\n    ON DELETE {}", fk.on_delete));
    }
    if !fk.on_update.trim().is_empty() && fk.on_update != "NO ACTION" {
        inner.push_str(&format!("\n    ON UPDATE {}", fk.on_update));
    }
    if fk.deferrable {
        inner.push_str("\n    DEFERRABLE");
        if fk.initially_deferred {
            inner.push_str(" INITIALLY DEFERRED");
        } else {
            inner.push_str(" INITIALLY IMMEDIATE");
        }
    }
    if fk.not_valid && !table_is_partitioned {
        // NOT VALID adds the FK without scanning existing rows (fast, low lock
        // contention).  Validation must happen in a SEPARATE migration using
        // `VALIDATE CONSTRAINT` — auto-appending it here would negate the benefit
        // because both statements would hold locks for the full row-scan duration.
        // NOTE: PostgreSQL does not support NOT VALID on FKs from partitioned tables;
        // silently omit it when the table is partitioned.
        inner.push_str("\n    NOT VALID");
    }
    inner.push(';');
    // Wrap in a DO block so re-runs are idempotent: if the constraint already
    // exists the duplicate_object exception is silently swallowed.
    //
    // The live referenced table may already be partitioned on columns that differ
    // from today's proto manifest (for example after changing partition_column on
    // an existing parent table). PostgreSQL requires FKs to partitioned parents to
    // reference every live partition key column. If the child FK does not carry
    // those columns, skip this FK with a NOTICE rather than aborting force-sync.
    format!(
        "DO $$\n\
         DECLARE\n\
         \t_missing_partition_cols TEXT[];\n\
         BEGIN\n\
         \tSELECT COALESCE(array_agg(a.attname ORDER BY key.ord), ARRAY[]::TEXT[])\n\
         \tINTO _missing_partition_cols\n\
         \tFROM pg_partitioned_table p\n\
         \tJOIN LATERAL unnest(p.partattrs) WITH ORDINALITY AS key(attnum, ord) ON TRUE\n\
         \tJOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = key.attnum\n\
         \tWHERE p.partrelid = to_regclass({ref_relation})\n\
         \t  AND NOT (a.attname = ANY({ref_columns}));\n\
         \n\
         \tIF COALESCE(array_length(_missing_partition_cols, 1), 0) > 0 THEN\n\
         \t\tRAISE NOTICE 'Skipping FK {fk_name}: referenced partition key columns % are not present in ref_columns {ref_columns_notice}', _missing_partition_cols;\n\
         \t\tRETURN;\n\
         \tEND IF;\n\
         \n\
         \t{inner}\n\
         EXCEPTION WHEN duplicate_object THEN NULL;\n\
         END$$;",
        ref_relation = ql(&format!("{}.{}", fk.ref_schema, fk.ref_table)),
        ref_columns = sql_text_array(&fk.ref_columns),
        fk_name = name.replace('\'', "''"),
        ref_columns_notice = fk.ref_columns.join(", ").replace('\'', "''"),
        inner = inner
    )
}

pub(crate) fn render_add_check(schema: &str, table: &str, check: &ManifestCheck) -> String {
    let name = if check.name.trim().is_empty() {
        format!("chk_{}_auto", table)
    } else {
        check.name.clone()
    };
    // Wrap in anonymous DO block so the operation is idempotent:
    // if the constraint already exists (duplicate_object) we silently skip,
    // matching the same pattern used by render_add_fk.
    format!(
        "DO $$BEGIN\n  ALTER TABLE {}.{} ADD CONSTRAINT {} CHECK ({});\nEXCEPTION WHEN duplicate_object THEN NULL;\nEND$$;",
        qi(schema),
        qi(table),
        qi(&name),
        check.expression
    )
}

pub(crate) fn render_policy(schema: &str, table: &str, policy: &ManifestPolicy) -> String {
    // GAP 8: Use ALTER POLICY when the policy already exists to avoid the security
    // gap introduced by DROP + CREATE (table is momentarily without the policy if
    // CREATE fails).  A DO block tries ALTER first and falls back to CREATE for new
    // policies — this is safe for both bootstrap and delta runs.
    let mode = if policy.permissive {
        "PERMISSIVE"
    } else {
        "RESTRICTIVE"
    };
    let command = if policy.command.trim().is_empty() {
        "ALL"
    } else {
        policy.command.as_str()
    };
    let using = policy.using_expression.trim();
    let check = policy.with_check.trim();

    let using_clause = if using.is_empty() {
        String::new()
    } else {
        format!("\n        USING ({})", using)
    };
    let check_clause = if check.is_empty() {
        String::new()
    } else {
        format!("\n        WITH CHECK ({})", check)
    };

    // The ALTER POLICY branch handles updates to USING / WITH CHECK without dropping.
    // The EXCEPTION branch creates the policy if it does not yet exist.
    format!(
        "DO $$BEGIN\n\
         \tALTER POLICY {name} ON {schema}.{table}{using_clause}{check_clause};\n\
         EXCEPTION WHEN undefined_object THEN\n\
         \tCREATE POLICY {name} ON {schema}.{table}\n\
         \t    AS {mode} FOR {command}{using_clause}{check_clause};\n\
         END$$;",
        name = qi(&policy.name),
        schema = qi(schema),
        table = qi(table),
        mode = mode,
        command = command,
        using_clause = using_clause,
        check_clause = check_clause,
    )
}

pub(crate) fn find_column<'a>(table: &'a ManifestTable, name: &str) -> Option<&'a ManifestColumn> {
    table
        .columns
        .iter()
        .find(|column| column.column_name == name)
}

/// Derive the auto-generated name for an index, honoring an explicit `name`.
/// Single source of truth shared by the finder (`find_index`) and the renderer
/// (`render_index_impl`) so the two cannot drift apart.
pub(crate) fn derive_index_name(table: &ManifestTable, index: &ManifestIndex) -> String {
    if index.name.trim().is_empty() {
        format!(
            "idx_{}_{}_{}",
            table.schema,
            table.table,
            index.columns.join("_")
        )
    } else {
        index.name.clone()
    }
}

/// Derive the auto-generated name for a foreign key, honoring an explicit `name`.
/// Single source of truth shared by the finder (`find_fk`) and the renderer
/// (`render_add_fk`).
pub(crate) fn derive_fk_name(table: &str, fk: &ManifestForeignKey) -> String {
    if fk.name.trim().is_empty() {
        format!("fk_{}_{}", table, fk.columns.join("_"))
    } else {
        fk.name.clone()
    }
}

pub(crate) fn find_index<'a>(table: &'a ManifestTable, name: &str) -> Option<&'a ManifestIndex> {
    table
        .indexes
        .iter()
        .find(|index| derive_index_name(table, index) == name)
}

pub(crate) fn find_fk<'a>(table: &'a ManifestTable, name: &str) -> Option<&'a ManifestForeignKey> {
    table
        .foreign_keys
        .iter()
        .find(|fk| derive_fk_name(&table.table, fk) == name)
}

pub(crate) fn find_check<'a>(table: &'a ManifestTable, name: &str) -> Option<&'a ManifestCheck> {
    table.checks.iter().find(|check| {
        if check.name.trim().is_empty() {
            check.expression == name
        } else {
            check.name == name
        }
    })
}

pub(crate) fn find_policy<'a>(table: &'a ManifestTable, name: &str) -> Option<&'a ManifestPolicy> {
    table.rls_policies.iter().find(|policy| policy.name == name)
}

pub(crate) fn find_extension<'a>(
    manifest: &'a CatalogManifest,
    schema: &str,
    name: &str,
) -> Option<&'a ManifestExtension> {
    manifest
        .tables
        .iter()
        .flat_map(|table| table.extensions.iter())
        .find(|extension| extension.schema == schema && extension.name == name)
}

pub(crate) fn find_materialized_view<'a>(
    table: &'a ManifestTable,
    object_name: &str,
) -> Option<&'a ManifestMaterializedView> {
    table
        .materialized_views
        .iter()
        .find(|view| format!("{}.{}", view.schema, view.name) == object_name)
}

pub(crate) fn find_trigger<'a>(
    table: &'a ManifestTable,
    name: &str,
) -> Option<&'a ManifestTrigger> {
    table.triggers.iter().find(|trigger| trigger.name == name)
}

/// Render an index DDL statement.
///
/// `in_transaction` controls whether CONCURRENTLY can be emitted: inside a
/// `BEGIN/COMMIT` block PostgreSQL forbids CONCURRENTLY, so we downgrade
/// automatically to a plain (locking) index creation.
/// Render an index DDL statement for a **standalone, non-transactional**
/// artifact (`in_transaction=false`), so `CONCURRENTLY` is preserved for
/// concurrent non-unique indexes. Used by the bootstrap/delta concurrent-index
/// artifacts (#120); the content MUST be the single statement (no surrounding
/// `SET`/`BEGIN`) so the applier runs it in autocommit.
pub(crate) fn render_index_standalone(table: &ManifestTable, index: &ManifestIndex) -> String {
    render_index_impl(table, index, /*in_transaction=*/ false)
}

#[cfg(test)]
pub(crate) fn render_index(table: &ManifestTable, index: &ManifestIndex) -> String {
    render_index_impl(table, index, /*in_transaction=*/ false)
}

pub(crate) fn render_index_in_tx(table: &ManifestTable, index: &ManifestIndex) -> String {
    render_index_impl(table, index, /*in_transaction=*/ true)
}

pub(crate) fn render_index_impl(
    table: &ManifestTable,
    index: &ManifestIndex,
    in_transaction: bool,
) -> String {
    if index.columns.is_empty() {
        return String::new();
    }
    let unique = if index.unique { "UNIQUE " } else { "" };
    // GAP 8: CONCURRENTLY is only valid outside a transaction; unique indexes
    // also cannot be created concurrently in PostgreSQL.
    let concurrently = if index.concurrent && !in_transaction && !index.unique {
        "CONCURRENTLY "
    } else {
        ""
    };
    let name = derive_index_name(table, index);
    let raw_method = if index.method.trim().is_empty() {
        "BTREE".to_string()
    } else {
        index.method.to_ascii_uppercase()
    };
    // PostgreSQL only supports unique indexes with the BTREE access method.
    // Automatically downgrade to BTREE when uniqueness is requested with an
    // incompatible method (e.g. HASH) to avoid DDL errors at apply time.
    let method = if index.unique && raw_method != "BTREE" {
        "BTREE".to_string()
    } else {
        raw_method
    };

    // PostgreSQL requires UNIQUE indexes on partitioned tables to include the
    // partition key column. Keep that rule centralized so bootstrap and delta
    // SQL cannot drift apart.
    let effective_columns = if index.unique {
        partition_aware_unique_columns(table, &index.columns)
    } else {
        index.columns.clone()
    };

    let column_sql_parts = effective_columns
        .iter()
        .map(|column| {
            // Resolve operator class: explicit > auto-default for vector index methods.
            let op_class = if !index.operator_class.trim().is_empty() {
                index.operator_class.as_str().to_string()
            } else if matches!(method.as_str(), "HNSW" | "IVFFLAT") {
                // pgvector requires an explicit operator class for ANN indexes.
                // Default to cosine similarity, which is the most common choice
                // for embedding search. Users can override via operator_class.
                "vector_cosine_ops".to_string()
            } else {
                String::new()
            };
            if op_class.is_empty() {
                col_or_expr(column)
            } else {
                format!("{} {}", col_or_expr(column), op_class)
            }
        })
        .collect::<Vec<_>>();
    let include = if index.include_columns.is_empty() {
        String::new()
    } else {
        format!(" INCLUDE ({})", quote_list(&index.include_columns))
    };
    let params = if index.index_params.is_empty() {
        String::new()
    } else {
        format!(
            " WITH ({})",
            index
                .index_params
                .iter()
                .map(|param| format!("{} = {}", param.key, param.value))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let where_clause = if index.where_clause.trim().is_empty() {
        String::new()
    } else {
        format!("\n    WHERE {}", index.where_clause)
    };
    if index.unique {
        return render_partitioned_unique_index_create(
            table,
            &name,
            &effective_columns,
            &column_sql_parts,
            &method,
            &format!("{params}{include}"),
            &where_clause,
        );
    }
    format!(
        "CREATE {}INDEX {}IF NOT EXISTS {}\n    ON {}.{} USING {} ({}){}{}{};\n\n",
        unique,
        concurrently,
        qi(&name),
        qi(&table.schema),
        qi(&table.table),
        method,
        column_sql_parts.join(", "),
        params,
        include,
        where_clause
    )
}

// ── GAP 4: ENUM type DDL ───────────────────────────────────────────────────────

// Emit idempotent CREATE TYPE … AS ENUM DDL for every column that declares
// `enum_values`. The enum type name is derived as `<table>_<column>_enum`
// scoped to the table's schema. PostgreSQL does not have CREATE TYPE IF NOT
// EXISTS, so we wrap inside a DO block that ignores duplicate_object.
// ── GAP 31: ENUM value validation ────────────────────────────────────────────

/// Validate a single ENUM value before embedding it in generated SQL DDL.
///
/// The `replace('\'', "''")` escaping technique is insufficient against
/// dollar-quoting escape sequences (`$$...$$`, `E'...'`).  Rejecting unsafe
/// characters at source is the safer approach.
///
/// Returns `Ok(())` when the value is safe to embed; `Err(reason)` otherwise.
pub(crate) fn validate_enum_value(v: &str) -> Result<(), String> {
    if v.is_empty() {
        return Err("enum value cannot be empty".to_string());
    }
    if v.len() > 128 {
        return Err(format!(
            "enum value exceeds 128 characters (got {})",
            v.len()
        ));
    }
    // Single quotes, backslashes, dollar signs, NUL bytes, and newlines can all
    // break out of PostgreSQL string literals or the surrounding DO $$ block.
    if v.contains(['\'', '\\', '$', '\0', '\n', '\r']) {
        return Err(
            "enum value contains an unsafe character (quote, backslash, dollar, null, or newline)"
                .to_string(),
        );
    }
    Ok(())
}

pub(crate) fn render_enum_types(table: &ManifestTable) -> String {
    let mut out = String::new();
    for column in &table.columns {
        if column.enum_values.is_empty() {
            continue;
        }
        let mut values = column.enum_values.clone();
        values.sort();
        values.dedup();
        let type_name = format!(
            "{}.{}_{}_enum",
            qi(&table.schema),
            table.table,
            column.column_name
        );
        let value_list = values
            .iter()
            .filter(|v| {
                // GAP 31: skip values that could break out of the SQL literal or
                // the surrounding DO $$…END$$ block.
                if let Err(reason) = validate_enum_value(v) {
                    tracing::warn!(
                        enum_type = %type_name,
                        value = %v,
                        reason = %reason,
                        "skipping unsafe enum value in DDL generation"
                    );
                    return false;
                }
                true
            })
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "DO $$BEGIN\n  CREATE TYPE {type_name} AS ENUM ({value_list});\nEXCEPTION WHEN duplicate_object THEN NULL;\nEND$$;\n\n"
        ));
    }
    out
}

// ── GAP 1: JSONB auto GIN index ───────────────────────────────────────────────

/// Emit `CREATE INDEX … USING GIN` for every column marked `is_jsonb: true`.
/// The index name is `gin_<schema>_<table>_<column>`.
/// When `json_path_ops: true` the `jsonb_path_ops` operator class is used
/// (smaller index, only supports `@>` / `@@`; otherwise full GIN is created).
pub(crate) fn render_jsonb_gin_indexes(table: &ManifestTable) -> String {
    let mut out = String::new();
    for column in &table.columns {
        if !column.is_jsonb {
            continue;
        }
        let op_class = if column.json_path_ops {
            " jsonb_path_ops".to_string()
        } else {
            String::new()
        };
        let name = format!(
            "gin_{}_{}_{}",
            table.schema, table.table, column.column_name
        );
        out.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS {}\n    ON {}.{} USING GIN ({}{});\n\n",
            qi(&name),
            qi(&table.schema),
            qi(&table.table),
            qi(&column.column_name),
            op_class
        ));
    }
    out
}

// ── GAP 5: tsvector + trigram indexes ─────────────────────────────────────────

/// Emit FTS expression indexes for `is_tsvector` columns and pg_trgm GIN indexes
/// for `trigram_index` columns.  These require the `pg_trgm` extension to be
/// already installed (declare it in the proto extensions block).
pub(crate) fn render_tsvector_indexes(table: &ManifestTable) -> String {
    let mut out = String::new();
    for column in &table.columns {
        // tsvector FTS expression index
        if column.is_tsvector && !column.tsvector_source_columns.is_empty() {
            // Validate the language name: PostgreSQL regconfig identifiers must
            // be plain alphanumeric + underscores. Embedding an unvalidated value
            // directly inside a single-quoted string literal is a SQL injection
            // risk (e.g. a value of "english', evil_fn()--" breaks out).
            let raw_lang = if column.tsvector_language.trim().is_empty() {
                "simple"
            } else {
                column.tsvector_language.as_str()
            };
            let lang = if raw_lang
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                raw_lang
            } else {
                tracing::warn!(
                    schema = %table.schema,
                    table = %table.table,
                    column = %column.column_name,
                    lang = %raw_lang,
                    "tsvector_language contains unsafe characters — falling back to 'simple'"
                );
                "simple"
            };
            let concat = column
                .tsvector_source_columns
                .iter()
                .map(|c| format!("coalesce({}, '')", qi(c)))
                .collect::<Vec<_>>()
                .join(" || ' ' || ");
            let idx_name = format!(
                "idx_fts_{}_{}_{}",
                table.schema, table.table, column.column_name
            );
            out.push_str(&format!(
                "CREATE INDEX IF NOT EXISTS {}\n    ON {}.{} USING GIN (to_tsvector('{}', {}));\n\n",
                qi(&idx_name),
                qi(&table.schema),
                qi(&table.table),
                lang,
                concat,
            ));
        }
        // pg_trgm GIN index for ILIKE search
        if column.trigram_index {
            let idx_name = format!(
                "idx_trgm_{}_{}_{}",
                table.schema, table.table, column.column_name
            );
            out.push_str(&format!(
                "CREATE INDEX IF NOT EXISTS {}\n    ON {}.{} USING GIN ({} gin_trgm_ops);\n\n",
                qi(&idx_name),
                qi(&table.schema),
                qi(&table.table),
                qi(&column.column_name),
            ));
        }
    }
    out
}

// ── GAP 3: Partition child setup ──────────────────────────────────────────────

/// Emit pg_partman `create_parent()` call and optional DEFAULT partition.
/// A table can provide an explicit `partition_interval`; otherwise the UDB
/// partition strategy enum is the source of truth (`RANGE_MONTH` -> monthly).
pub(crate) fn render_partition_setup(table: &ManifestTable) -> String {
    if !is_partitioned(table) {
        return String::new();
    }
    let interval = partition_interval_for_table(table);
    if interval.trim().is_empty() {
        return String::new();
    }
    let premake = if table.partition_premake > 0 {
        table.partition_premake
    } else {
        4
    };
    let mut out = String::new();
    out.push_str(&render_partition_unique_constraint_cleanup(table));
    // pg_partman call — idempotent: skip if this table is already registered
    // in partman.part_config to avoid duplicate key violations on re-runs.
    // All user-controlled string arguments are passed through ql() (single-quote
    // escaping) to prevent SQL injection. p_premake is an integer — safe as-is.
    // The fully-qualified table name must use the raw schema/table strings inside
    // a string literal (pg_partman accepts a text argument, not an identifier),
    // so we use ql() on the concatenated form rather than qi().
    out.push_str(&format!(
        "DO $$\n\
         DECLARE\n\
         \t_rel REGCLASS := to_regclass({parent_table});\n\
         \t_control_col TEXT := {part_col};\n\
         BEGIN\n\
         IF _rel IS NOT NULL THEN\n\
         \tSELECT a.attname\n\
         \tINTO _control_col\n\
         \tFROM pg_partitioned_table p\n\
         \tJOIN LATERAL unnest(p.partattrs) WITH ORDINALITY AS key(attnum, ord) ON TRUE\n\
         \tJOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = key.attnum\n\
         \tWHERE p.partrelid = _rel\n\
         \tORDER BY key.ord\n\
         \tLIMIT 1;\n\
         \t_control_col := COALESCE(_control_col, {part_col});\n\
         END IF;\n\
         \n\
         IF NOT EXISTS (\n\
         \t    SELECT 1 FROM partman.part_config WHERE parent_table = {parent_table}\n\
         ) THEN\n\
         \tPERFORM partman.create_parent(\n\
         \t\tp_parent_table := {parent_table},\n\
         \t\tp_control := _control_col,\n\
         \t\tp_interval := {interval},\n\
         \t\tp_premake := {premake},\n\
         \t\tp_start_partition := to_char(now(), 'YYYY-MM-DD'),\n\
         \t\tp_date_trunc_interval := {date_trunc_interval}\n\
         \t);\n\
         END IF;\n\
         END$$;\n\n",
        parent_table = ql(&format!("{}.{}", table.schema, table.table)),
        part_col = ql(&table.partition_column),
        interval = ql(&normalize_partition_interval(&interval)),
        premake = premake,
        date_trunc_interval = partition_date_trunc_interval(&interval)
            .map(|value| ql(&value))
            .unwrap_or_else(|| "NULL".to_string()),
    ));

    // DEFAULT partition: catches rows whose partition key falls outside every
    // premade child range (without it such an INSERT errors). pg_partman does
    // not create this automatically, so emit it when the table opts in.
    let parent_ident = format!("{}.{}", qi(&table.schema), qi(&table.table));
    if table.partition_default {
        let default_ident = format!(
            "{}.{}",
            qi(&table.schema),
            qi(&format!("{}_default", table.table))
        );
        out.push_str(&format!(
            "CREATE TABLE IF NOT EXISTS {default_ident} PARTITION OF {parent_ident} DEFAULT;\n\n"
        ));
    }

    // Retention: configure pg_partman to drop child partitions older than the
    // declared window. retention_keep_table=false detaches AND drops the child
    // (rather than leaving an orphaned table). run_maintenance enforces it.
    if table.retention_days > 0 {
        out.push_str(&format!(
            "UPDATE partman.part_config\n\
             \tSET retention = {retention}, retention_keep_table = false\n\
             \tWHERE parent_table = {parent_lit};\n\n",
            retention = ql(&format!("{} days", table.retention_days)),
            parent_lit = ql(&format!("{}.{}", table.schema, table.table)),
        ));
    }
    out
}

pub(crate) fn is_partitioned(table: &ManifestTable) -> bool {
    !table.partition_strategy.trim().is_empty()
        && !table.partition_column.trim().is_empty()
        && !table.partition_strategy.ends_with("NONE")
        && !table.partition_strategy.ends_with("UNSPECIFIED")
}

/// Translate human-readable partition interval names to valid PostgreSQL
/// interval strings accepted by pg_partman 5.x.
pub(crate) fn normalize_partition_interval(interval: &str) -> String {
    match interval.trim().to_ascii_uppercase().as_str() {
        "MONTHLY" | "MONTH" => "1 month".to_string(),
        "WEEKLY" | "WEEK" => "1 week".to_string(),
        "DAILY" | "DAY" => "1 day".to_string(),
        "HOURLY" | "HOUR" => "1 hour".to_string(),
        "YEARLY" | "YEAR" => "1 year".to_string(),
        _ => interval.to_string(), // already a valid PG interval string
    }
}

fn partition_interval_for_table(table: &ManifestTable) -> String {
    let explicit = table.partition_interval.trim();
    if !explicit.is_empty() {
        return explicit.to_string();
    }
    match table
        .partition_strategy
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        value if value.contains("RANGE_YEAR") => "YEARLY".to_string(),
        value if value.contains("RANGE_MONTH") => "MONTHLY".to_string(),
        value if value.contains("RANGE_WEEK") => "WEEKLY".to_string(),
        value if value.contains("RANGE_DAY") => "DAILY".to_string(),
        value if value.contains("RANGE_HOUR") => "HOURLY".to_string(),
        _ => String::new(),
    }
}

fn partition_date_trunc_interval(interval: &str) -> Option<String> {
    match interval.trim().to_ascii_uppercase().as_str() {
        "YEARLY" | "YEAR" | "1 YEAR" => Some("year".to_string()),
        "MONTHLY" | "MONTH" | "1 MONTH" => Some("month".to_string()),
        "WEEKLY" | "WEEK" | "1 WEEK" => Some("week".to_string()),
        "DAILY" | "DAY" | "1 DAY" => Some("day".to_string()),
        "HOURLY" | "HOUR" | "1 HOUR" => Some("hour".to_string()),
        _ => None,
    }
}

pub(crate) fn normalize_partition_strategy(value: &str) -> &'static str {
    let value = value.to_ascii_uppercase();
    if value.contains("LIST") {
        "LIST"
    } else if value.contains("HASH") {
        "HASH"
    } else {
        "RANGE"
    }
}

pub(crate) fn quote_list(values: &[String]) -> String {
    values
        .iter()
        .map(|value| qi(value))
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn first_non_empty<'a>(a: &'a str, b: &'a str, fallback: &'a str) -> &'a str {
    if !a.trim().is_empty() {
        a
    } else if !b.trim().is_empty() {
        b
    } else {
        fallback
    }
}

pub(crate) fn delta_slug(ops: &[&ChangeOperation]) -> String {
    let mut parts = ops
        .iter()
        .map(|op| format!("{:?}", op.kind).to_ascii_lowercase())
        .collect::<Vec<_>>();
    parts.sort();
    parts.dedup();
    let slug = parts.join("_");
    if slug.len() > 64 {
        slug[..64].to_string()
    } else {
        slug
    }
}

pub(crate) fn split_qualified_name(value: &str, fallback_schema: &str) -> (String, String) {
    if let Some((schema, name)) = value.split_once('.') {
        (schema.to_string(), name.to_string())
    } else {
        (fallback_schema.to_string(), value.to_string())
    }
}

// Canonical SQL identifier quoter — single source shared across generation,
// planning, and runtime (runtime's `executor_utils::qi_runtime` delegates here).
pub(crate) fn qi(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Render a column reference OR a raw SQL expression.
/// If `col` contains `(` or `'` it is treated as a SQL expression and passed
/// through as-is (e.g. `date_trunc('day', requested_at)`). Otherwise it is
/// quoted as a plain identifier with `qi()`.
pub(crate) fn col_or_expr(col: &str) -> String {
    if col.contains('(') || col.contains('\'') {
        col.to_string()
    } else {
        qi(col)
    }
}

pub(crate) fn ql(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

// generated_at_unix() intentionally removed — embedding timestamps in artifact
// content broke idempotency: every run produced a new SHA-256 checksum for
// otherwise-identical artifacts, causing the skip logic in execute_sql_artifacts
// to treat every artifact as unapplied. Timestamps belong in the audit ledger
// (schema_migrations.applied_at), not in the SQL content itself.
