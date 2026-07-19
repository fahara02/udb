use serde::{Deserialize, Serialize};

use crate::ast::ProtoSchema;
use crate::migration::diff::{ChangeKind, ChangeOperation, ChangeSafety};

use super::manifest::{
    CatalogManifest, ManifestCheck, ManifestColumn, ManifestExtension, ManifestForeignKey,
    ManifestIndex, ManifestMaterializedView, ManifestPolicy, ManifestSqlArtifact, ManifestTable,
    ManifestTrigger,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlGenerationConfig {
    pub generator_name: String,
    pub lock_timeout: String,
    pub statement_timeout: String,
    pub qdrant: QdrantGenerationConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QdrantGenerationConfig {
    pub default_vector_dimension: i64,
    pub default_distance: String,
    pub default_hnsw_m: i64,
    pub default_hnsw_ef_construct: i64,
}

const TENANT_CONTEXT_GUC: &str = "app.current_tenant_id";
const PROJECT_CONTEXT_GUC: &str = "app.current_project_id";
pub const TENANT_COLUMN_CANDIDATES: &[&str] =
    &["tenant_id", "_tenant_id", "org_id", "institution_id"];
pub const PROJECT_COLUMN_CANDIDATES: &[&str] = &["project_id", "_project_id"];

impl Default for QdrantGenerationConfig {
    fn default() -> Self {
        Self {
            default_vector_dimension: 1536,
            default_distance: "Cosine".to_string(),
            default_hnsw_m: 16,
            default_hnsw_ef_construct: 100,
        }
    }
}

impl Default for SqlGenerationConfig {
    fn default() -> Self {
        Self {
            generator_name: "udb".to_string(),
            lock_timeout: "5s".to_string(),
            statement_timeout: "120s".to_string(),
            qdrant: QdrantGenerationConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GeneratedArtifact {
    pub rel_path: String,
    pub kind: String,
    pub schema: String,
    pub table: String,
    pub content: String,
}

pub fn generate_bootstrap_sql(
    schemas: &[ProtoSchema],
    config: &SqlGenerationConfig,
) -> Result<Vec<GeneratedArtifact>, serde_json::Error> {
    let manifest = CatalogManifest::from_schemas(schemas)?;
    let mut out = Vec::new();
    let extension_sql = render_extensions(&manifest);
    if !extension_sql.trim().is_empty() {
        out.push(GeneratedArtifact {
            rel_path: "000_extensions.sql".to_string(),
            kind: "bootstrap".to_string(),
            schema: "public".to_string(),
            table: String::new(),
            content: extension_sql,
        });
    }
    for table in &manifest.tables {
        out.push(GeneratedArtifact {
            rel_path: format!("{}/001_{}.sql", table.schema, table.table),
            kind: "bootstrap".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            content: render_bootstrap_table(table, &manifest.checksum_sha256, config),
        });
        // #120: concurrent (non-unique) indexes cannot live inside the table's
        // transactional artifact. Emit each as its own standalone, single-statement
        // non-transactional artifact (sorted after the `001_` table create via the
        // `950_cidx_` prefix) so `CREATE INDEX CONCURRENTLY` runs in autocommit.
        for index in table.indexes.iter().filter(|i| i.concurrent && !i.unique) {
            out.push(GeneratedArtifact {
                rel_path: format!(
                    "{}/950_cidx_{}.sql",
                    table.schema,
                    derive_index_name(table, index)
                ),
                kind: "bootstrap".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                content: format!(
                    "{}{}\n",
                    render_non_tx_header(
                        &manifest.checksum_sha256,
                        &table.schema,
                        &table.table,
                        "bootstrap_concurrent_index",
                        config,
                    ),
                    render_index_standalone(table, index),
                ),
            });
        }
    }

    // Collect ALL FK constraints (same-schema AND cross-schema) into a single
    // final artifact.  Inlining same-schema FKs inside CREATE TABLE requires the
    // referenced table to already exist — this breaks whenever migration_order
    // numbers don't perfectly encode every dependency.  Deferring ALL FKs to the
    // zzz_ artifact (which runs after every per-table artifact) is safe and
    // eliminates all ordering issues.
    // Using a "zzz_" prefix guarantees it sorts after every per-schema artifact,
    // so all referenced tables are guaranteed to exist by the time these run.
    // Build a map of (schema, table) -> partition_column for all partitioned tables.
    // PostgreSQL only rejects a FK to a partitioned table when the referenced unique
    // constraint does not include ALL partition key columns.  FKs that already carry
    // the partition key in their ref_columns list are valid and must NOT be skipped.
    let partition_columns: std::collections::HashMap<(&str, &str), &str> = manifest
        .tables
        .iter()
        .filter(|t| is_partitioned(t) && !t.partition_column.trim().is_empty())
        .map(|t| {
            (
                (t.schema.as_str(), t.table.as_str()),
                t.partition_column.as_str(),
            )
        })
        .collect();

    let mut fk_lines: Vec<String> = Vec::new();
    for table in &manifest.tables {
        for fk in &table.foreign_keys {
            let ref_key = (fk.ref_schema.as_str(), fk.ref_table.as_str());
            if let Some(&part_col) = partition_columns.get(&ref_key) {
                // Referenced table is partitioned — FK is only valid if it references
                // the partition key column so PostgreSQL can find the matching unique
                // constraint (which UDB auto-extends with the partition key).
                if !fk.ref_columns.iter().any(|c| c.as_str() == part_col) {
                    fk_lines.push(format!(
                        "-- SKIPPED FK {}.{} -> {}.{}: referenced table is partitioned on '{}' but FK ref_columns {:?} do not include the partition key; add a denormalised partition-key column to the child table\n",
                        table.schema, table.table, fk.ref_schema, fk.ref_table, part_col, fk.ref_columns
                    ));
                    continue;
                }
            }
            fk_lines.push(render_add_fk(
                &table.schema,
                &table.table,
                fk,
                is_partitioned(table),
            ));
        }
    }
    if !fk_lines.is_empty() {
        out.push(GeneratedArtifact {
            rel_path: "zzz_foreign_keys.sql".to_string(),
            kind: "bootstrap".to_string(),
            schema: String::new(),
            table: String::new(),
            content: format!(
                "{}{}\n",
                render_foreign_keys_header(&manifest.checksum_sha256, config),
                fk_lines.join("\n")
            ),
        });
    }

    Ok(out)
}

pub fn generate_delta_sql(
    manifest: &CatalogManifest,
    changes: &[ChangeOperation],
    config: &SqlGenerationConfig,
) -> Vec<GeneratedArtifact> {
    let mut grouped: Vec<((&str, &str), Vec<&ChangeOperation>)> = Vec::new();
    // #116/#120: ops PostgreSQL forbids inside a transaction (enum ADD VALUE,
    // concurrent index) are pulled out of the grouped BEGIN/COMMIT artifact and
    // emitted as standalone single-statement non-transactional artifacts.
    let mut standalone: Vec<&ChangeOperation> = Vec::new();
    for change in changes
        .iter()
        .filter(|change| change.safety == ChangeSafety::SafeAuto)
    {
        // HintWarning (UDB-MIG-001) is an informational audit record, never DDL;
        // now that consumed/stale allow_drop hints are SafeAuto they would
        // otherwise create an empty no-op delta artifact.
        if matches!(
            change.kind,
            ChangeKind::AddSchema | ChangeKind::CreateStore | ChangeKind::HintWarning
        ) {
            continue;
        }
        if op_requires_standalone(manifest, change) {
            standalone.push(change);
            continue;
        }
        let key = (change.schema.as_str(), change.table.as_str());
        if let Some((_, ops)) = grouped.iter_mut().find(|(existing, _)| *existing == key) {
            ops.push(change);
        } else {
            grouped.push((key, vec![change]));
        }
    }

    let mut artifacts: Vec<GeneratedArtifact> = grouped
        .into_iter()
        .filter_map(|((schema, table), ops)| {
            let content = render_delta_table(manifest, schema, table, &ops, config);
            if content.trim().is_empty() {
                return None;
            }
            let slug = delta_slug(&ops);
            let file_table = if table.is_empty() { "schema" } else { table };
            Some(GeneratedArtifact {
                rel_path: format!("{schema}/900_auto_{file_table}_{slug}.sql"),
                kind: "proto_delta".to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                content,
            })
        })
        .collect();

    // Standalone non-transactional delta artifacts, sorted after the grouped
    // `900_auto_` artifacts via the `960_nontx_` prefix.
    for (idx, op) in standalone.iter().enumerate() {
        let content = render_standalone_delta(manifest, op, config);
        if content.trim().is_empty() {
            continue;
        }
        let file_table = if op.table.is_empty() {
            "schema"
        } else {
            op.table.as_str()
        };
        let kind_slug = format!("{:?}", op.kind).to_ascii_lowercase();
        artifacts.push(GeneratedArtifact {
            rel_path: format!(
                "{}/960_nontx_{}_{}_{}.sql",
                op.schema, file_table, kind_slug, idx
            ),
            kind: "proto_delta".to_string(),
            schema: op.schema.clone(),
            table: op.table.clone(),
            content,
        });
    }

    artifacts
}

pub fn render_bootstrap_table(
    table: &ManifestTable,
    manifest_checksum: &str,
    config: &SqlGenerationConfig,
) -> String {
    let mut sql = String::new();
    sql.push_str(&render_header(table, manifest_checksum, config));
    sql.push('\n');
    sql.push_str(&format!(
        "CREATE SCHEMA IF NOT EXISTS {};\n\n",
        qi(&table.schema)
    ));
    // GAP 4: ENUM type DDL — must run before CREATE TABLE references the type
    sql.push_str(&render_enum_types(table));
    sql.push_str(&render_sql_artifacts(table, "before_table"));
    let table_kind = if table.unlogged {
        "CREATE UNLOGGED TABLE IF NOT EXISTS"
    } else {
        "CREATE TABLE IF NOT EXISTS"
    };
    sql.push_str(&format!(
        "{} {}.{} (\n",
        table_kind,
        qi(&table.schema),
        qi(&table.table)
    ));

    let mut lines: Vec<String> = table
        .columns
        .iter()
        .map(|column| format!("    {}", render_column(column)))
        .collect();

    if !table.primary_key.is_empty() {
        let pk_columns = partition_aware_unique_columns(table, &table.primary_key);
        lines.push(format!(
            "    CONSTRAINT {} PRIMARY KEY ({})",
            qi(&format!("pk_{}", table.table)),
            quote_list(&pk_columns)
        ));
    }

    for fk in &table.foreign_keys {
        // All FKs (same-schema and cross-schema) are deferred to zzz_foreign_keys.sql
        // to avoid CREATE TABLE ordering dependencies. Nothing inlined here.
        let _ = fk; // suppress unused warning
    }
    for check in &table.checks {
        let name = if check.name.trim().is_empty() {
            format!("chk_{}_{}", table.table, lines.len())
        } else {
            check.name.clone()
        };
        lines.push(format!(
            "    CONSTRAINT {} CHECK ({})",
            qi(&name),
            check.expression
        ));
    }

    sql.push_str(&lines.join(",\n"));
    if is_partitioned(table) {
        sql.push_str(&format!(
            "\n) PARTITION BY {} ({}){};\n\n",
            normalize_partition_strategy(&table.partition_strategy),
            qi(&table.partition_column),
            render_tablespace(table)
        ));
    } else {
        sql.push_str(&format!("\n){};\n\n", render_tablespace(table)));
    }

    // Idempotent column backfill: if the table already existed (CREATE TABLE
    // was a no-op) any columns added to the proto since the last migration are
    // still absent from the DB. Emit ADD COLUMN IF NOT EXISTS for every
    // non-generated, non-identity column so that subsequent index / trigger /
    // partition statements can always reference the declared columns.
    //
    // This is required for partitioned tables too: ALTER TABLE on the parent is
    // supported and propagates to partitions. Skipping it leaves old parents
    // without newly introduced audit/partition columns such as created_at.
    for column in &table.columns {
        // Generated/identity columns cannot have their expression changed via
        // ADD COLUMN IF NOT EXISTS and are always part of the original CREATE
        // TABLE — skip them here.
        if column.generated || column.is_identity {
            continue;
        }
        let col_def = render_column(column);
        sql.push_str(&format!(
            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {};\n",
            qi(&table.schema),
            qi(&table.table),
            col_def
        ));
    }
    sql.push('\n');

    sql.push_str(&render_partition_unique_constraint_repair(table));

    // GAP 1: GIN indexes for JSONB columns (automatic)
    sql.push_str(&render_jsonb_gin_indexes(table));

    // GAP 5: FTS tsvector + pg_trgm indexes
    sql.push_str(&render_tsvector_indexes(table));

    for column in table
        .columns
        .iter()
        .filter(|column| column.unique && !column.is_primary)
    {
        let single_column = vec![column.column_name.clone()];
        if has_explicit_unique_index_for_columns(table, &single_column) {
            continue;
        }
        sql.push_str(&render_partitioned_unique_index_create(
            table,
            &format!(
                "uidx_{}_{}_{}",
                table.schema, table.table, column.column_name
            ),
            std::slice::from_ref(&column.column_name),
            &[qi(&column.column_name)],
            "BTREE",
            "",
            "",
        ));
    }

    for index in &table.indexes {
        // #120: a concurrent (non-unique) index cannot run inside this table's
        // transactional bootstrap artifact — it is emitted separately as a
        // standalone non-transactional artifact by `generate_bootstrap_sql`.
        if index.concurrent && !index.unique {
            continue;
        }
        sql.push_str(&render_index_in_tx(table, index));
    }

    if table.enable_rls {
        sql.push_str(&format!(
            "ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY;\n",
            qi(&table.schema),
            qi(&table.table)
        ));
        if table.force_rls {
            sql.push_str(&format!(
                "ALTER TABLE {}.{} FORCE ROW LEVEL SECURITY;\n",
                qi(&table.schema),
                qi(&table.table)
            ));
        }
        sql.push('\n');
    }

    for policy in &table.rls_policies {
        if policy.name.trim().is_empty() {
            continue;
        }
        sql.push_str(&render_policy(&table.schema, &table.table, policy));
        sql.push_str("\n\n");
    }

    // Item 137 (Stage 2): when a tenant table opts into RLS but declares no
    // explicit policy, synthesize a default tenant-isolation policy from its
    // tenant column so multi-tenant isolation is enforced by default. The
    // predicate reads `app.current_tenant_id`, which the broker (and the
    // native fast path) set via `set_config`/`SET LOCAL` per request.
    if table.enable_rls && table.rls_policies.is_empty() {
        if let Some(policy) = default_tenant_rls_policy(table) {
            sql.push_str(&render_policy(&table.schema, &table.table, &policy));
            sql.push_str("\n\n");
        }
    }

    if !table.comment.trim().is_empty() {
        sql.push_str(&format!(
            "COMMENT ON TABLE {}.{} IS {};\n",
            qi(&table.schema),
            qi(&table.table),
            ql(&table.comment)
        ));
    }
    for column in &table.columns {
        if !column.comment.trim().is_empty() {
            sql.push_str(&format!(
                "COMMENT ON COLUMN {}.{}.{} IS {};\n",
                qi(&table.schema),
                qi(&table.table),
                qi(&column.column_name),
                ql(&column.comment)
            ));
        }
    }

    sql.push_str(&render_sql_artifacts(table, "before_triggers"));
    for view in &table.materialized_views {
        sql.push_str(&render_materialized_view(view));
    }
    for trigger in &table.triggers {
        sql.push_str(&render_trigger(trigger));
    }
    sql.push_str(&render_sql_artifacts(table, "after_triggers"));

    // GAP 3: partition child setup (pg_partman call + DEFAULT partition)
    sql.push_str(&render_partition_setup(table));

    sql
}

/// Item 137 (Stage 2): synthesize a default tenant-isolation RLS policy from a
/// table's tenant column. The proto-derived `is_tenant_column` flag is the
/// source of truth; conventional tenant-discriminator names are only a
/// compatibility fallback. Returns `None` for tables without one (they get no
/// default policy). The predicate compares the tenant column against
/// `current_setting('app.current_tenant_id', true)` so a missing setting yields
/// SQL NULL (no rows) rather than an error, matching the explicit policies the
/// proto entities already declare.
fn default_tenant_rls_policy(table: &ManifestTable) -> Option<ManifestPolicy> {
    let mut predicates = Vec::new();
    if let Some(column_name) = resolve_tenant_column(table) {
        predicates.push(format!(
            "({col}::text = current_setting('{TENANT_CONTEXT_GUC}', true)::text)",
            col = qi(column_name)
        ));
    }
    if let Some(column_name) = resolve_project_column(table) {
        predicates.push(format!(
            "({col}::text = current_setting('{PROJECT_CONTEXT_GUC}', true)::text)",
            col = qi(column_name)
        ));
    }
    if predicates.is_empty() {
        return None;
    }
    let predicate = predicates.join(" AND ");
    Some(ManifestPolicy {
        name: "tenant_isolation".to_string(),
        command: "ALL".to_string(),
        using_expression: predicate.clone(),
        with_check: predicate,
        permissive: true,
    })
}

/// Resolve the tenant-isolation column under the shared policy used by
/// generation, planning, IR compilers, and runtime helpers.
pub fn resolve_tenant_column_ref(table: &ManifestTable) -> Option<&ManifestColumn> {
    declared_security_column_ref(table, &table.table_security.tenant_column)
        .or_else(|| table.columns.iter().find(|column| column.is_tenant_column))
        .or_else(|| find_named_column(table, TENANT_COLUMN_CANDIDATES))
}

pub fn resolve_tenant_column(table: &ManifestTable) -> Option<&str> {
    resolve_tenant_column_ref(table).map(|column| column.column_name.as_str())
}

/// Resolve the project-isolation column. Project columns have their own proto
/// flag and conventional `project_id` / `_project_id` names.
pub fn resolve_project_column_ref(table: &ManifestTable) -> Option<&ManifestColumn> {
    declared_security_column_ref(table, &table.table_security.project_column)
        .or_else(|| table.columns.iter().find(|column| column.is_project_column))
        .or_else(|| find_named_column(table, PROJECT_COLUMN_CANDIDATES))
}

pub fn resolve_project_column(table: &ManifestTable) -> Option<&str> {
    resolve_project_column_ref(table).map(|column| column.column_name.as_str())
}

pub fn table_requires_tenant_column(table: &ManifestTable) -> bool {
    table.enable_rls
        || tenant_isolation_enabled(&table.table_security.tenant_isolation_mode)
        || !table.table_security.tenant_column.trim().is_empty()
}

pub fn tenant_isolation_enabled(mode: &str) -> bool {
    !matches!(
        mode.trim().to_ascii_lowercase().as_str(),
        "" | "none" | "global" | "disabled" | "off"
    )
}

fn declared_security_column_ref<'a>(
    table: &'a ManifestTable,
    name: &str,
) -> Option<&'a ManifestColumn> {
    let name = name.trim();
    if name.is_empty() {
        return None;
    }
    table
        .columns
        .iter()
        .find(|column| column.column_name == name || column.field_name == name)
}

fn find_named_column<'a>(
    table: &'a ManifestTable,
    candidates: &[&str],
) -> Option<&'a ManifestColumn> {
    table.columns.iter().find(|column| {
        candidates.iter().any(|candidate| {
            column.column_name.eq_ignore_ascii_case(candidate)
                || column.field_name.eq_ignore_ascii_case(candidate)
        })
    })
}

// Phase I: sql.rs split into helper modules.
mod render_core;
mod render_ext;
pub(crate) use render_core::*;
pub(crate) use render_ext::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn named_column(field_name: &str, column_name: &str) -> ManifestColumn {
        ManifestColumn {
            field_name: field_name.to_string(),
            column_name: column_name.to_string(),
            sql_type: "TEXT".to_string(),
            ..ManifestColumn::default()
        }
    }

    fn partitioned_table() -> ManifestTable {
        ManifestTable {
            schema: "example_mfs".to_string(),
            table: "mfs_transactions".to_string(),
            primary_key: vec!["transaction_id".to_string()],
            partition_strategy: "PARTITION_STRATEGY_RANGE_MONTH".to_string(),
            partition_column: "created_at".to_string(),
            partition_interval: "MONTHLY".to_string(),
            partition_premake: 3,
            columns: vec![
                ManifestColumn {
                    column_name: "transaction_id".to_string(),
                    sql_type: "UUID".to_string(),
                    is_primary: true,
                    not_null: true,
                    ..ManifestColumn::default()
                },
                ManifestColumn {
                    column_name: "external_transaction_id".to_string(),
                    sql_type: "VARCHAR(100)".to_string(),
                    unique: true,
                    not_null: true,
                    ..ManifestColumn::default()
                },
                ManifestColumn {
                    column_name: "created_at".to_string(),
                    sql_type: "TIMESTAMPTZ".to_string(),
                    not_null: true,
                    ..ManifestColumn::default()
                },
            ],
            indexes: vec![ManifestIndex {
                name: "idx_mfs_transactions_txn_id".to_string(),
                columns: vec!["external_transaction_id".to_string()],
                unique: true,
                method: "BTREE".to_string(),
                ..ManifestIndex::default()
            }],
            ..ManifestTable::default()
        }
    }

    #[test]
    fn shared_tenant_resolver_uses_flag_system_name_and_legacy_names() {
        let flagged = ManifestTable {
            columns: vec![
                named_column("tenant_id", "tenant_id"),
                ManifestColumn {
                    field_name: "account".to_string(),
                    column_name: "account_id".to_string(),
                    is_tenant_column: true,
                    ..named_column("account", "account_id")
                },
            ],
            ..ManifestTable::default()
        };
        assert_eq!(resolve_tenant_column(&flagged), Some("account_id"));

        let system = ManifestTable {
            columns: vec![named_column("_tenant_id", "_tenant_id")],
            ..ManifestTable::default()
        };
        assert_eq!(resolve_tenant_column(&system), Some("_tenant_id"));

        let legacy = ManifestTable {
            columns: vec![named_column("org_id", "organization_id")],
            ..ManifestTable::default()
        };
        assert_eq!(resolve_tenant_column(&legacy), Some("organization_id"));
    }

    #[test]
    fn shared_project_resolver_uses_flag_and_system_name() {
        let flagged = ManifestTable {
            columns: vec![ManifestColumn {
                field_name: "workspace".to_string(),
                column_name: "workspace_id".to_string(),
                is_project_column: true,
                ..named_column("workspace", "workspace_id")
            }],
            ..ManifestTable::default()
        };
        assert_eq!(resolve_project_column(&flagged), Some("workspace_id"));

        let system = ManifestTable {
            columns: vec![named_column("_project_id", "_project_id")],
            ..ManifestTable::default()
        };
        assert_eq!(resolve_project_column(&system), Some("_project_id"));
    }

    #[test]
    fn partitioned_unique_index_appends_live_partition_columns() {
        let table = partitioned_table();
        let sql = render_index(&table, &table.indexes[0]);

        assert!(sql.contains("pg_partitioned_table p"), "{sql}");
        assert!(
            sql.contains("ARRAY['external_transaction_id', 'created_at']::TEXT[]"),
            "{sql}"
        );
        assert!(
            sql.contains("ARRAY['\"external_transaction_id\"', '\"created_at\"']::TEXT[]"),
            "{sql}"
        );
        assert!(
            sql.contains("_column_sql_parts := _column_sql_parts || format('%I', _part_col)"),
            "{sql}"
        );
        assert!(
            sql.contains("CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I.%I USING %s (%s)"),
            "{sql}"
        );
    }

    #[test]
    fn concurrent_index_uses_postgres_keyword_order() {
        let table = ManifestTable {
            schema: "example_examplegent".to_string(),
            table: "agent_knowledge_embeddings".to_string(),
            ..ManifestTable::default()
        };
        let index = ManifestIndex {
            name: "idx_agent_knowledge_embeddings_fts_simple".to_string(),
            columns: vec!["to_tsvector('simple', content)".to_string()],
            method: "GIN".to_string(),
            concurrent: true,
            ..ManifestIndex::default()
        };

        let sql = render_index(&table, &index);

        assert!(
            sql.starts_with(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_agent_knowledge_embeddings_fts_simple\""
            ),
            "{sql}"
        );
        assert!(!sql.contains("CREATE CONCURRENTLY INDEX"), "{sql}");
    }

    #[test]
    fn bootstrap_emits_concurrent_index_as_standalone_non_tx_artifact() {
        // #120: a concurrent (non-unique) index must NOT be downgraded into the
        // table's transactional bootstrap artifact; it is split into its own
        // standalone, single-statement, non-transactional artifact that keeps
        // CONCURRENTLY (legal only in autocommit).
        let table = ManifestTable {
            schema: "example_examplegent".to_string(),
            table: "agent_knowledge_embeddings".to_string(),
            columns: vec![ManifestColumn {
                column_name: "content".to_string(),
                sql_type: "TEXT".to_string(),
                ..ManifestColumn::default()
            }],
            indexes: vec![ManifestIndex {
                name: "idx_agent_knowledge_embeddings_fts_simple".to_string(),
                columns: vec!["to_tsvector('simple', content)".to_string()],
                method: "GIN".to_string(),
                concurrent: true,
                ..ManifestIndex::default()
            }],
            ..ManifestTable::default()
        };

        // The inline table artifact must NOT contain the concurrent index at all.
        let table_sql =
            render_bootstrap_table(&table, "sha256:test", &SqlGenerationConfig::default());
        assert!(
            !table_sql.contains("idx_agent_knowledge_embeddings_fts_simple"),
            "concurrent index must not be inlined into the table artifact: {table_sql}"
        );

        // The standalone artifact body (header + single statement) keeps
        // CONCURRENTLY and carries no BEGIN/COMMIT.
        let cfg = SqlGenerationConfig::default();
        let body = format!(
            "{}{}\n",
            render_non_tx_header(
                "sha256:test",
                &table.schema,
                &table.table,
                "bootstrap_concurrent_index",
                &cfg,
            ),
            render_index_standalone(&table, &table.indexes[0]),
        );
        assert!(body.contains("CONCURRENTLY"), "{body}");
        assert!(body.contains("UDB:no_transaction=true"), "{body}");
        assert!(!body.contains("BEGIN;"), "{body}");
        assert!(!body.contains("COMMIT;"), "{body}");
    }

    #[test]
    fn add_fk_skips_when_live_parent_partition_keys_are_missing() {
        let fk = ManifestForeignKey {
            name: "fk_mfs_webhooks_transaction".to_string(),
            columns: vec![
                "mfs_transaction_id".to_string(),
                "transaction_created_at".to_string(),
            ],
            ref_schema: "example_mfs".to_string(),
            ref_table: "mfs_transactions".to_string(),
            ref_columns: vec!["transaction_id".to_string(), "created_at".to_string()],
            on_delete: "SET NULL".to_string(),
            ..ManifestForeignKey::default()
        };

        let sql = render_add_fk("example_mfs", "mfs_webhooks", &fk, false);

        assert!(sql.contains("pg_partitioned_table p"), "{sql}");
        assert!(
            sql.contains("to_regclass('example_mfs.mfs_transactions')"),
            "{sql}"
        );
        assert!(
            sql.contains("ARRAY['transaction_id', 'created_at']::TEXT[]"),
            "{sql}"
        );
        assert!(
            sql.contains("Skipping FK fk_mfs_webhooks_transaction"),
            "{sql}"
        );
    }

    #[test]
    fn partition_repair_uses_manifest_and_live_partition_columns() {
        let table = partitioned_table();
        let sql = render_partition_unique_constraint_repair(&table);

        assert!(
            sql.contains("_required_partition_cols TEXT[] := ARRAY['created_at']::TEXT[]"),
            "{sql}"
        );
        assert!(sql.contains("pg_partitioned_table p"), "{sql}");
        assert!(
            sql.contains("FROM unnest(_required_partition_cols) AS required(attname)"),
            "{sql}"
        );
        assert!(
            sql.contains("_pk_column_sql_parts := _pk_column_sql_parts || format('%I', _part_col)"),
            "{sql}"
        );
        assert!(sql.contains("ADD CONSTRAINT %I PRIMARY KEY (%s)"), "{sql}");
    }

    #[test]
    fn partman_setup_uses_live_control_column_for_existing_partitioned_parent() {
        let table = partitioned_table();
        let sql = render_partition_setup(&table);

        assert!(sql.contains("_control_col TEXT := 'created_at'"), "{sql}");
        assert!(sql.contains("FROM pg_partitioned_table p"), "{sql}");
        assert!(
            sql.contains("IF to_regclass('partman.part_config') IS NOT NULL THEN"),
            "{sql}"
        );
        assert!(sql.contains("p_control := _control_col"), "{sql}");
    }

    #[test]
    fn partition_setup_creates_current_child_without_partman() {
        let table = partitioned_table();
        let sql = render_partition_setup(&table);

        assert!(
            sql.contains("OR to_regclass('partman.part_config') IS NOT NULL THEN"),
            "{sql}"
        );
        assert!(sql.contains("date_trunc('month', now())"), "{sql}");
        assert!(sql.contains("INTERVAL '1 month'"), "{sql}");
        assert!(sql.contains("to_char(_start_ts, 'YYYYMM')"), "{sql}");
        assert!(
            sql.contains("PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)"),
            "{sql}"
        );
    }

    #[test]
    fn partition_setup_skips_current_child_when_default_partition_is_declared() {
        let mut table = partitioned_table();
        table.partition_default = true;
        let sql = render_partition_setup(&table);

        assert!(sql.contains("PARTITION OF \"example_mfs\".\"mfs_transactions\" DEFAULT"));
        assert!(
            !sql.contains("date_trunc('month', now())"),
            "default partition should remain the fallback for tables that opt in:\n{sql}"
        );
    }

    #[test]
    fn pg_partman_extension_creation_is_skipped_when_unavailable() {
        let sql = render_create_extension(&ManifestExtension {
            name: "pg_partman".to_string(),
            schema: "partman".to_string(),
            version: String::new(),
        });

        assert!(
            sql.contains(
                "IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_partman') THEN"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("CREATE EXTENSION IF NOT EXISTS \"pg_partman\" SCHEMA \"partman\""),
            "{sql}"
        );
        assert!(sql.contains("PERFORM pg_advisory_xact_lock"), "{sql}");
        assert!(!sql.contains("SELECT pg_advisory_xact_lock"), "{sql}");
    }
}
