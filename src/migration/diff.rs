use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::generation::manifest::{
    CatalogManifest, ManifestCheck, ManifestColumn, ManifestExtension, ManifestForeignKey,
    ManifestIndex, ManifestMaterializedView, ManifestPolicy, ManifestReservedRange,
    ManifestSqlArtifact, ManifestTable, ManifestTrigger, check_key, fk_key, index_key,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChangeKind {
    HintWarning,
    AddSchema,
    #[default]
    CreateTable,
    RenameTable,
    DropTable,
    AddColumn,
    RenameColumn,
    ChangeColumnType,
    SetDefault,
    DropDefault,
    SetNotNull,
    DropNotNull,
    SetColumnCollation,
    DropColumn,
    SetTableLogged,
    SetTableUnlogged,
    SetTablespace,
    AddCheck,
    AddUnique,
    DropUnique,
    CreateIndex,
    DropIndex,
    AddForeignKey,
    DropForeignKey,
    EnableRls,
    DisableRls,
    CreatePolicy,
    DropPolicy,
    CreateExtension,
    DropExtension,
    CreateMaterializedView,
    DropMaterializedView,
    CreateTrigger,
    DropTrigger,
    CreateStore,
    UpdateStore,
    DropStore,
    ValidationError,
    CreateEnum,
    AlterEnumAddValue,
    DropEnum,
    /// Partitioning lifecycle marker. Existing table partition conversion is
    /// review-only; bootstrap handles new partitioned-table setup.
    CreatePartition,
    AttachPartition,
    DetachPartition,
    DropPartition,
    // ── U14 — Backend-specific change variants ───────────────────────────
    /// Mongo / Qdrant: a new collection resource.
    CreateCollection,
    /// Mongo / Qdrant: collection options changed.
    UpdateCollection,
    /// Mongo / Qdrant: collection dropped.
    DropCollection,
    /// Mongo: collection-level validator update.
    UpdateValidator,
    /// Neo4j: constraint or index added.
    CreateConstraint,
    /// Neo4j: constraint or index options changed.
    UpdateConstraint,
    /// Neo4j: constraint or index dropped.
    DropConstraint,
    /// ClickHouse: engine / ORDER BY / PARTITION change.
    ChangeTableEngine,
    /// S3 / MinIO: bucket created.
    CreateBucket,
    /// S3 / MinIO: bucket lifecycle / policy / versioning changed.
    UpdateLifecyclePolicy,
    /// S3 / MinIO: bucket dropped.
    DropBucket,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChangeSafety {
    SafeAuto,
    #[default]
    RequiresReview,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChangeOperation {
    pub kind: ChangeKind,
    pub safety: ChangeSafety,
    pub priority: i32,
    pub schema: String,
    pub table: String,
    pub column: String,
    pub object_name: String,
    pub reason: String,
    pub blocked_reason: String,
    pub fingerprint: String,
}

pub fn diff_manifests(
    old: Option<&CatalogManifest>,
    new: &CatalogManifest,
) -> Vec<ChangeOperation> {
    let mut ops = Vec::new();

    for error in &new.validation_errors {
        ops.push(op(
            ChangeKind::ValidationError,
            ChangeSafety::Blocked,
            "",
            "",
            "",
            "manifest",
            error,
            error,
        ));
    }

    let Some(old) = old else {
        let mut schemas = BTreeSet::new();
        for table in &new.tables {
            if schemas.insert(table.schema.clone()) {
                ops.push(op(
                    ChangeKind::AddSchema,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    "",
                    "",
                    &table.schema,
                    "schema is present in desired proto AST",
                    "",
                ));
            }
            ops.push(op(
                ChangeKind::CreateTable,
                ChangeSafety::SafeAuto,
                &table.schema,
                &table.table,
                "",
                &table.table,
                "table is present in desired proto AST",
                "",
            ));
        }
        for store in &new.stores {
            ops.push(op(
                ChangeKind::CreateStore,
                ChangeSafety::SafeAuto,
                &store.owner_schema,
                &store.owner_table,
                "",
                &store.resource_name,
                "external data resource is present in desired proto AST",
                "",
            ));
        }
        finalize_ops(&mut ops);
        return ops;
    };

    if old.checksum_sha256 == new.checksum_sha256 {
        return ops;
    }

    let old_tables = table_map(&old.tables);
    let new_tables = table_map(&new.tables);
    let rename_sources = rename_sources(&new.tables);
    lint_hints(old, new, &old_tables, &mut ops);

    for table in &new.tables {
        if let Some(old_table) = old_tables.get(&table_key(&table.schema, &table.table)) {
            if old_table.checksum_sha256 != table.checksum_sha256 {
                diff_table(old_table, table, &mut ops);
            }
            continue;
        }

        if !table.previous_table_name.trim().is_empty() {
            let old_key = table_key(&table.schema, &table.previous_table_name);
            if let Some(old_table) = old_tables.get(&old_key) {
                ops.push(op(
                    ChangeKind::RenameTable,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    "",
                    &table.previous_table_name,
                    "desired table declares previous_table_name migration hint",
                    "",
                ));
                diff_table(old_table, table, &mut ops);
            } else {
                ops.push(op(
                    ChangeKind::CreateTable,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    "",
                    &table.table,
                    "previous_table_name did not match an old table; creating table",
                    "",
                ));
            }
        } else {
            ops.push(op(
                ChangeKind::CreateTable,
                ChangeSafety::SafeAuto,
                &table.schema,
                &table.table,
                "",
                &table.table,
                "table was added to desired proto AST",
                "",
            ));
        }
    }

    for old_table in &old.tables {
        let key = table_key(&old_table.schema, &old_table.table);
        if new_tables.contains_key(&key) || rename_sources.contains(&key) {
            continue;
        }
        let safety = if old_table.allow_drop {
            ChangeSafety::SafeAuto
        } else {
            ChangeSafety::Blocked
        };
        let blocked = if safety == ChangeSafety::Blocked {
            "drop_table is destructive; add allow_drop only after explicit review"
        } else {
            ""
        };
        ops.push(op(
            ChangeKind::DropTable,
            safety,
            &old_table.schema,
            &old_table.table,
            "",
            &old_table.table,
            "table no longer exists in desired proto AST",
            blocked,
        ));
    }

    diff_stores(old, new, &mut ops);
    diff_extensions(old, new, &mut ops);
    finalize_ops(&mut ops);
    ops
}

fn lint_hints(
    old: &CatalogManifest,
    new: &CatalogManifest,
    old_tables: &BTreeMap<String, &ManifestTable>,
    ops: &mut Vec<ChangeOperation>,
) {
    for table in &new.tables {
        if !table.previous_table_name.trim().is_empty() {
            let key = table_key(&table.schema, &table.previous_table_name);
            if !old_tables.contains_key(&key) {
                ops.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::RequiresReview,
                    &table.schema,
                    &table.table,
                    "",
                    &table.previous_table_name,
                    "previous_table_name does not match the prior manifest",
                    "stale or mistyped rename hints require review",
                ));
            }
        }
        if table.allow_drop && old.table(&table.schema, &table.table).is_some() {
            ops.push(op(
                ChangeKind::HintWarning,
                ChangeSafety::RequiresReview,
                &table.schema,
                &table.table,
                "",
                "allow_drop",
                "allow_drop is still set on a table that exists in desired proto",
                "remove stale allow_drop after the destructive migration has been applied",
            ));
        }

        let prior_table = old.table(&table.schema, &table.table);
        for column in &table.columns {
            if !column.previous_column_name.trim().is_empty() {
                let matched = prior_table
                    .map(|prior| {
                        prior
                            .columns
                            .iter()
                            .any(|old_col| old_col.column_name == column.previous_column_name)
                    })
                    .unwrap_or(false);
                if !matched {
                    ops.push(op(
                        ChangeKind::HintWarning,
                        ChangeSafety::RequiresReview,
                        &table.schema,
                        &table.table,
                        &column.column_name,
                        &column.previous_column_name,
                        "previous_column_name does not match the prior manifest",
                        "stale or mistyped rename hints require review",
                    ));
                }
            }
            if column.allow_drop
                && prior_table
                    .map(|prior| {
                        prior
                            .columns
                            .iter()
                            .any(|old_col| old_col.column_name == column.column_name)
                    })
                    .unwrap_or(false)
            {
                ops.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::RequiresReview,
                    &table.schema,
                    &table.table,
                    &column.column_name,
                    "allow_drop",
                    "allow_drop is still set on a column that exists in desired proto",
                    "remove stale allow_drop after the destructive migration has been applied",
                ));
            }
        }
    }
}

fn diff_table(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    diff_table_properties(old, new, ops);
    diff_columns(old, new, ops);
    diff_checks(old, new, ops);
    diff_foreign_keys(old, new, ops);
    diff_indexes(old, new, ops);
    diff_policies(old, new, ops);
    diff_materialized_views(old, new, ops);
    diff_triggers(old, new, ops);
    diff_sql_artifacts(old, new, ops);

    if !old.enable_rls && new.enable_rls {
        ops.push(op(
            ChangeKind::EnableRls,
            ChangeSafety::SafeAuto,
            &new.schema,
            &new.table,
            "",
            &new.table,
            "desired proto AST enables row-level security",
            "",
        ));
    } else if old.enable_rls && !new.enable_rls {
        ops.push(op(
            ChangeKind::DisableRls,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            "",
            &new.table,
            "desired proto AST disables row-level security",
            "disable_rls requires explicit migration approval",
        ));
    }
}

fn diff_table_properties(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    diff_partitioning(old, new, ops);

    if old.unlogged != new.unlogged {
        let (kind, reason) = if new.unlogged {
            (
                ChangeKind::SetTableUnlogged,
                "desired proto AST marks table as UNLOGGED",
            )
        } else {
            (
                ChangeKind::SetTableLogged,
                "desired proto AST marks table as LOGGED",
            )
        };
        ops.push(op(
            kind,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            "",
            &new.table,
            reason,
            "table persistence changes require explicit review",
        ));
    }

    if old.tablespace != new.tablespace {
        ops.push(op(
            ChangeKind::SetTablespace,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            "",
            &new.tablespace,
            "desired proto AST changes table tablespace",
            "tablespace moves require explicit review",
        ));
    }
}

fn diff_partitioning(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_partitioned = is_manifest_partitioned(old);
    let new_partitioned = is_manifest_partitioned(new);
    match (old_partitioned, new_partitioned) {
        (false, true) => ops.push(op(
            ChangeKind::CreatePartition,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            &new.partition_column,
            "partitioning",
            "desired proto AST adds partitioning to an existing table",
            "converting an existing table to a partitioned table requires a reviewed migration",
        )),
        (true, false) => ops.push(op(
            ChangeKind::DropPartition,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            &old.partition_column,
            "partitioning",
            "desired proto AST removes partitioning from an existing table",
            "removing partitioning requires a reviewed migration",
        )),
        (true, true)
            if old.partition_strategy != new.partition_strategy
                || old.partition_column != new.partition_column
                || old.partition_interval != new.partition_interval
                || old.partition_premake != new.partition_premake
                || old.partition_default != new.partition_default =>
        {
            ops.push(op(
                ChangeKind::AttachPartition,
                ChangeSafety::RequiresReview,
                &new.schema,
                &new.table,
                &new.partition_column,
                "partitioning",
                "desired proto AST changes partitioning options",
                "partitioning option changes require a reviewed migration",
            ));
        }
        _ => {}
    }
}

fn diff_policies(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_policies = old
        .rls_policies
        .iter()
        .map(policy_key)
        .collect::<BTreeSet<_>>();
    let new_policies = new
        .rls_policies
        .iter()
        .map(policy_key)
        .collect::<BTreeSet<_>>();
    for policy in &new.rls_policies {
        if !old_policies.contains(&policy_key(policy)) {
            ops.push(op(
                ChangeKind::CreatePolicy,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &policy.name,
                "RLS policy was added to desired proto AST",
                "",
            ));
        }
    }
    for policy in &old.rls_policies {
        if !new_policies.contains(&policy_key(policy)) {
            ops.push(op(
                ChangeKind::DropPolicy,
                ChangeSafety::RequiresReview,
                &old.schema,
                &old.table,
                "",
                &policy.name,
                "RLS policy no longer exists in desired proto AST",
                "drop_policy requires explicit review",
            ));
        }
    }
}

fn diff_columns(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_columns = column_map(&old.columns);
    let new_columns = column_map(&new.columns);
    let renamed_sources = new
        .columns
        .iter()
        .filter(|col| !col.previous_column_name.is_empty())
        .map(|col| col.previous_column_name.clone())
        .collect::<BTreeSet<_>>();

    detect_field_number_reuse(old, new, &old_columns, ops);

    for new_col in &new.columns {
        if let Some(old_col) = old_columns.get(&new_col.column_name) {
            diff_column(new, old_col, new_col, ops);
            continue;
        }
        if !new_col.enum_values.is_empty() {
            ops.push(op(
                ChangeKind::CreateEnum,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                &new_col.column_name,
                &enum_type_name(new, &new_col.column_name),
                "enum column was added to desired proto AST",
                "",
            ));
        }
        if !new_col.previous_column_name.trim().is_empty()
            && old_columns.contains_key(&new_col.previous_column_name)
        {
            ops.push(op(
                ChangeKind::RenameColumn,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                &new_col.column_name,
                &new_col.previous_column_name,
                "desired column declares previous_column_name migration hint",
                "",
            ));
        } else {
            let (safety, blocked) = add_column_safety(new, new_col);
            ops.push(op(
                ChangeKind::AddColumn,
                safety,
                &new.schema,
                &new.table,
                &new_col.column_name,
                &new_col.column_name,
                "column was added to desired proto AST",
                blocked,
            ));
        }
    }

    for old_col in &old.columns {
        if new_columns.contains_key(&old_col.column_name)
            || renamed_sources.contains(&old_col.column_name)
        {
            continue;
        }
        let safety = if old_col.allow_drop || new.allow_drop {
            ChangeSafety::SafeAuto
        } else {
            ChangeSafety::Blocked
        };
        let blocked = if safety == ChangeSafety::Blocked {
            "drop_column is destructive; add allow_drop only after explicit review"
        } else {
            ""
        };
        ops.push(op(
            ChangeKind::DropColumn,
            safety,
            &old.schema,
            &old.table,
            &old_col.column_name,
            &old_col.column_name,
            "column no longer exists in desired proto AST",
            blocked,
        ));
    }
}

fn diff_column(
    table: &ManifestTable,
    old: &ManifestColumn,
    new: &ManifestColumn,
    ops: &mut Vec<ChangeOperation>,
) {
    if old.sql_type != new.sql_type {
        let (safety, blocked) = type_change_safety(old, new);
        ops.push(op(
            ChangeKind::ChangeColumnType,
            safety,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.sql_type,
            "column SQL type changed",
            blocked,
        ));
    }
    if old.oneof_group != new.oneof_group {
        ops.push(op(
            ChangeKind::ValidationError,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.oneof_group,
            "column oneof membership changed",
            "oneof evolution changes protobuf presence semantics and requires review",
        ));
    }
    diff_enum_values(table, old, new, ops);
    if old.collation != new.collation {
        ops.push(op(
            ChangeKind::SetColumnCollation,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.collation,
            "column collation changed",
            "collation changes rewrite comparison semantics and require explicit review",
        ));
    }
    if old.default_value != new.default_value {
        let kind = if new.default_value.trim().is_empty() {
            ChangeKind::DropDefault
        } else {
            ChangeKind::SetDefault
        };
        ops.push(op(
            kind,
            ChangeSafety::SafeAuto,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.default_value,
            "column default changed",
            "",
        ));
    }
    if !old.not_null && new.not_null {
        let safety = if new.default_value.trim().is_empty() && new.backfill_sql.trim().is_empty() {
            ChangeSafety::RequiresReview
        } else {
            ChangeSafety::SafeAuto
        };
        let blocked = if safety == ChangeSafety::RequiresReview {
            "set_not_null requires default_value or backfill_sql"
        } else {
            ""
        };
        ops.push(op(
            ChangeKind::SetNotNull,
            safety,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.column_name,
            "column became NOT NULL",
            blocked,
        ));
    } else if old.not_null && !new.not_null {
        ops.push(op(
            ChangeKind::DropNotNull,
            ChangeSafety::SafeAuto,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.column_name,
            "column became nullable",
            "",
        ));
    }
    if !old.unique && new.unique {
        ops.push(op(
            ChangeKind::AddUnique,
            ChangeSafety::SafeAuto,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.column_name,
            "column became unique",
            "",
        ));
    } else if old.unique && !new.unique {
        ops.push(op(
            ChangeKind::DropUnique,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.column_name,
            "column is no longer unique",
            "drop_unique requires explicit review",
        ));
    }
}

fn detect_field_number_reuse(
    old: &ManifestTable,
    new: &ManifestTable,
    old_columns: &BTreeMap<String, &ManifestColumn>,
    ops: &mut Vec<ChangeOperation>,
) {
    let mut old_by_number = BTreeMap::<i32, &ManifestColumn>::new();
    for column in &old.columns {
        if column.field_number > 0 {
            old_by_number.insert(column.field_number, column);
        }
    }
    for column in &new.columns {
        if column.field_number <= 0 {
            continue;
        }
        let Some(old_col) = old_by_number.get(&column.field_number) else {
            continue;
        };
        if old_col.field_name == column.field_name && old_col.column_name == column.column_name {
            continue;
        }
        if column.previous_column_name == old_col.column_name
            && old_columns.contains_key(&column.previous_column_name)
        {
            continue;
        }
        ops.push(op(
            ChangeKind::ValidationError,
            ChangeSafety::Blocked,
            &new.schema,
            &new.table,
            &column.column_name,
            &column.field_number.to_string(),
            "proto field number is reused by a different field",
            "field number reuse breaks protobuf compatibility; reserve the old number or declare previous_column_name for an intentional rename",
        ));
    }
}

fn type_change_safety<'a>(old: &ManifestColumn, new: &ManifestColumn) -> (ChangeSafety, &'a str) {
    if !new.using_expression.trim().is_empty() {
        return (ChangeSafety::SafeAuto, "");
    }
    if is_widening_sql_type(&old.sql_type, &new.sql_type) {
        return (ChangeSafety::SafeAuto, "");
    }
    if is_narrowing_sql_type(&old.sql_type, &new.sql_type) {
        return (
            ChangeSafety::Blocked,
            "type narrowing requires using_expression annotation and explicit review",
        );
    }
    (
        ChangeSafety::RequiresReview,
        "change_type requires using_expression annotation",
    )
}

fn is_widening_sql_type(old: &str, new: &str) -> bool {
    matches!(
        (
            normalize_type_token(old).as_str(),
            normalize_type_token(new).as_str()
        ),
        ("INTEGER", "BIGINT")
            | ("INTEGER", "NUMERIC")
            | ("BIGINT", "NUMERIC")
            | ("REAL", "DOUBLE PRECISION")
            | ("REAL", "NUMERIC")
            | ("DOUBLE PRECISION", "NUMERIC")
            | ("VARCHAR", "TEXT")
            | ("CHAR", "TEXT")
            | ("JSON", "JSONB")
    )
}

fn is_narrowing_sql_type(old: &str, new: &str) -> bool {
    matches!(
        (
            normalize_type_token(old).as_str(),
            normalize_type_token(new).as_str()
        ),
        ("BIGINT", "INTEGER")
            | ("NUMERIC", "BIGINT")
            | ("NUMERIC", "INTEGER")
            | ("DOUBLE PRECISION", "REAL")
            | ("TEXT", "VARCHAR")
            | ("TEXT", "CHAR")
            | ("JSONB", "JSON")
    )
}

fn normalize_type_token(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn diff_enum_values(
    table: &ManifestTable,
    old: &ManifestColumn,
    new: &ManifestColumn,
    ops: &mut Vec<ChangeOperation>,
) {
    let old_values = old.enum_values.iter().cloned().collect::<BTreeSet<_>>();
    let new_values = new.enum_values.iter().cloned().collect::<BTreeSet<_>>();
    let enum_name = enum_type_name(table, &new.column_name);

    if old_values.is_empty() && !new_values.is_empty() {
        ops.push(op(
            ChangeKind::CreateEnum,
            ChangeSafety::SafeAuto,
            &table.schema,
            &table.table,
            &new.column_name,
            &enum_name,
            "column gained enum values",
            "",
        ));
        return;
    }

    for value in new_values.difference(&old_values) {
        ops.push(op(
            ChangeKind::AlterEnumAddValue,
            ChangeSafety::SafeAuto,
            &table.schema,
            &table.table,
            value,
            &enum_name,
            "enum value was added to desired proto AST",
            "",
        ));
    }

    if old_values.difference(&new_values).next().is_some() {
        ops.push(op(
            ChangeKind::DropEnum,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            &new.column_name,
            &enum_name,
            "enum values were removed from desired proto AST",
            "PostgreSQL cannot drop enum values in place; a reviewed replacement migration is required",
        ));
    }
}

fn diff_checks(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_checks = old.checks.iter().map(check_key).collect::<BTreeSet<_>>();
    for check in &new.checks {
        if !old_checks.contains(&check_key(check)) {
            ops.push(op(
                ChangeKind::AddCheck,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &check_name(check),
                "check constraint was added to desired proto AST",
                "",
            ));
        }
    }
}

fn diff_indexes(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_indexes = old.indexes.iter().map(index_key).collect::<BTreeSet<_>>();
    let new_indexes = new.indexes.iter().map(index_key).collect::<BTreeSet<_>>();
    for index in &new.indexes {
        if !old_indexes.contains(&index_key(index)) {
            ops.push(op(
                ChangeKind::CreateIndex,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &index_name(new, index),
                "index was added to desired proto AST",
                "",
            ));
        }
    }
    for index in &old.indexes {
        if !new_indexes.contains(&index_key(index)) {
            let safety = if new.allow_drop || column_allows_drop(new, &index.columns) {
                ChangeSafety::SafeAuto
            } else {
                ChangeSafety::RequiresReview
            };
            let blocked = if safety == ChangeSafety::RequiresReview {
                "drop_index requires allow_drop on table or one indexed column"
            } else {
                ""
            };
            ops.push(op(
                ChangeKind::DropIndex,
                safety,
                &old.schema,
                &old.table,
                "",
                &index_name(old, index),
                "index no longer exists in desired proto AST",
                blocked,
            ));
        }
    }
}

fn diff_foreign_keys(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_fks = old.foreign_keys.iter().map(fk_key).collect::<BTreeSet<_>>();
    let new_fks = new.foreign_keys.iter().map(fk_key).collect::<BTreeSet<_>>();
    for fk in &new.foreign_keys {
        if !old_fks.contains(&fk_key(fk)) {
            ops.push(op(
                ChangeKind::AddForeignKey,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &fk_name(new, fk),
                "foreign key was added to desired proto AST",
                "",
            ));
        }
    }
    for fk in &old.foreign_keys {
        if !new_fks.contains(&fk_key(fk)) {
            let safety = if new.allow_drop || column_allows_drop(new, &fk.columns) {
                ChangeSafety::SafeAuto
            } else {
                ChangeSafety::RequiresReview
            };
            let blocked = if safety == ChangeSafety::RequiresReview {
                "drop_foreign_key requires allow_drop on table or one FK column"
            } else {
                ""
            };
            ops.push(op(
                ChangeKind::DropForeignKey,
                safety,
                &old.schema,
                &old.table,
                "",
                &fk_name(old, fk),
                "foreign key no longer exists in desired proto AST",
                blocked,
            ));
        }
    }
}

fn diff_stores(old: &CatalogManifest, new: &CatalogManifest, ops: &mut Vec<ChangeOperation>) {
    let old_keys = old
        .stores
        .iter()
        .map(store_identity_key)
        .collect::<BTreeSet<_>>();
    let new_keys = new
        .stores
        .iter()
        .map(store_identity_key)
        .collect::<BTreeSet<_>>();
    let old_store_map = old
        .stores
        .iter()
        .map(|store| (store_identity_key(store), store))
        .collect::<BTreeMap<_, _>>();

    for store in &new.stores {
        let key = store_identity_key(store);
        if !old_keys.contains(&key) {
            ops.push(op(
                ChangeKind::CreateStore,
                ChangeSafety::SafeAuto,
                &store.owner_schema,
                &store.owner_table,
                "",
                &store.resource_name,
                "external data resource was added to desired proto AST",
                "",
            ));
        } else if let Some(old_store) = old_store_map.get(&key)
            && store_signature(old_store) != store_signature(store)
        {
            let safety = store_update_safety(store);
            let blocked_reason = if safety == ChangeSafety::SafeAuto {
                ""
            } else {
                "external store mutation requires backend-aware review"
            };
            ops.push(op(
                ChangeKind::UpdateStore,
                safety,
                &store.owner_schema,
                &store.owner_table,
                "",
                &store.resource_name,
                "external data resource options changed in desired proto AST",
                blocked_reason,
            ));
        }
    }
    for store in &old.stores {
        if !new_keys.contains(&store_identity_key(store)) {
            ops.push(op(
                ChangeKind::DropStore,
                ChangeSafety::RequiresReview,
                &store.owner_schema,
                &store.owner_table,
                "",
                &store.resource_name,
                "external data resource no longer exists in desired proto AST",
                "drop_store requires explicit review; backend data may be lost",
            ));
        }
    }
}

fn diff_extensions(old: &CatalogManifest, new: &CatalogManifest, ops: &mut Vec<ChangeOperation>) {
    let old_extensions = extension_map(old);
    let new_extensions = extension_map(new);
    for (key, extension) in &new_extensions {
        if !old_extensions.contains_key(key) {
            ops.push(op(
                ChangeKind::CreateExtension,
                ChangeSafety::SafeAuto,
                &extension.schema,
                "",
                "",
                &extension.name,
                "extension is present in desired proto AST",
                "",
            ));
        }
    }
    for (key, extension) in &old_extensions {
        if !new_extensions.contains_key(key) {
            ops.push(op(
                ChangeKind::DropExtension,
                ChangeSafety::RequiresReview,
                &extension.schema,
                "",
                "",
                &extension.name,
                "extension no longer exists in desired proto AST",
                "drop_extension requires explicit review; database objects may depend on it",
            ));
        }
    }
}

fn diff_materialized_views(
    old: &ManifestTable,
    new: &ManifestTable,
    ops: &mut Vec<ChangeOperation>,
) {
    let old_views = old
        .materialized_views
        .iter()
        .map(|view| (materialized_view_key(view), view))
        .collect::<BTreeMap<_, _>>();
    let new_views = new
        .materialized_views
        .iter()
        .map(|view| (materialized_view_key(view), view))
        .collect::<BTreeMap<_, _>>();
    for (key, view) in &new_views {
        if !old_views.contains_key(key) {
            ops.push(op(
                ChangeKind::CreateMaterializedView,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &format!("{}.{}", view.schema, view.name),
                "materialized view is present in desired proto AST",
                "",
            ));
        }
    }
    for (key, view) in &old_views {
        if !new_views.contains_key(key) {
            ops.push(op(
                ChangeKind::DropMaterializedView,
                ChangeSafety::RequiresReview,
                &old.schema,
                &old.table,
                "",
                &format!("{}.{}", view.schema, view.name),
                "materialized view no longer exists in desired proto AST",
                "drop_materialized_view requires explicit review",
            ));
        }
    }
}

fn diff_triggers(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_triggers = old
        .triggers
        .iter()
        .map(|trigger| (trigger_key(trigger), trigger))
        .collect::<BTreeMap<_, _>>();
    let new_triggers = new
        .triggers
        .iter()
        .map(|trigger| (trigger_key(trigger), trigger))
        .collect::<BTreeMap<_, _>>();
    for (key, trigger) in &new_triggers {
        if !old_triggers.contains_key(key) {
            ops.push(op(
                ChangeKind::CreateTrigger,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &trigger.name,
                "trigger is present in desired proto AST",
                "",
            ));
        }
    }
    for (key, trigger) in &old_triggers {
        if !new_triggers.contains_key(key) {
            ops.push(op(
                ChangeKind::DropTrigger,
                ChangeSafety::RequiresReview,
                &old.schema,
                &old.table,
                "",
                &trigger.name,
                "trigger no longer exists in desired proto AST",
                "drop_trigger requires explicit review",
            ));
        }
    }
}

fn add_column_safety<'a>(
    table: &ManifestTable,
    column: &ManifestColumn,
) -> (ChangeSafety, &'a str) {
    // NW-universal: the proto3 `reserved` block exists precisely to
    // prevent accidental reuse of a previously-deleted field number
    // or name. The manifest carries `reserved_numbers` /
    // `reserved_names` for every table; if the proposed new column
    // collides with either, refuse the migration up front rather
    // than apply it and silently mis-route reads against any wire
    // payload still referencing the old slot.
    if column.field_number > 0
        && table
            .reserved_numbers
            .iter()
            .any(|r| r.contains(column.field_number))
    {
        return (
            ChangeSafety::Blocked,
            "add_column reuses a field number declared `reserved` in the proto; \
             either pick a fresh number or remove the `reserved` entry",
        );
    }
    if !column.column_name.is_empty()
        && table
            .reserved_names
            .iter()
            .any(|n| n == &column.column_name)
    {
        return (
            ChangeSafety::Blocked,
            "add_column reuses a field name declared `reserved` in the proto; \
             either pick a fresh name or remove the `reserved` entry",
        );
    }
    if column.not_null && column.default_value.is_empty() && column.backfill_sql.is_empty() {
        (
            ChangeSafety::RequiresReview,
            "add_column NOT NULL requires default_value or backfill_sql",
        )
    } else if table
        .columns
        .iter()
        .filter(|existing| existing.column_name == column.column_name)
        .count()
        > 1
    {
        (ChangeSafety::Blocked, "duplicate desired column name")
    } else {
        (ChangeSafety::SafeAuto, "")
    }
}

fn column_allows_drop(table: &ManifestTable, columns: &[String]) -> bool {
    table
        .columns
        .iter()
        .any(|col| col.allow_drop && columns.contains(&col.column_name))
}

fn table_map(tables: &[ManifestTable]) -> BTreeMap<String, &ManifestTable> {
    tables
        .iter()
        .map(|table| (table_key(&table.schema, &table.table), table))
        .collect()
}

fn column_map(columns: &[ManifestColumn]) -> BTreeMap<String, &ManifestColumn> {
    columns
        .iter()
        .map(|column| (column.column_name.clone(), column))
        .collect()
}

fn is_manifest_partitioned(table: &ManifestTable) -> bool {
    let strategy = table.partition_strategy.trim();
    !strategy.is_empty()
        && !table.partition_column.trim().is_empty()
        && !strategy.ends_with("NONE")
        && !strategy.ends_with("UNSPECIFIED")
}

fn enum_type_name(table: &ManifestTable, column_name: &str) -> String {
    format!("{}_{}_enum", table.table, column_name)
}

fn rename_sources(tables: &[ManifestTable]) -> BTreeSet<String> {
    tables
        .iter()
        .filter(|table| !table.previous_table_name.is_empty())
        .map(|table| table_key(&table.schema, &table.previous_table_name))
        .collect()
}

fn finalize_ops(ops: &mut [ChangeOperation]) {
    for op in ops.iter_mut() {
        op.priority = priority(&op.kind);
        op.fingerprint = operation_fingerprint(op);
    }
    ops.sort_by(|a, b| {
        (
            a.schema.as_str(),
            a.priority,
            a.table.as_str(),
            a.column.as_str(),
            a.object_name.as_str(),
        )
            .cmp(&(
                b.schema.as_str(),
                b.priority,
                b.table.as_str(),
                b.column.as_str(),
                b.object_name.as_str(),
            ))
    });
}

fn operation_fingerprint(op: &ChangeOperation) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!(
        "{:?}\0{}\0{}\0{}\0{}\0{:?}",
        op.kind, op.schema, op.table, op.column, op.object_name, op.safety
    ));
    format!("sha256:{:x}", hasher.finalize())
}

fn priority(kind: &ChangeKind) -> i32 {
    match kind {
        ChangeKind::ValidationError => 0,
        ChangeKind::HintWarning => 1,
        ChangeKind::CreateExtension | ChangeKind::DropExtension => 5,
        ChangeKind::AddSchema => 10,
        ChangeKind::CreateEnum => 15,
        ChangeKind::AlterEnumAddValue => 16,
        ChangeKind::CreateTable => 20,
        ChangeKind::CreatePartition => 22,
        ChangeKind::AttachPartition => 23,
        ChangeKind::RenameTable => 25,
        ChangeKind::SetTableLogged | ChangeKind::SetTableUnlogged | ChangeKind::SetTablespace => 28,
        ChangeKind::AddColumn => 30,
        ChangeKind::RenameColumn
        | ChangeKind::ChangeColumnType
        | ChangeKind::SetColumnCollation => 35,
        ChangeKind::SetDefault | ChangeKind::DropDefault => 40,
        ChangeKind::DropNotNull => 45,
        ChangeKind::SetNotNull => 50,
        ChangeKind::AddCheck => 55,
        ChangeKind::AddUnique | ChangeKind::DropUnique => 60,
        ChangeKind::DropForeignKey => 63,
        ChangeKind::AddForeignKey => 65,
        ChangeKind::CreateIndex | ChangeKind::DropIndex => 70,
        ChangeKind::EnableRls | ChangeKind::DisableRls => 80,
        ChangeKind::CreatePolicy | ChangeKind::DropPolicy => 85,
        ChangeKind::CreateMaterializedView | ChangeKind::DropMaterializedView => 88,
        ChangeKind::CreateTrigger | ChangeKind::DropTrigger => 89,
        ChangeKind::CreateCollection | ChangeKind::CreateBucket => 90,
        ChangeKind::CreateStore => 90,
        ChangeKind::UpdateCollection
        | ChangeKind::UpdateValidator
        | ChangeKind::UpdateConstraint
        | ChangeKind::ChangeTableEngine
        | ChangeKind::UpdateLifecyclePolicy => 94,
        ChangeKind::UpdateStore => 95,
        ChangeKind::CreateConstraint => 96,
        ChangeKind::DropConstraint => 178,
        ChangeKind::DetachPartition => 180,
        ChangeKind::DropPartition => 185,
        ChangeKind::DropCollection | ChangeKind::DropBucket => 188,
        ChangeKind::DropStore => 190,
        ChangeKind::DropEnum => 195,
        ChangeKind::DropColumn => 200,
        ChangeKind::DropTable => 210,
    }
}

#[allow(clippy::too_many_arguments)]
fn op(
    kind: ChangeKind,
    safety: ChangeSafety,
    schema: &str,
    table: &str,
    column: &str,
    object_name: &str,
    reason: &str,
    blocked_reason: &str,
) -> ChangeOperation {
    ChangeOperation {
        kind,
        safety,
        schema: schema.to_string(),
        table: table.to_string(),
        column: column.to_string(),
        object_name: object_name.to_string(),
        reason: reason.to_string(),
        blocked_reason: blocked_reason.to_string(),
        ..ChangeOperation::default()
    }
}

fn table_key(schema: &str, table: &str) -> String {
    format!("{schema}.{table}")
}

fn store_identity_key(store: &crate::generation::manifest::ManifestStore) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        store.store_kind, store.backend, store.owner_schema, store.owner_table, store.resource_name
    )
}

fn store_signature(store: &crate::generation::manifest::ManifestStore) -> String {
    let mut options = store
        .options
        .iter()
        .map(|option| format!("{}={}", option.key, option.value))
        .collect::<Vec<_>>();
    options.sort();
    format!(
        "db={}|ns={}|resource={}|dsn_env={}|dsn={}|payload={}|options={}",
        store.database_name,
        store.namespace,
        store.resource_name,
        store.dsn_env_key,
        store.dsn,
        normalize_policy_expr(&store.payload_schema_json),
        options.join(";")
    )
}

fn store_update_safety(store: &crate::generation::manifest::ManifestStore) -> ChangeSafety {
    match store.store_kind.as_str() {
        "cache" | "kv" | "key_value" | "key-value" => ChangeSafety::SafeAuto,
        "object" | "blob" | "storage" => ChangeSafety::RequiresReview,
        "vector" | "graph" | "nosql" | "document" | "timeseries" | "column" | "model_registry" => {
            ChangeSafety::RequiresReview
        }
        _ => ChangeSafety::RequiresReview,
    }
}

fn extension_map(manifest: &CatalogManifest) -> BTreeMap<String, ManifestExtension> {
    let mut out = BTreeMap::new();
    for table in &manifest.tables {
        for extension in &table.extensions {
            if !extension.name.trim().is_empty() {
                out.insert(extension_key(extension), extension.clone());
            }
        }
    }
    out
}

fn extension_key(extension: &ManifestExtension) -> String {
    format!(
        "{}.{}@{}",
        extension.schema.to_ascii_lowercase(),
        extension.name.to_ascii_lowercase(),
        extension.version
    )
}

fn index_name(table: &ManifestTable, index: &ManifestIndex) -> String {
    if index.name.is_empty() {
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

fn fk_name(table: &ManifestTable, fk: &ManifestForeignKey) -> String {
    if fk.name.is_empty() {
        format!("fk_{}_{}", table.table, fk.columns.join("_"))
    } else {
        fk.name.clone()
    }
}

fn policy_key(policy: &ManifestPolicy) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        policy.name.to_ascii_lowercase(),
        policy.command.to_ascii_uppercase(),
        normalize_policy_expr(&policy.using_expression),
        normalize_policy_expr(&policy.with_check),
        policy.permissive
    )
}

fn normalize_policy_expr(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('"', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
}

fn materialized_view_key(view: &ManifestMaterializedView) -> String {
    format!(
        "{}.{}|{}|{}",
        view.schema.to_ascii_lowercase(),
        view.name.to_ascii_lowercase(),
        normalize_policy_expr(&view.query),
        view.with_data
    )
}

fn trigger_key(trigger: &ManifestTrigger) -> String {
    format!(
        "{}.{}.{}|{}|{}|{}|{}|{}",
        trigger.schema.to_ascii_lowercase(),
        trigger.table.to_ascii_lowercase(),
        trigger.name.to_ascii_lowercase(),
        trigger.timing.to_ascii_uppercase(),
        trigger.event.to_ascii_uppercase(),
        trigger.function,
        trigger.for_each.to_ascii_uppercase(),
        normalize_policy_expr(&trigger.when_clause)
    )
}

fn check_name(check: &ManifestCheck) -> String {
    if check.name.is_empty() {
        check.expression.clone()
    } else {
        check.name.clone()
    }
}

/// Returns a stable key that captures the identity AND content of a sql_artifact.
/// Two artifacts with the same name but different SQL produce different keys,
/// so `diff_sql_artifacts` can detect function body changes.
fn sql_artifact_content_key(artifact: &ManifestSqlArtifact) -> String {
    let sql_hash = {
        let mut h = Sha256::new();
        h.update(artifact.sql.as_bytes());
        format!("{:x}", h.finalize())
    };
    format!(
        "{}|{}|{}|{}",
        artifact.name.to_ascii_lowercase(),
        artifact.backend.to_ascii_lowercase(),
        artifact.phase.to_ascii_lowercase(),
        sql_hash
    )
}

/// Detect sql_artifact additions or content changes.
///
/// When a sql_artifact is new or its SQL body has changed, we emit a
/// `CreateTrigger` change for every trigger on the same table.  This
/// causes `generate_delta_sql` (via `render_delta_table`) to prepend the
/// updated function DDL before re-creating the trigger — guaranteeing that
/// the function and all triggers that depend on it are refreshed atomically.
fn diff_sql_artifacts(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let old_keys: BTreeSet<String> = old
        .sql_artifacts
        .iter()
        .map(sql_artifact_content_key)
        .collect();

    let has_changed = new
        .sql_artifacts
        .iter()
        .any(|artifact| !old_keys.contains(&sql_artifact_content_key(artifact)));

    if !has_changed {
        return;
    }

    // Re-emit CreateTrigger for every trigger associated with this table so
    // that `render_delta_table` includes both the refreshed function DDL and
    // each trigger that calls it.
    for trigger in &new.triggers {
        // Only emit if the trigger is NOT already in the ops list to avoid
        // duplicate entries when diff_triggers already covered it.
        let already_emitted = ops.iter().any(|existing| {
            matches!(existing.kind, ChangeKind::CreateTrigger)
                && existing.object_name == trigger.name
        });
        if !already_emitted {
            ops.push(op(
                ChangeKind::CreateTrigger,
                ChangeSafety::SafeAuto,
                &new.schema,
                &new.table,
                "",
                &trigger.name,
                "sql_artifact function body changed — refreshing trigger to re-apply function",
                "",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, sql_type: &str) -> ManifestColumn {
        ManifestColumn {
            column_name: name.to_string(),
            field_name: name.to_string(),
            sql_type: sql_type.to_string(),
            ..ManifestColumn::default()
        }
    }

    fn table(columns: Vec<ManifestColumn>) -> ManifestTable {
        ManifestTable {
            schema: "public".to_string(),
            table: "patients".to_string(),
            columns,
            ..ManifestTable::default()
        }
    }

    fn manifest(
        mut table: ManifestTable,
        manifest_checksum: &str,
        table_checksum: &str,
    ) -> CatalogManifest {
        table.checksum_sha256 = table_checksum.to_string();
        CatalogManifest {
            checksum_sha256: manifest_checksum.to_string(),
            tables: vec![table],
            ..CatalogManifest::default()
        }
    }

    // ── NW-universal: reserved-field reuse blocked ────────────────

    #[test]
    fn diff_blocks_new_column_reusing_reserved_field_number() {
        // Schema declared `reserved 5;` in v1, then v2 tries to add
        // a new field at number 5. The diff must Block — silently
        // allowing this would mis-route any cached payload still
        // pointing at field 5 to a totally different column.
        let old_manifest = manifest(table(vec![column("id", "TEXT")]), "v1", "v1-table");
        let mut new_table = table(vec![column("id", "TEXT")]);
        new_table.reserved_numbers = vec![ManifestReservedRange { start: 5, end: 5 }];
        let mut new_col = column("badge_no", "TEXT");
        new_col.field_number = 5; // reuses a reserved slot
        new_table.columns.push(new_col);
        let new_manifest = manifest(new_table, "v2", "v2-table");

        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        let add_col = changes
            .iter()
            .find(|c| c.kind == ChangeKind::AddColumn && c.column == "badge_no")
            .expect("add_column emitted");
        assert_eq!(add_col.safety, ChangeSafety::Blocked);
        assert!(
            add_col.blocked_reason.contains("reserved"),
            "blocked_reason should mention 'reserved': {}",
            add_col.blocked_reason
        );
    }

    #[test]
    fn diff_blocks_new_column_reusing_reserved_name() {
        let old_manifest = manifest(table(vec![column("id", "TEXT")]), "v1", "v1-table");
        let mut new_table = table(vec![column("id", "TEXT")]);
        new_table.reserved_names = vec!["old_email".to_string()];
        let mut new_col = column("old_email", "TEXT");
        new_col.field_number = 7;
        new_table.columns.push(new_col);
        let new_manifest = manifest(new_table, "v2", "v2-table");

        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        let add_col = changes
            .iter()
            .find(|c| c.kind == ChangeKind::AddColumn && c.column == "old_email")
            .expect("add_column emitted");
        assert_eq!(add_col.safety, ChangeSafety::Blocked);
        assert!(add_col.blocked_reason.contains("reserved"));
    }

    #[test]
    fn diff_allows_new_column_in_reserved_range_with_different_number() {
        // Regression guard: a new column whose number is OUTSIDE the
        // reserved range must still pass.
        let old_manifest = manifest(table(vec![column("id", "TEXT")]), "v1", "v1-table");
        let mut new_table = table(vec![column("id", "TEXT")]);
        new_table.reserved_numbers = vec![ManifestReservedRange { start: 5, end: 10 }];
        let mut new_col = column("status", "TEXT");
        new_col.field_number = 11; // outside reserved range
        new_table.columns.push(new_col);
        let new_manifest = manifest(new_table, "v2", "v2-table");

        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        let add_col = changes
            .iter()
            .find(|c| c.kind == ChangeKind::AddColumn && c.column == "status")
            .expect("add_column emitted");
        // SafeAuto (or RequiresReview if NOT NULL without default — not our case).
        assert_ne!(add_col.safety, ChangeSafety::Blocked);
    }

    #[test]
    fn manifest_reserved_range_contains_endpoints() {
        let r = ManifestReservedRange { start: 5, end: 10 };
        assert!(r.contains(5));
        assert!(r.contains(10));
        assert!(r.contains(7));
        assert!(!r.contains(4));
        assert!(!r.contains(11));
        // `reserved N to max;` shape.
        let to_max = ManifestReservedRange {
            start: 99,
            end: i32::MAX,
        };
        assert!(to_max.contains(100));
        assert!(to_max.contains(1_000_000));
    }

    #[test]
    fn diff_emits_enum_value_addition() {
        let mut old_status = column("status", "patients_status_enum");
        old_status.enum_values = vec!["active".to_string()];
        let mut new_status = old_status.clone();
        new_status.enum_values.push("paused".to_string());

        let old_manifest = manifest(table(vec![old_status]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_status]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::AlterEnumAddValue
                && change.safety == ChangeSafety::SafeAuto
                && change.object_name == "patients_status_enum"
                && change.column == "paused"
        }));
    }

    #[test]
    fn diff_blocks_enum_value_removal_for_review() {
        let mut old_status = column("status", "patients_status_enum");
        old_status.enum_values = vec!["active".to_string(), "paused".to_string()];
        let mut new_status = old_status.clone();
        new_status.enum_values = vec!["active".to_string()];

        let old_manifest = manifest(table(vec![old_status]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_status]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::DropEnum
                && change.safety == ChangeSafety::RequiresReview
                && change.object_name == "patients_status_enum"
        }));
    }

    #[test]
    fn diff_blocks_proto_field_number_reuse() {
        let mut old_id = column("id", "UUID");
        old_id.field_number = 1;
        let mut new_external_id = column("external_id", "UUID");
        new_external_id.field_number = 1;

        let old_manifest = manifest(table(vec![old_id]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_external_id]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::ValidationError
                && change.safety == ChangeSafety::Blocked
                && change.object_name == "1"
        }));
    }

    #[test]
    fn diff_blocks_type_narrowing_without_using_expression() {
        let old_amount = column("amount", "BIGINT");
        let new_amount = column("amount", "INTEGER");

        let old_manifest = manifest(table(vec![old_amount]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_amount]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::ChangeColumnType
                && change.safety == ChangeSafety::Blocked
                && change.column == "amount"
        }));
    }

    #[test]
    fn diff_requires_review_for_oneof_membership_changes() {
        let old_email = column("email", "TEXT");
        let mut new_email = old_email.clone();
        new_email.oneof_group = "contact".to_string();

        let old_manifest = manifest(table(vec![old_email]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_email]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::ValidationError
                && change.safety == ChangeSafety::RequiresReview
                && change.column == "email"
        }));
    }

    #[test]
    fn diff_marks_partitioning_added_as_review_only() {
        let old_table = table(vec![
            column("id", "UUID"),
            column("created_at", "TIMESTAMPTZ"),
        ]);
        let mut new_table = old_table.clone();
        new_table.partition_strategy = "PARTITION_STRATEGY_RANGE_MONTH".to_string();
        new_table.partition_column = "created_at".to_string();
        new_table.partition_interval = "MONTHLY".to_string();

        let old_manifest = manifest(old_table, "old", "old-table");
        let new_manifest = manifest(new_table, "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::CreatePartition
                && change.safety == ChangeSafety::RequiresReview
                && change.column == "created_at"
        }));
    }

    #[test]
    fn diff_marks_partition_option_change_as_review_only() {
        let mut old_table = table(vec![
            column("id", "UUID"),
            column("created_at", "TIMESTAMPTZ"),
        ]);
        old_table.partition_strategy = "PARTITION_STRATEGY_RANGE_MONTH".to_string();
        old_table.partition_column = "created_at".to_string();
        old_table.partition_interval = "MONTHLY".to_string();

        let mut new_table = old_table.clone();
        new_table.partition_interval = "WEEKLY".to_string();

        let old_manifest = manifest(old_table, "old", "old-table");
        let new_manifest = manifest(new_table, "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::AttachPartition
                && change.safety == ChangeSafety::RequiresReview
                && change.object_name == "partitioning"
        }));
    }
}
