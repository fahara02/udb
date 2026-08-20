use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::generation::manifest::{
    CatalogManifest, ManifestCheck, ManifestColumn, ManifestExtension, ManifestForeignKey,
    ManifestIndex, ManifestMaterializedView, ManifestPolicy, ManifestSqlArtifact, ManifestTable,
    ManifestTrigger, check_key, fk_key, index_key,
};
use crate::generation::sql::validate_enum_value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ChangeKind {
    HintWarning,
    ApplySqlArtifact,
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
    /// A change to a table's `db_table_security` (tenant/project isolation mode or
    /// column, RLS policy template, soft-delete mode, audit mode, retention,
    /// encryption/PII profile). Review-required: it alters the tenant-isolation /
    /// compliance posture of persisted data.
    AlterTableSecurity,
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
    /// Whether applying this operation can DESTROY existing row data.
    ///
    /// Consumers gate deploys on the plan, and the only signal used to be the
    /// operation name — so "never drop a column" had to be written as "reject
    /// anything starting with Drop". That also rejects `DropIndex`/`DropPolicy`
    /// (reissued on ordinary version upgrades, each paired with a matching
    /// Create) and `DropNotNull` (a widening that cannot reject any write which
    /// succeeds today), which makes routine upgrades impossible. Downstream
    /// tooling should gate on THIS flag, never on the verb.
    ///
    /// True only where committed rows or their values can be lost. Dropping a
    /// constraint, index, policy or default removes enforcement or a derived
    /// object, not data, so those are false.
    pub data_destructive: bool,
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
            // Gate enum types created during bootstrap with the same safety
            // classification as enum additions on an existing table: unsafe values
            // are Blocked, otherwise SafeAuto. Without this, a fresh-bootstrap
            // (old = None) path would emit unvalidated CREATE TYPE ... AS ENUM DDL.
            for col in &table.columns {
                if !col.enum_values.is_empty() {
                    let (safety, blocked_reason) = enum_create_safety(&col.enum_values);
                    ops.push(op(
                        ChangeKind::CreateEnum,
                        safety,
                        &table.schema,
                        &table.table,
                        &col.column_name,
                        &enum_type_name(table, &col.column_name),
                        "enum column is present in desired proto AST",
                        blocked_reason.as_deref().unwrap_or_default(),
                    ));
                }
            }
            lint_bootstrap_review_gates(table, &mut ops);
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
    // UDB-MIG-001: classify still-present `allow_drop` annotations only AFTER all
    // drop operations are known, so a hint that unblocked a drop this run is
    // recorded (non-blocking) rather than re-blocking the reviewed migration.
    classify_allow_drop_hints(old, new, &mut ops);
    finalize_ops(&mut ops);
    ops
}

/// UDB-MIG-001: an `allow_drop` annotation that remains on a table/column which
/// still exists in the desired proto is the middle phase of the documented
/// two-phase migration (`allow_drop` → apply → remove `allow_drop`). Emit its
/// diagnostic as a NON-blocking `HintWarning` so startup is never caught in the
/// circular gate where keeping the hint blocks on the warning and removing it
/// re-blocks the drop:
///
///  * **Consumed** — the annotation made at least one otherwise-blocked drop
///    (a non-unique `DropIndex` / `DropForeignKey`, or a `DropColumn`) safe in
///    *this* diff. The hint names the exact freed index/constraint so the audit
///    trail records which operation the annotation approved.
///  * **Unconsumed** — no destructive op in this diff needed the annotation, so
///    it is a stale leftover; advise the operator to remove it. Still
///    non-blocking.
///
/// A drop that is genuinely unsafe (no `allow_drop`) keeps its own
/// `RequiresReview` op from `diff_indexes` / `diff_foreign_keys` / `diff_columns`
/// / the drop-table pass, so the fail-closed posture is unchanged.
fn classify_allow_drop_hints(
    old: &CatalogManifest,
    new: &CatalogManifest,
    ops: &mut Vec<ChangeOperation>,
) {
    const STALE_HINT_ADVICE: &str = "remove the stale allow_drop annotation now that the destructive migration has been applied";
    let mut hints: Vec<ChangeOperation> = Vec::new();
    for table in &new.tables {
        // A created or dropped table cannot carry a "still-present" hint: the
        // annotation only matters while the table survives in both manifests.
        let Some(old_table) = old.table(&table.schema, &table.table) else {
            continue;
        };
        let has_table_allow_drop = table.allow_drop;
        let column_allow_drops: Vec<&ManifestColumn> = table
            .columns
            .iter()
            .filter(|column| {
                column.allow_drop
                    && old_table
                        .columns
                        .iter()
                        .any(|old_col| old_col.column_name == column.column_name)
            })
            .collect();
        if !has_table_allow_drop && column_allow_drops.is_empty() {
            continue;
        }

        // The SafeAuto destructive drops this diff already decided for the table
        // are the single source of truth for "what an allow_drop freed" — never
        // re-derive the drop decision here.
        let safe_drops: Vec<&ChangeOperation> = ops
            .iter()
            .filter(|o| {
                o.safety == ChangeSafety::SafeAuto
                    && o.schema == table.schema
                    && o.table == table.table
                    && matches!(
                        o.kind,
                        ChangeKind::DropIndex
                            | ChangeKind::DropForeignKey
                            | ChangeKind::DropColumn
                            | ChangeKind::DropUnique
                    )
            })
            .collect();

        // Map each freed index / FK object name back to its columns so a
        // column-scoped hint can claim the exact resource it unblocked.
        let index_cols: BTreeMap<String, &Vec<String>> = old_table
            .indexes
            .iter()
            .map(|idx| (index_name(old_table, idx), &idx.columns))
            .collect();
        let fk_cols: BTreeMap<String, &Vec<String>> = old_table
            .foreign_keys
            .iter()
            .map(|fk| (fk_name(old_table, fk), &fk.columns))
            .collect();
        let covers_column = |o: &ChangeOperation, column: &str| -> bool {
            match o.kind {
                ChangeKind::DropIndex => index_cols
                    .get(&o.object_name)
                    .map(|cols| cols.iter().any(|c| c.as_str() == column))
                    .unwrap_or(false),
                ChangeKind::DropForeignKey => fk_cols
                    .get(&o.object_name)
                    .map(|cols| cols.iter().any(|c| c.as_str() == column))
                    .unwrap_or(false),
                // A DropUnique is column-scoped (its object_name IS the column),
                // so an allow_drop on that retained column is what freed it.
                ChangeKind::DropUnique => o.object_name == column,
                _ => false,
            }
        };

        // ── Table-level allow_drop still present ──────────────────────────
        if has_table_allow_drop {
            match safe_drops.first() {
                Some(freed) => hints.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    "",
                    &freed.object_name,
                    &format!(
                        "table allow_drop consumed this run by {:?} {}",
                        freed.kind, freed.object_name
                    ),
                    "",
                )),
                None => hints.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    "",
                    "allow_drop",
                    "table allow_drop is set but no destructive operation consumed it this run",
                    STALE_HINT_ADVICE,
                )),
            }
        }

        // ── Column-level allow_drop still present ─────────────────────────
        for column in column_allow_drops {
            match safe_drops
                .iter()
                .find(|o| covers_column(o, &column.column_name))
            {
                Some(freed) => hints.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    &column.column_name,
                    &freed.object_name,
                    &format!(
                        "column allow_drop consumed this run by {:?} {}",
                        freed.kind, freed.object_name
                    ),
                    "",
                )),
                None => hints.push(op(
                    ChangeKind::HintWarning,
                    ChangeSafety::SafeAuto,
                    &table.schema,
                    &table.table,
                    &column.column_name,
                    "allow_drop",
                    "column allow_drop is set but no destructive operation consumed it this run",
                    STALE_HINT_ADVICE,
                )),
            }
        }
    }
    ops.extend(hints);
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
        // UDB-MIG-001: a still-present `allow_drop` on a retained table/column is
        // NOT classified here. Its blocking-vs-advisory status depends on whether
        // any destructive op in this same diff consumed it, which is only known
        // after the table/index/FK/column diffs run — see
        // `classify_allow_drop_hints`, invoked once at the end of `diff_manifests`.

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
            // UDB-MIG-001: still-present column `allow_drop` is classified in
            // `classify_allow_drop_hints` after the drop diffs are known.
        }
    }
}

fn lint_bootstrap_review_gates(table: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    for artifact in table
        .sql_artifacts
        .iter()
        .filter(|artifact| artifact.requires_review)
    {
        ops.push(op(
            ChangeKind::ApplySqlArtifact,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            "",
            &artifact.name,
            "sql_artifact is marked requires_review in desired proto AST",
            "review-required SQL artifacts cannot be applied by unattended bootstrap",
        ));
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
    diff_table_security(old, new, ops);

    if !old.enable_rls && new.enable_rls {
        // urgent_fix #25: enabling RLS on an EXISTING table is review-required, not
        // SafeAuto — turning RLS on immediately filters every row that doesn't match
        // a policy, so an existing table's reads can silently go empty until the
        // policies are confirmed. Symmetric with DisableRls (both alter the
        // row-visibility posture). (Fresh-bootstrap tables create RLS via CreateTable,
        // not this diff op, so first-time setup is unaffected.)
        ops.push(op(
            ChangeKind::EnableRls,
            ChangeSafety::RequiresReview,
            &new.schema,
            &new.table,
            "",
            &new.table,
            "desired proto AST enables row-level security on an existing table",
            "enabling RLS changes row visibility on existing data and requires migration review",
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

/// urgent_fix #23: surface `db_table_security` changes as a review-required
/// migration item. Previously the migration diff never inspected `table_security`,
/// so a change to the tenant-isolation mode/column, RLS policy template,
/// soft-delete mode, audit mode, retention, or encryption/PII profile produced NO
/// migration/drift item (only the orthogonal CLI contract-diff saw it). These are
/// security-posture changes to persisted data and must not apply unattended.
fn diff_table_security(old: &ManifestTable, new: &ManifestTable, ops: &mut Vec<ChangeOperation>) {
    let o = &old.table_security;
    let n = &new.table_security;
    let mut changed: Vec<&str> = Vec::new();
    if o.tenant_isolation_mode != n.tenant_isolation_mode {
        changed.push("tenant_isolation_mode");
    }
    if o.project_isolation_mode != n.project_isolation_mode {
        changed.push("project_isolation_mode");
    }
    if o.tenant_column != n.tenant_column {
        changed.push("tenant_column");
    }
    if o.project_column != n.project_column {
        changed.push("project_column");
    }
    if o.rls_policy_template != n.rls_policy_template {
        changed.push("rls_policy_template");
    }
    if o.soft_delete_mode != n.soft_delete_mode {
        changed.push("soft_delete_mode");
    }
    if o.retention_class != n.retention_class {
        changed.push("retention_class");
    }
    if o.retention_days != n.retention_days {
        changed.push("retention_days");
    }
    if o.audit_mode != n.audit_mode {
        changed.push("audit_mode");
    }
    if o.encryption_profile != n.encryption_profile {
        changed.push("encryption_profile");
    }
    if o.pii_profile != n.pii_profile {
        changed.push("pii_profile");
    }
    if o.break_glass_visible != n.break_glass_visible {
        changed.push("break_glass_visible");
    }
    if o.export_eligible != n.export_eligible {
        changed.push("export_eligible");
    }
    if o.data_residency_policy_ref != n.data_residency_policy_ref {
        changed.push("data_residency_policy_ref");
    }
    if changed.is_empty() {
        return;
    }
    ops.push(op(
        ChangeKind::AlterTableSecurity,
        ChangeSafety::RequiresReview,
        &new.schema,
        &new.table,
        "",
        &new.table,
        &format!("db_table_security changed: {}", changed.join(", ")),
        "table-security changes alter tenant-isolation / compliance posture and require migration review",
    ));
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
            let (safety, blocked_reason) = enum_create_safety(&new_col.enum_values);
            ops.push(op(
                ChangeKind::CreateEnum,
                safety,
                &new.schema,
                &new.table,
                &new_col.column_name,
                &enum_type_name(new, &new_col.column_name),
                "enum column was added to desired proto AST",
                blocked_reason.as_deref().unwrap_or_default(),
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
        // Mirror DropColumn: an explicit `allow_drop` on the column is the
        // operator's reviewed opt-in, so promote the drop to SafeAuto; otherwise
        // it needs review (approved plan or manual apply). Without this, a
        // column-level `unique: true → false` had NO annotation route and could
        // only ever go through the review gate.
        let safety = if old.allow_drop || new.allow_drop {
            ChangeSafety::SafeAuto
        } else {
            ChangeSafety::RequiresReview
        };
        let blocked = if safety == ChangeSafety::SafeAuto {
            ""
        } else {
            "drop_unique requires explicit review (or add allow_drop to the column)"
        };
        ops.push(op(
            ChangeKind::DropUnique,
            safety,
            &table.schema,
            &table.table,
            &new.column_name,
            &new.column_name,
            "column is no longer unique",
            blocked,
        ));
    }
}

/// True when `column` is one of the auto-appended audit columns on an
/// audit-enabled table, i.e. generator-owned rather than schema-author-owned.
fn is_audit_column(table: &ManifestTable, column: &ManifestColumn) -> bool {
    table.audit_fields
        && crate::generation::manifest::AUDIT_FIELD_COLUMNS.contains(&column.column_name.as_str())
}

/// True when `old_col` sat in the block the generator APPENDS: one of the audit
/// trio, numbered above every column the schema author declared.
///
/// `append_missing_audit_columns` only ever adds the trio after the highest
/// explicit number, so an author-declared `created_at` in the middle of a
/// message does not match. This is the structural half of the proof that a
/// number was synthetic rather than a real wire field number.
fn sat_in_appended_audit_block(old: &ManifestTable, old_col: &ManifestColumn) -> bool {
    if !crate::generation::manifest::AUDIT_FIELD_COLUMNS.contains(&old_col.column_name.as_str()) {
        return false;
    }
    old.columns
        .iter()
        .filter(|c| {
            !crate::generation::manifest::AUDIT_FIELD_COLUMNS.contains(&c.column_name.as_str())
        })
        .all(|c| c.field_number < old_col.field_number)
}

/// True when the number `old_col` held was a GENERATOR-APPENDED audit column's
/// number, so nothing on the wire can break by reusing it.
///
/// Two independent proofs, either of which is sufficient once
/// [`sat_in_appended_audit_block`] holds:
///
/// * the new manifest carries that column at or above
///   `AUDIT_FIELD_NUMBER_BASE`. Only the generator puts a column there - had the
///   author declared `created_at`, the generator would have left their number
///   alone - so the column is generator-owned on both sides.
/// * the new manifest has no column of that name at all, and audit was enabled
///   on the old table. A column the author declares does not disappear when
///   audit is switched off, so its absence proves it existed only because the
///   generator appended it.
///
/// The old table's `audit_fields` flag is deliberately NOT required for the
/// first proof. Requiring it blocked upgrades for any deployment whose stored
/// manifest did not carry the flag, which is exactly the unrecoverable case
/// this exemption exists to prevent.
fn audit_column_vacated_number(
    old: &ManifestTable,
    new: &ManifestTable,
    old_col: &ManifestColumn,
) -> bool {
    if !sat_in_appended_audit_block(old, old_col) {
        return false;
    }
    let relocated_into_reserved_range = new.columns.iter().any(|candidate| {
        candidate.column_name == old_col.column_name
            && candidate.field_number >= crate::generation::manifest::AUDIT_FIELD_NUMBER_BASE
    });
    if relocated_into_reserved_range {
        return true;
    }
    let dropped_with_audit = old.audit_fields
        && !new
            .columns
            .iter()
            .any(|candidate| candidate.column_name == old_col.column_name);
    dropped_with_audit
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
        // The number used to belong to a generator-appended audit column that has
        // since moved into the reserved audit range. Nothing on the wire breaks:
        // `created_at`/`updated_at`/`created_by` are synthesized into the manifest
        // and appear in no `.proto`, so their number was never a real protobuf
        // field number — it was an artifact of numbering them `max(explicit) + 1`.
        // Without this, every deployment whose manifest recorded the audit block at
        // low numbers is permanently blocked from upgrading the moment an explicit
        // field is appended, with no supported recovery.
        //
        // The rule is deliberately narrow: the vacating column must be one of the
        // audit trio on an audit-enabled table AND must now sit at or above
        // `AUDIT_FIELD_NUMBER_BASE`. Only the generator places a column there, so a
        // hand-written `created_at` that a schema author renumbered is still
        // reported.
        if audit_column_vacated_number(old, new, old_col) {
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
            if is_audit_column(old, old_col) {
                "field number reuse breaks protobuf compatibility; this number previously held the auto-generated audit column, so it is not a field you declared — regenerate the manifest with a build that anchors the audit block, or reserve the old number"
            } else {
                "field number reuse breaks protobuf compatibility; reserve the old number or declare previous_column_name for an intentional rename"
            },
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
        let (safety, blocked_reason) = enum_create_safety(&new.enum_values);
        ops.push(op(
            ChangeKind::CreateEnum,
            safety,
            &table.schema,
            &table.table,
            &new.column_name,
            &enum_name,
            "column gained enum values",
            blocked_reason.as_deref().unwrap_or_default(),
        ));
        return;
    }

    for value in new_values.difference(&old_values) {
        let safety = if validate_enum_value(value).is_ok() {
            ChangeSafety::RequiresReview
        } else {
            ChangeSafety::Blocked
        };
        let blocked_reason = if safety == ChangeSafety::Blocked {
            "enum value failed SQL safety validation"
        } else {
            "PostgreSQL enum value additions are non-transactional and require production review"
        };
        ops.push(op(
            ChangeKind::AlterEnumAddValue,
            safety,
            &table.schema,
            &table.table,
            value,
            &enum_name,
            "enum value was added to desired proto AST",
            blocked_reason,
        ));
    }

    if old_values.difference(&new_values).next().is_some() {
        ops.push(op(
            ChangeKind::DropEnum,
            ChangeSafety::Blocked,
            &table.schema,
            &table.table,
            &new.column_name,
            &enum_name,
            "enum values were removed from desired proto AST",
            "PostgreSQL cannot drop enum values in place; a reviewed replacement migration is required",
        ));
    }

    // Reordering: identical value set but a different ordered sequence. This is
    // not caught by the set-based add/drop checks above, yet it changes protobuf
    // enum field numbers (wire compatibility) and PostgreSQL enum sort order.
    // Treat as review-required (mirrors the oneof presence-semantics gate).
    if old_values == new_values && old.enum_values != new.enum_values {
        ops.push(op(
            ChangeKind::ValidationError,
            ChangeSafety::RequiresReview,
            &table.schema,
            &table.table,
            &new.column_name,
            &enum_name,
            "enum value order changed",
            "enum value reordering changes protobuf field numbers and PostgreSQL enum sort order; requires review",
        ));
    }
}

fn enum_create_safety(values: &[String]) -> (ChangeSafety, Option<String>) {
    for value in values {
        if let Err(reason) = validate_enum_value(value) {
            return (
                ChangeSafety::Blocked,
                Some(format!(
                    "enum value '{}' failed SQL safety validation: {}",
                    value, reason
                )),
            );
        }
    }
    (ChangeSafety::SafeAuto, None)
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
        op.data_destructive = is_data_destructive(&op.kind);
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

/// Whether a change kind can destroy committed row data.
///
/// Deliberately conservative: anything that removes rows, removes a column's
/// values, or rewrites values in place is destructive. Removing enforcement (a
/// constraint, index, policy, default or NOT NULL) is not, and neither is
/// detaching a partition, whose rows survive in the detached table.
pub fn is_data_destructive(kind: &ChangeKind) -> bool {
    match kind {
        // Rows or their values are lost.
        ChangeKind::DropTable
        | ChangeKind::DropColumn
        | ChangeKind::DropPartition
        | ChangeKind::DropCollection
        | ChangeKind::DropBucket
        | ChangeKind::DropStore => true,
        // An in-place type rewrite can be lossy (narrowing, failed cast); the
        // planner emits a USING expression where it can, but the operation is
        // still value-rewriting, so it must not be auto-approved as harmless.
        ChangeKind::ChangeColumnType => true,
        // Storage-engine and validator rewrites re-materialise existing rows.
        ChangeKind::ChangeTableEngine => true,
        // Everything else removes enforcement (constraint, index, policy, default,
        // NOT NULL), removes a DERIVED object that can be rebuilt
        // (`DropMaterializedView`), detaches a partition whose rows survive in the
        // detached table, or only adds. None of those lose source-of-truth rows,
        // and flagging them is what made routine upgrades impossible downstream.
        _ => false,
    }
}

fn priority(kind: &ChangeKind) -> i32 {
    match kind {
        ChangeKind::ValidationError => 0,
        ChangeKind::HintWarning => 1,
        ChangeKind::ApplySqlArtifact => 87,
        ChangeKind::CreateExtension | ChangeKind::DropExtension => 5,
        ChangeKind::AddSchema => 10,
        ChangeKind::CreateEnum => 15,
        ChangeKind::AlterEnumAddValue => 16,
        ChangeKind::CreateTable => 20,
        ChangeKind::CreatePartition => 22,
        ChangeKind::AttachPartition => 23,
        ChangeKind::RenameTable => 25,
        ChangeKind::SetTableLogged | ChangeKind::SetTableUnlogged | ChangeKind::SetTablespace => 28,
        ChangeKind::AlterTableSecurity => 29,
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
        // A redefined index is represented as Drop+Create with the SAME name, and
        // `diff_indexes` pushes every CreateIndex before any DropIndex. While both
        // shared one priority the sort tied on (schema, priority, table, column,
        // object_name) and the stable sort kept that insertion order, so the
        // migration emitted `CREATE INDEX x` followed by `DROP INDEX x` and
        // destroyed the index it had just built — the apply still reported
        // success, and startup verification then failed on an index the manifest
        // requires. Drop sorts first now, exactly as DropPolicy/CreatePolicy below.
        ChangeKind::DropIndex => 69,
        ChangeKind::CreateIndex => 70,
        ChangeKind::EnableRls | ChangeKind::DisableRls => 80,
        // A changed policy is represented as Drop+Create with the same name.
        // Drop must sort first or CREATE collides with the still-live old policy.
        ChangeKind::DropPolicy => 84,
        ChangeKind::CreatePolicy => 85,
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
        "{}|{}|{}|{}|{}|{}|{}",
        artifact.name.to_ascii_lowercase(),
        artifact.backend.to_ascii_lowercase(),
        artifact.phase.to_ascii_lowercase(),
        artifact.file,
        artifact.checksum_sha256,
        artifact.requires_review,
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

    for artifact in new
        .sql_artifacts
        .iter()
        .filter(|artifact| !old_keys.contains(&sql_artifact_content_key(artifact)))
    {
        ops.push(op(
            ChangeKind::ApplySqlArtifact,
            if artifact.requires_review {
                ChangeSafety::RequiresReview
            } else {
                ChangeSafety::SafeAuto
            },
            &new.schema,
            &new.table,
            "",
            &artifact.name,
            "sql_artifact content, file, checksum, phase, or review flag changed",
            if artifact.requires_review {
                "review-required SQL artifacts cannot be applied unattended"
            } else {
                ""
            },
        ));
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
                if new
                    .sql_artifacts
                    .iter()
                    .any(|artifact| artifact.requires_review)
                {
                    ChangeSafety::RequiresReview
                } else {
                    ChangeSafety::SafeAuto
                },
                &new.schema,
                &new.table,
                "",
                &trigger.name,
                "sql_artifact function body changed — refreshing trigger to re-apply function",
                if new
                    .sql_artifacts
                    .iter()
                    .any(|artifact| artifact.requires_review)
                {
                    "review-required SQL artifacts cannot refresh triggers unattended"
                } else {
                    ""
                },
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `ManifestReservedRange` is only constructed by the reserved-field-reuse
    // tests; production code reads the `reserved_numbers`/`reserved_names`
    // ManifestTable fields, not the range type itself.
    use crate::generation::manifest::ManifestReservedRange;

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

    /// The verb is not the signal. A downstream guard written as "reject anything
    /// starting with Drop" also rejects the drops UDB reissues on ordinary version
    /// upgrades, which made every upgrade impossible for a consumer. `data_destructive`
    /// exists so tooling gates on intent instead.
    #[test]
    fn data_destructive_marks_row_loss_and_not_mere_verbs() {
        for kind in [
            ChangeKind::DropTable,
            ChangeKind::DropColumn,
            ChangeKind::DropPartition,
            ChangeKind::DropCollection,
            ChangeKind::DropBucket,
            ChangeKind::DropStore,
            ChangeKind::ChangeColumnType,
            ChangeKind::ChangeTableEngine,
        ] {
            assert!(
                is_data_destructive(&kind),
                "{kind:?} can lose committed rows or values and must be flagged"
            );
        }

        // Each of these is a "Drop*" that removes enforcement or a derived object.
        // Flagging them is what blocked routine upgrades downstream.
        for kind in [
            ChangeKind::DropIndex,
            ChangeKind::DropPolicy,
            ChangeKind::DropNotNull,
            ChangeKind::DropUnique,
            ChangeKind::DropForeignKey,
            ChangeKind::DropConstraint,
            ChangeKind::DropDefault,
            ChangeKind::DropTrigger,
            ChangeKind::DropMaterializedView,
            ChangeKind::DetachPartition,
        ] {
            assert!(
                !is_data_destructive(&kind),
                "{kind:?} removes enforcement or a rebuildable object, not rows"
            );
        }

        // Additive work is never destructive.
        for kind in [
            ChangeKind::CreateTable,
            ChangeKind::AddColumn,
            ChangeKind::CreateIndex,
        ] {
            assert!(!is_data_destructive(&kind), "{kind:?} only adds");
        }
    }

    /// The flag must be populated on every finalized operation, not left default.
    #[test]
    fn finalize_populates_data_destructive() {
        let mut ops = vec![
            op(
                ChangeKind::DropColumn,
                ChangeSafety::RequiresReview,
                "public",
                "patients",
                "ssn",
                "",
                "column removed from proto",
                "",
            ),
            op(
                ChangeKind::DropIndex,
                ChangeSafety::SafeAuto,
                "public",
                "patients",
                "",
                "idx_patients_name",
                "index removed from proto",
                "",
            ),
        ];
        finalize_ops(&mut ops);
        let dropped_column = ops
            .iter()
            .find(|o| o.kind == ChangeKind::DropColumn)
            .expect("drop column present");
        let dropped_index = ops
            .iter()
            .find(|o| o.kind == ChangeKind::DropIndex)
            .expect("drop index present");
        assert!(dropped_column.data_destructive);
        assert!(!dropped_index.data_destructive);
    }

    /// A redefined index (same name, changed columns) must DROP before it
    /// CREATEs. Both ops previously shared priority 70, and `diff_indexes` emits
    /// every create before any drop, so the stable sort left `CREATE INDEX x`
    /// ahead of `DROP INDEX x` — the migration deleted the index it had just
    /// created, reported success, and startup then failed verification on an
    /// index the manifest requires.
    #[test]
    fn redefined_index_drops_before_it_creates() {
        let mut ops = vec![
            op(
                ChangeKind::CreateIndex,
                ChangeSafety::SafeAuto,
                "udb_vault",
                "vault_db_credential_leases",
                "",
                "idx_vault_db_credential_leases_tenant",
                "index was added to desired proto AST",
                "",
            ),
            op(
                ChangeKind::DropIndex,
                ChangeSafety::SafeAuto,
                "udb_vault",
                "vault_db_credential_leases",
                "",
                "idx_vault_db_credential_leases_tenant",
                "index no longer exists in desired proto AST",
                "",
            ),
        ];
        finalize_ops(&mut ops);
        assert_eq!(
            ops[0].kind,
            ChangeKind::DropIndex,
            "the drop must run first or the create is undone by it"
        );
        assert_eq!(ops[1].kind, ChangeKind::CreateIndex);
        assert!(
            priority(&ChangeKind::DropIndex) < priority(&ChangeKind::CreateIndex),
            "index drop/create must not share a priority"
        );
    }

    /// The generator appends the audit trio after the highest explicit field.
    /// When the schema author later adds a field on one of those numbers, the
    /// upgrade was refused as protobuf field-number reuse - but a synthetic audit
    /// number was never a wire field number, so there is nothing to break.
    ///
    /// The first exemption only recognised this when the OLD manifest recorded
    /// `audit_fields: true` and the new one still carried the column. Two shapes
    /// escaped it and left an upgrade permanently blocked with no recovery: a
    /// stored manifest without the flag, and audit switched off so the columns
    /// are gone. Both are proved synthetic by other means; a genuine user-field
    /// reuse must still be refused.
    #[test]
    fn synthetic_audit_numbers_never_block_a_later_explicit_field() {
        let numbered = |name: &str, number: i32| {
            let mut c = column(name, "TEXT");
            c.field_number = number;
            c
        };
        let base = crate::generation::manifest::AUDIT_FIELD_NUMBER_BASE;
        let mk = |cols: Vec<ManifestColumn>, audit: bool| {
            let mut t = table(cols);
            t.audit_fields = audit;
            t
        };
        let blocked = |old: &ManifestTable, new: &ManifestTable| -> Vec<String> {
            let oc: BTreeMap<String, &ManifestColumn> = old
                .columns
                .iter()
                .map(|c| (c.column_name.clone(), c))
                .collect();
            let mut ops = Vec::new();
            detect_field_number_reuse(old, new, &oc, &mut ops);
            ops.iter().map(|o| o.column.clone()).collect()
        };

        // The stored manifest never recorded `audit_fields`, but the new manifest
        // puts `created_at` in the reserved range - only the generator does that,
        // so the column is generator-owned and its old number was synthetic.
        let old = mk(vec![numbered("f1", 1), numbered("created_at", 12)], false);
        let new = mk(
            vec![
                numbered("f1", 1),
                numbered("project_id", 12),
                numbered("created_at", base),
            ],
            true,
        );
        assert!(
            blocked(&old, &new).is_empty(),
            "a manifest without the audit flag must still upgrade: {:?}",
            blocked(&old, &new)
        );

        // Audit switched off: the trio is gone. A column the AUTHOR declares does
        // not vanish when audit is disabled, so its absence proves it was
        // generator-appended.
        let old = mk(vec![numbered("f1", 1), numbered("created_at", 12)], true);
        let new = mk(vec![numbered("f1", 1), numbered("project_id", 12)], false);
        assert!(
            blocked(&old, &new).is_empty(),
            "dropping audit must not block reusing its synthetic number: {:?}",
            blocked(&old, &new)
        );

        // A real user field losing its number is still a compatibility break.
        let old = mk(vec![numbered("f1", 1), numbered("legacy", 12)], true);
        let new = mk(vec![numbered("f1", 1), numbered("replacement", 12)], true);
        assert_eq!(
            blocked(&old, &new).len(),
            1,
            "a genuine field-number reuse must stay blocked"
        );

        // An author-declared audit column sitting BELOW an explicit field is not
        // in the appended block, so reusing its number stays blocked.
        let old = mk(vec![numbered("created_at", 3), numbered("f9", 9)], true);
        let new = mk(vec![numbered("taken", 3), numbered("f9", 9)], true);
        assert_eq!(
            blocked(&old, &new).len(),
            1,
            "an author-declared created_at is a real wire field"
        );
    }
    /// A deployment created before the audit block was anchored records
    /// `created_at`/`updated_at`/`created_by` at low numbers. Appending explicit
    /// fields moved the block, so those numbers were re-used by real fields and
    /// the drift gate blocked startup — unconditionally, on numbers the schema
    /// author never wrote. Relocating the audit block into the reserved range is
    /// not a wire break (audit columns exist in no `.proto`), so it must not be
    /// reported.
    #[test]
    fn audit_block_relocation_is_not_field_number_reuse() {
        let numbered = |name: &str, number: i32| {
            let mut c = column(name, "TEXT");
            c.field_number = number;
            c
        };
        let audited = |columns: Vec<ManifestColumn>| {
            let mut t = table(columns);
            t.audit_fields = true;
            t
        };

        // Recorded by the older broker: 11 explicit fields, audit block at 12-14.
        let old = audited(vec![
            numbered("f1", 1),
            numbered("created_at", 12),
            numbered("updated_at", 13),
            numbered("created_by", 14),
        ]);
        // Current schema: explicit fields appended onto 12-14, audit block moved
        // into the reserved range.
        let new = audited(vec![
            numbered("f1", 1),
            numbered("project_id", 12),
            numbered("idempotency_key", 13),
            numbered("request_hash", 14),
            numbered(
                "created_at",
                crate::generation::manifest::AUDIT_FIELD_NUMBER_BASE,
            ),
            numbered(
                "updated_at",
                crate::generation::manifest::AUDIT_FIELD_NUMBER_BASE + 1,
            ),
            numbered(
                "created_by",
                crate::generation::manifest::AUDIT_FIELD_NUMBER_BASE + 2,
            ),
        ]);

        let old_columns: BTreeMap<String, &ManifestColumn> = old
            .columns
            .iter()
            .map(|c| (c.column_name.clone(), c))
            .collect();
        let mut ops = Vec::new();
        detect_field_number_reuse(&old, &new, &old_columns, &mut ops);
        assert!(
            ops.is_empty(),
            "relocating the generated audit block must not block the upgrade: {:?}",
            ops.iter().map(|o| o.column.clone()).collect::<Vec<_>>()
        );
    }

    /// The forgiveness above must stay narrow: a genuine reuse of a number that a
    /// non-audit column vacated is still a blocked change.
    #[test]
    fn ordinary_field_number_reuse_is_still_blocked() {
        let numbered = |name: &str, number: i32| {
            let mut c = column(name, "TEXT");
            c.field_number = number;
            c
        };
        let old = table(vec![numbered("legacy_code", 7)]);
        let new = table(vec![numbered("tenant_ref", 7)]);
        let old_columns: BTreeMap<String, &ManifestColumn> = old
            .columns
            .iter()
            .map(|c| (c.column_name.clone(), c))
            .collect();
        let mut ops = Vec::new();
        detect_field_number_reuse(&old, &new, &old_columns, &mut ops);
        assert_eq!(
            ops.len(),
            1,
            "a real field-number reuse must still be caught"
        );
        assert_eq!(ops[0].safety, ChangeSafety::Blocked);
    }

    fn sql_artifact(name: &str, sql: &str) -> ManifestSqlArtifact {
        ManifestSqlArtifact {
            name: name.to_string(),
            backend: "postgres".to_string(),
            phase: "before_triggers".to_string(),
            sql: sql.to_string(),
            ..ManifestSqlArtifact::default()
        }
    }

    #[test]
    fn sql_artifact_identity_includes_file_checksum_and_review_flag() {
        let mut a = sql_artifact("touch", "SELECT 1;");
        let mut b = a.clone();
        assert_eq!(sql_artifact_content_key(&a), sql_artifact_content_key(&b));

        b.file = "sql/touch.sql".to_string();
        assert_ne!(sql_artifact_content_key(&a), sql_artifact_content_key(&b));

        a.file = b.file.clone();
        b.checksum_sha256 = "abc123".to_string();
        assert_ne!(sql_artifact_content_key(&a), sql_artifact_content_key(&b));

        a.checksum_sha256 = b.checksum_sha256.clone();
        b.requires_review = true;
        assert_ne!(sql_artifact_content_key(&a), sql_artifact_content_key(&b));
    }

    #[test]
    fn changed_rls_policy_drops_before_recreate() {
        let mut old_table = table(vec![column("tenant_id", "TEXT")]);
        old_table.rls_policies = vec![ManifestPolicy {
            name: "table_security_isolation".to_string(),
            command: "ALL".to_string(),
            using_expression: "tenant_id = current_setting('app.current_tenant_id')".to_string(),
            with_check: "tenant_id = current_setting('app.current_tenant_id')".to_string(),
            permissive: true,
        }];
        let mut new_table = old_table.clone();
        new_table.rls_policies[0].using_expression =
            "tenant_id = current_setting('app.current_tenant_id') AND project_id = current_setting('app.current_project_id')".to_string();
        new_table.rls_policies[0].with_check = new_table.rls_policies[0].using_expression.clone();

        let old_manifest = manifest(old_table, "old", "old-table");
        let new_manifest = manifest(new_table, "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        let policy_changes = changes
            .iter()
            .filter(|change| change.object_name == "table_security_isolation")
            .map(|change| change.kind.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            policy_changes,
            vec![ChangeKind::DropPolicy, ChangeKind::CreatePolicy],
            "changed same-name RLS policy must be dropped before it is recreated"
        );
    }

    #[test]
    fn review_required_sql_artifact_blocks_unattended_delta() {
        let old_manifest = manifest(table(vec![column("id", "TEXT")]), "v1", "v1-table");
        let mut new_table = table(vec![column("id", "TEXT")]);
        let mut artifact = sql_artifact("touch", "SELECT 2;");
        artifact.requires_review = true;
        new_table.sql_artifacts.push(artifact);
        let new_manifest = manifest(new_table, "v2", "v2-table");

        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        let artifact_change = changes
            .iter()
            .find(|c| c.kind == ChangeKind::ApplySqlArtifact && c.object_name == "touch")
            .expect("sql artifact change emitted");
        assert_eq!(artifact_change.safety, ChangeSafety::RequiresReview);
    }

    #[test]
    fn structured_trigger_and_materialized_view_creation_is_auto_safe() {
        let old_manifest = manifest(table(vec![column("id", "TEXT")]), "v1", "v1-table");
        let mut new_table = table(vec![column("id", "TEXT")]);
        new_table.triggers.push(ManifestTrigger {
            name: "trg_patients_touch".to_string(),
            schema: "public".to_string(),
            table: "patients".to_string(),
            event: "UPDATE".to_string(),
            timing: "BEFORE".to_string(),
            function: "touch_updated_at()".to_string(),
            for_each: "ROW".to_string(),
            ..ManifestTrigger::default()
        });
        new_table.materialized_views.push(ManifestMaterializedView {
            name: "patient_summary".to_string(),
            schema: "public".to_string(),
            query: "SELECT count(*) FROM public.patients".to_string(),
            with_data: false,
        });
        let new_manifest = manifest(new_table, "v2", "v2-table");

        let changes = diff_manifests(Some(&old_manifest), &new_manifest);
        assert!(changes.iter().any(|c| {
            c.kind == ChangeKind::CreateTrigger && c.safety == ChangeSafety::SafeAuto
        }));
        assert!(changes.iter().any(|c| {
            c.kind == ChangeKind::CreateMaterializedView && c.safety == ChangeSafety::SafeAuto
        }));
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
                && change.safety == ChangeSafety::RequiresReview
                && change.object_name == "patients_status_enum"
                && change.column == "paused"
        }));
    }

    #[test]
    fn diff_blocks_enum_value_removal() {
        let mut old_status = column("status", "patients_status_enum");
        old_status.enum_values = vec!["active".to_string(), "paused".to_string()];
        let mut new_status = old_status.clone();
        new_status.enum_values = vec!["active".to_string()];

        let old_manifest = manifest(table(vec![old_status]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_status]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::DropEnum
                && change.safety == ChangeSafety::Blocked
                && change.object_name == "patients_status_enum"
        }));
    }

    #[test]
    fn diff_flags_enum_reorder_as_review() {
        // Same value set, different order: not caught by set-based add/drop, but it
        // changes protobuf field numbers + Postgres enum sort order → review.
        let mut old_status = column("status", "patients_status_enum");
        old_status.enum_values = vec!["active".to_string(), "paused".to_string()];
        let mut new_status = old_status.clone();
        new_status.enum_values = vec!["paused".to_string(), "active".to_string()];

        let old_manifest = manifest(table(vec![old_status]), "old", "old-table");
        let new_manifest = manifest(table(vec![new_status]), "new", "new-table");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        assert!(
            changes.iter().any(|change| {
                change.kind == ChangeKind::ValidationError
                    && change.safety == ChangeSafety::RequiresReview
                    && change.object_name == "patients_status_enum"
            }),
            "enum reorder must be review-required, got: {changes:?}"
        );
        // And it must NOT be misclassified as a safe add/drop.
        assert!(!changes.iter().any(|change| {
            change.kind == ChangeKind::DropEnum || change.kind == ChangeKind::AlterEnumAddValue
        }));
    }

    #[test]
    fn diff_blocks_unsafe_enum_value() {
        let mut status = column("status", "patients_status_enum");
        status.enum_values = vec!["active".to_string(), "bad$value".to_string()];

        let new_manifest = manifest(table(vec![status]), "new", "new-table");
        let changes = diff_manifests(None, &new_manifest);

        assert!(changes.iter().any(|change| {
            change.kind == ChangeKind::CreateEnum
                && change.safety == ChangeSafety::Blocked
                && change
                    .blocked_reason
                    .contains("failed SQL safety validation")
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

    // ── UDB-MIG-001: allow_drop must survive the startup review gate ──────────
    //
    // The reviewed two-phase migration is `allow_drop` → apply → remove
    // `allow_drop`. The startup gate (`src/control/lifecycle.rs`) blocks on any
    // change whose `safety != SafeAuto`, so a still-present `allow_drop` hint must
    // never be `RequiresReview` while it is doing its job.

    fn nonunique_index(name: &str, columns: &[&str]) -> ManifestIndex {
        ManifestIndex {
            name: name.to_string(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
            unique: false,
            ..ManifestIndex::default()
        }
    }

    /// `partner.rate_rules` with the given secondary index; `allow_drop` on the
    /// retained `destination_district_code` column is caller-controlled.
    fn rate_rules_table(index: ManifestIndex, district_allow_drop: bool) -> ManifestTable {
        let mut district = column("destination_district_code", "TEXT");
        district.allow_drop = district_allow_drop;
        ManifestTable {
            schema: "partner".to_string(),
            table: "rate_rules".to_string(),
            columns: vec![
                column("partner_id", "UUID"),
                district,
                column("rate_purpose", "TEXT"),
            ],
            indexes: vec![index],
            ..ManifestTable::default()
        }
    }

    #[test]
    fn allow_drop_consumed_in_same_diff_does_not_block_startup() {
        // v2 → v3 non-unique index swap. `allow_drop` is added to a retained
        // column of the old index so the DropIndex becomes auto-safe.
        let old_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v2",
                &["partner_id", "destination_district_code"],
            ),
            false,
        );
        let new_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v3",
                &["partner_id", "rate_purpose"],
            ),
            true,
        );
        let old_manifest = manifest(old_table, "m-v2", "t-v2");
        let new_manifest = manifest(new_table, "m-v3", "t-v3");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        // The reviewed drop of the old index is auto-safe.
        assert!(
            changes.iter().any(|c| c.kind == ChangeKind::DropIndex
                && c.object_name == "idx_rate_rules_resolution_v2"
                && c.safety == ChangeSafety::SafeAuto),
            "old index drop must be auto-safe under allow_drop"
        );
        // Startup gate: nothing here is `!= SafeAuto`, so the broker starts
        // without force-sync. This is the exact circular gate the bug reported.
        let blocking: Vec<_> = changes
            .iter()
            .filter(|c| c.safety != ChangeSafety::SafeAuto)
            .collect();
        assert!(
            blocking.is_empty(),
            "consumed allow_drop must leave no blocking op, got: {blocking:?}"
        );
        // The consumed hint is recorded for the audit trail and names the EXACT
        // freed index rather than only `partner.rate_rules()`.
        let hint = changes
            .iter()
            .find(|c| c.kind == ChangeKind::HintWarning)
            .expect("consumed allow_drop hint must be recorded");
        assert_eq!(hint.safety, ChangeSafety::SafeAuto);
        assert_eq!(hint.column, "destination_district_code");
        assert_eq!(hint.object_name, "idx_rate_rules_resolution_v2");
        assert!(
            hint.reason.contains("consumed"),
            "hint should record the operation it approved: {}",
            hint.reason
        );
    }

    #[test]
    fn unconsumed_allow_drop_next_run_warns_but_does_not_block() {
        // Post-migration: the v3 index is in place and the operator has not yet
        // removed the `allow_drop` annotation. An unrelated additive change makes
        // the manifest checksum advance, so the diff runs again. No drop consumes
        // the lingering hint this run.
        let old_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v3",
                &["partner_id", "rate_purpose"],
            ),
            true,
        );
        let mut new_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v3",
                &["partner_id", "rate_purpose"],
            ),
            true,
        );
        new_table.columns.push(column("subject_type", "TEXT"));
        let old_manifest = manifest(old_table, "m-v3", "t-v3");
        let new_manifest = manifest(new_table.clone(), "m-v4", "t-v4");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        // Non-blocking: the stale hint never prevents startup.
        let blocking: Vec<_> = changes
            .iter()
            .filter(|c| c.safety != ChangeSafety::SafeAuto)
            .collect();
        assert!(
            blocking.is_empty(),
            "unconsumed allow_drop must not block startup, got: {blocking:?}"
        );
        let hint = changes
            .iter()
            .find(|c| c.kind == ChangeKind::HintWarning)
            .expect("stale allow_drop must surface a cleanup warning");
        assert_eq!(hint.safety, ChangeSafety::SafeAuto);
        assert_eq!(hint.column, "destination_district_code");
        assert!(
            hint.reason.contains("no destructive operation consumed"),
            "stale hint should advise cleanup: {}",
            hint.reason
        );
        assert!(
            hint.blocked_reason.contains("remove the stale allow_drop"),
            "stale hint should tell the operator to remove it: {}",
            hint.blocked_reason
        );

        // Removing the annotation entirely leaves a clean plan (no hint at all).
        let mut clean_new = new_table.clone();
        for col in &mut clean_new.columns {
            col.allow_drop = false;
        }
        let clean_manifest = manifest(clean_new, "m-v5", "t-v5");
        let clean_changes = diff_manifests(Some(&old_manifest), &clean_manifest);
        assert!(
            !clean_changes
                .iter()
                .any(|c| c.kind == ChangeKind::HintWarning),
            "removing allow_drop must leave no hint warning"
        );
    }

    #[test]
    fn nonunique_index_drop_without_allow_drop_still_blocks() {
        // Same v2 → v3 swap but WITHOUT any allow_drop: the destructive index
        // drop must remain `RequiresReview` (fail-closed) and no hint is emitted.
        let old_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v2",
                &["partner_id", "destination_district_code"],
            ),
            false,
        );
        let new_table = rate_rules_table(
            nonunique_index(
                "idx_rate_rules_resolution_v3",
                &["partner_id", "rate_purpose"],
            ),
            false,
        );
        let old_manifest = manifest(old_table, "m-v2", "t-v2");
        let new_manifest = manifest(new_table, "m-v3", "t-v3");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        let drop = changes
            .iter()
            .find(|c| c.kind == ChangeKind::DropIndex)
            .expect("index drop must be present");
        assert_eq!(
            drop.safety,
            ChangeSafety::RequiresReview,
            "unreviewed non-unique index drop must stay blocked"
        );
        assert_eq!(drop.object_name, "idx_rate_rules_resolution_v2");
        assert!(
            !changes.iter().any(|c| c.kind == ChangeKind::HintWarning),
            "no allow_drop annotation means no hint warning"
        );
    }

    #[test]
    fn drop_unique_without_allow_drop_requires_review() {
        // A column-level `unique: true → false` with no annotation must stay
        // RequiresReview (the operator opts in via allow_drop or an approved plan).
        let mut old_col = column("email", "TEXT");
        old_col.unique = true;
        let new_col = column("email", "TEXT"); // unique = false
        let old_manifest = manifest(table(vec![old_col]), "m-old", "t-old");
        let new_manifest = manifest(table(vec![new_col]), "m-new", "t-new");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        let drop = changes
            .iter()
            .find(|c| c.kind == ChangeKind::DropUnique)
            .expect("drop-unique must be present");
        assert_eq!(drop.safety, ChangeSafety::RequiresReview);
        assert!(
            !changes.iter().any(|c| c.kind == ChangeKind::HintWarning),
            "no allow_drop means no consumed/stale hint"
        );
    }

    #[test]
    fn drop_unique_with_allow_drop_is_safe_and_consumes_the_hint() {
        // The report's exact case: replacing a column-level unique with a
        // composite one drops the column unique. An explicit allow_drop on the
        // column is the reviewed opt-in, so the DropUnique is SafeAuto AND the
        // allow_drop is recognized as consumed (no stale-annotation warning).
        let mut old_col = column("name", "TEXT");
        old_col.unique = true;
        old_col.allow_drop = true;
        let mut new_col = column("name", "TEXT"); // unique = false
        new_col.allow_drop = true;
        let old_manifest = manifest(table(vec![old_col]), "m-old", "t-old");
        let new_manifest = manifest(table(vec![new_col]), "m-new", "t-new");
        let changes = diff_manifests(Some(&old_manifest), &new_manifest);

        let drop = changes
            .iter()
            .find(|c| c.kind == ChangeKind::DropUnique)
            .expect("drop-unique must be present");
        assert_eq!(
            drop.safety,
            ChangeSafety::SafeAuto,
            "allow_drop must promote a column DropUnique to SafeAuto"
        );
        let hint = changes
            .iter()
            .find(|c| c.kind == ChangeKind::HintWarning)
            .expect("consumed allow_drop must emit exactly one hint");
        assert_eq!(hint.safety, ChangeSafety::SafeAuto);
        assert!(
            hint.reason.contains("consumed this run by"),
            "hint must record the DropUnique as consuming the allow_drop, got: {}",
            hint.reason
        );
    }
}
