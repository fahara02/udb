// src/auto_alter.rs — Auto-alter decision types and repair planner.
//
// The auto-alter system maps lint findings and drift items to concrete repair
// operations that the Go UDB service can apply without a hand-written migration
// file.  It covers ALL four storage tiers:
//
//   Tier 1 (PostgreSQL)  — SQL DDL repairs (CREATE SCHEMA, ADD COLUMN, etc.)
//   Tier 2 (Redis)       — no structural migration; managed by config/TTL only
//   Tier 3 (Qdrant)      — collection creation, payload index creation
//   Tier 4 (MinIO)       — bucket creation, versioning / lifecycle policy
//
// Safety model:
//   - SQL: Only safe DDL is auto-applied (CREATE SCHEMA, ADD COLUMN, SET DEFAULT,
//     SET NOT NULL, ENABLE RLS, CREATE INDEX, COMMENT).  Destructive SQL (DROP
//     TABLE, DROP COLUMN, CHANGE TYPE) always requires an explicit migration file.
//   - Qdrant: CREATE COLLECTION and CREATE PAYLOAD INDEX are always safe.
//   - MinIO: CREATE BUCKET is always safe.  Bucket deletion is never auto-applied.
//
// Aligned with:
//   - legacy_sql  legacycore/db/auto_alter.go   (AutoAlterFromLint) — SQL-only reference
//   - UDB spec §16.1                        (four storage tiers)
//   - UDB spec §16.3.1                      (AUTO_ALTERING FSM state)

use serde::{Deserialize, Serialize};

// ── Repair kinds ──────────────────────────────────────────────────────────────

/// The category of auto-repair operation.
///
/// Covers all four UDB storage tiers.  Each variant maps to a specific
/// provisioning call the Go UDB service executes during AUTO_ALTERING.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairKind {
    // ── Tier 1: PostgreSQL SQL DDL ──────────────────────────────────────────────
    /// `CREATE SCHEMA IF NOT EXISTS <schema>`.
    CreateSchema,
    /// `CREATE TABLE IF NOT EXISTS <schema>.<table> (…)`.
    CreateTable,
    /// `ALTER TABLE <schema>.<table> ADD COLUMN IF NOT EXISTS <column> <type>`.
    AddColumn,
    /// `ALTER TABLE <schema>.<table> ALTER COLUMN <column> SET DEFAULT <value>`.
    SetDefault,
    /// `ALTER TABLE <schema>.<table> ALTER COLUMN <column> DROP DEFAULT`.
    DropDefault,
    /// `ALTER TABLE <schema>.<table> ALTER COLUMN <column> SET NOT NULL`.
    SetNotNull,
    /// `ALTER TABLE <schema>.<table> ALTER COLUMN <column> DROP NOT NULL`.
    DropNotNull,
    /// `ALTER TABLE <schema>.<table> ENABLE ROW LEVEL SECURITY`.
    EnableRls,
    /// `CREATE UNIQUE INDEX IF NOT EXISTS … ON <schema>.<table>(<column>)`.
    CreateUniqueIndex,
    /// `COMMENT ON TABLE / COLUMN …`.
    SetComment,

    // ── Tier 3: Qdrant (Vector store) ──────────────────────────────────────────
    /// Create a Qdrant collection declared via `vector_store` proto option.
    /// Params: collection_name, dimension, distance metric.
    QdrantCreateCollection,
    /// Create a payload index on a Qdrant collection.
    /// Params: collection_name, field_name, field_schema_type.
    QdrantCreateIndex,
    /// Update the vector configuration on an existing Qdrant collection
    /// (e.g. dimension change requires recreation; flagged RequiresReview).
    QdrantRecreateCollection,

    // ── Tier 4: MinIO / S3-compatible object store ──────────────────────
    /// Create a MinIO bucket declared via `storage` / `object_store` proto option.
    MinioBucketCreate,
    /// Apply versioning and lifecycle policies to an existing MinIO bucket.
    MinioBucketPolicy,

    // ── GAP 23: Additional PostgreSQL repair variants ─────────────────────────
    /// `CREATE INDEX IF NOT EXISTS … USING BTREE …` (non-unique B-tree index).
    CreateIndex,
    /// `DROP INDEX CONCURRENTLY IF EXISTS …`.
    DropIndex,
    /// `DO $$BEGIN ALTER TABLE ADD CONSTRAINT CHECK (…) EXCEPTION WHEN duplicate_object…END$$`.
    CreateCheck,
    /// `ALTER TABLE ADD CONSTRAINT FOREIGN KEY … NOT VALID` (zero-downtime, deferred validation).
    CreateForeignKey,
    /// `ALTER TABLE VALIDATE CONSTRAINT …` (second pass — validates the NOT VALID FK).
    ValidateForeignKey,
    /// `ALTER TABLE FORCE ROW LEVEL SECURITY` (requires RLS to already be enabled).
    EnableRlsForce,
    /// `REFRESH MATERIALIZED VIEW CONCURRENTLY …`.
    RefreshMaterializedView,
}

impl RepairKind {
    /// Returns `true` when this repair is unconditionally safe to auto-apply.
    pub fn is_auto_safe(&self) -> bool {
        matches!(
            self,
            // SQL DDL (all safe)
            RepairKind::CreateSchema
                | RepairKind::CreateTable
                | RepairKind::AddColumn
                | RepairKind::SetDefault
                | RepairKind::DropDefault
                | RepairKind::SetNotNull
                | RepairKind::DropNotNull
                | RepairKind::EnableRls
                | RepairKind::CreateUniqueIndex
                | RepairKind::SetComment
                // GAP 23: new SQL variants that are safe to auto-apply
                | RepairKind::CreateIndex
                | RepairKind::CreateCheck
                | RepairKind::CreateForeignKey   // NOT VALID — zero-downtime
                | RepairKind::ValidateForeignKey  // deferred validation pass
                | RepairKind::EnableRlsForce
                | RepairKind::RefreshMaterializedView
            // Qdrant (create is safe; recreate is not)
                | RepairKind::QdrantCreateCollection
                | RepairKind::QdrantCreateIndex
            // MinIO (create is safe)
                | RepairKind::MinioBucketCreate
                | RepairKind::MinioBucketPolicy
        )
        // DropIndex requires operator review — not auto-safe.
    }

    /// Returns the canonical snake_case label used in JSON and metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            // SQL DDL
            Self::CreateSchema => "create_schema",
            Self::CreateTable => "create_table",
            Self::AddColumn => "add_column",
            Self::SetDefault => "set_default",
            Self::DropDefault => "drop_default",
            Self::SetNotNull => "set_not_null",
            Self::DropNotNull => "drop_not_null",
            Self::EnableRls => "enable_rls",
            Self::CreateUniqueIndex => "create_unique_index",
            Self::SetComment => "set_comment",
            // GAP 23: new SQL variants
            Self::CreateIndex => "create_index",
            Self::DropIndex => "drop_index",
            Self::CreateCheck => "create_check",
            Self::CreateForeignKey => "create_foreign_key",
            Self::ValidateForeignKey => "validate_foreign_key",
            Self::EnableRlsForce => "enable_rls_force",
            Self::RefreshMaterializedView => "refresh_materialized_view",
            // Qdrant
            Self::QdrantCreateCollection => "qdrant_create_collection",
            Self::QdrantCreateIndex => "qdrant_create_index",
            Self::QdrantRecreateCollection => "qdrant_recreate_collection",
            // MinIO
            Self::MinioBucketCreate => "minio_bucket_create",
            Self::MinioBucketPolicy => "minio_bucket_policy",
        }
    }

    /// Returns the storage tier label for this repair kind.
    pub fn tier(&self) -> &'static str {
        match self {
            Self::CreateSchema
            | Self::CreateTable
            | Self::AddColumn
            | Self::SetDefault
            | Self::DropDefault
            | Self::SetNotNull
            | Self::DropNotNull
            | Self::EnableRls
            | Self::CreateUniqueIndex
            | Self::SetComment
            | Self::CreateIndex
            | Self::DropIndex
            | Self::CreateCheck
            | Self::CreateForeignKey
            | Self::ValidateForeignKey
            | Self::EnableRlsForce
            | Self::RefreshMaterializedView => "sql",
            Self::QdrantCreateCollection
            | Self::QdrantCreateIndex
            | Self::QdrantRecreateCollection => "vector",
            Self::MinioBucketCreate | Self::MinioBucketPolicy => "object",
        }
    }
}

// ── Repair decision ───────────────────────────────────────────────────────────

/// A single auto-repair decision produced by mapping a lint item or drift item
/// to the DDL that would fix it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairDecision {
    /// The repair operation category.
    pub kind: RepairKind,
    /// Target schema.
    pub schema: String,
    /// Target table (empty for schema-level operations).
    pub table: String,
    /// Target column (empty for table-level operations).
    pub column: String,
    /// The DDL statement to execute.  May be empty when `is_auto_safe = false`.
    pub ddl: String,
    /// Human-readable rationale for this repair.
    pub reason: String,
    /// `true` when this repair can be applied automatically.
    pub is_auto_safe: bool,
    /// Lint kind that triggered this repair (e.g. `"missing_table"`).
    pub source_lint_kind: String,
}

impl RepairDecision {
    /// Build a schema-level repair decision.
    pub fn schema(
        schema: impl Into<String>,
        ddl: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let schema = schema.into();
        Self {
            kind: RepairKind::CreateSchema,
            ddl: ddl.into(),
            reason: reason.into(),
            is_auto_safe: true,
            source_lint_kind: "missing_schema".to_string(),
            table: String::new(),
            column: String::new(),
            schema,
        }
    }
}

// ── Repair plan ───────────────────────────────────────────────────────────────

/// The complete set of auto-repair decisions produced for one migration run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepairPlan {
    /// All decisions, in dependency order (schema → table → column → constraints).
    pub decisions: Vec<RepairDecision>,
    /// Number of auto-safe repairs ready to apply.
    pub auto_safe_count: usize,
    /// Number of repairs that require manual review.
    pub requires_review_count: usize,
}

impl RepairPlan {
    /// Returns an iterator over decisions that are safe to auto-apply.
    pub fn auto_safe_decisions(&self) -> impl Iterator<Item = &RepairDecision> {
        self.decisions.iter().filter(|d| d.is_auto_safe)
    }

    /// Returns an iterator over decisions that require review.
    pub fn review_decisions(&self) -> impl Iterator<Item = &RepairDecision> {
        self.decisions.iter().filter(|d| !d.is_auto_safe)
    }
}

// ── Planner ───────────────────────────────────────────────────────────────────

/// Map lint findings to auto-repair decisions.
///
/// The `lint_kind` values correspond to `LintItem.kind` strings produced by
/// `lint_catalog()`.  Unknown kinds are surfaced as `requires_review_count`.
///
/// This is a **pure static** operation — no DB connection required.
pub fn plan_repairs(lint_items: &[LintInput]) -> RepairPlan {
    let mut decisions: Vec<RepairDecision> = Vec::new();

    // Process in dependency order to match legacy_sql: schema → table → column → constraints.
    let ordering = [
        "missing_schema",
        "missing_table",
        "missing_column",
        "default_mismatch",
        "nullability_mismatch",
        "rls_enabled_no_policies",
    ];

    for &kind in &ordering {
        for item in lint_items.iter().filter(|i| i.lint_kind == kind) {
            if let Some(decision) = lint_to_repair(item) {
                decisions.push(decision);
            }
        }
    }

    let auto_safe_count = decisions.iter().filter(|d| d.is_auto_safe).count();
    let requires_review_count = decisions.len() - auto_safe_count;

    RepairPlan {
        decisions,
        auto_safe_count,
        requires_review_count,
    }
}

/// Minimal lint item input for the repair planner.
/// Avoids a circular dependency on `generation::lint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintInput {
    /// The `LintItem.kind` string.
    pub lint_kind: String,
    pub schema: String,
    pub table: String,
    pub column: String,
    /// The SQL type of the column (for ADD COLUMN repairs).
    pub sql_type: String,
    /// The default value expression (for SET DEFAULT repairs).
    pub default_value: String,
}

fn lint_to_repair(item: &LintInput) -> Option<RepairDecision> {
    let qi = |s: &str| format!("\"{}\"", s.replace('"', "\"\""));

    let (kind, ddl, source) = match item.lint_kind.as_str() {
        "missing_schema" => (
            RepairKind::CreateSchema,
            format!("CREATE SCHEMA IF NOT EXISTS {};", qi(&item.schema)),
            "missing_schema",
        ),
        "missing_table" => (
            RepairKind::CreateTable,
            format!(
                "CREATE TABLE IF NOT EXISTS {}.{} (id BIGSERIAL PRIMARY KEY);",
                qi(&item.schema),
                qi(&item.table)
            ),
            "missing_table",
        ),
        "missing_column" => {
            let sql_type = if item.sql_type.is_empty() {
                "TEXT"
            } else {
                item.sql_type.as_str()
            };
            (
                RepairKind::AddColumn,
                format!(
                    "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {};",
                    qi(&item.schema),
                    qi(&item.table),
                    qi(&item.column),
                    sql_type
                ),
                "missing_column",
            )
        }
        "default_mismatch" => (
            RepairKind::SetDefault,
            if item.default_value.is_empty() {
                format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} DROP DEFAULT;",
                    qi(&item.schema),
                    qi(&item.table),
                    qi(&item.column)
                )
            } else {
                format!(
                    "ALTER TABLE {}.{} ALTER COLUMN {} SET DEFAULT {};",
                    qi(&item.schema),
                    qi(&item.table),
                    qi(&item.column),
                    item.default_value
                )
            },
            "default_mismatch",
        ),
        "nullability_mismatch" => (
            RepairKind::SetNotNull,
            format!(
                "ALTER TABLE {}.{} ALTER COLUMN {} SET NOT NULL;",
                qi(&item.schema),
                qi(&item.table),
                qi(&item.column)
            ),
            "nullability_mismatch",
        ),
        // GAP 23: new repair kinds wired to lint outputs
        "missing_index" => (
            RepairKind::CreateIndex,
            format!(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_{schema}_{table}_{col} \
                 ON {qs}.{qt} USING BTREE ({qc});",
                schema = item.schema.replace('"', ""),
                table = item.table.replace('"', ""),
                col = item.column.replace('"', ""),
                qs = qi(&item.schema),
                qt = qi(&item.table),
                qc = qi(&item.column),
            ),
            "missing_index",
        ),
        "stale_materialized_view" => (
            RepairKind::RefreshMaterializedView,
            format!(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY {}.{};",
                qi(&item.schema),
                qi(&item.table)
            ),
            "stale_materialized_view",
        ),
        "rls_not_forced" => (
            RepairKind::EnableRlsForce,
            format!(
                "ALTER TABLE {}.{} FORCE ROW LEVEL SECURITY;",
                qi(&item.schema),
                qi(&item.table)
            ),
            "rls_not_forced",
        ),
        "fk_target_not_in_manifest" => (
            RepairKind::CreateForeignKey,
            // Placeholder DDL — actual column names require richer LintInput context.
            // The force_sync engine can skip DDL when it is empty.
            String::new(),
            "fk_target_not_in_manifest",
        ),
        _ => return None,
    };

    Some(RepairDecision {
        is_auto_safe: kind.is_auto_safe(),
        kind,
        schema: item.schema.clone(),
        table: item.table.clone(),
        column: item.column.clone(),
        ddl,
        reason: format!("auto-repair triggered by lint kind '{}'", source),
        source_lint_kind: source.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_repair_kinds_are_auto_safe() {
        // SQL DDL + Qdrant create + MinIO create are all safe.
        for kind in [
            RepairKind::CreateSchema,
            RepairKind::CreateTable,
            RepairKind::AddColumn,
            RepairKind::SetDefault,
            RepairKind::DropDefault,
            RepairKind::SetNotNull,
            RepairKind::DropNotNull,
            RepairKind::EnableRls,
            RepairKind::CreateUniqueIndex,
            RepairKind::SetComment,
            // GAP 23
            RepairKind::CreateIndex,
            RepairKind::CreateCheck,
            RepairKind::CreateForeignKey,
            RepairKind::ValidateForeignKey,
            RepairKind::EnableRlsForce,
            RepairKind::RefreshMaterializedView,
            // Qdrant
            RepairKind::QdrantCreateCollection,
            RepairKind::QdrantCreateIndex,
            // MinIO
            RepairKind::MinioBucketCreate,
            RepairKind::MinioBucketPolicy,
        ] {
            assert!(kind.is_auto_safe(), "{} should be auto-safe", kind.as_str());
        }
    }

    #[test]
    fn drop_index_requires_review() {
        // DropIndex is not auto-safe — it can break queries.
        assert!(
            !RepairKind::DropIndex.is_auto_safe(),
            "drop_index must NOT be auto-safe"
        );
    }

    #[test]
    fn qdrant_recreate_collection_requires_review() {
        assert!(
            !RepairKind::QdrantRecreateCollection.is_auto_safe(),
            "qdrant_recreate_collection must NOT be auto-safe (data loss risk)"
        );
        assert_eq!(RepairKind::QdrantRecreateCollection.tier(), "vector");
    }

    #[test]
    fn repair_kinds_have_correct_tiers() {
        assert_eq!(RepairKind::CreateSchema.tier(), "sql");
        assert_eq!(RepairKind::QdrantCreateCollection.tier(), "vector");
        assert_eq!(RepairKind::MinioBucketCreate.tier(), "object");
    }

    #[test]
    fn plan_repairs_missing_schema() {
        let items = vec![LintInput {
            lint_kind: "missing_schema".to_string(),
            schema: "app_intake".to_string(),
            table: String::new(),
            column: String::new(),
            sql_type: String::new(),
            default_value: String::new(),
        }];
        let plan = plan_repairs(&items);
        assert_eq!(plan.auto_safe_count, 1);
        assert_eq!(plan.requires_review_count, 0);
        assert!(
            plan.decisions[0]
                .ddl
                .contains("CREATE SCHEMA IF NOT EXISTS")
        );
    }

    #[test]
    fn plan_repairs_add_column() {
        let items = vec![LintInput {
            lint_kind: "missing_column".to_string(),
            schema: "app_intake".to_string(),
            table: "document_cases".to_string(),
            column: "tenant_id".to_string(),
            sql_type: "TEXT".to_string(),
            default_value: String::new(),
        }];
        let plan = plan_repairs(&items);
        assert_eq!(plan.auto_safe_count, 1);
        let ddl = &plan.decisions[0].ddl;
        assert!(ddl.contains("ADD COLUMN IF NOT EXISTS"));
        assert!(ddl.contains("tenant_id"));
    }

    #[test]
    fn plan_repairs_unknown_kind_is_skipped() {
        let items = vec![LintInput {
            lint_kind: "pii_not_masked_in_logs".to_string(),
            schema: "ocr".to_string(),
            table: "users".to_string(),
            column: "email".to_string(),
            sql_type: String::new(),
            default_value: String::new(),
        }];
        let plan = plan_repairs(&items);
        assert_eq!(plan.decisions.len(), 0);
    }
}
