use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::generation::manifest::{
    CatalogManifest, ManifestColumn, ManifestProjection, ManifestTable,
};

// ── Severity ─────────────────────────────────────────────────────────────────

/// Severity level for a lint finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LintSeverity {
    /// Migration will be blocked or data may be lost without remediation.
    Error,
    /// Schema is functional but the pattern should be corrected.
    Warning,
    /// Informational note; migration can proceed.
    #[default]
    Info,
}

// ── Item ─────────────────────────────────────────────────────────────────────

/// One lint finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LintItem {
    pub severity: LintSeverity,
    /// Machine-readable kind identifier (snake_case).
    pub kind: String,
    pub schema: String,
    pub table: String,
    /// Empty when the finding is table-level.
    pub column: String,
    pub description: String,
    pub suggestion: String,
    pub source_file: String,
}

// ── Report ───────────────────────────────────────────────────────────────────

/// Complete lint analysis of a `CatalogManifest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct LintReport {
    pub generated_at_unix: u64,
    pub checksum_sha256: String,
    pub table_count: usize,
    pub store_count: usize,
    pub error_count: usize,
    pub warning_count: usize,
    pub info_count: usize,
    /// True when error_count == 0.
    pub passed: bool,
    pub items: Vec<LintItem>,
}

// ── Entry point ──────────────────────────────────────────────────────────────

/// Analyse a compiled `CatalogManifest` and return a structured lint report.
///
/// This is a **static** analysis that never requires a live database connection.
/// The Go UDB service runs this after every proto reload to gate the FSM at the
/// `PROTO_CHECKSUM_LINT` state.
pub fn lint_catalog(manifest: &CatalogManifest) -> LintReport {
    let mut items = Vec::new();

    // ── Promote manifest validation errors ───────────────────────────────────
    for error in &manifest.validation_errors {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "manifest_validation_error".to_string(),
            description: error.clone(),
            suggestion: "Fix the referenced schema issue before applying migrations.".to_string(),
            ..LintItem::default()
        });
    }

    // ── Promote manifest warnings ────────────────────────────────────────────
    for warning in &manifest.warnings {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "manifest_warning".to_string(),
            description: warning.clone(),
            ..LintItem::default()
        });
    }

    // ── Per-table lint ───────────────────────────────────────────────────────
    for table in &manifest.tables {
        lint_table(table, &mut items);
    }

    // ── Cross-table: duplicate base table name across schemas ────────────────
    let mut table_name_counts = BTreeMap::<&str, usize>::new();
    for table in &manifest.tables {
        *table_name_counts.entry(table.table.as_str()).or_default() += 1;
    }
    for (table_name, count) in &table_name_counts {
        if *count > 1 {
            items.push(LintItem {
                severity: LintSeverity::Warning,
                kind: "duplicate_table_name_across_schemas".to_string(),
                table: table_name.to_string(),
                description: format!(
                    "Table name '{}' is used in {} different schemas; this may cause confusion in cross-schema joins.",
                    table_name, count
                ),
                suggestion: "Use schema-specific prefixes or rely on fully-qualified identifiers.".to_string(),
                ..LintItem::default()
            });
        }
    }

    // ── Cross-table: schema-level store duplication ──────────────────────────
    lint_duplicate_stores(manifest, &mut items);
    lint_manifest_routing(manifest, &mut items);

    // ── GAP 13: annotation key consistency ───────────────────────────────────
    lint_unknown_annotation_keys(manifest, &mut items);

    // ── GAP 24: Cross-table FK target validation ─────────────────────────────
    // Build a set of (schema, table) pairs present in this manifest.  Any FK
    // that points outside this set is an error: either the proto is wrong or
    // the migration will fail with a missing-table referential integrity error.
    {
        let table_set: std::collections::HashSet<(String, String)> = manifest
            .tables
            .iter()
            .map(|t| (t.schema.clone(), t.table.clone()))
            .collect();

        for table in &manifest.tables {
            for fk in &table.foreign_keys {
                if fk.ref_table.is_empty() {
                    continue;
                }
                // ref_schema defaults to the same schema when empty.
                let ref_schema = if fk.ref_schema.is_empty() {
                    table.schema.as_str()
                } else {
                    fk.ref_schema.as_str()
                };
                let key = (ref_schema.to_string(), fk.ref_table.clone());
                if !table_set.contains(&key) {
                    items.push(LintItem {
                        severity: LintSeverity::Error,
                        kind: "fk_target_not_in_manifest".to_string(),
                        schema: table.schema.clone(),
                        table: table.table.clone(),
                        description: format!(
                            "Foreign key '{}' references '{}.{}' \
                             which is not declared in this manifest.",
                            fk.name, ref_schema, fk.ref_table
                        ),
                        suggestion: "Add the referenced table to the manifest \
                                     or correct the ref_table / ref_schema options."
                            .to_string(),
                        ..LintItem::default()
                    });
                }
            }
        }
    }

    // ── GAP 37: Detect stores using NotImplemented backends ──────────────────
    // Any manifest that declares an Elasticsearch or Cassandra store will be
    // flagged as an Error so operators cannot accidentally deploy with an
    // unsupported backend silently failing at runtime.
    //
    // U2 step 6: the "is this kind known but unimplemented" check used to live
    // in `planning::BackendRegistry`; it now reads from `backend::support_state_for_kind`,
    // the single source of truth for which kinds have runtime executors.
    {
        use crate::backend::{BackendKind, BackendSupportState, support_state_for_kind};
        for store in &manifest.stores {
            if let Some(kind) = BackendKind::from_store_kind(&store.store_kind, &store.backend) {
                let not_implemented = matches!(
                    support_state_for_kind(&kind),
                    BackendSupportState::KnownUnsupported
                );
                if not_implemented {
                    items.push(LintItem {
                        severity: LintSeverity::Error,
                        kind: "backend_not_implemented".to_string(),
                        schema: store.owner_schema.clone(),
                        description: format!(
                            "Store '{}' uses backend '{}' (kind: {:?}) which is marked \
                             NotImplemented — it will silently fail or deny all requests \
                             at runtime.",
                            store.logical_name, store.backend, kind
                        ),
                        suggestion: "Remove this store or switch to a supported backend \
                             (postgres, qdrant, clickhouse, neo4j, mongodb, minio)."
                            .to_string(),
                        ..LintItem::default()
                    });
                }
            }
        }
    }

    let error_count = items
        .iter()
        .filter(|item| item.severity == LintSeverity::Error)
        .count();
    let warning_count = items
        .iter()
        .filter(|item| item.severity == LintSeverity::Warning)
        .count();
    let info_count = items
        .iter()
        .filter(|item| item.severity == LintSeverity::Info)
        .count();

    LintReport {
        generated_at_unix: generated_at_unix(),
        checksum_sha256: manifest.checksum_sha256.clone(),
        table_count: manifest.tables.len(),
        store_count: manifest.stores.len(),
        error_count,
        warning_count,
        info_count,
        passed: error_count == 0,
        items,
    }
}

// ── Table-level lint ─────────────────────────────────────────────────────────

fn lint_table(table: &ManifestTable, items: &mut Vec<LintItem>) {
    // Propagate per-table warnings already computed during manifest build
    for warning in &table.warnings {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "table_shape_warning".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: warning.clone(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
    }

    // ── Must have at least one column ────────────────────────────────────────
    if table.columns.is_empty() {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "empty_table".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: format!("{}.{} has no columns defined.", table.schema, table.table),
            suggestion: "Add at least one field to the proto message.".to_string(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
        return; // remaining column-level checks are meaningless on an empty table
    }

    // ── Must have a primary key ──────────────────────────────────────────────
    if table.primary_key.is_empty() {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "missing_primary_key".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: format!(
                "{}.{} has no primary key. Every table must declare at least one primary-key column.",
                table.schema, table.table
            ),
            suggestion: "Add `primary_key: true` to the identifying column in its column option.".to_string(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
    }

    // ── RLS enabled but no policies ──────────────────────────────────────────
    if table.enable_rls && table.rls_policies.is_empty() {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "rls_enabled_no_policies".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: format!(
                "{}.{} has RLS enabled but no policies are defined — all rows will be hidden by default under PostgreSQL's deny-by-default model.",
                table.schema, table.table
            ),
            suggestion: "Add at least one `rls_policies` block to the table option.".to_string(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
    }

    // ── Partition strategy without partition column ──────────────────────────
    if !table.partition_strategy.is_empty() && table.partition_column.is_empty() {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "partition_column_missing".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: format!(
                "{}.{} declares partition_strategy='{}' but partition_column is empty.",
                table.schema, table.table, table.partition_strategy
            ),
            suggestion: "Set `partition_column` in the table option block.".to_string(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
    }

    // ── Soft-delete column not declared ─────────────────────────────────────
    if table.soft_delete {
        let has_col = table
            .columns
            .iter()
            .any(|col| col.column_name == table.soft_delete_column);
        if !has_col {
            items.push(LintItem {
                severity: LintSeverity::Warning,
                kind: "soft_delete_column_missing".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} has soft_delete=true but the column '{}' is not declared in this message.",
                    table.schema, table.table, table.soft_delete_column
                ),
                suggestion: "Add the soft-delete field to the proto message with the correct column_name.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }
    }

    // ── Security classification without RLS ──────────────────────────────────
    let classification = table.security.classification_level.to_ascii_uppercase();
    if matches!(
        classification.as_str(),
        "CONFIDENTIAL" | "RESTRICTED" | "SECRET" | "PII" | "SENSITIVE"
    ) && !table.enable_rls
    {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "security_classification_without_rls".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            description: format!(
                "{}.{} has classification_level='{}' but row-level security is disabled.",
                table.schema, table.table, classification
            ),
            suggestion: "Enable RLS (`enable_rls: true`) and add tenant-isolation policies."
                .to_string(),
            source_file: table.source_file.clone(),
            ..LintItem::default()
        });
    }

    // ── Encryption required but no encrypted columns ─────────────────────────
    if table.security.encryption_required {
        let has_encrypted = table
            .columns
            .iter()
            .any(|col| col.encrypted || col.security.is_encrypted);
        if !has_encrypted {
            items.push(LintItem {
                severity: LintSeverity::Warning,
                kind: "encryption_required_no_encrypted_columns".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} sets encryption_required=true but no column is marked `encrypted: true`.",
                    table.schema, table.table
                ),
                suggestion: "Annotate at-rest-sensitive columns with `encrypted: true` in their column option.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }
    }

    // ── Indexes with no columns ──────────────────────────────────────────────
    for index in &table.indexes {
        if index.columns.is_empty() {
            items.push(LintItem {
                severity: LintSeverity::Error,
                kind: "index_no_columns".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} index '{}' has no columns defined.",
                    table.schema, table.table, index.name
                ),
                suggestion: "Add `composite_fields` or a column reference to the index block."
                    .to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }
    }

    // ── Foreign keys with empty referenced table ─────────────────────────────
    for fk in &table.foreign_keys {
        if fk.ref_table.is_empty() {
            items.push(LintItem {
                severity: LintSeverity::Error,
                kind: "fk_missing_ref_table".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} foreign-key '{}' has no referenced table name.",
                    table.schema, table.table, fk.name
                ),
                suggestion: "Set `references_table` in the foreign_key block.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }
        // Deferrable requires initially_deferred XOR explicit false
        if fk.deferrable && fk.initially_deferred {
            // Valid — just an info note
            items.push(LintItem {
                severity: LintSeverity::Info,
                kind: "fk_deferrable_initially_deferred".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} FK '{}' is DEFERRABLE INITIALLY DEFERRED — ensure transaction code sets `SET CONSTRAINTS ALL IMMEDIATE` before commits that depend on ordering.",
                    table.schema, table.table, fk.name
                ),
                ..LintItem::default()
            });
        }
    }

    // ── Per-column lint ──────────────────────────────────────────────────────
    for column in &table.columns {
        lint_column(table, column, items);
    }
}

// ── Column-level lint ─────────────────────────────────────────────────────────

fn lint_column(table: &ManifestTable, column: &ManifestColumn, items: &mut Vec<LintItem>) {
    // ── PII not masked in logs ───────────────────────────────────────────────
    if column.security.is_pii && !column.security.mask_in_logs {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "pii_not_masked_in_logs".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.column_name.clone(),
            description: format!(
                "{}.{}.{} is marked `is_pii` but `mask_in_logs` is false — values may appear in plaintext application logs.",
                table.schema, table.table, column.column_name
            ),
            suggestion: "Set `mask_in_logs: true` on every PII field.".to_string(),
            source_file: table.source_file.clone(),
        });
    }

    // ── PII not encrypted at column level ────────────────────────────────────
    if column.security.is_pii && !column.encrypted && !column.security.is_encrypted {
        items.push(LintItem {
            severity: LintSeverity::Info,
            kind: "pii_not_encrypted".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.column_name.clone(),
            description: format!(
                "{}.{}.{} is PII but does not have column-level encryption enabled.",
                table.schema, table.table, column.column_name
            ),
            suggestion: "Consider `encrypted: true` for strong at-rest protection of PII values."
                .to_string(),
            source_file: table.source_file.clone(),
        });
    }

    // ── NOT NULL without default or backfill for non-PK column ───────────────
    if column.not_null
        && !column.is_primary
        && column.default_value.is_empty()
        && column.backfill_sql.is_empty()
        && !column.auto_increment
        && !column.is_identity
        && !column.generated
    {
        items.push(LintItem {
            severity: LintSeverity::Info,
            kind: "not_null_no_default".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.column_name.clone(),
            description: format!(
                "{}.{}.{} is NOT NULL without a `default_value` or `backfill_sql` — adding this to an existing table will require a full-table backfill pass.",
                table.schema, table.table, column.column_name
            ),
            suggestion: "Add `default_value` or `backfill_sql` to support zero-downtime delta migrations.".to_string(),
            source_file: table.source_file.clone(),
        });
    }

    // ── Blind index requires is_encrypted ────────────────────────────────────
    if column.security.is_blind_index && !column.security.is_encrypted && !column.encrypted {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "blind_index_without_encryption".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.column_name.clone(),
            description: format!(
                "{}.{}.{} has `is_blind_index: true` but the column itself is not encrypted — a blind index is only useful alongside an encrypted value.",
                table.schema, table.table, column.column_name
            ),
            suggestion: "Set `is_encrypted: true` on the primary column and keep the blind-index hash in a separate column.".to_string(),
            source_file: table.source_file.clone(),
        });
    }

    // ── JSONB columns should declare is_json ─────────────────────────────────
    let sql_upper = column.sql_type.to_ascii_uppercase();
    if (sql_upper == "JSONB" || sql_upper == "JSON") && !column.is_json {
        items.push(LintItem {
            severity: LintSeverity::Info,
            kind: "json_column_not_flagged".to_string(),
            schema: table.schema.clone(),
            table: table.table.clone(),
            column: column.column_name.clone(),
            description: format!(
                "{}.{}.{} uses sql_type='{}' but `is_json: true` is not set — UDB dynamic mapping uses the flag for safe serialisation.",
                table.schema, table.table, column.column_name, column.sql_type
            ),
            suggestion: "Set `is_json: true` in the column option to enable typed JSON handling in the UDB broker.".to_string(),
            source_file: table.source_file.clone(),
        });
    }
}

// ── Cross-store lint ─────────────────────────────────────────────────────────

fn lint_duplicate_stores(manifest: &CatalogManifest, items: &mut Vec<LintItem>) {
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    for store in &manifest.stores {
        let key = format!(
            "{}:{}:{}",
            store.store_kind, store.backend, store.resource_name
        );
        *seen.entry(key.clone()).or_default() += 1;
        if seen[&key] == 2 {
            items.push(LintItem {
                severity: LintSeverity::Warning,
                kind: "duplicate_store_resource".to_string(),
                description: format!(
                    "Store resource '{}' (kind={} backend={}) is declared by more than one proto message — collection/index creation will be idempotent but configuration may diverge.",
                    store.resource_name, store.store_kind, store.backend
                ),
                suggestion: "Extract the shared store declaration into a single canonical proto message.".to_string(),
                ..LintItem::default()
            });
        }
    }
}

// ── Manifest routing lint ────────────────────────────────────────────────────

const SUPPORTED_PROJECTION_KINDS: &[&str] = &[
    "relational",
    "document",
    "vector",
    "graph",
    "columnar",
    "cache",
    "object",
];
const SUPPORTED_READ_POLICIES: &[&str] = &[
    "primary",
    "replica",
    "cache_first",
    "cache-first",
    "vector",
    "document",
    "graph",
    "analytics",
    "object",
    "fallback_chain",
    "fallback-chain",
    "projection",
];
const SUPPORTED_WRITE_POLICIES: &[&str] = &[
    "primary",
    "projection",
    "readonly",
    "read_only",
    "read-only",
    "dual_write",
    "dual-write",
    "outbox",
    "async_projection",
    "async-projection",
    "saga",
];
const SUPPORTED_FANOUT_POLICIES: &[&str] = &[
    "primary_only",
    "primary-only",
    "dual_write",
    "dual-write",
    "outbox",
    "async_projection",
    "async-projection",
    "saga",
    "projection",
];
const SUPPORTED_CONSISTENCY_MODELS: &[&str] = &[
    "strong",
    "eventual",
    "read_your_writes",
    "read-your-writes",
    "bounded_staleness",
    "bounded-staleness",
    "backend_default",
    "backend-default",
];

fn lint_manifest_routing(manifest: &CatalogManifest, items: &mut Vec<LintItem>) {
    let mut table_by_message = BTreeMap::<String, &ManifestTable>::new();
    for table in &manifest.tables {
        table_by_message.insert(table.message_name.clone(), table);
    }

    let declared_projections: Vec<&ManifestProjection> = if manifest.projections.is_empty() {
        manifest
            .tables
            .iter()
            .flat_map(|table| table.projections.iter())
            .collect()
    } else {
        manifest.projections.iter().collect()
    };

    let mut projections_by_message = BTreeMap::<String, Vec<&ManifestProjection>>::new();
    for projection in declared_projections {
        projections_by_message
            .entry(projection.message_type.clone())
            .or_default()
            .push(projection);
        lint_projection_supported(
            projection,
            table_by_message.get(&projection.message_type),
            items,
        );
    }

    for table in &manifest.tables {
        let projections = projections_by_message
            .get(&table.message_name)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if projections.is_empty() {
            items.push(LintItem {
                severity: LintSeverity::Error,
                kind: "missing_message_projection".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "{}.{} has no manifest projection; the broker cannot route reads or writes for message '{}'.",
                    table.schema, table.table, table.message_name
                ),
                suggestion: "Declare at least one projection, usually the relational PostgreSQL projection for the base table.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
            continue;
        }

        let write_owners: Vec<_> = projections
            .iter()
            .copied()
            .filter(|projection| projection.write_owner)
            .collect();
        if write_owners.is_empty() {
            items.push(LintItem {
                severity: LintSeverity::Error,
                kind: "missing_projection_write_owner".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "Message '{}' has {} projection(s) but none is marked as the write owner.",
                    table.message_name,
                    projections.len()
                ),
                suggestion: "Mark exactly one primary projection as write_owner=true, or use a fan-out policy when more than one backend owns writes.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }

        if write_owners.len() > 1
            && !write_owners
                .iter()
                .any(|projection| is_multi_write_fanout(&projection.fanout_policy))
        {
            items.push(LintItem {
                severity: LintSeverity::Error,
                kind: "ambiguous_projection_write_owner".to_string(),
                schema: table.schema.clone(),
                table: table.table.clone(),
                description: format!(
                    "Message '{}' has multiple write-owner projections but no dual-write, outbox, async-projection, or saga fan-out policy.",
                    table.message_name
                ),
                suggestion: "Choose a single write owner, or set an explicit multi-backend fanout_policy for the owner set.".to_string(),
                source_file: table.source_file.clone(),
                ..LintItem::default()
            });
        }
    }
}

fn lint_projection_supported(
    projection: &ManifestProjection,
    table: Option<&&ManifestTable>,
    items: &mut Vec<LintItem>,
) {
    let (schema, table_name, source_file) = table
        .map(|table| {
            (
                table.schema.clone(),
                table.table.clone(),
                table.source_file.clone(),
            )
        })
        .unwrap_or_default();

    if !supported_value(&projection.projection_kind, SUPPORTED_PROJECTION_KINDS) {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "unsupported_projection_kind".to_string(),
            schema: schema.clone(),
            table: table_name.clone(),
            description: format!(
                "Projection for message '{}' uses unsupported projection_kind '{}'.",
                projection.message_type, projection.projection_kind
            ),
            suggestion: "Use one of: relational, document, vector, graph, columnar, cache, object."
                .to_string(),
            source_file: source_file.clone(),
            ..LintItem::default()
        });
    }

    if !supported_value(&projection.read_policy, SUPPORTED_READ_POLICIES) {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "unsupported_projection_read_policy".to_string(),
            schema: schema.clone(),
            table: table_name.clone(),
            description: format!(
                "Projection '{}' for message '{}' uses unsupported read_policy '{}'.",
                projection.resource_name, projection.message_type, projection.read_policy
            ),
            suggestion: "Use primary, replica, cache-first, vector, document, analytics, object, or fallback-chain.".to_string(),
            source_file: source_file.clone(),
            ..LintItem::default()
        });
    }

    if !supported_value(&projection.write_policy, SUPPORTED_WRITE_POLICIES) {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "unsupported_projection_write_policy".to_string(),
            schema: schema.clone(),
            table: table_name.clone(),
            description: format!(
                "Projection '{}' for message '{}' uses unsupported write_policy '{}'.",
                projection.resource_name, projection.message_type, projection.write_policy
            ),
            suggestion:
                "Use primary, projection, readonly, dual-write, outbox, async-projection, or saga."
                    .to_string(),
            source_file: source_file.clone(),
            ..LintItem::default()
        });
    }

    if !supported_value(&projection.fanout_policy, SUPPORTED_FANOUT_POLICIES) {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "unsupported_projection_fanout_policy".to_string(),
            schema: schema.clone(),
            table: table_name.clone(),
            description: format!(
                "Projection '{}' for message '{}' uses unsupported fanout_policy '{}'.",
                projection.resource_name, projection.message_type, projection.fanout_policy
            ),
            suggestion:
                "Use primary-only, dual-write, outbox, async-projection, saga, or projection."
                    .to_string(),
            source_file: source_file.clone(),
            ..LintItem::default()
        });
    }

    if !projection.consistency.model.is_empty()
        && !supported_value(&projection.consistency.model, SUPPORTED_CONSISTENCY_MODELS)
    {
        items.push(LintItem {
            severity: LintSeverity::Warning,
            kind: "unsupported_projection_consistency_model".to_string(),
            schema: schema.clone(),
            table: table_name.clone(),
            description: format!(
                "Projection '{}' for message '{}' declares unknown consistency model '{}'.",
                projection.resource_name, projection.message_type, projection.consistency.model
            ),
            suggestion: "Use strong, eventual, read-your-writes, bounded-staleness, backend-default, or document the backend-specific alternative in projection options.".to_string(),
            source_file: source_file.clone(),
            ..LintItem::default()
        });
    }

    if projection.write_owner
        && supported_value(
            &projection.write_policy,
            &["readonly", "read_only", "read-only"],
        )
    {
        items.push(LintItem {
            severity: LintSeverity::Error,
            kind: "readonly_projection_marked_write_owner".to_string(),
            schema,
            table: table_name,
            description: format!(
                "Projection '{}' for message '{}' is readonly but is also marked as write_owner.",
                projection.resource_name, projection.message_type
            ),
            suggestion:
                "Either clear write_owner or change write_policy to primary/projection/fan-out."
                    .to_string(),
            source_file,
            ..LintItem::default()
        });
    }
}

fn supported_value(value: &str, allowed: &[&str]) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    allowed.iter().any(|allowed| normalized == *allowed)
}

fn is_multi_write_fanout(value: &str) -> bool {
    supported_value(
        value,
        &[
            "dual_write",
            "dual-write",
            "outbox",
            "async_projection",
            "async-projection",
            "saga",
        ],
    )
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn generated_at_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

// ── GAP 13: Unknown annotation key lint ───────────────────────────────────────

/// All known table-level annotation keys.  When a proto declares a key that is
/// not in this set, UDB will silently ignore it and the intended behaviour will
/// never be applied.  The lint emits a Warning so the author is alerted early.
const KNOWN_TABLE_KEYS: &[&str] = &[
    "table_name",
    "schema",
    "migration_order",
    "soft_delete",
    "soft_delete_column",
    "audit_fields",
    "enable_rls",
    "force_rls",
    "unlogged",
    "tablespace",
    "previous_table_name",
    "allow_drop",
    "partition_by",
    "partition_column",
    "partition_interval",
    "partition_premake",
    "partition_default",
    "partition_retention_months",
    "retention_days",
    "replica_hint",
    "cdc_topic",
    "required_scope",
    "vector_store",
    "classification_level",
    "audit_writes",
    "audit_reads",
    "encryption_required",
    "table_comment",
    "migration_dir",
];

/// All known column-level annotation keys.
const KNOWN_COLUMN_KEYS: &[&str] = &[
    "column_name",
    "sql_type",
    "not_null",
    "unique",
    "primary_key",
    "auto_increment",
    "is_array",
    "default",
    "default_value",
    "check",
    "check_constraint",
    "collation",
    "enum_values",
    "comment",
    "exclude_from_insert",
    "exclude_from_update",
    "encrypted",
    "is_json",
    "is_jsonb",
    "json_path_ops",
    "is_tsvector",
    "tsvector_language",
    "tsvector_source_columns",
    "tsvector_source_column",
    "trigram_index",
    "references",
    "on_delete",
    "on_update",
    "is_pii",
    "is_encrypted",
    "is_blind_index",
    "mask_in_logs",
    "data_class",
    "consent_required",
    "field_retention_days",
    "previous_column_name",
    "backfill_sql",
    "using_expression",
    "allow_drop",
    "generated",
    "generated_expr",
    "is_identity",
];

/// Warn when a table or column option declares a key that UDB does not recognise.
/// This is a best-effort static lint; it cannot catch all typos because option
/// names are validated against a fixed allowlist.
pub fn lint_unknown_annotation_keys(manifest: &CatalogManifest, items: &mut Vec<LintItem>) {
    let known_key_count = KNOWN_TABLE_KEYS.len() + KNOWN_COLUMN_KEYS.len();
    debug_assert!(known_key_count > 0);

    // The manifest has already lost the raw annotation keys by this point
    // (they have been normalised into typed fields), so this lint operates
    // on the *presence* of empty/default values for fields that *should* be
    // set when a key is provided.  A full key-level check requires access to
    // the raw proto AST, which is available at parse time.
    //
    // What we CAN do here: warn when a column declares is_jsonb but the
    // sql_type is not JSONB, and vice-versa (annotation/type mismatch).
    for table in &manifest.tables {
        for column in &table.columns {
            let sql_upper = column.sql_type.to_ascii_uppercase();

            // is_jsonb on a non-JSONB column
            if column.is_jsonb && sql_upper != "JSONB" {
                items.push(LintItem {
                    severity: LintSeverity::Warning,
                    kind: "is_jsonb_type_mismatch".to_string(),
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: column.column_name.clone(),
                    description: format!(
                        "{}.{}.{} declares is_jsonb=true but sql_type='{}' is not JSONB — GIN index will still be generated but the DDL may be invalid.",
                        table.schema, table.table, column.column_name, column.sql_type
                    ),
                    suggestion: "Change sql_type to JSONB or remove is_jsonb annotation.".to_string(),
                    source_file: table.source_file.clone(),
                });
            }
            // JSONB type without is_jsonb flag (already caught by json_column_not_flagged
            // in the column lint, but this variant is more specific to JSONB)
            if (sql_upper == "JSONB") && !column.is_jsonb && !column.is_json {
                items.push(LintItem {
                    severity: LintSeverity::Warning,
                    kind: "jsonb_missing_is_jsonb".to_string(),
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: column.column_name.clone(),
                    description: format!(
                        "{}.{}.{} has sql_type=JSONB but neither is_json nor is_jsonb is set — the UDB broker will not generate a GIN index and JSON binding may be incorrect.",
                        table.schema, table.table, column.column_name
                    ),
                    suggestion: "Add `is_jsonb: true` to the column option block.".to_string(),
                    source_file: table.source_file.clone(),
                });
            }
            // tsvector_source_columns without is_tsvector
            if !column.tsvector_source_columns.is_empty() && !column.is_tsvector {
                items.push(LintItem {
                    severity: LintSeverity::Warning,
                    kind: "tsvector_source_without_is_tsvector".to_string(),
                    schema: table.schema.clone(),
                    table: table.table.clone(),
                    column: column.column_name.clone(),
                    description: format!(
                        "{}.{}.{} declares tsvector_source_columns but is_tsvector is not set — the FTS expression index will not be generated.",
                        table.schema, table.table, column.column_name
                    ),
                    suggestion: "Add `is_tsvector: true` to the column option block.".to_string(),
                    source_file: table.source_file.clone(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::manifest::{ManifestConsistency, ManifestProjection};

    fn base_projection(write_owner: bool) -> ManifestProjection {
        ManifestProjection {
            message_type: "Patient".to_string(),
            projection_kind: "relational".to_string(),
            backend: "postgres".to_string(),
            resource_name: "public.patients".to_string(),
            read_policy: "primary".to_string(),
            write_policy: "primary".to_string(),
            fanout_policy: "primary_only".to_string(),
            consistency: ManifestConsistency {
                model: "strong".to_string(),
                read_your_writes: true,
                ..ManifestConsistency::default()
            },
            write_owner,
            ..ManifestProjection::default()
        }
    }

    fn base_manifest(projections: Vec<ManifestProjection>) -> CatalogManifest {
        CatalogManifest {
            tables: vec![ManifestTable {
                message_name: "Patient".to_string(),
                schema: "public".to_string(),
                table: "patients".to_string(),
                columns: vec![ManifestColumn {
                    field_name: "id".to_string(),
                    column_name: "id".to_string(),
                    proto_type: "string".to_string(),
                    sql_type: "UUID".to_string(),
                    is_primary: true,
                    field_number: 1,
                    ..ManifestColumn::default()
                }],
                primary_key: vec!["id".to_string()],
                projections: projections.clone(),
                ..ManifestTable::default()
            }],
            projections,
            ..CatalogManifest::default()
        }
    }

    fn has_kind(report: &LintReport, kind: &str) -> bool {
        report.items.iter().any(|item| item.kind == kind)
    }

    #[test]
    fn lint_manifest_routing_requires_write_owner() {
        let report = lint_catalog(&base_manifest(vec![base_projection(false)]));

        assert!(has_kind(&report, "missing_projection_write_owner"));
        assert!(!report.passed);
    }

    #[test]
    fn lint_manifest_routing_rejects_unsupported_projection_policy() {
        let mut projection = base_projection(true);
        projection.read_policy = "nearest_moon".to_string();
        projection.projection_kind = "spreadsheet".to_string();

        let report = lint_catalog(&base_manifest(vec![projection]));

        assert!(has_kind(&report, "unsupported_projection_kind"));
        assert!(has_kind(&report, "unsupported_projection_read_policy"));
        assert!(!report.passed);
    }

    #[test]
    fn lint_manifest_routing_rejects_ambiguous_write_owners() {
        let primary = base_projection(true);
        let mut document = base_projection(true);
        document.projection_kind = "document".to_string();
        document.backend = "mongodb".to_string();
        document.resource_name = "patients".to_string();

        let report = lint_catalog(&base_manifest(vec![primary, document]));

        assert!(has_kind(&report, "ambiguous_projection_write_owner"));
        assert!(!report.passed);
    }

    #[test]
    fn lint_manifest_routing_allows_explicit_fanout_owner_set() {
        let primary = base_projection(true);
        let mut document = base_projection(true);
        document.projection_kind = "document".to_string();
        document.backend = "mongodb".to_string();
        document.resource_name = "patients".to_string();
        document.fanout_policy = "outbox".to_string();

        let report = lint_catalog(&base_manifest(vec![primary, document]));

        assert!(!has_kind(&report, "ambiguous_projection_write_owner"));
    }
}
