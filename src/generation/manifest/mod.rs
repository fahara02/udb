#![allow(clippy::items_after_test_module)]

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::ast::{
    DEFAULT_MIGRATION_ORDER, GenericStore, ProtoColumn, ProtoForeignKey, ProtoIndex, ProtoSchema,
};

pub const GENERATOR_VERSION: &str = "3";

/// First field number of the auto-appended audit block (`created_at`,
/// `updated_at`, `created_by`).
///
/// These columns are synthesized by the generator and appear in NO `.proto`, so
/// their field number is a manifest-internal artifact and never reaches the
/// wire. They used to be numbered `max(explicit) + 1`, which meant appending an
/// explicit field to a message with `audit_fields: true` slid the whole block
/// upward — and the new explicit fields inherited the numbers a deployed
/// manifest had recorded for the audit trio. The drift gate then reported
/// field-number reuse against fields the schema author never numbered, and
/// blocked the upgrade unconditionally (a startup crash-loop, not a skipped
/// table).
///
/// Anchoring the block well above hand-written fields makes it stable: explicit
/// fields can be appended forever without moving it. 9000 sits far above real
/// UDB messages and below protobuf's reserved 19000-19999 range.
pub const AUDIT_FIELD_NUMBER_BASE: i32 = 9000;

/// Column names of the auto-appended audit block, in emission order.
pub const AUDIT_FIELD_COLUMNS: [&str; 3] = ["created_at", "updated_at", "created_by"];
pub const POLICY_PRIMARY: &str = "primary";
pub const POLICY_REPLICA: &str = "replica";
pub const POLICY_PRIMARY_ONLY: &str = "primary_only";
pub const POLICY_ASYNC_PROJECTION: &str = "async_projection";
pub const POLICY_PROJECTION: &str = "projection";
pub const POLICY_CACHE_FIRST: &str = "cache_first";
pub const POLICY_STRONG: &str = "strong";
pub const POLICY_EVENTUAL: &str = "eventual";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CatalogManifest {
    pub checksum_sha256: String,
    pub generator_version: String,
    pub schema_order: Vec<String>,
    pub schema_checksums: Vec<ManifestSchemaChecksum>,
    pub tables: Vec<ManifestTable>,
    pub stores: Vec<ManifestStore>,
    pub projections: Vec<ManifestProjection>,
    pub validation_errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestMigrationReport {
    pub from_version: String,
    pub to_version: String,
    pub migrated: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestSchemaChecksum {
    pub schema: String,
    pub checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestTable {
    pub checksum_sha256: String,
    pub message_name: String,
    pub proto_package: String,
    pub php_namespace: String,
    pub php_class_prefix: String,
    pub php_class: String,
    pub php_metadata_namespace: String,
    pub php_metadata_class: String,
    /// NW-universal: every protoc language file option declared in
    /// the source `.proto` file. Keyed by the canonical protoc
    /// option name (`"java_package"`, `"csharp_namespace"`,
    /// `"go_package"`, etc.). Pre-fix only the three PHP options
    /// above were propagated through; every other language was
    /// dropped at the parser → ast → manifest boundary.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub language_options: BTreeMap<String, String>,
    /// NW-universal: derived fully qualified class / type name per
    /// language, keyed by short language name (`"php"`, `"java"`,
    /// `"csharp"`, `"go"`, `"ruby"`, `"objc"`, `"swift"`, `"scala"`,
    /// `"rust"`, `"ts"`, `"dart"`). Computed at manifest build time
    /// from `language_options` + the message name + the language's
    /// canonical separator. Codegen consumers read this rather than
    /// rebuilding the FQN themselves.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub language_classes: BTreeMap<String, String>,
    pub schema: String,
    pub table: String,
    pub migration_order: i32,
    pub columns: Vec<ManifestColumn>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<ManifestIndex>,
    pub foreign_keys: Vec<ManifestForeignKey>,
    pub checks: Vec<ManifestCheck>,
    pub partition_strategy: String,
    pub partition_column: String,
    /// pg_partman interval: "MONTHLY", "WEEKLY", "DAILY", "YEARLY" (GAP 3)
    pub partition_interval: String,
    /// Number of future partitions to pre-create (GAP 3)
    pub partition_premake: i32,
    /// Create DEFAULT catch-all partition (GAP 3)
    pub partition_default: bool,
    pub retention_days: i32,
    pub replica_hint: String,
    pub cdc_topic: String,
    pub required_scope: String,
    pub enable_rls: bool,
    pub force_rls: bool,
    pub rls_policies: Vec<ManifestPolicy>,
    pub table_security: ManifestTableSecurity,
    pub soft_delete: bool,
    pub soft_delete_column: String,
    pub audit_fields: bool,
    pub security: ManifestSecurity,
    pub unlogged: bool,
    pub tablespace: String,
    pub extensions: Vec<ManifestExtension>,
    pub materialized_views: Vec<ManifestMaterializedView>,
    pub triggers: Vec<ManifestTrigger>,
    pub sql_artifacts: Vec<ManifestSqlArtifact>,
    pub projections: Vec<ManifestProjection>,
    pub comment: String,
    pub source_file: String,
    pub previous_table_name: String,
    pub allow_drop: bool,
    pub warnings: Vec<String>,
    /// NW-universal: proto3 `reserved <n>;` field numbers. Carried
    /// through to drift / migration safety checks — a NEW column
    /// whose `field_number` falls inside any reserved range is
    /// blocked by the diff layer (prevents accidental reuse of a
    /// number that was previously deleted).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_numbers: Vec<ManifestReservedRange>,
    /// NW-universal: proto3 `reserved "name";` field names. Same
    /// reuse-prevention rationale as `reserved_numbers`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_names: Vec<String>,
}

impl ManifestTable {
    /// Fully-qualified protobuf message name (`proto_package.message_name`).
    /// The canonical identity for routing and collision detection. Falls back to
    /// the bare `message_name` when the package is absent (older manifests /
    /// packageless protos), so callers get a stable non-empty key either way.
    pub fn message_fqn(&self) -> String {
        fq_message(&self.proto_package, &self.message_name)
    }
}

impl ManifestProjection {
    /// Fully-qualified protobuf message name (`proto_package.message_type`) this
    /// projection belongs to — the identity used to group write-owners and route
    /// reads/writes. Two projections whose short `message_type` matches but whose
    /// packages differ are DISTINCT and must not be grouped together.
    pub fn message_fqn(&self) -> String {
        fq_message(&self.proto_package, &self.message_type)
    }
}

/// Join a proto package and a (possibly already-qualified) message name into the
/// fully-qualified message name. When `package` is empty the message name is
/// returned unchanged; when `message` already carries the package prefix it is
/// returned as-is (idempotent) so callers can pass either form safely.
pub fn fq_message(package: &str, message: &str) -> String {
    let package = package.trim();
    let message = message.trim();
    if package.is_empty() || message.starts_with(&format!("{package}.")) {
        message.to_string()
    } else {
        format!("{package}.{message}")
    }
}

/// Manifest counterpart to `ProtoReservedRange` — `[start, end]`
/// inclusive. Both endpoints equal a single reserved number;
/// `end = i32::MAX` represents `reserved N to max;`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestReservedRange {
    pub start: i32,
    pub end: i32,
}

impl ManifestReservedRange {
    /// Does this range cover the given field number?
    pub fn contains(&self, number: i32) -> bool {
        number >= self.start && number <= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestProjection {
    pub message_type: String,
    /// Proto package that owns this projection's message. Carried so routing and
    /// collision detection can key by the FULLY-QUALIFIED message name
    /// (`proto_package` + `message_type`) instead of the bare short name — two
    /// different packages that reuse a short name (e.g. consumer
    /// `acme.authn.entity.v1.User` vs embedded `udb.core.authn.entity.v1.User`)
    /// are distinct identities and must not collide. Empty on manifests loaded
    /// from pre-FQN JSON, where the FQN degrades to the bare `message_type`.
    pub proto_package: String,
    pub projection_kind: String,
    pub backend: String,
    pub instance: String,
    pub resource_name: String,
    pub read_policy: String,
    pub write_policy: String,
    pub fanout_policy: String,
    pub consistency: ManifestConsistency,
    pub write_owner: bool,
    pub options: Vec<ManifestStoreOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestConsistency {
    pub model: String,
    pub read_your_writes: bool,
    pub max_replica_lag_ms: u64,
    pub eventual_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestColumn {
    pub field_name: String,
    pub column_name: String,
    pub proto_type: String,
    pub sql_type: String,
    pub not_null: bool,
    pub unique: bool,
    pub is_primary: bool,
    pub auto_increment: bool,
    pub is_array: bool,
    /// proto3 `optional` (explicit presence). Deliberately NOT serialized: the
    /// manifest JSON feeds `checksum_sha256`, and UDB's own protos use `optional`,
    /// so serializing it would change embedded-manifest checksums and trip the
    /// startup manifest-validation gate. Codegen reads it in-process from freshly
    /// parsed source (`--project-proto`), which never round-trips through JSON.
    #[serde(skip)]
    pub has_presence: bool,
    pub default_value: String,
    pub check_constraint: String,
    pub collation: String,
    pub enum_values: Vec<String>,
    pub comment: String,
    pub exclude_from_insert: bool,
    pub exclude_from_update: bool,
    pub encrypted: bool,
    pub is_json: bool,
    /// Force JSONB storage — auto-generates GIN index (GAP 1)
    pub is_jsonb: bool,
    /// Use jsonb_path_ops operator class on the auto-GIN index (GAP 1)
    pub json_path_ops: bool,
    /// Column is a TSVECTOR type — auto-generates FTS expression index (GAP 5)
    pub is_tsvector: bool,
    /// Language for to_tsvector(); defaults to "simple" (GAP 5)
    pub tsvector_language: String,
    /// Source columns for the auto-generated tsvector expression (GAP 5)
    pub tsvector_source_columns: Vec<String>,
    /// Add GIN trigram index for ILIKE search on this column (GAP 5)
    pub trigram_index: bool,
    pub references: String,
    pub security: ManifestColumnSecurity,
    pub field_number: i32,
    pub oneof_group: String,
    pub previous_column_name: String,
    pub backfill_sql: String,
    pub using_expression: String,
    pub allow_drop: bool,
    pub generated: bool,
    pub generated_expr: String,
    pub is_identity: bool,
    /// Tenant-isolation key designator (proto `tenant_column: true`). Drives
    /// query-plan tenant enforcement. Kept out of `ColumnDdl`/checksum since it
    /// has no DDL effect; `skip_serializing_if` preserves existing manifests.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_tenant_column: bool,
    /// Project-isolation key designator (proto `project_column: true`). Drives
    /// query-plan project enforcement; kept out of `ColumnDdl`/checksum (no DDL
    /// effect); `skip_serializing_if` preserves existing manifests.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_project_column: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub method: String,
    pub where_clause: String,
    pub include_columns: Vec<String>,
    pub operator_class: String,
    pub index_params: Vec<ManifestStoreOption>,
    /// Build index CONCURRENTLY (no write lock; must run outside a transaction) (GAP 8)
    pub concurrent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct ManifestForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub ref_schema: String,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
    pub not_valid: bool,
    pub deferrable: bool,
    pub initially_deferred: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestCheck {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestPolicy {
    pub name: String,
    pub command: String,
    pub using_expression: String,
    pub with_check: String,
    pub permissive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestTableSecurity {
    pub tenant_isolation_mode: String,
    pub project_isolation_mode: String,
    pub tenant_column: String,
    pub project_column: String,
    pub rls_policy_template: String,
    pub soft_delete_mode: String,
    pub retention_class: String,
    pub retention_days: i32,
    pub audit_mode: String,
    pub encryption_profile: String,
    pub pii_profile: String,
    pub break_glass_visible: bool,
    pub export_eligible: bool,
    pub data_residency_policy_ref: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestSecurity {
    pub classification_level: String,
    pub audit_writes: bool,
    pub audit_reads: bool,
    pub retention_days: i32,
    pub encryption_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestColumnSecurity {
    pub is_pii: bool,
    pub is_encrypted: bool,
    pub is_blind_index: bool,
    pub mask_in_logs: bool,
    pub data_class: String,
    pub consent_required: bool,
    pub retention_days: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestExtension {
    pub name: String,
    pub schema: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestMaterializedView {
    pub name: String,
    pub schema: String,
    pub query: String,
    pub with_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestTrigger {
    pub name: String,
    pub schema: String,
    pub table: String,
    pub event: String,
    pub timing: String,
    pub function: String,
    pub for_each: String,
    pub when_clause: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestSqlArtifact {
    pub name: String,
    pub backend: String,
    pub phase: String,
    pub sql: String,
    pub file: String,
    pub checksum_sha256: String,
    pub requires_review: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestStore {
    pub store_kind: String,
    pub backend: String,
    pub logical_name: String,
    pub database_name: String,
    pub namespace: String,
    pub resource_name: String,
    pub dsn_env_key: String,
    pub dsn: String,
    pub owner_schema: String,
    pub owner_table: String,
    pub payload_schema_json: String,
    pub options: Vec<ManifestStoreOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ManifestStoreOption {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct CatalogDdl {
    generator_version: String,
    schemas: Vec<SchemaDdl>,
    stores: Vec<ManifestStore>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SchemaDdl {
    schema: String,
    tables: Vec<TableDdl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct TableDdl {
    message_name: String,
    schema: String,
    table: String,
    migration_order: i32,
    columns: Vec<ColumnDdl>,
    primary_key: Vec<String>,
    indexes: Vec<ManifestIndex>,
    foreign_keys: Vec<ManifestForeignKey>,
    checks: Vec<ManifestCheck>,
    rls_policies: Vec<ManifestPolicy>,
    table_security: ManifestTableSecurity,
    partition_strategy: String,
    partition_column: String,
    partition_interval: String,
    partition_premake: i32,
    partition_default: bool,
    retention_days: i32,
    replica_hint: String,
    cdc_topic: String,
    required_scope: String,
    enable_rls: bool,
    force_rls: bool,
    soft_delete: bool,
    soft_delete_column: String,
    audit_fields: bool,
    security: ManifestSecurity,
    unlogged: bool,
    tablespace: String,
    extensions: Vec<ManifestExtension>,
    materialized_views: Vec<ManifestMaterializedView>,
    triggers: Vec<ManifestTrigger>,
    sql_artifacts: Vec<ManifestSqlArtifact>,
    comment: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ColumnDdl {
    field_name: String,
    field_number: i32,
    column_name: String,
    proto_type: String,
    sql_type: String,
    not_null: bool,
    unique: bool,
    is_primary: bool,
    auto_increment: bool,
    is_array: bool,
    default_value: String,
    check_constraint: String,
    collation: String,
    enum_values: Vec<String>,
    comment: String,
    exclude_from_insert: bool,
    exclude_from_update: bool,
    encrypted: bool,
    is_json: bool,
    is_jsonb: bool,
    json_path_ops: bool,
    is_tsvector: bool,
    tsvector_language: String,
    tsvector_source_columns: Vec<String>,
    trigram_index: bool,
    references: String,
    security: ManifestColumnSecurity,
    oneof_group: String,
    generated: bool,
    generated_expr: String,
    is_identity: bool,
}

impl CatalogManifest {
    pub fn from_schemas(schemas: &[ProtoSchema]) -> Result<Self, serde_json::Error> {
        let mut tables = Vec::new();
        let mut stores = Vec::new();
        let mut warnings = Vec::new();

        for schema in schemas {
            if schema.is_table {
                let table = table_from_schema(schema);
                tables.push(table);
            }
            stores.extend(stores_from_schema(schema));
        }

        tables.sort_by(|a, b| {
            (a.schema.as_str(), a.migration_order, a.table.as_str()).cmp(&(
                b.schema.as_str(),
                b.migration_order,
                b.table.as_str(),
            ))
        });
        stores.sort_by(|a, b| {
            (
                a.store_kind.as_str(),
                a.backend.as_str(),
                a.owner_schema.as_str(),
                a.owner_table.as_str(),
                a.resource_name.as_str(),
            )
                .cmp(&(
                    b.store_kind.as_str(),
                    b.backend.as_str(),
                    b.owner_schema.as_str(),
                    b.owner_table.as_str(),
                    b.resource_name.as_str(),
                ))
        });

        for table in &mut tables {
            table.checksum_sha256 = checksum_hex(&table_ddl(table))?;
        }
        let projections = build_manifest_projections(&mut tables, &stores);

        let schema_order = compute_schema_order(&tables);
        let schema_checksums = compute_schema_checksums(&tables)?;
        let mut validation_errors = validate_manifest_tables(&tables);
        // urgent_fix #24: hard-fail when `pg_table` and `db_table_security` set the
        // SAME concern to conflicting values, instead of silently preferring one.
        // Surfaced as a validation error → Critical drift item → the migration
        // cannot apply unattended until the divergence is resolved in one place.
        for schema in schemas {
            if schema.is_table {
                validation_errors.extend(pg_table_security_conflicts(schema));
            }
        }
        warnings.extend(partitioned_fk_warnings(&tables));
        warnings.extend(duplicate_migration_order_warnings(&tables));
        let checksum_sha256 = checksum_hex(&catalog_ddl(&tables, &stores))?;

        Ok(Self {
            checksum_sha256,
            generator_version: GENERATOR_VERSION.to_string(),
            schema_order,
            schema_checksums,
            tables,
            stores,
            projections,
            validation_errors,
            warnings,
        })
    }

    pub fn table(&self, schema: &str, table: &str) -> Option<&ManifestTable> {
        self.tables
            .iter()
            .find(|item| item.schema == schema && item.table == table)
    }

    pub fn has_validation_errors(&self) -> bool {
        !self.validation_errors.is_empty()
    }

    pub fn migrate_to_current(&mut self) -> ManifestMigrationReport {
        migrate_manifest_to_current(self)
    }
}

pub fn migrate_manifest_to_current(manifest: &mut CatalogManifest) -> ManifestMigrationReport {
    let from_version = manifest.generator_version.clone();
    let mut report = ManifestMigrationReport {
        from_version: from_version.clone(),
        to_version: GENERATOR_VERSION.to_string(),
        migrated: from_version != GENERATOR_VERSION,
        warnings: Vec::new(),
    };

    if from_version.trim().is_empty() {
        manifest.generator_version = "1".to_string();
        report
            .warnings
            .push("missing generator_version assumed to be version 1".to_string());
    }

    match manifest.generator_version.as_str() {
        GENERATOR_VERSION => {}
        "1" => {
            migrate_v1_to_v2(manifest);
            migrate_v2_to_v3(manifest);
            manifest.generator_version = GENERATOR_VERSION.to_string();
        }
        "2" => {
            migrate_v2_to_v3(manifest);
            manifest.generator_version = GENERATOR_VERSION.to_string();
        }
        other => {
            report.warnings.push(format!(
                "unknown manifest generator_version '{}'; preserved payload and marked current",
                other
            ));
            manifest.generator_version = GENERATOR_VERSION.to_string();
        }
    }

    if manifest.projections.is_empty()
        || manifest
            .tables
            .iter()
            .any(|table| table.projections.is_empty())
    {
        manifest.projections = build_manifest_projections(&mut manifest.tables, &manifest.stores);
    }

    report.migrated = report.from_version != manifest.generator_version;
    report.to_version = manifest.generator_version.clone();
    report
}

// Phase I: manifest.rs split into helper modules.
mod build;
pub(crate) use build::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn base_table() -> ManifestTable {
        ManifestTable {
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
            ..ManifestTable::default()
        }
    }

    #[test]
    fn migrate_manifest_populates_message_projections() {
        let mut manifest = CatalogManifest {
            generator_version: GENERATOR_VERSION.to_string(),
            tables: vec![base_table()],
            stores: vec![ManifestStore {
                store_kind: "vector".to_string(),
                backend: "qdrant".to_string(),
                logical_name: "patient_vectors".to_string(),
                resource_name: "patient_embeddings".to_string(),
                owner_schema: "public".to_string(),
                owner_table: "patients".to_string(),
                options: vec![ManifestStoreOption {
                    key: "instance".to_string(),
                    value: "vector_a".to_string(),
                }],
                ..ManifestStore::default()
            }],
            ..CatalogManifest::default()
        };

        let report = migrate_manifest_to_current(&mut manifest);

        assert_eq!(report.to_version, GENERATOR_VERSION);
        assert_eq!(manifest.tables[0].projections.len(), 2);
        assert_eq!(manifest.projections.len(), 2);
        assert!(manifest.projections.iter().any(|projection| {
            projection.projection_kind == "relational"
                && projection.backend == "postgres"
                && projection.write_owner
                && projection.consistency.model == "strong"
        }));
        assert!(manifest.projections.iter().any(|projection| {
            projection.projection_kind == "vector"
                && projection.backend == "qdrant"
                && projection.instance == "vector_a"
                && projection.read_policy == "vector"
                && projection.fanout_policy == "async_projection"
        }));
    }

    // ── NW-universal: per-language manifest fields ────────────────

    #[test]
    fn manifest_table_propagates_language_options_from_schema() {
        // ProtoSchema with multiple language namespaces → ManifestTable
        // carries language_options + language_classes maps for every
        // declared language. Pre-NW-universal only the three PHP
        // fields propagated.
        let mut schema = ProtoSchema::new("Customer");
        schema.proto_package = "acme.billing.v1".to_string();
        schema.is_table = true;
        schema.declared_schema_name = "billing".to_string();
        schema.schema_name = "billing".to_string();
        schema.table_name = "customers".to_string();
        schema.migration_order = 1;
        schema.columns = vec![ProtoColumn {
            column_name: "id".to_string(),
            field_name: "id".to_string(),
            field_number: 1,
            proto_type: "string".to_string(),
            sql_type: "UUID".to_string(),
            is_primary: true,
            ..ProtoColumn::default()
        }];
        // Populate the universal language_options + the legacy
        // PHP-specific fields (both round-trip into ManifestTable).
        schema.php_namespace = "Acme\\Billing\\V1".to_string();
        schema.php_class_prefix = "AB".to_string();
        schema.language_options.insert(
            "java_package".to_string(),
            "com.acme.billing.v1".to_string(),
        );
        schema.language_options.insert(
            "csharp_namespace".to_string(),
            "Acme.Billing.V1".to_string(),
        );
        schema.language_options.insert(
            "go_package".to_string(),
            "github.com/acme/billing/v1".to_string(),
        );

        let mt = build::table_from_schema(&schema);

        // Universal options map round-trips unchanged.
        assert_eq!(
            mt.language_options.get("java_package").unwrap(),
            "com.acme.billing.v1"
        );
        assert_eq!(
            mt.language_options.get("csharp_namespace").unwrap(),
            "Acme.Billing.V1"
        );
        // Derived FQN class names — separator applied per language.
        assert_eq!(
            mt.language_classes.get("java").unwrap(),
            "com.acme.billing.v1.Customer"
        );
        assert_eq!(
            mt.language_classes.get("csharp").unwrap(),
            "Acme.Billing.V1.Customer"
        );
        // Legacy PHP fields still populated for back-compat.
        assert_eq!(mt.php_namespace, "Acme\\Billing\\V1");
        assert_eq!(mt.php_class, "Acme\\Billing\\V1\\ABCustomer");
    }
}

fn detect_fk_cycles(tables: &[ManifestTable]) -> Vec<String> {
    let mut adj = BTreeMap::<String, Vec<String>>::new();
    for table in tables {
        let src = format!("{}.{}", table.schema, table.table);
        adj.entry(src.clone()).or_default();
        for fk in &table.foreign_keys {
            if fk.not_valid {
                continue;
            }
            let dst = format!("{}.{}", fk.ref_schema, fk.ref_table);
            if dst != src {
                adj.entry(src.clone()).or_default().push(dst);
            }
        }
    }
    for edges in adj.values_mut() {
        edges.sort();
        edges.dedup();
    }

    let mut state = BTreeMap::<String, u8>::new();
    let mut path = Vec::<String>::new();
    let mut errors = Vec::new();
    for node in adj.keys() {
        if state.get(node).copied().unwrap_or_default() == 0 {
            dfs_cycle(node, &adj, &mut state, &mut path, &mut errors);
        }
    }
    errors
}

fn partitioned_fk_warnings(tables: &[ManifestTable]) -> Vec<String> {
    let partition_columns: BTreeMap<(&str, &str), &str> = tables
        .iter()
        .filter(|table| {
            !table.partition_strategy.trim().is_empty() && !table.partition_column.trim().is_empty()
        })
        .map(|table| {
            (
                (table.schema.as_str(), table.table.as_str()),
                table.partition_column.as_str(),
            )
        })
        .collect();

    let mut warnings = Vec::new();
    for table in tables {
        for fk in &table.foreign_keys {
            let ref_key = (fk.ref_schema.as_str(), fk.ref_table.as_str());
            let Some(&partition_column) = partition_columns.get(&ref_key) else {
                continue;
            };
            if fk
                .ref_columns
                .iter()
                .any(|column| column.as_str() == partition_column)
            {
                continue;
            }
            warnings.push(format!(
                "skipped FK {}.{} -> {}.{}: referenced table is partitioned on '{}' but FK ref_columns {:?} do not include the partition key; add a denormalized partition-key column to the child table",
                table.schema,
                table.table,
                fk.ref_schema,
                fk.ref_table,
                partition_column,
                fk.ref_columns
            ));
        }
    }
    warnings
}

fn dfs_cycle(
    node: &str,
    adj: &BTreeMap<String, Vec<String>>,
    state: &mut BTreeMap<String, u8>,
    path: &mut Vec<String>,
    errors: &mut Vec<String>,
) {
    state.insert(node.to_string(), 1);
    path.push(node.to_string());

    if let Some(edges) = adj.get(node) {
        for next in edges {
            match state.get(next).copied().unwrap_or_default() {
                0 => dfs_cycle(next, adj, state, path, errors),
                1 => {
                    if let Some(pos) = path.iter().position(|item| item == next) {
                        let mut cycle = path[pos..].to_vec();
                        cycle.push(next.clone());
                        errors.push(format!(
                            "FK circular reference detected: {}. Annotate one FK with not_valid:true to break bootstrap ordering.",
                            cycle.join(" -> ")
                        ));
                    }
                }
                _ => {}
            }
        }
    }

    path.pop();
    state.insert(node.to_string(), 2);
}

/// Warn when two tables in the same schema claim the same `migration_order`.
///
/// Tables sort by `(schema, migration_order, table)`, so a collision does not
/// crash — the tie is broken by table name and the declared ordering is quietly
/// wrong from then on. Worse, the next person to add a table has no way to see
/// which orders are taken without grepping every proto, so collisions
/// accumulate. Naming the next free order turns the warning into the fix.
fn duplicate_migration_order_warnings(tables: &[ManifestTable]) -> Vec<String> {
    let mut by_schema: BTreeMap<&str, BTreeMap<i32, Vec<&str>>> = BTreeMap::new();
    for table in tables {
        by_schema
            .entry(table.schema.as_str())
            .or_default()
            .entry(table.migration_order)
            .or_default()
            .push(table.table.as_str());
    }
    let mut warnings = Vec::new();
    for (schema, orders) in &by_schema {
        let next_free = orders.keys().max().map(|max| max + 1).unwrap_or(1);
        for (order, names) in orders {
            if names.len() > 1 {
                warnings.push(format!(
                    "{schema} has {} tables sharing migration_order {order} ({}); the declared order is decided by table name instead. Next free order in {schema}: {next_free}",
                    names.len(),
                    names.join(", ")
                ));
            }
        }
    }
    warnings
}

fn validate_table_shape(schema: &str, table: &str, columns: &[ManifestColumn]) -> Vec<String> {
    let mut warnings = Vec::new();
    let mut names = BTreeSet::new();
    let mut fields = BTreeSet::new();
    for column in columns {
        if !names.insert(column.column_name.clone()) {
            warnings.push(format!(
                "{}.{} has duplicate column name {}",
                schema, table, column.column_name
            ));
        }
        if column.field_number > 0 && !fields.insert(column.field_number) {
            warnings.push(format!(
                "{}.{} has duplicate proto field number {}",
                schema, table, column.field_number
            ));
        }
    }
    warnings.extend(validate_mutation_identity_shape(schema, table, columns));
    warnings
}

/// Warn when a table is shaped so that ordinary writes cannot name the row they
/// touched.
///
/// A database-assigned primary key (`BIGSERIAL`/auto-increment) is absent from
/// the request record, so the broker falls back to scanning for an `id`/`*_id`
/// field to build the mutation's `resource_uri`. With more than one such column
/// that scan is ambiguous and fails CLOSED — every write to the table is
/// rejected with `mutation_resource_uri_identity_ambiguous`.
///
/// A deployment lost six days of tracking rows to exactly this: the writer
/// produced rows, the broker refused all of them, and it surfaced only when
/// somebody noticed the table's newest row was a week old. The runtime error
/// already names the remedies, but it arrives at every write rather than once,
/// at publish time — which is where a shape problem belongs.
fn validate_mutation_identity_shape(
    schema: &str,
    table: &str,
    columns: &[ManifestColumn],
) -> Vec<String> {
    let database_assigned_pk = columns
        .iter()
        .any(|column| column.is_primary && column.auto_increment);
    if !database_assigned_pk {
        return Vec::new();
    }
    let identity_candidates: Vec<&str> = columns
        .iter()
        .filter(|column| !column.is_primary)
        .map(|column| column.column_name.as_str())
        .filter(|name| *name == "id" || name.ends_with("_id"))
        .collect();
    if identity_candidates.len() < 2 {
        return Vec::new();
    }
    vec![format!(
        "{schema}.{table} has a database-assigned primary key and {} identity-shaped columns          ({}); every write will be refused as ambiguous unless the caller sets return_record          (so the persisted row's key names it), declares conflict_fields, or supplies the primary key itself",
        identity_candidates.len(),
        identity_candidates.join(", ")
    )]
}

fn validate_audit_fields(schema: &str, table: &str, columns: &[ManifestColumn]) -> Vec<String> {
    let present = columns
        .iter()
        .map(|column| column.column_name.as_str())
        .collect::<BTreeSet<_>>();
    ["created_at", "updated_at", "created_by"]
        .iter()
        .filter(|required| !present.contains(**required))
        .map(|required| {
            format!(
                "{}.{} audit_fields=true but required audit column {} is missing",
                schema, table, required
            )
        })
        .collect()
}

fn catalog_ddl(tables: &[ManifestTable], stores: &[ManifestStore]) -> CatalogDdl {
    let mut by_schema: BTreeMap<String, Vec<TableDdl>> = BTreeMap::new();
    for table in tables {
        by_schema
            .entry(table.schema.clone())
            .or_default()
            .push(table_ddl(table));
    }
    CatalogDdl {
        generator_version: GENERATOR_VERSION.to_string(),
        schemas: by_schema
            .into_iter()
            .map(|(schema, tables)| SchemaDdl { schema, tables })
            .collect(),
        stores: stores.to_vec(),
    }
}

/// Recompute the semantic catalog checksum from decoded manifest authority.
///
/// The generator version is read from the manifest so a durable catalog keeps
/// the checksum semantics of the generator that produced it. Callers must not
/// trust the embedded `checksum_sha256` field as proof of payload integrity.
pub(crate) fn catalog_checksum_sha256(
    manifest: &CatalogManifest,
) -> Result<String, serde_json::Error> {
    let mut ddl = catalog_ddl(&manifest.tables, &manifest.stores);
    ddl.generator_version = manifest.generator_version.clone();
    checksum_hex(&ddl)
}

fn table_ddl(table: &ManifestTable) -> TableDdl {
    TableDdl {
        message_name: table.message_name.clone(),
        schema: table.schema.clone(),
        table: table.table.clone(),
        migration_order: table.migration_order,
        columns: table.columns.iter().map(column_ddl).collect(),
        primary_key: table.primary_key.clone(),
        indexes: table.indexes.clone(),
        foreign_keys: table.foreign_keys.clone(),
        checks: table.checks.clone(),
        rls_policies: table.rls_policies.clone(),
        table_security: table.table_security.clone(),
        partition_strategy: table.partition_strategy.clone(),
        partition_column: table.partition_column.clone(),
        partition_interval: table.partition_interval.clone(),
        partition_premake: table.partition_premake,
        partition_default: table.partition_default,
        retention_days: table.retention_days,
        replica_hint: table.replica_hint.clone(),
        cdc_topic: table.cdc_topic.clone(),
        required_scope: table.required_scope.clone(),
        enable_rls: table.enable_rls,
        force_rls: table.force_rls,
        soft_delete: table.soft_delete,
        soft_delete_column: table.soft_delete_column.clone(),
        audit_fields: table.audit_fields,
        security: table.security.clone(),
        unlogged: table.unlogged,
        tablespace: table.tablespace.clone(),
        extensions: table.extensions.clone(),
        materialized_views: table.materialized_views.clone(),
        triggers: table.triggers.clone(),
        sql_artifacts: table.sql_artifacts.clone(),
        comment: table.comment.clone(),
    }
}

fn column_ddl(column: &ManifestColumn) -> ColumnDdl {
    ColumnDdl {
        field_name: column.field_name.clone(),
        field_number: column.field_number,
        column_name: column.column_name.clone(),
        proto_type: column.proto_type.clone(),
        sql_type: column.sql_type.clone(),
        not_null: column.not_null,
        unique: column.unique,
        is_primary: column.is_primary,
        auto_increment: column.auto_increment,
        is_array: column.is_array,
        default_value: column.default_value.clone(),
        check_constraint: column.check_constraint.clone(),
        collation: column.collation.clone(),
        enum_values: column.enum_values.clone(),
        comment: column.comment.clone(),
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
        references: column.references.clone(),
        security: column.security.clone(),
        oneof_group: column.oneof_group.clone(),
        generated: column.generated,
        generated_expr: column.generated_expr.clone(),
        is_identity: column.is_identity,
    }
}

fn checksum_hex<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let json = serde_json::to_vec(value)?;
    let digest = Sha256::digest(json);
    Ok(format!("sha256:{digest:x}"))
}

pub fn index_key(index: &ManifestIndex) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}",
        index.name.to_ascii_lowercase(),
        index.columns.join(","),
        index.unique,
        index.method.to_ascii_uppercase(),
        normalize_expression(&index.where_clause),
        index.include_columns.join(",")
    )
}

pub fn fk_key(fk: &ManifestForeignKey) -> String {
    format!(
        "{}|{}|{}.{}({})|{}|{}|{}|{}|{}",
        fk.name.to_ascii_lowercase(),
        fk.columns.join(","),
        fk.ref_schema,
        fk.ref_table,
        fk.ref_columns.join(","),
        fk.on_delete,
        fk.on_update,
        fk.not_valid,
        fk.deferrable,
        fk.initially_deferred
    )
}

pub fn check_key(check: &ManifestCheck) -> String {
    normalize_expression(&check.expression)
}

fn sort_schemas(values: &mut [String], min_order: &BTreeMap<String, i32>) {
    values.sort_by(|a, b| {
        (
            min_order.get(a).copied().unwrap_or(DEFAULT_MIGRATION_ORDER),
            a.as_str(),
        )
            .cmp(&(
                min_order.get(b).copied().unwrap_or(DEFAULT_MIGRATION_ORDER),
                b.as_str(),
            ))
    });
}

fn defaulted(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value.trim().to_string()
    }
}

fn first_non_empty(a: &str, b: &str, fallback: &str) -> String {
    if !a.trim().is_empty() {
        a.trim().to_string()
    } else if !b.trim().is_empty() {
        b.trim().to_string()
    } else {
        fallback.to_string()
    }
}

fn normalize_ident(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_ident_or(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        normalize_ident(value)
    }
}

fn normalize_sql_type(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        "TEXT".to_string()
    } else {
        value.to_ascii_uppercase()
    }
}

fn normalize_action(value: &str) -> String {
    let value = value.trim().to_ascii_uppercase();
    if value.is_empty() {
        "NO ACTION".to_string()
    } else {
        value
    }
}

fn normalize_policy_command(value: &str) -> String {
    let value = normalize_enum(value);
    if value.is_empty() {
        "ALL".to_string()
    } else {
        value
    }
}

fn normalize_enum(value: &str) -> String {
    value
        .trim()
        .to_ascii_uppercase()
        .trim_start_matches("RLS_COMMAND_")
        .trim_start_matches("POLICY_COMMAND_")
        .trim_start_matches("COMMAND_")
        .to_string()
}

fn normalize_replica_hint(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "primary" => "primary".to_string(),
        "replica" | "replica_ok" | "readonly" | "read_only" => "replica".to_string(),
        _ => String::new(),
    }
}

fn normalize_expression(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .replace('"', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
}

fn normalize_backend(value: &str) -> String {
    let upper = value.trim().to_ascii_uppercase();
    for prefix in [
        "SQL_BACKEND_",
        "NOSQL_BACKEND_",
        "VECTOR_BACKEND_",
        "GRAPH_BACKEND_",
        "TIMESERIES_BACKEND_",
        "TIME_SERIES_BACKEND_",
        "COLUMN_BACKEND_",
        "CACHE_BACKEND_",
        "STORAGE_BACKEND_",
        "OBJECT_BACKEND_",
        "MODEL_BACKEND_",
    ] {
        if let Some(stripped) = upper.strip_prefix(prefix) {
            return stripped.to_ascii_lowercase();
        }
    }
    upper.to_ascii_lowercase()
}
