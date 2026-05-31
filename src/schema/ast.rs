use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SourceSpan {
    pub file: String,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoCatalog {
    pub schemas: Vec<ProtoSchema>,
}

/// Every well-known protoc file-level language option, keyed by the
/// option name as it appears in the `.proto` file. Built out from the
/// canonical protoc descriptor — `descriptor.proto`'s `FileOptions`
/// message — so any language protoc supports is recognised here, not
/// just PHP (which was the only one wired pre-NW-universal).
///
/// The values are constants so the parser can do an O(1) recognition
/// check without parsing strings. Pre-fix gaps: only `php_namespace`,
/// `php_class_prefix`, `php_metadata_namespace` were recognised; every
/// other entry in this list was silently dropped by the parser, which
/// meant Java / C# / Go / Ruby / Swift / Objective-C / Kotlin clients
/// had no way to drive their language namespace through UDB.
///
/// Spec source: https://protobuf.dev/reference/protobuf/proto3-spec/#fileoptions
pub const KNOWN_LANGUAGE_FILE_OPTIONS: &[&str] = &[
    // PHP
    "php_namespace",
    "php_class_prefix",
    "php_metadata_namespace",
    "php_generic_services",
    // Java (also covers Kotlin — Kotlin reuses java_package)
    "java_package",
    "java_outer_classname",
    "java_multiple_files",
    "java_string_check_utf8",
    "java_generic_services",
    "java_generate_equals_and_hash",
    // C# / .NET
    "csharp_namespace",
    // Go
    "go_package",
    // Ruby
    "ruby_package",
    // Objective-C
    "objc_class_prefix",
    // Swift
    "swift_prefix",
    // Python
    "py_generic_services",
    // Dart (no native namespace in protoc; some toolchains use
    // `dart_package` non-standard option — recognise it anyway)
    "dart_package",
    // Rust (prost / tonic / rust-protobuf ecosystems use
    // `rust_namespace` as a community convention)
    "rust_namespace",
    // TypeScript / JavaScript (community ts-proto convention)
    "ts_namespace",
    // Scala (scalapb)
    "scala_package",
];

/// Separator used when a code generator concatenates the language
/// namespace with a message class name. Returned by
/// [`ProtoSchema::namespace_separator`].
pub const PHP_NAMESPACE_SEPARATOR: &str = "\\";
pub const JAVA_NAMESPACE_SEPARATOR: &str = ".";
pub const CSHARP_NAMESPACE_SEPARATOR: &str = ".";
pub const GO_NAMESPACE_SEPARATOR: &str = ".";
pub const PYTHON_NAMESPACE_SEPARATOR: &str = ".";
pub const RUBY_NAMESPACE_SEPARATOR: &str = "::";
pub const OBJC_NAMESPACE_SEPARATOR: &str = "";
pub const SWIFT_NAMESPACE_SEPARATOR: &str = "";
pub const SCALA_NAMESPACE_SEPARATOR: &str = ".";
pub const RUST_NAMESPACE_SEPARATOR: &str = "::";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoSchema {
    pub message_name: String,
    pub proto_package: String,
    pub php_namespace: String,
    pub php_class_prefix: String,
    pub php_metadata_namespace: String,
    /// Universal language-options map — every recognised
    /// language option from `KNOWN_LANGUAGE_FILE_OPTIONS` lands here,
    /// keyed by the option name. The legacy `php_*` fields above stay
    /// for back-compat (downstream readers haven't been migrated yet);
    /// new code should consume this map instead.
    ///
    /// Example after parsing `option java_package = "com.acme.billing";`:
    ///   `language_options = { "java_package" → "com.acme.billing" }`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub language_options: BTreeMap<String, String>,
    pub schema_name: String,
    pub declared_schema_name: String,
    pub migration_dir: String,
    pub table_name: String,
    pub migration_order: i32,
    pub is_table: bool,
    pub columns: Vec<ProtoColumn>,
    pub indexes: Vec<ProtoIndex>,
    pub foreign_keys: Vec<ProtoForeignKey>,
    pub file: String,
    pub partition_strategy: String,
    pub partition_column: String,
    /// Interval for pg_partman time-based auto-partitioning, e.g. "MONTHLY" (GAP 3)
    pub partition_interval: String,
    /// Number of future partitions to pre-create via pg_partman (GAP 3)
    pub partition_premake: i32,
    /// Create a DEFAULT partition to catch unmatched rows (GAP 3)
    pub partition_default: bool,
    /// Months of data to retain; 0 = infinite (GAP 3)
    pub partition_retention_months: i32,
    pub retention_days: i32,
    pub replica_hint: String,
    pub cdc_topic: String,
    pub required_scope: String,
    pub soft_delete: bool,
    pub soft_delete_column: String,
    pub audit_fields: bool,
    pub enable_rls: bool,
    pub force_rls: bool,
    pub rls_policies: Vec<RlsPolicy>,
    pub security: ProtoSecurity,
    pub unlogged: bool,
    pub tablespace: String,
    pub extensions: Vec<DbExtension>,
    pub materialized_views: Vec<MaterializedView>,
    pub triggers: Vec<DbTrigger>,
    pub sql_artifacts: Vec<SqlArtifact>,
    pub table_comment: String,
    pub vector_store: Option<VectorStore>,
    pub graph_store: Option<GraphStore>,
    pub document_store: Option<DocumentStore>,
    pub timeseries_store: Option<TimeSeriesStore>,
    pub cache: Option<CacheStore>,
    pub model_registry: Option<ModelRegistry>,
    pub column_store: Option<ColumnStore>,
    pub generic_stores: Vec<GenericStore>,
    pub previous_table_name: String,
    pub allow_drop: bool,
    /// NW-universal: proto3 `reserved <n>, <n>;` field numbers.
    /// Pre-fix the `db_parser` silently skipped reserved blocks,
    /// which meant the manifest had no way to detect a NEW field that
    /// happens to reuse a number marked reserved in the old proto.
    /// Schema-evolution safety depends on this — protoc's compile-time
    /// `reserved` enforcement only applies to the SAME .proto file
    /// at compile time, not across the manifest's drift / diff path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_numbers: Vec<ProtoReservedRange>,
    /// NW-universal: proto3 `reserved "old_name";` field names.
    /// Same rationale as `reserved_numbers` — protect against a NEW
    /// field that reuses an old name slot.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reserved_names: Vec<String>,
}

impl ProtoSchema {
    pub fn new(message_name: impl Into<String>) -> Self {
        Self {
            message_name: message_name.into(),
            migration_order: 9999,
            ..Self::default()
        }
    }

    /// Look up the value of any language file option by its protoc
    /// name (e.g. `"java_package"`, `"csharp_namespace"`,
    /// `"go_package"`). Returns `None` when the option wasn't declared
    /// in the source `.proto` file. Replaces the pre-NW-universal
    /// pattern of reading `schema.php_namespace` directly — that field
    /// only worked for PHP.
    pub fn language_option(&self, option_name: &str) -> Option<&str> {
        self.language_options.get(option_name).map(String::as_str)
    }

    /// Get the namespace for a target language using the canonical
    /// protoc option name for that language:
    ///
    /// - `"php"` → `php_namespace`
    /// - `"java"` / `"kotlin"` → `java_package`
    /// - `"csharp"` / `"dotnet"` → `csharp_namespace`
    /// - `"go"` → `go_package`
    /// - `"ruby"` → `ruby_package`
    /// - `"objc"` → `objc_class_prefix`
    /// - `"swift"` → `swift_prefix`
    /// - `"scala"` → `scala_package`
    /// - `"rust"` → `rust_namespace`
    /// - `"ts"` / `"typescript"` → `ts_namespace`
    /// - `"dart"` → `dart_package`
    /// - `"python"` → falls back to `proto_package`
    ///   (protoc emits no python-specific namespace option)
    ///
    /// Returns `Some(&str)` when the option was declared. Returns
    /// `None` when neither the option nor the fallback exists.
    pub fn namespace_for(&self, language: &str) -> Option<&str> {
        let lang = language.to_ascii_lowercase();
        let option_name = match lang.as_str() {
            "php" => "php_namespace",
            "java" | "kotlin" => "java_package",
            "csharp" | "cs" | "dotnet" | ".net" => "csharp_namespace",
            "go" | "golang" => "go_package",
            "ruby" | "rb" => "ruby_package",
            "objc" | "objective-c" | "objectivec" => "objc_class_prefix",
            "swift" => "swift_prefix",
            "scala" => "scala_package",
            "rust" | "rs" => "rust_namespace",
            "ts" | "typescript" | "js" | "javascript" => "ts_namespace",
            "dart" => "dart_package",
            "python" | "py" => {
                // Python has no dedicated namespace option; protoc
                // uses the `package` directive directly. Surface
                // that as the python namespace.
                return if self.proto_package.is_empty() {
                    None
                } else {
                    Some(self.proto_package.as_str())
                };
            }
            _ => return None,
        };
        // Prefer the universal map; fall back to the legacy PHP fields
        // for code paths that still write those directly.
        self.language_options
            .get(option_name)
            .map(String::as_str)
            .or_else(|| match option_name {
                "php_namespace" if !self.php_namespace.is_empty() => Some(&self.php_namespace),
                _ => None,
            })
    }

    /// Separator the given language uses between namespace components
    /// and the message class name. Used by code generators that build
    /// fully qualified type names like `com.acme.billing.Customer` or
    /// `App\Billing\Customer`. Defaults to `"."` when the language is
    /// unknown — safest convention for new languages we haven't
    /// catalogued.
    pub fn namespace_separator(language: &str) -> &'static str {
        match language.to_ascii_lowercase().as_str() {
            "php" => PHP_NAMESPACE_SEPARATOR,
            "java" | "kotlin" => JAVA_NAMESPACE_SEPARATOR,
            "csharp" | "cs" | "dotnet" | ".net" => CSHARP_NAMESPACE_SEPARATOR,
            "go" | "golang" => GO_NAMESPACE_SEPARATOR,
            "ruby" | "rb" => RUBY_NAMESPACE_SEPARATOR,
            "objc" | "objective-c" | "objectivec" => OBJC_NAMESPACE_SEPARATOR,
            "swift" => SWIFT_NAMESPACE_SEPARATOR,
            "scala" => SCALA_NAMESPACE_SEPARATOR,
            "rust" | "rs" => RUST_NAMESPACE_SEPARATOR,
            "python" | "py" => PYTHON_NAMESPACE_SEPARATOR,
            _ => ".",
        }
    }

    /// Build the fully qualified class / type name for `message_name`
    /// in the given target language, applying the right separator.
    /// Returns the bare message name when no namespace is declared
    /// for that language.
    ///
    /// Example: with `option java_package = "com.acme.billing"` and
    /// `message_name = "Customer"`:
    ///   `fully_qualified_name("java", "Customer")` →
    ///   `"com.acme.billing.Customer"`.
    pub fn fully_qualified_name(&self, language: &str, message_name: &str) -> String {
        let Some(ns) = self.namespace_for(language) else {
            return message_name.to_string();
        };
        let sep = Self::namespace_separator(language);
        // Trim trailing separator chars (PHP namespaces sometimes have
        // trailing `\\`; Java packages don't but a paranoid trim is
        // cheap).
        let cleaned = ns
            .trim()
            .trim_matches(|c: char| c == '\\' || c == '/' || c == '.');
        if cleaned.is_empty() {
            return message_name.to_string();
        }
        if sep.is_empty() {
            // Objective-C / Swift use prefix-as-class-prefix (no
            // separator). `objc_class_prefix = "AC"` + `Customer` →
            // `ACCustomer`.
            format!("{cleaned}{message_name}")
        } else {
            format!("{cleaned}{sep}{message_name}")
        }
    }

    /// Convenience: list every language that has any namespace /
    /// package / prefix declared. Used by codegen entry points to
    /// know which language artifacts to emit.
    pub fn declared_languages(&self) -> Vec<&'static str> {
        let mut langs: Vec<&'static str> = Vec::new();
        for (key, _) in &self.language_options {
            let lang: Option<&'static str> = match key.as_str() {
                "php_namespace" | "php_class_prefix" | "php_metadata_namespace" => Some("php"),
                "java_package" | "java_outer_classname" | "java_multiple_files" => Some("java"),
                "csharp_namespace" => Some("csharp"),
                "go_package" => Some("go"),
                "ruby_package" => Some("ruby"),
                "objc_class_prefix" => Some("objc"),
                "swift_prefix" => Some("swift"),
                "scala_package" => Some("scala"),
                "rust_namespace" => Some("rust"),
                "ts_namespace" => Some("ts"),
                "dart_package" => Some("dart"),
                _ => None,
            };
            if let Some(l) = lang
                && !langs.contains(&l)
            {
                langs.push(l);
            }
        }
        // Legacy PHP fields populate even when they didn't land in
        // language_options (back-compat for older parsing paths).
        if (!self.php_namespace.is_empty() || !self.php_class_prefix.is_empty())
            && !langs.contains(&"php")
        {
            langs.push("php");
        }
        langs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoColumn {
    pub field_name: String,
    pub column_name: String,
    pub proto_type: String,
    pub sql_type: String,
    pub not_null: bool,
    pub unique: bool,
    pub is_primary: bool,
    pub auto_increment: bool,
    pub is_array: bool,
    pub default_value: String,
    pub check_constraint: String,
    pub collation: String,
    pub enum_values: Vec<String>,
    pub comment: String,
    pub exclude_from_insert: bool,
    pub exclude_from_update: bool,
    pub encrypted: bool,
    pub is_json: bool,
    /// Force JSONB (not JSON) storage — enables GIN auto-index (GAP 1)
    pub is_jsonb: bool,
    /// Use jsonb_path_ops operator class on auto-generated GIN index (GAP 1)
    pub json_path_ops: bool,
    /// Column is a TSVECTOR generated column (GAP 5)
    pub is_tsvector: bool,
    /// Language for to_tsvector(), e.g. "english", "simple" (GAP 5)
    pub tsvector_language: String,
    /// Source columns for auto-generated tsvector expression index (GAP 5)
    pub tsvector_source_columns: Vec<String>,
    /// Add GIN index using pg_trgm for ILIKE queries (GAP 5)
    pub trigram_index: bool,
    pub references: String,
    pub on_delete: String,
    pub on_update: String,
    pub security: ProtoColumnSecurity,
    pub foreign_key: Option<ProtoForeignKey>,
    pub indexes: Vec<ProtoIndex>,
    pub storage: Option<StorageField>,
    pub field_number: i32,
    pub oneof_group: String,
    pub previous_column_name: String,
    pub backfill_sql: String,
    pub using_expression: String,
    pub allow_drop: bool,
    pub generated: bool,
    pub generated_expr: String,
    pub is_identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RlsPolicy {
    pub name: String,
    pub command: String,
    pub using_expression: String,
    pub with_check: String,
    pub permissive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoSecurity {
    pub classification_level: String,
    pub audit_writes: bool,
    pub audit_reads: bool,
    pub retention_days: i32,
    pub encryption_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoColumnSecurity {
    pub is_pii: bool,
    pub is_encrypted: bool,
    pub is_blind_index: bool,
    pub mask_in_logs: bool,
    pub data_class: String,
    pub consent_required: bool,
    pub retention_days: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DbExtension {
    pub name: String,
    pub schema: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MaterializedView {
    pub name: String,
    pub schema: String,
    pub query: String,
    pub with_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DbTrigger {
    pub name: String,
    pub event: String,
    pub timing: String,
    pub function: String,
    pub for_each: String,
    pub when_clause: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SqlArtifact {
    pub name: String,
    pub backend: String,
    pub phase: String,
    pub sql: String,
    pub file: String,
    pub checksum_sha256: String,
    pub requires_review: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoIndex {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: String,
    pub index_method: String,
    pub where_clause: String,
    pub include_columns: Vec<String>,
    pub operator_class: String,
    pub index_params: Vec<StoreOption>,
    /// Create index CONCURRENTLY (no table lock, cannot run inside transaction) (GAP 8)
    pub concurrent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoForeignKey {
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
pub struct VectorStore {
    pub backend: String,
    pub collection_name: String,
    pub dimension: i32,
    pub distance: String,
    pub shard_count: i32,
    pub replica_count: i32,
    pub on_disk: bool,
    pub payload_schema_json: String,
    pub hnsw_m: i32,
    pub hnsw_ef_construction: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GraphStore {
    pub backend: String,
    pub graph_name: String,
    pub node_label: String,
    pub id_field: String,
    pub tenant_field: String,
    pub edge_source_field: String,
    pub edge_target_field: String,
    pub payload_schema_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DocumentStore {
    pub backend: String,
    pub database_name: String,
    pub collection_name: String,
    pub partition_key: String,
    pub id_field: String,
    pub tenant_field: String,
    pub ttl_seconds: i32,
    pub payload_schema_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TimeSeriesStore {
    pub backend: String,
    pub database_name: String,
    pub measurement_name: String,
    pub time_field: String,
    pub tenant_field: String,
    pub tag_fields: Vec<String>,
    pub value_fields: Vec<String>,
    pub retention_days: i32,
    pub downsample_policy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CacheStore {
    pub backend: String,
    pub key_pattern: String,
    pub ttl_seconds: i32,
    pub write_through: bool,
    pub read_through: bool,
    pub eviction_policy: String,
    pub cluster_env_key: String,
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StorageField {
    pub backend: String,
    pub bucket_env_key: String,
    pub key_prefix: String,
    pub presigned_read: bool,
    pub presigned_write: bool,
    pub presigned_ttl_seconds: i32,
    pub server_side_encryption: bool,
    pub kms_key_id: String,
    pub acl: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ModelRegistry {
    pub backend: String,
    pub experiment_name: String,
    pub artifact_path: String,
    pub auto_register: bool,
    pub stage: String,
    pub metric_keys: Vec<String>,
    pub param_keys: Vec<String>,
    pub storage_uri_env: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ColumnStore {
    pub backend: String,
    pub database_name: String,
    pub table_name: String,
    pub partition_key: String,
    pub sort_key: String,
    pub compression: String,
    pub ttl_seconds: i32,
    pub payload_schema_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GenericStore {
    pub store_kind: String,
    pub backend: String,
    pub logical_name: String,
    pub database_name: String,
    pub namespace: String,
    pub resource_name: String,
    pub dsn_env_key: String,
    pub dsn: String,
    pub payload_schema_json: String,
    pub options: Vec<StoreOption>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct StoreOption {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoFileAst {
    pub syntax: String,
    pub package: String,
    pub imports: Vec<ProtoImport>,
    pub options: Vec<ProtoOptionAssignment>,
    pub definitions: Vec<ProtoDefinition>,
    pub file: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoImport {
    pub path: String,
    pub weak: bool,
    pub public: bool,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtoDefinition {
    Message(ProtoMessageAst),
    Enum(ProtoEnumAst),
    Service(ProtoServiceAst),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoMessageAst {
    pub name: String,
    pub fields: Vec<ProtoFieldAst>,
    pub options: Vec<ProtoOptionAssignment>,
    pub nested: Vec<ProtoDefinition>,
    pub reserved_numbers: Vec<ProtoReservedRange>,
    pub reserved_names: Vec<String>,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoFieldAst {
    pub label: String,
    pub field_type: String,
    pub name: String,
    pub number: i32,
    pub oneof_group: String,
    pub options: Vec<ProtoOptionAssignment>,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoReservedRange {
    pub start: i32,
    pub end: i32,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoEnumAst {
    pub name: String,
    pub values: Vec<ProtoEnumValueAst>,
    pub options: Vec<ProtoOptionAssignment>,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoEnumValueAst {
    pub name: String,
    pub number: i32,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoServiceAst {
    pub name: String,
    pub rpcs: Vec<ProtoRpcAst>,
    pub options: Vec<ProtoOptionAssignment>,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoRpcAst {
    pub name: String,
    pub request_type: String,
    pub response_type: String,
    pub client_streaming: bool,
    pub server_streaming: bool,
    pub options: Vec<ProtoOptionAssignment>,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProtoOptionAssignment {
    pub name: String,
    pub value: ProtoOptionLiteral,
    pub span: SourceSpan,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtoOptionLiteral {
    Scalar(String),
    List(Vec<ProtoOptionLiteral>),
    Object(Vec<ProtoOptionAssignment>),
}

impl Default for ProtoOptionLiteral {
    fn default() -> Self {
        Self::Scalar(String::new())
    }
}
