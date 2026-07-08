use std::collections::HashMap;

use crate::ast::{
    CacheStore, ColumnStore, DbExtension, DbTrigger, DocumentStore, GenericStore, GraphStore,
    MaterializedView, ModelRegistry, ProtoColumn, ProtoColumnSecurity, ProtoForeignKey, ProtoIndex,
    ProtoSchema, ProtoSecurity, RlsPolicy, SqlArtifact, StorageField, StoreOption, TimeSeriesStore,
    VectorStore,
};

use super::naming::{
    normalize_backend, normalize_enum, normalize_index_type, normalize_referential_action,
    normalize_store_kind, parse_bool, parse_i32, to_snake_case,
};

const DEFAULT_TRIGGER_FOR_EACH: &str = "ROW";
const DEFAULT_INDEX_TYPE: &str = "BTREE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OptionKind {
    Table,
    Column,
    VectorStore,
    GraphStore,
    DocumentStore,
    TimeSeriesStore,
    Cache,
    Storage,
    Security,
    TableSecurity,
    FieldSecurityScalar,
    ColumnSecurity,
    ModelRegistry,
    ColumnStore,
    SqlStore,
    NoSqlStore,
    GenericStore,
    ObjectStore,
}

impl OptionKind {
    pub(super) fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Column => "column",
            Self::VectorStore => "vector_store",
            Self::GraphStore => "graph_store",
            Self::DocumentStore => "document_store",
            Self::TimeSeriesStore => "timeseries_store",
            Self::Cache => "cache",
            Self::Storage => "storage",
            Self::Security => "security",
            Self::TableSecurity => "db_table_security",
            Self::FieldSecurityScalar => "field_security_scalar",
            Self::ColumnSecurity => "db_column_security",
            Self::ModelRegistry => "model_registry",
            Self::ColumnStore => "column_store",
            Self::SqlStore => "sql_store",
            Self::NoSqlStore => "nosql_store",
            Self::GenericStore => "data_store",
            Self::ObjectStore => "object_store",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParserOptionMetadata {
    pub option_name: &'static str,
    pub kind: &'static str,
    pub option_type: &'static str,
    pub target: &'static str,
    pub required: bool,
    pub since_version: &'static str,
    pub description: &'static str,
    pub example: &'static str,
    pub accepted_keys: &'static [&'static str],
    /// Subset of `accepted_keys` whose values are parsed as numbers (i32).
    /// Single source of truth for `is_numeric_annotation_key` — keep these as
    /// a slice of names that also appear in `accepted_keys`.
    pub numeric_keys: &'static [&'static str],
}

const TABLE_KEYS: &[&str] = &[
    "table_name",
    "schema_name",
    "migration_order",
    "is_table",
    "comment",
    "soft_delete",
    "soft_delete_column",
    "audit_fields",
    "enable_rls",
    "force_rls",
    "unlogged",
    "tablespace",
    "partition_strategy",
    "partition_by",
    "partition_column",
    "retention_days",
    "partition_interval",
    "partition_premake",
    "partition_default",
    "partition_retention_months",
    "replica_hint",
    "cdc_topic",
    "required_scope",
    "previous_table_name",
    "allow_drop",
    "indexes",
    "foreign_keys",
    "rls_policies",
    "extensions",
    "materialized_views",
    "triggers",
    "sql_artifacts",
    "vector_store",
];
const COLUMN_KEYS: &[&str] = &[
    "column_name",
    "sql_type",
    "not_null",
    "nullable",
    "unique",
    "primary_key",
    "is_primary_key",
    "auto_increment",
    "default_value",
    "check_constraint",
    "collation",
    "enum_values",
    "enum_value",
    "comment",
    "exclude_from_insert",
    "exclude_from_update",
    "encrypted",
    "encrypt",
    "is_json",
    "is_jsonb",
    "json_path_ops",
    "is_tsvector",
    "tsvector_language",
    "tsvector_source_columns",
    "tsvector_source_column",
    "trigram_index",
    "references",
    "foreign_key",
    "tenant_column",
    "project_column",
    "on_delete",
    "on_update",
    "previous_column_name",
    "backfill_sql",
    "using_expression",
    "allow_drop",
    "generated",
    "generated_expr",
    "identity",
    "is_identity",
    "index",
];
const VECTOR_KEYS: &[&str] = &[
    "backend",
    "collection_name",
    "dimension",
    "distance",
    "shard_count",
    "replica_count",
    "on_disk",
    "payload_schema_json",
    "hnsw_m",
    "hnsw_ef_construction",
];
const GRAPH_KEYS: &[&str] = &[
    "backend",
    "graph_name",
    "node_label",
    "id_field",
    "tenant_field",
    "edge_source_field",
    "edge_target_field",
    "payload_schema_json",
];
const DOCUMENT_KEYS: &[&str] = &[
    "backend",
    "database_name",
    "collection_name",
    "partition_key",
    "id_field",
    "tenant_field",
    "ttl_seconds",
    "payload_schema_json",
];
const TIMESERIES_KEYS: &[&str] = &[
    "backend",
    "database_name",
    "measurement_name",
    "time_field",
    "tenant_field",
    "tag_fields",
    "value_fields",
    "retention_days",
    "downsample_policy",
];
const COLUMN_STORE_KEYS: &[&str] = &[
    "backend",
    "database_name",
    "table_name",
    "partition_key",
    "sort_key",
    "compression",
    "ttl_seconds",
    "payload_schema_json",
];
const CACHE_KEYS: &[&str] = &[
    "backend",
    "key_pattern",
    "ttl_seconds",
    "write_through",
    "read_through",
    "eviction_policy",
    "cluster_env_key",
    "namespace",
];
const STORAGE_KEYS: &[&str] = &[
    "backend",
    "bucket_env_key",
    "key_prefix",
    "presigned_read",
    "presigned_write",
    "presigned_ttl_seconds",
    "server_side_encryption",
    "kms_key_id",
    "acl",
];
const MODEL_REGISTRY_KEYS: &[&str] = &[
    "backend",
    "experiment_name",
    "artifact_path",
    "auto_register",
    "stage",
    "metric_keys",
    "param_keys",
    "storage_uri_env",
];
const SECURITY_KEYS: &[&str] = &[
    "classification_level",
    "audit_writes",
    "audit_reads",
    "retention_days",
    "encryption_required",
];
const TABLE_SECURITY_KEYS: &[&str] = &[
    "tenant_isolation_mode",
    "project_isolation_mode",
    "tenant_column",
    "project_column",
    "rls_policy_template",
    "soft_delete_mode",
    "retention_class",
    "retention_days",
    "audit_mode",
    "encryption_profile",
    "pii_profile",
    "break_glass_visible",
    "export_eligible",
    "data_residency_policy_ref",
];
const DB_COLUMN_SECURITY_KEYS: &[&str] = &[
    "secret_classification",
    "output_view",
    "redaction_strategy",
    "tokenization_strategy",
    "hashing_strategy",
    "hashing_algorithm",
    "encryption_key_class",
    "searchable_encrypted",
];
const FIELD_SECURITY_SCALAR_KEYS: &[&str] = &["<scalar value>"];
const GENERIC_STORE_KEYS: &[&str] = &[
    "store_kind",
    "kind",
    "backend",
    "driver",
    "logical_name",
    "name",
    "database_name",
    "database",
    "namespace",
    "schema_name",
    "resource_name",
    "table_name",
    "collection_name",
    "graph_name",
    "measurement_name",
    "bucket",
    "dsn_env_key",
    "env_key",
    "dsn",
    "uri",
    "payload_schema_json",
    "options_json",
    "<backend-specific option>",
];

// Numeric (i32-parsed) subsets of the key lists above. Each entry must also
// appear in its option's `accepted_keys`; these slices are the single source
// of truth consulted by `is_numeric_annotation_key`.
const TABLE_NUMERIC_KEYS: &[&str] = &[
    "migration_order",
    "retention_days",
    "partition_premake",
    "partition_retention_months",
];
const VECTOR_NUMERIC_KEYS: &[&str] = &[
    "dimension",
    "shard_count",
    "replica_count",
    "hnsw_m",
    "hnsw_ef_construction",
];
const DOCUMENT_NUMERIC_KEYS: &[&str] = &["ttl_seconds"];
const TIMESERIES_NUMERIC_KEYS: &[&str] = &["retention_days"];
const COLUMN_STORE_NUMERIC_KEYS: &[&str] = &["ttl_seconds"];
const CACHE_NUMERIC_KEYS: &[&str] = &["ttl_seconds"];
const STORAGE_NUMERIC_KEYS: &[&str] = &["presigned_ttl_seconds"];
const SECURITY_NUMERIC_KEYS: &[&str] = &["retention_days"];
const TABLE_SECURITY_NUMERIC_KEYS: &[&str] = &["retention_days"];
const NO_NUMERIC_KEYS: &[&str] = &[];

const DOCUMENTED_OPTION_METADATA: &[ParserOptionMetadata] = &[
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.pg_table",
        kind: "table",
        option_type: "MessageOptions",
        target: "PostgreSQL",
        required: true,
        since_version: "0.1.0",
        description: "Marks a proto message as a mapped relational table.",
        example: r#"option (udb.core.common.v1.pg_table) = { table_name: "users" schema_name: "app" is_table: true };"#,
        accepted_keys: TABLE_KEYS,
        numeric_keys: TABLE_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.pg_column",
        kind: "column",
        option_type: "FieldOptions",
        target: "PostgreSQL",
        required: false,
        since_version: "0.1.0",
        description: "Maps a proto field to a SQL column.",
        example: r#"string email = 2 [(udb.core.common.v1.pg_column) = { column_name: "email" sql_type: "TEXT" }];"#,
        accepted_keys: COLUMN_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.vector_store",
        kind: "vector_store",
        option_type: "MessageOptions",
        target: "Qdrant / Milvus / Weaviate / pgvector / Pinecone / OpenSearch",
        required: false,
        since_version: "0.2.0",
        description: "Projects a message into a vector store.",
        example: r#"option (udb.core.common.v1.vector_store) = { backend: VECTOR_BACKEND_QDRANT collection_name: "users" dimension: 1536 };"#,
        accepted_keys: VECTOR_KEYS,
        numeric_keys: VECTOR_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.graph_store",
        kind: "graph_store",
        option_type: "MessageOptions",
        target: "Neo4j / Memgraph / ArangoDB",
        required: false,
        since_version: "0.2.0",
        description: "Projects a message into a graph store.",
        example: r#"option (udb.core.common.v1.graph_store) = { backend: GRAPH_BACKEND_NEO4J node_label: "User" id_field: "id" };"#,
        accepted_keys: GRAPH_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.document_store",
        kind: "document_store",
        option_type: "MessageOptions",
        target: "MongoDB / DynamoDB / Cosmos DB",
        required: false,
        since_version: "0.2.0",
        description: "Projects a message into a document store.",
        example: r#"option (udb.core.common.v1.document_store) = { backend: NOSQL_BACKEND_MONGODB collection_name: "users" id_field: "id" };"#,
        accepted_keys: DOCUMENT_KEYS,
        numeric_keys: DOCUMENT_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.nosql_store",
        kind: "nosql_store",
        option_type: "MessageOptions",
        target: "Generic NoSQL backends",
        required: false,
        since_version: "0.2.0",
        description: "Declares a generic NoSQL store binding.",
        example: r#"option (udb.core.common.v1.nosql_store) = { backend: "mongodb" collection_name: "users" };"#,
        accepted_keys: GENERIC_STORE_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.sql_store",
        kind: "sql_store",
        option_type: "MessageOptions",
        target: "Generic SQL backends",
        required: false,
        since_version: "0.2.0",
        description: "Declares a generic SQL store binding.",
        example: r#"option (udb.core.common.v1.sql_store) = { backend: "postgres" table_name: "users" };"#,
        accepted_keys: GENERIC_STORE_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.timeseries_store",
        kind: "timeseries_store",
        option_type: "MessageOptions",
        target: "TimescaleDB / InfluxDB / ClickHouse",
        required: false,
        since_version: "0.2.0",
        description: "Projects a message into a time-series store.",
        example: r#"option (udb.core.common.v1.timeseries_store) = { backend: TIMESERIES_BACKEND_TIMESCALEDB measurement_name: "usage" time_field: "created_at" };"#,
        accepted_keys: TIMESERIES_KEYS,
        numeric_keys: TIMESERIES_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.column_store",
        kind: "column_store",
        option_type: "MessageOptions",
        target: "ClickHouse / Cassandra / Bigtable",
        required: false,
        since_version: "0.2.0",
        description: "Projects a message into a column-oriented store.",
        example: r#"option (udb.core.common.v1.column_store) = { backend: COLUMN_BACKEND_CLICKHOUSE table_name: "events" partition_key: "tenant_id" };"#,
        accepted_keys: COLUMN_STORE_KEYS,
        numeric_keys: COLUMN_STORE_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.cache",
        kind: "cache",
        option_type: "MessageOptions",
        target: "Redis / Memcached / Dragonfly / in-process",
        required: false,
        since_version: "0.3.0",
        description: "Declares cache projection metadata for a message.",
        example: r#"option (udb.core.common.v1.cache) = { backend: CACHE_BACKEND_REDIS key_pattern: "user:{id}" ttl_seconds: 300 };"#,
        accepted_keys: CACHE_KEYS,
        numeric_keys: CACHE_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.storage",
        kind: "storage",
        option_type: "FieldOptions",
        target: "S3 / MinIO / GCS / Azure Blob / local",
        required: false,
        since_version: "0.2.0",
        description: "Marks a field as an object-storage key or URI.",
        example: r#"string object_key = 5 [(udb.core.common.v1.storage) = { backend: STORAGE_BACKEND_S3 bucket_env_key: "UDB_BUCKET" }];"#,
        accepted_keys: STORAGE_KEYS,
        numeric_keys: STORAGE_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.object_store",
        kind: "object_store",
        option_type: "MessageOptions",
        target: "Generic object stores",
        required: false,
        since_version: "0.2.0",
        description: "Declares a generic object-store binding.",
        example: r#"option (udb.core.common.v1.object_store) = { backend: "s3" bucket: "artifacts" };"#,
        accepted_keys: GENERIC_STORE_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.model_registry",
        kind: "model_registry",
        option_type: "MessageOptions",
        target: "MLflow / DVC / Hugging Face / BentoML / Triton / custom",
        required: false,
        since_version: "0.3.0",
        description: "Declares model registry or artifact repository metadata.",
        example: r#"option (udb.core.common.v1.model_registry) = { backend: MODEL_BACKEND_MLFLOW experiment_name: "ranking" };"#,
        accepted_keys: MODEL_REGISTRY_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.security",
        kind: "security",
        option_type: "MessageOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.3.0",
        description: "Attaches message-level security metadata.",
        example: r#"option (udb.core.common.v1.security) = { classification_level: "internal" audit_reads: true };"#,
        accepted_keys: SECURITY_KEYS,
        numeric_keys: SECURITY_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.column_security",
        kind: "db_column_security",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.3.0",
        description: "Attaches structured field redaction and secret handling metadata.",
        example: r#"string password_hash = 3 [(udb.core.common.v1.column_security) = { secret_classification: SECRET_CLASSIFICATION_CREDENTIAL output_view: OUTPUT_VIEW_STORAGE_ONLY }];"#,
        accepted_keys: DB_COLUMN_SECURITY_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.db_table_security",
        kind: "db_table_security",
        option_type: "MessageOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Attaches enterprise table isolation and retention metadata.",
        example: r#"option (udb.core.common.v1.db_table_security) = { tenant_column: "tenant_id" tenant_isolation_mode: "tenant" };"#,
        accepted_keys: TABLE_SECURITY_KEYS,
        numeric_keys: TABLE_SECURITY_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.db_column_security",
        kind: "db_column_security",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Attaches enterprise column redaction and secret handling metadata.",
        example: r#"string password_hash = 3 [(udb.core.common.v1.db_column_security) = { secret_classification: SECRET_CLASSIFICATION_CREDENTIAL output_view: OUTPUT_VIEW_STORAGE_ONLY }];"#,
        accepted_keys: DB_COLUMN_SECURITY_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.pii",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Marks a field as personally identifiable information.",
        example: r#"string email = 2 [(udb.core.common.v1.pii) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.encrypted_security",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Marks a field as encrypted for security metadata.",
        example: r#"string token = 3 [(udb.core.common.v1.encrypted_security) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.log_masked",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Masks a field in log output.",
        example: r#"string email = 2 [(udb.core.common.v1.log_masked) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.log_redacted",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Redacts a field in log output.",
        example: r#"string token = 3 [(udb.core.common.v1.log_redacted) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.sensitive",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Marks a field as sensitive and log-masked.",
        example: r#"string token = 3 [(udb.core.common.v1.sensitive) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.requires_consent",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Marks a field as requiring consent.",
        example: r#"string email = 2 [(udb.core.common.v1.requires_consent) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.data_purpose",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Records the purpose attached to a field.",
        example: r#"string email = 2 [(udb.core.common.v1.data_purpose) = "login"];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.retention_days",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Sets field-level retention metadata in days.",
        example: r#"string email = 2 [(udb.core.common.v1.retention_days) = 30];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.tokenized",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Marks a field as tokenized or blind-indexed.",
        example: r#"string account_number = 4 [(udb.core.common.v1.tokenized) = true];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.security_classification",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Sets field-level security classification metadata.",
        example: r#"string token = 3 [(udb.core.common.v1.security_classification) = SECRET_CLASSIFICATION_TOKEN];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.data_category",
        kind: "field_security_scalar",
        option_type: "FieldOptions",
        target: "UDB security layer",
        required: false,
        since_version: "0.4.0",
        description: "Sets field-level data category metadata.",
        example: r#"string email = 2 [(udb.core.common.v1.data_category) = DATA_CATEGORY_CONTACT];"#,
        accepted_keys: FIELD_SECURITY_SCALAR_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
    ParserOptionMetadata {
        option_name: "udb.core.common.v1.data_store",
        kind: "data_store",
        option_type: "MessageOptions",
        target: "Generic external stores",
        required: false,
        since_version: "0.4.0",
        description: "Declares a generic external data-store binding.",
        example: r#"option (udb.core.common.v1.data_store) = { store_kind: "search" backend: "opensearch" resource_name: "users" };"#,
        accepted_keys: GENERIC_STORE_KEYS,
        numeric_keys: NO_NUMERIC_KEYS,
    },
];

pub fn documented_option_metadata() -> &'static [ParserOptionMetadata] {
    DOCUMENTED_OPTION_METADATA
}

pub fn parser_option_kind_name(option_name: &str, namespace: &str) -> Option<&'static str> {
    option_kind(option_name, namespace).map(|kind| kind.as_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OptionValue {
    Scalar(String),
    Block(HashMap<String, Vec<OptionValue>>),
}

pub(super) fn option_kind(option_name: &str, namespace: &str) -> Option<OptionKind> {
    let option_name = normalize_option_name(option_name);
    let namespace = namespace.trim().trim_matches('.');
    let needle = |name: &str| {
        let suffix = format!(".{name}");
        let namespaced_match = option_name.ends_with(&suffix)
            && !option_name.starts_with("buf.validate.")
            && !option_name.starts_with("validate.");
        if namespace.is_empty() {
            // Match the bare name and UDB-style project-owned extension
            // namespaces (`udb.core.common.v1.pg_table`,
            // `acme.platform.udb.v9.table`, legacy `db.table`, etc.). Keep
            // known validation namespaces out so unrelated `.table`/`.column`
            // annotations are not promoted to database metadata.
            option_name == name || namespaced_match
        } else {
            option_name == format!("{namespace}.{name}")
        }
    };
    if needle("table") || needle("pg_table") {
        Some(OptionKind::Table)
    } else if needle("vector_store") {
        Some(OptionKind::VectorStore)
    } else if needle("graph_store") {
        Some(OptionKind::GraphStore)
    } else if needle("document_store") {
        Some(OptionKind::DocumentStore)
    } else if needle("nosql_store") || needle("no_sql_store") || needle("kv_store") {
        Some(OptionKind::NoSqlStore)
    } else if needle("timeseries_store") || needle("time_series_store") {
        Some(OptionKind::TimeSeriesStore)
    } else if needle("column_store") || needle("wide_column_store") || needle("columnar_store") {
        Some(OptionKind::ColumnStore)
    } else if needle("cache") {
        Some(OptionKind::Cache)
    } else if needle("storage") {
        Some(OptionKind::Storage)
    } else if needle("object_store") || needle("blob_store") {
        Some(OptionKind::ObjectStore)
    } else if matches!(
        option_name.as_str(),
        "pii"
            | "encrypted_security"
            | "log_masked"
            | "log_redacted"
            | "sensitive"
            | "requires_consent"
            | "data_purpose"
            | "retention_days"
            | "tokenized"
            | "security_classification"
            | "data_category"
    ) || (option_name.starts_with("udb.core.common.v1.")
        && matches!(
            option_name
                .strip_prefix("udb.core.common.v1.")
                .unwrap_or_default(),
            "pii"
                | "encrypted_security"
                | "log_masked"
                | "log_redacted"
                | "sensitive"
                | "requires_consent"
                | "data_purpose"
                | "retention_days"
                | "tokenized"
                | "security_classification"
                | "data_category"
        ))
    {
        Some(OptionKind::FieldSecurityScalar)
    } else if needle("db_table_security") || needle("table_security") {
        Some(OptionKind::TableSecurity)
    } else if needle("db_column_security") || needle("column_security") {
        Some(OptionKind::ColumnSecurity)
    } else if needle("security") {
        Some(OptionKind::Security)
    } else if needle("model_registry") {
        Some(OptionKind::ModelRegistry)
    } else if needle("sql_store") || needle("relational_store") {
        Some(OptionKind::SqlStore)
    } else if needle("data_store") || needle("external_store") || needle("search_store") {
        Some(OptionKind::GenericStore)
    } else if needle("column") || needle("pg_column") {
        Some(OptionKind::Column)
    } else {
        None
    }
}

fn normalize_option_name(option_name: &str) -> String {
    option_name
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim_start_matches('.')
        .to_string()
}

pub(super) fn is_numeric_annotation_key(key: &str) -> bool {
    let key = key.trim().to_ascii_lowercase();
    // Single-sourced from the metadata table: a key is numeric iff some
    // documented option lists it among its `numeric_keys`. This cannot drift
    // from the metadata because there is no separate hand-maintained list.
    DOCUMENTED_OPTION_METADATA
        .iter()
        .flat_map(|meta| meta.numeric_keys.iter())
        .any(|numeric| *numeric == key)
}

/// Declarative scalar-option applier.
///
/// Iterates the scalar entries of `$values` once and dispatches each recognized
/// key to its setter expression. The setter receives the borrowed target
/// (`$target`) and the owned `String` value via the bound identifier named
/// before `=>`. Mirrors the hand-written `match key.as_str()` bodies it
/// replaces — only recognized keys are touched; every other key is ignored.
///
/// ```ignore
/// apply_scalar_options!(out, values, value => {
///     "backend" => out.backend = normalize_enum(&value),
///     "dimension" => out.dimension = parse_i32(&value),
/// });
/// ```
macro_rules! apply_scalar_options {
    ($target:expr, $values:expr, $value:ident => { $( $($key:literal)|+ => $setter:expr ),* $(,)? }) => {
        for (key, $value) in scalar_entries($values) {
            match key.as_str() {
                $( $($key)|+ => $setter, )*
                _ => {}
            }
        }
    };
}

pub(super) fn apply_table_security_values(
    schema: &mut ProtoSchema,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    apply_scalar_options!(schema, values, value => {
        "tenant_isolation_mode" => schema.table_security.tenant_isolation_mode = normalize_enum(&value).to_ascii_lowercase(),
        "project_isolation_mode" => schema.table_security.project_isolation_mode = normalize_enum(&value).to_ascii_lowercase(),
        "tenant_column" => schema.table_security.tenant_column = to_snake_case(&value),
        "project_column" => schema.table_security.project_column = to_snake_case(&value),
        "rls_policy_template" => schema.table_security.rls_policy_template = value.trim().to_string(),
        "soft_delete_mode" => schema.table_security.soft_delete_mode = normalize_enum(&value).to_ascii_lowercase(),
        "retention_class" => schema.table_security.retention_class = normalize_enum(&value).to_ascii_lowercase(),
        "retention_days" => schema.table_security.retention_days = parse_i32(&value),
        "audit_mode" => schema.table_security.audit_mode = normalize_enum(&value).to_ascii_lowercase(),
        "encryption_profile" => schema.table_security.encryption_profile = value.trim().to_string(),
        "pii_profile" => schema.table_security.pii_profile = value.trim().to_string(),
        "break_glass_visible" => schema.table_security.break_glass_visible = parse_bool(&value),
        "export_eligible" => schema.table_security.export_eligible = parse_bool(&value),
        "data_residency_policy_ref" => schema.table_security.data_residency_policy_ref = value.trim().to_string(),
    });
}

pub(super) fn apply_table_values(
    schema: &mut ProtoSchema,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    apply_scalar_options!(schema, values, value => {
        "table_name" => schema.table_name = value,
        "schema_name" => schema.declared_schema_name = value,
        "migration_order" => schema.migration_order = parse_i32(&value),
        "is_table" => schema.is_table = parse_bool(&value),
        "comment" => schema.table_comment = value,
        "soft_delete" => schema.soft_delete = parse_bool(&value),
        "soft_delete_column" => schema.soft_delete_column = to_snake_case(&value),
        "audit_fields" => schema.audit_fields = parse_bool(&value),
        "enable_rls" => schema.enable_rls = parse_bool(&value),
        "force_rls" => schema.force_rls = parse_bool(&value),
        "unlogged" => schema.unlogged = parse_bool(&value),
        "tablespace" => schema.tablespace = value,
        "partition_strategy" => schema.partition_strategy = normalize_enum(&value),
        "partition_by" => schema.partition_strategy = normalize_enum(&value),
        "partition_column" => schema.partition_column = value,
        "retention_days" => schema.retention_days = parse_i32(&value),
        "partition_interval" => schema.partition_interval = normalize_enum(&value),
        "partition_premake" => schema.partition_premake = parse_i32(&value),
        "partition_default" => schema.partition_default = parse_bool(&value),
        "partition_retention_months" => schema.partition_retention_months = parse_i32(&value),
        "replica_hint" => schema.replica_hint = normalize_enum(&value).to_ascii_lowercase(),
        "cdc_topic" => schema.cdc_topic = value.trim().to_string(),
        "required_scope" => schema.required_scope = value.trim().to_string(),
        "previous_table_name" => schema.previous_table_name = to_snake_case(&value),
        "allow_drop" => schema.allow_drop = parse_bool(&value),
    });

    for (key, list) in values {
        match key.as_str() {
            "indexes" | "index" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.indexes.push(index_from_values(None, block));
                    }
                }
            }
            "foreign_keys" | "foreign_key" | "fk" | "fks" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.foreign_keys.push(foreign_key_from_values("", block));
                    }
                }
            }
            "rls_policies" | "rls_policy" | "policy" | "policies" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.rls_policies.push(rls_policy_from_values(block));
                    }
                }
            }
            "extensions" | "extension" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.extensions.push(extension_from_values(block));
                    }
                }
            }
            "materialized_views" | "materialized_view" | "matview" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema
                            .materialized_views
                            .push(materialized_view_from_values(block));
                    }
                }
            }
            "triggers" | "trigger" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.triggers.push(trigger_from_values(block));
                    }
                }
            }
            "sql_artifacts" | "sql_artifact" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.sql_artifacts.push(sql_artifact_from_values(block));
                    }
                }
            }
            "vector_store" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        schema.vector_store = Some(vector_from_values(block));
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn apply_column_values(
    column: &mut ProtoColumn,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    for (key, list) in values {
        if matches!(key.as_str(), "foreign_key" | "index") {
            continue;
        }
        for value in list {
            if let OptionValue::Scalar(value) = value {
                apply_column_value(column, key, value);
            }
        }
    }

    for (key, list) in values {
        match key.as_str() {
            "foreign_key" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        column.foreign_key =
                            Some(foreign_key_from_values(column.column_name.as_str(), block));
                    }
                }
            }
            "index" => {
                for value in list {
                    if let OptionValue::Block(block) = value {
                        column
                            .indexes
                            .push(index_from_values(Some(column.column_name.as_str()), block));
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn apply_column_value(column: &mut ProtoColumn, key: &str, value: &str) {
    match key {
        "column_name" => column.column_name = to_snake_case(value),
        "sql_type" => column.sql_type = value.to_string(),
        "not_null" => column.not_null = parse_bool(value),
        // `nullable: false` is the inverse spelling of `not_null: true`; honoring
        // it (it was previously silently ignored → columns rendered NULLable).
        "nullable" => column.not_null = !parse_bool(value),
        "unique" => column.unique = parse_bool(value),
        "primary_key" | "is_primary_key" => column.is_primary = parse_bool(value),
        "auto_increment" => column.auto_increment = parse_bool(value),
        "default_value" => column.default_value = value.to_string(),
        "check_constraint" => column.check_constraint = value.to_string(),
        "collation" => column.collation = value.to_string(),
        "enum_values" | "enum_value" => column.enum_values.push(value.to_string()),
        "comment" => column.comment = value.to_string(),
        "exclude_from_insert" => column.exclude_from_insert = parse_bool(value),
        "exclude_from_update" => column.exclude_from_update = parse_bool(value),
        "encrypted" | "encrypt" => column.encrypted = parse_bool(value),
        "is_json" => column.is_json = parse_bool(value),
        "is_jsonb" => column.is_jsonb = parse_bool(value),
        "json_path_ops" => column.json_path_ops = parse_bool(value),
        "is_tsvector" => column.is_tsvector = parse_bool(value),
        "tsvector_language" => column.tsvector_language = value.to_string(),
        "tsvector_source_columns" | "tsvector_source_column" => {
            column.tsvector_source_columns.push(value.to_string())
        }
        "trigram_index" => column.trigram_index = parse_bool(value),
        "references" | "foreign_key" => column.references = value.to_string(),
        "tenant_column" => column.is_tenant = parse_bool(value),
        "project_column" => column.is_project = parse_bool(value),
        "on_delete" => column.on_delete = normalize_referential_action(value),
        "on_update" => column.on_update = normalize_referential_action(value),
        "previous_column_name" => column.previous_column_name = to_snake_case(value),
        "backfill_sql" => column.backfill_sql = value.to_string(),
        "using_expression" => column.using_expression = value.to_string(),
        "allow_drop" => column.allow_drop = parse_bool(value),
        "generated" => column.generated = parse_bool(value),
        "generated_expr" => column.generated_expr = value.to_string(),
        "identity" | "is_identity" => column.is_identity = parse_bool(value),
        _ => {}
    }
}

fn foreign_key_from_values(
    local_column: &str,
    values: &HashMap<String, Vec<OptionValue>>,
) -> ProtoForeignKey {
    let mut fk = ProtoForeignKey::default();
    if !local_column.trim().is_empty() {
        fk.columns.push(local_column.to_string());
    }
    apply_scalar_options!(fk, values, value => {
        "columns" | "column" | "local_column" | "local_columns" => {
            fk.columns.push(to_snake_case(&value))
        },
        "references_table" => fk.ref_table = to_snake_case(&value),
        "references_column" | "references_columns" => {
            fk.ref_columns.push(to_snake_case(&value))
        },
        "references_schema" => fk.ref_schema = value.to_lowercase(),
        "on_delete" => fk.on_delete = normalize_referential_action(&value),
        "on_update" => fk.on_update = normalize_referential_action(&value),
        "constraint_name" => fk.name = value,
        "not_valid" => fk.not_valid = parse_bool(&value),
        "deferrable" => fk.deferrable = parse_bool(&value),
        "initially_deferred" => fk.initially_deferred = parse_bool(&value),
    });
    // Preserve parse-order so column[i] corresponds to ref_column[i].
    // A sort here would desync local columns from ref_columns, producing incorrect FK SQL.
    let mut seen = std::collections::HashSet::new();
    fk.columns
        .retain(|col| !col.trim().is_empty() && seen.insert(col.clone()));
    fk
}

pub(super) fn foreign_key_from_reference(column: &ProtoColumn) -> Option<ProtoForeignKey> {
    let reference = column.references.trim();
    if reference.is_empty() {
        return None;
    }
    let (target, ref_col) = if let Some(start) = reference.find('(') {
        let end = reference[start + 1..]
            .find(')')
            .map(|idx| start + 1 + idx)
            .unwrap_or(reference.len());
        (&reference[..start], reference[start + 1..end].trim())
    } else {
        (reference, "")
    };
    let parts = target
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (ref_schema, ref_table) = match parts.as_slice() {
        [table] => ("", *table),
        [schema, table] => (*schema, *table),
        _ => return None,
    };
    let ref_column = if ref_col.is_empty() {
        column.column_name.as_str()
    } else {
        ref_col
    };
    Some(ProtoForeignKey {
        columns: vec![column.column_name.clone()],
        ref_schema: ref_schema.to_ascii_lowercase(),
        ref_table: to_snake_case(ref_table),
        ref_columns: vec![to_snake_case(ref_column)],
        on_delete: column.on_delete.clone(),
        on_update: column.on_update.clone(),
        ..ProtoForeignKey::default()
    })
}

fn rls_policy_from_values(values: &HashMap<String, Vec<OptionValue>>) -> RlsPolicy {
    let mut policy = RlsPolicy {
        permissive: true,
        ..RlsPolicy::default()
    };
    apply_scalar_options!(policy, values, value => {
        "policy_name" | "name" => policy.name = value,
        "command" => policy.command = normalize_enum(&value),
        "using" | "using_expression" => policy.using_expression = value,
        "with_check" | "check_expression" => policy.with_check = value,
        "permissive" => policy.permissive = parse_bool(&value),
    });
    policy
}

pub(super) fn apply_schema_security_values(
    security: &mut ProtoSecurity,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    apply_scalar_options!(security, values, value => {
        "classification_level" => security.classification_level = normalize_enum(&value),
        "audit_writes" => security.audit_writes = parse_bool(&value),
        "audit_reads" => security.audit_reads = parse_bool(&value),
        "retention_days" => security.retention_days = parse_i32(&value),
        "encryption_required" => security.encryption_required = parse_bool(&value),
    });
}

pub(super) fn apply_column_security_values(
    security: &mut ProtoColumnSecurity,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    apply_scalar_options!(security, values, value => {
        "is_pii" => security.is_pii = parse_bool(&value),
        "pii_kind" => {
            security.is_pii = !value.trim().is_empty();
            security.data_class = normalize_enum(&value);
        },
        "is_encrypted" => security.is_encrypted = parse_bool(&value),
        "is_blind_index" => security.is_blind_index = parse_bool(&value),
        "mask_in_logs" => security.mask_in_logs = parse_bool(&value),
        "data_class" => security.data_class = normalize_enum(&value),
        "consent_required" => security.consent_required = parse_bool(&value),
        "retention_days" => security.retention_days = parse_i32(&value),
    });
}

pub(super) fn apply_column_security_scalar_option(
    security: &mut ProtoColumnSecurity,
    option_name: &str,
    value: &str,
) {
    let option_name = normalize_option_name(option_name);
    let key = option_name
        .strip_prefix("udb.core.common.v1.")
        .unwrap_or(option_name.as_str());
    match key {
        "pii" => {
            security.is_pii = parse_bool(value);
            if security.is_pii && security.data_class.trim().is_empty() {
                security.data_class = "PERSONAL".to_string();
            }
        }
        "encrypted_security" => security.is_encrypted = parse_bool(value),
        "log_masked" | "log_redacted" => security.mask_in_logs = parse_bool(value),
        "sensitive" => {
            if parse_bool(value) {
                security.mask_in_logs = true;
                if security.data_class.trim().is_empty() {
                    security.data_class = "SENSITIVE".to_string();
                }
            }
        }
        "requires_consent" => security.consent_required = parse_bool(value),
        "retention_days" => security.retention_days = parse_i32(value),
        "tokenized" => security.is_blind_index = parse_bool(value),
        "security_classification" | "data_category" => security.data_class = normalize_enum(value),
        "data_purpose" => {
            if security.data_class.trim().is_empty() && !value.trim().is_empty() {
                security.data_class = "PURPOSE_LIMITED".to_string();
            }
        }
        _ => {}
    }
}

pub(super) fn apply_db_column_security_values(
    security: &mut ProtoColumnSecurity,
    values: &HashMap<String, Vec<OptionValue>>,
) {
    apply_scalar_options!(security, values, value => {
        "secret_classification" => {
            let normalized = normalize_enum(&value)
                .trim_start_matches("SECRET_CLASSIFICATION_")
                .to_string();
            if !normalized.is_empty() && normalized != "UNSPECIFIED" {
                security.data_class = normalized;
            }
        },
        "output_view" => {
            let normalized = normalize_enum(&value);
            if matches!(
                normalized.as_str(),
                "OUTPUT_VIEW_STORAGE_ONLY" | "OUTPUT_VIEW_NEVER"
            ) {
                security.mask_in_logs = true;
            }
        },
        "redaction_strategy" => {
            let normalized = normalize_enum(&value);
            if !matches!(
                normalized.as_str(),
                "" | "REDACTION_STRATEGY_UNSPECIFIED" | "REDACTION_STRATEGY_NONE"
            ) {
                security.mask_in_logs = true;
            }
        },
        "tokenization_strategy" => {
            if !value.trim().is_empty() {
                security.is_blind_index = true;
            }
        },
        "hashing_strategy" | "hashing_algorithm" => {
            if !value.trim().is_empty() {
                security.is_blind_index = true;
            }
        },
        "encryption_key_class" => {
            if !value.trim().is_empty() {
                security.is_encrypted = true;
            }
        },
        "searchable_encrypted" => {
            let enabled = parse_bool(&value);
            security.is_encrypted |= enabled;
            security.is_blind_index |= enabled;
        },
    });
}

fn extension_from_values(values: &HashMap<String, Vec<OptionValue>>) -> DbExtension {
    let mut out = DbExtension::default();
    apply_scalar_options!(out, values, value => {
        "name" | "extension_name" => out.name = value,
        "schema" | "schema_name" => out.schema = value,
        "version" => out.version = value,
    });
    out
}

fn materialized_view_from_values(values: &HashMap<String, Vec<OptionValue>>) -> MaterializedView {
    let mut out = MaterializedView::default();
    apply_scalar_options!(out, values, value => {
        "name" | "view_name" => out.name = to_snake_case(&value),
        "schema" | "schema_name" => out.schema = value.to_ascii_lowercase(),
        "query" | "sql" => out.query = value,
        "with_data" => out.with_data = parse_bool(&value),
    });
    out
}

fn trigger_from_values(values: &HashMap<String, Vec<OptionValue>>) -> DbTrigger {
    let mut out = DbTrigger {
        for_each: DEFAULT_TRIGGER_FOR_EACH.to_string(),
        ..DbTrigger::default()
    };
    apply_scalar_options!(out, values, value => {
        "name" | "trigger_name" => out.name = to_snake_case(&value),
        "event" => out.event = normalize_enum(&value),
        "timing" => out.timing = normalize_enum(&value),
        "function" | "function_name" => out.function = value,
        "for_each" => out.for_each = normalize_enum(&value),
        "when" | "when_clause" => out.when_clause = value,
    });
    out
}

fn sql_artifact_from_values(values: &HashMap<String, Vec<OptionValue>>) -> SqlArtifact {
    let mut out = SqlArtifact::default();
    apply_scalar_options!(out, values, value => {
        "name" | "artifact_name" => out.name = to_snake_case(&value),
        "backend" => out.backend = normalize_backend(&value),
        "phase" => out.phase = value.to_ascii_lowercase(),
        "sql" | "body" => out.sql = value,
        "file" | "path" => out.file = value,
        "checksum_sha256" | "sha256" => out.checksum_sha256 = value,
        "requires_review" => out.requires_review = parse_bool(&value),
    });
    out
}

fn index_from_values(
    local_column: Option<&str>,
    values: &HashMap<String, Vec<OptionValue>>,
) -> ProtoIndex {
    let mut index = ProtoIndex {
        columns: local_column
            .map(|col| vec![col.to_string()])
            .unwrap_or_default(),
        index_type: DEFAULT_INDEX_TYPE.to_string(),
        ..ProtoIndex::default()
    };
    let local = local_column.unwrap_or_default();
    apply_scalar_options!(index, values, value => {
        "index_name" => index.name = value,
        "index_type" => index.index_type = normalize_index_type(&value),
        "unique" => index.unique = parse_bool(&value),
        "composite_fields" | "columns" | "column" => {
            // composite_fields entries may be:
            //   (a) simple column names: "col_a, col_b" or separate repeated fields
            //   (b) SQL expressions with function calls that contain commas
            //       themselves: "date_trunc('day', usage_date)"
            // Only split on commas that are at top-level (depth 0),
            // i.e. not inside parentheses or single-quoted strings.
            for part in split_top_level_commas(&value) {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                // Preserve SQL expressions as-is; only snake_case plain identifiers.
                let col = if part.contains('(') || part.contains('\'') {
                    part.to_string()
                } else {
                    to_snake_case(part)
                };
                if !col.is_empty() && col != local {
                    index.columns.push(col);
                }
            }
        },
        "include_columns" => index.include_columns.push(to_snake_case(&value)),
        "index_method" => index.index_method = value,
        "where_clause" => index.where_clause = value,
        "operator_class" => index.operator_class = value,
        "concurrent" | "concurrently" => index.concurrent = parse_bool(&value),
    });
    for (key, list) in values {
        if key == "index_params" || key == "index_param" {
            for value in list {
                if let OptionValue::Block(block) = value {
                    let param_key = first_scalar(block, "key");
                    let param_value = first_scalar(block, "value");
                    if !param_key.trim().is_empty() {
                        index.index_params.push(StoreOption {
                            key: param_key,
                            value: param_value,
                        });
                    }
                }
            }
        }
    }
    index
}

/// Split `s` on commas that are at nesting depth 0 (not inside parentheses
/// or single-quoted strings). This lets composite_fields entries contain SQL
/// function calls such as `date_trunc('day', requested_at)` without being
/// incorrectly split at the comma inside the function call.
fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut start = 0;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                // Toggle single-quote context (naively; good enough for identifiers).
                in_single_quote = !in_single_quote;
            }
            b'(' if !in_single_quote => depth += 1,
            b')' if !in_single_quote => depth -= 1,
            b',' if !in_single_quote && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&s[start..]);
    parts
}

fn first_scalar(values: &HashMap<String, Vec<OptionValue>>, key: &str) -> String {
    values
        .get(key)
        .and_then(|list| {
            list.iter().find_map(|value| match value {
                OptionValue::Scalar(value) => Some(value.clone()),
                OptionValue::Block(_) => None,
            })
        })
        .unwrap_or_default()
}

pub(super) fn vector_from_values(values: &HashMap<String, Vec<OptionValue>>) -> VectorStore {
    let mut out = VectorStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "collection_name" => out.collection_name = value,
        "dimension" => out.dimension = parse_i32(&value),
        "distance" => out.distance = normalize_enum(&value),
        "shard_count" => out.shard_count = parse_i32(&value),
        "replica_count" => out.replica_count = parse_i32(&value),
        "on_disk" => out.on_disk = parse_bool(&value),
        "payload_schema_json" => out.payload_schema_json = value,
        "hnsw_m" => out.hnsw_m = parse_i32(&value),
        "hnsw_ef_construction" => out.hnsw_ef_construction = parse_i32(&value),
    });
    out
}

pub(super) fn graph_from_values(values: &HashMap<String, Vec<OptionValue>>) -> GraphStore {
    let mut out = GraphStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "graph_name" => out.graph_name = value,
        "node_label" => out.node_label = value,
        "id_field" => out.id_field = to_snake_case(&value),
        "tenant_field" => out.tenant_field = to_snake_case(&value),
        "edge_source_field" => out.edge_source_field = to_snake_case(&value),
        "edge_target_field" => out.edge_target_field = to_snake_case(&value),
        "payload_schema_json" => out.payload_schema_json = value,
    });
    out
}

pub(super) fn document_store_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> DocumentStore {
    let mut out = DocumentStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "database_name" => out.database_name = value,
        "collection_name" => out.collection_name = value,
        "partition_key" => out.partition_key = to_snake_case(&value),
        "id_field" => out.id_field = to_snake_case(&value),
        "tenant_field" => out.tenant_field = to_snake_case(&value),
        "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
        "payload_schema_json" => out.payload_schema_json = value,
    });
    out
}

pub(super) fn timeseries_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> TimeSeriesStore {
    let mut out = TimeSeriesStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "database_name" => out.database_name = value,
        "measurement_name" => out.measurement_name = value,
        "time_field" => out.time_field = to_snake_case(&value),
        "tenant_field" => out.tenant_field = to_snake_case(&value),
        "tag_fields" => out.tag_fields.push(to_snake_case(&value)),
        "value_fields" => out.value_fields.push(to_snake_case(&value)),
        "retention_days" => out.retention_days = parse_i32(&value),
        "downsample_policy" => out.downsample_policy = value,
    });
    out
}

pub(super) fn cache_from_values(values: &HashMap<String, Vec<OptionValue>>) -> CacheStore {
    let mut out = CacheStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "key_pattern" => out.key_pattern = value,
        "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
        "write_through" => out.write_through = parse_bool(&value),
        "read_through" => out.read_through = parse_bool(&value),
        "eviction_policy" => out.eviction_policy = value,
        "cluster_env_key" => out.cluster_env_key = value,
        "namespace" => out.namespace = value,
    });
    out
}

pub(super) fn storage_from_values(values: &HashMap<String, Vec<OptionValue>>) -> StorageField {
    let mut out = StorageField::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "bucket_env_key" => out.bucket_env_key = value,
        "key_prefix" => out.key_prefix = value,
        "presigned_read" => out.presigned_read = parse_bool(&value),
        "presigned_write" => out.presigned_write = parse_bool(&value),
        "presigned_ttl_seconds" => out.presigned_ttl_seconds = parse_i32(&value),
        "server_side_encryption" => out.server_side_encryption = parse_bool(&value),
        "kms_key_id" => out.kms_key_id = value,
        "acl" => out.acl = value,
    });
    out
}

pub(super) fn model_registry_from_values(
    values: &HashMap<String, Vec<OptionValue>>,
) -> ModelRegistry {
    let mut out = ModelRegistry::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "experiment_name" => out.experiment_name = value,
        "artifact_path" => out.artifact_path = value,
        "auto_register" => out.auto_register = parse_bool(&value),
        "stage" => out.stage = value,
        "metric_keys" => out.metric_keys.push(value),
        "param_keys" => out.param_keys.push(value),
        "storage_uri_env" => out.storage_uri_env = value,
    });
    out
}

pub(super) fn column_store_from_values(values: &HashMap<String, Vec<OptionValue>>) -> ColumnStore {
    let mut out = ColumnStore::default();
    apply_scalar_options!(out, values, value => {
        "backend" => out.backend = normalize_enum(&value),
        "database_name" => out.database_name = value,
        "table_name" => out.table_name = to_snake_case(&value),
        "partition_key" => out.partition_key = to_snake_case(&value),
        "sort_key" => out.sort_key = to_snake_case(&value),
        "compression" => out.compression = value,
        "ttl_seconds" => out.ttl_seconds = parse_i32(&value),
        "payload_schema_json" => out.payload_schema_json = value,
    });
    out
}

pub(super) fn generic_store_from_values(
    default_kind: &str,
    values: &HashMap<String, Vec<OptionValue>>,
) -> GenericStore {
    let mut out = GenericStore {
        store_kind: default_kind.to_string(),
        ..GenericStore::default()
    };
    for (key, value) in scalar_entries(values) {
        match key.as_str() {
            "store_kind" | "kind" => out.store_kind = normalize_store_kind(&value),
            "backend" | "driver" => out.backend = normalize_backend(&value),
            "logical_name" | "name" => out.logical_name = value,
            "database_name" | "database" => out.database_name = value,
            "namespace" | "schema_name" => out.namespace = value,
            "resource_name" | "table_name" | "collection_name" | "graph_name"
            | "measurement_name" | "bucket" => out.resource_name = value,
            "dsn_env_key" | "env_key" => out.dsn_env_key = value,
            "dsn" | "uri" => out.dsn = value,
            "payload_schema_json" | "options_json" => out.payload_schema_json = value,
            _ => out.options.push(StoreOption { key, value }),
        }
    }
    out
}

fn scalar_entries(values: &HashMap<String, Vec<OptionValue>>) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (key, list) in values {
        for value in list {
            if let OptionValue::Scalar(value) = value {
                out.push((key.clone(), value.clone()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The canonical set of numeric annotation keys. `is_numeric_annotation_key`
    /// must agree exactly with this set, and so must the per-option
    /// `numeric_keys` slices in `DOCUMENTED_OPTION_METADATA`. Neither side can
    /// drift without failing one of the assertions below.
    const EXPECTED_NUMERIC_KEYS: &[&str] = &[
        "dimension",
        "hnsw_ef_construction",
        "hnsw_m",
        "migration_order",
        "partition_premake",
        "partition_retention_months",
        "presigned_ttl_seconds",
        "replica_count",
        "retention_days",
        "shard_count",
        "ttl_seconds",
    ];

    #[test]
    fn numeric_keys_derived_from_metadata_match_documented_eleven() {
        // Set built from the metadata table (what `is_numeric_annotation_key`
        // consults) must equal exactly the documented 11 keys.
        let derived: BTreeSet<&str> = DOCUMENTED_OPTION_METADATA
            .iter()
            .flat_map(|meta| meta.numeric_keys.iter().copied())
            .collect();
        let expected: BTreeSet<&str> = EXPECTED_NUMERIC_KEYS.iter().copied().collect();

        assert_eq!(
            derived, expected,
            "metadata-derived numeric keys drifted from the documented 11"
        );
        assert_eq!(expected.len(), 11, "expected exactly 11 numeric keys");

        // The public predicate is consistent with the same set, and every
        // key it marks numeric is genuinely parsed as a number (same i32
        // contract the parser's numeric-value lint enforces, via parse_i32).
        for key in &expected {
            assert!(
                is_numeric_annotation_key(key),
                "`{key}` should be recognized as numeric"
            );
            assert_eq!(
                parse_i32("42"),
                42,
                "numeric key `{key}` must parse its value as i32"
            );
            assert!(
                "42".trim().parse::<i32>().is_ok(),
                "numeric key `{key}` value must satisfy the i32 lint contract"
            );
        }
    }

    #[test]
    fn metadata_numeric_keys_are_subset_of_accepted_keys() {
        // A numeric key must also be an accepted key of its own option, so the
        // numeric flag describes a real parsed key rather than a typo.
        for meta in DOCUMENTED_OPTION_METADATA {
            for numeric in meta.numeric_keys {
                assert!(
                    meta.accepted_keys.contains(numeric),
                    "numeric key `{}` is not in accepted_keys of `{}`",
                    numeric,
                    meta.option_name
                );
            }
        }
    }

    #[test]
    fn non_numeric_keys_are_not_treated_as_numeric() {
        for key in [
            "table_name",
            "backend",
            "collection_name",
            "tenant_column",
            "ttl",
            "",
        ] {
            assert!(
                !is_numeric_annotation_key(key),
                "`{key}` must not be numeric"
            );
        }
    }
}
