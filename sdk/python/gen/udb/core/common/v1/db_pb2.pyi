from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PartitionStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PARTITION_STRATEGY_UNSPECIFIED: _ClassVar[PartitionStrategy]
    PARTITION_STRATEGY_NONE: _ClassVar[PartitionStrategy]
    PARTITION_STRATEGY_RANGE_YEAR: _ClassVar[PartitionStrategy]
    PARTITION_STRATEGY_RANGE_MONTH: _ClassVar[PartitionStrategy]
    PARTITION_STRATEGY_LIST: _ClassVar[PartitionStrategy]
    PARTITION_STRATEGY_HASH: _ClassVar[PartitionStrategy]

class ReferentialAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REFERENTIAL_ACTION_UNSPECIFIED: _ClassVar[ReferentialAction]
    REFERENTIAL_ACTION_NO_ACTION: _ClassVar[ReferentialAction]
    REFERENTIAL_ACTION_RESTRICT: _ClassVar[ReferentialAction]
    REFERENTIAL_ACTION_CASCADE: _ClassVar[ReferentialAction]
    REFERENTIAL_ACTION_SET_NULL: _ClassVar[ReferentialAction]
    REFERENTIAL_ACTION_SET_DEFAULT: _ClassVar[ReferentialAction]

class IndexType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_TYPE_UNSPECIFIED: _ClassVar[IndexType]
    INDEX_TYPE_NONE: _ClassVar[IndexType]
    INDEX_TYPE_BTREE: _ClassVar[IndexType]
    INDEX_TYPE_HASH: _ClassVar[IndexType]
    INDEX_TYPE_GIN: _ClassVar[IndexType]
    INDEX_TYPE_GIST: _ClassVar[IndexType]
    INDEX_TYPE_BRIN: _ClassVar[IndexType]
    INDEX_TYPE_HNSW: _ClassVar[IndexType]
    INDEX_TYPE_IVF: _ClassVar[IndexType]
    INDEX_TYPE_IVFPQ: _ClassVar[IndexType]

class StorageBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STORAGE_BACKEND_UNSPECIFIED: _ClassVar[StorageBackendType]
    STORAGE_BACKEND_S3: _ClassVar[StorageBackendType]
    STORAGE_BACKEND_MINIO: _ClassVar[StorageBackendType]
    STORAGE_BACKEND_GCS: _ClassVar[StorageBackendType]
    STORAGE_BACKEND_AZURE_BLOB: _ClassVar[StorageBackendType]
    STORAGE_BACKEND_LOCAL: _ClassVar[StorageBackendType]

class VectorBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR_BACKEND_UNSPECIFIED: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_QDRANT: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_MILVUS: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_WEAVIATE: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_PGVECTOR: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_PINECONE: _ClassVar[VectorBackendType]
    VECTOR_BACKEND_OPENSEARCH: _ClassVar[VectorBackendType]

class VectorDistanceMetric(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR_DISTANCE_UNSPECIFIED: _ClassVar[VectorDistanceMetric]
    VECTOR_DISTANCE_COSINE: _ClassVar[VectorDistanceMetric]
    VECTOR_DISTANCE_DOT: _ClassVar[VectorDistanceMetric]
    VECTOR_DISTANCE_EUCLIDEAN: _ClassVar[VectorDistanceMetric]
    VECTOR_DISTANCE_MANHATTAN: _ClassVar[VectorDistanceMetric]

class CacheBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CACHE_BACKEND_UNSPECIFIED: _ClassVar[CacheBackendType]
    CACHE_BACKEND_REDIS: _ClassVar[CacheBackendType]
    CACHE_BACKEND_MEMCACHED: _ClassVar[CacheBackendType]
    CACHE_BACKEND_IN_PROCESS: _ClassVar[CacheBackendType]
    CACHE_BACKEND_DRAGONFLY: _ClassVar[CacheBackendType]

class GraphBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GRAPH_BACKEND_UNSPECIFIED: _ClassVar[GraphBackendType]
    GRAPH_BACKEND_NEO4J: _ClassVar[GraphBackendType]
    GRAPH_BACKEND_MEMGRAPH: _ClassVar[GraphBackendType]
    GRAPH_BACKEND_ARANGODB: _ClassVar[GraphBackendType]

class NoSqlBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOSQL_BACKEND_UNSPECIFIED: _ClassVar[NoSqlBackendType]
    NOSQL_BACKEND_MONGODB: _ClassVar[NoSqlBackendType]
    NOSQL_BACKEND_DYNAMODB: _ClassVar[NoSqlBackendType]
    NOSQL_BACKEND_COSMOSDB: _ClassVar[NoSqlBackendType]

class TimeSeriesBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TIMESERIES_BACKEND_UNSPECIFIED: _ClassVar[TimeSeriesBackendType]
    TIMESERIES_BACKEND_TIMESCALEDB: _ClassVar[TimeSeriesBackendType]
    TIMESERIES_BACKEND_INFLUXDB: _ClassVar[TimeSeriesBackendType]
    TIMESERIES_BACKEND_CLICKHOUSE: _ClassVar[TimeSeriesBackendType]

class ColumnBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COLUMN_BACKEND_UNSPECIFIED: _ClassVar[ColumnBackendType]
    COLUMN_BACKEND_CLICKHOUSE: _ClassVar[ColumnBackendType]
    COLUMN_BACKEND_CASSANDRA: _ClassVar[ColumnBackendType]
    COLUMN_BACKEND_BIGTABLE: _ClassVar[ColumnBackendType]

class ModelBackendType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_BACKEND_UNSPECIFIED: _ClassVar[ModelBackendType]
    MODEL_BACKEND_MLFLOW: _ClassVar[ModelBackendType]
    MODEL_BACKEND_DVC: _ClassVar[ModelBackendType]
    MODEL_BACKEND_HUGGINGFACE: _ClassVar[ModelBackendType]
    MODEL_BACKEND_BENTOML: _ClassVar[ModelBackendType]
    MODEL_BACKEND_TRITON: _ClassVar[ModelBackendType]
    MODEL_BACKEND_TORCHSERVE: _ClassVar[ModelBackendType]
    MODEL_BACKEND_CUSTOM: _ClassVar[ModelBackendType]
PARTITION_STRATEGY_UNSPECIFIED: PartitionStrategy
PARTITION_STRATEGY_NONE: PartitionStrategy
PARTITION_STRATEGY_RANGE_YEAR: PartitionStrategy
PARTITION_STRATEGY_RANGE_MONTH: PartitionStrategy
PARTITION_STRATEGY_LIST: PartitionStrategy
PARTITION_STRATEGY_HASH: PartitionStrategy
REFERENTIAL_ACTION_UNSPECIFIED: ReferentialAction
REFERENTIAL_ACTION_NO_ACTION: ReferentialAction
REFERENTIAL_ACTION_RESTRICT: ReferentialAction
REFERENTIAL_ACTION_CASCADE: ReferentialAction
REFERENTIAL_ACTION_SET_NULL: ReferentialAction
REFERENTIAL_ACTION_SET_DEFAULT: ReferentialAction
INDEX_TYPE_UNSPECIFIED: IndexType
INDEX_TYPE_NONE: IndexType
INDEX_TYPE_BTREE: IndexType
INDEX_TYPE_HASH: IndexType
INDEX_TYPE_GIN: IndexType
INDEX_TYPE_GIST: IndexType
INDEX_TYPE_BRIN: IndexType
INDEX_TYPE_HNSW: IndexType
INDEX_TYPE_IVF: IndexType
INDEX_TYPE_IVFPQ: IndexType
STORAGE_BACKEND_UNSPECIFIED: StorageBackendType
STORAGE_BACKEND_S3: StorageBackendType
STORAGE_BACKEND_MINIO: StorageBackendType
STORAGE_BACKEND_GCS: StorageBackendType
STORAGE_BACKEND_AZURE_BLOB: StorageBackendType
STORAGE_BACKEND_LOCAL: StorageBackendType
VECTOR_BACKEND_UNSPECIFIED: VectorBackendType
VECTOR_BACKEND_QDRANT: VectorBackendType
VECTOR_BACKEND_MILVUS: VectorBackendType
VECTOR_BACKEND_WEAVIATE: VectorBackendType
VECTOR_BACKEND_PGVECTOR: VectorBackendType
VECTOR_BACKEND_PINECONE: VectorBackendType
VECTOR_BACKEND_OPENSEARCH: VectorBackendType
VECTOR_DISTANCE_UNSPECIFIED: VectorDistanceMetric
VECTOR_DISTANCE_COSINE: VectorDistanceMetric
VECTOR_DISTANCE_DOT: VectorDistanceMetric
VECTOR_DISTANCE_EUCLIDEAN: VectorDistanceMetric
VECTOR_DISTANCE_MANHATTAN: VectorDistanceMetric
CACHE_BACKEND_UNSPECIFIED: CacheBackendType
CACHE_BACKEND_REDIS: CacheBackendType
CACHE_BACKEND_MEMCACHED: CacheBackendType
CACHE_BACKEND_IN_PROCESS: CacheBackendType
CACHE_BACKEND_DRAGONFLY: CacheBackendType
GRAPH_BACKEND_UNSPECIFIED: GraphBackendType
GRAPH_BACKEND_NEO4J: GraphBackendType
GRAPH_BACKEND_MEMGRAPH: GraphBackendType
GRAPH_BACKEND_ARANGODB: GraphBackendType
NOSQL_BACKEND_UNSPECIFIED: NoSqlBackendType
NOSQL_BACKEND_MONGODB: NoSqlBackendType
NOSQL_BACKEND_DYNAMODB: NoSqlBackendType
NOSQL_BACKEND_COSMOSDB: NoSqlBackendType
TIMESERIES_BACKEND_UNSPECIFIED: TimeSeriesBackendType
TIMESERIES_BACKEND_TIMESCALEDB: TimeSeriesBackendType
TIMESERIES_BACKEND_INFLUXDB: TimeSeriesBackendType
TIMESERIES_BACKEND_CLICKHOUSE: TimeSeriesBackendType
COLUMN_BACKEND_UNSPECIFIED: ColumnBackendType
COLUMN_BACKEND_CLICKHOUSE: ColumnBackendType
COLUMN_BACKEND_CASSANDRA: ColumnBackendType
COLUMN_BACKEND_BIGTABLE: ColumnBackendType
MODEL_BACKEND_UNSPECIFIED: ModelBackendType
MODEL_BACKEND_MLFLOW: ModelBackendType
MODEL_BACKEND_DVC: ModelBackendType
MODEL_BACKEND_HUGGINGFACE: ModelBackendType
MODEL_BACKEND_BENTOML: ModelBackendType
MODEL_BACKEND_TRITON: ModelBackendType
MODEL_BACKEND_TORCHSERVE: ModelBackendType
MODEL_BACKEND_CUSTOM: ModelBackendType
TABLE_FIELD_NUMBER: _ClassVar[int]
table: _descriptor.FieldDescriptor
PG_TABLE_FIELD_NUMBER: _ClassVar[int]
pg_table: _descriptor.FieldDescriptor
VECTOR_STORE_FIELD_NUMBER: _ClassVar[int]
vector_store: _descriptor.FieldDescriptor
CACHE_FIELD_NUMBER: _ClassVar[int]
cache: _descriptor.FieldDescriptor
MODEL_REGISTRY_FIELD_NUMBER: _ClassVar[int]
model_registry: _descriptor.FieldDescriptor
GRAPH_STORE_FIELD_NUMBER: _ClassVar[int]
graph_store: _descriptor.FieldDescriptor
DOCUMENT_STORE_FIELD_NUMBER: _ClassVar[int]
document_store: _descriptor.FieldDescriptor
NOSQL_STORE_FIELD_NUMBER: _ClassVar[int]
nosql_store: _descriptor.FieldDescriptor
TIMESERIES_STORE_FIELD_NUMBER: _ClassVar[int]
timeseries_store: _descriptor.FieldDescriptor
COLUMN_STORE_FIELD_NUMBER: _ClassVar[int]
column_store: _descriptor.FieldDescriptor
DATA_STORE_FIELD_NUMBER: _ClassVar[int]
data_store: _descriptor.FieldDescriptor
SECURITY_FIELD_NUMBER: _ClassVar[int]
security: _descriptor.FieldDescriptor
COLUMN_FIELD_NUMBER: _ClassVar[int]
column: _descriptor.FieldDescriptor
PG_COLUMN_FIELD_NUMBER: _ClassVar[int]
pg_column: _descriptor.FieldDescriptor
STORAGE_FIELD_NUMBER: _ClassVar[int]
storage: _descriptor.FieldDescriptor
COLUMN_SECURITY_FIELD_NUMBER: _ClassVar[int]
column_security: _descriptor.FieldDescriptor

class TableOptions(_message.Message):
    __slots__ = ("table_name", "schema_name", "migration_order", "is_table", "comment", "soft_delete", "audit_fields", "enable_rls", "partition_strategy", "partition_column", "retention_days", "rls_policies", "force_rls", "soft_delete_column", "unlogged", "tablespace", "indexes", "foreign_keys", "extensions", "materialized_views", "triggers", "previous_table_name", "allow_drop", "sql_artifacts", "partition_interval", "partition_premake", "partition_default", "partition_retention_months", "replica_hint", "cdc_topic", "required_scope", "vector_store", "native_service_id")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    MIGRATION_ORDER_FIELD_NUMBER: _ClassVar[int]
    IS_TABLE_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    SOFT_DELETE_FIELD_NUMBER: _ClassVar[int]
    AUDIT_FIELDS_FIELD_NUMBER: _ClassVar[int]
    ENABLE_RLS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    PARTITION_COLUMN_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    RLS_POLICIES_FIELD_NUMBER: _ClassVar[int]
    FORCE_RLS_FIELD_NUMBER: _ClassVar[int]
    SOFT_DELETE_COLUMN_FIELD_NUMBER: _ClassVar[int]
    UNLOGGED_FIELD_NUMBER: _ClassVar[int]
    TABLESPACE_FIELD_NUMBER: _ClassVar[int]
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    FOREIGN_KEYS_FIELD_NUMBER: _ClassVar[int]
    EXTENSIONS_FIELD_NUMBER: _ClassVar[int]
    MATERIALIZED_VIEWS_FIELD_NUMBER: _ClassVar[int]
    TRIGGERS_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DROP_FIELD_NUMBER: _ClassVar[int]
    SQL_ARTIFACTS_FIELD_NUMBER: _ClassVar[int]
    PARTITION_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    PARTITION_PREMAKE_FIELD_NUMBER: _ClassVar[int]
    PARTITION_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    PARTITION_RETENTION_MONTHS_FIELD_NUMBER: _ClassVar[int]
    REPLICA_HINT_FIELD_NUMBER: _ClassVar[int]
    CDC_TOPIC_FIELD_NUMBER: _ClassVar[int]
    REQUIRED_SCOPE_FIELD_NUMBER: _ClassVar[int]
    VECTOR_STORE_FIELD_NUMBER: _ClassVar[int]
    NATIVE_SERVICE_ID_FIELD_NUMBER: _ClassVar[int]
    table_name: str
    schema_name: str
    migration_order: int
    is_table: bool
    comment: str
    soft_delete: bool
    audit_fields: bool
    enable_rls: bool
    partition_strategy: PartitionStrategy
    partition_column: str
    retention_days: int
    rls_policies: _containers.RepeatedCompositeFieldContainer[RlsPolicy]
    force_rls: bool
    soft_delete_column: str
    unlogged: bool
    tablespace: str
    indexes: _containers.RepeatedCompositeFieldContainer[IndexOptions]
    foreign_keys: _containers.RepeatedCompositeFieldContainer[TableForeignKey]
    extensions: _containers.RepeatedCompositeFieldContainer[DbExtension]
    materialized_views: _containers.RepeatedCompositeFieldContainer[MaterializedView]
    triggers: _containers.RepeatedCompositeFieldContainer[DbTrigger]
    previous_table_name: str
    allow_drop: bool
    sql_artifacts: _containers.RepeatedCompositeFieldContainer[SqlArtifact]
    partition_interval: str
    partition_premake: int
    partition_default: bool
    partition_retention_months: int
    replica_hint: str
    cdc_topic: str
    required_scope: str
    vector_store: VectorStoreOptions
    native_service_id: str
    def __init__(self, table_name: _Optional[str] = ..., schema_name: _Optional[str] = ..., migration_order: _Optional[int] = ..., is_table: bool = ..., comment: _Optional[str] = ..., soft_delete: bool = ..., audit_fields: bool = ..., enable_rls: bool = ..., partition_strategy: _Optional[_Union[PartitionStrategy, str]] = ..., partition_column: _Optional[str] = ..., retention_days: _Optional[int] = ..., rls_policies: _Optional[_Iterable[_Union[RlsPolicy, _Mapping]]] = ..., force_rls: bool = ..., soft_delete_column: _Optional[str] = ..., unlogged: bool = ..., tablespace: _Optional[str] = ..., indexes: _Optional[_Iterable[_Union[IndexOptions, _Mapping]]] = ..., foreign_keys: _Optional[_Iterable[_Union[TableForeignKey, _Mapping]]] = ..., extensions: _Optional[_Iterable[_Union[DbExtension, _Mapping]]] = ..., materialized_views: _Optional[_Iterable[_Union[MaterializedView, _Mapping]]] = ..., triggers: _Optional[_Iterable[_Union[DbTrigger, _Mapping]]] = ..., previous_table_name: _Optional[str] = ..., allow_drop: bool = ..., sql_artifacts: _Optional[_Iterable[_Union[SqlArtifact, _Mapping]]] = ..., partition_interval: _Optional[str] = ..., partition_premake: _Optional[int] = ..., partition_default: bool = ..., partition_retention_months: _Optional[int] = ..., replica_hint: _Optional[str] = ..., cdc_topic: _Optional[str] = ..., required_scope: _Optional[str] = ..., vector_store: _Optional[_Union[VectorStoreOptions, _Mapping]] = ..., native_service_id: _Optional[str] = ...) -> None: ...

class RlsPolicy(_message.Message):
    __slots__ = ("policy_name", "command", "using", "with_check", "permissive")
    POLICY_NAME_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    USING_FIELD_NUMBER: _ClassVar[int]
    WITH_CHECK_FIELD_NUMBER: _ClassVar[int]
    PERMISSIVE_FIELD_NUMBER: _ClassVar[int]
    policy_name: str
    command: str
    using: str
    with_check: str
    permissive: bool
    def __init__(self, policy_name: _Optional[str] = ..., command: _Optional[str] = ..., using: _Optional[str] = ..., with_check: _Optional[str] = ..., permissive: bool = ...) -> None: ...

class TableForeignKey(_message.Message):
    __slots__ = ("columns", "references_table", "references_column", "references_schema", "on_delete", "on_update", "constraint_name", "not_valid", "deferrable", "initially_deferred")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_TABLE_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_COLUMN_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    ON_DELETE_FIELD_NUMBER: _ClassVar[int]
    ON_UPDATE_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_NAME_FIELD_NUMBER: _ClassVar[int]
    NOT_VALID_FIELD_NUMBER: _ClassVar[int]
    DEFERRABLE_FIELD_NUMBER: _ClassVar[int]
    INITIALLY_DEFERRED_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    references_table: str
    references_column: _containers.RepeatedScalarFieldContainer[str]
    references_schema: str
    on_delete: ReferentialAction
    on_update: ReferentialAction
    constraint_name: str
    not_valid: bool
    deferrable: bool
    initially_deferred: bool
    def __init__(self, columns: _Optional[_Iterable[str]] = ..., references_table: _Optional[str] = ..., references_column: _Optional[_Iterable[str]] = ..., references_schema: _Optional[str] = ..., on_delete: _Optional[_Union[ReferentialAction, str]] = ..., on_update: _Optional[_Union[ReferentialAction, str]] = ..., constraint_name: _Optional[str] = ..., not_valid: bool = ..., deferrable: bool = ..., initially_deferred: bool = ...) -> None: ...

class DbExtension(_message.Message):
    __slots__ = ("name", "schema", "version")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    schema: str
    version: str
    def __init__(self, name: _Optional[str] = ..., schema: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class MaterializedView(_message.Message):
    __slots__ = ("view_name", "schema_name", "query", "with_data")
    VIEW_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    WITH_DATA_FIELD_NUMBER: _ClassVar[int]
    view_name: str
    schema_name: str
    query: str
    with_data: bool
    def __init__(self, view_name: _Optional[str] = ..., schema_name: _Optional[str] = ..., query: _Optional[str] = ..., with_data: bool = ...) -> None: ...

class DbTrigger(_message.Message):
    __slots__ = ("trigger_name", "timing", "event", "function_name", "for_each", "when_clause")
    TRIGGER_NAME_FIELD_NUMBER: _ClassVar[int]
    TIMING_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    FUNCTION_NAME_FIELD_NUMBER: _ClassVar[int]
    FOR_EACH_FIELD_NUMBER: _ClassVar[int]
    WHEN_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    trigger_name: str
    timing: str
    event: str
    function_name: str
    for_each: str
    when_clause: str
    def __init__(self, trigger_name: _Optional[str] = ..., timing: _Optional[str] = ..., event: _Optional[str] = ..., function_name: _Optional[str] = ..., for_each: _Optional[str] = ..., when_clause: _Optional[str] = ...) -> None: ...

class SqlArtifact(_message.Message):
    __slots__ = ("name", "backend", "phase", "sql", "file", "checksum_sha256", "requires_review")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    FILE_FIELD_NUMBER: _ClassVar[int]
    CHECKSUM_SHA256_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_REVIEW_FIELD_NUMBER: _ClassVar[int]
    name: str
    backend: str
    phase: str
    sql: str
    file: str
    checksum_sha256: str
    requires_review: bool
    def __init__(self, name: _Optional[str] = ..., backend: _Optional[str] = ..., phase: _Optional[str] = ..., sql: _Optional[str] = ..., file: _Optional[str] = ..., checksum_sha256: _Optional[str] = ..., requires_review: bool = ...) -> None: ...

class ColumnOptions(_message.Message):
    __slots__ = ("column_name", "sql_type", "not_null", "unique", "primary_key", "auto_increment", "default_value", "check_constraint", "foreign_key", "index", "comment", "exclude_from_insert", "exclude_from_update", "encrypted", "is_json", "is_jsonb", "json_path_ops", "is_tsvector", "tsvector_language", "tsvector_source_columns", "trigram_index", "collation", "enum_values", "previous_column_name", "backfill_sql", "using_expression", "allow_drop", "generated", "generated_expr", "identity", "references", "on_delete", "on_update", "nullable", "tenant_column", "project_column")
    COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    SQL_TYPE_FIELD_NUMBER: _ClassVar[int]
    NOT_NULL_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_KEY_FIELD_NUMBER: _ClassVar[int]
    AUTO_INCREMENT_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_VALUE_FIELD_NUMBER: _ClassVar[int]
    CHECK_CONSTRAINT_FIELD_NUMBER: _ClassVar[int]
    FOREIGN_KEY_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_FROM_INSERT_FIELD_NUMBER: _ClassVar[int]
    EXCLUDE_FROM_UPDATE_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    IS_JSON_FIELD_NUMBER: _ClassVar[int]
    IS_JSONB_FIELD_NUMBER: _ClassVar[int]
    JSON_PATH_OPS_FIELD_NUMBER: _ClassVar[int]
    IS_TSVECTOR_FIELD_NUMBER: _ClassVar[int]
    TSVECTOR_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    TSVECTOR_SOURCE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TRIGRAM_INDEX_FIELD_NUMBER: _ClassVar[int]
    COLLATION_FIELD_NUMBER: _ClassVar[int]
    ENUM_VALUES_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    BACKFILL_SQL_FIELD_NUMBER: _ClassVar[int]
    USING_EXPRESSION_FIELD_NUMBER: _ClassVar[int]
    ALLOW_DROP_FIELD_NUMBER: _ClassVar[int]
    GENERATED_FIELD_NUMBER: _ClassVar[int]
    GENERATED_EXPR_FIELD_NUMBER: _ClassVar[int]
    IDENTITY_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_FIELD_NUMBER: _ClassVar[int]
    ON_DELETE_FIELD_NUMBER: _ClassVar[int]
    ON_UPDATE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    PROJECT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    column_name: str
    sql_type: str
    not_null: bool
    unique: bool
    primary_key: bool
    auto_increment: bool
    default_value: str
    check_constraint: str
    foreign_key: ForeignKey
    index: IndexOptions
    comment: str
    exclude_from_insert: bool
    exclude_from_update: bool
    encrypted: bool
    is_json: bool
    is_jsonb: bool
    json_path_ops: bool
    is_tsvector: bool
    tsvector_language: str
    tsvector_source_columns: _containers.RepeatedScalarFieldContainer[str]
    trigram_index: bool
    collation: str
    enum_values: _containers.RepeatedScalarFieldContainer[str]
    previous_column_name: str
    backfill_sql: str
    using_expression: str
    allow_drop: bool
    generated: bool
    generated_expr: str
    identity: bool
    references: str
    on_delete: ReferentialAction
    on_update: ReferentialAction
    nullable: bool
    tenant_column: bool
    project_column: bool
    def __init__(self, column_name: _Optional[str] = ..., sql_type: _Optional[str] = ..., not_null: bool = ..., unique: bool = ..., primary_key: bool = ..., auto_increment: bool = ..., default_value: _Optional[str] = ..., check_constraint: _Optional[str] = ..., foreign_key: _Optional[_Union[ForeignKey, _Mapping]] = ..., index: _Optional[_Union[IndexOptions, _Mapping]] = ..., comment: _Optional[str] = ..., exclude_from_insert: bool = ..., exclude_from_update: bool = ..., encrypted: bool = ..., is_json: bool = ..., is_jsonb: bool = ..., json_path_ops: bool = ..., is_tsvector: bool = ..., tsvector_language: _Optional[str] = ..., tsvector_source_columns: _Optional[_Iterable[str]] = ..., trigram_index: bool = ..., collation: _Optional[str] = ..., enum_values: _Optional[_Iterable[str]] = ..., previous_column_name: _Optional[str] = ..., backfill_sql: _Optional[str] = ..., using_expression: _Optional[str] = ..., allow_drop: bool = ..., generated: bool = ..., generated_expr: _Optional[str] = ..., identity: bool = ..., references: _Optional[str] = ..., on_delete: _Optional[_Union[ReferentialAction, str]] = ..., on_update: _Optional[_Union[ReferentialAction, str]] = ..., nullable: bool = ..., tenant_column: bool = ..., project_column: bool = ...) -> None: ...

class ForeignKey(_message.Message):
    __slots__ = ("references_table", "references_column", "references_schema", "on_delete", "on_update", "constraint_name")
    REFERENCES_TABLE_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_COLUMN_FIELD_NUMBER: _ClassVar[int]
    REFERENCES_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    ON_DELETE_FIELD_NUMBER: _ClassVar[int]
    ON_UPDATE_FIELD_NUMBER: _ClassVar[int]
    CONSTRAINT_NAME_FIELD_NUMBER: _ClassVar[int]
    references_table: str
    references_column: str
    references_schema: str
    on_delete: ReferentialAction
    on_update: ReferentialAction
    constraint_name: str
    def __init__(self, references_table: _Optional[str] = ..., references_column: _Optional[str] = ..., references_schema: _Optional[str] = ..., on_delete: _Optional[_Union[ReferentialAction, str]] = ..., on_update: _Optional[_Union[ReferentialAction, str]] = ..., constraint_name: _Optional[str] = ...) -> None: ...

class IndexOptions(_message.Message):
    __slots__ = ("index_name", "index_type", "unique", "composite_fields", "include_columns", "index_method", "where_clause", "operator_class", "index_params", "concurrent", "columns")
    class IndexParamsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    INDEX_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIQUE_FIELD_NUMBER: _ClassVar[int]
    COMPOSITE_FIELDS_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    INDEX_METHOD_FIELD_NUMBER: _ClassVar[int]
    WHERE_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    OPERATOR_CLASS_FIELD_NUMBER: _ClassVar[int]
    INDEX_PARAMS_FIELD_NUMBER: _ClassVar[int]
    CONCURRENT_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    index_name: str
    index_type: str
    unique: bool
    composite_fields: _containers.RepeatedScalarFieldContainer[str]
    include_columns: _containers.RepeatedScalarFieldContainer[str]
    index_method: str
    where_clause: str
    operator_class: str
    index_params: _containers.ScalarMap[str, str]
    concurrent: bool
    columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, index_name: _Optional[str] = ..., index_type: _Optional[str] = ..., unique: bool = ..., composite_fields: _Optional[_Iterable[str]] = ..., include_columns: _Optional[_Iterable[str]] = ..., index_method: _Optional[str] = ..., where_clause: _Optional[str] = ..., operator_class: _Optional[str] = ..., index_params: _Optional[_Mapping[str, str]] = ..., concurrent: bool = ..., columns: _Optional[_Iterable[str]] = ...) -> None: ...

class StorageFieldOptions(_message.Message):
    __slots__ = ("backend", "bucket_env_key", "key_prefix", "presigned_read", "presigned_write", "presigned_ttl_seconds", "server_side_encryption", "kms_key_id", "acl")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    BUCKET_ENV_KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_READ_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_WRITE_FIELD_NUMBER: _ClassVar[int]
    PRESIGNED_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    SERVER_SIDE_ENCRYPTION_FIELD_NUMBER: _ClassVar[int]
    KMS_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ACL_FIELD_NUMBER: _ClassVar[int]
    backend: StorageBackendType
    bucket_env_key: str
    key_prefix: str
    presigned_read: bool
    presigned_write: bool
    presigned_ttl_seconds: int
    server_side_encryption: bool
    kms_key_id: str
    acl: str
    def __init__(self, backend: _Optional[_Union[StorageBackendType, str]] = ..., bucket_env_key: _Optional[str] = ..., key_prefix: _Optional[str] = ..., presigned_read: bool = ..., presigned_write: bool = ..., presigned_ttl_seconds: _Optional[int] = ..., server_side_encryption: bool = ..., kms_key_id: _Optional[str] = ..., acl: _Optional[str] = ...) -> None: ...

class VectorStoreOptions(_message.Message):
    __slots__ = ("backend", "collection_name", "dimension", "distance", "shard_count", "replica_count", "on_disk", "payload_schema_json", "hnsw_m", "hnsw_ef_construction")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_FIELD_NUMBER: _ClassVar[int]
    SHARD_COUNT_FIELD_NUMBER: _ClassVar[int]
    REPLICA_COUNT_FIELD_NUMBER: _ClassVar[int]
    ON_DISK_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    HNSW_M_FIELD_NUMBER: _ClassVar[int]
    HNSW_EF_CONSTRUCTION_FIELD_NUMBER: _ClassVar[int]
    backend: VectorBackendType
    collection_name: str
    dimension: int
    distance: VectorDistanceMetric
    shard_count: int
    replica_count: int
    on_disk: bool
    payload_schema_json: str
    hnsw_m: int
    hnsw_ef_construction: int
    def __init__(self, backend: _Optional[_Union[VectorBackendType, str]] = ..., collection_name: _Optional[str] = ..., dimension: _Optional[int] = ..., distance: _Optional[_Union[VectorDistanceMetric, str]] = ..., shard_count: _Optional[int] = ..., replica_count: _Optional[int] = ..., on_disk: bool = ..., payload_schema_json: _Optional[str] = ..., hnsw_m: _Optional[int] = ..., hnsw_ef_construction: _Optional[int] = ...) -> None: ...

class CacheOptions(_message.Message):
    __slots__ = ("backend", "key_pattern", "ttl_seconds", "write_through", "read_through", "eviction_policy", "cluster_env_key", "namespace")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    KEY_PATTERN_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    WRITE_THROUGH_FIELD_NUMBER: _ClassVar[int]
    READ_THROUGH_FIELD_NUMBER: _ClassVar[int]
    EVICTION_POLICY_FIELD_NUMBER: _ClassVar[int]
    CLUSTER_ENV_KEY_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    backend: CacheBackendType
    key_pattern: str
    ttl_seconds: int
    write_through: bool
    read_through: bool
    eviction_policy: str
    cluster_env_key: str
    namespace: str
    def __init__(self, backend: _Optional[_Union[CacheBackendType, str]] = ..., key_pattern: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., write_through: bool = ..., read_through: bool = ..., eviction_policy: _Optional[str] = ..., cluster_env_key: _Optional[str] = ..., namespace: _Optional[str] = ...) -> None: ...

class ModelRegistryOptions(_message.Message):
    __slots__ = ("backend", "experiment_name", "artifact_path", "auto_register", "stage", "metric_keys", "param_keys", "storage_uri_env")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_NAME_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PATH_FIELD_NUMBER: _ClassVar[int]
    AUTO_REGISTER_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    METRIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    PARAM_KEYS_FIELD_NUMBER: _ClassVar[int]
    STORAGE_URI_ENV_FIELD_NUMBER: _ClassVar[int]
    backend: ModelBackendType
    experiment_name: str
    artifact_path: str
    auto_register: bool
    stage: str
    metric_keys: _containers.RepeatedScalarFieldContainer[str]
    param_keys: _containers.RepeatedScalarFieldContainer[str]
    storage_uri_env: str
    def __init__(self, backend: _Optional[_Union[ModelBackendType, str]] = ..., experiment_name: _Optional[str] = ..., artifact_path: _Optional[str] = ..., auto_register: bool = ..., stage: _Optional[str] = ..., metric_keys: _Optional[_Iterable[str]] = ..., param_keys: _Optional[_Iterable[str]] = ..., storage_uri_env: _Optional[str] = ...) -> None: ...

class SecurityOptions(_message.Message):
    __slots__ = ("classification_level", "audit_writes", "audit_reads", "retention_days", "encryption_required")
    CLASSIFICATION_LEVEL_FIELD_NUMBER: _ClassVar[int]
    AUDIT_WRITES_FIELD_NUMBER: _ClassVar[int]
    AUDIT_READS_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    ENCRYPTION_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    classification_level: str
    audit_writes: bool
    audit_reads: bool
    retention_days: int
    encryption_required: bool
    def __init__(self, classification_level: _Optional[str] = ..., audit_writes: bool = ..., audit_reads: bool = ..., retention_days: _Optional[int] = ..., encryption_required: bool = ...) -> None: ...

class ColumnSecurityOptions(_message.Message):
    __slots__ = ("is_pii", "is_encrypted", "is_blind_index", "mask_in_logs", "data_class", "consent_required", "retention_days")
    IS_PII_FIELD_NUMBER: _ClassVar[int]
    IS_ENCRYPTED_FIELD_NUMBER: _ClassVar[int]
    IS_BLIND_INDEX_FIELD_NUMBER: _ClassVar[int]
    MASK_IN_LOGS_FIELD_NUMBER: _ClassVar[int]
    DATA_CLASS_FIELD_NUMBER: _ClassVar[int]
    CONSENT_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    is_pii: bool
    is_encrypted: bool
    is_blind_index: bool
    mask_in_logs: bool
    data_class: str
    consent_required: bool
    retention_days: int
    def __init__(self, is_pii: bool = ..., is_encrypted: bool = ..., is_blind_index: bool = ..., mask_in_logs: bool = ..., data_class: _Optional[str] = ..., consent_required: bool = ..., retention_days: _Optional[int] = ...) -> None: ...

class GraphStoreOptions(_message.Message):
    __slots__ = ("backend", "graph_name", "node_label", "id_field", "tenant_field", "edge_source_field", "edge_target_field", "payload_schema_json")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    GRAPH_NAME_FIELD_NUMBER: _ClassVar[int]
    NODE_LABEL_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_FIELD_NUMBER: _ClassVar[int]
    EDGE_SOURCE_FIELD_FIELD_NUMBER: _ClassVar[int]
    EDGE_TARGET_FIELD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    backend: GraphBackendType
    graph_name: str
    node_label: str
    id_field: str
    tenant_field: str
    edge_source_field: str
    edge_target_field: str
    payload_schema_json: str
    def __init__(self, backend: _Optional[_Union[GraphBackendType, str]] = ..., graph_name: _Optional[str] = ..., node_label: _Optional[str] = ..., id_field: _Optional[str] = ..., tenant_field: _Optional[str] = ..., edge_source_field: _Optional[str] = ..., edge_target_field: _Optional[str] = ..., payload_schema_json: _Optional[str] = ...) -> None: ...

class DocumentStoreOptions(_message.Message):
    __slots__ = ("backend", "database_name", "collection_name", "partition_key", "id_field", "tenant_field", "ttl_seconds", "payload_schema_json")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATABASE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_NAME_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    backend: NoSqlBackendType
    database_name: str
    collection_name: str
    partition_key: str
    id_field: str
    tenant_field: str
    ttl_seconds: int
    payload_schema_json: str
    def __init__(self, backend: _Optional[_Union[NoSqlBackendType, str]] = ..., database_name: _Optional[str] = ..., collection_name: _Optional[str] = ..., partition_key: _Optional[str] = ..., id_field: _Optional[str] = ..., tenant_field: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., payload_schema_json: _Optional[str] = ...) -> None: ...

class TimeSeriesStoreOptions(_message.Message):
    __slots__ = ("backend", "database_name", "measurement_name", "time_field", "tenant_field", "tag_fields", "value_fields", "retention_days", "downsample_policy")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATABASE_NAME_FIELD_NUMBER: _ClassVar[int]
    MEASUREMENT_NAME_FIELD_NUMBER: _ClassVar[int]
    TIME_FIELD_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELDS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELDS_FIELD_NUMBER: _ClassVar[int]
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    DOWNSAMPLE_POLICY_FIELD_NUMBER: _ClassVar[int]
    backend: TimeSeriesBackendType
    database_name: str
    measurement_name: str
    time_field: str
    tenant_field: str
    tag_fields: _containers.RepeatedScalarFieldContainer[str]
    value_fields: _containers.RepeatedScalarFieldContainer[str]
    retention_days: int
    downsample_policy: str
    def __init__(self, backend: _Optional[_Union[TimeSeriesBackendType, str]] = ..., database_name: _Optional[str] = ..., measurement_name: _Optional[str] = ..., time_field: _Optional[str] = ..., tenant_field: _Optional[str] = ..., tag_fields: _Optional[_Iterable[str]] = ..., value_fields: _Optional[_Iterable[str]] = ..., retention_days: _Optional[int] = ..., downsample_policy: _Optional[str] = ...) -> None: ...

class ColumnStoreOptions(_message.Message):
    __slots__ = ("backend", "database_name", "table_name", "partition_key", "sort_key", "compression", "ttl_seconds", "payload_schema_json")
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    DATABASE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    PARTITION_KEY_FIELD_NUMBER: _ClassVar[int]
    SORT_KEY_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    backend: ColumnBackendType
    database_name: str
    table_name: str
    partition_key: str
    sort_key: str
    compression: str
    ttl_seconds: int
    payload_schema_json: str
    def __init__(self, backend: _Optional[_Union[ColumnBackendType, str]] = ..., database_name: _Optional[str] = ..., table_name: _Optional[str] = ..., partition_key: _Optional[str] = ..., sort_key: _Optional[str] = ..., compression: _Optional[str] = ..., ttl_seconds: _Optional[int] = ..., payload_schema_json: _Optional[str] = ...) -> None: ...

class GenericStoreOptions(_message.Message):
    __slots__ = ("store_kind", "backend", "logical_name", "database_name", "namespace", "resource_name", "dsn_env_key", "dsn", "payload_schema_json", "options")
    class OptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    STORE_KIND_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    LOGICAL_NAME_FIELD_NUMBER: _ClassVar[int]
    DATABASE_NAME_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    DSN_ENV_KEY_FIELD_NUMBER: _ClassVar[int]
    DSN_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    store_kind: str
    backend: str
    logical_name: str
    database_name: str
    namespace: str
    resource_name: str
    dsn_env_key: str
    dsn: str
    payload_schema_json: str
    options: _containers.ScalarMap[str, str]
    def __init__(self, store_kind: _Optional[str] = ..., backend: _Optional[str] = ..., logical_name: _Optional[str] = ..., database_name: _Optional[str] = ..., namespace: _Optional[str] = ..., resource_name: _Optional[str] = ..., dsn_env_key: _Optional[str] = ..., dsn: _Optional[str] = ..., payload_schema_json: _Optional[str] = ..., options: _Optional[_Mapping[str, str]] = ...) -> None: ...
