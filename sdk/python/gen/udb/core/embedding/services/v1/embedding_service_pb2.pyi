from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmbeddingModelStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EMBEDDING_MODEL_STATUS_UNSPECIFIED: _ClassVar[EmbeddingModelStatus]
    EMBEDDING_MODEL_STATUS_ACTIVE: _ClassVar[EmbeddingModelStatus]
    EMBEDDING_MODEL_STATUS_DEPRECATED: _ClassVar[EmbeddingModelStatus]
    EMBEDDING_MODEL_STATUS_RETIRED: _ClassVar[EmbeddingModelStatus]

class EmbeddingTenantState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EMBEDDING_TENANT_STATE_UNSPECIFIED: _ClassVar[EmbeddingTenantState]
    EMBEDDING_TENANT_STATE_ACTIVE: _ClassVar[EmbeddingTenantState]
    EMBEDDING_TENANT_STATE_INACTIVE: _ClassVar[EmbeddingTenantState]
    EMBEDDING_TENANT_STATE_OFFLOADED: _ClassVar[EmbeddingTenantState]

class FusionStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FUSION_STRATEGY_UNSPECIFIED: _ClassVar[FusionStrategy]
    FUSION_STRATEGY_RRF: _ClassVar[FusionStrategy]
    FUSION_STRATEGY_WEIGHTED: _ClassVar[FusionStrategy]
    FUSION_STRATEGY_DBSF: _ClassVar[FusionStrategy]

class RerankStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RERANK_STRATEGY_UNSPECIFIED: _ClassVar[RerankStrategy]
    RERANK_STRATEGY_CROSS_ENCODER: _ClassVar[RerankStrategy]
    RERANK_STRATEGY_LATE_INTERACTION: _ClassVar[RerankStrategy]
EMBEDDING_MODEL_STATUS_UNSPECIFIED: EmbeddingModelStatus
EMBEDDING_MODEL_STATUS_ACTIVE: EmbeddingModelStatus
EMBEDDING_MODEL_STATUS_DEPRECATED: EmbeddingModelStatus
EMBEDDING_MODEL_STATUS_RETIRED: EmbeddingModelStatus
EMBEDDING_TENANT_STATE_UNSPECIFIED: EmbeddingTenantState
EMBEDDING_TENANT_STATE_ACTIVE: EmbeddingTenantState
EMBEDDING_TENANT_STATE_INACTIVE: EmbeddingTenantState
EMBEDDING_TENANT_STATE_OFFLOADED: EmbeddingTenantState
FUSION_STRATEGY_UNSPECIFIED: FusionStrategy
FUSION_STRATEGY_RRF: FusionStrategy
FUSION_STRATEGY_WEIGHTED: FusionStrategy
FUSION_STRATEGY_DBSF: FusionStrategy
RERANK_STRATEGY_UNSPECIFIED: RerankStrategy
RERANK_STRATEGY_CROSS_ENCODER: RerankStrategy
RERANK_STRATEGY_LATE_INTERACTION: RerankStrategy

class RegisterSourceRequest(_message.Message):
    __slots__ = ("tenant_id", "source_name", "source_message_type", "text_fields", "target_collection", "model_id", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELDS_FIELD_NUMBER: _ClassVar[int]
    TARGET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    source_message_type: str
    text_fields: _containers.RepeatedScalarFieldContainer[str]
    target_collection: str
    model_id: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., text_fields: _Optional[_Iterable[str]] = ..., target_collection: _Optional[str] = ..., model_id: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class RegisterSourceResponse(_message.Message):
    __slots__ = ("source_id", "source_name", "tenant_column", "message", "error")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    source_name: str
    tenant_column: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, source_id: _Optional[str] = ..., source_name: _Optional[str] = ..., tenant_column: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListSourcesRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class EmbeddingSourceSummary(_message.Message):
    __slots__ = ("source_id", "source_name", "source_message_type", "target_collection", "model_id", "status")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    source_name: str
    source_message_type: str
    target_collection: str
    model_id: str
    status: str
    def __init__(self, source_id: _Optional[str] = ..., source_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., target_collection: _Optional[str] = ..., model_id: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class ListSourcesResponse(_message.Message):
    __slots__ = ("sources", "message", "error", "next_page_token")
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    sources: _containers.RepeatedCompositeFieldContainer[EmbeddingSourceSummary]
    message: str
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, sources: _Optional[_Iterable[_Union[EmbeddingSourceSummary, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class DeleteSourceRequest(_message.Message):
    __slots__ = ("tenant_id", "source_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ...) -> None: ...

class DeleteSourceResponse(_message.Message):
    __slots__ = ("deleted", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class BackfillRequest(_message.Message):
    __slots__ = ("tenant_id", "source_name", "mode")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    mode: str
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., mode: _Optional[str] = ...) -> None: ...

class BackfillResponse(_message.Message):
    __slots__ = ("backfill_id", "accepted", "message", "error")
    BACKFILL_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    backfill_id: str
    accepted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, backfill_id: _Optional[str] = ..., accepted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReportEmbeddingRequest(_message.Message):
    __slots__ = ("tenant_id", "source_name", "row_pk", "vector", "model", "dims", "work_item_id", "chunk_hash", "token_count", "vector_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROW_PK_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    DIMS_FIELD_NUMBER: _ClassVar[int]
    WORK_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_HASH_FIELD_NUMBER: _ClassVar[int]
    TOKEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    row_pk: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    model: str
    dims: int
    work_item_id: str
    chunk_hash: str
    token_count: int
    vector_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., row_pk: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., model: _Optional[str] = ..., dims: _Optional[int] = ..., work_item_id: _Optional[str] = ..., chunk_hash: _Optional[str] = ..., token_count: _Optional[int] = ..., vector_name: _Optional[str] = ...) -> None: ...

class ReportEmbeddingResponse(_message.Message):
    __slots__ = ("upserted", "message", "error")
    UPSERTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    upserted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, upserted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class RetrieveRequest(_message.Message):
    __slots__ = ("tenant_id", "source_name", "query_text", "query_vector", "top_k", "filter_json", "score_threshold", "include_vectors", "mmr", "fusion", "prefetch_limit", "rerank", "vector_name", "parent_window", "include_citations")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    FILTER_JSON_FIELD_NUMBER: _ClassVar[int]
    SCORE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_VECTORS_FIELD_NUMBER: _ClassVar[int]
    MMR_FIELD_NUMBER: _ClassVar[int]
    FUSION_FIELD_NUMBER: _ClassVar[int]
    PREFETCH_LIMIT_FIELD_NUMBER: _ClassVar[int]
    RERANK_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_WINDOW_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_CITATIONS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    query_text: str
    query_vector: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    filter_json: str
    score_threshold: float
    include_vectors: bool
    mmr: MmrConfig
    fusion: FusionStrategy
    prefetch_limit: int
    rerank: RerankConfig
    vector_name: str
    parent_window: int
    include_citations: bool
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., query_text: _Optional[str] = ..., query_vector: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ..., filter_json: _Optional[str] = ..., score_threshold: _Optional[float] = ..., include_vectors: bool = ..., mmr: _Optional[_Union[MmrConfig, _Mapping]] = ..., fusion: _Optional[_Union[FusionStrategy, str]] = ..., prefetch_limit: _Optional[int] = ..., rerank: _Optional[_Union[RerankConfig, _Mapping]] = ..., vector_name: _Optional[str] = ..., parent_window: _Optional[int] = ..., include_citations: bool = ...) -> None: ...

class RetrieveHit(_message.Message):
    __slots__ = ("id", "score", "payload_json", "vector", "source_name", "parent_pk", "chunk_seq", "document_id", "doc_version", "vector_name", "rerank_score")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_PK_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SEQ_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    RERANK_SCORE_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    payload_json: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    source_name: str
    parent_pk: str
    chunk_seq: int
    document_id: str
    doc_version: str
    vector_name: str
    rerank_score: float
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., payload_json: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., source_name: _Optional[str] = ..., parent_pk: _Optional[str] = ..., chunk_seq: _Optional[int] = ..., document_id: _Optional[str] = ..., doc_version: _Optional[str] = ..., vector_name: _Optional[str] = ..., rerank_score: _Optional[float] = ...) -> None: ...

class RetrieveResponse(_message.Message):
    __slots__ = ("hits", "message", "error", "index_lag_ms", "rerank_applied", "evaluation_id")
    HITS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    INDEX_LAG_MS_FIELD_NUMBER: _ClassVar[int]
    RERANK_APPLIED_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_ID_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[RetrieveHit]
    message: str
    error: _dto_pb2.ApiError
    index_lag_ms: int
    rerank_applied: bool
    evaluation_id: str
    def __init__(self, hits: _Optional[_Iterable[_Union[RetrieveHit, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., index_lag_ms: _Optional[int] = ..., rerank_applied: bool = ..., evaluation_id: _Optional[str] = ...) -> None: ...

class MmrConfig(_message.Message):
    __slots__ = ("enabled",)
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    LAMBDA_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    def __init__(self, enabled: bool = ..., **kwargs) -> None: ...

class RerankConfig(_message.Message):
    __slots__ = ("enabled", "strategy", "model", "top_n", "fail_open")
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    STRATEGY_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    TOP_N_FIELD_NUMBER: _ClassVar[int]
    FAIL_OPEN_FIELD_NUMBER: _ClassVar[int]
    enabled: bool
    strategy: RerankStrategy
    model: str
    top_n: int
    fail_open: bool
    def __init__(self, enabled: bool = ..., strategy: _Optional[_Union[RerankStrategy, str]] = ..., model: _Optional[str] = ..., top_n: _Optional[int] = ..., fail_open: bool = ...) -> None: ...

class RegisterModelRequest(_message.Message):
    __slots__ = ("tenant_id", "model_id", "provider", "model_name", "version", "dimensions", "matryoshka_dims", "distance_metric", "normalize", "output_dtype", "rescore", "max_input_tokens", "tokenizer", "task_type", "asymmetric", "provider_endpoint_ref", "vector_backend", "vector_instance", "collection_alias", "active_collection", "chunking_strategy", "chunk_tokens", "chunk_overlap_tokens", "contextual_retrieval", "late_chunking", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    MATRYOSHKA_DIMS_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    NORMALIZE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DTYPE_FIELD_NUMBER: _ClassVar[int]
    RESCORE_FIELD_NUMBER: _ClassVar[int]
    MAX_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOKENIZER_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    ASYMMETRIC_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ENDPOINT_REF_FIELD_NUMBER: _ClassVar[int]
    VECTOR_BACKEND_FIELD_NUMBER: _ClassVar[int]
    VECTOR_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ALIAS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    CHUNKING_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CHUNK_OVERLAP_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CONTEXTUAL_RETRIEVAL_FIELD_NUMBER: _ClassVar[int]
    LATE_CHUNKING_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    model_id: str
    provider: str
    model_name: str
    version: str
    dimensions: int
    matryoshka_dims: _containers.RepeatedScalarFieldContainer[int]
    distance_metric: str
    normalize: bool
    output_dtype: str
    rescore: bool
    max_input_tokens: int
    tokenizer: str
    task_type: str
    asymmetric: bool
    provider_endpoint_ref: str
    vector_backend: str
    vector_instance: str
    collection_alias: str
    active_collection: str
    chunking_strategy: str
    chunk_tokens: int
    chunk_overlap_tokens: int
    contextual_retrieval: bool
    late_chunking: bool
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., model_id: _Optional[str] = ..., provider: _Optional[str] = ..., model_name: _Optional[str] = ..., version: _Optional[str] = ..., dimensions: _Optional[int] = ..., matryoshka_dims: _Optional[_Iterable[int]] = ..., distance_metric: _Optional[str] = ..., normalize: bool = ..., output_dtype: _Optional[str] = ..., rescore: bool = ..., max_input_tokens: _Optional[int] = ..., tokenizer: _Optional[str] = ..., task_type: _Optional[str] = ..., asymmetric: bool = ..., provider_endpoint_ref: _Optional[str] = ..., vector_backend: _Optional[str] = ..., vector_instance: _Optional[str] = ..., collection_alias: _Optional[str] = ..., active_collection: _Optional[str] = ..., chunking_strategy: _Optional[str] = ..., chunk_tokens: _Optional[int] = ..., chunk_overlap_tokens: _Optional[int] = ..., contextual_retrieval: bool = ..., late_chunking: bool = ..., metadata_json: _Optional[str] = ...) -> None: ...

class RegisterModelResponse(_message.Message):
    __slots__ = ("model_id", "active_collection", "message", "error")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    active_collection: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, model_id: _Optional[str] = ..., active_collection: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListModelsRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token", "status")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    status: EmbeddingModelStatus
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., status: _Optional[_Union[EmbeddingModelStatus, str]] = ...) -> None: ...

class EmbeddingModelSummary(_message.Message):
    __slots__ = ("model_id", "provider", "model_name", "version", "dimensions", "distance_metric", "output_dtype", "task_type", "status", "vector_backend", "collection_alias", "active_collection", "tenant_state")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DTYPE_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VECTOR_BACKEND_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ALIAS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    TENANT_STATE_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    provider: str
    model_name: str
    version: str
    dimensions: int
    distance_metric: str
    output_dtype: str
    task_type: str
    status: EmbeddingModelStatus
    vector_backend: str
    collection_alias: str
    active_collection: str
    tenant_state: EmbeddingTenantState
    def __init__(self, model_id: _Optional[str] = ..., provider: _Optional[str] = ..., model_name: _Optional[str] = ..., version: _Optional[str] = ..., dimensions: _Optional[int] = ..., distance_metric: _Optional[str] = ..., output_dtype: _Optional[str] = ..., task_type: _Optional[str] = ..., status: _Optional[_Union[EmbeddingModelStatus, str]] = ..., vector_backend: _Optional[str] = ..., collection_alias: _Optional[str] = ..., active_collection: _Optional[str] = ..., tenant_state: _Optional[_Union[EmbeddingTenantState, str]] = ...) -> None: ...

class ListModelsResponse(_message.Message):
    __slots__ = ("models", "next_page_token", "message", "error")
    MODELS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    models: _containers.RepeatedCompositeFieldContainer[EmbeddingModelSummary]
    next_page_token: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, models: _Optional[_Iterable[_Union[EmbeddingModelSummary, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteModelRequest(_message.Message):
    __slots__ = ("tenant_id", "model_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    model_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., model_id: _Optional[str] = ...) -> None: ...

class DeleteModelResponse(_message.Message):
    __slots__ = ("deleted", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class SetModelStatusRequest(_message.Message):
    __slots__ = ("tenant_id", "model_id", "status", "replacement_model_id", "retire_after_unix_ms", "tenant_state")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    RETIRE_AFTER_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    TENANT_STATE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    model_id: str
    status: EmbeddingModelStatus
    replacement_model_id: str
    retire_after_unix_ms: int
    tenant_state: EmbeddingTenantState
    def __init__(self, tenant_id: _Optional[str] = ..., model_id: _Optional[str] = ..., status: _Optional[_Union[EmbeddingModelStatus, str]] = ..., replacement_model_id: _Optional[str] = ..., retire_after_unix_ms: _Optional[int] = ..., tenant_state: _Optional[_Union[EmbeddingTenantState, str]] = ...) -> None: ...

class SetModelStatusResponse(_message.Message):
    __slots__ = ("updated", "message", "error", "tenant_state")
    UPDATED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    TENANT_STATE_FIELD_NUMBER: _ClassVar[int]
    updated: bool
    message: str
    error: _dto_pb2.ApiError
    tenant_state: EmbeddingTenantState
    def __init__(self, updated: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., tenant_state: _Optional[_Union[EmbeddingTenantState, str]] = ...) -> None: ...

class CutoverModelAliasRequest(_message.Message):
    __slots__ = ("tenant_id", "model_id", "expected_collection")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    model_id: str
    expected_collection: str
    def __init__(self, tenant_id: _Optional[str] = ..., model_id: _Optional[str] = ..., expected_collection: _Optional[str] = ...) -> None: ...

class CutoverModelAliasResponse(_message.Message):
    __slots__ = ("cutover", "collection_alias", "active_collection", "message", "error")
    CUTOVER_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ALIAS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    cutover: bool
    collection_alias: str
    active_collection: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, cutover: bool = ..., collection_alias: _Optional[str] = ..., active_collection: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class GetEmbeddingJobStatusRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ...) -> None: ...

class EmbeddingJobStatus(_message.Message):
    __slots__ = ("job_id", "source_name", "document_id", "job_type", "mode", "status", "rows_enumerated", "chunks_emitted", "vectors_stored", "failed", "error", "started_at_unix_ms", "finished_at_unix_ms")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ROWS_ENUMERATED_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_EMITTED_FIELD_NUMBER: _ClassVar[int]
    VECTORS_STORED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    source_name: str
    document_id: str
    job_type: str
    mode: str
    status: str
    rows_enumerated: int
    chunks_emitted: int
    vectors_stored: int
    failed: int
    error: str
    started_at_unix_ms: int
    finished_at_unix_ms: int
    def __init__(self, job_id: _Optional[str] = ..., source_name: _Optional[str] = ..., document_id: _Optional[str] = ..., job_type: _Optional[str] = ..., mode: _Optional[str] = ..., status: _Optional[str] = ..., rows_enumerated: _Optional[int] = ..., chunks_emitted: _Optional[int] = ..., vectors_stored: _Optional[int] = ..., failed: _Optional[int] = ..., error: _Optional[str] = ..., started_at_unix_ms: _Optional[int] = ..., finished_at_unix_ms: _Optional[int] = ...) -> None: ...

class GetEmbeddingJobStatusResponse(_message.Message):
    __slots__ = ("job", "message", "error")
    JOB_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    job: EmbeddingJobStatus
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, job: _Optional[_Union[EmbeddingJobStatus, _Mapping]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListEmbeddingWorkItemsRequest(_message.Message):
    __slots__ = ("tenant_id", "job_id", "status", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    job_id: str
    status: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., job_id: _Optional[str] = ..., status: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class EmbeddingWorkItemSummary(_message.Message):
    __slots__ = ("work_item_id", "point_id", "source_name", "parent_pk", "chunk_seq", "chunk_hash", "status", "attempt_count", "max_attempts", "last_error", "next_attempt_at_unix_ms")
    WORK_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    POINT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_PK_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SEQ_FIELD_NUMBER: _ClassVar[int]
    CHUNK_HASH_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    MAX_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    work_item_id: str
    point_id: str
    source_name: str
    parent_pk: str
    chunk_seq: int
    chunk_hash: str
    status: str
    attempt_count: int
    max_attempts: int
    last_error: str
    next_attempt_at_unix_ms: int
    def __init__(self, work_item_id: _Optional[str] = ..., point_id: _Optional[str] = ..., source_name: _Optional[str] = ..., parent_pk: _Optional[str] = ..., chunk_seq: _Optional[int] = ..., chunk_hash: _Optional[str] = ..., status: _Optional[str] = ..., attempt_count: _Optional[int] = ..., max_attempts: _Optional[int] = ..., last_error: _Optional[str] = ..., next_attempt_at_unix_ms: _Optional[int] = ...) -> None: ...

class ListEmbeddingWorkItemsResponse(_message.Message):
    __slots__ = ("work_items", "next_page_token", "message", "error")
    WORK_ITEMS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    work_items: _containers.RepeatedCompositeFieldContainer[EmbeddingWorkItemSummary]
    next_page_token: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, work_items: _Optional[_Iterable[_Union[EmbeddingWorkItemSummary, _Mapping]]] = ..., next_page_token: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReportEmbeddingBatchRequest(_message.Message):
    __slots__ = ("tenant_id", "items", "declared_capacity")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    DECLARED_CAPACITY_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    items: _containers.RepeatedCompositeFieldContainer[ReportEmbeddingRequest]
    declared_capacity: int
    def __init__(self, tenant_id: _Optional[str] = ..., items: _Optional[_Iterable[_Union[ReportEmbeddingRequest, _Mapping]]] = ..., declared_capacity: _Optional[int] = ...) -> None: ...

class ReportEmbeddingBatchItemResult(_message.Message):
    __slots__ = ("work_item_id", "row_pk", "upserted", "error")
    WORK_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    ROW_PK_FIELD_NUMBER: _ClassVar[int]
    UPSERTED_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    work_item_id: str
    row_pk: str
    upserted: bool
    error: str
    def __init__(self, work_item_id: _Optional[str] = ..., row_pk: _Optional[str] = ..., upserted: bool = ..., error: _Optional[str] = ...) -> None: ...

class ReportEmbeddingBatchResponse(_message.Message):
    __slots__ = ("results", "upserted", "failed", "message", "error")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    UPSERTED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ReportEmbeddingBatchItemResult]
    upserted: int
    failed: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, results: _Optional[_Iterable[_Union[ReportEmbeddingBatchItemResult, _Mapping]]] = ..., upserted: _Optional[int] = ..., failed: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReportEmbeddingFailureRequest(_message.Message):
    __slots__ = ("tenant_id", "work_item_id", "error", "retryable", "error_code")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    WORK_ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    work_item_id: str
    error: str
    retryable: bool
    error_code: str
    def __init__(self, tenant_id: _Optional[str] = ..., work_item_id: _Optional[str] = ..., error: _Optional[str] = ..., retryable: bool = ..., error_code: _Optional[str] = ...) -> None: ...

class ReportEmbeddingFailureResponse(_message.Message):
    __slots__ = ("recorded", "dead_lettered", "attempt_count", "message", "error")
    RECORDED_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTERED_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_COUNT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    recorded: bool
    dead_lettered: bool
    attempt_count: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, recorded: bool = ..., dead_lettered: bool = ..., attempt_count: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class IngestDocumentRequest(_message.Message):
    __slots__ = ("tenant_id", "external_id", "title", "raw_text", "storage_object_ref", "content_type", "doc_version", "model_id", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    RAW_TEXT_FIELD_NUMBER: _ClassVar[int]
    STORAGE_OBJECT_REF_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    external_id: str
    title: str
    raw_text: str
    storage_object_ref: str
    content_type: str
    doc_version: str
    model_id: str
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., external_id: _Optional[str] = ..., title: _Optional[str] = ..., raw_text: _Optional[str] = ..., storage_object_ref: _Optional[str] = ..., content_type: _Optional[str] = ..., doc_version: _Optional[str] = ..., model_id: _Optional[str] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class IngestDocumentResponse(_message.Message):
    __slots__ = ("document_id", "job_id", "accepted", "message", "error", "source_name")
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    document_id: str
    job_id: str
    accepted: bool
    message: str
    error: _dto_pb2.ApiError
    source_name: str
    def __init__(self, document_id: _Optional[str] = ..., job_id: _Optional[str] = ..., accepted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., source_name: _Optional[str] = ...) -> None: ...

class IngestDocumentBatchRequest(_message.Message):
    __slots__ = ("tenant_id", "documents")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    documents: _containers.RepeatedCompositeFieldContainer[IngestDocumentRequest]
    def __init__(self, tenant_id: _Optional[str] = ..., documents: _Optional[_Iterable[_Union[IngestDocumentRequest, _Mapping]]] = ...) -> None: ...

class IngestDocumentBatchResponse(_message.Message):
    __slots__ = ("documents", "accepted", "failed", "message", "error")
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    documents: _containers.RepeatedCompositeFieldContainer[IngestDocumentResponse]
    accepted: int
    failed: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, documents: _Optional[_Iterable[_Union[IngestDocumentResponse, _Mapping]]] = ..., accepted: _Optional[int] = ..., failed: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReportParsedDocumentRequest(_message.Message):
    __slots__ = ("tenant_id", "document_id", "job_id", "text", "content_hash")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    document_id: str
    job_id: str
    text: str
    content_hash: str
    def __init__(self, tenant_id: _Optional[str] = ..., document_id: _Optional[str] = ..., job_id: _Optional[str] = ..., text: _Optional[str] = ..., content_hash: _Optional[str] = ...) -> None: ...

class ReportParsedDocumentResponse(_message.Message):
    __slots__ = ("accepted", "chunks_emitted", "message", "error")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_EMITTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    chunks_emitted: int
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, accepted: bool = ..., chunks_emitted: _Optional[int] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ReportRetrievalEvaluationRequest(_message.Message):
    __slots__ = ("tenant_id", "evaluation_id", "context_relevance", "groundedness", "answer_relevance", "evaluator_model", "error")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    EVALUATION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_RELEVANCE_FIELD_NUMBER: _ClassVar[int]
    GROUNDEDNESS_FIELD_NUMBER: _ClassVar[int]
    ANSWER_RELEVANCE_FIELD_NUMBER: _ClassVar[int]
    EVALUATOR_MODEL_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    evaluation_id: str
    context_relevance: float
    groundedness: float
    answer_relevance: float
    evaluator_model: str
    error: str
    def __init__(self, tenant_id: _Optional[str] = ..., evaluation_id: _Optional[str] = ..., context_relevance: _Optional[float] = ..., groundedness: _Optional[float] = ..., answer_relevance: _Optional[float] = ..., evaluator_model: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class ReportRetrievalEvaluationResponse(_message.Message):
    __slots__ = ("accepted", "message", "error")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, accepted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
