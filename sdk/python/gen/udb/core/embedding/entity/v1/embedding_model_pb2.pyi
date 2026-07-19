import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from udb.core.common.v1 import db_pb2 as _db_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EmbeddingModel(_message.Message):
    __slots__ = ("model_id", "tenant_id", "provider", "model_name", "version", "dimensions", "matryoshka_dims_json", "distance_metric", "normalize", "output_dtype", "rescore", "max_input_tokens", "tokenizer", "task_type", "asymmetric", "provider_endpoint_ref", "status", "retire_after", "replacement_model_id", "vector_backend", "vector_instance", "collection_alias", "active_collection", "chunking_strategy", "chunk_tokens", "chunk_overlap_tokens", "contextual_retrieval", "late_chunking", "tenant_state", "metadata_json", "created_at", "updated_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DIMENSIONS_FIELD_NUMBER: _ClassVar[int]
    MATRYOSHKA_DIMS_JSON_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_METRIC_FIELD_NUMBER: _ClassVar[int]
    NORMALIZE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DTYPE_FIELD_NUMBER: _ClassVar[int]
    RESCORE_FIELD_NUMBER: _ClassVar[int]
    MAX_INPUT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOKENIZER_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    ASYMMETRIC_FIELD_NUMBER: _ClassVar[int]
    PROVIDER_ENDPOINT_REF_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RETIRE_AFTER_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_BACKEND_FIELD_NUMBER: _ClassVar[int]
    VECTOR_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_ALIAS_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_COLLECTION_FIELD_NUMBER: _ClassVar[int]
    CHUNKING_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    CHUNK_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CHUNK_OVERLAP_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CONTEXTUAL_RETRIEVAL_FIELD_NUMBER: _ClassVar[int]
    LATE_CHUNKING_FIELD_NUMBER: _ClassVar[int]
    TENANT_STATE_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    tenant_id: str
    provider: str
    model_name: str
    version: str
    dimensions: int
    matryoshka_dims_json: str
    distance_metric: str
    normalize: bool
    output_dtype: str
    rescore: bool
    max_input_tokens: int
    tokenizer: str
    task_type: str
    asymmetric: bool
    provider_endpoint_ref: str
    status: str
    retire_after: _timestamp_pb2.Timestamp
    replacement_model_id: str
    vector_backend: str
    vector_instance: str
    collection_alias: str
    active_collection: str
    chunking_strategy: str
    chunk_tokens: int
    chunk_overlap_tokens: int
    contextual_retrieval: bool
    late_chunking: bool
    tenant_state: str
    metadata_json: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., tenant_id: _Optional[str] = ..., provider: _Optional[str] = ..., model_name: _Optional[str] = ..., version: _Optional[str] = ..., dimensions: _Optional[int] = ..., matryoshka_dims_json: _Optional[str] = ..., distance_metric: _Optional[str] = ..., normalize: bool = ..., output_dtype: _Optional[str] = ..., rescore: bool = ..., max_input_tokens: _Optional[int] = ..., tokenizer: _Optional[str] = ..., task_type: _Optional[str] = ..., asymmetric: bool = ..., provider_endpoint_ref: _Optional[str] = ..., status: _Optional[str] = ..., retire_after: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., replacement_model_id: _Optional[str] = ..., vector_backend: _Optional[str] = ..., vector_instance: _Optional[str] = ..., collection_alias: _Optional[str] = ..., active_collection: _Optional[str] = ..., chunking_strategy: _Optional[str] = ..., chunk_tokens: _Optional[int] = ..., chunk_overlap_tokens: _Optional[int] = ..., contextual_retrieval: bool = ..., late_chunking: bool = ..., tenant_state: _Optional[str] = ..., metadata_json: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
