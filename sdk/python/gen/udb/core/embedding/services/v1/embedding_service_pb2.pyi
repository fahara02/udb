from google.api import annotations_pb2 as _annotations_pb2
from udb.core.common.v1 import dto_pb2 as _dto_pb2
from udb.core.common.v1 import security_pb2 as _security_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

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
    __slots__ = ("tenant_id", "source_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("tenant_id", "source_name", "row_pk", "vector", "model", "dims")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    ROW_PK_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    DIMS_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    row_pk: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    model: str
    dims: int
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., row_pk: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., model: _Optional[str] = ..., dims: _Optional[int] = ...) -> None: ...

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
    __slots__ = ("tenant_id", "source_name", "query_text", "query_vector", "top_k", "filter_json", "score_threshold")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    FILTER_JSON_FIELD_NUMBER: _ClassVar[int]
    SCORE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    source_name: str
    query_text: str
    query_vector: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    filter_json: str
    score_threshold: float
    def __init__(self, tenant_id: _Optional[str] = ..., source_name: _Optional[str] = ..., query_text: _Optional[str] = ..., query_vector: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ..., filter_json: _Optional[str] = ..., score_threshold: _Optional[float] = ...) -> None: ...

class RetrieveHit(_message.Message):
    __slots__ = ("id", "score", "payload_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    payload_json: str
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., payload_json: _Optional[str] = ...) -> None: ...

class RetrieveResponse(_message.Message):
    __slots__ = ("hits", "message", "error")
    HITS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[RetrieveHit]
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, hits: _Optional[_Iterable[_Union[RetrieveHit, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
