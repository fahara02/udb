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

class SearchMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SEARCH_MODE_UNSPECIFIED: _ClassVar[SearchMode]
    SEARCH_MODE_TEXT: _ClassVar[SearchMode]
    SEARCH_MODE_VECTOR: _ClassVar[SearchMode]
    SEARCH_MODE_HYBRID: _ClassVar[SearchMode]
SEARCH_MODE_UNSPECIFIED: SearchMode
SEARCH_MODE_TEXT: SearchMode
SEARCH_MODE_VECTOR: SearchMode
SEARCH_MODE_HYBRID: SearchMode

class CreateIndexRequest(_message.Message):
    __slots__ = ("tenant_id", "index_name", "source_message_type", "backend", "resource_name", "vector_dims", "metadata_json")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DIMS_FIELD_NUMBER: _ClassVar[int]
    METADATA_JSON_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    index_name: str
    source_message_type: str
    backend: str
    resource_name: str
    vector_dims: int
    metadata_json: str
    def __init__(self, tenant_id: _Optional[str] = ..., index_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., backend: _Optional[str] = ..., resource_name: _Optional[str] = ..., vector_dims: _Optional[int] = ..., metadata_json: _Optional[str] = ...) -> None: ...

class CreateIndexResponse(_message.Message):
    __slots__ = ("index_id", "index_name", "tenant_column", "message", "error")
    INDEX_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    TENANT_COLUMN_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    index_id: str
    index_name: str
    tenant_column: str
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, index_id: _Optional[str] = ..., index_name: _Optional[str] = ..., tenant_column: _Optional[str] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class DeleteIndexRequest(_message.Message):
    __slots__ = ("tenant_id", "index_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    index_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., index_name: _Optional[str] = ...) -> None: ...

class DeleteIndexResponse(_message.Message):
    __slots__ = ("deleted", "message", "error")
    DELETED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    deleted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, deleted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...

class ListIndexesRequest(_message.Message):
    __slots__ = ("tenant_id", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class SearchIndexSummary(_message.Message):
    __slots__ = ("index_id", "index_name", "source_message_type", "backend", "resource_name", "vector_dims", "status")
    INDEX_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    BACKEND_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_DIMS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    index_id: str
    index_name: str
    source_message_type: str
    backend: str
    resource_name: str
    vector_dims: int
    status: str
    def __init__(self, index_id: _Optional[str] = ..., index_name: _Optional[str] = ..., source_message_type: _Optional[str] = ..., backend: _Optional[str] = ..., resource_name: _Optional[str] = ..., vector_dims: _Optional[int] = ..., status: _Optional[str] = ...) -> None: ...

class ListIndexesResponse(_message.Message):
    __slots__ = ("indexes", "message", "error", "next_page_token")
    INDEXES_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    indexes: _containers.RepeatedCompositeFieldContainer[SearchIndexSummary]
    message: str
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, indexes: _Optional[_Iterable[_Union[SearchIndexSummary, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("tenant_id", "index_name", "query_text", "query_vector", "top_k", "mode", "page_size", "page_token")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    QUERY_TEXT_FIELD_NUMBER: _ClassVar[int]
    QUERY_VECTOR_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    index_name: str
    query_text: str
    query_vector: _containers.RepeatedScalarFieldContainer[float]
    top_k: int
    mode: SearchMode
    page_size: int
    page_token: str
    def __init__(self, tenant_id: _Optional[str] = ..., index_name: _Optional[str] = ..., query_text: _Optional[str] = ..., query_vector: _Optional[_Iterable[float]] = ..., top_k: _Optional[int] = ..., mode: _Optional[_Union[SearchMode, str]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class SearchHit(_message.Message):
    __slots__ = ("id", "score", "index_name", "payload_json")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    index_name: str
    payload_json: str
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., index_name: _Optional[str] = ..., payload_json: _Optional[str] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("hits", "message", "error", "next_page_token")
    HITS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[SearchHit]
    message: str
    error: _dto_pb2.ApiError
    next_page_token: str
    def __init__(self, hits: _Optional[_Iterable[_Union[SearchHit, _Mapping]]] = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ReindexRequest(_message.Message):
    __slots__ = ("tenant_id", "index_name")
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_NAME_FIELD_NUMBER: _ClassVar[int]
    tenant_id: str
    index_name: str
    def __init__(self, tenant_id: _Optional[str] = ..., index_name: _Optional[str] = ...) -> None: ...

class ReindexResponse(_message.Message):
    __slots__ = ("reindex_id", "accepted", "message", "error")
    REINDEX_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    reindex_id: str
    accepted: bool
    message: str
    error: _dto_pb2.ApiError
    def __init__(self, reindex_id: _Optional[str] = ..., accepted: bool = ..., message: _Optional[str] = ..., error: _Optional[_Union[_dto_pb2.ApiError, _Mapping]] = ...) -> None: ...
