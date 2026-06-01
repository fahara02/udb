from google.protobuf import struct_pb2 as _struct_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorSearchRequest(_message.Message):
    __slots__ = ("context", "collection", "vector", "filter", "limit", "score_threshold", "with_payload")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    SCORE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    collection: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    filter: _struct_pb2.Struct
    limit: int
    score_threshold: float
    with_payload: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., collection: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., score_threshold: _Optional[float] = ..., with_payload: bool = ...) -> None: ...

class VectorHybridSearchRequest(_message.Message):
    __slots__ = ("context", "collection", "vector", "text_query", "filter", "limit", "fusion_weights", "with_payload")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    TEXT_QUERY_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    FUSION_WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    collection: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    text_query: str
    filter: _struct_pb2.Struct
    limit: int
    fusion_weights: _containers.RepeatedScalarFieldContainer[float]
    with_payload: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., collection: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., text_query: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., fusion_weights: _Optional[_Iterable[float]] = ..., with_payload: bool = ...) -> None: ...

class VectorPointMutation(_message.Message):
    __slots__ = ("id", "vector", "payload")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    payload: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class VectorUpsertRequest(_message.Message):
    __slots__ = ("context", "collection", "points", "idempotency_key")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    collection: str
    points: _containers.RepeatedCompositeFieldContainer[VectorPointMutation]
    idempotency_key: str
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., collection: _Optional[str] = ..., points: _Optional[_Iterable[_Union[VectorPointMutation, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class VectorPoint(_message.Message):
    __slots__ = ("id", "score", "payload")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    payload: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class VectorSet(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[VectorPoint]
    def __init__(self, points: _Optional[_Iterable[_Union[VectorPoint, _Mapping]]] = ...) -> None: ...
