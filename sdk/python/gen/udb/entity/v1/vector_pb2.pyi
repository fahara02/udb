from google.protobuf import struct_pb2 as _struct_pb2
from udb.entity.v1 import context_pb2 as _context_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorFusionStrategy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VECTOR_FUSION_STRATEGY_UNSPECIFIED: _ClassVar[VectorFusionStrategy]
    VECTOR_FUSION_STRATEGY_RRF: _ClassVar[VectorFusionStrategy]
    VECTOR_FUSION_STRATEGY_WEIGHTED: _ClassVar[VectorFusionStrategy]
    VECTOR_FUSION_STRATEGY_DBSF: _ClassVar[VectorFusionStrategy]
VECTOR_FUSION_STRATEGY_UNSPECIFIED: VectorFusionStrategy
VECTOR_FUSION_STRATEGY_RRF: VectorFusionStrategy
VECTOR_FUSION_STRATEGY_WEIGHTED: VectorFusionStrategy
VECTOR_FUSION_STRATEGY_DBSF: VectorFusionStrategy

class VectorSearchRequest(_message.Message):
    __slots__ = ("context", "collection", "vector", "filter", "limit", "score_threshold", "with_payload", "with_vector", "vector_name", "quantization_rescore")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    SCORE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WITH_VECTOR_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    QUANTIZATION_RESCORE_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    collection: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    filter: _struct_pb2.Struct
    limit: int
    score_threshold: float
    with_payload: bool
    with_vector: bool
    vector_name: str
    quantization_rescore: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., collection: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., score_threshold: _Optional[float] = ..., with_payload: bool = ..., with_vector: bool = ..., vector_name: _Optional[str] = ..., quantization_rescore: bool = ...) -> None: ...

class VectorHybridSearchRequest(_message.Message):
    __slots__ = ("context", "collection", "vector", "text_query", "filter", "limit", "fusion_weights", "with_payload", "with_vector", "vector_name", "fusion_strategy", "prefetch_limit", "quantization_rescore")
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    COLLECTION_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    TEXT_QUERY_FIELD_NUMBER: _ClassVar[int]
    FILTER_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    FUSION_WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    WITH_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    WITH_VECTOR_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    FUSION_STRATEGY_FIELD_NUMBER: _ClassVar[int]
    PREFETCH_LIMIT_FIELD_NUMBER: _ClassVar[int]
    QUANTIZATION_RESCORE_FIELD_NUMBER: _ClassVar[int]
    context: _context_pb2.RequestContext
    collection: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    text_query: str
    filter: _struct_pb2.Struct
    limit: int
    fusion_weights: _containers.RepeatedScalarFieldContainer[float]
    with_payload: bool
    with_vector: bool
    vector_name: str
    fusion_strategy: VectorFusionStrategy
    prefetch_limit: int
    quantization_rescore: bool
    def __init__(self, context: _Optional[_Union[_context_pb2.RequestContext, _Mapping]] = ..., collection: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., text_query: _Optional[str] = ..., filter: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., limit: _Optional[int] = ..., fusion_weights: _Optional[_Iterable[float]] = ..., with_payload: bool = ..., with_vector: bool = ..., vector_name: _Optional[str] = ..., fusion_strategy: _Optional[_Union[VectorFusionStrategy, str]] = ..., prefetch_limit: _Optional[int] = ..., quantization_rescore: bool = ...) -> None: ...

class VectorPointMutation(_message.Message):
    __slots__ = ("id", "vector", "payload", "vector_name")
    ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    vector: _containers.RepeatedScalarFieldContainer[float]
    payload: _struct_pb2.Struct
    vector_name: str
    def __init__(self, id: _Optional[str] = ..., vector: _Optional[_Iterable[float]] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., vector_name: _Optional[str] = ...) -> None: ...

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
    __slots__ = ("id", "score", "payload", "vector", "vector_name")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    VECTOR_NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    score: float
    payload: _struct_pb2.Struct
    vector: _containers.RepeatedScalarFieldContainer[float]
    vector_name: str
    def __init__(self, id: _Optional[str] = ..., score: _Optional[float] = ..., payload: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., vector: _Optional[_Iterable[float]] = ..., vector_name: _Optional[str] = ...) -> None: ...

class VectorSet(_message.Message):
    __slots__ = ("points",)
    POINTS_FIELD_NUMBER: _ClassVar[int]
    points: _containers.RepeatedCompositeFieldContainer[VectorPoint]
    def __init__(self, points: _Optional[_Iterable[_Union[VectorPoint, _Mapping]]] = ...) -> None: ...
